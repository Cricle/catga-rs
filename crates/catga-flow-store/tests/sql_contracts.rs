//! Shared real-SQL contracts for the public FlowStore adapters.
//!
//! Each backend test owns connection setup and migrations, then delegates the
//! backend-neutral persistence rules here. Keeping the rules in one place
//! prevents MySQL, PostgreSQL, and SQL Server coverage from drifting.

use std::{
    env,
    time::{Duration, SystemTime},
};

use catga_codec_memorypack::MemoryPackable;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{
    DslStepProgress, DslStepProgressStore, DueFlowScheduler, StateMachineSnapshot,
    StateMachineStore,
};

/// Returns an explicitly configured service URL.
///
/// A developer workstation without the variable skips an external-service
/// contract. Any CI invocation missing its required URL fails loudly instead
/// of silently downgrading an E2E target into a no-op.
pub fn service_url(variable: &str) -> CatgaResult<Option<Box<str>>> {
    match env::var(variable) {
        Ok(url) if !url.trim().is_empty() => Ok(Some(url.into_boxed_str())),
        _ if env::var_os("CI").is_some_and(|value| !value.is_empty()) => Err(CatgaError::new(
            ErrorCode::Unavailable,
            format!("{variable} must be configured when CI executes this SQL E2E test"),
        )),
        _ => Ok(None),
    }
}

/// A compact state persisted through the public bounded MemoryPack snapshot codec.
#[derive(Clone, Debug, Eq, MemoryPackable, PartialEq)]
pub struct ContractState {
    paid: bool,
    quantity: u32,
}

/// Verifies durable snapshots preserve create, load, and optimistic-CAS semantics.
pub async fn state_machine_contract<S>(store: &S, prefix: &str) -> CatgaResult<()>
where
    S: StateMachineStore<ContractState> + Sync,
{
    let id = format!("{prefix}/snapshot");
    let initial = StateMachineSnapshot::new(
        id.as_str(),
        ContractState {
            paid: false,
            quantity: 3,
        },
    );
    assert!(store.create(initial.clone()).await?);
    assert!(!store.create(initial.clone()).await?);
    assert_eq!(store.get(id.as_str()).await?, Some(initial.clone()));

    let next = initial.next_version(ContractState {
        paid: true,
        quantity: 3,
    })?;
    assert!(store.update(initial.version(), next.clone()).await?);
    assert!(!store.update(initial.version(), next.clone()).await?);
    assert_eq!(store.get(id.as_str()).await?, Some(next));

    let race_id = format!("{prefix}/snapshot-race");
    let race = StateMachineSnapshot::new(
        race_id.as_str(),
        ContractState {
            paid: false,
            quantity: 1,
        },
    );
    assert!(store.create(race.clone()).await?);
    let first = race.next_version(ContractState {
        paid: true,
        quantity: 2,
    })?;
    let second = race.next_version(ContractState {
        paid: true,
        quantity: 3,
    })?;
    let (first, second) = tokio::join!(
        store.update(race.version(), first),
        store.update(race.version(), second),
    );
    assert_eq!(usize::from(first?) + usize::from(second?), 1);
    Ok(())
}

/// Verifies recoverable DSL progress is versioned, readable, and deletable.
pub async fn dsl_progress_contract<S>(store: &S, prefix: &str) -> CatgaResult<()>
where
    S: DslStepProgressStore + Sync,
{
    let flow_id = format!("{prefix}/dsl-progress");
    let initial = DslStepProgress::new(flow_id.as_str(), 4, b"initial".as_slice());
    assert!(store.create(initial.clone()).await?);
    assert!(!store.create(initial.clone()).await?);
    assert_eq!(store.get(flow_id.as_str(), 4).await?, Some(initial.clone()));

    let next = initial.clone().next_version(b"recovered".as_slice())?;
    assert!(store.update(initial.version(), next.clone()).await?);
    assert!(!store.update(initial.version(), initial).await?);
    assert_eq!(store.get(flow_id.as_str(), 4).await?, Some(next));
    assert!(store.delete(flow_id.as_str(), 4).await?);
    assert!(!store.delete(flow_id.as_str(), 4).await?);
    assert!(store.get(flow_id.as_str(), 4).await?.is_none());
    Ok(())
}

/// Verifies idempotent creation, bounded claims, recovery, and owner fencing.
pub async fn scheduler_contract<S>(scheduler: &S, prefix: &str) -> CatgaResult<()>
where
    S: DueFlowScheduler + Sync,
{
    let flow_id = format!("{prefix}/scheduler-flow");
    let due = SystemTime::now() + Duration::from_secs(10);
    let (first, second) = tokio::join!(
        scheduler.schedule_resume(flow_id.as_str(), "charge", due),
        scheduler.schedule_resume(flow_id.as_str(), "charge", due),
    );
    let schedule_id = first?;
    assert_eq!(schedule_id, second?);

    let first_claim = scheduler
        .claim_due("worker-a", due, Duration::from_secs(30), 2)
        .await?;
    assert_eq!(first_claim.len(), 1);
    let claimed = required(first_claim.first(), "initial schedule was not claimed")?;
    assert_eq!(claimed.schedule_id(), schedule_id.as_ref());
    assert!(
        scheduler
            .claim_due("worker-b", due, Duration::from_secs(30), 2)
            .await?
            .is_empty()
    );
    assert!(!scheduler.cancel_resume(claimed.schedule_id()).await?);
    assert!(
        scheduler
            .renew_due(
                "worker-a",
                claimed.schedule_id(),
                due + Duration::from_secs(1),
                Duration::from_secs(30),
            )
            .await?
    );
    assert!(
        scheduler
            .release_due("worker-a", claimed.schedule_id())
            .await?
    );

    let recovered = scheduler
        .claim_due(
            "worker-b",
            due + Duration::from_secs(2),
            Duration::from_secs(30),
            2,
        )
        .await?;
    let reclaimed = required(recovered.first(), "released schedule was not reclaimed")?;
    assert_eq!(reclaimed.schedule_id(), schedule_id.as_ref());
    assert!(
        !scheduler
            .ack_due("worker-a", reclaimed.schedule_id())
            .await?
    );
    assert!(
        scheduler
            .ack_due("worker-b", reclaimed.schedule_id())
            .await?
    );
    assert!(
        !scheduler
            .ack_due("worker-b", reclaimed.schedule_id())
            .await?
    );

    for state_id in ["reserve", "notify", "receipt"] {
        scheduler
            .schedule_resume(flow_id.as_str(), state_id, due)
            .await?;
    }
    assert_eq!(
        scheduler
            .claim_due("worker-c", due, Duration::from_secs(30), 2)
            .await?
            .len(),
        2
    );
    Ok(())
}

fn required<'a, T>(value: Option<&'a T>, message: &'static str) -> CatgaResult<&'a T> {
    value.ok_or_else(|| CatgaError::new(ErrorCode::Internal, message))
}
