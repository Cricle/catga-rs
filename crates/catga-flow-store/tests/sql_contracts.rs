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
    DslStepProgress, DslStepProgressStore, DueFlowScheduler, FlowContinuation, FlowQuery,
    FlowState, FlowStatus, StateMachineSnapshot, StateMachineStore, SuspendedFlowStore,
    WaitCondition, WaitPolicy,
};

/// Returns an explicitly configured service URL.
///
/// A job without `CATGA_REQUIRE_EXTERNAL_SERVICES` skips an external-service
/// contract. E2E jobs set that marker, so a missing required URL fails loudly
/// instead of silently downgrading an E2E target into a no-op.
pub fn service_url(variable: &str) -> CatgaResult<Option<Box<str>>> {
    match env::var(variable) {
        Ok(url) if !url.trim().is_empty() => Ok(Some(url.into_boxed_str())),
        _ if env::var_os("CATGA_REQUIRE_EXTERNAL_SERVICES")
            .is_some_and(|value| !value.is_empty()) =>
        {
            Err(CatgaError::new(
                ErrorCode::Unavailable,
                format!("{variable} must be configured when SQL E2E is required"),
            ))
        }
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
    let prefix = isolated_prefix(prefix);
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
    let prefix = isolated_prefix(prefix);
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
    let prefix = isolated_prefix(prefix);
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

/// Verifies continuation mutation is version-fenced while physical mutations stay idempotent.
///
/// The same contract runs against every server SQL implementation. It deliberately checks
/// stale snapshots after a heartbeat and child-result write: both alter the physical revision
/// without advancing the business version, which is the race the stores must reject.
pub async fn suspended_flow_contract<S>(store: &S, prefix: &str) -> CatgaResult<()>
where
    S: SuspendedFlowStore + Sync,
{
    let prefix = isolated_prefix(prefix);
    let waiting_id = format!("{prefix}/waiting");
    let correlation = format!("{prefix}/wait");
    let waiting = FlowContinuation::waiting(
        FlowState::new(waiting_id.as_str(), "continuation-contract", [], "node-a").suspended(),
        "await-children",
        WaitCondition::new(
            correlation.as_str(),
            WaitPolicy::All,
            2,
            SystemTime::now(),
            Duration::from_secs(30),
        ),
    );
    assert!(store.create(waiting.clone()).await?);
    assert!(!store.create(waiting.clone()).await?);
    assert!(store.get("missing-continuation").await?.is_none());
    assert_eq!(store.get(waiting_id.as_str()).await?, Some(waiting.clone()));

    let discovered = store
        .query(
            &FlowQuery::new(1, 4)?
                .with_status(FlowStatus::Suspended)
                .with_flow_type("continuation-contract")
                .created_between(
                    waiting.created_at(),
                    waiting.created_at() + Duration::from_secs(1),
                )?,
        )
        .await?;
    assert_eq!(discovered.len(), 1);
    assert_eq!(discovered[0].id(), waiting_id);
    assert_eq!(
        store
            .get_by_wait_correlation(correlation.as_str())
            .await?
            .as_ref()
            .map(|value| value.state().id()),
        Some(waiting_id.as_str())
    );
    assert!(
        store
            .get_by_wait_correlation("missing-continuation-correlation")
            .await?
            .is_none()
    );

    assert!(
        !store
            .record_wait_success(waiting_id.as_str(), 1, "child-a", b"stale".to_vec())
            .await?
    );
    assert!(
        store
            .record_wait_success(waiting_id.as_str(), 0, "child-a", b"accepted".to_vec())
            .await?
    );
    assert!(
        store
            .record_wait_success(waiting_id.as_str(), 0, "child-a", b"duplicate".to_vec())
            .await?
    );
    assert!(
        store
            .record_wait_failure(
                waiting_id.as_str(),
                0,
                "child-b",
                CatgaError::new(ErrorCode::Validation, "child rejected"),
            )
            .await?
    );
    assert!(
        store
            .record_wait_failure(
                waiting_id.as_str(),
                0,
                "child-b",
                CatgaError::new(ErrorCode::Validation, "duplicate rejection"),
            )
            .await?
    );
    let persisted = store.get(waiting_id.as_str()).await?;
    let persisted_wait = required(persisted.as_ref(), "persisted wait")?;
    let condition = required(persisted_wait.wait(), "persisted wait condition")?;
    assert_eq!(condition.completed_count(), 2);
    assert_eq!(condition.results()[0].payload(), Some(&b"accepted"[..]));
    assert_eq!(
        condition.results()[1].error().map(CatgaError::code),
        Some(ErrorCode::Validation)
    );

    let claim_id = format!("{prefix}/claim");
    let runnable = FlowContinuation::new(
        FlowState::new(claim_id.as_str(), "continuation-contract", [], "node-a"),
        "run",
    );
    assert!(store.create(runnable.clone()).await?);
    assert!(!store.heartbeat(claim_id.as_str(), "wrong-owner", 0).await?);
    assert!(store.heartbeat(claim_id.as_str(), "node-a", 0).await?);
    let stale_claim = runnable.clone().with_state(
        runnable
            .state()
            .clone()
            .claimed_by("node-b")
            .next_version()?,
    );
    assert!(!store.claim(&runnable, stale_claim).await?);

    let stored_current = store.get(claim_id.as_str()).await?;
    let current = required(stored_current.as_ref(), "heartbeated continuation")?;
    let claimed = current.clone().with_state(
        current
            .state()
            .clone()
            .claimed_by("node-b")
            .next_version()?,
    );
    assert!(store.claim(current, claimed.clone()).await?);
    assert!(
        !store
            .update(claimed.state().version() + 1, claimed.clone())
            .await?
    );
    let ready = claimed
        .clone()
        .ready()
        .with_state(claimed.state().clone().suspended().next_version()?);
    assert!(
        store
            .update(claimed.state().version(), ready.clone())
            .await?
    );
    assert!(
        !store
            .delete(claim_id.as_str(), claimed.state().version())
            .await?
    );
    assert!(
        store
            .delete(claim_id.as_str(), ready.state().version())
            .await?
    );
    assert!(
        !store
            .delete(claim_id.as_str(), ready.state().version())
            .await?
    );
    Ok(())
}

fn required<'a, T>(value: Option<&'a T>, message: &'static str) -> CatgaResult<&'a T> {
    value.ok_or_else(|| CatgaError::new(ErrorCode::Internal, message))
}

/// Creates an ID namespace unique to one concurrently executing SQL contract.
///
/// All real-service tests share one database per backend in CI. Test functions
/// are scheduled concurrently, so a stable prefix would turn the second
/// `create` assertion into a cross-test collision rather than an idempotency
/// check of the store under test.
fn isolated_prefix(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4())
}
