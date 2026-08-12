//! Shared strict edge-case contracts for the SQL store dialects.
//!
//! Each dialect test file wires one [`EdgeDialect`] implementation (raw column
//! rewrites through its own driver) into these runners, so every backend proves
//! the same corruption fencing, optimistic-concurrency rejection, and validation
//! behavior without duplicating the assertions. Corruption here always means a
//! direct column update; the stores under test must detect the inconsistency
//! instead of trusting the indexed columns.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use catga_core::MemoryPackable;
use catga_core::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackSerializer, MemoryPackWriter,
};
use catga_core::flow::{
    DslStepProgress, DslStepProgressStore, DueFlowScheduler, FlowContinuation, FlowQuery,
    FlowScheduler, FlowState, FlowStore, StateMachineSnapshot, StateMachineStore,
    SuspendedFlowStore, TimedOutFlowPoll, TimedOutFlowStore, WaitCondition, WaitPolicy,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow_store::{
    SqlDslStepProgressStore, SqlFlowScheduler, SqlFlowStore, SqlStateMachineStore,
    SqlSuspendedFlowStore,
};

/// One byte beyond the durable one-megabyte payload bound.
const OVERSIZE_PAYLOAD_BYTES: usize = 1024 * 1024 + 1;

/// A compact state persisted through the default bounded MemoryPack snapshot codec.
#[derive(Clone, Debug, Eq, MemoryPackable, PartialEq)]
pub struct EdgeState {
    pub paid: bool,
    pub quantity: u32,
}

/// A state whose encoded frame can exceed the durable snapshot payload bound.
#[derive(Clone, Debug, Eq, MemoryPackable, PartialEq)]
pub struct BigState {
    data: Vec<u8>,
}

/// Raw column rewrites a dialect must provide so the shared runners can inject
/// indexed-column/payload inconsistencies that no public API is allowed to create.
#[async_trait]
pub trait EdgeDialect: Sync {
    async fn set_flow_identity(&self, flow_id: &str, replacement: &str) -> CatgaResult<()>;
    async fn flow_payload(&self, flow_id: &str) -> CatgaResult<Vec<u8>>;
    async fn set_flow_payload(&self, flow_id: &str, payload: &[u8]) -> CatgaResult<()>;
    async fn set_flow_heartbeat_ms(&self, flow_id: &str, heartbeat_ms: i64) -> CatgaResult<()>;

    async fn set_continuation_identity(&self, flow_id: &str, replacement: &str) -> CatgaResult<()>;
    async fn continuation_payload(&self, flow_id: &str) -> CatgaResult<Vec<u8>>;
    async fn set_continuation_payload(&self, flow_id: &str, payload: &[u8]) -> CatgaResult<()>;
    async fn set_continuation_wait_correlation(
        &self,
        flow_id: &str,
        correlation: &str,
        correlation_key: &[u8; 32],
    ) -> CatgaResult<()>;
    async fn set_continuation_status(&self, flow_id: &str, status: i64) -> CatgaResult<()>;
    async fn set_continuation_updated_subsec_ns(
        &self,
        flow_id: &str,
        subsec_ns: i64,
    ) -> CatgaResult<()>;

    async fn set_progress_identity(
        &self,
        flow_id: &str,
        step_index: u32,
        replacement: &str,
    ) -> CatgaResult<()>;
    async fn progress_payload(&self, flow_id: &str, step_index: u32) -> CatgaResult<Vec<u8>>;
    async fn set_progress_payload(
        &self,
        flow_id: &str,
        step_index: u32,
        payload: &[u8],
    ) -> CatgaResult<()>;

    async fn set_snapshot_identity(&self, instance_id: &str, replacement: &str) -> CatgaResult<()>;
    async fn set_snapshot_version(&self, instance_id: &str, version: i64) -> CatgaResult<()>;
    async fn set_snapshot_payload(&self, instance_id: &str, payload: &[u8]) -> CatgaResult<()>;

    async fn set_schedule_identity(&self, schedule_id: &str, replacement: &str) -> CatgaResult<()>;

    async fn bump_flow_revision(&self, flow_id: &str) -> CatgaResult<()>;
    async fn bump_continuation_revision(&self, flow_id: &str) -> CatgaResult<()>;
    async fn bump_progress_revision(&self, flow_id: &str, step_index: u32) -> CatgaResult<()>;
    async fn bump_snapshot_revision(&self, instance_id: &str) -> CatgaResult<()>;
}

/// Asserts that one operation fails and returns its error for a code assertion.
pub fn expect_failure<T: std::fmt::Debug>(result: CatgaResult<T>, operation: &str) -> CatgaError {
    match result {
        Ok(value) => panic!("{operation} unexpectedly succeeded: {value:?}"),
        Err(error) => error,
    }
}

/// Exercises duplicate identities, hash-collision fencing, payload/identity agreement,
/// stale-claim filtering, batch insertion, and every lifecycle-status encoding.
pub async fn flow_store_edges<D: EdgeDialect>(
    store: &SqlFlowStore,
    dialect: &D,
    prefix: &str,
) -> CatgaResult<()> {
    let flow_type = format!("{prefix}-type");

    let missing_id = format!("{prefix}-missing");
    let missing = FlowState::new(missing_id.as_str(), flow_type.as_str(), [], "node-a");
    assert!(!store.update(0, missing.clone().next_version()?).await?);
    assert!(!store.update(3, missing).await?);
    assert!(!store.heartbeat(missing_id.as_str(), "node-a", 0).await?);

    let original_id = format!("{prefix}-original");
    let original = FlowState::new(original_id.as_str(), flow_type.as_str(), [], "node-a");
    assert!(store.create(original.clone()).await?);
    assert!(!store.create(original).await?);
    let original_payload = dialect.flow_payload(original_id.as_str()).await?;

    let collision_id = format!("{prefix}-collision");
    let collision = FlowState::new(collision_id.as_str(), flow_type.as_str(), [], "node-a");
    assert!(store.create(collision.clone()).await?);
    let alien_id = format!("{prefix}-alien");
    dialect
        .set_flow_identity(collision_id.as_str(), alien_id.as_str())
        .await?;
    assert_eq!(
        expect_failure(store.create(collision).await, "colliding flow create").code(),
        ErrorCode::Internal
    );
    dialect
        .set_flow_identity(alien_id.as_str(), collision_id.as_str())
        .await?;

    let mismatched_id = format!("{prefix}-mismatched");
    let mismatched = FlowState::new(mismatched_id.as_str(), flow_type.as_str(), [], "node-a");
    assert!(store.create(mismatched).await?);
    dialect
        .set_flow_payload(mismatched_id.as_str(), &original_payload)
        .await?;
    assert_eq!(
        expect_failure(
            store.get(mismatched_id.as_str()).await,
            "mismatched flow read"
        )
        .code(),
        ErrorCode::Internal
    );

    assert!(store.create_batch(Vec::new()).await?.is_empty());
    let batch_a = FlowState::new(
        format!("{prefix}-batch-a").as_str(),
        flow_type.as_str(),
        [],
        "node-a",
    );
    let batch_b = FlowState::new(
        format!("{prefix}-batch-b").as_str(),
        flow_type.as_str(),
        [],
        "node-a",
    );
    assert_eq!(
        store.create_batch(vec![batch_a.clone(), batch_b]).await?,
        vec![true, true]
    );
    let batch_c_id = format!("{prefix}-batch-c");
    let batch_c = FlowState::new(batch_c_id.as_str(), flow_type.as_str(), [], "node-a");
    assert_eq!(
        store.create_batch(vec![batch_a, batch_c.clone()]).await?,
        vec![false, true]
    );
    let batch_alien = format!("{prefix}-batch-alien");
    dialect
        .set_flow_identity(batch_c_id.as_str(), batch_alien.as_str())
        .await?;
    assert_eq!(
        expect_failure(
            store.create_batch(vec![batch_c]).await,
            "colliding flow batch"
        )
        .code(),
        ErrorCode::Internal
    );
    dialect
        .set_flow_identity(batch_alien.as_str(), batch_c_id.as_str())
        .await?;

    let claim_type = format!("{prefix}-claim");
    let stale = FlowState::new(
        format!("{prefix}-stale").as_str(),
        claim_type.as_str(),
        [],
        "node-a",
    )
    .heartbeated_at(SystemTime::UNIX_EPOCH);
    assert!(store.create(stale).await?);
    let claimed = store
        .try_claim(claim_type.as_str(), "node-b", Duration::from_secs(1))
        .await?
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "stale flow was not claimed"))?;
    assert_eq!(claimed.owner(), Some("node-b"));
    assert!(
        store
            .try_claim(claim_type.as_str(), "node-c", Duration::from_secs(1))
            .await?
            .is_none()
    );

    let race_type = format!("{prefix}-claim-race");
    let race = FlowState::new(
        format!("{prefix}-claim-race-row").as_str(),
        race_type.as_str(),
        [],
        "node-a",
    )
    .heartbeated_at(SystemTime::UNIX_EPOCH);
    assert!(store.create(race).await?);
    let (first, second) = tokio::join!(
        store.try_claim(race_type.as_str(), "node-b", Duration::from_secs(1)),
        store.try_claim(race_type.as_str(), "node-c", Duration::from_secs(1)),
    );
    let first = first?;
    let second = second?;
    assert_eq!(
        usize::from(first.is_some()) + usize::from(second.is_some()),
        1,
        "exactly one concurrent worker may claim a stale flow"
    );

    let skip_type = format!("{prefix}-skip");
    let fresh_id = format!("{prefix}-fresh");
    let fresh = FlowState::new(fresh_id.as_str(), skip_type.as_str(), [], "node-a");
    assert!(store.create(fresh).await?);
    dialect.set_flow_heartbeat_ms(fresh_id.as_str(), 0).await?;
    assert!(
        store
            .try_claim(skip_type.as_str(), "node-b", Duration::from_secs(3_600))
            .await?
            .is_none(),
        "a flow whose payload heartbeat is fresh must be skipped even when its column looks stale"
    );

    let corrupt_type = format!("{prefix}-corrupt");
    let corrupt_id = format!("{prefix}-corrupt-row");
    let corrupt = FlowState::new(corrupt_id.as_str(), corrupt_type.as_str(), [], "node-a")
        .heartbeated_at(SystemTime::UNIX_EPOCH);
    assert!(store.create(corrupt).await?);
    dialect
        .set_flow_payload(corrupt_id.as_str(), &original_payload)
        .await?;
    assert_eq!(
        expect_failure(
            store
                .try_claim(corrupt_type.as_str(), "node-b", Duration::from_secs(1))
                .await,
            "mismatched flow claim",
        )
        .code(),
        ErrorCode::Internal
    );

    let heartbeat_id = format!("{prefix}-heartbeat");
    let heartbeated = FlowState::new(heartbeat_id.as_str(), flow_type.as_str(), [], "node-a");
    assert!(store.create(heartbeated).await?);
    assert!(!store.heartbeat(heartbeat_id.as_str(), "node-b", 0).await?);
    assert!(store.heartbeat(heartbeat_id.as_str(), "node-a", 0).await?);

    let failure = || CatgaError::new(ErrorCode::Internal, "forced terminal failure");
    let statuses = [
        FlowState::new(
            format!("{prefix}-status-compensating").as_str(),
            flow_type.as_str(),
            [],
            "node-a",
        )
        .compensating(),
        FlowState::new(
            format!("{prefix}-status-suspended").as_str(),
            flow_type.as_str(),
            [],
            "node-a",
        )
        .suspended(),
        FlowState::new(
            format!("{prefix}-status-done").as_str(),
            flow_type.as_str(),
            [],
            "node-a",
        )
        .done(1),
        FlowState::new(
            format!("{prefix}-status-failed").as_str(),
            flow_type.as_str(),
            [],
            "node-a",
        )
        .failed(failure()),
        FlowState::new(
            format!("{prefix}-status-cancelled").as_str(),
            flow_type.as_str(),
            [],
            "node-a",
        )
        .cancelled(),
    ];
    for state in statuses {
        assert!(store.create(state.clone()).await?);
        let stored = store
            .get(state.id())
            .await?
            .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "status variant was not stored"))?;
        assert_eq!(stored.status(), state.status());
    }
    Ok(())
}

/// Exercises continuation collision fencing, frame agreement, version compare-and-set
/// rejection, wait-correlation index agreement, and summary decoding failures.
pub async fn suspended_store_edges<D: EdgeDialect>(
    store: &SqlSuspendedFlowStore,
    dialect: &D,
    prefix: &str,
) -> CatgaResult<()> {
    let flow_type = format!("{prefix}-type");

    let missing_id = format!("{prefix}-missing");
    let missing = FlowContinuation::new(
        FlowState::new(missing_id.as_str(), flow_type.as_str(), [], "node-a"),
        "run",
    );
    let missing_next = missing
        .clone()
        .with_state(missing.state().clone().next_version()?);
    assert!(!store.update(0, missing_next.clone()).await?);
    assert!(!store.update(0, missing.clone()).await?);
    assert!(!store.claim(&missing, missing_next).await?);
    assert!(!store.claim(&missing, missing.clone()).await?);
    assert!(
        !store
            .record_wait_success(missing_id.as_str(), 0, "child-a", b"payload".to_vec())
            .await?
    );
    assert!(
        !store
            .record_wait_failure(
                missing_id.as_str(),
                0,
                "child-a",
                CatgaError::new(ErrorCode::Internal, "forced child failure"),
            )
            .await?
    );
    assert!(!store.heartbeat(missing_id.as_str(), "node-a", 0).await?);

    let duplicate_id = format!("{prefix}-duplicate");
    let duplicate = FlowContinuation::new(
        FlowState::new(duplicate_id.as_str(), flow_type.as_str(), [], "node-a"),
        "run",
    );
    assert!(store.create(duplicate.clone()).await?);
    assert!(!store.create(duplicate.clone()).await?);
    let duplicate_payload = dialect.continuation_payload(duplicate_id.as_str()).await?;
    let alien_id = format!("{prefix}-alien");
    dialect
        .set_continuation_identity(duplicate_id.as_str(), alien_id.as_str())
        .await?;
    assert_eq!(
        expect_failure(
            store.create(duplicate).await,
            "colliding continuation create"
        )
        .code(),
        ErrorCode::Internal
    );
    dialect
        .set_continuation_identity(alien_id.as_str(), duplicate_id.as_str())
        .await?;

    let mismatched_id = format!("{prefix}-mismatched");
    let mismatched = FlowContinuation::new(
        FlowState::new(mismatched_id.as_str(), flow_type.as_str(), [], "node-a"),
        "run",
    );
    assert!(store.create(mismatched).await?);
    dialect
        .set_continuation_payload(mismatched_id.as_str(), &duplicate_payload)
        .await?;
    assert_eq!(
        expect_failure(
            store.get(mismatched_id.as_str()).await,
            "mismatched continuation read"
        )
        .code(),
        ErrorCode::Internal
    );

    let target_id = format!("{prefix}-target");
    let target = FlowContinuation::new(
        FlowState::new(target_id.as_str(), flow_type.as_str(), [], "node-a"),
        "run",
    );
    assert!(store.create(target.clone()).await?);
    let target_next = target
        .clone()
        .with_state(target.state().clone().next_version()?);
    assert!(store.update(0, target_next.clone()).await?);
    assert!(!store.update(0, target_next).await?);
    assert!(
        !store
            .record_wait_success(target_id.as_str(), 0, "child-a", b"payload".to_vec())
            .await?,
        "a stale business version must reject a wait result"
    );
    assert!(
        !store
            .record_wait_success(target_id.as_str(), 1, "child-a", b"payload".to_vec())
            .await?,
        "a continuation without a wait must reject a success result"
    );
    assert!(
        !store
            .record_wait_failure(
                target_id.as_str(),
                0,
                "child-a",
                CatgaError::new(ErrorCode::Internal, "stale version"),
            )
            .await?
    );
    assert!(
        !store
            .record_wait_failure(
                target_id.as_str(),
                1,
                "child-a",
                CatgaError::new(ErrorCode::Internal, "no wait"),
            )
            .await?
    );

    let correlation_id = format!("{prefix}-correlation");
    let waiting_id = format!("{prefix}-waiting");
    let waiting = FlowContinuation::waiting(
        FlowState::new(waiting_id.as_str(), flow_type.as_str(), [], "node-a").suspended(),
        "resume",
        WaitCondition::new(
            correlation_id.as_str(),
            WaitPolicy::All,
            1,
            SystemTime::now(),
            Duration::from_secs(30),
        ),
    );
    assert!(store.create(waiting).await?);
    let bogus = format!("{prefix}-bogus");
    dialect
        .set_continuation_wait_correlation(
            waiting_id.as_str(),
            bogus.as_str(),
            &catga_core::hash::sha256_digest(bogus.as_bytes()),
        )
        .await?;
    assert_eq!(
        expect_failure(
            store.get_by_wait_correlation(bogus.as_str()).await,
            "mismatched wait correlation",
        )
        .code(),
        ErrorCode::Internal
    );

    let overflowing = FlowContinuation::waiting(
        FlowState::new(
            format!("{prefix}-overflow").as_str(),
            flow_type.as_str(),
            [],
            "node-a",
        )
        .suspended(),
        "resume",
        WaitCondition::new(
            format!("{prefix}-overflow/wait"),
            WaitPolicy::All,
            1,
            SystemTime::UNIX_EPOCH + Duration::from_millis(900_000_000_000_000),
            Duration::from_millis(9_600_000_000_000_000_000),
        ),
    );
    assert_eq!(
        expect_failure(store.create(overflowing).await, "overflowing wait deadline").code(),
        ErrorCode::Validation
    );

    let measurable_id = format!("{prefix}-measurable");
    let measurable = FlowContinuation::new(
        FlowState::new(measurable_id.as_str(), flow_type.as_str(), [], "node-a"),
        "run",
    );
    assert!(store.create(measurable).await?);
    dialect
        .set_continuation_updated_subsec_ns(measurable_id.as_str(), 2_000_000)
        .await?;
    assert_eq!(
        expect_failure(
            store.query(&FlowQuery::new(16, 16)?).await,
            "invalid update precision"
        )
        .code(),
        ErrorCode::Validation
    );
    dialect
        .set_continuation_updated_subsec_ns(measurable_id.as_str(), 0)
        .await?;
    dialect
        .set_continuation_status(measurable_id.as_str(), 99)
        .await?;
    assert_eq!(
        expect_failure(
            store.query(&FlowQuery::new(16, 16)?).await,
            "unknown status code"
        )
        .code(),
        ErrorCode::Internal
    );
    Ok(())
}

/// Exercises progress payload bounds, identity fencing, frame decoding failures,
/// and logical-version compare-and-set rejection.
pub async fn dsl_progress_edges<D: EdgeDialect>(
    store: &SqlDslStepProgressStore,
    dialect: &D,
    prefix: &str,
) -> CatgaResult<()> {
    let oversized_id = format!("{prefix}-oversized");
    let oversized =
        DslStepProgress::new(oversized_id.as_str(), 0, vec![0_u8; OVERSIZE_PAYLOAD_BYTES]);
    assert_eq!(
        expect_failure(
            store.create(oversized.clone()).await,
            "oversized progress create"
        )
        .code(),
        ErrorCode::Validation
    );
    assert_eq!(
        expect_failure(
            store
                .update(
                    0,
                    oversized.next_version(vec![0_u8; OVERSIZE_PAYLOAD_BYTES])?
                )
                .await,
            "oversized progress update",
        )
        .code(),
        ErrorCode::Validation
    );

    let duplicate_id = format!("{prefix}-duplicate");
    let duplicate = DslStepProgress::new(duplicate_id.as_str(), 3, b"initial".as_slice());
    assert!(store.create(duplicate.clone()).await?);
    assert!(!store.create(duplicate.clone()).await?);
    let alien_id = format!("{prefix}-alien");
    dialect
        .set_progress_identity(duplicate_id.as_str(), 3, alien_id.as_str())
        .await?;
    assert_eq!(
        expect_failure(store.create(duplicate).await, "colliding progress create").code(),
        ErrorCode::Internal
    );
    dialect
        .set_progress_identity(alien_id.as_str(), 3, duplicate_id.as_str())
        .await?;

    let target_id = format!("{prefix}-target");
    let target = DslStepProgress::new(target_id.as_str(), 1, b"v0".as_slice());
    assert!(!store.update(0, target.clone()).await?);
    assert!(
        !store
            .update(0, target.clone().next_version(b"v1".as_slice())?)
            .await?
    );
    assert!(store.create(target.clone()).await?);
    let target_next = target.next_version(b"v1".as_slice())?;
    assert!(store.update(0, target_next.clone()).await?);
    assert!(!store.update(0, target_next).await?);
    assert!(store.delete(target_id.as_str(), 1).await?);

    let other_id = format!("{prefix}-other");
    let other = DslStepProgress::new(other_id.as_str(), 9, b"other".as_slice());
    assert!(store.create(other).await?);
    let foreign = dialect.progress_payload(other_id.as_str(), 9).await?;
    let victim_id = format!("{prefix}-victim");
    let victim = DslStepProgress::new(victim_id.as_str(), 5, b"victim".as_slice());
    assert!(store.create(victim).await?);
    dialect
        .set_progress_payload(victim_id.as_str(), 5, &foreign)
        .await?;
    assert_eq!(
        expect_failure(
            store.get(victim_id.as_str(), 5).await,
            "mismatched progress read"
        )
        .code(),
        ErrorCode::Internal
    );

    let corrupt_id = format!("{prefix}-corrupt");
    for (step_index, frame) in [
        (20_u32, Vec::new()),
        (21_u32, vec![0xEE_u8]),
        (22_u32, vec![2_u8, 0xFF, 0xFF, 0xFF, 0xFF]),
    ] {
        let row = DslStepProgress::new(corrupt_id.as_str(), step_index, b"ok".as_slice());
        assert!(store.create(row).await?);
        dialect
            .set_progress_payload(corrupt_id.as_str(), step_index, &frame)
            .await?;
        assert_eq!(
            expect_failure(
                store.get(corrupt_id.as_str(), step_index).await,
                "corrupt progress frame",
            )
            .code(),
            ErrorCode::Internal
        );
    }

    let big_id = format!("{prefix}-big");
    let big = DslStepProgress::new(big_id.as_str(), 30, vec![0_u8; OVERSIZE_PAYLOAD_BYTES]);
    let mut big_frame = vec![2_u8];
    big_frame.extend_from_slice(
        &MemoryPackSerializer::serialize(&big)
            .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?,
    );
    let slot = DslStepProgress::new(big_id.as_str(), 30, b"slot".as_slice());
    assert!(store.create(slot).await?);
    dialect
        .set_progress_payload(big_id.as_str(), 30, &big_frame)
        .await?;
    assert_eq!(
        expect_failure(
            store.get(big_id.as_str(), 30).await,
            "oversized stored progress"
        )
        .code(),
        ErrorCode::Internal
    );
    Ok(())
}

/// Exercises snapshot identity fencing, frame/column agreement, payload bounds, and
/// optimistic-concurrency rejection.
pub async fn state_machine_edges<D: EdgeDialect>(
    store: &SqlStateMachineStore<EdgeState>,
    dialect: &D,
    prefix: &str,
) -> CatgaResult<()> {
    let state = |paid: bool, quantity: u32| EdgeState { paid, quantity };

    let missing_id = format!("{prefix}-missing");
    let missing = StateMachineSnapshot::new(missing_id.as_str(), state(false, 0));
    assert!(!store.update(0, missing.clone()).await?);
    assert!(
        !store
            .update(0, missing.next_version(state(true, 1))?)
            .await?
    );

    let duplicate_id = format!("{prefix}-duplicate");
    let duplicate = StateMachineSnapshot::new(duplicate_id.as_str(), state(false, 1));
    assert!(store.create(duplicate.clone()).await?);
    assert!(!store.create(duplicate.clone()).await?);
    let alien_id = format!("{prefix}-alien");
    dialect
        .set_snapshot_identity(duplicate_id.as_str(), alien_id.as_str())
        .await?;
    assert_eq!(
        expect_failure(store.create(duplicate).await, "colliding snapshot create").code(),
        ErrorCode::Internal
    );
    dialect
        .set_snapshot_identity(alien_id.as_str(), duplicate_id.as_str())
        .await?;

    let target_id = format!("{prefix}-target");
    let target = StateMachineSnapshot::new(target_id.as_str(), state(false, 2));
    assert!(store.create(target.clone()).await?);
    let target_next = target.clone().next_version(state(true, 2))?;
    assert!(store.update(0, target_next).await?);
    assert!(
        !store
            .update(0, target.next_version(state(true, 3))?)
            .await?
    );

    let mismatched_id = format!("{prefix}-mismatched");
    let mismatched = StateMachineSnapshot::new(mismatched_id.as_str(), state(false, 4));
    assert!(store.create(mismatched).await?);
    dialect
        .set_snapshot_version(mismatched_id.as_str(), 7)
        .await?;
    assert_eq!(
        expect_failure(
            store.get(mismatched_id.as_str()).await,
            "mismatched snapshot read"
        )
        .code(),
        ErrorCode::Internal
    );

    let oversized_id = format!("{prefix}-oversized");
    let oversized = StateMachineSnapshot::new(oversized_id.as_str(), state(false, 5));
    assert!(store.create(oversized).await?);
    dialect
        .set_snapshot_payload(
            oversized_id.as_str(),
            &vec![0_u8; OVERSIZE_PAYLOAD_BYTES + 9],
        )
        .await?;
    assert_eq!(
        expect_failure(
            store.get(oversized_id.as_str()).await,
            "oversized stored snapshot"
        )
        .code(),
        ErrorCode::Internal
    );
    Ok(())
}

/// Exercises the encode-side durable payload bound for snapshot states.
pub async fn state_machine_oversize_encode(
    store: &SqlStateMachineStore<BigState>,
    prefix: &str,
) -> CatgaResult<()> {
    let big = StateMachineSnapshot::new(
        format!("{prefix}-big").as_str(),
        BigState {
            data: vec![0_u8; OVERSIZE_PAYLOAD_BYTES],
        },
    );
    assert_eq!(
        expect_failure(store.create(big).await, "oversized snapshot encode").code(),
        ErrorCode::Validation
    );
    Ok(())
}

/// Exercises lease validation, claim bounds, pre-epoch due-time round-trips, and
/// schedule-target collision fencing.
pub async fn scheduler_edges<D: EdgeDialect>(
    scheduler: &SqlFlowScheduler,
    dialect: &D,
    prefix: &str,
) -> CatgaResult<()> {
    let now = SystemTime::now();
    assert!(
        scheduler
            .claim_due("worker", now, Duration::from_secs(1), 0)
            .await?
            .is_empty()
    );
    assert_eq!(
        expect_failure(
            scheduler.claim_due("worker", now, Duration::ZERO, 1).await,
            "zero lease claim",
        )
        .code(),
        ErrorCode::Validation
    );
    assert_eq!(
        expect_failure(
            scheduler.claim_due("worker", now, Duration::MAX, 1).await,
            "overflowing lease claim",
        )
        .code(),
        ErrorCode::Validation
    );
    assert_eq!(
        expect_failure(
            scheduler
                .claim_due("worker", now, Duration::from_secs(1), usize::MAX)
                .await,
            "unbounded claim",
        )
        .code(),
        ErrorCode::Validation
    );

    let exact = SystemTime::UNIX_EPOCH - Duration::from_secs(5);
    let fractional = SystemTime::UNIX_EPOCH - Duration::new(5, 500);
    let exact_id = scheduler
        .schedule_resume(format!("{prefix}-exact").as_str(), "resume", exact)
        .await?;
    let fractional_id = scheduler
        .schedule_resume(
            format!("{prefix}-fractional").as_str(),
            "resume",
            fractional,
        )
        .await?;
    let claimed = scheduler
        .claim_due("worker", SystemTime::UNIX_EPOCH, Duration::from_secs(30), 8)
        .await?;
    assert_eq!(claimed.len(), 2);
    for resume in &claimed {
        let expected = if resume.schedule_id() == exact_id.as_ref() {
            exact
        } else if resume.schedule_id() == fractional_id.as_ref() {
            fractional
        } else {
            return Err(CatgaError::new(
                ErrorCode::Internal,
                "scheduler returned an unknown schedule",
            ));
        };
        assert_eq!(resume.due_at(), expected);
        assert!(scheduler.ack_due("worker", resume.schedule_id()).await?);
    }

    let target_flow = format!("{prefix}-target");
    let target_id = scheduler
        .schedule_resume(
            target_flow.as_str(),
            "resume",
            now + Duration::from_secs(60),
        )
        .await?;
    dialect
        .set_schedule_identity(target_id.as_ref(), format!("{prefix}-alien").as_str())
        .await?;
    assert_eq!(
        expect_failure(
            scheduler
                .schedule_resume(
                    target_flow.as_str(),
                    "resume",
                    now + Duration::from_secs(120)
                )
                .await,
            "colliding schedule target",
        )
        .code(),
        ErrorCode::Internal
    );
    Ok(())
}

/// Exercises timeout-poll instant arithmetic at both ends of the supported range.
pub async fn timeout_edges(store: &SqlSuspendedFlowStore) -> CatgaResult<()> {
    if let Some(far_future) =
        SystemTime::UNIX_EPOCH.checked_add(Duration::from_millis(i64::MAX as u64 - 10))
    {
        let overflow = TimedOutFlowPoll::new(far_future, 1, 1)?;
        assert_eq!(
            expect_failure(
                store.poll_timed_out(&overflow).await,
                "overflowing poll lease"
            )
            .code(),
            ErrorCode::Validation
        );
    }
    let pre_epoch = TimedOutFlowPoll::new(SystemTime::UNIX_EPOCH - Duration::from_secs(5), 1, 1)?;
    assert!(store.poll_timed_out(&pre_epoch).await?.is_empty());
    Ok(())
}

/// Number of contention rounds each compare-and-set loop gets to exhaust its retries.
///
/// Every contender bumps the row's physical revision in a tight loop, so a bounded retry loop
/// loses its guard on nearly every iteration. Four hundred rounds with three concurrent bumpers
/// make an unobserved exhaustion vanishingly unlikely even on a heavily loaded machine, while
/// keeping the failure mode loud rather than timing-dependent.
const CAS_CONTENTION_ROUNDS: usize = 400;

/// Number of concurrent revision bumpers pressuring one compare-and-set loop.
const CAS_CONTENTION_BUMPERS: usize = 3;

/// Drives `op` against a row whose physical revision `bump` keeps moving until the bounded
/// compare-and-set loop inside the store exhausts and surfaces a transient error.
async fn run_until_cas_exhausted<Bump, BumpFut, Op, OpFut>(
    bump: Bump,
    mut op: Op,
) -> CatgaResult<()>
where
    Bump: Fn(Arc<AtomicBool>) -> BumpFut,
    BumpFut: Future<Output = ()> + Send + 'static,
    Op: FnMut() -> OpFut,
    OpFut: Future<Output = CatgaResult<bool>>,
{
    for _ in 0..CAS_CONTENTION_ROUNDS {
        let stop = Arc::new(AtomicBool::new(false));
        let bumpers: Vec<_> = (0..CAS_CONTENTION_BUMPERS)
            .map(|_| tokio::spawn(bump(Arc::clone(&stop))))
            .collect();
        let result = op().await;
        stop.store(true, Ordering::SeqCst);
        for bumper in bumpers {
            bumper
                .await
                .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
        }
        match result {
            Ok(_) => {}
            Err(error) if error.code() == ErrorCode::Transient => return Ok(()),
            // A store that cannot even take the write lock inside its busy timeout reports a
            // retryable availability failure (SQLITE_BUSY, server lock-wait or deadlock victim).
            // That is contention noise rather than bounded-retry exhaustion, so the round
            // retries; only the Transient guard exhaustion below proves the bounded loop.
            Err(error) if error.code() == ErrorCode::Unavailable && error.is_retryable() => {}
            Err(error) => return Err(error),
        }
    }
    Err(CatgaError::new(
        ErrorCode::Internal,
        "bounded compare-and-set retries never exhausted under contention",
    ))
}

/// Forces the flow heartbeat retry loop to exhaust, then proves the row still accepts work.
pub async fn flow_heartbeat_cas_exhaustion<D: EdgeDialect + Send + Sync + 'static>(
    store: &SqlFlowStore,
    dialect: &Arc<D>,
    prefix: &str,
) -> CatgaResult<()> {
    let flow_id = format!("{prefix}-heartbeat-cas");
    let flow_type = format!("{prefix}-cas-type");
    store
        .create(FlowState::new(
            flow_id.as_str(),
            flow_type.as_str(),
            [],
            "node-a",
        ))
        .await?;
    let bumper_dialect = Arc::clone(dialect);
    let bumper_target = flow_id.clone();
    let bump = move |stop: Arc<AtomicBool>| {
        let dialect = Arc::clone(&bumper_dialect);
        let flow_id = bumper_target.clone();
        async move {
            while !stop.load(Ordering::SeqCst) {
                let _ = dialect.bump_flow_revision(flow_id.as_str()).await;
            }
        }
    };
    run_until_cas_exhausted(bump, || store.heartbeat(flow_id.as_str(), "node-a", 0)).await?;
    assert!(store.heartbeat(flow_id.as_str(), "node-a", 0).await?);
    Ok(())
}

/// Forces every continuation mutation loop to exhaust its bounded retries.
pub async fn suspended_cas_exhaustion<D: EdgeDialect + Send + Sync + 'static>(
    store: &SqlSuspendedFlowStore,
    dialect: &Arc<D>,
    prefix: &str,
) -> CatgaResult<()> {
    let flow_type = format!("{prefix}-cas-type");

    let heartbeat_id = format!("{prefix}-heartbeat-cas");
    store
        .create(FlowContinuation::new(
            FlowState::new(heartbeat_id.as_str(), flow_type.as_str(), [], "node-a"),
            "run",
        ))
        .await?;
    let bumper_dialect = Arc::clone(dialect);
    let bumper_target = heartbeat_id.clone();
    let bump = move |stop: Arc<AtomicBool>| {
        let dialect = Arc::clone(&bumper_dialect);
        let flow_id = bumper_target.clone();
        async move {
            while !stop.load(Ordering::SeqCst) {
                let _ = dialect.bump_continuation_revision(flow_id.as_str()).await;
            }
        }
    };
    run_until_cas_exhausted(bump, || store.heartbeat(heartbeat_id.as_str(), "node-a", 0)).await?;

    let update_id = format!("{prefix}-update-cas");
    let update_base = FlowContinuation::new(
        FlowState::new(update_id.as_str(), flow_type.as_str(), [], "node-a"),
        "run",
    );
    store.create(update_base.clone()).await?;
    let current = Rc::new(RefCell::new(update_base));
    let bumper_dialect = Arc::clone(dialect);
    let bumper_target = update_id.clone();
    let bump = move |stop: Arc<AtomicBool>| {
        let dialect = Arc::clone(&bumper_dialect);
        let flow_id = bumper_target.clone();
        async move {
            while !stop.load(Ordering::SeqCst) {
                let _ = dialect.bump_continuation_revision(flow_id.as_str()).await;
            }
        }
    };
    run_until_cas_exhausted(bump, || {
        let current = Rc::clone(&current);
        let base = current.borrow().clone();
        async move {
            let next_state = base.state().clone().next_version()?;
            let candidate = base.with_state(next_state);
            let expected = candidate.state().version() - 1;
            let updated = store.update(expected, candidate.clone()).await?;
            if updated {
                *current.borrow_mut() = candidate;
            }
            Ok(updated)
        }
    })
    .await?;

    let waiting_id = format!("{prefix}-wait-cas");
    let waiting = FlowContinuation::waiting(
        FlowState::new(waiting_id.as_str(), flow_type.as_str(), [], "node-a").suspended(),
        "resume",
        WaitCondition::new(
            format!("{prefix}-wait-cas/correlation"),
            WaitPolicy::All,
            512,
            SystemTime::now(),
            Duration::from_secs(30),
        ),
    );
    store.create(waiting).await?;
    let bumper_dialect = Arc::clone(dialect);
    let bumper_target = waiting_id.clone();
    let bump = move |stop: Arc<AtomicBool>| {
        let dialect = Arc::clone(&bumper_dialect);
        let flow_id = bumper_target.clone();
        async move {
            while !stop.load(Ordering::SeqCst) {
                let _ = dialect.bump_continuation_revision(flow_id.as_str()).await;
            }
        }
    };
    let round = Rc::new(Cell::new(0_u32));
    run_until_cas_exhausted(&bump, || {
        let round = Rc::clone(&round);
        let waiting_id = waiting_id.clone();
        async move {
            round.set(round.get() + 1);
            store
                .record_wait_success(
                    waiting_id.as_str(),
                    0,
                    format!("success-child-{}", round.get()).as_str(),
                    b"payload".to_vec(),
                )
                .await
        }
    })
    .await?;
    run_until_cas_exhausted(bump, || {
        let round = Rc::clone(&round);
        let waiting_id = waiting_id.clone();
        async move {
            round.set(round.get() + 1);
            store
                .record_wait_failure(
                    waiting_id.as_str(),
                    0,
                    format!("failure-child-{}", round.get()).as_str(),
                    CatgaError::new(ErrorCode::Internal, "contended child failure"),
                )
                .await
        }
    })
    .await?;

    let delete_id = format!("{prefix}-delete-cas");
    let fresh = FlowContinuation::new(
        FlowState::new(delete_id.as_str(), flow_type.as_str(), [], "node-a"),
        "run",
    );
    store.create(fresh.clone()).await?;
    let bumper_dialect = Arc::clone(dialect);
    let bumper_target = delete_id.clone();
    let bump = move |stop: Arc<AtomicBool>| {
        let dialect = Arc::clone(&bumper_dialect);
        let flow_id = bumper_target.clone();
        async move {
            while !stop.load(Ordering::SeqCst) {
                let _ = dialect.bump_continuation_revision(flow_id.as_str()).await;
            }
        }
    };
    run_until_cas_exhausted(bump, || {
        let fresh = fresh.clone();
        let delete_id = delete_id.clone();
        async move {
            store.create(fresh).await?;
            store.delete(delete_id.as_str(), 0).await
        }
    })
    .await?;
    Ok(())
}

/// Forces the DSL progress update and delete retry loops to exhaust.
pub async fn dsl_progress_cas_exhaustion<D: EdgeDialect + Send + Sync + 'static>(
    store: &SqlDslStepProgressStore,
    dialect: &Arc<D>,
    prefix: &str,
) -> CatgaResult<()> {
    let update_id = format!("{prefix}-update-cas");
    let update_base = DslStepProgress::new(update_id.as_str(), 4, b"initial".as_slice());
    store.create(update_base.clone()).await?;
    let current = Rc::new(RefCell::new(update_base));
    let bumper_dialect = Arc::clone(dialect);
    let bumper_target = update_id.clone();
    let bump = move |stop: Arc<AtomicBool>| {
        let dialect = Arc::clone(&bumper_dialect);
        let flow_id = bumper_target.clone();
        async move {
            while !stop.load(Ordering::SeqCst) {
                let _ = dialect.bump_progress_revision(flow_id.as_str(), 4).await;
            }
        }
    };
    run_until_cas_exhausted(bump, || {
        let current = Rc::clone(&current);
        let base = current.borrow().clone();
        async move {
            let candidate = base.next_version(b"candidate".as_slice())?;
            let expected = candidate.version() - 1;
            let updated = store.update(expected, candidate.clone()).await?;
            if updated {
                *current.borrow_mut() = candidate;
            }
            Ok(updated)
        }
    })
    .await?;

    let delete_id = format!("{prefix}-delete-cas");
    let fresh = DslStepProgress::new(delete_id.as_str(), 7, b"fresh".as_slice());
    store.create(fresh.clone()).await?;
    let bumper_dialect = Arc::clone(dialect);
    let bumper_target = delete_id.clone();
    let bump = move |stop: Arc<AtomicBool>| {
        let dialect = Arc::clone(&bumper_dialect);
        let flow_id = bumper_target.clone();
        async move {
            while !stop.load(Ordering::SeqCst) {
                let _ = dialect.bump_progress_revision(flow_id.as_str(), 7).await;
            }
        }
    };
    run_until_cas_exhausted(bump, || {
        let fresh = fresh.clone();
        let delete_id = delete_id.clone();
        async move {
            store.create(fresh).await?;
            store.delete(delete_id.as_str(), 7).await
        }
    })
    .await?;
    Ok(())
}

/// Forces the snapshot update retry loop to exhaust.
pub async fn state_machine_cas_exhaustion<D: EdgeDialect + Send + Sync + 'static>(
    store: &SqlStateMachineStore<EdgeState>,
    dialect: &Arc<D>,
    prefix: &str,
) -> CatgaResult<()> {
    let instance_id = format!("{prefix}-update-cas");
    let initial = StateMachineSnapshot::new(
        instance_id.as_str(),
        EdgeState {
            paid: false,
            quantity: 0,
        },
    );
    store.create(initial.clone()).await?;
    let current = Rc::new(RefCell::new(initial));
    let quantity = Rc::new(Cell::new(0_u32));
    let bumper_dialect = Arc::clone(dialect);
    let bumper_target = instance_id.clone();
    let bump = move |stop: Arc<AtomicBool>| {
        let dialect = Arc::clone(&bumper_dialect);
        let instance_id = bumper_target.clone();
        async move {
            while !stop.load(Ordering::SeqCst) {
                let _ = dialect.bump_snapshot_revision(instance_id.as_str()).await;
            }
        }
    };
    run_until_cas_exhausted(bump, || {
        let current = Rc::clone(&current);
        let quantity = Rc::clone(&quantity);
        let candidate = current.borrow().clone().next_version(EdgeState {
            paid: true,
            quantity: quantity.get(),
        });
        quantity.set(quantity.get() + 1);
        async move {
            let candidate = candidate?;
            let expected = candidate.version() - 1;
            let updated = store.update(expected, candidate.clone()).await?;
            if updated {
                *current.borrow_mut() = candidate;
            }
            Ok(updated)
        }
    })
    .await?;
    Ok(())
}
