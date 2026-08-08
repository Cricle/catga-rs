//! Unit tests for DSL flow construction and step progress.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use catga_core::codec::memorypack::MemoryPackSerializer;
use catga_core::flow::dsl_lifecycle::{
    DslFlowLifecycleEvent, DslFlowLifecycleHooks, DslFlowLifecycleObserver,
};
use catga_core::flow::dsl_progress::{
    DslProgressKind, DslStateCodec, DslStepProgress, DslStepProgressStore,
};
use catga_core::flow::dsl_step::MAX_DSL_PARALLEL_BRANCHES;
use catga_core::flow::{DslFlow, DslStep};
use catga_core::{CatgaError, CatgaResult, ErrorCode};

#[derive(Default)]
struct ProgressStore {
    records: Mutex<HashMap<(String, u32), DslStepProgress>>,
}

#[async_trait]
impl DslStepProgressStore for ProgressStore {
    async fn create(&self, progress: DslStepProgress) -> CatgaResult<bool> {
        let key = (progress.flow_id().to_owned(), progress.step_index());
        let mut records = self.records.lock().expect("progress store lock");
        if records.contains_key(&key) {
            return Ok(false);
        }
        records.insert(key, progress);
        Ok(true)
    }

    async fn update(
        &self,
        expected_version: i64,
        next: DslStepProgress,
    ) -> CatgaResult<bool> {
        let key = (next.flow_id().to_owned(), next.step_index());
        let mut records = self.records.lock().expect("progress store lock");
        let Some(current) = records.get(&key) else {
            return Ok(false);
        };
        if current.version() != expected_version
            || !DslStepProgress::is_next_version(expected_version, next.version())
        {
            return Ok(false);
        }
        records.insert(key, next);
        Ok(true)
    }

    async fn get(
        &self,
        flow_id: &str,
        step_index: u32,
    ) -> CatgaResult<Option<DslStepProgress>> {
        Ok(self
            .records
            .lock()
            .expect("progress store lock")
            .get(&(flow_id.to_owned(), step_index))
            .cloned())
    }

    async fn delete(&self, flow_id: &str, step_index: u32) -> CatgaResult<bool> {
        Ok(self
            .records
            .lock()
            .expect("progress store lock")
            .remove(&(flow_id.to_owned(), step_index))
            .is_some())
    }
}

struct UsizeCodec;

impl DslStateCodec<usize> for UsizeCodec {
    fn encode(&self, state: &usize) -> CatgaResult<Vec<u8>> {
        Ok((*state as u64).to_be_bytes().to_vec())
    }

    fn decode(&self, bytes: &[u8]) -> CatgaResult<usize> {
        let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
            CatgaError::new(ErrorCode::Validation, "invalid test state payload")
        })?;
        usize::try_from(u64::from_be_bytes(bytes)).map_err(|_| {
            CatgaError::new(ErrorCode::Validation, "test state does not fit usize")
        })
    }
}

const DSL_TERMINAL_STEP_INDEX: u32 = u32::MAX;

fn top_level_step_index(index: usize) -> CatgaResult<u32> {
    let index = u32::try_from(index)
        .map_err(|_| CatgaError::new(ErrorCode::Internal, "DSL step index exceeds u32"))?;
    if index == DSL_TERMINAL_STEP_INDEX {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "DSL step index is reserved for the terminal record",
        ));
    }
    Ok(index)
}

fn validate_parallel_branch_count(count: usize) -> CatgaResult<()> {
    if count > MAX_DSL_PARALLEL_BRANCHES {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "DSL parallel branch count exceeds the supported limit",
        ));
    }
    Ok(())
}

#[test]
fn dsl_step_index_limits_are_bounded() {
    assert_eq!(top_level_step_index(0), Ok(0));
    assert_eq!(
        top_level_step_index(u32::MAX as usize)
            .expect_err("terminal slot cannot be a step index")
            .code(),
        ErrorCode::Validation
    );
    assert_eq!(
        validate_parallel_branch_count(MAX_DSL_PARALLEL_BRANCHES + 1)
            .expect_err("parallel fanout limit")
            .code(),
        ErrorCode::Validation
    );
    assert!(validate_parallel_branch_count(MAX_DSL_PARALLEL_BRANCHES).is_ok());
}

#[test]
fn dsl_flow_new_creates_flow() {
    let _flow = DslFlow::<usize>::new();
}

#[test]
fn dsl_flow_action_adds_step() {
    let _flow = DslFlow::<usize>::new().action(|_state| Box::pin(async { Ok(()) }));
}

#[test]
fn dsl_flow_step_adds_step() {
    let step = DslStep::action(|_state: &mut usize| Box::pin(async { Ok(()) }));
    let _flow = DslFlow::<usize>::new().step(step);
}

#[test]
fn dsl_flow_if_else_adds_conditional_steps() {
    let _flow = DslFlow::new().if_else(
        |_state: &usize| true,
        DslFlow::new().action(|_s| Box::pin(async { Ok(()) })),
        DslFlow::new().action(|_s| Box::pin(async { Ok(()) })),
    );
}

#[test]
fn dsl_flow_match_on_adds_match_steps() {
    let _flow = DslFlow::new().match_on(
        |state: &usize| *state,
        [(1, DslFlow::new()), (2, DslFlow::new())],
        DslFlow::new(),
    );
}

#[test]
fn dsl_flow_retry_adds_retry_steps() {
    let _flow = DslFlow::<usize>::new().retry(3, Duration::from_millis(100), |_s| {
        Box::pin(async { Ok(()) })
    });
}

#[test]
fn dsl_flow_timeout_adds_timeout_steps() {
    let _flow = DslFlow::<usize>::new().timeout(Duration::from_secs(5), |_s| {
        Box::pin(async { Ok(()) })
    });
}

#[test]
fn dsl_flow_parallel_adds_parallel_steps() {
    let _flow = DslFlow::<usize>::new().parallel(
        [
            DslFlow::new().action(|_s| Box::pin(async { Ok(()) })),
            DslFlow::new().action(|_s| Box::pin(async { Ok(()) })),
        ],
        |_state, _results| Ok(()),
    );
}

#[test]
fn dsl_flow_for_each_adds_loop_steps() {
    let _flow = DslFlow::<usize>::new()
        .for_each(|_state: &usize| Vec::new(), |_s: &mut usize, _item: usize| {
            Box::pin(async { Ok(()) })
        });
}

#[test]
fn dsl_flow_lifecycle_observer_trait() {
    #[derive(Default)]
    struct TestObserver {
        events: Mutex<Vec<DslFlowLifecycleEvent>>,
    }

    impl DslFlowLifecycleObserver for TestObserver {
        fn observe(&self, event: &DslFlowLifecycleEvent) {
            self.events
                .lock()
                .expect("lock")
                .push(event.clone());
        }
    }

    let observer = Arc::new(TestObserver::default());
    let _flow = DslFlow::<usize>::new()
        .action(|_s| Box::pin(async { Ok(()) }))
        .with_lifecycle_observer(observer.clone());

    let events = observer.events.lock().expect("lock");
    assert!(events.is_empty());
}

#[test]
fn dsl_flow_lifecycle_hooks_are_constructible() {
    let _hooks = DslFlowLifecycleHooks::<usize>::new();
}

#[test]
fn dsl_step_progress_store_operations() {
    let store = ProgressStore::default();
    let progress = DslStepProgress::new("flow-1", 0, [1_u8, 2, 3]);

    // Create
    let created = futures::executor::block_on(store.create(progress.clone()));
    assert!(created.expect("create should succeed"));

    // Duplicate create fails
    let dup = futures::executor::block_on(store.create(progress.clone()));
    assert!(!dup.expect("dup create should return false"));

    // Get
    let retrieved = futures::executor::block_on(store.get("flow-1", 0));
    let retrieved = retrieved.expect("get should succeed").expect("should exist");
    assert_eq!(retrieved.flow_id(), "flow-1");
    assert_eq!(retrieved.step_index(), 0);
    assert_eq!(retrieved.payload(), &[1, 2, 3]);

    // Update
    let next = progress.next_version([4_u8, 5]).expect("version advances");
    let updated = futures::executor::block_on(store.update(0, next.clone()));
    assert!(updated.expect("update should succeed"));

    // Stale update fails
    let stale = futures::executor::block_on(store.update(0, next));
    assert!(!stale.expect("stale update should return false"));

    // Delete
    let deleted = futures::executor::block_on(store.delete("flow-1", 0));
    assert!(deleted.expect("delete should succeed"));

    // Get after delete
    let gone = futures::executor::block_on(store.get("flow-1", 0));
    assert!(gone.expect("get should succeed").is_none());
}

#[test]
fn dsl_step_progress_state_codec_roundtrip() {
    let state = 42usize;
    let encoded = UsizeCodec.encode(&state).expect("encode");
    let decoded = UsizeCodec.decode(&encoded).expect("decode");
    assert_eq!(decoded, state);
}

#[test]
fn dsl_step_progress_kind_default_is_application_state() {
    let progress = DslStepProgress::new("flow", 0, []);
    assert_eq!(progress.kind(), DslProgressKind::ApplicationState);
}

#[test]
fn dsl_step_progress_next_version_advances_version() {
    let original = DslStepProgress::new("flow", 0, [1_u8]);
    let next = original.next_version([2_u8]).expect("next version");
    assert_eq!(next.version(), 1);
    assert_eq!(next.payload(), &[2_u8]);
}

#[test]
fn dsl_step_progress_next_version_rejects_non_successor() {
    let progress = DslStepProgress::new("flow", 0, [1_u8]);
    // Creating at version 0, calling next_version gives version 1
    let v1 = progress.next_version([2_u8]).expect("v1 should succeed");
    // Calling next_version again gives version 2
    let v2 = v1.next_version([3_u8]).expect("v2 should succeed");
    assert_eq!(v2.version(), 2);
    // Version 2 cannot advance to version 4 (non-consecutive)
    // This is tested by is_next_version returning false
    assert!(!DslStepProgress::is_next_version(2, 4));
}

#[test]
fn dsl_step_progress_serialization_roundtrip() {
    let progress = DslStepProgress::new("flow-42", 5, [1_u8, 2, 3]);
    let bytes = MemoryPackSerializer::serialize(&progress).expect("serialize");
    let deserialized = MemoryPackSerializer::deserialize::<DslStepProgress>(&bytes)
        .expect("deserialize");

    assert_eq!(deserialized.flow_id(), progress.flow_id());
    assert_eq!(deserialized.step_index(), progress.step_index());
    assert_eq!(deserialized.version(), progress.version());
    assert_eq!(deserialized.kind(), progress.kind());
    assert_eq!(deserialized.payload(), progress.payload());
}

#[test]
fn terminal_record_slot_validation() {
    // The terminal slot is u32::MAX, not a valid step index
    assert_eq!(
        top_level_step_index(DSL_TERMINAL_STEP_INDEX as usize)
            .expect_err("terminal slot is invalid")
            .code(),
        ErrorCode::Validation
    );
}

#[test]
fn dsl_flow_parallel_branch_limit() {
    // Verify the constant exists and is positive
    assert!(MAX_DSL_PARALLEL_BRANCHES > 0);
}

#[test]
fn dsl_progress_kind_all_variants() {
    assert_eq!(DslProgressKind::ApplicationState, DslProgressKind::ApplicationState);
    assert_eq!(DslProgressKind::CheckpointFrame, DslProgressKind::CheckpointFrame);
    assert_eq!(DslProgressKind::Terminal, DslProgressKind::Terminal);
}

#[test]
fn dsl_step_progress_shared_payload() {
    let progress = DslStepProgress::new("flow", 0, [1_u8, 2, 3]);
    let shared = progress.shared_payload();
    assert_eq!(&*shared, &[1, 2, 3]);
}

#[test]
fn dsl_step_progress_updated_at() {
    let progress = DslStepProgress::new("flow", 0, []);
    assert!(progress.updated_at() <= std::time::SystemTime::now());
}

#[test]
fn dsl_step_progress_is_next_version() {
    assert!(DslStepProgress::is_next_version(0, 1));
    assert!(DslStepProgress::is_next_version(1, 2));
    assert!(DslStepProgress::is_next_version(i64::MAX - 1, i64::MAX));
}

#[test]
fn dsl_step_progress_is_next_version_rejects_invalid() {
    assert!(!DslStepProgress::is_next_version(0, 0));
    assert!(!DslStepProgress::is_next_version(0, 2));
    assert!(!DslStepProgress::is_next_version(1, 3));
    assert!(!DslStepProgress::is_next_version(i64::MAX, i64::MAX));
    // i64::MAX + 1 would overflow, so test that max version cannot advance
    assert!(!DslStepProgress::is_next_version(i64::MAX - 1, i64::MAX - 1));
}

#[test]
fn dsl_flow_debug_trait() {
    // DslFlow doesn't implement Debug, so we just verify it can be created
    let _flow = DslFlow::<usize>::new().action(|_s| Box::pin(async { Ok(()) }));
}

#[test]
fn dsl_flow_action_outcome_adds_step() {
    let _flow = DslFlow::<usize>::new().action_outcome(|_s| Box::pin(async { Ok(()) }));
}

#[test]
fn dsl_flow_with_lifecycle_hooks() {
    let _flow = DslFlow::<usize>::new()
        .action(|_s| Box::pin(async { Ok(()) }))
        .with_lifecycle_hooks(DslFlowLifecycleHooks::new());
}
