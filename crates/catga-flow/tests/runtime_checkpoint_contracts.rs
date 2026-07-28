//! External contracts for durable runtime recovery and checkpoint rejection.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{
    DslFlow, DslStateCodec, DslStepProgress, DslStepProgressStore, FlowContinuation,
    FlowDefinition, FlowQuery, FlowRuntime, FlowState, FlowStatus, FlowStepOutcome, FlowSummary,
    MemoryFlowScheduler, SuspendedFlowStore, WaitCondition, WaitPolicy,
};
use tokio::sync::Mutex;

#[derive(Default)]
struct ContinuationStore {
    records: Mutex<HashMap<Box<str>, FlowContinuation>>,
}

#[async_trait]
impl SuspendedFlowStore for ContinuationStore {
    async fn create(&self, continuation: FlowContinuation) -> CatgaResult<bool> {
        let mut records = self.records.lock().await;
        let id: Box<str> = continuation.state().id().into();
        if records.contains_key(id.as_ref()) {
            return Ok(false);
        }
        records.insert(id, continuation);
        Ok(true)
    }

    async fn get(&self, flow_id: &str) -> CatgaResult<Option<FlowContinuation>> {
        Ok(self.records.lock().await.get(flow_id).cloned())
    }

    async fn get_by_wait_correlation(
        &self,
        correlation_id: &str,
    ) -> CatgaResult<Option<FlowContinuation>> {
        Ok(self
            .records
            .lock()
            .await
            .values()
            .find(|continuation| {
                continuation
                    .wait()
                    .is_some_and(|wait| wait.correlation_id() == correlation_id)
            })
            .cloned())
    }

    async fn query(&self, query: &FlowQuery) -> CatgaResult<Vec<FlowSummary>> {
        Ok(self
            .records
            .lock()
            .await
            .values()
            .take(query.max_scan())
            .filter(|continuation| query.matches(continuation))
            .take(query.max_results())
            .map(FlowSummary::from_continuation)
            .collect())
    }

    async fn update(&self, expected_version: i64, next: FlowContinuation) -> CatgaResult<bool> {
        let mut records = self.records.lock().await;
        let Some(current) = records.get(next.state().id()) else {
            return Ok(false);
        };
        if current.state().version() != expected_version
            || !FlowState::is_next_version(expected_version, next.state().version())
        {
            return Ok(false);
        }
        records.insert(next.state().id().into(), next);
        Ok(true)
    }

    async fn claim(
        &self,
        expected: &FlowContinuation,
        next: FlowContinuation,
    ) -> CatgaResult<bool> {
        let mut records = self.records.lock().await;
        if records.get(expected.state().id()) != Some(expected)
            || !FlowState::is_next_version(expected.state().version(), next.state().version())
        {
            return Ok(false);
        }
        records.insert(next.state().id().into(), next);
        Ok(true)
    }

    async fn record_wait_success(
        &self,
        flow_id: &str,
        version: i64,
        child_id: &str,
        payload: Vec<u8>,
    ) -> CatgaResult<bool> {
        self.record_wait(flow_id, version, |wait| {
            wait.record_success(child_id, payload)
        })
        .await
    }

    async fn record_wait_failure(
        &self,
        flow_id: &str,
        version: i64,
        child_id: &str,
        error: CatgaError,
    ) -> CatgaResult<bool> {
        self.record_wait(flow_id, version, |wait| {
            wait.record_failure(child_id, error)
        })
        .await
    }

    async fn heartbeat(&self, flow_id: &str, owner: &str, version: i64) -> CatgaResult<bool> {
        let mut records = self.records.lock().await;
        let Some(continuation) = records.get(flow_id).cloned() else {
            return Ok(false);
        };
        if continuation.state().owner() != Some(owner) || continuation.state().version() != version
        {
            return Ok(false);
        }
        records.insert(
            flow_id.into(),
            continuation.clone().with_state(
                continuation
                    .state()
                    .clone()
                    .heartbeated_at(SystemTime::now()),
            ),
        );
        Ok(true)
    }
}

impl ContinuationStore {
    async fn record_wait(
        &self,
        flow_id: &str,
        version: i64,
        update: impl FnOnce(&WaitCondition) -> WaitCondition,
    ) -> CatgaResult<bool> {
        let mut records = self.records.lock().await;
        let Some(continuation) = records.get(flow_id).cloned() else {
            return Ok(false);
        };
        if continuation.state().version() != version {
            return Ok(false);
        }
        let Some(wait) = continuation.wait() else {
            return Ok(false);
        };
        let next_wait = update(wait);
        records.insert(flow_id.into(), continuation.with_wait(next_wait));
        Ok(true)
    }
}

fn runtime(
    definition: FlowDefinition,
) -> (
    Arc<ContinuationStore>,
    Arc<MemoryFlowScheduler>,
    FlowRuntime<ContinuationStore, MemoryFlowScheduler>,
) {
    let store = Arc::new(ContinuationStore::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        definition,
        "worker",
    );
    (store, scheduler, runtime)
}

#[tokio::test]
async fn correlated_any_wait_rejects_invalid_input_then_resumes_exactly_once() -> CatgaResult<()> {
    let wait = WaitCondition::for_children(
        "order-42/children",
        WaitPolicy::Any,
        ["reserve", "charge"],
        SystemTime::now(),
        Duration::from_secs(3_600),
    )?;
    let definition = FlowDefinition::new("checkout")
        .step("await-child", move |_| {
            let wait = wait.clone();
            async move { Ok(FlowStepOutcome::wait(wait)) }
        })
        .step("complete", |_| async { Ok(FlowStepOutcome::complete()) });
    let (_, _, runtime) = runtime(definition);

    let suspended = runtime.start("order-42", []).await?;
    assert!(suspended.is_suspended());
    let missing = runtime
        .record_wait_success_by_correlation("missing", "reserve", Vec::new())
        .await
        .expect_err("unknown correlations must not create a result");
    assert_eq!(missing.code(), ErrorCode::NotFound);
    let invalid_child = runtime
        .record_wait_success_by_correlation("order-42/children", "other", Vec::new())
        .await
        .expect_err("only persisted child identities are accepted");
    assert_eq!(invalid_child.code(), ErrorCode::Validation);

    let completed = runtime
        .record_wait_success_by_correlation("order-42/children", "charge", b"ok".to_vec())
        .await?;
    assert!(completed.is_success());
    assert_eq!(completed.state().status(), FlowStatus::Done);
    assert!(runtime.resume("order-42").await?.is_success());
    Ok(())
}

#[tokio::test]
async fn delayed_suspension_reconciliation_persists_one_schedule_identity() -> CatgaResult<()> {
    let (store, scheduler, runtime) = runtime(
        FlowDefinition::new("checkout")
            .step("complete", |_| async { Ok(FlowStepOutcome::complete()) }),
    );
    let due_at = UNIX_EPOCH + Duration::from_secs(86_400);
    let delayed = FlowContinuation::new(
        FlowState::new("delayed-42", "checkout", [], "lost-worker").suspended(),
        "complete",
    )
    .delayed_until(due_at);
    assert!(store.create(delayed).await?);

    assert_eq!(runtime.reconcile_delayed_suspensions(1, 1).await?, 1);
    let stored = store
        .get("delayed-42")
        .await?
        .expect("reconciled continuation remains durable");
    assert_eq!(stored.schedule_id(), Some("flow-resume-0"));
    assert_eq!(stored.resume_at(), Some(due_at));
    assert_eq!(runtime.reconcile_delayed_suspensions(1, 1).await?, 0);

    let schedules = scheduler.take_due(due_at);
    assert_eq!(schedules.len(), 1);
    assert_eq!(schedules[0].flow_id(), "delayed-42");
    assert_eq!(schedules[0].state_id(), "complete");
    Ok(())
}

#[derive(Default)]
struct ProgressStore {
    records: Mutex<HashMap<(Box<str>, u32), DslStepProgress>>,
}

#[async_trait]
impl DslStepProgressStore for ProgressStore {
    async fn create(&self, progress: DslStepProgress) -> CatgaResult<bool> {
        let key = (progress.flow_id().into(), progress.step_index());
        let mut records = self.records.lock().await;
        if records.contains_key(&key) {
            return Ok(false);
        }
        records.insert(key, progress);
        Ok(true)
    }

    async fn update(&self, expected_version: i64, next: DslStepProgress) -> CatgaResult<bool> {
        let key = (next.flow_id().into(), next.step_index());
        let mut records = self.records.lock().await;
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

    async fn get(&self, flow_id: &str, step_index: u32) -> CatgaResult<Option<DslStepProgress>> {
        Ok(self
            .records
            .lock()
            .await
            .get(&(flow_id.into(), step_index))
            .cloned())
    }

    async fn delete(&self, flow_id: &str, step_index: u32) -> CatgaResult<bool> {
        Ok(self
            .records
            .lock()
            .await
            .remove(&(flow_id.into(), step_index))
            .is_some())
    }
}

struct UsizeCodec;

impl DslStateCodec<usize> for UsizeCodec {
    fn encode(&self, state: &usize) -> CatgaResult<Vec<u8>> {
        Ok(state.to_be_bytes().to_vec())
    }

    fn decode(&self, bytes: &[u8]) -> CatgaResult<usize> {
        let bytes: [u8; std::mem::size_of::<usize>()] = bytes.try_into().map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "checkpoint state has an invalid size",
            )
        })?;
        Ok(usize::from_be_bytes(bytes))
    }
}

#[tokio::test]
async fn checkpointed_dsl_rejects_a_corrupted_internal_frame_before_running_actions()
-> CatgaResult<()> {
    let actions = Arc::new(AtomicUsize::new(0));
    let action_calls = Arc::clone(&actions);
    let flow = DslFlow::new().action(move |state: &mut usize| {
        let action_calls = Arc::clone(&action_calls);
        Box::pin(async move {
            action_calls.fetch_add(1, Ordering::SeqCst);
            *state += 1;
            Ok(())
        })
    });
    let progress = ProgressStore::default();
    assert!(
        progress
            .create(DslStepProgress::new("corrupt-frame", 0, b"CDF1".to_vec()))
            .await?
    );

    let error = flow
        .run_checkpointed("corrupt-frame", 0, &progress, &UsizeCodec)
        .await
        .expect_err("a truncated Catga-owned checkpoint frame is never application state");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(actions.load(Ordering::SeqCst), 0);
    Ok(())
}
