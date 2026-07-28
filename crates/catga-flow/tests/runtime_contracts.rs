//! Durable runtime contracts exercised against an in-memory continuation store.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{
    FlowContinuation, FlowDefinition, FlowRuntime, FlowState, FlowStatus, FlowStepOutcome,
    MemoryFlowScheduler, SuspendedFlowStore, WaitCondition, WaitPolicy,
};
use tokio::sync::Mutex;

#[derive(Default)]
struct MemoryContinuations {
    records: Mutex<HashMap<Box<str>, FlowContinuation>>,
}

#[async_trait]
impl SuspendedFlowStore for MemoryContinuations {
    async fn create(&self, continuation: FlowContinuation) -> CatgaResult<bool> {
        let mut records = self.records.lock().await;
        if records.contains_key(continuation.state().id()) {
            return Ok(false);
        }
        records.insert(continuation.state().id().into(), continuation);
        Ok(true)
    }

    async fn get(&self, flow_id: &str) -> CatgaResult<Option<FlowContinuation>> {
        Ok(self.records.lock().await.get(flow_id).cloned())
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
        let state = continuation
            .state()
            .clone()
            .heartbeated_at(SystemTime::now());
        records.insert(flow_id.into(), continuation.with_state(state));
        Ok(true)
    }
}

impl MemoryContinuations {
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
    Arc<MemoryContinuations>,
    Arc<MemoryFlowScheduler>,
    FlowRuntime<MemoryContinuations, MemoryFlowScheduler>,
) {
    let store = Arc::new(MemoryContinuations::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        definition,
        "test",
    );
    (store, scheduler, runtime)
}

fn child_wait(policy: WaitPolicy) -> WaitCondition {
    WaitCondition::for_children(
        "checkout-children",
        policy,
        ["reserve", "charge"],
        SystemTime::now(),
        Duration::from_secs(60 * 60),
    )
    .expect("test child identities are valid")
}

#[tokio::test]
async fn runtime_waits_for_all_children_before_running_the_next_step() -> CatgaResult<()> {
    let wait = child_wait(WaitPolicy::All);
    let definition = FlowDefinition::new("checkout")
        .step("await-children", move |_| {
            let wait = wait.clone();
            async move { Ok(FlowStepOutcome::wait(wait)) }
        })
        .step("complete", |_| async { Ok(FlowStepOutcome::complete()) });
    let (store, _, runtime) = runtime(definition);

    let started = runtime.start("checkout-1", []).await?;
    assert!(started.is_suspended());
    assert_eq!(started.state().status(), FlowStatus::Suspended);
    let duplicate = runtime
        .start("checkout-1", [])
        .await
        .expect_err("flow identities are unique");
    assert_eq!(duplicate.code(), ErrorCode::Conflict);
    assert_eq!(
        store
            .get("checkout-1")
            .await?
            .expect("started continuation persists")
            .step_name(),
        "complete"
    );

    let pending = runtime
        .record_wait_success("checkout-1", "reserve", b"reserved".to_vec())
        .await?;
    assert!(pending.is_suspended());
    let done = runtime
        .record_wait_success("checkout-1", "charge", b"charged".to_vec())
        .await?;
    assert!(done.is_success());
    assert_eq!(done.state().step(), 2);

    let replay = runtime.resume("checkout-1").await?;
    assert!(replay.is_success());
    Ok(())
}

#[tokio::test]
async fn runtime_fails_an_all_wait_when_a_child_reports_an_error() -> CatgaResult<()> {
    let wait = child_wait(WaitPolicy::All);
    let definition = FlowDefinition::new("checkout")
        .step("await-children", move |_| {
            let wait = wait.clone();
            async move { Ok(FlowStepOutcome::wait(wait)) }
        })
        .step("unreachable", |_| async { Ok(FlowStepOutcome::complete()) });
    let (_, _, runtime) = runtime(definition);

    runtime.start("checkout-2", []).await?;
    let unknown = runtime
        .record_wait_success("checkout-2", "unknown", b"ignored".to_vec())
        .await
        .expect_err("only persisted children can complete a durable wait");
    assert_eq!(unknown.code(), ErrorCode::Validation);
    let failed = runtime
        .record_wait_failure(
            "checkout-2",
            "reserve",
            CatgaError::new(ErrorCode::Validation, "reservation rejected"),
        )
        .await?;

    assert!(failed.is_failure());
    assert_eq!(
        failed.state().error().map(CatgaError::code),
        Some(ErrorCode::Validation)
    );
    let terminal = runtime
        .record_wait_success("checkout-2", "charge", b"ignored".to_vec())
        .await?;
    assert!(terminal.is_failure());
    Ok(())
}

#[tokio::test]
async fn runtime_cancellation_fences_a_delayed_resume_and_removes_its_schedule() -> CatgaResult<()>
{
    let due_at = UNIX_EPOCH + Duration::from_secs(60 * 60);
    let definition = FlowDefinition::new("delayed")
        .step("pause", move |_| async move {
            Ok(FlowStepOutcome::suspend_until(due_at))
        })
        .step("complete", |_| async { Ok(FlowStepOutcome::complete()) });
    let (_, scheduler, runtime) = runtime(definition);

    let suspended = runtime.start("delayed-1", []).await?;
    assert!(suspended.is_suspended());
    let stale = runtime
        .resume_scheduled("delayed-1", "old-step")
        .await
        .expect_err("a schedule must target the continuation's current step");
    assert_eq!(stale.code(), ErrorCode::Conflict);
    let cancelled = runtime.cancel("delayed-1").await?;
    assert!(cancelled.is_cancelled());
    assert!(scheduler.take_due(due_at).is_empty());

    let replay = runtime.resume_at("delayed-1", due_at).await?;
    assert!(replay.is_cancelled());
    Ok(())
}
