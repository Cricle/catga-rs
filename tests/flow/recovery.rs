use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use catga_core::CatgaResult;
use catga_flow::{
    FlowContinuation, FlowDefinition, FlowRuntime, FlowScheduler, FlowState, FlowStepOutcome,
    MemoryFlowScheduler, SuspendedFlowStore, WaitCondition, WaitPolicy,
};
use catga_memory::MemorySuspendedFlows;

struct HeartbeatBeforeClaimStore {
    inner: Arc<MemorySuspendedFlows>,
}

#[async_trait]
impl SuspendedFlowStore for HeartbeatBeforeClaimStore {
    async fn create(&self, continuation: FlowContinuation) -> CatgaResult<bool> {
        self.inner.create(continuation).await
    }

    async fn get(&self, flow_id: &str) -> CatgaResult<Option<FlowContinuation>> {
        self.inner.get(flow_id).await
    }

    async fn update(&self, expected_version: i64, next: FlowContinuation) -> CatgaResult<bool> {
        self.inner.update(expected_version, next).await
    }

    async fn claim(
        &self,
        expected: &FlowContinuation,
        next: FlowContinuation,
    ) -> CatgaResult<bool> {
        if next.state().status() == catga_flow::FlowStatus::Running
            && next.state().owner() == Some("node-b")
        {
            assert!(self.inner.heartbeat("heartbeat-race", "node-a", 0).await?);
        }
        self.inner.claim(expected, next).await
    }

    async fn record_wait_success(
        &self,
        flow_id: &str,
        version: i64,
        child_id: &str,
        payload: Vec<u8>,
    ) -> CatgaResult<bool> {
        self.inner
            .record_wait_success(flow_id, version, child_id, payload)
            .await
    }

    async fn record_wait_failure(
        &self,
        flow_id: &str,
        version: i64,
        child_id: &str,
        error: catga_core::CatgaError,
    ) -> CatgaResult<bool> {
        self.inner
            .record_wait_failure(flow_id, version, child_id, error)
            .await
    }

    async fn heartbeat(&self, flow_id: &str, owner: &str, version: i64) -> CatgaResult<bool> {
        self.inner.heartbeat(flow_id, owner, version).await
    }
}

#[tokio::test]
async fn runtime_rejects_a_continuation_owned_by_a_different_definition() {
    let store = Arc::new(MemorySuspendedFlows::default());
    store
        .create(FlowContinuation::new(
            FlowState::new("wrong", "payments", b"input".to_vec(), "node-a").suspended(),
            "finish",
        ))
        .await
        .unwrap();
    let runtime = FlowRuntime::new(
        store,
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("refunds")
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-b",
    );

    assert!(runtime.resume("wrong").await.is_err());
}

#[tokio::test]
async fn wrong_definition_cannot_record_a_child_result() {
    let store = Arc::new(MemorySuspendedFlows::default());
    store
        .create(FlowContinuation::waiting(
            FlowState::new("wrong-wait", "payments", b"input".to_vec(), "node-a").suspended(),
            "finish",
            WaitCondition::new(
                "wrong-wait-condition",
                WaitPolicy::All,
                1,
                SystemTime::now(),
                Duration::from_secs(30),
            ),
        ))
        .await
        .unwrap();
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("refunds")
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-b",
    );

    assert!(
        runtime
            .record_wait_success("wrong-wait", "child", b"payload".to_vec())
            .await
            .is_err()
    );
    assert_eq!(
        store
            .get("wrong-wait")
            .await
            .unwrap()
            .unwrap()
            .wait()
            .unwrap()
            .completed_count(),
        0
    );
}

#[tokio::test]
async fn memory_scheduler_allows_distinct_state_resumes_for_one_flow() {
    let scheduler = MemoryFlowScheduler::default();
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);

    assert!(
        scheduler
            .schedule_resume("flow-15", "reserve", now)
            .await
            .is_ok()
    );
    assert!(
        scheduler
            .schedule_resume("flow-15", "charge", now)
            .await
            .is_ok()
    );

    let due = scheduler.take_due(now);
    let targets: Vec<(&str, &str)> = due
        .iter()
        .map(|schedule| (schedule.flow_id(), schedule.state_id()))
        .collect();

    assert_eq!(targets, [("flow-15", "reserve"), ("flow-15", "charge")]);
}

#[tokio::test]
async fn memory_scheduler_yields_due_resumes_in_deadline_order() {
    let scheduler = MemoryFlowScheduler::default();
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);

    scheduler
        .schedule_resume("late", "late-state", now + Duration::from_secs(30))
        .await
        .unwrap();
    scheduler
        .schedule_resume("first", "first-state", now + Duration::from_secs(10))
        .await
        .unwrap();
    scheduler
        .schedule_resume("middle", "middle-state", now + Duration::from_secs(20))
        .await
        .unwrap();

    let due = scheduler.take_due(now + Duration::from_secs(30));
    let flow_ids: Vec<&str> = due.iter().map(|schedule| schedule.flow_id()).collect();

    assert_eq!(flow_ids, ["first", "middle", "late"]);
}

#[tokio::test]
async fn concurrent_resumes_return_current_state_instead_of_transient_errors() {
    let store = Arc::new(MemorySuspendedFlows::default());
    store
        .create(FlowContinuation::waiting(
            FlowState::new("race", "payment", b"input".to_vec(), "node-a").suspended(),
            "finish",
            WaitCondition::new(
                "race-wait",
                WaitPolicy::All,
                0,
                SystemTime::now(),
                Duration::from_secs(30),
            ),
        ))
        .await
        .unwrap();
    let runtime = Arc::new(FlowRuntime::new(
        store,
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("payment").step("finish", |_| async {
            tokio::task::yield_now().await;
            Ok(FlowStepOutcome::complete())
        }),
        "node-b",
    ));

    let first_runtime = Arc::clone(&runtime);
    let second_runtime = Arc::clone(&runtime);
    let first = tokio::spawn(async move { first_runtime.resume("race").await });
    let second = tokio::spawn(async move { second_runtime.resume("race").await });
    let (first, second) = tokio::join!(first, second);

    assert!(first.unwrap().is_ok());
    assert!(second.unwrap().is_ok());
}

#[tokio::test]
async fn runtime_claims_an_abandoned_running_continuation_after_its_stale_timeout() {
    let store = Arc::new(MemorySuspendedFlows::default());
    store
        .create(FlowContinuation::new(
            FlowState::new("abandoned", "payment", b"input".to_vec(), "dead-node")
                .running()
                .heartbeated_at(SystemTime::UNIX_EPOCH),
            "finish",
        ))
        .await
        .unwrap();
    let runtime = FlowRuntime::new(
        store,
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("payment")
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-b",
    )
    .with_stale_after(Duration::ZERO);

    assert!(runtime.resume("abandoned").await.unwrap().is_success());
}

#[tokio::test]
async fn heartbeat_prevents_a_live_running_continuation_from_being_reclaimed() {
    let store = Arc::new(MemorySuspendedFlows::default());
    store
        .create(FlowContinuation::new(
            FlowState::new("live", "payment", b"input".to_vec(), "node-a")
                .running()
                .heartbeated_at(SystemTime::UNIX_EPOCH),
            "finish",
        ))
        .await
        .unwrap();
    let owner = FlowRuntime::new(
        Arc::clone(&store),
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("payment")
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    );
    let contender = FlowRuntime::new(
        store,
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("payment")
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-b",
    )
    .with_stale_after(Duration::from_secs(86_400));

    assert!(owner.heartbeat("live", 0).await.unwrap());
    assert!(contender.resume("live").await.unwrap().is_running());
}

#[tokio::test]
async fn stale_claim_cannot_overwrite_a_heartbeat_that_arrives_after_the_stale_read() {
    let inner = Arc::new(MemorySuspendedFlows::default());
    let store = Arc::new(HeartbeatBeforeClaimStore {
        inner: Arc::clone(&inner),
    });
    store
        .create(FlowContinuation::new(
            FlowState::new("heartbeat-race", "payment", b"input".to_vec(), "node-a")
                .running()
                .heartbeated_at(SystemTime::UNIX_EPOCH),
            "finish",
        ))
        .await
        .unwrap();
    let contender = FlowRuntime::new(
        store,
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("payment")
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-b",
    )
    .with_stale_after(Duration::ZERO);

    let result = contender.resume("heartbeat-race").await.unwrap();

    assert!(result.is_running());
    let current = inner.get("heartbeat-race").await.unwrap().unwrap();
    assert_eq!(current.state().owner(), Some("node-a"));
}

#[tokio::test]
async fn cancellation_is_terminal_and_resume_is_idempotent() {
    let store = Arc::new(MemorySuspendedFlows::default());
    store
        .create(FlowContinuation::new(
            FlowState::new("cancel", "payment", b"input".to_vec(), "node-a").suspended(),
            "finish",
        ))
        .await
        .unwrap();
    let runtime = FlowRuntime::new(
        store,
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("payment")
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-b",
    );

    assert!(runtime.cancel("cancel").await.unwrap().is_cancelled());
    assert!(runtime.cancel("cancel").await.unwrap().is_cancelled());
    assert!(runtime.resume("cancel").await.unwrap().is_cancelled());
}

#[tokio::test]
async fn restart_retries_the_persisted_durable_compensation_before_marking_failed() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let attempts = Arc::new(AtomicUsize::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let reserve_events = Arc::clone(&events);
    let compensate_attempts = Arc::clone(&attempts);
    let compensate_events = Arc::clone(&events);
    let definition = FlowDefinition::new("payment")
        .step_with_compensation(
            "reserve",
            move |_| {
                let events = Arc::clone(&reserve_events);
                async move {
                    events.lock().unwrap().push("reserve");
                    Ok(FlowStepOutcome::Advance)
                }
            },
            move |_| {
                let attempts = Arc::clone(&compensate_attempts);
                let events = Arc::clone(&compensate_events);
                async move {
                    events.lock().unwrap().push("release");
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Err(catga_core::CatgaError::new(
                            catga_core::ErrorCode::Transient,
                            "release is temporarily unavailable",
                        ));
                    }
                    Ok(())
                }
            },
        )
        .step("charge", |_| async {
            Err(catga_core::CatgaError::new(
                catga_core::ErrorCode::Validation,
                "charge was rejected",
            ))
        });
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::new(MemoryFlowScheduler::default()),
        definition,
        "node-a",
    )
    .with_stale_after(Duration::ZERO);

    let first = runtime.start("compensate-after-restart", []).await;
    assert!(first.is_err());
    assert_eq!(
        store
            .get("compensate-after-restart")
            .await
            .unwrap()
            .unwrap()
            .state()
            .status(),
        catga_flow::FlowStatus::Compensating
    );
    let cancellation = runtime
        .cancel("compensate-after-restart")
        .await
        .expect_err("cancellation must not abandon persisted rollback actions");
    assert_eq!(cancellation.code(), catga_core::ErrorCode::Conflict);

    let recovered = runtime.resume("compensate-after-restart").await.unwrap();

    assert!(recovered.is_failure());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(*events.lock().unwrap(), ["reserve", "release", "release"]);
}

#[tokio::test]
async fn durable_compensation_runs_successful_steps_in_reverse_completion_order() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let events = Arc::new(Mutex::new(Vec::new()));
    let reserve_events = Arc::clone(&events);
    let release_events = Arc::clone(&events);
    let charge_events = Arc::clone(&events);
    let refund_events = Arc::clone(&events);
    let definition = FlowDefinition::new("payment")
        .step_with_compensation(
            "reserve",
            move |_| {
                let events = Arc::clone(&reserve_events);
                async move {
                    events.lock().unwrap().push("reserve");
                    Ok(FlowStepOutcome::Advance)
                }
            },
            move |_| {
                let events = Arc::clone(&release_events);
                async move {
                    events.lock().unwrap().push("release");
                    Ok(())
                }
            },
        )
        .step_with_compensation(
            "charge",
            move |_| {
                let events = Arc::clone(&charge_events);
                async move {
                    events.lock().unwrap().push("charge");
                    Ok(FlowStepOutcome::Advance)
                }
            },
            move |_| {
                let events = Arc::clone(&refund_events);
                async move {
                    events.lock().unwrap().push("refund");
                    Ok(())
                }
            },
        )
        .step("finalize", |_| async {
            Err(catga_core::CatgaError::new(
                catga_core::ErrorCode::Validation,
                "finalization was rejected",
            ))
        });
    let runtime = FlowRuntime::new(
        store,
        Arc::new(MemoryFlowScheduler::default()),
        definition,
        "node-a",
    );

    let result = runtime.start("reverse-compensation", []).await.unwrap();

    assert!(result.is_failure());
    assert_eq!(
        *events.lock().unwrap(),
        ["reserve", "charge", "refund", "release"]
    );
}
