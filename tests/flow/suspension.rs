use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{
    FlowContinuation, FlowDefinition, FlowQuery, FlowRuntime, FlowState, FlowStepOutcome,
    FlowSummary, FlowTimeoutOptions, FlowTimeoutService, MAX_FLOW_TIMEOUT_BATCH_SIZE,
    MAX_FLOW_TIMEOUT_SCAN_LIMIT, MemoryFlowScheduler, SuspendedFlowStore, TimedOutFlowPoll,
    TimedOutFlowReceipt, TimedOutFlowStore, WaitCondition, WaitPolicy,
};
use catga_memory::MemorySuspendedFlows;
use tokio_util::sync::CancellationToken;

mod timeout_store_contract;

struct ObservingTimeoutStore {
    inner: MemorySuspendedFlows,
    receipts: Vec<TimedOutFlowReceipt>,
    fail_ack: bool,
    fail_release: bool,
    released: tokio::sync::Mutex<Vec<Box<str>>>,
}

impl ObservingTimeoutStore {
    fn new(receipts: Vec<TimedOutFlowReceipt>, fail_ack: bool) -> Self {
        Self {
            inner: MemorySuspendedFlows::default(),
            receipts,
            fail_ack,
            fail_release: false,
            released: tokio::sync::Mutex::new(Vec::new()),
        }
    }

    fn with_release_failure(mut self) -> Self {
        self.fail_release = true;
        self
    }

    async fn released(&self) -> Vec<String> {
        self.released
            .lock()
            .await
            .iter()
            .map(ToString::to_string)
            .collect()
    }
}

#[async_trait]
impl SuspendedFlowStore for ObservingTimeoutStore {
    async fn create(&self, continuation: FlowContinuation) -> CatgaResult<bool> {
        self.inner.create(continuation).await
    }

    async fn get(&self, flow_id: &str) -> CatgaResult<Option<FlowContinuation>> {
        self.inner.get(flow_id).await
    }

    async fn query(&self, query: &FlowQuery) -> CatgaResult<Vec<FlowSummary>> {
        self.inner.query(query).await
    }

    async fn delete(&self, flow_id: &str, expected_version: i64) -> CatgaResult<bool> {
        self.inner.delete(flow_id, expected_version).await
    }

    async fn update(&self, expected_version: i64, next: FlowContinuation) -> CatgaResult<bool> {
        self.inner.update(expected_version, next).await
    }

    async fn claim(
        &self,
        expected: &FlowContinuation,
        next: FlowContinuation,
    ) -> CatgaResult<bool> {
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
        error: CatgaError,
    ) -> CatgaResult<bool> {
        self.inner
            .record_wait_failure(flow_id, version, child_id, error)
            .await
    }

    async fn heartbeat(&self, flow_id: &str, owner: &str, version: i64) -> CatgaResult<bool> {
        self.inner.heartbeat(flow_id, owner, version).await
    }
}

#[async_trait]
impl TimedOutFlowStore for ObservingTimeoutStore {
    async fn poll_timed_out(
        &self,
        _poll: &TimedOutFlowPoll,
    ) -> CatgaResult<Vec<TimedOutFlowReceipt>> {
        Ok(self.receipts.clone())
    }

    async fn ack_timed_out(&self, _receipt: &TimedOutFlowReceipt) -> CatgaResult<()> {
        if self.fail_ack {
            return Err(CatgaError::new(ErrorCode::Transient, "ack failed"));
        }
        Ok(())
    }

    async fn release_timed_out(&self, receipt: &TimedOutFlowReceipt) -> CatgaResult<()> {
        self.released.lock().await.push(receipt.flow_id().into());
        if self.fail_release {
            return Err(CatgaError::new(ErrorCode::Internal, "release failed"));
        }
        Ok(())
    }
}

#[test]
fn continuation_keeps_shared_input_and_wait_completion_is_immutable() {
    let state = FlowState::new("flow-12", "payment", b"input".to_vec(), "node-a");
    let continuation = FlowContinuation::waiting(
        state,
        "charge",
        WaitCondition::new(
            "wait-12",
            WaitPolicy::All,
            2,
            SystemTime::now(),
            Duration::from_secs(30),
        ),
    );
    let next = continuation
        .wait()
        .unwrap()
        .record_success("child-a", b"ok".to_vec());

    assert_eq!(continuation.step_name(), "charge");
    assert_eq!(next.completed_count(), 1);
    assert_eq!(next.expected_count(), 2);
    assert_eq!(continuation.state().data(), b"input");
}

#[test]
fn delayed_step_outcome_advances_for_zero_and_suspends_for_a_positive_duration() {
    assert!(matches!(
        FlowStepOutcome::delay(Duration::ZERO).unwrap(),
        FlowStepOutcome::Advance
    ));

    let before = SystemTime::now();
    let outcome = FlowStepOutcome::delay(Duration::from_secs(1)).unwrap();
    let FlowStepOutcome::SuspendUntil(resume_at) = outcome else {
        panic!("a positive delay must suspend the durable flow");
    };
    assert!(resume_at.duration_since(before).unwrap() >= Duration::from_secs(1));
}

#[tokio::test]
async fn concurrent_wait_results_are_not_lost() {
    let store = MemorySuspendedFlows::default();
    let continuation = FlowContinuation::waiting(
        FlowState::new("flow-12", "payment", b"input".to_vec(), "node-a"),
        "charge",
        WaitCondition::new(
            "wait-12",
            WaitPolicy::All,
            2,
            SystemTime::now(),
            Duration::from_secs(30),
        ),
    );
    assert!(store.create(continuation).await.unwrap());

    let first = store.record_wait_success("flow-12", 0, "child-a", b"a".to_vec());
    let second = store.record_wait_success("flow-12", 0, "child-b", b"b".to_vec());
    let (first, second) = tokio::join!(first, second);

    assert!(first.unwrap());
    assert!(second.unwrap());
    assert_eq!(
        store
            .get("flow-12")
            .await
            .unwrap()
            .unwrap()
            .wait()
            .unwrap()
            .completed_count(),
        2
    );
}

#[tokio::test]
async fn delayed_flow_persists_and_resumes_registered_steps() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let definition = FlowDefinition::new("payment")
        .step("reserve", |_| async {
            Ok(FlowStepOutcome::suspend_until(SystemTime::now()))
        })
        .step("charge", |_| async { Ok(FlowStepOutcome::complete()) });
    let runtime = FlowRuntime::new(store, Arc::clone(&scheduler), definition, "node-a");

    let suspended = runtime.start("flow-13", b"input".to_vec()).await.unwrap();
    assert!(suspended.is_suspended());
    assert_eq!(scheduler.take_due(SystemTime::now()).len(), 1);

    assert!(runtime.resume("flow-13").await.unwrap().is_success());
}

#[tokio::test]
async fn cancelling_a_delayed_flow_cancels_its_scheduled_resume() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let resume_at = SystemTime::now() + Duration::from_secs(60);
    let runtime = FlowRuntime::new(
        store,
        Arc::clone(&scheduler),
        FlowDefinition::new("payment")
            .step("reserve", move |_| async move {
                Ok(FlowStepOutcome::suspend_until(resume_at))
            })
            .step("charge", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    );

    assert!(
        runtime
            .start("flow-14", b"input".to_vec())
            .await
            .unwrap()
            .is_suspended()
    );
    assert!(runtime.cancel("flow-14").await.unwrap().is_cancelled());
    assert!(scheduler.take_due(resume_at).is_empty());
}

#[tokio::test]
async fn scheduled_resume_rejects_a_stale_state_target() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let resume_at = SystemTime::now();
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        scheduler,
        FlowDefinition::new("payment")
            .step("reserve", move |_| async move {
                Ok(FlowStepOutcome::suspend_until(resume_at))
            })
            .step("charge", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    );

    assert!(
        runtime
            .start("flow-16", b"input".to_vec())
            .await
            .unwrap()
            .is_suspended()
    );
    assert_eq!(
        runtime
            .resume_scheduled("flow-16", "obsolete-state")
            .await
            .expect_err("stale scheduled state must not resume the flow")
            .code(),
        ErrorCode::Conflict
    );
    assert!(
        runtime
            .resume_scheduled("flow-16", "charge")
            .await
            .unwrap()
            .is_success()
    );
    assert!(
        store
            .get("flow-16")
            .await
            .unwrap()
            .unwrap()
            .state()
            .status()
            .is_terminal()
    );
}

#[tokio::test]
async fn timeout_service_fails_a_waiting_flow_without_an_external_resume_event() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
    let runtime = Arc::new(FlowRuntime::new(
        Arc::clone(&store),
        scheduler,
        FlowDefinition::new("payment")
            .step("wait", move |_| async move {
                Ok(FlowStepOutcome::wait(WaitCondition::new(
                    "payment-wait",
                    WaitPolicy::All,
                    1,
                    now - Duration::from_secs(1),
                    Duration::from_millis(1),
                )))
            })
            .step("charge", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    ));
    assert!(
        runtime
            .start("flow-timeout", b"input".to_vec())
            .await
            .unwrap()
            .is_suspended()
    );

    let service = FlowTimeoutService::new(Arc::clone(&runtime), Arc::clone(&store));
    assert_eq!(service.check_at(now).await.unwrap(), 1);
    assert!(
        runtime
            .resume_at("flow-timeout", now)
            .await
            .unwrap()
            .is_failure()
    );
}

#[tokio::test]
async fn timeout_service_releases_every_over_returned_receipt_before_reporting_validation() {
    let store = Arc::new(ObservingTimeoutStore::new(
        vec![
            TimedOutFlowReceipt::new("over-return/1", [1]),
            TimedOutFlowReceipt::new("over-return/2", [2]),
        ],
        false,
    ));
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let runtime = Arc::new(FlowRuntime::new(
        Arc::clone(&store),
        scheduler,
        FlowDefinition::new("over-return")
            .step("work", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    ));
    let service = FlowTimeoutService::new(runtime, Arc::clone(&store))
        .with_options(FlowTimeoutOptions::new(Duration::from_secs(1), 1, 1).unwrap())
        .unwrap();

    let error = service.check_at(SystemTime::now()).await.unwrap_err();

    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(
        store.released().await,
        vec!["over-return/1", "over-return/2"]
    );
}

#[tokio::test]
async fn timeout_service_releases_the_current_receipt_when_acknowledgement_fails() {
    let receipt = TimedOutFlowReceipt::new("ack-failure/1", [1]);
    let store = Arc::new(ObservingTimeoutStore::new(vec![receipt], true).with_release_failure());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let runtime = Arc::new(FlowRuntime::new(
        Arc::clone(&store),
        scheduler,
        FlowDefinition::new("ack-failure")
            .step("work", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    ));
    assert!(
        store
            .create(FlowContinuation::new(
                FlowState::new("ack-failure/1", "ack-failure", [], "node-a").suspended(),
                "work",
            ))
            .await
            .unwrap()
    );
    let service = FlowTimeoutService::new(runtime, Arc::clone(&store));

    let error = service.check_at(SystemTime::now()).await.unwrap_err();

    assert_eq!(error.code(), ErrorCode::Transient);
    assert_eq!(error.message(), "ack failed");
    assert_eq!(store.released().await, vec!["ack-failure/1"]);
}

#[tokio::test]
async fn timeout_service_cancellation_interrupts_a_running_resume_and_releases_claimed_receipts() {
    let store = Arc::new(ObservingTimeoutStore::new(
        vec![
            TimedOutFlowReceipt::new("cancel-timeout/1", [1]),
            TimedOutFlowReceipt::new("cancel-timeout/2", [2]),
        ],
        false,
    ));
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let entered = Arc::new(tokio::sync::Notify::new());
    let never_complete = Arc::new(tokio::sync::Notify::new());
    let runtime = Arc::new(FlowRuntime::new(
        Arc::clone(&store),
        scheduler,
        FlowDefinition::new("cancel-timeout").step("work", {
            let entered = Arc::clone(&entered);
            let never_complete = Arc::clone(&never_complete);
            move |_| {
                let entered = Arc::clone(&entered);
                let never_complete = Arc::clone(&never_complete);
                async move {
                    entered.notify_one();
                    never_complete.notified().await;
                    Ok(FlowStepOutcome::complete())
                }
            }
        }),
        "node-a",
    ));
    for flow_id in ["cancel-timeout/1", "cancel-timeout/2"] {
        assert!(
            store
                .create(FlowContinuation::new(
                    FlowState::new(flow_id, "cancel-timeout", [], "node-a").suspended(),
                    "work",
                ))
                .await
                .unwrap()
        );
    }
    let service = Arc::new(FlowTimeoutService::new(runtime, Arc::clone(&store)));
    let cancellation = CancellationToken::new();
    let run = tokio::spawn({
        let service = Arc::clone(&service);
        let cancellation = cancellation.clone();
        async move { service.run(cancellation).await }
    });

    entered.notified().await;
    cancellation.cancel();

    let result = tokio::time::timeout(Duration::from_secs(1), run)
        .await
        .expect("timeout service must stop promptly")
        .expect("timeout task must not panic");
    assert!(result.is_ok());
    assert_eq!(
        store.released().await,
        vec!["cancel-timeout/1", "cancel-timeout/2"]
    );
}

#[tokio::test]
async fn memory_timeout_store_polls_a_bounded_native_due_index() {
    timeout_store_contract::run_timeout_store_contract(
        &MemorySuspendedFlows::default(),
        "memory-timeout",
        false,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn long_running_step_renews_its_runtime_ownership() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let entered = Arc::new(tokio::sync::Notify::new());
    let entered_observer = Arc::clone(&entered);
    let release = Arc::new(tokio::sync::Notify::new());
    let executions = Arc::new(AtomicUsize::new(0));
    let definition = |entered: Arc<tokio::sync::Notify>,
                      release: Arc<tokio::sync::Notify>,
                      executions: Arc<AtomicUsize>| {
        FlowDefinition::new("long-running").step("work", move |_| {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            let executions = Arc::clone(&executions);
            async move {
                let attempt = executions.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    entered.notify_one();
                    release.notified().await;
                }
                Ok(FlowStepOutcome::complete())
            }
        })
    };
    let first = Arc::new(
        FlowRuntime::new(
            Arc::clone(&store),
            Arc::clone(&scheduler),
            definition(
                Arc::clone(&entered),
                Arc::clone(&release),
                Arc::clone(&executions),
            ),
            "worker-a",
        )
        .with_stale_after(Duration::from_millis(40)),
    );
    let second = FlowRuntime::new(
        Arc::clone(&store),
        scheduler,
        definition(entered, Arc::clone(&release), Arc::clone(&executions)),
        "worker-b",
    )
    .with_stale_after(Duration::from_millis(40));
    let first_task = tokio::spawn({
        let first = Arc::clone(&first);
        async move { first.start("long-running/1", []).await }
    });
    entered_observer.notified().await;
    tokio::time::sleep(Duration::from_millis(90)).await;

    assert!(second.resume("long-running/1").await.unwrap().is_running());
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    release.notify_waiters();
    assert!(first_task.await.unwrap().unwrap().is_success());
}

#[test]
fn timeout_sweep_bounds_are_validated_before_store_access() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);

    for result in [
        TimedOutFlowPoll::new(now, 0, 1),
        TimedOutFlowPoll::new(now, 2, 1),
        TimedOutFlowPoll::new(now, MAX_FLOW_TIMEOUT_BATCH_SIZE + 1, 128),
        TimedOutFlowPoll::new(now, 1, MAX_FLOW_TIMEOUT_SCAN_LIMIT + 1),
    ] {
        assert_eq!(
            result.unwrap_err().code(),
            catga_core::ErrorCode::Validation
        );
    }
    assert!(FlowTimeoutOptions::new(Duration::from_secs(1), 0, 1).is_err());
}

#[tokio::test]
async fn timeout_service_processes_only_one_bounded_batch_per_check() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
    let runtime = Arc::new(FlowRuntime::new(
        Arc::clone(&store),
        scheduler,
        FlowDefinition::new("bounded-timeout")
            .step("wait", move |_| async move {
                Ok(FlowStepOutcome::wait(WaitCondition::new(
                    "bounded-timeout-wait",
                    WaitPolicy::All,
                    1,
                    now - Duration::from_secs(1),
                    Duration::from_millis(1),
                )))
            })
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    ));
    for id in [
        "bounded-timeout/1",
        "bounded-timeout/2",
        "bounded-timeout/3",
    ] {
        assert!(runtime.start(id, []).await.unwrap().is_suspended());
    }
    let service = FlowTimeoutService::new(Arc::clone(&runtime), Arc::clone(&store))
        .with_options(FlowTimeoutOptions::new(Duration::from_secs(1), 2, 3).unwrap())
        .unwrap();

    assert_eq!(service.check_at(now).await.unwrap(), 2);
    let first_pass_terminal = futures::future::try_join_all(
        [
            "bounded-timeout/1",
            "bounded-timeout/2",
            "bounded-timeout/3",
        ]
        .into_iter()
        .map(|id| async {
            Ok::<_, catga_core::CatgaError>(
                store.get(id).await?.unwrap().state().status().is_terminal(),
            )
        }),
    )
    .await
    .unwrap()
    .into_iter()
    .filter(|terminal| *terminal)
    .count();
    assert_eq!(first_pass_terminal, 2);

    assert_eq!(service.check_at(now).await.unwrap(), 1);
    for id in [
        "bounded-timeout/1",
        "bounded-timeout/2",
        "bounded-timeout/3",
    ] {
        assert!(
            store
                .get(id)
                .await
                .unwrap()
                .unwrap()
                .state()
                .status()
                .is_terminal()
        );
    }
}

#[tokio::test]
async fn wait_policies_resume_once_and_expire_deterministically() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let all_runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        FlowDefinition::new("all")
            .step("wait", |_| async {
                Ok(FlowStepOutcome::wait(WaitCondition::new(
                    "all-wait",
                    WaitPolicy::All,
                    2,
                    SystemTime::now(),
                    Duration::from_secs(30),
                )))
            })
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    );
    assert!(
        all_runtime
            .start("all", b"input".to_vec())
            .await
            .unwrap()
            .is_suspended()
    );
    assert!(
        all_runtime
            .record_wait_success("all", "one", b"one".to_vec())
            .await
            .unwrap()
            .is_suspended()
    );
    assert!(
        all_runtime
            .record_wait_success("all", "two", b"two".to_vec())
            .await
            .unwrap()
            .is_success()
    );

    let any_runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        FlowDefinition::new("any")
            .step("wait", |_| async {
                Ok(FlowStepOutcome::wait(WaitCondition::new(
                    "any-wait",
                    WaitPolicy::Any,
                    2,
                    SystemTime::now(),
                    Duration::from_secs(30),
                )))
            })
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    );
    assert!(
        any_runtime
            .start("any", b"input".to_vec())
            .await
            .unwrap()
            .is_suspended()
    );
    assert!(
        any_runtime
            .record_wait_success("any", "one", b"one".to_vec())
            .await
            .unwrap()
            .is_success()
    );

    let expired_runtime = FlowRuntime::new(
        Arc::clone(&store),
        scheduler,
        FlowDefinition::new("expired")
            .step("wait", |_| async {
                Ok(FlowStepOutcome::wait(WaitCondition::new(
                    "expired-wait",
                    WaitPolicy::All,
                    1,
                    SystemTime::UNIX_EPOCH,
                    Duration::from_secs(1),
                )))
            })
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    );
    assert!(
        expired_runtime
            .start("expired", b"input".to_vec())
            .await
            .unwrap()
            .is_suspended()
    );
    assert!(
        expired_runtime
            .resume_at("expired", SystemTime::now())
            .await
            .unwrap()
            .is_failure()
    );
}

#[tokio::test]
async fn named_transition_persists_the_selected_branch_and_executes_only_that_handler() {
    let selected = Arc::new(AtomicUsize::new(0));
    let rejected = Arc::new(AtomicUsize::new(0));
    let definition = FlowDefinition::new("payment")
        .step("choose", |_| async {
            Ok(FlowStepOutcome::goto("accepted"))
        })
        .step("rejected", {
            let rejected = Arc::clone(&rejected);
            move |_| {
                let rejected = Arc::clone(&rejected);
                async move {
                    rejected.fetch_add(1, Ordering::SeqCst);
                    Ok(FlowStepOutcome::complete())
                }
            }
        })
        .step("accepted", {
            let selected = Arc::clone(&selected);
            move |_| {
                let selected = Arc::clone(&selected);
                async move {
                    selected.fetch_add(1, Ordering::SeqCst);
                    Ok(FlowStepOutcome::complete())
                }
            }
        });
    let runtime = FlowRuntime::new(
        Arc::new(MemorySuspendedFlows::default()),
        Arc::new(MemoryFlowScheduler::default()),
        definition,
        "node-a",
    );

    assert!(
        runtime
            .start("branch", b"input".to_vec())
            .await
            .unwrap()
            .is_success()
    );
    assert_eq!(selected.load(Ordering::SeqCst), 1);
    assert_eq!(rejected.load(Ordering::SeqCst), 0);
}
