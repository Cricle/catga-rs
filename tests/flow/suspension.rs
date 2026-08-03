//! Flow suspension integration helpers.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_core::flow::{
    FlowChildLauncher, FlowCompletion, FlowCompletionAdapter, FlowContinuation, FlowDefinition,
    FlowQuery, FlowRuntime, FlowState, FlowStepOutcome, FlowSummary, FlowTagPolicy,
    FlowTimeoutOptions, FlowTimeoutService, MAX_FLOW_TIMEOUT_BATCH_SIZE,
    MAX_FLOW_TIMEOUT_SCAN_LIMIT, MAX_WAIT_CHILDREN, MAX_WAIT_RESULT_BYTES, MemoryFlowScheduler,
    SuspendedFlowStore, TimedOutFlowPoll, TimedOutFlowReceipt, TimedOutFlowStore, WaitCondition,
    WaitPolicy,
};
use catga_memory::MemorySuspendedFlows;
use tokio_util::sync::CancellationToken;

mod timeout_store_contract;

#[tokio::test]
async fn cancellation_wins_over_a_running_step_and_fences_its_late_completion() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let step_calls = Arc::new(AtomicUsize::new(0));
    let runtime = Arc::new(FlowRuntime::new(
        Arc::clone(&store),
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("cancel-race")
            .step("external-effect", {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                let step_calls = Arc::clone(&step_calls);
                move |_| {
                    let entered = Arc::clone(&entered);
                    let release = Arc::clone(&release);
                    let step_calls = Arc::clone(&step_calls);
                    async move {
                        step_calls.fetch_add(1, Ordering::SeqCst);
                        entered.notify_one();
                        release.notified().await;
                        Ok(FlowStepOutcome::Advance)
                    }
                }
            })
            .step("must-not-run", |_| async {
                Ok(FlowStepOutcome::complete())
            }),
        "node-a",
    ));

    let running = tokio::spawn({
        let runtime = Arc::clone(&runtime);
        async move { runtime.start("cancel-race/1", []).await }
    });
    entered.notified().await;

    assert!(
        runtime
            .cancel("cancel-race/1")
            .await
            .expect("cancellation persists a terminal state")
            .is_cancelled()
    );
    release.notify_one();

    assert!(
        running
            .await
            .expect("running task does not panic")
            .expect("late step completion resolves to the durable state")
            .is_cancelled()
    );
    let persisted = store
        .get("cancel-race/1")
        .await
        .expect("memory store remains available")
        .expect("cancelled continuation remains durable");
    assert_eq!(
        persisted.state().status(),
        catga_core::flow::FlowStatus::Cancelled
    );
    assert_eq!(step_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn durable_runtime_rejects_duplicate_step_names_before_persisting_or_running() {
    let invocations = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(MemorySuspendedFlows::default());
    let first_invocations = Arc::clone(&invocations);
    let second_invocations = Arc::clone(&invocations);
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("duplicate-step-names")
            .step("work", move |_| {
                let invocations = Arc::clone(&first_invocations);
                async move {
                    invocations.fetch_add(1, Ordering::Relaxed);
                    Ok(FlowStepOutcome::Advance)
                }
            })
            .step("work", move |_| {
                let invocations = Arc::clone(&second_invocations);
                async move {
                    invocations.fetch_add(1, Ordering::Relaxed);
                    Ok(FlowStepOutcome::complete())
                }
            }),
        "node-a",
    );

    let error = runtime
        .start("duplicate-step-names/1", [])
        .await
        .expect_err("duplicate durable step names must be rejected before execution");

    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(invocations.load(Ordering::Relaxed), 0);
    assert!(
        store
            .get("duplicate-step-names/1")
            .await
            .expect("memory store remains available")
            .is_none()
    );
}

#[tokio::test]
async fn durable_runtime_completes_when_the_final_sequential_step_advances() {
    let invocations = Arc::new(AtomicUsize::new(0));
    let first_invocations = Arc::clone(&invocations);
    let final_invocations = Arc::clone(&invocations);
    let runtime = FlowRuntime::new(
        Arc::new(MemorySuspendedFlows::default()),
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("implicit-completion")
            .step("first", move |_| {
                let invocations = Arc::clone(&first_invocations);
                async move {
                    invocations.fetch_add(1, Ordering::Relaxed);
                    Ok(FlowStepOutcome::Advance)
                }
            })
            .step("final", move |_| {
                let invocations = Arc::clone(&final_invocations);
                async move {
                    invocations.fetch_add(1, Ordering::Relaxed);
                    Ok(FlowStepOutcome::Advance)
                }
            }),
        "node-a",
    );

    let result = runtime
        .start("implicit-completion/1", [])
        .await
        .expect("final advance must complete the durable flow");

    assert!(result.is_success());
    assert_eq!(invocations.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn durable_runtime_rejects_a_suspension_after_the_completed_step_counter_is_exhausted() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("step-counter-exhaustion")
            .step("wait", |_| async {
                Ok(FlowStepOutcome::wait(WaitCondition::new(
                    "step-counter-exhaustion-wait",
                    WaitPolicy::All,
                    1,
                    SystemTime::now(),
                    Duration::from_secs(30),
                )))
            })
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    );
    assert!(
        store
            .create(FlowContinuation::new(
                FlowState::new(
                    "step-counter-exhaustion/1",
                    "step-counter-exhaustion",
                    [],
                    "node-a",
                )
                .at_step(u32::MAX)
                .suspended(),
                "wait",
            ))
            .await
            .expect("exhausted state remains representable for validation")
    );

    let error = runtime
        .resume("step-counter-exhaustion/1")
        .await
        .expect_err("a durable transition must not reuse u32::MAX as the next completed step");

    assert_eq!(error.code(), ErrorCode::Internal);
    assert_eq!(
        store
            .get("step-counter-exhaustion/1")
            .await
            .expect("memory store remains available")
            .expect("original continuation remains durable")
            .state()
            .step(),
        u32::MAX
    );
}

#[tokio::test]
async fn durable_runtime_rejects_waits_without_a_correlation_or_expected_children() {
    for (suffix, correlation_id, expected_count) in
        [("empty-correlation", "", 1), ("zero-count", "wait", 0)]
    {
        let store = Arc::new(MemorySuspendedFlows::default());
        let runtime = FlowRuntime::new(
            Arc::clone(&store),
            Arc::new(MemoryFlowScheduler::default()),
            FlowDefinition::new(format!("invalid-wait-{suffix}"))
                .step("wait", move |_| async move {
                    Ok(FlowStepOutcome::wait(WaitCondition::new(
                        correlation_id,
                        WaitPolicy::All,
                        expected_count,
                        SystemTime::now(),
                        Duration::from_secs(30),
                    )))
                })
                .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
            "node-a",
        );

        let result = runtime
            .start(format!("invalid-wait-{suffix}/1"), [])
            .await
            .expect("invalid wait must become a terminal flow failure");

        assert!(result.is_failure());
        assert_eq!(
            result.state().error().map(CatgaError::code),
            Some(ErrorCode::Validation)
        );
        assert!(
            store
                .get(result.state().id())
                .await
                .expect("memory store remains available")
                .is_some_and(|continuation| continuation.wait().is_none())
        );
    }
}

#[tokio::test]
async fn tagged_durable_step_retries_only_transient_failures_within_its_bound() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let runtime = FlowRuntime::new(
        Arc::new(MemorySuspendedFlows::default()),
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("tagged-retry").step_with_tag("request", "remote", {
            let attempts = Arc::clone(&attempts);
            move |_| {
                let attempts = Arc::clone(&attempts);
                async move {
                    if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                        Err(CatgaError::new(
                            ErrorCode::Transient,
                            "upstream unavailable",
                        ))
                    } else {
                        Ok(FlowStepOutcome::complete())
                    }
                }
            }
        }),
        "node-a",
    )
    .with_tag_policy(FlowTagPolicy::new(Duration::from_secs(1), 2));

    assert!(
        runtime
            .start("tagged-retry-1", b"input".to_vec())
            .await
            .unwrap()
            .is_success()
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn tagged_durable_step_timeout_returns_a_structured_timeout_without_background_work() {
    let runtime = FlowRuntime::new(
        Arc::new(MemorySuspendedFlows::default()),
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("tagged-timeout").step_with_tag("request", "remote", |_| async {
            std::future::pending::<CatgaResult<FlowStepOutcome>>().await
        }),
        "node-a",
    )
    .with_tag_policy(FlowTagPolicy::new(Duration::from_millis(1), 0));

    let result = runtime
        .start("tagged-timeout-1", b"input".to_vec())
        .await
        .unwrap();
    assert!(result.is_failure());
    assert_eq!(result.state().error().unwrap().code(), ErrorCode::Timeout);
}

#[tokio::test]
async fn tagged_durable_step_does_not_retry_a_non_transient_failure() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let runtime = FlowRuntime::new(
        Arc::new(MemorySuspendedFlows::default()),
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("tagged-validation").step_with_tag("request", "remote", {
            let attempts = Arc::clone(&attempts);
            move |_| {
                let attempts = Arc::clone(&attempts);
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(CatgaError::new(ErrorCode::Validation, "request is invalid"))
                }
            }
        }),
        "node-a",
    )
    .with_tag_policy(FlowTagPolicy::new(Duration::from_secs(1), 3));

    let result = runtime
        .start("tagged-validation-1", b"input".to_vec())
        .await
        .unwrap();
    assert!(result.is_failure());
    assert_eq!(
        result.state().error().unwrap().code(),
        ErrorCode::Validation
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

type ChildLaunchCall = (Box<str>, Box<str>, Box<str>);

#[derive(Default)]
struct RecordingChildLauncher {
    calls: std::sync::Mutex<Vec<ChildLaunchCall>>,
}

struct RecordThenPanicLauncher {
    child_ids: Arc<std::sync::Mutex<Vec<Box<str>>>>,
}

#[async_trait]
impl FlowChildLauncher for RecordThenPanicLauncher {
    async fn launch(&self, _: &str, child_id: &str, _: &str) -> CatgaResult<()> {
        self.child_ids.lock().unwrap().push(child_id.into());
        panic!("simulated process crash after accepting the child launch");
    }
}

struct RecordingOneChildLauncher {
    child_ids: Arc<std::sync::Mutex<Vec<Box<str>>>>,
}

#[async_trait]
impl FlowChildLauncher for RecordingOneChildLauncher {
    async fn launch(&self, _: &str, child_id: &str, _: &str) -> CatgaResult<()> {
        self.child_ids.lock().unwrap().push(child_id.into());
        Ok(())
    }
}

#[async_trait]
impl FlowChildLauncher for RecordingChildLauncher {
    async fn launch(
        &self,
        parent_flow_id: &str,
        child_id: &str,
        correlation_id: &str,
    ) -> CatgaResult<()> {
        self.calls.lock().unwrap().push((
            parent_flow_id.into(),
            child_id.into(),
            correlation_id.into(),
        ));
        Ok(())
    }
}

#[tokio::test]
async fn durable_child_fan_out_launches_each_stable_child_once_and_rejects_unknown_results() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let wait = WaitCondition::for_children(
        "parent-wait",
        WaitPolicy::All,
        ["child-a", "child-b"],
        SystemTime::now(),
        Duration::from_secs(30),
    )
    .unwrap();
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        scheduler,
        FlowDefinition::new("parent")
            .step("wait", move |_| {
                let wait = wait.clone();
                async move { Ok(FlowStepOutcome::wait(wait)) }
            })
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    );
    let launcher = RecordingChildLauncher::default();

    assert!(
        runtime
            .start("parent-1", b"input".to_vec())
            .await
            .unwrap()
            .is_suspended()
    );
    assert_eq!(
        runtime
            .launch_waiting_children("parent-1", &launcher)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        runtime
            .launch_waiting_children("parent-1", &launcher)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        launcher.calls.lock().unwrap().as_slice(),
        [
            ("parent-1".into(), "child-a".into(), "parent-wait".into()),
            ("parent-1".into(), "child-b".into(), "parent-wait".into()),
        ]
    );

    assert_eq!(
        runtime
            .record_wait_success("parent-1", "unknown-child", b"ignored".to_vec())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    assert!(
        runtime
            .record_wait_success("parent-1", "child-a", b"a".to_vec())
            .await
            .unwrap()
            .is_suspended()
    );
    assert!(
        runtime
            .record_wait_success("parent-1", "child-a", b"duplicate".to_vec())
            .await
            .unwrap()
            .is_suspended()
    );
    assert!(
        runtime
            .record_wait_success("parent-1", "child-b", b"b".to_vec())
            .await
            .unwrap()
            .is_success()
    );
}

#[tokio::test]
async fn durable_child_completion_resumes_the_parent_by_correlation_id() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let wait = WaitCondition::for_children(
        "parent-correlation",
        WaitPolicy::All,
        ["child-a", "child-b"],
        SystemTime::now(),
        Duration::from_secs(30),
    )
    .expect("bounded child wait is valid");
    let runtime = FlowRuntime::new(
        store,
        scheduler,
        FlowDefinition::new("parent")
            .step("wait", move |_| {
                let wait = wait.clone();
                async move { Ok(FlowStepOutcome::wait(wait)) }
            })
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    );

    assert!(
        runtime
            .start("parent-by-correlation", [])
            .await
            .expect("parent starts")
            .is_suspended()
    );
    assert!(
        runtime
            .record_wait_success_by_correlation("parent-correlation", "child-a", b"first".to_vec(),)
            .await
            .expect("first child completion is accepted")
            .is_suspended()
    );
    assert!(runtime
        .record_wait_success_by_correlation(
            "parent-correlation",
            "child-b",
            b"second".to_vec(),
        )
        .await
        .expect("second child completion resumes the parent")
        .is_success());
}

#[tokio::test]
async fn durable_child_failure_fails_the_parent_by_correlation_id() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let wait = WaitCondition::for_children(
        "parent-failure-correlation",
        WaitPolicy::All,
        ["child-a"],
        SystemTime::now(),
        Duration::from_secs(30),
    )
    .expect("bounded child wait is valid");
    let runtime = FlowRuntime::new(
        store,
        scheduler,
        FlowDefinition::new("parent-failure")
            .step("wait", move |_| {
                let wait = wait.clone();
                async move { Ok(FlowStepOutcome::wait(wait)) }
            })
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    );

    assert!(
        runtime
            .start("parent-failure-by-correlation", [])
            .await
            .expect("parent starts")
            .is_suspended()
    );
    assert!(
        runtime
            .record_wait_failure_by_correlation(
                "parent-failure-correlation",
                "child-a",
                CatgaError::new(ErrorCode::Transient, "child failed"),
            )
            .await
            .expect("child failure is accepted")
            .is_failure()
    );
}

#[tokio::test]
async fn flow_completion_adapter_records_successes_by_correlation() {
    let wait = WaitCondition::for_children(
        "adapter-success-correlation",
        WaitPolicy::All,
        ["child-a", "child-b"],
        SystemTime::now(),
        Duration::from_secs(30),
    )
    .expect("bounded child wait is valid");
    let runtime = Arc::new(FlowRuntime::new(
        Arc::new(MemorySuspendedFlows::default()),
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("adapter-success")
            .step("wait", move |_| {
                let wait = wait.clone();
                async move { Ok(FlowStepOutcome::wait(wait)) }
            })
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    ));
    let adapter = FlowCompletionAdapter::new(Arc::clone(&runtime));

    assert!(
        runtime
            .start("adapter-success/1", [])
            .await
            .expect("parent starts")
            .is_suspended()
    );

    assert!(
        adapter
            .record(FlowCompletion::success(
                "adapter-success-correlation",
                "child-a",
                b"first".to_vec(),
            ))
            .await
            .expect("first completion is accepted")
            .is_suspended()
    );
    assert!(
        adapter
            .record(FlowCompletion::success(
                "adapter-success-correlation",
                "child-b",
                b"second".to_vec(),
            ))
            .await
            .expect("final completion resumes the parent")
            .is_success()
    );
}

#[tokio::test]
async fn flow_completion_adapter_records_failures_by_correlation() {
    let wait = WaitCondition::for_children(
        "adapter-failure-correlation",
        WaitPolicy::All,
        ["child-a"],
        SystemTime::now(),
        Duration::from_secs(30),
    )
    .expect("bounded child wait is valid");
    let runtime = Arc::new(FlowRuntime::new(
        Arc::new(MemorySuspendedFlows::default()),
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("adapter-failure")
            .step("wait", move |_| {
                let wait = wait.clone();
                async move { Ok(FlowStepOutcome::wait(wait)) }
            })
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    ));
    let adapter = FlowCompletionAdapter::new(Arc::clone(&runtime));

    assert!(
        runtime
            .start("adapter-failure/1", [])
            .await
            .expect("parent starts")
            .is_suspended()
    );

    let result = adapter
        .record(FlowCompletion::failure(
            "adapter-failure-correlation",
            "child-a",
            CatgaError::new(ErrorCode::Transient, "child failed"),
        ))
        .await
        .expect("child failure is accepted");

    assert!(result.is_failure());
    let persisted = result
        .state()
        .error()
        .expect("failed parent retains the supplied child error");
    assert_eq!(persisted.code(), ErrorCode::Transient);
    assert_eq!(persisted.message(), "child failed");
}

#[tokio::test]
async fn flow_completion_adapter_retains_duplicate_completion_outcomes() {
    let wait = WaitCondition::for_children(
        "adapter-duplicate-correlation",
        WaitPolicy::All,
        ["child-a", "child-b"],
        SystemTime::now(),
        Duration::from_secs(30),
    )
    .expect("bounded child wait is valid");
    let runtime = Arc::new(FlowRuntime::new(
        Arc::new(MemorySuspendedFlows::default()),
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("adapter-duplicate")
            .step("wait", move |_| {
                let wait = wait.clone();
                async move { Ok(FlowStepOutcome::wait(wait)) }
            })
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    ));
    let adapter = FlowCompletionAdapter::new(Arc::clone(&runtime));

    assert!(
        runtime
            .start("adapter-duplicate/1", [])
            .await
            .expect("parent starts")
            .is_suspended()
    );

    for payload in [b"first".as_slice(), b"duplicate-first".as_slice()] {
        assert!(
            adapter
                .record(FlowCompletion::success(
                    "adapter-duplicate-correlation",
                    "child-a",
                    payload.to_vec(),
                ))
                .await
                .expect("duplicate pending completion is accepted idempotently")
                .is_suspended()
        );
    }
    assert!(
        adapter
            .record(FlowCompletion::success(
                "adapter-duplicate-correlation",
                "child-b",
                b"second".to_vec(),
            ))
            .await
            .expect("final completion resumes the parent")
            .is_success()
    );
}

#[tokio::test]
async fn flow_completion_adapter_rejects_oversized_success_payloads() {
    let wait = WaitCondition::for_children(
        "adapter-oversized-correlation",
        WaitPolicy::All,
        ["child-a"],
        SystemTime::now(),
        Duration::from_secs(30),
    )
    .expect("bounded child wait is valid");
    let runtime = Arc::new(FlowRuntime::new(
        Arc::new(MemorySuspendedFlows::default()),
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("adapter-oversized")
            .step("wait", move |_| {
                let wait = wait.clone();
                async move { Ok(FlowStepOutcome::wait(wait)) }
            })
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    ));
    let adapter = FlowCompletionAdapter::new(Arc::clone(&runtime));

    assert!(
        runtime
            .start("adapter-oversized/1", [])
            .await
            .expect("parent starts")
            .is_suspended()
    );
    assert_eq!(
        adapter
            .record(FlowCompletion::success(
                "adapter-oversized-correlation",
                "child-a",
                vec![0; MAX_WAIT_RESULT_BYTES + 1],
            ))
            .await
            .expect_err("oversized child payload is rejected")
            .code(),
        ErrorCode::Validation
    );
}

#[tokio::test]
async fn flow_completion_adapter_rejects_unknown_correlations() {
    let runtime = Arc::new(FlowRuntime::new(
        Arc::new(MemorySuspendedFlows::default()),
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("adapter-unknown")
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    ));
    let adapter = FlowCompletionAdapter::new(runtime);

    assert_eq!(
        adapter
            .record(FlowCompletion::success(
                "unknown-correlation",
                "child-a",
                b"result".to_vec(),
            ))
            .await
            .expect_err("unknown correlations must be rejected")
            .code(),
        ErrorCode::NotFound
    );
}

#[tokio::test]
async fn correlated_child_completion_rejects_ambiguous_parent_waits() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let wait = WaitCondition::new(
        "shared-parent-correlation",
        WaitPolicy::All,
        1,
        SystemTime::now(),
        Duration::from_secs(30),
    );
    let runtime = FlowRuntime::new(
        store,
        scheduler,
        FlowDefinition::new("ambiguous-parent")
            .step("wait", move |_| {
                let wait = wait.clone();
                async move { Ok(FlowStepOutcome::wait(wait)) }
            })
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    );

    for flow_id in ["ambiguous-parent/1", "ambiguous-parent/2"] {
        assert!(
            runtime
                .start(flow_id, [])
                .await
                .expect("parent starts")
                .is_suspended()
        );
    }

    assert_eq!(
        runtime
            .record_wait_success_by_correlation(
                "shared-parent-correlation",
                "child-a",
                b"completion".to_vec(),
            )
            .await
            .expect_err("ambiguous correlation must not choose a parent")
            .code(),
        ErrorCode::Conflict
    );
}

#[tokio::test]
async fn durable_child_launch_recovers_an_expired_claim_with_the_same_child_identity() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let wait = WaitCondition::for_children(
        "crash-wait",
        WaitPolicy::All,
        ["stable-child"],
        SystemTime::now(),
        Duration::from_secs(30),
    )
    .unwrap();
    let runtime = Arc::new(
        FlowRuntime::new(
            store,
            scheduler,
            FlowDefinition::new("crash-parent")
                .step("wait", move |_| {
                    let wait = wait.clone();
                    async move { Ok(FlowStepOutcome::wait(wait)) }
                })
                .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
            "node-a",
        )
        .with_stale_after(Duration::ZERO),
    );
    let child_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
    let crashing = RecordThenPanicLauncher {
        child_ids: Arc::clone(&child_ids),
    };
    let recovering = RecordingOneChildLauncher {
        child_ids: Arc::clone(&child_ids),
    };

    assert!(
        runtime
            .start("crash-parent-1", b"input".to_vec())
            .await
            .unwrap()
            .is_suspended()
    );
    let crashed = tokio::spawn({
        let runtime = Arc::clone(&runtime);
        async move {
            runtime
                .launch_waiting_children("crash-parent-1", &crashing)
                .await
        }
    })
    .await;
    assert!(crashed.is_err());

    assert_eq!(
        runtime
            .launch_waiting_children("crash-parent-1", &recovering)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        child_ids.lock().unwrap().as_slice(),
        [
            Box::<str>::from("stable-child"),
            Box::<str>::from("stable-child")
        ]
    );
}

#[tokio::test]
async fn durable_child_wait_rejects_excess_children_and_oversized_results_before_retention() {
    let too_many = (0..=MAX_WAIT_CHILDREN).map(|index| format!("child-{index}"));
    assert_eq!(
        WaitCondition::for_children(
            "too-many",
            WaitPolicy::All,
            too_many,
            SystemTime::now(),
            Duration::from_secs(30),
        )
        .unwrap_err()
        .code(),
        ErrorCode::Validation
    );

    let store = Arc::new(MemorySuspendedFlows::default());
    let wait = WaitCondition::for_children(
        "bounded-result",
        WaitPolicy::All,
        ["child"],
        SystemTime::now(),
        Duration::from_secs(30),
    )
    .unwrap();
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("bounded-result")
            .step("wait", move |_| {
                let wait = wait.clone();
                async move { Ok(FlowStepOutcome::wait(wait)) }
            })
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    );
    runtime
        .start("bounded-result", b"input".to_vec())
        .await
        .unwrap();

    assert_eq!(
        runtime
            .record_wait_success(
                "bounded-result",
                "child",
                vec![0; MAX_WAIT_RESULT_BYTES + 1]
            )
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
}

struct ObservingTimeoutStore {
    inner: MemorySuspendedFlows,
    receipts: Vec<TimedOutFlowReceipt>,
    fail_ack: bool,
    fail_release: bool,
    released: tokio::sync::Mutex<Vec<Box<str>>>,
}

struct ScheduleIdentityWriteFailureStore {
    inner: Arc<MemorySuspendedFlows>,
    fail_next_schedule_identity_write: AtomicBool,
}

#[async_trait]
impl SuspendedFlowStore for ScheduleIdentityWriteFailureStore {
    async fn create(&self, continuation: FlowContinuation) -> CatgaResult<bool> {
        self.inner.create(continuation).await
    }

    async fn get(&self, flow_id: &str) -> CatgaResult<Option<FlowContinuation>> {
        self.inner.get(flow_id).await
    }

    async fn query(&self, query: &FlowQuery) -> CatgaResult<Vec<FlowSummary>> {
        self.inner.query(query).await
    }

    async fn update(&self, expected_version: i64, next: FlowContinuation) -> CatgaResult<bool> {
        if next.schedule_id().is_some()
            && self
                .fail_next_schedule_identity_write
                .swap(false, Ordering::SeqCst)
        {
            return Err(CatgaError::new(
                ErrorCode::Transient,
                "simulated schedule identity persistence failure",
            ));
        }
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
async fn flow_summary_exposes_the_latest_durable_continuation_update() {
    let store = MemorySuspendedFlows::default();
    let continuation = FlowContinuation::waiting(
        FlowState::new(
            "flow-summary-update",
            "payment",
            b"input".to_vec(),
            "node-a",
        ),
        "charge",
        WaitCondition::new(
            "wait-summary-update",
            WaitPolicy::All,
            1,
            SystemTime::now(),
            Duration::from_secs(30),
        ),
    );
    let created_at = continuation.created_at();
    assert!(store.create(continuation).await.unwrap());

    tokio::time::sleep(Duration::from_millis(1)).await;
    assert!(
        store
            .record_wait_success("flow-summary-update", 0, "child-a", b"ok".to_vec())
            .await
            .unwrap()
    );

    let summaries = store.query(&FlowQuery::new(1, 1).unwrap()).await.unwrap();
    assert_eq!(summaries.len(), 1);
    assert!(summaries[0].updated_at() > created_at);
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
async fn delayed_schedule_identity_write_failure_is_reconciled_without_duplicate_jobs()
-> CatgaResult<()> {
    let store = Arc::new(ScheduleIdentityWriteFailureStore {
        inner: Arc::new(MemorySuspendedFlows::default()),
        fail_next_schedule_identity_write: AtomicBool::new(true),
    });
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let due_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        FlowDefinition::new("schedule-reconciliation")
            .step("delay", move |_| async move {
                Ok(FlowStepOutcome::suspend_until(due_at))
            })
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    );

    assert!(
        runtime
            .start("schedule-reconciliation/1", [])
            .await?
            .is_suspended()
    );
    let persisted = store.get("schedule-reconciliation/1").await?;
    assert!(persisted.is_some_and(|continuation| continuation.schedule_id().is_none()));

    assert_eq!(runtime.reconcile_delayed_suspensions(1, 1).await?, 1);
    assert_eq!(runtime.reconcile_delayed_suspensions(1, 1).await?, 0);
    assert_eq!(scheduler.take_due(due_at).len(), 1);
    Ok(())
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
