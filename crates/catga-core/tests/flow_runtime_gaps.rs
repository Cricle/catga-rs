//! Gap-filling contracts for the durable [`FlowRuntime`]: goto transitions,
//! child launch fan-out, wait-correlation completions, schedule
//! reconciliation, heartbeat/renewal loops, and version-race fallbacks that
//! the main contract suite does not reach.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use catga_core::flow::definition::{FlowDefinition, FlowStepOutcome};
use catga_core::flow::{
    FlowRuntime, FlowStatus, FlowTagPolicy, MAX_WAIT_RESULT_BYTES, WaitCondition, WaitPolicy,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use tokio::sync::{Barrier, Notify};

#[path = "support/flow_runtime_support.rs"]
mod runtime_support;

use runtime_support::{CancelFailingScheduler, FlakyScheduler, GapFlowStore, RecordingLauncher};

fn waiting_definition(name: &str, correlation: &str, expected: u32) -> FlowDefinition {
    let correlation: Box<str> = correlation.into();
    FlowDefinition::new(name)
        .step("await", move |_state| {
            let correlation = correlation.clone();
            async move {
                Ok(FlowStepOutcome::wait(WaitCondition::new(
                    correlation,
                    WaitPolicy::All,
                    expected,
                    SystemTime::now(),
                    Duration::from_secs(3_600),
                )))
            }
        })
        .step("finish", |_state| async { Ok(FlowStepOutcome::complete()) })
}

fn delayed_definition(name: &str, due: SystemTime) -> FlowDefinition {
    FlowDefinition::new(name)
        .step("begin", move |_state| async move {
            Ok(FlowStepOutcome::suspend_until(due))
        })
        .step("finish", |_state| async { Ok(FlowStepOutcome::complete()) })
}

#[tokio::test]
async fn compensating_takeover_reports_interim_state_and_finishes_rollback() -> CatgaResult<()> {
    let store = Arc::new(GapFlowStore::default());
    let scheduler = Arc::new(catga_core::flow::MemoryFlowScheduler::default());
    let started = Arc::new(Notify::new());
    let release = Arc::new(Barrier::new(3));
    let log = Arc::new(Mutex::new(Vec::<&'static str>::new()));

    let build_definition = || {
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        let log = Arc::clone(&log);
        FlowDefinition::new("blocking-saga")
            .step_with_compensation(
                "reserve",
                |_state| async { Ok(FlowStepOutcome::Advance) },
                move |_state| {
                    let started = Arc::clone(&started);
                    let release = Arc::clone(&release);
                    let log = Arc::clone(&log);
                    async move {
                        log.lock().expect("log lock").push("compensate:reserve");
                        started.notify_one();
                        release.wait().await;
                        Ok(())
                    }
                },
            )
            .step("boom", |_state| async {
                Err(CatgaError::new(ErrorCode::Internal, "forward failed"))
            })
    };

    let owner_a = Arc::new(
        FlowRuntime::new(
            Arc::clone(&store),
            Arc::clone(&scheduler),
            build_definition(),
            "worker-a",
        )
        .with_stale_after(Duration::ZERO),
    );
    let owner_b = FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        build_definition(),
        "worker-b",
    )
    .with_stale_after(Duration::from_millis(1));

    let task_runtime = Arc::clone(&owner_a);
    let original =
        tokio::spawn(async move { task_runtime.start("flow-blocked", Vec::new()).await });
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .expect("the compensation starts");
    assert_eq!(
        store.continuation("flow-blocked").state().status(),
        FlowStatus::Compensating
    );

    // An interim resume with a fresh ownership window reports the compensating
    // status without re-claiming the flow.
    let observer = FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        build_definition(),
        "worker-c",
    )
    .with_stale_after(Duration::from_secs(60));
    let interim = observer.resume("flow-blocked").await?;
    assert!(interim.is_compensating());
    assert!(!interim.is_success() && !interim.is_failure() && !interim.is_cancelled());

    // A stale-owner takeover claims the compensating flow and drives rollback.
    let takeover = tokio::spawn(async move {
        owner_b
            .resume_at("flow-blocked", SystemTime::now() + Duration::from_secs(5))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .expect("the takeover re-executes the compensation");

    release.wait().await;
    let original_result = tokio::time::timeout(Duration::from_secs(5), original)
        .await
        .expect("the original execution completes")
        .expect("the original execution does not panic")?;
    let takeover_result = tokio::time::timeout(Duration::from_secs(5), takeover)
        .await
        .expect("the takeover completes")
        .expect("the takeover does not panic")?;
    assert!(takeover_result.is_failure());
    assert!(
        !original_result.is_success(),
        "the stale original owner cannot report success"
    );
    assert_eq!(
        store.continuation("flow-blocked").state().status(),
        FlowStatus::Failed
    );
    assert_eq!(
        log.lock().expect("log lock").len(),
        2,
        "rollback is at-least-once across the takeover"
    );
    Ok(())
}

#[tokio::test]
async fn resume_scheduled_and_cancel_report_not_found_for_unknown_flows() {
    let store = Arc::new(GapFlowStore::default());
    let scheduler = Arc::new(catga_core::flow::MemoryFlowScheduler::default());
    let definition = FlowDefinition::new("known")
        .step("only", |_state| async { Ok(FlowStepOutcome::complete()) });
    let runtime = FlowRuntime::new(Arc::clone(&store), scheduler, definition, "worker-a")
        .with_stale_after(Duration::ZERO);

    for error in [
        runtime
            .resume_scheduled("ghost", "only")
            .await
            .expect_err("an unknown flow cannot resume on schedule"),
        runtime
            .cancel("ghost")
            .await
            .expect_err("an unknown flow cannot be cancelled"),
    ] {
        assert_eq!(error.code(), ErrorCode::NotFound);
    }
}

#[tokio::test]
async fn cancel_keeps_the_cancellation_when_the_schedule_cannot_be_cancelled() -> CatgaResult<()> {
    let store = Arc::new(GapFlowStore::default());
    let scheduler = Arc::new(CancelFailingScheduler::default());
    let due = SystemTime::now() + Duration::from_secs(3_600);
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        scheduler,
        delayed_definition("delayed", due),
        "worker-a",
    )
    .with_stale_after(Duration::ZERO);
    assert!(
        runtime
            .start("flow-delayed", Vec::new())
            .await?
            .is_suspended()
    );

    let cancelled = runtime.cancel("flow-delayed").await?;
    assert!(
        cancelled.is_cancelled(),
        "the cancellation persists even when the scheduler cannot cancel the resume"
    );
    Ok(())
}

#[tokio::test]
async fn cancel_and_resume_report_current_state_when_races_are_lost() -> CatgaResult<()> {
    let store = Arc::new(GapFlowStore::default());
    let scheduler = Arc::new(catga_core::flow::MemoryFlowScheduler::default());
    let due = SystemTime::now() + Duration::from_secs(3_600);
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        scheduler,
        delayed_definition("racy", due),
        "worker-a",
    )
    .with_stale_after(Duration::ZERO);
    assert!(runtime.start("flow-racy", Vec::new()).await?.is_suspended());

    store.fail_next_update.store(true, Ordering::SeqCst);
    let cancel = runtime.cancel("flow-racy").await?;
    assert!(
        cancel.is_suspended(),
        "a lost cancellation update reports the current durable state"
    );

    store.fail_next_claim.store(true, Ordering::SeqCst);
    let resumed = runtime
        .resume_at("flow-racy", due + Duration::from_secs(1))
        .await?;
    assert!(
        resumed.is_suspended(),
        "a lost claim reports the current durable state"
    );
    Ok(())
}

#[tokio::test]
async fn resume_reports_pending_waits_without_reexecuting() -> CatgaResult<()> {
    let store = Arc::new(GapFlowStore::default());
    let scheduler = Arc::new(catga_core::flow::MemoryFlowScheduler::default());
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        scheduler,
        waiting_definition("pending-join", "corr-pending", 2),
        "worker-a",
    )
    .with_stale_after(Duration::ZERO);
    assert!(
        runtime
            .start("flow-pending", Vec::new())
            .await?
            .is_suspended()
    );

    let resumed = runtime.resume("flow-pending").await?;
    assert!(
        resumed.is_suspended(),
        "an unsatisfied wait stays suspended on resume"
    );

    let recorded = runtime
        .record_wait_success("flow-pending", "child-1", vec![1_u8])
        .await?;
    assert!(
        recorded.is_suspended(),
        "one of two expected children does not resume the flow"
    );
    let done = runtime
        .record_wait_success("flow-pending", "child-2", vec![2_u8])
        .await?;
    assert!(done.is_success());
    Ok(())
}

#[tokio::test]
async fn record_wait_success_rejects_unknown_terminal_and_over_limit_completions() -> CatgaResult<()>
{
    let store = Arc::new(GapFlowStore::default());
    let scheduler = Arc::new(catga_core::flow::MemoryFlowScheduler::default());
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        scheduler,
        waiting_definition("bounds", "corr-bounds", 1),
        "worker-a",
    )
    .with_stale_after(Duration::ZERO);

    let error = runtime
        .record_wait_success("ghost", "child-1", Vec::new())
        .await
        .expect_err("an unknown flow cannot record child results");
    assert_eq!(error.code(), ErrorCode::NotFound);

    let done_definition = FlowDefinition::new("done-flow")
        .step("only", |_state| async { Ok(FlowStepOutcome::complete()) });
    let done_runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::new(catga_core::flow::MemoryFlowScheduler::default()),
        done_definition,
        "worker-a",
    )
    .with_stale_after(Duration::ZERO);
    assert!(
        done_runtime
            .start("flow-done", Vec::new())
            .await?
            .is_success()
    );
    let terminal = done_runtime
        .record_wait_success("flow-done", "child-1", Vec::new())
        .await?;
    assert!(
        terminal.is_success(),
        "a terminal flow reports its terminal state"
    );

    assert!(
        runtime
            .start("flow-bounds", Vec::new())
            .await?
            .is_suspended()
    );
    let error = runtime
        .record_wait_success(
            "flow-bounds",
            "child-1",
            vec![0_u8; MAX_WAIT_RESULT_BYTES + 1],
        )
        .await
        .expect_err("an over-limit payload is rejected");
    assert_eq!(error.code(), ErrorCode::Validation);
    Ok(())
}

#[tokio::test]
async fn record_wait_success_reconciles_a_lost_acknowledgement() -> CatgaResult<()> {
    // A ready wait: the lost ack is reconciled by resuming the now-ready flow.
    let store = Arc::new(GapFlowStore::default());
    let scheduler = Arc::new(catga_core::flow::MemoryFlowScheduler::default());
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        scheduler,
        waiting_definition("lost-ack", "corr-lost-ack", 1),
        "worker-a",
    )
    .with_stale_after(Duration::ZERO);
    assert!(
        runtime
            .start("flow-lost-ack", Vec::new())
            .await?
            .is_suspended()
    );

    store.drop_wait_ack.store(true, Ordering::SeqCst);
    let resumed = runtime
        .record_wait_success("flow-lost-ack", "child-1", vec![7_u8])
        .await?;
    store.drop_wait_ack.store(false, Ordering::SeqCst);
    assert!(
        resumed.is_success(),
        "a durably recorded ready wait still resumes the flow"
    );

    // A still-pending wait: the lost ack reports the current suspended state.
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::new(catga_core::flow::MemoryFlowScheduler::default()),
        waiting_definition("lost-ack-pending", "corr-lost-ack-2", 2),
        "worker-a",
    )
    .with_stale_after(Duration::ZERO);
    assert!(
        runtime
            .start("flow-lost-ack-pending", Vec::new())
            .await?
            .is_suspended()
    );
    store.drop_wait_ack.store(true, Ordering::SeqCst);
    let current = runtime
        .record_wait_success("flow-lost-ack-pending", "child-1", vec![7_u8])
        .await?;
    store.drop_wait_ack.store(false, Ordering::SeqCst);
    assert!(
        current.is_suspended(),
        "a lost ack on a pending wait reports the current state"
    );
    Ok(())
}

#[tokio::test]
async fn wait_correlation_lookups_route_child_completions() -> CatgaResult<()> {
    let store = Arc::new(GapFlowStore::default());
    let scheduler = Arc::new(catga_core::flow::MemoryFlowScheduler::default());
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        waiting_definition("corr-success", "corr-alpha", 1),
        "worker-a",
    )
    .with_stale_after(Duration::ZERO);
    assert!(
        runtime
            .start("flow-corr-success", Vec::new())
            .await?
            .is_suspended()
    );

    let error = runtime
        .record_wait_success_by_correlation("corr-missing", "child-1", Vec::new())
        .await
        .expect_err("an unknown correlation is rejected");
    assert_eq!(error.code(), ErrorCode::NotFound);

    let resumed = runtime
        .record_wait_success_by_correlation("corr-alpha", "child-1", vec![3_u8])
        .await?;
    assert!(resumed.is_success());

    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        scheduler,
        waiting_definition("corr-failure", "corr-beta", 1),
        "worker-a",
    )
    .with_stale_after(Duration::ZERO);
    assert!(
        runtime
            .start("flow-corr-failure", Vec::new())
            .await?
            .is_suspended()
    );
    let error = runtime
        .record_wait_failure_by_correlation(
            "corr-missing",
            "child-1",
            CatgaError::new(ErrorCode::HandlerFailed, "boom"),
        )
        .await
        .expect_err("an unknown correlation is rejected");
    assert_eq!(error.code(), ErrorCode::NotFound);

    let failed = runtime
        .record_wait_failure_by_correlation(
            "corr-beta",
            "child-1",
            CatgaError::new(ErrorCode::HandlerFailed, "child exploded"),
        )
        .await?;
    assert!(failed.is_failure());
    Ok(())
}

#[tokio::test]
async fn record_wait_failure_validates_state_shape_and_lost_races() -> CatgaResult<()> {
    let store = Arc::new(GapFlowStore::default());
    let scheduler = Arc::new(catga_core::flow::MemoryFlowScheduler::default());

    let guarded = FlowDefinition::new("guarded")
        .step("await", |_state| async {
            Ok(FlowStepOutcome::wait(
                WaitCondition::for_children(
                    "corr-guarded",
                    WaitPolicy::All,
                    ["child-1"],
                    SystemTime::now(),
                    Duration::from_secs(3_600),
                )
                .expect("a single child is valid"),
            ))
        })
        .step("finish", |_state| async { Ok(FlowStepOutcome::complete()) });
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        guarded,
        "worker-a",
    )
    .with_stale_after(Duration::ZERO);
    assert!(
        runtime
            .start("flow-guarded", Vec::new())
            .await?
            .is_suspended()
    );

    let done_definition = FlowDefinition::new("done")
        .step("only", |_state| async { Ok(FlowStepOutcome::complete()) });
    let done_runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        done_definition,
        "worker-a",
    )
    .with_stale_after(Duration::ZERO);
    assert!(
        done_runtime
            .start("flow-done", Vec::new())
            .await?
            .is_success()
    );
    let terminal = done_runtime
        .record_wait_failure(
            "flow-done",
            "child-1",
            CatgaError::new(ErrorCode::HandlerFailed, "late"),
        )
        .await?;
    assert!(terminal.is_success(), "a terminal flow keeps its state");

    let error = runtime
        .record_wait_failure(
            "flow-guarded",
            "intruder",
            CatgaError::new(ErrorCode::HandlerFailed, "foreign"),
        )
        .await
        .expect_err("a foreign child is rejected");
    assert_eq!(error.code(), ErrorCode::Validation);

    store.refuse_wait_record.store(true, Ordering::SeqCst);
    let current = runtime
        .record_wait_failure(
            "flow-guarded",
            "child-1",
            CatgaError::new(ErrorCode::HandlerFailed, "lost race"),
        )
        .await?;
    store.refuse_wait_record.store(false, Ordering::SeqCst);
    assert!(
        current.is_suspended(),
        "a lost record race reports the current durable state"
    );

    let delayed_runtime = FlowRuntime::new(
        Arc::clone(&store),
        scheduler,
        delayed_definition(
            "not-waiting",
            SystemTime::now() + Duration::from_secs(3_600),
        ),
        "worker-a",
    )
    .with_stale_after(Duration::ZERO);
    assert!(
        delayed_runtime
            .start("flow-not-waiting", Vec::new())
            .await?
            .is_suspended()
    );
    let error = delayed_runtime
        .record_wait_failure(
            "flow-not-waiting",
            "child-1",
            CatgaError::new(ErrorCode::HandlerFailed, "no wait"),
        )
        .await
        .expect_err("a delayed flow is not waiting");
    assert_eq!(error.code(), ErrorCode::Validation);
    Ok(())
}

fn child_fanout_definition(name: &str) -> FlowDefinition {
    FlowDefinition::new(name)
        .step("await", |_state| async {
            Ok(FlowStepOutcome::wait(
                WaitCondition::for_children(
                    "corr-fanout",
                    WaitPolicy::All,
                    ["child-1", "child-2"],
                    SystemTime::now(),
                    Duration::from_secs(3_600),
                )
                .expect("two children are valid"),
            ))
        })
        .step("finish", |_state| async { Ok(FlowStepOutcome::complete()) })
}

#[tokio::test]
async fn launch_waiting_children_launches_deduplicates_and_releases_failed_claims()
-> CatgaResult<()> {
    let store = Arc::new(GapFlowStore::default());
    let scheduler = Arc::new(catga_core::flow::MemoryFlowScheduler::default());
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        scheduler,
        child_fanout_definition("fanout"),
        "worker-a",
    )
    .with_stale_after(Duration::ZERO);
    assert!(
        runtime
            .start("flow-fanout", Vec::new())
            .await?
            .is_suspended()
    );

    let launcher = RecordingLauncher::default();
    launcher.fail_first.store(true, Ordering::SeqCst);
    let error = runtime
        .launch_waiting_children("flow-fanout", &launcher)
        .await
        .expect_err("a rejected launch surfaces the launcher error");
    assert_eq!(error.code(), ErrorCode::Internal);
    assert_eq!(launcher.launch_count(), 0);

    let launched = runtime
        .launch_waiting_children("flow-fanout", &launcher)
        .await?;
    assert_eq!(launched, 2, "both stable children launch exactly once");
    assert_eq!(launcher.launch_count(), 2);
    assert!(
        launcher.launches.lock().expect("launcher lock").iter().all(
            |(parent, _, correlation)| parent == "flow-fanout" && correlation == "corr-fanout"
        )
    );

    let again = runtime
        .launch_waiting_children("flow-fanout", &launcher)
        .await?;
    assert_eq!(again, 0, "launched children are not claimed again");

    let resumed = runtime
        .record_wait_success("flow-fanout", "child-1", vec![1_u8])
        .await?;
    assert!(resumed.is_suspended());
    let done = runtime
        .record_wait_success("flow-fanout", "child-2", vec![2_u8])
        .await?;
    assert!(done.is_success());

    let finished = runtime
        .launch_waiting_children("flow-fanout", &launcher)
        .await?;
    assert_eq!(finished, 0, "a terminal flow has no launchable children");
    Ok(())
}

#[tokio::test]
async fn launch_waiting_children_skips_unknown_and_waitless_flows() -> CatgaResult<()> {
    let store = Arc::new(GapFlowStore::default());
    let scheduler = Arc::new(catga_core::flow::MemoryFlowScheduler::default());
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        scheduler,
        delayed_definition("waitless", SystemTime::now() + Duration::from_secs(3_600)),
        "worker-a",
    )
    .with_stale_after(Duration::ZERO);
    assert!(
        runtime
            .start("flow-waitless", Vec::new())
            .await?
            .is_suspended()
    );

    let launcher = RecordingLauncher::default();
    let error = runtime
        .launch_waiting_children("ghost", &launcher)
        .await
        .expect_err("an unknown flow cannot launch children");
    assert_eq!(error.code(), ErrorCode::NotFound);

    let launched = runtime
        .launch_waiting_children("flow-waitless", &launcher)
        .await?;
    assert_eq!(
        launched, 0,
        "a delayed suspension has no children to launch"
    );
    assert_eq!(launcher.launch_count(), 0);
    Ok(())
}

#[tokio::test]
async fn heartbeat_checks_owner_versions_and_definitions() -> CatgaResult<()> {
    let store = Arc::new(GapFlowStore::default());
    let scheduler = Arc::new(catga_core::flow::MemoryFlowScheduler::default());
    let started = Arc::new(Notify::new());
    let release = Arc::new(Barrier::new(2));

    let started_marker = Arc::clone(&started);
    let release_marker = Arc::clone(&release);
    let definition = FlowDefinition::new("hb").step("work", move |_state| {
        let started = Arc::clone(&started_marker);
        let release = Arc::clone(&release_marker);
        async move {
            started.notify_one();
            release.wait().await;
            Ok(FlowStepOutcome::complete())
        }
    });
    let runtime = Arc::new(
        FlowRuntime::new(
            Arc::clone(&store),
            Arc::clone(&scheduler),
            definition,
            "worker-a",
        )
        .with_stale_after(Duration::from_secs(60)),
    );

    let task_runtime = Arc::clone(&runtime);
    let task = tokio::spawn(async move { task_runtime.start("flow-hb", Vec::new()).await });
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .expect("the step starts");

    assert!(
        !runtime
            .heartbeat("ghost", 1)
            .await
            .expect("heartbeat succeeds"),
        "an unknown flow cannot heartbeat"
    );
    let version = store.continuation("flow-hb").state().version();
    assert!(
        runtime.heartbeat("flow-hb", version).await?,
        "the current owner refreshes its running lease"
    );
    assert!(
        !runtime
            .heartbeat("flow-hb", version.wrapping_add(9))
            .await
            .expect("heartbeat succeeds"),
        "a stale version cannot heartbeat"
    );

    let foreign = FlowRuntime::new(
        Arc::clone(&store),
        scheduler,
        FlowDefinition::new("other")
            .step("only", |_state| async { Ok(FlowStepOutcome::complete()) }),
        "worker-b",
    )
    .with_stale_after(Duration::ZERO);
    let error = foreign
        .heartbeat("flow-hb", version)
        .await
        .expect_err("a foreign definition cannot heartbeat the flow");
    assert_eq!(error.code(), ErrorCode::Validation);

    release.wait().await;
    let result = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("the start task completes")
        .expect("the start task does not panic")?;
    assert!(result.is_success());
    Ok(())
}

#[tokio::test]
async fn reconcile_delayed_suspensions_registers_missing_schedule_identities() -> CatgaResult<()> {
    let store = Arc::new(GapFlowStore::default());
    let scheduler = Arc::new(FlakyScheduler::failing_times(1));
    let due = SystemTime::now() + Duration::from_secs(3_600);
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        scheduler,
        delayed_definition("flaky-delay", due),
        "worker-a",
    )
    .with_stale_after(Duration::ZERO);

    let suspended = runtime.start("flow-flaky-delay", Vec::new()).await?;
    assert!(suspended.is_suspended());
    assert!(
        store
            .continuation("flow-flaky-delay")
            .schedule_id()
            .is_none(),
        "a failed scheduling attempt leaves the suspension without a schedule identity"
    );

    let reconciled = runtime.reconcile_delayed_suspensions(10, 100).await?;
    assert_eq!(reconciled, 1, "the missing schedule identity is registered");
    assert!(
        store
            .continuation("flow-flaky-delay")
            .schedule_id()
            .is_some()
    );

    let again = runtime.reconcile_delayed_suspensions(10, 100).await?;
    assert_eq!(again, 0, "already scheduled suspensions are skipped");

    let resumed = runtime.resume_at("flow-flaky-delay", due).await?;
    assert!(resumed.is_success(), "the reconciled flow still resumes");
    Ok(())
}

#[tokio::test]
async fn advance_outcomes_complete_goto_transitions_and_fence_unknown_targets() -> CatgaResult<()> {
    let store = Arc::new(GapFlowStore::default());
    let scheduler = Arc::new(catga_core::flow::MemoryFlowScheduler::default());
    let log = Arc::new(Mutex::new(Vec::<&'static str>::new()));

    let advanced = FlowDefinition::new("advanced")
        .step("only", |_state| async { Ok(FlowStepOutcome::Advance) });
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        advanced,
        "worker-a",
    )
    .with_stale_after(Duration::ZERO);
    let result = runtime.start("flow-advanced", Vec::new()).await?;
    assert!(
        result.is_success(),
        "advancing past the final step completes the flow"
    );

    let first_log = Arc::clone(&log);
    let second_log = Arc::clone(&log);
    let third_log = Arc::clone(&log);
    let gotos = FlowDefinition::new("gotos")
        .step("first", move |_state| {
            let log = Arc::clone(&first_log);
            async move {
                log.lock().expect("log lock").push("first");
                Ok(FlowStepOutcome::goto("third"))
            }
        })
        .step("second", move |_state| {
            let log = Arc::clone(&second_log);
            async move {
                log.lock().expect("log lock").push("second");
                Ok(FlowStepOutcome::Advance)
            }
        })
        .step("third", move |_state| {
            let log = Arc::clone(&third_log);
            async move {
                log.lock().expect("log lock").push("third");
                Ok(FlowStepOutcome::complete())
            }
        });
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        gotos,
        "worker-a",
    )
    .with_stale_after(Duration::ZERO);
    let result = runtime.start("flow-goto", Vec::new()).await?;
    assert!(result.is_success());
    assert_eq!(
        log.lock().expect("log lock").as_slice(),
        &["first", "third"],
        "goto skips intermediate steps"
    );

    let dangling = FlowDefinition::new("dangling")
        .step("first", |_state| async {
            Ok(FlowStepOutcome::goto("missing"))
        })
        .step("second", |_state| async { Ok(FlowStepOutcome::complete()) });
    let runtime = FlowRuntime::new(Arc::clone(&store), scheduler, dangling, "worker-a")
        .with_stale_after(Duration::ZERO);
    let result = runtime.start("flow-dangling", Vec::new()).await?;
    assert!(result.is_failure(), "an unknown goto target fails the flow");
    let error = result.state().error().expect("the error is retained");
    assert_eq!(error.code(), ErrorCode::NotFound);
    Ok(())
}

#[tokio::test]
async fn suspend_and_wait_outcomes_validate_their_following_steps() -> CatgaResult<()> {
    let store = Arc::new(GapFlowStore::default());
    let scheduler = Arc::new(catga_core::flow::MemoryFlowScheduler::default());

    let dangling_delay = FlowDefinition::new("dangling-delay").step("only", move |_state| {
        let due = SystemTime::now() + Duration::from_secs(3_600);
        async move { Ok(FlowStepOutcome::suspend_until(due)) }
    });
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        dangling_delay,
        "worker-a",
    )
    .with_stale_after(Duration::ZERO);
    let result = runtime.start("flow-dangling-delay", Vec::new()).await?;
    assert!(result.is_failure());
    assert_eq!(
        result
            .state()
            .error()
            .expect("the failure is retained")
            .code(),
        ErrorCode::Validation
    );

    let dangling_wait = FlowDefinition::new("dangling-wait").step("only", |_state| async {
        Ok(FlowStepOutcome::wait(WaitCondition::new(
            "corr-dangling",
            WaitPolicy::All,
            1,
            SystemTime::now(),
            Duration::from_secs(3_600),
        )))
    });
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        dangling_wait,
        "worker-a",
    )
    .with_stale_after(Duration::ZERO);
    let result = runtime.start("flow-dangling-wait", Vec::new()).await?;
    assert!(result.is_failure());
    assert_eq!(
        result
            .state()
            .error()
            .expect("the failure is retained")
            .code(),
        ErrorCode::Validation
    );

    let invalid_wait = FlowDefinition::new("invalid-wait")
        .step("await", |_state| async {
            Ok(FlowStepOutcome::wait(WaitCondition::new(
                "corr-invalid",
                WaitPolicy::All,
                0,
                SystemTime::now(),
                Duration::from_secs(3_600),
            )))
        })
        .step("finish", |_state| async { Ok(FlowStepOutcome::complete()) });
    let runtime = FlowRuntime::new(Arc::clone(&store), scheduler, invalid_wait, "worker-a")
        .with_stale_after(Duration::ZERO);
    let result = runtime.start("flow-invalid-wait", Vec::new()).await?;
    assert!(result.is_failure(), "an invalid wait fails the flow");
    assert_eq!(
        result.state().error().expect("error retained").code(),
        ErrorCode::Validation
    );
    Ok(())
}

#[tokio::test]
async fn tagged_steps_run_untimed_without_a_policy_and_fence_retry_ownership() -> CatgaResult<()> {
    let store = Arc::new(GapFlowStore::default());
    let scheduler = Arc::new(catga_core::flow::MemoryFlowScheduler::default());

    let untagged_policy =
        FlowDefinition::new("untagged-policy").step_with_tag("work", "io", |_state| async {
            Ok(FlowStepOutcome::complete())
        });
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        untagged_policy,
        "worker-a",
    )
    .with_stale_after(Duration::ZERO);
    assert!(
        runtime
            .start("flow-untagged-policy", Vec::new())
            .await?
            .is_success(),
        "a tagged step without a policy runs like an ordinary step"
    );

    let attempts = Arc::new(AtomicUsize::new(0));
    let attempt_marker = Arc::clone(&attempts);
    let flaky = FlowDefinition::new("flaky").step_with_tag("work", "io", move |_state| {
        let attempts = Arc::clone(&attempt_marker);
        async move {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err(CatgaError::new(ErrorCode::Transient, "still busy"))
        }
    });
    let runtime = FlowRuntime::new(Arc::clone(&store), scheduler, flaky, "worker-a")
        .with_stale_after(Duration::ZERO)
        .with_tag_policy(FlowTagPolicy::new(Duration::from_secs(30), 2));
    store.fail_heartbeats.store(true, Ordering::SeqCst);
    let result = runtime.start("flow-flaky", Vec::new()).await?;
    store.fail_heartbeats.store(false, Ordering::SeqCst);
    assert!(
        result.is_failure(),
        "ownership loss before a tagged retry fails the flow"
    );
    let error = result.state().error().expect("the error is retained");
    assert_eq!(error.code(), ErrorCode::Conflict);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn heartbeat_loop_renews_ownership_while_a_step_runs() -> CatgaResult<()> {
    let store = Arc::new(GapFlowStore::default());
    let scheduler = Arc::new(catga_core::flow::MemoryFlowScheduler::default());

    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        FlowDefinition::new("slow-step").step("work", |_state| async {
            tokio::time::sleep(Duration::from_millis(40)).await;
            Ok(FlowStepOutcome::complete())
        }),
        "worker-a",
    )
    .with_stale_after(Duration::from_millis(20));
    let result = runtime.start("flow-slow-step", Vec::new()).await?;
    assert!(
        result.is_success(),
        "the heartbeat loop keeps ownership while the step sleeps"
    );
    assert_eq!(
        store.continuation("flow-slow-step").state().version(),
        2,
        "heartbeats refresh the lease without bumping the business version"
    );
    Ok(())
}

#[tokio::test]
async fn heartbeat_loop_reports_lost_ownership_mid_step() -> CatgaResult<()> {
    let store = Arc::new(GapFlowStore::default());
    let scheduler = Arc::new(catga_core::flow::MemoryFlowScheduler::default());
    store.fail_heartbeats.store(true, Ordering::SeqCst);

    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        scheduler,
        FlowDefinition::new("lost-step").step("work", |_state| async {
            tokio::time::sleep(Duration::from_millis(40)).await;
            Ok(FlowStepOutcome::complete())
        }),
        "worker-a",
    )
    .with_stale_after(Duration::from_millis(20));
    let result = runtime.start("flow-lost-step", Vec::new()).await?;
    store.fail_heartbeats.store(false, Ordering::SeqCst);
    assert!(
        result.is_failure(),
        "lost ownership aborts the running step"
    );
    assert_eq!(
        result
            .state()
            .error()
            .expect("the error is retained")
            .code(),
        ErrorCode::Conflict
    );
    Ok(())
}

#[tokio::test]
async fn tagged_deadline_times_out_a_slow_step_under_heartbeat_renewal() -> CatgaResult<()> {
    let store = Arc::new(GapFlowStore::default());
    let scheduler = Arc::new(catga_core::flow::MemoryFlowScheduler::default());

    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        scheduler,
        FlowDefinition::new("deadline").step_with_tag("hang", "io", |_state| async {
            futures::future::pending::<CatgaResult<FlowStepOutcome>>().await
        }),
        "worker-a",
    )
    .with_stale_after(Duration::from_secs(60))
    .with_tag_policy(
        FlowTagPolicy::new(Duration::from_secs(3_600), 0)
            .with_timeout("io", Duration::from_millis(30)),
    );
    let result = runtime.start("flow-deadline", Vec::new()).await?;
    assert!(result.is_failure(), "the select-loop deadline fires");
    assert_eq!(
        result.state().error().expect("error retained").code(),
        ErrorCode::Timeout
    );
    Ok(())
}

#[tokio::test]
async fn compensation_heartbeats_renew_and_fence_ownership() -> CatgaResult<()> {
    let store = Arc::new(GapFlowStore::default());
    let scheduler = Arc::new(catga_core::flow::MemoryFlowScheduler::default());
    let log = Arc::new(Mutex::new(Vec::<&'static str>::new()));

    let log_marker = Arc::clone(&log);
    let build = || {
        let log = Arc::clone(&log_marker);
        FlowDefinition::new("slow-saga")
            .step_with_compensation(
                "reserve",
                |_state| async { Ok(FlowStepOutcome::Advance) },
                move |_state| {
                    let log = Arc::clone(&log);
                    async move {
                        tokio::time::sleep(Duration::from_millis(40)).await;
                        log.lock().expect("log lock").push("compensate:reserve");
                        Ok(())
                    }
                },
            )
            .step("boom", |_state| async {
                Err(CatgaError::new(ErrorCode::Internal, "forward failed"))
            })
    };

    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        build(),
        "worker-a",
    )
    .with_stale_after(Duration::from_millis(20));
    let result = runtime.start("flow-slow-saga", Vec::new()).await?;
    assert!(result.is_failure());
    assert_eq!(
        log.lock().expect("log lock").as_slice(),
        &["compensate:reserve"],
        "the rollback completes under heartbeat renewal"
    );

    store.fail_heartbeats.store(true, Ordering::SeqCst);
    let runtime = FlowRuntime::new(Arc::clone(&store), scheduler, build(), "worker-b")
        .with_stale_after(Duration::from_millis(20));
    let error = runtime
        .start("flow-lost-saga", Vec::new())
        .await
        .expect_err("lost ownership aborts the running compensation");
    store.fail_heartbeats.store(false, Ordering::SeqCst);
    assert_eq!(error.code(), ErrorCode::Conflict);
    Ok(())
}

#[tokio::test]
async fn failure_persistence_races_report_the_current_state() -> CatgaResult<()> {
    let store = Arc::new(GapFlowStore::default());
    let scheduler = Arc::new(catga_core::flow::MemoryFlowScheduler::default());
    let failing = FlowDefinition::new("racy-failure").step("boom", |_state| async {
        Err(CatgaError::new(ErrorCode::Internal, "forward failed"))
    });
    let runtime = FlowRuntime::new(Arc::clone(&store), scheduler, failing, "worker-a")
        .with_stale_after(Duration::ZERO);

    store.fail_next_update.store(true, Ordering::SeqCst);
    let result = runtime.start("flow-racy-failure", Vec::new()).await?;
    assert!(
        result.is_running(),
        "a lost failure update reports the current claimed state"
    );
    Ok(())
}
