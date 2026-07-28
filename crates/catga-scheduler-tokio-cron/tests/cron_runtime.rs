use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use catga_core::{
    CatgaError, CatgaResult, ErrorCode, ScheduledTask, ScheduledTaskId, TaskSchedule, TaskScheduler,
};
use catga_flow::{
    FlowDefinition, FlowDueService, FlowRuntime, FlowState, FlowStepOutcome, MemoryFlowScheduler,
    SuspendedFlowStore,
};
use catga_memory::MemorySuspendedFlows;
use catga_scheduler_tokio_cron::{CronRuntime, flow_due_job};
use tokio::sync::Notify;

struct NotifyTask(Arc<Notify>);

#[async_trait::async_trait]
impl ScheduledTask for NotifyTask {
    async fn execute(&self) -> CatgaResult<()> {
        self.0.notify_one();
        Ok(())
    }
}

struct FailingTask;

#[async_trait::async_trait]
impl ScheduledTask for FailingTask {
    async fn execute(&self) -> CatgaResult<()> {
        Err(CatgaError::new(
            ErrorCode::Internal,
            "intentional task failure",
        ))
    }
}

struct CountingTask {
    ticks: Arc<AtomicUsize>,
    notified: Arc<Notify>,
}

#[async_trait::async_trait]
impl ScheduledTask for CountingTask {
    async fn execute(&self) -> CatgaResult<()> {
        self.ticks.fetch_add(1, Ordering::Relaxed);
        self.notified.notify_one();
        Ok(())
    }
}

#[tokio::test]
async fn cron_runtime_implements_the_core_task_scheduler_contract() {
    let mut runtime = CronRuntime::new()
        .await
        .expect("scheduler construction succeeds");
    let ran = Arc::new(Notify::new());
    let task = Arc::new(NotifyTask(Arc::clone(&ran)));

    let task_id = TaskScheduler::schedule(
        &runtime,
        TaskSchedule::cron("* * * * * *").expect("valid nonempty cron expression"),
        task,
    )
    .await
    .expect("core scheduled task registers");
    let ran_once = ran.notified();
    runtime
        .start()
        .await
        .expect("explicit scheduler start succeeds");
    tokio::time::timeout(Duration::from_secs(4), ran_once)
        .await
        .expect("scheduled core task runs");
    TaskScheduler::cancel(&runtime, &task_id)
        .await
        .expect("core scheduled task cancels");
    runtime
        .shutdown()
        .await
        .expect("explicit scheduler shutdown succeeds");
}

#[tokio::test]
async fn invalid_cron_job_is_a_catga_validation_error_before_registration() {
    let runtime = CronRuntime::new()
        .await
        .expect("scheduler construction succeeds");

    let error = match CronRuntime::new_async_job("this is not cron", |_job_id, _scheduler| {
        Box::pin(async {})
    }) {
        Ok(_) => panic!("invalid cron must not construct a job for registration"),
        Err(error) => error,
    };

    assert_eq!(error.code(), ErrorCode::Validation);
    drop(runtime);
}

#[tokio::test]
async fn core_schedule_rejects_invalid_cron_syntax_before_registration() {
    let runtime = CronRuntime::new()
        .await
        .expect("scheduler construction succeeds");

    let error = TaskScheduler::schedule(
        &runtime,
        TaskSchedule::cron("this is not cron").expect("Core validates only shared invariants"),
        Arc::new(NotifyTask(Arc::new(Notify::new()))),
    )
    .await
    .expect_err("adapter rejects invalid cron syntax");

    assert_eq!(error.code(), ErrorCode::Validation);
}

#[tokio::test]
async fn core_cancel_rejects_an_invalid_task_identifier() {
    let runtime = CronRuntime::new()
        .await
        .expect("scheduler construction succeeds");
    let invalid_id = ScheduledTaskId::new("not-a-uuid").expect("opaque Core ID is nonempty");

    let error = TaskScheduler::cancel(&runtime, &invalid_id)
        .await
        .expect_err("cron adapter requires a UUID task identifier");

    assert_eq!(error.code(), ErrorCode::Validation);
}

#[tokio::test]
async fn core_cancel_reports_not_found_for_an_unknown_task_identifier() {
    let runtime = CronRuntime::new()
        .await
        .expect("scheduler construction succeeds");
    let missing_id = ScheduledTaskId::new("00000000-0000-0000-0000-000000000001")
        .expect("valid UUID is a valid opaque Core ID");

    let error = TaskScheduler::cancel(&runtime, &missing_id)
        .await
        .expect_err("unknown scheduled task must report the Core not-found contract");

    assert_eq!(error.code(), ErrorCode::NotFound);
}

#[tokio::test]
async fn a_core_task_identifier_cannot_be_cancelled_twice() {
    let runtime = CronRuntime::new()
        .await
        .expect("scheduler construction succeeds");
    let task_id = TaskScheduler::schedule(
        &runtime,
        TaskSchedule::cron("* * * * * *").expect("valid nonempty cron expression"),
        Arc::new(NotifyTask(Arc::new(Notify::new()))),
    )
    .await
    .expect("task registers");

    TaskScheduler::cancel(&runtime, &task_id)
        .await
        .expect("first cancellation succeeds");
    let error = TaskScheduler::cancel(&runtime, &task_id)
        .await
        .expect_err("a cancelled Core task identifier is no longer known");

    assert_eq!(error.code(), ErrorCode::NotFound);
}

#[tokio::test]
async fn concurrent_core_cancellations_do_not_both_succeed() {
    let runtime = CronRuntime::new()
        .await
        .expect("scheduler construction succeeds");
    let task_id = TaskScheduler::schedule(
        &runtime,
        TaskSchedule::cron("* * * * * *").expect("valid nonempty cron expression"),
        Arc::new(NotifyTask(Arc::new(Notify::new()))),
    )
    .await
    .expect("task registers");

    let (first, second) = tokio::join!(
        TaskScheduler::cancel(&runtime, &task_id),
        TaskScheduler::cancel(&runtime, &task_id),
    );

    assert!(
        first.is_ok() ^ second.is_ok(),
        "exactly one concurrent cancellation may succeed"
    );
    let error = first
        .err()
        .or_else(|| second.err())
        .expect("one cancellation must fail");
    assert!(
        matches!(error.code(), ErrorCode::Conflict | ErrorCode::NotFound),
        "losing cancellation reports that the task is being removed or has been removed"
    );
}

#[tokio::test]
async fn raw_remove_makes_a_core_task_identifier_unknown() {
    let runtime = CronRuntime::new()
        .await
        .expect("scheduler construction succeeds");
    let task_id = TaskScheduler::schedule(
        &runtime,
        TaskSchedule::cron("* * * * * *").expect("valid nonempty cron expression"),
        Arc::new(NotifyTask(Arc::new(Notify::new()))),
    )
    .await
    .expect("task registers");
    let job_id = uuid::Uuid::parse_str(task_id.as_str()).expect("Core cron ID is a UUID");

    runtime
        .remove(&job_id)
        .await
        .expect("raw job removal succeeds");
    let error = TaskScheduler::cancel(&runtime, &task_id)
        .await
        .expect_err("raw removal clears the Core cancellation identity");

    assert_eq!(error.code(), ErrorCode::NotFound);
}

#[tokio::test]
async fn cancelling_a_started_core_task_stops_future_cron_ticks() {
    let mut runtime = CronRuntime::new()
        .await
        .expect("scheduler construction succeeds");
    let ticks = Arc::new(AtomicUsize::new(0));
    let notified = Arc::new(Notify::new());
    let task_id = TaskScheduler::schedule(
        &runtime,
        TaskSchedule::cron("* * * * * *").expect("valid nonempty cron expression"),
        Arc::new(CountingTask {
            ticks: Arc::clone(&ticks),
            notified: Arc::clone(&notified),
        }),
    )
    .await
    .expect("task registers");

    let first_tick = notified.notified();
    runtime
        .start()
        .await
        .expect("explicit scheduler start succeeds");
    tokio::time::timeout(Duration::from_secs(4), first_tick)
        .await
        .expect("started task receives its first cron tick");
    TaskScheduler::cancel(&runtime, &task_id)
        .await
        .expect("started task cancels");
    let ticks_after_cancel = ticks.load(Ordering::Relaxed);

    assert!(
        tokio::time::timeout(Duration::from_secs(2), notified.notified())
            .await
            .is_err(),
        "cancelled task must not receive a later cron tick"
    );
    assert_eq!(ticks.load(Ordering::Relaxed), ticks_after_cancel);
    runtime
        .shutdown()
        .await
        .expect("explicit scheduler shutdown succeeds");
}

#[tokio::test]
async fn core_schedule_assigns_unique_task_identifiers() {
    let runtime = CronRuntime::new()
        .await
        .expect("scheduler construction succeeds");
    let schedule = TaskSchedule::cron("* * * * * *").expect("valid nonempty cron expression");

    let first = TaskScheduler::schedule(
        &runtime,
        schedule.clone(),
        Arc::new(NotifyTask(Arc::new(Notify::new()))),
    )
    .await
    .expect("first task registers");
    let second = TaskScheduler::schedule(
        &runtime,
        schedule,
        Arc::new(NotifyTask(Arc::new(Notify::new()))),
    )
    .await
    .expect("second task registers");

    assert_ne!(first, second);
    TaskScheduler::cancel(&runtime, &first)
        .await
        .expect("first task cancels");
    TaskScheduler::cancel(&runtime, &second)
        .await
        .expect("second task cancels");
}

#[tokio::test]
async fn a_failing_core_task_does_not_stop_an_unrelated_task() {
    let mut runtime = CronRuntime::new()
        .await
        .expect("scheduler construction succeeds");
    let ran = Arc::new(Notify::new());

    TaskScheduler::schedule(
        &runtime,
        TaskSchedule::cron("* * * * * *").expect("valid nonempty cron expression"),
        Arc::new(FailingTask),
    )
    .await
    .expect("failing task registers");
    TaskScheduler::schedule(
        &runtime,
        TaskSchedule::cron("* * * * * *").expect("valid nonempty cron expression"),
        Arc::new(NotifyTask(Arc::clone(&ran))),
    )
    .await
    .expect("unrelated task registers");

    let ran_once = ran.notified();
    runtime
        .start()
        .await
        .expect("explicit scheduler start succeeds");
    tokio::time::timeout(Duration::from_secs(4), ran_once)
        .await
        .expect("unrelated task still runs after another task returns an error");
    runtime
        .shutdown()
        .await
        .expect("explicit scheduler shutdown succeeds");
}

#[tokio::test]
async fn scheduler_runs_only_after_explicit_start_and_stops_after_shutdown() {
    let mut runtime = CronRuntime::new()
        .await
        .expect("scheduler construction succeeds");
    let ran = Arc::new(Notify::new());
    let callback_ran = Arc::clone(&ran);
    let job = CronRuntime::new_async_job("* * * * * *", move |_job_id, _scheduler| {
        let callback_ran = Arc::clone(&callback_ran);
        Box::pin(async move { callback_ran.notify_one() })
    })
    .expect("valid cron job constructs");

    runtime.add(job).await.expect("job registers");

    assert!(
        tokio::time::timeout(Duration::from_millis(250), ran.notified())
            .await
            .is_err()
    );

    let ran_once = ran.notified();
    runtime
        .start()
        .await
        .expect("explicit scheduler start succeeds");
    tokio::time::timeout(Duration::from_secs(4), ran_once)
        .await
        .expect("started scheduler executes its registered job");
    runtime
        .shutdown()
        .await
        .expect("explicit scheduler shutdown succeeds");
}

#[tokio::test]
async fn removed_job_does_not_run_after_explicit_start() {
    let mut runtime = CronRuntime::new()
        .await
        .expect("scheduler construction succeeds");
    let ran = Arc::new(Notify::new());
    let callback_ran = Arc::clone(&ran);
    let job = CronRuntime::new_async_job("* * * * * *", move |_job_id, _scheduler| {
        let callback_ran = Arc::clone(&callback_ran);
        Box::pin(async move { callback_ran.notify_one() })
    })
    .expect("valid cron job constructs");

    let job_id = runtime.add(job).await.expect("job registers");
    runtime.remove(&job_id).await.expect("job removes");

    runtime
        .start()
        .await
        .expect("explicit scheduler start succeeds");
    assert!(
        tokio::time::timeout(Duration::from_secs(2), ran.notified())
            .await
            .is_err()
    );
    runtime
        .shutdown()
        .await
        .expect("explicit scheduler shutdown succeeds");
}

#[tokio::test]
async fn flow_due_job_performs_a_bounded_due_sweep() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let completed = Arc::new(Notify::new());
    let completed_step = Arc::clone(&completed);
    let definition = FlowDefinition::new("cron-due")
        .step("suspend", |_| async {
            Ok(FlowStepOutcome::SuspendUntil(SystemTime::now()))
        })
        .step("complete", move |_state: FlowState| {
            let completed_step = Arc::clone(&completed_step);
            async move {
                completed_step.notify_one();
                Ok(FlowStepOutcome::Complete)
            }
        });
    let flow_runtime = Arc::new(FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        definition,
        "cron-runtime",
    ));
    let due_service = Arc::new(FlowDueService::new(
        Arc::clone(&flow_runtime),
        Arc::clone(&scheduler),
        "cron-due-job",
    ));

    flow_runtime
        .start("due-flow", [])
        .await
        .expect("fixture flow suspends and registers durable due work");

    let mut runtime = CronRuntime::new()
        .await
        .expect("scheduler construction succeeds");
    runtime
        .add(flow_due_job("* * * * * *", due_service).expect("valid cron job constructs"))
        .await
        .expect("due job registers");

    let completed_once = completed.notified();
    runtime
        .start()
        .await
        .expect("explicit scheduler start succeeds");
    tokio::time::timeout(Duration::from_secs(4), completed_once)
        .await
        .expect("cron callback invokes one bounded due sweep that resumes the flow");
    runtime
        .shutdown()
        .await
        .expect("explicit scheduler shutdown succeeds");

    let continuation = store
        .get("due-flow")
        .await
        .expect("fixture flow remains queryable")
        .expect("fixture flow exists");
    assert!(continuation.state().status().is_terminal());
}
