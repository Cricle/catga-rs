#![allow(missing_docs)]

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use catga_core::ErrorCode;
use catga_flow::{
    FlowDefinition, FlowDueService, FlowRuntime, FlowState, FlowStepOutcome, MemoryFlowScheduler,
    SuspendedFlowStore,
};
use catga_memory::MemorySuspendedFlows;
use catga_scheduler_tokio_cron::{CronRuntime, flow_due_job};
use tokio::sync::Notify;

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
