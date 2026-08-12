//! Strict scenario contracts for the durable [`FlowRuntime`].
//!
//! Covers step sequencing, version-fenced transitions, compensation, durable
//! delay suspension and scheduled resume, external wait conditions, wait
//! timeouts, cancellation, tagged retry/timeout policies, stale-owner
//! takeover, the due-work service, and the timeout sweep service.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use catga_core::flow::definition::{FlowDefinition, FlowStepOutcome};
use catga_core::flow::{
    DueFlowOptions, FlowContinuation, FlowDueService, FlowRuntime, FlowStatus, FlowTagPolicy,
    FlowTimeoutOptions, FlowTimeoutService, MemoryFlowScheduler, SuspendedFlowStore,
    TimedOutFlowPoll, TimedOutFlowReceipt, TimedOutFlowStore, WaitCondition, WaitPolicy,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use tokio::sync::{Barrier, Notify};

#[derive(Default)]
struct MemoryFlowStore {
    records: Mutex<HashMap<Box<str>, FlowContinuation>>,
    acked_timeouts: Mutex<Vec<Box<str>>>,
    released_timeouts: Mutex<Vec<Box<str>>>,
    ignore_poll_limit: AtomicBool,
}

impl MemoryFlowStore {
    fn continuation(&self, flow_id: &str) -> FlowContinuation {
        self.records
            .lock()
            .expect("store lock")
            .get(flow_id)
            .cloned()
            .expect("the flow continuation exists")
    }

    fn acked_timeouts(&self) -> Vec<Box<str>> {
        self.acked_timeouts.lock().expect("store lock").clone()
    }

    fn released_timeouts(&self) -> Vec<Box<str>> {
        self.released_timeouts.lock().expect("store lock").clone()
    }
}

#[async_trait]
impl SuspendedFlowStore for MemoryFlowStore {
    async fn create(&self, continuation: FlowContinuation) -> CatgaResult<bool> {
        let mut records = self.records.lock().expect("store lock");
        let id: Box<str> = continuation.state().id().into();
        if records.contains_key(&id) {
            return Ok(false);
        }
        records.insert(id, continuation);
        Ok(true)
    }

    async fn get(&self, flow_id: &str) -> CatgaResult<Option<FlowContinuation>> {
        Ok(self
            .records
            .lock()
            .expect("store lock")
            .get(flow_id)
            .cloned())
    }

    async fn update(&self, expected_version: i64, next: FlowContinuation) -> CatgaResult<bool> {
        let mut records = self.records.lock().expect("store lock");
        let id: Box<str> = next.state().id().into();
        let Some(current) = records.get(&id) else {
            return Ok(false);
        };
        if current.state().version() != expected_version {
            return Ok(false);
        }
        records.insert(id, next);
        Ok(true)
    }

    async fn claim(
        &self,
        expected: &FlowContinuation,
        next: FlowContinuation,
    ) -> CatgaResult<bool> {
        let mut records = self.records.lock().expect("store lock");
        let id: Box<str> = expected.state().id().into();
        let Some(current) = records.get(&id) else {
            return Ok(false);
        };
        if current != expected {
            return Ok(false);
        }
        records.insert(id, next);
        Ok(true)
    }

    async fn record_wait_success(
        &self,
        flow_id: &str,
        version: i64,
        child_id: &str,
        payload: Vec<u8>,
    ) -> CatgaResult<bool> {
        let mut records = self.records.lock().expect("store lock");
        let Some(current) = records.get(flow_id).cloned() else {
            return Ok(false);
        };
        if current.state().version() != version {
            return Ok(false);
        }
        let Some(wait) = current.wait() else {
            return Ok(false);
        };
        let next_wait = wait.record_success(child_id, payload);
        if next_wait.results().len() == wait.results().len() {
            return Ok(false);
        }
        records.insert(flow_id.into(), current.with_wait(next_wait));
        Ok(true)
    }

    async fn record_wait_failure(
        &self,
        flow_id: &str,
        version: i64,
        child_id: &str,
        error: CatgaError,
    ) -> CatgaResult<bool> {
        let mut records = self.records.lock().expect("store lock");
        let Some(current) = records.get(flow_id).cloned() else {
            return Ok(false);
        };
        if current.state().version() != version {
            return Ok(false);
        }
        let Some(wait) = current.wait() else {
            return Ok(false);
        };
        let next_wait = wait.record_failure(child_id, error);
        if next_wait.results().len() == wait.results().len() {
            return Ok(false);
        }
        records.insert(flow_id.into(), current.with_wait(next_wait));
        Ok(true)
    }

    async fn heartbeat(&self, flow_id: &str, owner: &str, version: i64) -> CatgaResult<bool> {
        let mut records = self.records.lock().expect("store lock");
        let Some(current) = records.get(flow_id).cloned() else {
            return Ok(false);
        };
        if current.state().version() != version || current.state().owner() != Some(owner) {
            return Ok(false);
        }
        let heartbeated = current.state().clone().heartbeated_at(SystemTime::now());
        records.insert(flow_id.into(), current.with_state(heartbeated));
        Ok(true)
    }
}

#[async_trait]
impl TimedOutFlowStore for MemoryFlowStore {
    async fn poll_timed_out(
        &self,
        poll: &TimedOutFlowPoll,
    ) -> CatgaResult<Vec<TimedOutFlowReceipt>> {
        let records = self.records.lock().expect("store lock");
        let limit = if self.ignore_poll_limit.load(Ordering::SeqCst) {
            usize::MAX
        } else {
            poll.limit()
        };
        let mut receipts = Vec::new();
        for continuation in records.values().take(poll.scan_limit()) {
            let expired = continuation.state().status() == FlowStatus::Suspended
                && continuation
                    .wait()
                    .is_some_and(|wait| wait.is_expired_at(poll.now()));
            if expired {
                receipts.push(TimedOutFlowReceipt::new(
                    continuation.state().id(),
                    continuation.state().id().as_bytes().to_vec(),
                ));
                if receipts.len() == limit {
                    break;
                }
            }
        }
        Ok(receipts)
    }

    async fn ack_timed_out(&self, receipt: &TimedOutFlowReceipt) -> CatgaResult<()> {
        self.acked_timeouts
            .lock()
            .expect("store lock")
            .push(receipt.flow_id().into());
        Ok(())
    }

    async fn release_timed_out(&self, receipt: &TimedOutFlowReceipt) -> CatgaResult<()> {
        self.released_timeouts
            .lock()
            .expect("store lock")
            .push(receipt.flow_id().into());
        Ok(())
    }
}

type TestRuntime = FlowRuntime<MemoryFlowStore, MemoryFlowScheduler>;

fn test_runtime(
    store: &Arc<MemoryFlowStore>,
    scheduler: &Arc<MemoryFlowScheduler>,
    definition: FlowDefinition,
    owner: &str,
) -> TestRuntime {
    FlowRuntime::new(Arc::clone(store), Arc::clone(scheduler), definition, owner)
        .with_stale_after(Duration::ZERO)
}

/// A wait definition whose per-flow timeout seconds are carried in the flow
/// input so one definition can produce expired and patient waits.
fn waiting_definition(name: &str, created_at: SystemTime) -> FlowDefinition {
    FlowDefinition::new(name)
        .step("await", move |state| async move {
            let timeout_secs = u64::from(*state.data().first().expect("a timeout byte"));
            Ok(FlowStepOutcome::wait(WaitCondition::new(
                "corr",
                WaitPolicy::All,
                1,
                created_at,
                Duration::from_secs(timeout_secs),
            )))
        })
        .step("finish", |_state| async { Ok(FlowStepOutcome::complete()) })
}

#[tokio::test]
async fn runtime_start_runs_every_step_to_completion() -> CatgaResult<()> {
    let store = Arc::new(MemoryFlowStore::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let log = Arc::new(Mutex::new(Vec::new()));

    let reserve_log = Arc::clone(&log);
    let charge_log = Arc::clone(&log);
    let definition = FlowDefinition::new("order")
        .step("reserve", move |_state| {
            let log = Arc::clone(&reserve_log);
            async move {
                log.lock().expect("log lock").push("reserve");
                Ok(FlowStepOutcome::Advance)
            }
        })
        .step("charge", move |_state| {
            let log = Arc::clone(&charge_log);
            async move {
                log.lock().expect("log lock").push("charge");
                Ok(FlowStepOutcome::complete())
            }
        });
    let runtime = test_runtime(&store, &scheduler, definition, "worker-a");

    let result = runtime.start("flow-1", Vec::new()).await?;
    assert!(result.is_success());
    assert_eq!(result.state().status(), FlowStatus::Done);
    assert_eq!(result.state().step(), 2);
    assert_eq!(result.state().version(), 4);
    assert_eq!(
        log.lock().expect("log lock").as_slice(),
        &["reserve", "charge"]
    );
    Ok(())
}

#[tokio::test]
async fn runtime_rejects_invalid_definitions_and_duplicate_starts() -> CatgaResult<()> {
    let store = Arc::new(MemoryFlowStore::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());

    let duplicated = FlowDefinition::new("dup")
        .step("a", |_state| async { Ok(FlowStepOutcome::Advance) })
        .step("a", |_state| async { Ok(FlowStepOutcome::complete()) });
    let runtime = test_runtime(&store, &scheduler, duplicated, "worker-a");
    let error = runtime
        .start("flow-dup", Vec::new())
        .await
        .expect_err("duplicate step names are invalid");
    assert_eq!(error.code(), ErrorCode::Validation);

    let empty = FlowDefinition::new("empty");
    let runtime = test_runtime(&store, &scheduler, empty, "worker-a");
    let error = runtime
        .start("flow-empty", Vec::new())
        .await
        .expect_err("a definition without steps is invalid");
    assert_eq!(error.code(), ErrorCode::Validation);

    let valid = FlowDefinition::new("valid")
        .step("only", |_state| async { Ok(FlowStepOutcome::complete()) });
    let runtime = test_runtime(&store, &scheduler, valid, "worker-a");
    assert!(runtime.start("flow-once", Vec::new()).await?.is_success());
    let error = runtime
        .start("flow-once", Vec::new())
        .await
        .expect_err("a second start with the same identity conflicts");
    assert_eq!(error.code(), ErrorCode::Conflict);
    Ok(())
}

#[tokio::test]
async fn runtime_marks_failed_steps_with_the_original_error() -> CatgaResult<()> {
    let store = Arc::new(MemoryFlowStore::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());

    let failing = FlowDefinition::new("failing").step("charge", |_state| async {
        Err(CatgaError::new(ErrorCode::Internal, "charge declined"))
    });
    let runtime = test_runtime(&store, &scheduler, failing, "worker-a");
    let result = runtime.start("flow-err", Vec::new()).await?;
    assert!(result.is_failure());
    let error = result.state().error().expect("the failure is retained");
    assert_eq!(error.code(), ErrorCode::Internal);
    assert_eq!(error.message(), "charge declined");

    let business = FlowDefinition::new("business").step("charge", |_state| async {
        Ok(FlowStepOutcome::Fail(CatgaError::new(
            ErrorCode::HandlerFailed,
            "business rule rejected",
        )))
    });
    let runtime = test_runtime(&store, &scheduler, business, "worker-a");
    let result = runtime.start("flow-fail", Vec::new()).await?;
    assert!(result.is_failure());
    assert_eq!(result.state().status(), FlowStatus::Failed);
    let error = result.state().error().expect("the failure is retained");
    assert_eq!(error.code(), ErrorCode::HandlerFailed);
    assert_eq!(error.message(), "business rule rejected");
    Ok(())
}

#[tokio::test]
async fn runtime_compensates_completed_steps_in_reverse_order() -> CatgaResult<()> {
    let store = Arc::new(MemoryFlowStore::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let log = Arc::new(Mutex::new(Vec::new()));

    let reserve_log = Arc::clone(&log);
    let charge_log = Arc::clone(&log);
    let definition = FlowDefinition::new("saga")
        .step_with_compensation(
            "reserve",
            |_state| async { Ok(FlowStepOutcome::Advance) },
            move |_state| {
                let log = Arc::clone(&reserve_log);
                async move {
                    log.lock().expect("log lock").push("compensate:reserve");
                    Ok(())
                }
            },
        )
        .step_with_compensation(
            "charge",
            |_state| async { Ok(FlowStepOutcome::Advance) },
            move |_state| {
                let log = Arc::clone(&charge_log);
                async move {
                    log.lock().expect("log lock").push("compensate:charge");
                    Ok(())
                }
            },
        )
        .step("ship", |_state| async {
            Err(CatgaError::new(ErrorCode::NotFound, "no carrier"))
        });
    let runtime = test_runtime(&store, &scheduler, definition, "worker-a");

    let result = runtime.start("flow-saga", Vec::new()).await?;
    assert!(result.is_failure());
    let error = result
        .state()
        .error()
        .expect("the original error is retained");
    assert_eq!(error.code(), ErrorCode::NotFound);
    assert_eq!(error.message(), "no carrier");
    assert_eq!(
        log.lock().expect("log lock").as_slice(),
        &["compensate:charge", "compensate:reserve"],
        "rollback runs in reverse completion order and skips the failed step"
    );
    Ok(())
}

#[tokio::test]
async fn runtime_rejects_cancelling_a_compensating_flow() -> CatgaResult<()> {
    let store = Arc::new(MemoryFlowStore::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let compensation_started = Arc::new(Notify::new());
    let release = Arc::new(Barrier::new(2));

    let started_marker = Arc::clone(&compensation_started);
    let release_marker = Arc::clone(&release);
    let definition = FlowDefinition::new("blocking-saga")
        .step_with_compensation(
            "reserve",
            |_state| async { Ok(FlowStepOutcome::Advance) },
            move |_state| {
                let started = Arc::clone(&started_marker);
                let release = Arc::clone(&release_marker);
                async move {
                    started.notify_one();
                    release.wait().await;
                    Ok(())
                }
            },
        )
        .step("boom", |_state| async {
            Err(CatgaError::new(ErrorCode::Internal, "forward failed"))
        });
    let runtime = Arc::new(test_runtime(&store, &scheduler, definition, "worker-a"));

    let started_wait = Arc::clone(&compensation_started);
    let task_runtime = Arc::clone(&runtime);
    let task = tokio::spawn(async move { task_runtime.start("flow-blocked", Vec::new()).await });
    tokio::time::timeout(Duration::from_secs(5), started_wait.notified())
        .await
        .expect("the compensation starts");
    assert_eq!(
        store.continuation("flow-blocked").state().status(),
        FlowStatus::Compensating
    );

    let error = runtime
        .cancel("flow-blocked")
        .await
        .expect_err("a compensating flow cannot be cancelled");
    assert_eq!(error.code(), ErrorCode::Conflict);

    release.wait().await;
    let result = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("the start task completes")
        .expect("the start task does not panic")?;
    assert!(result.is_failure());
    Ok(())
}

#[tokio::test]
async fn runtime_suspends_and_resumes_only_at_the_scheduled_time() -> CatgaResult<()> {
    let store = Arc::new(MemoryFlowStore::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let due = SystemTime::now() + Duration::from_secs(3_600);
    let finish_attempts = Arc::new(AtomicUsize::new(0));

    let finish_marker = Arc::clone(&finish_attempts);
    let definition = FlowDefinition::new("delayed")
        .step("begin", move |_state| async move {
            Ok(FlowStepOutcome::suspend_until(due))
        })
        .step("finish", move |_state| {
            let attempts = Arc::clone(&finish_marker);
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Ok(FlowStepOutcome::complete())
            }
        });
    let runtime = test_runtime(&store, &scheduler, definition, "worker-a");

    let result = runtime.start("flow-delayed", Vec::new()).await?;
    assert!(result.is_suspended());
    assert_eq!(result.state().step(), 1);
    let suspended = store.continuation("flow-delayed");
    assert_eq!(suspended.step_name(), "finish");
    assert_eq!(suspended.resume_at(), Some(due));
    assert!(
        suspended.schedule_id().is_some(),
        "the schedule identity is persisted for restart reconciliation"
    );

    let early = runtime
        .resume_at("flow-delayed", due - Duration::from_secs(1))
        .await?;
    assert!(early.is_suspended(), "an early resume is a no-op");
    assert_eq!(finish_attempts.load(Ordering::SeqCst), 0);

    let resumed = runtime.resume_at("flow-delayed", due).await?;
    assert!(resumed.is_success());
    assert_eq!(resumed.state().step(), 2);
    assert_eq!(finish_attempts.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn runtime_resume_scheduled_fences_stale_targets() -> CatgaResult<()> {
    let store = Arc::new(MemoryFlowStore::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let due = SystemTime::now() + Duration::from_secs(3_600);

    let definition = FlowDefinition::new("scheduled")
        .step("begin", move |_state| async move {
            Ok(FlowStepOutcome::suspend_until(due))
        })
        .step("finish", |_state| async { Ok(FlowStepOutcome::complete()) });
    let runtime = test_runtime(&store, &scheduler, definition, "worker-a");
    assert!(
        runtime
            .start("flow-scheduled", Vec::new())
            .await?
            .is_suspended()
    );

    let error = runtime
        .resume_scheduled("flow-scheduled", "bogus-step")
        .await
        .expect_err("a stale schedule target is rejected");
    assert_eq!(error.code(), ErrorCode::Conflict);

    let early = runtime.resume_scheduled("flow-scheduled", "finish").await?;
    assert!(
        early.is_suspended(),
        "a matching target still waits for the due time"
    );

    let resumed = runtime.resume_at("flow-scheduled", due).await?;
    assert!(resumed.is_success());
    Ok(())
}

#[tokio::test]
async fn runtime_cancel_fences_the_flow_and_cancels_its_schedule() -> CatgaResult<()> {
    let store = Arc::new(MemoryFlowStore::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let due = SystemTime::now() + Duration::from_secs(3_600);

    let definition = FlowDefinition::new("cancellable")
        .step("begin", move |_state| async move {
            Ok(FlowStepOutcome::suspend_until(due))
        })
        .step("finish", |_state| async { Ok(FlowStepOutcome::complete()) });
    let runtime = test_runtime(&store, &scheduler, definition, "worker-a");
    assert!(
        runtime
            .start("flow-cancel", Vec::new())
            .await?
            .is_suspended()
    );

    let cancelled = runtime.cancel("flow-cancel").await?;
    assert!(cancelled.is_cancelled());
    assert!(
        scheduler
            .take_due(due + Duration::from_secs(3_600))
            .is_empty(),
        "cancellation cancels the scheduled resume"
    );

    let again = runtime.cancel("flow-cancel").await?;
    assert!(
        again.is_cancelled(),
        "cancelling a terminal flow is idempotent"
    );
    let resumed = runtime.resume("flow-cancel").await?;
    assert!(
        resumed.is_cancelled(),
        "resuming a terminal flow returns its terminal state"
    );
    Ok(())
}

#[tokio::test]
async fn runtime_wait_resumes_when_a_child_result_completes_the_condition() -> CatgaResult<()> {
    let store = Arc::new(MemoryFlowStore::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let finish_attempts = Arc::new(AtomicUsize::new(0));

    let finish_marker = Arc::clone(&finish_attempts);
    let definition = FlowDefinition::new("join")
        .step("await", |_state| async {
            Ok(FlowStepOutcome::wait(WaitCondition::new(
                "corr-1",
                WaitPolicy::All,
                1,
                SystemTime::now(),
                Duration::from_secs(3_600),
            )))
        })
        .step("finish", move |_state| {
            let attempts = Arc::clone(&finish_marker);
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Ok(FlowStepOutcome::complete())
            }
        });
    let runtime = test_runtime(&store, &scheduler, definition, "worker-a");
    assert!(runtime.start("flow-wait", Vec::new()).await?.is_suspended());
    assert_eq!(finish_attempts.load(Ordering::SeqCst), 0);

    let resumed = runtime
        .record_wait_success("flow-wait", "child-1", vec![7_u8])
        .await?;
    assert!(resumed.is_success(), "the satisfied wait resumes the flow");
    assert_eq!(finish_attempts.load(Ordering::SeqCst), 1);

    let unknown = runtime
        .record_wait_failure(
            "flow-ghost",
            "child-1",
            CatgaError::new(ErrorCode::HandlerFailed, "child exploded"),
        )
        .await;
    assert!(
        matches!(unknown, Err(error) if error.code() == ErrorCode::NotFound),
        "an unknown flow cannot record child results"
    );

    let failing = FlowDefinition::new("join-failure")
        .step("await", |_state| async {
            Ok(FlowStepOutcome::wait(WaitCondition::new(
                "corr-2",
                WaitPolicy::All,
                1,
                SystemTime::now(),
                Duration::from_secs(3_600),
            )))
        })
        .step("finish", |_state| async { Ok(FlowStepOutcome::complete()) });
    let runtime = test_runtime(&store, &scheduler, failing, "worker-a");
    assert!(
        runtime
            .start("flow-wait-failure", Vec::new())
            .await?
            .is_suspended()
    );
    let result = runtime
        .record_wait_failure(
            "flow-wait-failure",
            "child-1",
            CatgaError::new(ErrorCode::HandlerFailed, "child exploded"),
        )
        .await?;
    assert!(
        result.is_failure(),
        "an All wait fails with the child error"
    );
    let error = result.state().error().expect("the child error is retained");
    assert_eq!(error.code(), ErrorCode::HandlerFailed);
    assert_eq!(error.message(), "child exploded");
    Ok(())
}

#[tokio::test]
async fn runtime_wait_rejects_foreign_children_and_non_waiting_flows() -> CatgaResult<()> {
    let store = Arc::new(MemoryFlowStore::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());

    let guarded = FlowDefinition::new("guarded-join")
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
    let runtime = test_runtime(&store, &scheduler, guarded, "worker-a");
    assert!(
        runtime
            .start("flow-guarded", Vec::new())
            .await?
            .is_suspended()
    );

    let error = runtime
        .record_wait_success("flow-guarded", "intruder", Vec::new())
        .await
        .expect_err("a child outside the wait is rejected");
    assert_eq!(error.code(), ErrorCode::Validation);

    let resumed = runtime
        .record_wait_success("flow-guarded", "child-1", Vec::new())
        .await?;
    assert!(resumed.is_success());

    let delayed = FlowDefinition::new("not-waiting")
        .step("begin", |_state| async {
            Ok(FlowStepOutcome::suspend_until(
                SystemTime::now() + Duration::from_secs(3_600),
            ))
        })
        .step("finish", |_state| async { Ok(FlowStepOutcome::complete()) });
    let runtime = test_runtime(&store, &scheduler, delayed, "worker-a");
    assert!(
        runtime
            .start("flow-not-waiting", Vec::new())
            .await?
            .is_suspended()
    );
    let error = runtime
        .record_wait_success("flow-not-waiting", "child-1", Vec::new())
        .await
        .expect_err("a delayed flow is not waiting for child results");
    assert_eq!(error.code(), ErrorCode::Validation);
    Ok(())
}

#[tokio::test]
async fn runtime_expired_wait_fails_the_flow_with_a_timeout_error() -> CatgaResult<()> {
    let store = Arc::new(MemoryFlowStore::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let created_at = SystemTime::now();

    let definition = FlowDefinition::new("impatient")
        .step("await", move |_state| async move {
            Ok(FlowStepOutcome::wait(WaitCondition::new(
                "corr-timeout",
                WaitPolicy::All,
                1,
                created_at,
                Duration::from_secs(1),
            )))
        })
        .step("finish", |_state| async { Ok(FlowStepOutcome::complete()) });
    let runtime = test_runtime(&store, &scheduler, definition, "worker-a");
    assert!(
        runtime
            .start("flow-timeout", Vec::new())
            .await?
            .is_suspended()
    );

    let result = runtime
        .resume_at("flow-timeout", created_at + Duration::from_secs(2))
        .await?;
    assert!(result.is_failure(), "an expired wait fails the flow");
    let error = result
        .state()
        .error()
        .expect("the timeout error is retained");
    assert_eq!(error.code(), ErrorCode::Timeout);
    assert_eq!(error.message(), "flow wait condition timed out");
    Ok(())
}

#[tokio::test]
async fn timeout_service_transitions_only_expired_waits_to_failed() -> CatgaResult<()> {
    let store = Arc::new(MemoryFlowStore::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let created_at = SystemTime::now() - Duration::from_secs(10);

    let runtime = Arc::new(test_runtime(
        &store,
        &scheduler,
        waiting_definition("swept", created_at),
        "worker-a",
    ));
    assert!(
        runtime
            .start("flow-expired", vec![1_u8])
            .await?
            .is_suspended()
    );
    assert!(
        runtime
            .start("flow-patient", vec![0xFF_u8])
            .await?
            .is_suspended()
    );

    let service = FlowTimeoutService::new(Arc::clone(&runtime), Arc::clone(&store))
        .with_options(FlowTimeoutOptions::new(Duration::from_millis(10), 2, 4)?)
        .expect("valid timeout options");
    let expired = service.check_at(SystemTime::now()).await?;
    assert_eq!(expired, 1, "only the expired flow is transitioned");

    let failed = store.continuation("flow-expired");
    assert_eq!(failed.state().status(), FlowStatus::Failed);
    let error = failed
        .state()
        .error()
        .expect("the timeout error is retained");
    assert_eq!(error.code(), ErrorCode::Timeout);
    assert_eq!(error.message(), "flow wait condition timed out");
    assert_eq!(
        store.continuation("flow-patient").state().status(),
        FlowStatus::Suspended,
        "an unexpired wait is left alone"
    );
    assert_eq!(
        store.acked_timeouts().as_slice(),
        &[Box::<str>::from("flow-expired")],
        "the processed receipt is acknowledged"
    );
    assert!(store.released_timeouts().is_empty());
    Ok(())
}

#[tokio::test]
async fn timeout_service_rejects_over_limit_receipts_and_releases_them() -> CatgaResult<()> {
    let store = Arc::new(MemoryFlowStore::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let created_at = SystemTime::now() - Duration::from_secs(10);

    let runtime = test_runtime(
        &store,
        &scheduler,
        waiting_definition("flooded", created_at),
        "worker-a",
    );
    assert!(
        runtime
            .start("flow-flood-a", vec![1_u8])
            .await?
            .is_suspended()
    );
    assert!(
        runtime
            .start("flow-flood-b", vec![1_u8])
            .await?
            .is_suspended()
    );

    store.ignore_poll_limit.store(true, Ordering::SeqCst);
    let service = FlowTimeoutService::new(Arc::new(runtime), Arc::clone(&store))
        .with_options(FlowTimeoutOptions::new(Duration::from_millis(10), 1, 4)?)
        .expect("valid timeout options");
    let error = service
        .check_at(SystemTime::now())
        .await
        .expect_err("a store exceeding the requested batch is rejected");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(
        store.released_timeouts().len(),
        2,
        "every unacknowledged receipt is released"
    );
    assert!(store.acked_timeouts().is_empty());
    Ok(())
}

#[tokio::test]
async fn due_service_claims_resumes_and_acknowledges_schedules() -> CatgaResult<()> {
    let store = Arc::new(MemoryFlowStore::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let finish_attempts = Arc::new(AtomicUsize::new(0));
    let past_due = SystemTime::now() - Duration::from_secs(1);

    let finish_marker = Arc::clone(&finish_attempts);
    let definition = FlowDefinition::new("due")
        .step("begin", move |_state| async move {
            Ok(FlowStepOutcome::suspend_until(past_due))
        })
        .step("finish", move |_state| {
            let attempts = Arc::clone(&finish_marker);
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Ok(FlowStepOutcome::complete())
            }
        });
    let runtime = Arc::new(test_runtime(&store, &scheduler, definition, "worker-a"));
    assert!(runtime.start("flow-due", Vec::new()).await?.is_suspended());

    let service = FlowDueService::new(Arc::clone(&runtime), Arc::clone(&scheduler), "worker-due")
        .with_options(DueFlowOptions {
            batch_size: 2,
            lease_for: Duration::from_secs(30),
            poll_interval: Duration::from_secs(1),
        })
        .expect("valid due options");

    let acknowledged = service.check_at(SystemTime::now()).await?;
    assert_eq!(
        acknowledged, 1,
        "the due schedule is claimed and acknowledged"
    );
    assert_eq!(finish_attempts.load(Ordering::SeqCst), 1);
    assert_eq!(
        store.continuation("flow-due").state().status(),
        FlowStatus::Done
    );

    let acknowledged = service.check_at(SystemTime::now()).await?;
    assert_eq!(acknowledged, 0, "acknowledged work is not redelivered");
    Ok(())
}

#[test]
fn due_service_options_validate_positive_bounds() {
    for options in [
        DueFlowOptions {
            batch_size: 0,
            ..DueFlowOptions::default()
        },
        DueFlowOptions {
            lease_for: Duration::ZERO,
            ..DueFlowOptions::default()
        },
        DueFlowOptions {
            poll_interval: Duration::ZERO,
            ..DueFlowOptions::default()
        },
    ] {
        let store = Arc::new(MemoryFlowStore::default());
        let scheduler = Arc::new(MemoryFlowScheduler::default());
        let definition = FlowDefinition::new("options")
            .step("only", |_state| async { Ok(FlowStepOutcome::complete()) });
        let runtime = Arc::new(test_runtime(&store, &scheduler, definition, "worker-a"));
        let service = FlowDueService::new(runtime, scheduler, "worker-due");
        let error = match service.with_options(options) {
            Ok(_) => panic!("invalid due options must be rejected"),
            Err(error) => error,
        };
        assert_eq!(error.code(), ErrorCode::Validation);
    }
}

#[tokio::test]
async fn tagged_step_retries_transient_errors_until_success() -> CatgaResult<()> {
    let store = Arc::new(MemoryFlowStore::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let attempts = Arc::new(AtomicUsize::new(0));

    let attempt_marker = Arc::clone(&attempts);
    let definition = FlowDefinition::new("tagged").step_with_tag("flaky", "io", move |_state| {
        let attempts = Arc::clone(&attempt_marker);
        async move {
            if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                return Err(CatgaError::new(ErrorCode::Transient, "still busy"));
            }
            Ok(FlowStepOutcome::complete())
        }
    });
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        definition,
        "worker-a",
    )
    .with_stale_after(Duration::ZERO)
    .with_tag_policy(FlowTagPolicy::new(Duration::from_secs(30), 2));

    let result = runtime.start("flow-tagged", Vec::new()).await?;
    assert!(
        result.is_success(),
        "the tagged policy retries transient errors"
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    Ok(())
}

#[tokio::test]
async fn tagged_step_timeout_fails_the_flow() -> CatgaResult<()> {
    let store = Arc::new(MemoryFlowStore::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let attempts = Arc::new(AtomicUsize::new(0));

    let attempt_marker = Arc::clone(&attempts);
    let definition = FlowDefinition::new("slow").step_with_tag("hang", "io", move |_state| {
        let attempts = Arc::clone(&attempt_marker);
        async move {
            attempts.fetch_add(1, Ordering::SeqCst);
            futures::future::pending::<CatgaResult<FlowStepOutcome>>().await
        }
    });
    let runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        definition,
        "worker-a",
    )
    .with_stale_after(Duration::ZERO)
    .with_tag_policy(
        FlowTagPolicy::new(Duration::from_secs(3_600), 0)
            .with_timeout("io", Duration::from_millis(20)),
    );

    let result = runtime.start("flow-slow", Vec::new()).await?;
    assert!(result.is_failure(), "a tagged timeout fails the flow");
    let error = result
        .state()
        .error()
        .expect("the timeout error is retained");
    assert_eq!(error.code(), ErrorCode::Timeout);
    assert_eq!(error.message(), "tagged flow step timed out");
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "timeouts are not retried"
    );
    Ok(())
}

#[tokio::test]
async fn runtime_resume_returns_quietly_for_a_live_owned_flow() -> CatgaResult<()> {
    let store = Arc::new(MemoryFlowStore::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let attempts = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Barrier::new(2));

    let attempt_marker = Arc::clone(&attempts);
    let started_marker = Arc::clone(&started);
    let release_marker = Arc::clone(&release);
    let definition = FlowDefinition::new("owned").step("work", move |_state| {
        let attempts = Arc::clone(&attempt_marker);
        let started = Arc::clone(&started_marker);
        let release = Arc::clone(&release_marker);
        async move {
            attempts.fetch_add(1, Ordering::SeqCst);
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
    let task = tokio::spawn(async move { task_runtime.start("flow-owned", Vec::new()).await });
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .expect("the step starts");

    let current = runtime.resume("flow-owned").await?;
    assert!(
        current.is_running(),
        "a live owner keeps its execution; resume reports the current state"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "the step is not re-executed"
    );

    release.wait().await;
    let result = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("the start task completes")
        .expect("the start task does not panic")?;
    assert!(result.is_success());
    Ok(())
}

#[tokio::test]
async fn stale_owner_takeover_reexecutes_the_step_at_least_once() -> CatgaResult<()> {
    let store = Arc::new(MemoryFlowStore::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let attempts = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Barrier::new(3));

    let build_definition = || {
        let attempts = Arc::clone(&attempts);
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        FlowDefinition::new("stale").step("work", move |_state| {
            let attempts = Arc::clone(&attempts);
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                started.notify_one();
                release.wait().await;
                Ok(FlowStepOutcome::complete())
            }
        })
    };

    let owner_a = Arc::new(
        FlowRuntime::new(
            Arc::clone(&store),
            Arc::clone(&scheduler),
            build_definition(),
            "worker-a",
        )
        .with_stale_after(Duration::from_secs(60)),
    );
    let owner_b = FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        build_definition(),
        "worker-b",
    )
    .with_stale_after(Duration::from_millis(1));

    let task_runtime = Arc::clone(&owner_a);
    let first_task =
        tokio::spawn(async move { task_runtime.start("flow-stale", Vec::new()).await });
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .expect("the first execution starts");
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    // The recorded heartbeat is far past worker-b's staleness window, so
    // worker-b claims the flow and re-executes the in-flight step.
    let takeover_task = tokio::spawn(async move {
        owner_b
            .resume_at("flow-stale", SystemTime::now() + Duration::from_secs(1))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .expect("the stale flow is taken over and re-executed");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    release.wait().await;
    tokio::time::timeout(Duration::from_secs(5), first_task)
        .await
        .expect("the first execution completes")
        .expect("the first execution does not panic")?;
    let takeover = tokio::time::timeout(Duration::from_secs(5), takeover_task)
        .await
        .expect("the takeover completes")
        .expect("the takeover does not panic")?;
    assert!(
        takeover.is_success(),
        "the takeover persists the terminal state"
    );
    assert_eq!(
        store.continuation("flow-stale").state().status(),
        FlowStatus::Done,
        "version fencing persists exactly one terminal state"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "the step ran once per owner: durable steps are at-least-once"
    );
    Ok(())
}

#[tokio::test]
async fn runtime_rejects_resume_of_unknown_or_foreign_flows() -> CatgaResult<()> {
    let store = Arc::new(MemoryFlowStore::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());

    let definition = FlowDefinition::new("known")
        .step("only", |_state| async { Ok(FlowStepOutcome::complete()) });
    let runtime = test_runtime(&store, &scheduler, definition, "worker-a");
    let error = runtime
        .resume("ghost")
        .await
        .expect_err("an unknown flow cannot resume");
    assert_eq!(error.code(), ErrorCode::NotFound);

    assert!(runtime.start("flow-known", Vec::new()).await?.is_success());
    let foreign = FlowDefinition::new("foreign")
        .step("only", |_state| async { Ok(FlowStepOutcome::complete()) });
    let runtime = test_runtime(&store, &scheduler, foreign, "worker-b");
    let error = runtime
        .resume("flow-known")
        .await
        .expect_err("a continuation belongs to its original definition");
    assert_eq!(error.code(), ErrorCode::Validation);
    Ok(())
}
