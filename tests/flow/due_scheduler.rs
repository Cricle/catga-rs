//! Due scheduler ownership and lease recovery tests.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_core::flow::{
    DueFlowOptions, DueFlowScheduler, FlowContinuation, FlowDefinition, FlowDueService, FlowQuery,
    FlowRuntime, FlowScheduler, FlowState, FlowStepOutcome, FlowSummary, MemoryFlowScheduler,
    ScheduledResume, SuspendedFlowStore,
};
use catga_memory::MemorySuspendedFlows;
use tokio::sync::oneshot;

struct ScheduleIdentityWriteFailureStore {
    inner: Arc<MemorySuspendedFlows>,
    fail_next_schedule_identity_write: AtomicBool,
    successful_schedule_identity_writes: AtomicUsize,
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
        if next.schedule_id().is_some() {
            if self
                .fail_next_schedule_identity_write
                .swap(false, Ordering::SeqCst)
            {
                return Err(CatgaError::new(
                    ErrorCode::Transient,
                    "simulated schedule identity persistence failure",
                ));
            }
            self.successful_schedule_identity_writes
                .fetch_add(1, Ordering::SeqCst);
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

struct RenewalObservableScheduler {
    state: Mutex<RenewalObservableSchedulerState>,
    renewal_signal: Mutex<Option<oneshot::Sender<()>>>,
}

#[derive(Default)]
struct RenewalObservableSchedulerState {
    schedule: Option<ScheduledResume>,
    owner: Option<Box<str>>,
    renewed: bool,
}

impl RenewalObservableScheduler {
    fn with_renewal_signal() -> (Self, oneshot::Receiver<()>) {
        let (renewed, renewal_signal) = oneshot::channel();
        (
            Self {
                state: Mutex::new(RenewalObservableSchedulerState::default()),
                renewal_signal: Mutex::new(Some(renewed)),
            },
            renewal_signal,
        )
    }
}

#[async_trait]
impl FlowScheduler for RenewalObservableScheduler {
    async fn schedule_resume(
        &self,
        flow_id: &str,
        state_id: &str,
        due_at: SystemTime,
    ) -> CatgaResult<Box<str>> {
        let schedule_id: Box<str> = "renewal-observed".into();
        self.state.lock().unwrap().schedule = Some(ScheduledResume::new(
            schedule_id.clone(),
            flow_id,
            state_id,
            due_at,
        ));
        Ok(schedule_id)
    }

    async fn cancel_resume(&self, _schedule_id: &str) -> CatgaResult<bool> {
        Ok(false)
    }
}

#[async_trait]
impl DueFlowScheduler for RenewalObservableScheduler {
    async fn claim_due(
        &self,
        owner: &str,
        _now: SystemTime,
        _lease_for: Duration,
        limit: usize,
    ) -> CatgaResult<Vec<ScheduledResume>> {
        let mut state = self.state.lock().unwrap();
        let Some(schedule) = state.schedule.clone() else {
            return Ok(Vec::new());
        };
        if limit == 0 {
            return Ok(Vec::new());
        }
        match state.owner.as_deref() {
            None => {
                state.owner = Some(owner.into());
                Ok(vec![schedule])
            }
            Some(current_owner) if current_owner == owner || state.renewed => Ok(Vec::new()),
            Some(_) => {
                state.owner = Some(owner.into());
                Ok(vec![schedule])
            }
        }
    }

    async fn ack_due(&self, owner: &str, schedule_id: &str) -> CatgaResult<bool> {
        let mut state = self.state.lock().unwrap();
        if state.owner.as_deref() != Some(owner)
            || state
                .schedule
                .as_ref()
                .is_none_or(|schedule| schedule.schedule_id() != schedule_id)
        {
            return Ok(false);
        }
        state.owner = None;
        state.schedule = None;
        Ok(true)
    }

    async fn release_due(&self, owner: &str, _schedule_id: &str) -> CatgaResult<bool> {
        let mut state = self.state.lock().unwrap();
        if state.owner.as_deref() != Some(owner) {
            return Ok(false);
        }
        state.owner = None;
        Ok(true)
    }

    async fn renew_due(
        &self,
        owner: &str,
        _schedule_id: &str,
        _now: SystemTime,
        _lease_for: Duration,
    ) -> CatgaResult<bool> {
        let renewed = {
            let mut state = self.state.lock().unwrap();
            if state.owner.as_deref() != Some(owner) {
                false
            } else {
                state.renewed = true;
                true
            }
        };
        if !renewed {
            return Ok(false);
        }
        if let Some(signal) = self.renewal_signal.lock().unwrap().take() {
            let _ = signal.send(());
        }
        Ok(true)
    }
}

#[tokio::test]
async fn due_scheduler_claims_once_checks_owner_and_recovers_expired_leases() {
    let scheduler = MemoryFlowScheduler::default();
    let now = SystemTime::now();
    let id = scheduler
        .schedule_resume("payment/7", "charge", now)
        .await
        .unwrap();
    assert_eq!(
        scheduler
            .claim_due("a", now, Duration::from_secs(1), 1)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        scheduler
            .claim_due("b", now, Duration::from_secs(1), 1)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(!scheduler.ack_due("b", &id).await.unwrap());
    assert!(
        scheduler
            .claim_due("b", now + Duration::from_secs(2), Duration::from_secs(1), 1)
            .await
            .unwrap()
            .len()
            == 1
    );
    assert!(scheduler.ack_due("b", &id).await.unwrap());
}

#[tokio::test]
async fn acknowledging_due_work_releases_its_flow_state_for_rescheduling() {
    let scheduler = MemoryFlowScheduler::default();
    let now = SystemTime::now();
    let id = scheduler
        .schedule_resume("payment/8", "charge", now)
        .await
        .unwrap();

    scheduler
        .claim_due("worker", now, Duration::from_secs(1), 1)
        .await
        .unwrap();
    assert!(scheduler.ack_due("worker", &id).await.unwrap());

    assert!(
        scheduler
            .schedule_resume("payment/8", "charge", now + Duration::from_secs(1))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn due_scheduler_rejects_zero_length_leases() {
    let scheduler = MemoryFlowScheduler::default();
    let now = SystemTime::now();
    scheduler
        .schedule_resume("payment/9", "charge", now)
        .await
        .unwrap();

    let error = scheduler
        .claim_due("worker", now, Duration::ZERO, 1)
        .await
        .unwrap_err();

    assert_eq!(error.code(), ErrorCode::Validation);
}

#[tokio::test]
async fn due_scheduler_rejects_lease_deadlines_outside_system_time_range() {
    let scheduler = MemoryFlowScheduler::default();
    let error = scheduler
        .claim_due("worker", SystemTime::UNIX_EPOCH, Duration::MAX, 1)
        .await
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::Validation);

    let schedule_id = scheduler
        .schedule_resume("payment/overflow", "charge", SystemTime::UNIX_EPOCH)
        .await
        .unwrap();
    assert_eq!(
        scheduler
            .claim_due("worker", SystemTime::UNIX_EPOCH, Duration::from_secs(1), 1)
            .await
            .unwrap()
            .len(),
        1
    );
    let error = scheduler
        .renew_due(
            "worker",
            &schedule_id,
            SystemTime::UNIX_EPOCH,
            Duration::MAX,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::Validation);
}

#[tokio::test]
async fn due_scheduler_reconciles_cancelled_heap_entries_before_claiming_next_due_schedule() {
    let scheduler = MemoryFlowScheduler::default();
    let now = SystemTime::now();
    let cancelled = scheduler
        .schedule_resume("payment/cancelled", "charge", now)
        .await
        .unwrap();
    scheduler
        .schedule_resume("payment/live", "charge", now)
        .await
        .unwrap();
    assert!(scheduler.cancel_resume(&cancelled).await.unwrap());

    let claimed = scheduler
        .claim_due("worker", now, Duration::from_secs(1), 1)
        .await
        .unwrap();

    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].flow_id(), "payment/live");
}

#[tokio::test]
async fn due_scheduler_bounds_foreign_claim_inspection_and_eventually_reaches_later_work() {
    const FOREIGN_CLAIMS: usize = 8;

    let scheduler = MemoryFlowScheduler::default();
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
    for index in 0..FOREIGN_CLAIMS {
        scheduler
            .schedule_resume(&format!("payment/foreign-{index}"), "charge", now)
            .await
            .unwrap();
    }
    assert_eq!(
        scheduler
            .claim_due("first-owner", now, Duration::from_secs(60), FOREIGN_CLAIMS,)
            .await
            .unwrap()
            .len(),
        FOREIGN_CLAIMS
    );

    let later_id = scheduler
        .schedule_resume("payment/later", "charge", now)
        .await
        .unwrap();

    assert!(
        scheduler
            .claim_due("second-owner", now, Duration::from_secs(60), 1)
            .await
            .unwrap()
            .is_empty()
    );
    for _ in 1..FOREIGN_CLAIMS {
        assert!(
            scheduler
                .claim_due("second-owner", now, Duration::from_secs(60), 1)
                .await
                .unwrap()
                .is_empty()
        );
    }

    let claimed = scheduler
        .claim_due("second-owner", now, Duration::from_secs(60), 1)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].schedule_id(), later_id.as_ref());
}

#[tokio::test]
async fn due_flow_service_resumes_claimed_work_and_acknowledges_it() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let due_at = SystemTime::now() - Duration::from_secs(1);
    let runtime = Arc::new(FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        FlowDefinition::new("scheduled-payment")
            .step("delay", move |_| async move {
                Ok(FlowStepOutcome::SuspendUntil(due_at))
            })
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "flow-worker",
    ));
    runtime.start("scheduled-payment/1", []).await.unwrap();
    let service = FlowDueService::new(runtime, scheduler, "scheduler-worker")
        .with_options(DueFlowOptions {
            batch_size: 8,
            lease_for: Duration::from_secs(30),
            poll_interval: Duration::from_secs(1),
        })
        .unwrap();

    assert_eq!(service.check_at(SystemTime::now()).await.unwrap(), 1);
    assert!(
        store
            .get("scheduled-payment/1")
            .await
            .unwrap()
            .unwrap()
            .state()
            .status()
            .is_terminal()
    );
}

#[tokio::test]
async fn due_flow_service_reconciles_a_missing_schedule_identity_before_claiming_due_work()
-> CatgaResult<()> {
    let store = Arc::new(ScheduleIdentityWriteFailureStore {
        inner: Arc::new(MemorySuspendedFlows::default()),
        fail_next_schedule_identity_write: AtomicBool::new(true),
        successful_schedule_identity_writes: AtomicUsize::new(0),
    });
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let due_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
    let runtime = Arc::new(FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        FlowDefinition::new("due-reconciliation")
            .step("delay", move |_| async move {
                Ok(FlowStepOutcome::SuspendUntil(due_at))
            })
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "flow-worker",
    ));

    assert!(
        runtime
            .start("due-reconciliation/1", [])
            .await?
            .is_suspended()
    );
    let service = FlowDueService::new(runtime, scheduler, "scheduler-worker");

    assert_eq!(service.check_at(due_at).await?, 1);
    assert_eq!(
        store
            .successful_schedule_identity_writes
            .load(Ordering::SeqCst),
        1
    );
    assert!(
        store
            .get("due-reconciliation/1")
            .await?
            .is_some_and(|continuation| continuation.state().status().is_terminal())
    );
    Ok(())
}

#[tokio::test]
async fn due_flow_service_releases_the_whole_unvisited_batch_after_an_early_error() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let now = SystemTime::now();
    for index in 0..2 {
        let flow_id = format!("wrong-definition/{index}");
        assert!(
            store
                .create(FlowContinuation::new(
                    FlowState::new(flow_id.as_str(), "other-definition", [], "flow-worker")
                        .suspended(),
                    "finish",
                ))
                .await
                .unwrap()
        );
        scheduler
            .schedule_resume(&flow_id, "finish", now)
            .await
            .unwrap();
    }
    let runtime = Arc::new(FlowRuntime::new(
        store,
        Arc::clone(&scheduler),
        FlowDefinition::new("expected-definition")
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "flow-worker",
    ));
    let service = FlowDueService::new(runtime, Arc::clone(&scheduler), "scheduler-worker");

    assert_eq!(
        service.check_at(now).await.unwrap_err().code(),
        ErrorCode::Validation
    );
    assert_eq!(
        scheduler
            .claim_due("recovery-worker", now, Duration::from_secs(1), 2)
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test(start_paused = true)]
async fn due_flow_service_renews_a_schedule_while_its_step_is_running() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let (scheduler, renewal_observed) = RenewalObservableScheduler::with_renewal_signal();
    let scheduler = Arc::new(scheduler);
    let due_at = SystemTime::UNIX_EPOCH;
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let runtime = Arc::new(FlowRuntime::new(
        store,
        Arc::clone(&scheduler),
        FlowDefinition::new("renewed-schedule")
            .step("delay", move |_| async move {
                Ok(FlowStepOutcome::SuspendUntil(due_at))
            })
            .step("finish", {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                move |_| {
                    let entered = Arc::clone(&entered);
                    let release = Arc::clone(&release);
                    async move {
                        entered.notify_one();
                        release.notified().await;
                        Ok(FlowStepOutcome::complete())
                    }
                }
            }),
        "flow-worker",
    ));
    assert!(
        runtime
            .start("renewed-schedule/1", [])
            .await
            .unwrap()
            .is_suspended()
    );
    let service = Arc::new(
        FlowDueService::new(runtime, Arc::clone(&scheduler), "scheduler-worker")
            .with_options(DueFlowOptions {
                batch_size: 1,
                lease_for: Duration::from_secs(2),
                poll_interval: Duration::from_secs(1),
            })
            .unwrap(),
    );
    let check = tokio::spawn({
        let service = Arc::clone(&service);
        async move { service.check_at(SystemTime::UNIX_EPOCH).await }
    });
    entered.notified().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    renewal_observed
        .await
        .expect("renewal signal sender must remain available");

    assert!(
        scheduler
            .claim_due(
                "competing-worker",
                SystemTime::UNIX_EPOCH,
                Duration::from_secs(2),
                1,
            )
            .await
            .unwrap()
            .is_empty()
    );
    release.notify_waiters();
    assert_eq!(check.await.unwrap().unwrap(), 1);
}

#[tokio::test]
async fn legacy_due_take_and_cancellation_cannot_bypass_an_active_claim() {
    let scheduler = MemoryFlowScheduler::default();
    let now = SystemTime::now();
    let id = scheduler
        .schedule_resume("payment/10", "charge", now)
        .await
        .unwrap();

    assert_eq!(
        scheduler
            .claim_due("worker", now, Duration::from_secs(1), 1)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(scheduler.take_due(now).is_empty());
    assert!(!scheduler.cancel_resume(&id).await.unwrap());
    assert_eq!(scheduler.take_due(now + Duration::from_secs(2)).len(), 1);
}
