use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use parking_lot::Mutex;

/// Schedules durable flow resumption without coupling flows to a job system.
#[async_trait]
pub trait FlowScheduler: Send + Sync {
    /// Schedules one suspended `state_id` of `flow_id` for resumption at `due_at`.
    ///
    /// The `state_id` keeps independently suspended branches of the same flow distinct. The
    /// returned identity can be used to cancel precisely this scheduled resumption.
    ///
    /// Returns [`ErrorCode::Unavailable`] when this scheduler has exhausted its non-reusable
    /// schedule identities.
    async fn schedule_resume(
        &self,
        flow_id: &str,
        state_id: &str,
        due_at: SystemTime,
    ) -> CatgaResult<Box<str>>;

    /// Cancels a scheduled resume when it has not started.
    async fn cancel_resume(&self, schedule_id: &str) -> CatgaResult<bool>;
}

/// Adds at-least-once due-work claiming to a persistent [`FlowScheduler`].
///
/// A worker acknowledges only after successful resume handling. Unacknowledged claims become
/// eligible again after their lease, so process loss cannot silently discard a scheduled flow.
#[async_trait]
pub trait DueFlowScheduler: FlowScheduler {
    /// Claims at most `limit` schedules due no later than `now` for `owner`.
    ///
    /// Each call examines at most `limit` due heap entries, including entries held by other live
    /// owners. Requeued entries receive a later inspection order, so repeated calls make progress
    /// through due work without allowing one bounded call to traverse the entire heap.
    ///
    /// Returns [`ErrorCode::Validation`] when `lease_for` is zero or its deadline cannot be
    /// represented by [`SystemTime`], because such a claim cannot establish ownership safely.
    async fn claim_due(
        &self,
        owner: &str,
        now: SystemTime,
        lease_for: Duration,
        limit: usize,
    ) -> CatgaResult<Vec<ScheduledResume>>;

    /// Acknowledges a claimed schedule only when it is still owned by `owner`.
    async fn ack_due(&self, owner: &str, schedule_id: &str) -> CatgaResult<bool>;

    /// Releases a claimed schedule for another worker to retry after a failed resume.
    async fn release_due(&self, owner: &str, schedule_id: &str) -> CatgaResult<bool>;

    /// Extends a still-live, owned schedule lease from `now` without changing its due target.
    ///
    /// Returns [`ErrorCode::Validation`] when the requested deadline is outside the supported
    /// [`SystemTime`] range.
    async fn renew_due(
        &self,
        owner: &str,
        schedule_id: &str,
        now: SystemTime,
        lease_for: Duration,
    ) -> CatgaResult<bool>;
}

/// One due flow-resume request emitted by [`MemoryFlowScheduler`].
#[derive(Clone, Debug)]
pub struct ScheduledResume {
    schedule_id: Box<str>,
    flow_id: Box<str>,
    state_id: Box<str>,
    due_at: SystemTime,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ScheduleTarget {
    flow_id: Box<str>,
    state_id: Box<str>,
}

impl ScheduleTarget {
    fn new(flow_id: impl Into<Box<str>>, state_id: impl Into<Box<str>>) -> Self {
        Self {
            flow_id: flow_id.into(),
            state_id: state_id.into(),
        }
    }

    fn from_schedule(schedule: &ScheduledResume) -> Self {
        Self::new(schedule.flow_id.clone(), schedule.state_id.clone())
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DueSchedule {
    available_at: SystemTime,
    inspection_order: u64,
    due_at: SystemTime,
    schedule_id: Box<str>,
}

impl ScheduledResume {
    /// Creates a scheduler-owned flow-resume request.
    ///
    /// Backend implementations use this constructor after atomically claiming durable work.
    pub fn new(
        schedule_id: impl Into<Box<str>>,
        flow_id: impl Into<Box<str>>,
        state_id: impl Into<Box<str>>,
        due_at: SystemTime,
    ) -> Self {
        Self {
            schedule_id: schedule_id.into(),
            flow_id: flow_id.into(),
            state_id: state_id.into(),
            due_at,
        }
    }

    /// Returns the scheduler-specific cancellation identity.
    pub fn schedule_id(&self) -> &str {
        &self.schedule_id
    }

    /// Returns the flow that should be resumed.
    pub fn flow_id(&self) -> &str {
        &self.flow_id
    }

    /// Returns the suspended state or branch that should be resumed.
    pub fn state_id(&self) -> &str {
        &self.state_id
    }

    /// Returns when this resume becomes eligible.
    pub const fn due_at(&self) -> SystemTime {
        self.due_at
    }
}

/// A deterministic in-memory scheduler for local development and integration tests.
#[derive(Default)]
pub struct MemoryFlowScheduler {
    next_id: AtomicU64,
    state: Mutex<SchedulerState>,
    #[cfg(test)]
    claim_test_hook: Mutex<Option<ClaimTestHook>>,
}

#[derive(Default)]
struct SchedulerState {
    schedules: HashMap<Box<str>, ScheduledResume>,
    target_schedules: HashMap<ScheduleTarget, Box<str>>,
    due_schedules: BinaryHeap<Reverse<DueSchedule>>,
    claims: HashMap<Box<str>, ScheduleClaim>,
    next_inspection_order: u64,
}

struct ScheduleClaim {
    owner: Box<str>,
    expires_at: SystemTime,
}

#[cfg(test)]
#[derive(Clone)]
struct ClaimTestHook {
    schedule_read: std::sync::Arc<std::sync::Barrier>,
    continue_claim: std::sync::Arc<std::sync::Barrier>,
}

impl SchedulerState {
    fn enqueue_due(
        &mut self,
        available_at: SystemTime,
        due_at: SystemTime,
        schedule_id: Box<str>,
    ) -> CatgaResult<()> {
        let inspection_order = self.next_inspection_order;
        self.next_inspection_order =
            self.next_inspection_order.checked_add(1).ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::Unavailable,
                    "due-work inspection ordering is exhausted",
                )
            })?;
        self.due_schedules.push(Reverse(DueSchedule {
            available_at,
            due_at,
            inspection_order,
            schedule_id,
        }));
        Ok(())
    }

    fn compact_cancelled_schedules(&mut self) {
        let live = self.schedules.len();
        if self.due_schedules.len() <= live.saturating_mul(2).saturating_add(64) {
            return;
        }
        self.due_schedules
            .retain(|Reverse(schedule)| self.schedules.contains_key(schedule.schedule_id.as_ref()));
    }

    fn remove_due_schedule(&mut self, schedule_id: &str) {
        self.due_schedules
            .retain(|Reverse(schedule)| schedule.schedule_id.as_ref() != schedule_id);
    }

    fn restore_due_eligibility(&mut self, schedule_id: &str, due_at: SystemTime) {
        let mut due_schedules = std::mem::take(&mut self.due_schedules).into_vec();
        for Reverse(schedule) in &mut due_schedules {
            if schedule.schedule_id.as_ref() == schedule_id {
                schedule.available_at = due_at;
            }
        }
        self.due_schedules = BinaryHeap::from(due_schedules);
    }
}

impl MemoryFlowScheduler {
    #[cfg(test)]
    fn pause_before_claim_lock(&self) {
        let hook = self.claim_test_hook.lock().clone();
        if let Some(hook) = hook {
            hook.schedule_read.wait();
            hook.continue_claim.wait();
        }
    }

    fn allocate_schedule_id(&self) -> CatgaResult<u64> {
        self.next_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |id| id.checked_add(1))
            .map_err(|_| {
                CatgaError::new(
                    ErrorCode::Unavailable,
                    "schedule identity space is exhausted",
                )
            })
    }

    /// Removes and returns every schedule due no later than `now`.
    ///
    /// This compatibility helper never consumes work with a live [`DueFlowScheduler`] claim.
    /// Applications should use [`DueFlowScheduler::claim_due`] with acknowledgement for durable
    /// production work.
    pub fn take_due(&self, now: SystemTime) -> Vec<ScheduledResume> {
        let mut state = self.state.lock();
        let mut due = Vec::new();
        let mut claimed = Vec::new();
        while state
            .due_schedules
            .peek()
            .is_some_and(|Reverse(schedule)| schedule.available_at <= now)
        {
            let Some(Reverse(schedule)) = state.due_schedules.pop() else {
                break;
            };
            if state
                .claims
                .get(&schedule.schedule_id)
                .is_some_and(|claim| claim.expires_at > now)
            {
                claimed.push(schedule);
            } else {
                state.claims.remove(&schedule.schedule_id);
                due.push(schedule.schedule_id);
            }
        }
        state.due_schedules.extend(claimed.into_iter().map(Reverse));

        due.into_iter()
            .filter_map(|id| {
                let schedule = state.schedules.remove(&id)?;
                let target = ScheduleTarget::from_schedule(&schedule);
                if state
                    .target_schedules
                    .get(&target)
                    .is_some_and(|mapped| mapped.as_ref() == schedule.schedule_id())
                {
                    state.target_schedules.remove(&target);
                }
                Some(schedule)
            })
            .collect()
    }
}

#[async_trait]
impl FlowScheduler for MemoryFlowScheduler {
    async fn schedule_resume(
        &self,
        flow_id: &str,
        state_id: &str,
        due_at: SystemTime,
    ) -> CatgaResult<Box<str>> {
        let target = ScheduleTarget::new(flow_id, state_id);
        let id = self.allocate_schedule_id()?;
        let schedule_id: Box<str> = format!("flow-resume-{id}").into();
        let mut state = self.state.lock();
        if state.target_schedules.contains_key(&target) {
            return Err(catga_core::CatgaError::new(
                catga_core::ErrorCode::Conflict,
                "a resume is already scheduled for this flow state",
            ));
        }
        state.enqueue_due(due_at, due_at, schedule_id.clone())?;
        state
            .target_schedules
            .insert(target.clone(), schedule_id.clone());
        state.schedules.insert(
            schedule_id.clone(),
            ScheduledResume {
                schedule_id: schedule_id.clone(),
                flow_id: target.flow_id.clone(),
                state_id: target.state_id.clone(),
                due_at,
            },
        );
        Ok(schedule_id)
    }

    async fn cancel_resume(&self, schedule_id: &str) -> CatgaResult<bool> {
        let mut state = self.state.lock();
        if state
            .claims
            .get(schedule_id)
            .is_some_and(|claim| claim.expires_at > SystemTime::now())
        {
            return Ok(false);
        }
        state.claims.remove(schedule_id);
        let Some(schedule) = state.schedules.remove(schedule_id) else {
            return Ok(false);
        };
        let target = ScheduleTarget::from_schedule(&schedule);
        if state
            .target_schedules
            .get(&target)
            .is_some_and(|mapped| mapped.as_ref() == schedule.schedule_id())
        {
            state.target_schedules.remove(&target);
        }
        state.remove_due_schedule(schedule_id);
        state.compact_cancelled_schedules();
        Ok(true)
    }
}

#[async_trait]
impl DueFlowScheduler for MemoryFlowScheduler {
    async fn claim_due(
        &self,
        owner: &str,
        now: SystemTime,
        lease_for: Duration,
        limit: usize,
    ) -> CatgaResult<Vec<ScheduledResume>> {
        if lease_for.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "due-work lease duration must be greater than zero",
            ));
        }
        let expires_at = lease_deadline(now, lease_for)?;
        let mut claimed = Vec::new();
        let mut inspected = 0;
        #[cfg(test)]
        self.pause_before_claim_lock();
        let mut state = self.state.lock();
        while inspected < limit {
            if state
                .due_schedules
                .peek()
                .is_none_or(|Reverse(due)| due.available_at > now)
            {
                break;
            }
            let Some(Reverse(due)) = state.due_schedules.pop() else {
                break;
            };
            inspected += 1;
            let Some(schedule) = state.schedules.get(due.schedule_id.as_ref()).cloned() else {
                continue;
            };
            let can_claim = state
                .claims
                .get(&schedule.schedule_id)
                .is_none_or(|claim| claim.expires_at <= now);
            // A live foreign claim must rotate behind work that became due after it. Releasing
            // the claim restores its original eligibility, so a clock rollback cannot hide it.
            let available_at = if can_claim { due.available_at } else { now };
            let requeue_at = due.due_at;
            let requeue_id = schedule.schedule_id.clone();
            if let Err(error) = state.enqueue_due(available_at, requeue_at, requeue_id) {
                state.due_schedules.push(Reverse(due));
                return Err(error);
            }
            if schedule.due_at != due.due_at {
                continue;
            }
            if can_claim {
                state.claims.insert(
                    schedule.schedule_id.clone(),
                    ScheduleClaim {
                        owner: owner.into(),
                        expires_at,
                    },
                );
                claimed.push(schedule);
            }
        }
        Ok(claimed)
    }

    async fn ack_due(&self, owner: &str, schedule_id: &str) -> CatgaResult<bool> {
        let mut state = self.state.lock();
        if state
            .claims
            .get(schedule_id)
            .is_none_or(|claim| claim.owner.as_ref() != owner)
        {
            return Ok(false);
        }
        let Some(schedule) = state.schedules.remove(schedule_id) else {
            state.claims.remove(schedule_id);
            return Ok(false);
        };
        let target = ScheduleTarget::from_schedule(&schedule);
        if state
            .target_schedules
            .get(&target)
            .is_some_and(|mapped| mapped.as_ref() == schedule.schedule_id())
        {
            state.target_schedules.remove(&target);
        }
        state.claims.remove(schedule_id);
        state.remove_due_schedule(schedule_id);
        state.compact_cancelled_schedules();
        Ok(true)
    }

    async fn release_due(&self, owner: &str, schedule_id: &str) -> CatgaResult<bool> {
        let mut state = self.state.lock();
        if state
            .claims
            .get(schedule_id)
            .is_some_and(|claim| claim.owner.as_ref() == owner)
        {
            let due_at = state
                .schedules
                .get(schedule_id)
                .map(ScheduledResume::due_at);
            state.claims.remove(schedule_id);
            if let Some(due_at) = due_at {
                state.restore_due_eligibility(schedule_id, due_at);
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn renew_due(
        &self,
        owner: &str,
        schedule_id: &str,
        now: SystemTime,
        lease_for: Duration,
    ) -> CatgaResult<bool> {
        if lease_for.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "due-work lease duration must be greater than zero",
            ));
        }
        let expires_at = lease_deadline(now, lease_for)?;
        let mut state = self.state.lock();
        if let Some(claim) = state.claims.get_mut(schedule_id)
            && claim.owner.as_ref() == owner
            && claim.expires_at > now
        {
            claim.expires_at = expires_at;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

fn lease_deadline(now: SystemTime, lease_for: Duration) -> CatgaResult<SystemTime> {
    now.checked_add(lease_for).ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Validation,
            "due-work lease deadline exceeds the supported SystemTime range",
        )
    })
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Barrier, mpsc},
        thread,
        time::{Duration, SystemTime},
    };

    use catga_core::ErrorCode;

    use super::{ClaimTestHook, DueFlowScheduler, FlowScheduler, MemoryFlowScheduler};

    #[test]
    fn an_expired_claim_cannot_be_renewed_by_its_former_owner() {
        let scheduler = MemoryFlowScheduler::default();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let schedule_id = futures::executor::block_on(scheduler.schedule_resume(
            "payment/renew-expired",
            "charge",
            now,
        ))
        .expect("schedule creation must succeed");

        assert_eq!(
            futures::executor::block_on(scheduler.claim_due(
                "owner",
                now,
                Duration::from_secs(1),
                1,
            ))
            .expect("claim must succeed")
            .len(),
            1
        );
        assert!(
            !futures::executor::block_on(scheduler.renew_due(
                "owner",
                &schedule_id,
                now + Duration::from_secs(1),
                Duration::from_secs(1),
            ))
            .expect("renewal must complete")
        );
    }

    #[test]
    fn released_due_work_remains_eligible_after_a_clock_rollback() {
        let scheduler = MemoryFlowScheduler::default();
        let due_at = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let observed_at = due_at + Duration::from_secs(100);
        let rolled_back_now = due_at + Duration::from_secs(50);
        let schedule_id = futures::executor::block_on(scheduler.schedule_resume(
            "payment/clock-rollback",
            "charge",
            due_at,
        ))
        .expect("schedule creation must succeed");

        assert_eq!(
            futures::executor::block_on(scheduler.claim_due(
                "first-owner",
                observed_at,
                Duration::from_secs(60),
                1,
            ))
            .expect("initial claim must succeed")
            .len(),
            1
        );
        assert!(
            futures::executor::block_on(scheduler.release_due("first-owner", &schedule_id))
                .expect("release must succeed")
        );
        assert_eq!(
            futures::executor::block_on(scheduler.claim_due(
                "second-owner",
                rolled_back_now,
                Duration::from_secs(60),
                1,
            ))
            .expect("claim after clock rollback must succeed")
            .len(),
            1
        );
    }

    #[test]
    fn releasing_a_foreign_rotated_claim_restores_its_original_eligibility() {
        let scheduler = MemoryFlowScheduler::default();
        let due_at = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let observed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(200);
        let rolled_back_now = SystemTime::UNIX_EPOCH + Duration::from_secs(150);
        let schedule_id = futures::executor::block_on(scheduler.schedule_resume(
            "payment/foreign-rollback",
            "charge",
            due_at,
        ))
        .expect("schedule creation must succeed");

        assert_eq!(
            futures::executor::block_on(scheduler.claim_due(
                "owner-a",
                observed_at,
                Duration::from_secs(60),
                1,
            ))
            .expect("initial claim must succeed")
            .len(),
            1
        );
        assert!(
            futures::executor::block_on(scheduler.claim_due(
                "owner-b",
                observed_at,
                Duration::from_secs(60),
                1,
            ))
            .expect("foreign claim inspection must succeed")
            .is_empty()
        );
        assert!(
            futures::executor::block_on(scheduler.release_due("owner-a", &schedule_id))
                .expect("release must succeed")
        );

        assert_eq!(
            futures::executor::block_on(scheduler.claim_due(
                "owner-b",
                rolled_back_now,
                Duration::from_secs(60),
                1,
            ))
            .expect("claim after rollback must succeed")
            .len(),
            1
        );
    }

    #[test]
    fn bounded_claim_rotates_a_live_foreign_claim_behind_later_due_work() {
        let scheduler = MemoryFlowScheduler::default();
        let first_due_at = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let later_due_at = first_due_at + Duration::from_secs(1);
        let foreign_id = futures::executor::block_on(scheduler.schedule_resume(
            "payment/foreign",
            "charge",
            first_due_at,
        ))
        .expect("foreign schedule creation must succeed");
        assert_eq!(
            futures::executor::block_on(scheduler.claim_due(
                "foreign-owner",
                first_due_at,
                Duration::from_secs(60),
                1,
            ))
            .expect("foreign claim must succeed")[0]
                .schedule_id(),
            foreign_id.as_ref()
        );
        let later_id = futures::executor::block_on(scheduler.schedule_resume(
            "payment/later",
            "charge",
            later_due_at,
        ))
        .expect("later schedule creation must succeed");

        assert!(
            futures::executor::block_on(scheduler.claim_due(
                "local-owner",
                later_due_at,
                Duration::from_secs(60),
                1,
            ))
            .expect("bounded foreign inspection must succeed")
            .is_empty()
        );
        let claimed = futures::executor::block_on(scheduler.claim_due(
            "local-owner",
            later_due_at,
            Duration::from_secs(60),
            1,
        ))
        .expect("later due claim must succeed");

        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].schedule_id(), later_id.as_ref());
    }

    #[test]
    fn exhausted_schedule_ids_return_unavailable_without_reusing_live_work() {
        let scheduler = MemoryFlowScheduler::default();
        let now = SystemTime::UNIX_EPOCH;
        let first_id = futures::executor::block_on(scheduler.schedule_resume(
            "payment/id-live",
            "charge",
            now,
        ))
        .expect("initial schedule creation must succeed");
        scheduler
            .next_id
            .store(u64::MAX, std::sync::atomic::Ordering::Relaxed);

        let error = futures::executor::block_on(scheduler.schedule_resume(
            "payment/id-exhausted",
            "charge",
            now,
        ))
        .expect_err("exhausted identifiers must not wrap");

        assert_eq!(error.code(), ErrorCode::Unavailable);
        assert_eq!(scheduler.take_due(now)[0].schedule_id(), first_id.as_ref());
    }

    #[test]
    fn a_cancelled_schedule_is_never_returned_by_a_racing_claimer() {
        let scheduler = Arc::new(MemoryFlowScheduler::default());
        let now = std::time::SystemTime::now();
        let schedule_id =
            futures::executor::block_on(scheduler.schedule_resume("payment/race", "charge", now))
                .expect("schedule creation must succeed");
        let hook = ClaimTestHook {
            schedule_read: Arc::new(Barrier::new(2)),
            continue_claim: Arc::new(Barrier::new(2)),
        };
        *scheduler.claim_test_hook.lock() = Some(hook.clone());

        let claim_scheduler = Arc::clone(&scheduler);
        let claim = thread::spawn(move || {
            futures::executor::block_on(claim_scheduler.claim_due(
                "claim-owner",
                now,
                Duration::from_secs(60),
                1,
            ))
            .expect("claim must succeed")
        });
        hook.schedule_read.wait();

        let cancel_scheduler = Arc::clone(&scheduler);
        let cancel_id = schedule_id.clone();
        let (cancelled_tx, cancelled_rx) = mpsc::channel();
        let cancellation_completed = Arc::new(Barrier::new(2));
        let cancellation_completed_in_thread = Arc::clone(&cancellation_completed);
        let cancel = thread::spawn(move || {
            let cancelled = futures::executor::block_on(cancel_scheduler.cancel_resume(&cancel_id))
                .expect("cancellation must succeed");
            cancelled_tx
                .send(cancelled)
                .expect("test receiver must remain available");
            cancellation_completed_in_thread.wait();
        });

        cancellation_completed.wait();
        hook.continue_claim.wait();

        let claimed = claim.join().expect("claim thread must not panic");
        let cancelled = cancelled_rx
            .recv()
            .expect("cancellation thread must report before the claim continues");
        cancel.join().expect("cancellation thread must not panic");

        assert!(cancelled, "the racing cancellation must succeed");
        assert!(
            claimed.is_empty(),
            "a cancelled schedule must not be returned by a claimant"
        );
    }
}
