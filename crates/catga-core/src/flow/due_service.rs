//! Worker orchestration for durable due flow resumes.

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use crate::{CatgaError, CatgaResult, ErrorCode};
use tokio_util::sync::CancellationToken;

use crate::flow::{
    runtime::{FlowRuntime, FlowRuntimeResult},
    scheduler::{DueFlowScheduler, ScheduledResume},
    suspension_store::SuspendedFlowStore,
};

/// Polling and ownership policy for [`FlowDueService`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DueFlowOptions {
    /// Maximum number of due resumes claimed by one [`FlowDueService::check_at`] call.
    pub batch_size: usize,
    /// Exclusive ownership duration for a claimed resume.
    pub lease_for: Duration,
    /// Delay between successful polls in [`FlowDueService::run`].
    pub poll_interval: Duration,
}

impl Default for DueFlowOptions {
    fn default() -> Self {
        Self {
            batch_size: 64,
            lease_for: Duration::from_secs(30),
            poll_interval: Duration::from_secs(1),
        }
    }
}

/// Claims and executes due flow resumes through a [`DueFlowScheduler`].
///
/// This service is the pure-Rust counterpart to an external scheduler callback. It makes no
/// background task of its own: applications either call [`Self::check_at`] from their own loop or
/// supervise [`Self::run`] with a cancellation token. A completed resume is acknowledged. A
/// transient runtime failure is released for retry. Before each due claim, it also makes one
/// bounded attempt to reconcile delayed suspensions whose schedule identity was not persisted.
/// Stores that do not support bounded continuation discovery retain the previous due-work-only
/// behavior. Stale schedule targets and deleted flows are acknowledged because retrying them can
/// never make them current again.
pub struct FlowDueService<S: ?Sized, H: ?Sized> {
    runtime: Arc<FlowRuntime<S, H>>,
    scheduler: Arc<H>,
    owner: Box<str>,
    options: DueFlowOptions,
}

impl<S, H> FlowDueService<S, H>
where
    S: SuspendedFlowStore + ?Sized,
    H: DueFlowScheduler + ?Sized,
{
    /// Creates a service that identifies its scheduler claims as `owner`.
    pub fn new(
        runtime: Arc<FlowRuntime<S, H>>,
        scheduler: Arc<H>,
        owner: impl Into<Box<str>>,
    ) -> Self {
        Self {
            runtime,
            scheduler,
            owner: owner.into(),
            options: DueFlowOptions::default(),
        }
    }

    /// Replaces the bounded polling and lease policy.
    pub fn with_options(mut self, options: DueFlowOptions) -> CatgaResult<Self> {
        if options.batch_size == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "due flow batch_size must be greater than zero",
            ));
        }
        if options.lease_for.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "due flow lease_for must be greater than zero",
            ));
        }
        if options.poll_interval.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "due flow poll_interval must be greater than zero",
            ));
        }
        self.options = options;
        Ok(self)
    }

    /// Claims and handles one bounded batch due no later than `now`.
    ///
    /// Returns the number of schedules acknowledged in this batch. If a resume fails with a
    /// retryable runtime error, the schedule is released and that error is returned so the task
    /// owner can apply its normal supervision policy. Delayed-suspension reconciliation uses the
    /// same `batch_size` for both its result and scan bounds before this claim.
    pub async fn check_at(&self, now: SystemTime) -> CatgaResult<usize> {
        self.check_at_until(now, None).await
    }

    async fn check_at_until(
        &self,
        now: SystemTime,
        cancellation: Option<&CancellationToken>,
    ) -> CatgaResult<usize> {
        if let Err(error) = self
            .runtime
            .reconcile_delayed_suspensions(self.options.batch_size, self.options.batch_size)
            .await
            && error.code() != ErrorCode::Unsupported
        {
            return Err(error);
        }
        let schedules = self
            .scheduler
            .claim_due(
                &self.owner,
                now,
                self.options.lease_for,
                self.options.batch_size,
            )
            .await?;
        let mut acknowledged = 0_usize;
        for (index, schedule) in schedules.iter().enumerate() {
            if cancellation.is_some_and(CancellationToken::is_cancelled) {
                self.release_all(&schedules[index..]).await;
                return Ok(acknowledged);
            }
            let result = match self.resume_with_renewal(schedule, cancellation).await {
                Ok(Some(result)) => result,
                Ok(None) => {
                    // The active resume may have been dropped while cancellation interrupted it.
                    // Keep its lease and durable schedule until the lease expires so another
                    // worker can recover it; only release work that was never started.
                    self.release_all(&schedules[index.saturating_add(1)..])
                        .await;
                    return Ok(acknowledged);
                }
                Err(error) => {
                    self.release_all(&schedules[index..]).await;
                    return Err(error);
                }
            };
            match result {
                Ok(_) => {}
                Err(error) if matches!(error.code(), ErrorCode::NotFound | ErrorCode::Conflict) => {
                }
                Err(error) => {
                    self.release_all(&schedules[index..]).await;
                    return Err(error);
                }
            }
            let acknowledged_schedule = self
                .scheduler
                .ack_due(&self.owner, schedule.schedule_id())
                .await;
            if !matches!(acknowledged_schedule, Ok(true)) {
                self.release_all(&schedules[index..]).await;
                acknowledged_schedule?;
                return Err(CatgaError::new(
                    ErrorCode::Transient,
                    "due flow scheduler claim was lost before acknowledgement",
                ));
            }
            acknowledged = acknowledged.saturating_add(1);
        }
        Ok(acknowledged)
    }

    async fn resume_with_renewal(
        &self,
        schedule: &ScheduledResume,
        cancellation: Option<&CancellationToken>,
    ) -> CatgaResult<Option<CatgaResult<FlowRuntimeResult>>> {
        let resume = self
            .runtime
            .resume_scheduled(schedule.flow_id(), schedule.state_id());
        tokio::pin!(resume);
        let renewal_interval = (self.options.lease_for / 2).max(Duration::from_nanos(1));
        loop {
            tokio::select! {
                result = &mut resume => return Ok(Some(result)),
                _ = tokio::time::sleep(renewal_interval) => {
                    if !self.scheduler.renew_due(
                        &self.owner,
                        schedule.schedule_id(),
                        SystemTime::now(),
                        self.options.lease_for,
                    ).await? {
                        return Err(CatgaError::new(
                            ErrorCode::Transient,
                            "due flow scheduler claim was lost during execution",
                        ));
                    }
                }
                _ = async {
                    if let Some(cancellation) = cancellation {
                        cancellation.cancelled().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => return Ok(None),
            }
        }
    }

    async fn release_all(&self, schedules: &[ScheduledResume]) {
        for schedule in schedules {
            match self
                .scheduler
                .release_due(&self.owner, schedule.schedule_id())
                .await
            {
                Ok(true) => {}
                Ok(false) => tracing::warn!(
                    schedule_id = schedule.schedule_id(),
                    "flow due scheduler claim was already lost before cleanup"
                ),
                Err(error) => tracing::warn!(
                    schedule_id = schedule.schedule_id(),
                    error = ?error,
                    "flow due scheduler claim could not be released during cleanup"
                ),
            }
        }
    }

    /// Polls for due work until `cancellation` is cancelled.
    pub async fn run(&self, cancellation: CancellationToken) -> CatgaResult<()> {
        loop {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            self.check_at_until(SystemTime::now(), Some(&cancellation))
                .await?;
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                _ = tokio::time::sleep(self.options.poll_interval) => {}
            }
        }
    }
}
