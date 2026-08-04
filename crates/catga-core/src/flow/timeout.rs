use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use crate::{CatgaError, CatgaResult, ErrorCode};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::flow::{
    runtime::FlowRuntime, scheduler::FlowScheduler, state::FlowStatus,
    suspension::FlowContinuation, suspension_store::SuspendedFlowStore,
};

/// Default maximum number of expired flows processed by one timeout sweep.
pub const DEFAULT_FLOW_TIMEOUT_BATCH_SIZE: usize = 32;
/// Default maximum number of continuation records inspected by one timeout sweep.
pub const DEFAULT_FLOW_TIMEOUT_SCAN_LIMIT: usize = 128;
/// Largest supported timeout result batch.
pub const MAX_FLOW_TIMEOUT_BATCH_SIZE: usize = 256;
/// Largest supported timeout backend scan.
pub const MAX_FLOW_TIMEOUT_SCAN_LIMIT: usize = 1_024;

/// Returns the checked UTC epoch-millisecond deadline indexed for an active suspended wait.
///
/// Fractional milliseconds are rounded up so a millisecond-resolution backend never discovers a
/// wait before its actual deadline.
pub fn flow_timeout_deadline_unix_ms(continuation: &FlowContinuation) -> CatgaResult<Option<u64>> {
    if continuation.state().status() != FlowStatus::Suspended {
        return Ok(None);
    }
    let Some(wait) = continuation.wait() else {
        return Ok(None);
    };
    let deadline = wait
        .created_at()
        .checked_add(wait.timeout())
        .ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Validation,
                "flow wait deadline exceeds the supported range",
            )
        })?;
    let elapsed = deadline
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "flow wait deadline precedes the Unix epoch",
            )
        })?;
    let rounded_millis = elapsed
        .as_millis()
        .checked_add(u128::from(elapsed.subsec_nanos() % 1_000_000 != 0))
        .ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Validation,
                "flow wait deadline exceeds the supported millisecond range",
            )
        })?;
    u64::try_from(rounded_millis).map(Some).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "flow wait deadline exceeds the supported millisecond range",
        )
    })
}

/// One validated, bounded page request for expired durable waits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TimedOutFlowPoll {
    now: SystemTime,
    limit: usize,
    scan_limit: usize,
}

impl TimedOutFlowPoll {
    /// Creates a poll whose result and backend-work bounds are positive, ordered, and supported.
    pub fn new(now: SystemTime, limit: usize, scan_limit: usize) -> CatgaResult<Self> {
        validate_bounds(limit, scan_limit)?;
        Ok(Self {
            now,
            limit,
            scan_limit,
        })
    }

    /// Returns the wall-clock instant used to evaluate wait deadlines.
    pub const fn now(&self) -> SystemTime {
        self.now
    }

    /// Returns the maximum number of expired flow identities to return.
    pub const fn limit(&self) -> usize {
        self.limit
    }

    /// Returns the maximum number of backend records or index entries to inspect.
    pub const fn scan_limit(&self) -> usize {
        self.scan_limit
    }
}

/// One backend-owned receipt for an expired durable wait.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TimedOutFlowReceipt {
    flow_id: Box<str>,
    token: Box<[u8]>,
}

impl TimedOutFlowReceipt {
    /// Creates an opaque receipt for `flow_id`.
    pub fn new(flow_id: impl Into<Box<str>>, token: impl Into<Box<[u8]>>) -> Self {
        Self {
            flow_id: flow_id.into(),
            token: token.into(),
        }
    }

    /// Returns the expired flow identity.
    pub fn flow_id(&self) -> &str {
        &self.flow_id
    }

    /// Returns the backend settlement token.
    pub fn token(&self) -> &[u8] {
        &self.token
    }
}

/// Polls suspended flows whose wait condition has expired by a supplied wall-clock instant.
#[async_trait]
pub trait TimedOutFlowStore: SuspendedFlowStore {
    /// Claims a bounded set of due receipts without enumerating the primary continuation store.
    ///
    /// `limit` bounds retained receipts and `scan_limit` bounds stale-index reconciliation or
    /// broker deliveries. The receipt remains owned by the poller until it is acknowledged or
    /// released. Runtime transitions still use the continuation store's CAS as the final guard.
    async fn poll_timed_out(
        &self,
        poll: &TimedOutFlowPoll,
    ) -> CatgaResult<Vec<TimedOutFlowReceipt>>;

    /// Settles one receipt after the corresponding flow was processed or proved stale.
    async fn ack_timed_out(&self, receipt: &TimedOutFlowReceipt) -> CatgaResult<()>;

    /// Returns one unprocessed receipt to the backend due index.
    async fn release_timed_out(&self, receipt: &TimedOutFlowReceipt) -> CatgaResult<()>;
}

/// Scheduling policy for a caller-owned [`FlowTimeoutService`] loop.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlowTimeoutOptions {
    /// Time between bounded scans for expired durable flow waits.
    pub check_interval: Duration,
    /// Maximum expired flows processed by one scan.
    pub batch_size: usize,
    /// Maximum continuation records inspected by one backend scan.
    pub scan_limit: usize,
}

impl FlowTimeoutOptions {
    /// Creates a validated bounded timeout scan policy.
    pub fn new(
        check_interval: Duration,
        batch_size: usize,
        scan_limit: usize,
    ) -> CatgaResult<Self> {
        let options = Self {
            check_interval,
            batch_size,
            scan_limit,
        };
        options.validate()?;
        Ok(options)
    }

    fn validate(self) -> CatgaResult<()> {
        if self.check_interval.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "flow timeout check_interval must be greater than zero",
            ));
        }
        validate_bounds(self.batch_size, self.scan_limit)
    }
}

impl Default for FlowTimeoutOptions {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(30),
            batch_size: DEFAULT_FLOW_TIMEOUT_BATCH_SIZE,
            scan_limit: DEFAULT_FLOW_TIMEOUT_SCAN_LIMIT,
        }
    }
}

/// Resolves expired durable flow waits that receive no external completion event.
///
/// The service does not spawn a task. Applications own the call to [`Self::run`] and stop it
/// with a [`CancellationToken`], making shutdown and task supervision explicit.
pub struct FlowTimeoutService<S: ?Sized, H: ?Sized> {
    runtime: Arc<FlowRuntime<S, H>>,
    store: Arc<S>,
    options: FlowTimeoutOptions,
}

impl<S, H> FlowTimeoutService<S, H>
where
    S: TimedOutFlowStore + ?Sized,
    H: FlowScheduler + ?Sized,
{
    /// Creates a timeout service with the default thirty-second scan interval.
    pub fn new(runtime: Arc<FlowRuntime<S, H>>, store: Arc<S>) -> Self {
        Self {
            runtime,
            store,
            options: FlowTimeoutOptions::default(),
        }
    }

    /// Replaces the periodic scan policy.
    pub fn with_options(mut self, options: FlowTimeoutOptions) -> CatgaResult<Self> {
        options.validate()?;
        self.options = options;
        Ok(self)
    }

    /// Scans one bounded page at `now` and transitions still-expired waits to timeout results.
    ///
    /// The returned count includes only flows whose runtime actually persisted a failed state;
    /// stale scan results that have advanced concurrently are ignored. Every receipt that cannot
    /// be acknowledged is returned to the store best-effort before this method returns.
    pub async fn check_at(&self, now: SystemTime) -> CatgaResult<usize> {
        self.check_at_until(now, None).await
    }

    async fn check_at_until(
        &self,
        now: SystemTime,
        cancellation: Option<&CancellationToken>,
    ) -> CatgaResult<usize> {
        let poll = TimedOutFlowPoll::new(now, self.options.batch_size, self.options.scan_limit)?;
        let receipts = self.store.poll_timed_out(&poll).await?;
        if receipts.len() > poll.limit() {
            self.release_all(&receipts).await;
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "timeout store returned more receipts than requested",
            ));
        }
        let mut expired: usize = 0;
        for (index, receipt) in receipts.iter().enumerate() {
            if cancellation.is_some_and(CancellationToken::is_cancelled) {
                self.release_all(&receipts[index..]).await;
                return Ok(expired);
            }
            let result = if let Some(cancellation) = cancellation {
                tokio::select! {
                    result = self.runtime.resume_at(receipt.flow_id(), now) => Some(result),
                    _ = cancellation.cancelled() => None,
                }
            } else {
                Some(self.runtime.resume_at(receipt.flow_id(), now).await)
            };
            let Some(result) = result else {
                self.release_all(&receipts[index..]).await;
                return Ok(expired);
            };
            match result {
                Ok(result) if result.is_failure() => expired = expired.saturating_add(1),
                Ok(_) => {}
                Err(error) if matches!(error.code(), ErrorCode::NotFound | ErrorCode::Conflict) => {
                }
                Err(error) => {
                    self.release_all(&receipts[index..]).await;
                    return Err(error);
                }
            }
            if let Err(error) = self.store.ack_timed_out(receipt).await {
                self.release_all(&receipts[index..]).await;
                return Err(error);
            }
        }
        Ok(expired)
    }

    async fn release_all(&self, receipts: &[TimedOutFlowReceipt]) {
        for receipt in receipts {
            if let Err(error) = self.store.release_timed_out(receipt).await {
                tracing::warn!(
                    flow_id = receipt.flow_id(),
                    error = ?error,
                    "timed-out flow receipt could not be released during cleanup"
                );
            }
        }
    }

    /// Periodically scans until `cancellation` is cancelled.
    ///
    /// A scan error is returned to the task owner; successful scans continue even when an
    /// individual flow changes concurrently after it was listed. Cancellation interrupts an
    /// in-progress resume and best-effort releases its receipt before returning.
    pub async fn run(&self, cancellation: CancellationToken) -> CatgaResult<()> {
        loop {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            self.check_at_until(SystemTime::now(), Some(&cancellation))
                .await?;
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                _ = tokio::time::sleep(self.options.check_interval) => {}
            }
        }
    }
}

fn validate_bounds(limit: usize, scan_limit: usize) -> CatgaResult<()> {
    if limit == 0
        || scan_limit == 0
        || limit > scan_limit
        || limit > MAX_FLOW_TIMEOUT_BATCH_SIZE
        || scan_limit > MAX_FLOW_TIMEOUT_SCAN_LIMIT
    {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "flow timeout bounds must be positive, ordered, and within supported limits",
        ));
    }
    Ok(())
}
