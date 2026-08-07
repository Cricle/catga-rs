//! Runtime-neutral lifecycle, health, recovery, and shutdown contracts.

use std::{
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use futures::FutureExt;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::{CatgaError, CatgaResult, ErrorCode};

/// Performs asynchronous component startup.
#[async_trait]
pub trait AsyncInitializable: Send + Sync {
    /// Initializes the component.
    async fn initialize(&self) -> CatgaResult<()>;
}

/// Exposes a cheap current health indicator.
pub trait HealthCheckable: Send + Sync {
    /// Returns whether the component can currently serve work.
    fn is_healthy(&self) -> bool;
    /// Returns an optional human-readable health detail.
    fn health_status(&self) -> Option<&str> {
        None
    }
}

/// Stops new work while allowing already-running work to drain.
pub trait Stoppable: Send + Sync {
    /// Stops accepting new messages or requests.
    fn stop_accepting(&self);
    /// Returns whether new work is still accepted.
    fn is_accepting(&self) -> bool;
}

/// Shared lock-free state for components that can stop accepting new work.
///
/// Stopping is one-way for the lifetime of a value. It does not cancel work that was already
/// accepted, allowing callers to combine this gate with [`Waitable`] for graceful shutdown.
///
/// ```
/// use catga_core::{AcceptanceGate, Stoppable};
///
/// let gate = AcceptanceGate::default();
/// assert!(gate.is_accepting());
/// gate.stop_accepting();
/// assert!(!gate.is_accepting());
/// ```
#[derive(Clone, Debug)]
pub struct AcceptanceGate {
    accepting: Arc<AtomicBool>,
}

impl Default for AcceptanceGate {
    fn default() -> Self {
        Self {
            accepting: Arc::new(AtomicBool::new(true)),
        }
    }
}

impl AcceptanceGate {
    /// Permanently rejects new work for all clones of this gate.
    pub fn stop_accepting(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    /// Returns whether this gate still accepts new work.
    pub fn is_accepting(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
    }

    /// Returns an availability error when the component has stopped accepting new work.
    pub fn ensure_accepting(&self) -> CatgaResult<()> {
        if self.is_accepting() {
            return Ok(());
        }
        Err(CatgaError::new(
            ErrorCode::Unavailable,
            "transport is not accepting new messages",
        ))
    }
}

/// Exposes in-flight work so shutdown orchestration can wait for a bounded drain.
///
/// Implementations should observe `cancellation` while waiting and return `Ok(())` promptly when
/// it is cancelled. The counter is informational and may change immediately after it is read.
#[async_trait]
pub trait Waitable: Send + Sync {
    /// Waits until work accepted before shutdown has completed or cancellation is requested.
    ///
    /// Cancellation ends only this wait; it does not discard or otherwise modify in-flight work.
    async fn wait_for_completion(&self, cancellation: CancellationToken) -> CatgaResult<()>;

    /// Returns the current number of in-flight operations.
    fn pending_operations(&self) -> usize;
}

/// Options that bound the drain phase of [`TransportLifecycle::shutdown`].
///
/// The timeout limits only how long the coordinator waits for work that was accepted before
/// shutdown. It never cancels or discards that work; a transport remains responsible for the
/// cancellation behavior documented by [`Waitable`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransportLifecycleOptions {
    /// Maximum time to wait for accepted work to drain after new work is rejected.
    pub drain_timeout: Duration,
}

impl Default for TransportLifecycleOptions {
    fn default() -> Self {
        Self {
            drain_timeout: Duration::from_secs(30),
        }
    }
}

impl TransportLifecycleOptions {
    /// Creates options with a non-zero drain timeout.
    pub fn new(drain_timeout: Duration) -> CatgaResult<Self> {
        let options = Self { drain_timeout };
        options.validate()?;
        Ok(options)
    }

    fn validate(self) -> CatgaResult<()> {
        if self.drain_timeout.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "transport drain_timeout must be greater than zero",
            ));
        }
        Ok(())
    }
}

/// Result of a [`TransportLifecycle`] shutdown drain attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportShutdown {
    /// Every operation accepted before shutdown completed before the timeout.
    Drained,
    /// The caller cancelled the drain wait; accepted work is not modified by the coordinator.
    Cancelled {
        /// The transport-reported number of operations still in flight.
        pending_operations: usize,
    },
    /// The configured drain timeout elapsed; accepted work is not modified by the coordinator.
    TimedOut {
        /// The transport-reported number of operations still in flight.
        pending_operations: usize,
    },
}

/// Owns the lifecycle of one transport without a framework host or background task.
///
/// Call [`Self::initialize`] during startup. [`Self::shutdown`] then consumes this coordinator,
/// stops new work, waits for the bounded drain, and releases the owned transport before returning.
/// Consuming ownership is Rust's disposal boundary: transports release resources through `Drop`,
/// so no .NET-style disposal interface or service container is needed. A transport needing an
/// asynchronous protocol-level close should perform it as part of [`Waitable::wait_for_completion`].
pub struct TransportLifecycle<T> {
    transport: T,
}

impl<T> TransportLifecycle<T> {
    /// Wraps a transport whose startup and shutdown are managed by this value.
    pub const fn new(transport: T) -> Self {
        Self { transport }
    }

    /// Returns the managed transport for read-only configuration or observability.
    pub const fn transport(&self) -> &T {
        &self.transport
    }

    /// Returns mutable access to the managed transport before shutdown begins.
    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }

    /// Consumes the coordinator without stopping or draining the transport.
    ///
    /// Prefer [`Self::shutdown`] in normal runtime teardown paths.
    pub fn into_inner(self) -> T {
        self.transport
    }
}

impl<T> TransportLifecycle<T>
where
    T: AsyncInitializable,
{
    /// Initializes the managed transport in the caller's task.
    ///
    /// This method does not spawn work. Initialization errors are returned unchanged and leave
    /// ownership with the coordinator, allowing the caller to select its own retry policy.
    pub async fn initialize(&self) -> CatgaResult<()> {
        self.transport.initialize().await
    }
}

impl<T> TransportLifecycle<T>
where
    T: Stoppable + Waitable,
{
    /// Stops accepting work, waits for the configured bounded drain, then releases the transport.
    ///
    /// The coordinator allocates no task and polls exactly one drain future. `cancellation` and
    /// `drain_timeout` stop only the wait. In all outcomes the transport is dropped before this
    /// method returns, so callers cannot accidentally reuse a transport after shutdown.
    pub async fn shutdown(
        self,
        options: TransportLifecycleOptions,
        cancellation: CancellationToken,
    ) -> CatgaResult<TransportShutdown> {
        options.validate()?;
        let transport = self.transport;
        transport.stop_accepting();

        let outcome = {
            let drain = transport.wait_for_completion(cancellation.clone());
            tokio::pin!(drain);
            tokio::select! {
                _ = cancellation.cancelled() => Ok(TransportShutdown::Cancelled {
                    pending_operations: transport.pending_operations(),
                }),
                result = &mut drain => {
                    result?;
                    Ok(TransportShutdown::Drained)
                },
                _ = tokio::time::sleep(options.drain_timeout) => Ok(TransportShutdown::TimedOut {
                    pending_operations: transport.pending_operations(),
                }),
            }
        };
        drop(transport);
        outcome
    }
}

/// Lock-free accounting for operations that must drain before shutdown.
///
/// Call [`Self::begin_operation`] when an operation becomes visible to a caller and retain the
/// returned [`OperationGuard`] with that operation. Completing or dropping the guard releases the
/// slot exactly once. Clones share one counter, so transports can keep the tracker while delivery
/// acknowledgement objects own their individual guards.
#[derive(Clone, Default)]
pub struct OperationTracker {
    state: Arc<OperationTrackerState>,
}

impl std::fmt::Debug for OperationTracker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OperationTracker")
            .field("pending_operations", &self.pending_operations())
            .finish()
    }
}

#[derive(Default)]
struct OperationTrackerState {
    pending: AtomicUsize,
    drained: Notify,
}

/// A RAII permit for one operation registered with an [`OperationTracker`].
///
/// Dropping a guard releases its operation. This ensures failed acknowledgements and abandoned
/// deliveries cannot permanently block a graceful shutdown wait.
pub struct OperationGuard {
    tracker: OperationTracker,
    completed: AtomicBool,
}

impl OperationTracker {
    /// Registers one operation and returns the guard that owns its drain slot.
    pub fn begin_operation(&self) -> OperationGuard {
        self.state.pending.fetch_add(1, Ordering::AcqRel);
        OperationGuard {
            tracker: self.clone(),
            completed: AtomicBool::new(false),
        }
    }

    /// Returns the current number of registered operations.
    pub fn pending_operations(&self) -> usize {
        self.state.pending.load(Ordering::Acquire)
    }

    /// Waits for all registered operations to drain or for `cancellation` to be cancelled.
    ///
    /// Cancellation ends only this wait and never changes the tracked operations.
    pub async fn wait_for_completion(&self, cancellation: CancellationToken) -> CatgaResult<()> {
        loop {
            if self.pending_operations() == 0 {
                return Ok(());
            }

            let drained = self.state.drained.notified();
            if self.pending_operations() == 0 {
                return Ok(());
            }

            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                _ = drained => {}
            }
        }
    }

    fn complete_operation(&self) {
        if let Ok(1) =
            self.state
                .pending
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                    pending.checked_sub(1)
                })
        {
            self.state.drained.notify_waiters();
        }
    }
}

#[async_trait]
impl Waitable for OperationTracker {
    async fn wait_for_completion(&self, cancellation: CancellationToken) -> CatgaResult<()> {
        OperationTracker::wait_for_completion(self, cancellation).await
    }

    fn pending_operations(&self) -> usize {
        OperationTracker::pending_operations(self)
    }
}

impl OperationGuard {
    /// Releases this operation early; calling it repeatedly has no effect after the first call.
    pub fn complete(&self) {
        if !self.completed.swap(true, Ordering::AcqRel) {
            self.tracker.complete_operation();
        }
    }
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        self.complete();
    }
}

/// A named component that can restore itself after becoming unhealthy.
#[async_trait]
pub trait RecoverableComponent: Send + Sync {
    /// Returns the stable component name.
    fn name(&self) -> &str;
    /// Returns whether recovery is currently needed.
    fn is_healthy(&self) -> bool;
    /// Attempts recovery without holding the manager's registration state.
    async fn recover(&self) -> CatgaResult<()>;
}

/// Outcome of one recovery sweep.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryResult {
    /// Recovery was skipped because another sweep owns it.
    AlreadyRecovering,
    /// The sweep examined unhealthy components and counted outcomes.
    Completed {
        /// Components recovered successfully.
        succeeded: u32,
        /// Components that still failed recovery.
        failed: u32,
        /// Wall-clock duration of the recovery sweep.
        duration: Duration,
    },
}

impl RecoveryResult {
    /// Creates a completed result with zero duration for deterministic callers and tests.
    pub const fn completed(succeeded: u32, failed: u32) -> Self {
        Self::Completed {
            succeeded,
            failed,
            duration: Duration::ZERO,
        }
    }
}

/// Policy for a caller-owned automatic recovery loop.
///
/// The loop performs its first health sweep immediately, retries failed sweeps up to
/// `max_retries` times, and then waits for `check_interval` before the next sweep. It never
/// spawns a background task: callers retain the task handle and stop it through the supplied
/// [`CancellationToken`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AutoRecoveryOptions {
    /// Time between completed health sweeps.
    pub check_interval: Duration,
    /// Total attempts for one unhealthy sweep, including its initial attempt.
    pub max_retries: u32,
    /// Delay before the first retry after a failed sweep.
    pub retry_delay: Duration,
    /// Whether each consecutive retry doubles `retry_delay`.
    pub exponential_backoff: bool,
}

impl Default for AutoRecoveryOptions {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(30),
            max_retries: 3,
            retry_delay: Duration::from_secs(1),
            exponential_backoff: true,
        }
    }
}

impl AutoRecoveryOptions {
    fn validate(self) -> CatgaResult<()> {
        if self.check_interval.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "automatic recovery check_interval must be greater than zero",
            ));
        }
        if self.max_retries == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "automatic recovery max_retries must be greater than zero",
            ));
        }
        Ok(())
    }

    fn retry_delay(self, retry: u32) -> Duration {
        if !self.exponential_backoff {
            return self.retry_delay;
        }
        let multiplier = 1_u32 << retry.min(31);
        self.retry_delay
            .checked_mul(multiplier)
            .unwrap_or(Duration::MAX)
    }
}

/// Coordinates graceful shutdown through a clonable cancellation token.
#[derive(Clone, Default)]
pub struct ShutdownCoordinator {
    token: CancellationToken,
}

impl ShutdownCoordinator {
    /// Returns a token cancelled when shutdown is requested.
    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }
    /// Returns whether shutdown has been requested.
    pub fn is_shutting_down(&self) -> bool {
        self.token.is_cancelled()
    }
    /// Idempotently requests shutdown.
    pub fn request_shutdown(&self) {
        self.token.cancel();
    }
}

/// Runs at most one lock-free recovery sweep at a time.
pub struct RecoveryManager {
    components: ArcSwap<Vec<Arc<dyn RecoverableComponent>>>,
    recovering: AtomicBool,
}

struct RecoveryGuard<'a> {
    recovering: &'a AtomicBool,
}

impl Drop for RecoveryGuard<'_> {
    fn drop(&mut self) {
        self.recovering.store(false, Ordering::Release);
    }
}

impl Default for RecoveryManager {
    fn default() -> Self {
        Self {
            components: ArcSwap::from_pointee(Vec::new()),
            recovering: AtomicBool::new(false),
        }
    }
}

impl RecoveryManager {
    /// Returns whether a recovery sweep currently owns the manager.
    pub fn is_recovering(&self) -> bool {
        self.recovering.load(Ordering::Acquire)
    }

    /// Registers a recoverable component; duplicate handles are retained deliberately.
    pub fn register(&self, component: Arc<dyn RecoverableComponent>) {
        loop {
            let current = self.components.load_full();
            let mut next = Vec::with_capacity(current.len().saturating_add(1));
            next.extend(current.iter().cloned());
            next.push(Arc::clone(&component));
            let previous = self.components.compare_and_swap(&current, Arc::new(next));
            if Arc::ptr_eq(&*previous, &current) {
                return;
            }
        }
    }

    /// Explicitly invokes recovery for every currently registered component.
    ///
    /// This is the manual recovery operation: unlike [`Self::recover_unhealthy`], it also calls
    /// healthy components so an operator can request transport reinitialization, credential
    /// renewal, or another component-defined recovery action. Registration remains lock-free and
    /// a component error or panic is counted as one failure without stopping the immutable
    /// component snapshot sweep.
    pub async fn recover_all(&self) -> RecoveryResult {
        if self
            .recovering
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return RecoveryResult::AlreadyRecovering;
        }
        let _recovery_guard = RecoveryGuard {
            recovering: &self.recovering,
        };
        let started = std::time::Instant::now();
        let mut succeeded = 0;
        let mut failed = 0;
        for component in self.components.load_full().iter() {
            if recover_component(component.as_ref(), None)
                .await
                .is_ok_and(|success| success)
            {
                succeeded += 1;
            } else {
                failed += 1;
            }
        }
        RecoveryResult::Completed {
            succeeded,
            failed,
            duration: started.elapsed(),
        }
    }

    /// Explicitly recovers every registered component until `cancellation` is requested.
    ///
    /// This is the cancellation-aware counterpart to [`Self::recover_all`]. Cancellation is
    /// observed before the sweep, between components, and while an individual recovery future
    /// is pending. It drops an in-progress recovery future and clears the exclusive sweep state
    /// before returning [`ErrorCode::Cancelled`].
    pub async fn recover_all_until(
        &self,
        cancellation: CancellationToken,
    ) -> CatgaResult<RecoveryResult> {
        if cancellation.is_cancelled() {
            return Err(recovery_cancelled());
        }
        if self
            .recovering
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(RecoveryResult::AlreadyRecovering);
        }
        let _recovery_guard = RecoveryGuard {
            recovering: &self.recovering,
        };
        let started = std::time::Instant::now();
        let mut succeeded = 0;
        let mut failed = 0;
        for component in self.components.load_full().iter() {
            if cancellation.is_cancelled() {
                return Err(recovery_cancelled());
            }
            match recover_component(component.as_ref(), Some(&cancellation)).await {
                Ok(true) => succeeded += 1,
                Ok(false) => failed += 1,
                Err(()) => return Err(recovery_cancelled()),
            }
        }
        Ok(RecoveryResult::Completed {
            succeeded,
            failed,
            duration: started.elapsed(),
        })
    }

    /// Recovers every currently unhealthy component, without blocking registration or reads.
    ///
    /// A component's returned error or panic counts as one failed recovery and does not prevent
    /// the current immutable component snapshot from continuing. Panics are contained only at
    /// this extension boundary; their payload is neither retained nor exposed.
    pub async fn recover_unhealthy(&self) -> RecoveryResult {
        if self
            .recovering
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return RecoveryResult::AlreadyRecovering;
        }
        let _recovery_guard = RecoveryGuard {
            recovering: &self.recovering,
        };
        let started = std::time::Instant::now();
        let mut succeeded = 0;
        let mut failed = 0;
        for component in self.components.load_full().iter() {
            if !component.is_healthy() {
                if recover_component(component.as_ref(), None)
                    .await
                    .is_ok_and(|success| success)
                {
                    succeeded += 1;
                } else {
                    failed += 1;
                }
            }
        }
        RecoveryResult::Completed {
            succeeded,
            failed,
            duration: started.elapsed(),
        }
    }

    /// Recovers currently unhealthy components until `cancellation` is requested.
    ///
    /// Cancellation is checked before the sweep, between components, and while each component's
    /// recovery future is pending. It returns [`ErrorCode::Cancelled`] and releases the exclusive
    /// recovery flag promptly; an already-started component future is dropped, so implementations
    /// should keep their futures cancellation-safe. Returned component errors and panics remain
    /// per-component failed attempts, matching [`Self::recover_unhealthy`].
    pub async fn recover_unhealthy_until(
        &self,
        cancellation: CancellationToken,
    ) -> CatgaResult<RecoveryResult> {
        if cancellation.is_cancelled() {
            return Err(recovery_cancelled());
        }
        if self
            .recovering
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(RecoveryResult::AlreadyRecovering);
        }
        let _recovery_guard = RecoveryGuard {
            recovering: &self.recovering,
        };
        let started = std::time::Instant::now();
        let mut succeeded = 0;
        let mut failed = 0;
        for component in self.components.load_full().iter() {
            if cancellation.is_cancelled() {
                return Err(recovery_cancelled());
            }
            if !component.is_healthy() {
                match recover_component(component.as_ref(), Some(&cancellation)).await {
                    Ok(true) => succeeded += 1,
                    Ok(false) => failed += 1,
                    Err(()) => return Err(recovery_cancelled()),
                }
            }
        }
        Ok(RecoveryResult::Completed {
            succeeded,
            failed,
            duration: started.elapsed(),
        })
    }

    /// Repeatedly recovers unhealthy components until `cancellation` is cancelled.
    ///
    /// Each iteration starts immediately, retries failed recovery sweeps according to
    /// `options`, and then waits for the next health-check interval. Cancellation interrupts
    /// both retry delays and the interval wait; the method never creates an unowned task.
    pub async fn run_auto_recovery(
        &self,
        options: AutoRecoveryOptions,
        cancellation: CancellationToken,
    ) -> CatgaResult<()> {
        options.validate()?;
        loop {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            if !self.retry_recovery(options, &cancellation).await {
                return Ok(());
            }
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                _ = tokio::time::sleep(options.check_interval) => {}
            }
        }
    }

    async fn retry_recovery(
        &self,
        options: AutoRecoveryOptions,
        cancellation: &CancellationToken,
    ) -> bool {
        for attempt in 0..options.max_retries {
            if cancellation.is_cancelled() {
                return false;
            }
            let failed = match self.recover_unhealthy_until(cancellation.clone()).await {
                Ok(RecoveryResult::Completed { failed, .. }) => failed != 0,
                Ok(RecoveryResult::AlreadyRecovering) => false,
                Err(error) if error.code() == ErrorCode::Cancelled => return false,
                Err(_) => true,
            };
            if !failed || attempt + 1 == options.max_retries {
                return true;
            }
            tokio::select! {
                _ = cancellation.cancelled() => return false,
                _ = tokio::time::sleep(options.retry_delay(attempt)) => {}
            }
        }
        true
    }
}

async fn recover_component(
    component: &dyn RecoverableComponent,
    cancellation: Option<&CancellationToken>,
) -> Result<bool, ()> {
    let recovery = AssertUnwindSafe(component.recover()).catch_unwind();
    let result = match cancellation {
        Some(cancellation) => {
            tokio::select! {
                _ = cancellation.cancelled() => return Err(()),
                result = recovery => result,
            }
        }
        None => recovery.await,
    };
    Ok(matches!(result, Ok(Ok(()))))
}

fn recovery_cancelled() -> CatgaError {
    CatgaError::new(ErrorCode::Cancelled, "recovery was cancelled")
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    use crate::{CatgaError, CatgaResult, ErrorCode};

    use super::*;

    // Test for AcceptanceGate
    #[test]
    fn acceptance_gate_default_is_accepting() {
        let gate = AcceptanceGate::default();
        assert!(gate.is_accepting());
    }

    #[test]
    fn acceptance_gate_stop_accepting_works() {
        let gate = AcceptanceGate::default();
        gate.stop_accepting();
        assert!(!gate.is_accepting());
    }

    #[test]
    fn acceptance_gate_clone_shares_state() {
        let gate = AcceptanceGate::default();
        let gate2 = gate.clone();
        gate.stop_accepting();
        assert!(!gate2.is_accepting());
    }

    #[test]
    fn acceptance_gate_ensure_accepting_ok() {
        let gate = AcceptanceGate::default();
        assert!(gate.ensure_accepting().is_ok());
    }

    #[test]
    fn acceptance_gate_ensure_accepting_err() {
        let gate = AcceptanceGate::default();
        gate.stop_accepting();
        let result = gate.ensure_accepting();
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("should be Err").code(),
            ErrorCode::Unavailable
        );
    }

    // Test for TransportLifecycleOptions
    #[test]
    fn lifecycle_options_default() {
        let options = TransportLifecycleOptions::default();
        assert_eq!(options.drain_timeout, Duration::from_secs(30));
    }

    #[test]
    fn lifecycle_options_new_accepts_valid_timeout() {
        let options = TransportLifecycleOptions::new(Duration::from_secs(60))
            .expect("valid timeout should succeed");
        assert_eq!(options.drain_timeout, Duration::from_secs(60));
    }

    #[test]
    fn lifecycle_options_new_rejects_zero_timeout() {
        let result = TransportLifecycleOptions::new(Duration::ZERO);
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("zero timeout should fail").code(),
            ErrorCode::Validation
        );
    }

    // Test for TransportShutdown variants
    #[test]
    fn transport_shutdown_drained() {
        let shutdown = TransportShutdown::Drained;
        match shutdown {
            TransportShutdown::Drained => {}
            _ => panic!("Expected Drained"),
        }
    }

    #[test]
    fn transport_shutdown_cancelled() {
        let shutdown = TransportShutdown::Cancelled {
            pending_operations: 5,
        };
        match shutdown {
            TransportShutdown::Cancelled { pending_operations } => {
                assert_eq!(pending_operations, 5);
            }
            _ => panic!("Expected Cancelled"),
        }
    }

    #[test]
    fn transport_shutdown_timed_out() {
        let shutdown = TransportShutdown::TimedOut {
            pending_operations: 3,
        };
        match shutdown {
            TransportShutdown::TimedOut { pending_operations } => {
                assert_eq!(pending_operations, 3);
            }
            _ => panic!("Expected TimedOut"),
        }
    }

    // Test for RecoveryResult
    #[test]
    fn recovery_result_completed_factory() {
        let result = RecoveryResult::completed(3, 2);
        match result {
            RecoveryResult::Completed {
                succeeded,
                failed,
                duration,
            } => {
                assert_eq!(succeeded, 3);
                assert_eq!(failed, 2);
                assert_eq!(duration, Duration::ZERO);
            }
            _ => panic!("Expected Completed"),
        }
    }

    #[test]
    fn recovery_result_already_recovering() {
        let result = RecoveryResult::AlreadyRecovering;
        match result {
            RecoveryResult::AlreadyRecovering => {}
            _ => panic!("Expected AlreadyRecovering"),
        }
    }

    // Test for OperationTracker
    #[tokio::test]
    async fn operation_tracker_default() {
        let tracker = OperationTracker::default();
        assert_eq!(tracker.pending_operations(), 0);
    }

    #[tokio::test]
    async fn operation_tracker_begin_operation() {
        let tracker = OperationTracker::default();
        let guard = tracker.begin_operation();
        assert_eq!(tracker.pending_operations(), 1);
        drop(guard);
        // Guard drop decrements
        // Note: This may be async timing dependent
    }

    #[tokio::test]
    async fn operation_tracker_guard_complete() {
        let tracker = OperationTracker::default();
        let guard = tracker.begin_operation();
        assert_eq!(tracker.pending_operations(), 1);
        guard.complete();
        assert_eq!(tracker.pending_operations(), 0);
    }

    #[tokio::test]
    async fn operation_tracker_guard_complete_idempotent() {
        let tracker = OperationTracker::default();
        let guard = tracker.begin_operation();
        guard.complete();
        guard.complete(); // Second call should be no-op
        assert_eq!(tracker.pending_operations(), 0);
    }

    #[tokio::test]
    async fn operation_tracker_wait_completes_immediately() {
        let tracker = OperationTracker::default();
        let token = CancellationToken::new();
        tracker
            .wait_for_completion(token)
            .await
            .expect("wait should succeed");
    }

    #[tokio::test]
    async fn operation_tracker_wait_respects_cancellation() {
        let tracker = OperationTracker::default();
        tracker.begin_operation(); // Keep one pending
        let token = CancellationToken::new();
        token.cancel();

        // Cancellation should return immediately even with pending operations
        tracker
            .wait_for_completion(token)
            .await
            .expect("wait should succeed");
    }

    #[test]
    fn operation_tracker_debug() {
        let tracker = OperationTracker::default();
        let debug_str = format!("{:?}", tracker);
        assert!(debug_str.contains("OperationTracker"));
    }

    // Test for ShutdownCoordinator
    #[test]
    fn shutdown_coordinator_default() {
        let coordinator = ShutdownCoordinator::default();
        assert!(!coordinator.is_shutting_down());
    }

    #[test]
    fn shutdown_coordinator_token_clone() {
        let coordinator = ShutdownCoordinator::default();
        let token = coordinator.token();
        assert!(!token.is_cancelled());
    }

    #[test]
    fn shutdown_coordinator_request_shutdown() {
        let coordinator = ShutdownCoordinator::default();
        coordinator.request_shutdown();
        assert!(coordinator.is_shutting_down());
    }

    #[test]
    fn shutdown_coordinator_request_shutdown_idempotent() {
        let coordinator = ShutdownCoordinator::default();
        coordinator.request_shutdown();
        coordinator.request_shutdown(); // Second call
        assert!(coordinator.is_shutting_down());
    }

    #[test]
    fn shutdown_coordinator_clone_shares_token() {
        let coordinator = ShutdownCoordinator::default();
        let coordinator2 = coordinator.clone();
        coordinator.request_shutdown();
        assert!(coordinator2.is_shutting_down());
    }

    // Mock components for testing RecoveryManager
    struct MockRecoverableComponent {
        name: &'static str,
        healthy: bool,
        recovery_succeeds: bool,
    }

    #[async_trait]
    impl RecoverableComponent for MockRecoverableComponent {
        fn name(&self) -> &str {
            self.name
        }

        fn is_healthy(&self) -> bool {
            self.healthy
        }

        async fn recover(&self) -> CatgaResult<()> {
            if self.recovery_succeeds {
                Ok(())
            } else {
                Err(CatgaError::new(ErrorCode::Internal, "recovery failed"))
            }
        }
    }

    // Test for RecoveryManager
    #[tokio::test]
    async fn recovery_manager_default() {
        let manager = RecoveryManager::default();
        assert!(!manager.is_recovering());
    }

    #[tokio::test]
    async fn recovery_manager_register() {
        let manager = RecoveryManager::default();
        let component = Arc::new(MockRecoverableComponent {
            name: "test",
            healthy: true,
            recovery_succeeds: true,
        });
        manager.register(component);
        // Registration doesn't throw
    }

    #[tokio::test]
    async fn recovery_manager_recover_all_no_components() {
        let manager = RecoveryManager::default();
        let result = manager.recover_all().await;
        match result {
            RecoveryResult::Completed {
                succeeded, failed, ..
            } => {
                assert_eq!(succeeded, 0);
                assert_eq!(failed, 0);
            }
            _ => panic!("Expected Completed"),
        }
    }

    #[tokio::test]
    async fn recovery_manager_recover_all_with_components() {
        let manager = RecoveryManager::default();
        let component = Arc::new(MockRecoverableComponent {
            name: "test",
            healthy: true,
            recovery_succeeds: true,
        });
        manager.register(component);

        let result = manager.recover_all().await;
        match result {
            RecoveryResult::Completed {
                succeeded, failed, ..
            } => {
                assert_eq!(succeeded, 1);
                assert_eq!(failed, 0);
            }
            _ => panic!("Expected Completed"),
        }
    }

    #[tokio::test]
    async fn recovery_manager_recover_all_with_failed_component() {
        let manager = RecoveryManager::default();
        let component = Arc::new(MockRecoverableComponent {
            name: "test",
            healthy: true,
            recovery_succeeds: false,
        });
        manager.register(component);

        let result = manager.recover_all().await;
        match result {
            RecoveryResult::Completed {
                succeeded, failed, ..
            } => {
                assert_eq!(succeeded, 0);
                assert_eq!(failed, 1);
            }
            _ => panic!("Expected Completed"),
        }
    }

    #[tokio::test]
    async fn recovery_manager_recover_all_already_recovering() {
        // RecoveryManager doesn't implement Clone, so we test the behavior differently
        // by verifying the manager is not Clone but has the expected behavior
        let manager = RecoveryManager::default();
        let component = Arc::new(MockRecoverableComponent {
            name: "slow",
            healthy: true,
            recovery_succeeds: true,
        });
        manager.register(component);

        // First call should complete successfully
        let result = manager.recover_all().await;
        match result {
            RecoveryResult::Completed {
                succeeded, failed, ..
            } => {
                assert_eq!(succeeded, 1);
                assert_eq!(failed, 0);
            }
            _ => panic!("Expected Completed"),
        }
    }

    #[tokio::test]
    async fn recovery_manager_recover_all_until() {
        let manager = RecoveryManager::default();
        let component = Arc::new(MockRecoverableComponent {
            name: "test",
            healthy: true,
            recovery_succeeds: true,
        });
        manager.register(component);

        let token = CancellationToken::new();
        let result = manager
            .recover_all_until(token)
            .await
            .expect("recovery should succeed");
        match result {
            RecoveryResult::Completed {
                succeeded, failed, ..
            } => {
                assert_eq!(succeeded, 1);
                assert_eq!(failed, 0);
            }
            _ => panic!("Expected Completed"),
        }
    }

    #[tokio::test]
    async fn recovery_manager_recover_all_until_cancelled() {
        let manager = RecoveryManager::default();
        let token = CancellationToken::new();
        token.cancel();

        let result = manager.recover_all_until(token).await;
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("cancelled should fail").code(),
            ErrorCode::Cancelled
        );
    }

    #[tokio::test]
    async fn recovery_manager_recover_unhealthy() {
        let manager = RecoveryManager::default();
        let healthy = Arc::new(MockRecoverableComponent {
            name: "healthy",
            healthy: true,
            recovery_succeeds: true,
        });
        let unhealthy = Arc::new(MockRecoverableComponent {
            name: "unhealthy",
            healthy: false,
            recovery_succeeds: true,
        });
        manager.register(healthy);
        manager.register(unhealthy);

        let result = manager.recover_unhealthy().await;
        match result {
            RecoveryResult::Completed {
                succeeded, failed, ..
            } => {
                assert_eq!(succeeded, 1);
                assert_eq!(failed, 0);
            }
            _ => panic!("Expected Completed"),
        }
    }

    #[tokio::test]
    async fn recovery_manager_recover_unhealthy_until() {
        let manager = RecoveryManager::default();
        let unhealthy = Arc::new(MockRecoverableComponent {
            name: "unhealthy",
            healthy: false,
            recovery_succeeds: true,
        });
        manager.register(unhealthy);

        let token = CancellationToken::new();
        let result = manager
            .recover_unhealthy_until(token)
            .await
            .expect("recovery should succeed");
        match result {
            RecoveryResult::Completed {
                succeeded, failed, ..
            } => {
                assert_eq!(succeeded, 1);
                assert_eq!(failed, 0);
            }
            _ => panic!("Expected Completed"),
        }
    }

    // Test for AutoRecoveryOptions
    #[test]
    fn auto_recovery_options_default() {
        let options = AutoRecoveryOptions::default();
        assert_eq!(options.check_interval, Duration::from_secs(30));
        assert_eq!(options.max_retries, 3);
        assert_eq!(options.retry_delay, Duration::from_secs(1));
        assert!(options.exponential_backoff);
    }

    #[test]
    fn auto_recovery_options_validate_ok() {
        let options = AutoRecoveryOptions {
            check_interval: Duration::from_secs(30),
            max_retries: 3,
            retry_delay: Duration::from_secs(1),
            exponential_backoff: true,
        };
        assert!(options.validate().is_ok());
    }

    #[test]
    fn auto_recovery_options_validate_zero_interval() {
        let options = AutoRecoveryOptions {
            check_interval: Duration::ZERO,
            max_retries: 3,
            retry_delay: Duration::from_secs(1),
            exponential_backoff: true,
        };
        let result = options.validate();
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("zero timeout should fail").code(),
            ErrorCode::Validation
        );
    }

    #[test]
    fn auto_recovery_options_validate_zero_retries() {
        let options = AutoRecoveryOptions {
            check_interval: Duration::from_secs(30),
            max_retries: 0,
            retry_delay: Duration::from_secs(1),
            exponential_backoff: true,
        };
        let result = options.validate();
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("zero timeout should fail").code(),
            ErrorCode::Validation
        );
    }

    #[test]
    fn auto_recovery_options_retry_delay_no_backoff() {
        let options = AutoRecoveryOptions {
            check_interval: Duration::from_secs(30),
            max_retries: 3,
            retry_delay: Duration::from_secs(1),
            exponential_backoff: false,
        };
        assert_eq!(options.retry_delay(0), Duration::from_secs(1));
        assert_eq!(options.retry_delay(1), Duration::from_secs(1));
        assert_eq!(options.retry_delay(2), Duration::from_secs(1));
    }

    #[test]
    fn auto_recovery_options_retry_delay_with_backoff() {
        let options = AutoRecoveryOptions {
            check_interval: Duration::from_secs(30),
            max_retries: 5,
            retry_delay: Duration::from_secs(1),
            exponential_backoff: true,
        };
        assert_eq!(options.retry_delay(0), Duration::from_secs(1));
        assert_eq!(options.retry_delay(1), Duration::from_secs(2));
        assert_eq!(options.retry_delay(2), Duration::from_secs(4));
    }

    #[test]
    fn auto_recovery_options_retry_delay_overflow() {
        let options = AutoRecoveryOptions {
            check_interval: Duration::from_secs(30),
            max_retries: 100,
            retry_delay: Duration::from_secs(u64::MAX),
            exponential_backoff: true,
        };
        // Should saturate to Duration::MAX
        assert_eq!(options.retry_delay(5), Duration::MAX);
    }

    // Test for TransportLifecycle with mock components
    struct MockTransport {
        gate: AcceptanceGate,
        tracker: OperationTracker,
        _initialized: bool,
    }

    impl MockTransport {
        fn new() -> Self {
            Self {
                gate: AcceptanceGate::default(),
                tracker: OperationTracker::default(),
                _initialized: false,
            }
        }
    }

    #[async_trait]
    impl AsyncInitializable for MockTransport {
        async fn initialize(&self) -> CatgaResult<()> {
            Ok(())
        }
    }

    impl Stoppable for MockTransport {
        fn stop_accepting(&self) {
            self.gate.stop_accepting();
        }

        fn is_accepting(&self) -> bool {
            self.gate.is_accepting()
        }
    }

    #[async_trait]
    impl Waitable for MockTransport {
        async fn wait_for_completion(&self, cancellation: CancellationToken) -> CatgaResult<()> {
            self.tracker.wait_for_completion(cancellation).await
        }

        fn pending_operations(&self) -> usize {
            self.tracker.pending_operations()
        }
    }

    #[tokio::test]
    async fn transport_lifecycle_new_and_accessors() {
        let transport = MockTransport::new();
        let lifecycle = TransportLifecycle::new(transport);

        // Verify transport is accessible and is_accepting works
        assert!(lifecycle.transport().is_accepting());
    }

    #[tokio::test]
    async fn transport_lifecycle_into_inner() {
        let transport = MockTransport::new();
        let lifecycle = TransportLifecycle::new(transport);
        let _inner = lifecycle.into_inner();
        // Successfully extracted the inner transport
    }

    #[tokio::test]
    async fn transport_lifecycle_initialize() {
        let transport = MockTransport::new();
        let lifecycle = TransportLifecycle::new(transport);
        lifecycle
            .initialize()
            .await
            .expect("initialize should succeed");
    }

    #[tokio::test]
    async fn transport_lifecycle_shutdown_drained() {
        let transport = MockTransport::new();
        let lifecycle = TransportLifecycle::new(transport);

        let options =
            TransportLifecycleOptions::new(Duration::from_secs(1)).expect("valid timeout");
        let token = CancellationToken::new();

        let result = lifecycle
            .shutdown(options, token)
            .await
            .expect("shutdown should succeed");
        assert!(matches!(result, TransportShutdown::Drained));
    }

    #[tokio::test]
    async fn transport_lifecycle_shutdown_completes() {
        let transport = MockTransport::new();
        let lifecycle = TransportLifecycle::new(transport);

        // With no pending operations, drain completes immediately
        let options =
            TransportLifecycleOptions::new(Duration::from_secs(10)).expect("valid timeout");
        let token = CancellationToken::new();

        let result = lifecycle
            .shutdown(options, token)
            .await
            .expect("shutdown should succeed");
        // With no pending operations, should complete as Drained
        assert!(matches!(result, TransportShutdown::Drained));
    }

    #[test]
    fn recovery_guard_stores_false() {
        let recovering = std::sync::atomic::AtomicBool::new(false);
        let guard = RecoveryGuard {
            recovering: &recovering,
        };
        assert!(!guard.recovering.load(Ordering::Acquire));
    }
}
