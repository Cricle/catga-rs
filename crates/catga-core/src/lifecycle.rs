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
