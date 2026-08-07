//! Explicit, bounded resilience execution for transport and persistence calls.

use std::{
    collections::VecDeque,
    future::Future,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError},
    time::sleep,
};
use tokio_util::sync::CancellationToken;

use crate::{
    CatgaError, CatgaResult, CircuitBreakerOptions, ErrorCode, RetryJitter,
    retry_jitter::RetryJitterState,
    telemetry::{RESILIENCE_CIRCUIT_OPENED, RESILIENCE_RETRIES},
};

const CLOSED: u8 = 0;
const OPEN: u8 = 1;
const HALF_OPEN: u8 = 2;

/// Configuration for one caller-owned [`ResilienceExecutor`].
///
/// A zero `max_concurrent` disables admission limiting. Otherwise, at most
/// `max_concurrent + max_queued` calls are retained. The timeout applies to
/// each attempt; retryable errors other than `Cancelled` are retried or counted
/// by the circuit breaker.
///
/// ```
/// use std::time::Duration;
/// use catga_core::ResilienceOptions;
///
/// let options = ResilienceOptions {
///     max_concurrent: 10,
///     max_queued: 50,
///     timeout: Duration::from_secs(5),
///     max_retries: 3,
///     retry_delay: Duration::from_millis(100),
///     ..ResilienceOptions::default()
/// };
/// assert_eq!(options.max_retries, 3);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResilienceOptions {
    /// Operations allowed to execute at once; zero disables admission limiting.
    pub max_concurrent: usize,
    /// Callers allowed to await an execution permit.
    pub max_queued: usize,
    /// Maximum duration of one operation attempt.
    pub timeout: Duration,
    /// Additional attempts after the first recoverable failure.
    pub max_retries: u32,
    /// Delay before the first retry; later delays double with saturation.
    pub retry_delay: Duration,
    /// Recoverable failed calls required to open the circuit.
    pub circuit_failure_threshold: usize,
    /// Open-circuit duration before one recovery probe is allowed.
    pub circuit_reset_timeout: Duration,
}

impl Default for ResilienceOptions {
    fn default() -> Self {
        Self {
            max_concurrent: 0,
            max_queued: 0,
            timeout: Duration::from_secs(3),
            max_retries: 0,
            retry_delay: Duration::ZERO,
            circuit_failure_threshold: 20,
            circuit_reset_timeout: Duration::from_secs(30),
        }
    }
}

impl ResilienceOptions {
    fn validate(self) -> CatgaResult<()> {
        if self.max_concurrent == 0 && self.max_queued != 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "resilience max_queued requires max_concurrent",
            ));
        }
        if self.timeout.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "resilience timeout must be positive",
            ));
        }
        Ok(())
    }

    fn retry_delay(self, retry: u32) -> Duration {
        self.retry_delay.saturating_mul(1_u32 << retry.min(31))
    }
}

/// Executes caller-supplied work with bounded admission, timeout, retry, and
/// circuit-breaking behavior.
///
/// The executor owns no background task. Share it through [`Arc`] when several
/// adapters must share one budget and circuit. Each operation receives a child
/// cancellation token which is cancelled when that attempt times out.
pub struct ResilienceExecutor {
    options: ResilienceOptions,
    permits: Option<Arc<Semaphore>>,
    waiting: AtomicUsize,
    retry_jitter: RetryJitterState,
    circuit: Circuit,
}

impl ResilienceExecutor {
    /// Creates an executor after validating admission and timing bounds.
    pub fn new(options: ResilienceOptions) -> CatgaResult<Self> {
        Self::with_jitter(options, RetryJitter::production_default())
    }

    /// Creates an executor with an explicit bounded retry-jitter policy.
    ///
    /// [`RetryJitter::None`] preserves calculated retry delays without jitter.
    pub fn with_jitter(options: ResilienceOptions, retry_jitter: RetryJitter) -> CatgaResult<Self> {
        let circuit = CircuitBreakerOptions::builder(
            options.circuit_failure_threshold,
            options.circuit_reset_timeout,
        )
        .build()?;
        Self::with_policies(options, circuit, retry_jitter)
    }

    /// Creates an executor with explicit circuit and retry-jitter policies.
    ///
    /// The supplied circuit options replace the compatibility circuit fields in
    /// [`ResilienceOptions`], while admission, timeout, and retry limits still
    /// come from `options`.
    pub fn with_policies(
        options: ResilienceOptions,
        circuit_options: CircuitBreakerOptions,
        retry_jitter: RetryJitter,
    ) -> CatgaResult<Self> {
        options.validate()?;
        Ok(Self {
            permits: (options.max_concurrent != 0)
                .then(|| Arc::new(Semaphore::new(options.max_concurrent))),
            waiting: AtomicUsize::new(0),
            retry_jitter: RetryJitterState::new(retry_jitter),
            circuit: Circuit::new(circuit_options),
            options,
        })
    }

    /// Returns this executor's configured retry-jitter policy without sampling it.
    pub const fn jitter_policy(&self) -> RetryJitter {
        self.retry_jitter.policy()
    }

    /// Executes `operation` under this executor's policy.
    ///
    /// The operation is called once plus at most `max_retries` times. Caller
    /// cancellation prevents new attempts; timeout cancels the attempt token
    /// before returning [`ErrorCode::Timeout`].
    pub async fn execute<T, F, Fut>(
        &self,
        cancellation: CancellationToken,
        operation: F,
    ) -> CatgaResult<T>
    where
        F: Fn(CancellationToken) -> Fut,
        Fut: Future<Output = CatgaResult<T>>,
    {
        if cancellation.is_cancelled() {
            return Err(cancelled());
        }
        let _permit = self.acquire(cancellation.clone()).await?;
        let probe = self.circuit.permit()?;
        let result = self.attempts(cancellation, operation).await;
        self.circuit.complete(probe, result.as_ref());
        result
    }

    async fn acquire(
        &self,
        cancellation: CancellationToken,
    ) -> CatgaResult<Option<OwnedSemaphorePermit>> {
        let Some(permits) = &self.permits else {
            return Ok(None);
        };
        match Arc::clone(permits).try_acquire_owned() {
            Ok(permit) => return Ok(Some(permit)),
            Err(TryAcquireError::Closed) => {
                return Err(unavailable("resilience executor is closed"));
            }
            Err(TryAcquireError::NoPermits) => {}
        }
        loop {
            let waiting = self.waiting.load(Ordering::Acquire);
            if waiting >= self.options.max_queued {
                return Err(unavailable("resilience executor admission queue is full"));
            }
            if self
                .waiting
                .compare_exchange_weak(waiting, waiting + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
        let result = tokio::select! {
            _ = cancellation.cancelled() => Err(cancelled()),
            permit = Arc::clone(permits).acquire_owned() => permit.map(Some).map_err(|_| unavailable("resilience executor is closed")),
        };
        self.waiting.fetch_sub(1, Ordering::AcqRel);
        result
    }

    async fn attempts<T, F, Fut>(
        &self,
        cancellation: CancellationToken,
        operation: F,
    ) -> CatgaResult<T>
    where
        F: Fn(CancellationToken) -> Fut,
        Fut: Future<Output = CatgaResult<T>>,
    {
        for retry in 0..=self.options.max_retries {
            if cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let attempt = cancellation.child_token();
            let result = tokio::select! {
                _ = cancellation.cancelled() => Err(cancelled()),
                result = tokio::time::timeout(self.options.timeout, operation(attempt.clone())) => match result {
                    Ok(result) => result,
                    Err(_) => { attempt.cancel(); Err(CatgaError::new(ErrorCode::Timeout, "resilience operation timed out")) }
                },
            };
            match result {
                Err(error) if retry < self.options.max_retries && recoverable(&error) => {
                    metrics::counter!(RESILIENCE_RETRIES).increment(1);
                    let delay = self.retry_jitter.delay(self.options.retry_delay(retry));
                    if !delay.is_zero() {
                        let _pending = crate::telemetry::retry_pending();
                        tokio::select! { _ = cancellation.cancelled() => return Err(cancelled()), _ = sleep(delay) => {} }
                    }
                }
                result => return result,
            }
        }
        Err(CatgaError::new(
            ErrorCode::Internal,
            "resilience retry loop completed without a result",
        ))
    }
}

struct Circuit {
    outcomes: Mutex<OutcomeWindow>,
    state: AtomicU8,
    opened_at_ns: AtomicU64,
    half_open_probe: AtomicBool,
    minimum_throughput: usize,
    failure_ratio_numerator: u32,
    failure_ratio_denominator: u32,
    reset_timeout: Duration,
    started_at: Instant,
}

impl Circuit {
    fn new(options: CircuitBreakerOptions) -> Self {
        Self {
            outcomes: Mutex::new(OutcomeWindow::new(options.sampling_window())),
            state: AtomicU8::new(CLOSED),
            opened_at_ns: AtomicU64::new(0),
            half_open_probe: AtomicBool::new(false),
            minimum_throughput: options.minimum_throughput(),
            failure_ratio_numerator: options.failure_ratio_numerator(),
            failure_ratio_denominator: options.failure_ratio_denominator(),
            reset_timeout: options.reset_timeout(),
            started_at: Instant::now(),
        }
    }

    fn permit(&self) -> CatgaResult<bool> {
        loop {
            match self.state.load(Ordering::Acquire) {
                CLOSED => return Ok(false),
                OPEN if self.elapsed_since_open() < self.reset_timeout => {
                    return Err(circuit_open());
                }
                OPEN => {
                    if self
                        .state
                        .compare_exchange(OPEN, HALF_OPEN, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        self.half_open_probe.store(false, Ordering::Release);
                    }
                }
                HALF_OPEN => match self.half_open_probe.compare_exchange(
                    false,
                    true,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return Ok(true),
                    Err(_) => return Err(circuit_open()),
                },
                _ => {
                    return Err(CatgaError::new(
                        ErrorCode::Internal,
                        "resilience circuit has an invalid state",
                    ));
                }
            }
        }
    }

    fn complete<T>(&self, probe: bool, result: Result<&T, &CatgaError>) {
        match result {
            Ok(_) => self.record_success(probe),
            Err(error) if !recoverable(error) && probe => self.record_success(true),
            Err(error) if !recoverable(error) => {}
            Err(_) if probe => self.open(),
            Err(_) => self.record_failure(),
        }
    }

    fn record_success(&self, probe: bool) {
        let mut outcomes = self.lock_outcomes();
        if probe {
            outcomes.clear();
            self.half_open_probe.store(false, Ordering::Release);
            self.state.store(CLOSED, Ordering::Release);
        } else {
            outcomes.push(true);
        }
    }

    fn record_failure(&self) {
        let opens = {
            let mut outcomes = self.lock_outcomes();
            outcomes.push(false);
            outcomes.len() >= self.minimum_throughput
                && (outcomes.failures() as u128)
                    .saturating_mul(u128::from(self.failure_ratio_denominator))
                    >= (outcomes.len() as u128)
                        .saturating_mul(u128::from(self.failure_ratio_numerator))
        };
        if opens {
            self.open();
        }
    }

    fn open(&self) {
        if self.state.swap(OPEN, Ordering::AcqRel) != OPEN {
            self.opened_at_ns
                .store(self.elapsed_ns(), Ordering::Release);
            self.half_open_probe.store(false, Ordering::Release);
            metrics::counter!(RESILIENCE_CIRCUIT_OPENED).increment(1);
        }
    }

    fn elapsed_since_open(&self) -> Duration {
        Duration::from_nanos(
            self.elapsed_ns()
                .saturating_sub(self.opened_at_ns.load(Ordering::Acquire)),
        )
    }
    fn elapsed_ns(&self) -> u64 {
        self.started_at
            .elapsed()
            .as_nanos()
            .min(u128::from(u64::MAX)) as u64
    }

    fn lock_outcomes(&self) -> MutexGuard<'_, OutcomeWindow> {
        match self.outcomes.lock() {
            Ok(outcomes) => outcomes,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

struct OutcomeWindow {
    outcomes: VecDeque<bool>,
    failures: usize,
    limit: usize,
}

impl OutcomeWindow {
    fn new(limit: usize) -> Self {
        Self {
            outcomes: VecDeque::with_capacity(limit),
            failures: 0,
            limit,
        }
    }

    fn clear(&mut self) {
        self.outcomes.clear();
        self.failures = 0;
    }

    fn push(&mut self, success: bool) {
        if self.outcomes.len() == self.limit && self.outcomes.pop_front() == Some(false) {
            self.failures = self.failures.saturating_sub(1);
        }
        if !success {
            self.failures = self.failures.saturating_add(1);
        }
        self.outcomes.push_back(success);
    }

    fn len(&self) -> usize {
        self.outcomes.len()
    }

    fn failures(&self) -> usize {
        self.failures
    }
}

fn recoverable(error: &CatgaError) -> bool {
    error.code() != ErrorCode::Cancelled && error.is_retryable()
}
fn cancelled() -> CatgaError {
    CatgaError::new(ErrorCode::Cancelled, "resilience operation was cancelled")
}
fn unavailable(message: &'static str) -> CatgaError {
    CatgaError::new(ErrorCode::Unavailable, message)
}
fn circuit_open() -> CatgaError {
    CatgaError::new(ErrorCode::Transient, "resilience circuit is open")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resilience_options_default() {
        let options = ResilienceOptions::default();
        assert_eq!(options.max_concurrent, 0);
        assert_eq!(options.max_queued, 0);
        assert_eq!(options.timeout, Duration::from_secs(3));
        assert_eq!(options.max_retries, 0);
        assert_eq!(options.retry_delay, Duration::ZERO);
        assert_eq!(options.circuit_failure_threshold, 20);
        assert_eq!(options.circuit_reset_timeout, Duration::from_secs(30));
    }

    #[test]
    fn resilience_options_validate_accepts_valid_config() {
        let options = ResilienceOptions::default();
        assert!(options.validate().is_ok());
    }

    #[test]
    fn resilience_options_validate_rejects_queue_without_concurrent() {
        let options = ResilienceOptions {
            max_concurrent: 0,
            max_queued: 1,
            ..ResilienceOptions::default()
        };
        let result = options.validate();
        assert!(matches!(result, Err(e) if e.code() == ErrorCode::Validation));
    }

    #[test]
    fn resilience_options_validate_rejects_zero_timeout() {
        let options = ResilienceOptions {
            timeout: Duration::ZERO,
            ..ResilienceOptions::default()
        };
        let result = options.validate();
        assert!(matches!(result, Err(e) if e.code() == ErrorCode::Validation));
    }

    #[test]
    fn resilience_options_retry_delay_exponential() {
        let options = ResilienceOptions {
            retry_delay: Duration::from_millis(100),
            ..ResilienceOptions::default()
        };

        assert_eq!(options.retry_delay(0), Duration::from_millis(100));
        assert_eq!(options.retry_delay(1), Duration::from_millis(200));
        assert_eq!(options.retry_delay(2), Duration::from_millis(400));
        assert_eq!(options.retry_delay(3), Duration::from_millis(800));
    }

    #[test]
    fn resilience_options_retry_delay_saturates() {
        let options = ResilienceOptions {
            retry_delay: Duration::from_millis(100),
            ..ResilienceOptions::default()
        };

        // Very large retry numbers should saturate
        let saturated = options.retry_delay(100);
        assert_eq!(saturated, Duration::from_millis(100) * (1_u32 << 31));
    }

    #[test]
    fn resilience_options_retry_delay_zero_base() {
        let options = ResilienceOptions {
            retry_delay: Duration::ZERO,
            ..ResilienceOptions::default()
        };

        assert_eq!(options.retry_delay(0), Duration::ZERO);
        assert_eq!(options.retry_delay(10), Duration::ZERO);
    }

    #[test]
    fn recoverable_detects_retryable_errors() {
        let transient = CatgaError::new(ErrorCode::Transient, "transient");
        assert!(recoverable(&transient));

        let timeout = CatgaError::new(ErrorCode::Timeout, "timeout");
        assert!(recoverable(&timeout));

        let unavailable = CatgaError::new(ErrorCode::Unavailable, "unavailable");
        assert!(recoverable(&unavailable));
    }

    #[test]
    fn recoverable_rejects_non_retryable_errors() {
        let validation = CatgaError::new(ErrorCode::Validation, "validation");
        assert!(!recoverable(&validation));

        let cancelled = CatgaError::new(ErrorCode::Cancelled, "cancelled");
        assert!(!recoverable(&cancelled));

        let internal = CatgaError::new(ErrorCode::Internal, "internal");
        assert!(!recoverable(&internal));
    }

    #[test]
    fn cancelled_error_has_correct_code() {
        let error = cancelled();
        assert_eq!(error.code(), ErrorCode::Cancelled);
    }

    #[test]
    fn unavailable_error_has_correct_code_and_message() {
        let error = unavailable("test message");
        assert_eq!(error.code(), ErrorCode::Unavailable);
    }

    #[test]
    fn circuit_open_error_has_correct_code() {
        let error = circuit_open();
        assert_eq!(error.code(), ErrorCode::Transient);
    }

    #[test]
    fn outcome_window_new_creates_empty_window() {
        let window = OutcomeWindow::new(5);
        assert_eq!(window.len(), 0);
        assert_eq!(window.failures(), 0);
    }

    #[test]
    fn outcome_window_clear_resets_state() {
        let mut window = OutcomeWindow::new(5);
        window.push(true);
        window.push(false);
        assert_eq!(window.len(), 2);
        assert_eq!(window.failures(), 1);

        window.clear();
        assert_eq!(window.len(), 0);
        assert_eq!(window.failures(), 0);
    }

    #[test]
    fn outcome_window_eviction_behavior() {
        let mut window = OutcomeWindow::new(3);

        window.push(false); // failures: 1
        window.push(false); // failures: 2
        window.push(false); // failures: 3
        assert_eq!(window.len(), 3);
        assert_eq!(window.failures(), 3);

        // Adding 4th element should evict the first false
        window.push(true);
        assert_eq!(window.len(), 3);
        assert_eq!(window.failures(), 2); // One false was evicted
    }

    #[test]
    fn outcome_window_multiple_successes_then_failures() {
        let mut window = OutcomeWindow::new(10);

        for _ in 0..5 {
            window.push(true);
        }
        assert_eq!(window.failures(), 0);

        for _ in 0..3 {
            window.push(false);
        }
        assert_eq!(window.failures(), 3);
    }

    #[test]
    fn resilience_executor_new_accepts_valid_options() {
        let options = ResilienceOptions::default();
        let executor = ResilienceExecutor::new(options);
        assert!(executor.is_ok());
    }

    #[test]
    fn resilience_executor_new_rejects_invalid_options() {
        let options = ResilienceOptions {
            max_queued: 10,
            ..ResilienceOptions::default()
        };
        let executor = ResilienceExecutor::new(options);
        assert!(executor.is_err());
    }

    #[test]
    fn resilience_executor_with_jitter_accepts_custom_jitter() {
        let options = ResilienceOptions::default();
        let executor = ResilienceExecutor::with_jitter(options, RetryJitter::none());
        assert!(executor.is_ok());
    }

    #[test]
    fn resilience_executor_with_policies_accepts_valid_input() {
        let options = ResilienceOptions::default();
        let circuit_options = CircuitBreakerOptions::builder(5, Duration::from_secs(30))
            .build()
            .expect("valid options");
        let executor =
            ResilienceExecutor::with_policies(options, circuit_options, RetryJitter::none());
        assert!(executor.is_ok());
    }

    #[test]
    fn resilience_executor_jitter_policy_returns_configured_policy() {
        let options = ResilienceOptions::default();
        let executor =
            ResilienceExecutor::with_jitter(options, RetryJitter::none()).expect("valid options");
        assert_eq!(executor.jitter_policy(), RetryJitter::none());

        let fixed_jitter = RetryJitter::fixed(Duration::from_millis(100));
        let executor =
            ResilienceExecutor::with_jitter(options, fixed_jitter).expect("valid options");
        assert_eq!(executor.jitter_policy(), fixed_jitter);
    }
}
