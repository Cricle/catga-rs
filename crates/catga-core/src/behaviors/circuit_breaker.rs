//! Bounded circuit breaking for request pipelines.

use std::{
    collections::VecDeque,
    sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant},
};

use async_trait::async_trait;

use crate::{
    Behavior, CatgaError, CatgaResult, ErrorCode, Next, Request,
    telemetry::RESILIENCE_CIRCUIT_OPENED,
};

const CLOSED: u8 = 0;
const OPEN: u8 = 1;
const HALF_OPEN: u8 = 2;
const MAX_SAMPLING_WINDOW: usize = 10_000;

/// Validated failure-ratio configuration for a [`CircuitBreakerBehavior`].
///
/// Construct this type through [`CircuitBreakerOptions::builder`]. The
/// compatibility defaults use a window and minimum throughput equal to the
/// failure threshold, with a 100 percent failure ratio. That preserves the
/// consecutive-failure behavior of [`CircuitBreakerBehavior::new`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CircuitBreakerOptions {
    failure_threshold: usize,
    reset_timeout: Duration,
    sampling_window: usize,
    minimum_throughput: usize,
    failure_ratio_numerator: u32,
    failure_ratio_denominator: u32,
}

impl CircuitBreakerOptions {
    /// Starts a circuit-breaker configuration with compatibility defaults.
    ///
    /// Validation occurs when [`CircuitBreakerOptionsBuilder::build`] is
    /// called, so callers can set interdependent throughput and window limits
    /// in either order.
    pub const fn builder(
        failure_threshold: usize,
        reset_timeout: Duration,
    ) -> CircuitBreakerOptionsBuilder {
        CircuitBreakerOptionsBuilder {
            failure_threshold,
            reset_timeout,
            sampling_window: failure_threshold,
            minimum_throughput: failure_threshold,
            failure_ratio_numerator: 1,
            failure_ratio_denominator: 1,
        }
    }

    /// Returns the maximum number of recent classified outcomes retained.
    pub const fn sampling_window(self) -> usize {
        self.sampling_window
    }

    /// Returns the number of classified outcomes required before opening.
    pub const fn minimum_throughput(self) -> usize {
        self.minimum_throughput
    }

    /// Returns the numerator of the configured failure ratio.
    pub const fn failure_ratio_numerator(self) -> u32 {
        self.failure_ratio_numerator
    }

    /// Returns the denominator of the configured failure ratio.
    pub const fn failure_ratio_denominator(self) -> u32 {
        self.failure_ratio_denominator
    }

    /// Returns the open-circuit cooldown before a recovery probe is permitted.
    pub const fn reset_timeout(self) -> Duration {
        self.reset_timeout
    }
}

/// Builder for validated [`CircuitBreakerOptions`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CircuitBreakerOptionsBuilder {
    failure_threshold: usize,
    reset_timeout: Duration,
    sampling_window: usize,
    minimum_throughput: usize,
    failure_ratio_numerator: u32,
    failure_ratio_denominator: u32,
}

impl CircuitBreakerOptionsBuilder {
    /// Sets the maximum number of recent classified outcomes retained.
    ///
    /// The value must be between one and 10,000 inclusive.
    pub const fn sampling_window(mut self, sampling_window: usize) -> Self {
        self.sampling_window = sampling_window;
        self
    }

    /// Sets the minimum number of classified outcomes required before opening.
    ///
    /// The value must be positive and no larger than the sampling window.
    pub const fn minimum_throughput(mut self, minimum_throughput: usize) -> Self {
        self.minimum_throughput = minimum_throughput;
        self
    }

    /// Sets the failure ratio as an exact positive fraction.
    ///
    /// The numerator must not exceed the denominator. For example, `(1, 2)`
    /// opens when at least half of the retained outcomes failed.
    pub const fn failure_ratio(mut self, numerator: u32, denominator: u32) -> Self {
        self.failure_ratio_numerator = numerator;
        self.failure_ratio_denominator = denominator;
        self
    }

    /// Validates and returns the circuit-breaker options.
    pub fn build(self) -> CatgaResult<CircuitBreakerOptions> {
        if self.failure_threshold == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "circuit breaker failure threshold must be greater than zero",
            ));
        }
        if self.reset_timeout.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "circuit breaker reset timeout must be greater than zero",
            ));
        }
        if self.sampling_window == 0 || self.sampling_window > MAX_SAMPLING_WINDOW {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "circuit breaker sampling window must be between one and 10000",
            ));
        }
        if self.minimum_throughput == 0 || self.minimum_throughput > self.sampling_window {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "circuit breaker minimum throughput must fit within the sampling window",
            ));
        }
        if self.failure_ratio_numerator == 0
            || self.failure_ratio_denominator == 0
            || self.failure_ratio_numerator > self.failure_ratio_denominator
        {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "circuit breaker failure ratio must be a positive fraction no greater than one",
            ));
        }
        Ok(CircuitBreakerOptions {
            failure_threshold: self.failure_threshold,
            reset_timeout: self.reset_timeout,
            sampling_window: self.sampling_window,
            minimum_throughput: self.minimum_throughput,
            failure_ratio_numerator: self.failure_ratio_numerator,
            failure_ratio_denominator: self.failure_ratio_denominator,
        })
    }
}

/// Rejects requests after repeated failures and probes recovery after a cooldown.
///
/// The outcome window is protected only while adding one completed result; no
/// lock is held while a request handler is awaited. One behavior instance can
/// therefore be shared by an immutable pipeline without serializing normal
/// request execution.
pub struct CircuitBreakerBehavior {
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

impl CircuitBreakerBehavior {
    /// Opens the circuit after `failure_threshold` consecutive transient failures.
    ///
    /// This compatibility constructor uses a 100 percent failure ratio and a
    /// sampling window equal to `failure_threshold`.
    pub fn new(failure_threshold: usize, reset_timeout: Duration) -> CatgaResult<Self> {
        let options = CircuitBreakerOptions::builder(failure_threshold, reset_timeout).build()?;
        Ok(Self::with_options(options))
    }

    /// Creates a circuit breaker from validated failure-ratio options.
    pub fn with_options(options: CircuitBreakerOptions) -> Self {
        Self {
            outcomes: Mutex::new(OutcomeWindow::new(options.sampling_window)),
            state: AtomicU8::new(CLOSED),
            opened_at_ns: AtomicU64::new(0),
            half_open_probe: AtomicBool::new(false),
            minimum_throughput: options.minimum_throughput,
            failure_ratio_numerator: options.failure_ratio_numerator,
            failure_ratio_denominator: options.failure_ratio_denominator,
            reset_timeout: options.reset_timeout,
            started_at: Instant::now(),
        }
    }

    fn permit(&self) -> CatgaResult<bool> {
        loop {
            match self.state.load(Ordering::Acquire) {
                CLOSED => return Ok(false),
                OPEN => {
                    if self.elapsed_since_open() < self.reset_timeout {
                        return Err(circuit_open_error());
                    }
                    if self
                        .state
                        .compare_exchange(OPEN, HALF_OPEN, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                    {
                        continue;
                    }
                    self.half_open_probe.store(false, Ordering::Release);
                }
                HALF_OPEN => {
                    if self
                        .half_open_probe
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        return Ok(true);
                    }
                    return Err(circuit_open_error());
                }
                _ => {
                    return Err(CatgaError::new(
                        ErrorCode::Internal,
                        "circuit breaker has an invalid state",
                    ));
                }
            }
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

    fn record_failure(&self, probe: bool) {
        if probe {
            self.open();
            return;
        }
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
        if self.state.swap(OPEN, Ordering::AcqRel) == OPEN {
            return;
        }
        self.opened_at_ns
            .store(self.elapsed_ns(), Ordering::Release);
        self.half_open_probe.store(false, Ordering::Release);
        metrics::counter!(RESILIENCE_CIRCUIT_OPENED).increment(1);
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

#[async_trait]
impl<M: Request> Behavior<M> for CircuitBreakerBehavior {
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response> {
        let probe = self.permit()?;
        let mut guard = ProbeGuard::new(self, probe);
        let result = next.run(message).await;
        guard.complete(result.as_ref());
        result
    }
}

struct ProbeGuard<'a> {
    breaker: &'a CircuitBreakerBehavior,
    probe: bool,
    completed: bool,
}

impl<'a> ProbeGuard<'a> {
    fn new(breaker: &'a CircuitBreakerBehavior, probe: bool) -> Self {
        Self {
            breaker,
            probe,
            completed: false,
        }
    }

    fn complete<T>(&mut self, result: Result<&T, &CatgaError>) {
        match result {
            Ok(_) => self.breaker.record_success(self.probe),
            Err(error) if error.code() != ErrorCode::Cancelled && error.is_retryable() => {
                self.breaker.record_failure(self.probe);
            }
            Err(_) if self.probe => self.breaker.record_success(true),
            Err(_) => {}
        }
        self.completed = true;
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

impl Drop for ProbeGuard<'_> {
    fn drop(&mut self) {
        if self.probe && !self.completed {
            self.breaker.open();
        }
    }
}

fn circuit_open_error() -> CatgaError {
    CatgaError::new(ErrorCode::Transient, "circuit breaker is open")
}
