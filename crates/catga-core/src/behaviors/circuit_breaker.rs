//! Lock-free circuit breaking for request pipelines.

use std::{
    sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
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

/// Rejects requests after repeated failures and probes recovery after a cooldown.
///
/// All mutable state is atomic. One behavior instance can therefore be shared by
/// an immutable pipeline without serializing normal request execution.
pub struct CircuitBreakerBehavior {
    failures: AtomicUsize,
    state: AtomicU8,
    opened_at_ns: AtomicU64,
    half_open_probe: AtomicBool,
    failure_threshold: usize,
    reset_timeout: Duration,
    started_at: Instant,
}

impl CircuitBreakerBehavior {
    /// Opens the circuit after `failure_threshold` consecutive failed requests.
    pub fn new(failure_threshold: usize, reset_timeout: Duration) -> CatgaResult<Self> {
        if failure_threshold == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "circuit breaker failure threshold must be greater than zero",
            ));
        }
        if reset_timeout.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "circuit breaker reset timeout must be greater than zero",
            ));
        }
        Ok(Self {
            failures: AtomicUsize::new(0),
            state: AtomicU8::new(CLOSED),
            opened_at_ns: AtomicU64::new(0),
            half_open_probe: AtomicBool::new(false),
            failure_threshold,
            reset_timeout,
            started_at: Instant::now(),
        })
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

    fn record_success(&self) {
        self.failures.store(0, Ordering::Release);
        self.half_open_probe.store(false, Ordering::Release);
        self.state.store(CLOSED, Ordering::Release);
    }

    fn record_failure(&self, probe: bool) {
        if probe {
            self.open();
            return;
        }

        let failures = self.failures.fetch_add(1, Ordering::AcqRel) + 1;
        if failures >= self.failure_threshold {
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
}

#[async_trait]
impl<M: Request> Behavior<M> for CircuitBreakerBehavior {
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response> {
        let probe = self.permit()?;
        let mut guard = ProbeGuard::new(self, probe);
        let result = next.run(message).await;
        guard.complete(result.is_ok());
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

    fn complete(&mut self, success: bool) {
        if success {
            self.breaker.record_success();
        } else {
            self.breaker.record_failure(self.probe);
        }
        self.completed = true;
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
