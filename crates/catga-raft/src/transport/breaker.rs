//! Circuit breaker implementation for fault isolation.
//!
//! This module provides the `CircuitBreaker` type that implements the
//! circuit breaker pattern to prevent cascading failures.
//!
//! # States
//!
//! - `Closed`: Normal operation, requests pass through
//! - `Open`: Failure threshold exceeded, requests are blocked
//! - `HalfOpen`: Testing recovery, limited requests allowed
//!
//! # Configuration
//!
//! - `failure_threshold`: Number of failures before opening
//! - `recovery_timeout`: Time to wait before attempting recovery
//! - `half_open_max_calls`: Max requests in half-open state

use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

/// Circuit breaker is in normal state, requests pass through.
const STATE_CLOSED: u8 = 0;
/// Circuit breaker is open, requests are blocked.
const STATE_OPEN: u8 = 1;
/// Circuit breaker is half-open, testing recovery.
const STATE_HALF_OPEN: u8 = 2;

/// Default failure threshold.
pub const DEFAULT_FAILURE_THRESHOLD: usize = 5;
/// Default recovery timeout in seconds.
///
/// Kept short on purpose: while the breaker is open every message to the
/// peer is dropped, and raft heartbeats/elections only tolerate a few
/// missed rounds. A long blackout would turn one slow peer into a
/// partition; 2s bounds the damage while still breaking hot failure loops.
pub const DEFAULT_RECOVERY_TIMEOUT_SECS: u64 = 2;
/// Default max calls in half-open state.
pub const DEFAULT_HALF_OPEN_MAX_CALLS: usize = 3;

/// Circuit breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitBreakerState {
    /// Normal operation, requests pass through.
    Closed,
    /// Circuit is open, requests are blocked.
    Open,
    /// Testing recovery, limited requests allowed.
    HalfOpen,
}

impl CircuitBreakerState {
    /// Convert from raw u8 value.
    fn from_u8(value: u8) -> Self {
        match value {
            STATE_OPEN => CircuitBreakerState::Open,
            STATE_HALF_OPEN => CircuitBreakerState::HalfOpen,
            _ => CircuitBreakerState::Closed,
        }
    }

    /// Convert to raw u8 value.
    fn as_u8(&self) -> u8 {
        match self {
            CircuitBreakerState::Closed => STATE_CLOSED,
            CircuitBreakerState::Open => STATE_OPEN,
            CircuitBreakerState::HalfOpen => STATE_HALF_OPEN,
        }
    }
}

/// Circuit breaker configuration.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before opening the circuit.
    pub failure_threshold: usize,
    /// Time to wait before transitioning from Open to HalfOpen.
    pub recovery_timeout: Duration,
    /// Maximum number of requests allowed in HalfOpen state.
    pub half_open_max_calls: usize,
    /// Minimum number of requests before counting failures (prevents flapping).
    pub minimum_requests: usize,
    /// Success rate threshold in half-open state to close the circuit.
    pub success_threshold: f64,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: DEFAULT_FAILURE_THRESHOLD,
            recovery_timeout: Duration::from_secs(DEFAULT_RECOVERY_TIMEOUT_SECS),
            half_open_max_calls: DEFAULT_HALF_OPEN_MAX_CALLS,
            minimum_requests: 1,
            success_threshold: 0.5,
        }
    }
}

impl CircuitBreakerConfig {
    /// Create a new config with custom failure threshold.
    pub fn with_failure_threshold(mut self, threshold: usize) -> Self {
        self.failure_threshold = threshold;
        self
    }

    /// Create a new config with custom recovery timeout.
    pub fn with_recovery_timeout(mut self, timeout: Duration) -> Self {
        self.recovery_timeout = timeout;
        self
    }

    /// Create a new config with custom half-open max calls.
    pub fn with_half_open_max_calls(mut self, max_calls: usize) -> Self {
        self.half_open_max_calls = max_calls;
        self
    }
}

/// Circuit breaker for fault isolation.
///
/// Implements the circuit breaker pattern to prevent cascading failures
/// by blocking requests to a failing service.
pub struct CircuitBreaker {
    /// Current state.
    state: AtomicU8,
    /// Consecutive failure count.
    failure_count: AtomicU64,
    /// Timestamp of last state change.
    last_state_change: AtomicU64,
    /// Configuration.
    config: CircuitBreakerConfig,
    /// Half-open call counter.
    half_open_calls: AtomicU64,
    /// Half-open success counter.
    half_open_successes: AtomicU64,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with default configuration.
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            state: AtomicU8::new(STATE_CLOSED),
            failure_count: AtomicU64::new(0),
            last_state_change: AtomicU64::new(0),
            config,
            half_open_calls: AtomicU64::new(0),
            half_open_successes: AtomicU64::new(0),
        }
    }

    /// Create a new circuit breaker with default settings.
    pub fn default_breaker() -> Self {
        Self::new(CircuitBreakerConfig::default())
    }

    /// Get the current state.
    pub fn state(&self) -> CircuitBreakerState {
        let current_state = self.state.load(Ordering::Acquire);
        let state = CircuitBreakerState::from_u8(current_state);

        // Check if we should transition from Open to HalfOpen
        if state == CircuitBreakerState::Open {
            let last_change = self.last_state_change.load(Ordering::Acquire);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let elapsed_secs = now.saturating_sub(last_change);
            let elapsed = Duration::from_secs(elapsed_secs);

            if elapsed >= self.config.recovery_timeout {
                // Transition to half-open
                self.transition_to(CircuitBreakerState::HalfOpen);
                return CircuitBreakerState::HalfOpen;
            }
        }

        state
    }

    /// Check if a request is allowed.
    pub fn is_allowed(&self) -> bool {
        match self.state() {
            CircuitBreakerState::Closed => true,
            CircuitBreakerState::Open => false,
            CircuitBreakerState::HalfOpen => {
                let calls = self.half_open_calls.load(Ordering::Acquire);
                calls < self.config.half_open_max_calls as u64
            }
        }
    }

    /// Record a successful request.
    pub fn record_success(&self) {
        let current_state = self.state.load(Ordering::Acquire);
        let state = CircuitBreakerState::from_u8(current_state);

        match state {
            CircuitBreakerState::Closed => {
                // Reset failure count on success
                self.failure_count.store(0, Ordering::Release);
            }
            CircuitBreakerState::HalfOpen => {
                let calls = self.half_open_calls.fetch_add(1, Ordering::AcqRel) + 1;
                let successes = self.half_open_successes.fetch_add(1, Ordering::AcqRel) + 1;

                // Check if we've met the success threshold
                let success_rate = successes as f64 / calls as f64;
                if success_rate >= self.config.success_threshold {
                    self.transition_to(CircuitBreakerState::Closed);
                }
            }
            CircuitBreakerState::Open => {
                // Should not receive successes in open state
            }
        }
    }

    /// Record a failed request.
    pub fn record_failure(&self) {
        let current_state = self.state.load(Ordering::Acquire);
        let state = CircuitBreakerState::from_u8(current_state);

        match state {
            CircuitBreakerState::Closed => {
                let failures = self.failure_count.fetch_add(1, Ordering::AcqRel) + 1;
                if failures >= self.config.failure_threshold as u64 {
                    self.transition_to(CircuitBreakerState::Open);
                }
            }
            CircuitBreakerState::HalfOpen => {
                let calls = self.half_open_calls.fetch_add(1, Ordering::AcqRel) + 1;
                let successes = self.half_open_successes.load(Ordering::Acquire);
                let failures = calls - successes;

                // Any failure in half-open state opens the circuit
                if failures > 0 {
                    self.transition_to(CircuitBreakerState::Open);
                }
            }
            CircuitBreakerState::Open => {
                // Already open, nothing to do
            }
        }
    }

    /// Manually transition to a specific state.
    ///
    /// This is useful for testing or administrative actions.
    pub fn transition_to(&self, new_state: CircuitBreakerState) {
        let old_state = self.state.load(Ordering::Acquire);
        self.state.store(new_state.as_u8(), Ordering::Release);

        // Use a simple counter for state change time (since Instant isn't wall clock time)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.last_state_change.store(now, Ordering::Release);

        // Reset counters based on transition
        match new_state {
            CircuitBreakerState::Closed => {
                self.failure_count.store(0, Ordering::Release);
                self.half_open_calls.store(0, Ordering::Release);
                self.half_open_successes.store(0, Ordering::Release);
            }
            CircuitBreakerState::HalfOpen => {
                self.half_open_calls.store(0, Ordering::Release);
                self.half_open_successes.store(0, Ordering::Release);
            }
            CircuitBreakerState::Open => {
                // Keep failure count for potential analysis
            }
        }

        tracing::debug!(
            from = ?CircuitBreakerState::from_u8(old_state),
            to = ?new_state,
            "circuit breaker state changed"
        );
    }

    /// Reset the circuit breaker to closed state.
    pub fn reset(&self) {
        self.transition_to(CircuitBreakerState::Closed);
    }

    /// Get the failure count.
    pub fn failure_count(&self) -> usize {
        self.failure_count.load(Ordering::Acquire) as usize
    }

    /// Get the time until next half-open attempt.
    pub fn time_until_retry(&self) -> Duration {
        if self.state() != CircuitBreakerState::Open {
            return Duration::ZERO;
        }

        let last_change = self.last_state_change.load(Ordering::Acquire);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let elapsed_secs = now.saturating_sub(last_change);
        let elapsed = Duration::from_secs(elapsed_secs);

        if elapsed >= self.config.recovery_timeout {
            Duration::ZERO
        } else {
            self.config.recovery_timeout - elapsed
        }
    }

    /// Get the configuration.
    pub fn config(&self) -> &CircuitBreakerConfig {
        &self.config
    }
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new(CircuitBreakerConfig::default())
    }
}

impl std::fmt::Debug for CircuitBreaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CircuitBreaker")
            .field("state", &self.state())
            .field("failure_count", &self.failure_count())
            .field("config", &self.config)
            .finish()
    }
}
