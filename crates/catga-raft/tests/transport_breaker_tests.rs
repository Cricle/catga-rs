//! Integration tests for the circuit breaker (`transport::breaker`).
//!
//! Covers `CircuitBreaker`, `CircuitBreakerConfig`, and `CircuitBreakerState`
//! through the crate's public API: construction/defaults, config builders,
//! state transitions (Closed -> Open -> HalfOpen -> Closed), request
//! admission in each state, `time_until_retry`, counter resets, and
//! concurrent recording.
//!
//! Note: `CircuitBreakerConfig::minimum_requests` is part of the public
//! config surface but is not consulted by the current breaker logic, so it
//! is only asserted on its default value.

use std::time::Duration;

use bytes::Bytes;
use catga_raft::transport::breaker::{
    DEFAULT_FAILURE_THRESHOLD, DEFAULT_HALF_OPEN_MAX_CALLS, DEFAULT_RECOVERY_TIMEOUT_SECS,
};
use catga_raft::transport::grpc::PeerClient;
use catga_raft::{CatgaRaftError, CircuitBreaker, CircuitBreakerConfig, CircuitBreakerState};

// ============================================================================
// Construction / defaults
// ============================================================================

#[test]
fn breaker_default_config_matches_constants() {
    let config = CircuitBreakerConfig::default();
    assert_eq!(config.failure_threshold, DEFAULT_FAILURE_THRESHOLD);
    assert_eq!(config.failure_threshold, 5);
    assert_eq!(
        config.recovery_timeout,
        Duration::from_secs(DEFAULT_RECOVERY_TIMEOUT_SECS)
    );
    // 2s: an open breaker drops every message to the peer, and a longer
    // blackout costs followers their elections (partition amplification).
    assert_eq!(config.recovery_timeout, Duration::from_secs(2));
    assert_eq!(config.half_open_max_calls, DEFAULT_HALF_OPEN_MAX_CALLS);
    assert_eq!(config.half_open_max_calls, 3);
    assert_eq!(config.minimum_requests, 1);
    assert_eq!(config.success_threshold, 0.5);
}

#[test]
fn breaker_config_builder_chain_overrides_fields() {
    let config = CircuitBreakerConfig::default()
        .with_failure_threshold(7)
        .with_recovery_timeout(Duration::from_millis(250))
        .with_half_open_max_calls(9);

    assert_eq!(config.failure_threshold, 7);
    assert_eq!(config.recovery_timeout, Duration::from_millis(250));
    assert_eq!(config.half_open_max_calls, 9);
    // Untouched fields keep their defaults.
    assert_eq!(config.minimum_requests, 1);
    assert_eq!(config.success_threshold, 0.5);
}

#[test]
fn breaker_constructors_start_closed_and_idle() {
    let a = CircuitBreaker::default();
    let b = CircuitBreaker::default_breaker();
    let c = CircuitBreaker::new(CircuitBreakerConfig::default());

    for breaker in [&a, &b, &c] {
        assert_eq!(breaker.state(), CircuitBreakerState::Closed);
        assert!(breaker.is_allowed());
        assert_eq!(breaker.failure_count(), 0);
        assert_eq!(breaker.time_until_retry(), Duration::ZERO);
    }
}

#[test]
fn breaker_config_accessor_reflects_construction() {
    let config = CircuitBreakerConfig::default()
        .with_failure_threshold(42)
        .with_half_open_max_calls(11);
    let breaker = CircuitBreaker::new(config);

    assert_eq!(breaker.config().failure_threshold, 42);
    assert_eq!(breaker.config().half_open_max_calls, 11);
}

// ============================================================================
// State transitions and admission control
// ============================================================================

#[test]
fn breaker_zero_recovery_timeout_promotes_open_to_half_open() {
    let config = CircuitBreakerConfig::default()
        .with_failure_threshold(1)
        .with_recovery_timeout(Duration::ZERO);
    let breaker = CircuitBreaker::new(config);

    breaker.record_failure();
    // With a zero recovery timeout the Open state is immediately promoted to
    // HalfOpen the next time the state is inspected.
    assert_eq!(breaker.state(), CircuitBreakerState::HalfOpen);
    assert!(breaker.is_allowed());
}

#[test]
fn breaker_half_open_allows_up_to_max_calls() {
    let config = CircuitBreakerConfig {
        half_open_max_calls: 2,
        // Unreachable rate so the breaker stays in HalfOpen on successes.
        success_threshold: 1.5,
        ..Default::default()
    };
    let breaker = CircuitBreaker::new(config);
    breaker.transition_to(CircuitBreakerState::HalfOpen);

    assert!(breaker.is_allowed());
    breaker.record_success(); // calls = 1
    assert!(breaker.is_allowed());
    breaker.record_success(); // calls = 2
    assert!(!breaker.is_allowed());
    assert_eq!(breaker.state(), CircuitBreakerState::HalfOpen);
}

#[test]
fn breaker_half_open_zero_max_calls_blocks_immediately() {
    let config = CircuitBreakerConfig {
        half_open_max_calls: 0,
        ..Default::default()
    };
    let breaker = CircuitBreaker::new(config);
    breaker.transition_to(CircuitBreakerState::HalfOpen);
    assert!(!breaker.is_allowed());
}

#[test]
fn breaker_half_open_failure_reopens_circuit() {
    let config = CircuitBreakerConfig::default().with_recovery_timeout(Duration::from_secs(60));
    let breaker = CircuitBreaker::new(config);
    breaker.transition_to(CircuitBreakerState::HalfOpen);
    assert!(breaker.is_allowed());

    // Any failure while half-open reopens the circuit.
    breaker.record_failure();
    assert_eq!(breaker.state(), CircuitBreakerState::Open);
    assert!(!breaker.is_allowed());
}

#[test]
fn breaker_half_open_success_closes_circuit_and_resets_failures() {
    let breaker = CircuitBreaker::new(CircuitBreakerConfig::default());
    breaker.record_failure();
    assert_eq!(breaker.failure_count(), 1);

    breaker.transition_to(CircuitBreakerState::HalfOpen);
    // Half-open resets the half-open counters but keeps the failure count.
    assert_eq!(breaker.failure_count(), 1);

    // One success gives a 1.0 success rate >= the 0.5 threshold.
    breaker.record_success();
    assert_eq!(breaker.state(), CircuitBreakerState::Closed);
    assert_eq!(breaker.failure_count(), 0);
    assert!(breaker.is_allowed());
}

#[test]
fn breaker_open_blocks_requests_and_ignores_recordings() {
    let config = CircuitBreakerConfig::default()
        .with_failure_threshold(2)
        .with_recovery_timeout(Duration::from_secs(60));
    let breaker = CircuitBreaker::new(config);

    breaker.record_failure();
    assert!(breaker.is_allowed());
    breaker.record_failure();
    assert_eq!(breaker.state(), CircuitBreakerState::Open);
    assert!(!breaker.is_allowed());

    // The failure count survives the transition to Open, and recordings in
    // the Open state are ignored.
    let count = breaker.failure_count();
    assert_eq!(count, 2);
    breaker.record_failure();
    breaker.record_success();
    assert_eq!(breaker.state(), CircuitBreakerState::Open);
    assert_eq!(breaker.failure_count(), count);
}

#[test]
fn breaker_transition_to_resets_counters_per_state() {
    let config = CircuitBreakerConfig::default().with_failure_threshold(100);
    let breaker = CircuitBreaker::new(config);
    breaker.record_failure();
    breaker.record_failure();
    assert_eq!(breaker.failure_count(), 2);

    // Open keeps the failure count for analysis.
    breaker.transition_to(CircuitBreakerState::Open);
    assert_eq!(breaker.failure_count(), 2);

    // HalfOpen keeps the failure count but re-enables trial calls.
    breaker.transition_to(CircuitBreakerState::HalfOpen);
    assert_eq!(breaker.failure_count(), 2);
    assert!(breaker.is_allowed());

    // Closed resets everything.
    breaker.transition_to(CircuitBreakerState::Closed);
    assert_eq!(breaker.failure_count(), 0);
    assert_eq!(breaker.state(), CircuitBreakerState::Closed);
    assert!(breaker.is_allowed());
}

// ============================================================================
// Retry timing
// ============================================================================

#[test]
fn breaker_time_until_retry_per_state() {
    // Closed breaker: nothing to wait for.
    let breaker = CircuitBreaker::default_breaker();
    assert_eq!(breaker.time_until_retry(), Duration::ZERO);

    // Freshly opened breaker: wait is positive and bounded by the timeout.
    let config = CircuitBreakerConfig::default()
        .with_failure_threshold(1)
        .with_recovery_timeout(Duration::from_secs(30));
    let breaker = CircuitBreaker::new(config);
    breaker.record_failure();
    assert_eq!(breaker.state(), CircuitBreakerState::Open);
    let wait = breaker.time_until_retry();
    assert!(wait > Duration::ZERO);
    assert!(wait <= Duration::from_secs(30));

    // Once the recovery timeout has elapsed (zero timeout here), the breaker
    // promotes itself to HalfOpen and reports no remaining wait.
    let config = CircuitBreakerConfig::default()
        .with_failure_threshold(1)
        .with_recovery_timeout(Duration::ZERO);
    let breaker = CircuitBreaker::new(config);
    breaker.record_failure();
    assert_eq!(breaker.time_until_retry(), Duration::ZERO);
}

// ============================================================================
// Misc: Debug impl, state enum, concurrency
// ============================================================================

#[test]
fn breaker_debug_output_and_state_enum_traits() {
    let breaker = CircuitBreaker::default();
    let dbg = format!("{:?}", breaker);
    assert!(dbg.contains("CircuitBreaker"));
    assert!(dbg.contains("Closed"));

    // CircuitBreakerState is Copy + Eq.
    let s = CircuitBreakerState::Open;
    let t = s;
    assert_eq!(s, t);
    assert_ne!(CircuitBreakerState::Closed, CircuitBreakerState::HalfOpen);
}

#[test]
fn breaker_concurrent_recordings_stay_consistent() {
    let config = CircuitBreakerConfig::default()
        .with_failure_threshold(50)
        .with_recovery_timeout(Duration::from_secs(60));
    let breaker = std::sync::Arc::new(CircuitBreaker::new(config));

    std::thread::scope(|scope| {
        for _ in 0..4 {
            let failures = std::sync::Arc::clone(&breaker);
            scope.spawn(move || {
                for _ in 0..100 {
                    failures.record_failure();
                }
            });
            let successes = std::sync::Arc::clone(&breaker);
            scope.spawn(move || {
                for _ in 0..100 {
                    successes.record_success();
                }
            });
        }
    });

    // Starting from Closed with no manual transitions, the breaker can only
    // end up Closed or Open; admission must be consistent with the state.
    match breaker.state() {
        CircuitBreakerState::Closed => assert!(breaker.is_allowed()),
        CircuitBreakerState::Open => assert!(!breaker.is_allowed()),
        CircuitBreakerState::HalfOpen => {
            // Not reachable in this scenario; tolerated for robustness.
        }
    }
}

// ============================================================================
// PeerClient integration: which errors feed the breaker
// ============================================================================

/// Backpressure is local load-shedding, not peer health: repeated
/// `Backpressure` rejections must not count as breaker failures. Counting
/// them would let our own throttling open the circuit and silently drop
/// every message to the peer, amplifying a transient overload into a
/// partition.
#[tokio::test]
async fn peer_backpressure_does_not_trip_breaker() {
    // In-flight limit 1 with 32 concurrent sends forces load-shedding: on
    // the single-threaded test runtime every send increments the in-flight
    // count before any connection attempt can complete, so all but the
    // first are rejected with `Backpressure` before touching the network.
    let peer = PeerClient::with_config(
        7,
        "http://127.0.0.1:1".to_string(),
        1,
        128,
        1,
        8,
        Duration::from_millis(1),
        // Default threshold (5): the one real connection failure below
        // cannot open the circuit on its own.
        CircuitBreakerConfig::default(),
    );

    let sends: Vec<_> = (0..32)
        .map(|_| peer.send(Bytes::from_static(b"x")))
        .collect();
    let results = futures::future::join_all(sends).await;

    let mut backpressure_errors = 0;
    let mut transport_errors = 0;
    for result in results {
        match result {
            Err(CatgaRaftError::Backpressure) => backpressure_errors += 1,
            Err(_) => transport_errors += 1,
            Ok(()) => {}
        }
    }
    assert!(
        backpressure_errors > 0,
        "expected load-shedding rejections (transport errors: {transport_errors})"
    );

    // The Backpressure storm must not have opened the breaker: it is local
    // load-shedding, not peer health.
    assert_eq!(
        peer.circuit_breaker_state(),
        CircuitBreakerState::Closed,
        "backpressure must not open the breaker"
    );

    // Still admitted afterwards: the next send reaches the transport layer
    // (and fails to connect) instead of being blocked by an open circuit.
    let err = peer.send(Bytes::from_static(b"x")).await.unwrap_err();
    assert!(
        matches!(err, CatgaRaftError::Transport(_)),
        "breaker must still admit sends after backpressure, got {err:?}"
    );
}

/// Genuine transport failures still feed the breaker: once
/// `DEFAULT_FAILURE_THRESHOLD` sends to an unreachable endpoint have failed,
/// the circuit opens and later sends are rejected with `CircuitBreakerOpen`
/// without touching the network.
#[tokio::test]
async fn peer_transport_failures_still_trip_breaker() {
    // Port 1 is not served, so every eager connection attempt fails fast.
    let peer = PeerClient::new(8, "http://127.0.0.1:1".to_string());

    for _ in 0..DEFAULT_FAILURE_THRESHOLD {
        let err = peer.send(Bytes::from_static(b"x")).await.unwrap_err();
        assert!(matches!(err, CatgaRaftError::Transport(_)), "got {err:?}");
    }

    assert_eq!(peer.circuit_breaker_state(), CircuitBreakerState::Open);

    let err = peer.send(Bytes::from_static(b"x")).await.unwrap_err();
    assert!(
        matches!(err, CatgaRaftError::CircuitBreakerOpen),
        "got {err:?}"
    );
}
