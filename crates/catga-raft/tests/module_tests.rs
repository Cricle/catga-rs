//! Unified module tests for catga-raft.
//!
//! This file contains basic unit tests extracted from inline test modules
//! that were previously located in various source files.

use catga_raft::transport::{
    backpressure::BackpressureController,
    breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitBreakerState},
    codec::{BincodeCodec, JsonCodec, RaftCodec},
    connection::ConnectionPool,
};

// ==================== Transport Module Tests ====================

#[test]
fn test_initial_state() {
    let breaker = CircuitBreaker::default_breaker();
    assert_eq!(breaker.state(), CircuitBreakerState::Closed);
    assert!(breaker.is_allowed());
}

#[test]
fn test_failure_opens_circuit() {
    let config = CircuitBreakerConfig {
        failure_threshold: 3,
        ..Default::default()
    };
    let breaker = CircuitBreaker::new(config);

    assert!(breaker.is_allowed());

    breaker.record_failure();
    assert!(breaker.is_allowed());

    breaker.record_failure();
    assert!(breaker.is_allowed());

    breaker.record_failure();
    assert!(!breaker.is_allowed());
    assert_eq!(breaker.state(), CircuitBreakerState::Open);
}

#[test]
fn test_success_resets_failure_count() {
    let config = CircuitBreakerConfig {
        failure_threshold: 3,
        ..Default::default()
    };
    let breaker = CircuitBreaker::new(config);

    breaker.record_failure();
    breaker.record_failure();
    assert_eq!(breaker.failure_count(), 2);

    breaker.record_success();
    assert_eq!(breaker.failure_count(), 0);

    // Need 3 more failures to open
    breaker.record_failure();
    breaker.record_failure();
    breaker.record_failure();
    assert!(!breaker.is_allowed());
}

#[test]
fn test_reset() {
    let breaker = CircuitBreaker::default_breaker();

    // Open the circuit
    for _ in 0..5 {
        breaker.record_failure();
    }
    assert_eq!(breaker.state(), CircuitBreakerState::Open);

    // Reset
    breaker.reset();
    assert_eq!(breaker.state(), CircuitBreakerState::Closed);
    assert!(breaker.is_allowed());
}

#[test]
fn test_backpressure_creation() {
    let controller = BackpressureController::default();
    assert_eq!(controller.current_limit(), 256);
    assert_eq!(controller.total_inflight(), 0);
}

#[test]
fn test_send_and_complete() {
    let controller = BackpressureController::new(10);

    // Initial state
    assert!(controller.can_send(1));
    assert_eq!(controller.current_inflight(1), 0);

    // Send message
    let count = controller.on_send(1);
    assert_eq!(count, 1);
    assert_eq!(controller.total_inflight(), 1);

    // Still can send
    assert!(controller.can_send(1));

    // Complete message
    let count = controller.on_complete(1);
    assert_eq!(count, 0);
    assert_eq!(controller.total_inflight(), 0);
}

#[test]
fn test_backpressure_blocking() {
    let controller = BackpressureController::new(3);

    // Fill up the peer
    controller.on_send(1);
    controller.on_send(1);
    controller.on_send(1);

    // Should not be able to send more
    assert!(!controller.can_send(1));

    // Complete one
    controller.on_complete(1);

    // Should be able to send again
    assert!(controller.can_send(1));
}

#[test]
fn test_adjust_limit() {
    let controller = BackpressureController::new(2);

    // Fill up
    controller.on_send(1);
    controller.on_send(1);
    assert!(!controller.can_send(1));

    // Increase limit
    controller.adjust_limit(1, 5);
    assert!(controller.can_send(1));

    // Decrease limit
    controller.adjust_limit(1, 1);
    assert!(!controller.can_send(1));
}

#[test]
fn test_remove_peer() {
    let controller = BackpressureController::new(10);

    controller.on_send(1);
    controller.on_send(1);
    assert_eq!(controller.total_inflight(), 2);

    controller.remove_peer(1);
    assert_eq!(controller.total_inflight(), 0);
}

#[test]
fn test_stats() {
    let controller = BackpressureController::new(10);

    controller.on_send(1);
    controller.on_send(1);
    controller.on_send(2);

    let stats = controller.stats();
    assert_eq!(stats.len(), 2);
    assert_eq!(stats[&1], (2, 10));
    assert_eq!(stats[&2], (1, 10));
}

#[test]
fn test_utilization() {
    let controller = BackpressureController::new(10);

    controller.on_send(1);
    controller.on_send(1);
    controller.on_send(1);

    assert!((controller.utilization(1) - 0.3).abs() < 0.01);
}

#[test]
fn test_bincode_codec() {
    let codec = BincodeCodec;

    let data: Vec<u8> = vec![1, 2, 3, 4, 5];
    let encoded = codec.encode(&data).unwrap();
    let decoded: Vec<u8> = codec.decode(&encoded).unwrap();

    assert_eq!(data, decoded);
}

#[test]
fn test_bincode_codec_struct() {
    let codec = BincodeCodec;

    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct TestMessage {
        id: u64,
        data: Vec<u8>,
        name: String,
    }

    let msg = TestMessage {
        id: 42,
        data: vec![10, 20, 30],
        name: "test".to_string(),
    };

    let encoded = codec.encode(&msg).unwrap();
    let decoded: TestMessage = codec.decode(&encoded).unwrap();

    assert_eq!(msg, decoded);
}

#[test]
fn test_json_codec() {
    let codec = JsonCodec;

    let data: Vec<u8> = vec![1, 2, 3, 4, 5];
    let encoded = codec.encode(&data).unwrap();
    let decoded: Vec<u8> = codec.decode(&encoded).unwrap();

    assert_eq!(data, decoded);
}

#[test]
fn test_pool_creation() {
    let pool = ConnectionPool::new("http://127.0.0.1:8080".to_string(), 4);
    assert_eq!(pool.endpoint(), "http://127.0.0.1:8080");
    assert_eq!(pool.pool_size(), 4);
    assert_eq!(pool.connection_count(), 0);
    assert!(pool.is_empty());
    assert!(!pool.is_full());
}

#[test]
fn test_pool_size_clamping() {
    // Test upper bound
    let pool = ConnectionPool::new("http://127.0.0.1:8080".to_string(), 100);
    assert_eq!(pool.pool_size(), 16);

    // Test lower bound
    let pool = ConnectionPool::new("http://127.0.0.1:8080".to_string(), 0);
    assert_eq!(pool.pool_size(), 1);
}

#[test]
fn test_clear() {
    let pool = ConnectionPool::new("http://127.0.0.1:8080".to_string(), 4);
    assert!(pool.is_empty());

    pool.clear();
    assert!(pool.is_empty());
}
