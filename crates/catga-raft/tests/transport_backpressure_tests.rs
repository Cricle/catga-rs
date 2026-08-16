//! Integration tests for `catga_raft::transport::backpressure`.
//!
//! Covers the public surface of `BackpressureController`:
//! - construction / defaults / limit clamping (`new`, `with_default_limit`, `Default`)
//! - module constants (`DEFAULT_INFLIGHT_LIMIT`, `MIN_INFLIGHT_LIMIT`, `MAX_INFLIGHT_LIMIT`)
//! - send/complete accounting (`on_send`, `on_complete`, `increment_inflight`,
//!   `decrement_inflight`, `current_inflight`, `total_inflight`)
//! - capacity checks (`can_send`, `can_send_with_count`)
//! - limit queries and adjustment (`limit`, `current_limit`, `adjust_limit`)
//! - peer removal (`remove_peer`), statistics (`stats`), utilization
//!   (`utilization`, `average_utilization`), and the `Debug` impl
//! - thread-safety under concurrent send/complete
//!
//! Note: basic happy-path tests for this type also exist in `module_tests.rs`;
//! the names here are distinct and focus on edge cases not covered there.
//! The module is entirely synchronous, so no `#[tokio::test]` is required.

use catga_raft::transport::backpressure::{
    BackpressureController, DEFAULT_INFLIGHT_LIMIT, MAX_INFLIGHT_LIMIT, MIN_INFLIGHT_LIMIT,
};

// ============================================================================
// Construction / defaults / constants
// ============================================================================

#[test]
fn bp_default_constructor_uses_default_inflight_limit() {
    let controller = BackpressureController::default();
    assert_eq!(controller.current_limit(), DEFAULT_INFLIGHT_LIMIT);
    assert_eq!(controller.total_inflight(), 0);
    assert!(controller.stats().is_empty());
    assert!(controller.can_send(42));
}

#[test]
fn bp_new_clamps_limit_into_valid_range() {
    // Below minimum: clamped up to MIN_INFLIGHT_LIMIT.
    let low = BackpressureController::new(0);
    assert_eq!(low.current_limit(), MIN_INFLIGHT_LIMIT);

    // Above maximum: clamped down to MAX_INFLIGHT_LIMIT.
    let high = BackpressureController::new(usize::MAX);
    assert_eq!(high.current_limit(), MAX_INFLIGHT_LIMIT);

    // In-range value passes through unchanged.
    let mid = BackpressureController::new(64);
    assert_eq!(mid.current_limit(), 64);
}

#[test]
fn bp_with_default_limit_matches_new_semantics() {
    let controller = BackpressureController::with_default_limit(7);
    assert_eq!(controller.current_limit(), 7);
    // New peers inherit the default limit.
    assert_eq!(controller.limit(1), 7);

    // Clamping applies here too since it delegates to `new`.
    let clamped = BackpressureController::with_default_limit(0);
    assert_eq!(clamped.current_limit(), MIN_INFLIGHT_LIMIT);
}

#[test]
fn bp_constants_are_consistent() {
    assert_eq!(DEFAULT_INFLIGHT_LIMIT, 256);
    assert_eq!(MIN_INFLIGHT_LIMIT, 1);
    assert_eq!(MAX_INFLIGHT_LIMIT, 65536);
    assert!(MIN_INFLIGHT_LIMIT <= DEFAULT_INFLIGHT_LIMIT);
    assert!(DEFAULT_INFLIGHT_LIMIT <= MAX_INFLIGHT_LIMIT);
}

#[test]
fn bp_root_reexport_is_the_same_type() {
    // The crate root re-exports the controller; both paths must be usable.
    let controller = BackpressureController::new(5);
    let reexported: &catga_raft::BackpressureController = &controller;
    assert_eq!(reexported.current_limit(), 5);
}

// ============================================================================
// Send / complete accounting
// ============================================================================

#[test]
fn bp_send_and_complete_roundtrip_tracks_counts() {
    let controller = BackpressureController::new(8);

    assert_eq!(controller.on_send(1), 1);
    assert_eq!(controller.on_send(1), 2);
    assert_eq!(controller.current_inflight(1), 2);
    assert_eq!(controller.total_inflight(), 2);

    assert_eq!(controller.on_complete(1), 1);
    // increment_inflight / decrement_inflight are aliases of on_send/on_complete.
    assert_eq!(controller.increment_inflight(1), 2);
    assert_eq!(controller.decrement_inflight(1), 1);
    assert_eq!(controller.on_complete(1), 0);

    assert_eq!(controller.current_inflight(1), 0);
    assert_eq!(controller.total_inflight(), 0);
}

#[test]
fn bp_peers_are_tracked_independently() {
    let controller = BackpressureController::new(2);

    // Saturate peer 1.
    controller.on_send(1);
    controller.on_send(1);
    assert!(!controller.can_send(1));

    // Peer 2 is unaffected.
    assert!(controller.can_send(2));
    assert_eq!(controller.current_inflight(2), 0);
    assert_eq!(controller.on_send(2), 1);
    assert_eq!(controller.current_inflight(1), 2);
    assert_eq!(controller.total_inflight(), 3);

    // Completing peer 2 does not touch peer 1.
    assert_eq!(controller.on_complete(2), 0);
    assert_eq!(controller.current_inflight(1), 2);
    assert_eq!(controller.total_inflight(), 2);
}

#[test]
fn bp_can_send_with_count_uses_provided_value() {
    let controller = BackpressureController::new(5);

    // Semantics: `provided < limit`, independent of the internal counter.
    assert!(controller.can_send_with_count(1, 0));
    assert!(controller.can_send_with_count(1, 4));
    assert!(!controller.can_send_with_count(1, 5));
    assert!(!controller.can_send_with_count(1, 6));

    // Even with in-flight messages recorded, the provided count decides.
    controller.on_send(1);
    controller.on_send(1);
    assert!(controller.can_send_with_count(1, 4));
    assert!(!controller.can_send_with_count(1, 5));
}

#[test]
fn bp_complete_unknown_peer_is_noop() {
    let controller = BackpressureController::new(4);
    controller.on_send(1);

    // Completing a never-registered peer returns 0 and leaves totals intact.
    assert_eq!(controller.on_complete(99), 0);
    assert_eq!(controller.total_inflight(), 1);
    assert_eq!(controller.current_inflight(1), 1);
    // Unknown peer is not created by on_complete.
    assert_eq!(controller.stats().len(), 1);
}

#[test]
fn bp_queries_for_unknown_peer_fall_back_safely() {
    let controller = BackpressureController::new(9);

    assert_eq!(controller.current_inflight(7), 0);
    assert_eq!(controller.limit(7), 9); // falls back to the default limit
    assert_eq!(controller.utilization(7), 0.0);
    assert_eq!(controller.average_utilization(), 0.0);
    // None of these reads should register the peer.
    assert!(controller.stats().is_empty());
}

// ============================================================================
// Limit adjustment
// ============================================================================

#[test]
fn bp_adjust_limit_clamps_and_affects_only_target_peer() {
    let controller = BackpressureController::new(10);
    // Materialize both peers.
    controller.on_send(1);
    controller.on_send(2);

    // Clamp below minimum and above maximum.
    controller.adjust_limit(1, 0);
    assert_eq!(controller.limit(1), MIN_INFLIGHT_LIMIT);
    controller.adjust_limit(1, usize::MAX);
    assert_eq!(controller.limit(1), MAX_INFLIGHT_LIMIT);

    // Normal adjustment gates can_send.
    controller.adjust_limit(1, 1);
    assert!(!controller.can_send(1)); // count=1, limit=1
    controller.adjust_limit(1, 100);
    assert!(controller.can_send(1));

    // Other peer keeps the default limit.
    assert_eq!(controller.limit(2), 10);
}

#[test]
fn bp_adjust_limit_on_new_peer_creates_it_first() {
    let controller = BackpressureController::new(10);
    controller.adjust_limit(5, 3);
    // Peer now exists with the adjusted limit and zero in-flight.
    assert_eq!(controller.limit(5), 3);
    assert_eq!(controller.current_inflight(5), 0);
    assert_eq!(controller.stats()[&5], (0, 3));
}

// ============================================================================
// Removal / stats / utilization / debug
// ============================================================================

#[test]
fn bp_remove_peer_clears_state_and_total() {
    let controller = BackpressureController::new(10);
    controller.on_send(1);
    controller.on_send(1);
    controller.on_send(2);
    assert_eq!(controller.total_inflight(), 3);

    controller.remove_peer(1);
    assert_eq!(controller.total_inflight(), 1);
    assert_eq!(controller.current_inflight(1), 0);
    assert!(!controller.stats().contains_key(&1));

    // Removing a non-existent peer is a no-op.
    controller.remove_peer(42);
    assert_eq!(controller.total_inflight(), 1);

    // A removed peer can be re-registered with the default limit.
    assert_eq!(controller.on_send(1), 1);
    assert_eq!(controller.limit(1), 10);
    assert_eq!(controller.total_inflight(), 2);
}

#[test]
fn bp_utilization_and_average_utilization() {
    let controller = BackpressureController::new(4);

    controller.on_send(1); // 1/4 = 0.25
    controller.on_send(2);
    controller.on_send(2);
    controller.on_send(2); // 3/4 = 0.75

    assert!((controller.utilization(1) - 0.25).abs() < 1e-9);
    assert!((controller.utilization(2) - 0.75).abs() < 1e-9);
    assert!((controller.average_utilization() - 0.5).abs() < 1e-9);

    // Stats reflect (count, limit) per peer.
    let stats = controller.stats();
    assert_eq!(stats.len(), 2);
    assert_eq!(stats[&1], (1, 4));
    assert_eq!(stats[&2], (3, 4));
}

#[test]
fn bp_debug_impl_includes_key_fields() {
    let controller = BackpressureController::new(16);
    controller.on_send(3);
    let debug = format!("{controller:?}");
    assert!(debug.contains("BackpressureController"));
    assert!(debug.contains("default_limit"));
    assert!(debug.contains("peer_count"));
}

// ============================================================================
// Thread safety
// ============================================================================

#[test]
fn bp_concurrent_send_complete_stays_consistent() {
    use std::sync::Arc;

    let controller = Arc::new(BackpressureController::new(MAX_INFLIGHT_LIMIT));
    let threads = 4usize;
    let iterations = 250usize;

    let handles: Vec<_> = (0..threads)
        .map(|_| {
            let c = Arc::clone(&controller);
            std::thread::spawn(move || {
                for _ in 0..iterations {
                    c.on_send(1);
                    c.on_complete(1);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("worker thread panicked");
    }

    // Balanced increments/decrements must leave everything at zero.
    assert_eq!(controller.current_inflight(1), 0);
    assert_eq!(controller.total_inflight(), 0);
    assert!(controller.can_send(1));
}
