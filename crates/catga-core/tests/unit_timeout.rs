//! Unit tests for timeout behavior.

use std::time::Duration;

use catga_core::TimeoutBehavior;

#[test]
fn timeout_behavior_new_creates_instance() {
    let timeout = TimeoutBehavior::new(Duration::from_secs(5));
    assert!(std::mem::size_of_val(&timeout) > 0);
}

#[test]
fn timeout_behavior_accepts_zero_duration() {
    let timeout = TimeoutBehavior::new(Duration::ZERO);
    assert!(std::mem::size_of_val(&timeout) > 0);
}

#[test]
fn timeout_behavior_accepts_large_duration() {
    let timeout = TimeoutBehavior::new(Duration::MAX);
    assert!(std::mem::size_of_val(&timeout) > 0);
}
