//! Unit tests for WaitResult and WaitCondition.

use catga_core::flow::suspension::WaitCondition;
use catga_core::flow::suspension::WaitPolicy;
use catga_core::{CatgaError, ErrorCode};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[test]
fn wait_condition_new_creates_empty_results() {
    let wait = WaitCondition::new("corr-1", WaitPolicy::All, 3, UNIX_EPOCH, Duration::from_secs(30));
    assert_eq!(wait.correlation_id(), "corr-1");
    assert_eq!(wait.policy(), WaitPolicy::All);
    assert_eq!(wait.expected_count(), 3);
    assert!(wait.results().is_empty());
    assert_eq!(wait.completed_count(), 0);
}

#[test]
fn wait_condition_record_success_adds_result() {
    let wait = WaitCondition::new("corr", WaitPolicy::All, 2, UNIX_EPOCH, Duration::from_secs(60));
    let wait = wait.record_success("child-1", [1_u8, 2]);

    assert_eq!(wait.completed_count(), 1);
    let results = wait.results();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].child_id(), "child-1");
    assert!(results[0].is_success());
    assert_eq!(results[0].payload(), Some(&[1, 2][..]));
}

#[test]
fn wait_condition_record_failure_adds_result() {
    let error = CatgaError::new(ErrorCode::Unavailable, "timeout");
    let wait = WaitCondition::new("corr", WaitPolicy::Any, 2, UNIX_EPOCH, Duration::from_secs(60));
    let wait = wait.record_failure("child-2", error);

    assert_eq!(wait.completed_count(), 1);
    let results = wait.results();
    assert_eq!(results[0].child_id(), "child-2");
    assert!(!results[0].is_success());
    assert!(results[0].payload().is_none());
    let err = results[0].error().expect("should have error");
    assert_eq!(err.code(), ErrorCode::Unavailable);
}

#[test]
fn wait_condition_is_expired_at() {
    let past = SystemTime::UNIX_EPOCH;
    let now = past + Duration::from_secs(61);
    let wait = WaitCondition::new("corr", WaitPolicy::All, 1, past, Duration::from_secs(60));

    assert!(wait.is_expired_at(now));
    assert!(!wait.is_expired_at(past));
}

#[test]
fn wait_condition_for_children_validates_identities() {
    let result = WaitCondition::for_children(
        "parent",
        WaitPolicy::All,
        ["child-a", "child-b"],
        UNIX_EPOCH,
        Duration::from_secs(30),
    );
    assert!(result.is_ok());

    // Empty child id should fail
    let empty_result = WaitCondition::for_children(
        "parent",
        WaitPolicy::All,
        ["", "child-b"],
        UNIX_EPOCH,
        Duration::from_secs(30),
    );
    assert!(empty_result.is_err());

    // Duplicate child id should fail
    let dup_result = WaitCondition::for_children(
        "parent",
        WaitPolicy::All,
        ["child", "child"],
        UNIX_EPOCH,
        Duration::from_secs(30),
    );
    assert!(dup_result.is_err());
}

#[test]
fn wait_condition_for_children_requires_at_least_one_child() {
    let result = WaitCondition::for_children(
        "parent",
        WaitPolicy::All,
        [] as [&str; 0],
        UNIX_EPOCH,
        Duration::from_secs(30),
    );
    assert!(result.is_err());
}

#[test]
fn wait_condition_accepts_child_for_generic_wait() {
    let wait = WaitCondition::new("corr", WaitPolicy::All, 2, UNIX_EPOCH, Duration::from_secs(60));
    assert!(wait.accepts_child("any-id"));
}

#[test]
fn wait_condition_accepts_child_for_durable_wait() {
    let wait = WaitCondition::for_children(
        "parent",
        WaitPolicy::All,
        ["child-a", "child-b"],
        UNIX_EPOCH,
        Duration::from_secs(30),
    ).expect("valid");

    assert!(wait.accepts_child("child-a"));
    assert!(wait.accepts_child("child-b"));
    assert!(!wait.accepts_child("unknown"));
}

#[test]
fn wait_condition_accepts_payload_len() {
    let wait = WaitCondition::new("corr", WaitPolicy::All, 1, UNIX_EPOCH, Duration::from_secs(60));

    assert!(wait.accepts_payload_len(1024));
    assert!(wait.accepts_payload_len(64 * 1024));
    assert!(!wait.accepts_payload_len(64 * 1024 + 1));
}

#[test]
fn wait_condition_validate_rejects_invalid_correlation() {
    let wait = WaitCondition::new("", WaitPolicy::All, 1, UNIX_EPOCH, Duration::from_secs(60));
    assert!(wait.validate().is_err());
}

#[test]
fn wait_condition_validate_rejects_zero_expected_count() {
    let wait = WaitCondition::new("corr", WaitPolicy::All, 0, UNIX_EPOCH, Duration::from_secs(60));
    assert!(wait.validate().is_err());
}

#[test]
fn wait_condition_clone_preserves_results() {
    let wait = WaitCondition::new("corr", WaitPolicy::All, 2, UNIX_EPOCH, Duration::from_secs(60));
    let wait = wait.record_success("child", [1_u8]);
    let cloned = wait.clone();

    assert_eq!(cloned.results().len(), 1);
    assert_eq!(cloned.results()[0].child_id(), "child");
}

#[test]
fn wait_condition_shared_payload_returns_arc_clone() {
    let wait = WaitCondition::new("corr", WaitPolicy::All, 1, UNIX_EPOCH, Duration::from_secs(60));
    let wait = wait.record_success("child", [1_u8, 2]);

    let shared1 = wait.results()[0].shared_payload();
    let shared2 = wait.results()[0].shared_payload();

    assert!(Arc::ptr_eq(shared1.as_ref().unwrap(), shared2.as_ref().unwrap()));
}
