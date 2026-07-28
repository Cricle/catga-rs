//! Public durable wait validation and bounded-result contracts.

use std::time::{Duration, UNIX_EPOCH};

use catga_core::{CatgaError, ErrorCode};
use catga_flow::{MAX_WAIT_RESULT_BYTES, WaitCondition, WaitPolicy};

#[test]
fn child_waits_accept_only_persisted_children_and_deduplicate_results() {
    let wait = WaitCondition::for_children(
        "parent/42",
        WaitPolicy::All,
        ["charge", "reserve"],
        UNIX_EPOCH,
        Duration::from_secs(30),
    )
    .expect("distinct child identities are valid");
    assert!(wait.accepts_child("charge"));
    assert!(!wait.accepts_child("unknown"));

    let unknown = wait.record_success("unknown", [1_u8]);
    assert_eq!(unknown.completed_count(), 0);
    let completed = wait.record_success("charge", [2_u8]);
    assert_eq!(completed.completed_count(), 1);
    assert_eq!(completed.results()[0].payload(), Some(&[2_u8][..]));
    let duplicate = completed.record_failure(
        "charge",
        CatgaError::new(ErrorCode::Transient, "duplicate result must be ignored"),
    );
    assert_eq!(duplicate.completed_count(), 1);
    assert!(duplicate.results()[0].is_success());
    assert!(completed.validate().is_ok());
}

#[test]
fn waits_reject_invalid_child_sets_and_oversized_results_without_retaining_them() {
    let duplicate = WaitCondition::for_children(
        "parent/duplicate",
        WaitPolicy::Any,
        ["child", "child"],
        UNIX_EPOCH,
        Duration::from_secs(1),
    )
    .expect_err("child launch identities must be unique");
    assert_eq!(duplicate.code(), ErrorCode::Validation);
    let empty = WaitCondition::for_children(
        "parent/empty",
        WaitPolicy::Any,
        std::iter::empty::<&str>(),
        UNIX_EPOCH,
        Duration::from_secs(1),
    )
    .expect_err("a child wait needs at least one child");
    assert_eq!(empty.code(), ErrorCode::Validation);

    let external = WaitCondition::new(
        "external",
        WaitPolicy::Any,
        1,
        UNIX_EPOCH,
        Duration::from_secs(1),
    );
    assert!(external.accepts_child("any-external-child"));
    assert!(external.accepts_payload_len(MAX_WAIT_RESULT_BYTES));
    assert!(!external.accepts_payload_len(MAX_WAIT_RESULT_BYTES.saturating_add(1)));
    let too_large =
        external.record_success("any-external-child", vec![0; MAX_WAIT_RESULT_BYTES + 1]);
    assert_eq!(too_large.completed_count(), 0);
    assert!(!external.is_expired_at(UNIX_EPOCH));
    assert!(external.is_expired_at(UNIX_EPOCH + Duration::from_secs(1)));
}
