//! Public durable wait validation and bounded-result contracts.

use std::time::{Duration, UNIX_EPOCH};

use catga_core::{CatgaError, ErrorCode};
use catga_flow::{
    FlowContinuation, FlowState, FlowStatus, MAX_WAIT_RESULT_BYTES, WaitCondition, WaitPolicy,
};

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

#[test]
fn continuation_trigger_transitions_keep_only_the_current_wait_or_delay_metadata() {
    let wait = WaitCondition::new(
        "external",
        WaitPolicy::Any,
        1,
        UNIX_EPOCH,
        Duration::from_secs(30),
    );
    let initial = FlowState::new("flow-1", "checkout", [], "worker").suspended();
    let delayed = FlowContinuation::waiting(initial, "await-result", wait.clone())
        .delayed_until(UNIX_EPOCH + Duration::from_secs(60));

    assert!(delayed.wait().is_none());
    assert_eq!(
        delayed.resume_at(),
        Some(UNIX_EPOCH + Duration::from_secs(60))
    );
    let ready = delayed.at_step("complete");
    assert_eq!(ready.step_name(), "complete");
    assert!(ready.wait().is_none());
    assert!(ready.resume_at().is_none());
    assert_eq!(ready.state().status(), FlowStatus::Suspended);

    let waiting = ready.with_wait(wait);
    assert!(waiting.wait().is_some());
    assert!(waiting.resume_at().is_none());
}

#[test]
fn external_waits_bound_distinct_results_and_preserve_failure_diagnostics() {
    let wait = WaitCondition::new(
        "external/42",
        WaitPolicy::All,
        2,
        UNIX_EPOCH,
        Duration::from_secs(30),
    )
    .record_success("provider-a", [7_u8, 8])
    .record_failure(
        "provider-b",
        CatgaError::new(ErrorCode::Unavailable, "provider-b did not respond"),
    )
    .record_success("provider-c", [9_u8]);

    assert_eq!(wait.completed_count(), 2);
    assert_eq!(wait.results().len(), 2);
    assert_eq!(wait.results()[0].child_id(), "provider-a");
    assert_eq!(wait.results()[0].payload(), Some(&[7_u8, 8][..]));
    assert_eq!(
        wait.results()[0]
            .shared_payload()
            .expect("successful results retain their bounded payload")
            .as_ref(),
        &[7_u8, 8]
    );
    assert_eq!(wait.results()[1].child_id(), "provider-b");
    assert_eq!(
        wait.results()[1].error().map(CatgaError::code),
        Some(ErrorCode::Unavailable)
    );
    assert!(wait.validate().is_ok());

    let empty_correlation =
        WaitCondition::new("", WaitPolicy::Any, 1, UNIX_EPOCH, Duration::from_secs(1));
    assert!(matches!(
        empty_correlation.validate(),
        Err(error) if error.code() == ErrorCode::Validation
    ));
    let zero_expected = WaitCondition::new(
        "external/zero",
        WaitPolicy::Any,
        0,
        UNIX_EPOCH,
        Duration::from_secs(1),
    );
    assert!(matches!(
        zero_expected.validate(),
        Err(error) if error.code() == ErrorCode::Validation
    ));
}

#[test]
fn wait_condition_preserves_policy_and_time_metadata_across_immutable_updates() {
    let created_at = UNIX_EPOCH + Duration::from_secs(12);
    let timeout = Duration::from_secs(34);
    let wait = WaitCondition::new("external/metadata", WaitPolicy::Any, 2, created_at, timeout);
    let updated = wait
        .record_failure(
            "first",
            CatgaError::new(ErrorCode::Unavailable, "first provider unavailable"),
        )
        .record_success("second", [1_u8, 2, 3]);

    assert_eq!(updated.correlation_id(), "external/metadata");
    assert_eq!(updated.policy(), WaitPolicy::Any);
    assert_eq!(updated.expected_count(), 2);
    assert_eq!(updated.created_at(), created_at);
    assert_eq!(updated.timeout(), timeout);
    assert_eq!(updated.completed_count(), 2);
    assert!(updated.is_expired_at(created_at + timeout));
    assert!(updated.validate().is_ok());
}
