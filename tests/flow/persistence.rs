use std::time::{Duration, SystemTime};

use catga_core::{CatgaError, ErrorCode};
use catga_flow::{
    FlowContinuation, FlowState, WaitCondition, WaitPolicy, decode_continuation,
    encode_continuation,
};
use serde::Serialize;

#[derive(Serialize)]
struct VersionOneContinuation {
    state: FlowState,
    step_name: Box<str>,
    wait: Option<WaitCondition>,
    resume_at: Option<SystemTime>,
}

#[derive(Serialize)]
struct VersionThreeWaitCondition {
    correlation_id: Box<str>,
    policy: WaitPolicy,
    expected_count: u32,
    results: Vec<VersionThreeWaitResult>,
    created_at: SystemTime,
    timeout: Duration,
}

#[derive(Serialize)]
struct VersionThreeWaitResult {
    child_id: Box<str>,
    payload: Option<Vec<u8>>,
    error: Option<CatgaError>,
}

#[derive(Serialize)]
struct VersionThreeContinuation {
    state: FlowState,
    step_name: Box<str>,
    wait: Option<VersionThreeWaitCondition>,
    resume_at: Option<SystemTime>,
    schedule_id: Option<Box<str>>,
    created_at: SystemTime,
}

#[derive(Serialize)]
struct VersionFourContinuation {
    state: FlowState,
    step_name: Box<str>,
    wait: Option<WaitCondition>,
    resume_at: Option<SystemTime>,
    schedule_id: Option<Box<str>>,
    created_at: SystemTime,
}

#[test]
fn continuation_codec_preserves_terminal_error_and_wait_results() {
    let state = FlowState::new("payment-42", "payment", b"input".to_vec(), "node-a")
        .failed(CatgaError::new(ErrorCode::Validation, "payment declined"));
    let wait = WaitCondition::new(
        "payment-42-children",
        WaitPolicy::All,
        3,
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
        Duration::from_secs(30),
    )
    .record_success("receipt", b"approved".to_vec())
    .record_failure(
        "notification",
        CatgaError::new(ErrorCode::Transient, "notification unavailable"),
    );
    let continuation = FlowContinuation::waiting(state, "charge", wait);

    let encoded = encode_continuation(&continuation).expect("encode continuation");
    let restored = decode_continuation(&encoded).expect("decode continuation");

    assert_eq!(restored, continuation);
    assert_eq!(restored.wait().expect("wait condition").results().len(), 2);
}

#[test]
fn continuation_codec_rejects_unknown_format_versions_explicitly() {
    let error = decode_continuation(&[6]).expect_err("unknown format version must fail");

    assert_eq!(error.code(), ErrorCode::Internal);
    assert_eq!(
        error.message(),
        "unsupported flow continuation format version 6"
    );
}

#[test]
fn continuation_codec_migrates_v3_waits_without_child_launch_intents() {
    let created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_100);
    let legacy = VersionThreeContinuation {
        state: FlowState::new("payment-44", "payment", b"input".to_vec(), "node-a"),
        step_name: "charge".into(),
        wait: Some(VersionThreeWaitCondition {
            correlation_id: "payment-44-children".into(),
            policy: WaitPolicy::All,
            expected_count: 1,
            results: vec![VersionThreeWaitResult {
                child_id: "receipt".into(),
                payload: Some(b"approved".to_vec()),
                error: None,
            }],
            created_at,
            timeout: Duration::from_secs(30),
        }),
        resume_at: None,
        schedule_id: None,
        created_at,
    };
    let mut encoded = vec![3];
    encoded.extend(postcard::to_allocvec(&legacy).expect("encode v3 continuation"));

    let restored = decode_continuation(&encoded).expect("v3 layout must migrate");
    let wait = restored.wait().expect("migrated wait");

    assert_eq!(restored.created_at(), created_at);
    assert_eq!(wait.results().len(), 1);
    assert!(wait.child_launches().is_empty());
}

#[test]
fn continuation_codec_migrates_v4_records_without_compensations() {
    let created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_200);
    let legacy = VersionFourContinuation {
        state: FlowState::new("payment-45", "payment", b"input".to_vec(), "node-a"),
        step_name: "charge".into(),
        wait: None,
        resume_at: None,
        schedule_id: Some("schedule-45".into()),
        created_at,
    };
    let mut encoded = vec![4];
    encoded.extend(postcard::to_allocvec(&legacy).expect("encode v4 continuation"));

    let restored = decode_continuation(&encoded).expect("v4 layout must migrate");

    assert_eq!(restored.state(), &legacy.state);
    assert_eq!(restored.step_name(), legacy.step_name.as_ref());
    assert_eq!(restored.schedule_id(), legacy.schedule_id.as_deref());
    assert_eq!(restored.created_at(), created_at);
    assert!(restored.compensation_steps().is_empty());
}

#[test]
fn continuation_codec_migrates_the_previous_field_layout_without_a_schedule_id() {
    let legacy = VersionOneContinuation {
        state: FlowState::new("payment-43", "payment", b"input".to_vec(), "node-a"),
        step_name: "charge".into(),
        wait: None,
        resume_at: None,
    };
    let mut encoded = vec![1];
    encoded.extend(postcard::to_allocvec(&legacy).expect("encode legacy continuation"));

    let restored = decode_continuation(&encoded).expect("v1 layout must migrate to v2");

    assert_eq!(restored.state(), &legacy.state);
    assert_eq!(restored.step_name(), legacy.step_name.as_ref());
    assert!(restored.wait().is_none());
    assert_eq!(restored.resume_at(), legacy.resume_at);
    assert!(restored.schedule_id().is_none());
}
