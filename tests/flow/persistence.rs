//! Durable flow records use one bounded MemoryPack frame format.

use std::time::{Duration, SystemTime};

use catga_core::{CatgaError, ErrorCode};
use catga_flow::{
    FlowContinuation, FlowState, WaitCondition, WaitPolicy, decode_continuation,
    encode_continuation,
};

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
fn continuation_codec_rejects_non_memorypack_format_versions() {
    for version in [1_u8, 3, 5, 6, 8] {
        let error = decode_continuation(&[version])
            .expect_err("legacy and unknown continuation frames must be rejected");

        assert_eq!(error.code(), ErrorCode::Internal);
        assert_eq!(
            error.message(),
            format!("unsupported flow continuation format version {version}")
        );
    }
}

#[test]
fn continuation_codec_requires_an_exact_memorypack_frame() {
    let continuation = FlowContinuation::new(
        FlowState::new("payment-43", "payment", [], "node-a"),
        "charge",
    );
    let mut encoded = encode_continuation(&continuation).expect("encode continuation");
    encoded.push(0);

    let error = decode_continuation(&encoded)
        .expect_err("trailing bytes must not be accepted as a second frame");

    assert_eq!(error.code(), ErrorCode::Internal);
}
