#![allow(missing_docs)]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use catga_codec_memorypack::{MemoryPackSnapshotCodec, MemoryPackable};
use catga_core::CatgaResult;
use catga_flow::{
    FlowContinuation, FlowState, StateMachineSnapshot, WaitCondition, WaitPolicy,
    decode_continuation, decode_state_machine_snapshot, encode_continuation,
    encode_state_machine_snapshot, flow_timeout_deadline_unix_ms,
};

#[derive(Clone, Debug, Eq, PartialEq, MemoryPackable)]
struct State {
    paid: bool,
}

#[test]
fn continuation_codec_preserves_compensation_and_rejects_trailing_bytes() -> CatgaResult<()> {
    let continuation = FlowContinuation::new(
        FlowState::new("payment-rollback", "payment", [], "node-a"),
        "charge",
    )
    .record_compensation("reserve")?
    .record_compensation("charge")?;
    let mut encoded = encode_continuation(&continuation)?;
    let restored = decode_continuation(&encoded)?;
    let steps: Vec<&str> = restored
        .compensation_steps()
        .iter()
        .map(AsRef::as_ref)
        .collect();
    assert_eq!(steps, ["reserve", "charge"]);

    encoded.push(0);
    assert!(decode_continuation(&encoded).is_err());
    Ok(())
}

#[test]
fn state_machine_frames_preserve_audit_times() -> CatgaResult<()> {
    let created_at = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let updated_at = created_at + Duration::from_nanos(42);
    let snapshot =
        StateMachineSnapshot::restore("order-7", State { paid: false }, 4, created_at, updated_at)?;
    let codec = MemoryPackSnapshotCodec::<State>::default();

    let encoded = encode_state_machine_snapshot(&snapshot, &codec)?;
    let decoded = decode_state_machine_snapshot("order-7", &encoded, &codec)?;

    assert_eq!(decoded, snapshot);
    assert_eq!(decoded.created_at(), created_at);
    assert_eq!(decoded.updated_at(), updated_at);
    Ok(())
}

#[test]
fn timeout_deadline_rounds_fractional_milliseconds_up() -> CatgaResult<()> {
    let continuation = FlowContinuation::waiting(
        FlowState::new("fractional-timeout", "test", [], "node-a").suspended(),
        "resume",
        WaitCondition::new(
            "fractional-timeout/wait",
            WaitPolicy::All,
            1,
            SystemTime::UNIX_EPOCH + Duration::from_nanos(1),
            Duration::ZERO,
        ),
    );

    assert_eq!(flow_timeout_deadline_unix_ms(&continuation)?, Some(1));
    Ok(())
}
