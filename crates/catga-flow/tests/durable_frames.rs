use std::time::{Duration, SystemTime, UNIX_EPOCH};

use catga_codec_memorypack::{MemoryPackSerializer, MemoryPackSnapshotCodec, MemoryPackable};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{
    FlowContinuation, FlowState, MAX_FLOW_DATA_BYTES, StateMachineSnapshot, WaitCondition,
    WaitPolicy, decode_continuation, decode_state_machine_snapshot, encode_continuation,
    encode_state_machine_snapshot, flow_timeout_deadline_unix_ms,
};

#[derive(Clone, Debug, Eq, PartialEq, MemoryPackable)]
struct State {
    paid: bool,
}

#[derive(MemoryPackable)]
struct RawTimeWire {
    before_epoch: bool,
    seconds: u64,
    nanoseconds: u32,
}

#[derive(MemoryPackable)]
struct RawFlowStateWire {
    id: String,
    flow_type: String,
    status: u8,
    step: u32,
    version: i64,
    owner: Option<String>,
    heartbeat: RawTimeWire,
    data: Vec<u8>,
    error: Option<RawErrorWire>,
}

#[derive(Default, MemoryPackable)]
struct RawErrorWire {
    code: u8,
    message: String,
    details: Option<String>,
    retryable: bool,
}

#[test]
fn continuation_codec_round_trips_and_rejects_trailing_bytes() -> CatgaResult<()> {
    let continuation = FlowContinuation::new(
        FlowState::new("payment-rollback", "payment", [], "node-a"),
        "charge",
    );
    let mut encoded = encode_continuation(&continuation)?;
    let restored = decode_continuation(&encoded)?;
    let steps: Vec<&str> = restored
        .compensation_steps()
        .iter()
        .map(AsRef::as_ref)
        .collect();
    assert!(steps.is_empty());

    encoded.push(0);
    assert!(decode_continuation(&encoded).is_err());
    Ok(())
}

#[test]
fn flow_state_codec_rejects_data_larger_than_the_durable_limit() -> CatgaResult<()> {
    let boundary = FlowState::new(
        "flow-data-boundary",
        "payment",
        vec![0_u8; MAX_FLOW_DATA_BYTES],
        "node-a",
    );
    assert!(MemoryPackSerializer::serialize(&boundary).is_ok());
    let encoded = encode_continuation(&FlowContinuation::new(boundary, "charge"))?;
    assert_eq!(
        decode_continuation(&encoded)?.state().data().len(),
        MAX_FLOW_DATA_BYTES
    );

    let oversized = FlowState::new(
        "flow-data-oversized",
        "payment",
        vec![0_u8; MAX_FLOW_DATA_BYTES + 1],
        "node-a",
    );
    assert!(MemoryPackSerializer::serialize(&oversized).is_err());
    assert!(encode_continuation(&FlowContinuation::new(oversized, "charge")).is_err());
    Ok(())
}

#[test]
fn flow_state_decoder_rejects_an_oversized_raw_data_field() -> CatgaResult<()> {
    let raw = MemoryPackSerializer::serialize(&RawFlowStateWire {
        id: "flow-data-raw".into(),
        flow_type: "payment".into(),
        status: 0,
        step: 0,
        version: 0,
        owner: Some("node-a".into()),
        heartbeat: RawTimeWire {
            before_epoch: false,
            seconds: 0,
            nanoseconds: 0,
        },
        data: vec![0_u8; MAX_FLOW_DATA_BYTES + 1],
        error: None,
    })
    .map_err(|error| {
        CatgaError::new(
            ErrorCode::Internal,
            format!("encode oversized raw flow-state frame: {error}"),
        )
    })?;

    assert!(
        MemoryPackSerializer::deserialize_bounded::<FlowState>(
            &raw,
            FlowState::memorypack_decode_limits()?,
        )
        .is_err()
    );
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
fn state_machine_frames_reject_corrupt_metadata_before_decoding_state() -> CatgaResult<()> {
    let created_at = UNIX_EPOCH + Duration::from_secs(20);
    let updated_at = created_at + Duration::from_secs(1);
    let snapshot = StateMachineSnapshot::restore(
        "order-corrupt",
        State { paid: true },
        4,
        created_at,
        updated_at,
    )?;
    let codec = MemoryPackSnapshotCodec::<State>::default();
    let encoded = encode_state_machine_snapshot(&snapshot, &codec)?;

    for mut invalid in [encoded[..34].to_vec(), encoded.clone()] {
        if invalid.len() == encoded.len() {
            invalid[0] = 2;
        }
        let error = decode_state_machine_snapshot("order-corrupt", &invalid, &codec)
            .expect_err("incomplete and unknown-version metadata are rejected");
        assert_eq!(error.code(), ErrorCode::Internal);
    }

    let mut invalid_timestamp = encoded.clone();
    invalid_timestamp[18..22].copy_from_slice(&1_000_000_000_u32.to_be_bytes());
    let error = decode_state_machine_snapshot("order-corrupt", &invalid_timestamp, &codec)
        .expect_err("timestamps cannot contain more than one billion nanoseconds");
    assert_eq!(error.code(), ErrorCode::Internal);

    let mut reversed_audit_times = encoded;
    // The first timestamp starts after the format/version metadata (1 + 8 bytes). The second
    // starts exactly one 13-byte time wire later; setting it to the epoch precedes `created_at`.
    reversed_audit_times[22..35].copy_from_slice(&[0; 13]);
    let error = decode_state_machine_snapshot("order-corrupt", &reversed_audit_times, &codec)
        .expect_err("updated audit time cannot precede the creation time");
    assert_eq!(error.code(), ErrorCode::Validation);
    Ok(())
}

#[test]
fn flow_state_memorypack_decoder_rejects_invalid_time_and_error_wires() -> CatgaResult<()> {
    let invalid_time = MemoryPackSerializer::serialize(&RawFlowStateWire {
        id: "bad-time".into(),
        flow_type: "payment".into(),
        status: 0,
        step: 0,
        version: 0,
        owner: Some("node-a".into()),
        heartbeat: RawTimeWire {
            before_epoch: false,
            seconds: 0,
            nanoseconds: 1_000_000_000,
        },
        data: Vec::new(),
        error: None,
    })
    .map_err(|error| CatgaError::new(ErrorCode::Internal, format!("encode raw time: {error}")))?;
    assert!(MemoryPackSerializer::deserialize::<FlowState>(&invalid_time).is_err());

    let invalid_error = MemoryPackSerializer::serialize(&RawFlowStateWire {
        id: "bad-error".into(),
        flow_type: "payment".into(),
        status: 0,
        step: 0,
        version: 0,
        owner: Some("node-a".into()),
        heartbeat: RawTimeWire {
            before_epoch: false,
            seconds: 0,
            nanoseconds: 0,
        },
        data: Vec::new(),
        error: Some(RawErrorWire {
            code: u8::MAX,
            message: "unknown error code".into(),
            details: None,
            retryable: false,
        }),
    })
    .map_err(|error| CatgaError::new(ErrorCode::Internal, format!("encode raw error: {error}")))?;
    assert!(MemoryPackSerializer::deserialize::<FlowState>(&invalid_error).is_err());
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
            SystemTime::UNIX_EPOCH + Duration::from_nanos(100),
            Duration::ZERO,
        ),
    );

    assert_eq!(flow_timeout_deadline_unix_ms(&continuation)?, Some(1));
    Ok(())
}
