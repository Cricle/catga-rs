//! Integration tests for `transport::codec` (the `RaftCodec` trait and its
//! implementations: `BincodeCodec`, `ProstCodec`, `JsonCodec`).
//!
//! Basic `BincodeCodec`/`JsonCodec` round trips already live in
//! `module_tests.rs`; this file focuses on construction, default trait
//! methods, edge cases, error paths and thread-safety properties.

use std::sync::Arc;

use catga_raft::transport::codec::JsonCodec;
use catga_raft::{BincodeCodec, CatgaRaftError, ProstCodec, RaftCodec};

/// A serde-serializable message resembling a Raft RPC payload.
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug, Clone)]
struct RaftLikeMessage {
    term: u64,
    from: u64,
    to: u64,
    kind: String,
    entries: Vec<Vec<u8>>,
    commit: Option<u64>,
}

fn sample_message() -> RaftLikeMessage {
    RaftLikeMessage {
        term: 7,
        from: 1,
        to: 2,
        kind: "append-entries".to_string(),
        entries: vec![vec![1, 2, 3], vec![], vec![255; 16]],
        commit: Some(42),
    }
}

/// Compile-time check that codecs satisfy the trait's Send + Sync contract.
fn assert_send_sync<T: Send + Sync>() {}

// ============================================================================
// Construction / defaults
// ============================================================================

#[test]
fn codecs_are_zero_sized_unit_structs() {
    // Codecs are stateless unit structs: trivially constructible, no config.
    let _bincode = BincodeCodec;
    let _prost = ProstCodec;
    let _json = JsonCodec;

    assert_eq!(std::mem::size_of::<BincodeCodec>(), 0);
    assert_eq!(std::mem::size_of::<ProstCodec>(), 0);
    assert_eq!(std::mem::size_of::<JsonCodec>(), 0);
}

#[test]
fn codecs_are_send_and_sync() {
    assert_send_sync::<BincodeCodec>();
    assert_send_sync::<ProstCodec>();
    assert_send_sync::<JsonCodec>();
}

// ============================================================================
// BincodeCodec happy paths and edge cases
// ============================================================================

#[test]
fn bincode_roundtrip_complex_message() {
    let codec = BincodeCodec;
    let msg = sample_message();

    let encoded = codec.encode(&msg).unwrap();
    assert!(!encoded.is_empty());

    let decoded: RaftLikeMessage = codec.decode(&encoded).unwrap();
    assert_eq!(msg, decoded);
}

#[test]
fn bincode_roundtrip_empty_payloads() {
    let codec = BincodeCodec;

    let empty_vec: Vec<u8> = Vec::new();
    let encoded = codec.encode(&empty_vec).unwrap();
    let decoded: Vec<u8> = codec.decode(&encoded).unwrap();
    assert_eq!(empty_vec, decoded);

    let empty_string = String::new();
    let encoded = codec.encode(&empty_string).unwrap();
    let decoded: String = codec.decode(&encoded).unwrap();
    assert_eq!(empty_string, decoded);
}

#[test]
fn bincode_roundtrip_large_payload() {
    let codec = BincodeCodec;
    let data: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();

    let encoded = codec.encode(&data).unwrap();
    let decoded: Vec<u8> = codec.decode(&encoded).unwrap();
    assert_eq!(data, decoded);
}

#[test]
fn bincode_decode_empty_input_fails_with_codec_error() {
    let codec = BincodeCodec;
    let err = codec.decode::<Vec<u8>>(&[]).unwrap_err();

    match &err {
        CatgaRaftError::Codec(msg) => assert!(
            msg.contains("bincode decode error"),
            "unexpected codec message: {msg}"
        ),
        other => panic!("expected Codec error, got: {other:?}"),
    }
    assert!(err.to_string().starts_with("codec error:"));
}

#[test]
fn bincode_decode_truncated_input_fails() {
    let codec = BincodeCodec;
    let data: Vec<u8> = vec![9; 64];

    let encoded = codec.encode(&data).unwrap();
    assert!(encoded.len() > 4);
    let truncated = &encoded[..3];

    assert!(codec.decode::<Vec<u8>>(truncated).is_err());
}

#[test]
fn bincode_decode_wrong_type_fails() {
    let codec = BincodeCodec;

    // "abc" encodes to 4 bytes (varint length + payload); a fixed-size
    // [u8; 8] needs 8 raw bytes, so decoding must fail on the short input.
    let encoded = codec.encode(&String::from("abc")).unwrap();
    assert!(codec.decode::<[u8; 8]>(&encoded).is_err());
}

// ============================================================================
// ProstCodec (placeholder implementation)
// ============================================================================

#[test]
fn prost_codec_encode_and_decode_report_unimplemented() {
    let codec = ProstCodec;

    let encode_err = codec.encode(&sample_message()).unwrap_err();
    match &encode_err {
        CatgaRaftError::Codec(msg) => assert!(
            msg.contains("protobuf code generation"),
            "unexpected message: {msg}"
        ),
        other => panic!("expected Codec error, got: {other:?}"),
    }

    let decode_err = codec.decode::<RaftLikeMessage>(&[1, 2, 3]).unwrap_err();
    match &decode_err {
        CatgaRaftError::Codec(msg) => assert!(
            msg.contains("protobuf code generation"),
            "unexpected message: {msg}"
        ),
        other => panic!("expected Codec error, got: {other:?}"),
    }
}

#[test]
fn prost_codec_default_message_methods_also_fail() {
    // Default trait methods delegate to encode/decode, so they must surface
    // the same placeholder error.
    let codec = ProstCodec;

    assert!(codec.encode_message(&sample_message()).is_err());
    assert!(codec.decode_message::<RaftLikeMessage>(&[0]).is_err());
    assert!(codec.encode_snapshot(&vec![1u8, 2, 3]).is_err());
    assert!(codec.decode_snapshot::<Vec<u8>>(&[0]).is_err());
}

// ============================================================================
// JsonCodec happy paths and error cases
// ============================================================================

#[test]
fn json_encode_produces_readable_json_and_roundtrips() {
    let codec = JsonCodec;
    let msg = sample_message();

    let encoded = codec.encode(&msg).unwrap();

    // Output must be valid JSON with the expected fields.
    let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(value["term"], 7);
    assert_eq!(value["kind"], "append-entries");
    assert_eq!(value["commit"], 42);

    let decoded: RaftLikeMessage = codec.decode(&encoded).unwrap();
    assert_eq!(msg, decoded);
}

#[test]
fn json_decode_invalid_input_fails_with_codec_error() {
    let codec = JsonCodec;
    let err = codec.decode::<RaftLikeMessage>(b"this is not json").unwrap_err();

    match &err {
        CatgaRaftError::Codec(msg) => assert!(
            msg.contains("JSON decode error"),
            "unexpected message: {msg}"
        ),
        other => panic!("expected Codec error, got: {other:?}"),
    }
}

#[test]
fn json_encode_nan_serializes_as_null() {
    let codec = JsonCodec;

    // serde_json maps non-finite floats to null, so encoding succeeds.
    let bytes = codec.encode(&f64::NAN).unwrap();
    assert_eq!(bytes, b"null");
}

// ============================================================================
// Default trait methods (encode_message / decode_message / snapshots)
// ============================================================================

#[test]
fn default_message_and_snapshot_methods_delegate_to_encode_decode() {
    let codec = BincodeCodec;
    let msg = sample_message();

    let via_message = codec.encode_message(&msg).unwrap();
    let via_encode = codec.encode(&msg).unwrap();
    assert_eq!(via_message, via_encode);

    let decoded: RaftLikeMessage = codec.decode_message(&via_message).unwrap();
    assert_eq!(msg, decoded);

    let snapshot = sample_message().entries;
    let snap_bytes = codec.encode_snapshot(&snapshot).unwrap();
    assert_eq!(snap_bytes, codec.encode(&snapshot).unwrap());
    let decoded_snap: Vec<Vec<u8>> = codec.decode_snapshot(&snap_bytes).unwrap();
    assert_eq!(snapshot, decoded_snap);
}

// ============================================================================
// Cross-codec behaviour
// ============================================================================

#[test]
fn codecs_are_not_wire_compatible() {
    let bincode = BincodeCodec;
    let json = JsonCodec;
    let msg = sample_message();

    // Bytes produced by one codec must not silently decode with the other.
    let bincode_bytes = bincode.encode(&msg).unwrap();
    assert!(json.decode::<RaftLikeMessage>(&bincode_bytes).is_err());

    let json_bytes = json.encode(&msg).unwrap();
    assert!(bincode.decode::<RaftLikeMessage>(&json_bytes).is_err());
}

// ============================================================================
// Concurrency
// ============================================================================

#[tokio::test]
async fn codec_is_shared_safely_across_tasks() {
    let codec = Arc::new(BincodeCodec);
    let mut handles = Vec::new();

    for i in 0..4u64 {
        let c = Arc::clone(&codec);
        handles.push(tokio::spawn(async move {
            let bytes = c.encode(&i).unwrap();
            c.decode::<u64>(&bytes).unwrap()
        }));
    }

    for (i, handle) in handles.into_iter().enumerate() {
        assert_eq!(handle.await.unwrap(), i as u64);
    }
}
