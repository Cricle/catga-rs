//! Public MemoryPack codec API tests.

use std::{sync::Arc, time::Duration};

use catga_codec_memorypack::{
    MemoryPackCodec, MemoryPackRequestClient, MemoryPackRpcResponse, MemoryPackSerializer,
    MemoryPackSnapshotCodec, MemoryPackable,
};
use catga_core::{
    CatgaError, CatgaResult, Envelope, ErrorCode, MessageMetadata, Request, RequestClient,
    RequestTransport, SnapshotCodec, SnowflakeIdGenerator, SnowflakeLayout,
};

#[derive(Debug, Eq, PartialEq, MemoryPackable)]
struct Value {
    id: u64,
    name: String,
}

#[derive(MemoryPackable, catga_core::Message)]
struct WireOnlyRequest(u32);

impl Request for WireOnlyRequest {
    type Response = WireOnlyResponse;
}

#[derive(Debug, Eq, PartialEq, MemoryPackable)]
struct WireOnlyResponse(u32);

struct FailingRequestTransport;

#[async_trait::async_trait]
impl RequestTransport for FailingRequestTransport {
    async fn request(&self, _: &str, _: Envelope, _: Duration) -> CatgaResult<Envelope> {
        Err(CatgaError::new(
            ErrorCode::Unavailable,
            "test transport is unavailable",
        ))
    }
}

#[test]
fn value_helpers_round_trip_and_reuse_the_caller_buffer() {
    let codec = MemoryPackCodec::default();
    let value = Value {
        id: 42,
        name: "memorypack".into(),
    };
    let mut output = Vec::with_capacity(128);
    let capacity = output.capacity();

    codec
        .encode_value_into(&value, &mut output)
        .expect("value encodes into the caller buffer");

    assert_eq!(output.capacity(), capacity);
    assert_eq!(
        output,
        codec
            .encode_value(&value)
            .expect("value encodes into a new frame")
    );
    assert_eq!(
        codec.decode_value::<Value>(&output).expect("value decodes"),
        value
    );
}

#[test]
fn serializer_writes_directly_into_a_reusable_buffer() {
    let value = Value {
        id: 7,
        name: "direct-write".into(),
    };
    let mut output = Vec::with_capacity(128);
    let allocation = output.as_ptr();

    MemoryPackSerializer::serialize_into(&value, &mut output)
        .expect("the reusable buffer serializes");

    assert_eq!(output.as_ptr(), allocation);
    assert_eq!(
        MemoryPackSerializer::deserialize::<Value>(&output).expect("the direct frame decodes"),
        value
    );
}

#[test]
fn typed_rpc_responses_preserve_success_and_failure() {
    let codec = MemoryPackCodec::default();
    let request = Envelope::new(1, "request", vec![], MessageMetadata::new(2, Some(3)));
    let value = Value {
        id: 42,
        name: "memorypack".into(),
    };

    let success = codec
        .typed_success(&request, &value)
        .expect("success response encodes");
    assert!(matches!(
        codec
            .decode_rpc_response::<Value>(success.payload())
            .expect("success response decodes"),
        MemoryPackRpcResponse::Success(decoded) if decoded == value
    ));

    let failure = codec
        .typed_failure(
            &request,
            CatgaError::new(ErrorCode::Conflict, "already exists"),
        )
        .expect("failure response encodes");
    assert!(matches!(
        codec
            .decode_rpc_response::<Value>(failure.payload())
            .expect("failure response decodes"),
        MemoryPackRpcResponse::Failure(error) if error.code() == ErrorCode::Conflict
    ));
}

#[test]
fn snapshot_codec_uses_the_same_strict_memorypack_frame_decoder() {
    let codec = MemoryPackSnapshotCodec::<Value>::default();
    let value = Value {
        id: 42,
        name: "memorypack".into(),
    };
    let mut bytes = codec.encode_state(&value).expect("snapshot encodes");
    bytes.push(0);

    let error = codec
        .decode_state(&bytes)
        .expect_err("snapshot decode must reject trailing input");

    assert_eq!(error.code(), ErrorCode::Validation);
}

#[test]
fn memorypack_request_client_implements_the_format_agnostic_core_trait() {
    let generator = Arc::new(
        SnowflakeIdGenerator::new(1, SnowflakeLayout::default())
            .expect("test Snowflake configuration is valid"),
    );
    let client = MemoryPackRequestClient::new(
        Arc::new(FailingRequestTransport),
        "inventory",
        Duration::from_secs(1),
        generator,
    )
    .expect("client configuration is valid");

    let error = futures::executor::block_on(RequestClient::request(&client, &WireOnlyRequest(7)))
        .expect_err("the test transport must return its error");

    assert_eq!(error.code(), ErrorCode::Unavailable);
}
