//! Strict contract tests for the MemoryPack and Bincode codecs: scalar and
//! collection round trips, decode budget enforcement, the envelope wire
//! record, typed RPC responses, the scheduled outbox, and the request client.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, LinkedList, VecDeque};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use catga_core::codec::bincode::BincodeCodec;
use catga_core::codec::memorypack::api::{
    MemoryPackRequestClientFactory, MemoryPackScheduledOutbox,
};
use catga_core::codec::memorypack::traits::{MultiDimArray, NullableString, NullableVec};
use catga_core::codec::memorypack::{
    MemoryPackCodec, MemoryPackDecodeLimits, MemoryPackDeserialize, MemoryPackError,
    MemoryPackReader, MemoryPackRpcResponse, MemoryPackSerialize, MemoryPackSerializer,
    MemoryPackSnapshotCodec, MemoryPackWriter, MemoryPackable,
};
use catga_core::memory::MemoryOutbox;
use catga_core::{
    CatgaError, CatgaResult, DelayedMessage, Envelope, EnvelopeCodec, EnvelopeHeaders, ErrorCode,
    Event, Message, MessageMetadata, OutboxStore, PayloadDecoder, PayloadEncoder, QualityOfService,
    Request, RequestTransport, SnapshotCodec,
};

fn round_trip<T>(value: T)
where
    T: MemoryPackSerialize + MemoryPackDeserialize + PartialEq + Debug,
{
    let bytes = MemoryPackSerializer::serialize(&value).expect("serialize succeeds");
    let decoded = MemoryPackSerializer::deserialize::<T>(&bytes).expect("deserialize succeeds");
    assert_eq!(decoded, value);
}

// ---------------------------------------------------------------------------
// Trait implementations
// ---------------------------------------------------------------------------

#[test]
fn memorypack_scalars_round_trip() {
    round_trip(());
    round_trip(true);
    round_trip(false);
    round_trip(-5_i8);
    round_trip(250_u8);
    round_trip(-300_i16);
    round_trip(60_000_u16);
    round_trip(-70_000_i32);
    round_trip(4_000_000_000_u32);
    round_trip(-9_000_000_000_i64);
    round_trip(18_000_000_000_u64);
    round_trip(1.25_f32);
    round_trip(-2.5_f64);
    round_trip(-170_141_183_460_469_231_731_687_303_715_884_i128);
    round_trip(340_282_366_920_938_463_463_374_607_431_768_u128);
    round_trip('é');
}

#[test]
fn memorypack_strings_and_options_round_trip() {
    round_trip(String::new());
    round_trip("hello world".to_string());
    round_trip("多字节字符 🚀".to_string());
    round_trip(Box::<str>::from("borrowed"));
    round_trip(std::borrow::Cow::Borrowed("cow"));

    round_trip(Option::<u32>::None);
    round_trip(Some(42_u32));
    round_trip(NullableString(None));
    round_trip(NullableString(Some("kept".to_string())));
    round_trip(NullableVec::<u32>(None));
    round_trip(NullableVec(Some(vec![1_u32, 2, 3])));
}

#[test]
fn memorypack_collections_round_trip() {
    round_trip(vec![1_u32, 2, 3]);
    round_trip(Vec::<u8>::new());
    round_trip([1_i64, -2, 3]);
    round_trip(VecDeque::from([4_u16, 5]));
    round_trip(LinkedList::from([6_i32, 7]));
    round_trip(HashSet::from([8_u32, 9]));
    round_trip(BTreeSet::from([10_u32, 11]));

    round_trip(HashMap::from([
        ("a".to_string(), 1_u32),
        ("b".to_string(), 2),
    ]));
    round_trip(HashMap::from([(7_i32, vec![1_u8])]));
    round_trip(BTreeMap::from([("k".to_string(), 9_u64)]));
    round_trip(BTreeMap::from([(3_u64, 4_i128)]));
    round_trip(HashMap::from([('x', true)]));

    round_trip(hashbrown::HashMap::from([("h".to_string(), 2_u32)]));
    round_trip(hashbrown::HashSet::from([5_i32, 6]));
    round_trip(ahash::AHashMap::from([(1_i64, 2_u16)]));
    round_trip(ahash::AHashSet::from([7_u8]));

    round_trip((1_u8,));
    round_trip((1_u8, "two".to_string()));
    round_trip((1_u8, 2_u16, 3_u32, 4_u64));

    round_trip(Box::new(5_u64));
    round_trip(std::rc::Rc::new(6_u64));
    round_trip(Arc::new(7_u64));

    let grid = MultiDimArray::new(vec![2, 2], vec![1_i32, 2, 3, 4]).expect("shape validates");
    assert_eq!(grid.rank(), 2);
    round_trip(grid);
    assert!(MultiDimArray::new(vec![2, 3], vec![1_i32]).is_err());
    assert!(MultiDimArray::new(vec![usize::MAX, usize::MAX], Vec::<u8>::new()).is_err());
}

#[test]
fn memorypack_extended_math_and_datetime_types_round_trip() {
    round_trip(uuid::Uuid::from_u128(
        0x1234_5678_9abc_def0_1234_5678_9abc_def0,
    ));
    round_trip(rust_decimal::Decimal::new(-12_345, 2));
    round_trip(half::f16::from_f32(1.5));
    round_trip(
        "-123456789012345678901234567890"
            .parse::<num_bigint::BigInt>()
            .expect("parses"),
    );
    round_trip(
        "123456789012345678901234567890"
            .parse::<num_bigint::BigUint>()
            .expect("parses"),
    );
    round_trip(url::Url::parse("https://example.com/path?q=1").expect("parses"));

    round_trip(num_complex::Complex::new(1.5_f64, -2.25_f64));
    round_trip(glam::Vec2::new(1.0, -2.5));
    round_trip(glam::Vec3::new(1.0, 2.0, 3.5));
    round_trip(glam::Vec4::new(1.0, 2.0, 3.0, 4.5));
    round_trip(glam::Quat::from_xyzw(0.1, 0.2, 0.3, 0.9));
    round_trip(glam::Mat3A::from_cols_array(&[
        1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.5, 0.25, 1.0,
    ]));
    round_trip(glam::Mat4::from_cols_array(&[
        1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.5, 2.5, 3.5, 1.0,
    ]));

    round_trip(chrono::TimeDelta::milliseconds(-5_000));
    round_trip(chrono::DateTime::from_timestamp(1_700_000_000, 123_456_700).expect("valid"));
    round_trip(chrono::NaiveTime::from_hms_nano_opt(23, 59, 59, 999_999_900).expect("valid"));
    round_trip(chrono::NaiveDate::from_ymd_opt(2024, 2, 29).expect("valid"));
    let offset = chrono::FixedOffset::east_opt(3_600).expect("valid");
    round_trip(
        chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .expect("valid")
            .with_timezone(&offset),
    );
    let _ = chrono::DateTime::from_timestamp(1_700_000_000, 0)
        .expect("valid")
        .with_timezone(&chrono::Local);
}

// ---------------------------------------------------------------------------
// Decode budgets and frame exactness
// ---------------------------------------------------------------------------

#[test]
fn memorypack_decode_limits_validate_and_bound_reads() {
    // Zero budgets and inconsistent budgets are rejected.
    assert!(MemoryPackDecodeLimits::new(0, 1, 1, 1, 1).is_err());
    assert!(MemoryPackDecodeLimits::new(8, 4, 8, 1, 1).is_err());
    let limits = MemoryPackDecodeLimits::new(64, 64, 32, 4, 2).expect("limits build");
    assert_eq!(limits.max_frame_bytes(), 64);

    // A frame larger than the receive budget fails before any decoding.
    let bytes = MemoryPackSerializer::serialize(&vec![1_u32, 2, 3]).expect("serialize succeeds");
    let tight = MemoryPackDecodeLimits::new(2, 4096, 256, 64, 8).expect("limits build");
    assert!(matches!(
        MemoryPackSerializer::deserialize_bounded::<Vec<u32>>(&bytes, tight),
        Err(MemoryPackError::LimitExceeded { .. })
    ));

    // A collection larger than its item budget fails before allocation.
    let two_items = MemoryPackDecodeLimits::new(64, 64, 32, 2, 8).expect("limits build");
    assert!(matches!(
        MemoryPackSerializer::deserialize_bounded::<Vec<u32>>(&bytes, two_items),
        Err(MemoryPackError::LimitExceeded { .. })
    ));

    // A negative collection length is malformed input.
    let malformed = (-2_i32).to_le_bytes();
    let mut reader = MemoryPackReader::new_bounded(&malformed, MemoryPackDecodeLimits::default())
        .expect("reader builds");
    assert!(matches!(
        Vec::<u32>::deserialize(&mut reader),
        Err(MemoryPackError::InvalidLength(-2))
    ));

    // Nesting depth budgets guard derived object scopes.
    let mut reader = MemoryPackReader::new_bounded(&[], limits).expect("reader builds");
    reader.enter_object().expect("first scope enters");
    reader.enter_object().expect("second scope enters");
    assert!(matches!(
        reader.enter_object(),
        Err(MemoryPackError::LimitExceeded { .. })
    ));
    reader.leave_object();
    reader.leave_object();

    // Deserialization consumes exactly one frame.
    assert!(matches!(
        MemoryPackSerializer::deserialize::<u8>(&[5, 5]),
        Err(MemoryPackError::TrailingBytes)
    ));

    // Truncated frames fail instead of panicking.
    assert!(MemoryPackSerializer::deserialize::<u64>(&[1, 2]).is_err());
}

// ---------------------------------------------------------------------------
// MemoryPackCodec and the envelope wire record
// ---------------------------------------------------------------------------

#[derive(MemoryPackable, Clone, Debug, PartialEq)]
struct Reminder {
    text: String,
    count: u32,
}
impl Message for Reminder {}

fn reminder() -> Reminder {
    Reminder {
        text: "stretch".into(),
        count: 3,
    }
}

#[test]
fn memorypack_codec_bounds_values_and_reuses_buffers() {
    let codec = MemoryPackCodec::default();
    assert_eq!(codec.decode_limits(), MemoryPackDecodeLimits::default());

    // Typed values round-trip through the codec helpers.
    let bytes = codec.encode_value(&reminder()).expect("encode succeeds");
    assert_eq!(
        codec
            .decode_value::<Reminder>(&bytes)
            .expect("decode succeeds"),
        reminder()
    );

    // The payload traits share the same bounded path.
    let bytes = codec.encode_payload(&reminder()).expect("encode succeeds");
    let decoded: Reminder = codec.decode_payload(&bytes).expect("decode succeeds");
    assert_eq!(decoded, reminder());

    // Caller-owned buffers retain their allocation across frames.
    let mut reusable = Vec::with_capacity(128);
    codec
        .encode_value_into(&reminder(), &mut reusable)
        .expect("encode succeeds");
    assert_eq!(
        MemoryPackSerializer::deserialize::<Reminder>(&reusable).expect("decode succeeds"),
        reminder()
    );

    // Outbound frames obey the configured ceiling on every entry point.
    let tight = MemoryPackCodec::new(
        MemoryPackDecodeLimits::new(4, 4096, 256, 64, 8).expect("limits build"),
    );
    let error = tight
        .encode_value(&reminder())
        .expect_err("an oversized outbound frame must fail");
    assert_eq!(error.code(), ErrorCode::Validation);
    let error = tight
        .encode_payload(&reminder())
        .expect_err("an oversized outbound payload must fail");
    assert_eq!(error.code(), ErrorCode::Validation);
    let mut output = Vec::new();
    let error = tight
        .encode_value_into(&reminder(), &mut output)
        .expect_err("an oversized buffer encode must fail");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert!(
        output.is_empty(),
        "a rejected frame leaves no partial bytes"
    );

    // Corrupt input maps to a validation failure.
    let error = codec
        .decode_value::<Reminder>(&[0xff, 0xff, 0xff])
        .expect_err("corrupt input must fail");
    assert_eq!(error.code(), ErrorCode::Validation);
}

#[test]
fn memorypack_envelopes_round_trip_the_full_wire_record() {
    let codec = MemoryPackCodec::default();
    let headers =
        EnvelopeHeaders::try_new([("tenant", "a"), ("trace", "t-1")]).expect("headers build");
    let envelope = Envelope::versioned(
        42,
        "Reminder",
        codec.encode_value(&reminder()).expect("encode succeeds"),
        MessageMetadata::new(42, Some(7))
            .with_quality_of_service(QualityOfService::AtMostOnce)
            .with_priority(catga_core::MessagePriority::High),
        3,
    )
    .with_reply_to("inbox.reply")
    .with_headers(headers)
    .with_sent_at(SystemTime::now())
    .expect("sent_at builds");

    let bytes = codec.encode(&envelope).expect("encode succeeds");
    let decoded = codec.decode(&bytes).expect("decode succeeds");
    assert_eq!(decoded.id(), 42);
    assert_eq!(decoded.message_type(), "Reminder");
    assert_eq!(decoded.schema_version(), 3);
    assert_eq!(decoded.reply_to(), Some("inbox.reply"));
    assert_eq!(decoded.metadata().correlation_id(), Some(7));
    assert_eq!(
        decoded.metadata().quality_of_service(),
        QualityOfService::AtMostOnce
    );
    assert_eq!(
        decoded.metadata().priority(),
        catga_core::MessagePriority::High
    );
    assert_eq!(decoded.sent_at_unix_ms(), envelope.sent_at_unix_ms());
    assert_eq!(decoded.header("tenant"), Some("a"));
    assert_eq!(decoded.header("trace"), Some("t-1"));
    assert_eq!(decoded.payload(), envelope.payload());

    // The buffer-reusing envelope encode matches the allocating one.
    let mut output = Vec::new();
    codec
        .encode_into(&envelope, &mut output)
        .expect("encode succeeds");
    assert_eq!(output, bytes);

    // Garbage frames fail as validation errors.
    let error = codec.decode(&[0xff]).expect_err("corrupt input must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    // Outbound envelopes obey the frame ceiling.
    let tight = MemoryPackCodec::new(
        MemoryPackDecodeLimits::new(8, 4096, 256, 64, 8).expect("limits build"),
    );
    let error = tight
        .encode(&envelope)
        .expect_err("an oversized outbound envelope must fail");
    assert_eq!(error.code(), ErrorCode::Validation);
}

// ---------------------------------------------------------------------------
// Typed RPC responses
// ---------------------------------------------------------------------------

#[test]
fn memorypack_rpc_responses_round_trip_success_and_failure() {
    let codec = MemoryPackCodec::default();

    let success = MemoryPackRpcResponse::Success(reminder());
    let bytes = MemoryPackSerializer::serialize(&success).expect("serialize succeeds");
    assert_eq!(
        codec
            .decode_rpc_response::<Reminder>(&bytes)
            .expect("decode succeeds"),
        success
    );

    let failure = MemoryPackRpcResponse::<Reminder>::Failure(
        CatgaError::new(ErrorCode::Timeout, "slow").with_details("stage-2"),
    );
    let bytes = MemoryPackSerializer::serialize(&failure).expect("serialize succeeds");
    let decoded = codec
        .decode_rpc_response::<Reminder>(&bytes)
        .expect("decode succeeds");
    let MemoryPackRpcResponse::Failure(error) = decoded else {
        panic!("a failure tag decodes as a failure");
    };
    assert_eq!(error.code(), ErrorCode::Timeout);
    assert_eq!(error.message(), "slow");
    assert_eq!(error.details(), Some("stage-2"));

    // Unknown tags and unknown error codes are malformed frames.
    assert!(codec.decode_rpc_response::<Reminder>(&[9]).is_err());
}

#[test]
fn memorypack_typed_response_envelopes_preserve_correlation() {
    let codec = MemoryPackCodec::default();
    let request = Envelope::new(
        11,
        "Ping",
        codec.encode_value(&5_u64).expect("encode succeeds"),
        MessageMetadata::new(11, Some(11)).with_priority(catga_core::MessagePriority::Critical),
    );

    let response = codec
        .typed_success(&request, &reminder())
        .expect("typed success builds");
    assert_eq!(response.metadata().correlation_id(), Some(11));
    assert_eq!(
        response.metadata().priority(),
        catga_core::MessagePriority::Critical
    );
    match codec
        .decode_rpc_response::<Reminder>(response.payload())
        .expect("decode succeeds")
    {
        MemoryPackRpcResponse::Success(value) => assert_eq!(value, reminder()),
        MemoryPackRpcResponse::Failure(_) => panic!("a success tag decodes as a success"),
    }

    let response = codec
        .typed_failure(&request, CatgaError::new(ErrorCode::Unavailable, "down"))
        .expect("typed failure builds");
    match codec
        .decode_rpc_response::<Reminder>(response.payload())
        .expect("decode succeeds")
    {
        MemoryPackRpcResponse::Failure(error) => {
            assert_eq!(error.code(), ErrorCode::Unavailable);
        }
        MemoryPackRpcResponse::Success(_) => panic!("a failure tag decodes as a failure"),
    }
}

// ---------------------------------------------------------------------------
// Snapshot codec
// ---------------------------------------------------------------------------

#[test]
fn memorypack_snapshot_codec_round_trips_state() {
    let codec = MemoryPackSnapshotCodec::<Reminder>::default();
    let bytes = codec.encode_state(&reminder()).expect("encode succeeds");
    assert_eq!(
        codec.decode_state(&bytes).expect("decode succeeds"),
        reminder()
    );
    assert!(codec.decode_state(&[0xff, 0xff]).is_err());
}

// ---------------------------------------------------------------------------
// Scheduled outbox
// ---------------------------------------------------------------------------

fn ids() -> Arc<catga_core::SnowflakeIdGenerator> {
    Arc::new(
        catga_core::SnowflakeIdGenerator::new(1, catga_core::SnowflakeLayout::default())
            .expect("generator builds"),
    )
}

#[derive(MemoryPackable, Clone)]
struct Later {
    text: String,
}
impl Message for Later {}
impl DelayedMessage for Later {
    fn delay(&self) -> Option<Duration> {
        Some(Duration::from_millis(1))
    }
}

#[derive(MemoryPackable, Clone)]
struct Notice {
    text: String,
}
impl Message for Notice {}
impl Event for Notice {}
impl DelayedMessage for Notice {
    fn delay(&self) -> Option<Duration> {
        Some(Duration::from_millis(1))
    }
}

#[tokio::test]
async fn memorypack_scheduled_outbox_persists_typed_delayed_messages() {
    let outbox = Arc::new(MemoryOutbox::default());
    let scheduler = MemoryPackScheduledOutbox::new(Arc::clone(&outbox), ids());

    // A far-future schedule persists but is not yet claimable, and cancels.
    let id = scheduler
        .schedule_at(&reminder(), SystemTime::now() + Duration::from_secs(3600))
        .await
        .expect("schedule succeeds");
    assert!(
        outbox
            .claim("worker", 10)
            .await
            .expect("claim succeeds")
            .is_empty()
    );
    assert!(scheduler.cancel(id).await.expect("cancel succeeds"));
    assert!(!scheduler.cancel(id).await.expect("cancel succeeds"));

    // A zero delay is immediately due with its typed payload and QoS intact.
    let id = scheduler
        .schedule_after(&reminder(), Duration::ZERO)
        .await
        .expect("schedule succeeds");
    let claimed = outbox.claim("worker", 10).await.expect("claim succeeds");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id(), id);
    assert_eq!(
        claimed[0].envelope().message_type(),
        reminder().message_type()
    );
    assert_eq!(
        claimed[0].envelope().metadata().quality_of_service(),
        QualityOfService::AtLeastOnce
    );
    let decoded = MemoryPackCodec::default()
        .decode_value::<Reminder>(claimed[0].envelope().payload())
        .expect("payload decodes");
    assert_eq!(decoded, reminder());

    // Message-declared delays resolve once at persistence.
    let id = scheduler
        .schedule_delayed(&Later {
            text: "soon".into(),
        })
        .await
        .expect("schedule succeeds");
    assert!(id > 0);

    // Events default to at-most-once; the reliable variant keeps at-least-once.
    let event = Notice {
        text: "ping".into(),
    };
    let event_id = scheduler
        .schedule_event_at(&event, SystemTime::now())
        .await
        .expect("schedule succeeds");
    assert!(event_id > 0);
    scheduler
        .schedule_delayed_event(&event)
        .await
        .expect("schedule succeeds");
    scheduler
        .schedule_delayed_reliable_event(&event)
        .await
        .expect("schedule succeeds");
    let id = scheduler
        .schedule_reliable_event_after(&event, Duration::ZERO)
        .await
        .expect("schedule succeeds");
    let claimed = outbox.claim("worker", 10).await.expect("claim succeeds");
    let event_message = claimed
        .iter()
        .find(|message| message.id() == id)
        .expect("the reliable event is claimable");
    assert_eq!(
        event_message.envelope().metadata().quality_of_service(),
        QualityOfService::AtLeastOnce
    );
}

// ---------------------------------------------------------------------------
// Request client
// ---------------------------------------------------------------------------

#[derive(MemoryPackable, Clone)]
struct Ping {
    value: u64,
}
impl Message for Ping {}
impl Request for Ping {
    type Response = Pong;
}

#[derive(MemoryPackable, Clone, Debug, PartialEq)]
struct Pong {
    text: String,
}
impl Message for Pong {}

struct Stub<F>(F)
where
    F: Fn(Envelope) -> CatgaResult<Envelope> + Send + Sync;

#[async_trait]
impl<F> RequestTransport for Stub<F>
where
    F: Fn(Envelope) -> CatgaResult<Envelope> + Send + Sync,
{
    async fn request(
        &self,
        _destination: &str,
        request: Envelope,
        _timeout: Duration,
    ) -> CatgaResult<Envelope> {
        (self.0)(request)
    }
}

#[tokio::test]
async fn memorypack_request_client_validates_and_round_trips() {
    // The factory and client validate their configuration.
    let transport = Arc::new(Stub(|request: Envelope| {
        MemoryPackCodec::default().typed_success(&request, &Pong { text: "ack".into() })
    }));
    let error = MemoryPackRequestClientFactory::new(Arc::clone(&transport), Duration::ZERO, ids())
        .map(|_| ())
        .expect_err("a zero factory timeout must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    let factory =
        MemoryPackRequestClientFactory::new(Arc::clone(&transport), Duration::from_secs(5), ids())
            .expect("factory builds");
    assert_eq!(factory.default_timeout(), Duration::from_secs(5));
    let error = factory
        .create_to::<Ping>("")
        .map(|_| ())
        .expect_err("an empty destination must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    // A typed request round-trips through the success envelope.
    let client = factory.create::<Ping>().expect("client builds");
    let response = client
        .request_default(&Ping { value: 9 })
        .await
        .expect("request succeeds");
    assert_eq!(response, Pong { text: "ack".into() });

    // The trait entry point shares the same path.
    let client = factory
        .create_to_with_timeout::<Ping>("services.pong", Duration::from_secs(2))
        .expect("client builds");
    assert_eq!(client.destination(), "services.pong");
    let response = catga_core::RequestClient::request(&client, &Ping { value: 1 })
        .await
        .expect("request succeeds");
    assert_eq!(response, Pong { text: "ack".into() });
}

#[tokio::test]
async fn memorypack_request_client_surfaces_failures_and_mismatches() {
    // A remote failure envelope becomes the original typed error.
    let transport = Arc::new(Stub(|request: Envelope| {
        MemoryPackCodec::default().typed_failure(
            &request,
            CatgaError::new(ErrorCode::Unavailable, "downstream"),
        )
    }));
    let factory = MemoryPackRequestClientFactory::new(transport, Duration::from_secs(5), ids())
        .expect("factory builds");
    let client = factory.create::<Ping>().expect("client builds");
    let error = client
        .request_default(&Ping { value: 1 })
        .await
        .expect_err("a remote failure propagates");
    assert_eq!(error.code(), ErrorCode::Unavailable);
    assert_eq!(error.message(), "downstream");

    // A reply correlated to another request is rejected.
    let transport = Arc::new(Stub(|_request: Envelope| {
        Ok(Envelope::new(
            1,
            "Pong",
            vec![0],
            MessageMetadata::new(1, Some(999_999)),
        ))
    }));
    let factory = MemoryPackRequestClientFactory::new(transport, Duration::from_secs(5), ids())
        .expect("factory builds");
    let client = factory.create::<Ping>().expect("client builds");
    let error = client
        .request(&Ping { value: 1 }, Duration::from_secs(1))
        .await
        .expect_err("a mismatched correlation must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    // A zero per-request timeout is rejected before transport work.
    let error = client
        .request(&Ping { value: 1 }, Duration::ZERO)
        .await
        .expect_err("a zero timeout must fail");
    assert_eq!(error.code(), ErrorCode::Validation);
}

// ---------------------------------------------------------------------------
// Bincode
// ---------------------------------------------------------------------------

#[test]
fn bincode_codec_round_trips_and_rejects_bad_frames() {
    let codec = BincodeCodec;

    let value = vec![1_u64, 2, 3, u64::MAX];
    let bytes = codec.encode_payload(&value).expect("encode succeeds");
    let decoded: Vec<u64> = codec.decode_payload(&bytes).expect("decode succeeds");
    assert_eq!(decoded, value);

    let text = "bounded bincode frame".to_string();
    let bytes = codec.encode_payload(&text).expect("encode succeeds");
    let decoded: String = codec.decode_payload(&bytes).expect("decode succeeds");
    assert_eq!(decoded, text);

    // Trailing bytes are rejected instead of silently ignored.
    let mut padded = codec.encode_payload(&7_u8).expect("encode succeeds");
    padded.push(0);
    let error = codec
        .decode_payload(&padded)
        .map(|_: u8| ())
        .expect_err("trailing bytes must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    // Corrupt frames fail as validation errors.
    let error = codec
        .decode_payload(&[0xff])
        .map(|_: Vec<u64>| ())
        .expect_err("corrupt input must fail");
    assert_eq!(error.code(), ErrorCode::Validation);
}
