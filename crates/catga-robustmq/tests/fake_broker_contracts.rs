//! Contract tests for the RobustMQ mailbox adapter against an in-process fake broker.
//!
//! The fake broker speaks just enough of the NATS text protocol for the mq9 SDK, so these tests
//! exercise the adapter's real connection, framing, subscription, and request/reply paths
//! deterministically and without a live RobustMQ service.

#[path = "support/fake_broker.rs"]
mod fake_broker;

use std::time::Duration;

use catga_core::codec::memorypack::{
    MemoryPackCodec, MemoryPackDeserialize, MemoryPackError, MemoryPackReader,
    MemoryPackRpcResponse, MemoryPackSerialize, MemoryPackWriter,
};
use catga_core::{
    CatgaError, CatgaResult, Envelope, EnvelopeCodec, ErrorCode, MemoryPackable, Message,
    MessageMetadata, MessagePriority, MessageTypeId, Request, RequestTransport, request_handler,
};
use catga_robustmq::{MailboxClient, MailboxConfig, MailboxPriority, MailboxRequestServer};
use fake_broker::FakeBroker;
use tokio::sync::mpsc;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

struct PingTypeId;
impl MessageTypeId for PingTypeId {
    const NAME: &'static str = "catga.test.Ping";
}

/// Typed request used by the request/reply contracts.
#[derive(Debug, PartialEq, Eq, MemoryPackable)]
struct Ping {
    value: u64,
}
impl Message for Ping {}
impl Request for Ping {
    type Response = u64;
    type TypeId = PingTypeId;
}

struct BulkTypeId;
impl MessageTypeId for BulkTypeId {
    const NAME: &'static str = "catga.test.Bulk";
}

/// Typed request whose response can exceed the codec's bounded frame limit.
#[derive(Debug, PartialEq, Eq, MemoryPackable)]
struct Bulk {
    value: u64,
}
impl Message for Bulk {}
impl Request for Bulk {
    type Response = Vec<u8>;
    type TypeId = BulkTypeId;
}

/// A custom wire format that proves the adapter threads the configured codec through every
/// envelope path instead of hardcoding MemoryPack.
struct PrefixedCodec(MemoryPackCodec);

const CODEC_PREFIX: &[u8] = b"catga.test.codec:";

impl EnvelopeCodec for PrefixedCodec {
    fn encode(&self, envelope: &Envelope) -> CatgaResult<Vec<u8>> {
        let mut bytes = Vec::with_capacity(CODEC_PREFIX.len() + 64);
        bytes.extend_from_slice(CODEC_PREFIX);
        bytes.extend_from_slice(&self.0.encode(envelope)?);
        Ok(bytes)
    }

    fn decode(&self, bytes: &[u8]) -> CatgaResult<Envelope> {
        let payload = bytes
            .strip_prefix(CODEC_PREFIX)
            .ok_or_else(|| CatgaError::new(ErrorCode::Validation, "missing test codec prefix"))?;
        self.0.decode(payload)
    }
}

fn test_envelope(id: u64, message_type: &str, payload: Vec<u8>) -> Envelope {
    Envelope::new(
        id,
        message_type,
        payload,
        MessageMetadata::new(7, Some(99)).with_priority(MessagePriority::High),
    )
}

#[tokio::test]
async fn connect_failure_maps_to_transient_error() -> CatgaResult<()> {
    // Bind and immediately release a port so the connection is actively refused.
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
    let address = listener
        .local_addr()
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
    drop(listener);

    let error = match MailboxClient::connect(&format!("nats://{address}")).await {
        Ok(_) => panic!("connecting to a closed port must fail"),
        Err(error) => error,
    };
    assert_eq!(error.code(), ErrorCode::Transient);
    Ok(())
}

#[tokio::test]
async fn create_mailbox_roundtrips_configured_fields() -> CatgaResult<()> {
    let broker = FakeBroker::start().await;
    let client = MailboxClient::connect(&broker.url()).await?;

    let public = client
        .create(&MailboxConfig {
            server: broker.url().into(),
            ttl_seconds: 600,
            public: true,
            name: "orders".into(),
            description: "order commands".into(),
        })
        .await?;
    assert_eq!(public.mail_id, "mailbox-1");
    assert!(public.public);
    assert_eq!(public.name, "orders");
    assert_eq!(public.desc, "order commands");

    let private = client
        .create(&MailboxConfig {
            server: broker.url().into(),
            ttl_seconds: 60,
            public: false,
            name: "".into(),
            description: "".into(),
        })
        .await?;
    assert_eq!(private.mail_id, "mailbox-2");
    assert!(!private.public);

    let (subject, body) = broker.wait_for_publish("$mq9.AI.MAILBOX.CREATE").await;
    assert_eq!(subject, "$mq9.AI.MAILBOX.CREATE");
    let body = String::from_utf8(body)
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
    assert!(
        body.contains("\"ttl\":600"),
        "create body carries ttl: {body}"
    );

    let creates = broker
        .published()
        .into_iter()
        .filter(|(subject, _)| subject == "$mq9.AI.MAILBOX.CREATE")
        .count();
    assert_eq!(creates, 2);
    Ok(())
}

#[tokio::test]
async fn create_mailbox_control_plane_error_maps_to_transient() -> CatgaResult<()> {
    let broker = FakeBroker::start_failing_create("quota exceeded", 429).await;
    let client = MailboxClient::connect(&broker.url()).await?;

    let error = client
        .create(&MailboxConfig {
            server: broker.url().into(),
            ttl_seconds: 60,
            public: false,
            name: "".into(),
            description: "".into(),
        })
        .await
        .expect_err("a control-plane rejection must surface as an error");
    assert_eq!(error.code(), ErrorCode::Transient);
    assert!(
        error.message().contains("quota exceeded"),
        "the upstream reason must survive translation: {error:?}"
    );
    Ok(())
}

#[tokio::test]
async fn request_to_maps_mailbox_creation_failure_to_transient() -> CatgaResult<()> {
    let broker = FakeBroker::start_failing_create("quota exceeded", 429).await;
    let client = MailboxClient::connect(&broker.url()).await?;

    let error = client
        .request_to(
            "requests",
            test_envelope(1, "catga.test.Ping", Vec::new()),
            TEST_TIMEOUT,
        )
        .await
        .expect_err("a failing reply-mailbox creation must abort the request");
    assert_eq!(error.code(), ErrorCode::Transient);
    Ok(())
}

#[tokio::test]
async fn send_delivers_raw_payload_to_subscribers() -> CatgaResult<()> {
    let broker = FakeBroker::start().await;
    let client = MailboxClient::connect(&broker.url()).await?;

    let (delivered, mut received) = mpsc::unbounded_channel();
    let subscription = client
        .subscribe(
            "orders",
            move |message| {
                let delivered = delivered.clone();
                async move {
                    let _ = delivered.send(message);
                }
            },
            None,
            "workers",
        )
        .await?;
    broker
        .wait_for_subscription("$mq9.AI.MAILBOX.MSG.orders.*")
        .await;

    let envelope = test_envelope(11, "catga.test.raw", vec![9, 8, 7, 6]);
    client
        .send("orders", &envelope, MailboxPriority::High)
        .await?;

    let message = tokio::time::timeout(TEST_TIMEOUT, received.recv())
        .await
        .expect("raw delivery must arrive")
        .expect("subscription channel stays open");
    assert_eq!(message.mail_id, "orders");
    assert_eq!(message.payload, envelope.payload());
    assert_eq!(message.priority, robustmq::Priority::High);

    let (subject, _) = broker.wait_for_publish("$mq9.AI.MAILBOX.MSG.orders.").await;
    assert_eq!(subject, "$mq9.AI.MAILBOX.MSG.orders.high");

    subscription.unsubscribe();
    Ok(())
}

#[tokio::test]
async fn send_envelope_roundtrips_through_subscribe_envelopes() -> CatgaResult<()> {
    let broker = FakeBroker::start().await;
    let client = MailboxClient::connect(&broker.url()).await?;

    let (delivered, mut received) = mpsc::unbounded_channel();
    let subscription = client
        .subscribe_envelopes(
            "events",
            move |decoded| {
                let delivered = delivered.clone();
                async move {
                    let _ = delivered.send(decoded);
                }
            },
            Some(MailboxPriority::Low),
            "",
        )
        .await?;
    broker
        .wait_for_subscription("$mq9.AI.MAILBOX.MSG.events.low")
        .await;

    let original =
        test_envelope(21, "catga.test.Event", vec![1, 2, 3]).with_reply_to("events.replies");
    client
        .send_envelope("events", &original, MailboxPriority::Low)
        .await?;

    let decoded = tokio::time::timeout(TEST_TIMEOUT, received.recv())
        .await
        .expect("envelope delivery must arrive")
        .expect("subscription channel stays open")?;
    assert_eq!(decoded.id(), original.id());
    assert_eq!(decoded.message_type(), original.message_type());
    assert_eq!(decoded.payload(), original.payload());
    assert_eq!(decoded.reply_to(), Some("events.replies"));
    assert_eq!(decoded.metadata().priority(), MessagePriority::High);
    assert_eq!(decoded.metadata().correlation_id(), Some(99));

    subscription.unsubscribe();
    Ok(())
}

#[tokio::test]
async fn subscribe_envelopes_surfaces_decode_errors_without_dying() -> CatgaResult<()> {
    let broker = FakeBroker::start().await;
    let client = MailboxClient::connect(&broker.url()).await?;

    let (delivered, mut received) = mpsc::unbounded_channel();
    let subscription = client
        .subscribe_envelopes(
            "mixed",
            move |decoded| {
                let delivered = delivered.clone();
                async move {
                    let _ = delivered.send(decoded);
                }
            },
            None,
            "",
        )
        .await?;
    broker
        .wait_for_subscription("$mq9.AI.MAILBOX.MSG.mixed.*")
        .await;

    let garbage = test_envelope(31, "catga.test.raw", vec![0xFF, 0xFE, 0xFD, 0x00]);
    client
        .send("mixed", &garbage, MailboxPriority::Normal)
        .await?;
    let malformed = tokio::time::timeout(TEST_TIMEOUT, received.recv())
        .await
        .expect("the malformed frame must reach the callback")
        .expect("subscription channel stays open");
    let error = malformed.expect_err("malformed bytes must surface as a decode error");
    assert_eq!(error.code(), ErrorCode::Validation);

    // The subscription survives malformed frames and keeps delivering.
    let valid = test_envelope(32, "catga.test.Event", vec![1]);
    client
        .send_envelope("mixed", &valid, MailboxPriority::Normal)
        .await?;
    let decoded = tokio::time::timeout(TEST_TIMEOUT, received.recv())
        .await
        .expect("the valid frame must arrive after the malformed one")
        .expect("subscription channel stays open")?;
    assert_eq!(decoded.id(), valid.id());

    subscription.unsubscribe();
    Ok(())
}

#[tokio::test]
async fn request_reply_roundtrip_carries_typed_success() -> CatgaResult<()> {
    let broker = FakeBroker::start().await;
    let server_client = MailboxClient::connect(&broker.url()).await?;
    let requester = MailboxClient::connect(&broker.url()).await?;

    let mut server = MailboxRequestServer::subscribe(server_client, "requests", 8).await?;
    broker
        .wait_for_subscription("$mq9.AI.MAILBOX.MSG.requests.*")
        .await;

    let serving = tokio::spawn(async move {
        let handler = request_handler(|ping: Ping| async move { Ok(ping.value.saturating_mul(2)) });
        server.handle_next::<Ping, _>(&handler).await
    });

    let payload = MemoryPackCodec::default().encode_value(&Ping { value: 21 })?;
    let request = test_envelope(42, "catga.test.Ping", payload);
    let reply = RequestTransport::request(&requester, "requests", request, TEST_TIMEOUT).await?;

    serving
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))??;

    assert_eq!(reply.id(), 42);
    assert_eq!(reply.message_type(), std::any::type_name::<u64>());
    assert_eq!(reply.metadata().message_id(), 7);
    assert_eq!(reply.metadata().correlation_id(), Some(99));
    assert_eq!(reply.metadata().priority(), MessagePriority::High);
    let (marker, body) = reply
        .payload()
        .split_first()
        .expect("a typed success payload carries a marker byte");
    assert_eq!(*marker, 0, "the typed success marker must be zero");
    assert_eq!(MemoryPackCodec::default().decode_value::<u64>(body)?, 42);
    Ok(())
}

#[tokio::test]
async fn handle_next_reports_handler_errors_as_typed_failures() -> CatgaResult<()> {
    let broker = FakeBroker::start().await;
    let server_client = MailboxClient::connect(&broker.url()).await?;
    let requester = MailboxClient::connect(&broker.url()).await?;

    let mut server = MailboxRequestServer::subscribe(server_client, "requests", 8).await?;
    broker
        .wait_for_subscription("$mq9.AI.MAILBOX.MSG.requests.*")
        .await;

    let serving = tokio::spawn(async move {
        let handler = request_handler(|_: Ping| async move {
            Err::<u64, _>(CatgaError::new(ErrorCode::Validation, "ping rejected"))
        });
        server.handle_next::<Ping, _>(&handler).await
    });

    let payload = MemoryPackCodec::default().encode_value(&Ping { value: 1 })?;
    let reply = requester
        .request_to(
            "requests",
            test_envelope(43, "catga.test.Ping", payload),
            TEST_TIMEOUT,
        )
        .await?;

    serving
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))??;

    assert_eq!(reply.id(), 43);
    assert_eq!(reply.message_type(), "catga.rpc.error");
    let failure =
        MemoryPackCodec::default().decode_value::<MemoryPackRpcResponse<()>>(reply.payload())?;
    match failure {
        MemoryPackRpcResponse::Failure(error) => {
            assert_eq!(error.code(), ErrorCode::Validation);
            assert!(error.message().contains("ping rejected"));
        }
        MemoryPackRpcResponse::Success(()) => {
            panic!("a handler error must map to a typed failure")
        }
    }
    Ok(())
}

#[tokio::test]
async fn handle_next_reports_undecodable_requests_as_typed_failures() -> CatgaResult<()> {
    let broker = FakeBroker::start().await;
    let server_client = MailboxClient::connect(&broker.url()).await?;
    let watcher = MailboxClient::connect(&broker.url()).await?;

    let mut server = MailboxRequestServer::subscribe(server_client, "requests", 8).await?;
    broker
        .wait_for_subscription("$mq9.AI.MAILBOX.MSG.requests.*")
        .await;

    let (delivered, mut received) = mpsc::unbounded_channel();
    let watching = watcher
        .subscribe_envelopes(
            "replies",
            move |decoded| {
                let delivered = delivered.clone();
                async move {
                    let _ = delivered.send(decoded);
                }
            },
            None,
            "",
        )
        .await?;
    broker
        .wait_for_subscription("$mq9.AI.MAILBOX.MSG.replies.*")
        .await;

    let serving = tokio::spawn(async move {
        let handler = request_handler(|ping: Ping| async move { Ok(ping.value) });
        server.handle_next::<Ping, _>(&handler).await
    });

    let undecodable =
        test_envelope(44, "catga.test.Ping", vec![0xFF, 0xFE, 0xFD]).with_reply_to("replies");
    watcher
        .send_envelope("requests", &undecodable, MailboxPriority::Normal)
        .await?;

    serving
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))??;

    let reply = tokio::time::timeout(TEST_TIMEOUT, received.recv())
        .await
        .expect("the typed failure must reach the reply mailbox")
        .expect("reply channel stays open")?;
    assert_eq!(reply.id(), 44);
    assert_eq!(reply.message_type(), "catga.rpc.error");
    let failure =
        MemoryPackCodec::default().decode_value::<MemoryPackRpcResponse<()>>(reply.payload())?;
    assert!(matches!(failure, MemoryPackRpcResponse::Failure(_)));

    watching.unsubscribe();
    Ok(())
}

#[tokio::test]
async fn mailbox_request_requires_reply_to_to_respond() -> CatgaResult<()> {
    let broker = FakeBroker::start().await;
    let server_client = MailboxClient::connect(&broker.url()).await?;
    let sender = MailboxClient::connect(&broker.url()).await?;

    let mut server = MailboxRequestServer::subscribe(server_client, "requests", 8).await?;
    broker
        .wait_for_subscription("$mq9.AI.MAILBOX.MSG.requests.*")
        .await;

    // A request without reply_to can be inspected and decoded but not answered.
    let payload = MemoryPackCodec::default().encode_value(&7u64)?;
    let orphan = test_envelope(51, "catga.test.orphan", payload);
    sender
        .send_envelope("requests", &orphan, MailboxPriority::Normal)
        .await?;

    let request = tokio::time::timeout(TEST_TIMEOUT, server.next())
        .await
        .expect("the orphan request must arrive")?;
    assert_eq!(request.envelope().id(), 51);
    assert_eq!(request.envelope().message_type(), "catga.test.orphan");
    assert_eq!(request.decode::<u64>()?, 7);
    assert!(request.envelope().reply_to().is_none());

    let error = request
        .respond(test_envelope(51, "catga.test.answer", Vec::new()))
        .await
        .expect_err("responding without reply_to must be rejected");
    assert_eq!(error.code(), ErrorCode::Validation);

    // A request with reply_to supports direct typed-error responses and reports decode failures.
    let garbage = test_envelope(52, "catga.test.broken", vec![0xFF, 0xFE]).with_reply_to("replies");
    sender
        .send_envelope("requests", &garbage, MailboxPriority::Critical)
        .await?;

    let request = tokio::time::timeout(TEST_TIMEOUT, server.next())
        .await
        .expect("the broken request must arrive")?;
    assert!(request.decode::<u64>().is_err());
    request
        .respond_error(CatgaError::new(ErrorCode::Validation, "cannot decode"))
        .await?;

    let (subject, _) = broker
        .wait_for_publish("$mq9.AI.MAILBOX.MSG.replies.")
        .await;
    assert_eq!(subject, "$mq9.AI.MAILBOX.MSG.replies.high");
    Ok(())
}

#[tokio::test]
async fn request_to_times_out_when_no_responder_answers() -> CatgaResult<()> {
    let broker = FakeBroker::start().await;
    let client = MailboxClient::connect(&broker.url()).await?;

    let error = client
        .request_to(
            "unanswered",
            test_envelope(61, "catga.test.Ping", Vec::new()),
            Duration::from_millis(30),
        )
        .await
        .expect_err("an unanswered request must time out");
    assert_eq!(error.code(), ErrorCode::Timeout);
    Ok(())
}

#[tokio::test]
async fn custom_codec_roundtrips_through_envelope_paths() -> CatgaResult<()> {
    let broker = FakeBroker::start().await;
    let client =
        MailboxClient::connect_with_codec(&broker.url(), PrefixedCodec(MemoryPackCodec::default()))
            .await?;

    let (delivered, mut received) = mpsc::unbounded_channel();
    let subscription = client
        .subscribe_envelopes(
            "custom",
            move |decoded| {
                let delivered = delivered.clone();
                async move {
                    let _ = delivered.send(decoded);
                }
            },
            None,
            "",
        )
        .await?;
    broker
        .wait_for_subscription("$mq9.AI.MAILBOX.MSG.custom.*")
        .await;

    let original = test_envelope(71, "catga.test.Custom", vec![4, 2]);
    client
        .send_envelope("custom", &original, MailboxPriority::Normal)
        .await?;

    // The wire bytes prove the custom codec, not MemoryPack, framed the envelope.
    let (_, wire) = broker.wait_for_publish("$mq9.AI.MAILBOX.MSG.custom.").await;
    assert!(wire.starts_with(CODEC_PREFIX));

    let decoded = tokio::time::timeout(TEST_TIMEOUT, received.recv())
        .await
        .expect("custom-codec delivery must arrive")
        .expect("subscription channel stays open")?;
    assert_eq!(decoded.id(), original.id());
    assert_eq!(decoded.payload(), original.payload());

    subscription.unsubscribe();
    Ok(())
}

#[tokio::test]
async fn oversize_envelopes_fail_encoding_before_any_publish() -> CatgaResult<()> {
    let broker = FakeBroker::start().await;
    let client = MailboxClient::connect(&broker.url()).await?;
    let huge_payload = vec![7u8; 1_100_000];

    // send_envelope refuses oversize frames locally.
    let huge = test_envelope(81, "catga.test.Huge", huge_payload.clone());
    let error = client
        .send_envelope("limited", &huge, MailboxPriority::Normal)
        .await
        .expect_err("an oversize envelope must fail encoding");
    assert_eq!(error.code(), ErrorCode::Validation);

    // request_to refuses oversize frames after preparing the reply route.
    let error = client
        .request_to("requests", huge.clone(), TEST_TIMEOUT)
        .await
        .expect_err("an oversize request must fail encoding");
    assert_eq!(error.code(), ErrorCode::Validation);

    // Neither refusal may leak a frame onto the wire.
    assert!(
        broker.published().iter().all(|(subject, _)| {
            !subject.starts_with("$mq9.AI.MAILBOX.MSG.limited")
                && !subject.starts_with("$mq9.AI.MAILBOX.MSG.requests")
        }),
        "oversize frames must never be published"
    );

    // respond refuses oversize envelopes for a delivered request.
    let mut server = MailboxRequestServer::subscribe(client.clone(), "requests", 8).await?;
    broker
        .wait_for_subscription("$mq9.AI.MAILBOX.MSG.requests.*")
        .await;

    let normal = test_envelope(82, "catga.test.Ping", Vec::new()).with_reply_to("replies");
    client
        .send_envelope("requests", &normal, MailboxPriority::Normal)
        .await?;
    let request = tokio::time::timeout(TEST_TIMEOUT, server.next())
        .await
        .expect("the normal request must arrive")?;
    let error = request
        .respond(test_envelope(82, "catga.test.Huge", huge_payload))
        .await
        .expect_err("an oversize response must fail encoding");
    assert_eq!(error.code(), ErrorCode::Validation);

    // handle_next surfaces oversize typed responses from respond_value.
    let mut server = MailboxRequestServer::subscribe(client.clone(), "bulk", 8).await?;
    broker
        .wait_for_subscription("$mq9.AI.MAILBOX.MSG.bulk.*")
        .await;
    let serving = tokio::spawn(async move {
        let handler = request_handler(|_: Bulk| async move { Ok(vec![1u8; 1_100_000]) });
        server.handle_next::<Bulk, _>(&handler).await
    });
    let payload = MemoryPackCodec::default().encode_value(&Bulk { value: 0 })?;
    let bulk_request = test_envelope(83, "catga.test.Bulk", payload).with_reply_to("replies");
    client
        .send_envelope("bulk", &bulk_request, MailboxPriority::Normal)
        .await?;
    let error = serving
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?
        .expect_err("an oversize typed response must fail the handler turn");
    assert_eq!(error.code(), ErrorCode::Validation);
    Ok(())
}
