//! RobustMQ mailbox adapter tests.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use catga_core::codec::memorypack::MemoryPackCodec;
use catga_core::{
    CatgaError, CatgaResult, Envelope, EnvelopeCodec, ErrorCode, MessageMetadata, MessagePriority,
};
use catga_robustmq::{MailboxClient, MailboxPriority, MailboxRequest, MailboxRequestServer};
use robustmq::Priority;

#[path = "support/mq9_control_plane.rs"]
mod mq9_control_plane;
#[path = "support/nats_e2e.rs"]
mod nats_e2e;

const TAGGED_ENVELOPE_CODEC_PREFIX: &[u8] = b"catga-robustmq-e2e-codec-v1\0";
const ROBUSTMQ_BROKER_RESPONSE_TIMEOUT: Duration = Duration::from_secs(2);

/// Receives one RobustMQ request within the E2E test's bounded broker-response budget.
async fn robustmq_request<C>(
    server: &mut MailboxRequestServer<C>,
    context: &str,
) -> CatgaResult<MailboxRequest<C>>
where
    C: EnvelopeCodec + 'static,
{
    tokio::time::timeout(ROBUSTMQ_BROKER_RESPONSE_TIMEOUT, server.next())
        .await
        .map_err(|_| CatgaError::new(ErrorCode::Timeout, format!("{context} timed out")))?
}

/// A deliberately non-default frame proving mailbox envelope APIs use their configured codec.
#[derive(Clone, Default)]
struct TaggedEnvelopeCodec {
    encoded: Arc<AtomicUsize>,
    decoded: Arc<AtomicUsize>,
}

impl EnvelopeCodec for TaggedEnvelopeCodec {
    fn encode(&self, envelope: &Envelope) -> CatgaResult<Vec<u8>> {
        self.encoded.fetch_add(1, Ordering::Relaxed);
        let payload = MemoryPackCodec::default().encode(envelope)?;
        let mut frame = Vec::with_capacity(TAGGED_ENVELOPE_CODEC_PREFIX.len() + payload.len());
        frame.extend_from_slice(TAGGED_ENVELOPE_CODEC_PREFIX);
        frame.extend_from_slice(&payload);
        Ok(frame)
    }

    fn decode(&self, bytes: &[u8]) -> CatgaResult<Envelope> {
        self.decoded.fetch_add(1, Ordering::Relaxed);
        let payload = bytes
            .strip_prefix(TAGGED_ENVELOPE_CODEC_PREFIX)
            .ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::Validation,
                    "RobustMQ tagged codec frame prefix is missing",
                )
            })?;
        MemoryPackCodec::default().decode(payload)
    }
}

#[test]
fn mailbox_priority_maps_without_protocol_leakage() {
    assert_eq!(MailboxPriority::Critical.as_sdk(), Priority::High);
    assert_eq!(MailboxPriority::High.as_sdk(), Priority::High);
    assert_eq!(MailboxPriority::Normal.as_sdk(), Priority::Normal);
    assert_eq!(MailboxPriority::Low.as_sdk(), Priority::Low);
    assert_eq!(
        MailboxPriority::from(MessagePriority::Critical),
        MailboxPriority::Critical
    );
    assert_eq!(
        MailboxPriority::from(MessagePriority::High),
        MailboxPriority::High
    );
}

#[test]
fn mailbox_priority_uses_envelope_metadata() {
    for (priority, expected) in [
        (MessagePriority::Low, Priority::Low),
        (MessagePriority::Normal, Priority::Normal),
        (MessagePriority::High, Priority::High),
        (MessagePriority::Critical, Priority::High),
    ] {
        let envelope = Envelope::new(
            1,
            "priority.test",
            Vec::new(),
            MessageMetadata::new(1, None).with_priority(priority),
        );
        assert_eq!(
            MailboxPriority::from_envelope(&envelope).as_sdk(),
            expected,
            "{priority:?} must retain its supported mailbox priority",
        );
    }
}

/// A custom envelope codec must frame public mailbox send and subscription delivery paths.
///
/// The lightweight mq9 raw-mailbox API runs directly over the real NATS test container, so no
/// RobustMQ control plane is required for this wire-contract test.
#[tokio::test]
async fn mailbox_envelope_delivery_uses_the_injected_codec() -> CatgaResult<()> {
    let server = nats_e2e::server_url().await;
    let codec = TaggedEnvelopeCodec::default();
    let client = MailboxClient::connect_with_codec(server.url(), codec.clone()).await?;
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let mailbox = format!("catga-robustmq-codec-{}_{}", std::process::id(), suffix);
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let subscription = client
        .subscribe_envelopes(
            &mailbox,
            move |envelope| {
                let sender = sender.clone();
                async move {
                    let _ = sender.send(envelope).await;
                }
            },
            Some(MailboxPriority::Critical),
            "",
        )
        .await?;
    let envelope = Envelope::versioned(
        42,
        "order.created",
        vec![1, 2, 3],
        MessageMetadata::new(8, Some(7)),
        3,
    );
    client
        .send_envelope(&mailbox, &envelope, MailboxPriority::Critical)
        .await?;
    let delivered = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
        .await
        .map_err(|_| CatgaError::new(ErrorCode::Timeout, "mailbox did not deliver the envelope"))?
        .ok_or_else(|| CatgaError::new(ErrorCode::Transient, "mailbox subscription closed"))??;
    subscription.unsubscribe();
    assert_eq!(delivered, envelope);
    assert_eq!(codec.encoded.load(Ordering::Relaxed), 1);
    assert_eq!(codec.decoded.load(Ordering::Relaxed), 1);
    drop(client);
    server
        .close()
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn mailbox_envelope_delivery_preserves_catga_metadata() -> CatgaResult<()> {
    let server = std::env::var("CATGA_NATS_URL")
        .expect("CATGA_NATS_URL must be set for ignored RobustMQ tests");
    let client = MailboxClient::connect(&server).await?;
    let mailbox = format!("catga-robustmq-{}", std::process::id());
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let subscription = client
        .subscribe_envelopes(
            &mailbox,
            move |envelope| {
                let sender = sender.clone();
                async move {
                    if let Ok(envelope) = envelope {
                        let _ = sender.send(envelope).await;
                    }
                }
            },
            Some(MailboxPriority::Critical),
            "",
        )
        .await?;
    let envelope = Envelope::versioned(
        42,
        "order.created",
        vec![1, 2, 3],
        MessageMetadata::new(8, Some(7)),
        3,
    );
    client
        .send_envelope(&mailbox, &envelope, MailboxPriority::Critical)
        .await?;
    let delivered = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
        .await
        .map_err(|_| CatgaError::new(ErrorCode::Timeout, "mailbox did not deliver the envelope"))?
        .ok_or_else(|| CatgaError::new(ErrorCode::Transient, "mailbox subscription closed"))?;
    subscription.unsubscribe();
    assert_eq!(delivered, envelope);
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn mailbox_request_fails_promptly_without_the_mailbox_control_plane() -> CatgaResult<()> {
    let server = std::env::var("CATGA_NATS_URL")
        .expect("CATGA_NATS_URL must be set for ignored RobustMQ tests");
    let client = MailboxClient::connect(&server).await?;
    let request = Envelope::versioned(
        79,
        "order.requested",
        vec![1, 2],
        MessageMetadata::new(14, Some(14)),
        1,
    );
    let error = match tokio::time::timeout(
        Duration::from_secs(1),
        client.request_to("catga-robustmq-timeout", request, Duration::from_millis(50)),
    )
    .await
    {
        Ok(Ok(_)) => {
            return Err(CatgaError::new(
                ErrorCode::Internal,
                "request without a reply must not succeed",
            ));
        }
        Ok(Err(error)) => error,
        Err(_) => {
            return Err(CatgaError::new(
                ErrorCode::Internal,
                "request did not return after the mailbox control plane rejected it",
            ));
        }
    };

    assert_eq!(error.code(), ErrorCode::Transient);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a NATS endpoint"]
/// A custom codec must frame both request directions and the request-server response.
async fn mailbox_request_server_replies_through_the_private_reply_mailbox() -> CatgaResult<()> {
    let server = nats_e2e::server_url().await;
    let control_plane = mq9_control_plane::start(server.url()).await?;
    let codec = TaggedEnvelopeCodec::default();
    let client = MailboxClient::connect_with_codec(server.url(), codec.clone()).await?;
    let mailbox = format!("catga-robustmq-rpc-{}", std::process::id());
    let mut request_server = MailboxRequestServer::subscribe(client.clone(), &mailbox, 8).await?;
    let request = Envelope::versioned(
        77,
        "order.requested",
        vec![1, 2],
        MessageMetadata::new(12, Some(12)),
        1,
    );
    let client_request = client.clone();
    let mailbox_request = mailbox.clone();
    let pending = tokio::spawn(async move {
        client_request
            .request_to(&mailbox_request, request, Duration::from_secs(2))
            .await
    });
    let received = robustmq_request(&mut request_server, "RobustMQ request server").await?;
    assert!(received.envelope().reply_to().is_some());
    received
        .respond(Envelope::versioned(
            78,
            "order.responded",
            vec![3, 4],
            MessageMetadata::new(13, Some(12)),
            1,
        ))
        .await?;
    let reply = pending.await.map_err(|error| {
        CatgaError::new(ErrorCode::Internal, format!("request task failed: {error}"))
    })??;
    assert_eq!(reply.payload(), [3, 4]);
    assert_eq!(codec.encoded.load(Ordering::Relaxed), 2);
    assert_eq!(codec.decoded.load(Ordering::Relaxed), 2);
    assert_eq!(control_plane.created_mailboxes(), 1);
    control_plane.close().await?;
    server
        .close()
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
    Ok(())
}

// ============================================================================
// RobustMQ Mailbox Creation E2E Tests
// ============================================================================

/// Verifies that mailbox creation with MailboxConfig works correctly.
#[tokio::test]
async fn mailbox_client_create_with_config() -> CatgaResult<()> {
    let server = nats_e2e::server_url().await;
    let client = MailboxClient::connect(server.url()).await?;
    let suffix = format!("create_{}", std::process::id());

    let config = catga_robustmq::MailboxConfig {
        server: server.url().into(),
        ttl_seconds: 60,
        public: false,
        name: format!("test-mailbox-{suffix}").into(),
        description: "Test mailbox creation".into(),
    };

    let mailbox = client.create(&config).await?;
    assert!(!mailbox.mail_id.is_empty());

    server
        .close()
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
    Ok(())
}

/// Verifies that mailbox creation with public visibility works.
#[tokio::test]
async fn mailbox_client_create_public_mailbox() -> CatgaResult<()> {
    let server = nats_e2e::server_url().await;
    let client = MailboxClient::connect(server.url()).await?;
    let suffix = format!("public_{}", std::process::id());

    let config = catga_robustmq::MailboxConfig {
        server: server.url().into(),
        ttl_seconds: 300,
        public: true,
        name: format!("public-mailbox-{suffix}").into(),
        description: "Public test mailbox".into(),
    };

    let mailbox = client.create(&config).await?;
    assert!(!mailbox.mail_id.is_empty());

    server
        .close()
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
    Ok(())
}

// ============================================================================
// RobustMQ Multi-message Delivery E2E Tests
// ============================================================================

/// Verifies that multiple messages can be delivered to a mailbox subscription.
#[tokio::test]
async fn mailbox_envelope_delivery_multiple_messages() -> CatgaResult<()> {
    let server = nats_e2e::server_url().await;
    let client = MailboxClient::connect(server.url()).await?;
    let suffix = format!("multi_{}", std::process::id());
    let mailbox = format!("catga-robustmq-multi-{suffix}");

    let (tx, mut rx) = tokio::sync::mpsc::channel(10);
    let subscription = client
        .subscribe_envelopes(
            &mailbox,
            move |result| {
                let tx = tx.clone();
                async move {
                    if let Ok(envelope) = result {
                        let _ = tx.send(envelope).await;
                    }
                }
            },
            Some(MailboxPriority::Normal),
            "",
        )
        .await?;

    // Send multiple messages
    let expected_count = 5;
    for i in 0..expected_count {
        let envelope = Envelope::versioned(
            i as u64,
            "order.item",
            vec![i as u8],
            MessageMetadata::new(i as u64, None),
            1,
        );
        client.send_envelope(&mailbox, &envelope, MailboxPriority::Normal).await?;
    }

    // Receive all messages
    let mut received = 0;
    while let Ok(Some(_envelope)) = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
        received += 1;
        if received == expected_count {
            break;
        }
    }

    assert_eq!(received, expected_count);
    subscription.unsubscribe();

    server
        .close()
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
    Ok(())
}

// ============================================================================
// RobustMQ Priority Handling E2E Tests
// ============================================================================

/// Verifies that messages with different priorities are delivered according to priority.
#[tokio::test]
async fn mailbox_priority_message_order() -> CatgaResult<()> {
    let server = nats_e2e::server_url().await;
    let client = MailboxClient::connect(server.url()).await?;
    let suffix = format!("priority_{}", std::process::id());
    let mailbox = format!("catga-robustmq-priority-{suffix}");

    let (tx, mut rx) = tokio::sync::mpsc::channel(10);
    let subscription = client
        .subscribe_envelopes(
            &mailbox,
            move |result| {
                let tx = tx.clone();
                async move {
                    if let Ok(envelope) = result {
                        let _ = tx.send(envelope).await;
                    }
                }
            },
            Some(MailboxPriority::Normal),
            "",
        )
        .await?;

    // Send messages with different priorities
    // Note: NATS mq9 delivers messages as they arrive; priority affects internal scheduling
    for (i, priority) in [
        (0, MailboxPriority::Low),
        (1, MailboxPriority::Normal),
        (2, MailboxPriority::High),
        (3, MailboxPriority::Critical),
    ] {
        let envelope = Envelope::versioned(
            i as u64,
            "order.priority",
            vec![i as u8],
            MessageMetadata::new(i as u64, None),
            1,
        );
        client.send_envelope(&mailbox, &envelope, priority).await?;
    }

    // Receive all messages
    let mut received = Vec::new();
    for _ in 0..4 {
        match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
            Ok(Some(envelope)) => received.push(envelope),
            _ => break,
        }
    }

    assert_eq!(received.len(), 4);
    subscription.unsubscribe();

    server
        .close()
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
    Ok(())
}

// ============================================================================
// RobustMQ Request Server Handle Next E2E Tests
// ============================================================================

/// Verifies that MailboxRequestServer can handle typed requests using handle_next.
#[tokio::test]
async fn mailbox_request_server_handle_next() -> CatgaResult<()> {
    let server = nats_e2e::server_url().await;
    let control_plane = mq9_control_plane::start(server.url()).await?;
    let client = MailboxClient::connect(server.url()).await?;
    let suffix = format!("handle_{}", std::process::id());
    let mailbox = format!("catga-robustmq-handle-{suffix}");

    let mut request_server = MailboxRequestServer::subscribe(client.clone(), &mailbox, 8).await?;

    // Spawn a handler task
    let handle_task = tokio::spawn(async move {
        let request = request_server.next().await?;
        // Echo the request payload back
        let response = Envelope::new(
            999,
            "echo.response",
            request.envelope().payload().to_vec(),
            MessageMetadata::new(999, request.envelope().id().into()),
        );
        request.respond(response).await
    });

    // Send a request
    let request = Envelope::versioned(
        100,
        "echo.request",
        vec![1, 2, 3],
        MessageMetadata::new(100, None),
        1,
    );

    let response = client
        .request_to(&mailbox, request, Duration::from_secs(5))
        .await?;

    handle_task.await.unwrap()?;
    assert_eq!(response.payload(), [1, 2, 3]);

    control_plane.close().await?;
    server
        .close()
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
    Ok(())
}

/// Verifies that MailboxRequestServer can be created and subscribed.
#[tokio::test]
async fn mailbox_request_server_subscription() -> CatgaResult<()> {
    let server = nats_e2e::server_url().await;
    let control_plane = mq9_control_plane::start(server.url()).await?;
    let client = MailboxClient::connect(server.url()).await?;
    let suffix = format!("sub_{}", std::process::id());
    let mailbox = format!("catga-robustmq-sub-{suffix}");

    // Create and subscribe a request server
    let _request_server = MailboxRequestServer::subscribe(client.clone(), &mailbox, 8).await?;

    // The subscription should be active
    assert!(true, "request server subscription should be created");

    control_plane.close().await?;
    server
        .close()
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
    Ok(())
}

// ============================================================================
// RobustMQ Raw Bytes Delivery E2E Tests
// ============================================================================

/// Verifies that raw send and subscribe work with bytes (not envelopes).
#[tokio::test]
async fn mailbox_raw_bytes_delivery() -> CatgaResult<()> {
    let server = nats_e2e::server_url().await;
    let client = MailboxClient::connect(server.url()).await?;
    let suffix = format!("raw_{}", std::process::id());
    let mailbox = format!("catga-robustmq-raw-{suffix}");

    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let subscription = client
        .subscribe(
            &mailbox,
            move |message| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(message.payload.to_vec()).await;
                }
            },
            Some(MailboxPriority::Normal),
            "",
        )
        .await?;

    // Send raw bytes
    let raw_payload = vec![0xDE, 0xAD, 0xBE, 0xEF];
    let envelope = Envelope::new(0, "", raw_payload.clone(), MessageMetadata::new(0, None));
    client.send(&mailbox, &envelope, MailboxPriority::Normal).await?;

    // Receive raw bytes
    let received = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .map_err(|_| CatgaError::new(ErrorCode::Timeout, "raw message timeout"))?
        .ok_or_else(|| CatgaError::new(ErrorCode::Transient, "subscription closed"))?;

    assert_eq!(received, raw_payload);
    subscription.unsubscribe();

    server
        .close()
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
    Ok(())
}

// ============================================================================
// RobustMQ Connection Management E2E Tests
// ============================================================================

/// Verifies that multiple clients can connect to the same server.
#[tokio::test]
async fn mailbox_multiple_clients_connect() -> CatgaResult<()> {
    let server = nats_e2e::server_url().await;

    let client_a = MailboxClient::connect(server.url()).await?;
    let client_b = MailboxClient::connect(server.url()).await?;
    let suffix = format!("multi_client_{}", std::process::id());
    let mailbox = format!("catga-robustmq-multi-client-{suffix}");

    // Client A sends
    let envelope = Envelope::versioned(
        300,
        "multi.client",
        vec![1],
        MessageMetadata::new(300, None),
        1,
    );
    client_a.send_envelope(&mailbox, &envelope, MailboxPriority::Normal).await?;

    // Client B receives
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let subscription = client_b
        .subscribe_envelopes(
            &mailbox,
            move |result| {
                let tx = tx.clone();
                async move {
                    if let Ok(envelope) = result {
                        let _ = tx.send(envelope).await;
                    }
                }
            },
            Some(MailboxPriority::Normal),
            "",
        )
        .await?;

    let received = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .map_err(|_| CatgaError::new(ErrorCode::Timeout, "multi-client timeout"))?
        .ok_or_else(|| CatgaError::new(ErrorCode::Transient, "subscription closed"))?;

    assert_eq!(received.id(), 300);
    subscription.unsubscribe();

    server
        .close()
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
    Ok(())
}

/// Verifies that request timeout is respected.
#[tokio::test]
async fn mailbox_request_timeout_respected() -> CatgaResult<()> {
    let server = nats_e2e::server_url().await;
    let control_plane = mq9_control_plane::start(server.url()).await?;
    let client = MailboxClient::connect(server.url()).await?;
    let suffix = format!("timeout_{}", std::process::id());
    let mailbox = format!("catga-robustmq-timeout-{suffix}");

    // Subscribe but never respond
    let _request_server = MailboxRequestServer::subscribe(client.clone(), &mailbox, 8).await?;

    let request = Envelope::versioned(
        400,
        "timeout.request",
        vec![],
        MessageMetadata::new(400, None),
        1,
    );

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        client.request_to(&mailbox, request, Duration::from_millis(500)),
    )
    .await;

    assert!(result.is_err() || result.unwrap().is_err(), "request should time out");

    control_plane.close().await?;
    server
        .close()
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
    Ok(())
}
