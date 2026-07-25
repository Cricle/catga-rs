//! RobustMQ mailbox adapter tests.

use std::time::Duration;

use catga_core::{CatgaError, CatgaResult, Envelope, ErrorCode, MessageMetadata, MessagePriority};
use catga_robustmq::{MailboxClient, MailboxPriority, MailboxRequestServer};
use robustmq::Priority;

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
#[ignore = "requires CATGA_ROBUSTMQ_URL"]
async fn mailbox_request_server_replies_through_the_private_reply_mailbox() -> CatgaResult<()> {
    let server = std::env::var("CATGA_ROBUSTMQ_URL")
        .expect("CATGA_ROBUSTMQ_URL must be set for ignored RobustMQ tests");
    let client = MailboxClient::connect(&server).await?;
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
    let received = request_server.next().await?;
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
    Ok(())
}
