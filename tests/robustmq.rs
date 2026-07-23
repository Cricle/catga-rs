//! RobustMQ mailbox adapter tests.

use std::{sync::Arc, time::Duration};

use catga_core::{Envelope, MessageMetadata};
use catga_robustmq::{MailboxClient, MailboxPriority};
use robustmq::Priority;

#[test]
fn mailbox_priority_maps_without_protocol_leakage() {
    assert_eq!(MailboxPriority::Critical.as_sdk(), Priority::High);
    assert_eq!(MailboxPriority::Normal.as_sdk(), Priority::Normal);
    assert_eq!(MailboxPriority::Low.as_sdk(), Priority::Low);
}

#[tokio::test]
async fn mailbox_envelope_delivery_preserves_catga_metadata() {
    let Some(server) = std::env::var("CATGA_NATS_URL").ok() else {
        eprintln!("skipping RobustMQ integration test: CATGA_NATS_URL is unset");
        return;
    };
    let client = MailboxClient::connect(&server).await.unwrap();
    let mailbox = format!("catga-robustmq-{}", std::process::id());
    let delivered = Arc::new(std::sync::Mutex::new(None));
    let received = Arc::clone(&delivered);
    let subscription = client
        .subscribe_envelopes(
            &mailbox,
            move |envelope| {
                let received = Arc::clone(&received);
                async move {
                    *received.lock().unwrap() = Some(envelope.unwrap());
                }
            },
            Some(MailboxPriority::Critical),
            "",
        )
        .await
        .unwrap();
    let envelope = Envelope::versioned(
        42,
        "order.created",
        vec![1, 2, 3],
        MessageMetadata::new(8, Some(7)),
        3,
    );
    client
        .send_envelope(&mailbox, &envelope, MailboxPriority::Critical)
        .await
        .unwrap();
    let delivered = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Some(envelope) = delivered.lock().unwrap().take() {
                return envelope;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("mailbox delivers the envelope");
    subscription.unsubscribe();
    assert_eq!(delivered, envelope);
}
