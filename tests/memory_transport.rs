//! In-memory transport backpressure tests.

use std::time::Duration;

use catga_core::{Envelope, MessageMetadata, MessageTransport};
use catga_memory::MemoryTransport;

fn envelope(id: u64) -> Envelope {
    Envelope::new(
        id,
        "order.created",
        vec![id as u8],
        MessageMetadata::new(id, None),
    )
}

#[tokio::test]
async fn bounded_transport_applies_backpressure_and_preserves_delivery_order() {
    let transport = MemoryTransport::new(1);
    transport.publish(envelope(1)).await.unwrap();

    let pending = {
        let transport = transport.clone();
        tokio::spawn(async move { transport.publish(envelope(2)).await })
    };
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(!pending.is_finished());

    let first = transport.receive().await.unwrap();
    assert_eq!(first.envelope().id(), 1);
    transport.ack(first).await.unwrap();
    assert!(pending.await.unwrap().is_ok());

    let second = transport.receive().await.unwrap();
    assert_eq!(second.envelope().id(), 2);
    transport.ack(second).await.unwrap();
}
