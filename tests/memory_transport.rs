//! In-memory transport backpressure tests.

use std::time::Duration;

use catga_core::memory::{MemoryPubSubTransport, MemoryTransport};
use catga_core::{
    AsyncInitializable, CatgaResult, Envelope, HealthCheckable, MessageMetadata, MessageTransport,
    QualityOfService, Stoppable, Waitable,
};
use tokio_util::sync::CancellationToken;

fn envelope(id: u64) -> Envelope {
    Envelope::new(
        id,
        "order.created",
        vec![id as u8],
        MessageMetadata::new(id, None),
    )
}

#[test]
fn zero_capacity_returns_a_validation_error() {
    let error = MemoryTransport::new(0).expect_err("zero capacity must be rejected");

    assert_eq!(error.code(), catga_core::ErrorCode::Validation);
    assert_eq!(
        error.message(),
        "memory transport capacity must be greater than zero"
    );
}

/// Local Pub/Sub clones are independent subscribers, unlike the queue transport's work sharing.
#[tokio::test]
async fn memory_pubsub_transport_broadcasts_to_every_subscriber() -> CatgaResult<()> {
    let first = MemoryPubSubTransport::new(4)?;
    let second = first.subscribe();
    first
        .publish(Envelope::new(
            41,
            "order.broadcast",
            vec![4, 1],
            MessageMetadata::new(41, None).with_quality_of_service(QualityOfService::AtMostOnce),
        ))
        .await?;

    let first_delivery = first.receive().await?;
    let second_delivery = second.receive().await?;
    assert_eq!(first_delivery.envelope().payload(), [4, 1]);
    assert_eq!(second_delivery.envelope().payload(), [4, 1]);
    first.ack(first_delivery).await?;
    second.ack(second_delivery).await?;
    assert_eq!(first.pending_operations(), 0);
    Ok(())
}

/// The in-memory broadcast adapter must not present queue guarantees it cannot provide.
#[tokio::test]
async fn memory_pubsub_transport_rejects_durable_qos() -> CatgaResult<()> {
    let transport = MemoryPubSubTransport::new(1)?;
    let Err(error) = transport.publish(envelope(42)).await else {
        return Err(catga_core::CatgaError::new(
            catga_core::ErrorCode::Internal,
            "memory Pub/Sub accepted a durable QoS request",
        ));
    };
    assert_eq!(error.code(), catga_core::ErrorCode::Unsupported);
    Ok(())
}

#[tokio::test]
async fn bounded_transport_applies_backpressure_and_preserves_delivery_order() {
    let transport = MemoryTransport::new(1).unwrap();
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

#[tokio::test]
async fn waitable_tracks_a_received_delivery_until_it_is_acknowledged() {
    let transport = MemoryTransport::new(1).unwrap();
    transport.publish(envelope(1)).await.unwrap();
    let delivery = transport.receive().await.unwrap();

    assert_eq!(transport.pending_operations(), 1);

    let completion = transport.wait_for_completion(CancellationToken::new());
    tokio::pin!(completion);
    assert!(
        tokio::time::timeout(Duration::from_millis(5), &mut completion)
            .await
            .is_err()
    );

    transport.ack(delivery).await.unwrap();
    completion.await.unwrap();
    assert_eq!(transport.pending_operations(), 0);
}

#[tokio::test]
async fn waitable_returns_promptly_when_shutdown_is_cancelled() {
    let transport = MemoryTransport::new(1).unwrap();
    transport.publish(envelope(1)).await.unwrap();
    let _delivery = transport.receive().await.unwrap();
    let cancellation = CancellationToken::new();
    let wait = transport.wait_for_completion(cancellation.clone());
    tokio::pin!(wait);

    cancellation.cancel();
    tokio::time::timeout(Duration::from_millis(100), &mut wait)
        .await
        .expect("a cancelled shutdown must not wait for in-flight work")
        .unwrap();
}

#[tokio::test]
async fn negative_acknowledgement_releases_the_waitable_delivery_slot() {
    let transport = MemoryTransport::new(1).unwrap();
    transport.publish(envelope(1)).await.unwrap();
    let delivery = transport.receive().await.unwrap();

    let error = transport.nack(delivery).await.unwrap_err();
    assert_eq!(error.code(), catga_core::ErrorCode::Unsupported);
    assert_eq!(transport.pending_operations(), 0);
}

#[tokio::test]
async fn dropped_delivery_releases_the_waitable_delivery_slot() {
    let transport = MemoryTransport::new(1).unwrap();
    transport.publish(envelope(1)).await.unwrap();
    let delivery = transport.receive().await.unwrap();

    drop(delivery);

    assert_eq!(transport.pending_operations(), 0);
}

#[tokio::test]
async fn stopped_transport_rejects_new_publications_without_discarding_received_work() {
    let transport = MemoryTransport::new(1).unwrap();
    transport.publish(envelope(1)).await.unwrap();
    let delivery = transport.receive().await.unwrap();

    assert!(transport.is_accepting());
    transport.stop_accepting();
    assert!(!transport.is_accepting());

    let error = transport.publish(envelope(2)).await.unwrap_err();
    assert_eq!(error.code(), catga_core::ErrorCode::Unavailable);

    transport.ack(delivery).await.unwrap();
}

#[tokio::test]
async fn memory_transport_is_initialized_and_reports_its_health() {
    let transport = MemoryTransport::new(1).unwrap();

    transport.initialize().await.unwrap();

    assert!(transport.is_healthy());
    assert_eq!(
        transport.health_status(),
        Some("in-memory transport is ready")
    );
}
