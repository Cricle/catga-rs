//! Strict contract tests for the process-local transports: the bounded FIFO
//! queue with acknowledgement tracking and the ephemeral AtMostOnce broadcast
//! pub/sub with per-subscriber cursors.

use catga_core::memory::{MemoryPubSubTransport, MemoryTransport};
use catga_core::{
    AsyncInitializable, Destination, DestinationTransport, Envelope, ErrorCode, HealthCheckable,
    MessageMetadata, MessageTransport, QualityOfService, Stoppable, Waitable,
};
use tokio_util::sync::CancellationToken;

fn envelope(id: u64) -> Envelope {
    Envelope::new(id, "Tick", vec![id as u8], MessageMetadata::new(id, None))
}

fn fire_and_forget(id: u64) -> Envelope {
    Envelope::new(
        id,
        "Tick",
        vec![id as u8],
        MessageMetadata::new(id, None).with_quality_of_service(QualityOfService::AtMostOnce),
    )
}

// ---------------------------------------------------------------------------
// MemoryTransport
// ---------------------------------------------------------------------------

#[tokio::test]
async fn memory_transport_queues_fifo_with_tracked_acknowledgements() {
    let error = MemoryTransport::new(0)
        .map(|_| ())
        .expect_err("zero capacity must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    let transport = MemoryTransport::new(4).expect("transport builds");
    transport.initialize().await.expect("initialize succeeds");
    assert!(transport.is_healthy());
    assert!(transport.health_status().is_some());
    assert!(transport.is_accepting());

    transport
        .publish(envelope(1))
        .await
        .expect("publish succeeds");
    transport
        .publish(envelope(2))
        .await
        .expect("publish succeeds");

    // Received deliveries hold an in-flight slot until acknowledged.
    let first = transport.receive().await.expect("receive succeeds");
    assert_eq!(first.envelope().id(), 1);
    assert_eq!(transport.pending_operations(), 1);
    transport.ack(first).await.expect("ack succeeds");
    assert_eq!(transport.pending_operations(), 0);

    // Negative acknowledgement is unsupported but still releases the slot.
    let second = transport.receive().await.expect("receive succeeds");
    assert_eq!(second.envelope().id(), 2);
    assert_eq!(transport.pending_operations(), 1);
    let error = second
        .negative_acknowledge()
        .await
        .expect_err("memory transport refuses negative acknowledgement");
    assert_eq!(error.code(), ErrorCode::Unsupported);
    assert_eq!(transport.pending_operations(), 0);

    // A drained transport completes immediately.
    transport
        .wait_for_completion(CancellationToken::new())
        .await
        .expect("drain succeeds");

    // A stopped transport refuses new publications.
    transport.stop_accepting();
    assert!(!transport.is_accepting());
    let error = transport
        .publish(envelope(3))
        .await
        .expect_err("a stopped transport refuses publishes");
    assert_eq!(error.code(), ErrorCode::Unavailable);
}

#[tokio::test]
async fn memory_transport_routes_declared_destinations() {
    let transport = MemoryTransport::new(4).expect("transport builds");
    let orders = Destination::parse("orders").expect("destination parses");

    // Undeclared destinations report not-found on both directions.
    let error = transport
        .send_to(&orders, envelope(1))
        .await
        .expect_err("an undeclared destination is not found");
    assert_eq!(error.code(), ErrorCode::NotFound);
    let error = transport
        .receive_from(&orders)
        .await
        .expect_err("an undeclared destination is not found");
    assert_eq!(error.code(), ErrorCode::NotFound);

    transport
        .declare_destination(orders.clone())
        .expect("declaration succeeds");
    let error = transport
        .declare_destination(orders.clone())
        .expect_err("a duplicate declaration conflicts");
    assert_eq!(error.code(), ErrorCode::Conflict);

    // The trait forwarder declares through the same path.
    let refunds = Destination::parse("refunds").expect("destination parses");
    DestinationTransport::declare_destination(&transport, &refunds).expect("declaration succeeds");

    transport
        .send_to(&orders, envelope(7))
        .await
        .expect("send succeeds");
    let delivery = transport
        .receive_from(&orders)
        .await
        .expect("receive succeeds");
    assert_eq!(delivery.envelope().id(), 7);
    assert_eq!(transport.pending_operations(), 1);
    delivery.acknowledge().await.expect("ack succeeds");
    assert_eq!(transport.pending_operations(), 0);

    // A stopped transport refuses destination sends as well.
    transport.stop_accepting();
    let error = transport
        .send_to(&orders, envelope(8))
        .await
        .expect_err("a stopped transport refuses sends");
    assert_eq!(error.code(), ErrorCode::Unavailable);
}

// ---------------------------------------------------------------------------
// MemoryPubSubTransport
// ---------------------------------------------------------------------------

#[tokio::test]
async fn memory_pubsub_broadcasts_to_every_subscriber_cursor() {
    let error = MemoryPubSubTransport::new(0)
        .map(|_| ())
        .expect_err("zero capacity must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    let publisher = MemoryPubSubTransport::new(8).expect("transport builds");
    publisher.initialize().await.expect("initialize succeeds");
    assert!(publisher.is_healthy());
    assert!(publisher.health_status().is_some());
    assert_eq!(publisher.pending_operations(), 0);
    publisher
        .wait_for_completion(CancellationToken::new())
        .await
        .expect("broadcasts never retain work");

    // Publishing without subscribers is a valid ephemeral broadcast.
    publisher
        .publish(fire_and_forget(1))
        .await
        .expect("publish succeeds");

    let first = publisher.subscribe();
    let second = publisher.clone();
    publisher
        .publish(fire_and_forget(2))
        .await
        .expect("publish succeeds");

    // Every subscriber observes the publication independently.
    let delivery = first.receive().await.expect("receive succeeds");
    assert_eq!(delivery.envelope().id(), 2);
    let delivery = second.receive().await.expect("receive succeeds");
    assert_eq!(delivery.envelope().id(), 2);

    // Only AtMostOnce envelopes may broadcast.
    let error = publisher
        .publish(envelope(3))
        .await
        .expect_err("durable delivery guarantees are unsupported");
    assert_eq!(error.code(), ErrorCode::Unsupported);

    // A stopped bus refuses new broadcasts.
    publisher.stop_accepting();
    assert!(!publisher.is_accepting());
    let error = publisher
        .publish(fire_and_forget(4))
        .await
        .expect_err("a stopped bus refuses broadcasts");
    assert_eq!(error.code(), ErrorCode::Unavailable);
}

#[tokio::test]
async fn memory_pubsub_reports_lagged_subscribers_as_transient() {
    let publisher = MemoryPubSubTransport::new(1).expect("transport builds");
    let slow = publisher.subscribe();

    // A ring of one drops the oldest message once a second is published.
    publisher
        .publish(fire_and_forget(1))
        .await
        .expect("publish succeeds");
    publisher
        .publish(fire_and_forget(2))
        .await
        .expect("publish succeeds");

    let error = slow
        .receive()
        .await
        .expect_err("a lagged subscriber observes a transient failure");
    assert_eq!(error.code(), ErrorCode::Transient);

    // The cursor recovers and receives the retained message.
    let delivery = slow.receive().await.expect("receive succeeds");
    assert_eq!(delivery.envelope().id(), 2);
}
