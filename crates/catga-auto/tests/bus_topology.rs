//! Tests for Bus topology routing: publish routes to the correct endpoint destination.

use std::sync::Arc;

use catga_auto::Bus;
use catga_codec_memorypack::{MemoryPackCodec, MemoryPackable};
use catga_core::{CatgaResult, Message, TypedDeliveryHandler};
use catga_memory::MemoryTransport;

#[derive(Clone, MemoryPackable)]
struct Order(u32);
impl Message for Order {}

#[derive(Clone, MemoryPackable)]
struct Payment(u32);
impl Message for Payment {}

struct Record;

#[async_trait::async_trait]
impl TypedDeliveryHandler<Order> for Record {
    async fn handle(&self, _: &Order) -> CatgaResult<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl TypedDeliveryHandler<Payment> for Record {
    async fn handle(&self, _: &Payment) -> CatgaResult<()> {
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn routed_endpoint_consumes_from_its_own_destination() {
    let transport = Arc::new(MemoryTransport::new(64).expect("transport"));
    let (bus, publisher) = Bus::builder(Arc::clone(&transport))
        .routed_endpoint::<Order, _, _>(
            "orders",
            Arc::new(Record),
            Arc::new(MemoryPackCodec::default()),
            1,
        )
        .expect("endpoint")
        .build_with_publisher(MemoryPackCodec::default())
        .expect("build");

    publisher.publish(&Order(1)).await.expect("publish");

    let token = bus.shutdown_token();
    let run = async { bus.run_until_cancelled().await };
    let stop = async {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        token.cancel();
    };
    let (result, _) = tokio::join!(run, stop);
    let runs = result.expect("bus run");
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].received(), 1);
    assert_eq!(runs[0].acknowledged(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn publisher_routes_by_message_type() {
    let transport = Arc::new(MemoryTransport::new(64).expect("transport"));
    let (bus, publisher) = Bus::builder(Arc::clone(&transport))
        .routed_endpoint::<Order, _, _>(
            "orders",
            Arc::new(Record),
            Arc::new(MemoryPackCodec::default()),
            1,
        )
        .expect("order endpoint")
        .routed_endpoint::<Payment, _, _>(
            "payments",
            Arc::new(Record),
            Arc::new(MemoryPackCodec::default()),
            1,
        )
        .expect("payment endpoint")
        .build_with_publisher(MemoryPackCodec::default())
        .expect("build");

    publisher.publish(&Order(1)).await.expect("publish order");
    publisher
        .publish(&Payment(2))
        .await
        .expect("publish payment");
    publisher.publish(&Order(3)).await.expect("publish order");

    let token = bus.shutdown_token();
    let run = async { bus.run_until_cancelled().await };
    let stop = async {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        token.cancel();
    };
    let (result, _) = tokio::join!(run, stop);
    let runs = result.expect("bus run");
    assert_eq!(runs.len(), 2);
    // orders endpoint: 2 messages
    assert_eq!(runs[0].received(), 2);
    assert_eq!(runs[0].acknowledged(), 2);
    // payments endpoint: 1 message
    assert_eq!(runs[1].received(), 1);
    assert_eq!(runs[1].acknowledged(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn publish_unrouted_type_returns_not_found() {
    let transport = Arc::new(MemoryTransport::new(64).expect("transport"));
    let (_bus, publisher) = Bus::builder(Arc::clone(&transport))
        .routed_endpoint::<Order, _, _>(
            "orders",
            Arc::new(Record),
            Arc::new(MemoryPackCodec::default()),
            1,
        )
        .expect("endpoint")
        .build_with_publisher(MemoryPackCodec::default())
        .expect("build");

    let error = publisher
        .publish(&Payment(1))
        .await
        .expect_err("should fail");
    assert_eq!(error.code(), catga_core::ErrorCode::NotFound);
}

#[tokio::test(flavor = "current_thread")]
async fn duplicate_route_registration_fails() {
    let transport = Arc::new(MemoryTransport::new(64).expect("transport"));
    let result = Bus::builder(Arc::clone(&transport))
        .routed_endpoint::<Order, _, _>(
            "orders",
            Arc::new(Record),
            Arc::new(MemoryPackCodec::default()),
            1,
        )
        .expect("first")
        .routed_endpoint::<Order, _, _>(
            "orders-again",
            Arc::new(Record),
            Arc::new(MemoryPackCodec::default()),
            1,
        );
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn unrouted_endpoint_does_not_receive_routed_messages() {
    let transport = Arc::new(MemoryTransport::new(64).expect("transport"));
    let (bus, publisher) = Bus::builder(Arc::clone(&transport))
        .routed_endpoint::<Order, _, _>(
            "orders",
            Arc::new(Record),
            Arc::new(MemoryPackCodec::default()),
            1,
        )
        .expect("routed endpoint")
        .endpoint::<Payment, _, _>(
            "payments-shared",
            Arc::new(Record),
            Arc::new(MemoryPackCodec::default()),
            1,
        )
        .expect("unrouted endpoint")
        .build_with_publisher(MemoryPackCodec::default())
        .expect("build");

    publisher.publish(&Order(1)).await.expect("publish");

    let token = bus.shutdown_token();
    let run = async { bus.run_until_cancelled().await };
    let stop = async {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        token.cancel();
    };
    let (result, _) = tokio::join!(run, stop);
    let runs = result.expect("bus run");
    assert_eq!(runs[0].received(), 1);
    assert_eq!(runs[1].received(), 0);
}
