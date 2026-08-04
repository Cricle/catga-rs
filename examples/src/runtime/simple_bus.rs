//! Minimal bus example with typed endpoint registration.
//!
//! Demonstrates how to set up a bus with typed endpoints for command and event handling.
//!
//! ```bash
//! cargo run --bin simple_bus
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use catga_core::auto::Bus;
use catga_core::codec::memorypack::{
    MemoryPackCodec, MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackWriter, MemoryPackable,
};
use catga_core::memory::MemoryTransport;
use catga_core::{
    CatgaResult, Envelope, EnvelopePublisher, Message, MessageMetadata, PayloadEncoder,
    TypedDeliveryHandler,
};

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

#[derive(Clone, MemoryPackable)]
struct PlaceOrder {
    order_id: u32,
}
impl Message for PlaceOrder {}

#[derive(Clone, MemoryPackable)]
struct OrderPlaced {
    order_id: u32,
}
impl Message for OrderPlaced {}

// ---------------------------------------------------------------------------
// Handlers — struct-based for TypedDeliveryHandler (requires #[async_trait])
// ---------------------------------------------------------------------------

struct PlaceOrderHandler;

#[async_trait]
impl TypedDeliveryHandler<PlaceOrder> for PlaceOrderHandler {
    async fn handle(&self, cmd: &PlaceOrder) -> CatgaResult<()> {
        println!("  [command] placing order #{}", cmd.order_id);
        // In a real app, you'd publish OrderPlaced here via PublisherHandle
        Ok(())
    }
}

struct OrderPlacedHandler;

#[async_trait]
impl TypedDeliveryHandler<OrderPlaced> for OrderPlacedHandler {
    async fn handle(&self, event: &OrderPlaced) -> CatgaResult<()> {
        println!("  [event] order #{} placed", event.order_id);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    let transport = Arc::new(MemoryTransport::new(64)?);

    // Build bus with typed endpoints
    let bus = Bus::builder(transport.clone())
        .routed_endpoint::<PlaceOrder, _, _>(
            "commands",
            Arc::new(PlaceOrderHandler),
            Arc::new(MemoryPackCodec::default()),
            2,
        )?
        .routed_endpoint::<OrderPlaced, _, _>(
            "events",
            Arc::new(OrderPlacedHandler),
            Arc::new(MemoryPackCodec::default()),
            2,
        )?
        .build();

    println!(
        "simple_bus: bus built with 2 endpoints: {:?}",
        bus.endpoint_names()
    );

    let token = bus.shutdown_token();
    let run = bus.run_until_cancelled();
    let driver = async {
        // Simulate publishing messages to the transport
        // Note: In a real app, you'd use PublisherHandle or BusPublisher
        let codec = MemoryPackCodec::default();
        for i in 1..=3_u32 {
            let payload = codec
                .encode_payload(&PlaceOrder { order_id: i })
                .expect("encode PlaceOrder should not fail");
            transport
                .publish(Envelope::new(
                    i as u64,
                    "PlaceOrder",
                    payload,
                    MessageMetadata::new(i as u64, None),
                ))
                .await
                .expect("transport publish should not fail");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        token.cancel();
    };

    let (result, _) = tokio::join!(run, driver);
    println!("simple_bus: finished with result: {:?}", result);

    Ok(())
}
