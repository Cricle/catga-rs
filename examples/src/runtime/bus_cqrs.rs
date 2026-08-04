//! In-memory CQRS example using the Bus topology API.
//!
//! Demonstrates: routed endpoints, BusPublisher, PublisherHandle (publish-from-handler),
//! and graceful shutdown — all without a broker.
//!
//! This example shows two handler patterns:
//! - **Stateful handler**: `PlaceOrderHandler` uses `PublisherHandle` to publish events.
//!   Requires a struct with `#[async_trait]` because it holds state.
//! - **Stateless handler**: `OrderPlacedHandler` is stateless. Can be replaced with a
//!   plain async fn if `TypedDeliveryHandler` blanket impls are added in the future.
//!
//! ```bash
//! cargo run --bin bus_cqrs
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use catga_core::auto::{Bus, PublisherHandle};
use catga_core::codec::memorypack::{
    MemoryPackCodec, MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackWriter, MemoryPackable,
};
use catga_core::memory::MemoryTransport;
use catga_core::{CatgaResult, Message, TypedDeliveryHandler};

#[derive(Clone, MemoryPackable)]
struct PlaceOrder {
    order_id: u32,
    item: String,
}
impl Message for PlaceOrder {}

#[derive(Clone, MemoryPackable)]
struct OrderPlaced {
    order_id: u32,
    item: String,
}
impl Message for OrderPlaced {}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Stateful handler: publishes events via PublisherHandle.
/// This pattern REQUIRES a struct because it holds state (the publisher handle).
struct PlaceOrderHandler {
    publisher: PublisherHandle<MemoryTransport, MemoryPackCodec>,
}

#[async_trait]
impl TypedDeliveryHandler<PlaceOrder> for PlaceOrderHandler {
    async fn handle(&self, cmd: &PlaceOrder) -> CatgaResult<()> {
        println!("  [command] placing order #{}: {}", cmd.order_id, cmd.item);
        self.publisher
            .publish(&OrderPlaced {
                order_id: cmd.order_id,
                item: cmd.item.clone(),
            })
            .await
    }
}

/// Stateless handler: only logs the event.
/// This pattern COULD be replaced with a plain async fn once TypedDeliveryHandler
/// blanket impls are added (similar to the Handler blanket impls).
struct OrderPlacedHandler;

// FUTURE: Once TypedDeliveryHandler blanket impls are added, this could become:
// ```rust
// async fn order_placed_handler(event: OrderPlaced) -> CatgaResult<()> {
//     println!("  [event]   order #{} placed: {}", event.order_id, event.item);
//     Ok(())
// }
// ```
// For now, use the struct-based approach.

#[async_trait]
impl TypedDeliveryHandler<OrderPlaced> for OrderPlacedHandler {
    async fn handle(&self, event: &OrderPlaced) -> CatgaResult<()> {
        println!(
            "  [event]   order #{} placed: {}",
            event.order_id, event.item
        );
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    let transport = Arc::new(MemoryTransport::new(64)?);
    let handle = PublisherHandle::new();

    let (bus, publisher) = Bus::builder(Arc::clone(&transport))
        .routed_endpoint::<PlaceOrder, _, _>(
            "commands",
            Arc::new(PlaceOrderHandler {
                publisher: handle.clone(),
            }),
            Arc::new(MemoryPackCodec::default()),
            1,
        )?
        .routed_endpoint::<OrderPlaced, _, _>(
            "events",
            Arc::new(OrderPlacedHandler),
            Arc::new(MemoryPackCodec::default()),
            1,
        )?
        .build_with_publisher(MemoryPackCodec::default())?;

    handle.bind(publisher);

    println!("bus_cqrs: publishing 3 orders...");
    let token = bus.shutdown_token();
    let run = bus.run_until_cancelled();
    let driver = async {
        for i in 1..=3 {
            handle
                .publish(&PlaceOrder {
                    order_id: i,
                    item: format!("item-{i}"),
                })
                .await
                .expect("publish");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        token.cancel();
    };
    let (result, _) = tokio::join!(run, driver);
    let runs = result?;

    println!(
        "bus_cqrs: done. commands={} events={}",
        runs[0].acknowledged(),
        runs[1].acknowledged()
    );
    Ok(())
}
