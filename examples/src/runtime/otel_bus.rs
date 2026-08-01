//! Demonstrates Bus observability: tracing spans and metrics emitted during consumption.
//!
//! Run with `RUST_LOG=catga=info cargo run --bin otel_bus` to see structured spans.
//! In production, replace the fmt subscriber with `tracing-opentelemetry` to export
//! spans and metrics to any OTel-compatible backend (Jaeger, Grafana, Datadog).

use std::sync::Arc;

use async_trait::async_trait;
use catga_auto::{Bus, PublisherHandle};
use catga_codec_memorypack::{MemoryPackCodec, MemoryPackable};
use catga_core::{CatgaResult, Message, TypedDeliveryHandler};
use catga_memory::MemoryTransport;

#[derive(Clone, MemoryPackable)]
struct Ping(u32);
impl Message for Ping {}

struct Pong;

#[async_trait]
impl TypedDeliveryHandler<Ping> for Pong {
    async fn handle(&self, msg: &Ping) -> CatgaResult<()> {
        tracing::info!(ping = msg.0, "handled ping");
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_target(true)
        .init();

    let transport = Arc::new(MemoryTransport::new(64)?);
    let handle = PublisherHandle::new();

    let (bus, publisher) = Bus::builder(Arc::clone(&transport))
        .routed_endpoint::<Ping, _, _>(
            "pings",
            Arc::new(Pong),
            Arc::new(MemoryPackCodec::default()),
            1,
        )?
        .build_with_publisher(MemoryPackCodec::default())?;

    handle.bind(publisher);

    let token = bus.shutdown_token();
    let run = bus.run_until_cancelled();
    let driver = async {
        for i in 1..=5 {
            handle.publish(&Ping(i)).await.expect("publish");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        token.cancel();
    };
    let (result, _) = tokio::join!(run, driver);
    let runs = result?;

    println!("otel_bus: consumed {} pings", runs[0].acknowledged());
    Ok(())
}
