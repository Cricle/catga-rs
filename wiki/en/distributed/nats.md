# NATS Transport

## Introduction

NATS JetStream provides persistence and competing consumer support.

## Configuration

```toml
[dependencies]
catga-nats = "0.1"
```

## Basic Usage

```rust
use catga_nats::NatsTransport;
use catga_core::{Destination, Envelope};
use std::sync::Arc;

#[tokio::main]
async fn main() -> CatgaResult<()> {
    let transport = NatsTransport::connect("nats://localhost:4222").await?;

    // Publish Event
    transport
        .publish(
            envelope,
            Destination::Topic("orders.created"),
        )
        .await?;

    Ok(())
}
```

## Request-Response

```rust
use catga_core::RequestTransport;

// Send request
let response = transport
    .send_request(
        envelope,
        Destination::Queue("order-service"),
        Duration::from_secs(5),
    )
    .await?;
```

## Competing Consumer

```rust
use catga_core::CompetingConsumer;

let consumer = CompetingConsumer::new(transport.clone())
    .with_stream("ORDERS")
    .with_consumer("processor-1")
    .with_group("order-processors");

consumer.run(|delivery| async move {
    // Process message
    delivery.ack().await?;
    Ok(())
}).await?;
```
