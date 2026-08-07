# Redis Streams

## Introduction

Redis Streams provides high-performance persistent message queuing.

## Configuration

```toml
[dependencies]
catga-redis = "0.1"
```

## Basic Usage

```rust
use catga_redis::RedisTransport;
use catga_core::{Destination, Envelope};
use std::sync::Arc;

#[tokio::main]
async fn main() -> CatgaResult<()> {
    let transport = RedisTransport::connect("redis://localhost").await?;

    // Publish Event
    transport
        .publish(
            envelope,
            Destination::Topic("events"),
        )
        .await?;

    Ok(())
}
```

## Competing Consumer

```rust
use catga_core::CompetingConsumer;

let consumer = CompetingConsumer::new(transport.clone())
    .with_stream("my-stream")
    .with_group("processors")
    .with_consumer("instance-1");

// Process message
consumer.run(|delivery| async move {
    // Processing logic
    delivery.ack().await?;
    Ok(())
}).await?;
```

## Persistence

Redis Streams automatically persists messages, supporting consumer acknowledgment:

```rust
// Message acknowledgment
delivery.ack().await?;

// Negative acknowledgment (re-queue)
delivery.nack().await?;

// Dead letter
delivery.dlq().await?;
```
