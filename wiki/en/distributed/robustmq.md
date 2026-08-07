# RocketMQ Transport

## Introduction

RocketMQ (robustmq) is used for large-scale distributed message processing.

## Configuration

```toml
[dependencies]
catga-robustmq = "0.1"
```

## Basic Usage

```rust
use catga_robustmq::RobustMqTransport;
use catga_core::{Destination, Envelope};

#[tokio::main]
async fn main() -> CatgaResult<()> {
    let transport = RobustMqTransport::connect(
        "mq://localhost:8080",
        "my-producer-group",
    ).await?;

    // Publish message
    transport
        .publish(
            envelope,
            Destination::Topic("orders"),
        )
        .await?;

    Ok(())
}
```

## Ordered Messages

```rust
use catga_core::Message;

struct OrderMessage {
    order_id: String,
    sequence: u32,
}

impl Message for OrderMessage {}

// Messages with the same order_id are guaranteed to be ordered
transport
    .publish_ordered(
        envelope,
        Destination::Topic("orders"),
        |msg| msg.order_id.clone(),  // Partition key
    )
    .await?;
```

## Transaction Messages

```rust
use catga_core::TransactionContext;

let tx = transport.begin_transaction().await?;

tx.send(Command::CreateOrder { ... }).await?;
tx.publish(Event::OrderCreated { ... }).await?;

tx.commit().await?;
```
