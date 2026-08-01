# RocketMQ 传输

## 简介

RocketMQ (robustmq) 用于大规模分布式消息处理。

## 配置

```toml
[dependencies]
catga-robustmq = "0.1"
```

## 基础用法

```rust
use catga_robustmq::RobustMqTransport;
use catga_core::{Destination, Envelope};

#[tokio::main]
async fn main() -> CatgaResult<()> {
    let transport = RobustMqTransport::connect(
        "mq://localhost:8080",
        "my-producer-group",
    ).await?;

    // 发布消息
    transport
        .publish(
            envelope,
            Destination::Topic("orders"),
        )
        .await?;

    Ok(())
}
```

## 顺序消息

```rust
use catga_core::Message;

struct OrderMessage {
    order_id: String,
    sequence: u32,
}

impl Message for OrderMessage {}

// 相同 order_id 的消息保证顺序
transport
    .publish_ordered(
        envelope,
        Destination::Topic("orders"),
        |msg| msg.order_id.clone(),  // 分区键
    )
    .await?;
```

## 事务消息

```rust
use catga_core::TransactionContext;

let tx = transport.begin_transaction().await?;

tx.send(Command::CreateOrder { ... }).await?;
tx.publish(Event::OrderCreated { ... }).await?;

tx.commit().await?;
```
