# NATS 传输

## 简介

NATS JetStream 提供持久化和消费者组支持。

## 配置

```toml
[dependencies]
catga-nats = "0.1"
```

## 基础用法

```rust
use catga_nats::NatsTransport;
use catga_core::{Destination, Envelope};
use std::sync::Arc;

#[tokio::main]
async fn main() -> CatgaResult<()> {
    let transport = NatsTransport::connect("nats://localhost:4222").await?;

    // 发布事件
    transport
        .publish(
            envelope,
            Destination::Topic("orders.created"),
        )
        .await?;

    Ok(())
}
```

## 请求-响应

```rust
use catga_core::RequestTransport;

// 发送请求
let response = transport
    .send_request(
        envelope,
        Destination::Queue("order-service"),
        Duration::from_secs(5),
    )
    .await?;
```

## 消费者组

```rust
use catga_core::CompetingConsumer;

let consumer = CompetingConsumer::new(transport.clone())
    .with_stream("ORDERS")
    .with_consumer("processor-1")
    .with_group("order-processors");

consumer.run(|delivery| async move {
    // 处理消息
    delivery.ack().await?;
    Ok(())
}).await?;
```
