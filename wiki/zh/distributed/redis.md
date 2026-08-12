# Redis Streams

## 简介

Redis Streams 提供高性能的持久化消息队列。

## 配置

```toml
[dependencies]
catga-redis = "0.1"
```

## 基础用法

```rust
use catga_redis::RedisTransport;
use catga_core::{Destination, Envelope};
use std::sync::Arc;

#[tokio::main]
async fn main() -> CatgaResult<()> {
    let transport = RedisTransport::connect("redis://localhost").await?;

    // 发布事件
    transport
        .publish(
            envelope,
            Destination::Topic("events"),
        )
        .await?;

    Ok(())
}
```

## 消费者组

```rust
use catga_core::CompetingConsumer;

let consumer = CompetingConsumer::new(transport.clone())
    .with_stream("my-stream")
    .with_group("processors")
    .with_consumer("instance-1");

// 处理消息
consumer.run(|delivery| async move {
    // 处理逻辑
    delivery.ack().await?;
    Ok(())
}).await?;
```

## 持久化

Redis Streams 自动持久化消息，支持消费者组确认：

```rust
// 消息确认
delivery.ack().await?;

// 否定确认 (重新入队)
delivery.nack().await?;

// 死信
delivery.dlq().await?;
```
