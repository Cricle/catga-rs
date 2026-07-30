# Transport：消息传输契约与适配器

## 核心契约（`catga-core`）

```rust,ignore
#[async_trait]
pub trait MessageTransport: Send + Sync {
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()>;
    async fn receive(&self) -> CatgaResult<Delivery>;
    async fn ack(&self, delivery: Delivery) -> CatgaResult<()>;   // 处理成功后确认
    async fn nack(&self, delivery: Delivery) -> CatgaResult<()>;  // 请求重投
    // 批量：publish_batch / publish_batch_with_concurrency（默认并发上限
    // DEFAULT_TRANSPORT_BATCH_CONCURRENCY = 100，0 会返回 Validation）
}

// 具名持久化目的地（队列语义）：send_to / send_batch_to / receive_from
pub trait DestinationTransport: MessageTransport { /* ... */ }
```

`Envelope` 是线路上的消息载体（payload 为已编码字节）：

```rust,ignore
use catga_core::{Envelope, MessageMetadata};

let envelope = Envelope::new(
    1,                                   // 消息 id（u64）
    "order.created",                     // 稳定消息类型名
    payload_bytes,                       // Vec<u8>，用 codec 编码
    MessageMetadata::new(1, None),       // (message_id, correlation_id: Option<u64>)
);
```

- `Destination::parse("order-queue")?` — 校验非空目的地名（空/纯空白 → `ErrorCode::Validation`）。
- 确认语义：处理成功后 `ack`；处理失败 `nack` 请求重投。**未 ack 就被丢弃的 delivery，持久化适配器可以自由重投** → 消费端必须按 at-least-once 设计（Inbox/幂等键去重）。
- 不支持 nack 的后端返回 `ErrorCode::Unsupported` 而不是静默丢消息。
- `Delivery::attempts()` 可读取后端投递次数。

## 内存传输（`catga-memory`）

本地组合与确定性测试用，容量有界：

```rust,ignore
use catga_memory::MemoryTransport;

let transport = MemoryTransport::new(16)?;              // 有界容量
transport.publish(envelope).await?;
let delivery = transport.receive().await?;
assert_eq!(delivery.envelope().message_type(), "order.created");
transport.ack(delivery).await?;
```

## NATS（`catga-nats`）

两种传输，配置名稳定（重启同一 worker 要继续用同一组 stream/consumer 名）：

```rust,ignore
use catga_nats::{
    NatsConfig, NatsPubSubConfig, NatsPubSubTransport, NatsReceiveOptions, NatsTransport,
};

// Core NATS Pub/Sub：无 JetStream 资源，仅在线订阅者可见（ ephemeral ）
let pubsub = NatsPubSubTransport::connect(NatsPubSubConfig {
    server: "nats://127.0.0.1:4222".into(),
    subject: "orders.notifications".into(),
}).await?;

// JetStream 持久化传输：stream + subject + durable pull consumer
let config = NatsConfig {
    server: "nats://127.0.0.1:4222".into(),
    stream: "orders".into(),
    subject: "orders.created".into(),
    consumer: "orders-worker".into(),
};
// NatsTransport::connect(config) 使用默认的 64 条预取。
// 复用已有连接：from_client(client, config)；自定义编解码：connect_with_codec(config, codec)

// receive() 默认每次向 JetStream 请求 64 条并在 transport 内部逐条交付。
// 用连接时选项覆盖预取上限；每条 Delivery 仍需独立 ack/nack。
let durable = NatsTransport::connect_with_receive_options(
    config,
    NatsReceiveOptions::default().with_pull_batch_size(128)?,
).await?;
```

- 具名目的地资源用 `NatsDestinationConfig { stream, subject, consumer }` 显式供给——**不会**从目的地名自动推导，保证保留策略与消费者身份可审查。
- RPC：`NatsRequestClient` / `NatsRequestServer` / `NatsTypedRequestClient`。

## Redis（`catga-redis`）

```rust,ignore
use catga_redis::{RedisConfig, RedisPubSubConfig, RedisPubSubTransport, RedisTransport};

// Pub/Sub（ephemeral）
let pubsub = RedisPubSubTransport::connect(RedisPubSubConfig {
    server: "redis://127.0.0.1/".into(),
    channel: "orders.notifications".into(),
}).await?;

// Stream 持久化传输：stream + 消费者组 + 消费者名（含 pending reclaim 选项）
let transport = RedisTransport::connect(RedisConfig {
    server: "redis://127.0.0.1/".into(),
    stream: "orders".into(),
    group: "workers".into(),
    consumer: "worker-a".into(),
}).await?;
// from_client / connect_with_client / connect_with_codec 变体可选
```

RPC：`RedisRequestClient` / `RedisRequestServer`。

## RobustMQ（`catga-robustmq`）

mq9 mailbox 扩展：`MailboxClient` 发送完整 Catga envelope，`MailboxRequestServer` / `MailboxRequest` 提供显式请求/应答；默认有界 MemoryPack 编解码（`*_with_codec` 可变）。

```rust,ignore
use catga_robustmq::MailboxConfig;

let replies = MailboxConfig {
    server: "nats://127.0.0.1:4222".into(),
    ttl_seconds: 60,          // 有限 TTL
    public: false,            // 应答 mailbox 保持私有
    name: "order-replies".into(),
    description: "private request replies".into(),
};
```

安全：mailbox 可见性与关联不等于授权边界——调用方必须认证服务器 URL，并在应用载荷前校验每个入站 envelope 的身份与授权。

## TypedTransport：typed 消息直发（`catga-core`）

免去手写 Envelope：自动编解码、分配分布式 ID、写入 schema version 与优先级。

```rust,ignore
use catga_core::{DistributedIdGenerator, SnowflakeIdGenerator, TypedTransport};

let id_generator = SnowflakeIdGenerator::new(worker_id, Default::default())?;
let typed = TypedTransport::new(transport, Arc::new(id_generator));   // 默认编解码
// 指定 codec：TypedTransport::new_with_codec(..) / new_with_shared_codec(..)

typed.publish(&message).await?;                 // typed Message → envelope
typed.publish_event(&event).await?;             // Event（带扇出语义）
typed.publish_reliable_event(&event).await?;    // 可靠事件（优先级/可靠投递）
typed.publish_batch(messages).await?;           // 有界批量（*_with_concurrency 变体）

// 接收侧：解码为 typed delivery，ack/nack 所有权仍在调用方
let delivery: TypedDelivery<MyMessage> = typed.receive().await?;
// 或者一条龙：解码 + 处理 + 按结果 ack/nack
typed.process_next(|message: MyMessage| async move { Ok(()) }).await?;   // → TypedProcessOutcome
// 具名目的地：receive_from::<M>(destination) / process_next_from(..)
```

`process_next` 是一条消息的便利 API，适合测试、命令式工具和调用方自己管理循环的场景。生产消费循环请使用 `CompetingConsumer`：它有界并发、统一 ack/nack，并在取消后完成已接收的 delivery。

```rust,ignore
use std::sync::Arc;
use async_trait::async_trait;
use catga_core::{CatgaResult, CompetingConsumer, TypedDeliveryHandler};

struct OrderCreatedHandler;

#[async_trait]
impl TypedDeliveryHandler<OrderCreated> for OrderCreatedHandler {
    async fn handle(&self, order: &OrderCreated) -> CatgaResult<()> {
        // Only business work belongs here. Catga decodes, acks, nacks, and scopes context.
        persist_order_projection(order).await
    }
}

let consumer = CompetingConsumer::typed(
    Arc::clone(&transport),
    Arc::new(OrderCreatedHandler),
    Arc::new(codec),
    16,
)?;
let shutdown = shutdown_token.clone();
tokio::spawn(async move { consumer.run_until_cancelled(shutdown).await });
```

The spawned consumer owns each `Delivery` until Catga resolves it. Applications do not need an
`Acknowledger: Sync` bound to run this loop in a Tokio task, and retaining exclusive ack ownership
prevents cross-task double acknowledgement.

消息可用 `#[catga(priority = high)]` 与 `schema_version()` 控制线上元数据（见 [mediator.md](mediator.md)）。

## RPC：请求-响应（`catga-core` + 适配器）

- `RemoteRequest` — 可远程调用的 Request 标记；`RequestClient<M>` — typed 客户端契约。
- `EnvelopeRequestClient::new(..)` — 基于 envelope 的通用客户端：`request(envelope)` / `request_with_timeout(..)`；`RequestTransport` — 服务端契约。
- 现成实现：NATS `NatsRequestClient` / `NatsRequestServer` / `NatsTypedRequestClient`；Redis `RedisRequestClient` / `RedisRequestServer`；MemoryPack `MemoryPackRequestClient`；RobustMQ `MailboxRequestServer` / `MailboxRequest`。
- DslFlow 里用 `.remote_send(client, |state| request)` 作为远程步骤。

## 弹性、批处理与路由（`catga-core`）

- `ResilientTransport` — 给任意 `MessageTransport` 包一层有界重试；独立 `ResilienceExecutor::new(ResilienceOptions)?` 可包装任意 async 操作（`execute(..)`）。
- `TransportBatcher` / `TransportBatchRunner`（`TransportBatchOptions`）— 发布侧批量聚合；runner 由应用任务驱动。
- `VersionedMessageTransport` + `EventVersionRegistry` / `EventUpgrader` — 消息版本升级（见 [event-sourcing.md](event-sourcing.md)）。
- `MessageRouter::new()` / `MessageDestinationRouter::new(default_destination)` — 按消息类型或键值把消息路由到 `Destination`（`add_route(..)`）。
- `CompetingConsumer` / `SubscriptionRunner` — 消费循环构件（见 [reliability.md](reliability.md)）。

## 选择建议

| 场景 | 选择 |
| --- | --- |
| 单进程/测试 | `MemoryTransport` |
| 跨进程 fire-and-forget 通知 | NATS Core `NatsPubSubTransport` 或 Redis Pub/Sub |
| 持久化投递、消费者组、重投 | NATS JetStream `NatsTransport` 或 Redis Stream `RedisTransport` |
| 请求-响应（RPC） | 各适配器的 `*RequestClient/*RequestServer` |
| RobustMQ mq9 生态 | `MailboxClient` / `MailboxRequestServer` |
| 接入自有系统 | 实现 `MessageTransport`（+ `DestinationTransport`）契约即可 |
