# Transport: Message Transport Contracts and Adapters

## Core Contract (`catga-core`)

```rust,ignore
#[async_trait]
pub trait MessageTransport: Send + Sync {
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()>;
    async fn receive(&self) -> CatgaResult<Delivery>;
    async fn ack(&self, delivery: Delivery) -> CatgaResult<()>;   // Acknowledge after successful processing
    async fn nack(&self, delivery: Delivery) -> CatgaResult<()>;  // Request redelivery
    // Batch: publish_batch / publish_batch_with_concurrency (default concurrency limit
    // DEFAULT_TRANSPORT_BATCH_CONCURRENCY = 100, 0 returns Validation)
}

// Named persistent destination (queue semantics): send_to / send_batch_to / receive_from
pub trait DestinationTransport: MessageTransport { /* ... */ }
```

`Envelope` is the wire-format message carrier (payload is encoded bytes):

```rust,ignore
use catga_core::{Envelope, MessageMetadata};

let envelope = Envelope::new(
    1,                                   // Message id (u64)
    "order.created",                     // Stable message type name
    payload_bytes,                       // Vec<u8>, encoded via codec
    MessageMetadata::new(1, None),       // (message_id, correlation_id: Option<u64>)
);
```

- `Destination::parse("order-queue")?` — Validates non-empty destination name (empty/pure whitespace → `ErrorCode::Validation`).
- Acknowledgment semantics: `ack` after successful processing; `nack` after processing failure to request redelivery. **Deliveries dropped without ack can be freely redelivered by persistent adapters** → consumers must design for at-least-once (Inbox/idempotency key deduplication).
- Backends that don't support nack return `ErrorCode::Unsupported` instead of silently dropping messages.
- `Delivery::attempts()` reads the backend's delivery attempt count.

## Memory Transport (`catga-memory`)

For local composition and deterministic testing, capacity is bounded:

```rust,ignore
use catga_memory::MemoryTransport;

let transport = MemoryTransport::new(16)?;              // Bounded capacity
transport.publish(envelope).await?;
let delivery = transport.receive().await?;
assert_eq!(delivery.envelope().message_type(), "order.created");
transport.ack(delivery).await?;
```

## NATS (`catga-nats`)

Two transports. Default JetStream consumer is durable: restarting the same worker must continue using the same stream/consumer names to resume from acknowledged cursor.

```rust,ignore
use catga_nats::{
    NatsConfig, NatsPubSubConfig, NatsPubSubTransport, NatsReceiveOptions, NatsTransport,
};

// Core NATS Pub/Sub: no JetStream resources, only visible to online subscribers (ephemeral)
let pubsub = NatsPubSubTransport::connect(NatsPubSubConfig {
    server: "nats://127.0.0.1:4222".into(),
    subject: "orders.notifications".into(),
}).await?;

// JetStream persistent transport: stream + subject + durable pull consumer
let config = NatsConfig {
    server: "nats://127.0.0.1:4222".into(),
    stream: "orders".into(),
    subject: "orders.created".into(),
    consumer: "orders-worker".into(),
};
// NatsTransport::connect(config) uses default prefetch of 64.
// Reuse existing connection: from_client(client, config); custom codec: connect_with_codec(config, codec)

// receive() requests 64 messages per call to JetStream by default and delivers them one by one internally.
// Override prefetch limit with connection options; each Delivery still requires independent ack/nack.
let durable = NatsTransport::connect_with_receive_options(
    config,
    NatsReceiveOptions::default().with_pull_batch_size(128)?,
).await?;
```

Read model one-time rebuild or high-frequency temporary workers should not create durable cursors. Explicitly select ephemeral and set cleanup time according to operations retention policy; at this point `NatsConfig.consumer` only maintains compatibility and won't create a consumer with that name in JetStream:

```rust,ignore
use std::time::Duration;
use catga_nats::{NatsConsumerOptions, NatsTransportOptions};

let replay = NatsTransport::connect_with_options(
    config,
    NatsTransportOptions::default().with_consumer(
        NatsConsumerOptions::ephemeral().with_inactive_threshold(Duration::from_secs(300)),
    ),
).await?;
```

Do not use ephemeral cursor as projection progress storage: after process restart it starts from the server's default delivery position. For recoverable read models, retain durable consumers, or use `ProjectionCheckpointStore` to persist EventStore replay progress.

Use `NatsPublisher` when the publishing side does not consume messages — it only creates or reuses streams and won't leave idle durable consumers:

```rust,ignore
use catga_nats::{NatsPublisher, NatsPublisherConfig};

let publisher = NatsPublisher::connect(NatsPublisherConfig {
    server: "nats://127.0.0.1:4222".into(),
    stream: "orders".into(),
    subject: "orders.created".into(),
}).await?;
publisher.publish(envelope).await?;
```

- Named destination resources use `NatsDestinationConfig { stream, subject, consumer }` explicitly provisioned — **won't** be auto-derived from destination name, ensuring retention policy and consumer identity are auditable.
- RPC: `NatsRequestClient` / `NatsRequestServer` / `NatsTypedRequestClient`.

## Redis (`catga-redis`)

```rust,ignore
use catga_redis::{RedisConfig, RedisPubSubConfig, RedisPubSubTransport, RedisTransport};

// Pub/Sub (ephemeral)
let pubsub = RedisPubSubTransport::connect(RedisPubSubConfig {
    server: "redis://127.0.0.1/".into(),
    channel: "orders.notifications".into(),
}).await?;

// Stream persistent transport: stream + consumer group + consumer name (with pending reclaim option)
let transport = RedisTransport::connect(RedisConfig {
    server: "redis://127.0.0.1/".into(),
    stream: "orders".into(),
    group: "workers".into(),
    consumer: "worker-a".into(),
}).await?;
// from_client / connect_with_client / connect_with_codec variants available
```

RPC: `RedisRequestClient` / `RedisRequestServer`.

## RobustMQ (`catga-robustmq`)

mq9 mailbox extension: `MailboxClient` sends complete Catga envelope, `MailboxRequestServer` / `MailboxRequest` provide explicit request/reply; default bounded MemoryPack codec (variants with `*_with_codec` available):

```rust,ignore
use catga_robustmq::MailboxConfig;

let replies = MailboxConfig {
    server: "nats://127.0.0.1:4222".into(),
    ttl_seconds: 60,          // Bounded TTL
    public: false,            // Reply mailbox stays private
    name: "order-replies".into(),
    description: "private request replies".into(),
};
```

Security: mailbox visibility and correlation are not authorization boundaries — callers must authenticate the server URL and validate identity and authorization on each inbound envelope before application payload.

## TypedTransport: Typed Message Direct Send (`catga-core`)

Eliminates manual Envelope writing: auto-encodes/decodes, allocates distributed IDs, writes schema version and priority.

```rust,ignore
use catga_core::{DistributedIdGenerator, SnowflakeIdGenerator, TypedTransport};

let id_generator = SnowflakeIdGenerator::new(worker_id, Default::default())?;
let typed = TypedTransport::new(transport, Arc::new(id_generator));   // Default codec
// Specify codec: TypedTransport::new_with_codec(..) / new_with_shared_codec(..)

typed.publish(&message).await?;                 // Typed Message → envelope
typed.publish_event(&event).await?;             // Event (with fan-out semantics)
typed.publish_reliable_event(&event).await?;    // Reliable event (priority/reliable delivery)
typed.publish_batch(messages).await?;           // Bounded batch (*_with_concurrency variants)

// Receive side: decode to typed delivery, ack/nack ownership still with caller
let delivery: TypedDelivery<MyMessage> = typed.receive().await?;
// Or all-in-one: decode + process + ack/nack by result
typed.process_next(|message: MyMessage| async move { Ok(()) }).await?;   // → TypedProcessOutcome
// Named destination: receive_from::<M>(destination) / process_next_from(..)
```

`process_next` is a single-message convenience API suitable for testing, imperative tools, and scenarios where the caller manages the loop themselves. For production consumer loops use `CompetingConsumer`: it has bounded concurrency, unified ack/nack, and completes already-received deliveries after cancellation.

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

Messages can control online metadata via `#[catga(priority = high)]` and `schema_version()` (see [mediator.md](mediator.md)).

## RPC: Request-Response (`catga-core` + Adapters)

- `RemoteRequest` — Marker for remotely invokable Request; `RequestClient<M>` — Typed client contract.
- `EnvelopeRequestClient::new(..)` — Generic envelope-based client: `request(envelope)` / `request_with_timeout(..)`; `RequestTransport` — Server contract.
- Ready-made implementations: NATS `NatsRequestClient` / `NatsRequestServer` / `NatsTypedRequestClient`; Redis `RedisRequestClient` / `RedisRequestServer`; MemoryPack `MemoryPackRequestClient`; RobustMQ `MailboxRequestServer` / `MailboxRequest`.
- Use `.remote_send(client, |state| request)` as remote step in DslFlow.

## Resilience, Batching, and Routing (`catga-core`)

- `ResilientTransport` — Wraps any `MessageTransport` with bounded retry; standalone `ResilienceExecutor::new(ResilienceOptions)?` can wrap any async operation (`execute(..)`).
- `TransportBatcher` / `TransportBatchRunner` (`TransportBatchOptions`) — Publish-side batch aggregation; runner driven by application tasks.
- `VersionedMessageTransport` + `EventVersionRegistry` / `EventUpgrader` — Message version upgrading (see [event-sourcing.md](event-sourcing.md)).
- `MessageRouter::new()` / `MessageDestinationRouter::new(default_destination)` — Routes messages by message type or key value to `Destination` (`add_route(..)`).
- `CompetingConsumer` / `SubscriptionRunner` — Consumer loop components (see [reliability.md](reliability.md)).

## Selection Guide

| Scenario | Choose |
| --- | --- |
| Single process / testing | `MemoryTransport` |
| Cross-process fire-and-forget notifications | NATS Core `NatsPubSubTransport` or Redis Pub/Sub |
| Persistent delivery, consumer groups, redelivery | NATS JetStream `NatsTransport` or Redis Stream `RedisTransport` |
| Request-response (RPC) | Each adapter's `*RequestClient/*RequestServer` |
| RobustMQ mq9 ecosystem | `MailboxClient` / `MailboxRequestServer` |
| Integrating your own system | Implement `MessageTransport` (+ `DestinationTransport`) contract |
