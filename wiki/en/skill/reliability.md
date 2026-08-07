# Message Reliability Patterns: Outbox / Inbox / Idempotency / Dead Letter / Subscription / Consumer Loops

Catga breaks "exactly once" into explicit components: write side **Outbox**, read side **Inbox + Idempotency**, terminal **Dead Letter**, loops driven by **application-owned tasks**. Storage implementations are in [stores.md](stores.md).

## 1. Outbox (Write-Side Reliable Publishing)

Persist envelope to the database first after request succeeds, then publish asynchronously via processor — avoids the "database written, message lost" dual-write problem.

```rust,ignore
use catga_core::{OutboxBehavior, OutboxEnvelope, OutboxProcessor, OutboxLoopOptions};

// In pipeline: persist envelope after successful request (messages implementing OutboxEnvelope)
let pipeline = catga_pipeline!(PlaceOrder; OutboxBehavior::new(outbox_store.clone()))?;

// Application-owned worker: claim → publish → ack
let processor = OutboxProcessor::new(
    outbox_store,            // Arc<impl OutboxStore>
    transport,               // Arc<impl MessageTransport>
    "worker-1",              // owner identity (claim ownership)
    64,                      // batch size per scan (≤ MAX_OUTBOX_CLAIM_LIMIT)
)?;
// new_with_concurrency(.., concurrency_limit): concurrent publishing with independent ack/release per message
processor.flush_once().await?;                 // Process a batch, returns OutboxRun stats
// Or continuous loop (observe cancellation between batches; storage failures back off with error_delay):
processor.run_until_cancelled(OutboxLoopOptions::new(scan_interval, error_delay)?, token).await?;
```

- `OutboxMessage::new(envelope)`, state machine `OutboxState::Pending → ...`; retry limit `DEFAULT_OUTBOX_MAX_RETRIES`, claim lease `DEFAULT_OUTBOX_CLAIM_LEASE`.
- `OutboxStore` contract has no transaction boundary — atomicity with handler's own persistence is guaranteed by store implementation or application.
- **Scheduled Outbox**: messages implement `DelayedMessage` (`scheduled_at()` takes precedence over `delay()`; `deliver_at(now)` parses the deadline), with `MemoryPackScheduledOutbox` persistence, published by processor after expiry. The declaration itself creates no timers.

## 2. Inbox and Idempotency (Read-Side Deduplication)

Transports are at-least-once; consumers must deduplicate:

- `InboxBehavior::new(store: Arc<dyn InboxStore>, codec)` — Deduplicates in pipeline by `InboxKey`; claim lease `DEFAULT_INBOX_CLAIM_LEASE`, `ProcessingState` records processing state.
- `IdempotencyBehavior::new(store: Arc<dyn IdempotencyStore>, codec)` — Request-side idempotency by `IdempotencyKey` (retention `DEFAULT_IDEMPOTENCY_RETENTION`).
- Selection: message consumption chain uses Inbox; external API/command entry uses Idempotency.

## 3. Dead Letter (Terminal Isolation)

- `DeadLetterStore` contract + `DeadLetter` / `DeadLetterDiagnostics`; description and stage names are bounded (`MAX_DEAD_LETTER_DESCRIPTION_BYTES` / `MAX_DEAD_LETTER_STAGE_BYTES`).
- `DeadLetterBehavior` (pipeline) or `CompetingConsumer` dead letter strategy (enter dead letter after `max_attempts` and ack, preventing infinite redelivery).
- Dead letter is an **operations entry point**: applications should provide inspection and replay paths, not let them silently accumulate.

## 4. Competing Consumer Loop

```rust,ignore
use catga_core::{CompetingConsumer, DeliveryHandler};

struct OrderWorker;
#[async_trait]
impl DeliveryHandler for OrderWorker {
    async fn handle(&self, envelope: &Envelope) -> CatgaResult<()> {
        // Ok(()) → ack; Err(..) → nack requests redelivery (no downtime)
    }
}

let consumer = CompetingConsumer::new(transport, Arc::new(OrderWorker), 8)?;  // Concurrency limit > 0
let run: ConsumerRun = consumer.run_until_cancelled(cancellation_token).await?;
// run.received() / acknowledged() / rejected() / dead_lettered()
```

- Competing consumer group membership is **transport configuration** (Redis consumer group / NATS durable consumer): starting multiple runners against the same configuration makes it distributed competing consumption.
- Ack ownership is in the consumer, not the handler — handler cannot prematurely acknowledge before side effects complete.

## 5. Persistent Subscriptions (Event Stream → Handlers)

```rust,ignore
use catga_core::{PersistentSubscription, SubscriptionLoopOptions};

// Match single stream, "prefix*" prefix, or "*" for all streams; can also filter by event type
let subscription = PersistentSubscription::new("order-projection", "order-*")
    .with_event_types(["OrderCreated", "OrderShipped"]);

// SubscriptionRunner (single instance) or CompetingSubscriptionRunner (multi-instance load sharing)
// Driven by application tasks; SubscriptionLoopOptions::new(poll_interval)? (non-zero)
```

- Per-stream checkpoint: `SubscriptionCheckpoint` + `SubscriptionStore` (implementations: `MemorySubscriptions` / `NatsSubscriptions` / `RedisSubscriptions`).
- `SubscriptionRun` reports per-round processing volume; loop options default to 100ms poll interval.

## 6. Lifecycle Points

1. All loops (`OutboxProcessor`, `CompetingConsumer`, `SubscriptionRunner`) accept `CancellationToken` and are spawned by application tasks — shutdown order is under your control (stop receiving first → drain → then stop storage).
2. Claim lease expiry releases incomplete work for other workers to take over; processing logic must be idempotent.
3. Retention cleanup is bounded: `validate_retention_cleanup_limit` / `MAX_RETENTION_CLEANUP_LIMIT`.
