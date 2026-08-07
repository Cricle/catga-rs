# Event Sourcing, Projection, and Read Model

`catga-core` provides complete Event Sourcing (ES) contracts; backend implementations are in [stores.md](stores.md) (`MemoryEventStore` / `NatsEventStore` / `RedisEventStore`).

## 1. Aggregate

```rust,ignore
use catga_core::{Aggregate, CatgaResult, Envelope};

#[derive(Clone)]
struct Order { id: String, version: i64, pending: Vec<Envelope>, /* business fields */ }

impl Aggregate for Order {
    fn new(id: &str) -> Self { /* version = -1 (no events), pending is empty */ }
    fn stream_id(id: &str) -> Box<str> { format!("order-{id}").into() }  // Stable stream name
    fn id(&self) -> &str { &self.id }
    fn version(&self) -> i64 { self.version }          // Zero-based version of most recent applied, initial -1
    fn apply(&mut self, event: &Envelope) -> CatgaResult<()> {
        // Decode event, mutate state, version += 1
    }
    fn pending_events(&self) -> &[Envelope] { &self.pending }   // Applied but not yet persisted
    fn clear_pending_events(&mut self) { self.pending.clear() }
}
```

## 2. AggregateRepository (Snapshot-aware)

```rust,ignore
use catga_core::{AggregateRepository, EventCountSnapshotStrategy};

let strategy = EventCountSnapshotStrategy::new(100).expect("nonzero");
let repository = AggregateRepository::<Order, _, _>::new(&event_store, &snapshot_store, strategy);

// Load: latest snapshot + replay subsequent events; no snapshot and no events -> Ok(None)
let mut order = repository.load("42").await?.unwrap_or_else(|| Order::new("42"));
// ... business operations produce pending events ...
repository.save(&mut order).await?;   // Optimistic append by original stream version (conflict -> ErrorCode::Conflict)
```

Snapshot strategies (`SnapshotStrategy` / standalone functional):

- `EventCountSnapshotStrategy::new(interval)` — Every N new events (`Option` return, 0 -> `None`).
- `TimeBasedSnapshotStrategy::new(duration)` — Fixed time elapsed since last snapshot.
- `CompositeSnapshotStrategy::new(events, time)` — Both combined.
- `AutoSnapshotManager` — Automatic snapshot management.

## 3. EventStore Contract

```rust,ignore
#[async_trait]
pub trait EventStore: Send + Sync {
    async fn append(..) -> CatgaResult<()>;                                  // Optimistic concurrent append
    async fn read_page(..) -> CatgaResult<EventPage>;                        // Paginated read from cursor
    async fn version(&self, stream_id: &str) -> CatgaResult<i64>;
    async fn read_to_version_page(..) -> CatgaResult<EventPage>;             // Read to specified version
    async fn read_to_time_page(..) -> CatgaResult<EventPage>;                // Read to specified time
    async fn version_history_page(..) -> CatgaResult<VersionHistoryPage>;    // Lightweight metadata
    async fn stream_ids_page(..) -> CatgaResult<StreamIdsPage>;              // Stream discovery
}
```

- Hard limit `MAX_EVENT_STORE_PAGE_SIZE = 1024`; backends validate with `validate_event_store_page_size`.
- `StoredEvent`: `version()` (zero-based) / `envelope()` (`Arc<Envelope>`, zero-copy) / `timestamp()`.
- `EventPage` has `next_version()` cursor — to process more records you must follow the cursor; memory and stream history are decoupled.

## 4. Event Upgrade (Schema Evolution)

Old-version events are upgraded to the new model on read:

- `EventUpgrader` — Single upgrade step (v_n -> v_{n+1}).
- `EventVersionRegistry` — Register upgrade chains.
- `UpgradingEventStore` — Wrap any `EventStore`, read path automatically upgrades along the chain.

On the message side, use `Message::schema_version()` (or `#[catga(schema_version = 2)]`) to declare version.

## 5. Time Travel

- `TimeTravelService` — Contract; `SnapshotTimeTravelService` — Implementation based on snapshot + event replay.
- `StateComparison` — Compare state at two points in time (limit `MAX_STATE_COMPARISON_EVENTS`).

## 6. Projection

```rust,ignore
#[async_trait]
pub trait Projection: Send + Sync {
    async fn apply(&self, event: &StoredEvent) -> CatgaResult<()>;  // Incremental application
    async fn reset(&self) -> CatgaResult<()>;                       // Clear before rebuild
}
```

- `ProjectionCheckpoint` (`ProjectionCheckpoint::new(..)` / `from_persisted(..)`, keyed by `projection_name + stream_id`) records progress.
- `ProjectionCheckpointStore`: `save` / `load` / `delete` / `delete_all`.
- `CatchUpProjectionRunner::new(&events, &checkpoints, &projection)` (can `with_batch_size(..)`) — Catch-up rebuild; `ProjectionRun` reports progress.
- `LiveProjection` — Live projection contract.
- Projection runners are also **tasks owned by the application**, not background daemons.

The NATS EventStore projection can use `NatsProjectionRunner` to consolidate connection, KV checkpoint, and catch-up replay; it only reads EventStore, not transport messages, so it won't mistake JetStream consumer cursor for read-model progress:

```rust,ignore
use catga_nats::{NatsProjectionConfig, NatsProjectionRunner};

let runner = NatsProjectionRunner::connect(
    "nats://127.0.0.1:4222",
    NatsProjectionConfig {
        event_stream: "ORDER_EVENTS".into(),
        event_subject_prefix: "orders.events".into(),
        checkpoint_bucket: "ORDER_TOTALS_CHECKPOINTS".into(),
    },
    OrderTotalsProjection::default(),
).await?;
runner.run().await?;       // Apply only events after checkpoint
// runner.rebuild().await?; // Clear read model and checkpoint, then full replay
```

Live notifications still use `CompetingConsumer`; when running both the runner and consumer simultaneously, read model updates must remain idempotent, because both live delivery and subsequent catch-up may see the same event.

## 7. Read Model Sync (ReadModel)

Sync write-side changes to query-side storage:

- `ReadModelStore` — Read model CRUD/pagination (`MAX_READ_MODEL_PAGE_SIZE = 1024`, `validate_read_model_page_size`).
- `ChangeTracker`: `pending_page(max_count)` gets pending changes, `mark_synced(&change_ids)` marks complete; `ChangeRecord` / `ChangeKind` describe changes.
- `SyncStrategy` — Strategy for applying changes:
  - `RealtimeSyncStrategy::new(action)` — Apply immediately one by one.
  - `BatchSyncStrategy` — Apply in batches.
  - `ScheduledSyncStrategy::new(interval, action)` — Apply on schedule.
- `ReadModelSynchronizer` — Synchronizer combining tracker + strategy + store.

## 8. Persistent Subscription (Cross-stream Consumption)

See `PersistentSubscription` / `SubscriptionRunner` in [reliability.md](reliability.md) — subscriptions are the bridge between event-sourcing read side and transport consumption.

## Writing Rules

1. `apply` must be **deterministic** and only perform state mutations; side effects belong in projection/subscription handlers.
2. Optimistic concurrent conflict (`Conflict`) from `save` is a normal signal: re-`load` and retry business decisions, don't blindly overwrite.
3. Snapshots are only for acceleration: the true source of truth is always the event stream; snapshot version must match aggregate `version()` (mismatch -> `Validation`).
4. Replay/projection memory usage is determined by page size; don't bypass `MAX_EVENT_STORE_PAGE_SIZE` to build a full read.
