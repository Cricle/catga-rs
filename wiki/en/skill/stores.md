# Stores: Persistent Storage

## catga-flow-store (SQL / Redis durable flow state)

A public crate that compiles only selected database drivers by feature:

| Backend | Feature | Description |
| --- | --- | --- |
| SQLite | `sqlite` | Embedded; WAL enabled + `synchronous=NORMAL` at construction |
| MySQL | `mysql` | Native SQLx MySQL pool |
| PostgreSQL | `postgres` | Native SQLx PostgreSQL pool |
| SQL Server | `mssql` | Bounded Tiberius pool (`MssqlPool`) |
| Redis | `redis` | Re-exports `RedisFlows` / `RedisSuspendedFlows` |
| Rustls | `tls-rustls` | Pairs with network SQL features |

Provided store types (same contract for each SQL backend):

| Type | Implemented Contract |
| --- | --- |
| `SqlFlowStore` | `FlowStore` (simple flow state) |
| `SqlSuspendedFlowStore` | `SuspendedFlowStore` (durable flow suspend/resume, version fencing) |
| `SqlFlowScheduler` | `DueFlowScheduler` (due scheduling, application polls `claim_due`) |
| `SqlStateMachineStore` | `StateMachineStore` |
| `SqlDslStepProgressStore` | `DslStepProgressStore` (DSL checkpoint) |

### Connection and Migration

```rust,ignore
use catga_flow_store::{SqlFlowStore, SqlSuspendedFlowStore};

// Convenience constructors (each store builds its own connection pool)
let store = SqlSuspendedFlowStore::connect_sqlite("sqlite:catga.db").await?;
// Other backends: connect_mysql / connect_postgres / connect_mssql (url: &str)

// Call migrate during controlled startup — idempotent, but must complete before flow processing
store.migrate().await?;

// Production recommendation: reuse application's own connection pool, unify connection budget
let pool = sqlx::sqlite::SqlitePoolOptions::new()
    .max_connections(12)
    .connect("sqlite:catga.db")
    .await?;
let store = SqlFlowStore::from_sqlite_pool(pool);   // from_mysql_pool / from_postgres_pool / from_mssql_pool
store.migrate().await?;

// When you don't want to expose driver type: connect_*_with_options(url, SqlFlowStoreOptions)
```

Rules:
1. `migrate()` is called during **controlled startup phase**, successful completion before flow processing begins.
2. Adapters **do not create** workers/timers — `SqlFlowScheduler`'s `claim_due` is explicitly polled by application workers (usually with `FlowDueService`).
3. Multiple SQL features can coexist in the same binary; constructors select pools by type, no dynamic SQL.
4. Write throughput is dominated by fsync per commit: increase concurrency (database group commit) or batch writes rather than disable persistence.

## catga-memory (Testing and Local Composition)

Bounded in-memory implementations of all store contracts (capacity limit `DEFAULT_MEMORY_RECORD_CAPACITY`):

`MemoryFlows`, `MemoryOutbox`, `MemoryInbox`, `MemoryIdempotency`, `MemoryEventStore`, `MemoryLeases`, `MemorySnapshots`, `MemoryEnhancedSnapshots`, `MemoryProjectionCheckpoints`, `MemoryReadModels`, `MemoryChangeTracker`, `MemoryDeadLetters`, `MemoryStateMachines`, `MemoryDslStepProgress`, `MemoryPubSubTransport`.

**Recommended workflow**: Write applications and unit tests with these in-memory implementations first, replace with SQL/Redis/NATS implementations of the same contract in production — application code stays the same.

## NATS (`catga-nats`) and Redis (`catga-redis`) Storage Capabilities

Each provides its own storage family (construct via `connect(..)`, Redis form is `connect(server, prefix, ..)`):

| Capability | NATS | Redis | Contract (catga-core) |
| --- | --- | --- | --- |
| Event Sourcing | `NatsEventStore` | `RedisEventStore` | `EventStore` (page limit `MAX_EVENT_STORE_PAGE_SIZE`) |
| Outbox | `NatsOutbox` | `RedisOutbox` | `OutboxStore` |
| Inbox (consumer deduplication) | `NatsInbox` | `RedisInbox` | `InboxStore` |
| Idempotency key | `NatsIdempotency` | `RedisIdempotency` | `IdempotencyStore` |
| Lease/distributed lock | `NatsLeases` | `RedisLeases` | `LeaseStore` |
| Flow state/suspend | `NatsFlows` | `RedisFlows` / `RedisSuspendedFlows` | `FlowStore` / `SuspendedFlowStore` |
| Flow scheduling | `NatsFlowScheduler` | `RedisFlowScheduler` | `DueFlowScheduler` |
| Projection checkpoint | `NatsProjectionCheckpoints` | `RedisProjectionCheckpoints` | `ProjectionCheckpointStore` |
| Snapshots | `NatsEnhancedSnapshots` | `RedisEnhancedSnapshots` | `EnhancedSnapshotStore` |
| Dead letter | `NatsDeadLetters` | `RedisDeadLetters` | `DeadLetterStore` |
| DSL progress | `NatsDslStepProgress` | `RedisDslStepProgress` | `DslStepProgressStore` |

## Codecs

- `catga-codec-memorypack`: Default bounded MemoryPack codec (envelopes, snapshots, RPC frames).
- `catga-codec-bincode`: Standalone `bincode-next` payload codec.
- `catga-core` contracts: `PayloadEncoder` / `PayloadDecoder` / `EnvelopeCodec` / `SnapshotCodec` / `CachedResultCodec` — custom implementations can plug in.

## Event Sourcing (`catga-core`)

- `Aggregate` + `AggregateRepository` + `EventStore`: Aggregate root persistence; `StoredEvent`, `EventPage`.
- Snapshots: `SnapshotStore` / `EnhancedSnapshotStore`; strategies `EventCountSnapshotStrategy` / `TimeBasedSnapshotStrategy` / `CompositeSnapshotStrategy`; `AutoSnapshotManager`.
- Event upgrading: `EventVersionRegistry` + `EventUpgrader` + `UpgradingEventStore` (upgrades old-version events when read).
- Projections and read models: `Projection` / `LiveProjection` / `CatchUpProjectionRunner`, `ReadModelStore` / `ReadModelSynchronizer` (`MAX_READ_MODEL_PAGE_SIZE`).
- Time travel: `SnapshotTimeTravelService` / `TimeTravelService`.
