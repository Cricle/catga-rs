# Stores：持久化存储

## catga-flow-store（SQL / Redis durable flow 状态）

一个公开 crate，按 feature 只编译选中的数据库驱动：

| 后端 | feature | 说明 |
| --- | --- | --- |
| SQLite | `sqlite` | 嵌入式；构造时启用 WAL + `synchronous=NORMAL` |
| MySQL | `mysql` | 原生 SQLx MySQL 池 |
| PostgreSQL | `postgres` | 原生 SQLx PostgreSQL 池 |
| SQL Server | `mssql` | 有界 Tiberius 池（`MssqlPool`） |
| Redis | `redis` | 重导出 `RedisFlows` / `RedisSuspendedFlows` |
| Rustls | `tls-rustls` | 与网络 SQL feature 搭配 |

提供的 store 类型（每种 SQL 后端同一套契约）：

| 类型 | 实现的契约 |
| --- | --- |
| `SqlFlowStore` | `FlowStore`（简单 flow 状态） |
| `SqlSuspendedFlowStore` | `SuspendedFlowStore`（durable flow 挂起/恢复，版本栅栏） |
| `SqlFlowScheduler` | `DueFlowScheduler`（到期调度，应用轮询 `claim_due`） |
| `SqlStateMachineStore` | `StateMachineStore` |
| `SqlDslStepProgressStore` | `DslStepProgressStore`（DSL checkpoint） |

### 连接与迁移

```rust,ignore
use catga_flow_store::{SqlFlowStore, SqlSuspendedFlowStore};

// 便捷构造（每个 store 自建连接池）
let store = SqlSuspendedFlowStore::connect_sqlite("sqlite:catga.db").await?;
// 其他后端：connect_mysql / connect_postgres / connect_mssql（url: &str）

// 受控启动阶段调用 migrate——幂等，但必须先于 flow 处理完成
store.migrate().await?;

// 生产推荐：复用应用自己的连接池，统一连接预算
let pool = sqlx::sqlite::SqlitePoolOptions::new()
    .max_connections(12)
    .connect("sqlite:catga.db")
    .await?;
let store = SqlFlowStore::from_sqlite_pool(pool);   // from_mysql_pool / from_postgres_pool / from_mssql_pool
store.migrate().await?;

// 不想暴露驱动类型时：connect_*_with_options(url, SqlFlowStoreOptions)
```

规则：
1. `migrate()` 在**受控启动阶段**调用，成功后再开始处理 flow。
2. 适配器**不创建** worker/timer——`SqlFlowScheduler` 的 `claim_due` 由应用 worker 显式轮询（通常配合 `FlowDueService`）。
3. 多 SQL feature 可在同一二进制共存；构造函数按类型选择池，无动态 SQL。
4. 写吞吐受每次提交的 fsync 支配：提并发（数据库 group commit）或批量写入比关持久化更安全。

## catga-memory（测试与本地组合）

全部 store 契约的有界内存实现（容量上限 `DEFAULT_MEMORY_RECORD_CAPACITY`）：

`MemoryFlows`、`MemoryOutbox`、`MemoryInbox`、`MemoryIdempotency`、`MemoryEventStore`、`MemoryLeases`、`MemorySnapshots`、`MemoryEnhancedSnapshots`、`MemoryProjectionCheckpoints`、`MemoryReadModels`、`MemoryChangeTracker`、`MemoryDeadLetters`、`MemoryStateMachines`、`MemoryDslStepProgress`、`MemoryPubSubTransport`。

**推荐工作流**：先用这些内存实现写应用与单元测试，生产环境换成同契约的 SQL/Redis/NATS 实现，应用代码不变。

## NATS（`catga-nats`）与 Redis（`catga-redis`）的存储能力

两者各自提供同名存储家族（构造均为 `connect(..)`，Redis 形态为 `connect(server, prefix, ..)`）：

| 能力 | NATS | Redis | 契约（catga-core） |
| --- | --- | --- | --- |
| 事件溯源 | `NatsEventStore` | `RedisEventStore` | `EventStore`（分页上限 `MAX_EVENT_STORE_PAGE_SIZE`） |
| Outbox | `NatsOutbox` | `RedisOutbox` | `OutboxStore` |
| Inbox（消费去重） | `NatsInbox` | `RedisInbox` | `InboxStore` |
| 幂等键 | `NatsIdempotency` | `RedisIdempotency` | `IdempotencyStore` |
| 租约/分布式锁 | `NatsLeases` | `RedisLeases` | `LeaseStore` |
| Flow 状态/挂起 | `NatsFlows` | `RedisFlows` / `RedisSuspendedFlows` | `FlowStore` / `SuspendedFlowStore` |
| Flow 调度 | `NatsFlowScheduler` | `RedisFlowScheduler` | `DueFlowScheduler` |
| 投影 checkpoint | `NatsProjectionCheckpoints` | `RedisProjectionCheckpoints` | `ProjectionCheckpointStore` |
| 快照 | `NatsEnhancedSnapshots` | `RedisEnhancedSnapshots` | `EnhancedSnapshotStore` |
| 死信 | `NatsDeadLetters` | `RedisDeadLetters` | `DeadLetterStore` |
| DSL 进度 | `NatsDslStepProgress` | `RedisDslStepProgress` | `DslStepProgressStore` |

## 编解码

- `catga-codec-memorypack`：默认有界 MemoryPack 编解码（envelope、快照、RPC 帧）。
- `catga-codec-bincode`：独立的 `bincode-next` payload codec。
- `catga-core` 契约：`PayloadEncoder` / `PayloadDecoder` / `EnvelopeCodec` / `SnapshotCodec` / `CachedResultCodec`——可自定义实现接入。

## 事件溯源（`catga-core`）

- `Aggregate` + `AggregateRepository` + `EventStore`：聚合根持久化；`StoredEvent`、`EventPage`。
- 快照：`SnapshotStore` / `EnhancedSnapshotStore`；策略 `EventCountSnapshotStrategy` / `TimeBasedSnapshotStrategy` / `CompositeSnapshotStrategy`；`AutoSnapshotManager`。
- 事件升级：`EventVersionRegistry` + `EventUpgrader` + `UpgradingEventStore`（老版本事件读取时升级）。
- 投影与读模型：`Projection` / `LiveProjection` / `CatchUpProjectionRunner`、`ReadModelStore` / `ReadModelSynchronizer`（`MAX_READ_MODEL_PAGE_SIZE`）。
- 时间旅行：`SnapshotTimeTravelService` / `TimeTravelService`。
