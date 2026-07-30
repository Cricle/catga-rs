# 事件溯源、投影与读模型

`catga-core` 提供完整的事件溯源（ES）契约；后端实现见 [stores.md](stores.md)（`MemoryEventStore` / `NatsEventStore` / `RedisEventStore`）。

## 1. 聚合（Aggregate）

```rust,ignore
use catga_core::{Aggregate, CatgaResult, Envelope};

#[derive(Clone)]
struct Order { id: String, version: i64, pending: Vec<Envelope>, /* 业务字段 */ }

impl Aggregate for Order {
    fn new(id: &str) -> Self { /* version = -1（无事件），pending 为空 */ }
    fn stream_id(id: &str) -> Box<str> { format!("order-{id}").into() }  // 稳定流名
    fn id(&self) -> &str { &self.id }
    fn version(&self) -> i64 { self.version }          // 最近应用的零基版本，初始 -1
    fn apply(&mut self, event: &Envelope) -> CatgaResult<()> {
        // 解码事件、变更状态、version += 1
    }
    fn pending_events(&self) -> &[Envelope] { &self.pending }   // 已应用未持久化
    fn clear_pending_events(&mut self) { self.pending.clear() }
}
```

## 2. AggregateRepository（快照感知）

```rust,ignore
use catga_core::{AggregateRepository, EventCountSnapshotStrategy};

let strategy = EventCountSnapshotStrategy::new(100).expect("nonzero");
let repository = AggregateRepository::<Order, _, _>::new(&event_store, &snapshot_store, strategy);

// 加载：最新快照 + 其后事件重放；无快照且无事件 → Ok(None)
let mut order = repository.load("42").await?.unwrap_or_else(|| Order::new("42"));
// ... 业务操作产生 pending events ...
repository.save(&mut order).await?;   // 按原始流版本乐观追加（冲突 → ErrorCode::Conflict）
```

快照策略（`SnapshotStrategy` / 独立函数式）：

- `EventCountSnapshotStrategy::new(interval)` — 每 N 个新事件（`Option` 返回，0 → `None`）。
- `TimeBasedSnapshotStrategy::new(duration)` — 距上次快照经过固定时长。
- `CompositeSnapshotStrategy::new(events, time)` — 两者组合。
- `AutoSnapshotManager` — 自动快照管理。

## 3. EventStore 契约

```rust,ignore
#[async_trait]
pub trait EventStore: Send + Sync {
    async fn append(..) -> CatgaResult<()>;                                  // 乐观并发追加
    async fn read_page(..) -> CatgaResult<EventPage>;                        // 从游标分页读
    async fn version(&self, stream_id: &str) -> CatgaResult<i64>;
    async fn read_to_version_page(..) -> CatgaResult<EventPage>;             // 读到指定版本
    async fn read_to_time_page(..) -> CatgaResult<EventPage>;                // 读到指定时间
    async fn version_history_page(..) -> CatgaResult<VersionHistoryPage>;    // 轻量元数据
    async fn stream_ids_page(..) -> CatgaResult<StreamIdsPage>;              // 流发现
}
```

- 分页硬上限 `MAX_EVENT_STORE_PAGE_SIZE = 1024`；后端用 `validate_event_store_page_size` 校验。
- `StoredEvent`：`version()`（零基）/ `envelope()`（`Arc<Envelope>`，零拷贝）/ `timestamp()`。
- `EventPage` 带 `next_version()` 游标——处理更多记录必须跟随游标，内存与流历史解耦。

## 4. 事件升级（版本演化）

老版本事件在读取时升级到新模型：

- `EventUpgrader` — 单个升级步骤（vₙ → vₙ₊₁）。
- `EventVersionRegistry` — 注册升级链。
- `UpgradingEventStore` — 包装任意 `EventStore`，读路径自动沿链升级。

消息侧用 `Message::schema_version()`（或 `#[catga(schema_version = 2)]`）声明版本。

## 5. 时间旅行

- `TimeTravelService` — 契约；`SnapshotTimeTravelService` — 基于快照 + 事件重放的实现。
- `StateComparison` — 两个时间点状态对比（上限 `MAX_STATE_COMPARISON_EVENTS`）。

## 6. 投影（Projection）

```rust,ignore
#[async_trait]
pub trait Projection: Send + Sync {
    async fn apply(&self, event: &StoredEvent) -> CatgaResult<()>;  // 增量应用
    async fn reset(&self) -> CatgaResult<()>;                       // 重建前清空
}
```

- `ProjectionCheckpoint`（`ProjectionCheckpoint::new(..)` / `from_persisted(..)`，按 `projection_name + stream_id` 键控）记录进度。
- `ProjectionCheckpointStore`：`save` / `load` / `delete` / `delete_all`。
- `CatchUpProjectionRunner::new(&events, &checkpoints, &projection)`（可 `with_batch_size(..)`）— 追赶式重建；`ProjectionRun` 报告进度。
- `LiveProjection` — 实时投影契约。
- 投影 runner 同样是**应用拥有的任务**，不是后台守护。

## 7. 读模型同步（ReadModel）

把写侧变更同步到查询侧存储：

- `ReadModelStore` — 读模型 CRUD/分页（`MAX_READ_MODEL_PAGE_SIZE = 1024`，`validate_read_model_page_size`）。
- `ChangeTracker`：`pending_page(max_count)` 取待同步变更、`mark_synced(&change_ids)` 标记完成；`ChangeRecord` / `ChangeKind` 描述变更。
- `SyncStrategy` — 应用变更的策略：
  - `RealtimeSyncStrategy::new(action)` — 立即逐条应用。
  - `BatchSyncStrategy` — 批量应用。
  - `ScheduledSyncStrategy::new(interval, action)` — 定时应用。
- `ReadModelSynchronizer` — 组合 tracker + strategy + store 的同步器。

## 8. 持久订阅（跨流消费）

见 [reliability.md](reliability.md) 的 `PersistentSubscription` / `SubscriptionRunner`——订阅是事件溯源读侧与传输消费之间的桥梁。

## 编写规则

1. `apply` 必须**确定性**且只做状态变更；副作用放在投影/订阅处理器里。
2. `save` 的乐观并发冲突（`Conflict`）是正常信号：重新 `load` 后重试业务决策，而不是盲目覆盖。
3. 快照只是加速：状态真源永远是事件流；快照版本必须与聚合 `version()` 一致（不一致 → `Validation`）。
4. 重放/投影内存占用由页大小决定；不要绕过 `MAX_EVENT_STORE_PAGE_SIZE` 自制全量读取。
