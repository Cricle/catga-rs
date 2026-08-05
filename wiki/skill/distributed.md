# 分布式构件：集群 / Raft / 分布式 ID / 租约 / 任务调度

## 1. catga-cluster：集群协调

契约与实现分离：应用驱动 `RaftNode`/`RaftRuntime`，自备 `RaftTransport` 与持久化，crate 不创建网络监听。

### 协调契约（`ClusterCoordinator`）

```rust,ignore
use catga_cluster::{ClusterCoordinator, MemoryCluster};

// 确定性内存拓扑：测试与单进程组合（无后台网络）
let cluster = MemoryCluster::new("one", ["http://cluster/one", "http://cluster/two"]);
let node = cluster.node("one").expect("configured member");

node.node_id();
node.is_leader();
node.leader_endpoint();              // Option<Arc<str>>
node.leadership_snapshot();          // 当前快照（不订阅）
let mut sub = node.subscribe_leadership();   // 有界、非阻塞订阅
let snapshot = sub.recv().await?;    // 慢读者会被合并到最新快照：比较 epoch 重同步
node.member_endpoints();
node.wait_for_leadership(timeout).await;
```

- `LeadershipSnapshot { epoch, leader_node_id, leader_endpoint }` — `epoch` 单调递增，作为栅栏。
- `ClusterHealth` / `cluster_health()` — 健康文档（配合 HTTP `/healthz` 自行暴露）。
- `ClusterCoordinatorExt` — 协调器扩展方法。

### Leader-only 与转发

- `LeaderOnlyBehavior` / `LeaderOnlyCommand` — 只在 leader 上执行的 mediator 行为。
- `ForwardToLeaderBehavior` / `ClusterForwarder` — follower 把写请求转发给 leader（HTTP 实现：`HttpClusterForwarder`，见 [http.md](http.md)）。
- `SingletonTaskRunner` — 集群内只跑一份的任务（配合租约/leader 观察）。

### Raft

- 节点：`RaftNode` / `RaftClusterNode` / `RaftMember` / `RaftMessage` / `RaftCommittedEntry` / `RaftApplicationSnapshot`。
- 运行时：`RaftRuntime`（应用驱动 tick/消息泵）+ `RaftTransport` 契约（HTTP 实现 `HttpRaftTransport`）。
- 配置：`RaftClusterConfig` / `RaftClusterMemberConfig` / `RaftTiming`（校验错误 `RaftClusterConfigError`）。
- 状态机：`RaftStateMachine` / `RaftStateMachineDriver` / `RaftStateMachineRuntime`——committed entry 应用到**应用自己的**状态机。
- HTTP 入口安全（必须做）：`raft_message_route` 前置 mTLS 或签名帧认证 → 附加已验证的 `RaftPeerIdentity` → 用本节点与可信 peer 配置 `StaticRaftInboundPolicy`；入口帧上限 `MAX_RAFT_MESSAGE_BYTES = 1 MiB`。

### 安全准则（重要）

1. **Leadership 是观察，不是分布式锁**：对外可见的 leader 专属副作用，必须用 Raft term、应用版本或存储租约做栅栏，并保证幂等。
2. 订阅被合并是常态：消费者比较 `epoch` 并重同步，不要假设每次选举都被送达。
3. 先关停 `RaftRuntime`，再 drop 其 worker 可能访问的应用资源。

## 2. 分布式 ID（`catga-core`）

```rust,ignore
use catga_core::{DistributedIdGenerator, SnowflakeIdGenerator, SnowflakeLayout};

// 雪花 ID：布局为 时间戳位 + worker 位 + 序列位（总和必须 63），自定义 epoch
let layout = SnowflakeLayout::new(44, 8, 11, 1_704_067_200_000)?;   // 或 SnowflakeLayout::default()
let generator = SnowflakeIdGenerator::new(worker_id, layout)?;       // worker_id ≤ layout.max_worker_id()
let id: u64 = generator.next_id()?;                                  // 正 63 位 ID
generator.fill(&mut ids)?;                                           // 批量填充
generator.try_write_next_id(&mut buf)?;                              // 无分配十进制写入
let metadata = generator.parse(id);                                  // 解码时间戳/worker/序列
assert_eq!(metadata.worker_id(), worker_id);
```

用途：envelope 消息 id、幂等键、outbox 记录 id 等跨进程唯一标识。`TypedTransport::new(transport, id_generator)` 需要一个（见 [transport.md](transport.md)）。

## 3. 租约（`LeaseStore`）

```rust,ignore
#[async_trait]
pub trait LeaseStore: Send + Sync {
    async fn try_acquire(&self, resource: &str, owner: &str, ttl: Duration) -> CatgaResult<bool>;
    async fn renew(&self, resource: &str, owner: &str, ttl: Duration) -> CatgaResult<bool>;  // 仅属主
    async fn release(&self, resource: &str, owner: &str) -> CatgaResult<bool>;               // 仅属主
}
```

实现：`MemoryLeases` / `NatsLeases` / `RedisLeases`。用途：单例任务、leader 职责栅栏、`DistributedLockBehavior`。锁失败（`LockFailed`）**刻意不可重试**——调用方无法推断所有权。

## 4. 任务调度

### 契约（`catga-core`）

```rust,ignore
#[async_trait]
pub trait ScheduledTask: Send + Sync { async fn execute(&self) -> CatgaResult<()>; }

#[async_trait]
pub trait TaskScheduler: Send + Sync {
    async fn schedule(..) -> CatgaResult<ScheduledTaskId>;
    async fn cancel(&self, task_id: &ScheduledTaskId) -> CatgaResult<()>;
}
```

- `TaskSchedule`（含 cron 表达式）；`ScheduledTaskId::new(..)?`。
- 有界：`MAX_CRON_SCHEDULE_BYTES = 512`、`MAX_SCHEDULED_TASK_ID_BYTES = 256`。

### tokio-cron 适配器（`catga-scheduler-tokio-cron`）

```rust,ignore
use catga_scheduler_tokio_cron::{CronRuntime, flow_due_job};

let runtime = CronRuntime::new().await?;          // 构造不启动
let job = flow_due_job("0/5 * * * * *", due_service.clone())?;  // 每 tick 恰好一次有界 FlowDueService::check_at
let job_id = runtime.add(job).await?;
runtime.start().await?;                           // 显式启动（此时才创建调度任务）
// ... 关停：runtime.shutdown().await?;（drop 前调用）
```

- `flow_due_job` 刻意**不**调用 `FlowDueService::run`：cron 频率是应用策略，每次回调仍受 `DueFlowOptions::batch_size` 约束；失败记录日志留待下一 tick。
- 该适配器不持久化 job、不装信号处理器；需要持久化 cron 时直接使用上游 `JobScheduler`（已重导出 `Job` / `JobScheduler` / `JobId`）。
