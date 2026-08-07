# Distributed Components: Cluster / Raft / Distributed ID / Lease / Task Scheduling

## 1. catga-cluster: Cluster Coordination

Contract and implementation are separated: applications drive `RaftNode`/`RaftRuntime`, provide their own `RaftTransport` and persistence, and the crate does not create network listeners.

### Coordination Contract (`ClusterCoordinator`)

```rust,ignore
use catga_cluster::{ClusterCoordinator, MemoryCluster};

// Deterministic in-memory topology: testing and single-process composition (no background network)
let cluster = MemoryCluster::new("one", ["http://cluster/one", "http://cluster/two"]);
let node = cluster.node("one").expect("configured member");

node.node_id();
node.is_leader();
node.leader_endpoint();              // Option<Arc<str>>
node.leadership_snapshot();          // Current snapshot (not subscribed)
let mut sub = node.subscribe_leadership();   // Bounded, non-blocking subscription
let snapshot = sub.recv().await?;    // Slow readers are merged into latest snapshot: compare epoch to resync
node.member_endpoints();
node.wait_for_leadership(timeout).await;
```

- `LeadershipSnapshot { epoch, leader_node_id, leader_endpoint }` — `epoch` monotonically increases as a barrier.
- `ClusterHealth` / `cluster_health()` — Health document (expose yourself via HTTP `/healthz`).
- `ClusterCoordinatorExt` — Coordinator extension methods.

### Leader-only and Forwarding

- `LeaderOnlyBehavior` / `LeaderOnlyCommand` — Execute only on leader mediator behavior.
- `ForwardToLeaderBehavior` / `ClusterForwarder` — Followers forward write requests to leader (HTTP implementation: `HttpClusterForwarder`, see [http.md](http.md)).
- `SingletonTaskRunner` — Tasks that run only once within a cluster (use with lease/leader observation).

### Raft

- Nodes: `RaftNode` / `RaftClusterNode` / `RaftMember` / `RaftMessage` / `RaftCommittedEntry` / `RaftApplicationSnapshot`.
- Runtime: `RaftRuntime` (application-driven tick/message pump) + `RaftTransport` contract (HTTP implementation `HttpRaftTransport`).
- Configuration: `RaftClusterConfig` / `RaftClusterMemberConfig` / `RaftTiming` (validation error `RaftClusterConfigError`).
- State machine: `RaftStateMachine` / `RaftStateMachineDriver` / `RaftStateMachineRuntime` — committed entries are applied to **the application's own** state machine.
- HTTP endpoint security (mandatory): Prepend mTLS or signed frame authentication to `raft_message_route` → attach verified `RaftPeerIdentity` → configure `StaticRaftInboundPolicy` with this node and trusted peers; inbound frame limit `MAX_RAFT_MESSAGE_BYTES = 1 MiB`.

### Security Guidelines (Important)

1. **Leadership is observation, not a distributed lock**: For externally visible leader-exclusive side effects, use Raft term, application version, or storage lease as a barrier, and ensure idempotency.
2. Subscription merging is normal: Consumers compare `epoch` and resync; do not assume every election is delivered.
3. Shutdown `RaftRuntime` first, then drop application resources that its workers may access.

## 2. Distributed ID (`catga-core`)

```rust,ignore
use catga_core::{DistributedIdGenerator, SnowflakeIdGenerator, SnowflakeLayout};

// Snowflake ID: layout is timestamp bits + worker bits + sequence bits (total must be 63), custom epoch
let layout = SnowflakeLayout::new(44, 8, 11, 1_704_067_200_000)?;   // or SnowflakeLayout::default()
let generator = SnowflakeIdGenerator::new(worker_id, layout)?;       // worker_id <= layout.max_worker_id()
let id: u64 = generator.next_id()?;                                  // Positive 63-bit ID
generator.fill(&mut ids)?;                                           // Batch fill
generator.try_write_next_id(&mut buf)?;                              // Zero-allocation decimal write
let metadata = generator.parse(id);                                  // Decode timestamp/worker/sequence
assert_eq!(metadata.worker_id(), worker_id);
```

Use cases: Cross-process unique identifiers for envelope message IDs, idempotency keys, outbox record IDs, etc. `TypedTransport::new(transport, id_generator)` requires one (see [transport.md](transport.md)).

## 3. Lease (`LeaseStore`)

```rust,ignore
#[async_trait]
pub trait LeaseStore: Send + Sync {
    async fn try_acquire(&self, resource: &str, owner: &str, ttl: Duration) -> CatgaResult<bool>;
    async fn renew(&self, resource: &str, owner: &str, ttl: Duration) -> CatgaResult<bool>;  // Owner only
    async fn release(&self, resource: &str, owner: &str) -> CatgaResult<bool>;               // Owner only
}
```

Implementations: `MemoryLeases` / `NatsLeases` / `RedisLeases`. Use cases: singleton tasks, leader duty barriers, `DistributedLockBehavior`. Lock failure (`LockFailed`) is **intentionally non-retryable** — the caller cannot infer ownership.

## 4. Task Scheduling

### Contract (`catga-core`)

```rust,ignore
#[async_trait]
pub trait ScheduledTask: Send + Sync { async fn execute(&self) -> CatgaResult<()>; }

#[async_trait]
pub trait TaskScheduler: Send + Sync {
    async fn schedule(..) -> CatgaResult<ScheduledTaskId>;
    async fn cancel(&self, task_id: &ScheduledTaskId) -> CatgaResult<()>;
}
```

- `TaskSchedule` (includes cron expression); `ScheduledTaskId::new(..)?`.
- Bounded: `MAX_CRON_SCHEDULE_BYTES = 512`, `MAX_SCHEDULED_TASK_ID_BYTES = 256`.

### tokio-cron Adapter (`catga-scheduler-tokio-cron`)

```rust,ignore
use catga_scheduler_tokio_cron::{CronRuntime, flow_due_job};

let runtime = CronRuntime::new().await?;          // Construction does not start
let job = flow_due_job("0/5 * * * * *", due_service.clone())?;  // Exactly one bounded FlowDueService::check_at per tick
let job_id = runtime.add(job).await?;
runtime.start().await?;                           // Explicit start (scheduler task created here)
// ... shutdown: runtime.shutdown().await?; (call before drop)
```

- `flow_due_job` intentionally **does not** call `FlowDueService::run`: cron frequency is application policy, each callback is still constrained by `DueFlowOptions::batch_size`; failures are logged and retried on next tick.
- This adapter does not persist jobs or install signal handlers; for persistent cron, use the upstream `JobScheduler` directly (re-exported `Job` / `JobScheduler` / `JobId`).
