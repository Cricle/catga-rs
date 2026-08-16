# Raft 实现统一到 sorock 设计文档

## 背景

当前 catga-rs 有两个 Raft 实现：
- `catga-cluster`: 基于 raft-rs + HTTP 传输
- `catga-sorock`: 基于 sorock + gRPC 传输

性能测试显示：
- 进程内 channel transport: ~70,000 writes/s
- HTTP API (distributed-kv): ~114 writes/s

sorock 的优势：
- gRPC + HTTP/2 多路复用
- 批量复制流
- 心跳多路复用
- redb 高效存储

## 变更计划

### 1. 删除 catga-cluster

**删除整个 crate**，功能完全由 catga-sorock 替代。

#### 删除的文件

```
crates/catga-cluster/
├── benches/                      # 全部删除
│   ├── flow_throughput.rs
│   ├── handler_dispatch.rs
│   ├── macro_expansion.rs
│   ├── mediator_throughput.rs
│   └── registry.rs
├── src/
│   ├── config.rs               # 配置结构
│   ├── consensus_bridge.rs      # raft-rs bridge
│   ├── dispatch.rs             # Transport 分发
│   ├── execution.rs             # ClusterCoordinatorExt
│   ├── forward.rs              # ClusterForwarder
│   ├── inbound.rs              # RaftInboundPolicy
│   ├── metrics.rs              # 指标
│   ├── raft.rs                # raft-rs RaftNode
│   ├── runtime.rs             # RaftRuntime (raft-rs)
│   ├── singleton_task.rs       # 依赖 Leader 观测
│   ├── state_machine.rs       # RaftStateMachine
│   ├── state_machine_runtime.rs # RaftStateMachineRuntime
│   └── storage.rs             # raft-engine 存储
├── tests/
│   ├── common/                # 保留通用测试工具
│   │   ├── channel_transport.rs
│   │   ├── recording_machine.rs
│   │   └── ...
│   ├── consensus_bridge.rs
│   ├── forward_leader.rs
│   ├── inbound_policy.rs
│   └── ... (其他测试)
└── Cargo.toml
```

#### 保留的文件

```
crates/catga-cluster/tests/common/
├── channel_transport.rs    # 保留（可移到 tests/）
└── recording_machine.rs   # 保留（可移到 tests/）
```

### 2. 删除的 API

| 删除的 API | 原因 |
|-----------|------|
| `RaftNode` | sorock 用自己的节点 |
| `RaftRuntime` | sorock 用 `SorockRuntime` |
| `RaftStateMachine` | sorock 用 `SorockApp` |
| `RaftStorage` | sorock 用 redb |
| `RaftTransport` | sorock 用固定 gRPC |
| `RaftInboundPolicy` | sorock 用 TLS |
| `ConsensusCoordinator` | sorock 不支持 Leader 观测 |
| `SingletonTaskRunner` | 依赖 Leader 观测 |
| `RaftClusterConfig` | sorock 用 `SorockNodeConfig` |

### 3. 保留的 API（在 catga-core 中）

这些 API 已经在 catga-core 中，不受变更影响：

```rust
// catga-core 中的 trait（保持不变）
pub trait ConsensusStateMachine: Send { ... }
pub trait ConsensusRuntime: Send + Sync { ... }

// 集群相关
pub use cluster::{ClusterHealth, LeaderOnlyBehavior, LeaderOnlyCommand, cluster_health};
```

### 4. catga-sorock 变更

#### 4.1 添加 SorockRuntimeBuilder

为 CLI/测试提供便捷构建方式：

```rust
// 新增 builder.rs
pub struct SorockRuntimeBuilder {
    // 类似现有的 SorockRuntimeBuilder
}

impl SorockRuntimeBuilder {
    pub fn from_cli(base_port: u16, node: u64, nodes: u64) -> CatgaResult<Self>;
    pub async fn start<M>(self, machine: M) -> CatgaResult<SorockRuntime>;
}
```

#### 4.2 保留的配置

```rust
// 保留的配置结构（适配到 sorock）
pub struct SorockNodeConfig {
    pub node_id: String,
    pub shard: u32,
    pub request_timeout: Duration,
    pub propose_retry: SorockProposeRetry,
    pub failover_watchdog: bool,
    pub snapshot_interval: u64,
}
```

### 5. 依赖关系变更

#### 变更前

```
catga-core/        - 无 raft 依赖
catga-cluster/   - catga-core + raft-rs + raft-engine + tonic
catga-sorock/    - catga-core + sorock + tonic
```

#### 变更后

```
catga-core/        - 无 raft 依赖（保持不变）
catga-sorock/    - catga-core + sorock + tonic（唯一的 Raft 实现）
```

### 6. catga-axum 变更

#### 删除的 API

```rust
// 删除
pub struct HttpRaftTransport { ... }  // 用 sorock 的 gRPC
pub struct RaftHttpCluster { ... }   // 用 SorockRuntime
```

#### 保留的 API

```rust
// HTTP 服务器相关（保留）
pub struct MediatorState { ... }
pub struct CorrelationHttpClient { ... }
pub struct HttpClusterForwarder { ... }  // 客户端转发
```

### 7. distributed-kv 示例变更

#### 变更前

```rust
pub enum Backend {
    Raft,    // catga-cluster
    Sorock,  // catga-sorock
}
```

#### 变更后

```rust
// 只保留 sorock
let runtime = SorockRuntimeBuilder::from_cli(...).start(kv_machine).await?;
```

### 8. 测试变更

#### 删除的测试

```
tests/
├── raft_kv_cluster.rs          # 依赖 catga-cluster
├── cluster_cqrs_flow.rs       # 依赖 catga-cluster
└── e2e_cluster.rs            # 依赖 catga-cluster
```

#### 保留/新增的测试

```
catga-sorock/tests/
├── write_perf.rs              # 保留
├── failover_tuning.rs         # 保留
├── multi_shard.rs            # 保留
└── ...
```

### 9. 文档变更

#### 更新的文档

| 文档 | 变更 |
|------|------|
| `wiki/zh/distributed/cluster.md` | 删除（功能合并到 sorock.md） |
| `wiki/zh/distributed/sorock.md` | 更新为主 Raft 文档 |
| `wiki/en/distributed/cluster.md` | 删除 |
| `wiki/en/distributed/sorock.md` | 更新为主 Raft 文档 |
| `CHANGELOG.md` | 记录本次变更 |

### 10. Workspace 变更

#### Cargo.toml

```toml
[workspace]
members = [
    "crates/catga-core",
    "crates/catga-flow-store",
    "crates/catga-memorypack-derive",
    "crates/catga-axum",      # 保留（移除 Raft 部分）
    "crates/catga-sorock",    # 唯一的 Raft 实现
    "crates/catga-nats",
    "crates/catga-redis",
    "crates/catga-robustmq",
    "tests",
    "examples",
    "examples/distributed-kv",
]
# 删除: "crates/catga-cluster"
```

## 实施步骤

### Step 1: 更新 distributed-kv 示例

1. 移除 `--backend raft` 选项
2. 使用 `SorockRuntimeBuilder`
3. 移除 HTTP Raft 路由

### Step 2: 更新 catga-axum

1. 移除 `HttpRaftTransport`
2. 移除 `RaftHttpCluster`
3. 保留 HTTP 服务器相关 API

### Step 3: 创建统一的 Raft 入口

在 `catga-sorock` 中提供统一的构建方式：

```rust
// catga-sorock/src/lib.rs
pub use app::SorockApp;
pub use builder::SorockRuntimeBuilder;
pub use config::{SorockNodeConfig, SorockStorage};
pub use coordinator::SorockCoordinator;
pub use node::SorockNode;
pub use runtime::SorockRuntime;
```

### Step 4: 迁移测试

1. 将 `catga-cluster/tests/common/` 移到 `tests/support/`
2. 更新 `tests/e2e_cluster.rs` 使用 sorock
3. 删除依赖 catga-cluster 的测试

### Step 5: 删除 catga-cluster

```bash
rm -rf crates/catga-cluster
```

### Step 6: 更新文档

1. 合并 cluster.md 到 sorock.md
2. 更新 README.md
3. 更新 CHANGELOG.md

## 风险和注意事项

### 1. Leader 不可观测

sorock 不支持 `ConsensusCoordinator`，应用无法直接判断是否为 Leader。

**影响**：无法做读写分离（读请求无法自动路由到 Leader）

**解决方案**：应用层自己管理，或者接受 eventual consistency

### 2. 固定心跳间隔

sorock 的心跳间隔固定为 300ms，无法调低。

**影响**：Follower 可见性延迟约 300-800ms

**解决方案**：接受这个限制

### 3. 固定 gRPC 传输

无法使用自定义传输层。

**影响**：无法用 UDP、自定义协议等

**解决方案**：使用 sorock 的 gRPC

## 总结

通过本次变更：
- 删除 ~13,000 行 raft-rs 代码
- 统一 Raft 实现为 sorock
- 简化依赖关系
- 获得更好的性能（gRPC + 批量）

**代价**：
- 失去 Leader 观测能力
- 失去自定义传输灵活性
- 失去细粒度配置能力
