# Raft 实现替换：catga-sorock → catga-raft

## 目标

用 TiKV 的 raft + raft-engine 替换 catga-sorock，实现完整的 TiKV 风格 Pipeline 异步复制。

## 技术栈

| 组件 | 来源 | 版本 |
|------|------|------|
| raft | tikv/raft-rs (master) | 0.7.0 |
| raft-engine | tikv/raft-engine | 0.4.2 |
| protobuf | pingcap/rust-protobuf (v2.8) | - |
| transport | gRPC (tonic) | - |

## 依赖（使用 crates.io）

```toml
[dependencies]
raft = "0.7"
raft-engine = "0.4"
protobuf = "2.28"
tonic = "0.12"
prost = "0.13"

# 移除 git 依赖，使用 crates.io 稳定版本
[patch.crates-io]
# 不需要 patch，直接用 crates.io

## 架构设计

```
┌─────────────────────────────────────────────────────────────────┐
│                        catga-raft                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐        │
│  │ RaftNode    │    │ Pipeline    │    │ ApplyThread │        │
│  │ (raft crate)│───▶│ Manager     │───▶│             │        │
│  │             │    │             │    │             │        │
│  │ - tick()    │    │ - batch    │    │ - commit   │        │
│  │ - step()    │    │   propose  │    │   entries  │        │
│  │ - propose() │    │ - async    │    │ - apply to │        │
│  │             │    │   replicate │    │   SM        │        │
│  └─────────────┘    └─────────────┘    └─────────────┘        │
│         │                  │                   │                │
│         ▼                  ▼                   ▼                │
│  ┌─────────────────────────────────────────────────┐         │
│  │              raft-engine                        │         │
│  │  - Log storage (append, compact, purge)       │         │
│  │  - Multi-Raft support                         │         │
│  │  - Memory-mapped I/O (swap feature)         │         │
│  └─────────────────────────────────────────────────┘         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 核心组件

### 1. RaftNode

封装 raft 库的 RawNode：
- 管理 Raft 状态机
- 处理 tick (election, heartbeat)
- 处理消息传递

### 2. PipelineManager

实现 TiKV 风格的 Pipeline：
- **异步批量复制**：不等待每次 propose 返回
- **批量提交**：定期提交多个 entry
- **流量控制**：控制 in-flight 消息数量

### 3. ApplyThread

应用已提交的 entry 到状态机：
- 从 raft-engine 读取已提交 entry
- 调用状态机的 apply()
- 维护 apply index

### 4. Transport

gRPC 传输层：
- Leader → Follower 复制流
- 快照传输
- 成员变更 RPC

## TiKV Pipeline 关键优化

### 1. 异步批量复制

```rust
// 传统方式（sorock）
async fn propose(&self, data: Vec<u8>) -> Result<()> {
    self.send_append().await?;  // 等待
    self.wait_commit().await?;    // 等待
    Ok(())
}

// TiKV Pipeline（异步）
async fn propose(&self, data: Vec<u8>) -> AsyncResult {
    // 立即返回，复制在后台进行
    self.batch_propose(data);
    AsyncResult::pending()
}
```

### 2. 批量提交

```rust
// TiKV 定期批量提交
fn advance_commit(&mut self) {
    let new_commit = self.peers.find_new_commit_index();
    if new_commit > self.cur_commit {
        self.commit_index = new_commit;  // 批量更新
    }
}
```

### 3. Append 合并

多个小 Entry 合并为一次 RPC 发送。

## 公开 API

```rust
// catga-raft/src/lib.rs
pub mod prelude;
pub mod error;

pub use config::{CatgaRaftConfig, PipelineConfig};
pub use runtime::CatgaRaftRuntime;
pub use coordinator::CatgaRaftCoordinator;
pub use app::CatgaRaftApp;
```

## trait 实现

```rust
// 实现 catga_core 的 trait
impl ConsensusRuntime for CatgaRaftRuntime { ... }
impl ConsensusCoordinator for CatgaRaftCoordinator { ... }
impl ConsensusStateMachine for CatgaRaftApp { ... }
```

## 删除的文件

- `crates/catga-sorock/` - 整个目录删除

## 变更的文件

- `Cargo.toml` - 移除 catga-sorock，添加 catga-raft
- `examples/distributed-kv/` - 更新使用 catga-raft
- `crates/catga-core/src/cluster.rs` - 更新 ClusterForwarder（如果需要）

## 测试

- `catga-raft/tests/` - 单元测试
- `catga-raft/tests/pipeline_perf.rs` - Pipeline 性能测试
- `examples/distributed-kv/` - 集成测试

## 风险

1. **Rust 版本**：raft-engine 需要 Rust 1.85+
2. **复杂度**：Pipeline 实现比 sorock 复杂
3. **调试**：异步 Pipeline 调试难度高

## 实施步骤

1. 创建 catga-raft crate
2. 配置 Cargo.toml（复制 TiKV 依赖）
3. 实现基础 Raft 节点
4. 实现 Pipeline Manager
5. 实现 Apply Thread
6. 实现 gRPC Transport
7. 实现 catga-core trait
8. 更新 distributed-kv 示例
9. 删除 catga-sorock
10. 测试和性能验证
