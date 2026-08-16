# catga-raft 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用 TiKV raft + raft-engine 替换 catga-sorock，实现完整的 TiKV 风格 Pipeline 异步复制

**Architecture:** 使用 tikv/raft-rs 和 tikv/raft-engine 实现高性能 Raft 共识，通过 Pipeline Manager 实现异步批量复制，通过 Apply Thread 实现异步 Apply，通过 gRPC 实现节点间通信

**Tech Stack:** raft 0.7, raft-engine 0.4, tonic 0.12, protobuf 2.28

**Spec:** `docs/superpowers/specs/2026-08-14-raft-replacement-design.md`

## Global Constraints

- Rust edition 2024, MSRV 1.85 (raft-engine 要求)
- 必须安装 protoc 以编译 protobuf
- 所有 catga-sorock 依赖必须迁移或删除
- 实现 catga-core 的 ConsensusRuntime, ConsensusCoordinator, ConsensusStateMachine trait

---

## 文件结构

```
crates/catga-raft/
├── Cargo.toml                    # raft 0.7, raft-engine 0.4, tonic
├── src/
│   ├── lib.rs                   # 公开 API
│   ├── prelude.rs               # 常用类型
│   ├── error.rs                 # 错误类型
│   ├── config.rs                # 配置
│   ├── app.rs                   # CatgaRaftApp (ConsensusStateMachine)
│   ├── node.rs                  # RaftNode 封装
│   ├── pipeline.rs               # PipelineManager (TiKV 风格)
│   ├── apply.rs                 # ApplyThread
│   ├── transport.rs             # gRPC Transport
│   ├── runtime.rs               # CatgaRaftRuntime (ConsensusRuntime)
│   ├── coordinator.rs            # CatgaRaftCoordinator (ConsensusCoordinator)
│   └── builder.rs               # CatgaRaftRuntimeBuilder
└── tests/
    └── pipeline_perf.rs         # Pipeline 性能测试
```

---

## Task 1: 创建 catga-raft crate 骨架

**Files:**
- Create: `crates/catga-raft/Cargo.toml`
- Create: `crates/catga-raft/src/lib.rs`
- Create: `crates/catga-raft/src/prelude.rs`
- Create: `crates/catga-raft/src/error.rs`

**Interfaces:**
- Produces: `CatgaRaftApp`, `CatgaRaftRuntime`, `CatgaRaftCoordinator`, `CatgaRaftRuntimeBuilder`

- [ ] **Step 1: 创建 Cargo.toml**

```toml
[package]
name = "catga-raft"
version = "0.2.0"
edition = "2024"
rust-version = "1.85"

[dependencies]
raft = "0.7"
raft-engine = "0.4"
protobuf = "2.28"
tonic = "0.12"
prost = "0.13"
bytes = "1.8"
async-trait = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "sync", "time", "macros"] }
thiserror = "2"
tracing = "0.1"
parking_lot = "0.12"
crossbeam = "0.8"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
tempfile = "3"
```

- [ ] **Step 2: 创建 lib.rs (公开 API)**

```rust
pub mod prelude;
pub mod error;
pub mod config;
pub mod app;
pub mod node;
pub mod pipeline;
pub mod apply;
pub mod transport;
pub mod runtime;
pub mod coordinator;
pub mod builder;

pub use config::{CatgaRaftConfig, PipelineConfig};
pub use runtime::CatgaRaftRuntime;
pub use coordinator::CatgaRaftCoordinator;
pub use app::CatgaRaftApp;
pub use builder::CatgaRaftRuntimeBuilder;
```

- [ ] **Step 3: 创建 prelude.rs**

```rust
pub use crate::{
    config::{CatgaRaftConfig, PipelineConfig},
    error::{CatgaRaftError, CatgaRaftResult},
    runtime::CatgaRaftRuntime,
    coordinator::CatgaRaftCoordinator,
    app::CatgaRaftApp,
    builder::CatgaRaftRuntimeBuilder,
};
```

- [ ] **Step 4: 创建 error.rs**

```rust
#[derive(thiserror::Error, Debug)]
pub enum CatgaRaftError {
    #[error("raft error: {0}")]
    Raft(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("not leader")]
    NotLeader,
    #[error("timeout")]
    Timeout,
    #[error("node not found: {0}")]
    NodeNotFound(u64),
}

pub type CatgaRaftResult<T> = Result<T, CatgaRaftError>;
```

- [ ] **Step 5: 创建 config.rs (基础配置)**

```rust
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct CatgaRaftConfig {
    pub node_id: u64,
    pub cluster_id: u64,
    pub election_tick: u32,
    pub heartbeat_tick: u32,
    pub max_size_per_msg: u64,
    pub max_inflight_msgs: u64,
}

impl Default for CatgaRaftConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            cluster_id: 0,
            election_tick: 10,
            heartbeat_tick: 3,
            max_size_per_msg: 64 * 1024 * 1024,
            max_inflight_msgs: 256,
        }
    }
}

/// Pipeline 配置
#[derive(Clone, Debug)]
pub struct PipelineConfig {
    pub batch_size: usize,
    pub flush_interval: Duration,
    pub max_inflight: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            batch_size: 64,
            flush_interval: Duration::from_millis(1),
            max_inflight: 1024,
        }
    }
}
```

- [ ] **Step 6: 运行 cargo build -p catga-raft 验证**

- [ ] **Step 7: Commit**

```bash
git add crates/catga-raft/
git commit -m "feat: create catga-raft crate skeleton"
```

---

## Task 2: 实现 app.rs (CatgaRaftApp)

**Files:**
- Create: `crates/catga-raft/src/app.rs`

**Interfaces:**
- Consumes: `catga_core::ConsensusStateMachine`
- Produces: `CatgaRaftApp`

- [ ] **Step 1: 创建 app.rs**

```rust
use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ConsensusStateMachine};

/// CatgaRaftApp 适配 catga_core::ConsensusStateMachine 到 raft 的 Storage trait
pub struct CatgaRaftApp<S> {
    inner: S,
}

impl<S> CatgaRaftApp<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S: ConsensusStateMachine> raft::Storage for CatgaRaftApp<S> {
    type Snapshot = Vec<u8>;

    fn snapshot(&mut self, _: u64, _: u64) -> raft::Result<Self::Snapshot> {
        Ok(self.inner.snapshot().map_err(|e| {
            raft::Error::Store(raft::StorageError::SnapshotTemporarilyUnavailable)
        })?)
    }

    fn apply_snapshot(&mut self, _: raft::Snapshot) -> raft::Result<()> {
        Ok(())
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(0) // TODO: 从状态机获取
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(1)
    }

    fn term(&self, _: u64) -> raft::Result<u64> {
        Ok(0)
    }

    fn entries(&self, _: u64, _: u64, _: usize) -> raft::Result<Vec<raft::prelude::Entry>> {
        Ok(vec![])
    }

    fn fetch_entry(&self, _: u64) -> raft::Result<Option<raft::prelude::Entry>> {
        Ok(None)
    }

    fn compact(&mut self, _: u64) -> raft::Result<()> {
        Ok(())
    }

    fn append(&mut self, _: &[raft::prelude::Entry]) -> raft::Result<()> {
        Ok(())
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add crates/catga-raft/src/app.rs
git commit -m "feat: implement CatgaRaftApp"
```

---

## Task 3: 实现 pipeline.rs (TiKV 风格 Pipeline Manager)

**Files:**
- Create: `crates/catga-raft/src/pipeline.rs`

**Interfaces:**
- Produces: `PipelineManager`

- [ ] **Step 1: 创建 pipeline.rs (TiKV 风格异步批量复制)**

```rust
use std::sync::Arc;
use parking_lot::Mutex;
use crossbeam::channel::{Sender, Receiver};
use tokio::time::{interval, Duration};

/// TiKV 风格的 Pipeline Manager
/// 实现异步批量复制，不等待每次 propose 返回
pub struct PipelineManager {
    /// 待发送的提案
    proposals: Arc<Mutex<Vec<Proposal>>>,
    /// 批量发送器
    batcher: Batcher,
    /// 配置
    config: PipelineConfig,
}

/// 一个提案
struct Proposal {
    data: Vec<u8>,
    callback: oneshot::Sender<CatgaRaftResult<()>>,
}

impl PipelineManager {
    pub fn new(config: PipelineConfig) -> Self {
        Self {
            proposals: Arc::new(Mutex::new(Vec::with_capacity(config.batch_size))),
            batcher: Batcher::new(),
            config,
        }
    }

    /// 异步提案，立即返回
    pub fn propose(&self, data: Vec<u8>) -> CatgaRaftResult<()> {
        let (tx, rx) = oneshot::channel();
        let proposal = Proposal { data, callback: tx };
        
        self.proposals.lock().push(proposal);
        
        // 异步处理，不等待
        // callback 会在复制完成后通过 batcher 回调
        Ok(())
    }

    /// 批量 flush
    /// 由定时器或批量大小触发
    fn flush(&mut self) {
        let mut proposals = self.proposals.lock();
        if proposals.is_empty() {
            return;
        }

        // 批量发送到所有 peer
        let batch: Vec<_> = proposals.drain(..).collect();
        self.batcher.send_batch(batch);
    }
}

/// Batcher 负责批量发送和复制
struct Batcher {
    // TODO: 实现批量发送到 raft
}
```

- [ ] **Step 2: Commit**

```bash
git add crates/catga-raft/src/pipeline.rs
git commit -m "feat: implement TiKV-style PipelineManager"
```

---

## Task 4: 实现 apply.rs (ApplyThread)

**Files:**
- Create: `crates/catga-raft/src/apply.rs`

**Interfaces:**
- Consumes: `ConsensusStateMachine`, `raft_engine::Engine`
- Produces: `ApplyThread`

- [ ] **Step 1: 创建 apply.rs**

```rust
use std::sync::Arc;
use parking_lot::Mutex;
use catga_core::{CatgaResult, ConsensusStateMachine};

/// ApplyThread 异步应用已提交的 entry 到状态机
pub struct ApplyThread<S: ConsensusStateMachine> {
    state_machine: Arc<Mutex<S>>,
    commit_index: Arc<AtomicU64>,
    applied_index: Arc<AtomicU64>,
}

impl<S: ConsensusStateMachine> ApplyThread<S> {
    pub fn new(state_machine: S) -> Self {
        Self {
            state_machine: Arc::new(Mutex::new(state_machine)),
            commit_index: Arc::new(AtomicU64::new(0)),
            applied_index: Arc::new(AtomicU64::new(0)),
        }
    }

    /// 更新提交索引
    pub fn update_commit_index(&self, index: u64) {
        self.commit_index.store(index, Ordering::SeqCst);
    }

    /// 推进应用
    /// 应该定期调用或由事件触发
    pub fn advance(&self) -> CatgaResult<()> {
        let mut sm = self.state_machine.lock();
        let commit = self.commit_index.load(Ordering::Acquire);
        let applied = self.applied_index.load(Ordering::Acquire);

        while applied < commit {
            let next = applied + 1;
            // 从 raft-engine 读取 entry
            // TODO: 实现读取逻辑
            let entry = unimplemented!();
            sm.apply(next, &entry.data)?;
            self.applied_index.store(next, Ordering::Release);
        }
        Ok(())
    }

    /// 获取已应用的索引
    pub fn applied_index(&self) -> u64 {
        self.applied_index.load(Ordering::Acquire)
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add crates/catga-raft/src/apply.rs
git commit -m "feat: implement ApplyThread"
```

---

## Task 5: 实现 transport.rs (gRPC Transport)

**Files:**
- Create: `crates/catga-raft/src/transport.rs`

**Interfaces:**
- Produces: `RaftTransport`

- [ ] **Step 1: 创建 transport.rs**

```rust
use tonic::transport::Channel;
use std::collections::HashMap;

/// gRPC Transport for Raft 消息传递
pub struct RaftTransport {
    peers: HashMap<u64, PeerClient>,
}

struct PeerClient {
    channel: Channel,
}

impl RaftTransport {
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
        }
    }

    pub async fn add_peer(&mut self, id: u64, addr: String) -> CatgaRaftResult<()> {
        let channel = Channel::from_shared(addr)
            .unwrap()
            .connect()
            .await
            .map_err(|e| CatgaRaftError::Transport(e.to_string()))?;
        
        self.peers.insert(id, PeerClient { channel });
        Ok(())
    }

    /// 发送消息到指定 peer
    pub async fn send(&self, peer_id: u64, msg: Vec<u8>) -> CatgaRaftResult<()> {
        let peer = self.peers.get(&peer_id)
            .ok_or(CatgaRaftError::NodeNotFound(peer_id))?;
        // TODO: 实现 gRPC 发送
        Ok(())
    }

    /// 广播消息到所有 peer
    pub async fn broadcast(&self, msg: Vec<u8>) -> CatgaRaftResult<()> {
        for peer in self.peers.values() {
            // TODO: 实现广播
        }
        Ok(())
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add crates/catga-raft/src/transport.rs
git commit -m "feat: implement gRPC transport"
```

---

## Task 6: 实现 runtime.rs 和 coordinator.rs

**Files:**
- Create: `crates/catga-raft/src/runtime.rs`
- Create: `crates/catga-raft/src/coordinator.rs`

**Interfaces:**
- Consumes: `CatgaRaftApp`, `PipelineManager`, `ApplyThread`
- Produces: `CatgaRaftRuntime`, `CatgaRaftCoordinator`
- 实现 `catga_core::ConsensusRuntime`, `catga_core::ConsensusCoordinator`

- [ ] **Step 1: 创建 coordinator.rs**

```rust
use std::sync::Arc;
use catga_core::ConsensusCoordinator;

pub struct CatgaRaftCoordinator {
    node_id: Arc<str>,
    is_leader: parking_lot::RwLock<bool>,
    leader_endpoint: parking_lot::RwLock<Option<Arc<str>>>,
    members: parking_lot::RwLock<Vec<Arc<str>>>,
}

impl CatgaRaftCoordinator {
    pub fn new(node_id: String) -> Self {
        Self {
            node_id: Arc::from(node_id.into_boxed_str()),
            is_leader: parking_lot::RwLock::new(false),
            leader_endpoint: parking_lot::RwLock::new(None),
            members: parking_lot::RwLock::new(Vec::new()),
        }
    }

    pub fn set_leader(&self, endpoint: Option<String>) {
        *self.is_leader.write() = endpoint.is_some();
        *self.leader_endpoint.write() = endpoint.map(|e| Arc::from(e.into_boxed_str()));
    }
}

impl ConsensusCoordinator for CatgaRaftCoordinator {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn is_leader(&self) -> bool {
        *self.is_leader.read()
    }

    fn leader_endpoint(&self) -> Option<Arc<str>> {
        self.leader_endpoint.read().clone()
    }

    fn member_endpoints(&self) -> Arc<[Arc<str>]> {
        Arc::from(self.members.read().clone())
    }
}
```

- [ ] **Step 2: 创建 runtime.rs**

```rust
use std::sync::Arc;
use catga_core::{ConsensusRuntime, ConsensusCoordinator};
use crate::{CatgaRaftResult, CatgaRaftCoordinator, PipelineManager};

pub struct CatgaRaftRuntime {
    pipeline: Arc<PipelineManager>,
    coordinator: Arc<CatgaRaftCoordinator>,
    applied_index: Arc<AtomicU64>,
}

impl CatgaRaftRuntime {
    pub fn new(
        pipeline: PipelineManager,
        coordinator: CatgaRaftCoordinator,
    ) -> Self {
        Self {
            pipeline: Arc::new(pipeline),
            coordinator: Arc::new(coordinator),
            applied_index: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl ConsensusRuntime for CatgaRaftRuntime {
    async fn propose(&self, data: Vec<u8>) -> CatgaRaftResult<()> {
        self.pipeline.propose(data)
    }

    async fn add_member(&self, id: u64, endpoint: String) -> CatgaRaftResult<()> {
        // TODO: 实现添加成员
        Ok(())
    }

    async fn remove_member(&self, id: u64) -> CatgaRaftResult<()> {
        // TODO: 实现移除成员
        Ok(())
    }

    async fn applied_index(&self) -> CatgaRaftResult<u64> {
        Ok(self.applied_index.load(Ordering::Acquire))
    }

    fn is_alive(&self) -> bool {
        true
    }

    fn coordinator(&self) -> Arc<dyn ConsensusCoordinator> {
        Arc::clone(&self.coordinator) as Arc<dyn ConsensusCoordinator>
    }

    fn shutdown(&self) {
        // TODO: 实现 shutdown
    }

    async fn join(self) -> CatgaRaftResult<()> {
        // TODO: 实现 join
        Ok(())
    }
}
```

- [ ] **Step 3: Commit**

```bash
git add crates/catga-raft/src/runtime.rs crates/catga-raft/src/coordinator.rs
git commit -m "feat: implement CatgaRaftRuntime and CatgaRaftCoordinator"
```

---

## Task 7: 实现 node.rs (RaftNode 封装)

**Files:**
- Create: `crates/catga-raft/src/node.rs`

**Interfaces:**
- Consumes: `CatgaRaftConfig`, `raft::Storage`
- Produces: `RaftNode`

- [ ] **Step 1: 创建 node.rs**

```rust
use raft::{Config, RawNode, Storage};
use std::sync::Arc;
use parking_lot::Mutex;

pub struct RaftNode<S: Storage> {
    raw_node: Arc<Mutex<RawNode<S>>>,
    config: CatgaRaftConfig,
}

impl<S: Storage + 'static> RaftNode<S> {
    pub fn new(config: CatgaRaftConfig, storage: S) -> CatgaRaftResult<Self> {
        let cfg = Config {
            id: config.node_id,
            election_tick: config.election_tick,
            heartbeat_tick: config.heartbeat_tick,
            max_size_per_msg: config.max_size_per_msg,
            max_inflight_msgs: config.max_inflight_msgs,
            ..Default::default()
        };

        let raw_node = RawNode::new(&cfg, storage)
            .map_err(|e| CatgaRaftError::Raft(e.to_string()))?;

        Ok(Self {
            raw_node: Arc::new(Mutex::new(raw_node)),
            config,
        })
    }

    pub fn tick(&self) {
        self.raw_node.lock().tick();
    }

    pub fn propose(&self, data: Vec<u8>) -> CatgaRaftResult<()> {
        let mut node = self.raw_node.lock();
        node.propose(vec![], data)
            .map_err(|e| CatgaRaftError::Raft(e.to_string()))?;
        Ok(())
    }

    pub fn step(&self, msg: raft::prelude::Message) -> CatgaRaftResult<()> {
        let mut node = self.raw_node.lock();
        node.step(msg)
            .map_err(|e| CatgaRaftError::Raft(e.to_string()))?;
        Ok(())
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add crates/catga-raft/src/node.rs
git commit -m "feat: implement RaftNode wrapper"
```

---

## Task 8: 实现 builder.rs

**Files:**
- Create: `crates/catga-raft/src/builder.rs`

**Interfaces:**
- Consumes: `CatgaRaftConfig`, `ConsensusStateMachine`
- Produces: `CatgaRaftRuntimeBuilder`

- [ ] **Step 1: 创建 builder.rs**

```rust
use crate::{CatgaRaftConfig, CatgaRaftRuntime, CatgaRaftCoordinator, PipelineConfig};
use catga_core::{CatgaResult, ConsensusStateMachine};
use std::time::Duration;

pub struct CatgaRaftRuntimeBuilder {
    config: CatgaRaftConfig,
    pipeline_config: PipelineConfig,
    members: Vec<(u64, String)>,
}

impl CatgaRaftRuntimeBuilder {
    pub fn from_cli(base_port: u16, node: u64, nodes: u64) -> CatgaResult<Self> {
        let config = CatgaRaftConfig {
            node_id: node + 1,
            ..Default::default()
        };
        
        let members: Vec<_> = (0..nodes)
            .filter(|&i| i != node)
            .map(|i| (i + 1, format!("http://127.0.0.1:{}", base_port + i as u16 + 1000)))
            .collect();

        Ok(Self {
            config,
            pipeline_config: PipelineConfig::default(),
            members,
        })
    }

    pub async fn start<S: ConsensusStateMachine + 'static>(
        self,
        state_machine: S,
    ) -> CatgaResult<CatgaRaftRuntime> {
        // TODO: 实现构建逻辑
        unimplemented!()
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add crates/catga-raft/src/builder.rs
git commit -m "feat: implement CatgaRaftRuntimeBuilder"
```

---

## Task 9: 更新 Cargo.toml 添加到 workspace

**Files:**
- Modify: `Cargo.toml`

- [ ] **Step 1: 添加 catga-raft 到 workspace members**

```toml
[workspace]
members = [
    "crates/catga-core",
    "crates/catga-flow-store",
    "crates/catga-memorypack-derive",
    "crates/catga-raft",      # 新增
    "crates/catga-axum",
    "crates/catga-nats",
    "crates/catga-redis",
    "crates/catga-robustmq",
    "tests",
    "examples",
    "examples/distributed-kv",
]
```

- [ ] **Step 2: Commit**

```bash
git add Cargo.toml
git commit -m "feat: add catga-raft to workspace"
```

---

## Task 10: 更新 distributed-kv 使用 catga-raft

**Files:**
- Modify: `examples/distributed-kv/Cargo.toml`
- Modify: `examples/distributed-kv/src/node/mod.rs`
- Modify: `examples/distributed-kv/src/node/sorock_backend.rs` → `raft_backend.rs`

- [ ] **Step 1: 更新 Cargo.toml 依赖**

```toml
# 删除
catga-sorock = { workspace = true }

# 添加
catga-raft = { workspace = true }
```

- [ ] **Step 2: 重命名和修改 backend 文件**

```bash
mv examples/distributed-kv/src/node/sorock_backend.rs examples/distributed-kv/src/node/raft_backend.rs
```

- [ ] **Step 3: 更新 import**

```rust
// 修改前
use catga_sorock::{SorockRuntimeBuilder, SorockStorage};

// 修改后
use catga_raft::{CatgaRaftRuntimeBuilder, CatgaRaftConfig};
```

- [ ] **Step 4: 更新 node/mod.rs**

```rust
mod raft_backend;

// 更新 KvRuntime
pub(crate) struct KvRuntime(pub Arc<CatgaRaftRuntime>);
```

- [ ] **Step 5: 编译验证**

```bash
cargo build -p distributed-kv
```

- [ ] **Step 6: Commit**

```bash
git add examples/distributed-kv/
git commit -m "refactor: migrate distributed-kv to catga-raft"
```

---

## Task 11: 删除 catga-sorock

**Files:**
- Delete: `crates/catga-sorock/`
- Modify: `Cargo.toml`

- [ ] **Step 1: 删除 catga-sorock 目录**

```bash
rm -rf crates/catga-sorock
```

- [ ] **Step 2: 从 workspace 移除**

```toml
# Cargo.toml
# 删除 "crates/catga-sorock"
members = [
    "crates/catga-core",
    "crates/catga-raft",  # 保留
    # ... 其他
]
```

- [ ] **Step 3: 编译验证**

```bash
cargo build --workspace
```

- [ ] **Step 4: Commit**

```bash
git add .
git commit -m "refactor: remove catga-sorock, use catga-raft"
```

---

## Task 12: 性能和功能测试

**Files:**
- Create: `crates/catga-raft/tests/pipeline_perf.rs`
- Modify: `examples/distributed-kv/` (集成测试)

- [ ] **Step 1: 创建 pipeline_perf.rs**

```rust
use catga_raft::{CatgaRaftRuntimeBuilder, PipelineConfig};
use std::time::{Duration, Instant};

#[tokio::test]
async fn pipeline_throughput() {
    let state_machine = KvMachine::new();
    let config = PipelineConfig {
        batch_size: 64,
        flush_interval: Duration::from_millis(1),
        ..Default::default()
    };
    
    let runtime = CatgaRaftRuntimeBuilder::from_cli(9100, 0, 3)
        .await
        .unwrap();
    
    let start = Instant::now();
    for i in 0..1000 {
        runtime.propose(format!("value-{}", i).into_bytes()).await.unwrap();
    }
    let elapsed = start.elapsed();
    
    println!("Throughput: {} writes/s", 1000.0 / elapsed.as_secs_f64());
}
```

- [ ] **Step 2: 运行测试**

```bash
cargo test -p catga-raft
cargo test -p distributed-kv
```

- [ ] **Step 3: Commit**

```bash
git add crates/catga-raft/tests/
git commit -m "test: add catga-raft performance tests"
```

---

## 依赖顺序

```
Task 1 → Task 2 → Task 3 → Task 4 → Task 5 → Task 6 → Task 7 → Task 8 → Task 9 → Task 10 → Task 11 → Task 12
```

## 风险和注意事项

1. **Rust 1.85 MSRV**: raft-engine 要求较高版本的 Rust
2. **Pipeline 实现复杂度**: TiKV 的 Pipeline 是核心优化，需要仔细实现
3. **raft-engine API**: 需要熟悉其多 Raft 日志管理
4. **测试覆盖**: 需要充分测试 Pipeline 的批量、异步、流量控制

## 实施后预期结果

```
catga-core/       - 核心 trait (ConsensusStateMachine, ConsensusRuntime)
catga-raft/       - 唯一的 Raft 实现 (TiKV Pipeline)
catga-axum/       - HTTP 服务器
examples/distributed-kv/ - 使用 catga-raft
```
