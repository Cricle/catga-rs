# Raft 统一到 sorock 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 删除 catga-cluster，统一 Raft 实现到 catga-sorock

**Architecture:** 删除所有 raft-rs 相关代码，更新 distributed-kv 只使用 sorock，更新 catga-axum 移除 Raft HTTP 相关代码

**Tech Stack:** sorock 0.12, tonic, gRPC

**Spec:** `docs/superpowers/specs/2026-08-14-raft-unification-design.md`

## Global Constraints

- Rust edition 2024, MSRV 1.96
- 必须安装 protoc 以编译 sorock
- 所有 catga-cluster 依赖必须迁移或删除

---

## Task 1: 更新 distributed-kv 只使用 sorock

**Files:**
- Modify: `examples/distributed-kv/src/main.rs`
- Modify: `examples/distributed-kv/src/node/mod.rs`
- Delete: `examples/distributed-kv/src/node/raft_backend.rs`
- Modify: `examples/distributed-kv/src/node/sorock_backend.rs`

**Interfaces:**
- Consumes: `catga_sorock::SorockRuntimeBuilder`
- Produces: 移除 `Backend` enum，只保留 sorock

- [ ] **Step 1: 修改 main.rs 移除 backend 参数**

```rust
// 修改前
pub(crate) enum Backend {
    Raft,
    Sorock,
}
impl Backend {
    fn parse(raw: &str) -> CatgaResult<Self> {
        match raw {
            "raft" => Ok(Self::Raft),
            "sorock" => Ok(Self::Sorock),
            other => Err(invalid(...)),
        }
    }
}

// 修改后 - 移除 Backend
// 只使用 sorock
```

- [ ] **Step 2: 修改 node/mod.rs 移除 Raft 导入**

```rust
// 移除
use catga_cluster::RaftStateMachineRuntime;

// 修改 KvRuntime 只保留 Sorock
pub(crate) enum KvRuntime {
    // Raft(RaftStateMachineRuntime),  // 删除
    Sorock(SorockRuntime),
}
```

- [ ] **Step 3: 删除 raft_backend.rs**

```bash
rm examples/distributed-kv/src/node/raft_backend.rs
```

- [ ] **Step 4: 修改 main.rs 简化启动逻辑**

```rust
// 修改前
match args.backend {
    Backend::Raft => run_raft(...).await?,
    Backend::Sorock => run_sorock(...).await?,
}

// 修改后 - 直接调用 sorock
run_sorock(...).await?
```

- [ ] **Step 5: 更新 Cargo.toml 移除 catga-cluster**

```toml
# examples/distributed-kv/Cargo.toml
[dependencies]
catga-cluster = { workspace = true }  # 删除
catga-sorock = { workspace = true }   # 保留
```

- [ ] **Step 6: 运行测试验证**

```bash
cargo build -p distributed-kv
cargo test -p distributed-kv
```

- [ ] **Step 7: Commit**

```bash
git add examples/distributed-kv/
git commit -m "refactor: remove raft backend, use sorock only"
```

---

## Task 2: 更新 catga-axum 移除 Raft HTTP 代码

**Files:**
- Delete: `crates/catga-axum/src/cluster.rs`
- Delete: `crates/catga-axum/src/client.rs` (HttpRaftTransport 部分)
- Modify: `crates/catga-axum/src/lib.rs`
- Modify: `crates/catga-axum/src/prelude.rs`

**Interfaces:**
- Consumes: `catga_cluster::RaftTransport`, `catga_axum::RaftHttpCluster`
- Produces: 保留 HTTP 服务器相关类型

- [ ] **Step 1: 检查 catga-axum 的公开 API**

```bash
grep -n "^pub " crates/catga-axum/src/lib.rs
```

- [ ] **Step 2: 识别需要保留的类型**

保留：
- `HttpClusterForwarder`
- `CorrelationHttpClient`
- `MediatorState`
- `shutdown_signal`
- `RAFT_HTTP_HEALTH_PATH`
- `RAFT_HTTP_STATUS_PATH`

删除：
- `HttpRaftTransport`
- `RaftHttpCluster`
- `RaftHttpClusterBuilder`

- [ ] **Step 3: 移除 cluster.rs**

```bash
rm crates/catga-axum/src/cluster.rs
```

- [ ] **Step 4: 清理 lib.rs 中的 Raft 相关导出**

```rust
// 删除
pub use cluster::{RaftHttpCluster, RaftHttpClusterBuilder};
pub use client::{HttpRaftTransport, ...}; // 只保留非 Raft 部分
```

- [ ] **Step 5: 更新 prelude.rs**

```rust
// 修改前
pub use super::{RaftHttpCluster, RaftHttpClusterBuilder, ...};

// 修改后 - 移除 Raft 相关
pub use super::{shutdown_signal, RAFT_HTTP_HEALTH_PATH, ...};
```

- [ ] **Step 6: 移除 client.rs 中的 HttpRaftTransport**

将 `client.rs` 拆分为：
- `client.rs` - 保留 `CorrelationHttpClient`, `HttpClusterForwarder`
- 删除 `HttpRaftTransport` 相关代码

- [ ] **Step 7: 更新 Cargo.toml 移除 catga-cluster**

```toml
[dependencies]
catga-cluster = { workspace = true }  # 删除
```

- [ ] **Step 8: 修复编译错误**

```bash
cargo build -p catga-axum 2>&1 | head -50
```

- [ ] **Step 9: Commit**

```bash
git add crates/catga-axum/
git commit -m "refactor: remove Raft HTTP transport from catga-axum"
```

---

## Task 3: 删除 catga-cluster crate

**Files:**
- Delete: `crates/catga-cluster/`

- [ ] **Step 1: 检查所有依赖 catga-cluster 的地方**

```bash
grep -r "catga-cluster" --include="*.rs" --include="*.toml" | grep -v "catga-sorock"
```

预期依赖：
- `crates/catga-axum/Cargo.toml` (已处理)
- `examples/distributed-kv/Cargo.toml` (已处理)
- `tests/Cargo.toml`
- `examples/Cargo.toml`

- [ ] **Step 2: 更新 tests/Cargo.toml**

```toml
# tests/Cargo.toml
[dependencies]
catga-cluster = { workspace = true }  # 删除
```

- [ ] **Step 3: 检查 tests/ 目录中的测试文件**

```bash
grep -l "catga_cluster" tests/*.rs
```

预期文件：
- `tests/e2e_cluster.rs` - 依赖 catga-cluster
- `tests/cluster_cqrs_flow.rs` - 依赖 catga-cluster
- `tests/support/raft_kv_cluster.rs` - 依赖 catga-cluster

这些测试需要删除或迁移到 sorock。

- [ ] **Step 4: 删除或迁移测试文件**

```bash
# 删除依赖 catga-cluster 的测试
rm tests/e2e_cluster.rs
rm tests/cluster_cqrs_flow.rs
rm tests/support/raft_kv_cluster.rs
```

- [ ] **Step 5: 删除 catga-cluster crate**

```bash
rm -rf crates/catga-cluster
```

- [ ] **Step 6: 更新 workspace Cargo.toml**

```toml
[workspace]
members = [
    "crates/catga-core",
    "crates/catga-flow-store",
    "crates/catga-memorypack-derive",
    "crates/catga-axum",
    # "crates/catga-cluster",  # 删除
    "crates/catga-sorock",
    "crates/catga-nats",
    "crates/catga-redis",
    "crates/catga-robustmq",
    "tests",
    "examples",
    "examples/distributed-kv",
]
```

- [ ] **Step 7: 更新 workspace.dependencies**

```toml
[workspace.dependencies]
catga-cluster = { path = "crates/catga-cluster", version = "0.2.0" }  # 删除
```

- [ ] **Step 8: 验证编译**

```bash
cargo build --workspace 2>&1 | head -100
```

- [ ] **Step 9: 运行测试**

```bash
cargo test --workspace 2>&1 | tail -50
```

- [ ] **Step 10: Commit**

```bash
git add .
git commit -m "refactor: delete catga-cluster crate, unify on sorock"
```

---

## Task 4: 更新文档

**Files:**
- Delete: `wiki/zh/distributed/cluster.md`
- Delete: `wiki/en/distributed/cluster.md`
- Modify: `wiki/zh/distributed/sorock.md`
- Modify: `wiki/en/distributed/sorock.md`
- Modify: `wiki/zh/skill/SKILL.md`
- Modify: `wiki/en/skill/SKILL.md`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: 更新 wiki/zh/distributed/sorock.md**

添加说明：
```
## catga-cluster 合并

catga-cluster (raft-rs over HTTP) 已合并到 catga-sorock。
所有功能现在由 sorock 提供，不再需要单独的 Raft HTTP 传输。
```

- [ ] **Step 2: 更新 SKILL.md 中的说明**

```markdown
# 修改前
| 集群/Raft、单例任务、leader-only 执行 | `catga-cluster = "0.2"` |

# 修改后
| Raft 共识 (gRPC) | `catga-sorock = "0.2"` |
```

- [ ] **Step 3: 删除 cluster.md**

```bash
rm wiki/zh/distributed/cluster.md
rm wiki/en/distributed/cluster.md
```

- [ ] **Step 4: 更新 CHANGELOG.md**

```markdown
## [Unreleased]

### Breaking Changes

- **删除 catga-cluster**: Raft 实现统一到 catga-sorock。
  移除了基于 raft-rs + HTTP 的 catga-cluster crate，
  所有 Raft 功能现在由 sorock 提供。
  - 移除: `catga-cluster` crate
  - 移除: `HttpRaftTransport`, `RaftHttpCluster`
  - 移除: `SingletonTaskRunner` (依赖 Leader 观测)
  - 影响: Leader 不可直接观测，请参考 sorock 文档
```

- [ ] **Step 5: Commit**

```bash
git add wiki/ CHANGELOG.md
git commit -m "docs: update documentation for Raft unification"
```

---

## Task 5: 最终验证

- [ ] **Step 1: 完整编译**

```bash
cargo build --release --workspace 2>&1 | tail -20
```

- [ ] **Step 2: 运行所有测试**

```bash
cargo test --workspace 2>&1 | tail -30
```

- [ ] **Step 3: 检查文档完整性**

```bash
# 确认 cluster.md 已删除
ls wiki/zh/distributed/
ls wiki/en/distributed/
```

- [ ] **Step 4: 更新 README 如果需要**

检查 `README.md` 是否提到 catga-cluster。

---

## 依赖顺序

```
Task 1 (distributed-kv) → Task 2 (catga-axum) → Task 3 (catga-cluster) → Task 4 (文档) → Task 5 (验证)
```

## 风险和注意事项

1. **protoc 依赖**: sorock 需要 protoc，确保环境已安装
2. **Leader 观测**: sorock 不支持 `is_leader()` 查询
3. **心跳 300ms**: 固定值，无法调整
4. **测试覆盖**: 删除的测试需要确保有替代覆盖

---

## 实施后预期结果

```
catga-core/       - 核心 trait (ConsensusStateMachine, ConsensusRuntime)
catga-sorock/     - 唯一的 Raft 实现 (gRPC + 批量复制)
catga-axum/       - HTTP 服务器 (移除 Raft 部分)
examples/distributed-kv/ - 只使用 sorock
```
