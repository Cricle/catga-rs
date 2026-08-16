# Catga 易用性优化设计

## 目标

在不改变功能、性能、概念模型的前提下，降低用户认知负担：
1. **10 行代码入门** — `cargo add catga-core` 后 minimal working example
2. **统一入口** — 单一 prelude 包含 95% API
3. **零配置默认值** — 常用场景无需调参

## 约束

- 不合并 crate
- 不改变概念模型
- 不使用类型别名
- 不改变核心 trait 定义
- 不影响现有功能

---

## 当前状态分析

### Crate 概览

| Crate | 代码行数 | 主要模块 | 当前入口复杂度 |
|-------|----------|---------|---------------|
| catga-core | 39,944L | 217 files | ~100 个公开类型分散导出 |
| catga-flow-store | 7,952L | 45 files | 多个子模块，每个需要单独 import |
| catga-cluster | 5,204L | 14 files | RaftRuntime, ClusterCoordinator 等 |
| catga-nats | 8,431L | 26 files | JetStream 传输层 |
| catga-redis | 5,911L | 23 files | 队列和发布订阅 |
| catga-axum | 2,970L | 8 files | HTTP 集成 |
| catga-sorock | 1,782L | 8 files | Multi-Raft 后端 |
| catga-memorypack-derive | 1,421L | 6 files | derive macro |

### 当前痛点

**catga-core**：
```rust
// 用户需要记住多个路径
use catga_core::{Mediator, Registry, Handler, Request};
use catga_core::flow::DslFlow;
use catga_core::memory::MemoryTransport;
use catga_core::auto::AutoApp;
```

**catga-cluster**：
```rust
// Raft 相关的配置和运行时分散
use catga_cluster::{RaftRuntime, RaftClusterConfig, ClusterCoordinator};
use catga_cluster::RaftStateMachine;
```

**catga-flow-store**：
```rust
// 每个存储类型需要单独 import
use catga_flow_store::{SqlFlowStore, SqlFlowScheduler};
use catga_flow_store::SqlFlowStoreOptions;
```

---

## 改动 1: 统一 prelude（catga-core）

### 实现

新建 `catga-core/src/prelude.rs`：

```rust
//! Catga unified prelude — one import for 95% of use cases.

pub use crate::auto::{AutoApp, AutoAppBuilder};
pub use crate::error::{CatgaError, CatgaResult, ErrorCode};
pub use crate::handler::Handler;
pub use crate::macros::{catga_command, catga_event, catga_request, catga_service};
pub use crate::mediator::Mediator;
pub use crate::message::{Command, Event, Message, Request};
pub use crate::registry::Registry;
pub use crate::flow::dsl::DslFlow;
pub use crate::memory::MemoryTransport;
```

### 在 lib.rs 中重导出

```rust
pub mod prelude;
pub use prelude::*;
```

---

## 改动 2: App 一键启动

### 目标
提供 `App` 作为统一的本地应用入口。

### API 设计

```rust
/// Application entry point providing mediator and transport.
#[derive(Clone)]
pub struct App {
    mediator: Arc<Mediator>,
    transport: Arc<MemoryTransport>,
}

impl App {
    /// Creates an App with in-memory transport and default settings.
    pub fn local() -> CatgaResult<Self> {
        Self::builder().local().build()
    }

    pub fn mediator(&self) -> Mediator { self.mediator.clone() }
    pub fn registry(&self) -> &Registry { self.mediator.registry() }
    pub fn transport(&self) -> &MemoryTransport { &self.transport }

    pub fn builder() -> AppBuilder {
        AppBuilder::new()
    }
}

pub struct AppBuilder {
    queue_capacity: usize,
}

impl AppBuilder {
    pub fn new() -> Self { Self { queue_capacity: 1024 } }
    pub fn queue_capacity(mut self, cap: usize) -> Self { self.queue_capacity = cap; self }
    pub fn local(self) -> Self { self }
    pub fn build(self) -> CatgaResult<App> {
        let transport = Arc::new(MemoryTransport::new(self.queue_capacity)?);
        let mediator = Mediator::new(Registry::new());
        Ok(App { mediator, transport })
    }
}
```

---

## 改动 3: catga-cluster prelude

### 当前复杂度

```rust
use catga_cluster::{RaftRuntime, RaftClusterConfig, ClusterCoordinator};
use catga_cluster::{RaftTransport, RaftStateMachine};
use catga_cluster::{MemoryCluster, LeadershipSubscription};
```

### 优化后

新建 `catga-cluster/src/prelude.rs`：

```rust
//! Catga cluster unified prelude.

pub use crate::{ClusterCoordinator, LeadershipSubscription};
pub use crate::{RaftRuntime, RaftTransport};
pub use crate::{MemoryCluster, RaftStateMachine};
pub use crate::config::{RaftClusterConfig, RaftClusterMemberConfig};
```

---

## 改动 4: catga-sorock prelude

### 当前复杂度

```rust
use catga_sorock::{SorockRuntime, SorockNode, SorockApp};
use catga_sorock::{SorockRuntimeBuilder, SorockNodeConfig};
use catga_sorock::{SorockCoordinator, SorockStorage};
```

### 优化后

新建 `catga-sorock/src/prelude.rs`：

```rust
//! Catga sorock unified prelude — multi-Raft backend.

pub use crate::{SorockRuntime, SorockNode, SorockApp};
pub use crate::{SorockRuntimeBuilder, SorockCoordinator};
pub use crate::{SorockNodeConfig, SorockStorage};
```

---

## 改动 5: catga-flow-store prelude

### 当前复杂度

```rust
use catga_flow_store::{SqlFlowStore, SqlFlowScheduler};
use catga_flow_store::{SqlSuspendedFlowStore, SqlFlowScheduler};
use catga_flow_store::SqlFlowStoreOptions;
```

### 优化后

新建 `catga-flow-store/src/prelude.rs`：

```rust
//! Catga flow store unified prelude.

#[cfg(feature = "sqlite")]
pub use crate::SqlFlowStore;
#[cfg(feature = "sqlite")]
pub use crate::SqlFlowScheduler;

#[cfg(feature = "postgres")]
pub use crate::postgres::{PgFlowStore, PgFlowScheduler};

#[cfg(feature = "mysql")]
pub use crate::mysql::{MySqlFlowStore, MySqlFlowScheduler};

#[cfg(feature = "redis")]
pub use crate::redis::{RedisFlowStore, RedisFlowScheduler};
```

---

## 改动 6: catga-nats prelude

### 当前复杂度

```rust
use catga_nats::{NatsTransport, NatsJetStream};
use catga_nats::NatsOptions;
```

### 优化后

新建 `catga-nats/src/prelude.rs`：

```rust
//! Catga NATS unified prelude — JetStream transport.

pub use crate::{NatsTransport, NatsJetStream};
pub use crate::NatsOptions;
```

---

## 改动 7: catga-redis prelude

### 当前复杂度

```rust
use catga_redis::{RedisTransport, RedisQueue};
use catga_redis::RedisOptions;
```

### 优化后

新建 `catga-redis/src/prelude.rs`：

```rust
//! Catga Redis unified prelude — queue and pub/sub transport.

pub use crate::{RedisTransport, RedisQueue};
pub use crate::RedisOptions;
```

---

## 改动 8: catga-axum prelude

### 当前复杂度

```rust
use catga_axum::{CatgaRouter, ClusterLayer};
use catga_axum::{extract_state, extract_mediator};
```

### 优化后

新建 `catga-axum/src/prelude.rs`：

```rust
//! Catga Axum unified prelude — HTTP integration.

pub use crate::{CatgaRouter, ClusterLayer};
pub use crate::{extract_state, extract_mediator};
pub use crate::TlsConfig;
```

---

## 改动 9: 简化 README

### 目标
第一个示例不超过 15 行。

```rust
// 15 行 minimal example
use catga_core::prelude::*;

#[catga_request(response = u64)]
struct Double(u64);

struct Calculator;

#[catga_service]
impl Calculator {
    async fn double(&self, msg: Double) -> CatgaResult<u64> {
        Ok(msg.0 * 2)
    }
}

#[tokio::main]
async fn main() -> CatgaResult<()> {
    let app = App::local()?;
    app.registry().register_request::<Double, _>(Calculator)?;
    let result = app.mediator().send(Double(21)).await?;
    assert_eq!(result, 42);
    Ok(())
}
```

---

## 改动 10: 精简示例文件

### 保留的核心示例

| 示例 | 行数 | 演示内容 |
|------|------|---------|
| `distributed-kv` | ~500L | Raft/Sorock 多节点 KV |
| `quickstart.rs` | ~40L | App::local() + Handler |
| `cqrs.rs` | ~80L | Request/Command/Event |
| `flow.rs` | ~60L | DslFlow 补偿事务 |

---

## 文件变更清单

### catga-core
| 文件 | 操作 |
|------|------|
| `catga-core/src/prelude.rs` | 新建 |
| `catga-core/src/lib.rs` | 添加 `pub mod prelude; pub use prelude::*;` |

### catga-cluster
| 文件 | 操作 |
|------|------|
| `catga-cluster/src/prelude.rs` | 新建 |
| `catga-cluster/src/lib.rs` | 添加 `pub mod prelude;` |

### catga-sorock
| 文件 | 操作 |
|------|------|
| `catga-sorock/src/prelude.rs` | 新建 |
| `catga-sorock/src/lib.rs` | 添加 `pub mod prelude;` |

### catga-flow-store
| 文件 | 操作 |
|------|------|
| `catga-flow-store/src/prelude.rs` | 新建 |
| `catga-flow-store/src/lib.rs` | 添加 `pub mod prelude;` |

### catga-nats
| 文件 | 操作 |
|------|------|
| `catga-nats/src/prelude.rs` | 新建 |
| `catga-nats/src/lib.rs` | 添加 `pub mod prelude;` |

### catga-redis
| 文件 | 操作 |
|------|------|
| `catga-redis/src/prelude.rs` | 新建 |
| `catga-redis/src/lib.rs` | 添加 `pub mod prelude;` |

### catga-axum
| 文件 | 操作 |
|------|------|
| `catga-axum/src/prelude.rs` | 新建 |
| `catga-axum/src/lib.rs` | 添加 `pub mod prelude;` |

### 文档
| 文件 | 操作 |
|------|------|
| `README.md` | 更新快速开始示例 |
| `examples/src/quickstart.rs` | 重写为精简版 |

---

## 兼容性

所有改动都是纯增量：
- 现有 `use catga_core::{Mediator, ...}` 导入方式仍然有效
- 新 prelude 只做重导出，不改变任何类型定义
- 现有测试全部通过

---

## 实施顺序

1. 为每个 crate 创建 prelude.rs
2. 更新 lib.rs 导出 prelude
3. 更新 README 和示例
4. 添加 prelude 契约测试
5. 运行全量测试

---

## 用户体验对比

### Before

```rust
use catga_core::{Mediator, Registry, Handler, Request, catga_service};
use catga_core::flow::DslFlow;
use catga_core::memory::MemoryTransport;
use catga_cluster::{RaftRuntime, RaftClusterConfig};
use catga_sorock::{SorockRuntime, SorockNode};
```

### After

```rust
use catga_core::prelude::*;
use catga_cluster::prelude::*;
use catga_sorock::prelude::*;
```

一个 prelude 包含单个 crate 95% 的常用类型。
