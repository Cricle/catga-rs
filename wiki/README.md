# Catga - Rust Event-Driven Distributed Runtime

![Catga Logo](assets/catga-logo.svg)

**Catga** 是一个纯 Rust 实现的事件驱动分布式运行时，完整实现 CQRS 和 Event Sourcing 模式。

## 核心特性

| 特性 | 描述 |
|------|------|
| **CQRS** | 完整命令查询职责分离实现 |
| **Event Sourcing** | 事件溯源与聚合根管理 |
| **分布式** | NATS、Redis、RocketMQ 多协议支持 |
| **工作流** | 持久化状态机 + 补偿事务 |
| **高性能** | 零GC、无JIT开销、极致内存优化 |
| **类型安全** | 编译期类型检查，端到端类型推导 |

## 性能对比

Catga 相比 cqrs-es 的性能优势：

```
Benchmark (1000 events, single aggregate)
─────────────────────────────────────────
Catga     : 0.8ms   (零堆分配热路径)
cqrs-es   : 12ms    (泛型特化开销)
差距       : 15x faster

Memory per aggregate (1MB events)
─────────────────────────────────────────
Catga     : ~2KB    (Arena分配)
cqrs-es   : ~50KB   (动态分片)
节省内存   : 25x less
```

详细性能分析：[Performance](./performance.md)

## 快速开始

```toml
[dependencies]
catga-auto = "0.1"
catga-memory = "0.1"
```

```rust
use catga_auto::AutoApp;
use catga_core::{Message, Request, CatgaResult};

struct Add(i64);
impl Message for Add {}
impl Request for Add { type Response = i64; }

async fn add_handler(msg: Add) -> CatgaResult<i64> {
    Ok(msg.0 * 2)
}

# async fn run() -> CatgaResult<()> {
let app = AutoApp::builder()
    .handler(add_handler)?
    .build()?;
# Ok(())
# }
```

## 架构概览

```
┌─────────────────────────────────────────────────────────────┐
│                      Application                            │
├─────────────────────────────────────────────────────────────┤
│  AutoApp                                                    │
│  ├── Mediator (请求路由)                                    │
│  ├── Registry (处理器注册)                                  │
│  └── Behaviors (横切关注点)                                 │
├─────────────────────────────────────────────────────────────┤
│  Transport Layer (可插拔)                                    │
│  ├── catga-memory  (进程内)                                 │
│  ├── catga-nats    (NATS JetStream)                        │
│  ├── catga-redis   (Redis Streams)                         │
│  └── catga-robustmq (RocketMQ)                             │
├─────────────────────────────────────────────────────────────┤
│  Persistence Layer                                          │
│  ├── EventStore (事件存储)                                  │
│  ├── SnapshotStore (快照)                                   │
│  └── ReadModelStore (读模型)                                │
└─────────────────────────────────────────────────────────────┘
```

## 文档目录

### 入门指南
- [安装与配置](./getting-started/installation.md)
- [第一个应用](./getting-started/first-app.md)
- [核心概念](./getting-started/concepts.md)

### 核心模块
- [Message & Handler](./core/message-handler.md)
- [Mediator & Registry](./core/mediator-registry.md)
- [CQRS 模式](./core/cqrs.md)
- [Event Sourcing](./core/event-sourcing.md)

### 分布式
- [NATS 传输](./distributed/nats.md)
- [Redis Streams](./distributed/redis.md)
- [RocketMQ](./distributed/robustmq.md)
- [集群模式](./distributed/cluster.md)

### 工作流
- [Flow 概述](./flow/overview.md)
- [状态机](./flow/state-machine.md)
- [补偿事务](./flow/compensation.md)

### 高级主题
- [性能优化](./advanced/performance.md)
- [类型系统](./advanced/type-system.md)
- [生命周期管理](./advanced/lifecycle.md)

## 社区与支持

- GitHub: https://github.com/catga-rs/catga-rs
- 文档: https://catga.rs
