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

详细性能分析：[Performance](./zh/advanced/performance.md)

## 快速开始

```toml
[dependencies]
catga-core = "0.2"
async-trait = "0.1"
```

```rust
use catga_core::auto::AutoApp;
use catga_core::{CatgaResult, Handler, Message, Request};

struct Add(i64);
impl Message for Add {}
impl Request for Add {
    type Response = i64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct AddHandler;
#[async_trait::async_trait]
impl Handler<Add> for AddHandler {
    async fn handle(&self, msg: Add) -> CatgaResult<i64> {
        Ok(msg.0 * 2)
    }
}

# async fn run() -> CatgaResult<()> {
let app = AutoApp::builder()
    .handler(AddHandler)?
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
│  ├── catga-core::memory (进程内)                            │
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
- [安装与配置](./zh/getting-started/installation.md)
- [第一个应用](./zh/getting-started/first-app.md)
- [核心概念](./zh/getting-started/concepts.md)
- [测试指南](./zh/getting-started/testing.md)

### 核心模块
- [Message & Handler](./zh/core/message-handler.md)
- [Mediator & Registry](./zh/core/mediator-registry.md)
- [CQRS 模式](./zh/core/cqrs.md)
- [Event Sourcing](./zh/core/event-sourcing.md)

### 分布式
- [NATS 传输](./zh/distributed/nats.md)
- [Redis Streams](./zh/distributed/redis.md)
- [RocketMQ](./zh/distributed/robustmq.md)
- [集群模式](./zh/distributed/cluster.md)
- [sorock 多 Raft 后端](./zh/distributed/sorock.md)

### 工作流
- [Flow 概述](./zh/flow/overview.md)
- [状态机](./zh/flow/state-machine.md)
- [补偿事务](./zh/flow/compensation.md)

### 高级主题
- [性能优化](./zh/advanced/performance.md)
- [类型系统](./zh/advanced/type-system.md)
- [生命周期管理](./zh/advanced/lifecycle.md)
- [CI 分级与发布](./zh/advanced/ci-release.md)

### 对比
- [Catga vs cqrs-es 深度对比](./zh/comparison/cqrs-es.md)

### Skill（应用开发指南）
- [应用开发指南（SKILL）](./zh/skill/SKILL.md)
- [Mediator：消息、处理器与派发](./zh/skill/mediator.md)
- [Pipeline：请求策略与内置 Behavior](./zh/skill/pipeline.md)
- [Flow：补偿流程与工作流](./zh/skill/flow.md)
- [StateMachine：事件驱动的持久化状态机](./zh/skill/state-machine.md)
- [Transport：消息传输契约与适配器](./zh/skill/transport.md)
- [消息可靠性模式：Outbox / Inbox / 幂等 / 死信 / 订阅 / 消费循环](./zh/skill/reliability.md)
- [事件溯源、投影与读模型](./zh/skill/event-sourcing.md)
- [Stores：持久化存储](./zh/skill/stores.md)
- [分布式构件：集群 / Raft / 分布式 ID / 租约 / 任务调度](./zh/skill/distributed.md)
- [HTTP 集成（catga-axum）](./zh/skill/http.md)
- [编解码、压缩与消息签名](./zh/skill/codec.md)
- [错误处理、幂等与生产检查清单](./zh/skill/production.md)
- [Catga Auto 与运行时正确性实施计划](./zh/skill/auto-plan.md)
- [catga-auto design](./zh/skill/auto-design.md)
- [Catga Auto 示例实施计划](./zh/skill/auto-examples-plan.md)

## 社区与支持

- GitHub: https://github.com/catga-rs/catga-rs
- 文档: https://catga.rs
