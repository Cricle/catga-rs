---
name: catga
description: 使用 Catga Rust 库（catga-core、catga-flow、catga-flow-store、catga-memory、catga-nats、catga-redis、catga-axum、catga-cluster、catga-testing 等 catga-* crate）开发 CQRS、事件溯源、工作流与分布式消息应用的指南。当用户用 Catga 编写、调试或重构 Rust 代码，或提到 Mediator、catga_handlers、catga_typed_mediator、catga_pipeline、Request/Command/Event、Handler、Flow、DslFlow、FlowDefinition、FlowRuntime、StateMachine、FlowStore、MessageTransport、TypedTransport、MemoryTransport、Envelope、Outbox、Inbox、幂等、死信、Aggregate、EventStore、快照、Projection、ReadModel、Raft、集群、雪花 ID、租约、cron 调度、MemoryPack、Axum 集成、补偿流程等 Catga 概念时使用。
---

# Catga 应用开发指南

Catga 是纯 Rust 的 CQRS、事件溯源、工作流与分布式运行时工作区。本 skill 指导如何用它的公开 API 编写**应用代码**。

## 设计哲学（决定代码写法）

1. **显式组合，无隐式机制**：无反射、无服务定位器、无隐藏后台线程、无无界队列。一切依赖在启动时显式构造并传入。
2. **调用方拥有生命周期**：任何 `Registry`、`Mediator`、`FlowRuntime`、store、transport 的构造都不会启动后台任务。轮询、调度、恢复、关停全部由应用的监督任务显式驱动。
3. **边界可替换**：先用内存适配器（`catga-memory`）写应用代码，需要持久化/分布式时只替换对应边界（如用 NATS 替换 `MemoryTransport`），应用模型不变。
4. **at-least-once 语义**：Flow 重试、传输重投、超时恢复都是至少一次。外部副作用（支付、邮件等）必须由应用用幂等键兜底，Catga 不会自动让重试变安全。
5. **有界优先**：批量、分页、缓冲区都有显式上限（`MAX_*` 常量）；超时、重试次数必须有限。

## Crate 选择

从拥有所需契约的最小 crate 开始，按需添加，不要默认启用所有适配器。

| 需求 | 依赖 |
| --- | --- |
| 进程内 typed 请求/命令/事件（必需核心） | `catga-core = "0.0.2"` |
| 补偿性 / 持久化工作流 | `catga-flow = "0.0.2"` |
| 有界内存适配器、确定性测试 | `catga-memory = "0.0.2"` |
| SQL/Redis 持久化 Flow 状态 | `catga-flow-store = { version = "0.0.2", features = ["sqlite"] }` |
| NATS 传输与 JetStream 存储 | `catga-nats = "0.0.2"` |
| Redis 传输与存储 | `catga-redis = "0.0.2"` |
| RobustMQ 传输（mq9 mailbox） | `catga-robustmq = "0.0.2"` |
| Axum HTTP 集成 | `catga-axum = "0.0.2"` |
| 集群/Raft、单例任务、leader-only 执行 | `catga-cluster = "0.0.2"` |

运行时需要 `tokio = { version = "1", features = ["macros", "rt-multi-thread"] }`；结构体处理器需要 `async-trait = "0.1"`。

## 快速上手（最小可运行）

```toml
[dependencies]
catga-core = "0.0.2"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust,no_run
use catga_core::{CatgaResult, Mediator, Request, catga_handlers, request_handler};

struct Double(u64);
impl catga_core::Message for Double {}
impl Request for Double {
    type Response = u64;
}

#[tokio::main]
async fn main() -> CatgaResult<()> {
    // catga_handlers! 在启动时构建 Registry；重复注册 request/command 会报 Conflict
    let mediator = Mediator::new(catga_handlers! {
        request Double => request_handler(|request: Double| async move { Ok(request.0 * 2) })
    }?);
    let result = mediator.send(Double(21)).await?;
    assert_eq!(result, 42);
    Ok(())
}
```

## 三种消息角色（CQRS）

- **Request**：恰好一个处理器，返回 typed 响应（`mediator.send`）。
- **Command**：恰好一个处理器，无响应（`mediator.send_command`）。
- **Event**：零或多个处理器，扇出（`mediator.publish`），消息类型必须 `Clone`。

## 编写规则（必须遵守）

1. 所有可失败 API 返回 `CatgaResult<T>`，用 `?` 传播；在应用边界用 `error.code()`（`ErrorCode`）与 `error.is_retryable()` 决策，不要匹配错误文本。
2. handler、behavior、store、transport 实例在**启动时一次性构造**并显式共享（通常 `Arc`）；不要在每次请求时新建。
3. SQL store 在受控启动阶段调用 `migrate()`，迁移成功后再开始处理 flow。
4. 调度器、outbox 处理器、接收循环运行在**应用自己拥有的监督任务**里；适配器不会替你 spawn。
5. 重试外部副作用前先选定幂等键（durable flow 用稳定的 flow id + step 名派生）。
6. 设置有限的命令超时与有界批量大小；接受不可信输入前选好 `MAX_*` 上限内的页/批量。
7. 热路径且处理器集合在启动时已知 → 用 `catga_typed_mediator!`（零分配单态化派发）；运行时才注册或需要 `Arc<Mediator>` 共享 → 用动态 `Mediator`。

## 选型速查

| 场景 | 入口 |
| --- | --- |
| 单进程 typed 请求/查询 | `Mediator` + `catga_handlers!`（见 [mediator.md](mediator.md)） |
| 零分配极致吞吐派发 | `catga_typed_mediator!`（见 [mediator.md](mediator.md)） |
| 请求需要重试/超时/授权/校验 | `catga_pipeline!` + 内置 Behavior（见 [pipeline.md](pipeline.md)） |
| 本地可补偿多步操作 | `Flow` / `compensating_flow!`（见 [flow.md](flow.md)） |
| 进程内有状态的复杂分支流程 | `DslFlow`（见 [flow.md](flow.md)） |
| 需要重启恢复/等待外部结果/定时恢复的流程 | `FlowDefinition` + `FlowRuntime` + durable store（见 [flow.md](flow.md)、[stores.md](stores.md)） |
| 事件驱动的实体状态迁移 | `StateMachine`（见 [state-machine.md](state-machine.md)） |
| 本地发布/确认消息 | `MemoryTransport`（见 [transport.md](transport.md)） |
| typed 消息直发（免手写 Envelope） | `TypedTransport`（见 [transport.md](transport.md)） |
| 跨进程消息（NATS/Redis/RobustMQ） | 对应 `catga-*` 传输适配器（见 [transport.md](transport.md)） |
| 跨进程请求-响应（RPC） | `*RequestClient` / `*RequestServer`（见 [transport.md](transport.md)） |
| 写库后可靠发消息 | Outbox：`OutboxBehavior` + `OutboxProcessor`（见 [reliability.md](reliability.md)） |
| 消费去重 / 接口幂等 | `InboxBehavior` / `IdempotencyBehavior`（见 [reliability.md](reliability.md)） |
| 失败消息终局隔离 | 死信 `DeadLetterStore`（见 [reliability.md](reliability.md)） |
| 消费循环 / 竞争消费 | `CompetingConsumer` / `SubscriptionRunner`（见 [reliability.md](reliability.md)） |
| 事件溯源聚合 | `Aggregate` + `AggregateRepository` + `EventStore`（见 [event-sourcing.md](event-sourcing.md)） |
| 快照 / 事件升级 / 时间旅行 | 见 [event-sourcing.md](event-sourcing.md) |
| 投影与读模型同步 | `Projection` / `ReadModelSynchronizer`（见 [event-sourcing.md](event-sourcing.md)） |
| 持久化存储后端选型 | [stores.md](stores.md) |
| 集群协调 / Raft / leader-only / 单例任务 | [distributed.md](distributed.md) |
| 分布式唯一 ID / 租约 / cron 调度 | [distributed.md](distributed.md) |
| Axum HTTP 服务 | `MediatorState` + `CatgaHttpResult`（见 [http.md](http.md)） |
| 编解码 / 压缩 / 消息签名 | [codec.md](codec.md) |
| 测试（spy/harness/断言） | `catga-testing`（见 [production.md](production.md)） |
| 错误分类、重试决策、上线检查 | [production.md](production.md) |

## 参考文件

- [mediator.md](mediator.md) — 消息 trait、处理器、注册宏、派发 API、typed mediator
- [pipeline.md](pipeline.md) — `catga_pipeline!` 与全部内置 Behavior
- [flow.md](flow.md) — 本地 Flow、`DslFlow`、持久化 `FlowDefinition`/`FlowRuntime`
- [state-machine.md](state-machine.md) — 事件驱动状态机（构建器、迁移、持久化执行）
- [transport.md](transport.md) — `MessageTransport` 契约、Envelope、memory/NATS/Redis 适配器、TypedTransport、RPC、路由
- [reliability.md](reliability.md) — Outbox/Inbox/幂等/死信/持久订阅/竞争消费循环
- [event-sourcing.md](event-sourcing.md) — 聚合、EventStore、快照、事件升级、时间旅行、投影、读模型
- [stores.md](stores.md) — `catga-flow-store` 后端、连接/迁移、NATS/Redis/memory 存储矩阵
- [distributed.md](distributed.md) — cluster/Raft/leader-only/单例任务、雪花 ID、租约、cron 调度
- [http.md](http.md) — catga-axum：MediatorState、错误映射、上下文传播、集群路由
- [codec.md](codec.md) — MemoryPack/bincode 编解码、压缩、HMAC 消息签名
- [production.md](production.md) — `CatgaError`/`ErrorCode`、幂等与重试准则、生命周期、可观测性、测试工具、验证命令

## 仓库内可运行的示例

本仓库自带无需 Docker 即可运行的示例；按场景分组和完整运行说明见
[`docs/examples.md`](../docs/examples.md)。写代码前可参考：

```bash
cargo run -p catga-examples --bin mediator          # 最小 mediator
cargo run -p catga-examples --bin typed_mediator    # 零分配 typed mediator
cargo run -p catga-examples --bin flow              # 本地补偿 Flow
cargo run -p catga-examples --bin memory_transport  # 内存传输 publish/receive/ack
cargo run -p catga-examples --bin checkout          # CQRS + Flow 补偿 + 事件确认
cargo run -p catga-examples --bin order_service     # 完整 HTTP 订单服务（axum + cluster）
```
