# Catga: Rust 事件驱动分布式运行时

> 纯 Rust 实现的事件驱动分布式系统框架。包含 CQRS、事件溯源、工作流、队列、RPC、竞争消费者、可靠 Outbox/Inbox 处理。

## 什么是 Catga？

Catga 是一个用于构建**事件驱动分布式系统**的 Rust 框架。它将你的业务逻辑组织成：

- **命令 (Command)** - 修改状态的写操作
- **查询 (Query)** - 读取数据
- **事件 (Event)** - 状态变化的记录

### 核心概念

| 概念 | 说明 |
| --- | --- |
| **CQRS** | 命令查询职责分离 - 命令和查询使用不同的模型，简化复杂业务逻辑 |
| **事件溯源 (Event Sourcing)** | 用事件序列替代当前状态，完整保留业务历史，支持回溯和重放 |
| **竞争消费者 (Competing Consumers)** | 多个消费者并发处理同一队列的消息，提高吞吐量 |
| **Outbox/Inbox** | 确保消息传递的可靠性，避免分布式系统中的数据不一致 |
| **Saga/补偿事务** | 跨服务的分布式事务处理，失败时执行补偿操作 |

### 为什么选择 Catga？

- **类型安全** - 编译时检查，零运行时开销
- **零依赖核心** - `catga-core` 无外部依赖，仅需 tokio
- **渐进式采用** - 从单个服务开始，逐步引入分布式特性
- **生产就绪** - 内置 Raft 共识、故障转移、快照支持

## 架构图

```
┌─────────────────────────────────────────────────────────────┐
│                      Application                            │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│  │   Command   │    │    Query    │    │    Event    │     │
│  │  Handler    │    │  Handler    │    │  Handler    │     │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘     │
│         │                   │                   │            │
│         └───────────────────┼───────────────────┘            │
│                             ▼                                │
│                    ┌─────────────────┐                        │
│                    │    Mediator     │                        │
│                    │  (请求派发器)    │                        │
│                    └────────┬────────┘                        │
│                             │                                 │
│         ┌───────────────────┼───────────────────┐            │
│         ▼                   ▼                   ▼            │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│  │    NATS    │    │    Redis    │    │    Axum     │     │
│  │ (JetStream)│    │ (队列/订阅)  │    │  (HTTP)     │     │
│  └─────────────┘    └─────────────┘    └─────────────┘     │
│                             │                                 │
│                             ▼                                 │
│                    ┌─────────────────┐                        │
│                    │   Raft Cluster  │                        │
│                    │  (catga-sorock) │                        │
│                    └─────────────────┘                        │
└─────────────────────────────────────────────────────────────┘
```

## 安装

```toml
[dependencies]
catga-core = "0.2"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## 快速开始

### 一行宏定义消息

```rust
use catga_core::CatgaResult;

// Request: #[catga_core::catga_request(response = ResponseType)]
#[catga_core::catga_request(response = u64)]
struct Double(u64);

// Command: #[derive(catga_core::catga_command)]
#[derive(catga_core::catga_command)]
struct Log(String);
```

### 服务处理器 (catga_service)

使用 `#[catga_service]` 自动识别请求/命令，生成注册代码：

```rust
use catga_core::{auto::AutoApp, CatgaResult, catga_service};

#[catga_core::catga_request(response = u64)]
struct Double(u64);

#[derive(catga_core::catga_command)]
struct Log(String);

struct Calculator;

#[catga_service]
impl Calculator {
    // CatgaResult<T> (T != ()) → 请求处理器
    async fn double(&self, msg: Double) -> CatgaResult<u64> {
        Ok(msg.0 * 2)
    }

    // CatgaResult<()> → 命令处理器
    async fn log(&self, msg: Log) -> CatgaResult<()> {
        println!("[Calculator] {}", msg.0);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> CatgaResult<()> {
    let app = AutoApp::from_registry(Calculator::registry())?;
    let result = app.mediator().send(Double(21)).await?;
    assert_eq!(result, 42);
    Ok(())
}
```

### Typed Mediator：高性能免分配派发

默认 `Mediator` 使用方便，但通过注册表动态派发会有少量性能开销。`#[catga_service(MyMediator)]` 生成的 typed mediator 在编译时静态绑定 Handler，是零分配的高性能选项：

```rust
use catga_core::{catga_request, catga_command, catga_service};

#[catga_request(response = u64)]
struct GetBalance { account_id: u64 }

#[derive(catga_command)]
struct TransferFunds { from: u64, to: u64, amount: u64 }

struct BankService;

#[catga_service(BankMediator)]
impl BankService {
    async fn get_balance(&self, msg: GetBalance) -> CatgaResult<u64> {
        Ok(msg.account_id * 1000)
    }

    async fn transfer(&self, cmd: TransferFunds) -> CatgaResult<()> {
        println!("transferred {} from {} to {}", cmd.amount, cmd.from, cmd.to);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> CatgaResult<()> {
    let mediator = BankMediator::new(BankService);
    let balance = mediator.send(GetBalance { account_id: 42 }).await?;
    assert_eq!(balance, 42_000);
    Ok(())
}
```

## 核心功能

### 命令、查询、事件 (CQRS)

```rust
use catga_core::auto::AutoApp;
use catga_core::CatgaResult;

#[catga_core::catga_request(response = OrderCreated)]
struct CreateOrder { product_id: u64, quantity: u32 }

#[derive(catga_core::catga_event, Clone)]
struct OrderCreated { order_id: u64, product_id: u64 }

struct OrderHandler;

#[catga_core::catga_service]
impl OrderHandler {
    // 返回值作为事件发布
    async fn create_order(&self, cmd: CreateOrder) -> CatgaResult<OrderCreated> {
        Ok(OrderCreated { order_id: 1, product_id: cmd.product_id })
    }

    // 监听并处理事件
    async fn on_order_created(&self, event: OrderCreated) -> CatgaResult<()> {
        println!("订单 {} 已创建", event.order_id);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> CatgaResult<()> {
    let app = AutoApp::from_registry(OrderHandler::registry())?;
    // ...
    Ok(())
}
```

### 带补偿的工作流 (Saga)

```rust
use catga_core::flow::Flow;

let result = Flow::new("order_checkout")
    .step(
        || async { Ok(()) },  // 预留库存
        || async { Ok(()) },  // 补偿: 释放库存
    )
    .step(
        || async { Ok(()) },  // 扣款
        || async { Ok(()) },  // 补偿: 退款
    )
    .run()
    .await?;
```

## 示例

| 示例 | 说明 |
| --- | --- |
| [simple_handler.rs](examples/src/quickstart/simple_handler.rs) | 显式 Handler trait 实现 |
| [service_handler.rs](examples/src/quickstart/service_handler.rs) | #[catga_service] 服务处理器 |
| [typed_mediator.rs](examples/src/quickstart/typed_mediator.rs) | 免分配 typed mediator |
| [flow.rs](examples/src/quickstart/flow.rs) | 工作流与补偿 |
| [distributed-kv](examples/distributed-kv) | 三节点 Raft KV 集群：领导者转发、快照、故障转移；双后端 `--backend raft\|sorock` |

运行示例：

```bash
cargo run --example simple_handler
cargo run --example service_handler
cargo run --example typed_mediator
cargo run --example flow
```

## 模块

| 模块 | 说明 |
| --- | --- |
| `catga-core` | 核心接口：Mediator、Registry、Handler traits |
| `catga-flow-store` | Flow 状态持久化 (SQLite/PostgreSQL) |
| `catga-nats` | NATS JetStream 传输层 |
| `catga-redis` | Redis 队列和发布订阅 |
| `catga-axum` | Axum HTTP 集成 |
| `catga-sorock` | sorock 多 Raft 共识后端 (gRPC/redb) |

## 测试

```bash
# 运行所有测试
cargo test --workspace

# 运行基准测试
cargo +nightly bench --workspace

# 检查代码质量
cargo clippy --workspace
```

## 相关资源

- [Wiki 文档](./wiki/) - 完整开发指南
- [性能基准](./wiki/zh/advanced/performance.md) - 性能测试结果
- [CQRS/ES 对比](./wiki/zh/comparison/cqrs-es.md) - 与其他框架的比较

## 许可证

MIT
