# Catga: Rust 事件驱动分布式运行时

纯 Rust 实现的事件驱动分布式系统框架。包含 CQRS、事件溯源、工作流、队列、RPC、竞争消费者、可靠 Outbox/Inbox 处理。

## 安装

```toml
[dependencies]
catga-core = "0.0.2"
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
    let app = AutoApp::from_registry(Calculator::registry()?)?;
    let result = app.mediator().send(Double(21)).await?;
    assert_eq!(result, 42);
    Ok(())
}
```

### 零分配 typed mediator (可选)

使用 `#[catga_service(MyMediator)]` 生成零分配中介者：

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
    async fn create_order(&self, cmd: CreateOrder) -> CatgaResult<OrderCreated> {
        Ok(OrderCreated { order_id: 1, product_id: cmd.product_id })
    }

    async fn on_order_created(&self, event: OrderCreated) -> CatgaResult<()> {
        println!("订单 {} 已创建", event.order_id);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> CatgaResult<()> {
    let app = AutoApp::from_registry(OrderHandler::registry()?)?;
    // ...
    Ok(())
}
```

### 带补偿的工作流

```rust
use catga_core::flow::Flow;

let result = Flow::new("order_checkout")
    .step(
        || async { Ok(()) },  // 预留库存
        || async { Ok(()) },  // 释放库存
    )
    .step(
        || async { Ok(()) },  // 扣款
        || async { Ok(()) },  // 退款
    )
    .run()
    .await?;
```

## 示例

| 示例 | 说明 |
| --- | --- |
| [simple_handler.rs](examples/src/quickstart/simple_handler.rs) | 显式 Handler trait 实现 |
| [service_handler.rs](examples/src/quickstart/service_handler.rs) | #[catga_service] 服务处理器 |
| [typed_mediator.rs](examples/src/quickstart/typed_mediator.rs) | 零分配 typed mediator |
| [flow.rs](examples/src/quickstart/flow.rs) | 工作流与补偿 |

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

## 测试

```bash
# 运行所有测试
cargo test --workspace

# 运行基准测试
cargo +nightly bench --workspace

# 检查代码质量
cargo clippy --workspace
```

## 许可证

MIT
