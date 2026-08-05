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
use catga_core::{CatgaResult, Mediator};

// Request: #[catga_core::catga_request(response = ResponseType)]
#[catga_core::catga_request(response = u64)]
struct Double(u64);

// Command: #[derive(catga_core::catga_command)]
#[derive(catga_core::catga_command)]
struct Log(String);

// 异步函数自动满足 Handler trait
async fn double_handler(msg: Double) -> CatgaResult<u64> {
    Ok(msg.0 * 2)
}

async fn log_handler(msg: Log) -> CatgaResult<()> {
    println!("[log] {}", msg.0);
    Ok(())
}
```

### 一行注册处理器

```rust
#[tokio::main]
async fn main() -> CatgaResult<()> {
    // 消息 => 处理器;
    let registry = catga_core::catga_handlers! {
        request Double => double_handler;
        command Log => log_handler;
    }?;

    let mediator = Mediator::new(registry);
    let result = mediator.send(Double(21)).await?;
    println!("21 * 2 = {}", result); // 输出: 21 * 2 = 42
    Ok(())
}
```

## 核心功能

### 命令、查询、事件 (CQRS)

```rust
use catga_core::auto::AutoApp;
use catga_core::CatgaResult;

// Request: #[catga_core::catga_request(response = Type)]
#[catga_core::catga_request(response = OrderCreated)]
struct CreateOrder { product_id: u64, quantity: u32 }

// Event: #[derive(catga_core::catga_event)]
#[derive(catga_core::catga_event)]
struct OrderCreated { order_id: u64, product_id: u64 }

async fn create_order_handler(cmd: CreateOrder) -> CatgaResult<OrderCreated> {
    Ok(OrderCreated { order_id: 1, product_id: cmd.product_id })
}

async fn notify_handler(event: OrderCreated) -> CatgaResult<()> {
    println!("订单 {} 已创建", event.order_id);
    Ok(())
}

// 一行注册！
let app = AutoApp::from(catga_core::catga_handlers! {
    request CreateOrder => create_order_handler;
    event OrderCreated => notify_handler;
}?);
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
| [simple_handler.rs](examples/src/quickstart/simple_handler.rs) | 最简处理器（无宏） |
| [mediator.rs](examples/src/quickstart/mediator.rs) | AutoApp 中介者 |
| [flow.rs](examples/src/quickstart/flow.rs) | 工作流与补偿 |

运行示例：

```bash
cargo run --example simple_handler
cargo run --example mediator
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
