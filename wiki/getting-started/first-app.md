# 第一个应用

本指南创建完整的 Catga 应用。

## 项目结构

```
my-app/
├── Cargo.toml
└── src/
    └── main.rs
```

## Cargo.toml

```toml
[package]
name = "my-app"
version = "0.1.0"
edition = "2021"

[dependencies]
catga-auto = "0.1"
tokio = { version = "1", features = ["full"] }
```

## 完整示例

```rust
use catga_auto::AutoApp;
use catga_core::{CatgaResult, Message, Request};

// 定义消息
struct Add { lhs: i64, rhs: i64 }
impl Message for Add {}
impl Request for Add { type Response = i64; }

struct Multiply { lhs: i64, rhs: i64 }
impl Message for Multiply {}
impl Request for Multiply { type Response = i64; }

// 定义处理器
async fn add_handler(msg: Add) -> CatgaResult<i64> {
    Ok(msg.lhs + msg.rhs)
}

async fn multiply_handler(msg: Multiply) -> CatgaResult<i64> {
    Ok(msg.lhs * msg.rhs)
}

#[tokio::main]
async fn main() -> CatgaResult<()> {
    // 构建应用
    let app = AutoApp::builder()
        .handler(add_handler)?
        .handler(multiply_handler)?
        .build()?;

    let handle = app.handle().clone();

    // 发送请求
    let sum = handle.send(Add { lhs: 10, rhs: 20 }).await?;
    println!("10 + 20 = {}", sum); // 30

    let product = handle.send(Multiply { lhs: 6, rhs: 7 }).await?;
    println!("6 * 7 = {}", product); // 42

    Ok(())
}
```

## 运行

```bash
cargo run
```

输出：

```
10 + 20 = 30
6 * 7 = 42
```

## 下一步

- [添加命令处理](./cqrs.md)
- [发布和订阅事件](./events.md)
- [配置传输层](../distributed/nats.md)
