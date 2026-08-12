# 安装与配置

## 环境要求

- Rust 1.96+（edition 2024）
- Tokio 运行时 (async)

## 添加依赖

```toml
[dependencies]
catga-core = "0.2"      # 核心：Mediator、`auto` 门面、内存适配器、Flow 引擎
async-trait = "0.1"     # 结构体处理器需要

# 可选：跨进程传输层（进程内通信用 catga-core 内置的 memory 模块）
catga-nats = "0.2"       # NATS JetStream
catga-redis = "0.2"      # Redis Streams
```

## 最小化依赖

Catga 的依赖设计遵循最小化原则：

| 层级 | 依赖 | 说明 |
|------|------|------|
| `catga-core` | async-trait, tokio | 仅异步运行时依赖 |
| `catga-core::auto` | catga-core, tokio-util | 便捷构建器（`AutoApp`） |
| `catga-core::memory` | catga-core | 零外部传输依赖 |

## Hello World

```rust
use catga_core::auto::AutoApp;
use catga_core::{CatgaResult, Handler, Message, Request};

struct Ping;
impl Message for Ping {}
impl Request for Ping {
    type Response = String;
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct PingHandler;
#[async_trait::async_trait]
impl Handler<Ping> for PingHandler {
    async fn handle(&self, _: Ping) -> CatgaResult<String> {
        Ok("pong".to_string())
    }
}

#[tokio::main]
async fn main() -> CatgaResult<()> {
    let app = AutoApp::builder()
        .handler(PingHandler)?
        .build()?;

    let response = app.mediator().send(Ping).await?;
    println!("{}", response); // "pong"

    Ok(())
}
```

## 配置选项

### Tokio 运行时

```rust
use tokio::runtime::Runtime;

let rt = Runtime::new()?;
rt.block_on(async {
    let app = AutoApp::builder()
        .handler(PingHandler)?
        .build()?;
    // ...
});
```

### 传输层选择

```rust
// 内存传输（进程内，catga-core 内置）
use catga_core::memory::MemoryTransport;

let transport = MemoryTransport::new(1024)?; // 有界容量上限
```

传输实例在启动时显式构造并传给需要的组件；跨进程时换用 `catga-nats` / `catga-redis` 提供的同名适配器，应用模型不变。

## 验证安装

运行测试验证安装：

```bash
cargo test --package catga-core --lib
```
