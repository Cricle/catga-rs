# 安装与配置

## 环境要求

- Rust 1.75+
- Tokio 运行时 (async)

## 添加依赖

```toml
[dependencies]
catga-auto = "0.1"
catga-core = "0.1"

# 选择一个传输层
catga-memory = "0.1"     # 进程内通信
catga-nats = "0.1"       # NATS JetStream
catga-redis = "0.1"      # Redis Streams
```

## 最小化依赖

Catga 的依赖设计遵循最小化原则：

| 层级 | 依赖 | 说明 |
|------|------|------|
| `catga-core` | async-trait, tokio | 仅异步运行时依赖 |
| `catga-auto` | catga-core, tokio-util | 便捷构建器 |
| `catga-memory` | catga-core | 零外部传输依赖 |

## Hello World

```rust
use catga_auto::AutoApp;
use catga_core::{Message, Request, CatgaResult};

struct Ping;
impl Message for Ping {}
impl Request for Ping { type Response = String; }

async fn ping_handler(_: Ping) -> CatgaResult<String> {
    Ok("pong".to_string())
}

#[tokio::main]
async fn main() -> CatgaResult<()> {
    let app = AutoApp::builder()
        .handler(ping_handler)?
        .build()?;

    let response = app.handle().send(Ping).await?;
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
        .handler(ping_handler)?
        .build()?;
    // ...
});
```

### 传输层选择

```rust
// 内存传输 (进程内)
use catga_memory::MemoryTransport;

let transport = Arc::new(MemoryTransport::new());
let app = AutoApp::builder()
    .transport(transport)
    .handler(ping_handler)?
    .build()?;
```

## 验证安装

运行测试验证安装：

```bash
cargo test --package catga-auto --lib
```
