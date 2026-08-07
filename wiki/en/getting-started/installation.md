# Installation and Configuration

## Environment Requirements

- Rust 1.75+
- Tokio runtime (async)

## Adding Dependencies

```toml
[dependencies]
catga-auto = "0.1"
catga-core = "0.1"

# Choose a transport layer
catga-memory = "0.1"     # In-process communication
catga-nats = "0.1"       # NATS JetStream
catga-redis = "0.1"      # Redis Streams
```

## Minimal Dependencies

Catga's dependency design follows the principle of minimalism:

| Layer | Dependency | Description |
|-------|------------|-------------|
| `catga-core` | async-trait, tokio | Async runtime only |
| `catga-auto` | catga-core, tokio-util | Convenience builders |
| `catga-memory` | catga-core | Zero external transport dependencies |

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

## Configuration Options

### Tokio Runtime

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

### Transport Layer Selection

```rust
// Memory transport (in-process)
use catga_memory::MemoryTransport;

let transport = Arc::new(MemoryTransport::new());
let app = AutoApp::builder()
    .transport(transport)
    .handler(ping_handler)?
    .build()?;
```

## Verifying Installation

Run tests to verify installation:

```bash
cargo test --package catga-auto --lib
```
