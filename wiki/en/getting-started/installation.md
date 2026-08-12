# Installation and Configuration

## Environment Requirements

- Rust 1.96+ (edition 2024)
- Tokio runtime (async)

## Adding Dependencies

```toml
[dependencies]
catga-core = "0.2"      # Core: Mediator, `auto` facade, memory adapters, Flow engine
async-trait = "0.1"     # Required for struct handlers

# Optional: cross-process transports (in-process messaging uses the memory module built into catga-core)
catga-nats = "0.2"       # NATS JetStream
catga-redis = "0.2"      # Redis Streams
```

## Minimal Dependencies

Catga's dependency design follows the principle of minimalism:

| Layer | Dependency | Description |
|-------|------------|-------------|
| `catga-core` | async-trait, tokio | Async runtime only |
| `catga-core::auto` | catga-core, tokio-util | Convenience builders (`AutoApp`) |
| `catga-core::memory` | catga-core | Zero external transport dependencies |

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

## Configuration Options

### Tokio Runtime

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

### Transport Layer Selection

```rust
// Memory transport (in-process, built into catga-core)
use catga_core::memory::MemoryTransport;

let transport = MemoryTransport::new(1024)?; // Bounded capacity limit
```

Transport instances are explicitly constructed at startup and passed to the components that need them; for cross-process messaging swap in the same-named adapters from `catga-nats` / `catga-redis` — the application model stays unchanged.

## Verifying Installation

Run tests to verify installation:

```bash
cargo test --package catga-core --lib
```
