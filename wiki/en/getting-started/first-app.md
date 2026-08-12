# Your First Application

This guide creates a complete Catga application.

## Project Structure

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
catga-core = "0.2"
async-trait = "0.1"
tokio = { version = "1", features = ["full"] }
```

## Complete Example

```rust
use catga_core::auto::AutoApp;
use catga_core::{CatgaResult, Handler, Message, Request};

// Define messages
struct Add { lhs: i64, rhs: i64 }
impl Message for Add {}
impl Request for Add {
    type Response = i64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct Multiply { lhs: i64, rhs: i64 }
impl Message for Multiply {}
impl Request for Multiply {
    type Response = i64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

// Define handlers
struct AddHandler;
#[async_trait::async_trait]
impl Handler<Add> for AddHandler {
    async fn handle(&self, msg: Add) -> CatgaResult<i64> {
        Ok(msg.lhs + msg.rhs)
    }
}

struct MultiplyHandler;
#[async_trait::async_trait]
impl Handler<Multiply> for MultiplyHandler {
    async fn handle(&self, msg: Multiply) -> CatgaResult<i64> {
        Ok(msg.lhs * msg.rhs)
    }
}

#[tokio::main]
async fn main() -> CatgaResult<()> {
    // Build application
    let app = AutoApp::builder()
        .handler(AddHandler)?
        .handler(MultiplyHandler)?
        .build()?;

    let mediator = app.mediator();

    // Send requests
    let sum = mediator.send(Add { lhs: 10, rhs: 20 }).await?;
    println!("10 + 20 = {}", sum); // 30

    let product = mediator.send(Multiply { lhs: 6, rhs: 7 }).await?;
    println!("6 * 7 = {}", product); // 42

    Ok(())
}
```

## Running

```bash
cargo run
```

Output:

```
10 + 20 = 30
6 * 7 = 42
```

## Next Steps

- [Adding Command handling](../core/cqrs.md)
- [Publishing and subscribing to Events](../core/message-handler.md)
- [Configuring the transport layer](../distributed/nats.md)
