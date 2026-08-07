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
catga-auto = "0.1"
tokio = { version = "1", features = ["full"] }
```

## Complete Example

```rust
use catga_auto::AutoApp;
use catga_core::{CatgaResult, Message, Request};

// Define messages
struct Add { lhs: i64, rhs: i64 }
impl Message for Add {}
impl Request for Add { type Response = i64; }

struct Multiply { lhs: i64, rhs: i64 }
impl Message for Multiply {}
impl Request for Multiply { type Response = i64; }

// Define handlers
async fn add_handler(msg: Add) -> CatgaResult<i64> {
    Ok(msg.lhs + msg.rhs)
}

async fn multiply_handler(msg: Multiply) -> CatgaResult<i64> {
    Ok(msg.lhs * msg.rhs)
}

#[tokio::main]
async fn main() -> CatgaResult<()> {
    // Build application
    let app = AutoApp::builder()
        .handler(add_handler)?
        .handler(multiply_handler)?
        .build()?;

    let handle = app.handle().clone();

    // Send requests
    let sum = handle.send(Add { lhs: 10, rhs: 20 }).await?;
    println!("10 + 20 = {}", sum); // 30

    let product = handle.send(Multiply { lhs: 6, rhs: 7 }).await?;
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

- [Adding Command handling](./cqrs.md)
- [Publishing and subscribing to Events](./events.md)
- [Configuring the transport layer](../distributed/nats.md)
