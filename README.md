# Catga: Rust Event-Driven Distributed Runtime

Catga is a pure-Rust runtime for event-driven distributed systems. CQRS, event
sourcing, workflows, queues, RPC, competing consumers, durable outbox and
inbox processing, Raft coordination, and cron scheduling are explicit,
composable building blocks.

Applications own their handlers, stores, transports, task supervision, and
shutdown. There is no reflection, service locator, hidden worker, or unbounded
queue. Start in memory, then replace only the boundary that needs durability or
distribution.

## Start Here

| Need | Example | Description |
| --- | --- | --- |
| Typed local command/query | [`examples/src/quickstart/mediator.rs`](examples/src/quickstart/mediator.rs) | Basic mediator with plain async handlers |
| Zero-cost typed dispatch | [`examples/src/quickstart/typed_mediator.rs`](examples/src/quickstart/typed_mediator.rs) | Compile-time dispatch on hot paths |
| Routed messages and workers | [`examples/src/runtime/bus_cqrs.rs`](examples/src/runtime/bus_cqrs.rs) | Bus with request/command/event handlers |
| HTTP with CQRS | [`examples/src/web/order_service.rs`](examples/src/web/order_service.rs) | Axum + Catga integration |
| Durable flows | [`examples/src/quickstart/flow.rs`](examples/src/quickstart/flow.rs) | State-machine workflows with compensation |

## Quick Start

Plain async functions automatically satisfy handler traits via Fn-blanket impls.
No `#[async_trait]` needed for simple handlers:

```rust
use catga_auto::AutoApp;
use catga_core::{CatgaResult, Message, Request};

struct Double(u64);
impl Message for Double {}
impl Request for Double { type Response = u64; }

async fn double_handler(value: Double) -> CatgaResult<u64> {
    Ok(value.0 * 2)
}

#[tokio::main]
async fn main() -> CatgaResult<()> {
    let app = AutoApp::builder()
        .request::<Double, _>(double_handler)?
        .build()?;
    let result = app.mediator().send(Double(21)).await?;
    assert_eq!(result, 42);
    println!("21 doubled is {result}");
    Ok(())
}
```

## Crates

| Crate | Purpose |
| --- | --- |
| `catga-core` | Core contracts: Mediator, Registry, behaviors, memory adapters |
| `catga-auto` | High-level App builder with fluent API |
| `catga-core-macros` | `#[catga_main]`, `catga_handlers!` procedural macros |
| `catga-axum` | Axum integration with typed request/response mapping |
| `catga-cluster` | Raft-based distributed coordination |
| `catga-nats` | NATS JetStream transport for publish/subscribe and request/reply |
| `catga-redis` | Redis transport for queues, pub/sub, and stream operations |
| `catga-robustmq` | RobustMQ transport for priority queues and scheduled messages |
| `catga-flow-store` | Durable state persistence for Flow workflows (SQLite/Redis) |

## Install

```toml
[dependencies]
catga-auto = "0.0.2"
catga-core = "0.0.2"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

### Feature Flags

```toml
# Flow state persistence
catga-flow-store = { version = "0.0.2", features = ["sqlite"] }
catga-flow-store = { version = "0.0.2", features = ["sqlx-postgres"] }

# Transports
catga-nats = "0.0.2"
catga-redis = "0.0.2"
catga-robustmq = "0.0.2"

# HTTP adapter
catga-axum = "0.0.2"
```

## Core Concepts

### Mediator Pattern

Request (one handler, returns response), Command (one handler, no response),
and Event (multiple handlers) messages with typed dispatch:

```rust
use catga_core::{Message, Request, Command, Event, Mediator, Registry};

struct GetUser(u64);
impl Message for GetUser {}
impl Request for GetUser { type Response = User; }

struct CreateUser(String);
impl Message for CreateUser {}
impl Command for CreateUser {}

struct UserCreated(u64);
impl Message for UserCreated {}
impl Event for UserCreated {}
```

### Compensating Flows

Multi-step workflows with automatic retry and rollback on failure:

```rust
use catga_core::{compensating_flow, CatgaResult, Message, Command};

struct Order { id: u64 }
impl Message for Order {}
impl Command for Order {}

let flow = compensating_flow! {
    "checkout";
    context = Order { id: 1 };
    steps {
        reserve_inventory => release_inventory;
        capture_payment => refund_payment;
    }
};
```

### Durable Patterns

- **Outbox**: Reliable event publishing with at-least-once delivery
- **Inbox**: Idempotent command processing with deduplication
- **Competing Consumers**: Multiple workers processing shared queues
- **Dead Letter**: Failed messages routing to error queues

## Run Examples

```bash
# Basic mediator
cargo run -p catga-examples --bin mediator

# Typed mediator
cargo run -p catga-examples --bin typed_mediator

# Flow example
cargo run -p catga-examples --bin flow

# Bus with CQRS
cargo run -p catga-examples --bin simple_bus
cargo run -p catga-examples --bin bus_cqrs

# HTTP with Axum
cargo run -p catga-examples --bin checkout

# Distributed (requires Docker)
docker compose --file examples/distributed-todo/compose.yaml up --build
```

## Production Guidelines

- **Delivery guarantees**: Flow recovery and retries are at-least-once.
  Make external effects idempotent with application-owned stable keys.
- **Ownership**: Run migrations in controlled startup, then supervise
  consumers, schedulers, outbox processors, and shutdown in application-owned tasks.
- **Feature selection**: Select only the Cargo features and adapters your deployment uses.
- **Connection pools**: Keep database connection pools application-owned.
  Catga exposes configuration but does not impose a connection count.
- **Consumer patterns**: Use `CompetingConsumer` for durable production receive loops.

## Verification

```bash
# Check all examples compile
cargo check -p catga-examples --bins

# Run all tests
cargo test --workspace --all-features

# Format check
cargo fmt --all -- --check

# Lint check
cargo clippy --workspace --all-features -- -D warnings
```

NATS JetStream and external infrastructure tests run with `#[ignore]` locally
and in CI against ephemeral Testcontainers.

## Documentation

- [Examples Guide](docs/examples.md): Ordered runnable programs
- [Performance Report](docs/performance.md): Throughput, latency, and durability benchmarks
- [Skill Guide](skill/SKILL.md): API selection and ownership rules
- [Transport Guide](skill/transport.md): Transports, typed delivery, RPC patterns
- [Reliability Guide](skill/reliability.md): Outbox, inbox, idempotency, dead-lettering

## License

MIT. See [LICENSE](LICENSE).
