# Catga for Rust

Catga is a pure-Rust CQRS, event-sourcing, workflow, and distributed-runtime
workspace. Applications compose typed, bounded components explicitly: there is
no reflection, service locator, hidden worker, or unbounded queue.

## Quick start

Start with `catga-core` and register handlers during application startup:

```toml
[dependencies]
async-trait = "0.1"
catga-core = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust,no_run
use async_trait::async_trait;
use catga_core::{CatgaResult, Handler, Mediator, Request, catga_handlers};

struct Double(u64);
impl catga_core::Message for Double {}
impl Request for Double {
    type Response = u64;
}

struct DoubleHandler;

#[async_trait]
impl Handler<Double> for DoubleHandler {
    async fn handle(&self, request: Double) -> CatgaResult<u64> {
        Ok(request.0 * 2)
    }
}

#[tokio::main]
async fn main() -> CatgaResult<()> {
    let mediator = Mediator::new(catga_handlers! { request Double => DoubleHandler }?);
    let result = mediator.send(Double(21)).await?;
    assert_eq!(result, 42);
    Ok(())
}
```

Use `catga_pipeline!` when a request needs explicit retries, timeouts, or
authorization. Its stages are caller-owned values, so their limits and
lifecycle are visible at startup.

## Flow

`catga-flow` provides both a small compensating local `Flow` and durable,
restart-safe flow definitions. Keep side effects in named step handlers and
choose a caller-owned `FlowStore` for durable work:

```toml
[dependencies]
catga-core = "0.1"
catga-flow = "0.1"
```

```rust,no_run
use catga_core::CatgaResult;
use catga_flow::Flow;

async fn reserve_then_charge() -> CatgaResult<()> {
    let result = Flow::new("checkout")
        .step(|| async { Ok(()) }, || async { Ok(()) })
        .step(|| async { Ok(()) }, || async { Ok(()) })
        .run()
        .await;

    assert!(result.is_success());
    assert_eq!(result.completed_steps(), 2);
    Ok(())
}
```

For a durable flow, construct `FlowDefinition`, `FlowRuntime`, and a store
explicitly. Completion events can be passed to `FlowCompletionAdapter`; it
does not start a polling task or decode transport messages for you.

## FlowStore

`catga-flow-store` keeps one public crate while compiling only the database
drivers that an application selects. Enable the smallest feature set needed by
your deployment:

| Backend | Dependency feature | Notes |
| --- | --- | --- |
| SQLite | `catga-flow-store = { version = "0.1", features = ["sqlite"] }` | Embedded SQL store. |
| MySQL | `features = ["mysql"]` | Uses a native SQLx MySQL pool. |
| PostgreSQL | `features = ["postgres"]` | Uses a native SQLx PostgreSQL pool. |
| SQL Server | `features = ["mssql"]` | Uses a bounded Tiberius pool. |
| Redis | `features = ["redis"]` | Re-exports Redis flow and suspension stores. |

Enable `tls-rustls` with a network SQL feature when the deployment requires
Rustls. Call each SQL store's `migrate` method during controlled startup, and
drive due work with the caller-owned scheduler API; adapters never create a
background worker.

## Features

- `catga-codec-memorypack` is the default bounded MemoryPack integration for
  envelopes, snapshots, and RPC frames.
- `catga-codec-bincode` supplies an independent `bincode-next` payload codec
  for the format-neutral Core payload traits.
- `catga-memory` is useful for deterministic local composition and tests.
- `catga-nats`, `catga-redis`, `catga-robustmq`, `catga-cluster`,
  `catga-axum`, and `catga-scheduler-tokio-cron` are opt-in adapters. Their
  external connection settings are always explicit.

## External services and boundaries

NATS JetStream tests start and remove an isolated Testcontainers server when
`CATGA_NATS_URL` is unset; set it to test an externally managed NATS service.
Redis, MySQL, PostgreSQL, SQL Server, and RobustMQ tests are real-service E2E
tests marked `#[ignore]` locally. Provide the matching `CATGA_*_URL` and run
the target with `-- --ignored`; CI provisions all of these services and runs
the ignored E2E targets. A default local test run therefore does not prove an
external-service contract beyond its automatic NATS container coverage.

RabbitMQ/AMQP, Flow hot reload, and an HTTP health endpoint are intentionally
not part of this Rust workspace. Use OpenTelemetry-compatible tracing and
metrics from the public crate APIs for observability instead.

## Verification

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --doc --workspace --all-features
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps
```

## License

MIT. See [LICENSE](LICENSE).
