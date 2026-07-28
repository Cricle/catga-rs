# Catga for Rust

Catga is a pure-Rust CQRS, event-sourcing, workflow, and distributed-runtime
workspace. Applications compose typed, bounded components explicitly: there is
no reflection, service locator, hidden worker, or unbounded queue.

## Install and run

Start with the crate that owns the contract you need. `catga-core` provides
typed messages, handlers, pipelines, and transport traits; the other crates
are opt-in implementations and integrations.

```toml
[dependencies]
async-trait = "0.1"
catga-core = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Add capabilities explicitly as the application needs them:

| Need | Dependency |
| --- | --- |
| Compensating or durable flows | `catga-flow = "0.1"` |
| Bounded local adapters and deterministic tests | `catga-memory = "0.1"` |
| SQL- or Redis-backed durable flow state | `catga-flow-store = { version = "0.1", features = ["sqlite"] }` (select the backend feature) |
| NATS, Redis, RobustMQ, cluster, Axum, or cron integration | Add the matching opt-in `catga-*` crate |

The repository keeps the introductory programs small and runnable:

```bash
cargo run -p catga-examples --bin mediator
cargo run -p catga-examples --bin flow
cargo run -p catga-examples --bin memory_transport
```

Their source lives in [`examples/src/bin`](examples/src/bin).

They are deliberately in-memory. Select a production transport or store only
where the application crosses that boundary.

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

## Errors and retries

Every fallible API returns `CatgaResult<T>`, an alias for
`Result<T, CatgaError>`. Propagate errors with `?` when the application
boundary can decide the response; at a boundary, inspect the stable category
and retry hint instead of matching error text:

```rust,no_run
use catga_core::CatgaResult;

async fn report(result: CatgaResult<u64>) -> CatgaResult<u64> {
    match result {
        Ok(value) => Ok(value),
        Err(error) if error.is_retryable() => {
            eprintln!("retry {}: {}", error.code().as_stable_str(), error.message());
            Err(error)
        }
        Err(error) => {
            eprintln!("reject {}: {}", error.code().as_stable_str(), error.message());
            Err(error)
        }
    }
}
```

Catga does not make a retry safe by itself. Before retrying a side effect,
choose an idempotency key and the persistence or transport guarantee that owns
duplicate handling.

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

Durable steps are at-least-once. Give external effects such as payments and
emails an idempotency key derived from the stable flow and step identity, and
run the scheduler or completion worker under application ownership.

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

## Extension points

Customize behavior at the contracts rather than behind a global runtime:

- Register `Handler`, `CommandHandler`, and `EventHandler` implementations
  with `catga_handlers!` during startup.
- Compose request policy with `catga_pipeline!` and caller-owned `Behavior`
  values; use the built-in retry, timeout, authorization, validation, and
  tracing behaviors where they fit.
- Implement `MessageTransport`, `EventStore`, `OutboxStore`, or the flow store
  traits when an adapter must match an existing system. `catga-memory` provides
  bounded implementations for local composition and deterministic tests.

This boundary keeps connection management, polling, retry policy, and shutdown
ownership in the application. Adapters expose operations; they do not create a
service locator or a background worker on your behalf.

## External services and boundaries

NATS JetStream tests start and remove an isolated Testcontainers server when
`CATGA_NATS_URL` is unset; set it to test an externally managed NATS service.
Redis, MySQL, PostgreSQL, and SQL Server tests are real-service E2E tests
marked `#[ignore]` locally. Provide the matching `CATGA_*_URL` and run the
target with `-- --ignored`; CI provisions those services and runs the ignored
E2E targets. RobustMQ's published mq9 SDK is exercised over real NATS with a
test-only mailbox-creation control-plane harness, because the public RobustMQ
broker image does not expose the mq9/NATS protocol. A default local test run
therefore does not prove an external-service contract beyond its automatic
NATS container coverage.

### Local Docker E2E

`scripts/e2e.sh` starts the required local services, exports the matching
`CATGA_*_URL` values with dynamically assigned loopback ports, runs the declared
scenario matrix, and requires at least 95% of the selected scenarios to pass.
Every scenario currently marked critical must pass regardless of that percentage.

```bash
bash scripts/e2e.sh --profile core
bash scripts/e2e.sh --profile sql
bash scripts/e2e.sh --profile full
```

The Docker Compose file is [testing/docker/compose.yaml](testing/docker/compose.yaml).
Set `CATGA_CONTAINER_IMAGE_PREFIX` to an OCI registry prefix without a trailing
slash to use an internal registry or domestic mirror. The selected registry
must mirror the `library/*` Docker Hub paths and the `azure-sql-edge` MCR path.
Azure SQL Edge keeps the SQL Server-compatible E2E profile small.

```bash
export CATGA_CONTAINER_IMAGE_PREFIX=registry.example.cn
bash scripts/e2e.sh --profile full
```

RabbitMQ/AMQP, Flow hot reload, and an HTTP health endpoint are intentionally
not part of this Rust workspace. Use OpenTelemetry-compatible tracing and
metrics from the public crate APIs for observability instead.

## Verification

```bash
cargo check -p catga-examples
cargo test -p catga-examples
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --doc --workspace --all-features
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps
```

## License

MIT. See [LICENSE](LICENSE).
