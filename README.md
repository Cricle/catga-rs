# Catga for Rust

Catga is a pure-Rust CQRS, event-sourcing, workflow, and distributed-runtime
workspace. Applications compose typed, bounded components explicitly: there is
no reflection, service locator, hidden worker, or unbounded queue.

Start with a small in-memory program, then replace only the boundary that needs
to become durable or distributed. This keeps ordinary application code short
while leaving transports, stores, codecs, authentication, and scheduling under
your control.

## Choose a starting point

| If you need to… | Start here | Then add |
| --- | --- | --- |
| Send a typed command or query in one process | [`mediator`](examples/src/bin/mediator.rs) | `catga-core` handlers and optional pipelines |
| Run a compensating sequence of local steps | [`flow`](examples/src/bin/flow.rs) | `catga-flow` and a durable `FlowStore` when restarts matter |
| Publish and acknowledge messages locally | [`memory_transport`](examples/src/bin/memory_transport.rs) | NATS, Redis, RobustMQ, or an application transport implementation |
| Build a complete HTTP checkout service | [`order_service`](examples/src/bin/order_service.rs) | durable stores, outbox worker, and a production cluster deployment |

Each example is runnable without Docker or credentials. They demonstrate the
same public traits used in production, so moving from local development to a
real service does not require changing the application model.

## Install and run

Start with the crate that owns the contract you need. `catga-core` provides
typed messages, handlers, pipelines, and transport traits; the other crates
are opt-in implementations and integrations.

```toml
[dependencies]
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
cargo run -p catga-examples --bin order_service
```

Their source lives in [`examples/src/bin`](examples/src/bin).

They are deliberately in-memory. Select a production transport or store only
where the application crosses that boundary.

## Performance snapshot

The latest complete release-mode Docker run is available as the
[performance artifact](https://github.com/Cricle/catga-rs/suites/82436920246/artifacts/8709395607).
It ran the functional E2E preflight and every manual benchmark on commit
[`76d49dc`](https://github.com/Cricle/catga-rs/commit/76d49dc81b62598ceec9e7a825575e3a3a71b889).
The figures below are observations from that shared CI runner, not performance
thresholds or hardware-independent guarantees.

That historical artifact predates the structured storage benchmark. The next
manual or release run writes one complete JSON report per benchmark, including
SQLite, MySQL, PostgreSQL, SQL Server, and Redis FlowStore lifecycles, then
renders their payload sizes, latency scope, p50/p95/p99, process RSS, and Docker
container statistics into the published artifact.

| Source | Benchmark | Operations | Throughput (ops/s) | p50 | p95 | p99 | RSS before / after / peak |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Memory | Tokio mpsc round-trip lower bound | 4,096 | 3,318,426 | 230ns | 281ns | 431ns | 3.10 / 3.17 / 3.17 MiB |
| Memory | Catga publish / receive / ack | 4,096 | 967,975 | 962ns | 1.03µs | 1.24µs | 3.20 / 3.20 / 3.20 MiB |
| Memory | Mediator request | 4,096 | 3,058,275 | 281ns | 291ns | 331ns | 3.20 / 3.20 / 3.20 MiB |
| Memory | Three-step local Flow | 4,096 | 3,847,089 | 200ns | 221ns | 241ns | 3.80 / 3.71 / 3.86 MiB |
| Memory | Retain 4,096 outbox records (256B payload) | 4,096 | 1,013,808 | 511ns | 3.77µs | 5.22µs | 3.71 / 6.45 / 6.45 MiB |
| In-process | CQRS + Flow + transport workflow | 4,096 | 452,452 | — | — | — | — |
| In-process | Bounded mediator batch scheduler | 4,096 | 1,725,882 | — | — | — | — |
| In-process | Local Flow execution | 4,096 | 3,072,480 | — | — | — | — |
| In-process | Local DSL Flow execution | 4,096 | 577,910 | — | — | — | — |
| NATS JetStream | Durable publish / receive / ack | 1,000 | 2,040 | — | — | — | — |
| Docker E2E | Axum HTTP quote | 512 | 15,028 | 61.3µs | 89.8µs | 118.6µs | — |
| Docker E2E | NATS JetStream round-trip | 512 | 2,065 | 472.3µs | 553.3µs | 646.4µs | — |

The Tokio row is deliberately only a lower bound: it does not include Catga's
delivery acknowledgement, lifecycle-drain tracking, bounded telemetry, or
typed error contract. The outbox row retains 1MiB of payload plus record and
index metadata. Run `scripts/performance.sh --profile full` manually or from a
release workflow to produce the current machine-readable JSON reports and the
complete Markdown total table.

## Quick start

Start with `catga-core` and register handlers during application startup:

```toml
[dependencies]
async-trait = "0.1"
catga-core = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust,no_run
use catga_core::{CatgaResult, Mediator, Request, catga_handlers, request_handler};

struct Double(u64);
impl catga_core::Message for Double {}
impl Request for Double {
    type Response = u64;
}

#[tokio::main]
async fn main() -> CatgaResult<()> {
    let mediator = Mediator::new(catga_handlers! {
        request Double => request_handler(|request: Double| async move { Ok(request.0 * 2) })
    }?);
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

## Production checklist

1. Keep external effects idempotent. Flow retries, transport redelivery, and
   timeout recovery are deliberately at-least-once boundaries.
2. Select the smallest Cargo feature set for the services your deployment
   actually uses; do not enable every adapter by default.
3. Run store migrations during controlled startup, then run schedulers and
   receivers in application-owned supervised tasks.
4. Set finite command timeouts and bounded batch sizes. Redis command adapters
   use a finite response timeout by default; stream long-polling is isolated.
5. For Raft HTTP ingress, put mTLS or signed-frame authentication in front of
   `raft_message_route`, attach the verified `RaftPeerIdentity`, and configure
   `StaticRaftInboundPolicy` with the local node and its trusted peers.

These choices are explicit because the caller, not a framework global, owns
availability, credentials, retry budgets, and graceful shutdown.

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
