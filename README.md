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
| Maximum-throughput dispatch (zero allocation) | [`typed_mediator`](examples/src/bin/typed_mediator.rs) | `catga_typed_mediator!` for compile-time monomorphized dispatch |
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
cargo run -p catga-examples --bin typed_mediator
cargo run -p catga-examples --bin flow
cargo run -p catga-examples --bin memory_transport
cargo run -p catga-examples --bin order_service
```

Their source lives in [`examples/src/bin`](examples/src/bin).

They are deliberately in-memory. Select a production transport or store only
where the application crosses that boundary.

## Performance snapshot

The latest complete release-mode Docker run is available from the
[performance workflow](https://github.com/Cricle/catga-rs/actions/runs/30416325626).
It ran the functional E2E preflight and every manual benchmark on commit
[`497042c`](https://github.com/Cricle/catga-rs/commit/497042c1849f2fb4bb9a1fe7166f133d2b59a80d).
The figures below are observations from that shared CI runner, not performance
thresholds or hardware-independent guarantees.

Every benchmark now emits machine-readable payload size, operation scope,
nearest-rank p50/p95/p99, and Linux process RSS; the artifact also contains
Docker container statistics. Storage rows measure the same 256-byte FlowStore
create, read, and optimistic-update lifecycle.

| Source | Benchmark | Throughput (ops/s) | p50 | p95 | p99 |
| --- | --- | ---: | ---: | ---: | ---: |
| Memory | Tokio mpsc round-trip lower bound | 3,017,307 | 260ns | 301ns | 451ns |
| Memory | Catga publish / receive / ack | 946,038 | 962ns | 1.05µs | 1.58µs |
| Memory | Mediator request | 2,987,781 | 281ns | 291ns | 361ns |
| Memory | Three-step local Flow | 3,863,579 | 200ns | 211ns | 320ns |
| Memory | Retain 4,096 outbox records | 1,098,454 | 441ns | 3.74µs | 4.98µs |
| In-process | CQRS + Flow + transport workflow | 557,414 | 1.67µs | 1.82µs | 2.93µs |
| In-process | Bounded mediator batch scheduler (batch) | 1,814 | 538.1µs | 590.8µs | 590.8µs |
| In-process | Local Flow execution | 2,723,667 | 290ns | 440ns | 571ns |
| In-process | Local DSL Flow execution | 581,837 | 1.71µs | 1.96µs | 2.19µs |
| NATS JetStream | Durable publish / receive / ack | 2,009 | 484.7µs | 617.2µs | 791.6µs |
| SQLite | FlowStore lifecycle | 863 | 1.19ms | 1.52ms | 3.21ms |
| MySQL | FlowStore lifecycle | 334 | 2.76ms | 4.29ms | 6.56ms |
| PostgreSQL | FlowStore lifecycle | 513 | 1.71ms | 3.08ms | 5.00ms |
| SQL Server | FlowStore lifecycle | 240 | 3.97ms | 5.00ms | 6.67ms |
| Redis | FlowStore lifecycle | 1,830 | 527.6µs | 636.9µs | 787.3µs |
| Docker E2E | Axum HTTP quote | 15,158 | 61.4µs | 93.1µs | 108.5µs |
| Docker E2E | NATS JetStream round-trip | 2,046 | 477.3µs | 560.0µs | 836.9µs |

The Tokio row is deliberately only a lower bound: it does not include Catga's
delivery acknowledgement, lifecycle-drain tracking, bounded telemetry, or
typed error contract. The outbox row retains 1MiB of payload plus record and
index metadata. Run `scripts/performance.sh --profile full` manually or from a
release workflow to produce the JSON reports and complete Markdown total table.

### Mediator dispatch micro-benchmarks

The following numbers measure pure in-process dispatch with no tracing
subscriber attached (the `span.is_disabled()` fast path). They reflect the
Vec-slot registry optimization that replaced `HashMap<TypeId>` with a
contiguous linear scan. Run locally with:

```bash
cargo test --release -p catga-tests --test mediator_pure_throughput -- --ignored --nocapture
```

| Path | Mode | Throughput | Avg latency |
| --- | --- | ---: | ---: |
| Request `send` | Concurrent (16 tasks) | 7.92 M msg/s | 126 ns |
| Request `send` | Sequential | 3.53 M msg/s | 283 ns |
| Request `send_batch` | 1024-batch, 64 concurrency | 2.48 M msg/s | 402 ns |
| Event `publish` | Concurrent (16 tasks, 1 handler) | 9.68 M events/s | 103 ns |
| Event `publish` | Sequential (1 handler) | 3.79 M events/s | 263 ns |
| Event `publish` | Sequential (3 handlers fan-out) | 2.63 M events/s | 379 ns |

Event publish is faster than request dispatch because it skips the response
`Box<dyn Any>` downcast. The 3-handler fan-out adds ~30% overhead from two
extra event clones.

These were observed on a Windows 11 workstation (4 tokio worker threads,
100,000 operations per measurement). They are not hardware-independent
guarantees; use them as a relative baseline for regression detection.

### Typed mediator (zero-allocation dispatch)

`catga_typed_mediator!` generates a concrete struct with typed handler fields.
Dispatch is monomorphized per message type at compile time — no `Box<dyn Any>`,
no `downcast`, no vtable indirection. Use it on the hot path when the handler
set is known at startup:

```rust,ignore
catga_typed_mediator! {
    pub struct AppMediator;
    request GetOrder => GetOrderHandler;
    command ShipOrder => ShipOrderHandler;
    event OrderCreated => [ProjectionHandler, AuditHandler];
}

let mediator = AppMediator::new(
    GetOrderHandler,
    ShipOrderHandler,
    [ProjectionHandler, AuditHandler],
);
let order = mediator.send(GetOrder { id: 1 }).await?;
```

| Path | Mode | Throughput | Avg latency |
| --- | --- | ---: | ---: |
| Request `send` | Concurrent (16 tasks) | 55.73 M msg/s | 17 ns |
| Request `send` | Sequential | 20.34 M msg/s | 49 ns |
| Event `publish` | Sequential (1 handler) | 16.18 M events/s | 61 ns |

Compared to the dynamic `Mediator` (Vec-slot registry), the typed mediator is
5.8× faster sequential and 7.0× faster concurrent. The dynamic mediator remains
the right choice when handlers are registered at runtime or when `Arc<Mediator>`
must be shared across heterogeneous boundaries.

### Flow and workflow benchmarks

| Benchmark | Throughput | Notes |
| --- | ---: | --- |
| Local Flow (3 steps) | 1,555,640 flows/s | Compensating sequence, in-memory |
| Local DSL Flow (3 steps) | 601,195 flows/s | Typed DSL with state threading |
| CQRS + Flow + transport workflow | 447,587 workflows/s | End-to-end critical path |
| NATS JetStream publish/receive/ack | 1,436 msg/s | Durable, 256B payload, Podman |

Run all benchmarks:

```bash
cargo test --release -p catga-tests --test mediator_pure_throughput --test typed_mediator_bench --test flow_performance --test critical_path_performance -- --ignored --nocapture
```

### Why the network database rows look slow

The FlowStore lifecycle (create + read + optimistic update) is dominated by
**per-commit durability fsync**, not client overhead. The client already issues
the minimal statements: the optimistic update is a single conditional `UPDATE`
with no pre-read round trip (a contract checked by `tests/performance_workflow.rs`).

Every SQL backend flushes its write-ahead or redo log to disk on each commit in
its default configuration (MySQL `innodb_flush_log_at_trx_commit=1`, PostgreSQL
`synchronous_commit=on`, SQL Server full recovery with a per-commit log flush).
On a virtualized or shared disk that flush costs roughly 1-10ms, which caps
serial (concurrency=1) throughput. Redis avoids it because its persistence is
asynchronous, and SQLite avoids it because the WAL journal only syncs on
checkpoint.

The serial number is a worst case, not the deployment reality. Two observations
isolate the fsync cost. All Catga SQL store constructors now enable SQLite WAL
with `synchronous=NORMAL`, and the same-host SQLite FlowStore lifecycle rises
from ~416 to ~2,100 ops/s (about 5x) purely from skipping the per-commit fsync.
Disabling durability on the network databases (a diagnostic, not a
recommendation) confirms the same cause:

| Backend | Durable default (c=1) | Durability disabled (c=1) | Isolated fsync cost |
| --- | ---: | ---: | ---: |
| MySQL | 85 ops/s | 424 ops/s (`innodb_flush_log_at_trx_commit=2`, `sync_binlog=0`) | ~5x |
| PostgreSQL | 219 ops/s | 577 ops/s (`synchronous_commit=off`) | ~2.6x |

### Durability need not be sacrificed

The fsync cost is amortized, not eliminated, by the levers below; all of them
keep every commit fully durable.

- **Concurrency drives database group commit.** The engine coalesces the fsyncs
  of concurrent transactions into fewer disk syncs. Measured with full
  durability at concurrency=16, MySQL reaches ~547 ops/s and PostgreSQL ~1,591
  ops/s — both *higher* than the durability-disabled serial numbers above. Real
  workloads run many concurrent flow workers, so they benefit from this without
  any configuration change.
- **Batch writes into fewer transactions.** Committing N flow-state changes in
  one transaction pays one fsync for all N records. This is the application-level
  lever for low-concurrency writers and preserves durability completely.
- **Tune group commit, still durably.** PostgreSQL `commit_delay`/`commit_siblings`
  and MySQL `binlog_group_commit_sync_delay` briefly wait to merge more fsyncs,
  trading a little latency for throughput with no loss of durability.
- **Use faster durable storage.** NVMe with a battery-backed write-back cache
  makes each fsync microseconds instead of milliseconds while remaining
  power-loss safe. The virtualized disk under a local podman/WSL VM is the main
  reason the local numbers above are far below a production NVMe host.

Relaxing durability (the diagnostic table) is therefore a last resort, not the
intended tuning path. To reproduce these numbers locally with podman (no Docker
Desktop required), use:

```powershell
# Durable defaults for every backend.
./scripts/performance-local.ps1
# Reproduce the fsync-isolation experiment for MySQL and PostgreSQL.
./scripts/performance-local.ps1 -Backends postgres,mysql -RelaxedDurability
```

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
