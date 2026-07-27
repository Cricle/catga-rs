# Catga for Rust

Catga is a pure-Rust CQRS, event-sourcing, workflow, and distributed-runtime
workspace derived semantically from the upstream Catga library. It favors
explicit construction, typed contracts, bounded concurrency, and compact
owned data over reflection, global service locators, and unbounded task or
queue growth.

## Crates

| Crate | Purpose |
| --- | --- |
| `catga-core` | Typed mediator, pipeline contracts, reliability stores, event sourcing, and lifecycle APIs. |
| `catga-memory` | Bounded in-memory transport and persistence implementations. |
| `catga-codec-memorypack` | Bounded MemoryPack envelope, request/reply, snapshot, and scheduled-outbox codecs. |
| `catga-codec-bincode` | Bounded `bincode-next` payload codec for format-neutral Core transport contracts. |
| `catga-flow` | Durable Flow DSL, state machines, suspension, and scheduling. |
| `catga-flow-store` | Feature-gated SQLite, MySQL, PostgreSQL, SQL Server, and Redis durable Flow stores. |
| `catga-scheduler-tokio-cron` | Opt-in `tokio-cron-scheduler` adapter for explicit, bounded durable-flow due sweeps. |
| `catga-redis` | Redis Streams transport and Redis-backed persistence. |
| `catga-nats` | JetStream transport and NATS-backed persistence. |
| `catga-axum` | Typed Axum routes and cluster forwarding. |
| `catga-cluster` | Raft coordination, persistence, and single-owner state-machine runtimes. |
| `catga-macros` | Compile-time message and handler registration support. |
| `catga-testing` | Handler spies and an explicit integration-test harness. |

## Quick start

```rust,no_run
use async_trait::async_trait;
use catga_core::{CatgaResult, Handler, Mediator, Registry, Request};

struct Double(u64);
impl catga_core::Message for Double {}
impl Request for Double {
    type Response = u64;
}

struct DoubleHandler;
#[async_trait]
impl Handler<Double> for DoubleHandler {
    async fn handle(&self, message: Double) -> CatgaResult<u64> {
        Ok(message.0 * 2)
    }
}

# async fn run() -> CatgaResult<()> {
let mut registry = Registry::new();
registry.register_request::<Double, _>(DoubleHandler)?;
let mediator = Mediator::new(registry);
assert_eq!(mediator.send(Double(21)).await?, 42);
# Ok(())
# }
```

## Design guarantees

- Public operational failures use `CatgaResult`; production source does not
  use panic-prone `unwrap` or `expect`.
- Catga-authored public API documentation is a compile-time requirement. The
  vendored MemoryPack compatibility surface keeps its upstream API shape and
  is documented at its module boundary.
- Batch, transport, consumer, and outbox operations have explicit positive
  concurrency limits and retain only bounded in-flight work.
- The core has no dependency on adapter crates. Applications compose concrete
  stores and transports explicitly at startup.
- The excluded upstream transport adapter is not included in this workspace.

## Verification

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps
```

The all-feature gates compile every optional store and `streams-rpc` path. Live
database contracts are opt-in: set `CATGA_REDIS_URL`, `CATGA_MYSQL_URL`,
`CATGA_POSTGRES_URL`, or `CATGA_MSSQL_URL` before running the corresponding
`catga-redis` or `catga-flow-store` tests. The test targets skip their live
contract body when the matching URL is absent, so a local unit-test pass is not
evidence that an external service was exercised.

The source-level migration and its test evidence are tracked in
[`docs/source-compatibility-matrix.md`](docs/source-compatibility-matrix.md).

## License

MIT. See [`LICENSE`](LICENSE).
