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
| `catga-codec-postcard` | Compact Postcard envelope, request/reply, snapshot, and scheduled-outbox codecs. |
| `catga-flow` | Durable Flow DSL, state machines, suspension, scheduling, and hot reload. |
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
- Public API documentation is a compile-time requirement.
- Batch, transport, consumer, and outbox operations have explicit positive
  concurrency limits and retain only bounded in-flight work.
- The core has no dependency on adapter crates. Applications compose concrete
  stores and transports explicitly at startup.
- The excluded upstream transport adapter is not included in this workspace.

## Verification

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps
```

The source-level migration and its test evidence are tracked in
[`docs/source-compatibility-matrix.md`](docs/source-compatibility-matrix.md).

## License

MIT. See [`LICENSE`](LICENSE).
