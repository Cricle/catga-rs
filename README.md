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

| Need | Start with | Next step |
| --- | --- | --- |
| A typed local command or query | [`mediator`](examples/src/quickstart/mediator.rs) | Add policies or a durable boundary. |
| The lowest-overhead fixed handler set | [`typed_mediator`](examples/src/quickstart/typed_mediator.rs) | Use compile-time dispatch on a hot path. |
| Routed messages and worker topology | [`bus_cqrs`](examples/src/runtime/bus_cqrs.rs) | Choose an application-owned transport. |
| An HTTP application | [`order_service`](examples/src/web/order_service.rs) | Replace in-memory adapters with durable ones. |
| A real multi-process deployment | [`distributed Todo`](examples/distributed-todo/compose.yaml) | Run Axum, NATS JetStream, consumer, and projection together. |

The complete ordered path, prerequisites, and commands are in
[Examples](docs/examples.md).

## Install

Choose the smallest crate that owns the contract you need:

```toml
[dependencies]
catga-auto = "0.0.2"
catga-core = "0.0.2"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

| Need | Dependency |
| --- | --- |
| Compensating or durable flows | `catga-flow = "0.0.2"` |
| Bounded local adapters and deterministic tests | `catga-memory = "0.0.2"` |
| SQL or Redis durable Flow state | `catga-flow-store = { version = "0.0.2", features = ["sqlite"] }` |
| NATS, Redis, RobustMQ, cluster, Axum, or cron | Add the matching opt-in `catga-*` crate. |

## Run An Example

Run a local program without Docker:

```bash
cargo run -p catga-examples --bin mediator
```

Run the full distributed reference application:

```bash
docker compose --file examples/distributed-todo/compose.yaml up --build
examples/distributed-todo/verify.sh
```

The Todo API publishes durable commands to JetStream. A typed competing consumer
appends events, and the API rebuilds its in-memory read model through a durable
projection checkpoint after restart. The API is publish-only and does not create
an idle consumer. Configure production resource names and identities with the
documented `CATGA_TODO_*` environment variables.

## Production Boundaries

- Delivery, Flow recovery, and retries are at-least-once. Make external effects
  idempotent with an application-owned stable key.
- Run migrations in controlled startup, then supervise consumers, schedulers,
  outbox processors, and shutdown in application-owned tasks.
- Select only the Cargo features and adapters your deployment uses.
- Use `CompetingConsumer` for durable production receive loops. `process_next`
  is a useful one-message composition and test helper.
- Keep database connection pools application-owned. Catga exposes configuration
  but does not impose a connection count on the application.

## Performance

The current release-mode benchmark table, measurement scope, database durability
analysis, and reproduction commands live in [Performance](docs/performance.md).
Release artifacts contain the complete machine-readable JSON reports.

## Documentation

- [Examples](docs/examples.md): ordered runnable programs and the distributed
  Todo reference application.
- [Performance](docs/performance.md): full throughput, latency, memory, and
  database durability report.
- [Catga application guide](skill/SKILL.md): API selection and ownership rules.
- [Transport guide](skill/transport.md): transports, typed delivery, RPC, and
  production consumption patterns.
- [Reliability guide](skill/reliability.md): outbox, inbox, idempotency,
  dead-lettering, and competing consumers.

## Verification

```bash
cargo check -p catga-examples --bins
cargo test -p catga-examples --all-features
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

The strict Docker E2E and coverage gate runs only for release tags and manually
dispatched performance workflows.

## License

MIT. See [LICENSE](LICENSE).
