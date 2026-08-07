---
name: catga
description: A guide for developing CQRS, Event Sourcing, workflow, and distributed message applications using the Catga Rust library (catga-core, catga-flow, catga-flow-store, catga-memory, catga-nats, catga-redis, catga-axum, catga-cluster, catga-testing and other catga-* crates). Use when users write, debug, or refactor Rust code with Catga, or when they mention Catga concepts like Mediator, catga_handlers, catga_typed_mediator, catga_pipeline, Request/Command/Event, Handler, Flow, DslFlow, FlowDefinition, FlowRuntime, StateMachine, FlowStore, MessageTransport, TypedTransport, MemoryTransport, Envelope, Outbox, Inbox, idempotency, dead letter, Aggregate, EventStore, snapshot, Projection, ReadModel, Raft, cluster, snowflake ID, lease, cron scheduling, MemoryPack, Axum integration, compensating flow, etc.
---

# Catga Application Development Guide

Catga is a pure-Rust CQRS, Event Sourcing, workflow, and distributed runtime workspace. This skill guides how to write **application code** using its public API.

## Design Philosophy (Determines Code Style)

1. **Explicit composition, no implicit mechanisms**: No reflection, no service locator, no hidden background threads, no unbounded queues. All dependencies are explicitly constructed at startup and passed in.
2. **Caller owns lifecycle**: Constructing any `Registry`, `Mediator`, `FlowRuntime`, store, or transport does not start background tasks. Polling, scheduling, recovery, and shutdown are all driven explicitly by the application's supervisor task.
3. **Boundaries are swappable**: Write application code with in-memory adapters (`catga-memory`) first; when you need persistence/distribution, only replace the corresponding boundary (e.g., replace `MemoryTransport` with NATS), and the application model remains unchanged.
4. **At-least-once semantics**: Flow retries, transport redelivery, and timeout recovery are all at-least-once. External side effects (payments, emails, etc.) must be backed by idempotency keys in the application; Catga does not automatically make retries safe.
5. **Prefer bounded**: Batches, pagination, and buffers all have explicit upper bounds (`MAX_*` constants); timeouts and retry counts must be finite.

## Crate Selection

Start with the smallest crate that has the required contracts, and add more as needed. Do not enable all adapters by default.

| Requirement | Dependency |
| --- | --- |
| In-process typed request/command/event (required core) | `catga-core = "0.0.2"` |
| Compensating / persistent workflows | `catga-flow = "0.0.2"` |
| Bounded memory adapter, deterministic tests | `catga-memory = "0.0.2"` |
| SQL/Redis persistent Flow state | `catga-flow-store = { version = "0.0.2", features = ["sqlite"] }` |
| NATS transport and JetStream storage | `catga-nats = "0.0.2"` |
| Redis transport and storage | `catga-redis = "0.0.2"` |
| RobustMQ transport (mq9 mailbox) | `catga-robustmq = "0.0.2"` |
| Axum HTTP integration | `catga-axum = "0.0.2"` |
| Cluster/Raft, singleton tasks, leader-only execution | `catga-cluster = "0.0.2"` |

Runtime requires `tokio = { version = "1", features = ["macros", "rt-multi-thread"] }`; struct handlers require `async-trait = "0.1"`.

## Quick Start (Minimal Runnable)

```toml
[dependencies]
catga-core = "0.0.2"
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
    // catga_handlers! builds the Registry at startup; duplicate registration of request/command reports Conflict
    let mediator = Mediator::new(catga_handlers! {
        request Double => request_handler(|request: Double| async move { Ok(request.0 * 2) })
    }?);
    let result = mediator.send(Double(21)).await?;
    assert_eq!(result, 42);
    Ok(())
}
```

## Three Message Roles (CQRS)

- **Request**: Exactly one handler, returns typed response (`mediator.send`).
- **Command**: Exactly one handler, no response (`mediator.send_command`).
- **Event**: Zero or more handlers, fan-out (`mediator.publish`), message type must be `Clone`.

## Writing Rules (Must Follow)

1. All fallible APIs return `CatgaResult<T>`, use `?` for propagation; make decisions at application boundaries using `error.code()` (`ErrorCode`) and `error.is_retryable()`, not by matching error text.
2. Handler, behavior, store, and transport instances are constructed **once at startup** and explicitly shared (usually `Arc`); do not create new ones on each request.
3. SQL stores call `migrate()` during controlled startup; only start processing flows after migration succeeds.
4. Schedulers, outbox processors, and receive loops run in **tasks owned by the application**; adapters do not spawn for you.
5. Before retrying external side effects, select an idempotency key (durable flows use a stable flow ID + step name derivation).
6. Set finite command timeouts and bounded batch sizes; before accepting untrusted input, select pages/batches within `MAX_*` limits.
7. Hot path with handler set known at startup → use `catga_typed_mediator!` (zero-allocation monomorphized dispatch); runtime registration or need for `Arc<Mediator>` sharing → use dynamic `Mediator`.

## Quick Reference

| Scenario | Entry Point |
| --- | --- |
| Single-process typed request/query | `Mediator` + `catga_handlers!` (see [mediator.md](mediator.md)) |
| Zero-allocation high-throughput dispatch | `catga_typed_mediator!` (see [mediator.md](mediator.md)) |
| Request needs retry/timeout/authorization/validation | `catga_pipeline!` + built-in Behavior (see [pipeline.md](pipeline.md)) |
| Local compensating multi-step operations | `Flow` / `compensating_flow!` (see [flow.md](flow.md)) |
| In-process stateful complex branching flows | `DslFlow` (see [flow.md](flow.md)) |
| Flows needing restart recovery/waiting for external results/timed recovery | `FlowDefinition` + `FlowRuntime` + durable store (see [flow.md](flow.md), [stores.md](stores.md)) |
| Event-driven entity state transitions | `StateMachine` (see [state-machine.md](state-machine.md)) |
| Local publish/confirm messages | `MemoryTransport` (see [transport.md](transport.md)) |
| Typed message direct send (no manual Envelope writing) | `TypedTransport` (see [transport.md](transport.md)) |
| Cross-process messages (NATS/Redis/RobustMQ) | Corresponding `catga-*` transport adapter (see [transport.md](transport.md)) |
| Cross-process request-response (RPC) | `*RequestClient` / `*RequestServer` (see [transport.md](transport.md)) |
| Reliable message sending after database write | Outbox: `OutboxBehavior` + `OutboxProcessor` (see [reliability.md](reliability.md)) |
| Consumer deduplication / interface idempotency | `InboxBehavior` / `IdempotencyBehavior` (see [reliability.md](reliability.md)) |
| Terminal isolation of failed messages | Dead letter `DeadLetterStore` (see [reliability.md](reliability.md)) |
| Consumer loop / competing consumers | `CompetingConsumer` / `SubscriptionRunner` (see [reliability.md](reliability.md)) |
| Event-sourcing aggregate | `Aggregate` + `AggregateRepository` + `EventStore` (see [event-sourcing.md](event-sourcing.md)) |
| Snapshot / event upgrade / time travel | See [event-sourcing.md](event-sourcing.md) |
| Projection and read model sync | `Projection` / `ReadModelSynchronizer` (see [event-sourcing.md](event-sourcing.md)) |
| Persistent storage backend selection | [stores.md](stores.md) |
| Cluster coordination / Raft / leader-only / singleton tasks | [distributed.md](distributed.md) |
| Distributed unique ID / lease / cron scheduling | [distributed.md](distributed.md) |
| Axum HTTP service | `MediatorState` + `CatgaHttpResult` (see [http.md](http.md)) |
| Codec/compression/message signing | [codec.md](codec.md) |
| Testing (spy/harness/assertions) | `catga-testing` (see [production.md](production.md)) |
| Error classification, retry decisions, production checklist | [production.md](production.md) |

## Reference Files

- [mediator.md](mediator.md) — Message traits, handlers, registration macros, dispatch APIs, typed mediator
- [pipeline.md](pipeline.md) — `catga_pipeline!` and all built-in Behavior
- [flow.md](flow.md) — Local Flow, `DslFlow`, persistent `FlowDefinition`/`FlowRuntime`
- [state-machine.md](state-machine.md) — Event-driven state machine (builder, transitions, persistent execution)
- [transport.md](transport.md) — `MessageTransport` contract, Envelope, memory/NATS/Redis adapters, TypedTransport, RPC, routing
- [reliability.md](reliability.md) — Outbox/Inbox/idempotency/dead-letter/persistent subscription/competing consumer loop
- [event-sourcing.md](event-sourcing.md) — Aggregate, EventStore, snapshot, event upgrade, time travel, projection, read model
- [stores.md](stores.md) — `catga-flow-store` backends, connection/migration, NATS/Redis/memory store matrix
- [distributed.md](distributed.md) — cluster/Raft/leader-only/singleton tasks, snowflake ID, lease, cron scheduling
- [http.md](http.md) — catga-axum: MediatorState, error mapping, context propagation, cluster routing
- [codec.md](codec.md) — MemoryPack/bincode codec, compression, HMAC message signing
- [production.md](production.md) — `CatgaError`/`ErrorCode`, idempotency and retry guidelines, lifecycle, observability, testing tools, validation commands

## Runnable Examples in Repository

This repository includes examples that run without Docker; see
[`docs/examples.md`](../docs/examples.md) for scenario grouping and complete run instructions. Reference before writing code:

```bash
cargo run -p catga-examples --bin mediator          # Minimal mediator
cargo run -p catga-examples --bin typed_mediator    # Zero-allocation typed mediator
cargo run -p catga-examples --bin flow              # Local compensating Flow
cargo run -p catga-examples --bin memory_transport  # Memory transport publish/receive/ack
cargo run -p catga-examples --bin checkout          # CQRS + Flow compensation + event acknowledgment
cargo run -p catga-examples --bin order_service     # Full HTTP order service (axum + cluster)
```
