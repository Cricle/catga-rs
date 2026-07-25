# Catga Rust Core Design

## Goal

Create the first production-ready stage of a pure-Rust port of Catga: a fast
CQRS mediator, message pipeline, in-memory transport and persistence, plus
Redis and NATS adapters. The full source-library port remains the program
goal. HTTP/Axum integration, Flow, event sourcing, cluster support, and
schedulers are expanded in their own implementation milestones.

## Source Compatibility Boundary

The reference is `upstream-catga` at commit `4802f338245c8a6c95cf6db434ea6eb5ce618f54`.
This stage covers the semantic core of its `Catga`, `Catga.Transport.InMemory`,
`Catga.Persistence.InMemory`, `Catga.Transport.Redis`, and
`Catga.Transport.Nats` packages:

- Typed request/response commands, fire-and-forget commands, and events.
- Message identity, correlation identifiers, creation timestamps, QoS,
  delivery mode, priority, delay metadata, and typed failures.
- Handler execution, ordered request pipeline behavior, fan-out event
  delivery, batch and stream APIs.
- Idempotency, inbox, outbox, dead-letter, retry, and cancellation contracts.
- In-memory implementations and Redis/NATS transport adapters.

The Rust public API is idiomatic rather than source-level C# compatible. Each
source behavior receives an equivalent Rust contract and integration test;
users are not expected to emulate .NET dependency injection or reflection.

## Workspace Layout

```
catga-rs/
  crates/
    catga-core/             # Message contracts, mediator, pipeline, results
    catga-macros/           # Proc macros for declarations and registrations
    catga-memory/           # In-memory transport and persistence
    catga-redis/            # Optional Redis adapter
    catga-nats/             # Optional NATS / JetStream adapter
    catga-robustmq/         # Optional RobustMQ mq9 mailbox adapter
  tests/
    compatibility/          # Core behavioral tests derived from Catga tests
    integration/            # Redis/NATS tests, skipped without services
```

`catga-core` depends only on runtime-neutral abstractions. Adapter crates
depend on core, never the reverse. Feature flags keep Redis and NATS clients
out of the default dependency graph.

## Public API Design

Messages are normal Rust data types. `#[derive(Message)]` supplies metadata
defaults and compile-time type identifiers. `Request<Response>`, `Command`,
and `Event` are marker traits. A handler implements `Handler<M>` with an
async associated response type, while event handlers implement
`EventHandler<E>`.

The `catga_handlers!` macro generates typed-to-erased adapters and a
`Registry` constructor. Applications therefore register handlers in one
explicit, compile-time checked location:

```rust
let mediator = catga::Mediator::builder()
    .register(catga_handlers![
        CreateOrder => CreateOrderHandler,
        OrderCreated => [AuditOrder, NotifyCustomer],
    ])
    .build();
```

This keeps the simple application experience of Catga source generation
without global reflection or hidden runtime discovery. The macro reports
response-type and duplicate-command-handler errors at compile time.

`Mediator::send`, `send_batch`, `send_stream`, `publish`, and `publish_batch`
preserve source semantics. Scheduling APIs are traits in this phase and return
`Unsupported` unless an outbox scheduler implementation is registered.

## Runtime And Memory Design

The core uses Tokio and `Send + Sync + 'static` contracts. It stores a typed
handler router built once at startup; requests use a single type-erasure step
at the mediator boundary and do not scan registrations. Requests are moved
into their handler, not cloned. `CatgaResult<T>` is a compact `Result`-like
enum with structured error code and message, avoiding exception allocation on
the success path.

The in-memory adapter uses `DashMap` for independently keyed stores,
`parking_lot` locks for short critical sections, `tokio::sync` channels for
bounded backpressure, and cancellation-aware tasks. It exposes capacity and
worker-count configuration instead of unbounded queues. Batch dispatch uses
`FuturesUnordered` with a configured concurrency limit.

Metrics and tracing use `tracing` and optional OpenTelemetry integration;
all hot-path instrumentation is disabled when the feature is off.

## Persistence And Transport Contracts

`MessageTransport`, `InboxStore`, `OutboxStore`, `IdempotencyStore`, and
`DeadLetterStore` are async traits with explicit ownership, timeout, and
acknowledgement behavior. In-memory stores provide the reference baseline and
are test doubles only when the test is specifically adapter-independent.

`catga-redis` uses the `redis` crate with multiplexed async connections and
Redis Streams consumer groups. It provides at-least-once delivery,
acknowledgement, pending-message reclamation, and idempotency records with
TTL. It is verified against a real Redis container when `CATGA_REDIS_URL` is
set.

`catga-nats` uses `async-nats` and JetStream durable consumers. It provides
at-least-once delivery, explicit acknowledgements, redelivery handling, and
durable stream/consumer provisioning. It is verified against a real NATS
server when `CATGA_NATS_URL` is set.

Integration tests skip with an explicit message if the service URL is absent;
CI will later supply service containers so these adapters are not considered
fully verified merely by compiling.

`catga-robustmq` uses the `robustmq` Rust SDK's `MQ9Client` for persistent
mailboxes, priority delivery, and offline recipients. It remains a transport
adapter: Catga persistence is never coupled to RobustMQ internals. Standard
topic dispatch uses RobustMQ's NATS-compatible endpoint through the same NATS
transport contract; mq9 mailbox functions are an opt-in extension trait.
Integration tests use a real broker and verify send, receive, acknowledgement,
and redelivery.

## Reliability Pipeline

Pipeline behavior wraps requests in registration order. The phase implements
correlation propagation, validation hooks, bounded retry, timeout, logging,
idempotency, inbox/outbox, and dead-letter hooks as composable behaviors.
Retry only applies to errors marked transient. Idempotency keys are explicit
traits, not heuristic serialization. Outbox delivery persists before
transport publish and is delete-on-acknowledgement.

## Tests And Acceptance Criteria

Each behavior begins with a failing Rust test before implementation. The
initial suite proves:

- a request reaches exactly one typed handler and preserves its response;
- an event reaches every registered handler and reports dispatch failure;
- behavior order, cancellation, batch concurrency, and streaming semantics;
- correlation and message identifiers propagate without allocations in the
  normal request path beyond the user message itself;
- bounded in-memory transport applies backpressure and supports acknowledgements;
- inbox/outbox/idempotency/dead-letter state transitions are atomic per key;
- Redis Streams and NATS JetStream round trips, acknowledgements, and
  redelivery behavior pass against their real services.

The first stage is complete only when the default workspace test suite,
feature-specific adapter compilation, formatting, Clippy, and service-backed
Redis/NATS integration suites pass. Benchmarks compare direct typed dispatch
and mediator dispatch for allocations and throughput; no blanket numeric
performance claim is made before those measurements exist.

## Deferred Work

Subsequent specs cover event sourcing and snapshots, Flow DSL/Saga and state
machines, Axum integration, schedulers, cluster coordination,
source-compatible documentation/examples, and the remaining C# test-derived
compatibility matrix.
