# catga-auto design

## Goal

`catga-auto` is a thin, typed application-composition facade. It reduces startup
boilerplate for web and real-time services without adding reflection, hidden
background tasks, dynamic dispatch on hot paths, or a second persistence model.
Existing `catga-core`, transport, Flow, cluster, and Axum APIs remain usable and
remain the implementation boundary.

## Boundaries

The first release has three layers:

1. `catga-auto` core provides an `AutoApp` builder, typed registration helpers,
   explicit shutdown ownership, and a small application state object. It depends
   only on `catga-core` and has no runtime-specific default.
2. The `axum` feature provides typed JSON route adapters, bounded body defaults,
   correlation propagation, and an application router builder. It delegates
   dispatch to the existing mediator and maps `CatgaError` through
   `catga-axum`.
3. Optional `nats`, `redis`, `flow`, and `cluster` features provide constructors
   that accept caller-owned clients/stores and configuration. They never create
   a task implicitly; every runner is returned to the caller for supervision.

The facade is intentionally not a replacement for an aggregate implementation.
For CQRS/event sourcing, applications keep explicit aggregate state and events,
while `catga-auto` removes repetitive registration and HTTP/consumer wiring.

## Correctness prerequisites

Before exposing durable auto builders, the existing runtime must satisfy these
contracts:

- NATS event-store append allocates versions with a broker-side compare-and-set
  for every append mode, rejects or recovers partial batches, and can rebuild its
  stream identifier index from retained subjects.
- NATS consumers always filter the configured subject, including shared streams.
- Projection cursor arithmetic is checked and projection replay semantics require
  idempotent apply or an atomic state/checkpoint implementation.
- Flow wait completion, due-service cancellation, and Raft committed-entry drain
  preserve a durable recovery trigger across crash/cancel windows.
- The distributed Todo example persists its read model or rebuilds its in-memory
  projection when a durable checkpoint exists; its Compose black-box test runs in
  CI and covers restart recovery.
- Release metadata uses one version source for manifests, README snippets, and
  release tags.

## TDD slices

Each slice starts with a focused failing test, then the smallest implementation,
then a full affected-crate test and Clippy run:

1. NATS append concurrency, partial failure, index recovery, and subject filter.
2. Projection overflow and idempotent/restart behavior.
3. Flow and Raft crash-window regression tests.
4. `catga-auto` core builder and typed registration.
5. Axum command/query routes and request limits.
6. Explicit consumer/projection lifecycle and graceful shutdown.
7. End-to-end NATS web example and CI/release verification.

## Performance contract

`catga-auto` performs all registration, validation, and route construction at
startup. Dispatch uses existing typed or statically registered handlers. It must
not add a per-request reflection lookup, an unbounded allocation, or an implicit
task. Any convenience adapter that requires dynamic dispatch is opt-in and kept
off the typed hot path.

## cqrs-es comparison

`cqrs-es` remains the narrower and more mature choice for aggregate-centric CQRS,
event repositories, upcasters, view repositories, SQL adapters, and its testing
framework. Catga's differentiated surface is the distributed runtime around
those domain contracts: durable transports, competing consumers, Flow, outbox /
inbox, NATS/Redis/RobustMQ, Raft, leases, and scheduling. `catga-auto` makes that
broader surface easier to compose; it does not claim to replace the mature
aggregate/event-store ecosystem.
