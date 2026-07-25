# Observability Contract Design

## Goal

Complete the first observability migration slice for the pure-Rust Catga
workspace. The slice must expose the upstream framework's operational signals
for event storage, inbox and idempotency claims, outbox delivery, leases, and
resilience without introducing a .NET-style global registry, background
workers, or panic paths.

## Scope

The existing mediator request, event, and pipeline signals remain unchanged.
This slice adds operation metrics and tracing spans for the durable core
contracts and their in-memory, Redis, and JetStream implementations, plus
producer-side metrics and spans for every supported transport publish boundary.
Transport receive lifecycle remains a separate follow-up because it owns
long-lived consumer state rather than one caller-owned future. Durable Flow
runtime lifecycle and registered-step instrumentation use their own explicit
claim-and-persist ownership boundaries in this slice.

## Rust API

`catga_core::telemetry` will be a documented public module intended for Catga
adapter authors. It exports the stable tracing target and metric-name constants
and a small `Operation` guard. An adapter starts an operation with borrowed
static component and operation names, awaits its existing storage call, and
passes the resulting `CatgaResult` to the guard. The guard records exactly once
when explicitly completed or dropped.

The guard owns only `Instant`, `tracing::Span`, and static references. It does
not allocate, lock, spawn, or capture a request future. Its `Drop` path records
an aborted operation as a failure, so cancellation cannot leave an active span
or gauge silently open. The explicit completion path records an outcome tag and
duration histogram before returning the original result unchanged.

Metric names use the established `catga.*` namespace. Every persistence record
carries the bounded static labels `backend`, `component`, `operation`, and
`outcome`; dynamic identifiers such as stream, key, envelope id, and lease
owner stay in tracing spans only. This prevents a cardinality-driven memory
failure in Prometheus-like recorders while retaining inspectable diagnostics.

## Instrumentation Boundaries

`EventStore` implementations report append and read-family operations;
`InboxStore` and `IdempotencyStore` report claim, completion, failure, state,
and result operations; `OutboxStore` reports enqueue, claim, acknowledge,
release, and cancel operations; `LeaseStore` reports acquire, renew, and
release. `OutboxProcessor` also emits durable delivery success/failure counts.
`RetryBehavior` emits every actual retry, and `CircuitBreakerBehavior` emits an
opening transition only when it changes state.

The implementation adds no blanket trait wrapper. That would force users to
wrap every store and would obscure which backend performed the I/O. Each
adapter instruments its own existing operation around its real await or
lock-free state transition.

Transport publishers use the same explicit ownership boundary. Each queue,
destination queue, broadcast, Core NATS, JetStream, and deduplicated JetStream
publish calls `record_message_publish` around its existing future. The helper
creates no task and preserves the adapter result. It emits
`catga.messages.published`, `catga.messages.failed`, or
`catga.messages.aborted`, together with the
`catga.messages.publish.duration` histogram. Labels contain only static
`backend` and `mode` values; subjects, destination names, and message ids stay
out of metric labels. A producer tracing span carries the same bounded fields
and is dropped cleanly when caller cancellation aborts the future.

For NATS ExactlyOnce publications, JetStream owns the deduplication window.
When its publish acknowledgement reports `duplicate`, the adapter records the
unlabelled `catga.nats.dedup.drops` counter while retaining successful publish
semantics. The adapter deliberately has no local identity cache or eviction
metric: a process-local cache would add memory pressure and could disagree with
the broker's cross-client result.

`FlowRuntime` records lifecycle signals only after its existing durable
ownership transitions succeed. A successful continuation creation records a
start, every registered step records an attempt, outcome, and duration, and a
successful terminal persistence records completion or failure. Its active-flow
gauge measures only currently executing claimed `drive` futures, never stored
waiting continuations. An atomic RAII guard restores that gauge and records an
`aborted` duration if cancellation drops the caller future. This makes the
gauge restart-safe without retaining unbounded flow identities in process
memory. Flow IDs, definition names, and step names remain tracing fields, not
metric labels.

## Error and Cancellation Semantics

Telemetry is observational. It must never replace, mask, retry, or panic on a
storage or behavior error. Dropping an incomplete guard records an `aborted`
outcome but has no effect on its caller. Static labels are chosen at compile
time; externally supplied ids are recorded only by tracing's structured fields
and only where enabled.

## Tests and Verification

Integration tests install a local `metrics` recorder and a scoped tracing
subscriber, run representative successful, failed, and cancelled operations,
then assert metric names, static labels, and unchanged Catga error codes.
Unit tests prove a completed guard records once and an uncompleted guard records
the aborted outcome. Redis and JetStream tests retain their existing
environment-gated service boundary: when their endpoint variables are absent,
the code is compile-checked but no claim is made that a live server was
exercised. The suite must pass formatting, Clippy with warnings denied, Rustdoc
with warnings denied, the focused integration tests, the full workspace tests,
the production panic audit, the forbidden-adapter text audit, and `git diff
--check`.
