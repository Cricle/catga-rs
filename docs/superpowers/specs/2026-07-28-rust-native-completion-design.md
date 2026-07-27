# Rust-native completion design

## Purpose

Complete the remaining audited C# Catga semantics while retaining explicit Rust
composition, bounded memory, deterministic tests, and caller-owned async
lifecycle. This design covers the three confirmed gaps: production retry
jitter, child-Flow completion routing, and state-machine event categories.

## Scope and exclusions

The implementation changes `catga-core` and `catga-flow` only. It adds no
RabbitMQ/AMQP adapter, Flow hot reload, HTTP health route, reflection-driven
discovery, dependency-injection container, global registry, hidden worker, or
background polling task. It does not change the C# reference.

## Decisions

### Retry defaults

`RetryBehavior::new` and `ResilienceExecutor::new` will use bounded full
jitter by default. Each instance holds one fixed-size atomic pseudo-random
state and samples a delay in the inclusive range `0..=base_delay`. The default
seed is a stable nonzero constant; therefore no system RNG, allocation, or
blocking entropy source is required. `RetryJitter::none` and
`RetryJitter::fixed` remain explicit options for deterministic applications and
tests. Existing `with_jitter` and `with_policies` constructors retain their
specified policy exactly.

### Flow completion adapter

`catga-flow` will expose a small, runtime-neutral `FlowCompletionAdapter`
which owns an `Arc<FlowRuntime<S>>` for a caller-selected `FlowStore` `S`. Its typed completion input contains a
bounded correlation id, child id, and either a bounded payload or `CatgaError`.
`record` delegates exactly once to the existing
`record_wait_success_by_correlation` or `record_wait_failure_by_correlation`
methods. Thus lookup, payload validation, durable version fencing,
idempotency, and resumption remain the responsibility of `FlowRuntime` and the
selected store. The adapter neither decodes wire data nor consumes a broker
delivery; a mediator or transport handler calls it after its own decode and
acknowledgement policy.

Duplicate, terminal, stale, unknown-correlation, and invalid-child outcomes
are returned as the existing structured result/error. The adapter introduces no
new retry, task, timer, queue, or persistence path.

### State-machine event categories

`catga-flow` will define a public `EventCategory` trait. An event opts into
category matching by returning a fixed-size, caller-declared list of category
`TypeId`s. State-machine transition definitions gain an explicit
category-transition constructor alongside the exact-event constructor. A
category transition receives an explicitly supplied extractor/adapter; it
cannot downcast arbitrary derived events or perform reflection.

At runtime, exact event-type transitions are checked first. Category
transitions are checked only for categories declared by the incoming event.
The definition validates duplicate registrations and bounds category
registrations per state, so routing remains bounded and immutable after
startup. Guards and actions preserve typed event access through their explicit
adapter. An event that does not declare a category never matches a category
transition.

## Data flow

1. An application receives and validates a child completion using its selected
   transport or mediator.
2. It passes the typed completion to `FlowCompletionAdapter::record`.
3. The adapter delegates to `FlowRuntime`; the store performs indexed lookup
   and version-fenced update, then the caller-owned runtime future resumes the
   parent where appropriate.
4. Separately, a state-machine caller submits an event. The immutable
   definition selects exact transitions first, then only explicitly declared
   categories, invokes typed guards/actions, and persists through the existing
   state-machine store path.

## Errors, safety, and observability

All invalid input and operational failure paths return `CatgaResult`; public
production paths add no `unwrap`, `expect`, or panic-based control flow.
Existing Flow and resilience tracing/metrics remain the observation surface;
new adapter failures preserve their error code for the application's handler
to record. Full jitter uses lock-free fixed-size state. Category lookup is
startup-built immutable data; no per-message registry mutation or task-local
allocation is required beyond already necessary typed dispatch.

## Tests and verification

Integration tests under `tests/` will prove:

- default retry and resilience constructors select full jitter, while explicit
  none/fixed policies remain deterministic;
- completion success, structured failure, duplicate delivery, unknown
  correlation, and payload-bound errors retain existing durable semantics;
- exact transitions win before category transitions; opt-in category matches,
  undeclared categories do not, and invalid duplicate/bound registrations fail
  without panicking.

The implementation is accepted only after formatting, targeted tests, the
workspace all-feature test suite, Clippy with warnings denied, Rustdoc with
warnings denied, and `git diff --check` pass. Service-dependent existing E2E
targets remain run with their explicit URLs or testcontainers configuration;
the new behavior itself is transport-neutral and deterministic.
