# Observability Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add allocation-conscious, cancellation-safe observability for Catga's durable core contracts and their in-memory, Redis, and JetStream adapters.

**Architecture:** `catga-core::telemetry` owns stable metric names and a small explicit operation guard. Core behaviors and each storage adapter start and complete the guard around their existing operation, so telemetry cannot change control flow or hide backend ownership. Tests use local recorders and subscribers rather than external telemetry services.

**Tech Stack:** Rust 2024, `tracing`, `metrics`, Tokio, async traits, existing integration test harness.

---

### Task 1: Define the telemetry operation contract

**Files:**
- Modify: `crates/catga-core/src/observability.rs`
- Modify: `crates/catga-core/src/lib.rs`
- Test: `tests/observability.rs`

- [x] **Step 1: Write failing operation-recording tests**

Add a metrics recorder assertion that a successful operation emits one
`catga.persistence.operations` counter with `backend=memory`,
`component=event_store`, `operation=append`, and `outcome=success`; add a
second assertion that dropping an incomplete operation emits `outcome=aborted`.

- [x] **Step 2: Run the focused test and verify it fails**

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test observability telemetry_operation --quiet`

Expected: compilation failure because the public `catga_core::telemetry`
operation API does not exist.

- [x] **Step 3: Implement the documented guard**

Expose `pub mod telemetry` and implement:

```rust
pub const PERSISTENCE_OPERATIONS: &str = "catga.persistence.operations";
pub const PERSISTENCE_DURATION: &str = "catga.persistence.duration";

pub fn persistence_operation(
    backend: &'static str,
    component: &'static str,
    operation: &'static str,
) -> Operation;
```

`Operation::complete(&mut self, &CatgaResult<T>)` must consume neither the
result nor the guard, must record at most once, and `Drop` must record only an
uncompleted operation as `aborted`. Record elapsed milliseconds from `Instant`
and use the existing `catga` tracing target.

- [x] **Step 4: Run the focused test and verify it passes**

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test observability telemetry_operation --quiet`

Expected: PASS.

- [x] **Step 5: Commit**

Do not commit in the current shared dirty worktree. Stage only the three listed
files when the worktree is clean enough for an isolated commit.

### Task 2: Instrument core behavior-owned durable operations

**Files:**
- Modify: `crates/catga-core/src/outbox_processor.rs`
- Modify: `crates/catga-core/src/behaviors/retry.rs`
- Modify: `crates/catga-core/src/behaviors/circuit_breaker.rs`
- Test: `tests/observability.rs`

- [x] **Step 1: Write failing outcome tests**

Add tests with the local recorder proving an outbox publish/ack increments
`catga.outbox.published`, a released delivery increments
`catga.outbox.failed`, a retry increments `catga.resilience.retries`, and a
closed-to-open circuit transition increments `catga.resilience.circuit.opened`
exactly once.

- [x] **Step 2: Run the focused tests and verify they fail**

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test observability durable_behavior_metrics --quiet`

Expected: FAIL because these metric emissions are absent.

- [x] **Step 3: Implement core instrumentation**

Call `telemetry::persistence_operation("core", "outbox", "flush")` around
one `flush_once` batch and record the named publish/failure counters from its
final `OutboxRun`. In `RetryBehavior`, increment the retry counter immediately
before each actual sleep. In `CircuitBreakerBehavior::open`, increment the
circuit counter only after a successful transition from a non-open state.

- [x] **Step 4: Run the focused tests and verify they pass**

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test observability durable_behavior_metrics --quiet`

Expected: PASS.

- [x] **Step 5: Commit**

Do not commit in the current shared dirty worktree. Stage only the listed files
when an isolated commit is possible.

### Task 3: Instrument the in-memory persistence implementations

**Files:**
- Modify: `crates/catga-memory/src/event_store.rs`
- Modify: `crates/catga-memory/src/inbox.rs`
- Modify: `crates/catga-memory/src/idempotency.rs`
- Modify: `crates/catga-memory/src/outbox.rs`
- Modify: `crates/catga-memory/src/lease.rs`
- Test: `tests/observability.rs`

- [x] **Step 1: Write failing memory-store tests**

Exercise one success and one conflict for every store family. Assert the
generic persistence counter has `backend=memory` and the matching static
component/operation/outcome labels. Do not assert durations or stream/key ids.

- [x] **Step 2: Run the focused tests and verify they fail**

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test observability memory_store_metrics --quiet`

Expected: FAIL because memory adapters currently perform no telemetry calls.

- [x] **Step 3: Instrument existing operation boundaries**

Start a guard at each public trait method and call `complete` exactly once with
the method's original result. Preserve lock-free `ArcSwap`, `DashMap`, and
atomic code paths; do not clone payloads or turn errors into telemetry errors.

- [x] **Step 4: Run the focused tests and verify they pass**

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test observability memory_store_metrics --quiet`

Expected: PASS.

- [x] **Step 5: Commit**

Do not commit in the current shared dirty worktree. Stage only the listed files
when an isolated commit is possible.

### Task 4: Instrument Redis and JetStream persistence implementations

**Files:**
- Modify: `crates/catga-redis/src/{event_store,inbox,outbox,lease}.rs`
- Modify: `crates/catga-nats/src/{event_store,inbox,idempotency,outbox,lease}.rs`
- Test: `tests/redis.rs`
- Test: `tests/nats.rs`

- [x] **Step 1: Write and run the reusable async-wrapper failure test**

Add a `record_persistence` test that returns an original conflict error and
asserts the `backend=redis`, `component=event_store`, `operation=append`, and
`outcome=failure` labels. Verify it fails before the wrapper exists.

- [x] **Step 2: Instrument every public persistence trait method**

Use `telemetry::record_persistence` around existing provider I/O with bounded
backend/component/operation labels. Preserve every existing Lua script,
JetStream CAS loop, error mapping, and bounded local collection.

- [x] **Step 3: Compile and run conditional integration suites**

Run `rtk cargo check -p catga-redis -p catga-nats` and the `redis` and `nats`
test targets. If endpoint variables are absent, record that the suites skip
their service calls rather than treating a skip as live-server verification.

### Task 5: Finalize documentation and quality gates

**Files:**
- Modify: `docs/source-compatibility-matrix.md`
- Test: `tests/observability.rs`

- [x] **Step 1: Document the Rust-native mapping**

Update the compatibility matrix to identify the generic bounded-label metric
contract and explicitly state that service-backed providers report their own
backend names without a global telemetry registry.

- [x] **Step 2: Verify public documentation**

Run: `rtk env RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps`

Expected: PASS.

- [x] **Step 3: Run quality gates**

Run:

```bash
rtk cargo fmt --check
rtk cargo clippy --workspace --all-targets -- -D warnings
rtk cargo test --workspace --all-targets --quiet
rtk rg -n '\\.(unwrap|expect)[[:space:]]*\\(|(unreachable|todo|unimplemented)![[:space:]]*\\(' crates --glob '*.rs' --glob '!catga-testing/**'
rtk git diff --check
```

Expected: all commands succeed except the production-panic audit, which must
produce no matches and therefore exit with status `1`.

- [ ] **Step 4: Commit**

Do not commit in the current shared dirty worktree. Preserve unrelated changes
and create a focused commit only after the worktree ownership is clear.
