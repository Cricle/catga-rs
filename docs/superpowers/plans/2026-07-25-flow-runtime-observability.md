# Flow Runtime Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Record bounded, cancellation-safe Flow runtime lifecycle and step metrics without changing durable execution semantics.

**Architecture:** Private RAII guards in `catga-flow::metrics` own an `Instant`, tracing span, and an `Arc<AtomicUsize>` for one runtime's active-drive gauge. `FlowRuntime` creates guards only after its existing claim/create boundaries succeed, then explicitly completes them after existing handler and persistence results are known.

**Tech Stack:** Rust 2024, Tokio, `metrics`, `tracing`, atomics, existing memory flow test harness.

---

### Task 1: Define private Flow metrics guards

**Files:**
- Modify: `crates/catga-flow/src/metrics.rs`
- Test: `tests/observability.rs`

- [x] **Step 1: Write failing lifecycle metric test**

Add `flow_runtime_records_terminal_and_step_metrics` that starts a two-step
successful `FlowDefinition` and a one-step failing definition, then asserts
the following counters through the existing local `MetricRecorder`:

```rust
assert_eq!(recorder.counter("catga.flow.started|"), 2);
assert_eq!(recorder.counter("catga.flow.completed|"), 1);
assert_eq!(recorder.counter("catga.flow.failed|"), 1);
assert_eq!(recorder.counter("catga.flow.step.executed|"), 3);
assert_eq!(recorder.counter("catga.flow.step.succeeded|"), 2);
assert_eq!(recorder.counter("catga.flow.step.failed|"), 1);
```

Extend the local `MetricRecorder` with a `gauges: Arc<Mutex<HashMap<String,
f64>>>`, a `gauge` accessor, and a `RecordedGauge` implementation so the
later cancellation test can read `catga.flow.active` without installing a
global recorder.

- [x] **Step 2: Verify the test fails**

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test observability flow_runtime_records_terminal --quiet`

Expected: FAIL because `FlowRuntime` emits no lifecycle counters.

- [x] **Step 3: Implement static metric constants and guards**

Add `FlowMetrics`, `FlowExecution`, and `FlowStepOperation` to
`crates/catga-flow/src/metrics.rs`. `FlowExecution::new` increments an
`AtomicUsize` and sets `catga.flow.active`; `complete` records one static
outcome and duration; `Drop` records `aborted` and decrements exactly once.
`FlowStepOperation` follows the same one-record rule for a step duration and
outcome. Expose only `pub(crate)` methods required by `runtime.rs`.

- [x] **Step 4: Verify the lifecycle test passes**

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test observability flow_runtime_records_terminal --quiet`

Expected: PASS.

### Task 2: Instrument durable FlowRuntime boundaries

**Files:**
- Modify: `crates/catga-flow/src/runtime.rs`
- Modify: `crates/catga-flow/src/hot_reload/mod.rs` only if its shared runtime constructor needs the metric handle
- Test: `tests/observability.rs`

- [x] **Step 1: Add cancellation regression test**

Add `flow_runtime_cancellation_releases_active_execution_metric`. Its first
step waits on a `tokio::sync::Notify`; poll `start` until the handler begins,
drop that future, and assert:

```rust
assert_eq!(recorder.gauge("catga.flow.active|"), 0.0);
assert_eq!(recorder.histogram_samples("catga.flow.duration|outcome=aborted"), 1);
```

- [x] **Step 2: Verify the cancellation regression**

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test observability flow_runtime_cancellation --quiet`

Expected: PASS. The guard implementation from Task 1 is intentionally shared
with this coverage, so this regression was added after the lifecycle guard's
initial red-green cycle.

- [x] **Step 3: Instrument only existing ownership transitions**

Store `FlowMetrics` in `FlowRuntime`. Increment `started` only after
`store.create` returns `true`. Create `FlowExecution` at the beginning of
`drive`; create `FlowStepOperation` immediately before
`definition.execute`. Complete the step guard with handler success/failure.
After `persist` succeeds for `Done` or `Failed`, increment the corresponding
terminal counter and complete the execution guard with `success` or `failure`.
Complete the execution guard with `suspended` before every successfully
persisted wait or scheduled suspension return. Do not instrument failed CAS
or a flow merely observed as terminal.

- [x] **Step 4: Verify both Flow tests pass**

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test observability flow_runtime --quiet`

Expected: PASS.

### Task 3: Document and verify the public contract

**Files:**
- Modify: `docs/source-compatibility-matrix.md`
- Modify: `docs/superpowers/specs/2026-07-25-observability-contract-design.md`
- Test: `tests/observability.rs`

- [x] **Step 1: Update documentation**

Replace the Flow lifecycle observability follow-up statement with the exact
bounded metric contract and explain that `catga.flow.active` counts active
in-process drives rather than persisted waiting continuations.

- [ ] **Step 2: Run focused and static checks**

Run:

```bash
rtk cargo fmt --check
rtk cargo test --manifest-path tests/Cargo.toml --test observability --quiet
rtk cargo test --manifest-path tests/Cargo.toml --test flow_suspension --quiet
rtk cargo clippy -p catga-flow --all-targets -- -D warnings
rtk env RUSTDOCFLAGS='-D warnings' cargo doc -p catga-flow --no-deps
rtk git diff --check
```

Expected: every command succeeds.

- [ ] **Step 3: Run the production panic audit**

Run: `rtk rg -n '\\.(unwrap|expect)[[:space:]]*\\(|(unreachable|todo|unimplemented)![[:space:]]*\\(' crates --glob '*.rs' --glob '!catga-testing/**'`

Expected: no matches and status `1`.

- [ ] **Step 4: Commit**

Do not commit in the shared dirty worktree. Preserve the focused diff for an
isolated commit once ownership is clear.
