# Flow Best-Effort ForEach Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` or `superpowers:executing-plans` task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Add explicit, observable continue-on-item-failure Flow operations without weakening the existing fail-fast DSL contract.

**Architecture:** Each best-effort operation owns its error callback. The callback receives the zero-based item index and original `CatgaError`, so application state can record, compensate, or deliberately reject the failure. Replayable checkpoint execution writes the callback-updated state and the next item cursor together; non-replayable operations remain process-local.

**Tech Stack:** Rust, Tokio, Futures, existing `DslFlow` checkpoints and metrics.

---

### Task 1: Specify the local best-effort contract

**Files:**
- Modify: `tests/flow/local.rs`
- Modify: `crates/catga-flow/src/dsl.rs`

- [ ] **Step 1: Write a failing test**

Add a `for_each_continue_on_error` test with items `[3, 5, 8]`: make item `5` return `Validation`, have the mandatory error callback append `(1, Validation)` to state, and assert that item `8` and the later flow step execute.

- [ ] **Step 2: Verify the test fails**

Run: `rtk cargo test -p catga-tests --test flows dsl_flow_for_each_continue_on_error`

Expected: compilation failure because `DslFlow::for_each_continue_on_error` does not exist.

- [ ] **Step 3: Implement the minimal local API**

Add an explicitly named `DslFlow::for_each_continue_on_error` operation. It must record existing success/failure metrics, call the error callback with the item index and original error, stop only if that callback fails, and avoid retaining items or errors after the callback completes.

- [ ] **Step 4: Verify the focused test passes**

Run: `rtk cargo test -p catga-tests --test flows dsl_flow_for_each_continue_on_error`

Expected: one passing test.

### Task 2: Preserve recovery semantics for replayable items

**Files:**
- Modify: `tests/flow/dsl_progress.rs`
- Modify: `crates/catga-flow/src/dsl.rs`

- [x] **Step 1: Write a failing replay test**

Add a checkpointed replayable best-effort flow where a failed item is recorded in state, later items execute, and a second run starts at the persisted cursor without re-running completed or failed items.

- [x] **Step 2: Verify the test fails**

Run: `rtk cargo test -p catga-tests --test dsl_progress checkpointed_dsl_replayable_for_each_continue_on_error`

Expected: compilation failure because the replayable best-effort builder does not exist.

- [x] **Step 3: Implement durable best-effort execution**

Add `for_each_replayable_continue_on_error`. On every action result, run the callback on failure, advance the bounded cursor, and persist the encoded state and original item frame in the existing one-CAS checkpoint update. A callback error must preserve the cursor so retry repeats only the unresolved item.

- [x] **Step 4: Verify the focused test passes**

Run: `rtk cargo test -p catga-tests --test dsl_progress checkpointed_dsl_replayable_for_each_continue_on_error`

Expected: one passing test.

### Task 3: Quality gates and documentation

**Files:**
- Modify: `crates/catga-flow/src/dsl.rs`
- Modify: `docs/source-compatibility-matrix.md`

- [ ] **Step 1: Document the public APIs and source mapping**

Rustdoc must state the callback, retention, fail-fast-on-callback-error, and checkpointing rules. The compatibility matrix must say that Rust replaces silent C# `ContinueOnFailure` success with an explicit callback.

- [ ] **Step 2: Run validation**

Run: `rtk cargo test -p catga-tests --test flows && rtk cargo test -p catga-tests --test flow_progress && rtk cargo clippy -p catga-flow --all-targets -- -D warnings && rtk env RUSTDOCFLAGS='-D warnings' cargo doc -p catga-flow --no-deps && rtk cargo fmt --all -- --check && rtk git diff --check`

Expected: all commands exit successfully.
