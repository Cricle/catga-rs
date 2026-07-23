# Durable Flow Cancellation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow callers to durably cancel a suspended or running registered flow and make later resumes idempotently report that terminal result.

**Architecture:** Represent cancellation as a terminal immutable `FlowStatus` and advance the continuation business version through the existing store update. A running handler may finish after cancellation, but its stale version cannot write a later state; application side effects remain at-least-once.

**Tech Stack:** Rust 2024, immutable continuation state, optimistic version updates, root `tokio` integration tests.

---

### Task 1: Add a persisted terminal cancellation state

**Files:**

- Modify: `tests/flow_recovery.rs`
- Modify: `crates/catga-flow/src/state.rs`
- Modify: `crates/catga-flow/src/runtime.rs`

- [x] **Step 1: Write the failing root integration test**

```rust
#[tokio::test]
async fn cancellation_is_terminal_and_resume_is_idempotent() {
    // Create a suspended continuation, cancel it through FlowRuntime,
    // then assert cancel and resume both return a cancelled result.
}
```

- [x] **Step 2: Run the test to verify it fails**

Run: `rtk proxy cargo test -p catga-tests --test flow_recovery cancellation_is_terminal_and_resume_is_idempotent`

Expected: compilation failure because `FlowRuntime::cancel` and `FlowRuntimeResult::is_cancelled` do not exist.

- [x] **Step 3: Add terminal status and immutable state transition**

```rust
pub enum FlowStatus { Running, Compensating, Suspended, Done, Failed, Cancelled }

pub fn cancelled(self) -> Self {
    Self { status: FlowStatus::Cancelled, owner: None, error: None, ..self }
}
```

Ensure `Cancelled` is terminal through `FlowStatus::is_terminal`.

- [x] **Step 4: Add idempotent runtime cancellation**

Load the continuation, reject a different definition, return an existing terminal state unchanged, or write `ready().cancelled().next_version()` through `SuspendedFlowStore::update`. If a concurrent state transition wins, return the loaded current state instead of an error.

- [x] **Step 5: Format and verify focused tests**

Run: `rtk proxy cargo fmt --all && rtk proxy cargo test -p catga-tests --test flow_recovery && rtk proxy cargo test -p catga-tests --test flow_suspension`

Expected: all focused tests pass.

- [x] **Step 6: Commit**

```bash
git add crates/catga-flow/src/state.rs crates/catga-flow/src/runtime.rs tests/flow_recovery.rs docs/superpowers/plans/2026-07-23-flow-cancellation.md
git commit -m "feat: add durable flow cancellation"
```

### Task 2: Verify the workspace boundary

**Files:**

- Verify: workspace

- [x] **Step 1: Run lint and full tests using Cargo's default parallelism**

Run: `rtk proxy env CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --workspace --all-targets -- -D warnings && rtk proxy env CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --workspace`

Expected: exit status 0 with no warnings and no test failures.
