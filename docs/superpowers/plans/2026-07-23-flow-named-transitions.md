# Durable Flow Named Transitions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a registered durable-flow handler choose its next named step, so conditional paths survive suspension and process restart.

**Architecture:** Add a `Goto(Box<str>)` flow-step outcome and persist its chosen step name exactly as the existing runtime persists linear advances. The runtime validates that the named target is registered before writing the transition, then claims and drives it through the same CAS-based ownership path as a linear advance.

**Tech Stack:** Rust 2024, `tokio`, immutable `FlowState`/`FlowContinuation`, `ArcSwap` and `DashMap` in the memory store.

---

### Task 1: Define and test a durable named transition

**Files:**

- Modify: `tests/flow_suspension.rs`
- Modify: `crates/catga-flow/src/definition.rs`
- Modify: `crates/catga-flow/src/runtime.rs`

- [x] **Step 1: Write the failing root integration test**

```rust
#[tokio::test]
async fn named_transition_persists_the_selected_branch_and_executes_only_that_handler() {
    let selected = Arc::new(AtomicUsize::new(0));
    let rejected = Arc::new(AtomicUsize::new(0));
    let definition = FlowDefinition::new("payment")
        .step("choose", |_| async { Ok(FlowStepOutcome::goto("accepted")) })
        .step("rejected", move |_| {
            let rejected = Arc::clone(&rejected);
            async move { rejected.fetch_add(1, Ordering::SeqCst); Ok(FlowStepOutcome::complete()) }
        })
        .step("accepted", move |_| {
            let selected = Arc::clone(&selected);
            async move { selected.fetch_add(1, Ordering::SeqCst); Ok(FlowStepOutcome::complete()) }
        });
    // start the runtime and assert success, selected == 1, rejected == 0
}
```

- [x] **Step 2: Run the test to verify it fails**

Run: `rtk proxy cargo test -p catga-tests --test flow_suspension named_transition_persists_the_selected_branch_and_executes_only_that_handler`

Expected: compilation failure because `FlowStepOutcome::goto` does not exist.

- [x] **Step 3: Add the minimal outcome and registered-target lookup**

```rust
pub enum FlowStepOutcome {
    Advance,
    Goto(Box<str>),
    // existing variants
}

impl FlowStepOutcome {
    pub fn goto(step_name: impl Into<Box<str>>) -> Self {
        Self::Goto(step_name.into())
    }
}

pub(crate) fn has_step(&self, name: &str) -> bool {
    self.steps.iter().any(|step| step.name.as_ref() == name)
}
```

- [x] **Step 4: Persist and claim the selected transition**

In `FlowRuntime::drive`, handle `Goto(next_step)` by rejecting an unknown target with `ErrorCode::NotFound`; otherwise create the next immutable suspended continuation with `at_step(next_step)`, increment its business version, persist it, claim it, and continue the drive loop. Reuse the same code path and ownership behavior as `Advance`.

- [x] **Step 5: Run focused tests and format**

Run: `rtk proxy cargo fmt --all && rtk proxy cargo test -p catga-tests --test flow_suspension && rtk proxy cargo test -p catga-tests --test flow_recovery`

Expected: all focused tests pass.

- [x] **Step 6: Commit**

```bash
git add crates/catga-flow/src/definition.rs crates/catga-flow/src/runtime.rs tests/flow_suspension.rs docs/superpowers/plans/2026-07-23-flow-named-transitions.md
git commit -m "feat: add durable named flow transitions"
```

### Task 2: Verify the workspace boundary

**Files:**

- Verify: workspace

- [x] **Step 1: Run lint and full tests using Cargo's default parallelism**

Run: `rtk proxy env CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --workspace --all-targets -- -D warnings && rtk proxy env CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --workspace`

Expected: exit status 0 with no warnings and no test failures.
