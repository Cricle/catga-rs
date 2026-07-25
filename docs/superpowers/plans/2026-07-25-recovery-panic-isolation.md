# Recovery Panic Isolation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent one recoverable component panic from aborting a Rust recovery sweep.

**Architecture:** `RecoveryManager` owns the recovery-loop boundary, so it catches unwinds only around each component's async `recover` invocation. The result is folded into the existing bounded `RecoveryResult` counts, leaving registration, retry, and cancellation semantics unchanged.

**Tech Stack:** Rust, Tokio, futures `FutureExt`, `catch_unwind`, integration tests.

---

### Task 1: Specify The Recovery Boundary

**Files:**
- Modify: `tests/lifecycle.rs`

- [x] **Step 1: Replace the panic-propagation test with a failure-isolation test**

```rust
let outcome = manager.recover_unhealthy().await;
assert!(matches!(
    outcome,
    RecoveryResult::Completed {
        succeeded: 0,
        failed: 1,
        ..
    }
));
assert!(!manager.is_recovering());
```

- [x] **Step 2: Run the focused test and observe the component panic escape**

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test lifecycle recovery_manager_isolates_component_panics --quiet`

Expected: FAIL because `recover_unhealthy` currently allows `PanickingRecoverable::recover` to unwind.

### Task 2: Isolate Recoverable Component Panics

**Files:**
- Modify: `crates/catga-core/src/lifecycle.rs`
- Test: `tests/lifecycle.rs`

- [x] **Step 1: Catch the component future's unwind at the recovery boundary**

```rust
match AssertUnwindSafe(component.recover()).catch_unwind().await {
    Ok(Ok(())) => succeeded += 1,
    Ok(Err(_)) | Err(_) => failed += 1,
}
```

- [x] **Step 2: Document that panics are counted as failed recovery attempts**

Add the contract to `RecoveryManager::recover_unhealthy` Rustdoc.

- [x] **Step 3: Run the focused lifecycle test**

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test lifecycle recovery_manager_isolates_component_panics --quiet`

Expected: PASS.

### Task 3: Verify The Lifecycle Contract

**Files:**
- Modify: `docs/superpowers/specs/2026-07-25-recovery-panic-isolation-design.md`
- Modify: `docs/superpowers/plans/2026-07-25-recovery-panic-isolation.md`

- [x] **Step 1: Run the complete lifecycle integration target**

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test lifecycle --quiet`

Expected: PASS.

- [x] **Step 2: Run core lint and Rustdoc gates**

Run: `rtk cargo clippy -p catga-core --all-targets -- -D warnings`

Expected: PASS.

Run: `RUSTDOCFLAGS='-D missing_docs -D warnings' rtk cargo doc -p catga-core --no-deps`

Expected: PASS.

- [x] **Step 3: Mark verified plan tasks complete**

Update this plan's checkboxes after the recorded commands pass.
