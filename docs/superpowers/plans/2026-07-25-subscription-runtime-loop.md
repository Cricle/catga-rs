# Subscription Runtime Loop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Provide a caller-owned, cancellable continuous subscription loop.

**Architecture:** `SubscriptionLoopOptions` validates the poll interval. `SubscriptionRunner` reuses `run_once`, aggregates its bounded results, and waits through `tokio::select!` so cancellation interrupts only the inter-pass delay.

**Tech Stack:** Rust, Tokio cancellation tokens, Catga event subscriptions, integration tests.

---

### Task 1: Specify Runtime Behavior

**Files:**
- Modify: `tests/subscriptions.rs`

- [x] **Step 1: Add a failing immediate-run and cancellation test**

The test configures a 60-second interval, verifies the first event is handled
within one second, cancels the caller-owned task, and asserts the returned
aggregate contains one handled event.

- [x] **Step 2: Add a zero-interval validation assertion**

```rust
assert_eq!(
    SubscriptionLoopOptions::new(Duration::ZERO).expect_err("zero interval is invalid").code(),
    ErrorCode::Validation,
);
```

- [x] **Step 3: Run the focused test and verify the API is absent**

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test subscriptions subscription_runtime_runs_immediately_and_stops_on_cancellation --quiet`

Expected: FAIL to compile because the runtime loop API does not exist.

### Task 2: Implement The Caller-Owned Loop

**Files:**
- Modify: `crates/catga-core/src/subscription.rs`
- Modify: `crates/catga-core/src/lib.rs`
- Test: `tests/subscriptions.rs`

- [x] **Step 1: Add validated `SubscriptionLoopOptions` with a 100ms default**

- [x] **Step 2: Add `SubscriptionRunner::run_until_cancelled` with immediate first pass and cancellation-aware delay**

- [x] **Step 3: Export the public options type and add complete Rustdoc**

- [x] **Step 4: Run the focused test**

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test subscriptions subscription_runtime_runs_immediately_and_stops_on_cancellation --quiet`

Expected: PASS.

### Task 3: Verify The Runtime Contract

**Files:**
- Modify: `docs/source-compatibility-matrix.md`
- Modify: `docs/superpowers/plans/2026-07-25-subscription-runtime-loop.md`

- [x] **Step 1: Run the complete subscription target**

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test subscriptions --quiet`

Expected: PASS.

- [x] **Step 2: Run formatting, core Clippy, and Rustdoc gates**

Run: `rtk cargo fmt --check`

Expected: PASS.

Run: `rtk cargo clippy -p catga-core --all-targets -- -D warnings`

Expected: PASS.

Run: `rtk proxy env RUSTDOCFLAGS='-D missing_docs -D warnings' cargo doc -p catga-core --no-deps`

Expected: PASS.

- [x] **Step 3: Mark plan tasks complete after verified commands pass**
