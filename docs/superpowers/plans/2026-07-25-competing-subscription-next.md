# Competing Subscription Next-Event Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add bounded, fair single-event processing to a competing event subscription.

**Architecture:** A private `SubscriptionRunner` helper consumes bounded pages until it handles one selected event or exhausts a stream. `CompetingSubscriptionRunner` wraps this helper with the existing durable lease and preserves the established release-on-success-or-error contract.

**Tech Stack:** Rust, Tokio, Catga event store and subscription traits, Memory subscription integration tests.

---

### Task 1: Specify Single-Event Competing Processing

**Files:**
- Modify: `tests/subscriptions.rs`

- [x] **Step 1: Add a failing fairness and checkpoint regression**

```rust
assert_eq!(first.try_process_next().await?, Some(true));
assert_eq!(second.try_process_next().await?, Some(true));
assert_eq!(first.try_process_next().await?, Some(false));
```

The test must place a filtered event before two matching events and assert that
each call handles only one matching event, advances the checkpoint, and frees
the lease for the next caller.

- [x] **Step 2: Run the focused test and verify the method is absent**

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test subscriptions competing_subscription_processes_at_most_one_matching_event_per_lease --quiet`

Expected: FAIL to compile because `try_process_next` does not yet exist.

### Task 2: Add The Bounded Next-Event Path

**Files:**
- Modify: `crates/catga-core/src/subscription.rs`
- Test: `tests/subscriptions.rs`

- [x] **Step 1: Add a private paged helper that advances filtered events and stops after one handled event**

The helper must save each inspected event's checkpoint and return immediately
after one successful handler invocation.

- [x] **Step 2: Add `CompetingSubscriptionRunner::try_process_next`**

```rust
pub async fn try_process_next(&self) -> CatgaResult<Option<bool>>
```

Acquire the existing store lease, call the next-event helper, release the
lease, and preserve the primary structured failure.

- [x] **Step 3: Document the return states and lease behavior in Rustdoc**

- [x] **Step 4: Run the focused test**

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test subscriptions competing_subscription_processes_at_most_one_matching_event_per_lease --quiet`

Expected: PASS.

### Task 3: Verify The Subscription Contract

**Files:**
- Modify: `docs/source-compatibility-matrix.md`
- Modify: `docs/superpowers/plans/2026-07-25-competing-subscription-next.md`

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

- [x] **Step 3: Mark plan tasks complete after the recorded commands pass**
