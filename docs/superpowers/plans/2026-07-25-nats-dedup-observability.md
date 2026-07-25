# NATS Deduplication Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Record broker-confirmed ExactlyOnce duplicate suppression without an in-process deduplication cache.

**Architecture:** A private NATS transport helper receives the JetStream publish acknowledgement's boolean duplicate result. It increments one static counter only when the broker reports a duplicate; publish results remain successful and payload ownership remains unchanged.

**Tech Stack:** Rust 2024, `async-nats`, `metrics`, Tokio, existing NATS transport tests.

---

### Task 1: Test broker-duplicate accounting

**Files:**
- Modify: `crates/catga-nats/Cargo.toml`
- Modify: `crates/catga-nats/src/transport.rs`

- [x] **Step 1: Write a failing unit test**

In `transport.rs`'s existing test module, install a local recorder, call the
private acknowledgement helper with `false` then `true`, and assert:

```rust
assert_eq!(recorder.counter("catga.nats.dedup.drops"), 1);
```

- [x] **Step 2: Verify the test fails**

Run: `rtk cargo test -p catga-nats transport::tests::broker_duplicate --quiet`

Expected: FAIL because no helper or counter exists.

- [x] **Step 3: Implement the minimum broker-acknowledgement helper**

Add the direct `metrics` dependency. Define the documented static counter and
a helper that increments it only for a true `PublishAck::duplicate`. Apply the
helper after each successful ExactlyOnce JetStream acknowledgement in both
`publish` and `publish_durable`; do not allocate a local identity cache or
convert duplicate acknowledgement into an error.

- [x] **Step 4: Verify the unit test passes**

Run: `rtk cargo test -p catga-nats transport::tests::broker_duplicate --quiet`

Expected: PASS.

### Task 2: Document and verify the provider contract

**Files:**
- Modify: `docs/source-compatibility-matrix.md`
- Modify: `docs/superpowers/specs/2026-07-25-observability-contract-design.md`

- [x] **Step 1: Document the broker-owned mapping**

State that `catga.nats.dedup.drops` reports broker-confirmed suppressions and
that the omitted eviction metric is intentional because Rust retains no local
deduplication cache.

- [ ] **Step 2: Run checks**

Run:

```bash
rtk cargo fmt --check
rtk cargo test -p catga-nats --quiet
rtk cargo test --manifest-path tests/Cargo.toml --test nats --quiet
rtk cargo clippy -p catga-nats --all-targets -- -D warnings
rtk env RUSTDOCFLAGS='-D warnings' cargo doc -p catga-nats --no-deps
rtk git diff --check
```

Expected: every command succeeds. The NATS integration target may take its
existing deterministic skip path when `CATGA_NATS_URL` is absent.
