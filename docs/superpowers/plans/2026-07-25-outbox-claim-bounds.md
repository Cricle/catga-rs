# Outbox Claim Bounds Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans`
> to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for
> tracking.

**Goal:** Bound outbox claim memory and Redis due-record scans with one
consistent Rust contract.

**Architecture:** `catga-core` publishes a maximum claim count and a shared
validator. Every store calls it before allocation or I/O, while
`OutboxProcessor` validates its batch at construction. Redis supplies the Lua
script a bounded candidate scan count in addition to the requested result
count.

**Tech Stack:** Rust 2024, DashMap, Tokio, Redis Lua, NATS JetStream KV.

---

### Task 1: Establish the bounded public behavior

**Files:**
- Modify: `tests/memory_outbox.rs`
- Modify: `tests/outbox_processor.rs`

- [ ] **Step 1: Add failing oversized-claim coverage**

  Import `MAX_OUTBOX_CLAIM_LIMIT`. Assert a memory claim at the maximum is an
  empty success and a request for `MAX_OUTBOX_CLAIM_LIMIT + 1` returns
  `ErrorCode::Validation` without claiming an enqueued message.

- [ ] **Step 2: Add failing oversized-processor coverage**

  Construct an outbox processor with `MAX_OUTBOX_CLAIM_LIMIT + 1` and assert a
  validation error. Keep the existing positive batch-size validation test.

- [ ] **Step 3: Run the focused targets and observe missing constant/bounds**

  Run: `rtk cargo test --manifest-path tests/Cargo.toml --test memory_outbox --quiet`
  and `rtk cargo test --manifest-path tests/Cargo.toml --test outbox_processor --quiet`.
  Expected: compile failure for the missing maximum constant or an assertion
  failure because oversized values are currently accepted.

### Task 2: Add the core budget and memory validation

**Files:**
- Modify: `crates/catga-core/src/store.rs`
- Modify: `crates/catga-core/src/lib.rs`
- Modify: `crates/catga-core/src/outbox_processor.rs`
- Modify: `crates/catga-memory/src/outbox.rs`
- Modify: `tests/reliability_contracts.rs`

- [ ] **Step 1: Publish a documented maximum and shared validator**

  Add `MAX_OUTBOX_CLAIM_LIMIT: usize = 1024` and a documented core helper
  that accepts zero but maps larger values to `ErrorCode::Validation`.
  Re-export the constant from `catga-core`.

- [ ] **Step 2: Validate before work starts**

  Validate `batch_size` in both processor constructors and `limit` before the
  memory store creates its claimed vector. Apply the same check in the
  recording contract store so tests preserve the trait invariant.

- [ ] **Step 3: Run the focused targets**

  Run: `rtk cargo test --manifest-path tests/Cargo.toml --test memory_outbox --quiet`
  and `rtk cargo test --manifest-path tests/Cargo.toml --test outbox_processor --quiet`.
  Expected: PASS.

### Task 3: Bound durable store work

**Files:**
- Modify: `crates/catga-redis/src/outbox.rs`
- Modify: `crates/catga-nats/src/outbox.rs`
- Modify: `tests/redis.rs`
- Modify: `tests/nats.rs`

- [ ] **Step 1: Validate NATS before heap allocation**

  Call the core validator before `BinaryHeap::with_capacity(limit)` and before
  beginning the JetStream key stream.

- [ ] **Step 2: Add a bounded Redis candidate scan**

  Validate before Lua invocation. Pass a checked `limit * 4` candidate budget
  to the claim script and use Redis `ZRANGEBYSCORE ... LIMIT` to enumerate no
  more candidates than that budget while retaining at most `limit` claims.

- [ ] **Step 3: Add endpoint-gated boundary tests**

  For each durable backend, call `claim` with the public maximum plus one and
  assert `ErrorCode::Validation`; the existing no-service paths return early.

### Task 4: Document and verify

**Files:**
- Modify: `docs/source-compatibility-matrix.md`
- Modify: `docs/superpowers/specs/2026-07-25-outbox-claim-bounds-design.md`

- [ ] **Step 1: Record explicit budget and Redis contention tradeoff**

  State the 1,024-message hard maximum, non-silent rejection policy, and
  bounded Redis candidate scan behavior.

- [ ] **Step 2: Run quality gates**

  ```bash
  rtk cargo fmt --check
  rtk cargo test --manifest-path tests/Cargo.toml --test memory_outbox --quiet
  rtk cargo test --manifest-path tests/Cargo.toml --test outbox_processor --quiet
  rtk cargo test --manifest-path tests/Cargo.toml --test reliability_contracts --quiet
  rtk cargo test --manifest-path tests/Cargo.toml --test redis --quiet
  rtk cargo test --manifest-path tests/Cargo.toml --test nats --quiet
  rtk cargo clippy -p catga-core -p catga-memory -p catga-redis -p catga-nats --all-targets -- -D warnings
  rtk env RUSTDOCFLAGS='-D warnings' cargo doc -p catga-core -p catga-memory -p catga-redis -p catga-nats --no-deps
  rtk proxy rg -n '\\.(unwrap|expect)[[:space:]]*\\(|(unreachable|todo|unimplemented)![[:space:]]*\\(' crates/catga-core/src crates/catga-memory/src crates/catga-redis/src crates/catga-nats/src
  rtk proxy rg -n -i '[r]abbitmq|rabbit[ ]mq|am[q]p' --glob '!target/**' --glob '!Cargo.lock' .
  rtk git diff --check
  ```

  Expected: every deterministic check succeeds; Redis and NATS tests pass
  against services when configured, otherwise execute their explicit skip path.
