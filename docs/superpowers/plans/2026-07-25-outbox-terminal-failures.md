# Outbox Terminal Failures Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans`
> to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for
> tracking.

**Goal:** Stop unbounded outbox retries while retaining source-compatible,
inspectable terminal failures across the memory, Redis, and NATS stores.

**Architecture:** Core owns the bounded retry model and exposes one
owner-checked `record_failure` transition. Each backend persists that model
atomically and the processor calls the transition after a publish or ack error.

**Tech Stack:** Rust 2024, Tokio, DashMap, Redis Lua, NATS JetStream KV,
Postcard.

---

### Task 1: Lock down the public retry contract

**Files:**
- Modify: `tests/memory_outbox.rs`
- Modify: `tests/outbox_processor.rs`
- Modify: `tests/reliability_contracts.rs`

- [ ] **Step 1: Write failing store-state tests**

  Assert a new message reports the default maximum and no last error. Claim it,
  record failures below and at the maximum, then assert the penultimate result
  is `Pending` with a count and the terminal result is `Failed` with no owner.
  Assert a future claim is empty, stale owners cannot mutate it, an explicit
  maximum is preserved, and an oversized Unicode reason remains valid UTF-8
  within the public byte cap.

- [ ] **Step 2: Write the failing processor exhaustion test**

  Use a transport that always returns a transient error. After exactly the
  default number of `flush_once` calls, assert the message is failed and later
  scans publish nothing. Keep the existing fail-once test to prove a counted
  failure below the limit remains retryable.

- [ ] **Step 3: Run focused tests and confirm they fail for missing API**

  Run: `rtk cargo test --manifest-path tests/Cargo.toml --test memory_outbox --quiet`
  and `rtk cargo test --manifest-path tests/Cargo.toml --test outbox_processor --quiet`.
  Expected: compile failures for the retry accessors and `record_failure`.

### Task 2: Add the core retry model and processor transition

**Files:**
- Modify: `crates/catga-core/src/store.rs`
- Modify: `crates/catga-core/src/lib.rs`
- Modify: `crates/catga-core/src/outbox_processor.rs`
- Modify: `tests/reliability_contracts.rs`

- [ ] **Step 1: Add documented bounded fields and accessors**

  Add `Failed`, default/max-reason constants, `retry_count`, `max_retries`, and
  optional boxed error to `OutboxMessage`. Add a nonzero validated retry-policy
  builder, accessors, and a crate-visible transition that increments
  saturatingly, trims at a character boundary, clears ownership, and selects
  pending versus failed.

- [ ] **Step 2: Extend the store contract**

  Add `record_failure(&self, owner, id, reason)`. Update the recording test
  store with poison-aware locking and owner checks so the changed contract
  compiles without panic-prone synchronization.

- [ ] **Step 3: Record actual delivery failures**

  Replace the processor's unconditional failure `release` with
  `record_failure`, preserving a bounded textual distinction between transport
  publication and durable acknowledgment failure.

- [ ] **Step 4: Run focused core regressions**

  Run: `rtk cargo test --manifest-path tests/Cargo.toml --test memory_outbox --quiet`
  and `rtk cargo test --manifest-path tests/Cargo.toml --test outbox_processor --quiet`.
  Expected: PASS.

### Task 3: Persist atomic transitions in each backend

**Files:**
- Modify: `crates/catga-memory/src/outbox.rs`
- Modify: `crates/catga-redis/src/outbox.rs`
- Modify: `crates/catga-nats/src/outbox.rs`
- Modify: `tests/redis.rs`
- Modify: `tests/nats.rs`

- [ ] **Step 1: Implement the memory transition**

  Mutate the owner-matching DashMap record in place; its state controls future
  claim eligibility.

- [ ] **Step 2: Implement the Redis Lua transition**

  Extend hash creation and claiming to persist retry policy/state/error.
  One Lua script must verify owner, increment the count, set the bounded reason,
  clear owner, and `ZREM` only when it selects `Failed`. Rebuild the claimed
  public message from all persisted fields.

- [ ] **Step 3: Implement versioned NATS records**

  Encode a current format marker plus owner, public retry data, and payload.
  Decode the old owner-prefix record as a default pending message. A
  revision-CAS failure transition must reject stale owners and never make a
  failed record claimable.

- [ ] **Step 4: Add endpoint-gated durable regressions**

  In Redis and NATS tests, enqueue, claim, record three failures, and assert
  the first two reappear with counts while the final record is absent from
  `claim`.

### Task 4: Document and verify

**Files:**
- Modify: `docs/source-compatibility-matrix.md`
- Modify: `docs/superpowers/specs/2026-07-25-outbox-terminal-failures-design.md`

- [ ] **Step 1: Record source mapping and durable bounds**

  Explain the per-message default, terminal retention, no implicit DLQ mapping,
  bounded errors, and backend atomicity.

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

  Expected: focused tests pass; endpoint-gated tests pass or take their
  intentional no-service path; all static quality gates succeed.
