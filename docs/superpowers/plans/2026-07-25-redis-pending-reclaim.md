# Redis Pending Reclaim Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recover idle Redis Streams group deliveries from a stopped consumer through bounded `XAUTOCLAIM` scans.

**Architecture:** Keep the existing typed, acknowledgement-owning delivery API.  Add validated reclaim options and a per-stream cursor inside `InFlight`; `receive_stream` recovers one local pending message first, then makes at most a configured number of `XAUTOCLAIM COUNT 1` calls before waiting for a new message.

**Tech Stack:** Rust 2024, `redis` 1 Streams API, Tokio, DashMap, Catga integration tests.

---

### Task 1: Define validated reclaim options

**Files:**
- Modify: `crates/catga-redis/src/config.rs`
- Modify: `crates/catga-redis/src/lib.rs`
- Test: `tests/redis.rs`

- [ ] **Step 1: Write the failing validation test**

```rust
let error = RedisPendingReclaimOptions::new(Duration::ZERO, 1)
    .expect_err("zero reclaim idle duration must be rejected");
assert_eq!(error.code(), ErrorCode::Validation);
```

- [ ] **Step 2: Verify the test fails because the type is missing**

Run: `rtk cargo test -p catga-tests --test redis redis_pending_reclaim_options_reject_zero_idle --no-run`

Expected: compilation fails because `RedisPendingReclaimOptions` is undefined.

- [ ] **Step 3: Implement the options**

```rust
pub struct RedisPendingReclaimOptions { /* validated duration and scan limit */ }

impl RedisPendingReclaimOptions {
    pub fn new(minimum_idle: Duration, max_scans: usize) -> CatgaResult<Self> { /* validate */ }
}
```

- [ ] **Step 4: Verify the validation test passes**

Run: `rtk cargo test -p catga-tests --test redis redis_pending_reclaim_options_reject_zero_idle`

Expected: one passing test.

### Task 2: Reclaim one idle delivery across consumers

**Files:**
- Modify: `crates/catga-redis/src/transport.rs`
- Test: `tests/redis.rs`

- [ ] **Step 1: Write the ignored two-consumer integration regression**

```rust
let abandoned = first.receive().await?;
drop(abandoned);
let reclaimed = timeout(Duration::from_secs(1), second.receive()).await??;
assert_eq!(reclaimed.envelope().id(), envelope.id());
assert!(reclaimed.attempts() >= 2);
second.ack(reclaimed).await?;
```

- [ ] **Step 2: Verify the test fails to compile before the new constructor exists**

Run: `rtk cargo test -p catga-tests --test redis redis_transport_reclaims_an_idle_delivery_from_another_consumer --no-run`

Expected: compilation fails because `connect_with_reclaim_options` is undefined.

- [ ] **Step 3: Implement bounded reclaim**

```rust
let reply = connection.xautoclaim_options(
    stream, group, consumer, minimum_idle_ms, cursor,
    StreamAutoClaimOptions::default().count(1),
).await?;
```

Store `reply.next_stream_id`, return only the first claimed entry, and stop
after `max_scans`.  Do not retain a Redis reply collection or DashMap guard
across an await.

- [ ] **Step 4: Compile and run the Redis service-gated regression when configured**

Run: `rtk cargo test -p catga-tests --test redis redis_transport_reclaims_an_idle_delivery_from_another_consumer -- --ignored`

Expected: one passing test when `CATGA_REDIS_URL` names Redis 6.2 or newer.

### Task 3: Document and verify the crate

**Files:**
- Modify: `docs/source-compatibility-matrix.md`
- Test: `tests/redis.rs`

- [ ] **Step 1: Add the Redis recovery guarantee to the compatibility matrix**

State that recovery uses bounded `XAUTOCLAIM` rather than an unbounded pending-list scan.

- [ ] **Step 2: Run focused quality gates**

Run: `rtk cargo test -p catga-tests --test redis --no-run && rtk cargo clippy -p catga-redis --all-targets -- -D warnings && rtk env RUSTDOCFLAGS='-D warnings' cargo doc -p catga-redis --no-deps && rtk cargo fmt --all -- --check && rtk git diff --check`

Expected: all commands succeed.

- [ ] **Step 3: Commit the completed batch**

```bash
rtk git add crates/catga-redis/src/config.rs crates/catga-redis/src/lib.rs \
  crates/catga-redis/src/transport.rs tests/redis.rs \
  docs/source-compatibility-matrix.md
rtk git commit -m "fix: reclaim idle Redis stream deliveries"
```
