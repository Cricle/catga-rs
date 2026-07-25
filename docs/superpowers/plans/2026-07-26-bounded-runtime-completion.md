# Bounded Runtime Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the remaining unbounded Rust runtime paths and provide Rust-native equivalents for the upstream test and explicit MemoryPack extension surfaces.

**Architecture:** Event stores expose validated, cursor-like fixed-size pages; all core consumers process one page at a time. Raft retains at most a configured number of unapplied application entries and returns structured backpressure rather than growing memory. Test and MemoryPack facilities are typed, explicit, and reflection-free.

**Tech Stack:** Rust 2024, async-trait, Tokio, Redis Streams, async-nats JetStream, catga-testing, bounded MemoryPack reader/writer.

---

### Task 1: Bound event-store reads and all core consumers

**Files:**

- Modify: `crates/catga-core/src/event_store.rs`, `aggregate.rs`, `subscription.rs`, `projection.rs`, `time_travel.rs`, `upgrading_event_store.rs`, `lib.rs`
- Modify: `crates/catga-memory/src/event_store.rs`, `crates/catga-redis/src/event_store.rs`, `crates/catga-nats/src/event_store.rs`
- Test: `tests/{aggregates,event_store,subscriptions,projections,time_travel,redis,nats}.rs`

- [ ] **Step 1: Add failing page-bound tests.**

  Cover a page limit of zero and above `MAX_EVENT_STORE_PAGE_SIZE` returning `ErrorCode::Validation`; append more than two pages, replay an aggregate and a time-travel aggregate without requesting `usize::MAX`; and page stream IDs in stable lexical order.

- [ ] **Step 2: Run the focused tests to prove RED.**

  Run: `rtk cargo test -p catga-tests --test event_store --test aggregates --test time_travel`

  Expected: compile failures for `EventPage`/page methods, or validation assertions failing because unbounded methods still exist.

- [ ] **Step 3: Implement the bounded contract.**

  Add `MAX_EVENT_STORE_PAGE_SIZE`, `EventPage`, and validated `read_page`, `read_to_version_page`, `read_to_time_page`, `version_history_page`, and `stream_ids_page` methods. Each page owns no more than the supplied validated limit and carries a next cursor. Replace callers that materialize whole history with loops that apply one page before fetching the next. Use Redis `XRANGE COUNT` and scan cursors; use bounded JetStream collection; preserve existing version, time and checkpoint semantics.

- [ ] **Step 4: Run backend and core tests.**

  Run: `rtk cargo test -p catga-tests --test event_store --test aggregates --test subscriptions --test projections --test time_travel`

  Expected: PASS; service-gated Redis/NATS tests compile and retain their ignored runtime boundaries.

- [ ] **Step 5: Commit the bounded event-store task.**

  Run: `rtk git add crates/catga-core crates/catga-memory crates/catga-redis crates/catga-nats tests docs/source-compatibility-matrix.md && rtk git commit -m "feat: bound event store pagination"`

### Task 2: Bound unapplied Raft entries

**Files:**

- Modify: `crates/catga-cluster/src/{raft,runtime,state_machine,lib}.rs`
- Test: `tests/{raft_cluster,raft_runtime,raft_state_machine,raft_state_machine_runtime}.rs`

- [ ] **Step 1: Add a failing backpressure test.**

  Construct a node with one pending-entry slot, commit two application entries without draining, and assert the second drive returns a documented structured capacity error while the first payload remains available exactly once.

- [ ] **Step 2: Run RED.**

  Run: `rtk cargo test -p catga-tests --test raft_cluster --test raft_runtime`

  Expected: missing bounded-construction API or no backpressure error.

- [ ] **Step 3: Implement the capacity boundary.**

  Replace the unbounded committed `Vec` with a bounded `VecDeque`, validate a non-zero maximum at construction, preflight every ready batch before taking ownership, and expose one-entry and bounded-drain consumption. Propagate the structured backpressure error through the single-owner runtime; preserve committed-index acknowledgement and snapshot recovery.

- [ ] **Step 4: Run cluster tests.**

  Run: `rtk cargo test -p catga-tests --test raft_cluster --test raft_runtime --test raft_state_machine --test raft_state_machine_runtime`

  Expected: PASS.

- [ ] **Step 5: Commit the Raft task.**

  Run: `rtk git add crates/catga-cluster tests && rtk git commit -m "feat: bound pending raft commits"`

### Task 3: Add typed aggregate and Flow test contexts

**Files:**

- Create: `crates/catga-testing/src/{aggregate,flow}.rs`
- Modify: `crates/catga-testing/src/lib.rs`
- Test: `tests/testing/{aggregate,flow}.rs`

- [ ] **Step 1: Add failing ergonomic tests.**

  Specify an `AggregateScenario` that seeds immutable envelopes, replays an aggregate through the real store, and asserts version/events; specify a `FlowTestContext` exposing the existing bounded memory continuation store and scheduler without a global mediator.

- [ ] **Step 2: Run RED.**

  Run: `rtk cargo test -p catga-tests --test testing_aggregate --test testing_flow`

  Expected: missing public testing types.

- [ ] **Step 3: Implement the small typed helpers.**

  Keep seeded data in caller-owned vectors, use real `MemoryEventStore` and `MemoryFlowScheduler`, return `CatgaResult` for invalid test setup, and document that helpers are test-only and do not provide production runtime registration.

- [ ] **Step 4: Run helper tests.**

  Run: `rtk cargo test -p catga-tests --test testing_aggregate --test testing_flow --test testing_harness --test testing_helpers`

  Expected: PASS.

- [ ] **Step 5: Commit the testing task.**

  Run: `rtk git add crates/catga-testing tests/testing && rtk git commit -m "feat: add typed aggregate and flow test contexts"`

### Task 4: Add explicit application-value MemoryPack codecs

**Files:**

- Create: `crates/catga-codec-memorypack/src/value.rs`
- Modify: `crates/catga-codec-memorypack/src/lib.rs`
- Test: `tests/memorypack.rs`

- [ ] **Step 1: Add a failing schema-codec test.**

  Define a small application `Order` and an explicit codec using `MemoryPackWriter`/`MemoryPackReader`; assert encode/decode round-trip, malformed/trailing input rejection, and the configured allocation budget.

- [ ] **Step 2: Run RED.**

  Run: `rtk cargo test -p catga-tests --test memorypack`

  Expected: missing `MemoryPackValueCodec`, `encode_value`, and `decode_value`.

- [ ] **Step 3: Implement the explicit codec boundary.**

  Add `MemoryPackValueCodec<T>` plus generic encode/decode helpers that construct the bounded reader/writer, require exact frame completion, and never use runtime type lookup. Document that application schemas are intentionally supplied by callers rather than inferred through C# reflection.

- [ ] **Step 4: Run MemoryPack tests.**

  Run: `rtk cargo test -p catga-tests --test memorypack -p catga-codec-memorypack`

  Expected: PASS.

- [ ] **Step 5: Commit the codec task.**

  Run: `rtk git add crates/catga-codec-memorypack tests/memorypack.rs docs/source-compatibility-matrix.md && rtk git commit -m "feat: add explicit memorypack value codecs"`

### Task 5: Workspace quality gates and migration evidence

**Files:**

- Modify: `docs/source-compatibility-matrix.md`

- [ ] **Step 1: Update the matrix.**

  Record paged EventStore reads, capped Raft pending commits, typed test contexts, and explicit schema codecs. Keep RabbitMQ/AMQP and HTTP health routes listed as excluded.

- [ ] **Step 2: Run complete quality gates.**

  Run: `rtk cargo fmt --all -- --check && rtk git diff --check && rtk env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo clippy --workspace --all-targets -- -D warnings && rtk env CARGO_INCREMENTAL=0 CARGO_PROFILE_TEST_DEBUG=0 cargo test --workspace --no-fail-fast && rtk env RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps`

  Expected: exit code 0. Service-backed tests remain explicitly ignored when their endpoint variables are absent.

- [ ] **Step 3: Commit verified documentation.**

  Run: `rtk git add docs/source-compatibility-matrix.md && rtk git commit -m "docs: record bounded Rust migration guarantees"`
