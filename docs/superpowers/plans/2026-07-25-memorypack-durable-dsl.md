# MemoryPack Compatibility and Durable DSL Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add source-compatible, bounded MemoryPack persistence records and restart-safe nested DSL checkpoints.

**Architecture:** Keep the new MemoryPack codec additive and explicit, with C# 1.21.3 bytes as the oracle. Reuse the existing versioned DSL progress CAS record for one bounded execution checkpoint per top-level step; encode nested cursor state inside its opaque payload.

**Tech Stack:** Rust, Tokio, Serde/Postcard for Rust-native checkpoint framing, Redis Lua CAS, NATS JetStream KV CAS, immutable MemoryPack 1.21.3 fixture data.

---

### Task 1: Validate immutable MemoryPack compatibility fixtures

**Files:**
- Create: `tests/fixtures/memorypack/v1/*.bin`
- Create: `tests/fixtures/memorypack/v1/manifest.json`

- [ ] Keep the supplied v1 payloads and SHA-256 manifest as immutable test input; do not add, invoke, or modify C# code.
- [ ] Add a Rust fixture test that validates the manifest hashes and fails until a decoder exists.

### Task 2: Implement the bounded MemoryPack wire reader and writer

**Files:**
- Create: `crates/catga-codec-memorypack/Cargo.toml`
- Create: `crates/catga-codec-memorypack/src/{lib.rs,reader.rs,writer.rs,error.rs,limits.rs}`
- Modify: `Cargo.toml`
- Test: `crates/catga-codec-memorypack/src/lib.rs`

- [ ] Write failing unit tests for null headers, fixed object headers, LE primitives, UTF-8 strings, byte/int arrays, `DateTime.ToBinary` i64 values, malformed lengths, budget exhaustion and trailing bytes.
- [ ] Run `rtk cargo test -p catga-codec-memorypack`; confirm the missing crate/API failure.
- [ ] Implement only checked arithmetic, pre-allocation limits, strict 0/1 bool decoding and exact-frame consumption required by the tests.
- [ ] Re-run `rtk cargo test -p catga-codec-memorypack` and `rtk cargo clippy -p catga-codec-memorypack --all-targets -- -D warnings`.

### Task 3: Implement Catga-owned MemoryPack records and golden compatibility tests

**Files:**
- Create: `crates/catga-codec-memorypack/src/{records.rs,fixtures.rs}`
- Test: `tests/memorypack.rs`

- [ ] Write failing golden-byte and semantic decode tests for `FlowState`, outbox, inbox, dead-letter, stored-snapshot metadata, NATS stored snapshot and ForEach progress fixtures.
- [ ] Implement explicit record codecs with one Rust type per stable C# formatter layout; reject headers whose member count differs from the source formatter contract.
- [ ] Run `rtk cargo test -p catga-tests --test memorypack` and inspect each C# fixture's semantic fields.

### Task 4: Introduce durable nested DSL checkpoint data

**Files:**
- Modify: `crates/catga-flow/src/{dsl.rs,dsl_progress.rs,lib.rs}`
- Test: `tests/flow/dsl_progress.rs`

- [ ] Write failing tests that interrupt after an `if` branch action and after a selected `match` branch action, then resume without re-running completed child actions.
- [ ] Add an internal versioned checkpoint frame containing a bounded nested path and encoded state; preserve the existing `DslStepProgressStore` CAS API.
- [ ] Make `run_checkpointed` recursively execute and checkpoint replayable branches, decoding the latest cursor before dispatching a child action.
- [ ] Run `rtk cargo test -p catga-tests --test flow_dsl_progress`.

### Task 5: Add bounded ForEach and parallel recovery

**Files:**
- Modify: `crates/catga-flow/src/{dsl.rs,dsl_progress.rs}`
- Test: `tests/flow/dsl_progress.rs`

- [ ] Write failing tests that recover after each sequential ForEach item and after each parallel branch, asserting completed work is not re-run and merge order remains declaration order.
- [ ] Persist ForEach next-index plus compact completed/failed ranges, and persist a fixed branch vector of status, path and encoded branch state. Reject progress exceeding explicit path, branch, item-range or payload budgets.
- [ ] Re-admit only unfinished parallel branches through the existing configured permit limit; atomically update the one top-level progress record after each completed unit.
- [ ] Reject checkpointed streaming selectors with a validation error because they have no stable replay cursor.
- [ ] Run the flow progress target and the memory/Redis/NATS progress-store targets.

### Task 6: Integrate and verify

**Files:**
- Modify: `docs/source-compatibility-matrix.md`
- Test: `tests/{memorypack,flow/dsl_progress,redis,nats}.rs`

- [ ] Mark Flow as partial until every nested recovery test is green; name RabbitMQ/AMQP and HTTP health routes as intentionally excluded.
- [ ] Run `rtk cargo test --workspace`, `rtk cargo fmt --check`, `rtk cargo clippy --workspace --all-targets -- -D warnings`, `rtk env RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps`, and `rtk git diff --check`.
- [ ] Run source scans for `RabbitMQ|AMQP|lapin|amqprs` and production `unwrap|expect|panic|todo|unimplemented`; finally run `rtk cargo clean`.
