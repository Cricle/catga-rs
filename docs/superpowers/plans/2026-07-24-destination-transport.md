# Destination Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add explicit, durable destination send and receive contracts compatible with upstream `IMessageTransport`.

**Architecture:** A core `DestinationTransport` trait keeps named durable queues distinct from topic publication.  Memory provides deterministic bounded named queues; Redis maps destinations to Streams; NATS requires explicitly registered JetStream resources.

**Tech Stack:** Rust 2024, Tokio, futures, DashMap, Redis Streams, async-nats JetStream.

---

### Task 1: Define the core destination contract

**Files:**
- Modify: `crates/catga-core/src/transport.rs`, `crates/catga-core/src/lib.rs`
- Test: `tests/transport/destination.rs`

- [x] Write a failing test for validated destination parsing and a zero-concurrency batch failure.
- [x] Run `cargo test -p catga-tests --test transport_destination` and confirm the missing API failure.
- [x] Add documented `Destination` and `DestinationTransport` with streaming bounded `send_batch_to`.
- [x] Re-run the focused test and confirm it passes.

### Task 2: Implement deterministic Memory destinations

**Files:**
- Modify: `crates/catga-memory/src/transport.rs`
- Test: `tests/transport/destination.rs`

- [x] Write failing tests for explicit declaration, unknown-destination rejection, FIFO delivery, and stopped-send rejection.
- [x] Run the focused test and confirm it fails for the missing implementation.
- [x] Add bounded named queues and acknowledgement tracking with a receiver lock scoped to each destination.
- [x] Re-run the focused test and confirm it passes.

### Task 3: Add durable adapter destinations

**Files:**
- Modify: `crates/catga-redis/src/transport.rs`, `crates/catga-nats/src/transport.rs`
- Test: `tests/redis.rs`, `tests/nats.rs`

- [x] Write service-backed failing round-trip tests guarded by `CATGA_REDIS_URL` and `CATGA_NATS_URL`.
- [x] Run each focused test and confirm it compiles then skips without its broker.
- [x] Implement Redis Stream/group and explicitly provisioned NATS JetStream destination paths.
- [x] Run service-backed tests against temporary local Redis and JetStream services.

### Task 4: Audit and verify

**Files:**
- Modify: public Rustdoc in affected files as needed

- [x] Run `cargo fmt --check`, workspace tests, Clippy with `-D warnings`, and `cargo doc` with `RUSTDOCFLAGS=-D warnings`.
- [x] Search production crates for panic-prone `unwrap`, `expect`, `todo`, `unimplemented`, and `unreachable` macros.
- [x] Search the Rust port for prohibited broker references and require no matches.
