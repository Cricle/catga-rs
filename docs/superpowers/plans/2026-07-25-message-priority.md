# Typed Message Priority Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Carry a typed message's declared priority into every typed outbound envelope.

**Architecture:** Add a default `Message::priority` method and a zero-allocation derive configuration. Apply its `Copy` enum value at each existing typed envelope construction point, leaving headers and adapter mappings unchanged.

**Tech Stack:** Rust, `syn`, `quote`, Serde/Postcard, Tokio integration tests.

---

### Task 1: Specify Priority at the Core and Macro Boundary

**Files:**
- Modify: `crates/catga-core/src/message.rs`
- Modify: `crates/catga-macros/src/lib.rs`
- Test: `tests/macros.rs`

- [x] Add `fn priority(&self) -> MessagePriority { MessagePriority::Normal }` to `Message`, with Rustdoc stating that implementations may select a transport priority.
- [x] Add `#[catga(priority = high)]` to a derived macro test type and assert `priority() == MessagePriority::High`.
- [x] Run `cargo test --manifest-path tests/Cargo.toml --test macros --quiet`; it failed because the macro rejected `priority`.
- [x] Parse one message-level `priority` name-value option, reject duplicates and unknown values, and emit `Message::priority` only when configured.
- [x] Re-run the focused macro test; it succeeded.

### Task 2: Propagate Priority Through Typed Publication

**Files:**
- Modify: `crates/catga-codec-postcard/src/lib.rs`
- Test: `tests/codec.rs`

- [x] Define a serializable derived high-priority message and assert the envelope received through `PostcardTransport` has `MessagePriority::High`.
- [x] Mutate the metadata assignment and run the focused test; it failed at `Normal != High`.
- [x] Add `.with_priority(message.priority())` to the metadata built by `PostcardTransport`.
- [x] Re-run the focused test; it succeeded.

### Task 3: Propagate Priority Through Durable Scheduling and Requests

**Files:**
- Modify: `crates/catga-codec-postcard/src/lib.rs`
- Test: `tests/reliability/scheduled_outbox.rs`
- Test: `tests/transport/request_client.rs`

- [x] Declare a high-priority scheduled message and assert its due claimed envelope is high priority.
- [x] Declare a high-priority typed request and assert the capturing request transport receives high priority.
- [x] Mutate each metadata assignment and run both focused tests; each failed at `Normal != High`.
- [x] Add `.with_priority(message.priority())` to scheduled metadata and `.with_priority(request.priority())` to request metadata.
- [x] Re-run both focused tests; they succeeded.

### Task 4: Document and Verify the Slice

**Files:**
- Modify: `docs/source-compatibility-matrix.md`
- Modify: `docs/superpowers/specs/2026-07-25-message-priority-design.md`

- [x] Add the source-to-Rust priority mapping and state that priority is a typed metadata field rather than `x-priority`.
- [x] Run formatting, focused tests, Clippy, Rustdoc, and workspace tests without failures.
- [x] Search production code for `unwrap`/`expect` additions and scan the workspace for excluded-broker references; no production panic additions or excluded-adapter references were found.
