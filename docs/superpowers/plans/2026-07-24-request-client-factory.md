# Request Client Factory Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an allocation-light typed Postcard request-client factory corresponding to upstream `IRequestClientFactory`.

**Architecture:** The codec-local factory shares an envelope `RequestTransport` and distributed ID generator with `Arc`, validates one default timeout, and creates independent `PostcardRequestClient`s.  It uses a type name only when callers select the default destination.

**Tech Stack:** Rust 2024, catga-core request contracts, Postcard, Tokio tests.

---

### Task 1: Specify factory behavior with a failing integration test

**Files:**
- Modify: `tests/transport/request_client.rs`

- [x] Add a failing test for type-name default routing, explicit routing, and zero-timeout validation.
- [x] Run `cargo test -p catga-tests --test transport_request_client` and confirm the factory API is absent.

### Task 2: Implement and document the factory

**Files:**
- Modify: `crates/catga-codec-postcard/src/lib.rs`

- [x] Add documented factory construction and creation methods without shared mutable state.
- [x] Re-run `cargo test -p catga-tests --test transport_request_client` and confirm it passes.

### Task 3: Verify quality

**Files:**
- Modify: Rustdoc only when required

- [x] Run formatting, workspace tests, Clippy with `-D warnings`, and docs with `RUSTDOCFLAGS=-D warnings`.
- [x] Audit production source for panic-prone calls and prohibited broker references.
