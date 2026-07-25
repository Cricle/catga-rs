# Message Router Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the header-driven destination router represented by upstream `IMessageRouter`.

**Architecture:** `MessageRouter` owns a small ordered vector of validated rules and an optional fallback `Destination`; it receives borrowed header slices and returns borrowed destinations.  It does not add an allocation-bearing header map to `Envelope`.

**Tech Stack:** Rust 2024, `catga-core` only.

---

### Task 1: Add the desired routing behavior test

**Files:**
- Modify: `tests/Cargo.toml`
- Create: `tests/routing.rs`

- [x] Write failing tests for invalid route input, first-match precedence, and default fallback.
- [x] Run `cargo test -p catga-tests --test routing` and confirm the missing API failure.

### Task 2: Implement the compact router

**Files:**
- Create: `crates/catga-core/src/routing.rs`
- Modify: `crates/catga-core/src/lib.rs`

- [x] Implement documented rule validation, ordered storage, and borrowed resolution.
- [x] Run `cargo test -p catga-tests --test routing` and confirm the tests pass.

### Task 3: Verify workspace quality

**Files:**
- Modify: public Rustdoc only if quality checks require it

- [x] Run formatting, workspace tests, Clippy with `-D warnings`, and documentation with `RUSTDOCFLAGS=-D warnings`.
- [x] Audit new production code for panic-prone calls and prohibited broker references.
