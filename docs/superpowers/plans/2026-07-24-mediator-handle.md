# Mediator Handle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let startup-constructed handlers explicitly use a once-bound mediator without global state.

**Architecture:** `MediatorHandle` shares an `Arc<OnceLock<Arc<Mediator>>>`; binding is one-time and dispatch reads are lock-free.  It exposes typed `send` and `publish` delegation with structured pre-bind and duplicate-bind failures.

**Tech Stack:** Rust 2024, std `OnceLock`, catga-core mediator and registry.

---

### Task 1: Write the failure-mode and handler-cycle test

**Files:**
- Modify: `tests/mediator.rs`

- [x] Add a failing test for pre-bind `Unavailable`, handler event publication after bind, and duplicate-bind `Conflict`.
- [x] Run `cargo test -p catga-tests --test mediator` and confirm `MediatorHandle` is absent.

### Task 2: Implement the explicit handle

**Files:**
- Modify: `crates/catga-core/src/mediator.rs`, `crates/catga-core/src/lib.rs`

- [x] Add documented construction, one-time binding, typed send, and typed publish methods.
- [x] Re-run `cargo test -p catga-tests --test mediator` and confirm it passes.

### Task 3: Verify quality

**Files:**
- Modify: Rustdoc only when required

- [x] Run formatting, workspace tests, Clippy with `-D warnings`, and docs with `RUSTDOCFLAGS=-D warnings`.
- [x] Audit production source for panic-prone calls and prohibited broker references.
