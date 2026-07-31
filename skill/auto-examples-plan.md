# Catga Auto Examples Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace repetitive CQRS and HTTP startup composition in the primary examples with `catga-auto` while preserving explicit transport, Flow, cluster, and shutdown ownership.

**Architecture:** `catga-auto` remains a thin facade over the existing `Registry`, `Mediator`, and `MediatorHandle`. Examples use `AutoApp::builder()` for typed registration and obtain an `Arc<Mediator>` through a small explicit accessor for Axum state; infrastructure-only and zero-allocation examples retain their lower-level APIs. Distributed Todo keeps NATS and its consumer/projection runners caller-owned, using the facade only for application lifecycle and typed registration where it adds value.

**Tech Stack:** Rust 2024, `catga-auto`, `catga-core`, Axum, Tokio, existing `catga-axum` adapters, and example integration tests.

---

### Task 1: Expose the application-owned mediator arc

**Files:**
- Modify: `crates/catga-auto/src/lib.rs`
- Test: `crates/catga-auto/tests/builder.rs`

- [ ] Write a test that clones `app.mediator_arc()` and dispatches the same typed request as `app.mediator()`.
- [ ] Run `rtk cargo test -p catga-auto --test builder mediator_arc` and verify it fails because the accessor is missing.
- [ ] Add `pub fn mediator_arc(&self) -> Arc<Mediator>` returning a clone of the existing mediator arc; do not add a new allocation or task.
- [ ] Run the focused test, all `catga-auto` tests, and strict clippy.

### Task 2: Replace the standalone mediator example

**Files:**
- Modify: `examples/src/bin/mediator.rs`
- Modify: `examples/Cargo.toml`

- [ ] Change the example to construct `AutoApp::builder()`, register the typed request handler, call `build()`, and dispatch through the built app.
- [ ] Keep the example dependency-free from Axum/NATS and preserve the same output and assertion.
- [ ] Run `rtk cargo run -p catga-examples --bin mediator` and expect `21 doubled is 42`.

### Task 3: Replace Axum checkout startup composition

**Files:**
- Modify: `examples/src/bin/axum_checkout.rs`
- Modify: `examples/Cargo.toml`

- [ ] Add `catga-auto` to the examples workspace dependency list.
- [ ] Change `build_mediator` into `build_app`, registering both request handlers on `AutoApp::builder()` and returning `AutoApp`.
- [ ] Build the router with `app.mediator_arc()` and retain existing correlation, trace, JSON, and error adapters.
- [ ] Keep business handlers unchanged and preserve the existing HTTP behavior.
- [ ] Run `rtk cargo check -p catga-examples --bin axum_checkout` and the existing Axum example tests.

### Task 4: Replace order-service registry binding

**Files:**
- Modify: `examples/src/order_service/service.rs`
- Modify: `examples/src/order_service/in_memory.rs`
- Modify: `examples/Cargo.toml`
- Test: `examples/tests/order_service.rs`

- [ ] Add a failing construction assertion that the service uses the application-owned mediator handle for a typed request.
- [ ] Run the focused order-service test and verify the current hand-built registry is the only implementation.
- [ ] Build the handler registry through `AutoApp::builder()`, bind the runtime's mediator handle using the app handle, and store the app's mediator arc for Axum state.
- [ ] Preserve the existing in-memory event store, outbox, transport, Flow, and cluster fields; no background task is introduced.
- [ ] Run all order-service tests and the example binary check.

### Task 5: Integrate AutoApp lifecycle into distributed Todo

**Files:**
- Modify: `examples/src/bin/distributed_todo_api.rs`
- Modify: `examples/src/bin/distributed_todo_worker.rs`
- Modify: `examples/Cargo.toml`
- Test: `examples/tests/distributed_todo.rs`

- [ ] Add a lifecycle contract assertion that the API and worker own explicit cancellation tokens rather than creating hidden tasks.
- [ ] Keep NATS publisher, event store, projection runner, and `CompetingConsumer` explicitly constructed by each process.
- [ ] Use `AutoApp` shutdown tokens to coordinate the API server and consumer select loops, and retain current-thread runtime semantics for non-`Sync` delivery ownership.
- [ ] Run the distributed Todo unit/integration tests and compile both binaries with all example features.

### Task 6: Final example verification

**Files:**
- Modify: `README.md` only if command/dependency snippets need correction.

- [ ] Run `rtk cargo fmt --all -- --check`.
- [ ] Run `rtk cargo clippy -p catga-examples --all-targets --all-features -- -D warnings`.
- [ ] Run `rtk cargo test -p catga-examples --all-features`.
- [ ] Run `rtk git diff --check` and verify no infrastructure-only example was changed unnecessarily.
- [ ] Run the distributed Todo Docker verification only when external services are available; otherwise leave it to CI and report that limitation.
