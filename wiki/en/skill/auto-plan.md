# Catga Auto and Runtime Correctness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Repair the reviewed durability and recovery contracts, then add a typed `catga-auto` facade for web and real-time applications without adding hot-path overhead.

**Architecture:** Existing transports, stores, projections, Flow, and cluster runtimes remain the source of truth. Correctness fixes are isolated behind their current traits; `catga-auto` only composes typed handlers, explicit lifecycle runners, and optional Axum/NATS/Redis adapters at startup.

**Tech Stack:** Rust 2024, Tokio, async-trait, NATS JetStream, Axum, existing Catga macros, integration tests, Docker Compose E2E.

---

## Task 1: NATS consumer subject isolation

**Files:**
- Modify: `crates/catga-nats/src/transport.rs`
- Test: `crates/catga-nats/tests/nats_contracts.rs`

- [ ] Add a failing contract test that provisions one stream with `orders.created` and `orders.other`, connects a worker for `orders.created`, publishes one message to each subject, and asserts the worker receives only `orders.created`.
- [ ] Run `cargo test -p catga-nats --test nats_contracts shared_stream_subject_filter -- --ignored`; verify failure shows an unrelated delivery.
- [ ] Pass `config.subject` into every JetStream pull-consumer configuration, including named destinations, and validate an existing consumer with a conflicting filter as a structured validation error.
- [ ] Run the focused test, all regular NATS tests, strict Clippy, and fmt.

## Task 2: NATS event-store versioning and index recovery

**Files:**
- Modify: `crates/catga-nats/src/event_store.rs`
- Modify: `crates/catga-nats/src/projection.rs`
- Test: `crates/catga-nats/tests/nats_contracts.rs`

- [ ] Add failing tests for concurrent `append(..., None)`, stream-ID reconstruction after a missing `_IDS` entry, and a retry after a partial multi-event publish.
- [ ] Run each focused ignored test against isolated NATS and record the expected duplicate-version/orphan behavior.
- [ ] Make every append reserve the subject sequence with JetStream CAS, use a recoverable append marker or deterministic event IDs for multi-event retries, and reconcile stream IDs from retained event subjects before projection enumeration.
- [ ] Preserve the existing optimistic `expected_version` conflict contract and map broker conflicts to `ErrorCode::Conflict`.
- [ ] Run all NATS contract tests, including the 25 ignored tests, plus Clippy and fmt.

## Task 3: Projection, Flow, and Raft recovery windows

**Files:**
- Modify: `crates/catga-core/src/projection.rs`
- Modify: `crates/catga-flow/src/runtime.rs`
- Modify: `crates/catga-flow/src/due_service.rs`
- Modify: `crates/catga-cluster/src/runtime.rs`
- Test: `crates/catga-core/tests/recovery_runtime_contracts.rs`
- Test: `crates/catga-flow/tests/recovery.rs`
- Test: `crates/catga-cluster/tests/runtime.rs`

- [ ] Add failing tests for `i64::MAX` projection cursors, checkpoint-save retry, interrupted wait completion, cancellation during due-service resume, and bounded Raft committed-entry refill.
- [ ] Run the focused tests and confirm each fails for the reviewed crash window rather than a test setup error.
- [ ] Replace unchecked cursor increments with `checked_add` and `CatgaError::Validation`; make projection replay require idempotent application or return a clear contract error.
- [ ] Make wait completion and due-service cancellation retain a durable resumable state until the continuation is acknowledged; make runtime drain refill the pending queue and return refill errors.
- [ ] Run affected crate tests, ignored recovery tests where available, strict Clippy, and fmt.

## Task 4: Restart-safe distributed Todo example

**Files:**
- Modify: `examples/src/distributed_todo.rs`
- Modify: `examples/src/bin/distributed_todo_api.rs`
- Modify: `examples/distributed-todo/verify.sh`
- Test: `examples/tests/distributed_todo.rs`

- [ ] Add a failing restart test that writes an event, recreates the API projection, runs catch-up with the durable checkpoint, and expects the Todo to remain visible.
- [ ] Run the test and verify the fresh in-memory projection incorrectly skips the historical event.
- [ ] Make the sample projection idempotent by Todo ID and rebuild it when using a durable checkpoint, or persist the read model with the checkpoint; retain the asynchronous `202 Accepted` contract.
- [ ] Extend the black-box script to stop/restart the API and verify the Todo remains visible; add worker readiness/log failure checks.
- [ ] Run example tests and the host NATS black-box test.

## Task 5: `catga-auto` core facade

**Files:**
- Create: `crates/catga-auto/Cargo.toml`
- Create: `crates/catga-auto/src/lib.rs`
- Modify: `Cargo.toml`
- Test: `crates/catga-auto/tests/builder.rs`

- [ ] Add the crate to the workspace and write failing tests for `AutoApp::builder`, typed command/query/event registration, duplicate registration validation, and explicit shutdown ownership.
- [ ] Run `cargo test -p catga-auto --test builder` and verify missing APIs fail compilation.
- [ ] Implement `AutoAppBuilder` over the existing `Registry`/`Mediator` and `CompetingConsumer` contracts. Store only startup-built typed/static components; expose `mediator()`, `consumer()`, and `run_until_cancelled()` without spawning tasks.
- [ ] Add feature flags `axum`, `nats`, `redis`, `flow`, and `cluster`; keep default dependencies limited to `catga-core`.
- [ ] Run crate tests, doctests, strict Clippy, and fmt.

## Task 6: `catga-auto` Axum and real-time adapters

**Files:**
- Modify: `crates/catga-auto/Cargo.toml`
- Modify: `crates/catga-auto/src/lib.rs`
- Create: `crates/catga-auto/src/axum.rs`
- Create: `crates/catga-auto/src/realtime.rs`
- Test: `crates/catga-auto/tests/axum.rs`

- [ ] Add failing tests for typed JSON command/query routes, default bounded body limits, correlation propagation, and a typed event subscription stream that has explicit cancellation.
- [ ] Run the focused tests and verify missing route/stream adapters fail.
- [ ] Implement Axum adapters as startup-built routes delegating to `catga-axum`; use existing `CatgaHttpResult` mapping and no per-request reflection.
- [ ] Implement a transport-neutral realtime subscription interface over `SubscriptionRunner`/typed transport with explicit backpressure and shutdown; do not add a websocket dependency to the default feature.
- [ ] Run Axum tests, examples, Clippy, fmt, and a local HTTP integration test.

## Task 7: CI, release metadata, and comparison documentation

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/release.yml`
- Modify: `README.md`
- Modify: `examples/distributed-todo/compose.yaml`
- Modify: `examples/distributed-todo/verify.sh`

- [ ] Add a failing CI validation step that runs the distributed Todo black-box script and a restart assertion.
- [ ] Run the workflow validation script locally and verify the new step is selected by the E2E profile.
- [ ] Add NATS healthcheck/service-healthy ordering and worker readiness checks.
- [ ] Define one release version source, validate tag-to-manifest equality before publish, and update README install versions consistently.
- [ ] Add a short `cqrs-es` comparison that states its aggregate/ES maturity and Catga's distributed-runtime scope without claiming replacement.
- [ ] Run the complete workspace fmt, strict Clippy, regular tests, selected ignored E2E tests, and `git diff --check`.

## Final verification

- [ ] Run `cargo test --workspace` with external-service tests selected according to CI availability.
- [ ] Run all affected ignored NATS/Flow/cluster recovery tests with Compose services.
- [ ] Run `examples/distributed-todo/verify.sh` including API restart.
- [ ] Run `cargo fmt --all -- --check`, workspace Clippy with `-D warnings`, and `git diff --check`.
- [ ] Review the final diff for accidental API or dependency changes and report any environment-blocked E2E tests explicitly.
