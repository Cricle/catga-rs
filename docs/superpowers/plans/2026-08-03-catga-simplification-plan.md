# Catga-RS Simplification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Simplify catga-rs from 18 crates to 4 by merging macros, testing, auto-discovery, and codecs into catga-core.

**Architecture:** The implementation merges catga-macros, catga-testing, catga-auto, and both codec crates into catga-core. The Transport trait is preserved as-is from new_transport.rs. Handler attachment remains struct-based (Handler trait impl), not functional.

**Tech Stack:** Rust 1.96, async-trait, tokio, dashmap

## Global Constraints

- Rust version: 1.96 (from workspace)
- All crates use `edition = "2024"`
- No `unsafe_code` allowed
- Public APIs must have Rustdoc (`missing_docs = "deny"`)
- Use `expect()` instead of `unwrap()` with context

---

## Task 1: Create merged catga-core/src/macros.rs directory

**Files:**
- Create: `crates/catga-core/src/macros/`
- Create: `crates/catga-core/src/macros/lib.rs`
- Create: `crates/catga-core/src/macros/derive_command.rs`
- Create: `crates/catga-core/src/macros/derive_event.rs`
- Create: `crates/catga-core/src/macros/derive_request.rs`
- Create: `crates/catga-core/src/macros/handlers.rs`
- Create: `crates/catga-core/src/macros/message.rs`
- Create: `crates/catga-core/src/macros/catga_main.rs`
- Create: `crates/catga-core/src/macros/typed_mediator.rs`
- Create: `crates/catga-core/src/macros/auto.rs`
- Delete: `crates/catga-macros/src/`

**Interfaces:**
- Produces: Public re-exports via `catga_core::catga_request`, `catga_core::catga_command`, `catga_core::catga_event`, `catga_core::catga_handlers`, `catga_core::catga_main`, `catga_core::catga_auto`, `catga_core::catga_handler`, `catga_core::catga_typed_mediator`

- [ ] **Step 1: Create macros directory structure**

```bash
mkdir -p crates/catga-core/src/macros
```

- [ ] **Step 2: Copy macro files from catga-macros**

Copy all `.rs` files from `crates/catga-macros/src/` to `crates/catga-core/src/macros/`

- [ ] **Step 3: Create macros/lib.rs**

```rust
#![forbid(unsafe_code)]
//! Procedural macros for Catga message types and handler registration.

mod auto;
mod catga_main;
mod derive_command;
mod derive_event;
mod derive_request;
mod handlers;
mod message;
mod typed_mediator;

pub use auto::{catga_auto, catga_handler};
pub use catga_main::catga_main;
pub use derive_command::catga_command;
pub use derive_event::catga_event;
pub use derive_request::catga_request;
pub use handlers::catga_handlers;
pub use message::Message;
pub use typed_mediator::catga_typed_mediator;
```

- [ ] **Step 4: Update catga-core/src/lib.rs**

Replace the `catga_macros` re-export with:
```rust
pub mod macros;
pub use macros::{
    catga_auto, catga_command, catga_event, catga_handler, catga_handlers, catga_main,
    catga_request, catga_typed_mediator, Message,
};
```

- [ ] **Step 5: Fix import paths in copied files**

Update all `use catga_macros::` to use crate-relative paths or `super::`

- [ ] **Step 6: Update Cargo.toml dependencies**

In `crates/catga-core/Cargo.toml`, add:
```toml
[dependencies]
proc-macro2 = "1"
quote = "1"
syn = "2"
```

- [ ] **Step 7: Remove catga-macros from workspace**

Remove `"crates/catga-macros"` from workspace members in `Cargo.toml`

- [ ] **Step 8: Run tests to verify macros work**

```bash
cd crates/catga-core && cargo test
```

- [ ] **Step 9: Commit**

```bash
git add crates/catga-core/src/macros crates/catga-macros
git commit -m "refactor: merge catga-macros into catga-core"
```

---

## Task 2: Create merged catga-core/src/testing.rs module

**Files:**
- Create: `crates/catga-core/src/testing.rs`
- Create: `crates/catga-core/src/testing/aggregate.rs`
- Create: `crates/catga-core/src/testing/bus_harness.rs`
- Create: `crates/catga-core/src/testing/flow.rs`
- Create: `crates/catga-core/src/testing/harness.rs`
- Delete: `crates/catga-testing/src/`

**Interfaces:**
- Consumes: catga-core types (CatgaError, Handler, EventHandler, Request, Event, Command)
- Produces: Public re-exports via `catga_core::{CatgaTestHarness, HandlerSpy, EventHandlerSpy, FlowTestContext, MessageCapture, assert_* functions}`

- [ ] **Step 1: Create testing directory structure**

```bash
mkdir -p crates/catga-core/src/testing
```

- [ ] **Step 2: Copy testing files from catga-testing**

Copy all `.rs` files from `crates/catga-testing/src/` to `crates/catga-core/src/testing/`

- [ ] **Step 3: Create testing.rs module file**

```rust
#![forbid(unsafe_code)]
//! Test helpers for Catga applications.

mod aggregate;
mod bus_harness;
mod flow;
mod harness;

pub use aggregate::{AggregateScenario, ReplayedAggregate};
pub use bus_harness::{BusTestHarness, ConsumedLog, RunningBusHarness};
pub use flow::FlowTestContext;
pub use harness::{CatgaTestHarness, RunningCatgaTestHarness};
pub use bus_harness::{HandlerSpy, EventHandlerSpy, MessageCapture};
pub use bus_harness::{assert_success, assert_failure, assert_value, assert_contains, assert_error_code};
```

- [ ] **Step 4: Update catga-core/src/lib.rs**

Add module declaration and re-exports:
```rust
pub mod testing;
pub use testing::{
    AggregateScenario, CatgaTestHarness, EventHandlerSpy, FlowTestContext, HandlerSpy,
    MessageCapture, ReplayedAggregate, RunningCatgaTestHarness, assert_contains,
    assert_error_code, assert_failure, assert_success, assert_value,
    BusTestHarness, ConsumedLog, RunningBusHarness,
};
```

- [ ] **Step 5: Add testing dependencies to Cargo.toml**

```toml
[dependencies]
dashmap = "5"
futures = "0.3"
```

- [ ] **Step 6: Remove catga-testing from workspace**

Remove `"crates/catga-testing"` from workspace members

- [ ] **Step 7: Update imports in copied files**

Update imports to use crate-relative paths

- [ ] **Step 8: Run tests to verify testing helpers work**

```bash
cd crates/catga-core && cargo test
```

- [ ] **Step 9: Commit**

```bash
git add crates/catga-core/src/testing crates/catga-testing
git commit -m "refactor: merge catga-testing into catga-core"
```

---

## Task 3: Create merged catga-core/src/auto.rs module

**Files:**
- Create: `crates/catga-core/src/auto.rs`
- Create: `crates/catga-core/src/auto/global_dispatch.rs`
- Create: `crates/catga-core/src/auto/bus.rs`
- Delete: `crates/catga-auto/src/`

**Interfaces:**
- Consumes: catga-core types (Transport, Mediator, Registry, Handler, CommandHandler, EventHandler)
- Produces: `catga_core::AutoAppBuilder`, `catga_core::AutoApp`, `catga_core::bind_transport`, `catga_core::send`, `catga_core::publish`, `catga_core::send_command`

- [ ] **Step 1: Create auto directory structure**

```bash
mkdir -p crates/catga-core/src/auto
```

- [ ] **Step 2: Copy auto files from catga-auto**

Copy all `.rs` files from `crates/catga-auto/src/` to `crates/catga-core/src/auto/`

- [ ] **Step 3: Create auto.rs module file**

```rust
#![forbid(unsafe_code)]
//! Compile-time handler discovery and application facade.

pub mod bus;
pub mod global_dispatch;

pub use bus::{Bus, BusBuilder, BusFaultPublisher, BusPublisher, BusRequestClient};
pub use global_dispatch::{bind_mediator, bind_transport, is_bound, mediator_handle, publish, send, send_command, transport};
```

- [ ] **Step 4: Update catga-core/src/lib.rs**

Add:
```rust
pub mod auto;
pub use auto::{
    AutoApp, AutoAppBuilder, Bus, BusBuilder, BusFaultPublisher, BusPublisher,
    BusRequestClient, bind_mediator, bind_transport, is_bound, mediator_handle,
    publish, send, send_command, transport,
};
```

- [ ] **Step 5: Add auto dependencies to Cargo.toml**

```toml
[dependencies]
tokio-util = { version = "0.7", features = ["sync"] }
```

- [ ] **Step 6: Remove catga-auto from workspace**

Remove `"crates/catga-auto"` from workspace members

- [ ] **Step 7: Update imports in copied files**

Update imports to use crate-relative paths, remove `pub use catga_core::catga_auto`

- [ ] **Step 8: Run tests to verify auto module works**

```bash
cd crates/catga-core && cargo test
```

- [ ] **Step 9: Commit**

```bash
git add crates/catga-core/src/auto crates/catga-auto
git commit -m "refactor: merge catga-auto into catga-core"
```

---

## Task 4: Create merged codecs in catga-core

**Files:**
- Create: `crates/catga-core/src/codecs/`
- Create: `crates/catga-core/src/codecs/mod.rs`
- Create: `crates/catga-core/src/codecs/bincode.rs` (copy from catga-codec-bincode)
- Create: `crates/catga-core/src/codecs/memorypack.rs` (copy from catga-codec-memorypack)
- Delete: `crates/catga-codec-bincode/src/`
- Delete: `crates/catga-codec-memorypack/src/`
- Delete: `crates/catga-codec-memorypack/memorypack-derive/`

**Interfaces:**
- Consumes: catga-core codec traits (EnvelopeCodec, PayloadEncoder, PayloadDecoder)
- Produces: `catga_core::codecs::BincodeCodec`, `catga_core::codecs::MemoryPackCodec`

- [ ] **Step 1: Create codecs directory structure**

```bash
mkdir -p crates/catga-core/src/codecs
```

- [ ] **Step 2: Copy bincode codec files**

Copy `crates/catga-codec-bincode/src/codec.rs` to `crates/catga-core/src/codecs/bincode.rs`

- [ ] **Step 3: Copy memorypack codec files**

Copy entire `crates/catga-codec-memorypack/src/` contents to `crates/catga-core/src/codecs/memorypack/` and restructure

- [ ] **Step 4: Create codecs/mod.rs**

```rust
#![forbid(unsafe_code)]
//! Built-in message codecs for Catga.

pub mod bincode;
pub mod memorypack;

pub use bincode::BincodeCodec;
pub use memorypack::MemoryPackCodec;
```

- [ ] **Step 5: Update catga-core/src/lib.rs**

Replace existing codec exports with:
```rust
pub mod codecs;
pub use codecs::{BincodeCodec, MemoryPackCodec};
```

- [ ] **Step 6: Add codec dependencies to Cargo.toml**

```toml
[dependencies]
bincode = "2"
memorypack = "0.2"
```

- [ ] **Step 7: Remove codec crates from workspace**

Remove:
- `"crates/catga-codec-bincode"`
- `"crates/catga-codec-memorypack"`
- `"crates/catga-codec-memorypack/memorypack-derive"`

- [ ] **Step 8: Update imports in copied codec files**

Update imports to use crate-relative paths

- [ ] **Step 9: Run tests to verify codecs work**

```bash
cd crates/catga-core && cargo test
```

- [ ] **Step 10: Commit**

```bash
git add crates/catga-core/src/codecs crates/catga-codec-bincode crates/catga-codec-memorypack
git commit -m "refactor: merge codecs into catga-core"
```

---

## Task 5: Update Transport implementations (catga-local, catga-nats, catga-redis, catga-robustmq)

**Files:**
- Modify: `crates/catga-local/src/lib.rs`
- Modify: `crates/catga-nats/src/lib.rs`
- Modify: `crates/catga-redis/src/lib.rs`
- Modify: `crates/catga-robustmq/src/lib.rs`

**Interfaces:**
- Consumes: `catga_core::Transport` trait
- Produces: Updated implementations that match the simplified Transport trait

- [ ] **Step 1: Update catga-local to use merged Transport**

Verify `crates/catga-local/src/lib.rs` uses `catga_core::Transport` (it already does)

- [ ] **Step 2: Run catga-local tests**

```bash
cd crates/catga-local && cargo test
```

- [ ] **Step 3: Update catga-nats Transport implementation**

Check and update `crates/catga-nats/src/lib.rs` to ensure it implements the new Transport trait

- [ ] **Step 4: Run catga-nats tests**

```bash
cd crates/catga-nats && cargo test
```

- [ ] **Step 5: Update catga-redis Transport implementation**

Check and update `crates/catga-redis/src/lib.rs` to ensure it implements the new Transport trait

- [ ] **Step 6: Run catga-redis tests**

```bash
cd crates/catga-redis && cargo test
```

- [ ] **Step 7: Update catga-robustmq Transport implementation**

Check and update `crates/catga-robustmq/src/lib.rs` to ensure it implements the new Transport trait

- [ ] **Step 8: Run catga-robustmq tests**

```bash
cd crates/catga-robustmq && cargo test
```

- [ ] **Step 9: Commit**

```bash
git add crates/catga-local crates/catga-nats crates/catga-redis crates/catga-robustmq
git commit -m "refactor: update Transport implementations for simplified API"
```

---

## Task 6: Update tests and examples to use merged catga-core

**Files:**
- Modify: `tests/macros.rs`
- Modify: `tests/pipeline/auto_batching.rs`
- Modify: `crates/catga-testing/tests/public_contracts.rs` (move to catga-core)
- Modify: `examples/src/simple_example.rs`
- Delete: `crates/catga-testing/tests/`

**Interfaces:**
- Consumes: Merged catga-core API
- Produces: Updated imports using `catga_core::{...}` instead of `catga_testing::{...}`

- [ ] **Step 1: Move public_contracts.rs to catga-core tests**

Copy `crates/catga-testing/tests/public_contracts.rs` to `crates/catga-core/tests/public_contracts.rs`

- [ ] **Step 2: Update test imports**

Update all `use catga_testing::` to `use catga_core::testing::` in test files

- [ ] **Step 3: Run all tests**

```bash
cargo test --workspace --exclude catga-flow --exclude catga-flow-store --exclude catga-cluster --exclude catga-memory --exclude catga-scheduler-tokio-cron --exclude catga-axum
```

- [ ] **Step 4: Update examples**

Update `examples/src/simple_example.rs` to use merged imports

- [ ] **Step 5: Commit**

```bash
git add tests/ examples/ crates/catga-core/tests/ crates/catga-testing/tests/
git commit -m "refactor: update tests and examples for merged catga-core"
```

---

## Task 7: Final cleanup and workspace verification

**Files:**
- Modify: `Cargo.toml` (workspace members)
- Delete: Removed crate directories

**Interfaces:**
- Produces: Clean 4-crate workspace

- [ ] **Step 1: Verify workspace Cargo.toml**

Final workspace members should be:
```toml
members = [
    "crates/catga-core",
    "crates/catga-local",
    "crates/catga-nats",
    "crates/catga-redis",
    "crates/catga-robustmq",
    "tests",
    "examples",
]
```

- [ ] **Step 2: Run full workspace build**

```bash
cargo build --workspace
```

- [ ] **Step 3: Run full workspace test**

```bash
cargo test --workspace
```

- [ ] **Step 4: Run clippy**

```bash
cargo clippy --workspace -- -D warnings
```

- [ ] **Step 5: Commit final state**

```bash
git add -A
git commit -m "refactor: complete crate consolidation to 4 targets"
```

---

## Success Criteria Checklist

- [ ] 4 crates: catga-core, catga-local, catga-nats, catga-robustmq (catga-redis kept as-is)
- [ ] catga-macros merged into catga-core/src/macros/
- [ ] catga-testing merged into catga-core/src/testing/
- [ ] catga-auto merged into catga-core/src/auto/
- [ ] Codecs merged into catga-core/src/codecs/
- [ ] Transport trait unchanged in new_transport.rs
- [ ] Handler attachment remains struct-based (Handler trait impl)
- [ ] All tests pass
- [ ] Examples work with new structure
