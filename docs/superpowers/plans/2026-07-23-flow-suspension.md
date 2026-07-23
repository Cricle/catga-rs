# Durable Flow Suspension Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add durable delay and external-result suspension to `catga-flow`, with lock-free in-memory persistence and scheduler-neutral resumption.

**Architecture:** Persist only immutable continuation data and registered step names. `FlowDefinition` keeps executable Rust handlers in process memory while `FlowContinuation` records the next named step, deadline, and optional `WaitCondition`. The `MemorySuspendedFlows` adapter uses a `DashMap` index to `ArcSwap` slots, so state transitions and concurrent child result recording use pointer CAS rather than broad locks.

**Tech Stack:** Rust 2024, Tokio, async-trait, futures, ArcSwap, DashMap, root `catga-tests` integration package.

---

### Task 1: Add Suspension Contracts

**Files:**
- Create: `crates/catga-flow/src/suspension.rs`
- Modify: `crates/catga-flow/src/lib.rs`
- Modify: `tests/Cargo.toml`
- Create: `tests/flow_suspension.rs`

- [ ] **Step 1: Write the failing contract test**

```rust
use std::time::{Duration, SystemTime};

use catga_flow::{FlowContinuation, FlowState, WaitCondition, WaitPolicy};

#[test]
fn continuation_keeps_shared_input_and_wait_completion_is_immutable() {
    let state = FlowState::new("flow-12", "payment", b"input".to_vec(), "node-a");
    let continuation = FlowContinuation::waiting(
        state,
        "charge",
        WaitCondition::new("wait-12", WaitPolicy::All, 2, SystemTime::now(), Duration::from_secs(30)),
    );
    let next = continuation.wait().unwrap().record_success("child-a", b"ok".to_vec());
    assert_eq!(continuation.step_name(), "charge");
    assert_eq!(next.completed_count(), 1);
    assert_eq!(next.expected_count(), 2);
}
```

- [ ] **Step 2: Verify failure**

Run: `rtk proxy cargo test -p catga-tests --test flow_suspension`

Expected: imports fail because suspension types are absent.

- [ ] **Step 3: Add immutable suspension data**

Define `WaitPolicy::{All, Any}`, `WaitResult`, `WaitCondition`, and `FlowContinuation`. Use `Arc<[u8]>` result payloads, de-duplicate child ids, derive expiry from `created_at + timeout`, and expose consuming transition methods:

```rust
pub struct FlowContinuation { state: FlowState, step_name: Box<str>, wait: Option<WaitCondition>, resume_at: Option<SystemTime> }
pub struct WaitCondition { correlation_id: Box<str>, policy: WaitPolicy, expected_count: u32, results: Arc<[WaitResult]>, created_at: SystemTime, timeout: Duration }
```

- [ ] **Step 4: Verify green**

Run: `rtk proxy cargo test -p catga-tests --test flow_suspension`

Expected: the contract test passes.

- [ ] **Step 5: Commit**

```bash
rtk proxy git add crates/catga-flow tests/Cargo.toml tests/flow_suspension.rs
rtk proxy git commit -m "feat: add durable flow suspension contracts"
```

### Task 2: Add CAS Suspension Store and Memory Adapter

**Files:**
- Create: `crates/catga-flow/src/suspension_store.rs`
- Create: `crates/catga-memory/src/suspended_flow.rs`
- Modify: `crates/catga-flow/src/lib.rs`
- Modify: `crates/catga-memory/src/lib.rs`
- Modify: `tests/flow_suspension.rs`

- [ ] **Step 1: Write failing concurrency tests**

```rust
assert!(store.create(continuation).await.unwrap());
let a = store.record_wait_success("flow-12", 0, "child-a", b"a".to_vec());
let b = store.record_wait_success("flow-12", 0, "child-b", b"b".to_vec());
let (a, b) = tokio::join!(a, b);
assert!(a.unwrap());
assert!(b.unwrap());
assert_eq!(store.get("flow-12").await.unwrap().unwrap().wait().unwrap().completed_count(), 2);
```

- [ ] **Step 2: Verify failure**

Run: `rtk proxy cargo test -p catga-tests --test flow_suspension concurrent_wait_results_are_not_lost`

Expected: failure because `SuspendedFlowStore` and `MemorySuspendedFlows` are absent.

- [ ] **Step 3: Define storage operations and CAS semantics**

```rust
#[async_trait]
pub trait SuspendedFlowStore: Send + Sync {
    async fn create(&self, continuation: FlowContinuation) -> CatgaResult<bool>;
    async fn get(&self, flow_id: &str) -> CatgaResult<Option<FlowContinuation>>;
    async fn update(&self, expected_version: i64, next: FlowContinuation) -> CatgaResult<bool>;
    async fn record_wait_success(&self, flow_id: &str, version: i64, child_id: &str, payload: Vec<u8>) -> CatgaResult<bool>;
}
```

Use per-flow `ArcSwap<FlowContinuation>` slots. `record_wait_success` retries only pointer-CAS conflicts at the same version, leaves duplicate child ids unchanged, and never retains a DashMap guard through the loop.

- [ ] **Step 4: Verify focused tests and lint**

Run: `rtk proxy cargo test -p catga-tests --test flow_suspension && rtk proxy cargo clippy -p catga-flow -p catga-memory --all-targets -- -D warnings`

Expected: all suspension tests pass and no warnings remain.

- [ ] **Step 5: Commit**

```bash
rtk proxy git add crates/catga-flow crates/catga-memory tests/flow_suspension.rs
rtk proxy git commit -m "feat: add lock-free suspended flow storage"
```

### Task 3: Add Registered Definitions and Delayed Resume

**Files:**
- Create: `crates/catga-flow/src/definition.rs`
- Create: `crates/catga-flow/src/scheduler.rs`
- Modify: `crates/catga-flow/src/lib.rs`
- Modify: `tests/flow_suspension.rs`

- [ ] **Step 1: Write failing delayed-resume test**

```rust
let scheduler = TestScheduler::default();
let definition = FlowDefinition::new("payment")
    .step("reserve", |_| async { Ok(FlowStepOutcome::suspend_until(SystemTime::now())) })
    .step("charge", |_| async { Ok(FlowStepOutcome::complete()) });
let result = runtime.start("flow-13", b"input".to_vec()).await.unwrap();
assert!(result.is_suspended());
assert_eq!(scheduler.take_due(SystemTime::now()).len(), 1);
assert!(runtime.resume("flow-13").await.unwrap().is_success());
```

- [ ] **Step 2: Verify failure**

Run: `rtk proxy cargo test -p catga-tests --test flow_suspension delayed_flow_persists_and_resumes_registered_steps`

Expected: failure because definition/runtime/scheduler APIs are absent.

- [ ] **Step 3: Implement handler registry and scheduler boundary**

Define `FlowStepHandler` as a boxed async handler and `FlowDefinition` as an ordered name-to-handler registry. `FlowStepOutcome` is `Advance`, `SuspendUntil(SystemTime)`, `Wait(WaitCondition)`, or `Fail(CatgaError)`. `FlowScheduler` has `schedule_resume(flow_id, due_at)` and `cancel(schedule_id)`; `TestScheduler` is a deterministic in-memory adapter for integration tests. `FlowRuntime` CAS-persistes before scheduling and resumes only from a registered step name.

- [ ] **Step 4: Verify delayed resumption**

Run: `rtk proxy cargo test -p catga-tests --test flow_suspension delayed_flow_persists_and_resumes_registered_steps`

Expected: one schedule is emitted; resumption executes only the next step and stores Done.

- [ ] **Step 5: Commit**

```bash
rtk proxy git add crates/catga-flow tests/flow_suspension.rs
rtk proxy git commit -m "feat: add resumable registered flow definitions"
```

### Task 4: Add Wait Policy Evaluation and Final Validation

**Files:**
- Modify: `crates/catga-flow/src/definition.rs`
- Modify: `tests/flow_suspension.rs`

- [ ] **Step 1: Write failing All/Any and timeout tests**

```rust
assert!(runtime.record_wait_success("flow-14", "one", b"one".to_vec()).await.unwrap().is_suspended());
assert!(runtime.record_wait_success("flow-14", "two", b"two".to_vec()).await.unwrap().is_success());
assert!(runtime.resume_at("expired", SystemTime::now() + Duration::from_secs(31)).await.unwrap().is_failure());
```

- [ ] **Step 2: Verify failure**

Run: `rtk proxy cargo test -p catga-tests --test flow_suspension wait_policies_resume_once_and_expire_deterministically`

Expected: failure because the runtime does not yet evaluate stored wait conditions.

- [ ] **Step 3: Implement policy evaluation**

`All` resumes after all successful child ids; the first failed child makes the continuation Failed. `Any` resumes on the first success and fails only once every expected child failed. Each child update is CAS-persisted before policy evaluation. Expired conditions move the flow to Failed with `ErrorCode::Timeout`; terminal continuations ignore duplicate results.

- [ ] **Step 4: Run complete verification at default parallelism**

Run:

```bash
rtk proxy cargo fmt --all -- --check
rtk proxy git diff --check
rtk proxy env CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --workspace --all-targets -- -D warnings
rtk proxy env CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --workspace
```

Expected: all commands exit zero; root flow suspension tests cover delay, all/any, timeout, duplicate results, and concurrent updates.

- [ ] **Step 5: Commit**

```bash
rtk proxy git add crates/catga-flow tests/flow_suspension.rs
rtk proxy git commit -m "feat: add durable flow wait policies"
```
