# Flow and Cluster Runtime Gaps Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the audited Rust semantic gaps in durable Flow coordination, leadership loss, bounded DSL execution, and wire-visible errors.

**Architecture:** Preserve explicit caller ownership. Durable Flow child fan-out receives an application-provided bounded launcher and records completions through existing CAS-backed wait methods; the runtime never owns detached child tasks. Leadership work receives a cancellation token whose epoch is invalidated by the coordinator. Concurrent streams maintain a fixed number of in-flight futures and feed an incremental reducer, while errors carry bounded structured protocol detail.

**Tech Stack:** Rust, Tokio, `tokio-util::sync::CancellationToken`, `futures`, existing Catga traits, Postcard/Serde.

---

### Task 1: Leadership-epoch cancellation

**Files:**
- Modify: `crates/catga-cluster/src/execution.rs`, `crates/catga-cluster/src/lib.rs`
- Test: `tests/cluster.rs`

- [ ] **Step 1: Write failing cancellation test**

Add a Tokio test which starts `execute_if_leader_cancellable`, waits until the action observes its token, calls `MemoryCluster::elect("node-b")`, and asserts the action returns `Cancelled` rather than running to completion.

- [ ] **Step 2: Run RED test**

Run: `rtk cargo test -p catga-tests --test cluster leadership_loss_cancels_active_action`

Expected: compilation failure naming `execute_if_leader_cancellable`.

- [ ] **Step 3: Implement the typed extension**

Expose `ClusterCoordinatorExt::execute_if_leader_cancellable(&self, action)` where `action: FnOnce(CancellationToken) -> Future<Output = CatgaResult<T>>`. Select action completion against `wait_for_leadership_change(true)`, cancel the token before returning `ErrorCode::Cancelled`, and keep nonleaders rejected with `Unavailable` before calling `action`.

- [ ] **Step 4: Run focused GREEN test**

Run: `rtk cargo test -p catga-tests --test cluster leadership_loss_cancels_active_action`

Expected: one passing test.

### Task 2: Bounded lazy concurrent Flow stream

**Files:**
- Modify: `crates/catga-flow/src/dsl.rs`, `crates/catga-flow/src/lib.rs`
- Test: `tests/flow/dsl.rs`

- [ ] **Step 1: Write failing bounded-stream test**

Add a test constructing a `futures::stream::iter(0..128)` selector, a limit of four, and an atomic in-flight counter. It must assert peak work is at most four, source polling does not reach item five before one completion is released, and a reducer receives result values in source order.

- [ ] **Step 2: Run RED test**

Run: `rtk cargo test -p catga-tests --test flow -- dsl::concurrent_stream`

Expected: compilation failure naming `for_each_stream_concurrent`.

- [ ] **Step 3: Implement incremental bounded API**

Add `DslFlow::for_each_stream_concurrent(limit, select, work, reduce)` with a `BoxStream`, fixed in-flight `FuturesUnordered`, monotonically increasing indices, and one reducer call per completed item. Do not collect items or results into `Vec`; reject zero limits using `CatgaError::Validation`.

- [ ] **Step 4: Run focused GREEN test**

Run: `rtk cargo test -p catga-tests --test flow -- dsl::concurrent_stream`

Expected: focused test passes.

### Task 3: Durable bounded child fan-out

**Files:**
- Modify: `crates/catga-flow/src/{runtime.rs,suspension.rs,lib.rs}`
- Test: `tests/flow/{suspension.rs,recovery.rs}`

- [x] **Step 1: Write failing fan-out test**

Add a test with a `WhenAll` wait for two child identities and a launcher recording exactly two requests. Verify duplicate launch attempts do not relaunch, duplicate child completion is idempotent, and the parent drives only after its second child result.

- [x] **Step 2: Run RED test**

Run: `rtk cargo test -p catga-tests --test flow_suspension durable_child_fan_out_launches_each_stable_child_once_and_rejects_unknown_results -- --exact`

Expected: compilation failure naming `FlowChildLauncher` or `launch_waiting_children`.

- [x] **Step 3: Implement bounded launcher boundary**

Add `FlowChildLauncher` with `launch(parent_id, child_id, correlation_id) -> CatgaResult<()>`. `WaitCondition::for_children` persists stable identities before any external call. `FlowRuntime::launch_waiting_children` advances each identity through a bounded pending/claimed/launched state with an expiring owner claim and exact-continuation CAS. A crash after external dispatch can repeat the stable identity, so the launcher owns idempotency. Completion remains `record_wait_success`/`record_wait_failure` followed by existing CAS resume; unknown children and oversized payloads are rejected before retention.

- [x] **Step 4: Run focused GREEN test**

Run: `rtk cargo test -p catga-tests --test flow_suspension durable_child_fan_out_launches_each_stable_child_once_and_rejects_unknown_results -- --exact`

Expected: focused test passes.

### Task 4: Wire Flow persistence tags

**Files:**
- Modify: `crates/catga-flow/src/{definition.rs,tag_policy.rs,runtime.rs}`
- Test: `tests/flow/{dsl.rs,recovery.rs}`

- [x] **Step 1: Write failing tagged-execution tests**

Add tagged definition tests that assert transient execution failures retry only within the configured bound, non-transient failures do not retry, and timeout returns `ErrorCode::Timeout` through the caller-owned runtime future. Existing durable recovery tests prove every named-step transition is already persisted, regardless of a source-style persist marker.

- [x] **Step 2: Run RED test**

Run: `rtk cargo test -p catga-tests --test flow_suspension tagged_durable_step_retries_only_transient_failures_within_its_bound -- --exact`

Expected: compilation failure naming `with_tag_policy` or `step_with_tag`.

- [x] **Step 3: Implement explicit policy composition**

Store each step's optional static tag and expose `FlowRuntime::with_tag_policy`. Execute timeout and retries in the caller-owned runtime future, retry only structured `Transient` errors while the owner heartbeat remains valid, and never spawn a detached retry task. `FlowRuntime` always persists every durable transition, so source-style persist markers do not weaken that recovery invariant.

- [x] **Step 4: Run focused GREEN tests**

Run: `rtk cargo test -p catga-tests --test flow_suspension tagged_durable_step_retries_only_transient_failures_within_its_bound -- --exact`

Expected: focused test passes.

### Task 5: Bounded wire-error detail

**Files:**
- Modify: `crates/catga-core/src/error.rs`, `crates/catga-core/src/lib.rs`, `docs/source-compatibility-matrix.md`
- Test: `tests/{error.rs,codec.rs,nats_request.rs}`

- [ ] **Step 1: Write failing error protocol tests**

Add tests that construct `CatgaError::with_details`, verify a 1 KiB limit and retryability derived from its category, and Postcard round-trip the error without losing code, message, retryability, or details.

- [ ] **Step 2: Run RED tests**

Run: `rtk cargo test -p catga-tests --test error --test codec`

Expected: compilation failure naming `with_details`, `is_retryable`, or `details`.

- [ ] **Step 3: Implement compact error fields**

Add serde-defaulted `retryable: bool` and optional bounded `details: Box<str>` to `CatgaError`. `new` derives retryability from `Transient`, `Timeout`, and `Unavailable`; constructors validate all supplied data. Extend `ErrorCode` stable mapping aliases for source `TRANSPORT_FAILED` and `SERIALIZATION_FAILED` without creating untyped string error categories.

- [ ] **Step 4: Run focused GREEN tests**

Run: `rtk cargo test -p catga-tests --test error --test codec`

Expected: focused tests pass.

### Task 6: Integrate and verify

**Files:**
- Modify: `docs/source-compatibility-matrix.md`

- [ ] **Step 1: Document completed Rust-native mappings**

Update the matrix with the exact bounds, cancellation ownership, and intentional non-goals above. Do not add RabbitMQ/AMQP or HTTP health routes.

- [ ] **Step 2: Run all quality gates**

Run: `rtk cargo fmt --all -- --check && rtk git diff --check && rtk cargo clippy --workspace --all-targets -- -D warnings && rtk cargo test --workspace --no-fail-fast && RUSTDOCFLAGS='-D warnings' rtk cargo doc --workspace --no-deps`

Expected: every command exits zero.

- [ ] **Step 3: Commit and push**

Run: `rtk git add crates tests docs && rtk git commit -m "feat: close bounded flow and cluster runtime gaps" && rtk git push -u origin feature/flow-cluster-runtime-audit`
