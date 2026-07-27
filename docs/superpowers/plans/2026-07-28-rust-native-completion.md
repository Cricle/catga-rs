# Rust-native completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the audited Rust-native equivalents for production retry jitter, child-Flow completion routing, and state-machine event-category matching.

**Architecture:** `catga-core` supplies a bounded full-jitter default while retaining explicit deterministic policies. `catga-flow` supplies a transport-neutral completion adapter over the existing fenced runtime API, and a deliberately explicit category transition path that retains typed extraction rather than using reflection. All behavior stays caller-driven; no workers, polling, or queues are introduced.

**Tech Stack:** Rust 2024, Tokio, `Arc`, atomic operations, Catga Core/Flow traits, integration tests in the workspace `tests/` crate.

---

## File structure

- Modify: `crates/catga-core/src/retry_jitter.rs` — default full-jitter seed and policy inspection.
- Modify: `crates/catga-core/src/behaviors/retry.rs` — default policy and read-only policy accessor.
- Modify: `crates/catga-core/src/resilience.rs` — default policy and read-only policy accessor.
- Modify: `crates/catga-core/src/message.rs` — zero-allocation event category declaration hook.
- Modify: `crates/catga-flow/src/completion.rs` — transport-neutral completion value and adapter.
- Modify: `crates/catga-flow/src/lib.rs` — public Flow completion exports.
- Modify: `crates/catga-flow/src/state_machine/actions.rs` — exact/category transition matching and typed extraction.
- Modify: `crates/catga-flow/src/state_machine/definition.rs` — immutable category transition definition and builder API.
- Modify: `crates/catga-flow/src/state_machine/executor.rs` and `router.rs` — forward declared categories through typed handling.
- Modify: `tests/pipeline.rs`, `tests/resilience.rs`, `tests/flow/suspension.rs`, and `tests/state_machine.rs` — external behavior regressions.
- Modify: `docs/source-compatibility-matrix.md` — resulting source-to-Rust mapping.

### Task 1: Make full jitter the observable production default

**Files:**
- Modify: `crates/catga-core/src/retry_jitter.rs`
- Modify: `crates/catga-core/src/behaviors/retry.rs`
- Modify: `crates/catga-core/src/resilience.rs`
- Test: `tests/pipeline.rs`
- Test: `tests/resilience.rs`

- [ ] **Step 1: Write external failing tests for constructor policy selection.**

  Add tests that assert a default retry behavior and resilience executor expose `RetryJitter::Full`, and that explicit `RetryJitter::none()` and `RetryJitter::fixed(Duration::ZERO)` policies are preserved. Use a zero retry delay so the test never waits.

  ```rust
  #[test]
  fn default_retry_behavior_uses_full_jitter() {
      assert!(matches!(
          RetryBehavior::new(1, Duration::ZERO).jitter_policy(),
          RetryJitter::Full { .. }
      ));
  }
  ```

- [ ] **Step 2: Run the new tests and verify they fail because the accessors/default do not exist.**

  Run: `rtk cargo test -p catga-tests --test pipeline default_retry_behavior_uses_full_jitter -- --exact`

  Expected: compilation failure mentioning `jitter_policy` or an assertion failure showing `RetryJitter::None`.

- [ ] **Step 3: Add one bounded default policy and use it in both constructors.**

  In `retry_jitter.rs`, add one documented `const DEFAULT_FULL_JITTER_SEED: u64` and:

  ```rust
  pub const fn production_default() -> Self {
      Self::Full { seed: DEFAULT_FULL_JITTER_SEED }
  }
  ```

  Add `RetryJitterState::policy(&self) -> RetryJitter`. Change `RetryBehavior::new` and `ResilienceExecutor::new` to pass `RetryJitter::production_default()`. Add documented `jitter_policy(&self) -> RetryJitter` accessors which return the stored policy without sampling or allocation. Do not alter `with_jitter` or `with_policies`.

- [ ] **Step 4: Run the focused tests and the Core quality gates.**

  Run: `rtk cargo test -p catga-tests --test pipeline default_retry_behavior_uses_full_jitter -- --exact && rtk cargo test -p catga-tests --test resilience default_resilience_executor_uses_full_jitter -- --exact && rtk cargo clippy -p catga-core --all-targets --all-features -- -D warnings`

  Expected: each test reports `1 passed`; Clippy exits zero.

- [ ] **Step 5: Commit the isolated Core policy change.**

  ```bash
  rtk git add crates/catga-core/src/retry_jitter.rs crates/catga-core/src/behaviors/retry.rs crates/catga-core/src/resilience.rs tests/pipeline.rs tests/resilience.rs
  rtk git commit -m "feat: default resilience retries to full jitter"
  ```

### Task 2: Add a caller-owned Flow completion adapter

**Files:**
- Create: `crates/catga-flow/src/completion.rs`
- Modify: `crates/catga-flow/src/lib.rs`
- Test: `tests/flow/suspension.rs`

- [ ] **Step 1: Write external failing completion-adapter tests.**

  Reuse the existing memory suspended-flow setup. Assert that `FlowCompletionAdapter::record(FlowCompletion::success("parent-correlation", "child-a", payload))` suspends after the first child and succeeds after the final child; assert `FlowCompletion::failure` produces the same terminal failure as `record_wait_failure_by_correlation`; assert an unknown correlation returns `ErrorCode::NotFound`.

  ```rust
  let adapter = FlowCompletionAdapter::new(Arc::clone(&runtime));
  let result = adapter.record(FlowCompletion::success(
      "parent-correlation", "child-a", b"first".to_vec(),
  )).await?;
  assert!(result.is_suspended());
  ```

- [ ] **Step 2: Run the new tests and verify they fail because the public types do not exist.**

  Run: `rtk cargo test -p catga-tests --test flow_suspension flow_completion_adapter -- --nocapture`

  Expected: compilation failure naming `FlowCompletionAdapter` and `FlowCompletion`.

- [ ] **Step 3: Implement the thin adapter without duplicating persistence logic.**

  Create `completion.rs` with this shape:

  ```rust
  pub enum FlowCompletion {
      Success { correlation_id: Box<str>, child_id: Box<str>, payload: Vec<u8> },
      Failure { correlation_id: Box<str>, child_id: Box<str>, error: CatgaError },
  }

  pub struct FlowCompletionAdapter<S: ?Sized, H: ?Sized> {
      runtime: Arc<FlowRuntime<S, H>>,
  }

  impl<S, H> FlowCompletionAdapter<S, H>
  where S: SuspendedFlowStore + ?Sized, H: FlowScheduler + ?Sized {
      pub async fn record(&self, completion: FlowCompletion) -> CatgaResult<FlowRuntimeResult> {
          match completion {
              FlowCompletion::Success { correlation_id, child_id, payload } =>
                  self.runtime.record_wait_success_by_correlation(&correlation_id, &child_id, payload).await,
              FlowCompletion::Failure { correlation_id, child_id, error } =>
                  self.runtime.record_wait_failure_by_correlation(&correlation_id, &child_id, error).await,
          }
      }
  }
  ```

  Provide documented `success`, `failure`, and read-only identity constructors/accessors that own input text as `Box<str>`. Export both types in `lib.rs`. Do not decode bytes, acknowledge transport deliveries, add retries, or catch errors; `FlowRuntime` remains the single source of validation, fencing, duplicate handling, and resumption.

- [ ] **Step 4: Run focused Flow tests and documentation checks.**

  Run: `rtk cargo test -p catga-tests --test flow_suspension flow_completion_adapter -- --nocapture && rtk env RUSTDOCFLAGS='-D warnings' cargo doc -p catga-flow --all-features --no-deps`

  Expected: completion tests pass and Rustdoc exits zero.

- [ ] **Step 5: Commit the completion adapter.**

  ```bash
  rtk git add crates/catga-flow/src/completion.rs crates/catga-flow/src/lib.rs tests/flow/suspension.rs
  rtk git commit -m "feat: add explicit flow completion adapter"
  ```

### Task 3: Add explicit, bounded state-machine event categories

**Files:**
- Modify: `crates/catga-core/src/message.rs`
- Modify: `crates/catga-flow/src/state_machine/actions.rs`
- Modify: `crates/catga-flow/src/state_machine/definition.rs`
- Modify: `crates/catga-flow/src/state_machine/executor.rs`
- Modify: `crates/catga-flow/src/state_machine/router.rs`
- Test: `tests/state_machine.rs`

- [ ] **Step 1: Write failing category transition tests.**

  Define a marker `PaymentEvent`, an event whose `Event::categories` returns `&[TypeId::of::<PaymentEvent>()]`, and an unrelated event. Configure an exact transition and a category transition in the same state. Assert exact transition wins; assert the categorized event reaches the category transition; assert the unrelated event is unhandled; assert an extractor returning `None` is unhandled rather than panicking.

  ```rust
  impl Event for CardPaid {
      fn categories(&self) -> &'static [TypeId] { &[TypeId::of::<PaymentEvent>()] }
  }
  ```

- [ ] **Step 2: Run the focused category tests and verify they fail to compile.**

  Run: `rtk cargo test -p catga-tests --test state_machine category_transition -- --nocapture`

  Expected: compilation failure because `Event::categories` and `on_category` do not exist.

- [ ] **Step 3: Add the category declaration and immutable matching path.**

  Extend `catga_core::Event` with documented default method:

  ```rust
  fn categories(&self) -> &'static [std::any::TypeId] { &[] }
  ```

  Replace `ErasedTransition::event_type()` selection with a documented `matches(event_type, categories)` method. Exact `TypedTransition<E>` matches only `TypeId::of::<E>()`. Add a `CategoryTransition<S, K>` holding the marker `TypeId`, an explicit extractor `Fn(&dyn Any) -> Option<&dyn Any>`, and erased guard/action callbacks. Its `matches` only checks `categories.contains(&marker)`, and its extractor returning `None` makes the transition inapplicable. Add `StateDefinitionBuilder::on_category<C, F>(extractor: F)` returning a category builder whose `when`, `execute`, `execute_async`, `transition_to`, and `finish` mirror the existing transition builder with `&dyn Any` input.

  Thread `event.categories()` from the typed `StateMachine::handle`, executor, and router paths into `handle_erased`. Keep `handle_erased` crate-private and accept an explicit category slice so fallback callers pass the categories available from their static event type. Exact transitions are scanned before category transitions, while preserving registration order within each class. Do not add a mutable registry, a type scan, or a per-message allocation.

- [ ] **Step 4: Run all state-machine tests and Flow Clippy.**

  Run: `rtk cargo test -p catga-tests --test state_machine && rtk cargo clippy -p catga-flow --all-targets --all-features -- -D warnings`

  Expected: all state-machine tests pass; Clippy exits zero.

- [ ] **Step 5: Commit the category support.**

  ```bash
  rtk git add crates/catga-core/src/message.rs crates/catga-flow/src/state_machine/actions.rs crates/catga-flow/src/state_machine/definition.rs crates/catga-flow/src/state_machine/executor.rs crates/catga-flow/src/state_machine/router.rs tests/state_machine.rs
  rtk git commit -m "feat: add explicit state-machine event categories"
  ```

### Task 4: Document and verify the combined behavior

**Files:**
- Modify: `docs/source-compatibility-matrix.md`
- Test: workspace-wide gates

- [ ] **Step 1: Update the migration matrix with the precise Rust mappings.**

  State that retry defaults now use full jitter with explicit deterministic injection; Flow completion is a caller-owned adapter over correlation-fenced APIs; state-machine category matching is explicit and extractor-based, rather than reflection-based inheritance. Retain the exclusions for RabbitMQ/AMQP, hot reload, and HTTP health routes.

- [ ] **Step 2: Run formatting and focused regression tests.**

  Run: `rtk cargo fmt --all -- --check && rtk cargo test -p catga-tests --test pipeline --test resilience --test flow_suspension --test state_machine`

  Expected: formatting exits zero and all selected tests pass.

- [ ] **Step 3: Run the workspace quality gates under the disk-safe profile.**

  Run: `rtk proxy env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --all-features && rtk proxy env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo clippy --workspace --all-targets --all-features -- -D warnings && rtk proxy env RUSTDOCFLAGS='-D warnings' CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo doc --workspace --all-features --no-deps && rtk git diff --check`

  Expected: all commands exit zero. If the service URLs are configured, additionally run the existing Flow-store E2E targets; otherwise record that those integration tests deliberately skip live operations.

- [ ] **Step 4: Commit documentation and verification-only changes.**

  ```bash
  rtk git add docs/source-compatibility-matrix.md
  rtk git commit -m "docs: record rust-native completion semantics"
  ```
