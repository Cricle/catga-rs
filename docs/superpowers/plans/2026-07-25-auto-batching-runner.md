# Auto Batching Runner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans`
> to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for
> tracking.

**Goal:** Make automatic batching caller-supervised and bounded without hidden
Tokio tasks.

**Architecture:** Construction creates a bounded channel and returns an
`AutoBatchingBehavior` sender with its single-use `AutoBatchingRunner` receiver.
The runner owns all mutable queues and in-flight flush futures; it is driven by
the caller through a cancellation-aware future. The behavior only admits one
request and waits for its reply.

**Tech Stack:** Rust 2024, Tokio `mpsc` and `oneshot`, `tokio-util`
`CancellationToken`, `futures::stream::FuturesUnordered`, Catga pipelines.

---

### Task 1: Specify explicit startup

**Files:**
- Modify: `tests/pipeline/auto_batching.rs`
- Modify: `tests/Cargo.toml` only if the focused test target is not registered

- [x] **Step 1: Write a failing dropped-runner regression**

  Change behavior construction to destructure its intended paired result and
  add a test which drops the runner before sending:

  ```rust
  let (behavior, runner) = AutoBatchingBehavior::new(BatchOptions::default())?;
  drop(runner);
  let result = mediator.send_with(BatchedWork { id: 1, lane: "default" }, &pipeline).await;
  assert_eq!(result.expect_err("closed runner is reported").code(), ErrorCode::Unavailable);
  ```

- [x] **Step 2: Verify the regression fails for the missing paired API**

  Run: `rtk cargo test --manifest-path tests/Cargo.toml --test auto_batching dropped_runner --quiet`

  Expected: compilation fails because constructors return only a behavior.

- [x] **Step 3: Add test runner setup to existing batching cases**

  Use `CancellationToken::new()` and spawn the returned
  `runner.run_until_cancelled(shutdown.clone())` in each normal batching test;
  cancel and join it after assertions. This captures task ownership in the
  public usage example while retaining real mediator/pipeline execution.

- [x] **Step 4: Verify the target still fails only on the missing runner API**

  Run: `rtk cargo test --manifest-path tests/Cargo.toml --test auto_batching --quiet`

  Expected: compilation failure naming `AutoBatchingRunner` or the paired
  constructor result; no assertion failure is expected yet.

### Task 2: Add the public ownership boundary

**Files:**
- Modify: `crates/catga-core/src/behaviors/auto_batching.rs`
- Modify: `crates/catga-core/src/behaviors.rs`
- Modify: `crates/catga-core/src/lib.rs`

- [x] **Step 1: Replace lazy sender startup with paired construction**

  Define the receiver-owning type and make constructors return it:

  ```rust
  pub struct AutoBatchingRunner<M: Request> {
      receiver: mpsc::Receiver<Queued<M>>,
      options: BatchOptions,
  }

  fn with_key(...) -> CatgaResult<(Self, AutoBatchingRunner<M>)> {
      options.validate()?;
      let (sender, receiver) = mpsc::channel(options.max_queue_length);
      Ok((Self { options: options.clone(), key_selector: Arc::new(key_selector), sender },
          AutoBatchingRunner { receiver, options }))
  }
  ```

  Remove `OnceCell`, the asynchronous `sender` method, and every `tokio::spawn`
  call. Map a closed `send` or reply channel to `ErrorCode::Unavailable`.

- [x] **Step 2: Export the runner with complete Rustdoc**

  Change the behavior re-export to:

  ```rust
  pub use auto_batching::{AutoBatchingBehavior, AutoBatchingRunner, BatchOptions};
  ```

  Apply the same export in `lib.rs`; document constructor ownership, runner
  single use, admission backpressure, and shutdown result codes.

- [x] **Step 3: Verify the startup regression passes**

  Run: `rtk cargo test --manifest-path tests/Cargo.toml --test auto_batching dropped_runner --quiet`

  Expected: PASS.

### Task 3: Run bounded batches without detached tasks

**Files:**
- Modify: `crates/catga-core/src/behaviors/auto_batching.rs`
- Modify: `tests/pipeline/auto_batching.rs`

- [x] **Step 1: Write a failing cancellation regression**

  Create a runner without spawning it, enqueue one request in an owned task,
  then start and cancel its runner before the timeout. Assert that the request
  resolves with `ErrorCode::Unavailable`, rather than waiting for the timeout
  or losing the reply token.

- [x] **Step 2: Verify the cancellation regression fails**

  Run: `rtk cargo test --manifest-path tests/Cargo.toml --test auto_batching cancellation --quiet`

  Expected: FAIL because `run_until_cancelled` does not yet exist.

- [x] **Step 3: Implement cancellation-aware runner execution**

  Add `run_until_cancelled(self, shutdown: CancellationToken) -> CatgaResult<()>`.
  Its event loop must select among cancellation, new queue entries, the nearest
  non-active shard deadline, and completion from a `FuturesUnordered`. Keep at
  most `max_shards` active flush futures and track their keys so a later batch
  never overtakes an active batch for the same key. On shutdown or channel
  closure, drain each queued `Pending` with:

  ```rust
  reject_entry(entry, "automatic batch runner is unavailable");
  ```

  and then await all already-started flushes. A flush removes at most
  `max_batch_size` entries and uses
  `stream::iter(batch).for_each_concurrent(Some(flush_concurrency), execute_entry)`;
  the active-key set preserves ordering between batches for one shard.

- [x] **Step 4: Verify all batching cases pass**

  Run: `rtk cargo test --manifest-path tests/Cargo.toml --test auto_batching --quiet`

  Expected: PASS, including threshold, timeout, keyed, overflow, dropped-runner,
  cancellation, and intra-batch concurrency cases.

### Task 4: Finish documentation and verification

**Files:**
- Modify: `docs/source-compatibility-matrix.md`
- Modify: `docs/superpowers/specs/2026-07-25-auto-batching-runner-design.md`
- Modify: `docs/superpowers/plans/2026-07-25-auto-batching-runner.md`

- [x] **Step 1: Record the source mapping and the deliberate lifecycle divergence**

  Explain that the source's automatic behavior maps to a caller-owned Rust
  runner, and that it deliberately avoids an unobservable background task.

- [x] **Step 2: Run focused quality gates**

  Run:

  ```bash
  rtk cargo fmt --check
  rtk cargo test --manifest-path tests/Cargo.toml --test auto_batching --quiet
  rtk cargo test -p catga-core --quiet
  rtk cargo clippy -p catga-core --all-targets -- -D warnings
  rtk env RUSTDOCFLAGS='-D warnings' cargo doc -p catga-core --no-deps
  rtk rg -n '\.(unwrap|expect)[[:space:]]*\(|(unreachable|todo|unimplemented)![[:space:]]*\(' crates/catga-core/src/behaviors/auto_batching.rs
  rtk git diff --check
  ```

  Expected: all commands succeed and the no-panic search has no output.
