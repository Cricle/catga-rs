# Auto Batching Runner Design

## Goal

Replace automatic, framework-owned Tokio tasks with a caller-owned batching
runner. The change preserves keyed, bounded request batching while making task
startup, supervision, cancellation, and shutdown visible in application code.

## Alternatives

Keeping the lazy worker is source-compatible, but leaves task lifetime hidden
and makes failures impossible to supervise. Exposing a `start` method still
requires an internal detached flush task and allows accidental repeated starts.
The selected design returns a behavior and its single-use runner together.
It is the clearest ownership model and avoids every framework-created task.

## Public API

`AutoBatchingBehavior::new`, `with_key`, and the message-configuration
constructors return `(AutoBatchingBehavior<M>, AutoBatchingRunner<M>)` after
validating the options. The behavior owns the bounded `mpsc::Sender`; the
runner owns its matching receiver, the shard map, and all in-flight flush
futures. The runner is exported from `catga_core` alongside `BatchOptions`.

Applications explicitly supervise the returned future:

```rust
let (batching, runner) = AutoBatchingBehavior::<Request>::new(options)?;
let shutdown = CancellationToken::new();
let task = tokio::spawn(runner.run_until_cancelled(shutdown.clone()));
```

No constructor, behavior method, or flush helper calls `tokio::spawn`.

## Execution And Bounds

`handle` first short-circuits a `max_batch_size` of one. Otherwise it creates
one `oneshot`, derives a key, and sends a request into the bounded queue. A
closed runner returns `Unavailable`; a full queue waits under Tokio backpressure
until it can enqueue or its calling future is cancelled.

The runner maintains at most `max_shards` keyed `VecDeque`s and applies the
existing per-shard overflow policy, rejecting the oldest item with `Transient`.
Each deadline or threshold removes at most `max_batch_size` entries from one
non-active shard and adds its flush future to a `FuturesUnordered`. At most
`max_shards` shard futures are polled at once; an active-key set prevents a
later batch for the same key from passing an earlier batch. Each batch executes
up to `flush_concurrency` entries concurrently, retaining the source option's
throughput meaning while keeping active futures and handler calls bounded by
configured limits. Input admission remains bounded by the channel and each
per-key queue.

## Shutdown And Errors

`run_until_cancelled` owns the cancellation token. On cancellation or channel
closure it stops accepting queued entries, waits for started flushes to finish,
then resolves all unstarted queued entries with `Unavailable`. It returns
success after this deterministic drain. A dropped runner causes waiting sends
and replies to resolve as `Unavailable`, never as `Internal`.

Panics from a request handler are caught at the behavior boundary and are
reported only to that request as `Internal`; they cannot terminate the runner
or leave a reply token unresolved. Normal operational errors remain the
handler's `CatgaError` and do not affect other entries.

## Validation

Focused integration tests prove that a behavior does not execute until its
caller starts the runner, threshold and deadline flushing work with a
supervised runner, keyed queues remain independent, overflow remains bounded,
and cancellation rejects unstarted work while allowing in-flight work to
finish. Unit tests cover runner shutdown accounting and capacity handling.
`cargo fmt`, the focused test target, Clippy with warnings denied, Rustdoc with
warnings denied, and the production no-panic search are required before this
slice is considered complete.
