# Flow Runtime Observability Design

## Goal

Expose the upstream Flow lifecycle signals through the pure-Rust durable
`FlowRuntime` without introducing a registry, polling loop, dynamic metric
labels, background task, or changed flow result.

## Boundaries

The runtime owns a lifecycle event only when it owns a durable transition:

* `start` records `catga.flow.started` after `SuspendedFlowStore::create`
  successfully creates a new continuation.
* `drive` records one step attempt before invoking the registered handler. It
  records a success and duration when the handler returns a `FlowStepOutcome`,
  or a failure and duration when the handler returns an error.
* Successful persistence of `Done` records `catga.flow.completed`; successful
  persistence of `Failed` records `catga.flow.failed`. A stale CAS error
  records neither terminal outcome because this runtime did not own the state
  change.
* A `FlowExecution` guard increments `catga.flow.active` before the claimed
  continuation is driven and decrements it on completion or future
  cancellation. This is an exact in-process execution gauge, not a count of
  durable suspended continuations. It therefore remains correct after restart
  without retaining flow identities in memory.

Cancellation, validation, scheduler, persistence, and handler errors retain
their original `CatgaResult`. The guard records no synthetic result and starts
no task.

## Metric Contract

The internal `catga_flow::metrics` module defines these stable metric names:

| Signal | Name | Labels |
| --- | --- | --- |
| Flow creation | `catga.flow.started` | none |
| Terminal success | `catga.flow.completed` | none |
| Terminal failure | `catga.flow.failed` | none |
| Step attempt | `catga.flow.step.executed` | none |
| Step outcome | `catga.flow.step.succeeded`, `catga.flow.step.failed` | none |
| Flow execution duration | `catga.flow.duration` | `outcome` (`success`, `failure`, `suspended`, `aborted`) |
| Step duration | `catga.flow.step.duration` | `outcome` (`success`, `failure`, `aborted`) |
| Active drives | `catga.flow.active` | none |

Flow IDs, definition names, step names, and error text are dynamic values. They
are written only as structured fields on a `catga` tracing span and never used
as metric labels. The per-runtime gauge uses `AtomicUsize` with a guard that
cannot decrement twice, so it needs no lock or map allocation.

## Runtime Shape

`FlowMetrics` owns `Arc<FlowMetricsState>`, which contains only the active
atomic counter. `FlowRuntime` owns one `FlowMetrics` value; constructors and
the registry runtime's shared-definition constructor pass it through unchanged.
`FlowExecution` and `FlowStepOperation` are private RAII guards. Each captures
an `Instant`, a tracing span, and static metadata. They update the configured
`metrics` recorder synchronously in `Drop` or explicit completion.

This keeps metric state O(number of runtimes) and the active count O(1).
Durable flow data stays in its configured store and all concurrency remains
governed by existing claim and CAS operations.

## Tests

The integration recorder in `tests/observability.rs` runs one successful
two-step flow and one handler-failing flow against `MemorySuspendedFlows` and
`MemoryFlowScheduler`. It asserts exact counter values, two successful step
durations, one failed step duration, one terminal completion, one terminal
failure, and a final active gauge of zero. A cancellation test drops a pending
step future after it has entered `drive`; it asserts that the active gauge is
restored to zero and the flow-duration histogram is tagged `aborted`.

All tests run without Redis or JetStream. Formatting, Clippy with warnings
denied, Rustdoc with warnings denied, and the production panic audit remain
required gates.
