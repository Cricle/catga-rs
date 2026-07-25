# Durable Flow Suspension Design

## Scope

Add the first reusable Rust DSL runtime slice: a flow can suspend for a delay or an externally
completed wait condition, persist its immutable continuation, and resume through an application
provided scheduler. This intentionally excludes nested branch execution and `ForEach`; those need
their own recoverable progress format and follow after this persistence boundary is stable.

## API

`catga-flow` gains three small contracts:

- `FlowScheduler` schedules and cancels a named flow resume without coupling Catga to a job system.
- `WaitCondition` is immutable data containing the flow id, step, deadline, expected result count,
  and completed results. `WaitPolicy` supports `All` and `Any`.
- `SuspendedFlowStore` persists and atomically updates `FlowContinuation` records plus their wait
  conditions. Continuations use the existing `FlowState` identity/version/owner invariants and
  store step names rather than Rust closures.

A `FlowDefinition` registers named `FlowStepHandler`s in process memory. A handler receives the
immutable state and returns one of `Advance`, `SuspendUntil`, `Wait`, or `Fail`. This keeps the
user API simple while making persistence safe: closures are never serialized. A restarted process
re-registers the definition and then resumes from the persisted step name.

## Execution and recovery

Starting a definition creates a `FlowState` and a continuation at the first step. Each successful
transition is a CAS update. `SuspendUntil` records a deadline then asks `FlowScheduler` to enqueue
a resume; scheduler calls are explicitly at-least-once, so `resume` is idempotent and accepts a
continuation only once through its version CAS. `Wait` stores the condition before returning
`Suspended`; `record_wait_result` CAS-updates the condition. A terminal result or timeout moves the
continuation forward or fails it with the original error.

The scheduler is deliberately external. Catga offers a deterministic in-memory test scheduler but
does not hide a spawned timer task, avoiding ownership races and process-local scheduling that
cannot survive restart. Production adapters can target any Rust job system without changing flow
semantics.

## Memory and concurrency

`MemorySuspendedFlows` uses `DashMap` only to find an `ArcSwap` slot. Each continuation and wait
condition is immutable; writes use short pointer CAS loops, and map guards never survive a loop or
an await. Input state remains `Arc<[u8]>`; wait result payloads use `Arc<[u8]>` for the same reason.
No mutex is introduced for execution, scheduling, or wait completion.

## Error handling

Missing definitions, missing steps, duplicate schedule attempts, and stale versions return
structured `CatgaError`s. Scheduler failure is persisted as an ordinary flow failure only after a
continuation has been saved, so there is no untracked suspended state. Duplicate child results are
ignored by child id. `All` fails when any result fails; `Any` completes on the first success and
fails only once every expected child has failed. Expired waits fail deterministically.

## Tests

Only root integration tests will be added. They prove delayed continuation persistence and resume,
`All` and `Any` result semantics, timeout behavior, duplicate-result idempotency, stale CAS
rejection, and concurrent wait-result recording. Tests use the in-memory store/scheduler and do
not place test modules in source directories.
