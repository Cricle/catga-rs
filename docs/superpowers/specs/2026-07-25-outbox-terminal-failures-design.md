# Outbox Terminal Failures Design

## Goal

Preserve the upstream outbox retry contract in pure Rust: a failed publication
records its reason and attempt count, becomes terminal after its configured
limit, and is never reclaimed for an unbounded retry loop.

## Model

`OutboxMessage` owns a fixed-size retry policy and mutable delivery outcome:
`retry_count`, `max_retries`, and an optional last-error string. New messages
default to the upstream limit of three failed publish attempts. A caller can
choose a different nonzero per-message maximum through a validated builder.
Error text is retained as `Box<str>` and is limited to 1 KiB at a UTF-8 code
point boundary, so a remote error cannot grow an outbox record without bound.

`OutboxState` adds `Failed`. It is terminal: it has no owner, is not eligible
for `claim`, and cannot be cancelled as a still-pending scheduled message.
The failed record remains durable for inspection, matching the source
`OutboxStatus.Failed` behavior. It is intentionally not translated into the
portable dead-letter abstraction, which represents a different contract.

## Store Contract

`OutboxStore::record_failure(owner, id, reason)` is the only counting failure
transition. Backends must verify ownership and atomically either return the
record to `Pending` or mark it `Failed`. A stale worker makes no change.
`release` remains a non-counting owner-checked return to pending for callers
that abandon a claim before attempting publication.

Memory mutates the single DashMap shard entry. Redis uses one Lua script to
check owner, update fields, and remove terminal records from its sorted pending
index. NATS carries status, retry fields, and error in a versioned private KV
record and performs the transition through its existing revision compare-and-
swap. Legacy NATS records decode as pending messages with the default retry
policy, so upgrading does not discard queued messages.

## Processor And Safety

`OutboxProcessor` records either a publish or durable-acknowledgement error
through `record_failure`; it never blindly releases a delivery failure. A
backend failure while recording that transition remains a structured error and
does not hide the original durability fault. Batches remain bounded by the
existing claim and concurrency limits, with no retained retry task or runtime
worker.

## Validation

Focused tests cover the default and explicit policy, counted retry transition,
terminal non-reclaimability, stale-owner safety, bounded UTF-8 error storage,
and processor exhaustion. Redis and NATS endpoint-gated regressions verify the
same durable transition. Quality gates compile public documentation with
warnings denied, reject production panic macros, and audit excluded broker
dependencies.
