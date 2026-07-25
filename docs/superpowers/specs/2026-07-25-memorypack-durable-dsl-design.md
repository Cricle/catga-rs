# MemoryPack Compatibility and Durable DSL Recovery Design

## Goal

Close two source-compatibility gaps in the pure-Rust Catga migration: bounded
interop with C# MemoryPack 1.21.3 records used by Catga persistence, and
restart-safe nested DSL progress. HTTP health routes and the RabbitMQ/AMQP
adapter remain intentionally excluded.

## MemoryPack boundary

The new `catga-codec-memorypack` crate will be additive; Postcard remains the
default Rust-native codec. It will implement a strict, allocation-bounded
subset of MemoryPack 1.21.3 rather than depend on an unverified third-party
format implementation. The first public surface only encodes and decodes the
Catga-owned stable records whose C# formatter layout is checked into this
repository: `FlowState`, outbox, inbox, dead-letter, flow snapshot metadata,
NATS flow snapshot, and ForEach progress.

The reader consumes an exact frame, validates fixed object member counts,
strict booleans, UTF-8/UTF-16 lengths, nesting, total input and allocation
budgets before allocation, and rejects trailing bytes. Rust application
payloads are deliberately not claimed compatible until an explicit
application-owned schema implements the compatibility trait. The checked-in
MemoryPack 1.21.3 golden bytes are immutable compatibility input for Rust
tests; source code is not sufficient proof of a binary protocol. No C# code
or .NET build is part of the Rust workspace workflow.

## Durable nested DSL recovery

`DslFlow::run_checkpointed` currently persists only whole top-level steps.
The replacement checkpoint payload will retain a versioned execution cursor:

* a bounded path of step indices for nested `if`/`match` scopes;
* sequential ForEach total, next index, and completed/failed ranges;
* fixed-cardinality parallel branch status, cursor and encoded isolated state;
* the single selected winner for `when_any`.

The existing `DslStepProgressStore` remains the one per-top-level-step CAS
record. Its opaque payload becomes an internal, versioned checkpoint frame;
the store keeps one bounded value and no background task. Each checkpoint
write advances its existing optimistic version. On recovery, actions already
represented as completed are not re-run; unfinished work is admitted through
the configured parallel limit and state merge still happens in declaration
order. Streaming selectors remain process-local because replaying a generic
stream has no stable cursor; durable nested recovery therefore accepts only
replayable `Vec`-based DSL operations and returns a documented validation
error for a checkpointed streaming operation.

## Verification

MemoryPack tests use checked-in C# 1.21.3 fixtures for non-ASCII strings,
null/empty values, arrays, DateTime binary values, malformed lengths and
trailing bytes. DSL tests force a failure after each nested branch, ForEach
item and parallel branch, re-create the flow, and assert exactly-once recovery
of completed units for memory, Redis and NATS progress stores. All production
paths use `CatgaResult`, bounded allocations and no panic-prone unwraps.
