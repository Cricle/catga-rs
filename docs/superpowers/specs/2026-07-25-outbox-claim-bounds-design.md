# Outbox Claim Bounds Design

## Goal

Make every outbox claim path retain a fixed, explicit amount of process and
backend working memory. A caller-controlled `usize` must never determine a
vector, heap, or Redis Lua scan size without validation.

## Public Contract

`MAX_OUTBOX_CLAIM_LIMIT` is 1,024 messages. `OutboxStore::claim` accepts zero
as an allocation-free empty result for compatibility, accepts values through
the maximum, and returns `ErrorCode::Validation` for larger values. The same
upper bound is enforced by `OutboxProcessor` construction so a normal worker
cannot request a batch that stores must reject later.

Rejecting is deliberate. Clamping would make a caller believe it asked for one
batch size while silently processing another, which makes drain and throughput
control unreliable. A caller that needs more work performs another bounded
claim after completing the previous batch.

## Backend Behavior

Memory validates before `Vec::with_capacity`. NATS validates before allocating
its bounded candidate heap and continues to stream JetStream keys instead of
collecting the listing. Redis validates before invoking Lua. The Lua script
scans at most four times the requested limit from the sorted due index, while
returning at most the requested number of owner-CAS claims. This bounds Redis
script memory and CPU even if a large backlog is due; under heavy contention a
scan may intentionally return fewer candidates and the next worker iteration
tries again.

## Validation

Core and memory tests prove a maximum-sized request remains valid and an
oversized request fails before any allocation or state change. Processor tests
prove its constructor rejects the same oversized batch. Redis and NATS
environment-gated tests exercise the shared store contract. Formatting,
Clippy, warning-denied Rustdoc, panic-macro audit, broker-dependency audit,
and diff validation complete the slice.
