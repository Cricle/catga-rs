# Typed Postcard Transport Design

## Goal

Provide the source transport surface as an explicit, typed Rust adapter while
retaining the existing envelope-level traits as the stable backend boundary.

## Mapping

The source transport accepts arbitrary generic values, discovers their type and
delivery properties at runtime, serializes them, and invokes a callback-based
subscription API. Rust keeps backend contracts in terms of owned `Envelope`
values, which preserves acknowledgement-token ownership and lets every backend
apply its own backpressure. `catga-codec-postcard` will add the ergonomic typed
layer on top of that boundary.

`PostcardTransport<T>` owns an `Arc<T>`, a distributed ID generator, and the
zero-sized `PostcardCodec`. It constructs an envelope once per input, encodes
only the typed payload, and moves the envelope into the existing transport. It
provides typed publish, destination send, default and destination-specific
receive, and bounded batch methods. Destination strings are validated by the
existing `Destination` type before any payload is encoded.

Ordinary typed messages use `AtLeastOnce`, matching requests and commands. An
event-specific method constructs `AtMostOnce` metadata, matching the source
event contract. A reliable-event-specific method constructs `AtLeastOnce`
metadata. The distinction is explicit in method bounds and names because
stable Rust does not have trait specialization that could infer a subtrait's
default at an arbitrary `Message` call site. The low-level `Envelope` API
continues to accept an explicit QoS for applications that require an
alternative policy.

The same explicit choices apply to `PostcardScheduledOutbox`. Its ordinary
schedule methods retain `AtLeastOnce`; `schedule_event_at` and
`schedule_event_after` retain the event default; the reliable-event methods
retain `AtLeastOnce`. All variants use one checked wall-clock calculation and
the same durable outbox insertion path.

## Receiving and Acknowledgement

`receive<T>` will await the existing transport once, deserialize the payload
without an intermediate copy, and return `PostcardDelivery<T>`. The wrapper
owns the original `Delivery`; its `acknowledge` and `nack` methods consume the
wrapper and forward the backend acknowledgement token exactly once. Decode
failure returns a structured validation error and explicitly nacks the
delivery before returning, so a malformed retryable delivery is never silently
acknowledged or orphaned.

This replaces the source callback subscription model with caller-owned
receives. It fits Rust async cancellation and ownership: applications choose
the task lifetime, while `CompetingConsumer` remains the bounded concurrent
runner for callback-like processing.

## Memory and Concurrency

Batch APIs consume an `IntoIterator` lazily and retain at most the selected
concurrency worth of encoded envelopes and futures. Convenience batch methods
use `DEFAULT_TRANSPORT_BATCH_CONCURRENCY`; explicit methods accept a validated
override. They delegate the actual publish/send concurrency to the existing
bounded core methods. A zero limit is rejected before consuming the input. The
typed wrapper introduces no background tasks, queues, subscription registries,
global reply maps, or payload clones.

## Errors and Tests

All allocation, ID-generation, serialization, destination validation,
transport, deserialization, acknowledgement, and negative-acknowledgement
failures are propagated as `CatgaError`; public production paths do not panic.
Regression coverage uses the in-memory queue transport to prove typed event
and reliable-event QoS, destination routing, batch completion, decoded
acknowledgement, and decode-failure negative acknowledgement.
