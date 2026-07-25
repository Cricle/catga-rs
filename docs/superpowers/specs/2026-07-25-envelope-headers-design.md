# Envelope Headers Design

## Goal

Map the source `TransportContext.Metadata` contract to an owned Rust envelope
representation that survives durable transport encoding without adding storage
or allocation to envelopes that have no headers.

## Representation

`EnvelopeHeader` owns one validated key/value pair as `Box<str>`. `Envelope`
stores `Option<Arc<[EnvelopeHeader]>>`: `None` is the common no-header case,
and cloning an envelope with headers increments one atomic reference count
instead of copying every string. Read APIs provide allocation-free iteration
and key lookup; header ordering is retained for deterministic wire output.

`Envelope::with_headers` validates all input before assigning it. Keys must be
nonblank and unique, and a message may contain at most 64 headers totaling at
most 8 KiB of UTF-8 key/value bytes. These fixed limits turn source's mutable,
unbounded dictionary into a reliable Rust transport boundary without hidden
per-message maps or adversarial allocation growth. Invalid input returns
`ErrorCode::Validation`.

## Wire Compatibility

Postcard adds a trailing `headers: Vec<HeaderWire>` field with Serde's default
for absent data. Existing Postcard payloads therefore decode as an envelope
with no headers. Encoding is deterministic in caller-provided order. Decoding
uses `TryFrom<EnvelopeWire>` so malformed remote headers are rejected as
structured validation errors instead of bypassing the core limits.

Memory transport clones envelopes directly. Redis, NATS, and RobustMQ already
serialize envelopes through `EnvelopeCodec` and consequently carry headers
without an adapter-specific header protocol or an extra broker allocation.

## Typed Transport Surface

`PostcardTransport` exposes explicit `publish_with_headers` and
`send_to_with_headers` operations in addition to ordinary and event calls.
They accept a shared `EnvelopeHeaders` value, clone only its `Arc`, and attach
it before delegating to the existing bounded transport methods. Ordinary
methods remain allocation-identical and use no headers. Further batch context
methods can delegate to these operations without altering backend contracts.

## Inbound Propagation

The source transport invokes handlers inside an ambient `TransportContext`.
Rust exposes the same inheritance explicitly through
`PostcardDelivery::with_transport_context`: the caller keeps the delivery and
chooses acknowledgement timing while the supplied future sees the received
correlation ID, `Copy` priority, and shared header slice. Nested
`PostcardTransport` calls and `PostcardScheduledOutbox` insertion inherit that
state without copying a payload or spawning a task. `PostcardRequestClient`
uses the same context when building a request/reply envelope, including the
request's declared schema version. When an outgoing call also supplies
headers, its keys override inherited keys in place and its new keys append
deterministically; the merged result is revalidated against the existing
bounds.

## Validation

Core tests prove no-header envelopes expose an empty iterator, headers are
immutable and cheaply cloneable, invalid duplicates and resource limits fail,
and header lookup preserves stable values. Codec tests prove full round-trip,
legacy missing-field decode, invalid decoded headers, and typed publication.
Formatting, focused tests, Clippy, Rustdoc, production no-panic search, and
the excluded-adapter audit are required before the slice is considered done.
