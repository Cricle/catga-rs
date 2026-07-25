# Destination Transport Design

## Goal

Port the `SendAsync`, destination-specific `SubscribeAsync`, and
`SendBatchAsync` portions of upstream `IMessageTransport` without reducing a
durable queue to best-effort topic publication.  The public Rust API must be
small, fallible, documented, and independent of any particular broker.

## Chosen Design

`catga-core` adds a validated `Destination` value and a separate
`DestinationTransport` trait.  It is deliberately separate from
`MessageTransport`: `publish` remains a transport's configured topic
operation, while `send_to` means delivery to a named, durable destination.
The trait owns input envelopes, has bounded streaming batch dispatch, and
uses pull-based `receive_from` returning `Delivery`.  Pull delivery matches
the existing Rust acknowledgement model and avoids spawning unbounded
callback tasks behind a `subscribe` convenience method.

`Destination::parse` rejects empty or whitespace-only names and preserves a
compact `Box<str>` after construction.  Invalid names return
`ErrorCode::Validation`; stopped transports return `ErrorCode::Unavailable`;
broker errors remain normal `CatgaError`s.  No public operation panics for
invalid user input.

## Adapter Semantics

* `MemoryTransport` holds a small registry of named bounded queues.  Queues
  are created explicitly by `declare_destination`; sending to an unknown name
  returns `NotFound`.  This makes unit tests deterministic and prevents an
  accidental typo from allocating an unbounded set of queues.
* `RedisTransport` maps a destination to `stream:<destination>` and consumes
  it through the configured consumer group and consumer.  Group provisioning
  is idempotent and uses `MKSTREAM`; a destination send is an `XADD`, never
  Pub/Sub.
* `NatsTransport` requires a registered `NatsDestination` containing a
  subject, JetStream stream, and durable consumer.  Sends use JetStream and
  receives use that destination's pull consumer.  It never synthesizes stream
  or durable names from arbitrary user input.

## Resource And Concurrency Properties

Batch sending uses `futures::stream::buffer_unordered`; active publish
futures and their message state are bounded by the caller-selected limit.
Items are moved, not cloned, and every item is attempted before the first
observed error is returned.  Destination registries use short-lived shardable
maps; no global async mutex covers network I/O.

## Testing And Documentation

Tests begin at the core contract and Memory backend, including invalid names,
unknown destinations, bounded queue backpressure, acknowledgement drain, and
batch error draining.  Redis and NATS integration tests use the existing
environment-variable skip convention and prove durable round trips only when
the corresponding broker is available.  Every new public item receives
Rustdoc explaining ownership, acknowledgement, errors, and delivery
guarantees.
