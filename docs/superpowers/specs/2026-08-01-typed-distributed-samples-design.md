# Typed Distributed Samples Design

## Goal

Make the distributed Todo application express business operations instead of
wire-envelope construction, while retaining explicit ownership of connections,
codecs, IDs, stores, and lifecycle.

## Breaking API Changes

`catga-core` adds a small `EnvelopePublisher` contract for publish-only
backends. `NatsPublisher` implements this contract without pretending to be a
consumer. `TypedPublisher<T, C>` wraps an application-owned publisher, ID
generator, and payload codec; `MessageTransport` gets a blanket adapter to the
same contract. A discoverable `NatsPublisher::typed(ids, codec)` helper returns
the typed facade without changing NATS lifecycle semantics.

`catga-core` adds `TypedEventStore<S, C>`. It accepts an application-owned
`EventStore`, distributed ID generator, and payload codec. `append_event` builds
the envelope from a typed event and an explicit expected version;
`append_new_event` is a convenience for a new stream and uses expected version
`-1`. Existing low-level `EventStore::append` remains available as an escape
hatch, but the distributed Todo sample no longer uses it directly.

The typed facade derives message type and schema version from `Message`, uses
the codec supplied by the caller, and propagates current transport correlation
context. It performs no connection creation, task spawning, global lookup, or
registry discovery.

## Ownership and Performance

`TypedPublisher` and `TypedEventStore` hold `Arc` references supplied by the
application. Encoding and envelope creation stay on the existing synchronous
hot path, with no additional dynamic registry or per-message configuration
lookup. Applications can inject custom codecs, ID generators, transports, and
stores for tests or production deployments.

## Sample Migration

The distributed Todo API stores a typed command publisher and calls
`publish(&command)`. The worker stores a typed event facade and calls
`append_new_event`. NATS URLs, stream names, consumer names, concurrency, and
shutdown remain process-owned configuration. Shared domain types stay in the
example library; infrastructure wiring moves into focused connection helpers.

## Acceptance Criteria

- `NatsPublisher` can be used anywhere a `MessageTransport` is required.
- A typed publisher can be created from caller-owned NATS publisher, IDs, and codec.
- `TypedEventStore` supports explicit expected versions and new-stream append.
- Unit tests cover metadata, payload encoding, correlation propagation, and conflicts.
- Distributed Todo contains no manual `MemoryPackCodec::encode_payload` or
  `Envelope::new` calls in its API/worker business paths.
- Existing low-level transport and event-store APIs remain usable.
- Formatting, clippy, focused tests, and example compilation pass.
