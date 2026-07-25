# Postcard Request Client Factory Design

## Goal

Port the useful behavior of upstream `IRequestClientFactory` and
`RequestClientFactory`: create typed, destination-bound request clients with a
single validated default timeout and a message-type default destination.

## Chosen API

The factory belongs in `catga-codec-postcard`, because producing a typed
request client requires a serialization format and distributed ID generator.
`PostcardRequestClientFactory<T>` owns shared `Arc<T>` transport and ID
generator handles plus one nonzero default `Duration`.

* `new` validates the default timeout and never opens a connection or spawns a
  task.
* `create::<M>()` selects `type_name::<M>()` as its stable default destination.
* `create_to::<M>(destination)` overrides the destination while retaining the
  factory timeout.
* `create_to_with_timeout::<M>(destination, timeout)` validates an explicit
  timeout for the exceptional per-client case.

Each method returns a normal `CatgaResult`; clients only clone `Arc` handles
and own their compact destination string.  There is no global pending-reply
map, dynamic service locator, or mutable factory state.

## Alternatives Considered

Adding a generic factory to `catga-core` would make core depend on Postcard or
need a codec type-erasure layer, increasing API surface and runtime indirection.
Requiring every caller to invoke `PostcardRequestClient::new` preserves
behavior but omits the upstream's reusable timeout/default-destination policy.
The codec-local factory keeps both policy and codec bounds explicit.

## Testing

Tests use a real in-process `RequestTransport` to prove that the default route
is the request type name, an explicit route and timeout reach the transport,
and a zero timeout returns `ErrorCode::Validation` without a request attempt.
All new public items receive Rustdoc.
