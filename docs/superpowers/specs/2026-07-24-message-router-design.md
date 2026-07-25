# Message Router Design

## Goal

Port upstream `IMessageRouter` and `MessageRouter`: select a destination from
transport headers with ordered rules and an optional fallback destination.

## Rust API

`catga-core` exposes `MessageRouter`.  Construction and `add_route` validate
all destination names through `Destination::parse`; empty header keys and
values return `ErrorCode::Validation`.  `resolve(&[(&str, &str)])` returns
`Option<&Destination>`, borrowing the configured rule instead of allocating or
cloning.

Rules retain insertion order.  The first `(header key, header value)` match
wins; if none matches, `resolve` returns the configured default.  This is the
same observable precedence as upstream.

## Memory And Transport Boundary

`Envelope` deliberately keeps fixed, compact metadata and does not gain an
unbounded header map.  Callers that need header routing provide a borrowed
slice at the routing boundary, normally from their HTTP, broker, or RPC
adapter.  For the small rule and header sets expected at this boundary, a
linear scan avoids hashing, allocation, and synchronization.  The returned
destination can be passed directly to `DestinationTransport::send_to`.

## Tests

Tests prove validation, ordered first-match behavior, fallback behavior, and
that a route result borrows rather than clones the configured destination.
All public types and methods receive Rustdoc.
