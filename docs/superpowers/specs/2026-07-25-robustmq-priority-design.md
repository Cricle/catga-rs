# RobustMQ Priority Propagation Design

## Goal

Preserve `Envelope` priority through the RobustMQ mailbox request/reply path.
The adapter already accepts an explicit priority for one-way sends, but request
and reply operations currently hard-code the broker's normal priority. This
would silently discard the priority carried by `MessageMetadata`.

## Boundary

`Envelope::metadata().priority()` is the single authoritative Catga priority.
The adapter converts that value through `MailboxPriority`, which intentionally
collapses Catga's `High` and `Critical` values into RobustMQ's highest SDK
priority. No header parsing, process-local cache, or mutable request context is
introduced.

`MailboxClient::request_to` sends the encoded request using the request
envelope's priority. `MailboxRequest::respond` sends the supplied response
using that response envelope's priority. `PostcardCodec::typed_success` and
`PostcardCodec::typed_failure` retain the request priority when constructing
their response metadata, so `respond_value` and `respond_error` preserve it
without an adapter-specific side channel. Callers needing a different reply
priority create a response envelope with explicit
`MessageMetadata::with_priority`.

## Resource and Error Behavior

Priority conversion is a `const` match over a four-value enum and allocates
nothing. Existing bounded reply and request-server channels retain their
current capacity behavior. Mailbox SDK failures continue to map to structured
`ErrorCode::Transient` errors; no new panic-prone operations are needed.

## Validation

Unit tests cover the direct envelope-to-SDK mapping for low, normal, high, and
critical priority. They also establish that typed response metadata inherits a
prioritized request. The focused RobustMQ target compiles and runs without a
server; its live tests remain explicitly environment-gated.
