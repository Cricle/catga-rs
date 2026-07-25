# Envelope Sent-At Design

## Goal

Carry the source transport context's `SentAt` value through Catga's pure-Rust
envelope and Postcard adapters. Upstream Redis and NATS transports serialize
and restore this timestamp; it must not be inferred from a Snowflake ID
because callers can use another ID generator and ID creation is not delivery
creation.

## Representation

`Envelope` owns `sent_at_unix_ms: Option<u64>`, expressed as UTC epoch
milliseconds. The field is separate from `MessageMetadata`: quality of
service, scheduling, priority, and correlation are delivery policy, while
`sent_at` records when this envelope was constructed for transport. An option
preserves both an exact epoch timestamp (`Some(0)`) and the absence required
when decoding legacy payloads.

`Envelope::new` and `Envelope::versioned` capture the current UTC time using
a checked conversion that returns `None` if the platform clock is before the
Unix epoch or exceeds `u64` milliseconds. No construction path panics. The
`with_sent_at` and `with_sent_at_unix_ms` builders allow deterministic replay
and explicit caller overrides; `with_sent_at` rejects pre-epoch and
unrepresentable values with `ErrorCode::Validation`.

## Wire Compatibility

Postcard adds `sent_at_unix_ms` after the existing trailing header field. The
decoder attempts layouts in strict order: current (`headers + sent_at`), prior
header-bearing layout, then the original no-header layout. Each fallback must
consume the complete byte slice. A byte sequence exactly equal to a historical
layout intentionally decodes as historical: without a preceding wire-version
marker, a decoder cannot distinguish it from a suffix truncated exactly at the
old boundary. Inputs that leave trailing bytes are rejected rather than being
silently treated as an older frame.

## Resource and Adapter Behavior

The timestamp is one fixed-width optional scalar and requires no map, string,
worker, or background task. Codec-backed Redis, NATS, memory, and RobustMQ
adapters carry it automatically. Existing header-free envelopes remain
allocation-free; legacy decoded envelopes expose `None` rather than a forged
clock value.

## Validation

Core tests verify automatic timestamp population, exact explicit epoch and
wall-clock values, and invalid explicit time rejection. Codec tests verify
round-trip, prior header-bearing wire compatibility, original legacy wire
compatibility, and rejection of trailing or truncated data. Focused tests,
Clippy, warning-denied Rustdoc, a production no-panic scan, formatting, and
diff validation are required.
