# NATS Deduplication Observability Design

## Goal

Expose the broker-confirmed duplicate suppression signal for ExactlyOnce NATS
publishes while retaining the Rust adapter's bounded-memory design.

## Mapping

The upstream implementation maintains an in-process, time-expiring map of
received identities and reports both local duplicate drops and cache evictions.
The Rust adapter instead supplies `Nats-Msg-Id` to JetStream and reads the
broker's `PublishAck::duplicate` result. This removes the process-local map,
its cleanup scan, and its eviction path while making the signal authoritative
across clients.

Each broker acknowledgement whose `duplicate` field is true increments
`catga.nats.dedup.drops`. The metric has no labels. It is emitted for both the
primary JetStream subject and explicitly provisioned destination subjects.
The acknowledgement remains a successful publish: duplicate suppression is an
ExactlyOnce success, not a transport failure. No `catga.nats.dedup.evictions`
metric exists because the Rust adapter owns no deduplication cache; JetStream
owns retention-window eviction.

## Tests

A NATS crate unit test passes `false` and `true` acknowledgement flags through
the private helper using a local metrics recorder. It proves only `true`
increments the counter. Existing endpoint-gated integration coverage remains
responsible for verifying a live JetStream acknowledgement when a server URL
is supplied.
