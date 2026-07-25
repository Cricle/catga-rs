# RobustMQ RPC Design

## Goal

Give RobustMQ the same envelope-level request/reply server ergonomics already
provided by the Redis and NATS adapters.

## Design

`MailboxClient` owns an `Arc<MQ9Client>`, making client clones inexpensive and
allowing a request server and its reply handle to share one SDK connection.
`MailboxRequestServer::subscribe` creates one SDK subscription and forwards
decoded envelopes to a bounded Tokio channel. `next` returns `MailboxRequest`.

`MailboxRequest` borrows no server state. It owns the decoded envelope and a
clone of the client handle, and `respond` serializes a response with the
existing Postcard codec before sending it to `Envelope::reply_to`. A missing
reply address is a validation error. The server stores its subscription in an
option and aborts it on drop, so dropped servers do not leave receive tasks.

## Concurrency and failure behavior

The bounded channel applies backpressure to the SDK callback rather than
dropping requests. Each request client call creates its own private mailbox;
there is no global pending-reply map. The existing one-shot handoff remains
per-request and uses only a brief local critical section to claim the first
reply. SDK, codec, timeout, and missing-reply failures are mapped to Catga
errors.

## Testing

Tests remain in `tests/robustmq.rs`. Request/server tests require
`CATGA_ROBUSTMQ_URL`: a plain NATS endpoint is not sufficient because the
mailbox protocol needs its control-plane responder to create private reply
mailboxes. A separate normal-NATS regression test verifies that the missing
control plane fails promptly as a structured transient error instead of leaving
the caller or test task waiting indefinitely.
