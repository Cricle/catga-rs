# RabbitMQ Transport Design

## Goal

Add a pure-Rust RabbitMQ adapter that gives Catga envelopes the source library's
durable topic routing, bounded at-least-once delivery, explicit acknowledgement,
native request/reply, priority, delayed delivery, and competing-consumer behavior.

## Scope and source boundary

The reference is `upstream-catga/src/Catga.Transport.RabbitMQ` and its transport
and integration tests.  The Rust API is intentionally idiomatic rather than a
translation of .NET dependency injection or reflection APIs.  It must cover:

- topic exchanges, explicit destination routing, deterministic prefix handling,
  durable/auto-delete queues, and publisher confirmation;
- broker QoS as a hard upper bound on unacknowledged deliveries;
- `Delivery`-owned ack and nack with no duplicate acknowledgement path;
- native RPC through one exclusive, auto-delete reply queue per request;
- TTL, priority queue declaration and clamping, `x-delay` delayed exchanges,
  reply addresses, and W3C trace propagation in AMQP properties and headers;
- concurrent competing consumers with bounded handler work and poison-message
  rejection after the configured number of attempts.

The adapter does not introduce a .NET-style service provider.  It does not
depend on Redis, NATS, or an external serialization runtime.  Existing
`Envelope`, `MessageTransport`, `RequestTransport`, `PostcardCodec`, and typed
Postcard request clients remain the cross-adapter contracts.

## Architecture

`catga-rabbitmq` depends on `lapin`, Tokio, `catga-core`, and
`catga-codec-postcard`. `RabbitMqTransport::connect` opens one AMQP connection,
declares the configured exchange, and opens a publish channel with confirms
enabled.  The transport serializes an `Envelope` exactly once with
`PostcardCodec`; the AMQP body is that wire value.  This preserves Catga's
message ID, correlation ID, type, payload, metadata, and reply address without
duplicating those fields in an adapter-specific payload.

`publish_to` resolves a destination to a normalized routing key, converts only
broker-native delivery fields to AMQP properties, publishes with mandatory
confirmation, and maps broker failures to `ErrorCode::Transient`.  A `Consumer`
owns its AMQP channel and delivery stream.  It sets QoS before consuming and
turns every broker delivery into `catga_core::Delivery`; its acknowledger owns
the `lapin::message::Delivery` token so ack or nack can execute at most once.
The consumer stream never holds a channel/map lock while user code awaits.

`RabbitMqRequestClient` implements `RequestTransport`.  Each call opens an
exclusive broker-generated reply queue and a dedicated consumer, publishes the
request with that queue in both AMQP `reply_to` and the envelope reply address,
then waits for the matching correlation ID under `tokio::time::timeout`.  The
queue/consumer are dropped on success, timeout, cancellation, or decode error;
there is no global pending-reply map or cross-request mutex.

`RabbitMqCompetingConsumer` is an opt-in runner around one shared queue.  It
uses QoS plus a `Semaphore` sized to `max_concurrency`, acknowledges after a
successful handler, nacks for redelivery below the attempt limit, and rejects
without requeue at the limit.  A pluggable async dead-letter callback runs
before the final reject and a callback failure keeps the delivery retryable.

## Public API

`RabbitMqConfig` uses owned `Box<str>` fields and explicit defaults: AMQP URI,
exchange, `topic` exchange type, `catga.` prefix, durable exchange/queues,
prefetch, optional TTL, optional max priority, optional delayed exchange, and
default request timeout.  Its validation rejects empty URI/exchange, zero
prefetch, and invalid timeout before a connection is opened.

`RabbitMqTransport::connect(config)` returns an initialized transport.  It
implements `MessageTransport` and exposes `publish_to(envelope, destination)`
for source-equivalent explicit routing. `subscribe(destination)` creates a
bounded consumer.  `RabbitMqRequestClient::new(transport)` and `.typed(...)`
parallel the NATS request client API, so existing `PostcardRequestClient` works
unchanged. `RabbitMqCompetingConsumer::connect` accepts a queue/routing-key
pair and `CompetingConsumerConfig` with group name, consumer name, concurrency,
and maximum delivery attempts.

## Broker metadata

The envelope remains authoritative.  AMQP properties mirror the fields that
other AMQP producers and RabbitMQ itself need: correlation ID, message ID,
type, reply-to, persistence mode, expiration, timestamp, and priority.
`x-delay`, `traceparent`, `tracestate`, and non-reserved metadata use headers.
Priority is clamped to the declared maximum.  A delayed exchange is declared
as `x-delayed-message` with `x-delayed-type` set to the configured underlying
exchange type.  On receive, native priority and headers are merged into
envelope metadata only when the envelope did not already provide the same key.

## Failure, concurrency, and memory behavior

All client/library errors become structured `CatgaError` values; invalid local
configuration is `Validation`, elapsed RPC timeouts are `Timeout`, and AMQP
connection/channel/publisher failures are `Transient`.  The hot publish path
allocates one encoded `Vec<u8>` for the envelope and moves it into the AMQP
call.  Receive decodes directly from the broker bytes.  Broker prefetch and
the competing-consumer semaphore bound memory and handler concurrency.  No
unbounded task list, global reply registry, or mutex held across `.await` is
permitted.

## Tests and acceptance criteria

Test-first Rust coverage in `tests/rabbitmq.rs` will prove configuration
validation, routing-prefix normalization, AMQP property construction, priority
clamping, delayed-exchange arguments, and request input validation without a
broker.  When `CATGA_RABBITMQ_URL` is set, integration tests prove publish /
receive / ack, nack redelivery, custom destination routing, publisher confirms,
metadata round-trip, RPC success and timeout cleanup, queue priority, delayed
delivery, and competing-consumer exactly-once processing per delivery.
Absence of the environment variable produces an explicit skip message, not a
false claim of broker verification.  Completion requires formatting, Clippy
with warnings denied, full workspace tests, and the RabbitMQ test suite.
