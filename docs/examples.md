# Catga Examples

The examples use the same explicit ownership model as a production service:
your application constructs its handlers, stores, transports, runners, and
shutdown boundaries. Start with the smallest program that matches the problem,
then replace only the boundary that needs durability or distribution.

## 1. Local Building Blocks

These programs run without Docker or credentials.

| Example | Demonstrates | Run | Next step |
| --- | --- | --- | --- |
| [`mediator`](../examples/src/quickstart/mediator.rs) | Typed request/response dispatch with `catga-auto`. | `cargo run -p catga-examples --bin mediator` | Add a policy pipeline or move to a typed mediator hot path. |
| [`typed_mediator`](../examples/src/quickstart/typed_mediator.rs) | Compile-time, zero-allocation command, request, and event dispatch. | `cargo run -p catga-examples --bin typed_mediator` | Use it when the handler set is known at startup. |
| [`memory_transport`](../examples/src/quickstart/memory_transport.rs) | Bounded publish, receive, and acknowledgement. | `cargo run -p catga-examples --bin memory_transport` | Replace the local transport with NATS, Redis, or RobustMQ. |
| [`flow`](../examples/src/quickstart/flow.rs) | A local compensating sequence. | `cargo run -p catga-examples --bin flow` | Use a durable Flow definition and store when a restart must resume work. |

## 2. Runtime Composition

| Example | Demonstrates | Run | Next step |
| --- | --- | --- | --- |
| [`bus_cqrs`](../examples/src/runtime/bus_cqrs.rs) | Type-routed Bus endpoints and command-to-event publication. | `cargo run -p catga-examples --bin bus_cqrs` | Use an application-owned durable transport for cross-process delivery. |
| [`otel_bus`](../examples/src/runtime/otel_bus.rs) | Bus spans, metrics, and structured tracing. | `RUST_LOG=catga=info cargo run -p catga-examples --bin otel_bus` | Export the tracing stream to your OpenTelemetry collector. |

## 3. HTTP Applications

| Example | Demonstrates | Run | Next step |
| --- | --- | --- | --- |
| [`axum_checkout`](../examples/src/web/axum_checkout.rs) | Axum request extraction, error mapping, correlation, and tracing. | `cargo run -p catga-examples --bin axum_checkout` | Attach durable stores and an application-owned worker. |
| [`checkout`](../examples/src/web/checkout.rs) | CQRS validation, a compensating Flow, and acknowledged event delivery. | `cargo run -p catga-examples --bin checkout` | Move the in-memory adapters to durable implementation boundaries. |
| [`order_service`](../examples/src/web/order_service.rs) | A complete in-memory HTTP order service with CQRS, Flow, outbox delivery, and cluster leadership. | `cargo run -p catga-examples --bin order_service` | Replace in-memory adapters with deployment-owned durable stores and transport. |

## 4. Distributed Todo

[`distributed-todo`](../examples/distributed-todo/compose.yaml) is the full
multi-process reference application. It runs an Axum API, a typed competing
consumer worker, and NATS JetStream. Commands are durable, worker delivery and
projection checkpoints are separate, and the API rebuilds its read model from
the event stream after restart.

```bash
docker compose --file examples/distributed-todo/compose.yaml up --build
examples/distributed-todo/verify.sh
```

The two process entry points are
[`todo_api`](../examples/src/distributed/todo_api.rs) and
[`todo_worker`](../examples/src/distributed/todo_worker.rs). The application
explicitly owns NATS connections, the consumer loop, projection replay, and
shutdown. Configure production names and IDs through the documented
`CATGA_TODO_*` environment variables rather than relying on sample defaults.

## Choosing Production Consumption APIs

`process_next` is a useful one-message composition and test helper. A durable
production receive loop should use `CompetingConsumer` so concurrency,
acknowledgement ownership, retry policy, and shutdown are explicit. See the
[transport guide](../skill/transport.md) and
[reliability guide](../skill/reliability.md) before selecting a transport or
delivery policy.
