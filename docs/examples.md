# Examples Guide

Examples are in the [`examples/`](examples) directory. Each example is a runnable
binary demonstrating a specific aspect of Catga.

## Quickstart Examples

| Example | File | Description |
| --- | --- | --- |
| Mediator | [`src/quickstart/mediator.rs`](examples/src/quickstart/mediator.rs) | Basic request/response with Mediator |
| Typed Mediator | [`src/quickstart/typed_mediator.rs`](examples/src/quickstart/typed_mediator.rs) | Compile-time dispatch without Box |
| Plain Handler | [`src/quickstart/plain_handler.rs`](examples/src/quickstart/plain_handler.rs) | Minimal handler using async fn |
| Simple Handler | [`src/quickstart/simple_handler.rs`](examples/src/quickstart/simple_handler.rs) | Handler struct pattern |
| Memory Transport | [`src/quickstart/memory_transport.rs`](examples/src/quickstart/memory_transport.rs) | In-memory message passing |
| Flow | [`src/quickstart/flow.rs`](examples/src/quickstart/flow.rs) | State-machine workflow with compensation |

## Runtime Examples

| Example | File | Description |
| --- | --- | --- |
| Simple Bus | [`src/runtime/simple_bus.rs`](examples/src/runtime/simple_bus.rs) | Bus with typed publishers |
| Bus CQRS | [`src/runtime/bus_cqrs.rs`](examples/src/runtime/bus_cqrs.rs) | Request/Command/Event on one bus |
| OTEL Bus | [`src/runtime/otel_bus.rs`](examples/src/runtime/otel_bus.rs) | OpenTelemetry tracing integration |

## Web Examples

| Example | File | Description |
| --- | --- | --- |
| Checkout | [`src/web/checkout.rs`](examples/src/web/checkout.rs) | HTTP endpoint with CQRS |
| Axum Checkout | [`src/web/axum_checkout.rs`](examples/src/web/axum_checkout.rs) | Full Axum integration |
| Order Service | [`src/web/order_service.rs`](examples/src/web/order_service.rs) | Complete order management API |

## Running Examples

```bash
# List all available example binaries
cargo run -p catga-examples --bin -- --list

# Run the mediator example
cargo run -p catga-examples --bin mediator

# Run the flow example
cargo run -p catga-examples --bin flow

# Run the bus example
cargo run -p catga-examples --bin bus_cqrs
```

## Distributed Todo Application

The [`examples/distributed-todo/`](examples/distributed-todo) directory contains
a complete distributed application with:

- **API**: Axum HTTP server accepting commands
- **Consumer**: Competing consumer processing commands
- **Projection**: Rebuilds read model from events
- **NATS JetStream**: Message transport with durability

### Run with Docker Compose

```bash
# Start all services
docker compose --file examples/distributed-todo/compose.yaml up --build

# Verify the application
examples/distributed-todo/verify.sh

# Clean up
docker compose --file examples/distributed-todo/compose.yaml down
```

### Environment Variables

| Variable | Default | Description |
| --- | --- | --- |
| `CATGA_TODO_NATS_URL` | `nats://localhost:4222` | NATS server URL |
| `CATGA_TODO_STREAM` | `TODOS` | JetStream stream name |
| `CATGA_TODO_CONSUMER` | `TODO_WORKER` | Consumer group name |

## Choosing an Example

| If you want... | Start with... |
| --- | --- |
| Understand the mediator pattern | `mediator` |
| High-performance typed dispatch | `typed_mediator` |
| Minimal boilerplate | `plain_handler` |
| Stateful workflows | `flow` |
| Multiple message types on one bus | `bus_cqrs` |
| HTTP integration | `checkout` or `axum_checkout` |
| Full distributed system | `distributed-todo` |
