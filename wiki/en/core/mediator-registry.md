# Mediator & Registry

## Mediator

Mediator is the core of message dispatch:

```rust
use catga_core::{Mediator, Registry};

let registry = Registry::new();
registry.register_request::<Ping, _>(PingHandler)?;
registry.register_command::<CreateOrder, _>(CreateOrderHandler)?;
registry.register_event::<OrderCreated, _>(OrderProjection)?;

let mediator = Mediator::new(registry);
```

## Registry

Registry manages handler registration:

```rust
use catga_core::Registry;

// Create
let registry = Registry::new();

// Register request handler (unique)
registry.register_request::<GetUser, _>(GetUserHandler)?;

// Register command handler (unique)
registry.register_command::<CreateOrder, _>(CreateOrderHandler)?;

// Register event handlers (multiple)
registry.register_event::<UserCreated, _>(Projection1);
registry.register_event::<UserCreated, _>(Projection2);
```

## MediatorHandle

Dispatch messages:

```rust
use catga_core::MediatorHandle;

// Send request (wait for response)
let user = handle.send(GetUser { id: "123".into() }).await?;

// Send command (no response)
handle.send_command(CreateOrder { item: "widget".into() }).await?;

// Publish event
handle.publish(UserCreated { id: "123".into() }).await?;
```

## Pipeline Behaviors

Add cross-cutting concerns before processing:

```rust
use catga_core::{catga_pipeline, RetryBehavior, TimeoutBehavior};
use std::time::Duration;

let pipeline = catga_pipeline!(
    Request;
    RetryBehavior::new(3, Duration::from_millis(100)),
    TimeoutBehavior::new(Duration::from_secs(5)),
)?;

let mediator = Mediator::new(registry).with_pipeline(pipeline);
```

## catga_handlers! Macro

Simplify multi-handler registration:

```rust
use catga_core::{catga_handlers, request_handler, command_handler, event_handler};

let handlers = catga_handlers! {
    request Ping => ping_handler,
    request GetUser => get_user_handler,
    command CreateOrder => create_order_handler,
    event OrderCreated => order_projection,
}?;

let mediator = Mediator::new(catga_core::Registry::with_handlers(handlers));
```
