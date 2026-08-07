# Testing Guide

## catga-testing

The testing utility library provides mocking and assertion capabilities.

## HandlerSpy

Intercept messages for testing:

```rust
use catga_core::Message;
use catga_testing::HandlerSpy;

let spy = HandlerSpy::<Ping>::default();

// Mock handler
let handler = spy.as_handler();

// Send message
handler.handle(Ping).await?;

// Assert
assert_eq!(spy.count(), 1);
assert_eq!(spy.get(0), Some(&Ping));
```

## EventHandlerSpy

Event handler testing:

```rust
use catga_testing::EventHandlerSpy;

let spy = EventHandlerSpy::<UserCreated>::new();

let projection = spy.delegating_handler(real_projection);

projection.handle(UserCreated { id: "1".into() }).await?;

// Verify event was handled
assert!(spy.contains(&UserCreated { id: "1".into() }));
```

## FlowTestContext

Flow workflow testing:

```rust
use catga_flow_testing::FlowTestContext;

let ctx = FlowTestContext::new();
let flow = TestFlow::new(&ctx);

// Trigger event
flow.trigger(CreateOrder { items: vec![] }).await?;

// Assert state
assert_eq!(flow.state(), TestFlow::Processing);
```

## Integration Tests

```rust
#[tokio::test]
async fn test_order_workflow() {
    // Initialize test environment
    let store = InMemoryEventStore::new();
    let transport = MemoryTransport::new();

    let app = AutoApp::builder()
        .with_store(store.clone())
        .with_transport(transport.clone())
        .handler(order_handler)?
        .build()?;

    // Execute command
    app.handle()
        .send_command(CreateOrder { items: vec![item] })
        .await?;

    // Verify result
    let order = store.find::<Order>("order-1").await?;
    assert_eq!(order.status(), OrderStatus::Created);
}
```

## Mock Messages

```rust
use catga_testing::MockMessage;

let msg = MockMessage::<UserCreated>::new()
    .with_id("test-id")
    .withcorrelation_id("corr-id");

assert_eq!(msg.id(), "test-id");
```

## Assertion Helpers

```rust
use catga_testing::{assert_success, assert_failure, assert_error_code};

let result = handler.handle(msg).await?;

assert_success!(result);
assert_failure!(result, ErrorCode::Validation);
assert_error_code!(result, ErrorCode::Conflict);
```
