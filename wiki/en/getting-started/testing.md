# Testing Guide

## `catga_core::testing`

The testing utilities (spies, capture, harness, assertions) ship with catga-core in the `catga_core::testing` module — no extra crate required.

## HandlerSpy

Wraps a request handler and records every call for assertions:

```rust
use catga_core::testing::HandlerSpy;
use catga_core::Handler;

// Wrap a real handler; or use HandlerSpy::with_action(|msg: Ping| async move { ... }) to skip declaring a handler type
let spy = HandlerSpy::new(PingHandler);

// The spy is itself a Handler: register or call it as usual
spy.handle(Ping).await?;

// Assert
assert_eq!(spy.call_count(), 1);
assert_eq!(spy.last_call(), Some(Ping));
```

## EventHandlerSpy

Event handler testing:

```rust
use catga_core::testing::EventHandlerSpy;
use catga_core::EventHandler;

let spy = EventHandlerSpy::<UserCreated>::new();        // Records only, no side effects
// or EventHandlerSpy::with_handler(real_projection)    records then delegates to the real handler

spy.handle(UserCreated { id: "1".into() }).await?;

// Verify the event was handled
assert_eq!(spy.call_count(), 1);
```

## FlowTestContext

In-memory dependencies for durable Flow runtime tests (isolated suspended-flow store + deterministic scheduler):

```rust
use catga_core::testing::FlowTestContext;

let ctx = FlowTestContext::new();

// Clone out and construct the FlowRuntime under test directly
let suspended = ctx.suspended_flows();  // Arc<MemorySuspendedFlows>
let scheduler = ctx.scheduler();        // Arc<MemoryFlowScheduler>
```

## Integration Tests

`CatgaTestHarness` builds a typed, in-process test environment: registration stays separate from execution, and messages are captured automatically:

```rust
use catga_core::testing::CatgaTestHarness;
use catga_core::CatgaResult;

#[tokio::test]
async fn test_order_workflow() -> CatgaResult<()> {
    let mut harness = CatgaTestHarness::new()?;
    harness.register_captured_request::<CreateOrder, _>(CreateOrderHandler)?;
    harness.capture_event::<OrderCreated>(); // Captures every publication (no application handler required)

    let running = harness.start();

    // Execute command
    running.mediator().send(CreateOrder { /* ... */ }).await?;

    // Assert captured messages
    assert_eq!(running.consumed_of::<CreateOrder>().len(), 1);
    assert_eq!(running.published_of::<OrderCreated>().len(), 1);

    Ok(())
}
```

## Message Capture

`MessageCapture` is a concurrently safe message recorder for custom assertions:

```rust
use catga_core::testing::MessageCapture;

let capture = MessageCapture::<UserCreated>::default();

capture.record_published(UserCreated { id: "1".into() });

assert_eq!(capture.published().len(), 1);
assert!(capture.consumed().is_empty());
capture.clear();
```

## Assertion Helpers

`assert_success` / `assert_failure` / `assert_value` / `assert_error_code` are plain functions (not macros):

```rust
use catga_core::testing::{assert_error_code, assert_success};
use catga_core::ErrorCode;

let value = assert_success(handler.handle(msg).await);                          // Success: returns T
let err = assert_error_code(handler.handle(msg).await, ErrorCode::Conflict);    // Failure with matching code: returns CatgaError
```
