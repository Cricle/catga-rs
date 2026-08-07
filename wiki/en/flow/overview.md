# Flow Workflow

## Overview

Catga Flow provides persistent state machine and workflow capabilities.

## Core Concepts

```
┌─────────┐   trigger    ┌─────────┐   complete   ┌─────────┐
│  idle   │────────────▶│ running │────────────▶│ done    │
└─────────┘             └─────────┘             └─────────┘
                              │
                              │ compensate
                              ▼
                        ┌─────────┐
                        │failed   │
                        └─────────┘
```

## Defining a Workflow

```rust
use catga_flow::{Flow, State, Trigger};

#[derive(State)]
enum OrderWorkflow {
    #[state(initial)]
    Idle,

    #[state(accepts = [CreateOrder])]
    Pending,

    #[state(accepts = [ApproveOrder, RejectOrder])]
    Processing,

    #[state(final)]
    Completed,

    #[state(compensate = refund)]
    Failed,
}

impl Flow for OrderWorkflow {
    type Context = OrderContext;

    fn trigger(event: &Event) -> Option<Self::Trigger> {
        match event {
            Event::CreateOrder(o) => Some(Trigger::Transition {
                from: Self::Idle,
                to: Self::Pending,
                context: OrderContext::new(o),
            }),
            // ...
        }
    }
}
```

## Compensation Transactions

```rust
impl OrderWorkflow {
    async fn refund(ctx: &OrderContext) -> CatgaResult<()> {
        // Roll back executed external operations
        payment_gateway.refund(&ctx.payment_id).await?;
        inventory.restore(&ctx.items).await?;
        Ok(())
    }
}
```

## Executing a Workflow

```rust
use catga_flow::FlowRuntime;

let runtime = FlowRuntime::new(store);

runtime.trigger(CreateOrder { items: vec![...] }).await?;
```
