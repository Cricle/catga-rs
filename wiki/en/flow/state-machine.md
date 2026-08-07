# State Machine

## FlowState Trait

Defines workflow states:

```rust
use catga_flow::{FlowState, Event, Transition};

#[derive(FlowState)]
enum PaymentFlow {
    #[state(initial)]
    Pending,

    #[state(accepts = [ProcessPayment])]
    Processing,

    #[state(final)]
    Completed,

    #[state(compensate = cancel_payment)]
    Failed,
}

impl PaymentFlow {
    fn transitions() -> Vec<Transition> {
        vec![
            Transition::new(Self::Pending, Self::Processing, |e: &ProcessPayment| {
                e.payment_id.clone()
            }),
            Transition::new(Self::Processing, Self::Completed, |_: &_| ()),
            Transition::new(Self::Processing, Self::Failed, |e: &PaymentFailed| {
                e.reason.clone()
            }),
        ]
    }

    async fn cancel_payment(ctx: &PaymentContext) -> CatgaResult<()> {
        payment_gateway.refund(&ctx.payment_id).await
    }
}
```

## Event Handling

```rust
use catga_flow::{FlowHandler, FlowEvent};

impl FlowHandler<PaymentFlow> for PaymentFlowHandler {
    async fn handle(&self, event: FlowEvent<PaymentFlow>) -> CatgaResult<()> {
        match event {
            FlowEvent::Transition { from, to, context } => {
                log::info!("Payment flow: {:?} -> {:?}", from, to);
                // Execute business logic
            }
            FlowEvent::Compensate { state, error } => {
                log::error!("Compensating: {:?} due to {:?}", state, error);
                // Invoke compensation logic
            }
        }
        Ok(())
    }
}
```

## Persistence

State is automatically persisted to the store:

```rust
use catga_flow::{FlowStore, FlowSnapshot};

let store = FlowStore::new(redis_client);

let mut flow = PaymentFlow::restore("payment-123", &store).await?;

// Process events
flow.process(ProcessPayment { payment_id: "pay-456".into() }).await?;

// State is automatically saved
flow.save(&store).await?;
```
