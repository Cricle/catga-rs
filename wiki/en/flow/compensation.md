# Compensation Transactions

## Overview

When a workflow fails, compensation transactions revoke the executed side effects.

## Compensation Pattern

```
Normal Flow:                   Failure Compensation:
Payment → Reserve → Ship  →  Done
         ↓                    ↓
       Error              Refund → Undo Reserve → Done
```

## Defining Compensation

```rust
use catga_flow::Compensable;

#[derive(FlowState)]
enum OrderFlow {
    #[state(initial)]
    Created,

    #[state(compensate = reserve_inventory)]
    InventoryReserved,

    #[state(compensate = process_payment)]
    PaymentProcessed,

    #[state(compensate = cancel_shipment)]
    Shipped,

    #[state(final)]
    Completed,
}

impl OrderFlow {
    async fn reserve_inventory(ctx: &OrderContext) -> CatgaResult<()> {
        // Release reservation
        inventory.release(&ctx.items).await
    }

    async fn process_payment(ctx: &OrderContext) -> CatgaResult<()> {
        // Refund
        payment.refund(&ctx.payment_id).await
    }

    async fn cancel_shipment(ctx: &OrderContext) -> CatgaResult<()> {
        // Cancel shipment
        shipping.cancel(&ctx.tracking_id).await
    }
}
```

## Saga Pattern

```rust
use catga_flow::{Saga, SagaStep};

struct OrderSaga;

impl Saga for OrderSaga {
    type Context = OrderContext;

    fn steps() -> Vec<SagaStep<Self>> {
        vec![
            SagaStep::new(Self::reserve_inventory)
                .forward(|ctx| async { inventory.reserve(&ctx.items).await })
                .compensation(Self::release_inventory),

            SagaStep::new(Self::process_payment)
                .forward(|ctx| async { payment.charge(&ctx.payment).await })
                .compensation(Self::refund_payment),

            SagaStep::new(Self::schedule_shipment)
                .forward(|ctx| async { shipping.schedule(&ctx.address).await })
                .compensation(Self::cancel_shipment),
        ]
    }
}
```

## Automatic Compensation

```rust
use catga_flow::SagaExecutor;

let executor = SagaExecutor::new(store);

match executor.execute(OrderSaga, context).await {
    Ok(_) => println!("Order completed"),
    Err(FlowError::Compensated) => println!("Order rolled back"),
    Err(e) => println!("Execution failed: {:?}", e),
}
```

## Compensation Order

Compensations execute in reverse order:

```rust
#[test]
fn compensations_run_in_reverse_order() {
    let mut saga = TestSaga::new();

    saga.add_step("step1").forward(()).compensate(|| {
        called.push("step1_comp");
    });
    saga.add_step("step2").forward(()).compensate(|| {
        called.push("step2_comp");
    });
    saga.add_step("step3").forward(()).compensate(|| {
        called.push("step3_comp");
    });

    saga.fail_at("step3");

    saga.execute().expect_err("saga failed");

    assert_eq!(called, vec!["step3_comp", "step2_comp", "step1_comp"]);
}
```
