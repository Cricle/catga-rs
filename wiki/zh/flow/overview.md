# Flow 工作流

## 概述

Catga Flow 提供持久化状态机和工作流能力。

## 核心概念

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

## 定义工作流

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

## 补偿事务

```rust
impl OrderWorkflow {
    async fn refund(ctx: &OrderContext) -> CatgaResult<()> {
        // 回滚已执行的外部操作
        payment_gateway.refund(&ctx.payment_id).await?;
        inventory.restore(&ctx.items).await?;
        Ok(())
    }
}
```

## 执行工作流

```rust
use catga_flow::FlowRuntime;

let runtime = FlowRuntime::new(store);

runtime.trigger(CreateOrder { items: vec![...] }).await?;
```
