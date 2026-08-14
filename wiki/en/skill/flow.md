# Flow: Compensating Flow and Workflow

The `flow` module of `catga-core` provides two execution models, **select based on persistence and waiting needs**:

| Model | Use Case | Persistence | Wait External/Timed |
| --- | --- | --- | --- |
| `DslFlow<S>` | In-process branching/parallel/loop flows with shared mutable state `S`, supports compensation | Optional checkpoint | No |
| `FlowDefinition` + `FlowRuntime` | Durable flows needing restart recovery, waiting for child results, timed recovery | Yes (caller provides store) | Yes |

## 1. `DslFlow<S>`: In-process Compensating Flow

A flow owns a caller-provided mutable state `S`, and steps read/write it. **Runs only while the caller keeps the future alive**.

### Basic Usage

Every step is a plain closure `Fn(&mut S) -> BoxFuture<CatgaResult<()>>`:

```rust,ignore
use catga_core::flow::DslFlow;

let mut flow = DslFlow::<State>::new()
    .action(|state| {
        Box::pin(async move {
            state.balance -= 100;
            Ok(())
        })
    });
```

### Compensating Steps

Steps are compensated in reverse order: when a later step fails, compensation closures of completed steps execute in opposite order.

```rust,ignore
use catga_core::flow::DslFlow;

let result = DslFlow::<()>::new()
    .compensate(
        |_| async { Ok(()) },  // Execute step
        |_| async { Ok(()) },  // Compensate on failure
    )
    .compensate(
        |_| async { Ok(()) },
        |_| async { Ok(()) },
    )
    .run_compensatable(&mut ())
    .await;

match result {
    Ok(data) => println!("Completed {} steps", data.completed_steps),
    Err(e) => println!("Failed after {} steps: {}", e.completed_steps, e.error),
}
```

### More DSL Features

`DslFlow` provides rich DSL features:

```rust,ignore
use catga_core::flow::DslFlow;
use std::time::Duration;

DslFlow::<State>::new()
    .action(|s| Box::pin(async move {
        s.value += 1;
        Ok(())
    }))
    .retry(3, Duration::from_secs(1), |s| Box::pin(async move {
        external_call().await
    }))
    .timeout(Duration::from_secs(5), |s| Box::pin(async move {
        slow_operation().await
    }))
    .if_else(
        |s: &State| s.is_valid,
        DslFlow::new().action(|s| Box::pin(async move {
            s.approve();
            Ok(())
        })),
        DslFlow::new().action(|s| Box::pin(async move {
            s.reject();
            Ok(())
        })),
    )
```

### Lifecycle Hooks

```rust,ignore
use catga_core::flow::{DslFlow, DslFlowLifecycleHooks};

let flow = DslFlow::<State>::new()
    .with_lifecycle_hooks(
        DslFlowLifecycleHooks::new()
            .on_step_succeeded(|state, step_index| {
                Box::pin(async move {
                    tracing::info!("step {} succeeded", step_index);
                    Ok(())
                })
            })
            .on_flow_failed(|state, error| {
                Box::pin(async move {
                    tracing::error!("flow failed: {}", error);
                    Ok(())
                })
            }),
    );
```

## 2. `FlowDefinition` + `FlowRuntime`: Durable Flow

When flows need persistence, recovery, and timed waiting, use the durable model:

```rust,ignore
use catga_core::flow::{FlowDefinition, FlowRuntime};
use catga_core::flow::{FlowScheduler, FlowStore};
use std::sync::Arc;

let definition = FlowDefinition::new("payment")
    .step("reserve", |state| async move {
        state.reserve()?;
        Ok(FlowStepOutcome::Advance)
    })
    .step("charge", |state| async move {
        state.charge()?;
        Ok(FlowStepOutcome::Complete)
    });

let runtime = FlowRuntime::new(store, scheduler, owner);
let result = runtime.start("payment-123", &definition, input).await?;
```

## Compensation Pattern Comparison

| Pattern | Persistence | Compensation Timing | Use Case |
| --- | --- | --- | --- |
| `DslFlow.compensate()` | No | In-memory reverse order | In-process transactions |
| `FlowDefinition` + `FlowRuntime` | Yes | After persistence as needed | Distributed transactions |
