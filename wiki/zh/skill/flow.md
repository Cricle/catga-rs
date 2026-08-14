# Flow：补偿流程与工作流

`catga-core` 的 `flow` 模块提供两种执行模型，**按持久化与等待需求选型**：

| 模型 | 适用 | 持久化 | 等待外部/定时 |
| --- | --- | --- | --- |
| `DslFlow<S>` | 进程内、共享可变状态 `S` 的分支/并行/循环流程，支持补偿 | 可选 checkpoint | 否 |
| `FlowDefinition` + `FlowRuntime` | 需重启恢复、等待子结果、定时恢复的 durable 流程 | 是（caller 提供 store） | 是 |

## 1. `DslFlow<S>`：进程内补偿流程

`DslFlow` 拥有调用方传入的可变状态 `S`，步骤读取/修改它。**只在 caller 保持 future 存活期间运行**。

### 基础用法

每个步骤就是一个普通闭包 `Fn(&mut S) -> BoxFuture<CatgaResult<()>>`：

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

### 带补偿的步骤

步骤逆序补偿：后续步骤失败时，已完成步骤的补偿闭包按相反顺序执行。

```rust,ignore
use catga_core::flow::DslFlow;

let result = DslFlow::<()>::new()
    .compensate(
        |_| async { Ok(()) },  // 执行步骤
        |_| async { Ok(()) },  // 失败时补偿
    )
    .compensate(
        |_| async { Ok(()) },
        |_| async { Ok(()) },
    )
    .run_compensatable(&mut ())
    .await;

match result {
    Ok(data) => println!("完成 {} 步", data.completed_steps),
    Err(e) => println!("失败，完成 {} 步: {}", e.completed_steps, e.error),
}
```

### 更多 DSL 功能

`DslFlow` 提供丰富的 DSL 功能：

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
        }),
        DslFlow::new().action(|s| Box::pin(async move {
            s.reject();
            Ok(())
        }),
    )
```

### 生命周期钩子

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

## 2. `FlowDefinition` + `FlowRuntime`：Durable 流程

当流程需要持久化、恢复、定时等待时，使用 durable 模型：

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

## 补偿模式对比

| 模式 | 持久化 | 补偿时机 | 适用场景 |
| --- | --- | --- | --- |
| `DslFlow.compensate()` | 否 | 内存中逆序执行 | 进程内事务 |
| `FlowDefinition` + `FlowRuntime` | 是 | 持久化后按需执行 | 分布式事务 |
