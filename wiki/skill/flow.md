# Flow：补偿流程与工作流

`catga-flow` 提供三种执行模型，**按持久化与等待需求选型**：

| 模型 | 适用 | 持久化 | 等待外部/定时 |
| --- | --- | --- | --- |
| `Flow`（本地补偿） | 进程内短流程，后续步骤失败时逆序补偿 | 否 | 否 |
| `DslFlow<S>` | 进程内、共享可变状态 `S` 的分支/并行/循环流程 | 可选 checkpoint | 否 |
| `FlowDefinition` + `FlowRuntime` | 需重启恢复、等待子结果、定时恢复的 durable 流程 | 是（caller 提供 store） | 是 |

## 1. 本地补偿 `Flow`

步骤逆序补偿：后续步骤失败时，已完成步骤的补偿闭包按相反顺序执行。

```rust,ignore
use catga_flow::Flow;

let result = Flow::new("checkout")
    // 第一个闭包执行步骤；第二个闭包在后续步骤失败时补偿本步骤
    .step(|| async { reserve() }, || async { release() })
    .step(|| async { charge() }, || async { refund() })
    .run()
    .await;

assert!(result.is_success());
assert_eq!(result.completed_steps(), 2);
```

- 共享上下文：`.step_with(context.clone(), |ctx| async move { .. }, |ctx| async move { .. })`。
- 其他入口：`run_until_cancelled(token)`、`run_from(start_step, max_compensations)`。
- `compensating_flow!` 宏让「动作 → 补偿」更易读：

```rust,ignore
use catga_flow::compensating_flow;

let flow = compensating_flow! {
    "reserve-order";
    context = Reservation(Arc::clone(&log));
    steps {
        reserve => release;   // 调用 context 上的 async 方法
    }
};
// 也接受显式函数形式： action_fn => compensate_fn;
```

## 2. `DslFlow<S>`：进程内状态化流程

一个 flow 拥有一个调用方传入的可变状态 `S`，步骤读取/修改它。**只在 caller 保持 future 存活期间运行**；不建模 durable timer 或外部等待。

```rust,ignore
use catga_flow::{DslFlow, dsl_action, dsl_each_action};

struct State { total: u32 }

let flow = DslFlow::new()
    .action(dsl_action!(|state: &mut State| async move {
        state.total += 1;
        Ok::<_, catga_core::CatgaError>(())
    }))
    // 重试 / 超时包装单个 action
    .retry(3, Duration::from_millis(10), dsl_action!(|s: &mut State| async move { .. }))
    .timeout(Duration::from_secs(1), dsl_action!(|s: &mut State| async move { .. }))
    // 条件分支 / 匹配分支 / 并行 / 竞争
    .if_else(condition, then_branch, else_branch)
    .match_on(selector, cases, default_branch)
    .parallel(branches, merge)
    .when_any(branches, merge_winner)
    // 集合迭代（含 continue_on_error / replayable / stream 变体）
    .for_each(|s: &State| items, dsl_each_action!(|s: &mut State, item: u32| async move { .. }));

let mut state = State { total: 0 };
flow.run(&mut state).await?;
```

- 与 CQRS 联动：`.send(mediator, |state| request)` / `.send_into(..)` / `.publish(mediator, |state| event)` / `.remote_send(client, ..)`。
- 共享并发预算：`FlowThrottle::new(limit)?` + `.throttle(throttle, action)`；分支上限 `MAX_DSL_PARALLEL_BRANCHES`。
- 生命周期观测：`with_lifecycle_observer` / `with_lifecycle_hooks`。
- `run_checkpointed(..)` 可为嵌套分支、replayable for_each、parallel 分支持久化 checkpoint，但**仍不含** durable timer/外部等待——需要时上 `FlowDefinition`。
- 辅助宏：`dsl_action!`、`dsl_each_action!` 把自然 async 闭包转成 boxed future。

## 3. Durable flow：`FlowDefinition` + `FlowRuntime`

### 定义

步骤有**稳定名字**，处理器接收输入并返回 `FlowStepOutcome`：

```rust,ignore
use catga_flow::{FlowStepOutcome, flow_definition};

let definition = flow_definition! {
    "checkout";
    "reserve" => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::Advance) };
    "charge"  => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::complete()) };
};
// 等同：FlowDefinition::new("checkout").step("reserve", h1).step("charge", h2)
```

`FlowStepOutcome`：

- `Advance` — 推进到下一步；`complete()` — 流程完成。
- `delay(duration)?` — 定时恢复（`Duration::ZERO` 立即推进，不分配 timer）。
- `wait(WaitCondition)` — 挂起等待子流程/外部结果：`WaitCondition::for_children(flow_id, WaitPolicy::All, child_ids, now, timeout)?`。

### 运行时

```rust,ignore
use std::sync::Arc;
use catga_flow::FlowRuntime;

// store: SuspendedFlowStore（如 SqlSuspendedFlowStore）；scheduler: FlowScheduler（如 SqlFlowScheduler / MemoryFlowScheduler）
let runtime = FlowRuntime::new(store, scheduler, definition, "worker-1")
    .with_stale_after(Duration::from_secs(30));   // owner 心跳/租约时长

// 启动新流程并执行到挂起或终态；data 为序列化后的输入（≤ MAX_FLOW_DATA_BYTES）
let result = runtime.start("order-42", payload_bytes).await?;
// 从持久化的具名步骤恢复（由你的 worker 在调度到期/子结果到达时调用）
runtime.resume("order-42").await?;
runtime.resume_scheduled("order-42", &state_id).await?;   // 防过期调度误恢复
runtime.cancel("order-42").await?;                        // 栅栏后续写；不撤销已发出的外部动作
```

`FlowRuntimeResult`：`is_success()` / `is_failure()` / `is_suspended()` / `is_running()` / `is_compensating()` / `is_cancelled()` / `state()`。注意：`CatgaResult` 为 Ok 不代表业务成功——业务失败要检查 `is_failure()`。

### 到期调度（应用拥有的 worker）

适配器永不自建后台任务；由你的监督任务驱动：

```rust,ignore
use catga_flow::FlowDueService;

// 在应用 spawn 的任务里运行；只有 resume 完成后才确认调度，失败的 claim 会释放重试
due_service.run(cancellation_token).await?;
```

子流程完成结果经 `FlowCompletionAdapter` 或 `FlowRuntime::record_wait_*` 路由回父流程。

### Durable flow 的硬性规则

1. **步骤是 at-least-once**：崩溃恢复可能重放已开始的步骤。支付、邮件等外部副作用必须用稳定 `flow_id + step 名` 派生的**幂等键**。
2. 租约只防止过期 executor 继续写状态，**不能撤销**已被外部系统接受的动作。
3. 有界：`MAX_FLOW_DATA_BYTES`（输入）、`MAX_WAIT_CHILDREN`、`MAX_WAIT_RESULT_BYTES`。
4. 等待子流程时先记录稳定子身份，并把它作为子启动器的幂等键（父恢复可能重复 launch）。
5. 版本栅栏：store 实现保持 `SuspendedFlowStore` 的乐观并发语义，版本不匹配不是覆盖。

### 其他可选组件

- `FlowExecutor`（`FlowHeartbeatOptions` / `FlowRecoveryOptions`）：执行与崩溃恢复辅助。
- `FlowTimeoutService`（`FlowTimeoutOptions` + `TimedOutFlowStore`）：流程级超时扫描，批量有界。
- `StateMachine` / `StateMachineBuilder`：事件驱动的状态机（`StateMachineStore` 持久化）。
