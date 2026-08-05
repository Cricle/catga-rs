# StateMachine：事件驱动的持久化状态机

`catga-flow` 的 state machine 适合「实体在事件驱动下沿显式状态迁移」的场景（订单、工单、设备影子）。定义不可变且读优化，实例状态由 store 持久化。

## 概念模型

- `S: StateMachineState<K>` — 实例状态（实现 `current_state()` / `set_current_state()`）。
- `K` — 状态键（`Clone + Eq + Hash`），如枚举 `OrderStatus`。
- 事件即 `catga_core::Event`；`Event::categories()` 可声明类别标记，用于类别级迁移。

## 定义（启动期构建）

```rust,ignore
use catga_flow::StateMachine;

let mut builder = StateMachine::<OrderState, OrderStatus>::builder(OrderStatus::Created);

// 事件可创建缺失实例（懒创建）
builder.starts_with::<OrderPlaced, _>(OrderStatus::Created, |event, instance_id| OrderState::new(instance_id));
// 或共享默认初始状态：builder.create_instance_from::<OrderPlaced, _>(..)
// 或 Default 兜底：builder.default_initial_state();

// 配置单个状态：进入/退出动作 + 事件迁移（on::<E>() 开启一条迁移配置）
builder
    .state(OrderStatus::Created)
    .on_enter(|state: &mut OrderState| { /* 同步进入动作 */ Ok(()) })
    .on::<OrderPaid>()                                        // 精确事件迁移
    .when(|state, event| event.amount > 0)                    // 可选守卫
    .execute(|state, event| { /* 迁移动作 */ Ok(()) })        // 或 execute_async
    .transition_to(OrderStatus::Paid)                         // 成功后切换状态
    .on::<OrderUpdated>()                                     // 无 transition_to → 内部迁移
    .execute(|state, event| Ok(()))
    .finish();                                                // 或 .and()

let machine = builder.build();   // 冻结为无锁读的定义，可 Clone 共享
```

- `on::<E>()` — 精确事件迁移；`on_category::<C, _>(extractor)` — 类别迁移（事件需在 `categories()` 声明 `C`，extractor 负责从事件还原出暴露值）。
- 迁移优先级：精确匹配优先于类别匹配；动作变体：`execute`（同步）/ `execute_async`；状态动作变体：`on_enter`/`on_enter_async`/`on_exit`/`on_exit_async`。

## 运行

```rust,ignore
// 内存中驱动（不持久化）
let result = machine.handle(&mut state, &event).await?;
// StateMachineResult：previous / current / transitioned

// 持久化执行：StateMachineExecutor + StateMachineStore（SQL: SqlStateMachineStore，见 stores.md）
// 事件路由：StateMachineEventRouter（按关联把事件投递到实例）
```

- `StateMachineSnapshot` + `encode_state_machine_snapshot` / `decode_state_machine_snapshot`：实例快照的显式编解码。
- store 实现契约：`StateMachineStore`（`SqlStateMachineStore::connect_sqlite(..)` 等，迁移 `migrate()`）。

## 与 Flow 的分工

| 场景 | 选择 |
| --- | --- |
| 线性/分支步骤序列、补偿、定时恢复 | `FlowDefinition` + `FlowRuntime`（见 [flow.md](flow.md)） |
| 事件驱动的长期存活实体状态迁移 | `StateMachine` |
| 进程内一次性状态计算 | `machine.handle` 直接驱动（无 store） |

## 规则

1. 定义在启动期构建并冻结；不要在请求路径上重建。
2. 实例创建策略必须显式（`starts_with` / `create_instance_from` / `default_initial_state`）——缺失实例默认是错误而不是隐式创建。
3. 事件类别是显式声明而非继承：`Event::categories()` 列出它愿意暴露的每个标记。
4. 动作（on_enter/on_exit/迁移）保持幂等：持久化执行路径同样是 at-least-once。
