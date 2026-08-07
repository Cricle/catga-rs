# StateMachine: Event-Driven Persistent State Machine

The state machine of `catga-flow` is suitable for scenarios where entities follow explicit state transitions driven by events (orders, tickets, device shadows). Definitions are immutable and read-optimized; instance state is persisted by stores.

## Conceptual Model

- `S: StateMachineState<K>` — Instance state (implements `current_state()` / `set_current_state()`).
- `K` — State key (`Clone + Eq + Hash`), e.g., enum `OrderStatus`.
- Events are `catga_core::Event`; `Event::categories()` can declare category markers used for category-level transitions.

## Definition (Built at Startup)

```rust,ignore
use catga_flow::StateMachine;

let mut builder = StateMachine::<OrderState, OrderStatus>::builder(OrderStatus::Created);

// Events can create missing instances (lazy creation)
builder.starts_with::<OrderPlaced, _>(OrderStatus::Created, |event, instance_id| OrderState::new(instance_id));
// Or shared default initial state: builder.create_instance_from::<OrderPlaced, _>(..)
// Or Default fallback: builder.default_initial_state();

// Configure single state: enter/exit actions + event transitions (on::<E>() starts a transition config)
builder
    .state(OrderStatus::Created)
    .on_enter(|state: &mut OrderState| { /* Synchronous enter action */ Ok(()) })
    .on::<OrderPaid>()                                        // Exact event transition
    .when(|state, event| event.amount > 0)                    // Optional guard
    .execute(|state, event| { /* Transition action */ Ok(()) })        // or execute_async
    .transition_to(OrderStatus::Paid)                         // Switch state on success
    .on::<OrderUpdated>()                                     // No transition_to → internal transition
    .execute(|state, event| Ok(()))
    .finish();                                                // or .and()

let machine = builder.build();   // Frozen lock-free read definition, can be Clone and shared
```

- `on::<E>()` — Exact event transition; `on_category::<C, _>(extractor)` — Category transition (event must declare `C` in `categories()`, extractor restores exposed value from event).
- Transition priority: exact match takes precedence over category match; action variants: `execute` (sync) / `execute_async`; state action variants: `on_enter`/`on_enter_async`/`on_exit`/`on_exit_async`.

## Running

```rust,ignore
// In-memory drive (no persistence)
let result = machine.handle(&mut state, &event).await?;
// StateMachineResult: previous / current / transitioned

// Persistent execution: StateMachineExecutor + StateMachineStore (SQL: SqlStateMachineStore, see stores.md)
// Event routing: StateMachineEventRouter (routes events to instances by correlation)
```

- `StateMachineSnapshot` + `encode_state_machine_snapshot` / `decode_state_machine_snapshot`: Explicit codec for instance snapshots.
- Store implementation contract: `StateMachineStore` (`SqlStateMachineStore::connect_sqlite(..)`, etc., migration via `migrate()`).

## Division with Flow

| Scenario | Choose |
| --- | --- |
| Linear/branched step sequences, compensation, timed recovery | `FlowDefinition` + `FlowRuntime` (see [flow.md](flow.md)) |
| Event-driven long-lived entity state transitions | `StateMachine` |
| In-process one-off state computation | `machine.handle` direct drive (no store) |

## Rules

1. Definition is built and frozen at startup; don't rebuild on the request path.
2. Instance creation strategy must be explicit (`starts_with` / `create_instance_from` / `default_initial_state`) — missing instances default to error, not implicit creation.
3. Event categories are explicitly declared, not inherited: `Event::categories()` lists every marker it is willing to expose.
4. Actions (on_enter/on_exit/transitions) keep idempotent: persistent execution path is also at-least-once.
