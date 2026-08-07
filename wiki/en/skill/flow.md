# Flow: Compensating Flow and Workflow

`catga-flow` provides three execution models, **select based on persistence and waiting needs**:

| Model | Use Case | Persistence | Wait External/Timed |
| --- | --- | --- | --- |
| `Flow` (local compensating) | In-process short flows, reverse compensation on step failure | No | No |
| `DslFlow<S>` | In-process branching/parallel/loop flows with shared mutable state `S` | Optional checkpoint | No |
| `FlowDefinition` + `FlowRuntime` | Durable flows needing restart recovery, waiting for child results, timed recovery | Yes (caller provides store) | Yes |

## 1. Local Compensating `Flow`

Steps are compensated in reverse order: when a later step fails, compensation closures of completed steps execute in opposite order.

```rust,ignore
use catga_flow::Flow;

let result = Flow::new("checkout")
    // First closure executes step; second closure compensates this step when later steps fail
    .step(|| async { reserve() }, || async { release() })
    .step(|| async { charge() }, || async { refund() })
    .run()
    .await;

assert!(result.is_success());
assert_eq!(result.completed_steps(), 2);
```

- Shared context: `.step_with(context.clone(), |ctx| async move { .. }, |ctx| async move { .. })`.
- Other entry points: `run_until_cancelled(token)`, `run_from(start_step, max_compensations)`.
- The `compensating_flow!` macro makes "action -> compensation" more readable:

```rust,ignore
use catga_flow::compensating_flow;

let flow = compensating_flow! {
    "reserve-order";
    context = Reservation(Arc::clone(&log));
    steps {
        reserve => release;   // Calls async method on context
    }
};
// Also accepts explicit function form: action_fn => compensate_fn;
```

## 2. `DslFlow<S>`: In-process Stateful Flow

A flow owns a caller-provided mutable state `S`, and steps read/write it. **Runs only while the caller keeps the future alive**; does not model durable timers or external waits.

```rust,ignore
use catga_flow::{DslFlow, dsl_action, dsl_each_action};

struct State { total: u32 }

let flow = DslFlow::new()
    .action(dsl_action!(|state: &mut State| async move {
        state.total += 1;
        Ok::<_, catga_core::CatgaError>(())
    }))
    // Retry / timeout wrap single action
    .retry(3, Duration::from_millis(10), dsl_action!(|s: &mut State| async move { .. }))
    .timeout(Duration::from_secs(1), dsl_action!(|s: &mut State| async move { .. }))
    // Conditional branch / match branch / parallel / race
    .if_else(condition, then_branch, else_branch)
    .match_on(selector, cases, default_branch)
    .parallel(branches, merge)
    .when_any(branches, merge_winner)
    // Collection iteration (including continue_on_error / replayable / stream variants)
    .for_each(|s: &State| items, dsl_each_action!(|s: &mut State, item: u32| async move { .. }));

let mut state = State { total: 0 };
flow.run(&mut state).await?;
```

- CQRS integration: `.send(mediator, |state| request)` / `.send_into(..)` / `.publish(mediator, |state| event)` / `.remote_send(client, ..)`.
- Shared concurrency budget: `FlowThrottle::new(limit)?` + `.throttle(throttle, action)`; branch limit `MAX_DSL_PARALLEL_BRANCHES`.
- Lifecycle observation: `with_lifecycle_observer` / `with_lifecycle_hooks`.
- `run_checkpointed(..)` can persist checkpoints for nested branches, replayable for_each, and parallel branches, but still **does not include** durable timer/external waiting — use `FlowDefinition` when needed.
- Helper macros: `dsl_action!`, `dsl_each_action!` convert natural async closures to boxed futures.

## 3. Durable Flow: `FlowDefinition` + `FlowRuntime`

### Definition

Steps have **stable names**, handlers receive input and return `FlowStepOutcome`:

```rust,ignore
use catga_flow::{FlowStepOutcome, flow_definition};

let definition = flow_definition! {
    "checkout";
    "reserve" => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::Advance) };
    "charge"  => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::complete()) };
};
// Equivalent: FlowDefinition::new("checkout").step("reserve", h1).step("charge", h2)
```

`FlowStepOutcome`:

- `Advance` — Proceed to next step; `complete()` — Flow complete.
- `delay(duration)?` — Timed recovery (`Duration::ZERO` advances immediately, no timer allocation).
- `wait(WaitCondition)` — Suspend waiting for child flow/external result: `WaitCondition::for_children(flow_id, WaitPolicy::All, child_ids, now, timeout)?`.

### Runtime

```rust,ignore
use std::sync::Arc;
use catga_flow::FlowRuntime;

// store: SuspendedFlowStore (e.g., SqlSuspendedFlowStore); scheduler: FlowScheduler (e.g., SqlFlowScheduler / MemoryFlowScheduler)
let runtime = FlowRuntime::new(store, scheduler, definition, "worker-1")
    .with_stale_after(Duration::from_secs(30));   // Owner heartbeat/lease duration

// Start new flow and execute to suspended or terminal state; data is serialized input (<= MAX_FLOW_DATA_BYTES)
let result = runtime.start("order-42", payload_bytes).await?;
// Resume from persisted named step (called by your worker when schedule is due or child result arrives)
runtime.resume("order-42").await?;
runtime.resume_scheduled("order-42", &state_id).await?;   // Prevent expired schedule from resuming incorrectly
runtime.cancel("order-42").await?;                        // Barrier subsequent writes; does not revoke already-issued external actions
```

`FlowRuntimeResult`: `is_success()` / `is_failure()` / `is_suspended()` / `is_running()` / `is_compensating()` / `is_cancelled()` / `state()`. Note: `CatgaResult::Ok` does not mean business success — check `is_failure()` for business failures.

### Due Scheduling (Application-owned Worker)

Adapters never create background tasks; your supervisor task drives them:

```rust,ignore
use catga_flow::FlowDueService;

// Run in application-spawned task; schedule is acknowledged only after resume completes; failed claim releases for retry
due_service.run(cancellation_token).await?;
```

Child flow completion results are routed back to parent flow via `FlowCompletionAdapter` or `FlowRuntime::record_wait_*`.

### Hard Rules for Durable Flow

1. **Steps are at-least-once**: Crash recovery may replay started steps. External side effects (payments, emails, etc.) must use **idempotency keys** derived from stable `flow_id + step name`.
2. Leases only prevent expired executors from continuing to write state, **cannot revoke** actions already accepted by external systems.
3. Bounded: `MAX_FLOW_DATA_BYTES` (input), `MAX_WAIT_CHILDREN`, `MAX_WAIT_RESULT_BYTES`.
4. When waiting for child flows, first record stable child identity and use it as the idempotency key for the child launcher (parent recovery may re-launch).
5. Version barrier: store implementations maintain optimistic concurrency semantics of `SuspendedFlowStore`; version mismatch is not an overwrite.

### Other Optional Components

- `FlowExecutor` (`FlowHeartbeatOptions` / `FlowRecoveryOptions`): Execution and crash recovery helpers.
- `FlowTimeoutService` (`FlowTimeoutOptions` + `TimedOutFlowStore`): Flow-level timeout scanning, batched and bounded.
- `StateMachine` / `StateMachineBuilder`: Event-driven state machine (`StateMachineStore` for persistence).
