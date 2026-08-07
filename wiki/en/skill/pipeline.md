# Pipeline: Request Strategies and Built-in Behaviors

Pipelines compose cross-cutting policies (retry, timeout, authorization, validation, etc.) before requests reach handlers. Behaviors are **caller-owned values**: constructed once at startup, their state (e.g., circuit breaker) is shared across requests — there is no global policy state.

## Building Pipelines

```rust,ignore
use std::time::Duration;
use catga_core::{Pipeline, RetryBehavior, TimeoutBehavior, catga_pipeline};

// Returns CatgaResult<Pipeline<M>>; exceeding MAX_PIPELINE_DEPTH returns a validation error
let pipeline: Pipeline<GetOrder> = catga_pipeline!(
    GetOrder;
    RetryBehavior::new(2, Duration::from_millis(10)),
    TimeoutBehavior::new(Duration::from_secs(1)),
)?;

// Pass explicitly at dispatch time
let response = mediator.send_with(request, &pipeline).await?;

// Command counterpart
use catga_core::{CommandPipeline, catga_command_pipeline};
let command_pipeline: CommandPipeline<Archive> = catga_command_pipeline!(Archive;)?;
mediator.send_command_with(command, &command_pipeline).await?;
```

- `catga_pipeline!(Type; b1, b2)` — Request pipeline; `catga_command_pipeline!(Type; ...)` — Command pipeline.
- Macros accept **pre-constructed behavior expressions**, assembled in order via `try_with`; depth limit `MAX_PIPELINE_DEPTH`.
- Custom Behavior: implement `Behavior<M>` (Request) or `CommandBehavior<C>` (Command), call `next.run(message).await` in `handle(&self, message, next)` to continue the chain.

## Built-in Behavior Quick Reference

| Behavior | Construction | Purpose and Notes |
| --- | --- | --- |
| `RetryBehavior` | `RetryBehavior::new(max_retries, initial_delay)` | Bounded exponential backoff retry; retries only errors where `is_retryable()` is true and not `Cancelled`. `with_jitter(..)` specifies `RetryJitter` (production default is full jitter; `RetryJitter::fixed` for deterministic testing) |
| `TimeoutBehavior` | `TimeoutBehavior::new(duration)` | Single attempt timeout, returns `ErrorCode::Timeout` on timeout |
| `ValidationBehavior` | `ValidationBehavior::new(validators)` | Runs `Arc<dyn Validator<M>>` list before handler; failure returns `ErrorCode::Validation`. Also standalone functions `validate_required`/`validate_not_empty`/`validate_max_length`/`validate_min_length`/`validate_min_count`/`validate_positive`/`validate_range` |
| `AuthorizationBehavior` | `AuthorizationBehavior::new()` / `with_policies(..)` | Works with `#[catga(authorize, roles(..), policy(..))]` or `AuthorizedRequest` to check `SecurityClaims`; unauthenticated `Unauthorized`, unauthorized `Forbidden` |
| `TracingBehavior` / `LoggingBehavior` | Default construction | Structured tracing / logging; trace tags require explicit opt-in on messages (`#[catga(trace_tag)]`) |
| `CircuitBreakerBehavior` | `CircuitBreakerBehavior::new(failure_threshold, reset_timeout)?` or `CircuitBreakerOptions::builder(..).build()?` | Circuit breaker; construct once at startup to preserve state across requests |
| `IdempotencyBehavior` | `IdempotencyBehavior::new(store: Arc<dyn IdempotencyStore>, codec)` | Request-side idempotent deduplication (works with `IdempotencyKey`) |
| `InboxBehavior` | `InboxBehavior::new(store: Arc<dyn InboxStore>, codec)` | Consumer-side deduplication (works with `InboxKey`) |
| `OutboxBehavior` | `OutboxBehavior::new(store)` | Persists `OutboxEnvelope` to `OutboxStore` after successful request; published asynchronously by application-owned `OutboxProcessor` |
| `CompensationBehavior` | `CompensationBehavior::new(mediator, factory)` | Publishes compensation message on failure |
| `DeadLetterBehavior` | (works with `DeadLetterEnvelope`/`DeadLetterStore`) | Failed messages enter dead letter |
| `DistributedLockBehavior` | (works with `DistributedLockKey` + `LeaseStore`) | Cross-instance mutual exclusion execution |
| `AutoBatchingBehavior` | `AutoBatchingBehavior::new(BatchOptions)?` returns `(behavior, runner)` | Aggregates concurrent requests into batched dispatch; `runner` driven by application tasks |
| `CorrelationBehavior` / `FaultPublishingBehavior` | `CorrelationBehavior` / `FaultPublishingBehavior::new(publisher)` | Correlation ID propagation / failure event publishing |

## Writing Notes

1. **Order matters**: Outer behaviors execute first. Typical order: authorization → validation → idempotency → retry → timeout (retry wraps timeout so each attempt has its own timeout).
2. **State sharing**: Stateful behaviors like circuit breakers and batchers must be constructed once at startup and shared via `Arc`, not created per request.
3. **Retry does not guarantee safety**: `RetryBehavior` mechanically retries by error code only; side-effect safety remains the responsibility of idempotency keys/Inbox/Outbox.
4. Messages need `Clone` to enter `RetryBehavior` (retry re-dispatches the same message).
