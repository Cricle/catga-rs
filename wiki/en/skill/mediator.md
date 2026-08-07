# Mediator: Messages, Handlers, and Dispatch

The in-process CQRS dispatch core of `catga-core`: `Message` → `Registry` (registered at startup) → `Mediator` (dispatched at runtime).

## 1. Message Traits

```rust,ignore
// Base trait for all messages: Send + Sync + 'static
pub trait Message: Send + Sync + 'static {
    fn message_type(&self) -> &'static str { std::any::type_name::<Self>() } // Default stable type name
    fn schema_version(&self) -> u32 { 1 }                                    // Override for evolving messages
    fn priority(&self) -> MessagePriority { MessagePriority::Normal }
}

pub trait Request: Message { type Response: Send + 'static; } // Has response
pub trait Command: Message {}                                  // No response
pub trait Event: Message + Clone {}                            // Fan-out, must be Clone
```

Two declaration styles:

```rust,ignore
// Style 1: Hand-written impl (no dependencies, most common)
struct GetBalance { account_id: u64 }
impl Message for GetBalance {}
impl Request for GetBalance { type Response = u64; }

#[derive(Clone)]
struct TransferCompleted { /* ... */ }
impl Message for TransferCompleted {}
impl Event for TransferCompleted {}

// Style 2: Derive (catga_core re-exports catga_macros Message derive)
use catga_core::Message;
#[derive(Message)]
#[catga(priority = high)]              // Optional: static transport priority low/normal/high/critical
struct RebuildSearchIndex {
    #[catga(trace_tag)]                // Optional: structured tracing tag (explicit opt-in, privacy-safe)
    tenant: String,
}
// Other derive attributes:
// #[catga(schema_version = 2)]
// #[catga(batch_key = "field_name")]        → Implements BatchKeyProvider
// #[catga(authorize, roles("admin"), policy("p"))] → Implements AuthorizedRequest
// #[catga(trace_tag = "name")] / #[catga(trace_tags(prefix = "x.", include = [...], exclude = [...]))]
```

## 2. Handler Traits

All handlers are implemented with `#[async_trait]`, `handle` returns `CatgaResult`:

```rust,ignore
use async_trait::async_trait;
use catga_core::{CatgaResult, CommandHandler, EventHandler, Handler};

struct BalanceHandler;
#[async_trait]
impl Handler<GetBalance> for BalanceHandler {                 // Request → CatgaResult<M::Response>
    async fn handle(&self, query: GetBalance) -> CatgaResult<u64> { Ok(query.account_id * 1000) }
}

struct TransferHandler;
#[async_trait]
impl CommandHandler<TransferFunds> for TransferHandler {      // Command → CatgaResult<()>
    async fn handle(&self, cmd: TransferFunds) -> CatgaResult<()> { /* ... */ Ok(()) }
}

#[derive(Clone)]
struct AuditLogger;
#[async_trait]
impl EventHandler<TransferCompleted> for AuditLogger {        // Event → CatgaResult<()>
    async fn handle(&self, event: TransferCompleted) -> CatgaResult<()> { /* ... */ Ok(()) }
}
```

Closure shortcuts (no struct definition needed; `*_with` variants explicitly pass cloneable context, suitable for shared `Arc` dependencies):

```rust,ignore
use catga_core::{command_handler, event_handler, request_handler, request_handler_with};

request_handler(|value: Double| async move { Ok(value.0 * 2) })
request_handler_with(Arc::new(2u64), |factor: Arc<u64>, value: Double| async move {
    Ok(value.0 * *factor)
})
command_handler(|cmd: Credit| async move { Ok(()) })
event_handler(|evt: Credited| async move { Ok(()) })
// command_handler_with / event_handler_with work similarly
```

## 3. Registration

### `catga_handlers!` Macro (Recommended)

Builds `CatgaResult<Registry>`; syntax is semicolon-separated entries, event handlers are bracket lists:

```rust,ignore
let mediator = Mediator::new(catga_handlers! {
    request GetBalance => BalanceHandler;
    command TransferFunds => TransferHandler;
    event TransferCompleted => [AuditLogger, ProjectionHandler];
}?);
```

- Duplicate registration of the same request/command: macro errors at compile time (runtime `Registry` also returns `ErrorCode::Conflict`).
- Events require at least one handler.

### Manual `Registry` (For Dynamic Composition)

```rust,ignore
let mut registry = Registry::new();
registry.register_request::<GetBalance, _>(BalanceReader)?;   // Duplicate → Conflict
registry.register_command::<Credit, _>(CreditWriter)?;
registry.register_event::<BalanceChanged, _>(BalanceProjection); // Can register multiple
let mediator = Mediator::new(registry);
```

## 4. Dispatch (`Mediator`)

```rust,ignore
mediator.send(GetBalance { account_id: 42 }).await?;          // Request → M::Response
mediator.send_command(TransferFunds { .. }).await?;           // Command → ()
mediator.publish(TransferCompleted { .. }).await?;         // Event → Fan-out to all handlers

// Batching (same message type): maximum MAX_MEDIATOR_BATCH_SIZE = 1024 messages
let responses = mediator.send_batch(vec![req1, req2]).await?;
// For unbounded streaming use send_stream

// Through typed pipeline (see pipeline.md)
mediator.send_with(request, &pipeline).await?;
mediator.send_command_with(command, &command_pipeline).await?;

// Cooperative cancellation (tokio_util::sync::CancellationToken)
mediator.send_with_cancellation(request, token.clone()).await?;
// send_command_with_cancellation / publish_with_cancellation work similarly
```

- Handler panic under unwind strategy is isolated as `ErrorCode::Internal`; `panic = "abort"` builds terminate directly.
- `Mediator` is immutable and safe to wrap in `Arc` for sharing across tasks.

### `MediatorHandle`: Deferred Binding at Startup

Used when components constructed at startup need the mediator, but the mediator isn't built yet:

```rust,ignore
let handle = MediatorHandle::new();          // Clone into each handler
// ... Build registry / mediator ...
handle.bind(Arc::new(mediator))?;          // Bind exactly once; second bind → Conflict
handle.send(request).await?;                // Call before bind → ErrorCode::Unavailable
```

## 5. `catga_typed_mediator!`: Zero-Allocation Dispatch

Used for hot paths when the handler set is known at startup. Generates a concrete struct with compile-time monomorphic dispatch — no `Box<dyn Any>`, no downcast, no vtable. Approximately 5.8× faster sequentially and 7.0× faster concurrently than dynamic `Mediator`.

```rust,ignore
use catga_core::catga_typed_mediator;

catga_typed_mediator! {
    pub struct BankMediator;
    request GetBalance => BalanceHandler;
    command TransferFunds => TransferHandler;
    event TransferCompleted => [AuditLogger, MetricsHandler];
}

// Parameters to new follow declaration order inside the macro; event handlers take an array
let mediator = BankMediator::new(BalanceHandler, TransferHandler, [AuditLogger, MetricsHandler]);
let balance = mediator.send(GetBalance { account_id: 42 }).await?;
mediator.send_command(TransferFunds { .. }).await?;
mediator.publish(TransferCompleted { .. }).await?;
```

**Selection**: Handlers registered at runtime, or must share `Arc<Mediator>` across heterogeneous boundaries → dynamic `Mediator`; known at startup and pursuing maximum throughput → typed mediator.
