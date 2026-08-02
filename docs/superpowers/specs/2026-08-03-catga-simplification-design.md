# Catga-RS Simplification Design

> **Date:** 2026-08-03
> **Status:** Draft for Review

## Goal

Simplify catga-rs for easier user adoption by reducing crates, unifying message types, and providing a simple one-line transport switch API.

## Architecture

### Current State (18 crates)

```
catga-core, catga-flow, catga-flow-store, catga-local, catga-codec-bincode,
catga-codec-memorypack, catga-axum, catga-auto, catga-cluster, catga-macros,
catga-memory, catga-nats, catga-redis, catga-robustmq, catga-scheduler-tokio-cron,
catga-testing, tests, examples
```

### Target State (4 crates)

```
catga-core    # All-in-one: core traits, macros, testing helpers, auto-discovery
catga-nats    # NATS transport
catga-redis   # Redis transport (streams, pub/sub)
catga-robustmq # RobustMQ transport
```

**Merged into catga-core:**
- catga-macros → catga-core (derive macros)
- catga-testing → catga-core (test helpers)
- catga-auto → catga-core (compile-time handler discovery)
- catga-codec-bincode → catga-core (built-in codec)
- catga-codec-memorypack → catga-core (built-in codec)

**Removed:**
- catga-flow, catga-flow-store, catga-scheduler-tokio-cron → future extensions
- catga-cluster, catga-memory → future extensions
- catga-axum → example only

## Message Types

### Unified Traits (3 types, not 7)

```rust
// Request: expects a response
#[catga_request(response = T)]
struct GetUser(u64);

// Command: fire-and-forget
#[catga_command]
struct ArchiveSession(u64);

// Event: publish/subscribe
#[catga_event]
struct UserLoggedIn { user_id: u64, timestamp: i64 }
```

### Handler Attachment (struct-based, not functional)

```rust
struct UserService;

#[async_trait]
impl Handler<GetUser> for UserService {
    async fn handle(&self, request: GetUser) -> CatgaResult<User> {
        // ...
    }
}
```

## Transport API

### Unified Transport Trait

```rust
pub trait Transport: Send + Sync {
    async fn send<R>(&self, request: R) -> CatgaResult<R::Response>
    where R: Request + 'static;

    async fn send_command<C>(&self, command: C) -> CatgaResult<()>
    where C: Command + 'static;

    async fn publish<E>(&self, event: E) -> CatgaResult<()>
    where E: Event + 'static;

    // Delayed variants
    async fn send_delayed<R>(&self, request: R, delay: Duration) -> CatgaResult<R::Response>;
    async fn send_command_delayed<C>(&self, command: C, delay: Duration) -> CatgaResult<()>;
    async fn publish_delayed<E>(&self, event: E, delay: Duration) -> CatgaResult<()>;
}
```

### One-Line Transport Switching

```rust
// Local (in-memory)
#[catga_main(impl Transport = LocalTransport::new())]
async fn run() -> CatgaResult<()> {
    Ok(())
}

// Distributed (NATS)
#[catga_main(impl Transport = NatsTransport::connect("nats://localhost:4222").await?)]
async fn run() -> CatgaResult<()> {
    Ok(())
}
```

### Resolving #[catga_main] vs #[tokio::main] Conflict

The solution is to attach #[catga_main] to a **separate function**, not main:

```rust
// #[catga_main] on separate function, avoiding #[tokio::main] conflict
#[catga_main(impl Transport = LocalTransport::new())]
async fn run() -> CatgaResult<()> {
    let user = transport.send(GetUser(42)).await?;
    transport.publish(UserLoggedIn { user_id: 42, timestamp: now() }).await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    run().await;
}
```

## Compile-Time Auto-Discovery

Using `catga_auto` pattern:

```rust
#[catga_auto]
struct MyApp;

impl catga_auto::App for MyApp {
    fn handlers<H: catga_auto::HandlerRegistry>(self, registry: &mut H) {
        registry.register_request(GetUserHandler);
        registry.register_command(ArchiveSessionHandler);
        registry.register_event(UserLoggedInHandler);
    }
}
```

## Testing API

```rust
use catga_testing::{CatgaTestHarness, HandlerSpy, EventHandlerSpy};

#[tokio::test]
async fn test_handler() {
    let harness = CatgaTestHarness::new().unwrap();
    harness.register_request::<GetUser, _>(UserService);
    harness.capture_event::<UserLoggedIn>();

    let running = harness.start();
    let user = running.mediator().send(GetUser(42)).await.unwrap();

    assert_eq!(running.consumed_of::<GetUser>(), [GetUser(42)]);
    assert!(running.published_of::<UserLoggedIn>().len() > 0);
}
```

## Error Handling

```rust
use catga_core::{CatgaError, ErrorCode, CatgaResult};

match result {
    Ok(value) => { /* success */ }
    Err(e) => {
        match e.code() {
            ErrorCode::NotFound => { /* handle 404 */ }
            ErrorCode::Validation => { /* handle 400 */ }
            ErrorCode::Timeout => { /* handle 504 */ }
            _ => { /* handle other */ }
        }
    }
}
```

## File Structure (catga-core)

```
catga-core/src/
├── lib.rs              # Main entry, public re-exports
├── message.rs          # Message, Request, Command, Event traits
├── transport.rs        # Transport trait
├── new_transport.rs    # Simplified Transport trait
├── handler.rs          # Handler trait
├── error.rs            # CatgaError, ErrorCode, CatgaResult
├── macros.rs           # catga_request!, catga_command!, catga_event! derive macros
├── catga_main.rs       # #[catga_main] macro
├── testing.rs          # Test helpers (merged from catga-testing)
├── auto.rs             # catga_auto (merged from catga-auto)
└── codecs/             # Built-in codecs
    ├── bincode.rs
    └── memorypack.rs
```

## Migration Path

1. Create new catga-core with merged functionality
2. Update catga-local, catga-nats, catga-redis, catga-robustmq to implement new Transport trait
3. Deprecate old crates with migration guide
4. Update examples to use new API

## Success Criteria

- [ ] 4 crates instead of 18
- [ ] 3 message types instead of 7
- [ ] One-line transport switch
- [ ] Handler attached to struct (not functional)
- [ ] #[catga_main] on separate function (no #[tokio::main] conflict)
- [ ] Compile-time handler auto-discovery
- [ ] Built-in testing helpers
- [ ] Clear migration path from current API
