# Message & Handler

## Message Trait

All messages must implement `Message`:

```rust
use catga_core::Message;

trait Message: Send + Sync + 'static {
    fn message_type(&self) -> &'static str;
    fn schema_version(&self) -> i32;
}
```

Implement `#[derive(Message)]` to auto-generate:

```rust
use catga_core::Message;

#[derive(Message)]
#[catga(message_type = "user.created", version = 1)]
struct UserCreated {
    user_id: String,
    email: String,
}
```

## Handler Pattern

Catga has two Handler modes:

### 1. Mediator Path (Value Semantics)

Used for request/command/event processing:

```rust
use catga_core::{Handler, Message, Request, CatgaResult};

// Trait definition
trait Handler<M>: Send + Sync
where
    M: Message,
{
    async fn handle(&self, msg: M) -> CatgaResult<M::Response>;
}

// Usage
async fn handler(msg: MyRequest) -> CatgaResult<MyResponse> {
    // logic
}

// Automatically satisfies Handler<M> trait
```

### 2. Consumer Path (Reference Semantics)

Used for message consumption (requires holding ack):

```rust
use catga_core::TypedDeliveryHandler;

trait TypedDeliveryHandler<M>: Send + Sync
where
    M: Message,
{
    async fn handle(&self, msg: &M) -> CatgaResult<()>;
}
```

## Closure Handler

```rust
use catga_core::{request_handler, RequestHandlerFn};

let handler = request_handler(|msg: MyRequest| async move {
    Ok(msg.value * 2)
});
```

## Fn-blanket Implementation

Any function with signature `async fn(M) -> CatgaResult<R>` automatically satisfies `Handler<M>`.
