# Type System

## Compile-Time Type Safety

Catga's core design principle is compile-time type checking.

## Message Type Derivation

```rust
use catga_core::Message;

#[derive(Message)]
#[catga(message_type = "order.created")]
struct OrderCreated {
    order_id: String,
    total: f64,
}

// Compiler automatically generates:
// fn message_type(&self) -> &'static str { "order.created" }
// fn schema_version(&self) -> i32 { 1 }
```

## Handler Type Constraints

```rust
use catga_core::{Handler, Message, Request, CatgaResult};

// Handler<M> requires M: Message
trait Handler<M>
where
    M: Message,
{
    async fn handle(&self, msg: M) -> CatgaResult<M::Response>;
}

// Request<M> provides the Response type
trait Request: Message {
    type Response;
}

// Usage
impl Handler<Ping> for PingHandler {
    async fn handle(&self, msg: Ping) -> CatgaResult<Ping::Response> {
        // msg type: Ping
        // Return type: Ping::Response
        Ok("pong".to_string())
    }
}
```

## Generic Specialization

Catga uses Rust's generic specialization for performance optimization:

```rust
// Generic implementation
impl<T: PayloadEncoder<M>, M: Message> TypedPublisher<T, M> {
    pub async fn publish(&self, msg: &M) -> CatgaResult<()> {
        let encoded = self.codec.encode_payload(msg)?;
        // ...
    }
}

// JsonEncoder specialization
impl TypedPublisher<JsonEncoder, UserCreated> {
    pub async fn publish(&self, msg: &UserCreated) -> CatgaResult<()> {
        // Faster specialized implementation
    }
}
```

## Transport Layer Type Boundaries

```rust
use catga_core::{MessageTransport, Destination};

// Type checking at publish time
transport.publish(envelope, Destination::Topic("events")).await?;

// Type safety at receive time
let delivery: Delivery<Ping> = consumer.receive().await?;
let msg: Ping = delivery.decode()?;
// msg is type-safe
```

## Typed State Machine

```rust
use catga_core::Flow;

#[derive(State)]
enum OrderFlow {
    #[state(initial)]
    Created,

    #[state(accepts = [ProcessPayment])]
    Processing,

    #[state(final)]
    Completed,
}

// Compiler ensures:
// - Only ProcessPayment can transition from Created -> Processing
// - Completed is a terminal state
```
