# CQRS Pattern

## Command Query Separation

Catga implements a complete CQRS pattern:

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   Command   │─────▶│  Aggregates │─────▶│    Events   │
│   Handler   │      │   (Write)   │      │   (State)   │
└─────────────┘      └─────────────┘      └─────────────┘
                                                │
                                                ▼
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   Queries   │◀─────│  Projector  │◀─────│    Event    │
│  (Read Model)│      │  (Read Model)│      │   Handler   │
└─────────────┘      └─────────────┘      └─────────────┘
```

## Command

Commands change state:

```rust
use catga_core::{Message, Command, CatgaResult, CommandHandler};

#[derive(Message)]
struct CreateOrder {
    customer_id: String,
    items: Vec<OrderItem>,
}

impl Command for CreateOrder {}

// Command handler
struct CreateOrderHandler;

#[async_trait::async_trait]
impl CommandHandler<CreateOrder> for CreateOrderHandler {
    async fn handle(&self, cmd: CreateOrder) -> CatgaResult<()> {
        // Validation
        if cmd.items.is_empty() {
            return Err(CatgaError::validation("items cannot be empty"));
        }

        // Create aggregate
        let order = OrderAggregate::create(cmd.customer_id, cmd.items)?;
        order_repository.save(order).await?;

        Ok(())
    }
}
```

## Request (Query)

Queries are read-only:

```rust
use catga_core::{Message, Request, Handler, CatgaResult};

#[derive(Message)]
struct GetOrderTotal {
    order_id: String,
}

impl Request for GetOrderTotal {
    type Response = Money;
}

struct GetOrderTotalHandler {
    read_model: Arc<dyn ReadModelStore>,
}

#[async_trait::async_trait]
impl Handler<GetOrderTotal> for GetOrderTotalHandler {
    async fn handle(&self, req: GetOrderTotal) -> CatgaResult<Money> {
        self.read_model
            .get::<OrderSummary>(&req.order_id)
            .await
            .map(|s| s.total)
    }
}
```

## Event

Events notify about state changes:

```rust
use catga_core::{Message, Event, CatgaResult, EventHandler};

#[derive(Message, Clone)]
struct OrderCreated {
    order_id: String,
    customer_id: String,
    total: Money,
}

impl Event for OrderCreated {}

// Event handler (can have multiple)
struct OrderCreatedEmailer;

#[async_trait::async_trait]
impl EventHandler<OrderCreated> for OrderCreatedEmailer {
    async fn handle(&self, evt: OrderCreated) -> CatgaResult<()> {
        send_confirmation_email(&evt.customer_id, &evt.order_id).await?;
        Ok(())
    }
}

struct OrderCreatedAnalytics;

#[async_trait::async_trait]
impl EventHandler<OrderCreated> for OrderCreatedAnalytics {
    async fn handle(&self, evt: OrderCreated) -> CatgaResult<()> {
        analytics.track("order_created", &evt).await?;
        Ok(())
    }
}
```

## Type Safety

All message types are checked at compile time:

```rust
// Attempting to register duplicate request handler -> compile error
registry.register_request::<GetUser, _>(Handler1)?;
// registry.register_request::<GetUser, _>(Handler2)?; // Error!

// Events can have multiple handlers
registry.register_event::<OrderCreated, _>(Handler1);
registry.register_event::<OrderCreated, _>(Handler2); // OK
```
