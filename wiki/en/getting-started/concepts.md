# Core Concepts

This guide explains Catga's core concepts. Whether you're new to event-driven architecture or have experience, you'll understand how these concepts work in Catga.

## Why Do We Need These Concepts?

In traditional CRUD applications, we read and write databases directly. But as systems grow complex, we encounter:

- **Concurrency conflicts** - multiple users modifying the same data
- **Audit trails** - need to know who changed what and when
- **Distributed transactions** - how to ensure consistency across services
- **Event-driven communication** - how to decouple systems

Catga's core concepts solve these problems.

## Message

Message is the base trait for all messages in Catga. It represents the unit of information passed through the system.

```rust
use catga_core::Message;

struct UserCreated {
    user_id: String,
    email: String,
}

impl Message for UserCreated {}
```

**Why it matters?** Messages are Catga's core abstraction. All business operations go through messages, enabling:
- Asynchronous processing
- Persistent replay
- Distributed communication

## Request / Command / Event

Three different message roles that determine how messages are processed:

| Type | Response | Handler count | When to use |
|------|----------|---------------|-------------|
| `Request<M>` | Returns `M::Response` | 1 | Queries or requests that need a response |
| `Command` | Returns `()` | 1 | Operations without return value |
| `Event` | Returns `()` | N | Notifications that multiple handlers can listen to |

### When to Use Which?

```
User clicks "View Order"
    → Request<GetOrder> → Returns order details

User clicks "Create Order"
    → Command<CreateOrder> → Creates order, no response needed

After order is created
    → Event<OrderCreated> → Inventory service: deduct stock
                      → Email service: send confirmation
                      → Analytics service: log data
```

```rust
use catga_core::{Message, Request, Command, Event};

// Request - has return value, for queries or operations needing response
struct GetUser { id: String }
impl Message for GetUser {}
impl Request for GetUser { type Response = User; }

// Command - no return value, for executing operations
struct CreateUser { email: String }
impl Message for CreateUser {}
impl Command for CreateUser {}

// Event - multiple handlers can subscribe, for decoupled notifications
struct UserCreated { id: String, email: String }
impl Message for UserCreated {}
impl Event for UserCreated {}
```

### Using Macros (Simpler)

Macros simplify the definitions:

```rust
use catga_core::{catga_request, catga_command, catga_event};

// Request - #[catga_request(response = ...)]
#[catga_core::catga_request(response = User)]
struct GetUser { id: String }

// Command - #[derive(catga_command)]
#[derive(catga_core::catga_command)]
struct CreateUser { email: String }

// Event - #[derive(catga_event)]
#[derive(catga_core::catga_event)]
struct UserCreated { id: String, email: String }
```

## Handler

Handlers are the business logic that processes messages. When a message is dispatched, its corresponding handler is called.

### Simple Way: Direct async fn

```rust
use catga_core::{Handler, Message, Request, CatgaResult};

struct GetUser;
impl Message for GetUser {}
impl Request for GetUser { type Response = User; }

// Use async function directly as handler
async fn get_user_handler(msg: GetUser) -> CatgaResult<User> {
    Ok(User { id: msg.id, email: "test@example.com".into() })
}
```

### Using #[catga_service] (Recommended)

The recommended way is using the `#[catga_service]` macro:

```rust
use catga_core::{catga_service, catga_request, CatgaResult};

#[catga_request(response = User)]
struct GetUser { id: String }

struct UserService;

#[catga_service]
impl UserService {
    async fn get_user(&self, msg: GetUser) -> CatgaResult<User> {
        Ok(User { id: msg.id, email: "test@example.com".into() })
    }
}
```

## Transport

Transport is the abstraction layer for message delivery. It decouples your business logic from specific communication protocols.

### Transport Patterns

| Pattern | Description | Use cases |
|---------|-------------|-----------|
| **Queue** | Point-to-point, message processed by one consumer | Commands, requests |
| **Topic** | Publish-subscribe, all subscribers receive message | Event notifications |

```rust
use catga_core::{MessageTransport, Destination};

// Publish to topic (all subscribers receive)
transport.publish(envelope, Destination::Topic("users.created")).await?;

// Send to queue (one consumer processes)
let response = transport
    .send(envelope, Destination::Queue("user-service"))
    .await?;
```

### Supported Backends

Catga supports multiple transport backends:

| Backend | Description | Features |
|---------|-------------|----------|
| NATS/JetStream | High-performance messaging | Streams, persistence, consumer groups |
| Redis | Common queue solution | Simple, lightweight |
| HTTP | Built on Axum | Easy integration, simple debugging |

## EventStore

Event store is the core of event sourcing. Instead of storing current state, it stores the sequence of all state changes (events).

### Why Use Event Store?

**Traditional approach (state storage):**
```
Current balance: $100
```

**Event store:**
```
[Deposit $50] → [Withdraw $30] → [Deposit $80] = Current balance: $100
```

Benefits of event store:
- Complete history, fully auditable
- Can replay to rebuild state at any point
- Simplified concurrency (append-only)

### Basic Operations

```rust
use catga_core::{EventStore, EventPage};

// Append new events (optimistic concurrency control)
store.append("user-123", vec![envelope], Some(expected_version)).await?;

// Read events with pagination
let page = store.read_page("user-123", 0, 100).await?;
```

## Aggregate Root

Aggregate root is a Domain-Driven Design (DDD) concept. It's an entity that serves as a boundary for a group of related objects.

```rust
use catga_core::{Aggregate, Event, CatgaResult};

struct BankAccount {
    id: String,
    balance: u64,
    version: u64,
}

impl Aggregate for BankAccount {
    type Event = BankAccountEvent;

    fn aggregate_id(&self) -> &str {
        &self.id
    }
}

// Define events
#[derive(Clone)]
enum BankAccountEvent {
    Deposited { amount: u64 },
    Withdrawn { amount: u64 },
}

// Business logic
impl BankAccount {
    fn deposit(&mut self, amount: u64) -> CatgaResult<BankAccountEvent> {
        self.balance += amount;
        Ok(BankAccountEvent::Deposited { amount })
    }

    fn withdraw(&mut self, amount: u64) -> CatgaResult<BankAccountEvent> {
        if self.balance < amount {
            return Err("Insufficient balance".into());
        }
        self.balance -= amount;
        Ok(BankAccountEvent::Withdrawn { amount })
    }

    // Apply event to rebuild state
    fn apply(&mut self, event: &BankAccountEvent) {
        match event {
            BankAccountEvent::Deposited { amount } => self.balance += amount,
            BankAccountEvent::Withdrawn { amount } => self.balance -= amount,
        }
    }
}
```

## Next Steps

- [Installation](./installation.md) - Set up your development environment
- [First Application](./first-app.md) - Hands-on getting started
- [CQRS and Event Sourcing](../core/cqrs.md) - Deep dive into architectural patterns
