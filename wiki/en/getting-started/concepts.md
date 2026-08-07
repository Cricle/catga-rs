# Core Concepts

## Message

Message is the base trait for all messages in Catga:

```rust
use catga_core::Message;

struct UserCreated {
    user_id: String,
    email: String,
}

impl Message for UserCreated {}
```

## Request / Command / Event

Three message roles:

| Type | Response | Handler count | Use case |
|------|----------|---------------|----------|
| `Request<M>` | `M::Response` | 1 | Query/request |
| `Command` | `()` | 1 | Command |
| `Event` | `()` | N | Event notification |

```rust
use catga_core::{Message, Request, Command, Event};

// Request - has return value
struct GetUser { id: String }
impl Message for GetUser {}
impl Request for GetUser { type Response = User; }

// Command - no return value
struct CreateUser { email: String }
impl Message for CreateUser {}
impl Command for CreateUser {}

// Event - multiple handlers
struct UserCreated { id: String, email: String }
impl Message for UserCreated {}
impl Event for UserCreated {}
```

## Handler

Handlers are the business logic that processes messages:

```rust
use catga_core::{Handler, Message, Request, CatgaResult};

struct GetUser;
impl Message for GetUser {}
impl Request for GetUser { type Response = User; }

// Simple approach: use async fn directly
async fn get_user_handler(msg: GetUser) -> CatgaResult<User> {
    Ok(User { id: msg.id, email: "test@example.com".into() })
}
```

## Transport

Transport is the abstraction layer for message delivery:

```rust
use catga_core::{MessageTransport, Destination};

// Publish a message
transport.publish(envelope, Destination::Topic("users.created")).await?;

// Send a request and wait for response
let response = transport
    .send(envelope, Destination::Queue("user-service"))
    .await?;
```

## EventStore

Event store:

```rust
use catga_core::{EventStore, EventPage};

// Append events
store.append("user-123", vec![envelope], Some(expected_version)).await?;

// Read events
let page = store.read_page("user-123", 0, 100).await?;
```
