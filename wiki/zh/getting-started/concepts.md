# 核心概念

## Message

Message 是 Catga 中所有消息的基 trait：

```rust
use catga_core::Message;

struct UserCreated {
    user_id: String,
    email: String,
}

impl Message for UserCreated {}
```

## Request / Command / Event

三种消息角色：

| 类型 | 响应 | 处理器数量 | 用途 |
|------|------|-----------|------|
| `Request<M>` | `M::Response` | 1 | 查询/请求 |
| `Command` | `()` | 1 | 命令 |
| `Event` | `()` | N | 事件通知 |

```rust
use catga_core::{Message, Request, Command, Event};

// 请求 - 有返回值
struct GetUser { id: String }
impl Message for GetUser {}
impl Request for GetUser { type Response = User; }

// 命令 - 无返回值
struct CreateUser { email: String }
impl Message for CreateUser {}
impl Command for CreateUser {}

// 事件 - 多处理器
struct UserCreated { id: String, email: String }
impl Message for UserCreated {}
impl Event for UserCreated {}
```

## Handler

处理器是处理消息的业务逻辑：

```rust
use catga_core::{Handler, Message, Request, CatgaResult};

struct GetUser;
impl Message for GetUser {}
impl Request for GetUser { type Response = User; }

// 简单方式：直接用 async fn
async fn get_user_handler(msg: GetUser) -> CatgaResult<User> {
    Ok(User { id: msg.id, email: "test@example.com".into() })
}
```

## Transport

Transport 是消息的传输层抽象：

```rust
use catga_core::{MessageTransport, Destination};

// 发布消息
transport.publish(envelope, Destination::Topic("users.created")).await?;

// 发送请求并等待响应
let response = transport
    .send(envelope, Destination::Queue("user-service"))
    .await?;
```

## EventStore

事件存储：

```rust
use catga_core::{EventStore, EventPage};

// 追加事件
store.append("user-123", vec![envelope], Some(expected_version)).await?;

// 读取事件
let page = store.read_page("user-123", 0, 100).await?;
```
