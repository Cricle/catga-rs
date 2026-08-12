# 核心概念

本指南帮助你理解 Catga 的核心概念。无论你是刚接触事件驱动架构，还是已有经验，这里都会解释这些概念在 Catga 中如何工作。

## 为什么需要这些概念？

在传统 CRUD 应用中，我们直接读写数据库。但随着系统变复杂，会遇到：

- **并发冲突** - 多个用户同时修改同一数据
- **审计追踪** - 需要知道谁在什么时候改了什么
- **分布式事务** - 跨多个服务的操作如何保证一致性
- **事件驱动** - 系统间如何松耦合通信

Catga 的核心概念正是为了解决这些问题。

## Message（消息）

Message 是 Catga 中所有消息的基 trait。它代表系统中传递的信息单元。

```rust
use catga_core::Message;

struct UserCreated {
    user_id: String,
    email: String,
}

impl Message for UserCreated {}
```

**为什么重要？** 消息是 Catga 的核心抽象。所有业务操作都通过消息传递，这使得系统可以：
- 异步处理
- 持久化重放
- 分布式通信

## Request / Command / Event（请求/命令/事件）

这是三种不同的消息角色，决定了消息如何被处理：

| 类型 | 响应 | 处理器数量 | 什么时候用 |
|------|------|-----------|-----------|
| `Request<M>` | 返回 `M::Response` | 1 个 | 需要等待结果的查询或请求 |
| `Command` | 返回 `()` | 1 个 | 执行操作但不关心返回值 |
| `Event` | 返回 `()` | N 个 | 通知发生了某事，可被多方监听 |

### 什么时候用哪种？

```
用户点击"查看订单"
    → Request<GetOrder> → 返回订单详情

用户点击"创建订单"
    → Command<CreateOrder> → 创建订单，不关心返回值

订单创建成功后
    → Event<OrderCreated> → 通知库存服务扣库存
                      → 通知邮件服务发确认邮件
                      → 通知分析服务记录数据
```

```rust
use catga_core::{Message, Request, Command, Event};

// 请求 - 有返回值，用于查询或需要等待结果的操作
struct GetUser { id: String }
impl Message for GetUser {}
impl Request for GetUser { type Response = User; }

// 命令 - 无返回值，用于执行操作
struct CreateUser { email: String }
impl Message for CreateUser {}
impl Command for CreateUser {}

// 事件 - 可被多个处理器监听，用于解耦的通知
struct UserCreated { id: String, email: String }
impl Message for UserCreated {}
impl Event for UserCreated {}
```

### 用宏更简单

使用 Catga 提供的宏可以简化定义：

```rust
use catga_core::{catga_request, catga_command, catga_event};

// 请求 - #[catga_request(response = ...)]
#[catga_core::catga_request(response = User)]
struct GetUser { id: String }

// 命令 - #[derive(catga_command)]
#[derive(catga_core::catga_command)]
struct CreateUser { email: String }

// 事件 - #[derive(catga_event)]
#[derive(catga_core::catga_event)]
struct UserCreated { id: String, email: String }
```

## Handler（处理器）

Handler 是处理消息的业务逻辑。当消息被派发时，对应的 Handler 会被调用。

### 简单方式：直接用 async fn

```rust
use catga_core::{Handler, Message, Request, CatgaResult};

struct GetUser;
impl Message for GetUser {}
impl Request for GetUser { type Response = User; }

// 直接用 async 函数作为处理器
async fn get_user_handler(msg: GetUser) -> CatgaResult<User> {
    Ok(User { id: msg.id, email: "test@example.com".into() })
}
```

### 使用 #[catga_service]

更推荐的方式是用 `#[catga_service]` 宏：

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

## Transport（传输层）

Transport 是消息的传输层抽象，负责消息的发送和接收。它让你的业务逻辑与具体的通信协议解耦。

### 传输模式

| 模式 | 说明 | 适用场景 |
|------|------|---------|
| **Queue (队列)** | 点对点发送，一条消息只被一个消费者处理 | 命令、请求 |
| **Topic (主题)** | 发布订阅，一条消息被所有订阅者处理 | 事件通知 |

```rust
use catga_core::{MessageTransport, Destination};

// 发布消息到主题（所有订阅者都会收到）
transport.publish(envelope, Destination::Topic("users.created")).await?;

// 发送消息到队列（只有一个消费者会处理）
let response = transport
    .send(envelope, Destination::Queue("user-service"))
    .await?;
```

### 支持的后端

Catga 支持多种传输后端：

| 后端 | 说明 | 特点 |
|------|------|------|
| NATS/JetStream | 高性能消息系统 | 支持流、持久化、消费者组 |
| Redis | 常用队列解决方案 | 简单、轻量 |
| HTTP | 基于 Axum | 易于集成、调试简单 |

## EventStore（事件存储）

事件存储是事件溯源模式的核心。它不是存储当前状态，而是存储所有状态变化的事件序列。

### 为什么用事件存储？

**传统方式（状态存储）：**
```
当前余额: $100
```

**事件存储：**
```
[存款 $50] → [取款 $30] → [存款 $80] = 当前余额: $100
```

事件存储的好处：
- 完整保留历史，可审计
- 可随时重放重建任意时刻状态
- 简化并发处理（追加而非更新）

### 基本操作

```rust
use catga_core::{EventStore, EventPage};

// 追加新事件（乐观并发控制）
store.append("user-123", vec![envelope], Some(expected_version)).await?;

// 分页读取事件历史
let page = store.read_page("user-123", 0, 100).await?;
```

## 聚合根 (Aggregate Root)

聚合根是领域驱动设计 (DDD) 中的概念。它是一个实体，作为一组相关对象的边界。

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

// 定义事件
#[derive(Clone)]
enum BankAccountEvent {
    Deposited { amount: u64 },
    Withdrawn { amount: u64 },
}

// 聚合的业务逻辑
impl BankAccount {
    fn deposit(&mut self, amount: u64) -> CatgaResult<BankAccountEvent> {
        self.balance += amount;
        Ok(BankAccountEvent::Deposited { amount })
    }

    fn withdraw(&mut self, amount: u64) -> CatgaResult<BankAccountEvent> {
        if self.balance < amount {
            return Err("余额不足".into());
        }
        self.balance -= amount;
        Ok(BankAccountEvent::Withdrawn { amount })
    }

    // 应用事件重建状态
    fn apply(&mut self, event: &BankAccountEvent) {
        match event {
            BankAccountEvent::Deposited { amount } => self.balance += amount,
            BankAccountEvent::Withdrawn { amount } => self.balance -= amount,
        }
    }
}
```

## 下一步

- [安装指南](./installation.md) - 搭建开发环境
- [第一个应用](./first-app.md) - 实战入门
- [CQRS 和事件溯源](../core/cqrs.md) - 深入理解架构模式
