# CQRS 模式

## 命令查询分离

Catga 实现完整的 CQRS 模式：

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   Command   │─────▶│  Aggregates │─────▶│    Events   │
│   Handler   │      │   (写入)    │      │   (状态)    │
└─────────────┘      └─────────────┘      └─────────────┘
                                                │
                                                ▼
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   Queries   │◀─────│  Projector  │◀─────│    Event    │
│   (读模型)   │      │   (读模型)   │      │   Handler   │
└─────────────┘      └─────────────┘      └─────────────┘
```

## Command

命令改变状态：

```rust
use catga_core::{Message, Command, CatgaResult, CommandHandler};

#[derive(Message)]
struct CreateOrder {
    customer_id: String,
    items: Vec<OrderItem>,
}

impl Command for CreateOrder {}

// 命令处理器
struct CreateOrderHandler;

#[async_trait::async_trait]
impl CommandHandler<CreateOrder> for CreateOrderHandler {
    async fn handle(&self, cmd: CreateOrder) -> CatgaResult<()> {
        // 验证
        if cmd.items.is_empty() {
            return Err(CatgaError::validation("items cannot be empty"));
        }

        // 创建聚合
        let order = OrderAggregate::create(cmd.customer_id, cmd.items)?;
        order_repository.save(order).await?;

        Ok(())
    }
}
```

## Request (Query)

查询只读：

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

事件通知状态变化：

```rust
use catga_core::{Message, Event, CatgaResult, EventHandler};

#[derive(Message, Clone)]
struct OrderCreated {
    order_id: String,
    customer_id: String,
    total: Money,
}

impl Event for OrderCreated {}

// 事件处理器 (可以有多个)
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

## 类型安全

所有消息类型在编译时检查：

```rust
// 尝试注册重复请求处理器 → 编译错误
registry.register_request::<GetUser, _>(Handler1)?;
// registry.register_request::<GetUser, _>(Handler2)?; // Error!

// 事件可以有多个处理器
registry.register_event::<OrderCreated, _>(Handler1);
registry.register_event::<OrderCreated, _>(Handler2); // OK
```
