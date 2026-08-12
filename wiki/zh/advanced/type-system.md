# 类型系统

## 编译期类型安全

Catga 的核心设计原则是编译期类型检查。

## Message 类型推导

```rust
use catga_core::Message;

#[derive(Message)]
#[catga(message_type = "order.created")]
struct OrderCreated {
    order_id: String,
    total: f64,
}

// 编译器自动生成:
// fn message_type(&self) -> &'static str { "order.created" }
// fn schema_version(&self) -> i32 { 1 }
```

## Handler 类型约束

```rust
use catga_core::{Handler, Message, Request, CatgaResult};

// Handler<M> 要求 M: Message
trait Handler<M>
where
    M: Message,
{
    async fn handle(&self, msg: M) -> CatgaResult<M::Response>;
}

// Request<M> 提供 Response 类型
trait Request: Message {
    type Response;
}

// 使用
impl Handler<Ping> for PingHandler {
    async fn handle(&self, msg: Ping) -> CatgaResult<Ping::Response> {
        // msg 类型: Ping
        // 返回类型: Ping::Response
        Ok("pong".to_string())
    }
}
```

## 泛型特化

Catga 使用 Rust 的泛型特化优化性能：

```rust
// 通用实现
impl<T: PayloadEncoder<M>, M: Message> TypedPublisher<T, M> {
    pub async fn publish(&self, msg: &M) -> CatgaResult<()> {
        let encoded = self.codec.encode_payload(msg)?;
        // ...
    }
}

// JsonEncoder 特化
impl TypedPublisher<JsonEncoder, UserCreated> {
    pub async fn publish(&self, msg: &UserCreated) -> CatgaResult<()> {
        // 更快的特化实现
    }
}
```

## 传输层类型边界

```rust
use catga_core::{MessageTransport, Destination};

// 发布时类型检查
transport.publish(envelope, Destination::Topic("events")).await?;

// 接收时类型安全
let delivery: Delivery<Ping> = consumer.receive().await?;
let msg: Ping = delivery.decode()?;
// msg 类型安全
```

## 类型化状态机

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

// 编译器确保:
// - 只有 ProcessPayment 能从 Created -> Processing
// - Completed 是终态
```
