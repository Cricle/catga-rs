# Message & Handler

## Message Trait

所有消息必须实现 `Message`：

```rust
use catga_core::Message;

trait Message: Send + Sync + 'static {
    fn message_type(&self) -> &'static str;
    fn schema_version(&self) -> i32;
}
```

实现 `#[derive(Message)]` 自动生成：

```rust
use catga_core::Message;

#[derive(Message)]
#[catga(message_type = "user.created", version = 1)]
struct UserCreated {
    user_id: String,
    email: String,
}
```

## Handler 模式

Catga 有两种 Handler 模式：

### 1. Mediator Path (值语义)

用于请求/命令/事件处理：

```rust
use catga_core::{Handler, Message, Request, CatgaResult};

// 特征定义
trait Handler<M>: Send + Sync
where
    M: Message,
{
    async fn handle(&self, msg: M) -> CatgaResult<M::Response>;
}

// 使用方式
async fn handler(msg: MyRequest) -> CatgaResult<MyResponse> {
    // 逻辑
}

// 自动满足 Handler<M> trait
```

### 2. Consumer Path (引用语义)

用于消息消费（需要持有 ack）：

```rust
use catga_core::TypedDeliveryHandler;

trait TypedDeliveryHandler<M>: Send + Sync
where
    M: Message,
{
    async fn handle(&self, msg: &M) -> CatgaResult<()>;
}
```

## 闭包处理器

```rust
use catga_core::{request_handler, RequestHandlerFn};

let handler = request_handler(|msg: MyRequest| async move {
    Ok(msg.value * 2)
});
```

## Fn-blanket 实现

任何签名为 `async fn(M) -> CatgaResult<R>` 的函数自动满足 `Handler<M>`。
