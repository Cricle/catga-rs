# Mediator & Registry

## Mediator

Mediator 是消息分发的核心：

```rust
use catga_core::{Mediator, Registry};

let registry = Registry::new();
registry.register_request::<Ping, _>(PingHandler)?;
registry.register_command::<CreateOrder, _>(CreateOrderHandler)?;
registry.register_event::<OrderCreated, _>(OrderProjection)?;

let mediator = Mediator::new(registry);
```

## Registry

Registry 管理处理器注册：

```rust
use catga_core::Registry;

// 创建
let registry = Registry::new();

// 注册请求处理器（唯一）
registry.register_request::<GetUser, _>(GetUserHandler)?;

// 注册命令处理器（唯一）
registry.register_command::<CreateOrder, _>(CreateOrderHandler)?;

// 注册事件处理器（多个）
registry.register_event::<UserCreated, _>(Projection1);
registry.register_event::<UserCreated, _>(Projection2);
```

## MediatorHandle

分发消息：

```rust
use catga_core::MediatorHandle;

// 发送请求（等待响应）
let user = handle.send(GetUser { id: "123".into() }).await?;

// 发送命令（无响应）
handle.send_command(CreateOrder { item: "widget".into() }).await?;

// 发布事件
handle.publish(UserCreated { id: "123".into() }).await?;
```

## Pipeline Behaviors

在处理前添加横切关注点：

```rust
use catga_core::{catga_pipeline, RetryBehavior, TimeoutBehavior};
use std::time::Duration;

let pipeline = catga_pipeline!(
    Request;
    RetryBehavior::new(3, Duration::from_millis(100)),
    TimeoutBehavior::new(Duration::from_secs(5)),
)?;

let mediator = Mediator::new(registry).with_pipeline(pipeline);
```

## catga_handlers! 宏

简化多处理器注册：

```rust
use catga_core::{catga_handlers, request_handler, command_handler, event_handler};

let handlers = catga_handlers! {
    request Ping => ping_handler,
    request GetUser => get_user_handler,
    command CreateOrder => create_order_handler,
    event OrderCreated => order_projection,
}?;

let mediator = Mediator::new(catga_core::Registry::with_handlers(handlers));
```
