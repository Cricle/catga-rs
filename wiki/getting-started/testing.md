# 测试指南

## catga-testing

测试工具库提供模拟和断言功能。

## HandlerSpy

拦截消息用于测试：

```rust
use catga_core::Message;
use catga_testing::HandlerSpy;

let spy = HandlerSpy::<Ping>::default();

// 模拟处理器
let handler = spy.as_handler();

// 发送消息
handler.handle(Ping).await?;

// 断言
assert_eq!(spy.count(), 1);
assert_eq!(spy.get(0), Some(&Ping));
```

## EventHandlerSpy

事件处理器测试：

```rust
use catga_testing::EventHandlerSpy;

let spy = EventHandlerSpy::<UserCreated>::new();

let projection = spy.delegating_handler(real_projection);

projection.handle(UserCreated { id: "1".into() }).await?;

// 验证事件被处理
assert!(spy.contains(&UserCreated { id: "1".into() }));
```

## FlowTestContext

Flow 工作流测试：

```rust
use catga_flow_testing::FlowTestContext;

let ctx = FlowTestContext::new();
let flow = TestFlow::new(&ctx);

// 触发事件
flow.trigger(CreateOrder { items: vec![] }).await?;

// 断言状态
assert_eq!(flow.state(), TestFlow::Processing);
```

## 集成测试

```rust
#[tokio::test]
async fn test_order_workflow() {
    // 初始化测试环境
    let store = InMemoryEventStore::new();
    let transport = MemoryTransport::new();

    let app = AutoApp::builder()
        .with_store(store.clone())
        .with_transport(transport.clone())
        .handler(order_handler)?
        .build()?;

    // 执行命令
    app.handle()
        .send_command(CreateOrder { items: vec![item] })
        .await?;

    // 验证结果
    let order = store.find::<Order>("order-1").await?;
    assert_eq!(order.status(), OrderStatus::Created);
}
```

## Mock 消息

```rust
use catga_testing::MockMessage;

let msg = MockMessage::<UserCreated>::new()
    .with_id("test-id")
    .withcorrelation_id("corr-id");

assert_eq!(msg.id(), "test-id");
```

## 断言辅助

```rust
use catga_testing::{assert_success, assert_failure, assert_error_code};

let result = handler.handle(msg).await?;

assert_success!(result);
assert_failure!(result, ErrorCode::Validation);
assert_error_code!(result, ErrorCode::Conflict);
```
