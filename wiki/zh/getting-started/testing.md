# 测试指南

## `catga_core::testing`

测试工具（spy、捕获、harness、断言）随 catga-core 一起发布，位于 `catga_core::testing` 模块，无需额外 crate。

## HandlerSpy

包装请求处理器并记录每次调用，用于断言：

```rust
use catga_core::testing::HandlerSpy;
use catga_core::Handler;

// 包装真实处理器；也可用 HandlerSpy::with_action(|msg: Ping| async move { ... }) 免写处理器类型
let spy = HandlerSpy::new(PingHandler);

// spy 本身就是 Handler：照常注册或调用
spy.handle(Ping).await?;

// 断言
assert_eq!(spy.call_count(), 1);
assert_eq!(spy.last_call(), Some(Ping));
```

## EventHandlerSpy

事件处理器测试：

```rust
use catga_core::testing::EventHandlerSpy;
use catga_core::EventHandler;

let spy = EventHandlerSpy::<UserCreated>::new();        // 仅记录，无副作用
// 或 EventHandlerSpy::with_handler(real_projection)    记录后委托给真实处理器

spy.handle(UserCreated { id: "1".into() }).await?;

// 验证事件被处理
assert_eq!(spy.call_count(), 1);
```

## FlowTestContext

durable Flow 运行时测试的内存依赖（隔离的挂起流存储 + 确定性调度器）：

```rust
use catga_core::testing::FlowTestContext;

let ctx = FlowTestContext::new();

// 取出克隆，直接用于构造被测的 FlowRuntime
let suspended = ctx.suspended_flows();  // Arc<MemorySuspendedFlows>
let scheduler = ctx.scheduler();        // Arc<MemoryFlowScheduler>
```

## 集成测试

`CatgaTestHarness` 构建 typed 的进程内测试环境：注册阶段与执行阶段分离，并自动捕获消息：

```rust
use catga_core::testing::CatgaTestHarness;
use catga_core::CatgaResult;

#[tokio::test]
async fn test_order_workflow() -> CatgaResult<()> {
    let mut harness = CatgaTestHarness::new()?;
    harness.register_captured_request::<CreateOrder, _>(CreateOrderHandler)?;
    harness.capture_event::<OrderCreated>(); // 捕获每次发布（可以没有应用处理器）

    let running = harness.start();

    // 执行命令
    running.mediator().send(CreateOrder { /* ... */ }).await?;

    // 断言捕获的消息
    assert_eq!(running.consumed_of::<CreateOrder>().len(), 1);
    assert_eq!(running.published_of::<OrderCreated>().len(), 1);

    Ok(())
}
```

## 消息捕获

`MessageCapture` 是并发安全的消息记录器，供自定义断言使用：

```rust
use catga_core::testing::MessageCapture;

let capture = MessageCapture::<UserCreated>::default();

capture.record_published(UserCreated { id: "1".into() });

assert_eq!(capture.published().len(), 1);
assert!(capture.consumed().is_empty());
capture.clear();
```

## 断言辅助

`assert_success` / `assert_failure` / `assert_value` / `assert_error_code` 是普通函数（不是宏）：

```rust
use catga_core::testing::{assert_error_code, assert_success};
use catga_core::ErrorCode;

let value = assert_success(handler.handle(msg).await);                          // 成功：返回 T
let err = assert_error_code(handler.handle(msg).await, ErrorCode::Conflict);    // 失败且错误码匹配：返回 CatgaError
```
