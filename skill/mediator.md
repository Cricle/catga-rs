# Mediator：消息、处理器与派发

`catga-core` 的进程内 CQRS 派发核心：`Message` → `Registry`（启动时注册）→ `Mediator`（运行时派发）。

## 1. 消息 trait

```rust,ignore
// 所有消息的基 trait：Send + Sync + 'static
pub trait Message: Send + Sync + 'static {
    fn message_type(&self) -> &'static str { std::any::type_name::<Self>() } // 默认稳定类型名
    fn schema_version(&self) -> u32 { 1 }                                    // 演化的消息覆盖
    fn priority(&self) -> MessagePriority { MessagePriority::Normal }
}

pub trait Request: Message { type Response: Send + 'static; } // 有响应
pub trait Command: Message {}                                  // 无响应
pub trait Event: Message + Clone {}                            // 扇出，必须 Clone
```

两种声明方式：

```rust,ignore
// 方式一：手写 impl（无依赖，最常用）
struct GetBalance { account_id: u64 }
impl Message for GetBalance {}
impl Request for GetBalance { type Response = u64; }

#[derive(Clone)]
struct TransferCompleted { /* ... */ }
impl Message for TransferCompleted {}
impl Event for TransferCompleted {}

// 方式二：derive（catga_core 重导出 catga_macros 的 Message derive）
use catga_core::Message;
#[derive(Message)]
#[catga(priority = high)]              // 可选：静态传输优先级 low/normal/high/critical
struct RebuildSearchIndex {
    #[catga(trace_tag)]                // 可选：结构化 tracing 标签（显式 opt-in，隐私安全）
    tenant: String,
}
// 其他 derive 属性：
// #[catga(schema_version = 2)]
// #[catga(batch_key = "field_name")]        → 实现 BatchKeyProvider
// #[catga(authorize, roles("admin"), policy("p"))] → 实现 AuthorizedRequest
// #[catga(trace_tag = "name")] / #[catga(trace_tags(prefix = "x.", include = [...], exclude = [...]))]
```

## 2. 处理器 trait

所有处理器用 `#[async_trait]` 实现，`handle` 返回 `CatgaResult`：

```rust,ignore
use async_trait::async_trait;
use catga_core::{CatgaResult, CommandHandler, EventHandler, Handler};

struct BalanceHandler;
#[async_trait]
impl Handler<GetBalance> for BalanceHandler {                 // Request → CatgaResult<M::Response>
    async fn handle(&self, query: GetBalance) -> CatgaResult<u64> { Ok(query.account_id * 1000) }
}

struct TransferHandler;
#[async_trait]
impl CommandHandler<TransferFunds> for TransferHandler {      // Command → CatgaResult<()>
    async fn handle(&self, cmd: TransferFunds) -> CatgaResult<()> { /* ... */ Ok(()) }
}

#[derive(Clone)]
struct AuditLogger;
#[async_trait]
impl EventHandler<TransferCompleted> for AuditLogger {        // Event → CatgaResult<()>
    async fn handle(&self, event: TransferCompleted) -> CatgaResult<()> { /* ... */ Ok(()) }
}
```

闭包快捷方式（无需定义结构体；`*_with` 版本显式传入可克隆上下文，适合共享 `Arc` 依赖）：

```rust,ignore
use catga_core::{command_handler, event_handler, request_handler, request_handler_with};

request_handler(|value: Double| async move { Ok(value.0 * 2) })
request_handler_with(Arc::new(2u64), |factor: Arc<u64>, value: Double| async move {
    Ok(value.0 * *factor)
})
command_handler(|cmd: Credit| async move { Ok(()) })
event_handler(|evt: Credited| async move { Ok(()) })
// command_handler_with / event_handler_with 同理
```

## 3. 注册

### `catga_handlers!` 宏（推荐）

构建 `CatgaResult<Registry>`；语法为分号分隔的条目，event 的处理器是方括号列表：

```rust,ignore
let mediator = Mediator::new(catga_handlers! {
    request GetBalance => BalanceHandler;
    command TransferFunds => TransferHandler;
    event TransferCompleted => [AuditLogger, ProjectionHandler];
}?);
```

- 同一 request/command 重复注册：宏在编译期报错（运行时 `Registry` 也会返回 `ErrorCode::Conflict`）。
- event 至少需要一个处理器。

### 手动 `Registry`（动态组合时用）

```rust,ignore
let mut registry = Registry::new();
registry.register_request::<GetBalance, _>(BalanceReader)?;   // 重复 → Conflict
registry.register_command::<Credit, _>(CreditWriter)?;
registry.register_event::<BalanceChanged, _>(BalanceProjection); // 可注册多个
let mediator = Mediator::new(registry);
```

## 4. 派发（`Mediator`）

```rust,ignore
mediator.send(GetBalance { account_id: 42 }).await?;          // Request → M::Response
mediator.send_command(TransferFunds { .. }).await?;           // Command → ()
mediator.publish(TransferCompleted { .. }).await?;            // Event → 扇出到全部处理器

// 批量（同一消息类型）：最多 MAX_MEDIATOR_BATCH_SIZE = 1024 条
let responses = mediator.send_batch(vec![req1, req2]).await?;
// 无界流式场景用 send_stream

// 经过 typed 管道（见 pipeline.md）
mediator.send_with(request, &pipeline).await?;
mediator.send_command_with(command, &command_pipeline).await?;

// 协作式取消（tokio_util::sync::CancellationToken）
mediator.send_with_cancellation(request, token.clone()).await?;
// send_command_with_cancellation / publish_with_cancellation 同理
```

- handler panic 在 unwind 策略下被隔离为 `ErrorCode::Internal`；`panic = "abort"` 构建直接终止。
- `Mediator` 不可变且可安全地包在 `Arc` 里跨任务共享。

### `MediatorHandle`：启动期延迟绑定

启动时构造的组件需要 mediator，但 mediator 还没建好时用：

```rust,ignore
let handle = MediatorHandle::new();          // 克隆进各个 handler
// ... 构建 registry / mediator ...
handle.bind(Arc::new(mediator))?;            // 恰好绑定一次；二次绑定 → Conflict
handle.send(request).await?;                 // 绑定前调用 → ErrorCode::Unavailable
```

## 5. `catga_typed_mediator!`：零分配派发

热路径、处理器集合启动时已知时使用。生成具体 struct，编译期单态化派发——无 `Box<dyn Any>`、无 downcast、无 vtable。比动态 `Mediator` 顺序快约 5.8×、并发快约 7.0×。

```rust,ignore
use catga_core::catga_typed_mediator;

catga_typed_mediator! {
    pub struct BankMediator;
    request GetBalance => BalanceHandler;
    command TransferFunds => TransferHandler;
    event TransferCompleted => [AuditLogger, MetricsHandler];
}

// new 的参数顺序与宏内声明顺序一致；event 处理器传数组
let mediator = BankMediator::new(BalanceHandler, TransferHandler, [AuditLogger, MetricsHandler]);
let balance = mediator.send(GetBalance { account_id: 42 }).await?;
mediator.send_command(TransferFunds { .. }).await?;
mediator.publish(TransferCompleted { .. }).await?;
```

**选择**：处理器在运行时注册、或必须跨异构边界共享 `Arc<Mediator>` → 动态 `Mediator`；启动时已知且追求极致吞吐 → typed mediator。
