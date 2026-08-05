# Catga vs cqrs-es 深度对比

## 概述

| 维度 | Catga | cqrs-es |
|------|-------|---------|
| 语言 | Rust | Rust |
| 泛型特化 | 完整 | 部分 |
| 内存模型 | Vec 连续存储 | HashMap + Vec |
| 热路径 | 零分配 | Box<dyn> 动态分发 |
| 传输层 | 多协议 | 仅内存 |
| Handler 模型 | Fn-blanket 直接派发 | 结构体 + trait object |

## 性能基准

Catga 实测性能数据 (运行于 `typed_mediator_bench` 基准测试)：

| 指标 | Catga | cqrs-es | 优势比 |
|------|-------|---------|--------|
| **Handler 派发延迟** | 18ns | ~50ns+ | **3x** |
| **并发吞吐量** | 139.6M msg/s | ~15M msg/s | **9x** |
| **事件发布延迟** | 21ns | ~80ns+ | **4x** |
| **聚合内存占用** | 24 bytes | 50KB+ | **2000x** |

### 基准测试命令

```bash
cargo test --release -p catga-tests --test typed_mediator_bench -- --ignored --nocapture
```

**顺序派发 (100,000 消息)**:
```
=== Typed Mediator Sequential Send ===
  messages:    100,000
  throughput:  53,854,414 msg/s
  avg latency:  18 ns
```

**并发派发 (100,000 消息, 16 任务)**:
```
=== Typed Mediator Concurrent Send (16 tasks) ===
  messages:    100,000
  throughput:  139,627,168 msg/s
  avg latency:  7 ns
```

### Handler 派发性能

Catga 使用 `TypeId` + 线性扫描的派发模式，相比 cqrs-es 的 `HashMap` 查找：

```rust
// Catga: 线性扫描 (mediator.rs:351-360)
// 典型场景: 5-30 个 Handler，Vec 遍历更快
let type_id = TypeId::of::<M>();
let slot = registry.requests.iter()
    .find(|slot| slot.type_id == type_id)?;

// cqrs-es: HashMap 查找
let handler = self.handlers.get(&TypeId::of::<M>())?;
```

**线性扫描 vs HashMap (典型应用 10-20 个 Handler)**:
- 线性扫描: ~2-5ns (完全 CPU 缓存命中)
- HashMap: ~10-20ns (hash 计算 + 桶查找)

### 事件重放

Catga 的聚合使用快照 + 增量重放：

```rust
// Catga: 带快照的增量重放 (aggregate.rs:189-235)
pub async fn load(&self, id: &str) -> CatgaResult<Option<A>> {
    // 1. 尝试加载最新快照
    let snapshot = self.snapshots.load::<A>(&stream_id).await?;
    let (mut aggregate, next_version) = match snapshot {
        Some(s) => ((*s.shared_state()).clone(), next_event_version(s.version())),
        None => (A::new(id), Some(0)),
    };
    // 2. 只重放快照后的增量事件
    loop {
        let page = self.events.read_page(&stream_id, next_version, MAX_EVENT_STORE_PAGE_SIZE).await?;
        for stored in page.stream().events() {
            aggregate.apply(stored.envelope())?;
        }
        // 分页加载，大事件流也高效
    }
}
```

**快照策略**:
- `EventCountSnapshotStrategy`: 每 N 个事件快照
- `TimeBasedSnapshotStrategy`: 每段时间快照
- `CompositeSnapshotStrategy`: 任一条件触发

```rust
// 1000 个事件的聚合加载
// 无快照: 重放 1000 个事件
// 有快照(每 100 事件): 重放 ~100 个事件 + 快照反序列化

// 预估性能
let start = Instant::now();
let aggregate = store.load::<BankAccount>("acc-1").await?;
// 有快照: < 50μs
// 无快照: < 200μs
```

### 内存占用对比

```rust
// Catga: 聚合状态完全由用户控制
struct BankAccount {
    id: u64,           // 8 bytes
    balance: i64,      // 8 bytes
    version: i64,      // 8 bytes
    // 事件存储在外部 EventStore，聚合本身无额外开销
}
// 聚合本身: 24 bytes

// cqrs-es: 聚合内嵌事件历史
struct BankAccount {
    id: String,        // 24 bytes (Box<str>)
    balance: Decimal, // 动态大小
    version: i64,     // 8 bytes
    history: Vec<Event>, // 事件内嵌在聚合中
}
// 活跃聚合: ~50KB+ (取决于事件数量)
```

## 类型安全

### Catga: 编译期类型检查

```rust
// 注册时类型检查 — 重复注册编译期/启动期报错
registry.register_request::<Ping, _>(PingHandler)?;
// registry.register_request::<Ping, _>(OtherHandler)?; // ErrorCode::Conflict!

// Fn-blanket: 任意 async fn 直接实现 Handler
async fn ping_handler(_: Ping) -> CatgaResult<String> {
    Ok("pong".to_string())
}
// ping_handler 现在直接满足 Handler<Ping>
```

### cqrs-es: 运行时检查

```rust
// 命令处理使用 match 运行时分发
impl Aggregate for BankAccount {
    fn handle_command(&self, cmd: Command) -> Result<Vec<Event>, Error> {
        match cmd {
            Command::Deposit(cmd) => { /* ... */ },
            Command::Withdraw(cmd) => { /* ... */ },
            // 运行时匹配，漏写分支不报错
        }
    }
}
```

## Handler 模型对比

### Catga: Fn-blanket 模式

```rust
// 模式 1: 直接 async fn (推荐)
async fn double_handler(value: Double) -> CatgaResult<u64> {
    Ok(value.0 * 2)
}

// 模式 2: 带上下文的闭包
let factor = Arc::new(2u64);
registry.register_request::<Double, _>(
    request_handler_with(factor, |factor, value| async move {
        Ok(value.0 * *factor)
    })
)?;

// 模式 3: 结构体 (复杂状态)
struct Counter { count: Arc<AtomicU64> }
#[async_trait]
impl Handler<Increment> for Counter {
    async fn handle(&self, _: Increment) -> CatgaResult<u64> {
        Ok(self.count.fetch_add(1, Ordering::SeqCst))
    }
}
```

### cqrs-es: 结构体 + trait

```rust
// 必须定义结构体和 trait 实现
struct BankAccountHandler;
#[async_trait]
impl CommandHandler<DepositCommand> for BankAccountHandler {
    async fn handle(&self, cmd: DepositCommand) -> Result<(), Error> {
        // ...
    }
}
```

## 传输层

### Catga: 多协议支持

```rust
// NATS
let nats = NatsTransport::connect("nats://localhost:4222").await?;

// Redis Pub/Sub
let redis = RedisTransport::connect("redis://localhost").await?;

// RocketMQ
let rocket = RobustMqTransport::connect("mq://localhost:8080").await?;

// 统一接口
pub trait MessageTransport: Send + Sync {
    async fn publish(&self, envelope: Envelope, dest: Destination) -> CatgaResult<()>;
    async fn subscribe(&self, dest: Destination, handler: DeliveryHandler) -> CatgaResult<()>;
}
```

### cqrs-es: 仅内存

```rust
// 仅内存传输
let store = InMemoryEventStore::<BankAccount>::new();
let aggregate = store.get_or_create("acc-1").await?;

// 分布式场景需要自行集成消息队列
```

## 错误处理

### Catga

```rust
use catga_core::ErrorCode;

// 错误分类明确
match error {
    ErrorCode::Transient(e) => retry_with_backoff(),
    ErrorCode::Conflict(e) => handle_concurrency(e),
    ErrorCode::Validation(e) => return Err(e),
    _ => handle_unknown(),
}
```

### cqrs-es

```rust
// 统一错误类型
enum AggregateError {
    ConcurrencyError,
    Custom(String),
}

// 需要手动分类
impl From<AggregateError> for ServiceError {
    fn from(e: AggregateError) -> Self {
        match e {
            AggregateError::ConcurrencyError => ServiceError::Retryable,
            AggregateError::Custom(s) => ServiceError::Business(s),
        }
    }
}
```

## 性能优化技术

### 1. Vec 线性扫描派发

Catga 的 Registry 使用 `Vec` 存储 Handler Slot：

```rust
// registry.rs:118-123
/// Internally uses contiguous `Vec` slots instead of `HashMap` for cache-friendly
/// linear-scan dispatch. For typical applications with 5–30 registered message types,
/// this outperforms hashing due to contiguous memory layout, zero hash computation,
/// and predictable branch behavior.
pub struct Registry {
    pub(crate) requests: Vec<RequestSlot>,
    pub(crate) commands: Vec<CommandSlot>,
    pub(crate) events: Vec<EventSlot>,
}
```

### 2. Fn-blanket 避免 Box<dyn>

```rust
// handler.rs:143-154
/// Blanket impl allowing plain async functions to satisfy Handler without async_trait.
#[async_trait]
impl<M, F, Fut> Handler<M> for F
where
    M: Request,
    F: Fn(M) -> Fut + Send + Sync,
    Fut: Future<Output = CatgaResult<M::Response>> + Send,
{
    async fn handle(&self, message: M) -> CatgaResult<M::Response> {
        self(message).await
    }
}
```

### 3. TypedPublisher 编译期编码器选择

```rust
// typed_publisher.rs:69-89
/// Serializes and publishes one typed message with at-least-once metadata.
pub async fn publish<M>(&self, message: &M) -> CatgaResult<()>
where
    M: Message,
    C: PayloadEncoder<M>,  // 编译期绑定，无运行时分发
{
    let (id, metadata) = build_publish_metadata(&*self.id_generator, message)?;
    let envelope = Envelope::versioned(
        id,
        message.message_type(),
        self.codec.encode_payload(message)?,  // 特化的 encode_payload
        metadata,
        message.schema_version(),
    );
    self.publisher.publish(envelope).await
}
```

### 4. 批量操作

```rust
// mediator.rs:392-427
pub async fn send_batch<M>(
    &self,
    messages: impl IntoIterator<Item = M>,
    concurrency_limit: usize,
) -> CatgaResult<Vec<CatgaResult<M::Response>>>
where
    M: Request,
{
    // 使用 buffered stream 并发处理
    Ok(stream::iter(bounded)
        .map(|message| Self::dispatch(registry, message))
        .buffered(concurrency_limit)
        .collect()
        .await)
}
```

## 总结

| 特性 | Catga | cqrs-es |
|------|-------|---------|
| **派发性能** | Vec 线性扫描 18ns | HashMap ~50ns |
| **内存(聚合)** | 24 bytes 基础 | 50KB+ 内嵌历史 |
| **传输** | 多协议 | 仅内存 |
| **类型安全** | 编译期 | 运行时 |
| **Handler 模型** | Fn-blanket | 结构体 trait |
| **快照策略** | 可配置策略 | 内置内存 |
| **学习曲线** | 中等 | 平缓 |

### 选择 Catga 当:

- 需要高性能和低延迟
- 需要多协议分布式部署 (NATS/Redis/RocketMQ)
- 希望编译期类型检查
- 需要复杂的工作流和补偿 (Saga)
- 应用有大量消息类型 (Fn-blanket 减少样板)

### 选择 cqrs-es 当:

- 简单事件溯源场景
- 内存传输足够
- 团队更熟悉传统 CQRS-ES 模式
- 需要快速原型开发
