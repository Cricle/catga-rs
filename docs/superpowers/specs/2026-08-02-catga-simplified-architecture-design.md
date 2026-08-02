# catga-rs 简化架构设计

> **目标**: 简化框架，降低学习曲线，保持扩展性

## 架构概览

```
用户代码
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│                     catga-auto                          │
│              (入口 + Transport 自动发现)                  │
└─────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│                     catga-core                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │
│  │  Transport  │  │   Mediator  │  │  Fn-blanket     │  │
│  │   (trait)   │  │             │  │  impls          │  │
│  └─────────────┘  └─────────────┘  └─────────────────┘  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │
│  │  Request    │  │  Command    │  │     Event       │  │
│  │  Command    │  │  Event      │  │                 │  │
│  │  Event      │  │             │  │                 │  │
│  └─────────────┘  └─────────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────┘
    │                         ▲
    ▼                         │
┌─────────────┐               │
│ catga-macros│───────────────┘
└─────────────┘
```

## 消息类型简化 (7 → 3)

### 当前状态

```rust
// 当前 7 种消息类型
trait Message { ... }
trait Request: Message { ... }
trait Command: Message { ... }
trait Event: Message { ... }
trait DelayedMessage: Message { ... }
trait DelayedRequest: Request { ... }
trait DelayedEvent: Event { ... }
```

### 简化后 (3 种消息类型) - 编译时类型安全

采用**类型标记（TypeId）** 模式实现编译时类型安全，避免手写字符串：

```rust
use std::marker::PhantomData;

/// 编译时类型安全的消息类型标记
pub trait MessageTypeId: 'static {
    const NAME: &'static str;
}

/// 请求-响应消息，带延迟支持
pub trait Request: Clone + Send + Sync + 'static {
    type Response: Clone + Send + Sync + 'static;
    type TypeId: MessageTypeId;
}

/// 命令消息（单向），带延迟支持
pub trait Command: Clone + Send + Sync + 'static {
    type TypeId: MessageTypeId;
}

/// 事件消息（发布-订阅），带延迟支持
pub trait Event: Clone + Send + Sync + 'static {
    type TypeId: MessageTypeId;
}
```

### derive 宏生成的类型标记

```rust
// 用户代码
#[derive(catga_request)]
struct GetUser(String);

// 宏自动生成（用户不可见）
mod __catga_types {
    pub struct GetUserTypeId;
    impl crate::MessageTypeId for GetUserTypeId {
        const NAME: &'static str = "GetUser";
    }
}

// 用户使用
impl Request for GetUser {
    type Response = String;
    type TypeId = __catga_types::GetUserTypeId;
}

// 获取类型名（编译时确定）
let name = <GetUser as Request>::TypeId::NAME;  // "GetUser"
```

### 好处

1. **编译时唯一性** - 每个消息类型对应唯一 TypeId
2. **无手写错误** - derive 宏自动生成，无需手动写字符串
3. **编译时路由匹配** - 泛型约束保证类型安全
4. **运行时友好** - `TypeId::NAME` 只是读取常量

### 延迟消息

延迟通过方法签名表达，而非独立 trait：

```rust
// 当前
trait DelayedRequest: Request { ... }

// 简化后：延迟是方法签名的一部分
impl catga_core::Transport for MyTransport {
    async fn send_delayed<M: Request>(
        &self,
        msg: M,
        delay: Duration,
    ) -> CatgaResult<M::Response>;
}
```

## Transport 抽象

### 核心 Trait

```rust
/// Transport 是框架的抽象传输层
/// 第三方实现此 trait 即可扩展
pub trait Transport: Send + Sync {
    /// 发送请求并等待响应
    async fn send<M: Request>(
        &self,
        msg: M,
    ) -> CatgaResult<M::Response>;

    /// 发送命令（单向）
    async fn send_command<M: Command>(
        &self,
        cmd: M,
    ) -> CatgaResult<()>;

    /// 发布事件（广播）
    async fn publish<M: Event>(
        &self,
        event: M,
    ) -> CatgaResult<()>;

    /// 延迟发送请求
    async fn send_delayed<M: Request>(
        &self,
        msg: M,
        delay: Duration,
    ) -> CatgaResult<M::Response>;

    /// 延迟发送命令
    async fn send_command_delayed<M: Command>(
        &self,
        cmd: M,
        delay: Duration,
    ) -> CatgaResult<()>;

    /// 延迟发布事件
    async fn publish_delayed<M: Event>(
        &self,
        event: M,
        delay: Duration,
    ) -> CatgaResult<()>;
}
```

### 内置实现

```rust
// catga-nats
pub struct NatsTransport { ... }
impl catga_core::Transport for NatsTransport { ... }

// catga-redis
pub struct RedisTransport { ... }
impl catga_core::Transport for RedisTransport { ... }
```

### 一行切换

```rust
use catga_core::{Transport, NatsTransport, RedisTransport};

// 单机模式
#[catga_main(transport = LocalTransport::default())]
// 或分布式
#[catga_main(transport = NatsTransport::new("nats://localhost:4222"))]
// 或 Redis
#[catga_main(transport = RedisTransport::new("redis://localhost"))]

async fn main() -> CatgaResult<()> { ... }
```

## Handler 简化

### Fn-blanket 实现（保持不变）

```rust
// 用户只需写函数，无需实现 trait
async fn get_user(msg: GetUserRequest) -> Result<GetUserResponse, MyError> {
    // 业务逻辑
}

// 自动注册
impl<M: Request> Handler<M> for impl Fn(M) -> ... { ... }
```

### Handler Trait（内部使用）

```rust
/// 内部 handler 接口，供 Transport 调用
pub trait Handler<M>: Send + Sync {
    async fn handle(&self, msg: M) -> CatgaResult<...>;
}
```

## Crate 结构 (15 → 6)

### 最终结构

```
catga-rs/
├── catga-core/          # 所有抽象 + Transport trait
│   ├── message.rs       # Request, Command, Event traits
│   ├── handler.rs       # Handler trait + fn-blanket impls
│   ├── transport.rs     # Transport trait (核心抽象)
│   ├── mediator.rs      # 请求路由
│   ├── error.rs         # 错误类型
│   └── lib.rs
│
├── catga-macros/        # 派生宏
│   ├── catga_request    # Request derive
│   ├── catga_command    # Command derive
│   ├── catga_event      # Event derive
│   ├── catga_handler    # Handler derive (可选)
│   └── catga_auto       # AutoAppBuilder derive
│
├── catga-auto/          # 自动发现入口
│   └── lib.rs
│
├── catga-nats/          # NATS 传输实现
│   └── lib.rs
│
├── catga-redis/         # Redis 传输实现
│   └── lib.rs
│
└── catga-local/         # 单机内存实现（默认）
    └── lib.rs
```

### 依赖关系

```
catga-auto
    └── catga-core
    └── catga-macros

catga-nats
    └── catga-core
    └── nats.rs (第三方)

catga-redis
    └── catga-core
    └── redis (第三方)

catga-local
    └── catga-core
```

## 用户 API 设计

### 最小示例

```rust
use catga_core::*;

// 1. 定义消息
#[derive(Clone, Debug, catga_request(response = String))]
struct GetUser(String);

#[derive(Clone, Debug, catga_command)]
struct UpdateUser { id: String, name: String }

#[derive(Clone, Debug, catga_event)]
struct UserUpdated { id: String, name: String }

// 2. 写 handler（普通 async 函数）
async fn get_user_handler(msg: GetUser) -> Result<String, AppError> {
    Ok(format!("User: {}", msg.0))
}

async fn update_user_handler(
    cmd: UpdateUser,
    transport: &impl Transport,
) -> Result<(), AppError> {
    // 发送命令
    transport.send_command(cmd.clone()).await?;
    // 发布事件
    transport.publish(UserUpdated { id: cmd.id, name: cmd.name }).await?;
    Ok(())
}

// 3. 启动应用（一行配置）
#[catga_main(transport = NatsTransport::new("nats://localhost:4222"))]
async fn main() -> CatgaResult<()> {
    // handlers 自动注册
    Ok(())
}
```

### 本地开发模式

```rust
#[catga_main(transport = LocalTransport::default())]
async fn main() -> CatgaResult<()> {
    // 完全相同代码，只是 transport 不同
    Ok(())
}
```

## 第三方扩展

### 实现 Transport trait

```rust
use catga_core::Transport;

// 第三方 Kafka 实现
pub struct KafkaTransport { ... }

impl Transport for KafkaTransport {
    async fn send<M: Request>(&self, msg: M) -> CatgaResult<M::Response> {
        // Kafka 请求-响应实现
    }

    async fn send_command<M: Command>(&self, cmd: M) -> CatgaResult<()> {
        // Kafka 命令发送
    }

    async fn publish<M: Event>(&self, event: M) -> CatgaResult<()> {
        // Kafka 事件发布
    }

    // ... 其他方法
}

// 使用
#[catga_main(transport = KafkaTransport::new(...))]
async fn main() -> CatgaResult<()> { ... }
```

## 迁移策略

### Phase 1: Core 重构
1. 创建 `transport.rs`，定义 Transport trait
2. 简化 message.rs（7 → 3 traits）
3. 保持 handler.rs 不变

### Phase 2: 实现分离
1. 将 catga-nats, catga-redis, catga-local 改为实现 Transport trait
2. 更新 catga-auto 使用 Transport

### Phase 3: 清理
1. 删除冗余抽象
2. 更新文档
3. 更新示例

## 关键设计决策

1. **Transport 作为核心抽象**: 所有传输实现遵循相同接口
2. **消息类型简化为 3**: Request, Command, Event（延迟通过方法签名表达）
3. **Fn-blanket 保持**: 用户写普通函数，无需实现 trait
4. **库隔离**: 用户只导入需要的 crate
5. **最小依赖**: Transport 实现可以独立演进

## 待定事项

- [ ] 确认 Transport trait 方法签名
- [ ] 确认延迟消息的设计（方法签名 vs 独立 trait）
- [ ] 确认 catga-auto 如何发现 handlers
