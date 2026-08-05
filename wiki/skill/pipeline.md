# Pipeline：请求策略与内置 Behavior

管道在请求到达 handler 前组合横切策略（重试、超时、授权、校验等）。Behavior 是**调用方拥有的值**：在启动时构造一次，其状态（如熔断器）跨请求共享，不存在全局策略状态。

## 构建管道

```rust,ignore
use std::time::Duration;
use catga_core::{Pipeline, RetryBehavior, TimeoutBehavior, catga_pipeline};

// 返回 CatgaResult<Pipeline<M>>；级数超过 MAX_PIPELINE_DEPTH 会返回校验错误
let pipeline: Pipeline<GetOrder> = catga_pipeline!(
    GetOrder;
    RetryBehavior::new(2, Duration::from_millis(10)),
    TimeoutBehavior::new(Duration::from_secs(1)),
)?;

// 派发时显式传入
let response = mediator.send_with(request, &pipeline).await?;

// 命令对应物
use catga_core::{CommandPipeline, catga_command_pipeline};
let command_pipeline: CommandPipeline<Archive> = catga_command_pipeline!(Archive;)?;
mediator.send_command_with(command, &command_pipeline).await?;
```

- `catga_pipeline!(Type; b1, b2)` — Request 管道；`catga_command_pipeline!(Type; ...)` — Command 管道。
- 宏接受**已构造好的 behavior 表达式**，按顺序 `try_with` 组装；深度上限 `MAX_PIPELINE_DEPTH`。
- 自定义 Behavior：实现 `Behavior<M>`（Request）或 `CommandBehavior<C>`（Command），`handle(&self, message, next)` 中调用 `next.run(message).await` 继续链条。

## 内置 Behavior 速查

| Behavior | 构造 | 作用与要点 |
| --- | --- | --- |
| `RetryBehavior` | `RetryBehavior::new(max_retries, initial_delay)` | 有界指数退避重试；只重试 `is_retryable()` 且非 `Cancelled` 的错误。`with_jitter(..)` 可指定 `RetryJitter`（生产默认 full jitter；`RetryJitter::fixed` 用于确定性测试） |
| `TimeoutBehavior` | `TimeoutBehavior::new(duration)` | 单次尝试超时，超时返回 `ErrorCode::Timeout` |
| `ValidationBehavior` | `ValidationBehavior::new(validators)` | 在 handler 前运行 `Arc<dyn Validator<M>>` 列表；失败返回 `ErrorCode::Validation`。另有独立函数 `validate_required`/`validate_not_empty`/`validate_max_length`/`validate_min_length`/`validate_min_count`/`validate_positive`/`validate_range` |
| `AuthorizationBehavior` | `AuthorizationBehavior::new()` / `with_policies(..)` | 配合 `#[catga(authorize, roles(..), policy(..))]` 或 `AuthorizedRequest` 检查 `SecurityClaims`；未认证 `Unauthorized`、无权 `Forbidden` |
| `TracingBehavior` / `LoggingBehavior` | 默认构造 | 结构化 tracing / 日志；trace tag 需消息显式 opt-in（`#[catga(trace_tag)]`） |
| `CircuitBreakerBehavior` | `CircuitBreakerBehavior::new(failure_threshold, reset_timeout)?` 或 `CircuitBreakerOptions::builder(..).build()?` | 熔断；启动时构造一次以跨请求保留状态 |
| `IdempotencyBehavior` | `IdempotencyBehavior::new(store: Arc<dyn IdempotencyStore>, codec)` | 请求侧幂等去重（配合 `IdempotencyKey`） |
| `InboxBehavior` | `InboxBehavior::new(store: Arc<dyn InboxStore>, codec)` | 消费侧去重（配合 `InboxKey`） |
| `OutboxBehavior` | `OutboxBehavior::new(store)` | 请求成功后把 `OutboxEnvelope` 持久化到 `OutboxStore`，由应用自有的 `OutboxProcessor` 异步发布 |
| `CompensationBehavior` | `CompensationBehavior::new(mediator, factory)` | 失败时发布补偿消息 |
| `DeadLetterBehavior` | （配合 `DeadLetterEnvelope`/`DeadLetterStore`） | 失败消息进入死信 |
| `DistributedLockBehavior` | （配合 `DistributedLockKey` + `LeaseStore`） | 跨实例互斥执行 |
| `AutoBatchingBehavior` | `AutoBatchingBehavior::new(BatchOptions)?` 返回 `(behavior, runner)` | 把并发请求聚合成批量派发；`runner` 由应用任务驱动 |
| `CorrelationBehavior` / `FaultPublishingBehavior` | `CorrelationBehavior` / `FaultPublishingBehavior::new(publisher)` | 关联 ID 传播 / 失败事件发布 |

## 编写注意事项

1. **顺序敏感**：外层的先执行。典型顺序：授权 → 校验 → 幂等 → 重试 → 超时（重试包着超时，使每次尝试都有独立超时）。
2. **状态共享**：熔断器、批量器等有状态 behavior 必须启动时构造一次并 `Arc` 共享，不能每次请求新建。
3. **重试不保证安全**：`RetryBehavior` 只按错误码机械重试；副作用安全仍由幂等键/Inbox/Outbox 负责。
4. 消息需要 `Clone` 才能进 `RetryBehavior`（重试要再次派发同一消息）。
