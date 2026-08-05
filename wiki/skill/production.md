# 错误处理、幂等与生产检查清单

## CatgaResult 与 CatgaError

每个可失败 API 返回 `CatgaResult<T>`（= `Result<T, CatgaError>`）。用 `?` 传播；在**应用边界**按稳定类别与重试提示决策，不要匹配错误文本：

```rust,ignore
use catga_core::{CatgaError, CatgaResult, ErrorCode};

// 构造错误：CatgaError::new(code, message)，可选 .with_details(..)（≤ MAX_ERROR_DETAILS_BYTES = 1024）
return Err(CatgaError::new(ErrorCode::Validation, "an order must contain at least one item"));

// 边界处理
match result {
    Ok(value) => Ok(value),
    Err(error) if error.is_retryable() => {
        eprintln!("retry {}: {}", error.code().as_stable_str(), error.message());
        Err(error)
    }
    Err(error) => Err(error),
}
```

`CatgaError` 访问器：`code()` → `ErrorCode`、`message()`、`details()`、`is_retryable()`。

## ErrorCode 分类

| 类别 | 含义 | 可重试 |
| --- | --- | --- |
| `Validation` | 输入不满足校验规则 | 否 |
| `HandlerFailed` / `PipelineFailed` | 处理器/管道报告的分类失败 | 否 |
| `HandlerNotFound` | 消息类型未注册处理器 | 否 |
| `PersistenceFailed` / `LockFailed` | 持久化/锁失败（调用方无法推断幂等与所有权，**故意不可重试**） | 否 |
| `TransportFailed` | 传输通信失败，通常可安全重试 | **是** |
| `SerializationFailed` | 序列化/反序列化失败 | 否 |
| `NotFound` / `Conflict` | 资源不存在 / 与已持久化状态冲突（如重复注册、flow 身份已存在） | 否 |
| `Unauthorized` / `Forbidden` | 未认证 / 已认证但无权 | 否 |
| `Cancelled` | 工作在完成前被取消 | 否 |
| `Timeout` / `FlowTimeout` | 超过配置期限 | **是** |
| `FlowFailed` / `FlowCompensating` / `FlowCancelled` | durable flow 业务失败 / 补偿中 / 已取消（终态） | 否 |
| `Transient` | 契约上重试可能成功 | **是** |
| `Unavailable` | 组件暂时不接受/无法服务 | **是** |
| `Unsupported` | 没有已配置组件支持该操作（如后端不支持 nack） | 否 |
| `Internal` | 框架意外失败 | 否 |

- `code.as_stable_str()` → 稳定 wire 名（`"validation"`、`"conflict"` 等）；`ErrorCode::from_stable_str(..)` 解析。
- `code.http_status_u16()` → 约定 HTTP 状态码（框架无关，HTTP 适配器据此映射）。

## 重试与幂等准则

1. **Catga 不会自动让重试安全**。重试副作用前：选定幂等键 + 由谁去重（`IdempotencyStore` / `InboxStore` / durable flow 步骤键）。
2. 只重试 `is_retryable()` 为真的类别；`RetryBehavior` 已内置此判断。
3. durable flow 步骤、传输投递、超时恢复都是 **at-least-once**：消费者必须容忍重复。
4. 抖动策略：生产用 `RetryJitter::production_default()`（full jitter）；确定性测试用 `RetryJitter::fixed(duration)`。

## 生产检查清单

1. **保持外部副作用幂等**：flow 重试、传输重投、超时恢复都是刻意的 at-least-once 边界。
2. **最小 feature 集**：只为实际部署的服务启用 Cargo feature，不要默认启用所有适配器。
3. **迁移先行**：受控启动阶段运行 store 的 `migrate()`，再在应用自有的监督任务里跑调度器与接收循环。
4. **有限超时与有界批量**：Redis 命令适配器默认有限响应超时；长轮询相互隔离。
5. **Raft HTTP 入口鉴权**：在 `raft_message_route` 前置 mTLS 或签名帧认证，附加已验证的 `RaftPeerIdentity`，并用本节点与可信 peer 配置 `StaticRaftInboundPolicy`。

可用性、凭据、重试预算、优雅关停都由**调用方**拥有——这是设计使然。

## 生命周期与关停（`catga-core`）

- `TransportLifecycle`：单个传输的显式模式——`initialize` → 停止接收 → 有界 drain（`TransportLifecycleOptions`）。
- `ShutdownCoordinator` / `OperationTracker` / `AcceptanceGate`：协调优雅关停。
- `RecoveryManager` / `RecoverableComponent`（`AutoRecoveryOptions`）：组件恢复。
- `HealthCheckable`：健康检查契约。
- 协作式取消：`scope_cancellation` / `current_cancellation`（task-local `CancellationToken`）。

## 可观测性

- OpenTelemetry 兼容的 tracing/metrics 来自公开 crate API；`TRACING_TARGET` 是结构化事件的 target。
- trace tag 需要消息显式 opt-in（`#[catga(trace_tag)]`），默认不导出任何应用数据（隐私安全）。
- 关联：`CorrelationBehavior` / `scope_correlation_id` / `current_correlation_id`；`TraceContext`（W3C `traceparent`/`tracestate`）。
- 观测用 tracing/metrics；仓库刻意不提供内置 HTTP 健康端点（健康状态由 `HealthCheckable` 契约自行暴露）。

## 测试工具（`catga-testing`）

进程内 typed 测试构件（每个测试用例构造新实例，不跨并发用例共享）：

- `CatgaTestHarness` / `RunningCatgaTestHarness` — 启动前注册 handler，启动后获得 mediator。
- `HandlerSpy::new(handler)` / `with_action(..)` / `without_handler()` — 记录请求供断言（`calls()` / `call_count()` / `last_call()`）；`EventHandlerSpy` 同理（`new()` 纯记录 / `with_handler(..)` 记录后委托真实 handler）。
- `FlowTestContext::new()` — 隔离的 flow 持久化依赖（`suspended_flows()` 等共享同一 Arc）。
- `AggregateScenario` / `ReplayedAggregate` — 聚合事件重放测试。
- `MessageCapture<T>` — 并发安全的发布/消费捕获（`record_published` / `record_consumed` / `published()` / `consumed()`）。
- 断言助手：`assert_success(..)` / `assert_failure(..)` / `assert_value(..)` / `assert_contains(..)` / `assert_error_code(..)`。

边界：这些工具模拟 Catga 契约而非生产部署；调度/传输行为用各适配器自己的集成测试覆盖。

## 验证命令（本仓库 / 使用方均可参考）

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --doc --workspace --all-features
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps
```

性能基准**必须**用 release 模式：`cargo test --release ... -- --ignored --nocapture`。

## 外部服务测试（E2E）

- NATS 测试在未设 `CATGA_NATS_URL` 时自动启动 Testcontainers 隔离实例；设置后指向外部服务。
- Redis/MySQL/PostgreSQL/SQL Server 测试是 `#[ignore]` 标记的真实服务 E2E：提供对应 `CATGA_*_URL` 并用 `-- --ignored` 运行。
- 本地完整 E2E：`scripts/e2e.sh --profile core|sql|full`（Docker Compose 见 `testing/docker/compose.yaml`；`CATGA_CONTAINER_IMAGE_PREFIX` 可指向内部/国内镜像仓库）。
