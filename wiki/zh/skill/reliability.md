# 消息可靠性模式：Outbox / Inbox / 幂等 / 死信 / 订阅 / 消费循环

Catga 把「恰好一次」拆成显式的组合件：写侧 **Outbox**、读侧 **Inbox + 幂等**、终局 **死信**，循环由**应用拥有的任务**驱动。存储实现见 [stores.md](stores.md)。

## 1. Outbox（写侧可靠发布）

请求成功后先把 envelope 落库，再由处理器异步发布——避免「库已写、消息丢了」的双写问题。

```rust,ignore
use catga_core::{OutboxBehavior, OutboxEnvelope, OutboxProcessor, OutboxLoopOptions};

// 管道内：请求成功后持久化 envelope（实现 OutboxEnvelope 的消息）
let pipeline = catga_pipeline!(PlaceOrder; OutboxBehavior::new(outbox_store.clone()))?;

// 应用拥有的 worker：claim → publish → ack
let processor = OutboxProcessor::new(
    outbox_store,            // Arc<impl OutboxStore>
    transport,               // Arc<impl MessageTransport>
    "worker-1",              // owner 身份（claim 归属）
    64,                      // 每次扫描的批量（≤ MAX_OUTBOX_CLAIM_LIMIT）
)?;
// new_with_concurrency(.., concurrency_limit)：并发发布且逐条独立 ack/释放
processor.flush_once().await?;                 // 处理一批，返回 OutboxRun 统计
// 或持续循环（批次间观察取消；存储失败按 error_delay 退避）：
processor.run_until_cancelled(OutboxLoopOptions::new(scan_interval, error_delay)?, token).await?;
```

- `OutboxMessage::new(envelope)`，状态机 `OutboxState::Pending → ...`；重试上限 `DEFAULT_OUTBOX_MAX_RETRIES`，claim 租约 `DEFAULT_OUTBOX_CLAIM_LEASE`。
- `OutboxStore` 契约无事务边界——与 handler 自身持久化的原子性由 store 实现或应用保证。
- **定时 Outbox**：消息实现 `DelayedMessage`（`scheduled_at()` 优先于 `delay()`；`deliver_at(now)` 解析期限），配合 `MemoryPackScheduledOutbox` 持久化，到期后由处理器发布。声明本身不创建任何 timer。

## 2. Inbox 与幂等（读侧去重）

传输是 at-least-once，消费端必须去重：

- `InboxBehavior::new(store: Arc<dyn InboxStore>, codec)` — 管道内按 `InboxKey` 去重；claim 租约 `DEFAULT_INBOX_CLAIM_LEASE`，`ProcessingState` 记录处理状态。
- `IdempotencyBehavior::new(store: Arc<dyn IdempotencyStore>, codec)` — 按 `IdempotencyKey` 做请求侧幂等（保留期 `DEFAULT_IDEMPOTENCY_RETENTION`）。
- 选择：消息消费链路用 Inbox；对外 API/命令入口用 Idempotency。

## 3. 死信（终局隔离）

- `DeadLetterStore` 契约 + `DeadLetter` / `DeadLetterDiagnostics`；描述与阶段名有界（`MAX_DEAD_LETTER_DESCRIPTION_BYTES` / `MAX_DEAD_LETTER_STAGE_BYTES`）。
- `DeadLetterBehavior`（管道）或 `CompetingConsumer` 的死信策略（`max_attempts` 后入死信并 ack，阻止无限重投）。
- 死信是**运维入口**：应用应提供巡检与重放路径，而不是静默堆积。

## 4. 竞争消费循环

```rust,ignore
use catga_core::{CompetingConsumer, DeliveryHandler};

struct OrderWorker;
#[async_trait]
impl DeliveryHandler for OrderWorker {
    async fn handle(&self, envelope: &Envelope) -> CatgaResult<()> {
        // Ok(()) → ack；Err(..) → nack 请求重投（不停机）
    }
}

let consumer = CompetingConsumer::new(transport, Arc::new(OrderWorker), 8)?;  // 并发上限 > 0
let run: ConsumerRun = consumer.run_until_cancelled(cancellation_token).await?;
// run.received() / acknowledged() / rejected() / dead_lettered()
```

- 竞争消费组成员身份属于**传输配置**（Redis 消费组 / NATS durable consumer）：对同一配置起多个 runner 即成分布式竞争消费。
- ack 所有权在 consumer 而不在 handler——handler 无法在副作用完成前误确认。

## 5. 持久订阅（事件流 → 处理器）

```rust,ignore
use catga_core::{PersistentSubscription, SubscriptionLoopOptions};

// 匹配单个流、"prefix*" 前缀、或 "*" 全部流；可再按事件类型过滤
let subscription = PersistentSubscription::new("order-projection", "order-*")
    .with_event_types(["OrderCreated", "OrderShipped"]);

// SubscriptionRunner（单实例）或 CompetingSubscriptionRunner（多实例分摊）
// 由应用任务驱动；SubscriptionLoopOptions::new(poll_interval)?（非零）
```

- 每流 checkpoint：`SubscriptionCheckpoint` + `SubscriptionStore`（实现：`MemorySubscriptions` / `NatsSubscriptions` / `RedisSubscriptions`）。
- `SubscriptionRun` 报告每轮处理量；循环选项默认 100ms 轮询间隔。

## 6. 生命周期要点

1. 所有循环（`OutboxProcessor`、`CompetingConsumer`、`SubscriptionRunner`）都接受 `CancellationToken` 并由应用任务 spawn——关停顺序由你控制（先停接收 → drain → 再停存储）。
2. claim 租约到期会释放未完成工作供其他 worker 接管；处理逻辑必须幂等。
3. 保留期清理有界：`validate_retention_cleanup_limit` / `MAX_RETENTION_CLEANUP_LIMIT`。
