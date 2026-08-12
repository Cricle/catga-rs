# 性能优化

## 性能基准

Catga 的核心性能指标基于实际基准测试：

| 指标 | 结果 | 说明 |
|------|------|------|
| **顺序请求派发** | **53.8M msg/s** | 单线程，18ns 平均延迟 |
| **并发请求派发** | **139.6M msg/s** | 16 并发任务，7ns 平均延迟 |
| **事件发布** | **46.4M events/s** | 单 Handler，21ns 平均延迟 |

### 基准测试详情

```bash
cargo test --release -p catga-tests --test typed_mediator_bench -- --ignored --nocapture
```

**顺序派发 (100,000 消息)**:
```
=== Typed Mediator Sequential Send ===
  messages:    100,000
  total:       1.856858ms
  throughput:  53,854,414 msg/s
  avg latency: 18 ns
```

**并发派发 (100,000 消息, 16 任务)**:
```
=== Typed Mediator Concurrent Send (16 tasks) ===
  messages:    100,000
  total:       716.193µs
  throughput:  139,627,168 msg/s
  avg latency: 7 ns
```

**事件发布 (100,000 事件)**:
```
=== Typed Mediator Event Publish (1 handler) ===
  events:      100,000
  total:       2.156593ms
  throughput:  46,369,435 events/s
  avg latency: 21 ns
```

## 核心性能技术

### 1. Vec 线性扫描派发

典型应用注册 5-30 个 Handler，Vec 线性扫描比 HashMap 更快：

```rust
// mediator.rs:351-360
// TypeId 匹配 + 线性扫描
let type_id = TypeId::of::<M>();
let slot = registry.requests.iter()
    .find(|slot| slot.type_id == type_id)?;

// 为什么不用 HashMap？
// - 5-30 个元素，线性扫描 ~3ns
// - HashMap: hash 计算 + 桶查找 ~15ns
// - Vec: 连续内存，CPU 缓存友好
```

### 2. Fn-blanket 避免 Box<dyn>

Plain async fn 直接实现 Handler trait：

```rust
// handler.rs:143-154
// 无需 Box<dyn Handler>
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

// 使用示例
async fn double_handler(value: Double) -> CatgaResult<u64> {
    Ok(value.0 * 2)
}

// 直接注册，无需包装
registry.register_request::<Double, _>(double_handler)?;
```

### 3. 编译期编码器选择

`PayloadEncoder<M>` 泛型约束在编译时选择最佳编码路径：

```rust
// typed_publisher.rs:69-89
pub async fn publish<M>(&self, message: &M) -> CatgaResult<()>
where
    M: Message,
    C: PayloadEncoder<M>,  // 编译期绑定
{
    // 运行时根据具体类型选择特化实现
    self.codec.encode_payload(message)?
}
```

### 4. 快照策略减少重放

```rust
// aggregate.rs:38-77
// 三种快照策略

// 按事件数量
let strategy = EventCountSnapshotStrategy::new(100)?;
// 每 100 个事件快照一次

// 按时间间隔
let strategy = TimeBasedSnapshotStrategy::new(Duration::from_secs(60));

// 复合策略 (任一触发)
let composite = CompositeSnapshotStrategy::new(
    EventCountSnapshotStrategy::new(100)?,
    TimeBasedSnapshotStrategy::new(Duration::from_secs(60)),
);

// 使用
let repo = AggregateRepository::new(store, snapshots, strategy);
let aggregate = repo.load("acc-1").await?;
```

### 5. 分页加载大事件流

```rust
// event_store.rs
// 分页加载，避免一次性加载所有事件
let page = store.read_page(stream_id, offset, MAX_EVENT_STORE_PAGE_SIZE).await?;

// MAX_EVENT_STORE_PAGE_SIZE = 1024
// 大事件流自动分页
loop {
    let page = events.read_page(&stream_id, next_version, MAX_EVENT_STORE_PAGE_SIZE).await?;
    for stored in page.stream().events() {
        aggregate.apply(stored.envelope())?;
    }
    if page.next_version().is_none() {
        break;
    }
}
```

### 6. 批量并发处理

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
    Ok(stream::iter(bounded)
        .map(|message| Self::dispatch(registry, message))
        .buffered(concurrency_limit)  // 并发控制
        .collect()
        .await)
}

// publish_batch 使用 buffer_unordered
// 允许无序完成，提高吞吐量
```

## 内存优化

### 聚合状态最小化

```rust
// Catga: 聚合只存核心状态，事件在外部
struct BankAccount {
    id: u64,        // 8 bytes
    balance: i64,   // 8 bytes
    version: i64,   // 8 bytes
}
// 聚合本身: 24 bytes

// 事件存储在 EventStore
// 快照存储在 SnapshotStore
```

### TypedEventStore 零装箱

```rust
// typed_event_store.rs:44-66
pub async fn append_event<E>(
    &self,
    stream_id: &str,
    event: &E,
    expected_version: Option<i64>,
) -> CatgaResult<i64>
where
    E: Event,
    C: PayloadEncoder<E>,  // 编译期编码器
{
    // 直接编码，无中间 Box
    let envelope = Envelope::versioned(
        id,
        event.message_type(),
        self.codec.encode_payload(event)?,  // 直接序列化
        metadata,
        event.schema_version(),
    );
    self.store.append(stream_id, vec![envelope], expected_version).await
}
```

## 集群 / Raft 调优

以下数字来自 distributed-kv 示例（3 个本地节点，loopback）的实测，仅供参考，不是性能保证。

- **实测基线（2026-08-12 复测）**:Windows 开发机 loopback、三节点、debug 构建，经 distributed-kv 的 HTTP API 单连接顺序写 300 次，3 次取中位：
  - **raft 后端**:~114 writes/s,mean ≈ 8.7ms,**p50 ≈ 8.6ms,p99 ≈ 22ms**；杀 leader 故障转移（以写恢复计时，经存活 follower 转发重试）中位 **~3.5s**(3.1 / 3.5 / 6.2s)；写完成后经 follower GET 读回中位 ~170ms(40–296ms)。
  - **sorock 后端**（平衡默认 `request_timeout=2s` + `propose_retry=10×200ms`):~106 writes/s,mean ≈ 9.4ms,**p50 ≈ 9.0ms,p99 ≈ 13.9ms**；杀 leader 故障转移中位 **~3.0s**(2.9 / 3.0 / 9.2s——4 次测量中 1 次在持续写压下触发 sorock 0.12 选举活锁 >4 分钟，机制与运维建议见 [sorock 页](../distributed/sorock.md#故障转移行为与调优))；看门狗档 crate 级测试 ~2.1s（示例未暴露开关）;follower 读回中位 ~230ms(214–300ms,sorock 硬编码 300ms 心跳节拍的下限）。
  - 一次复制写的延迟下限约为一个 tick 加 RTT，两后端写延迟同量级；经 follower 转发增加 <1ms。默认 tick 10ms / election 150ms / heartbeat 50ms（`RaftClusterConfig` 的 `tickIntervalMs` 等字段）。降低 `tickIntervalMs` 是以 CPU 换延迟；heartbeat 保持在 election 的 1/3 左右。
- **有界队列即背压**：运行时 inbox 容量 256，写满后 HTTP 边缘返回 429；pending-commit 队列有界（默认 1024，见 raft.rs，可用 `new_with_pending_commit_capacity` 调整），满时 propose 快速失败 `PendingCommitCapacity`——调用方应带抖动重试，而不是无限放大队列。
- **传输超时必须配置**：owner 任务会 await 发送结果，`HttpRaftTransport` 必须链式 `.with_request_timeout(...)`，否则对端挂起会拖住 owner 任务。
- **Checkpoint 节奏**：checkpoint 压缩 Raft 日志；重启时重放快照 + 后缀，按 128 条一页读取以限制恢复内存。节奏越稀疏，日志越大、恢复越慢（示例每 10s 一次）。
- **内存预期**：节点在 1k keys 时 RSS ≈ 18MB；raft-engine 日志在压缩前约 200KB / 1k 小条目。
- **规模验证**：规模契约测试（`crates/catga-cluster/tests/raft_scale.rs`，进程内内存集群）覆盖 10 与 50 投票者的选举、全量复制、leader 丢失重选与 wiped 节点追赶；50 投票者下顺序单客户端写的本地接受延迟约 2.4ms(quorum 26/50)。故障转移（leader 停止 → 重选）实测 ≈ 1s（每对端有界派发修复前为 52s——死对端的挂起发送曾拖住整个 Raft 逻辑时钟）。
- **精确 quorum 死锁（运维准则）**：恰好 quorum 个节点存活时，每次选举都要全票，而 pre-vote 授予非互斥，并发候选者会每个 term 分裂选票——集群可能持续 livelock。恢复**必须恢复选举余量**：重入 ≥2 个节点，或先 `remove_voter` 缩小成员表。低于 quorum 则安全（`check_quorum` 退位 + pre-vote 阻止选举，零提交）。生产部署建议**奇数投票者**(3/5/7)。
- **明确不支持**：无分片（单 leader 写入，写容量按一个 leader 规划）；无 read-index（follower 读是 stale）。

## 与 cqrs-es 对比

| 维度 | Catga | cqrs-es |
|------|-------|---------|
| **Handler 派发** | ~18ns (Vec 扫描) | ~50ns+ (HashMap) |
| **并发吞吐** | 139M msg/s | ~15M msg/s |
| **聚合内存** | 24 bytes 基础 | 50KB+ 内嵌历史 |
| **传输层** | 多协议 (NATS/Redis/RocketMQ) | 仅内存 |
| **类型安全** | 编译期 | 运行时 |

Catga 的性能优势来自：
1. Vec 线性扫描派发 (CPU 缓存友好)
2. Fn-blanket 模式 (无 Box<dyn> 分配)
3. 编译期编码器选择 (无运行时分发)
4. 纯异步设计 (无同步阻塞)

## 最佳实践

### 推荐

1. **使用 Fn-blanket Handler** - 直接注册 async fn，避免额外分配
2. **配置快照策略** - 大事件流一定要配快照
3. **批量操作** - `send_batch` / `publish_batch` 减少开销
4. **分页加载** - 大事件流自动分页

### 避免

1. **避免 Box<dyn Handler>** - 使用 Fn-blanket
2. **避免大聚合** - 事件存储在 EventStore
3. **避免同步阻塞** - 全异步 API

### 性能监控

```rust
// 使用 tracing 观测
tracing::info!("request dispatched in {:?}", elapsed);

// 或使用自定义 observability
let span = observability::request_span(request_type);
observability::record_request(&span, request_type, elapsed, &result);
```

## 编写基准测试

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual performance benchmark"]
async fn my_benchmark() -> CatgaResult<()> {
    // 预热
    for i in 0..1000 {
        mediator.send(Ping(i)).await?;
    }

    let started = Instant::now();
    for i in 0..100_000 {
        mediator.send(Ping(i as u64)).await?;
    }
    let elapsed = started.elapsed();

    println!("throughput: {:.0} msg/s", 100_000.0 / elapsed.as_secs_f64());
    Ok(())
}
```
