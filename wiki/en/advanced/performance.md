# Performance Optimization

## Performance Benchmarks

Catga's core performance metrics are based on real benchmarks:

| Metric | Result | Notes |
|--------|--------|-------|
| **Sequential Request Dispatch** | **53.8M msg/s** | Single-threaded, 18ns avg latency |
| **Concurrent Request Dispatch** | **139.6M msg/s** | 16 concurrent tasks, 7ns avg latency |
| **Event Publishing** | **46.4M events/s** | Single Handler, 21ns avg latency |

### Benchmark Details

```bash
cargo test --release -p catga-tests --test typed_mediator_bench -- --ignored --nocapture
```

**Sequential Dispatch (100,000 messages)**:
```
=== Typed Mediator Sequential Send ===
  messages:    100,000
  total:       1.856858ms
  throughput:  53,854,414 msg/s
  avg latency: 18 ns
```

**Concurrent Dispatch (100,000 messages, 16 tasks)**:
```
=== Typed Mediator Concurrent Send (16 tasks) ===
  messages:    100,000
  total:       716.193µs
  throughput:  139,627,168 msg/s
  avg latency: 7 ns
```

**Event Publishing (100,000 events)**:
```
=== Typed Mediator Event Publish (1 handler) ===
  events:      100,000
  total:       2.156593ms
  throughput:  46,369,435 events/s
  avg latency: 21 ns
```

## Core Performance Techniques

### 1. Vec Linear Scan Dispatch

Typical applications register 5-30 Handlers; Vec linear scan is faster than HashMap:

```rust
// mediator.rs:351-360
// TypeId matching + linear scan
let type_id = TypeId::of::<M>();
let slot = registry.requests.iter()
    .find(|slot| slot.type_id == type_id)?;

// Why not HashMap?
// - 5-30 elements, linear scan ~3ns
// - HashMap: hash computation + bucket lookup ~15ns
// - Vec: contiguous memory, CPU cache friendly
```

### 2. Fn-blanket Avoids Box<dyn>

Plain async fn directly implements the Handler trait:

```rust
// handler.rs:143-154
// No Box<dyn Handler> needed
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

// Usage example
async fn double_handler(value: Double) -> CatgaResult<u64> {
    Ok(value.0 * 2)
}

// Register directly, no wrapping needed
registry.register_request::<Double, _>(double_handler)?;
```

### 3. Compile-Time Encoder Selection

`PayloadEncoder<M>` generic bounds select the optimal encoding path at compile time:

```rust
// typed_publisher.rs:69-89
pub async fn publish<M>(&self, message: &M) -> CatgaResult<()>
where
    M: Message,
    C: PayloadEncoder<M>,  // Compile-time binding
{
    // Runtime selects specialized implementation based on concrete type
    self.codec.encode_payload(message)?
}
```

### 4. Snapshot Strategies to Reduce Replay

```rust
// aggregate.rs:38-77
// Three snapshot strategies

// By event count
let strategy = EventCountSnapshotStrategy::new(100)?;
// Snapshot every 100 events

// By time interval
let strategy = TimeBasedSnapshotStrategy::new(Duration::from_secs(60));

// Composite strategy (trigger on either)
let composite = CompositeSnapshotStrategy::new(
    EventCountSnapshotStrategy::new(100)?,
    TimeBasedSnapshotStrategy::new(Duration::from_secs(60)),
);

// Usage
let repo = AggregateRepository::new(store, snapshots, strategy);
let aggregate = repo.load("acc-1").await?;
```

### 5. Paginated Loading for Large Event Streams

```rust
// event_store.rs
// Paginated loading to avoid loading all events at once
let page = store.read_page(stream_id, offset, MAX_EVENT_STORE_PAGE_SIZE).await?;

// MAX_EVENT_STORE_PAGE_SIZE = 1024
// Large event streams are automatically paginated
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

### 6. Batch Concurrent Processing

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
        .buffered(concurrency_limit)  // Concurrency control
        .collect()
        .await)

// publish_batch uses buffer_unordered
// Allows out-of-order completion for higher throughput
```

## Memory Optimization

### Minimize Aggregate State

```rust
// Catga: Aggregate only stores core state; events are external
struct BankAccount {
    id: u64,        // 8 bytes
    balance: i64,   // 8 bytes
    version: i64,   // 8 bytes
}
// Aggregate itself: 24 bytes

// Events are stored in EventStore
// Snapshots are stored in SnapshotStore
```

### TypedEventStore Zero Boxing

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
    C: PayloadEncoder<E>,  // Compile-time encoder
{
    // Direct encoding, no intermediate Box
    let envelope = Envelope::versioned(
        id,
        event.message_type(),
        self.codec.encode_payload(event)?,  // Direct serialization
        metadata,
        event.schema_version(),
    );
    self.store.append(stream_id, vec![envelope], expected_version).await
}
```

## Cluster / Raft Tuning

The numbers below are loopback measurements from the distributed-kv example (3 local nodes); treat them as indicative, not guarantees.

- **Measured baselines (re-measured 2026-08-12)**: Windows dev machine, loopback, three nodes, debug build, 300 sequential single-connection writes through distributed-kv's HTTP API, median of 3 runs:
  - **raft backend**: ~114 writes/s, mean ≈ 8.7ms, **p50 ≈ 8.6ms, p99 ≈ 22ms**; kill-leader failover (timed until a write succeeds again, retried through a surviving follower) median **~3.5s** (3.1 / 3.5 / 6.2s); read-back through a follower GET after the write completes, median ~170ms (40–296ms).
  - **sorock backend** (balanced defaults `request_timeout=2s` + `propose_retry=10×200ms`): ~106 writes/s, mean ≈ 9.4ms, **p50 ≈ 9.0ms, p99 ≈ 13.9ms**; kill-leader failover median **~3.0s** (2.9 / 3.0 / 9.2s — 1 of 4 measurement runs hit a >4-minute sorock 0.12 election livelock under sustained write pressure; mechanism and operational guidance on the [sorock page](../distributed/sorock.md#failover-behavior-and-tuning)); the watchdog profile measures ~2.1s in the crate-level test (the example does not expose the flag); follower read-back median ~230ms (214–300ms, the floor of sorock's hardcoded 300ms heartbeat cadence).
  - A replicated write's latency floor is roughly one tick plus RTTs, and both backends land in the same write-latency band; forwarding through a follower adds <1ms. Defaults are tick 10ms / election 150ms / heartbeat 50ms (`RaftClusterConfig` fields such as `tickIntervalMs`). Lowering `tickIntervalMs` trades CPU for latency; keep heartbeat around election/3.
- **Bounded queues as backpressure**: the runtime inbox holds 256 frames and the HTTP edge returns 429 when full; the pending-commit queue is bounded (default 1024, see raft.rs, tunable via `new_with_pending_commit_capacity`) and propose fails fast with `PendingCommitCapacity` when full — callers should retry with jitter, not grow the queue unboundedly.
- **Transport timeouts are mandatory**: the owner task awaits sends, so always chain `.with_request_timeout(...)` on `HttpRaftTransport`; a hung peer otherwise stalls the owner task.
- **Checkpoint cadence**: a checkpoint compacts the Raft log; restart replays the snapshot plus the suffix, read in pages of 128 entries to bound recovery memory. Sparcer cadence means a larger log and slower recovery (the example checkpoints every 10s).
- **Memory expectations**: ~18MB RSS per node at 1k keys; the raft-engine log is ~200KB per 1k small entries before compaction.
- **Verified scale**: the scale contract tests (`crates/catga-cluster/tests/raft_scale.rs`, in-memory in-process clusters) cover election, full replication, leader-loss re-election, and wiped-node catch-up at 10 and 50 voters; at 50 voters the local-accept latency of sequential single-client writes is about 2.4ms (quorum 26/50). Measured failover (leader stop → re-election) is ≈ 1s — it was 52s before per-peer bounded dispatch, when a dead peer's hung send could stall the entire Raft logical clock.
- **Exact-quorum livelock (ops guidance)**: with exactly quorum nodes alive, every election needs a unanimous vote and pre-vote grants are non-exclusive, so concurrent candidates split the vote fatally every term — the cluster can livelock. Recovery **must restore election margin**: rejoin ≥2 nodes, or `remove_voter` first to shrink the member table. Below quorum is safe instead (`check_quorum` step-down plus pre-vote blocking means zero commits). Prefer **odd voter counts** (3/5/7) in production.
- **Explicitly not provided**: no sharding (single writer leader — size write capacity around one leader); no read-index (follower reads are stale).

## Comparison with cqrs-es

| Dimension | Catga | cqrs-es |
|-----------|-------|---------|
| **Handler Dispatch** | ~18ns (Vec scan) | ~50ns+ (HashMap) |
| **Concurrent Throughput** | 139M msg/s | ~15M msg/s |
| **Aggregate Memory** | 24 bytes base | 50KB+ embedded history |
| **Transport Layer** | Multi-protocol (NATS/Redis/RocketMQ) | Memory only |
| **Type Safety** | Compile-time | Runtime |

Catga's performance advantages come from:
1. Vec linear scan dispatch (CPU cache friendly)
2. Fn-blanket pattern (no Box<dyn> allocation)
3. Compile-time encoder selection (no runtime dispatch)
4. Pure async design (no sync blocking)

## Best Practices

### Recommended

1. **Use Fn-blanket Handler** - Register async fn directly, avoid extra allocation
2. **Configure Snapshot Strategy** - Large event streams must have snapshots configured
3. **Batch Operations** - Use `send_batch` / `publish_batch` to reduce overhead
4. **Paginated Loading** - Large event streams load paginated automatically

### Avoid

1. **Avoid Box<dyn Handler>** - Use Fn-blanket instead
2. **Avoid Large Aggregates** - Store events in EventStore
3. **Avoid Sync Blocking** - Use fully async API

### Performance Monitoring

```rust
// Use tracing for observability
tracing::info!("request dispatched in {:?}", elapsed);

// Or use custom observability
let span = observability::request_span(request_type);
observability::record_request(&span, request_type, elapsed, &result);
```

## Writing Benchmarks

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual performance benchmark"]
async fn my_benchmark() -> CatgaResult<()> {
    // Warm up
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
