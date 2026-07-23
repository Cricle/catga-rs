# Catga Rust Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first pure-Rust Catga release: typed CQRS dispatch, reliability contracts, in-memory implementations, and Redis/NATS adapters.

**Architecture:** `catga-core` owns stable contracts and an explicit typed handler registry. `catga-macros` removes registration boilerplate while adapters remain separate crates. Memory is the reference implementation; Redis Streams and NATS JetStream implement the same transport contract behind opt-in features.

**Tech Stack:** Rust 2024, Tokio, futures, DashMap, parking_lot, thiserror, tracing, serde, proc-macro2/syn/quote, redis, async-nats, testcontainers.

---

## File Structure

| Path | Responsibility |
|---|---|
| `Cargo.toml` | Workspace, lint policy, dependency versions, default members |
| `crates/catga-core/src/{message,error,handler,registry,mediator,pipeline,store,transport}.rs` | Public contracts and runtime |
| `crates/catga-macros/src/lib.rs` | `Message` derive and `catga_handlers!` registry generation |
| `crates/catga-memory/src/{store,transport}.rs` | Bounded transport and atomic in-memory stores |
| `crates/catga-redis/src/lib.rs` | Redis Streams transport and idempotency store |
| `crates/catga-nats/src/lib.rs` | JetStream transport and durable consumer |
| `tests/compatibility/*.rs` | Core semantics derived from upstream tests |
| `tests/integration/{redis,nats}.rs` | Real-service adapter tests |

### Task 1: Bootstrap The Workspace And Core Result Types

**Files:**
- Create: `Cargo.toml`, `rustfmt.toml`, `clippy.toml`
- Create: `crates/catga-core/Cargo.toml`, `crates/catga-core/src/lib.rs`, `crates/catga-core/src/error.rs`
- Create: `tests/compatibility/result.rs`

- [ ] **Step 1: Write the failing result test**

```rust
use catga_core::{CatgaError, CatgaResult, ErrorCode};

#[test]
fn successful_result_maps_without_allocating_an_error() {
    let value: CatgaResult<u64> = Ok(7);
    assert_eq!(value.map(|value| value + 1), Ok(8));
    assert_eq!(CatgaError::new(ErrorCode::Validation, "bad").code(), ErrorCode::Validation);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p catga-core --test result`

Expected: FAIL because package `catga-core` does not yet exist.

- [ ] **Step 3: Create the workspace and minimal result implementation**

```toml
# Cargo.toml
[workspace]
members = ["crates/catga-core", "crates/catga-macros", "crates/catga-memory", "crates/catga-redis", "crates/catga-nats"]
resolver = "3"

[workspace.lints.rust]
unsafe_code = "forbid"
missing_docs = "warn"
```

```rust
// crates/catga-core/src/error.rs
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorCode { Validation, NotFound, Conflict, Cancelled, Timeout, Unsupported, Transient, Internal }
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatgaError { code: ErrorCode, message: Box<str> }
impl CatgaError { pub fn new(code: ErrorCode, message: impl Into<Box<str>>) -> Self { Self { code, message: message.into() } } pub fn code(&self) -> ErrorCode { self.code } }
pub type CatgaResult<T> = Result<T, CatgaError>;
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p catga-core --test result`

Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add Cargo.toml rustfmt.toml clippy.toml crates/catga-core tests/compatibility/result.rs && git commit -m "feat: bootstrap Catga core workspace"`

### Task 2: Add Message Metadata And Typed Handler Contracts

**Files:**
- Create: `crates/catga-core/src/message.rs`, `crates/catga-core/src/handler.rs`
- Modify: `crates/catga-core/src/lib.rs`
- Create: `tests/compatibility/message.rs`

- [ ] **Step 1: Write the failing message metadata test**

```rust
#[derive(Debug)] struct CreateOrder { id: u64 }
impl catga_core::Message for CreateOrder {}
impl catga_core::Request for CreateOrder { type Response = u64; }

#[test]
fn request_metadata_preserves_message_and_correlation_ids() {
    let metadata = catga_core::MessageMetadata::new(11, Some(3));
    assert_eq!(metadata.message_id(), 11);
    assert_eq!(metadata.correlation_id(), Some(3));
    assert!(CreateOrder { id: 1 }.message_type().ends_with("CreateOrder"));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p catga-core --test message`

Expected: FAIL because `Request` and `MessageMetadata` are undefined.

- [ ] **Step 3: Implement marker traits and asynchronous handler contracts**

```rust
pub trait Message: Send + Sync + 'static { fn message_type(&self) -> &'static str { std::any::type_name::<Self>() } }
pub trait Request: Message { type Response: Send + 'static; }
pub trait Command: Message {}
pub trait Event: Message {}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MessageMetadata { message_id: u64, correlation_id: Option<u64> }
impl MessageMetadata { pub const fn new(message_id: u64, correlation_id: Option<u64>) -> Self { Self { message_id, correlation_id } } pub const fn message_id(self) -> u64 { self.message_id } pub const fn correlation_id(self) -> Option<u64> { self.correlation_id } }
```

- [ ] **Step 4: Run the focused and full core tests**

Run: `cargo test -p catga-core --test message && cargo test -p catga-core`

Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add crates/catga-core tests/compatibility/message.rs && git commit -m "feat: add typed message contracts"`

### Task 3: Build Explicit Registration, Typed Send, And Event Fan-Out

**Files:**
- Create: `crates/catga-core/src/{registry,mediator}.rs`
- Modify: `crates/catga-core/src/{handler,lib}.rs`
- Create: `tests/compatibility/mediator.rs`

- [ ] **Step 1: Write failing mediator tests**

```rust
#[tokio::test]
async fn request_routes_to_one_handler_and_event_fans_out() {
    let mediator = test_mediator();
    assert_eq!(mediator.send(CreateOrder { id: 4 }).await.unwrap(), 8);
    mediator.publish(OrderCreated { id: 4 }).await.unwrap();
    assert_eq!(AUDIT_COUNT.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(NOTIFY_COUNT.load(std::sync::atomic::Ordering::Relaxed), 1);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p catga-core --test mediator request_routes_to_one_handler_and_event_fans_out`

Expected: FAIL because `Mediator` and registration are undefined.

- [ ] **Step 3: Implement registry and mediator**

```rust
pub struct Mediator { router: std::sync::Arc<Router> }
impl Mediator {
    pub async fn send<M: Request>(&self, message: M) -> CatgaResult<M::Response> {
        self.router.send(message).await
    }
    pub async fn publish<E: Event>(&self, event: E) -> CatgaResult<()> { self.router.publish(event).await }
}
```

Use `TypeId` keys and `Box<dyn Any + Send>` only inside `Router`; reject a second request handler for the same request type during registration and await all event handlers before returning.

- [ ] **Step 4: Run tests and Clippy**

Run: `cargo test -p catga-core --test mediator && cargo clippy -p catga-core -- -D warnings`

Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add crates/catga-core tests/compatibility/mediator.rs && git commit -m "feat: add typed mediator routing"`

### Task 4: Add Ordered Pipeline, Batch, Streaming, And Cancellation

**Files:**
- Create: `crates/catga-core/src/pipeline.rs`
- Modify: `crates/catga-core/src/mediator.rs`
- Create: `tests/compatibility/{pipeline,batch_stream}.rs`

- [ ] **Step 1: Write failing behavior-order and batch-bound tests**

```rust
#[tokio::test]
async fn pipeline_is_entered_in_order_and_exited_in_reverse_order() {
    let trace = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let result = mediator_with_trace(trace.clone()).send(CreateOrder { id: 1 }).await;
    assert_eq!(result.unwrap(), 2);
    assert_eq!(*trace.lock().await, ["a+", "b+", "handler", "b-", "a-"]);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p catga-core --test pipeline --test batch_stream`

Expected: FAIL because behavior composition and batch APIs are absent.

- [ ] **Step 3: Implement behavior composition and bounded concurrency**

```rust
pub trait Behavior<M: Request>: Send + Sync { fn handle<'a>(&'a self, message: M, next: Next<'a, M>) -> BoxFuture<'a, CatgaResult<M::Response>>; }
pub async fn send_batch<M: Request>(&self, messages: impl IntoIterator<Item = M>, limit: usize) -> Vec<CatgaResult<M::Response>> {
    futures::stream::iter(messages).map(|message| self.send(message)).buffered(limit).collect().await
}
```

Implement `send_stream` with `StreamExt::then`, stop before handler invocation when the cancellation token is cancelled, and reject `limit == 0` with `ErrorCode::Validation`.

- [ ] **Step 4: Run tests**

Run: `cargo test -p catga-core --test pipeline --test batch_stream`

Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add crates/catga-core tests/compatibility/pipeline.rs tests/compatibility/batch_stream.rs && git commit -m "feat: add mediator pipeline and batch APIs"`

### Task 5: Create Reliability Store And Transport Contracts

**Files:**
- Create: `crates/catga-core/src/{store,transport}.rs`
- Modify: `crates/catga-core/src/lib.rs`
- Create: `tests/compatibility/reliability_contracts.rs`

- [ ] **Step 1: Write failing state-transition tests**

```rust
#[tokio::test]
async fn outbox_claim_is_exclusive_and_ack_removes_only_the_claimed_message() {
    let store = TestOutbox::default();
    store.enqueue(message(1)).await.unwrap();
    assert_eq!(store.claim("worker-a", 1).await.unwrap().len(), 1);
    assert!(store.claim("worker-b", 1).await.unwrap().is_empty());
    store.ack("worker-a", 1).await.unwrap();
    assert!(store.get(1).await.unwrap().is_none());
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p catga-core --test reliability_contracts`

Expected: FAIL because store traits and message envelopes do not exist.

- [ ] **Step 3: Define explicit async contracts**

```rust
pub trait OutboxStore: Send + Sync { async fn enqueue(&self, message: OutboxMessage) -> CatgaResult<()>; async fn claim(&self, owner: &str, limit: usize) -> CatgaResult<Vec<OutboxMessage>>; async fn ack(&self, owner: &str, id: u64) -> CatgaResult<()>; }
pub trait MessageTransport: Send + Sync { async fn publish(&self, envelope: Envelope) -> CatgaResult<()>; async fn receive(&self) -> CatgaResult<Delivery>; }
```

Define equivalent `InboxStore`, `IdempotencyStore`, and `DeadLetterStore` traits with explicit `Pending`, `Completed`, `Failed`, and `Claimed` states.

- [ ] **Step 4: Run tests**

Run: `cargo test -p catga-core --test reliability_contracts`

Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add crates/catga-core tests/compatibility/reliability_contracts.rs && git commit -m "feat: add transport and reliability contracts"`

### Task 6: Implement In-Memory Stores And Bounded Transport

**Files:**
- Create: `crates/catga-memory/{Cargo.toml,src/lib.rs,src/store.rs,src/transport.rs}`
- Create: `tests/compatibility/memory.rs`

- [ ] **Step 1: Write failing atomicity and backpressure tests**

```rust
#[tokio::test]
async fn bounded_transport_waits_for_capacity_then_delivers_and_acks() {
    let transport = MemoryTransport::new(1);
    transport.publish(envelope(1)).await.unwrap();
    let second = tokio::spawn({ let transport = transport.clone(); async move { transport.publish(envelope(2)).await } });
    assert!(!second.is_finished());
    assert_eq!(transport.receive().await.unwrap().id(), 1);
    assert!(second.await.unwrap().is_ok());
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p catga-memory --test memory`

Expected: FAIL because `catga-memory` does not exist.

- [ ] **Step 3: Implement memory adapters**

```rust
pub struct MemoryTransport { sender: tokio::sync::mpsc::Sender<Envelope>, receiver: std::sync::Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<Envelope>>> }
impl MemoryTransport { pub fn new(capacity: usize) -> Self { let (sender, receiver) = tokio::sync::mpsc::channel(capacity); Self { sender, receiver: std::sync::Arc::new(tokio::sync::Mutex::new(receiver)) } } }
```

Use `DashMap<u64, _>` plus per-record compare-and-swap state changes for stores; do not hold a mutex across `.await`.

- [ ] **Step 4: Run tests under Tokio's multithread runtime**

Run: `cargo test -p catga-memory --test memory -- --test-threads=4`

Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add crates/catga-memory tests/compatibility/memory.rs && git commit -m "feat: add in-memory Catga adapters"`

### Task 7: Implement The Macro Ergonomics

**Files:**
- Create: `crates/catga-macros/{Cargo.toml,src/lib.rs}`
- Modify: `crates/catga-core/src/lib.rs`
- Create: `tests/compatibility/macros.rs`, `tests/ui/duplicate_handler.rs`

- [ ] **Step 1: Write failing compile and runtime macro tests**

```rust
#[derive(catga::Message)]
struct Ping;
#[test]
fn derived_message_has_a_stable_type_name() { assert_eq!(Ping.message_type(), "Ping"); }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p catga-core --test macros`

Expected: FAIL because derive macro expansion is absent.

- [ ] **Step 3: Implement derives and registration macro**

```rust
#[proc_macro_derive(Message)]
pub fn derive_message(input: TokenStream) -> TokenStream {
    let item = syn::parse_macro_input!(input as syn::DeriveInput);
    let name = item.ident;
    quote::quote!(impl ::catga_core::Message for #name { fn message_type(&self) -> &'static str { stringify!(#name) } }).into()
}
```

Make `catga_handlers!` emit `Registry::request::<M, H>()` and `Registry::event::<E, H>()`; have `trybuild` assert duplicate request registration reports the duplicate message type.

- [ ] **Step 4: Run macro tests**

Run: `cargo test -p catga-macros && cargo test -p catga-core --test macros`

Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add crates/catga-macros crates/catga-core tests/compatibility/macros.rs tests/ui && git commit -m "feat: add Catga registration macros"`

### Task 8: Add Retry, Timeout, Correlation, Idempotency, And Dead-Letter Behaviors

**Files:**
- Create: `crates/catga-core/src/behaviors/{mod,retry,timeout,correlation,idempotency,dead_letter}.rs`
- Modify: `crates/catga-core/src/{lib,pipeline}.rs`
- Create: `tests/compatibility/behaviors.rs`

- [ ] **Step 1: Write failing behavior tests**

```rust
#[tokio::test]
async fn retry_replays_transient_failure_only_and_dead_letters_terminal_failure() {
    let result = mediator_with_retry(2).send(FailsThenSucceeds::new()).await;
    assert_eq!(result.unwrap(), "ok");
    assert_eq!(ATTEMPTS.load(Ordering::Relaxed), 2);
    assert_eq!(dead_letters().await.len(), 0);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p catga-core --test behaviors`

Expected: FAIL because the standard behaviors are absent.

- [ ] **Step 3: Implement behavior semantics**

```rust
if error.code() == ErrorCode::Transient && attempt < retries {
    tokio::time::sleep(backoff(attempt)).await;
    continue;
}
return Err(error);
```

Use `tokio::time::timeout`, propagate correlation through task-local context, short-circuit completed idempotency keys, and send only unrecoverable errors to `DeadLetterStore`.

- [ ] **Step 4: Run tests**

Run: `cargo test -p catga-core --test behaviors`

Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add crates/catga-core tests/compatibility/behaviors.rs && git commit -m "feat: add reliability pipeline behaviors"`

### Task 9: Implement Redis Streams Adapter

**Files:**
- Create: `crates/catga-redis/{Cargo.toml,src/lib.rs,src/transport.rs,src/store.rs}`
- Create: `tests/integration/redis.rs`
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Write failing Redis integration tests**

```rust
#[tokio::test]
async fn redis_stream_round_trip_ack_and_pending_reclaim() {
    let Some(url) = std::env::var("CATGA_REDIS_URL").ok() else { return; };
    let transport = RedisTransport::connect(&url, unique_stream()).await.unwrap();
    transport.publish(envelope(1)).await.unwrap();
    let delivery = transport.receive().await.unwrap();
    transport.ack(delivery).await.unwrap();
    assert!(transport.pending().await.unwrap().is_empty());
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `CATGA_REDIS_URL=redis://127.0.0.1:6379 cargo test -p catga-redis --test redis`

Expected: FAIL because the crate and `RedisTransport` are absent.

- [ ] **Step 3: Implement Redis Streams transport and TTL idempotency**

```rust
pub async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
    self.connection.xadd(&self.stream, "*", &[("payload", envelope.encode())]).await.map_err(redis_error)?;
    Ok(())
}
```

Provision a consumer group with `XGROUP CREATE ... MKSTREAM`, receive via `XREADGROUP`, acknowledge with `XACK`, reclaim idle deliveries with `XAUTOCLAIM`, and use `SET key value NX PX ttl` for idempotency.

- [ ] **Step 4: Run compile, unit, and service-backed tests**

Run: `cargo test -p catga-redis && CATGA_REDIS_URL=redis://127.0.0.1:6379 cargo test -p catga-redis --test redis`

Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add crates/catga-redis tests/integration/redis.rs .github/workflows/ci.yml && git commit -m "feat: add Redis Streams adapter"`

### Task 10: Implement NATS JetStream Adapter

**Files:**
- Create: `crates/catga-nats/{Cargo.toml,src/lib.rs,src/transport.rs}`
- Create: `tests/integration/nats.rs`
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Write failing NATS integration test**

```rust
#[tokio::test]
async fn jetstream_round_trip_ack_and_redelivery() {
    let Some(url) = std::env::var("CATGA_NATS_URL").ok() else { return; };
    let transport = NatsTransport::connect(&url, unique_subject()).await.unwrap();
    transport.publish(envelope(9)).await.unwrap();
    let delivery = transport.receive().await.unwrap();
    delivery.ack().await.unwrap();
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `CATGA_NATS_URL=nats://127.0.0.1:4222 cargo test -p catga-nats --test nats`

Expected: FAIL because the crate and `NatsTransport` are absent.

- [ ] **Step 3: Implement durable JetStream transport**

```rust
let context = async_nats::jetstream::new(async_nats::connect(url).await?);
let stream = context.get_or_create_stream(config).await?;
stream.publish(subject, envelope.encode().into()).await?.await?;
```

Create a durable pull consumer with explicit acknowledgements, request a bounded batch, call `ack` only after consumer success, and map JetStream redelivery metadata into `Delivery`.

- [ ] **Step 4: Run compile, unit, and service-backed tests**

Run: `cargo test -p catga-nats && CATGA_NATS_URL=nats://127.0.0.1:4222 cargo test -p catga-nats --test nats`

Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add crates/catga-nats tests/integration/nats.rs .github/workflows/ci.yml && git commit -m "feat: add NATS JetStream adapter"`

### Task 11: Add Cross-Adapter Contract Tests, Benchmarks, And Release Gates

**Files:**
- Create: `tests/compatibility/transport_contract.rs`, `benches/mediator.rs`, `.github/workflows/ci.yml`
- Modify: `README.md`, `Cargo.toml`

- [ ] **Step 1: Write failing cross-adapter contract test**

```rust
async fn transport_contract<T: MessageTransport>(transport: T) {
    transport.publish(envelope(5)).await.unwrap();
    let delivery = transport.receive().await.unwrap();
    assert_eq!(delivery.envelope().id(), 5);
    delivery.ack().await.unwrap();
}
```

- [ ] **Step 2: Run the test to verify it fails for an adapter**

Run: `cargo test --test transport_contract`

Expected: FAIL until every adapter exposes identical acknowledgement semantics.

- [ ] **Step 3: Make the contract and quality tooling pass**

```toml
[[bench]]
name = "mediator"
harness = false
```

Benchmark direct typed handler invocation against `Mediator::send`; document measured allocations and throughput without setting an unverified target. CI runs formatting, Clippy, default tests, feature compilation, and Redis/NATS service jobs.

- [ ] **Step 4: Run full verification**

Run: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets --all-features -- -D warnings && cargo test --workspace --all-features && cargo bench --bench mediator --no-run`

Expected: PASS. Run service tests with their corresponding URLs before declaring either adapter complete.

- [ ] **Step 5: Commit**

Run: `git add tests benches .github README.md Cargo.toml && git commit -m "test: add Catga compatibility release gates"`

## Plan Self-Review

Spec coverage: Tasks 1-4 cover typed messages, handlers, pipeline, fan-out, batch and stream semantics. Tasks 5-8 cover persistence, bounded transport, acknowledgement, correlation, retry, timeout, idempotency, outbox, inbox, and dead-letter semantics. Tasks 9-10 cover the only requested non-memory adapters, Redis and NATS. Task 11 supplies cross-adapter tests, benchmarks, and CI gates. Event sourcing, Flow, Web, RabbitMQ, scheduling, and clustering remain explicitly deferred by the approved spec.

Consistency check: `CatgaResult<T>`, `MessageTransport`, `Envelope`, `Delivery`, and `ErrorCode` are defined in core before later tasks use them. The Redis and NATS tests use the same `Envelope`/`Delivery` acknowledgement model. Each implementation task starts with a focused failing test and ends with a verification command and commit.
