# RabbitMQ Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a pure-Rust RabbitMQ transport with bounded acknowledged delivery and native envelope request/reply.

**Architecture:** `catga-rabbitmq` owns all AMQP details and depends only on the core envelope contracts and Postcard codec. The transport has one confirmed publish channel; each consumer or RPC call owns its own channel, avoiding locks across receive and user-handler awaits.

**Tech Stack:** Rust 2024, Tokio, Lapin, async-trait, Postcard, Catga core contracts.

---

## File structure

| Path | Responsibility |
|---|---|
| `Cargo.toml` | Register the adapter workspace member. |
| `crates/catga-rabbitmq/Cargo.toml` | Isolated AMQP adapter dependencies. |
| `crates/catga-rabbitmq/src/config.rs` | Validated config, routing names, AMQP property helpers. |
| `crates/catga-rabbitmq/src/acknowledgement.rs` | One-shot broker ack/nack adapter. |
| `crates/catga-rabbitmq/src/transport.rs` | Confirmed publish, bounded consume, and `MessageTransport`. |
| `crates/catga-rabbitmq/src/rpc.rs` | Native reply queue request/reply and typed client alias. |
| `crates/catga-rabbitmq/src/competing_consumer.rs` | Bounded shared-queue handler runner and poison policy. |
| `crates/catga-rabbitmq/src/lib.rs` | Focused public exports. |
| `tests/Cargo.toml` | Register the adapter and its integration target. |
| `tests/rabbitmq.rs` | Unit-safe validation plus URL-gated broker integration tests. |

### Task 1: Scaffold the crate and validate configuration

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/catga-rabbitmq/Cargo.toml`
- Create: `crates/catga-rabbitmq/src/lib.rs`
- Create: `crates/catga-rabbitmq/src/config.rs`
- Modify: `tests/Cargo.toml`
- Create: `tests/rabbitmq.rs`

- [ ] **Step 1: Write failing configuration tests**

```rust
use catga_core::ErrorCode;
use catga_rabbitmq::RabbitMqConfig;

#[test]
fn config_rejects_invalid_broker_limits_before_connecting() {
    let error = RabbitMqConfig { prefetch: 0, ..RabbitMqConfig::default() }
        .validate()
        .expect_err("zero prefetch must be rejected locally");
    assert_eq!(error.code(), ErrorCode::Validation);
}

#[test]
fn routing_key_normalizes_the_prefix_once() {
    let config = RabbitMqConfig { prefix: "catga".into(), ..RabbitMqConfig::default() };
    assert_eq!(config.routing_key("orders.created").unwrap(), "catga.orders.created");
    assert_eq!(config.routing_key("catga.orders.created").unwrap(), "catga.orders.created");
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `rtk proxy cargo test -p catga-tests --test rabbitmq config_rejects_invalid_broker_limits_before_connecting`

Expected: FAIL because `catga-rabbitmq` is not a workspace package.

- [ ] **Step 3: Add the package and the minimal configuration API**

```toml
# Cargo.toml member
"crates/catga-rabbitmq",

# crates/catga-rabbitmq/Cargo.toml
[dependencies]
async-trait = "0.1"
catga-codec-postcard = { path = "../catga-codec-postcard" }
catga-core = { path = "../catga-core" }
futures = "0.3"
lapin = "4.10"
tokio = { version = "1", features = ["sync", "time"] }
```

```rust
#[derive(Clone, Debug)]
pub struct RabbitMqConfig { pub uri: Box<str>, pub exchange: Box<str>, pub prefix: Box<str>, pub prefetch: u16, pub request_timeout: std::time::Duration, /* queue and exchange flags */ }
impl RabbitMqConfig {
    pub fn validate(&self) -> CatgaResult<()> { /* reject empty URI/exchange, zero prefetch, zero timeout */ }
    pub fn routing_key(&self, destination: &str) -> CatgaResult<Box<str>> { /* trim and prefix exactly once */ }
}
```

- [ ] **Step 4: Run the focused test**

Run: `rtk proxy cargo test -p catga-tests --test rabbitmq --quiet`

Expected: PASS for the two configuration tests.

- [ ] **Step 5: Commit the isolated scaffold**

Run: `git add Cargo.toml crates/catga-rabbitmq tests/Cargo.toml tests/rabbitmq.rs && git commit -m "feat: scaffold RabbitMQ transport adapter"`

### Task 2: Implement confirmed publish and one-shot acknowledgement

**Files:**
- Create: `crates/catga-rabbitmq/src/acknowledgement.rs`
- Create: `crates/catga-rabbitmq/src/transport.rs`
- Modify: `crates/catga-rabbitmq/src/lib.rs`
- Modify: `tests/rabbitmq.rs`

- [ ] **Step 1: Write failing transport tests**

```rust
#[tokio::test]
async fn rabbitmq_round_trip_acknowledges_exactly_once() {
    let Some(url) = rabbitmq_url() else { return; };
    let transport = transport(&url, "ack").await;
    transport.publish(envelope(1)).await.unwrap();
    let delivery = transport.receive().await.unwrap();
    assert_eq!(delivery.envelope().id(), 1);
    transport.ack(delivery).await.unwrap();
}

#[tokio::test]
async fn negative_acknowledgement_redelivers_the_same_envelope() {
    let Some(url) = rabbitmq_url() else { return; };
    let transport = transport(&url, "nack").await;
    transport.publish(envelope(2)).await.unwrap();
    transport.nack(transport.receive().await.unwrap()).await.unwrap();
    assert_eq!(transport.receive().await.unwrap().envelope().id(), 2);
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `CATGA_RABBITMQ_URL=amqp://guest:guest@localhost:5672 rtk proxy cargo test -p catga-tests --test rabbitmq rabbitmq_round_trip_acknowledges_exactly_once -- --nocapture`

Expected: FAIL because `RabbitMqTransport` is absent. If no broker is available, the test prints its explicit skip message.

- [ ] **Step 3: Implement bounded delivery and properties**

```rust
#[async_trait]
impl Acknowledger for RabbitMqAcknowledger {
    async fn acknowledge(self: Box<Self>) -> CatgaResult<()> { self.delivery.ack(BasicAckOptions::default()).await.map_err(map_error) }
    async fn negative_acknowledge(self: Box<Self>) -> CatgaResult<()> { self.delivery.nack(BasicNackOptions { requeue: true, ..Default::default() }).await.map_err(map_error) }
}

#[async_trait]
impl MessageTransport for RabbitMqTransport {
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> { self.publish_to(envelope, "").await }
    async fn receive(&self) -> CatgaResult<Delivery> { self.consumer.next_delivery().await }
}
```

Declare exchange/queue/binding, call `basic_qos` before consumption, publish the single Postcard encoding with confirms, and construct `Delivery::with_acknowledger` from the received Lapin delivery. Map connection, channel, confirm, and codec errors through the crate-local `map_error`.

- [ ] **Step 4: Run focused checks**

Run: `rtk proxy cargo test -p catga-tests --test rabbitmq --quiet && rtk proxy cargo clippy -p catga-rabbitmq -- -D warnings`

Expected: PASS; broker-only assertions skip when the URL is unset.

- [ ] **Step 5: Commit publish/consume support**

Run: `git add crates/catga-rabbitmq tests/rabbitmq.rs && git commit -m "feat: add RabbitMQ acknowledged transport"`

### Task 3: Add native envelope RPC

**Files:**
- Create: `crates/catga-rabbitmq/src/rpc.rs`
- Modify: `crates/catga-rabbitmq/src/lib.rs`
- Modify: `tests/rabbitmq.rs`

- [ ] **Step 1: Write failing RPC tests**

```rust
#[tokio::test]
async fn request_uses_a_private_reply_queue_and_preserves_correlation() {
    let Some(url) = rabbitmq_url() else { return; };
    let (client, responder) = request_pair(&url, "rpc").await;
    let responder = tokio::spawn(async move { responder.respond_once().await });
    let reply = client.request("orders.rpc", envelope(7), Duration::from_secs(2)).await.unwrap();
    responder.await.unwrap().unwrap();
    assert_eq!(reply.metadata().correlation_id(), Some(7));
}

#[tokio::test]
async fn request_rejects_zero_timeout_without_opening_a_reply_queue() {
    let error = disconnected_request_client().request("orders.rpc", envelope(1), Duration::ZERO).await.unwrap_err();
    assert_eq!(error.code(), ErrorCode::Validation);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `rtk proxy cargo test -p catga-tests --test rabbitmq request_rejects_zero_timeout_without_opening_a_reply_queue`

Expected: FAIL because `RabbitMqRequestClient` is undefined.

- [ ] **Step 3: Implement `RequestTransport` without shared reply state**

```rust
#[async_trait]
impl RequestTransport for RabbitMqRequestClient {
    async fn request(&self, destination: &str, request: Envelope, timeout: Duration) -> CatgaResult<Envelope> {
        if timeout.is_zero() { return Err(CatgaError::new(ErrorCode::Validation, "RabbitMQ request timeout must be greater than zero")); }
        self.request_to(destination, request, timeout).await
    }
}
pub type RabbitMqTypedRequestClient = PostcardRequestClient<RabbitMqRequestClient>;
```

`request_to` must use a broker-generated exclusive queue, set AMQP `reply_to` and `correlation_id`, add the queue name with `Envelope::with_reply_to`, consume only a matching response, and close the temporary channel through RAII on every exit path.

- [ ] **Step 4: Run RPC checks**

Run: `rtk proxy cargo test -p catga-tests --test rabbitmq --quiet`

Expected: PASS with URL-gated round-trip test and unconditional timeout validation.

- [ ] **Step 5: Commit RPC support**

Run: `git add crates/catga-rabbitmq/src/rpc.rs crates/catga-rabbitmq/src/lib.rs tests/rabbitmq.rs && git commit -m "feat: add RabbitMQ envelope RPC"`

### Task 4: Add priority, delayed delivery, and competing consumers

**Files:**
- Create: `crates/catga-rabbitmq/src/competing_consumer.rs`
- Modify: `crates/catga-rabbitmq/src/config.rs`
- Modify: `crates/catga-rabbitmq/src/transport.rs`
- Modify: `crates/catga-rabbitmq/src/lib.rs`
- Modify: `tests/rabbitmq.rs`

- [ ] **Step 1: Write failing broker-metadata and concurrency tests**

```rust
#[test]
fn configured_max_priority_clamps_outgoing_priority() {
    let config = RabbitMqConfig { max_priority: Some(3), ..RabbitMqConfig::default() };
    assert_eq!(config.clamp_priority(Some(9)), Some(3));
}

#[tokio::test]
async fn competing_consumers_process_each_delivery_once() {
    let Some(url) = rabbitmq_url() else { return; };
    let seen = run_two_competing_consumers(&url, 20).await;
    assert_eq!(seen.len(), 20);
    assert!(seen.iter().all(|count| *count == 1));
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `rtk proxy cargo test -p catga-tests --test rabbitmq configured_max_priority_clamps_outgoing_priority`

Expected: FAIL because priority helpers and the competing consumer are absent.

- [ ] **Step 3: Implement broker-native metadata and poison policy**

```rust
pub struct CompetingConsumerConfig { pub max_concurrency: usize, pub max_delivery_attempts: u32, pub group_name: Box<str>, pub consumer_name: Box<str> }
// Worker loop: acquire semaphore; decode; await handler; ack on success;
// nack(requeue = true) below the limit; call dead-letter callback then reject at the limit.
```

Declare `x-max-priority` when configured, map priority and `x-delay` into AMQP properties, declare the delayed exchange with `x-delayed-type`, and retain trace headers without re-encoding the envelope. Attempt tracking must be keyed by broker message ID and bounded by successful/final delivery removal.

- [ ] **Step 4: Run focused checks**

Run: `rtk proxy cargo test -p catga-tests --test rabbitmq --quiet && rtk proxy cargo clippy -p catga-rabbitmq -- -D warnings`

Expected: PASS; URL-gated cases print a skip reason without a broker.

- [ ] **Step 5: Commit advanced delivery features**

Run: `git add crates/catga-rabbitmq tests/rabbitmq.rs && git commit -m "feat: add RabbitMQ priority and competing consumers"`

### Task 5: Verify adapter and workspace integration

**Files:**
- Modify: `docs/superpowers/specs/2026-07-24-rabbitmq-transport-design.md` only if verification finds a design mismatch.

- [ ] **Step 1: Format the changed workspace files**

Run: `rtk proxy cargo fmt --check`

Expected: PASS.

- [ ] **Step 2: Run adapter linting**

Run: `rtk proxy cargo clippy -p catga-rabbitmq --all-targets -- -D warnings`

Expected: PASS.

- [ ] **Step 3: Run all workspace tests**

Run: `rtk proxy cargo test --workspace --all-targets --quiet`

Expected: PASS; RabbitMQ integration explicitly skips only when `CATGA_RABBITMQ_URL` is unset.

- [ ] **Step 4: Run broker-backed tests when a URL is configured**

Run: `rtk proxy cargo test -p catga-tests --test rabbitmq -- --nocapture`

Expected: publish/ack, nack redelivery, routing, metadata, RPC, priority, delay, and competing-consumer tests pass against the configured broker.

- [ ] **Step 5: Commit the verified feature**

Run: `git add Cargo.toml Cargo.lock crates/catga-rabbitmq tests/Cargo.toml tests/rabbitmq.rs docs/superpowers && git commit -m "feat: add RabbitMQ Catga transport"`
