# Typed Distributed Facade Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove envelope, payload, metadata, and ID boilerplate from publish-only and event-store application paths without hiding caller-owned infrastructure.

**Architecture:** `EnvelopePublisher` models only durable envelope publication. `TypedPublisher` uses it to build envelopes from typed messages. `TypedEventStore` performs the analogous conversion before calling `EventStore`. The NATS publisher receives a typed constructor helper; distributed Todo injects both typed facades.

**Tech Stack:** Rust 2024, `catga-core`, `catga-nats`, `catga-codec-memorypack`, `catga-memory`, Tokio.

---

### Task 1: Add the publish-only typed facade

**Files:**
- Create: `crates/catga-core/src/typed_publisher.rs`
- Modify: `crates/catga-core/src/lib.rs`
- Modify: `tests/typed_transport.rs`

- [ ] **Step 1: Write failing behavioral tests**

Add a `RecordingPublisher` implementing the new absent `EnvelopePublisher` trait, then prove the intended API:

```rust
let publisher = TypedPublisher::new_with_codec(recording, ids()?, TestCodec);
publisher.publish(&TestMessage(7)).await?;
let envelope = recording.published().pop().expect("one envelope");
assert_eq!(envelope.message_type(), std::any::type_name::<TestMessage>());
assert_eq!(envelope.schema_version(), 1);
assert_eq!(envelope.metadata().quality_of_service(), QualityOfService::AtLeastOnce);
```

Add a scoped-correlation test asserting an inherited correlation ID appears in the captured envelope.

- [ ] **Step 2: Verify red**

Run: `rtk cargo test -p catga-tests --test typed_transport typed_publisher`

Expected: compilation fails because `EnvelopePublisher` and `TypedPublisher` are absent.

- [ ] **Step 3: Implement the smallest reusable API**

Define `EnvelopePublisher` in `typed_publisher.rs` and give every `MessageTransport` a blanket implementation:

```rust
#[async_trait]
pub trait EnvelopePublisher: Send + Sync {
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()>;
}

#[async_trait]
impl<T: MessageTransport + ?Sized> EnvelopePublisher for T {
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        MessageTransport::publish(self, envelope).await
    }
}
```

Implement `TypedPublisher<T, C>` with `new_with_codec`, `new_with_shared_codec`, and
`publish<M>(&self, &M)`. Its envelope construction must mirror `TypedTransport`: message ID,
correlation context, message type, schema version, priority, and AtLeastOnce QoS. Re-export both
types from `catga_core`.

- [ ] **Step 4: Verify green**

Run: `rtk cargo test -p catga-tests --test typed_transport typed_publisher`

Expected: all typed-publisher tests pass.

### Task 2: Add TypedEventStore

**Files:**
- Create: `crates/catga-core/src/typed_event_store.rs`
- Modify: `crates/catga-core/src/lib.rs`
- Modify: `crates/catga-auto/src/lib.rs`
- Create: `tests/typed_event_store.rs`
- Modify: `tests/Cargo.toml`

- [ ] **Step 1: Write failing tests**

Use `MemoryEventStore` and `MemoryPackCodec` to prove:

```rust
let events = TypedEventStore::new_with_codec(store, ids()?, MemoryPackCodec::default());
events.append_new_event("order-7", &OrderCreated { id: 7 }).await?;
let page = store.read_page("order-7", 0, 1).await?;
assert_eq!(page.events()[0].envelope().message_type(), std::any::type_name::<OrderCreated>());
```

Add a test that `append_event("order-7", event, Some(-1))` preserves the supplied expected
version and returns `ErrorCode::Conflict` on a second new-stream append. Add a correlation test
using `scope_transport_context`.

- [ ] **Step 2: Verify red**

Run: `rtk cargo test -p catga-tests --test typed_event_store`

Expected: compilation fails because `TypedEventStore` is absent.

- [ ] **Step 3: Implement the facade**

Store `Arc<S>`, `Arc<dyn DistributedIdGenerator>`, and `Arc<C>`. Implement:

```rust
pub async fn append_event<E>(&self, stream_id: &str, event: &E, expected_version: Option<i64>)
    -> CatgaResult<i64>
where E: Event, C: PayloadEncoder<E>;

pub async fn append_new_event<E>(&self, stream_id: &str, event: &E) -> CatgaResult<i64>
where E: Event, C: PayloadEncoder<E>;
```

Build one versioned envelope and delegate to `EventStore::append(stream_id, vec![envelope], expected_version)`.
`append_new_event` delegates with `Some(-1)`; it must not swallow conflicts. Re-export it from
`catga_core` and `catga_auto`.

- [ ] **Step 4: Verify green**

Run: `rtk cargo test -p catga-tests --test typed_event_store`

Expected: all tests pass.

### Task 3: Connect NatsPublisher without changing its lifecycle role

**Files:**
- Modify: `crates/catga-nats/src/publisher.rs`
- Modify: `crates/catga-nats/src/lib.rs` documentation if needed
- Create: `crates/catga-nats/tests/typed_publisher.rs`

- [ ] **Step 1: Write the failing compile contract**

Write a compile-level test exercising:

```rust
fn typed<C>(publisher: Arc<NatsPublisher>, ids: Arc<dyn DistributedIdGenerator>, codec: C)
    -> TypedPublisher<NatsPublisher, C> {
    publisher.typed(ids, codec)
}
```

- [ ] **Step 2: Verify red**

Run: `rtk cargo test -p catga-nats --test typed_publisher`

Expected: compilation fails because `NatsPublisher::typed` and its `EnvelopePublisher` implementation are absent.

- [ ] **Step 3: Implement only the adapter**

Implement `EnvelopePublisher for NatsPublisher<C>` by delegating to its inherent
`NatsPublisher::publish`. Add:

```rust
pub fn typed<P>(self: Arc<Self>, ids: Arc<dyn DistributedIdGenerator>, codec: P)
    -> TypedPublisher<Self, P> {
    TypedPublisher::new_with_codec(self, ids, codec)
}
```

Do not add `receive`, a consumer, or a background task.

- [ ] **Step 4: Verify green**

Run: `rtk cargo test -p catga-nats --test typed_publisher`

Expected: PASS without a NATS server.

### Task 4: Migrate distributed Todo to typed business operations

**Files:**
- Modify: `examples/src/distributed/todo.rs`
- Modify: `examples/src/distributed/todo_api.rs`
- Modify: `examples/src/distributed/todo_worker.rs`
- Modify: `examples/Cargo.toml` only if a feature/export requires it
- Modify: `tests/examples.rs`

- [ ] **Step 1: Write the failing source contract**

Assert Todo API contains `commands.publish(&command)` and worker contains
`events.append_new_event`, while their business paths contain neither `Envelope::new` nor
`encode_payload`.

- [ ] **Step 2: Verify red**

Run: `rtk cargo test -p catga-tests --test examples distributed_todo_uses_typed_publish_and_event_store`

Expected: FAIL against the manual envelope code.

- [ ] **Step 3: Migrate business code**

Keep NATS configuration and process ownership explicit. In API construction, create an
application-owned `Arc<NatsPublisher>`, construct `publisher.typed(ids, MemoryPackCodec::default())`,
and store it as `TypedPublisher`. In the worker, construct `TypedEventStore` from the existing NATS
store, existing ID generator, and MemoryPack codec. Preserve the explicit `Conflict` handling after
`append_new_event`.

- [ ] **Step 4: Verify green**

Run: `rtk cargo test -p catga-tests --test examples distributed_todo_uses_typed_publish_and_event_store && rtk cargo check -p catga-examples --bins`

Expected: PASS.

### Task 5: Public API verification and documentation

**Files:**
- Modify: `docs/examples.md`

- [ ] **Step 1: Document the ownership boundary**

Explain that typed publisher/event-store facades own no connections or tasks and accept caller-built
adapters, IDs, and codecs. Keep raw `Envelope`, `EventStore`, and `NatsPublisher::publish` documented
as advanced escape hatches.

- [ ] **Step 2: Run validation**

Run: `rtk cargo fmt --all -- --check && rtk cargo clippy -p catga-core -p catga-nats -p catga-auto --all-targets --all-features -- -D warnings && rtk git diff --check`

Expected: no formatting, lint, or whitespace failures.

- [ ] **Step 3: Run confidence suite**

Run: `rtk cargo test -p catga-tests --test typed_transport && rtk cargo test -p catga-tests --test typed_event_store && rtk cargo test -p catga-tests --test examples && rtk cargo test -p catga-examples --all-features`

Expected: all suites pass; Docker E2E remains CI/manual-release coverage.
