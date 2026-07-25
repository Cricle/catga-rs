# Typed Postcard Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Rust-native typed Postcard transport facade that maps source generic transport operations onto owned Catga envelopes.

**Architecture:** `PostcardTransport<T>` stays in `catga-codec-postcard`, where serialization belongs. It builds owned envelopes using a shared ID generator and delegates all backend work to existing `MessageTransport` and `DestinationTransport` contracts. A typed delivery wrapper retains the existing acknowledgement token rather than recreating acknowledgement state.

**Tech Stack:** Rust 2024, Postcard, Tokio, Futures, `catga-core` transport traits, `catga-memory` regression transport.

---

### Task 1: Add failing typed transport regressions

**Files:**
- Modify: `tests/codec.rs`

- [x] **Step 1: Write typed event and reliable-event tests**

Define serializable event values and exercise a missing `PostcardTransport` API:

```rust
transport.publish_event(&Event(7)).await?;
assert_eq!(received.envelope().metadata().quality_of_service(), QualityOfService::AtMostOnce);

transport.publish_reliable_event(&ReliableEvent(8)).await?;
assert_eq!(received.envelope().metadata().quality_of_service(), QualityOfService::AtLeastOnce);
```

- [x] **Step 2: Run the focused target to verify it fails**

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test codec typed_postcard --quiet`

Expected: FAIL because `PostcardTransport` and its typed operations do not exist.

### Task 2: Implement owned typed publication and sending

**Files:**
- Modify: `crates/catga-codec-postcard/src/lib.rs`

- [x] **Step 1: Add `PostcardTransport<T>` and envelope construction**

Store `Arc<T>`, `Arc<dyn DistributedIdGenerator>`, and `PostcardCodec`. Use a
private `encode_envelope` helper that obtains one ID, propagates or roots the
ambient correlation ID, serializes the value, and applies its requested QoS.
The helper must return `CatgaResult<Envelope>` without panicking.

- [x] **Step 2: Add publish, event publish, reliable event publish, and destination send methods**

Require `Message + Serialize` for ordinary publication and `Event + Serialize`
for event publication. Require `DestinationTransport` only on send methods.
Parse the destination before encoding the payload. Delegate each produced
envelope into the existing backend trait method.

- [x] **Step 3: Run the focused target to verify it passes**

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test codec typed_postcard --quiet`

Expected: PASS.

### Task 3: Add bounded typed batches and decoded deliveries

**Files:**
- Modify: `tests/codec.rs`
- Modify: `crates/catga-codec-postcard/src/lib.rs`

- [x] **Step 1: Write failing batch and decode/acknowledgement regressions**

Use a memory transport to prove that typed batches preserve explicit bounded
concurrency and that `PostcardDelivery<T>::acknowledge` resolves the original
delivery token. Add a malformed payload case whose `receive::<T>` returns a
validation error after a negative acknowledgement attempt.

- [x] **Step 2: Run the focused target to verify the new cases fail**

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test codec typed_postcard --quiet`

Expected: FAIL because typed batches and typed delivery are absent.

- [x] **Step 3: Implement lazy bounded batches and `PostcardDelivery<T>`**

Use a lazy `stream::iter` plus `buffer_unordered` with validated positive
concurrency. Retain only `O(concurrency)` encoded envelopes/futures. `receive`
must decode from the envelope byte slice, preserve attempts, expose immutable
message and envelope views, and consume itself for acknowledgement actions.
On decode failure, request negative acknowledgement; retain the decode error
unless negative acknowledgement is the only failure.

The completed implementation also exposes source-compatible default batch
methods. They delegate to the explicit methods with
`DEFAULT_TRANSPORT_BATCH_CONCURRENCY`; a failing regression confirmed the
event default before those wrappers were added.

The completed scheduler now uses the same QoS policy: a failing durable-outbox
regression proved `schedule_event_at` writes `AtMostOnce` and
`schedule_reliable_event_at` writes `AtLeastOnce`; all delayed variants share
one checked delay calculation.

- [x] **Step 4: Run the focused target to verify it passes**

Ordinary message publication and destination-send regression coverage was
added after the shared facade was implemented. It verifies the generic source
default remains `AtLeastOnce`; the event and batch behavior above followed the
red-green cycle.

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test codec typed_postcard --quiet`

Expected: PASS.

### Task 4: Document the source mapping and run checks

**Files:**
- Modify: `docs/source-compatibility-matrix.md`
- Modify: `docs/superpowers/specs/2026-07-25-typed-postcard-transport-design.md`

- [x] **Step 1: Update the compatibility mapping**

Document that `PostcardTransport` maps source generic transport calls to typed,
caller-owned Rust operations; explain explicit event QoS methods and the
acknowledgement-owning delivery wrapper.

- [x] **Step 2: Run formatting, targeted tests, lint, documentation, and diff checks**

Run:

```bash
rtk cargo fmt --check
rtk cargo test --manifest-path tests/Cargo.toml --test codec --quiet
rtk cargo clippy -p catga-codec-postcard --all-targets -- -D warnings
rtk env RUSTDOCFLAGS='-D warnings' cargo doc -p catga-codec-postcard --no-deps
rtk git diff --check
```

Expected: every command succeeds.
