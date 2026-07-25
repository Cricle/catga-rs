# RobustMQ RPC Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add ergonomic RobustMQ request server and reply APIs with bounded backpressure.

**Architecture:** Share `MQ9Client` behind `Arc`; bridge the callback-based SDK subscription to a bounded Tokio receiver. Keep Postcard envelope encoding and private per-request reply mailboxes.

**Tech Stack:** Rust, Tokio mpsc/oneshot, RobustMQ SDK, Postcard, Catga envelope APIs.

---

### Task 1: Specify the public server behavior

**Files:**
- Modify: `tests/robustmq.rs`
- Modify: `tests/Cargo.toml`

- [x] **Step 1: Add the public API integration test**

```rust
let mut server = MailboxRequestServer::subscribe(client.clone(), &mailbox, 8).await?;
let request = server.next().await?;
assert_eq!(request.envelope().reply_to(), Some("reply"));
request.respond(response).await?;
```

- [x] **Step 2: Verify the focused target compiles and skips without a configured service**

Run: `rtk proxy env CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p catga-tests --test robustmq`

Expected: the public server API compiles; the request/server integration test
skips when `CATGA_ROBUSTMQ_URL` is not configured.

### Task 2: Implement the request server

**Files:**
- Modify: `crates/catga-robustmq/src/client.rs`
- Modify: `crates/catga-robustmq/src/lib.rs`

- [x] **Step 1: Add the minimal public types**

```rust
pub struct MailboxRequestServer { subscription: Option<Subscription>, requests: mpsc::Receiver<CatgaResult<MailboxRequest>> }
pub struct MailboxRequest { client: Arc<MQ9Client>, envelope: Envelope }
```

- [x] **Step 2: Bridge the RobustMQ callback to a bounded channel**

```rust
let (sender, requests) = mpsc::channel(capacity);
let subscription = client.subscribe(mailbox_id, move |message| {
    let sender = sender.clone();
    async move { let _ = sender.send(codec.decode(&message.payload).map(...)).await; }
}, None, queue_group).await?;
```

- [x] **Step 3: Add response and lifecycle handling**

```rust
pub async fn respond(self, response: Envelope) -> CatgaResult<()> { /* validate reply_to; encode; send */ }
impl Drop for MailboxRequestServer { fn drop(&mut self) { if let Some(subscription) = self.subscription.take() { subscription.unsubscribe(); } } }
```

- [x] **Step 4: Run the RobustMQ test**

Run: `rtk proxy env CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p catga-tests --test robustmq`

Expected: PASS or deliberate request/server integration-test skip when
`CATGA_ROBUSTMQ_URL` is unset. A normal NATS endpoint runs the separate
missing-control-plane regression test.

### Task 3: Verify quality gates

**Files:**
- Modify: none

- [x] **Step 1: Format and inspect patch**

Run: `rtk proxy env CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo fmt --all -- --check && rtk git diff --check`

Expected: both commands succeed.

- [x] **Step 2: Run focused lint and workspace test suite**

Run: `rtk proxy env CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --workspace --all-targets -- -D warnings && rtk proxy env CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --workspace`

Expected: all checks pass.
