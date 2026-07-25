# RobustMQ Priority Propagation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans`
> to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for
> tracking.

**Goal:** Preserve Catga envelope priority in RobustMQ request and response
delivery without adding runtime state or allocations.

**Architecture:** Extend the existing protocol-neutral `MailboxPriority`
conversion with an envelope accessor. Retain request priority while creating
typed Postcard response metadata, then use envelope priority at the two
hard-coded mailbox SDK send sites. No cache, task, or mutable context is
introduced.

**Tech Stack:** Rust 2024, Catga core envelopes, Postcard, RobustMQ SDK.

---

### Task 1: Establish priority behavior

**Files:**
- Modify: `tests/robustmq.rs`
- Modify: `tests/codec.rs`

- [x] **Step 1: Write the failing adapter mapping test**

  Add a table-driven assertion that `MailboxPriority::from_envelope` maps an
  envelope with `Low`, `Normal`, `High`, or `Critical` metadata to the
  corresponding SDK priority, with `High` and `Critical` both becoming
  `robustmq::Priority::High`.

- [x] **Step 2: Run the focused test and verify it fails because the method is absent**

  Run: `rtk cargo test --manifest-path tests/Cargo.toml --test robustmq mailbox_priority_uses_envelope_metadata --quiet`

  Expected: compile failure mentioning `from_envelope`.

- [x] **Step 3: Confirm typed responses retain request priority**

  Add a `PostcardCodec::typed_success` assertion for a request whose metadata
  is `MessagePriority::Critical`; assert the response metadata remains
  `Critical` before it reaches the adapter mapping.

- [x] **Step 4: Run the focused codec test**

  Run: `rtk cargo test --manifest-path tests/Cargo.toml --test codec typed_success_preserves_request_priority --quiet`

  Expected: FAIL because `response_metadata` currently constructs the default
  `MessagePriority::Normal` value.

### Task 2: Use envelope priority at mailbox send boundaries

**Files:**
- Modify: `crates/catga-codec-postcard/src/lib.rs`
- Modify: `crates/catga-robustmq/src/priority.rs`
- Modify: `crates/catga-robustmq/src/client.rs`

- [x] **Step 1: Preserve priority when creating typed response metadata**

  Change `response_metadata` to retain only the request priority in addition
  to its existing correlation behavior:

  ```rust
  MessageMetadata::new(request.metadata().message_id(), Some(correlation_id))
      .with_priority(request.metadata().priority())
  ```

  Do not copy request QoS, delivery mode, or scheduling boundary into a reply:
  they describe delivery of the original request, not the newly constructed
  response.

- [x] **Step 2: Add the allocation-free envelope priority conversion**

  Implement:

  ```rust
  pub const fn from_envelope(envelope: &Envelope) -> Self {
      Self::from_message_priority(envelope.metadata().priority())
  }
  ```

  Keep the existing `From<MessagePriority>` compatibility implementation and
  document the RobustMQ three-level collapse.

- [x] **Step 3: Replace hard-coded request and reply priorities**

  In `MailboxClient::request_to`, capture the request priority before the
  envelope is moved into the encoded payload, then pass `priority.as_sdk()` to
  `MQ9Client::send`. In `MailboxRequest::respond`, pass
  `MailboxPriority::from_envelope(&response).as_sdk()` to the SDK.

- [x] **Step 4: Run focused codec and adapter tests**

  Run: `rtk cargo test --manifest-path tests/Cargo.toml --test codec postcard_codec_builds_correlated_typed_success_and_failure_envelopes --quiet && rtk cargo test --manifest-path tests/Cargo.toml --test robustmq mailbox_priority_uses_envelope_metadata --quiet`

  Expected: PASS.

### Task 3: Record and verify the compatibility slice

**Files:**
- Modify: `docs/source-compatibility-matrix.md`
- Modify: `docs/superpowers/specs/2026-07-25-robustmq-priority-design.md`

- [x] **Step 1: Record the priority boundary**

  Document that mailbox requests and replies use immutable envelope metadata,
  with a deliberate `High`/`Critical` collapse mandated by RobustMQ's SDK.

- [x] **Step 2: Run scope-appropriate quality gates**

  ```bash
  rtk cargo fmt --check
  rtk cargo test --manifest-path tests/Cargo.toml --test robustmq --quiet
  rtk cargo test --manifest-path tests/Cargo.toml --test codec --quiet
  rtk cargo clippy -p catga-robustmq -p catga-codec-postcard --all-targets -- -D warnings
  rtk env RUSTDOCFLAGS='-D warnings' cargo doc -p catga-robustmq -p catga-codec-postcard --no-deps
  rtk rg -n '\\.(unwrap|expect)[[:space:]]*\\(|(unreachable|todo|unimplemented)![[:space:]]*\\(' crates/catga-robustmq/src crates/catga-codec-postcard/src
  rtk git diff --check
  ```

  Expected: every command succeeds and the production no-panic search has no
  output.
