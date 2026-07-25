# Envelope Headers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans`
> to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for
> tracking.

**Goal:** Carry source transport metadata through Rust envelopes and Postcard
without penalizing messages that do not have headers.

**Architecture:** `catga-core` owns immutable, bounded header values on the
envelope. `catga-codec-postcard` serializes them with a defaulted trailing wire
field and maps decoded data through the core validator. Existing adapter codec
use carries the enriched envelope unchanged.

**Tech Stack:** Rust 2024, `Arc<[T]>`, Postcard, Serde, Tokio integration tests.

---

### Task 1: Specify core header semantics

**Files:**
- Modify: `tests/codec.rs`
- Modify: `tests/message.rs`

- [x] **Step 1: Write failing core header tests**

  Express the intended public API:

  ```rust
  let headers = EnvelopeHeaders::try_from([("tenant", "blue"), ("route", "priority")])?;
  let envelope = Envelope::new(1, "work", vec![], metadata).with_headers(headers.clone());
  assert_eq!(envelope.header("tenant"), Some("blue"));
  assert_eq!(envelope.headers().collect::<Vec<_>>(), vec![("tenant", "blue"), ("route", "priority")]);
  ```

  Add duplicate-key, blank-key, maximum-count, and maximum-byte validation cases.

- [x] **Step 2: Run the core-focused target and confirm the API is absent**

  Run: `rtk cargo test --manifest-path tests/Cargo.toml --test message envelope_headers --quiet`

  Expected: FAIL because `EnvelopeHeaders` and envelope header methods do not exist.

### Task 2: Implement immutable bounded headers

**Files:**
- Modify: `crates/catga-core/src/store.rs`
- Modify: `crates/catga-core/src/lib.rs`

- [x] **Step 1: Add documented header types and validation**

  Define `EnvelopeHeader` and `EnvelopeHeaders` backed by
  `Option<Arc<[EnvelopeHeader]>>`, with `TryFrom<[(K, V); N]>`/iterator support
  or a validated constructor. Reject duplicate or blank keys, more than 64
  headers, and more than 8 KiB combined UTF-8 bytes with `ErrorCode::Validation`.

- [x] **Step 2: Add zero-copy envelope accessors**

  Add `Envelope::with_headers`, `Envelope::headers`, and `Envelope::header`.
  Preserve `None` storage for envelopes without headers and document clone,
  lookup, and ordering behavior.

- [x] **Step 3: Run the core header regressions**

  Run: `rtk cargo test --manifest-path tests/Cargo.toml --test message envelope_headers --quiet`

  Expected: PASS.

### Task 3: Persist headers through Postcard and typed transport

**Files:**
- Modify: `tests/codec.rs`
- Modify: `crates/catga-codec-postcard/src/wire.rs`
- Modify: `crates/catga-codec-postcard/src/lib.rs`

- [x] **Step 1: Write failing codec and typed facade regressions**

  Add a codec round trip with headers, a raw legacy `EnvelopeWire` payload that
  omits headers, and a typed `publish_with_headers` assertion against memory
  transport. Assert a malformed decoded duplicate key returns validation.

- [x] **Step 2: Run the codec target and confirm missing behavior**

  Run: `rtk cargo test --manifest-path tests/Cargo.toml --test codec headers --quiet`

  Expected: FAIL because the wire format and typed publication method omit headers.

- [x] **Step 3: Add defaulted header wire data and contextual typed publication**

  Add `HeaderWire`, a defaulted `headers` field, and fallible wire-to-envelope
  conversion. Implement `publish_with_headers` and `send_to_with_headers`
  through the shared envelope builder, cloning only the header `Arc`.

- [x] **Step 4: Run codec regressions**

  Run: `rtk cargo test --manifest-path tests/Cargo.toml --test codec headers --quiet`

  Expected: PASS.

### Task 4: Document and verify

**Files:**
- Modify: `docs/source-compatibility-matrix.md`
- Modify: `docs/superpowers/specs/2026-07-25-envelope-headers-design.md`

- [x] **Step 1: Record the TransportContext metadata mapping**

  State that immutable bounded envelope headers replace the source mutable
  metadata dictionary and are serialized by all codec-backed adapters.

- [x] **Step 2: Run quality gates**

  ```bash
  rtk cargo fmt --check
  rtk cargo test --manifest-path tests/Cargo.toml --test message --quiet
  rtk cargo test --manifest-path tests/Cargo.toml --test codec --quiet
  rtk cargo test -p catga-core --quiet
  rtk cargo clippy -p catga-core -p catga-codec-postcard --all-targets -- -D warnings
  rtk env RUSTDOCFLAGS='-D warnings' cargo doc -p catga-core -p catga-codec-postcard --no-deps
  rtk rg -n '\.(unwrap|expect)[[:space:]]*\(|(unreachable|todo|unimplemented)![[:space:]]*\(' crates/catga-core/src crates/catga-codec-postcard/src
  rtk git diff --check
  ```

  Expected: every command succeeds and the production no-panic search has no output.
