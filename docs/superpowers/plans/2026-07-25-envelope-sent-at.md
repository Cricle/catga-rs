# Envelope Sent-At Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans`
> to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for
> tracking.

**Goal:** Preserve source transport `SentAt` semantics in Rust envelopes and
Postcard without using mutable context, ID-derived time, or hidden tasks.

**Architecture:** `catga-core::Envelope` owns an optional UTC epoch-millisecond
timestamp with checked builders. `catga-codec-postcard` appends it after
headers, then decodes current, header-bearing historical, and original legacy
layouts only when each consumes all bytes.

**Tech Stack:** Rust 2024, `std::time::SystemTime`, Serde, Postcard.

---

### Task 1: Establish envelope timestamp behavior

**Files:**
- Modify: `tests/message.rs`

- [x] **Step 1: Write the failing envelope timestamp tests**

  Add a constructor assertion that `Envelope::new(...).sent_at()` returns a
  value between two `SystemTime::now()` observations. Add exact override
  assertions:

  ```rust
  let epoch = UNIX_EPOCH;
  let envelope = Envelope::new(1, "work", vec![], metadata).with_sent_at(epoch)?;
  assert_eq!(envelope.sent_at_unix_ms(), Some(0));
  assert_eq!(envelope.sent_at(), Some(epoch));
  ```

  Also assert a pre-epoch override returns `ErrorCode::Validation`.

- [x] **Step 2: Run the focused target and confirm the API is absent**

  Run: `rtk cargo test --manifest-path tests/Cargo.toml --test message envelope_sent_at --quiet`

  Expected: compile failure because the timestamp accessors and builders do
  not exist.

### Task 2: Implement bounded timestamp representation

**Files:**
- Modify: `crates/catga-core/src/store.rs`

- [x] **Step 1: Add documented timestamp storage and constructors**

  Add `sent_at_unix_ms: Option<u64>` to `Envelope`. Have `new` and `versioned`
  call a private checked `current_unix_ms()` helper. Add `sent_at`,
  `sent_at_unix_ms`, `with_sent_at`, and `with_sent_at_unix_ms` accessors.
  `with_sent_at` converts through `duration_since(UNIX_EPOCH)` and checked
  `u64::try_from`, mapping both failures to `ErrorCode::Validation`.

- [x] **Step 2: Run the focused core tests**

  Run: `rtk cargo test --manifest-path tests/Cargo.toml --test message envelope_sent_at --quiet`

  Expected: PASS.

### Task 3: Evolve the Postcard envelope layouts

**Files:**
- Modify: `tests/codec.rs`
- Modify: `crates/catga-codec-postcard/src/wire.rs`
- Modify: `crates/catga-codec-postcard/src/lib.rs`

- [x] **Step 1: Write failing current and historical wire tests**

  Add a current codec round-trip with an explicit `Some(0)` timestamp. Define
  a test-only historical header-bearing struct with all existing fields through
  `headers`, encode it with Postcard, and assert decoding succeeds with
  `sent_at_unix_ms() == None`. Retain the original legacy test.

- [x] **Step 2: Run the focused test and confirm old wire decoding fails**

  Run: `rtk cargo test --manifest-path tests/Cargo.toml --test codec sent_at --quiet`

  Expected: FAIL because the current decoder has no timestamp field or
  header-bearing fallback layout.

- [x] **Step 3: Add current and fallback wire structs**

  Append `sent_at_unix_ms: Option<u64>` to `EnvelopeWire`. Extract the prior
  header-bearing layout into a private `HeadersEnvelopeWire`; retain
  `LegacyEnvelopeWire`. Decode through `postcard::take_from_bytes`, requiring
  empty remainders for each fallback. Explicitly apply `None` when converting
  either historical layout to avoid constructor-time timestamps.

- [x] **Step 4: Run the focused codec regressions**

  Run: `rtk cargo test --manifest-path tests/Cargo.toml --test codec sent_at --quiet`

  Expected: PASS.

### Task 4: Document and verify

**Files:**
- Modify: `docs/source-compatibility-matrix.md`
- Modify: `docs/superpowers/specs/2026-07-25-envelope-sent-at-design.md`

- [x] **Step 1: Record the SentAt mapping**

  State that Rust uses optional UTC epoch milliseconds on `Envelope`, that
  current envelopes are timestamped without Snowflake coupling, and that old
  Postcard layouts preserve an unknown timestamp as `None`.

- [x] **Step 2: Run quality gates**

  ```bash
  rtk cargo fmt --check
  rtk cargo test --manifest-path tests/Cargo.toml --test message --quiet
  rtk cargo test --manifest-path tests/Cargo.toml --test codec --quiet
  rtk cargo clippy -p catga-core -p catga-codec-postcard --all-targets -- -D warnings
  rtk env RUSTDOCFLAGS='-D warnings' cargo doc -p catga-core -p catga-codec-postcard --no-deps
  rtk rg -n '\\.(unwrap|expect)[[:space:]]*\\(|(unreachable|todo|unimplemented)![[:space:]]*\\(' crates/catga-core/src crates/catga-codec-postcard/src
  rtk git diff --check
  ```

  Expected: every command succeeds and the production no-panic search has no
  output.
