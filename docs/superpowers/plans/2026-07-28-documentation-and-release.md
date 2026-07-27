# Documentation and Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace historical documents with Rustdoc, concise examples, a simple README, and tag-driven crates.io publishing.

**Architecture:** Public crate roots and README are the documentation surface. Runnable assertions live in Rustdoc doctests while full behavioral coverage remains under `tests/`. GitHub Actions separates quality checks from protected `v*` publishing.

**Tech Stack:** Rustdoc, Cargo examples/doctests, GitHub Actions, crates.io token secret.

---

### Task 1: Replace the documentation surface

**Files:**
- Delete: `docs/`
- Modify: `README.md`
- Modify: public crate `src/lib.rs` roots
- Test: `tests/documentation.rs`

- [ ] Write a failing external documentation-contract test that asserts the README contains `Quick start`, `Flow`, `FlowStore`, and `Features` headings, then run `rtk cargo test -p catga-tests --test documentation` and observe failure.
- [ ] Replace README with installation, one minimal mediator snippet, Flow snippet, FlowStore feature table, external-service boundary, and verification commands.
- [ ] Add concise crate-root Rustdoc with runnable asserted examples for `catga-core`, `catga-flow`, `catga-memory`, and codec crates; external adapters use `no_run` examples.
- [ ] Delete `docs/` only after its required content has been moved into README/Rustdoc, then run `rtk cargo test --doc --workspace --all-features` and `rtk cargo test -p catga-tests --test documentation`.

### Task 2: Add simple compiling examples

**Files:**
- Create: `crates/catga-core/examples/mediator_basics.rs`
- Create: `crates/catga-flow/examples/flow_basics.rs`
- Create: `crates/catga-flow-store/examples/flow_store_features.rs`
- Create: `crates/catga-memory/examples/transport_basics.rs`
- Test: `tests/examples.rs`

- [ ] Write failing external test invoking `cargo check --examples --workspace --all-features`, then add the four examples using `CatgaResult`, explicit construction, and no credentials.
- [ ] Run `rtk cargo check --examples --workspace --all-features` and `rtk cargo test -p catga-tests --test examples`; commit the example-only change.

### Task 3: Add quality and release workflows

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `.github/workflows/release.yml`
- Test: `tests/workflows.rs`

- [ ] Write failing tests that parse workflow text and assert CI has fmt, Clippy, tests, doctests, Rustdoc; assert release triggers only `v*`, references `secrets.CRATES_KEY`, sets `CARGO_REGISTRY_TOKEN`, and invokes `cargo publish --dry-run` before publish.
- [ ] Add CI for pull requests and `main`, with stable Rust, cache, format, all-feature Clippy, tests/doctests, Rustdoc warnings denied.
- [ ] Add tag-only release workflow with per-crate dependency order, `cargo publish --dry-run`, publish, and already-published version skip; never create or push tags.
- [ ] Run `rtk cargo test -p catga-tests --test workflows`, YAML parse checks, workspace checks, then commit.

### Task 4: Final verification and delivery

- [ ] Run `rtk cargo fmt --all -- --check`, `rtk cargo clippy --workspace --all-targets --all-features -- -D warnings`, `rtk cargo test --workspace --all-features`, `rtk cargo test --doc --workspace --all-features`, `rtk env RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps`, and `rtk git diff --check`.
- [ ] Confirm `test ! -d docs`, do not create a tag, commit and push the final change.
