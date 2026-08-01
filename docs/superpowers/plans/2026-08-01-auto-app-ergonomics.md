# AutoApp Ergonomics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `catga-auto` the concise default composition path for local and Axum applications while preserving Catga's explicit low-level APIs.

**Architecture:** Add consuming fluent registration methods to `AutoAppBuilder`, make `build` one-shot through ownership, and expose the already-owned mediator `Arc` through `AutoApp`. An Axum feature-gated state helper merely adapts that `Arc` to `MediatorState`; it does not own a router or spawn work. Migrate the two introductory examples to demonstrate this path.

**Tech Stack:** Rust 2024, `catga-core`, `catga-auto`, `catga-axum`, Axum 0.8, Tokio.

---

### Task 1: Prove the fluent one-shot application API

**Files:**
- Modify: `crates/catga-auto/tests/builder.rs`
- Modify: `crates/catga-auto/src/lib.rs:45-106`

- [ ] **Step 1: Write the failing test**

Add a test that registers a request without turbofish syntax, consumes the builder with `build`, and dispatches through both the mediator and handle:

```rust
#[tokio::test]
async fn fluent_auto_app_registration_infers_the_message_type() -> CatgaResult<()> {
    let app = AutoApp::builder().request(PingHandler)?.build()?;

    assert_eq!(app.mediator().send(Ping).await?, "pong");
    assert_eq!(app.handle().send(Ping).await?, "pong");
    Ok(())
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `rtk cargo test -p catga-auto --test builder fluent_auto_app_registration_infers_the_message_type`

Expected: compilation fails because `AutoAppBuilder::request` does not exist.

- [ ] **Step 3: Write the minimal implementation**

Add consuming `request`, `command`, and `event` methods that return `CatgaResult<Self>` (or `Self` for event) and internally delegate to the existing registry. Change `build(&mut self)` to `build(self)`, moving the registry and binding the existing handle. Retain the existing `register_*` APIs for callers that need imperative registration.

```rust
pub fn request<M, H>(mut self, handler: H) -> CatgaResult<Self>
where
    M: Request,
    H: Handler<M> + 'static,
{
    self.registry.register_request(handler)?;
    Ok(self)
}

pub fn build(self) -> CatgaResult<AutoApp> {
    let mediator = Arc::new(Mediator::new(self.registry));
    self.handle.bind(Arc::clone(&mediator))?;
    Ok(AutoApp { mediator, handle: self.handle, shutdown: self.shutdown })
}
```

- [ ] **Step 4: Run the focused test to verify it passes**

Run: `rtk cargo test -p catga-auto --test builder fluent_auto_app_registration_infers_the_message_type`

Expected: PASS.

### Task 2: Expose AutoApp's existing mediator ownership for integrations

**Files:**
- Modify: `crates/catga-auto/tests/builder.rs`
- Modify: `crates/catga-auto/src/lib.rs:116-140`

- [ ] **Step 1: Write the failing test**

Add a test that obtains an `Arc<Mediator>` through `mediator_arc`, drops the `AutoApp`, and dispatches successfully through the returned owner:

```rust
#[tokio::test]
async fn mediator_arc_keeps_the_built_application_graph_alive() -> CatgaResult<()> {
    let mediator = AutoApp::builder().request(PingHandler)?.build()?.mediator_arc();

    assert_eq!(mediator.send(Ping).await?, "pong");
    Ok(())
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `rtk cargo test -p catga-auto --test builder mediator_arc_keeps_the_built_application_graph_alive`

Expected: compilation fails because `mediator_arc` does not exist.

- [ ] **Step 3: Write the minimal implementation**

Add:

```rust
/// Clones the application-owned mediator for framework integration.
pub fn mediator_arc(&self) -> Arc<Mediator> {
    Arc::clone(&self.mediator)
}
```

- [ ] **Step 4: Run the focused test to verify it passes**

Run: `rtk cargo test -p catga-auto --test builder mediator_arc_keeps_the_built_application_graph_alive`

Expected: PASS.

### Task 3: Add an explicit, zero-work Axum state adapter

**Files:**
- Modify: `crates/catga-auto/tests/builder.rs`
- Modify: `crates/catga-auto/src/lib.rs:145-153`

- [ ] **Step 1: Write the failing test**

Feature-gate an Axum test that builds an app through `AutoApp`, passes `web::mediator_state(&app)` to `Router::with_state`, and serves a request handler extracting `MediatorState`.

```rust
#[cfg(feature = "axum")]
#[tokio::test]
async fn axum_state_uses_the_auto_app_mediator() -> CatgaResult<()> {
    let app = AutoApp::builder().request(PingHandler)?.build()?;
    let state = catga_auto::web::mediator_state(&app);
    assert_eq!(state.send(Ping).await?, "pong");
    Ok(())
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `rtk cargo test -p catga-auto --features axum --test builder axum_state_uses_the_auto_app_mediator`

Expected: compilation fails because `web::mediator_state` does not exist.

- [ ] **Step 3: Write the minimal implementation**

Use the already existing conversion and no other runtime machinery:

```rust
pub fn mediator_state(app: &AutoApp) -> catga_axum::MediatorState {
    catga_axum::MediatorState::from(app.mediator_arc())
}
```

- [ ] **Step 4: Run the focused test to verify it passes**

Run: `rtk cargo test -p catga-auto --features axum --test builder axum_state_uses_the_auto_app_mediator`

Expected: PASS.

### Task 4: Make quickstart and Axum examples demonstrate the default path

**Files:**
- Modify: `examples/src/quickstart/mediator.rs`
- Modify: `examples/src/web/axum_checkout.rs`
- Test: `tests/examples.rs`

- [ ] **Step 1: Write the failing static contract**

Extend `tests/examples.rs` with assertions that the mediator quickstart contains `AutoApp::builder`, and the Axum sample contains both `AutoApp::builder` and `web::mediator_state`, while neither contains `Registry::new`.

- [ ] **Step 2: Run the contract to verify it fails**

Run: `rtk cargo test -p catga-tests --test examples public_examples_use_auto_app_for_introductory_composition`

Expected: FAIL because both samples currently compose raw `Registry` values.

- [ ] **Step 3: Write the minimal example migration**

For the mediator quickstart, derive `catga_core::Message`, construct with:

```rust
let app = catga_auto::AutoApp::builder()
    .request(request_handler(|value: Double| async move { Ok(value.0 * 2) }))?
    .build()?;
let result = app.mediator().send(Double(21)).await?;
```

For the Axum example, replace `build_mediator` with `build_app -> CatgaResult<AutoApp>`, register handlers via consuming `request` calls, and pass `catga_auto::web::mediator_state(&app)` to `Router::with_state`.

- [ ] **Step 4: Run examples and static contract to verify they pass**

Run: `rtk cargo test -p catga-tests --test examples public_examples_use_auto_app_for_introductory_composition && rtk cargo test -p catga-examples --all-features && rtk cargo check -p catga-examples --bins`

Expected: PASS.

### Task 5: Validate the public API and documentation

**Files:**
- Modify: `docs/examples.md`
- Modify: `crates/catga-auto/src/lib.rs` documentation as required by public API changes

- [ ] **Step 1: Update documentation**

Describe `catga-auto` as the default application composition API, and link advanced users to raw `Registry` and transport APIs only when they need custom integration behavior.

- [ ] **Step 2: Format and run focused verification**

Run: `rtk cargo fmt --all -- --check && rtk cargo clippy -p catga-auto --all-targets --all-features -- -D warnings && rtk cargo test -p catga-auto --all-features && rtk git diff --check`

Expected: all commands pass with no warnings.

- [ ] **Step 3: Run workspace confidence checks**

Run: `rtk cargo test -p catga-tests --test examples && rtk cargo test -p catga-examples --all-features && rtk cargo clippy --workspace --all-targets --all-features -- -D warnings`

Expected: all commands pass with no warnings.
