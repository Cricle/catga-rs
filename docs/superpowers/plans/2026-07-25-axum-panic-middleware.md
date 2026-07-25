# Axum Endpoint Panic Middleware Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in Axum middleware that converts handler unwinds into Catga's stable internal-error response.

**Architecture:** The middleware wraps `Next::run` in `AssertUnwindSafe` and `FutureExt::catch_unwind`. The normal path returns Axum's original response unchanged; the unwind path delegates to `CatgaHttpError` rather than creating another HTTP error mapper.

**Tech Stack:** Rust, Axum 0.8, futures 0.3, tower 0.5 test utilities, Tokio.

---

### Task 1: Specify panic containment at the HTTP boundary

**Files:**
- Modify: `tests/Cargo.toml`
- Modify: `tests/axum.rs`

- [ ] **Step 1: Add the integration-test dependency**

```toml
tower = "0.5"
```

- [ ] **Step 2: Write the failing test**

```rust
#[tokio::test]
async fn endpoint_panic_middleware_returns_a_stable_internal_error() {
    let app = Router::new()
        .route("/panic", post(|| async { panic!("test endpoint panic") }))
        .layer(middleware::from_fn(endpoint_panic_middleware));
    let response = app
        .oneshot(Request::post("/panic").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}
```

- [ ] **Step 3: Run the focused test to verify it fails**

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test axum endpoint_panic_middleware_returns_a_stable_internal_error --quiet`

Expected: compilation fails because `endpoint_panic_middleware` is not exported.

### Task 2: Add the middleware with no duplicate error mapping

**Files:**
- Modify: `crates/catga-axum/Cargo.toml`
- Modify: `crates/catga-axum/src/lib.rs`

- [ ] **Step 1: Add the runtime future extension dependency**

```toml
futures = "0.3"
```

- [ ] **Step 2: Implement the middleware**

```rust
pub async fn endpoint_panic_middleware(request: AxumRequest, next: Next) -> Response {
    match AssertUnwindSafe(next.run(request)).catch_unwind().await {
        Ok(response) => response,
        Err(_) => CatgaHttpError::from(CatgaError::new(
            ErrorCode::Internal,
            "endpoint handler panicked",
        ))
        .into_response(),
    }
}
```

- [ ] **Step 3: Run the focused test to verify it passes**

Run: `rtk cargo test --manifest-path tests/Cargo.toml --test axum endpoint_panic_middleware_returns_a_stable_internal_error --quiet`

Expected: one passing test with a `500` Catga JSON response.

### Task 3: Record migration evidence and verify the workspace

**Files:**
- Modify: `docs/source-compatibility-matrix.md`

- [ ] **Step 1: Update the HTTP compatibility row**

Record that `endpoint_panic_middleware` is the opt-in Rust replacement for
the upstream endpoint exception middleware.

- [ ] **Step 2: Run quality gates**

Run: `rtk cargo fmt --check && rtk cargo clippy -p catga-axum --all-targets -- -D warnings && rtk env RUSTDOCFLAGS='-D warnings' cargo doc -p catga-axum --no-deps && rtk cargo test --workspace --all-targets --quiet`

Expected: all commands pass. Then run the production panic-path and
excluded-broker audits documented in the migration matrix.
