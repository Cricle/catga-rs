# Axum Result Response Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert successful and failed `CatgaResult` values into idiomatic Axum responses without duplicate error mapping or unnecessary allocations.

**Architecture:** A public extension trait in `catga-axum` owns `CatgaResult<T>` at the HTTP boundary. It delegates errors to the existing `CatgaHttpError`, serializes successful values via Axum `Json`, and has a dedicated created response path for the caller-supplied `Location` header.

**Tech Stack:** Rust, Axum 0.8, serde, `catga-core`, Tokio integration tests.

---

### Task 1: Specify successful result conversion

**Files:**
- Modify: `tests/axum.rs`
- Modify: `crates/catga-axum/src/lib.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn catga_result_response_serializes_success_with_the_requested_status() {
    let response = Ok::<_, CatgaError>(ForwardRequest { value: 7 })
        .into_catga_response(StatusCode::ACCEPTED);
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(to_bytes(response.into_body(), 1024).await.unwrap(), r#"{"value":7}"#);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `rtk cargo test -p catga-integration-tests --test axum catga_result_response_serializes_success_with_the_requested_status --quiet`

Expected: failure because `IntoCatgaHttpResponse` and `into_catga_response` do not exist.

- [ ] **Step 3: Implement the minimal trait method**

```rust
pub trait IntoCatgaHttpResponse {
    fn into_catga_response(self, success_status: StatusCode) -> Response;
}

impl<T> IntoCatgaHttpResponse for CatgaResult<T>
where
    T: Serialize,
{
    fn into_catga_response(self, success_status: StatusCode) -> Response {
        match self {
            Ok(value) if success_status == StatusCode::NO_CONTENT => success_status.into_response(),
            Ok(value) => (success_status, Json(value)).into_response(),
            Err(error) => CatgaHttpError::from(error).into_response(),
        }
    }
}
```

- [ ] **Step 4: Run the focused test to verify it passes**

Run: `rtk cargo test -p catga-integration-tests --test axum catga_result_response_serializes_success_with_the_requested_status --quiet`

Expected: one passing test.

### Task 2: Cover no-content and created responses

**Files:**
- Modify: `tests/axum.rs`
- Modify: `crates/catga-axum/src/lib.rs`

- [ ] **Step 1: Write failing tests**

```rust
assert!(to_bytes(Ok::<_, CatgaError>(ForwardRequest { value: 7 })
    .into_catga_response(StatusCode::NO_CONTENT).into_body(), 1024).await.unwrap().is_empty());
let response = Ok::<_, CatgaError>(ForwardRequest { value: 7 })
    .into_catga_created("/orders/7");
assert_eq!(response.headers()[LOCATION], "/orders/7");
```

- [ ] **Step 2: Run the focused Axum test and verify it fails**

Run: `rtk cargo test -p catga-integration-tests --test axum catga_result_response --quiet`

Expected: failure because `into_catga_created` is absent.

- [ ] **Step 3: Add `into_catga_created`**

```rust
fn into_catga_created(self, location: &str) -> Response {
    match self {
        Ok(value) => match HeaderValue::from_str(location) {
            Ok(location) => (StatusCode::CREATED, [(LOCATION, location)], Json(value)).into_response(),
            Err(_) => CatgaHttpError::from(CatgaError::new(ErrorCode::Internal, "invalid Location header")).into_response(),
        },
        Err(error) => CatgaHttpError::from(error).into_response(),
    }
}
```

- [ ] **Step 4: Run the focused test to verify it passes**

Run: `rtk cargo test -p catga-integration-tests --test axum catga_result_response --quiet`

Expected: all result-response tests pass.

### Task 3: Prove error delegation and quality gates

**Files:**
- Modify: `tests/axum.rs`
- Modify: `docs/source-compatibility-matrix.md`

- [ ] **Step 1: Write a failing error-delegation test**

```rust
let response = Err::<ForwardRequest, _>(CatgaError::new(ErrorCode::NotFound, "missing"))
    .into_catga_response(StatusCode::OK);
assert_eq!(response.status(), StatusCode::NOT_FOUND);
```

- [ ] **Step 2: Run the focused test and verify it passes through `CatgaHttpError`**

Run: `rtk cargo test -p catga-integration-tests --test axum catga_result_response --quiet`

Expected: the test passes without a second error mapping table.

- [ ] **Step 3: Update migration evidence**

Add the result-response trait to the HTTP row of `docs/source-compatibility-matrix.md` after all tests pass.

- [ ] **Step 4: Run quality gates**

Run: `rtk cargo fmt --check && rtk cargo clippy -p catga-axum --all-targets -- -D warnings && RUSTDOCFLAGS='-D warnings' rtk cargo doc -p catga-axum --no-deps && rtk cargo test -p catga-integration-tests --test axum --quiet`

Expected: all commands pass. Then run the production panic-path and excluded-broker audits documented in the migration matrix.
