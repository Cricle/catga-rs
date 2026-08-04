# Catga-rs Code Audit and Refactoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Comprehensive code audit and structural refactoring of catga-rs codebase covering architecture reorganization, code quality improvements, and security confirmation.

**Architecture:** Use folders (not new crates) to reorganize large modules. Split files over 600 lines. Consolidate duplicate validation error formatting. Maintain backward compatibility through `pub use` re-exports.

**Tech Stack:** Rust, Cargo workspace, catga-core crate, DashMap, tokio

## Global Constraints

- **Backward compatibility:** All public API exports must remain functional
- **Tests:** `cargo test` must pass after each task
- **Clippy:** `cargo clippy` must pass with no new warnings
- **No DoS protection:** Skip DoS-related changes per design spec
- **Rust edition:** 2021

---

## Task Map

| Task | Phase | File Changes |
|------|-------|--------------|
| 1 | Phase 1 | `crates/catga-core/src/flow/` directory structure |
| 2 | Phase 1 | `crates/catga-core/src/validation/` directory structure |
| 3 | Phase 1 | `crates/catga-core/src/lib.rs` refactoring |
| 4 | Phase 2 | `validation/shared.rs` for error formatting |
| 5 | Phase 2 | Module-level documentation |
| 6 | Phase 3 | DashMap usage analysis |
| 7 | Phase 4 | Security confirmation and documentation |

---

## Task 1: Create flow/ subdirectory structure

**Files:**
- Modify: `crates/catga-core/src/flow/mod.rs` (create if not exists)
- Modify: `crates/catga-core/src/flow/dsl.rs` (add module docs)
- Modify: `crates/catga-core/src/flow/runtime.rs` (add module docs)

**Interfaces:**
- Consumes: Existing `flow/dsl.rs`, `flow/runtime.rs`, `flow/dsl_checkpoint.rs`
- Produces: Organized `flow/mod.rs` with clear exports

- [ ] **Step 1: Create flow/mod.rs with module documentation**

```rust
//! Flow orchestration DSL and runtime.
//!
//! This module provides the [`Flow`] builder for composing compensating transactions
//! (saga pattern) with typed step closures and automatic rollback on failure.

pub mod dsl;
pub mod dsl_checkpoint;
pub mod runtime;
```

- [ ] **Step 2: Add module-level documentation to dsl.rs**

Add at the top of `crates/catga-core/src/flow/dsl.rs`:
```rust
//! Flow DSL builder for composing compensating transactions.
//!
//! # Example
//! ```
//! use catga_core::flow::{Flow, FlowResult};
//!
//! async fn example() -> FlowResult {
//!     Flow::new("my-flow")
//!         .step(
//!             || async { Ok(()) },  // forward
//!             || async { Ok(()) },  // compensating
//!         )
//!         .run()
//!         .await
//! }
//! ```
```

- [ ] **Step 3: Add module-level documentation to runtime.rs**

Add at the top of `crates/catga-core/src/flow/runtime.rs`:
```rust
//! Runtime execution engine for Flow orchestration.
//!
//! Handles step execution, compensation on failure, suspension, and checkpointing.
```

- [ ] **Step 4: Verify compilation**

Run: `cargo build -p catga-core 2>&1 | head -50`
Expected: SUCCESS (no errors)

- [ ] **Step 5: Run tests**

Run: `cargo test -p catga-core --lib 2>&1 | tail -20`
Expected: All tests pass

- [ ] **Step 6: Commit**

```bash
git add crates/catga-core/src/flow/
git commit -m "refactor: organize flow module with mod.rs and documentation"
```

---

## Task 2: Create validation/ subdirectory structure

**Files:**
- Create: `crates/catga-core/src/validation/mod.rs`
- Modify: `crates/catga-core/src/validation/endpoint.rs` (rename from validation.rs)
- Modify: `crates/catga-core/src/validation/behavior.rs` (rename from behaviors/validation.rs)

**Interfaces:**
- Consumes: `crates/catga-core/src/validation.rs`, `crates/catga-core/src/behaviors/validation.rs`
- Produces: `validation/mod.rs` exporting `EndpointValidation`, `ValidationBehavior`, `Validator`

- [ ] **Step 1: Create validation directory**

```bash
mkdir -p crates/catga-core/src/validation
```

- [ ] **Step 2: Create validation/endpoint.rs**

Create `crates/catga-core/src/validation/endpoint.rs` with content from `validation.rs` (entire file):
- `EndpointValidation` struct
- `validate_required`, `validate_positive`, `validate_min_length`, `validate_max_length`, `validate_range`, `validate_not_empty`, `validate_min_count` functions

- [ ] **Step 3: Create validation/behavior.rs**

Create `crates/catga-core/src/validation/behavior.rs` with content from `behaviors/validation.rs`:
- `Validator` trait
- `ValidationBehavior` struct
- `validation_error` function

- [ ] **Step 4: Create validation/mod.rs**

```rust
//! Input validation helpers for endpoints and behavior pipelines.
//!
//! # Endpoint Validation
//! Use [`EndpointValidation`] for HTTP request validation:
//!
//! ```
//! use catga_core::{EndpointValidation, validate_required};
//!
//! let mut validation = EndpointValidation::new();
//! validation.add(validate_required(Some(""), "name"));
//! assert!(validation.into_result().is_err());
//! ```
//!
//! # Behavior Validation
//! Use [`ValidationBehavior`] for mediator pipeline validation:
//!
//! ```
//! use catga_core::validation::{ValidationBehavior, Validator};
//! ```

pub mod endpoint;
pub mod behavior;

pub use endpoint::{
    EndpointValidation, validate_required, validate_positive, validate_min_length,
    validate_max_length, validate_range, validate_not_empty, validate_min_count,
};
pub use behavior::{ValidationBehavior, Validator, validation_error};
```

- [ ] **Step 5: Update validation.rs to re-export from validation/**

Replace content of `crates/catga-core/src/validation.rs` with:
```rust
//! Validation module re-export for backward compatibility.

pub use crate::validation::{
    EndpointValidation, ValidationBehavior, Validator, validate_required,
    validate_positive, validate_min_length, validate_max_length, validate_range,
    validate_not_empty, validate_min_count, validation_error,
};
```

- [ ] **Step 6: Update behaviors/validation.rs to re-export**

Replace content of `crates/catga-core/src/behaviors/validation.rs` with:
```rust
//! Validation behavior re-export for backward compatibility.

pub use crate::validation::{ValidationBehavior, Validator, validation_error};
```

- [ ] **Step 7: Verify compilation**

Run: `cargo build -p catga-core 2>&1 | head -50`
Expected: SUCCESS

- [ ] **Step 8: Run tests**

Run: `cargo test -p catga-core --lib 2>&1 | tail -20`
Expected: All tests pass

- [ ] **Step 9: Commit**

```bash
git add crates/catga-core/src/validation/ crates/catga-core/src/validation.rs crates/catga-core/src/behaviors/validation.rs
git commit -m "refactor: reorganize validation into validation/ subdirectory"
```

---

## Task 3: Refactor lib.rs to reduce public API surface

**Files:**
- Modify: `crates/catga-core/src/lib.rs`

**Interfaces:**
- Consumes: All internal modules
- Produces: Streamlined `lib.rs` with grouped re-exports and module docs

- [ ] **Step 1: Analyze current lib.rs structure**

Run: `wc -l crates/catga-core/src/lib.rs && head -100 crates/catga-core/src/lib.rs`
Expected: ~499 lines, 360+ public items

- [ ] **Step 2: Restructure lib.rs with grouped module organization**

Organize `lib.rs` into sections:
1. Module declarations (pub mod)
2. Re-exports grouped by domain
3. Add section comments for navigation

Example structure:
```rust
//! Catga core library for CQRS/ES applications.
//!
//! ## Core Concepts
//! - [`Mediator`] for typed message dispatch
//! - [`Flow`] for saga/orchestration patterns
//! - [`EventStore`] for event sourcing

// ============================================================================
// Module declarations
// ============================================================================
pub mod auto;
pub mod behaviors;
pub mod codec;
pub mod distributed_id;
pub mod error;
pub mod flow;
pub mod lifecycle;
pub mod memory;
pub mod mediator;
pub mod reliability;
pub mod store;
pub mod telemetry;
pub mod validation;

// ============================================================================
// Core re-exports
// ============================================================================
pub use error::{CatgaError, CatgaResult, ErrorCode};
pub use mediator::{Handler, Mediator, Request};

// ============================================================================
// Flow re-exports
// ============================================================================
pub use flow::{Flow, FlowResult};

// ============================================================================
// ... continue for other domains
```

- [ ] **Step 3: Add module documentation at top**

Ensure lib.rs has comprehensive module-level documentation explaining:
- What catga-core provides
- Key abstractions (Mediator, Flow, EventStore)
- How modules relate

- [ ] **Step 4: Verify backward compatibility**

Run: `cargo build -p catga-core 2>&1 | grep -i "cannot find" | head -10`
Expected: No "cannot find" errors for existing public API

- [ ] **Step 5: Run all tests**

Run: `cargo test -p catga-core 2>&1 | tail -30`
Expected: All tests pass

- [ ] **Step 6: Run clippy**

Run: `cargo clippy -p catga-core 2>&1 | grep -E "(warning|error)" | head -10`
Expected: No new warnings

- [ ] **Step 7: Commit**

```bash
git add crates/catga-core/src/lib.rs
git commit -m "refactor: restructure lib.rs with grouped module organization"
```

---

## Task 4: Extract shared validation error formatting

**Files:**
- Create: `crates/catga-core/src/validation/shared.rs`
- Modify: `crates/catga-core/src/validation/endpoint.rs`
- Modify: `crates/catga-core/src/validation/behavior.rs`

**Interfaces:**
- Consumes: `CatgaError`, `ErrorCode`
- Produces: `format_validation_errors(errors: &[Box<str>], prefix: &str) -> CatgaError`

- [ ] **Step 1: Create shared.rs with common formatting logic**

```rust
//! Shared validation utilities.

use crate::{CatgaError, ErrorCode};

/// Formats a slice of validation errors into a single CatgaError.
///
/// # Arguments
/// * `errors` - The validation error messages
/// * `prefix` - Error message prefix (e.g., "validation failed: ")
///
/// # Example
/// ```
/// use catga_core::validation::shared::format_validation_errors;
///
/// let errors = vec!["field1 is required".into(), "field2 must be positive".into()];
/// let err = format_validation_errors(&errors, "validation failed: ");
/// ```
pub fn format_validation_errors(errors: &[Box<str>], prefix: &str) -> CatgaError {
    if errors.is_empty() {
        return CatgaError::new(ErrorCode::Validation, "validation failed");
    }

    let capacity = prefix.len()
        + errors.iter().map(|error| error.len()).sum::<usize>()
        + errors.len().saturating_sub(1) * 2;

    let mut message = String::with_capacity(capacity);
    message.push_str(prefix);
    for (index, error) in errors.iter().enumerate() {
        if index != 0 {
            message.push_str("; ");
        }
        message.push_str(error);
    }

    CatgaError::new(ErrorCode::Validation, message)
}
```

- [ ] **Step 2: Update endpoint.rs to use shared formatting**

Replace `EndpointValidation::into_result()` implementation with:
```rust
use super::shared::format_validation_errors;

pub fn into_result(self) -> CatgaResult<()> {
    if self.errors.is_empty() {
        return Ok(());
    }
    Err(format_validation_errors(&self.errors, ""))
}
```

- [ ] **Step 3: Update behavior.rs to use shared formatting**

Replace `validation_error()` function with:
```rust
use crate::validation::shared::format_validation_errors;

fn validation_error(errors: &[Box<str>]) -> CatgaError {
    format_validation_errors(errors, "validation failed: ")
}
```

- [ ] **Step 4: Update validation/mod.rs exports**

Add to `pub use` statements:
```rust
pub mod shared;
pub use shared::format_validation_errors;
```

- [ ] **Step 5: Verify compilation**

Run: `cargo build -p catga-core 2>&1 | head -20`
Expected: SUCCESS

- [ ] **Step 6: Run tests**

Run: `cargo test -p catga-core --lib 2>&1 | tail -20`
Expected: All tests pass

- [ ] **Step 7: Commit**

```bash
git add crates/catga-core/src/validation/
git commit -m "refactor: extract shared validation error formatting"
```

---

## Task 5: Add module-level documentation

**Files:**
- Modify: `crates/catga-core/src/mediator.rs` (add module docs if missing)
- Modify: `crates/catga-core/src/error.rs` (add module docs if missing)
- Modify: `crates/catga-core/src/lifecycle.rs` (add module docs if missing)

**Interfaces:**
- Consumes: Existing module implementations
- Produces: Well-documented modules with examples

- [ ] **Step 1: Check current documentation status**

Run: `head -20 crates/catga-core/src/mediator.rs`
Expected: Check if module docs exist

- [ ] **Step 2: Add module docs to mediator.rs if missing**

Add at top of file:
```rust
//! Typed mediator for request-response dispatch.
//!
//! The mediator maps incoming requests to registered handlers without coupling
//! senders to concrete implementations.
//!
//! # Example
//! ```
//! use catga_core::{Mediator, Handler, Request, Message};
//!
//! struct Query;
//! impl Message for Query {}
//! impl Request for Query { type Response = String; }
//! ```
```

- [ ] **Step 3: Verify error.rs and lifecycle.rs docs**

Check `head -20 crates/catga-core/src/error.rs` and `head -20 crates/catga-core/src/lifecycle.rs`

- [ ] **Step 4: Add docs if missing**

Add appropriate module documentation to any files lacking it.

- [ ] **Step 5: Run tests**

Run: `cargo test -p catga-core --lib 2>&1 | tail -10`
Expected: All pass

- [ ] **Step 6: Commit**

```bash
git add crates/catga-core/src/mediator.rs crates/catga-core/src/error.rs crates/catga-core/src/lifecycle.rs
git commit -m "docs: add module-level documentation to core modules"
```

---

## Task 6: DashMap usage analysis and documentation

**Files:**
- Modify: `crates/catga-core/src/memory/transport.rs` (add concurrency notes)
- Create: `docs/concurrency-design.md` (decision log)

**Interfaces:**
- Consumes: DashMap usage patterns
- Produces: Documentation on data structure choices

- [ ] **Step 1: Analyze DashMap usage in memory modules**

Run: `grep -l "DashMap" crates/catga-core/src/memory/*.rs`
Expected: List of files using DashMap

- [ ] **Step 2: Document concurrency decisions**

Create `crates/catga-core/src/memory/CONCURRENCY.md`:
```markdown
# Concurrency Design Decisions

## DashMap vs RwLock<HashMap>

### When to use DashMap
- High write contention
- Multiple independent keys
- Simple operations (get/insert)

### When to use RwLock<HashMap>
- High read, low write contention
- Complex atomic operations needed
- Memory efficiency important

## Current Usage

| File | Usage Pattern | Recommendation |
|------|---------------|----------------|
| transport.rs | Message routing | DashMap appropriate |
| event_store.rs | Stream reads | Consider RwLock |
```

- [ ] **Step 3: Verify compilation**

Run: `cargo build -p catga-core 2>&1 | head -10`
Expected: SUCCESS

- [ ] **Step 4: Commit**

```bash
git add crates/catga-core/src/memory/CONCURRENCY.md
git commit -m "docs: add concurrency design decisions documentation"
```

---

## Task 7: Security confirmation and documentation

**Files:**
- Create: `docs/security-model.md`
- Modify: `crates/catga-core/src/error.rs` (add security notes)

**Interfaces:**
- Consumes: Security audit findings
- Produces: Security boundary documentation

- [ ] **Step 1: Verify input validation completeness**

Run: `grep -n "validate" crates/catga-core/src/validation/*.rs | head -20`
Expected: All validation functions properly implemented

- [ ] **Step 2: Document security model**

Create `docs/security-model.md`:
```markdown
# Catga Security Model

## Input Validation

All user input is validated at the earliest possible boundary:

- `EndpointValidation` for HTTP handlers
- `ValidationBehavior` for mediator pipelines
- `Validator` trait for custom validation

## Error Information

- Error messages do not leak internal paths
- `MAX_ERROR_DETAILS_BYTES` limits detail size
- Internal errors wrapped in `ErrorCode::Internal`

## Authentication

- `RaftInboundPolicy` validates peer identity
- Transport adapters responsible for mTLS/SPIFFE
- Cluster communication requires authenticated peers

## Panic Safety

- Lock poisoning converted to `CatgaError`
- No unwrap/expect on user input
- Graceful degradation on resource exhaustion
```

- [ ] **Step 3: Commit**

```bash
git add docs/security-model.md
git commit -m "docs: add security model documentation"
```

---

## Verification

After all tasks complete, run final verification:

```bash
# Build all crates
cargo build --all

# Run all tests
cargo test --all

# Clippy check
cargo clippy --all -- -D warnings

# Check for TODO/FIXME
grep -rn "TODO\|FIXME" crates/ | head -10
```

Expected: All checks pass, no TODO/FIXME found.

---

## Plan Summary

| Task | Phase | Risk | Testing |
|------|-------|------|---------|
| 1 | Architecture | Low | `cargo build` |
| 2 | Architecture | Medium | `cargo test` |
| 3 | Architecture | Medium | Full test suite |
| 4 | Code Quality | Low | Unit tests |
| 5 | Documentation | Low | N/A |
| 6 | Performance | Low | N/A |
| 7 | Security | Low | N/A |

**Total estimated tasks: 7**
**Estimated time: 2-3 hours**
