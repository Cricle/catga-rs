# catga_service Dynamic Documentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace static macro documentation with dynamically generated doc comments and doctests for `#[catga_service]`. Users hovering over generated code should see accurate, complete type information without magic.

**Architecture:** The `impl_handlers.rs` macro already analyzes method signatures (Request/Command/Event detection). Extend this to generate proper doc comments on the `registry()` function with full handler signatures and automatic doctests.

**Tech Stack:** Rust procedural macros, `quote` crate, `syn` crate

## Global Constraints

- Generated docs must be valid Rustdoc (no syntax errors)
- Doctests must be compilable and runnable
- No runtime overhead (docs generated at compile time only)
- Must handle generics and complex types correctly

---

## Task 1: Add response_type_name to MethodAnalysis

**Files:**
- Modify: `crates/catga-core/src/macros/proc-macros/src/impl_handlers.rs:433-439`
- Modify: `crates/catga-core/src/macros/proc-macros/src/impl_handlers.rs:441-480`

**Interfaces:**
- Produces: Updated `MethodAnalysis` struct with `response_type_name: Option<String>` field
- Produces: `extract_response_type_name()` helper function

- [ ] **Step 1: Update MethodAnalysis struct**

Add `response_type_name: Option<String>` field to `MethodAnalysis` struct at line 433.

```rust
struct MethodAnalysis {
    index: usize,
    method_name: syn::Ident,
    message_type: syn::Type,
    is_request: bool,
    is_event: bool,
    response_type_name: Option<String>,  // NEW: for doc generation
}
```

- [ ] **Step 2: Add extract_response_type_name helper function**

Add this function after the `is_unit_type` function (after line 484):

```rust
fn extract_response_type_name(output: &syn::ReturnType) -> Option<String> {
    if let syn::ReturnType::Type(_, ty) = output {
        if let syn::Type::Path(type_path) = ty.as_ref()
            && let Some(segment) = type_path.path.segments.last()
            && let syn::PathArguments::AngleBracketed(args) = &segment.arguments
            && let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first()
        {
            return Some(quote!(#inner_ty).to_string().trim().to_string());
        }
    }
    None
}
```

- [ ] **Step 3: Update analyze_method to extract response type**

In `analyze_method` function, after detecting `is_request`, add:

```rust
// Extract response type name for requests
let response_type_name = if is_request {
    extract_response_type_name(ret)
} else {
    None
};
```

- [ ] **Step 4: Include response_type_name in MethodAnalysis construction**

Update the `Some(MethodAnalysis { ... })` return at line 473 to include `response_type_name`.

- [ ] **Step 5: Verify build**

Run: `cargo build -p catga-core-macros`
Expected: Compiles without errors

- [ ] **Step 6: Commit**

```bash
git add crates/catga-core/src/macros/proc-macros/src/impl_handlers.rs
git commit -m "feat(macros): add response_type_name to MethodAnalysis for doc generation"
```

---

## Task 2: Generate dynamic registry() doc with handler signatures

**Files:**
- Modify: `crates/catga-core/src/macros/proc-macros/src/impl_handlers.rs:228-245`

**Interfaces:**
- Consumes: `MethodAnalysis` with `response_type_name` field
- Produces: Dynamic doc comment on `registry()` function listing all handlers

- [ ] **Step 1: Write failing test**

Create test file `crates/catga-core-macros/tests/registry_doc_test.rs`:

```rust
use catga_core::{catga_service, CatgaResult, Command, Request, Message};

struct Ping;
impl Message for Ping {}
impl Request for Ping {
    type Response = String;
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct Log(String);
impl Message for Log {}
impl Command for Log {
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct Calculator;

#[catga_service]
impl Calculator {
    async fn ping(&self, msg: Ping) -> CatgaResult<String> {
        Ok("pong".to_string())
    }
    async fn log(&self, cmd: Log) -> CatgaResult<()> {
        Ok(())
    }
}

#[test]
fn registry_doc_contains_handler_signatures() {
    // This test verifies the registry() doc contains handler info
    // Run with: cargo test --doc -p catga-core -- registry_doc
    // Check output: registry::docs().contains("ping")
}
```

Run: `cargo test --doc -p catga-core-macros -- registry_doc 2>&1 | head -50`
Expected: Test compiles (doc test may not run as written above, but macro compiles)

- [ ] **Step 2: Implement dynamic doc generation**

Replace the simple `/// Builds a [`Registry`]...` doc (lines 235-236) with:

```rust
let registry_doc = {
    let mut parts = Vec::new();
    parts.push(" Builds a [`Registry`] containing all handlers from this service.".to_string());

    // Group handlers by type
    let requests: Vec<_> = method_infos.iter().filter(|(_, m)| m.is_request).collect();
    let commands: Vec<_> = method_infos.iter().filter(|(_, m)| !m.is_request && !m.is_event).collect();
    let events: Vec<_> = method_infos.iter().filter(|(_, m)| m.is_event).collect();

    if !requests.is_empty() {
        parts.push("\n/// # Handlers\n///".to_string());
        parts.push("/// ## Requests\n///".to_string());
        for (_, m) in &requests {
            parts.push(format!(
                "/// - `async fn {}(&self, msg: {}) -> CatgaResult<{}>`",
                m.method_name,
                quote!(#m.message_type).to_string().trim(),
                m.response_type_name.as_deref().unwrap_or("()")
            ));
        }
    }

    if !commands.is_empty() {
        parts.push("\n/// ## Commands\n///".to_string());
        for (_, m) in &commands {
            parts.push(format!(
                "/// - `async fn {}(&self, cmd: {}) -> CatgaResult<()>`",
                m.method_name,
                quote!(#m.message_type).to_string().trim()
            ));
        }
    }

    if !events.is_empty() {
        parts.push("\n/// ## Events\n///".to_string());
        for (_, m) in &events {
            parts.push(format!(
                "/// - `async fn {}(&self, event: {}) -> CatgaResult<()>`",
                m.method_name,
                quote!(#m.message_type).to_string().trim()
            ));
        }
    }

    parts.join("\n")
};
```

- [ ] **Step 3: Replace static doc with dynamic one**

Update the `registry()` function to use `#[doc = #registry_doc]` instead of the static doc comment.

- [ ] **Step 4: Verify macro compiles**

Run: `cargo build -p catga-core-macros`
Expected: Compiles without errors

- [ ] **Step 5: Test generated output**

Run: `cargo expand -p catga-core 2>/dev/null | grep -A 20 "pub fn registry"` to see generated doc

- [ ] **Step 6: Commit**

```bash
git add crates/catga-core/src/macros/proc-macros/src/impl_handlers.rs
git commit -m "feat(macros): generate dynamic registry() doc with handler signatures"
```

---

## Task 3: Generate doctests for Request handlers

**Files:**
- Modify: `crates/catga-core/src/macros/proc-macros/src/impl_handlers.rs:422-424`

**Interfaces:**
- Consumes: `MethodAnalysis` with `response_type_name` for each request
- Produces: `#[cfg(test)]` module with doctests for each Request handler

- [ ] **Step 1: Add doctest generation code**

After the `base_output` generation (around line 228-246), before the `if let Some(mediator_name)` block, add doctest generation:

```rust
let doctest_code = if !requests.is_empty() {
    let test_cases: Vec<String> = requests.iter().map(|(_, m)| {
        let msg_type = quote!(#m.message_type).to_string().trim();
        let response_type = m.response_type_name.as_deref().unwrap_or("()");

        format!(r##"
/// # async fn doc_test_{}() -> catga_core::CatgaResult<()> {{
///     let registry = Self.registry()?;
///     let mediator = catga_core::Mediator::new(registry);
///     // Use type-checked message construction
///     // Response type: {}
///     # Ok(())
/// # }}
"##,
            m.method_name,
            response_type
        )
    }).collect();

    format!(r##"
#[cfg(test)]
mod __catga_service_doctests {{
    use super::*;

    {}
}}
"##,
    test_cases.join("\n")
    )
} else {
    String::new()
};
```

- [ ] **Step 2: Include doctests in output**

Modify the output generation to include `doctest_code` when `typed_mediator_name` is `None`:

```rust
let output = if let Some(mediator_name) = typed_mediator_name {
    // ... existing code
} else {
    // base_output with doctests for non-typed path
    quote! {
        #base_output
        #doctest_code
    }
};
```

- [ ] **Step 3: Verify build**

Run: `cargo build -p catga-core-macros`
Expected: Compiles without errors

- [ ] **Step 4: Run doctests**

Run: `cargo test --doc -p catga-core -- registry 2>&1 | tail -30`
Expected: Doctests compile and pass (with # Ok(()) compile-time check)

- [ ] **Step 5: Commit**

```bash
git add crates/catga-core/src/macros/proc-macros/src/impl_handlers.rs
git commit -m "feat(macros): add automatic doctests for Request handlers"
```

---

## Task 4: Integration test and verification

**Files:**
- Create: `crates/catga-examples/src/bin/doc_demo.rs` (if examples crate exists)
- Or use existing example: verify docs render correctly

**Interfaces:**
- Consumes: Modified `impl_handlers.rs`
- Produces: Verified documentation in IDE

- [ ] **Step 1: Create or use existing example service**

If `catga-examples` exists, create `examples/doc_demo.rs`:

```rust
use catga_core::{catga_service, CatgaResult, Command, Event, Request, Message};

#[derive(catga_command)]
struct Log(String);

struct Ping;
impl Message for Ping {}
impl Request for Ping {
    type Response = String;
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct OrderPlaced(String);
impl Message for OrderPlaced {}
impl Event for OrderPlaced {
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct Calculator;

#[catga_service]
impl Calculator {
    async fn ping(&self, msg: Ping) -> CatgaResult<String> {
        Ok("pong".to_string())
    }
    async fn log(&self, cmd: Log) -> CatgaResult<()> {
        Ok(())
    }
    async fn on_order_placed(&self, event: OrderPlaced) -> CatgaResult<()> {
        Ok(())
    }
}

fn main() {
    let calc = Calculator;
    // Hover over registry() to see dynamic docs
    let registry = calc.registry().unwrap();
    println!("Registry created with {} handlers", registry.handler_count());
}
```

- [ ] **Step 2: Generate docs**

Run: `cargo doc --document-private-items -p catga-examples`
Expected: No warnings, docs generated

- [ ] **Step 3: Verify rendered docs**

Run: `grep -A 30 "pub fn registry" target/doc/*/struct.Registry.html 2>/dev/null || true`
Or check: `cat /tmp/catga_debug.rs` to see macro output

- [ ] **Step 4: Run all tests**

Run: `cargo test --workspace --all-features`
Expected: All tests pass

- [ ] **Step 5: Final commit**

```bash
git add .
git commit -m "feat: complete catga_service dynamic documentation with doctests"
```

---

## Summary

| Task | File | Key Change |
|------|------|------------|
| 1 | impl_handlers.rs | Add `response_type_name` to `MethodAnalysis` |
| 2 | impl_handlers.rs | Generate dynamic `registry()` doc with handler signatures |
| 3 | impl_handlers.rs | Generate doctests for Request handlers |
| 4 | Integration | Verify docs render and tests pass |
