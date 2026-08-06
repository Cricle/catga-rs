# catga_service Dynamic Documentation Spec

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace static macro documentation with dynamically generated doc comments and doctests for `#[catga_service]`. Users hovering over generated code should see accurate, complete type information without magic.

**Architecture:** The `impl_handlers.rs` macro already analyzes method signatures (Request/Command/Event detection). We extend this to generate proper doc comments on the `registry()` function with full handler signatures and automatic doctests.

**Tech Stack:** Rust procedural macros, `quote` crate, `syn` crate

## Global Constraints

- Generated docs must be valid Rustdoc (no syntax errors)
- Doctests must be compilable and runnable
- No runtime overhead (docs generated at compile time only)
- Must handle generics and complex types correctly

---

## Design

### Current Problem

`#[catga_service]` generates static docs like:
```rust
// Handler wrapper for `Calculator` method `double`
```

This tells users nothing about message types or handler signatures.

### Desired Behavior

Users hover over `Calculator::registry()` and see:

```rust
/// Builds a [`Registry`] containing all handlers from this service.
///
/// # Handlers
///
/// ## Requests
/// - `async fn double(&self, msg: Double) -> CatgaResult<u64>`
/// - `async fn get_order(&self, msg: GetOrder) -> CatgaResult<OrderPlaced>`
///
/// ## Commands
/// - `async fn log(&self, cmd: Log) -> CatgaResult<()>`
/// - `async fn place_order(&self, cmd: PlaceOrder) -> CatgaResult<()>`
///
/// ## Events
/// - `async fn on_order_placed(&self, event: OrderPlaced) -> CatgaResult<()>`
```

Plus automatic doctests for Request handlers.

---

## Implementation

### File: `crates/catga-core/src/macros/proc-macros/src/impl_handlers.rs`

#### Change 1: Generate `registry()` doc with handler signatures

Replace the simple `/// Builds a [`Registry`]...` doc with a comprehensive one:

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
        parts.push("/// ## Commands\n///".to_string());
        for (_, m) in &commands {
            parts.push(format!(
                "/// - `async fn {}(&self, cmd: {}) -> CatgaResult<()>`",
                m.method_name,
                quote!(#m.message_type).to_string().trim()
            ));
        }
    }

    if !events.is_empty() {
        parts.push("/// ## Events\n///".to_string());
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

#### Change 2: Generate doctests for Request handlers

Add automatic doctest generation for each Request handler:

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

    // Generate the doctest module
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

#### Change 3: Add response type to MethodAnalysis

Update `MethodAnalysis` struct to capture response type name:

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

Update `analyze_method` to extract response type:

```rust
// In analyze_method, after detecting is_request:
let response_type_name = if is_request {
    extract_response_type_name(&method.sig.output)
} else {
    None
};

// New helper function:
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

#### Change 4: Assemble final output

Replace the `registry()` function generation in `impl_handlers.rs`:

```rust
let base_output = quote! {
    #(#impl_attrs)*
    #impl_defaultness
    #impl_unsafety
    impl #impl_generics #ty {
        #(#original_method_tokens)*

        #[doc = #registry_doc]
        pub fn registry(self) -> catga_core::CatgaResult<catga_core::Registry> {
            let mut registry = catga_core::Registry::new();
            #(#registry_calls)*
            Ok(registry)
        }
    }

    #(#wrapper_structs)*
    #(#wrapper_impls)*
};
```

Note: Doctest generation should be added to the final output when `typed_mediator_name` is `None` (the standard registry path).

---

## Testing

1. Build the macro crate
2. Create a test service with multiple handler types
3. Run `cargo doc --document-private-items` on a test crate
4. Verify docs render correctly
5. Run doctests to ensure they compile and pass

---

## Files to Modify

- `crates/catga-core/src/macros/proc-macros/src/impl_handlers.rs`
