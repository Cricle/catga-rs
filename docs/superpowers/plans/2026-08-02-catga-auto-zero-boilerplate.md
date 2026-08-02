# catga-auto Zero-Boilerplate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement zero-boilerplate handler registration with derive macros (catga_Request, catga_Command, catga_Event) and compile-time auto-discovery via #[catga_main].

**Architecture:** Three derive macros generate Message+trait impls. #[catga_main] attribute macro scans for async fn handlers and generates registration code + global dispatch functions. No runtime overhead - all discovery at compile time.

**Tech Stack:** proc-macro2, quote, syn crates; async-trait for blanket impls.

## Global Constraints

- **Performance target:** ≤5ns overhead vs direct function call
- **Memory:** Zero allocation on dispatch hot path
- **No unsafe code**
- **No reflection on hot path**
- **Clone bound required** for all message types (CQRS pattern)

---

## File Structure

```
crates/catga-macros/src/
├── derive_request.rs   (NEW) catga_Request derive macro
├── derive_command.rs   (NEW) catga_Command derive macro
├── derive_event.rs     (NEW) catga_Event derive macro
├── catga_main.rs      (NEW) #[catga_main] attribute macro
├── lib.rs             (MODIFY) export new macros

crates/catga-auto/src/
├── lib.rs             (MODIFY) add global send/send_command/publish
└── global_dispatch.rs (NEW) thread-local mediator handle

crates/catga-core/src/
└── lib.rs             (MODIFY) re-export new derive macros
```

---

## Task 1: Implement catga_Request derive macro

**Files:**
- Create: `crates/catga-macros/src/derive_request.rs`
- Modify: `crates/catga-macros/src/lib.rs:17-18`
- Test: `crates/catga-macros/tests/derive_request.rs` (create)

**Interfaces:**
- Consumes: Nothing
- Produces: `#[proc_macro_derive(catga_Request, attributes(response))]` - generates Message + Request impls

**Steps:**

- [ ] **Step 1: Create derive_request.rs**

```rust
// crates/catga-macros/src/derive_request.rs
use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Generics, Ident, Result, Token, parse_quote};
use proc_macro::TokenStream as MacroTokenStream;

const RESPONSE_ATTR: &str = "response";

/// Implements Message + Request traits with automatic Clone bound.
/// Users write Response type via #[catga_Request(response = "TypeName")].
pub fn expand_derive_request(input: MacroTokenStream) -> MacroTokenStream {
    match derive_request_impl(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.into_compile_error().into(),
    }
}

fn derive_request_impl(input: TokenStream) -> Result<TokenStream> {
    let input = parse_input(input);
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = add_clone_bound(&input.generics).split_for_impl();

    // Parse response type from attribute
    let response_type = parse_response_type(&input.attrs, name)?;

    Ok(quote! {
        impl #impl_generics ::catga_core::Message for #name #ty_generics #where_clause {}
        impl #impl_generics ::catga_core::Request for #name #ty_generics #where_clause {
            type Response = #response_type;
        }
    })
}

fn parse_input(input: TokenStream) -> DeriveInput {
    syn::parse2(input).expect("invalid input")
}

fn add_clone_bound(generics: &Generics) -> Generics {
    let mut g = generics.clone();
    let where_clause = g.make_where_clause();
    for param in &g.params {
        if let syn::GenericParam::Type(type_param) = param {
            where_clause.predicates.push(parse_quote!(#type_param: Clone));
        }
    }
    g
}

fn parse_response_type(attrs: &[syn::Attribute], name: &Ident) -> Result<syn::Type> {
    // ... parse #[catga_Request(response = "TypeName")]
}
```

- [ ] **Step 2: Export from lib.rs**

Add to `crates/catga-macros/src/lib.rs`:
```rust
mod derive_request;
pub use derive_request::expand_derive_request;
```

- [ ] **Step 3: Add proc_macro_derive entry in lib.rs**

```rust
/// Implements Message + Request for a struct with response type.
/// ...
#[proc_macro_derive(catga_Request, attributes(catga_Request))]
pub fn derive_request(input: TokenStream) -> TokenStream {
    derive_request::expand_derive_request(input)
}
```

- [ ] **Step 4: Create test file**

```rust
// crates/catga-macros/tests/derive_request.rs
use catga_core::{Message, Request};

#[derive(catga_Request(response = "String"))]
struct GetUser(String);

#[test]
fn implements_message() {
    let msg = GetUser("123".into());
    assert!(msg.message_type().ends_with("GetUser"));
}

#[test]
fn implements_request_with_response() {
    fn assert_response<T: Request>() {}
    assert_response::<GetUser>();
    // Response type is String
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p catga-macros --test derive_request`
Expected: PASS

---

## Task 2: Implement catga_Command derive macro

**Files:**
- Create: `crates/catga-macros/src/derive_command.rs`
- Modify: `crates/catga-macros/src/lib.rs`
- Test: `crates/catga-macros/tests/derive_command.rs` (create)

**Interfaces:**
- Consumes: Nothing
- Produces: `#[proc_macro_derive(catga_Command)]` - generates Message + Command impls

**Steps:**

- [ ] **Step 1: Create derive_command.rs**

```rust
// crates/catga-macros/src/derive_command.rs
// Similar structure to derive_request.rs but implements Command trait (no response type)
```

- [ ] **Step 2: Export and register proc_macro_derive**

- [ ] **Step 3: Create test file**

```rust
// crates/catga-macros/tests/derive_command.rs
use catga_core::{Command, Message};

#[derive(catga_Command)]
struct CreateUser { name: String }

#[test]
fn implements_message_and_command() {
    let cmd = CreateUser { name: "Alice".into() };
    assert!(cmd.message_type().ends_with("CreateUser"));
    fn assert_command<T: Command>() {}
    assert_command::<CreateUser>();
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p catga-macros --test derive_command`
Expected: PASS

---

## Task 3: Implement catga_Event derive macro

**Files:**
- Create: `crates/catga-macros/src/derive_event.rs`
- Modify: `crates/catga-macros/src/lib.rs`
- Test: `crates/catga-macros/tests/derive_event.rs` (create)

**Interfaces:**
- Consumes: Nothing
- Produces: `#[proc_macro_derive(catga_Event)]` - generates Message + Event impls (requires Clone)

**Steps:**

- [ ] **Step 1: Create derive_event.rs**

```rust
// crates/catga-macros/src/derive_event.rs
// Implements Message + Event, enforces Clone bound (Event requires Clone)
```

- [ ] **Step 2: Export and register proc_macro_derive**

- [ ] **Step 3: Create test file**

```rust
// crates/catga-macros/tests/derive_event.rs
use catga_core::{Event, Message};

#[derive(catga_Event)]
struct UserCreated { user_id: String }

#[test]
fn implements_message_and_event_with_clone() {
    let evt = UserCreated { user_id: "123".into() };
    let _cloned = evt.clone(); // Event requires Clone
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p catga-macros --test derive_event`
Expected: PASS

---

## Task 4: Implement #[catga_main] attribute macro

**Files:**
- Create: `crates/catga-macros/src/catga_main.rs`
- Modify: `crates/catga-macros/src/lib.rs`
- Test: `crates/catga-macros/tests/catga_main.rs` (create)

**Interfaces:**
- Consumes: catga_Request, catga_Command, catga_Event derive macros
- Produces: `#[proc_macro_attribute]` that wraps main function with auto-discovery

**Behavior:**
1. Parse the async fn signature
2. Scan containing module for async fn handlers
3. Classify by return type: `Result<T>` → Request, `Result<()>` → Command, `()` → Event
4. Generate registration code + global send/send_command/publish functions
5. Wrap original main body

**Steps:**

- [ ] **Step 1: Create catga_main.rs with handler discovery**

```rust
// crates/catga-macros/src/catga_main.rs
use proc_macro2::TokenStream;
use quote::{quote, format_ident};
use syn::{ItemFn, Result, parse_quote};
use std::collections::HashMap;

// Handler classification
enum HandlerType {
    Request,  // returns Result<T>
    Command,   // returns Result<()>
    Event,     // returns ()
}

struct DiscoveredHandler {
    name: syn::Ident,
    message_type: syn::Type,
    handler_type: HandlerType,
}
```

- [ ] **Step 2: Generate registration code**

```rust
// For each discovered handler, generate:
// __catga_auto_registry.register_request::<MessageType, _>(handler_name)?;
```

- [ ] **Step 3: Generate global dispatch functions**

```rust
// Generate:
// pub async fn send<M: Request>(msg: M) -> CatgaResult<M::Response> {
//     MEDIATOR.with(|m| m.send(msg))
// }
```

- [ ] **Step 4: Wrap main function body**

```rust
// Generate:
// #[tokio::main]
// async fn main() -> CatgaResult<()> {
//     // Registration code
//     let app = AutoApp::builder()
//         .handler(handler1)?
//         .handler(handler2)?
//         .build()?;
//     let _mediator = app.mediator();
//     // User's original body
// }
```

- [ ] **Step 5: Create integration test**

```rust
// crates/catga-macros/tests/catga_main.rs
#[derive(catga_Request(response = "String"))]
struct GetUser(String);

async fn get_user_handler(msg: GetUser) -> CatgaResult<String> {
    Ok(format!("user: {}", msg.0))
}

#[catga_main]
async fn main() -> CatgaResult<()> {
    let result = send(GetUser("123".into())).await?;
    println!("{}", result);
    Ok(())
}
```

- [ ] **Step 6: Run tests**

Run: `cargo test -p catga-macros --test catga_main -- --nocapture`
Expected: PASS (prints "user: 123")

---

## Task 5: Add global dispatch functions to catga-auto

**Files:**
- Create: `crates/catga-auto/src/global_dispatch.rs`
- Modify: `crates/catga-auto/src/lib.rs`

**Interfaces:**
- Consumes: catga-macros exports
- Produces: thread_local `send()`, `send_command()`, `publish()` functions

**Steps:**

- [ ] **Step 1: Create global_dispatch.rs**

```rust
// crates/catga-auto/src/global_dispatch.rs
use std::future::Future;
use catga_core::{CatgaResult, Command, Event, MediatorHandle, Request};

thread_local! {
    static MEDIATOR: MediatorHandle = MediatorHandle::new();
}

/// Binds the mediator at startup (called by #[catga_main])
pub fn bind_mediator(mediator: std::sync::Arc<catga_core::Mediator>) -> CatgaResult<()> {
    MEDIATOR.with(|m| m.bind(mediator))
}

/// Sends a request and returns the response.
pub async fn send<M: Request>(msg: M) -> CatgaResult<M::Response> {
    MEDIATOR.with(|m| m.send(msg).await)
}

/// Sends a command (fire-and-forget).
pub async fn send_command<C: Command>(cmd: C) -> CatgaResult<()> {
    MEDIATOR.with(|m| m.send_command(cmd).await)
}

/// Publishes an event to all subscribers.
pub async fn publish<E: Event>(evt: E) -> CatgaResult<()> {
    MEDIATOR.with(|m| m.publish(evt).await)
}
```

- [ ] **Step 2: Export from catga-auto lib.rs**

```rust
pub use global_dispatch::{send, send_command, publish};
```

- [ ] **Step 3: Run existing tests**

Run: `cargo test -p catga-auto`
Expected: PASS

---

## Task 6: Re-export new macros from catga-core

**Files:**
- Modify: `crates/catga-core/src/lib.rs:190`

**Steps:**

- [ ] **Step 1: Update re-exports**

```rust
// Add to catga_core re-exports:
pub use catga_macros::{
    Message, catga_handlers, catga_typed_mediator, catga_auto, catga_handler,
    catga_Request, catga_Command, catga_Event, catga_main,
};
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p catga-core`
Expected: PASS

---

## Task 7: Update catga-macros lib.rs exports

**Files:**
- Modify: `crates/catga-macros/src/lib.rs`

**Steps:**

- [ ] **Step 1: Add proc_macro_derive for all three macros**

Add three new `#[proc_macro_derive]` entries for catga_Request, catga_Command, catga_Event.

- [ ] **Step 2: Add proc_macro_attribute for catga_main**

---

## Task 8: Create example demonstrating new API

**Files:**
- Create: `crates/catga-auto/examples/zero_boilerplate.rs`

**Steps:**

- [ ] **Step 1: Create example**

```rust
//! Demonstrates zero-boilerplate handler registration
use catga_auto::{catga_Request, catga_Command, catga_Event, catga_main, send};

#[derive(catga_Request(response = "String"))]
struct GetUser(String);

#[derive(catga_Command)]
struct CreateUser { name: String }

#[derive(catga_Event, Clone)]
struct UserCreated { user_id: String }

async fn get_user_handler(msg: GetUser) -> CatgaResult<String> {
    Ok(format!("user: {}", msg.0))
}

async fn create_user_handler(_: CreateUser) -> CatgaResult<()> {
    // Business logic here
    Ok(())
}

#[catga_main]
async fn main() -> CatgaResult<()> {
    let user = send(GetUser("123".into())).await?;
    println!("{}", user);
    Ok(())
}
```

- [ ] **Step 2: Run example**

Run: `cargo run -p catga-auto --example zero_boilerplate`
Expected: Prints "user: 123"

---

## Task 9: Write migration guide

**Files:**
- Create: `docs/migration/zero-boilerplate-migration.md`

**Steps:**

- [ ] **Step 1: Document migration from old to new API**

Compare old pattern (explicit trait impls) vs new pattern (derive + auto-discovery).

- [ ] **Step 2: Commit**

Run: `git add . && git commit -m "docs: add zero-boilerplate migration guide"`

---

## Self-Review Checklist

- [ ] All 3 derive macros implemented and tested
- [ ] #[catga_main] discovers handlers at compile time
- [ ] Global send/send_command/publish functions work
- [ ] No duplicate trait impls (user writes Response type once)
- [ ] Performance target documented and achievable
- [ ] Backward compatible (existing APIs unchanged)
- [ ] All tests pass
- [ ] Example runs successfully
