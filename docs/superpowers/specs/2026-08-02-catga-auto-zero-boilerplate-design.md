# catga-auto Zero-Boilerplate Design

## Goal

Reduce user code to pure business logic. Users define messages and handlers without framework ceremony; the framework discovers and registers them automatically.

## User Experience

### Before (Current)

```rust
use catga_core::{CatgaResult, Message, Request};
use catga_auto::AutoApp;

#[derive(Clone)]
struct GetUser(String);
impl Message for GetUser {}
impl Request for GetUser { type Response = String; }

async fn get_user_handler(msg: GetUser) -> CatgaResult<String> {
    Ok(format!("user: {}", msg.0))
}

#[tokio::main]
async fn main() -> CatgaResult<()> {
    let app = AutoApp::builder()
        .handler(get_user_handler)?
        .build()?;
    let result = app.mediator().send(GetUser("123".into())).await?;
    println!("{}", result);
    Ok(())
}
```

### After (Target)

```rust
use catga_auto::{catga_Request, catga_main, send};

// 1. Define message with single derive
#[derive(catga_Request)]
struct GetUser(String);
impl catga_Request for GetUser { type Response = String; }

// 2. Write handler (pure business logic)
async fn get_user_handler(msg: GetUser) -> String {
    format!("user: {}", msg.0)
}

// 3. Launch (auto-discovery + build)
#[catga_main]
async fn main() -> CatgaResult<()> {
    let result = send(GetUser("123".into())).await?;
    println!("{}", result);
    Ok(())
}
```

## Design

### 1. Message Derive Macros

Three derive macros replace `#[derive(Message)]` + trait impls:

| Derive | Trait | Notes |
|--------|-------|-------|
| `#[derive(catga_Request)]` | `Request` | Response type from handler return |
| `#[derive(catga_Command)]` | `Command` | No response |
| `#[derive(catga_Event)]` | `Event` | Multi-handler pattern |

Each macro:
- Implements the corresponding trait (`Message` + `Request/Command/Event`)
- Adds `Clone` bound automatically (CQRS requires message cloneability)
- Adds no hidden state or runtime overhead

### 2. Handler Auto-Discovery

`#[catga_main]` attribute:

```rust
#[catga_main]
async fn main() -> CatgaResult<()> {
    // ...
}
```

Behavior:
1. Scan the module containing `main` for async functions
2. For each async fn, infer message type from first parameter
3. Classify by return type:
   - `-> Result<T>` → Request, Response = T
   - `-> Result<()>` → Command
   - `-> ()` → Event
4. Register all discovered handlers
5. Build `AutoApp` and bind mediator
6. Provide global `send()`, `send_command()`, `publish()` functions

### 3. Global Dispatch Functions

After `#[catga_main]`:

```rust
// For Request (returns response)
let result = send(MyRequest("data".into())).await?;

// For Command (fire-and-forget)
send_command(MyCommand).await?;

// For Event (publish, no wait)
publish(MyEvent).await?;
```

These use a thread-local or static `MediatorHandle` bound at startup.

### 4. Migration Path

| Feature | Status | Migration |
|---------|--------|-----------|
| `#[derive(Message)]` | Deprecated | Use `#[derive(catga_Request/Command/Event)]` |
| `AutoApp::builder()` | Keep | Still available for explicit control |
| `#[catga_auto]` | Keep | For library/module auto-discovery |
| `Handler` trait | Keep | For complex handlers needing state |

Users migrate incrementally: start with new messages using derive macros, existing code continues working.

## Components

### New Crate: `catga-derive` (or extend `catga-macros`)

```rust
// catga_Request derive
#[proc_macro_derive(catga_Request, attributes(response))]
pub fn derive_request(input: TokenStream) -> TokenStream {
    // 1. Parse struct name and fields
    // 2. Implement Message trait
    // 3. Implement Request trait with placeholder Response
    // 4. Emit struct with Clone bound
}

// catga_Command derive
#[proc_macro_derive(catga_Command)]
pub fn derive_command(input: TokenStream) -> TokenStream {
    // 1. Implement Message trait
    // 2. Implement Command trait
    // 3. Emit struct with Clone bound
}

// catga_Event derive
#[proc_macro_derive(catga_Event)]
pub fn derive_event(input: TokenStream) -> TokenStream {
    // 1. Implement Message trait
    // 2. Implement Event trait
    // 3. Emit struct with Clone bound
}
```

### New Attribute: `#[catga_main]`

```rust
#[proc_macro_attribute]
pub fn catga_main(attr: TokenStream, item: TokenStream) -> TokenStream {
    // 1. Parse async fn
    // 2. Extract module items
    // 3. Generate handler registration code
    // 4. Wrap body with AutoApp setup
    // 5. Inject global send/send_command/publish
}
```

### Global Handle

```rust
// Thread-local or Arc<MediatorHandle>
thread_local! {
    static MEDIATOR: MediatorHandle = todo!();
}

pub fn send<M: Request>(msg: M) -> impl Future<Output = CatgaResult<M::Response>> {
    MEDIATOR.with(|m| m.send(msg))
}
```

## Error Handling

| Error | Behavior |
|-------|----------|
| Duplicate handler | Compile error: "Handler for X already registered" |
| Missing handler | Compile error: "No handler found for Y" |
| Invalid signature | Compile error with suggestion |
| Build failure | Propagate as `CatgaResult` |

## Backward Compatibility

- Existing `catga-core`, `catga-auto` APIs unchanged
- `#[catga_auto]` module attribute still works
- Users can mix old and new styles
- No runtime performance penalty for auto-discovery

## Files to Create/Modify

### Create
- `crates/catga-macros/src/derive_request.rs`
- `crates/catga-macros/src/derive_command.rs`
- `crates/catga-macros/src/derive_event.rs`
- `crates/catga-macros/src/catga_main.rs`

### Modify
- `crates/catga-macros/src/lib.rs` - export new macros
- `crates/catga-auto/src/lib.rs` - add global send functions
- `crates/catga-core/src/lib.rs` - re-export derive macros

## Implementation Order

1. Implement derive macros (`catga_Request`, `catga_Command`, `catga_Event`)
2. Add unit tests for each derive macro
3. Implement `#[catga_main]` with basic discovery
4. Add global `send()` / `send_command()` / `publish()` functions
5. Update examples to demonstrate new API
6. Write migration guide
