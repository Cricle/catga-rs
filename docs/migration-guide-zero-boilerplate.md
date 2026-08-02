# Migration Guide: Zero-Boilerplate Handler Registration

This guide helps existing catga users migrate to the new zero-boilerplate handler registration API.

## Overview

The new API reduces boilerplate by:
1. Derive macros for message types (`#[catga_request]`, `#[catga_command]`, `#[catga_event]`)
2. Fn-blanket implementations allowing plain async handlers
3. Global dispatch functions for sending messages without passing mediator handles

## Before vs After

### Request Handler

**Before:**
```rust
use async_trait::async_trait;
use catga_core::{CatgaResult, Handler, Message, Request};

#[derive(Clone)]
struct GetUser(String);
impl Message for GetUser {}
impl Request for GetUser { type Response = String; }

struct GetUserHandler;
#[async_trait]
impl Handler<GetUser> for GetUserHandler {
    async fn handle(&self, msg: GetUser) -> CatgaResult<String> {
        Ok(format!("User {} found", msg.0))
    }
}
```

**After:**
```rust
use catga_core::{catga_request, CatgaResult};

#[catga_request(response = String)]
struct GetUser(String);

async fn get_user_handler(msg: GetUser) -> CatgaResult<String> {
    Ok(format!("User {} found", msg.0))
}
```

### Command Handler

**Before:**
```rust
use async_trait::async_trait;
use catga_core::{Command, CommandHandler, Message};

struct CreateUser(String);
impl Message for CreateUser {}
impl Command for CreateUser {}

struct CreateUserHandler;
#[async_trait]
impl CommandHandler<CreateUser> for CreateUserHandler {
    async fn handle(&self, msg: CreateUser) -> CatgaResult<()> {
        // ...
        Ok(())
    }
}
```

**After:**
```rust
use catga_core::{catga_command, CatgaResult};

#[catga_command]
struct CreateUser(String);

async fn create_user_handler(msg: CreateUser) -> CatgaResult<()> {
    // ...
    Ok(())
}
```

### Event Handler

**Before:**
```rust
use async_trait::async_trait;
use catga_core::{Event, EventHandler, Message};

#[derive(Clone)]
struct UserCreated(String);
impl Message for UserCreated {}
impl Event for UserCreated {}

struct UserCreatedProjection;
#[async_trait]
impl EventHandler<UserCreated> for UserCreatedProjection {
    async fn handle(&self, evt: UserCreated) -> CatgaResult<()> {
        // ...
        Ok(())
    }
}
```

**After:**
```rust
use catga_core::{catga_event, CatgaResult};

#[catga_event]
struct UserCreated(String);

async fn user_created_handler(evt: UserCreated) -> CatgaResult<()> {
    // ...
    Ok(())
}
```

## Application Setup

### Before
```rust
use catga_auto::AutoApp;

let app = AutoApp::builder()
    .request::<GetUser, _>(GetUserHandler)?
    .build()?;
```

### After
```rust
use catga_auto::AutoApp;

let app = AutoApp::builder()
    .handler(get_user_handler)?  // Type inferred from handler signature
    .build()?;
```

## Global Dispatch

### Before
```rust
use catga_auto::AutoApp;

let app = AutoApp::builder().build()?;
let handle = app.handle();

// Call handler via handle
let result = handle.send(GetUser("123".into())).await?;
```

### After
```rust
use catga_auto::{AutoApp, send};

let app = AutoApp::builder()
    .handler(get_user_handler)?
    .build()?;

// Call handler from anywhere - no handle needed!
let result = send(GetUser("123".into())).await?;
```

## Key Differences

| Feature | Before | After |
|---------|--------|-------|
| Message impl | Manual trait impls | `#[catga_request]` attribute |
| Handler impl | `#[async_trait]` + struct | Plain async fn |
| Response type | In impl block | `#[catga_request(response = "Type")]` |
| Registration | `.request::<M, _>(Handler)` | `.handler(handler_fn)` |
| Dispatch | Via mediator handle | `send()`, `send_command()`, `publish()` |

## Dependencies

The new API still requires:
- `async-trait` for complex handlers requiring `self` state (optional)
- `catga_core` for traits and types
- `catga_auto` for the application builder and global dispatch

No new runtime dependencies are added. The compile-time macro expansion produces identical code to the manual implementation.

## Feature Flags

The zero-boilerplate API is available in:
- `catga-core` (derive macros re-exported)
- `catga-auto` (global dispatch, AutoApp)

All existing features remain unchanged.
