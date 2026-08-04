# Catga Security Model

## Overview

Catga provides a security model for request authorization with:
- Task-scoped identity propagation
- Role-based access control (RBAC)
- Policy-based authorization
- Application claims with bounded storage

## Input Validation Boundaries

### Endpoint Validation

`EndpointValidation` provides framework-agnostic HTTP input validation:

```rust
use catga_core::{EndpointValidation, validate_required, validate_max_length};

let mut validation = EndpointValidation::new();
validation.add(validate_required(Some("user@example.com"), "email"));
validation.add(validate_max_length(Some("John Doe"), 100, "name"));
validation.into_result()?;
```

### Behavior Validation

`ValidationBehavior` validates requests before handler execution:

```rust
use catga_core::validation::{ValidationBehavior, Validator};
use async_trait::async_trait;

struct RequestValidator;
#[async_trait]
impl Validator<MyRequest> for RequestValidator {
    async fn validate(&self, request: &MyRequest, errors: &mut Vec<Box<str>>) -> CatgaResult<()> {
        if request.user_id.is_empty() {
            errors.push("user_id is required".into());
        }
        Ok(())
    }
}
```

## Security Identity

### Task-Scoped Identity

Security identity propagates through async task chains:

```rust
use catga_core::{SecurityIdentity, scope_security_identity, current_security_identity};

let identity = SecurityIdentity::new("user-42", ["admin", "editor"]);

let result = scope_security_identity(identity, async {
    // Security identity available here
    let current = current_security_identity().unwrap();
    process_request(current)
}).await;
```

### Role-Based Access Control

Roles are matched case-insensitively:

```rust
let identity = SecurityIdentity::new("user-42", ["admin", "editor"]);
// Both of these return true:
identity.has_role("Admin");   // ASCII case-insensitive
identity.has_role("EDITOR");  // ASCII case-insensitive
```

### Application Claims

Claims are bounded to prevent memory exhaustion:

- Maximum 32 claims per identity (`MAX_SECURITY_CLAIMS`)
- Claim keys: max 64 bytes, ASCII alphanumeric with `.`, `_`, `-`
- Claim values: max 1024 bytes
- Duplicate keys are rejected

```rust
let identity = SecurityIdentity::try_with_claims(
    "user-42",
    ["admin"],
    [("tenant", "acme-corp"), ("department", "engineering")]
)?;
```

## Authorization Requirements

Requests declare their authorization requirements:

```rust
use catga_core::AuthorizationRequirements;

struct AdminRequest;
impl AuthorizedRequest for AdminRequest {
    fn authorization() -> AuthorizationRequirements {
        AuthorizationRequirements::with_roles(&["admin"])
    }
}
```

The `AuthorizationBehavior` enforces these requirements at the pipeline level.

## Security Boundaries

### What Catga Provides

- **Identity propagation** via task-local storage
- **Role matching** with case-insensitive comparison
- **Claim validation** with bounded storage
- **Authorization enforcement** via behavior pipeline

### What Callers Must Implement

- **Identity creation** — Catga doesn't authenticate users; callers create identities from their auth system
- **Policy evaluation** — `AuthorizationPolicy` is a trait; callers implement their policy logic
- **Transport security** — TLS, mTLS, etc. are outside Catga's scope
- **Injection prevention** — Input validation helpers exist, but callers must use them

## Error Handling

Security failures return specific error codes:

- `ErrorCode::Unauthorized` — No identity present when one is required
- `ErrorCode::Forbidden` — Identity lacks required roles/policy
- `ErrorCode::Validation` — Input validation failed

## Memory Bounds

All security-related data structures have explicit limits:

| Constant | Value | Purpose |
|----------|-------|---------|
| `MAX_SECURITY_CLAIMS` | 32 | Prevent unbounded claim storage |
| `MAX_SECURITY_CLAIM_KEY_BYTES` | 64 | Bounded ASCII identifier |
| `MAX_SECURITY_CLAIM_VALUE_BYTES` | 1024 | Bounded claim values |
