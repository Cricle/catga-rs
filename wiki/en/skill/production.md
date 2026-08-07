# Error Handling, Idempotency, and Production Checklist

## CatgaResult and CatgaError

Every fallible API returns `CatgaResult<T>` (= `Result<T, CatgaError>`). Use `?` for propagation; decide by stable category and retry hint at **application boundaries**, not by matching error text:

```rust,ignore
use catga_core::{CatgaError, CatgaResult, ErrorCode};

// Construct error: CatgaError::new(code, message), optional .with_details(..) (≤ MAX_ERROR_DETAILS_BYTES = 1024)
return Err(CatgaError::new(ErrorCode::Validation, "an order must contain at least one item"));

// Boundary handling
match result {
    Ok(value) => Ok(value),
    Err(error) if error.is_retryable() => {
        eprintln!("retry {}: {}", error.code().as_stable_str(), error.message());
        Err(error)
    }
    Err(error) => Err(error),
}
```

`CatgaError` accessors: `code()` → `ErrorCode`, `message()`, `details()`, `is_retryable()`.

## ErrorCode Classification

| Category | Meaning | Retriable |
| --- | --- | --- |
| `Validation` | Input does not satisfy validation rules | No |
| `HandlerFailed` / `PipelineFailed` | Classified failure reported by handler/pipeline | No |
| `HandlerNotFound` | Message type has no registered handler | No |
| `PersistenceFailed` / `LockFailed` | Persistence/lock failure (caller cannot infer idempotency or ownership, **intentionally non-retriable**) | No |
| `TransportFailed` | Transport communication failure, usually safe to retry | **Yes** |
| `SerializationFailed` | Serialization/deserialization failure | No |
| `NotFound` / `Conflict` | Resource does not exist / conflicts with persisted state (e.g., duplicate registration, flow identity already exists) | No |
| `Unauthorized` / `Forbidden` | Not authenticated / authenticated but unauthorized | No |
| `Cancelled` | Work was cancelled before completion | No |
| `Timeout` / `FlowTimeout` | Configured deadline exceeded | **Yes** |
| `FlowFailed` / `FlowCompensating` / `FlowCancelled` | Durable flow business failure / compensating / cancelled (terminal states) | No |
| `Transient` | Contractually may succeed on retry | **Yes** |
| `Unavailable` | Component temporarily not accepting/cannot serve requests | **Yes** |
| `Unsupported` | No configured component supports this operation (e.g., backend does not support nack) | No |
| `Internal` | Framework unexpected failure | No |

- `code.as_stable_str()` → Stable wire name (`"validation"`, `"conflict"`, etc.); `ErrorCode::from_stable_str(..)` parses it.
- `code.http_status_u16()` → Conventional HTTP status codes (framework-agnostic, HTTP adapter maps based on this).

## Retry and Idempotency Guidelines

1. **Catga does not automatically make retries safe**. Before retrying side effects: select an idempotency key + who deduplicates (`IdempotencyStore` / `InboxStore` / durable flow step key).
2. Only retry categories where `is_retryable()` is true; `RetryBehavior` has this check built in.
3. Durable flow steps, transport redelivery, and timeout recovery are all **at-least-once**: consumers must tolerate duplicates.
4. Jitter strategy: use `RetryJitter::production_default()` (full jitter) in production; `RetryJitter::fixed(duration)` for deterministic testing.

## Production Checklist

1. **Keep external side effects idempotent**: Flow retries, transport redelivery, and timeout recovery are all intentional at-least-once boundaries.
2. **Minimal feature set**: Enable Cargo features only for services actually deployed; don't enable all adapters by default.
3. **Migrations first**: Run store `migrate()` during controlled startup phase, then run scheduler and receive loops in application-owned supervision tasks.
4. **Bounded timeouts and batch sizes**: Redis command adapter has bounded response timeout by default; long polls are isolated from each other.
5. **Raft HTTP endpoint authentication**: Place mTLS or signed frame authentication before `raft_message_route`, attach verified `RaftPeerIdentity`, and configure `StaticRaftInboundPolicy` with this node and trusted peers.

Availability, credentials, retry budgets, and graceful shutdown are all owned by the **caller** — this is by design.

## Lifecycle and Shutdown (`catga-core`)

- `TransportLifecycle`: Explicit mode for individual transport — `initialize` → stop receiving → bounded drain (`TransportLifecycleOptions`).
- `ShutdownCoordinator` / `OperationTracker` / `AcceptanceGate`: Coordinate graceful shutdown.
- `RecoveryManager` / `RecoverableComponent` (`AutoRecoveryOptions`): Component recovery.
- `HealthCheckable`: Health check contract.
- Cooperative cancellation: `scope_cancellation` / `current_cancellation` (task-local `CancellationToken`).

## Observability

- OpenTelemetry-compatible tracing/metrics come from public crate APIs; `TRACING_TARGET` is the structured event target.
- Trace tags require explicit opt-in on messages (`#[catga(trace_tag)]`), no application data exported by default (privacy-safe).
- Correlation: `CorrelationBehavior` / `scope_correlation_id` / `current_correlation_id`; `TraceContext` (W3C `traceparent`/`tracestate`).
- Use tracing/metrics for observability; the crate deliberately does not provide built-in HTTP health endpoints (health status is exposed via the `HealthCheckable` contract).

## Testing Tools (`catga-testing`)

In-process typed testing utilities (new instance per test case, not shared across concurrent tests):

- `CatgaTestHarness` / `RunningCatgaTestHarness` — Register handlers before startup, get mediator after startup.
- `HandlerSpy::new(handler)` / `with_action(..)` / `without_handler()` — Records requests for assertions (`calls()` / `call_count()` / `last_call()`); `EventHandlerSpy` works similarly (`new()` record-only / `with_handler(..)` record then delegate to real handler).
- `FlowTestContext::new()` — Isolated flow persistence dependencies (`suspended_flows()` etc. share the same Arc).
- `AggregateScenario` / `ReplayedAggregate` — Aggregate event replay testing.
- `MessageCapture<T>` — Concurrency-safe publish/consume capture (`record_published` / `record_consumed` / `published()` / `consumed()`).
- Assertion helpers: `assert_success(..)` / `assert_failure(..)` / `assert_value(..)` / `assert_contains(..)` / `assert_error_code(..)`.

Boundaries: These tools simulate Catga contracts, not production deployments; scheduling/transport behaviors are covered by each adapter's own integration tests.

## Validation Commands (This Repository / Consumers Can Reference)

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --doc --workspace --all-features
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps
```

Performance benchmarks **must** use release mode: `cargo test --release ... -- --ignored --nocapture`.

## External Service Testing (E2E)

- NATS tests automatically start isolated Testcontainers instances when `CATGA_NATS_URL` is not set; when set, points to external service.
- Redis/MySQL/PostgreSQL/SQL Server tests are `#[ignore]`-marked real service E2E: provide corresponding `CATGA_*_URL` and run with `-- --ignored`.
- Full local E2E: `scripts/e2e.sh --profile core|sql|full` (Docker Compose in `testing/docker/compose.yaml`; `CATGA_CONTAINER_IMAGE_PREFIX` can point to internal/regional image registries).
