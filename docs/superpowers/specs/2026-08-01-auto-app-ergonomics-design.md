# AutoApp Ergonomics Design

## Goal

Make `catga-auto` the practical default for local CQRS and Axum applications
without hiding application-owned resources, tasks, transports, or configuration.

## Scope

This change covers mediator composition and Axum state wiring. It does not add
new NATS publishing APIs, connection defaults, background workers, or global
state. Those require separate transport compatibility and performance work.

## Design

`AutoAppBuilder` remains the explicit registration point, but `build` consumes
the builder. A builder cannot be reused after its registry is moved into the
immutable application graph, so its API must express that fact in Rust's type
system rather than return a runtime "already built" error.

`AutoApp` exposes `mediator_arc`, returning a clone of its existing
`Arc<Mediator>`. This is an ownership-preserving convenience for integrations
such as Axum; it creates no new mediator, registry, task, or synchronization
layer.

With the `axum` feature, `catga-auto::web` supplies a small adapter from
`&AutoApp` to `catga_axum::MediatorState`. Application code still constructs
the router, owns server lifecycle, chooses middleware, and attaches health
routes. The adapter only removes repetitive `Arc` plumbing.

The quickstart and Axum examples use `AutoApp` and `#[derive(Message)]` where
applicable. Handler dependencies remain explicit values passed to handlers.
The advanced order and distributed samples retain their current lower-level
composition until equivalent typed transport APIs are designed separately.

## Error Handling

Handler-registration errors still come from `Registry` while building. Because
`build(self)` consumes the builder, a failed build consumes it too; this is a
normal startup failure and avoids a partially reusable composition object.
The Axum adapter is infallible because it only clones an existing `Arc`.

## Performance and Ownership

The API performs only `Arc::clone` at integration boundaries. Request dispatch
and handlers use the existing `Mediator` unchanged. No hidden tasks, global
singletons, dynamic discovery, serialization, or transport setup are added.

## Acceptance Criteria

- `AutoApp::builder()` supports typed handler registration and one-shot build.
- `AutoApp::mediator_arc()` interoperates with `catga_axum::MediatorState`.
- The Axum quickstart composes app state without raw `Registry` or `Mediator`.
- The local mediator quickstart uses `catga-auto` and the `Message` derive.
- Focused unit and integration tests cover the new ownership and Axum paths.
- Workspace formatting, targeted tests, and clippy remain clean.
