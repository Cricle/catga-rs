# Mediator Handle Design

## Goal

Allow startup-constructed request handlers to publish follow-up events or send
follow-up requests through the application mediator.  This ports the useful
dependency-injection behavior of upstream handlers without creating a global
mediator or making the immutable `Registry` mutable after startup.

## Design

`MediatorHandle` owns `Arc<OnceLock<Arc<Mediator>>>`.  A handler receives a
clone of the empty handle while the registry is assembled.  After
`Arc<Mediator>` is built from that immutable registry, startup calls `bind` on
the same handle exactly once.

`send` and `publish` delegate to the bound mediator.  Before binding they
return `ErrorCode::Unavailable`; a second bind returns `ErrorCode::Conflict`.
Reads after binding use `OnceLock::get`, so dispatch adds no mutex, allocation,
task, or global lookup.  The handle is explicit application state and can be
passed directly into handlers, flows, and endpoints.

## Testing

Tests construct a handler that owns the initially empty handle, register it,
build the mediator, and bind the handle.  They prove pre-bind rejection,
post-bind event publication, and duplicate-bind rejection.  This validates the
actual construction cycle rather than a mock mediator.
