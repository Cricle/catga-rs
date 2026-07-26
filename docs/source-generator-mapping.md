# C# Source Generator Mapping

This document is a file-level audit of `upstream-catga/src/Catga.SourceGenerator`.
Rust uses procedural macros, trait bounds, and explicit registry construction in
place of Roslyn analyzers, reflection discovery, and module initializers.

| Upstream file | Rust replacement | Verification |
| --- | --- | --- |
| `ActivityTagProviderGenerator.cs` | `#[derive(Message)]` with `#[catga(trace_tag)]` fields emits `Message::visit_trace_tags`. | `tests/macros.rs`, `tests/observability.rs` |
| `Analyzers/BlockingCallAnalyzer.cs` | Tokio async APIs plus Clippy and workspace review reject blocking runtime paths; Catga public methods are async. | `cargo clippy --workspace --all-targets -D warnings` |
| `Analyzers/CatgaAnalyzerRules.cs` | Stable `CatgaResult`/`ErrorCode` contracts and Rust lint configuration in root `Cargo.toml`. | workspace Clippy and Rustdoc gates |
| `Analyzers/MissingMemoryPackableAttributeAnalyzer.cs` | `Serialize`/`DeserializeOwned` bounds are required at the concrete Postcard API boundary. | `tests/codec.rs` |
| `Analyzers/MissingSerializerRegistrationAnalyzer.cs` | Codec availability is a compile-time generic bound; no reflection registration exists. | `catga-codec-postcard` Rustdoc and tests |
| `Analyzers/MultipleHandlersAnalyzer.cs` | `catga_handlers!` rejects duplicate request registrations at macro expansion. | `crates/catga-macros/src/handlers.rs` tests |
| `Analyzers/NamingConventionAnalyzer.cs` | Rust item naming is enforced by compiler and Clippy lints rather than a framework-specific analyzer. | workspace Clippy gate |
| `Analyzers/ScopedLifetimeMismatchAnalyzer.cs` | `Arc` ownership and `Send + Sync + 'static` trait bounds express runtime sharing without DI lifetimes. | crate public trait bounds |
| `EndpointRegistrationGenerator.cs` | `catga_routes!` emits explicit typed Axum route registrations. | `tests/axum.rs` |
| `EventRouterGenerator.cs` | `catga_handlers!` emits static event handler registration. | `tests/macros.rs`, `tests/mediator.rs` |
| `FlowDslRegistrationGenerator.cs` | Explicit `FlowDefinition` and `FlowRuntime` construction. Dynamic flow hot reload is intentionally excluded. | `tests/flow/{local,executor,recovery}.rs` |
| `FlowStateChangeTrackingGenerator.cs` | Immutable versioned `FlowState` revisions and explicit DSL checkpoints. | `tests/flow/{state,persistence,dsl_progress}.rs` |
| `MessageIdGenerator.cs` | `DistributedIdGenerator` and lock-free `SnowflakeIdGenerator`; `fill` and `try_write_next_id` use caller-owned buffers. | `tests/distributed_id.rs` |
| `UnifiedModuleInitializerGenerator.cs` | Explicit application startup composition; Rust has no global module initializer registration. | `Registry::new`, `catga_handlers!` |
| `UnifiedRegistrationGenerator.cs` | `catga_handlers!` returns one checked `Registry` without reflection scanning. | `tests/macros.rs` |

## Deliberate Differences

The Rust APIs do not reproduce .NET dependency-injection lifetime diagnostics or
MemoryPack-specific annotations. Those mechanisms configure a runtime discovery
system that Rust does not use. Required capabilities are instead represented by
generic bounds at the call site, while ownership and concurrency are expressed
by the language type system.
