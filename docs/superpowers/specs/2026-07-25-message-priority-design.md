# Typed Message Priority Design

## Goal

Represent the source `IPrioritizedMessage.Priority` contract in typed Rust
publication without string metadata, per-message maps, or a transport-specific
priority API. A message may declare one of Catga's four existing priority
levels and every typed outbound path must preserve it in `Envelope` metadata.

## API

`catga_core::Message` gains `priority(&self) -> MessagePriority`, with
`MessagePriority::Normal` as its backwards-compatible default. Implementors
whose priority depends on runtime data can override that method without a new
trait object or allocation.

`#[derive(Message)]` accepts `#[catga(priority = low | normal | high |
critical)]`. It emits a direct `Message::priority` implementation containing
the corresponding enum variant. The macro rejects duplicate declarations and
any non-identifier or unsupported value with a compile-time diagnostic.

The attribute is deliberately a message-level configuration. Priority belongs
to a complete transport unit, not an individual field, and therefore needs no
runtime field lookup or cloning.

## Propagation

`PostcardTransport` applies the declared priority while constructing its
`MessageMetadata`. `PostcardScheduledOutbox` does the same before durable
insertion. `PostcardRequestClient` applies it to the request envelope. Each
path keeps its existing correlation, schema-version, QoS, delivery-mode, and
header behavior.

An inbound `TransportContext` also retains the received priority as a `Copy`
enum. Nested typed publication, durable scheduling, and typed requests use
that scoped value before the outgoing message's declared value. This preserves
the source transport-context precedence without converting priority into a
header or allocating an override map.

This maps the source's effective outgoing priority but improves its wire
representation: Rust uses the validated typed `MessageMetadata::priority`
field rather than adding an untyped `x-priority` header. Codec-backed adapters
already preserve this field, and RobustMQ already maps it to its supported
mailbox levels.

## Resource and Error Semantics

The implementation copies only the `Copy` priority enum into the metadata
already being constructed. It does not allocate, parse strings, add headers,
or change batch concurrency. Macro validation happens at compile time. Runtime
transport errors remain existing `CatgaResult` values; no `unwrap` or `expect`
is introduced in production paths.

## Verification

Tests prove the macro returns `High`, direct typed publication delivers `High`,
a due scheduled outbox record has `High`, and the request backend receives
`High`. Scoped inbound priority is verified across all three outbound paths.
Existing normal-priority behavior stays covered by the default method.
Focused tests, formatting, Clippy, Rustdoc warning checks, workspace tests,
and a forbidden-broker source scan are required after the implementation.
