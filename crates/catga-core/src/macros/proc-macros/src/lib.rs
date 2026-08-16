#![forbid(unsafe_code)]
//! Internal procedural macros for catga-core.
//!
//! Every macro in this crate expands to code that references `::catga_core` (or unqualified
//! handler traits) at the use site, so the macros are only usable from crates that depend on
//! `catga-core`; applications consume them through the `catga_core` re-exports
//! (`catga_core::Message`, `catga_core::catga_handlers!`, and so on) rather than by depending on
//! this crate directly.
//!
//! Because this proc-macro crate has no `catga-core` dependency, positive usage examples below
//! are marked `ignore` with an on-page reason; rejection contracts are covered by `compile_fail`
//! doctests, which fail during macro expansion before any `catga_core` name is resolved.

mod derive_command;
mod derive_event;
mod derive_request;
mod handler;
mod handlers;
mod impl_handlers;
mod message;
mod typed_mediator;

use proc_macro::TokenStream;

/// Implements `catga_core::Message` with the fully qualified, monomorphized Rust type name.
///
/// # Generated code
///
/// The impl provides `message_type()` as `core::any::type_name::<Self>()` plus the attribute
/// overrides below; every type parameter gains a `Send + Sync + 'static` bound. Depending on the
/// `#[catga(...)]` options present, the derive additionally emits:
///
/// - `AuthorizedRequest` — for `authorize`, `roles("...", ...)`, and/or `policy("...")`
///   (bare `authorize` requires an authenticated caller),
/// - `BatchKeyProvider` — for `batch_key = "field"` on a named-field struct; the field must
///   exist and is stringified per message,
/// - `BatchOptionsProvider` — for `batch(max_batch_size = N, timeout_ms = N, max_queue_length
///   = N, max_shards = N, flush_concurrency = N)`; every value must be a positive integer,
/// - a `visit_trace_tags` override — for field-level `#[catga(trace_tag)]` /
///   `#[catga(trace_tag = "name")]` or the bulk `trace_tags(prefix = "...",
///   include = [...], exclude = [...], all_public = ...)` form (prefix defaults to
///   `catga.message.`, `all_public` defaults to `true`).
///
/// `version = N` (positive, declared at most once; default `1`) feeds `schema_version()`, and
/// `priority = low|normal|high|critical` (at most once) feeds `priority()`. Invalid or
/// duplicated options are compile-time errors raised during expansion:
///
/// ```compile_fail
/// // Message versions must be positive.
/// use catga_core_macros::Message;
///
/// #[derive(Message)]
/// #[catga(version = 0)]
/// struct Payment;
/// ```
///
/// ```compile_fail
/// // A batch key must name an existing field of the struct.
/// use catga_core_macros::Message;
///
/// #[derive(Message)]
/// #[catga(batch_key = "account_id")]
/// struct Payment { id: u64 }
/// ```
///
/// # Example
///
/// ```ignore
/// // Ignored: the expansion implements `::catga_core` traits, and this proc-macro crate
/// // has no `catga-core` dependency to link a doctest against.
/// use catga_core::Message;
///
/// #[derive(Message)]
/// #[catga(version = 2, priority = high, authorize, roles("admin", "ops"))]
/// struct Refund {
///     #[catga(trace_tag)]
///     order_id: u64,
///     amount_cents: u64,
/// }
/// ```
#[proc_macro_derive(Message, attributes(catga))]
pub fn derive_message(input: TokenStream) -> TokenStream {
    message::expand_message(input.into()).into()
}

/// Builds an explicit `catga_core::CatgaResult<Registry>` from typed request, command, and
/// event handler expressions.
///
/// # Grammar
///
/// ```text
/// catga_handlers! {
///     request  <MessagePath> => <handler expr>;
///     command  <MessagePath> => <handler expr>;
///     event    <MessagePath> => [<handler expr>, ...];
///     ...
/// }
/// ```
///
/// Entries are separated by `;`. A request or command message may appear at most once and an
/// event entry must list at least one handler; violations are compile-time errors raised during
/// expansion, before any handler expression is type-checked:
///
/// ```compile_fail
/// // A request message cannot have two handlers.
/// use catga_core_macros::catga_handlers;
///
/// catga_handlers! {
///     request GetBalance => read_balance;
///     request GetBalance => read_balance_replica;
/// }
/// ```
///
/// # Generated code
///
/// The expansion is a block expression of type `CatgaResult<Registry>`: it creates a
/// `Registry::new()`, registers each entry in order (`register_request`/`register_command`
/// propagate registration conflicts with `?`; every event handler is registered with
/// `register_event`), and yields the populated registry.
///
/// ```ignore
/// // Ignored: the expansion references `::catga_core::Registry`, and this proc-macro crate
/// // has no `catga-core` dependency to link a doctest against.
/// use catga_core::{Mediator, catga_handlers, request_handler};
///
/// # async fn run() -> catga_core::CatgaResult<()> {
/// let mediator = Mediator::new(catga_handlers! {
///     request GetBalance => request_handler(|q: GetBalance| async move { Ok(42_u64) });
/// }?);
/// # Ok(())
/// # }
/// ```
#[proc_macro]
pub fn catga_handlers(input: TokenStream) -> TokenStream {
    handlers::expand(input.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Generates a fully monomorphized mediator struct with zero-allocation dispatch.
///
/// # Grammar
///
/// ```text
/// catga_typed_mediator! {
///     [pub] struct <MediatorName>;
///     request  <MessagePath> => <handler expr>;
///     command  <MessagePath> => <handler expr>;
///     event    <MessagePath> => [<handler expr>, ...];
///     ...
/// }
/// ```
///
/// Duplicate request or command messages and empty event handler lists are compile-time errors
/// raised during expansion:
///
/// ```compile_fail
/// // A command message cannot be registered twice.
/// use catga_core_macros::catga_typed_mediator;
///
/// catga_typed_mediator! {
///     pub struct BankMediator;
///     command Transfer => transfer_a;
///     command Transfer => transfer_b;
/// }
/// ```
///
/// # Generated code
///
/// The expansion defines `<MediatorName>` with one typed field per registration (event handlers
/// are stored as fixed-size arrays in registration order), a `new` constructor taking the
/// handlers positionally, and inherent `send`/`send_command`/`publish` methods. Dispatch goes
/// through per-message `SealedRequestDispatch`/`SealedCommandDispatch`/`SealedEventDispatch`
/// impls, so sending an unregistered message type is a compile-time error and the hot path has
/// no `dyn`, downcast, or heap allocation. Event fan-out is sequential in registration order and
/// returns the first handler error after all handlers ran.
///
/// ```ignore
/// // Ignored: the expansion references `::catga_core` sealed dispatch traits, and this
/// // proc-macro crate has no `catga-core` dependency to link a doctest against.
/// use catga_core::catga_typed_mediator;
///
/// catga_typed_mediator! {
///     pub struct ShopMediator;
///     request GetCart => CartReader;
///     command Checkout => CheckoutWriter;
///     event OrderPlaced => [Projector, Notifier];
/// }
///
/// # async fn run() -> catga_core::CatgaResult<()> {
/// let mediator = ShopMediator::new(CartReader, CheckoutWriter, [Projector, Notifier]);
/// let cart = mediator.send(GetCart { id: 1 }).await?;
/// # Ok(())
/// # }
/// ```
#[proc_macro]
pub fn catga_typed_mediator(input: TokenStream) -> TokenStream {
    typed_mediator::expand(input.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Scans an impl block for async methods and generates handler registrations.
///
/// # Automatic Type Detection
///
/// The macro automatically detects handler types based on method signatures:
///
/// - `async fn name(&self, msg: M) -> CatgaResult<T>` where `T != ()` -> **Request handler**
/// - `async fn name(&self, cmd: C) -> CatgaResult<()>` -> **Command handler**
/// - `async fn on_name(&self, event: E) -> CatgaResult<()>` -> **Event handler**
///
/// # Typed Mediator (Optional)
///
/// Pass a name to generate a zero-allocation typed mediator. Each generated wrapper stores a
/// clone of the service, so the service type must be `Clone`; without a name the wrappers share
/// one `Arc` of the service instead:
///
/// ```ignore
/// // Ignored: the expansion references `::catga_core` handler traits, and this proc-macro
/// // crate has no `catga-core` dependency to link a doctest against.
/// #[catga_service(BankMediator)]
/// impl BankService {
///     async fn get_balance(&self, msg: GetBalance) -> CatgaResult<u64> { ... }
///     async fn transfer(&self, cmd: Transfer) -> CatgaResult<()> { ... }
/// }
///
/// let mediator = BankMediator::new(bank_service.clone());
/// let balance = mediator.send(GetBalance { account_id: 1 }).await?;
/// ```
///
/// # Generated Code
///
/// The macro generates:
/// - A `registry()` function returning `CatgaResult<Registry>`
/// - Wrapper structs implementing `Handler<M>`, `CommandHandler<C>`, or `EventHandler<E>` for each method
///
/// ```ignore
/// // Ignored: the expansion references `::catga_core` types, and this proc-macro crate
/// // has no `catga-core` dependency to link a doctest against.
/// use catga_core::{CatgaResult, Mediator, catga_request, catga_command, catga_service};
///
/// #[catga_request(response = u64)]
/// struct Double(u64);
///
/// #[derive(catga_command)]
/// struct Log(String);
///
/// struct Calculator;
///
/// #[catga_service]
/// impl Calculator {
///     async fn double(&self, msg: Double) -> CatgaResult<u64> {
///         Ok(msg.0 * 2)
///     }
///     async fn log(&self, msg: Log) -> CatgaResult<()> {
///         Ok(())
///     }
/// }
///
/// # async fn example() -> CatgaResult<()> {
/// let mediator = Mediator::new(Calculator::registry()?);
/// assert_eq!(mediator.send(Double(21)).await?, 42);
/// # Ok(())
/// # }
/// ```
#[proc_macro_attribute]
pub fn catga_service(attr: TokenStream, input: TokenStream) -> TokenStream {
    let typed_mediator_name = if attr.is_empty() {
        None
    } else {
        match syn::parse::<syn::Ident>(attr) {
            Ok(ident) => Some(ident),
            Err(e) => return e.into_compile_error().into(),
        }
    };

    impl_handlers::expand_impl_handlers(input.into(), typed_mediator_name).into()
}

/// Implements `catga_core::Message` and `catga_core::Request` with the response type
/// specified via `#[catga_request(response = TypeName)]`.
///
/// # Generated code
///
/// The annotated item is re-emitted unchanged; the macro adds a marker `Message` impl and a
/// `Request` impl with `type Response` taken from the attribute. Every type parameter gains
/// `Clone + Send + Sync + 'static` bounds so generic messages satisfy `Message` (existing
/// bounds on a parameter are preserved). The response type accepts any syntactically valid type
/// expression, including qualified paths and generic arguments.
///
/// The `response` key is required; omitting it is a compile-time error raised during expansion:
///
/// ```compile_fail
/// // #[catga_request] requires `response = <type>`.
/// use catga_core_macros::catga_request;
///
/// #[catga_request]
/// struct GetBalance { account_id: u64 }
/// ```
///
/// ```ignore
/// // Ignored: the expansion implements `::catga_core` traits, and this proc-macro crate
/// // has no `catga-core` dependency to link a doctest against.
/// use catga_core::catga_request;
///
/// #[catga_request(response = u64)]
/// struct GetBalance { account_id: u64 }
/// ```
#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn catga_request(attr: TokenStream, input: TokenStream) -> TokenStream {
    derive_request::expand_catga_request(attr, input)
}

/// Implements `catga_core::Message` and `catga_core::Command`.
///
/// # Generated code
///
/// The derive adds a marker `Message` impl and an empty `Command` impl. Every type parameter
/// gains `Clone + Send + Sync + 'static` bounds so generic messages satisfy `Message`
/// (existing bounds on a parameter are preserved).
///
/// ```ignore
/// // Ignored: the expansion implements `::catga_core` traits, and this proc-macro crate
/// // has no `catga-core` dependency to link a doctest against.
/// use catga_core::catga_command;
///
/// #[derive(catga_command)]
/// struct ChargeCard { order_id: u64, amount_cents: u64 }
/// ```
#[proc_macro_derive(catga_command)]
pub fn derive_command(input: TokenStream) -> TokenStream {
    derive_command::expand_derive_command(input)
}

/// Implements `catga_core::Message` and `catga_core::Event`.
/// Events must be Clone, so this derive enforces that bound.
///
/// # Generated code
///
/// The derive adds a marker `Message` impl and an empty `Event` impl. Every type parameter
/// gains `Clone + Send + Sync + 'static` bounds so events can be fanned out to every
/// registered handler.
///
/// ```ignore
/// // Ignored: the expansion implements `::catga_core` traits, and this proc-macro crate
/// // has no `catga-core` dependency to link a doctest against.
/// use catga_core::catga_event;
///
/// #[derive(Clone, catga_event)]
/// struct OrderPlaced { order_id: u64 }
/// ```
#[proc_macro_derive(catga_event)]
pub fn derive_event(input: TokenStream) -> TokenStream {
    derive_event::expand_derive_event(input)
}

/// Marks an impl block as a Catga handler for explicit registration.
///
/// # Generated code
///
/// The impl block is validated and then re-emitted unchanged; the attribute itself emits no
/// registration code. Register the handler explicitly with a `Registry` (for example via
/// `register_request`, `register_command`, or `register_event`).
///
/// The impl block must implement exactly one of `Handler<M>`, `CommandHandler<M>`, or
/// `EventHandler<M>` with an explicit message type; other traits, non-trait impl blocks, and
/// untyped impls are compile-time errors raised during expansion:
///
/// ```compile_fail
/// // #[catga_handler] requires a supported trait impl with a message type.
/// use catga_core_macros::catga_handler;
///
/// struct NotAHandler;
///
/// #[catga_handler]
/// impl NotAHandler {}
/// ```
#[proc_macro_attribute]
pub fn catga_handler(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let impl_item: syn::ItemImpl = match syn::parse2(item.into()) {
        Ok(item) => item,
        Err(e) => return e.into_compile_error().into(),
    };

    match handler::expand_handler(impl_item) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.into_compile_error().into(),
    }
}
