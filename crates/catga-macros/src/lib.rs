#![forbid(unsafe_code)]
//! Procedural macros for Catga, including message derives with stable Rust type identities.

use proc_macro::TokenStream;

mod handlers;
mod typed_mediator;
pub(crate) mod auto;

mod derive_command;
mod derive_event;
mod derive_request;
mod catga_main;
mod message;

use crate::auto::expand_auto;

/// Implements `catga_core::Message` with the fully qualified, monomorphized Rust type name.
///
/// The generated `message_type` is identical to the default implementation:
/// `std::any::type_name::<Self>()`.
///
/// `#[catga(authorize, roles("role"), policy("name"))]` additionally emits
/// an `catga_core::AuthorizedRequest` implementation for request types.
/// `#[catga(batch_key = "field_name")]` emits `catga_core::BatchKeyProvider`.
/// `#[catga(priority = high)]` emits a static typed transport priority.
/// `#[catga(trace_tag)]` on a named field emits an explicit structured-tracing tag using the
/// `catga.message.{field}` name; `#[catga(trace_tag = "name")]` selects an explicit name.
/// `#[catga(trace_tags(prefix = "name.", include = ["field"], exclude = ["field"]))]`
/// bulk-selects named fields. An explicit `include` takes precedence over public-field selection,
/// `exclude` removes bulk-selected fields, and field-level `trace_tag` declarations retain their
/// explicit names. `all_public = false` disables the public-field fallback.
/// Generic message structs are supported when their type parameters satisfy Catga's required
/// `Send + Sync + 'static` bounds.
///
/// # Example
///
/// ```
/// use catga_core::Message as _;
/// use catga_macros::Message;
///
/// #[derive(Message)]
/// struct RebuildSearchIndex {
///     tenant: String,
/// }
///
/// let message = RebuildSearchIndex { tenant: "acme".into() };
/// assert!(message.message_type().ends_with("RebuildSearchIndex"));
/// ```
#[proc_macro_derive(Message, attributes(catga))]
pub fn derive_message(input: TokenStream) -> TokenStream {
    message::expand_message(input.into()).into()
}

/// Builds an explicit `catga_core::CatgaResult<Registry>` from typed request, command, and
/// event handler expressions.
///
/// Each request or command message may be registered exactly once. Repeating either message
/// kind is reported during macro expansion when it is syntactically visible; equivalent type
/// aliases return a startup `catga_core::ErrorCode::Conflict` instead of panicking. Event
/// messages may intentionally register multiple handlers.
/// Handler entries are expressions, so applications can register a unit-like handler path or
/// construct a handler with explicit Rust dependencies, for example
/// `request CreateOrder => CreateOrderHandler::new(repository)` or
/// `command RebuildIndex => RebuildIndexHandler::new(repository)`.
///
/// # Example
///
/// The macro emits a registration function for the selected `catga_core::Mediator`. Handler
/// types remain ordinary, explicit Rust values; the hidden setup below is only the minimal event
/// and handler definition needed to compile the visible registration.
///
/// ```no_run
/// # use async_trait::async_trait;
/// # use catga_core::{CatgaError, CatgaResult, Event, EventHandler, Message};
/// use catga_macros::catga_handlers;
///
/// # #[derive(Clone)]
/// # struct InventoryRebuilt;
/// # impl Message for InventoryRebuilt {}
/// # impl Event for InventoryRebuilt {}
/// # struct RefreshReadModel;
/// # #[async_trait]
/// # impl EventHandler<InventoryRebuilt> for RefreshReadModel {
/// #     async fn handle(&self, _: InventoryRebuilt) -> CatgaResult<()> { Ok(()) }
/// # }
/// # struct PublishAuditEvent;
/// # #[async_trait]
/// # impl EventHandler<InventoryRebuilt> for PublishAuditEvent {
/// #     async fn handle(&self, _: InventoryRebuilt) -> CatgaResult<()> { Ok(()) }
/// # }
/// # fn register() -> Result<(), CatgaError> {
/// catga_handlers! {
///     event InventoryRebuilt => [RefreshReadModel, PublishAuditEvent]
/// }?;
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
/// Unlike `catga_handlers!` which builds a type-erased `Registry`, this macro generates a
/// concrete struct where each handler is stored as a typed field. Dispatch goes through
/// sealed traits that the compiler monomorphizes per message type — no `Box<dyn Any>`,
/// no `downcast`, no vtable indirection on the hot path.
///
/// # Example
///
/// ```ignore
/// catga_typed_mediator! {
///     pub struct AppMediator;
///     request GetOrder => GetOrderHandler;
///     command ShipOrder => ShipOrderHandler;
///     event OrderCreated => [ProjectionHandler, AuditHandler];
/// }
///
/// let mediator = AppMediator::new(
///     GetOrderHandler,
///     ShipOrderHandler,
///     [ProjectionHandler, AuditHandler],
/// );
/// let order = mediator.send(GetOrder { id: 1 }).await?;
/// ```
#[proc_macro]
pub fn catga_typed_mediator(input: TokenStream) -> TokenStream {
    typed_mediator::expand(input.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Scans a module for handlers and generates registration code.
///
/// The macro scans the module for impl blocks (Handler, CommandHandler, EventHandler)
/// and plain async fn handlers, then generates a `__catga_auto_register` function
/// that registers all handlers with a `Registry`.
///
/// # Example
///
/// ```ignore
/// #[catga_auto]
/// mod handlers {
///     use super::*;
///
///     // Plain async fn - no struct needed!
///     async fn ping_handler(_: Ping) -> CatgaResult<String> {
///         Ok("pong".to_string())
///     }
///
///     // Or use impl blocks
///     struct EchoService;
///     impl Handler<Echo> for EchoService {
///         async fn handle(&self, _: Echo) -> CatgaResult<String> {
///             Ok("echo".to_string())
///         }
///     }
/// }
///
/// // In your app builder:
/// let registry = handlers::__catga_auto_register(Registry::new())?;
/// ```
#[proc_macro_attribute]
pub fn catga_auto(_attr: TokenStream, item: TokenStream) -> TokenStream {
    expand_auto(item.into())
}

/// Implements `catga_core::Message` and `catga_core::Request` with the response type
/// specified via `#[catga_request(response = TypeName)]`.
#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn catga_request(attr: TokenStream, input: TokenStream) -> TokenStream {
    derive_request::expand_catga_request(attr, input)
}

/// Implements `catga_core::Message` and `catga_core::Command`.
#[proc_macro_derive(catga_command)]
pub fn derive_command(input: TokenStream) -> TokenStream {
    derive_command::expand_derive_command(input)
}

/// Implements `catga_core::Message` and `catga_core::Event`.
/// Events must be Clone, so this derive enforces that bound.
#[proc_macro_derive(catga_event)]
pub fn derive_event(input: TokenStream) -> TokenStream {
    derive_event::expand_derive_event(input)
}

/// Zero-boilerplate application entry point with auto-handler discovery.
///
/// This macro wraps your async main function and auto-discovers handlers defined
/// in the same module. Handlers are plain async fns that return:
/// - `Result<T>` for request handlers
/// - `Result<()>` for command handlers
/// - `()` for event handlers
///
/// ```ignore
/// use catga_auto::{catga_request, catga_main, send};
///
/// #[catga_request(response = String)]
/// struct GetUser(String);
///
/// async fn get_user_handler(msg: GetUser) -> CatgaResult<String> {
///     Ok(format!("user: {}", msg.0))
/// }
///
/// #[catga_main]
/// async fn main() -> CatgaResult<()> {
///     let result = send(GetUser("123".into())).await?;
///     println!("{}", result);
///     Ok(())
/// }
/// ```
#[proc_macro_attribute]
pub fn catga_main(attr: TokenStream, input: TokenStream) -> TokenStream {
    catga_main::expand_catga_main(attr, input)
}

/// Marks an impl block as a Catga handler for auto-registration.
///
/// Use this attribute on `impl Handler<M>`, `impl CommandHandler<C>`, or
/// `impl EventHandler<E>` blocks inside a `#[catga_auto]` module. The impl
/// will be automatically registered with the application's `Registry` at
/// compile time.
///
/// ```ignore
/// #[catga_auto]
/// mod handlers {
///     struct MyService;
///
///     #[catga_handler]
///     impl Handler<Ping> for MyService {
///         async fn handle(&self, ping: Ping) -> CatgaResult<String> {
///             Ok("pong".to_string())
///         }
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn catga_handler(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let impl_item: syn::ItemImpl = match syn::parse2(item.into()) {
        Ok(item) => item,
        Err(e) => return e.into_compile_error().into(),
    };

    match auto::expand_handler(impl_item) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.into_compile_error().into(),
    }
}
