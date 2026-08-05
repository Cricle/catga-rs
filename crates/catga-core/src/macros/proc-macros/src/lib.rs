#![forbid(unsafe_code)]
//! Internal procedural macros for catga-core.

mod auto;
mod catga_main;
mod derive_command;
mod derive_event;
mod derive_request;
mod handlers;
mod impl_handlers;
mod message;
mod typed_mediator;

use proc_macro::TokenStream;

/// Implements `catga_core::Message` with the fully qualified, monomorphized Rust type name.
#[proc_macro_derive(Message, attributes(catga))]
pub fn derive_message(input: TokenStream) -> TokenStream {
    message::expand_message(input.into()).into()
}

/// Builds an explicit `catga_core::CatgaResult<Registry>` from typed request, command, and
/// event handler expressions.
#[proc_macro]
pub fn catga_handlers(input: TokenStream) -> TokenStream {
    handlers::expand(input.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Generates a fully monomorphized mediator struct with zero-allocation dispatch.
#[proc_macro]
pub fn catga_typed_mediator(input: TokenStream) -> TokenStream {
    typed_mediator::expand(input.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Scans a module for handlers and generates registration code.
#[proc_macro_attribute]
pub fn catga_auto(_attr: TokenStream, item: TokenStream) -> TokenStream {
    auto::expand_auto(item.into())
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
/// # Generated Code
///
/// The macro generates:
/// - A `registry()` function returning `CatgaResult<Registry>`
/// - Wrapper structs implementing `Handler<M>` or `CommandHandler<C>` for each method
///
/// # Example
///
/// ```
/// use catga_core::{CatgaResult, auto::AutoApp, catga_request, catga_command, catga_service};
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
/// let app = AutoApp::from_registry(Calculator::registry()?)?;
/// assert_eq!(app.mediator().send(Double(21)).await?, 42);
/// # Ok(())
/// # }
/// ```
#[proc_macro_attribute]
pub fn catga_service(_attr: TokenStream, input: TokenStream) -> TokenStream {
    impl_handlers::expand_impl_handlers(input.into()).into()
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
#[proc_macro_attribute]
pub fn catga_main(attr: TokenStream, input: TokenStream) -> TokenStream {
    catga_main::expand_catga_main(attr, input)
}

/// Marks an impl block as a Catga handler for auto-registration.
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
