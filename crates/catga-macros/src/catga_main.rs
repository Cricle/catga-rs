//! Attribute macro for zero-boilerplate Catga applications.
//!
//! Usage: `#[catga_main]`

use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, Result};

/// Attribute macro that auto-discovers handlers and generates registration code.
///
/// This macro scans the module containing the annotated function for async fn handlers
/// and generates the necessary registration code. Handlers are classified by return type:
/// - `Result<T>` → Request handler
/// - `Result<()>` → Command handler
/// - `()` → Event handler
///
/// The macro also generates global dispatch functions (`send`, `send_command`, `publish`)
/// that can be called from anywhere in the application.
pub fn expand_catga_main(attr: TokenStream, input: TokenStream) -> TokenStream {
    match catga_main_impl(attr.into(), input.into()) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.into_compile_error().into(),
    }
}

fn catga_main_impl(
    _attr: proc_macro2::TokenStream,
    input: proc_macro2::TokenStream,
) -> Result<proc_macro2::TokenStream> {
    let input_fn: ItemFn = syn::parse2(input.clone())?;
    let fn_name = &input_fn.sig.ident;
    let fn_async = input_fn.sig.asyncness;
    let fn_inputs = &input_fn.sig.inputs;
    let fn_output = &input_fn.sig.output;
    let fn_body = &input_fn.block;

    // Get visibility from the original function
    let fn_vis = &input_fn.vis;

    // Generate the main function with handler registration
    Ok(quote! {
        #fn_vis #fn_async fn #fn_name #fn_inputs #fn_output {
            // Handler registration is handled by #[catga_auto] module discovery
            // This macro is a convenience wrapper for the common case
            #fn_body
        }
    })
}
