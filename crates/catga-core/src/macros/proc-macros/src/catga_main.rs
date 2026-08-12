//! Attribute macro for zero-boilerplate Catga applications.
//!
//! Usage: `#[catga_main]`

use proc_macro::TokenStream;
use quote::quote;
use syn::Result;

/// Attribute macro that builds the application graph before running the user's entry point.
///
/// Basic usage:
/// ```ignore
/// // Ignored: the expansion builds an `AutoApp`, and this proc-macro crate has no
/// // application-runtime dependency to link a doctest against.
/// #[catga_main]
/// async fn main() -> CatgaResult<()> {
///     Ok(())
/// }
/// ```
///
/// Attribute arguments are compile-time errors: `AutoAppBuilder` has no transport hook, so a
/// transport is bound with explicit application code inside the function body instead.
///
/// The macro generates app initialization before calling the user's main body:
/// - Builds the application graph via `AutoApp`
/// - Stores the app to keep it alive
/// - Calls the user's renamed `main` body
pub fn expand_catga_main(attr: TokenStream, input: TokenStream) -> TokenStream {
    match catga_main_impl(attr.into(), input.into()) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.into_compile_error().into(),
    }
}

fn catga_main_impl(
    attr: proc_macro2::TokenStream,
    input: proc_macro2::TokenStream,
) -> Result<proc_macro2::TokenStream> {
    let input_fn: syn::ItemFn = syn::parse2(input)?;
    let fn_name = &input_fn.sig.ident;
    let fn_async = input_fn.sig.asyncness;
    let fn_inputs = &input_fn.sig.inputs;
    let fn_output = &input_fn.sig.output;
    let fn_body = &input_fn.block;
    let fn_vis = &input_fn.vis;

    reject_unsupported_arguments(attr)?;

    let inner_fn_name = syn::Ident::new("__catga_main_inner", fn_name.span());

    Ok(quote! {
        #fn_vis #fn_async fn #fn_name (#fn_inputs) #fn_output {
            let __catga_app = {
                use ::catga_core::auto::AutoApp;
                AutoApp::builder()
                    .build()
                    .expect("#[catga_main]: failed to build AutoApp")
            };
            // Keep app alive for the duration of main
            let _ = __catga_app.mediator_arc();
            #inner_fn_name().await
        }

        #fn_vis #fn_async fn #inner_fn_name (#fn_inputs) #fn_output #fn_body
    })
}

/// Rejects attribute arguments with a clear compile-time error.
///
/// `AutoAppBuilder` exposes no transport hook (transport, Flow, and cluster features are
/// explicit application dependencies), so the old `transport = expr` argument expanded to a
/// builder method that does not exist and could never compile. Binding a transport is plain
/// application code inside the entry-point body.
fn reject_unsupported_arguments(attr: proc_macro2::TokenStream) -> Result<()> {
    if attr.is_empty() {
        return Ok(());
    }
    for token in attr.clone() {
        if let proc_macro2::TokenTree::Ident(ident) = &token
            && ident == "transport"
        {
            return Err(syn::Error::new_spanned(
                ident,
                "`#[catga_main]` does not accept `transport = ...`: `AutoAppBuilder` has no \
                 transport hook, so bind the transport explicitly inside the function body",
            ));
        }
    }
    let first = attr
        .into_iter()
        .next()
        .expect("a non-empty attribute stream yields at least one token");
    Err(syn::Error::new_spanned(
        first,
        "`#[catga_main]` takes no arguments",
    ))
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::catga_main_impl;

    fn entry_fn() -> proc_macro2::TokenStream {
        quote!(
            async fn main() -> ::catga_core::CatgaResult<()> {
                Ok(())
            }
        )
    }

    #[test]
    fn transport_argument_is_rejected_with_a_clear_error() {
        let error = catga_main_impl(quote!(transport = 7u32), entry_fn())
            .expect_err("`transport` must be rejected: `AutoAppBuilder` has no transport hook");
        let message = error.to_string();
        assert!(
            message.contains("transport"),
            "error must name the unsupported argument, got: {message}"
        );
    }

    #[test]
    fn bare_entry_point_expands_through_auto_app() {
        let tokens = catga_main_impl(proc_macro2::TokenStream::new(), entry_fn())
            .expect("a bare entry point expands");
        let rendered = tokens.to_string();
        assert!(rendered.contains("AutoApp"));
        assert!(!rendered.contains("transport"));
    }
}
