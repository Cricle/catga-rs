//! Attribute macro for zero-boilerplate Catga applications.
//!
//! Usage: `#[catga_main]` or `#[catga_main(transport = expr)]`

use proc_macro::TokenStream;
use quote::quote;
use syn::Result;

/// Attribute macro that builds the application graph and optionally initializes a transport.
///
/// Basic usage:
/// ```
/// #[catga_main]
/// async fn main() -> CatgaResult<()> {
///     Ok(())
/// }
/// ```
///
/// With transport initialization:
/// ```
/// #[catga_main(transport = catga_local::LocalTransport::new())]
/// async fn main() -> CatgaResult<()> {
///     Ok(())
/// }
/// ```
///
/// The macro generates app initialization before calling the user's main body:
/// - Builds the application graph via `AutoApp` (which binds the mediator and transport)
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
    let input_fn: syn::ItemFn = syn::parse2(input.clone())?;
    let fn_name = &input_fn.sig.ident;
    let fn_async = input_fn.sig.asyncness;
    let fn_inputs = &input_fn.sig.inputs;
    let fn_output = &input_fn.sig.output;
    let fn_body = &input_fn.block;
    let fn_vis = &input_fn.vis;

    // Parse: transport = Expr
    let transport_expr = parse_transport_attr(attr)?;

    let inner_fn_name = syn::Ident::new(&"__catga_main_inner".to_string(), fn_name.span());

    if let Some(expr) = transport_expr {
        // With transport: .transport(expr) binds both mediator and transport in build()
        Ok(quote! {
            #fn_vis #fn_async fn #fn_name #fn_inputs #fn_output {
                let __catga_app = {
                    use catga_auto::AutoApp;
                    AutoApp::builder()
                        .transport(#expr)
                        .build()
                        .expect("#[catga_main]: failed to build AutoApp with transport")
                };
                // Keep app alive for the duration of main
                let _ = __catga_app.mediator_arc();
                #inner_fn_name().await
            }

            #fn_vis #fn_async fn #inner_fn_name #fn_inputs #fn_output #fn_body
        })
    } else {
        // Without transport: .build() binds the mediator via global_dispatch
        Ok(quote! {
            #fn_vis #fn_async fn #fn_name #fn_inputs #fn_output {
                let __catga_app = {
                    use catga_auto::AutoApp;
                    AutoApp::builder()
                        .build()
                        .expect("#[catga_main]: failed to build AutoApp")
                };
                // Keep app alive for the duration of main
                let _ = __catga_app.mediator_arc();
                #inner_fn_name().await
            }

            #fn_vis #fn_async fn #inner_fn_name #fn_inputs #fn_output #fn_body
        })
    }
}

/// Parses `transport = Expr` from the attribute token stream.
fn parse_transport_attr(attr: proc_macro2::TokenStream) -> Result<Option<proc_macro2::TokenStream>> {
    let mut rest = attr.into_iter().peekable();
    while let Some(token) = rest.next() {
        if let proc_macro2::TokenTree::Ident(ident) = &token && ident == "transport" {
            // Expect '='
            match rest.next() {
                Some(proc_macro2::TokenTree::Punct(p)) if p.as_char() == '=' => {}
                Some(other) => {
                    return Err(syn::Error::new_spanned(
                        other,
                        "expected `=` after `transport`",
                    ));
                }
                None => {
                    return Err(syn::Error::new(
                        ident.span(),
                        "expected expression after `transport =`",
                    ));
                }
            }
            // Collect the rest as the expression
            let expr_tokens: proc_macro2::TokenStream = rest.collect();
            if expr_tokens.is_empty() {
                return Err(syn::Error::new(
                    ident.span(),
                    "expected expression after `transport =`",
                ));
            }
            return Ok(Some(expr_tokens));
        }
    }
    Ok(None)
}
