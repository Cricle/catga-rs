//! Attribute macro for implementing `catga_core::Message` and `catga_core::Request`.
//!
//! Usage: `#[catga_request(response = TypeName)]` or `#[catga_request(response = path::to::Type)]`

use proc_macro::TokenStream;
use quote::quote;
use syn::{GenericParam, Generics, Ident, Result, parse_quote};

/// Implements `catga_core::Message` and `catga_core::Request` with the response type
/// specified via `#[catga_request(response = TypeName)]`.
pub fn expand_catga_request(attr: TokenStream, input: TokenStream) -> TokenStream {
    match catga_request_impl(attr.into(), input.into()) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.into_compile_error().into(),
    }
}

fn catga_request_impl(
    attr: proc_macro2::TokenStream,
    input: proc_macro2::TokenStream,
) -> Result<proc_macro2::TokenStream> {
    let input = syn::parse2::<syn::DeriveInput>(input.clone())?;
    let name = &input.ident;
    let generics = add_message_bounds(&input.generics);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    // Parse response type from attribute
    let response_type = parse_response_attr(&attr, name)?;

    Ok(quote! {
        #input

        impl #impl_generics ::catga_core::Message for #name #ty_generics #where_clause {}
        impl #impl_generics ::catga_core::Request for #name #ty_generics #where_clause {
            type Response = #response_type;
        }
    })
}

/// Adds the bounds required by `Message` and `Request` to every type parameter.
///
/// Only the parameter identifier is interpolated: interpolating a [`syn::TypeParam`] that
/// already carries bounds would emit `T: Bound: Clone` and panic `parse_quote!`.
fn add_message_bounds(generics: &Generics) -> Generics {
    let mut g = generics.clone();
    let idents: Vec<Ident> = g
        .params
        .iter()
        .filter_map(|param| match param {
            GenericParam::Type(type_param) => Some(type_param.ident.clone()),
            _ => None,
        })
        .collect();
    let where_clause = g.make_where_clause();
    for ident in idents {
        where_clause
            .predicates
            .push(parse_quote!(#ident: Clone + Send + Sync + 'static));
    }
    g
}

// Parse: response = TypeName from token stream
// Supports: response = String, response = std::string::String, response = Result<T, E>
fn parse_response_attr(attr: &proc_macro2::TokenStream, name: &Ident) -> Result<syn::Type> {
    let mut tokens = attr.clone().into_iter().peekable();

    while let Some(token) = tokens.next() {
        if let proc_macro2::TokenTree::Ident(ident) = &token
            && ident == "response"
            && let Some(next) = tokens.next()
            && let proc_macro2::TokenTree::Punct(punct) = next
            && punct.as_char() == '='
        {
            // Collect all tokens after '=' as the type expression
            let mut type_tokens = Vec::new();
            while let Some(next_token) = tokens.peek() {
                if let proc_macro2::TokenTree::Punct(p) = next_token
                    && p.as_char() == '='
                {
                    break;
                }
                if let Some(token) = tokens.next() {
                    type_tokens.push(token);
                } else {
                    break;
                }
            }

            if type_tokens.is_empty() {
                let span = proc_macro2::Ident::new("response", proc_macro2::Span::call_site());
                return Err(syn::Error::new_spanned(
                    &span,
                    "response type must not be empty",
                ));
            }

            // Parse the collected type tokens as a type
            let type_stream = proc_macro2::TokenStream::from_iter(type_tokens);
            return syn::parse2(type_stream).map_err(|_| {
                let span = proc_macro2::Ident::new("type", proc_macro2::Span::call_site());
                syn::Error::new_spanned(&span, "invalid type in response")
            });
        }
    }

    Err(syn::Error::new_spanned(
        name.clone(),
        r#"#[catga_request(response = TypeName)] required"#,
    ))
}
