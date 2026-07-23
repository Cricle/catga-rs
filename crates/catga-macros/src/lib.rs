#![forbid(unsafe_code)]
//! Procedural macros for Catga.

use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, parse_macro_input};

mod handlers;

/// Implements `catga_core::Message` with the item's short Rust type name.
#[proc_macro_derive(Message)]
pub fn derive_message(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    quote! {
        impl ::catga_core::Message for #name {
            fn message_type(&self) -> &'static str {
                stringify!(#name)
            }
        }
    }
    .into()
}

/// Builds an explicit `Registry` from typed request and event handlers.
#[proc_macro]
pub fn catga_handlers(input: TokenStream) -> TokenStream {
    handlers::expand(input.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
