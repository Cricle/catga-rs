//! Derive macro for implementing `catga_core::Message` and `catga_core::Event`.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_quote, GenericParam, Generics};

/// Implements `catga_core::Message` and `catga_core::Event`.
/// Event requires Clone, so this derive enforces that bound.
pub fn expand_derive_event(input: TokenStream) -> TokenStream {
    match derive_event_impl(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.into_compile_error().into(),
    }
}

fn derive_event_impl(input: TokenStream2) -> Result<TokenStream2, syn::Error> {
    let input = syn::parse2::<syn::DeriveInput>(input)?;
    let name = &input.ident;
    let generics = add_event_bounds(&input.generics);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics ::catga_core::Message for #name #ty_generics #where_clause {}
        impl #impl_generics ::catga_core::Event for #name #ty_generics #where_clause {}
    })
}

fn add_event_bounds(generics: &Generics) -> Generics {
    let mut g = generics.clone();
    let params: Vec<_> = g.params.iter().cloned().collect();
    let where_clause = g.make_where_clause();
    for param in params {
        if let GenericParam::Type(type_param) = param {
            // Events require Clone (from Event trait bound)
            // Also add Send + Sync for Message trait compatibility
            where_clause.predicates.push(parse_quote!(#type_param: Clone + Send + Sync + 'static));
        }
    }
    g
}
