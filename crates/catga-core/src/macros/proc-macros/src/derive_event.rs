//! Derive macro for implementing `catga_core::Message` and `catga_core::Event`.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{GenericParam, Generics, Ident, parse_quote};

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

/// Adds the bounds required by `Message` and `Event` to every type parameter.
///
/// Only the parameter identifier is interpolated: interpolating a [`syn::TypeParam`] that
/// already carries bounds would emit `T: Bound: Clone + ...` and panic `parse_quote!`.
fn add_event_bounds(generics: &Generics) -> Generics {
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
        // Events require Clone (from the `Event` trait bound) plus `Send + Sync + 'static`
        // from `Message`.
        where_clause
            .predicates
            .push(parse_quote!(#ident: Clone + Send + Sync + 'static));
    }
    g
}
