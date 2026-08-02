//! Derive macro for implementing `catga_core::Message` and `catga_core::Command`.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_quote, GenericParam, Generics};

/// Implements `catga_core::Message` and `catga_core::Command`.
pub fn expand_derive_command(input: TokenStream) -> TokenStream {
    match derive_command_impl(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.into_compile_error().into(),
    }
}

fn derive_command_impl(input: TokenStream2) -> Result<TokenStream2, syn::Error> {
    let input = syn::parse2::<syn::DeriveInput>(input)?;
    let name = &input.ident;
    let generics = add_clone_bound(&input.generics);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics ::catga_core::Message for #name #ty_generics #where_clause {}
        impl #impl_generics ::catga_core::Command for #name #ty_generics #where_clause {}
    })
}

fn add_clone_bound(generics: &Generics) -> Generics {
    let mut g = generics.clone();
    let params: Vec<_> = g.params.iter().cloned().collect();
    let where_clause = g.make_where_clause();
    for param in params {
        if let GenericParam::Type(type_param) = param {
            where_clause.predicates.push(parse_quote!(#type_param: Clone));
        }
    }
    g
}
