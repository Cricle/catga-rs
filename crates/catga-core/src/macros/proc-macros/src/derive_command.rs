//! Derive macro for implementing `catga_core::Message` and `catga_core::Command`.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{GenericParam, Generics, Ident, parse_quote};

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
    let vis = &input.vis;
    let generics = add_message_bounds(&input.generics);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    // Generate unique TypeId struct name
    let type_id_name = syn::Ident::new(&format!("{name}TypeId"), name.span());

    Ok(quote! {
        #vis struct #type_id_name;
        impl ::catga_core::MessageTypeId for #type_id_name {
            const NAME: &'static str = ::core::stringify!(#name);
        }

        impl #impl_generics ::catga_core::Message for #name #ty_generics #where_clause {}
        impl #impl_generics ::catga_core::Command for #name #ty_generics #where_clause {
            type TypeId = #type_id_name;
        }
    })
}

/// Adds the bounds required by `Message` and `Command` to every type parameter.
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
