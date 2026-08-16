use proc_macro2::TokenStream;
use quote::quote;
use syn::{ItemImpl, Result};

/// Checks if the last segment of a path matches the given ident.
fn last_segment_is(path: &syn::Path, ident: &str) -> bool {
    path.segments.last().is_some_and(|seg| seg.ident == ident)
}

/// Extracts the trait being implemented from an impl block, returning the
/// trait path and the implementing type.
fn extract_trait_info(impl_item: &ItemImpl) -> Option<(&syn::Path, &syn::Type)> {
    let trait_ = impl_item.trait_.as_ref()?;
    // trait_ is (Option<syn::token::Not>, syn::Path, For) in syn 2.x
    let (_, trait_path, _) = trait_;
    Some((trait_path, &impl_item.self_ty))
}

/// Returns the first type argument from a path, if present.
fn first_type_arg(path: &syn::Path) -> Option<&syn::Type> {
    path.segments.last().and_then(|seg| {
        if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
            args.args.first().and_then(|arg| {
                if let syn::GenericArgument::Type(ty) = arg {
                    Some(ty)
                } else {
                    None
                }
            })
        } else {
            None
        }
    })
}

/// Expands `#[catga_handler]` on an impl block.
///
/// The impl must implement `Handler<M>`, `CommandHandler<M>`, or `EventHandler<M>` with an
/// explicit message type `M`; after validation the impl block is re-emitted unchanged.
pub fn expand_handler(impl_item: ItemImpl) -> Result<TokenStream> {
    let trait_path = match extract_trait_info(&impl_item) {
        Some((path, _)) if last_segment_is(path, "Handler") => path,
        Some((path, _)) if last_segment_is(path, "CommandHandler") => path,
        Some((path, _)) if last_segment_is(path, "EventHandler") => path,
        Some((path, ty)) => {
            return Err(syn::Error::new_spanned(
                path,
                format!(
                    "`#[catga_handler]` only supports `Handler`, `CommandHandler`, or `EventHandler`, \
                     not `{}` (impl for `{}`)",
                    quote::quote!(#path),
                    quote::quote!(#ty),
                ),
            ));
        }
        None => {
            return Err(syn::Error::new_spanned(
                &impl_item.self_ty,
                "`#[catga_handler]` requires a trait impl block (`impl Handler<M> for T` etc.)",
            ));
        }
    };

    if first_type_arg(trait_path).is_none() {
        return Err(syn::Error::new_spanned(
            trait_path,
            "`#[catga_handler]` requires a typed trait impl (`impl Handler<M>`)",
        ));
    }

    Ok(quote! { #impl_item })
}
