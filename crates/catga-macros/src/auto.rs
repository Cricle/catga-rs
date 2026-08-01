use proc_macro2::TokenStream;
use quote::quote;
use syn::{ItemImpl, Result};

/// Checks if the last segment of a path matches the given ident.
fn last_segment_is(path: &syn::Path, ident: &str) -> bool {
    path.segments.last().map_or(false, |seg| seg.ident == ident)
}

/// Extracts the trait being implemented from an impl block, returning the
/// trait path and the implementing type.
fn extract_trait_info(impl_item: &ItemImpl) -> Option<(&syn::Path, &syn::Type)> {
    let trait_ = impl_item.trait_.as_ref()?;
    let (trait_path, _) = trait_;
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

/// Expands `#[catga_handler]` on an impl block into registration code.
#[allow(dead_code)]
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

    let _message_type = match first_type_arg(trait_path) {
        Some(ty) => ty,
        None => {
            return Err(syn::Error::new_spanned(
                trait_path,
                "`#[catga_handler]` requires a typed trait impl (`impl Handler<M>`)",
            ));
        }
    };

    let struct_name = &impl_item.self_ty;
    let handler_ident = quote::format_ident!("__CatgaHandler_{}", quote::quote!(#struct_name).to_string().replace(['<', '>', ' ', ',', ':', '&', '(' , ')', '[', ']'], "_"));

    Ok(quote! {
        {
            struct #handler_ident;
            impl #impl_item
            #handler_ident
        }
        ::catga_core::CatgaResult::<()>
    })
}

pub fn expand_auto(input: TokenStream) -> proc_macro::TokenStream {
    match expand_auto_impl(input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.into_compile_error().into(),
    }
}

fn expand_auto_impl(input: TokenStream) -> Result<TokenStream> {
    let module: syn::ItemMod = syn::parse2(input)?;
    let module_ident = &module.ident;
    let module_vis = &module.vis;

    let mut registrations = Vec::new();
    let mut module_items = Vec::new();

    if let Some((_, items)) = &module.content {
        for item in items {
            match item {
                syn::Item::Impl(impl_item) => {
                    // Auto-discover Handler, CommandHandler, and EventHandler impl blocks
                    let trait_path = match extract_trait_info(impl_item) {
                        Some((path, _)) if last_segment_is(path, "Handler") => path,
                        Some((path, _)) if last_segment_is(path, "CommandHandler") => path,
                        Some((path, _)) if last_segment_is(path, "EventHandler") => path,
                        _ => {
                            module_items.push(item.clone());
                            continue;
                        }
                    };

                    let message_type = match first_type_arg(trait_path) {
                        Some(ty) => ty,
                        None => {
                            return Err(syn::Error::new_spanned(
                                trait_path,
                                "`impl Handler<M>` requires a typed trait impl (`impl Handler<M>`)",
                            ));
                        }
                    };

                    let struct_name = &impl_item.self_ty;

                    // Generate registration based on trait type
                    let registration = match last_segment_is(trait_path, "Handler") {
                        true => quote! {
                            __catga_auto_registry.register_request::<#message_type, _>(#struct_name {})?;
                        },
                        false if last_segment_is(trait_path, "CommandHandler") => quote! {
                            __catga_auto_registry.register_command::<#message_type, _>(#struct_name {})?;
                        },
                        false if last_segment_is(trait_path, "EventHandler") => quote! {
                            __catga_auto_registry.register_event::<#message_type, _>(#struct_name {});
                        },
                        _ => unreachable!(),
                    };

                    registrations.push(registration);
                    module_items.push(item.clone());
                }
                syn::Item::Fn(fn_item) => {
                    // Check if this is an async fn that can satisfy Handler via blanket impl
                    if fn_item.sig.asyncness.is_some() {
                        let fn_name = &fn_item.sig.ident;

                        // Determine the message type from function arguments
                        // For async fn(Message) -> Result, message is at index 0
                        // For async fn(&self, Message) -> Result, message is at index 1
                        let message_type = if let Some(first_arg) = fn_item.sig.inputs.iter().next() {
                            match first_arg {
                                syn::FnArg::Typed(pat_type) => {
                                    // Check if first arg is `self` (Pat::Ident with ident "self")
                                    if matches!(&*pat_type.pat, syn::Pat::Ident(p) if p.ident == "self") {
                                        // Self is first, message is second
                                        fn_item.sig.inputs.iter().nth(1)
                                    } else {
                                        // First arg is the message
                                        Some(first_arg)
                                    }
                                }
                                _ => None,
                            }
                        } else {
                            None
                        };

                        if let Some(message_arg) = message_type {
                            let message_type = match message_arg {
                                syn::FnArg::Typed(pat_type) => &pat_type.ty,
                                _ => {
                                    module_items.push(item.clone());
                                    continue;
                                }
                            };

                            // All async fn handlers are treated as request handlers for now
                            let registration = quote! {
                                __catga_auto_registry.register_request::<#message_type, _>(#fn_name)?;
                            };
                            registrations.push(registration);
                        }
                    }
                    module_items.push(item.clone());
                }
                _ => {
                    module_items.push(item.clone());
                }
            }
        }
    }

    if registrations.is_empty() {
        return Err(syn::Error::new(
            module_ident.span(),
            "#[catga_auto] module contains no handlers",
        ));
    }

    // Generate the registration function
    let all_registrations = registrations;
    let all_module_items = module_items;

    Ok(quote! {
        #module_vis mod #module_ident {
            use super::*;

            #(#all_module_items)*

            pub fn __catga_auto_register(
                mut __catga_auto_registry: ::catga_core::Registry,
            ) -> ::catga_core::CatgaResult<::catga_core::Registry> {
                #(#all_registrations)*
                Ok(__catga_auto_registry)
            }
        }
    })
}
