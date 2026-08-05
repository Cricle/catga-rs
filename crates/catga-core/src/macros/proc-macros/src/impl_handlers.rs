use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Ident, Type};

pub fn expand_impl_handlers(input: TokenStream) -> TokenStream {
    match syn::parse2::<syn::ItemImpl>(input) {
        Ok(impl_item) => expand_impl(impl_item),
        Err(e) => e.into_compile_error(),
    }
}

struct MethodAnalysis {
    method_name: Ident,
    message_type: Type,
    is_request: bool,
    is_event: bool,
}

fn expand_impl(impl_item: syn::ItemImpl) -> TokenStream {
    let ty = &impl_item.self_ty;

    let mut methods = Vec::new();

    for item in &impl_item.items {
        if let syn::ImplItem::Fn(method) = item
            && method.sig.asyncness.is_some()
            && let Some(analysis) = analyze_method(method)
        {
            methods.push(analysis);
        }
    }

    // Generate registry function with fixed name "registry"
    let registry_fn_name = format_ident!("registry");

    // Generate wrapper structs and trait impls
    let mut wrapper_structs = Vec::new();
    let mut wrapper_impls = Vec::new();
    let mut registry_calls = Vec::new();

    for (idx, m) in methods.iter().enumerate() {
        let message_type = &m.message_type;
        let wrapper_name = format_ident!("__CatgaServiceHandler_{}", idx);
        let method_name = &m.method_name;

        if m.is_request {
            wrapper_structs.push(quote! {
                struct #wrapper_name;
            });

            wrapper_impls.push(quote! {
                #[::async_trait::async_trait]
                impl catga_core::Handler<#message_type> for #wrapper_name {
                    async fn handle(&self, msg: #message_type) -> catga_core::CatgaResult<<#message_type as catga_core::Request>::Response> {
                        let svc = #ty;
                        svc.#method_name(msg).await
                    }
                }
            });

            registry_calls.push(quote! {
                registry.register_request::<#message_type, #wrapper_name>(#wrapper_name)?;
            });
        } else if m.is_event {
            wrapper_structs.push(quote! {
                struct #wrapper_name;
            });

            wrapper_impls.push(quote! {
                #[::async_trait::async_trait]
                impl catga_core::EventHandler<#message_type> for #wrapper_name {
                    async fn handle(&self, event: #message_type) -> catga_core::CatgaResult<()> {
                        let svc = #ty;
                        svc.#method_name(event).await
                    }
                }
            });

            registry_calls.push(quote! {
                registry.register_event::<#message_type, #wrapper_name>(#wrapper_name);
            });
        } else {
            wrapper_structs.push(quote! {
                struct #wrapper_name;
            });

            wrapper_impls.push(quote! {
                #[::async_trait::async_trait]
                impl catga_core::CommandHandler<#message_type> for #wrapper_name {
                    async fn handle(&self, cmd: #message_type) -> catga_core::CatgaResult<()> {
                        let svc = #ty;
                        svc.#method_name(cmd).await
                    }
                }
            });

            registry_calls.push(quote! {
                registry.register_command::<#message_type, #wrapper_name>(#wrapper_name)?;
            });
        }
    }

    let expanded = quote! {
        #impl_item

        #(#wrapper_structs)*
        #(#wrapper_impls)*

        impl #ty {
            pub fn #registry_fn_name() -> catga_core::CatgaResult<catga_core::Registry> {
                let mut registry = catga_core::Registry::new();
                #(#registry_calls)*
                Ok(registry)
            }
        }
    };

    expanded
}

fn analyze_method(method: &syn::ImplItemFn) -> Option<MethodAnalysis> {
    let method_name = method.sig.ident.clone();
    let method_name_str = method_name.to_string();

    // Events: methods starting with "on_" returning CatgaResult<()>
    let is_event = method_name_str.starts_with("on_");

    // Get first param after self
    let mut inputs = method.sig.inputs.iter();
    inputs.next()?; // skip self
    let first_arg = inputs.next()?;
    let message_type = extract_type_from_fn_arg(first_arg)?;

    // Parse return type
    let ret = &method.sig.output;
    let is_request = match ret {
        syn::ReturnType::Type(_, ty) => {
            // Check if return type is CatgaResult<T>
            if let Type::Path(type_path) = ty.as_ref()
                && let Some(segment) = type_path.path.segments.last()
                && (segment.ident == "Result" || segment.ident == "CatgaResult")
                && let syn::PathArguments::AngleBracketed(args) = &segment.arguments
                && let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first()
            {
                // CatgaResult<T> where T is NOT () = request
                // CatgaResult<()> = command (or event if method starts with "on_")
                if !is_unit_type(inner_ty) {
                    return Some(MethodAnalysis {
                        method_name,
                        message_type,
                        is_request: true,
                        is_event: false,
                    });
                } else {
                    // It's CatgaResult<()>, so it's a command or event
                    return Some(MethodAnalysis {
                        method_name,
                        message_type,
                        is_request: false,
                        is_event,
                    });
                }
            }
            // Not a CatgaResult, check if it's unit
            !is_unit_type(ty)
        }
        syn::ReturnType::Default => false,
    };

    Some(MethodAnalysis {
        method_name,
        message_type,
        is_request,
        is_event,
    })
}

fn extract_type_from_fn_arg(arg: &syn::FnArg) -> Option<Type> {
    match arg {
        syn::FnArg::Typed(pat_type) => Some((*pat_type.ty).clone()),
        _ => None,
    }
}

fn is_unit_type(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(t) if t.elems.is_empty())
}
