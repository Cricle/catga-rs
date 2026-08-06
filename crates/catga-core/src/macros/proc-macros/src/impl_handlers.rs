use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use std::fs;
use std::path::PathBuf;

pub fn expand_impl_handlers(
    input: TokenStream,
    typed_mediator_name: Option<syn::Ident>,
) -> TokenStream {
    let impl_item: syn::ItemImpl = match syn::parse2(input) {
        Ok(item) => item,
        Err(e) => return e.into_compile_error(),
    };

    let ty = &impl_item.self_ty;
    let impl_attrs = impl_item.attrs.clone();
    let impl_generics = impl_item.generics.clone();
    let impl_unsafety = impl_item.unsafety;
    let impl_defaultness = impl_item.defaultness;

    // Collect async methods with their analysis by index
    let method_infos: Vec<_> = impl_item
        .items
        .iter()
        .enumerate()
        .filter_map(|(idx, item)| {
            if let syn::ImplItem::Fn(method) = item
                && method.sig.asyncness.is_some()
            {
                analyze_method(method, idx).map(|analysis| (idx, analysis))
            } else {
                None
            }
        })
        .collect();

    // Get original method tokens by iterating items and collecting async ones
    let original_method_tokens: Vec<TokenStream> = impl_item
        .items
        .iter()
        .filter_map(|item| {
            if let syn::ImplItem::Fn(method) = item
                && method.sig.asyncness.is_some()
            {
                Some(quote::ToTokens::to_token_stream(method))
            } else {
                None
            }
        })
        .collect();

    // Generate output based on whether we're creating a typed mediator
    let (wrapper_structs, wrapper_impls, registry_calls) = if typed_mediator_name.is_some() {
        // Typed mediator path: wrapper structs are concrete, no generics needed
        let wrapper_structs: Vec<TokenStream> = method_infos
            .iter()
            .map(|(_, m)| {
                let wrapper_name = format_ident!("__CatgaServiceHandler_{}", m.index);
                let doc = format!(
                    " Handler wrapper for [`{}`] method `{}`",
                    quote!(#ty).to_string().trim(),
                    m.method_name
                );
                quote! {
                    #[doc = #doc]
                    #[derive(::std::clone::Clone)]
                    struct #wrapper_name {
                        service: #ty,
                    }
                }
            })
            .collect();

        let wrapper_impls: Vec<TokenStream> = method_infos
            .iter()
            .map(|(_, m)| {
                let wrapper_name = format_ident!("__CatgaServiceHandler_{}", m.index);
                let method_name = &m.method_name;
                let message_type = &m.message_type;
                if m.is_request {
                    let doc = format!(" Implements [`Handler`] for `{}` — dispatches [`{}`]", wrapper_name, quote!(#message_type));
                    quote! {
                        #[doc = #doc]
                        #[::async_trait::async_trait]
                        impl catga_core::Handler<#message_type> for #wrapper_name {
                            async fn handle(&self, msg: #message_type) -> catga_core::CatgaResult<<#message_type as catga_core::Request>::Response> {
                                self.service.#method_name(msg).await
                            }
                        }
                    }
                } else if m.is_event {
                    let doc = format!(" Implements [`EventHandler`] for `{}` — dispatches [`{}`]", wrapper_name, quote!(#message_type));
                    quote! {
                        #[doc = #doc]
                        #[::async_trait::async_trait]
                        impl catga_core::EventHandler<#message_type> for #wrapper_name {
                            async fn handle(&self, event: #message_type) -> catga_core::CatgaResult<()> {
                                self.service.#method_name(event).await
                            }
                        }
                    }
                } else {
                    let doc = format!(" Implements [`CommandHandler`] for `{}` — dispatches [`{}`]", wrapper_name, quote!(#message_type));
                    quote! {
                        #[doc = #doc]
                        #[::async_trait::async_trait]
                        impl catga_core::CommandHandler<#message_type> for #wrapper_name {
                            async fn handle(&self, cmd: #message_type) -> catga_core::CatgaResult<()> {
                                self.service.#method_name(cmd).await
                            }
                        }
                    }
                }
            })
            .collect();

        let registry_calls: Vec<TokenStream> = method_infos
            .iter()
            .map(|(_, m)| {
                let wrapper_name = format_ident!("__CatgaServiceHandler_{}", m.index);
                let message_type = &m.message_type;
                // Clone for each handler to avoid move issues
                if m.is_request {
                    quote! {
                        registry.register_request::<#message_type, #wrapper_name>(#wrapper_name { service: self.clone() })?;
                    }
                } else if m.is_event {
                    quote! {
                        registry.register_event::<#message_type, #wrapper_name>(#wrapper_name { service: self.clone() });
                    }
                } else {
                    quote! {
                        registry.register_command::<#message_type, #wrapper_name>(#wrapper_name { service: self.clone() })?;
                    }
                }
            })
            .collect();

        (wrapper_structs, wrapper_impls, registry_calls)
    } else {
        // Non-typed path: all handlers use Arc-wrapped service
        let wrapper_structs: Vec<TokenStream> = method_infos
            .iter()
            .map(|(_, m)| {
                let wrapper_name = format_ident!("__CatgaServiceHandler_{}", m.index);
                let doc = format!(
                    " Handler wrapper for [`{}`] method `{}`",
                    quote!(#ty).to_string().trim(),
                    m.method_name
                );
                quote! {
                    #[doc = #doc]
                    #[derive(::std::clone::Clone)]
                    struct #wrapper_name {
                        service: ::std::sync::Arc<#ty>,
                    }
                }
            })
            .collect();

        let wrapper_impls: Vec<TokenStream> = method_infos
            .iter()
            .map(|(_, m)| {
                let wrapper_name = format_ident!("__CatgaServiceHandler_{}", m.index);
                let method_name = &m.method_name;
                let message_type = &m.message_type;
                if m.is_request {
                    let doc = format!(" Implements [`Handler`] for `{}` — dispatches [`{}`]", wrapper_name, quote!(#message_type));
                    quote! {
                        #[doc = #doc]
                        #[::async_trait::async_trait]
                        impl catga_core::Handler<#message_type> for #wrapper_name {
                            async fn handle(&self, msg: #message_type) -> catga_core::CatgaResult<<#message_type as catga_core::Request>::Response> {
                                self.service.#method_name(msg).await
                            }
                        }
                    }
                } else if m.is_event {
                    let doc = format!(" Implements [`EventHandler`] for `{}` — dispatches [`{}`]", wrapper_name, quote!(#message_type));
                    quote! {
                        #[doc = #doc]
                        #[::async_trait::async_trait]
                        impl catga_core::EventHandler<#message_type> for #wrapper_name {
                            async fn handle(&self, event: #message_type) -> catga_core::CatgaResult<()> {
                                self.service.#method_name(event).await
                            }
                        }
                    }
                } else {
                    let doc = format!(" Implements [`CommandHandler`] for `{}` — dispatches [`{}`]", wrapper_name, quote!(#message_type));
                    quote! {
                        #[doc = #doc]
                        #[::async_trait::async_trait]
                        impl catga_core::CommandHandler<#message_type> for #wrapper_name {
                            async fn handle(&self, cmd: #message_type) -> catga_core::CatgaResult<()> {
                                self.service.#method_name(cmd).await
                            }
                        }
                    }
                }
            })
            .collect();

        let registry_calls: Vec<TokenStream> = method_infos
            .iter()
            .map(|(_, m)| {
                let wrapper_name = format_ident!("__CatgaServiceHandler_{}", m.index);
                let message_type = &m.message_type;
                if m.is_request {
                    quote! {
                        registry.register_request::<#message_type, #wrapper_name>(#wrapper_name { service: ::std::sync::Arc::new(self.clone()) })?;
                    }
                } else if m.is_event {
                    quote! {
                        registry.register_event::<#message_type, #wrapper_name>(#wrapper_name { service: ::std::sync::Arc::new(self.clone()) });
                    }
                } else {
                    quote! {
                        registry.register_command::<#message_type, #wrapper_name>(#wrapper_name { service: ::std::sync::Arc::new(self.clone()) })?;
                    }
                }
            })
            .collect();

        (wrapper_structs, wrapper_impls, registry_calls)
    };

    let base_output = quote! {
        #(#impl_attrs)*
        #impl_defaultness
        #impl_unsafety
        impl #impl_generics #ty {
            #(#original_method_tokens)*

            /// Builds a [`Registry`] containing all handlers from this service.
            pub fn registry(self) -> catga_core::CatgaResult<catga_core::Registry> {
                let mut registry = catga_core::Registry::new();
                #(#registry_calls)*
                Ok(registry)
            }
        }

        #(#wrapper_structs)*
        #(#wrapper_impls)*
    };

    let output = if let Some(mediator_name) = typed_mediator_name {
        let request_dispatch_impls: Vec<TokenStream> = method_infos
            .iter()
            .filter(|(_, m)| m.is_request)
            .map(|(_, m)| {
                let message_type = &m.message_type;
                let method_name = &m.method_name;
                let doc = format!(
                    " Sealed dispatch impl for [`{}`] request → {}",
                    quote!(#message_type),
                    m.method_name
                );
                quote! {
                    #[doc = #doc]
                    impl ::catga_core::sealed_dispatch::SealedRequestDispatch<#message_type>
                        for #mediator_name
                    {
                        async fn __dispatch_request(
                            &self,
                            message: #message_type,
                        ) -> ::catga_core::CatgaResult<
                            <#message_type as ::catga_core::Request>::Response,
                        > {
                            self.service.#method_name(message).await
                        }
                    }
                }
            })
            .collect();

        let command_dispatch_impls: Vec<TokenStream> = method_infos
            .iter()
            .filter(|(_, m)| !m.is_request && !m.is_event)
            .map(|(_, m)| {
                let message_type = &m.message_type;
                let method_name = &m.method_name;
                let doc = format!(
                    " Sealed dispatch impl for [`{}`] command → {}",
                    quote!(#message_type),
                    m.method_name
                );
                quote! {
                    #[doc = #doc]
                    impl ::catga_core::sealed_dispatch::SealedCommandDispatch<#message_type>
                        for #mediator_name
                    {
                        async fn __dispatch_command(
                            &self,
                            command: #message_type,
                        ) -> ::catga_core::CatgaResult<()> {
                            self.service.#method_name(command).await
                        }
                    }
                }
            })
            .collect();

        let event_dispatch_impls: Vec<TokenStream> = method_infos
            .iter()
            .filter(|(_, m)| m.is_event)
            .map(|(_, m)| {
                let message_type = &m.message_type;
                let method_name = &m.method_name;
                let doc = format!(
                    " Sealed dispatch impl for [`{}`] event → {}",
                    quote!(#message_type),
                    m.method_name
                );
                quote! {
                    #[doc = #doc]
                    impl ::catga_core::sealed_dispatch::SealedEventDispatch<#message_type>
                        for #mediator_name
                    {
                        async fn __dispatch_event(
                            &self,
                            event: #message_type,
                        ) -> ::catga_core::CatgaResult<()> {
                            self.service.#method_name(event).await
                        }
                    }
                }
            })
            .collect();

        let mediator_doc = {
            let handlers: Vec<_> = method_infos
                .iter()
                .map(|(_, m)| {
                    let msg_type = &m.message_type;
                    format!(
                        "  - `{}` → `{}`",
                        m.method_name,
                        quote!(#msg_type).to_string().trim()
                    )
                })
                .collect();
            let handlers_list = handlers.join("\n");
            format!(
                " A compile-time monomorphized mediator for `{}`.\n\n\
                 ## Registered Handlers\n\n\
                 {}\n\n\
                 ## Example\n\n\
                 ```ignore\n\
                 let mediator = {}::new(service);\n\
                 let response = mediator.send(request).await?;\n\
                 ```",
                quote!(#ty).to_string().trim(),
                handlers_list,
                mediator_name
            )
        };

        quote! {
            #base_output

            #[doc = #mediator_doc]
            #[derive(::std::clone::Clone)]
            pub struct #mediator_name {
                service: #ty,
            }

            #[doc = " Typed mediator for compile-time dispatch."]
            impl #mediator_name {
                #[doc = " Creates a new typed mediator with the given service."]
                pub fn new(service: #ty) -> Self {
                    Self { service }
                }

                #[doc = " Dispatches a request with compile-time dispatch, zero heap allocation."]
                pub async fn send<__M: ::catga_core::Request>(
                    &self,
                    message: __M,
                ) -> ::catga_core::CatgaResult<__M::Response>
                where
                    Self: ::catga_core::sealed_dispatch::SealedRequestDispatch<__M>,
                {
                    ::catga_core::sealed_dispatch::SealedRequestDispatch::__dispatch_request(
                        self, message,
                    )
                    .await
                }

                #[doc = " Dispatches a command with compile-time dispatch, zero heap allocation."]
                pub async fn send_command<__C: ::catga_core::Command>(
                    &self,
                    command: __C,
                ) -> ::catga_core::CatgaResult<()>
                where
                    Self: ::catga_core::sealed_dispatch::SealedCommandDispatch<__C>,
                {
                    ::catga_core::sealed_dispatch::SealedCommandDispatch::__dispatch_command(
                        self, command,
                    )
                    .await
                }

                #[doc = " Publishes an event with compile-time dispatch, zero heap allocation."]
                pub async fn publish<__E: ::catga_core::Event>(
                    &self,
                    event: __E,
                ) -> ::catga_core::CatgaResult<()>
                where
                    Self: ::catga_core::sealed_dispatch::SealedEventDispatch<__E>,
                {
                    ::catga_core::sealed_dispatch::SealedEventDispatch::__dispatch_event(
                        self, event,
                    )
                    .await
                }
            }

            #(#request_dispatch_impls)*
            #(#command_dispatch_impls)*
            #(#event_dispatch_impls)*
        }
    } else {
        base_output
    };

    // Debug: write generated output to /tmp/catga_debug.rs
    let debug_path = PathBuf::from("/tmp/catga_debug.rs");
    fs::write(debug_path, output.to_string()).ok();

    output
}

struct MethodAnalysis {
    index: usize,
    method_name: syn::Ident,
    message_type: syn::Type,
    is_request: bool,
    is_event: bool,
    response_type_name: Option<String>, // NEW: for doc generation
}

fn analyze_method(method: &syn::ImplItemFn, index: usize) -> Option<MethodAnalysis> {
    let method_name = method.sig.ident.clone();
    let method_name_str = method_name.to_string();

    let is_event = method_name_str.starts_with("on_");

    let mut inputs = method.sig.inputs.iter();
    inputs.next()?;
    let first_arg = inputs.next()?;

    let message_type = match first_arg {
        syn::FnArg::Typed(pat_type) => (*pat_type.ty).clone(),
        _ => return None,
    };

    let ret = &method.sig.output;
    let is_request = match ret {
        syn::ReturnType::Type(_, ty) => {
            if let syn::Type::Path(type_path) = ty.as_ref()
                && let Some(segment) = type_path.path.segments.last()
                && (segment.ident == "Result" || segment.ident == "CatgaResult")
                && let syn::PathArguments::AngleBracketed(args) = &segment.arguments
                && let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first()
            {
                !is_unit_type(inner_ty)
            } else {
                false
            }
        }
        syn::ReturnType::Default => false,
    };

    // Extract response type name for requests
    let response_type_name = if is_request {
        extract_response_type_name(ret)
    } else {
        None
    };

    Some(MethodAnalysis {
        index,
        method_name,
        message_type,
        is_request,
        is_event,
        response_type_name,
    })
}

fn is_unit_type(ty: &syn::Type) -> bool {
    matches!(ty, syn::Type::Tuple(t) if t.elems.is_empty())
}

fn extract_response_type_name(output: &syn::ReturnType) -> Option<String> {
    if let syn::ReturnType::Type(_, ty) = output {
        if let syn::Type::Path(type_path) = ty.as_ref()
            && let Some(segment) = type_path.path.segments.last()
            && let syn::PathArguments::AngleBracketed(args) = &segment.arguments
            && let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first()
        {
            return Some(quote!(#inner_ty).to_string().trim().to_string());
        }
    }
    None
}
