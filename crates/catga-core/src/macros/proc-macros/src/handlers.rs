use std::collections::HashSet;

use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Expr, Ident, Path, Result, Token, bracketed,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
};

enum Registration {
    Request {
        message: Path,
        handler: Expr,
    },
    Command {
        message: Path,
        handler: Expr,
    },
    Event {
        message: Path,
        handlers: Punctuated<Expr, Token![,]>,
    },
}

impl Parse for Registration {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let kind: Ident = input.parse()?;
        let message: Path = input.parse()?;
        input.parse::<Token![=>]>()?;
        match kind.to_string().as_str() {
            "request" => Ok(Self::Request {
                message,
                handler: input.parse()?,
            }),
            "command" => Ok(Self::Command {
                message,
                handler: input.parse()?,
            }),
            "event" => {
                let content;
                bracketed!(content in input);
                Ok(Self::Event {
                    message,
                    handlers: content.parse_terminated(Expr::parse, Token![,])?,
                })
            }
            _ => Err(syn::Error::new(
                kind.span(),
                "expected `request`, `command`, or `event`",
            )),
        }
    }
}

struct Registrations(Punctuated<Registration, Token![;]>);

impl Parse for Registrations {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        Ok(Self(Punctuated::parse_terminated(input)?))
    }
}

pub(crate) fn expand(input: TokenStream) -> Result<TokenStream> {
    let Registrations(registrations) = syn::parse2(input)?;
    let mut request_messages = HashSet::with_capacity(registrations.len());
    let mut command_messages = HashSet::with_capacity(registrations.len());
    for registration in &registrations {
        match registration {
            Registration::Request { message, .. } => {
                if !request_messages.insert(quote!(#message).to_string()) {
                    return Err(syn::Error::new_spanned(
                        message,
                        "duplicate request handler registration",
                    ));
                }
            }
            Registration::Command { message, .. } => {
                if !command_messages.insert(quote!(#message).to_string()) {
                    return Err(syn::Error::new_spanned(
                        message,
                        "duplicate command handler registration",
                    ));
                }
            }
            Registration::Event { handlers, .. } if handlers.is_empty() => {
                return Err(syn::Error::new_spanned(
                    handlers,
                    "event registration requires at least one handler",
                ));
            }
            Registration::Event { .. } => {}
        };
    }
    let entries = registrations.iter().map(|registration| match registration {
        Registration::Request { message, handler } => quote! {
            registry.register_request::<#message, _>(#handler)?;
        },
        Registration::Command { message, handler } => quote! {
            registry.register_command::<#message, _>(#handler)?;
        },
        Registration::Event { message, handlers } => {
            let registrations = handlers.iter().map(|handler| {
                quote! {
                    registry.register_event::<#message, _>(#handler);
                }
            });
            quote! { #(#registrations)* }
        }
    });
    Ok(quote! {{
        (|| -> ::catga_core::CatgaResult<::catga_core::Registry> {
            let mut registry = ::catga_core::Registry::new();
            #(#entries)*
            Ok(registry)
        })()
    }})
}
