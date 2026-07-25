use std::collections::HashSet;

use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Ident, Path, Result, Token, bracketed,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
};

enum Registration {
    Request {
        message: Path,
        handler: Path,
    },
    Event {
        message: Path,
        handlers: Punctuated<Path, Token![,]>,
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
            "event" => {
                let content;
                bracketed!(content in input);
                Ok(Self::Event {
                    message,
                    handlers: content.parse_terminated(Path::parse, Token![,])?,
                })
            }
            _ => Err(syn::Error::new(
                kind.span(),
                "expected `request` or `event`",
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

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::expand;

    #[test]
    fn duplicate_request_registration_is_rejected_during_macro_expansion() {
        let error = expand(quote! {
            request CreateOrder => CreateOrderHandler;
            request CreateOrder => ReplayOrderHandler;
        })
        .expect_err("duplicate request registrations must not defer to a startup panic");

        assert!(
            error
                .to_string()
                .contains("duplicate request handler registration")
        );
    }

    #[test]
    fn empty_event_registration_is_rejected_during_macro_expansion() {
        let error = expand(quote! {
            event OrderCreated => [];
        })
        .expect_err("an event with no handlers would silently discard delivery");

        assert!(
            error
                .to_string()
                .contains("event registration requires at least one handler")
        );
    }
}
