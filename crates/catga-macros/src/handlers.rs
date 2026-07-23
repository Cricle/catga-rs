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
    let entries = registrations.iter().map(|registration| match registration {
        Registration::Request { message, handler } => quote! {
            registry.register_request::<#message, _>(#handler)
                .expect("Catga request handler registration must be unique");
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
        let mut registry = ::catga_core::Registry::new();
        #(#entries)*
        registry
    }})
}
