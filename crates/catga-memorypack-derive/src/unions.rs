use quote::quote;
use syn::{Expr, Fields, Lit, Meta};

pub fn resolve_union_tags(data_enum: &syn::DataEnum) -> syn::Result<Vec<u8>> {
    let mut tags = Vec::with_capacity(data_enum.variants.len());

    for (ordinal, variant) in data_enum.variants.iter().enumerate() {
        match &variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {}
            _ => {
                return Err(syn::Error::new_spanned(
                    variant,
                    "Union variants must have exactly one unnamed field",
                ));
            }
        }

        let mut explicit_tag = None;
        for attr in variant
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("tag"))
        {
            if explicit_tag.is_some() {
                return Err(syn::Error::new_spanned(
                    attr,
                    "union variants may specify only one tag",
                ));
            }

            let Meta::NameValue(tag) = &attr.meta else {
                return Err(syn::Error::new_spanned(
                    attr,
                    "union tags must use #[tag = N]",
                ));
            };
            let Expr::Lit(tag) = &tag.value else {
                return Err(syn::Error::new_spanned(
                    &tag.value,
                    "union tags must be integer literals",
                ));
            };
            let Lit::Int(tag_literal) = &tag.lit else {
                return Err(syn::Error::new_spanned(
                    &tag.lit,
                    "union tags must be integer literals",
                ));
            };
            let tag = tag_literal.base10_parse::<u16>().map_err(|_| {
                syn::Error::new_spanned(
                    tag_literal,
                    "union tags must be integer literals in 0..=255",
                )
            })?;
            if tag > u8::MAX as u16 {
                return Err(syn::Error::new_spanned(
                    tag_literal,
                    "union tags must be in 0..=255",
                ));
            }
            explicit_tag = Some(tag as u8);
        }

        let tag = match explicit_tag {
            Some(tag) => tag,
            None => u8::try_from(ordinal).map_err(|_| {
                syn::Error::new_spanned(
                    variant,
                    "union variants without an explicit tag must have an ordinal in 0..=255",
                )
            })?,
        };
        if tags.contains(&tag) {
            return Err(syn::Error::new_spanned(
                variant,
                format!("duplicate union tag {tag}"),
            ));
        }
        tags.push(tag);
    }

    Ok(tags)
}

pub fn generate_union_serialize(
    data_enum: &syn::DataEnum,
    tags: &[u8],
) -> proc_macro2::TokenStream {
    let variants = data_enum.variants.iter().zip(tags).map(|(variant, tag)| {
        let variant_name = &variant.ident;

        quote! {
            Self::#variant_name(inner) => {
                writer.write_u8(#tag)?;
                MemoryPackSerialize::serialize(inner, writer)?;
            }
        }
    });

    quote! {
        match self {
            #(#variants)*
        }
    }
}

pub fn generate_union_deserialize(
    name: &syn::Ident,
    data_enum: &syn::DataEnum,
    tags: &[u8],
) -> proc_macro2::TokenStream {
    let variants = data_enum.variants.iter().zip(tags).map(|(variant, tag)| {
        let variant_name = &variant.ident;

        quote! {
            #tag => {
                let inner = MemoryPackDeserialize::deserialize(reader)?;
                Ok(Self::#variant_name(inner))
            }
        }
    });

    quote! {
        let tag = reader.read_u8()?;
        match tag {
            #(#variants)*
            _ => Err(MemoryPackError::DeserializationError(
                format!("Unknown union tag {} for {}", tag, stringify!(#name))
            ))
        }
    }
}
