//! Code generation for MemoryPack derive on tagged union enums.
//!
//! Tagged unions serialize as a tag byte followed by the variant's data.
//! Each variant must have exactly one unnamed field (the payload).
//!
//! # Format
//!
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | Tag | u8 | Identifies which variant is present |
//! | Data | varies | The serialized payload of the active variant |
//!
//! # Example
//!
//! ```ignore
//! #[memorypack(union)]
//! enum Shape {
//!     #[tag = 1]
//!     Circle(f64),      // radius
//!     #[tag = 2]
//!     Rectangle(f64, f64), // width, height
//! }
//! ```
//!
//! Serializes as: `[1, <radius bytes>]` or `[2, <width bytes>, <height bytes>]`

use quote::quote;
use syn::{Expr, Fields, Lit, Meta};

/// Resolves the tag byte for each variant in a union enum.
///
/// Tags can be specified explicitly via `#[tag = N]` or auto-assigned
/// sequentially starting from 0. This function validates that:
///
/// - Each variant has exactly one unnamed field
/// - All tag values are within 0..=255
/// - No duplicate tags exist
///
/// # Arguments
///
/// * `data_enum` - The enum data to resolve tags for
///
/// # Returns
///
/// A `Vec<u8>` containing the tag for each variant in declaration order
///
/// # Errors
///
/// Returns `syn::Error` if:
/// - A variant has no fields or multiple fields
/// - A tag value exceeds 255
/// - Duplicate tags are detected
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

/// Generates MemoryPack serialize code for a tagged union enum.
///
/// Serializes each variant as a tag byte followed by the serialized payload.
/// The tag identifies which variant is active; the payload is serialized
/// using the standard MemoryPack serialization for the inner type.
///
/// # Arguments
///
/// * `data_enum` - The enum data to generate serialize code for
/// * `tags` - The tag value for each variant in declaration order
///
/// # Example output
///
/// ```ignore
/// match self {
///     MyUnion::Variant1(inner) => {
///         writer.write_u8(0)?;
///         MemoryPackSerialize::serialize(inner, writer)?;
///     }
///     MyUnion::Variant2(inner) => {
///         writer.write_u8(1)?;
///         MemoryPackSerialize::serialize(inner, writer)?;
///     }
/// }
/// ```
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

/// Generates MemoryPack deserialize code for a tagged union enum.
///
/// Reads the tag byte, then deserializes the payload into the appropriate
/// variant. Returns an error if the tag does not match any known variant.
///
/// # Arguments
///
/// * `name` - The enum type name (for error messages)
/// * `data_enum` - The enum data to generate deserialize code for
/// * `tags` - The tag value for each variant in declaration order
///
/// # Example output
///
/// ```ignore
/// let tag = reader.read_u8()?;
/// match tag {
///     0 => {
///         let inner = MemoryPackDeserialize::deserialize(reader)?;
///         Ok(Self::Variant1(inner))
///     }
///     1 => {
///         let inner = MemoryPackDeserialize::deserialize(reader)?;
///         Ok(Self::Variant2(inner))
///     }
///     _ => Err(MemoryPackError::DeserializationError(
///         format!("Unknown union tag {} for MyUnion", tag)
///     ))
/// }
/// ```
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_union_tags_auto_assigns_sequential() {
        // Union variants must have exactly one unnamed field
        let input: syn::DeriveInput =
            syn::parse_str("enum Color { Red(i32), Green(i32), Blue(i32) }").unwrap();
        let syn::Data::Enum(data) = input.data else {
            panic!("expected enum")
        };
        let tags = resolve_union_tags(&data).unwrap();
        assert_eq!(tags, vec![0, 1, 2]);
    }

    #[test]
    fn resolve_union_tags_respects_explicit_tags() {
        let input: syn::DeriveInput =
            syn::parse_str("enum Status { Active(i32), #[tag = 20] Inactive(i32), }").unwrap();
        let syn::Data::Enum(data) = input.data else {
            panic!("expected enum")
        };
        let tags = resolve_union_tags(&data).unwrap();
        assert_eq!(tags, vec![0, 20]);
    }

    #[test]
    fn resolve_union_tags_rejects_mixed_fields() {
        let input: syn::DeriveInput =
            syn::parse_str("enum Bad { Variant1(i32), Variant2 { x: i32 }, }").unwrap();
        let syn::Data::Enum(data) = input.data else {
            panic!("expected enum")
        };
        let result = resolve_union_tags(&data);
        assert!(result.is_err());
    }

    #[test]
    fn resolve_union_tags_rejects_duplicate_tags() {
        let input: syn::DeriveInput =
            syn::parse_str("enum Duplicate { #[tag = 1] First(i32), #[tag = 1] Second(i32), }")
                .unwrap();
        let syn::Data::Enum(data) = input.data else {
            panic!("expected enum")
        };
        let result = resolve_union_tags(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("duplicate"));
    }

    #[test]
    fn generate_union_serialize_produces_match_arms() {
        let input: syn::DeriveInput =
            syn::parse_str("enum Shape { Circle(f64), Square(f64), }").unwrap();
        let syn::Data::Enum(data) = input.data else {
            panic!("expected enum")
        };
        let tags = vec![0u8, 1];
        let tokens = generate_union_serialize(&data, &tags);
        let output = tokens.to_string();

        // Should have match with write_u8 and the inner field access
        assert!(output.contains("match"), "should contain match: {}", output);
        assert!(output.contains("write_u8"), "should contain write_u8: {}", output);
        assert!(output.contains("Circle") && output.contains("Square"));
    }

    #[test]
    fn generate_union_deserialize_produces_match_on_tag() {
        let name = syn::Ident::new("Shape", proc_macro2::Span::call_site());
        let input: syn::DeriveInput =
            syn::parse_str("enum Shape { Circle(f64), Square(f64), }").unwrap();
        let syn::Data::Enum(data) = input.data else {
            panic!("expected enum")
        };
        let tags = vec![0u8, 1];
        let tokens = generate_union_deserialize(&name, &data, &tags);
        let output = tokens.to_string();

        // Should read tag byte and match on it
        assert!(output.contains("read_u8"), "should contain read_u8: {}", output);
        assert!(output.contains("match"), "should contain match: {}", output);
        assert!(output.contains("Unknown union tag"));
    }
}
