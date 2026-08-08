//! Helper utilities for MemoryPack derive macro code generation.
//!
//! This module provides utility functions used during serialization and deserialization
//! code generation, including field ordering, skipping, and type introspection.

use syn::Field;

/// Returns true if the field should be skipped during serialization/deserialization.
///
/// A field is skipped if it has:
///
/// - `#[memorypack(skip)]` or `#[memorypack(ignore)]` attribute
/// - An identifier starting with `_` (convention for unused fields)
///
/// # Arguments
///
/// * `field` - The struct field to check
///
/// # Example
///
/// ```
/// // Example of what should_skip_field checks for:
/// // - #[memorypack(skip)] or #[memorypack(ignore)] attributes
/// // - Fields with identifiers starting with underscore (e.g., _internal)
/// ```
#[inline]
pub fn should_skip_field(field: &Field) -> bool {
    field.attrs.iter().any(|attr| {
        attr.path().is_ident("memorypack")
            && attr
                .meta
                .require_list()
                .map(|m| {
                    let tokens = m.tokens.to_string();
                    tokens.contains("skip") || tokens.contains("ignore")
                })
                .unwrap_or(false)
    }) || field
        .ident
        .as_ref()
        .map(|ident| ident.to_string().starts_with('_'))
        .unwrap_or(false)
}

/// Extracts the field order from `#[memorypack(order = N)]` attribute.
///
/// Returns `None` if the field has no explicit order, otherwise returns
/// the specified order index.
///
/// # Arguments
///
/// * `field` - The struct field to check
///
/// # Example
///
/// ```
/// // To use get_field_order, you need to parse a struct field:
/// //
/// // #[memorypack(order = 2)]
/// // third_field: String
/// //
/// // The function returns Some(2) for the above field,
/// // or None if no order attribute is specified.
/// ```
pub fn get_field_order(field: &Field) -> Option<usize> {
    field.attrs.iter().find_map(|attr| {
        if !attr.path().is_ident("memorypack") {
            return None;
        }

        let list = attr.meta.require_list().ok()?;
        let tokens = list.tokens.to_string();
        let order_pos = tokens.find("order")?;
        let after_order = &tokens[order_pos..];
        let eq_pos = after_order.find('=')?;
        let after_eq = after_order[eq_pos + 1..].trim();

        let num_str = after_eq
            .find(|c: char| !c.is_ascii_digit())
            .map(|end| &after_eq[..end])
            .unwrap_or(after_eq);

        num_str.parse::<usize>().ok()
    })
}

/// Returns true if the field has `#[memorypack(zero_copy)]` attribute.
///
/// Zero-copy fields deserialize by borrowing from the input buffer instead
/// of allocating owned data.
///
/// # Arguments
///
/// * `field` - The struct field to check
#[inline]
pub fn is_zero_copy_field(field: &Field) -> bool {
    field.attrs.iter().any(|attr| {
        attr.path().is_ident("memorypack")
            && attr
                .meta
                .require_list()
                .map(|m| m.tokens.to_string().contains("zero_copy"))
                .unwrap_or(false)
    })
}

/// Returns true if the struct has exactly one field of type `i32`.
///
/// This is used to determine if a struct should be treated as a transparent `i32`
/// wrapper for MemoryPack serialization.
///
/// # Arguments
///
/// * `data_struct` - The struct data to check
pub fn is_single_field_i32(data_struct: &syn::DataStruct) -> bool {
    match &data_struct.fields {
        syn::Fields::Named(fields) => {
            fields.named.len() == 1
                && fields
                    .named
                    .first()
                    .map(|f| matches!(&f.ty, syn::Type::Path(p) if p.path.is_ident("i32")))
                    .unwrap_or(false)
        }
        syn::Fields::Unnamed(fields) => {
            fields.unnamed.len() == 1
                && fields
                    .unnamed
                    .first()
                    .map(|f| matches!(&f.ty, syn::Type::Path(p) if p.path.is_ident("i32")))
                    .unwrap_or(false)
        }
        syn::Fields::Unit => false,
    }
}

/// Returns true if the type is `&str` (borrowed string).
///
/// This is used to determine if a field should use zero-copy string deserialization.
///
/// # Arguments
///
/// * `ty` - The type to check
#[inline]
pub fn is_borrowed_str(ty: &syn::Type) -> bool {
    if let syn::Type::Reference(type_ref) = ty
        && let syn::Type::Path(inner_path) = &*type_ref.elem
    {
        return inner_path.path.is_ident("str");
    }
    false
}

/// Returns true if the type is `&[u8]` (borrowed byte slice).
///
/// This is used to determine if a field should use zero-copy byte slice deserialization.
///
/// # Arguments
///
/// * `ty` - The type to check
#[inline]
pub fn is_borrowed_u8_slice(ty: &syn::Type) -> bool {
    let syn::Type::Reference(type_ref) = ty else {
        return false;
    };
    let syn::Type::Slice(slice) = &*type_ref.elem else {
        return false;
    };
    matches!(&*slice.elem, syn::Type::Path(type_path) if type_path.path.is_ident("u8"))
}

/// Represents a field with its serialization order determined by `#[memorypack(order)]`.
pub struct OrderedField<'a> {
    /// The order index for serialization (lower values come first).
    pub order: usize,
    /// The field definition.
    pub field: &'a Field,
    /// The field identifier, if named.
    pub ident: &'a Option<syn::Ident>,
}

/// Sorts fields by their explicit order or index position.
///
/// Fields without `#[memorypack(order = N)]` retain their declaration order.
/// Fields with explicit order are sorted by that order value.
///
/// # Arguments
///
/// * `fields` - Slice of field references to sort
///
/// # Returns
///
/// A vector of `OrderedField` sorted by order value
pub fn prepare_ordered_fields<'a>(fields: &'a [&'a Field]) -> Vec<OrderedField<'a>> {
    let mut ordered: Vec<_> = fields
        .iter()
        .enumerate()
        .map(|(idx, f)| OrderedField {
            order: get_field_order(f).unwrap_or(idx),
            field: f,
            ident: &f.ident,
        })
        .collect();
    ordered.sort_by_key(|f| f.order);
    ordered
}

/// Generates deserialization code for a single field.
///
/// This function handles the various cases:
///
/// - Skipped fields get `Default::default()`
/// - Zero-copy fields borrow from the reader
/// - Regular fields use standard deserialization
///
/// # Arguments
///
/// * `field` - The field to generate deserialization for
/// * `is_zero_copy_struct` - Whether the containing struct has `#[memorypack(zero_copy)]`
pub fn generate_field_deserialize(
    field: &Field,
    is_zero_copy_struct: bool,
) -> proc_macro2::TokenStream {
    use quote::quote;

    let name = &field.ident;
    let ty = &field.ty;

    if should_skip_field(field) {
        return quote! {
            let #name: #ty = Default::default();
        };
    }

    let is_field_zero_copy = is_zero_copy_field(field);

    if is_zero_copy_struct || is_field_zero_copy {
        if is_borrowed_str(ty) {
            return quote! { let #name = reader.read_str()?; };
        }
        if is_borrowed_u8_slice(ty) {
            return quote! {
                let #name: #ty = {
                    let length = reader.read_i32()?;
                    match length {
                        -1 | 0 => &[],
                        value if value < 0 => {
                            return Err(MemoryPackError::DeserializationError(
                                "invalid zero-copy byte slice length".into(),
                            ));
                        }
                        value => reader.read_bytes(value as usize)?,
                    }
                };
            };
        }
        if is_field_zero_copy {
            return quote! { let #name = MemoryPackDeserializeZeroCopy::deserialize(reader)?; };
        }
    }

    quote! { let #name = MemoryPackDeserialize::deserialize(reader)?; }
}

