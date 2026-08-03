//! Derive support for Catga's bounded MemoryPack wire format.
//!
//! This proc-macro crate is the implementation behind
//! `catga_core::codec::memorypack::MemoryPackable`. It generates static
//! `catga_core::codec::memorypack::MemoryPackSerialize` and
//! `catga_core::codec::memorypack::MemoryPackDeserialize` implementations without
//! reflection or runtime registration. Supported forms are ordinary structs,
//! C-like `#[repr(i32)]` enums, tagged unions, transparent `i32` wrappers, and
//! the documented zero-copy forms. Circular and version-tolerant layouts are
//! rejected because Catga's receive limits require bounded decoding.

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, parse_macro_input};

mod attributes;
mod enums;
mod helpers;
mod regular;
mod unions;

use attributes::AttributeFlags;
use enums::{
    generate_enum_deserialize_safe, generate_enum_serialize, generate_flags_impls,
    generate_transparent_deserialize, generate_transparent_serialize,
};
use helpers::is_single_field_i32;
use regular::{generate_deserialize, generate_serialize};
use unions::{generate_union_deserialize, generate_union_serialize, resolve_union_tags};

/// Derives Catga's static MemoryPack serialization traits for a type.
///
/// The generated implementation writes and consumes exactly one value using
/// `catga_codec_memorypack` readers and writers. Apply `#[repr(i32)]` to a
/// C-like enum unless it assigns explicit discriminants. The `memorypack`
/// and `tag` helper attributes select supported layout forms; unsupported
/// circular and version-tolerant layouts are rejected at compile time.
#[proc_macro_derive(MemoryPackable, attributes(memorypack, tag))]
pub fn derive_memorypack(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let attrs = AttributeFlags::parse(&input.attrs);

    if let Data::Struct(data_struct) = &input.data
        && data_struct.fields.len() > usize::from(u8::MAX)
    {
        return syn::Error::new_spanned(
            &input,
            "MemoryPack objects cannot contain more than 255 serialized fields",
        )
        .to_compile_error()
        .into();
    }

    let (serialize_impl, deserialize_impl) = match &input.data {
        Data::Struct(data_struct) if attrs.is_transparent && is_single_field_i32(data_struct) => (
            generate_transparent_serialize(),
            generate_transparent_deserialize(),
        ),
        Data::Struct(_) if attrs.is_circular || attrs.is_version_tolerant => {
            return syn::Error::new_spanned(
                &input,
                "Catga's bounded MemoryPack codec does not support circular or version_tolerant derives",
            )
            .to_compile_error()
            .into();
        }
        Data::Struct(_) => (
            generate_serialize(&input.data, attrs.is_zero_copy),
            generate_deserialize(&input.data, attrs.is_zero_copy),
        ),
        Data::Enum(data_enum) if attrs.is_union => {
            let tags = match resolve_union_tags(data_enum) {
                Ok(tags) => tags,
                Err(error) => return error.to_compile_error().into(),
            };
            (
                generate_union_serialize(data_enum, &tags),
                generate_union_deserialize(name, data_enum, &tags),
            )
        }
        Data::Enum(data_enum) => {
            if !attrs.has_repr_i32 {
                return syn::Error::new_spanned(
                    &input,
                    "C-like enums for MemoryPack must have either #[repr(i32)] or explicit discriminants"
                ).to_compile_error().into();
            }

            (
                generate_enum_serialize(data_enum),
                generate_enum_deserialize_safe(data_enum),
            )
        }
        Data::Union(_) => {
            return syn::Error::new_spanned(
                &input,
                "MemoryPackable cannot be derived for Rust unions",
            )
            .to_compile_error()
            .into();
        }
    };

    let flags_impl = if attrs.is_flags && attrs.is_transparent {
        generate_flags_impls(name)
    } else {
        quote! {}
    };

    let zero_copy_impl = if attrs.is_zero_copy {
        quote! {
            impl<'a> MemoryPackDeserializeZeroCopy<'a> for #name<'a> {
                #[inline]
                fn deserialize(reader: &mut MemoryPackReader<'a>) -> Result<Self, MemoryPackError> {
                    #deserialize_impl
                }
            }
        }
    } else {
        quote! {}
    };

    let deserialize_regular_impl = if attrs.is_zero_copy {
        quote! {}
    } else {
        quote! {
            impl #impl_generics MemoryPackDeserialize for #name #ty_generics #where_clause {
                #[inline]
                fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
                    #deserialize_impl
                }
            }
        }
    };

    let expanded = quote! {
        impl #impl_generics MemoryPackSerialize for #name #ty_generics #where_clause {
            #[inline]
            fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
                #serialize_impl
                Ok(())
            }
        }

        #deserialize_regular_impl

        #zero_copy_impl

        #flags_impl
    };

    expanded.into()
}
