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
use unions::{generate_union_deserialize, generate_union_serialize};

#[proc_macro_derive(MemoryPackable, attributes(memorypack, tag))]
pub fn derive_memorypack(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let attrs = AttributeFlags::parse(&input.attrs);

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
            generate_serialize(&input.data),
            generate_deserialize(&input.data, attrs.is_zero_copy),
        ),
        Data::Enum(data_enum) if attrs.is_union => (
            generate_union_serialize(data_enum),
            generate_union_deserialize(name, data_enum),
        ),
        Data::Enum(data_enum) => {
            if !attrs.has_repr_i32 {
                return syn::Error::new_spanned(
                    &input,
                    "C-like enums for MemoryPack must have either #[repr(i32)] or explicit discriminants"
                ).to_compile_error().into();
            }

            (
                generate_enum_serialize(),
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
            impl<'a> catga_codec_memorypack::MemoryPackDeserializeZeroCopy<'a> for #name<'a> {
                #[inline]
                fn deserialize(reader: &mut catga_codec_memorypack::MemoryPackReader<'a>) -> Result<Self, catga_codec_memorypack::MemoryPackError> {
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
            impl #impl_generics catga_codec_memorypack::MemoryPackDeserialize for #name #ty_generics #where_clause {
                #[inline]
                fn deserialize(reader: &mut catga_codec_memorypack::MemoryPackReader) -> Result<Self, catga_codec_memorypack::MemoryPackError> {
                    #deserialize_impl
                }
            }
        }
    };

    let expanded = quote! {
        impl #impl_generics catga_codec_memorypack::MemoryPackSerialize for #name #ty_generics #where_clause {
            #[inline]
            fn serialize(&self, writer: &mut catga_codec_memorypack::MemoryPackWriter) -> Result<(), catga_codec_memorypack::MemoryPackError> {
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
