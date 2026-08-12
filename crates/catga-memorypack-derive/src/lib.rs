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
//!
//! # Using the derive
//!
//! Applications never depend on this crate directly; the derive is re-exported as
//! `catga_core::codec::memorypack::MemoryPackable` (and at the `catga_core` root). The
//! generated code names the codec traits **unqualified**, so the deriving module must bring them
//! into scope together with the reader, writer, and error types:
//!
//! ```ignore
//! // Ignored: the expansion references `catga_core` codec traits, and this proc-macro
//! // crate deliberately has no `catga-core` dependency to link a doctest against.
//! // A runnable version of this example lives in `catga-core`'s codec documentation.
//! use catga_core::MemoryPackable;
//! use catga_core::codec::memorypack::{
//!     MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
//!     MemoryPackWriter,
//! };
//!
//! #[derive(Clone, Debug, PartialEq, MemoryPackable)]
//! struct OrderState {
//!     items: u32,
//!     total: f64,
//!     paid: bool,
//! }
//! ```
//!
//! See [`derive@MemoryPackable`] for the full input contract, generated items, and limits.

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
/// The derive is re-exported as `catga_core::codec::memorypack::MemoryPackable`; depending on
/// this proc-macro crate directly is not the supported path.
///
/// # Scope requirement
///
/// The expansion names `MemoryPackSerialize`, `MemoryPackDeserialize`, `MemoryPackReader`,
/// `MemoryPackWriter`, and `MemoryPackError` **unqualified** (zero-copy expansions also name
/// `MemoryPackDeserializeZeroCopy`). Import them from `catga_core::codec::memorypack` in the
/// deriving module, or compilation fails with unresolved-name errors.
///
/// # Supported input forms and generated items
///
/// - **Named, tuple, and unit structs** — gain `MemoryPackSerialize` and
///   `MemoryPackDeserialize` impls. The wire frame is a `u8` field count followed by the field
///   values in declaration order; decoding rejects a frame whose field count differs. Unit
///   structs encode as zero bytes beyond the frame written by their container.
/// - **Transparent `i32` newtypes** (`#[repr(transparent)]` around a single `i32` field) —
///   serialize as the bare `i32` with no field-count frame.
/// - **C-like enums** — require `#[repr(i32)]` or an explicit discriminant on every variant;
///   each variant serializes as its `i32` discriminant and decoding rejects unknown
///   discriminants. Data-carrying variants are only supported through the tagged-union form
///   below.
/// - **Tagged unions** (`#[memorypack(union)]`) — each variant carries exactly one unnamed
///   payload field. The frame is a `u8` tag (explicit `#[tag = N]` or the declaration ordinal)
///   followed by the payload; decoding rejects unknown tags.
/// - **Zero-copy structs** (`#[memorypack(zero_copy)]`) — the struct's only generic parameter
///   must be the borrowed lifetime `'a`, and `&'a str` / `&'a [u8]` fields then deserialize by
///   borrowing from the reader. The expansion provides `MemoryPackSerialize` and
///   `MemoryPackDeserializeZeroCopy<'a>` instead of the owned `MemoryPackDeserialize`.
/// - **Flags newtypes** (`#[repr(transparent)] #[memorypack(flags)]` around one `i32`) —
///   additionally generate inherent `contains`/`is_empty` and the `BitOr`/`BitAnd`/`BitXor`/
///   `Not` operator impls.
///
/// # Field attributes
///
/// - `#[memorypack(skip)]` / `#[memorypack(ignore)]` — excluded from the frame; decoded with
///   `Default::default()`. Fields whose name starts with `_` are skipped by convention.
/// - `#[memorypack(order = N)]` — serializes the field at position `N` instead of its
///   declaration position.
/// - `#[memorypack(zero_copy)]` on a field — borrows that `&str` / `&[u8]` field even when the
///   containing struct is not wholly zero-copy.
///
/// # Limits
///
/// A struct may declare at most 255 serialized fields and union tags must be unique values in
/// `0..=255`; both bounds come from the single-byte frame prefixes. Skipped fields do not count
/// toward the field limit.
///
/// # Compile-time rejections
///
/// Layouts that would defeat Catga's bounded decoding are refused during macro expansion:
///
/// ```compile_fail
/// // Circular references admit unbounded graphs, so they are rejected.
/// use catga_memorypack_derive::MemoryPackable;
///
/// #[derive(MemoryPackable)]
/// #[memorypack(circular)]
/// struct Node { next: Option<Box<Node>> }
/// ```
///
/// ```compile_fail
/// // Version-tolerant frames allow trailing unknown fields, so they are rejected.
/// use catga_memorypack_derive::MemoryPackable;
///
/// #[derive(MemoryPackable)]
/// #[memorypack(version_tolerant)]
/// struct Account { balance: i64 }
/// ```
///
/// ```compile_fail
/// // Rust unions have no defined active variant to serialize.
/// use catga_memorypack_derive::MemoryPackable;
///
/// #[derive(MemoryPackable)]
/// union Value { int: i32, float: f32 }
/// ```
///
/// ```compile_fail
/// // C-like enums must opt into stable wire values with #[repr(i32)] or explicit
/// // discriminants on every variant.
/// use catga_memorypack_derive::MemoryPackable;
///
/// #[derive(MemoryPackable)]
/// enum Status { Open, Closed }
/// ```
///
/// ```compile_fail
/// // Union tags are single bytes: 0..=255, unique across variants.
/// use catga_memorypack_derive::MemoryPackable;
///
/// #[derive(MemoryPackable)]
/// #[memorypack(union)]
/// enum Shape {
///     #[tag = 0]
///     Point(f64),
///     #[tag = 0]
///     Circle(f64),
/// }
/// ```
///
/// ```compile_fail
/// // Every union variant carries exactly one unnamed payload field.
/// use catga_memorypack_derive::MemoryPackable;
///
/// #[derive(MemoryPackable)]
/// #[memorypack(union)]
/// enum Message { Text(String), Pair(i32, i32) }
/// ```
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
            let all_variants_have_explicit_discriminants =
                data_enum.variants.iter().all(|v| v.discriminant.is_some());
            if !attrs.has_repr_i32 && !all_variants_have_explicit_discriminants {
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
