//! Code generation for MemoryPack derive on enums.
//!
//! This module handles the different enum forms supported by the MemoryPack derive:
//!
//! - **C-like enums**: `#[repr(i32)]` enums with integer discriminants
//! - **Flags enums**: Enums with `#[memorypack(flags)]` for bitwise operations
//! - **Transparent wrappers**: Single-field `#[repr(transparent)]` enums with i32 variants

use quote::quote;

/// Generates serialize code for a C-like enum with `#[repr(i32)]`.
///
/// Each variant serializes as its discriminant value as an i32.
///
/// # Example output
///
/// ```ignore
/// match self {
///     MyEnum::Variant1 => writer.write_i32(self as i32)?,
///     MyEnum::Variant2 => writer.write_i32(self as i32)?,
/// }
/// ```
pub fn generate_enum_serialize(data_enum: &syn::DataEnum) -> proc_macro2::TokenStream {
    let variants = data_enum.variants.iter().map(|variant| {
        let variant_name = &variant.ident;
        quote! {
            Self::#variant_name => writer.write_i32(Self::#variant_name as i32)?,
        }
    });

    quote! {
        match self {
            #(#variants)*
        }
    }
}

/// Generates safe deserialize code for a C-like enum.
///
/// This function validates the incoming discriminant before constructing the enum,
/// returning a `DeserializationError` for unknown values.
///
/// # Example output
///
/// ```ignore
/// let value = reader.read_i32()?;
/// match value {
///     0 => Ok(Self::Variant1),
///     1 => Ok(Self::Variant2),
///     _ => Err(MemoryPackError::DeserializationError(...))
/// }
/// ```
pub fn generate_enum_deserialize_safe(data_enum: &syn::DataEnum) -> proc_macro2::TokenStream {
    let variants = data_enum.variants.iter().map(|variant| {
        let variant_name = &variant.ident;

        quote! {
            value if value == Self::#variant_name as i32 => Ok(Self::#variant_name),
        }
    });

    quote! {
        let value = reader.read_i32()?;
        match value {
            #(#variants)*
            _ => Err(catga_core::codec::memorypack::MemoryPackError::DeserializationError(
                format!("Invalid discriminant {} for enum {}", value, stringify!(Self))
            ))
        }
    }
}

/// Generates serialize code for a transparent i32 wrapper enum.
///
/// Transparent enums serialize their inner i32 value directly.
///
/// # Example
///
/// For `#[repr(transparent)] enum MyEnum(i32)`, generates:
///
/// ```ignore
/// writer.write_i32(self.0)?;
/// ```
#[inline]
pub fn generate_transparent_serialize() -> proc_macro2::TokenStream {
    quote! {
        writer.write_i32(self.0)?;
    }
}

/// Generates deserialize code for a transparent i32 wrapper enum.
///
/// Transparent enums deserialize an i32 and wrap it in the enum constructor.
///
/// # Example
///
/// For `#[repr(transparent)] enum MyEnum(i32)`, generates:
///
/// ```ignore
/// Ok(Self(reader.read_i32()?))
/// ```
#[inline]
pub fn generate_transparent_deserialize() -> proc_macro2::TokenStream {
    quote! {
        Ok(Self(reader.read_i32()?))
    }
}

/// Generates bitwise operation implementations for flags enums.
///
/// When an enum has both `#[memorypack(flags)]` and `#[repr(transparent)]`,
/// this generates implementations for:
///
/// - `contains(other)` - checks if all bits of `other` are set
/// - `is_empty()` - checks if no bits are set
/// - `BitOr` - union of two flag sets
/// - `BitAnd` - intersection of two flag sets
/// - `BitXor` - symmetric difference of two flag sets
/// - `Not` - bitwise complement
///
/// # Example
///
/// ```ignore
/// impl MyFlags {
///     pub const fn contains(self, other: Self) -> bool {
///         (self.0 & other.0) == other.0
///     }
///
///     pub const fn is_empty(self) -> bool {
///         self.0 == 0
///     }
/// }
///
/// impl std::ops::BitOr for MyFlags { ... }
/// impl std::ops::BitAnd for MyFlags { ... }
/// impl std::ops::BitXor for MyFlags { ... }
/// impl std::ops::Not for MyFlags { ... }
/// ```
pub fn generate_flags_impls(name: &syn::Ident) -> proc_macro2::TokenStream {
    quote! {
        impl #name {
            /// Returns true if all bits in `other` are set in `self`.
            #[inline]
            pub const fn contains(self, other: #name) -> bool {
                (self.0 & other.0) == other.0
            }

            /// Returns true if no bits are set.
            #[inline]
            pub const fn is_empty(self) -> bool {
                self.0 == 0
            }
        }

        impl std::ops::BitOr for #name {
            type Output = Self;
            #[inline]
            fn bitor(self, rhs: Self) -> Self {
                Self(self.0 | rhs.0)
            }
        }

        impl std::ops::BitAnd for #name {
            type Output = Self;
            #[inline]
            fn bitand(self, rhs: Self) -> Self {
                Self(self.0 & rhs.0)
            }
        }

        impl std::ops::BitXor for #name {
            type Output = Self;
            #[inline]
            fn bitxor(self, rhs: Self) -> Self {
                Self(self.0 ^ rhs.0)
            }
        }

        impl std::ops::Not for Self {
            type Output = Self;
            #[inline]
            fn not(self) -> Self {
                Self(!self.0)
            }
        }
    }
}

