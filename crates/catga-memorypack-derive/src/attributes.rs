//! Parsing and representation of MemoryPack derive macro attributes.
//!
//! This module handles:
//! - `#[repr(...)]` attributes for transparent wrappers and C-like enums
//! - `#[memorypack(...)]` attributes for MemoryPack-specific options
//!
//! # Supported attributes
//!
//! ## `#[repr(...)]`
//! - `transparent` - Marks a single-field struct as a transparent wrapper
//! - `i32` - Marks an enum as a C-like enum with i32 discriminants
//!
//! ## `#[memorypack(...)]`
//! - `flags` - Marks an enum as a flags enum with bitwise operations
//! - `union` - Marks an enum as a tagged union
//! - `version_tolerant` - Enables version-tolerant deserialization (not supported in Catga)
//! - `circular` - Enables circular reference support (not supported in Catga)
//! - `zero_copy` - Enables zero-copy deserialization for string/byte fields

/// Flags parsed from a type's attributes, determining how MemoryPack code is generated.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AttributeFlags {
    /// True if the struct is a transparent wrapper (single i32 field).
    pub is_transparent: bool,
    /// True if the enum is a flags enum with bitwise operations.
    pub is_flags: bool,
    /// True if the enum is a tagged union.
    pub is_union: bool,
    /// True if version-tolerant deserialization is enabled (not supported in Catga).
    pub is_version_tolerant: bool,
    /// True if circular reference support is enabled (not supported in Catga).
    pub is_circular: bool,
    /// True if zero-copy deserialization is enabled.
    pub is_zero_copy: bool,
    /// True if the type has `#[repr(i32)]`.
    pub has_repr_i32: bool,
}

impl AttributeFlags {
    /// Parses all relevant attributes from a type definition.
    ///
    /// This function examines both `#[repr(...)]` and `#[memorypack(...)]` attributes
    /// to determine how the MemoryPack derive should generate code.
    ///
    /// # Arguments
    ///
    /// * `attrs` - The attributes from a `syn::DeriveInput`
    ///
    /// # Example
    ///
    /// ```
    /// // To use AttributeFlags::parse, you need to parse a struct with attributes:
    /// //
    /// // #[memorypack(zero_copy)]
    /// // struct MyStruct { name: String }
    /// //
    /// // After parsing:
    /// // let flags = AttributeFlags::parse(&input.attrs);
    /// // assert!(flags.is_zero_copy);  // true
    /// ```
    pub fn parse(attrs: &[syn::Attribute]) -> Self {
        let mut result = Self {
            is_transparent: false,
            is_flags: false,
            is_union: false,
            is_version_tolerant: false,
            is_circular: false,
            is_zero_copy: false,
            has_repr_i32: false,
        };

        for attr in attrs {
            Self::parse_repr_attr(attr, &mut result);
            Self::parse_memorypack_attr(attr, &mut result);
        }

        result
    }

    /// Parses `#[repr(...)]` attributes for `transparent` and `i32` flags.
    fn parse_repr_attr(attr: &syn::Attribute, result: &mut Self) {
        if !attr.path().is_ident("repr") {
            return;
        }

        let Ok(list) = attr.meta.require_list() else {
            return;
        };

        // Parse the repr list more carefully using meta tokens
        let _tokens = &list.tokens;

        // Handle #[repr(C)], #[repr(transparent)], #[repr(i32)], etc.
        if let syn::Meta::List(meta_list) = &attr.meta {
            for token in meta_list.tokens.clone() {
                if let proc_macro2::TokenTree::Ident(ident) = token {
                    match ident.to_string().as_str() {
                        "transparent" => result.is_transparent = true,
                        "i32" => result.has_repr_i32 = true,
                        _ => {}
                    }
                }
            }
        }
    }

    /// Parses `#[memorypack(...)]` attributes for MemoryPack-specific options.
    fn parse_memorypack_attr(attr: &syn::Attribute, result: &mut Self) {
        if !attr.path().is_ident("memorypack") {
            return;
        }

        let Ok(list) = attr.meta.require_list() else {
            return;
        };

        // Parse memorypack meta tokens
        let _tokens = &list.tokens;

        if let syn::Meta::List(meta_list) = &attr.meta {
            Self::parse_memorypack_tokens(&meta_list.tokens, result);
        }
    }

    /// Parses the inner tokens of a `#[memorypack(...)]` attribute.
    ///
    /// Handles the following forms:
    /// - `#[memorypack(flags)]`
    /// - `#[memorypack(union)]`
    /// - `#[memorypack(version_tolerant)]`
    /// - `#[memorypack(zero_copy)]`
    /// - `#[memorypack(flags, zero_copy)]`
    fn parse_memorypack_tokens(tokens: &proc_macro2::TokenStream, result: &mut Self) {
        let mut tokens_iter = tokens.clone().into_iter().peekable();

        while let Some(token) = tokens_iter.next() {
            match token {
                proc_macro2::TokenTree::Ident(ident) => match ident.to_string().as_str() {
                    "flags" => result.is_flags = true,
                    "union" => result.is_union = true,
                    "version_tolerant" => result.is_version_tolerant = true,
                    "circular" => result.is_circular = true,
                    "zero_copy" => result.is_zero_copy = true,
                    _ => {}
                },
                proc_macro2::TokenTree::Punct(punct) if punct.as_char() != ',' => {
                    // If it's not a comma, skip any following tokens that might be values
                    while let Some(next) = tokens_iter.peek() {
                        match next {
                            proc_macro2::TokenTree::Punct(p) if p.as_char() == ',' => break,
                            _ => {
                                tokens_iter.next();
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

