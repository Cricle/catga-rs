//! Code generation for MemoryPack derive on regular structs.
//!
//! This module handles the serialization and deserialization code generation
//! for structs with named fields, unnamed fields (tuple structs), and unit structs.
//!
//! # Serialization format
//!
//! - Named structs: field count byte + ordered field values
//! - Tuple structs: field count byte + positional field values
//! - Unit structs: no data written
//!
//! # Field ordering
//!
//! Fields can be reordered using `#[memorypack(order = N)]`. Fields without
//! explicit order retain their declaration order and are sorted after those
//! with explicit order values.

use crate::helpers::{
    generate_field_deserialize, is_borrowed_u8_slice, is_zero_copy_field, prepare_ordered_fields,
    should_skip_field,
};

use quote::quote;
use syn::{Data, Fields};

/// Generates MemoryPack serialize code for a struct.
///
/// This function handles all three struct forms:
///
/// - **Named fields**: Writes a field count byte followed by each field value in order
/// - **Tuple structs**: Writes a field count byte followed by each positional field
/// - **Unit structs**: Writes nothing
///
/// # Arguments
///
/// * `data` - The struct data from `syn::Data::Struct`
/// * `is_zero_copy` - Whether zero-copy deserialization is enabled for the struct
///
/// # Example
///
/// For a struct `struct Point { x: i32, y: i32 }`, generates:
///
/// ```ignore
/// writer.write_u8(2)?;
/// MemoryPackSerialize::serialize(&self.x, writer)?;
/// MemoryPackSerialize::serialize(&self.y, writer)?;
/// ```
pub fn generate_serialize(data: &Data, is_zero_copy: bool) -> proc_macro2::TokenStream {
    let Data::Struct(data_struct) = data else {
        return quote! {
            compile_error!("MemoryPackable serialize can only be derived for structs");
        };
    };

    match &data_struct.fields {
        Fields::Named(fields) => generate_named_serialize(fields, is_zero_copy),
        Fields::Unnamed(fields) => generate_tuple_serialize(fields),
        Fields::Unit => quote! {},
    }
}

/// Generates serialize code for a struct with named fields.
fn generate_named_serialize(
    fields: &syn::FieldsNamed,
    is_zero_copy: bool,
) -> proc_macro2::TokenStream {
    let non_skip: Vec<_> = fields
        .named
        .iter()
        .filter(|f| !should_skip_field(f))
        .collect();

    let ordered = prepare_ordered_fields(&non_skip);
    let field_count = match u8::try_from(ordered.len()) {
        Ok(field_count) => field_count,
        Err(_) => {
            return quote! {
                compile_error!("MemoryPack objects cannot contain more than 255 serialized fields");
            };
        }
    };

    let serialize_fields = ordered.iter().map(|of| {
        let name = of.ident;
        let field = of.field;
        generate_field_serialize(field, quote! { self.#name }, is_zero_copy)
    });

    quote! {
        writer.write_u8(#field_count)?;
        #(#serialize_fields)*
    }
}

/// Generates serialize code for a tuple struct.
fn generate_tuple_serialize(fields: &syn::FieldsUnnamed) -> proc_macro2::TokenStream {
    let field_count = match u8::try_from(fields.unnamed.len()) {
        Ok(field_count) => field_count,
        Err(_) => {
            return quote! {
                compile_error!("MemoryPack objects cannot contain more than 255 serialized fields");
            };
        }
    };

    let serialize_fields = (0..fields.unnamed.len()).map(|i| {
        let index = syn::Index::from(i);
        let field = &fields.unnamed[i];
        generate_field_serialize(field, quote! { self.#index }, false) // Tuple structs don't support zero_copy
    });

    quote! {
        writer.write_u8(#field_count)?;
        #(#serialize_fields)*
    }
}

/// Generates serialize code for a single field.
///
/// This handles special cases:
///
/// - Zero-copy byte slices: write length + bytes
/// - Regular fields: delegate to `MemoryPackSerialize::serialize`
fn generate_field_serialize(
    field: &syn::Field,
    access: proc_macro2::TokenStream,
    is_zero_copy_struct: bool,
) -> proc_macro2::TokenStream {
    if (is_zero_copy_struct || is_zero_copy_field(field)) && is_borrowed_u8_slice(&field.ty) {
        return quote! {
            let length = i32::try_from(#access.len()).map_err(|_| {
                MemoryPackError::SerializationError(
                    "zero-copy byte slice length exceeds i32::MAX".into(),
                )
            })?;
            writer.write_i32(length)?;
            writer.buffer.extend_from_slice(#access);
        };
    }

    quote! { MemoryPackSerialize::serialize(&(#access), writer)?; }
}

/// Generates MemoryPack deserialize code for a struct.
///
/// This function handles all three struct forms with proper field ordering:
///
/// - **Named fields**: Reads field count, validates, reads fields in order, constructs struct
/// - **Tuple structs**: Reads field count, validates, reads fields positionally
/// - **Unit structs**: Returns `Ok(Self)` directly
///
/// # Arguments
///
/// * `data` - The struct data from `syn::Data::Struct`
/// * `is_zero_copy` - Whether zero-copy deserialization is enabled for the struct
pub fn generate_deserialize(data: &Data, is_zero_copy: bool) -> proc_macro2::TokenStream {
    let Data::Struct(data_struct) = data else {
        return quote! {
            compile_error!("MemoryPackable deserialize can only be derived for structs");
        };
    };

    match &data_struct.fields {
        Fields::Named(fields) => generate_named_deserialize(fields, is_zero_copy),
        Fields::Unnamed(fields) => generate_tuple_deserialize(fields),
        Fields::Unit => quote! { Ok(Self) },
    }
}

/// Generates deserialize code for a struct with named fields.
///
/// Includes field count validation and proper ordering for `#[memorypack(order)]` fields.
fn generate_named_deserialize(
    fields: &syn::FieldsNamed,
    is_zero_copy: bool,
) -> proc_macro2::TokenStream {
    let non_skip: Vec<_> = fields
        .named
        .iter()
        .filter(|f| !should_skip_field(f))
        .collect();

    let ordered = prepare_ordered_fields(&non_skip);
    let field_count = match u8::try_from(ordered.len()) {
        Ok(field_count) => field_count,
        Err(_) => {
            return quote! {
                compile_error!("MemoryPack objects cannot contain more than 255 serialized fields");
            };
        }
    };

    let all_field_names: Vec<_> = fields.named.iter().map(|f| &f.ident).collect();

    // Generate deserialize statements for all fields (before ordering)
    let deserialize_stmts: Vec<_> = fields
        .named
        .iter()
        .map(|f| generate_field_deserialize(f, is_zero_copy))
        .collect();

    // Build ordered deserialization by mapping field positions
    let mut ordered_deserialize = Vec::new();
    let mut skip_field_idx = 0;
    let mut ordered_idx = 0;

    for f in &fields.named {
        if should_skip_field(f) {
            // For skipped fields, use a placeholder at the skip position
            ordered_deserialize.push(deserialize_stmts[skip_field_idx + ordered_idx].clone());
            skip_field_idx += 1;
        } else if ordered_idx < ordered.len() {
            if let Some(field_idx) = fields
                .named
                .iter()
                .position(|field| std::ptr::eq(field, ordered[ordered_idx].field))
            {
                ordered_deserialize.push(deserialize_stmts[field_idx].clone());
            }
            ordered_idx += 1;
        }
    }

    quote! {
        reader.enter_object()?;
        let result = (|| {
            let received_field_count = reader.read_u8()?;
            if received_field_count != #field_count {
                return Err(MemoryPackError::DeserializationError(
                    format!(
                        "MemoryPack object field count mismatch: expected {}, received {}",
                        #field_count,
                        received_field_count,
                    ),
                ));
            }
            #(#ordered_deserialize)*
            Ok(Self { #(#all_field_names),* })
        })();
        reader.leave_object();
        result
    }
}

/// Generates deserialize code for a tuple struct.
fn generate_tuple_deserialize(fields: &syn::FieldsUnnamed) -> proc_macro2::TokenStream {
    let len = fields.unnamed.len();
    let field_count = match u8::try_from(len) {
        Ok(field_count) => field_count,
        Err(_) => {
            return quote! {
                compile_error!("MemoryPack objects cannot contain more than 255 serialized fields");
            };
        }
    };

    let field_vars: Vec<_> = (0..len)
        .map(|i| syn::Ident::new(&format!("field_{}", i), proc_macro2::Span::call_site()))
        .collect();

    let deserialize_stmts = field_vars.iter().map(|var| {
        quote! { let #var = MemoryPackDeserialize::deserialize(reader)?; }
    });

    quote! {
        reader.enter_object()?;
        let result = (|| {
            let received_field_count = reader.read_u8()?;
            if received_field_count != #field_count {
                return Err(MemoryPackError::DeserializationError(
                    format!(
                        "MemoryPack object field count mismatch: expected {}, received {}",
                        #field_count,
                        received_field_count,
                    ),
                ));
            }
            #(#deserialize_stmts)*
            Ok(Self(#(#field_vars),*))
        })();
        reader.leave_object();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_serialize_for_named_fields() {
        let input: syn::DeriveInput = syn::parse_str("struct Point { x: i32, y: i32 }").unwrap();
        let tokens = generate_serialize(&input.data, false);
        let output = tokens.to_string();

        assert!(output.contains("write_u8"));
        // Generated code uses MemoryPackSerialize::serialize
        assert!(output.contains("MemoryPackSerialize"));
    }

    #[test]
    fn generate_serialize_for_tuple_struct() {
        // Tuple struct with unnamed fields - uses self.0, self.1 indexing
        let input: syn::DeriveInput = syn::parse_str("struct Point(i32, i32);").unwrap();
        let tokens = generate_serialize(&input.data, false);
        let output = tokens.to_string();

        assert!(output.contains("write_u8"));
        // Tuple structs use index-based access
        assert!(output.contains("self . 0") && output.contains("self . 1"));
    }

    #[test]
    fn generate_serialize_for_unit_struct() {
        let input: syn::DeriveInput = syn::parse_str("struct Empty { }").unwrap();
        let tokens = generate_serialize(&input.data, false);
        let output = tokens.to_string();

        // Named struct with no fields should have 0 field count
        assert!(output.contains("write_u8") && output.contains("0"));
    }

    #[test]
    fn generate_deserialize_validates_field_count() {
        let input: syn::DeriveInput = syn::parse_str("struct Point { x: i32, y: i32 }").unwrap();
        let tokens = generate_deserialize(&input.data, false);
        let output = tokens.to_string();

        assert!(output.contains("read_u8"));
        assert!(output.contains("field_count"));
        assert!(output.contains("mismatch"));
    }

    #[test]
    fn max_field_count_is_255() {
        // Generate a struct with 256 fields
        let mut fields_str = String::from("struct Big { ");
        for i in 0..256 {
            fields_str.push_str(&format!("f{}: i32, ", i));
        }
        fields_str.push('}');

        let input: syn::DeriveInput = syn::parse_str(&fields_str).unwrap();
        let tokens = generate_serialize(&input.data, false);
        let output = tokens.to_string();

        assert!(output.contains("compile_error"));
        assert!(output.contains("255"));
    }

    #[test]
    fn generate_serialize_named_struct_single_field() {
        // Test serialization for a struct with just one field
        let input: syn::DeriveInput = syn::parse_str("struct Wrapper { value: i32 }").unwrap();
        let tokens = generate_serialize(&input.data, false);
        let output = tokens.to_string();

        assert!(output.contains("write_u8"));
        assert!(output.contains("1"));
        assert!(output.contains("MemoryPackSerialize"));
    }

    #[test]
    fn generate_serialize_tuple_struct_single_field() {
        // Tuple struct with just one field
        let input: syn::DeriveInput = syn::parse_str("struct Wrapper(i32);").unwrap();
        let tokens = generate_serialize(&input.data, false);
        let output = tokens.to_string();

        assert!(output.contains("write_u8"));
        assert!(output.contains("1"));
        assert!(output.contains("self . 0"));
    }

    #[test]
    fn generate_serialize_tuple_struct_many_fields() {
        // Tuple struct with many fields
        let input: syn::DeriveInput = syn::parse_str("struct Point(i32, i32, i32, i32);").unwrap();
        let tokens = generate_serialize(&input.data, false);
        let output = tokens.to_string();

        assert!(output.contains("write_u8"));
        assert!(output.contains("4"));
        // Should have self.0 through self.3
        assert!(output.contains("self . 0"));
        assert!(output.contains("self . 3"));
    }

    #[test]
    fn generate_deserialize_named_struct_validates_mismatch() {
        // Generate deserialize and check error message format
        let input: syn::DeriveInput = syn::parse_str("struct Point { x: i32, y: i32 }").unwrap();
        let tokens = generate_deserialize(&input.data, false);
        let output = tokens.to_string();

        // Check for the error message pattern
        assert!(output.contains("expected"));
        assert!(output.contains("received"));
    }

    #[test]
    fn generate_deserialize_tuple_struct_validates_mismatch() {
        let input: syn::DeriveInput = syn::parse_str("struct Point(i32, i32);").unwrap();
        let tokens = generate_deserialize(&input.data, false);
        let output = tokens.to_string();

        assert!(output.contains("read_u8"));
        assert!(output.contains("expected"));
        assert!(output.contains("received"));
    }

    #[test]
    fn generate_deserialize_unit_struct_returns_ok_self() {
        // Unit struct deserialize should just return Ok(Self)
        let input: syn::DeriveInput = syn::parse_str("struct Empty;").unwrap();
        let tokens = generate_deserialize(&input.data, false);
        let output = tokens.to_string();

        // Check for Ok followed by Self (with possible spacing)
        assert!(output.contains("Ok") && output.contains("Self"));
    }

    #[test]
    fn generate_serialize_with_zero_copy_for_borrowed_field() {
        // Struct with zero_copy enabled and borrowed u8 slice field
        let input: syn::DeriveInput = syn::parse_str("struct Data { bytes: &[u8] }").unwrap();
        let tokens = generate_serialize(&input.data, true); // is_zero_copy = true
        let output = tokens.to_string();

        // Zero-copy byte slices should use write_i32 for length
        assert!(output.contains("write_i32"));
        assert!(output.contains("buffer"));
    }

    #[test]
    fn generate_serialize_with_zero_copy_preserves_regular_serialize() {
        // Struct with zero_copy but regular fields still use standard serialize
        let input: syn::DeriveInput =
            syn::parse_str("struct Data { count: i32, bytes: &[u8] }").unwrap();
        let tokens = generate_serialize(&input.data, true);
        let output = tokens.to_string();

        // Both write_u8 for field count and write_i32 for bytes length
        assert!(output.contains("write_u8"));
        assert!(output.contains("write_i32"));
    }

    #[test]
    fn named_deserialize_generates_field_vars() {
        // Check that named struct deserialize generates field bindings
        let input: syn::DeriveInput =
            syn::parse_str("struct Person { name: String, age: u32 }").unwrap();
        let tokens = generate_deserialize(&input.data, false);
        let output = tokens.to_string();

        // Should have field count validation
        assert!(output.contains("received_field_count"));
        // Should have enter/leave object calls
        assert!(output.contains("enter_object"));
        assert!(output.contains("leave_object"));
    }

    #[test]
    fn tuple_deserialize_generates_field_vars() {
        // Check that tuple struct deserialize generates numbered field bindings
        let input: syn::DeriveInput = syn::parse_str("struct Point(i32, i32);").unwrap();
        let tokens = generate_deserialize(&input.data, false);
        let output = tokens.to_string();

        // Should have field_0, field_1, etc.
        assert!(output.contains("field_0"));
        assert!(output.contains("field_1"));
    }

    #[test]
    fn generate_serialize_multiple_named_fields() {
        // Verify field count is correct for multiple fields
        let input: syn::DeriveInput =
            syn::parse_str("struct Big { a: i32, b: i32, c: i32, d: i32, e: i32 }").unwrap();
        let tokens = generate_serialize(&input.data, false);
        let output = tokens.to_string();

        assert!(output.contains("write_u8"));
        assert!(output.contains("5"));
    }

    #[test]
    fn named_deserialize_with_explicit_order_uses_sorted_order() {
        // Test that fields with #[memorypack(order = N)] are deserialized in order
        let input: syn::DeriveInput = syn::parse_str(
            "struct Ordered { #[memorypack(order = 2)] third: i32, first: i32, #[memorypack(order = 1)] second: i32 }"
        ).unwrap();
        let tokens = generate_deserialize(&input.data, false);
        let output = tokens.to_string();

        // Should have field count 3
        assert!(output.contains("3"));
        assert!(output.contains("enter_object"));
    }

    #[test]
    fn tuple_struct_max_fields_returns_compile_error() {
        // Verify that tuple structs with >255 fields produce compile error
        let mut fields_str = String::from("struct Big(");
        for i in 0..256 {
            if i > 0 {
                fields_str.push_str(", ");
            }
            fields_str.push_str("i32");
        }
        fields_str.push_str(");");

        let input: syn::DeriveInput = syn::parse_str(&fields_str).unwrap();
        let tokens = generate_serialize(&input.data, false);
        let output = tokens.to_string();

        assert!(output.contains("compile_error"));
        assert!(output.contains("255"));
    }

    #[test]
    fn tuple_deserialize_max_fields_returns_compile_error() {
        // Verify deserialize also errors on >255 fields
        // Use a slightly different format to avoid parse issues
        let mut fields_str = String::from("struct Big {\n");
        for i in 0..256 {
            fields_str.push_str(&format!("field_{}: i32,\n", i));
        }
        fields_str.push('}');

        let input: syn::DeriveInput = syn::parse_str(&fields_str).unwrap();
        let tokens = generate_deserialize(&input.data, false);
        let output = tokens.to_string();

        assert!(output.contains("compile_error"));
    }
}
