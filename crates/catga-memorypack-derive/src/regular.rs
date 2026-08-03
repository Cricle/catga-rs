use crate::helpers::{
    generate_field_deserialize, is_borrowed_u8_slice, is_zero_copy_field, prepare_ordered_fields,
    should_skip_field,
};

use quote::quote;
use syn::{Data, Fields};

pub fn generate_serialize(data: &Data, is_zero_copy: bool) -> proc_macro2::TokenStream {
    let Data::Struct(data_struct) = data else {
        return quote! {
            compile_error!("MemoryPackable serialize can only be derived for structs");
        };
    };

    match &data_struct.fields {
        Fields::Named(fields) => {
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
        Fields::Unnamed(fields) => {
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
                generate_field_serialize(field, quote! { self.#index }, is_zero_copy)
            });

            quote! {
                writer.write_u8(#field_count)?;
                #(#serialize_fields)*
            }
        }
        Fields::Unit => quote! {},
    }
}

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

    quote! { MemoryPackSerialize::serialize(&#access, writer)?; }
}

pub fn generate_deserialize(data: &Data, is_zero_copy: bool) -> proc_macro2::TokenStream {
    let Data::Struct(data_struct) = data else {
        return quote! {
            compile_error!("MemoryPackable deserialize can only be derived for structs");
        };
    };

    match &data_struct.fields {
        Fields::Named(fields) => {
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

            let deserialize_stmts: Vec<_> = fields
                .named
                .iter()
                .map(|f| generate_field_deserialize(f, is_zero_copy))
                .collect();

            let mut ordered_deserialize = Vec::new();
            let mut skip_field_idx = 0;
            let mut ordered_idx = 0;

            for f in &fields.named {
                if should_skip_field(f) {
                    ordered_deserialize
                        .push(deserialize_stmts[skip_field_idx + ordered_idx].clone());
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
        Fields::Unnamed(fields) => {
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
        Fields::Unit => quote! { Ok(Self) },
    }
}
