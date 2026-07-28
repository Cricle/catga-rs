#![forbid(unsafe_code)]
//! Procedural macros for Catga, including message derives with stable Rust type identities.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Attribute, Data, DeriveInput, Expr, ExprLit, Fields, GenericParam, Generics, Ident, Lit,
    LitInt, LitStr, Meta, Result, Token, parse_macro_input, parse_quote, punctuated::Punctuated,
    spanned::Spanned,
};

mod handlers;

/// Implements `catga_core::Message` with the fully qualified, monomorphized Rust type name.
///
/// The generated `message_type` is identical to the default implementation:
/// `std::any::type_name::<Self>()`.
///
/// `#[catga(authorize, roles("role"), policy("name"))]` additionally emits
/// an `catga_core::AuthorizedRequest` implementation for request types.
/// `#[catga(batch_key = "field_name")]` emits `catga_core::BatchKeyProvider`.
/// `#[catga(priority = high)]` emits a static typed transport priority.
/// `#[catga(trace_tag)]` on a named field emits an explicit structured-tracing tag using the
/// `catga.message.{field}` name; `#[catga(trace_tag = "name")]` selects an explicit name.
/// `#[catga(trace_tags(prefix = "name.", include = ["field"], exclude = ["field"]))]`
/// bulk-selects named fields. An explicit `include` takes precedence over public-field selection,
/// `exclude` removes bulk-selected fields, and field-level `trace_tag` declarations retain their
/// explicit names. `all_public = false` disables the public-field fallback.
/// Generic message structs are supported when their type parameters satisfy Catga's required
/// `Send + Sync + 'static` bounds.
///
/// # Example
///
/// ```
/// use catga_core::Message as _;
/// use catga_macros::Message;
///
/// #[derive(Message)]
/// struct RebuildSearchIndex {
///     tenant: String,
/// }
///
/// let message = RebuildSearchIndex { tenant: "acme".into() };
/// assert!(message.message_type().ends_with("RebuildSearchIndex"));
/// ```
#[proc_macro_derive(Message, attributes(catga))]
pub fn derive_message(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let generics = message_generics(&input.generics);
    let schema_version = match schema_version_impl(&input.attrs) {
        Ok(version) => version,
        Err(error) => return error.into_compile_error().into(),
    };
    let priority = match priority_impl(&input.attrs) {
        Ok(priority) => priority,
        Err(error) => return error.into_compile_error().into(),
    };
    let authorization = match authorization_impl(&input.attrs, &input.ident, &generics) {
        Ok(authorization) => authorization,
        Err(error) => return error.into_compile_error().into(),
    };
    let batch_key = match batch_key_impl(&input, &generics) {
        Ok(batch_key) => batch_key,
        Err(error) => return error.into_compile_error().into(),
    };
    let batch_options = match batch_options_impl(&input, &generics) {
        Ok(batch_options) => batch_options,
        Err(error) => return error.into_compile_error().into(),
    };
    let trace_tags = match trace_tags_impl(&input) {
        Ok(trace_tags) => trace_tags,
        Err(error) => return error.into_compile_error().into(),
    };
    let name = input.ident;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    quote! {
        impl #impl_generics ::catga_core::Message for #name #ty_generics #where_clause {
            fn message_type(&self) -> &'static str {
                ::std::any::type_name::<Self>()
            }

            fn schema_version(&self) -> u32 { #schema_version }

            #priority

            #trace_tags
        }
        #authorization
        #batch_key
        #batch_options
    }
    .into()
}

fn priority_impl(attributes: &[Attribute]) -> Result<Option<TokenStream2>> {
    let mut implementation = None;
    for attribute in attributes
        .iter()
        .filter(|attribute| attribute.path().is_ident("catga"))
    {
        for option in attribute.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)? {
            let Meta::NameValue(value) = option else {
                continue;
            };
            if !value.path.is_ident("priority") {
                continue;
            }
            if implementation.is_some() {
                return Err(syn::Error::new_spanned(
                    value,
                    "Catga message priority must be declared at most once",
                ));
            }
            let Expr::Path(path) = &value.value else {
                return Err(syn::Error::new_spanned(
                    value,
                    "Catga message priority must be one of: low, normal, high, critical",
                ));
            };
            let Some(priority) = path.path.get_ident() else {
                return Err(syn::Error::new_spanned(
                    path,
                    "Catga message priority must be one of: low, normal, high, critical",
                ));
            };
            let priority = match priority.to_string().as_str() {
                "low" => quote!(::catga_core::MessagePriority::Low),
                "normal" => quote!(::catga_core::MessagePriority::Normal),
                "high" => quote!(::catga_core::MessagePriority::High),
                "critical" => quote!(::catga_core::MessagePriority::Critical),
                _ => {
                    return Err(syn::Error::new_spanned(
                        priority,
                        "Catga message priority must be one of: low, normal, high, critical",
                    ));
                }
            };
            implementation = Some(quote! {
                fn priority(&self) -> ::catga_core::MessagePriority {
                    #priority
                }
            });
        }
    }
    Ok(implementation)
}

fn schema_version_impl(attributes: &[Attribute]) -> Result<u32> {
    let mut version = None;
    for attribute in attributes
        .iter()
        .filter(|attribute| attribute.path().is_ident("catga"))
    {
        for option in attribute.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)? {
            let Meta::NameValue(value) = option else {
                continue;
            };
            if !value.path.is_ident("version") {
                continue;
            }
            let Expr::Lit(ExprLit {
                lit: Lit::Int(value),
                ..
            }) = value.value
            else {
                return Err(syn::Error::new_spanned(
                    value,
                    "Catga message version must be a positive integer",
                ));
            };
            let parsed = value.base10_parse::<u32>()?;
            if parsed == 0 || version.replace(parsed).is_some() {
                return Err(syn::Error::new_spanned(
                    value,
                    "Catga message version must be declared once and be greater than zero",
                ));
            }
        }
    }
    Ok(version.unwrap_or(1))
}

fn message_generics(source: &Generics) -> Generics {
    let mut generics = source.clone();
    let where_clause = generics.make_where_clause();
    for parameter in &source.params {
        let GenericParam::Type(parameter) = parameter else {
            continue;
        };
        let identifier = &parameter.ident;
        where_clause
            .predicates
            .push(parse_quote!(#identifier: ::std::marker::Send + ::std::marker::Sync + 'static));
    }
    generics
}

fn trace_tags_impl(input: &DeriveInput) -> Result<Option<TokenStream2>> {
    let Data::Struct(structure) = &input.data else {
        return Ok(None);
    };
    let Fields::Named(fields) = &structure.fields else {
        return Ok(None);
    };
    let bulk_tags = trace_tags_config(&input.attrs)?;
    let mut explicit_fields = Vec::new();
    let mut records = Vec::new();
    for field in &fields.named {
        let Some(identifier) = &field.ident else {
            continue;
        };
        let mut trace_tag = None;
        for attribute in &field.attrs {
            if !attribute.path().is_ident("catga") {
                continue;
            }
            let options =
                attribute.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
            for option in options {
                let name = match option {
                    Meta::Path(path) if path.is_ident("trace_tag") => {
                        LitStr::new(&format!("catga.message.{identifier}"), identifier.span())
                    }
                    Meta::NameValue(value) if value.path.is_ident("trace_tag") => {
                        let Expr::Lit(ExprLit {
                            lit: Lit::Str(name),
                            ..
                        }) = value.value
                        else {
                            return Err(syn::Error::new_spanned(
                                value,
                                "Catga trace tag must be a string name",
                            ));
                        };
                        if name.value().is_empty() {
                            return Err(syn::Error::new_spanned(
                                name,
                                "Catga trace tag must not be empty",
                            ));
                        }
                        name
                    }
                    other => {
                        return Err(syn::Error::new_spanned(
                            other,
                            "expected `trace_tag` or `trace_tag = \"name\"` on a Catga message field",
                        ));
                    }
                };
                if trace_tag.replace(name).is_some() {
                    return Err(syn::Error::new_spanned(
                        attribute,
                        "duplicate Catga trace tag for a field",
                    ));
                }
            }
        }
        if let Some(name) = trace_tag {
            explicit_fields.push(identifier.to_string());
            records.push(quote!(visitor(#name, &self.#identifier);));
        }
    }
    if let Some(bulk_tags) = bulk_tags {
        for field in &fields.named {
            let Some(identifier) = &field.ident else {
                continue;
            };
            let field_name = identifier.to_string();
            let included = if bulk_tags.include.is_empty() {
                bulk_tags.all_public && matches!(field.vis, syn::Visibility::Public(_))
            } else {
                bulk_tags.include.iter().any(|name| name == &field_name)
            };
            if !included
                || bulk_tags.exclude.iter().any(|name| name == &field_name)
                || explicit_fields.iter().any(|name| name == &field_name)
            {
                continue;
            }
            let name = LitStr::new(
                &format!("{}{}", bulk_tags.prefix.value(), field_name),
                identifier.span(),
            );
            records.push(quote!(visitor(#name, &self.#identifier);));
        }
    }
    if records.is_empty() {
        return Ok(None);
    }
    Ok(Some(quote! {
        fn visit_trace_tags(&self, visitor: &mut dyn FnMut(&str, &dyn ::std::fmt::Display)) {
            #(#records)*
        }
    }))
}

struct TraceTagsConfig {
    prefix: LitStr,
    include: Vec<String>,
    exclude: Vec<String>,
    all_public: bool,
}

fn trace_tags_config(attributes: &[Attribute]) -> Result<Option<TraceTagsConfig>> {
    let mut config = None;
    for attribute in attributes
        .iter()
        .filter(|attribute| attribute.path().is_ident("catga"))
    {
        let options = attribute.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
        for option in options {
            let Meta::List(list) = option else {
                continue;
            };
            if !list.path.is_ident("trace_tags") {
                continue;
            }
            if config.is_some() {
                return Err(syn::Error::new_spanned(
                    list,
                    "Catga bulk trace tags must be declared at most once",
                ));
            }
            let options = list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
            let mut prefix = None;
            let mut include = None;
            let mut exclude = None;
            let mut all_public = None;
            for option in options {
                let Meta::NameValue(value) = option else {
                    return Err(syn::Error::new_spanned(
                        option,
                        "Catga bulk trace tags require `prefix`, `include`, `exclude`, or `all_public` options",
                    ));
                };
                let Some(name) = value.path.get_ident() else {
                    return Err(syn::Error::new_spanned(
                        value.path,
                        "Catga bulk trace tag option names must be identifiers",
                    ));
                };
                match name.to_string().as_str() {
                    "prefix" => {
                        if prefix.is_some() {
                            return Err(syn::Error::new_spanned(
                                value,
                                "Catga bulk trace tag prefix must be declared at most once",
                            ));
                        }
                        let Expr::Lit(ExprLit {
                            lit: Lit::Str(value),
                            ..
                        }) = value.value
                        else {
                            return Err(syn::Error::new_spanned(
                                value,
                                "Catga bulk trace tag prefix must be a string",
                            ));
                        };
                        if value.value().is_empty() {
                            return Err(syn::Error::new_spanned(
                                value,
                                "Catga bulk trace tag prefix must not be empty",
                            ));
                        }
                        prefix = Some(value);
                    }
                    "include" => {
                        if include.is_some() {
                            return Err(syn::Error::new_spanned(
                                value,
                                "Catga bulk trace tag include must be declared at most once",
                            ));
                        }
                        include = Some(trace_tag_field_names(value.value, "include")?);
                    }
                    "exclude" => {
                        if exclude.is_some() {
                            return Err(syn::Error::new_spanned(
                                value,
                                "Catga bulk trace tag exclude must be declared at most once",
                            ));
                        }
                        exclude = Some(trace_tag_field_names(value.value, "exclude")?);
                    }
                    "all_public" => {
                        if all_public.is_some() {
                            return Err(syn::Error::new_spanned(
                                value,
                                "Catga bulk trace tag all_public must be declared at most once",
                            ));
                        }
                        let Expr::Lit(ExprLit {
                            lit: Lit::Bool(value),
                            ..
                        }) = value.value
                        else {
                            return Err(syn::Error::new_spanned(
                                value,
                                "Catga bulk trace tag all_public must be true or false",
                            ));
                        };
                        all_public = Some(value.value);
                    }
                    _ => {
                        return Err(syn::Error::new_spanned(
                            name,
                            "expected `prefix`, `include`, `exclude`, or `all_public` for Catga bulk trace tags",
                        ));
                    }
                }
            }
            config = Some(TraceTagsConfig {
                prefix: prefix.unwrap_or_else(|| LitStr::new("catga.message.", list.span())),
                include: include.unwrap_or_default(),
                exclude: exclude.unwrap_or_default(),
                all_public: all_public.unwrap_or(true),
            });
        }
    }
    Ok(config)
}

fn trace_tag_field_names(value: Expr, option: &str) -> Result<Vec<String>> {
    let Expr::Array(values) = value else {
        return Err(syn::Error::new_spanned(
            value,
            format!("Catga bulk trace tag {option} must be a string array"),
        ));
    };
    values
        .elems
        .into_iter()
        .map(|value| {
            let Expr::Lit(ExprLit {
                lit: Lit::Str(value),
                ..
            }) = value
            else {
                return Err(syn::Error::new_spanned(
                    value,
                    format!("Catga bulk trace tag {option} entries must be strings"),
                ));
            };
            if value.value().is_empty() {
                return Err(syn::Error::new_spanned(
                    value,
                    format!("Catga bulk trace tag {option} entries must not be empty"),
                ));
            }
            Ok(value.value())
        })
        .collect()
}

fn authorization_impl(
    attributes: &[Attribute],
    name: &Ident,
    generics: &Generics,
) -> Result<Option<TokenStream2>> {
    let mut declared = false;
    let mut roles = Vec::new();
    let mut policy = None;
    for attribute in attributes {
        if !attribute.path().is_ident("catga") {
            continue;
        }
        let options = attribute.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
        for option in options {
            match option {
                Meta::Path(path) if path.is_ident("authorize") => declared = true,
                Meta::NameValue(value)
                    if value.path.is_ident("version") || value.path.is_ident("priority") => {}
                Meta::List(list) if list.path.is_ident("roles") => {
                    declared = true;
                    let values =
                        list.parse_args_with(Punctuated::<LitStr, Token![,]>::parse_terminated)?;
                    roles.extend(values);
                }
                Meta::List(list) if list.path.is_ident("policy") => {
                    declared = true;
                    if policy.is_some() {
                        return Err(syn::Error::new_spanned(
                            list,
                            "duplicate Catga authorization policy",
                        ));
                    }
                    policy = Some(list.parse_args::<LitStr>()?);
                }
                Meta::List(list) if list.path.is_ident("batch") => {}
                Meta::List(list) if list.path.is_ident("trace_tags") => {}
                Meta::NameValue(value) if value.path.is_ident("batch_key") => {}
                Meta::NameValue(value)
                    if value.path.is_ident("version") || value.path.is_ident("priority") => {}
                other => {
                    return Err(syn::Error::new_spanned(
                        other,
                        "expected `authorize`, `roles(\"...\")`, `policy(\"...\")`, `batch_key = \"field\"`, or `batch(...)`",
                    ));
                }
            }
        }
    }
    if !declared {
        return Ok(None);
    }
    let requirements = match (roles.is_empty(), policy) {
        (true, None) => quote!(::catga_core::AuthorizationRequirements::authenticated()),
        (false, None) => {
            quote!(::catga_core::AuthorizationRequirements::with_roles(&[#(#roles),*]))
        }
        (true, Some(policy)) => {
            quote!(::catga_core::AuthorizationRequirements::with_policy(#policy))
        }
        (false, Some(policy)) => quote!(
            ::catga_core::AuthorizationRequirements::with_roles_and_policy(&[#(#roles),*], #policy)
        ),
    };
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    Ok(Some(quote! {
        impl #impl_generics ::catga_core::AuthorizedRequest for #name #ty_generics #where_clause {
            fn authorization() -> ::catga_core::AuthorizationRequirements {
                #requirements
            }
        }
    }))
}

fn batch_options_impl(input: &DeriveInput, generics: &Generics) -> Result<Option<TokenStream2>> {
    let mut options_impl = None;
    for attribute in &input.attrs {
        if !attribute.path().is_ident("catga") {
            continue;
        }
        let options = attribute.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
        for option in options {
            let Meta::List(list) = option else {
                continue;
            };
            if !list.path.is_ident("batch") {
                continue;
            }
            if options_impl.is_some() {
                return Err(syn::Error::new_spanned(
                    list,
                    "duplicate Catga batch configuration",
                ));
            }
            let fields = list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
            let mut updates = Vec::new();
            for field in fields {
                let Meta::NameValue(field) = field else {
                    return Err(syn::Error::new_spanned(
                        field,
                        "Catga batch options must be `name = positive_integer`",
                    ));
                };
                let Some(identifier) = field.path.get_ident() else {
                    return Err(syn::Error::new_spanned(
                        field.path,
                        "Catga batch option names must be identifiers",
                    ));
                };
                let Expr::Lit(ExprLit {
                    lit: Lit::Int(value),
                    ..
                }) = field.value
                else {
                    return Err(syn::Error::new_spanned(
                        field,
                        "Catga batch options must be positive integers",
                    ));
                };
                let parsed = value.base10_parse::<u64>().map_err(|_| {
                    syn::Error::new_spanned(&value, "Catga batch option is too large")
                })?;
                if parsed == 0 {
                    return Err(syn::Error::new_spanned(
                        value,
                        "Catga batch options must be greater than zero",
                    ));
                }
                updates.push(batch_option_update(identifier, value)?);
            }
            let name = &input.ident;
            let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
            options_impl = Some(quote! {
                impl #impl_generics ::catga_core::BatchOptionsProvider for #name #ty_generics #where_clause {
                    fn batch_options() -> ::catga_core::BatchOptions {
                        let mut options = ::catga_core::BatchOptions::default();
                        #(#updates)*
                        options
                    }
                }
            });
        }
    }
    Ok(options_impl)
}

fn batch_option_update(identifier: &Ident, value: LitInt) -> Result<TokenStream2> {
    match identifier.to_string().as_str() {
        "max_batch_size" => Ok(quote!(options.max_batch_size = #value;)),
        "timeout_ms" => Ok(quote!(
            options.batch_timeout = ::std::time::Duration::from_millis(#value);
        )),
        "max_queue_length" => Ok(quote!(options.max_queue_length = #value;)),
        "max_shards" => Ok(quote!(options.max_shards = #value;)),
        "flush_concurrency" => Ok(quote!(options.flush_concurrency = #value;)),
        _ => Err(syn::Error::new_spanned(
            identifier,
            "expected `max_batch_size`, `timeout_ms`, `max_queue_length`, `max_shards`, or `flush_concurrency`",
        )),
    }
}

fn batch_key_impl(input: &DeriveInput, generics: &Generics) -> Result<Option<TokenStream2>> {
    let mut key = None;
    for attribute in &input.attrs {
        if !attribute.path().is_ident("catga") {
            continue;
        }
        let options = attribute.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
        for option in options {
            let Meta::NameValue(value) = option else {
                continue;
            };
            if !value.path.is_ident("batch_key") {
                continue;
            }
            if key.is_some() {
                return Err(syn::Error::new_spanned(
                    value,
                    "duplicate Catga batch key field",
                ));
            }
            let Expr::Lit(ExprLit {
                lit: Lit::Str(field),
                ..
            }) = value.value
            else {
                return Err(syn::Error::new_spanned(
                    value,
                    "Catga batch key must be a string field name",
                ));
            };
            if field.value().is_empty() {
                return Err(syn::Error::new_spanned(
                    field,
                    "Catga batch key field must not be empty",
                ));
            }
            key = Some(field);
        }
    }

    let Some(key) = key else {
        return Ok(None);
    };
    let field = format_ident!("{}", key.value());
    let Data::Struct(structure) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "Catga batch key can only be declared on a struct with named fields",
        ));
    };
    let Fields::Named(fields) = &structure.fields else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "Catga batch key can only reference a named struct field",
        ));
    };
    if !fields
        .named
        .iter()
        .any(|candidate| candidate.ident == Some(field.clone()))
    {
        return Err(syn::Error::new_spanned(
            key,
            "Catga batch key field does not exist",
        ));
    }

    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    Ok(Some(quote! {
        impl #impl_generics ::catga_core::BatchKeyProvider for #name #ty_generics #where_clause {
            fn batch_key(&self) -> ::core::option::Option<::std::boxed::Box<str>> {
                ::core::option::Option::Some(
                    ::std::string::ToString::to_string(&self.#field).into_boxed_str()
                )
            }
        }
    }))
}

/// Builds an explicit `catga_core::CatgaResult<Registry>` from typed request, command, and
/// event handler expressions.
///
/// Each request or command message may be registered exactly once. Repeating either message
/// kind is reported during macro expansion when it is syntactically visible; equivalent type
/// aliases return a startup `catga_core::ErrorCode::Conflict` instead of panicking. Event
/// messages may intentionally register multiple handlers.
/// Handler entries are expressions, so applications can register a unit-like handler path or
/// construct a handler with explicit Rust dependencies, for example
/// `request CreateOrder => CreateOrderHandler::new(repository)` or
/// `command RebuildIndex => RebuildIndexHandler::new(repository)`.
///
/// # Example
///
/// The macro emits a registration function for the selected `catga_core::Mediator`. The
/// handler types must implement the corresponding Catga handler traits, so this short form is
/// marked `no_run` and is intended to be completed with application dependencies.
///
/// ```ignore
/// use catga_macros::catga_handlers;
///
/// catga_handlers! {
///     event InventoryRebuilt => [RefreshReadModel, PublishAuditEvent]
/// }
/// ```
#[proc_macro]
pub fn catga_handlers(input: TokenStream) -> TokenStream {
    handlers::expand(input.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
