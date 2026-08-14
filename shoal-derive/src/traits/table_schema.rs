//! Generate the TableSchemaSupport trait implementation for a type

use quote::quote;
use std::collections::HashSet;
use syn::Ident;

use crate::traits::fingerprint;

/// Extend a token stream with a TableSchemaSupport implementation
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `all_fields` - All fields in the struct (ident, type)
/// * `partition_fields` - Fields marked as partition keys
/// * `sort_fields` - Fields marked as sort keys
/// * `filter_fields` - Fields marked as filter keys
/// * `update_fields` - Fields marked as updatable
pub fn add(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    all_fields: &[(syn::Ident, syn::Type)],
    partition_fields: &[(syn::Ident, syn::Type)],
    sort_fields: &[(syn::Ident, syn::Type)],
    filter_fields: &[(syn::Ident, syn::Type)],
    update_fields: &[(syn::Ident, syn::Type)],
) {
    // Build field name strings for field_names()
    let field_name_strs: Vec<_> = all_fields
        .iter()
        .map(|(ident, _)| ident.to_string())
        .collect();

    // Build validator match arms
    let validator_arms: Vec<_> = all_fields
        .iter()
        .map(|(ident, ty)| {
            let name_str = ident.to_string();
            quote! {
                #name_str => Some(shoal_core::shared::queries::parser::make_validator::<#ty>()),
            }
        })
        .collect();

    // Build sets for quick lookup
    let partition_names: HashSet<String> = partition_fields
        .iter()
        .map(|(ident, _)| ident.to_string())
        .collect();
    let sort_names: HashSet<String> = sort_fields
        .iter()
        .map(|(ident, _)| ident.to_string())
        .collect();
    let filter_names: HashSet<String> = filter_fields
        .iter()
        .map(|(ident, _)| ident.to_string())
        .collect();

    // Build role match arms - only for fields with a role
    let role_arms: Vec<_> = all_fields
        .iter()
        .filter_map(|(ident, _)| {
            let name_str = ident.to_string();
            if partition_names.contains(&name_str) {
                Some(quote! {
                    #name_str => Some(shoal_core::shared::queries::parser::FieldRole::Partition),
                })
            } else if sort_names.contains(&name_str) {
                Some(quote! {
                    #name_str => Some(shoal_core::shared::queries::parser::FieldRole::Sort),
                })
            } else if filter_names.contains(&name_str) {
                Some(quote! {
                    #name_str => Some(shoal_core::shared::queries::parser::FieldRole::Filter),
                })
            } else {
                None
            }
        })
        .collect();

    // fold this tables whole shape into the constant the two peers compare in their handshake
    //
    // the update fields are mixed in even though they have no query role, because they shape the
    // generated Update struct and that struct travels inside QueryKinds
    let schema_fingerprint = fingerprint::row_expr(
        name,
        all_fields,
        partition_fields,
        sort_fields,
        filter_fields,
        update_fields,
    );

    stream.extend(quote! {
        #[automatically_derived]
        impl shoal_core::shared::traits::TableSchemaSupport for #name {
            const SCHEMA_FINGERPRINT: u64 = #schema_fingerprint;

            fn get_field_validator(field_name: &str) -> Option<shoal_core::shared::queries::parser::TypeValidator> {
                match field_name {
                    #(#validator_arms)*
                    _ => None,
                }
            }

            fn get_field_role(field_name: &str) -> Option<shoal_core::shared::queries::parser::FieldRole> {
                match field_name {
                    #(#role_arms)*
                    _ => None,
                }
            }

            fn field_names() -> Vec<&'static str> {
                vec![#(#field_name_strs),*]
            }
        }
    });
}
