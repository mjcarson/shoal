//! Different utilites for deriving traits/code

use quote::format_ident;
use syn::{FieldsNamed, Ident};

// Helper functions to identify types
pub(super) fn is_string_type(ty: &syn::Type) -> bool {
    if let syn::Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            return segment.ident == "String";
        }
    }
    false
}

pub(super) fn is_u64_type(ty: &syn::Type) -> bool {
    if let syn::Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            return segment.ident == "u64";
        }
    }
    false
}

pub(super) fn is_u32_type(ty: &syn::Type) -> bool {
    if let syn::Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            return segment.ident == "u32";
        }
    }
    false
}

/// Convert snake case strings to pascal case
///
/// # Arguments
///
/// * `snake_case` - The snake case string to convert
pub fn snake_to_pascal_case(snake_case: &str) -> String {
    snake_case
        .split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => {
                    first.to_uppercase().collect::<String>() + &chars.as_str().to_lowercase()
                }
            }
        })
        .collect()
}
/// Get the variant names for our enum
///
/// # Arguments
///
/// * `fields` - The named fields to get the pascal case variants for
pub fn get_variant_names(fields: &FieldsNamed) -> Vec<Ident> {
    // Extract the inner type name from each field's generic parameter
    fields
        .named
        .iter()
        .map(|field| {
            extract_inner_table_ident(&field.ty)
                .expect("Failed to extract inner table type for variant name")
        })
        .collect()
}

/// Determine if a field type is an unsorted table
pub fn is_unsorted_table(ty: &syn::Type) -> bool {
    if let syn::Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.first() {
            let type_name = segment.ident.to_string();
            return type_name.contains("Unsorted");
        }
    }
    false
}

/// Determine if a field type is a sorted table
pub fn is_sorted_table(ty: &syn::Type) -> bool {
    if let syn::Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.first() {
            let type_name = segment.ident.to_string();
            return type_name.contains("Sorted");
        }
    }
    false
}

/// Extract the first generic argument from a type like `PersistentUnsortedTable<Movie, ...>`
/// Returns the inner type (e.g., `Movie`)
pub fn extract_inner_table_type(ty: &syn::Type) -> Option<&syn::Type> {
    if let syn::Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.first() {
            if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                if let Some(syn::GenericArgument::Type(inner_type)) = args.args.first() {
                    return Some(inner_type);
                }
            }
        }
    }
    None
}

/// Extract the inner type as an Ident from a type like `PersistentUnsortedTable<Movie, ...>`
pub fn extract_inner_table_ident(ty: &syn::Type) -> Option<Ident> {
    if let Some(inner_type) = extract_inner_table_type(ty) {
        if let syn::Type::Path(type_path) = inner_type {
            if let Some(segment) = type_path.path.segments.last() {
                return Some(segment.ident.clone());
            }
        }
    }
    None
}

/// Rewrite table fields to add `<StructName>` to storage types and append `{StructName}TableNames`.
///
/// Transforms `PersistentUnsortedTable<Movie, FileSystem>` into
/// `PersistentUnsortedTable<Movie, FileSystem<Tmdb>, TmdbTableNames>`.
pub fn rewrite_table_fields(fields: &mut syn::FieldsNamed, struct_ident: &Ident) {
    let table_names_ident = format_ident!("{}TableNames", struct_ident);
    for field in fields.named.iter_mut() {
        let ty = &mut field.ty;
        if let syn::Type::Path(type_path) = ty {
            if let Some(segment) = type_path.path.segments.first_mut() {
                let type_name = segment.ident.to_string();
                // Only rewrite Persistent table types
                if !type_name.contains("Persistent") {
                    continue;
                }
                if let syn::PathArguments::AngleBracketed(args) = &mut segment.arguments {
                    // Add <StructName> generic to the storage type (2nd arg)
                    if let Some(syn::GenericArgument::Type(storage_ty)) = args.args.iter_mut().nth(1) {
                        if let syn::Type::Path(storage_path) = storage_ty {
                            if let Some(storage_seg) = storage_path.path.segments.last_mut() {
                                // Only add the generic if there isn't one already
                                if matches!(storage_seg.arguments, syn::PathArguments::None) {
                                    storage_seg.arguments = syn::PathArguments::AngleBracketed(
                                        syn::AngleBracketedGenericArguments {
                                            colon2_token: None,
                                            lt_token: syn::token::Lt::default(),
                                            args: {
                                                let mut punct = syn::punctuated::Punctuated::new();
                                                punct.push(syn::GenericArgument::Type(
                                                    syn::parse_quote!(#struct_ident),
                                                ));
                                                punct
                                            },
                                            gt_token: syn::token::Gt::default(),
                                        },
                                    );
                                }
                            }
                        }
                    }
                    // Append TableNames as the 3rd generic arg if not already present
                    if args.args.len() == 2 {
                        args.args.push(syn::GenericArgument::Type(
                            syn::parse_quote!(#table_names_ident),
                        ));
                    }
                }
            }
        }
    }
}
