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

/// Take the projections each table of a database declares off of its field
///
/// A projection is declared on the field holding the table it projects, because the database
/// macro is the only thing that sees every projection of every table at once: the response
/// kinds enum needs a variant for each of them, and an enum cannot be added to later.
///
/// The attribute is consumed here rather than left on the struct, since the struct this macro
/// emits has no derive that would know what `shoal` means.
///
/// # Arguments
///
/// * `fields` - The named fields of the database struct
pub fn take_projections(fields: &mut syn::FieldsNamed) -> Vec<Vec<Ident>> {
    // collect the projections declared on each table, in field order
    let mut declared = Vec::with_capacity(fields.named.len());
    for field in fields.named.iter_mut() {
        // the projections this field declared, if it declared any
        let mut projections = Vec::new();
        // keep every attribute that is not a projection list, so nothing else is swallowed
        field.attrs.retain(|attr| {
            // only a `#[shoal(..)]` attribute can hold a projection list
            if !attr.path().is_ident("shoal") {
                return true;
            }
            // whether this attribute turned out to be the projection list
            let mut is_projections = false;
            // walk the arguments looking for `projections(..)`
            let parsed = attr.parse_nested_meta(|meta| {
                // anything other than a projection list belongs to somebody else
                if !meta.path.is_ident("projections") {
                    return Err(meta.error("expected `projections(..)`"));
                }
                is_projections = true;
                // each name in the list is a projection of this fields table
                meta.parse_nested_meta(|inner| {
                    let ident = inner
                        .path
                        .get_ident()
                        .ok_or_else(|| inner.error("a projection has to be a type name"))?;
                    projections.push(ident.clone());
                    Ok(())
                })
            });
            // a `#[shoal(..)]` that is not a projection list is left for whoever owns it
            match parsed {
                Ok(()) if is_projections => false,
                _ => true,
            }
        });
        declared.push(projections);
    }
    declared
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
