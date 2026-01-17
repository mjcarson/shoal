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
    // Extract field names and convert to PascalCase for enum variants
    fields
        .named
        .iter()
        .filter_map(|field| field.ident.as_ref())
        .map(|field_name| {
            // Convert snake_case to PascalCase
            let variant_name = snake_to_pascal_case(&field_name.to_string());
            format_ident!("{}", variant_name)
        })
        .collect()
}
