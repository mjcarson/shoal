//! Generate the TableRowFormat trait implementation for archived types

use quote::{format_ident, quote};
use syn::Ident;

/// Extend a token stream with a TableRowFormat implementation for the archived type
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `all_fields` - All fields in the struct (ident, type)
pub fn add(stream: &mut proc_macro2::TokenStream, name: &Ident, all_fields: &[(syn::Ident, syn::Type)]) {
    // Build the archived type name
    let archived_name = format_ident!("Archived{}", name);

    // Build field name strings for headers()
    let field_name_strs: Vec<_> = all_fields
        .iter()
        .map(|(ident, _)| ident.to_string())
        .collect();

    // Build row_values() expressions - format each field using Debug
    // since not all archived types implement Display (e.g., ArchivedVec)
    let row_value_exprs: Vec<_> = all_fields
        .iter()
        .map(|(ident, _)| {
            quote! {
                format!("{:?}", self.#ident)
            }
        })
        .collect();

    stream.extend(quote! {
        #[automatically_derived]
        impl ::shoal::shared::traits::TableRowFormat for #archived_name {
            fn headers() -> Vec<&'static str> {
                vec![#(#field_name_strs),*]
            }

            fn row_values(&self) -> Vec<String> {
                vec![#(#row_value_exprs),*]
            }
        }
    });
}
