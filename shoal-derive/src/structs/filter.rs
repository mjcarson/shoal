//! Generate a filter struct for a type

use quote::{format_ident, quote};
use syn::Ident;

/// Extend a token stream with a Filter struct definition
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `filter_fields` - The fields to include in the filter struct (ident, type)
pub fn add(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    filter_fields: &[(syn::Ident, syn::Type)],
) {
    // Build the filter struct name
    let filter_name = format_ident!("{}Filter", name);

    // If no filter fields, create an empty struct
    if filter_fields.is_empty() {
        stream.extend(quote! {
            #[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Default)]
            #[rkyv(derive(Debug))]
            pub struct #filter_name;
        });
        return;
    }

    // Build the fields for the filter struct (all optional)
    let fields = filter_fields.iter().map(|(ident, ty)| {
        quote! {
            pub #ident: Option<#ty>
        }
    });

    // Generate the filter struct
    stream.extend(quote! {
        #[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Default)]
        #[rkyv(derive(Debug))]
        pub struct #filter_name {
            #(#fields),*
        }
    });
}
