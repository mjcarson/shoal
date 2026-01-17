//! Generate the from query trait implementation for a type

use quote::quote;
use syn::Ident;

/// Extend a token stream with a From<#name> for *SortedQueryKinds implementation
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `query_name` - The name of the query type
pub fn add_sorted(stream: &mut proc_macro2::TokenStream, name: &Ident, query_name: &Ident) {
    // extend our token stream
    stream.extend(quote! {
        #[automatically_derived]
        impl From<#name> for #query_name {
            fn from(row: #name) -> #query_name {
                // get our rows partition key
                let key = #name::get_partition_key(&row);
                // build our query kind
                #query_name::#name(shoal_core::shared::queries::SortedQuery::Insert { key, row })
            }
        }
    });
}

/// Extend a token stream with a From<#name> for *UnsortedQueryKinds implementation
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `query_name` - The name of the query type
pub fn add_unsorted(stream: &mut proc_macro2::TokenStream, name: &Ident, query_name: &Ident) {
    // extend our token stream
    stream.extend(quote! {
        #[automatically_derived]
        impl From<#name> for #query_name {
            fn from(row: #name) -> #query_name {
                // get our rows partition key
                let key = #name::get_partition_key(&row);
                // build our query kind
                #query_name::#name(shoal_core::shared::queries::UnsortedQuery::Insert { key, row })
            }
        }
    });
}
