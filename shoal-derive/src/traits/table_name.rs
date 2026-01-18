//! Generate the TableNameSupport trait for a shoal database

use quote::quote;
use syn::Ident;

/// Build our table name support impl
///
/// # Arguments
///
/// * `stream` - The token stream to add too
/// * `enum_ident` - The identity of the table name entity to create
/// * `variants` - The different tables in this db
pub fn add(stream: &mut proc_macro2::TokenStream, enum_ident: &Ident, variants: &Vec<Ident>) {
    // Generate and add a table name support impl
    stream.extend(quote! {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum #enum_ident {
            #(#variants),*
        }

        #[automatically_derived]
        impl shoal_core::shared::traits::TableNameSupport for #enum_ident {}
    });
}
