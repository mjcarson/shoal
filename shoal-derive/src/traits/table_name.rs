//! Generate the TableNameSupport trait for a shoal database

use quote::quote;
use syn::Ident;

use crate::schema::Schema;

/// Build our table name support impl
///
/// # Arguments
///
/// * `stream` - The token stream to add too
/// * `enum_ident` - The identity of the table name entity to create
/// * `schema` - The tables in this db
pub fn add(stream: &mut proc_macro2::TokenStream, enum_ident: &Ident, schema: &Schema) {
    // the variant of each table, which is the row it holds
    let variants = schema.variants();
    // the name each variant is spelled as, which is what its stable identity is hashed from
    let names: Vec<String> = variants.iter().map(Ident::to_string).collect();
    // where each variant sits among the schema's tables, the order `table_names` lists them in
    let positions: Vec<usize> = (0..variants.len()).collect();
    // the bucket that generated each table, if one did
    let buckets = schema.tables.iter().map(|table| match &table.bucket {
        Some(marker) => {
            let name = marker.to_string();
            quote!(Some(#name))
        }
        None => quote!(None),
    });
    // Generate and add a table name support impl
    stream.extend(quote! {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum #enum_ident {
            #(#variants),*
        }

        #[automatically_derived]
        impl ::shoal::shared::traits::TableNameSupport for #enum_ident {
            /// Get the stable identity of this table, the hash of its name
            fn table_id(&self) -> ::shoal::shared::identity::TableId {
                match self {
                    #(#enum_ident::#variants => ::shoal::shared::identity::TableId::of(#names),)*
                }
            }

            /// Get where this table sits among the schema's tables, in declaration order
            fn position(&self) -> usize {
                match self {
                    #(#enum_ident::#variants => #positions,)*
                }
            }

            /// Get the bucket that generated this table, or `None` for one the schema declared
            fn bucket(&self) -> Option<&'static str> {
                match self {
                    #(#enum_ident::#variants => #buckets,)*
                }
            }
        }
    });
}
