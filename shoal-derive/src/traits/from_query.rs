//! Generate the from query trait implementation for a type

use syn::{FieldsNamed, Ident};

use crate::utils;

mod sorted;
mod unsorted;

// /// Extend a token stream with a From<#name> for *SortedQueryKinds implementation
// ///
// /// # Arguments
// ///
// /// * `stream` - The stream to extend
// /// * `name` - The name of the type we are extending
// /// * `query_name` - The name of the query type
// pub fn add_sorted(stream: &mut proc_macro2::TokenStream, name: &Ident, query_name: &Ident) {
//     // extend our token stream
//     stream.extend(quote! {
//         #[automatically_derived]
//         impl From<#name> for #query_name {
//             fn from(row: #name) -> #query_name {
//                 // get our rows partition key
//                 let key = #name::get_partition_key(&row);
//                 // build our query kind
//                 #query_name::#name(::shoal::shared::queries::SortedQuery::Insert { key, row })
//             }
//         }
//     });
// }

/// Add the correct trait implementation based on if this is a sorted or unsorted table
pub fn add(stream: &mut proc_macro2::TokenStream, db_name: &Ident, fields: &FieldsNamed) {
    // build the name of the query kinds for this db
    let query_kinds = syn::Ident::new(&format!("{db_name}QueryKinds"), db_name.span());
    // check each table in this db
    for table in &fields.named {
        // get the inner type name from the generic parameter
        let table_name = utils::extract_inner_table_ident(&table.ty)
            .expect("Failed to extract inner table ident");
        // get this tables type
        let table_type = &table.ty;
        // check if this is an unsorted table
        if utils::is_unsorted_table(table_type) {
            // this is an unsorted table so implement the conversions for unsorted tables
            unsorted::add(stream, &table_name, &query_kinds);
        // check if this is a sorted table
        } else if utils::is_sorted_table(table_type) {
            // this is an sorted table so implement the conversions for sorted tables
            sorted::add(stream, &table_name, &query_kinds);
        }
    }
}
