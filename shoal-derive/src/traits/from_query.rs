//! Generate the from query trait implementation for a type

use quote::format_ident;
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
//                 #query_name::#name(shoal_core::shared::queries::SortedQuery::Insert { key, row })
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
        // raise an error on any fields without idents
        let table_name_snake = match &table.ident {
            // convert our table name to pascal case
            Some(table_name) => table_name,
            None => panic!("Shoal DB structs must be named fields: {:?}", table.ty),
        };
        // convert our table name from snake case to pascal case
        let table_name_str = utils::snake_to_pascal_case(&table_name_snake.to_string());
        // cast this pascal case table name to an ident
        let table_name = format_ident!("{table_name_str}");
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
