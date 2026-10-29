//! Generate the from query trait implementation for a type

use crate::schema::Schema;
use crate::tables::TableKinds;

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
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `schema` - The tables in this database
pub fn add(stream: &mut proc_macro2::TokenStream, schema: &Schema) {
    // build the name of the query kinds for this db
    let query_kinds = syn::Ident::new(&format!("{}QueryKinds", schema.db), schema.db.span());
    // check each table in this db, a bucket's included
    for table in &schema.tables {
        match table.kind {
            // this is an unsorted table so implement the conversions for unsorted tables
            TableKinds::Unsorted => unsorted::add(stream, &table.row, &query_kinds),
            // this is an sorted table so implement the conversions for sorted tables
            TableKinds::Sorted => sorted::add(stream, &table.row, &query_kinds),
        }
    }
}
