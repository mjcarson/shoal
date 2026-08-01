//! Generate the unsorted from query trait implementations for a type

use quote::{format_ident, quote};
use syn::Ident;

/// Extend a token stream with an implementation for converting an insert into a query kind
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `table_name` - The name of the table
/// * `query_name` - The name of the query kinds enum
fn add_insert(stream: &mut proc_macro2::TokenStream, table_name: &Ident, query_name: &Ident) {
    // extend our token stream
    stream.extend(quote! {
        #[automatically_derived]
        impl From<#table_name> for #query_name {
            fn from(row: #table_name) -> #query_name {
                // import partition key support so we can use the get_partition_key method
                use shoal_core::shared::traits::PartitionKeySupport;
                // get our rows partition key
                let key = #table_name::get_partition_key(&row);
                // build our query kind
                #query_name::#table_name(shoal_core::shared::queries::UnsortedQuery::Insert { key, row })
            }
        }
    });
}

/// Extend a token stream with an implementation for converting a get into a query kind
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `table_name` - The name of the table
/// * `query_name` - The name of the query kinds enum
fn add_get(stream: &mut proc_macro2::TokenStream, table_name: &Ident, query_kinds: &Ident) {
    // build the name for our get query
    let get_name = format_ident!("{}Get", table_name);
    // extend our token stream with an impl to turn a get query into a query kind
    stream.extend(quote! {
          #[automatically_derived]
          impl From<#get_name> for #query_kinds {
              /// Build a `QueryKind` for getting rows
              fn from(specific: #get_name) -> Self {
                  // hash each partition key, keeping them in the order they were asked for
                  let partition_keys = specific.partition_keys
                      .iter()
                      .map(|key| <#table_name as shoal_core::shared::traits::PartitionKeySupport>::get_partition_key_from_values(key))
                      .collect();
                  // build the general query
                  let general = shoal_core::shared::queries::UnsortedGet {
                      partition_keys,
                      filters: specific.filters,
                      limit: specific.limit,
                  };
                  // build our query kind
                  Self::#table_name(shoal_core::shared::queries::UnsortedQuery::Get(general))
              }
          }
      });
}

/// Extend a token stream with an implementation for converting an update into a query kind
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `table_name` - The name of the table
/// * `query_name` - The name of the query kinds enum
fn add_update(stream: &mut proc_macro2::TokenStream, table_name: &Ident, query_kinds: &Ident) {
    // build the name for our update query
    let update_name = format_ident!("{}Update", table_name);
    // build the name for just our update data in our query
    let update_data_name = format_ident!("{}UpdateData", table_name);
    // extend our token stream with a from impl for our update to our query kinds
    stream.extend(quote! {
        #[automatically_derived]
        impl From<#update_name> for #query_kinds {
            /// Build a `QueryKind` for updating a row
            fn from(specific: #update_name) -> Self {
                // hash the partition key to get the u64 key
                let partition_key = <#table_name as shoal_core::shared::traits::PartitionKeySupport>::get_partition_key_from_values(&specific.partition_key);
                // cast this update to a generalized update
                let general = shoal_core::shared::queries::UnsortedUpdate {
                    partition_key,
                    update: #update_data_name::from(specific),
                };
                // wrap our general update in a query
                let query = shoal_core::shared::queries::UnsortedQuery::Update(general);
                // wrap in table specific query kind
                #query_kinds::#table_name(query)
            }
        }
    });
}

/// Extend a token stream with an implementation for converting a get into a query kind
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `table_name` - The name of the table
/// * `query_name` - The name of the query kinds enum
fn add_delete(stream: &mut proc_macro2::TokenStream, table_name: &Ident, query_kinds: &Ident) {
    // build the name for our delete query
    let delete_name = format_ident!("{}Delete", table_name);
    // extend our token stream with an impl to turn a delete query into a query kind
    stream.extend(quote! {
        #[automatically_derived]
        impl From<#delete_name> for #query_kinds {
            /// Build a `QueryKind` for deleting a row
            fn from(delete: #delete_name) -> Self {
                // hash the partition key to get the u64 key
                let key = <#table_name as shoal_core::shared::traits::PartitionKeySupport>::get_partition_key_from_values(&delete.partition_key);
                let query = shoal_core::shared::queries::UnsortedQuery::Delete { key };
                #query_kinds::#table_name(query)
            }
        }
    });
}

/// Extend a token stream with an implementation for converting an exists into a query kind
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `table_name` - The name of the table
/// * `query_name` - The name of the query kinds enum
fn add_exists(stream: &mut proc_macro2::TokenStream, table_name: &Ident, query_kinds: &Ident) {
    // build the name for our exists query
    let exists_name = format_ident!("{}Exists", table_name);
    // extend our token stream with an impl to turn an exists query into a query kind
    stream.extend(quote! {
        #[automatically_derived]
        impl From<#exists_name> for #query_kinds {
            /// Build a `QueryKind` for checking if rows exist
            fn from(specific: #exists_name) -> Self {
                // build the partition key by hashing the key
                let partition_key =
                    <#table_name as shoal_core::shared::traits::PartitionKeySupport>::get_partition_key_from_values(&specific.partition_key);
                // build the general query
                let general = shoal_core::shared::queries::UnsortedExists {
                    partition_key,
                    filters: specific.filters,
                };
                // build our query kind
                Self::#table_name(shoal_core::shared::queries::UnsortedQuery::Exists(general))
            }
        }
    });
}

/// Extend a token stream with a From<#name> for *UnsortedQueryKinds implementation
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `table_name` - The name of the type/table we are extending
/// * `query_name` - The name of the query type
pub fn add(stream: &mut proc_macro2::TokenStream, table_name: &Ident, query_kinds: &Ident) {
    // add the different query kind conversion implementations
    add_insert(stream, table_name, query_kinds);
    add_get(stream, table_name, query_kinds);
    add_update(stream, table_name, query_kinds);
    add_delete(stream, table_name, query_kinds);
    add_exists(stream, table_name, query_kinds);
}
