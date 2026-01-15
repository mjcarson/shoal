//! Derives the different traits needed for shoal

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DataStruct, Fields, FieldsNamed, Ident};

use super::utils;

/// Extend a token stream with a PartitionKeySuppport for #name implementation
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `query_name` - The name of the query type
pub(super) fn add_partition_key_support(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    partition_fields: &[(syn::Ident, syn::Type)],
) {
    // get this tables name
    let table_name = name.to_string();
    // Build the partition key type
    let partition_key_type = if partition_fields.len() == 1 {
        // we have a single partition field so we can just use that type
        let (_, ty) = &partition_fields[0];
        quote! { #ty }
    } else {
        // we have multiple partition fields so well need to wrap it in a tuple
        let types: Vec<_> = partition_fields.iter().map(|(_, ty)| ty).collect();
        quote! { (#(#types),*) }
    };
    // Build the arguments to pass to get_partition_key_from_values
    let partition_key_args = if partition_fields.len() == 1 {
        // get our single partition field ident
        let (ident, _) = &partition_fields[0];
        // we only need to pass a single argument
        quote! { &self.#ident }
    } else {
        // get the idents for all of our partition fields
        let idents: Vec<_> = partition_fields.iter().map(|(ident, _)| ident).collect();
        // we have multiple fields to pass
        quote! { &(#(&self.#idents),*) }
    };
    // Build hash statements for get_partition_key_from_values
    let hash_values_stmts: Vec<_> = if partition_fields.len() == 1 {
        vec![quote! {
            Self::hash_field(&mut hasher, values);
        }]
    } else {
        partition_fields
            .iter()
            .enumerate()
            .map(|(idx, _)| {
                let index = syn::Index::from(idx);
                quote! {
                    Self::hash_field(&mut hasher, &values.#index);
                }
            })
            .collect()
    };
    // Build hash statements for archived values
    let hash_archived_stmts: Vec<_> = partition_fields
        .iter()
        .map(|(ident, ty)| {
            // Handle archived rkyv types
            if utils::is_string_type(ty) {
                quote! {
                    hasher.write(intent.#ident.as_bytes());
                }
            } else if utils::is_u64_type(ty) {
                quote! {
                    hasher.write_u64(intent.#ident.to_native());
                }
            } else if utils::is_u32_type(ty) {
                quote! {
                    hasher.write_u32(intent.#ident.to_native());
                }
            } else {
                // For other types, assume they have to_native()
                quote! {
                    hasher.write_u64(intent.#ident.to_native() as u64);
                }
            }
        })
        .collect();
    // extend our token stream
    stream.extend(quote! {
        #[automatically_derived]
        impl PartitionKeySupport for #name {
            /// The partition key type for this data
            type PartitionKey = #partition_key_type;

            /// The name of this table
            #[inline]
            fn name() -> &'static str {
                #table_name
            }

            /// Calculate the partition key for this row
            fn get_partition_key(&self) -> u64 {
                Self::get_partition_key_from_values(#partition_key_args)
            }

            /// Calculate the partition key for this row
            ///
            /// # Arguments
            ///
            /// * `values` - The values to hash to generate our partition key
            #[inline]
            fn get_partition_key_from_values(values: &Self::PartitionKey) -> u64 {
                let mut hasher = GxHasher::default();
                #(#hash_values_stmts)*
                hasher.finish()
            }

            /// Get the partition key for this row from an archived value
            ///
            /// # Arguments
            ///
            /// * `intent` - The intent to get a partition key from
            fn get_partition_key_from_archived_insert(intent: &<Self as Archive>::Archived) -> u64 {
                let mut hasher = GxHasher::default();
                #(#hash_archived_stmts)*
                hasher.finish()
            }
        }

        #[automatically_derived]
        impl #name {
            // Helper method to hash different field types
            fn hash_field<H: std::hash::Hasher, T: std::hash::Hash>(hasher: &mut H, value: &T) {
                value.hash(hasher);
            }
        }
    });
}

/// Extend a token stream with a FromShoal implementation
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `db_name` - The name of the database
/// * `response_name` - The name of the response type
pub(super) fn add_from_shoal(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    db_name: &Ident,
    response_name: &Ident,
) {
    // extend our token stream
    stream.extend(
        quote! {
            #[automatically_derived]
            impl shoal_core::FromShoal<#db_name> for #name  {
                type ResponseKinds = <#db_name as shoal_core::shared::traits::QuerySupport>::ResponseKinds;

                fn retrieve(archived: &#response_name) -> Result<&rkyv::option::ArchivedOption<rkyv::vec::ArchivedVec<<Self as Archive>::Archived>>, shoal_core::client::Errors> {
                    // make sure its the right data kind
                    if let #response_name::#name(action) = archived {
                        // make sure its a get action
                       if let shoal_core::shared::responses::ArchivedResponseAction::Get(rows) = &action.data {
                            return Ok(rows);
                        }
                    }
                    Err(shoal_core::client::Errors::WrongType("Wrong Type!".to_owned()))
                }                
            }
        }
    );
}

/// Extend a token stream with a RkyvSupport impl
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
pub(super) fn add_rkyv_support(stream: &mut proc_macro2::TokenStream, name: &Ident) {
    // extend our token stream
    stream.extend(quote! {
        #[automatically_derived]
        impl shoal_core::shared::traits::RkyvSupport for #name {}
    });
}

/// Extend a token stream with a From<#name> for *SortedQueryKinds implementation
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `query_name` - The name of the query type
pub(super) fn add_from_for_sorted_query(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    query_name: &Ident,
) {
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
pub(super) fn add_from_for_unsorted_query(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    query_name: &Ident,
) {
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

///// Extend a token stream with a FromShoal implementation
/////
///// # Arguments
/////
///// * `stream` - The stream to extend
///// * `name` - The name of the type we are extending
///// * `table_name` - The name of the table
///// * `response_name` - The name of the response type
//fn add_shoal_table(stream: &mut proc_macro2::TokenStream, name: &Ident) {
//    // extend our token stream
//    stream.extend(quote! {
//        #[automatically_derived]
//        impl ShoalTable for #name {
//            /// The sort type for this data
//            type Sort = String;
//
//            /// Build the sort tuple for this row
//            fn get_sort(&self) -> &Self::Sort {
//                &self.key
//            }
//
//            /// Calculate the partition key for this row
//            fn partition_key(sort: &Self::Sort) -> u64 {
//                // create a new hasher
//                let mut hasher = GxHasher::default();
//                // hash the first key
//                hasher.write(sort.as_bytes());
//                // get our hash
//                hasher.finish()
//            }
//
//            /// Any filters to apply when listing/crawling rows
//            type Filters = String;
//
//            /// Determine if a row should be filtered
//            ///
//            /// # Arguments
//            ///
//            /// * `filters` - The filters to apply
//            /// * `row` - The row to filter
//            fn is_filtered(filter: &Self::Filters, row: &Self) -> bool {
//                &row.value == filter
//            }
//        }
//    });
//}

