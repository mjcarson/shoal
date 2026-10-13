//! Generate the conditional write implementations for a table's writes
//!
//! A row, its `Update` and its `Delete` can each be made conditional on the row stored under
//! their key ([F68](../../../docs/src/features/conditional-writes.md)). The table derive knows
//! how each one's keys are hashed, so it is what turns a write into the table's own form of it;
//! `#[shoal::db]` only wraps that in its query kinds.

use quote::{format_ident, quote};
use syn::Ident;

/// Extend a token stream with the conditional write implementations for an unsorted table
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the table's row type
pub fn add_unsorted(stream: &mut proc_macro2::TokenStream, name: &Ident) {
    // build the names of this tables update, update data and delete structs
    let update_name = format_ident!("{}Update", name);
    let update_data_name = format_ident!("{}UpdateData", name);
    let delete_name = format_ident!("{}Delete", name);
    // extend our token stream with an implementation for each of the three writes
    stream.extend(quote! {
        #[automatically_derived]
        impl ::shoal::shared::queries::ConditionalWrite for #name {
            type Table = #name;
            type Write = ::shoal::shared::queries::UnsortedWrite<#name>;

            /// Turn this row into an insert of it, beside its hashed partition key
            fn into_write(self) -> Self::Write {
                // hash this rows partition key the way a plain insert does
                let key = <#name as ::shoal::shared::traits::PartitionKeySupport>::get_partition_key(&self);
                ::shoal::shared::queries::UnsortedWrite::Insert { key, row: self }
            }
        }

        #[automatically_derived]
        impl ::shoal::shared::queries::ConditionalInsert for #name {}

        #[automatically_derived]
        impl ::shoal::shared::queries::ConditionalWrite for #update_name {
            type Table = #name;
            type Write = ::shoal::shared::queries::UnsortedWrite<#name>;

            /// Turn this update into the table's own update, hashing its partition key
            fn into_write(self) -> Self::Write {
                // hash the partition key to get the u64 key
                let partition_key = <#name as ::shoal::shared::traits::PartitionKeySupport>::get_partition_key_from_values(&self.partition_key);
                // keep just the values this update sets
                let update = #update_data_name::from(self);
                ::shoal::shared::queries::UnsortedWrite::Update(
                    ::shoal::shared::queries::UnsortedUpdate { partition_key, update },
                )
            }
        }

        #[automatically_derived]
        impl ::shoal::shared::queries::ConditionalWrite for #delete_name {
            type Table = #name;
            type Write = ::shoal::shared::queries::UnsortedWrite<#name>;

            /// Turn this delete into the table's own delete, hashing its partition key
            fn into_write(self) -> Self::Write {
                // hash the partition key to get the u64 key
                let partition_key = <#name as ::shoal::shared::traits::PartitionKeySupport>::get_partition_key_from_values(&self.partition_key);
                ::shoal::shared::queries::UnsortedWrite::Delete { partition_key }
            }
        }
    });
}

/// Extend a token stream with the conditional write implementations for a sorted table
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the table's row type
pub fn add_sorted(stream: &mut proc_macro2::TokenStream, name: &Ident) {
    // build the names of this tables update and delete structs
    let update_name = format_ident!("{}Update", name);
    let delete_name = format_ident!("{}Delete", name);
    // extend our token stream with an implementation for each of the three writes
    stream.extend(quote! {
        #[automatically_derived]
        impl ::shoal::shared::queries::ConditionalWrite for #name {
            type Table = #name;
            type Write = ::shoal::shared::queries::SortedWrite<#name>;

            /// Turn this row into an insert of it, beside its hashed partition key
            fn into_write(self) -> Self::Write {
                // hash this rows partition key the way a plain insert does
                let key = <#name as ::shoal::shared::traits::PartitionKeySupport>::get_partition_key(&self);
                ::shoal::shared::queries::SortedWrite::Insert { key, row: self }
            }
        }

        #[automatically_derived]
        impl ::shoal::shared::queries::ConditionalInsert for #name {}

        #[automatically_derived]
        impl ::shoal::shared::queries::ConditionalWrite for #update_name {
            type Table = #name;
            type Write = ::shoal::shared::queries::SortedWrite<#name>;

            /// Turn this update into the table's own update, hashing its partition key
            fn into_write(self) -> Self::Write {
                // hash the partition key to get the u64 key
                let partition_key = <#name as ::shoal::shared::traits::PartitionKeySupport>::get_partition_key_from_values(&self.partition_key);
                // split the row's sort key from the values this update sets
                let (sort_key, update) = self.into_update_parts();
                ::shoal::shared::queries::SortedWrite::Update(
                    ::shoal::shared::queries::SortedUpdate { partition_key, sort_key, update },
                )
            }
        }

        #[automatically_derived]
        impl ::shoal::shared::queries::ConditionalWrite for #delete_name {
            type Table = #name;
            type Write = ::shoal::shared::queries::SortedWrite<#name>;

            /// Turn this delete into the table's own delete, hashing its partition key
            fn into_write(self) -> Self::Write {
                // hash the partition key to get the u64 key
                let partition_key = <#name as ::shoal::shared::traits::PartitionKeySupport>::get_partition_key_from_values(&self.partition_key);
                ::shoal::shared::queries::SortedWrite::Delete {
                    partition_key,
                    sort_key: self.sort_key,
                }
            }
        }
    });
}
