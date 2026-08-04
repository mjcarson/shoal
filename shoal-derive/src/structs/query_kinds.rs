//! Generate QueryKinds and ResponseKinds enums for a Shoal database

use quote::{format_ident, quote};
use syn::{FieldsNamed, Ident};

use crate::{tables::TableKinds, utils};

/// Information about a table field needed for code generation
struct TableInfo {
    /// The variant name in PascalCase
    variant_ident: Ident,
    /// The inner data type
    inner_type: Ident,
    /// The kind of table this is
    kind: TableKinds,
}

/// Extract table information from fields
fn extract_table_info(fields: &FieldsNamed) -> Vec<TableInfo> {
    fields
        .named
        .iter()
        .map(|field| {
            // get the ident and type for this field
            let field_ident = match &field.ident {
                Some(field_ident) => field_ident,
                None => panic!("Field in shoal db is missing an ident: {field:?}"),
            };
            let field_type = &field.ty;
            // Extract the inner type from the table wrapper
            let inner_type = match utils::extract_inner_table_ident(field_type) {
                Some(inner_type) => inner_type,
                None => panic!("Failed to extract inner table ident: {field_type:?}"),
            };
            // deteremine what kind of table this field is using
            let kind = TableKinds::new(field_type);
            // Use the inner type name as the variant name
            let variant_ident = inner_type.clone();
            // build our table info object
            TableInfo {
                variant_ident,
                inner_type,
                kind,
            }
        })
        .collect()
}

/// Generate the QueryKinds and ResponseKinds enums and their trait implementations
///
/// A projection answers in its own response variant rather than sharing its tables one, so
/// that the rows a projected get comes back with have a type the client can name. That is also
/// why every arm below is built over the tables and their projections together: a variant the
/// merge or the reorder does not cover would silently drop a projected gets rows.
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `struct_ident` - The name of the database these queries are for
/// * `fields` - The tables in this database
/// * `projections` - The projections each of those tables declared
pub fn add(
    stream: &mut proc_macro2::TokenStream,
    struct_ident: &Ident,
    fields: &FieldsNamed,
    projections: &[Vec<Ident>],
) {
    // extract the info for all tables in this db
    let tables = extract_table_info(fields);
    // every projection of every table, which each answer in a variant of their own
    let projected: Vec<&Ident> = projections.iter().flatten().collect();
    // build our ident for the structs we need to use/generate
    let query_ident = format_ident!("{}QueryKinds", struct_ident);
    let response_ident = format_ident!("{}ResponseKinds", struct_ident);
    let archived_response_ident = format_ident!("Archived{}ResponseKinds", struct_ident);
    // Generate QueryKinds enum variants
    let query_variants = tables.iter().map(|table| {
        // get the ident and type for this table
        let variant = &table.variant_ident;
        let inner = &table.inner_type;
        // get the correct query type for this table kind
        match table.kind {
            TableKinds::Unsorted => {
                // use the variant for unsorted tables
                quote! {
                    #variant(shoal_core::shared::queries::UnsortedQuery<#inner>)
                }
            }
            TableKinds::Sorted => {
                // use the variant for sorted tables
                quote! {
                    #variant(shoal_core::shared::queries::SortedQuery<#inner>)
                }
            }
        }
    });
    // Generate ResponseKinds enum variants
    let response_variants = tables
        .iter()
        .map(|table| {
            let variant = &table.variant_ident;
            let inner = &table.inner_type;
            quote! {
                #variant(shoal_core::shared::responses::Response<#inner>)
            }
        })
        // a projection answers in a variant named after itself, holding its own rows
        .chain(projected.iter().map(|projection| {
            quote! {
                #projection(shoal_core::shared::responses::Response<#projection>)
            }
        }));
    // Generate response_query_id match arms
    let response_query_id_arms = tables
        .iter()
        .map(|table| table.variant_ident.clone())
        // a projections rows are answered with the same way a rows are
        .chain(projected.iter().map(|projection| (*projection).clone()))
        .map(|variant| {
            quote! {
                #archived_response_ident::#variant(resp) => Ok(&resp.id)
            }
        });
    // Generate split_by_shard match arms
    //
    // each table splits its own query, so the narrowed queries that comes back have to
    // be wrapped back up in the variant they came out of
    let split_by_shard_arms = tables.iter().map(|table| {
        let variant = &table.variant_ident;
        quote! {
            #query_ident::#variant(query) => {
                // split this tables query up by shard
                let mut split = Vec::default();
                query.split_by_shard(ring, &mut split);
                // wrap each narrowed query back up in the variant it came from
                for (shard, narrowed) in split {
                    found.push((shard, #query_ident::#variant(narrowed)));
                }
            }
        }
    });
    // Generate limit match arms
    let limit_arms = tables.iter().map(|table| {
        let variant = &table.variant_ident;
        quote! {
            #query_ident::#variant(query) => query.limit()
        }
    });
    // Generate partition_keys match arms
    let partition_keys_arms = tables.iter().map(|table| {
        let variant = &table.variant_ident;
        quote! {
            #query_ident::#variant(query) => query.partition_keys()
        }
    });
    // Generate order_by_partitions match arms
    //
    // every row knows the partition it came from, whichever kind of table it is, so both
    // kinds are reordered the same way
    let order_by_partitions_arms = tables
        .iter()
        .map(|table| table.variant_ident.clone())
        // a projections rows are answered with the same way a rows are
        .chain(projected.iter().map(|projection| (*projection).clone()))
        .map(|variant| {
            quote! {
                #response_ident::#variant(response) => response.order_by_partitions(order)
            }
        });
    // Generate merge match arms
    //
    // both shares answer the same query, so they are always the same variant
    let merge_arms = tables
        .iter()
        .map(|table| table.variant_ident.clone())
        // every shard answers a projected get with the same projection, so its shares are
        // always the same variant too
        .chain(projected.iter().map(|projection| (*projection).clone()))
        .map(|variant| {
            quote! {
                (#response_ident::#variant(ours), #response_ident::#variant(theirs)) => {
                    ours.merge(theirs)
                }
            }
        });
    // Generate truncate match arms
    let truncate_arms = tables
        .iter()
        .map(|table| table.variant_ident.clone())
        // a projections rows are answered with the same way a rows are
        .chain(projected.iter().map(|projection| (*projection).clone()))
        .map(|variant| {
            quote! {
                #response_ident::#variant(response) => response.truncate(limit)
            }
        });
    // Generate get_index_archived match arms
    let get_index_arms = tables
        .iter()
        .map(|table| table.variant_ident.clone())
        // a projections rows are answered with the same way a rows are
        .chain(projected.iter().map(|projection| (*projection).clone()))
        .map(|variant| {
            quote! {
                #archived_response_ident::#variant(resp) => resp.index.to_native() as usize
            }
        });
    // Generate is_end_of_stream match arms
    let is_end_of_stream_arms = tables
        .iter()
        .map(|table| table.variant_ident.clone())
        // a projections rows are answered with the same way a rows are
        .chain(projected.iter().map(|projection| (*projection).clone()))
        .map(|variant| {
            quote! {
                #archived_response_ident::#variant(resp) => resp.end
            }
        });
    // Generate get_query_id match arms
    let get_query_id_arms = tables
        .iter()
        .map(|table| table.variant_ident.clone())
        // a projections rows are answered with the same way a rows are
        .chain(projected.iter().map(|projection| (*projection).clone()))
        .map(|variant| {
            quote! {
                #archived_response_ident::#variant(resp) => resp.id.to_owned()
            }
        });

    // Generate the enums and trait implementations
    stream.extend(quote! {
        /// The different tables we can query
        #[derive(Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone)]
        pub enum #query_ident {
            #(#query_variants),*
        }

        /// The different tables we can get responses from
        #[derive(Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
        pub enum #response_ident {
            #(#response_variants),*
        }

        #[automatically_derived]
        impl shoal_core::shared::traits::RkyvSupport for #query_ident {}

        #[automatically_derived]
        impl shoal_core::shared::traits::ShoalQuerySupport for #query_ident {
            /// Deserialize our response types
            ///
            /// # Arguments
            ///
            /// * `buff` - The buffer to deserialize into a response
            fn response_query_id(buff: &[u8]) -> Result<&uuid::Uuid, rkyv::rancor::Error> {
                let archive = <#response_ident as shoal_core::shared::traits::RkyvSupport>::access(buff)?;
                match archive {
                    #(#response_query_id_arms),*
                }
            }

            /// Split this query into the per shard queries that answer it
            ///
            /// # Arguments
            ///
            /// * `ring` - The shard ring to check against
            /// * `found` - The per shard queries we found for this query
            fn split_by_shard<'a>(
                &self,
                ring: &'a shoal_core::server::ring::Ring,
                found: &mut Vec<(&'a shoal_core::server::shard::ShardInfo, Self)>,
            ) {
                match &self {
                    #(#split_by_shard_arms),*
                }
            }

            /// Get the most rows this query asked for, if it set a limit
            fn limit(&self) -> Option<usize> {
                match &self {
                    #(#limit_arms),*
                }
            }

            /// Get the partitions this query named, in the order it named them
            fn partition_keys(&self) -> &[u64] {
                match &self {
                    #(#partition_keys_arms),*
                }
            }
        }

        #[automatically_derived]
        impl shoal_core::shared::traits::RkyvSupport for #response_ident {}

        #[automatically_derived]
        impl shoal_core::shared::traits::ShoalResponseSupport for #response_ident {
            /// Get the index of a single response
            fn get_index_archived(archived: &<Self as rkyv::Archive>::Archived) -> usize {
                match archived {
                    #(#get_index_arms),*
                }
            }

            /// Get whether this is the last response in a response stream
            fn is_end_of_stream(archived: &<Self as rkyv::Archive>::Archived) -> bool {
                match archived {
                    #(#is_end_of_stream_arms),*
                }
            }

            /// Get the query id from the response
            fn get_query_id(archived: &<Self as rkyv::Archive>::Archived) -> uuid::Uuid {
                match archived {
                    #(#get_query_id_arms),*
                }
            }

            /// Merge another shards share of one queries answer into this one
            ///
            /// # Arguments
            ///
            /// * `other` - The other shards share of this queries answer
            fn merge(&mut self, other: Self) {
                match (self, other) {
                    #(#merge_arms),*,
                    // a query is only ever routed to one table, so two shares of one
                    // answer are always the same variant
                    _ => (),
                }
            }

            /// Put our rows back into the order the query named their partitions in
            ///
            /// # Arguments
            ///
            /// * `order` - The partitions this query named, in the order it named them
            fn order_by_partitions(&mut self, order: &[u64]) {
                match self {
                    #(#order_by_partitions_arms),*
                }
            }

            /// Drop any rows past this queries limit
            ///
            /// # Arguments
            ///
            /// * `limit` - The most rows this query asked for
            fn truncate(&mut self, limit: usize) {
                match self {
                    #(#truncate_arms),*
                }
            }
        }
    });
}
