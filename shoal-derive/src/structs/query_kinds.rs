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
pub fn add(stream: &mut proc_macro2::TokenStream, struct_ident: &Ident, fields: &FieldsNamed) {
    // extract the info for all tables in this db
    let tables = extract_table_info(fields);
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
    let response_variants = tables.iter().map(|table| {
        let variant = &table.variant_ident;
        let inner = &table.inner_type;
        quote! {
            #variant(shoal_core::shared::responses::Response<#inner>)
        }
    });
    // Generate response_query_id match arms
    let response_query_id_arms = tables.iter().map(|table| {
        let variant = &table.variant_ident;
        quote! {
            #archived_response_ident::#variant(resp) => Ok(&resp.id)
        }
    });
    // Generate find_shard match arms
    let find_shard_arms = tables.iter().map(|table| {
        let variant = &table.variant_ident;
        quote! {
            #query_ident::#variant(query) => query.find_shard(ring, found)
        }
    });
    // Generate get_index_archived match arms
    let get_index_arms = tables.iter().map(|table| {
        let variant = &table.variant_ident;
        quote! {
            #archived_response_ident::#variant(resp) => resp.index.to_native() as usize
        }
    });
    // Generate is_end_of_stream match arms
    let is_end_of_stream_arms = tables.iter().map(|table| {
        let variant = &table.variant_ident;
        quote! {
            #archived_response_ident::#variant(resp) => resp.end
        }
    });
    // Generate get_query_id match arms
    let get_query_id_arms = tables.iter().map(|table| {
        let variant = &table.variant_ident;
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

            /// Find the right shards for this query
            ///
            /// # Arguments
            ///
            /// * `ring` - The shard ring to check against
            /// * `found` - The shards we found for this query
            fn find_shard<'a>(
                &self,
                ring: &'a shoal_core::server::ring::Ring,
                found: &mut Vec<&'a shoal_core::server::shard::ShardInfo>,
            ) {
                match &self {
                    #(#find_shard_arms),*
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
        }
    });
}
