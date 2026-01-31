//! Generate a struct for this databases client

use quote::{format_ident, quote};
use syn::{FieldsNamed, Ident};

use crate::{tables::TableKinds, utils};

/// Information about a table needed for code generation
struct TableInfo {
    /// The variant name in PascalCase
    variant_ident: Ident,
    /// The inner data type (e.g., Movie, MoviesByKeyword)
    inner_type: Ident,
    /// The kind of table (sorted or unsorted)
    kind: TableKinds,
}

/// Extract table information from fields
fn extract_table_info(fields: &FieldsNamed) -> Vec<TableInfo> {
    fields
        .named
        .iter()
        .map(|field| {
            let field_ident = field.ident.as_ref().expect("Field must have ident");
            let field_type = &field.ty;
            let inner_type = utils::extract_inner_table_ident(field_type)
                .expect("Failed to extract inner table ident");
            let kind = TableKinds::new(field_type);
            let variant_name = utils::snake_to_pascal_case(&field_ident.to_string());
            let variant_ident = format_ident!("{}", variant_name);
            TableInfo {
                variant_ident,
                inner_type,
                kind,
            }
        })
        .collect()
}

// Add a client for this database
pub fn add(stream: &mut proc_macro2::TokenStream, struct_ident: &Ident, fields: &FieldsNamed) {
    // extract the info for all tables in this db
    let tables = extract_table_info(fields);
    // build our new idents
    let client_ident = format_ident!("{}Client", struct_ident);
    let query_ident = format_ident!("{struct_ident}QueryKinds");
    let response_ident = format_ident!("{struct_ident}ResponseKinds");
    let archived_response_ident = format_ident!("Archived{struct_ident}ResponseKinds");
    // build our succeeded response arms
    let succeeded_arms = tables.iter().map(|table| {
        let variant_ident = &table.variant_ident;
        quote! {
            #archived_response_ident::#variant_ident(response)=> response.succeeded(opts),
        }
    });
    // build our kind arms
    let kind_arms = tables.iter().map(|table| {
        let variant_ident = &table.variant_ident;
        quote! {
            #archived_response_ident::#variant_ident(response)=> response.kind(),
        }
    });
    // build our get_exists arms
    let get_exists_arms = tables.iter().map(|table| {
        let variant_ident = &table.variant_ident;
        quote! {
            #archived_response_ident::#variant_ident(response)=> response.get_exists(),
        }
    });
    // build our parse arms for each table
    let parse_arms = tables.iter().map(|table| {
        let variant_ident = &table.variant_ident;
        let inner_type = &table.inner_type;
        // The table name string to match against (the struct name)
        let table_name_str = inner_type.to_string();
        // Build the get struct name
        let get_ident = format_ident!("{}Get", inner_type);

        match table.kind {
            TableKinds::Unsorted => {
                quote! {
                    #table_name_str => {
                        // Type check all conditions against this table's schema
                        for condition in &parsed.conditions {
                            // Validate field exists
                            let _role = <#inner_type as shoal_core::shared::traits::TableSchemaSupport>::get_field_role(&condition.field)
                                .ok_or_else(|| shoal_core::client::ShqlParseError::new(
                                    format!(
                                        "Unknown field '{}'. Valid fields are: {:?}",
                                        condition.field,
                                        <#inner_type as shoal_core::shared::traits::TableSchemaSupport>::field_names()
                                    ),
                                    condition.value_start,
                                    condition.value_end,
                                    query,
                                ))?;
                            // Validate field type
                            let validator = <#inner_type as shoal_core::shared::traits::TableSchemaSupport>::get_field_validator(&condition.field)
                                .ok_or_else(|| shoal_core::client::ShqlParseError::new(
                                    format!("No validator for field '{}'", condition.field),
                                    condition.value_start,
                                    condition.value_end,
                                    query,
                                ))?;
                            validator(&condition.value).map_err(|err| {
                                shoal_core::client::ShqlParseError::new(
                                    format!("Type mismatch for field '{}': {}", condition.field, err),
                                    condition.value_start,
                                    condition.value_end,
                                    query,
                                )
                            })?;
                        }
                        // Extract partition key from conditions
                        let partition_key = parsed.conditions.iter()
                            .find(|c| {
                                <#inner_type as shoal_core::shared::traits::TableSchemaSupport>::get_field_role(&c.field)
                                    == Some(shoal_core::shared::queries::parser::FieldRole::Partition)
                            })
                            .ok_or_else(|| shoal_core::client::ShqlParseError::new(
                                "Missing partition key in WHERE clause".to_string(),
                                0,
                                query.len(),
                                query,
                            ))?;
                        // Deserialize the partition key value
                        let partition_value = shoal_core::serde_json::from_value(partition_key.value.clone())
                            .map_err(|e| shoal_core::client::ShqlParseError::new(
                                format!("Failed to deserialize partition key: {}", e),
                                partition_key.value_start,
                                partition_key.value_end,
                                query,
                            ))?;
                        // Build the Get query
                        let mut get_query = #get_ident::new(partition_value);
                        if let Some(limit) = parsed.limit {
                            get_query.limit = Some(limit);
                        }
                        // Wrap in UnsortedQuery::Get and then in QueryKinds
                        let unsorted_query = shoal_core::shared::queries::UnsortedQuery::Get(
                            shoal_core::shared::queries::UnsortedGet {
                                partition_key: <#inner_type as shoal_core::shared::traits::PartitionKeySupport>::get_partition_key_from_values(&get_query.partition_key),
                                filters: get_query.filters,
                                limit: get_query.limit,
                            }
                        );
                        Ok(#query_ident::#variant_ident(unsorted_query))
                    }
                }
            }
            TableKinds::Sorted => {
                quote! {
                    #table_name_str => {
                        // Type check all conditions against this table's schema
                        for condition in &parsed.conditions {
                            // Validate field exists
                            let _role = <#inner_type as shoal_core::shared::traits::TableSchemaSupport>::get_field_role(&condition.field)
                                .ok_or_else(|| shoal_core::client::ShqlParseError::new(
                                    format!(
                                        "Unknown field '{}'. Valid fields are: {:?}",
                                        condition.field,
                                        <#inner_type as shoal_core::shared::traits::TableSchemaSupport>::field_names()
                                    ),
                                    condition.value_start,
                                    condition.value_end,
                                    query,
                                ))?;
                            // Validate field type
                            let validator = <#inner_type as shoal_core::shared::traits::TableSchemaSupport>::get_field_validator(&condition.field)
                                .ok_or_else(|| shoal_core::client::ShqlParseError::new(
                                    format!("No validator for field '{}'", condition.field),
                                    condition.value_start,
                                    condition.value_end,
                                    query,
                                ))?;
                            validator(&condition.value).map_err(|err| {
                                shoal_core::client::ShqlParseError::new(
                                    format!("Type mismatch for field '{}': {}", condition.field, err),
                                    condition.value_start,
                                    condition.value_end,
                                    query,
                                )
                            })?;
                        }
                        // Extract partition keys from conditions
                        let partition_conditions: Vec<_> = parsed.conditions.iter()
                            .filter(|c| {
                                <#inner_type as shoal_core::shared::traits::TableSchemaSupport>::get_field_role(&c.field)
                                    == Some(shoal_core::shared::queries::parser::FieldRole::Partition)
                            })
                            .collect();
                        if partition_conditions.is_empty() {
                            return Err(shoal_core::client::ShqlParseError::new(
                                "Missing partition key in WHERE clause".to_string(),
                                0,
                                query.len(),
                                query,
                            ));
                        }
                        // Build partition keys vec
                        let mut partition_keys = Vec::with_capacity(partition_conditions.len());
                        for pc in &partition_conditions {
                            let value = shoal_core::serde_json::from_value(pc.value.clone())
                                .map_err(|e| shoal_core::client::ShqlParseError::new(
                                    format!("Failed to deserialize partition key: {}", e),
                                    pc.value_start,
                                    pc.value_end,
                                    query,
                                ))?;
                            partition_keys.push(value);
                        }
                        // Extract sort keys from conditions (if any)
                        let sort_conditions: Vec<_> = parsed.conditions.iter()
                            .filter(|c| {
                                <#inner_type as shoal_core::shared::traits::TableSchemaSupport>::get_field_role(&c.field)
                                    == Some(shoal_core::shared::queries::parser::FieldRole::Sort)
                            })
                            .collect();
                        let mut sort_keys = Vec::with_capacity(sort_conditions.len());
                        for sc in &sort_conditions {
                            let value = shoal_core::serde_json::from_value(sc.value.clone())
                                .map_err(|e| shoal_core::client::ShqlParseError::new(
                                    format!("Failed to deserialize sort key: {}", e),
                                    sc.value_start,
                                    sc.value_end,
                                    query,
                                ))?;
                            sort_keys.push(value);
                        }
                        // Build the Get query
                        let mut get_query = #get_ident::new(partition_keys.clone());
                        get_query.sort_keys = sort_keys.clone();
                        if let Some(limit) = parsed.limit {
                            get_query.limit = Some(limit);
                        }
                        // Build partition key hashes
                        let partition_key_hashes: Vec<u64> = partition_keys.iter()
                            .map(|pk| <#inner_type as shoal_core::shared::traits::PartitionKeySupport>::get_partition_key_from_values(pk))
                            .collect();
                        // Wrap in SortedQuery::Get and then in QueryKinds
                        let sorted_query = shoal_core::shared::queries::SortedQuery::Get(
                            shoal_core::shared::queries::SortedGet {
                                partition_keys: partition_key_hashes,
                                sort_keys: get_query.sort_keys,
                                filters: get_query.filters,
                                limit: get_query.limit,
                            }
                        );
                        Ok(#query_ident::#variant_ident(sorted_query))
                    }
                }
            }
        }
    });
    // add our client struct and query support for the client
    stream.extend(quote! {
        pub struct #client_ident {}

        impl shoal_core::shared::traits::QuerySupport for #client_ident {
            /// The different tables or types of queries we will handle
            type QueryKinds = #query_ident;

            /// The different tables we can get responses from
            type ResponseKinds = #response_ident;

            /// Make sure queries have succeeded based on some critiera
            ///
            /// # Arguments
            ///
            /// * `opts` - The options to use when validating query responses
            fn succeeded(
                archived: &<Self::ResponseKinds as rkyv::Archive>::Archived,
                opts: shoal_core::client::QuerySuceededOpts,
            ) -> Result<(), shoal_core::client::Errors> {
                match archived {
                    #(#succeeded_arms)*
                }
            }

            /// Get the kind of query this is a response to
            ///
            /// # Arguments
            ///
            /// * `archived` - The archived query to get the query kind for
            fn kind(archived: &<Self::ResponseKinds as rkyv::Archive>::Archived) -> shoal_core::shared::responses::ResponseActionNames {
                match archived {
                    #(#kind_arms)*
                }
            }

            /// Get the exists result from an Exists response
            ///
            /// # Arguments
            ///
            /// * `archived` - The archived response to get the exists result from
            fn get_exists(archived: &<Self::ResponseKinds as rkyv::Archive>::Archived) -> Option<bool> {
                match archived {
                    #(#get_exists_arms)*
                }
            }

            /// Parse a SHQL query string into a QueryKinds
            ///
            /// # Arguments
            ///
            /// * `query` - The SHQL query string to parse
            fn parse(query: &str) -> Result<Self::QueryKinds, shoal_core::client::ShqlParseError> {
                // Parse the query string
                let parsed = shoal_core::shared::queries::parser::ParsedSelect::new(query)?;
                // Match on the table name
                match parsed.table_name.as_str() {
                    #(#parse_arms)*
                    _ => Err(shoal_core::client::ShqlParseError::new(
                        format!("Unknown table '{}'", parsed.table_name),
                        0,
                        query.len(),
                        query,
                    )),
                }
            }
        }
    });
}
