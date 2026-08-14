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
            let variant_ident = utils::extract_inner_table_ident(field_type)
                .expect("Failed to extract inner table ident for variant");
            TableInfo {
                variant_ident,
                inner_type,
                kind,
            }
        })
        .collect()
}

/// Add a client for this database
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `struct_ident` - The name of the database this client is for
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
    // build our new idents
    let client_ident = format_ident!("{}Client", struct_ident);
    let query_ident = format_ident!("{struct_ident}QueryKinds");
    let response_ident = format_ident!("{struct_ident}ResponseKinds");
    let archived_response_ident = format_ident!("Archived{struct_ident}ResponseKinds");
    // build our succeeded response arms
    let succeeded_arms = tables
        .iter()
        .map(|table| table.variant_ident.clone())
        // a projected get answers in a variant of its own, which reads the same way
        .chain(projected.iter().map(|projection| (*projection).clone()))
        .map(|variant_ident| {
            quote! {
                #archived_response_ident::#variant_ident(response)=> response.succeeded(opts),
            }
        });
    // build our kind arms
    let kind_arms = tables
        .iter()
        .map(|table| table.variant_ident.clone())
        // a projected get answers in a variant of its own, which reads the same way
        .chain(projected.iter().map(|projection| (*projection).clone()))
        .map(|variant_ident| {
            quote! {
                #archived_response_ident::#variant_ident(response)=> response.kind(),
            }
        });
    // build our get_exists arms
    let get_exists_arms = tables
        .iter()
        .map(|table| table.variant_ident.clone())
        // a projected get answers in a variant of its own, which reads the same way
        .chain(projected.iter().map(|projection| (*projection).clone()))
        .map(|variant_ident| {
            quote! {
                #archived_response_ident::#variant_ident(response)=> response.get_exists(),
            }
        });
    // build our error arms
    let error_arms = tables
        .iter()
        .map(|table| table.variant_ident.clone())
        // a projected get answers in a variant of its own, which reads the same way
        .chain(projected.iter().map(|projection| (*projection).clone()))
        .map(|variant_ident| {
            quote! {
                #archived_response_ident::#variant_ident(response)=> response.error(),
            }
        });
    // build our table names ident
    let table_names_ident = format_ident!("{}TableNames", struct_ident);
    // build our query_table_name arms
    let query_table_name_arms = tables.iter().map(|table| {
        let variant_ident = &table.variant_ident;
        quote! {
            #query_ident::#variant_ident(_) => #table_names_ident::#variant_ident,
        }
    });
    // build our response_table_name arms
    let mut response_table_name_arms = Vec::with_capacity(tables.len() + projected.len());
    for (table, declared) in tables.iter().zip(projections) {
        let variant_ident = &table.variant_ident;
        // this tables own response, which holds whole rows
        response_table_name_arms.push(quote! {
            #archived_response_ident::#variant_ident(_) => #table_names_ident::#variant_ident,
        });
        // a projections rows came out of the table it projects, so it names that one
        for projection in declared {
            response_table_name_arms.push(quote! {
                #archived_response_ident::#projection(_) => #table_names_ident::#variant_ident,
            });
        }
    }
    // build our format_response arms
    let format_response_arms = tables
        .iter()
        .map(|table| table.inner_type.clone())
        // a projection prints the fields it named rather than every field of its row
        .chain(projected.iter().map(|projection| (*projection).clone()))
        .map(|variant_ident| {
        let archived_inner = format_ident!("Archived{}", variant_ident);
        quote! {
            #archived_response_ident::#variant_ident(response) => {
                match &response.data {
                    shoal_core::shared::responses::ArchivedResponseAction::Get(opt) => {
                        match opt {
                            rkyv::option::ArchivedOption::Some(rows) => {
                                let headers = <#archived_inner as shoal_core::shared::traits::TableRowFormat>::headers();
                                let values: Vec<Vec<String>> = rows.iter().map(|row| {
                                    <#archived_inner as shoal_core::shared::traits::TableRowFormat>::row_values(row)
                                }).collect();
                                Some((headers, values))
                            }
                            rkyv::option::ArchivedOption::None => None,
                        }
                    }
                    // a query that failed has no rows to print, and printing it as an empty
                    // table would say it found nothing rather than that it did not run
                    shoal_core::shared::responses::ArchivedResponseAction::Error(_) => None,
                    _ => None,
                }
            }
        }
    });
    // build the name of each table as it must be typed in a query
    let table_name_strs: Vec<String> = tables
        .iter()
        .map(|table| table.inner_type.to_string())
        .collect();
    // build every projection name in this database paired with the table it projects
    let projection_name_pairs = tables.iter().zip(projections).flat_map(|(table, declared)| {
        let table_name_str = table.inner_type.to_string();
        declared.iter().map(move |projection| {
            let projection_str = projection.to_string();
            let table_name_str = table_name_str.clone();
            quote! { (#projection_str, #table_name_str) }
        })
    });
    // build our table_fields arms
    let table_fields_arms = tables.iter().map(|table| {
        let inner_type = &table.inner_type;
        let table_name_str = inner_type.to_string();
        quote! {
            #table_name_str => Some(<#inner_type as shoal_core::shared::traits::TableSchemaSupport>::fields()),
        }
    });
    // build our table_field_validator arms
    let table_field_validator_arms = tables.iter().map(|table| {
        let inner_type = &table.inner_type;
        let table_name_str = inner_type.to_string();
        quote! {
            #table_name_str => <#inner_type as shoal_core::shared::traits::TableSchemaSupport>::get_field_validator(field),
        }
    });
    // build our parse arms for each table
    let parse_arms = tables.iter().zip(projections).map(|(table, declared)| {
        let variant_ident = &table.variant_ident;
        let inner_type = &table.inner_type;
        // build the name of this tables projection enum
        let projection_enum = format_ident!("{}Projection", inner_type);
        // the name of this table as it must be typed in a query
        let table_name_str_for_projection = inner_type.to_string();
        // build one arm per projection this table declared, matched on by name
        let projection_arms = declared.iter().map(|projection| {
            let projection_str = projection.to_string();
            quote! {
                #projection_str => #projection_enum::#projection,
            }
        });
        // name every projection this table has, so a wrong one can say what the right ones are
        let projection_strs: Vec<String> =
            declared.iter().map(|projection| projection.to_string()).collect();
        // Build the projection binding every parse arm needs
        //
        // a projection is a named type rather than a column list, so the name a query wrote
        // has to be one this table declared. a projection of another table is rejected here
        // rather than answered with rows from a table the query never named
        let bind_projection = quote! {
            let projection = match &parsed.projection {
                // this query named a projection, so it has to be one of ours
                Some(named) => match named.name.as_str() {
                    #(#projection_arms)*
                    _ => {
                        // annotated because a table with no projections has an empty list here
                        let known: &[&str] = &[#(#projection_strs),*];
                        return Err(shoal_core::client::ShqlParseError::new(
                            format!(
                                "'{}' is not a projection of {}. Its projections are: {:?}",
                                named.name,
                                #table_name_str_for_projection,
                                known,
                            ),
                            named.start,
                            named.end,
                            query,
                        ));
                    }
                },
                // a query that wrote a star asked for every field of every row
                None => #projection_enum::Full,
            };
        };
        // The table name string to match against (the struct name)
        let table_name_str = inner_type.to_string();
        // Build the get struct name
        let get_ident = format_ident!("{}Get", inner_type);
        // Build the type check every parse arm starts with
        //
        // a condition may name several values, and each of them has to be one this field can
        // actually hold, so the check runs per value rather than per condition
        let check_conditions = quote! {
            for condition in &parsed.conditions {
                // Validate field exists
                let _role = <#inner_type as shoal_core::shared::traits::TableSchemaSupport>::get_field_role(&condition.field)
                    .ok_or_else(|| shoal_core::client::ShqlParseError::new(
                        format!(
                            "Unknown field '{}'. Valid fields are: {:?}",
                            condition.field,
                            <#inner_type as shoal_core::shared::traits::TableSchemaSupport>::field_names()
                        ),
                        condition.field_start,
                        condition.field_end,
                        query,
                    ))?;
                // Validate field type
                let validator = <#inner_type as shoal_core::shared::traits::TableSchemaSupport>::get_field_validator(&condition.field)
                    .ok_or_else(|| shoal_core::client::ShqlParseError::new(
                        format!("No validator for field '{}'", condition.field),
                        condition.field_start,
                        condition.field_end,
                        query,
                    ))?;
                for found in condition.values() {
                    validator(&found.value).map_err(|err| {
                        shoal_core::client::ShqlParseError::new(
                            format!("Type mismatch for field '{}': {}", condition.field, err),
                            found.start,
                            found.end,
                            query,
                        )
                    })?;
                }
            }
        };
        // Build the partition key extraction every parse arm needs
        //
        // a partition key is named by at most one condition, since the parser rejects a field
        // constrained twice, so every partition this query reads comes from that one condition
        let partition_keys = quote! {
            let partition_condition = parsed.conditions.iter()
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
            // a partition is located by its exact key, so there is nothing to bound it with
            let partition_values = partition_condition.as_values()
                .ok_or_else(|| shoal_core::client::ShqlParseError::new(
                    format!(
                        "'{}' is a partition key and cannot be given a range. A partition is \
                         located by its exact key, so name the ones to read with = or IN",
                        partition_condition.field,
                    ),
                    partition_condition.field_start,
                    partition_condition.field_end,
                    query,
                ))?;
            let mut partition_keys = Vec::with_capacity(partition_values.len());
            for found in partition_values {
                let value = shoal_core::serde_json::from_value(found.value.clone())
                    .map_err(|e| shoal_core::client::ShqlParseError::new(
                        format!("Failed to deserialize partition key: {}", e),
                        found.start,
                        found.end,
                        query,
                    ))?;
                partition_keys.push(value);
            }
        };

        match table.kind {
            TableKinds::Unsorted => {
                quote! {
                    #table_name_str => {
                        // Type check every condition against this table's schema
                        #check_conditions
                        // Extract the partition keys, which every query has to constrain
                        #partition_keys
                        // Work out which of each rows fields this query asked for
                        #bind_projection
                        // Build the Get query
                        let mut get_query = #get_ident::new(partition_keys.clone());
                        if let Some(limit) = parsed.limit {
                            get_query.limit = Some(limit);
                        }
                        // Build any filters named by the where conditions
                        get_query.filters = <#inner_type>::shql_build_filters(&parsed.conditions, query)?;
                        // Hash each partition key into the key of the partition holding it
                        let partition_key_hashes: Vec<u64> = partition_keys.iter()
                            .map(|pk| <#inner_type as shoal_core::shared::traits::PartitionKeySupport>::get_partition_key_from_values(pk))
                            .collect();
                        // Wrap in UnsortedQuery::Get and then in QueryKinds
                        let unsorted_query = shoal_core::shared::queries::UnsortedQuery::Get(
                            shoal_core::shared::queries::UnsortedGet {
                                partition_keys: partition_key_hashes,
                                filters: get_query.filters,
                                limit: get_query.limit,
                                projection,
                            }
                        );
                        Ok(#query_ident::#variant_ident(unsorted_query))
                    }
                }
            }
            TableKinds::Sorted => {
                quote! {
                    #table_name_str => {
                        // Type check every condition against this table's schema
                        #check_conditions
                        // Extract the partition keys, which every query has to constrain
                        #partition_keys
                        // Work out which of each rows fields this query asked for
                        #bind_projection
                        // Work out which rows of each partition this query selected
                        //
                        // a sort key is named by at most one condition - the parser folds the
                        // two halves of a range together - so the whole selection comes from
                        // that one condition, and a query naming none selects every row
                        let sort_select = match parsed.conditions.iter()
                            .find(|c| {
                                <#inner_type as shoal_core::shared::traits::TableSchemaSupport>::get_field_role(&c.field)
                                    == Some(shoal_core::shared::queries::parser::FieldRole::Sort)
                            })
                        {
                            // this query narrowed itself, so read how it did it
                            Some(sort_condition) => match &sort_condition.constraint {
                                // a set of values names the rows to return
                                shoal_core::shared::queries::parser::WhereConstraint::Values(values) => {
                                    let mut sort_keys = Vec::with_capacity(values.len());
                                    for found in values {
                                        let value = shoal_core::serde_json::from_value(found.value.clone())
                                            .map_err(|e| shoal_core::client::ShqlParseError::new(
                                                format!("Failed to deserialize sort key: {}", e),
                                                found.start,
                                                found.end,
                                                query,
                                            ))?;
                                        sort_keys.push(value);
                                    }
                                    shoal_core::shared::queries::SortSelect::Keys(sort_keys)
                                }
                                // a range bounds the rows to return at one or both ends
                                shoal_core::shared::queries::parser::WhereConstraint::Range(range) => {
                                    // turn one end of the parsed range into a bound on a sort key
                                    let bind = |bound: &Option<shoal_core::shared::queries::parser::WhereBound>|
                                        -> Result<std::ops::Bound<_>, shoal_core::client::ShqlParseError>
                                    {
                                        // an end that was never written bounds nothing
                                        let Some(bound) = bound else {
                                            return Ok(std::ops::Bound::Unbounded);
                                        };
                                        let value = shoal_core::serde_json::from_value(bound.value.value.clone())
                                            .map_err(|e| shoal_core::client::ShqlParseError::new(
                                                format!("Failed to deserialize sort key: {}", e),
                                                bound.value.start,
                                                bound.value.end,
                                                query,
                                            ))?;
                                        // keep whether the operator included the value it named
                                        Ok(if bound.inclusive {
                                            std::ops::Bound::Included(value)
                                        } else {
                                            std::ops::Bound::Excluded(value)
                                        })
                                    };
                                    shoal_core::shared::queries::SortSelect::Range(
                                        shoal_core::shared::queries::SortRange::new(
                                            bind(&range.lower)?,
                                            bind(&range.upper)?,
                                        )
                                    )
                                }
                            },
                            // this query never mentioned its sort key, so it wants every row
                            None => shoal_core::shared::queries::SortSelect::All,
                        };
                        // Build the Get query
                        let mut get_query = #get_ident::new(partition_keys.clone());
                        get_query.sort_select = sort_select;
                        if let Some(limit) = parsed.limit {
                            get_query.limit = Some(limit);
                        }
                        // Build any filters named by the where conditions
                        get_query.filters = <#inner_type>::shql_build_filters(&parsed.conditions, query)?;
                        // Hash each partition key into the key of the partition holding it
                        let partition_key_hashes: Vec<u64> = partition_keys.iter()
                            .map(|pk| <#inner_type as shoal_core::shared::traits::PartitionKeySupport>::get_partition_key_from_values(pk))
                            .collect();
                        // Wrap in SortedQuery::Get and then in QueryKinds
                        let sorted_query = shoal_core::shared::queries::SortedQuery::Get(
                            shoal_core::shared::queries::SortedGet {
                                partition_keys: partition_key_hashes,
                                sort_select: get_query.sort_select,
                                filters: get_query.filters,
                                limit: get_query.limit,
                                projection,
                            }
                        );
                        Ok(#query_ident::#variant_ident(sorted_query))
                    }
                }
            }
        }
    });
    // fold every table, projection and row of this database into one constant
    //
    // this is what the two peers compare when a connection opens, so it has to move whenever
    // anything that can reach the wire moves
    let variants: Vec<Ident> = tables
        .iter()
        .map(|table| table.inner_type.clone())
        .collect();
    let schema_fingerprint =
        crate::traits::fingerprint::db_expr(struct_ident, fields, &variants, projections);
    // add our client struct and query support for the client
    stream.extend(quote! {
        pub struct #client_ident {}

        impl shoal_core::shared::traits::QuerySupport for #client_ident {
            /// A hash over every part of this databases schema that can reach the wire
            const SCHEMA_FINGERPRINT: u64 = #schema_fingerprint;

            /// The different tables or types of queries we will handle
            type QueryKinds = #query_ident;

            /// The different tables we can get responses from
            type ResponseKinds = #response_ident;

            /// The different tables in this database
            type TableNames = #table_names_ident;

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

            /// Get the failure this query answered with, if it failed
            ///
            /// # Arguments
            ///
            /// * `archived` - The archived response to get the failure from
            fn error(archived: &<Self::ResponseKinds as rkyv::Archive>::Archived) -> Option<&shoal_core::shared::responses::ArchivedResponseError> {
                match archived {
                    #(#error_arms)*
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

            /// Get the names of every table in this database
            fn table_names() -> &'static [&'static str] {
                &[#(#table_name_strs),*]
            }

            /// Get every projection in this database, paired with the table it projects
            fn projection_names() -> &'static [(&'static str, &'static str)] {
                &[#(#projection_name_pairs),*]
            }

            /// Get the fields for a table and the role each one plays in a query
            ///
            /// # Arguments
            ///
            /// * `table` - The name of the table to get fields for
            fn table_fields(table: &str) -> Option<Vec<shoal_core::shared::queries::parser::FieldInfo>> {
                match table {
                    #(#table_fields_arms)*
                    _ => None,
                }
            }

            /// Get the type validator for a single field in a table
            ///
            /// # Arguments
            ///
            /// * `table` - The name of the table this field is in
            /// * `field` - The name of the field to get a validator for
            fn table_field_validator(
                table: &str,
                field: &str,
            ) -> Option<shoal_core::shared::queries::parser::TypeValidator> {
                match table {
                    #(#table_field_validator_arms)*
                    _ => None,
                }
            }

            /// Get the table name from a query
            fn query_table_name(query: &Self::QueryKinds) -> Self::TableNames {
                match query {
                    #(#query_table_name_arms)*
                }
            }

            /// Get the table name from an archived response
            fn response_table_name(
                archived: &<Self::ResponseKinds as rkyv::Archive>::Archived,
            ) -> Self::TableNames {
                match archived {
                    #(#response_table_name_arms)*
                }
            }

            /// Format an archived response into column headers and row values
            fn format_response(
                archived: &<Self::ResponseKinds as rkyv::Archive>::Archived,
            ) -> Option<(Vec<&'static str>, Vec<Vec<String>>)> {
                match archived {
                    #(#format_response_arms)*
                }
            }
        }
    });
}
