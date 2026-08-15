//! Generate a filter struct for a type

use quote::{format_ident, quote};
use syn::Ident;

/// Extend a token stream with a Filter struct definition
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `filter_fields` - The fields to include in the filter struct (ident, type)
pub fn add(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    filter_fields: &[(syn::Ident, syn::Type)],
) {
    // Build the filter struct name
    let filter_name = format_ident!("{}Filter", name);

    // If no filter fields, create an empty struct
    if filter_fields.is_empty() {
        stream.extend(quote! {
            #[derive(Debug, Clone, ::shoal::rkyv::Archive, ::shoal::rkyv::Serialize, ::shoal::rkyv::Deserialize, Default)]
            #[rkyv(derive(Debug))]
            pub struct #filter_name;

            #[automatically_derived]
            impl #name {
                /// Build filters for this table from a set of SHQL where conditions
                ///
                /// This table has no filterable fields, so there is never anything to build.
                ///
                /// # Arguments
                ///
                /// * `conditions` - The where conditions to build filters from
                /// * `query` - The original query string (for error reporting)
                pub fn shql_build_filters(
                    conditions: &[::shoal::shared::queries::parser::WhereClause],
                    query: &str,
                ) -> Result<Option<#filter_name>, ::shoal::client::ShqlParseError> {
                    // this table has no filterable fields so there is nothing to build
                    let _ = (conditions, query);
                    Ok(None)
                }
            }
        });
        return;
    }

    // Build the fields for the filter struct
    //
    // a field holds every value it was given rather than a single one, so that `=` and `IN`
    // are the same filter with one and several values in it
    let fields = filter_fields.iter().map(|(ident, ty)| {
        quote! {
            pub #ident: Option<Vec<#ty>>
        }
    });

    // Build the per field extraction steps used when building filters from a SHQL query
    let build_steps = filter_fields.iter().map(|(ident, ty)| {
        let field_name_str = ident.to_string();
        quote! {
            // look for a where condition naming this filter field
            if let Some(condition) = conditions.iter().find(|cond| cond.field == #field_name_str) {
                // a filter is a membership test, so there is nothing to bound it with
                let condition_values = condition.as_values()
                    .ok_or_else(|| ::shoal::client::ShqlParseError::new(
                        format!(
                            "'{}' is a filter and cannot be given a range. A filter checks a row \
                             against the values it may take, so name them with = or IN. Only a \
                             sort key can be bounded, because it is what a partition is ordered by",
                            #field_name_str,
                        ),
                        condition.field_start,
                        condition.field_end,
                        query,
                    ))?;
                // a condition may name several values, all of which this field may take
                let mut values = Vec::with_capacity(condition_values.len());
                // convert each literal from the query into this field's type
                for found in condition_values {
                    let value = ::shoal::serde_json::from_value::<#ty>(found.value.clone())
                        .map_err(|error| ::shoal::client::ShqlParseError::new(
                            format!("Failed to deserialize filter '{}': {}", #field_name_str, error),
                            found.start,
                            found.end,
                            query,
                        ))?;
                    values.push(value);
                }
                // set this filter and remember that we have at least one
                filters.#ident = Some(values);
                any_set = true;
            }
        }
    });

    // Generate the filter struct
    stream.extend(quote! {
        #[derive(Debug, Clone, ::shoal::rkyv::Archive, ::shoal::rkyv::Serialize, ::shoal::rkyv::Deserialize, Default)]
        #[rkyv(derive(Debug))]
        pub struct #filter_name {
            #(#fields),*
        }

        #[automatically_derived]
        impl #name {
            /// Build filters for this table from a set of SHQL where conditions
            ///
            /// Any condition naming a filterable field is converted into the matching field on
            /// this table's filter struct. Conditions on partition or sort keys are left alone
            /// since they are handled separately by the generated parse arm.
            ///
            /// # Arguments
            ///
            /// * `conditions` - The where conditions to build filters from
            /// * `query` - The original query string (for error reporting)
            ///
            /// # Returns
            ///
            /// The filters to apply, or None if no condition named a filterable field
            pub fn shql_build_filters(
                conditions: &[::shoal::shared::queries::parser::WhereClause],
                query: &str,
            ) -> Result<Option<#filter_name>, ::shoal::client::ShqlParseError> {
                // start with an empty set of filters
                let mut filters = #filter_name::default();
                // track whether any condition actually set a filter
                let mut any_set = false;
                // try to pull each filterable field out of our conditions
                #(#build_steps)*
                // only return filters if at least one was set
                if any_set {
                    Ok(Some(filters))
                } else {
                    Ok(None)
                }
            }
        }
    });
}
