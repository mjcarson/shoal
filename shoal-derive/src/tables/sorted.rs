//! Generate the traits and code for sorted tables

use quote::{format_ident, quote};
use syn::Ident;

/// Extend a token stream with an update method for ShoalSortedTable
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `sort_fields` - The fields this table's rows are sorted by, in declaration order (ident, type)
/// * `update_fields` - The fields used for updates (ident, type)
pub fn add(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    sort_fields: &[(syn::Ident, syn::Type)],
    update_fields: &[(syn::Ident, syn::Type)],
) {
    //// Must have at least one sort field
    //if sort_fields.is_empty() {
    //    panic!("ShoalSortedTable requires at least one sort field!");
    //}
    // Build the Sort type and get_sort body
    let (sort_type, get_sort_body) = if sort_fields.len() == 1 {
        // Single field - clone and return
        let (ident, ty) = &sort_fields[0];
        (quote! { #ty }, quote! { self.#ident.clone() })
    } else {
        // Multiple fields - build a tuple
        let types: Vec<_> = sort_fields.iter().map(|(_, ty)| ty).collect();
        let clones: Vec<_> = sort_fields
            .iter()
            .map(|(ident, _)| quote! { self.#ident.clone() })
            .collect();
        (quote! { (#(#types),*) }, quote! { (#(#clones),*) })
    };
    // build the update assignments
    let update_assignments: Vec<_> = update_fields
        .iter()
        .map(|(ident, _)| {
            quote! {
                if let Some(ref new_val) = update.update.#ident {
                    self.#ident = new_val.clone();
                }
            }
        })
        .collect();
    // build how a SHQL query's where clauses become the rows of a partition it selects
    let sort_select_body = if sort_fields.len() == 1 {
        single_sort_select(&sort_fields[0])
    } else {
        composite_sort_select(sort_fields)
    };
    // generate the ShoalUnsortedTable implementation
    stream.extend(quote! {
        #[automatically_derived]
        impl ::shoal::shared::traits::ShoalSortedTable for #name {
            /// The sort type for this data
            type Sort = #sort_type;

            /// Build the sort key for this row
            #[inline]
            fn get_sort(&self) -> Self::Sort {
                #get_sort_body
            }

            /// Apply an update to a single row
            ///
            /// # Arguments
            ///
            /// * `update` - The update to apply to a specific row
            fn update(&mut self, update: &::shoal::shared::queries::SortedUpdate<Self>) {
                #(#update_assignments)*
            }
        }

        #[automatically_derived]
        impl #name {
            /// Build the rows of each partition a SHQL query selected, from its where clauses
            ///
            /// Generated here rather than in the database's parse arm, because only this table
            /// knows its sort fields' names and types: a composite key is built field by field,
            /// in declaration order (item 42). A query that never mentions its sort key selects
            /// every row.
            ///
            /// # Arguments
            ///
            /// * `conditions` - The where clauses of the query
            /// * `query` - The original query string, for error reporting
            pub fn shql_build_sort_select(
                conditions: &[::shoal::shared::queries::parser::WhereClause],
                query: &str,
            ) -> Result<
                ::shoal::shared::queries::SortSelect<
                    <Self as ::shoal::shared::traits::ShoalSortedTable>::Sort,
                >,
                ::shoal::client::ShqlParseError,
            > {
                #sort_select_body
            }
        }
    });
}

/// Build how a table with one sort field reads its selection from a query's where clauses
///
/// A set of values names the rows to return and a range bounds them, exactly as the database's
/// parse arm read them before this moved here.
///
/// # Arguments
///
/// * `field` - The table's sort field (ident, type)
fn single_sort_select(field: &(syn::Ident, syn::Type)) -> proc_macro2::TokenStream {
    let (ident, ty) = field;
    let field_name = ident.to_string();
    quote! {
        // a sort key is named by at most one condition - the parser folds the two halves of a
        // range together - so the whole selection comes from that one condition, and a query
        // naming none selects every row
        let Some(sort_condition) = conditions.iter().find(|condition| condition.field == #field_name) else {
            return Ok(::shoal::shared::queries::SortSelect::All);
        };
        // this query narrowed itself, so read how it did it
        match &sort_condition.constraint {
            // a set of values names the rows to return
            ::shoal::shared::queries::parser::WhereConstraint::Values(values) => {
                let mut sort_keys = Vec::with_capacity(values.len());
                for found in values {
                    let value = ::shoal::serde_json::from_value::<#ty>(found.value.clone())
                        .map_err(|e| ::shoal::client::ShqlParseError::new(
                            format!("Failed to deserialize sort key: {}", e),
                            found.start,
                            found.end,
                            query,
                        ))?;
                    sort_keys.push(value);
                }
                Ok(::shoal::shared::queries::SortSelect::Keys(sort_keys))
            }
            // a range bounds the rows to return at one or both ends
            ::shoal::shared::queries::parser::WhereConstraint::Range(range) => {
                // turn one end of the parsed range into a bound on a sort key
                let bind = |bound: &Option<::shoal::shared::queries::parser::WhereBound>|
                    -> Result<std::ops::Bound<#ty>, ::shoal::client::ShqlParseError>
                {
                    // an end that was never written bounds nothing
                    let Some(bound) = bound else {
                        return Ok(std::ops::Bound::Unbounded);
                    };
                    let value = ::shoal::serde_json::from_value::<#ty>(bound.value.value.clone())
                        .map_err(|e| ::shoal::client::ShqlParseError::new(
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
                Ok(::shoal::shared::queries::SortSelect::Range(
                    ::shoal::shared::queries::SortRange::new(
                        bind(&range.lower)?,
                        bind(&range.upper)?,
                    )
                ))
            }
        }
    }
}

/// Build how a table with a composite sort key reads its selection from a query's where clauses
///
/// Every field is named by a point or a set of them, or none is; the keys they name together are
/// every pairing of them, in declaration order, counted against the bound before any is built.
/// The server sorts and deduplicates them, as it does every set of keys.
///
/// # Arguments
///
/// * `fields` - The table's sort fields, in declaration order (ident, type)
fn composite_sort_select(fields: &[(syn::Ident, syn::Type)]) -> proc_macro2::TokenStream {
    // the fields' names, as a query types them and an error names them
    let names: Vec<String> = fields.iter().map(|(ident, _)| ident.to_string()).collect();
    // one list of points and one point for each field, in declaration order
    let points: Vec<Ident> = (0..fields.len())
        .map(|index| format_ident!("points_{}", index))
        .collect();
    let point: Vec<Ident> = (0..fields.len())
        .map(|index| format_ident!("point_{}", index))
        .collect();
    // read each field's clause as points of its own type
    let reads = fields.iter().enumerate().map(|(index, (_, ty))| {
        let points = &points[index];
        quote! {
            let #points = ::shoal::shared::queries::parser::composite_sort_points::<#ty>(
                clauses[#index],
                FIELDS,
                query,
            )?;
        }
    });
    // the innermost step pushes one key built from a point of every field
    let mut cross = quote! {
        keys.push((#(::std::clone::Clone::clone(#point)),*));
    };
    // and each field wraps it in a loop over its points, the last field innermost
    for (points, point) in points.iter().zip(&point).rev() {
        cross = quote! {
            for #point in &#points {
                #cross
            }
        };
    }
    quote! {
        // the key's fields, in declaration order
        const FIELDS: &[&str] = &[#(#names),*];
        // find the clause naming each field; a query naming none of them wants every row
        let Some(clauses) = ::shoal::shared::queries::parser::composite_sort_clauses(conditions, FIELDS, query)? else {
            return Ok(::shoal::shared::queries::SortSelect::All);
        };
        // read each field's points, a range or a value its type cannot hold refused by name
        #(#reads)*
        // count the keys they name together, refused past the bound before any is built
        let count = ::shoal::shared::queries::parser::check_sort_key_product(
            &[#(#points.len()),*],
            FIELDS,
            query,
        )?;
        // and cross them into every key they name together
        let mut keys = Vec::with_capacity(count);
        #cross
        Ok(::shoal::shared::queries::SortSelect::Keys(keys))
    }
}
