//! The different derive traits for shoal tables

use darling::FromField;
use quote::{format_ident, quote};
use syn::Ident;

pub mod sorted;
pub mod unsorted;

/// A field attribute for columns/fields in a Shoal table
#[derive(Debug, FromField)]
#[darling(attributes(shoal))]
pub(super) struct ShoalField {
    /// The identifier for this field/column
    pub ident: Option<syn::Ident>,
    /// The type for this field/column
    pub ty: syn::Type,
    /// The position of this field in the partition key, if its in it
    #[darling(default)]
    pub partition: bool,
    /// The position of this field in the sort key, if its in it
    #[darling(default)]
    pub sort: bool,
    /// Whether this field can be used to filter what rows are returned from shoal or not
    #[darling(default)]
    pub filter: bool,
    /// Whether this field can be updated
    #[darling(default)]
    pub update: bool,
}

/// The arguments for a FromShoal derive
#[derive(Debug, darling::FromAttributes)]
#[darling(attributes(shoal_table))]
pub(super) struct ShoalTable {
    /// The name of the database this table is in
    pub db: String,
}

/// Extend a token stream with a ShoalTableSupport implementation
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `filter_fields` - The fields used for filtering (ident, type)
/// * `update_fields` - The fields used for updates (ident, type)
pub(super) fn add(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    filter_fields: &[(syn::Ident, syn::Type)],
    update_fields: &[(syn::Ident, syn::Type)],
) {
    // build the struct names
    let filter_name = format_ident!("{}Filter", name);
    let update_name = format_ident!("{}Update", name);
    let update_data_name = format_ident!("{}UpdateData", name);
    // build the is_filtered checks for regular rows
    let filter_checks: Vec<_> = filter_fields
        .iter()
        .map(|(ident, _)| {
            quote! {
                if let Some(ref filter_val) = filter.#ident {
                    if &row.#ident != filter_val {
                        return false;
                    }
                }
            }
        })
        .collect();
    // build the is_filtered checks for archived rows
    let filter_archived_checks: Vec<_> = filter_fields
        .iter()
        .map(|(ident, _)| {
            quote! {
                if let Some(ref filter_val) = filter.#ident {
                    if &row.#ident != filter_val {
                        return false;
                    }
                }
            }
        })
        .collect();
    // generate the ShoalTableSupport implementation
    stream.extend(quote! {
        #[automatically_derived]
        impl shoal_core::shared::traits::ShoalTableSupport for #name {
            /// The updates that can be applied to this table
            type Update = #update_name;

            /// The server facing updates that can be applied to this table (just the updates no keys)
            type UpdateData = #update_data_name;

            /// Any filters to apply when listing/crawling rows
            type Filters = #filter_name;

            fn is_filtered(filter: &Self::Filters, row: &Self) -> bool {
                #(#filter_checks)*
                true
            }

            fn is_filtered_archived(
                filter: &Self::Filters,
                row: &<Self as rkyv::Archive>::Archived,
            ) -> bool {
                #(#filter_archived_checks)*
                true
            }
        }
    });
}
