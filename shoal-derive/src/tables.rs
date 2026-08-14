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
    let projection_enum = format_ident!("{}Projection", name);
    // build the is_filtered checks for regular rows
    //
    // a filter holds every value its field may take, so a row matches when it holds any one
    // of them. separate filters still have to all match, which is what makes a query naming
    // two different fields a conjunction
    let filter_checks: Vec<_> = filter_fields
        .iter()
        .map(|(ident, _)| {
            quote! {
                if let Some(ref filter_vals) = filter.#ident {
                    if !filter_vals.iter().any(|filter_val| &row.#ident == filter_val) {
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
                if let Some(ref filter_vals) = filter.#ident {
                    if !filter_vals.iter().any(|filter_val| &row.#ident == filter_val) {
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

            /// The subsets of this tables rows that a get can ask to be answered with
            type Projection = #projection_enum;

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

        /// A whole row is the identity projection of itself
        ///
        /// This is what lets a projected get and an unprojected one be the same code path: a
        /// scan is generic in what it is building, and a get that named no projection builds
        /// this one. Both conversions are what a get has always done, so once they are inlined
        /// an unprojected get costs exactly what it did before projections existed.
        #[automatically_derived]
        impl shoal_core::shared::traits::ShoalProjection for #name {
            /// A row projects its own table
            type Row = #name;

            /// The identity projection of a row fingerprints as the row it is
            ///
            /// There is nothing here the table does not already fold in, so borrowing its
            /// constant is what keeps the two from ever disagreeing about the same fields.
            const SCHEMA_FINGERPRINT: u64 =
                <#name as shoal_core::shared::traits::TableSchemaSupport>::SCHEMA_FINGERPRINT;

            /// The whole row is the projection a get gets when it names none
            const PROJECTION: #projection_enum = #projection_enum::Full;

            /// Build a whole row from a resident one, which is a clone
            ///
            /// # Arguments
            ///
            /// * `row` - The row to project
            #[inline]
            fn from_row(row: &#name) -> Self {
                row.clone()
            }

            /// Build a whole row from an archived one, which is a deserialize
            ///
            /// # Arguments
            ///
            /// * `row` - The archived row to project
            #[inline]
            fn from_archived(row: &<#name as rkyv::Archive>::Archived) -> Self {
                <#name as shoal_core::shared::traits::RkyvSupport>::deserialize(row).unwrap()
            }
        }
    });
}

/// The different types of tables
pub enum TableKinds {
    /// Treats each partition as a single value (key/value store)
    Unsorted,
    /// Partition in this table can contain many sorted rows
    Sorted,
}

impl TableKinds {
    /// Get the kind of table from a type
    pub fn new(ty: &syn::Type) -> Self {
        // we only can get table kinds from path like types
        if let syn::Type::Path(type_path) = ty {
            // get the first segemnt from this types path
            if let Some(segment) = type_path.path.segments.first() {
                // convert this type to a string so we can compare it
                let type_name = segment.ident.to_string();
                // check if this our type name contains our kinds
                match (type_name.contains("Unsorted"), type_name.contains("Sorted")) {
                    (true, false) => return Self::Unsorted,
                    (false, true) => return Self::Sorted,
                    (true, true) => panic!("Ambiguous table kind detected: {type_name}"),
                    (false, false) => panic!("Failed to detect table kind: {type_name}"),
                }
            }
        }
        panic!("Invalid type checked when detect table kind: {ty:?}")
    }
}
