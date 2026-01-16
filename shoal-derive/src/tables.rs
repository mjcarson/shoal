//! The different derive traits for shoal tables

use darling::FromField;

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
