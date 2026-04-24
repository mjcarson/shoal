extern crate proc_macro;

use darling::{FromAttributes, FromField};
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, Fields, Ident};

mod structs;
mod tables;
mod traits;
mod utils;

use tables::{ShoalField, ShoalTable};

/// Derive the basic traits and functions for a type to be a table in shoal
#[proc_macro_derive(ShoalSortedTable, attributes(shoal_table, shoal))]
pub fn derive_shoal_sorted_table(stream: TokenStream) -> TokenStream {
    // parse our target struct
    let ast = syn::parse_macro_input!(stream as syn::DeriveInput);
    // get the name of our struct
    let name = &ast.ident;
    // we only support structs right now
    let data_struct = match &ast.data {
        Data::Struct(data_struct) => data_struct,
        _ => unimplemented!("Only structs are currently supported"),
    };
    // get our fields
    let fields = match &data_struct.fields {
        Fields::Named(fields) => &fields.named,
        _ => panic!("ShoalTable requires named fields"),
    };
    // instance vecs to store our partition, filter, and update keys
    let mut all_fields = Vec::default();
    let mut partition_fields = Vec::default();
    let mut sort_fields = Vec::default();
    let mut filter_fields = Vec::default();
    let mut update_fields = Vec::default();
    // step over all fields and find our partition, filter, and update keys
    for field in fields {
        // parse our field attributes
        let field_attrs = ShoalField::from_field(field).expect("Failed to parse field attributes");
        if let Some(ident) = field_attrs.ident.clone() {
            // collect all fields for schema support
            all_fields.push((ident.clone(), field_attrs.ty.clone()));
            // check if this is a partition or a sort key
            match (field_attrs.partition, field_attrs.sort) {
                (true, false) => partition_fields.push((ident.clone(), field_attrs.ty.clone())),
                (false, true) => sort_fields.push((ident.clone(), field_attrs.ty.clone())),
                (false, false) => (),
                (true, true) => panic!("Fields cannot be both partition and sort keys!: {ident}"),
            }
            // check if this field is a filter field
            if field_attrs.filter {
                filter_fields.push((ident.clone(), field_attrs.ty.clone()));
            }
            // check if this field is an update field
            if field_attrs.update {
                update_fields.push((ident.clone(), field_attrs.ty.clone()));
            }
        }
    }
    // make sure at least one partition key was set
    if partition_fields.is_empty() {
        panic!("Sorted tables require at least one partition key to be set!");
    }
    // Must have at least one sort field
    if sort_fields.is_empty() {
        panic!("Sorted tables require at least one sort key to be set!");
    }
    // start with an empty stream
    let mut output = quote! {};
    // get the attributes for our sorted table
    let attrs =
        ShoalTable::from_attributes(&ast.attrs).expect("Failed to parse ShoalTable attributes");
    // get our db and table name as a ident
    let db_name = Ident::new(&attrs.db, name.span());
    // get our db and table name as a ident
    //let table_name = Ident::new(&attrs.name, name.span());
    let client_name = syn::Ident::new(&format!("{}Client", db_name), name.span());
    // build the name of our kinds
    let query_name = syn::Ident::new(&format!("{db_name}QueryKinds"), name.span());
    let response_name = syn::Ident::new(&format!("Archived{db_name}ResponseKinds"), name.span());
    // extend this type
    // generate the core traits for this type
    traits::from_shoal::add(&mut output, name, &client_name, &response_name);
    traits::rkyv::add(&mut output, name);
    traits::partition_key::add(&mut output, name, &partition_fields);
    traits::table_schema::add(
        &mut output,
        name,
        &all_fields,
        &partition_fields,
        &sort_fields,
        &filter_fields,
    );
    traits::table_row_format::add(&mut output, name, &all_fields);
    //traits::from_query::add_sorted(&mut output, name, &query_name);
    // generate the Filter and Update structs
    structs::filter::add(&mut output, name, &filter_fields);
    structs::get::add_sorted(&mut output, name, &partition_fields, &sort_fields);
    structs::update::add_sorted(
        &mut output,
        name,
        &partition_fields,
        &sort_fields,
        &update_fields,
    );
    structs::delete::add_sorted(&mut output, name, &partition_fields, &sort_fields);
    // generate the ShoalTableSupport implementation
    tables::add(&mut output, name, &filter_fields, &update_fields);
    // generate the ShoalUnsortedTable implementation
    tables::sorted::add(&mut output, name, &sort_fields, &update_fields);
    // convert and return our stream
    output.into()
}

/// Derive the basic traits and functions for a type to be a table in shoal
#[proc_macro_derive(ShoalUnsortedTable, attributes(shoal_table, shoal))]
pub fn derive_shoal_unsorted_table(stream: TokenStream) -> TokenStream {
    // parse our target struct
    let ast = syn::parse_macro_input!(stream as syn::DeriveInput);
    // get the name of our struct
    let name = &ast.ident;
    // we only support structs right now
    let data_struct = match &ast.data {
        Data::Struct(data_struct) => data_struct,
        _ => unimplemented!("Only structs are currently supported"),
    };
    // get our fields
    let fields = match &data_struct.fields {
        Fields::Named(fields) => &fields.named,
        _ => panic!("ShoalTable requires named fields"),
    };
    // instance vecs to store our partition, filter, and update keys
    let mut all_fields = Vec::default();
    let mut partition_fields = Vec::default();
    let mut filter_fields = Vec::default();
    let mut update_fields = Vec::default();
    // step over all fields and find our partition, filter, and update keys
    for field in fields {
        // parse our field attributes
        let field_attrs = ShoalField::from_field(field).expect("Failed to parse field attributes");
        if let Some(ident) = field_attrs.ident.clone() {
            // collect all fields for schema support
            all_fields.push((ident.clone(), field_attrs.ty.clone()));
            // check if this is a partition or a sort key
            match (field_attrs.partition, field_attrs.sort) {
                (true, false) => partition_fields.push((ident.clone(), field_attrs.ty.clone())),
                (false, true) => panic!("Unsorted tables do not support sort keys!: {ident}"),
                (false, false) => (),
                (true, true) => panic!("Fields cannot be both partition and sort keys!: {ident}"),
            }
            // check if this field is a filter field
            if field_attrs.filter {
                filter_fields.push((ident.clone(), field_attrs.ty.clone()));
            }
            // check if this field is an update field
            if field_attrs.update {
                update_fields.push((ident.clone(), field_attrs.ty.clone()));
            }
        }
    }
    // make sure at least one partition key was set
    if partition_fields.is_empty() {
        panic!("Unsorted tables require at least one partition key to be set!");
    }
    // start with an empty stream
    let mut output = quote! {};
    // get the attributes for our unsorted table
    let attrs =
        ShoalTable::from_attributes(&ast.attrs).expect("Failed to parse ShoalTable attributes");
    // get our db and table name as a ident
    let db_name = Ident::new(&attrs.db, name.span());
    //let table_name = Ident::new(&attrs.name, name.span());
    let client_name = syn::Ident::new(&format!("{}Client", db_name), name.span());
    // build the name of our kinds
    let query_name = syn::Ident::new(&format!("{}QueryKinds", db_name), name.span());
    let response_name = syn::Ident::new(&format!("Archived{}ResponseKinds", db_name), name.span());
    // generate the core traits for this type
    traits::from_shoal::add(&mut output, name, &client_name, &response_name);
    traits::rkyv::add(&mut output, name);
    traits::partition_key::add(&mut output, name, &partition_fields);
    traits::table_schema::add(
        &mut output,
        name,
        &all_fields,
        &partition_fields,
        &[],  // unsorted tables have no sort fields
        &filter_fields,
    );
    traits::table_row_format::add(&mut output, name, &all_fields);
    // generate the Filter and Update structs
    structs::filter::add(&mut output, name, &filter_fields);
    structs::get::add_unsorted(&mut output, name, &partition_fields);
    structs::update::add_unsorted(&mut output, name, &partition_fields, &update_fields);
    structs::delete::add_unsorted(&mut output, name, &partition_fields);
    // generate the ShoalTableSupport implementation
    tables::add(&mut output, name, &filter_fields, &update_fields);
    // generate the ShoalUnsortedTable implementation
    tables::unsorted::add(&mut output, name, &update_fields);
    // convert and return our stream
    output.into()
}

/// Attribute macro that rewrites table field types and generates all supporting code for a Shoal database.
///
/// Transforms simplified field types like `PersistentUnsortedTable<Movie, FileSystem>`
/// into full types like `PersistentUnsortedTable<Movie, FileSystem<Tmdb>, TmdbTableNames>`,
/// then generates the TableNames enum, Client struct, QueryKinds/ResponseKinds, and trait impls.
#[proc_macro_attribute]
pub fn shoal_db(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // parse the input as a struct
    let mut item_struct = syn::parse_macro_input!(item as syn::ItemStruct);
    let struct_ident = item_struct.ident.clone();
    let enum_ident = format_ident!("{struct_ident}TableNames");

    // validate we have named fields
    let Fields::Named(_) = &item_struct.fields else {
        return syn::Error::new_spanned(
            &item_struct,
            "shoal_db only supports structs with named fields",
        )
        .to_compile_error()
        .into();
    };

    // rewrite field types: add <StructName> to storage and append TableNames
    if let Fields::Named(fields) = &mut item_struct.fields {
        if fields.named.is_empty() {
            return syn::Error::new_spanned(
                &item_struct,
                "Struct must have named fields to generate enum",
            )
            .to_compile_error()
            .into();
        }
        utils::rewrite_table_fields(fields, &struct_ident);
    }

    // emit the rewritten struct definition
    let mut output = quote! { #item_struct };

    // now borrow the rewritten fields immutably for codegen
    let fields = match &item_struct.fields {
        Fields::Named(fields) => fields,
        _ => unreachable!(),
    };

    // get our field names converted to pascal case
    let variants = utils::get_variant_names(fields);
    // build the table names for this db and add the TableNameSupport trait
    traits::table_name::add(&mut output, &enum_ident, &variants);
    // add display support to this enum
    traits::display::add(&mut output, &enum_ident, &variants);
    // add ShoalDatabase support to our root struct
    traits::db::add(&mut output, &struct_ident, fields, &variants);
    // add our client
    structs::client::add(&mut output, &struct_ident, fields);
    // add our query kinds and response kinds enums with trait impls
    structs::query_kinds::add(&mut output, &struct_ident, fields);
    // add our query conversion traits
    traits::from_query::add(&mut output, &struct_ident, fields);
    output.into()
}
