extern crate proc_macro;

use darling::{FromAttributes, FromField};
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DataStruct, Fields, Ident};

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
    let mut partition_fields = Vec::default();
    let mut sort_fields = Vec::default();
    let mut filter_fields = Vec::default();
    let mut update_fields = Vec::default();
    // step over all fields and find our partition, filter, and update keys
    for field in fields {
        // parse our field attributes
        let field_attrs = ShoalField::from_field(field).expect("Failed to parse field attributes");
        if let Some(ident) = field_attrs.ident.clone() {
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
    let mut partition_fields = Vec::default();
    let mut filter_fields = Vec::default();
    let mut update_fields = Vec::default();
    // step over all fields and find our partition, filter, and update keys
    for field in fields {
        // parse our field attributes
        let field_attrs = ShoalField::from_field(field).expect("Failed to parse field attributes");
        if let Some(ident) = field_attrs.ident.clone() {
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

/// Derive the basic traits and functions for a type to be a table in shoal
#[proc_macro_derive(ShoalDB)]
pub fn derive_shoal_db(stream: TokenStream) -> TokenStream {
    // parse our target struct
    let ast = syn::parse_macro_input!(stream as syn::DeriveInput);
    // get the name of our struct
    let struct_ident = &ast.ident;
    // Parse attributes to get custom enum name
    let enum_ident = format_ident!("{struct_ident}TableNames");
    // start with an empty stream
    let mut output = quote! {};
    // handle each possible type of data structure
    // for anything other then a struct well return an error
    match &ast.data {
        Data::Struct(data_struct) => {
            // handle the diferrent type of fields
            // we can only support named fields and will return an error for all others
            match &data_struct.fields {
                Fields::Named(fields) => {
                    // make sure we have some fields in this struct
                    // if we don't then we have to return an error
                    if fields.named.is_empty() {
                        return syn::Error::new_spanned(
                            &ast,
                            "Struct must have named fields to generate enum",
                        )
                        .to_compile_error()
                        .into();
                    }
                    // get our field names converted to pascal case
                    let variants = utils::get_variant_names(fields);
                    // build the table names for this db and add the TableNameSupport trait
                    traits::table_name::add(&mut output, &enum_ident, &variants);
                    // add display support to this enum
                    traits::display::add(&mut output, &enum_ident, &variants);
                    // add ShoalDatabase support to our root struct
                    traits::db::add(&mut output, struct_ident, fields, &variants);
                    // add our client
                    structs::client::add(&mut output, struct_ident, &variants);
                    // add our query kinds and response kinds enums with trait impls
                    structs::query_kinds::add(&mut output, struct_ident, fields);
                    // add our query conversion traits
                    traits::from_query::add(&mut output, struct_ident, fields);
                }
                Fields::Unnamed(_) => {
                    return syn::Error::new_spanned(
                        &ast,
                        "FieldsEnum only supports structs with named fields",
                    )
                    .to_compile_error()
                    .into();
                }
                Fields::Unit => {
                    return syn::Error::new_spanned(
                        &ast,
                        "FieldsEnum only supports structs with named fields",
                    )
                    .to_compile_error()
                    .into();
                }
            }
        }
        Data::Enum(_) => {
            return syn::Error::new_spanned(&ast, "FieldsEnum only supports structs, not enums")
                .to_compile_error()
                .into();
        }
        Data::Union(_) => {
            return syn::Error::new_spanned(&ast, "FieldsEnum does not support unions")
                .to_compile_error()
                .into();
        }
    }
    // convert and return our stream
    output.into()
}
