extern crate proc_macro;

use darling::{FromAttributes, FromField};
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, Fields, Ident};

mod projections;
mod structs;
mod tables;
mod traits;
mod utils;

use tables::{ShoalField, ShoalTable};

/// Which half of a schema an expansion of [`db`] emits
///
/// A schema describes two things at once: a wire contract, which both peers need, and a database,
/// which only a server has. Splitting them is what lets a client be built without an engine - see
/// `docs/src/features/client-server-split.md`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum DbHalf {
    /// The whole schema: the database struct, its `ShoalDatabase` impl, its `ShardRouting` impl,
    /// and the client
    Both,
    /// The client alone, for a crate that never starts a server
    ///
    /// The database struct itself is not emitted, which is what lets a client schema name
    /// `PersistentSortedTable` and `FileSystem` in field position without either type existing
    /// in its build at all. It also means such a schema must never `use` them.
    Client,
}

impl DbHalf {
    /// Read which half was asked for off the attribute
    ///
    /// # Arguments
    ///
    /// * `attr` - The tokens between the parentheses of the attribute, if there were any
    fn parse(attr: TokenStream) -> Result<Self, syn::Error> {
        // no argument at all is the whole schema, which is what every server writes
        if attr.is_empty() {
            return Ok(DbHalf::Both);
        }
        // the only argument this takes is a bare `client`
        let ident = syn::parse::<Ident>(attr)
            .map_err(|err| syn::Error::new(err.span(), "expected `client` or no argument"))?;
        if ident == "client" {
            Ok(DbHalf::Client)
        } else {
            // there is deliberately no `server` spelling - one meaning gets one spelling
            Err(syn::Error::new_spanned(
                &ident,
                format!("expected `client` or no argument, found `{ident}`"),
            ))
        }
    }
}

/// Derive the traits that let a struct be a projection of one of a databases tables
///
/// A projection names a subset of a rows fields and a get can ask to be answered with it,
/// which copies only the fields it named out of each row instead of every field the row has.
/// The table it projects is named with `#[shoal_projection(table = "Movie")]`, and the
/// database it belongs to lists it on the field holding that table.
#[proc_macro_derive(ShoalProjection, attributes(shoal_projection, shoal))]
pub fn derive_shoal_projection(stream: TokenStream) -> TokenStream {
    // parse our target struct
    let ast = syn::parse_macro_input!(stream as syn::DeriveInput);
    // build everything this projection needs
    projections::derive(&ast).into()
}

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
    let mut mirror_fields = Vec::default();
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
            // and again with whether the field asserted a mirror of its own, for the rearchiver
            mirror_fields.push((
                ident.clone(),
                field_attrs.ty.clone(),
                field_attrs.rearchive,
            ));
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
        &update_fields,
    );
    traits::table_row_format::add(&mut output, name, &all_fields);
    traits::rearchive::add(&mut output, name, &mirror_fields);
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
    let mut mirror_fields = Vec::default();
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
            // and again with whether the field asserted a mirror of its own, for the rearchiver
            mirror_fields.push((
                ident.clone(),
                field_attrs.ty.clone(),
                field_attrs.rearchive,
            ));
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
        &[], // unsorted tables have no sort fields
        &filter_fields,
        &update_fields,
    );
    traits::table_row_format::add(&mut output, name, &all_fields);
    traits::rearchive::add(&mut output, name, &mirror_fields);
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
///
/// # Emitting only the client
///
/// `#[shoal::db(client)]` emits everything a caller needs to *talk* to a database and nothing it
/// would need to *be* one - no `ShoalDatabase` impl, no `ShardRouting` impl, and not even the
/// struct itself. A crate that writes one links no storage engine and no async runtime beyond the
/// client's own, which is what `shoalctl` does:
///
/// ```ignore
/// #[shoal::db(client)]
/// pub struct Tmdb {
///     pub movies: PersistentSortedTable<Movie, FileSystem>,
/// }
/// ```
///
/// Because the struct is never emitted, `PersistentSortedTable` and `FileSystem` above are read
/// for their names and then discarded - they never reach type resolution. **A client schema must
/// therefore name its table and storage types in field position only, and must never `use` them**,
/// since the import would fail in a build where they do not exist.
#[proc_macro_attribute]
pub fn db(attr: TokenStream, item: TokenStream) -> TokenStream {
    // work out whether we are emitting a whole schema or only its client
    let half = match DbHalf::parse(attr) {
        Ok(half) => half,
        Err(err) => return err.to_compile_error().into(),
    };
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

    // the projections each of this databases tables declared, in field order
    let mut projections = Vec::default();

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
        // take the projections each table declared off of its field before we emit the struct
        projections = utils::take_projections(fields);
        utils::rewrite_table_fields(fields, &struct_ident);
    }

    // emit the rewritten struct definition, which only a server has any use for
    //
    // the rewrite above still runs for a client, because everything below reads the fields and
    // has to see byte identical input in both halves - that is what makes a client schema's
    // generated client the same code as a server schema's
    let mut output = match half {
        DbHalf::Both => quote! { #item_struct },
        DbHalf::Client => quote! {},
    };

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
    // add the enum naming each tables projections, since only we see all of them
    projections::add_enums(&mut output, &variants, &projections);
    // add ShoalDatabase support to our root struct
    //
    // this is the whole server half: it is the only emission naming glommio, kanal, the storage
    // loaders or the server config, so skipping it is what makes a client build possible
    if half == DbHalf::Both {
        traits::db::add(&mut output, &struct_ident, fields, &variants, &projections);
    }
    // add our client
    structs::client::add(&mut output, &struct_ident, fields, &projections);
    // add our query kinds and response kinds enums with trait impls
    structs::query_kinds::add(&mut output, &struct_ident, fields, &projections, half);
    // add our query conversion traits
    traits::from_query::add(&mut output, &struct_ident, fields);
    // let every projection be pulled back out of a response the same way a row is
    projections::add_from_shoal(&mut output, &struct_ident, &projections);
    output.into()
}
