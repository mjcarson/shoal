extern crate proc_macro;

use darling::FromAttributes;
use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DataStruct, GenericArgument, Ident, LitStr, PathArguments};

/// The arguments for a FromShoal derive
#[derive(Debug, darling::FromAttributes)]
#[darling(attributes(shoal_table))]
struct ShoalTable {
    ///// The name of this table
    //pub name: String,
    /// The name of the database this table is in
    pub db: String,
}

/// Extend a token stream with a FromShoal implementation
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `db_name` - The name of the database
/// * `response_name` - The name of the response type
fn add_from_shoal(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    db_name: &Ident,
    response_name: &Ident,
) {
    // extend our token stream
    stream.extend(
        quote! {
            #[automatically_derived]
            impl shoal_core::FromShoal<#db_name> for #name  {
                type ResponseKinds = <#db_name as shoal_core::shared::traits::ShoalDatabase>::ResponseKinds;

                fn retrieve(kind: #response_name) -> Result<Option<Vec<Self>>, shoal_core::client::Errors> {
                    // make sure its the right data kind
                    if let #response_name::#name(action) = kind {
                        // make sure its a get action
                        if let shoal_core::shared::responses::ResponseAction::Get(rows) = action.data {
                            return Ok(rows);
                        }
                    }
                    Err(shoal_core::client::Errors::WrongType("Wrong Type!".to_owned()))
                }
            }
        }
    );
}

// TODO make this generic if possible
///// Extend a token stream with a FromShoal implementation
/////
///// # Arguments
/////
///// * `stream` - The stream to extend
///// * `name` - The name of the type we are extending
///// * `query_name` - The name of the query type
//fn add_from_for_query(stream: &mut proc_macro2::TokenStream, name: &Ident, query_name: &Ident) {
//    // extend our token stream
//    stream.extend(quote! {
//        #[automatically_derived]
//        impl From<#name> for #query_name {
//            fn from(row: #name) -> #query_name {
//                // get our rows partition key
//                let key = #name::partition_key(&row.key);
//                // build our query kind
//                #query_name::KeyValueQuery(Query::Insert { key, row })
//            }
//        }
//    });
//}

// TODO make this generic if possible
///// Extend a token stream with a FromShoal implementation
/////
///// # Arguments
/////
///// * `stream` - The stream to extend
///// * `name` - The name of the type we are extending
///// * `table_name` - The name of the table
///// * `response_name` - The name of the response type
//fn add_shoal_table(stream: &mut proc_macro2::TokenStream, name: &Ident) {
//    // extend our token stream
//    stream.extend(quote! {
//        #[automatically_derived]
//        impl ShoalTable for #name {
//            /// The sort type for this data
//            type Sort = String;
//
//            /// Build the sort tuple for this row
//            fn get_sort(&self) -> &Self::Sort {
//                &self.key
//            }
//
//            /// Calculate the partition key for this row
//            fn partition_key(sort: &Self::Sort) -> u64 {
//                // create a new hasher
//                let mut hasher = GxHasher::default();
//                // hash the first key
//                hasher.write(sort.as_bytes());
//                // get our hash
//                hasher.finish()
//            }
//
//            /// Any filters to apply when listing/crawling rows
//            type Filters = String;
//
//            /// Determine if a row should be filtered
//            ///
//            /// # Arguments
//            ///
//            /// * `filters` - The filters to apply
//            /// * `row` - The row to filter
//            fn is_filtered(filter: &Self::Filters, row: &Self) -> bool {
//                &row.value == filter
//            }
//        }
//    });
//}

/// Derive the basic traits and functions for a type to be a table in shoal
#[proc_macro_derive(ShoalTable, attributes(shoal_table))]
pub fn derive_shoal_table(stream: TokenStream) -> TokenStream {
    // parse our target struct
    let ast = syn::parse_macro_input!(stream as syn::DeriveInput);
    // get the name of our struct
    let name = &ast.ident;
    // we only support structs right now
    match &ast.data {
        Data::Struct(DataStruct { .. }) => (),
        _ => unimplemented!("Only structs are currently supported"),
    }
    // start with an empty stream
    let mut output = quote! {};
    let attrs =
        ShoalTable::from_attributes(&ast.attrs).expect("Failed to parse ShoalTable attributes");
    // get our db and table name as a ident
    let db_name = Ident::new(&attrs.db, name.span());
    //let table_name = Ident::new(&attrs.name, name.span());
    // build the name of our kinds
    //let query_name = syn::Ident::new(&format!("{}QueryKinds", db_name), name.span());
    let response_name = syn::Ident::new(&format!("{}ResponseKinds", db_name), name.span());
    // extend this type
    add_from_shoal(&mut output, name, &db_name, &response_name);
    //add_from_for_query(&mut output, name, &query_name);
    //add_shoal_table(&mut output, name);
    // convert and return our stream
    output.into()
}

///// The arguments for a ShoalDB derive
//#[derive(Debug, darling::FromAttributes)]
//#[darling(attributes(shoal_db))]
//struct ShoalDB {
//    /// The name of this database
//    pub name: String,
//}
//
///// Extend a token stream with a ShoalDatabase implementation
/////
///// # Arguments
/////
///// * `stream` - The stream to extend
///// * `name` - The name of the type we are extending
///// * `table_name` - The name of the table
///// * `response_name` - The name of the response type
//fn add_shoal_database(
//    stream: &mut proc_macro2::TokenStream,
//    name: &Ident,
//    query_name: &Ident,
//    response_name: &Ident,
//    archived_response_name: &Ident,
//    tables: &Vec<Ident>,
//) {
//    stream.extend(quote! {
//        #[automatically_derived]
//        impl ShoalDatabase for #name {
//            /// The different tables or types of queries we will handle
//            type QueryKinds = #query_name;
//
//            /// The different tables we can get responses from
//            type ResponseKinds = #response_name;
//
//            /// Deserialize our query types
//            fn unarchive(buff: &[u8]) -> Queries<Self> {
//                // try to cast this query
//                let query = shoal_core::rkyv::check_archived_root::<Queries<Self>>(buff).unwrap();
//                // deserialize it
//                query.deserialize(&mut rkyv::Infallible).unwrap()
//            }
//
//            // Deserialize our response types
//            fn unarchive_response(buff: &[u8]) -> Self::ResponseKinds {
//                // try to cast this query
//                let query = shoal_core::rkyv::check_archived_root::<Self::ResponseKinds>(buff).unwrap();
//                // deserialize it
//                query.deserialize(&mut rkyv::Infallible).unwrap()
//            }
//
//            // Deserialize our response types
//            fn response_query_id(buff: &[u8]) -> &Uuid {
//                // try to cast this query
//                let kinds = shoal_core::rkyv::check_archived_root::<Self::ResponseKinds>(buff).unwrap();
//                // pop our table kinds
//                shoal_core::build_response_query_id_match!(kinds, #archived_response_name, KeyValueRow)
//            }
//
//            /// Get the index of a single [`Self::ResponseKinds`]
//            ///
//            /// # Arguments
//            ///
//            /// * `resp` - The resp to to get the order index for
//            fn response_index(resp: &Self::ResponseKinds) -> usize {
//                shoal_core::build_response_index_match!(resp, #response_name, KeyValueRow)
//            }
//
//            /// Get whether this is the last response in a response stream
//            ///
//            /// # Arguments
//            ///
//            /// * `resp` - The response to check
//            fn is_end_of_stream(resp: &Self::ResponseKinds) -> bool {
//                shoal_core::build_response_end_match!(resp, #response_name, KeyValueRow)
//            }
//
//            /// Forward our queries to the correct shards
//            async fn send_to_shard(
//                ring: &Ring,
//                mesh_tx: &mut Senders<MeshMsg<Self>>,
//                addr: SocketAddr,
//                queries: Queries<Self>,
//            ) -> Result<(), ServerError> {
//                let mut tmp = Vec::with_capacity(1);
//                // get the index for the last query in this bundle
//                let end_index = queries.queries.len() - 1;
//                // crawl over our queries
//                for (index, kind) in queries.queries.into_iter().enumerate() {
//                    // get our target shards info
//                    match &kind {
//                        #query_name::KeyValueQuery(query) => {
//                            // get our shards info
//                            query.find_shard(ring, &mut tmp);
//                        }
//                    };
//                    // send this query to the right shards
//                    for shard_info in tmp.drain(..) {
//                        match &shard_info.contact {
//                            ShardContact::Local(id) => {
//                                //println!("coord - sending query to shard: {id}");
//                                mesh_tx
//                                    .send_to(
//                                        *id,
//                                        MeshMsg::Query {
//                                            addr,
//                                            id: queries.id,
//                                            index,
//                                            query: kind.clone(),
//                                            end: index == end_index,
//                                        },
//                                    )
//                                    .await
//                                    .unwrap();
//                            }
//                        };
//                    }
//                }
//                Ok(())
//            }
//
//            /// Handle messages for different table types
//            async fn handle(
//                &mut self,
//                id: Uuid,
//                index: usize,
//                typed_query: Self::QueryKinds,
//                end: bool,
//            ) -> Self::ResponseKinds {
//                // match on the right query and execute it
//                match typed_query {
//                    #query_name::KeyValueQuery(query) => {
//                        // handle these queries
//                        #response_name::KeyValueRow(self.key_value.handle(id, index, query, end))
//                    }
//                }
//            }
//        }
//    });
//}

//#[proc_macro_derive(ShoalDB, attributes(shoal_db))]
//pub fn derive_shoal_database(stream: TokenStream) -> TokenStream {
//    // parse our target struct
//    let ast = syn::parse_macro_input!(stream as syn::DeriveInput);
//    // get the name of our struct
//    let name = &ast.ident;
//    let attrs = ShoalDB::from_attributes(&ast.attrs).expect("Failed to parse ShoalDB attributes");
//    // build the names for our different derived types
//    let query_name = syn::Ident::new(&format!("{}QueryKinds", attrs.name), name.span());
//    let response_name = syn::Ident::new(&format!("{}ResponseKinds", attrs.name), name.span());
//    let archived_response_name =
//        syn::Ident::new(&format!("Archived{}ResponseKinds", attrs.name), name.span());
//    // we only support structs right now
//    let fields = match &ast.data {
//        Data::Struct(DataStruct { fields, .. }) => fields,
//        _ => unimplemented!("Only structs are currently supported"),
//    };
//    // start with an empty stream
//    let mut output = quote! {};
//    // init a list of the right size for the tables in our database
//    let mut tables: Vec<Ident> = Vec::with_capacity(fields.len());
//    // build a list of our field identies
//    for field in fields {
//        if let syn::Type::Path(path) = &field.ty {
//            if let Some(segment) = path.path.segments.first() {
//                if let PathArguments::AngleBracketed(angled) = &segment.arguments {
//                    if let Some(arg) = angled.args.first() {
//                        if let GenericArgument::Type(type_arg) = arg {
//                            if let syn::Type::Path(path) = type_arg {
//                                if let Some(segment) = path.path.segments.first() {
//                                    // add our field identity
//                                    tables.push(segment.ident.clone());
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//        }
//    }
//    // add a ShoalDatabase implementation
//    add_shoal_database(
//        &mut output,
//        name,
//        &query_name,
//        &response_name,
//        &archived_response_name,
//        &tables,
//    );
//    // convert and return our stream
//    output.into()
//}
