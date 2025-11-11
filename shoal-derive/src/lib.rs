extern crate proc_macro;

use darling::FromAttributes;
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DataStruct, Fields, FieldsNamed, Ident};

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
                type ResponseKinds = <#db_name as shoal_core::shared::traits::QuerySupport>::ResponseKinds;

                fn retrieve(kind: &#response_name) -> Result<&rkyv::option::ArchivedOption<rkyv::vec::ArchivedVec<<Self as Archive>::Archived>>, shoal_core::client::Errors> {
                    // make sure its the right data kind
                    if let #response_name::#name(action) = kind {
                        // make sure its a get action
                       if let shoal_core::shared::responses::ArchivedResponseAction::Get(rows) = &action.data {
                            return Ok(rows);
                        }
                    }
                    Err(shoal_core::client::Errors::WrongType("Wrong Type!".to_owned()))
                }
            }
        }
    );
}

/// Extend a token stream with a RkyvSupport impl
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
fn add_rkyv_support(stream: &mut proc_macro2::TokenStream, name: &Ident) {
    // extend our token stream
    stream.extend(quote! {
        #[automatically_derived]
        impl shoal_core::shared::traits::RkyvSupport for #name {}
    });
}

/// Extend a token stream with a From<#name> for *SortedQueryKinds implementation
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `query_name` - The name of the query type
fn add_from_for_sorted_query(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    query_name: &Ident,
) {
    // extend our token stream
    stream.extend(quote! {
        #[automatically_derived]
        impl From<#name> for #query_name {
            fn from(row: #name) -> #query_name {
                // get our rows partition key
                let key = #name::get_partition_key(&row);
                // build our query kind
                #query_name::#name(shoal_core::shared::queries::SortedQuery::Insert { key, row })
            }
        }
    });
}

/// Extend a token stream with a From<#name> for *UnsortedQueryKinds implementation
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `query_name` - The name of the query type
fn add_from_for_unsorted_query(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    query_name: &Ident,
) {
    // extend our token stream
    stream.extend(quote! {
        #[automatically_derived]
        impl From<#name> for #query_name {
            fn from(row: #name) -> #query_name {
                // get our rows partition key
                let key = #name::get_partition_key(&row);
                // build our query kind
                #query_name::#name(shoal_core::shared::queries::UnsortedQuery::Insert { key, row })
            }
        }
    });
}

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
#[proc_macro_derive(ShoalSortedTable, attributes(shoal_table))]
pub fn derive_shoal_sorted_table(stream: TokenStream) -> TokenStream {
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
    // get our db and table name as a ident
    //let table_name = Ident::new(&attrs.name, name.span());
    // build the name of our kinds
    let query_name = syn::Ident::new(&format!("{}QueryKinds", db_name), name.span());
    let response_name = syn::Ident::new(&format!("Archived{}ResponseKinds", db_name), name.span());
    // extend this type
    add_from_shoal(&mut output, name, &db_name, &response_name);
    add_rkyv_support(&mut output, name);
    add_from_for_sorted_query(&mut output, name, &query_name);
    //add_shoal_table(&mut output, name);
    // convert and return our stream
    output.into()
}

/// Derive the basic traits and functions for a type to be a table in shoal
#[proc_macro_derive(ShoalUnsortedTable, attributes(shoal_table))]
pub fn derive_shoal_unsorted_table(stream: TokenStream) -> TokenStream {
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
    let client_name = syn::Ident::new(&format!("{}Client", db_name), name.span());
    // build the name of our kinds
    let query_name = syn::Ident::new(&format!("{}QueryKinds", db_name), name.span());
    let response_name = syn::Ident::new(&format!("Archived{}ResponseKinds", db_name), name.span());
    // extend this type
    add_from_shoal(&mut output, name, &client_name, &response_name);
    add_rkyv_support(&mut output, name);
    add_from_for_unsorted_query(&mut output, name, &query_name);
    //add_shoal_table(&mut output, name);
    // convert and return our stream
    output.into()
}

//fn add_query_kinds(name: &Ident) -> proc_macro2::TokenStream {
//    // build the query kinds type to set
//    quote!(
//        type QueryKinds = concat!(#name, QueryKinds);
//    )
//}
//
//fn add_db_new(name: &Ident, fields: &Fields) -> proc_macro2::TokenStream {
//    // build the entry for each field name and type
//    let field_iter = fields.iter().map(|field| {
//        // get this fields name
//        let ident = &field.ident;
//        let ftype = format_ident!("{}", stringify!(field.ty).split_once('<').unwrap().0);
//        // build this fields entry
//        quote! { #ident: #ftype::new(shared_name, conf).await? }
//    });
//    // build the query kinds type to set
//    quote!(
//        async fn new(shard_name: &str, conf: &Conf) -> Result<Self, ServerError> {
//            let db = #name {
//            //    key_value: PersistentTable::new(shard_name, conf).await?,
//                #(#field_iter,)*
//            };
//            Ok(db)
//        }
//    )
//}
//
//fn add_db_trait(name: &Ident, fields: &Fields, stream: &mut proc_macro2::TokenStream) {
//    // build our new idents
//    let query_kinds = format_ident!("{}QueryKinds", name);
//    let response_kinds = format_ident!("{}ResponseKinds", name);
//    // build our different function implementations
//    let new = add_db_new(name, fields);
//    // build the entry for each field name and type
//    let field_iter = fields.iter().map(|field| {
//        // get this fields name
//        let ident = &field.ident;
//        let ftype = format_ident!("{}", stringify!(field.ty).split_once('<').unwrap().0);
//        // build this fields entry
//        quote! { #ident: #ftype::new(shared_name, conf).await? }
//    });
//    stream.extend(quote! {
//        impl ShoalDatabase for Basic {
//            /// The different tables or types of queries we will handle
//            type QueryKinds = #query_kinds;
//
//            /// The different tables we can get responses from
//            type ResponseKinds = #response_kinds;
//
//            /// Create a new shoal db instance
//            ///
//            /// # Arguments
//            ///
//            /// * `shard_name` - The id of the shard that owns this table
//            /// * `conf` - A shoal config
//            //#new
//            async fn new(shard_name: &str, conf: &Conf) -> Result<Self, ServerError> {
//                let db = Basic {
//                    key_value: PersistentTable::new(shard_name, conf).await?,
//                };
//                Ok(db)
//            }
//
//            /// Handle messages for different table types
//            async fn handle(
//                &mut self,
//                meta: QueryMetadata,
//                typed_query: Self::QueryKinds,
//            ) -> Option<(SocketAddr, Self::ResponseKinds)> {
//                // match on the right query and execute it
//                match typed_query {
//                    BasicQueryKinds::KeyValue(query) => {
//                        // handle these queries
//                        match self.key_value.handle(meta, query).await {
//                            Some((addr, response)) => {
//                                // wrap our response with the right table kind
//                                let wrapped = BasicResponseKinds::KeyValue(response);
//                                Some((addr, wrapped))
//                            }
//                            None => None,
//                        }
//                    }
//                }
//            }
//
//            /// Flush any in flight writes to disk
//            async fn flush(&mut self) -> Result<(), ServerError> {
//                self.key_value.flush().await
//            }
//
//            /// Get all flushed messages and send their response back
//            ///
//            /// # Arguments
//            ///
//            /// * `flushed` - The flushed response to send back
//            fn handle_flushed(&mut self, flushed: &mut Vec<(SocketAddr, Self::ResponseKinds)>) {
//                // get all flushed queries in their specific format
//                let specific = self.key_value.get_flushed();
//                // wrap and add our specific queries
//                let wrapped = specific
//                    .drain(..)
//                    .map(|(addr, resp)| (addr, BasicResponseKinds::KeyValue(resp)));
//                // extend our response list with our wrapped queries
//                flushed.extend(wrapped);
//            }
//
//            /// Shutdown this table and flush any data to disk if needed
//            async fn shutdown(&mut self) -> Result<(), ServerError> {
//                // shutdown the key value table
//                self.key_value.shutdown().await
//            }
//        }
//
//    });
//}

/// Convert snake case strings to pascal case
///
/// # Arguments
///
/// * `snake_case` - The snake case string to convert
fn snake_to_pascal_case(snake_case: &str) -> String {
    snake_case
        .split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => {
                    first.to_uppercase().collect::<String>() + &chars.as_str().to_lowercase()
                }
            }
        })
        .collect()
}
/// Get the variant names for our enum
fn get_variant_names(fields: &FieldsNamed) -> Vec<Ident> {
    // Extract field names and convert to PascalCase for enum variants
    fields
        .named
        .iter()
        .filter_map(|field| field.ident.as_ref())
        .map(|field_name| {
            // Convert snake_case to PascalCase
            let variant_name = snake_to_pascal_case(&field_name.to_string());
            format_ident!("{}", variant_name)
        })
        .collect()
}

/// Build the enum for this struct
fn build_enum(stream: &mut proc_macro2::TokenStream, enum_ident: &Ident, variants: &Vec<Ident>) {
    // Generate the enum and add it to our token stream
    stream.extend(quote! {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum #enum_ident {
            #(#variants),*
        }
    });
}

/// Build our table name support impl
fn add_table_name_support(stream: &mut proc_macro2::TokenStream, enum_ident: &Ident) {
    // Generate and add a table name support impl
    stream.extend(quote! {
        impl shoal_core::shared::traits::TableNameSupport for #enum_ident {}
    });
}

/// Build the FromStr implementation
fn build_display(stream: &mut proc_macro2::TokenStream, enum_ident: &Ident, variants: &Vec<Ident>) {
    // get the variant names as a string
    let variant_names: Vec<_> = variants
        .iter()
        .map(|variant_name| variant_name.to_string())
        .collect();
    // build our from str arms
    let to_str_arms = variants
        .iter()
        .zip(&variant_names)
        .map(|(variant, variant_name)| {
            quote! {
                #enum_ident::#variant => write!(f, "{}", #variant_name),
            }
        });
    // add our FromStr impl to our token stream
    stream.extend(quote! {
        impl std::fmt::Display for #enum_ident {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    #(#to_str_arms)*
                }
            }
        }
    });
}

fn add_db_trait2(
    stream: &mut proc_macro2::TokenStream,
    struct_ident: &Ident,
    fields: &FieldsNamed,
    variants: &Vec<Ident>,
) {
    // build our new idents
    let client_ident = format_ident!("{}Client", struct_ident);
    let table_names_ident = format_ident!("{}TableNames", struct_ident);
    let query_ident = format_ident!("{struct_ident}QueryKinds");
    let response_ident = format_ident!("{struct_ident}ResponseKinds");
    // build our new table arms
    // get the field idents
    let new_arms = fields.named.iter().zip(variants.iter()).map(|(field, variant)| {
        // get our field ident and type
        let field_ident = field.ident.as_ref().unwrap();
        let field_type = &field.ty;
        // build our new arm for this field
        quote! {
            #field_ident: <#field_type>::new(shard_name, #table_names_ident::#variant, #table_names_ident::#variant, shard_archive_map, loader_channels, conf, medium_priority, memory_usage, lru, shard_local_tx).await?,
        }
    });
    // build our handle query arms
    let handle_arms = fields.named.iter().map(|field| {
        // get our field ident and type
        let field_ident = field.ident.as_ref().unwrap();
        // convert this field name to pascal case
        let variant_str = snake_to_pascal_case(&field_ident.to_string());
        // convert our variant name to an ident
        let variant_ident = format_ident!("{variant_str}");
        // build our handle query arm for this field
        quote! {
            #query_ident::#variant_ident(query) => {
                // handle these queries
                match self.#field_ident.handle(meta, query).await {
                    Some((client, response)) => {
                        // wrap our response with the right table kind
                        let wrapped = #response_ident::#variant_ident(response);
                        Some((client, wrapped))
                    }
                    None => None,
                }
            },
        }
    });
    // build our mark evictable partitions arms
    let mark_evictable_arms = fields
        .named
        .iter()
        .zip(variants)
        .map(|(field, variant_ident)| {
        // get our field ident and type
        let field_ident = field.ident.as_ref().unwrap();
        // build our evict partition arm for this table
        quote! {
            #table_names_ident::#variant_ident=> self.#field_ident.mark_evictable(generation, partitions),
        }
    });
    // build our evict partitions arms
    let evict_arms = fields
        .named
        .iter()
        .zip(variants)
        .map(|(field, variant_ident)| {
        // get our field ident and type
        let field_ident = field.ident.as_ref().unwrap();
        // build our evict partition arm for this table
        quote! {
            #table_names_ident::#variant_ident=> self.#field_ident.evict(victims),
        }
    });
    // build our flush arms
    let flush_arms = fields.named.iter().map(|field| {
        // get our field ident and type
        let field_ident = field.ident.as_ref().unwrap();
        // build our flush arm for this field
        quote! {
            self.#field_ident.flush().await?;
        }
    });
    // build our handle flushed arms
    let handle_flushed_arms = fields.named.iter().map(|field| {
        // get our field ident and type
        let field_ident = field.ident.as_ref().unwrap();
        // convert this field name to pascal case
        let variant_str = snake_to_pascal_case(&field_ident.to_string());
        // convert our variant name to an ident
        let variant_ident = format_ident!("{variant_str}");
        // build our handle flushed  arm for this field
        quote! {
            // get all flushed queries in their specific format
            let specific = self.#field_ident.get_flushed().await?;
            // wrap and add our specific queries
            let wrapped = specific
                .drain(..)
                .map(|(client, resp)| (client, #response_ident::#variant_ident(resp)));
            // extend our response list with our wrapped queries
            flushed.extend(wrapped);
        }
    });
    // build our load partition arms
    let load_partition_arms = fields
        .named
        .iter()
        .zip(variants)
        .map(|(field, variant_ident)| {
        // get our field ident and type
        let field_ident = field.ident.as_ref().unwrap();
        // build our load partition arm for this field
        quote! {
            #table_names_ident::#variant_ident=> {
                //  get this partition loads table name and partition id
                let table = loaded_kinds.table;
                let id = loaded_kinds.loaded.partition_id;
                if let Some((unblocked, generation)) = self.#field_ident.load_partition(loaded_kinds.loaded).await {
                    // build a mark evictable message for this partition so we don't mark this as
                    // evictable until we have completed all blocked queries to prevent load/reloading
                    // the same partition over and over again
                    let mark_evict_msg = ShardMsg::MarkEvictable { generation, table, partitions: vec![id] };
                    // convert our unblocked queries into shard messages
                    for (meta, unwrapped) in unblocked {
                        // wrap our query
                        let query = #query_ident::#variant_ident(unwrapped);
                        // build our shard message
                        let query_msg = ShardMsg::Query { meta, query };
                        // send this message
                        shard_local_tx.send(query_msg).await.unwrap();
                    }
                    // send our partition is evictable message after this query is finished
                    shard_local_tx.send(mark_evict_msg).await.unwrap();
                }
            }
        }
    });
    // build our shutdown arms
    let shutdown_arms = fields.named.iter().map(|field| {
        // get our field ident and type
        let field_ident = field.ident.as_ref().unwrap();
        // build our shutdown arm for this field
        quote! {
            self.#field_ident.shutdown().await?;
        }
    });
    // build our ShoalDatabase impl
    stream.extend(quote! {
        impl ShoalDatabase for #struct_ident {
            /// This databases external client type
            type ClientType = #client_ident;

            /// The different tables in this database
            type TableNames = #table_names_ident;

            /// Create a new shoal db instance
            ///
            /// # Arguments
            ///
            /// * `shard_name` - The id of the shard that owns this table
            /// * `conf` - A shoal config
            async fn new(
                shard_name: &str,
                shard_archive_map: &FullArchiveMap<Self::TableNames>,
                loader_channels: &mut HashMap<
                    Loaders,
                    (AsyncSender<LoaderMsg<Self::TableNames>>, AsyncReceiver<LoaderMsg<Self::TableNames>>),
                >,
                conf: &Conf,
                medium_priority: TaskQueueHandle,
                memory_usage: &std::sync::Arc<std::cell::RefCell<usize>>,
                lru: &std::sync::Arc<std::cell::RefCell<shoal_core::lru::LruCache<(Self::TableNames, u64), usize, std::hash::BuildHasherDefault<shoal_core::xxhash_rust::xxh3::Xxh3>>>>,
                shard_local_tx: &AsyncSender<ShardMsg<Self>>,
            ) -> Result<Self, ServerError> {
                let db = Tmdb {
                    #(#new_arms)*
                };
                Ok(db)
            }

            /// Initialize the different loaders for our storage kinds
            async fn init_storage_loaders(
                &self,
                table_map: &FullArchiveMap<Self::TableNames>,
                loader_channels: &mut HashMap<
                    Loaders,
                    (AsyncSender<LoaderMsg<Self::TableNames>>, AsyncReceiver<LoaderMsg<Self::TableNames>>),
                >,
                shard_local_tx: &AsyncSender<ShardMsg<Self>>,
            ) -> Result<(), ServerError> {
                // create a list to keep track of our spawned loaders
                let mut spawned = Vec::with_capacity(1);
                // get the storage engine this table needs
                let needed = self.movie.loader_kind();
                // only spawn this loader if it has not yet been spawned
                if !spawned.contains(&needed) {
                    // get this tables loader kind
                    let loader_kind = self.movie.loader_kind();
                    // get the correct load rx channel
                    let (_, loader_rx) = loader_channels
                        .entry(loader_kind)
                        .or_insert_with(|| kanal::unbounded_async());
                    // spawn this loader
                    self.movie
                        .spawn_loader(table_map, loader_rx, shard_local_tx)
                        .await?;
                    // add our newly spawned loader to our spawned loader set
                    spawned.push(needed);
                }
                Ok(())
            }

            /// Handle messages for different table types
            async fn handle(
                &mut self,
                meta: QueryMetadata,
                typed_query: <Self::ClientType as QuerySupport>::QueryKinds,
            ) -> Option<(
                uuid::Uuid,
                <Self::ClientType as QuerySupport>::ResponseKinds,
            )> {
                // match on the right query and execute it
                match typed_query {
                    #(#handle_arms)*
                }
            }

            /// Mark partitions as evictable if they are no longer in the intent log
            ///
            /// # Arguments
            ///
            /// * `table_name` - The name of the table with the partition to mark as evictable
            /// * `generation` - The generation of data to mark as evictable
            /// * `partitions` - The partitions to mark as evictable
            fn mark_evictable(
                &mut self,
                table_name: Self::TableNames,
                generation: u64,
                partitions: Vec<u64>,
            ) {
                match table_name {
                    #(#mark_evictable_arms)*
                }    
            }


            /// Evict specific partitions from a table
            ///
            /// # Arguments
            ///
            /// * `table_name` - The name of the table to evict data from
            /// * `victims` - The partitions to evict
            fn evict(&mut self, table_name: Self::TableNames, victims: Vec<u64>) {
                match table_name {
                    #(#evict_arms)*
                }    
            }

            /// Flush any in flight writes to disk
            async fn flush(&mut self) -> Result<(), ServerError> {
                #(#flush_arms)*
                Ok(())
            }

            /// Get all flushed messages and send their response back
            ///
            /// # Arguments
            ///
            /// * `flushed` - The flushed response to send back
            async fn handle_flushed(
                &mut self,
                flushed: &mut Vec<(
                    uuid::Uuid,
                    <Self::ClientType as QuerySupport>::ResponseKinds,
                )>,
            ) -> Result<(), ServerError> {
                #(#handle_flushed_arms)*
                Ok(())
            }

            /// Load a partition and execute any pending queries
            async fn load_partition(
                &mut self,
                loaded_kinds: shoal_core::server::messages::LoadedPartitionKinds<Self>,
                shard_local_tx: &AsyncSender<ShardMsg<Self>>,
            ) -> Result<(), ServerError> {
                match loaded_kinds.table {
                    #(#load_partition_arms)*
                };
                Ok(())
            }
            

            /// Shutdown this table and flush any data to disk if needed
            async fn shutdown(&mut self) -> Result<(), ServerError> {
                #(#shutdown_arms)*
                Ok(())
            }
        }
    });
}

// Add a client for this database
fn add_client(stream: &mut proc_macro2::TokenStream, struct_ident: &Ident) {
    // build our new idents
    let client_ident = format_ident!("{}Client", struct_ident);
    let query_ident = format_ident!("{struct_ident}QueryKinds");
    let response_ident = format_ident!("{struct_ident}ResponseKinds");
    // add our client struct and query support for the client
    stream.extend(quote! {
        pub struct #client_ident {}

        impl QuerySupport for #client_ident {
            /// The different tables or types of queries we will handle
            type QueryKinds = #query_ident;

            /// The different tables we can get responses from
            type ResponseKinds = #response_ident;
        }
    });
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
                    let variants = get_variant_names(fields);
                    // build the enum from our variant names
                    build_enum(&mut output, &enum_ident, &variants);
                    // add TableNameSupport to this enum
                    add_table_name_support(&mut output, &enum_ident);
                    // add display support to this enum
                    build_display(&mut output, &enum_ident, &variants);
                    // add ShoalDatabase support to our root struct
                    add_db_trait2(&mut output, struct_ident, fields, &variants);
                    // add our client
                    add_client(&mut output, struct_ident);
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
    //// we only support structs right now
    //let struct_data = match &ast.data {
    //    Data::Struct(struct_data) => struct_data,
    //    _ => unimplemented!("Only structs are currently supported"),
    //};
    //let fields = &struct_data.fields;
    // start with an empty stream
    //let mut output = quote! {};
    //// add our shoal db trait
    //add_db_trait(name, fields, &mut output);
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
