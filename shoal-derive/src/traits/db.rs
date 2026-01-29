//! Generate the traits and code required for a database in Shoal

use quote::{quote, format_ident};
use syn::{Ident, FieldsNamed};

use crate::utils;


pub fn add(
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
    // build our spawn loader kind arms
    let spawn_loader_arms = fields.named.iter().map(|field| {
        // get our field ident and type
        let field_ident = field.ident.as_ref().unwrap();
        // build our spawn loader arm for this field
        quote! {
            // get the storage engine this table needs
            let needed = self.#field_ident.loader_kind();
            // only spawn this loader if it has not yet been spawned
            if !spawned.contains(&needed) {
                // get the correct load rx channel
                let (_, loader_rx) = loader_channels
                    .entry(needed.clone())
                    .or_insert_with(|| kanal::unbounded_async());
                // spawn this loader
                self.#field_ident
                    .spawn_loader(table_map, loader_rx, shard_local_tx)
                    .await?;
                // add our newly spawned loader to our spawned loader set
                spawned.push(needed);
            }
        }
    });
    // build our handle query arms
    let handle_arms = fields.named.iter().map(|field| {
        // get our field ident and type
        let field_ident = field.ident.as_ref().unwrap();
        // convert this field name to pascal case
        let variant_str = utils::snake_to_pascal_case(&field_ident.to_string());
        // convert our variant name to an ident
        let variant_ident = format_ident!("{variant_str}");
        // build our handle query arm for this field
        quote! {
            #query_ident::#variant_ident(query) => {
                // handle these queries
                match self.#field_ident.handle(meta, query).await {
                    Some((client, query_id, response)) => {
                        // wrap our response with the right table kind
                        let wrapped = #response_ident::#variant_ident(response);
                        Some((client, query_id, wrapped))
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
        let variant_str = utils::snake_to_pascal_case(&field_ident.to_string());
        // convert our variant name to an ident
        let variant_ident = format_ident!("{variant_str}");
        // build our handle flushed  arm for this field
        quote! {
            // get all flushed queries in their specific format
            let specific = self.#field_ident.get_flushed().await?;
            // wrap and add our specific queries
            let wrapped = specific
                .drain(..)
                .map(|(client, query_id, span, resp)| (client, query_id, span, #response_ident::#variant_ident(resp)));
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
                    let mark_evict_msg = shoal_core::server::messages::ServerMsg::MarkEvictable { generation, table, partitions: vec![id] };
                    // convert our unblocked queries into shard messages
                    for (meta, unwrapped) in unblocked {
                        // wrap our query
                        let query = #query_ident::#variant_ident(unwrapped);
                        // build our shard message
                        let query_msg = shoal_core::server::messages::ServerMsg::Query { meta, query};
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
        #[automatically_derived]
        impl shoal_core::shared::traits::ShoalDatabase for #struct_ident {
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
                shard_archive_map: &shoal_core::storage::FullArchiveMap<Self::TableNames>,
                loader_channels: &mut std::collections::HashMap<
                    shoal_core::storage::Loaders,
                    (kanal::AsyncSender<shoal_core::storage::LoaderMsg<Self::TableNames>>, kanal::AsyncReceiver<shoal_core::storage::LoaderMsg<Self::TableNames>>),
                >,
                conf: &shoal_core::server::Conf,
                medium_priority: glommio::TaskQueueHandle,
                memory_usage: &std::sync::Arc<std::cell::RefCell<usize>>,
                lru: &std::sync::Arc<std::cell::RefCell<shoal_core::lru::LruCache<(Self::TableNames, u64), usize, std::hash::BuildHasherDefault<shoal_core::gxhash::GxHasher>>>>,
                shard_local_tx: &kanal::AsyncSender<shoal_core::server::messages::ServerMsg<Self>>,
            ) -> Result<Self, shoal_core::server::ServerError> {
                let db = #struct_ident {
                    #(#new_arms)*
                };
                Ok(db)
            }

            /// Initialize the different loaders for our storage kinds
            async fn init_storage_loaders(
                &self,
                table_map: &shoal_core::storage::FullArchiveMap<Self::TableNames>,
                loader_channels: &mut std::collections::HashMap<
                    shoal_core::storage::Loaders,
                    (kanal::AsyncSender<shoal_core::storage::LoaderMsg<Self::TableNames>>, kanal::AsyncReceiver<shoal_core::storage::LoaderMsg<Self::TableNames>>),
                >,
                shard_local_tx: &kanal::AsyncSender<shoal_core::server::messages::ServerMsg<Self>>,
            ) -> Result<(), shoal_core::server::ServerError> {
                // create a list to keep track of our spawned loaders
                let mut spawned = Vec::with_capacity(1);
                // spawn this loader if needed
                #(#spawn_loader_arms)*
                Ok(())
            }

            /// Handle messages for different table types
            async fn handle(
                &mut self,
                meta: shoal_core::server::messages::QueryMetadata,
                typed_query: <Self::ClientType as shoal_core::shared::traits::QuerySupport>::QueryKinds,
            ) -> Option<(
                uuid::Uuid,
                uuid::Uuid,
                <Self::ClientType as shoal_core::shared::traits::QuerySupport>::ResponseKinds,
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
            async fn flush(&self) -> Result<(), shoal_core::server::ServerError> {
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
                    uuid::Uuid,
                    shoal_core::tracing::Span,
                    <Self::ClientType as shoal_core::shared::traits::QuerySupport>::ResponseKinds,
                )>,
            ) -> Result<(), shoal_core::server::ServerError> {
                #(#handle_flushed_arms)*
                Ok(())
            }

            /// Load a partition and execute any pending queries
            async fn load_partition(
                &mut self,
                loaded_kinds: shoal_core::server::messages::LoadedPartitionKinds<Self>,
                shard_local_tx: &kanal::AsyncSender<shoal_core::server::messages::ServerMsg<Self>>,
            ) -> Result<(), shoal_core::server::ServerError> {
                match loaded_kinds.table {
                    #(#load_partition_arms)*
                };
                Ok(())
            }
            

            /// Shutdown this table and flush any data to disk if needed
            async fn shutdown(&mut self) -> Result<(), shoal_core::server::ServerError> {
                #(#shutdown_arms)*
                Ok(())
            }
        }
    });
}
