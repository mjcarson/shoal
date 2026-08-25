//! Generate the traits and code required for a database in Shoal

use quote::{format_ident, quote};
use syn::{FieldsNamed, Ident};

use crate::utils;

/// Extend a token stream with a `ShoalDatabase` implementation for a database struct
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `struct_ident` - The name of the database struct
/// * `fields` - The tables in this database
/// * `variants` - The row type of each of those tables
/// * `projections` - The projections each of those tables declared
pub fn add(
    stream: &mut proc_macro2::TokenStream,
    struct_ident: &Ident,
    fields: &FieldsNamed,
    variants: &Vec<Ident>,
    projections: &[Vec<Ident>],
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
            // a table with no storage engine has nothing to read back, so it has no loader
            //
            // this is checked before the spawned set rather than after, since a table that
            // claimed a kind it does not need would mark that kind spawned without spawning
            // it and starve a table declared after it that does need one
            let wanted = needed != ::shoal::storage::Loaders::None;
            // only spawn this loader if it is needed and has not yet been spawned
            if wanted && !spawned.contains(&needed) {
                // get the correct load rx channel
                let (_, loader_rx) = loader_channels
                    .entry(needed.clone())
                    .or_insert_with(|| ::shoal::kanal::unbounded_async());
                // spawn this loader
                self.#field_ident
                    .spawn_loader(table_map, loader_rx, shard_local_tx)
                    .await?;
                // add our newly spawned loader to our spawned loader set
                spawned.push(needed);
            }
        }
    });
    // build our recovery stat arms
    let recovery_stats_arms = fields.named.iter().map(|field| {
        // get our field ident
        let field_ident = field.ident.as_ref().unwrap();
        // fold what this tables recovery discarded into our total
        quote! {
            // add this tables counts to the ones we already have
            stats.merge(self.#field_ident.recovery_stats());
        }
    });
    // build our handle query arms
    let handle_arms = fields
        .named
        .iter()
        .zip(projections)
        .map(|(field, declared)| {
            // get our field ident and type
            let field_ident = field.ident.as_ref().unwrap();
            // get the variant name from the inner type
            let variant_ident = utils::extract_inner_table_ident(&field.ty)
                .expect("Failed to extract inner table ident");
            // build the name of this tables projection enum
            let projection_ident = format_ident!("{}Projection", variant_ident);
            // build one arm per projection, each picking up the row type it answers with
            //
            // the table is generic in what it builds, so this match is the only place a
            // projection costs anything at runtime, and it runs once per query rather than
            // once per row
            let projection_arms = declared.iter().map(|projection| {
                quote! {
                    #projection_ident::#projection => {
                        match self.#field_ident.handle::<#projection>(meta, query).await {
                            Some((client, query_id, stamps, response)) => {
                                let wrapped = #response_ident::#projection(response);
                                Some((client, query_id, stamps, wrapped))
                            }
                            None => None,
                        }
                    }
                }
            });
            // build our handle query arm for this field
            quote! {
                #query_ident::#variant_ident(query) => {
                    // a get can ask to be answered with a subset of each rows fields, which
                    // decides the row type this table builds and the variant it answers in
                    match query.projection() {
                        #(#projection_arms)*
                        // a get that named no projection, and every query that is not a get,
                        // answers with whole rows
                        _ => {
                            match self.#field_ident.handle::<#variant_ident>(meta, query).await {
                                Some((client, query_id, stamps, response)) => {
                                    // wrap our response with the right table kind
                                    let wrapped = #response_ident::#variant_ident(response);
                                    Some((client, query_id, stamps, wrapped))
                                }
                                None => None,
                            }
                        }
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
    // build our compaction due arms
    let compaction_due_arms = fields.named.iter().map(|field| {
        // get our field ident and type
        let field_ident = field.ident.as_ref().unwrap();
        // build our compaction due arm for this field
        quote! {
            // one table being due is enough, so stop looking at the first one
            if self.#field_ident.compaction_due() {
                return true;
            }
        }
    });
    // build our handle flushed arms
    let handle_flushed_arms = fields.named.iter().map(|field| {
        // get our field ident and type
        let field_ident = field.ident.as_ref().unwrap();
        // get the variant name from the inner type
        let variant_ident = utils::extract_inner_table_ident(&field.ty)
            .expect("Failed to extract inner table ident");
        // build our handle flushed arm for this field
        quote! {
            // get all flushed queries in their specific format
            let specific = self.#field_ident.get_flushed().await?;
            // wrap and add our specific queries
            let wrapped = specific
                .drain(..)
                .map(|(client, query_id, span, stamps, resp)| (client, query_id, span, stamps, #response_ident::#variant_ident(resp)));
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
                // a load is where an archive is validated, so it can fail on a corrupt one
                match self.#field_ident.load_partition(loaded_kinds.loaded).await? {
                    // nothing was parked on this partition, so there is nobody to release
                    ::shoal::tables::PartitionLoad::Idle => (),
                    ::shoal::tables::PartitionLoad::Loaded(unblocked, generation) => {
                        // build a mark evictable message for this partition so we don't mark this as
                        // evictable until we have completed all blocked queries to prevent load/reloading
                        // the same partition over and over again
                        //
                        // the generation we get back is the newest one that has been compacted, not the
                        // one we are writing in, since the queries we are about to release can write to
                        // this partition and their intents would not be in an archive yet
                        let mark_evict_msg = ::shoal::server::messages::ServerMsg::MarkEvictable { generation, table, partitions: vec![id] };
                        // convert our unblocked queries into shard messages
                        for (meta, unwrapped) in unblocked {
                            // wrap our query
                            let query = #query_ident::#variant_ident(unwrapped);
                            // build our shard message
                            let query_msg = ::shoal::server::messages::ServerMsg::Released { meta, query };
                            // send this message
                            shard_local_tx.send(query_msg).await?;
                        }
                        // send our partition is evictable message after this query is finished
                        shard_local_tx.send(mark_evict_msg).await?;
                    }
                    // this load gave up part way through, so replay what it released carrying
                    // the failure it released them with
                    //
                    // no mark evictable message follows this one, unlike a load that succeeded:
                    // nothing entered this tables partitions and nothing was taken out of the
                    // lru that has to be put back
                    ::shoal::tables::PartitionLoad::Failed(released) => {
                        for (meta, unwrapped) in released {
                            // wrap our query
                            let query = #query_ident::#variant_ident(unwrapped);
                            // build our shard message
                            let query_msg = ::shoal::server::messages::ServerMsg::Released { meta, query };
                            // send this message
                            shard_local_tx.send(query_msg).await?;
                        }
                    }
                }
            }
        }
    });
    // build our fail partition arms
    let fail_partition_arms = fields
        .named
        .iter()
        .zip(variants)
        .map(|(field, variant_ident)| {
        // get our field ident
        let field_ident = field.ident.as_ref().unwrap();
        // build our fail partition arm for this field
        quote! {
            #table_names_ident::#variant_ident => {
                // take the queries that were parked on this partition, if there were any
                if let Some(released) = self.#field_ident.fail_partition(partition_id, error.as_ref()) {
                    // replay each of them, marked to answer without the read that failed
                    //
                    // no mark evictable message follows this one, unlike a load that
                    // succeeded: nothing was read, so nothing entered this tables partitions
                    // and nothing was taken out of the lru that has to be put back
                    for (meta, unwrapped) in released {
                        // wrap our query
                        let query = #query_ident::#variant_ident(unwrapped);
                        // build our shard message
                        let query_msg = ::shoal::server::messages::ServerMsg::Released { meta, query };
                        // send this message
                        shard_local_tx.send(query_msg).await?;
                    }
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
        impl ::shoal::ShoalDatabase for #struct_ident {
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
                shard_archive_map: &::shoal::storage::FullArchiveMap<Self::TableNames>,
                loader_channels: &mut std::collections::HashMap<
                    ::shoal::storage::Loaders,
                    (::shoal::kanal::AsyncSender<::shoal::storage::LoaderMsg<Self::TableNames>>, ::shoal::kanal::AsyncReceiver<::shoal::storage::LoaderMsg<Self::TableNames>>),
                >,
                conf: &::shoal::server::Conf,
                medium_priority: ::shoal::glommio::TaskQueueHandle,
                memory_usage: &std::sync::Arc<std::cell::RefCell<usize>>,
                lru: &std::sync::Arc<std::cell::RefCell<::shoal::lru::LruCache<(Self::TableNames, u64), usize, std::hash::BuildHasherDefault<::shoal::gxhash::GxHasher>>>>,
                shard_local_tx: &::shoal::kanal::AsyncSender<::shoal::server::messages::ServerMsg<Self>>,
            ) -> Result<Self, ::shoal::server::ServerError> {
                let db = #struct_ident {
                    #(#new_arms)*
                };
                Ok(db)
            }

            /// Initialize the different loaders for our storage kinds
            async fn init_storage_loaders(
                &self,
                table_map: &::shoal::storage::FullArchiveMap<Self::TableNames>,
                loader_channels: &mut std::collections::HashMap<
                    ::shoal::storage::Loaders,
                    (::shoal::kanal::AsyncSender<::shoal::storage::LoaderMsg<Self::TableNames>>, ::shoal::kanal::AsyncReceiver<::shoal::storage::LoaderMsg<Self::TableNames>>),
                >,
                shard_local_tx: &::shoal::kanal::AsyncSender<::shoal::server::messages::ServerMsg<Self>>,
            ) -> Result<(), ::shoal::server::ServerError> {
                // create a list to keep track of our spawned loaders
                let mut spawned = Vec::with_capacity(1);
                // spawn this loader if needed
                #(#spawn_loader_arms)*
                Ok(())
            }

            /// Get what replaying every tables intent logs had to discard
            fn recovery_stats(&self) -> ::shoal::storage::RecoveryStats {
                // start with nothing discarded
                let mut stats = ::shoal::storage::RecoveryStats::default();
                // add in what each of our tables recovery discarded
                #(#recovery_stats_arms)*
                stats
            }

            /// Turn one query of an already validated bundle back into one we can execute
            ///
            /// # Arguments
            ///
            /// * `archived` - The query to deserialize, read out of the bundle it arrived in
            fn deserialize_query(
                archived: &<<Self::ClientType as ::shoal::shared::traits::QuerySupport>::QueryKinds as ::shoal::rkyv::Archive>::Archived,
            ) -> Result<
                <Self::ClientType as ::shoal::shared::traits::QuerySupport>::QueryKinds,
                ::shoal::rkyv::rancor::Error,
            > {
                // copy this querys own bytes out of the bundle they arrived in
                ::shoal::rkyv::deserialize::<_, ::shoal::rkyv::rancor::Error>(archived)
            }

            /// Handle messages for different table types
            async fn handle(
                &mut self,
                meta: ::shoal::server::messages::QueryMetadata,
                typed_query: <Self::ClientType as ::shoal::shared::traits::QuerySupport>::QueryKinds,
            ) -> Option<(
                ::shoal::uuid::Uuid,
                ::shoal::uuid::Uuid,
                ::shoal::server::stage_profile::StageStamps,
                <Self::ClientType as ::shoal::shared::traits::QuerySupport>::ResponseKinds,
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
            async fn flush(&mut self) -> Result<(), ::shoal::server::ServerError> {
                #(#flush_arms)*
                Ok(())
            }

            /// Check if any of our tables intent logs are due to be rotated
            fn compaction_due(&self) -> bool {
                #(#compaction_due_arms)*
                // no table has grown past the size it rotates at
                false
            }

            /// Get all flushed messages and send their response back
            ///
            /// # Arguments
            ///
            /// * `flushed` - The flushed response to send back
            async fn handle_flushed(
                &mut self,
                flushed: &mut Vec<(
                    ::shoal::uuid::Uuid,
                    ::shoal::uuid::Uuid,
                    ::shoal::tracing::Span,
                    ::shoal::server::stage_profile::StageStamps,
                    <Self::ClientType as ::shoal::shared::traits::QuerySupport>::ResponseKinds,
                )>,
            ) -> Result<(), ::shoal::server::ServerError> {
                #(#handle_flushed_arms)*
                Ok(())
            }

            /// Load a partition and execute any pending queries
            async fn load_partition(
                &mut self,
                loaded_kinds: ::shoal::server::messages::LoadedPartitionKinds<Self>,
                shard_local_tx: &::shoal::kanal::AsyncSender<::shoal::server::messages::ServerMsg<Self>>,
            ) -> Result<(), ::shoal::server::ServerError> {
                match loaded_kinds.table {
                    #(#load_partition_arms)*
                };
                Ok(())
            }

            /// Release the queries waiting on a partition that could not be read
            async fn fail_partition(
                &mut self,
                table: Self::TableNames,
                partition_id: u64,
                error: Option<::shoal::shared::responses::ResponseError>,
                shard_local_tx: &::shoal::kanal::AsyncSender<::shoal::server::messages::ServerMsg<Self>>,
            ) -> Result<(), ::shoal::server::ServerError> {
                match table {
                    #(#fail_partition_arms)*
                };
                Ok(())
            }

            /// Shutdown this table and flush any data to disk if needed
            async fn shutdown(mut self) -> Result<(), ::shoal::server::ServerError> {
                #(#shutdown_arms)*
                Ok(())
            }
        }
    });
}
