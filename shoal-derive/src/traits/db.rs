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
    // the borrowed mirror, which a reply built out of resident rows is serialized through
    let response_ref_ident = format_ident!("{struct_ident}ResponseKindsRef");
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
                        // how to serialize a reply whose rows are still in the partitions
                        // holding them, into the variant this projection answers in
                        //
                        // the table calls this while the scan's borrow is alive, which is why
                        // it is handed down rather than the borrowed reply being handed up:
                        // an answer carrying a lifetime cannot leave the table that found it
                        // while `self` is still borrowed for the reply after it
                        fn seal(
                            response: ::shoal::shared::responses::Response<
                                ::shoal::shared::row_ref::RowRef<'_, #projection>,
                            >,
                        ) -> Result<::shoal::rkyv::util::AlignedVec<16>, ::shoal::rkyv::rancor::Error> {
                            ::shoal::rkyv::to_bytes(&#response_ref_ident::#projection(response))
                        }
                        match self.#field_ident.handle::<#projection>(meta, query, seal).await {
                            Some((client, query_id, stamps, answer)) => {
                                // an answer that is still a value is wrapped here; one that is
                                // already bytes was wrapped by `seal` before it became them
                                let wrapped = match answer {
                                    ::shoal::server::messages::Answer::Open(response) => {
                                        ::shoal::server::messages::Answer::Open(
                                            #response_ident::#projection(response),
                                        )
                                    }
                                    ::shoal::server::messages::Answer::Sealed(bytes) => {
                                        ::shoal::server::messages::Answer::Sealed(bytes)
                                    }
                                };
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
                            // the identity projection's sealer, which is the one that matters:
                            // a get that named no projection is the one whose rows can be
                            // answered with where they lie rather than copied first
                            fn seal(
                                response: ::shoal::shared::responses::Response<
                                    ::shoal::shared::row_ref::RowRef<'_, #variant_ident>,
                                >,
                            ) -> Result<::shoal::rkyv::util::AlignedVec<16>, ::shoal::rkyv::rancor::Error> {
                                ::shoal::rkyv::to_bytes(&#response_ref_ident::#variant_ident(response))
                            }
                            match self.#field_ident.handle::<#variant_ident>(meta, query, seal).await {
                                Some((client, query_id, stamps, answer)) => {
                                    // wrap our response with the right table kind, unless the
                                    // table already sealed it into those bytes itself
                                    let wrapped = match answer {
                                        ::shoal::server::messages::Answer::Open(response) => {
                                            ::shoal::server::messages::Answer::Open(
                                                #response_ident::#variant_ident(response),
                                            )
                                        }
                                        ::shoal::server::messages::Answer::Sealed(bytes) => {
                                            ::shoal::server::messages::Answer::Sealed(bytes)
                                        }
                                    };
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
                if let Some(released) = self.#field_ident.fail_partition(partition_id, span, error.as_ref()) {
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
    // build our write command arms: the table, the partition key and the serialized intent
    let write_command_arms = fields
        .named
        .iter()
        .zip(variants)
        .map(|(field, variant_ident)| {
            let field_ident = field.ident.as_ref().unwrap();
            let row_ident = utils::extract_inner_table_ident(&field.ty)
                .expect("Failed to extract inner table ident");
            quote! {
                #query_ident::#row_ident(query) => self
                    .#field_ident
                    .build_intent(query)
                    .map(|(key, payload)| (#table_names_ident::#variant_ident, key, payload)),
            }
        });
    // build our apply command arms
    let apply_command_arms = fields.named.iter().zip(variants).map(|(field, variant_ident)| {
        let field_ident = field.ident.as_ref().unwrap();
        quote! {
            #table_names_ident::#variant_ident => self.#field_ident.apply(command, generation, skip_disk),
        }
    });
    // build our write response arms, in each table's own variant
    let write_response_arms = fields.named.iter().zip(variants).map(|(field, variant_ident)| {
        let row_ident = utils::extract_inner_table_ident(&field.ty)
            .expect("Failed to extract inner table ident");
        quote! {
            #table_names_ident::#variant_ident => {
                let data = match result.kind {
                    ::shoal::server::replication::ResultKind::Insert => ::shoal::shared::responses::ResponseAction::Insert(result.ok),
                    ::shoal::server::replication::ResultKind::Delete => ::shoal::shared::responses::ResponseAction::Delete(result.ok),
                    ::shoal::server::replication::ResultKind::Update => ::shoal::shared::responses::ResponseAction::Update(result.ok),
                    // a scrub is proposed by the shard and never answered to a client
                    ::shoal::server::replication::ResultKind::Scrub => ::shoal::shared::responses::ResponseAction::Insert(result.ok),
                };
                #response_ident::#row_ident(::shoal::shared::responses::Response::<#row_ident> { id, index, data, end })
            }
        }
    });
    // build our request load arms
    let request_load_arms = fields.named.iter().zip(variants).map(|(field, variant_ident)| {
        let field_ident = field.ident.as_ref().unwrap();
        quote! {
            #table_names_ident::#variant_ident => self.#field_ident.request_load(partition_key, span).await,
        }
    });
    // build our compaction sink arms
    let compaction_sink_arms = fields
        .named
        .iter()
        .zip(variants)
        .map(|(field, variant_ident)| {
            let field_ident = field.ident.as_ref().unwrap();
            quote! {
                if let Some(sink) = self.#field_ident.compaction_sink() {
                    sinks.push((#table_names_ident::#variant_ident, sink));
                }
            }
        });
    // build our digest arms
    let digest_arms = fields
        .named
        .iter()
        .zip(variants)
        .map(|(field, variant_ident)| {
            let field_ident = field.ident.as_ref().unwrap();
            quote! {
                #table_names_ident::#variant_ident => self.#field_ident.digest().await,
            }
        });
    // build our canonical cut arms
    let canonical_cut_arms = fields.named.iter().zip(variants).map(|(field, variant_ident)| {
        let field_ident = field.ident.as_ref().unwrap();
        quote! {
            #table_names_ident::#variant_ident => self.#field_ident.canonical_cut(tablets).await,
        }
    });
    // build our snapshot partition arms
    let snapshot_partition_arms = fields.named.iter().zip(variants).map(|(field, variant_ident)| {
        let field_ident = field.ident.as_ref().unwrap();
        quote! {
            #table_names_ident::#variant_ident => self.#field_ident.snapshot_partitions(tablets),
        }
    });
    // build our evict tablets arms
    let evict_tablets_arms = fields
        .named
        .iter()
        .zip(variants)
        .map(|(field, variant_ident)| {
            let field_ident = field.ident.as_ref().unwrap();
            quote! {
                #table_names_ident::#variant_ident => self.#field_ident.evict_tablets(tablets),
            }
        });
    // build our install partitions arms
    let install_partitions_arms = fields.named.iter().zip(variants).map(|(field, variant_ident)| {
        let field_ident = field.ident.as_ref().unwrap();
        quote! {
            #table_names_ident::#variant_ident => self.#field_ident.install_partitions(tablets, records),
        }
    });
    // build our fold intents arms, one per table, each naming its own type so the engine is
    // reached through the table's partition and row types
    let fold_intents_arms = fields.named.iter().zip(variants).map(|(field, variant_ident)| {
        let field_type = &field.ty;
        quote! {
            #table_names_ident::#variant_ident => <#field_type>::fold_intents(shard_name, #table_names_ident::#variant_ident, conf).await,
        }
    });
    // build our export arms, one per table, each naming its own type the same way
    let export_archives_arms = fields.named.iter().zip(variants).map(|(field, variant_ident)| {
        let field_type = &field.ty;
        quote! {
            #table_names_ident::#variant_ident => <#field_type>::export_archives(shard_names, conf, path, provenance, group, schema_id).await,
        }
    });
    // build our table-of-id arms
    let table_of_id_arms = variants.iter().map(|variant_ident| {
        quote! {
            if id == ::shoal::shared::traits::TableNameSupport::table_id(&#table_names_ident::#variant_ident) {
                return Some(#table_names_ident::#variant_ident);
            }
        }
    });
    // the names of the persistent tables, read off the field types
    let persistent_names: Vec<String> = fields
        .named
        .iter()
        .filter(|field| utils::is_persistent_table(&field.ty))
        .map(|field| {
            utils::extract_inner_table_ident(&field.ty)
                .expect("Failed to extract inner table ident")
                .to_string()
        })
        .collect();
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
                ::shoal::server::messages::Answer<
                    <Self::ClientType as ::shoal::shared::traits::QuerySupport>::ResponseKinds,
                >,
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
                span: &::shoal::tracing::Span,
                error: Option<::shoal::shared::responses::ResponseError>,
                shard_local_tx: &::shoal::kanal::AsyncSender<::shoal::server::messages::ServerMsg<Self>>,
            ) -> Result<(), ::shoal::server::ServerError> {
                match table {
                    #(#fail_partition_arms)*
                };
                Ok(())
            }

            /// The command a write proposes through its tablet group, or none for a read
            fn write_command(
                &self,
                query: &<Self::ClientType as ::shoal::shared::traits::QuerySupport>::QueryKinds,
            ) -> Option<(Self::TableNames, u64, Vec<u8>)> {
                match query {
                    #(#write_command_arms)*
                }
            }

            /// Apply a committed command to the table it names
            fn apply_command(
                &mut self,
                table: Self::TableNames,
                command: &::shoal::shared::protocol::peer::Command,
                generation: u64,
                skip_disk: bool,
            ) -> ::shoal::tables::ApplyStep {
                match table {
                    #(#apply_command_arms)*
                }
            }

            /// The response a proposal's result is answered with
            fn write_response(
                table: Self::TableNames,
                id: ::shoal::uuid::Uuid,
                index: usize,
                end: bool,
                result: ::shoal::server::replication::CommandResult,
            ) -> <Self::ClientType as ::shoal::shared::traits::QuerySupport>::ResponseKinds {
                match table {
                    #(#write_response_arms)*
                }
            }

            /// Ask a table for a partition a replicated apply needs
            async fn request_load(
                &mut self,
                table: Self::TableNames,
                partition_key: u64,
                span: &::shoal::tracing::Span,
            ) -> Result<bool, ::shoal::server::ServerError> {
                match table {
                    #(#request_load_arms)*
                }
            }

            /// Every table's compactor channel
            fn compaction_sinks(&self) -> Vec<(Self::TableNames, ::shoal::kanal::AsyncSender<::shoal::storage::CompactionJob>)> {
                let mut sinks = Vec::new();
                #(#compaction_sink_arms)*
                sinks
            }

            /// Hash a table's applied state, archived partitions included
            async fn digest_table(&self, table: Self::TableNames) -> Result<(u64, u64), ::shoal::server::ServerError> {
                match table {
                    #(#digest_arms)*
                }
            }

            /// Take a canonical cut of some tablets of a table, for a scrub
            async fn canonical_cut(&self, table: Self::TableNames, tablets: &[u16]) -> Result<::shoal::server::replication::PendingDigest, ::shoal::server::ServerError> {
                match table {
                    #(#canonical_cut_arms)*
                }
            }

            /// Every resident partition of some tablets of a table, for a volatile snapshot
            fn snapshot_partitions(&self, table: Self::TableNames, tablets: &[u16]) -> Vec<(u64, Vec<u8>)> {
                match table {
                    #(#snapshot_partition_arms)*
                }
            }

            /// Drop every resident partition of some tablets of a table
            fn evict_tablets(&mut self, table: Self::TableNames, tablets: &[u16]) {
                match table {
                    #(#evict_tablets_arms)*
                }
            }

            /// Replace every resident partition of some tablets of a table with a snapshot's records
            fn install_partitions(&mut self, table: Self::TableNames, tablets: &[u16], records: Vec<(u64, Vec<u8>)>) -> Result<(), ::shoal::server::ServerError> {
                match table {
                    #(#install_partitions_arms)*
                }
            }

            /// The table a stable identity names
            fn table_of_id(id: ::shoal::shared::identity::TableId) -> Option<Self::TableNames> {
                #(#table_of_id_arms)*
                None
            }

            /// The names of every persistent table
            fn persistent_tables() -> Vec<&'static str> {
                vec![#(#persistent_names),*]
            }

            /// Fold a shard's intent logs of a table into its archives, with no table built
            async fn fold_intents(shard_name: &str, table: Self::TableNames, conf: &::shoal::server::Conf) -> Result<u64, ::shoal::server::ServerError> {
                match table {
                    #(#fold_intents_arms)*
                }
            }

            /// Write every archived partition some shards hold of a table as one snapshot file
            async fn export_archives(
                shard_names: &[String],
                table: Self::TableNames,
                conf: &::shoal::server::Conf,
                path: &::std::path::Path,
                provenance: &::shoal::server::replication::snapshot::SnapshotProvenance,
                group: ::shoal::shared::identity::GroupId,
                schema_id: u64,
            ) -> Result<::shoal::server::replication::snapshot::SnapshotManifest, ::shoal::server::ServerError> {
                match table {
                    #(#export_archives_arms)*
                }
            }

            /// Shutdown this table and flush any data to disk if needed
            async fn shutdown(mut self) -> Result<(), ::shoal::server::ServerError> {
                #(#shutdown_arms)*
                Ok(())
            }
        }
    });
}
