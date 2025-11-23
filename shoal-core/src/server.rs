use clap::Parser;
use glommio::{
    channels::channel_mesh::{Full, MeshBuilder},
    ExecutorJoinHandle, LocalExecutorBuilder, Placement, PoolThreadHandles,
};
use kanal::AsyncSender;
use rkyv::rancor::Strategy;
use rkyv::Archive;
use rkyv::{
    bytecheck::CheckBytes,
    de::Pool,
    validation::{archive::ArchiveValidator, shared::SharedValidator, Validator},
};
use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tracing::instrument;

mod args;
mod conf;
//mod coordinator;
pub mod errors;
pub mod messages;
pub mod ring;
pub mod shard;
pub mod tables;
pub mod trace;

pub use conf::Conf;
//use coordinator::Coordinator;
pub use errors::ServerError;
use messages::{MeshMsg, Msg};

use crate::shared::{
    queries::Queries,
    traits::{QuerySupport, ShoalDatabase},
};

///// Spawns our shard coordinator
/////
///// # Arguments
/////
///// * `conf` - The shoal config
///// * `mesh` - The mesh to send  messages over
//fn spawn_coordinator<S: ShoalDatabase>(
//    conf: Conf,
//    mesh: MeshBuilder<MeshMsg<S>, Full>,
//) -> Result<
//    (
//        AsyncSender<Msg<S>>,
//        ExecutorJoinHandle<Result<(), ServerError>>,
//    ),
//    ServerError,
//>
//where
//    <<S::ClientType as QuerySupport>::QueryKinds as Archive>::Archived: rkyv::Deserialize<
//        <S::ClientType as QuerySupport>::QueryKinds,
//        Strategy<Pool, rkyv::rancor::Error>,
//    >,
//    for<'a> <Queries<S::ClientType> as Archive>::Archived: rkyv::bytecheck::CheckBytes<
//        rkyv::rancor::Strategy<
//            rkyv::validation::Validator<
//                rkyv::validation::archive::ArchiveValidator<'a>,
//                rkyv::validation::shared::SharedValidator,
//            >,
//            rkyv::rancor::Error,
//        >,
//    >,
//{
//    // create our kanal chaannels
//    let (kanal_tx, kanal_rx) = kanal::bounded_async(8192);
//    // get a copy of our kanal transmission channel for the coordinator
//    let kanal_tx_coord = kanal_tx.clone();
//    // start our coordinator node
//    let coord_handle = LocalExecutorBuilder::new(glommio::Placement::Fixed(0))
//        .io_memory(300 << 20)
//        .spawn(
//            || async move { Coordinator::<S>::start(conf, mesh, kanal_tx_coord, kanal_rx).await },
//        )?;
//    Ok((kanal_tx, coord_handle))
//}

/// A pool of ShoalDB shards
pub struct ShoalPool<S: ShoalDatabase> {
    ///// A handle to the the coordinator for this node
    //coordinator_handle: ExecutorJoinHandle<Result<(), ServerError>>,
    /// A handle to the Shoal shard threads
    shard_handles: PoolThreadHandles<Result<(), ServerError>>,
    /// Whether this shoal pool should start shutting down or not
    should_shutdown: Arc<AtomicBool>,
    ///// A channel used to communicate with shards and coordinators
    //kanal_tx: AsyncSender<Msg<S>>,
    idk: PhantomData<S>,
}

impl<S: ShoalDatabase> ShoalPool<S>
where
    <<S::ClientType as QuerySupport>::QueryKinds as Archive>::Archived: rkyv::Deserialize<
        <S::ClientType as QuerySupport>::QueryKinds,
        Strategy<Pool, rkyv::rancor::Error>,
    >,
    for<'a> <Queries<S::ClientType> as Archive>::Archived: rkyv::bytecheck::CheckBytes<
        rkyv::rancor::Strategy<
            rkyv::validation::Validator<
                rkyv::validation::archive::ArchiveValidator<'a>,
                rkyv::validation::shared::SharedValidator,
            >,
            rkyv::rancor::Error,
        >,
    >,
{
    /// Start this shoal database
    #[instrument(name = "ShoalPool::start", skip_all, err(Debug))]
    pub fn start(conf: Conf) -> Result<Self, ServerError>
    where
        for<'a> <<<S as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as Archive>::Archived:
            CheckBytes<
                Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
            >,
    {
        // get the total number of cpus that we have
        let cpus = conf.resources.cpus()?;
        // we can only join the mesh once per executor so limit to the number of cores
        // we are planning to use
        let mesh_size = cpus.len();
        // build the mesh for this node to talk over
        let mesh: MeshBuilder<MeshMsg<S>, Full> = MeshBuilder::full(mesh_size, 8192);
        // spawn our coordinator
        //let (kanal_tx, coordinator_handle) = spawn_coordinator::<S>(conf.clone(), mesh.clone())?;
        // spawn our shards
        let (shard_handles, should_shutdown) = shard::start::<S>(conf, cpus, mesh)?;
        // build the shoal pool object
        let pool = ShoalPool {
            shard_handles,
            should_shutdown,
            idk: PhantomData,
        };
        Ok(pool)
    }

    /// Signal this pool to exit on all shards
    #[instrument(name = "ShoalPool::exit", skip_all, err(Debug))]
    pub fn exit(self) -> Result<(), ServerError> {
        // tell our shoal shards to shutdown
        self.should_shutdown
            .store(true, std::sync::atomic::Ordering::Relaxed);
        //// start an executor to tell shoal to exit
        //LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
        //});
        //// signal this pools shards and cooridinator to exit
        //    .spawn(|| async move { self.kanal_tx.send(Msg::Shutdown).await })?;
        //// wait for our coordinator to finish
        //self.coordinator_handle.join()??;
        // wait for all of our shards to finish
        self.shard_handles.join_all();
        Ok(())
    }
}
