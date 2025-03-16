use tracing::instrument;

mod args;
mod conf;
mod coordinator;
pub mod errors;
pub mod messages;
pub mod ring;
pub mod shard;
pub mod tables;
pub mod trace;

use clap::Parser;
use glommio::{
    channels::channel_mesh::{Full, MeshBuilder},
    ExecutorJoinHandle, LocalExecutorBuilder, Placement, PoolThreadHandles,
};
use kanal::AsyncSender;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::Archive;

use args::Args;
pub use conf::Conf;
use coordinator::Coordinator;
pub use errors::ServerError;
use messages::{MeshMsg, Msg};

use crate::shared::traits::{QuerySupport, ShoalDatabase};

/// Spawns our shard coordinator
///
/// # Arguments
///
/// * `conf` - The shoal config
/// * `mesh` - The mesh to send  messages over
fn spawn_coordinator<S: ShoalDatabase>(
    conf: &Conf,
    mesh: &MeshBuilder<MeshMsg<S>, Full>,
) -> Result<
    (
        AsyncSender<Msg<S>>,
        ExecutorJoinHandle<Result<(), ServerError>>,
    ),
    ServerError,
>
where
    <<S::ClientType as QuerySupport>::QueryKinds as Archive>::Archived: rkyv::Deserialize<
        <S::ClientType as QuerySupport>::QueryKinds,
        Strategy<Pool, rkyv::rancor::Error>,
    >,
{
    // clone our mesh to pass to our coordinator
    let mesh = mesh.clone();
    // clone our conf for our coordinator
    let conf = conf.clone();
    // create our kanal chaannels
    let (kanal_tx, kanal_rx) = kanal::bounded_async(8192);
    // get a copy of our kanal transmission channel for the coordinator
    let kanal_tx_coord = kanal_tx.clone();
    // start our coordinator node
    let coord_handle =
        LocalExecutorBuilder::new(glommio::Placement::Fixed(0)).spawn(|| async move {
            Coordinator::<S>::start(conf, mesh, kanal_tx_coord, kanal_rx).await
        })?;
    Ok((kanal_tx, coord_handle))
}

/// A pool of ShoalDB shards
pub struct ShoalPool<S: ShoalDatabase> {
    /// A handle to the the coordinator for this node
    coordinator_handle: ExecutorJoinHandle<Result<(), ServerError>>,
    /// A handle to the Shoal shard threads
    shard_handles: PoolThreadHandles<Result<(), ServerError>>,
    /// A channel used to communicate with shards and coordinators
    kanal_tx: AsyncSender<Msg<S>>,
}

impl<S: ShoalDatabase> ShoalPool<S>
where
    <<S::ClientType as QuerySupport>::QueryKinds as Archive>::Archived: rkyv::Deserialize<
        <S::ClientType as QuerySupport>::QueryKinds,
        Strategy<Pool, rkyv::rancor::Error>,
    >,
{
    /// Start this shoal database
    #[instrument(name = "ShoalPool::start", skip_all, err(Debug))]
    pub fn start() -> Result<Self, ServerError> {
        // get our command line args
        let args = Args::parse();
        // load our config
        let conf = Conf::new(&args.conf)?;
        // setup tracing/telemetry
        trace::setup(&conf);
        // get the total number of cpus that we have
        let cpus = conf.compute.cpus()?;
        // build the mesh for this node to talk over
        let mesh: MeshBuilder<MeshMsg<S>, Full> = MeshBuilder::full(cpus.len(), 8192);
        // spawn our coordinator
        let (kanal_tx, coordinator_handle) = spawn_coordinator::<S>(&conf, &mesh)?;
        // remove one core from our cpuset for the coordinator
        let cpus = cpus.filter(|core| core.cpu != 0);
        // spawn our shards
        let shard_handles = shard::start::<S>(conf, cpus, mesh)?;
        // build the shoal pool object
        let pool = ShoalPool {
            coordinator_handle,
            shard_handles,
            kanal_tx,
        };
        Ok(pool)
    }

    /// Signal this pool to exit on all shards
    #[instrument(name = "ShoalPool::exit", skip_all, err(Debug))]
    pub fn exit(self) -> Result<(), ServerError> {
        // signal this pools shards and cooridinator to exit
        LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move { self.kanal_tx.send(Msg::Shutdown).await })?;
        // wait for our coordinator to finish
        self.coordinator_handle.join()??;
        // wait for all of our shards to finish
        self.shard_handles.join_all();
        Ok(())
    }
}
