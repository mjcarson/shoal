mod args;
mod conf;
mod coordinator;
pub mod errors;
pub mod messages;
pub mod ring;
pub mod shard;
pub mod tables;
pub mod tracing;

use clap::Parser;
use glommio::{
    channels::channel_mesh::{Full, MeshBuilder},
    ExecutorJoinHandle, LocalExecutorBuilder,
};
use rkyv::{
    ser::serializers::{
        AlignedSerializer, AllocScratch, CompositeSerializer, FallbackScratch, HeapScratch,
        SharedSerializeMap,
    },
    AlignedVec,
};

use args::Args;
pub use conf::Conf;
use coordinator::Coordinator;
pub use errors::ServerError;
use messages::MeshMsg;

use crate::shared::traits::ShoalDatabase;

/// Spawns our shard coordinator
///
/// # Arguments
///
/// * `conf` - The shoal config
/// * `mesh` - The mesh to send  messages over
fn spawn_coordinator<S: ShoalDatabase>(
    conf: &Conf,
    mesh: &MeshBuilder<MeshMsg<S>, Full>,
) -> Result<ExecutorJoinHandle<Result<(), ServerError>>, ServerError> {
    // clone our mesh to pass to our coordinator
    let mesh = mesh.clone();
    // clone our conf for our coordinator
    let conf = conf.clone();
    // start our coordinator node
    let coord_handle = LocalExecutorBuilder::new(glommio::Placement::Fixed(0))
        .spawn(|| async move { Coordinator::<S>::start(conf, mesh).await })?;
    Ok(coord_handle)
}

/// Start the shoal database
pub fn start<S: ShoalDatabase>() -> Result<(), ServerError>
where
    <S as ShoalDatabase>::ResponseKinds: rkyv::Serialize<
        CompositeSerializer<
            AlignedSerializer<AlignedVec>,
            FallbackScratch<HeapScratch<256>, AllocScratch>,
            SharedSerializeMap,
        >,
    >,
{
    // get our command line args
    let args = Args::parse();
    // load our config
    let conf = Conf::new(&args.conf)?;
    // setup tracing/telemetry
    tracing::setup(&conf);
    // get the total number of cpus that we have
    let cpus = conf.compute.cpus()?;
    // build this mesh for this node to talk over
    let mesh: MeshBuilder<MeshMsg<S>, Full> = MeshBuilder::full(cpus.len(), 8192);
    // spawn our coordinator
    let coord_handle = spawn_coordinator::<S>(&conf, &mesh)?;
    // remove one core from our cpuset for the coordinator
    let cpus = cpus.filter(|core| core.cpu != 0);
    // spawn our shards
    let pool = shard::start::<S>(conf, cpus, mesh)?;
    // wait for our coordinator to finish
    coord_handle.join()??;
    // wait for all of our shards to finish
    pool.join_all();
    Ok(())
}
