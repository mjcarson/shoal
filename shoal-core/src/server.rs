use glommio::PoolThreadHandles;
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
use tracing::{event, instrument, Level};

mod args;
mod comms;
mod conf;
pub mod errors;
pub mod messages;
pub mod ring;
pub mod shard;
pub mod tables;
pub mod trace;

use comms::Comms;
pub use conf::Conf;
pub use errors::ServerError;

use crate::shared::{
    queries::Queries,
    traits::{QuerySupport, ShoalDatabase},
};

/// A pool of ShoalDB shards
pub struct ShoalPool<S: ShoalDatabase> {
    /// A handle to the Shoal shard threads
    shard_handles: PoolThreadHandles<Result<(), ServerError>>,
    /// Whether this shoal pool should start shutting down or not
    should_shutdown: Arc<AtomicBool>,
    /// The database this shoal pool is handling
    phantom: PhantomData<S>,
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
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn start(conf: Conf) -> Result<Self, ServerError>
    where
        for<'a> <<<S as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as Archive>::Archived:
            CheckBytes<
                Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
            >,
    {
        // get the total number of cpus that we have
        let cpus = conf.resources.cpus()?;
        // spawn our shards
        let (shard_handles, should_shutdown) = shard::start::<S>(conf, cpus)?;
        // build the shoal pool object
        let pool = ShoalPool {
            shard_handles,
            should_shutdown,
            phantom: PhantomData,
        };
        Ok(pool)
    }

    /// Signal this pool to exit on all shards
    #[instrument(name = "ShoalPool::exit", skip_all, err(Debug))]
    pub fn exit(self) -> Result<(), ServerError> {
        // tell our shoal shards to shutdown
        self.should_shutdown
            .store(true, std::sync::atomic::Ordering::Relaxed);
        // wait for all of our shards to finish
        for handle in self.shard_handles.join_all() {
            // log any errors
            if let Err(error) = handle {
                // log this error
                event!(Level::ERROR, error = error.to_string());
            }
        }
        Ok(())
    }
}
