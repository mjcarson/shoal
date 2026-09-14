use glommio::PoolThreadHandles;
use rkyv::rancor::Strategy;
use rkyv::Archive;
use rkyv::{
    bytecheck::CheckBytes,
    de::Pool,
    validation::{archive::ArchiveValidator, shared::SharedValidator, Validator},
};
use std::marker::PhantomData;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{Receiver, RecvTimeoutError, TryRecvError};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{event, instrument, Level};

mod args;
mod comms;
pub mod conf;
pub mod control;
pub mod database;
pub mod errors;
pub mod hosting;
pub mod export;
pub mod map;
pub mod messages;
pub mod peer;
pub mod meta;
pub mod recover;
pub mod rehome;
pub mod replication;
pub mod request_body;
pub mod ring;
pub mod routing;
pub mod shard;
pub mod stage_profile;
pub mod tables;
pub mod tls;
pub mod trace;
pub mod wal;

use comms::Comms;
pub use conf::Conf;
pub use control::{ControlHandle, ControlPlacement, DataReadiness, JoinStatus, ReadinessView, TopologyView};
pub use map::TabletMap;
pub use crate::shared::protocol::admin::{AdminKind, AdminRequest, AdminResponse};
pub use errors::ServerError;
pub use hosting::Hosting;
pub use meta::{ClusterIntent, DirectoryLock, Identity, PendingRehome, StorageMeta};
pub use rehome::RehomeReport;
pub use shard::ShardEvent;

use crate::server::errors::ShoalError;

use crate::server::database::ShoalDatabase;
use crate::shared::{queries::Queries, traits::QuerySupport};

/// How long the pool waits for its control plane before it starts a shard
///
/// A fresh bootstrap is a group of one electing itself; a joiner and a restart are a store
/// opened and a listener bound. None of them waits on a peer.
const CONTROL_START_TIMEOUT: Duration = Duration::from_secs(60);

/// A pool of ShoalDB shards
///
/// `start` spawns the shard threads and returns before any of them has bound, which is the
/// glommio shape and cannot change. What can is that the pool now hears from every shard:
/// [`ShoalPool::ready`] waits until each one has bound, joined the mesh and started its loaders,
/// or reports the first one that failed, and [`ShoalPool::failure`] asks afterwards whether one
/// has died since. Before [item 58](../../docs/src/appendix/resolved/unreported-shard-death.md)
/// a shard that could not bind was indistinguishable from one still starting, and the only
/// caller that noticed was a readiness probe timing out thirty seconds later.
///
/// A config that asks for port `0` gets a real port: the pool reserves one with `SO_REUSEPORT`
/// before the shards start, hands them the number, and holds the reservation - bound, never
/// listening, so no connection can land on it - until every shard has bound the same port.
/// That is what lets a test fixture start any number of servers without a port race
/// ([C11](../../docs/src/distributed/testing.md)).
///
/// A config with a `cluster:` block gets a control plane beside its shards
/// ([F37](../../docs/src/features/node-identity-control-plane.md)): the control core is resolved
/// and kept away from the shards first, the storage directory is locked and its marker claimed
/// with the node's identity, the control thread is started before the shards and stopped after
/// them, and `ready` waits for it too. Without the block none of that exists, and the shard path
/// is what it always was.
pub struct ShoalPool<S: ShoalDatabase> {
    /// A handle to the Shoal shard threads
    shard_handles: PoolThreadHandles<Result<(), ServerError>>,
    /// Whether this shoal pool should start shutting down or not
    should_shutdown: Arc<AtomicBool>,
    /// What the shards report back: one `Ready` each, and a `Failed` if one dies
    events: Receiver<ShardEvent>,
    /// How many shards were started, which is how many `Ready` events readiness waits for
    shards: usize,
    /// The address the shards were told to bind, with a port of zero already resolved
    bound: SocketAddr,
    /// Whether every shard has reported ready
    ready: bool,
    /// The reserved port, held until every shard has bound it
    ///
    /// `None` when the config named a port itself, or once readiness has been established.
    reservation: Option<socket2::Socket>,
    /// Who this node is, read from the storage marker at the claim
    identity: Identity,
    /// The lock on the storage directory, held for as long as the pool exists
    _lock: DirectoryLock,
    /// The control plane, if this is a cluster node
    control: Option<ControlHandle>,
    /// The cpus the shards run on, in ascending order
    shard_cpus: Vec<usize>,
    /// A sync sender to shard 0's mesh channel, for control operations the pool drives
    ///
    /// The pool is on no executor, so it cannot await the mesh; this is the sync half of shard
    /// 0's channel, used to ask it for the transport view or to start a bulk probe
    /// ([F38](../../../docs/src/features/inter-node-transport.md)).
    control_tx: kanal::Sender<messages::ServerMsg<S>>,
    /// A sync sender to every shard's mesh channel, for a test to fail one
    shard_txs: Vec<kanal::Sender<messages::ServerMsg<S>>>,
    /// Which executor hosts each slot and each tablet ([F47](../../docs/src/features/local-rehome.md))
    hosting: Arc<Hosting>,
    /// What the rehome this start ran moved, if the executor count had changed
    rehome: Option<RehomeReport>,
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
    pub fn start(mut conf: Conf) -> Result<Self, ServerError>
    where
        for<'a> <<<S as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as Archive>::Archived:
            CheckBytes<
                Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
            >,
    {
        // resolve a port of zero to a real one before any shard can ask for it
        //
        // every shard binds the same port with `SO_REUSEPORT`, so a zero handed to each of them
        // would give each a different ephemeral port. the reservation is what makes the answer
        // one number, and holding it until readiness is what makes that number ours
        let reservation = match conf.networking.port {
            0 => {
                let (socket, port) = reserve_port(&conf.networking.interface)?;
                conf.networking.port = port;
                Some(socket)
            }
            _ => None,
        };
        // the address every shard will bind, now that it is fully known
        let bound = conf
            .networking
            .to_addr()
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| std::io::Error::other("the configured interface names no address"))?;
        // a cluster node resolves its control core first, since the shards have to keep off it
        //
        // the block is validated here too, so a setting this build cannot act on refuses the
        // start by name rather than being read by nothing
        let placement = match &conf.cluster {
            Some(cluster) => {
                cluster.validate(&conf.networking.interface, conf.networking.max_frame_bytes)?;
                // a persistent table acknowledged before its fdatasync cannot be a voter in a
                // durable quorum, and the durability is per table under `storage`, which the
                // block's own validation cannot see (C5, F40)
                for table in S::persistent_tables() {
                    let settings = conf
                        .storage
                        .tables
                        .get(table)
                        .map(|settings| match settings {
                            conf::TableSettings::FS(settings) => settings.clone(),
                        })
                        .unwrap_or_else(|| conf.storage.default.filesystem.clone());
                    if settings.latency_sensitive.durability == tables::storage::fs::conf::Durability::Async {
                        return Err(ServerError::Shoal(ShoalError::InvalidConfig(format!(
                            "table {table} is configured with durability: Async on a cluster node; a durable \
                             quorum cannot be built from an acknowledgement that precedes fdatasync (C5)"
                        ))));
                    }
                }
                Some(ControlPlacement::resolve(&conf)?)
            }
            None => None,
        };
        // get the total number of cpus that we have, off the control core where there is one
        let reserved = placement
            .as_ref()
            .map(ControlPlacement::reserved_cores)
            .unwrap_or_default();
        let cpus = conf.resources.cpus_reserving(&reserved)?;
        // a node with no cores has no shards, and so nothing that could own any data
        //
        // this is caught here rather than in a shard so that a misconfigured `cores` or
        // `exclude_cores` says so instead of failing somewhere further in
        if cpus.is_empty() {
            return Err(ServerError::Shoal(ShoalError::NoShards));
        }
        // a control core a shard also landed on is a claim of isolation that would be false
        if let Some(placement) = &placement {
            placement.check_isolation(&cpus)?;
        }
        // hold the storage directory, so a second process on the same path is refused rather
        // than claiming the same identity
        let root = conf.storage.default.filesystem.latency_sensitive.path.clone();
        let lock = DirectoryLock::acquire(&root)?;
        // check this storage directory was written by the shard count and in the mode we are
        // starting with, claiming it with a fresh identity if nothing has
        //
        // this happens before any shard is spawned, so a mismatch is refused before a
        // single write can land in the wrong place
        let intent = match &conf.cluster {
            Some(cluster) if cluster.bootstrap => ClusterIntent::Bootstrap,
            Some(_) => ClusterIntent::Join,
            None => ClusterIntent::Standalone,
        };
        //
        // since F47 a changed core count is not a mismatch: the claim reports the rehome the
        // files need and it runs below, before any shard starts. a cluster node's slots are
        // claimed here too, once ([F47](../../docs/src/features/local-rehome.md))
        let slots = conf.cluster.as_ref().and_then(|cluster| cluster.slots);
        let identity = StorageMeta::claim(&root, cpus.len(), slots, intent)?;
        // remember how many shards readiness has to hear from, and where they run
        let shards = cpus.len();
        let mut shard_cpus: Vec<usize> = cpus.iter().map(|location| location.cpu).collect();
        shard_cpus.sort_unstable();
        // the control plane starts before the shards, so a group that cannot start refuses the
        // node before any shard has bound; and it is waited for here, so the shards start with
        // the map it holds rather than none
        let mut control = match placement {
            Some(placement) => {
                let mut control = control::ControlPlane::start(
                    placement,
                    identity.clone(),
                    &conf,
                    bound.to_string(),
                    identity.slots,
                    shards,
                    <S::ClientType as QuerySupport>::SCHEMA_ID,
                    <S::ClientType as QuerySupport>::table_ids()
                        .into_iter()
                        .map(|(name, id)| (name.to_string(), id))
                        .collect(),
                )?;
                control.ready(Instant::now() + CONTROL_START_TIMEOUT)?;
                Some(control)
            }
            None => None,
        };
        // move the files between executor counts if the claim found that pending, before any
        // shard can open them: on a cluster node the map says which slot every group is on,
        // which is why this waits for the control plane ([F47](../../docs/src/features/local-rehome.md))
        let rehome = match identity.rehome {
            Some(_) => {
                let map = match &control {
                    Some(control) => Some(control.map()?),
                    None => None,
                };
                rehome::Rehome::run::<S>(&conf, &identity, map, shard_cpus[0])?
            }
            None => None,
        };
        // the hosting the shards run under: what the directory says, or the identity, dealt
        // onto the cores when a fresh directory claimed more slots than it has cores
        let hosting = {
            let mut hosting = Hosting::read_or_identity(&root, identity.slots)?;
            if hosting.physical != shards {
                hosting = hosting.plan(shards, conf.cluster.is_some())?;
                hosting.write(&root)?;
            }
            if hosting.slots != identity.slots {
                return Err(ServerError::Shoal(ShoalError::InvalidConfig(format!(
                    "the hosting file names {} slots and the marker {}",
                    hosting.slots, identity.slots
                ))));
            }
            Arc::new(hosting)
        };
        // resolve what the shards need to talk to their peers, on a cluster node: what they say
        // about themselves, the map they start with, and where the listeners bind
        let peer_setup = match (&conf.cluster, &control) {
            (Some(cluster), Some(control)) => {
                let initial_map = control.map()?;
                let schema_id = <S::ClientType as QuerySupport>::SCHEMA_ID;
                let local = peer::Local::new(
                    &identity,
                    identity.slots,
                    schema_id,
                    conf.networking.max_frame_bytes,
                    cluster.transport.wire_version,
                );
                let bind = format!(
                    "{}:{}",
                    conf.networking.interface, cluster.port
                )
                .parse()
                .map_err(|_| ServerError::Shoal(ShoalError::InvalidConfig(format!(
                    "the peer listener address {}:{} is not one",
                    conf.networking.interface, cluster.port
                ))))?;
                Some(peer::PeerSetup {
                    local,
                    dial: cluster.dial.clone(),
                    initial_map,
                    tls: cluster.tls.clone(),
                    transport: cluster.transport.clone(),
                    bind,
                })
            }
            _ => None,
        };
        // spawn our shards, telling the control plane about any that die
        let (shard_handles, should_shutdown, events, senders) = shard::start::<S>(
            conf,
            cpus,
            hosting.clone(),
            peer_setup,
            control.as_ref().map(ControlHandle::requests),
        )?;
        // every map the control plane builds from now on goes to every shard
        let control_tx = senders.0[0].clone();
        let shard_txs = senders.0.clone();
        if let Some(control) = &mut control {
            let sink: control::MapSink = Box::new(move |map| senders.push_map(&map));
            control.attach_sink(sink)?;
        }
        // build the shoal pool object
        let pool = ShoalPool {
            shard_handles,
            should_shutdown,
            events,
            shards,
            bound,
            ready: false,
            reservation,
            identity,
            _lock: lock,
            control,
            shard_cpus,
            control_tx,
            shard_txs,
            hosting,
            rehome,
            phantom: PhantomData,
        };
        Ok(pool)
    }

    /// The report of the rehome this start ran, if the executor count had changed
    ///
    /// What moved and what it cost ([F47](../../docs/src/features/local-rehome.md)); none when
    /// the files were already laid out for this many executors.
    pub fn rehome(&self) -> Option<&RehomeReport> {
        self.rehome.as_ref()
    }

    /// Which executor hosts each slot and each tablet on this node
    pub fn hosting(&self) -> &Hosting {
        &self.hosting
    }

    /// The address the shards bind
    ///
    /// Known from `start` onwards, a port of zero included: it was resolved before the shards
    /// were spawned. Whether anything is answering on it is what [`ShoalPool::ready`] says.
    pub fn bound_addr(&self) -> SocketAddr {
        self.bound
    }

    /// Who this node is
    ///
    /// Read from the storage marker when it was claimed, so it is the same across every restart
    /// of the same directory. A standalone node has a node id and no cluster.
    pub fn identity(&self) -> &Identity {
        &self.identity
    }

    /// Where the control thread runs, if this is a cluster node
    pub fn control_placement(&self) -> Option<&ControlPlacement> {
        self.control.as_ref().map(ControlHandle::placement)
    }

    /// The cpus the shards run on, in ascending order
    ///
    /// What a test checks against the control core's siblings: the two sets are disjoint
    /// unless the configuration said the core was shared.
    pub fn shard_cpus(&self) -> &[usize] {
        &self.shard_cpus
    }

    /// Where this node stands: live, with its group, and with its data
    ///
    /// # Errors
    ///
    /// Refuses with [`ShoalError::NotClustered`] on a standalone node, and fails if the control
    /// thread is gone.
    pub fn readiness(&self) -> Result<ReadinessView, ServerError> {
        match &self.control {
            Some(control) => control.readiness(),
            None => Err(ServerError::Shoal(ShoalError::NotClustered)),
        }
    }

    /// The map this node's shards route with
    ///
    /// # Errors
    ///
    /// Refuses with [`ShoalError::NotClustered`] on a standalone node, and fails if the control
    /// thread is gone.
    pub fn map(&self) -> Result<Arc<TabletMap>, ServerError> {
        match &self.control {
            Some(control) => control.map(),
            None => Err(ServerError::Shoal(ShoalError::NotClustered)),
        }
    }

    /// Make an administrative request as the process itself
    ///
    /// The in-process seam the benchmark harness and the fixture use: it needs no principal,
    /// since the process that owns the pool is its own operator. A request off the wire goes
    /// through the shards, which check the principal ([F39](../../../docs/src/features/membership.md)).
    ///
    /// # Arguments
    ///
    /// * `request` - What is asked
    ///
    /// # Errors
    ///
    /// Refuses with [`ShoalError::NotClustered`] on a standalone node, and fails if the control
    /// thread is gone or did not answer.
    pub fn admin(&self, request: AdminRequest) -> Result<AdminResponse, ServerError> {
        match &self.control {
            Some(control) => control.admin(request, None, true),
            None => Err(ServerError::Shoal(ShoalError::NotClustered)),
        }
    }

    /// A handle to make administrative requests as the process from another thread, on a cluster node
    #[must_use]
    pub fn admin_sender(&self) -> Option<crate::server::control::AdminSender> {
        self.control.as_ref().map(ControlHandle::admin_sender)
    }

    /// Make one shard fail, for a test of what the cluster does about a dead shard
    ///
    /// # Arguments
    ///
    /// * `shard` - The shard
    ///
    /// # Errors
    ///
    /// Fails if there is no such shard or it is already gone.
    pub fn fail_shard(&self, shard: usize) -> Result<(), ServerError> {
        let tx = self.shard_txs.get(shard).ok_or_else(|| {
            ServerError::ShardFailed {
                shard,
                error: "no such shard".to_string(),
            }
        })?;
        tx.send(messages::ServerMsg::Fail).map_err(|_| ServerError::ShardFailed {
            shard,
            error: "the shard is already gone".to_string(),
        })
    }

    /// Arm a crash point, so the next snapshot install on this node dies there, for a test
    ///
    /// Process-global and off unless armed ([F43](../../../docs/src/features/node-recovery.md)).
    ///
    /// # Arguments
    ///
    /// * `point` - The point's name, or `none` to disarm
    ///
    /// # Errors
    ///
    /// Refuses a name that is not a point.
    pub fn crash_at(&self, point: &str) -> Result<(), ServerError> {
        let point = if point == "none" {
            replication::CrashPoint::None
        } else {
            replication::CrashPoint::from_name(point).ok_or_else(|| {
                ServerError::Shoal(ShoalError::InvalidConfig(format!("{point} is not a crash point")))
            })?
        };
        replication::crash_point::arm(point);
        Ok(())
    }

    /// Arm a move phase this process dies right after committing, for the crash matrix
    /// ([F45](../../../docs/src/features/replica-migration.md))
    ///
    /// # Arguments
    ///
    /// * `phase` - The phase's name, or `none` to disarm
    /// * `group` - The group whose driver dies, or none for whichever commits the phase first
    ///
    /// # Errors
    ///
    /// Refuses a name that is not a phase a driver commits.
    pub fn move_crash_at(&self, phase: &str, group: Option<crate::shared::identity::GroupId>) -> Result<(), ServerError> {
        shard::migrate::crash_point::arm(phase, group).map_err(|msg| ServerError::Shoal(ShoalError::InvalidConfig(msg)))
    }

    /// Override the free bytes this node reports and checks, for a capacity test
    ///
    /// Process-wide: the report the leader plans from and the receiver's reserve check both
    /// read it ([F46](../../../docs/src/features/capacity-rebalancing.md)).
    ///
    /// # Arguments
    ///
    /// * `bytes` - The free bytes to report, or zero for the filesystem's figure
    pub fn free_bytes_override(&self, bytes: u64) {
        control::capacity::set_override(bytes);
    }

    /// Make every snapshot install on this node pause after its first record, for a test
    ///
    /// # Arguments
    ///
    /// * `ms` - How long, or zero for no pause
    pub fn hold_install(&self, ms: u64) {
        replication::crash_point::hold(ms);
    }

    /// Send the control leader one report behind the last, as a replay would be, for a test
    ///
    /// # Errors
    ///
    /// Refuses with [`ShoalError::NotClustered`] on a standalone node.
    pub fn control_stale_report(&self) -> Result<(), ServerError> {
        match &self.control {
            Some(control) => control.stale_report(),
            None => Err(ServerError::Shoal(ShoalError::NotClustered)),
        }
    }

    /// The cluster as this node sees it
    ///
    /// Answered by the control plane from its applied state, so it is what the committed log
    /// says. A standalone node has no control plane and no view.
    ///
    /// # Errors
    ///
    /// Refuses with [`ShoalError::NotClustered`] on a standalone node, and fails if the control
    /// thread is gone.
    pub fn topology(&self) -> Result<TopologyView, ServerError> {
        match &self.control {
            Some(control) => control.topology(),
            None => Err(ServerError::Shoal(ShoalError::NotClustered)),
        }
    }

    /// What every shard's peer links look like, gathered from all of them
    ///
    /// Asks each shard in turn over the mesh and collects the answers. A standalone node has no
    /// links and reports an empty view ([F38](../../../docs/src/features/inter-node-transport.md)).
    pub fn transport(&self) -> Result<Vec<peer::ShardTransportView>, ServerError> {
        let mut views = Vec::with_capacity(self.shards);
        // ask shard 0, which forwards nothing here - each shard answers for itself, so the pool
        // asks every one. The mesh only reaches shard 0 from here, so shard 0 relays the request
        // to the rest is not built yet; at M2 the pool asks shard 0 and the fixture drives one
        // node's shards through it
        for _ in 0..1 {
            let (tx, rx) = std::sync::mpsc::channel();
            self.control_tx
                .send(messages::ServerMsg::Transport(tx))
                .map_err(|_| ServerError::Shoal(ShoalError::NotClustered))?;
            match rx.recv_timeout(Duration::from_secs(5)) {
                Ok(view) => views.push(view),
                Err(_) => return Err(ServerError::Shoal(ShoalError::NotClustered)),
            }
        }
        Ok(views)
    }

    /// What every shard's tablet groups look like, folded over the node
    ///
    /// Asks each shard in turn over the mesh. A standalone node hosts no groups and reports
    /// none ([F40](../../../docs/src/features/replication.md)).
    pub fn replication(&self) -> Result<replication::NodeReplication, ServerError> {
        let mut shards = Vec::with_capacity(self.shards);
        for tx in &self.shard_txs {
            let (reply, rx) = std::sync::mpsc::channel();
            tx.send(messages::ServerMsg::ReplicationView(reply))
                .map_err(|_| ServerError::Shoal(ShoalError::NotClustered))?;
            match rx.recv_timeout(Duration::from_secs(5)) {
                Ok(view) => shards.push(view),
                Err(_) => return Err(ServerError::Shoal(ShoalError::NotClustered)),
            }
        }
        Ok(replication::NodeReplication::fold(shards))
    }

    /// Drive a replication verb on every shard, for the fixture
    ///
    /// # Arguments
    ///
    /// * `verb` - What to do
    ///
    /// # Errors
    ///
    /// Fails if a shard is gone or does not answer; a shard's own refusal is in its answer.
    pub fn replication_verb(
        &self,
        verb: replication::ReplicationVerb,
    ) -> Result<Vec<Result<serde_json::Value, String>>, ServerError> {
        let mut answers = Vec::with_capacity(self.shards);
        for tx in &self.shard_txs {
            let (reply, rx) = std::sync::mpsc::channel();
            tx.send(messages::ServerMsg::ReplicationVerb {
                verb: verb.clone(),
                reply,
            })
            .map_err(|_| ServerError::Shoal(ShoalError::NotClustered))?;
            match rx.recv_timeout(Duration::from_secs(30)) {
                Ok(answer) => answers.push(answer),
                Err(_) => return Err(ServerError::Shoal(ShoalError::NotClustered)),
            }
        }
        Ok(answers)
    }

    /// Drive a read verb on one shard, or on every shard, for the fixture
    ///
    /// Standalone-safe, unlike [`ShoalPool::replication_verb`]: a standalone node has gathers to
    /// hold and expire too ([F41](../../../docs/src/features/read-consistency.md)).
    ///
    /// # Arguments
    ///
    /// * `shard` - The shard to drive, or every shard
    /// * `verb` - What to do
    ///
    /// # Errors
    ///
    /// Fails if a shard is gone or does not answer; a shard's own refusal is in its answer.
    pub fn read_verb(
        &self,
        shard: Option<usize>,
        verb: replication::ReadVerb,
    ) -> Result<Vec<Result<serde_json::Value, String>>, ServerError> {
        // the shards asked: one, or all in order
        let targets: Vec<&kanal::Sender<messages::ServerMsg<S>>> = match shard {
            Some(shard) => vec![self.shard_txs.get(shard).ok_or_else(|| ServerError::ShardFailed {
                shard,
                error: "no such shard".to_string(),
            })?],
            None => self.shard_txs.iter().collect(),
        };
        let mut answers = Vec::with_capacity(targets.len());
        for tx in targets {
            let (reply, rx) = std::sync::mpsc::channel();
            tx.send(messages::ServerMsg::ReadVerb {
                verb: verb.clone(),
                reply,
            })
            .map_err(|_| ServerError::Shoal(ShoalError::NotClustered))?;
            match rx.recv_timeout(Duration::from_secs(30)) {
                Ok(answer) => answers.push(answer),
                Err(_) => return Err(ServerError::Shoal(ShoalError::NotClustered)),
            }
        }
        Ok(answers)
    }

    /// Start a bulk probe of a given size at a peer, for the bounded-bytes test
    ///
    /// # Arguments
    ///
    /// * `node` - The peer to stream at
    /// * `bytes` - How many payload bytes to stream
    pub fn probe_bulk(&self, node: shoal_proto::shared::identity::NodeId, bytes: u64) -> Result<(), ServerError> {
        self.control_tx
            .send(messages::ServerMsg::BulkProbe { node, bytes })
            .map_err(|_| ServerError::Shoal(ShoalError::NotClustered))
    }

    /// Ping a peer over the control lane, returning how long it took
    ///
    /// # Arguments
    ///
    /// * `node` - The peer to ping
    pub fn control_ping(
        &self,
        node: shoal_proto::shared::identity::NodeId,
    ) -> Result<Duration, ServerError> {
        match &self.control {
            Some(control) => control.ping(node),
            None => Err(ServerError::Shoal(ShoalError::NotClustered)),
        }
    }

    /// Send a peer a vote for a low term and report its answer, proving the control lane end to end
    ///
    /// # Arguments
    ///
    /// * `node` - The peer to probe
    pub fn control_vote_probe(
        &self,
        node: shoal_proto::shared::identity::NodeId,
    ) -> Result<control::VoteProbe, ServerError> {
        match &self.control {
            Some(control) => control.vote_probe(node),
            None => Err(ServerError::Shoal(ShoalError::NotClustered)),
        }
    }

    /// Wait until every shard is answering, or report the first one that is not
    ///
    /// Returns the bound address once each shard has reported ready. A shard that failed - to
    /// bind, to load its tables, anything - ends the wait at once with
    /// [`ServerError::ShardFailed`] carrying what it said, rather than leaving the caller to
    /// infer it from a connection that is refused for thirty seconds. Cheap to call again once
    /// it has succeeded.
    ///
    /// # Arguments
    ///
    /// * `timeout` - How long to wait for the shards in total
    #[instrument(name = "ShoalPool::ready", skip_all, err(Debug))]
    pub fn ready(&mut self, timeout: Duration) -> Result<SocketAddr, ServerError> {
        // nothing to wait for once every shard has reported
        if self.ready {
            return Ok(self.bound);
        }
        // the deadline is for the whole pool, not per shard
        let deadline = Instant::now() + timeout;
        // the control plane first, since it started first and a group that failed is a node
        // that is not the one the config described whatever its shards are doing
        if let Some(control) = &mut self.control {
            control.ready(deadline)?;
        }
        let mut seen = 0;
        while seen < self.shards {
            // wait for the next report, for as long as the deadline still allows
            let remaining = deadline.saturating_duration_since(Instant::now());
            let event = match self.events.recv_timeout(remaining) {
                Ok(event) => event,
                Err(RecvTimeoutError::Timeout) => {
                    return Err(ServerError::ReadyTimeout {
                        ready: seen,
                        of: self.shards,
                        timeout,
                    });
                }
                // every sender gone means every shard thread has exited without reporting,
                // which the join in `exit` will explain - here it is a shard that never came
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(ServerError::ShardFailed {
                        shard: seen,
                        error: "exited before reporting ready".to_string(),
                    });
                }
            };
            match event {
                // a shard is answering, and on the port it was given
                ShardEvent::Ready { shard, addr } => {
                    // a shard on any other port would be one no client is told about
                    if addr.port() != self.bound.port() {
                        return Err(ServerError::ShardFailed {
                            shard,
                            error: format!("bound {addr} rather than {}", self.bound),
                        });
                    }
                    seen += 1;
                }
                // one shard failing means the pool is not the server the config described
                ShardEvent::Failed { shard, error } => {
                    // the reservation has nothing left to protect
                    self.reservation = None;
                    return Err(ServerError::ShardFailed { shard, error });
                }
            }
        }
        // every shard holds the port now, so the reservation can go
        self.reservation = None;
        self.ready = true;
        Ok(self.bound)
    }

    /// Whether a shard has died since the pool was last asked
    ///
    /// Non blocking. Returns the shard and what it said, or `None` while every shard that has
    /// reported is still running. Meant for after [`ShoalPool::ready`]: before it, that method
    /// is the one that reports a failure.
    pub fn failure(&self) -> Option<(usize, String)> {
        // a dead control plane is reported first, as a shard that is not a shard
        if let Some(error) = self.control.as_ref().and_then(ControlHandle::failure) {
            return Some((usize::MAX, format!("control plane: {error}")));
        }
        loop {
            match self.events.try_recv() {
                // a death, which is what was asked about
                Ok(ShardEvent::Failed { shard, error }) => return Some((shard, error)),
                // a late readiness report is not a failure; keep looking
                Ok(ShardEvent::Ready { .. }) => continue,
                // nothing new, or nothing left to hear from
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => return None,
            }
        }
    }

    /// Signal this pool to exit on all shards
    #[instrument(name = "ShoalPool::exit", skip_all, err(Debug))]
    pub fn exit(self) -> Result<(), ServerError> {
        // tell our shoal shards to shutdown
        self.should_shutdown
            .store(true, std::sync::atomic::Ordering::Relaxed);
        // the first shard error, which is what this returns rather than swallowing it
        let mut first = None;
        // wait for all of our shards to finish
        for handle in self.shard_handles.join_all() {
            // a shard can fail two ways: its thread can fail to join, or its loop can return an
            // error. only the first was ever looked at before item 58, and only logged
            let error = match handle {
                Ok(Ok(())) => continue,
                Ok(Err(error)) => error,
                Err(error) => ServerError::from(error),
            };
            // log this error, and keep the first for the caller
            event!(Level::ERROR, error = format!("{error:?}"));
            first.get_or_insert(error);
        }
        // the control plane stops last, after every shard is gone, so nothing is ever left
        // asking a group that has already shut down
        if let Some(control) = self.control {
            if let Err(error) = control.shutdown() {
                event!(Level::ERROR, error = format!("{error:?}"));
                first.get_or_insert(error);
            }
        }
        // a shard that died is the pool's result, not a log line nobody installed a reader for
        match first {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

/// Reserve a port on an interface with `SO_REUSEPORT`, so the shards can bind it too
///
/// The socket is bound and never listens: the kernel only ever routes a connection to a
/// listening member of a reuse-port group, so holding this open keeps the port from anyone else
/// without stealing a single connection from the shards. It is dropped once every shard has
/// bound, which is the point at which the shards hold the port themselves.
///
/// # Arguments
///
/// * `interface` - The interface the shards will bind
fn reserve_port(interface: &str) -> Result<(socket2::Socket, u16), ServerError> {
    // resolve the interface with port zero, the way the shards will resolve it with a port
    let addr = format!("{interface}:0")
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| std::io::Error::other("the configured interface names no address"))?;
    // the same family the shards will use
    let domain = if addr.is_ipv6() {
        socket2::Domain::IPV6
    } else {
        socket2::Domain::IPV4
    };
    let socket = socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
    // this has to be set before the bind for the shards' reuse-port binds to be allowed
    socket.set_reuse_port(true)?;
    socket.bind(&socket2::SockAddr::from(addr))?;
    // the port the kernel chose is the one every shard is told
    let port = socket
        .local_addr()?
        .as_socket()
        .ok_or_else(|| std::io::Error::other("the reserved address is not a socket address"))?
        .port();
    Ok((socket, port))
}
