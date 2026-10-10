//! Sending each query to the node that serves it, by the topology the cluster pushes
//!
//! A client is pushed every topology version on every connection it subscribes, and until
//! [F74](../../../../docs/src/features/client-routing.md) it routed by none of them: a bundle
//! went to whichever node the pool handed a connection to, and that node forwarded it, proposed
//! it through a leader elsewhere, or asked another node for a read barrier. This module is what
//! picks the node instead:
//!
//! - [`plan_runs`] gives each query of a bundle a target - its group's preferred leader for a
//!   write or a strong read, any holder for a read at `One` - and cuts the bundle into runs of
//!   adjacent queries bound for one target. It is pure and reads only the route table.
//! - [`Router`] holds a pool for each node, opened the first time a run is sent there, and the
//!   nodes a connection recently died on, which nothing is routed to for a moment.
//!
//! **Routing is advice and never authority.** Every target is a guess from a map that can be
//! stale, and a node that is sent what it does not serve forwards it exactly as it did before
//! this module existed; a node that cannot be reached at all has its runs sent through the
//! client's endpoints instead. So a wrong guess costs a hop and never an answer.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use shoal_proto::shared::identity::{GroupId, NodeId, TableId};
use shoal_proto::shared::protocol::read::ReadLevel;
use shoal_proto::shared::routes::RouteTable;
use shoal_proto::shared::traits::{QuerySupport, ShoalQuerySupport, TableNameSupport};
use tracing::{event, Level};

use super::builder::PoolConfig;
use super::{Errors, ShoalConnectionManager, TopologyState};

/// How long nothing is routed to a node after a connection to it died owing answers, or no
/// connection to it could be had
///
/// Long enough to cover the queries in flight when it went, short enough that a node that was
/// only slow is routed to again before anyone would notice. The control group calls a node that
/// stays gone down, and the next pushed frame takes it out of every route for good.
pub(crate) const SUSPECT_FOR: Duration = Duration::from_secs(2);

/// How long a leader a write was told of is followed before its group is routed by its weights
/// again
///
/// Only a write that hops is told where its group's lead is, so nothing corrects a hint for a
/// client that has gone on to read, and the balancer hands a lead back to its preferred leader
/// as soon as it can ([item 223](../../../../docs/src/appendix/resolved/leader-hints-lapse.md)).
/// Five seconds is the balancer's own pace, one group a shard at a time; a lead still away
/// from its preferred leader when its hint lapses costs a hop to each write to the group planned
/// before the first such hop's answer teaches the client again
/// ([O99](../../../../docs/src/appendix/optimizations.md)).
pub const LEADER_HINT_FOR: Duration = Duration::from_secs(5);

/// Where a client sends its queries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Routing {
    /// To the node that serves each query, by the topology the cluster pushes
    ///
    /// A write and a strong read go to their group's preferred leader, a read at `One` to any
    /// copy, and a bundle whose queries belong on different nodes is sent as a run to each.
    /// Until a topology arrives, and for anything it cannot place, a query goes through the
    /// endpoints as it does under [`Routing::Endpoints`]
    /// ([F74](../../../../docs/src/features/client-routing.md)).
    #[default]
    Topology,
    /// Through the endpoints the client was given, whatever the topology says
    ///
    /// Every bundle goes whole to whichever endpoint the pool hands a connection to, and that
    /// node routes it - what every client did before F74, and what a caller that means to reach
    /// one node, such as a test of the forward path, asks for.
    Endpoints,
}

/// What routing one query needs to know about it
#[derive(Debug, Clone, Copy)]
pub(crate) struct QueryRoute<'a> {
    /// The table it names
    pub table: TableId,
    /// Every partition it touches, as their hashes
    pub keys: &'a [u64],
    /// Whether it changes the table
    pub write: bool,
    /// Whether it goes to its groups' leaders: a write, or a read at a level that waits on a
    /// barrier the leader grants
    pub leader_bound: bool,
}

/// A run of adjacent queries of one bundle bound for one target
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Run {
    /// The member it goes to, as an index into the route table's members; none for the endpoints
    pub target: Option<u16>,
    /// The offset of its first query in the bundle
    pub start: usize,
    /// How many queries it holds
    pub len: usize,
}

/// What a plan asks of the client's state about the members
pub(crate) struct Judge<'a> {
    /// Whether a member can be routed to now, by index
    pub usable: &'a dyn Fn(u16) -> bool,
    /// The member a table's tablet's leader is on, as far as the client knows: the leader a
    /// write through its group was last answered with, else its preferred leader
    pub leader: &'a dyn Fn(TableId, usize) -> Option<u16>,
}

/// The member a query on one tablet goes to first: the leader it is bound for, or the first holder
///
/// A write goes to its group's leader - the one a write through the group was last answered
/// with, else the preferred one - when that member is up and can be routed to, whatever its
/// copy's quarantine, since a write is proposed through the group and not read from a copy. A strong read goes there too unless that copy is quarantined, since the read is
/// served from it. A read at `One`, and anything whose leader cannot be reached, goes to the
/// first holder that can serve it. None means no member can, and the query goes through the
/// endpoints.
///
/// # Arguments
///
/// * `routes` - The route table
/// * `query` - The query
/// * `tablet` - The tablet of one of its keys
/// * `judge` - Which members can be routed to, and where each group's leader is
fn first_choice(
    routes: &RouteTable,
    query: &QueryRoute<'_>,
    tablet: usize,
    judge: &Judge<'_>,
) -> Option<u16> {
    // the leader, when it is bound for one and can take it
    if query.leader_bound {
        let leader = (judge.leader)(query.table, tablet).filter(|member| {
            routes
                .members
                .get(usize::from(*member))
                .is_some_and(|known| known.up)
                && (judge.usable)(*member)
                && (query.write || !routes.is_quarantined(*member, tablet))
        });
        if leader.is_some() {
            return leader;
        }
    }
    // else the first holder that can serve it
    routes
        .holders(tablet)
        .find(|member| (judge.usable)(*member))
}

/// Whether a member is somewhere a query on one tablet may go
///
/// A query bound for a leader may go only where [`first_choice`] sends it; a read at `One` may
/// go to any holder that can serve it.
///
/// # Arguments
///
/// * `routes` - The route table
/// * `query` - The query
/// * `tablet` - The tablet of one of its keys
/// * `member` - The member
/// * `judge` - Which members can be routed to, and where each group's leader is
fn accepts(
    routes: &RouteTable,
    query: &QueryRoute<'_>,
    tablet: usize,
    member: u16,
    judge: &Judge<'_>,
) -> bool {
    if query.leader_bound {
        first_choice(routes, query, tablet, judge) == Some(member)
    } else {
        (judge.usable)(member) && routes.holders(tablet).any(|holder| holder == member)
    }
}

/// The target of one query, given the run it would join and the bundle's home
///
/// The first of these that every key of the query accepts: the run it would join, so a bundle
/// is cut as rarely as it can be; the bundle's home - the client's own endpoint when it is a
/// member, else a member in turn - so reads at `One` go where the client was pointed, as they did
/// before it routed; and otherwise the member the most keys accept, ties to the first key's own
/// choice. A query is never split: a get whose
/// keys live on several members goes whole to one, and that member gathers the rest.
///
/// # Arguments
///
/// * `routes` - The route table
/// * `query` - The query
/// * `current` - The target of the run it would join, if that run goes to a member
/// * `home` - The bundle's home member, if it has one
/// * `judge` - Which members can be routed to, and where each group's leader is
fn choose(
    routes: &RouteTable,
    query: &QueryRoute<'_>,
    current: Option<u16>,
    home: Option<u16>,
    judge: &Judge<'_>,
) -> Option<u16> {
    // a query that names no partition goes wherever its neighbours go
    let Some(first) = query.keys.first() else {
        return current.or(home);
    };
    // whether a member is somewhere every key of the query may go
    let every = |member: u16| {
        query
            .keys
            .iter()
            .all(|key| accepts(routes, query, RouteTable::tablet_of(*key), member, judge))
    };
    // the run it would join, then the bundle's home
    for candidate in [current, home].into_iter().flatten() {
        if every(candidate) {
            return Some(candidate);
        }
    }
    // a query of one key goes where that key's tablet sends it
    let first_tablet = RouteTable::tablet_of(*first);
    let preferred = first_choice(routes, query, first_tablet, judge);
    if query.keys.len() == 1 {
        return preferred;
    }
    // a query of several goes to the member the most of them accept, ties to the first key's
    let votes = |member: u16| {
        query
            .keys
            .iter()
            .filter(|key| accepts(routes, query, RouteTable::tablet_of(**key), member, judge))
            .count()
    };
    let mut best = preferred;
    let mut best_votes = preferred.map_or(0, votes);
    for member in 0..routes.members.len() {
        // truncation cannot happen: the table indexes its members by u16
        #[allow(clippy::cast_possible_truncation)]
        let member = member as u16;
        let count = votes(member);
        if count > best_votes {
            best = Some(member);
            best_votes = count;
        }
    }
    best.filter(|_| best_votes > 0)
}

/// Give every query of a bundle a target and cut the bundle into runs of adjacent ones
///
/// A run's queries keep their order and their offsets, so each run is sent as a frame of its
/// own under the bundle's id and answers under the bundle's own indexes. Adjacent queries with
/// one target share a run; a target of none is the endpoints.
///
/// # Arguments
///
/// * `routes` - The route table
/// * `queries` - What routing each query needs to know, in the bundle's order
/// * `home` - The bundle's home member, if it has one
/// * `judge` - Which members can be routed to, and where each group's leader is
pub(crate) fn plan_runs(
    routes: &RouteTable,
    queries: &[QueryRoute<'_>],
    home: Option<u16>,
    judge: &Judge<'_>,
) -> Vec<Run> {
    let mut runs: Vec<Run> = Vec::with_capacity(1);
    for (offset, query) in queries.iter().enumerate() {
        // the run this query would join, if it goes to a member
        let current = runs.last().and_then(|run| run.target);
        let target = choose(routes, query, current, home, judge);
        // join the run before it, or start one
        match runs.last_mut() {
            Some(run) if run.target == target => run.len += 1,
            _ => runs.push(Run {
                target,
                start: offset,
                len: 1,
            }),
        }
    }
    runs
}

/// What routing each query of a bundle needs to know about it
///
/// A write goes to its leader; a read goes to its leader when the bundle asks for `Quorum`, or
/// names no level and its table's policy is stronger than `One`.
///
/// # Arguments
///
/// * `routes` - The route table, which holds each table's read policy
/// * `queries` - The bundle's queries
/// * `read` - The level the bundle asked its reads to be served at, if it named one
pub(crate) fn query_routes<'a, S: QuerySupport>(
    routes: &RouteTable,
    queries: &'a [S::QueryKinds],
    read: Option<ReadLevel>,
) -> Vec<QueryRoute<'a>> {
    queries
        .iter()
        .map(|query| {
            let table = S::query_table_name(query).table_id();
            let write = query.is_write();
            // a read is bound for its leader when it waits on the leader's barrier
            let strong = match read {
                Some(ReadLevel::Quorum) => true,
                Some(ReadLevel::One) => false,
                None => routes.strong_reads(table),
            };
            QueryRoute {
                table,
                keys: query.route_keys(),
                write,
                leader_bound: write || strong,
            }
        })
        .collect()
}

/// The nodes a connection recently died on, which nothing is routed to until a moment passes
#[derive(Debug, Default)]
pub(crate) struct Suspects {
    /// When each suspect node may be routed to again
    until: Mutex<HashMap<NodeId, Instant>>,
}

impl Suspects {
    /// Route nothing to a node for a while
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    /// * `why` - What made it suspect, for the log
    pub(crate) fn mark(&self, node: NodeId, why: &str) {
        let mut until = self.until.lock().unwrap_or_else(PoisonError::into_inner);
        until.insert(node, Instant::now() + SUSPECT_FOR);
        event!(Level::INFO, msg = "routing around a node for a moment", %node, reason = why);
    }

    /// Whether a node is suspect now
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    pub(crate) fn is_suspect(&self, node: NodeId) -> bool {
        let mut until = self.until.lock().unwrap_or_else(PoisonError::into_inner);
        match until.get(&node) {
            Some(when) if *when > Instant::now() => true,
            // a mark that ran out is forgotten
            Some(_) => {
                until.remove(&node);
                false
            }
            None => false,
        }
    }
}

/// The leaders a client's writes were told of, by group, each for [`LEADER_HINT_FOR`]
///
/// A write proposed through its group's leader from the node it reached is answered with that
/// leader beside its session token, and the group's writes and strong reads go there rather than
/// to its preferred leader. A lead that has not settled where the weights put it - after a
/// restart, or while a busy group's followers are too far behind for the balancer to hand it
/// back - is then followed after one hop rather than paid on every write
/// ([F74](../../../../docs/src/features/client-routing.md)). Only a write that hops is told, so
/// a read never corrects a hint, and the balancer hands every lead it can back to its preferred
/// leader: a hint kept for good sent a client's strong reads to a node that had stopped leading,
/// for as long as no write hopped. So a hint lapses, and its group is routed by its weights again
/// ([item 223](../../../../docs/src/appendix/resolved/leader-hints-lapse.md)).
#[derive(Debug, Default)]
pub(crate) struct LeaderHints {
    /// The leader each group's last hopped write was answered with, and when
    leaders: papaya::HashMap<GroupId, (NodeId, Instant)>,
}

impl LeaderHints {
    /// Remember the leader a write through a group was answered with
    ///
    /// # Arguments
    ///
    /// * `group` - The group, as the write's token names it
    /// * `leader` - The node leading it
    pub(crate) fn note(&self, group: GroupId, leader: NodeId) {
        self.leaders.pin().insert(group, (leader, Instant::now()));
    }

    /// The leader a group's writes were last told of, if they were told within
    /// [`LEADER_HINT_FOR`] of `now`
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `now` - The time the hint is asked for at, read once a bundle
    pub(crate) fn leader(&self, group: GroupId, now: Instant) -> Option<NodeId> {
        self.leaders
            .pin()
            .get(&group)
            // a hint older than its lapse is no better a guess than the weights
            .filter(|(_, noted)| now.saturating_duration_since(*noted) < LEADER_HINT_FOR)
            .map(|(leader, _)| *leader)
    }
}

/// The connections a client keeps to one node
#[derive(Clone)]
pub(crate) enum NodePools {
    /// The client's own endpoint pools: the node is the one endpoint the client was given, so a
    /// second pool to it would only duplicate the first
    Endpoints,
    /// Pools of the node's own
    Own {
        /// The connections every bundle to the node shares
        pool: bb8::Pool<ShoalConnectionManager>,
        /// The connections set apart for long streams, if any are
        bulk: Option<bb8::Pool<ShoalConnectionManager>>,
    },
}

/// One node a client routes to: where it is, and its pools once a run has been sent there
struct NodeEntry {
    /// Where clients reach it, as the topology frame spelled it when this entry was made
    addr: String,
    /// Its pools, built the first time a run goes there; none if it cannot be reached
    pools: tokio::sync::OnceCell<Option<NodePools>>,
}

/// Each member's client address as resolved, and which members are the client's own endpoints
///
/// A member that advertises a name is resolved off the send path, once a route table version, so
/// a plan never waits on a resolver.
#[derive(Debug, Default)]
struct Resolved {
    /// The route table version the addresses were resolved for
    version: u64,
    /// Whether a resolution is running
    pending: bool,
    /// Each member's address, by node, for those that resolved
    addrs: HashMap<NodeId, SocketAddr>,
    /// The version the homes below were judged for
    homes_version: u64,
    /// Whether every member's address was known when they were, so they need no judging again
    homes_final: bool,
    /// The members that are the client's own endpoints, by index
    homes: Vec<u16>,
}

/// Picks where each query goes and keeps a pool for every node a query has gone to
pub(crate) struct Router {
    /// The topology the servers push, which holds the route table every bundle is planned by
    topology: Arc<TopologyState>,
    /// The manager a node's pools are made from, which every node's connections share their
    /// ids, proxy, credentials and encryption with
    template: ShoalConnectionManager,
    /// How each node's pool is sized and aged
    node_pool: PoolConfig,
    /// How many connections to each node are set apart for long streams
    dedicated: u32,
    /// The one endpoint the client was given, whose node shares the endpoint pools
    seed: Option<SocketAddr>,
    /// Every endpoint the client was given, whose members a read at `One` prefers
    seeds: Vec<SocketAddr>,
    /// The members' resolved addresses, and which of them are the client's endpoints
    resolved: Arc<Mutex<Resolved>>,
    /// Whether every endpoint the client was given is on this host
    seeds_loopback: bool,
    /// Every node routed to so far, by identity
    nodes: Mutex<HashMap<NodeId, Arc<NodeEntry>>>,
    /// The route table version the nodes were last pruned at
    pruned_at: AtomicU64,
    /// The member the next bundle starts at, which turns bundle by bundle
    next_home: AtomicUsize,
    /// The nodes nothing is routed to for a moment
    suspects: Arc<Suspects>,
    /// The leaders the client's writes were told of, which the readers fill
    hints: Arc<LeaderHints>,
}

/// Whether an advertised address can be dialled from a client given these endpoints
///
/// An unspecified address or port is the bind address of a node that advertised none, and a
/// loopback address reached from a client whose endpoints are not on this host names a server
/// on the client's own machine - possibly another one serving the same schema. A name is assumed
/// to be dialable until it is resolved.
///
/// # Arguments
///
/// * `addr` - The address a member advertised
/// * `seeds_loopback` - Whether every endpoint the client was given is on this host
fn dialable(addr: &SocketAddr, seeds_loopback: bool) -> bool {
    !addr.ip().is_unspecified() && addr.port() != 0 && (seeds_loopback || !addr.ip().is_loopback())
}

impl Router {
    /// Build a router that makes its node pools from a manager
    ///
    /// # Arguments
    ///
    /// * `topology` - The topology the servers push
    /// * `template` - The client's endpoint manager, which every node's manager copies
    /// * `node_pool` - How each node's pool is sized and aged
    /// * `dedicated` - How many connections to each node are set apart for long streams
    /// * `endpoints` - The endpoints the client was given
    /// * `suspects` - The nodes nothing is routed to for a moment, which the readers mark
    /// * `hints` - The leaders the client's writes were told of, which the readers fill
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        topology: Arc<TopologyState>,
        template: ShoalConnectionManager,
        node_pool: PoolConfig,
        dedicated: u32,
        endpoints: &[SocketAddr],
        suspects: Arc<Suspects>,
        hints: Arc<LeaderHints>,
    ) -> Self {
        Router {
            topology,
            template,
            node_pool,
            dedicated,
            // a node is only folded into the endpoint pools when the client has one endpoint
            seed: (endpoints.len() == 1).then(|| endpoints[0]),
            seeds: endpoints.to_vec(),
            resolved: Arc::new(Mutex::new(Resolved::default())),
            seeds_loopback: !endpoints.is_empty()
                && endpoints.iter().all(|endpoint| endpoint.ip().is_loopback()),
            nodes: Mutex::new(HashMap::new()),
            pruned_at: AtomicU64::new(0),
            next_home: AtomicUsize::new(0),
            suspects,
            hints,
        }
    }

    /// The route table to plan a bundle by, if the servers have pushed one that places anything
    pub(crate) fn routes(&self) -> Option<Arc<RouteTable>> {
        self.topology.routes()
    }

    /// Whether a member can be routed to now: not suspect, and not advertising an address no
    /// client could dial
    ///
    /// # Arguments
    ///
    /// * `routes` - The route table
    /// * `member` - The member's index in it
    pub(crate) fn usable(&self, routes: &RouteTable, member: u16) -> bool {
        let Some(member) = routes.members.get(usize::from(member)) else {
            return false;
        };
        // an address that names an IP is judged now; a name is judged when it is resolved
        let dialable = member
            .client
            .parse::<SocketAddr>()
            .map_or(true, |addr| dialable(&addr, self.seeds_loopback));
        dialable && !self.suspects.is_suspect(member.node)
    }

    /// The members that are the client's own endpoints, by index
    ///
    /// A member advertising an address is matched against the endpoints at once; one advertising
    /// a name is matched once a resolution started here, off the send path, has resolved it.
    ///
    /// # Arguments
    ///
    /// * `routes` - The route table
    fn homes(&self, routes: &RouteTable) -> Vec<u16> {
        let mut resolved = self.resolved.lock().unwrap_or_else(PoisonError::into_inner);
        // judged already for this version, with every address known
        if resolved.homes_version == routes.version && resolved.homes_final {
            return resolved.homes.clone();
        }
        // resolve this version's names in the background, once
        if resolved.version != routes.version && !resolved.pending {
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                resolved.pending = true;
                let members: Vec<(NodeId, String)> = routes
                    .members
                    .iter()
                    .map(|member| (member.node, member.client.clone()))
                    .collect();
                let shared = self.resolved.clone();
                let version = routes.version;
                runtime.spawn(async move {
                    // each member's first answer, if its name resolves at all
                    let mut addrs = HashMap::with_capacity(members.len());
                    for (node, client) in members {
                        if let Ok(mut found) = tokio::net::lookup_host(client.as_str()).await {
                            if let Some(addr) = found.next() {
                                addrs.insert(node, addr);
                            }
                        }
                    }
                    let mut resolved = shared.lock().unwrap_or_else(PoisonError::into_inner);
                    resolved.version = version;
                    resolved.addrs = addrs;
                    resolved.pending = false;
                });
            }
        }
        // the members whose address is one of the client's endpoints, as far as is known
        let mut known = true;
        let mut homes = Vec::new();
        for (at, member) in routes.members.iter().enumerate() {
            let addr = match member.client.parse::<SocketAddr>() {
                Ok(addr) => Some(addr),
                Err(_) if resolved.version == routes.version => {
                    resolved.addrs.get(&member.node).copied()
                }
                Err(_) => {
                    known = false;
                    None
                }
            };
            if addr.is_some_and(|addr| self.seeds.contains(&addr)) {
                // truncation cannot happen: the table indexes its members by u16
                #[allow(clippy::cast_possible_truncation)]
                homes.push(at as u16);
            }
        }
        resolved.homes_version = routes.version;
        resolved.homes_final = known;
        resolved.homes.clone_from(&homes);
        homes
    }

    /// The member a bundle starts at: one of the client's own endpoints that is a member and can
    /// be routed to, in turn, else any member that can
    ///
    /// A read at `One` sticks to its bundle's home where it holds the tablet, so a client keeps
    /// reading where it was pointed - a node of its own host, often, or the one its operator
    /// chose - as it did before it routed; only a read its endpoints cannot serve goes elsewhere
    /// ([F74](../../../../docs/src/features/client-routing.md)).
    ///
    /// # Arguments
    ///
    /// * `routes` - The route table
    /// * `usable` - Whether each member can be routed to now, by index
    /// * `homes` - The members that are the client's own endpoints, by index
    pub(crate) fn home(&self, routes: &RouteTable, usable: &[bool], homes: &[u16]) -> Option<u16> {
        let count = routes.members.len();
        if count == 0 {
            return None;
        }
        let start = self.next_home.fetch_add(1, Ordering::Relaxed);
        // the client's own endpoints first, in turn among those that can serve
        let own: Vec<u16> = homes
            .iter()
            .copied()
            .filter(|at| {
                let at = usize::from(*at);
                routes.members.get(at).is_some_and(|member| member.up) && usable[at]
            })
            .collect();
        if !own.is_empty() {
            return Some(own[start % own.len()]);
        }
        // else every member, in turn
        (0..count)
            .map(|offset| (start.wrapping_add(offset)) % count)
            .find(|at| routes.members[*at].up && usable[*at])
            // truncation cannot happen: the table indexes its members by u16
            .map(|at| at as u16)
    }

    /// Plan a bundle's queries into runs, each with the node it goes to
    ///
    /// One run to the endpoints when there is no route table, which is exactly what a client
    /// that does not route sends.
    ///
    /// # Arguments
    ///
    /// * `queries` - The bundle's queries
    /// * `read` - The level the bundle asked its reads to be served at, if it named one
    pub(crate) fn plan<S: QuerySupport>(
        &self,
        queries: &[S::QueryKinds],
        read: Option<ReadLevel>,
    ) -> (u64, Vec<(Option<NodeId>, usize, usize)>) {
        // nothing to route by yet: one run, through the endpoints
        let Some(routes) = self.routes() else {
            return (0, vec![(None, 0, queries.len())]);
        };
        let described = query_routes::<S>(&routes, queries, read);
        // whether each member can be routed to, judged once a bundle rather than once a key
        let judged: Vec<bool> = (0..routes.members.len())
            .map(|at| {
                // truncation cannot happen: the table indexes its members by u16
                #[allow(clippy::cast_possible_truncation)]
                let member = at as u16;
                self.usable(&routes, member)
            })
            .collect();
        let usable = |member: u16| judged.get(usize::from(member)).copied().unwrap_or(false);
        // a group's leader: the one its writes were last answered with while that hint is fresh
        // and that member can be routed to, else the one its weights prefer
        let now = Instant::now();
        let leader = |table: TableId, tablet: usize| {
            let hinted = routes
                .group(table, tablet)
                .and_then(|group| self.hints.leader(group, now))
                .and_then(|node| routes.member_of(node))
                .filter(|member| usable(*member) && routes.members[usize::from(*member)].up);
            hinted.or_else(|| {
                routes
                    .leader(table, tablet)
                    .and_then(|leader| routes.member_of(leader.node))
            })
        };
        let judge = Judge {
            usable: &usable,
            leader: &leader,
        };
        let homes = self.homes(&routes);
        let home = self.home(&routes, &judged, &homes);
        let runs = plan_runs(&routes, &described, home, &judge);
        // the runs by node rather than by member index, which is stable across tables
        let runs = runs
            .into_iter()
            .map(|run| {
                let node = run
                    .target
                    .map(|member| routes.members[usize::from(member)].node);
                (node, run.start, run.len)
            })
            .collect();
        (routes.version, runs)
    }

    /// Whether a node can be routed to now, by the newest route table
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    pub(crate) fn node_usable(&self, node: NodeId) -> bool {
        self.routes().is_some_and(|routes| {
            routes.member_of(node).is_some_and(|member| {
                routes.members[usize::from(member)].up && self.usable(&routes, member)
            })
        })
    }

    /// Route nothing to a node for a moment
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    /// * `why` - What made it suspect
    pub(crate) fn mark_suspect(&self, node: NodeId, why: &str) {
        self.suspects.mark(node, why);
    }

    /// The pools of a node, opened the first time anything is routed there
    ///
    /// None when the node is not in the newest route table, or its address cannot be resolved or
    /// dialled: its runs go through the endpoints instead. A node whose address changed gets new
    /// pools, and every node a newer table no longer names has its pools closed.
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    pub(crate) async fn pools(&self, node: NodeId) -> Option<NodePools> {
        let routes = self.routes()?;
        let addr = routes
            .member_of(node)
            .map(|member| routes.members[usize::from(member)].client.clone())?;
        // the node's entry, made again when its address moved
        let entry = {
            let mut nodes = self.nodes.lock().unwrap_or_else(PoisonError::into_inner);
            // a node a newer table no longer names is forgotten, and its pools with it
            if self.pruned_at.swap(routes.version, Ordering::Relaxed) != routes.version {
                nodes.retain(|known, entry| {
                    routes.member_of(*known).is_some_and(|member| {
                        routes.members[usize::from(member)].client == entry.addr
                    })
                });
            }
            let entry = nodes
                .entry(node)
                .or_insert_with(|| {
                    Arc::new(NodeEntry {
                        addr: addr.clone(),
                        pools: tokio::sync::OnceCell::new(),
                    })
                })
                .clone();
            if entry.addr == addr {
                entry
            } else {
                let fresh = Arc::new(NodeEntry {
                    addr,
                    pools: tokio::sync::OnceCell::new(),
                });
                nodes.insert(node, fresh.clone());
                fresh
            }
        };
        // build them once, whoever asks first; everyone else waits for the same answer
        entry
            .pools
            .get_or_init(|| self.build(node, &entry.addr))
            .await
            .clone()
    }

    /// Resolve a node's address and open its pools
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    /// * `addr` - Where clients reach it, as advertised
    async fn build(&self, node: NodeId, addr: &str) -> Option<NodePools> {
        // resolve what it advertised, taking the first answer
        let resolved = match tokio::net::lookup_host(addr).await {
            Ok(mut addrs) => addrs.next(),
            Err(error) => {
                event!(Level::WARN, msg = "a member's address did not resolve, so its queries go through the endpoints", %node, addr, %error);
                None
            }
        }?;
        // an address no client could dial is routed through the endpoints instead
        if !dialable(&resolved, self.seeds_loopback) {
            event!(Level::WARN, msg = "a member advertises an address this client cannot dial, so its queries go through the endpoints", %node, addr);
            return None;
        }
        // the node is the one endpoint this client was given, whose pools it already has
        if self.seed == Some(resolved) {
            return Some(NodePools::Endpoints);
        }
        // ask for the member's own name when it advertised one and the caller named none
        let host = addr.rsplit_once(':').map_or(addr, |(host, _)| host);
        let server_name = (host.parse::<IpAddr>().is_err()).then(|| host.to_string());
        let manager = self.template.for_node(node, resolved, server_name);
        // the shared pool, filled in the background so this send never waits on it
        let pool = bb8::Pool::builder()
            .min_idle(self.node_pool.min_idle)
            .max_size(self.node_pool.max_size)
            .connection_timeout(self.node_pool.connection_timeout)
            .idle_timeout(self.node_pool.idle_timeout)
            .max_lifetime(self.node_pool.max_lifetime)
            .build_unchecked(manager.clone());
        // and the connections set apart for long streams, opened only when one is needed
        let bulk = (self.dedicated > 0).then(|| {
            bb8::Pool::builder()
                .min_idle(0)
                .max_size(self.dedicated)
                .connection_timeout(self.node_pool.connection_timeout)
                .idle_timeout(self.node_pool.idle_timeout)
                .max_lifetime(self.node_pool.max_lifetime)
                .build_unchecked(manager)
        });
        event!(Level::DEBUG, msg = "opened a pool to a member", %node, %resolved);
        Some(NodePools::Own { pool, bulk })
    }

    /// The connections this client holds to each node it has routed to, shared and set apart
    pub(crate) fn connections(&self) -> Vec<(NodeId, u32, u32)> {
        let nodes = self.nodes.lock().unwrap_or_else(PoisonError::into_inner);
        let mut held: Vec<(NodeId, u32, u32)> = nodes
            .iter()
            .filter_map(|(node, entry)| match entry.pools.get() {
                Some(Some(NodePools::Own { pool, bulk })) => Some((
                    *node,
                    pool.state().connections,
                    bulk.as_ref().map_or(0, |bulk| bulk.state().connections),
                )),
                _ => None,
            })
            .collect();
        held.sort();
        held
    }
}

/// Fail a send with everything a caller needs to know about a pool that could not be had
///
/// # Arguments
///
/// * `error` - What the pool said
pub(crate) fn pool_failed(error: impl std::fmt::Display) -> Errors {
    Errors::ConnectionPool(format!("failed to get connection from pool: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{plan_runs, Judge, QueryRoute, Run, Suspects};
    use shoal_proto::shared::identity::{ClusterId, NodeId, TableId};
    use shoal_proto::shared::placement::TABLET_COUNT;
    use shoal_proto::shared::protocol::admin::{TopologyFrame, TopologyMember};
    use shoal_proto::shared::routes::RouteTable;
    use std::time::{Duration, Instant};

    /// A frame of some members at some factor, every one up
    ///
    /// # Arguments
    ///
    /// * `members` - How many members
    /// * `rf` - The replication factor
    /// * `weights` - Each member's lead weight
    fn frame(members: u64, rf: u32, weights: &[u32]) -> TopologyFrame {
        let members: Vec<TopologyMember> = (1..=members)
            .map(|n| TopologyMember {
                node: NodeId::from(n),
                client: format!("10.0.0.{n}:12000"),
                data: format!("10.0.0.{n}:12001"),
                control: format!("10.0.0.{n}:12002"),
                shards: 2,
                role: "voter".to_string(),
                health: "up".to_string(),
                phase: "member".to_string(),
                state: "up".to_string(),
                incarnation: 1,
                shards_failed: Vec::new(),
                quarantined: Vec::new(),
                lead_weight: weights.get((n - 1) as usize).copied().unwrap_or(1),
            })
            .collect();
        TopologyFrame {
            cluster: ClusterId::mint(),
            version: 3,
            leader: None,
            placement: members.iter().map(|member| member.node).collect(),
            members,
            desired_rf: rf,
            active_rf: rf,
            write_consistency: "quorum".to_string(),
            read_consistency: "one".to_string(),
            tables: vec![("Notes".to_string(), TableId::of("Notes"))],
            table_read_policy: Vec::new(),
            configurations: Vec::new(),
            moves: Vec::new(),
            tombstones: Vec::new(),
        }
    }

    /// A key on a tablet
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    fn key(tablet: usize) -> u64 {
        (tablet as u64) << 52
    }

    /// A write of one key
    ///
    /// # Arguments
    ///
    /// * `keys` - The key, in a slice that outlives the route
    fn write(keys: &[u64]) -> QueryRoute<'_> {
        QueryRoute {
            table: TableId::of("Notes"),
            keys,
            write: true,
            leader_bound: true,
        }
    }

    /// A read at `One`
    ///
    /// # Arguments
    ///
    /// * `keys` - Its keys, in a slice that outlives the route
    fn read(keys: &[u64]) -> QueryRoute<'_> {
        QueryRoute {
            table: TableId::of("Notes"),
            keys,
            write: false,
            leader_bound: false,
        }
    }

    /// Every member can be routed to
    fn all(_: u16) -> bool {
        true
    }

    /// Each group's leader as its weights prefer, with no hint
    ///
    /// # Arguments
    ///
    /// * `routes` - The route table
    fn preferred(routes: &RouteTable) -> impl Fn(TableId, usize) -> Option<u16> + '_ {
        move |table, tablet| {
            routes
                .leader(table, tablet)
                .and_then(|leader| routes.member_of(leader.node))
        }
    }

    /// A bundle's runs cover every query once, in order, at their own offsets, and adjacent
    /// queries bound for one member share a run
    #[test]
    fn a_bundle_is_cut_into_runs_that_keep_their_offsets() {
        let routes = RouteTable::from_frame(&frame(3, 3, &[])).expect("placed");
        // sixteen writes on tablets whose primaries are members 0, 0, 1, 2, 2, 0, ...
        let tablets = [0usize, 3, 1, 2, 5, 6, 9, 4, 7, 10, 13, 8, 11, 12, 15, 14];
        let keys: Vec<[u64; 1]> = tablets.iter().map(|tablet| [key(*tablet)]).collect();
        let queries: Vec<QueryRoute<'_>> = keys.iter().map(|key| write(key)).collect();
        let runs = plan_runs(
            &routes,
            &queries,
            None,
            &Judge {
                usable: &all,
                leader: &preferred(&routes),
            },
        );
        // every query once, in order
        let mut next = 0;
        for run in &runs {
            assert_eq!(run.start, next);
            assert!(run.len > 0);
            next += run.len;
        }
        assert_eq!(next, queries.len());
        // each write goes to its tablet's primary, which equal weights make its leader
        for run in &runs {
            for offset in run.start..run.start + run.len {
                let tablet = tablets[offset];
                let primary = routes.replicas(tablet)[0].node;
                assert_eq!(
                    run.target,
                    routes.member_of(primary),
                    "tablet {tablet} went elsewhere"
                );
            }
        }
        // and no two neighbouring runs go to the same member
        for pair in runs.windows(2) {
            assert_ne!(pair[0].target, pair[1].target);
        }
    }

    /// Reads at `One` stick to the run they join and to the bundle's home, so a bundle of them on
    /// a cluster that holds every tablet everywhere is one run, and the home spreads bundles
    #[test]
    fn reads_at_one_stick_to_their_run() {
        let routes = RouteTable::from_frame(&frame(3, 3, &[])).expect("placed");
        let keys: Vec<[u64; 1]> = (0..16).map(|tablet| [key(tablet * 7)]).collect();
        let queries: Vec<QueryRoute<'_>> = keys.iter().map(|key| read(key)).collect();
        for home in 0..3u16 {
            let runs = plan_runs(
                &routes,
                &queries,
                Some(home),
                &Judge {
                    usable: &all,
                    leader: &preferred(&routes),
                },
            );
            assert_eq!(
                runs,
                vec![Run {
                    target: Some(home),
                    start: 0,
                    len: 16
                }]
            );
        }
        // with no home the reads still stick to the first one's choice
        assert_eq!(
            plan_runs(
                &routes,
                &queries,
                None,
                &Judge {
                    usable: &all,
                    leader: &preferred(&routes)
                }
            )
            .len(),
            1
        );
    }

    /// A write goes to its weighted leader, and to the first holder once that member cannot be
    /// routed to; with no member left, through the endpoints
    #[test]
    fn writes_go_to_the_leader_and_fall_back_to_a_holder() {
        let routes = RouteTable::from_frame(&frame(3, 3, &[3, 1, 1])).expect("placed");
        let table = TableId::of("Notes");
        for tablet in (0..TABLET_COUNT).step_by(97) {
            let keys = [key(tablet)];
            let query = write(&keys);
            let leader = routes
                .member_of(routes.leader(table, tablet).expect("a leader").node)
                .expect("a member");
            assert_eq!(
                plan_runs(
                    &routes,
                    &[query],
                    None,
                    &Judge {
                        usable: &all,
                        leader: &preferred(&routes)
                    }
                )[0]
                .target,
                Some(leader)
            );
            // the leader cannot be routed to: the first other holder
            let without = |member: u16| member != leader;
            let fallback = routes.holders(tablet).find(|member| *member != leader);
            assert_eq!(
                plan_runs(
                    &routes,
                    &[query],
                    None,
                    &Judge {
                        usable: &without,
                        leader: &preferred(&routes)
                    }
                )[0]
                .target,
                fallback
            );
            // nobody can: the endpoints
            assert_eq!(
                plan_runs(
                    &routes,
                    &[query],
                    None,
                    &Judge {
                        usable: &|_| false,
                        leader: &preferred(&routes)
                    }
                )[0]
                .target,
                None
            );
        }
    }

    /// A write goes to the leader its group's writes were last answered with, over its preferred
    /// one, while that member can be routed to; and a hint is kept by group
    #[test]
    fn writes_follow_a_hinted_leader() {
        let routes = RouteTable::from_frame(&frame(3, 3, &[])).expect("placed");
        let table = TableId::of("Notes");
        let keys = [key(0)];
        let query = write(&keys);
        let preferred_member = routes
            .member_of(routes.leader(table, 0).expect("a leader").node)
            .expect("a member");
        // a hint names another member, which the write follows
        let hinted = (preferred_member + 1) % 3;
        let leader = |_: TableId, _: usize| Some(hinted);
        let judge = Judge {
            usable: &all,
            leader: &leader,
        };
        assert_eq!(
            plan_runs(&routes, &[query], None, &judge)[0].target,
            Some(hinted)
        );
        // and the hints keep the newest leader a group was told of
        let hints = super::LeaderHints::default();
        let group = routes.group(table, 0).expect("a group");
        assert_eq!(hints.leader(group, Instant::now()), None);
        hints.note(group, routes.members[1].node);
        hints.note(group, routes.members[2].node);
        assert_eq!(
            hints.leader(group, Instant::now()),
            Some(routes.members[2].node)
        );
    }

    /// A leader a group's writes were told of is followed until it lapses, and then the group is
    /// routed by its weights again (item 223)
    #[test]
    fn a_hint_lapses_back_to_the_preferred_leader() {
        let routes = RouteTable::from_frame(&frame(3, 3, &[])).expect("placed");
        let table = TableId::of("Notes");
        let group = routes.group(table, 0).expect("a group");
        let hints = super::LeaderHints::default();
        // a hint taught between these two moments
        let before = Instant::now();
        hints.note(group, routes.members[1].node);
        let after = Instant::now();
        // followed until its lapse, however late within it
        assert_eq!(hints.leader(group, after), Some(routes.members[1].node));
        assert_eq!(
            hints.leader(
                group,
                before + super::LEADER_HINT_FOR - Duration::from_millis(1)
            ),
            Some(routes.members[1].node)
        );
        // and not from its lapse on
        assert_eq!(hints.leader(group, after + super::LEADER_HINT_FOR), None);
        // a time before the hint was taught reads it as fresh, not as an underflow
        assert_eq!(hints.leader(group, before), Some(routes.members[1].node));
        // a newer hop teaches it again
        hints.note(group, routes.members[2].node);
        assert_eq!(
            hints.leader(group, Instant::now()),
            Some(routes.members[2].node)
        );
    }

    /// A get whose keys live on several members goes whole to the member holding the most
    #[test]
    fn a_multi_key_get_goes_whole_to_the_member_holding_most() {
        // three members at a factor of one: every tablet on one member
        let routes = RouteTable::from_frame(&frame(3, 1, &[])).expect("placed");
        let owner = |tablet: usize| routes.member_of(routes.replicas(tablet)[0].node);
        // two keys on member 1 and one on member 0, the member 0 key first
        let keys = [key(0), key(1), key(4)];
        assert_eq!(owner(0), Some(0));
        assert_eq!(owner(1), Some(1));
        assert_eq!(owner(4), Some(1));
        let runs = plan_runs(
            &routes,
            &[read(&keys)],
            None,
            &Judge {
                usable: &all,
                leader: &preferred(&routes),
            },
        );
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].target, Some(1));
        // a tie goes to the first key's own choice
        let tied = [key(0), key(1)];
        assert_eq!(
            plan_runs(
                &routes,
                &[read(&tied)],
                None,
                &Judge {
                    usable: &all,
                    leader: &preferred(&routes)
                }
            )[0]
            .target,
            Some(0)
        );
    }

    /// A bundle's home is the client's own endpoint when it is a member, and any member in turn
    /// once that one cannot be routed to, so reads at `One` stay where the client was pointed
    #[test]
    fn the_home_is_the_clients_own_endpoint() {
        use super::super::{ShoalConnectionManager, StreamConfig, TopologyState};
        use super::{PoolConfig, Router};
        use std::sync::Arc;
        // a client pointed at the second of three members
        let endpoint: std::net::SocketAddr = "10.0.0.2:12000".parse().expect("an address");
        let (proxy_tx, _proxy_rx) = kanal::unbounded_async();
        let manager = ShoalConnectionManager::new(
            vec![endpoint],
            std::time::Duration::from_secs(1),
            proxy_tx,
            &Arc::new(papaya::HashMap::new()),
            &Arc::new(papaya::HashMap::new()),
            &Arc::new(std::sync::atomic::AtomicU32::new(0)),
            &Arc::new(std::sync::atomic::AtomicU64::new(0)),
            0,
            super::super::ClientOptions::new(),
            StreamConfig::default(),
        )
        .expect("a manager");
        let topology = Arc::new(TopologyState::new());
        topology.install(frame(3, 3, &[]));
        let router = Router::new(
            topology,
            manager,
            PoolConfig::per_node(),
            2,
            &[endpoint],
            Arc::new(Suspects::default()),
            Arc::new(super::LeaderHints::default()),
        );
        let routes = router.routes().expect("a placed frame");
        assert_eq!(router.homes(&routes), vec![1]);
        // every bundle starts there while it can be routed to
        let all = vec![true; 3];
        for _ in 0..4 {
            assert_eq!(router.home(&routes, &all, &[1]), Some(1));
        }
        // and at the others in turn once it cannot
        let without = vec![true, false, true];
        let homes: Vec<Option<u16>> = (0..4)
            .map(|_| router.home(&routes, &without, &[1]))
            .collect();
        assert!(
            homes.iter().all(|home| matches!(home, Some(0 | 2))),
            "{homes:?}"
        );
        assert!(homes.contains(&Some(0)) && homes.contains(&Some(2)));
    }

    /// A suspect node is routed around until its mark runs out
    #[test]
    fn a_suspect_is_routed_around_for_a_moment() {
        let suspects = Suspects::default();
        let node = NodeId::from(7u64);
        assert!(!suspects.is_suspect(node));
        suspects.mark(node, "a test");
        assert!(suspects.is_suspect(node));
        assert!(!suspects.is_suspect(NodeId::from(8u64)));
    }

    /// An address no client could dial is refused, and loopback only from a loopback client
    #[test]
    fn only_dialable_addresses_are_routed_to() {
        let parse = |addr: &str| addr.parse().expect("an address");
        assert!(super::dialable(&parse("10.0.0.1:12000"), false));
        assert!(!super::dialable(&parse("0.0.0.0:12000"), false));
        assert!(!super::dialable(&parse("10.0.0.1:0"), false));
        assert!(!super::dialable(&parse("127.0.0.1:12000"), false));
        assert!(super::dialable(&parse("127.0.0.1:12000"), true));
    }
}
