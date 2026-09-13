//! The outbound peer links one shard owns, and what it is owed back over them
//!
//! A shard that forwards a query to another node owns a [`Link`] per peer and lane, spawns it
//! lazily on first use, and keeps a `pending` record per forwarded query until an answer, a
//! deadline or a lost link resolves it. Every one of those three is a definite outcome for the
//! client: an answer is the answer, a lost link with the frame still queued is a definite refusal
//! (nothing was written), and anything in between - written and unanswered when the link drops or
//! the deadline passes - is an unknown outcome, because a write may have applied.

use rustls::ClientConfig;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::Arc;

use super::handshake::Local;
use super::link::{Frame, FrameKey, Link, LinkEvent, LinkView};
use super::Lane;
use crate::server::conf::cluster::{DialOverride, Transport};
use crate::server::map::MapCell;
use crate::server::database::ShoalDatabase;
use crate::server::messages::{PeerEvent, ServerMsg};
use crate::server::stage_profile::{StageStamps, Stamp};
use crate::shared::identity::NodeId;
use kanal::Sender;
use tracing::Span;
use uuid::Uuid;

/// What this shard is owed for one forwarded query
pub struct Pending<D: ShoalDatabase> {
    /// The client the answer is owed to, which is the connection the bundle came in on
    pub client: Uuid,
    /// The span the answer is written under
    pub span: Span,
    /// When the query reached each stage
    pub stamps: StageStamps,
    /// The table the query named, so a failure can be built in the right variant
    pub table: <D::ClientType as crate::shared::traits::QuerySupport>::TableNames,
    /// Whether the query was the last of its stream
    pub end: bool,
    /// Whether the answer is a share to be merged rather than a whole answer
    pub share: bool,
    /// When it was forwarded
    pub sent_at: Stamp,
    /// When the origin stops waiting: the forward timeout from `sent_at`, or the bundle's
    /// deadline if that is sooner ([F41](../../../../docs/src/features/read-consistency.md))
    pub deadline: Stamp,
    /// The attempt at the bundle it was forwarded under
    pub attempt: u64,
    /// The slot of the origin's gather it fills, if it is a share
    pub slot: u16,
}

/// The links to every peer this shard forwards to
pub struct Peers<D: ShoalDatabase> {
    /// One link per peer and lane, spawned on first use
    links: HashMap<(NodeId, Lane), Link>,
    /// The map this shard holds, which is where every member's address comes from
    map: MapCell,
    /// Where particular members are dialled instead of where they advertise
    dial: BTreeMap<NodeId, DialOverride>,
    /// What this node says about itself in a hello
    local: Rc<RefCell<Local>>,
    /// What to dial peers with, if the lanes are encrypted
    tls: Option<Arc<ClientConfig>>,
    /// The bounds and timers
    transport: Transport,
    /// A sync handle on this shard's own mesh channel, for a link to deliver events on
    events: Sender<ServerMsg<D>>,
    /// What this shard is owed, keyed by bundle, index and the node it went to
    pub pending: HashMap<(Uuid, u64, NodeId), Pending<D>>,
}

impl<D: ShoalDatabase> Peers<D> {
    /// Build the peers for a shard
    ///
    /// # Arguments
    ///
    /// * `map` - The map this shard holds
    /// * `dial` - Where particular members are dialled instead of where they advertise
    /// * `local` - What this node says about itself
    /// * `tls` - What to dial peers with, if encrypted
    /// * `transport` - The bounds and timers
    /// * `events` - This shard's own mesh channel, for links to deliver on
    pub fn new(
        map: MapCell,
        dial: BTreeMap<NodeId, DialOverride>,
        local: Rc<RefCell<Local>>,
        tls: Option<Arc<ClientConfig>>,
        transport: Transport,
        events: Sender<ServerMsg<D>>,
    ) -> Self {
        Peers {
            links: HashMap::new(),
            map,
            dial,
            local,
            tls,
            transport,
            events,
            pending: HashMap::new(),
        }
    }

    /// Get the link to a peer on a lane, spawning it if this is the first frame
    ///
    /// # Arguments
    ///
    /// * `node` - The peer
    /// * `lane` - The lane
    fn link(&mut self, node: NodeId, lane: Lane) -> Option<&Link> {
        // a peer the map does not know is one we cannot dial
        let mut entry = self.map.get().peer_addr(node)?;
        // dialled where this node was told to dial it, if it was told
        if let Some(target) = self.dial.get(&node) {
            if let Some(control) = &target.control {
                entry.control.clone_from(control);
            }
            if let Some(data) = &target.data {
                entry.data.clone_from(data);
            }
        }
        // a link to an address the member no longer advertises is dropped and dialled afresh;
        // every frame it held is reported down, which the shard answers as it answers any lost
        // link
        if self
            .links
            .get(&(node, lane))
            .is_some_and(|link| *link.target() != entry)
        {
            self.links.remove(&(node, lane));
        }
        let local = self.local.clone();
        let tls = self.tls.clone();
        let transport = self.transport.clone();
        let events = self.events.clone();
        Some(self.links.entry((node, lane)).or_insert_with(|| {
            // the link delivers what it learns onto this shard's mesh, sync because a closure
            // cannot await; the mesh is unbounded, so a try_send never fails for capacity
            let on_event = move |event: LinkEvent| {
                let _ = events.try_send(ServerMsg::Peer(PeerEvent::Link(event)));
            };
            Link::spawn(lane, entry, local, &transport, tls, on_event)
        }))
    }

    /// Enqueue a frame to a peer, or refuse it because the queue is full or the peer is unknown
    ///
    /// # Arguments
    ///
    /// * `node` - The peer
    /// * `lane` - The lane
    /// * `frame` - The frame to send
    ///
    /// # Errors
    ///
    /// Hands the frame back with the bound it would have passed, or `None` for a peer that is
    /// not in the placement at all.
    pub fn enqueue(
        &mut self,
        node: NodeId,
        lane: Lane,
        frame: Frame,
    ) -> Result<(), Option<usize>> {
        let Some(link) = self.link(node, lane) else {
            return Err(None);
        };
        link.enqueue(frame).map_err(|(_, bound)| Some(bound))
    }

    /// Record what is owed for a forwarded query
    ///
    /// # Arguments
    ///
    /// * `bundle` - The bundle the query arrived in
    /// * `index` - The index the answer is owed under
    /// * `node` - The peer it was forwarded to
    /// * `pending` - What is owed
    pub fn expect(&mut self, bundle: Uuid, index: u64, node: NodeId, pending: Pending<D>) {
        self.pending.insert((bundle, index, node), pending);
    }

    /// Take what was owed for one answered query, if it is still owed
    ///
    /// # Arguments
    ///
    /// * `bundle` - The bundle the query arrived in
    /// * `index` - The index the answer is owed under
    /// * `node` - The peer that answered
    pub fn take(&mut self, bundle: Uuid, index: u64, node: NodeId) -> Option<Pending<D>> {
        self.pending.remove(&(bundle, index, node))
    }

    /// Take everything owed to one node whose keys are in the given set
    ///
    /// The frames a lost link never wrote name exactly the queries whose outcome is a definite
    /// refusal; everything else owed to that node is an unknown outcome.
    ///
    /// # Arguments
    ///
    /// * `node` - The node whose link was lost
    /// * `unsent` - The keys of frames the link never wrote
    pub fn drain_node(
        &mut self,
        node: NodeId,
        unsent: &[FrameKey],
    ) -> (Vec<((Uuid, u64), Pending<D>)>, Vec<((Uuid, u64), Pending<D>)>) {
        // which (bundle, index) were never written
        let mut never_written = std::collections::HashSet::new();
        for key in unsent {
            if let FrameKey::Forward(pairs) = key {
                for pair in pairs {
                    never_written.insert(*pair);
                }
            }
        }
        // split what is owed to this node into the two outcomes
        let owed: Vec<_> = self
            .pending
            .keys()
            .filter(|(_, _, owed_node)| *owed_node == node)
            .copied()
            .collect();
        let mut refused = Vec::new();
        let mut unknown = Vec::new();
        for (bundle, index, owed_node) in owed {
            let pending = self.pending.remove(&(bundle, index, owed_node)).expect("just listed");
            if never_written.contains(&(bundle, index)) {
                refused.push(((bundle, index), pending));
            } else {
                unknown.push(((bundle, index), pending));
            }
        }
        (refused, unknown)
    }

    /// Take everything that has waited past its own deadline
    ///
    /// # Arguments
    ///
    /// * `now` - The current stamp
    pub fn expired(&mut self, now: Stamp) -> Vec<((Uuid, u64, NodeId), Pending<D>)> {
        let stale: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, pending)| now.since(pending.deadline) > 0)
            .map(|(key, _)| *key)
            .collect();
        stale
            .into_iter()
            .map(|key| (key, self.pending.remove(&key).expect("just listed")))
            .collect()
    }

    /// Forget everything owed for one query, whichever nodes it went to
    ///
    /// A gather that expired has answered its query; the shares still owed for it must not be
    /// answered a second time when their forward deadline passes.
    ///
    /// # Arguments
    ///
    /// * `bundle` - The bundle the query arrived in
    /// * `index` - The index the answer was owed under
    pub fn forget(&mut self, bundle: Uuid, index: u64) -> usize {
        let before = self.pending.len();
        self.pending
            .retain(|(owed_bundle, owed_index, _), _| !(*owed_bundle == bundle && *owed_index == index));
        before - self.pending.len()
    }

    /// What every link this shard owns looks like from outside
    #[must_use]
    pub fn views(&self) -> Vec<LinkView> {
        self.links.values().map(Link::view).collect()
    }

    /// The bounds and timers, for the shard to read its deadline off
    #[must_use]
    pub fn transport(&self) -> &Transport {
        &self.transport
    }
}
