//! The stripe model: an object's entry and stripes, their holders, and everyone acting on them
//!
//! [`StripeWorld::apply`] is the only way the model moves, and it is total, as the tablet
//! model's is: an event that does not fit the state - a delivery of a message never sent, a
//! restart of a node that is up, a step of an apply nobody learned about - is skipped and
//! counted, so every subsequence of a valid schedule is a valid schedule and the minimizer can
//! drop any event. After every event the checker judges the durable facts against the clauses.

use std::collections::{BTreeMap, VecDeque};

use crate::ids::{NodeId, OpId};
use crate::invariants::Violation;
use crate::stripe::actors::Driver;
use crate::stripe::check::{StripeChecker, StripeCoverage};
use crate::stripe::content::{encode, Unit};
use crate::stripe::event::{Body, Endpoint, Message, StripeEvent};
use crate::stripe::group::{EntryGroup, StripeGroup};
use crate::stripe::holder::{Chunk, Disk, HolderFact, Node, Slice};
use crate::stripe::ids::{DeviceId, DiskId, Label, Pos, SliceId, StripeIx};
use crate::stripe::layout::Layout;
use crate::stripe::oracle::{OpKind, StripeLedger, StripeOutcome};
use crate::stripe::policy::{DeviceIdentity, StripePolicy};
use crate::stripe::progress::{Progress, ProgressFailure};
use crate::stripe::schedule::{StripeParams, StripeSchedule};

/// The messages sent and the ones still in flight
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Network {
    /// Every message ever sent, in order; a delivery of any of them is legal
    pub sent: Vec<Message>,
    /// The ones not yet delivered, in send order
    pub in_flight: Vec<Message>,
}

impl Network {
    /// Drop a message in flight, so it is never delivered unless duplicated later
    ///
    /// # Arguments
    ///
    /// * `msg` - The message
    pub fn drop_message(&mut self, msg: &Message) -> bool {
        match self.in_flight.iter().position(|m| m == msg) {
            Some(position) => {
                self.in_flight.remove(position);
                true
            }
            None => false,
        }
    }
}

/// The leader's advisory reservations of one stripe
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Reservations {
    /// Who has the turn
    pub holder: Option<Endpoint>,
    /// Who asked and waits, in order
    pub queue: VecDeque<Endpoint>,
}

/// The model
#[derive(Debug, Clone)]
pub struct StripeWorld {
    /// What the schedule was built with
    pub params: StripeParams,
    /// The policy in force
    pub policy: StripePolicy,
    /// The step the next event will be applied at
    pub step: u64,
    /// The network
    pub net: Network,
    /// The object entry's tablet group
    pub entry: EntryGroup,
    /// Each stripe's tablet group
    pub rows: Vec<StripeGroup>,
    /// The nodes
    pub nodes: BTreeMap<NodeId, Node>,
    /// The disks, the ones swapped out included
    pub disks: BTreeMap<DiskId, Disk>,
    /// The slices, the ones gone included
    pub slices: BTreeMap<SliceId, Slice>,
    /// Every operation's driver, while it runs
    pub drivers: BTreeMap<OpId, Driver>,
    /// The round each operation is in; an answer to an earlier one is dropped
    pub rounds: BTreeMap<OpId, u8>,
    /// The leader's reservations
    pub reservations: BTreeMap<StripeIx, Reservations>,
    /// The client history
    pub ledger: StripeLedger,
    /// The checker
    pub checker: StripeChecker,
    /// The progress check
    pub progress: Progress,
    /// The first violation found, which freezes the world
    pub violation: Option<Violation>,
    /// The first progress bound broken
    pub stalled: Option<ProgressFailure>,
    /// How many events did not fit the state and were skipped
    pub skipped: u32,
    /// The next number to mint a disk, device or slice identity with
    pub next_id: u32,
    /// Messages to send at the end of this step
    pub outbox: Vec<Message>,
    /// Holder facts for the checker at the end of this step
    pub facts: Vec<(SliceId, HolderFact)>,
}

/// What replaying a stripe schedule produced
#[derive(Debug, Clone)]
pub struct StripeOutcomeOfRun {
    /// The first violation, the oracle's included
    pub violation: Option<Violation>,
    /// The first progress bound broken
    pub stalled: Option<ProgressFailure>,
    /// The history
    pub ledger: StripeLedger,
    /// What the run exercised
    pub coverage: StripeCoverage,
    /// The steps readers and writers took, for the progress record
    pub progress: Progress,
    /// How many events were skipped
    pub skipped: u32,
    /// How many steps were applied
    pub steps: u64,
}

impl StripeWorld {
    /// Build a fresh world: one object put whole over every stripe, spare slices beside it
    ///
    /// # Arguments
    ///
    /// * `params` - The layout, the number of stripes and spares
    /// * `policy` - The policy
    pub fn new(params: &StripeParams, policy: StripePolicy) -> Self {
        let layout = params.layout;
        let width = layout.width();
        let mut nodes = BTreeMap::new();
        let mut disks = BTreeMap::new();
        let mut slices = BTreeMap::new();
        // one node a device, each serving one slice: the stripe's positions, then the spares
        for n in 0..width + usize::from(params.spares) {
            let id = n as u32;
            let node = NodeId(n as u8);
            disks.insert(
                DiskId(id),
                Disk {
                    id: DiskId(id),
                    healthy: true,
                    full: false,
                    marker: Some((DeviceId(id), SliceId(id))),
                },
            );
            nodes.insert(
                node,
                Node {
                    id: node,
                    up: true,
                    disk: DiskId(id),
                    slice: Some(SliceId(id)),
                    reported: false,
                },
            );
            slices.insert(
                SliceId(id),
                Slice::new(SliceId(id), DeviceId(id), DiskId(id), node),
            );
        }
        // the put wrote every stripe whole, each chunk under the object's own label
        let positions: Vec<SliceId> = (0..width).map(|pos| SliceId(pos as u32)).collect();
        let put: Vec<Unit> = vec![Unit::Write(OpId(0)); layout.data_units()];
        for stripe in 0..params.stripes {
            for (pos, slice) in positions.iter().enumerate() {
                let pos = Pos(pos as u8);
                slices.get_mut(slice).expect("placed").chunks.insert(
                    StripeIx(stripe),
                    Chunk {
                        pos,
                        label: Label::PUT,
                        content: encode(layout, pos, &put),
                    },
                );
            }
        }
        let rows = (0..params.stripes)
            .map(|_| StripeGroup::new(layout, positions.clone()))
            .collect();
        let size = u32::from(params.stripes) * layout.data_units() as u32;
        let checker = StripeChecker::new(params);
        Self {
            params: params.clone(),
            policy,
            step: 0,
            net: Network::default(),
            entry: EntryGroup::new(size),
            rows,
            nodes,
            disks,
            slices,
            drivers: BTreeMap::new(),
            rounds: BTreeMap::new(),
            reservations: BTreeMap::new(),
            ledger: StripeLedger::default(),
            checker,
            progress: Progress::new(u64::from(params.calm_from)),
            violation: None,
            stalled: None,
            skipped: 0,
            next_id: (width + usize::from(params.spares)) as u32,
            outbox: Vec::new(),
            facts: Vec::new(),
        }
    }

    /// The pool's layout
    pub fn layout(&self) -> Layout {
        self.params.layout
    }

    /// Queue a message to be sent when this step ends
    ///
    /// # Arguments
    ///
    /// * `from` - The sender
    /// * `to` - The receiver
    /// * `body` - What it says
    pub fn send(&mut self, from: Endpoint, to: Endpoint, body: Body) {
        self.outbox.push(Message { from, to, body });
    }

    /// The endpoint an operation's driver speaks from in its current round
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    pub fn endpoint(&self, op: OpId) -> Endpoint {
        Endpoint::Op {
            op,
            round: self.rounds.get(&op).copied().unwrap_or(0),
        }
    }

    /// Whether a slice can answer: its node is up, its device is its own, and its disk answers
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    pub fn answers(&self, slice: SliceId) -> bool {
        let Some(s) = self.slices.get(&slice) else {
            return false;
        };
        let up = self.nodes.get(&s.node).is_some_and(|node| node.up);
        up && !s.gone && self.disk_of(slice).is_some_and(|disk| disk.healthy)
    }

    /// The disk a slice's bytes are on
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    pub fn disk_of(&self, slice: SliceId) -> Option<&Disk> {
        self.slices
            .get(&slice)
            .and_then(|s| self.disks.get(&s.disk))
    }

    /// Whether a coordinator believes a slice is up: its node runs and its disk is not reported
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    pub fn believed_up(&self, slice: SliceId) -> bool {
        let Some(s) = self.slices.get(&slice) else {
            return false;
        };
        !s.gone
            && self
                .nodes
                .get(&s.node)
                .is_some_and(|node| node.up && !node.reported)
    }

    /// Apply one event
    ///
    /// Returns the violation it caused, if any. Once a violation has been found the world is
    /// frozen and every further event is ignored.
    ///
    /// # Arguments
    ///
    /// * `event` - What happens
    pub fn apply(&mut self, event: &StripeEvent) -> Option<Violation> {
        if self.violation.is_some() {
            return None;
        }
        let fits = match event {
            StripeEvent::Write { op, stripe, units } => self.invoke_write(*op, *stripe, units),
            StripeEvent::Retry { op } => self.retry(*op),
            StripeEvent::Truncate { op, len } => self.invoke_truncate(*op, *len),
            StripeEvent::Read { op, stripe, strong } => self.invoke_read(*op, *stripe, *strong),
            StripeEvent::ClientTimeout { op } => self.client_timeout(*op),
            StripeEvent::StagerTimeout { op } => self.stager_timeout(*op),
            StripeEvent::DriverCrash { op } => self.driver_crash(*op),
            StripeEvent::Deliver { msg } => self.deliver(msg, None),
            StripeEvent::DeliverLagging { msg, lag } => self.deliver(msg, Some(*lag)),
            StripeEvent::Sync { slice } => self.sync(*slice),
            StripeEvent::ApplyStep { slice, stripe } => self.apply_step(*slice, *stripe),
            StripeEvent::Ask { slice, stripe } => self.ask(*slice, *stripe),
            StripeEvent::Crash { node } => self.crash(*node),
            StripeEvent::Restart { node } => self.restart(*node),
            StripeEvent::DiskFail { node } => self.disk_fail(*node),
            StripeEvent::DiskReport { node } => self.disk_report(*node),
            StripeEvent::DiskReplace { node } => self.disk_replace(*node),
            StripeEvent::Fill { node } => self.fill(*node, true),
            StripeEvent::Free { node } => self.fill(*node, false),
            StripeEvent::Noop { stripe } => self.noop(*stripe),
            StripeEvent::Rebuild { op, stripe, pos } => {
                self.invoke_rebuild(*op, *stripe, *pos, None)
            }
            StripeEvent::Move {
                op,
                stripe,
                pos,
                to,
            } => self.invoke_rebuild(*op, *stripe, *pos, Some(*to)),
            StripeEvent::Reclaim { op } => self.invoke_reclaim(*op),
            StripeEvent::ReservationLapse { stripe } => self.lapse(*stripe),
        };
        if !fits {
            self.skipped += 1;
        }
        self.finish_step()
    }

    /// Route what the step sent, run the checks, and advance the step
    fn finish_step(&mut self) -> Option<Violation> {
        // the messages go out
        for msg in std::mem::take(&mut self.outbox) {
            self.net.sent.push(msg.clone());
            self.net.in_flight.push(msg);
        }
        // the checker reads the whole world, so it is taken out while it does
        let mut checker = std::mem::take(&mut self.checker);
        for (slice, fact) in std::mem::take(&mut self.facts) {
            if self.violation.is_none() {
                if let Some(mut violation) = checker.on_fact(slice, &fact, self) {
                    violation.step = self.step;
                    self.violation = Some(violation);
                }
            }
        }
        if self.violation.is_none() {
            if let Some(mut violation) = checker.after_step(self) {
                violation.step = self.step;
                self.violation = Some(violation);
            }
        }
        self.checker = checker;
        self.step += 1;
        self.violation.clone()
    }

    /// A message arrives
    ///
    /// # Arguments
    ///
    /// * `msg` - The message
    /// * `lag` - For a read of a group, how far behind the replica answering it is
    fn deliver(&mut self, msg: &Message, lag: Option<u8>) -> bool {
        // a message never sent cannot arrive
        if !self.net.sent.contains(msg) {
            return false;
        }
        // one already delivered arriving again is a duplicate, which is allowed
        if !self.net.drop_message(msg) {
            self.checker.coverage.duplicates += 1;
        }
        // only a read of a group can be answered by a replica that lags
        if lag.is_some()
            && !matches!(
                msg.body,
                Body::RowRead { strong: false } | Body::EntryRead { strong: false }
            )
        {
            return false;
        }
        match msg.to {
            Endpoint::Entry => self.entry_receive(msg, lag),
            Endpoint::Row(stripe) => self.row_receive(stripe, msg, lag),
            Endpoint::Slice(slice) => self.slice_receive(slice, msg),
            Endpoint::Op { op, round } => {
                // an answer to an earlier round, or to a driver that is gone, is dropped
                if self.rounds.get(&op).copied().unwrap_or(0) != round
                    || !self.drivers.contains_key(&op)
                {
                    return true;
                }
                self.driver_receive(op, msg);
                true
            }
        }
    }

    /// The entry's group receives a message
    ///
    /// # Arguments
    ///
    /// * `msg` - The message
    /// * `lag` - How far behind the replica answering a read is
    fn entry_receive(&mut self, msg: &Message, lag: Option<u8>) -> bool {
        match &msg.body {
            Body::EntryRead { .. } => {
                if lag.is_some() {
                    self.checker.coverage.lagging_answers += 1;
                }
                let (index, state) = match lag {
                    Some(lag) => self.entry.lagging(lag),
                    None => self.entry.latest(),
                };
                let state = state.clone();
                self.send(msg.to, msg.from, Body::EntryAnswer { index, state });
                true
            }
            Body::EntryPropose(cmd) => {
                let before = self.entry.history.len();
                let decision = self.entry.propose(cmd);
                if self.entry.history.len() > before {
                    let index = (self.entry.history.len() - 1) as u32;
                    let mut checker = std::mem::take(&mut self.checker);
                    checker.on_entry_commit(index, cmd, self);
                    self.checker = checker;
                }
                self.send(msg.to, msg.from, Body::Decided(decision));
                true
            }
            _ => false,
        }
    }

    /// A stripe's group receives a message
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    /// * `msg` - The message
    /// * `lag` - How far behind the replica answering a read is
    fn row_receive(&mut self, stripe: StripeIx, msg: &Message, lag: Option<u8>) -> bool {
        let Some(group) = self.rows.get(usize::from(stripe.0)) else {
            return false;
        };
        match &msg.body {
            Body::RowRead { .. } => {
                if lag.is_some() {
                    self.checker.coverage.lagging_answers += 1;
                }
                let (index, state) = match lag {
                    Some(lag) => group.lagging(lag),
                    None => group.latest(),
                };
                let state = state.clone();
                // a retried write is recognised by its identity
                let written = match msg.from {
                    Endpoint::Op { op, .. } => group.written.contains_key(&op),
                    _ => false,
                };
                self.send(
                    msg.to,
                    msg.from,
                    Body::RowAnswer {
                        stripe,
                        index,
                        state,
                        written,
                    },
                );
                true
            }
            Body::Propose(cmd) => {
                let decision = self.commit(stripe, cmd);
                self.send(msg.to, msg.from, Body::Decided(decision));
                true
            }
            Body::Reserve { .. } => {
                // the leader knows a writer by its request identity, whatever round it asks from
                let same = |endpoint: &Endpoint| match (endpoint, &msg.from) {
                    (Endpoint::Op { op: held, .. }, Endpoint::Op { op: asking, .. }) => {
                        held == asking
                    }
                    (held, asking) => held == asking,
                };
                // the leader grants the turn to the first who asks, and queues the rest; a holder
                // asking again from a new round keeps its turn rather than queueing behind itself
                let reservations = self.reservations.entry(stripe).or_default();
                if reservations.holder.as_ref().is_none_or(same) {
                    reservations.holder = Some(msg.from);
                    self.send(msg.to, msg.from, Body::Granted { stripe });
                } else if let Some(queued) = reservations.queue.iter_mut().find(|e| same(e)) {
                    *queued = msg.from;
                } else {
                    reservations.queue.push_back(msg.from);
                }
                true
            }
            _ => false,
        }
    }

    /// Commit a command to a stripe's group, and let the checker see it
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    /// * `cmd` - The command
    pub fn commit(
        &mut self,
        stripe: StripeIx,
        cmd: &crate::stripe::group::StripeCommand,
    ) -> crate::stripe::group::Decision {
        let policy = self.policy;
        let group = &mut self.rows[usize::from(stripe.0)];
        let before = group.history.len();
        let decision = group.propose(cmd, &policy);
        if group.history.len() > before {
            let index = (group.history.len() - 1) as u32;
            self.checker.coverage.commits += 1;
            let mut checker = std::mem::take(&mut self.checker);
            if let Some(mut violation) = checker.on_stripe_commit(stripe, index, cmd, self) {
                if self.violation.is_none() {
                    violation.step = self.step;
                    self.violation = Some(violation);
                }
            }
            self.checker = checker;
        } else if decision == crate::stripe::group::Decision::Refused {
            self.checker.coverage.refusals += 1;
        }
        decision
    }

    /// A slice receives a message
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    /// * `msg` - The message
    fn slice_receive(&mut self, slice: SliceId, msg: &Message) -> bool {
        let Some(s) = self.slices.get(&slice) else {
            return false;
        };
        // a slice whose node is down, or that is gone, fails what is asked of it at once, as a
        // refused connection or a node that no longer serves it would; what tells it is lost
        let up = self.nodes.get(&s.node).is_some_and(|node| node.up);
        let healthy = up && !s.gone && self.disk_of(slice).is_some_and(|disk| disk.healthy);
        if !up || s.gone {
            let me = Endpoint::Slice(slice);
            let refusal = match &msg.body {
                Body::Stage {
                    stripe, pos, label, ..
                } => Some(Body::StageAnswer {
                    stripe: *stripe,
                    pos: *pos,
                    label: *label,
                    refused: Some(crate::stripe::event::StageRefusal::Failed),
                }),
                Body::Confirm { stripe, pos, label } => Some(Body::ConfirmAnswer {
                    stripe: *stripe,
                    pos: *pos,
                    label: *label,
                    holds: false,
                    unreachable: true,
                }),
                Body::ChunkRead { stripe, pos, label } => Some(Body::ChunkAnswer {
                    stripe: *stripe,
                    pos: *pos,
                    label: *label,
                    reply: crate::stripe::event::ChunkReply::Nothing,
                }),
                _ => None,
            };
            if let Some(body) = refusal {
                self.send(me, msg.from, body);
            }
            return true;
        }
        let full = self.disk_of(slice).is_some_and(|disk| disk.full);
        let policy = self.policy;
        let me = Endpoint::Slice(slice);
        match &msg.body {
            Body::Stage {
                stripe,
                pos,
                label,
                base_exists,
                base,
                expects,
                payload,
            } => {
                // a failed disk refuses everything
                if !healthy {
                    self.send(
                        me,
                        msg.from,
                        Body::StageAnswer {
                            stripe: *stripe,
                            pos: *pos,
                            label: *label,
                            refused: Some(crate::stripe::event::StageRefusal::Failed),
                        },
                    );
                    return true;
                }
                let staged = crate::stripe::holder::Staged {
                    pos: *pos,
                    label: *label,
                    base_exists: *base_exists,
                    base: *base,
                    expects: *expects,
                    record: crate::stripe::holder::Record::Units(Vec::new()),
                    reserved: false,
                };
                let answer = self.slices.get_mut(&slice).expect("checked").stage(
                    *stripe,
                    staged,
                    payload.clone(),
                    full,
                    msg.from,
                    &policy,
                );
                if let Some(refused) = answer {
                    self.send(
                        me,
                        msg.from,
                        Body::StageAnswer {
                            stripe: *stripe,
                            pos: *pos,
                            label: *label,
                            refused,
                        },
                    );
                }
                true
            }
            Body::Confirm { stripe, pos, label } => {
                let holds = healthy && self.slices[&slice].holds(*stripe, *pos, *label);
                self.send(
                    me,
                    msg.from,
                    Body::ConfirmAnswer {
                        stripe: *stripe,
                        pos: *pos,
                        label: *label,
                        holds,
                        unreachable: false,
                    },
                );
                true
            }
            Body::ChunkRead { stripe, pos, label } => {
                let reply = if healthy {
                    // the reader's label comes from a committed row: a staged write it names is committed
                    let s = self.slices.get_mut(&slice).expect("checked");
                    s.learn_named(*stripe, *label, &policy);
                    s.read(*stripe, *pos, *label, &policy)
                } else {
                    crate::stripe::event::ChunkReply::Nothing
                };
                self.send(
                    me,
                    msg.from,
                    Body::ChunkAnswer {
                        stripe: *stripe,
                        pos: *pos,
                        label: *label,
                        reply,
                    },
                );
                true
            }
            Body::Apply { stripe, label } => {
                if healthy {
                    self.slices
                        .get_mut(&slice)
                        .expect("checked")
                        .learn_named(*stripe, *label, &policy);
                }
                true
            }
            Body::Drop { stripe, label } => {
                let facts = self
                    .slices
                    .get_mut(&slice)
                    .expect("checked")
                    .drop_label(*stripe, *label, &policy);
                self.facts
                    .extend(facts.into_iter().map(|fact| (slice, fact)));
                true
            }
            Body::Discard { stripe, through } => {
                let facts = self
                    .slices
                    .get_mut(&slice)
                    .expect("checked")
                    .discard_stripe(*stripe, *through);
                self.checker.coverage.discards += facts.len() as u32;
                self.facts
                    .extend(facts.into_iter().map(|fact| (slice, fact)));
                true
            }
            Body::RowAnswer { stripe, state, .. } => {
                if healthy {
                    let facts = self
                        .slices
                        .get_mut(&slice)
                        .expect("checked")
                        .learn_row(*stripe, state, &policy);
                    self.checker.coverage.discards += facts.len() as u32;
                    self.facts
                        .extend(facts.into_iter().map(|fact| (slice, fact)));
                }
                true
            }
            _ => false,
        }
    }

    /// A slice syncs what it staged and answers the stages
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    fn sync(&mut self, slice: SliceId) -> bool {
        if !self.answers(slice) || self.slices[&slice].unsynced.is_empty() {
            return false;
        }
        let policy = self.policy;
        let answers = self.slices.get_mut(&slice).expect("checked").sync(&policy);
        for (to, stripe, pos, label) in answers {
            self.send(
                Endpoint::Slice(slice),
                to,
                Body::StageAnswer {
                    stripe,
                    pos,
                    label,
                    refused: None,
                },
            );
        }
        true
    }

    /// A slice takes one step of an apply
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    /// * `stripe` - The stripe
    fn apply_step(&mut self, slice: SliceId, stripe: StripeIx) -> bool {
        if !self.answers(slice) {
            return false;
        }
        let full = self.disk_of(slice).is_some_and(|disk| disk.full);
        let policy = self.policy;
        match self
            .slices
            .get_mut(&slice)
            .expect("checked")
            .apply_step(stripe, full, &policy)
        {
            Some(facts) => {
                self.facts
                    .extend(facts.into_iter().map(|fact| (slice, fact)));
                true
            }
            None => false,
        }
    }

    /// A slice holding a staged write asks the stripe's group about it
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    /// * `stripe` - The stripe
    fn ask(&mut self, slice: SliceId, stripe: StripeIx) -> bool {
        if !self.answers(slice) || !self.slices[&slice].holds_staged(stripe) {
            return false;
        }
        self.send(
            Endpoint::Slice(slice),
            Endpoint::Row(stripe),
            Body::RowRead { strong: false },
        );
        true
    }

    /// A node crashes
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    fn crash(&mut self, node: NodeId) -> bool {
        let Some(n) = self.nodes.get_mut(&node) else {
            return false;
        };
        if !n.up {
            return false;
        }
        n.up = false;
        let served = n.slice;
        if let Some(slice) = served {
            if let Some(s) = self.slices.get_mut(&slice) {
                if s.applying
                    .values()
                    .any(|a| a.phase == crate::stripe::holder::Phase::Writing)
                {
                    self.checker.coverage.torn_applies += 1;
                }
                s.crash();
            }
        }
        self.checker.coverage.crashes += 1;
        true
    }

    /// A node restarts and claims what is at its device's path
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    fn restart(&mut self, node: NodeId) -> bool {
        let Some(n) = self.nodes.get(&node) else {
            return false;
        };
        if n.up {
            return false;
        }
        let disk = n.disk;
        let old = n.slice;
        let marker = self.disks.get(&disk).and_then(|d| d.marker);
        let served = match marker {
            // a marked disk is the device it says it is
            Some((_, slice)) => slice,
            // an empty one is a new device with a new slice, unless known by its path alone
            None => match (self.policy.identity, old) {
                (DeviceIdentity::PathOnly, Some(old)) => {
                    // POLICY P7, P17: the unsafe setting takes the empty disk for the old device
                    let device = self.slices[&old].device;
                    let s = self.slices.get_mut(&old).expect("served before");
                    s.wipe();
                    s.gone = false;
                    s.disk = disk;
                    self.disks.get_mut(&disk).expect("in its bay").marker = Some((device, old));
                    old
                }
                _ => {
                    let id = self.next_id;
                    self.next_id += 1;
                    let slice = SliceId(id);
                    self.slices
                        .insert(slice, Slice::new(slice, DeviceId(id), disk, node));
                    self.disks.get_mut(&disk).expect("in its bay").marker =
                        Some((DeviceId(id), slice));
                    if let Some(old) = old {
                        if let Some(s) = self.slices.get_mut(&old) {
                            s.gone = true;
                        }
                    }
                    slice
                }
            },
        };
        let n = self.nodes.get_mut(&node).expect("checked");
        n.up = true;
        n.slice = Some(served);
        // a fresh disk has nothing reported against it
        if marker.is_none() {
            n.reported = false;
        }
        let facts = self.slices.get_mut(&served).expect("claimed").recover();
        self.checker.coverage.replays += facts.len() as u32;
        self.checker.coverage.restarts += 1;
        self.facts
            .extend(facts.into_iter().map(|fact| (served, fact)));
        true
    }

    /// A node's disk fails silently
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    fn disk_fail(&mut self, node: NodeId) -> bool {
        let Some(disk) = self.nodes.get(&node).map(|n| n.disk) else {
            return false;
        };
        let d = self.disks.get_mut(&disk).expect("in its bay");
        if !d.healthy {
            return false;
        }
        d.healthy = false;
        self.checker.coverage.disk_failures += 1;
        self.checker.losses += 1;
        true
    }

    /// A node reports its disk failed
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    fn disk_report(&mut self, node: NodeId) -> bool {
        let Some(n) = self.nodes.get(&node) else {
            return false;
        };
        let healthy = self.disks.get(&n.disk).is_some_and(|d| d.healthy);
        if !n.up || healthy || n.reported {
            return false;
        }
        self.nodes.get_mut(&node).expect("checked").reported = true;
        true
    }

    /// A down node's disk is swapped for an empty one at the same path
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    fn disk_replace(&mut self, node: NodeId) -> bool {
        let Some(n) = self.nodes.get(&node) else {
            return false;
        };
        if n.up {
            return false;
        }
        let old_disk = n.disk;
        let id = self.next_id;
        self.next_id += 1;
        self.disks.insert(
            DiskId(id),
            Disk {
                id: DiskId(id),
                healthy: true,
                full: false,
                marker: None,
            },
        );
        let served = n.slice;
        self.nodes.get_mut(&node).expect("checked").disk = DiskId(id);
        // whatever was on the old disk is gone with it
        if let Some(slice) = served {
            if let Some(s) = self.slices.get_mut(&slice) {
                if s.disk == old_disk {
                    s.wipe();
                    s.gone = true;
                }
            }
        }
        if self.disks.get(&old_disk).is_some_and(|d| d.healthy) {
            self.checker.losses += 1;
        }
        self.disks
            .get_mut(&old_disk)
            .expect("was in the bay")
            .healthy = false;
        self.checker.coverage.disk_replacements += 1;
        true
    }

    /// A node's disk fills, or gets its space back
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    /// * `full` - Whether it is now full
    fn fill(&mut self, node: NodeId, full: bool) -> bool {
        let Some(disk) = self.nodes.get(&node).map(|n| n.disk) else {
            return false;
        };
        let d = self.disks.get_mut(&disk).expect("in its bay");
        if d.full == full {
            return false;
        }
        d.full = full;
        if full {
            self.checker.coverage.fills += 1;
        }
        true
    }

    /// The leader's timer: a no-op that moves the sequence and changes no label
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    fn noop(&mut self, stripe: StripeIx) -> bool {
        let Some(group) = self.rows.get(usize::from(stripe.0)) else {
            return false;
        };
        let base = group.latest().1.seq;
        let cmd = crate::stripe::group::StripeCommand::Noop { base };
        let decision = self.commit(stripe, &cmd);
        if matches!(decision, crate::stripe::group::Decision::Committed { .. }) {
            self.checker.coverage.noops += 1;
        }
        true
    }

    /// The leader's reservation of a stripe lapses, and the next in line is granted
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    pub fn lapse(&mut self, stripe: StripeIx) -> bool {
        let Some(reservations) = self.reservations.get_mut(&stripe) else {
            return false;
        };
        if reservations.holder.is_none() {
            return false;
        }
        reservations.holder = reservations.queue.pop_front();
        if let Some(next) = reservations.holder {
            self.send(Endpoint::Row(stripe), next, Body::Granted { stripe });
        }
        true
    }

    /// Release a reservation an operation holds or waits for, when it finishes
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    /// * `op` - The operation
    pub fn release(&mut self, stripe: StripeIx, op: OpId) {
        let Some(reservations) = self.reservations.get_mut(&stripe) else {
            return;
        };
        reservations
            .queue
            .retain(|endpoint| !matches!(endpoint, Endpoint::Op { op: o, .. } if *o == op));
        let holds = matches!(reservations.holder, Some(Endpoint::Op { op: o, .. }) if o == op);
        if holds {
            self.lapse(stripe);
        }
    }

    /// Record an operation's outcome, and let the checker judge an acknowledgement
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `outcome` - What it was told
    pub fn complete(&mut self, op: OpId, outcome: StripeOutcome) {
        let fresh = self.ledger.complete(op, self.step, outcome.clone());
        if !fresh {
            return;
        }
        let mut checker = std::mem::take(&mut self.checker);
        if let Some(mut violation) = checker.on_complete(op, &outcome, self) {
            if self.violation.is_none() {
                violation.step = self.step;
                self.violation = Some(violation);
            }
        }
        self.checker = checker;
        self.progress.on_complete(op, &self.ledger, self.step);
    }

    /// The kind of an operation, as the client asked for it
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    pub fn kind_of(&self, op: OpId) -> Option<OpKind> {
        self.ledger.kind_of(op)
    }

    /// Replay a schedule from a fresh world and judge what it produced
    ///
    /// # Arguments
    ///
    /// * `schedule` - The schedule
    pub fn replay(schedule: &StripeSchedule) -> StripeOutcomeOfRun {
        Self::replay_world(schedule).finish()
    }

    /// Replay a schedule and hand back the world it left, for a test to inspect
    ///
    /// # Arguments
    ///
    /// * `schedule` - The schedule
    pub fn replay_world(schedule: &StripeSchedule) -> StripeWorld {
        let mut world = StripeWorld::new(&schedule.params, schedule.policy);
        for event in &schedule.events {
            if world.apply(event).is_some() {
                break;
            }
        }
        world
    }

    /// End the run: close the ledger, judge every read against the whole history, and the bounds
    pub fn finish(mut self) -> StripeOutcomeOfRun {
        self.ledger.finish(self.step);
        if self.violation.is_none() {
            let checker = std::mem::take(&mut self.checker);
            if let Some(mut violation) = checker.judge_history(&self) {
                violation.step = self.step;
                self.violation = Some(violation);
            }
            self.checker = checker;
        }
        if self.stalled.is_none() {
            self.stalled = self.progress.judge(&self.ledger, self.step, &self.params);
        }
        StripeOutcomeOfRun {
            violation: self.violation.clone(),
            stalled: self.stalled.clone(),
            ledger: self.ledger.clone(),
            coverage: self.checker.coverage,
            progress: self.progress.clone(),
            skipped: self.skipped,
            steps: self.step,
        }
    }
}
