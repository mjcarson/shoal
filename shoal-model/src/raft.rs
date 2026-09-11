//! The replication protocol, one group per tablet per node
//!
//! A Raft-shaped protocol written against the contract's positions: appended, durable,
//! committed, applied and checkpointed are five different things here, and an entry only
//! becomes durable through an explicit storage completion. The [`Policy`] is read in exactly
//! six places, each marked, and in every one of them the first variant is the contract and the
//! other is a violation the checker exists to catch.
//!
//! The protocol is the one the contract describes, not a particular library's. Election is by
//! the log-matching restriction (Raft 5.4.1); commitment needs a majority of the configured
//! voters to have fsynced a current-term entry (5.4.2, with durability made explicit); a
//! follower applies only what the leader has committed; and a leader's own log counts once,
//! after its own fsync, like anyone else's.

use std::collections::{BTreeMap, BTreeSet};

use crate::event::{Actor, Body, ClientOp, Effect, Event, Input, Message, Output};
use crate::ids::{Attempt, LogIndex, NodeId, TabletId, Term};
use crate::oracle::Outcome;
use crate::policy::{AckTiming, AsyncReceipt, DuplicateAck, Election, Policy, QuorumRule, Visibility};
use crate::storage::{AppliedState, Checkpoint, Command, Entry, StableStorage, Volatile};

/// The committed voter configuration, which is what a quorum is counted over
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// The voters
    pub voters: BTreeSet<NodeId>,
    /// The learners, who replicate but never count
    pub learners: BTreeSet<NodeId>,
}

impl Config {
    /// Whether a count of voters is a majority of the configuration
    ///
    /// # Arguments
    ///
    /// * `count` - How many distinct voters
    pub fn is_majority(&self, count: usize) -> bool {
        count * 2 > self.voters.len()
    }

    /// Every node, voters and learners, in order
    pub fn all(&self) -> Vec<NodeId> {
        self.voters.iter().chain(self.learners.iter()).copied().collect()
    }
}

/// Whether a node is running
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    /// Running
    Up,
    /// Stopped without losing anything; inputs queue until it resumes
    Paused {
        /// Everything that arrived while paused, in arrival order
        inbox: Vec<Input>,
    },
    /// Down, with only stable storage left
    Crashed,
}

/// What a group is doing in its term
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    /// Following whoever leads
    Follower,
    /// Asking for votes
    Candidate {
        /// The voters who granted so far, self included
        votes: BTreeSet<NodeId>,
    },
    /// Leading
    Leader(LeaderState),
}

/// What a leader tracks about its followers
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeaderState {
    /// The next index to send each node
    pub next_index: BTreeMap<NodeId, LogIndex>,
    /// The highest index each node has vouched for as durable and matching; a watermark
    pub durable_match: BTreeMap<NodeId, LogIndex>,
    /// The highest index each node has merely received; never evidence under the contract
    pub received_match: BTreeMap<NodeId, LogIndex>,
    /// Every durable acknowledgement delivered, in order; read only by the unsafe duplicate knob
    pub ack_tally: Vec<(NodeId, LogIndex)>,
    /// The attempts waiting on each index
    pub pending: BTreeMap<LogIndex, Vec<Attempt>>,
}

/// One tablet's replica on one node
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Group {
    /// Which tablet
    pub tablet: TabletId,
    /// What survives a crash
    pub stable: StableStorage,
    /// What does not
    pub volatile: Volatile,
    /// What this replica is doing in its term
    pub role: Role,
    /// Who last sent this replica entries in its term, so it knows where to send acks
    pub leader_hint: Option<NodeId>,
}

/// One node: its status and its groups
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    /// Which node
    pub id: NodeId,
    /// Whether it is running
    pub status: Status,
    /// Whether it is a voter in the configuration
    pub is_voter: bool,
    /// Whether its disk acknowledges before fsync, the way an `Async` setting would
    pub async_disk: bool,
    /// Its replica of every tablet
    pub groups: BTreeMap<TabletId, Group>,
    /// The observer's last up list, which the contract says never changes a quorum
    pub up_view: BTreeSet<NodeId>,
}

/// What a group handler needs to know about the node around it
struct Ctx<'a> {
    /// The node's id
    me: NodeId,
    /// The configuration
    cfg: &'a Config,
    /// The policy
    policy: &'a Policy,
    /// Whether the node's disk acknowledges early
    async_disk: bool,
    /// The node's view of who is up
    up_view: &'a BTreeSet<NodeId>,
}

impl Node {
    /// Build a node with an empty replica of every tablet
    ///
    /// # Arguments
    ///
    /// * `id` - Its id
    /// * `tablets` - The tablets it replicates
    /// * `is_voter` - Whether it votes
    /// * `async_disk` - Whether its disk acknowledges before fsync
    pub fn new(id: NodeId, tablets: &[TabletId], is_voter: bool, async_disk: bool) -> Self {
        Self {
            id,
            status: Status::Up,
            is_voter,
            async_disk,
            groups: tablets
                .iter()
                .map(|tablet| {
                    (
                        *tablet,
                        Group {
                            tablet: *tablet,
                            stable: StableStorage::default(),
                            volatile: Volatile::default(),
                            role: Role::Follower,
                            leader_hint: None,
                        },
                    )
                })
                .collect(),
            up_view: BTreeSet::new(),
        }
    }

    /// Whether this node is up
    pub fn is_up(&self) -> bool {
        matches!(self.status, Status::Up)
    }

    /// Handle one input
    ///
    /// A paused node queues it; a crashed node drops it; an up node dispatches it to the group
    /// it names.
    ///
    /// # Arguments
    ///
    /// * `input` - What arrived
    /// * `cfg` - The configuration
    /// * `policy` - The policy
    pub fn step(&mut self, input: Input, cfg: &Config, policy: &Policy) -> Vec<Output> {
        // a paused node hears nothing until it resumes; a crashed one never
        match &mut self.status {
            Status::Paused { inbox } => {
                inbox.push(input);
                return Vec::new();
            }
            Status::Crashed => return Vec::new(),
            Status::Up => {}
        }
        let mut out = Vec::new();
        // the up list is node level, not group level
        if let Input::Message(Message {
            body: Body::UpList { up },
            ..
        }) = &input
        {
            self.up_view = up.iter().copied().collect();
            return out;
        }
        let ctx = Ctx {
            me: self.id,
            cfg,
            policy,
            async_disk: self.async_disk,
            up_view: &self.up_view,
        };
        let is_voter = self.is_voter;
        match input {
            Input::Message(msg) => {
                // a message names its tablet
                if let Some(group) = self.groups.get_mut(&msg.tablet) {
                    group.on_message(&ctx, msg, &mut out);
                }
            }
            Input::Local(event) => match event {
                Event::ClientInvoke {
                    attempt,
                    tablet,
                    op,
                    ..
                } => {
                    if let Some(group) = self.groups.get_mut(&tablet) {
                        group.on_client(&ctx, attempt, op, &mut out);
                    }
                }
                Event::ElectionTimeout { tablet, .. } => {
                    if let Some(group) = self.groups.get_mut(&tablet) {
                        group.on_election_timeout(&ctx, is_voter, &mut out);
                    }
                }
                Event::HeartbeatTick { tablet, .. } => {
                    if let Some(group) = self.groups.get_mut(&tablet) {
                        group.on_heartbeat(&ctx, &mut out);
                    }
                }
                Event::Report { tablet, .. } => {
                    if let Some(group) = self.groups.get_mut(&tablet) {
                        group.on_report(&ctx, &mut out);
                    }
                }
                Event::StorageComplete { tablet, .. } => {
                    if let Some(group) = self.groups.get_mut(&tablet) {
                        group.on_storage_complete(&ctx, &mut out);
                    }
                }
                Event::Checkpoint { tablet, .. } => {
                    if let Some(group) = self.groups.get_mut(&tablet) {
                        group.on_checkpoint(&ctx, &mut out);
                    }
                }
                // the rest are handled by the world, not by a node
                _ => {}
            },
        }
        out
    }

    /// Lose everything but stable storage
    pub fn crash(&mut self) -> Vec<Output> {
        self.status = Status::Crashed;
        // every group keeps its stable storage and nothing else
        for group in self.groups.values_mut() {
            group.volatile = Volatile::default();
            group.role = Role::Follower;
            group.leader_hint = None;
        }
        self.up_view.clear();
        vec![Output::Effect(Effect::Crashed { node: self.id })]
    }

    /// Come back from stable storage
    pub fn restart(&mut self) -> Vec<Output> {
        self.status = Status::Up;
        // applied state comes from the checkpoint, if there is one, and the log is not
        // re-applied until a leader says how far it is committed
        for group in self.groups.values_mut() {
            group.volatile = Volatile::default();
            group.role = Role::Follower;
            group.leader_hint = None;
            if let Some(checkpoint) = &group.stable.checkpoint {
                group.volatile.applied = checkpoint.state.clone();
                group.volatile.commit_index = checkpoint.last_included.0;
            }
        }
        vec![Output::Effect(Effect::Restarted { node: self.id })]
    }

    /// Stop running without losing anything
    pub fn pause(&mut self) -> Vec<Output> {
        self.status = Status::Paused { inbox: Vec::new() };
        vec![Output::Effect(Effect::Paused { node: self.id })]
    }

    /// Run again, handling everything that queued in arrival order
    ///
    /// # Arguments
    ///
    /// * `cfg` - The configuration
    /// * `policy` - The policy
    pub fn resume(&mut self, cfg: &Config, policy: &Policy) -> Vec<Output> {
        // take the queue before flipping the status, so the drain below is handled as up
        let inbox = match std::mem::replace(&mut self.status, Status::Up) {
            Status::Paused { inbox } => inbox,
            _ => Vec::new(),
        };
        let mut out = vec![Output::Effect(Effect::Resumed { node: self.id })];
        for input in inbox {
            out.extend(self.step(input, cfg, policy));
        }
        out
    }
}

impl Group {
    /// How many entries the log holds, durable and appended together
    pub fn len(&self) -> LogIndex {
        LogIndex((self.stable.log.len() + self.volatile.appended.len()) as u64)
    }

    /// Whether the log is empty
    pub fn is_empty(&self) -> bool {
        self.len() == LogIndex(0)
    }

    /// The durable prefix's length
    pub fn durable_len(&self) -> LogIndex {
        LogIndex(self.stable.log.len() as u64)
    }

    /// The entry at an index, durable or appended
    ///
    /// # Arguments
    ///
    /// * `index` - The index, from one
    pub fn entry(&self, index: LogIndex) -> Option<&Entry> {
        // zero names no entry
        if index.0 == 0 {
            return None;
        }
        let position = (index.0 - 1) as usize;
        let durable = self.stable.log.len();
        if position < durable {
            self.stable.log.get(position)
        } else {
            self.volatile.appended.get(position - durable)
        }
    }

    /// The last entry's index and term, zero for an empty log
    pub fn last(&self) -> (LogIndex, Term) {
        let last = self.len();
        let term = self.entry(last).map_or(Term(0), |entry| entry.term);
        (last, term)
    }

    /// Whether this replica leads
    pub fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader(_))
    }

    /// Move to a higher term as a follower
    ///
    /// # Arguments
    ///
    /// * `ctx` - The node around this group
    /// * `term` - The new term
    /// * `out` - Where effects go
    fn bump_term(&mut self, ctx: &Ctx<'_>, term: Term, out: &mut Vec<Output>) {
        // a leader in the old term is one no longer
        if self.is_leader() {
            out.push(Output::Effect(Effect::SteppedDown {
                node: ctx.me,
                tablet: self.tablet,
                term: self.stable.term,
            }));
        }
        // term and vote are persisted before anything is sent
        self.stable.term = term;
        self.stable.voted_for = None;
        self.role = Role::Follower;
        self.volatile.match_bound = LogIndex(0);
        self.leader_hint = None;
    }

    /// Build a message from this replica
    fn message(&self, ctx: &Ctx<'_>, to: Actor, body: Body) -> Message {
        Message {
            from: Actor::Node(ctx.me),
            to,
            tablet: self.tablet,
            term: self.stable.term,
            body,
        }
    }

    /// Dispatch a message by its body
    fn on_message(&mut self, ctx: &Ctx<'_>, msg: Message, out: &mut Vec<Output>) {
        let from = match msg.from {
            Actor::Node(node) => node,
            // the observer's messages are the only ones without a node behind them
            Actor::Observer => {
                if let Body::Promote { term } = msg.body {
                    self.on_promote(ctx, term, out);
                }
                return;
            }
        };
        match msg.body {
            Body::RequestVote {
                last_index,
                last_term,
            } => self.on_request_vote(ctx, from, msg.term, last_index, last_term, out),
            Body::VoteResponse { granted } => self.on_vote_response(ctx, from, msg.term, granted, out),
            Body::AppendEntries {
                prev_index,
                prev_term,
                entries,
                leader_commit,
            } => self.on_append_entries(
                ctx,
                from,
                msg.term,
                prev_index,
                prev_term,
                entries,
                leader_commit,
                out,
            ),
            Body::AppendResponse {
                success,
                durable_to,
                durable,
                conflict_hint,
            } => self.on_append_response(
                ctx,
                from,
                msg.term,
                success,
                durable_to,
                durable,
                conflict_hint,
                out,
            ),
            // reports go to the observer, up lists are handled by the node
            Body::ProgressReport { .. } | Body::UpList { .. } | Body::Promote { .. } => {}
        }
    }

    /// The election timer fired: become a candidate, or lead outright in a group of one
    fn on_election_timeout(&mut self, ctx: &Ctx<'_>, is_voter: bool, out: &mut Vec<Output>) {
        // learners never stand, and a leader has nothing to run for
        if !is_voter || self.is_leader() {
            return;
        }
        // a new term, voted for ourselves, persisted before the requests go out
        self.stable.term = Term(self.stable.term.0 + 1);
        self.stable.voted_for = Some(ctx.me);
        self.volatile.match_bound = LogIndex(0);
        self.leader_hint = None;
        let mut votes = BTreeSet::new();
        votes.insert(ctx.me);
        // alone, that is already a majority
        if ctx.cfg.is_majority(votes.len()) {
            self.role = Role::Candidate { votes };
            self.become_leader(ctx, false, out);
            return;
        }
        self.role = Role::Candidate { votes };
        // ask every other voter, naming our last entry so they can apply the restriction
        let (last_index, last_term) = self.last();
        for voter in ctx.cfg.voters.iter().filter(|voter| **voter != ctx.me) {
            out.push(Output::Send(self.message(
                ctx,
                Actor::Node(*voter),
                Body::RequestVote {
                    last_index,
                    last_term,
                },
            )));
        }
    }

    /// A candidate asked for our vote
    fn on_request_vote(
        &mut self,
        ctx: &Ctx<'_>,
        from: NodeId,
        term: Term,
        last_index: LogIndex,
        last_term: Term,
        out: &mut Vec<Output>,
    ) {
        // an old term is refused with ours
        if term < self.stable.term {
            out.push(Output::Send(self.message(
                ctx,
                Actor::Node(from),
                Body::VoteResponse { granted: false },
            )));
            return;
        }
        // a newer term moves us on, whatever we were doing
        if term > self.stable.term {
            self.bump_term(ctx, term, out);
        }
        // the election restriction: the candidate's log must be at least as up to date as ours
        let (my_index, my_term) = self.last();
        let up_to_date = last_term > my_term || (last_term == my_term && last_index >= my_index);
        // one vote per term, and only for a candidate who could preserve what we hold
        let free = self.stable.voted_for.map_or(true, |voted| voted == from);
        let granted = free && up_to_date;
        if granted {
            // persisted before the reply
            self.stable.voted_for = Some(from);
        }
        out.push(Output::Send(self.message(
            ctx,
            Actor::Node(from),
            Body::VoteResponse { granted },
        )));
    }

    /// A voter answered
    fn on_vote_response(
        &mut self,
        ctx: &Ctx<'_>,
        from: NodeId,
        term: Term,
        granted: bool,
        out: &mut Vec<Output>,
    ) {
        // a newer term ends our candidacy
        if term > self.stable.term {
            self.bump_term(ctx, term, out);
            return;
        }
        // only a current candidacy counts votes, and only from voters
        let won = match &mut self.role {
            Role::Candidate { votes } if term == self.stable.term => {
                if granted && ctx.cfg.voters.contains(&from) {
                    votes.insert(from);
                }
                ctx.cfg.is_majority(votes.len())
            }
            _ => false,
        };
        if won {
            self.become_leader(ctx, false, out);
        }
    }

    /// Start leading: a fresh leader state, a noop of our term, and a first replication
    ///
    /// # Arguments
    ///
    /// * `ctx` - The node around this group
    /// * `by_promotion` - Whether the observer appointed us rather than an election
    /// * `out` - Where messages and effects go
    fn become_leader(&mut self, ctx: &Ctx<'_>, by_promotion: bool, out: &mut Vec<Output>) {
        let (last, _) = self.last();
        let mut state = LeaderState::default();
        // every follower is assumed caught up until it says otherwise
        for node in ctx.cfg.all() {
            state.next_index.insert(node, last.next());
        }
        self.role = Role::Leader(state);
        self.leader_hint = Some(ctx.me);
        out.push(Output::Effect(Effect::BecameLeader {
            node: ctx.me,
            tablet: self.tablet,
            term: self.stable.term,
            by_promotion,
            last_index: last,
        }));
        // the noop is what lets earlier terms' entries be committed under ours
        self.append_local(ctx, Command::Noop, out);
        // and everyone hears about it
        self.send_append_to_all(ctx, out);
    }

    /// Append an entry to our own log as leader
    ///
    /// # Arguments
    ///
    /// * `ctx` - The node around this group
    /// * `command` - What the entry carries
    /// * `out` - Where effects go
    fn append_local(&mut self, ctx: &Ctx<'_>, command: Command, out: &mut Vec<Output>) -> LogIndex {
        let index = self.len().next();
        self.volatile.appended.push(Entry {
            tablet: self.tablet,
            index,
            term: self.stable.term,
            command,
        });
        // the fsync of this entry is a separate event
        self.volatile.pending_fsync.push_back(index);
        out.push(Output::Effect(Effect::Appended {
            node: ctx.me,
            tablet: self.tablet,
            from: index,
            to: index,
        }));
        // POLICY P1: the contract counts our own copy only once its fsync has completed; the
        // unsafe setting counts it the moment it is appended
        if ctx.policy.ack_timing == AckTiming::OnReceipt {
            if let Role::Leader(state) = &mut self.role {
                state.durable_match.insert(ctx.me, index);
            }
            out.push(Output::Effect(Effect::DurableClaim {
                node: ctx.me,
                tablet: self.tablet,
                through: index,
            }));
        }
        index
    }

    /// Replicate to every other node
    fn send_append_to_all(&self, ctx: &Ctx<'_>, out: &mut Vec<Output>) {
        for node in ctx.cfg.all() {
            if node != ctx.me {
                self.send_append(ctx, node, out);
            }
        }
    }

    /// Replicate to one node, from where we believe it is
    ///
    /// # Arguments
    ///
    /// * `ctx` - The node around this group
    /// * `to` - The follower
    /// * `out` - Where the message goes
    fn send_append(&self, ctx: &Ctx<'_>, to: NodeId, out: &mut Vec<Output>) {
        let Role::Leader(state) = &self.role else {
            return;
        };
        // everything from the follower's next index, after the entry it should already hold
        let next = state.next_index.get(&to).copied().unwrap_or(LogIndex(1));
        let prev_index = next.prev();
        let prev_term = self.entry(prev_index).map_or(Term(0), |entry| entry.term);
        let entries = (next.0..=self.len().0)
            .filter_map(|index| self.entry(LogIndex(index)).cloned())
            .collect();
        out.push(Output::Send(self.message(
            ctx,
            Actor::Node(to),
            Body::AppendEntries {
                prev_index,
                prev_term,
                entries,
                leader_commit: self.volatile.commit_index,
            },
        )));
    }

    /// A client sent an operation
    fn on_client(&mut self, ctx: &Ctx<'_>, attempt: Attempt, op: ClientOp, out: &mut Vec<Output>) {
        match op {
            ClientOp::Mutate(op) => {
                // only a leader orders mutations; anyone else refuses outright
                if !self.is_leader() {
                    out.push(Output::Client {
                        attempt,
                        outcome: Outcome::Rejected,
                    });
                    return;
                }
                // an identity already applied returns what it returned the first time (C5)
                if let Some(result) = self.volatile.applied.results.get(&attempt.id) {
                    out.push(Output::Client {
                        attempt,
                        outcome: Outcome::Ok(*result),
                    });
                    return;
                }
                // an identity already in flight waits on the same entry
                if let Role::Leader(state) = &mut self.role {
                    let parked = state
                        .pending
                        .iter_mut()
                        .find(|(_, attempts)| attempts.iter().any(|a| a.id == attempt.id));
                    if let Some((_, attempts)) = parked {
                        attempts.push(attempt);
                        return;
                    }
                }
                // otherwise it is a new entry, and the client waits on its index
                let index = self.append_local(ctx, Command::Client { attempt, op }, out);
                if let Role::Leader(state) = &mut self.role {
                    state.pending.entry(index).or_default().push(attempt);
                }
                self.send_append_to_all(ctx, out);
                // our own copy may already count, under the unsafe timing
                self.try_advance_commit(ctx, out);
            }
            ClientOp::Read { key } => {
                // POLICY P4: the contract answers from the committed, applied prefix; the unsafe
                // setting answers from the appended suffix too
                let (value, observed) = match ctx.policy.visibility {
                    Visibility::CommittedApplied => (
                        self.volatile.applied.rows.get(&key).copied(),
                        self.volatile.applied.last_applied,
                    ),
                    Visibility::AppendedSuffix => {
                        let state = self.speculative_state();
                        (state.rows.get(&key).copied(), state.last_applied)
                    }
                };
                out.push(Output::Effect(Effect::Read {
                    node: ctx.me,
                    tablet: self.tablet,
                    key,
                    observed,
                }));
                out.push(Output::Client {
                    attempt,
                    outcome: Outcome::Ok(crate::event::OpResult::Value(value)),
                });
            }
        }
    }

    /// The applied state plus every appended entry, committed or not
    ///
    /// Only the unsafe visibility setting reads this.
    fn speculative_state(&self) -> AppliedState {
        let mut state = self.volatile.applied.clone();
        for index in (state.last_applied.0 + 1)..=self.len().0 {
            if let Some(entry) = self.entry(LogIndex(index)) {
                state.apply(entry);
            }
        }
        state
    }

    /// A leader sent entries, or a heartbeat
    #[allow(clippy::too_many_arguments)]
    fn on_append_entries(
        &mut self,
        ctx: &Ctx<'_>,
        from: NodeId,
        term: Term,
        prev_index: LogIndex,
        prev_term: Term,
        entries: Vec<Entry>,
        leader_commit: LogIndex,
        out: &mut Vec<Output>,
    ) {
        // an old leader is refused with our term, and learns it from the reply
        if term < self.stable.term {
            out.push(Output::Send(self.message(
                ctx,
                Actor::Node(from),
                Body::AppendResponse {
                    success: false,
                    durable_to: LogIndex(0),
                    durable: true,
                    conflict_hint: LogIndex(0),
                },
            )));
            return;
        }
        // a newer term moves us on
        if term > self.stable.term {
            self.bump_term(ctx, term, out);
        }
        // a leader in our term means we are not one, whatever we thought
        if !matches!(self.role, Role::Follower) {
            if self.is_leader() {
                out.push(Output::Effect(Effect::SteppedDown {
                    node: ctx.me,
                    tablet: self.tablet,
                    term: self.stable.term,
                }));
            }
            self.role = Role::Follower;
        }
        self.leader_hint = Some(from);
        // log matching: the entry before the new ones must be the one the leader thinks it is
        let matches = prev_index.0 == 0
            || self.entry(prev_index).map(|entry| entry.term) == Some(prev_term);
        if !matches {
            let hint = LogIndex(prev_index.0.min(self.len().0 + 1));
            out.push(Output::Send(self.message(
                ctx,
                Actor::Node(from),
                Body::AppendResponse {
                    success: false,
                    durable_to: LogIndex(0),
                    durable: true,
                    conflict_hint: hint,
                },
            )));
            return;
        }
        // everything through the last entry sent now matches the leader, held already or not
        let matched = LogIndex(prev_index.0 + entries.len() as u64);
        // take the entries: skip what we hold, truncate what conflicts, append the rest
        let mut new_from = None;
        let mut new_to = None;
        for entry in entries {
            match self.entry(entry.index) {
                // already ours
                Some(existing) if existing.term == entry.term => continue,
                // a conflict: everything from here is the old leader's, and goes
                Some(_) => {
                    let removed = self.truncate_from(entry.index);
                    out.push(Output::Effect(Effect::Truncated {
                        node: ctx.me,
                        tablet: self.tablet,
                        from: entry.index,
                        removed,
                    }));
                }
                None => {}
            }
            new_from.get_or_insert(entry.index);
            new_to = Some(entry.index);
            self.volatile.appended.push(entry);
        }
        if let (Some(from_index), Some(to_index)) = (new_from, new_to) {
            // one fsync for the batch
            self.volatile.pending_fsync.push_back(to_index);
            out.push(Output::Effect(Effect::Appended {
                node: ctx.me,
                tablet: self.tablet,
                from: from_index,
                to: to_index,
            }));
        }
        // and the bound only ever grows within a term
        self.volatile.match_bound = self.volatile.match_bound.max(matched);
        // commit what the leader has, as far as we match it
        let commit = leader_commit.min(self.volatile.match_bound);
        if commit > self.volatile.commit_index {
            self.volatile.commit_index = commit;
            self.apply_committed(ctx, out);
        }
        // the reply: at once for a heartbeat, after the fsync for new entries
        let durable_now = self.durable_len().min(self.volatile.match_bound);
        if new_to.is_none() {
            // a cumulative acknowledgement of what is already durable and matching
            out.push(Output::Send(self.message(
                ctx,
                Actor::Node(from),
                Body::AppendResponse {
                    success: true,
                    durable_to: durable_now,
                    durable: true,
                    conflict_hint: LogIndex(0),
                },
            )));
            out.push(Output::Effect(Effect::DurableClaim {
                node: ctx.me,
                tablet: self.tablet,
                through: durable_now,
            }));
            return;
        }
        // POLICY P1: the contract vouches for new entries only after their fsync; the unsafe
        // setting vouches on receipt
        if ctx.policy.ack_timing == AckTiming::OnReceipt {
            out.push(Output::Send(self.message(
                ctx,
                Actor::Node(from),
                Body::AppendResponse {
                    success: true,
                    durable_to: self.volatile.match_bound,
                    durable: true,
                    conflict_hint: LogIndex(0),
                },
            )));
            out.push(Output::Effect(Effect::DurableClaim {
                node: ctx.me,
                tablet: self.tablet,
                through: self.volatile.match_bound,
            }));
            return;
        }
        // an Async disk sends a receipt now, which the contract says is worth nothing
        if ctx.async_disk {
            out.push(Output::Send(self.message(
                ctx,
                Actor::Node(from),
                Body::AppendResponse {
                    success: true,
                    durable_to: self.volatile.match_bound,
                    durable: false,
                    conflict_hint: LogIndex(0),
                },
            )));
        }
    }

    /// Remove every entry from an index on, durable or not
    ///
    /// # Arguments
    ///
    /// * `from` - The first index to remove
    fn truncate_from(&mut self, from: LogIndex) -> Vec<(LogIndex, Term)> {
        let mut removed = Vec::new();
        // collect what goes, in order
        for index in from.0..=self.len().0 {
            if let Some(entry) = self.entry(LogIndex(index)) {
                removed.push((entry.index, entry.term));
            }
        }
        // then cut both halves of the log
        let durable = self.stable.log.len();
        let position = (from.0 - 1) as usize;
        if position < durable {
            self.stable.log.truncate(position);
            self.volatile.appended.clear();
        } else {
            self.volatile.appended.truncate(position - durable);
        }
        // a pending fsync cannot reach past what remains
        let len = self.len();
        for boundary in &mut self.volatile.pending_fsync {
            *boundary = (*boundary).min(len);
        }
        removed
    }

    /// The oldest pending fsync completed
    fn on_storage_complete(&mut self, ctx: &Ctx<'_>, out: &mut Vec<Output>) {
        // nothing pending means nothing completes
        let Some(through) = self.volatile.pending_fsync.pop_front() else {
            return;
        };
        // move the appended prefix through the boundary into the durable log
        while self
            .volatile
            .appended
            .first()
            .is_some_and(|entry| entry.index <= through)
        {
            let entry = self.volatile.appended.remove(0);
            self.stable.log.push(entry);
        }
        let durable = self.durable_len();
        out.push(Output::Effect(Effect::Fsynced {
            node: ctx.me,
            tablet: self.tablet,
            through: durable,
            at_term: self.stable.term,
        }));
        if self.is_leader() {
            // our own copy counts once, from here
            if let Role::Leader(state) = &mut self.role {
                let mine = state.durable_match.entry(ctx.me).or_default();
                *mine = (*mine).max(durable);
            }
            out.push(Output::Effect(Effect::DurableClaim {
                node: ctx.me,
                tablet: self.tablet,
                through: durable,
            }));
            self.try_advance_commit(ctx, out);
        } else if let Some(leader) = self.leader_hint {
            // tell the leader how far we are durable and matching
            let durable_to = durable.min(self.volatile.match_bound);
            out.push(Output::Send(self.message(
                ctx,
                Actor::Node(leader),
                Body::AppendResponse {
                    success: true,
                    durable_to,
                    durable: true,
                    conflict_hint: LogIndex(0),
                },
            )));
            out.push(Output::Effect(Effect::DurableClaim {
                node: ctx.me,
                tablet: self.tablet,
                through: durable_to,
            }));
        }
    }

    /// A follower answered a replication
    #[allow(clippy::too_many_arguments)]
    fn on_append_response(
        &mut self,
        ctx: &Ctx<'_>,
        from: NodeId,
        term: Term,
        success: bool,
        durable_to: LogIndex,
        durable: bool,
        conflict_hint: LogIndex,
        out: &mut Vec<Output>,
    ) {
        // a newer term ends our leadership
        if term > self.stable.term {
            self.bump_term(ctx, term, out);
            return;
        }
        // only a current leader reads acknowledgements
        if term != self.stable.term || !self.is_leader() {
            return;
        }
        // POLICY P3: the contract treats an Async receipt as no evidence; the unsafe setting
        // counts it like an fsync
        let counts = durable || ctx.policy.async_receipt == AsyncReceipt::CountsAsDurable;
        if let Role::Leader(state) = &mut self.role {
            if !success {
                // back up to where the follower says it diverged and try again
                state.next_index.insert(from, LogIndex(conflict_hint.0.max(1)));
            } else {
                if counts {
                    // a watermark: a repeated cumulative acknowledgement changes nothing
                    let mark = state.durable_match.entry(from).or_default();
                    *mark = (*mark).max(durable_to);
                    // the tally is what the unsafe duplicate knob reads
                    state.ack_tally.push((from, durable_to));
                } else {
                    let mark = state.received_match.entry(from).or_default();
                    *mark = (*mark).max(durable_to);
                }
                let next = state.next_index.entry(from).or_default();
                *next = (*next).max(durable_to.next());
            }
        }
        if !success {
            self.send_append(ctx, from, out);
            return;
        }
        self.try_advance_commit(ctx, out);
    }

    /// The evidence for an index being replicated, and the population it is judged against
    ///
    /// # Arguments
    ///
    /// * `ctx` - The node around this group
    /// * `index` - The index
    fn quorum_evidence(&self, ctx: &Ctx<'_>, index: LogIndex) -> (Vec<NodeId>, Vec<NodeId>) {
        let Role::Leader(state) = &self.role else {
            return (Vec::new(), Vec::new());
        };
        let vouched = |node: &NodeId| state.durable_match.get(node).is_some_and(|mark| *mark >= index);
        // POLICY P3: the contract counts the committed voter configuration; the unsafe setting
        // counts whoever the observer currently lists as up
        let over: Vec<NodeId> = match ctx.policy.quorum {
            QuorumRule::DurableDistinctVoters => ctx.cfg.voters.iter().copied().collect(),
            QuorumRule::CurrentUpList => ctx.up_view.iter().copied().collect(),
        };
        // POLICY P3: the contract counts each replica once, by its watermark; the unsafe setting
        // counts every acknowledgement it ever received
        let evidence = match ctx.policy.duplicate_ack {
            DuplicateAck::Watermark => over.iter().copied().filter(vouched).collect(),
            DuplicateAck::CountsAgain => {
                let mut tally: Vec<NodeId> = state
                    .ack_tally
                    .iter()
                    .filter(|(_, to)| *to >= index)
                    .map(|(node, _)| *node)
                    .collect();
                if vouched(&ctx.me) {
                    tally.push(ctx.me);
                }
                tally
            }
        };
        (evidence, over)
    }

    /// Advance the commit index as far as the evidence allows
    fn try_advance_commit(&mut self, ctx: &Ctx<'_>, out: &mut Vec<Output>) {
        if !self.is_leader() {
            return;
        }
        // from the newest entry down: the first one with a quorum commits everything before it
        let last = self.len();
        let mut index = last;
        while index > self.volatile.commit_index {
            // only an entry of our own term is committed by counting (Raft 5.4.2)
            let ours = self.entry(index).is_some_and(|entry| entry.term == self.stable.term);
            if ours {
                let (evidence, over) = self.quorum_evidence(ctx, index);
                if evidence.len() * 2 > over.len() {
                    self.volatile.commit_index = index;
                    out.push(Output::Effect(Effect::CommitAdvanced {
                        node: ctx.me,
                        tablet: self.tablet,
                        term: self.stable.term,
                        to: index,
                        evidence,
                        over,
                    }));
                    self.apply_committed(ctx, out);
                    return;
                }
            }
            index = index.prev();
        }
    }

    /// Apply everything committed and not yet applied, answering the clients waiting on it
    fn apply_committed(&mut self, _ctx: &Ctx<'_>, out: &mut Vec<Output>) {
        let from = self.volatile.applied.last_applied.next();
        let to = self.volatile.commit_index;
        for index in from.0..=to.0 {
            let index = LogIndex(index);
            // an index past our log is one a checkpoint covered, or a hole the leader will fill
            let Some(entry) = self.entry(index).cloned() else {
                break;
            };
            let answered = self.volatile.applied.apply(&entry);
            // the leader answers everyone parked on this index with the result it derived
            let Some((_, result)) = answered else {
                continue;
            };
            let waiting = match &mut self.role {
                Role::Leader(state) => state.pending.remove(&index).unwrap_or_default(),
                _ => Vec::new(),
            };
            for attempt in waiting {
                out.push(Output::Client {
                    attempt,
                    outcome: Outcome::Ok(result),
                });
                out.push(Output::Effect(Effect::ClientOk {
                    attempt,
                    tablet: self.tablet,
                    index,
                }));
            }
        }
    }

    /// The heartbeat timer fired
    fn on_heartbeat(&mut self, ctx: &Ctx<'_>, out: &mut Vec<Output>) {
        if self.is_leader() {
            self.send_append_to_all(ctx, out);
        }
    }

    /// Report our progress to the observer
    fn on_report(&mut self, ctx: &Ctx<'_>, out: &mut Vec<Output>) {
        let (last_index, last_term) = self.last();
        out.push(Output::Send(self.message(
            ctx,
            Actor::Observer,
            Body::ProgressReport {
                last_index,
                last_term,
                leader: self.is_leader(),
            },
        )));
    }

    /// Take a checkpoint
    fn on_checkpoint(&mut self, ctx: &Ctx<'_>, out: &mut Vec<Output>) {
        // POLICY P4: the contract checkpoints the committed, applied prefix; the unsafe setting
        // checkpoints the appended suffix too
        let state = match ctx.policy.visibility {
            Visibility::CommittedApplied => self.volatile.applied.clone(),
            Visibility::AppendedSuffix => self.speculative_state(),
        };
        let last = state.last_applied;
        let term = self.entry(last).map_or(Term(0), |entry| entry.term);
        self.stable.checkpoint = Some(Checkpoint {
            last_included: (last, term),
            state,
        });
        out.push(Output::Effect(Effect::Checkpointed {
            node: ctx.me,
            tablet: self.tablet,
            last_included: last,
        }));
    }

    /// The observer appointed us
    fn on_promote(&mut self, ctx: &Ctx<'_>, term: Term, out: &mut Vec<Output>) {
        // POLICY P5: the contract establishes a leader only by election; the unsafe setting lets
        // a topology edit do it
        if ctx.policy.election != Election::HeartbeatMaxReport {
            return;
        }
        if term > self.stable.term {
            self.bump_term(ctx, term, out);
        }
        self.become_leader(ctx, true, out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Actor, MutationOp, OpResult};
    use crate::ids::{Key, OpId, Value};
    use crate::schedule::{Builder, ScheduleParams};

    /// A three voter, one tablet world with nothing scheduled
    fn builder(policy: Policy) -> Builder {
        let params = ScheduleParams {
            steps: 0,
            ops: 0,
            ..ScheduleParams::default_small()
        };
        Builder::new("unit", &params, policy)
    }

    const T: TabletId = TabletId::new(1, 0);

    /// An election followed by a write commits on every replica, after every fsync
    #[test]
    fn three_nodes_elect_replicate_and_commit() {
        let mut b = builder(Policy::safe());
        b.elect(NodeId(1), T);
        assert!(b.world().group(NodeId(1), T).is_leader());
        b.write(MutationOp::Insert { key: Key(1), value: Value(5) }, T, NodeId(1));
        // appended everywhere, committed nowhere until the fsyncs complete
        b.deliver_all();
        assert_eq!(b.world().group(NodeId(1), T).volatile.commit_index, LogIndex(1));
        b.fsync_all();
        b.deliver_all();
        let leader = b.world().group(NodeId(1), T);
        assert_eq!(leader.volatile.commit_index, LogIndex(2));
        assert_eq!(leader.volatile.applied.rows.get(&Key(1)), Some(&Value(5)));
        // the client heard, with the result derived in committed order
        let record = &b.world().ledger.records[0];
        assert_eq!(record.outcome, Some(crate::oracle::Outcome::Ok(OpResult::Applied(true))));
        // followers apply once the heartbeat carries the commit index
        b.event(Event::HeartbeatTick { node: NodeId(1), tablet: T });
        b.deliver_all();
        for node in [NodeId(2), NodeId(3)] {
            assert_eq!(b.world().group(node, T).volatile.applied.rows.get(&Key(1)), Some(&Value(5)));
        }
        assert!(b.world().violation.is_none());
    }

    /// A conflicting suffix is truncated when a newer leader's entries arrive
    #[test]
    fn a_conflicting_suffix_is_truncated() {
        let mut b = builder(Policy::safe());
        b.elect(NodeId(1), T);
        // a write that reaches nobody, sitting appended on the leader alone
        b.write(MutationOp::Insert { key: Key(1), value: Value(1) }, T, NodeId(1));
        b.drop_from_to(Actor::Node(NodeId(1)), Actor::Node(NodeId(2)));
        b.drop_from_to(Actor::Node(NodeId(1)), Actor::Node(NodeId(3)));
        b.fsync(NodeId(1), T);
        assert_eq!(b.world().group(NodeId(1), T).len(), LogIndex(2));
        // node 1 pauses, node 2 is elected and writes in its term
        b.event(Event::Pause { node: NodeId(1) });
        b.elect(NodeId(2), T);
        b.write(MutationOp::Insert { key: Key(1), value: Value(2) }, T, NodeId(2));
        b.replicate_fully(NodeId(2), T);
        // node 1 comes back and hears the new leader: its lone entry goes
        b.event(Event::Resume { node: NodeId(1) });
        b.deliver_all();
        b.fsync_all();
        b.deliver_all();
        let one = b.world().group(NodeId(1), T);
        assert!(!one.is_leader());
        assert_eq!(one.entry(LogIndex(2)).map(|e| e.term), Some(Term(2)));
        assert!(b.world().checker.coverage.truncations >= 1);
        assert!(b.world().violation.is_none(), "{}", b.world().violation.clone().unwrap());
    }

    /// A repeated cumulative acknowledgement moves a watermark and adds no vote
    #[test]
    fn a_heartbeat_re_ack_is_a_watermark() {
        let mut b = builder(Policy::safe());
        b.elect(NodeId(1), T);
        b.write(MutationOp::Insert { key: Key(1), value: Value(1) }, T, NodeId(1));
        // only node 2 hears and fsyncs; its ack, delivered twice, is one voter
        b.deliver_from_to(Actor::Node(NodeId(1)), Actor::Node(NodeId(2)));
        b.drop_from_to(Actor::Node(NodeId(1)), Actor::Node(NodeId(3)));
        b.fsync(NodeId(2), T);
        b.deliver_from_to(Actor::Node(NodeId(2)), Actor::Node(NodeId(1)));
        b.redeliver_last(Actor::Node(NodeId(2)), Actor::Node(NodeId(1)));
        let leader = b.world().group(NodeId(1), T);
        let Role::Leader(state) = &leader.role else { panic!("not leading") };
        assert_eq!(state.durable_match.get(&NodeId(2)), Some(&LogIndex(2)));
        // the leader's own fsync is what makes two, and commits
        assert_eq!(leader.volatile.commit_index, LogIndex(1));
        b.fsync(NodeId(1), T);
        assert_eq!(b.world().group(NodeId(1), T).volatile.commit_index, LogIndex(2));
        assert!(b.world().violation.is_none());
    }

    /// A retry of an applied identity returns the original result without a second entry
    #[test]
    fn a_retry_returns_the_stored_result() {
        let mut b = builder(Policy::safe());
        b.elect(NodeId(1), T);
        b.write(MutationOp::Insert { key: Key(1), value: Value(1) }, T, NodeId(1));
        b.replicate_fully(NodeId(1), T);
        let len = b.world().group(NodeId(1), T).len();
        // the same identity again, after a delete that would let a fresh insert succeed
        b.write(MutationOp::Delete { key: Key(1) }, T, NodeId(1));
        b.replicate_fully(NodeId(1), T);
        b.event(Event::ClientInvoke {
            attempt: Attempt { id: OpId(0), retry: 1 },
            tablet: T,
            op: ClientOp::Mutate(MutationOp::Insert { key: Key(1), value: Value(1) }),
            target: NodeId(1),
        });
        assert_eq!(b.world().group(NodeId(1), T).len(), LogIndex(len.0 + 1), "the retry was appended");
        let retry = b.world().ledger.records.last().unwrap();
        assert_eq!(retry.outcome, Some(crate::oracle::Outcome::Ok(OpResult::Applied(true))));
        assert_eq!(b.world().group(NodeId(1), T).volatile.applied.rows.get(&Key(1)), None);
    }

    /// A crash loses the appended suffix and keeps the durable log; a restart is a follower
    #[test]
    fn a_crash_keeps_only_stable_storage() {
        let mut b = builder(Policy::safe());
        b.elect(NodeId(1), T);
        b.write(MutationOp::Insert { key: Key(1), value: Value(1) }, T, NodeId(1));
        let before = b.world().group(NodeId(1), T).clone();
        assert_eq!(before.volatile.appended.len(), 1);
        b.event(Event::Crash { node: NodeId(1) });
        b.event(Event::Restart { node: NodeId(1) });
        let after = b.world().group(NodeId(1), T);
        assert_eq!(after.stable.log, before.stable.log);
        assert!(after.volatile.appended.is_empty());
        assert!(!after.is_leader());
        assert_eq!(after.stable.term, before.stable.term);
    }
}
