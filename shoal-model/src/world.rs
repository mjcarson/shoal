//! The whole model: nodes, observer, network, ledger and checker, driven one event at a time
//!
//! `apply` is the only way the model moves, and it is total: an event that does not fit the
//! state - a delivery of a message never sent, a restart of a node that is up - is skipped
//! rather than refused. That is what makes every subsequence of a valid schedule a valid
//! schedule, which is what the minimizer relies on: a removed delivery is a dropped message, a
//! removed storage completion is a stalled disk, a removed crash is a node that stayed up.

use std::collections::BTreeMap;

use crate::event::{Actor, Event, Input, Message, Output};
use crate::ids::{Attempt, LogIndex, NodeId, TabletId};
use crate::invariants::{Checker, Coverage, Violation};
use crate::observer::Observer;
use crate::oracle::{self, Ledger, OracleError, Outcome};
use crate::policy::Policy;
use crate::raft::{Config, Node, Role, Status};
use crate::schedule::{Schedule, ScheduleParams};

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

/// The model
#[derive(Debug, Clone)]
pub struct World {
    /// What the schedule was built with
    pub params: ScheduleParams,
    /// The policy in force
    pub policy: Policy,
    /// The voter configuration
    pub cfg: Config,
    /// The nodes
    pub nodes: BTreeMap<NodeId, Node>,
    /// The control-plane observer
    pub observer: Observer,
    /// The network
    pub net: Network,
    /// The history
    pub ledger: Ledger,
    /// The checker
    pub checker: Checker,
    /// The step the next event will be applied at
    pub step: u64,
    /// The first violation found, which freezes the world
    pub violation: Option<Violation>,
    /// How many events did not fit the state and were skipped
    pub skipped: u32,
}

/// What replaying a schedule produced
#[derive(Debug, Clone)]
pub struct RunOutcome {
    /// The first violation, if any
    pub violation: Option<Violation>,
    /// The oracle's verdict on the history
    pub oracle: Result<(), OracleError>,
    /// The history
    pub ledger: Ledger,
    /// What the run exercised
    pub coverage: Coverage,
    /// How many events were skipped
    pub skipped: u32,
}

impl World {
    /// Build a fresh world
    ///
    /// # Arguments
    ///
    /// * `params` - The topology
    /// * `policy` - The policy
    pub fn new(params: &ScheduleParams, policy: Policy) -> Self {
        // nodes are numbered from one; every node not a learner votes
        let ids: Vec<NodeId> = (1..=params.nodes).map(NodeId).collect();
        let cfg = Config {
            voters: ids
                .iter()
                .filter(|id| !params.learners.contains(id))
                .copied()
                .collect(),
            learners: params.learners.iter().copied().collect(),
        };
        let nodes = ids
            .iter()
            .map(|id| {
                (
                    *id,
                    Node::new(
                        *id,
                        &params.tablets,
                        !params.learners.contains(id),
                        params.async_nodes.contains(id),
                    ),
                )
            })
            .collect();
        Self {
            params: params.clone(),
            policy,
            cfg,
            nodes,
            observer: Observer::default(),
            net: Network::default(),
            ledger: Ledger::default(),
            checker: Checker::default(),
            step: 0,
            violation: None,
            skipped: 0,
        }
    }

    /// Every node's id
    pub fn node_ids(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    /// Apply one event
    ///
    /// Returns the violation it caused, if any. Once a violation has been found the world is
    /// frozen and every further event is ignored.
    ///
    /// # Arguments
    ///
    /// * `event` - What happens
    pub fn apply(&mut self, event: &Event) -> Option<Violation> {
        if self.violation.is_some() {
            return None;
        }
        let mut outputs = Vec::new();
        match event {
            Event::Deliver { msg } => {
                // a message never sent cannot arrive
                if !self.net.sent.contains(msg) {
                    self.skipped += 1;
                    return self.finish_step(outputs);
                }
                // one already delivered arriving again is a duplicate, which is allowed
                if !self.net.drop_message(msg) {
                    self.checker.coverage.duplicates += 1;
                }
                self.checker.on_deliver(msg);
                match msg.to {
                    Actor::Node(node) => {
                        if let Some(node) = self.nodes.get_mut(&node) {
                            outputs = node.step(Input::Message(msg.clone()), &self.cfg, &self.policy);
                        }
                    }
                    Actor::Observer => {
                        let all = self.node_ids();
                        outputs = self.observer.on_report(msg, &all);
                    }
                }
            }
            Event::ClientInvoke {
                attempt,
                tablet,
                op,
                target,
            } => {
                if attempt.retry > 0 {
                    self.checker.coverage.retries += 1;
                }
                self.ledger.invoke(*attempt, *tablet, op.clone(), self.step);
                if let Some(node) = self.nodes.get_mut(target) {
                    outputs = node.step(Input::Local(event.clone()), &self.cfg, &self.policy);
                }
            }
            Event::ClientTimeout { attempt } => {
                // giving up is what makes an outcome unknown
                if self.ledger.complete(*attempt, self.step, Outcome::Unknown) {
                    self.checker.coverage.unknown_outcomes += 1;
                } else {
                    self.skipped += 1;
                }
            }
            Event::ElectionTimeout { node, .. }
            | Event::HeartbeatTick { node, .. }
            | Event::Report { node, .. }
            | Event::StorageComplete { node, .. }
            | Event::Checkpoint { node, .. } => {
                if let Some(node) = self.nodes.get_mut(node) {
                    outputs = node.step(Input::Local(event.clone()), &self.cfg, &self.policy);
                }
            }
            Event::Crash { node } => {
                outputs = self.transition(*node, |node| !matches!(node.status, Status::Crashed), Node::crash);
            }
            Event::Restart { node } => {
                outputs = self.transition(*node, |node| matches!(node.status, Status::Crashed), Node::restart);
            }
            Event::Pause { node } => {
                outputs = self.transition(*node, |node| matches!(node.status, Status::Up), Node::pause);
            }
            Event::Resume { node } => {
                let (cfg, policy) = (self.cfg.clone(), self.policy);
                outputs = self.transition(
                    *node,
                    |node| matches!(node.status, Status::Paused { .. }),
                    |node| node.resume(&cfg, &policy),
                );
            }
            Event::MarkDown { node } => {
                let all = self.node_ids();
                outputs = self
                    .observer
                    .on_mark_down(*node, &self.policy, &all, &self.params.tablets);
            }
        }
        self.finish_step(outputs)
    }

    /// Move a node between statuses, if the move is legal
    fn transition(
        &mut self,
        node: NodeId,
        legal: impl Fn(&Node) -> bool,
        go: impl FnOnce(&mut Node) -> Vec<Output>,
    ) -> Vec<Output> {
        match self.nodes.get_mut(&node) {
            Some(node) if legal(node) => go(node),
            _ => {
                self.skipped += 1;
                Vec::new()
            }
        }
    }

    /// Route a step's outputs, run the checks, and advance the step
    fn finish_step(&mut self, outputs: Vec<Output>) -> Option<Violation> {
        // the checker reads the whole world, so it is taken out while it does
        let mut checker = std::mem::take(&mut self.checker);
        for output in outputs {
            match output {
                Output::Send(msg) => {
                    self.net.sent.push(msg.clone());
                    self.net.in_flight.push(msg);
                }
                Output::Client { attempt, outcome } => {
                    // an answer to an attempt already given up on is ignored
                    self.ledger.complete(attempt, self.step, outcome);
                }
                Output::Effect(effect) => {
                    if self.violation.is_none() {
                        if let Some(mut violation) = checker.on_effect(&effect, self) {
                            violation.step = self.step;
                            self.violation = Some(violation);
                        }
                    }
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

    /// Replay a schedule from a fresh world and judge what it produced
    ///
    /// # Arguments
    ///
    /// * `schedule` - The schedule
    pub fn replay(schedule: &Schedule) -> RunOutcome {
        let mut world = World::new(&schedule.params, schedule.policy);
        for event in &schedule.events {
            if world.apply(event).is_some() {
                break;
            }
        }
        world.finish()
    }

    /// Replay a schedule and hand back the world it left, for a test to inspect
    ///
    /// # Arguments
    ///
    /// * `schedule` - The schedule
    pub fn replay_world(schedule: &Schedule) -> World {
        let mut world = World::new(&schedule.params, schedule.policy);
        for event in &schedule.events {
            if world.apply(event).is_some() {
                break;
            }
        }
        world
    }

    /// End the run: close the ledger and judge it
    pub fn finish(mut self) -> RunOutcome {
        self.ledger.finish(self.step);
        RunOutcome {
            violation: self.violation.clone(),
            oracle: oracle::check(&self.ledger),
            ledger: self.ledger.clone(),
            coverage: self.checker.coverage,
            skipped: self.skipped,
        }
    }

    /// The events that could happen next, by category, for the generator
    pub fn enabled(&self) -> Enabled {
        let mut enabled = Enabled::default();
        // deliveries and duplicates
        enabled.deliver = self
            .net
            .in_flight
            .iter()
            .map(|msg| Event::Deliver { msg: msg.clone() })
            .collect();
        let window = self.params.dup_window as usize;
        enabled.duplicate = self
            .net
            .sent
            .iter()
            .rev()
            .take(window)
            .map(|msg| Event::Deliver { msg: msg.clone() })
            .collect();
        // per node, per tablet
        for node in self.nodes.values() {
            let live = !matches!(node.status, Status::Crashed);
            let up = matches!(node.status, Status::Up);
            for (tablet, group) in &node.groups {
                if live && !group.volatile.pending_fsync.is_empty() {
                    enabled.fsync.push(Event::StorageComplete {
                        node: node.id,
                        tablet: *tablet,
                    });
                }
                if up && node.is_voter && !matches!(group.role, Role::Leader(_)) {
                    enabled.election.push(Event::ElectionTimeout {
                        node: node.id,
                        tablet: *tablet,
                    });
                }
                if up && matches!(group.role, Role::Leader(_)) {
                    enabled.heartbeat.push(Event::HeartbeatTick {
                        node: node.id,
                        tablet: *tablet,
                    });
                }
                if up {
                    enabled.report.push(Event::Report {
                        node: node.id,
                        tablet: *tablet,
                    });
                    enabled.checkpoint.push(Event::Checkpoint {
                        node: node.id,
                        tablet: *tablet,
                    });
                }
            }
        }
        // status changes, bounded by how many may be down at once
        let down = self
            .nodes
            .values()
            .filter(|node| !matches!(node.status, Status::Up))
            .count();
        let room = down < usize::from(self.params.max_down);
        for node in self.nodes.values() {
            match node.status {
                Status::Up => {
                    if room {
                        enabled.crash.push(Event::Crash { node: node.id });
                        enabled.pause.push(Event::Pause { node: node.id });
                    }
                    enabled.up.push(node.id);
                }
                Status::Paused { .. } => {
                    enabled.resume.push(Event::Resume { node: node.id });
                    enabled.crash.push(Event::Crash { node: node.id });
                }
                Status::Crashed => enabled.restart.push(Event::Restart { node: node.id }),
            }
        }
        for node in &self.observer.up {
            enabled.mark_down.push(Event::MarkDown { node: *node });
        }
        // client attempts
        for attempt in self.ledger.pending() {
            enabled.timeout.push(Event::ClientTimeout { attempt });
        }
        let retried: std::collections::BTreeSet<Attempt> =
            self.ledger.records.iter().map(|record| record.attempt).collect();
        for record in &self.ledger.records {
            let next = Attempt {
                id: record.attempt.id,
                retry: record.attempt.retry + 1,
            };
            let retryable = record.outcome == Some(Outcome::Unknown)
                && next.retry <= self.params.max_retries
                && !retried.contains(&next)
                && matches!(record.op, crate::event::ClientOp::Mutate(_));
            if retryable {
                enabled.retry.push(next);
            }
        }
        enabled
    }

    /// The last index of a node's tablet log, for a directed schedule to read
    pub fn last_index(&self, node: NodeId, tablet: TabletId) -> LogIndex {
        self.group(node, tablet).last().0
    }
}

/// What the generator can choose between, by category
#[derive(Debug, Clone, Default)]
pub struct Enabled {
    /// Messages in flight
    pub deliver: Vec<Event>,
    /// Recent messages, deliverable again
    pub duplicate: Vec<Event>,
    /// Pending fsyncs
    pub fsync: Vec<Event>,
    /// Election timers that could fire
    pub election: Vec<Event>,
    /// Heartbeat timers that could fire
    pub heartbeat: Vec<Event>,
    /// Reports that could be sent
    pub report: Vec<Event>,
    /// Attempts that could be given up on
    pub timeout: Vec<Event>,
    /// Attempts that could be retried
    pub retry: Vec<Attempt>,
    /// Nodes that could crash
    pub crash: Vec<Event>,
    /// Nodes that could restart
    pub restart: Vec<Event>,
    /// Nodes that could pause
    pub pause: Vec<Event>,
    /// Nodes that could resume
    pub resume: Vec<Event>,
    /// Checkpoints that could be taken
    pub checkpoint: Vec<Event>,
    /// Nodes the observer could mark down
    pub mark_down: Vec<Event>,
    /// Nodes that are up, for a client to target
    pub up: Vec<NodeId>,
}
