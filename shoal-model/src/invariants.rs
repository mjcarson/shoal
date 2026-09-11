//! The contract's properties, checked on every transition
//!
//! The checker never reads a node's opinion of what is committed, because the unsafe policies
//! corrupt exactly that. It computes commitment from durable facts alone - which entries are in
//! which stable logs, and what term each node held when each became durable - and judges every
//! effect against that ground truth. Each check names the `P` number it enforces, in its name and
//! in the detail of the violation it reports, which is what the M0 test
//! `protocol_model_preserves_acknowledged_history` requires of it.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::event::{Actor, Body, Effect, Message};
use crate::ids::{LogIndex, NodeId, TabletId, Term};
use crate::raft::{Node, Role, Status};
use crate::world::World;

/// The six clauses of the contract
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Property {
    /// The failure model: correctness under crashes, loss, duplication, reordering and pauses
    P1,
    /// Table-qualified stream identity with a logical, continuous index
    P2,
    /// Durable quorum: a majority of the committed voters, each counted once, after fsync
    P3,
    /// Committed visibility: reads and checkpoints see only the committed, applied prefix
    P4,
    /// Control/data authority split: only an election establishes a leader
    P5,
    /// No cross-tablet transaction promise
    P6,
}

impl fmt::Display for Property {
    /// The bare number, `P3`
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// A property broken at a step
///
/// The detail never contains the step, so that a minimized schedule reports the same failure at
/// an earlier step and `same_failure` can say so.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Violation {
    /// Which clause
    pub property: Property,
    /// The step of the schedule it was detected at
    pub step: u64,
    /// What happened, naming the nodes, indices and terms involved
    pub detail: String,
}

impl Violation {
    /// Whether two violations are the same failure, ignoring where in a schedule it was found
    ///
    /// # Arguments
    ///
    /// * `other` - The other violation
    pub fn same_failure(&self, other: &Violation) -> bool {
        self.property == other.property && self.detail == other.detail
    }
}

impl fmt::Display for Violation {
    /// `P5 at step 611: ...`
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at step {}: {}", self.property, self.step, self.detail)
    }
}

/// What a run exercised, so a run that exercised nothing cannot pass as evidence
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Coverage {
    /// Leaders established
    pub elections: u32,
    /// Log truncations after a conflict
    pub truncations: u32,
    /// Crashes
    pub crashes: u32,
    /// Pauses
    pub pauses: u32,
    /// Messages delivered a second time
    pub duplicates: u32,
    /// Commit advances
    pub commits: u32,
    /// Retried attempts
    pub retries: u32,
    /// Attempts whose outcome became unknown
    pub unknown_outcomes: u32,
}

impl Coverage {
    /// Add another run's counts to these
    ///
    /// # Arguments
    ///
    /// * `other` - The counts to add
    pub fn add(&mut self, other: &Coverage) {
        self.elections += other.elections;
        self.truncations += other.truncations;
        self.crashes += other.crashes;
        self.pauses += other.pauses;
        self.duplicates += other.duplicates;
        self.commits += other.commits;
        self.retries += other.retries;
        self.unknown_outcomes += other.unknown_outcomes;
    }
}

/// The ground truth and the checks over it
#[derive(Debug, Clone, Default)]
pub struct Checker {
    /// The committed log of each tablet, as (index, term), computed from durable facts only
    committed: BTreeMap<TabletId, Vec<(LogIndex, Term)>>,
    /// The term each node held when each entry of its stable log became durable
    fsync_terms: BTreeMap<(NodeId, TabletId), Vec<Term>>,
    /// Delivered durable acknowledgements: for a leader in a term, each voter's highest
    acks: BTreeMap<(TabletId, NodeId, Term), BTreeMap<NodeId, LogIndex>>,
    /// Delivered granted votes: for a candidate in a term, who granted
    votes: BTreeMap<(TabletId, NodeId, Term), BTreeSet<NodeId>>,
    /// What the run exercised
    pub coverage: Coverage,
}

impl Checker {
    /// The committed prefix of a tablet, as the checker knows it
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    pub fn committed_index(&self, tablet: TabletId) -> LogIndex {
        LogIndex(self.committed.get(&tablet).map_or(0, Vec::len) as u64)
    }

    /// A message is about to be delivered: record what it establishes
    ///
    /// # Arguments
    ///
    /// * `msg` - The message
    pub fn on_deliver(&mut self, msg: &Message) {
        let (Actor::Node(from), Actor::Node(to)) = (msg.from, msg.to) else {
            return;
        };
        match &msg.body {
            // a durable acknowledgement is evidence for the leader it was sent to, in its term
            Body::AppendResponse {
                success: true,
                durable_to,
                durable: true,
                ..
            } => {
                let mark = self
                    .acks
                    .entry((msg.tablet, to, msg.term))
                    .or_default()
                    .entry(from)
                    .or_default();
                *mark = (*mark).max(*durable_to);
            }
            // a granted vote is evidence for the candidate, in its term
            Body::VoteResponse { granted: true } => {
                self.votes
                    .entry((msg.tablet, to, msg.term))
                    .or_default()
                    .insert(from);
            }
            _ => {}
        }
    }

    /// An effect happened: update the ground truth, then check it
    ///
    /// # Arguments
    ///
    /// * `effect` - What a node did
    /// * `world` - The world after it did it
    pub fn on_effect(&mut self, effect: &Effect, world: &World) -> Option<Violation> {
        match effect {
            Effect::Fsynced {
                node,
                tablet,
                through,
                at_term,
            } => {
                // every newly durable entry was fsynced at this term
                let terms = self.fsync_terms.entry((*node, *tablet)).or_default();
                terms.truncate(through.0 as usize);
                while terms.len() < through.0 as usize {
                    terms.push(*at_term);
                }
                // and the committed prefix may have grown
                self.refresh_committed(world);
                None
            }
            Effect::Truncated {
                node,
                tablet,
                from,
                removed,
            } => {
                self.coverage.truncations += 1;
                // the durable record shrinks with the log
                if let Some(terms) = self.fsync_terms.get_mut(&(*node, *tablet)) {
                    terms.truncate((from.0 - 1) as usize);
                }
                // P1: a committed entry is never truncated; a conflicting entry at its index may be
                let committed = self.committed.get(tablet);
                for (index, term) in removed {
                    let hit = committed
                        .and_then(|log| log.get((index.0 - 1) as usize))
                        .is_some_and(|entry| entry == &(*index, *term));
                    if hit {
                        return Some(self.violation(
                            Property::P1,
                            format!(
                                "P1: node {} truncated committed entry {} (term {}) of tablet {tablet}",
                                node.0, index.0, term.0
                            ),
                        ));
                    }
                }
                None
            }
            Effect::DurableClaim {
                node,
                tablet,
                through,
            } => {
                // P1: a claim of durability is backed by a completed fsync
                let durable = world.group(*node, *tablet).durable_len();
                if *through > durable {
                    return Some(self.violation(
                        Property::P1,
                        format!(
                            "P1: node {} vouched for tablet {tablet} through {} with only {} durable",
                            node.0, through.0, durable.0
                        ),
                    ));
                }
                None
            }
            Effect::BecameLeader {
                node,
                tablet,
                term,
                by_promotion,
                last_index,
            } => {
                self.coverage.elections += 1;
                // P5: a leader has a majority of the voters behind it, by their votes
                let mut votes = self
                    .votes
                    .get(&(*tablet, *node, *term))
                    .cloned()
                    .unwrap_or_default();
                votes.insert(*node);
                let votes: BTreeSet<NodeId> =
                    votes.intersection(&world.cfg.voters).copied().collect();
                if !world.cfg.is_majority(votes.len()) {
                    let how = if *by_promotion { "by promotion" } else { "without a majority" };
                    return Some(self.violation(
                        Property::P5,
                        format!(
                            "P5: node {} became leader of tablet {tablet} in term {} {how} with votes {:?} of voters {:?}; its log ends at {}, the committed prefix ends at {}",
                            node.0,
                            term.0,
                            ids(&votes),
                            ids(&world.cfg.voters),
                            last_index.0,
                            self.committed_index(*tablet).0
                        ),
                    ));
                }
                // P1: a new leader holds every committed entry (leader completeness)
                let group = world.group(*node, *tablet);
                if let Some(log) = self.committed.get(tablet) {
                    for (index, term) in log {
                        let held = group.entry(*index).is_some_and(|entry| entry.term == *term);
                        if !held {
                            return Some(self.violation(
                                Property::P1,
                                format!(
                                    "P1: node {} became leader of tablet {tablet} without committed entry {} (term {})",
                                    node.0, index.0, term.0
                                ),
                            ));
                        }
                    }
                }
                None
            }
            Effect::Appended {
                node,
                tablet,
                from,
                to,
            } => {
                let group = world.group(*node, *tablet);
                // P2: every entry belongs to this stream, at a contiguous index
                for index in from.0..=to.0 {
                    let entry = group.entry(LogIndex(index));
                    let sound = entry.is_some_and(|entry| entry.tablet == *tablet && entry.index.0 == index);
                    if !sound {
                        return Some(self.violation(
                            Property::P2,
                            format!(
                                "P2: node {} holds an entry at {index} of tablet {tablet} that names another stream or index",
                                node.0
                            ),
                        ));
                    }
                }
                // P2: log matching - two logs agreeing at an index agree on everything before it
                for other in world.nodes.values().filter(|other| other.id != *node) {
                    let theirs = &other.groups[tablet];
                    for index in from.0..=to.0 {
                        let index = LogIndex(index);
                        let (Some(mine), Some(its)) = (group.entry(index), theirs.entry(index)) else {
                            continue;
                        };
                        if mine.term != its.term {
                            continue;
                        }
                        for earlier in 1..index.0 {
                            let earlier = LogIndex(earlier);
                            let mine = group.entry(earlier).map(|e| e.term);
                            let its = theirs.entry(earlier).map(|e| e.term);
                            if mine != its {
                                return Some(self.violation(
                                    Property::P2,
                                    format!(
                                        "P2: nodes {} and {} agree on entry {} of tablet {tablet} but differ at {}",
                                        node.0, other.id.0, index.0, earlier.0
                                    ),
                                ));
                            }
                        }
                    }
                }
                None
            }
            Effect::CommitAdvanced {
                node,
                tablet,
                term,
                to,
                evidence,
                over,
            } => {
                self.coverage.commits += 1;
                self.check_commit(world, *node, *tablet, *term, *to, evidence, over)
            }
            Effect::ClientOk {
                attempt,
                tablet,
                index,
            } => {
                // P3: a successful mutation is committed and applied before its client hears
                let committed = self.committed_index(*tablet);
                if *index > committed {
                    return Some(self.violation(
                        Property::P3,
                        format!(
                            "P3: attempt {}/{} was acknowledged at index {} of tablet {tablet} with only {} committed",
                            attempt.id.0, attempt.retry, index.0, committed.0
                        ),
                    ));
                }
                None
            }
            Effect::Read {
                node,
                tablet,
                key,
                observed,
            } => {
                // P4: a read sees a committed prefix, however stale
                let committed = self.committed_index(*tablet);
                if *observed > committed {
                    return Some(self.violation(
                        Property::P4,
                        format!(
                            "P4: node {} answered a read of key {} on tablet {tablet} from index {} with only {} committed",
                            node.0, key.0, observed.0, committed.0
                        ),
                    ));
                }
                None
            }
            Effect::Checkpointed {
                node,
                tablet,
                last_included,
            } => {
                // P4: a checkpoint holds only committed applied state
                let committed = self.committed_index(*tablet);
                if *last_included > committed {
                    return Some(self.violation(
                        Property::P4,
                        format!(
                            "P4: node {} checkpointed tablet {tablet} through index {} with only {} committed",
                            node.0, last_included.0, committed.0
                        ),
                    ));
                }
                None
            }
            Effect::Crashed { .. } => {
                self.coverage.crashes += 1;
                None
            }
            Effect::Paused { .. } => {
                self.coverage.pauses += 1;
                None
            }
            Effect::SteppedDown { .. } | Effect::Restarted { .. } | Effect::Resumed { .. } => None,
        }
    }

    /// P3: the evidence for a commit is a majority of distinct configured voters, each with a
    /// delivered durable acknowledgement, for an entry of the leader's own term
    #[allow(clippy::too_many_arguments)]
    fn check_commit(
        &self,
        world: &World,
        node: NodeId,
        tablet: TabletId,
        term: Term,
        to: LogIndex,
        evidence: &[NodeId],
        over: &[NodeId],
    ) -> Option<Violation> {
        let voters: Vec<NodeId> = world.cfg.voters.iter().copied().collect();
        // the population is the committed voter configuration, not the up list
        if over != voters.as_slice() {
            return Some(self.violation(
                Property::P3,
                format!(
                    "P3: node {} committed index {} of tablet {tablet} over the up list {:?} rather than the voters {:?}",
                    node.0, to.0, ids_slice(over), ids_slice(&voters)
                ),
            ));
        }
        // a replica counts once
        let mut seen = BTreeSet::new();
        for member in evidence {
            if !seen.insert(*member) {
                return Some(self.violation(
                    Property::P3,
                    format!(
                        "P3: node {} committed index {} of tablet {tablet} counting replica {} twice",
                        node.0, to.0, member.0
                    ),
                ));
            }
        }
        // and only a voter counts
        for member in evidence {
            if !world.cfg.voters.contains(member) {
                return Some(self.violation(
                    Property::P3,
                    format!(
                        "P3: node {} committed index {} of tablet {tablet} counting non-voter {}",
                        node.0, to.0, member.0
                    ),
                ));
            }
        }
        // each with a delivered durable acknowledgement, or the leader's own fsync
        for member in evidence {
            let vouched = if *member == node {
                world.group(node, tablet).durable_len() >= to
            } else {
                self.acks
                    .get(&(tablet, node, term))
                    .and_then(|acks| acks.get(member))
                    .is_some_and(|mark| *mark >= to)
            };
            if !vouched {
                return Some(self.violation(
                    Property::P3,
                    format!(
                        "P3: node {} committed index {} of tablet {tablet} counting replica {} without a delivered durable acknowledgement",
                        node.0, to.0, member.0
                    ),
                ));
            }
        }
        // for an entry of its own term
        let entry_term = world.group(node, tablet).entry(to).map(|entry| entry.term);
        if entry_term != Some(term) {
            return Some(self.violation(
                Property::P3,
                format!(
                    "P3: node {} committed index {} of tablet {tablet} by counting, though it is not of term {}",
                    node.0, to.0, term.0
                ),
            ));
        }
        // and a majority of them
        if !world.cfg.is_majority(evidence.len()) {
            return Some(self.violation(
                Property::P3,
                format!(
                    "P3: node {} committed index {} of tablet {tablet} with {} of {} voters",
                    node.0,
                    to.0,
                    evidence.len(),
                    voters.len()
                ),
            ));
        }
        None
    }

    /// The checks that hold at every step, whatever happened
    ///
    /// # Arguments
    ///
    /// * `world` - The world after the step
    pub fn after_step(&mut self, world: &World) -> Option<Violation> {
        self.refresh_committed(world);
        let mut leaders: BTreeMap<(TabletId, Term), NodeId> = BTreeMap::new();
        for node in world.nodes.values() {
            for (tablet, group) in &node.groups {
                // P2: indices are contiguous from one and every entry names this stream
                for (position, entry) in group
                    .stable
                    .log
                    .iter()
                    .chain(group.volatile.appended.iter())
                    .enumerate()
                {
                    if entry.index.0 != position as u64 + 1 || entry.tablet != *tablet {
                        return Some(self.violation(
                            Property::P2,
                            format!(
                                "P2: node {} holds entry {} of tablet {} at position {} of tablet {tablet}",
                                node.id.0,
                                entry.index.0,
                                entry.tablet,
                                position + 1
                            ),
                        ));
                    }
                }
                // P4: applied never runs ahead of committed, and committed never past the log
                // or the checkpoint that stands in for its prefix
                let checkpointed = group
                    .stable
                    .checkpoint
                    .as_ref()
                    .map_or(LogIndex(0), |checkpoint| checkpoint.last_included.0);
                let applied = group.volatile.applied.last_applied;
                let commit = group.volatile.commit_index;
                if applied > commit || commit > group.len().max(checkpointed) {
                    return Some(self.violation(
                        Property::P4,
                        format!(
                            "P4: node {} has applied {} and committed {} of tablet {tablet} with a log of {}",
                            node.id.0,
                            applied.0,
                            commit.0,
                            group.len().0
                        ),
                    ));
                }
                // P5: one leader per term, among the nodes that are not crashed
                if matches!(group.role, Role::Leader(_)) && !matches!(node.status, Status::Crashed) {
                    if let Some(other) = leaders.insert((*tablet, group.stable.term), node.id) {
                        return Some(self.violation(
                            Property::P5,
                            format!(
                                "P5: nodes {} and {} both lead tablet {tablet} in term {}",
                                other.0,
                                node.id.0,
                                group.stable.term.0
                            ),
                        ));
                    }
                }
            }
        }
        None
    }

    /// Recompute the committed prefix of every tablet from durable facts
    ///
    /// An entry is committed once a majority of the voters hold it durably and held its term
    /// when it became durable; and, with it, everything before it in those logs. The term
    /// condition is Raft's figure 8: an old-term entry on a majority is not yet safe, a
    /// current-term entry after it makes both safe. The prefix only ever grows.
    ///
    /// # Arguments
    ///
    /// * `world` - The world to read stable logs from
    fn refresh_committed(&mut self, world: &World) {
        for tablet in &world.params.tablets {
            let known = self.committed.entry(*tablet).or_default();
            // the longest durable log bounds the search
            let longest = world
                .nodes
                .values()
                .map(|node| node.groups[tablet].stable.log.len())
                .max()
                .unwrap_or(0);
            // from the top down: the highest index a majority fsynced at its own term
            let mut found: Option<(usize, NodeId)> = None;
            for index in (known.len() + 1..=longest).rev() {
                let mut holders: BTreeMap<Term, Vec<NodeId>> = BTreeMap::new();
                for voter in &world.cfg.voters {
                    let node = &world.nodes[voter];
                    let group = &node.groups[tablet];
                    let Some(entry) = group.stable.log.get(index - 1) else {
                        continue;
                    };
                    let fsync_term = self
                        .fsync_terms
                        .get(&(*voter, *tablet))
                        .and_then(|terms| terms.get(index - 1))
                        .copied();
                    if fsync_term == Some(entry.term) {
                        holders.entry(entry.term).or_default().push(*voter);
                    }
                }
                if let Some((_, members)) = holders
                    .iter()
                    .find(|(_, members)| world.cfg.is_majority(members.len()))
                {
                    found = Some((index, members[0]));
                    break;
                }
            }
            // extend through it from any holder: log matching says they agree on the prefix
            if let Some((index, holder)) = found {
                let log = &world.nodes[&holder].groups[tablet].stable.log;
                for position in known.len()..index {
                    let entry = &log[position];
                    known.push((entry.index, entry.term));
                }
            }
        }
    }

    /// A violation at the current step
    fn violation(&self, property: Property, detail: String) -> Violation {
        Violation {
            property,
            step: 0,
            detail,
        }
    }
}

/// Node ids as plain numbers, for a detail line
fn ids(nodes: &BTreeSet<NodeId>) -> Vec<u8> {
    nodes.iter().map(|node| node.0).collect()
}

/// Node ids as plain numbers, for a detail line
fn ids_slice(nodes: &[NodeId]) -> Vec<u8> {
    nodes.iter().map(|node| node.0).collect()
}

impl World {
    /// A node's replica of a tablet
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    /// * `tablet` - The tablet
    pub fn group(&self, node: NodeId, tablet: TabletId) -> &crate::raft::Group {
        &self.nodes[&node].groups[&tablet]
    }
}

/// Whether a node is not crashed, for the checks that look at live state
pub fn is_live(node: &Node) -> bool {
    !matches!(node.status, Status::Crashed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Actor, Event, MutationOp};
    use crate::ids::{Key, Value};
    use crate::policy::Policy;
    use crate::schedule::{Builder, ScheduleParams};

    const T: TabletId = TabletId::new(1, 0);

    /// Figure 8 of the Raft paper: an old-term entry on a majority is not committed until an
    /// entry of the current term is durable on a majority after it
    #[test]
    fn an_old_term_entry_on_a_majority_is_not_committed_until_a_current_one_is() {
        let params = ScheduleParams {
            nodes: 5,
            steps: 0,
            ops: 0,
            ..ScheduleParams::default_small()
        };
        let mut b = Builder::new("figure8", &params, Policy::safe());
        let (s1, s2, s3, s4, s5) = (NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5));
        // (a) S1 leads term 1 and its entry reaches S2 only
        b.elect(s1, T);
        b.write(MutationOp::Insert { key: Key(1), value: Value(1) }, T, s1);
        b.deliver_from_to(Actor::Node(s1), Actor::Node(s2));
        for node in [s3, s4, s5] {
            b.drop_from_to(Actor::Node(s1), Actor::Node(node));
        }
        b.fsync(s1, T);
        b.fsync(s2, T);
        b.drop_from_to(Actor::Node(s2), Actor::Node(s1));
        // (b) S1 crashes; S5 leads term 2 with S3 and S4's votes (S2 refuses: longer log).
        // only the votes travel: S5's noop must stay on S5 alone
        b.event(Event::Crash { node: s1 });
        b.event(Event::ElectionTimeout { node: s5, tablet: T });
        for node in [s2, s3, s4] {
            b.deliver_from_to(Actor::Node(s5), Actor::Node(node));
        }
        for node in [s2, s3, s4] {
            b.deliver_from_to(Actor::Node(node), Actor::Node(s5));
        }
        assert!(b.world().group(s5, T).is_leader());
        for node in [s2, s3, s4] {
            b.drop_from_to(Actor::Node(s5), Actor::Node(node));
        }
        // then S5 crashes and S1 returns
        b.fsync(s5, T);
        b.event(Event::Crash { node: s5 });
        b.event(Event::Restart { node: s1 });
        // S1 still holds term 1, so its first try lands in term 2, where S3 and S4 have voted;
        // the second lands in term 3 and wins with S2, S3 and S4
        b.event(Event::ElectionTimeout { node: s1, tablet: T });
        b.deliver_all();
        assert!(!b.world().group(s1, T).is_leader(), "S1 won term 2 against S5's voters");
        b.event(Event::ElectionTimeout { node: s1, tablet: T });
        b.deliver_all();
        assert!(b.world().group(s1, T).is_leader(), "S1 should lead term 3");
        assert_eq!(b.world().group(s1, T).stable.term, Term(3));
        // (c) S1 replicates the term-1 entry to S3, so it is durable on a majority
        b.deliver_all();
        b.fsync(s3, T);
        b.deliver_from_to(Actor::Node(s3), Actor::Node(s1));
        // index 1 is the noop `elect` replicated everywhere; index 2 is the figure's entry
        let committed = b.world().checker.committed_index(T);
        assert_eq!(committed, LogIndex(1), "the term-1 entry was counted committed too early");
        // (e) S1's own term-3 noop becomes durable on a majority: now everything before it is
        b.fsync_all();
        b.deliver_all();
        assert!(b.world().checker.committed_index(T) >= LogIndex(2));
        assert!(b.world().violation.is_none(), "{}", b.world().violation.clone().unwrap());
    }

    /// A violation's identity is its property and detail, not the step it was found at
    #[test]
    fn same_failure_ignores_the_step() {
        let a = Violation { property: Property::P3, step: 1, detail: "x".into() };
        let b = Violation { property: Property::P3, step: 9, detail: "x".into() };
        let c = Violation { property: Property::P4, step: 1, detail: "x".into() };
        assert!(a.same_failure(&b));
        assert!(!a.same_failure(&c));
    }
}
