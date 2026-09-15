//! The planner: which replica set goes where, from the members' weights and measured bytes
//!
//! Pure, and unit tested as such: the leader builds a [`PlanInput`] from the map it derives,
//! the phases and health the state commits and the capacity the members last reported, asks
//! for the steps a [`PlanKind`] needs, and commits what comes back
//! ([F46](../../../../docs/src/features/capacity-rebalancing.md),
//! [C8](../../../../docs/src/distributed/rebalancing.md)). Nothing here reads a clock, a
//! random source or the state; two leaders given the same input derive the same steps.
//!
//! The rules, in C8's priority order:
//!
//! 1. A **drain** moves every set the member is in, each to the feasible eligible member
//!    outside the set with the lowest weighted load - the operator's replacement first when
//!    one is named and feasible. A set with no feasible destination is left unplanned under a
//!    reason naming it and what is missing; the other steps still go.
//! 2. **Feasible** means the destination is up and a plain member, is not already in the
//!    set, reported enough free bytes for the set above the disk reserve, and has room under
//!    the per node move cap. A member that has reported no capacity yet is taken at its word
//!    that it has room: the receiver's own reserve check is what makes a stale or absent
//!    report safe.
//! 3. A **rebalance** gives every member a feasible target - its weight's share of the bytes
//!    held, capped at holding every set, the excess spread over the rest - and moves a set
//!    from the most loaded member above its target to the member below its target that
//!    gains the most, while the move brings both closer and the source is over its target by
//!    more than the hysteresis. At N = RF every member holds every set, no move is possible,
//!    and the answer is that nothing needs doing - twice in a row, the same answer.
//!
//! # Invariants
//!
//! **Never a same node move, never a change of factor.** Every step replaces one member of a
//! set with one member outside it; the set's size is what the placement rule gave it.
//!
//! **A set is planned once per plan while its step lives.** The caller passes the sets that
//! already have a live step and the planner leaves them alone; a step that failed is planned
//! again only while its failures are under the bound.

use std::collections::{BTreeMap, BTreeSet};

use crate::shared::identity::NodeId;

use super::plan::{PlanKind, PlanStep, StepState};

/// How many times a set is planned again after its move failed, before it is left blocked
pub const REPLAN_FAILURES: u32 = 2;

/// One member as the planner sees it
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInput {
    /// Whether it may be a destination: up, a plain member, and not the one being drained
    pub eligible: bool,
    /// Its weight, at least one
    pub weight: u32,
    /// The free bytes it last reported, if it has
    pub free_bytes: Option<u64>,
}

/// One replica set as the planner sees it
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetInput {
    /// The set's first tablet, which is how a move names it
    pub tablet: u16,
    /// Its members
    pub members: Vec<NodeId>,
    /// The bytes each holder reported for the set's groups, by holder
    pub bytes: BTreeMap<NodeId, u64>,
    /// Whether a live step of this plan, or a move not done, already covers it
    pub busy: bool,
    /// How many times a step for it has failed under this plan
    pub failures: u32,
}

impl SetInput {
    /// The bytes a destination would come to hold: what the source holds, or the most any
    /// holder reported, and at least one so an unmeasured set still counts as a set
    ///
    /// # Arguments
    ///
    /// * `from` - The member leaving, if the set is being drained
    #[must_use]
    pub fn size(&self, from: Option<NodeId>) -> u64 {
        let reported = from
            .and_then(|from| self.bytes.get(&from).copied())
            .filter(|bytes| *bytes > 0)
            .or_else(|| self.bytes.values().max().copied())
            .unwrap_or(0);
        reported.max(1)
    }
}

/// Everything the planner is given
#[derive(Debug, Clone, PartialEq)]
pub struct PlanInput {
    /// Every member, by node
    pub nodes: BTreeMap<NodeId, NodeInput>,
    /// Every replica set the map derives
    pub sets: Vec<SetInput>,
    /// The bytes a destination has to keep free above what it receives
    pub disk_reserve: u64,
    /// How many moves may have one member as their source, and as their destination
    pub moves_per_node: u32,
    /// The share of a member's target its load has to be over before it is a source
    pub hysteresis: f64,
    /// The moves in flight, as `(source, destination)` pairs, which count against the caps
    pub in_flight: Vec<(NodeId, NodeId)>,
}

/// What the planner derived
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PlanOutput {
    /// The steps, in the order they should be issued
    pub steps: Vec<PlanStep>,
    /// Why some set could not be placed, if one could not
    pub blocked: Option<String>,
    /// Why there was nothing to plan, when the plan is complete without a step
    pub nothing: Option<String>,
}

/// The load the planner accounts each member with as it plans
struct Ledger {
    /// Bytes each member holds, plus what the steps so far give it, less what they take
    held: BTreeMap<NodeId, u64>,
    /// Moves with each member as their source, in flight and planned
    sources: BTreeMap<NodeId, u32>,
    /// Moves with each member as their destination, in flight and planned
    destinations: BTreeMap<NodeId, u32>,
}

impl Ledger {
    /// Open the ledger from the sets' reported bytes and the moves in flight
    ///
    /// # Arguments
    ///
    /// * `input` - What the planner was given
    fn open(input: &PlanInput) -> Self {
        let mut held: BTreeMap<NodeId, u64> = input.nodes.keys().map(|node| (*node, 0)).collect();
        for set in &input.sets {
            for member in &set.members {
                *held.entry(*member).or_default() += set.size(Some(*member));
            }
        }
        let mut sources = BTreeMap::new();
        let mut destinations = BTreeMap::new();
        for (from, to) in &input.in_flight {
            *sources.entry(*from).or_default() += 1;
            *destinations.entry(*to).or_default() += 1;
        }
        Ledger {
            held,
            sources,
            destinations,
        }
    }

    /// Charge a step: the bytes move and both ends take a slot
    ///
    /// # Arguments
    ///
    /// * `step` - The step
    fn charge(&mut self, step: &PlanStep) {
        let held = self.held.entry(step.from).or_default();
        *held = held.saturating_sub(step.bytes);
        *self.held.entry(step.to).or_default() += step.bytes;
        *self.sources.entry(step.from).or_default() += 1;
        *self.destinations.entry(step.to).or_default() += 1;
    }

    /// A member's load per unit of weight
    ///
    /// # Arguments
    ///
    /// * `node` - The member
    /// * `weight` - Its weight
    fn load(&self, node: NodeId, weight: u32) -> f64 {
        // precision is not a concern for a ratio used to rank
        #[allow(clippy::cast_precision_loss)]
        let held = *self.held.get(&node).unwrap_or(&0) as f64;
        held / f64::from(weight.max(1))
    }
}

/// Why a destination is not feasible for a set
///
/// # Arguments
///
/// * `input` - What the planner was given
/// * `ledger` - The load so far
/// * `set` - The set
/// * `candidate` - The member
/// * `bytes` - The set's size
fn infeasible(
    input: &PlanInput,
    ledger: &Ledger,
    set: &SetInput,
    candidate: NodeId,
    bytes: u64,
) -> Option<String> {
    let Some(node) = input.nodes.get(&candidate) else {
        return Some(format!("{candidate} is not a member"));
    };
    if !node.eligible {
        return Some(format!("{candidate} is not an up member"));
    }
    if set.members.contains(&candidate) {
        return Some(format!("{candidate} already holds the set"));
    }
    if let Some(free) = node.free_bytes {
        let need = input.disk_reserve.saturating_add(bytes);
        if free < need {
            return Some(format!(
                "disk reserve: {candidate} reports {free} bytes free and the set needs {bytes} above the {} byte reserve",
                input.disk_reserve
            ));
        }
    }
    if *ledger.destinations.get(&candidate).unwrap_or(&0) >= input.moves_per_node {
        return Some(format!(
            "{candidate} already has {} moves onto it",
            input.moves_per_node
        ));
    }
    None
}

/// Derive the steps a plan needs
///
/// # Arguments
///
/// * `kind` - What the plan is for
/// * `input` - What the planner was given
#[must_use]
pub fn plan(kind: &PlanKind, input: &PlanInput) -> PlanOutput {
    match kind.drains() {
        Some(node) => drain(node, kind.replacement(), input),
        None => rebalance(input),
    }
}

/// Every set a member is in, to the least loaded feasible member outside it
///
/// # Arguments
///
/// * `node` - The member being drained
/// * `replacement` - The member named to take its place, if one
/// * `input` - What the planner was given
fn drain(node: NodeId, replacement: Option<NodeId>, input: &PlanInput) -> PlanOutput {
    let mut ledger = Ledger::open(input);
    let mut output = PlanOutput::default();
    let mut blocked: Vec<String> = Vec::new();
    // the sets the member holds, in tablet order
    let mut sets: Vec<&SetInput> = input
        .sets
        .iter()
        .filter(|set| set.members.contains(&node))
        .collect();
    sets.sort_by_key(|set| set.tablet);
    if sets.is_empty() {
        output.nothing = Some(format!("{node} holds no replica set"));
        return output;
    }
    for set in sets {
        // a set already under a step, or one that has failed too often, is left alone
        if set.busy {
            continue;
        }
        if set.failures >= REPLAN_FAILURES {
            blocked.push(format!(
                "tablet {}: its move failed {} times",
                set.tablet, set.failures
            ));
            continue;
        }
        // the source cap: one more move from this member has to fit
        if *ledger.sources.get(&node).unwrap_or(&0) >= input.moves_per_node {
            break;
        }
        let bytes = set.size(Some(node));
        // the replacement first, when it is feasible
        let chosen = replacement
            .filter(|candidate| infeasible(input, &ledger, set, *candidate, bytes).is_none())
            .or_else(|| {
                // otherwise the feasible member with the least load per weight, ties by node
                input
                    .nodes
                    .iter()
                    .filter(|(candidate, _)| **candidate != node)
                    .filter(|(candidate, _)| {
                        infeasible(input, &ledger, set, **candidate, bytes).is_none()
                    })
                    .map(|(candidate, member)| (*candidate, ledger.load(*candidate, member.weight)))
                    .min_by(|a, b| {
                        a.1.partial_cmp(&b.1)
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then(a.0.cmp(&b.0))
                    })
                    .map(|(candidate, _)| candidate)
            });
        match chosen {
            Some(to) => {
                let step = PlanStep {
                    tablet: set.tablet,
                    from: node,
                    to,
                    bytes,
                    op: None,
                    state: StepState::Pending,
                };
                ledger.charge(&step);
                output.steps.push(step);
            }
            None => blocked.push(why_blocked(input, &ledger, set, node, bytes)),
        }
    }
    if !blocked.is_empty() {
        output.blocked = Some(blocked.join("; "));
    }
    output
}

/// Why no member can take a set, naming what is missing
///
/// # Arguments
///
/// * `input` - What the planner was given
/// * `ledger` - The load so far
/// * `set` - The set
/// * `node` - The member being drained
/// * `bytes` - The set's size
fn why_blocked(
    input: &PlanInput,
    ledger: &Ledger,
    set: &SetInput,
    node: NodeId,
    bytes: u64,
) -> String {
    // the members outside the set that are eligible at all
    let outside: Vec<NodeId> = input
        .nodes
        .iter()
        .filter(|(candidate, member)| {
            **candidate != node && member.eligible && !set.members.contains(candidate)
        })
        .map(|(candidate, _)| *candidate)
        .collect();
    if outside.is_empty() {
        return format!(
            "tablet {}: every up member holds the set; a further member is needed to take {node}'s copy",
            set.tablet
        );
    }
    let reasons: Vec<String> = outside
        .iter()
        .filter_map(|candidate| infeasible(input, ledger, set, *candidate, bytes))
        .collect();
    format!("tablet {}: {}", set.tablet, reasons.join(", "))
}

/// Spread the sets over the members by weight, from the most over its target to the most under
///
/// # Arguments
///
/// * `input` - What the planner was given
fn rebalance(input: &PlanInput) -> PlanOutput {
    let mut ledger = Ledger::open(input);
    let mut output = PlanOutput::default();
    // the eligible members, which are the only ones with a target
    let eligible: BTreeSet<NodeId> = input
        .nodes
        .iter()
        .filter(|(_, member)| member.eligible)
        .map(|(node, _)| *node)
        .collect();
    if eligible.len() < 2 {
        output.nothing = Some("fewer than two up members; nothing to balance between".to_string());
        return output;
    }
    // at N = RF every member holds every set and no move exists
    let holds_everything = eligible
        .iter()
        .all(|node| input.sets.iter().all(|set| set.members.contains(node)));
    if holds_everything && !input.sets.is_empty() {
        output.nothing = Some(
            "every member holds every set; the replication factor is the member count".to_string(),
        );
        return output;
    }
    let targets = targets(input, &eligible, &ledger);
    let mut busy: BTreeSet<u16> = input
        .sets
        .iter()
        .filter(|set| set.busy)
        .map(|set| set.tablet)
        .collect();
    // one step at a time, from the most over to the best under, until no move improves things
    loop {
        // the source: the eligible member most over its target, past the hysteresis
        let source = eligible
            .iter()
            .filter(|node| *ledger.sources.get(node).unwrap_or(&0) < input.moves_per_node)
            .map(|node| (*node, over(&ledger, &targets, *node)))
            .filter(|(node, over)| {
                *over > input.hysteresis * targets.get(node).copied().unwrap_or(0.0)
            })
            .max_by(|a, b| {
                a.1.partial_cmp(&b.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(b.0.cmp(&a.0))
            });
        let Some((source, source_over)) = source else {
            break;
        };
        // the best move: the set and destination whose move brings both ends closest
        let mut best: Option<(u16, NodeId, u64, f64)> = None;
        for set in input
            .sets
            .iter()
            .filter(|set| set.members.contains(&source) && !busy.contains(&set.tablet))
        {
            if set.failures >= REPLAN_FAILURES {
                continue;
            }
            let bytes = set.size(Some(source));
            for candidate in &eligible {
                if infeasible(input, &ledger, set, *candidate, bytes).is_some() {
                    continue;
                }
                let deficit = -over(&ledger, &targets, *candidate);
                if deficit <= 0.0 {
                    continue;
                }
                // precision is not a concern for a ranking
                #[allow(clippy::cast_precision_loss)]
                let moved = bytes as f64;
                let before = source_over.abs() + deficit.abs();
                let after = (source_over - moved).abs() + (deficit - moved).abs();
                let improvement = before - after;
                if improvement <= 0.0 {
                    continue;
                }
                let better = best.is_none_or(|(tablet, node, _, gain)| {
                    improvement > gain
                        || (improvement == gain && (set.tablet, *candidate) < (tablet, node))
                });
                if better {
                    best = Some((set.tablet, *candidate, bytes, improvement));
                }
            }
        }
        let Some((tablet, to, bytes, _)) = best else {
            break;
        };
        let step = PlanStep {
            tablet,
            from: source,
            to,
            bytes,
            op: None,
            state: StepState::Pending,
        };
        ledger.charge(&step);
        busy.insert(tablet);
        output.steps.push(step);
    }
    if output.steps.is_empty() {
        output.nothing =
            Some("every member is within the hysteresis of its feasible target".to_string());
    }
    output
}

/// How far a member's load is over its target, in bytes; negative when under
///
/// # Arguments
///
/// * `ledger` - The load so far
/// * `targets` - Every member's target
/// * `node` - The member
fn over(ledger: &Ledger, targets: &BTreeMap<NodeId, f64>, node: NodeId) -> f64 {
    // precision is not a concern for a difference used to rank
    #[allow(clippy::cast_precision_loss)]
    let held = *ledger.held.get(&node).unwrap_or(&0) as f64;
    held - targets.get(&node).copied().unwrap_or(0.0)
}

/// Every eligible member's feasible target: its weight's share of the bytes held, capped at
/// holding every set, the excess spread over the members that are not capped
///
/// # Arguments
///
/// * `input` - What the planner was given
/// * `eligible` - The members with a target
/// * `ledger` - The load so far
fn targets(
    input: &PlanInput,
    eligible: &BTreeSet<NodeId>,
    ledger: &Ledger,
) -> BTreeMap<NodeId, f64> {
    // the bytes held across every member, and the most any one member could hold
    // precision is not a concern for a target
    #[allow(clippy::cast_precision_loss)]
    let total: f64 = eligible
        .iter()
        .map(|node| *ledger.held.get(node).unwrap_or(&0) as f64)
        .sum();
    #[allow(clippy::cast_precision_loss)]
    let everything: f64 = input.sets.iter().map(|set| set.size(None) as f64).sum();
    let mut targets: BTreeMap<NodeId, f64> = BTreeMap::new();
    let mut remaining = total;
    let mut open: BTreeSet<NodeId> = eligible.clone();
    // water-fill: a member whose share passes the cap is capped, and the rest share what is left
    loop {
        let weight: f64 = open
            .iter()
            .map(|node| f64::from(input.nodes[node].weight.max(1)))
            .sum();
        if weight <= 0.0 || open.is_empty() {
            break;
        }
        let mut capped = Vec::new();
        for node in &open {
            let share = remaining * f64::from(input.nodes[node].weight.max(1)) / weight;
            if share > everything {
                capped.push(*node);
            }
        }
        if capped.is_empty() {
            for node in &open {
                let share = remaining * f64::from(input.nodes[node].weight.max(1)) / weight;
                targets.insert(*node, share);
            }
            break;
        }
        for node in capped {
            targets.insert(node, everything);
            remaining -= everything;
            open.remove(&node);
        }
    }
    targets
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A set of members named by index
    fn nodes(n: usize) -> Vec<NodeId> {
        let mut nodes: Vec<NodeId> = (0..n).map(|_| NodeId::mint()).collect();
        nodes.sort();
        nodes
    }

    /// An input over some members with the same weight and plenty of disk
    fn input(members: &[NodeId], sets: Vec<SetInput>) -> PlanInput {
        PlanInput {
            nodes: members
                .iter()
                .map(|node| {
                    (
                        *node,
                        NodeInput {
                            eligible: true,
                            weight: 1,
                            free_bytes: Some(1 << 40),
                        },
                    )
                })
                .collect(),
            sets,
            disk_reserve: 1 << 30,
            moves_per_node: 1,
            hysteresis: 0.1,
            in_flight: Vec::new(),
        }
    }

    /// A set over some members with the same bytes on each
    fn set(tablet: u16, members: &[NodeId], bytes: u64) -> SetInput {
        SetInput {
            tablet,
            members: members.to_vec(),
            bytes: members.iter().map(|member| (*member, bytes)).collect(),
            busy: false,
            failures: 0,
        }
    }

    /// The drain rule places every set by weighted load with the replacement first, feasibility
    /// by reserve and cap names what is missing, the rebalance rule meets the weighted feasible
    /// target with hysteresis, N = RF is nothing twice, and a permuted input plans the same
    #[test]
    fn the_planner_drains_balances_and_blocks() {
        let n = nodes(4);
        let (a, b, c, d) = (n[0], n[1], n[2], n[3]);
        // three sets over three of four members, as three placed at a factor of three are
        let sets = vec![
            set(0, &[a, b, c], 100),
            set(1, &[b, c, a], 100),
            set(2, &[c, a, b], 100),
        ];
        // a drain of b: every set goes to d, the only member outside them, under the cap
        let mut drain_b = input(&n, sets.clone());
        drain_b.moves_per_node = 3;
        let out = plan(&PlanKind::Decommission { node: b }, &drain_b);
        assert_eq!(out.steps.len(), 3, "{out:?}");
        assert!(out
            .steps
            .iter()
            .all(|step| step.from == b && step.to == d && step.bytes == 100));
        assert_eq!(out.blocked, None);
        assert_eq!(out.nothing, None);
        // under a cap of one, one step is planned and the rest wait for the next round
        let out = plan(
            &PlanKind::Decommission { node: b },
            &input(&n, sets.clone()),
        );
        assert_eq!(out.steps.len(), 1);
        assert_eq!(out.blocked, None);
        // a member with no sets is nothing
        let out = plan(
            &PlanKind::Decommission { node: d },
            &input(&n, sets.clone()),
        );
        assert!(out.steps.is_empty());
        assert!(out.nothing.is_some());
        // at N = RF a drain is blocked naming the missing member, and every set is named
        let three = vec![set(0, &[a, b, c], 100), set(1, &[b, c, a], 100)];
        let mut drain_c = input(&[a, b, c], three.clone());
        drain_c.moves_per_node = 2;
        let out = plan(
            &PlanKind::Remove {
                node: c,
                replacement: None,
            },
            &drain_c,
        );
        assert!(out.steps.is_empty());
        let blocked = out.blocked.expect("blocked");
        assert!(blocked.contains("a further member is needed"), "{blocked}");
        assert!(
            blocked.contains("tablet 0") && blocked.contains("tablet 1"),
            "{blocked}"
        );
        // the disk reserve blocks by name, and a member with no report is taken at its word
        let mut short = input(&n, sets.clone());
        short.nodes.get_mut(&d).unwrap().free_bytes = Some(50);
        let out = plan(&PlanKind::Decommission { node: b }, &short);
        assert!(out.steps.is_empty());
        assert!(
            out.blocked
                .as_deref()
                .is_some_and(|reason| reason.contains("disk reserve")),
            "{out:?}"
        );
        short.nodes.get_mut(&d).unwrap().free_bytes = None;
        let out = plan(&PlanKind::Decommission { node: b }, &short);
        assert_eq!(out.steps.len(), 1);
        // a destination already at its cap is not a destination
        let mut busy = input(&n, sets.clone());
        busy.in_flight = vec![(a, d)];
        let out = plan(&PlanKind::Decommission { node: b }, &busy);
        assert!(out.steps.is_empty());
        assert!(
            out.blocked
                .as_deref()
                .is_some_and(|reason| reason.contains("moves onto it")),
            "{out:?}"
        );
        // a busy set is skipped and a set that failed too often is blocked by name
        let mut failed = input(&n, sets.clone());
        failed.moves_per_node = 3;
        failed.sets[0].busy = true;
        failed.sets[1].failures = REPLAN_FAILURES;
        let out = plan(&PlanKind::Decommission { node: b }, &failed);
        assert_eq!(out.steps.len(), 1);
        assert_eq!(out.steps[0].tablet, 2);
        assert!(
            out.blocked
                .as_deref()
                .is_some_and(|reason| reason.contains("failed")),
            "{out:?}"
        );
        // the replacement is chosen first when it is feasible; a fifth member with less load
        // would otherwise win
        let five = nodes(5);
        let (a, b, c, d, e) = (five[0], five[1], five[2], five[3], five[4]);
        let sets5 = vec![
            set(0, &[a, b, c], 100),
            set(1, &[b, c, d], 100),
            set(2, &[c, d, a], 100),
        ];
        let out = plan(
            &PlanKind::Remove {
                node: b,
                replacement: Some(d),
            },
            &input(&five, sets5.clone()),
        );
        assert_eq!(out.steps[0].to, d, "{out:?}");
        let out = plan(
            &PlanKind::Remove {
                node: b,
                replacement: None,
            },
            &input(&five, sets5.clone()),
        );
        assert_eq!(
            out.steps[0].to, e,
            "the least loaded member is chosen: {out:?}"
        );
        // a replacement inside the set is not used for that set
        let out = plan(
            &PlanKind::Remove {
                node: b,
                replacement: Some(c),
            },
            &input(&five, sets5),
        );
        assert_eq!(out.steps[0].to, e, "{out:?}");
        // a rebalance at 3:1:1:1 over three placed and one spare: the heavy member holds
        // everything, the three light ones end within a set of each other
        let n = nodes(4);
        let (a, b, c, d) = (n[0], n[1], n[2], n[3]);
        let sets = vec![
            set(0, &[a, b, c], 100),
            set(1, &[b, c, a], 100),
            set(2, &[c, a, b], 100),
        ];
        let mut weighted = input(&n, sets.clone());
        weighted.nodes.get_mut(&a).unwrap().weight = 3;
        weighted.moves_per_node = 3;
        let out = plan(&PlanKind::Rebalance, &weighted);
        assert_eq!(out.steps.len(), 2, "{out:?}");
        assert!(
            out.steps.iter().all(|step| step.to == d && step.from != a),
            "{out:?}"
        );
        let sources: BTreeSet<NodeId> = out.steps.iter().map(|step| step.from).collect();
        assert_eq!(sources, [b, c].into_iter().collect());
        assert_eq!(out.nothing, None);
        // applied, the same input is nothing: no oscillation
        let after = vec![
            set(0, &[a, d, c], 100),
            set(1, &[b, d, a], 100),
            set(2, &[c, a, b], 100),
        ];
        let mut settled = weighted.clone();
        settled.sets = after;
        let out = plan(&PlanKind::Rebalance, &settled);
        assert!(out.steps.is_empty(), "{out:?}");
        assert!(out.nothing.is_some());
        // equal weights over four: the same two moves, and a third would not improve
        let out = plan(&PlanKind::Rebalance, &input(&n, sets.clone()).with_cap(3));
        assert_eq!(out.steps.len(), 2, "{out:?}");
        // under the cap of one per node, one step at a time onto the spare
        let out = plan(&PlanKind::Rebalance, &input(&n, sets.clone()));
        assert_eq!(out.steps.len(), 1);
        // at N = RF a rebalance is nothing, naming the constraint
        let out = plan(&PlanKind::Rebalance, &input(&[a, b, c], sets.clone()));
        assert!(out.steps.is_empty());
        assert!(
            out.nothing
                .as_deref()
                .is_some_and(|reason| reason.contains("every member holds every set")),
            "{out:?}"
        );
        // unmeasured sets count as one byte each, so a cluster with nothing archived still balances by count
        let empty = vec![
            set(0, &[a, b, c], 0),
            set(1, &[b, c, a], 0),
            set(2, &[c, a, b], 0),
        ];
        let out = plan(&PlanKind::Rebalance, &input(&n, empty).with_cap(3));
        assert_eq!(out.steps.len(), 2, "{out:?}");
        // the same input in another order plans the same steps
        let mut permuted = input(&n, sets.clone()).with_cap(3);
        permuted.sets.reverse();
        let straight = plan(&PlanKind::Rebalance, &input(&n, sets).with_cap(3));
        let shuffled = plan(&PlanKind::Rebalance, &permuted);
        assert_eq!(straight.steps, shuffled.steps);
    }

    impl PlanInput {
        /// The same input under another cap, for the tests
        fn with_cap(mut self, cap: u32) -> Self {
            self.moves_per_node = cap;
            self
        }
    }
}
