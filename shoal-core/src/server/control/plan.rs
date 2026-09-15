//! What a placement plan records: the steps the leader derives and where each one stands
//!
//! A plan is the control plane's answer to "which set goes where"
//! ([F46](../../../../docs/src/features/capacity-rebalancing.md)). An operator's
//! `Decommission`, `Remove` or `Rebalance`, or the leader's own expiry of a down member's
//! grace, records one; the leader then derives its steps from the map and the members'
//! reported capacity, and every step is an ordinary `Move` the leader issues under the plan's
//! name. The record rides the topology view - not the shard map, since no shard reads it -
//! and a new leader resumes it from where the last one committed it.
//!
//! # Invariants
//!
//! **A step is a move.** Every transition a plan drives is one [F45](../../../../docs/src/features/replica-migration.md)
//! proved: nothing here moves bytes, changes a membership or retires a copy; a step names the
//! move that does, and is judged by that move's record.
//!
//! **A blocked plan is a visible plan.** A step the planner cannot place stays pending under
//! a reason naming the set and what is missing, and the plan is replanned as the membership
//! and the reported capacity change. Nothing is dropped, and nothing is forced.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::shared::identity::NodeId;

/// How many plan records the control state keeps, newest last
pub const KEPT_PLANS: usize = 32;

/// What a plan is for
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlanKind {
    /// Spread the sets over the members by their weights and measured bytes
    Rebalance,
    /// Drain a live member an operator is retiring
    Decommission {
        /// The member
        node: NodeId,
    },
    /// Rebuild a down or leaving member's sets elsewhere, at an operator's word
    Remove {
        /// The member
        node: NodeId,
        /// The member the operator named to take its place, if one
        replacement: Option<NodeId>,
    },
    /// Rebuild a down member's sets elsewhere, because its grace elapsed
    Expiry {
        /// The member
        node: NodeId,
        /// The down episode whose grace elapsed
        episode: Uuid,
    },
}

impl PlanKind {
    /// The member this plan drains, if it drains one
    #[must_use]
    pub fn drains(&self) -> Option<NodeId> {
        match self {
            PlanKind::Rebalance => None,
            PlanKind::Decommission { node }
            | PlanKind::Remove { node, .. }
            | PlanKind::Expiry { node, .. } => Some(*node),
        }
    }

    /// The member an operator named to take the drained one's place, if one
    #[must_use]
    pub fn replacement(&self) -> Option<NodeId> {
        match self {
            PlanKind::Remove { replacement, .. } => *replacement,
            _ => None,
        }
    }

    /// The kind's name, as a log line and a capture spell it
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            PlanKind::Rebalance => "rebalance",
            PlanKind::Decommission { .. } => "decommission",
            PlanKind::Remove { .. } => "remove",
            PlanKind::Expiry { .. } => "expiry",
        }
    }
}

/// Where one step of a plan stands
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StepState {
    /// Planned, and no move has been issued for it yet
    Pending,
    /// A move has been issued and is not done
    Moving,
    /// The move is done and the set is on its destination
    Moved,
    /// The move failed, and why
    Failed {
        /// Why
        reason: String,
    },
}

impl StepState {
    /// The state's name
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            StepState::Pending => "pending",
            StepState::Moving => "moving",
            StepState::Moved => "moved",
            StepState::Failed { .. } => "failed",
        }
    }
}

/// One step of a plan: one replica set from one member to another
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanStep {
    /// The set's first tablet, which is how a move names it
    pub tablet: u16,
    /// The member leaving the set
    pub from: NodeId,
    /// The member replacing it
    pub to: NodeId,
    /// The set's bytes on the source as the planner last saw them
    pub bytes: u64,
    /// The move issued for it, once one is
    pub op: Option<Uuid>,
    /// Where it stands
    pub state: StepState,
}

impl PlanStep {
    /// Whether the step still has something to do: pending or moving
    #[must_use]
    pub fn is_live(&self) -> bool {
        matches!(self.state, StepState::Pending | StepState::Moving)
    }
}

/// Why a plan cannot go on right now
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Blocked {
    /// What is missing, naming the set and the member or capacity it needs
    pub reason: String,
    /// The topology version the reason was recorded at
    pub since: u64,
}

/// Where a plan as a whole stands
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlanPhase {
    /// Recorded, and the leader has not derived its steps yet
    Planned,
    /// Steps are being issued and driven
    Running,
    /// At least one step cannot be placed; the rest go on, and the leader replans on change
    Blocked,
    /// Every step is moved, and the drained member is being taken out of the control group
    Finishing,
    /// Nothing more will happen under this plan
    Done,
}

impl PlanPhase {
    /// The phase's name
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            PlanPhase::Planned => "planned",
            PlanPhase::Running => "running",
            PlanPhase::Blocked => "blocked",
            PlanPhase::Finishing => "finishing",
            PlanPhase::Done => "done",
        }
    }
}

/// What a plan came to
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlanOutcome {
    /// Every step moved, and a drained member is out
    Completed {
        /// How many sets moved
        moved: u32,
        /// How many bytes they held on their sources when planned
        bytes: u64,
    },
    /// The plan could not be completed, and why
    Failed {
        /// Why
        reason: String,
    },
    /// There was nothing to do, and why
    Nothing {
        /// Why
        reason: String,
    },
}

impl PlanOutcome {
    /// The outcome's name
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            PlanOutcome::Completed { .. } => "completed",
            PlanOutcome::Failed { .. } => "failed",
            PlanOutcome::Nothing { .. } => "nothing",
        }
    }
}

/// A plan, as the control state records it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanRecord {
    /// The operation
    pub op: Uuid,
    /// What it is for
    pub kind: PlanKind,
    /// Every step derived so far, in the order they were derived
    pub steps: Vec<PlanStep>,
    /// Why it cannot go on right now, if it cannot
    pub blocked: Option<Blocked>,
    /// Where it stands
    pub phase: PlanPhase,
    /// What it came to, once done
    pub outcome: Option<PlanOutcome>,
    /// Who asked
    pub principal: String,
    /// The topology version it was recorded at
    pub requested_at: u64,
    /// How many times the leader has derived steps for it
    pub replanned: u32,
}

impl PlanRecord {
    /// A plan nobody has derived steps for yet
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `kind` - What it is for
    /// * `principal` - Who asked
    /// * `requested_at` - The topology version it is recorded at
    #[must_use]
    pub fn new(op: Uuid, kind: PlanKind, principal: &str, requested_at: u64) -> Self {
        PlanRecord {
            op,
            kind,
            steps: Vec::new(),
            blocked: None,
            phase: PlanPhase::Planned,
            outcome: None,
            principal: principal.to_string(),
            requested_at,
            replanned: 0,
        }
    }

    /// Whether nothing more will happen under this plan
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.phase == PlanPhase::Done
    }

    /// The steps not yet moved or failed
    pub fn live_steps(&self) -> impl Iterator<Item = &PlanStep> {
        self.steps.iter().filter(|step| step.is_live())
    }

    /// How many times a set has failed to move under this plan
    ///
    /// # Arguments
    ///
    /// * `tablet` - The set's first tablet
    #[must_use]
    pub fn failures_of(&self, tablet: u16) -> u32 {
        let failed = self
            .steps
            .iter()
            .filter(|step| step.tablet == tablet && matches!(step.state, StepState::Failed { .. }))
            .count();
        u32::try_from(failed).unwrap_or(u32::MAX)
    }

    /// The outcome a plan whose every step moved comes to
    #[must_use]
    pub fn completed(&self) -> PlanOutcome {
        let moved: Vec<&PlanStep> = self
            .steps
            .iter()
            .filter(|step| step.state == StepState::Moved)
            .collect();
        PlanOutcome::Completed {
            moved: u32::try_from(moved.len()).unwrap_or(u32::MAX),
            bytes: moved.iter().map(|step| step.bytes).sum(),
        }
    }
}

/// What the leader says about a plan
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlanUpdate {
    /// Steps derived, appended to the record, and whether anything could not be placed
    Steps {
        /// The steps
        steps: Vec<PlanStep>,
        /// Why some set could not be placed, if one could not
        blocked: Option<String>,
    },
    /// One step moved to a state
    Step {
        /// The set's first tablet
        tablet: u16,
        /// The move issued for it, if one was
        op: Option<Uuid>,
        /// Where it stands now
        state: StepState,
    },
    /// The plan is blocked, or no longer is
    Blocked(Option<String>),
    /// Every step is moved and the drained member is being taken out of the control group
    Finishing,
    /// Nothing more will happen under the plan
    Done(PlanOutcome),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A record's helpers read its steps: live ones, failures per set, and what it completed
    #[test]
    fn a_plan_record_reads_its_steps() {
        let (a, b) = (NodeId::mint(), NodeId::mint());
        let mut record =
            PlanRecord::new(Uuid::nil(), PlanKind::Decommission { node: a }, "alice", 3);
        assert_eq!(record.kind.drains(), Some(a));
        assert_eq!(record.kind.replacement(), None);
        assert_eq!(
            PlanKind::Remove {
                node: a,
                replacement: Some(b)
            }
            .replacement(),
            Some(b)
        );
        assert_eq!(PlanKind::Rebalance.drains(), None);
        assert!(!record.is_done());
        let step = |tablet, state| PlanStep {
            tablet,
            from: a,
            to: b,
            bytes: 10,
            op: None,
            state,
        };
        record.steps = vec![
            step(0, StepState::Moved),
            step(1, StepState::Moving),
            step(
                2,
                StepState::Failed {
                    reason: "x".to_string(),
                },
            ),
            step(
                2,
                StepState::Failed {
                    reason: "y".to_string(),
                },
            ),
            step(3, StepState::Pending),
        ];
        let live: Vec<u16> = record.live_steps().map(|step| step.tablet).collect();
        assert_eq!(live, vec![1, 3]);
        assert_eq!(record.failures_of(2), 2);
        assert_eq!(record.failures_of(0), 0);
        assert_eq!(
            record.completed(),
            PlanOutcome::Completed {
                moved: 1,
                bytes: 10
            }
        );
        // every name is distinct
        let names = [
            PlanPhase::Planned,
            PlanPhase::Running,
            PlanPhase::Blocked,
            PlanPhase::Finishing,
            PlanPhase::Done,
        ];
        let mut spelled: Vec<&str> = names.iter().map(PlanPhase::name).collect();
        spelled.dedup();
        assert_eq!(spelled.len(), names.len());
    }
}
