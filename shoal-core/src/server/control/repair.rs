//! What a repair operation records, and what a quarantined copy is
//!
//! A repair is an administrative operation committed to the control log
//! ([F44](../../../../docs/src/features/repair.md)): the record names the table, the groups the
//! placement derives for it at apply, the mode and the operator's source if one was given, and
//! every group's progress as its driver proposes it. Pending records ride the pushed map so a
//! group's leader learns of one; a driver that loses leadership abandons, and the next leader
//! resumes from the committed phase. A quarantine is decided locally first - by a checksum
//! failure or by a scrub's verdict - persisted beside the WAL, and committed second through the
//! node's report, so the map can route reads around the copy.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::shared::identity::{GroupId, NodeId, ShardAddr, TableId};
use crate::shared::protocol::admin::QuarantinedMember;

/// How many repair records the control state keeps, newest last
pub const KEPT_REPAIRS: usize = 64;

/// What a repair is asked to do
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairMode {
    /// Scrub and judge, quarantine what the verdict names, and install nothing
    Verify,
    /// Scrub, judge, and repair every quarantined copy from a verified source
    Repair,
}

impl RepairMode {
    /// The mode's name, as the admin frame spells it
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            RepairMode::Verify => "verify",
            RepairMode::Repair => "repair",
        }
    }

    /// Parse a mode from how the admin frame spells it
    ///
    /// # Arguments
    ///
    /// * `text` - The spelling
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "verify" => Some(RepairMode::Verify),
            "repair" => Some(RepairMode::Repair),
            _ => None,
        }
    }
}

/// Why a copy is quarantined
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuarantineReason {
    /// A record of it did not hash to its checksum
    Checksum,
    /// Its verified digest differed from the trusted majority's at a scrub
    Divergent,
    /// An operator named it
    Operator,
    /// A replicated apply could not read one of its partitions, so the copy stopped applying
    ///
    /// Decided by the copy itself, which needs no judgement: it is repaired from the group's
    /// leader without an operator ([Resolved #160](../../../../docs/src/appendix/resolved/unreadable-partition-stalls-one-copy.md)).
    Unreadable,
}

impl QuarantineReason {
    /// The reason's name
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            QuarantineReason::Checksum => "checksum",
            QuarantineReason::Divergent => "divergent",
            QuarantineReason::Operator => "operator",
            QuarantineReason::Unreadable => "unreadable",
        }
    }
}

/// A copy's quarantine, as the shard holding it persists it
///
/// Written to `wal/Shard-N/quarantine/<group>` when it is decided and removed when it is
/// lifted, so a restart finds the copy still quarantined.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quarantine {
    /// Why
    pub reason: QuarantineReason,
    /// The log index the copy was judged at, or the read's applied index for a checksum failure
    pub at: u64,
    /// The operation that decided it, or nil for a checksum failure met by a read
    pub op: Uuid,
}

/// What a member is told to do with its copy's quarantine
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuarantineAction {
    /// Quarantine the copy
    Set(Quarantine),
    /// Lift the quarantine, if it was decided under this operation or under any
    Lift {
        /// The operation the quarantine has to have been decided under, or none for any
        op: Option<Uuid>,
    },
    /// Restart a volatile group empty, so its leader feeds it whole again
    Rebuild,
}

/// One quarantined copy, as a node reports it and the map carries it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuarantinedCopy {
    /// The table
    pub table: TableId,
    /// The group
    pub group: GroupId,
    /// The tablets the group serves, which reads are routed around this node for
    pub tablets: Vec<u16>,
    /// Why
    pub reason: QuarantineReason,
}

impl QuarantinedCopy {
    /// The copy as a report or a frame spells it
    #[must_use]
    pub fn to_member(&self) -> QuarantinedMember {
        QuarantinedMember {
            table: self.table,
            group: self.group.0,
            tablets: self.tablets.clone(),
            reason: self.reason.as_str().to_string(),
        }
    }

    /// A copy from how a report spells it
    ///
    /// # Arguments
    ///
    /// * `member` - The spelling
    #[must_use]
    pub fn from_member(member: &QuarantinedMember) -> Self {
        QuarantinedCopy {
            table: member.table,
            group: GroupId(member.group),
            tablets: member.tablets.clone(),
            reason: match member.reason.as_str() {
                "checksum" => QuarantineReason::Checksum,
                "divergent" => QuarantineReason::Divergent,
                "unreadable" => QuarantineReason::Unreadable,
                _ => QuarantineReason::Operator,
            },
        }
    }
}

/// What one replica reported at a scrub, as the record keeps it
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DigestSummary {
    /// The canonical digest
    pub digest: u64,
    /// How many live rows the copy holds
    pub rows: u64,
    /// How many partitions
    pub partitions: u64,
    /// How many records failed their checksum; zero is a verified copy
    pub checksum_failures: u64,
}

/// Where a group's repair stands
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairPhase {
    /// Waiting for a move of the group's set to finish
    /// ([F45](../../../../docs/src/features/replica-migration.md))
    Queued {
        /// The move it waits behind
        behind: Uuid,
    },
    /// Nobody has driven it yet
    Pending,
    /// The scrub entry is proposed and the reports are being polled
    Scrubbing,
    /// The reports are in and judged; the outcome says what the verdict was
    Judged,
    /// A snapshot from the source is being installed on the target
    Installing {
        /// The verified copy being sent
        source: ShardAddr,
        /// The quarantined copy being replaced
        target: ShardAddr,
    },
    /// A second scrub is checking the installed copy
    Verifying,
    /// Nothing more will happen to this group under this operation
    Done,
}

impl RepairPhase {
    /// Where this phase stands in the order a repair moves through
    #[must_use]
    pub fn rank(&self) -> u8 {
        match self {
            RepairPhase::Queued { .. } => 0,
            RepairPhase::Pending => 1,
            RepairPhase::Scrubbing => 2,
            RepairPhase::Judged => 3,
            RepairPhase::Installing { .. } => 4,
            RepairPhase::Verifying => 5,
            RepairPhase::Done => 6,
        }
    }
}

/// What a group's repair came to
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairOutcome {
    /// Every verified copy agreed with the trusted majority, and nothing was quarantined
    Clean {
        /// The members that never reported, whose copies were not judged
        #[serde(default)]
        unreported: Vec<ShardAddr>,
    },
    /// A trusted majority was found and every other copy was quarantined
    Divergent {
        /// The copies quarantined, and why
        quarantined: Vec<(ShardAddr, QuarantineReason)>,
    },
    /// No trusted majority: nothing was quarantined or installed, and the digests are the evidence
    Unresolved {
        /// Every verified copy's digest, by member
        ///
        /// A list rather than a map: the record is JSON in the control log, and a JSON map
        /// cannot be keyed by a shard address.
        digests: Vec<(ShardAddr, DigestSummary)>,
        /// The copies that were not verified, by member
        invalid: Vec<ShardAddr>,
    },
    /// Every quarantined copy was replaced from a verified source and verified afterwards
    Repaired {
        /// The source
        source: ShardAddr,
        /// The copies replaced
        targets: Vec<ShardAddr>,
        /// The boundary the last snapshot was cut at
        boundary: u64,
        /// The index the second scrub verified the copies at
        verified: u64,
    },
    /// The repair could not be completed; the copy stays quarantined
    Failed {
        /// Why
        reason: String,
    },
    /// An operator released the copy's quarantine after an explicit outcome
    Released,
}

/// One group's progress under a repair operation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupRepair {
    /// Where it stands
    pub phase: RepairPhase,
    /// The node driving it, if one has said so
    pub driver: Option<NodeId>,
    /// The index the scrub committed at, once it has
    pub boundary: Option<u64>,
    /// Every member's report at the scrub, once judged, in member order
    #[serde(default)]
    pub reports: Vec<(ShardAddr, Result<DigestSummary, String>)>,
    /// What it came to, once done or judged
    pub outcome: Option<RepairOutcome>,
}

impl Default for GroupRepair {
    fn default() -> Self {
        GroupRepair {
            phase: RepairPhase::Pending,
            driver: None,
            boundary: None,
            reports: Vec::new(),
            outcome: None,
        }
    }
}

impl GroupRepair {
    /// Whether nothing more will happen to this group under its operation
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.phase == RepairPhase::Done
    }

    /// Whether the group waits for a move of its set to finish
    #[must_use]
    pub fn is_queued(&self) -> bool {
        matches!(self.phase, RepairPhase::Queued { .. })
    }
}

/// A repair operation, as the control state records it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairRecord {
    /// The operation
    pub op: Uuid,
    /// The table
    pub table: TableId,
    /// One tablet, or every tablet of the table
    pub tablet: Option<u16>,
    /// What was asked
    pub mode: RepairMode,
    /// The copy an operator named as trusted, overriding the majority rule
    pub source: Option<NodeId>,
    /// Whether the operator asked for the quarantines to be lifted rather than judged
    pub release: bool,
    /// Who asked
    pub principal: String,
    /// The topology version the request was applied at
    pub requested_at: u64,
    /// Every group of the table, or the one holding the tablet, and where each stands
    pub groups: BTreeMap<GroupId, GroupRepair>,
}

impl RepairRecord {
    /// Whether every group is done
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.groups.values().all(GroupRepair::is_done)
    }
}
