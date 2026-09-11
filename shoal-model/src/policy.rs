//! The safe protocol, and the six ways of getting it wrong
//!
//! Every knob's first variant is what the contract requires. Every other variant is one of the
//! violations the contract table in `docs/src/distributed/protocol.md` says the M0 model must
//! reject, and each is named for the `P` number it breaks. The unsafe settings are not options;
//! they exist so that the checker can be shown to catch them, on a saved schedule, every time.

use serde::{Deserialize, Serialize};

use crate::invariants::Property;

/// How a tablet's leader is established (P5)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Election {
    /// By a majority of votes under the log-matching election restriction
    LogMatching,
    /// By the observer promoting whichever node last reported the highest index
    ///
    /// The first draft's rule, and the B=100/C=101/A+B=102 schedule is what it loses.
    HeartbeatMaxReport,
}

/// Which population a quorum is counted over (P3)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuorumRule {
    /// The committed voter configuration, each voter counted once for durable matching history
    DurableDistinctVoters,
    /// Whichever nodes the observer currently lists as up
    CurrentUpList,
}

/// What a repeated cumulative acknowledgement does (P3)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DuplicateAck {
    /// Advances a per-replica watermark; a repeat changes nothing
    Watermark,
    /// Counts as another vote
    CountsAgain,
}

/// What an acknowledgement from an Async disk is worth (P3)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AsyncReceipt {
    /// Nothing, towards durability
    NotDurable,
    /// The same as an fsynced acknowledgement
    CountsAsDurable,
}

/// What reads and checkpoints see (P4)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Visibility {
    /// The committed, applied prefix
    CommittedApplied,
    /// The appended suffix too, before it is committed
    AppendedSuffix,
}

/// When a follower vouches for an entry (P1)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckTiming {
    /// After its fsync completes
    AfterFsync,
    /// As soon as it is received
    OnReceipt,
}

/// The six knobs together
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Policy {
    /// How leaders are established
    pub election: Election,
    /// Which population a quorum is counted over
    pub quorum: QuorumRule,
    /// What a repeated acknowledgement does
    pub duplicate_ack: DuplicateAck,
    /// What an Async receipt is worth
    pub async_receipt: AsyncReceipt,
    /// What reads and checkpoints see
    pub visibility: Visibility,
    /// When a follower vouches for an entry
    pub ack_timing: AckTiming,
}

impl Policy {
    /// The contract: the first variant of every knob
    pub const fn safe() -> Self {
        Self {
            election: Election::LogMatching,
            quorum: QuorumRule::DurableDistinctVoters,
            duplicate_ack: DuplicateAck::Watermark,
            async_receipt: AsyncReceipt::NotDurable,
            visibility: Visibility::CommittedApplied,
            ack_timing: AckTiming::AfterFsync,
        }
    }

    /// Every unsafe setting, one knob moved at a time, with the property it violates
    ///
    /// The names are what a saved schedule is filed under and what `deviations` reports.
    pub fn unsafe_knobs() -> Vec<(&'static str, Policy, Property)> {
        let safe = Self::safe();
        vec![
            (
                "election_by_heartbeat_max_report",
                Policy {
                    election: Election::HeartbeatMaxReport,
                    ..safe
                },
                Property::P5,
            ),
            (
                "quorum_from_current_up_list",
                Policy {
                    quorum: QuorumRule::CurrentUpList,
                    ..safe
                },
                Property::P3,
            ),
            (
                "duplicate_ack_counts_again",
                Policy {
                    duplicate_ack: DuplicateAck::CountsAgain,
                    ..safe
                },
                Property::P3,
            ),
            (
                "async_receipt_counts_as_durable",
                Policy {
                    async_receipt: AsyncReceipt::CountsAsDurable,
                    ..safe
                },
                Property::P3,
            ),
            (
                "reads_and_checkpoints_see_appended_suffix",
                Policy {
                    visibility: Visibility::AppendedSuffix,
                    ..safe
                },
                Property::P4,
            ),
            (
                "ack_on_receipt",
                Policy {
                    ack_timing: AckTiming::OnReceipt,
                    ..safe
                },
                Property::P1,
            ),
        ]
    }

    /// The names of every knob this policy moves away from the contract; empty means safe
    pub fn deviations(&self) -> Vec<&'static str> {
        // compare knob by knob against the contract
        let safe = Self::safe();
        let mut out = Vec::new();
        if self.election != safe.election {
            out.push("election_by_heartbeat_max_report");
        }
        if self.quorum != safe.quorum {
            out.push("quorum_from_current_up_list");
        }
        if self.duplicate_ack != safe.duplicate_ack {
            out.push("duplicate_ack_counts_again");
        }
        if self.async_receipt != safe.async_receipt {
            out.push("async_receipt_counts_as_durable");
        }
        if self.visibility != safe.visibility {
            out.push("reads_and_checkpoints_see_appended_suffix");
        }
        if self.ack_timing != safe.ack_timing {
            out.push("ack_on_receipt");
        }
        out
    }
}

impl Default for Policy {
    /// The contract
    fn default() -> Self {
        Self::safe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The contract deviates from itself in nothing
    #[test]
    fn the_safe_policy_has_no_deviations() {
        assert!(Policy::safe().deviations().is_empty());
    }

    /// Each unsafe setting moves exactly the knob it is named for
    #[test]
    fn every_unsafe_knob_deviates_in_exactly_its_own_name() {
        let knobs = Policy::unsafe_knobs();
        assert_eq!(knobs.len(), 6);
        for (name, policy, _) in knobs {
            assert_eq!(policy.deviations(), vec![name]);
        }
    }

    /// A policy survives the trip through a schedule file
    #[test]
    fn a_policy_round_trips_through_json() {
        for (_, policy, _) in Policy::unsafe_knobs() {
            let json = serde_json::to_string(&policy).unwrap();
            let back: Policy = serde_json::from_str(&json).unwrap();
            assert_eq!(back, policy);
        }
    }
}
