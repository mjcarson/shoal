//! The byte ledger: every operation a schedule made, with its interval and what it was told
//!
//! S16's ledger for ranges of bytes ([S16](../../../docs/src/object-storage/testing.md#the-model)).
//! A write that succeeded took effect whole and once; a refused one never did; an unknown one
//! may have, whole or not at all. A read records what it returned and which row and entry states
//! it took them from, so that [`crate::stripe::check`] can judge it against the sequential spec
//! at the end of a run, when every commit that could explain it is known.

use std::collections::BTreeMap;

use crate::ids::OpId;
use crate::stripe::content::Unit;
use crate::stripe::ids::{Label, Pos, SliceId, StripeIx};

/// What an operation asked for
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpKind {
    /// Write some data units of a stripe
    Write {
        /// The stripe
        stripe: StripeIx,
        /// The data units, by index within the stripe
        units: Vec<u8>,
    },
    /// Cut or grow the object
    Truncate {
        /// The new length
        len: u32,
    },
    /// Read a stripe
    Read {
        /// The stripe
        stripe: StripeIx,
        /// Whether it is strong
        strong: bool,
    },
    /// Rebuild a stale position, or move it to another slice
    Rebuild {
        /// The stripe
        stripe: StripeIx,
        /// The position
        pos: Pos,
        /// The slice it moves to, for a move
        to: Option<SliceId>,
    },
    /// Reclaim what the oldest floor hides
    Reclaim,
    /// Have a stripe's holders fold its pending bytes, and clear them from its row
    ClearPendingBytes {
        /// The stripe
        stripe: StripeIx,
    },
}

impl OpKind {
    /// Whether a client asked for it, as opposed to a driver of the group's own
    pub fn is_client(&self) -> bool {
        matches!(
            self,
            OpKind::Write { .. } | OpKind::Truncate { .. } | OpKind::Read { .. }
        )
    }
}

/// What a read returned, and what it read it from
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadResult {
    /// The index of the row state it consulted
    pub row: u32,
    /// The index of the entry state it hid and clipped by
    pub entry: u32,
    /// The latest row index when it finished, past which nothing it saw can be
    pub row_seen: u32,
    /// The latest entry index when it finished
    pub entry_seen: u32,
    /// The chunks it decoded, by position, at the label each was asked for
    pub used: Vec<(Pos, Label)>,
    /// Each data unit of the stripe, or none past the size
    pub units: Vec<Option<Unit>>,
}

/// What an operation was told
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StripeOutcome {
    /// It took effect
    Ok,
    /// It was refused before it could take effect
    Refused,
    /// The client gave up, or the driver died: it may or may not have taken effect
    Unknown,
    /// A read that could not be served, by name
    Failed,
    /// A read, and what it returned
    Read(ReadResult),
}

impl StripeOutcome {
    /// Whether this is an acknowledgement
    pub fn is_ok(&self) -> bool {
        matches!(self, StripeOutcome::Ok | StripeOutcome::Read(_))
    }
}

/// One operation's record
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpRecord {
    /// What it asked for
    pub kind: OpKind,
    /// The step it was first sent at
    pub invoke: u64,
    /// The step it was last answered at, if it has been
    pub complete: Option<u64>,
    /// What it was last told, if anything yet
    pub outcome: Option<StripeOutcome>,
    /// How many times it was tried
    pub attempts: u8,
}

/// Every operation a schedule made
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StripeLedger {
    /// The records, by identity
    pub records: BTreeMap<OpId, OpRecord>,
}

impl StripeLedger {
    /// Record an operation being sent for the first time
    ///
    /// # Arguments
    ///
    /// * `op` - Its identity
    /// * `kind` - What it asks for
    /// * `step` - When
    pub fn invoke(&mut self, op: OpId, kind: OpKind, step: u64) -> bool {
        if self.records.contains_key(&op) {
            return false;
        }
        self.records.insert(
            op,
            OpRecord {
                kind,
                invoke: step,
                complete: None,
                outcome: None,
                attempts: 1,
            },
        );
        true
    }

    /// Record an operation being tried again; its interval keeps its first invocation
    ///
    /// # Arguments
    ///
    /// * `op` - Its identity
    pub fn retry(&mut self, op: OpId) -> bool {
        let Some(record) = self.records.get_mut(&op) else {
            return false;
        };
        // only an operation whose outcome is not known, or was refused, is tried again
        if matches!(
            record.outcome,
            Some(StripeOutcome::Ok) | Some(StripeOutcome::Read(_))
        ) {
            return false;
        }
        record.attempts += 1;
        record.outcome = None;
        record.complete = None;
        true
    }

    /// Record an operation being answered; false if it was not waiting
    ///
    /// # Arguments
    ///
    /// * `op` - Its identity
    /// * `step` - When
    /// * `outcome` - What it was told
    pub fn complete(&mut self, op: OpId, step: u64, outcome: StripeOutcome) -> bool {
        let Some(record) = self.records.get_mut(&op) else {
            return false;
        };
        if record.outcome.is_some() {
            return false;
        }
        record.complete = Some(step);
        record.outcome = Some(outcome);
        true
    }

    /// Whether an operation is still waiting
    ///
    /// # Arguments
    ///
    /// * `op` - Its identity
    pub fn is_pending(&self, op: OpId) -> bool {
        self.records
            .get(&op)
            .is_some_and(|record| record.outcome.is_none())
    }

    /// What an operation asked for
    ///
    /// # Arguments
    ///
    /// * `op` - Its identity
    pub fn kind_of(&self, op: OpId) -> Option<OpKind> {
        self.records.get(&op).map(|record| record.kind.clone())
    }

    /// End the run: everything still waiting is unknown
    ///
    /// # Arguments
    ///
    /// * `step` - The step the run ended at
    pub fn finish(&mut self, step: u64) {
        for record in self.records.values_mut() {
            if record.outcome.is_none() {
                record.outcome = Some(StripeOutcome::Unknown);
                record.complete = Some(step);
            }
        }
    }
}
