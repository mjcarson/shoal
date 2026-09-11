//! The identifiers the model names things by
//!
//! Every one of them is a newtype over a small integer so that a schedule file stays readable
//! and a node cannot be handed where a term was meant. They serialize transparently.

use std::fmt;

use serde::{Deserialize, Serialize};

/// A node in the modeled cluster, numbered from one
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(pub u8);

/// A table, as stable schema metadata rather than a peer's enum layout (P2)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TableId(pub u32);

/// A logical tablet: `(TableId, range_id)`, the unit of replication, election and progress (P2)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TabletId {
    /// The table this tablet belongs to
    pub table: TableId,
    /// The range of that table's partition hash space
    pub range: u16,
}

impl TabletId {
    /// Name a tablet
    ///
    /// # Arguments
    ///
    /// * `table` - The table
    /// * `range` - The range within it
    pub const fn new(table: u32, range: u16) -> Self {
        Self {
            table: TableId(table),
            range,
        }
    }
}

impl fmt::Display for TabletId {
    /// Written as `(table,range)`, the way the design pages write it
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({},{})", self.table.0, self.range)
    }
}

/// A leadership term, persisted before any reply that depends on it
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct Term(pub u64);

/// A position in a tablet's logical log, continuous across terms; zero names no entry
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct LogIndex(pub u64);

impl LogIndex {
    /// The index after this one
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }

    /// The index before this one, saturating at zero
    pub fn prev(self) -> Self {
        Self(self.0.saturating_sub(1))
    }
}

/// The identity a client gives an operation, which a retry repeats (C5's retry identity)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OpId(pub u32);

/// One attempt at an operation: the identity plus how many times it has been retried
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Attempt {
    /// The operation's identity, shared by every attempt at it
    pub id: OpId,
    /// Which attempt this is; zero is the original
    pub retry: u8,
}

/// A row key within a tablet
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Key(pub u8);

/// A row value
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Value(pub u32);
