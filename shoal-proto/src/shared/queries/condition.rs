//! What a conditional write expects to find, and why one was refused
//!
//! A conditional write is applied only if the row stored under its key is as its writer
//! expects: absent, or present and matching a set of filters. The condition is judged where the
//! write is applied - at apply in committed order on every replica of a tablet group, or as the
//! write is handled on a standalone node - so every replica derives the same answer, and a
//! write that is refused changes nothing and answers with a [`ConditionRefusal`] a caller can
//! branch on ([F68](../../../../docs/src/features/conditional-writes.md)).
//!
//! Both table types share these. An unsorted table judges the one row of a partition, and a
//! sorted table the row at a partition key and a sort key.

use rkyv::{Archive, Deserialize, Serialize};

use crate::shared::traits::ShoalTableSupport;

/// What a conditional write expects to find stored under its row's key
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub enum WriteCondition<T: ShoalTableSupport> {
    /// No row is stored under the key, or the row stored there was deleted
    Absent,
    /// A row is stored under the key and passes these filters
    ///
    /// The filters are the table's own, evaluated the way a get evaluates them: the values
    /// named for one field are alternatives, and every field named has to match. A filter that
    /// names no field matches any row, so this with an empty filter means "a row exists".
    Matches(T::Filters),
}

impl<T: ShoalTableSupport> WriteCondition<T> {
    /// Judge this condition against the row stored under its write's key
    ///
    /// This and [`WriteCondition::judge_archived`] are the only places a condition is
    /// evaluated, so a write is judged the same way whether its row is resident, still in the
    /// archive it was read from, or being folded by a compaction.
    ///
    /// # Arguments
    ///
    /// * `row` - The live row stored under the key, or `None` if there is none
    ///
    /// # Errors
    ///
    /// Returns why the write is refused if the row is not as the condition expects.
    pub fn judge(&self, row: Option<&T>) -> Result<(), ConditionRefusal> {
        // compare what the writer expected with what is stored
        match (self, row) {
            // nothing is stored and nothing was expected
            (WriteCondition::Absent, None) => Ok(()),
            // a row is stored where none was expected
            (WriteCondition::Absent, Some(_)) => Err(ConditionRefusal::RowExists),
            // a row was expected and nothing is stored
            (WriteCondition::Matches(_), None) => Err(ConditionRefusal::RowMissing),
            // a row is stored, so it has to pass the filters the writer named
            (WriteCondition::Matches(filters), Some(row)) => {
                // a row that passes the filters is the row the writer expected
                if T::is_filtered(filters, row) {
                    Ok(())
                } else {
                    Err(ConditionRefusal::RowMismatch)
                }
            }
        }
    }

    /// Judge this condition against the archived form of the row stored under its key
    ///
    /// # Arguments
    ///
    /// * `row` - The archived live row stored under the key, or `None` if there is none
    ///
    /// # Errors
    ///
    /// Returns why the write is refused if the row is not as the condition expects.
    pub fn judge_archived(
        &self,
        row: Option<&<T as Archive>::Archived>,
    ) -> Result<(), ConditionRefusal> {
        // compare what the writer expected with what is stored
        match (self, row) {
            // nothing is stored and nothing was expected
            (WriteCondition::Absent, None) => Ok(()),
            // a row is stored where none was expected
            (WriteCondition::Absent, Some(_)) => Err(ConditionRefusal::RowExists),
            // a row was expected and nothing is stored
            (WriteCondition::Matches(_), None) => Err(ConditionRefusal::RowMissing),
            // a row is stored, so it has to pass the filters the writer named
            (WriteCondition::Matches(filters), Some(row)) => {
                // a row that passes the filters is the row the writer expected
                if T::is_filtered_archived(filters, row) {
                    Ok(())
                } else {
                    Err(ConditionRefusal::RowMismatch)
                }
            }
        }
    }
}

/// Why a conditional write was refused
///
/// A refusal is a definite answer derived in committed order, the same on every replica: the
/// write was not applied and nothing about the table changed. It is remembered with the
/// request's identity like any other result, so a retry of a refused write is refused again
/// for the same reason. The variants are appended, never inserted: rkyv and postcard both
/// derive this enum's encoding from its order.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Archive,
    Serialize,
    Deserialize,
    serde::Serialize,
    serde::Deserialize,
)]
#[rkyv(derive(Debug, PartialEq, Eq))]
pub enum ConditionRefusal {
    /// The write expected no row and one is stored
    RowExists,
    /// The write expected a row and none is stored
    RowMissing,
    /// The write expected a row matching its filters and the stored row does not
    RowMismatch,
}

impl ConditionRefusal {
    /// Name this refusal the way a person reads it
    #[must_use]
    pub fn describe(self) -> &'static str {
        // one phrase for each reason a write can be refused
        match self {
            ConditionRefusal::RowExists => "a row exists where none was expected",
            ConditionRefusal::RowMissing => "no row exists where one was expected",
            ConditionRefusal::RowMismatch => "the row does not match the expected values",
        }
    }
}

impl std::fmt::Display for ConditionRefusal {
    // Display a refusal as the phrase that describes it
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.describe())
    }
}

impl ArchivedConditionRefusal {
    /// Get the native form of this archived refusal
    #[must_use]
    pub fn to_native(&self) -> ConditionRefusal {
        // a unit enum archives to a mirror of itself, so this is a one to one mapping
        match self {
            ArchivedConditionRefusal::RowExists => ConditionRefusal::RowExists,
            ArchivedConditionRefusal::RowMissing => ConditionRefusal::RowMissing,
            ArchivedConditionRefusal::RowMismatch => ConditionRefusal::RowMismatch,
        }
    }
}

/// A write a caller built with a condition attached, before it is turned into a query
///
/// Built by [`ConditionalWrite::if_matches`] or [`ConditionalInsert::if_absent`], and added to
/// a bundle like any other query: `#[shoal::db]` converts one into its table's conditional
/// query.
#[derive(Debug, Clone)]
pub struct Conditional<Q: ConditionalWrite> {
    /// The write to apply if its condition holds
    pub write: Q,
    /// What the write expects to find stored under its key
    pub condition: WriteCondition<Q::Table>,
}

/// A write that can be made conditional on the row stored under its key
///
/// The table derives implement this for a row (an insert), its `Update` and its `Delete`.
pub trait ConditionalWrite: Sized {
    /// The table this write is to
    type Table: ShoalTableSupport;

    /// The table's own form of this write, which a conditional query carries
    type Write;

    /// Turn this write into the table's own form of it, hashing its keys as a query does
    fn into_write(self) -> Self::Write;

    /// Apply this write only if a row is stored under its key and passes these filters
    ///
    /// The filters are the table's `Filter` type, so only fields marked `#[shoal(filter)]` can
    /// be named. An empty filter matches any row, which makes the write conditional on a row
    /// existing at all.
    ///
    /// # Arguments
    ///
    /// * `filters` - The filters the stored row has to pass
    fn if_matches(self, filters: <Self::Table as ShoalTableSupport>::Filters) -> Conditional<Self> {
        // wrap this write with the condition it is judged by
        Conditional {
            write: self,
            condition: WriteCondition::Matches(filters),
        }
    }
}

/// An insert that can be made conditional on no row being stored under its key
///
/// Implemented for rows only: an update or a delete of a row that must not exist would never
/// do anything, so neither is offered the condition.
pub trait ConditionalInsert: ConditionalWrite {
    /// Apply this insert only if no row is stored under its key
    fn if_absent(self) -> Conditional<Self> {
        // wrap this insert with the condition it is judged by
        Conditional {
            write: self,
            condition: WriteCondition::Absent,
        }
    }
}
