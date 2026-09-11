//! What a node keeps, split by what survives a crash
//!
//! The contract's failure model (P1) says stable storage means a successful fsync and nothing
//! else. So a node's state is two structs: [`StableStorage`], which a crash keeps, and
//! [`Volatile`], which it loses - and an entry moves from the second to the first only through
//! an explicit [`Event::StorageComplete`](crate::event::Event::StorageComplete). Everything the
//! checker counts as durable is read from the first.

use std::collections::{BTreeMap, VecDeque};

use serde::{Deserialize, Serialize};

use crate::event::{MutationOp, OpResult};
use crate::ids::{Attempt, Key, LogIndex, OpId, TabletId, Term, Value};

/// What a log entry carries
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Command {
    /// A leader's first entry in its term, which is what lets it commit earlier terms' entries
    Noop,
    /// A client's mutation, with the identity a retry repeats
    Client {
        /// The attempt that first proposed it
        attempt: Attempt,
        /// The mutation
        op: MutationOp,
    },
}

/// One entry in a tablet's log
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// The tablet whose log this is (P2: never inferred from a position)
    pub tablet: TabletId,
    /// Its position, continuous across terms
    pub index: LogIndex,
    /// The term of the leader that appended it
    pub term: Term,
    /// What it carries
    pub command: Command,
}

/// What survives a crash
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StableStorage {
    /// The current term, persisted before any reply that depends on it
    pub term: Term,
    /// Who this node voted for in the current term, persisted before the vote is sent
    pub voted_for: Option<crate::ids::NodeId>,
    /// The durable log: every entry here has had its fsync complete
    pub log: Vec<Entry>,
    /// The last checkpoint taken, if any
    pub checkpoint: Option<Checkpoint>,
}

/// A durably installed checkpoint of applied state
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    /// The last log index and its term the state reflects
    pub last_included: (LogIndex, Term),
    /// The state
    pub state: AppliedState,
}

/// What a crash loses
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Volatile {
    /// Entries appended but not yet durable, continuing the stable log
    pub appended: Vec<Entry>,
    /// The index each pending fsync will make durable through, oldest first
    pub pending_fsync: VecDeque<LogIndex>,
    /// The highest index this node believes committed
    pub commit_index: LogIndex,
    /// The query-visible state, which reflects only committed entries under the safe policy
    pub applied: AppliedState,
    /// The highest index known to match the current leader's log in this term
    pub match_bound: LogIndex,
}

/// The state machine: rows, plus the result every applied identity produced
///
/// The results table is C5's deduplication state. It is part of the state machine rather than a
/// side table so that it is rebuilt by applying the log and carried by checkpoints, which is what
/// lets a retry after a leader change return the original result.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AppliedState {
    /// The rows
    pub rows: BTreeMap<Key, Value>,
    /// What each applied operation identity returned
    pub results: BTreeMap<OpId, OpResult>,
    /// The last log index applied
    pub last_applied: LogIndex,
}

impl AppliedState {
    /// Apply one entry, returning what it answered if it carried a client operation
    ///
    /// An identity already in the results table is not applied again: its stored result is
    /// returned unchanged, which is what makes a replayed retry harmless.
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry to apply, which must be the one after `last_applied`
    pub fn apply(&mut self, entry: &Entry) -> Option<(Attempt, OpResult)> {
        // every apply advances the position, whatever the entry carried
        self.last_applied = entry.index;
        match &entry.command {
            // a noop changes nothing and answers nobody
            Command::Noop => None,
            Command::Client { attempt, op } => {
                // a known identity returns what it returned the first time
                if let Some(result) = self.results.get(&attempt.id) {
                    return Some((*attempt, *result));
                }
                // otherwise step the key and remember the answer under the identity
                let key = op.key();
                let (value, result) = Self::step_key(self.rows.get(&key).copied(), op);
                match value {
                    Some(value) => self.rows.insert(key, value),
                    None => self.rows.remove(&key),
                };
                self.results.insert(attempt.id, result);
                Some((*attempt, result))
            }
        }
    }

    /// The sequential semantics of one mutation against one key
    ///
    /// Shared with the oracle, so that the model and its judge cannot disagree about what an
    /// insert against a present key does.
    ///
    /// # Arguments
    ///
    /// * `current` - The key's value before the mutation, `None` if absent
    /// * `op` - The mutation
    pub fn step_key(current: Option<Value>, op: &MutationOp) -> (Option<Value>, OpResult) {
        match op {
            // insert only if absent
            MutationOp::Insert { value, .. } => match current {
                None => (Some(*value), OpResult::Applied(true)),
                Some(existing) => (Some(existing), OpResult::Applied(false)),
            },
            // update only if present
            MutationOp::Update { value, .. } => match current {
                Some(_) => (Some(*value), OpResult::Applied(true)),
                None => (None, OpResult::Applied(false)),
            },
            // delete only if present
            MutationOp::Delete { .. } => match current {
                Some(_) => (None, OpResult::Applied(true)),
                None => (None, OpResult::Applied(false)),
            },
            // set only if the current value is the expected one
            MutationOp::Cas {
                expected, value, ..
            } => {
                if current == *expected {
                    (Some(*value), OpResult::Applied(true))
                } else {
                    (current, OpResult::Applied(false))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{Attempt, OpId};

    /// An entry carrying a client op
    fn entry(index: u64, id: u32, op: MutationOp) -> Entry {
        Entry {
            tablet: TabletId::new(1, 0),
            index: LogIndex(index),
            term: Term(1),
            command: Command::Client {
                attempt: Attempt {
                    id: OpId(id),
                    retry: 0,
                },
                op,
            },
        }
    }

    /// The four mutations do what their names say, and say whether they did anything
    #[test]
    fn step_key_has_the_sequential_semantics() {
        let key = Key(1);
        let v = |n| Value(n);
        assert_eq!(
            AppliedState::step_key(None, &MutationOp::Insert { key, value: v(1) }),
            (Some(v(1)), OpResult::Applied(true))
        );
        assert_eq!(
            AppliedState::step_key(Some(v(1)), &MutationOp::Insert { key, value: v(2) }),
            (Some(v(1)), OpResult::Applied(false))
        );
        assert_eq!(
            AppliedState::step_key(None, &MutationOp::Update { key, value: v(2) }),
            (None, OpResult::Applied(false))
        );
        assert_eq!(
            AppliedState::step_key(Some(v(1)), &MutationOp::Delete { key }),
            (None, OpResult::Applied(true))
        );
        assert_eq!(
            AppliedState::step_key(
                Some(v(1)),
                &MutationOp::Cas {
                    key,
                    expected: Some(v(2)),
                    value: v(3)
                }
            ),
            (Some(v(1)), OpResult::Applied(false))
        );
    }

    /// Applying an identity twice returns the first result and changes nothing
    #[test]
    fn a_known_identity_is_not_applied_again() {
        let mut state = AppliedState::default();
        let key = Key(1);
        let first = state.apply(&entry(
            1,
            7,
            MutationOp::Insert {
                key,
                value: Value(1),
            },
        ));
        assert_eq!(first.map(|(_, r)| r), Some(OpResult::Applied(true)));
        // the same identity at a later index, after a delete, would insert again if it were
        // applied - it is not
        state.apply(&entry(2, 8, MutationOp::Delete { key }));
        let again = state.apply(&entry(
            3,
            7,
            MutationOp::Insert {
                key,
                value: Value(1),
            },
        ));
        assert_eq!(again.map(|(_, r)| r), Some(OpResult::Applied(true)));
        assert_eq!(state.rows.get(&key), None, "the retry was applied again");
        assert_eq!(state.last_applied, LogIndex(3));
    }
}
