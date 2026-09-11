//! The write ledger, and the sequential oracle that judges it
//!
//! C11's ledger: every attempt is recorded with its identity, operation, key and the steps it was
//! invoked and completed at, and its outcome is one of three things held to three contracts. A
//! successful operation took effect exactly once and returned what it returned. A rejected one
//! never took effect. An unknown one may have taken effect zero times or once, and a later
//! successful retry of the same identity must agree with whatever it did.
//!
//! The scope is one tablet at a time (P6 applied to the oracle: nothing here relates two tablets
//! or promises a snapshot across them), and within a tablet one key at a time, because every
//! operation reads or writes one key and its result depends on that key alone - so a tablet's
//! history is consistent exactly when each key's is.

use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::event::{ClientOp, MutationOp, OpResult};
use crate::ids::{Attempt, Key, OpId, TabletId, Value};
use crate::storage::AppliedState;

/// What an attempt was told
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// It succeeded, with this result
    Ok(OpResult),
    /// It was refused before it could take effect
    Rejected,
    /// The client gave up; it may or may not have taken effect
    Unknown,
}

/// One attempt's record
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Which attempt
    pub attempt: Attempt,
    /// Which tablet
    pub tablet: TabletId,
    /// What it asked for
    pub op: ClientOp,
    /// The step it was sent at
    pub invoke: u64,
    /// The step it was answered or given up on at, if it has been
    pub complete: Option<u64>,
    /// What it was told, if anything yet
    pub outcome: Option<Outcome>,
}

/// Every attempt a schedule made
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Ledger {
    /// The records, in invocation order
    pub records: Vec<Record>,
}

impl Ledger {
    /// Record an attempt being sent
    ///
    /// # Arguments
    ///
    /// * `attempt` - Which attempt
    /// * `tablet` - Which tablet
    /// * `op` - What it asks for
    /// * `step` - When
    pub fn invoke(&mut self, attempt: Attempt, tablet: TabletId, op: ClientOp, step: u64) {
        self.records.push(Record {
            attempt,
            tablet,
            op,
            invoke: step,
            complete: None,
            outcome: None,
        });
    }

    /// Record an attempt being answered; false if it was not pending
    ///
    /// # Arguments
    ///
    /// * `attempt` - Which attempt
    /// * `step` - When
    /// * `outcome` - What it was told
    pub fn complete(&mut self, attempt: Attempt, step: u64, outcome: Outcome) -> bool {
        let Some(record) = self
            .records
            .iter_mut()
            .find(|record| record.attempt == attempt && record.outcome.is_none())
        else {
            return false;
        };
        record.complete = Some(step);
        record.outcome = Some(outcome);
        true
    }

    /// Whether an attempt is still waiting
    ///
    /// # Arguments
    ///
    /// * `attempt` - Which attempt
    pub fn is_pending(&self, attempt: Attempt) -> bool {
        self.records
            .iter()
            .any(|record| record.attempt == attempt && record.outcome.is_none())
    }

    /// Every attempt still waiting
    pub fn pending(&self) -> Vec<Attempt> {
        self.records
            .iter()
            .filter(|record| record.outcome.is_none())
            .map(|record| record.attempt)
            .collect()
    }

    /// The operation an identity asked for, from its first attempt
    ///
    /// # Arguments
    ///
    /// * `id` - The identity
    pub fn op_of(&self, id: OpId) -> Option<(TabletId, ClientOp)> {
        self.records
            .iter()
            .find(|record| record.attempt.id == id)
            .map(|record| (record.tablet, record.op.clone()))
    }

    /// End the run: everything still waiting is unknown
    ///
    /// # Arguments
    ///
    /// * `step` - The final step
    pub fn finish(&mut self, step: u64) {
        for record in &mut self.records {
            if record.outcome.is_none() {
                record.complete = Some(step);
                record.outcome = Some(Outcome::Unknown);
            }
        }
    }
}

/// Why a history is not one the sequential machine could have produced
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OracleError {
    /// Two successful attempts at one identity returned different results
    InconsistentRetry {
        /// The identity
        id: OpId,
        /// What they returned
        results: Vec<OpResult>,
    },
    /// A retry carried a different operation than the original
    PayloadMismatch {
        /// The identity
        id: OpId,
    },
    /// A key saw more operations than the search is bounded to
    TooManyOps {
        /// The tablet
        tablet: TabletId,
        /// The key
        key: Key,
        /// How many
        count: usize,
    },
    /// No sequential order explains what a key's operations saw
    NotLinearizable {
        /// The tablet
        tablet: TabletId,
        /// The key
        key: Key,
        /// The operations and reads, in invocation order
        detail: String,
    },
}

/// The most operations and reads the search is bounded to per key
pub const MAX_OPS_PER_KEY: usize = 32;

/// Whether a logical operation must have taken effect, or merely may have
#[derive(Debug, Clone, PartialEq, Eq)]
enum Necessity {
    /// At least one attempt succeeded, with this result
    Must(OpResult),
    /// No attempt succeeded and at least one is unknown
    May,
}

/// One identity's operation, folded from its attempts
#[derive(Debug, Clone, PartialEq, Eq)]
struct LogicalOp {
    /// The identity
    id: OpId,
    /// The mutation
    op: MutationOp,
    /// Whether it must have taken effect
    necessity: Necessity,
    /// The earliest invocation of any attempt: the effect cannot precede it
    invoke: u64,
    /// The earliest successful completion: the effect cannot follow it; infinite if unknown
    complete: u64,
}

/// One successful read
#[derive(Debug, Clone, PartialEq, Eq)]
struct ReadOp {
    /// The identity
    id: OpId,
    /// When it completed
    complete: u64,
    /// What it saw
    value: Option<Value>,
}

/// Judge a ledger
///
/// # Arguments
///
/// * `ledger` - The history to judge; every record must be complete
pub fn check(ledger: &Ledger) -> Result<(), OracleError> {
    // one key of one tablet at a time
    let mut keys: BTreeMap<(TabletId, Key), Vec<&Record>> = BTreeMap::new();
    for record in &ledger.records {
        keys.entry((record.tablet, record.op.key()))
            .or_default()
            .push(record);
    }
    for ((tablet, key), records) in keys {
        check_key(tablet, key, &records)?;
    }
    Ok(())
}

/// Judge one key's history
fn check_key(tablet: TabletId, key: Key, records: &[&Record]) -> Result<(), OracleError> {
    // fold the attempts at each identity into one logical operation
    let mut by_id: BTreeMap<OpId, Vec<&Record>> = BTreeMap::new();
    let mut reads = Vec::new();
    for record in records {
        match &record.op {
            ClientOp::Mutate(_) => by_id.entry(record.attempt.id).or_default().push(record),
            ClientOp::Read { .. } => {
                // only a successful read constrains anything
                if let (Some(Outcome::Ok(OpResult::Value(value))), Some(complete)) =
                    (record.outcome, record.complete)
                {
                    reads.push(ReadOp {
                        id: record.attempt.id,
                        complete,
                        value,
                    });
                }
            }
        }
    }
    let mut ops = Vec::new();
    for (id, attempts) in by_id {
        let ClientOp::Mutate(op) = &attempts[0].op else {
            continue;
        };
        // every attempt at an identity carries the same operation
        if attempts.iter().any(|attempt| attempt.op != attempts[0].op) {
            return Err(OracleError::PayloadMismatch { id });
        }
        // every success at an identity returned the same result
        let results: Vec<OpResult> = attempts
            .iter()
            .filter_map(|attempt| match attempt.outcome {
                Some(Outcome::Ok(result)) => Some(result),
                _ => None,
            })
            .collect();
        if results.windows(2).any(|pair| pair[0] != pair[1]) {
            return Err(OracleError::InconsistentRetry { id, results });
        }
        let invoke = attempts.iter().map(|attempt| attempt.invoke).min().unwrap_or(0);
        let complete = attempts
            .iter()
            .filter(|attempt| matches!(attempt.outcome, Some(Outcome::Ok(_))))
            .filter_map(|attempt| attempt.complete)
            .min();
        let unknown = attempts
            .iter()
            .any(|attempt| matches!(attempt.outcome, Some(Outcome::Unknown) | None));
        let necessity = match (results.first(), unknown) {
            (Some(result), _) => Necessity::Must(*result),
            (None, true) => Necessity::May,
            // every attempt was rejected: it never happened
            (None, false) => continue,
        };
        ops.push(LogicalOp {
            id,
            op: op.clone(),
            necessity,
            invoke,
            complete: complete.unwrap_or(u64::MAX),
        });
    }
    // the search is exponential in the worst case, so it is bounded
    if ops.len() > MAX_OPS_PER_KEY || reads.len() > MAX_OPS_PER_KEY {
        return Err(OracleError::TooManyOps {
            tablet,
            key,
            count: ops.len().max(reads.len()),
        });
    }
    let mut search = Search {
        ops: &ops,
        reads: &reads,
        visited: HashSet::new(),
    };
    if search.explore(0, None, 0) {
        return Ok(());
    }
    Err(OracleError::NotLinearizable {
        tablet,
        key,
        detail: describe(&ops, &reads),
    })
}

/// The depth-first search for a sequential order
struct Search<'a> {
    /// The logical operations
    ops: &'a [LogicalOp],
    /// The successful reads
    reads: &'a [ReadOp],
    /// States already explored: placed operations, value, satisfied reads
    visited: HashSet<(u64, Option<Value>, u64)>,
}

impl Search<'_> {
    /// Whether some order of the unplaced operations explains every read
    ///
    /// # Arguments
    ///
    /// * `placed` - Which operations have taken effect, as a bitmask
    /// * `value` - The key's value after them
    /// * `reads_ok` - Which reads have been explained, as a bitmask
    fn explore(&mut self, placed: u64, value: Option<Value>, reads_ok: u64) -> bool {
        // a read is explained at a state where it saw the value and nothing placed came after it;
        // with nothing placed, the state is the empty one and any read of it is explained
        let latest_invoke = (0..self.ops.len())
            .filter(|index| placed & (1 << index) != 0)
            .map(|index| self.ops[index].invoke)
            .max();
        let before = |complete: u64| latest_invoke.is_none_or(|latest| latest < complete);
        let mut reads_ok = reads_ok;
        for (index, read) in self.reads.iter().enumerate() {
            if reads_ok & (1 << index) == 0 && read.value == value && before(read.complete) {
                reads_ok |= 1 << index;
            }
        }
        // a read that completed before something placed was invoked can never be explained now
        for (index, read) in self.reads.iter().enumerate() {
            if reads_ok & (1 << index) == 0 && !before(read.complete) {
                return false;
            }
        }
        if !self.visited.insert((placed, value, reads_ok)) {
            return false;
        }
        // done when everything that must have happened has, and every read is explained
        let all_must = (0..self.ops.len())
            .all(|index| placed & (1 << index) != 0 || self.ops[index].necessity == Necessity::May);
        let all_reads = (0..self.reads.len()).all(|index| reads_ok & (1 << index) != 0);
        if all_must && all_reads {
            return true;
        }
        // try each operation that could come next: nothing unplaced completed before it started
        for index in 0..self.ops.len() {
            if placed & (1 << index) != 0 {
                continue;
            }
            let candidate = &self.ops[index];
            let blocked = (0..self.ops.len()).any(|other| {
                other != index
                    && placed & (1 << other) == 0
                    && self.ops[other].complete < candidate.invoke
            });
            if blocked {
                continue;
            }
            // it takes effect on the current value, and a success must get its recorded result
            let (next, result) = AppliedState::step_key(value, &candidate.op);
            if let Necessity::Must(recorded) = candidate.necessity {
                if recorded != result {
                    continue;
                }
            }
            if self.explore(placed | (1 << index), next, reads_ok) {
                return true;
            }
        }
        false
    }
}

/// A key's history, for an error
fn describe(ops: &[LogicalOp], reads: &[ReadOp]) -> String {
    let mut lines: Vec<(u64, String)> = ops
        .iter()
        .map(|op| {
            let necessity = match &op.necessity {
                Necessity::Must(result) => format!("ok {result:?}"),
                Necessity::May => "unknown".to_string(),
            };
            (
                op.invoke,
                format!("op {} {:?} invoked {} {necessity}", op.id.0, op.op, op.invoke),
            )
        })
        .chain(reads.iter().map(|read| {
            (
                read.complete,
                format!("read {} completed {} saw {:?}", read.id.0, read.complete, read.value),
            )
        }))
        .collect();
    lines.sort();
    lines
        .into_iter()
        .map(|(_, line)| line)
        .collect::<Vec<_>>()
        .join("; ")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A ledger with one tablet and one key, built from (id, retry, op, invoke, complete, outcome)
    fn ledger(rows: &[(u32, u8, ClientOp, u64, u64, Outcome)]) -> Ledger {
        let mut ledger = Ledger::default();
        for (id, retry, op, invoke, complete, outcome) in rows {
            let attempt = Attempt {
                id: OpId(*id),
                retry: *retry,
            };
            ledger.invoke(attempt, TabletId::new(1, 0), op.clone(), *invoke);
            ledger.complete(attempt, *complete, *outcome);
        }
        ledger
    }

    fn insert(v: u32) -> ClientOp {
        ClientOp::Mutate(MutationOp::Insert {
            key: Key(1),
            value: Value(v),
        })
    }

    fn read() -> ClientOp {
        ClientOp::Read { key: Key(1) }
    }

    fn saw(v: Option<u32>) -> Outcome {
        Outcome::Ok(OpResult::Value(v.map(Value)))
    }

    /// Two keys are judged apart: an impossible history on one is found on that one
    #[test]
    fn keys_are_judged_independently() {
        let mut ledger = ledger(&[(1, 0, insert(5), 1, 2, Outcome::Ok(OpResult::Applied(true)))]);
        let other = Attempt {
            id: OpId(2),
            retry: 0,
        };
        ledger.invoke(other, TabletId::new(1, 0), ClientOp::Read { key: Key(2) }, 3);
        ledger.complete(other, 4, saw(Some(9)));
        let error = check(&ledger).unwrap_err();
        assert!(matches!(error, OracleError::NotLinearizable { key: Key(2), .. }), "{error:?}");
    }

    /// The bound refuses a key with too much history rather than running forever
    #[test]
    fn too_many_operations_is_refused() {
        let rows: Vec<_> = (0..40)
            .map(|n| (n, 0, insert(n), u64::from(n), u64::from(n) + 1, Outcome::Unknown))
            .collect();
        let error = check(&ledger(&rows)).unwrap_err();
        assert!(matches!(error, OracleError::TooManyOps { count: 40, .. }), "{error:?}");
    }
}
