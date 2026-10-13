//! The progress check, beside the clauses
//!
//! The clauses are about safety, and two of X1's expected results are about progress: a reader
//! of a stripe written continuously that cannot finish unless a holder keeps a chunk's previous
//! state, and stagers that starve each other without a reservation
//! ([S16](../../../docs/src/object-storage/testing.md#the-model)). So a generated run names the
//! step its faults stop at, and after it every reader that began has to finish within a bound, and
//! of the stagers on one stripe one has to commit within a bound. A rebuild has to rewrite only a
//! chunk its holder does not hold current, at any step. A run that breaks one records the bound it
//! exceeded, not a clause.

use serde::{Deserialize, Serialize};

use crate::ids::OpId;
use crate::stripe::ids::{Pos, StripeIx};
use crate::stripe::oracle::{OpKind, StripeLedger, StripeOutcome};
use crate::stripe::schedule::StripeParams;

/// A progress bound a run broke
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgressFailure {
    /// Which bound: `reader`, `stager` or `rebuild`
    pub bound: String,
    /// Its limit, in steps
    pub limit: u64,
    /// What happened
    pub detail: String,
}

/// What a run's readers and stagers took, after its faults stopped
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Progress {
    /// The step the faults stopped at; zero for a run with no calm phase
    pub calm_from: u64,
    /// The steps each read begun in the calm phase took to return
    pub reader_steps: Vec<u64>,
    /// The steps each write begun in the calm phase took to be acknowledged
    pub writer_steps: Vec<u64>,
    /// Reads begun in the calm phase that failed by name, having read their row again too often
    pub failed_reads: u64,
    /// Writes begun in the calm phase that were refused, their row having moved
    pub refused_writes: u64,
    /// The first bound broken while the run went on: a rebuild of a current chunk
    pub failure: Option<ProgressFailure>,
}

impl Progress {
    /// A progress record for a run whose faults stop at a step
    ///
    /// # Arguments
    ///
    /// * `calm_from` - The step, zero for none
    pub fn new(calm_from: u64) -> Self {
        Self {
            calm_from,
            ..Self::default()
        }
    }

    /// An operation was answered
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `ledger` - The ledger, with its answer in it
    /// * `step` - The step it was answered at
    pub fn on_complete(&mut self, op: OpId, ledger: &StripeLedger, step: u64) {
        let Some(record) = ledger.records.get(&op) else {
            return;
        };
        if self.calm_from == 0 || record.invoke < self.calm_from {
            return;
        }
        match (&record.kind, &record.outcome) {
            (OpKind::Read { .. }, Some(StripeOutcome::Read(_))) => {
                self.reader_steps.push(step - record.invoke);
            }
            (OpKind::Write { .. }, Some(StripeOutcome::Ok)) => {
                self.writer_steps.push(step - record.invoke);
            }
            (OpKind::Read { .. }, Some(StripeOutcome::Failed)) => self.failed_reads += 1,
            (OpKind::Write { .. }, Some(StripeOutcome::Refused)) => self.refused_writes += 1,
            _ => {}
        }
    }

    /// A rebuild is about to write a chunk
    ///
    /// # Arguments
    ///
    /// * `op` - The rebuild
    /// * `stripe` - The stripe
    /// * `pos` - The position
    /// * `held` - Whether the holder it writes to already holds the label it writes
    /// * `step` - The step
    pub fn on_rebuild_write(
        &mut self,
        op: OpId,
        stripe: StripeIx,
        pos: Pos,
        held: bool,
        step: u64,
    ) {
        let _ = step;
        if held && self.failure.is_none() {
            self.failure = Some(ProgressFailure {
                bound: "rebuild".to_string(),
                limit: 0,
                detail: format!(
                    "rebuild {} rewrote stripe {} position {}, which its holder held current",
                    op.0, stripe.0, pos.0
                ),
            });
        }
    }

    /// Judge the calm phase once the run is over
    ///
    /// # Arguments
    ///
    /// * `ledger` - The ledger, closed
    /// * `end` - The step the run ended at
    /// * `params` - The bounds
    pub fn judge(
        &self,
        ledger: &StripeLedger,
        end: u64,
        params: &StripeParams,
    ) -> Option<ProgressFailure> {
        if let Some(failure) = &self.failure {
            return Some(failure.clone());
        }
        if self.calm_from == 0 {
            return None;
        }
        let read_bound = u64::from(params.read_bound);
        let write_bound = u64::from(params.write_bound);
        // every reader begun in the calm phase finishes within its bound
        for (op, record) in &ledger.records {
            let OpKind::Read { stripe, .. } = record.kind else {
                continue;
            };
            if record.invoke < self.calm_from {
                continue;
            }
            let returned = matches!(record.outcome, Some(StripeOutcome::Read(_)));
            let took = record.complete.unwrap_or(end) - record.invoke;
            let late = took > read_bound;
            // a read that failed by name did not finish; one still waiting at the end counts once
            // its bound has passed
            let gave_up = record.outcome == Some(StripeOutcome::Failed);
            let failed = gave_up || (!returned && end - record.invoke > read_bound);
            if late || failed {
                return Some(ProgressFailure {
                    bound: "reader".to_string(),
                    limit: read_bound,
                    detail: format!(
                        "read {} of stripe {}, begun at step {}, {} after {} steps",
                        op.0,
                        stripe.0,
                        record.invoke,
                        if returned {
                            "returned"
                        } else if gave_up {
                            "failed by name"
                        } else {
                            "had not returned"
                        },
                        took
                    ),
                });
            }
        }
        // of the stagers on one stripe in the calm phase, one commits within its bound
        let mut stripes: Vec<StripeIx> = ledger
            .records
            .values()
            .filter_map(|record| match &record.kind {
                OpKind::Write { stripe, .. } if record.invoke >= self.calm_from => Some(*stripe),
                _ => None,
            })
            .collect();
        stripes.sort_unstable();
        stripes.dedup();
        for stripe in stripes {
            let writes: Vec<_> = ledger
                .records
                .values()
                .filter(|record| {
                    record.invoke >= self.calm_from
                        && matches!(&record.kind, OpKind::Write { stripe: s, .. } if *s == stripe)
                })
                .collect();
            let first = writes
                .iter()
                .map(|record| record.invoke)
                .min()
                .unwrap_or(end);
            if end - first <= write_bound {
                continue;
            }
            let committed = writes.iter().any(|record| {
                record.outcome == Some(StripeOutcome::Ok)
                    && record
                        .complete
                        .is_some_and(|done| done - first <= write_bound)
            });
            if !committed {
                return Some(ProgressFailure {
                    bound: "stager".to_string(),
                    limit: write_bound,
                    detail: format!(
                        "of {} stagers on stripe {} from step {}, none committed within the bound",
                        writes.len(),
                        stripe.0,
                        first
                    ),
                });
            }
        }
        None
    }
}
