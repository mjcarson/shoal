//! Cutting an event arm's seconds into what the client saw before, during and after its event
//!
//! An event is anything done to the cluster while an arm runs: a node killed or stopped, a
//! rebalance, a repair. Whoever does it leaves [`Mark`]s on the arm's clock, and the arm keeps a
//! window a second. Read together, the three windows say what the event cost the client, so an
//! outage is never averaged into the run around it.
//!
//! The windows are cut at second resolution, which is what the series is kept at. A fault's
//! **during** runs from the kill to the start of the first [`SUSTAINED`] seconds that each saw an
//! answer and no failure; a background operation's from when it was asked for to when it was
//! done.

use serde::{Deserialize, Serialize};
use std::ops::Range;

/// How many clean seconds in a row count as recovered from a fault
pub const SUSTAINED: usize = 2;

/// Something that happened to the cluster, on the arm's clock
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mark {
    /// What happened: `kill`, `stop`, `restart`, `requested`, `planned`, `done`, `converged`, `failed`
    pub kind: String,
    /// When, in milliseconds since the arm started
    pub at_ms: u64,
    /// Anything worth saying about it
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl Mark {
    /// The second of the arm this mark fell in
    #[must_use]
    pub fn second(&self) -> usize {
        (self.at_ms / 1000) as usize
    }
}

/// The three windows an event arm is read in, as ranges of its seconds
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cut {
    /// The measured seconds before the event
    pub before: Range<usize>,
    /// The seconds the event was felt
    pub during: Range<usize>,
    /// The seconds after it
    pub after: Range<usize>,
    /// The first second the client saw a failure, for a fault
    pub failure: Option<usize>,
    /// The first of the clean seconds that ended the outage, for a fault
    pub recovery: Option<usize>,
}

/// Cut an arm at a fault, where the client's own answers say when it was felt and when it ended
///
/// # Arguments
///
/// * `ok` - How many operations succeeded in each second of the arm
/// * `failed` - How many failed or missed in each second
/// * `from` - The first measured second
/// * `mark` - The second the fault was injected in
#[must_use]
pub fn cut_fault(ok: &[u64], failed: &[u64], from: usize, mark: usize) -> Cut {
    // the arm's length, and the mark clamped into it
    let end = ok.len().min(failed.len());
    let mark = mark.clamp(from, end);
    // the first second at or after the fault with a failure in it
    let failure = (mark..end).find(|second| failed[*second] > 0);
    // and the first run of clean, answered seconds after that
    let recovery = failure.and_then(|failure| {
        (failure + 1..end).find(|start| {
            *start + SUSTAINED <= end
                && (*start..*start + SUSTAINED).all(|second| failed[second] == 0 && ok[second] > 0)
        })
    });
    // a fault nobody noticed has no during, and one never recovered from has no after
    let during_end = match (failure, recovery) {
        (None, _) => mark,
        (Some(_), Some(recovery)) => recovery,
        (Some(_), None) => end,
    };
    Cut {
        before: from..mark,
        during: mark..during_end,
        after: during_end..end,
        failure,
        recovery,
    }
}

/// Cut an arm at a background operation, from when it was asked for to when it was done
///
/// # Arguments
///
/// * `len` - How many seconds the arm ran
/// * `from` - The first measured second
/// * `requested` - The second it was asked for in
/// * `done` - The second it was done in, if it finished
#[must_use]
pub fn cut_background(len: usize, from: usize, requested: usize, done: Option<usize>) -> Cut {
    // the operation's own span, clamped into the arm
    let requested = requested.clamp(from, len);
    let finished = done.map_or(len, |done| (done + 1).clamp(requested, len));
    Cut {
        before: from..requested,
        during: requested..finished,
        after: finished..len,
        failure: None,
        recovery: None,
    }
}

/// The during window's p99 as a share of the before window's, in thousandths
///
/// The number a rebalance's cost to the client is judged on: 2000 is twice as slow.
///
/// # Arguments
///
/// * `before` - The p99 before, in milliseconds
/// * `during` - The p99 during, in milliseconds
#[must_use]
pub fn p99_ratio_permille(before: f64, during: f64) -> Option<u64> {
    // no before is no baseline to judge by
    if before <= 0.0 || during <= 0.0 {
        return None;
    }
    Some((during / before * 1000.0).round() as u64)
}

/// When a node that came back caught up, from its lag sampled once a second
///
/// Caught up is two samples in a row with nothing to apply, so one lucky sample between
/// batches is not taken for the end.
///
/// # Arguments
///
/// * `samples` - When each sample was taken, in milliseconds on the arm's clock, and the lag
#[must_use]
pub fn converged(samples: &[(u64, u64)]) -> Option<u64> {
    // the first of two caught up samples in a row
    samples
        .windows(2)
        .find(|pair| pair[0].1 == 0 && pair[1].1 == 0)
        .map(|pair| pair[0].0)
}

/// What a returning node's catch up looked like
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Catchup {
    /// Each sample: when, in milliseconds on the arm's clock, and how many entries it lagged by
    pub samples: Vec<(u64, u64)>,
    /// When it caught up, if it did
    pub converged_ms: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::{converged, cut_background, cut_fault, p99_ratio_permille};

    /// An outage runs from the fault to the start of the first clean run
    #[test]
    fn a_fault_is_cut_where_the_client_saw_it() {
        // ten seconds, measured from 2, fault at 4, failures at 5 and 6, a blip at 8
        let ok = [9, 9, 9, 9, 9, 3, 0, 9, 9, 9];
        let failed = [0, 0, 0, 0, 0, 5, 9, 0, 1, 0];
        let cut = cut_fault(&ok, &failed, 2, 4);
        assert_eq!(cut.failure, Some(5));
        // 7 is clean but 8 is not, so the sustained run starts at 9 - and 9 alone is too short
        assert_eq!(cut.recovery, None);
        assert_eq!((cut.before, cut.during, cut.after), (2..4, 4..10, 10..10));
        // with 8 clean, recovery is at 7
        let failed = [0, 0, 0, 0, 0, 5, 9, 0, 0, 0];
        let cut = cut_fault(&ok, &failed, 2, 4);
        assert_eq!(cut.recovery, Some(7));
        assert_eq!((cut.before, cut.during, cut.after), (2..4, 4..7, 7..10));
    }

    /// A fault nobody noticed has no during
    #[test]
    fn an_unnoticed_fault_has_no_outage() {
        let ok = [9; 6];
        let failed = [0; 6];
        let cut = cut_fault(&ok, &failed, 1, 3);
        assert_eq!(cut.failure, None);
        assert_eq!((cut.before, cut.during, cut.after), (1..3, 3..3, 3..6));
    }

    /// A background operation is cut at its own marks
    #[test]
    fn a_background_operation_is_cut_at_its_marks() {
        let cut = cut_background(20, 5, 8, Some(12));
        assert_eq!((cut.before, cut.during, cut.after), (5..8, 8..13, 13..20));
        // one that never finished is felt to the end
        let cut = cut_background(20, 5, 8, None);
        assert_eq!((cut.during, cut.after), (8..20, 20..20));
    }

    /// The ratio is in thousandths and needs a baseline
    #[test]
    fn the_ratio_needs_a_baseline() {
        assert_eq!(p99_ratio_permille(2.0, 5.0), Some(2500));
        assert_eq!(p99_ratio_permille(0.0, 5.0), None);
    }

    /// Catching up takes two quiet samples in a row
    #[test]
    fn catching_up_takes_two_quiet_samples() {
        assert_eq!(converged(&[(0, 9), (1000, 0), (2000, 4), (3000, 0), (4000, 0)]), Some(3000));
        assert_eq!(converged(&[(0, 9), (1000, 0)]), None);
    }
}
