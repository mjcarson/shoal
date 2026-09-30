//! What the stats view remembers of the answers it read, so it can chart them
//!
//! The `Stats` read answers the figures as they are now; nothing on the server keeps their past.
//! The view keeps its own: every metric of every member each time that member's figures are
//! new, and the cluster's metrics each time any member's are
//! ([F64](../../../../docs/src/features/stats-tui.md)).
//!
//! # Invariants
//!
//! **A member's figures are sampled once per report.** A poll faster than the member reports
//! reads the same figures again; they are recognized by `at_ms` and not sampled twice, so a
//! chart's points are the member's reports and nothing in between.
//!
//! **A stale member adds no points.** Its figures are held by the leader after it stopped
//! reporting; charting them would draw a dead node as a flat line of current load.
//!
//! **Time is this process's clock.** Each member stamps its figures by its own clock, and the
//! clocks disagree; a point is placed at the moment this view read it, so every line on a
//! chart shares one axis.

use shoal::shared::identity::NodeId;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::{Duration, Instant};

use super::metrics::{METRICS, Reader};
use super::{StatsModel, live};

/// How long samples are kept, which is the longest window the view offers
pub const RETENTION: Duration = Duration::from_secs(30 * 60);

/// Which line of a chart a sample belongs to
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Series {
    /// The cluster as a whole
    Cluster,
    /// One member
    Member(NodeId),
}

/// Every sample the view has taken, by metric and line
#[derive(Debug, Default)]
pub struct History {
    /// When the first sample was taken, which every point is placed after
    started: Option<Instant>,
    /// Each line's points, as seconds after `started` and a value, oldest first
    samples: BTreeMap<(usize, Series), VecDeque<(f64, f64)>>,
    /// Each member's figures' timestamp when it was last sampled
    last_at: HashMap<NodeId, u64>,
}

impl History {
    /// Sample an answer: every member whose figures are new and current, and the cluster if
    /// any were
    ///
    /// # Arguments
    ///
    /// * `model` - The answer
    /// * `now` - When it was read
    pub fn record(&mut self, model: &StatsModel, now: Instant) {
        // every point is placed by this process's clock
        let started = *self.started.get_or_insert(now);
        let at = now.saturating_duration_since(started).as_secs_f64();
        let mut sampled = false;
        for member in &model.view.members {
            // a stale member's figures are not current, so they are not charted
            let Some(stats) = live(member) else {
                continue;
            };
            // the same report read twice is sampled once
            if stats.at_ms != 0 && self.last_at.get(&member.node) == Some(&stats.at_ms) {
                continue;
            }
            self.last_at.insert(member.node, stats.at_ms);
            sampled = true;
            // every member metric, read from these figures
            for (index, metric) in METRICS.iter().enumerate() {
                if let Reader::Member(read) = metric.read {
                    self.push(index, Series::Member(member.node), at, read(stats));
                }
            }
        }
        // the cluster's figures only move when a member's do
        if sampled {
            for (index, metric) in METRICS.iter().enumerate() {
                if let Reader::Cluster(read) = metric.read {
                    self.push(index, Series::Cluster, at, read(&model.view));
                }
            }
        }
        // nothing older than the longest window is kept
        let horizon = at - RETENTION.as_secs_f64();
        for points in self.samples.values_mut() {
            while points.front().is_some_and(|(x, _)| *x < horizon) {
                points.pop_front();
            }
        }
        self.samples.retain(|_, points| !points.is_empty());
    }

    /// Add one point to a line
    ///
    /// # Arguments
    ///
    /// * `metric` - The metric's index
    /// * `series` - The line
    /// * `at` - Seconds after the first sample
    /// * `value` - The value
    fn push(&mut self, metric: usize, series: Series, at: f64, value: f64) {
        // a value that is not a number would break the chart's bounds, so it is dropped
        if value.is_finite() {
            self.samples
                .entry((metric, series))
                .or_default()
                .push_back((at, value));
        }
    }

    /// Every line of a metric within a window before `now`, placed at seconds before `now`
    ///
    /// # Arguments
    ///
    /// * `metric` - The metric's index
    /// * `window` - How far back to go
    /// * `now` - The chart's right edge
    #[must_use]
    pub fn series(&self, metric: usize, window: Duration, now: Instant) -> Vec<(Series, Vec<(f64, f64)>)> {
        // nothing sampled yet draws no lines
        let Some(started) = self.started else {
            return Vec::new();
        };
        // the right edge as seconds after the first sample
        let edge = now.saturating_duration_since(started).as_secs_f64();
        let from = edge - window.as_secs_f64();
        self.samples
            .range((metric, Series::Cluster)..)
            .take_while(|((index, _), _)| *index == metric)
            .map(|((_, series), points)| {
                // the points inside the window, measured back from the edge
                let points = points
                    .iter()
                    .filter(|(x, _)| *x >= from && *x <= edge)
                    .map(|(x, value)| (x - edge, *value))
                    .collect::<Vec<_>>();
                (*series, points)
            })
            .filter(|(_, points)| !points.is_empty())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::stats::metrics::index_of;
    use shoal::serde_json::json;
    use uuid::Uuid;

    /// An answer from two members: one reporting at `at_ms` with its applied inserts, one stale
    ///
    /// # Arguments
    ///
    /// * `at_ms` - When the live member derived its figures
    /// * `inserts` - Its applied inserts per second
    fn answer(at_ms: u64, inserts: f64) -> StatsModel {
        let a = "aaaaaaaa-1111-1111-1111-111111111111";
        let b = "bbbbbbbb-2222-2222-2222-222222222222";
        let view = shoal::serde_json::from_value(json!({
            "source": "leader", "answered_by": a, "at_ms": at_ms,
            "members": [
                { "node": a, "state": "up", "stats": {
                    "node": a, "at_ms": at_ms, "hostname": "hyperion",
                    "total": {
                        "applied": { "inserts": { "r10s": inserts } },
                        "led": { "inserts": { "r10s": inserts } }
                    }
                } },
                { "node": b, "state": "down", "stale": true, "stats": {
                    "node": b, "at_ms": 1, "total": {
                        "applied": { "inserts": { "r10s": 999.0 } }
                    }
                } }
            ]
        }))
        .expect("an answer decodes");
        StatsModel::new(view, None)
    }

    /// A report read twice is one point, a stale member adds none, the window slices and old
    /// points are dropped
    #[test]
    fn history_dedupes_skips_stale_and_trims() {
        let a = NodeId(Uuid::parse_str("aaaaaaaa-1111-1111-1111-111111111111").unwrap());
        let inserts = index_of("inserts").expect("the inserts metric");
        let cluster = index_of("cluster_writes").expect("the cluster metric");
        let start = Instant::now();
        let mut history = History::default();
        // two reports two seconds apart, the second read twice
        history.record(&answer(1000, 10.0), start);
        history.record(&answer(3000, 20.0), start + Duration::from_secs(2));
        history.record(&answer(3000, 20.0), start + Duration::from_secs(3));
        let now = start + Duration::from_secs(4);
        let lines = history.series(inserts, Duration::from_secs(60), now);
        // only the live member draws a line, and it has one point per report
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert_eq!(lines[0].0, Series::Member(a));
        assert_eq!(lines[0].1, vec![(-4.0, 10.0), (-2.0, 20.0)]);
        // the cluster line moves with the reports, once per row through the leaders
        let totals = history.series(cluster, Duration::from_secs(60), now);
        assert_eq!(totals, vec![(Series::Cluster, vec![(-4.0, 10.0), (-2.0, 20.0)])]);
        // a window of three seconds holds only the later point
        let recent = history.series(inserts, Duration::from_secs(3), now);
        assert_eq!(recent[0].1, vec![(-2.0, 20.0)]);
        // a point past the retention is dropped when the next is taken
        let later = start + RETENTION + Duration::from_secs(3);
        history.record(&answer(5000, 30.0), later);
        let kept = history.series(inserts, RETENTION, later);
        assert_eq!(kept[0].1.len(), 1, "{kept:?}");
        assert_eq!(kept[0].1[0], (0.0, 30.0));
        // and a chart drawn before anything was read draws nothing
        assert!(History::default().series(inserts, RETENTION, now).is_empty());
    }
}
