//! What the operations of some stretch of time did: latencies, counts and failures by kind
//!
//! A window holds full histograms, so windows add: an arm keeps one a second, and any stretch
//! of it - the measured part, or the part before an event - is the sum of its seconds. What is
//! written to a capture is a [`WindowSummary`], the numbers read off a window.
//!
//! Two latencies are kept, because bundling makes them different things. **Per query** is from
//! a bundle's send to that one query's answer; **per bundle** is from the send to the bundle's
//! last answer. Both are measured on the driver, from the send: a node's own figures (F65) are
//! timed from when the bundle's frame arrived, which is a different and shorter span.

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

/// The kinds of operation an arm mixes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OpKind {
    /// A get of rows that were preloaded
    Read,
    /// An insert of a row from the insert pool
    Insert,
}

impl OpKind {
    /// Every kind, in report order
    pub const ALL: [OpKind; 2] = [OpKind::Read, OpKind::Insert];

    /// What the kind is called
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        // the serialized spelling
        match self {
            OpKind::Read => "read",
            OpKind::Insert => "insert",
        }
    }
}

/// How one operation ended
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// It succeeded after this long
    Ok(Duration),
    /// It was a read that found no row
    Miss,
    /// It failed in a way that says to try again, and is being sent again
    Retried,
    /// It failed with this code and message
    Failed {
        /// The error code, or what ended the stream it was on
        code: String,
        /// What the failure said
        message: String,
    },
}

/// A latency histogram in microseconds, from one microsecond to two minutes
fn histogram() -> Histogram<u64> {
    // three significant figures, the bounds every window shares so they can be added
    Histogram::new_with_bounds(1, 120_000_000, 3).expect("the histogram bounds are valid")
}

/// Record a latency, saturating rather than dropping one past the top
///
/// # Arguments
///
/// * `histogram` - Where to record it
/// * `latency` - What to record
fn record(histogram: &mut Histogram<u64>, latency: Duration) {
    // anything under a microsecond is a microsecond
    let micros = u64::try_from(latency.as_micros()).unwrap_or(u64::MAX).max(1);
    histogram.saturating_record(micros);
}

/// What one kind of operation did
#[derive(Debug, Clone)]
pub struct KindWindow {
    /// The latency of every operation that succeeded, per query
    pub latency: Histogram<u64>,
    /// How many reads found no row
    pub misses: u64,
    /// How many were sent again after a retriable failure
    pub retried: u64,
    /// How many failed, by code
    pub errors: BTreeMap<String, u64>,
    /// The first message each code came with
    pub samples: BTreeMap<String, String>,
}

impl Default for KindWindow {
    /// An empty record
    fn default() -> Self {
        KindWindow {
            latency: histogram(),
            misses: 0,
            retried: 0,
            errors: BTreeMap::new(),
            samples: BTreeMap::new(),
        }
    }
}

impl KindWindow {
    /// Add another record into this one
    ///
    /// # Arguments
    ///
    /// * `other` - The record to add
    fn add(&mut self, other: &KindWindow) {
        // histograms share their bounds, so they always add
        self.latency
            .add(&other.latency)
            .expect("the histograms share their bounds");
        self.misses += other.misses;
        self.retried += other.retried;
        for (code, count) in &other.errors {
            *self.errors.entry(code.clone()).or_default() += count;
        }
        for (code, message) in &other.samples {
            self.samples
                .entry(code.clone())
                .or_insert_with(|| message.clone());
        }
    }

    /// How many operations ended any way at all
    #[must_use]
    pub fn total(&self) -> u64 {
        self.latency.len() + self.misses + self.errors.values().sum::<u64>()
    }

    /// How many failed or missed
    #[must_use]
    pub fn failed(&self) -> u64 {
        self.misses + self.errors.values().sum::<u64>()
    }
}

/// What every kind of operation did over some stretch of time
#[derive(Debug, Clone)]
pub struct Window {
    /// Reads
    pub read: KindWindow,
    /// Inserts
    pub insert: KindWindow,
    /// The latency of every bundle whose answers were all in, per bundle
    pub bundles: Histogram<u64>,
    /// How long workers waited on an insert feed with nothing parsed, in microseconds
    pub feed_wait_us: u64,
}

impl Default for Window {
    /// An empty window
    fn default() -> Self {
        Window {
            read: KindWindow::default(),
            insert: KindWindow::default(),
            bundles: histogram(),
            feed_wait_us: 0,
        }
    }
}

impl Window {
    /// The record of one kind
    ///
    /// # Arguments
    ///
    /// * `kind` - The kind
    #[must_use]
    pub fn kind(&self, kind: OpKind) -> &KindWindow {
        match kind {
            OpKind::Read => &self.read,
            OpKind::Insert => &self.insert,
        }
    }

    /// Record how one operation ended
    ///
    /// # Arguments
    ///
    /// * `kind` - What kind of operation it was
    /// * `outcome` - How it ended
    pub fn record(&mut self, kind: OpKind, outcome: Outcome) {
        // the kind's own record
        let stats = match kind {
            OpKind::Read => &mut self.read,
            OpKind::Insert => &mut self.insert,
        };
        match outcome {
            Outcome::Ok(latency) => record(&mut stats.latency, latency),
            Outcome::Miss => stats.misses += 1,
            Outcome::Retried => stats.retried += 1,
            Outcome::Failed { code, message } => {
                stats.samples.entry(code.clone()).or_insert(message);
                *stats.errors.entry(code).or_default() += 1;
            }
        }
    }

    /// Record a bundle whose answers are all in
    ///
    /// # Arguments
    ///
    /// * `latency` - From its send to its last answer
    pub fn record_bundle(&mut self, latency: Duration) {
        record(&mut self.bundles, latency);
    }

    /// Add another window into this one
    ///
    /// # Arguments
    ///
    /// * `other` - The window to add
    pub fn add(&mut self, other: &Window) {
        // each kind, the bundles and the waits
        self.read.add(&other.read);
        self.insert.add(&other.insert);
        self.bundles
            .add(&other.bundles)
            .expect("the histograms share their bounds");
        self.feed_wait_us += other.feed_wait_us;
    }

    /// The sum of a run of windows
    ///
    /// # Arguments
    ///
    /// * `windows` - The windows to add
    #[must_use]
    pub fn sum<'a>(windows: impl IntoIterator<Item = &'a Window>) -> Window {
        // start empty and add each
        let mut total = Window::default();
        for window in windows {
            total.add(window);
        }
        total
    }

    /// The numbers read off this window
    ///
    /// # Arguments
    ///
    /// * `elapsed` - How long the window covered, for its rates
    #[must_use]
    pub fn summary(&self, elapsed: Duration) -> WindowSummary {
        // rates over the window's own length, never dividing by zero
        let secs = elapsed.as_secs_f64().max(1e-9);
        let kind = |stats: &KindWindow| KindSummary {
            ok: stats.latency.len(),
            per_sec: stats.latency.len() as f64 / secs,
            latency: LatencySummary::of(&stats.latency),
            misses: stats.misses,
            retried: stats.retried,
            errors: stats.errors.clone(),
            samples: stats.samples.clone(),
        };
        WindowSummary {
            secs: elapsed.as_secs_f64(),
            read: kind(&self.read),
            insert: kind(&self.insert),
            bundle: LatencySummary::of(&self.bundles),
            feed_wait_ms: self.feed_wait_us as f64 / 1000.0,
        }
    }
}

/// A latency distribution's headline numbers, in milliseconds
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LatencySummary {
    /// How many were recorded
    pub count: u64,
    /// The mean
    pub mean_ms: f64,
    /// The median
    pub p50_ms: f64,
    /// The 90th percentile
    pub p90_ms: f64,
    /// The 99th percentile
    pub p99_ms: f64,
    /// The 99.9th percentile
    pub p999_ms: f64,
    /// The slowest
    pub max_ms: f64,
}

impl LatencySummary {
    /// Read the headline numbers off a histogram in microseconds
    ///
    /// # Arguments
    ///
    /// * `histogram` - The histogram
    #[must_use]
    pub fn of(histogram: &Histogram<u64>) -> Self {
        // an empty histogram has no latency, rather than a latency of zero
        if histogram.is_empty() {
            return LatencySummary::default();
        }
        let ms = |micros: u64| micros as f64 / 1000.0;
        LatencySummary {
            count: histogram.len(),
            mean_ms: histogram.mean() / 1000.0,
            p50_ms: ms(histogram.value_at_quantile(0.50)),
            p90_ms: ms(histogram.value_at_quantile(0.90)),
            p99_ms: ms(histogram.value_at_quantile(0.99)),
            p999_ms: ms(histogram.value_at_quantile(0.999)),
            max_ms: ms(histogram.max()),
        }
    }
}

/// What one kind of operation did, as numbers
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct KindSummary {
    /// How many succeeded
    pub ok: u64,
    /// How many succeeded a second
    pub per_sec: f64,
    /// Their latency per query, from the send
    pub latency: LatencySummary,
    /// How many reads found no row
    pub misses: u64,
    /// How many were sent again after a retriable failure
    #[serde(default)]
    pub retried: u64,
    /// How many failed, by code
    pub errors: BTreeMap<String, u64>,
    /// The first message each code came with
    pub samples: BTreeMap<String, String>,
}

impl KindSummary {
    /// How many failed or missed
    #[must_use]
    pub fn failed(&self) -> u64 {
        self.misses + self.errors.values().sum::<u64>()
    }
}

/// What a window did, as numbers
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WindowSummary {
    /// How long it covered
    pub secs: f64,
    /// Reads
    pub read: KindSummary,
    /// Inserts
    pub insert: KindSummary,
    /// The latency of whole bundles, from the send to the last answer
    pub bundle: LatencySummary,
    /// How long workers waited on an insert feed with nothing parsed
    pub feed_wait_ms: f64,
}

impl WindowSummary {
    /// One kind's numbers
    ///
    /// # Arguments
    ///
    /// * `kind` - The kind
    #[must_use]
    pub fn kind(&self, kind: OpKind) -> &KindSummary {
        match kind {
            OpKind::Read => &self.read,
            OpKind::Insert => &self.insert,
        }
    }

    /// Every operation that succeeded, a second
    #[must_use]
    pub fn ops_per_sec(&self) -> f64 {
        self.read.per_sec + self.insert.per_sec
    }

    /// One line saying what the window did, for a terminal
    #[must_use]
    pub fn line(&self) -> String {
        // one part per kind that did anything, then the bundles
        let mut parts = Vec::new();
        for kind in OpKind::ALL {
            let stats = self.kind(kind);
            if stats.ok + stats.failed() == 0 {
                continue;
            }
            let mut part = format!(
                "{} {:.0}/s p50 {:.2}ms p99 {:.2}ms",
                kind.as_str(),
                stats.per_sec,
                stats.latency.p50_ms,
                stats.latency.p99_ms
            );
            if stats.misses > 0 {
                part.push_str(&format!(" misses {}", stats.misses));
            }
            if stats.retried > 0 {
                part.push_str(&format!(" retried {}", stats.retried));
            }
            if !stats.errors.is_empty() {
                part.push_str(&format!(" errors {:?}", stats.errors));
            }
            parts.push(part);
        }
        if self.bundle.count > 0 {
            parts.push(format!(
                "bundle p50 {:.2}ms p99 {:.2}ms",
                self.bundle.p50_ms, self.bundle.p99_ms
            ));
        }
        if parts.is_empty() {
            return "idle".to_string();
        }
        parts.join(" | ")
    }
}

#[cfg(test)]
mod tests {
    use super::{OpKind, Outcome, Window};
    use std::time::Duration;

    /// Windows add, and a summary reads the sum
    #[test]
    fn windows_add_and_summarize() {
        let mut first = Window::default();
        let mut second = Window::default();
        for millis in 1..=100u64 {
            first.record(OpKind::Read, Outcome::Ok(Duration::from_millis(millis)));
        }
        second.record(OpKind::Read, Outcome::Miss);
        second.record(
            OpKind::Insert,
            Outcome::Failed {
                code: "Timeout".to_string(),
                message: "first".to_string(),
            },
        );
        second.record(
            OpKind::Insert,
            Outcome::Failed {
                code: "Timeout".to_string(),
                message: "second".to_string(),
            },
        );
        second.record_bundle(Duration::from_millis(5));
        let total = Window::sum([&first, &second]);
        let summary = total.summary(Duration::from_secs(2));
        assert_eq!(summary.read.ok, 100);
        assert_eq!(summary.read.per_sec, 50.0);
        assert_eq!(summary.read.misses, 1);
        assert!((summary.read.latency.p50_ms - 50.0).abs() < 0.1);
        assert!((summary.read.latency.max_ms - 100.0).abs() < 0.1);
        assert_eq!(summary.insert.errors["Timeout"], 2);
        // the first message a code came with is the one kept
        assert_eq!(summary.insert.samples["Timeout"], "first");
        assert_eq!(summary.bundle.count, 1);
        assert!(summary.line().contains("misses 1"));
    }

    /// An empty window has no latency rather than a latency of zero
    #[test]
    fn an_empty_window_reads_as_idle() {
        let summary = Window::default().summary(Duration::from_secs(1));
        assert_eq!(summary.read.latency.count, 0);
        assert_eq!(summary.line(), "idle");
    }
}
