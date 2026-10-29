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
//!
//! Since [F69](../../docs/src/features/driver-operation-kinds.md) a window also keeps the kinds
//! the driver was handed beside read and insert, by name, and the bytes its operations took on
//! the wire both ways: sent, counted for each bundle a stream wrote, and received, counted for
//! each answer and kept for each kind too.
//!
//! Since [F115](../../docs/src/features/bench-scans.md) a window that saw a scan's pages keeps
//! what they held beside their latency, which is the `scan` kind's: pages, partitions, rows and
//! archived bytes, how long each held its shard's loop and read off it, by the member that
//! answered, and every whole scan that ended in it.

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

/// The kinds of operation an arm's workload sends
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum OpKind {
    /// A get of rows that were preloaded
    Read,
    /// An insert of a row from the insert pool
    Insert,
    /// A kind the driver was handed, by its name
    /// ([F69](../../docs/src/features/driver-operation-kinds.md))
    Supplied(Arc<str>),
}

impl OpKind {
    /// The driver's own kinds, in report order
    pub const ALL: [OpKind; 2] = [OpKind::Read, OpKind::Insert];

    /// What the kind is called
    #[must_use]
    pub fn as_str(&self) -> &str {
        // the serialized spelling, or the name the kind was handed with
        match self {
            OpKind::Read => "read",
            OpKind::Insert => "insert",
            OpKind::Supplied(name) => name,
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

/// A histogram of a scan's figures in microseconds, from one microsecond to a minute
///
/// Two significant figures, not a latency's three: a scan keeps two of these a member every
/// second, and a page's hold and read time are read to a percent
/// ([F115](../../docs/src/features/bench-scans.md)).
fn scan_histogram() -> Histogram<u64> {
    // the bounds every scan window shares, so they can be added
    Histogram::new_with_bounds(1, 60_000_000, 2).expect("the scan histogram bounds are valid")
}

/// What one page of a scan held, as its answer says it
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PageFigures {
    /// The partitions it handed out
    pub partitions: u64,
    /// Their live rows
    pub rows: u64,
    /// Their archived bytes
    pub bytes: u64,
    /// How many came from memory
    pub resident: u64,
    /// How many came from an archive
    pub archived: u64,
    /// How many archived records had moved under it and were looked up again
    pub reprobed: u64,
    /// How long it held its shard's loop, in microseconds
    pub held_us: u64,
    /// How long its reads took off the loop, in microseconds
    pub read_us: u64,
    /// How long it waited from its admission to its capture, in microseconds; zero from a build
    /// before [F117](../../docs/src/features/scan-class.md)
    pub queued_us: u64,
}

/// What one member's answers to a scan's pages held
#[derive(Debug, Clone)]
pub struct MemberScanWindow {
    /// How many pages it answered
    pub pages: u64,
    /// The partitions they handed out
    pub partitions: u64,
    /// Their live rows
    pub rows: u64,
    /// Their archived bytes
    pub bytes: u64,
    /// How long each page held its shard's loop
    pub held: Histogram<u64>,
    /// How long each page's reads took off the loop
    pub read: Histogram<u64>,
    /// How long each page waited for its capture, from its admission
    pub queued: Histogram<u64>,
}

impl Default for MemberScanWindow {
    /// A member that answered nothing yet
    fn default() -> Self {
        MemberScanWindow {
            pages: 0,
            partitions: 0,
            rows: 0,
            bytes: 0,
            held: scan_histogram(),
            read: scan_histogram(),
            queued: scan_histogram(),
        }
    }
}

impl MemberScanWindow {
    /// Add another member's record into this one
    ///
    /// # Arguments
    ///
    /// * `other` - The record to add
    fn add(&mut self, other: &MemberScanWindow) {
        // the counts, and histograms that share their bounds
        self.pages += other.pages;
        self.partitions += other.partitions;
        self.rows += other.rows;
        self.bytes += other.bytes;
        self.held.add(&other.held).expect("the histograms share their bounds");
        self.read.add(&other.read).expect("the histograms share their bounds");
        self.queued.add(&other.queued).expect("the histograms share their bounds");
    }
}

/// What a scan did in some stretch of time, beside its pages' latency, which is the `scan` kind's
#[derive(Debug, Clone, Default)]
pub struct ScanWindow {
    /// How many pages were answered
    pub pages: u64,
    /// The partitions they handed out
    pub partitions: u64,
    /// Their live rows
    pub rows: u64,
    /// Their archived bytes
    pub bytes: u64,
    /// How many partitions came from memory
    pub resident: u64,
    /// How many came from an archive
    pub archived: u64,
    /// How many archived records had moved under a page and were looked up again
    pub reprobed: u64,
    /// How many pages a shard shed, reading as many as it reads at once, and that were asked again
    pub shed: u64,
    /// How many pages a copy that could not answer yet refused, and that were asked again
    pub waited: u64,
    /// Those waits, by what the copy was refused for or the member could not be reached for
    pub waits: BTreeMap<String, u64>,
    /// The first message each of those came with
    pub wait_samples: BTreeMap<String, String>,
    /// How many whole scans ended
    pub scans: u64,
    /// How long each whole scan that ended took, in microseconds
    pub whole_us: Vec<u64>,
    /// How long each scan that began took to its first page, in microseconds
    pub first_page_us: Vec<u64>,
    /// The pages each member answered, by its name
    pub members: BTreeMap<String, MemberScanWindow>,
}

impl ScanWindow {
    /// Count one page a member answered
    ///
    /// # Arguments
    ///
    /// * `member` - The member that answered it, by name
    /// * `page` - What it held
    pub fn page(&mut self, member: &str, page: &PageFigures) {
        // the scan's counts
        self.pages += 1;
        self.partitions += page.partitions;
        self.rows += page.rows;
        self.bytes += page.bytes;
        self.resident += page.resident;
        self.archived += page.archived;
        self.reprobed += page.reprobed;
        // and the member's, with how long the page held and read
        let mine = self.members.entry(member.to_string()).or_default();
        mine.pages += 1;
        mine.partitions += page.partitions;
        mine.rows += page.rows;
        mine.bytes += page.bytes;
        mine.held.saturating_record(page.held_us.max(1));
        mine.read.saturating_record(page.read_us.max(1));
        mine.queued.saturating_record(page.queued_us.max(1));
    }

    /// Count a page asked again because its copy could not answer yet
    ///
    /// # Arguments
    ///
    /// * `code` - What it was refused for, or `Unreached` for a member not reached
    /// * `message` - What it said, kept if it is the first of its code
    pub fn wait(&mut self, code: &str, message: String) {
        self.waited += 1;
        *self.waits.entry(code.to_string()).or_default() += 1;
        self.wait_samples.entry(code.to_string()).or_insert(message);
    }

    /// Add another scan's record into this one
    ///
    /// # Arguments
    ///
    /// * `other` - The record to add
    fn add(&mut self, other: &ScanWindow) {
        // every count, every whole scan, and each member's record
        self.pages += other.pages;
        self.partitions += other.partitions;
        self.rows += other.rows;
        self.bytes += other.bytes;
        self.resident += other.resident;
        self.archived += other.archived;
        self.reprobed += other.reprobed;
        self.shed += other.shed;
        self.waited += other.waited;
        for (code, count) in &other.waits {
            *self.waits.entry(code.clone()).or_default() += count;
        }
        for (code, message) in &other.wait_samples {
            self.wait_samples.entry(code.clone()).or_insert_with(|| message.clone());
        }
        self.scans += other.scans;
        self.whole_us.extend_from_slice(&other.whole_us);
        self.first_page_us.extend_from_slice(&other.first_page_us);
        for (name, mine) in &other.members {
            self.members.entry(name.clone()).or_default().add(mine);
        }
    }

    /// The numbers read off this record
    ///
    /// # Arguments
    ///
    /// * `secs` - How long the window covered, never zero
    fn summary(&self, secs: f64) -> ScanSummary {
        // a scan's hold, read and wait for its capture are every member's, added
        let mut held = scan_histogram();
        let mut read = scan_histogram();
        let mut queued = scan_histogram();
        for mine in self.members.values() {
            held.add(&mine.held).expect("the histograms share their bounds");
            read.add(&mine.read).expect("the histograms share their bounds");
            queued.add(&mine.queued).expect("the histograms share their bounds");
        }
        // the few whole scans and first pages, in a histogram made for them
        let of_list = |list: &[u64]| {
            let mut whole = histogram();
            for micros in list {
                whole.saturating_record((*micros).max(1));
            }
            LatencySummary::of(&whole)
        };
        ScanSummary {
            pages: self.pages,
            pages_per_sec: self.pages as f64 / secs,
            partitions: self.partitions,
            partitions_per_sec: self.partitions as f64 / secs,
            rows: self.rows,
            rows_per_sec: self.rows as f64 / secs,
            bytes: self.bytes,
            bytes_per_sec: self.bytes as f64 / secs,
            resident: self.resident,
            archived: self.archived,
            reprobed: self.reprobed,
            shed: self.shed,
            waited: self.waited,
            waits: self.waits.clone(),
            wait_samples: self.wait_samples.clone(),
            held: LatencySummary::of(&held),
            read: LatencySummary::of(&read),
            queued: LatencySummary::of(&queued),
            scans: self.scans,
            whole: of_list(&self.whole_us),
            first_page: of_list(&self.first_page_us),
            members: self
                .members
                .iter()
                .map(|(name, mine)| {
                    let summary = MemberScanSummary {
                        pages: mine.pages,
                        partitions: mine.partitions,
                        partitions_per_sec: mine.partitions as f64 / secs,
                        rows: mine.rows,
                        bytes: mine.bytes,
                        held: LatencySummary::of(&mine.held),
                        read: LatencySummary::of(&mine.read),
                        queued: LatencySummary::of(&mine.queued),
                    };
                    (name.clone(), summary)
                })
                .collect(),
        }
    }
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
    /// The bytes the answers took on the wire, frame header included
    pub bytes_received: u64,
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
            bytes_received: 0,
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
        self.bytes_received += other.bytes_received;
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
    /// Every kind the driver was handed, by name
    pub kinds: BTreeMap<Arc<str>, KindWindow>,
    /// The latency of every bundle whose answers were all in, per bundle
    pub bundles: Histogram<u64>,
    /// How long workers waited on an insert feed with nothing parsed, in microseconds
    pub feed_wait_us: u64,
    /// The bytes every bundle sent took on the wire, frame header included
    pub bytes_sent: u64,
    /// The bytes every answer took on the wire, frame header included, whatever it answered
    pub bytes_received: u64,
    /// What a scan's pages held, for a window that saw any
    /// ([F115](../../docs/src/features/bench-scans.md)); none otherwise, so a window of reads and
    /// inserts allocates nothing for it
    pub scan: Option<Box<ScanWindow>>,
}

impl Default for Window {
    /// An empty window
    fn default() -> Self {
        Window {
            read: KindWindow::default(),
            insert: KindWindow::default(),
            kinds: BTreeMap::new(),
            bundles: histogram(),
            feed_wait_us: 0,
            bytes_sent: 0,
            bytes_received: 0,
            scan: None,
        }
    }
}

impl Window {
    /// The record of one kind, if anything of that kind was recorded here
    ///
    /// # Arguments
    ///
    /// * `kind` - The kind
    #[must_use]
    pub fn kind(&self, kind: &OpKind) -> Option<&KindWindow> {
        match kind {
            OpKind::Read => Some(&self.read),
            OpKind::Insert => Some(&self.insert),
            OpKind::Supplied(name) => self.kinds.get(name),
        }
    }

    /// The record of one kind, made if this is its first
    ///
    /// # Arguments
    ///
    /// * `kind` - The kind
    fn kind_mut(&mut self, kind: &OpKind) -> &mut KindWindow {
        match kind {
            OpKind::Read => &mut self.read,
            OpKind::Insert => &mut self.insert,
            OpKind::Supplied(name) => self.kinds.entry(name.clone()).or_default(),
        }
    }

    /// Record how one operation ended
    ///
    /// # Arguments
    ///
    /// * `kind` - What kind of operation it was
    /// * `outcome` - How it ended
    pub fn record(&mut self, kind: &OpKind, outcome: Outcome) {
        // the kind's own record
        let stats = self.kind_mut(kind);
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

    /// Record the bytes an answer took on the wire
    ///
    /// # Arguments
    ///
    /// * `kind` - What it answered, if the driver was owed it
    /// * `bytes` - How many bytes, frame header included
    pub fn record_received(&mut self, kind: Option<&OpKind>, bytes: u64) {
        // every answer counts towards the window, and an owed one towards its kind too
        self.bytes_received += bytes;
        if let Some(kind) = kind {
            self.kind_mut(kind).bytes_received += bytes;
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

    /// The record of a scan's pages, made if this is the window's first
    pub fn scan_mut(&mut self) -> &mut ScanWindow {
        self.scan.get_or_insert_with(Box::default)
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
        for (name, stats) in &other.kinds {
            self.kinds.entry(name.clone()).or_default().add(stats);
        }
        self.bundles
            .add(&other.bundles)
            .expect("the histograms share their bounds");
        self.feed_wait_us += other.feed_wait_us;
        self.bytes_sent += other.bytes_sent;
        self.bytes_received += other.bytes_received;
        // and a scan's record, which a window without one takes as it is
        if let Some(scan) = &other.scan {
            self.scan_mut().add(scan);
        }
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
            bytes_received: stats.bytes_received,
        };
        WindowSummary {
            secs: elapsed.as_secs_f64(),
            read: kind(&self.read),
            insert: kind(&self.insert),
            kinds: self
                .kinds
                .iter()
                .map(|(name, stats)| (name.to_string(), kind(stats)))
                .collect(),
            bundle: LatencySummary::of(&self.bundles),
            feed_wait_ms: self.feed_wait_us as f64 / 1000.0,
            bytes_sent: self.bytes_sent,
            bytes_received: self.bytes_received,
            sent_per_sec: self.bytes_sent as f64 / secs,
            received_per_sec: self.bytes_received as f64 / secs,
            scan: self.scan.as_ref().map(|scan| scan.summary(secs)),
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
    /// The bytes the answers took on the wire, frame header included
    /// ([F69](../../docs/src/features/driver-operation-kinds.md)); zero in a capture from before it
    #[serde(default)]
    pub bytes_received: u64,
}

impl KindSummary {
    /// How many failed or missed
    #[must_use]
    pub fn failed(&self) -> u64 {
        self.misses + self.errors.values().sum::<u64>()
    }
}

/// What one member's answers to a scan's pages held, as numbers
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MemberScanSummary {
    /// How many pages it answered
    pub pages: u64,
    /// The partitions they handed out
    pub partitions: u64,
    /// The partitions they handed out, a second
    pub partitions_per_sec: f64,
    /// Their live rows
    pub rows: u64,
    /// Their archived bytes
    pub bytes: u64,
    /// How long each page held its shard's loop
    pub held: LatencySummary,
    /// How long each page's reads took off the loop
    pub read: LatencySummary,
    /// How long each page waited for its capture, from its admission
    #[serde(default)]
    pub queued: LatencySummary,
}

/// What a scan's pages did in a window, as numbers ([F115](../../docs/src/features/bench-scans.md))
///
/// A page's latency, from its ask to its answer, is the `scan` kind's; this is what the pages
/// held and what the members that answered them said they cost.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ScanSummary {
    /// How many pages were answered
    pub pages: u64,
    /// How many were answered a second
    pub pages_per_sec: f64,
    /// The partitions they handed out
    pub partitions: u64,
    /// The partitions they handed out, a second
    pub partitions_per_sec: f64,
    /// Their live rows
    pub rows: u64,
    /// Their live rows, a second
    pub rows_per_sec: f64,
    /// Their archived bytes
    pub bytes: u64,
    /// Their archived bytes, a second
    pub bytes_per_sec: f64,
    /// How many partitions came from memory
    pub resident: u64,
    /// How many came from an archive
    pub archived: u64,
    /// How many archived records had moved under a page and were looked up again
    pub reprobed: u64,
    /// How many pages a shard shed and were asked again
    pub shed: u64,
    /// How many pages a copy that could not answer yet refused, and were asked again
    pub waited: u64,
    /// Those waits, by what the copy was refused for, or `Unreached` for a member not reached
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub waits: BTreeMap<String, u64>,
    /// The first message each of those came with
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub wait_samples: BTreeMap<String, String>,
    /// How long each page held its shard's loop, every member's
    pub held: LatencySummary,
    /// How long each page's reads took off the loop, every member's
    pub read: LatencySummary,
    /// How long each page waited for its capture from its admission, every member's
    /// ([F117](../../docs/src/features/scan-class.md))
    #[serde(default)]
    pub queued: LatencySummary,
    /// How many whole scans ended
    pub scans: u64,
    /// How long each whole scan that ended took
    pub whole: LatencySummary,
    /// How long each scan took to its first page
    pub first_page: LatencySummary,
    /// The pages each member answered, by its name
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub members: BTreeMap<String, MemberScanSummary>,
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
    /// Every kind the driver was handed, by name; absent from a capture with none, and from one
    /// taken before [F69](../../docs/src/features/driver-operation-kinds.md)
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub kinds: BTreeMap<String, KindSummary>,
    /// The latency of whole bundles, from the send to the last answer
    pub bundle: LatencySummary,
    /// How long workers waited on an insert feed with nothing parsed
    pub feed_wait_ms: f64,
    /// The bytes every bundle sent took on the wire, frame header included; zero in a capture
    /// from before F69
    #[serde(default)]
    pub bytes_sent: u64,
    /// The bytes every answer took on the wire, frame header included; zero before F69
    #[serde(default)]
    pub bytes_received: u64,
    /// The bytes sent, a second
    #[serde(default)]
    pub sent_per_sec: f64,
    /// The bytes received, a second
    #[serde(default)]
    pub received_per_sec: f64,
    /// What a scan's pages held, for a window that saw any; absent from every other window, and
    /// from every capture taken before [F115](../../docs/src/features/bench-scans.md)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan: Option<ScanSummary>,
}

impl WindowSummary {
    /// One kind's numbers, if any of that kind were recorded
    ///
    /// # Arguments
    ///
    /// * `kind` - The kind
    #[must_use]
    pub fn kind(&self, kind: &OpKind) -> Option<&KindSummary> {
        match kind {
            OpKind::Read => Some(&self.read),
            OpKind::Insert => Some(&self.insert),
            OpKind::Supplied(name) => self.kinds.get(&**name),
        }
    }

    /// Every kind's numbers, the driver's own first and then each supplied one by name
    pub fn every_kind(&self) -> impl Iterator<Item = (&str, &KindSummary)> {
        [("read", &self.read), ("insert", &self.insert)]
            .into_iter()
            .chain(
                self.kinds
                    .iter()
                    .map(|(name, stats)| (name.as_str(), stats)),
            )
    }

    /// Every operation that succeeded, a second
    #[must_use]
    pub fn ops_per_sec(&self) -> f64 {
        self.every_kind().map(|(_, stats)| stats.per_sec).sum()
    }

    /// One line saying what the window did, for a terminal
    #[must_use]
    pub fn line(&self) -> String {
        // one part per kind that did anything, then the bundles and the bytes
        let mut parts = Vec::new();
        for (name, stats) in self.every_kind() {
            if stats.ok + stats.failed() == 0 {
                continue;
            }
            let mut part = format!(
                "{} {:.0}/s p50 {:.2}ms p99 {:.2}ms",
                name, stats.per_sec, stats.latency.p50_ms, stats.latency.p99_ms
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
        // a scan's partitions and its loop holds, with what it was refused for now
        if let Some(scan) = self.scan.as_ref().filter(|scan| scan.pages > 0) {
            let mut part = format!(
                "scanned {:.0} partitions/s held p99 {:.2}ms",
                scan.partitions_per_sec, scan.held.p99_ms
            );
            if scan.shed + scan.waited > 0 {
                part.push_str(&format!(" shed {} waited {}", scan.shed, scan.waited));
            }
            parts.push(part);
        }
        if self.bytes_sent + self.bytes_received > 0 {
            let mib = |bytes: f64| bytes / (1024.0 * 1024.0);
            parts.push(format!(
                "sent {:.2} MiB/s received {:.2} MiB/s",
                mib(self.sent_per_sec),
                mib(self.received_per_sec)
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
    use super::{OpKind, Outcome, PageFigures, Window, WindowSummary};
    use std::sync::Arc;
    use std::time::Duration;

    /// Windows add, and a summary reads the sum
    #[test]
    fn windows_add_and_summarize() {
        let mut first = Window::default();
        let mut second = Window::default();
        for millis in 1..=100u64 {
            first.record(&OpKind::Read, Outcome::Ok(Duration::from_millis(millis)));
        }
        second.record(&OpKind::Read, Outcome::Miss);
        second.record(
            &OpKind::Insert,
            Outcome::Failed {
                code: "Timeout".to_string(),
                message: "first".to_string(),
            },
        );
        second.record(
            &OpKind::Insert,
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

    /// A supplied kind and the bytes both ways add and summarize as read and insert do (F69)
    #[test]
    fn kinds_and_bytes_add_and_summarize() {
        let lookup = OpKind::Supplied(Arc::from("lookup"));
        let mut first = Window::default();
        let mut second = Window::default();
        for millis in 1..=10u64 {
            first.record(&lookup, Outcome::Ok(Duration::from_millis(millis)));
            first.record_received(Some(&lookup), 100);
        }
        first.bytes_sent += 4_000;
        second.record(&lookup, Outcome::Miss);
        second.record_received(Some(&lookup), 50);
        second.record(&OpKind::Read, Outcome::Ok(Duration::from_millis(1)));
        second.record_received(Some(&OpKind::Read), 30);
        // an answer the driver was not owed counts towards the window and no kind
        second.record_received(None, 7);
        second.bytes_sent += 1_000;
        let summary = Window::sum([&first, &second]).summary(Duration::from_secs(2));
        let stats = &summary.kinds["lookup"];
        assert_eq!(
            (stats.ok, stats.misses, stats.bytes_received),
            (10, 1, 1_050)
        );
        assert_eq!(summary.read.bytes_received, 30);
        assert_eq!(summary.bytes_received, 1_087);
        assert_eq!(summary.bytes_sent, 5_000);
        assert_eq!(summary.sent_per_sec, 2_500.0);
        assert_eq!(summary.ops_per_sec(), 5.5);
        assert!(summary.line().contains("lookup 5/s"), "{}", summary.line());
        assert_eq!(summary.kind(&lookup).map(|stats| stats.ok), Some(10));
    }

    /// A scan's pages add and summarize by member, and a window that saw none writes nothing of
    /// them, so every capture from before F115 reads and writes back as it was
    #[test]
    fn scan_pages_add_and_summarize_by_member() {
        let scan = OpKind::Supplied(Arc::from("scan"));
        let page = |held_us, read_us| PageFigures {
            partitions: 100,
            rows: 250,
            bytes: 102_400,
            resident: 40,
            archived: 60,
            reprobed: 1,
            held_us,
            read_us,
            queued_us: 30,
        };
        let mut first = Window::default();
        let mut second = Window::default();
        for held in 1..=10u64 {
            first.record(&scan, Outcome::Ok(Duration::from_millis(2)));
            first.scan_mut().page("titan", &page(held * 10, 1_000));
        }
        second.scan_mut().page("europa", &page(5, 500));
        second.scan_mut().shed += 3;
        second.scan_mut().wait("Unavailable", "titan: installing".to_string());
        second.scan_mut().wait("Unavailable", "titan: still installing".to_string());
        second.scan_mut().scans += 1;
        second.scan_mut().whole_us.push(4_000_000);
        let summary = Window::sum([&first, &second]).summary(Duration::from_secs(2));
        let scanned = summary.scan.as_ref().expect("a window that saw pages says so");
        assert_eq!((scanned.pages, scanned.partitions, scanned.rows), (11, 1_100, 2_750));
        assert_eq!(scanned.partitions_per_sec, 550.0);
        assert_eq!((scanned.resident, scanned.archived, scanned.reprobed), (440, 660, 11));
        assert_eq!((scanned.shed, scanned.waited), (3, 2));
        assert_eq!(scanned.waits["Unavailable"], 2);
        // the first message a wait came with is the one kept
        assert_eq!(scanned.wait_samples["Unavailable"], "titan: installing");
        assert_eq!(scanned.members["titan"].pages, 10);
        assert_eq!(scanned.members["europa"].partitions, 100);
        // a scan's hold is every member's, read to two figures, and so is its wait for its capture
        assert!((scanned.held.max_ms - 0.1).abs() < 0.01, "{:?}", scanned.held);
        assert_eq!(scanned.queued.count, 11);
        assert!((scanned.queued.max_ms - 0.03).abs() < 0.001, "{:?}", scanned.queued);
        assert_eq!(scanned.scans, 1);
        assert!((scanned.whole.max_ms - 4_000.0).abs() < 5.0, "{:?}", scanned.whole);
        assert_eq!(summary.kinds["scan"].ok, 10);
        assert!(summary.line().contains("scanned 550 partitions/s"), "{}", summary.line());
        // and a window of reads alone carries no scan, and writes none
        let mut reads = Window::default();
        reads.record(&OpKind::Read, Outcome::Ok(Duration::from_millis(1)));
        let written = serde_json::to_string(&reads.summary(Duration::from_secs(1))).expect("a summary writes");
        assert!(!written.contains("\"scan\""), "{written}");
    }

    /// A window written before supplied kinds and bytes reads, with none of either (F69)
    ///
    /// The capture format did not move, so every capture taken before F69 has to read as one
    /// with no supplied kind and no bytes, and write back without a `kinds` field at all.
    #[test]
    fn a_summary_from_before_kinds_still_reads() {
        let latency = r#"{"count": 1, "mean_ms": 1.0, "p50_ms": 1.0, "p90_ms": 1.0,
                          "p99_ms": 1.0, "p999_ms": 1.0, "max_ms": 1.0}"#;
        let kind = format!(
            r#"{{"ok": 1, "per_sec": 1.0, "latency": {latency}, "misses": 0, "retried": 0,
                 "errors": {{}}, "samples": {{}}}}"#
        );
        let old = format!(
            r#"{{"secs": 1.0, "read": {kind}, "insert": {kind}, "bundle": {latency},
                 "feed_wait_ms": 0.0}}"#
        );
        let summary: WindowSummary = serde_json::from_str(&old).expect("an old summary reads");
        assert!(summary.kinds.is_empty());
        assert_eq!((summary.bytes_sent, summary.bytes_received), (0, 0));
        assert_eq!(summary.read.bytes_received, 0);
        let written = serde_json::to_string(&summary).expect("a summary writes");
        assert!(!written.contains("\"kinds\""), "{written}");
    }
}
