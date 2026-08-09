//! Benchmarks shoal

use owo_colors::OwoColorize;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::time::Instant;

/// The schema version of a written baseline
///
/// A baseline is only comparable against a run of the same shape. Two earlier baseline files
/// taught us both halves of that: `.benchmark-old` could not be deserialized at all, which was
/// loud and harmless, and `.benchmark` *did* load while having been written when the insert
/// acknowledgement was inert, which was silent and produced a comparison between two different
/// operations.
///
/// Bump this whenever [`BenchResult`] changes shape or whenever what a sample measures
/// changes. A baseline carrying a different version is rejected rather than compared.
const BASELINE_VERSION: u32 = 1;

/// Print a benchmark result with colors
macro_rules! print_bench {
    ($name:expr, $prior:expr, $current:expr) => {
        let diff = match $prior.cmp(&$current) {
            Ordering::Less => {
                // get the difference to check if its a large change
                let diff = $current - $prior;
                // get the % change
                let change = diff.as_nanos() as f64 / $prior.as_nanos() as f64;
                // convert our change to a %
                let change_percent = change * 100.0;
                // check if this change is more then 2%
                if diff.as_nanos() as f64 > ($prior.as_nanos() as f64 * 0.02) {
                    format!("+{:.2?} (+{:.2}%)", diff, change_percent)
                        .bright_red()
                        .to_string()
                } else {
                    format!("+{:.2?} (+{:.2}%)", diff, change_percent)
                        .bright_blue()
                        .to_string()
                }
            }
            Ordering::Equal => format!("{:.2?} (0.00%)", Duration::from_secs(0))
                .bright_blue()
                .to_string(),
            Ordering::Greater => {
                // get the difference to check if its a large change
                let diff = $prior - $current;
                // get the % change
                let change = diff.as_nanos() as f64 / $prior.as_nanos() as f64;
                // convert our change to a %
                let change_percent = change * 100.0;
                format!("-{:.2?} (-{:.2}%)", diff, change_percent)
                    .bright_green()
                    .to_string()
            }
        };
        // print our result and the change
        println!("  {}: {:.2?} ({})", $name, $current, diff);
    };
}

/// The kind of operation a latency sample came from
///
/// Insert and get latencies are recorded separately because they are different
/// operations with different cost profiles — an insert waits on the intent log,
/// a get may not touch disk at all. Pooling them makes a percentile meaningless,
/// since it just reports where the boundary between the two distributions lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchOp {
    /// A sample from an insert query
    Insert,
    /// A sample from a get query
    Get,
}

/// The latency distribution for one kind of operation
#[derive(Debug, Serialize, Deserialize)]
pub struct Stats {
    /// The number of samples this distribution was built from
    count: usize,
    /// The max time seen
    max: Duration,
    /// The p99 of times seen
    p99: Duration,
    /// The p95 of times seen
    p95: Duration,
    /// The p90 of times seen
    p90: Duration,
    /// The p50 of times seen
    p50: Duration,
    /// The average time seen
    avg: Duration,
    /// The lowest time seen
    min: Duration,
}

impl Stats {
    /// Build a distribution from a set of samples
    ///
    /// Sorts `times` in place. An empty sample set yields an all zero distribution
    /// with a count of 0 rather than panicking, since a run that only inserts or
    /// only reads leaves the other set empty.
    ///
    /// # Arguments
    ///
    /// * `times` - The samples to summarize
    fn from_times(times: &mut Vec<Duration>) -> Self {
        // bail out with an empty distribution if we gathered no samples
        if times.is_empty() {
            return Stats {
                count: 0,
                max: Duration::ZERO,
                p99: Duration::ZERO,
                p95: Duration::ZERO,
                p90: Duration::ZERO,
                p50: Duration::ZERO,
                avg: Duration::ZERO,
                min: Duration::ZERO,
            };
        }
        // sort our samples so we can index percentiles out of them
        times.sort_unstable();
        // sum our samples in nanoseconds to get an average
        let sum = times.iter().map(|time| time.as_nanos()).sum::<u128>();
        let avg = Duration::from_nanos((sum / times.len() as u128) as u64);
        Stats {
            count: times.len(),
            // our samples are sorted so the extremes are just the ends
            max: times[times.len() - 1],
            p99: percentile(times, 0.99),
            p95: percentile(times, 0.95),
            p90: percentile(times, 0.90),
            p50: percentile(times, 0.50),
            avg,
            min: times[0],
        }
    }

    /// Print this distribution, comparing against a prior one if we have it
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the operation this distribution is for
    /// * `prior` - The distribution from our baseline if we loaded one
    fn print(&self, name: &str, prior: Option<&Stats>) {
        // note how many samples this distribution came from, since a percentile
        // over a handful of samples means very little
        println!("{name} ({} samples)", self.count);
        // bail out early if we have nothing to report
        if self.count == 0 {
            println!("  no samples");
            return;
        }
        // print each metric, comparing against our baseline if we have one
        match prior {
            Some(prior) if prior.count > 0 => {
                print_bench!("max", prior.max, self.max);
                print_bench!("p99", prior.p99, self.p99);
                print_bench!("p95", prior.p95, self.p95);
                print_bench!("p90", prior.p90, self.p90);
                print_bench!("p50", prior.p50, self.p50);
                print_bench!("average", prior.avg, self.avg);
                print_bench!("min", prior.min, self.min);
            }
            _ => {
                println!("  max: {:.2?}", self.max);
                println!("  p99: {:.2?}", self.p99);
                println!("  p95: {:.2?}", self.p95);
                println!("  p90: {:.2?}", self.p90);
                println!("  p50: {:.2?}", self.p50);
                println!("  average: {:.2?}", self.avg);
                println!("  min: {:.2?}", self.min);
            }
        }
    }
}

/// Get the sample at a percentile using nearest rank
///
/// For `n` samples the rank of a percentile `p` is `ceil(n * p)`, which is 1 based.
/// Our samples are 0 indexed, so the index is one less than that.
///
/// # Arguments
///
/// * `sorted` - The samples to index, already sorted ascending
/// * `percentile` - The percentile to get, between 0 and 1
fn percentile(sorted: &[Duration], percentile: f64) -> Duration {
    // get the 1 based rank of this percentile
    let rank = (sorted.len() as f64 * percentile).ceil() as usize;
    // convert our rank to a 0 based index, clamped into our sample set
    let index = std::cmp::min(rank.saturating_sub(1), sorted.len() - 1);
    sorted[index]
}

/// A benchmarks past results
///
/// This is written as JSON rather than as an rkyv archive. A baseline is a durable record that
/// has to outlive the build that wrote it and be readable by a human deciding whether two
/// numbers are comparable, and an unversioned binary archive is neither.
#[derive(Debug, Serialize, Deserialize)]
pub struct BenchResult {
    /// The schema version this result was written with
    #[serde(default)]
    version: u32,
    /// The name this run was captured under, if it was given one
    #[serde(default)]
    label: Option<String>,
    /// The latency distribution for insert queries
    insert: Stats,
    /// The latency distribution for get queries
    get: Stats,
    /// The total wall clock time the last run took
    total: Duration,
    /// The number of rows this run inserted
    #[serde(default)]
    inserted: u64,
    /// The number of rows this run read back
    #[serde(default)]
    retrieved: u64,
}

impl BenchResult {
    /// Get the rows per second this run sustained over its whole wall clock
    ///
    /// This counts every row the run touched, inserted and read, against the wall clock that
    /// covers CSV parsing and worker spawn as well as the queries. It is a throughput figure
    /// for the harness as a whole, not a service rate for the server.
    fn rows_per_sec(&self) -> f64 {
        // a run that took no measurable time has no meaningful rate
        let seconds = self.total.as_secs_f64();
        if seconds <= 0.0 {
            return 0.0;
        }
        (self.inserted + self.retrieved) as f64 / seconds
    }
}

/// A bench worker for benching across workers/processes
pub struct BenchWorker {
    /// The insert times recorded by this worker
    insert_times: Vec<Duration>,
    /// The get times recorded by this worker
    get_times: Vec<Duration>,
}

impl BenchWorker {
    /// Get a timer instance
    pub fn get_timer(&self) -> Instant {
        Instant::now()
    }

    /// Record how long an operation took
    ///
    /// The elapsed time is taken now, when the caller processes the response, not
    /// when the response arrived. See the benchmarking docs for what that means for
    /// how these samples should be read.
    ///
    /// # Arguments
    ///
    /// * `op` - The kind of operation this timer is for
    /// * `timer` - The timer to add
    pub fn add_timer(&mut self, op: BenchOp, timer: Instant) {
        // record this sample against the right operation
        match op {
            BenchOp::Insert => self.insert_times.push(timer.elapsed()),
            BenchOp::Get => self.get_times.push(timer.elapsed()),
        }
    }
}

/// A benchmarking tool for Shoal
pub struct Bencher {
    /// The total timer start time
    total_timer_start: Instant,
    /// The total timer end time
    total_timer_end: Option<Instant>,
    /// The insert times gathered from all of our workers
    insert_times: Vec<Duration>,
    /// The get times gathered from all of our workers
    get_times: Vec<Duration>,
    /// The path to write our benchmarks too
    path: PathBuf,
    /// The last benchmark run
    prior: Option<BenchResult>,
    /// The number of rows this run inserted
    inserted: u64,
    /// The number of rows this run read back
    retrieved: u64,
    /// The name this run was captured under, if it was given one
    label: Option<String>,
}

impl Bencher {
    /// Create a new bencher and load old results from disk if they exist
    ///
    /// A baseline that cannot be read is warned about and ignored rather than aborting the
    /// run, since losing a whole benchmark to a stale file is not a good trade. A baseline
    /// that reads cleanly but carries a different [`BASELINE_VERSION`] is refused for the
    /// opposite reason: it would produce a comparison that looks valid and is not.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to load old results from
    /// * `instances` - The number of instances this bencher will time
    pub fn new<P: AsRef<Path>>(path: P, instances: usize) -> Self {
        // check if we have any prior benchmark results
        let prior = match std::fs::read(&path) {
            Ok(buff) => Self::load_prior(&buff, path.as_ref()),
            // no baseline on disk is the normal case for a first run
            Err(_) => None,
        };
        // build our bencher
        Bencher {
            total_timer_start: Instant::now(),
            total_timer_end: None,
            insert_times: Vec::with_capacity(instances),
            get_times: Vec::with_capacity(instances),
            path: path.as_ref().to_path_buf(),
            prior,
            inserted: 0,
            retrieved: 0,
            label: None,
        }
    }

    /// Try to deserialize a baseline, warning instead of failing if we cannot
    ///
    /// # Arguments
    ///
    /// * `buff` - The raw baseline bytes
    /// * `path` - The path we read those bytes from, for the warning
    fn load_prior(buff: &[u8], path: &Path) -> Option<BenchResult> {
        // deserialize our baseline
        let prior = match serde_json::from_slice::<BenchResult>(buff) {
            Ok(prior) => prior,
            Err(error) => {
                eprintln!(
                    "warning: ignoring unreadable baseline at {}: {error}",
                    path.display()
                );
                return None;
            }
        };
        // refuse a baseline that was written against a different definition of a sample
        //
        // this is the dangerous case rather than the loud one. a file that fails to parse
        // costs a comparison; a file that parses and is not comparable costs a wrong answer.
        if prior.version != BASELINE_VERSION {
            eprintln!(
                "warning: ignoring baseline at {}: it is version {} and this build writes \
                 version {}, so the two are not comparable",
                path.display(),
                prior.version,
                BASELINE_VERSION
            );
            return None;
        }
        Some(prior)
    }

    /// Get a new bench worker
    ///
    /// # Arguments
    ///
    /// * `instances` - The estimated number of instances times this worker will gather
    pub fn worker(&self, instances: usize) -> BenchWorker {
        BenchWorker {
            insert_times: Vec::with_capacity(instances),
            get_times: Vec::with_capacity(instances),
        }
    }

    /// Merge a workers times into our own
    ///
    /// # Arguments
    ///
    /// * `worker` - The worker to merge in
    pub fn merge_worker(&mut self, mut worker: BenchWorker) {
        self.insert_times.append(&mut worker.insert_times);
        self.get_times.append(&mut worker.get_times);
    }

    /// Merge workers times into our own
    ///
    /// # Arguments
    ///
    /// * `workers` - The workers to merge in
    pub fn merge_workers(&mut self, workers: Vec<BenchWorker>) {
        // step over all workers and merge them in
        for worker in workers {
            // merge in this worker
            self.merge_worker(worker);
        }
    }

    /// Reset our total time start
    pub fn reset_total_time_start(&mut self) {
        self.total_timer_start = Instant::now()
    }

    /// Get a timer instance
    pub fn get_timer(&self) -> Instant {
        Instant::now()
    }

    /// Stop our total time now
    pub fn stop_total(&mut self) {
        self.total_timer_end = Some(Instant::now());
    }

    /// Print the latest benchmark results to screen
    ///
    /// # Arguments
    ///
    /// * `result` - The result to print
    pub fn print(&self, result: &BenchResult) {
        // print each operations distribution against its baseline
        result.insert.print("insert", self.prior.as_ref().map(|prior| &prior.insert));
        result.get.print("get", self.prior.as_ref().map(|prior| &prior.get));
        // print our total wall clock time
        println!("total");
        match &self.prior {
            Some(prior) => {
                print_bench!("wall clock", prior.total, result.total);
            }
            None => println!("  wall clock: {:.2?}", result.total),
        }
        // print the throughput this run sustained over that wall clock
        println!(
            "  rows/sec: {:.0} ({} inserted, {} read)",
            result.rows_per_sec(),
            result.inserted,
            result.retrieved
        );
    }

    /// Record how many rows this run moved
    ///
    /// These are counted by the caller rather than derived from the sample counts, because a
    /// sample is a batch and a batch is many rows.
    ///
    /// # Arguments
    ///
    /// * `inserted` - The number of rows this run inserted
    /// * `retrieved` - The number of rows this run read back
    pub fn set_row_counts(&mut self, inserted: u64, retrieved: u64) {
        self.inserted = inserted;
        self.retrieved = retrieved;
    }

    /// Name this run so its recorded result can be identified later
    ///
    /// # Arguments
    ///
    /// * `label` - The name to record this run under
    pub fn set_label(&mut self, label: impl Into<String>) {
        self.label = Some(label.into());
    }

    /// Summarize this run into a result
    fn summarize(&mut self) -> BenchResult {
        // get the total amount of time that this benchmark took
        let total = match self.total_timer_end {
            Some(end) => end.duration_since(self.total_timer_start),
            // we don't have an end time set so just use until now
            None => self.total_timer_start.elapsed(),
        };
        // build a distribution for each kind of operation
        BenchResult {
            version: BASELINE_VERSION,
            label: self.label.clone(),
            insert: Stats::from_times(&mut self.insert_times),
            get: Stats::from_times(&mut self.get_times),
            total,
            inserted: self.inserted,
            retrieved: self.retrieved,
        }
    }

    /// Get our total times and write them to disk if needed
    ///
    /// # Arguments
    ///
    /// * `write` - Whether to record this run as the new baseline
    /// * `json` - An extra path to write this run to, whatever `write` says
    pub fn finish(&mut self, write: bool, json: Option<&Path>) {
        // summarize every sample we gathered
        let result = self.summarize();
        // print our results
        self.print(&result);
        // serialize this run once for whichever of the two destinations want it
        let encoded =
            serde_json::to_vec_pretty(&result).expect("Failed to serialize benchmark result");
        // write a new benchmark to disk if requested
        if write {
            // write our benchmark to disk
            std::fs::write(&self.path, &encoded).expect("Failed to write benchmark to disk");
            println!("recorded baseline at {}", self.path.display());
        }
        // archive this run wherever the caller asked us to, if it asked
        if let Some(json) = json {
            // write this run out for whatever is collecting it
            std::fs::write(json, &encoded).expect("Failed to write benchmark json to disk");
            println!("wrote run to {}", json.display());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{percentile, Bencher, Stats};
    use std::time::Duration;

    /// Build 100 samples of 1ms through 100ms
    fn samples() -> Vec<Duration> {
        (1..=100).map(Duration::from_millis).collect()
    }

    #[test]
    /// Percentiles use nearest rank against a 0 indexed sample set
    ///
    /// The rank of a percentile is `ceil(n * p)` and is 1 based, so indexing a 0
    /// based vec with it directly reports the sample one rank too high.
    fn percentile_uses_nearest_rank() {
        let sorted = samples();
        assert_eq!(percentile(&sorted, 0.50), Duration::from_millis(50));
        assert_eq!(percentile(&sorted, 0.90), Duration::from_millis(90));
        assert_eq!(percentile(&sorted, 0.95), Duration::from_millis(95));
        assert_eq!(percentile(&sorted, 0.99), Duration::from_millis(99));
    }

    #[test]
    /// The extremes of a percentile range stay inside the sample set
    fn percentile_clamps_at_both_ends() {
        let sorted = samples();
        assert_eq!(percentile(&sorted, 0.0), Duration::from_millis(1));
        assert_eq!(percentile(&sorted, 1.0), Duration::from_millis(100));
    }

    #[test]
    /// A single sample is its own percentile
    fn percentile_of_one_sample() {
        let sorted = vec![Duration::from_millis(7)];
        assert_eq!(percentile(&sorted, 0.50), Duration::from_millis(7));
        assert_eq!(percentile(&sorted, 0.99), Duration::from_millis(7));
    }

    #[test]
    /// A distribution summarizes its samples without needing them pre sorted
    fn stats_summarize_unsorted_samples() {
        let mut times = samples();
        times.reverse();
        let stats = Stats::from_times(&mut times);
        assert_eq!(stats.count, 100);
        assert_eq!(stats.min, Duration::from_millis(1));
        assert_eq!(stats.max, Duration::from_millis(100));
        assert_eq!(stats.p50, Duration::from_millis(50));
        assert_eq!(stats.p99, Duration::from_millis(99));
        // 1..=100 averages to 50.5ms
        assert_eq!(stats.avg, Duration::from_micros(50_500));
    }

    #[test]
    /// An empty sample set yields a zeroed distribution rather than panicking
    ///
    /// Splitting insert and get latencies makes this reachable: a run that only
    /// inserts leaves the get set empty.
    fn stats_handle_no_samples() {
        let stats = Stats::from_times(&mut Vec::new());
        assert_eq!(stats.count, 0);
        assert_eq!(stats.max, Duration::ZERO);
        assert_eq!(stats.p99, Duration::ZERO);
    }

    #[test]
    /// An unreadable baseline is ignored rather than aborting the run
    fn unreadable_baseline_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("garbage.benchmark");
        // write bytes that are not a valid baseline
        std::fs::write(&path, b"this is not a baseline").unwrap();
        let bencher = Bencher::new(&path, 16);
        assert!(bencher.prior.is_none());
    }

    #[test]
    /// A missing baseline is the normal first run case
    fn missing_baseline_is_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let bencher = Bencher::new(dir.path().join("does-not-exist"), 16);
        assert!(bencher.prior.is_none());
    }

    #[test]
    /// A baseline written by this build round trips back into a comparison
    fn a_written_baseline_is_loaded_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("round-trip.benchmark");
        // record a run as the baseline
        let mut bencher = Bencher::new(&path, 16);
        bencher.set_row_counts(100, 25);
        bencher.set_label("round-trip");
        bencher.stop_total();
        bencher.finish(true, None);
        // a fresh bencher over the same path should pick it up
        let reloaded = Bencher::new(&path, 16);
        let prior = reloaded.prior.expect("a freshly written baseline did not load");
        assert_eq!(prior.version, super::BASELINE_VERSION);
        assert_eq!(prior.label.as_deref(), Some("round-trip"));
        assert_eq!(prior.inserted, 100);
        assert_eq!(prior.retrieved, 25);
    }

    #[test]
    /// A baseline from a different schema version is refused rather than compared
    ///
    /// This is the case that matters. A file that fails to parse costs a comparison; a file
    /// that parses cleanly while measuring something else costs a wrong answer, which is what
    /// the old `.benchmark` did when it was compared against a build whose insert
    /// acknowledgement was no longer inert.
    fn a_baseline_from_another_version_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("old-version.benchmark");
        // write a well formed baseline that claims a version we do not write
        let zeroed = r#"{"count":0,"max":{"secs":0,"nanos":0},"p99":{"secs":0,"nanos":0},
            "p95":{"secs":0,"nanos":0},"p90":{"secs":0,"nanos":0},"p50":{"secs":0,"nanos":0},
            "avg":{"secs":0,"nanos":0},"min":{"secs":0,"nanos":0}}"#;
        let body = format!(
            r#"{{"version":{},"label":"ancient","insert":{zeroed},"get":{zeroed},
                "total":{{"secs":1,"nanos":0}},"inserted":1,"retrieved":1}}"#,
            super::BASELINE_VERSION + 1
        );
        std::fs::write(&path, body).unwrap();
        // it parses, and it is still refused
        let bencher = Bencher::new(&path, 16);
        assert!(
            bencher.prior.is_none(),
            "a baseline from another version was accepted for comparison"
        );
    }

    #[test]
    /// Throughput is counted from the rows a run moved, not from its sample count
    fn throughput_counts_rows_not_samples() {
        let dir = tempfile::tempdir().unwrap();
        let mut bencher = Bencher::new(dir.path().join("throughput"), 16);
        // record two seconds of wall clock carrying 3000 rows
        bencher.set_row_counts(2000, 1000);
        let mut result = bencher.summarize();
        result.total = Duration::from_secs(2);
        // 3000 rows over 2 seconds is 1500 rows a second
        assert!((result.rows_per_sec() - 1500.0).abs() < f64::EPSILON);
    }

    #[test]
    /// A run that took no measurable time reports no rate rather than dividing by zero
    fn throughput_of_an_instant_run_is_zero() {
        let dir = tempfile::tempdir().unwrap();
        let mut bencher = Bencher::new(dir.path().join("instant"), 16);
        bencher.set_row_counts(10, 10);
        let mut result = bencher.summarize();
        result.total = Duration::ZERO;
        assert_eq!(result.rows_per_sec(), 0.0);
    }
}
