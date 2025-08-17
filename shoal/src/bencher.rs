//! Benchmarks shoal

use owo_colors::OwoColorize;
use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rkyv::{Archive, Deserialize, Serialize};
use tokio::time::Instant;

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
        println!("{}: {:.2?} ({})", $name, $current, diff);
    };
}

/// A benchmarks past results
#[derive(Debug, Archive, Serialize, Deserialize)]
pub struct BenchResult {
    /// The max time seen in the last bench
    max: Duration,
    /// The p99 of times seen in the last bench
    p99: Duration,
    /// The p95 of times seen in the last bench
    p95: Duration,
    /// The p90 of times seen in the last bench
    p90: Duration,
    /// The p50 of times seen in the last bench
    p50: Duration,
    /// The average time seen in the last bench
    avg: Duration,
    // The lowest time seen in the last bench
    min: Duration,
    /// The total time the last run took
    total: Duration,
}

impl BenchResult {
    /// Build a prior bench
    ///
    /// # Arguments
    ///
    /// * `max` - The maximum time it took perform an action in a benchmark
    /// * `p99` - The p99 for time it took to perform an action in a benchmark
    /// * `p95` - The p95 for time it took to perform an action in a benchmark
    /// * `p90` - The p90 for time it took to perform an action in a benchmark
    /// * `p50` - The p50 for time it took to perform an action in a benchmark
    /// * `avg` - The average time it took to perform an action in a benchmark
    /// * `min` - The minimum time it took to perform an action in a benchmark
    /// * `total` - The total time it took to perform an action in a benchmark
    fn new(
        max: Duration,
        p99: Duration,
        p95: Duration,
        p90: Duration,
        p50: Duration,
        avg: Duration,
        min: Duration,
        total: Duration,
    ) -> Self {
        BenchResult {
            max,
            p99,
            p95,
            p90,
            p50,
            avg,
            min,
            total,
        }
    }
}

/// A bench worker for benching across workers/processes
pub struct BenchWorker {
    /// The timer for a specific instance
    instance: Option<Instant>,
    /// The instance trimes recorded by this bencher
    instance_times: Vec<Duration>,
}

impl BenchWorker {
    /// Start a new instance timer
    pub fn instance_start(&mut self) {
        self.instance = Some(Instant::now());
    }

    /// Start a new instance timer
    pub fn instance_stop(&mut self) {
        match self.instance.take() {
            Some(instance) => self.instance_times.push(instance.elapsed()),
            None => panic!("NO INSTANT TIMER ACTIVE?"),
        }
    }
}

/// A benchmarking tool for Shoal
pub struct Bencher {
    /// The total timer start time
    total_timer_start: Instant,
    /// The total timer end time
    total_timer_end: Option<Instant>,
    /// The timer for a specific instance
    instance: Option<Instant>,
    /// The instance trimes recorded by this bencher
    instance_times: Vec<Duration>,
    /// The path to write our benchmarks too
    path: PathBuf,
    /// The last branches run
    prior: Option<BenchResult>,
    /// Whether our time data has already been sorted
    sorted: bool,
}

impl Bencher {
    /// Create a new bencher and load old results from disk if they exist
    ///
    /// # Arguments
    ///
    /// * `path` - The path to load old results from
    /// * `instances` - The number of instances this bencher will time
    pub fn new<P: AsRef<Path>>(path: P, instances: usize) -> Self {
        // check if we have any prior benchmark results
        let prior = if std::fs::exists(&path).unwrap() {
            // load our prior results from disk
            let buff = std::fs::read(&path).unwrap();
            // unarchive our results
            let archive =
                rkyv::access::<ArchivedBenchResult, rkyv::rancor::Error>(&buff[..]).unwrap();
            // deserialize our prior results
            let prior = rkyv::deserialize::<BenchResult, rkyv::rancor::Error>(archive).unwrap();
            Some(prior)
        } else {
            None
        };
        // try to load our old benchmark from disk
        // build our bencher
        Bencher {
            total_timer_start: Instant::now(),
            total_timer_end: None,
            instance: None,
            instance_times: Vec::with_capacity(instances),
            path: path.as_ref().to_path_buf(),
            prior,
            sorted: false,
        }
    }

    /// Get a new bench worker
    ///
    /// # Arguments
    ///
    /// * `instances` - The estimated number of instances times this worker will gather
    pub fn worker(&self, instances: usize) -> BenchWorker {
        BenchWorker {
            instance: None,
            instance_times: Vec::with_capacity(instances),
        }
    }

    /// Merge a workers times into our own
    ///
    /// # Arguments
    ///
    /// * `worker` - The worker to merge in
    pub fn merge_worker(&mut self, mut worker: BenchWorker) {
        self.instance_times.append(&mut worker.instance_times);
    }

    /// Merge workers times into our own
    ///
    /// # Arguments
    ///
    /// * `worker` - The worker to merge in
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

    /// Start a new instance timer
    pub fn instance_start(&mut self) {
        self.instance = Some(Instant::now());
    }

    /// Start a new instance timer
    pub fn instance_stop(&mut self) {
        match self.instance.take() {
            Some(instance) => self.instance_times.push(instance.elapsed()),
            None => panic!("NO INSTANT TIMER ACTIVE?"),
        }
    }

    /// Stop our total time now
    pub fn stop_total(&mut self) {
        self.total_timer_end = Some(Instant::now());
    }

    /// Calculate the p99 for all of our times
    fn percentile(&mut self, percentile: f64) -> Duration {
        // skip sorting if our data has already been sorted
        if !self.sorted {
            // sort our times
            self.instance_times.sort_unstable();
            // mark that our data has already been sorted
            self.sorted = true;
        }
        // calculate the index for the item in the 99th percentile
        let index = (self.instance_times.len() as f64 * percentile).ceil() as usize;
        // get the time at the target percentile
        self.instance_times
            .get(index)
            .expect("Failed to get p{percentile}")
            .clone()
    }

    /// Print the latest benchmark results to screen
    pub fn print(&self, result: &BenchResult) {
        // if we have prior results then also log the difference
        if let Some(prior) = &self.prior {
            print_bench!("max", prior.max, result.max);
            print_bench!("p99", prior.p99, result.p99);
            print_bench!("p95", prior.p95, result.p95);
            print_bench!("p90", prior.p90, result.p90);
            print_bench!("p50", prior.p50, result.p50);
            print_bench!("average", prior.avg, result.avg);
            print_bench!("min", prior.min, result.min);
            print_bench!("total", prior.total, result.total);
        } else {
            println!("max: {:?}", result.max);
            println!("p99: {:?}", result.p99);
            println!("p95: {:?}", result.p95);
            println!("p90: {:?}", result.p90);
            println!("p50: {:?}", result.p50);
            println!("average: {:?}", result.avg);
            println!("min: {:?}", result.min);
            println!("total: {:?}", result.total);
        }
    }

    /// Get our total times and write them to disk if needed
    pub fn finish(&mut self, write: bool) {
        // get the total amount of time that this benchmark took
        let total = match self.total_timer_end {
            Some(end) => end.duration_since(self.total_timer_start),
            // we don't have an end time set so just use until now
            None => self.total_timer_start.elapsed(),
        };
        // find the average in nano seconds
        let sum = self
            .instance_times
            .iter()
            .map(|time| time.as_nanos())
            .sum::<u128>();
        // get our p99, p95, p90
        let p99 = self.percentile(0.99);
        let p95 = self.percentile(0.95);
        let p90 = self.percentile(0.90);
        let p50 = self.percentile(0.50);
        let avg = Duration::from_nanos((sum / self.instance_times.len() as u128) as u64);
        // find the maximum and minimum
        let max = self.instance_times.iter().max().unwrap();
        let min = self.instance_times.iter().min().unwrap();
        // build a benchmark result
        let result = BenchResult::new(*max, p99, p95, p90, p50, avg, *min, total);
        // print our results
        self.print(&result);
        // write a new benchmark to disk if requested
        if write {
            // serialize our latest benchmark
            let archived = rkyv::to_bytes::<rkyv::rancor::Error>(&result)
                .expect("Failed to serialize benchmark");
            // write our archived benchmark to disk
            std::fs::write(&self.path, archived).expect("Failed to write benchmark to disk")
        }
    }
}
