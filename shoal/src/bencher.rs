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
    /// * `avg` - The average time it took to perform an action in a benchmark
    /// * `min` - The minimum time it took to perform an action in a benchmark
    /// * `total` - The total time it took to perform an action in a benchmark
    fn new(max: Duration, avg: Duration, min: Duration, total: Duration) -> Self {
        BenchResult {
            max,
            avg,
            min,
            total,
        }
    }
}

/// A benchmarking tool for Shoal
pub struct Bencher {
    /// The total timer
    total_timer: Instant,
    /// The timer for a specific instance
    instance: Option<Instant>,
    /// The instance trimes recorded by this bencher
    instance_times: Vec<Duration>,
    /// The path to write our benchmarks too
    path: PathBuf,
    /// The last branches run
    prior: Option<BenchResult>,
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
            total_timer: Instant::now(),
            instance: None,
            instance_times: Vec::with_capacity(instances),
            path: path.as_ref().to_path_buf(),
            prior,
        }
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

    /// Print the latest benchmark results to screen
    pub fn print(&self, result: &BenchResult) {
        // if we have prior results then also log the difference
        if let Some(prior) = &self.prior {
            print_bench!("max", prior.max, result.max);
            print_bench!("average", prior.avg, result.avg);
            print_bench!("min", prior.min, result.min);
            print_bench!("total", prior.total, result.total);
        } else {
            println!("max: {:?}", result.max);
            println!("average: {:?}", result.avg);
            println!("min: {:?}", result.min);
            println!("total: {:?}", result.total);
        }
    }

    /// Get our total times and write them to disk if needed
    pub fn finish(&mut self, write: bool) {
        // find the average in nano seconds
        let sum = self
            .instance_times
            .iter()
            .map(|time| time.as_nanos())
            .sum::<u128>();
        let avg = Duration::from_nanos((sum / self.instance_times.len() as u128) as u64);
        // find the maximum and minimum
        let max = self.instance_times.iter().max().unwrap();
        let min = self.instance_times.iter().min().unwrap();
        // get the total time this test took
        let total = self.total_timer.elapsed();
        // build a benchmark result
        let result = BenchResult::new(*max, avg, *min, total);
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
