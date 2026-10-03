//! The labels every table carries, the core a run is pinned to, and what each candidate does
//! beyond its speed: the kernel it says it chose, the threads it starts and what a call allocates

use crate::alloc_count;
use crate::buffers::AlignedBuf;
use crate::record::{AllocFact, Labels, SumFacts};
use crate::sums::{self, Splits, Sum};

/// Pin this thread, and so every candidate's work, to one core
///
/// # Arguments
///
/// * `core` - The core
pub fn pin(core: usize) -> std::io::Result<()> {
    // SAFETY: a cpu_set_t is plain data, zeroed is empty, and CPU_SET writes inside it
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(core, &mut set);
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Read a file to a trimmed string, or say it could not be read
///
/// # Arguments
///
/// * `path` - The file
fn read(path: &str) -> String {
    std::fs::read_to_string(path)
        .map(|text| text.trim().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
}

/// The instruction set extensions a candidate's dispatch names, as detected now
fn detected() -> Vec<String> {
    // each is checked by name, since the macro takes a literal
    let checks = [
        ("sse4.2", std::arch::is_x86_feature_detected!("sse4.2")),
        ("pclmulqdq", std::arch::is_x86_feature_detected!("pclmulqdq")),
        ("aes", std::arch::is_x86_feature_detected!("aes")),
        ("avx2", std::arch::is_x86_feature_detected!("avx2")),
        ("avx512f", std::arch::is_x86_feature_detected!("avx512f")),
        ("avx512vl", std::arch::is_x86_feature_detected!("avx512vl")),
        ("vpclmulqdq", std::arch::is_x86_feature_detected!("vpclmulqdq")),
        ("vaes", std::arch::is_x86_feature_detected!("vaes")),
    ];
    // keep the names of those present
    checks
        .into_iter()
        .filter(|(_, present)| *present)
        .map(|(name, _)| name.to_string())
        .collect()
}

/// The same extensions, as the compiler was allowed to assume them for this build
fn compiled() -> Vec<String> {
    // cfg! is decided at compile time, which is the point
    let checks = [
        ("sse4.2", cfg!(target_feature = "sse4.2")),
        ("pclmulqdq", cfg!(target_feature = "pclmulqdq")),
        ("aes", cfg!(target_feature = "aes")),
        ("avx2", cfg!(target_feature = "avx2")),
        ("avx512f", cfg!(target_feature = "avx512f")),
        ("avx512vl", cfg!(target_feature = "avx512vl")),
        ("vpclmulqdq", cfg!(target_feature = "vpclmulqdq")),
        ("vaes", cfg!(target_feature = "vaes")),
    ];
    // keep the names of those assumed
    checks
        .into_iter()
        .filter(|(_, present)| *present)
        .map(|(name, _)| name.to_string())
        .collect()
}

/// The harness's own cargo features that this binary was built with
fn cargo_features() -> Vec<String> {
    // each one changes what a gxhash compiles to
    let mut out = Vec::new();
    if cfg!(feature = "gxhash2-avx2") {
        out.push("gxhash2-avx2".to_string());
    }
    if cfg!(feature = "gxhash3-hybrid") {
        out.push("gxhash3-hybrid".to_string());
    }
    out
}

/// The labels for a run
///
/// # Arguments
///
/// * `core` - The core the run is pinned to
/// * `runs` - Measurements a speed cell
/// * `budget_ms` - The least time a measurement runs for
/// * `arena_mib` - The cold arena's size
/// * `quick` - Whether this is the quick pass
pub fn labels(core: usize, runs: usize, budget_ms: u64, arena_mib: usize, quick: bool) -> Labels {
    // the cpu's model is the first model name line
    let cpu = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|text| {
            text.lines()
                .find(|line| line.starts_with("model name"))
                .and_then(|line| line.split(':').nth(1))
                .map(|model| model.trim().to_string())
        })
        .unwrap_or_else(|| "unknown".to_string());
    // the date from the system's own clock, in UTC
    let date = std::process::Command::new("date")
        .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        .output()
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    Labels {
        host: read("/proc/sys/kernel/hostname"),
        cpu,
        governor: read(&format!(
            "/sys/devices/system/cpu/cpu{core}/cpufreq/scaling_governor"
        )),
        target_cpu: env!("SPIKE_TARGET_CPU").to_string(),
        target_features: env!("SPIKE_TARGET_FEATURES").to_string(),
        cargo_features: cargo_features(),
        compiled: compiled(),
        detected: detected(),
        rustc: env!("SPIKE_RUSTC").to_string(),
        date,
        core,
        runs,
        budget_ms,
        arena_mib,
        quick,
    }
}

/// The process's thread count, from `/proc/self/status`
pub fn threads() -> u64 {
    read("/proc/self/status")
        .lines()
        .find_map(|line| line.strip_prefix("Threads:"))
        .and_then(|count| count.trim().parse().ok())
        .unwrap_or(0)
}

/// Count what one call of each operation allocates at 64 KiB, after a warm-up call
///
/// # Arguments
///
/// * `sum` - The candidate
/// * `unit` - The bytes to checksum
fn allocations(sum: &dyn Sum, unit: &[u8]) -> Vec<AllocFact> {
    let mut out = Vec::new();
    // one call first, so a table built on first use is not counted
    std::hint::black_box(sum.one_shot(unit));
    let (_, allocations, bytes) = alloc_count::count(|| std::hint::black_box(sum.one_shot(unit)));
    out.push(AllocFact {
        op: "one-shot".to_string(),
        allocations,
        bytes,
    });
    // the incremental interface in pieces of 4 KiB, where there is one
    let splits = Splits::Every(4096);
    if sum.stream(unit, &splits).is_some() {
        let (_, allocations, bytes) =
            alloc_count::count(|| std::hint::black_box(sum.stream(unit, &splits)));
        out.push(AllocFact {
            op: "stream-4k".to_string(),
            allocations,
            bytes,
        });
    }
    out
}

/// What every candidate does beyond its speed
pub fn run() -> Vec<SumFacts> {
    // one seeded unit of 64 KiB, the size every allocation is counted at
    let unit = AlignedBuf::seeded(64 * 1024, 0xfac7);
    let mut out = Vec::new();
    for sum in sums::all() {
        // the threads before and after it ran say whether it starts any
        let threads_before = threads();
        let allocations = allocations(sum.as_ref(), &unit);
        let threads_after = threads();
        out.push(SumFacts {
            sum: sum.name().to_string(),
            bits: sum.bits(),
            kernel: sum.kernel(),
            threads_before,
            threads_after,
            allocations,
        });
    }
    out
}
