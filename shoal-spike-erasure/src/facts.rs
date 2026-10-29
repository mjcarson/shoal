//! The labels every table carries, the core a run is pinned to, and what each candidate does
//! beyond its speed: the threads it starts and what one call allocates

use crate::alloc_count;
use crate::buffers::Stripe;
use crate::codes::{self, Code, Layout};
use crate::record::{AllocFact, CodeFacts, Labels};

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

/// The instruction set extensions any candidate dispatches on, as detected now
fn features() -> Vec<String> {
    // the list is every extension a candidate's source names in its run-time dispatch
    let mut out = Vec::new();
    if std::arch::is_x86_feature_detected!("ssse3") {
        out.push("ssse3".to_string());
    }
    if std::arch::is_x86_feature_detected!("avx2") {
        out.push("avx2".to_string());
    }
    if std::arch::is_x86_feature_detected!("avx512f") {
        out.push("avx512f".to_string());
    }
    if std::arch::is_x86_feature_detected!("avx512bw") {
        out.push("avx512bw".to_string());
    }
    if std::arch::is_x86_feature_detected!("avx512vl") {
        out.push("avx512vl".to_string());
    }
    if std::arch::is_x86_feature_detected!("gfni") {
        out.push("gfni".to_string());
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
/// * `arena_mib` - The data arena's size
/// * `quick` - Whether this is the quick pass
/// * `pass` - The subcommand that is running
pub fn labels(
    core: usize,
    runs: usize,
    budget_ms: u64,
    arena_mib: usize,
    quick: bool,
    pass: &str,
) -> Labels {
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
        rse_arch: env!("SPIKE_RSE_ARCH").to_string(),
        rustc: env!("SPIKE_RUSTC").to_string(),
        features: features(),
        date,
        core,
        runs,
        budget_ms,
        arena_mib,
        quick,
        pass: pass.to_string(),
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

/// Count what one call of each operation allocates, at 4+2 and 64 KiB (4+1 for a code with one
/// parity chunk only), after a warm-up call
///
/// # Arguments
///
/// * `code` - The candidate
fn allocations(code: &mut dyn Code) -> Vec<AllocFact> {
    let unit = 64 * 1024;
    // 4+2 where the code runs it, and otherwise the same data chunks with one parity chunk
    let layout = if code.supports(Layout::new(4, 2), unit).is_ok() {
        Layout::new(4, 2)
    } else {
        Layout::new(4, 1)
    };
    // a candidate that cannot run the layout has nothing to count
    let shape = match code.prepare(layout, unit) {
        Ok(shape) => shape,
        Err(note) => {
            return vec![AllocFact {
                op: "all".to_string(),
                note: Some(note),
                ..AllocFact::default()
            }];
        }
    };
    let mut stripe = Stripe::new(layout.k, unit, shape.count, shape.len, 0xa110c);
    let mut out = Vec::new();
    // an encode, warmed once so lazily built state is not counted
    let mut encode = || {
        let (data, mut stored) = stripe.encode_refs();
        code.encode(&data, &mut stored)
    };
    let _ = encode();
    let (result, allocations, bytes) = alloc_count::count(&mut encode);
    out.push(AllocFact {
        op: "encode".to_string(),
        allocations,
        bytes,
        note: result.err(),
    });
    // a decode of one lost data unit, or one lost piece for a code that is not systematic
    let mut decode = || {
        let (mut data, mut stored) = stripe.refs_mut();
        code.decode(&mut data, &mut stored, &[0])
    };
    let _ = decode();
    let (result, allocations, bytes) = alloc_count::count(&mut decode);
    out.push(AllocFact {
        op: "decode-1".to_string(),
        allocations,
        bytes,
        note: result.err(),
    });
    // a rebuild of chunk zero
    let mut rebuild = || {
        let (mut data, mut stored) = stripe.refs_mut();
        code.rebuild(&mut data, &mut stored, 0)
    };
    let _ = rebuild();
    let (result, allocations, bytes) = alloc_count::count(&mut rebuild);
    out.push(AllocFact {
        op: "rebuild".to_string(),
        allocations,
        bytes,
        note: result.err(),
    });
    // an update of the whole of data unit zero to the bytes of unit one
    let mut update = || {
        let new = stripe.data[1].to_vec();
        let old = stripe.data[0].to_vec();
        let (_, mut stored) = stripe.refs_mut();
        code.update(0, &old, &new, &mut stored, 0..unit)
    };
    let _ = update();
    // the two copies above are the harness's, so count the call alone
    let new = stripe.data[1].to_vec();
    let old = stripe.data[0].to_vec();
    let (result, allocations, bytes) = alloc_count::count(|| {
        let (_, mut stored) = stripe.refs_mut();
        code.update(0, &old, &new, &mut stored, 0..unit)
    });
    out.push(AllocFact {
        op: "update".to_string(),
        allocations,
        bytes,
        note: result.err(),
    });
    out
}

/// Every candidate's facts: kernels, threads and allocations
pub fn run() -> Vec<CodeFacts> {
    let mut out = Vec::new();
    for mut code in codes::all() {
        // the threads before this candidate ran, then after
        let threads_before = threads();
        let allocations = allocations(code.as_mut());
        let threads_after = threads();
        // the one candidate that names the kernels it chose
        let kernels = if code.name() == "rusty_erasure" {
            let mut probe = codes::rusty::Rusty::default();
            probe.prepare(Layout::new(4, 2), 4096).ok();
            probe.kernels().map(str::to_string)
        } else {
            None
        };
        out.push(CodeFacts {
            code: code.name().to_string(),
            kernels,
            threads_before,
            threads_after,
            allocations,
        });
    }
    out
}
