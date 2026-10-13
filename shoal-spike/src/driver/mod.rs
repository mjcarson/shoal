//! X13: the benchmark's shape, measured
//!
//! [Q30](../../../docs/src/object-storage/contract.md) asks how the driver gains object operations,
//! byte metrics and an object dataset. F69 answered the first two; X13
//! (`docs/src/object-storage/spikes.md#x13-the-benchmarks-shape`) answers the rest: the dataset's
//! two shapes, a folder of real files and a seeded description, and how fast one core makes
//! seeded bytes and checksums them, measured against a server that discards.
//!
//! - `shoal-spike driver all --dir <scratch> [flags]` runs every section, the server in the same
//!   process, over loopback
//! - `shoal-spike driver serve --certs <dir>` runs the server alone, until killed, and
//!   `shoal-spike driver drive --addr <host> --certs <dir>` runs the streams against it
//! - `shoal-spike driver certs --out <dir>` makes the certificate both ends use
//! - `shoal-spike driver report <records.json…>` merges rounds and judges the three triggers
//!
//! Its sections:
//!
//! - `check`: every generator meets its published reference, seeks, and digests fixed cells alike
//!   on every host and build; and how fast a description expands to its object list
//! - `make`: each generator alone on one core, filling, with the unit's CRC, and checking
//! - `make-cores`: the same on several cores at once, with no wire
//! - `streams`: a put and a get through X11's server, which discards or answers from memory
//! - `cores`: the put from several client cores at once
//! - `folder`: a folder of real files read cold and hot
//!
//! Like every spike's code it is thrown away; what M13 builds from it is on the record page.

pub mod dataset;
pub mod folder;
pub mod generate;
pub mod make;
pub mod report;
pub mod stub;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use self::dataset::BodySource;
use self::generate::Generator;
use self::stub::Stub;
use crate::device::{facts, record, SideOut};
use crate::stream::client::{Rt, Target};
use crate::stream::server::{self, ServerConf};
use crate::stream::wire;

/// The seed every generator and description is drawn from: "X13"
pub const SEED: u64 = 0x5831_3133;

/// The plaintext port of the server's executor zero: clear of X11's ports, Shoal's and the
/// ephemeral range
pub const BASE_PORT: u16 = 13_200;

/// Every section, in the order a round runs them
pub const SECTIONS: &[&str] = &["check", "make", "make-cores", "streams", "cores", "folder"];

/// The bytes the folder's files of each size hold together
pub const FOLDER_BYTES: u64 = 2 << 30;

/// What every section is run with
pub struct Ctx {
    /// Whether this is a quick run, which measures nothing
    pub quick: bool,
    /// The leg, naming the host and the build, or the two hosts across the network
    pub leg: String,
    /// The line every table carries
    pub label: String,
    /// Where the server is, or where the folder is: what a record's `fs` holds
    pub place: String,
    /// Where records are written
    pub out: Option<PathBuf>,
    /// The cores one making thread, then more, are pinned to
    pub make_cores: Vec<usize>,
    /// How many making threads the cores section runs at once
    pub make_counts: Vec<usize>,
    /// How many client streams section 3b runs at once
    pub stream_counts: Vec<usize>,
    /// The bytes the folder's files of each size hold
    pub folder_bytes: u64,
}

impl Ctx {
    /// How many times each side of a section one cell is timed
    #[must_use]
    pub fn runs(&self) -> usize {
        3
    }

    /// The least time one timing of a section one side runs for
    #[must_use]
    pub fn budget(&self) -> Duration {
        if self.quick {
            Duration::from_millis(30)
        } else {
            Duration::from_millis(300)
        }
    }

    /// The warm-up before a stream's window
    #[must_use]
    pub fn warmup(&self) -> Duration {
        if self.quick {
            Duration::from_millis(300)
        } else {
            Duration::from_secs(3)
        }
    }

    /// A stream's measured window, and the cores section's
    #[must_use]
    pub fn window(&self) -> Duration {
        if self.quick {
            Duration::from_millis(700)
        } else {
            Duration::from_secs(5)
        }
    }
}

/// The value after a flag, if the flag is given
///
/// # Arguments
///
/// * `args` - The arguments
/// * `flag` - The flag
fn value_of(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|arg| arg == flag)
        .and_then(|at| args.get(at + 1))
        .cloned()
}

/// A comma separated list after a flag, if the flag is given
///
/// # Arguments
///
/// * `args` - The arguments
/// * `flag` - The flag
fn list_of<T: std::str::FromStr>(args: &[String], flag: &str) -> Option<Vec<T>> {
    value_of(args, flag).map(|list| {
        list.split(',')
            .map(|item| item.parse().unwrap_or_else(|_| panic!("{flag} takes a list")))
            .collect()
    })
}

/// The cores each part of a run is pinned to, from flags or from the host's cpu
///
/// A 7945HX keeps to its second CCD, cores 8 to 15, clear of cpu 0's core. A Zen1 host of four
/// cores numbers each core's two threads beside each other, so 6, 4, 2 and 0 are four cores.
struct Cores {
    /// The server's executors
    server: Vec<usize>,
    /// One client stream each, in order
    client: Vec<usize>,
    /// The runtime that asks for counters
    control: usize,
    /// One making thread each, in order
    make: Vec<usize>,
}

/// The cores of this host
///
/// # Arguments
///
/// * `args` - The arguments
fn cores(args: &[String]) -> Cores {
    let europa = crate::placement::timing::cpu_model().contains("7945HX");
    let default = if europa {
        Cores {
            server: vec![8, 9, 12, 13],
            client: vec![10, 11, 14, 15],
            control: 7,
            make: vec![10, 11, 14, 15],
        }
    } else {
        Cores {
            server: vec![2, 4],
            client: vec![6, 1],
            control: 0,
            make: vec![6, 4, 2, 0],
        }
    };
    Cores {
        server: list_of(args, "--server-cores").unwrap_or(default.server),
        client: list_of(args, "--client-cores").unwrap_or(default.client),
        control: value_of(args, "--control-core").map_or(default.control, |core| core.parse().expect("--control-core takes a cpu")),
        make: list_of(args, "--make-cores").unwrap_or(default.make),
    }
}

/// The build this binary is: the cpu it was compiled for and the features that gave it
#[must_use]
pub fn build() -> String {
    // the target cpu from the flags cargo compiled the crate with, written `-C target-cpu=x` on a
    // command line and `-Ctarget-cpu=x` in the workspace's config
    let flags = env!("SPIKE_RUSTFLAGS");
    let cpu = flags
        .split_whitespace()
        .find_map(|flag| flag.find("target-cpu=").map(|at| &flag[at + "target-cpu=".len()..]))
        .unwrap_or("default")
        .to_string();
    let mut features = Vec::new();
    if cfg!(target_feature = "avx2") {
        features.push("avx2");
    }
    if cfg!(target_feature = "avx512f") {
        features.push("avx512f");
    }
    if cfg!(target_feature = "vaes") {
        features.push("vaes");
    }
    if cfg!(target_feature = "aes") {
        features.push("aes");
    }
    if cfg!(target_feature = "sha") {
        features.push("sha");
    }
    format!("{cpu} ({})", features.join(" "))
}

/// The target cpu alone, which names a leg
#[must_use]
fn build_cpu() -> String {
    build().split_whitespace().next().unwrap_or("default").to_string()
}

/// The line every table carries
///
/// # Arguments
///
/// * `leg` - The leg
/// * `quick` - Whether this is a quick run
fn label(leg: &str, quick: bool) -> String {
    let kernel = std::fs::read_to_string("/proc/sys/kernel/osrelease").unwrap_or_default();
    let tier = crc_fast::get_calculator_target(crc_fast::CrcAlgorithm::Crc64Nvme);
    let quick = if quick { " · **quick: not a measurement**" } else { "" };
    format!(
        "{} · {} · governor {} · kernel {} · build {} · crc {tier} · leg {leg}{quick}",
        crate::hostname(),
        crate::placement::timing::cpu_model(),
        crate::governor(),
        kernel.trim(),
        build(),
    )
}

/// Run `shoal-spike driver` with the arguments after the subcommand
///
/// # Arguments
///
/// * `args` - The arguments after `driver`
pub fn main(args: &[String]) {
    let command = args.first().map(String::as_str).unwrap_or("help");
    match command {
        "report" => {
            let quick = args.iter().any(|arg| arg == "--quick");
            let paths: Vec<String> = args[1..].iter().filter(|arg| *arg != "--quick").cloned().collect();
            print!("{}", report::report(&record::read_all(&paths), quick));
        }
        "certs" => {
            let out = PathBuf::from(value_of(args, "--out").expect("--out names the certificate directory"));
            crate::stream::make_certs(&out);
        }
        "serve" => serve(args),
        "all" | "quick" | "drive" => run(command, args),
        _ => println!(
            "shoal-spike driver <all|quick|serve|drive|certs|report> [--dir <scratch>] [--certs <dir>] \
             [--addr <host>] [--sections check,make,make-cores,streams,cores,folder] [--round R | --rounds N] \
             [--leg <name>] [--out f.json] [--server-cores ..] [--client-cores ..] [--make-cores ..] [--quick]"
        ),
    }
}

/// The server's configuration: X11's server with a pattern for each generator
///
/// # Arguments
///
/// * `args` - The arguments
/// * `cores` - Its executors' cpus
/// * `certs` - The certificate's directory
/// * `generators` - The generators whose patterns it serves
fn server_conf(args: &[String], cores: &[usize], certs: &std::path::Path, generators: &[Arc<dyn Generator>]) -> ServerConf {
    ServerConf {
        cores: cores.iter().map(|&cpu| (cpu, facts::sibling(cpu))).collect(),
        bind: value_of(args, "--bind").unwrap_or_else(|| "0.0.0.0".to_string()),
        base_port: value_of(args, "--port").map_or(BASE_PORT, |port| port.parse().expect("--port takes a port")),
        // no file: a write is dropped, a read answered from a pattern
        dir: None,
        file_bytes: 1 << 30,
        cert: Some((certs.join("cert.pem"), certs.join("key.pem"))),
        seed: SEED,
        patterns: stub::patterns(generators),
    }
}

/// Run the server alone until the process is killed
///
/// # Arguments
///
/// * `args` - The arguments
fn serve(args: &[String]) {
    let cores = cores(args);
    let certs = PathBuf::from(value_of(args, "--certs").expect("serve needs --certs"));
    let generators = generate::all(SEED);
    let conf = server_conf(args, &cores.server, &certs, &generators);
    for handle in server::start(&conf) {
        let _ = handle.join();
    }
}

/// Run the sections: every one with a server here for `all` and `quick`, or the streams against
/// a server elsewhere for `drive`
///
/// # Arguments
///
/// * `command` - `all`, `quick` or `drive`
/// * `args` - The arguments
fn run(command: &str, args: &[String]) {
    let quick = command == "quick" || args.iter().any(|arg| arg == "--quick");
    let local = command != "drive";
    let cores = cores(args);
    let generators = generate::all(SEED);
    // the sections asked for, and the ones a drive can run
    let default: Vec<String> = if local {
        SECTIONS.iter().map(|section| (*section).to_string()).collect()
    } else {
        vec!["streams".to_string()]
    };
    let sections: Vec<String> = list_of(args, "--sections").unwrap_or(default);
    let wants = |name: &str| sections.iter().any(|want| want == name);
    let dir = value_of(args, "--dir").map(PathBuf::from);
    let leg = value_of(args, "--leg").unwrap_or_else(|| format!("{} {}", crate::hostname(), build_cpu()));
    let place = match (&dir, value_of(args, "--addr")) {
        (_, Some(addr)) => addr,
        (Some(dir), None) => dir.display().to_string(),
        (None, None) => "memory".to_string(),
    };
    let europa = crate::placement::timing::cpu_model().contains("7945HX");
    let ctx = Ctx {
        quick,
        label: label(&leg, quick),
        leg,
        place,
        out: value_of(args, "--out").map(PathBuf::from),
        make_counts: if europa { vec![1, 2, 4] } else { vec![1, 2, 4] },
        stream_counts: (1..=cores.client.len()).filter(|count| count.is_power_of_two()).collect(),
        make_cores: cores.make.clone(),
        folder_bytes: if quick { 64 << 20 } else { FOLDER_BYTES },
    };
    // the server and the client's runtimes, when a section streams
    let stub = (wants("streams") || wants("cores")).then(|| start_stub(args, local, &cores, &generators));
    let rounds: Vec<u32> = match value_of(args, "--round") {
        Some(round) => vec![round.parse().expect("--round takes a number")],
        None => (1..=value_of(args, "--rounds").map_or(1, |n| n.parse().expect("--rounds takes a count"))).collect(),
    };
    let mut next_id = 1u64;
    for round in rounds {
        for &name in SECTIONS.iter().filter(|name| wants(name)) {
            let started = Instant::now();
            let (title, outs) = match name {
                "check" => ("Checks", pinned(cores.make[0], || check(&generators))),
                "make" => ("1. Making bytes on one core, GiB/s", pinned(cores.make[0], || make::section(&ctx, &generators, round))),
                "make-cores" => ("3a. Making bytes on several cores, no wire", make::cores_section(&ctx, &generators, round)),
                "streams" => {
                    let stub = stub.as_ref().expect("the streams start the server");
                    ("2. Against a server that discards", stub::streams_section(&ctx, stub, round, &mut next_id))
                }
                "cores" => {
                    let stub = stub.as_ref().expect("the cores start the server");
                    ("3b. The put from several client cores", stub::cores_section(&ctx, stub, round, &mut next_id))
                }
                "folder" => {
                    let dir = dir.clone().expect("the folder needs --dir");
                    ("4. A folder of real files", pinned(cores.make[0], || folder::section(&ctx, &dir, round)))
                }
                _ => unreachable!("every section is matched"),
            };
            print!("{}", report::section_table(title, &ctx.label, round, name, &outs));
            let records: Vec<record::Record> = outs
                .into_iter()
                .map(|out| record::Record {
                    host: crate::hostname(),
                    fs: ctx.place.clone(),
                    leg: ctx.leg.clone(),
                    measurement: name.to_string(),
                    cell: out.cell,
                    side: out.side,
                    round,
                    quick,
                    metrics: out.metrics,
                })
                .collect();
            if let Some(out) = &ctx.out {
                record::append(out, &records);
            }
            eprintln!("x13: {name} round {round} took {:.0} s", started.elapsed().as_secs_f64());
        }
    }
    // the runtimes stop, and a server started here ends with the process
    drop(stub);
    std::process::exit(0);
}

/// Run a section on a thread pinned to a core, and wait for it
///
/// # Arguments
///
/// * `cpu` - The core
/// * `section` - The section
fn pinned<F: FnOnce() -> Vec<SideOut> + Send>(cpu: usize, section: F) -> Vec<SideOut> {
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                crate::placement::timing::pin(cpu);
                section()
            })
            .join()
            .expect("a pinned section finishes")
    })
}

/// Start the server here, unless it is elsewhere, and the client's runtimes and controls
///
/// # Arguments
///
/// * `args` - The arguments
/// * `local` - Whether the server runs in this process
/// * `cores` - The cores
/// * `generators` - The generators
fn start_stub(args: &[String], local: bool, cores: &Cores, generators: &[Arc<dyn Generator>]) -> Stub {
    // the certificate: named, or made for this run
    let certs = match value_of(args, "--certs") {
        Some(dir) => PathBuf::from(dir),
        None => {
            let scratch = std::env::temp_dir().join(format!("x13-certs-{}", std::process::id()));
            crate::stream::make_certs(&scratch);
            scratch
        }
    };
    let executors = if local {
        let conf = server_conf(args, &cores.server, &certs, generators);
        // the server's executors live until the process ends
        std::mem::forget(server::start(&conf));
        cores.server.len()
    } else {
        value_of(args, "--executors").map_or(cores.server.len(), |count| count.parse().expect("--executors takes a count"))
    };
    let host = if local {
        "127.0.0.1".to_string()
    } else {
        value_of(args, "--addr").expect("drive needs --addr")
    };
    let base_port = value_of(args, "--port").map_or(BASE_PORT, |port| port.parse().expect("--port takes a port"));
    let target = Target::new(&host, base_port, Some(&certs.join("cert.pem")));
    stub::wait_listening(&target, executors);
    let control_rt = Rt::start(cores.control, "x13-control");
    let controls = stub::controls(&control_rt, &target, executors);
    let client_rts = cores
        .client
        .iter()
        .enumerate()
        .map(|(at, &cpu)| Rt::start(cpu, &format!("x13-client-{at}")))
        .collect();
    let own = Arc::new(wire::pattern(SEED));
    let ledger = Arc::new(stub::ledger(&own));
    Stub {
        target,
        control_rt,
        controls,
        client_rts,
        client_cpus: cores.client.clone(),
        own,
        ledger,
        generators: Arc::new(generators.to_vec()),
        same_host: local || args.iter().any(|arg| arg == "--same-host"),
        executors,
    }
}

/// The checks: every generator's reference, seek and digests, the CRC's check value, and how fast
/// each example description expands
///
/// # Arguments
///
/// * `generators` - The generators
#[must_use]
pub fn check(generators: &[Arc<dyn Generator>]) -> Vec<SideOut> {
    let mut outs = Vec::new();
    for generator in generators {
        // its reference, where it has one, and its seek
        if let Some(ok) = generate::meets_reference(generator.name()) {
            outs.push(SideOut::new(generator.name(), "reference", &[("ok", f64::from(u8::from(ok)))]));
        }
        let seeks = generate::seeks(generator.as_ref());
        outs.push(SideOut::new(generator.name(), "seek", &[("ok", f64::from(u8::from(seeks)))]));
        // the digests, as two halves each, since a figure is a float
        let digests = generate::digests(generator.as_ref());
        let mut figures: Vec<(String, f64)> = Vec::new();
        for (at, digest) in digests.iter().enumerate() {
            figures.push((format!("d{at}_hi"), (digest >> 32) as f64));
            figures.push((format!("d{at}_lo"), (digest & 0xffff_ffff) as f64));
        }
        let named: Vec<(&str, f64)> = figures.iter().map(|(name, value)| (name.as_str(), *value)).collect();
        outs.push(SideOut::new(generator.name(), "digest", &named));
        eprintln!(
            "x13: {} digests {}",
            generator.name(),
            digests.iter().map(|digest| format!("{digest:016x}")).collect::<Vec<_>>().join(" ")
        );
    }
    // the CRC's own check value, and the kernel it dispatches to
    let check = crc_fast::checksum(crc_fast::CrcAlgorithm::Crc64Nvme, b"123456789") == 0xae8b_1486_0a79_9888;
    let tier = crc_fast::get_calculator_target(crc_fast::CrcAlgorithm::Crc64Nvme);
    outs.push(SideOut::new(
        "crc64nvme",
        "reference",
        &[("ok", f64::from(u8::from(check))), ("vpclmulqdq", f64::from(u8::from(tier.contains("vpclmulqdq"))))],
    ));
    // each example description expanded to a million objects' paths and sizes
    let aes = generators
        .iter()
        .find(|generator| generator.name() == "aes-ctr")
        .expect("AES-128-CTR is measured")
        .clone();
    for described in dataset::examples(SEED, 1_000_000) {
        let dataset::ObjectDataset::Described(description) = dataset::ObjectDataset::Described(described) else {
            unreachable!("a description was just wrapped");
        };
        let started = Instant::now();
        let (objects, total) = description.expand();
        let secs = started.elapsed().as_secs_f64();
        // an object's bytes through the body source a driver reads, the generator's own
        let bodies = dataset::DescribedBodies {
            description: description.clone(),
            generator: aes.clone(),
        };
        let (mut through, mut direct) = (vec![0u8; 4096], vec![0u8; 4096]);
        bodies.fill(3, 4096, &mut through);
        aes.fill(3, 4096, &mut direct);
        let same = through == direct && bodies.len(3) == description.size_of(3);
        let shape = match &description.sizes {
            dataset::SizeDistribution::Fixed(_) => "fixed",
            dataset::SizeDistribution::Uniform { .. } => "uniform",
            dataset::SizeDistribution::Doublings { .. } => "doublings",
            dataset::SizeDistribution::Table(_) => "table",
        };
        eprintln!("x13: describe {shape} digest {}", &description.digest()[..16]);
        outs.push(SideOut::new(
            format!("describe {shape}"),
            "expand",
            &[
                ("objects_per_s", objects.len() as f64 / secs),
                ("total_gib", total as f64 / f64::from(1u32 << 30)),
                ("ok", f64::from(u8::from(same))),
            ],
        ));
    }
    outs
}
