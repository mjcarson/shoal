//! X11: streamed bodies, measured
//!
//! [Q26](../../../docs/src/object-storage/contract.md) asks how an object's bytes cross the
//! client wire: in what frame, under what window, and at what cost to a connection shared with
//! small queries. X11 (`docs/src/object-storage/spikes.md#x11-streamed-bodies`) answers it with a
//! glommio server and a tokio client exchanging frames of plain bytes, with and without the
//! kernel's TLS, read straight into buffers for direct I/O and written to a file, beside small
//! requests, and with a connection handed from one executor to another.
//!
//! - `shoal-spike stream all --dir <scratch> [flags]` runs the server and the client in one
//!   process, over loopback
//! - `shoal-spike stream serve --dir <scratch> --certs <dir>` runs the server alone, until killed,
//!   and `shoal-spike stream drive --addr <host> --certs <dir>` runs the client against it
//! - `shoal-spike stream certs --out <dir>` makes the certificate both ends use
//! - `shoal-spike stream report <records.json…>` merges rounds and judges the three triggers
//! - `--sections tail --streams read` repeats only the cells of the named sections whose stream
//!   runs in a named direction, as item 213's repeat of section 3's reads did
//!
//! Its frames are its own and are thrown away with it; the product's are written by S1's
//! prerequisite for more than one frame a query, and by M13.

pub mod client;
pub mod measure;
pub mod report;
pub mod server;
pub mod sock;
pub mod wire;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use self::client::{Conn, Dial, Rt, Target};
use self::measure::Ctx;
use self::server::ServerConf;
use self::wire::Setup;
use crate::device::{facts, record};

/// The seed of the pattern every stream carries, which both ends make for themselves
pub const SEED: u64 = 0x5831_3131;

/// The plaintext port of executor zero unless one is named: clear of Shoal's ports and of the
/// ephemeral range
pub const BASE_PORT: u16 = 13_100;

/// The size of each executor's file
pub const FILE_BYTES: u64 = 1 << 30;

/// Every section, in the order a round runs them
pub const SECTIONS: &[&str] = &["rate", "window", "tail", "route"];

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
            .map(|item| {
                item.parse()
                    .unwrap_or_else(|_| panic!("{flag} takes a list"))
            })
            .collect()
    })
}

/// Make a self signed certificate for `localhost`, the name every client asks for
///
/// # Arguments
///
/// * `dir` - Where `cert.pem` and `key.pem` go
pub fn make_certs(dir: &Path) {
    std::fs::create_dir_all(dir).expect("the certificate directory is made");
    let issued = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("a certificate is made");
    std::fs::write(dir.join("cert.pem"), issued.cert.pem()).expect("the certificate is written");
    std::fs::write(dir.join("key.pem"), issued.key_pair.serialize_pem())
        .expect("the key is written");
}

/// The server's cpus and the client's two, from flags or from the host's cpu
///
/// # Arguments
///
/// * `args` - The arguments
fn cpus(args: &[String]) -> (Vec<usize>, Vec<usize>) {
    // a 7945HX keeps clear of cpu 0's core and of the first CCD; a Zen1 host of four cores has
    // its siblings numbered beside each other, so 2, 4 and 6 are three physical cores
    let europa = crate::placement::timing::cpu_model().contains("7945HX");
    let server = list_of(args, "--server-cores").unwrap_or_else(|| {
        if europa {
            vec![8, 9]
        } else {
            vec![2, 4]
        }
    });
    let client = list_of(args, "--client-cores").unwrap_or_else(|| {
        if europa {
            vec![10, 11]
        } else {
            vec![6, 1]
        }
    });
    (server, client)
}

/// The line every table carries
///
/// # Arguments
///
/// * `leg` - The leg
/// * `server` - The server's host
/// * `quick` - Whether this is a quick run
fn label(leg: &str, server: &str, quick: bool) -> String {
    let kernel = std::fs::read_to_string("/proc/sys/kernel/osrelease").unwrap_or_default();
    let quick = if quick {
        " · **quick: not a measurement**"
    } else {
        ""
    };
    format!(
        "client {} · {} · governor {} · kernel {} · server {server} · leg {leg}{quick}",
        crate::hostname(),
        crate::placement::timing::cpu_model(),
        crate::governor(),
        kernel.trim(),
    )
}

/// Run `shoal-spike stream` with the arguments after the subcommand
///
/// # Arguments
///
/// * `args` - The arguments after `stream`
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
            make_certs(&out);
        }
        "serve" => serve(args),
        "all" | "quick" | "drive" => drive(command, args),
        _ => println!(
            "shoal-spike stream <all|quick|serve|drive|certs|report> [--dir <scratch>] [--certs <dir>] \
             [--addr <host>] [--port P] [--server-cores a,b] [--client-cores c,d] [--sections rate,window,tail,route] \
             [--net] [--server-file] [--round R | --rounds N] [--leg <name>] [--out f.json]"
        ),
    }
}

/// Run the server alone until the process is killed
///
/// # Arguments
///
/// * `args` - The arguments
fn serve(args: &[String]) {
    let (server, _) = cpus(args);
    let conf = server_conf(args, &server);
    let handles = server::start(&conf);
    for handle in handles {
        let _ = handle.join();
    }
}

/// The server's configuration from flags
///
/// # Arguments
///
/// * `args` - The arguments
/// * `cores` - Its cpus
fn server_conf(args: &[String], cores: &[usize]) -> ServerConf {
    let dir = value_of(args, "--dir").map(PathBuf::from);
    if let Some(dir) = &dir {
        std::fs::create_dir_all(dir).expect("the scratch directory is made");
    }
    let certs = value_of(args, "--certs").map(PathBuf::from);
    ServerConf {
        cores: cores
            .iter()
            .map(|&cpu| (cpu, facts::sibling(cpu)))
            .collect(),
        bind: value_of(args, "--bind").unwrap_or_else(|| "0.0.0.0".to_string()),
        base_port: value_of(args, "--port")
            .map_or(BASE_PORT, |port| port.parse().expect("--port takes a port")),
        dir,
        file_bytes: FILE_BYTES,
        cert: certs.map(|dir| (dir.join("cert.pem"), dir.join("key.pem"))),
        seed: SEED,
        patterns: Vec::new(),
    }
}

/// Run the client's rounds, against a server started here or one already running elsewhere
///
/// # Arguments
///
/// * `command` - `all`, `quick` or `drive`
/// * `args` - The arguments
fn drive(command: &str, args: &[String]) {
    let quick = command == "quick" || args.iter().any(|arg| arg == "--quick");
    let (server_cores, client_cores) = cpus(args);
    // the certificate: named, or made for this run
    let scratch_certs = std::env::temp_dir().join(format!("x11-certs-{}", std::process::id()));
    let certs = match value_of(args, "--certs") {
        Some(dir) => PathBuf::from(dir),
        None => {
            make_certs(&scratch_certs);
            scratch_certs.clone()
        }
    };
    // the server: here for `all`, or already listening at `--addr`
    let local = command != "drive";
    let mut serve_args = args.to_vec();
    serve_args.extend(["--certs".to_string(), certs.display().to_string()]);
    let conf = server_conf(&serve_args, &server_cores);
    let server_handles = if local {
        Some(server::start(&conf))
    } else {
        None
    };
    let host = if local {
        "127.0.0.1".to_string()
    } else {
        value_of(args, "--addr").expect("drive needs --addr")
    };
    let target = Target::new(&host, conf.base_port, Some(&certs.join("cert.pem")));
    // give the executors time to write their files ahead and listen
    wait_listening(&target, server_cores.len());
    let stream_rt = Rt::start(client_cores[0], "x11-stream");
    let small_rt = Rt::start(
        client_cores.get(1).copied().unwrap_or(client_cores[0]),
        "x11-small",
    );
    // a plaintext connection to every executor, for its counters
    let controls = (0..server_cores.len())
        .map(|executor| {
            let target = target.clone();
            let dial = Dial {
                executor,
                setup: Setup {
                    window: 1,
                    ..Setup::default()
                },
                ..Dial::default()
            };
            Arc::new(small_rt.run(async move { Conn::open(target, dial).await }))
        })
        .collect();
    let server_name = if local {
        crate::hostname()
    } else {
        host.clone()
    };
    let leg = value_of(args, "--leg")
        .unwrap_or_else(|| format!("{} to {server_name}", crate::hostname()));
    let ctx = Ctx {
        target,
        stream_rt,
        small_rt,
        controls,
        same_host: local || args.iter().any(|arg| arg == "--same-host"),
        server_file: if local {
            conf.dir.is_some()
        } else {
            args.iter().any(|arg| arg == "--server-file")
        },
        two_executors: server_cores.len() > 1,
        quick,
        net: args.iter().any(|arg| arg == "--net"),
        label: label(&leg, &server_name, quick),
        leg,
        out: value_of(args, "--out").map(PathBuf::from),
    };
    let rounds: Vec<u32> = match value_of(args, "--round") {
        Some(round) => vec![round.parse().expect("--round takes a number")],
        None => (1..=value_of(args, "--rounds")
            .map_or(1, |n| n.parse().expect("--rounds takes a count")))
            .collect(),
    };
    let sections: Vec<String> = list_of(args, "--sections")
        .unwrap_or_else(|| SECTIONS.iter().map(|s| (*s).to_string()).collect());
    // only the streams of these directions, when a run repeats part of a section (item 213)
    let streams: Option<Vec<String>> = list_of(args, "--streams");
    let only = |cells: Vec<Vec<measure::Cell>>| keep_streams(cells, streams.as_deref());
    let mut next_id = 0u64;
    for round in rounds {
        for name in SECTIONS
            .iter()
            .filter(|name| sections.iter().any(|want| want == *name))
        {
            let started = Instant::now();
            let records = match *name {
                "rate" => measure::section(
                    &ctx,
                    "1. Rate and cpu by frame",
                    &only(measure::rate_cells(&ctx)),
                    round,
                    &mut next_id,
                ),
                "window" => measure::section(
                    &ctx,
                    "2. The window",
                    &only(measure::window_cells(&ctx)),
                    round,
                    &mut next_id,
                ),
                "tail" => measure::section(
                    &ctx,
                    "3. A small request beside a stream",
                    &only(measure::tail_cells(&ctx)),
                    round,
                    &mut next_id,
                ),
                "route" => {
                    let mut records = Vec::new();
                    if ctx.two_executors {
                        // the connection handed over, checked both ways, before it is timed
                        for tls in [false, true] {
                            let out = measure::handoff_check(&ctx, tls);
                            println!(
                                "x11: handoff check ({}): handed {}, ktls after {}, ok {}",
                                out.side,
                                out.get("handed"),
                                out.get("ktls_after"),
                                out.get("ok")
                            );
                            records.push(measure::record(&ctx, "handoff", round, out));
                        }
                    }
                    records.extend(measure::section(
                        &ctx,
                        "4. Routes to the other executor",
                        &only(measure::route_cells(&ctx)),
                        round,
                        &mut next_id,
                    ));
                    records
                }
                _ => unreachable!("every section is matched"),
            };
            if let Some(out) = &ctx.out {
                record::append(out, &records);
            }
            eprintln!(
                "x11: {name} round {round} took {:.0} s",
                started.elapsed().as_secs_f64()
            );
        }
    }
    // the client's runtimes stop, and a server started here ends with the process
    drop(ctx);
    if server_handles.is_some() {
        let _ = std::fs::remove_dir_all(&scratch_certs);
        std::process::exit(0);
    }
}

/// Keep the rows of a section whose stream runs in a direction asked for
///
/// A row with no stream, a small request alone, is kept only when nothing was asked for.
///
/// # Arguments
///
/// * `cells` - The section's rows, each a cell's sides
/// * `streams` - The directions to keep, or every row when none were named
fn keep_streams(cells: Vec<Vec<measure::Cell>>, streams: Option<&[String]>) -> Vec<Vec<measure::Cell>> {
    // every row unless a run named the directions it repeats
    let Some(streams) = streams else {
        return cells;
    };
    cells
        .into_iter()
        .filter(|sides| {
            sides.first().and_then(|cell| cell.dir).is_some_and(|dir| {
                streams.iter().any(|want| want == dir.name())
            })
        })
        .collect()
}

/// Wait until every executor accepts a connection
///
/// # Arguments
///
/// * `target` - Where the server is
/// * `executors` - How many executors it has
fn wait_listening(target: &Target, executors: usize) {
    let start = Instant::now();
    for executor in 0..executors {
        let port = server::plain_port(target.base_port, executor);
        while std::net::TcpStream::connect((target.host.as_str(), port)).is_err() {
            assert!(
                start.elapsed().as_secs() < 300,
                "the server did not listen on {port}"
            );
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
}
