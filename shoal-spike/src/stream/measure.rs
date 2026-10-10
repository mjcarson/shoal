//! X11's four sections: what a stream moves and costs, what its window holds, what a small
//! request waits beside it, and whether its bytes can reach another executor cheaply
//!
//! Every cell is a stream on one connection, run for a warm-up and then a measured window, with
//! the server's counters and the client's read at both ends of the window. A side of a cell is
//! one way of running it - plaintext or kTLS, an arrangement of the small requests, a route to
//! the other executor - and a round runs every side of every cell once, in the order the round
//! gives them.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::client::{self, Conn, Dial, Pacer, Rt, Target};
use super::wire::{Route, Setup, Stats};
use crate::device::record::Record;
use crate::device::stats::Samples;
use crate::device::table::Table;
use crate::device::{ordered, size_name, SideOut};

/// The frame sizes swept on loopback
pub const FRAMES: &[usize] = &[64 << 10, 256 << 10, 1 << 20, 4 << 20, 8 << 20];

/// The frame sizes swept across the lab's network, where every one is bounded by the link
pub const NET_FRAMES: &[usize] = &[64 << 10, 1 << 20, 8 << 20];

/// The frame sizes a small request is measured beside on loopback
pub const TAIL_FRAMES: &[usize] = &[64 << 10, 256 << 10, 1 << 20, 8 << 20];

/// The frame sizes a small request is measured beside across the network
pub const NET_TAIL_FRAMES: &[usize] = &[64 << 10, 256 << 10, 1 << 20];

/// The windows swept, in frames
pub const WINDOWS: &[usize] = &[1, 2, 4, 8, 16];

/// The frame sizes the window is swept at
pub const WINDOW_FRAMES: &[usize] = &[256 << 10, 1 << 20, 4 << 20];

/// The window every section but the second runs at
pub const WINDOW: usize = 4;

/// The time between a small request's slots
pub const PERIOD: Duration = Duration::from_millis(1);

/// Which way a stream's bytes go
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dir {
    /// From the client to the server, into its file
    Write,
    /// From the server to the client, ranges the client asks for
    Read,
}

impl Dir {
    /// The direction's name in a table
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Dir::Write => "write",
            Dir::Read => "read",
        }
    }
}

/// How a connection is encrypted
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tls {
    /// Not at all
    Plain,
    /// rustls's handshake and the kernel's record layer, as Shoal does it
    Ktls,
    /// The same, with the receiver told records carry no padding
    NoPad,
}

impl Tls {
    /// The name in a table
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Tls::Plain => "plain",
            Tls::Ktls => "ktls",
            Tls::NoPad => "ktls-nopad",
        }
    }
}

/// Where a cell's small requests travel
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arr {
    /// On a connection of their own, with no stream running
    Alone,
    /// On the stream's own connection
    Shared,
    /// The same, with `TCP_NOTSENT_LOWAT` on both ends at this many bytes
    SharedLowat(u32),
    /// The same, with the server writing its frames first in first out
    SharedFifo,
    /// On a connection of their own to the stream's executor
    OwnSame,
    /// On a connection of their own to the other executor
    OwnOther,
}

impl Arr {
    /// The name in a table
    #[must_use]
    pub fn name(self) -> String {
        match self {
            Arr::Alone => "alone".to_string(),
            Arr::Shared => "shared".to_string(),
            Arr::SharedLowat(bytes) => format!("shared-lowat-{}", size_name(u64::from(bytes))),
            Arr::SharedFifo => "shared-fifo".to_string(),
            Arr::OwnSame => "own-same".to_string(),
            Arr::OwnOther => "own-other".to_string(),
        }
    }
}

/// One side of one cell
#[derive(Debug, Clone)]
pub struct Cell {
    /// The section it belongs to
    pub section: &'static str,
    /// The cell's name, which every side of it shares
    pub cell: String,
    /// The side's name
    pub side: String,
    /// Which way the stream goes, if one runs
    pub dir: Option<Dir>,
    /// Payload bytes a frame
    pub frame: usize,
    /// How the connections are encrypted
    pub tls: Tls,
    /// Whether the stream's bytes land in, or come from, the executor's file
    pub file: bool,
    /// Frames the receiver holds at once
    pub window: usize,
    /// Where a write's bytes go once read
    pub route: Route,
    /// Where the small requests travel, if any are sent
    pub arr: Option<Arr>,
}

impl Cell {
    /// A cell with no small requests, directly written, at the default window
    ///
    /// # Arguments
    ///
    /// * `section` - The section
    /// * `cell` - The cell's name
    /// * `side` - The side's name
    /// * `dir` - Which way the stream goes
    /// * `frame` - Payload bytes a frame
    /// * `tls` - How it is encrypted
    /// * `file` - Whether bytes land in the file
    #[must_use]
    pub fn stream(
        section: &'static str,
        cell: String,
        side: String,
        dir: Dir,
        frame: usize,
        tls: Tls,
        file: bool,
    ) -> Self {
        Cell {
            section,
            cell,
            side,
            dir: Some(dir),
            frame,
            tls,
            file,
            window: WINDOW,
            route: Route::Direct,
            arr: None,
        }
    }
}

/// What every cell is run with
pub struct Ctx {
    /// Where the server is
    pub target: Target,
    /// The runtime that holds a stream's connection
    pub stream_rt: Rt,
    /// The runtime that paces small requests and asks for counters
    pub small_rt: Rt,
    /// A plaintext connection to each executor, for its counters
    pub controls: Vec<Arc<Conn>>,
    /// Whether the server is on this host, so the host's busy time is read once
    pub same_host: bool,
    /// Whether the server has a file to write to and read from
    pub server_file: bool,
    /// Whether the server has a second executor
    pub two_executors: bool,
    /// Whether this is a quick run, which measures nothing
    pub quick: bool,
    /// Whether the cells are the network's smaller set
    pub net: bool,
    /// The leg, naming where the client and the server are
    pub leg: String,
    /// The line every table carries
    pub label: String,
    /// Where records are written
    pub out: Option<std::path::PathBuf>,
}

impl Ctx {
    /// The warm-up before a measured window
    #[must_use]
    pub fn warmup(&self) -> Duration {
        if self.quick {
            Duration::from_millis(300)
        } else {
            Duration::from_secs(3)
        }
    }

    /// The measured window of a stream
    ///
    /// # Arguments
    ///
    /// * `section` - The section, since a small request's tail needs a longer one
    #[must_use]
    pub fn window(&self, section: &str) -> Duration {
        match (self.quick, section) {
            (true, _) => Duration::from_millis(700),
            (false, "tail") => Duration::from_secs(10),
            (false, _) => Duration::from_secs(5),
        }
    }
}

/// Every side of section one: rates and cpu by frame size and encryption
///
/// # Arguments
///
/// * `ctx` - What the cells run with
#[must_use]
pub fn rate_cells(ctx: &Ctx) -> Vec<Vec<Cell>> {
    let frames = if ctx.net { NET_FRAMES } else { FRAMES };
    let tls: &[Tls] = if ctx.net {
        &[Tls::Plain, Tls::Ktls]
    } else {
        &[Tls::Plain, Tls::Ktls, Tls::NoPad]
    };
    let targets: &[bool] = match (ctx.server_file, ctx.net) {
        (true, false) => &[true, false],
        (true, true) => &[true],
        (false, _) => &[false],
    };
    let mut cells = Vec::new();
    for dir in [Dir::Write, Dir::Read] {
        for &frame in frames {
            for &file in targets {
                let name = format!(
                    "{} {} {}",
                    dir.name(),
                    size_name(frame as u64),
                    if file { "file" } else { "memory" }
                );
                cells.push(
                    tls.iter()
                        .map(|&tls| {
                            Cell::stream(
                                "rate",
                                name.clone(),
                                tls.name().to_string(),
                                dir,
                                frame,
                                tls,
                                file,
                            )
                        })
                        .collect(),
                );
            }
        }
    }
    cells
}

/// Every side of section two: the window
///
/// # Arguments
///
/// * `ctx` - What the cells run with
#[must_use]
pub fn window_cells(ctx: &Ctx) -> Vec<Vec<Cell>> {
    let mut cells = Vec::new();
    for dir in [Dir::Write, Dir::Read] {
        for &frame in WINDOW_FRAMES {
            for &window in WINDOWS {
                let name = format!("{} {} w{window}", dir.name(), size_name(frame as u64));
                cells.push(
                    [Tls::Plain, Tls::Ktls]
                        .iter()
                        .map(|&tls| {
                            let mut cell = Cell::stream(
                                "window",
                                name.clone(),
                                tls.name().to_string(),
                                dir,
                                frame,
                                tls,
                                ctx.server_file,
                            );
                            cell.window = window;
                            cell
                        })
                        .collect(),
                );
            }
        }
    }
    cells
}

/// Every side of section three: a small request beside a stream
///
/// # Arguments
///
/// * `ctx` - What the cells run with
#[must_use]
pub fn tail_cells(ctx: &Ctx) -> Vec<Vec<Cell>> {
    let frames = if ctx.net {
        NET_TAIL_FRAMES
    } else {
        TAIL_FRAMES
    };
    let mut cells = Vec::new();
    // the requests alone, under each encryption
    for tls in [Tls::Ktls, Tls::Plain] {
        let mut cell = Cell::stream(
            "tail",
            format!("alone {}", tls.name()),
            "alone".to_string(),
            Dir::Read,
            0,
            tls,
            false,
        );
        cell.dir = None;
        cell.arr = Some(Arr::Alone);
        cells.push(vec![cell]);
    }
    // beside a stream, every arrangement a side
    let mut beside = |dir: Dir, frame: usize, tls: Tls| {
        let mut arrangements = vec![
            Arr::Shared,
            Arr::SharedLowat(16 << 10),
            Arr::SharedLowat(128 << 10),
        ];
        if dir == Dir::Read {
            arrangements.push(Arr::SharedFifo);
        }
        arrangements.push(Arr::OwnSame);
        if ctx.two_executors {
            arrangements.push(Arr::OwnOther);
        }
        let name = format!("{} {} {}", dir.name(), size_name(frame as u64), tls.name());
        cells.push(
            arrangements
                .into_iter()
                .map(|arr| {
                    let mut cell = Cell::stream(
                        "tail",
                        name.clone(),
                        arr.name(),
                        dir,
                        frame,
                        tls,
                        ctx.server_file,
                    );
                    cell.arr = Some(arr);
                    cell
                })
                .collect(),
        );
    };
    for dir in [Dir::Read, Dir::Write] {
        for &frame in frames {
            beside(dir, frame, Tls::Ktls);
        }
        beside(dir, 1 << 20, Tls::Plain);
    }
    cells
}

/// Every side of section four: the routes to the other executor
///
/// # Arguments
///
/// * `ctx` - What the cells run with
#[must_use]
pub fn route_cells(ctx: &Ctx) -> Vec<Vec<Cell>> {
    let mut cells = Vec::new();
    if !ctx.two_executors {
        return cells;
    }
    for frame in [64 << 10, 1 << 20] {
        for tls in [Tls::Plain, Tls::Ktls] {
            let name = format!("write {} {}", size_name(frame as u64), tls.name());
            cells.push(
                [Route::Direct, Route::Hop, Route::HopCopy, Route::Handoff]
                    .iter()
                    .map(|&route| {
                        let mut cell = Cell::stream(
                            "route",
                            name.clone(),
                            route.name().to_string(),
                            Dir::Write,
                            frame,
                            tls,
                            ctx.server_file,
                        );
                        cell.route = route;
                        cell
                    })
                    .collect(),
            );
        }
    }
    cells
}

/// What both ends said at one moment
struct Snap {
    /// When
    at: Instant,
    /// Every executor's counters
    server: Vec<Stats>,
    /// The stream runtime's cpu time
    client_cpu: u64,
    /// This host's busy time
    client_host: u64,
    /// Payload bytes the stream's connection received
    received: u64,
}

/// Read both ends' counters
///
/// # Arguments
///
/// * `ctx` - What the cells run with
/// * `conn` - The stream's connection, if one runs
/// * `reset` - Whether the peaks start again afterwards
fn snapshot(ctx: &Ctx, conn: Option<&Arc<Conn>>, reset: bool) -> Snap {
    let server = ctx
        .controls
        .iter()
        .map(|control| {
            let control = control.clone();
            ctx.small_rt.run(async move { control.stat(reset).await })
        })
        .collect();
    if reset {
        if let Some(conn) = conn {
            conn.shared.peak_sock.store(0, Ordering::Release);
        }
    }
    Snap {
        at: Instant::now(),
        server,
        client_cpu: ctx.stream_rt.cpu_ns(),
        client_host: super::sock::host_busy_ns(),
        received: conn.map_or(0, |conn| conn.shared.received.load(Ordering::Acquire)),
    }
}

/// Open a connection on a runtime
///
/// # Arguments
///
/// * `rt` - The runtime that holds its tasks
/// * `target` - Where the server is
/// * `dial` - How to open it
fn open(rt: &Rt, target: &Target, dial: Dial) -> Arc<Conn> {
    let target = target.clone();
    Arc::new(rt.run(async move { Conn::open(target, dial).await }))
}

/// Run one side of one cell
///
/// # Arguments
///
/// * `ctx` - What the cells run with
/// * `cell` - The side
/// * `id` - An id no other stream of this run has used
pub fn run(ctx: &Ctx, cell: &Cell, id: u64) -> SideOut {
    let tls = cell.tls != Tls::Plain;
    let lowat = match cell.arr {
        Some(Arr::SharedLowat(bytes)) => bytes,
        _ => 0,
    };
    let setup = Setup {
        file: cell.file,
        route: cell.route,
        fifo: cell.arr == Some(Arr::SharedFifo),
        window: cell.window as u32,
        ..Setup::default()
    };
    let dial = Dial {
        executor: 0,
        tls,
        nopad: cell.tls == Tls::NoPad,
        lowat,
        setup,
    };
    // the stream's connection, unless the requests run alone
    let conn = cell.dir.map(|_| open(&ctx.stream_rt, &ctx.target, dial));
    // the small requests' connection: the stream's own, or one of their own
    let pacer = Arc::new(Pacer::default());
    let small = match cell.arr {
        None => None,
        Some(Arr::Shared | Arr::SharedLowat(_) | Arr::SharedFifo) => conn.clone(),
        Some(arr) => {
            let executor = usize::from(arr == Arr::OwnOther);
            let own = Dial {
                executor,
                tls,
                setup: Setup {
                    window: 1,
                    ..Setup::default()
                },
                ..Dial::default()
            };
            Some(open(&ctx.small_rt, &ctx.target, own))
        }
    };
    if let Some(small) = &small {
        small.answer_to(&pacer);
    }
    // the stream, on the runtime that holds its connection
    let running = conn.as_ref().zip(cell.dir).map(|(conn, dir)| {
        let _guard = ctx.stream_rt.handle().enter();
        match dir {
            Dir::Write => client::start_write(conn, id, cell.frame),
            Dir::Read => client::start_read(conn, id, cell.frame, cell.window),
        }
    });
    // the small requests, paced from the other runtime
    let pacing = small.as_ref().map(|small| {
        let _guard = ctx.small_rt.handle().enter();
        client::start_pacing(small.small_sender(), pacer.clone(), PERIOD)
    });
    // a warm-up, then the measured window between two readings of both ends
    std::thread::sleep(ctx.warmup());
    let before = snapshot(ctx, conn.as_ref(), true);
    pacer.record(true);
    std::thread::sleep(ctx.window(cell.section));
    pacer.record(false);
    let after = snapshot(ctx, conn.as_ref(), false);
    // the requests and the stream end, and every connection closes
    pacer.stop();
    if let Some(pacing) = pacing {
        let _ = futures::executor::block_on(pacing);
    }
    let done = running.map(|running| running.finish(&ctx.stream_rt));
    let client_peak_sock = conn
        .as_ref()
        .map_or(0, |conn| conn.shared.peak_sock.load(Ordering::Acquire));
    let client_mismatches = conn
        .as_ref()
        .map_or(0, |conn| conn.shared.mismatches.load(Ordering::Acquire));
    let ktls = conn
        .as_ref()
        .map_or(false, |conn| conn.client_ktls && conn.server_ktls);
    let executor = conn.as_ref().map_or(0, |conn| conn.executor);
    drop(small);
    drop(conn);
    figures(
        ctx,
        cell,
        &before,
        &after,
        &pacer.samples(),
        client_peak_sock,
        client_mismatches,
        ktls,
        executor,
        done,
    )
}

/// The side's figures from the two readings
///
/// # Arguments
///
/// * `ctx` - What the cells run with
/// * `cell` - The side
/// * `before` - The reading at the window's start
/// * `after` - The reading at its end
/// * `samples` - The small requests' latencies
/// * `client_peak_sock` - The client socket's most memory
/// * `client_mismatches` - Payloads the client found wrong
/// * `ktls` - Whether kTLS held both ends
/// * `executor` - Which executor served the stream's connection
/// * `done` - The stream's end
#[allow(clippy::too_many_arguments)]
fn figures(
    ctx: &Ctx,
    cell: &Cell,
    before: &Snap,
    after: &Snap,
    samples: &Samples,
    client_peak_sock: u64,
    client_mismatches: u64,
    ktls: bool,
    executor: u64,
    done: Option<client::Done>,
) -> SideOut {
    let seconds = after.at.duration_since(before.at).as_secs_f64();
    let delta = |pick: fn(&Stats) -> u64| -> u64 {
        before
            .server
            .iter()
            .zip(&after.server)
            .map(|(b, a)| pick(a).saturating_sub(pick(b)))
            .sum()
    };
    // bytes the stream moved in the window: the server's for a write, the client's for a read
    let bytes = match cell.dir {
        Some(Dir::Write) => delta(|stats| stats.bytes_in),
        Some(Dir::Read) => after.received.saturating_sub(before.received),
        None => 0,
    };
    let gib = bytes as f64 / f64::from(1u32 << 30);
    let per_gib = |ns: u64| {
        if gib > 0.0 {
            ns as f64 / 1e6 / gib
        } else {
            0.0
        }
    };
    let server_cpu = delta(|stats| stats.thread_cpu_ns);
    let client_cpu = after.client_cpu.saturating_sub(before.client_cpu);
    // the host's busy time once on loopback, both hosts' across the network
    let server_host = after
        .server
        .first()
        .map_or(0, |a| a.host_busy_ns)
        .saturating_sub(before.server.first().map_or(0, |b| b.host_busy_ns));
    let host = if ctx.same_host {
        server_host
    } else {
        server_host + after.client_host.saturating_sub(before.client_host)
    };
    let peak = |pick: fn(&Stats) -> u64| after.server.iter().map(pick).max().unwrap_or(0);
    let mut metrics = vec![
        ("mib_s", bytes as f64 / seconds / f64::from(1u32 << 20)),
        ("server_cpu_ms_gib", per_gib(server_cpu)),
        ("client_cpu_ms_gib", per_gib(client_cpu)),
        ("host_cpu_ms_gib", per_gib(host)),
        ("server_cores", server_cpu as f64 / 1e9 / seconds),
        ("client_cores", client_cpu as f64 / 1e9 / seconds),
        (
            "server_peak_held_kib",
            peak(|stats| stats.peak_held) as f64 / 1024.0,
        ),
        (
            "server_peak_sock_kib",
            peak(|stats| stats.peak_sock) as f64 / 1024.0,
        ),
        ("client_peak_sock_kib", client_peak_sock as f64 / 1024.0),
        (
            "mismatches",
            (delta(|stats| stats.mismatches) + client_mismatches) as f64,
        ),
        ("ktls", f64::from(u8::from(ktls))),
        ("executor", executor as f64),
        (
            "done_mib",
            done.map_or(0.0, |done| done.bytes as f64 / f64::from(1u32 << 20)),
        ),
    ];
    // the small requests' latencies, when there were any
    if !samples.0.is_empty() {
        let summary = samples.summary();
        metrics.extend([
            ("small_n", summary.n as f64),
            ("small_p50_us", summary.p50),
            ("small_p99_us", summary.p99),
            ("small_p999_us", summary.p999),
            ("small_max_us", summary.max),
        ]);
    }
    SideOut::new(cell.cell.clone(), cell.side.clone(), &metrics)
}

/// Check that a kTLS connection survives being handed to the other executor, both ways
///
/// One connection asks to be handed over; the other executor answers its setup, reads a write
/// stream checking every payload against the pattern, and answers a read stream that the client
/// checks the same way.
///
/// # Arguments
///
/// * `ctx` - What the cells run with
/// * `tls` - Whether the connection is encrypted
pub fn handoff_check(ctx: &Ctx, tls: bool) -> SideOut {
    let setup = Setup {
        route: Route::Handoff,
        verify: true,
        window: WINDOW as u32,
        ..Setup::default()
    };
    let dial = Dial {
        executor: 0,
        tls,
        setup,
        ..Dial::default()
    };
    let conn = open(&ctx.stream_rt, &ctx.target, dial);
    let span = if ctx.quick {
        Duration::from_millis(500)
    } else {
        Duration::from_secs(2)
    };
    let before = snapshot(ctx, Some(&conn), true);
    // a write stream, every payload checked by the executor it was handed to
    let running = {
        let _guard = ctx.stream_rt.handle().enter();
        client::start_write(&conn, 1, 1 << 20)
    };
    std::thread::sleep(span);
    let written = running.finish(&ctx.stream_rt);
    // then a read stream on the same connection, every payload checked here
    let running = {
        let _guard = ctx.stream_rt.handle().enter();
        client::start_read(&conn, 2, 1 << 20, WINDOW)
    };
    std::thread::sleep(span);
    let read = running.finish(&ctx.stream_rt);
    let after = snapshot(ctx, Some(&conn), false);
    let server_mismatches: u64 = before
        .server
        .iter()
        .zip(&after.server)
        .map(|(b, a)| a.mismatches - b.mismatches)
        .sum();
    let client_mismatches = conn.shared.mismatches.load(Ordering::Acquire);
    let handed = conn.executor == 1;
    let ktls = !tls || (conn.client_ktls && conn.server_ktls);
    let ok = handed
        && ktls
        && server_mismatches == 0
        && client_mismatches == 0
        && written.bytes > 0
        && read.bytes > 0;
    SideOut::new(
        "handoff check",
        if tls { "ktls" } else { "plain" },
        &[
            ("handed", f64::from(u8::from(handed))),
            ("ktls_after", f64::from(u8::from(conn.server_ktls))),
            ("write_mib", written.bytes as f64 / f64::from(1u32 << 20)),
            ("read_mib", read.bytes as f64 / f64::from(1u32 << 20)),
            ("server_mismatches", server_mismatches as f64),
            ("client_mismatches", client_mismatches as f64),
            ("ok", f64::from(u8::from(ok))),
        ],
    )
}

/// Run every side of a section's cells for one round, printing a table and keeping records
///
/// # Arguments
///
/// * `ctx` - What the cells run with
/// * `title` - The section's title
/// * `cells` - Its cells, each a list of sides
/// * `round` - The round
/// * `next_id` - The next unused stream id
pub fn section(
    ctx: &Ctx,
    title: &str,
    cells: &[Vec<Cell>],
    round: u32,
    next_id: &mut u64,
) -> Vec<Record> {
    let mut table = Table::new(&[
        "cell",
        "side",
        "MiB/s",
        "server cpu ms/GiB",
        "client cpu ms/GiB",
        "host cpu ms/GiB",
        "held KiB",
        "sock KiB (server/client)",
        "small p50/p99/p999 µs",
    ]);
    let mut records = Vec::new();
    for sides in cells {
        for cell in ordered(sides, round) {
            *next_id += 1;
            let out = run(ctx, &cell, *next_id);
            let fmt = crate::device::stats::fmt;
            let small = if out.metrics.contains_key("small_p99_us") {
                format!(
                    "{}/{}/{}",
                    fmt(out.get("small_p50_us")),
                    fmt(out.get("small_p99_us")),
                    fmt(out.get("small_p999_us"))
                )
            } else {
                "—".to_string()
            };
            table.row(vec![
                out.cell.clone(),
                out.side.clone(),
                fmt(out.get("mib_s")),
                fmt(out.get("server_cpu_ms_gib")),
                fmt(out.get("client_cpu_ms_gib")),
                fmt(out.get("host_cpu_ms_gib")),
                fmt(out.get("server_peak_held_kib")),
                format!(
                    "{}/{}",
                    fmt(out.get("server_peak_sock_kib")),
                    fmt(out.get("client_peak_sock_kib"))
                ),
                small,
            ]);
            if out.get("mismatches") > 0.0 {
                eprintln!(
                    "x11: {} {} found {} payloads that were not the pattern",
                    out.cell,
                    out.side,
                    out.get("mismatches")
                );
            }
            records.push(record(ctx, cell.section, round, out));
        }
    }
    print!(
        "{}",
        table.render(&format!("{title}, round {round}"), &ctx.label)
    );
    records
}

/// A side's record
///
/// # Arguments
///
/// * `ctx` - What the cells run with
/// * `measurement` - The section
/// * `round` - The round
/// * `out` - The side's figures
#[must_use]
pub fn record(ctx: &Ctx, measurement: &str, round: u32, out: SideOut) -> Record {
    Record {
        host: crate::hostname(),
        fs: ctx.target.host.clone(),
        leg: ctx.leg.clone(),
        measurement: measurement.to_string(),
        cell: out.cell,
        side: out.side,
        round,
        quick: ctx.quick,
        metrics: out.metrics,
    }
}
