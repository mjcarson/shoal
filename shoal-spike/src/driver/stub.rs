//! Sections 2, 3b and 5: the driver stub against a server that discards
//!
//! The server is X11's: glommio executors, one pinned to a core each, speaking X11's frames. A put
//! stream's data frames are read into a buffer and dropped, which is the wire alone; a get
//! stream's ranges are answered from a pattern in memory, which costs no read. Since X13 the server
//! holds one pattern for each generator, and a connection's setup names the one its reads come
//! from, so a get can be checked by making its bytes again.
//!
//! The client is X13's own, tokio on a thread pinned to one core, as a driver's worker would run:
//!
//! - **a put** makes each 1 MiB frame's payload as it goes - a slice of the server's pattern, which
//!   makes nothing and is X11's baseline; a generator's fill; or a fill and the unit's CRC - and
//!   writes it, until it is told to stop;
//! - **a get** keeps four ranges outstanding and checks each answer - not at all; by its CRC
//!   against a table of the pattern's units made at setup, which is S16's ledger; or by making
//!   the bytes again and comparing.
//!
//! Each stream's runtime thread records its id, and its cpu is read from the kernel's schedstat for
//! that thread, so a loop that rarely yields is counted as the kernel counts it.

use std::hint::black_box;
use std::io::IoSlice;
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rustls::client::ClientConnectionData;
use shoal::shared::tls::Established;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;

use super::generate::Generator;
use super::make::crc;
use super::Ctx;
use crate::device::counters::{cpu_busy, cpu_jiffies};
use crate::device::{ordered, SideOut};
use crate::stream::client::{Conn, Dial, Rt, Target};
use crate::stream::server::{self, frame};
use crate::stream::sock;
use crate::stream::wire::{self, Header, Kind, Setup, Stats, HEADER_LEN, LAST};

/// The bytes of a data frame's payload, the frame X11 chose
pub const FRAME: usize = 1 << 20;

/// The ranges a get keeps outstanding, the window X11 chose
pub const WINDOW: usize = 4;

/// What every stream section shares: the server's address, the client's runtimes, the patterns
pub struct Stub {
    /// Where the server is
    pub target: Target,
    /// The runtime that asks the executors for their counters
    pub control_rt: Rt,
    /// A plaintext connection to every executor, for its counters
    pub controls: Vec<Arc<Conn>>,
    /// One runtime a client core, each pinned
    pub client_rts: Vec<Rt>,
    /// The cpu each runtime is pinned to
    pub client_cpus: Vec<usize>,
    /// The server's own pattern, which a setup names as zero
    pub own: Arc<Vec<u8>>,
    /// The CRC of each frame of the server's own pattern: the ledger a get's `crc` side checks
    pub ledger: Arc<Vec<u64>>,
    /// The generators, the server's pattern for each named by its place plus one
    pub generators: Arc<Vec<Arc<dyn Generator>>>,
    /// Whether the server is on this host, so the host's busy time is read once
    pub same_host: bool,
    /// How many executors the server has
    pub executors: usize,
}

/// The patterns the server answers reads from, one a generator: object zero's first 64 MiB
///
/// # Arguments
///
/// * `generators` - The generators
#[must_use]
pub fn patterns(generators: &[Arc<dyn Generator>]) -> Vec<Arc<Vec<u8>>> {
    generators
        .iter()
        .map(|generator| {
            let mut pattern = vec![0u8; wire::PATTERN_LEN];
            generator.fill(0, 0, &mut pattern);
            Arc::new(pattern)
        })
        .collect()
}

/// The CRC of each frame of a pattern
///
/// # Arguments
///
/// * `pattern` - The pattern
#[must_use]
pub fn ledger(pattern: &[u8]) -> Vec<u64> {
    pattern.chunks_exact(FRAME).map(crc).collect()
}

/// How a put's payload is made
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Payload {
    /// A slice of the server's own pattern: nothing is made, X11's baseline
    Pattern,
    /// A generator's fill
    Fill(usize),
    /// A generator's fill, then the unit's CRC
    FillCrc(usize),
}

/// How a get checks what comes back
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Check {
    /// Not at all
    None,
    /// By its CRC, against the ledger made at setup
    Crc,
    /// By making it again with a generator and comparing
    Regenerate(usize),
}

/// A connection opened and set up
struct Opened {
    /// The socket
    stream: TcpStream,
    /// Its TLS session, held for as long as the socket is used
    _tls: Option<Established<ClientConnectionData>>,
    /// Whether kTLS holds both ends
    ktls: bool,
}

/// Open a connection to an executor, encrypted or not, and tell the server what it is for
///
/// The steps are X11's `Conn::open`: connect, Nagle off, the handshake and kTLS, the setup, and
/// the server's answer, which says whether kTLS holds its end.
///
/// # Arguments
///
/// * `target` - Where the server is
/// * `executor` - Which executor serves it
/// * `tls` - Whether it is encrypted
/// * `setup` - What the server is told
async fn open(target: Target, executor: usize, tls: bool, setup: Setup) -> Opened {
    // the executor's port, TLS or plaintext
    let port = if tls {
        server::tls_port(target.base_port, executor)
    } else {
        server::plain_port(target.base_port, executor)
    };
    let addr: SocketAddr = tokio::net::lookup_host((target.host.as_str(), port))
        .await
        .expect("the server's address resolves")
        .next()
        .expect("the server has an address");
    let mut stream = TcpStream::connect(addr).await.expect("the server accepts");
    stream.set_nodelay(true).expect("Nagle turns off");
    // the handshake, then kTLS, as the product's client does it
    let session = if tls {
        let (config, options) = target.tls.as_ref().expect("a TLS dial needs a certificate");
        Some(
            shoal::client::tls::connect(&mut stream, config, options, &addr)
                .await
                .expect("the TLS handshake succeeds"),
        )
    } else {
        None
    };
    let client_ktls = sock::is_ktls(stream.as_raw_fd());
    // the setup, and the answer that names the executor and its kTLS
    stream
        .write_all(&frame(Kind::Setup, 0, &setup.encode()))
        .await
        .expect("the setup is written");
    let header = read_header(&mut stream).await.expect("the server answers the setup");
    let mut body = vec![0u8; header.len as usize];
    stream.read_exact(&mut body).await.expect("the setup's answer is read");
    assert_eq!(header.kind, Kind::SetupOk, "the server answers a setup with its own");
    Opened {
        stream,
        _tls: session,
        ktls: client_ktls && body.get(8) == Some(&1),
    }
}

/// Read the next header
///
/// # Arguments
///
/// * `reader` - The connection
async fn read_header<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<Header> {
    let mut bytes = [0u8; HEADER_LEN];
    reader.read_exact(&mut bytes).await?;
    Header::decode(&bytes).map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}

/// Write every byte of several buffers, in as few calls as the socket allows
///
/// # Arguments
///
/// * `writer` - Where to write
/// * `parts` - What to write, in order
async fn write_all_vectored<W: AsyncWrite + Unpin>(writer: &mut W, parts: &[&[u8]]) -> std::io::Result<()> {
    let mut slices: Vec<IoSlice<'_>> = parts
        .iter()
        .filter(|part| !part.is_empty())
        .map(|part| IoSlice::new(part))
        .collect();
    let mut bufs = &mut slices[..];
    while !bufs.is_empty() {
        let written = writer.write_vectored(bufs).await?;
        if written == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        IoSlice::advance_slices(&mut bufs, written);
    }
    Ok(())
}

/// What a stream shares with whoever measures it
#[derive(Default)]
struct Shared {
    /// Payload bytes written, or received
    bytes: AtomicU64,
    /// Answers that did not match
    mismatches: AtomicU64,
    /// The thread the stream runs on, once it runs
    tid: AtomicI32,
    /// Whether kTLS holds both ends, once open
    ktls: AtomicBool,
    /// Whether the stream is to end
    stop: AtomicBool,
}

/// Run a put on an open connection until it is told to stop, then wait for the server's count
///
/// # Arguments
///
/// * `opened` - The connection
/// * `id` - The stream's id, and the object its bytes are made for
/// * `payload` - How each frame's payload is made
/// * `own` - The server's own pattern, the baseline's payload
/// * `generators` - The generators
/// * `shared` - What the measurer reads
async fn put_loop(
    mut opened: Opened,
    id: u64,
    payload: Payload,
    own: Arc<Vec<u8>>,
    generators: Arc<Vec<Arc<dyn Generator>>>,
    shared: Arc<Shared>,
) -> u64 {
    // the stream opens, then a frame at a time until the stop, the last frame flagged
    let open = frame(Kind::Write, 0, &wire::two_words(id, 0));
    opened.stream.write_all(&open).await.expect("the stream opens");
    let mut buf = vec![0u8; FRAME];
    let mut offset = 0u64;
    let mut sink = 0u64;
    loop {
        let last = shared.stop.load(Ordering::Acquire);
        // the payload: cut from the pattern, made, or made and checksummed
        let bytes: &[u8] = match payload {
            Payload::Pattern => {
                let start = wire::pattern_at(offset);
                &own[start..start + FRAME]
            }
            Payload::Fill(at) => {
                generators[at].fill(id, offset, &mut buf);
                &buf
            }
            Payload::FillCrc(at) => {
                generators[at].fill(id, offset, &mut buf);
                sink ^= crc(&buf);
                &buf
            }
        };
        let header = Header::new(Kind::Data, if last { LAST } else { 0 }, wire::DATA_HEAD_LEN + FRAME).encode();
        let head = wire::data_head(id, offset);
        write_all_vectored(&mut opened.stream, &[&header, &head, bytes])
            .await
            .expect("a frame is written");
        shared.bytes.fetch_add(FRAME as u64, Ordering::AcqRel);
        offset += FRAME as u64;
        if last {
            break;
        }
    }
    black_box(sink);
    // the server's end of the stream, with what it received
    loop {
        let header = read_header(&mut opened.stream).await.expect("the server ends the stream");
        let mut body = vec![0u8; header.len as usize];
        opened.stream.read_exact(&mut body).await.expect("its body is read");
        if header.kind == Kind::Done {
            return wire::word(&body, 1);
        }
    }
}

/// Run a get on an open connection until it is told to stop and every range has come back
///
/// # Arguments
///
/// * `opened` - The connection
/// * `id` - The stream's id
/// * `check` - How each answer is checked
/// * `ledger` - The CRC of each frame of the pattern served
/// * `generators` - The generators
/// * `shared` - What the measurer reads
async fn get_loop(
    opened: Opened,
    id: u64,
    check: Check,
    ledger: Arc<Vec<u64>>,
    generators: Arc<Vec<Arc<dyn Generator>>>,
    shared: Arc<Shared>,
) {
    let Opened { stream, _tls, .. } = opened;
    let (mut reader, mut writer) = stream.into_split();
    // the writer asks for a range for every one answered, never more than the window out
    let permits = Arc::new(Semaphore::new(WINDOW));
    let asked = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let writing = {
        let (permits, asked, done, shared) = (permits.clone(), asked.clone(), done.clone(), shared.clone());
        tokio::spawn(async move {
            let mut offset = 0u64;
            while !shared.stop.load(Ordering::Acquire) {
                let permit = permits.acquire().await.expect("the semaphore stays open");
                permit.forget();
                let mut body = [0u8; 24];
                body[..16].copy_from_slice(&wire::two_words(id, offset));
                body[16..].copy_from_slice(&(FRAME as u64).to_le_bytes());
                writer
                    .write_all(&frame(Kind::Read, 0, &body))
                    .await
                    .expect("a range is asked for");
                asked.fetch_add(1, Ordering::AcqRel);
                offset += FRAME as u64;
            }
            done.store(true, Ordering::Release);
            writer
        })
    };
    // every answer read into one buffer, checked, and its permit given back
    let mut payload = vec![0u8; FRAME];
    let mut made = vec![0u8; FRAME];
    let mut answered = 0u64;
    loop {
        // nothing outstanding: the end once the writer has stopped, or its next request first,
        // since a read now would wait for an answer nobody asked for
        if asked.load(Ordering::Acquire).saturating_sub(answered) == 0 {
            if done.load(Ordering::Acquire) {
                break;
            }
            tokio::task::yield_now().await;
            continue;
        }
        let header = read_header(&mut reader).await.expect("an answer is read");
        let mut head = [0u8; wire::DATA_HEAD_LEN];
        reader.read_exact(&mut head).await.expect("its head is read");
        let len = header.len as usize - wire::DATA_HEAD_LEN;
        reader.read_exact(&mut payload[..len]).await.expect("its payload is read");
        let offset = wire::word(&head, 2);
        let bad = match check {
            Check::None => false,
            Check::Crc => crc(&payload[..len]) != ledger[wire::pattern_at(offset) / FRAME],
            Check::Regenerate(at) => {
                generators[at].fill(0, wire::pattern_at(offset) as u64, &mut made[..len]);
                made[..len] != payload[..len]
            }
        };
        if bad {
            shared.mismatches.fetch_add(1, Ordering::AcqRel);
        }
        shared.bytes.fetch_add(len as u64, Ordering::AcqRel);
        answered += 1;
        permits.add_permits(1);
    }
    let _ = writing.await;
}

/// The cpu a thread of this process has used, in nanoseconds, as the kernel's schedstat counts it
///
/// # Arguments
///
/// * `tid` - The thread
#[must_use]
pub fn thread_ns(tid: i32) -> u64 {
    // the first field of the thread's schedstat is its time on a cpu
    std::fs::read_to_string(format!("/proc/self/task/{tid}/schedstat"))
        .ok()
        .and_then(|text| text.split_whitespace().next().and_then(|field| field.parse().ok()))
        .unwrap_or(0)
}

/// Everything a side reads at the start and the end of its window
struct Snap {
    /// When
    at: Instant,
    /// Each stream's bytes
    bytes: Vec<u64>,
    /// Each stream's thread's cpu
    thread_ns: Vec<u64>,
    /// Every cpu's busy and total jiffies
    jiffies: Vec<(u64, u64)>,
    /// Each executor's counters
    stats: Vec<Stats>,
    /// This host's busy time over every cpu
    host_ns: u64,
}

/// One side of a stream cell: what each of its streams is, and how many there are
#[derive(Debug, Clone, Copy)]
pub enum Side {
    /// A put, its payload made one way
    Put(Payload),
    /// A get, checked one way
    Get(Check),
}

impl Stub {
    /// Read everything a side's window is measured between
    ///
    /// # Arguments
    ///
    /// * `streams` - The streams' shared state
    fn snap(&self, streams: &[Arc<Shared>]) -> Snap {
        let stats = self
            .controls
            .iter()
            .map(|conn| {
                let conn = conn.clone();
                self.control_rt.run(async move { conn.stat(false).await })
            })
            .collect();
        Snap {
            at: Instant::now(),
            bytes: streams.iter().map(|shared| shared.bytes.load(Ordering::Acquire)).collect(),
            thread_ns: streams
                .iter()
                .map(|shared| thread_ns(shared.tid.load(Ordering::Acquire)))
                .collect(),
            jiffies: cpu_jiffies(),
            stats,
            host_ns: sock::host_busy_ns(),
        }
    }

    /// Run one side: its streams started, warmed up, measured over a window, and stopped
    ///
    /// # Arguments
    ///
    /// * `ctx` - What the run is run with
    /// * `cell` - The cell
    /// * `name` - The side
    /// * `side` - What each stream is
    /// * `tls` - Whether the streams are encrypted
    /// * `cores` - How many streams, one a client core
    /// * `next_id` - The next stream id no stream of this run has used
    #[allow(clippy::too_many_arguments)]
    pub fn run(
        &self,
        ctx: &Ctx,
        cell: &str,
        name: &str,
        side: Side,
        tls: bool,
        cores: usize,
        next_id: &mut u64,
    ) -> SideOut {
        // each stream on its own runtime and its own executor
        let mut streams = Vec::with_capacity(cores);
        let mut tasks = Vec::with_capacity(cores);
        for core in 0..cores {
            let shared = Arc::new(Shared::default());
            let id = *next_id;
            *next_id += 1;
            let executor = core % self.executors;
            let (target, own, ledger, generators, task_shared) = (
                self.target.clone(),
                self.own.clone(),
                self.ledger.clone(),
                self.generators.clone(),
                shared.clone(),
            );
            let setup = Setup {
                window: WINDOW as u32,
                pattern: match side {
                    Side::Get(Check::Regenerate(at)) => u8::try_from(at + 1).expect("few generators"),
                    _ => 0,
                },
                ..Setup::default()
            };
            tasks.push(self.client_rts[core].handle().spawn(async move {
                // SAFETY: gettid has no preconditions
                task_shared.tid.store(unsafe { libc::gettid() }, Ordering::Release);
                let opened = open(target, executor, tls, setup).await;
                task_shared.ktls.store(opened.ktls, Ordering::Release);
                match side {
                    Side::Put(payload) => put_loop(opened, id, payload, own, generators, task_shared).await,
                    Side::Get(check) => {
                        get_loop(opened, id, check, ledger, generators, task_shared).await;
                        0
                    }
                }
            }));
            streams.push(shared);
        }
        // the warm-up, then the window between two snapshots
        std::thread::sleep(ctx.warmup());
        let first = self.snap(&streams);
        std::thread::sleep(ctx.window());
        let last = self.snap(&streams);
        for shared in &streams {
            shared.stop.store(true, Ordering::Release);
        }
        for task in tasks {
            futures::executor::block_on(task).expect("a stream's task finishes");
        }
        self.figures(cell, name, &streams, &first, &last)
    }

    /// A side's figures from its window
    ///
    /// # Arguments
    ///
    /// * `cell` - The cell
    /// * `name` - The side
    /// * `streams` - The streams' shared state
    /// * `first` - The window's start
    /// * `last` - Its end
    fn figures(&self, cell: &str, name: &str, streams: &[Arc<Shared>], first: &Snap, last: &Snap) -> SideOut {
        let secs = last.at.duration_since(first.at).as_secs_f64();
        let cores = streams.len();
        // the client's bytes and cpu, every stream summed
        let bytes: u64 = (0..cores).map(|at| last.bytes[at] - first.bytes[at]).sum();
        let thread_ns: u64 = (0..cores).map(|at| last.thread_ns[at].saturating_sub(first.thread_ns[at])).sum();
        let gib = bytes as f64 / f64::from(1u32 << 30);
        let mib_s = bytes as f64 / secs / f64::from(1u32 << 20);
        let busy = thread_ns as f64 / 1e9 / secs;
        let core_busy: f64 = self.client_cpus[..cores]
            .iter()
            .map(|&cpu| cpu_busy(&first.jiffies, &last.jiffies, cpu))
            .sum::<f64>()
            / cores as f64;
        // the server's cpu, every executor, and its busiest one
        let server_ns: Vec<u64> = first
            .stats
            .iter()
            .zip(&last.stats)
            .map(|(before, after)| after.thread_cpu_ns.saturating_sub(before.thread_cpu_ns))
            .collect();
        let server_total: u64 = server_ns.iter().sum();
        let server_max = server_ns.iter().copied().max().unwrap_or(0) as f64 / 1e9 / secs;
        // the host's busy time: the server's host, and this one when it is another
        let server_host = last.stats[0].host_busy_ns.saturating_sub(first.stats[0].host_busy_ns);
        let client_host = last.host_ns.saturating_sub(first.host_ns);
        let host_ns = if self.same_host { server_host } else { server_host + client_host };
        let per_gib = |ns: u64| if gib > 0.0 { ns as f64 / 1e6 / gib } else { 0.0 };
        let mismatches: u64 = streams.iter().map(|shared| shared.mismatches.load(Ordering::Acquire)).sum();
        let ktls = streams.iter().all(|shared| shared.ktls.load(Ordering::Acquire));
        SideOut::new(
            cell,
            name,
            &[
                ("mib_s", mib_s),
                ("client_cpu_ms_gib", per_gib(thread_ns)),
                ("client_busy", busy / cores as f64),
                ("client_core_busy", core_busy),
                ("capacity_mib_s", if busy > 0.0 { mib_s / busy } else { 0.0 }),
                ("server_cpu_ms_gib", per_gib(server_total)),
                ("server_busy_max", server_max),
                ("host_cpu_ms_gib", per_gib(host_ns)),
                ("client_host_cpu_ms_gib", if self.same_host { 0.0 } else { per_gib(client_host) }),
                ("mismatches", mismatches as f64),
                ("ktls", f64::from(u8::from(ktls))),
                ("streams", cores as f64),
            ],
        )
    }
}

/// The sides of the put cells: the baseline, then each generator's fill and fill with its CRC
///
/// # Arguments
///
/// * `generators` - The generators
#[must_use]
pub fn put_sides(generators: &[Arc<dyn Generator>]) -> Vec<(String, Side)> {
    let mut sides = vec![("pattern".to_string(), Side::Put(Payload::Pattern))];
    for (at, generator) in generators.iter().enumerate() {
        sides.push((format!("fill {}", generator.name()), Side::Put(Payload::Fill(at))));
        sides.push((format!("fill+crc {}", generator.name()), Side::Put(Payload::FillCrc(at))));
    }
    sides
}

/// The sides of the get cells: unchecked, checked by the ledger, then made again by each generator
///
/// # Arguments
///
/// * `generators` - The generators
#[must_use]
pub fn get_sides(generators: &[Arc<dyn Generator>]) -> Vec<(String, Side)> {
    let mut sides = vec![
        ("none".to_string(), Side::Get(Check::None)),
        ("crc".to_string(), Side::Get(Check::Crc)),
    ];
    for (at, generator) in generators.iter().enumerate() {
        sides.push((format!("regenerate {}", generator.name()), Side::Get(Check::Regenerate(at))));
    }
    sides
}

/// Run section two, or five across the network: puts and gets of 1 MiB frames, plaintext and kTLS
///
/// # Arguments
///
/// * `ctx` - What the run is run with
/// * `stub` - The server and the client's runtimes
/// * `round` - The round, which orders the sides
/// * `next_id` - The next stream id no stream of this run has used
#[must_use]
pub fn streams_section(ctx: &Ctx, stub: &Stub, round: u32, next_id: &mut u64) -> Vec<SideOut> {
    let mut outs = Vec::new();
    for tls in [false, true] {
        let encryption = if tls { "ktls" } else { "plain" };
        for (verb, sides) in [("put", put_sides(&stub.generators)), ("get", get_sides(&stub.generators))] {
            let cell = format!("{verb} 1M {encryption}");
            for (name, side) in ordered(&sides, round) {
                outs.push(stub.run(ctx, &cell, &name, side, tls, 1, next_id));
            }
        }
    }
    outs
}

/// Run section 3b: the put with each published generator's fill and CRC, plaintext, from one
/// client core and then from more, each its own stream to an executor of its own
///
/// # Arguments
///
/// * `ctx` - What the run is run with
/// * `stub` - The server and the client's runtimes
/// * `round` - The round, which orders the sides
/// * `next_id` - The next stream id no stream of this run has used
#[must_use]
pub fn cores_section(ctx: &Ctx, stub: &Stub, round: u32, next_id: &mut u64) -> Vec<SideOut> {
    let mut outs = Vec::new();
    let sides: Vec<(String, Side)> = stub
        .generators
        .iter()
        .enumerate()
        .filter(|(_, generator)| generator.published())
        .map(|(at, generator)| (format!("fill+crc {}", generator.name()), Side::Put(Payload::FillCrc(at))))
        .collect();
    for &count in &ctx.stream_counts {
        let cell = format!("put 1M plain x{count}");
        for (name, side) in ordered(&sides, round) {
            outs.push(stub.run(ctx, &cell, &name, side, false, count, next_id));
        }
    }
    outs
}

/// Wait until every executor accepts a connection
///
/// # Arguments
///
/// * `target` - Where the server is
/// * `executors` - How many executors it has
pub fn wait_listening(target: &Target, executors: usize) {
    let start = Instant::now();
    for executor in 0..executors {
        let port = server::plain_port(target.base_port, executor);
        while std::net::TcpStream::connect((target.host.as_str(), port)).is_err() {
            assert!(start.elapsed().as_secs() < 300, "the server did not listen on {port}");
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

/// A plaintext connection to every executor, on the control runtime, for its counters
///
/// # Arguments
///
/// * `rt` - The control runtime
/// * `target` - Where the server is
/// * `executors` - How many executors it has
#[must_use]
pub fn controls(rt: &Rt, target: &Target, executors: usize) -> Vec<Arc<Conn>> {
    (0..executors)
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
            Arc::new(rt.run(async move { Conn::open(target, dial).await }))
        })
        .collect()
}
