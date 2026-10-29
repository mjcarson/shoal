//! X12: a rebuild across the lab's hosts, whose survivors arrive over 1 GbE
//!
//! A slice's chunks are rebuilt from `k` survivors on other hosts, so a rebuild's rate across
//! hosts is bounded by what its destination's link takes in: about 112 MiB/s on the lab, which X11
//! measured, divided by `k`. This puts that to the lab. `shoal-spike device serve` runs on each
//! source host, a glommio executor that answers a request for a chunk of its populations with the
//! chunk read whole and verified, as a source never sends a chunk that fails its checksum. On the
//! destination, `rebuild-net` fetches every survivor of a stripe at once, each from a peer of its
//! own where there are enough, verifies what it receives, decodes the lost chunk and writes it
//! whole into its pool, as `rebuild`'s destination does with its survivors in memory, beside the
//! same foreground. The connections are plain TCP: at 1 GbE the link, not kernel TLS, is the bound
//! (X11). Every host's populations are made from the same seeds, so the destination checks what it
//! rebuilt against its own tables.

use std::path::PathBuf;
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use futures::{AsyncReadExt, AsyncWriteExt};
use glommio::net::{TcpListener, TcpStream};

use super::background::{self, DestPool, OutBuffers, Tally};
use super::foreground::{self, Fg, Load};
use super::io::{self, HEADER};
use super::paced::{until, Pace, Pacer, Window};
use super::rebuild::{rebuild_one, Paths, Rebuilt, Rig, PIECE, POOL_OBJECTS, POOL_SLOTS};
use super::stats::{fmt, Rng};
use super::stripes::{self, ChunkHeader, Codec, HeldChunk, Layout, Population, CHUNK, UNITS};
use super::table::Table;
use super::{on_core, ordered, Ctx, SideOut};

/// What 1 GbE carries between two of the lab's hosts, MiB a second, as X11 measured it
pub const LINK_MIB_S: f64 = 112.0;

/// The port a source serves on, unless told another
pub const PORT: u16 = 13400;

/// Rebuilds in flight at the destination
const STREAMS: usize = 2;

/// A request's length: the layout, the position, then the stripe
const REQUEST: usize = 16;

/// An answer's status: the chunk follows
const SENT: u64 = 0;

/// An answer's status: the chunk failed its checksum on the source, which sends nothing
const CORRUPT: u64 = 1;

/// Answer requests on one connection until it closes
///
/// # Arguments
///
/// * `populations` - The 4+2 and 2+1 populations
/// * `stream` - The connection
async fn answer(populations: Rc<(Population, Population)>, mut stream: TcpStream) {
    let mut request = [0_u8; REQUEST];
    let mut served = 0_u64;
    while stream.read_exact(&mut request).await.is_ok() {
        // which chunk
        let layout = Layout::from_code(request[0]).expect("a layout the spike knows");
        let position = usize::from(request[1]);
        let stripe = u64::from_le_bytes(request[8..16].try_into().expect("eight")) as usize;
        let population = if layout.population() == Layout::Rs21 { &populations.1 } else { &populations.0 };
        let file = &population.files[stripe][position];
        // read whole, four pieces at once
        let header = file.read_at_aligned(0, HEADER as usize).await.expect("a header");
        let pieces = join_all((0..CHUNK / PIECE).map(|index| file.read_at_aligned(HEADER + index * PIECE, PIECE as usize))).await;
        let pieces: Vec<_> = pieces.into_iter().map(|piece| piece.expect("a piece")).collect();
        // verified before it leaves: a corrupt chunk is never a source
        let sound = ChunkHeader::decode(&header).is_some_and(|header| {
            (0..UNITS).all(|unit| {
                let at = unit as u64 * stripes::UNIT;
                let piece = &pieces[(at / PIECE) as usize];
                let offset = (at % PIECE) as usize;
                stripes::crc(&piece[offset..offset + stripes::UNIT as usize]) == header.crcs[unit]
            })
        });
        let status = if sound { SENT } else { CORRUPT };
        if stream.write_all(&status.to_le_bytes()).await.is_err() {
            return;
        }
        if sound {
            if stream.write_all(&header).await.is_err() {
                return;
            }
            for piece in &pieces {
                if stream.write_all(piece).await.is_err() {
                    return;
                }
            }
        }
        served += 1;
    }
    eprintln!("x12 serve: a connection closed after {served} chunks");
}

/// Serve chunks of this host's populations until killed
///
/// # Arguments
///
/// * `dir` - The scratch directory the populations are under
/// * `core` - The executor's cpu
/// * `sibling` - Its blocking thread's cpu
/// * `port` - The port
/// * `quick` - Whether the populations are a quick run's
pub fn serve(dir: PathBuf, core: usize, sibling: Option<usize>, port: u16, quick: bool) {
    let base = dir.join("x12");
    let (rs42, rs21) = (Layout::Rs42.stripes(quick), Layout::Rs21.stripes(quick));
    on_core(core, sibling, move || async move {
        // the populations, made as every host makes them
        stripes::populate(stripes::population_dir(&base, Layout::Rs42), Layout::Rs42, rs42).await;
        stripes::populate(stripes::population_dir(&base, Layout::Rs21), Layout::Rs21, rs21).await;
        let populations = Rc::new((
            Population::open(&stripes::population_dir(&base, Layout::Rs42), Layout::Rs42, rs42).await,
            Population::open(&stripes::population_dir(&base, Layout::Rs21), Layout::Rs21, rs21).await,
        ));
        let listener = TcpListener::bind(("0.0.0.0", port)).expect("the port is free");
        eprintln!("x12 serve: listening on port {port} on cpu {core}");
        loop {
            let stream = listener.accept().await.expect("a connection");
            stream.set_nodelay(true).expect("nodelay");
            glommio::spawn_local(answer(populations.clone(), stream)).detach();
        }
    });
}

/// Fetch one chunk from a source into a buffer, or `None` if the source refused it
///
/// # Arguments
///
/// * `stream` - The connection to the source
/// * `layout` - The stripe's layout
/// * `stripe` - The stripe
/// * `position` - The position
/// * `bytes` - A chunk's buffer, reused
async fn fetch(stream: &mut TcpStream, layout: Layout, stripe: usize, position: usize, mut bytes: Vec<u8>) -> (Option<HeldChunk>, Vec<u8>) {
    let mut request = [0_u8; REQUEST];
    request[0] = layout.population().code();
    request[1] = position as u8;
    request[8..16].copy_from_slice(&(stripe as u64).to_le_bytes());
    stream.write_all(&request).await.expect("a request sent");
    let mut status = [0_u8; 8];
    stream.read_exact(&mut status).await.expect("an answer");
    if u64::from_le_bytes(status) != SENT {
        return (None, bytes);
    }
    let mut header = vec![0_u8; HEADER as usize];
    stream.read_exact(&mut header).await.expect("a header");
    bytes.resize(CHUNK as usize, 0);
    stream.read_exact(&mut bytes).await.expect("a chunk");
    let header = ChunkHeader::decode(&header).expect("a header the spike wrote");
    (Some(HeldChunk { header, bytes }), Vec::new())
}

/// One rebuild stream whose survivors come over the network
///
/// # Arguments
///
/// * `rig` - The round's rig
/// * `layout` - The stripes rebuilt
/// * `peers` - The sources, each `host:port`
/// * `stream` - This stream's number
/// * `window` - The side's window
/// * `tally` - Where counted bytes go
/// * `start` - The placement group the walk starts at
async fn net_stream(rig: Rc<Rig>, layout: Layout, peers: Vec<String>, stream: usize, window: Window, tally: Rc<Tally>, start: usize) -> Rebuilt {
    // a connection to every peer, made before the window starts
    let mut conns = Vec::with_capacity(peers.len());
    for peer in &peers {
        let conn = TcpStream::connect(peer.as_str()).await.unwrap_or_else(|error| panic!("connecting to {peer}: {error}"));
        conn.set_nodelay(true).expect("nodelay");
        conns.push(conn);
    }
    let mut pacer = Pacer::new(Pace::Unbounded, PIECE, rig.fg.gauge.clone());
    let codec = rig.codec(layout);
    let population = rig.population(layout);
    let walk = population.walk(start, false);
    let mut out = OutBuffers::new(PIECE);
    let mut spare: Vec<Vec<u8>> = Vec::new();
    let mut seen = Rebuilt::default();
    until(window.start).await;
    let mut nth = stream;
    while Instant::now() < window.end {
        let stripe = walk[nth % walk.len()];
        let lost = stripe % if layout == Layout::Copy { Layout::Rs42.k() } else { layout.width() };
        let survivors = codec.survivors(lost);
        let began = Instant::now();
        // the survivors dealt over the peers, each peer's fetched in turn on its connection and
        // the peers at once
        let mut dealt: Vec<Vec<(usize, usize)>> = vec![Vec::new(); conns.len()];
        for (index, &position) in survivors.iter().enumerate() {
            dealt[(index + nth) % conns.len()].push((index, position));
        }
        let buffers: Vec<Vec<Vec<u8>>> = dealt.iter().map(|share| share.iter().map(|_| spare.pop().unwrap_or_default()).collect()).collect();
        let fetched = join_all(conns.iter_mut().zip(dealt.iter().zip(buffers)).map(|(conn, (share, buffers))| async move {
            let mut got = Vec::with_capacity(share.len());
            for (&(index, position), buffer) in share.iter().zip(buffers) {
                let (held, left) = fetch(conn, layout, stripe, position, buffer).await;
                got.push((index, held, left));
            }
            got
        }))
        .await;
        let mut sources: Vec<Option<HeldChunk>> = (0..survivors.len()).map(|_| None).collect();
        let mut refused = false;
        for (index, held, left) in fetched.into_iter().flatten() {
            refused |= held.is_none();
            // a buffer a refusal left unused is kept
            if left.capacity() > 0 {
                spare.push(left);
            }
            sources[index] = held;
        }
        let received = Instant::now();
        if window.counts(began, received) {
            tally.received.set(tally.received.get() + (CHUNK + HEADER) * survivors.len() as u64);
        }
        if refused {
            seen.source_failures += 1;
            nth += STREAMS;
            continue;
        }
        let held: Vec<HeldChunk> = sources.into_iter().map(|source| source.expect("every survivor")).collect();
        let refs: Vec<&HeldChunk> = held.iter().collect();
        let expect = population.tables[stripe][lost];
        out = rebuild_one(&rig, codec, &refs, lost, &expect, stripe, nth % POOL_OBJECTS, out, 8, &mut pacer, &tally, window, &mut seen).await;
        let ended = Instant::now();
        if window.counts(began, ended) {
            seen.chunks += 1;
            seen.took.push(ended - began);
        }
        // the buffers kept for the next stripe
        spare.extend(held.into_iter().map(|chunk| chunk.bytes));
        nth += STREAMS;
    }
    seen
}

/// Run the rebuild across hosts for one round, on the destination
///
/// # Arguments
///
/// * `ctx` - The run
/// * `round` - The round
pub fn run(ctx: &Ctx, round: u32) {
    super::require_write_through(ctx);
    let peers = ctx.x12.peers.clone();
    assert!(!peers.is_empty(), "rebuild-net needs --peers host:port,host:port");
    let paths = Paths::of(ctx);
    paths.populate(ctx);
    let devices = ctx.facts.devices.clone();
    let (rotational, form) = (ctx.rotational(), ctx.dir_sync());
    let goal_us = ctx.x12.goal_us;
    let windows = (ctx.window(Duration::from_secs(3)), ctx.window(ctx.x12.counted()));
    let sides: Vec<Option<Layout>> = vec![None, Some(Layout::Copy), Some(Layout::Rs21), Some(Layout::Rs42)];
    let outs = on_core(ctx.core, ctx.sibling, move || async move {
        let fg = Fg::open(&paths.places, Load::for_device(rotational)).await;
        let rs42 = Population::open(&stripes::population_dir(&paths.base, Layout::Rs42), Layout::Rs42, paths.rs42).await;
        let rs21 = Population::open(&stripes::population_dir(&paths.base, Layout::Rs21), Layout::Rs21, paths.rs21).await;
        let pool = DestPool::make(&paths.pool, POOL_OBJECTS, POOL_SLOTS, form).await;
        let codecs = vec![Codec::new(Layout::Copy), Codec::new(Layout::Rs21), Codec::new(Layout::Rs42)];
        let rig = Rc::new(Rig { fg, rs42, rs21, ring: Vec::new(), pool, codecs, queue: background::queue(goal_us), devices, rotational, goal_us });
        let mut outs = Vec::new();
        for layout in ordered(&sides, round) {
            let window = Window::new(windows.0, windows.1);
            let edges = background::edges(rig.devices.clone(), window);
            let running = rig.fg.start(window, Rng::new(0x9e7 ^ u64::from(round)).next());
            let tally = Rc::new(Tally::default());
            let start = (Rng::new(u64::from(round) << 8).next() % stripes::PGS as u64) as usize;
            let tasks: Vec<_> = layout
                .map(|layout| {
                    (0..STREAMS)
                        .map(|stream| {
                            let (rig, tally, peers) = (rig.clone(), tally.clone(), peers.clone());
                            let queue = rig.queue;
                            glommio::spawn_local_into(net_stream(rig, layout, peers, stream, window, tally, start), queue).expect("the queue")
                        })
                        .collect()
                })
                .unwrap_or_default();
            let seen = running.finish().await;
            let streams = join_all(tasks).await;
            let (delta, cpu_ns) = edges.await;
            let (mut chunks, mut mismatches, mut refused) = (0, 0, 0);
            let mut took = super::stats::Samples::default();
            for one in &streams {
                chunks += one.chunks;
                mismatches += one.mismatches;
                refused += one.source_failures;
                took.extend(&one.took);
            }
            let secs = window.secs();
            let mib = |bytes: u64| bytes as f64 / secs / f64::from(1 << 20);
            let k = layout.map_or(1, Layout::k) as f64;
            let took = took.summary();
            let mut figures: Vec<(String, f64)> = vec![
                ("rebuilt_mib_s".into(), mib(tally.rebuilt.get())),
                ("net_mib_s".into(), mib(tally.received.get())),
                ("bound_mib_s".into(), if layout.is_some() { LINK_MIB_S / k } else { 0.0 }),
                ("chunks".into(), chunks as f64),
                ("mismatches".into(), mismatches as f64),
                ("source_failures".into(), refused as f64),
                ("chunk_p50".into(), took.p50),
                ("chunk_p99".into(), took.tail()),
                ("exec_busy".into(), background::busy(cpu_ns, &window)),
                ("busy".into(), delta.busy()),
                ("dev_write_mib_s".into(), delta.written as f64 / delta.secs.max(1e-9) / f64::from(1 << 20)),
                ("peers".into(), peers.len() as f64),
                ("rotational".into(), if rig.rotational { 1.0 } else { 0.0 }),
        ("journal_apart".into(), if rig.fg.journal_apart { 1.0 } else { 0.0 }),
        ("goal_us".into(), rig.goal_us as f64),
            ];
            figures.extend(foreground::figures(&seen));
            let figures: Vec<(&str, f64)> = figures.iter().map(|(name, value)| (name.as_str(), *value)).collect();
            outs.push(SideOut::new("net", layout.map_or("none", Layout::name), &figures));
            super::settle(&paths.pool).await;
        }
        if let Ok(rig) = Rc::try_unwrap(rig) {
            rig.fg.close().await;
            rig.rs42.close().await;
            rig.rs21.close().await;
        }
        io::wipe(&paths.pool);
        outs
    });
    let mut table = Table::new(&["side", "rebuilt MiB/s", "received MiB/s", "bound MiB/s", "chunk p50 ms", "read p99 ms", "write p99 ms", "mismatches", "refused"]);
    let ms = |out: &SideOut, name: &str| fmt(out.get(name) / 1e3);
    let mut records = Vec::new();
    for out in outs {
        table.row(vec![
            out.side.clone(),
            fmt(out.get("rebuilt_mib_s")),
            fmt(out.get("net_mib_s")),
            fmt(out.get("bound_mib_s")),
            ms(&out, "chunk_p50"),
            ms(&out, "read_p99"),
            ms(&out, "write_p99"),
            fmt(out.get("mismatches")),
            fmt(out.get("source_failures")),
        ]);
        records.push(ctx.record("rebuild-net", round, out));
    }
    print!(
        "{}",
        table.render(
            &format!("X12 · A rebuild across hosts, round {round} (survivors from {}, plain TCP; bound {LINK_MIB_S} MiB/s ÷ k)", ctx.x12.peers.join(" and ")),
            &ctx.label()
        )
    );
    ctx.emit(&records);
}
