//! X11's server: glommio executors pinned to cores, each listening on ports of its own
//!
//! Every executor listens on a plaintext port and, given a certificate, a TLS port of its own, so
//! a client chooses the executor that serves a connection by the port it dials. That is not how
//! Shoal accepts - its shards share a port and the kernel chooses - but every arrangement X11
//! compares needs to know which executor holds which connection, and a shared port would make
//! that the kernel's choice.
//!
//! A connection's first frame says what it is for ([`Setup`]). A write stream's data frames are
//! read straight into buffers for direct I/O and written to the executor's file, or dropped,
//! with at most the setup's window of them held at once; a range asked for is read from the file
//! or sliced from the pattern and written back as one data frame. A small request is answered at
//! once. Answers are written by a task of their own, which writes every small frame queued before
//! the next data frame: bytes already handed to the socket are never taken back.
//!
//! Two routes take a write's bytes to the other executor, as a slice owned there would need: as
//! buffers handed across a channel (a hop), or by handing the connection itself over.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::io::IoSlice;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use futures::channel::mpsc;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, StreamExt};
use glommio::channels::shared_channel::{
    self, ConnectedReceiver, ConnectedSender, SharedReceiver, SharedSender,
};
use glommio::io::{DmaBuffer, DmaFile, OpenOptions, ReadResult};
use glommio::net::{TcpListener, TcpStream};
use glommio::{ExecutorJoinHandle, Task};
use rustls::server::ServerConnectionData;
use rustls::ServerConfig;
use shoal::shared::tls::{server_config, Established, TlsServerOptions};

use super::sock;
use super::wire::{self, Header, Kind, Route, Setup, Stats, HEADER_LEN, LAST};
use crate::device::spawn_on;

/// How many ports apart an executor's TLS port is from its plaintext one
pub const TLS_PORT_OFFSET: u16 = 16;

/// How often a connection's socket memory is read
const SAMPLE_EVERY: Duration = Duration::from_millis(5);

/// How the server is run
#[derive(Debug, Clone)]
pub struct ServerConf {
    /// The cpu of each executor, with the sibling its blocking thread runs on
    pub cores: Vec<(usize, Option<usize>)>,
    /// The address to listen on
    pub bind: String,
    /// The plaintext port of executor zero; executor `i` listens on this plus `i`
    pub base_port: u16,
    /// The directory each executor's file is in, if writes are to land anywhere
    pub dir: Option<PathBuf>,
    /// How large each executor's file is
    pub file_bytes: u64,
    /// The certificate and key the TLS ports prove themselves with
    pub cert: Option<(PathBuf, PathBuf)>,
    /// The seed of the pattern every stream carries
    pub seed: u64,
    /// Patterns beyond the seed's own, which a connection's setup names by number from one: a
    /// read is answered from the one it names. X11 gives none; X13's generators give theirs
    pub patterns: Vec<Arc<Vec<u8>>>,
}

/// The plaintext port of an executor
///
/// # Arguments
///
/// * `base` - Executor zero's plaintext port
/// * `executor` - The executor
#[must_use]
pub fn plain_port(base: u16, executor: usize) -> u16 {
    base + executor as u16
}

/// The TLS port of an executor
///
/// # Arguments
///
/// * `base` - Executor zero's plaintext port
/// * `executor` - The executor
#[must_use]
pub fn tls_port(base: u16, executor: usize) -> u16 {
    base + TLS_PORT_OFFSET + executor as u16
}

/// A buffer of a write on its way to the other executor
pub struct HopBuf {
    /// The bytes
    bytes: Moved,
    /// Where in the file they go
    pos: u64,
    /// How many of them are payload
    len: usize,
}

/// The two ways a hop carries bytes
pub enum Moved {
    /// A buffer for direct I/O from the global allocator, moved whole
    Dma(DmaBuffer),
    /// An ordinary allocation, which the other executor copies into a buffer of its own
    Heap(Vec<u8>),
}

// SAFETY: a `Moved::Dma` only ever holds a buffer from `glommio::allocate_dma_buffer_global`, which
// is the fork's `BufferStorage::Sys`: a plain aligned heap allocation with no `Rc` and no tie to
// the executor that made it. The fork marks `DmaBuffer` `!Send` because its other storages are
// registered with one ring. A `Moved::Heap` is a `Vec`, which is `Send` already.
unsafe impl Send for HopBuf {}

// SAFETY: glommio's shared channel asks its items to be `Sync` as well, and a `HopBuf` is never
// reached through a shared reference from two threads: it is moved into the channel by one
// executor and out of it by the other.
unsafe impl Sync for HopBuf {}

/// What one executor asks the other to take on
pub enum Ctl {
    /// Write the buffers that arrive on this channel, and send each back once it is written
    Hop {
        /// Where the buffers arrive
        rx: SharedReceiver<HopBuf>,
        /// Where they go back
        back: SharedSender<HopBuf>,
    },
    /// Serve this connection from here on
    Handoff {
        /// A duplicate of the connection's descriptor
        fd: RawFd,
        /// What the connection asked for
        setup: Setup,
        /// The TLS session, kept for as long as the connection lives
        tls: Option<Established<ServerConnectionData>>,
    },
}

/// An executor's counters
#[derive(Debug, Default)]
pub struct Counters {
    /// Payload bytes taken from streams and finished with
    bytes_in: Cell<u64>,
    /// Payload bytes written to sockets
    bytes_out: Cell<u64>,
    /// Bytes held in buffers now
    held: Cell<u64>,
    /// The most held at once since the last reset
    peak_held: Cell<u64>,
    /// The most socket memory one connection held since the last reset
    peak_sock: Cell<u64>,
    /// Payloads that did not match the pattern
    mismatches: Cell<u64>,
}

impl Counters {
    /// Count bytes now held in a buffer
    ///
    /// # Arguments
    ///
    /// * `bytes` - How many
    fn take(&self, bytes: usize) {
        let held = self.held.get() + bytes as u64;
        self.held.set(held);
        self.peak_held.set(self.peak_held.get().max(held));
    }

    /// Count bytes no longer held
    ///
    /// # Arguments
    ///
    /// * `bytes` - How many
    fn release(&self, bytes: usize) {
        self.held.set(self.held.get().saturating_sub(bytes as u64));
    }

    /// Count bytes a stream finished with
    ///
    /// # Arguments
    ///
    /// * `bytes` - How many
    fn finished(&self, bytes: usize) {
        self.bytes_in.set(self.bytes_in.get() + bytes as u64);
    }
}

/// What every task on one executor shares
struct Exec {
    /// Which executor this is
    index: usize,
    /// The executor's file, if writes land anywhere
    file: Option<Rc<DmaFile>>,
    /// How large the file is
    file_bytes: u64,
    /// The pattern every stream carries
    pattern: Arc<Vec<u8>>,
    /// The patterns beyond it, which a setup names by number from one
    patterns: Vec<Arc<Vec<u8>>>,
    /// Its counters
    counters: Counters,
    /// Where to ask the other executor to take something on
    other: RefCell<Option<ConnectedSender<Ctl>>>,
}

impl Exec {
    /// The pattern a setup names: the seed's own at zero, or one of those beyond it
    ///
    /// # Arguments
    ///
    /// * `which` - The setup's pattern
    fn pattern_for(&self, which: u8) -> Arc<Vec<u8>> {
        // zero, or a number no pattern has, is the seed's own
        match usize::from(which).checked_sub(1).and_then(|at| self.patterns.get(at)) {
            Some(pattern) => pattern.clone(),
            None => self.pattern.clone(),
        }
    }
}

/// Start the server's executors, each serving its ports until the process ends
///
/// # Arguments
///
/// * `conf` - How to run it
#[must_use]
pub fn start(conf: &ServerConf) -> Vec<ExecutorJoinHandle<()>> {
    let count = conf.cores.len();
    // a channel into each executor, whose sender the next executor holds
    let mut receivers = Vec::with_capacity(count);
    let mut senders = Vec::with_capacity(count);
    for _ in 0..count {
        let (tx, rx) = shared_channel::new_bounded::<Ctl>(16);
        senders.push(Some(tx));
        receivers.push(Some(rx));
    }
    let mut handles = Vec::with_capacity(count);
    for (index, &(cpu, sibling)) in conf.cores.iter().enumerate() {
        // this executor's own channel, and the sender into the next one round
        let rx = receivers[index]
            .take()
            .expect("each receiver is taken once");
        let to_other = senders[(index + 1) % count].take().filter(|_| count > 1);
        let conf = conf.clone();
        handles.push(spawn_on(cpu, sibling, move || async move {
            run_executor(index, conf, rx, to_other).await;
        }));
    }
    handles
}

/// One executor: its file, its listeners and its channel from the other executor
///
/// # Arguments
///
/// * `index` - Which executor this is
/// * `conf` - How the server is run
/// * `rx` - Where the other executor asks it to take something on
/// * `to_other` - Where it asks the other executor
async fn run_executor(
    index: usize,
    conf: ServerConf,
    rx: SharedReceiver<Ctl>,
    to_other: Option<SharedSender<Ctl>>,
) {
    // the file every write lands in, written ahead with the pattern once
    let pattern = Arc::new(wire::pattern(conf.seed));
    let file = match &conf.dir {
        Some(dir) => Some(Rc::new(
            prepare_file(
                dir.join(format!("x11-e{index}.dat")),
                conf.file_bytes,
                &pattern,
            )
            .await,
        )),
        None => None,
    };
    let exec = Rc::new(Exec {
        index,
        file,
        file_bytes: conf.file_bytes,
        pattern,
        patterns: conf.patterns.clone(),
        counters: Counters::default(),
        other: RefCell::new(None),
    });
    // both ends of both channels are connected at once: a connect waits for the other end's, so
    // two executors each connecting their sender first would wait for each other forever
    let (other, ctl) = futures::join!(
        async move {
            match to_other {
                Some(tx) => Some(tx.connect().await),
                None => None,
            }
        },
        rx.connect()
    );
    *exec.other.borrow_mut() = other;
    // the other executor's requests, served for as long as it sends them
    glommio::spawn_local(serve_ctl(exec.clone(), ctl)).detach();
    // the plaintext port, and the TLS port when there is a certificate
    let plain = TcpListener::bind((conf.bind.as_str(), plain_port(conf.base_port, index)))
        .expect("the plaintext port binds");
    glommio::spawn_local(accept_loop(exec.clone(), plain, None)).detach();
    if let Some((cert, key)) = &conf.cert {
        let options = TlsServerOptions {
            cert: cert.clone(),
            key: key.clone(),
        };
        let config = server_config(&options).expect("the certificate loads");
        let tls = TcpListener::bind((conf.bind.as_str(), tls_port(conf.base_port, index)))
            .expect("the TLS port binds");
        glommio::spawn_local(accept_loop(exec.clone(), tls, Some(config))).detach();
    }
    eprintln!(
        "x11: executor {index} serving on port {}",
        plain_port(conf.base_port, index)
    );
    // the executor runs until the process ends
    futures::future::pending::<()>().await;
}

/// Open the executor's file, writing it ahead with the pattern if it is not already its size
///
/// # Arguments
///
/// * `path` - Where it is
/// * `bytes` - How large it is
/// * `pattern` - What it is filled with
async fn prepare_file(path: PathBuf, bytes: u64, pattern: &[u8]) -> DmaFile {
    // an existing file of the right size is taken as it is
    let fresh = std::fs::metadata(&path).map_or(true, |meta| meta.len() != bytes);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .dma_open(&path)
        .await
        .expect("the executor's file opens");
    if fresh {
        // written a pattern's length at a time, so a read finds the pattern and a write
        // overwrites blocks that are already allocated, as X6's pool written ahead does
        let step = (8 << 20).min(bytes) as usize;
        let mut pos = 0;
        while pos < bytes {
            let mut buf = file.alloc_dma_buffer(step);
            let start = wire::pattern_at(pos);
            buf.as_bytes_mut()
                .copy_from_slice(&pattern[start..start + step]);
            file.write_at(buf, pos)
                .await
                .expect("the file is written ahead");
            pos += step as u64;
        }
        file.fdatasync().await.expect("the file is synced");
    }
    file
}

/// Serve what the other executor asks of this one
///
/// # Arguments
///
/// * `exec` - This executor
/// * `rx` - Its requests
async fn serve_ctl(exec: Rc<Exec>, rx: ConnectedReceiver<Ctl>) {
    while let Some(ctl) = rx.recv().await {
        match ctl {
            // a hop: write what arrives, then send each buffer back
            Ctl::Hop { rx, back } => {
                glommio::spawn_local(hop_writer(exec.clone(), rx, back)).detach();
            }
            // a connection handed over: adopt the descriptor on this thread and serve it
            Ctl::Handoff { fd, setup, tls } => {
                // SAFETY: the descriptor is a `dup` the other executor made and gave up, so this
                // is its only owner
                let stream = unsafe { TcpStream::from_raw_fd(fd) };
                glommio::spawn_local(serve(exec.clone(), stream, tls, Some(setup))).detach();
            }
        }
    }
}

/// Accept connections on one listener for as long as it is open
///
/// # Arguments
///
/// * `exec` - This executor
/// * `listener` - The listener
/// * `tls` - The TLS configuration, if this is the TLS port
async fn accept_loop(exec: Rc<Exec>, listener: TcpListener, tls: Option<Arc<ServerConfig>>) {
    loop {
        let mut stream = match listener.accept().await {
            Ok(stream) => stream,
            Err(error) => {
                eprintln!("x11: accept failed: {error}");
                continue;
            }
        };
        let exec = exec.clone();
        let tls = tls.clone();
        glommio::spawn_local(async move {
            // Nagle off before the handshake, as Shoal sets it
            let _ = sock::set_nodelay(stream.as_raw_fd());
            // the handshake, then kTLS, on the executor that accepted
            let session = match &tls {
                Some(config) => match shoal::server::tls::accept(&mut stream, config).await {
                    Ok(session) => Some(session),
                    Err(error) => {
                        eprintln!("x11: a TLS handshake failed: {error}");
                        return;
                    }
                },
                None => None,
            };
            serve(exec, stream, session, None).await;
        })
        .detach();
    }
}

/// What a connection's writer is given
enum Out {
    /// A whole frame written before any data frame still queued
    Urgent(Vec<u8>),
    /// A data frame
    Data {
        /// Its header and head
        head: [u8; HEADER_LEN + wire::DATA_HEAD_LEN],
        /// Its payload
        payload: Payload,
    },
}

/// Where a data frame's payload is
enum Payload {
    /// A slice of the pattern, from here for this many bytes
    Pattern(usize, usize),
    /// What a read of the file returned
    File(ReadResult),
}

/// Serve one connection until its peer closes it
///
/// # Arguments
///
/// * `exec` - This executor
/// * `stream` - The connection
/// * `tls` - Its TLS session, if it has one
/// * `handed` - The setup it arrived with, if another executor handed it over
async fn serve(
    exec: Rc<Exec>,
    mut stream: TcpStream,
    tls: Option<Established<ServerConnectionData>>,
    handed: Option<Setup>,
) {
    let fd = stream.as_raw_fd();
    // the setup: read here, or the one the connection was handed over with
    let setup = match handed {
        Some(setup) => setup,
        None => {
            let Ok(header) = read_header(&mut stream).await else {
                return;
            };
            let mut body = vec![0u8; header.len as usize];
            if header.kind != Kind::Setup || stream.read_exact(&mut body).await.is_err() {
                eprintln!("x11: a connection did not open with a setup");
                return;
            }
            Setup::decode(&body)
        }
    };
    // a connection that asks to be handed over goes, whole, before anything else is read
    if setup.route == Route::Handoff && handed.is_none() {
        // a duplicate the other executor owns, and this executor's copy closed
        // SAFETY: dup has no preconditions; a failure is checked
        let dup = unsafe { libc::dup(fd) };
        assert!(dup >= 0, "the descriptor duplicates");
        drop(stream);
        let other = exec.other.borrow();
        let other = other.as_ref().expect("a handoff needs a second executor");
        if other
            .send(Ctl::Handoff {
                fd: dup,
                setup,
                tls,
            })
            .await
            .is_err()
        {
            eprintln!("x11: the other executor is gone");
        }
        return;
    }
    // the socket options the setup asks for
    if setup.lowat > 0 {
        let _ = sock::set_notsent_lowat(fd, setup.lowat);
    }
    if setup.nopad && tls.is_some() {
        if let Err(error) = sock::set_rx_no_pad(fd) {
            eprintln!("x11: TLS_RX_EXPECT_NO_PAD refused: {error}");
        }
    }
    // the writer, which every answer goes through
    let (reader, writer) = stream.split();
    let (tx, rx) = mpsc::unbounded::<Out>();
    let served = exec.pattern_for(setup.pattern);
    let writing = glommio::spawn_local(write_loop(exec.clone(), writer, rx, setup.fifo, served));
    // the socket's memory, read while the connection lives
    let alive = Rc::new(Cell::new(true));
    glommio::spawn_local(sample(exec.clone(), fd, alive.clone())).detach();
    // say which executor serves this, and whether kTLS holds its socket
    let mut ok = vec![0u8; 16];
    ok[..8].copy_from_slice(&(exec.index as u64).to_le_bytes());
    ok[8] = u8::from(sock::is_ktls(fd));
    let _ = tx.unbounded_send(Out::Urgent(frame(Kind::SetupOk, 0, &ok)));
    // read frames until the peer closes
    if let Err(error) = read_loop(&exec, reader, &tx, setup).await {
        if error.kind() != std::io::ErrorKind::UnexpectedEof {
            eprintln!("x11: a connection ended: {error}");
        }
    }
    alive.set(false);
    drop(tx);
    let _ = writing.await;
    // the session outlives the socket's last byte
    drop(tls);
}

/// Read the next header
///
/// # Arguments
///
/// * `reader` - The connection
async fn read_header<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<Header> {
    let mut bytes = [0u8; HEADER_LEN];
    reader.read_exact(&mut bytes).await?;
    Header::decode(&bytes)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}

/// A whole frame as one buffer
///
/// # Arguments
///
/// * `kind` - What it is
/// * `flags` - Its flags
/// * `body` - Its body
#[must_use]
pub fn frame(kind: Kind, flags: u16, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + body.len());
    out.extend_from_slice(&Header::new(kind, flags, body.len()).encode());
    out.extend_from_slice(body);
    out
}

/// A write stream being received
struct WriteState {
    /// The stream's id
    id: u64,
    /// Payload bytes received
    bytes: u64,
    /// The writes of its buffers still in flight, oldest first
    inflight: VecDeque<Task<()>>,
    /// The hop carrying it to the other executor, if it goes there
    hop: Option<Hop>,
}

/// The accepting executor's half of a hop
struct Hop {
    /// Where buffers go
    tx: ConnectedSender<HopBuf>,
    /// Where they come back
    back: ConnectedReceiver<HopBuf>,
    /// Buffers sent and not yet back
    out: usize,
    /// Buffers back and ready to fill
    free: Vec<HopBuf>,
}

/// Read a connection's frames and act on each
///
/// # Arguments
///
/// * `exec` - This executor
/// * `reader` - The connection's read half
/// * `tx` - Its writer
/// * `setup` - What it is for
async fn read_loop<R: AsyncRead + Unpin>(
    exec: &Rc<Exec>,
    mut reader: R,
    tx: &mpsc::UnboundedSender<Out>,
    setup: Setup,
) -> std::io::Result<()> {
    let window = setup.window.max(1) as usize;
    let mut write: Option<WriteState> = None;
    loop {
        let header = read_header(&mut reader).await?;
        match header.kind {
            // a write stream opens; a hop to the other executor is set up with it
            Kind::Write => {
                let mut body = [0u8; 16];
                reader.read_exact(&mut body).await?;
                let hop = match setup.route {
                    Route::Hop | Route::HopCopy => Some(open_hop(exec, window).await),
                    _ => None,
                };
                write = Some(WriteState {
                    id: wire::word(&body, 0),
                    bytes: 0,
                    inflight: VecDeque::new(),
                    hop,
                });
            }
            // bytes of a write stream
            Kind::Data => {
                let mut head = [0u8; wire::DATA_HEAD_LEN];
                reader.read_exact(&mut head).await?;
                let offset = wire::word(&head, 2);
                let len = header.len as usize - wire::DATA_HEAD_LEN;
                let state = write
                    .as_mut()
                    .ok_or_else(|| invalid("data with no stream open"))?;
                receive(exec, &mut reader, state, &setup, window, offset, len).await?;
                // the last frame: wait for every write of the stream, then say so
                if header.flags & LAST != 0 {
                    let mut state = write.take().expect("the stream is open");
                    while let Some(task) = state.inflight.pop_front() {
                        task.await;
                    }
                    if let Some(hop) = state.hop.as_mut() {
                        while hop.out > 0 {
                            let buf = hop
                                .back
                                .recv()
                                .await
                                .expect("the other executor sends every buffer back");
                            hop.out -= 1;
                            hop.free.push(buf);
                        }
                        for buf in hop.free.drain(..) {
                            exec.counters.release(buf.len);
                        }
                    }
                    let mut body = Vec::with_capacity(32);
                    body.extend_from_slice(&wire::two_words(state.id, state.bytes));
                    body.extend_from_slice(&wire::two_words(
                        exec.counters.peak_held.get(),
                        exec.counters.peak_sock.get(),
                    ));
                    let _ = tx.unbounded_send(Out::Urgent(frame(Kind::Done, 0, &body)));
                }
            }
            // a range asked for, answered from the file or the pattern
            Kind::Read => {
                let mut body = [0u8; 24];
                reader.read_exact(&mut body).await?;
                let (id, offset, len) = (
                    wire::word(&body, 0),
                    wire::word(&body, 1),
                    wire::word(&body, 2) as usize,
                );
                let mut head = [0u8; HEADER_LEN + wire::DATA_HEAD_LEN];
                head[..HEADER_LEN].copy_from_slice(
                    &Header::new(Kind::Data, LAST, wire::DATA_HEAD_LEN + len).encode(),
                );
                head[HEADER_LEN..].copy_from_slice(&wire::data_head(id, offset));
                match (&exec.file, setup.file) {
                    // a read of the file, in flight beside the others the client asked for
                    (Some(file), true) => {
                        let (file, exec, tx) = (file.clone(), exec.clone(), tx.clone());
                        let pos = offset % exec.file_bytes;
                        glommio::spawn_local(async move {
                            let result = file
                                .read_at_aligned(pos, len)
                                .await
                                .expect("the file reads");
                            exec.counters.take(len);
                            let _ = tx.unbounded_send(Out::Data {
                                head,
                                payload: Payload::File(result),
                            });
                        })
                        .detach();
                    }
                    // a slice of the pattern, which costs no read
                    _ => {
                        let _ = tx.unbounded_send(Out::Data {
                            head,
                            payload: Payload::Pattern(wire::pattern_at(offset), len),
                        });
                    }
                }
            }
            // a small request, answered at once
            Kind::Small => {
                let mut body = [0u8; wire::SMALL_LEN];
                reader.read_exact(&mut body).await?;
                let mut answer = vec![0u8; wire::ANSWER_LEN];
                answer[..8].copy_from_slice(&body[..8]);
                let _ = tx.unbounded_send(Out::Urgent(frame(Kind::Answer, 0, &answer)));
            }
            // the executor's counters, reset once read if asked
            Kind::Stat => {
                let mut body = [0u8; 8];
                reader.read_exact(&mut body).await?;
                let counters = &exec.counters;
                let stats = Stats {
                    thread_cpu_ns: crate::device::sys::thread_cpu_ns(),
                    host_busy_ns: sock::host_busy_ns(),
                    bytes_in: counters.bytes_in.get(),
                    bytes_out: counters.bytes_out.get(),
                    peak_held: counters.peak_held.get(),
                    peak_sock: counters.peak_sock.get(),
                    mismatches: counters.mismatches.get(),
                };
                if body[0] != 0 {
                    counters.peak_held.set(counters.held.get());
                    counters.peak_sock.set(0);
                }
                let _ = tx.unbounded_send(Out::Urgent(frame(Kind::Stats, 0, &stats.encode())));
            }
            // anything else is a peer out of step
            other => return Err(invalid(&format!("a client sent {other:?}"))),
        }
    }
}

/// A protocol error as an I/O error, which ends the connection
///
/// # Arguments
///
/// * `what` - What was wrong
fn invalid(what: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, what.to_string())
}

/// Ask the other executor to write a stream's buffers, and keep this end of the hop
///
/// # Arguments
///
/// * `exec` - This executor
/// * `window` - How many buffers may be away at once
async fn open_hop(exec: &Rc<Exec>, window: usize) -> Hop {
    let (tx, rx) = shared_channel::new_bounded::<HopBuf>(window + 1);
    let (back_tx, back_rx) = shared_channel::new_bounded::<HopBuf>(window + 1);
    {
        let other = exec.other.borrow();
        let other = other.as_ref().expect("a hop needs a second executor");
        if other.send(Ctl::Hop { rx, back: back_tx }).await.is_err() {
            panic!("the other executor is gone");
        }
    }
    let (tx, back) = futures::join!(tx.connect(), back_rx.connect());
    Hop {
        tx,
        back,
        out: 0,
        free: Vec::new(),
    }
}

/// Take one data frame's payload: into a buffer this executor writes, or one it hands across
///
/// # Arguments
///
/// * `exec` - This executor
/// * `reader` - The connection, at the payload
/// * `state` - The stream
/// * `setup` - What the connection is for
/// * `window` - How many buffers may be held at once
/// * `offset` - The payload's stream offset
/// * `len` - Its length
async fn receive<R: AsyncRead + Unpin>(
    exec: &Rc<Exec>,
    reader: &mut R,
    state: &mut WriteState,
    setup: &Setup,
    window: usize,
    offset: u64,
    len: usize,
) -> std::io::Result<()> {
    let pos = offset % exec.file_bytes;
    state.bytes += len as u64;
    match state.hop.as_mut() {
        // read here, written here
        None => {
            // straight into a buffer for direct I/O: the socket's bytes are the device's
            let mut buf = glommio::allocate_dma_buffer(len);
            reader.read_exact(&mut buf.as_bytes_mut()[..len]).await?;
            exec.counters.take(len);
            if setup.verify {
                check(exec, offset, &buf.as_bytes()[..len]);
            }
            match (&exec.file, setup.file) {
                // written while the next frames are read, at most a window at once
                (Some(file), true) => {
                    let (file, exec) = (file.clone(), exec.clone());
                    state.inflight.push_back(glommio::spawn_local(async move {
                        file.write_at(buf, pos).await.expect("the file writes");
                        exec.counters.release(len);
                        exec.counters.finished(len);
                    }));
                    while state.inflight.len() >= window {
                        state.inflight.pop_front().expect("one is in flight").await;
                    }
                }
                // dropped, which is the wire alone
                _ => {
                    exec.counters.release(len);
                    exec.counters.finished(len);
                }
            }
        }
        // read here, written by the other executor
        Some(hop) => {
            // a buffer back from the other side, or a new one while fewer than a window are out
            let mut buf = match hop.free.pop() {
                Some(buf) => buf,
                None if hop.out < window => {
                    exec.counters.take(len);
                    HopBuf {
                        bytes: match setup.route {
                            Route::HopCopy => Moved::Heap(vec![0u8; len]),
                            _ => Moved::Dma(glommio::allocate_dma_buffer_global(len)),
                        },
                        pos: 0,
                        len,
                    }
                }
                None => {
                    let buf = hop
                        .back
                        .recv()
                        .await
                        .expect("the other executor sends every buffer back");
                    hop.out -= 1;
                    buf
                }
            };
            // the payload into it, then across
            let target = match &mut buf.bytes {
                Moved::Dma(dma) => &mut dma.as_bytes_mut()[..len],
                Moved::Heap(heap) => &mut heap[..len],
            };
            reader.read_exact(target).await?;
            if setup.verify {
                let bytes = match &buf.bytes {
                    Moved::Dma(dma) => &dma.as_bytes()[..len],
                    Moved::Heap(heap) => &heap[..len],
                };
                check(exec, offset, bytes);
            }
            buf.pos = pos;
            buf.len = len;
            if hop.tx.send(buf).await.is_err() {
                return Err(invalid("the other executor is gone"));
            }
            hop.out += 1;
        }
    }
    Ok(())
}

/// Count a payload that is not the pattern at its offset
///
/// # Arguments
///
/// * `exec` - This executor
/// * `offset` - The payload's stream offset
/// * `bytes` - The payload
fn check(exec: &Exec, offset: u64, bytes: &[u8]) {
    let start = wire::pattern_at(offset);
    if exec.pattern[start..start + bytes.len()] != *bytes {
        exec.counters
            .mismatches
            .set(exec.counters.mismatches.get() + 1);
    }
}

/// The other end of a hop: write each buffer, then send it back
///
/// # Arguments
///
/// * `exec` - This executor
/// * `rx` - Where buffers arrive
/// * `back` - Where they go back
async fn hop_writer(exec: Rc<Exec>, rx: SharedReceiver<HopBuf>, back: SharedSender<HopBuf>) {
    let (rx, back) = futures::join!(rx.connect(), back.connect());
    let file = exec.file.clone();
    while let Some(mut buf) = rx.recv().await {
        let len = buf.len;
        match &file {
            Some(file) => {
                // a moved buffer is written as it is; a copied one goes into a buffer of ours
                let (dma, keep) = match std::mem::replace(&mut buf.bytes, Moved::Heap(Vec::new())) {
                    Moved::Dma(dma) => {
                        // the write takes the buffer, so the bytes are copied back out of the
                        // result only to return an allocation of the same kind; a buffer for
                        // direct I/O is made again on the accepting side instead
                        (dma, None)
                    }
                    Moved::Heap(heap) => {
                        let mut dma = glommio::allocate_dma_buffer(len);
                        dma.as_bytes_mut()[..len].copy_from_slice(&heap[..len]);
                        (dma, Some(heap))
                    }
                };
                file.write_at(dma, buf.pos).await.expect("the file writes");
                buf.bytes = match keep {
                    Some(heap) => Moved::Heap(heap),
                    None => Moved::Dma(glommio::allocate_dma_buffer_global(len)),
                };
            }
            None => {}
        }
        exec.counters.finished(len);
        if back.send(buf).await.is_err() {
            break;
        }
    }
}

/// Write a connection's answers: every urgent frame queued before the next data frame, or every
/// frame in the order it was queued
///
/// # Arguments
///
/// * `exec` - This executor
/// * `writer` - The connection's write half
/// * `rx` - What to write
/// * `fifo` - Whether frames go in the order they were queued, with no small frame first
/// * `pattern` - The pattern a read of this connection is answered from
async fn write_loop<W: AsyncWrite + Unpin>(
    exec: Rc<Exec>,
    mut writer: W,
    mut rx: mpsc::UnboundedReceiver<Out>,
    fifo: bool,
    pattern: Arc<Vec<u8>>,
) {
    let mut urgent: VecDeque<Vec<u8>> = VecDeque::new();
    let mut data: VecDeque<Out> = VecDeque::new();
    let mut closed = false;
    loop {
        // everything queued, sorted without waiting: in first in first out order, everything
        // waits in one queue
        loop {
            match rx.try_next() {
                Ok(Some(Out::Urgent(bytes))) if !fifo => urgent.push_back(bytes),
                Ok(Some(out)) => data.push_back(out),
                Ok(None) => {
                    closed = true;
                    break;
                }
                Err(_) => break,
            }
        }
        // a small frame first, then one data frame, then look again
        if let Some(bytes) = urgent.pop_front() {
            if writer.write_all(&bytes).await.is_err() {
                return;
            }
            continue;
        }
        if let Some(out) = data.pop_front() {
            let (head, payload) = match out {
                Out::Data { head, payload } => (head, payload),
                Out::Urgent(bytes) => {
                    if writer.write_all(&bytes).await.is_err() {
                        return;
                    }
                    continue;
                }
            };
            let (bytes, held): (&[u8], usize) = match &payload {
                Payload::Pattern(start, len) => (&pattern[*start..*start + *len], 0),
                Payload::File(result) => (&result[..], result.len()),
            };
            if write_all_vectored(&mut writer, &[&head, bytes])
                .await
                .is_err()
            {
                return;
            }
            exec.counters
                .bytes_out
                .set(exec.counters.bytes_out.get() + bytes.len() as u64);
            exec.counters.release(held);
            continue;
        }
        if closed {
            let _ = writer.close().await;
            return;
        }
        // nothing queued: wait for the next
        match rx.next().await {
            Some(Out::Urgent(bytes)) if !fifo => urgent.push_back(bytes),
            Some(out) => data.push_back(out),
            None => closed = true,
        }
    }
}

/// Write every byte of several buffers, in as few calls as the socket allows
///
/// # Arguments
///
/// * `writer` - Where to write
/// * `parts` - What to write, in order
pub async fn write_all_vectored<W: AsyncWrite + Unpin>(
    writer: &mut W,
    parts: &[&[u8]],
) -> std::io::Result<()> {
    let mut slices: Vec<IoSlice<'_>> = parts.iter().map(|part| IoSlice::new(part)).collect();
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

/// Read a connection's socket memory until it closes, keeping the executor's peak
///
/// # Arguments
///
/// * `exec` - This executor
/// * `fd` - The socket
/// * `alive` - Whether the connection is still open
async fn sample(exec: Rc<Exec>, fd: RawFd, alive: Rc<Cell<bool>>) {
    while alive.get() {
        let held = sock::memory(fd);
        if held > exec.counters.peak_sock.get() {
            exec.counters.peak_sock.set(held);
        }
        glommio::timer::sleep(SAMPLE_EVERY).await;
    }
}
