//! X11's client: tokio, as the product's client is, on threads pinned to cores
//!
//! A client runtime is a current-thread tokio runtime on a thread pinned to one cpu, so the cpu
//! time a stream costs the client is that thread's. Two run in every cell: one holds the stream's
//! connection, the other paces the small requests, so a small request's own pacing is never
//! starved by the stream it is measured beside.
//!
//! A connection has a writer task that writes every small frame queued before the next data
//! frame, a reader task that routes what the server answers, and a sampler that keeps the most
//! socket memory it saw. Its TLS is the product's own: rustls's handshake and the kernel's record
//! layer, through `shoal::client::tls::connect`.

use std::collections::HashMap;
use std::io::IoSlice;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::{Duration, Instant};

use rustls::client::ClientConnectionData;
use rustls::ClientConfig;
use shoal::shared::tls::{Established, TlsClientOptions};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, Semaphore};

use super::sock;
use super::wire::{self, Header, Kind, Setup, Stats, HEADER_LEN, LAST};
use crate::device::stats::Samples;

/// A tokio runtime on a thread of its own, pinned to one cpu
pub struct Rt {
    /// Where its tasks are spawned
    handle: Handle,
    /// Ends the thread when dropped
    stop: Option<oneshot::Sender<()>>,
    /// The thread
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Rt {
    /// Start a runtime on a pinned thread
    ///
    /// # Arguments
    ///
    /// * `cpu` - The cpu
    /// * `name` - The thread's name
    #[must_use]
    pub fn start(cpu: usize, name: &str) -> Rt {
        let (handle_tx, handle_rx) = std::sync::mpsc::channel();
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let thread = std::thread::Builder::new()
            .name(name.to_string())
            .spawn(move || {
                // pinned before the runtime exists, so every task it runs is on that cpu
                crate::placement::timing::pin(cpu);
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("the runtime builds");
                handle_tx
                    .send(runtime.handle().clone())
                    .expect("the handle is taken");
                // the thread drives its runtime until it is told to stop
                runtime.block_on(async {
                    let _ = stop_rx.await;
                });
            })
            .expect("the runtime's thread starts");
        Rt {
            handle: handle_rx.recv().expect("the runtime starts"),
            stop: Some(stop_tx),
            thread: Some(thread),
        }
    }

    /// Run a future on this runtime and wait for it
    ///
    /// # Arguments
    ///
    /// * `future` - The future
    pub fn run<F>(&self, future: F) -> F::Output
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let task = self.handle.spawn(future);
        futures::executor::block_on(task).expect("the task finishes")
    }

    /// The cpu time this runtime's thread has used, in nanoseconds
    #[must_use]
    pub fn cpu_ns(&self) -> u64 {
        self.run(async { crate::device::sys::thread_cpu_ns() })
    }

    /// Where this runtime's tasks are spawned
    #[must_use]
    pub fn handle(&self) -> &Handle {
        &self.handle
    }
}

impl Drop for Rt {
    /// Stop the runtime and wait for its thread
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Where the server is, and how to reach it over TLS
#[derive(Clone)]
pub struct Target {
    /// The server's host
    pub host: String,
    /// Executor zero's plaintext port
    pub base_port: u16,
    /// What the client trusts, for the TLS ports
    pub tls: Option<(Arc<ClientConfig>, TlsClientOptions)>,
}

impl Target {
    /// A target, trusting a certificate for its TLS ports
    ///
    /// # Arguments
    ///
    /// * `host` - The server's host
    /// * `base_port` - Executor zero's plaintext port
    /// * `ca` - The certificate to trust, if TLS is to be measured
    #[must_use]
    pub fn new(host: &str, base_port: u16, ca: Option<&std::path::Path>) -> Self {
        // the certificate names localhost, so that one file serves every host of the lab
        let tls = ca.map(|ca| {
            let options = TlsClientOptions::new(ca).server_name("localhost");
            let config =
                shoal::shared::tls::client_config(&options).expect("the certificate loads");
            (config, options)
        });
        Target {
            host: host.to_string(),
            base_port,
            tls,
        }
    }
}

/// How a connection is opened
#[derive(Debug, Clone, Copy, Default)]
pub struct Dial {
    /// Which executor serves it
    pub executor: usize,
    /// Whether it is encrypted
    pub tls: bool,
    /// Whether the receiving side is told kTLS records carry no padding
    pub nopad: bool,
    /// `TCP_NOTSENT_LOWAT` on both ends, zero for the kernel's default
    pub lowat: u32,
    /// What the server is told the connection is for
    pub setup: Setup,
}

/// What a stream's end reported
#[derive(Debug, Clone, Copy, Default)]
pub struct Done {
    /// Payload bytes the server received
    pub bytes: u64,
}

/// The small requests of a cell and their latencies
#[derive(Default)]
pub struct Pacer {
    /// When each request in flight was queued, by its sequence
    sent: Mutex<HashMap<u64, Instant>>,
    /// The latencies recorded while recording was on
    samples: Mutex<Samples>,
    /// Whether answers are recorded
    recording: AtomicBool,
    /// Whether pacing goes on
    running: AtomicBool,
}

impl Pacer {
    /// Note an answer, and its latency if recording
    ///
    /// # Arguments
    ///
    /// * `seq` - The request it answers
    fn answered(&self, seq: u64) {
        let sent = self
            .sent
            .lock()
            .expect("the map is not poisoned")
            .remove(&seq);
        if let Some(sent) = sent {
            if self.recording.load(Ordering::Acquire) {
                self.samples
                    .lock()
                    .expect("the samples are not poisoned")
                    .push(sent.elapsed());
            }
        }
    }

    /// Start or stop recording latencies
    ///
    /// # Arguments
    ///
    /// * `on` - Whether to record
    pub fn record(&self, on: bool) {
        self.recording.store(on, Ordering::Release);
    }

    /// The latencies recorded
    #[must_use]
    pub fn samples(&self) -> Samples {
        self.samples
            .lock()
            .expect("the samples are not poisoned")
            .clone()
    }

    /// Stop pacing
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }
}

/// What a connection's tasks share with whoever drives it
pub struct Shared {
    /// Payload bytes received in data frames
    pub received: AtomicU64,
    /// Payloads that did not match the pattern
    pub mismatches: AtomicU64,
    /// The most socket memory seen since the last reset
    pub peak_sock: AtomicU64,
    /// Whether the reader is still reading
    alive: AtomicBool,
    /// The ranges a read stream may have outstanding
    permits: Semaphore,
    /// Where a stream's end is reported
    done: Mutex<Option<oneshot::Sender<Done>>>,
    /// Where an executor's counters are reported
    stats: Mutex<Option<oneshot::Sender<Stats>>>,
    /// Where the setup's answer is reported
    setup_ok: Mutex<Option<oneshot::Sender<(u64, bool)>>>,
    /// The small requests this connection answers, if any
    pacer: Mutex<Option<Arc<Pacer>>>,
    /// Whether payloads are checked against the pattern
    verify: AtomicBool,
}

impl Default for Shared {
    /// A connection's shared state before anything has happened on it
    fn default() -> Self {
        Shared {
            received: AtomicU64::new(0),
            mismatches: AtomicU64::new(0),
            peak_sock: AtomicU64::new(0),
            alive: AtomicBool::new(false),
            // a read stream adds its window when it starts
            permits: Semaphore::new(0),
            done: Mutex::new(None),
            stats: Mutex::new(None),
            setup_ok: Mutex::new(None),
            pacer: Mutex::new(None),
            verify: AtomicBool::new(false),
        }
    }
}

/// A data frame for the writer
#[derive(Debug, Clone, Copy)]
enum DataOut {
    /// Bytes of a write stream
    Write {
        /// The stream
        id: u64,
        /// Where in it
        offset: u64,
        /// How many
        len: usize,
        /// Whether this is the stream's last frame
        last: bool,
    },
    /// A range asked for
    Read {
        /// The stream
        id: u64,
        /// Where in it
        offset: u64,
        /// How many
        len: usize,
    },
}

/// An open connection
pub struct Conn {
    /// Small frames, written before any data frame still queued
    urgent: mpsc::UnboundedSender<Vec<u8>>,
    /// Data frames, at most two queued
    data: mpsc::Sender<DataOut>,
    /// What its tasks share
    pub shared: Arc<Shared>,
    /// Which executor serves it
    pub executor: u64,
    /// Whether kTLS holds the server's socket
    pub server_ktls: bool,
    /// Whether kTLS holds this end's socket
    pub client_ktls: bool,
    /// Its TLS session, held for as long as the connection lives
    _tls: Option<Established<ClientConnectionData>>,
}

impl Conn {
    /// Open a connection and tell the server what it is for
    ///
    /// Run on the runtime that is to hold the connection's tasks.
    ///
    /// # Arguments
    ///
    /// * `target` - Where the server is
    /// * `dial` - How to open it
    pub async fn open(target: Target, dial: Dial) -> Conn {
        // the executor's port, TLS or plaintext
        let port = if dial.tls {
            super::server::tls_port(target.base_port, dial.executor)
        } else {
            super::server::plain_port(target.base_port, dial.executor)
        };
        let addr: SocketAddr = tokio::net::lookup_host((target.host.as_str(), port))
            .await
            .expect("the server's address resolves")
            .next()
            .expect("the server has an address");
        let mut stream = TcpStream::connect(addr).await.expect("the server accepts");
        let fd = stream.as_raw_fd();
        stream.set_nodelay(true).expect("Nagle turns off");
        // the handshake, then kTLS, as the product's client does it
        let tls = if dial.tls {
            let (config, options) = target.tls.as_ref().expect("a TLS dial needs a certificate");
            Some(
                shoal::client::tls::connect(&mut stream, config, options, &addr)
                    .await
                    .expect("the TLS handshake succeeds"),
            )
        } else {
            None
        };
        if dial.nopad && tls.is_some() {
            if let Err(error) = sock::set_rx_no_pad(fd) {
                eprintln!("x11: TLS_RX_EXPECT_NO_PAD refused: {error}");
            }
        }
        if dial.lowat > 0 {
            let _ = sock::set_notsent_lowat(fd, dial.lowat);
        }
        // the setup, before the halves part
        let mut setup = dial.setup;
        setup.nopad = dial.nopad;
        setup.lowat = dial.lowat;
        stream
            .write_all(&super::server::frame(Kind::Setup, 0, &setup.encode()))
            .await
            .expect("the setup is written");
        let client_ktls = sock::is_ktls(fd);
        // the tasks that write, read and sample it
        let shared = Arc::new(Shared::default());
        shared.alive.store(true, Ordering::Release);
        shared.verify.store(setup.verify, Ordering::Release);
        let (ok_tx, ok_rx) = oneshot::channel();
        *shared.setup_ok.lock().expect("not poisoned") = Some(ok_tx);
        let (reader, writer) = stream.into_split();
        let (urgent_tx, urgent_rx) = mpsc::unbounded_channel();
        let (data_tx, data_rx) = mpsc::channel(2);
        tokio::spawn(write_loop(writer, urgent_rx, data_rx));
        tokio::spawn(read_loop(reader, shared.clone()));
        tokio::spawn(sample(fd, shared.clone()));
        let (executor, server_ktls) = ok_rx.await.expect("the server answers the setup");
        Conn {
            urgent: urgent_tx,
            data: data_tx,
            shared,
            executor,
            server_ktls,
            client_ktls,
            _tls: tls,
        }
    }

    /// Ask the executor serving this connection for its counters
    ///
    /// # Arguments
    ///
    /// * `reset` - Whether its peaks start again afterwards
    pub async fn stat(&self, reset: bool) -> Stats {
        let (tx, rx) = oneshot::channel();
        *self.shared.stats.lock().expect("not poisoned") = Some(tx);
        let mut body = [0u8; 8];
        body[0] = u8::from(reset);
        let _ = self.urgent.send(super::server::frame(Kind::Stat, 0, &body));
        rx.await.expect("the server answers a stat")
    }

    /// A handle a pacer on another runtime sends small requests through
    #[must_use]
    pub fn small_sender(&self) -> SmallSender {
        SmallSender {
            urgent: self.urgent.clone(),
        }
    }

    /// Route this connection's small answers to a pacer
    ///
    /// # Arguments
    ///
    /// * `pacer` - The pacer
    pub fn answer_to(&self, pacer: &Arc<Pacer>) {
        *self.shared.pacer.lock().expect("not poisoned") = Some(pacer.clone());
    }
}

/// Where a pacer puts its small requests
#[derive(Clone)]
pub struct SmallSender {
    /// The connection's small frames
    urgent: mpsc::UnboundedSender<Vec<u8>>,
}

/// A stream running on a connection
pub struct Running {
    /// Set to end the stream
    stop: Arc<AtomicBool>,
    /// The stream's task, which ends once the stream has
    task: tokio::task::JoinHandle<Done>,
}

impl Running {
    /// End the stream and wait for its end, on the runtime it runs on
    ///
    /// # Arguments
    ///
    /// * `rt` - That runtime
    pub fn finish(self, rt: &Rt) -> Done {
        self.stop.store(true, Ordering::Release);
        let task = self.task;
        rt.run(async move { task.await.expect("the stream's task finishes") })
    }
}

/// Start a write stream of frames of one size, until it is told to stop
///
/// Run on the runtime that holds the connection.
///
/// # Arguments
///
/// * `conn` - The connection
/// * `id` - The stream's id
/// * `frame` - Payload bytes a frame
#[must_use]
pub fn start_write(conn: &Conn, id: u64, frame: usize) -> Running {
    let stop = Arc::new(AtomicBool::new(false));
    let (data, shared, stopping) = (conn.data.clone(), conn.shared.clone(), stop.clone());
    let task = tokio::spawn(async move {
        // the stream's end is reported here; the writer opens it ahead of its first frame
        let (done_tx, done_rx) = oneshot::channel();
        *shared.done.lock().expect("not poisoned") = Some(done_tx);
        let mut offset = 0u64;
        loop {
            let last = stopping.load(Ordering::Acquire);
            if data
                .send(DataOut::Write {
                    id,
                    offset,
                    len: frame,
                    last,
                })
                .await
                .is_err()
            {
                return Done::default();
            }
            offset += frame as u64;
            if last {
                break;
            }
        }
        done_rx.await.unwrap_or_default()
    });
    Running { stop, task }
}

/// Start a read stream: ranges of one size, at most a window of them outstanding
///
/// Run on the runtime that holds the connection.
///
/// # Arguments
///
/// * `conn` - The connection
/// * `id` - The stream's id
/// * `frame` - Bytes a range
/// * `window` - Ranges outstanding at once
#[must_use]
pub fn start_read(conn: &Conn, id: u64, frame: usize, window: usize) -> Running {
    let stop = Arc::new(AtomicBool::new(false));
    let (data, shared, stopping) = (conn.data.clone(), conn.shared.clone(), stop.clone());
    shared.permits.add_permits(window);
    let task = tokio::spawn(async move {
        let mut offset = 0u64;
        while !stopping.load(Ordering::Acquire) {
            // a range for every one answered, never more than the window outstanding
            let permit = shared
                .permits
                .acquire()
                .await
                .expect("the semaphore stays open");
            permit.forget();
            if data
                .send(DataOut::Read {
                    id,
                    offset,
                    len: frame,
                })
                .await
                .is_err()
            {
                break;
            }
            offset += frame as u64;
        }
        // every range answered before the stream counts as ended
        if let Ok(all) = shared.permits.acquire_many(window as u32).await {
            all.forget();
        }
        Done {
            bytes: shared.received.load(Ordering::Acquire),
        }
    });
    Running { stop, task }
}

/// Pace small requests through a connection from fixed slots, until the pacer is stopped
///
/// Run on the pacing runtime. Each request's latency is counted from the moment it is queued to
/// the connection, so the time a timer fires late is not the connection's.
///
/// # Arguments
///
/// * `sender` - The connection's small frames
/// * `pacer` - Where latencies are kept
/// * `period` - The time between slots
pub fn start_pacing(
    sender: SmallSender,
    pacer: Arc<Pacer>,
    period: Duration,
) -> tokio::task::JoinHandle<()> {
    pacer.running.store(true, Ordering::Release);
    tokio::spawn(async move {
        let start = tokio::time::Instant::now();
        let mut seq = 0u64;
        while pacer.running.load(Ordering::Acquire) {
            tokio::time::sleep_until(start + period * seq as u32).await;
            let mut body = [0u8; wire::SMALL_LEN];
            body[..8].copy_from_slice(&seq.to_le_bytes());
            pacer
                .sent
                .lock()
                .expect("not poisoned")
                .insert(seq, Instant::now());
            if sender
                .urgent
                .send(super::server::frame(Kind::Small, 0, &body))
                .is_err()
            {
                break;
            }
            seq += 1;
        }
    })
}

/// Write a connection's frames: every small frame queued before the next data frame
///
/// # Arguments
///
/// * `writer` - The connection's write half
/// * `urgent` - Small frames
/// * `data` - Data frames
async fn write_loop<W: AsyncWrite + Unpin>(
    mut writer: W,
    mut urgent: mpsc::UnboundedReceiver<Vec<u8>>,
    mut data: mpsc::Receiver<DataOut>,
) {
    let pattern = pattern();
    loop {
        // take the next frame, small ones first, until both queues are closed
        let Some(next) = next_out(&mut urgent, &mut data).await else {
            return;
        };
        match next {
            Out::Small(bytes) => {
                if writer.write_all(&bytes).await.is_err() {
                    return;
                }
            }
            Out::Data(out) => {
                let written = match out {
                    DataOut::Write { id, offset, len, last } => {
                        // a stream's first frame is preceded by its open
                        let open = if offset == 0 {
                            super::server::frame(Kind::Write, 0, &wire::two_words(id, 0))
                        } else {
                            Vec::new()
                        };
                        let header = Header::new(Kind::Data, if last { LAST } else { 0 }, wire::DATA_HEAD_LEN + len).encode();
                        let head = wire::data_head(id, offset);
                        let start = wire::pattern_at(offset);
                        write_all_vectored(&mut writer, &[&open, &header, &head, &pattern[start..start + len]]).await
                    }
                    DataOut::Read { id, offset, len } => {
                        let mut body = [0u8; 24];
                        body[..16].copy_from_slice(&wire::two_words(id, offset));
                        body[16..].copy_from_slice(&(len as u64).to_le_bytes());
                        writer.write_all(&super::server::frame(Kind::Read, 0, &body)).await
                    }
                };
                if written.is_err() {
                    return;
                }
            }
        }
    }
}

/// What a connection's writer takes next
enum Out {
    /// A small frame
    Small(Vec<u8>),
    /// A data frame
    Data(DataOut),
}

/// The next frame for a connection's writer: a small one whenever one is queued
///
/// Polls both queues in one future rather than racing two receives, so nothing taken from a queue
/// is ever dropped, and the workspace's raced receive scan has nothing to judge.
///
/// # Arguments
///
/// * `urgent` - Small frames
/// * `data` - Data frames
async fn next_out(
    urgent: &mut mpsc::UnboundedReceiver<Vec<u8>>,
    data: &mut mpsc::Receiver<DataOut>,
) -> Option<Out> {
    std::future::poll_fn(|cx| {
        // a small frame goes first whenever one is waiting
        let urgent_open = match urgent.poll_recv(cx) {
            Poll::Ready(Some(bytes)) => return Poll::Ready(Some(Out::Small(bytes))),
            Poll::Ready(None) => false,
            Poll::Pending => true,
        };
        // then a data frame, and the end only once both queues are closed
        match data.poll_recv(cx) {
            Poll::Ready(Some(out)) => Poll::Ready(Some(Out::Data(out))),
            Poll::Ready(None) if !urgent_open => Poll::Ready(None),
            Poll::Ready(None) | Poll::Pending => Poll::Pending,
        }
    })
    .await
}

/// The pattern every payload is cut from, made once a process
fn pattern() -> Arc<Vec<u8>> {
    static PATTERN: std::sync::OnceLock<Arc<Vec<u8>>> = std::sync::OnceLock::new();
    PATTERN
        .get_or_init(|| Arc::new(wire::pattern(super::SEED)))
        .clone()
}

/// Write every byte of several buffers, in as few calls as the socket allows
///
/// # Arguments
///
/// * `writer` - Where to write
/// * `parts` - What to write, in order
async fn write_all_vectored<W: AsyncWrite + Unpin>(
    writer: &mut W,
    parts: &[&[u8]],
) -> std::io::Result<()> {
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

/// Read what the server sends and route each frame to whoever waits for it
///
/// # Arguments
///
/// * `reader` - The connection's read half
/// * `shared` - What its tasks share
async fn read_loop<R: AsyncRead + Unpin>(mut reader: R, shared: Arc<Shared>) {
    let pattern = pattern();
    let mut payload = vec![0u8; wire::MAX_FRAME as usize];
    loop {
        let mut bytes = [0u8; HEADER_LEN];
        if reader.read_exact(&mut bytes).await.is_err() {
            break;
        }
        let Ok(header) = Header::decode(&bytes) else {
            eprintln!("x11: the server sent a frame the client cannot read");
            break;
        };
        let len = header.len as usize;
        match header.kind {
            // a range's bytes, into one buffer reused for every frame
            Kind::Data => {
                let mut head = [0u8; wire::DATA_HEAD_LEN];
                if reader.read_exact(&mut head).await.is_err() {
                    break;
                }
                let n = len - wire::DATA_HEAD_LEN;
                if reader.read_exact(&mut payload[..n]).await.is_err() {
                    break;
                }
                if shared.verify.load(Ordering::Relaxed) {
                    let start = wire::pattern_at(wire::word(&head, 2));
                    if pattern[start..start + n] != payload[..n] {
                        shared.mismatches.fetch_add(1, Ordering::Relaxed);
                    }
                }
                shared.received.fetch_add(n as u64, Ordering::AcqRel);
                shared.permits.add_permits(1);
            }
            // every other frame is small, read whole
            _ => {
                if reader.read_exact(&mut payload[..len]).await.is_err() {
                    break;
                }
                let body = &payload[..len];
                match header.kind {
                    Kind::SetupOk => {
                        if let Some(tx) = shared.setup_ok.lock().expect("not poisoned").take() {
                            let _ = tx.send((wire::word(body, 0), body.get(8) == Some(&1)));
                        }
                    }
                    Kind::Answer => {
                        let pacer = shared.pacer.lock().expect("not poisoned").clone();
                        if let Some(pacer) = pacer {
                            pacer.answered(wire::word(body, 0));
                        }
                    }
                    Kind::Done => {
                        if let Some(tx) = shared.done.lock().expect("not poisoned").take() {
                            let _ = tx.send(Done {
                                bytes: wire::word(body, 1),
                            });
                        }
                    }
                    Kind::Stats => {
                        if let Some(tx) = shared.stats.lock().expect("not poisoned").take() {
                            let _ = tx.send(Stats::decode(body));
                        }
                    }
                    other => eprintln!("x11: the server sent {other:?}"),
                }
            }
        }
    }
    shared.alive.store(false, Ordering::Release);
}

/// Read a connection's socket memory while it is open, keeping the most seen
///
/// # Arguments
///
/// * `fd` - The socket
/// * `shared` - What its tasks share
async fn sample(fd: RawFd, shared: Arc<Shared>) {
    while shared.alive.load(Ordering::Acquire) {
        shared
            .peak_sock
            .fetch_max(sock::memory(fd), Ordering::AcqRel);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
