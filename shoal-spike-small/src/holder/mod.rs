//! The holder: one slice's journal and chunks on one glommio executor, on one host
//!
//! S6's holder of a stripe chunk, built only as far as one small write in place needs
//! ([S6](../../../docs/src/object-storage/device-store.md#staging-two-cases)):
//!
//! - a **stage** reads the write's bytes off its lane straight into a buffer for direct I/O,
//!   writes a header block and the bytes into the journal, a ring written ahead, and answers once
//!   the committer's next sync covers them; the record is kept in memory until it is applied;
//! - an **apply** writes the staged bytes and a header block in place in the slot's chunk, a file
//!   written ahead, syncs the chunk and drops the record;
//! - a **fold** does the same for a write that rode inside its commit, with bytes it makes itself
//!   from the write's seed, as a replica on its host would hand them to its slice.
//!
//! S6 syncs each apply's chunk on its own ([`InPlaceSync::Each`]). For a supplement the holder can
//! instead cover every apply and fold that completed with one flush, as the journal's committer
//! covers stages ([`InPlaceSync::Batch`]).
//!
//! Every lane is a connection of its own under the product's mutual TLS, handed to the kernel, and
//! carries one request at a time; the holder's concurrency is its lanes'. The executor is pinned
//! to one cpu, and its I/O goes through its own ring, as a slice's executor's would.

pub mod io;
pub mod journal;

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use color_eyre::eyre::eyre;
use futures::{AsyncReadExt, AsyncWriteExt};
use glommio::io::{DmaBuffer, DmaFile};
use glommio::net::{TcpListener, TcpStream};
use glommio::{CpuSet, LocalExecutorBuilder, Placement, PoolPlacement};
use rustls::ServerConfig;
use shoal::shared::tls::{peer_server_config, PeerTlsOptions};

use crate::wire::{
    self, Header, HolderStats, Kind, ProbeOut, Request, StageHead, WireLabel, ALIGN, CHUNK_HEADER, HEADER_LEN,
};
use journal::{Committer, Ring};

/// How an apply or a fold is made durable once its bytes are written in place
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InPlaceSync {
    /// Its chunk's own `fdatasync`, one an apply, as S6 has it
    Each,
    /// One flush covering every apply and fold whose writes completed before it began
    ///
    /// The committer syncs a small file of its own: on XFS and ext4 an `fdatasync` flushes the
    /// device's cache even with nothing of its own to write
    /// ([X6](../../../docs/src/object-storage/device-store-ssd.md#what-the-probes-found)), so one
    /// flush covers every direct write into a chunk written ahead that completed before it. Whether
    /// one file's sync may stand for another's in the product is M14's to establish.
    Batch,
}

impl InPlaceSync {
    /// The way a name means
    ///
    /// # Arguments
    ///
    /// * `name` - `each` or `batch`
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "each" => Some(InPlaceSync::Each),
            "batch" => Some(InPlaceSync::Batch),
            _ => None,
        }
    }
}

/// How a holder is run
#[derive(Debug, Clone)]
pub struct HolderConf {
    /// Where it listens
    pub listen: SocketAddr,
    /// The directory its journal and chunks are in, on the device it measures
    pub dir: PathBuf,
    /// The cpu its executor is pinned to
    pub cpu: usize,
    /// How many chunk slots it keeps
    pub slots: u32,
    /// The bytes of units each chunk holds, after its header block
    pub chunk_bytes: u64,
    /// The length of the journal's ring
    pub ring_bytes: u64,
    /// The memory its executor's ring registers for I/O buffers
    pub io_memory: usize,
    /// How an apply or a fold is made durable
    pub in_place: InPlaceSync,
    /// Its certificate and key, and the authority its lanes' peers are signed by
    pub tls: PeerTlsOptions,
}

/// One record staged and not yet applied
struct Staged {
    /// Where in the chunk's units its bytes go
    offset: u64,
    /// Its header block
    header: Rc<DmaBuffer>,
    /// Its bytes
    payload: Rc<DmaBuffer>,
}

/// What the holder counts as it goes
#[derive(Default)]
struct Counters {
    /// Stages made durable
    stages: Cell<u64>,
    /// Bytes written to the journal, header blocks included
    stage_bytes: Cell<u64>,
    /// Applies made durable
    applies: Cell<u64>,
    /// Syncs of a chunk for an apply
    apply_syncs: Cell<u64>,
    /// Folds made durable
    folds: Cell<u64>,
    /// Syncs of a chunk for a fold
    fold_syncs: Cell<u64>,
    /// Bytes written in place in chunks, header blocks included
    chunk_bytes: Cell<u64>,
    /// Applies that found no staged record for their label
    apply_missing: Cell<u64>,
}

/// Everything one holder's executor holds
struct Holder {
    /// The journal's ring of offsets
    ring: Ring,
    /// The journal
    journal: Rc<DmaFile>,
    /// What syncs the journal
    committer: Rc<Committer>,
    /// The chunk of every slot, written ahead and kept open
    chunks: Vec<Rc<DmaFile>>,
    /// The bytes of units a chunk holds
    chunk_bytes: u64,
    /// A file a probe overwrites, apart from everything measured
    probe: Rc<DmaFile>,
    /// What covers applies and folds with one flush, when they are batched
    in_place: Option<Rc<Committer>>,
    /// Records staged and not yet applied, by slot and the label they make
    staged: RefCell<HashMap<(u32, u64, u64), Staged>>,
    /// What it has done
    counters: Counters,
}

/// Run a holder until its process ends
///
/// # Arguments
///
/// * `conf` - How to run it
///
/// # Errors
///
/// When its executor cannot be built or ends with an error.
pub fn run(conf: HolderConf) -> color_eyre::Result<()> {
    // the executor and its blocking thread on the one cpu the holder is given
    let cpu = conf.cpu;
    let online = CpuSet::online().map_err(|error| eyre!("the host's cpus: {error}"))?;
    let pool = PoolPlacement::Custom(vec![online.filter(|location| location.cpu == cpu)]);
    let handle = LocalExecutorBuilder::new(Placement::Fixed(cpu))
        .name("x8-holder")
        .io_memory(conf.io_memory)
        .ring_depth(256)
        .blocking_thread_pool_placement(pool)
        .spawn(move || async move { serve_all(conf).await })
        .map_err(|error| eyre!("the holder's executor: {error}"))?;
    handle
        .join()
        .map_err(|error| eyre!("the holder's executor ended: {error}"))?
}

/// Prepare the journal and the chunks, then accept lanes for as long as the process lives
///
/// # Arguments
///
/// * `conf` - How the holder is run
///
/// # Errors
///
/// When a file cannot be made or the listener cannot bind.
async fn serve_all(conf: HolderConf) -> color_eyre::Result<()> {
    // a fresh directory's files, every one written ahead before the first lane is accepted
    let chunks_dir = conf.dir.join("chunks");
    std::fs::create_dir_all(&chunks_dir)?;
    let started = Instant::now();
    let journal = Rc::new(io::open(&conf.dir.join("journal"), true).await?);
    io::zero_fill(&journal, conf.ring_bytes).await;
    let mut chunks = Vec::with_capacity(conf.slots as usize);
    for slot in 0..conf.slots {
        let chunk = io::open(&chunks_dir.join(format!("{slot:06}")), true).await?;
        io::zero_fill(&chunk, CHUNK_HEADER + conf.chunk_bytes).await;
        chunks.push(Rc::new(chunk));
    }
    let probe = Rc::new(io::open(&conf.dir.join("probe"), true).await?);
    io::zero_fill(&probe, 1 << 20).await;
    // the file a batched flush syncs, which holds nothing anyone reads
    let in_place = match conf.in_place {
        InPlaceSync::Each => None,
        InPlaceSync::Batch => {
            let flush = Rc::new(io::open(&conf.dir.join("flush"), true).await?);
            io::zero_fill(&flush, 4096).await;
            Some(Committer::start(flush))
        }
    };
    eprintln!(
        "x8-holder: {} chunks of {} bytes and a {} byte journal written ahead in {:?}",
        conf.slots,
        conf.chunk_bytes,
        conf.ring_bytes,
        started.elapsed()
    );
    let holder = Rc::new(Holder {
        ring: Ring::wrapping(conf.ring_bytes),
        committer: Committer::start(journal.clone()),
        journal,
        chunks,
        chunk_bytes: conf.chunk_bytes,
        probe,
        in_place,
        staged: RefCell::new(HashMap::new()),
        counters: Counters::default(),
    });
    // the lanes' TLS: this holder's leaf, and peers that chain to the authority
    let tls = peer_server_config(&conf.tls).map_err(|error| eyre!("the holder's tls: {error}"))?;
    let listener = TcpListener::bind(conf.listen).map_err(|error| eyre!("binding {}: {error}", conf.listen))?;
    eprintln!("x8-holder: listening on {} from cpu {}", conf.listen, conf.cpu);
    accept_loop(holder, listener, tls).await;
    Ok(())
}

/// Accept lanes for as long as the listener is open, each served on a task of its own
///
/// # Arguments
///
/// * `holder` - The holder
/// * `listener` - The listener
/// * `tls` - What the holder proves itself with and checks its peers against
async fn accept_loop(holder: Rc<Holder>, listener: TcpListener, tls: Arc<ServerConfig>) {
    loop {
        let mut stream = match listener.accept().await {
            Ok(stream) => stream,
            Err(error) => {
                eprintln!("x8-holder: accept failed: {error}");
                continue;
            }
        };
        let holder = holder.clone();
        let tls = tls.clone();
        glommio::spawn_local(async move {
            // Nagle off before the handshake, as Shoal sets it, then the handshake and kTLS
            let _ = io::set_nodelay(stream.as_raw_fd());
            let session = match shoal::server::tls::accept(&mut stream, &tls).await {
                Ok(session) => session,
                Err(error) => {
                    eprintln!("x8-holder: a TLS handshake failed: {error}");
                    return;
                }
            };
            if let Err(error) = serve(&holder, &mut stream).await {
                if error.kind() != std::io::ErrorKind::UnexpectedEof {
                    eprintln!("x8-holder: a lane ended: {error}");
                }
            }
            // the session outlives the socket's last byte
            drop(session);
        })
        .detach();
    }
}

/// Serve one lane: a request, its answer, and the next, until the peer closes it
///
/// # Arguments
///
/// * `holder` - The holder
/// * `stream` - The lane
///
/// # Errors
///
/// When the lane fails or sends a frame the holder does not speak.
async fn serve(holder: &Rc<Holder>, stream: &mut TcpStream) -> std::io::Result<()> {
    let invalid = |error: String| std::io::Error::new(std::io::ErrorKind::InvalidData, error);
    loop {
        // the next header, which a closed lane ends at
        let mut bytes = [0u8; HEADER_LEN];
        stream.read_exact(&mut bytes).await?;
        let header = Header::decode(&bytes).map_err(invalid)?;
        // a stage's bytes go straight to a buffer for direct I/O; anything else is read whole
        let answer = if header.kind == Kind::Stage {
            holder.stage(stream, header.len).await?
        } else {
            let mut body = vec![0u8; header.len as usize];
            stream.read_exact(&mut body).await?;
            match Request::decode(header.kind, &body) {
                Ok(request) => holder.answer(request, stream.as_raw_fd()).await,
                Err(error) => wire::frame(Kind::Error, error.as_bytes()),
            }
        };
        stream.write_all(&answer).await?;
    }
}

impl Holder {
    /// Stage a record: its bytes into the journal, answered once a sync covers them
    ///
    /// # Arguments
    ///
    /// * `stream` - The lane, at the stage's head
    /// * `len` - The frame's body length
    ///
    /// # Errors
    ///
    /// When the lane fails or the stage's head is not one the holder can write.
    async fn stage(&self, stream: &mut TcpStream, len: u32) -> std::io::Result<Vec<u8>> {
        let invalid = |error: String| std::io::Error::new(std::io::ErrorKind::InvalidData, error);
        // the head, then the bytes it names straight into a buffer for direct I/O
        let mut head = [0u8; StageHead::LEN];
        stream.read_exact(&mut head).await?;
        let head = StageHead::decode(&head).map_err(invalid)?;
        if len as usize != StageHead::LEN + head.len as usize {
            return Err(invalid(format!("a stage frame of {len} bytes names {} of payload", head.len)));
        }
        if head.slot as usize >= self.chunks.len() || u64::from(head.offset + head.len) > self.chunk_bytes {
            return Err(invalid(format!("slot {} at {} for {} is outside every chunk", head.slot, head.offset, head.len)));
        }
        let mut payload = glommio::allocate_dma_buffer(head.len as usize);
        stream.read_exact(&mut payload.as_bytes_mut()[..head.len as usize]).await?;
        let payload = Rc::new(payload);
        let header = io::header_block(&head);
        // the record's place in the ring, its header and bytes written together, then the sync
        let at = self.ring.take(CHUNK_HEADER + u64::from(head.len));
        let (first, second) = futures::join!(
            self.journal.write_rc_at(header.clone(), at),
            self.journal.write_rc_at(payload.clone(), at + CHUNK_HEADER)
        );
        first.map_err(|error| std::io::Error::other(error.to_string()))?;
        second.map_err(|error| std::io::Error::other(error.to_string()))?;
        self.committer.durable().await;
        // kept until its apply, by the label it makes
        self.staged.borrow_mut().insert(
            (head.slot, head.label.sequence, head.label.tag),
            Staged {
                offset: u64::from(head.offset),
                header,
                payload,
            },
        );
        self.counters.stages.set(self.counters.stages.get() + 1);
        self.counters
            .stage_bytes
            .set(self.counters.stage_bytes.get() + CHUNK_HEADER + u64::from(head.len));
        Ok(wire::frame(Kind::Staged, &[]))
    }

    /// Answer a request that is not a stage
    ///
    /// # Arguments
    ///
    /// * `request` - The request
    /// * `fd` - The lane's socket, which a stats answer says the TLS of
    async fn answer(&self, request: Request, fd: std::os::fd::RawFd) -> Vec<u8> {
        match request {
            Request::Apply { slot, label } => self.apply(slot, label).await,
            Request::Fold {
                slot,
                offset,
                len,
                seed,
                object,
            } => self.fold(slot, offset, len, seed, object).await,
            Request::Verify {
                slot,
                offset,
                len,
                seed,
                object,
            } => self.verify(slot, offset, len, seed, object).await,
            Request::Stats => wire::frame(Kind::StatsOut, &self.stats(fd).encode()),
            Request::Probe { count } => wire::frame(Kind::ProbeOut, &self.probe(count).await.encode()),
        }
    }

    /// Apply a staged record in place: its bytes and header block into its chunk, synced
    ///
    /// # Arguments
    ///
    /// * `slot` - The chunk slot
    /// * `label` - The label its committed write made
    async fn apply(&self, slot: u32, label: WireLabel) -> Vec<u8> {
        // the record the label names, which the apply drops once it is durable in place
        let staged = self.staged.borrow_mut().remove(&(slot, label.sequence, label.tag));
        let Some(staged) = staged else {
            self.counters.apply_missing.set(self.counters.apply_missing.get() + 1);
            return wire::frame(Kind::Error, format!("no record staged for slot {slot} at {label:?}").as_bytes());
        };
        let Some(chunk) = self.chunks.get(slot as usize) else {
            return wire::frame(Kind::Error, format!("no slot {slot}").as_bytes());
        };
        let len = staged.payload.len() as u64;
        let written = write_in_place(chunk, staged.header, staged.payload, staged.offset, self.in_place.as_ref()).await;
        if let Err(error) = written {
            return wire::frame(Kind::Error, error.as_bytes());
        }
        self.counters.applies.set(self.counters.applies.get() + 1);
        if self.in_place.is_none() {
            self.counters.apply_syncs.set(self.counters.apply_syncs.get() + 1);
        }
        self.counters
            .chunk_bytes
            .set(self.counters.chunk_bytes.get() + CHUNK_HEADER + len);
        wire::frame(Kind::Applied, &[])
    }

    /// Fold a write that rode inside its commit: its bytes, made here, into its chunk, synced
    ///
    /// # Arguments
    ///
    /// * `slot` - The chunk slot
    /// * `offset` - Where in the chunk's units the bytes go
    /// * `len` - How many bytes
    /// * `seed` - The write's seed
    /// * `object` - The stream its bytes are named by
    async fn fold(&self, slot: u32, offset: u32, len: u32, seed: u64, object: u64) -> Vec<u8> {
        let Some(chunk) = self.chunks.get(slot as usize) else {
            return wire::frame(Kind::Error, format!("no slot {slot}").as_bytes());
        };
        if u64::from(offset + len) > self.chunk_bytes {
            return wire::frame(Kind::Error, format!("{len} bytes at {offset} are outside the chunk").as_bytes());
        }
        // the bytes the commit carried, made from the write's seed as a replica would hand them
        let mut payload = glommio::allocate_dma_buffer(len as usize);
        crate::bytes::fill(seed, object, 0, &mut payload.as_bytes_mut()[..len as usize]);
        let head = StageHead {
            slot,
            offset,
            len,
            label: WireLabel { sequence: seed, tag: object },
            expected: WireLabel::default(),
        };
        let written = write_in_place(
            chunk,
            io::header_block(&head),
            Rc::new(payload),
            u64::from(offset),
            self.in_place.as_ref(),
        )
        .await;
        if let Err(error) = written {
            return wire::frame(Kind::Error, error.as_bytes());
        }
        self.counters.folds.set(self.counters.folds.get() + 1);
        if self.in_place.is_none() {
            self.counters.fold_syncs.set(self.counters.fold_syncs.get() + 1);
        }
        self.counters
            .chunk_bytes
            .set(self.counters.chunk_bytes.get() + CHUNK_HEADER + u64::from(len));
        wire::frame(Kind::Folded, &[])
    }

    /// Read a chunk's range back and say whether it holds the bytes a write's seed makes
    ///
    /// # Arguments
    ///
    /// * `slot` - The chunk slot
    /// * `offset` - Where in the chunk's units the range starts
    /// * `len` - How many bytes
    /// * `seed` - The write's seed
    /// * `object` - The stream its bytes are named by
    async fn verify(&self, slot: u32, offset: u32, len: u32, seed: u64, object: u64) -> Vec<u8> {
        let Some(chunk) = self.chunks.get(slot as usize) else {
            return wire::frame(Kind::Error, format!("no slot {slot}").as_bytes());
        };
        // the range as the device holds it, against the bytes the write carried
        match chunk.read_at_aligned(CHUNK_HEADER + u64::from(offset), len as usize).await {
            Ok(read) => {
                let held = &read[..];
                let matches = held.len() == len as usize && held == crate::bytes::make(seed, object, len as usize).as_slice();
                wire::frame(Kind::Verified, &[u8::from(matches)])
            }
            Err(error) => wire::frame(Kind::Error, error.to_string().as_bytes()),
        }
    }

    /// The holder's counters, as a lane asks for them
    ///
    /// # Arguments
    ///
    /// * `fd` - The lane's socket
    fn stats(&self, fd: std::os::fd::RawFd) -> HolderStats {
        let counters = &self.counters;
        HolderStats {
            stages: counters.stages.get(),
            stage_syncs: self.committer.counts.syncs.get(),
            stage_records: self.committer.counts.records.get(),
            stage_bytes: counters.stage_bytes.get(),
            applies: counters.applies.get(),
            apply_syncs: counters.apply_syncs.get(),
            folds: counters.folds.get(),
            fold_syncs: counters.fold_syncs.get(),
            chunk_bytes: counters.chunk_bytes.get(),
            exec_cpu_ns: io::thread_cpu_ns(),
            staged_now: self.staged.borrow().len() as u64,
            ktls: u64::from(io::is_ktls(fd)),
            apply_missing: counters.apply_missing.get(),
            in_place_syncs: self.in_place.as_ref().map_or(0, |committer| committer.counts.syncs.get()),
            batched: u64::from(self.in_place.is_some()),
        }
    }

    /// The device's sync floor: a 4 KiB overwrite and its sync, `count` times one after another
    ///
    /// # Arguments
    ///
    /// * `count` - How many
    async fn probe(&self, count: u32) -> ProbeOut {
        // one block of zeros, overwritten at successive offsets of a file written ahead
        let mut block = glommio::allocate_dma_buffer(ALIGN as usize);
        block.as_bytes_mut().fill(0);
        let block = Rc::new(block);
        let mut took = Vec::with_capacity(count as usize);
        for index in 0..u64::from(count.max(1)) {
            let started = Instant::now();
            let at = (index * ALIGN) % (1 << 20);
            if self.probe.write_rc_at(block.clone(), at).await.is_err() || self.probe.fdatasync().await.is_err() {
                break;
            }
            took.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        }
        took.sort_unstable();
        ProbeOut {
            p50_ns: took.get(took.len() / 2).copied().unwrap_or(0),
            max_ns: took.last().copied().unwrap_or(0),
        }
    }
}

/// Write a header block and units in place in a chunk, together, then make them durable
///
/// # Arguments
///
/// * `chunk` - The chunk
/// * `header` - Its header block
/// * `payload` - The units' new bytes
/// * `offset` - Where in the chunk's units they go
/// * `batch` - The committer whose next flush covers them, or none to sync the chunk itself
///
/// # Errors
///
/// When a write or the sync fails, as its message.
async fn write_in_place(
    chunk: &Rc<DmaFile>,
    header: Rc<DmaBuffer>,
    payload: Rc<DmaBuffer>,
    offset: u64,
    batch: Option<&Rc<Committer>>,
) -> Result<(), String> {
    // the header and the units issued at once, as an apply would issue them
    let (first, second) = futures::join!(
        chunk.write_rc_at(header, 0),
        chunk.write_rc_at(payload, CHUNK_HEADER + offset)
    );
    first.map_err(|error| error.to_string())?;
    second.map_err(|error| error.to_string())?;
    // then the chunk's own sync, or the batch's next flush, which began after both completed
    match batch {
        Some(committer) => committer.durable().await,
        None => chunk.fdatasync().await.map_err(|error| error.to_string())?,
    }
    Ok(())
}
