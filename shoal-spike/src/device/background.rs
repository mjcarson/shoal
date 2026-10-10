//! X12: a background's reads and writes, each piece paced, beside a foreground on one executor
//!
//! A rebuild and a deep scrub do their I/O in pieces, and every piece passes the side's pacer
//! before it is issued, so a fixed budget, a ceiling and pacing by idle time all act at the same
//! grain: the foreground waits behind at most the pieces in flight. Every piece that begins after
//! the warm-up and ends inside the window is counted, read and written apart. A rebuilt chunk is
//! written as S6 writes a whole chunk: into a file of a pool the slice keeps written ahead with
//! zeros, synced, renamed over the chunk's name, and its object's directory synced. The work
//! itself runs in a task queue of its own below the foreground's, as S13 orders a slice's work,
//! with the latency goal X9 found a shared executor needs.

use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures::stream::{FuturesUnordered, StreamExt};
use glommio::io::{Directory, DmaBuffer, DmaFile, ReadResult};
use glommio::{Latency, Shares, Task, TaskQueueHandle};

use super::counters::{Delta, Devices};
use super::io::{self, DirSync, HEADER};
use super::paced::{until, Pacer, Window};
use super::stripes::{ReadChunk, CHUNK, UNIT};
use super::sys;

/// The background queue's shares, against the default queue's thousand
const SHARES: usize = 250;

/// The background queue's latency goal unless a run names another: the goal X9 found holds a
/// table's executor to its goal plus a step, with steps of 64 KiB
pub const GOAL_US: u64 = 100;

/// The task queue a slice's background runs in, at a latency goal that arms the preemption a
/// yield between units waits on (X9)
///
/// # Arguments
///
/// * `goal_us` - The goal, microseconds
#[must_use]
pub fn queue(goal_us: u64) -> TaskQueueHandle {
    glommio::executor().create_task_queue(Shares::Static(SHARES), Latency::Matters(Duration::from_micros(goal_us)), "x12-background")
}

/// A background's bytes counted inside its window
#[derive(Debug, Default)]
pub struct Tally {
    /// Bytes read by pieces counted
    pub read: Cell<u64>,
    /// Bytes written by pieces counted
    pub written: Cell<u64>,
    /// Bytes of chunks rebuilt, counted by the pieces written
    pub rebuilt: Cell<u64>,
    /// Bytes received over the network, counted by the chunks received
    pub received: Cell<u64>,
}

impl Tally {
    /// Count a read piece if it lies in the window
    ///
    /// # Arguments
    ///
    /// * `window` - The window
    /// * `began` - When it was issued
    /// * `bytes` - Its length
    pub fn read(&self, window: &Window, began: Instant, bytes: u64) {
        if window.counts(began, Instant::now()) {
            self.read.set(self.read.get() + bytes);
        }
    }

    /// Count a written piece if it lies in the window
    ///
    /// # Arguments
    ///
    /// * `window` - The window
    /// * `began` - When it was issued
    /// * `bytes` - Its length
    /// * `units` - Whether it carried a rebuilt chunk's units, not its header
    pub fn written(&self, window: &Window, began: Instant, bytes: u64, units: bool) {
        if window.counts(began, Instant::now()) {
            self.written.set(self.written.get() + bytes);
            if units {
                self.rebuilt.set(self.rebuilt.get() + bytes);
            }
        }
    }
}

/// The device and the executor's thread between a window's warm-up and its end
///
/// # Arguments
///
/// * `devices` - The devices counted
/// * `window` - The window
#[must_use]
pub fn edges(devices: Devices, window: Window) -> Task<(Delta, u64)> {
    glommio::spawn_local(async move {
        until(window.warm).await;
        let (before, cpu) = (devices.snap(), sys::thread_cpu_ns());
        until(window.end).await;
        (before.delta(&devices.snap()), sys::thread_cpu_ns().saturating_sub(cpu))
    })
}

/// Read a chunk in pieces, each passing the pacer, at most `depth` in flight; `None` once the
/// window has ended, which is how a background stops
///
/// # Arguments
///
/// * `file` - The chunk
/// * `piece` - Bytes a piece, a multiple of a unit
/// * `depth` - Pieces in flight
/// * `pacer` - The side's pacer
/// * `tally` - Where counted bytes go
/// * `window` - The side's window
pub async fn read_chunk(file: &DmaFile, piece: u64, depth: usize, pacer: &mut Pacer, tally: &Tally, window: Window) -> Option<ReadChunk> {
    // the header block, which says what the units should hold
    pacer.admit(HEADER).await;
    if Instant::now() >= window.end {
        return None;
    }
    let began = Instant::now();
    let header = file.read_at_aligned(0, HEADER as usize).await.expect("a header");
    tally.read(&window, began, HEADER);
    // the units in pieces, the next issued as one ends
    let count = (CHUNK / piece) as usize;
    let mut pieces: Vec<Option<ReadResult>> = (0..count).map(|_| None).collect();
    let mut pending = FuturesUnordered::new();
    let mut stopped = false;
    for index in 0..count {
        while pending.len() >= depth {
            let (at, read): (usize, ReadResult) = pending.next().await.expect("a piece in flight");
            pieces[at] = Some(read);
        }
        pacer.admit(piece).await;
        if Instant::now() >= window.end {
            stopped = true;
            break;
        }
        pending.push(async move {
            let began = Instant::now();
            let read = file.read_at_aligned(HEADER + index as u64 * piece, piece as usize).await.expect("a piece");
            tally.read(&window, began, piece);
            (index, read)
        });
    }
    // every piece issued is waited for, even when the window has ended
    while let Some((at, read)) = pending.next().await {
        pieces[at] = Some(read);
    }
    if stopped {
        return None;
    }
    Some(ReadChunk { header, pieces: pieces.into_iter().map(|read| read.expect("every piece read")).collect(), piece })
}

/// A pool of chunk files a slice keeps written ahead with zeros, as S6's whole-chunk write takes
/// its file from: one object directory a rebuilt chunk, a file a position in it, each named `a`
/// or `b` as the last write left it
pub struct DestPool {
    /// The pool's directory
    root: PathBuf,
    /// Positions in an object
    slots: usize,
    /// Every object's directory, open for its sync
    dirs: Vec<Directory>,
    /// Whether each file is named `a` now, by object then slot
    named_a: Vec<Vec<Cell<bool>>>,
    /// The directory sync the probes chose
    form: DirSync,
}

impl DestPool {
    /// Make a pool, every file written ahead with zeros and synced, outside anything timed
    ///
    /// # Arguments
    ///
    /// * `root` - Its directory
    /// * `objects` - Objects in it
    /// * `slots` - Files in each
    /// * `form` - The directory sync the probes chose
    pub async fn make(root: &Path, objects: usize, slots: usize, form: DirSync) -> DestPool {
        io::wipe(root);
        let mut dirs = Vec::with_capacity(objects);
        for object in 0..objects {
            let dir = root.join(format!("{object:016x}"));
            std::fs::create_dir_all(&dir).expect("made");
            for slot in 0..slots {
                let file = io::open(&dir.join(format!("{slot}.a")), true).await;
                io::zero_fill(&file, HEADER + CHUNK).await;
                file.close().await.expect("closed");
            }
            dirs.push(Directory::open(&dir).await.expect("opened"));
        }
        let named_a = (0..objects).map(|_| (0..slots).map(|_| Cell::new(true)).collect()).collect();
        let _ = sys::syncfs(root);
        DestPool { root: root.to_path_buf(), slots, dirs, named_a, form }
    }

    /// The file a slot's chunk is in now
    ///
    /// # Arguments
    ///
    /// * `object` - The object
    /// * `slot` - The position
    #[must_use]
    pub fn current(&self, object: usize, slot: usize) -> PathBuf {
        let name = if self.named_a[object][slot].get() { "a" } else { "b" };
        self.root.join(format!("{object:016x}")).join(format!("{slot}.{name}"))
    }

    /// The name a slot's next chunk is renamed to
    ///
    /// # Arguments
    ///
    /// * `object` - The object
    /// * `slot` - The position
    fn next(&self, object: usize, slot: usize) -> PathBuf {
        let name = if self.named_a[object][slot].get() { "b" } else { "a" };
        self.root.join(format!("{object:016x}")).join(format!("{slot}.{name}"))
    }

    /// Write a rebuilt chunk whole into a slot: its pieces paced, the file synced, renamed over
    /// the chunk's name and the object's directory synced
    ///
    /// # Arguments
    ///
    /// * `object` - The object
    /// * `slot` - The position, which must be one the pool has
    /// * `header` - The header block
    /// * `pieces` - The units, in pieces
    /// * `depth` - Pieces in flight
    /// * `pacer` - The side's pacer
    /// * `tally` - Where counted bytes go
    /// * `window` - The side's window
    #[allow(clippy::too_many_arguments)]
    pub async fn write(
        &self,
        object: usize,
        slot: usize,
        header: Rc<DmaBuffer>,
        pieces: &[Rc<DmaBuffer>],
        depth: usize,
        pacer: &mut Pacer,
        tally: &Tally,
        window: Window,
    ) {
        assert!(slot < self.slots, "a slot the pool has");
        let file = io::open(&self.current(object, slot), false).await;
        // the header and the pieces, every one through the pacer
        let mut pending = FuturesUnordered::new();
        let mut at = 0_u64;
        let writes = std::iter::once((header, true)).chain(pieces.iter().cloned().map(|piece| (piece, false)));
        for (buffer, is_header) in writes {
            while pending.len() >= depth {
                pending.next().await;
            }
            let len = buffer.len() as u64;
            pacer.admit(len).await;
            let place = if is_header { 0 } else { HEADER + at };
            if !is_header {
                at += len;
            }
            let file = &file;
            pending.push(async move {
                let began = Instant::now();
                let written = file.write_rc_at(buffer, place).await.expect("a rebuilt piece lands");
                assert_eq!(written as u64, len, "a direct write lands whole");
                tally.written(&window, began, len, !is_header);
            });
        }
        while pending.next().await.is_some() {}
        drop(pending);
        // durable, then made the chunk by its name, then the name durable
        file.fdatasync().await.expect("synced");
        file.rename(self.next(object, slot)).await.expect("renamed");
        io::sync_dir(&self.dirs[object], self.form).await;
        file.close().await.expect("closed");
        self.named_a[object][slot].set(!self.named_a[object][slot].get());
    }
}

/// Buffers for a rebuilt chunk, taken back after each write so none is allocated in the window
pub struct OutBuffers {
    /// The header block
    pub header: DmaBuffer,
    /// The units, in pieces
    pub pieces: Vec<DmaBuffer>,
    /// Bytes a piece
    pub piece: u64,
}

impl OutBuffers {
    /// Buffers for one chunk
    ///
    /// # Arguments
    ///
    /// * `piece` - Bytes a piece, a multiple of a unit
    #[must_use]
    pub fn new(piece: u64) -> Self {
        debug_assert_eq!(piece % UNIT, 0, "a piece is whole units");
        OutBuffers {
            header: glommio::allocate_dma_buffer(HEADER as usize),
            pieces: (0..CHUNK / piece).map(|_| glommio::allocate_dma_buffer(piece as usize)).collect(),
            piece,
        }
    }

    /// Lend the buffers out for a write
    #[must_use]
    pub fn lend(self) -> (Rc<DmaBuffer>, Vec<Rc<DmaBuffer>>, u64) {
        (Rc::new(self.header), self.pieces.into_iter().map(Rc::new).collect(), self.piece)
    }

    /// Take the buffers back once a write has ended
    ///
    /// # Arguments
    ///
    /// * `header` - The header block lent
    /// * `pieces` - The pieces lent
    /// * `piece` - Bytes a piece
    #[must_use]
    pub fn back(header: Rc<DmaBuffer>, pieces: Vec<Rc<DmaBuffer>>, piece: u64) -> Self {
        let take = |buffer: Rc<DmaBuffer>| Rc::try_unwrap(buffer).ok().expect("a write ended holds no buffer");
        OutBuffers { header: take(header), pieces: pieces.into_iter().map(take).collect(), piece }
    }

    /// The units, whole, for checksumming
    ///
    /// # Arguments
    ///
    /// * `unit` - The unit
    #[must_use]
    pub fn unit(&self, unit: usize) -> &[u8] {
        let at = unit as u64 * UNIT;
        &self.pieces[(at / self.piece) as usize].as_bytes()[(at % self.piece) as usize..][..UNIT as usize]
    }
}

/// The share of a window an executor's thread was on a cpu
///
/// # Arguments
///
/// * `cpu_ns` - Its cpu time over the window
/// * `window` - The window
#[must_use]
pub fn busy(cpu_ns: u64, window: &Window) -> f64 {
    cpu_ns as f64 / 1e9 / window.secs()
}
