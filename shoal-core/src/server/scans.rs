//! What every shard of a node shares for a scan's pages
//! ([F118](../../../docs/src/features/scan-budget.md),
//! [F119](../../../docs/src/features/scan-pacing.md))
//!
//! A scan's page can be read on any shard of a node, so what bounds a node's scans together lives
//! here, made once at the node's start and handed to every shard: the scan budget every page's
//! buffers are drawn from, and the pace of every device a table's archives are on.
//!
//! A table's archives share a device with its WAL and with every get's partition read, so a scan
//! that read at its device's speed would queue them behind it. X12 found the shape that holds a
//! foreground's tail: a background issued only in its device's idle time, under a ceiling, where a
//! fixed rate alone was both too slow when the device was idle and too fast when it was not
//! ([S13](../../../docs/src/object-storage/isolation.md#io-on-a-slice)). So a scan's read of an
//! archive takes its bytes from its device's ceiling, and while any shard has a foreground read in
//! flight on that device from its trickle too, which a scan always gets so a device that is never
//! idle cannot starve it for ever ([W7](../../../docs/src/future-work/scans/isolation.md#device)).
//!
//! # Invariants
//!
//! **One scan budget a node**, never one a shard: the memory it bounds is the process's, and a
//! shard's share of it would let one shard's scans spill past what the node carved.
//!
//! **One pace a device**, shared by every shard of the node whose tables' archives are on it: the
//! device's time is the device's, and a pace a shard would let every shard's scans take a ceiling.
//!
//! **Every byte a scan reads of an archive is taken from its device's ceiling, and from its
//! trickle too while any shard has a foreground read in flight on the device**, and since
//! [F128](../../../docs/src/features/device-time-pace.md) its device's time, at what the scans'
//! own reads have shown a byte of the device costs, from its share; a read is taken whole, before
//! it is issued, and waits only until what it takes is due.
//!
//! **At most a bound of every shard's scan reads is in flight on a device at once**, and the time
//! any one is in flight is the scans' busy time on it, which their cost a byte is learned from.
//!
//! **A foreground read is counted from before it asks its device for a byte until its last is
//! read**, on every way out, and never while its outcome waits for its shard to take it.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use super::budget::Budget;
use super::conf::Conf;
use super::devices::Ceiling;

/// The bytes a scan takes from its device's pace at once: one lock a credit, never one a partition
pub const CREDIT: u64 = 64 << 10;

/// How long a scan waits before it asks its device's pace again, the object store's poll: now
/// only while a foreground read holds its trickle, which an idle device ends at once (F128)
pub const PACE_POLL: Duration = Duration::from_millis(1);

/// How long a scan's read waits before it looks again for room among its device's reads in
/// flight, about one read of a solid state device (F128)
pub const SLOT_POLL: Duration = Duration::from_micros(250);

/// The bytes the scans read of a device before what they cost a byte is learned again, and the
/// fewest a window learns from, so no read alone, small and slow, sets what every byte costs (F128)
pub const LEARN_BYTES: u64 = 1 << 20;

/// The most times what a byte cost in one window is counted past what it was learned to cost, so
/// one window slowed by something other than the scans' reads moves it at most 1.75 times (F128)
pub const LEARN_RISE: u64 = 4;

/// What a pace's ceiling or trickle holds at most: a tenth of a second's worth, so a scan that
/// waited never reads a second's worth at its device's speed, and never less than one credit
///
/// # Arguments
///
/// * `rate` - Bytes a second
#[must_use]
pub fn burst_of(rate: u64) -> u64 {
    (rate / 10).max(CREDIT)
}

/// One shard's foreground reads in flight on a device, on a cache line of its own, so a read is
/// counted without touching any other shard's line
#[derive(Debug, Default)]
#[repr(align(128))]
struct Slot(AtomicU64);

/// One device's pace for the scans of the tables on it
/// ([F119](../../../docs/src/features/scan-pacing.md))
///
/// A ceiling every shard's scans read the device under together, a trickle they read it under
/// while a foreground read is in flight on it, and those reads, a slot a shard.
#[derive(Debug)]
pub struct TablePace {
    /// The device, by its number
    device: u64,
    /// What every shard's scans read of it a second together
    ceiling: Ceiling,
    /// What they read of it a second while a foreground read is in flight on it
    trickle: Ceiling,
    /// The foreground reads in flight on it, one slot a shard
    slots: Box<[Slot]>,
    /// The scan bytes taken while no foreground read was in flight on it
    idle_bytes: AtomicU64,
    /// The scan bytes taken from its trickle
    trickled_bytes: AtomicU64,
    /// The device's busy time a second, in nanoseconds, every shard's scans may take of it
    /// together (F128)
    share: Ceiling,
    /// What the scans' reads have shown a byte of the device costs, in picoseconds of its busy
    /// time, none until they have shown it (F128)
    cost: AtomicU64,
    /// The scans' reads in flight on it, and the busy time and bytes since its cost was last
    /// learned, which every one of them takes once as it starts and once as it ends (F128)
    busy: Mutex<Busy>,
    /// The most scan reads in flight on it at once (F128)
    reads_bound: u64,
    /// The scans' busy time on it, every read in flight counted once, in nanoseconds (F128)
    busy_ns: AtomicU64,
    /// How many times a scan asked its buckets, for a test that a read waits once (O143)
    asks: AtomicU64,
}

/// The scans' reads in flight on one device, and what they have shown it costs since it was
/// last learned (F128)
#[derive(Debug)]
struct Busy {
    /// The scan reads in flight now
    in_flight: u64,
    /// When the busy time was last added up
    since: Instant,
    /// The busy time since the cost was last learned
    time: Duration,
    /// The bytes read in it
    bytes: u64,
}

/// One of a scan's reads in flight on its device, counted until it drops, its bytes then counted
/// toward what a byte of the device costs (F128)
#[derive(Debug)]
pub struct ScanRead {
    /// The device's pace
    pace: Arc<TablePace>,
    /// What the read reads
    bytes: u64,
}

impl Drop for ScanRead {
    /// The read is done, on every way out
    fn drop(&mut self) {
        self.pace.end_read(self.bytes, Instant::now());
    }
}

/// A foreground read in flight on a device, counted until it drops
#[derive(Debug)]
pub struct ForegroundRead<'a>(&'a AtomicU64);

impl Drop for ForegroundRead<'_> {
    /// The read is done
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl TablePace {
    /// A device's pace, full to start
    ///
    /// # Arguments
    ///
    /// * `device` - The device, by its number
    /// * `shards` - How many shards the node runs, a slot each
    /// * `ceiling` - What every shard's scans read of it a second together
    /// * `trickle` - What they read of it a second while a foreground read is in flight on it
    /// * `device_time` - Its busy time a second every shard's scans may take together
    /// * `device_reads` - The most scan reads in flight on it at once
    #[must_use]
    pub fn new(
        device: u64,
        shards: usize,
        ceiling: u64,
        trickle: u64,
        device_time: Duration,
        device_reads: u32,
    ) -> Self {
        // each bucket holds a tenth of a second's worth, and every shard has a slot of its own
        let share = u64::try_from(device_time.as_nanos()).unwrap_or(u64::MAX).max(1);
        TablePace {
            device,
            ceiling: Ceiling::with_burst(ceiling, burst_of(ceiling)),
            trickle: Ceiling::with_burst(trickle, burst_of(trickle)),
            slots: (0..shards.max(1)).map(|_| Slot::default()).collect(),
            idle_bytes: AtomicU64::new(0),
            trickled_bytes: AtomicU64::new(0),
            share: Ceiling::with_burst(share, (share / 10).max(1)),
            cost: AtomicU64::new(0),
            busy: Mutex::new(Busy {
                in_flight: 0,
                since: Instant::now(),
                time: Duration::ZERO,
                bytes: 0,
            }),
            reads_bound: u64::from(device_reads.max(1)),
            busy_ns: AtomicU64::new(0),
            asks: AtomicU64::new(0),
        }
    }

    /// The device, by its number
    #[must_use]
    pub fn device(&self) -> u64 {
        self.device
    }

    /// Count a foreground read in flight on the device until the guard drops
    ///
    /// # Arguments
    ///
    /// * `slot` - The shard's slot, its id on this node
    #[must_use]
    pub fn foreground(&self, slot: usize) -> ForegroundRead<'_> {
        // a slot past the node's shards shares another's line, which counts all the same
        let slot = &self.slots[slot % self.slots.len()].0;
        slot.fetch_add(1, Ordering::Relaxed);
        ForegroundRead(slot)
    }

    /// How many foreground reads are in flight on the device now
    #[must_use]
    pub fn foreground_reads(&self) -> u64 {
        self.slots.iter().map(|slot| slot.0.load(Ordering::Relaxed)).sum()
    }

    /// Whether any shard has a foreground read in flight on the device
    #[must_use]
    pub fn busy(&self) -> bool {
        self.slots.iter().any(|slot| slot.0.load(Ordering::Relaxed) > 0)
    }

    /// Take a chunk of a scan's read of the device whole, or say how long until it may be taken:
    /// its device time from its share, its bytes from the ceiling, and its bytes from the trickle
    /// too while a foreground read is in flight on the device, all of it or none (F128)
    ///
    /// A chunk is at most [`Self::chunk`] bytes, so each bucket holds the whole of it once it is due.
    ///
    /// # Arguments
    ///
    /// * `bytes` - What the chunk reads
    /// * `time` - Its device time, in nanoseconds, by what a byte has shown it costs; none until
    ///   that is learned
    ///
    /// # Errors
    ///
    /// How long to wait: until the share or the ceiling holds the chunk, or a poll while a
    /// foreground read holds the trickle, by when it may be done.
    pub fn take(&self, bytes: u64, time: u64) -> Result<(), Duration> {
        self.asks.fetch_add(1, Ordering::Relaxed);
        // its share of the device's time first
        if time > 0 {
            self.share.take(time)?;
        }
        // its bytes from the ceiling, which bounds it whatever a byte costs
        if let Err(wait) = self.ceiling.take(bytes) {
            if time > 0 {
                self.share.refund(time);
            }
            return Err(wait);
        }
        // a foreground read in flight: the trickle's bytes too, given back with the rest if it has
        // none, and asked again within a poll, since the read may end sooner
        if self.busy() {
            // a chunk sized while the device was idle is past what the trickle holds whole: given
            // back and asked again at once, sized for the trickle, so it never reads past it
            if bytes > self.trickle.burst() {
                self.ceiling.refund(bytes);
                if time > 0 {
                    self.share.refund(time);
                }
                return Err(Duration::ZERO);
            }
            if let Err(wait) = self.trickle.take(bytes) {
                self.ceiling.refund(bytes);
                if time > 0 {
                    self.share.refund(time);
                }
                return Err(wait.min(PACE_POLL));
            }
            self.trickled_bytes.fetch_add(bytes, Ordering::Relaxed);
            return Ok(());
        }
        // the device idle of foreground reads: the share's and the ceiling's alone
        self.idle_bytes.fetch_add(bytes, Ordering::Relaxed);
        Ok(())
    }

    /// The most bytes one take of a scan's read holds: the smallest of what the ceiling, the share
    /// at what a byte costs, and while a foreground read is in flight the trickle hold at once, so
    /// a read larger than it is taken a chunk at a time and never bursts past a bucket (F128)
    #[must_use]
    pub fn chunk(&self) -> u64 {
        self.chunk_at(self.cost.load(Ordering::Relaxed))
    }

    /// The most bytes one take holds at a cost a byte
    ///
    /// # Arguments
    ///
    /// * `cost` - What a byte of the device costs, in picoseconds of its busy time, none if unknown
    fn chunk_at(&self, cost: u64) -> u64 {
        // the ceiling's burst, the trickle's while it binds, and the share's in bytes once a byte's
        // cost is known
        let mut chunk = self.ceiling.burst();
        if self.busy() {
            chunk = chunk.min(self.trickle.burst());
        }
        if cost > 0 {
            let share = u128::from(self.share.burst()) * 1_000 / u128::from(cost);
            chunk = chunk.min(u64::try_from(share).unwrap_or(u64::MAX));
        }
        chunk.max(1)
    }

    /// The next chunk of a scan's read and its device time, both by one reading of what a byte
    /// costs, which another shard's read may move between two, so the chunk's time is never more
    /// than the share holds (F128)
    ///
    /// # Arguments
    ///
    /// * `left` - What the read has left to take
    #[must_use]
    pub fn next_chunk(&self, left: u64) -> (u64, u64) {
        let cost = self.cost.load(Ordering::Relaxed);
        let chunk = left.min(self.chunk_at(cost));
        (chunk, time_at(chunk, cost))
    }

    /// The device time a scan's read takes, in nanoseconds, by what a byte of the device has shown
    /// it costs; none until the scans' reads have shown it (F128)
    ///
    /// # Arguments
    ///
    /// * `bytes` - What the read reads
    #[must_use]
    pub fn time_of(&self, bytes: u64) -> u64 {
        time_at(bytes, self.cost.load(Ordering::Relaxed))
    }

    /// What a byte of the device has shown it costs, in picoseconds of its busy time, none until
    /// learned (F128)
    #[must_use]
    pub fn cost(&self) -> u64 {
        self.cost.load(Ordering::Relaxed)
    }

    /// The bytes the share gives every shard's scans of the device a second, at what a byte has
    /// shown it costs; none until that is learned (F128)
    #[must_use]
    pub fn share_bytes(&self) -> Option<u64> {
        // nanoseconds a second over picoseconds a byte
        let cost = self.cost.load(Ordering::Relaxed);
        (cost > 0).then(|| {
            u64::try_from(u128::from(self.share.rate()) * 1_000 / u128::from(cost)).unwrap_or(u64::MAX)
        })
    }

    /// The most bytes the device gives every shard's scans in a second: its ceiling, its trickle
    /// while a foreground read is in flight on it, and its share at what a byte has shown it costs
    /// (F128)
    #[must_use]
    pub fn second_bytes(&self) -> u64 {
        // the ceiling always, the trickle while it binds, the share once its cost is known
        let mut bytes = self.ceiling.rate();
        if self.busy() {
            bytes = bytes.min(self.trickle.rate());
        }
        if let Some(share) = self.share_bytes() {
            bytes = bytes.min(share);
        }
        bytes.max(1)
    }

    /// Wait for room among the scans' reads in flight on the device, then count one in flight
    /// until the guard drops (F128)
    ///
    /// # Arguments
    ///
    /// * `bytes` - What the read reads
    pub async fn read(self: &Arc<Self>, bytes: u64) -> ScanRead {
        // no read is refused for the bound, only held until one in flight ends
        while !self.start_read(Instant::now()) {
            glommio::timer::sleep(SLOT_POLL).await;
        }
        ScanRead {
            pace: self.clone(),
            bytes,
        }
    }

    /// Count a scan's read in flight on the device if there is room for it, adding up the busy
    /// time to now first
    ///
    /// # Arguments
    ///
    /// * `now` - The time now
    fn start_read(&self, now: Instant) -> bool {
        let mut busy = self.busy.lock().unwrap_or_else(PoisonError::into_inner);
        // at the bound, nothing more is issued until one ends
        if busy.in_flight >= self.reads_bound {
            return false;
        }
        self.add_busy(&mut busy, now);
        busy.in_flight += 1;
        true
    }

    /// Count a scan's read done, its bytes toward what a byte of the device costs, learned again
    /// once the window holds a mebibyte
    ///
    /// # Arguments
    ///
    /// * `bytes` - What it read
    /// * `now` - The time now
    fn end_read(&self, bytes: u64, now: Instant) {
        let mut busy = self.busy.lock().unwrap_or_else(PoisonError::into_inner);
        self.add_busy(&mut busy, now);
        busy.in_flight = busy.in_flight.saturating_sub(1);
        busy.bytes += bytes;
        // only a window of a mebibyte or more is learned from
        if busy.bytes < LEARN_BYTES {
            return;
        }
        // what a byte cost in it, counted at most LEARN_RISE times what it was learned to cost
        let shown = u64::try_from(busy.time.as_nanos() * 1_000 / u128::from(busy.bytes)).unwrap_or(u64::MAX);
        let cost = self.cost.load(Ordering::Relaxed);
        // the first window is the cost, and each after it moves it a quarter of the way
        let learned = if cost == 0 {
            shown
        } else {
            let shown = shown.min(cost.saturating_mul(LEARN_RISE));
            (cost.saturating_mul(3).saturating_add(shown)) / 4
        };
        self.cost.store(learned.max(1), Ordering::Relaxed);
        busy.time = Duration::ZERO;
        busy.bytes = 0;
    }

    /// Add the time since the busy time was last added up, if any scan read was in flight in it
    ///
    /// # Arguments
    ///
    /// * `busy` - The device's reads in flight and busy time, locked
    /// * `now` - The time now
    fn add_busy(&self, busy: &mut Busy, now: Instant) {
        // any read in flight makes the device busy for the scans, however many are
        if busy.in_flight > 0 {
            let passed = now.saturating_duration_since(busy.since);
            busy.time += passed;
            self.busy_ns
                .fetch_add(u64::try_from(passed.as_nanos()).unwrap_or(u64::MAX), Ordering::Relaxed);
        }
        busy.since = now;
    }

    /// The scans' busy time on the device so far, every read in flight counted once (F128)
    #[must_use]
    pub fn busy_time(&self) -> Duration {
        Duration::from_nanos(self.busy_ns.load(Ordering::Relaxed))
    }

    /// How many scan reads are in flight on the device now (F128)
    #[must_use]
    pub fn scan_reads(&self) -> u64 {
        self.busy.lock().unwrap_or_else(PoisonError::into_inner).in_flight
    }

    /// How many times scans asked the device's buckets, for a test that a read waits once (O143)
    #[must_use]
    pub fn asks(&self) -> u64 {
        self.asks.load(Ordering::Relaxed)
    }

    /// The scan bytes taken while no foreground read was in flight on the device
    #[must_use]
    pub fn idle_bytes(&self) -> u64 {
        self.idle_bytes.load(Ordering::Relaxed)
    }

    /// The scan bytes taken from the device's trickle
    #[must_use]
    pub fn trickled_bytes(&self) -> u64 {
        self.trickled_bytes.load(Ordering::Relaxed)
    }

    /// What every shard's scans read of the device a second together
    #[must_use]
    pub fn ceiling(&self) -> u64 {
        self.ceiling.rate()
    }

    /// What they read of it a second while a foreground read is in flight on it
    #[must_use]
    pub fn trickle(&self) -> u64 {
        self.trickle.rate()
    }
}

/// What one scan page has taken from its device's pace, and how long it waited for it
#[derive(Debug)]
pub struct PagePace {
    /// The device's pace
    pace: Arc<TablePace>,
    /// How long the page waited for its device
    waited: Duration,
    /// What the page has done so far, which whoever waits for its answer watches, so a page its
    /// device paces is never taken for one that stopped (F128)
    progress: Option<Arc<AtomicU64>>,
}

impl PagePace {
    /// A page's pace through a device, nothing taken yet
    ///
    /// # Arguments
    ///
    /// * `pace` - The device's pace
    #[must_use]
    pub fn new(pace: Arc<TablePace>) -> Self {
        PagePace {
            pace,
            waited: Duration::ZERO,
            progress: None,
        }
    }

    /// A page's pace through a device that counts what the page does where its answer's waiter
    /// sees it (F128)
    ///
    /// # Arguments
    ///
    /// * `pace` - The device's pace
    /// * `progress` - Where the page's progress is counted, none if nothing waits on it
    #[must_use]
    pub fn reporting(pace: Arc<TablePace>, progress: Option<Arc<AtomicU64>>) -> Self {
        PagePace {
            progress,
            ..PagePace::new(pace)
        }
    }

    /// Another pace of the same page through the same device, nothing taken yet, for the reads a
    /// page's reader issues apart from its merge, counting the page's progress where it does (M23)
    #[must_use]
    pub fn beside(&self) -> Self {
        PagePace::reporting(self.pace.clone(), self.progress.clone())
    }

    /// Count a step of the page's work done: a chunk of a read taken, a partition handed out, an
    /// encode step taken (F128)
    pub fn progressed(&self) {
        if let Some(progress) = &self.progress {
            progress.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The device's pace, which a read of the page holds its place among the device's scan reads
    /// in flight through (F128)
    #[must_use]
    pub fn device(&self) -> &Arc<TablePace> {
        &self.pace
    }

    /// Wait until the device gives the page `bytes` to read: its device time from its share, at
    /// what a byte has shown it costs, its bytes from the ceiling, and from the trickle too while a
    /// foreground read is in flight on it
    ///
    /// Taken in as few chunks as its buckets allow, one most often, each waited for only until it
    /// is due, so a read asks its device once a chunk unless its share, its ceiling or a foreground
    /// read holds it, never once a credit and never a poll beside an idle device
    /// ([O143](../../../docs/src/appendix/optimizations.md#o143-a-paced-scan-pages-waits-for-credit-double-its-cost-a-partition),
    /// [F128](../../../docs/src/features/device-time-pace.md)).
    ///
    /// # Arguments
    ///
    /// * `bytes` - What the page is about to read
    pub async fn take(&mut self, bytes: u64) {
        let started = Instant::now();
        let mut slept = false;
        let mut left = bytes;
        while left > 0 {
            // a chunk each bucket holds whole, its device time by what a byte costs, sized again at
            // every ask, since a foreground read or a cost learned meanwhile changes what they hold
            let (chunk, time) = self.pace.next_chunk(left);
            match self.pace.take(chunk, time) {
                // a chunk taken is the page moving, however slowly its device gives it bytes
                Ok(()) => {
                    left -= chunk;
                    self.progressed();
                }
                // a chunk sized for an idle device that found it busy is sized again at once,
                // which is no wait for the device
                Err(wait) => {
                    slept |= !wait.is_zero();
                    glommio::timer::sleep(wait).await;
                }
            }
        }
        // only a page that waited for its device says it was paced
        if slept {
            self.waited += started.elapsed();
        }
    }

    /// How long the page waited for its device
    #[must_use]
    pub fn waited(&self) -> Duration {
        self.waited
    }
}

/// The device time of a read at a cost a byte, in nanoseconds
///
/// # Arguments
///
/// * `bytes` - What the read reads
/// * `cost` - What a byte costs, in picoseconds of the device's busy time
fn time_at(bytes: u64, cost: u64) -> u64 {
    // picoseconds a byte, in nanoseconds for the read
    u64::try_from(u128::from(bytes) * u128::from(cost) / 1_000).unwrap_or(u64::MAX)
}

/// The device a path is on, by its number, read from the nearest directory above it that exists
///
/// # Arguments
///
/// * `path` - The path
fn device_of(path: &Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    path.ancestors().find_map(|dir| std::fs::metadata(dir).ok()).map(|meta| meta.dev())
}

/// Every device's pace on a node, each made the first time a shard asks for the device a table's
/// archives are on
#[derive(Debug)]
pub struct Paces {
    /// How many shards the node runs, a slot each in every pace
    shards: usize,
    /// What the scans of every device read of it a second together
    ceiling: u64,
    /// What they read of it a second while a foreground read is in flight on it
    trickle: u64,
    /// Its busy time a second every shard's scans may take together (F128)
    device_time: Duration,
    /// The most scan reads in flight on it at once (F128)
    device_reads: u32,
    /// Every device's pace, few enough to find by a walk
    devices: Mutex<Vec<Arc<TablePace>>>,
}

impl Paces {
    /// No device's pace yet
    ///
    /// # Arguments
    ///
    /// * `shards` - How many shards the node runs
    /// * `ceiling` - What the scans of every device read of it a second together
    /// * `trickle` - What they read of it a second while a foreground read is in flight on it
    /// * `device_time` - Each device's busy time a second the scans may take of it together
    /// * `device_reads` - The most scan reads in flight on each device at once
    #[must_use]
    pub fn new(shards: usize, ceiling: u64, trickle: u64, device_time: Duration, device_reads: u32) -> Self {
        Paces {
            shards,
            ceiling,
            trickle,
            device_time,
            device_reads,
            devices: Mutex::new(Vec::new()),
        }
    }

    /// The pace of the device a path is on, made the first time any shard asks for it
    ///
    /// Returns none for a path that neither it nor any directory above it can be read from.
    ///
    /// # Arguments
    ///
    /// * `path` - Where a table's archives are
    #[must_use]
    pub fn of(&self, path: &Path) -> Option<Arc<TablePace>> {
        let device = device_of(path)?;
        // asked once a table a shard, at its start, so the lock is no query's cost
        let mut devices = self.devices.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(pace) = devices.iter().find(|pace| pace.device == device) {
            return Some(pace.clone());
        }
        let pace = Arc::new(TablePace::new(
            device,
            self.shards,
            self.ceiling,
            self.trickle,
            self.device_time,
            self.device_reads,
        ));
        devices.push(pace.clone());
        Some(pace)
    }

    /// Every device's pace made so far
    #[must_use]
    pub fn all(&self) -> Vec<Arc<TablePace>> {
        self.devices.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }
}

/// What every shard of a node shares for a scan's pages
#[derive(Debug)]
pub struct ScanResources {
    /// The memory every scan page's buffers on this node are drawn from
    pub budget: Arc<Budget>,
    /// The pace of every device a table's archives are on, none on a node that answers no scans
    /// (F119)
    pub paces: Option<Paces>,
}

impl ScanResources {
    /// What a node's config gives its scans, its scan memory settled at its start
    ///
    /// # Arguments
    ///
    /// * `conf` - The node's config, `resources.scan_memory` settled
    /// * `shards` - How many shards the node runs
    #[must_use]
    pub fn new(conf: &Conf, shards: usize) -> Self {
        ScanResources {
            budget: Budget::new("the scan budget", conf.resources.scan_budget() as u64),
            paces: conf
                .scans
                .enabled
                .then(|| {
                    Paces::new(
                        shards,
                        conf.scans.ceiling,
                        conf.scans.trickle,
                        conf.scans.device_time.0,
                        conf.scans.device_reads,
                    )
                }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{burst_of, PagePace, Paces, TablePace, CREDIT, LEARN_BYTES};
    use glommio::LocalExecutorBuilder;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    /// A foreground read on any shard's slot makes the device busy for every shard's scans, an
    /// idle read is the ceiling's alone, and a busy one takes the trickle's bytes too, nothing taken
    /// of either while the ceiling has none
    #[test]
    fn a_devices_pace_is_shared_by_its_shards() {
        // a ceiling of two credits a second and a trickle of one, each holding one at most
        let pace = TablePace::new(7, 2, 2 * CREDIT, CREDIT, Duration::from_millis(75), 4);
        assert!(!pace.busy());
        // idle: the ceiling's credit, then a wait of half a second for the next
        assert_eq!(pace.take(CREDIT, 0), Ok(()));
        assert!(pace.take(CREDIT, 0).is_err());
        assert_eq!(pace.idle_bytes(), CREDIT);
        // shard one's read makes the device busy for shard zero's scans too
        let read = pace.foreground(1);
        assert!(pace.busy());
        assert_eq!(pace.foreground_reads(), 1);
        // the ceiling holds none, so nothing is trickled
        assert!(pace.take(CREDIT, 0).is_err());
        assert_eq!(pace.trickled_bytes(), 0);
        // once the ceiling holds a credit again the trickle's is taken with it
        std::thread::sleep(Duration::from_millis(550));
        assert_eq!(pace.take(CREDIT, 0), Ok(()));
        assert_eq!(pace.trickled_bytes(), CREDIT);
        assert!(pace.take(CREDIT, 0).is_err());
        // and a slot past the node's shards still counts
        let other = pace.foreground(5);
        assert_eq!(pace.foreground_reads(), 2);
        drop((read, other));
        assert!(!pace.busy());
        assert_eq!(burst_of(40 * CREDIT), 4 * CREDIT);
    }

    /// What a byte of a device costs is learned from the time any scan read is in flight on it,
    /// reads in flight together counted once, and a read is charged that time from the scans'
    /// share of the device, which bounds their bytes a second by it ([F128](../../../docs/src/features/device-time-pace.md))
    #[test]
    fn a_scans_device_time_is_learned_from_its_reads() {
        // a share of ten milliseconds a second, holding one at most
        let pace = TablePace::new(7, 1, 1 << 30, 1 << 30, Duration::from_millis(10), 4);
        assert_eq!(pace.time_of(1 << 20), 0, "a cost was known before any read");
        // two reads in flight together over two milliseconds, half a mebibyte each
        let start = Instant::now();
        assert!(pace.start_read(start));
        assert!(pace.start_read(start + Duration::from_micros(100)));
        pace.end_read(512 << 10, start + Duration::from_millis(1));
        pace.end_read(512 << 10, start + Duration::from_millis(2));
        // the device was busy two milliseconds for a mebibyte, not three
        assert_eq!(pace.busy_time(), Duration::from_millis(2));
        let time = pace.time_of(1 << 20);
        assert!((1_999_000..=2_001_000).contains(&time), "a mebibyte costs {time} ns");
        // the share holds a millisecond, half a mebibyte's time, so a read is taken half a
        // mebibyte at a time and the next half waits a tenth of a second: five mebibytes a second
        let chunk = pace.chunk();
        assert!((511 << 10..=513 << 10).contains(&chunk), "a chunk of {chunk} bytes");
        assert_eq!(pace.take(chunk, pace.time_of(chunk)), Ok(()));
        let wait = pace.take(chunk, pace.time_of(chunk)).expect_err("the share held more than it was given");
        assert!(wait >= Duration::from_millis(95) && wait <= Duration::from_millis(105), "{wait:?}");
    }

    /// No read alone sets what a byte of a device costs, however slow, and a window slowed by
    /// something other than the scans' reads moves it at most 1.75 times, where a fast one lowers it
    /// a quarter of the way ([F128](../../../docs/src/features/device-time-pace.md))
    #[test]
    fn a_slow_window_or_a_lone_read_moves_a_devices_cost_little() {
        let pace = TablePace::new(7, 1, 1 << 30, 1 << 30, Duration::from_millis(75), 4);
        // a small read in flight sixty milliseconds is no window of its own
        let start = Instant::now();
        assert!(pace.start_read(start));
        pace.end_read(4096, start + Duration::from_millis(60));
        assert_eq!(pace.cost(), 0, "a lone small read set what every byte costs");
        // the rest of its mebibyte at once: sixty milliseconds a mebibyte, the first window's cost
        let start = start + Duration::from_millis(100);
        assert!(pace.start_read(start));
        pace.end_read(LEARN_BYTES - 4096, start);
        let first = pace.time_of(LEARN_BYTES);
        assert!((59_900_000..=60_100_000).contains(&first), "a mebibyte costs {first} ns");
        // a mebibyte over three seconds is counted as four times the cost, which moves it 1.75 times
        let start = start + Duration::from_millis(100);
        assert!(pace.start_read(start));
        pace.end_read(LEARN_BYTES, start + Duration::from_secs(3));
        let slowed = pace.time_of(LEARN_BYTES);
        assert!((104_900_000..=105_100_000).contains(&slowed), "a slow window moved it to {slowed} ns");
        // and a mebibyte over a millisecond lowers it a quarter of the way toward a millisecond
        let start = start + Duration::from_secs(4);
        assert!(pace.start_read(start));
        pace.end_read(LEARN_BYTES, start + Duration::from_millis(1));
        let fast = pace.time_of(LEARN_BYTES);
        assert!((78_900_000..=79_100_000).contains(&fast), "a fast window moved it to {fast} ns");
    }

    /// A chunk sized while its device was idle and taken once a foreground read is in flight is
    /// refused at once with nothing taken, and sized again for the trickle, so no take reads past
    /// what the trickle holds ([F128](../../../docs/src/features/device-time-pace.md))
    #[test]
    fn a_chunk_sized_idle_is_sized_again_once_its_device_is_busy() {
        // a ceiling of a gibibyte a second and a trickle of sixteen credits
        let pace = TablePace::new(7, 1, 1 << 30, 16 * CREDIT, Duration::from_secs(1), 4);
        let (idle, _) = pace.next_chunk(u64::MAX);
        assert_eq!(idle, burst_of(1 << 30));
        // a foreground read begins before the chunk is taken: refused with no wait, nothing taken
        let read = pace.foreground(0);
        assert_eq!(pace.take(idle, 0), Err(Duration::ZERO));
        assert_eq!((pace.idle_bytes(), pace.trickled_bytes()), (0, 0));
        // sized again it is the trickle's, and taken whole from it
        let (busy, _) = pace.next_chunk(u64::MAX);
        assert_eq!(busy, burst_of(16 * CREDIT));
        assert_eq!(pace.take(busy, 0), Ok(()));
        assert_eq!(pace.trickled_bytes(), busy);
        // the refused chunk's bytes were given back to the ceiling, which holds the rest of it now
        drop(read);
        assert_eq!(pace.take(idle - busy, 0), Ok(()));
    }

    /// No more than a device's bound of scan reads is in flight on it at once, and one that ends
    /// makes room for the next ([F128](../../../docs/src/features/device-time-pace.md))
    #[test]
    fn a_device_holds_at_most_its_scan_reads_in_flight() {
        LocalExecutorBuilder::default()
            .spawn(|| async move {
                let pace = Arc::new(TablePace::new(7, 1, 1 << 30, 1 << 30, Duration::from_millis(75), 2));
                // two in flight, and a third waits
                let first = pace.read(4096).await;
                let second = pace.read(4096).await;
                assert_eq!(pace.scan_reads(), 2);
                assert!(!pace.start_read(Instant::now()));
                // one ends, and the third has its place
                drop(first);
                let third = pace.read(4096).await;
                assert_eq!(pace.scan_reads(), 2);
                drop((second, third));
                assert_eq!(pace.scan_reads(), 0);
            })
            .expect("an executor")
            .join()
            .expect("the test ran");
    }

    /// A paced read asks its device once if its buckets hold it, and again only once it is due,
    /// never a poll at a time: one ask for a mebibyte where the credits of before asked sixteen
    /// ([O143](../../../docs/src/appendix/optimizations.md#o143-a-paced-scan-pages-waits-for-credit-double-its-cost-a-partition))
    #[test]
    fn a_paced_read_asks_its_device_once_a_wait() {
        LocalExecutorBuilder::default()
            .spawn(|| async move {
                // a ceiling of ten mebibytes a second, holding one
                let pace = Arc::new(TablePace::new(7, 1, 10 << 20, 10 << 20, Duration::from_secs(1), 4));
                let mut page = PagePace::new(pace.clone());
                // a mebibyte the ceiling holds passes on one ask
                page.take(1 << 20).await;
                assert_eq!(pace.asks(), 1);
                // the next waits once, for exactly a tenth of a second, and passes on its second ask
                let started = Instant::now();
                page.take(1 << 20).await;
                assert!(started.elapsed() >= Duration::from_millis(95), "{:?}", started.elapsed());
                assert_eq!(pace.asks(), 3, "a paced read polled its device");
            })
            .expect("an executor")
            .join()
            .expect("the test ran");
    }

    /// Every shard asking for one device is handed the one pace, and a path under it that does not
    /// exist yet is that device's too
    #[test]
    fn every_shard_on_a_device_shares_its_pace() {
        let dir = std::env::temp_dir();
        let paces = Paces::new(4, 100 << 20, 2 << 20, Duration::from_millis(75), 4);
        let first = paces.of(&dir).expect("a device");
        let again = paces.of(&dir.join("not-made-yet").join("archives")).expect("its parent's device");
        assert!(Arc::ptr_eq(&first, &again));
        assert_eq!(paces.all().len(), 1);
        assert_eq!((first.ceiling(), first.trickle()), (100 << 20, 2 << 20));
    }

    /// A page reads only as its device gives it bytes: at its trickle while a foreground read is
    /// in flight, a read past what the trickle holds taken a chunk at a time, and at once in the
    /// device's idle time
    #[test]
    fn a_page_waits_for_its_trickle_while_a_read_is_in_flight() {
        LocalExecutorBuilder::default()
            .spawn(|| async move {
                // a trickle of sixteen credits a second, holding a tenth of a second's worth
                let pace = Arc::new(TablePace::new(7, 1, 1 << 30, 16 * CREDIT, Duration::from_secs(1), 4));
                let read = pace.foreground(0);
                let mut page = PagePace::new(pace.clone());
                // eight credits, taken 1.6 at a time: the first held, the other 6.4 at sixteen a
                // second, four tenths of a second
                let started = Instant::now();
                page.take(8 * CREDIT).await;
                let took = started.elapsed();
                assert!(took >= Duration::from_millis(350), "trickled 8 credits in {took:?}");
                assert!(page.waited() > Duration::ZERO);
                assert_eq!(pace.trickled_bytes(), 8 * CREDIT);
                // idle, the same bytes take no wait past the ceiling's
                drop(read);
                let mut idle = PagePace::new(pace.clone());
                let started = Instant::now();
                idle.take(8 * CREDIT).await;
                assert!(started.elapsed() < Duration::from_millis(100), "{:?}", started.elapsed());
                assert_eq!(idle.waited(), Duration::ZERO);
                assert_eq!(pace.idle_bytes(), 8 * CREDIT);
            })
            .expect("an executor")
            .join()
            .expect("the test ran");
    }
}
