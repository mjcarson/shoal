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
//! trickle too while any shard has a foreground read in flight on the device.**
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

/// How long a scan waits before it asks its device's pace again, the object store's poll
pub const PACE_POLL: Duration = Duration::from_millis(1);

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
    #[must_use]
    pub fn new(device: u64, shards: usize, ceiling: u64, trickle: u64) -> Self {
        // each bucket holds a tenth of a second's worth, and every shard has a slot of its own
        TablePace {
            device,
            ceiling: Ceiling::with_burst(ceiling, burst_of(ceiling)),
            trickle: Ceiling::with_burst(trickle, burst_of(trickle)),
            slots: (0..shards.max(1)).map(|_| Slot::default()).collect(),
            idle_bytes: AtomicU64::new(0),
            trickled_bytes: AtomicU64::new(0),
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

    /// Take one credit for a scan's reads of the device, or say how long until it asks again
    ///
    /// # Errors
    ///
    /// How long to wait: a poll while a foreground read holds the trickle, or the time the
    /// ceiling needs to hold a credit.
    pub fn credit(&self) -> Result<(), Duration> {
        // a foreground read in flight: the trickle's bytes, and the ceiling's
        if self.busy() {
            // none to trickle yet: asked again once a read may have ended
            if self.trickle.take(CREDIT).is_err() {
                return Err(PACE_POLL);
            }
            if let Err(wait) = self.ceiling.take(CREDIT) {
                // the trickle's bytes given back, since nothing is read on them
                self.trickle.refund(CREDIT);
                return Err(wait.max(PACE_POLL));
            }
            self.trickled_bytes.fetch_add(CREDIT, Ordering::Relaxed);
            return Ok(());
        }
        // the device idle of foreground reads: the ceiling's alone
        self.ceiling.take(CREDIT).map_err(|wait| wait.max(PACE_POLL))?;
        self.idle_bytes.fetch_add(CREDIT, Ordering::Relaxed);
        Ok(())
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
    /// The bytes taken and not yet read
    credit: u64,
    /// How long the page waited for its device
    waited: Duration,
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
            credit: 0,
            waited: Duration::ZERO,
        }
    }

    /// Another pace of the same page through the same device, nothing taken yet, for the reads a
    /// page's reader issues apart from its merge (M23)
    #[must_use]
    pub fn beside(&self) -> Self {
        PagePace::new(self.pace.clone())
    }

    /// Wait until the device gives the page `bytes` to read: in its idle time under its ceiling,
    /// or within its trickle
    ///
    /// # Arguments
    ///
    /// * `bytes` - What the page is about to read
    pub async fn take(&mut self, bytes: u64) {
        // what the page holds already pays first
        if self.credit >= bytes {
            self.credit -= bytes;
            return;
        }
        let started = Instant::now();
        let mut slept = false;
        while self.credit < bytes {
            match self.pace.credit() {
                Ok(()) => self.credit += CREDIT,
                Err(wait) => {
                    slept = true;
                    glommio::timer::sleep(wait).await;
                }
            }
        }
        // only a page that waited for its device says it was paced
        if slept {
            self.waited += started.elapsed();
        }
        self.credit -= bytes;
    }

    /// How long the page waited for its device
    #[must_use]
    pub fn waited(&self) -> Duration {
        self.waited
    }
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
    #[must_use]
    pub fn new(shards: usize, ceiling: u64, trickle: u64) -> Self {
        Paces {
            shards,
            ceiling,
            trickle,
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
        let pace = Arc::new(TablePace::new(device, self.shards, self.ceiling, self.trickle));
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
                .then(|| Paces::new(shards, conf.scans.ceiling, conf.scans.trickle)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{burst_of, PagePace, Paces, TablePace, CREDIT};
    use glommio::LocalExecutorBuilder;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    /// A foreground read on any shard's slot makes the device busy for every shard's scans, an
    /// idle credit is the ceiling's alone, and a busy one takes the trickle's bytes too, giving
    /// them back when the ceiling has none
    #[test]
    fn a_devices_pace_is_shared_by_its_shards() {
        // a ceiling of two credits a second and a trickle of one, each holding one at most
        let pace = TablePace::new(7, 2, 2 * CREDIT, CREDIT);
        assert!(!pace.busy());
        // idle: the ceiling's credit, then a wait of half a second for the next
        assert_eq!(pace.credit(), Ok(()));
        assert!(pace.credit().is_err());
        assert_eq!(pace.idle_bytes(), CREDIT);
        // shard one's read makes the device busy for shard zero's scans too
        let read = pace.foreground(1);
        assert!(pace.busy());
        assert_eq!(pace.foreground_reads(), 1);
        // the trickle holds its credit but the ceiling none: nothing trickled
        assert!(pace.credit().is_err());
        assert_eq!(pace.trickled_bytes(), 0);
        // once the ceiling holds a credit again the trickle's is taken with it: the credit given
        // back, since in a little over half a second the trickle tops up only half of one
        std::thread::sleep(Duration::from_millis(550));
        assert_eq!(pace.credit(), Ok(()));
        assert_eq!(pace.trickled_bytes(), CREDIT);
        assert!(pace.credit().is_err());
        // and a slot past the node's shards still counts
        let other = pace.foreground(5);
        assert_eq!(pace.foreground_reads(), 2);
        drop((read, other));
        assert!(!pace.busy());
        assert_eq!(burst_of(40 * CREDIT), 4 * CREDIT);
    }

    /// Every shard asking for one device is handed the one pace, and a path under it that does not
    /// exist yet is that device's too
    #[test]
    fn every_shard_on_a_device_shares_its_pace() {
        let dir = std::env::temp_dir();
        let paces = Paces::new(4, 100 << 20, 2 << 20);
        let first = paces.of(&dir).expect("a device");
        let again = paces.of(&dir.join("not-made-yet").join("archives")).expect("its parent's device");
        assert!(Arc::ptr_eq(&first, &again));
        assert_eq!(paces.all().len(), 1);
        assert_eq!((first.ceiling(), first.trickle()), (100 << 20, 2 << 20));
    }

    /// A page reads past its credits only as its device gives them: at its trickle while a
    /// foreground read is in flight, and at once in the device's idle time
    #[test]
    fn a_page_waits_for_its_trickle_while_a_read_is_in_flight() {
        LocalExecutorBuilder::default()
            .spawn(|| async move {
                // a trickle of sixteen credits a second, holding a tenth of a second's worth
                let pace = Arc::new(TablePace::new(7, 1, 1 << 30, 16 * CREDIT));
                let read = pace.foreground(0);
                let mut page = PagePace::new(pace.clone());
                // eight credits: 1.6 held, the other 6.4 at sixteen a second, four tenths of a second
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
