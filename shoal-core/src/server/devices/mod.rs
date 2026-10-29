//! A node's devices: claimed at its start, reported once a tick, served by its object executors
//!
//! A node claims every device its configuration names before any executor starts, as it claims
//! its storage roots ([`claim`]); what it claimed is held here, shared by the control thread,
//! which reports each device's figures and commits its identity, and by the object executors,
//! which own the slices and mark a device failed when it returns an error
//! ([S4](../../../../docs/src/object-storage/pools-and-devices.md),
//! [F85](../../../../docs/src/features/devices-and-pools.md)).
//!
//! # Invariants
//!
//! **A failing device stops nothing else** (P17): an error marks it and every slice on it failed,
//! it is reported, and every later operation on it is refused by name; the node, its tables and
//! its other devices go on ([S13](../../../../docs/src/object-storage/isolation.md#a-failing-pool-device-does-not-stop-its-node)).
//!
//! **An executor number is never reported.** Which executor owns a slice is the node's own
//! business; a report names devices and slices.

pub mod claim;
pub mod probe;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use crate::shared::devices::{
    ApplySync, DeviceFacts, DeviceHealth, DeviceId, DeviceReport, DeviceSpec, JournalId, SliceId,
    SliceReport,
};

pub use claim::{Claimed, DeviceJournal, FailedDevice};
pub use probe::Facts;

/// One slice a node serves, and the figures its owning executor keeps
#[derive(Debug)]
pub struct SliceState {
    /// Its id
    pub id: SliceId,
    /// Its index on its device
    pub index: u16,
    /// Its directory
    pub path: PathBuf,
    /// The directory its journal is in: under it, or under its device's journal on an SSD (F91)
    pub journal: PathBuf,
    /// The stripe chunks it holds
    pub chunks: AtomicU64,
    /// The bytes of the chunk files it holds, headers included: what reclamation gives back (M20)
    pub chunk_bytes: AtomicU64,
    /// The bytes staged on it and not yet applied or discarded
    pub staged_bytes: AtomicU64,
    /// The bytes its reads have answered, for a test that a range read reads only its range
    pub read_bytes: AtomicU64,
    /// Every cpu an operation on it has run on: its owner's alone, by S13's rule
    pub served_on: Mutex<std::collections::BTreeSet<usize>>,
    /// The foreground requests in flight on it: a background one waits for none (F88)
    pub foreground: AtomicU64,
    /// The reads this node chose it for and has not had answered, counted from the choice: its
    /// foreground count moves only once its executor takes a read from its mailbox, so this is the
    /// floor under its queue a read chooses a holder by (item 314)
    pub chosen: AtomicU64,
    /// The bytes background requests have moved on it
    pub background_bytes: AtomicU64,
    /// The rebuild and move requests in flight on it: a scrub's waits for none (F89)
    pub recovery: AtomicU64,
    /// Whether a scrub request holds the slice's one permit (F89)
    pub scrubbing: AtomicU64,
    /// The scrub requests being served on it, counted apart from the permit
    pub scrub_inflight: AtomicU64,
    /// The most scrub requests ever served on it at once, for a test that it is one
    pub scrub_peak: AtomicU64,
    /// The bytes deep scrubs have read and verified on it
    pub scrub_bytes: AtomicU64,
    /// The units and headers on it that failed their checksums, each found once
    pub checksum_failures: AtomicU64,
    /// The chunks it holds in quarantine
    pub quarantined: AtomicU64,
    /// The applies running on it: below the foreground, above a rebuild, a move and a scrub,
    /// which wait for none (S13, F91)
    pub applying: AtomicU64,
    /// The batches of applies a rotational slice has issued (F91)
    pub apply_batches: AtomicU64,
    /// Whether a batch of applies is in flight on it, one or zero
    pub apply_batch_in_flight: AtomicU64,
    /// The most applies one batch on it took
    pub apply_batch_peak: AtomicU64,
    /// The most applies ever running on it at once
    pub apply_inflight_peak: AtomicU64,
    /// The longest one batch on it took, in microseconds
    pub apply_batch_longest_us: AtomicU64,
    /// The most batches a read of it waited behind: the one in flight when it began, and every one
    /// begun while it ran
    pub read_batches_peak: AtomicU64,
    /// The reads of its chunks issued to its device, for a test that a read on a disk is one span
    pub read_ios: AtomicU64,
}

impl SliceState {
    /// Count a foreground request in flight on the slice until the guard drops
    #[must_use]
    pub fn foreground(self: &Arc<Self>) -> Foreground {
        self.foreground.fetch_add(1, Ordering::Relaxed);
        Foreground(self.clone())
    }

    /// Count a read this node chose the slice for until the guard drops, from the choice to the
    /// answer (item 314)
    #[must_use]
    pub fn chosen(self: &Arc<Self>) -> Chosen {
        self.chosen.fetch_add(1, Ordering::Relaxed);
        Chosen(self.clone())
    }

    /// The reads queued on it as a read choosing a holder sees them: every foreground request in
    /// flight on it, and never fewer than the reads this node chose it for (F97, item 314)
    #[must_use]
    pub fn queue(&self) -> u64 {
        self.foreground
            .load(Ordering::Relaxed)
            .max(self.chosen.load(Ordering::Relaxed))
    }

    /// Whether no foreground request is in flight on it
    #[must_use]
    pub fn idle(&self) -> bool {
        self.foreground.load(Ordering::Relaxed) == 0
    }

    /// Count an apply running on the slice until the guard drops (F91)
    #[must_use]
    pub fn applying(self: &Arc<Self>) -> Applying {
        let running = self.applying.fetch_add(1, Ordering::AcqRel) + 1;
        self.apply_inflight_peak.fetch_max(running, Ordering::Relaxed);
        Applying(self.clone())
    }

    /// Whether neither a foreground request nor an apply is in flight on it: what a rebuild and a
    /// move wait for, since applies are below the foreground and above recovery (S13)
    #[must_use]
    pub fn idle_for_background(&self) -> bool {
        self.idle() && self.applying.load(Ordering::Relaxed) == 0
    }

    /// Count a rebuild's or a move's request in flight on the slice until the guard drops (F89)
    #[must_use]
    pub fn recovering(self: &Arc<Self>) -> Recovering {
        self.recovery.fetch_add(1, Ordering::Relaxed);
        Recovering(self.clone())
    }

    /// Whether neither a foreground request nor a rebuild's or a move's is in flight on it: what
    /// a scrub waits for, since a scrub yields to recovery and recovery to the foreground
    #[must_use]
    pub fn idle_for_scrub(&self) -> bool {
        self.idle_for_background() && self.recovery.load(Ordering::Relaxed) == 0
    }

    /// Take the slice's one scrub permit, if no scrub request is in flight on it
    ///
    /// Answers the permit, which gives the slot back when it drops, or none when a scrub request
    /// holds it: a slice serves one scrub piece at a time ([X12](../../../../docs/src/object-storage/recovery-scrub-rates.md#the-comparison)).
    #[must_use]
    pub fn try_scrub(self: &Arc<Self>) -> Option<Scrubbing> {
        // one at a time: the slot is taken only from none
        if self
            .scrubbing
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return None;
        }
        // counted apart from the permit, so the peak says what was served at once
        let serving = self.scrub_inflight.fetch_add(1, Ordering::AcqRel) + 1;
        self.scrub_peak.fetch_max(serving, Ordering::Relaxed);
        Some(Scrubbing(self.clone()))
    }
}

/// Take some from a counter, never below zero: a figure an operator reads, which a count running
/// in the background at a slice's open may not have reached yet
///
/// # Arguments
///
/// * `counter` - The counter
/// * `n` - How much to take
pub(crate) fn take_from(counter: &AtomicU64, n: u64) {
    let mut now = counter.load(Ordering::Relaxed);
    // a swap from what it held, tried again if another changed it meanwhile
    while let Err(held) = counter.compare_exchange_weak(now, now.saturating_sub(n), Ordering::Relaxed, Ordering::Relaxed) {
        now = held;
    }
}

/// A foreground request in flight on a slice, counted until it drops
pub struct Foreground(Arc<SliceState>);

impl Drop for Foreground {
    /// The request is done
    fn drop(&mut self) {
        self.0.foreground.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A read this node chose a slice for, counted until it drops (item 314)
pub struct Chosen(Arc<SliceState>);

impl Drop for Chosen {
    /// The read is answered, or given up
    fn drop(&mut self) {
        self.0.chosen.fetch_sub(1, Ordering::Relaxed);
    }
}

/// An apply running on a slice, counted until it drops (F91)
pub struct Applying(Arc<SliceState>);

impl Drop for Applying {
    /// The apply is done
    fn drop(&mut self) {
        self.0.applying.fetch_sub(1, Ordering::AcqRel);
    }
}

/// A rebuild's or a move's request in flight on a slice, counted until it drops (F89)
pub struct Recovering(Arc<SliceState>);

impl Drop for Recovering {
    /// The request is done
    fn drop(&mut self) {
        self.0.recovery.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A slice's one scrub permit, held while a scrub request is served (F89)
pub struct Scrubbing(Arc<SliceState>);

impl Drop for Scrubbing {
    /// The request is done, and the slot free for the next
    fn drop(&mut self) {
        self.0.scrub_inflight.fetch_sub(1, Ordering::AcqRel);
        self.0.scrubbing.store(0, Ordering::Release);
    }
}

/// The bytes a device's recovery may still move this second, shared by every executor that
/// serves one of its slices
#[derive(Debug)]
pub struct Ceiling {
    /// Bytes a second
    rate: u64,
    /// The bytes that may move now, and when they were last topped up
    tokens: Mutex<(f64, std::time::Instant)>,
}

impl Ceiling {
    /// A ceiling of so many bytes a second, a second's worth to start
    ///
    /// # Arguments
    ///
    /// * `rate` - Bytes a second
    #[must_use]
    pub fn new(rate: u64) -> Self {
        Ceiling {
            rate: rate.max(1),
            tokens: Mutex::new((rate.max(1) as f64, std::time::Instant::now())),
        }
    }

    /// Take a request's bytes, or say how long until they can be taken
    ///
    /// A request larger than a second's worth waits for a whole second's and takes it all, so no
    /// request is held forever.
    ///
    /// # Arguments
    ///
    /// * `bytes` - What the request moves
    pub fn take(&self, bytes: u64) -> Result<(), std::time::Duration> {
        let mut tokens = self.tokens.lock().unwrap_or_else(PoisonError::into_inner);
        let now = std::time::Instant::now();
        // topped up for the time since, to a second's worth
        let rate = self.rate as f64;
        tokens.0 = (tokens.0 + now.duration_since(tokens.1).as_secs_f64() * rate).min(rate);
        tokens.1 = now;
        let want = (bytes as f64).min(rate);
        if tokens.0 >= want {
            tokens.0 -= want;
            return Ok(());
        }
        Err(std::time::Duration::from_secs_f64((want - tokens.0) / rate))
    }

    /// Bytes a second
    #[must_use]
    pub fn rate(&self) -> u64 {
        self.rate
    }
}

/// One device a node serves
#[derive(Debug)]
pub struct DeviceState {
    /// Its id
    pub id: DeviceId,
    /// What the configuration named
    pub spec: DeviceSpec,
    /// What it is on, as the probe found it at the claim
    pub facts: Facts,
    /// The free bytes it keeps
    pub reserve: u64,
    /// How its applies are made durable
    pub apply_sync: ApplySync,
    /// Its slices, by index
    pub slices: Vec<Arc<SliceState>>,
    /// What an operator is warned of about it
    pub warnings: Vec<String>,
    /// What its recovery may move a second, rebuilds and moves together (F88), and scrubs (F89)
    pub ceiling: Ceiling,
    /// Its journal on another device, if its slices do not stage under it (F91)
    pub journal: Option<DeviceJournal>,
    /// When each of its recent checksum failures was found, inside the window they are counted
    /// over (F89)
    checksum_times: Mutex<std::collections::VecDeque<std::time::Instant>>,
    /// When each of its checksum failures inside its drain window was found (F92)
    drain_times: Mutex<std::collections::VecDeque<std::time::Instant>>,
    /// Why its node asks for it to be drained, once it does (F92)
    drain_wanted: Mutex<Option<String>>,
    /// Why it failed, once it has
    failed: Mutex<Option<String>>,
}

impl DeviceState {
    /// A device and its slices as a node serves them, for a claim, a test or a harness
    ///
    /// # Arguments
    ///
    /// * `id` - Its id
    /// * `spec` - What the configuration named
    /// * `facts` - What it is on
    /// * `reserve` - The free bytes it keeps
    /// * `apply_sync` - How its applies are made durable
    /// * `slices` - Its slices' ids and directories, by index
    /// * `warnings` - What an operator is warned of about it
    #[must_use]
    pub fn new(
        id: DeviceId,
        spec: DeviceSpec,
        facts: Facts,
        reserve: u64,
        apply_sync: ApplySync,
        slices: Vec<(SliceId, PathBuf)>,
        warnings: Vec<String>,
    ) -> Self {
        let ceiling = Ceiling::new(spec.recovery_rate(facts.rotational));
        DeviceState {
            id,
            spec,
            facts,
            reserve,
            apply_sync,
            slices: slices
                .into_iter()
                .enumerate()
                .map(|(index, (id, path))| {
                    Arc::new(SliceState {
                        id,
                        index: u16::try_from(index).unwrap_or(u16::MAX),
                        journal: path.join("journal"),
                        path,
                        chunks: AtomicU64::new(0),
                        staged_bytes: AtomicU64::new(0),
                        read_bytes: AtomicU64::new(0),
                        served_on: Mutex::new(std::collections::BTreeSet::new()),
                        foreground: AtomicU64::new(0),
                        chosen: AtomicU64::new(0),
                        background_bytes: AtomicU64::new(0),
                        recovery: AtomicU64::new(0),
                        scrubbing: AtomicU64::new(0),
                        scrub_inflight: AtomicU64::new(0),
                        scrub_peak: AtomicU64::new(0),
                        scrub_bytes: AtomicU64::new(0),
                        checksum_failures: AtomicU64::new(0),
                        quarantined: AtomicU64::new(0),
                        chunk_bytes: AtomicU64::new(0),
                        applying: AtomicU64::new(0),
                        apply_batches: AtomicU64::new(0),
                        apply_batch_in_flight: AtomicU64::new(0),
                        apply_batch_peak: AtomicU64::new(0),
                        apply_inflight_peak: AtomicU64::new(0),
                        apply_batch_longest_us: AtomicU64::new(0),
                        read_batches_peak: AtomicU64::new(0),
                        read_ios: AtomicU64::new(0),
                    })
                })
                .collect(),
            warnings,
            ceiling,
            journal: None,
            checksum_times: Mutex::new(std::collections::VecDeque::new()),
            drain_times: Mutex::new(std::collections::VecDeque::new()),
            drain_wanted: Mutex::new(None),
            failed: Mutex::new(None),
        }
    }

    /// Stage its slices in a journal on another device, each slice in its own directory there
    ///
    /// For a claim and a test: the slices are the ones [`DeviceState::new`] just made, which nothing
    /// else holds yet, so each takes its journal's directory in place.
    ///
    /// # Arguments
    ///
    /// * `id` - The journal's id
    /// * `root` - The journal's directory
    /// * `lost` - Whether the journal the device last staged in is gone
    #[must_use]
    pub fn with_journal(mut self, id: JournalId, root: &std::path::Path, lost: bool) -> Self {
        for slice in &mut self.slices {
            if let Some(state) = Arc::get_mut(slice) {
                state.journal = claim::slice_journal_dir(&state.path, Some((root, self.id)), state.index);
            }
        }
        self.journal = Some(DeviceJournal {
            id,
            path: root.to_path_buf(),
            lost,
        });
        self
    }

    /// Whether its slices stage in a journal on another device
    #[must_use]
    pub fn journal_elsewhere(&self) -> bool {
        self.journal.is_some()
    }

    /// Whether the kernel calls its disk rotational, which batches its applies and reads a span in
    /// one I/O (F91)
    #[must_use]
    pub fn rotational(&self) -> bool {
        self.facts.rotational
    }

    /// Count a checksum failure found on one of its slices, and fail the device once its limit
    /// is reached inside its window (F89)
    ///
    /// A device whose chunks keep failing is failing: past the limit it is failed as an
    /// operator's order would fail it, its report says so, and its placement groups move off.
    /// Answers whether this failure failed it.
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice the failure was found on
    pub fn note_checksum_failure(&self, slice: &SliceState) -> bool {
        slice.checksum_failures.fetch_add(1, Ordering::Relaxed);
        // a few failures over a day ask for a drain first, while its chunks can still be copied
        // from it (F92)
        self.note_for_drain();
        let (limit, window) = self.spec.checksum_limit();
        if limit == 0 {
            return false;
        }
        // the failures inside the window, this one among them
        let now = std::time::Instant::now();
        let count = {
            let mut times = self.checksum_times.lock().unwrap_or_else(PoisonError::into_inner);
            times.push_back(now);
            while times
                .front()
                .is_some_and(|first| now.duration_since(*first) > window)
            {
                times.pop_front();
            }
            times.len()
        };
        if count < limit as usize || self.failed().is_some() {
            return false;
        }
        self.fail(format!(
            "{count} checksum failures within {}; a device whose chunks keep failing is failing",
            crate::shared::scrub::Span(window.as_secs())
        ));
        true
    }

    /// Count a checksum failure against the device's drain threshold, and ask for its drain once
    /// the threshold is reached inside its window (F92)
    fn note_for_drain(&self) {
        let (limit, window) = self.spec.checksum_drain();
        if limit == 0 {
            return;
        }
        // the failures inside the window, this one among them
        let now = std::time::Instant::now();
        let count = {
            let mut times = self.drain_times.lock().unwrap_or_else(PoisonError::into_inner);
            times.push_back(now);
            while times
                .front()
                .is_some_and(|first| now.duration_since(*first) > window)
            {
                times.pop_front();
            }
            times.len()
        };
        if count < limit as usize {
            return;
        }
        let mut wanted = self.drain_wanted.lock().unwrap_or_else(PoisonError::into_inner);
        if wanted.is_none() {
            let reason = format!(
                "{count} checksum failures within {}; a device whose chunks fail now and then is wearing out",
                crate::shared::scrub::Span(window.as_secs())
            );
            tracing::event!(
                tracing::Level::WARN,
                device = %self.spec.path.display(),
                id = %self.id,
                msg = "a pool device passed its drain threshold; its node asks for it to be drained",
                reason = %reason,
            );
            *wanted = Some(reason);
        }
    }

    /// Why its node asks for it to be drained, if it does (F92)
    #[must_use]
    pub fn drain_wanted(&self) -> Option<String> {
        self.drain_wanted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Where it is mounted
    #[must_use]
    pub fn path(&self) -> &std::path::Path {
        &self.spec.path
    }

    /// Mark it failed, and every slice on it with it; the first reason is kept
    ///
    /// # Arguments
    ///
    /// * `reason` - What failed
    pub fn fail(&self, reason: impl Into<String>) {
        let mut failed = self.failed.lock().unwrap_or_else(PoisonError::into_inner);
        if failed.is_none() {
            let reason = reason.into();
            tracing::event!(
                tracing::Level::ERROR,
                device = %self.spec.path.display(),
                id = %self.id,
                msg = "a pool device failed; its slices are failed with it and the node goes on",
                reason = %reason,
            );
            *failed = Some(reason);
        }
    }

    /// Why it failed, if it has
    #[must_use]
    pub fn failed(&self) -> Option<String> {
        self.failed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Its figures now, for a report
    #[must_use]
    pub fn report(&self) -> DeviceReport {
        // a device is measured as a root is
        let (size_bytes, free_bytes) =
            crate::server::control::capacity::size_and_free(&self.spec.path).unwrap_or((0, 0));
        DeviceReport {
            id: self.id,
            path: self.spec.path.to_string_lossy().into_owned(),
            class: self.spec.class.clone(),
            slices: self
                .slices
                .iter()
                .map(|slice| SliceReport {
                    id: slice.id,
                    index: slice.index,
                    chunks: slice.chunks.load(Ordering::Relaxed),
                    chunk_bytes: slice.chunk_bytes.load(Ordering::Relaxed),
                    staged_bytes: slice.staged_bytes.load(Ordering::Relaxed),
                    checksum_failures: slice.checksum_failures.load(Ordering::Relaxed),
                    quarantined: slice.quarantined.load(Ordering::Relaxed),
                    scrub_bytes: slice.scrub_bytes.load(Ordering::Relaxed),
                })
                .collect(),
            size_bytes,
            free_bytes,
            reserve_bytes: self.reserve,
            device: self.facts.device.clone(),
            filesystem: self.facts.filesystem.clone(),
            rotational: self.facts.rotational,
            write_cache: self.facts.write_cache.clone(),
            journal: self
                .journal
                .as_ref()
                .map(|journal| journal.path.to_string_lossy().into_owned()),
            warnings: self.warnings.clone(),
            health: match self.failed() {
                Some(reason) => DeviceHealth::Failed { reason },
                None => DeviceHealth::Serving,
            },
            drain_wanted: self.drain_wanted(),
        }
    }

    /// Its identity, for the pool map
    #[must_use]
    pub fn facts(&self) -> DeviceFacts {
        DeviceFacts {
            id: self.id,
            class: self.spec.class.clone(),
            slices: self.slices.iter().map(|slice| slice.id).collect(),
            size_bytes: self.facts.size_bytes,
            weight: self.spec.weight.map(|weight| weight.get()),
            write_back: self.facts.write_cache.as_deref() == Some("write back"),
            rotational: self.facts.rotational,
            journal: self.journal.as_ref().map(|journal| journal.id),
        }
    }
}

/// Every device a node claimed, the ones it serves and the ones it knows have failed
#[derive(Debug, Default)]
pub struct NodeDevices {
    /// The devices it serves
    pub devices: Vec<Arc<DeviceState>>,
    /// The devices it knows by an id and cannot serve
    pub missing: Vec<FailedDevice>,
    /// The claims, whose locks are held for as long as this is
    _claims: Option<Claimed>,
}

impl NodeDevices {
    /// Hold what a node claimed
    ///
    /// # Arguments
    ///
    /// * `claimed` - What the claim found
    #[must_use]
    pub fn new(claimed: Claimed) -> Self {
        let devices = claimed
            .devices
            .iter()
            .map(|device| {
                let state = DeviceState::new(
                    device.id,
                    device.spec.clone(),
                    device.facts.clone(),
                    device.reserve,
                    device.apply_sync,
                    device
                        .slices
                        .iter()
                        .map(|slice| (slice.id, slice.path.clone()))
                        .collect(),
                    device.warnings.clone(),
                );
                // its slices' journals where the claim found them, under it or on an SSD
                let state = match &device.journal {
                    Some(journal) => state.with_journal(journal.id, &journal.path, journal.lost),
                    None => state,
                };
                Arc::new(state)
            })
            .collect();
        NodeDevices {
            devices,
            missing: claimed.failed.clone(),
            _claims: Some(claimed),
        }
    }

    /// Whether the node has no device, served or failed
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.devices.is_empty() && self.missing.is_empty()
    }

    /// Every device's figures, the failed ones it cannot serve among them
    #[must_use]
    pub fn reports(&self) -> Vec<DeviceReport> {
        let mut reports: Vec<DeviceReport> = self.devices.iter().map(|device| device.report()).collect();
        for missing in &self.missing {
            reports.push(DeviceReport {
                id: missing.id,
                path: missing.path.to_string_lossy().into_owned(),
                class: missing.class.clone(),
                health: DeviceHealth::Failed {
                    reason: missing.reason.clone(),
                },
                ..DeviceReport::default()
            });
        }
        reports
    }

    /// The identity of every device it serves, for the pool map
    #[must_use]
    pub fn facts(&self) -> Vec<DeviceFacts> {
        self.devices.iter().map(|device| device.facts()).collect()
    }

    /// The classes of the devices whose journal was lost at this start, whose pools are scrubbed
    /// at once to find what the lost journal had staged and not applied (F91)
    #[must_use]
    pub fn lost_journal_classes(&self) -> std::collections::BTreeSet<String> {
        self.devices
            .iter()
            .filter(|device| device.journal.as_ref().is_some_and(|journal| journal.lost))
            .map(|device| device.spec.class.clone())
            .collect()
    }

    /// Whether it serves a rotational device and a solid state one, which no executor may serve
    /// together (S13, F91)
    #[must_use]
    pub fn serves_both_kinds(&self) -> bool {
        self.devices.iter().any(|device| device.rotational())
            && self.devices.iter().any(|device| !device.rotational())
    }

    /// Every device it knows to have failed, and why
    #[must_use]
    pub fn failed(&self) -> BTreeMap<DeviceId, String> {
        let mut failed: BTreeMap<DeviceId, String> = self
            .devices
            .iter()
            .filter_map(|device| device.failed().map(|reason| (device.id, reason)))
            .collect();
        for missing in &self.missing {
            failed.insert(missing.id, missing.reason.clone());
        }
        failed
    }

    /// Fail every device this node serves that an operator ordered failed, as an I/O error on it
    /// would; the node's next report says so, and that clears the order (F88)
    ///
    /// # Arguments
    ///
    /// * `orders` - Each device ordered failed, and why
    pub fn carry_out(&self, orders: &BTreeMap<DeviceId, String>) {
        for device in &self.devices {
            if let Some(reason) = orders.get(&device.id) {
                device.fail(reason.clone());
            }
        }
    }

    /// A slice it serves, and its device
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice's id
    #[must_use]
    pub fn slice(&self, slice: SliceId) -> Option<(Arc<DeviceState>, Arc<SliceState>)> {
        self.devices.iter().find_map(|device| {
            device
                .slices
                .iter()
                .find(|candidate| candidate.id == slice)
                .map(|found| (device.clone(), found.clone()))
        })
    }
}

/// The file a standalone node keeps its pool map in, at its primary root
///
/// A cluster's pool map is the control group's; a standalone node has none, so it keeps its own,
/// whose generations survive a restart as a cluster's do in its log.
pub const POOL_MAP_FILE: &str = "shoal-poolmap.json";

/// A standalone node's pool map, with what it claimed at this start recorded in it
///
/// Read from the primary root, its devices observed as a control group would observe them, every
/// pool ready on them pinned the first time it is, and written back, staged and renamed, only when
/// something changed.
///
/// # Arguments
///
/// * `root` - The primary root
/// * `node` - This node
/// * `devices` - What it claimed
/// * `pools` - The pools its file defines
///
/// # Errors
///
/// A pool map that does not read, a device the map holds on another node, or a file that cannot
/// be written.
pub fn standalone_pool_map(
    root: &std::path::Path,
    node: crate::shared::identity::NodeId,
    devices: &NodeDevices,
    pools: &BTreeMap<String, crate::shared::pools::PoolSpec>,
) -> Result<crate::shared::poolmap::PoolMap, crate::server::ServerError> {
    use crate::server::errors::ShoalError;
    use crate::server::ServerError;
    use std::io::Write as _;
    let path = root.join(POOL_MAP_FILE);
    // the map as the last start left it, or none
    let mut map: crate::shared::poolmap::PoolMap = match std::fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Default::default(),
        Err(error) => return Err(ServerError::IO(error)),
    };
    if devices.is_empty() && map.is_empty() {
        return Ok(map);
    }
    let before = map.clone();
    // a standalone node stands on its own machine
    let domains = BTreeMap::from([(
        crate::shared::domains::HOST.to_string(),
        crate::server::control::stats::hostname().unwrap_or_else(|| "localhost".to_string()),
    )]);
    map.observe(node, &domains, &devices.facts())
        .map_err(|error| ServerError::Shoal(ShoalError::InvalidConfig(error.to_string())))?;
    map.refresh_domains(node, &domains);
    map.set_failed(node, &devices.failed());
    // a pool ready on this node's devices is pinned the first time it is, as a cluster's leader
    // pins one, and places its stripes there for good: a standalone node moves no placement
    // group (F86, F88)
    for (name, pool) in pools {
        if map.pinned_at(name).is_some() {
            continue;
        }
        let ready = crate::shared::pool_placement::readiness(&map, name, pool, &BTreeMap::new(), |_| true);
        if ready.ready() {
            let generation = map.generation;
            map.pin(name, generation)
                .map_err(|error| ServerError::Shoal(ShoalError::InvalidConfig(error.to_string())))?;
        }
    }
    if map != before {
        let staged = root.join(format!("{POOL_MAP_FILE}.tmp"));
        let mut file = std::fs::File::create(&staged)?;
        file.write_all(&serde_json::to_vec_pretty(&map)?)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&staged, &path)?;
        std::fs::File::open(root)?.sync_all()?;
    }
    Ok(map)
}

#[cfg(test)]
mod ceiling_tests {
    use super::Ceiling;

    /// A ceiling lets a second's worth through at once, then makes the next request wait for what
    /// it lacks; one larger than a second's worth waits for a whole second's and takes it all (F88)
    #[test]
    fn a_ceiling_lets_its_rate_through_and_no_more() {
        let ceiling = Ceiling::new(1000);
        assert_eq!(ceiling.take(600), Ok(()));
        // 400 left: 600 more waits for about 200 bytes' worth, a fifth of a second
        let wait = ceiling.take(600).expect_err("over the ceiling");
        assert!(wait.as_secs_f64() > 0.15 && wait.as_secs_f64() <= 0.2, "{wait:?}");
        // a request past a second's worth is a second's worth
        let ceiling = Ceiling::new(1000);
        assert_eq!(ceiling.take(5000), Ok(()));
        assert!(ceiling.take(1).is_err());
    }
}

#[cfg(test)]
mod drain_tests {
    use super::{DeviceState, Facts};
    use crate::shared::devices::{ApplySync, DeviceId, DeviceSpec, SliceId};
    use crate::shared::scrub::Span;

    /// A device asks for its drain at its drain threshold, three checksum failures in a day by
    /// default, well before its failure limit fails it; one whose threshold is zero never asks, and
    /// its report says why it asks (F92)
    #[test]
    fn a_device_asks_for_its_drain_before_its_failures_fail_it() {
        let dir = std::env::temp_dir().join(format!("shoal-drain-{}", std::process::id()));
        let device = |spec: DeviceSpec| {
            DeviceState::new(
                DeviceId::mint(),
                spec,
                Facts::default(),
                0,
                ApplySync::Shared,
                vec![(SliceId::mint(), dir.join("slice-0"))],
                Vec::new(),
            )
        };
        let worn = device(DeviceSpec::new(&dir, "ssd"));
        let slice = worn.slices[0].clone();
        for _ in 0..2 {
            assert!(!worn.note_checksum_failure(&slice));
        }
        assert!(worn.drain_wanted().is_none(), "two failures asked for a drain");
        // the third asks for it, and fails nothing
        assert!(!worn.note_checksum_failure(&slice));
        let why = worn.drain_wanted().expect("three failures in a day ask for a drain");
        assert!(why.contains("3 checksum failures"), "{why}");
        assert!(worn.failed().is_none());
        assert_eq!(worn.report().drain_wanted.as_deref(), Some(why.as_str()));
        // a threshold of zero never asks
        let never = device(DeviceSpec::new(&dir, "ssd").with_checksum_drain(0, Span(60)));
        let slice = never.slices[0].clone();
        for _ in 0..5 {
            never.note_checksum_failure(&slice);
        }
        assert!(never.drain_wanted().is_none());
    }
}
