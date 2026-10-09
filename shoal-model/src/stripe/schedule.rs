//! Stripe schedules: generated from a seed, built by hand, saved, loaded and replayed
//!
//! A schedule is a list of events and the parameters of the world it runs in, saved as JSON
//! under `shoal-model/schedules/stripe/`, apart from the tablet model's: the tablet model's
//! loader reads every file directly in `schedules/` as a tablet schedule and does not descend
//! ([X1](../../../docs/src/object-storage/spikes.md#x1-the-stripe-protocol-as-a-model)).
//!
//! A generated run has two phases. Until `calm_from`, anything the failure model allows
//! happens: messages lost, duplicated and answered by replicas that lag, crashes, disks that fail,
//! fill and are swapped, moves, truncates, timeouts. From `calm_from` nothing fails: messages
//! arrive, nodes restart and repair, writers keep writing one stripe and readers keep reading it,
//! and the progress check judges what they took.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::ids::{NodeId, OpId};
use crate::invariants::Violation;
use crate::rng::SplitMix64;
use crate::stripe::actors::Driver;
use crate::stripe::event::{Body, Message, StripeEvent};
use crate::stripe::ids::{Pos, SliceId, StripeIx};
use crate::stripe::layout::Layout;
use crate::stripe::oracle::{OpKind, StripeOutcome};
use crate::stripe::policy::StripePolicy;
use crate::stripe::progress::ProgressFailure;
use crate::stripe::world::StripeWorld;

/// The schedule file format; a newer one is refused rather than misread
pub const FORMAT: u32 = 1;

/// How likely each kind of event is, against the others with something to pick
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StripeWeights {
    /// A message delivered
    pub deliver: u32,
    /// A message lost
    pub drop: u32,
    /// A message delivered again
    pub duplicate: u32,
    /// A group read answered by a replica that lags
    pub lagging: u32,
    /// A slice syncing
    pub sync: u32,
    /// A step of an apply
    pub apply: u32,
    /// A slice asking about what it staged
    pub ask: u32,
    /// A client write
    pub write: u32,
    /// A client trying again
    pub retry: u32,
    /// A truncate
    pub truncate: u32,
    /// A default read
    pub read: u32,
    /// A strong read
    pub strong_read: u32,
    /// A client giving up
    pub client_timeout: u32,
    /// A stager giving up
    pub stager_timeout: u32,
    /// A coordinator dying
    pub driver_crash: u32,
    /// A node crashing
    pub crash: u32,
    /// A node restarting
    pub restart: u32,
    /// A disk failing silently
    pub disk_fail: u32,
    /// A failed disk reported
    pub disk_report: u32,
    /// A disk swapped for an empty one
    pub disk_replace: u32,
    /// A disk filling
    pub fill: u32,
    /// A disk's space coming back
    pub free: u32,
    /// The leader's no-op
    pub noop: u32,
    /// A rebuild of a stale position
    pub rebuild: u32,
    /// A move of a position
    pub moves: u32,
    /// A reclamation
    pub reclaim: u32,
    /// A reservation lapsing
    pub lapse: u32,
    /// The leader's driver clearing a row's pending bytes
    #[serde(
        default = "default_clear_pending_bytes",
        skip_serializing_if = "is_default_clear_pending_bytes"
    )]
    pub clear_pending_bytes: u32,
}

/// The weight of a clear of pending bytes, which every schedule from before the small write was
/// modelled reads as and writes nothing of
fn default_clear_pending_bytes() -> u32 {
    2
}

/// Whether a clear's weight is the default, so a schedule file need not say it
///
/// # Arguments
///
/// * `weight` - The weight
fn is_default_clear_pending_bytes(weight: &u32) -> bool {
    *weight == default_clear_pending_bytes()
}

impl Default for StripeWeights {
    /// The weights the tests and the search use
    fn default() -> Self {
        Self {
            deliver: 40,
            drop: 2,
            duplicate: 3,
            lagging: 4,
            sync: 12,
            apply: 12,
            ask: 3,
            write: 8,
            retry: 2,
            truncate: 1,
            read: 5,
            strong_read: 4,
            client_timeout: 1,
            stager_timeout: 1,
            driver_crash: 1,
            crash: 2,
            restart: 4,
            disk_fail: 1,
            disk_report: 3,
            disk_replace: 1,
            fill: 1,
            free: 3,
            noop: 1,
            rebuild: 3,
            moves: 1,
            reclaim: 1,
            lapse: 1,
            clear_pending_bytes: default_clear_pending_bytes(),
        }
    }
}

/// The world a schedule runs in, and the bounds a generated one is held to
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StripeParams {
    /// The pool's layout
    pub layout: Layout,
    /// How many stripes the object has
    pub stripes: u8,
    /// Spare slices beside the placement group's, to move to
    pub spares: u8,
    /// How many steps a generated run takes
    pub steps: u32,
    /// The step its faults stop at; zero for no calm phase
    pub calm_from: u32,
    /// How many client operations it may start before the calm phase
    pub ops: u32,
    /// How many client operations may run at once before the calm phase
    pub max_active: u8,
    /// How many writers keep writing stripe zero at once in the calm phase
    pub calm_writers: u8,
    /// How many readers keep reading it at once
    pub calm_readers: u8,
    /// How many nodes may be down at once
    pub max_down: u8,
    /// How many disks a run may lose: the pool's `f`
    pub max_losses: u8,
    /// How far behind a replica that lags may be
    pub max_lag: u8,
    /// The steps a reader begun in the calm phase may take
    pub read_bound: u32,
    /// The steps the stagers on one stripe may take, from the first, until one commits
    pub write_bound: u32,
    /// The weights
    pub weights: StripeWeights,
}

impl StripeParams {
    /// The parameters the tests use: two stripes, a fault phase and a calm one
    ///
    /// # Arguments
    ///
    /// * `layout` - The pool's layout
    pub fn default_small(layout: Layout) -> Self {
        Self {
            layout,
            stripes: 2,
            spares: 2,
            steps: 1800,
            calm_from: 600,
            ops: 40,
            max_active: 4,
            calm_writers: 2,
            calm_readers: 2,
            max_down: 1,
            max_losses: 1,
            max_lag: 3,
            read_bound: 600,
            write_bound: 600,
            weights: StripeWeights::default(),
        }
    }

    /// The parameters a schedule built by hand runs in: no calm phase, no bounds
    ///
    /// # Arguments
    ///
    /// * `layout` - The pool's layout
    /// * `stripes` - How many stripes
    pub fn by_hand(layout: Layout, stripes: u8) -> Self {
        Self {
            steps: 0,
            calm_from: 0,
            ops: 0,
            stripes,
            ..Self::default_small(layout)
        }
    }
}

/// A saved stripe schedule
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StripeSchedule {
    /// The file format
    pub format: u32,
    /// What it is called, and its file name
    pub name: String,
    /// The schedule of S7's table it is, if one
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s7: Option<u8>,
    /// The seed that generated it; zero for one built by hand
    pub seed: u64,
    /// The world it runs in
    pub params: StripeParams,
    /// The policy it runs under
    pub policy: StripePolicy,
    /// The events
    pub events: Vec<StripeEvent>,
    /// The violation replaying it finds, if any
    pub expected: Option<Violation>,
    /// The progress bound replaying it breaks, if any
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stalled: Option<ProgressFailure>,
}

impl StripeSchedule {
    /// The schedule as pretty JSON
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("a schedule serializes")
    }

    /// A schedule from JSON
    ///
    /// # Arguments
    ///
    /// * `text` - The JSON
    pub fn from_json(text: &str) -> Result<Self, serde_json::Error> {
        let schedule: Self = serde_json::from_str(text)?;
        assert!(
            schedule.format <= FORMAT,
            "schedule format {} is newer than this model's {FORMAT}",
            schedule.format
        );
        Ok(schedule)
    }

    /// Where stripe schedules are saved
    pub fn dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("schedules")
            .join("stripe")
    }

    /// Load one schedule
    ///
    /// # Arguments
    ///
    /// * `path` - The file
    pub fn load(path: &Path) -> Self {
        let text = std::fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("reading {}: {error}", path.display()));
        Self::from_json(&text).unwrap_or_else(|error| panic!("parsing {}: {error}", path.display()))
    }

    /// Save this schedule
    ///
    /// # Arguments
    ///
    /// * `path` - The file
    pub fn save(&self, path: &Path) {
        std::fs::write(path, self.to_json() + "\n")
            .unwrap_or_else(|error| panic!("writing {}: {error}", path.display()));
    }

    /// Every saved stripe schedule, in file name order
    pub fn load_all() -> Vec<(PathBuf, StripeSchedule)> {
        let dir = Self::dir();
        let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
            .unwrap_or_else(|error| panic!("reading {}: {error}", dir.display()))
            .map(|entry| entry.expect("a directory entry").path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
            .collect();
        paths.sort();
        paths
            .into_iter()
            .map(|path| {
                let schedule = Self::load(&path);
                (path, schedule)
            })
            .collect()
    }
}

/// The events a world could take next, for the generator
#[derive(Debug, Clone, Default)]
pub struct StripeEnabled {
    /// Messages in flight
    pub deliver: Vec<Message>,
    /// Messages delivered recently, which could arrive again
    pub duplicate: Vec<Message>,
    /// Group reads in flight a lagging replica could answer
    pub lagging: Vec<Message>,
    /// Slices with stages to sync
    pub sync: Vec<SliceId>,
    /// Slices with an apply to step, and the stripe
    pub apply: Vec<(SliceId, StripeIx)>,
    /// Slices holding a staged write, and the stripe
    pub ask: Vec<(SliceId, StripeIx)>,
    /// Operations a client could try again
    pub retry: Vec<OpId>,
    /// Client operations waiting
    pub pending: Vec<OpId>,
    /// Running stagers
    pub stagers: Vec<OpId>,
    /// Running drivers of any kind
    pub drivers: Vec<OpId>,
    /// Nodes up
    pub up: Vec<NodeId>,
    /// Nodes down
    pub down: Vec<NodeId>,
    /// Up nodes whose disk failed and is not yet reported
    pub unreported: Vec<NodeId>,
    /// Full nodes
    pub full: Vec<NodeId>,
    /// Stale positions a rebuild could bring current
    pub stale: Vec<(StripeIx, Pos)>,
    /// Positions a move could take to another slice
    pub movable: Vec<(StripeIx, Pos, SliceId)>,
    /// Stripes whose leader holds a reservation
    pub reserved: Vec<StripeIx>,
    /// Stripes holding a staged write whose stager is gone
    pub orphaned: Vec<StripeIx>,
    /// Stripes holding any staged write, which a holder's question would prompt the timer for
    pub prompted: Vec<StripeIx>,
    /// Positions on a slice whose disk failed and was reported, or that is gone: a repair moves them
    pub repairs: Vec<(StripeIx, Pos, SliceId)>,
    /// Stripes whose rows hold pending bytes, which the leader's clear is for
    pub pending_rows: Vec<StripeIx>,
    /// Client operations running
    pub active_client: usize,
    /// Writes running that began in the calm phase
    pub calm_writes: usize,
    /// Reads running that began in the calm phase
    pub calm_reads: usize,
    /// The group's own drivers running: rebuilds, moves, reclamations
    pub active_group: usize,
    /// Drivers begun before the calm phase and still running, which may give up in it
    pub leftover: Vec<OpId>,
}

impl StripeWorld {
    /// The events that could happen next, by kind
    ///
    /// # Arguments
    ///
    /// * `dup_window` - How many of the latest messages may be delivered again
    pub fn enabled(&self, dup_window: usize) -> StripeEnabled {
        let mut enabled = StripeEnabled {
            deliver: self.net.in_flight.clone(),
            ..StripeEnabled::default()
        };
        // a message delivered not long ago can arrive again
        enabled.duplicate = self
            .net
            .sent
            .iter()
            .rev()
            .take(dup_window)
            .filter(|msg| !self.net.in_flight.contains(msg))
            .cloned()
            .collect();
        enabled.lagging = self
            .net
            .in_flight
            .iter()
            .filter(|msg| {
                matches!(
                    msg.body,
                    Body::RowRead { strong: false } | Body::EntryRead { strong: false }
                )
            })
            .cloned()
            .collect();
        for (id, slice) in &self.slices {
            if !self.answers(*id) {
                continue;
            }
            if !slice.unsynced.is_empty() {
                enabled.sync.push(*id);
            }
            let mut applying: Vec<StripeIx> = slice
                .committed
                .iter()
                .map(|(stripe, _)| *stripe)
                .chain(slice.applying.keys().copied())
                .collect();
            applying.sort_unstable();
            applying.dedup();
            enabled
                .apply
                .extend(applying.into_iter().map(|stripe| (*id, stripe)));
            let mut staged: Vec<StripeIx> =
                slice.staged.keys().map(|(stripe, _)| *stripe).collect();
            staged.dedup();
            enabled
                .ask
                .extend(staged.into_iter().map(|stripe| (*id, stripe)));
        }
        for (op, record) in &self.ledger.records {
            let retryable = matches!(record.kind, OpKind::Write { .. } | OpKind::Truncate { .. })
                && matches!(
                    record.outcome,
                    Some(StripeOutcome::Unknown) | Some(StripeOutcome::Refused)
                )
                && !self.drivers.contains_key(op);
            if retryable {
                enabled.retry.push(*op);
            }
            if record.outcome.is_none() && record.kind.is_client() {
                enabled.pending.push(*op);
            }
        }
        let calm_from = u64::from(self.params.calm_from);
        for (op, driver) in &self.drivers {
            enabled.drivers.push(*op);
            if matches!(driver, Driver::Write(_)) {
                enabled.stagers.push(*op);
            }
            let invoked = self
                .ledger
                .records
                .get(op)
                .map_or(0, |record| record.invoke);
            let calm = calm_from > 0 && invoked >= calm_from;
            let counted = calm_from == 0 || self.step < calm_from || calm;
            if !counted {
                enabled.leftover.push(*op);
                continue;
            }
            match driver {
                Driver::Write(_) => {
                    enabled.active_client += 1;
                    if calm {
                        enabled.calm_writes += 1;
                    }
                }
                Driver::Read(_) => {
                    enabled.active_client += 1;
                    if calm {
                        enabled.calm_reads += 1;
                    }
                }
                Driver::Truncate(_) => enabled.active_client += 1,
                Driver::Rebuild(_) | Driver::Reclaim(_) | Driver::Clear(_) => {
                    enabled.active_group += 1
                }
            }
        }
        for (id, node) in &self.nodes {
            if node.up {
                enabled.up.push(*id);
                let failed = self.disks.get(&node.disk).is_some_and(|disk| !disk.healthy);
                if failed && !node.reported {
                    enabled.unreported.push(*id);
                }
            } else {
                enabled.down.push(*id);
            }
            if self.disks.get(&node.disk).is_some_and(|disk| disk.full) {
                enabled.full.push(*id);
            }
        }
        for (stripe, group) in self.rows.iter().enumerate() {
            let stripe = StripeIx(stripe as u8);
            let (_, row) = group.latest();
            // a move goes to a slice that answers, holds no position of the stripe, and is not
            // already the target of another: one move a device at a time
            let targets: Vec<SliceId> = self
                .drivers
                .values()
                .filter_map(|driver| match driver {
                    Driver::Rebuild(mover) => mover.to,
                    _ => None,
                })
                .collect();
            let free = |id: &SliceId| {
                self.answers(*id) && !row.positions.contains(id) && !targets.contains(id)
            };
            for pos in self.layout().positions() {
                if !row.current(pos) && !row.tombstone && row.exists {
                    enabled.stale.push((stripe, pos));
                }
                for id in self.slices.keys() {
                    if free(id) {
                        enabled.movable.push((stripe, pos, *id));
                    }
                }
            }
            if self
                .reservations
                .get(&stripe)
                .is_some_and(|reservations| reservations.holder.is_some())
            {
                enabled.reserved.push(stripe);
            }
            // a staged write still undecided - staged against the row's present sequence - whose
            // stager is gone is what the leader's timer is for; one a commit has moved past is
            // excluded already, and its holder discards it on asking
            let orphaned = self.slices.values().any(|slice| {
                slice.staged.iter().any(|((s, label), staged)| {
                    *s == stripe
                        && staged.base == row.seq
                        && !self.drivers.values().any(|driver| match driver {
                            Driver::Write(stager) => stager.label == Some(*label),
                            _ => false,
                        })
                })
            });
            if orphaned {
                enabled.orphaned.push(stripe);
            }
            if self
                .slices
                .values()
                .any(|slice| slice.staged.keys().any(|(s, _)| *s == stripe))
            {
                enabled.prompted.push(stripe);
            }
            if row.pending_bytes.is_some() && !row.tombstone {
                enabled.pending_rows.push(stripe);
            }
            // a position on a reported or gone slice is moved to a spare to repair it
            for pos in self.layout().positions() {
                let slice = row.slice(pos);
                let lost = self.slices.get(&slice).is_none_or(|s| {
                    s.gone || self.nodes.get(&s.node).is_some_and(|node| node.reported)
                });
                if !lost {
                    continue;
                }
                for id in self.slices.keys() {
                    if free(id) {
                        enabled.repairs.push((stripe, pos, *id));
                    }
                }
            }
        }
        enabled
    }
}

/// The generator's kinds of event
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Category {
    Deliver,
    Drop,
    Duplicate,
    Lagging,
    Sync,
    Apply,
    Ask,
    Write,
    Retry,
    Truncate,
    Read,
    StrongRead,
    ClientTimeout,
    StagerTimeout,
    DriverCrash,
    Crash,
    Restart,
    DiskFail,
    DiskReport,
    DiskReplace,
    Fill,
    Free,
    Noop,
    Rebuild,
    Move,
    Reclaim,
    Lapse,
    ClearPendingBytes,
}

/// How many of the latest messages a duplicate is drawn from
const DUP_WINDOW: usize = 12;

/// Generate a schedule by a seeded random walk
///
/// At every step the world says what could happen, a kind is picked by weight among those with
/// a candidate, and a candidate within it by the seed. Each event is applied as it is recorded,
/// so the list replays without the seed. A drop records nothing: it is the absence of a delivery.
///
/// # Arguments
///
/// * `name` - What to call it
/// * `seed` - The seed
/// * `params` - The world and its bounds
/// * `policy` - The policy
pub fn generate(
    name: &str,
    seed: u64,
    params: &StripeParams,
    policy: StripePolicy,
) -> StripeSchedule {
    let mut rng = SplitMix64::new(seed);
    let mut world = StripeWorld::new(params, policy);
    let mut events = Vec::new();
    let mut next_op = 1u32;
    let mut client_ops = 0u32;
    let layout = params.layout;
    let data_units = layout.data_units() as u32;
    let max_len = u32::from(params.stripes) * data_units;
    for step in 0..params.steps {
        let calm = params.calm_from > 0 && step >= params.calm_from;
        let enabled = world.enabled(DUP_WINDOW);
        let w = &params.weights;
        let faults = !calm;
        let down = enabled.down.len() < usize::from(params.max_down);
        let losses = world.checker.losses < u32::from(params.max_losses);
        // before the calm phase a budget of operations, a few at once; in it, writers and
        // readers that keep stripe zero busy
        let room = enabled.active_client < usize::from(params.max_active);
        let ops_left = !calm && client_ops < params.ops && room;
        // in the calm phase clients start only once recovery has settled: every node up, every
        // failed disk reported and moved off, nothing left over from the faults
        let settled = enabled.down.is_empty()
            && enabled.unreported.is_empty()
            && enabled.repairs.is_empty()
            && enabled.leftover.is_empty();
        let write_room = if calm {
            settled && enabled.calm_writes < usize::from(params.calm_writers)
        } else {
            ops_left
        };
        let read_room = if calm {
            settled && enabled.calm_reads < usize::from(params.calm_readers)
        } else {
            ops_left
        };
        let group_room = enabled.active_group < if calm { 1 } else { 2 };
        let candidates: Vec<(Category, u32)> = [
            (Category::Deliver, w.deliver, !enabled.deliver.is_empty()),
            (
                Category::Drop,
                w.drop,
                faults && !enabled.deliver.is_empty(),
            ),
            (
                Category::Duplicate,
                w.duplicate,
                faults && !enabled.duplicate.is_empty(),
            ),
            (Category::Lagging, w.lagging, !enabled.lagging.is_empty()),
            (Category::Sync, w.sync, !enabled.sync.is_empty()),
            (Category::Apply, w.apply, !enabled.apply.is_empty()),
            (Category::Ask, w.ask, !enabled.ask.is_empty()),
            (Category::Write, w.write, write_room),
            (
                Category::Retry,
                w.retry,
                faults && !enabled.retry.is_empty(),
            ),
            (Category::Truncate, w.truncate, faults && ops_left),
            (Category::Read, w.read, read_room),
            (Category::StrongRead, w.strong_read, read_room),
            (
                Category::ClientTimeout,
                w.client_timeout,
                faults && !enabled.pending.is_empty(),
            ),
            (
                Category::StagerTimeout,
                w.stager_timeout,
                faults && !enabled.stagers.is_empty(),
            ),
            (
                Category::DriverCrash,
                w.driver_crash,
                if calm {
                    !enabled.leftover.is_empty()
                } else {
                    !enabled.drivers.is_empty()
                },
            ),
            (
                Category::Crash,
                w.crash,
                faults && down && !enabled.up.is_empty(),
            ),
            (Category::Restart, w.restart, !enabled.down.is_empty()),
            (
                Category::DiskFail,
                w.disk_fail,
                faults && losses && !enabled.up.is_empty(),
            ),
            (
                Category::DiskReport,
                w.disk_report,
                !enabled.unreported.is_empty(),
            ),
            (
                Category::DiskReplace,
                w.disk_replace,
                faults && losses && !enabled.down.is_empty(),
            ),
            (Category::Fill, w.fill, faults && !enabled.up.is_empty()),
            (Category::Free, w.free, !enabled.full.is_empty()),
            (
                Category::Noop,
                w.noop,
                if calm {
                    !enabled.orphaned.is_empty()
                } else {
                    !enabled.prompted.is_empty()
                },
            ),
            (
                Category::Rebuild,
                w.rebuild,
                group_room && !enabled.stale.is_empty(),
            ),
            (
                Category::Move,
                w.moves,
                group_room
                    && if calm {
                        !enabled.repairs.is_empty()
                    } else {
                        !enabled.movable.is_empty()
                    },
            ),
            (Category::Reclaim, w.reclaim, faults && group_room),
            (Category::Lapse, w.lapse, !enabled.reserved.is_empty()),
            // only a row holding pending bytes has any to clear, so a run that never takes the
            // small write's path never draws this
            (
                Category::ClearPendingBytes,
                w.clear_pending_bytes,
                group_room && !enabled.pending_rows.is_empty(),
            ),
        ]
        .into_iter()
        .filter(|(_, weight, possible)| *possible && *weight > 0)
        .map(|(category, weight, _)| (category, weight))
        .collect();
        if candidates.is_empty() {
            break;
        }
        let weights: Vec<u32> = candidates.iter().map(|(_, weight)| *weight).collect();
        let category = candidates[rng.weighted(&weights)].0;
        // pick one of a list by the seed
        fn pick<T: Clone>(rng: &mut SplitMix64, items: &[T]) -> T {
            items[rng.below(items.len() as u64) as usize].clone()
        }
        let event = match category {
            Category::Deliver => StripeEvent::Deliver {
                msg: pick(&mut rng, &enabled.deliver),
            },
            Category::Drop => {
                // dropped, and nothing recorded
                let msg = pick(&mut rng, &enabled.deliver);
                world.net.drop_message(&msg);
                continue;
            }
            Category::Duplicate => StripeEvent::Deliver {
                msg: pick(&mut rng, &enabled.duplicate),
            },
            Category::Lagging => StripeEvent::DeliverLagging {
                msg: pick(&mut rng, &enabled.lagging),
                lag: 1 + rng.below(u64::from(params.max_lag.max(1))) as u8,
            },
            Category::Sync => StripeEvent::Sync {
                slice: pick(&mut rng, &enabled.sync),
            },
            Category::Apply => {
                let (slice, stripe) = pick(&mut rng, &enabled.apply);
                StripeEvent::ApplyStep { slice, stripe }
            }
            Category::Ask => {
                let (slice, stripe) = pick(&mut rng, &enabled.ask);
                StripeEvent::Ask { slice, stripe }
            }
            Category::Write => {
                // in the calm phase the writers keep to stripe zero, which is written continuously
                let stripe = if calm {
                    StripeIx(0)
                } else {
                    StripeIx(rng.below(u64::from(params.stripes)) as u8)
                };
                // a run of units, the whole stripe now and then
                let first = rng.below(u64::from(data_units)) as u8;
                let len = 1 + rng.below(u64::from(data_units) - u64::from(first)) as u8;
                let units = if rng.below(4) == 0 {
                    (0..data_units as u8).collect()
                } else {
                    (first..first + len).collect()
                };
                let op = OpId(next_op);
                next_op += 1;
                client_ops += 1;
                StripeEvent::Write { op, stripe, units }
            }
            Category::Retry => StripeEvent::Retry {
                op: pick(&mut rng, &enabled.retry),
            },
            Category::Truncate => {
                // a cut at a stripe's edge, inside one, or growing the object back
                let len = rng.below(u64::from(max_len) + 1) as u32;
                let op = OpId(next_op);
                next_op += 1;
                client_ops += 1;
                StripeEvent::Truncate { op, len }
            }
            Category::Read | Category::StrongRead => {
                let stripe = if calm {
                    StripeIx(0)
                } else {
                    StripeIx(rng.below(u64::from(params.stripes)) as u8)
                };
                let op = OpId(next_op);
                next_op += 1;
                client_ops += 1;
                StripeEvent::Read {
                    op,
                    stripe,
                    strong: category == Category::StrongRead,
                }
            }
            Category::ClientTimeout => StripeEvent::ClientTimeout {
                op: pick(&mut rng, &enabled.pending),
            },
            Category::StagerTimeout => StripeEvent::StagerTimeout {
                op: pick(&mut rng, &enabled.stagers),
            },
            Category::DriverCrash => StripeEvent::DriverCrash {
                op: if calm {
                    pick(&mut rng, &enabled.leftover)
                } else {
                    pick(&mut rng, &enabled.drivers)
                },
            },
            Category::Crash => StripeEvent::Crash {
                node: pick(&mut rng, &enabled.up),
            },
            Category::Restart => StripeEvent::Restart {
                node: pick(&mut rng, &enabled.down),
            },
            Category::DiskFail => StripeEvent::DiskFail {
                node: pick(&mut rng, &enabled.up),
            },
            Category::DiskReport => StripeEvent::DiskReport {
                node: pick(&mut rng, &enabled.unreported),
            },
            Category::DiskReplace => StripeEvent::DiskReplace {
                node: pick(&mut rng, &enabled.down),
            },
            Category::Fill => StripeEvent::Fill {
                node: pick(&mut rng, &enabled.up),
            },
            Category::Free => StripeEvent::Free {
                node: pick(&mut rng, &enabled.full),
            },
            Category::Noop => {
                let stripe = if calm {
                    pick(&mut rng, &enabled.orphaned)
                } else {
                    pick(&mut rng, &enabled.prompted)
                };
                StripeEvent::Noop { stripe }
            }
            Category::Rebuild => {
                let (stripe, pos) = pick(&mut rng, &enabled.stale);
                let op = OpId(next_op);
                next_op += 1;
                StripeEvent::Rebuild { op, stripe, pos }
            }
            Category::Move => {
                let (stripe, pos, to) = if calm {
                    pick(&mut rng, &enabled.repairs)
                } else {
                    pick(&mut rng, &enabled.movable)
                };
                let op = OpId(next_op);
                next_op += 1;
                StripeEvent::Move {
                    op,
                    stripe,
                    pos,
                    to,
                }
            }
            Category::Reclaim => {
                let op = OpId(next_op);
                next_op += 1;
                StripeEvent::Reclaim { op }
            }
            Category::Lapse => StripeEvent::ReservationLapse {
                stripe: pick(&mut rng, &enabled.reserved),
            },
            Category::ClearPendingBytes => {
                let stripe = pick(&mut rng, &enabled.pending_rows);
                let op = OpId(next_op);
                next_op += 1;
                StripeEvent::ClearPendingBytes { op, stripe }
            }
        };
        let violation = world.apply(&event);
        events.push(event);
        if violation.is_some() {
            break;
        }
    }
    let mut schedule = StripeSchedule {
        format: FORMAT,
        name: name.to_string(),
        s7: None,
        seed,
        params: params.clone(),
        policy,
        events,
        expected: None,
        stalled: None,
    };
    let outcome = world.finish();
    schedule.expected = outcome.violation;
    schedule.stalled = outcome.stalled;
    schedule
}

/// A schedule written by hand, each event applied as it is recorded
#[derive(Debug, Clone)]
pub struct StripeBuilder {
    /// The world so far
    pub world: StripeWorld,
    /// The events so far
    pub events: Vec<StripeEvent>,
    /// The parameters
    pub params: StripeParams,
    /// The policy
    pub policy: StripePolicy,
}

/// How many deliveries settling a builder's world may take before it is a bug
const SETTLE_BOUND: usize = 10_000;

impl StripeBuilder {
    /// Start a schedule
    ///
    /// # Arguments
    ///
    /// * `params` - The world
    /// * `policy` - The policy
    pub fn new(params: StripeParams, policy: StripePolicy) -> Self {
        Self {
            world: StripeWorld::new(&params, policy),
            events: Vec::new(),
            params,
            policy,
        }
    }

    /// Apply and record one event
    ///
    /// # Arguments
    ///
    /// * `event` - The event
    pub fn event(&mut self, event: StripeEvent) -> &mut Self {
        self.world.apply(&event);
        self.events.push(event);
        self
    }

    /// Deliver the first message in flight that matches, if any; true if one did
    ///
    /// # Arguments
    ///
    /// * `matches` - Which message
    pub fn deliver_where(&mut self, matches: impl Fn(&Message) -> bool) -> bool {
        // a world frozen by a violation delivers nothing more
        if self.world.violation.is_some() {
            return false;
        }
        let Some(msg) = self
            .world
            .net
            .in_flight
            .iter()
            .find(|msg| matches(msg))
            .cloned()
        else {
            return false;
        };
        self.event(StripeEvent::Deliver { msg });
        true
    }

    /// Deliver every message in flight that matches, and what they send that matches, until none
    ///
    /// # Arguments
    ///
    /// * `matches` - Which messages
    pub fn deliver_all_where(&mut self, matches: impl Fn(&Message) -> bool) -> &mut Self {
        for _ in 0..SETTLE_BOUND {
            if !self.deliver_where(&matches) {
                return self;
            }
        }
        panic!("delivering never settled");
    }

    /// Drop the first message in flight that matches; nothing is recorded
    ///
    /// # Arguments
    ///
    /// * `matches` - Which message
    pub fn drop_where(&mut self, matches: impl Fn(&Message) -> bool) -> bool {
        let Some(msg) = self
            .world
            .net
            .in_flight
            .iter()
            .find(|msg| matches(msg))
            .cloned()
        else {
            return false;
        };
        self.world.net.drop_message(&msg);
        true
    }

    /// Sync every slice with something to sync
    pub fn sync_all(&mut self) -> &mut Self {
        let slices = self.world.enabled(0).sync;
        for slice in slices {
            self.event(StripeEvent::Sync { slice });
        }
        self
    }

    /// Deliver, sync and apply until nothing is left to do
    pub fn settle(&mut self) -> &mut Self {
        for _ in 0..SETTLE_BOUND {
            let enabled = self.world.enabled(0);
            if let Some(msg) = enabled.deliver.first().cloned() {
                self.event(StripeEvent::Deliver { msg });
            } else if let Some(slice) = enabled.sync.first().copied() {
                self.event(StripeEvent::Sync { slice });
            } else {
                // the first apply that fits; one that does not is taken back
                let mut stepped = false;
                for (slice, stripe) in enabled.apply {
                    let skipped = self.world.skipped;
                    self.event(StripeEvent::ApplyStep { slice, stripe });
                    if self.world.skipped > skipped {
                        self.events.pop();
                        continue;
                    }
                    stepped = true;
                    break;
                }
                if !stepped {
                    return self;
                }
            }
            if self.world.violation.is_some() {
                return self;
            }
        }
        panic!("settling never settled");
    }

    /// The schedule built, with the violation and the bound its replay finds
    ///
    /// # Arguments
    ///
    /// * `name` - What to call it
    /// * `s7` - The schedule of S7's table it is, if one
    pub fn finish(&self, name: &str, s7: Option<u8>) -> StripeSchedule {
        let mut schedule = StripeSchedule {
            format: FORMAT,
            name: name.to_string(),
            s7,
            seed: 0,
            params: self.params.clone(),
            policy: self.policy,
            events: self.events.clone(),
            expected: None,
            stalled: None,
        };
        let outcome = StripeWorld::replay(&schedule);
        schedule.expected = outcome.violation;
        schedule.stalled = outcome.stalled;
        schedule
    }
}
