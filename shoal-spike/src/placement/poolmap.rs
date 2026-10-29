//! The pool map as a frame, sized the way `fanout` sizes the tablet map's
//!
//! S5's pool map holds pools and bindings, devices and their slices, generations, and the moves
//! in flight, and is held to the tablet map's budget: it grows with devices, slices and moves and
//! never with placement groups, stripes or objects. These types are a **sketch for sizing**,
//! not a format: the fields S4 and S5 name, in the JSON the topology frame is pushed as, with
//! ids as UUIDs because a device and a slice are minted as a node is.
//!
//! Two representations are sized that S5 leaves open:
//!
//! - **Generations as change records.** A device carries the generation it joined at and the
//!   one it left at, and a reweight is one record, so the map at any kept generation is computed
//!   by filtering. Keeping a generation costs a record, not a copy of the map
//! - **Moves as the planner's in flight, not every group between generations.** S10's planner
//!   moves one placement group a device at a time, so that list is bounded by devices. The
//!   alternative, listing every group the change moves, is sized beside it

use std::fmt::Write as _;
use std::time::Instant;

use serde::Serialize;
use shoal::uuid::Uuid;

use super::measure::pending_after_add;
use super::score::SplitMix;
use super::shape::{Layout, Shape};

/// A pool's redundancy, as the frame spells it
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Redundancy {
    /// Whole copies
    Replicas(u8),
    /// Data and parity chunks
    Erasure {
        /// Data chunks
        data: u8,
        /// Parity chunks
        parity: u8,
    },
}

/// One pool, S4's five facts and its placement groups a tablet
#[derive(Serialize)]
pub struct PoolRecord {
    /// Its name
    pub name: String,
    /// The class of device it is made of
    pub class: String,
    /// Its redundancy
    pub redundancy: Redundancy,
    /// What no two of its chunks share
    pub failure_domain: String,
    /// Losses an acknowledged write survives beyond the first
    pub f: u8,
    /// Placement groups a tablet
    pub placement_groups: u16,
    /// Bytes a stripe
    pub stripe_bytes: u64,
    /// Bytes a chunk unit
    pub unit_bytes: u64,
    /// Bytes at or under which an object stays in its row
    pub inline_bytes: u64,
}

/// One consumer bound to a pool
#[derive(Serialize)]
pub struct BindingRecord {
    /// The consumer's name, a bucket's for now
    pub consumer: String,
    /// The id minted when the binding was committed
    pub id: u64,
    /// The pool
    pub pool: String,
}

/// One slice of a device
#[derive(Serialize)]
pub struct SliceRecord {
    /// Its id
    pub id: Uuid,
    /// Its state
    pub state: String,
}

/// One device
#[derive(Serialize)]
pub struct DeviceRecord {
    /// Its id
    pub id: Uuid,
    /// The node it is on
    pub node: Uuid,
    /// Its failure domains above itself, a list so a rack is a third entry
    pub domains: Vec<String>,
    /// Its class
    pub class: String,
    /// Its size in bytes
    pub size: u64,
    /// Its weight
    pub weight: u64,
    /// The key placement scores it by
    pub seat: u64,
    /// Its state
    pub state: String,
    /// The generation it joined at
    pub since: u64,
    /// The generation it left at, kept while a placement group is still at an older one
    #[serde(skip_serializing_if = "Option::is_none")]
    pub until: Option<u64>,
    /// Its slices
    pub slices: Vec<SliceRecord>,
}

/// One change to the map that made a generation
#[derive(Serialize)]
pub struct ChangeRecord {
    /// The generation it made
    pub generation: u64,
    /// What it was
    pub kind: String,
    /// The device it changed
    pub device: Uuid,
    /// The weight it set, for a reweight
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weight: Option<u64>,
}

/// One placement group between two generations
#[derive(Serialize)]
pub struct MoveRecord {
    /// Its consumer
    pub consumer: u64,
    /// Its tablet
    pub tablet: u16,
    /// Its sub-range in the tablet
    pub sub: u16,
    /// The generation its chunks are at
    pub from: u64,
    /// The generation they are moving to
    pub to: u64,
    /// Where the move stands
    pub phase: String,
}

/// One chunk the rule's answer does not hold
#[derive(Serialize)]
pub struct ExceptionRecord {
    /// Its group's consumer
    pub consumer: u64,
    /// Its group's tablet
    pub tablet: u16,
    /// Its group's sub-range
    pub sub: u16,
    /// The position overridden
    pub position: u8,
    /// The slice that holds it instead
    pub slice: Uuid,
}

/// The pool map, as a frame
#[derive(Serialize)]
pub struct PoolMapFrame {
    /// The cluster
    pub cluster: Uuid,
    /// How many committed changes the pool map has seen
    pub version: u64,
    /// The current generation
    pub generation: u64,
    /// The pools
    pub pools: Vec<PoolRecord>,
    /// The bindings
    pub bindings: Vec<BindingRecord>,
    /// Every device, with its slices
    pub devices: Vec<DeviceRecord>,
    /// The changes since the oldest generation kept
    pub changes: Vec<ChangeRecord>,
    /// The placement groups moving
    pub moves: Vec<MoveRecord>,
    /// The rule's exceptions
    pub exceptions: Vec<ExceptionRecord>,
}

/// What a frame carries beyond the devices
#[derive(Debug, Default, Clone, Copy)]
pub struct Extras {
    /// Change records kept
    pub changes: usize,
    /// Moves listed
    pub moves: usize,
    /// Exceptions listed
    pub exceptions: usize,
}

/// A UUID from the generator, as a minted one would print
///
/// # Arguments
///
/// * `rng` - The generator
fn uuid(rng: &mut SplitMix) -> Uuid {
    Uuid::from_u128((u128::from(rng.next_u64()) << 64) | u128::from(rng.next_u64()))
}

impl PoolMapFrame {
    /// The frame of a shape, with extras
    ///
    /// # Arguments
    ///
    /// * `shape` - The shape
    /// * `extras` - What it carries beyond the devices
    #[must_use]
    pub fn of(shape: &Shape, extras: Extras) -> Self {
        let mut rng = SplitMix::new(0xf4a3_e000 ^ shape.devices.len() as u64);
        // one node a host
        let nodes: Vec<Uuid> = shape.hosts.iter().map(|_| uuid(&mut rng)).collect();
        let pools: Vec<PoolRecord> = shape
            .pools
            .iter()
            .enumerate()
            .map(|(index, pool)| PoolRecord {
                name: format!("{}{index}", pool.name),
                class: pool.class.to_string(),
                redundancy: match pool.layout {
                    Layout::Replicas(r) => Redundancy::Replicas(r as u8),
                    Layout::Erasure(k, m) => Redundancy::Erasure {
                        data: k as u8,
                        parity: m as u8,
                    },
                },
                failure_domain: pool.domain.name().to_string(),
                f: 1,
                placement_groups: 4,
                stripe_bytes: 4 << 20,
                unit_bytes: 64 << 10,
                inline_bytes: 16 << 10,
            })
            .collect();
        // a few buckets bound to the first pool
        let bindings = (0..4)
            .map(|index| BindingRecord {
                consumer: format!("bucket-{index}"),
                id: rng.next_u64(),
                pool: pools[0].name.clone(),
            })
            .collect();
        let devices: Vec<DeviceRecord> = shape
            .devices
            .iter()
            .map(|device| DeviceRecord {
                id: uuid(&mut rng),
                node: nodes[device.host as usize],
                domains: vec![format!("host:{}", shape.hosts[device.host as usize].name)],
                class: device.class.to_string(),
                size: device.gib << 30,
                weight: device.weight as u64,
                seat: device.seat,
                state: "placeable".to_string(),
                since: 1,
                until: None,
                slices: (0..device.slices)
                    .map(|_| SliceRecord {
                        id: uuid(&mut rng),
                        state: "placeable".to_string(),
                    })
                    .collect(),
            })
            .collect();
        // changes, moves and exceptions over the devices, numbered as a busy map's would be
        let generation = 1 + extras.changes as u64;
        let changes = (0..extras.changes)
            .map(|index| ChangeRecord {
                generation: 2 + index as u64,
                kind: "reweight".to_string(),
                device: devices[index % devices.len()].id,
                weight: Some(devices[index % devices.len()].weight / 2),
            })
            .collect();
        let moves = (0..extras.moves)
            .map(|_| MoveRecord {
                consumer: rng.next_u64(),
                tablet: (rng.next_u64() % 4096) as u16,
                sub: (rng.next_u64() % 4) as u16,
                from: generation.saturating_sub(1),
                to: generation,
                phase: "copying".to_string(),
            })
            .collect();
        let exceptions = (0..extras.exceptions)
            .map(|index| ExceptionRecord {
                consumer: rng.next_u64(),
                tablet: (rng.next_u64() % 4096) as u16,
                sub: (rng.next_u64() % 4) as u16,
                position: (index % 6) as u8,
                slice: devices[index % devices.len()].slices[0].id,
            })
            .collect();
        PoolMapFrame {
            cluster: uuid(&mut rng),
            version: 100 + extras.changes as u64,
            generation,
            pools,
            bindings,
            devices,
            changes,
            moves,
            exceptions,
        }
    }

    /// Its encoded bytes
    #[must_use]
    pub fn bytes(&self) -> Vec<u8> {
        shoal::serde_json::to_vec(self).expect("a pool map encodes")
    }
}

/// The median of `rounds` timings of a closure
///
/// # Arguments
///
/// * `rounds` - How many
/// * `f` - What to time
fn median_us<F: FnMut()>(rounds: usize, mut f: F) -> f64 {
    let mut timings: Vec<f64> = (0..rounds)
        .map(|_| {
            let start = Instant::now();
            f();
            start.elapsed().as_secs_f64() * 1e6
        })
        .collect();
    timings.sort_by(f64::total_cmp);
    timings[rounds / 2]
}

/// The pool map's tables for `fanout`: a frame a shape, what each record costs, and a busy map
///
/// # Arguments
///
/// * `today` - The bytes of today's tablet frame at sixty-four members and sixteen tables
#[must_use]
pub fn fanout_tables(today: usize) -> String {
    let mut out = String::new();
    let threads = std::thread::available_parallelism().map_or(1, usize::from);
    let mut shapes = Shape::all();
    shapes.extend(Shape::timed().into_iter().filter(|shape| shape.name == "50x24-slices"));
    // a frame a shape, encoded once and copied a subscriber
    let _ = writeln!(out, "## Pool map frame (X2), JSON as the topology frame is pushed\n");
    let _ = writeln!(
        out,
        "| shape | devices | slices | frame bytes | over today's tablet frame | encode µs | push to 1 µs | to 100 µs | to 1000 µs |"
    );
    let _ = writeln!(out, "| --- | --- | --- | --- | --- | --- | --- | --- | --- |");
    for shape in &shapes {
        let frame = PoolMapFrame::of(shape, Extras::default());
        let bytes = frame.bytes().len();
        // fewer rounds for a large frame, so a thousand copies of one do not take minutes
        let rounds = (200_000_000 / (bytes * 1000)).clamp(5, 200);
        let encode = median_us(rounds.max(50), || {
            std::hint::black_box(frame.bytes());
        });
        let mut pushes = Vec::new();
        for subscribers in [1usize, 100, 1000] {
            pushes.push(median_us(rounds, || {
                let json = frame.bytes();
                let copies: Vec<Vec<u8>> = (0..subscribers).map(|_| json.clone()).collect();
                std::hint::black_box(copies);
            }));
        }
        let slices: usize = frame.devices.iter().map(|device| device.slices.len()).sum();
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {:.2}× | {:.1} | {:.1} | {:.1} | {:.1} |",
            shape.name,
            frame.devices.len(),
            slices,
            bytes,
            bytes as f64 / today as f64,
            encode,
            pushes[0],
            pushes[1],
            pushes[2]
        );
    }
    out.push('\n');
    // what each record adds, read as the difference one more makes
    let lab = shapes.iter().find(|shape| shape.name == "lab-2").expect("lab-2 is a shape");
    let base = PoolMapFrame::of(lab, Extras::default()).bytes().len();
    let per = |extras: Extras, count: usize| {
        (PoolMapFrame::of(lab, extras).bytes().len() - base) as f64 / count as f64
    };
    let _ = writeln!(out, "## What one record adds\n");
    let _ = writeln!(out, "| record | bytes |");
    let _ = writeln!(out, "| --- | --- |");
    let device = {
        let mut one_more = lab.clone();
        one_more = one_more.changed(super::shape::Change::Add, "ssd");
        PoolMapFrame::of(&one_more, Extras::default()).bytes().len() as f64 - base as f64
    };
    let _ = writeln!(out, "| a device of one slice | {device:.0} |");
    let sliced = shapes.iter().find(|shape| shape.name == "6x12-slices").expect("6x12-slices is a shape");
    let plain = shapes.iter().find(|shape| shape.name == "6x12").expect("6x12 is a shape");
    let extra_slices: usize = sliced.devices.iter().map(|d| usize::from(d.slices) - 1).sum();
    let slice = (PoolMapFrame::of(sliced, Extras::default()).bytes().len() as f64
        - PoolMapFrame::of(plain, Extras::default()).bytes().len() as f64)
        / extra_slices as f64;
    let _ = writeln!(out, "| a slice beyond a device's first | {slice:.0} |");
    let _ = writeln!(out, "| a change kept | {:.0} |", per(Extras { changes: 100, ..Extras::default() }, 100));
    let _ = writeln!(out, "| a move listed | {:.0} |", per(Extras { moves: 100, ..Extras::default() }, 100));
    let _ = writeln!(out, "| an exception | {:.0} |", per(Extras { exceptions: 100, ..Extras::default() }, 100));
    out.push('\n');
    // a busy map: history kept, the planner's moves in flight, exceptions, and the alternative
    let _ = writeln!(out, "## A busy pool map\n");
    let _ = writeln!(
        out,
        "Frame bytes. `pending listed` is the alternative to keeping a generation: every placement \
         group a device added moves, at 4 groups a tablet, for one consumer and for a hundred.\n"
    );
    let _ = writeln!(
        out,
        "| shape | devices alone | 64 changes kept | a move a device in flight | 1,000 exceptions | 10,000 exceptions | pending listed, 1 consumer | pending listed, 100 consumers |"
    );
    let _ = writeln!(out, "| --- | --- | --- | --- | --- | --- | --- | --- |");
    for name in ["lab-2", "6x12", "50x24"] {
        let shape = shapes.iter().find(|shape| shape.name == name).expect("the shape exists");
        let size = |extras: Extras| PoolMapFrame::of(shape, extras).bytes().len();
        let pool = shape.pools.last().expect("a shape has a pool");
        let pending = pending_after_add(shape, pool, 4, threads) as usize;
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} | {} ({pending} groups) | {} |",
            name,
            size(Extras::default()),
            size(Extras { changes: 64, ..Extras::default() }),
            size(Extras { moves: shape.devices.len(), ..Extras::default() }),
            size(Extras { exceptions: 1000, ..Extras::default() }),
            size(Extras { exceptions: 10_000, ..Extras::default() }),
            size(Extras { moves: pending, ..Extras::default() }),
            size(Extras { moves: pending * 100, ..Extras::default() }),
        );
    }
    out.push('\n');
    out
}
