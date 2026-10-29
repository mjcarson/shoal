//! The device store, driven on an executor of its own, and the object executors that own it (S6,
//! S13, F85)
//!
//! A slice stages a write durably without disturbing its chunk, applies it once its row says it
//! committed, and reads a range at a label, so a crash at any instant leaves the old bytes or the
//! new ones, verifiably ([S6](../../docs/src/object-storage/device-store.md)). The crash rows are
//! the fixture's, since a torn write ends the process (F70); these are the ones a process can
//! watch: space, checksums and the order of a read and an apply.

use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use shoal::server::devices::{DeviceState, Facts};
use shoal::server::faults::{self, Fault};
use shoal::server::objects::store::chunk::{ChunkId, BLOCK};
use shoal::server::objects::store::{ReadAnswer, Slice, SliceOptions, StageParity, StageUnits, StageWhole, StoreError};
use shoal::shared::devices::{ApplySync, DeviceId, DeviceSpec, JournalId, SliceId};
use shoal::shared::objects::Label;
use shoal::shared::poolmap::PlacementGroup;

mod utils;

/// A unit of 64 KiB
const UNIT: u32 = 64 << 10;
/// A chunk of four units
const CHUNK: u64 = 4 * UNIT as u64;

/// A device of one slice under a test directory, which reserves nothing unless told
///
/// # Arguments
///
/// * `root` - The device's directory
/// * `reserve` - The free bytes it keeps
fn device(root: &Path, reserve: u64) -> Arc<DeviceState> {
    // a rotational run stands a disk in, its cache writing through, its journal beside it (F91)
    let facts = if utils::rotational_run() {
        Facts {
            filesystem: "xfs".to_string(),
            rotational: true,
            write_cache: Some("write through".to_string()),
            ..Facts::default()
        }
    } else {
        Facts::default()
    };
    let state = DeviceState::new(
        DeviceId::mint(),
        DeviceSpec::new(root, "ssd"),
        facts,
        reserve,
        ApplySync::Shared,
        vec![(SliceId::mint(), root.join("slice-0"))],
        Vec::new(),
    );
    let state = if utils::rotational_run() {
        state.with_journal(JournalId::mint(), &utils::journal_beside(root), false)
    } else {
        state
    };
    Arc::new(state)
}

/// Open a device's one slice, keeping one file of the pool ready
///
/// # Arguments
///
/// * `device` - The device
async fn slice(device: &Arc<DeviceState>) -> Rc<Slice> {
    Slice::open(
        device.clone(),
        device.slices[0].clone(),
        SliceOptions {
            segment_bytes: 1 << 20,
            free_files: 1,
            chunk_lengths: vec![CHUNK],
            budget: None,
            keep_previous: true,
            apply_queue: None,
        },
        None,
    )
    .await
    .expect("the slice opens")
}

/// A chunk, by its object
///
/// # Arguments
///
/// * `object` - The object it is a chunk of
fn chunk(object: u128) -> ChunkId {
    ChunkId {
        consumer: 1,
        pg: PlacementGroup { tablet: 1, sub: 0 },
        object,
        stripe: 0,
        position: 0,
    }
}

/// A label
///
/// # Arguments
///
/// * `sequence` - Its sequence
fn label(sequence: u64) -> Label {
    Label {
        sequence,
        tag: sequence ^ 0x5eed,
    }
}

/// Seeded bytes
///
/// # Arguments
///
/// * `seed` - The seed
/// * `len` - How many
fn bytes(seed: u8, len: usize) -> Vec<u8> {
    // a mix of the offset, so no two units of a chunk hold the same bytes
    (0..len as u64)
        .map(|index| (shoal::shared::rendezvous::mix64(index ^ u64::from(seed) << 56) >> 24) as u8)
        .collect()
}

/// Run a test on an executor of its own
///
/// # Arguments
///
/// * `test` - The test
fn on_executor<F: std::future::Future<Output = ()> + 'static>(
    test: impl FnOnce() -> F + Send + 'static,
) {
    shoal::glommio::LocalExecutorBuilder::default()
        .spawn(move || async move { test().await })
        .expect("an executor")
        .join()
        .expect("the test ran");
}

/// Stage a whole chunk and apply it
///
/// # Arguments
///
/// * `slice` - The slice
/// * `id` - The chunk
/// * `made` - Its label
/// * `seed` - Its bytes' seed
async fn put(slice: &Rc<Slice>, id: ChunkId, made: Label, seed: u8) -> Vec<u8> {
    let data = bytes(seed, CHUNK as usize);
    slice
        .stage_whole(StageWhole {
            id,
            label: made,
            chunk_bytes: CHUNK,
            unit_bytes: UNIT,
            length: CHUNK,
            bytes: data.clone(),
        })
        .await
        .expect("staged");
    slice.apply(id, made).await.expect("applied");
    data
}

/// A device filled after a stage still applies it; a whole stage past the reserve takes a file a
/// removed chunk gave back, or is refused before any commit, since nothing is written ahead (S6,
/// F96)
#[test]
fn apply_needs_no_space() {
    use shoal::shared::protocol::peer::chunk::SealScope;
    on_executor(|| async {
        let root = utils::test_dir();
        let device = device(root.path(), 1 << 20);
        let slice = slice(&device).await;
        // a chunk, and part of it staged over it
        put(&slice, chunk(1), label(1), 1).await;
        let update = bytes(9, UNIT as usize);
        slice
            .stage_units(StageUnits {
                id: chunk(1),
                makes: label(2),
                expects: label(1),
                also: Vec::new(),
                length: CHUNK,
                units: vec![(2, update.clone())],
            })
            .await
            .expect("staged");
        // a chunk removed under its seal, whose file the pool keeps
        put(&slice, chunk(5), label(1), 5).await;
        let scope = SealScope::Object {
            consumer: chunk(5).consumer,
            pg: chunk(5).pg,
            object: chunk(5).object,
        };
        slice.seal(scope, 9);
        assert_eq!(slice.reclaim(scope, 9).await, Ok(1));
        assert_eq!(slice.free_files(), 1, "the removed chunk's file was not kept");
        // the device fills: not one byte more may be allocated
        faults::arm(root.path(), Fault::Full { budget: 0 });
        // the apply writes in place, so it needs nothing, and it applies
        slice.apply(chunk(1), label(2)).await.expect("an apply needs no space");
        let read = slice
            .read(chunk(1), 2 * u64::from(UNIT), u64::from(UNIT), label(2))
            .await
            .expect("read");
        assert_eq!(read, ReadAnswer::Bytes(update));
        // a whole chunk takes the file the removed chunk gave back, which needs no space either
        put(&slice, chunk(2), label(1), 2).await;
        assert_eq!(slice.free_files(), 0);
        // and with the pool empty and the device past its reserve, a stage, which would make a
        // fresh file, is refused before anything could commit on it
        let refused = slice
            .stage_whole(StageWhole {
                id: chunk(3),
                label: label(1),
                chunk_bytes: CHUNK,
                unit_bytes: UNIT,
                length: CHUNK,
                bytes: bytes(3, CHUNK as usize),
            })
            .await;
        assert!(matches!(refused, Err(StoreError::NoSpace { .. })), "{refused:?}");
        assert!(refused.as_ref().is_err_and(StoreError::retryable));
        assert!(device.failed().is_none(), "a full device is not a failed one");
        faults::clear(root.path());
        slice.close().await;
    });
}

/// A unit copied to another chunk or another offset fails verification (S6), and since F89 a chunk
/// found failing is set aside, its failure counted against its device
#[test]
fn unit_checksum_binds_bytes_to_their_place() {
    on_executor(|| async {
        let root = utils::test_dir();
        let device = device(root.path(), 0);
        let slice = slice(&device).await;
        let first = put(&slice, chunk(1), label(1), 1).await;
        put(&slice, chunk(2), label(1), 2).await;
        // unit 1 of the first chunk copied over unit 1 of the second, and over its own unit 3
        let unit = UNIT as usize;
        let source = slice.chunk_path(&chunk(1));
        let target = slice.chunk_path(&chunk(2));
        let copied = first[unit..2 * unit].to_vec();
        let mut other = std::fs::read(&target).expect("the second chunk");
        other[BLOCK + unit..BLOCK + 2 * unit].copy_from_slice(&copied);
        std::fs::write(&target, other).expect("written");
        let mut own = std::fs::read(&source).expect("the first chunk");
        own[BLOCK + 3 * unit..BLOCK + 4 * unit].copy_from_slice(&copied);
        std::fs::write(&source, own).expect("written");
        // the unit at its own place reads
        let read = slice
            .read(chunk(1), u64::from(UNIT), u64::from(UNIT), label(1))
            .await
            .expect("read");
        assert_eq!(read, ReadAnswer::Bytes(copied));
        // bytes that verify at their place verify nowhere else, and are never returned
        for (id, at) in [(chunk(2), 1u64), (chunk(1), 3)] {
            let read = slice
                .read(id, at * u64::from(UNIT), u64::from(UNIT), label(1))
                .await;
            assert_eq!(read, Err(StoreError::Corrupt { unit: at as u32 }), "{id:?}");
        }
        // and each chunk that failed is set aside, a failure each against the device (F89)
        assert_eq!(slice.quarantined().len(), 2);
        assert_eq!(slice.read(chunk(1), 0, CHUNK, label(1)).await, Ok(ReadAnswer::Label(None)));
        assert_eq!(device.slices[0].checksum_failures.load(std::sync::atomic::Ordering::Relaxed), 2);
        slice.close().await;
    });
}

/// A read racing an apply returns the old unit or the new one, verified, never a mix (S6)
#[test]
fn read_and_apply_never_overlap() {
    on_executor(|| async {
        let root = utils::test_dir();
        let device = device(root.path(), 0);
        let slice = slice(&device).await;
        let old = put(&slice, chunk(1), label(1), 1).await;
        // every unit of the chunk rewritten by one staged write
        let new: Vec<(u32, Vec<u8>)> = (0..4).map(|unit| (unit, bytes(100 + unit as u8, UNIT as usize))).collect();
        slice
            .stage_units(StageUnits {
                id: chunk(1),
                makes: label(2),
                expects: label(1),
                also: Vec::new(),
                length: CHUNK,
                units: new.clone(),
            })
            .await
            .expect("staged");
        let new_bytes: Vec<u8> = new.iter().flat_map(|(_, bytes)| bytes.clone()).collect();
        // readers of both labels, racing the apply on the one executor
        let mut readers = Vec::new();
        for round in 0..24u64 {
            let slice = slice.clone();
            let want = if round % 2 == 0 { label(1) } else { label(2) };
            readers.push(shoal::glommio::spawn_local(async move {
                (want, slice.read(chunk(1), 0, CHUNK, want).await)
            }));
        }
        let applier = {
            let slice = slice.clone();
            shoal::glommio::spawn_local(async move { slice.apply(chunk(1), label(2)).await })
        };
        applier.await.expect("applied");
        for reader in readers {
            let (want, read) = reader.await;
            match read.expect("read") {
                // the new label always reads whole, from its record or from the chunk
                ReadAnswer::Bytes(bytes) if want == label(2) => assert_eq!(bytes, new_bytes),
                // the old label reads whole before the apply, and after it is not made
                ReadAnswer::Bytes(bytes) => assert_eq!(bytes, old),
                ReadAnswer::Label(have) => {
                    assert_eq!(want, label(1));
                    assert_eq!(have, Some(label(2)));
                }
            }
        }
        slice.close().await;
    });
}

/// The schema the node-level tests here serve: one table, and no bucket
mod plain {
    use deepsize2::DeepSizeOf;
    use rkyv::{Archive, Deserialize, Serialize};
    use shoal::storage::FileSystem;
    use shoal::tables::PersistentUnsortedTable;
    use shoal_derive::{db, ShoalUnsortedTable};

    /// A note, keyed by its id
    #[derive(
        Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, DeepSizeOf,
    )]
    #[rkyv(derive(Debug))]
    #[shoal_table(db = "Notes")]
    pub struct Note {
        /// Its key
        #[shoal(partition)]
        pub id: u64,
        /// What it says
        #[shoal(update)]
        pub text: String,
    }

    /// The database every node here serves
    #[db]
    pub struct Notes {
        /// The notes
        pub notes: PersistentUnsortedTable<Note, FileSystem>,
    }
}

/// Start a standalone node with devices, its object executors on the highest cpus
///
/// # Arguments
///
/// * `dir` - Its storage
/// * `devices` - Its devices
/// * `executors` - How many object executors
/// * `tune` - Anything else to set
fn node(
    dir: &tempfile::TempDir,
    devices: &[DeviceSpec],
    executors: usize,
    tune: impl FnOnce(shoal::server::conf::Conf) -> shoal::server::conf::Conf,
) -> shoal::ShoalPool<plain::Notes> {
    let mut conf = utils::with_devices(utils::build_config(dir), devices);
    conf.resources = conf.resources.object_cpus(utils::object_cpus(executors));
    let mut pool = shoal::ShoalPool::<plain::Notes>::start(tune(conf)).expect("the node starts");
    pool.ready(std::time::Duration::from_secs(30)).expect("the node answers");
    pool
}

/// A whole chunk of seeded bytes to stage
///
/// # Arguments
///
/// * `object` - The object it is a chunk of
/// * `made` - Its label
/// * `chunk_bytes` - Its length
fn whole(object: u128, made: Label, chunk_bytes: u64) -> StageWhole {
    StageWhole {
        id: chunk(object),
        label: made,
        chunk_bytes,
        unit_bytes: UNIT,
        length: chunk_bytes,
        bytes: bytes(object as u8, chunk_bytes as usize),
    }
}

/// Every read, stage and apply of a slice runs on the executor recorded as its owner (S13)
#[test]
fn a_slice_is_served_by_one_executor() {
    let _facts = utils::device_facts(&[]);
    let dir = utils::test_dir();
    let first = utils::test_dir();
    let second = utils::test_dir();
    // a disk has one slice, so a rotational run cuts the first device into one (F91)
    let slices = if utils::rotational_run() { 1 } else { 2 };
    let pool = node(
        &dir,
        &[
            DeviceSpec::new(first.path(), "ssd").slices(slices),
            DeviceSpec::new(second.path(), "ssd"),
        ],
        2,
        |conf| conf,
    );
    let store = pool.object_store().expect("the node has object executors").clone();
    let owners = store.owners();
    assert_eq!(owners.len(), usize::from(slices) + 1);
    // a device's two slices are driven from two executors
    if slices == 2 {
        let first_device = &pool.devices()[0];
        let executors: std::collections::BTreeSet<usize> = first_device
            .slices
            .iter()
            .map(|slice| owners[&slice.id].executor)
            .collect();
        assert_eq!(executors.len(), 2, "{owners:?}");
    } else {
        println!("a rotational run: a disk has one slice, so a device's two slices on two executors is not asked");
    }
    // every operation on every slice
    futures::executor::block_on(async {
        for (index, slice) in owners.keys().enumerate() {
            let object = index as u128 + 1;
            store.stage_whole(*slice, whole(object, label(1), CHUNK)).await.expect("staged");
            store.apply(*slice, chunk(object), label(1)).await.expect("applied");
            store
                .stage_units(
                    *slice,
                    StageUnits {
                        id: chunk(object),
                        makes: label(2),
                        expects: label(1),
                        also: Vec::new(),
                        length: CHUNK,
                        units: vec![(1, bytes(9, UNIT as usize))],
                    },
                )
                .await
                .expect("staged");
            let read = store.read(*slice, chunk(object), 0, CHUNK, label(2)).await.expect("read");
            assert!(matches!(read, ReadAnswer::Bytes(_)));
            store.apply(*slice, chunk(object), label(2)).await.expect("applied");
        }
    });
    // and each ran on its owner's cpu and no other
    for (slice, cpus) in store.served_on() {
        let owner = owners[&slice];
        assert_eq!(cpus, std::collections::BTreeSet::from([owner.cpu]), "slice {slice} ran on {cpus:?}");
    }
    // a slice this node does not serve is refused by name
    let stranger = SliceId::mint();
    let refused = futures::executor::block_on(store.apply(stranger, chunk(1), label(1)));
    assert_eq!(refused, Err(StoreError::UnknownSlice(stranger)));
    pool.exit().expect("the node stops");
}

/// S10's `rebuild_waits_for_the_slices_idle_time`. A rebuild's request is a background one: sent
/// while a foreground request is in flight on its slice it is not served, and served at once when
/// that one is done; and a run of them moves no more than its device's recovery ceiling allows - a
/// second's worth at once, then the rest at the ceiling's rate - every byte counted (F88)
#[test]
fn rebuild_waits_for_the_slices_idle_time() {
    use shoal::server::objects::Holder;
    use shoal::shared::protocol::peer::chunk::{ChunkReply, ChunkRequest};
    let _facts = utils::device_facts(&[]);
    let dir = utils::test_dir();
    let disk = utils::test_dir();
    // a ceiling of a megabyte a second
    let pool = node(&dir, &[DeviceSpec::new(disk.path(), "ssd").with_recovery_ceiling(1 << 20)], 1, |conf| conf);
    let store = pool.object_store().expect("the node has object executors").clone();
    let (_, slice) = pool
        .device_slice(pool.devices()[0].slices[0].id)
        .expect("the node serves its slice");
    futures::executor::block_on(async {
        store.stage_whole(slice.id, whole(1, label(1), CHUNK)).await.expect("staged");
        store.apply(slice.id, chunk(1), label(1)).await.expect("applied");
    });
    let holder = Holder {
        node: store.node(),
        slice: slice.id,
    };
    let read = || {
        ChunkRequest::Background(Box::new(ChunkRequest::Read {
            chunk: chunk(1),
            offset: 0,
            len: CHUNK,
            labels: vec![label(1)],
        }))
    };
    // a foreground request in flight on the slice holds a background one back
    let held = slice.foreground();
    let (done, answered) = std::sync::mpsc::channel();
    {
        let store = store.clone();
        let request = read();
        std::thread::spawn(move || {
            let reply = futures::executor::block_on(store.ask_holder(holder, request));
            let _ = done.send(reply);
        });
    }
    assert!(
        answered.recv_timeout(std::time::Duration::from_millis(500)).is_err(),
        "a background read was served while a foreground request was in flight"
    );
    drop(held);
    let reply = answered
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("the background read was never served once the slice was idle");
    assert!(matches!(reply, ChunkReply::Bytes { .. }), "{reply:?}");
    // eight more at a quarter of a megabyte each: three left of the second's worth, the rest at
    // a megabyte a second, so not before about a second and a quarter
    let started = std::time::Instant::now();
    futures::executor::block_on(async {
        for _ in 0..8 {
            assert!(matches!(store.ask_holder(holder, read()).await, ChunkReply::Bytes { .. }));
        }
    });
    let took = started.elapsed();
    assert!(took >= std::time::Duration::from_millis(1_100), "eight background reads took {took:?} under a ceiling of 1 MiB/s");
    assert_eq!(store.background_bytes(), 9 * CHUNK, "the background bytes were not all counted");
    pool.exit().expect("the node stops");
}

/// An operation that cannot draw its buffers is refused with a code its caller can retry on, and
/// nothing grows (S13)
#[test]
fn object_work_past_its_budget_is_shed_by_name() {
    let _facts = utils::device_facts(&[]);
    let dir = utils::test_dir();
    let device = utils::test_dir();
    // a budget of 1 MiB, which a chunk of 4 MiB never fits
    let pool = node(&dir, &[DeviceSpec::new(device.path(), "ssd")], 1, |mut conf| {
        conf.resources = conf.resources.object_memory("1MiB").expect("a size");
        conf
    });
    let store = pool.object_store().expect("object executors").clone();
    let slice = *store.owners().keys().next().expect("a slice");
    futures::executor::block_on(async {
        let refused = store.stage_whole(slice, whole(1, label(1), 4 << 20)).await;
        assert!(
            matches!(refused, Err(StoreError::Budget { needed, limit }) if needed == 4 << 20 && limit == 1 << 20),
            "{refused:?}"
        );
        assert!(refused.as_ref().is_err_and(StoreError::retryable));
        // a chunk inside it is served
        store.stage_whole(slice, whole(2, label(1), CHUNK)).await.expect("staged");
        // and a read past it is shed by name too
        let read = store.read(slice, chunk(2), 0, 8 << 20, label(1)).await;
        assert!(matches!(read, Err(StoreError::Budget { .. })), "{read:?}");
    });
    // nothing is held once every operation is answered, and the sheds were counted
    let (used, limit, shed) = store.budgets()[0];
    assert_eq!((used, limit), (0, 1 << 20));
    assert_eq!(shed, 2);
    pool.exit().expect("the node stops");
}

/// Under a load of stages and reads inside its budget, a shard's resident rows do not shrink (S13)
#[test]
fn object_buffers_never_evict_rows() {
    let _facts = utils::device_facts(&[]);
    let dir = utils::test_dir();
    let device = utils::test_dir();
    // the node's memory, out of which the object budget is carved before the rows' is
    let pool = node(&dir, &[DeviceSpec::new(device.path(), "ssd")], 1, |mut conf| {
        conf.resources = conf
            .resources
            .node_memory("2GiB")
            .expect("a size")
            .object_memory("64MiB")
            .expect("a size");
        conf
    });
    // the rows' share is what is left once object work's is taken out, the cache of read units the
    // node keeps unless it names one among it (F107)
    let shards = 2;
    let reserved = (64usize << 20) + shoal::server::conf::OBJECT_IO_MEMORY + (2usize << 30) / 8;
    let mut resources = shoal::server::conf::Resources::default()
        .memory("1GiB")
        .expect("a size")
        .node_memory("2GiB")
        .expect("a size")
        .object_cpus(vec![1]);
    resources = resources.object_memory("64MiB").expect("a size");
    resources.settle_object_cache(true);
    assert_eq!(resources.shard_budget(shards), ((2usize << 30) - reserved) / shards);
    // rows resident in the table, then a load of object work that holds its whole budget
    let addr = pool.bound_addr();
    let evicted = shoal::server::shard::EVICTED_PARTITIONS.load(std::sync::atomic::Ordering::Relaxed);
    let runtime = tokio::runtime::Runtime::new().expect("a runtime");
    runtime.block_on(async {
        let client = shoal::client::Shoal::<plain::NotesClient>::new(addr.to_string())
            .await
            .expect("a client");
        let mut inserts = client.query();
        for id in 0..2000u64 {
            inserts = inserts.add(plain::Note { id, text: "x".repeat(512) });
        }
        let mut answers = client.send(inserts).await.expect("inserted");
        while let Some(answer) = answers.next().await.expect("answered") {
            assert!(answer.error().is_none(), "an insert failed");
        }
    });
    let store = pool.object_store().expect("object executors").clone();
    let slice = *store.owners().keys().next().expect("a slice");
    futures::executor::block_on(async {
        for round in 0..32u128 {
            store.stage_whole(slice, whole(round + 1, label(1), CHUNK)).await.expect("staged");
            store.apply(slice, chunk(round + 1), label(1)).await.expect("applied");
            let read = store.read(slice, chunk(round + 1), 0, CHUNK, label(1)).await.expect("read");
            assert!(matches!(read, ReadAnswer::Bytes(_)));
        }
    });
    // no shard evicted a row for bytes that are not rows
    let after = shoal::server::shard::EVICTED_PARTITIONS.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(after, evicted, "object work had a shard evict rows");
    pool.exit().expect("the node stops");
}

/// A scrub's verification of a whole chunk, as the deep scrub's driver asks it
///
/// # Arguments
///
/// * `object` - The chunk's object
/// * `made` - The label it should be at
fn scrub_verify(object: u128, made: Label) -> shoal::shared::protocol::peer::chunk::ChunkRequest {
    use shoal::shared::protocol::peer::chunk::ChunkRequest;
    ChunkRequest::Scrub(Box::new(ChunkRequest::Verify {
        chunk: chunk(object),
        label: made,
        offset: 0,
        len: CHUNK,
    }))
}

/// S11's `scrub_waits_for_the_slices_idle_time`. A scrub's request is not served while a foreground
/// request is in flight on its slice, nor while a rebuild's or a move's is - a scrub yields to
/// recovery, and recovery to the foreground - and is served at once when they are done; and
/// however many are sent at once, a slice serves one at a time (X12, F89)
#[test]
fn scrub_waits_for_the_slices_idle_time() {
    use shoal::server::objects::Holder;
    use shoal::shared::protocol::peer::chunk::ChunkReply;
    let _facts = utils::device_facts(&[]);
    let dir = utils::test_dir();
    let disk = utils::test_dir();
    let pool = node(&dir, &[DeviceSpec::new(disk.path(), "ssd")], 1, |conf| conf);
    let store = pool.object_store().expect("the node has object executors").clone();
    let (_, slice) = pool
        .device_slice(pool.devices()[0].slices[0].id)
        .expect("the node serves its slice");
    futures::executor::block_on(async {
        for object in 1..=8 {
            store.stage_whole(slice.id, whole(object, label(1), CHUNK)).await.expect("staged");
            store.apply(slice.id, chunk(object), label(1)).await.expect("applied");
        }
    });
    let holder = Holder {
        node: store.node(),
        slice: slice.id,
    };
    // a request sent from a thread of its own, answered on a channel
    let send = |object: u128| {
        let (done, answered) = std::sync::mpsc::channel();
        let store = store.clone();
        std::thread::spawn(move || {
            let reply = futures::executor::block_on(store.ask_holder(holder, scrub_verify(object, label(1))));
            let _ = done.send(reply);
        });
        answered
    };
    // held back by a foreground request in flight, then served
    let held = slice.foreground();
    let answered = send(1);
    assert!(
        answered.recv_timeout(std::time::Duration::from_millis(500)).is_err(),
        "a scrub was served while a foreground request was in flight"
    );
    drop(held);
    let reply = answered
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("the scrub was never served once the slice was idle");
    assert!(matches!(&reply, ChunkReply::Verified { label: at, crcs } if *at == label(1) && crcs.len() == 4), "{reply:?}");
    // held back by a rebuild's request in flight, then served
    let recovering = slice.recovering();
    let answered = send(2);
    assert!(
        answered.recv_timeout(std::time::Duration::from_millis(500)).is_err(),
        "a scrub was served while a rebuild's request was in flight"
    );
    drop(recovering);
    let reply = answered
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("the scrub was never served once recovery was done");
    assert!(matches!(reply, ChunkReply::Verified { .. }), "{reply:?}");
    // eight at once are all served, one at a time
    let waiting: Vec<_> = (1..=8).map(send).collect();
    for answered in waiting {
        let reply = answered.recv_timeout(std::time::Duration::from_secs(10)).expect("served");
        assert!(matches!(reply, ChunkReply::Verified { .. }), "{reply:?}");
    }
    assert_eq!(store.scrub_figures().scrub_peak, 1, "a slice served more than one scrub piece at once");
    // and every byte read was a whole chunk verified, ten of them
    assert_eq!(store.scrub_figures().scrub_bytes, 10 * CHUNK);
    pool.exit().expect("the node stops");
}

/// S11's `scrub_stays_inside_its_byte_budget`. A scrub's reads draw on the recovery ceiling of its
/// device: sampled through a run of them, the bytes read over any window are at most a second's
/// worth more than the ceiling allows over it, the one second's the ceiling starts with (F89)
#[test]
fn scrub_stays_inside_its_byte_budget() {
    use shoal::server::objects::Holder;
    use shoal::shared::protocol::peer::chunk::ChunkReply;
    let _facts = utils::device_facts(&[]);
    let dir = utils::test_dir();
    let disk = utils::test_dir();
    // a ceiling of a megabyte a second, and twelve chunks of a quarter of one each
    let rate = 1u64 << 20;
    let pool = node(&dir, &[DeviceSpec::new(disk.path(), "ssd").with_recovery_ceiling(rate)], 1, |conf| conf);
    let store = pool.object_store().expect("the node has object executors").clone();
    let (_, slice) = pool
        .device_slice(pool.devices()[0].slices[0].id)
        .expect("the node serves its slice");
    futures::executor::block_on(async {
        for object in 1..=12 {
            store.stage_whole(slice.id, whole(object, label(1), CHUNK)).await.expect("staged");
            store.apply(slice.id, chunk(object), label(1)).await.expect("applied");
        }
    });
    let holder = Holder {
        node: store.node(),
        slice: slice.id,
    };
    // the bytes scrubbed, sampled every 20 ms while the scrub runs
    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let sampler = {
        let store = store.clone();
        let running = running.clone();
        std::thread::spawn(move || {
            let start = std::time::Instant::now();
            let mut samples = Vec::new();
            while running.load(std::sync::atomic::Ordering::Acquire) {
                samples.push((start.elapsed(), store.scrub_figures().scrub_bytes));
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            samples.push((start.elapsed(), store.scrub_figures().scrub_bytes));
            samples
        })
    };
    let started = std::time::Instant::now();
    futures::executor::block_on(async {
        for object in 1..=12 {
            let reply = store.ask_holder(holder, scrub_verify(object, label(1))).await;
            assert!(matches!(reply, ChunkReply::Verified { .. }), "{reply:?}");
        }
    });
    let took = started.elapsed();
    running.store(false, std::sync::atomic::Ordering::Release);
    let samples = sampler.join().expect("the sampler ran");
    // three megabytes, a second's worth at once and two more at a megabyte a second
    assert!(took >= std::time::Duration::from_millis(1_800), "three megabytes were scrubbed in {took:?} under a ceiling of 1 MiB/s");
    assert_eq!(store.scrub_figures().scrub_bytes, 12 * CHUNK);
    // no window read more than the ceiling over it and the second's worth it starts with, a piece
    // read after its draw counted where it lands
    for (at, (from, before)) in samples.iter().enumerate() {
        for (to, after) in &samples[at + 1..] {
            let window = (*to - *from).as_secs_f64();
            let allowed = (rate as f64 * (window + 1.0)) as u64 + CHUNK;
            assert!(
                after - before <= allowed,
                "{} bytes scrubbed over {window:.3}s against a budget of {allowed}",
                after - before
            );
        }
    }
    pool.exit().expect("the node stops");
}

/// S11's `scrub_never_launders`. A unit damaged on disk keeps the checksum it was written with
/// whatever is written around it: a write in place of another unit and a small write's fold leave
/// it failing, a verification finds it and sets the chunk aside, and the chunk set aside is removed
/// only once a replacement placed at its id verifies - one that does not is set aside beside it
/// (P15, F89)
#[test]
fn scrub_never_launders() {
    use shoal::server::objects::store::chunk::{bound_crc, ChunkHeader};
    use shoal::server::objects::store::Verdict;
    on_executor(|| async {
        let root = utils::test_dir();
        let device = device(root.path(), 0);
        let slice = slice(&device).await;
        let id = chunk(1);
        let unit = UNIT as usize;
        let mut data = put(&slice, id, label(1), 1).await;
        // a bit of unit 1 flipped on disk, its header untouched
        let path = slice.chunk_path(&id);
        let mut file = std::fs::read(&path).expect("the chunk");
        file[BLOCK + unit + 10] ^= 1 << 3;
        std::fs::write(&path, &file).expect("written");
        let written_crc = bound_crc(shoal::shared::checksum::crc64(&data[unit..2 * unit]), &id, 1);
        // a write in place of unit 2, and a small write's fold of unit 3, over it
        let second = bytes(7, unit);
        slice
            .stage_units(StageUnits {
                id,
                makes: label(2),
                expects: label(1),
                also: Vec::new(),
                length: CHUNK,
                units: vec![(2, second.clone())],
            })
            .await
            .expect("staged");
        slice.apply(id, label(2)).await.expect("applied");
        let third = bytes(8, unit);
        slice
            .fold(id, label(3), vec![label(2)], CHUNK, vec![(3, third.clone())])
            .await
            .expect("folded");
        let _ = slice.apply(id, label(3)).await;
        data[2 * unit..3 * unit].copy_from_slice(&second);
        data[3 * unit..4 * unit].copy_from_slice(&third);
        // unit 1 still fails, its checksum the one it was written with, and the units around it
        // carry their own
        let header = ChunkHeader::decode(&std::fs::read(&path).expect("the chunk")[..BLOCK]).expect("a header");
        assert_eq!(header.label, label(3));
        assert_eq!(header.crcs[1], written_crc, "unit 1's checksum was computed again over its damaged bytes");
        assert_eq!(header.crcs[3], bound_crc(shoal::shared::checksum::crc64(&third), &id, 3));
        // a verification of units 0 to 1 finds it, and sets the chunk aside as evidence
        let verdict = slice.verify(id, label(3), 0, 2 * u64::from(UNIT)).await.expect("verified");
        assert_eq!(verdict, Verdict::Corrupt { label: label(3), units: vec![1] });
        assert!(!path.exists(), "a corrupt chunk was left where a read finds it");
        assert_eq!(slice.quarantined(), vec![(id, label(3))]);
        assert_eq!(device.slices[0].checksum_failures.load(std::sync::atomic::Ordering::Relaxed), 1);
        // a read is offered nothing, and a verification says it was found before
        assert_eq!(slice.read(id, 0, CHUNK, label(3)).await, Ok(ReadAnswer::Label(None)));
        assert_eq!(
            slice.verify(id, label(3), 0, CHUNK).await,
            Ok(Verdict::Corrupt { label: label(3), units: Vec::new() })
        );
        // a replacement whose units do not verify is set aside beside it, never taken for one
        let mut wrong = slice
            .begin_whole(id, label(3), CHUNK, UNIT, CHUNK, true)
            .await
            .expect("begun");
        wrong.piece(0, &[1, 2, 3, 4], &data).await.expect("written");
        wrong.finish(true).await.expect("placed");
        assert_eq!(slice.quarantined().len(), 2, "a replacement that does not verify was kept as current");
        assert!(!path.exists());
        // and one that verifies takes the evidence away with it
        slice
            .place_whole(StageWhole {
                id,
                label: label(3),
                chunk_bytes: CHUNK,
                unit_bytes: UNIT,
                length: CHUNK,
                bytes: data.clone(),
            })
            .await
            .expect("placed");
        assert!(slice.quarantined().is_empty(), "the evidence outlived a replacement that verifies");
        assert_eq!(device.slices[0].quarantined.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(slice.read(id, 0, CHUNK, label(3)).await, Ok(ReadAnswer::Bytes(data)));
        assert_eq!(device.slices[0].checksum_failures.load(std::sync::atomic::Ordering::Relaxed), 2);
        slice.close().await;
    });
}

/// A parity chunk folds a change to a data chunk into its own parity at the label it expects and
/// stages the new values, never the change: staged twice it is the same parity, applied it reads as
/// a fresh fold gives, and a stage expecting a label the chunk is not at is refused (S8, F90)
#[test]
fn a_parity_stage_folds_its_change_and_refuses_another_base() {
    on_executor(|| async {
        let root = utils::test_dir();
        let device = device(root.path(), 0);
        let slice = slice(&device).await;
        // a 4+2 stripe's first parity chunk, at label one
        let mut id = chunk(7);
        id.position = 4;
        let old = put(&slice, id, label(1), 7).await;
        // data chunk one's unit two changed: the change, old XOR new
        let change = bytes(8, UNIT as usize);
        let stage = || StageParity {
            id,
            makes: label(2),
            expects: label(1),
            length: CHUNK,
            data: 4,
            parity: 2,
            changes: vec![(1, vec![(2, change.clone())])],
        };
        slice.stage_parity(stage()).await.expect("staged");
        // the same stage again is staged already: a change folded twice is no parity at all
        slice.stage_parity(stage()).await.expect("staged again");
        slice.apply(id, label(2)).await.expect("applied");
        // unit two is the old parity with the change folded in, the rest as they were
        let mut folded = old[2 * UNIT as usize..3 * UNIT as usize].to_vec();
        shoal::server::objects::codec::fold_now(4, 2, 0, 1, &change, &mut folded).expect("a fold");
        let read = slice.read(id, 0, CHUNK, label(2)).await.expect("read");
        let mut expected = old.clone();
        expected[2 * UNIT as usize..3 * UNIT as usize].copy_from_slice(&folded);
        assert_eq!(read, ReadAnswer::Bytes(expected));
        // a stage computed against a label the chunk is not at is refused
        let refused = slice
            .stage_parity(StageParity {
                makes: label(4),
                expects: label(3),
                ..stage()
            })
            .await;
        assert!(matches!(refused, Err(StoreError::CannotMake { .. })), "{refused:?}");
        // and a change of a data chunk past k, or on a data position, is no parity stage
        let mut data_chunk = id;
        data_chunk.position = 1;
        let wrong = slice
            .stage_parity(StageParity {
                id: data_chunk,
                makes: label(3),
                expects: label(2),
                ..stage()
            })
            .await;
        assert!(matches!(wrong, Err(StoreError::Invalid(_))), "{wrong:?}");
    });
}


/// S13's `rotational_applies_are_batched_and_bounded`. On a device the kernel calls rotational,
/// committed applies are issued a whole batch at a time, every apply of a batch in flight at once,
/// each batch bounded by its time, and a foreground read waits for no more than one batch; a
/// solid state slice beside it never batches (X7, F91)
#[test]
fn rotational_applies_are_batched_and_bounded() {
    use shoal::server::objects::Holder;
    use shoal::shared::devices::Millis;
    use shoal::shared::protocol::peer::chunk::{ChunkReply, ChunkRequest};
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};
    let dir = utils::test_dir();
    let disk = utils::test_dir();
    let journal = utils::test_dir();
    let ssd = utils::test_dir();
    let _facts = utils::disk_facts(&[disk.path().to_path_buf()], Some("write through"));
    // a batch bounded at 50 ms, and an arm that takes 10 ms for each apply and each read
    let bound = Duration::from_millis(50);
    let arm = Duration::from_millis(10);
    let pool = node(
        &dir,
        &[
            DeviceSpec::new(disk.path(), "hdd").journal(journal.path()).apply_batch(Millis(50)),
            DeviceSpec::new(ssd.path(), "ssd"),
        ],
        2,
        |conf| conf,
    );
    let store = pool.object_store().expect("the node has object executors").clone();
    let devices = pool.devices();
    let disk_report = devices.iter().find(|device| device.class == "hdd").expect("the disk");
    let (_, slice) = pool.device_slice(disk_report.slices[0].id).expect("the disk's slice");
    let holder = Holder {
        node: store.node(),
        slice: slice.id,
    };
    // sixty-four chunks on the disk, and a unit of each staged over them
    const CHUNKS: u128 = 64;
    futures::executor::block_on(async {
        for object in 1..=CHUNKS {
            store.stage_whole(slice.id, whole(object, label(1), CHUNK)).await.expect("staged");
            store.apply(slice.id, chunk(object), label(1)).await.expect("applied");
            let units = StageUnits {
                id: chunk(object),
                makes: label(2),
                expects: label(1),
                also: Vec::new(),
                length: CHUNK,
                units: vec![(1, bytes(object as u8, UNIT as usize))],
            };
            store.stage_units(slice.id, units).await.expect("staged");
        }
    });
    let issued_before = slice.apply_batches.load(Ordering::Relaxed);
    // every apply at once, as a write's holders are told, and a read arriving among them
    shoal::server::objects::faults::slow_arm(arm);
    let reader = {
        let store = store.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            let started = Instant::now();
            let request = ChunkRequest::Read {
                chunk: chunk(CHUNKS),
                offset: 0,
                len: CHUNK,
                labels: vec![label(2), label(1)],
            };
            let reply = futures::executor::block_on(store.ask_holder(holder, request));
            (started.elapsed(), reply)
        })
    };
    let replies = futures::executor::block_on(futures::future::join_all((1..=CHUNKS).map(|object| {
        store.ask_holder(
            holder,
            ChunkRequest::Apply {
                chunk: chunk(object),
                label: label(2),
            },
        )
    })));
    let (read_took, read) = reader.join().expect("the reader finished");
    shoal::server::objects::faults::slow_arm(Duration::ZERO);
    assert!(replies.iter().all(|reply| matches!(reply, ChunkReply::Done)), "{replies:?}");
    assert!(matches!(read, ChunkReply::Bytes { .. }), "{read:?}");
    // batched: several applies a batch, each batch whole in flight at once
    let batches = slice.apply_batches.load(Ordering::Relaxed) - issued_before;
    let peak = slice.apply_batch_peak.load(Ordering::Relaxed);
    let inflight = slice.apply_inflight_peak.load(Ordering::Relaxed);
    let longest = Duration::from_micros(slice.apply_batch_longest_us.load(Ordering::Relaxed));
    println!("{batches} batches, at most {peak} applies a batch, {inflight} at once, the longest {longest:?}; a read took {read_took:?}");
    assert!(peak >= 2, "no batch held more than one apply");
    assert_eq!(inflight, peak, "a batch's applies were not all in flight at once");
    assert!(batches >= CHUNKS as u64 / peak, "{batches} batches for {CHUNKS} applies of at most {peak}");
    // bounded: as many as the bound holds at what an arm takes, and no longer than it
    let holds = (bound.as_micros() / arm.as_micros()) as u64 + 1;
    assert!(peak <= holds, "a batch of {peak} applies past the {holds} the bound holds");
    assert!(longest <= bound + arm + Duration::from_millis(60), "a batch took {longest:?} against a bound of {bound:?}");
    // a read waits for the one batch in flight when it came, and no other
    assert!(slice.read_batches_peak.load(Ordering::Relaxed) <= 1, "a read waited behind more than one batch");
    assert!(read_took <= bound + 2 * arm + Duration::from_millis(100), "a read beside applies took {read_took:?}");
    // and a solid state slice applies each as it arrives, in no batch
    if utils::rotational_run() {
        println!("a rotational run: every device is a disk, so no solid state slice is beside it");
    } else {
        let ssd_report = devices.iter().find(|device| device.class == "ssd").expect("the ssd");
        let (_, ssd_slice) = pool.device_slice(ssd_report.slices[0].id).expect("its slice");
        futures::executor::block_on(async {
            store.stage_whole(ssd_slice.id, whole(1, label(1), CHUNK)).await.expect("staged");
            store.apply(ssd_slice.id, chunk(1), label(1)).await.expect("applied");
        });
        assert_eq!(ssd_slice.apply_batches.load(Ordering::Relaxed), 0, "a solid state slice batched its applies");
    }
    pool.exit().expect("the node stops");
}

/// S13's `rotational_slice_never_shares_an_ssd_executor`. A node given an SSD's slice and a
/// rotational device's puts them on different executors, a disk alone on its own, and refuses a
/// start whose object cpus cannot keep them apart, by name (X7, F91)
#[test]
fn rotational_slice_never_shares_an_ssd_executor() {
    if utils::rotational_run() {
        println!("a rotational run: every device is a disk, so no solid state slice is beside one");
        return;
    }
    // three nodes, each with devices of its own, every disk among them rotational
    let dirs: Vec<tempfile::TempDir> = (0..15).map(|_| utils::test_dir()).collect();
    let disks: Vec<std::path::PathBuf> = (0..3).map(|node| dirs[node * 5 + 1].path().to_path_buf()).collect();
    let _facts = utils::disk_facts(&disks, Some("write through"));
    let (dir, disk, journal, first) = (&dirs[0], &dirs[1], &dirs[2], &dirs[3]);
    let disk_spec = DeviceSpec::new(disk.path(), "hdd").journal(journal.path());
    // two executors: the disk on one, the SSD on the other, every operation where its owner is
    let pool = node(dir, &[disk_spec, DeviceSpec::new(first.path(), "ssd")], 2, |conf| conf);
    let store = pool.object_store().expect("object executors").clone();
    let owners = store.owners();
    let devices = pool.devices();
    let disk_slice = devices.iter().find(|device| device.rotational).expect("the disk").slices[0].id;
    let ssd_slice = devices.iter().find(|device| !device.rotational).expect("the ssd").slices[0].id;
    assert_ne!(owners[&disk_slice].executor, owners[&ssd_slice].executor, "{owners:?}");
    futures::executor::block_on(async {
        for slice in [disk_slice, ssd_slice] {
            store.stage_whole(slice, whole(1, label(1), CHUNK)).await.expect("staged");
            store.apply(slice, chunk(1), label(1)).await.expect("applied");
        }
    });
    let served = store.served_on();
    assert!(served[&disk_slice].is_disjoint(&served[&ssd_slice]), "{served:?}");
    pool.exit().expect("the node stops");
    // three executors and two SSDs: the disk alone on one, the SSDs on the others
    let (dir, disk, journal, first, second) = (&dirs[5], &dirs[6], &dirs[7], &dirs[8], &dirs[9]);
    let pool = node(
        dir,
        &[
            DeviceSpec::new(first.path(), "ssd"),
            DeviceSpec::new(disk.path(), "hdd").journal(journal.path()),
            DeviceSpec::new(second.path(), "ssd"),
        ],
        3,
        |conf| conf,
    );
    let store = pool.object_store().expect("object executors").clone();
    let owners = store.owners();
    let devices = pool.devices();
    let disk_slice = devices.iter().find(|device| device.rotational).expect("the disk").slices[0].id;
    let disk_executor = owners[&disk_slice].executor;
    let sharing: Vec<_> = owners
        .iter()
        .filter(|(slice, owner)| **slice != disk_slice && owner.executor == disk_executor)
        .collect();
    assert!(sharing.is_empty(), "an SSD's slice shares the disk's executor: {sharing:?}");
    pool.exit().expect("the node stops");
    // one executor cannot keep them apart, and the start is refused by name
    let (dir, disk, journal, first) = (&dirs[10], &dirs[11], &dirs[12], &dirs[13]);
    let mut conf = utils::with_devices(
        utils::build_config(dir),
        &[
            DeviceSpec::new(disk.path(), "hdd").journal(journal.path()),
            DeviceSpec::new(first.path(), "ssd"),
        ],
    );
    conf.resources = conf.resources.object_cpus(utils::object_cpus(1));
    match shoal::ShoalPool::<plain::Notes>::start(conf) {
        Err(shoal::server::ServerError::Shoal(shoal::server::errors::ShoalError::ObjectCpuRefused { why, .. })) => {
            assert!(why.contains("never shares an executor"), "{why}");
        }
        Err(other) => panic!("refused otherwise: {other}"),
        Ok(pool) => {
            let _ = pool.exit();
            panic!("a disk and an SSD were served on one executor");
        }
    }
}

/// A seal holds what it covers: a request for a sealed object's chunk waits until the seal ends
/// and one for another object does not; a reclamation under another seal, or under one that
/// lapsed, removes nothing and is refused `NotSealed`; one under the live seal removes every chunk
/// of the object, ends the seal, lets what waited run, and sends each chunk's file back to the pool
/// while it is short of its target (F92)
#[test]
fn a_seal_defers_what_it_covers_and_a_reclamation_needs_it_live() {
    use shoal::shared::protocol::peer::chunk::SealScope;
    use std::time::Duration;
    on_executor(|| async {
        let root = utils::test_dir();
        let device = device(root.path(), 0);
        let slice = slice(&device).await;
        // two stripes of one object and a chunk of another, in one placement group
        let first = chunk(7);
        let second = ChunkId { stripe: 1, ..chunk(7) };
        let other = chunk(8);
        for id in [first, second, other] {
            put(&slice, id, label(1), 3).await;
        }
        let scope = SealScope::Object {
            consumer: first.consumer,
            pg: first.pg,
            object: first.object,
        };
        slice.seal(scope, 11);
        // a request for the sealed object waits; one for another object runs at once
        let waited = Rc::new(std::cell::Cell::new(false));
        let waiter = {
            let (slice, waited) = (slice.clone(), waited.clone());
            shoal::glommio::spawn_local(async move {
                slice.until_unsealed(&first).await;
                waited.set(true);
            })
        };
        slice.until_unsealed(&other).await;
        shoal::glommio::timer::sleep(Duration::from_millis(50)).await;
        assert!(!waited.get(), "a request ran under a seal over its object");
        // another seal's word removes nothing
        assert_eq!(slice.reclaim(scope, 12).await, Err(StoreError::NotSealed));
        assert_eq!(slice.read(first, 0, CHUNK, label(1)).await.map(|read| matches!(read, ReadAnswer::Bytes(_))), Ok(true));
        // the live seal's word removes both of the object's chunks and none of the other's
        let removed = slice.reclaim(scope, 11).await.expect("reclaimed");
        assert_eq!(removed, 2);
        waiter.await;
        assert!(waited.get(), "the request under the seal never ran once it ended");
        for id in [first, second] {
            assert_eq!(slice.read(id, 0, CHUNK, label(1)).await, Ok(ReadAnswer::Label(None)), "{id:?}");
            assert!(!slice.chunk_path(&id).exists(), "{id:?}'s file is still in place");
        }
        assert_eq!(slice.read(other, 0, CHUNK, label(1)).await.map(|read| matches!(read, ReadAnswer::Bytes(_))), Ok(true));
        // the freed files went back to a pool short of its target, or were unlinked past it: the
        // pool holds its target of one and no more
        assert_eq!(slice.free_files(), 1);
        // a seal that lapses removes nothing either, and what waited on it runs
        let scope = SealScope::Object {
            consumer: other.consumer,
            pg: other.pg,
            object: other.object,
        };
        slice.seal_for(scope, 21, Duration::from_millis(100));
        slice.until_unsealed(&other).await;
        assert_eq!(slice.reclaim(scope, 21).await, Err(StoreError::NotSealed));
        assert_eq!(slice.read(other, 0, CHUNK, label(1)).await.map(|read| matches!(read, ReadAnswer::Bytes(_))), Ok(true));
        // and an unseal under another id ends nothing
        slice.seal(scope, 31);
        assert!(!slice.unseal(scope, 32));
        assert!(slice.unseal(scope, 31));
        slice.until_unsealed(&other).await;
        slice.close().await;
    });
}

/// A group's seal covers every object of the group on the slice, and its reclamation removes
/// them all and the group's directory, leaving another group's chunks (F92)
#[test]
fn a_released_group_leaves_nothing_of_itself_on_its_slice() {
    use shoal::shared::protocol::peer::chunk::SealScope;
    on_executor(|| async {
        let root = utils::test_dir();
        let device = device(root.path(), 0);
        let slice = slice(&device).await;
        let kept = ChunkId {
            pg: PlacementGroup { tablet: 2, sub: 0 },
            ..chunk(9)
        };
        for id in [chunk(7), chunk(8), kept] {
            put(&slice, id, label(1), 4).await;
        }
        let scope = SealScope::Group {
            consumer: chunk(7).consumer,
            pg: chunk(7).pg,
        };
        slice.seal(scope, 5);
        assert_eq!(slice.reclaim(scope, 5).await.expect("released"), 2);
        let group_dir = slice
            .chunk_path(&chunk(7))
            .parent()
            .and_then(Path::parent)
            .map(Path::to_path_buf)
            .expect("a group directory");
        assert!(!group_dir.exists(), "the released group's directory is left: {}", group_dir.display());
        assert_eq!(slice.read(kept, 0, CHUNK, label(1)).await.map(|read| matches!(read, ReadAnswer::Bytes(_))), Ok(true));
        assert_eq!(device.slices[0].chunks.load(std::sync::atomic::Ordering::Relaxed), 1);
        slice.close().await;
    });
}

/// A slice lists every placement group it has a directory of, which is what a sweep asks it, and a
/// group released there leaves none: one with chunks, and one whose directory was left empty, which
/// would otherwise be listed and swept again at every pass (item 253)
#[test]
fn a_slice_lists_its_groups_and_a_released_one_is_no_longer_listed() {
    use shoal::shared::protocol::peer::chunk::SealScope;
    on_executor(|| async {
        let root = utils::test_dir();
        let device = device(root.path(), 0);
        let slice = slice(&device).await;
        let other = ChunkId {
            pg: PlacementGroup { tablet: 2, sub: 0 },
            ..chunk(9)
        };
        for id in [chunk(7), other] {
            put(&slice, id, label(1), 4).await;
        }
        // a third group's directory, left empty
        let empty = ChunkId {
            pg: PlacementGroup { tablet: 3, sub: 1 },
            ..chunk(10)
        };
        let empty_dir = slice
            .chunk_path(&empty)
            .parent()
            .and_then(Path::parent)
            .map(Path::to_path_buf)
            .expect("a group directory");
        std::fs::create_dir_all(&empty_dir).expect("the empty group's directory is made");
        let mut listed = slice.groups().expect("the groups list");
        listed.sort_unstable();
        assert_eq!(listed, vec![(chunk(7).consumer, chunk(7).pg), (other.consumer, other.pg), (empty.consumer, empty.pg)]);
        // each released under its seal
        for (id, pg, removed) in [(1u64, chunk(7).pg, 1u64), (2, empty.pg, 0)] {
            let scope = SealScope::Group {
                consumer: chunk(7).consumer,
                pg,
            };
            slice.seal(scope, id);
            assert_eq!(slice.reclaim(scope, id).await.expect("released"), removed);
        }
        assert!(!empty_dir.exists(), "the empty group's directory is left: {}", empty_dir.display());
        assert_eq!(slice.groups().expect("the groups list"), vec![(other.consumer, other.pg)]);
        slice.close().await;
    });
}

/// What a reclamation removed stays removed for a seal's lapse: a stage of a removed object's
/// chunk, a piece that was on its way or waited on the seal, is refused `Removed`, and another
/// object's stage in the same placement group is not (item 253)
#[test]
fn a_late_write_of_what_was_removed_is_refused() {
    use shoal::shared::protocol::peer::chunk::SealScope;
    on_executor(|| async {
        let root = utils::test_dir();
        let device = device(root.path(), 0);
        let slice = slice(&device).await;
        put(&slice, chunk(7), label(1), 5).await;
        let scope = SealScope::Object {
            consumer: chunk(7).consumer,
            pg: chunk(7).pg,
            object: chunk(7).object,
        };
        slice.seal(scope, 41);
        slice.reclaim(scope, 41).await.expect("reclaimed");
        // the removed object's chunk, staged again at once, is refused
        assert_eq!(slice.refuse_removed(&chunk(7)), Err(StoreError::Removed));
        // another object of the group is written as ever
        assert_eq!(slice.refuse_removed(&chunk(8)), Ok(()));
        slice.close().await;
    });
}
