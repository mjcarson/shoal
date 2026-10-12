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
use shoal::server::objects::store::{ReadAnswer, Slice, SliceOptions, StageUnits, StageWhole, StoreError};
use shoal::shared::devices::{ApplySync, DeviceId, DeviceSpec, SliceId};
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
    Arc::new(DeviceState::new(
        DeviceId::mint(),
        DeviceSpec::new(root, "ssd"),
        Facts::default(),
        reserve,
        ApplySync::Shared,
        vec![(SliceId::mint(), root.join("slice-0"))],
        Vec::new(),
    ))
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

/// Wait for a slice's pool to be refilled to its target
///
/// # Arguments
///
/// * `slice` - The slice
async fn refilled(slice: &Rc<Slice>) {
    for _ in 0..1000 {
        if slice.free_files() >= 1 {
            return;
        }
        shoal::glommio::timer::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("the pool was never refilled");
}

/// A device filled after a stage still applies it; a stage past the reserve is refused before any
/// commit (S6)
#[test]
fn apply_needs_no_space() {
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
                length: CHUNK,
                units: vec![(2, update.clone())],
            })
            .await
            .expect("staged");
        refilled(&slice).await;
        // the device fills: not one byte more may be allocated
        faults::arm(root.path(), Fault::Full { budget: 0 });
        // the apply writes in place, so it needs nothing, and it applies
        slice.apply(chunk(1), label(2)).await.expect("an apply needs no space");
        let read = slice
            .read(chunk(1), 2 * u64::from(UNIT), u64::from(UNIT), label(2))
            .await
            .expect("read");
        assert_eq!(read, ReadAnswer::Bytes(update));
        // a whole chunk takes the pool's file written ahead, which needs no space either
        put(&slice, chunk(2), label(1), 2).await;
        // and with the pool empty and the device past its reserve, a stage is refused before
        // anything could commit on it
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

/// A unit copied to another chunk or another offset fails verification (S6)
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
        // bytes that verify at their place verify nowhere else, and are never returned
        for (id, at) in [(chunk(2), 1u64), (chunk(1), 3)] {
            let read = slice
                .read(id, at * u64::from(UNIT), u64::from(UNIT), label(1))
                .await;
            assert_eq!(read, Err(StoreError::Corrupt { unit: at as u32 }), "{id:?}");
        }
        // and the unit at its own place still reads
        let read = slice
            .read(chunk(1), u64::from(UNIT), u64::from(UNIT), label(1))
            .await
            .expect("read");
        assert_eq!(read, ReadAnswer::Bytes(copied));
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
    let pool = node(
        &dir,
        &[
            DeviceSpec::new(first.path(), "ssd").slices(2),
            DeviceSpec::new(second.path(), "ssd"),
        ],
        2,
        |conf| conf,
    );
    let store = pool.object_store().expect("the node has object executors").clone();
    let owners = store.owners();
    assert_eq!(owners.len(), 3);
    // a device's two slices are driven from two executors
    let first_device = &pool.devices()[0];
    let executors: std::collections::BTreeSet<usize> = first_device
        .slices
        .iter()
        .map(|slice| owners[&slice.id].executor)
        .collect();
    assert_eq!(executors.len(), 2, "{owners:?}");
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
    // the rows' share is what is left once object work's is taken out
    let shards = 2;
    let reserved = (64usize << 20) + shoal::server::conf::OBJECT_IO_MEMORY;
    let mut resources = shoal::server::conf::Resources::default()
        .memory("1GiB")
        .expect("a size")
        .node_memory("2GiB")
        .expect("a size")
        .object_cpus(vec![1]);
    resources = resources.object_memory("64MiB").expect("a size");
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
