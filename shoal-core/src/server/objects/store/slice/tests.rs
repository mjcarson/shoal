//! A slice driven on its own executor: stage, apply, read, discard, and what a start recovers

use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use super::{ReadAnswer, Slice, SliceOptions, StageUnits, StageWhole};
use crate::server::devices::{DeviceState, Facts};
use crate::server::objects::store::chunk::{ChunkHeader, ChunkId, BLOCK};
use crate::server::objects::store::StoreError;
use crate::shared::devices::{ApplySync, DeviceId, DeviceSpec, SliceId};
use crate::shared::objects::Label;
use crate::shared::poolmap::PlacementGroup;

/// A unit of 64 KiB, and a chunk of four of them
const UNIT: u32 = 64 << 10;
/// A chunk of four units
const CHUNK: u64 = 4 * UNIT as u64;

/// A directory on the repository's own filesystem, where direct I/O is real
fn dir() -> tempfile::TempDir {
    let target = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/store-tests");
    std::fs::create_dir_all(&target).expect("made");
    tempfile::tempdir_in(target).expect("a directory")
}

/// A device of one slice under a directory
fn device(root: &Path, sync: ApplySync) -> Arc<DeviceState> {
    Arc::new(DeviceState::new(
        DeviceId::mint(),
        DeviceSpec::new(root, "ssd"),
        Facts::default(),
        0,
        sync,
        vec![(SliceId(uuid::Uuid::from_u128(77)), root.join("slice-0"))],
        Vec::new(),
    ))
}

/// Open a slice of a device
async fn slice(device: &Arc<DeviceState>) -> Rc<Slice> {
    Slice::open(
        device.clone(),
        device.slices[0].clone(),
        SliceOptions {
            segment_bytes: 1 << 20,
            free_files: 2,
            chunk_lengths: vec![CHUNK],
            budget: None,
        },
        None,
    )
    .await
    .expect("the slice opens")
}

/// A chunk
fn chunk() -> ChunkId {
    ChunkId {
        consumer: 1,
        pg: PlacementGroup { tablet: 5, sub: 0 },
        object: 99,
        stripe: 2,
        position: 1,
    }
}

/// A label
fn label(sequence: u64) -> Label {
    Label {
        sequence,
        tag: sequence * 31,
    }
}

/// Seeded bytes, so a mixed read can never pass
fn bytes(seed: u8, len: usize) -> Vec<u8> {
    (0..len as u64)
        .map(|index| (crate::shared::rendezvous::mix64(index ^ u64::from(seed) << 56) >> 24) as u8)
        .collect()
}

/// Run a test on an executor of its own
fn on_executor<F: std::future::Future<Output = ()> + 'static>(test: impl FnOnce() -> F + Send + 'static) {
    glommio::LocalExecutorBuilder::default()
        .spawn(move || async move { test().await })
        .expect("an executor")
        .join()
        .expect("the test ran");
}

/// Stage a whole chunk at a label
async fn stage_whole(slice: &Rc<Slice>, made: Label, seed: u8, length: u64) -> Vec<u8> {
    let data = bytes(seed, length as usize);
    slice
        .stage_whole(StageWhole {
            id: chunk(),
            label: made,
            chunk_bytes: CHUNK,
            unit_bytes: UNIT,
            length,
            bytes: data.clone(),
        })
        .await
        .expect("staged");
    data
}

/// A whole chunk reads at its label staged, then applied; another label is answered with the one
/// the chunk carries
#[test]
fn a_whole_chunk_stages_applies_and_reads() {
    on_executor(|| async {
        let root = dir();
        let device = device(root.path(), ApplySync::Each);
        let slice = slice(&device).await;
        // 200 KiB of a 256 KiB chunk: the last unit part filled, nothing past it
        let data = stage_whole(&slice, label(1), 3, 200 << 10).await;
        // staged, it is read from its file of the pool
        let read = slice.read(chunk(), 0, CHUNK, label(1)).await.expect("read");
        let mut expected = data.clone();
        expected.resize(CHUNK as usize, 0);
        assert_eq!(read, ReadAnswer::Bytes(expected.clone()));
        // applied, from the chunk
        slice.apply(chunk(), label(1)).await.expect("applied");
        assert_eq!(slice.staged_count(), 0);
        let read = slice.read(chunk(), 70_000, 1000, label(1)).await.expect("read");
        assert_eq!(read, ReadAnswer::Bytes(expected[70_000..71_000].to_vec()));
        // another label is not made, and the reader is told which one is
        let other = slice.read(chunk(), 0, 10, label(2)).await.expect("read");
        assert_eq!(other, ReadAnswer::Label(Some(label(1))));
        // applying it again changes nothing
        slice.apply(chunk(), label(1)).await.expect("applied again");
        assert_eq!(device.slices[0].chunks.load(std::sync::atomic::Ordering::Relaxed), 1);
        slice.close().await;
    });
}

/// Part of a chunk stages over a label the slice can make, reads overlaid, and applies in order
#[test]
fn units_stage_over_their_base_and_apply_in_order() {
    on_executor(|| async {
        let root = dir();
        let device = device(root.path(), ApplySync::Shared);
        let slice = slice(&device).await;
        let base = stage_whole(&slice, label(1), 5, CHUNK).await;
        slice.apply(chunk(), label(1)).await.expect("applied");
        // a label the slice cannot make is refused
        let refused = slice
            .stage_units(StageUnits {
                id: chunk(),
                makes: label(9),
                expects: label(8),
                length: CHUNK,
                units: vec![(0, bytes(1, UNIT as usize))],
            })
            .await;
        assert!(matches!(refused, Err(StoreError::CannotMake { .. })), "{refused:?}");
        // two writes, each over the one before
        let second = bytes(11, UNIT as usize);
        let third = bytes(12, UNIT as usize);
        for (made, over, unit, data) in [(2, 1, 1u32, &second), (3, 2, 3, &third)] {
            slice
                .stage_units(StageUnits {
                    id: chunk(),
                    makes: label(made),
                    expects: label(over),
                    length: CHUNK,
                    units: vec![(unit, data.clone())],
                })
                .await
                .expect("staged");
        }
        // read at the last label, overlaid from both records over the chunk
        let mut expected = base.clone();
        expected[UNIT as usize..2 * UNIT as usize].copy_from_slice(&second);
        expected[3 * UNIT as usize..].copy_from_slice(&third);
        assert_eq!(slice.read(chunk(), 0, CHUNK, label(3)).await.expect("read"), ReadAnswer::Bytes(expected.clone()));
        // the older label still reads as it was
        let mut middle = base.clone();
        middle[UNIT as usize..2 * UNIT as usize].copy_from_slice(&second);
        assert_eq!(slice.read(chunk(), 0, CHUNK, label(2)).await.expect("read"), ReadAnswer::Bytes(middle));
        // applying the last applies the one beneath it first
        slice.apply(chunk(), label(3)).await.expect("applied");
        assert_eq!(slice.staged_count(), 0);
        assert_eq!(slice.chunk_label(&chunk()).await.expect("read"), Some(label(3)));
        assert_eq!(slice.read(chunk(), 0, CHUNK, label(3)).await.expect("read"), ReadAnswer::Bytes(expected));
        // a shared flush covered the applies
        assert!(slice.flushes().0 >= 1);
        slice.close().await;
    });
}

/// A start finds a staged record and keeps it, drops an applied one, and a discarded whole chunk's
/// file goes back to the pool
#[test]
fn a_start_recovers_what_was_staged() {
    on_executor(|| async {
        let root = dir();
        let device = device(root.path(), ApplySync::Each);
        {
            let slice = slice(&device).await;
            stage_whole(&slice, label(1), 2, CHUNK).await;
            slice.apply(chunk(), label(1)).await.expect("applied");
            slice
                .stage_units(StageUnits {
                    id: chunk(),
                    makes: label(2),
                    expects: label(1),
                    length: CHUNK,
                    units: vec![(2, bytes(40, UNIT as usize))],
                })
                .await
                .expect("staged");
            // the slice is gone without a word, as a crash leaves it
            slice.close().await;
        }
        let slice = slice(&device).await;
        // the applied whole chunk's record is dropped and the staged one kept
        assert_eq!(slice.staged_labels(&chunk()), vec![label(2)]);
        slice.apply(chunk(), label(2)).await.expect("applied after the start");
        slice.close().await;
        // and once applied, the next start drops the record it finds again
        let slice = Slice::open(device.clone(), device.slices[0].clone(), slice.options().clone(), None)
            .await
            .expect("opened");
        assert_eq!(slice.staged_count(), 0);
        // a whole chunk staged and discarded gives its file back
        let before = slice.free_files();
        stage_whole(&slice, label(3), 9, CHUNK).await;
        slice.discard(chunk(), label(3)).await.expect("discarded");
        assert_eq!(slice.free_files(), before);
        slice.close().await;
    });
}

/// A unit torn after its apply's header was written is found at the next start and written again
/// from its record, never returned torn
#[test]
fn a_torn_apply_is_written_again_at_the_start() {
    on_executor(|| async {
        let root = dir();
        let device = device(root.path(), ApplySync::Each);
        let path;
        let new = bytes(66, UNIT as usize);
        {
            let slice = slice(&device).await;
            stage_whole(&slice, label(1), 4, CHUNK).await;
            slice.apply(chunk(), label(1)).await.expect("applied");
            slice
                .stage_units(StageUnits {
                    id: chunk(),
                    makes: label(2),
                    expects: label(1),
                    length: CHUNK,
                    units: vec![(0, new.clone())],
                })
                .await
                .expect("staged");
            path = slice.chunk_path(&chunk());
            slice.close().await;
        }
        // an apply that wrote its header and tore its unit: the new header, the unit half old
        let block = std::fs::read(&path).expect("the chunk");
        let header = ChunkHeader::decode(&block[..BLOCK]).expect("its header");
        let mut torn = header.clone();
        torn.label = label(2);
        torn.crcs[0] = crate::server::objects::store::chunk::bound_crc(
            crate::shared::checksum::crc64(&new),
            &chunk(),
            0,
        );
        let mut file = block.clone();
        file[..BLOCK].copy_from_slice(&torn.encode());
        file[BLOCK..BLOCK + UNIT as usize / 2].copy_from_slice(&new[..UNIT as usize / 2]);
        std::fs::write(&path, &file).expect("torn");
        // the start writes it again from the record
        let slice = slice(&device).await;
        assert_eq!(slice.staged_count(), 0);
        let read = slice.read(chunk(), 0, u64::from(UNIT), label(2)).await.expect("read");
        assert_eq!(read, ReadAnswer::Bytes(new));
        slice.close().await;
    });
}
