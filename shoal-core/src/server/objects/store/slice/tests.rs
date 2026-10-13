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
            keep_previous: true,
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
                also: Vec::new(),
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
                    also: Vec::new(),
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
                    also: Vec::new(),
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
                    also: Vec::new(),
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

/// A put's chunk, written in pieces with its sender's checksums, goes straight into place and
/// leaves no record; a staged one written the same way waits for its apply
#[test]
fn a_whole_chunk_in_pieces_is_placed_or_staged() {
    on_executor(|| async {
        let root = dir();
        let device = device(root.path(), ApplySync::Shared);
        let slice = slice(&device).await;
        let data = bytes(21, CHUNK as usize);
        // the sender's checksums of each unit, unbound, as an object lane carries them
        let crcs: Vec<u64> = data
            .chunks(UNIT as usize)
            .map(crate::shared::checksum::crc64)
            .collect();
        // two pieces of two units each, placed: readable at once, nothing staged
        let mut writer = slice
            .begin_whole(chunk(), Label::PUT, CHUNK, UNIT, CHUNK)
            .await
            .expect("begun");
        let half = 2 * UNIT as usize;
        writer.piece(0, &crcs[..2], &data[..half]).await.expect("first piece");
        // a piece out of order is refused
        assert!(writer.piece(0, &crcs[..2], &data[..half]).await.is_err());
        writer.piece(half as u64, &crcs[2..], &data[half..]).await.expect("second piece");
        writer.finish(true).await.expect("placed");
        assert_eq!(slice.staged_count(), 0);
        assert_eq!(
            slice.read(chunk(), 0, CHUNK, Label::PUT).await.expect("read"),
            ReadAnswer::Bytes(data.clone())
        );
        // a whole chunk staged over it in pieces reads at its label and leaves the put's alone
        let next = bytes(22, CHUNK as usize);
        let mut writer = slice
            .begin_whole(chunk(), label(1), CHUNK, UNIT, CHUNK)
            .await
            .expect("begun");
        writer.piece(0, &[], &next).await.expect("piece");
        writer.finish(false).await.expect("staged");
        assert_eq!(slice.staged_count(), 1);
        assert_eq!(
            slice.read(chunk(), 0, CHUNK, label(1)).await.expect("read"),
            ReadAnswer::Bytes(next.clone())
        );
        assert_eq!(slice.chunk_label(&chunk()).await.expect("label"), Some(Label::PUT));
        // a writer given up gives its file back
        let free = slice.free_files();
        let writer = slice
            .begin_whole(chunk(), label(7), CHUNK, UNIT, CHUNK)
            .await
            .expect("begun");
        writer.abort().await;
        assert!(slice.free_files() >= free.saturating_sub(1));
        slice.close().await;
    });
}

/// A stage laid over a label pending bytes fold from is taken when it names it as an alternative,
/// and a fold, a read over several labels and a confirmation each answer with the label made
#[test]
fn a_stage_a_fold_and_a_read_take_the_first_label_made() {
    on_executor(|| async {
        let root = dir();
        let device = device(root.path(), ApplySync::Shared);
        let slice = slice(&device).await;
        let base = stage_whole(&slice, label(1), 5, CHUNK).await;
        slice.apply(chunk(), label(1)).await.expect("applied");
        // a confirmation of a label the chunk is not at names the one it is at
        assert_eq!(
            slice.confirm(chunk(), &[label(2)]).await.expect("asked"),
            Err(Some(label(1)))
        );
        assert_eq!(
            slice.confirm(chunk(), &[label(2), label(1)]).await.expect("asked"),
            Ok(label(1))
        );
        // a small write's bytes folded over the base, the row's label first
        let pending = bytes(31, UNIT as usize);
        slice
            .fold(chunk(), label(2), vec![label(1)], CHUNK, vec![(0, pending.clone())])
            .await
            .expect("folded");
        // folding again over a chunk that makes the label already is nothing
        slice
            .fold(chunk(), label(2), vec![label(1)], CHUNK, vec![(0, pending.clone())])
            .await
            .expect("folded again");
        let mut folded = base.clone();
        folded[..UNIT as usize].copy_from_slice(&pending);
        // a read naming the folded label and its base answers with the folded one
        let (made, read) = slice
            .read_any(chunk(), 0, CHUNK, &[label(2), label(1)])
            .await
            .expect("read")
            .expect("made");
        assert_eq!(made, label(2));
        assert_eq!(read, folded);
        // a stage over the row's label or the base, carrying every unit, is laid over either
        let next = bytes(41, UNIT as usize);
        slice
            .stage_units(StageUnits {
                id: chunk(),
                makes: label(3),
                expects: label(9),
                also: vec![label(2)],
                length: CHUNK,
                units: vec![(1, next.clone())],
            })
            .await
            .expect("staged over the alternative");
        let mut after = folded.clone();
        after[UNIT as usize..2 * UNIT as usize].copy_from_slice(&next);
        assert_eq!(
            slice.read(chunk(), 0, CHUNK, label(3)).await.expect("read"),
            ReadAnswer::Bytes(after)
        );
        // a read of labels none of which it makes says what it holds
        let held = slice
            .read_any(chunk(), 0, 10, &[label(8)])
            .await
            .expect("read");
        assert!(matches!(held, Err(Some(_))));
        slice.close().await;
    });
}

/// A record is undecided once it is older than its stager's time, or at once once a start found it
#[test]
fn a_record_is_undecided_once_its_stager_had_its_time() {
    on_executor(|| async {
        let root = dir();
        let device = device(root.path(), ApplySync::Each);
        {
            let slice = slice(&device).await;
            stage_whole(&slice, label(1), 5, CHUNK).await;
            // a fresh stage is its stager's, and an aged one is the row's to decide
            assert!(slice.undecided(std::time::Duration::from_secs(60)).is_empty());
            assert_eq!(
                slice.undecided(std::time::Duration::ZERO),
                vec![(chunk(), label(1))]
            );
            slice.close().await;
        }
        // a start finds it, and its row decides it at once
        let slice = slice(&device).await;
        assert_eq!(
            slice.undecided(std::time::Duration::from_secs(60)),
            vec![(chunk(), label(1))]
        );
        slice.close().await;
    });
}

/// A chunk's state before its last apply is served to a reader naming its label until the next
/// apply, whether that apply wrote part of the chunk in place or a whole chunk over it; two applies
/// behind, or after a start, the reader is told the label the chunk carries (F86)
#[test]
fn a_reader_one_write_behind_is_served() {
    on_executor(|| async {
        let root = dir();
        let device = device(root.path(), ApplySync::Shared);
        let slice = slice(&device).await;
        let first = stage_whole(&slice, label(1), 5, CHUNK).await;
        slice.apply(chunk(), label(1)).await.expect("applied");
        // part of the chunk written in place: the unit it overwrote is kept
        let unit = bytes(21, UNIT as usize);
        slice
            .stage_units(StageUnits {
                id: chunk(),
                makes: label(2),
                expects: label(1),
                also: Vec::new(),
                length: CHUNK,
                units: vec![(2, unit.clone())],
            })
            .await
            .expect("staged");
        slice.apply(chunk(), label(2)).await.expect("applied");
        let mut second = first.clone();
        second[2 * UNIT as usize..3 * UNIT as usize].copy_from_slice(&unit);
        assert_eq!(slice.read(chunk(), 0, CHUNK, label(1)).await.expect("read"), ReadAnswer::Bytes(first.clone()));
        assert_eq!(slice.read(chunk(), 0, CHUNK, label(2)).await.expect("read"), ReadAnswer::Bytes(second.clone()));
        // a whole chunk applied over it: the chunk's file is kept aside
        let third = stage_whole(&slice, label(3), 6, CHUNK).await;
        slice.apply(chunk(), label(3)).await.expect("applied");
        assert_eq!(slice.read(chunk(), 0, CHUNK, label(2)).await.expect("read"), ReadAnswer::Bytes(second));
        assert_eq!(slice.read(chunk(), 0, CHUNK, label(3)).await.expect("read"), ReadAnswer::Bytes(third.clone()));
        // two applies behind is not served
        assert_eq!(slice.read(chunk(), 0, 10, label(1)).await.expect("read"), ReadAnswer::Label(Some(label(3))));
        slice.close().await;
        drop(slice);
        // and a start keeps no previous state, its linked files gone
        let started = self::slice(&device).await;
        assert_eq!(started.read(chunk(), 0, 10, label(2)).await.expect("read"), ReadAnswer::Label(Some(label(3))));
        assert_eq!(started.read(chunk(), 0, CHUNK, label(3)).await.expect("read"), ReadAnswer::Bytes(third));
        let aside = std::fs::read_dir(root.path().join("slice-0").join("previous")).expect("the directory").count();
        assert_eq!(aside, 0, "a start left a previous state's file behind");
        started.close().await;
    });
}

/// A slice told not to keep a chunk's previous state answers a reader one write behind with the
/// label the chunk carries, as every slice did before F86
#[test]
fn a_previous_state_is_kept_only_when_asked() {
    on_executor(|| async {
        let root = dir();
        let device = device(root.path(), ApplySync::Shared);
        let slice = Slice::open(
            device.clone(),
            device.slices[0].clone(),
            SliceOptions {
                segment_bytes: 1 << 20,
                free_files: 2,
                chunk_lengths: vec![CHUNK],
                budget: None,
                keep_previous: false,
            },
            None,
        )
        .await
        .expect("the slice opens");
        stage_whole(&slice, label(1), 5, CHUNK).await;
        slice.apply(chunk(), label(1)).await.expect("applied");
        stage_whole(&slice, label(2), 6, CHUNK).await;
        slice.apply(chunk(), label(2)).await.expect("applied");
        assert_eq!(slice.read(chunk(), 0, 10, label(1)).await.expect("read"), ReadAnswer::Label(Some(label(2))));
        slice.close().await;
    });
}

/// A record staged over a further label it may stand on applies once the chunk reaches that label
/// instead of the one it was staged over, and every record the apply passed is dropped
/// ([Resolved #239](../../../../../../docs/src/appendix/resolved/record-bases.md))
#[test]
fn a_record_applies_over_any_label_it_may_be_laid_over() {
    on_executor(|| async {
        let root = dir();
        let device = device(root.path(), ApplySync::Shared);
        let slice = slice(&device).await;
        let base = stage_whole(&slice, label(1), 5, CHUNK).await;
        slice.apply(chunk(), label(1)).await.expect("applied");
        // a small write's bytes pending in the row, and a write staged with them before the
        // fold reached this slice: over label 2 or its base, label 1, which is all it can make
        let pending = bytes(51, UNIT as usize);
        let own = bytes(52, UNIT as usize);
        slice
            .stage_units(StageUnits {
                id: chunk(),
                makes: label(3),
                expects: label(2),
                also: vec![label(1)],
                length: CHUNK,
                units: vec![(0, pending.clone()), (1, own.clone())],
            })
            .await
            .expect("staged over the base");
        // the fold arrives and applies, so the chunk is at label 2 before label 3 is applied
        slice
            .fold(chunk(), label(2), vec![label(1)], CHUNK, vec![(0, pending.clone())])
            .await
            .expect("folded");
        for _ in 0..1000 {
            if slice.chunk_label(&chunk()).await.expect("read") == Some(label(2)) {
                break;
            }
            glommio::timer::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert_eq!(slice.chunk_label(&chunk()).await.expect("read"), Some(label(2)));
        // label 3 still applies, over label 2, and holds every unit
        slice.apply(chunk(), label(3)).await.expect("applied over the label the fold made");
        let mut expected = base.clone();
        expected[..UNIT as usize].copy_from_slice(&pending);
        expected[UNIT as usize..2 * UNIT as usize].copy_from_slice(&own);
        assert_eq!(slice.chunk_label(&chunk()).await.expect("read"), Some(label(3)));
        assert_eq!(slice.read(chunk(), 0, CHUNK, label(3)).await.expect("read"), ReadAnswer::Bytes(expected));
        // a committed write the next one carries is passed over, and dropped when it is
        let fourth = bytes(53, UNIT as usize);
        let fifth = bytes(54, UNIT as usize);
        slice
            .stage_units(StageUnits {
                id: chunk(),
                makes: label(4),
                expects: label(3),
                also: Vec::new(),
                length: CHUNK,
                units: vec![(2, fourth.clone())],
            })
            .await
            .expect("staged");
        slice
            .stage_units(StageUnits {
                id: chunk(),
                makes: label(5),
                expects: label(3),
                also: vec![label(4)],
                length: CHUNK,
                units: vec![(2, fourth), (3, fifth)],
            })
            .await
            .expect("staged");
        slice.apply(chunk(), label(5)).await.expect("applied");
        assert_eq!(slice.chunk_label(&chunk()).await.expect("read"), Some(label(5)));
        assert_eq!(slice.staged_count(), 0, "a record the apply passed was left staged");
        slice.close().await;
    });
}
