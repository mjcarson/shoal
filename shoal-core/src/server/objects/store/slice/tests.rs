//! A slice driven on its own executor: stage, apply, read, discard, and what a start recovers

use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use super::{Found, ReadAnswer, Slice, SliceOptions, StageUnits, StageWhole};
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
            apply_queue: None,
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
        // a whole chunk staged and discarded gives its file back, to a pool short of its target of
        // two; a fresh file is made for the stage, since nothing is written ahead (F96)
        let before = slice.free_files();
        stage_whole(&slice, label(3), 9, CHUNK).await;
        slice.discard(chunk(), label(3)).await.expect("discarded");
        assert_eq!(slice.free_files(), (before + 1).min(2));
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
            .begin_whole(chunk(), Label::PUT, CHUNK, UNIT, CHUNK, true)
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
            .begin_whole(chunk(), label(1), CHUNK, UNIT, CHUNK, true)
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
        // a writer given up leaves the pool as it was: its fresh file is removed, never given back
        // half written (F96)
        let free = slice.free_files();
        let writer = slice
            .begin_whole(chunk(), label(7), CHUNK, UNIT, CHUNK, true)
            .await
            .expect("begun");
        writer.abort().await;
        assert_eq!(slice.free_files(), free);
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
                apply_queue: None,
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

/// A record another record of its chunk stands on is never offered to its row alone: three
/// records staged in a chain offer only the newest, a discard of it offers the one beneath, and an
/// apply of that one takes the chain and leaves nothing to decide
/// ([Resolved #283](../../../../../../docs/src/appendix/resolved/chained-records-decided-together.md))
#[test]
fn a_record_a_later_one_stands_on_is_never_offered_alone() {
    on_executor(|| async {
        let root = dir();
        let device = device(root.path(), ApplySync::Shared);
        let slice = slice(&device).await;
        stage_whole(&slice, label(1), 5, CHUNK).await;
        slice.apply(chunk(), label(1)).await.expect("applied");
        // three small writes' records, each standing on the one before, none of them applied, as
        // a disk's applies wait behind their batches while its folds are journalled
        for (makes, expects) in [(2u64, 1u64), (3, 2), (4, 3)] {
            slice
                .stage_units(StageUnits {
                    id: chunk(),
                    makes: label(makes),
                    expects: label(expects),
                    also: Vec::new(),
                    length: CHUNK,
                    units: vec![(0, bytes(50 + makes as u8, UNIT as usize))],
                })
                .await
                .expect("staged over the record beneath");
        }
        // only the newest is offered to its row; the two beneath stand under it
        assert_eq!(slice.undecided(std::time::Duration::ZERO), vec![(chunk(), label(4))]);
        // the newest discarded, the one beneath it is offered in its turn
        slice.discard(chunk(), label(4)).await.expect("discarded");
        assert_eq!(slice.undecided(std::time::Duration::ZERO), vec![(chunk(), label(3))]);
        // and its apply takes the chain beneath it, leaving nothing staged or to decide
        slice.apply(chunk(), label(3)).await.expect("applied over the chain");
        assert_eq!(slice.chunk_label(&chunk()).await.expect("read"), Some(label(3)));
        assert!(slice.undecided(std::time::Duration::ZERO).is_empty());
        assert_eq!(slice.staged_count(), 0, "a record the apply passed was left staged");
        slice.close().await;
    });
}

/// A whole chunk is never placed or applied over a newer one: a rebuild that made the chunk a
/// later label in place leaves an older staged whole chunk unapplied and refuses an older
/// placement, the chunk unchanged by either (F88, X1's `whole_apply_older_than_the_chunk`)
#[test]
fn a_whole_chunk_is_never_older_than_the_one_it_replaces() {
    on_executor(|| async {
        let root = dir();
        let device = device(root.path(), ApplySync::Each);
        let slice = slice(&device).await;
        // a write staged at 2, and a rebuild that places 3 before it applies
        stage_whole(&slice, label(2), 5, CHUNK).await;
        let rebuilt = bytes(9, CHUNK as usize);
        slice
            .place_whole(StageWhole {
                id: chunk(),
                label: label(3),
                chunk_bytes: CHUNK,
                unit_bytes: UNIT,
                length: CHUNK,
                bytes: rebuilt.clone(),
            })
            .await
            .expect("placed");
        // the older stage's apply leaves the chunk at 3, and its record goes
        slice.apply(chunk(), label(2)).await.expect("applied");
        assert_eq!(slice.chunk_label(&chunk()).await.expect("label"), Some(label(3)));
        assert_eq!(slice.staged_count(), 0);
        // and an older placement is refused, the chunk unchanged
        let older = slice
            .place_whole(StageWhole {
                id: chunk(),
                label: label(1),
                chunk_bytes: CHUNK,
                unit_bytes: UNIT,
                length: CHUNK,
                bytes: bytes(4, CHUNK as usize),
            })
            .await;
        assert!(matches!(older, Err(StoreError::CannotMake { .. })), "{older:?}");
        assert_eq!(
            slice.read(chunk(), 0, CHUNK, label(3)).await.expect("read"),
            ReadAnswer::Bytes(rebuilt)
        );
        slice.close().await;
    });
}

/// A placement group's listing names every chunk the slice holds of it in order, a staged whole
/// chunk with the label it stages, in pages past a place, and nothing of another group (F88)
#[test]
fn a_placement_group_is_listed_in_pages() {
    on_executor(|| async {
        let root = dir();
        let device = device(root.path(), ApplySync::Each);
        let slice = slice(&device).await;
        // three stripes placed, one staged, and one of another group
        let at = |object: u128, stripe: u64, sub: u16| ChunkId {
            object,
            stripe,
            pg: PlacementGroup { tablet: 5, sub },
            ..chunk()
        };
        for (id, made) in [(at(7, 0, 0), Label::PUT), (at(7, 1, 0), label(4)), (at(9, 0, 0), Label::PUT), (at(7, 0, 1), Label::PUT)] {
            slice
                .place_whole(StageWhole {
                    id,
                    label: made,
                    chunk_bytes: CHUNK,
                    unit_bytes: UNIT,
                    length: UNIT as u64,
                    bytes: bytes(3, UNIT as usize),
                })
                .await
                .expect("placed");
        }
        slice
            .stage_whole(StageWhole {
                id: at(8, 3, 0),
                label: label(2),
                chunk_bytes: CHUNK,
                unit_bytes: UNIT,
                length: UNIT as u64,
                bytes: bytes(6, UNIT as usize),
            })
            .await
            .expect("staged");
        let (first, more) = slice.list(at(0, 0, 0), true, 2, false).await.expect("listed");
        assert!(more);
        let flat = |listed: Vec<crate::shared::protocol::peer::chunk::ListedChunk>| -> Vec<(ChunkId, Label, u64, bool)> {
            listed
                .into_iter()
                .map(|listed| (listed.chunk, listed.label, listed.length, listed.staged))
                .collect()
        };
        assert_eq!(
            flat(first),
            vec![(at(7, 0, 0), Label::PUT, UNIT as u64, false), (at(7, 1, 0), label(4), UNIT as u64, false)]
        );
        let (rest, more) = slice.list(at(7, 1, 0), false, 10, false).await.expect("listed");
        assert!(!more);
        // a whole chunk staged and not yet applied is listed as staged (F89)
        assert_eq!(
            flat(rest),
            vec![(at(8, 3, 0), label(2), 0, true), (at(9, 0, 0), Label::PUT, UNIT as u64, false)]
        );
        // a failed device lists nothing as an error, never as an empty group
        device.fail("pulled");
        assert!(slice.list(at(0, 0, 0), true, 10, false).await.is_err());
        slice.close().await;
    });
}

/// A whole chunk's first piece to a slice whose device just failed is refused, and nothing
/// panics (item 242)
///
/// glommio polls a task it spawns before `spawn_local` returns, so the stream task a first piece
/// starts runs inside `Streams::take`. On a failed slice its first piece fails before anything is
/// awaited, and the task's way out borrows the streams' map, which `take` held while it spawned it:
/// on the lab an operator's `fail-device` ended a node's object executor so.
#[test]
fn a_first_piece_to_a_failed_slice_is_refused_and_panics_nothing() {
    use crate::server::objects::serve::{Replier, Streams};
    use crate::shared::protocol::peer::chunk::{ChunkReply, Piece};
    use std::cell::RefCell;
    on_executor(|| async {
        let root = dir();
        let device = device(root.path(), ApplySync::Shared);
        let slice = slice(&device).await;
        // the device fails as an operator's order or an I/O error fails it
        device.fail("failed by the test");
        let streams = Streams::default();
        let answers: Rc<RefCell<Vec<ChunkReply>>> = Rc::new(RefCell::new(Vec::new()));
        // two streams' first pieces, each of a chunk of two pieces
        for stream in 0..2u64 {
            let piece = Piece {
                stream,
                chunk: chunk(),
                label: label(1),
                chunk_bytes: CHUNK,
                unit_bytes: UNIT,
                length: CHUNK,
                offset: 0,
                last: false,
                stripe_length: CHUNK,
                owner: 0,
                crcs: Vec::new(),
                bytes: bytes(5, 2 * UNIT as usize).into(),
            };
            let answers = answers.clone();
            let replier: Replier = Box::new(move |reply| answers.borrow_mut().push(reply));
            streams.take(slice.clone(), 0, true, piece, None, replier);
        }
        // and each is refused, by the stream task it started
        glommio::timer::sleep(std::time::Duration::from_millis(50)).await;
        let answers = answers.borrow();
        assert_eq!(answers.len(), 2, "{answers:?}");
        assert!(
            answers.iter().all(|reply| matches!(reply, ChunkReply::Refused { .. })),
            "{answers:?}"
        );
        slice.close().await;
    });
}

/// A disk of one slice under a directory, its journal in another, as a rotational device's is on
/// an SSD of its node (F91)
fn disk(root: &Path, journal: &Path) -> Arc<DeviceState> {
    let facts = Facts {
        filesystem: "xfs".to_string(),
        rotational: true,
        write_cache: Some("write through".to_string()),
        ..Facts::default()
    };
    let state = DeviceState::new(
        DeviceId::mint(),
        DeviceSpec::new(root, "hdd"),
        facts,
        0,
        ApplySync::Shared,
        vec![(SliceId(uuid::Uuid::from_u128(78)), root.join("slice-0"))],
        Vec::new(),
    );
    Arc::new(state.with_journal(crate::shared::devices::JournalId::mint(), journal, false))
}

/// Under the shared flush, a whole chunk staged beside a journal on another device runs its own
/// device's flush before its record is synced, since the record's sync flushes the journal's
/// device and not the chunk's; one whose journal is on its device runs none (F91)
#[test]
fn a_whole_stage_beside_its_journal_flushes_its_device() {
    on_executor(|| async {
        // its journal on its own device: the record's sync is the stage's
        let root = dir();
        let beside = device(root.path(), ApplySync::Shared);
        let opened = slice(&beside).await;
        let before = opened.flushes().0;
        stage_whole(&opened, label(1), 4, CHUNK).await;
        assert_eq!(opened.flushes().0, before, "a stage beside its own journal flushed its device");
        opened.close().await;
        // its journal elsewhere: a fresh file is synced by itself, so the shared flush is not asked
        // (F96), and its record is there
        let root = dir();
        let journal = dir();
        let elsewhere = disk(root.path(), journal.path());
        let opened = slice(&elsewhere).await;
        let before = opened.flushes().0;
        stage_whole(&opened, label(1), 4, CHUNK).await;
        assert_eq!(opened.flushes().0, before, "a fresh file's stage asked the shared flush");
        assert_eq!(opened.staged_labels(&chunk()), vec![label(1)]);
        assert!(elsewhere.slices[0].journal.join("segment-0").exists(), "its journal is not where it was put");
        assert!(!root.path().join("slice-0").join("journal").exists(), "a journal was made under the disk");
        // and a file of the pool, the discarded stage's, flushes its own device first (F91)
        opened.discard(chunk(), label(1)).await.expect("discarded");
        assert_eq!(opened.free_files(), 1);
        let before = opened.flushes().0;
        stage_whole(&opened, label(2), 5, CHUNK).await;
        assert!(opened.flushes().0 > before, "a stage beside a journal elsewhere did not flush its device");
        opened.close().await;
    });
}

/// A read of several units is one run of its chunk on any device, laid over by what its records
/// hold: on a disk since F91, and on an SSD since F105, where a read was a unit at a time
#[test]
fn a_range_is_read_as_one_run() {
    use std::sync::atomic::Ordering;
    on_executor(|| async {
        for rotational in [false, true] {
            let root = dir();
            let journal = dir();
            let state = if rotational {
                disk(root.path(), journal.path())
            } else {
                device(root.path(), ApplySync::Shared)
            };
            let opened = slice(&state).await;
            let data = stage_whole(&opened, label(1), 7, CHUNK).await;
            opened.apply(chunk(), label(1)).await.expect("applied");
            // a unit staged over it, which the read lays over the span
            let unit = bytes(41, UNIT as usize);
            opened
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
            let ios = state.slices[0].read_ios.load(Ordering::Relaxed);
            // most of the chunk, across the staged unit, from part way into the first
            let (offset, len) = (1000, CHUNK - 2000);
            let ReadAnswer::Bytes(read) = opened.read(chunk(), offset, len, label(2)).await.expect("read") else {
                panic!("the label was not made");
            };
            let mut expected = data.clone();
            expected[2 * UNIT as usize..3 * UNIT as usize].copy_from_slice(&unit);
            assert_eq!(read, expected[offset as usize..(offset + len) as usize], "rotational: {rotational}");
            let issued = state.slices[0].read_ios.load(Ordering::Relaxed) - ios;
            assert_eq!(issued, 1, "rotational {rotational}: a read of three units from its chunk was {issued} reads");
            opened.close().await;
        }
    });
}

/// A journal found at another length than its device asks keeps the length it has while it holds
/// a record, and is made again at the length asked once a start finds it holding none, never
/// re-zeroed over a record (F91)
#[test]
fn a_journal_is_resized_only_once_it_holds_nothing() {
    on_executor(|| async {
        let root = dir();
        let device = device(root.path(), ApplySync::Each);
        let segment = device.slices[0].journal.join("segment-0");
        let sized = |bytes: u64| SliceOptions {
            segment_bytes: bytes,
            free_files: 2,
            chunk_lengths: vec![CHUNK],
            budget: None,
            keep_previous: true,
            apply_queue: None,
        };
        let open = |bytes: u64| {
            let device = device.clone();
            async move {
                Slice::open(device.clone(), device.slices[0].clone(), sized(bytes), None)
                    .await
                    .expect("the slice opens")
            }
        };
        // a record staged in a journal of 1 MiB segments
        let slice = open(1 << 20).await;
        stage_whole(&slice, label(1), 5, CHUNK).await;
        slice.apply(chunk(), label(1)).await.expect("applied");
        slice
            .stage_units(StageUnits {
                id: chunk(),
                makes: label(2),
                expects: label(1),
                also: Vec::new(),
                length: CHUNK,
                units: vec![(1, bytes(8, UNIT as usize))],
            })
            .await
            .expect("staged");
        slice.close().await;
        drop(slice);
        // asked for 2 MiB while it holds the record: kept at 1 MiB, the record with it
        let slice = open(2 << 20).await;
        assert_eq!(std::fs::metadata(&segment).expect("a segment").len(), 1 << 20);
        assert_eq!(slice.staged_labels(&chunk()), vec![label(2)]);
        slice.apply(chunk(), label(2)).await.expect("applied");
        slice.close().await;
        drop(slice);
        // holding nothing, the next start makes it again at 2 MiB
        let slice = open(2 << 20).await;
        assert_eq!(std::fs::metadata(&segment).expect("a segment").len(), 2 << 20);
        assert_eq!(slice.staged_count(), 0);
        slice.close().await;
    });
}

/// A whole chunk's file is the class of units it holds, a power of two of them and never more than
/// the chunk's, and the slice's figure counts each file at its own length; the whole chunk read
/// back is what it holds and zeros past it, a disk's span as well as a unit at a time, and nothing
/// past the file is read (F96)
#[test]
fn a_short_chunk_is_a_file_of_its_class() {
    use std::sync::atomic::Ordering;
    on_executor(|| async {
        let unit = u64::from(UNIT);
        for rotational in [false, true] {
            // the bytes a chunk holds, and the units of its file
            for (length, units) in [(1000, 1), (unit, 1), (unit + 1, 2), (2 * unit + 1, 4), (CHUNK, 4)] {
                let root = dir();
                let journal = dir();
                let state = if rotational {
                    disk(root.path(), journal.path())
                } else {
                    device(root.path(), ApplySync::Shared)
                };
                let opened = slice(&state).await;
                let data = stage_whole(&opened, label(1), 9, length).await;
                opened.apply(chunk(), label(1)).await.expect("applied");
                let size = std::fs::metadata(opened.chunk_path(&chunk())).expect("the chunk's file").len();
                assert_eq!(size, BLOCK as u64 + units * unit, "{length} bytes, rotational: {rotational}");
                assert_eq!(state.slices[0].chunk_bytes.load(Ordering::Relaxed), size, "the figure counts the file");
                // the whole chunk read: what it holds, then zeros
                let ReadAnswer::Bytes(read) = opened.read(chunk(), 0, CHUNK, label(1)).await.expect("read") else {
                    panic!("the label was not made");
                };
                let mut expected = data.clone();
                expected.resize(CHUNK as usize, 0);
                assert!(read == expected, "{length} bytes read back otherwise, rotational: {rotational}");
                opened.close().await;
            }
        }
    });
}

/// A chunk is never extended in place: a stage of units past its file is refused by name and
/// stages nothing, and an apply that would write past its file, as one over a file made smaller
/// beneath its record would, is refused with the file as it was, and the device not failed (F96)
#[test]
fn a_chunk_is_never_extended_in_place() {
    on_executor(|| async {
        let unit = u64::from(UNIT);
        // a chunk of one unit is a file of one unit, and a unit past it is refused
        let root = dir();
        let state = device(root.path(), ApplySync::Shared);
        let opened = slice(&state).await;
        stage_whole(&opened, label(1), 5, unit).await;
        opened.apply(chunk(), label(1)).await.expect("applied");
        let past = opened
            .stage_units(StageUnits {
                id: chunk(),
                makes: label(2),
                expects: label(1),
                also: Vec::new(),
                length: 2 * unit,
                units: vec![(1, bytes(6, UNIT as usize))],
            })
            .await;
        assert!(matches!(&past, Err(StoreError::Invalid(why)) if why.contains("staged whole")), "{past:?}");
        assert_eq!(opened.staged_count(), 0, "a refused stage left a record");
        opened.close().await;
        // a chunk of two units, a unit staged inside its file, and the file cut to one beneath it
        let root = dir();
        let state = device(root.path(), ApplySync::Shared);
        let opened = slice(&state).await;
        stage_whole(&opened, label(1), 7, 2 * unit).await;
        opened.apply(chunk(), label(1)).await.expect("applied");
        opened
            .stage_units(StageUnits {
                id: chunk(),
                makes: label(2),
                expects: label(1),
                also: Vec::new(),
                length: 2 * unit,
                units: vec![(1, bytes(8, UNIT as usize))],
            })
            .await
            .expect("a unit inside the file stages");
        let path = opened.chunk_path(&chunk());
        let cut = BLOCK as u64 + unit;
        std::fs::OpenOptions::new().write(true).open(&path).expect("opened").set_len(cut).expect("cut");
        let applied = opened.apply(chunk(), label(2)).await;
        assert!(matches!(&applied, Err(StoreError::Invalid(why)) if why.contains("never extended")), "{applied:?}");
        assert_eq!(std::fs::metadata(&path).expect("the file").len(), cut, "an apply extended a chunk's file");
        // the device still serves: what the cut file holds reads
        let read = opened.read(chunk(), 0, 10, label(1)).await;
        assert!(matches!(read, Ok(ReadAnswer::Bytes(_))), "{read:?}");
        opened.close().await;
    });
}

/// A reclaimed chunk's file is counted out at its own length, its class's, so the slice's figure
/// stays the bytes of the files it holds (F96)
#[test]
fn a_reclaimed_class_file_is_counted_out_by_its_length() {
    use crate::shared::protocol::peer::chunk::SealScope;
    use std::sync::atomic::Ordering;
    on_executor(|| async {
        let unit = u64::from(UNIT);
        let root = dir();
        let state = device(root.path(), ApplySync::Shared);
        let opened = slice(&state).await;
        // a whole chunk of one object and a chunk of one unit of another
        let short = ChunkId { object: 100, ..chunk() };
        for (id, length) in [(chunk(), CHUNK), (short, unit)] {
            opened
                .stage_whole(StageWhole {
                    id,
                    label: label(1),
                    chunk_bytes: CHUNK,
                    unit_bytes: UNIT,
                    length,
                    bytes: bytes(3, length as usize),
                })
                .await
                .expect("staged");
            opened.apply(id, label(1)).await.expect("applied");
        }
        let figure = |state: &Arc<DeviceState>| state.slices[0].chunk_bytes.load(Ordering::Relaxed);
        assert_eq!(figure(&state), 2 * BLOCK as u64 + CHUNK + unit);
        // the short one reclaimed under its seal: the whole one's file is what is left
        let scope = SealScope::Object {
            consumer: short.consumer,
            pg: short.pg,
            object: short.object,
        };
        opened.seal(scope, 5);
        assert_eq!(opened.reclaim(scope, 5).await, Ok(1));
        assert_eq!(figure(&state), BLOCK as u64 + CHUNK, "the reclaimed file was counted out at another length");
        opened.close().await;
    });
}

/// The names of every file under a slice's free pool, a length's directory at a time
///
/// # Arguments
///
/// * `root` - The device's directory
fn pool_files(root: &Path) -> Vec<String> {
    let pool = root.join("slice-0").join("free");
    std::fs::read_dir(pool)
        .into_iter()
        .flatten()
        .flatten()
        .flat_map(|length| std::fs::read_dir(length.path()).into_iter().flatten().flatten())
        .map(|file| file.file_name().to_string_lossy().into_owned())
        .collect()
}

/// A whole chunk's fresh file leaves its temporary name whole or not at all: a put's is renamed
/// over its chunk and a stage's into the pool under its record, an aborted or short writer's is
/// removed, and a file given back past the pool's target is removed rather than kept (F96)
#[test]
fn a_fresh_whole_leaves_no_temporary_and_the_pool_keeps_only_its_target() {
    on_executor(|| async {
        let root = dir();
        let state = device(root.path(), ApplySync::Shared);
        let opened = slice(&state).await;
        let temporaries = || pool_files(root.path()).iter().filter(|name| name.ends_with(".tmp")).count();
        let data = bytes(11, CHUNK as usize);
        // a put's chunk from a fresh file, renamed over its path
        let mut writer = opened
            .begin_whole(chunk(), Label::PUT, CHUNK, UNIT, CHUNK, true)
            .await
            .expect("begun");
        assert_eq!(temporaries(), 1, "a fresh file is not made under its temporary name");
        writer.piece(0, &[], &data).await.expect("piece");
        writer.finish(true).await.expect("placed");
        assert_eq!((temporaries(), opened.free_files()), (0, 0), "a placed chunk left a file");
        // an aborted writer's file is removed
        let writer = opened
            .begin_whole(chunk(), label(1), CHUNK, UNIT, CHUNK, true)
            .await
            .expect("begun");
        writer.abort().await;
        assert_eq!((temporaries(), opened.free_files()), (0, 0), "an aborted writer left a file");
        // and a writer given fewer units than its chunk holds
        let mut writer = opened
            .begin_whole(chunk(), label(1), CHUNK, UNIT, CHUNK, true)
            .await
            .expect("begun");
        writer.piece(0, &[], &data[..UNIT as usize]).await.expect("piece");
        assert!(matches!(writer.finish(false).await, Err(StoreError::Invalid(_))));
        assert_eq!((temporaries(), opened.free_files()), (0, 0), "a short writer left a file");
        // three whole chunks staged then discarded: the pool keeps two of their files, its target
        let ids: Vec<ChunkId> = (101..104).map(|object| ChunkId { object, ..chunk() }).collect();
        for id in &ids {
            opened
                .stage_whole(StageWhole {
                    id: *id,
                    label: label(2),
                    chunk_bytes: CHUNK,
                    unit_bytes: UNIT,
                    length: CHUNK,
                    bytes: data.clone(),
                })
                .await
                .expect("staged");
        }
        assert_eq!(temporaries(), 0, "a staged chunk's file kept its temporary name");
        for (index, id) in ids.iter().enumerate() {
            opened.discard(*id, label(2)).await.expect("discarded");
            assert_eq!(opened.free_files(), (index + 1).min(2));
        }
        assert_eq!(pool_files(root.path()).len(), 2, "the pool kept a file past its target");
        // the next whole chunk takes a file the pool kept, and makes none
        stage_whole(&opened, label(5), 12, CHUNK).await;
        assert_eq!((temporaries(), opened.free_files()), (0, 1));
        opened.close().await;
    });
}

/// A start after a crash at each step of a fresh whole file: one under its temporary name, just
/// made, allocated, or written whole, is removed; one adopted into the pool before a record named
/// it is the pool's; one a record names stays its stage's, and no number is made twice (F96)
#[test]
fn a_start_after_each_step_of_a_fresh_whole() {
    on_executor(|| async {
        let root = dir();
        let state = device(root.path(), ApplySync::Shared);
        let length = BLOCK as u64 + CHUNK;
        // a whole chunk staged, its file named by its record
        let data = {
            let opened = slice(&state).await;
            let data = stage_whole(&opened, label(1), 3, CHUNK).await;
            opened.close().await;
            data
        };
        // what a crash leaves at each step before a record names a file
        let pool = root.path().join("slice-0").join("free").join(length.to_string());
        std::fs::File::create(pool.join("0000000101.tmp")).expect("made");
        std::fs::File::create(pool.join("0000000102.tmp"))
            .and_then(|file| file.set_len(length))
            .expect("allocated");
        std::fs::write(pool.join("0000000103.tmp"), vec![0; length as usize]).expect("written");
        std::fs::write(pool.join("0000000104"), vec![0; length as usize]).expect("adopted");
        let opened = slice(&state).await;
        let names = pool_files(root.path());
        assert!(names.iter().all(|name| !name.ends_with(".tmp")), "a start kept a temporary file: {names:?}");
        assert_eq!(opened.free_files(), 1, "the adopted file is not the pool's");
        assert_eq!(opened.staged_labels(&chunk()), vec![label(1)]);
        // the staged chunk applies and reads, its file untouched by the start
        opened.apply(chunk(), label(1)).await.expect("applied");
        assert_eq!(
            opened.read(chunk(), 0, CHUNK, label(1)).await.expect("read"),
            ReadAnswer::Bytes(data)
        );
        // the pool's file is taken, and the fresh one after it is numbered past every file found
        let other = ChunkId { object: 7, ..chunk() };
        for made in [2, 3] {
            opened
                .stage_whole(StageWhole {
                    id: other,
                    label: label(made),
                    chunk_bytes: CHUNK,
                    unit_bytes: UNIT,
                    length: CHUNK,
                    bytes: bytes(made as u8, CHUNK as usize),
                })
                .await
                .expect("staged");
        }
        assert_eq!(opened.free_files(), 0);
        let mut names = pool_files(root.path());
        names.sort();
        assert_eq!(names, vec!["0000000104".to_string(), "0000000105".to_string()]);
        opened.close().await;
    });
}

/// A reader's read of units it read before is answered from its executor's cache without the
/// device, a range reaching a unit no read verified goes to the device and is kept in turn, a label
/// the chunk is not at is never answered from it, and a rebuild's or a move's read neither takes
/// from the cache nor fills it ([F102](../../../../../../docs/src/features/read-unit-cache.md))
#[test]
fn a_read_of_units_read_before_is_answered_without_the_device() {
    use crate::server::objects::store::cache::{CacheFigures, UnitCache};
    use crate::shared::protocol::peer::chunk::{ChunkReply, ChunkRequest};
    use std::sync::atomic::Ordering;
    on_executor(|| async {
        let root = dir();
        let device = device(root.path(), ApplySync::Each);
        let slice = slice(&device).await;
        let figures = Arc::new(CacheFigures::default());
        let cache = Rc::new(UnitCache::new(1 << 20, figures.clone()));
        slice.set_cache(cache.clone());
        let data = stage_whole(&slice, label(1), 3, CHUNK).await;
        slice.apply(chunk(), label(1)).await.expect("applied");
        let state = device.slices[0].clone();
        let ios = || state.read_ios.load(Ordering::Relaxed);
        // a recovery's read of the chunk reads the device and keeps nothing
        let background = ChunkRequest::Background(Box::new(ChunkRequest::Read {
            chunk: chunk(),
            offset: 0,
            len: CHUNK,
            labels: vec![label(1)],
        }));
        let reply = crate::server::objects::serve::serve(&slice, background).await;
        assert!(matches!(reply, ChunkReply::Bytes { .. }), "{reply:?}");
        assert_eq!(cache.units_of(state.id, &chunk()), 0, "a background read filled the cache");
        // a reader's first read is the device's, the unit it verified kept
        let read = ios();
        let first = slice.read_any(chunk(), 70_000, 1000, &[label(1)]).await.expect("read");
        assert_eq!(first, Ok((label(1), data[70_000..71_000].to_vec())));
        assert!(ios() > read);
        assert_eq!(cache.units_of(state.id, &chunk()), 1);
        // the second, of the same unit, is answered from the cache: no read reaches the device
        let read = ios();
        let second = slice.read_any(chunk(), 70_100, 500, &[label(1)]).await.expect("read");
        assert_eq!(second, Ok((label(1), data[70_100..70_600].to_vec())));
        assert_eq!(ios(), read, "a read of a unit held went to the device");
        assert_eq!(figures.report().hits, 1);
        // a range reaching units no read verified goes to the device, and is kept in turn
        let whole = slice.read_any(chunk(), 0, CHUNK, &[label(1)]).await.expect("read");
        assert_eq!(whole, Ok((label(1), data.clone())));
        assert!(ios() > read);
        assert_eq!(cache.units_of(state.id, &chunk()), (CHUNK / u64::from(UNIT)) as usize);
        // and a label the chunk is not at is never answered from it
        let other = slice.read_any(chunk(), 0, 10, &[label(2)]).await.expect("read");
        assert_eq!(other, Err(Some(label(1))));
        slice.close().await;
    });
}

/// Every change the slice makes to a chunk drops what its executor's cache holds of it: units
/// applied in place, a whole chunk placed over it, the chunk set aside, and a reclamation
/// ([F102](../../../../../../docs/src/features/read-unit-cache.md))
#[test]
fn every_change_to_a_chunk_drops_its_cached_units() {
    use crate::server::objects::store::cache::{CacheFigures, UnitCache};
    use crate::shared::protocol::peer::chunk::SealScope;
    on_executor(|| async {
        let root = dir();
        let device = device(root.path(), ApplySync::Shared);
        let slice = slice(&device).await;
        let cache = Rc::new(UnitCache::new(1 << 20, Arc::new(CacheFigures::default())));
        slice.set_cache(cache.clone());
        let id = device.slices[0].id;
        let held = |chunk: &ChunkId| cache.units_of(id, chunk);
        let whole = (CHUNK / u64::from(UNIT)) as usize;
        // a whole chunk read whole: every unit held
        stage_whole(&slice, label(1), 5, CHUNK).await;
        slice.apply(chunk(), label(1)).await.expect("applied");
        slice.read_any(chunk(), 0, CHUNK, &[label(1)]).await.expect("read").expect("bytes");
        assert_eq!(held(&chunk()), whole);
        // units applied in place drop them
        slice
            .stage_units(StageUnits {
                id: chunk(),
                makes: label(2),
                expects: label(1),
                also: Vec::new(),
                length: CHUNK,
                units: vec![(1, bytes(11, UNIT as usize))],
            })
            .await
            .expect("staged");
        slice.apply(chunk(), label(2)).await.expect("applied");
        assert_eq!(held(&chunk()), 0, "an apply in place left the chunk's units");
        // a whole chunk placed over it drops them
        slice.read_any(chunk(), 0, CHUNK, &[label(2)]).await.expect("read").expect("bytes");
        assert_eq!(held(&chunk()), whole);
        stage_whole(&slice, label(3), 6, CHUNK).await;
        slice.apply(chunk(), label(3)).await.expect("applied");
        assert_eq!(held(&chunk()), 0, "a whole chunk placed over it left its units");
        // the chunk set aside drops them: its first unit made to fail in its file, since a chunk is
        // set aside only while its file still fails
        slice.read_any(chunk(), 0, CHUNK, &[label(3)]).await.expect("read").expect("bytes");
        assert_eq!(held(&chunk()), whole);
        let path = slice.chunk_path(&chunk());
        let mut file = std::fs::read(&path).expect("the chunk's file");
        file[BLOCK + 10] ^= 0x01;
        std::fs::write(&path, &file).expect("a unit flipped");
        let aside = slice.quarantine(&chunk(), Some(label(3)), &[0], "a test").await.expect("set aside");
        assert!(aside, "a chunk whose unit fails was not set aside");
        assert_eq!(held(&chunk()), 0, "a chunk set aside left its units");
        // and a reclamation drops another object's
        let other = ChunkId { object: 100, ..chunk() };
        slice
            .stage_whole(StageWhole {
                id: other,
                label: label(1),
                chunk_bytes: CHUNK,
                unit_bytes: UNIT,
                length: CHUNK,
                bytes: bytes(7, CHUNK as usize),
            })
            .await
            .expect("staged");
        slice.apply(other, label(1)).await.expect("applied");
        slice.read_any(other, 0, CHUNK, &[label(1)]).await.expect("read").expect("bytes");
        assert_eq!(held(&other), whole);
        let scope = SealScope::Object {
            consumer: other.consumer,
            pg: other.pg,
            object: other.object,
        };
        slice.seal(scope, 5);
        assert_eq!(slice.reclaim(scope, 5).await, Ok(1));
        assert_eq!(held(&other), 0, "a reclamation left the chunk's units");
        slice.close().await;
    });
}

/// Read a range on a slice as its executor does, and the span it found
///
/// # Arguments
///
/// * `slice` - The slice
/// * `offset` - Where the range starts
/// * `len` - How long it is
/// * `made` - The label read
async fn span_of(slice: &Rc<Slice>, offset: u64, len: u64, made: Label) -> super::Span {
    match slice.read_inner(chunk(), offset, len, &[made], true, false, false).await.expect("read") {
        (_, Found::Bytes(span)) => span,
        (_, Found::Label(have)) => panic!("{made:?} was not made, the chunk holds {have:?}"),
    }
}

/// A range every unit of which its base holds is answered where the device put it, never copied;
/// one a unit holding no bytes or a record gives any of is laid out once in a buffer of its own;
/// either way the bytes are the label's; and a unit that fails its checksum in the middle of a
/// range is refused and its chunk set aside (F105)
#[test]
fn a_span_is_answered_where_the_device_put_it() {
    use crate::server::objects::store::StoreError;
    on_executor(|| async {
        for rotational in [false, true] {
            let root = dir();
            let journal = dir();
            let state = if rotational {
                disk(root.path(), journal.path())
            } else {
                device(root.path(), ApplySync::Shared)
            };
            let opened = slice(&state).await;
            let unit = UNIT as usize;
            // a chunk whose first three units hold bytes, the fourth none
            let data = stage_whole(&opened, label(1), 5, 3 * u64::from(UNIT)).await;
            opened.apply(chunk(), label(1)).await.expect("applied");
            // a range of what its base holds, from part way into a unit, where the device put it
            let span = span_of(&opened, 100, 2 * u64::from(UNIT), label(1)).await;
            assert!(span.in_place(), "rotational {rotational}: a range of the base was copied");
            assert_eq!(span.into_vec().await, data[100..100 + 2 * unit], "rotational {rotational}");
            // a range reaching the unit that holds no bytes is laid out, its end zeros
            let span = span_of(&opened, 2 * u64::from(UNIT), 2 * u64::from(UNIT), label(1)).await;
            assert!(!span.in_place(), "rotational {rotational}: zeros were answered from a device");
            let mut expected = data[2 * unit..].to_vec();
            expected.resize(2 * unit, 0);
            assert_eq!(span.into_vec().await, expected, "rotational {rotational}");
            // a unit staged over the base is laid over what the run read
            let staged = bytes(9, unit);
            opened
                .stage_units(StageUnits {
                    id: chunk(),
                    makes: label(2),
                    expects: label(1),
                    also: Vec::new(),
                    length: 3 * u64::from(UNIT),
                    units: vec![(1, staged.clone())],
                })
                .await
                .expect("staged");
            let span = span_of(&opened, 0, 3 * u64::from(UNIT), label(2)).await;
            assert!(!span.in_place(), "rotational {rotational}: a staged unit was not laid over");
            let mut expected = data.clone();
            expected[unit..2 * unit].copy_from_slice(&staged);
            assert_eq!(span.into_vec().await, expected, "rotational {rotational}");
            // a byte flipped in the middle unit of the base is found as the range is read
            opened.discard(chunk(), label(2)).await.expect("discarded");
            let path = opened.chunk_path(&chunk());
            let mut file = std::fs::read(&path).expect("the chunk's file");
            file[BLOCK + unit + 7] ^= 0x40;
            std::fs::write(&path, file).expect("written");
            let read = opened.read(chunk(), 0, 3 * u64::from(UNIT), label(1)).await;
            assert_eq!(read, Err(StoreError::Corrupt { unit: 1 }), "rotational {rotational}");
            assert_eq!(opened.quarantined().len(), 1, "rotational {rotational}: the chunk was not set aside");
            opened.close().await;
        }
    });
}

/// A range longer than a piece is one run of two pieces, each the device's, cut from the run's start,
/// and reads back the label's bytes on either medium (F105)
#[test]
fn a_run_of_several_pieces_is_answered_whole() {
    /// A chunk of 32 units, two pieces of a run
    const BIG: u64 = 32 * UNIT as u64;
    on_executor(|| async {
        for rotational in [false, true] {
            let root = dir();
            let journal = dir();
            let state = if rotational {
                disk(root.path(), journal.path())
            } else {
                device(root.path(), ApplySync::Shared)
            };
            let opened = Slice::open(
                state.clone(),
                state.slices[0].clone(),
                SliceOptions {
                    segment_bytes: 4 << 20,
                    free_files: 2,
                    chunk_lengths: vec![BIG],
                    budget: None,
                    keep_previous: true,
                    apply_queue: None,
                },
                None,
            )
            .await
            .expect("the slice opens");
            let data = bytes(11, BIG as usize);
            opened
                .stage_whole(StageWhole {
                    id: chunk(),
                    label: label(1),
                    chunk_bytes: BIG,
                    unit_bytes: UNIT,
                    length: BIG,
                    bytes: data.clone(),
                })
                .await
                .expect("staged");
            opened.apply(chunk(), label(1)).await.expect("applied");
            // from half a unit in to half a unit past the seventeenth: a run of eighteen units, a
            // piece of sixteen and one of two
            let offset = u64::from(UNIT) / 2;
            let len = 17 * u64::from(UNIT);
            let span = span_of(&opened, offset, len, label(1)).await;
            assert!(span.in_place(), "rotational {rotational}");
            assert_eq!(span.parts().len(), 2, "rotational {rotational}: the run was not two pieces");
            assert_eq!(
                span.into_vec().await,
                data[offset as usize..(offset + len) as usize],
                "rotational {rotational}"
            );
            opened.close().await;
        }
    });
}

/// A span holds its read's draw on its executor's budget until it is dropped, so a range read and
/// not yet answered is counted (F105)
#[test]
fn a_held_span_keeps_its_draw() {
    on_executor(|| async {
        let root = dir();
        let state = device(root.path(), ApplySync::Shared);
        let budget = crate::server::objects::budget::Budget::new(64 << 20);
        let opened = Slice::open(
            state.clone(),
            state.slices[0].clone(),
            SliceOptions {
                segment_bytes: 1 << 20,
                free_files: 2,
                chunk_lengths: vec![CHUNK],
                budget: Some(budget.clone()),
                keep_previous: true,
                apply_queue: None,
            },
            None,
        )
        .await
        .expect("the slice opens");
        stage_whole(&opened, label(1), 3, CHUNK).await;
        opened.apply(chunk(), label(1)).await.expect("applied");
        assert_eq!(budget.used(), 0, "a stage and an apply left a draw behind");
        // the span holds the range's draw, and gives it back once dropped
        let span = span_of(&opened, 0, CHUNK, label(1)).await;
        assert!(budget.used() >= CHUNK, "a held span counted {} of {CHUNK}", budget.used());
        drop(span);
        assert_eq!(budget.used(), 0, "a dropped span kept its draw");
        opened.close().await;
    });
}

/// A chain of records each staged over the one before, nine hundred deep, reads at its last label
/// before it applies, and applied there applies every record beneath it first, in order: both walks
/// nested a call for each record, and a chain this deep overflowed its executor's stack (item 313)
#[test]
fn a_long_chain_of_records_applies_in_order() {
    on_executor(|| async move {
        let root = dir();
        let device = device(root.path(), ApplySync::Shared);
        // a slice whose journal holds the whole chain, of a chunk of four 4 KiB units
        let unit = 4096usize;
        let chunk_bytes = 4 * unit as u64;
        let slice = Slice::open(
            device.clone(),
            device.slices[0].clone(),
            SliceOptions {
                segment_bytes: 16 << 20,
                free_files: 2,
                chunk_lengths: vec![chunk_bytes],
                budget: None,
                keep_previous: true,
                apply_queue: None,
            },
            None,
        )
        .await
        .expect("the slice opens");
        let mut expected = bytes(1, chunk_bytes as usize);
        slice
            .stage_whole(StageWhole {
                id: chunk(),
                label: label(1),
                chunk_bytes,
                unit_bytes: unit as u32,
                length: chunk_bytes,
                bytes: expected.clone(),
            })
            .await
            .expect("staged whole");
        slice.apply(chunk(), label(1)).await.expect("applied whole");
        // nine hundred records, each a unit over the label before, none applied
        let depth = 900u64;
        for made in 2..=depth + 1 {
            let index = (made % 4) as u32;
            let value = bytes((made % 251) as u8, unit);
            expected[index as usize * unit..(index as usize + 1) * unit].copy_from_slice(&value);
            slice
                .stage_units(StageUnits {
                    id: chunk(),
                    makes: label(made),
                    expects: label(made - 1),
                    also: Vec::new(),
                    length: chunk_bytes,
                    units: vec![(index, value)],
                })
                .await
                .expect("staged over the one before");
        }
        // a read at the last label walks every record down to the chunk
        let before = slice.read(chunk(), 0, chunk_bytes, label(depth + 1)).await.expect("read");
        assert!(before == ReadAnswer::Bytes(expected.clone()), "the chain read other bytes");
        // the last applied, and every record beneath it with it
        slice.apply(chunk(), label(depth + 1)).await.expect("the chain applied");
        let read = slice.read(chunk(), 0, chunk_bytes, label(depth + 1)).await.expect("read");
        assert!(read == ReadAnswer::Bytes(expected), "the chain applied out of order");
        slice.close().await;
    });
}
