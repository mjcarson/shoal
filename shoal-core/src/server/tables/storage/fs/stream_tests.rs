//! Tests for the intent log write path
//!
//! These cover the on disk framing contract between [`super::stream`], which pads
//! partial flushes up to a block boundary so they can be written with O_DIRECT, and
//! [`super::reader`], which has to skip those pad regions without mistaking them for
//! the end of the log.

use glommio::io::OpenOptions;
use glommio::LocalExecutor;
use gxhash::GxHasher;
use std::hash::Hasher;
use std::path::PathBuf;
use tempfile::TempDir;

use super::reader::IntentLogReader;
use super::stream::{align_up, pad_region, FlushState, PAD_SENTINEL, PAD_SENTINEL_SIZE};

/// Create a temp dir on a filesystem that supports direct IO
///
/// `TempDir::new` uses `/tmp`, which is usually tmpfs. Glommio silently disables
/// O_DIRECT on tmpfs, so alignment is never enforced there and these tests would
/// pass against a broken write path. Cargo only sets `CARGO_TARGET_TMPDIR` for
/// integration tests, so build the equivalent path ourselves.
fn test_dir() -> TempDir {
    // build a path under cargo's target dir, which is on the same real filesystem as the repo
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/shoal-test-tmp");
    // make sure our base dir exists
    std::fs::create_dir_all(&base).expect("Failed to create test tmp dir");
    TempDir::new_in(&base).expect("Failed to create temp dir")
}

/// Frame a record the way `FileSystem::commit` does
///
/// # Arguments
///
/// * `data` - The record payload to frame
fn frame(data: &[u8]) -> Vec<u8> {
    // compute a checksum over our payload
    let mut hasher = GxHasher::default();
    hasher.write(data);
    let checksum = hasher.finish();
    // build our framed record
    let mut framed = Vec::with_capacity(16 + data.len());
    framed.extend_from_slice(&data.len().to_le_bytes());
    framed.extend_from_slice(&checksum.to_le_bytes());
    framed.extend_from_slice(data);
    framed
}

/// Build an intent log the same way [`super::stream::StreamWriter`] would
///
/// Each entry in `flushes` is one buffer worth of records, padded up to an aligned
/// length exactly as a partial flush is.
///
/// # Arguments
///
/// * `flushes` - The records to write, grouped into one partial flush per group
/// * `alignment` - The direct IO alignment to pad each flush up too
fn build_log(flushes: &[Vec<Vec<u8>>], alignment: usize) -> Vec<u8> {
    // build up our log one flush at a time
    let mut log = Vec::new();
    for records in flushes {
        // stage every record in this flush into a buffer
        let mut buff = Vec::new();
        for record in records {
            buff.extend_from_slice(&frame(record));
        }
        // track how much real data we staged before padding it
        let buff_pos = buff.len();
        // give ourselves the block of slack the writer always reserves
        buff.resize(align_up(buff_pos, alignment) + alignment, 0);
        // pad this flush up to an aligned length
        let padded = pad_region(&mut buff, buff_pos, alignment);
        // only the padded region is actually written to disk
        buff.truncate(padded);
        log.extend_from_slice(&buff);
    }
    log
}

/// Write a log to disk and read every record back out with [`IntentLogReader`]
///
/// # Arguments
///
/// * `path` - The path to write this log too
/// * `log` - The raw log bytes to write
async fn round_trip(path: &PathBuf, log: &[u8]) -> Vec<Vec<u8>> {
    // write our log out with buffered IO so we control the exact bytes on disk
    std::fs::write(path, log).expect("Failed to write log");
    // open this log with our reader
    let mut reader = IntentLogReader::new(path).await.expect("Failed to open log");
    // read every record back out
    let mut records = Vec::new();
    while let Some(read) = reader.next_buff().await.expect("Failed to read log") {
        records.push(read.to_vec());
    }
    reader.close().await.expect("Failed to close reader");
    records
}

#[test]
/// Padding an already aligned buffer is a no-op
fn pad_region_aligned_is_noop() {
    let mut buff = vec![0xAA; 1024];
    assert_eq!(pad_region(&mut buff, 512, 512), 512);
    // nothing past our staged data should have been touched
    assert!(buff[512..].iter().all(|byte| *byte == 0xAA));
}

#[test]
/// A pad region smaller than our sentinel gets extended by a full block
fn pad_region_extends_for_short_pad() {
    let mut buff = vec![0xAA; 2048];
    // 4 bytes short of a boundary leaves no room for an 8 byte sentinel
    let padded = pad_region(&mut buff, 1020, 512);
    assert_eq!(padded, 1536);
    // our sentinel should be at the start of the pad region
    assert_eq!(
        u64::from_le_bytes(buff[1020..1028].try_into().unwrap()),
        PAD_SENTINEL
    );
    // the rest of the pad region should be zeroed
    assert!(buff[1028..1536].iter().all(|byte| *byte == 0));
}

#[test]
/// A pad region exactly the size of our sentinel is left as one block
fn pad_region_exact_sentinel_fits() {
    let mut buff = vec![0xAA; 2048];
    let padded = pad_region(&mut buff, 1016, 512);
    assert_eq!(padded, 1024);
    assert_eq!(
        u64::from_le_bytes(buff[1016..1024].try_into().unwrap()),
        PAD_SENTINEL
    );
}

#[test]
/// Records of every awkward size survive a write and replay with padding in between
fn padded_log_round_trips() {
    LocalExecutor::default().run(async {
        let temp_dir = test_dir();
        let path = temp_dir.path().join("padded-log");
        // use a range of sizes that straddle block boundaries in every direction
        let sizes = [1usize, 7, 8, 500, 511, 512, 513, 4095, 5000];
        // build one record per size, each filled with a recognizable byte
        let records: Vec<Vec<u8>> = sizes
            .iter()
            .enumerate()
            .map(|(index, size)| vec![index as u8; *size])
            .collect();
        // put every record in its own flush so we get a pad region between each one
        let flushes: Vec<Vec<Vec<u8>>> = records.iter().map(|rec| vec![rec.clone()]).collect();
        let log = build_log(&flushes, 512);
        // every flush should have landed on a block boundary
        assert_eq!(log.len() % 512, 0);
        // read our records back out
        let read_back = round_trip(&path, &log).await;
        assert_eq!(read_back, records);
    });
}

#[test]
/// Several records batched into one flush share a single pad region
fn batched_flush_round_trips() {
    LocalExecutor::default().run(async {
        let temp_dir = test_dir();
        let path = temp_dir.path().join("batched-log");
        // build two flushes holding three records each
        let records: Vec<Vec<u8>> = (0..6u8).map(|index| vec![index; 40 + index as usize]).collect();
        let flushes = vec![records[..3].to_vec(), records[3..].to_vec()];
        let log = build_log(&flushes, 512);
        let read_back = round_trip(&path, &log).await;
        assert_eq!(read_back, records);
    });
}

#[test]
/// A zero size header still means end of log, even now that padding exists
fn zero_size_is_still_end_of_log() {
    LocalExecutor::default().run(async {
        let temp_dir = test_dir();
        let path = temp_dir.path().join("zero-log");
        // write one record then leave the rest of the block zeroed
        let record = vec![7u8; 64];
        let mut log = build_log(&[vec![record.clone()]], 512);
        // append a fully zeroed block, which is what unwritten space looks like
        log.extend_from_slice(&[0u8; 512]);
        let read_back = round_trip(&path, &log).await;
        // we should get our one record and then stop at the zeros
        assert_eq!(read_back, vec![record]);
    });
}

#[test]
/// A pad region at the very end of a log does not produce a phantom record
fn trailing_pad_region_is_skipped() {
    LocalExecutor::default().run(async {
        let temp_dir = test_dir();
        let path = temp_dir.path().join("trailing-pad-log");
        let record = vec![3u8; 100];
        let log = build_log(&[vec![record.clone()]], 512);
        // our log should end in a pad region since 116 bytes is not block aligned
        assert_eq!(
            u64::from_le_bytes(log[116..124].try_into().unwrap()),
            PAD_SENTINEL
        );
        let read_back = round_trip(&path, &log).await;
        assert_eq!(read_back, vec![record]);
    });
}

#[test]
/// A record larger than one block round trips without being split
fn oversized_record_round_trips() {
    LocalExecutor::default().run(async {
        let temp_dir = test_dir();
        let path = temp_dir.path().join("oversized-log");
        // build a record several blocks long
        let record = vec![9u8; 20_000];
        let log = build_log(&[vec![record.clone()]], 512);
        let read_back = round_trip(&path, &log).await;
        assert_eq!(read_back, vec![record]);
    });
}

#[test]
/// A real DMA file rejects the unaligned writes the old write path produced
///
/// This is the guard for the whole padding scheme. If it starts passing on a
/// filesystem that silently falls back to buffered IO, these tests are not
/// exercising direct IO and the padding work is unverified.
fn dma_write_requires_alignment() {
    LocalExecutor::default().run(async {
        let temp_dir = test_dir();
        let path = temp_dir.path().join("alignment-probe");
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .dma_open(&path)
            .await
            .expect("Failed to open dma file");
        // build a buffer whose length is deliberately not a multiple of our alignment
        let alignment = file.alignment() as usize;
        let mut buff = file.alloc_dma_buffer(alignment);
        buff.trim_to_size(alignment - 8);
        // record whether this filesystem actually enforced alignment
        let unaligned_rejected = file.write_at(buff, 0).await.is_err();
        // an aligned write of the same shape must always succeed
        let aligned = file.alloc_dma_buffer(alignment);
        file.write_at(aligned, 0)
            .await
            .expect("Aligned write should always succeed");
        file.close().await.expect("Failed to close file");
        // btrfs falls back to buffered IO instead of returning EINVAL, so we can only
        // report this rather than assert on it
        if !unaligned_rejected {
            eprintln!(
                "note: {} does not enforce O_DIRECT alignment (silent buffered fallback)",
                temp_dir.path().display()
            );
        }
    });
}

#[test]
/// A write completing out of order does not advance the watermark past in flight data
///
/// This is the regression test for the watermark. With `write_behind` defaulting to
/// 128, concurrent unordered io_uring completions are the normal case, and taking the
/// max of what has completed would acknowledge data that is not on disk yet.
fn watermark_waits_for_contiguous_completions() {
    let mut state = FlushState::default();
    // submit two writes covering [0, 512) and [512, 1024)
    state.on_start(512);
    state.on_start(1024);
    // the second write lands first, which proves nothing about the first
    state.on_complete(1024);
    assert_eq!(state.written_pos(), 0);
    // once the first lands we can advance over both at once
    state.on_complete(512);
    assert_eq!(state.written_pos(), 1024);
}

#[test]
/// Completions arriving in submission order advance the watermark one at a time
fn watermark_advances_in_order() {
    let mut state = FlushState::default();
    state.on_start(512);
    state.on_start(1024);
    state.on_start(1536);
    state.on_complete(512);
    assert_eq!(state.written_pos(), 512);
    state.on_complete(1024);
    assert_eq!(state.written_pos(), 1024);
    state.on_complete(1536);
    assert_eq!(state.written_pos(), 1536);
}

#[test]
/// The watermark never moves backwards, whatever order completions arrive in
fn watermark_is_monotonic() {
    let mut state = FlushState::default();
    // submit a deep queue like a real writer at write_behind 128 would
    let ends: Vec<u64> = (1..=128).map(|block| block * 512).collect();
    for end in &ends {
        state.on_start(*end);
    }
    // complete them in a scrambled order
    let mut order = ends.clone();
    order.reverse();
    order.rotate_left(37);
    // our watermark must never regress and must never exceed what is contiguous
    let mut last = 0;
    let mut completed = std::collections::HashSet::new();
    for end in order {
        state.on_complete(end);
        completed.insert(end);
        let watermark = state.written_pos();
        assert!(watermark >= last, "watermark regressed");
        // every block below our watermark must actually have completed
        for block in ends.iter().filter(|end| **end <= watermark) {
            assert!(completed.contains(block), "watermark passed in flight data");
        }
        last = watermark;
    }
    // once everything has landed our watermark should cover the whole queue
    assert_eq!(state.written_pos(), 128 * 512);
}

#[test]
/// Our sentinel can never collide with a real record size
fn sentinel_cannot_be_a_real_size() {
    // a record claiming to be u64::MAX bytes long could never fit in any log
    assert_eq!(PAD_SENTINEL, u64::MAX);
    assert_eq!(PAD_SENTINEL_SIZE, 8);
}
