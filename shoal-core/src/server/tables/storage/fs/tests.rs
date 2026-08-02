//! Tests for the FileSystem storage engine
//!
//! These verify the corruption mitigation measures work correctly:
//! - Intent log reader handles truncated files gracefully
//! - Checksums detect corrupted intent log entries
//! - Pad regions are skipped rather than mistaken for the end of a log
//! - `SerializedMap` checksum detects corruption
//! - Inactive intent log discovery works correctly
//!
//! Fixtures are written with plain `std::fs` rather than a DMA writer so the exact
//! byte layout under test is explicit, which matters because these tests are all
//! about how the reader reacts to malformed layouts.

use glommio::io::OpenOptions;
use glommio::LocalExecutor;
use gxhash::GxHasher;
use std::hash::Hasher;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

use super::reader::IntentLogReader;
use super::stream::PAD_SENTINEL;
use super::find_inactive_intent_logs;

/// Create a temp dir on a filesystem that supports direct IO
///
/// `TempDir::new` uses `/tmp`, which is usually tmpfs. Glommio silently disables
/// O_DIRECT on tmpfs, so these tests would run against a buffered write path.
fn test_dir() -> TempDir {
    // build a path under cargo's target dir, which is on a real filesystem
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/shoal-test-tmp");
    // make sure our base dir exists
    std::fs::create_dir_all(&base).expect("Failed to create test tmp dir");
    TempDir::new_in(&base).expect("Failed to create temp dir")
}

/// Frame a valid intent log entry of `[8-byte size][8-byte checksum][data]`
///
/// # Arguments
///
/// * `data` - The record payload to frame
fn entry(data: &[u8]) -> Vec<u8> {
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

/// Frame an entry with a deliberately wrong checksum
///
/// # Arguments
///
/// * `data` - The record payload to frame
/// * `checksum` - The wrong checksum to write
fn bad_entry(data: &[u8], checksum: u64) -> Vec<u8> {
    let mut framed = Vec::with_capacity(16 + data.len());
    framed.extend_from_slice(&data.len().to_le_bytes());
    framed.extend_from_slice(&checksum.to_le_bytes());
    framed.extend_from_slice(data);
    framed
}

/// Read every record an intent log yields
///
/// # Arguments
///
/// * `path` - The intent log to read
async fn read_all(path: &Path) -> Vec<Vec<u8>> {
    // open this log with our reader
    let mut reader = IntentLogReader::new(&path.to_path_buf())
        .await
        .expect("Failed to open log");
    // read every record it will give us
    let mut records = Vec::new();
    while let Some(read) = reader.next_buff().await.expect("Failed to read log") {
        records.push(read.to_vec());
    }
    reader.close().await.expect("Failed to close reader");
    records
}

// ========================================================================
// IntentLogReader tests
// ========================================================================

#[test]
/// An empty intent log yields no records
fn reader_empty_file() {
    LocalExecutor::default().run(async {
        let temp_dir = test_dir();
        let path = temp_dir.path().join("empty-log");
        std::fs::write(&path, []).unwrap();
        assert!(read_all(&path).await.is_empty());
    });
}

#[test]
/// Every valid entry is read back in order
fn reader_valid_entries() {
    LocalExecutor::default().run(async {
        let temp_dir = test_dir();
        let path = temp_dir.path().join("valid-log");
        let entries: Vec<Vec<u8>> = vec![
            b"hello world".to_vec(),
            b"second entry with more data".to_vec(),
            b"third".to_vec(),
        ];
        // write every entry back to back
        let mut log = Vec::new();
        for data in &entries {
            log.extend_from_slice(&entry(data));
        }
        std::fs::write(&path, &log).unwrap();
        assert_eq!(read_all(&path).await, entries);
    });
}

#[test]
/// A partial size header stops the log rather than erroring
fn reader_truncated_size_header() {
    LocalExecutor::default().run(async {
        let temp_dir = test_dir();
        let path = temp_dir.path().join("truncated-size-log");
        let data = b"valid entry";
        // one good entry followed by 4 bytes of a size header
        let mut log = entry(data);
        log.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        std::fs::write(&path, &log).unwrap();
        // we should get our one good entry and then stop
        assert_eq!(read_all(&path).await, vec![data.to_vec()]);
    });
}

#[test]
/// An entry claiming more data than the file holds stops the log
fn reader_truncated_data() {
    LocalExecutor::default().run(async {
        let temp_dir = test_dir();
        let path = temp_dir.path().join("truncated-data-log");
        // claim 1000 bytes but only write 10
        let mut log = Vec::new();
        log.extend_from_slice(&1000usize.to_le_bytes());
        log.extend_from_slice(&0u64.to_le_bytes());
        log.extend_from_slice(&[0u8; 10]);
        std::fs::write(&path, &log).unwrap();
        assert!(read_all(&path).await.is_empty());
    });
}

#[test]
/// A size header pointing past EOF stops the log
fn reader_size_exceeds_file() {
    LocalExecutor::default().run(async {
        let temp_dir = test_dir();
        let path = temp_dir.path().join("oversize-log");
        std::fs::write(&path, 999_999usize.to_le_bytes()).unwrap();
        assert!(read_all(&path).await.is_empty());
    });
}

#[test]
/// A zero size header means the end of the log
///
/// This is what unwritten space in a partly filled log looks like, and it has to
/// stay distinct from a pad region.
fn reader_zero_size() {
    LocalExecutor::default().run(async {
        let temp_dir = test_dir();
        let path = temp_dir.path().join("zero-size-log");
        std::fs::write(&path, 0usize.to_le_bytes()).unwrap();
        assert!(read_all(&path).await.is_empty());
    });
}

#[test]
/// A pad region is skipped rather than treated as the end of the log
///
/// This is the counterpart to `reader_zero_size`. A partial flush is padded up to a
/// block boundary so it can be written with O_DIRECT, and that padding must not
/// truncate replay.
fn reader_pad_sentinel() {
    LocalExecutor::default().run(async {
        let temp_dir = test_dir();
        let path = temp_dir.path().join("pad-sentinel-log");
        let first = b"before the pad".to_vec();
        let second = b"after the pad".to_vec();
        // write one entry, pad out to a 512 byte boundary, then write another
        let mut log = entry(&first);
        log.extend_from_slice(&PAD_SENTINEL.to_le_bytes());
        log.resize(512, 0);
        log.extend_from_slice(&entry(&second));
        std::fs::write(&path, &log).unwrap();
        // both entries should survive the pad region between them
        assert_eq!(read_all(&path).await, vec![first, second]);
    });
}

#[test]
/// Several pad regions in a row are all skipped
fn reader_consecutive_pad_regions() {
    LocalExecutor::default().run(async {
        let temp_dir = test_dir();
        let path = temp_dir.path().join("multi-pad-log");
        let record = b"survives many pads".to_vec();
        // three empty padded flushes followed by a real record
        let mut log = Vec::new();
        for _ in 0..3 {
            let start = log.len();
            log.extend_from_slice(&PAD_SENTINEL.to_le_bytes());
            log.resize(start + 512, 0);
        }
        log.extend_from_slice(&entry(&record));
        std::fs::write(&path, &log).unwrap();
        assert_eq!(read_all(&path).await, vec![record]);
    });
}

#[test]
/// A bad checksum stops the log
fn reader_bad_checksum() {
    LocalExecutor::default().run(async {
        let temp_dir = test_dir();
        let path = temp_dir.path().join("bad-checksum-log");
        std::fs::write(&path, bad_entry(b"some data here", 0xDEAD_BEEF)).unwrap();
        assert!(read_all(&path).await.is_empty());
    });
}

#[test]
/// A bad checksum discards everything after it, including valid entries
///
/// This is deliberate: a corrupt record means we cannot trust that the records
/// after it are the ones that were actually committed next.
fn reader_good_then_bad_then_good() {
    LocalExecutor::default().run(async {
        let temp_dir = test_dir();
        let path = temp_dir.path().join("mixed-checksum-log");
        let good = b"good entry".to_vec();
        // one good entry, one with a bad checksum, then another good one
        let mut log = entry(&good);
        log.extend_from_slice(&bad_entry(b"bad entry", 0x0BAD_BAD_BAD));
        log.extend_from_slice(&entry(b"also good"));
        std::fs::write(&path, &log).unwrap();
        // only the entry before the corruption comes back
        assert_eq!(read_all(&path).await, vec![good]);
    });
}

/// Read an intent log and report whether the reader gave up on a damaged entry
///
/// # Arguments
///
/// * `path` - The intent log to read
async fn read_all_truncated(path: &Path) -> bool {
    // open this log with our reader
    let mut reader = IntentLogReader::new(&path.to_path_buf())
        .await
        .expect("Failed to open log");
    // read every record it will give us
    while reader.next_buff().await.expect("Failed to read log").is_some() {}
    // note whether it stopped on damage before closing it
    let truncated = reader.truncated;
    reader.close().await.expect("Failed to close reader");
    truncated
}

#[test]
/// A log that ends in damage is flagged as truncated
///
/// Every one of these stops the read the same way a clean end of log does, so
/// without the flag a caller cannot tell that anything was lost.
fn reader_flags_damaged_tails() {
    LocalExecutor::default().run(async {
        let temp_dir = test_dir();
        // a partial size header
        let size_header = temp_dir.path().join("flag-truncated-size");
        let mut log = entry(b"valid entry");
        log.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        std::fs::write(&size_header, &log).unwrap();
        assert!(read_all_truncated(&size_header).await);
        // an entry claiming more data than was written
        let short_data = temp_dir.path().join("flag-truncated-data");
        let mut log = Vec::new();
        log.extend_from_slice(&1000usize.to_le_bytes());
        log.extend_from_slice(&0u64.to_le_bytes());
        log.extend_from_slice(&[0u8; 10]);
        std::fs::write(&short_data, &log).unwrap();
        assert!(read_all_truncated(&short_data).await);
        // a size header pointing past the end of the file
        let oversize = temp_dir.path().join("flag-oversize");
        std::fs::write(&oversize, 999_999usize.to_le_bytes()).unwrap();
        assert!(read_all_truncated(&oversize).await);
        // an entry whose checksum does not match its data
        let checksum = temp_dir.path().join("flag-bad-checksum");
        std::fs::write(&checksum, bad_entry(b"some data here", 0xDEAD_BEEF)).unwrap();
        assert!(read_all_truncated(&checksum).await);
    });
}

#[test]
/// A log that ends the way a healthy log ends is not flagged as truncated
///
/// This is the half that matters. A padded log, an empty log and a partly filled
/// one all stop early by design, and flagging any of them would report data loss
/// on every clean startup.
fn reader_does_not_flag_healthy_tails() {
    LocalExecutor::default().run(async {
        let temp_dir = test_dir();
        // a log with nothing in it at all
        let empty = temp_dir.path().join("flag-empty");
        std::fs::write(&empty, []).unwrap();
        assert!(!read_all_truncated(&empty).await);
        // a log read all the way to its end
        let valid = temp_dir.path().join("flag-valid");
        std::fs::write(&valid, entry(b"hello world")).unwrap();
        assert!(!read_all_truncated(&valid).await);
        // unwritten space in a partly filled log
        let zero = temp_dir.path().join("flag-zero-size");
        std::fs::write(&zero, 0usize.to_le_bytes()).unwrap();
        assert!(!read_all_truncated(&zero).await);
        // the padding a partial flush is rounded up with
        let padded = temp_dir.path().join("flag-padded");
        let mut log = entry(b"before the pad");
        log.extend_from_slice(&PAD_SENTINEL.to_le_bytes());
        log.resize(512, 0);
        log.extend_from_slice(&entry(b"after the pad"));
        std::fs::write(&padded, &log).unwrap();
        assert!(!read_all_truncated(&padded).await);
    });
}

// ========================================================================
// Inactive intent log discovery tests
// ========================================================================

#[test]
/// An empty dir holds no inactive logs
fn find_inactive_intent_logs_empty_dir() {
    let temp_dir = test_dir();
    let result = find_inactive_intent_logs(&temp_dir.path().to_path_buf(), "shard-1");
    assert!(result.is_empty());
}

#[test]
/// Inactive logs are found for the right shard and sorted by generation
fn find_inactive_intent_logs_finds_and_sorts() {
    let temp_dir = test_dir();
    // create inactive log files in non-sequential order
    for generation in ["3", "0", "7", "1"] {
        std::fs::write(
            temp_dir.path().join(format!("shard-1-inactive-{generation}")),
            "",
        )
        .unwrap();
    }
    // also create some files that should not be picked up
    std::fs::write(temp_dir.path().join("shard-1-active"), "").unwrap();
    std::fs::write(temp_dir.path().join("shard-2-inactive-5"), "").unwrap();
    std::fs::write(temp_dir.path().join("unrelated-file"), "").unwrap();
    let result = find_inactive_intent_logs(&temp_dir.path().to_path_buf(), "shard-1");
    // verify we found exactly our shards logs, sorted by generation ascending
    let generations: Vec<u64> = result.iter().map(|(generation, _)| *generation).collect();
    assert_eq!(generations, vec![0, 1, 3, 7]);
}

#[test]
/// Files with a non numeric generation are ignored
fn find_inactive_intent_logs_ignores_non_numeric_gen() {
    let temp_dir = test_dir();
    for name in ["shard-1-inactive-abc", "shard-1-inactive-", "shard-1-inactive-2"] {
        std::fs::write(temp_dir.path().join(name), "").unwrap();
    }
    let result = find_inactive_intent_logs(&temp_dir.path().to_path_buf(), "shard-1");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, 2);
}

#[test]
/// A missing dir yields no logs rather than an error
fn find_inactive_intent_logs_nonexistent_dir() {
    let path = PathBuf::from("/tmp/nonexistent-shoal-test-dir-12345");
    let result = find_inactive_intent_logs(&path, "shard-1");
    assert!(result.is_empty());
}

// ========================================================================
// SerializedMap checksum tests
// ========================================================================

#[test]
/// A corrupt archive map is detected by its checksum before rkyv is trusted
fn map_corrupt_hash() {
    use crate::server::errors::ShoalError;
    use crate::server::tables::storage::fs::map::SerializedMap;

    LocalExecutor::default().run(async {
        let temp_dir = test_dir();
        let map_path = temp_dir.path().join("test-map");
        let intent_path = temp_dir.path().join("test-map-intent");
        // create an empty intent file
        let intent_file = OpenOptions::new()
            .create(true)
            .write(true)
            .dma_open(&intent_path)
            .await
            .unwrap();
        intent_file.close().await.unwrap();
        // write a map with a hash that does not match its payload
        let mut map = Vec::new();
        map.extend_from_slice(&0x0BAD_BAD_BADu64.to_le_bytes());
        map.extend_from_slice(b"this is not valid rkyv data but hash check comes first");
        std::fs::write(&map_path, &map).unwrap();
        // loading it should fail on the hash rather than on rkyv validation
        let result = SerializedMap::new(
            &map_path.to_path_buf(),
            &intent_path.to_path_buf(),
            "test",
        )
        .await;
        match result {
            Err(crate::server::ServerError::Shoal(ShoalError::MapCorruption { .. })) => (),
            Err(other) => panic!("Expected MapCorruption error, got: {other:?}"),
            Ok(_) => panic!("Expected MapCorruption error, got Ok"),
        }
    });
}

