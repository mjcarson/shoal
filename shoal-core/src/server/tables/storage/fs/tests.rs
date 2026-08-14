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

use uuid::Uuid;

use super::compactor::{classify_tail, TailLoss};
use super::conf::{
    FileSystemLatencyWriterConf, FileSystemTableConf, FileSystemThroughputWriterConf,
};
use super::loader::{classify, LoadFailure};
use super::map::ArchiveMap;
use super::reader::IntentLogReader;
use super::stream::PAD_SENTINEL;
use super::find_inactive_intent_logs;
use crate::server::{ServerError, ShoalError};

/// The errno for running out of file descriptors
///
/// Spelled out rather than pulled from `libc`, which this crate does not depend on.
const EMFILE: i32 = 24;

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
// Compaction tail loss tests
// ========================================================================

#[test]
/// A log a compaction read to its end costs nothing
fn classify_tail_reports_no_loss_for_a_clean_log() {
    // a log with records in it that was read all the way through
    assert_eq!(classify_tail(false, true), TailLoss::None);
    // and a log that was simply empty, which is what rotating an unwritten table leaves
    assert_eq!(classify_tail(false, false), TailLoss::None);
}

#[test]
/// A damaged log a compaction still read records out of loses only its tail
///
/// This is the case that used to be silent. The compaction deletes the log on every
/// path out, so the records after the damage go with the file, and reporting only the
/// log we could read nothing from meant the larger loss was the quiet one.
fn classify_tail_reports_a_dropped_tail() {
    assert_eq!(classify_tail(true, true), TailLoss::Tail);
}

#[test]
/// A damaged log a compaction read nothing out of is lost whole
fn classify_tail_reports_a_log_dropped_whole() {
    assert_eq!(classify_tail(true, false), TailLoss::Whole);
}

#[test]
/// Every kind of loss has to be distinguishable from a clean read
///
/// `apply_intents` turns this into `truncated_logs`, and folding either damaged case
/// in with a clean one would put a compaction that dropped records back into the
/// silence this came out of.
fn classify_tail_separates_loss_from_a_clean_read() {
    // both damaged shapes have to count as loss
    for read_any in [true, false] {
        assert_ne!(classify_tail(true, read_any), TailLoss::None);
    }
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


/// A partition pruned out from under a read is absent, not an error
///
/// This is the time-of-check-to-time-of-use race `FileSystem::load_partition` allows on
/// purpose. Classifying it as retryable would spin on a partition that no longer exists.
#[test]
fn a_pruned_partition_is_classified_absent() {
    // build the error a read of a pruned partition fails with
    let error = ServerError::Shoal(ShoalError::PartitionNotFound { partition_id: 7 });
    // it names a partition that is in no archive, so there is nothing to read
    assert_eq!(classify(&error), LoadFailure::Absent);
}

/// A table missing from the archive map is never retried
///
/// The map is built once when the loader is spawned, so a name missing from it is missing
/// for the life of the process and retrying it only spins.
#[test]
fn a_missing_table_map_is_not_retried() {
    // build the error a read against an unknown table fails with
    let error = ServerError::Shoal(ShoalError::TableMapMissing);
    // this cannot come good on its own
    assert_eq!(classify(&error), LoadFailure::Fatal);
}

/// An archive that could not be opened is worth trying again
///
/// Every read in flight holds a duplicated file handle and nothing bounds how many there
/// are, so a shortage that other reads will give back is the realistic failure here.
#[test]
fn an_archive_open_failure_is_retryable() {
    // build the error a read whose archive could not be opened fails with
    // EMFILE is the one worth naming: every read in flight holds a duplicated handle
    let error = ServerError::IO(std::io::Error::from_raw_os_error(EMFILE));
    // another read finishing may free the descriptor this one needed
    assert_eq!(classify(&error), LoadFailure::Retryable);
    // the enhanced form glommio reports for a named file classifies the same way
    let enhanced = ServerError::GlommioIO {
        source: std::io::Error::from_raw_os_error(EMFILE),
        op: "Opening",
        path: None,
        fd: None,
    };
    assert_eq!(classify(&enhanced), LoadFailure::Retryable);
}

/// An error nothing recognises is given up on rather than retried forever
///
/// A new error class defaulting to retryable would turn a permanent failure into a loop that
/// never reports anything, which is the failure mode this whole path exists to remove.
#[test]
fn an_unrecognised_error_is_fatal() {
    // build an error that has nothing to do with reading a partition
    let error = ServerError::Shoal(ShoalError::NoShards);
    // an error we cannot reason about is not assumed to be transient
    assert_eq!(classify(&error), LoadFailure::Fatal);
}

/// An archive that is not on disk is never retried
///
/// This has to stay out of the `IO`/`GlommioIO` arm, which is the arm the errno alone would
/// put it in. An archive that is missing is missing for good, and every query parked behind
/// the read would wait out all three attempts to arrive at the same answer.
#[test]
fn a_missing_archive_is_not_retried() {
    // build the error a read whose archive is not on disk fails with
    let error = ServerError::Shoal(ShoalError::ArchiveMissing {
        archive: Uuid::nil(),
        path: PathBuf::from("/does/not/exist"),
    });
    // no amount of trying puts a deleted file back
    assert_eq!(classify(&error), LoadFailure::Fatal);
}

/// A read of an archive that is not on disk is reported rather than creating one
///
/// This used to open with `create(true)`, so the read found an empty file it had just made,
/// came back short, and failed much later as a validation error over bytes nobody wrote -
/// which could not name the archive that had gone missing, and left the empty one behind for
/// every later read of the same partition to find.
#[test]
fn a_missing_archive_is_reported_not_created() {
    LocalExecutor::default().run(async {
        let temp_dir = test_dir();
        // build a config that keeps both halves of this tables storage in our temp dir
        let conf = FileSystemTableConf::builder()
            .latency_sensitive(FileSystemLatencyWriterConf::default().path(temp_dir.path()))
            .throughput_sensitive(FileSystemThroughputWriterConf::default().path(temp_dir.path()));
        // make the directories an archive map expects to find
        conf.setup_paths("TestRecord").await.unwrap();
        // load an archive map over them, which is empty since nothing has been written
        let map = ArchiveMap::new("shard-0", "TestRecord", &conf).await.unwrap();
        // name an archive that was never written, which is what an entry left behind by a
        // compaction that deleted the archive it re-pointed away from looks like
        let missing = Uuid::new_v4();
        // build the path that archive would live at
        let path = conf.get_archive_path("TestRecord").join(missing.to_string());
        // read it, which has to fail
        let error = map
            .get_archive(&missing)
            .await
            .expect_err("a missing archive was opened");
        // the failure names the archive rather than being an errno from somewhere
        match error {
            ServerError::Shoal(ShoalError::ArchiveMissing { archive, .. }) => {
                assert_eq!(archive, missing);
            }
            other => panic!("Expected ArchiveMissing error, got: {other:?}"),
        }
        // and nothing was left behind at the name it looked for
        assert!(
            !path.exists(),
            "a missing archive was created at {}",
            path.display()
        );
    });
}
