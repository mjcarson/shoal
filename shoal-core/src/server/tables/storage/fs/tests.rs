//! Tests for the FileSystem storage engine
//!
//! These tests verify the corruption mitigation measures work correctly:
//! - Intent log reader handles truncated files gracefully
//! - Checksums detect corrupted intent log entries
//! - SerializedMap checksum detects corruption
//! - Inactive intent log discovery works correctly

#[cfg(test)]
mod tests {
    use futures::AsyncWriteExt;
    use glommio::io::{DmaStreamWriterBuilder, OpenOptions};
    use glommio::LocalExecutor;
    use gxhash::GxHasher;
    use std::hash::Hasher;
    use tempfile::TempDir;

    use crate::server::tables::storage::fs::reader::IntentLogReader;
    use crate::server::tables::storage::fs::FileSystem;

    /// Helper to write a valid intent log entry (size + checksum + data) to a DmaStreamWriter
    async fn write_entry(
        writer: &mut glommio::io::DmaStreamWriter,
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let size = data.len();
        let mut hasher = GxHasher::default();
        hasher.write(data);
        let checksum = hasher.finish();
        writer.write_all(&size.to_le_bytes()).await?;
        writer.write_all(&checksum.to_le_bytes()).await?;
        writer.write_all(data).await?;
        Ok(())
    }

    /// Helper to write raw bytes to a DMA file
    async fn write_raw(
        writer: &mut glommio::io::DmaStreamWriter,
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        writer.write_all(data).await?;
        Ok(())
    }

    // ========================================================================
    // IntentLogReader tests
    // ========================================================================

    #[test]
    fn test_reader_empty_file() {
        LocalExecutor::default()
            .run(async {
                let temp_dir = TempDir::new().unwrap();
                let path = temp_dir.path().join("empty-log");
                // create an empty file
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .dma_open(&path)
                    .await
                    .unwrap();
                file.close().await.unwrap();
                // read it
                let mut reader = IntentLogReader::new(&path.to_path_buf()).await.unwrap();
                let result = reader.next_buff().await.unwrap();
                assert!(result.is_none(), "Empty file should return None");
                reader.close().await.unwrap();
            })
    }

    #[test]
    fn test_reader_valid_entries() {
        LocalExecutor::default()
            .run(async {
                let temp_dir = TempDir::new().unwrap();
                let path = temp_dir.path().join("valid-log");
                // write 3 valid entries
                let entries: Vec<Vec<u8>> = vec![
                    b"hello world".to_vec(),
                    b"second entry with more data".to_vec(),
                    b"third".to_vec(),
                ];
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .dma_open(&path)
                    .await
                    .unwrap();
                let mut writer = DmaStreamWriterBuilder::new(file).build();
                for entry in &entries {
                    write_entry(&mut writer, entry).await.unwrap();
                }
                writer.sync().await.unwrap();
                writer.close().await.unwrap();
                // read them back
                let mut reader = IntentLogReader::new(&path.to_path_buf()).await.unwrap();
                for expected in &entries {
                    let read = reader.next_buff().await.unwrap();
                    assert!(read.is_some(), "Should have read an entry");
                    let read = read.unwrap();
                    assert_eq!(
                        &read[..expected.len()],
                        expected.as_slice(),
                        "Entry data should match"
                    );
                }
                // should be done
                let read = reader.next_buff().await.unwrap();
                assert!(read.is_none(), "Should be no more entries");
                reader.close().await.unwrap();
            })
    }

    #[test]
    fn test_reader_truncated_size_header() {
        LocalExecutor::default()
            .run(async {
                let temp_dir = TempDir::new().unwrap();
                let path = temp_dir.path().join("truncated-size-log");
                // write one valid entry followed by 4 bytes (partial size header)
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .dma_open(&path)
                    .await
                    .unwrap();
                let mut writer = DmaStreamWriterBuilder::new(file).build();
                let data = b"valid entry";
                write_entry(&mut writer, data).await.unwrap();
                // write 4 bytes of garbage (incomplete size header)
                write_raw(&mut writer, &[0xDE, 0xAD, 0xBE, 0xEF]).await.unwrap();
                writer.sync().await.unwrap();
                writer.close().await.unwrap();
                // read back
                let mut reader = IntentLogReader::new(&path.to_path_buf()).await.unwrap();
                // first entry should be valid
                let read = reader.next_buff().await.unwrap();
                assert!(read.is_some(), "First entry should be readable");
                let read = read.unwrap();
                assert_eq!(&read[..data.len()], data.as_slice());
                // second read should return None (truncated size header)
                let read = reader.next_buff().await.unwrap();
                assert!(
                    read.is_none(),
                    "Truncated size header should return None, not error"
                );
                reader.close().await.unwrap();
            })
    }

    #[test]
    fn test_reader_truncated_data() {
        LocalExecutor::default()
            .run(async {
                let temp_dir = TempDir::new().unwrap();
                let path = temp_dir.path().join("truncated-data-log");
                // write a size header claiming 1000 bytes but only write 10 bytes of data
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .dma_open(&path)
                    .await
                    .unwrap();
                let mut writer = DmaStreamWriterBuilder::new(file).build();
                let claimed_size: usize = 1000;
                let checksum: u64 = 0; // doesn't matter, size check comes first
                write_raw(&mut writer, &claimed_size.to_le_bytes()).await.unwrap();
                write_raw(&mut writer, &checksum.to_le_bytes()).await.unwrap();
                write_raw(&mut writer, &[0u8; 10]).await.unwrap();
                writer.sync().await.unwrap();
                writer.close().await.unwrap();
                // read back — size (1000) + checksum (8) exceeds file, should return None
                let mut reader = IntentLogReader::new(&path.to_path_buf()).await.unwrap();
                let read = reader.next_buff().await.unwrap();
                assert!(
                    read.is_none(),
                    "Size exceeding remaining file should return None"
                );
                reader.close().await.unwrap();
            })
    }

    #[test]
    fn test_reader_size_exceeds_file() {
        LocalExecutor::default()
            .run(async {
                let temp_dir = TempDir::new().unwrap();
                let path = temp_dir.path().join("oversize-log");
                // write a size header pointing way past EOF
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .dma_open(&path)
                    .await
                    .unwrap();
                let mut writer = DmaStreamWriterBuilder::new(file).build();
                let big_size: usize = 999999;
                write_raw(&mut writer, &big_size.to_le_bytes()).await.unwrap();
                writer.sync().await.unwrap();
                writer.close().await.unwrap();
                // read back
                let mut reader = IntentLogReader::new(&path.to_path_buf()).await.unwrap();
                let read = reader.next_buff().await.unwrap();
                assert!(read.is_none(), "Size past EOF should return None");
                reader.close().await.unwrap();
            })
    }

    #[test]
    fn test_reader_zero_size() {
        LocalExecutor::default()
            .run(async {
                let temp_dir = TempDir::new().unwrap();
                let path = temp_dir.path().join("zero-size-log");
                // write a size of 0
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .dma_open(&path)
                    .await
                    .unwrap();
                let mut writer = DmaStreamWriterBuilder::new(file).build();
                let zero_size: usize = 0;
                write_raw(&mut writer, &zero_size.to_le_bytes()).await.unwrap();
                writer.sync().await.unwrap();
                writer.close().await.unwrap();
                // read back
                let mut reader = IntentLogReader::new(&path.to_path_buf()).await.unwrap();
                let read = reader.next_buff().await.unwrap();
                assert!(read.is_none(), "Zero size should return None");
                reader.close().await.unwrap();
            })
    }

    #[test]
    fn test_reader_bad_checksum() {
        LocalExecutor::default()
            .run(async {
                let temp_dir = TempDir::new().unwrap();
                let path = temp_dir.path().join("bad-checksum-log");
                // write one entry with a wrong checksum
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .dma_open(&path)
                    .await
                    .unwrap();
                let mut writer = DmaStreamWriterBuilder::new(file).build();
                let data = b"some data here";
                let size = data.len();
                let bad_checksum: u64 = 0xDEADBEEF;
                write_raw(&mut writer, &size.to_le_bytes()).await.unwrap();
                write_raw(&mut writer, &bad_checksum.to_le_bytes()).await.unwrap();
                write_raw(&mut writer, data).await.unwrap();
                writer.sync().await.unwrap();
                writer.close().await.unwrap();
                // read back — should fail checksum validation
                let mut reader = IntentLogReader::new(&path.to_path_buf()).await.unwrap();
                let read = reader.next_buff().await.unwrap();
                assert!(
                    read.is_none(),
                    "Bad checksum should return None (treated as end of log)"
                );
                reader.close().await.unwrap();
            })
    }

    #[test]
    fn test_reader_good_entry_then_bad_checksum_then_good_entry() {
        LocalExecutor::default()
            .run(async {
                let temp_dir = TempDir::new().unwrap();
                let path = temp_dir.path().join("mixed-checksum-log");
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .dma_open(&path)
                    .await
                    .unwrap();
                let mut writer = DmaStreamWriterBuilder::new(file).build();
                // write one good entry
                let good_data = b"good entry";
                write_entry(&mut writer, good_data).await.unwrap();
                // write a bad entry (wrong checksum)
                let bad_data = b"bad entry";
                let size = bad_data.len();
                let bad_checksum: u64 = 0xBADBADBAD;
                write_raw(&mut writer, &size.to_le_bytes()).await.unwrap();
                write_raw(&mut writer, &bad_checksum.to_le_bytes()).await.unwrap();
                write_raw(&mut writer, bad_data).await.unwrap();
                // write another good entry after the bad one
                write_entry(&mut writer, b"also good").await.unwrap();
                writer.sync().await.unwrap();
                writer.close().await.unwrap();
                // read back — should only get the first good entry
                let mut reader = IntentLogReader::new(&path.to_path_buf()).await.unwrap();
                let read = reader.next_buff().await.unwrap();
                assert!(read.is_some(), "First good entry should be readable");
                let read = read.unwrap();
                assert_eq!(&read[..good_data.len()], good_data.as_slice());
                // second entry has bad checksum — should stop here
                let read = reader.next_buff().await.unwrap();
                assert!(
                    read.is_none(),
                    "Bad checksum should stop reading (third entry should not be returned)"
                );
                reader.close().await.unwrap();
            })
    }

    // ========================================================================
    // Inactive intent log discovery tests
    // ========================================================================

    #[test]
    fn test_find_inactive_intent_logs_empty_dir() {
        let temp_dir = TempDir::new().unwrap();
        let result =
            FileSystem::find_inactive_intent_logs(&temp_dir.path().to_path_buf(), "shard-1");
        assert!(result.is_empty(), "Empty dir should return no inactive logs");
    }

    #[test]
    fn test_find_inactive_intent_logs_finds_and_sorts() {
        let temp_dir = TempDir::new().unwrap();
        // create inactive log files in non-sequential order
        std::fs::write(temp_dir.path().join("shard-1-inactive-3"), "").unwrap();
        std::fs::write(temp_dir.path().join("shard-1-inactive-0"), "").unwrap();
        std::fs::write(temp_dir.path().join("shard-1-inactive-7"), "").unwrap();
        std::fs::write(temp_dir.path().join("shard-1-inactive-1"), "").unwrap();
        // also create some files that should NOT be picked up
        std::fs::write(temp_dir.path().join("shard-1-active"), "").unwrap();
        std::fs::write(temp_dir.path().join("shard-2-inactive-5"), "").unwrap(); // different shard
        std::fs::write(temp_dir.path().join("unrelated-file"), "").unwrap();

        let result =
            FileSystem::find_inactive_intent_logs(&temp_dir.path().to_path_buf(), "shard-1");
        assert_eq!(result.len(), 4, "Should find exactly 4 inactive logs");
        // verify sorted by generation ascending
        let gens: Vec<u64> = result.iter().map(|(gen, _)| *gen).collect();
        assert_eq!(gens, vec![0, 1, 3, 7], "Should be sorted by generation");
    }

    #[test]
    fn test_find_inactive_intent_logs_ignores_non_numeric_gen() {
        let temp_dir = TempDir::new().unwrap();
        // create files with non-numeric generation strings
        std::fs::write(temp_dir.path().join("shard-1-inactive-abc"), "").unwrap();
        std::fs::write(temp_dir.path().join("shard-1-inactive-"), "").unwrap();
        std::fs::write(temp_dir.path().join("shard-1-inactive-2"), "").unwrap();

        let result =
            FileSystem::find_inactive_intent_logs(&temp_dir.path().to_path_buf(), "shard-1");
        assert_eq!(result.len(), 1, "Should only find the numeric generation");
        assert_eq!(result[0].0, 2);
    }

    #[test]
    fn test_find_inactive_intent_logs_nonexistent_dir() {
        let path = std::path::PathBuf::from("/tmp/nonexistent-shoal-test-dir-12345");
        let result = FileSystem::find_inactive_intent_logs(&path, "shard-1");
        assert!(
            result.is_empty(),
            "Non-existent dir should return empty vec, not error"
        );
    }

    // ========================================================================
    // SerializedMap checksum tests
    // ========================================================================

    #[test]
    fn test_map_corrupt_hash() {
        use crate::server::errors::ShoalError;
        use crate::server::tables::storage::fs::map::SerializedMap;

        LocalExecutor::default()
            .run(async {
                let temp_dir = TempDir::new().unwrap();
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
                // write a file with a wrong hash followed by some bytes that could be rkyv data
                // the hash check should fail before rkyv deserialization is attempted
                let bad_hash: u64 = 0xBADBADBAD;
                let fake_payload = b"this is not valid rkyv data but hash check comes first";
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .dma_open(&map_path)
                    .await
                    .unwrap();
                let mut writer = DmaStreamWriterBuilder::new(file).build();
                writer.write_all(&bad_hash.to_le_bytes()).await.unwrap();
                writer.write_all(fake_payload).await.unwrap();
                writer.sync().await.unwrap();
                writer.close().await.unwrap();
                // try to load — should get MapCorruption error
                let result = SerializedMap::new(
                    &map_path.to_path_buf(),
                    &intent_path.to_path_buf(),
                    "test",
                )
                .await;
                match result {
                    Err(crate::server::ServerError::Shoal(ShoalError::MapCorruption { .. })) => {
                        // expected — the hash doesn't match the payload
                    }
                    Err(other) => panic!("Expected MapCorruption error, got: {:?}", other),
                    Ok(_) => panic!("Expected MapCorruption error, got Ok"),
                }
            })
    }
}
