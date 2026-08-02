//! Integration tests for the identity of a Shoal storage directory
//!
//! A partition is owned by the shard its tablet is assigned to, and that assignment comes
//! from the shard count, so a directory reopened under a different count looks for every
//! partition in the wrong place. These tests hold the server to refusing that rather than
//! starting and quietly finding nothing.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal_core::server::errors::ShoalError;
use shoal_core::server::ServerError;
use shoal_core::storage::FileSystem;
use shoal_core::tables::PersistentUnsortedTable;
use shoal_derive::{db, ShoalUnsortedTable};
use std::time::Duration;
use tempfile::TempDir;

mod utils;

use utils::TestError;

/// A simple unsorted table for testing
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "TestDb")]
pub struct TestRecord {
    /// The partition key - groups related records
    #[shoal(partition)]
    pub partition_key: String,
    /// Some data payload
    #[shoal(update)]
    pub data: String,
}

/// The test database schema
#[db]
pub struct TestDb {
    /// The unsorted test table
    pub test_record: PersistentUnsortedTable<TestRecord, FileSystem>,
}

/// Start a server on this dir with a set shard count, then shut it back down
///
/// # Arguments
///
/// * `temp_dir` - The storage dir this servers data lives in
/// * `cores` - The number of cores to run this server with
async fn cycle_server(temp_dir: &TempDir, cores: usize) -> Result<(), TestError> {
    // build a config pinned to this dir
    let mut conf = utils::build_config(temp_dir);
    // run it with the shard count we were asked for
    conf.resources.cores = Some(cores);
    // start a shoal server on our existing data
    let (_client, pool) = utils::start_with_conf::<TestDb>(conf).await?;
    // shut it right back down
    pool.exit()?;
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

/// A storage dir reopened by the shard count that wrote it has to start
#[tokio::test(flavor = "multi_thread")]
async fn the_same_shard_count_restarts() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // claim it with some number of shards
    cycle_server(&temp_dir, 2).await?;
    // reopening it with that same count is the ordinary restart
    cycle_server(&temp_dir, 2).await?;
    Ok(())
}

/// A storage dir reopened by a different shard count has to be refused
///
/// Without this the server starts happily and every partition that moved to another
/// shard is simply not found, which is a silent loss rather than an error.
#[tokio::test(flavor = "multi_thread")]
async fn a_changed_shard_count_refuses_to_start() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // claim it with some number of shards
    cycle_server(&temp_dir, 2).await?;
    // reopening it with another count would look for data in the wrong place
    let error = cycle_server(&temp_dir, 3)
        .await
        .expect_err("a shard count change started");
    // and has to say so rather than start and lose the data
    assert!(
        matches!(
            error,
            TestError::Server(ServerError::Shoal(ShoalError::ShardCountMismatch {
                found: 2,
                expected: 3
            }))
        ),
        "a changed shard count failed with the wrong error: {error:?}",
    );
    Ok(())
}
