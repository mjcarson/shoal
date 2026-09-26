//! The canonical digest of a tablet group at a committed boundary
//!
//! A scrub is a log entry ([F44](../../../../docs/src/features/repair.md)): the group's leader
//! proposes [`Command::scrub`](crate::shared::protocol::peer::Command::scrub), every replica
//! applies it in committed order at some index `B`, and at that moment each replica's state is
//! exactly the effect of the entries at or below `B` - the one point at which replicas can be
//! compared with no clock and no pause coordination. What each replica hashes there is its
//! *logical* content, never its archive bytes: per partition the key, the row count and every
//! live row re-serialized in canonical order, folded in key order under the schema fingerprint
//! and the tablet list. Two replicas holding the same rows hash the same however their archives
//! are laid out, however often they were compacted, and whether a partition is resident or on
//! disk.
//!
//! Applying the scrub takes a **cut, not a walk**: on the shard loop the table hashes every
//! resident partition of the group's tablets and collects, for every non-resident archived
//! key, where its record is plus an open handle per distinct archive. The applied position
//! moves past `B` at once. A spawned task then reads, verifies and hashes every collected record
//! and posts the report to the loop. The pause on the loop is the resident pass alone; an open
//! handle survives the archive's later unlink, and a record's bytes are never rewritten in place
//! - the map only repoints - so the task reads exactly the state at `B` however long it takes.

use std::collections::{BTreeMap, HashMap};
use std::hash::Hasher as _;
use std::sync::Arc;

use glommio::io::DmaFile;
use serde::{Deserialize, Serialize};
use tracing::{event, Level};
use uuid::Uuid;

use crate::server::ServerError;
use crate::server::ShoalError;
use crate::storage::fs::map::ArchiveEntry;
use crate::storage::fs::ArchiveMap;

/// The seed every canonical hash is taken under, frozen like every other persisted hash
const CANONICAL_SEED: i64 = 0;

/// How many reports a group keeps, by operation, for the leader to poll
pub const KEPT_REPORTS: usize = 8;

/// One partition's contribution to a canonical digest
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionDigest {
    /// The hash over the key, the row count and every live row in canonical order
    pub hash: u64,
    /// How many live rows the partition holds
    pub rows: u64,
}

/// Hash one partition canonically
///
/// The key, then the row count, then every row's re-serialized bytes in the order given, which
/// the caller makes canonical - a sorted partition iterates in sort-key order, an unsorted one
/// holds one row. A partition with no live row contributes nothing, so a tombstone-only
/// partition on one replica and no partition on another agree.
///
/// # Arguments
///
/// * `key` - The partition key
/// * `rows` - Every live row's serialized bytes, in canonical order
pub fn hash_partition<'a, I: IntoIterator<Item = &'a [u8]>>(
    key: u64,
    rows: I,
) -> Option<PartitionDigest> {
    // fold the key first, then every row as it comes, counting them
    let mut hasher = gxhash::GxHasher::with_seed(CANONICAL_SEED);
    hasher.write_u64(key);
    let mut count = 0u64;
    let mut body = gxhash::GxHasher::with_seed(CANONICAL_SEED);
    for row in rows {
        count += 1;
        // a row is hashed as its length and its bytes, so two rows cannot run together
        body.write_u64(row.len() as u64);
        body.write(row);
    }
    // nothing live is nothing to report
    if count == 0 {
        return None;
    }
    hasher.write_u64(count);
    hasher.write_u64(body.finish());
    Some(PartitionDigest {
        hash: hasher.finish(),
        rows: count,
    })
}

/// Fold every partition's digest into the group's, in key order
///
/// Prefixed by the schema fingerprint and the tablet list, so a digest of the wrong schema or
/// of a different placement never agrees by accident. The map is ordered, so the fold is
/// independent of the order the partitions were found in. Returns the digest, how many
/// partitions it covers and how many rows they hold.
///
/// # Arguments
///
/// * `schema_id` - The structural fingerprint of the schema
/// * `tablets` - The tablets the group serves
/// * `partitions` - Every live partition's digest, by key
#[must_use]
pub fn fold_group(
    schema_id: u64,
    tablets: &[u16],
    partitions: &BTreeMap<u64, PartitionDigest>,
) -> (u64, u64, u64) {
    // the prefix: what the digest is of
    let mut hasher = gxhash::GxHasher::with_seed(CANONICAL_SEED);
    hasher.write_u64(schema_id);
    hasher.write_u64(tablets.len() as u64);
    for tablet in tablets {
        hasher.write_u16(*tablet);
    }
    // then every partition in key order
    let mut rows = 0u64;
    for (key, digest) in partitions {
        hasher.write_u64(*key);
        hasher.write_u64(digest.hash);
        rows += digest.rows;
    }
    (hasher.finish(), partitions.len() as u64, rows)
}

/// Whether a replica's copy was whole when it was hashed
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DigestIntegrity {
    /// Every record read hashed to its checksum, or was resident
    Verified,
    /// Some records did not: the copy is corrupt, and its digest describes what could be read
    Invalid {
        /// How many records failed
        checksum_failures: u64,
    },
}

/// What one replica reports of its copy at a scrub
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DigestReport {
    /// The log index the scrub was applied at
    pub boundary: u64,
    /// How many live partitions the copy holds
    pub partitions: u64,
    /// How many live rows they hold
    pub rows: u64,
    /// The canonical digest
    pub digest: u64,
    /// Whether every record read was whole
    pub integrity: DigestIntegrity,
    /// How many records came from an archive with no checksums
    pub unverified: u64,
    /// How many bytes were read from the archives
    pub bytes: u64,
}

/// The operation a scrub that only moves the log along is proposed under
///
/// A driver that needs an entry past an index - to move a checkpoint, to cut past a target's -
/// proposes a scrub nobody polls. Under this operation no replica takes a cut of it: on the lab
/// a copy restarted from a repair snapshot replayed forty of them, each reading a group of
/// 320,000 archived partitions, and the one digest its repair waited for came after them
/// ([Resolved #164](../../../../docs/src/appendix/resolved/replayed-scrub-cuts.md)).
pub const NUDGE: Uuid = Uuid::nil();

/// What a member answers a digest request with
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DigestAnswer {
    /// The scrub was applied and its task has not posted yet
    Pending,
    /// The report
    Report(DigestReport),
    /// The member never applied a scrub under this operation, or forgot it
    Unknown,
    /// The member's copy stopped applying on a partition it could not read, and never will
    /// apply the scrub ([Resolved #160](../../../../docs/src/appendix/resolved/unreadable-partition-stalls-one-copy.md))
    Stalled,
}

/// The archived records a canonical cut still has to read, with the handles to read them by
///
/// Collected on the shard loop while the map is what it is at the boundary; the handles are
/// duplicates the archive map handed out, so they outlive the archives' later unlink.
pub struct ArchivedCut {
    /// Where every non-resident partition's record is, in key order
    entries: Vec<ArchiveEntry>,
    /// An open handle per distinct archive
    handles: HashMap<Uuid, DmaFile>,
    /// The map the records belong to, which verifies and counts the reads
    map: Option<Arc<ArchiveMap>>,
}

impl ArchivedCut {
    /// A cut with nothing to read, which is what an engine with no archives collects
    #[must_use]
    pub fn empty() -> Self {
        ArchivedCut {
            entries: Vec::new(),
            handles: HashMap::new(),
            map: None,
        }
    }

    /// A cut of some records of a map, with a handle per archive they are in
    ///
    /// # Arguments
    ///
    /// * `map` - The archive map
    /// * `entries` - The records, in key order
    /// * `handles` - An open handle per distinct archive the records are in
    #[must_use]
    pub fn new(
        map: Arc<ArchiveMap>,
        entries: Vec<ArchiveEntry>,
        handles: HashMap<Uuid, DmaFile>,
    ) -> Self {
        ArchivedCut {
            entries,
            handles,
            map: Some(map),
        }
    }

    /// How many records the cut still has to read
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether there is nothing to read
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// How a table hashes one archived partition's bytes
///
/// A plain function rather than a closure, since the partition's type is known per table and
/// the cut crosses no boundary that could not carry a pointer.
pub type ArchivedHasher = fn(u64, &[u8]) -> Result<Option<PartitionDigest>, ServerError>;

/// A canonical cut taken on the loop, with what is left to read off it
pub struct PendingDigest {
    /// Every resident partition's digest, by key
    pub resident: BTreeMap<u64, PartitionDigest>,
    /// The archived records still to read
    pub archived: ArchivedCut,
    /// How the table hashes an archived record's bytes
    pub hasher: ArchivedHasher,
}

impl PendingDigest {
    /// Read, verify and hash every archived record, and fold the report
    ///
    /// Runs off the loop. A record that fails its checksum is counted and skipped, so the
    /// digest describes what could be read and the report says the copy is invalid; a record
    /// that cannot be read for any other reason fails the whole cut, since a report over a
    /// copy nobody could read would be evidence of nothing.
    ///
    /// # Arguments
    ///
    /// * `schema_id` - The structural fingerprint of the schema
    /// * `tablets` - The tablets the group serves
    /// * `boundary` - The index the scrub was applied at
    pub async fn finish(
        self,
        schema_id: u64,
        tablets: &[u16],
        boundary: u64,
    ) -> Result<DigestReport, ServerError> {
        let PendingDigest {
            mut resident,
            archived,
            hasher,
        } = self;
        let mut checksum_failures = 0u64;
        let mut bytes = 0u64;
        let unverified_before = archived
            .map
            .as_ref()
            .map_or(0, |map| map.integrity.unverified_reads.get());
        // every archived record, through the handle collected for its archive
        for entry in &archived.entries {
            let Some(map) = archived.map.as_ref() else {
                break;
            };
            let Some(handle) = archived.handles.get(&entry.archive) else {
                return Err(ServerError::GlommioGeneric(format!(
                    "the cut holds no handle for archive {}",
                    entry.archive
                )));
            };
            match map.read_record_from(handle, entry).await {
                Ok(read) => {
                    bytes += read.len() as u64;
                    if let Some(digest) = hasher(entry.key, &read)? {
                        resident.insert(entry.key, digest);
                    }
                }
                // a corrupt record is what the scrub exists to find: counted, and the
                // digest goes on over what could be read
                Err(ServerError::Shoal(ShoalError::CorruptArchive {
                    archive,
                    partition_id,
                    ..
                })) => {
                    checksum_failures += 1;
                    event!(Level::ERROR, msg = "a scrub found a corrupt record", archive = %archive, partition = format!("{partition_id:016x}"));
                }
                Err(error) => return Err(error),
            }
        }
        let unverified = archived
            .map
            .as_ref()
            .map_or(0, |map| map.integrity.unverified_reads.get())
            .saturating_sub(unverified_before);
        // the handles were duplicates, and go with the cut
        for (_, handle) in archived.handles {
            let _ = handle.close().await;
        }
        let (digest, partitions, rows) = fold_group(schema_id, tablets, &resident);
        Ok(DigestReport {
            boundary,
            partitions,
            rows,
            digest,
            integrity: if checksum_failures == 0 {
                DigestIntegrity::Verified
            } else {
                DigestIntegrity::Invalid { checksum_failures }
            },
            unverified,
            bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{fold_group, hash_partition, PartitionDigest};
    use std::collections::BTreeMap;

    /// The canonical fold depends on the rows and nothing else
    ///
    /// The same rows found resident or archived, in a map built in any order, with or without
    /// a tombstone-only partition beside them, fold to one digest; a changed row, a missing
    /// partition, a different schema or a different tablet list do not.
    #[test]
    fn canonical_fold_is_layout_and_order_independent() {
        let rows_a: Vec<&[u8]> = vec![b"alpha", b"beta"];
        let rows_b: Vec<&[u8]> = vec![b"gamma"];
        // a partition hashed twice from the same rows is one digest, whatever held them
        let a = hash_partition(1, rows_a.iter().copied()).expect("live rows");
        assert_eq!(
            a,
            hash_partition(1, rows_a.iter().copied()).expect("live rows")
        );
        let b = hash_partition(2, rows_b.iter().copied()).expect("live rows");
        // the rows in another order are a different partition: order is the caller's to make canonical
        let reversed: Vec<&[u8]> = rows_a.iter().rev().copied().collect();
        assert_ne!(a, hash_partition(1, reversed).expect("live rows"));
        // two rows that would run together as bytes do not
        let joined: Vec<&[u8]> = vec![b"alphabeta"];
        assert_ne!(a, hash_partition(1, joined).expect("live rows"));
        // a partition with no live row contributes nothing
        assert_eq!(hash_partition(3, std::iter::empty()), None);
        // the fold over a map built in either order is one digest
        let mut forward = BTreeMap::new();
        forward.insert(1, a);
        forward.insert(2, b);
        let mut backward = BTreeMap::new();
        backward.insert(2, b);
        backward.insert(1, a);
        let tablets = [0u16, 1, 2];
        let folded = fold_group(0xfeed, &tablets, &forward);
        assert_eq!(folded, fold_group(0xfeed, &tablets, &backward));
        assert_eq!(folded.1, 2, "two partitions");
        assert_eq!(folded.2, 3, "three rows");
        // a changed row is a different digest
        let changed: Vec<&[u8]> = vec![b"alpha", b"BETA"];
        let mut altered = forward.clone();
        altered.insert(1, hash_partition(1, changed).expect("live rows"));
        assert_ne!(folded.0, fold_group(0xfeed, &tablets, &altered).0);
        // a missing partition is a different digest
        let mut missing = forward.clone();
        missing.remove(&2);
        assert_ne!(folded.0, fold_group(0xfeed, &tablets, &missing).0);
        // an erased partition - every row deleted - is the missing one: a tombstone contributes nothing
        let mut erased = forward.clone();
        if let Some(digest) = hash_partition(2, std::iter::empty()) {
            erased.insert(2, digest);
        } else {
            erased.remove(&2);
        }
        assert_eq!(
            fold_group(0xfeed, &tablets, &erased).0,
            fold_group(0xfeed, &tablets, &missing).0
        );
        // another schema or another placement never agrees by accident
        assert_ne!(folded.0, fold_group(0xbeef, &tablets, &forward).0);
        assert_ne!(folded.0, fold_group(0xfeed, &[0u16, 1], &forward).0);
        let _ = PartitionDigest { hash: 0, rows: 0 };
    }
}
