//! What a backup and a restore record, and what a recovery records
//!
//! A backup is an administrative operation committed to the control log
//! ([F49](../../../../docs/src/features/backup-and-recovery.md)): the record names the table
//! - or every table - the directory the files go under, and every group the placement
//! derives, with each group's progress as its driver proposes it. It is **not** one
//! cross-tablet snapshot: every group's file is cut at a committed boundary of its own, which
//! the record names, and the files together are a backup of the tables at those boundaries. A
//! restore is the same shape driven the other way, on a **new** cluster: every group's leader
//! builds a file for its tablets from the backup's files and installs it on every member
//! through the repair install path, and the cluster records the cluster it was restored from
//! so the old identities are refused by name. A recovery is what `force_recover` writes when
//! an operator rewrites a stopped survivor's membership after a permanent majority loss.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::shared::identity::{ClusterId, GroupId, NodeId, TableId};

/// How many backup records the control state keeps, newest last
pub const KEPT_BACKUPS: usize = 64;

/// How many restore records the control state keeps, newest last
pub const KEPT_RESTORES: usize = 16;

/// The name of the manifest file written beside every backup file
pub const BACKUP_MANIFEST_SUFFIX: &str = ".json";

/// Where a group's backup stands
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackupPhase {
    /// Waiting for a move of the group's set, or a repair of it, to finish
    Queued {
        /// The operation it waits behind
        behind: Uuid,
    },
    /// Nobody has driven it yet
    Pending,
    /// The leader is cutting the group's snapshot at a committed boundary
    Cutting,
    /// The cut is being copied under the backup directory
    Writing,
    /// Nothing more will happen to this group under this operation
    Done,
}

impl BackupPhase {
    /// Where this phase stands in the order a backup moves through
    #[must_use]
    pub fn rank(&self) -> u8 {
        match self {
            BackupPhase::Queued { .. } => 0,
            BackupPhase::Pending => 1,
            BackupPhase::Cutting => 2,
            BackupPhase::Writing => 3,
            BackupPhase::Done => 4,
        }
    }
}

/// What a group's backup came to
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackupOutcome {
    /// The file is under the backup directory, verified against its manifest
    Written {
        /// The file, on the driver's node
        file: String,
        /// How many bytes it takes
        bytes: u64,
        /// What every byte of it hashes to
        checksum: u64,
        /// How many records it holds
        records: u64,
        /// How many remembered requests its trailer holds
        retries: u32,
    },
    /// The file could not be written; the reason is the evidence
    Failed {
        /// Why
        reason: String,
    },
    /// Nothing was written, on purpose: an ephemeral table's group holds nothing a restart keeps
    Skipped {
        /// Why
        reason: String,
    },
}

/// One group's progress under a backup operation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupBackup {
    /// Where it stands
    pub phase: BackupPhase,
    /// The node driving it, if one has said so
    pub driver: Option<NodeId>,
    /// The committed boundary the file was cut at, once cut
    pub boundary: Option<u64>,
    /// What it came to, once done
    pub outcome: Option<BackupOutcome>,
}

impl Default for GroupBackup {
    /// Pending, with nobody driving
    fn default() -> Self {
        GroupBackup {
            phase: BackupPhase::Pending,
            driver: None,
            boundary: None,
            outcome: None,
        }
    }
}

impl GroupBackup {
    /// Whether nothing more will happen to this group under its operation
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.phase == BackupPhase::Done
    }

    /// Whether the group waits for another operation on its set to finish
    #[must_use]
    pub fn is_queued(&self) -> bool {
        matches!(self.phase, BackupPhase::Queued { .. })
    }
}

/// A backup operation, as the control state records it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupRecord {
    /// The operation, which names the directory under the path
    pub op: Uuid,
    /// The table, or none for every table
    pub table: Option<TableId>,
    /// The directory the files go under, on every node that writes one
    pub path: String,
    /// Who asked
    pub principal: String,
    /// The topology version the request was applied at
    pub requested_at: u64,
    /// Every group of the table or the tables, and where each stands
    pub groups: BTreeMap<GroupId, GroupBackup>,
}

impl BackupRecord {
    /// Whether every group is done
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.groups.values().all(GroupBackup::is_done)
    }
}

/// What a backup file's manifest carries, as `<file>.json` beside every `.snap`
///
/// A restore reads what a file is without reading the file: the cluster and the schema it was
/// cut from, the group, the boundary, the tablets, the records and the checksum. The
/// snapshot's own manifest is not written as it is, since its membership is a map keyed by a
/// shard address, which JSON cannot hold; what a restore needs of it is here, and the
/// membership of a restored file is the restoring group's own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupManifest {
    /// The backup operation
    pub op: Uuid,
    /// The cluster the file was cut in
    pub cluster: ClusterId,
    /// The table's name, as the schema spells it
    pub table_name: String,
    /// The node that cut it
    pub origin: NodeId,
    /// The group
    pub group: GroupId,
    /// The table
    pub table: TableId,
    /// The structural fingerprint of the schema the file was cut from
    pub schema_id: u64,
    /// The boundary's index
    pub boundary: u64,
    /// The boundary's term
    pub term: u64,
    /// The tablets the file covers
    pub tablets: Vec<u16>,
    /// How many records the file holds
    pub records: u64,
    /// How many bytes it takes
    pub total: u64,
    /// What every byte of it hashes to
    pub checksum: u64,
    /// How many remembered requests the trailer holds
    pub retries: u32,
    /// The newest time-ordered identity the group had forgotten when it was cut
    pub expired_before: u64,
    /// When it was cut, in milliseconds since the epoch
    pub created_ms: u64,
}

impl BackupManifest {
    /// What a backup records of a cut
    ///
    /// # Arguments
    ///
    /// * `op` - The backup operation
    /// * `table_name` - The table's name
    /// * `manifest` - The cut's manifest, stamped
    #[must_use]
    pub fn of(
        op: Uuid,
        table_name: &str,
        manifest: &crate::server::replication::SnapshotManifest,
    ) -> Self {
        BackupManifest {
            op,
            cluster: manifest.cluster,
            table_name: table_name.to_string(),
            origin: manifest.origin,
            group: manifest.group,
            table: manifest.table,
            schema_id: manifest.schema_id,
            boundary: manifest.boundary.index,
            term: manifest.boundary.leader_id.term,
            tablets: manifest.tablets.clone(),
            records: manifest.records,
            total: manifest.total,
            checksum: manifest.checksum,
            retries: manifest.retries,
            expired_before: manifest.expired_before,
            created_ms: manifest.created_ms,
        }
    }

    /// The file's manifest as `verify` judges a file by: the sizes, the checksum and the header
    ///
    /// The membership is nobody's here, and the boundary's leader the origin's first slot.
    #[must_use]
    pub fn to_snapshot(&self) -> crate::server::replication::SnapshotManifest {
        use openraft::vote::RaftLeaderId as _;
        let leader = crate::server::wal::LeaderId::new(
            self.term,
            crate::shared::identity::ShardAddr::new(self.origin, 0),
        );
        crate::server::replication::SnapshotManifest {
            group: self.group,
            table: self.table,
            schema_id: self.schema_id,
            boundary: openraft::LogId::new(leader, self.boundary),
            membership: openraft::StoredMembership::default(),
            tablets: self.tablets.clone(),
            records: self.records,
            total: self.total,
            checksum: self.checksum,
            retries: self.retries,
            expired_before: self.expired_before,
            cluster: self.cluster,
            origin: self.origin,
            created_ms: self.created_ms,
        }
    }
}

/// Where a group's restore stands
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RestorePhase {
    /// Nobody has driven it yet
    Pending,
    /// The leader is reading the backup files covering its tablets into one file
    Loading,
    /// The file is being installed on every member
    Installing,
    /// A scrub is checking every member holds the same rows
    Verifying,
    /// Nothing more will happen to this group under this operation
    Done,
}

impl RestorePhase {
    /// Where this phase stands in the order a restore moves through
    #[must_use]
    pub fn rank(&self) -> u8 {
        match self {
            RestorePhase::Pending => 0,
            RestorePhase::Loading => 1,
            RestorePhase::Installing => 2,
            RestorePhase::Verifying => 3,
            RestorePhase::Done => 4,
        }
    }
}

/// What a group's restore came to
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RestoreOutcome {
    /// Every member holds the restored rows, verified by a scrub
    Restored {
        /// The committed boundary the restored file was installed at
        boundary: u64,
        /// How many records were restored
        records: u64,
        /// How many bytes the restored file took
        bytes: u64,
        /// How many remembered requests were restored
        retries: u32,
        /// The index the verifying scrub agreed at
        verified: u64,
    },
    /// The restore could not be completed; the reason is the evidence
    Failed {
        /// Why
        reason: String,
    },
    /// Nothing was installed, on purpose: an ephemeral table's group holds nothing a restart keeps
    Skipped {
        /// Why
        reason: String,
    },
}

/// One group's progress under a restore operation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupRestore {
    /// Where it stands
    pub phase: RestorePhase,
    /// The node driving it, if one has said so
    pub driver: Option<NodeId>,
    /// The backup files covering the group's tablets, by name under the backup directory
    #[serde(default)]
    pub files: Vec<String>,
    /// What it came to, once done
    pub outcome: Option<RestoreOutcome>,
    /// How many drives were given up because a member could not be reached, and tried again
    /// ([Resolved #155](../../../../docs/src/appendix/resolved/restore-retries-unreachable.md))
    #[serde(default)]
    pub attempts: u32,
    /// The phase the group had reached when it failed, which a retry drives it from
    /// ([Resolved #155](../../../../docs/src/appendix/resolved/restore-retry.md))
    #[serde(default)]
    pub failed_in: Option<RestorePhase>,
    /// How many times the operation's failed groups were retried when this group was last
    /// put back to be driven, so a driver of an earlier try is told apart from this one's
    #[serde(default)]
    pub generation: u32,
}

impl Default for GroupRestore {
    /// Pending, with nobody driving
    fn default() -> Self {
        GroupRestore {
            phase: RestorePhase::Pending,
            driver: None,
            files: Vec::new(),
            outcome: None,
            attempts: 0,
            failed_in: None,
            generation: 0,
        }
    }
}

impl GroupRestore {
    /// Whether nothing more will happen to this group under its operation
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.phase == RestorePhase::Done
    }

    /// Whether this group's restore ended in a failure
    #[must_use]
    pub fn failed(&self) -> bool {
        self.is_done() && matches!(self.outcome, Some(RestoreOutcome::Failed { .. }))
    }

    /// Put a failed group back to be driven again, from the phase it failed in
    ///
    /// A group that failed loading loads again, which checks its copies are still empty. One
    /// that failed installing or verifying installs again: a new file at a new boundary on
    /// every member, which replaces whatever a partial install left, and then verifies. The
    /// driver, the outcome and the attempts are cleared, and the generation moves on.
    pub fn retry(&mut self) {
        // where the next driver starts
        self.phase = match self.failed_in {
            Some(RestorePhase::Installing | RestorePhase::Verifying) => RestorePhase::Installing,
            Some(RestorePhase::Loading) => RestorePhase::Loading,
            _ => RestorePhase::Pending,
        };
        self.driver = None;
        self.outcome = None;
        self.attempts = 0;
        self.failed_in = None;
        self.generation += 1;
    }
}

/// The wire version a restore's failed groups can be retried from
///
/// The retry is a control command, and a group's record gained the phase it failed in and a
/// generation; a replica built before them would refuse the command or drop the fields, so it
/// is refused until every member speaks 6 and it is activated
/// ([F48](../../../../docs/src/features/rolling-compatibility.md)).
pub const RESTORE_RETRY_FROM_WIRE: u8 = 6;

/// One backup file as a restore judges it, read from its manifest
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupFile {
    /// The file's name, relative to the backup directory
    pub name: String,
    /// The table it holds, by its stable identity
    pub table: TableId,
    /// The table's name, as the schema that cut it spelled it
    pub table_name: String,
    /// The tablets it covers
    pub tablets: Vec<u16>,
    /// The boundary it was cut at
    pub boundary: u64,
    /// How many records it holds
    pub records: u64,
}

/// A restore operation, as the control state records it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreRecord {
    /// The operation
    pub op: Uuid,
    /// The directory the files were read from
    pub path: String,
    /// The cluster the files were cut in
    pub source: ClusterId,
    /// The structural fingerprint of the schema the files were cut from
    pub source_schema: u64,
    /// Who asked
    pub principal: String,
    /// The topology version the request was applied at
    pub requested_at: u64,
    /// Every file the restore reads, as judged at apply
    pub files: Vec<BackupFile>,
    /// Every group of every restored table, and where each stands
    pub groups: BTreeMap<GroupId, GroupRestore>,
}

impl RestoreRecord {
    /// Whether every group is done
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.groups.values().all(GroupRestore::is_done)
    }
}

/// What `force_recover` wrote, as the control state records it
///
/// The evidence of a permanent majority loss and of what an operator did about it: which
/// members were kept, which were lost and tombstoned, and where the survivor's log stood, so a
/// reader can tell what was and was not recovered
/// ([F49](../../../../docs/src/features/backup-and-recovery.md)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryRecord {
    /// The members kept as the whole membership
    pub survivors: Vec<NodeId>,
    /// The members lost, tombstoned by this recovery
    pub lost: Vec<NodeId>,
    /// The node the recovery was run on
    pub at: NodeId,
    /// The control log index the survivor had committed when it was recovered: every entry
    /// past it on a lost member is gone
    pub last_committed: u64,
    /// The topology version the recovery was applied at
    pub applied_at: u64,
    /// When the recovery was run, in milliseconds since the epoch
    pub recovered_ms: u64,
}

/// Read every backup manifest under a directory, and what they agree on
///
/// Walks the directory for `*.snap.json` files, reads each, and returns the cluster and the
/// schema the files were cut from - which every file has to agree on - and one
/// [`BackupFile`] per manifest, named by its `.snap` relative to the directory.
///
/// # Arguments
///
/// * `dir` - The backup directory, on this node
///
/// # Errors
///
/// Names the first manifest that does not read, or the first that names another cluster or
/// schema than the rest.
pub fn scan_backup(dir: &std::path::Path) -> Result<(ClusterId, u64, Vec<BackupFile>), String> {
    // every manifest under the directory, in name order so the judgment is deterministic
    let mut manifests = Vec::new();
    walk_manifests(dir, dir, &mut manifests)
        .map_err(|error| format!("reading {}: {error}", dir.display()))?;
    manifests.sort();
    if manifests.is_empty() {
        return Err(format!("{} holds no backup manifests", dir.display()));
    }
    let mut cluster: Option<ClusterId> = None;
    let mut schema: Option<u64> = None;
    let mut files = Vec::with_capacity(manifests.len());
    for (name, path) in manifests {
        let bytes =
            std::fs::read(&path).map_err(|error| format!("reading {}: {error}", path.display()))?;
        let manifest: BackupManifest = serde_json::from_slice(&bytes)
            .map_err(|error| format!("{} is not a backup manifest: {error}", path.display()))?;
        // every file of one backup was cut in one cluster from one schema
        match cluster {
            Some(seen) if seen != manifest.cluster => {
                return Err(format!(
                    "{name} was cut in cluster {} and the rest in {seen}",
                    manifest.cluster
                ));
            }
            _ => cluster = Some(manifest.cluster),
        }
        match schema {
            Some(seen) if seen != manifest.schema_id => {
                return Err(format!(
                    "{name} was cut from schema {:#018x} and the rest from {seen:#018x}",
                    manifest.schema_id
                ));
            }
            _ => schema = Some(manifest.schema_id),
        }
        files.push(BackupFile {
            name,
            table: manifest.table,
            table_name: manifest.table_name,
            tablets: manifest.tablets,
            boundary: manifest.boundary,
            records: manifest.records,
        });
    }
    Ok((
        cluster.unwrap_or_default(),
        schema.unwrap_or_default(),
        files,
    ))
}

/// Collect every `.snap.json` under a directory, named by its `.snap` relative to the root
///
/// # Arguments
///
/// * `root` - The backup directory
/// * `dir` - The directory being walked
/// * `found` - Where the manifests go
fn walk_manifests(
    root: &std::path::Path,
    dir: &std::path::Path,
    found: &mut Vec<(String, std::path::PathBuf)>,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            walk_manifests(root, &path, found)?;
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(snap) = file_name.strip_suffix(BACKUP_MANIFEST_SUFFIX) else {
            continue;
        };
        if !snap.ends_with(".snap") {
            continue;
        }
        // the name is the `.snap` file's path under the root
        let relative = path
            .parent()
            .and_then(|parent| parent.strip_prefix(root).ok())
            .map(|rel| rel.join(snap))
            .unwrap_or_else(|| std::path::PathBuf::from(snap));
        found.push((relative.to_string_lossy().into_owned(), path));
    }
    Ok(())
}

/// Judge the files of a backup against the tables a cluster serves
///
/// Every file has to be of a table the cluster has; every tablet of a restored table has to
/// be covered by exactly one file, since a tablet in two files would have two boundaries and
/// one in none no rows. Returns the tables covered, with their file names by tablet.
///
/// # Arguments
///
/// * `files` - The backup's files, as their manifests describe them
/// * `tables` - The tables the cluster serves, by name and identity
/// * `tablets` - How many tablets a table has
///
/// # Errors
///
/// Names the first file, table or tablet that breaks the rule.
pub fn judge_coverage(
    files: &[BackupFile],
    tables: &[(String, TableId)],
    tablets: u16,
) -> Result<BTreeMap<TableId, BTreeMap<u16, String>>, String> {
    if files.is_empty() {
        return Err("the backup holds no files".to_string());
    }
    let mut coverage: BTreeMap<TableId, BTreeMap<u16, String>> = BTreeMap::new();
    for file in files {
        // a table the cluster does not serve
        if !tables.iter().any(|(_, id)| *id == file.table) {
            return Err(format!(
                "{} holds table {} ({}), which this cluster does not serve",
                file.name, file.table_name, file.table
            ));
        }
        let covered = coverage.entry(file.table).or_default();
        for tablet in &file.tablets {
            // a tablet in two files has two boundaries
            if let Some(other) = covered.insert(*tablet, file.name.clone()) {
                return Err(format!(
                    "tablet {tablet} of table {} is in both {other} and {}, so has two boundaries",
                    file.table_name, file.name
                ));
            }
        }
    }
    // every tablet of every covered table, in exactly one file
    for (table, covered) in &coverage {
        for tablet in 0..tablets {
            if !covered.contains_key(&tablet) {
                let name = tables
                    .iter()
                    .find(|(_, id)| id == table)
                    .map(|(name, _)| name.as_str())
                    .unwrap_or("?");
                return Err(format!(
                    "tablet {tablet} of table {name} is in no file of the backup"
                ));
            }
        }
    }
    Ok(coverage)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A file covering tablets of a table
    fn file(name: &str, table: &str, tablets: Vec<u16>) -> BackupFile {
        BackupFile {
            name: name.to_string(),
            table: TableId::of(table),
            table_name: table.to_string(),
            tablets,
            boundary: 7,
            records: 3,
        }
    }

    /// A restore refuses a gap, an overlap and a foreign table, and accepts exact coverage
    #[test]
    fn a_restore_refuses_gaps_overlaps_and_a_foreign_table() {
        let tables = vec![
            ("Note".to_string(), TableId::of("Note")),
            ("Row".to_string(), TableId::of("Row")),
        ];
        // exact coverage of one table over four tablets, in two files
        let files = vec![
            file("a.snap", "Note", vec![0, 2]),
            file("b.snap", "Note", vec![1, 3]),
        ];
        let coverage = judge_coverage(&files, &tables, 4).expect("exact coverage is accepted");
        assert_eq!(coverage.len(), 1);
        assert_eq!(coverage[&TableId::of("Note")][&3], "b.snap");
        // a gap is named by tablet and table
        let gap = vec![
            file("a.snap", "Note", vec![0, 2]),
            file("b.snap", "Note", vec![1]),
        ];
        let error = judge_coverage(&gap, &tables, 4).expect_err("a gap is refused");
        assert!(
            error.contains("tablet 3") && error.contains("Note"),
            "{error}"
        );
        // an overlap is named by both files
        let overlap = vec![
            file("a.snap", "Note", vec![0, 1, 2]),
            file("b.snap", "Note", vec![2, 3]),
        ];
        let error = judge_coverage(&overlap, &tables, 4).expect_err("an overlap is refused");
        assert!(
            error.contains("a.snap") && error.contains("b.snap") && error.contains("tablet 2"),
            "{error}"
        );
        // a table the cluster does not serve
        let foreign = vec![file("a.snap", "Other", vec![0, 1, 2, 3])];
        let error = judge_coverage(&foreign, &tables, 4).expect_err("a foreign table is refused");
        assert!(
            error.contains("Other") && error.contains("does not serve"),
            "{error}"
        );
        // nothing at all
        assert!(judge_coverage(&[], &tables, 4).is_err());
        // a second table covered whole beside the first
        let both = vec![
            file("a.snap", "Note", (0..4).collect()),
            file("r.snap", "Row", (0..4).collect()),
        ];
        assert_eq!(
            judge_coverage(&both, &tables, 4).expect("two tables").len(),
            2
        );
    }
}
