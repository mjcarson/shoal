//! What a backup and a restore record, and what a recovery records
//!
//! A backup is an administrative operation committed to the control log
//! ([F49](../../../../docs/src/features/backup-and-recovery.md)): the record names the table
//! - or every table - the directory the files go under, and every group the placement
//! derives, with each group's progress as its driver proposes it. It is **not** one
//! cross-tablet snapshot: every group's file is cut at a committed boundary of its own, which
//! the record names, and the files together are a backup of the tables at those boundaries. Once
//! every group is done, every node writes the record beside the files as `backup.json`
//! ([`BackupRecordFile`]), which a restore reads before any file: a table or a bucket every group of
//! which failed leaves no file, and only the record says so (item 322). A
//! restore is the same shape driven the other way, on a **new** cluster: every group's leader
//! builds a file for its tablets from the backup's files and installs it on every member
//! through the repair install path, and the cluster records the cluster it was restored from
//! so the old identities are refused by name. A recovery is what `force_recover` writes when
//! an operator rewrites a stopped survivor's membership after a permanent majority loss.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use tracing::{event, Level};
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
        /// How many tries the group's backup took, this one among them, after refusals for now;
        /// none recorded by a build before they were counted (item 322)
        #[serde(default)]
        attempts: u32,
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
    /// The groups whose objects are archived rather than whose rows are copied, with the bucket
    /// each is the `ObjectMeta` table of: a bucket is backed up as its objects, never as its rows
    /// ([F93](../../../../docs/src/features/operations-and-the-real-cluster.md))
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub archives: BTreeMap<GroupId, String>,
    /// Every group's table and the tablets it served when the backup was asked for, which the
    /// record written beside the files names: a group's identity is a hash, so nothing else says
    /// which table a group that left no file was of (item 322). Empty in a record applied by a
    /// build before it
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub placed: BTreeMap<GroupId, PlacedGroup>,
}

impl BackupRecord {
    /// Whether every group is done
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.groups.values().all(GroupBackup::is_done)
    }
}

/// Where one group of a backup was placed when the backup was asked for (item 322)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacedGroup {
    /// The table the group serves
    pub table: TableId,
    /// The tablets it served
    pub tablets: Vec<u16>,
}

/// What the record written beside a backup's files is: `backup` (item 322)
pub const BACKUP_RECORD_KIND: &str = "backup";

/// The format of the record written beside a backup's files (item 322)
pub const BACKUP_RECORD_FORMAT: u16 = 1;

pub use crate::shared::protocol::admin::BACKUP_RECORD_FILE;

/// The record of a backup written beside its files once it ends, as `<path>/<op>/backup.json`
/// (item 322)
///
/// Every group the backup was asked for, where it was placed and what it came to. A restore
/// reads it before anything else and refuses a backup without it: a table or a bucket every group
/// of which failed leaves no file, and nothing but the record tells it from one the backup never
/// held, nor a backup gathered short of a file from one gathered whole. Every node writes the same
/// bytes from its committed state, so wherever the files are gathered from, the record is with
/// them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupRecordFile {
    /// What the file is: `backup`
    pub kind: String,
    /// Its format
    pub format: u16,
    /// The backup operation
    pub op: Uuid,
    /// The cluster the backup was taken in
    pub cluster: ClusterId,
    /// The table the backup was asked for, or none for every table
    pub table: Option<TableId>,
    /// Who asked
    pub principal: String,
    /// Every group of the backup, and what it came to
    pub groups: BTreeMap<GroupId, RecordedGroup>,
}

/// One group of a backup as its record names it (item 322)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedGroup {
    /// The table the group serves, if the record applied by this build knew it
    pub table: Option<TableId>,
    /// The table's name as the schema spells it, if known
    pub table_name: Option<String>,
    /// The tablets it served when the backup was asked for
    pub tablets: Vec<u16>,
    /// The bucket whose objects it archives, if it is a bucket's `ObjectMeta` group
    pub bucket: Option<String>,
    /// The node that drove it, if one did
    pub driver: Option<NodeId>,
    /// What its backup came to
    pub outcome: BackupOutcome,
}

impl RecordedGroup {
    /// What a refusal calls the files this group's kind writes, by its table or its bucket
    fn what(&self) -> String {
        match (&self.bucket, &self.table_name) {
            (Some(bucket), _) => format!("archives of bucket {bucket}"),
            (None, Some(table)) => format!("groups of table {table}"),
            (None, None) => "groups of a table this record does not name".to_string(),
        }
    }
}

impl BackupRecordFile {
    /// The record of a backup that has ended, or none while any of its groups is not done
    ///
    /// # Arguments
    ///
    /// * `record` - The backup, as the control state holds it
    /// * `cluster` - The cluster it was taken in
    /// * `tables` - The tables the cluster serves, by name and identity
    #[must_use]
    pub fn of(record: &BackupRecord, cluster: ClusterId, tables: &[(String, TableId)]) -> Option<Self> {
        // a record is written once, when nothing more will happen to any group
        if !record.is_done() {
            return None;
        }
        let groups = record
            .groups
            .iter()
            .map(|(group, progress)| {
                // where the group was placed, which a record applied by an older build lacks
                let placed = record.placed.get(group);
                let table = placed.map(|placed| placed.table);
                let table_name = table.and_then(|id| {
                    tables
                        .iter()
                        .find(|(_, served)| *served == id)
                        .map(|(name, _)| name.clone())
                });
                let recorded = RecordedGroup {
                    table,
                    table_name,
                    tablets: placed.map(|placed| placed.tablets.clone()).unwrap_or_default(),
                    bucket: record.archives.get(group).cloned(),
                    driver: progress.driver,
                    // a group done with no outcome is one nothing can say was written
                    outcome: progress.outcome.clone().unwrap_or_else(|| BackupOutcome::Failed {
                        reason: "the group is done with no outcome recorded".to_string(),
                    }),
                };
                (*group, recorded)
            })
            .collect();
        Some(BackupRecordFile {
            kind: BACKUP_RECORD_KIND.to_string(),
            format: BACKUP_RECORD_FORMAT,
            op: record.op,
            cluster,
            table: record.table,
            principal: record.principal.clone(),
            groups,
        })
    }
}

/// Turn a glommio error into an io error, for the record's write
///
/// # Arguments
///
/// * `error` - The glommio error
fn glommio_io<T>(error: glommio::GlommioError<T>) -> std::io::Error {
    match error {
        glommio::GlommioError::IoError(error) => error,
        other => std::io::Error::other(other.to_string()),
    }
}

/// Write a backup's record beside its files, atomically, as one node of many writing the same
/// bytes (item 322)
///
/// The record is staged under a name of this node's own, synced and renamed over
/// `backup.json`, then the directory is synced: a crash leaves the record whole or not there, and
/// nodes writing into one directory - a shared filesystem, or the fixture's children on one host -
/// never write one staged file between them.
///
/// # Arguments
///
/// * `dir` - The backup's directory, `<path>/<op>`, made if it is not there
/// * `writer` - The node writing it
/// * `bytes` - The record
///
/// # Errors
///
/// Fails as making the directory, writing, syncing or renaming does.
pub async fn write_record_file(
    dir: &std::path::Path,
    writer: NodeId,
    bytes: Vec<u8>,
) -> std::io::Result<()> {
    use glommio::io::{BufferedFile, Directory};
    std::fs::create_dir_all(dir)?;
    // staged under this node's own name beside the record
    let staged = dir.join(format!("{BACKUP_RECORD_FILE}.{writer}.tmp"));
    let mut file = BufferedFile::create(&staged).await.map_err(glommio_io)?;
    file.write_at(bytes, 0).await.map_err(glommio_io)?;
    file.fdatasync().await.map_err(glommio_io)?;
    // swapped in, which is the atomic step, and made durable
    file.rename(dir.join(BACKUP_RECORD_FILE))
        .await
        .map_err(glommio_io)?;
    file.close().await.map_err(glommio_io)?;
    let directory = Directory::open(dir).await.map_err(glommio_io)?;
    directory.sync().await.map_err(glommio_io)?;
    directory.close().await.map_err(glommio_io)?;
    Ok(())
}

/// Read the record written beside a backup's files (item 322)
///
/// # Arguments
///
/// * `dir` - The backup's directory, `<path>/<op>`
///
/// # Errors
///
/// A backup without one is refused by name, as is a record that does not read, is of another
/// kind, or is of a format past this build's.
pub fn read_record(dir: &std::path::Path) -> Result<BackupRecordFile, String> {
    let path = dir.join(BACKUP_RECORD_FILE);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        // a backup taken before the record, or gathered without it, cannot be shown whole
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(format!(
                "{} holds no {BACKUP_RECORD_FILE}, the record every backup writes beside its files of \
                 what each of its groups came to: without it a table or a bucket every group of which \
                 failed cannot be told from one the backup never held. A backup taken by a build \
                 before the record is restored with a build before it; one gathered without it needs \
                 it copied beside its files from any node of the cluster that took it",
                dir.display()
            ));
        }
        Err(error) => return Err(format!("reading {}: {error}", path.display())),
    };
    let record: BackupRecordFile = serde_json::from_slice(&bytes)
        .map_err(|error| format!("{} is not a backup's record: {error}", path.display()))?;
    if record.kind != BACKUP_RECORD_KIND {
        return Err(format!(
            "{} is a record of kind {:?}, not a backup's",
            path.display(),
            record.kind
        ));
    }
    if record.format > BACKUP_RECORD_FORMAT {
        return Err(format!(
            "{} is of format {}, past this build's {BACKUP_RECORD_FORMAT}",
            path.display(),
            record.format
        ));
    }
    Ok(record)
}

/// Judge a backup's files against its record: every group the record names written has its file
/// under the directory, no group failed, and every file found is one the record names written
/// (item 322)
///
/// # Arguments
///
/// * `record` - The backup's record
/// * `found` - Every file found under the directory, by the group its manifest names
/// * `dir` - The directory, for the refusal
///
/// # Errors
///
/// Names each table and bucket with a group that failed, with how many of its groups did and the
/// first reason; or each with a written file missing, with the first file and the node that wrote
/// it; or the first file found that the record does not name written.
pub fn judge_record(
    record: &BackupRecordFile,
    found: &BTreeMap<GroupId, String>,
    dir: &std::path::Path,
) -> Result<(), String> {
    // by what each group's kind writes: its table's groups or its bucket's archives
    let mut failed: BTreeMap<String, (usize, String)> = BTreeMap::new();
    let mut missing: BTreeMap<String, (usize, String)> = BTreeMap::new();
    let mut counted: BTreeMap<String, usize> = BTreeMap::new();
    for (group, recorded) in &record.groups {
        let what = recorded.what();
        *counted.entry(what.clone()).or_default() += 1;
        match &recorded.outcome {
            BackupOutcome::Failed { reason } => {
                let entry = failed
                    .entry(what)
                    .or_insert_with(|| (0, format!("group {group}: {reason}")));
                entry.0 += 1;
            }
            BackupOutcome::Written { file, .. } if !found.contains_key(group) => {
                let by = recorded
                    .driver
                    .map_or_else(String::new, |node| format!(", written on node {node}"));
                let entry = missing.entry(what).or_insert_with(|| (0, format!("{file}{by}")));
                entry.0 += 1;
            }
            BackupOutcome::Written { .. } | BackupOutcome::Skipped { .. } => {}
        }
    }
    // what each kind came to, as a refusal names it
    let named = |by: &BTreeMap<String, (usize, String)>, came: &str| -> String {
        by.iter()
            .map(|(what, (count, first))| {
                let of = counted.get(what).copied().unwrap_or(*count);
                format!("{what}: {count} of {of} {came}, the first {first}")
            })
            .collect::<Vec<_>>()
            .join("; ")
    };
    // a failed group leaves its tablets out of whatever a restore would put back
    if !failed.is_empty() {
        return Err(format!(
            "backup {} cannot be restored whole, since some of its groups failed - {} - and a restore \
             would put their tables back without the tablets those groups served: take the backup again",
            record.op,
            named(&failed, "failed")
        ));
    }
    // a written file not here was left behind when the backup was gathered
    if !missing.is_empty() {
        return Err(format!(
            "backup {} was not gathered whole into {}: {}; copy them from the nodes that wrote them, \
             which `shoaladm ship-backup` does",
            record.op,
            dir.display(),
            named(&missing, "have no file under it")
        ));
    }
    // and nothing here that the backup did not write
    for (group, name) in found {
        match record.groups.get(group).map(|recorded| &recorded.outcome) {
            Some(BackupOutcome::Written { .. }) => {}
            Some(outcome) => {
                return Err(format!(
                    "{name} is under {}, but backup {}'s record says its group {group} came to {outcome:?}",
                    dir.display(),
                    record.op
                ));
            }
            None => {
                return Err(format!(
                    "{name} is under {}, but is no file of backup {}: its record names no group {group}",
                    dir.display(),
                    record.op
                ));
            }
        }
    }
    Ok(())
}

/// What a backup file's manifest carries, as `<file>.json` beside every `.snap`
///
/// A restore reads what a file is without reading the file: the cluster it was cut in, its
/// table's own fingerprint, the group, the boundary, the tablets, the records and the checksum. The
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
    /// The table's own fingerprint as the build that cut the file held it, which a restore
    /// judges the file by ([F93](../../../../docs/src/features/operations-and-the-real-cluster.md))
    pub table_fingerprint: u64,
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
            table_fingerprint: manifest.table_fingerprint,
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
            table_fingerprint: self.table_fingerprint,
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
    /// The file the group's install put on every member, from the commit that names `Verifying`
    /// on, which a drive resumed past the install reports as its own (item 327)
    #[serde(default)]
    pub installed: Option<InstalledFile>,
    /// The term of the group's lead its drive drives under, the highest any drive of this try has
    /// committed under once [`RESTORE_FENCE_WIRE_VERSION`](crate::shared::protocol::RESTORE_FENCE_WIRE_VERSION)
    /// is activated, which refuses a drive under an older lead (item 336)
    #[serde(default)]
    pub term: u64,
}

/// The file a group's restore installed, as its outcome reports it (item 327)
///
/// Kept in the group's record from the commit that names `Verifying` on: a drive that resumes the
/// group there, because the one that installed lost its lead or met a member it could not reach,
/// never built the file and cannot count it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstalledFile {
    /// The committed boundary the file was installed at
    pub boundary: u64,
    /// How many records it holds
    pub records: u64,
    /// How many bytes it takes
    pub bytes: u64,
    /// How many remembered requests its trailer holds
    pub retries: u32,
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
            installed: None,
            term: 0,
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
    /// driver, the outcome, the attempts and the file installed are cleared, and the generation
    /// moves on, and the lead a drive of the next try drives under is its own (item 336).
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
        // the install is done again, and its own file is the one the record will name
        self.installed = None;
        // the next try's drives are fenced among themselves, by the generation from this one's
        self.term = 0;
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
    /// The table's own fingerprint as the build that cut it held it
    pub table_fingerprint: u64,
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
    /// Who asked
    pub principal: String,
    /// The topology version the request was applied at
    pub requested_at: u64,
    /// Every file the restore reads, as judged at apply
    pub files: Vec<BackupFile>,
    /// Every group of every restored table, and where each stands
    pub groups: BTreeMap<GroupId, GroupRestore>,
    /// Every archive of a bucket's objects the restore puts again, by its name under the backup
    /// directory, once every group is done ([F93](../../../../docs/src/features/operations-and-the-real-cluster.md))
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub archives: BTreeMap<String, ArchiveRestore>,
}

impl RestoreRecord {
    /// Whether every group is done, and every archive put again
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.groups.values().all(GroupRestore::is_done)
            && self.archives.values().all(ArchiveRestore::is_done)
    }

    /// Whether every group is done, so the archives can be put again
    #[must_use]
    pub fn tables_done(&self) -> bool {
        self.groups.values().all(GroupRestore::is_done)
    }
}

/// One archive of a bucket's objects, as a restore judges it from its manifest
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveFile {
    /// The archive's name, relative to the backup directory
    pub name: String,
    /// The bucket, by its name
    pub bucket: String,
    /// The bucket's identity
    pub bucket_id: crate::shared::identity::BucketId,
    /// The layout of the object rows its entries were written from
    pub rows_layout: u64,
    /// The archive format
    pub format: u16,
    /// How many objects it holds
    pub records: u64,
    /// How many bytes of objects
    pub bytes: u64,
}

/// Where an archive's restore stands
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArchivePhase {
    /// Nobody has put any of it yet, or the tables are not restored
    Pending,
    /// Its objects are being put again, `put` of them so far
    Putting,
    /// Nothing more will happen to it under this operation
    Done,
}

/// One archive's progress under a restore operation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveRestore {
    /// The archive
    pub file: ArchiveFile,
    /// The member whose node puts it again: any, since every host holds every file once the
    /// backup is shipped, dealt round the placement
    pub node: NodeId,
    /// Where it stands
    pub phase: ArchivePhase,
    /// How many of its objects are put again, in archive order, which a driver resumes past
    pub put: u64,
    /// How many bytes of objects they hold
    pub bytes: u64,
    /// Why it failed, once it did
    pub failed: Option<String>,
    /// How many times a retry put it back, so a driver of an earlier try is told apart
    pub generation: u32,
}

impl ArchiveRestore {
    /// Whether nothing more will happen to this archive under its operation
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.phase == ArchivePhase::Done
    }

    /// Whether this archive's restore ended in a failure
    #[must_use]
    pub fn failed(&self) -> bool {
        self.is_done() && self.failed.is_some()
    }

    /// Put a failed archive back to be put again, past the objects already put
    pub fn retry(&mut self) {
        self.phase = ArchivePhase::Pending;
        self.failed = None;
        self.generation += 1;
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

/// Read a backup's record and every manifest under its directory
///
/// The record written beside the files comes first (item 322): a backup without one is refused,
/// and its files have to be exactly the ones it names written - no group failed, no file missing,
/// none it does not name, and one file a group. Then every `*.snap.json` and `*.objects.json` is
/// returned as one [`BackupFile`] or [`ArchiveFile`], named by its file relative to the directory,
/// with the cluster the record names, which every file has to agree on. Each file carries its own
/// table's fingerprint and is judged by it alone, so a backup holds no schema of its own
/// ([F93](../../../../docs/src/features/operations-and-the-real-cluster.md)).
///
/// # Arguments
///
/// * `dir` - The backup directory, on this node
///
/// # Errors
///
/// Names a missing or unreadable record, what the record says the files lack or hold besides, the
/// first manifest that does not read, or the first that names another cluster or another backup
/// than the record.
pub fn scan_backup(
    dir: &std::path::Path,
) -> Result<(ClusterId, Vec<BackupFile>, Vec<ArchiveFile>), String> {
    // the record first, which says what every group of the backup came to (item 322)
    let record = read_record(dir)?;
    // every manifest under the directory, in name order so the judgment is deterministic
    let mut manifests = Vec::new();
    let mut archive_manifests = Vec::new();
    walk_manifests(dir, dir, &mut manifests, &mut archive_manifests)
        .map_err(|error| format!("reading {}: {error}", dir.display()))?;
    manifests.sort();
    archive_manifests.sort();
    // every file found, by the group its manifest names, which the record judges
    let mut found: BTreeMap<GroupId, String> = BTreeMap::new();
    // a file of the backup is of its operation and its cluster, and the only one of its group
    let mut belongs = |name: &str, op: Uuid, cluster: ClusterId, group: GroupId| -> Result<(), String> {
        if cluster != record.cluster {
            return Err(format!(
                "{name} was cut in cluster {cluster}, and the record beside it names {}",
                record.cluster
            ));
        }
        if op != record.op {
            return Err(format!(
                "{name} was written by backup {op}, and the record beside it is backup {}'s",
                record.op
            ));
        }
        if let Some(other) = found.insert(group, name.to_string()) {
            return Err(format!(
                "{name} and {other} are both files of group {group}, which writes one: a try's \
                 file was left beside the one the backup's record names"
            ));
        }
        Ok(())
    };
    let mut files = Vec::with_capacity(manifests.len());
    for (name, path) in manifests {
        let bytes =
            std::fs::read(&path).map_err(|error| format!("reading {}: {error}", path.display()))?;
        let manifest: BackupManifest = serde_json::from_slice(&bytes)
            .map_err(|error| format!("{} is not a backup manifest: {error}", path.display()))?;
        belongs(&name, manifest.op, manifest.cluster, manifest.group)?;
        files.push(BackupFile {
            name,
            table: manifest.table,
            table_name: manifest.table_name,
            table_fingerprint: manifest.table_fingerprint,
            tablets: manifest.tablets,
            boundary: manifest.boundary,
            records: manifest.records,
        });
    }
    // and every archive of a bucket's objects, with the tablets each names kept by bucket for the
    // judgment below
    let mut archives = Vec::with_capacity(archive_manifests.len());
    let mut by_bucket: BTreeMap<String, Vec<(String, Vec<u16>)>> = BTreeMap::new();
    for (name, path) in archive_manifests {
        let manifest = crate::server::objects::archive::read_manifest(&path)?;
        belongs(&name, manifest.op, manifest.cluster, manifest.group)?;
        by_bucket
            .entry(manifest.bucket.clone())
            .or_default()
            .push((name.clone(), manifest.tablets.clone()));
        archives.push(ArchiveFile {
            name,
            bucket: manifest.bucket,
            bucket_id: manifest.bucket_id,
            rows_layout: manifest.rows_layout,
            format: manifest.format,
            records: manifest.records,
            bytes: manifest.bytes,
        });
    }
    // the files against the record: no group failed, none missing and none it does not name, so a
    // table or a bucket every group of which failed is refused rather than restored empty
    judge_record(&record, &found, dir)?;
    if files.is_empty() && archives.is_empty() {
        return Err(format!("{} holds no backup manifests", dir.display()));
    }
    // every bucket's archives cover its tablets once each, or the restore would finish without
    // the objects of a group whose archive failed (item 315); a tablet id is twelve bits, so the
    // count fits a u16
    #[allow(clippy::cast_possible_truncation)]
    let tablets = crate::server::ring::TABLET_COUNT as u16;
    for (bucket, archived) in &by_bucket {
        judge_archive_coverage(bucket, archived, tablets)?;
    }
    Ok((record.cluster, files, archives))
}

/// Judge one bucket's archives of a backup: every tablet of its `ObjectMeta` table in exactly one
///
/// A group whose archive failed leaves no manifest, and a restore puts again only the archives it
/// finds, so without this a restore finished with every archive it knew of done while the objects
/// of a failed group's tablets were nowhere ([item 315](../../../../docs/src/appendix/resolved/archive-coverage.md)).
/// An archive written before archives named their tablets cannot be judged: a bucket all of whose
/// archives are of that kind restores as it did before, said at WARN, and one that mixes the two
/// kinds is refused, since it cannot be shown whole.
///
/// # Arguments
///
/// * `bucket` - The bucket, by its name
/// * `archived` - Each of its archives, by name, with the tablets its manifest names
/// * `tablets` - How many tablets a table has
///
/// # Errors
///
/// Names the bucket and the first tablet in no archive or in two, or the archive that names none
/// beside ones that do.
pub fn judge_archive_coverage(bucket: &str, archived: &[(String, Vec<u16>)], tablets: u16) -> Result<(), String> {
    // archives written before their manifests named tablets, which nothing can judge
    let unnamed: Vec<&str> = archived
        .iter()
        .filter(|(_, covered)| covered.is_empty())
        .map(|(name, _)| name.as_str())
        .collect();
    if unnamed.len() == archived.len() {
        event!(
            Level::WARN,
            msg = "a bucket's archives name no tablets, so a restore cannot judge them whole",
            bucket,
            archives = archived.len(),
        );
        return Ok(());
    }
    if let Some(name) = unnamed.first() {
        return Err(format!(
            "{name} names no tablets of bucket {bucket} beside archives that do, so the bucket cannot be shown whole"
        ));
    }
    // each tablet in one archive, since a tablet in two would be put again twice
    let mut covered: BTreeMap<u16, &str> = BTreeMap::new();
    for (name, names) in archived {
        for tablet in names {
            if let Some(other) = covered.insert(*tablet, name) {
                return Err(format!(
                    "tablet {tablet} of bucket {bucket} is in both {other} and {name}"
                ));
            }
        }
    }
    // and in some archive, or its group's objects would not be restored
    match (0..tablets).find(|tablet| !covered.contains_key(tablet)) {
        Some(tablet) => Err(format!(
            "tablet {tablet} of bucket {bucket} is in no archive of the backup: the archive of the group \
             that served it failed or is missing, so its objects would not be restored"
        )),
        None => Ok(()),
    }
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
    archives: &mut Vec<(String, std::path::PathBuf)>,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            walk_manifests(root, &path, found, archives)?;
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        // an archive of a bucket's objects, named by its `.objects` relative to the root (F93)
        if let Some(archive) = file_name.strip_suffix(".json") {
            if archive.ends_with(crate::server::objects::archive::ARCHIVE_SUFFIX) {
                let relative = path
                    .parent()
                    .and_then(|parent| parent.strip_prefix(root).ok())
                    .map(|rel| rel.join(archive))
                    .unwrap_or_else(|| std::path::PathBuf::from(archive));
                archives.push((relative.to_string_lossy().into_owned(), path));
                continue;
            }
        }
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
            table_fingerprint: 0,
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
        // no table file at all is a backup of a bucket's archives alone since F93, which
        // `scan_backup` refuses only when it holds no archive either
        assert!(judge_coverage(&[], &tables, 4).is_ok_and(|covered| covered.is_empty()));
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

    /// A bucket's archives are judged as a table's files are: every tablet in exactly one, a gap
    /// and an overlap refused by name. Archives that all name no tablets, written before they were
    /// named, cannot be judged and restore as before; one of them beside archives that name theirs
    /// is refused (item 315)
    #[test]
    fn a_buckets_archives_are_judged_whole() {
        // an archive naming the tablets of the group it was written for
        let archive = |name: &str, tablets: Vec<u16>| (name.to_string(), tablets);
        // every tablet in exactly one archive
        let whole = vec![archive("Reels/a.objects", vec![0, 2]), archive("Reels/b.objects", vec![1, 3])];
        assert!(judge_archive_coverage("Reels", &whole, 4).is_ok());
        // a group whose archive failed left a gap
        let gap = vec![archive("Reels/a.objects", vec![0, 2])];
        let error = judge_archive_coverage("Reels", &gap, 4).expect_err("a gap is refused");
        assert!(
            error.contains("tablet 1") && error.contains("Reels") && error.contains("in no archive"),
            "{error}"
        );
        // a tablet in two archives would be put again twice
        let overlap = vec![archive("Reels/a.objects", vec![0, 1, 2]), archive("Reels/b.objects", vec![2, 3])];
        let error = judge_archive_coverage("Reels", &overlap, 4).expect_err("an overlap is refused");
        assert!(
            error.contains("tablet 2") && error.contains("Reels/a.objects") && error.contains("Reels/b.objects"),
            "{error}"
        );
        // archives written before they named tablets cannot be judged, and restore as before
        let unnamed = vec![archive("Reels/a.objects", Vec::new()), archive("Reels/b.objects", Vec::new())];
        assert!(judge_archive_coverage("Reels", &unnamed, 4).is_ok());
        // but one of them beside archives that name theirs leaves the bucket unprovable
        let mixed = vec![archive("Reels/a.objects", vec![0, 1, 2, 3]), archive("Reels/b.objects", Vec::new())];
        let error = judge_archive_coverage("Reels", &mixed, 4).expect_err("a mix is refused");
        assert!(error.contains("Reels/b.objects") && error.contains("names no tablets"), "{error}");
    }

    /// A group's file, as its record names it written
    ///
    /// # Arguments
    ///
    /// * `file` - The file
    fn written(file: &str) -> BackupOutcome {
        BackupOutcome::Written {
            file: file.to_string(),
            bytes: 10,
            checksum: 1,
            records: 2,
            retries: 0,
            attempts: 1,
        }
    }

    /// A backup's record of three groups: two of a table's and one of a bucket's archives
    ///
    /// # Arguments
    ///
    /// * `outcomes` - What each came to, the table's first
    fn recorded(outcomes: [BackupOutcome; 3]) -> BackupRecordFile {
        let [first, second, archive] = outcomes;
        let group = |table: &str, bucket: Option<&str>, outcome: BackupOutcome| RecordedGroup {
            table: Some(TableId::of(table)),
            table_name: Some(table.to_string()),
            tablets: vec![0, 1],
            bucket: bucket.map(str::to_string),
            driver: Some(NodeId::mint()),
            outcome,
        };
        BackupRecordFile {
            kind: BACKUP_RECORD_KIND.to_string(),
            format: BACKUP_RECORD_FORMAT,
            op: Uuid::new_v4(),
            cluster: ClusterId::mint(),
            table: None,
            principal: "admin".to_string(),
            groups: [
                (GroupId(1), group("Doc", None, first)),
                (GroupId(2), group("Doc", None, second)),
                (GroupId(3), group("ReelsObjectMeta", Some("Reels"), archive)),
            ]
            .into_iter()
            .collect(),
        }
    }

    /// A restore judges a backup's files by its record: a failed group, a written file missing and
    /// a file the record does not name are each refused by name, a skipped group needs no file, and
    /// files exactly as recorded are accepted (item 322)
    #[test]
    fn a_record_refuses_a_failed_group_a_missing_file_and_a_file_it_does_not_name() {
        let dir = std::path::Path::new("/backups/x");
        let found: BTreeMap<GroupId, String> = [
            (GroupId(1), "Doc/a.snap".to_string()),
            (GroupId(2), "Doc/b.snap".to_string()),
            (GroupId(3), "Reels/c.objects".to_string()),
        ]
        .into_iter()
        .collect();
        // every file as its record names it
        let whole = recorded([written("a"), written("b"), written("c")]);
        judge_record(&whole, &found, dir).expect("files as their record names them are accepted");
        // every group of the table failed, which left no file of it: refused, naming the table
        let failure = || BackupOutcome::Failed {
            reason: "a cut timed out".to_string(),
        };
        let failed = recorded([failure(), failure(), written("c")]);
        let archive_only: BTreeMap<GroupId, String> =
            [(GroupId(3), "Reels/c.objects".to_string())].into_iter().collect();
        let error = judge_record(&failed, &archive_only, dir).expect_err("a failed table is refused");
        assert!(
            error.contains("groups of table Doc: 2 of 2 failed") && error.contains("a cut timed out"),
            "{error}"
        );
        // a written archive not gathered: refused, naming the bucket
        let tables_only: BTreeMap<GroupId, String> = found
            .iter()
            .filter(|(group, _)| group.0 != 3)
            .map(|(group, name)| (*group, name.clone()))
            .collect();
        let error = judge_record(&whole, &tables_only, dir).expect_err("a missing archive is refused");
        assert!(
            error.contains("not gathered whole") && error.contains("archives of bucket Reels: 1 of 1"),
            "{error}"
        );
        // a file the record does not name, as another backup's left in the directory
        let mut stray = found.clone();
        stray.insert(GroupId(9), "Doc/z.snap".to_string());
        let error = judge_record(&whole, &stray, dir).expect_err("a stray file is refused");
        assert!(error.contains("Doc/z.snap") && error.contains("names no group"), "{error}");
        // and a skipped group leaves no file to find
        let skipped = recorded([
            written("a"),
            written("b"),
            BackupOutcome::Skipped {
                reason: "derived".to_string(),
            },
        ]);
        judge_record(&skipped, &tables_only, dir).expect("a skipped group needs no file");
    }

    /// A record is made only once every group of its backup is done, names each group's table and
    /// tablets, and reads back as it was written (item 322)
    #[test]
    fn a_record_is_made_once_its_backup_ends() {
        let table = TableId::of("Doc");
        let mut record = BackupRecord {
            op: Uuid::new_v4(),
            table: None,
            path: "/backups".to_string(),
            principal: "admin".to_string(),
            requested_at: 1,
            groups: [(GroupId(1), GroupBackup::default())].into_iter().collect(),
            archives: BTreeMap::new(),
            placed: [(
                GroupId(1),
                PlacedGroup {
                    table,
                    tablets: vec![0, 1],
                },
            )]
            .into_iter()
            .collect(),
        };
        let tables = vec![("Doc".to_string(), table)];
        let cluster = ClusterId::mint();
        assert!(
            BackupRecordFile::of(&record, cluster, &tables).is_none(),
            "a backup still running has no record"
        );
        record.groups.insert(
            GroupId(1),
            GroupBackup {
                phase: BackupPhase::Done,
                driver: None,
                boundary: Some(4),
                outcome: Some(written("Doc/a.snap")),
            },
        );
        let file = BackupRecordFile::of(&record, cluster, &tables).expect("an ended backup has a record");
        assert_eq!((file.op, file.cluster), (record.op, cluster));
        assert_eq!(file.groups[&GroupId(1)].table_name.as_deref(), Some("Doc"));
        assert_eq!(file.groups[&GroupId(1)].tablets, vec![0, 1]);
        let bytes = serde_json::to_vec_pretty(&file).expect("a record serializes");
        let read: BackupRecordFile = serde_json::from_slice(&bytes).expect("a record reads back");
        assert_eq!(read, file);
    }

    /// A backup without its record is refused by the record's name, and one with a record of
    /// another kind or a later format is refused too (item 322)
    #[test]
    fn a_backup_without_its_record_is_refused_by_name() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let error = scan_backup(dir.path()).expect_err("a backup without its record is refused");
        assert!(error.contains(BACKUP_RECORD_FILE), "{error}");
        // a record of a later format than this build reads
        let mut later = recorded([written("a"), written("b"), written("c")]);
        later.format = BACKUP_RECORD_FORMAT + 1;
        std::fs::write(
            dir.path().join(BACKUP_RECORD_FILE),
            serde_json::to_vec(&later).expect("a record serializes"),
        )
        .expect("the record writes");
        let error = read_record(dir.path()).expect_err("a later format is refused");
        assert!(error.contains("past this build's"), "{error}");
        // and a file of another kind under the record's name
        later.format = BACKUP_RECORD_FORMAT;
        later.kind = "objects".to_string();
        std::fs::write(
            dir.path().join(BACKUP_RECORD_FILE),
            serde_json::to_vec(&later).expect("a record serializes"),
        )
        .expect("the record writes");
        let error = read_record(dir.path()).expect_err("another kind is refused");
        assert!(error.contains("not a backup's"), "{error}");
    }
}
