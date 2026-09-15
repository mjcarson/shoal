//! Exporting a standalone node's data as a backup a cluster restores
//!
//! A standalone directory is never served by a cluster configuration
//! (`StandaloneDirectoryInCluster`), and a marker is never migrated in place: a cluster
//! node's files are laid out per slot under identities every peer keys by, and a standalone
//! node's per tablet under none, so a directory started in the other mode would serve rows
//! from the wrong place. The supported path from single node data to a cluster is this
//! ([F49](../../../docs/src/features/backup-and-recovery.md)): with the source stopped, an
//! operator runs [`export_standalone`] with the source's configuration, which folds every
//! intent log of the source into its archives - what the source's own next start would do -
//! and writes every persistent table's archives as one snapshot file with a backup manifest
//! beside it, the shape a `Backup` writes and a `Restore` reads. A fresh cluster, initialized
//! at whatever factor and size the operator wants, restores the export the way it restores a
//! backup: every group's copies installed from the file's records of its tablets, quarantined
//! until a scrub agrees, then served. The source is the rollback: nothing of it is written but
//! the fold, and it starts standalone afterwards with every row.
//!
//! What is exported is the archives, never the intent logs or anything ephemeral: after the
//! fold there is nothing in the logs the archives do not hold, and an ephemeral table's rows
//! were the source's memory. The export is one directory into one directory; a table under
//! its own `storage.tables` root is refused by name rather than half exported.

use std::path::{Path, PathBuf};

use glommio::{LocalExecutorBuilder, Placement};
use serde::{Deserialize, Serialize};
use tracing::{event, instrument, Level};
use uuid::Uuid;

use super::conf::Conf;
use super::control::backup::BackupManifest;
use super::database::ShoalDatabase;
use super::errors::ShoalError;
use super::meta::{DirectoryLock, MarkerMode, StorageMeta};
use super::rehome::shard_name;
use super::replication::snapshot::{SnapshotProvenance, SNAPSHOT_V2_FROM_WIRE};
use super::ServerError;
use crate::shared::identity::{ClusterId, GroupId, NodeId, TableId};
use crate::shared::traits::QuerySupport;

/// What an export did, and where the rollback is
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportReport {
    /// The export's operation, which every manifest carries
    pub op: Uuid,
    /// The standalone node the data came from, which every file names as its origin
    pub source_node: NodeId,
    /// The identity the export is filed under, since a backup names the cluster it was cut in
    ///
    /// Minted here: a standalone node has none, and a restore refuses a backup cut in the
    /// cluster restoring it, so it has to be one that no cluster is.
    pub export_cluster: ClusterId,
    /// The directory it came from, which is the rollback: it still starts standalone
    pub rollback: PathBuf,
    /// The directory the files went into, which a `Restore` is given
    pub target: PathBuf,
    /// The persistent tables exported, by name, with the records each holds
    pub tables: Vec<(String, u64)>,
    /// How many executors the source's files were laid out on
    pub executors: usize,
    /// How many intent log frames were folded into the source's archives before the export
    pub rows_folded: u64,
    /// How many bytes the files took
    pub bytes_written: u64,
}

/// Export a stopped standalone directory as a backup into an empty directory
///
/// Refuses by name a configuration with a `cluster:` block, a source that is not a standalone
/// node's, a source another process holds, a target that is not empty, and a persistent
/// table under its own storage root. Holds the source's lock throughout.
///
/// # Arguments
///
/// * `conf` - The source's configuration, which names its directory and its tables
/// * `target` - The directory to write the export into
///
/// # Errors
///
/// Fails naming what was wrong; a refused export writes nothing to either directory.
#[instrument(name = "export::export_standalone", skip_all, err(Debug))]
pub fn export_standalone<S: ShoalDatabase>(
    conf: &Conf,
    target: &Path,
) -> Result<ExportReport, ServerError> {
    let source = conf
        .storage
        .default
        .filesystem
        .latency_sensitive
        .path
        .clone();
    // the configuration is the source's: a standalone node's, or the tables and roots it
    // names are not the source's
    if conf.cluster.is_some() {
        return Err(ServerError::Shoal(ShoalError::InvalidConfig(
            "an export is run with the standalone node's own configuration, which has no cluster: block".to_string(),
        )));
    }
    // the source: a standalone node's, stopped
    let _source_lock = DirectoryLock::acquire(&source)?;
    let marker = StorageMeta::read(&source)?.ok_or_else(|| {
        ServerError::Shoal(ShoalError::InvalidConfig(format!(
            "{} holds no storage marker; nothing to export",
            source.display()
        )))
    })?;
    if marker.mode != MarkerMode::Standalone || marker.cluster.is_some() {
        return Err(ServerError::Shoal(ShoalError::InvalidConfig(format!(
            "{} is not a standalone node's directory; an export brings single node data into a cluster, and a cluster member's data is a Backup's",
            source.display()
        ))));
    }
    // the target: empty, or absent
    if target.exists() {
        let mut entries = std::fs::read_dir(target)?;
        if entries.next().is_some() {
            return Err(ServerError::Shoal(ShoalError::InvalidConfig(format!(
                "{} is not empty; an export is into a new directory",
                target.display()
            ))));
        }
    }
    // every persistent table under the default root, since one under its own is not exported
    let tables: Vec<String> = S::persistent_tables()
        .iter()
        .map(|table| (*table).to_string())
        .collect();
    for table in &tables {
        if conf.storage.tables.contains_key(table) {
            return Err(ServerError::Shoal(ShoalError::InvalidConfig(format!(
                "table {table} is under its own storage root, which an export does not read; put it under the default root first"
            ))));
        }
    }
    std::fs::create_dir_all(target)?;
    let executors = marker.physical.unwrap_or(marker.shards);
    // the executors' shard names, whose archives are what is exported
    let shard_names: Vec<String> = (0..executors)
        .map(|executor| {
            // an executor count is small; the shard name is a u16
            #[allow(clippy::cast_possible_truncation)]
            shard_name(executor as u16)
        })
        .collect();
    // the files' provenance: an identity no cluster has, the source as the origin, and the
    // file format that carries both, which every build past F48 reads
    let op = Uuid::new_v4();
    let export_cluster = ClusterId::mint();
    let provenance = SnapshotProvenance::at(export_cluster, marker.node, SNAPSHOT_V2_FROM_WIRE);
    let schema_id = <S::ClientType as QuerySupport>::SCHEMA_ID;
    let named: Vec<(String, S::TableNames)> = tables
        .iter()
        .filter_map(|table| S::table_of_id(TableId::of(table)).map(|named| (table.clone(), named)))
        .collect();
    let conf_owned = conf.clone();
    let target_owned = target.to_path_buf();
    // the fold and the export, on an executor of their own
    let executor = LocalExecutorBuilder::new(Placement::Unbound)
        .name("shoal-export")
        .make()?;
    let (rows_folded, exported, bytes_written) = executor.run(async move {
        // every executor's intent logs of every table, into the source's archives
        let mut folded = 0u64;
        for name in &shard_names {
            for (_, table) in &named {
                folded += S::fold_intents(name, *table, &conf_owned).await?;
            }
        }
        // then every table's archives as one file, with the manifest a restore reads beside it
        let mut exported = Vec::with_capacity(named.len());
        let mut bytes = 0u64;
        for (table_name, table) in &named {
            let table_id = TableId::of(table_name);
            // the group named is the table's alone: a restore keys nothing by it
            let group = GroupId::of(table_id, &[]);
            let dir = target_owned.join(table_name);
            let file = dir.join(super::replication::snapshot::snapshot_name(group, 0));
            let manifest = S::export_archives(
                &shard_names,
                *table,
                &conf_owned,
                &file,
                &provenance,
                group,
                schema_id,
            )
            .await?;
            let beside = BackupManifest::of(op, table_name, &manifest);
            std::fs::write(
                super::shard::backup::manifest_path(&file),
                serde_json::to_vec_pretty(&beside)?,
            )?;
            let written = std::fs::File::open(super::shard::backup::manifest_path(&file))?;
            written.sync_all()?;
            bytes += manifest.total;
            exported.push((table_name.clone(), manifest.records));
        }
        let dir = std::fs::File::open(&target_owned)?;
        dir.sync_all()?;
        Ok::<_, ServerError>((folded, exported, bytes))
    })?;
    let report = ExportReport {
        op,
        source_node: marker.node,
        export_cluster,
        rollback: source.clone(),
        target: target.to_path_buf(),
        tables: exported,
        executors,
        rows_folded,
        bytes_written,
    };
    event!(
        Level::INFO,
        msg = "exported a standalone directory as a backup",
        source = %source.display(),
        target = %target.display(),
        source_node = %report.source_node,
        export_cluster = %report.export_cluster,
        tables = ?report.tables,
        executors,
        rows_folded,
        bytes_written,
    );
    Ok(report)
}
