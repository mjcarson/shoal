//! The manifest a rehome runs under
//!
//! A rehome is a list of steps over the files of a storage directory, and a crash can land
//! between any two of them. The manifest is what makes that survivable: it is planned whole
//! before the first file moves, written beside the marker, and rewritten - atomically, whole -
//! after every step is durable, so a restart finds exactly which steps are done and which one
//! it is in the middle of. Every step is idempotent when redone, which is the other half of
//! the contract ([F47](../../../../docs/src/features/local-rehome.md)).
//!
//! The plan is a function of the hosting before, the hosting after and the tables, so a
//! resumed rehome computes nothing: it reads the plan the crashed one wrote and continues it.
//! A manifest planned for one executor count is resumed only by that count; a start under a
//! third count is refused at the claim rather than planned over an unfinished move.

use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use uuid::Uuid;

use crate::server::hosting::Hosting;
use crate::server::ServerError;

/// The version of the manifest's own format
pub const MANIFEST_FORMAT: u32 = 1;

/// The name of the manifest within a storage directory
pub const MANIFEST_FILE: &str = "shoal-rehome.json";

/// The name the manifest is staged under before it is renamed into place
const MANIFEST_TEMP_FILE: &str = "shoal-rehome.json.tmp";

/// One kind of step a rehome takes
///
/// The order they are planned in is the order they are safe in: every `Fold` before any
/// `Archives`, so the source's data is all archives when it is read; every `Archives` and every
/// `Log` before any `Reclaim`, so nothing is deleted that has not been copied; `Finalize` last,
/// so the hosting on disk changes only once every file is where it says.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StepKind {
    /// Fold a source executor's intent logs of one table into its archives, on a standalone node
    Fold {
        /// The executor whose logs are folded
        source: u16,
        /// The table
        table: String,
    },
    /// Copy the archived records of one table that move from a source to a destination
    Archives {
        /// The executor the records are read from
        source: u16,
        /// The executor they are written to
        dest: u16,
        /// The table
        table: String,
    },
    /// Move the tablet groups of a source's WAL that now host on a destination, on a cluster node
    Log {
        /// The executor whose WAL is read
        source: u16,
        /// The executor whose WAL is appended to
        dest: u16,
    },
    /// Delete what a source no longer holds: a vanished executor's files, or a donor's moved entries
    Reclaim {
        /// The executor
        source: u16,
    },
    /// Write the new hosting and the marker, and delete the manifest
    Finalize,
}

/// One step of a rehome and whether it is done
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Step {
    /// What the step does
    #[serde(flatten)]
    pub kind: StepKind,
    /// Whether the step's effect is durable and the manifest has been told
    pub done: bool,
    /// The archive an `Archives` step writes into, recorded before its first record
    ///
    /// What a redo looks for: an archive the destination's map does not name is a partial
    /// one the crash left, and is removed before the copy is made again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archive: Option<Uuid>,
}

/// What a rehome moved and what it cost
///
/// Accumulated in the manifest as the steps run, so a resumed rehome's report counts what the
/// crashed one did; handed to the pool at the finalize and logged at INFO.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RehomeReport {
    /// The executor count the directory was laid out for
    pub from: usize,
    /// The executor count it is laid out for now
    pub to: usize,
    /// How many tablets changed executor, on a standalone node
    pub tablets_moved: usize,
    /// How many slots changed executor, on a cluster node
    pub slots_moved: usize,
    /// How many tablet groups' logs were moved
    pub groups: u64,
    /// How many archived records were copied
    pub records: u64,
    /// How many bytes of archived records were copied
    pub bytes: u64,
    /// How many partial snapshot installs were dropped for the leader to feed again
    pub installs_dropped: u64,
    /// How many partitions the folds wrote out of intent logs
    pub folded: u64,
    /// How long the rehome took, in milliseconds, across every start of it
    pub millis: u64,
    /// How many steps were begun again by a resumed rehome
    pub steps_redone: u64,
}

/// The manifest: the plan, the progress and the report of one rehome
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// The version of this file's own format
    pub format: u32,
    /// The executor count the directory was laid out for
    pub from: usize,
    /// The executor count it is being laid out for
    pub to: usize,
    /// Whether this is a cluster node, which moves slots and logs, or a standalone one
    pub cluster: bool,
    /// The hosting the files are under now
    pub before: Hosting,
    /// The hosting they will be under
    pub after: Hosting,
    /// The steps, in the order they run
    pub steps: Vec<Step>,
    /// When the rehome was planned, in milliseconds since the epoch
    pub started_ms: u64,
    /// What has been moved so far
    pub report: RehomeReport,
}

impl Manifest {
    /// Plan a rehome from one hosting to another
    ///
    /// # Arguments
    ///
    /// * `before` - The hosting the files are under
    /// * `after` - The hosting to move them to
    /// * `tables` - The persistent tables, whose archives move
    /// * `cluster` - Whether this is a cluster node
    #[must_use]
    pub fn plan(before: &Hosting, after: &Hosting, tables: &[String], cluster: bool) -> Self {
        // what moves: the items whose executor differs, and so the (source, dest) pairs
        let moves = before.moves_to(after, cluster);
        let mut pairs: Vec<(u16, u16)> = moves.iter().map(|(_, from, to)| (*from, *to)).collect();
        pairs.sort_unstable();
        pairs.dedup();
        let mut sources: Vec<u16> = pairs.iter().map(|(from, _)| *from).collect();
        sources.dedup();
        let mut steps = Vec::new();
        // every fold first, so a source's data is all archives when its records are read
        if !cluster {
            for source in &sources {
                for table in tables {
                    steps.push(Step {
                        kind: StepKind::Fold {
                            source: *source,
                            table: table.clone(),
                        },
                        done: false,
                        archive: None,
                    });
                }
            }
        }
        // then every copy of archives, per pair and table
        for (source, dest) in &pairs {
            for table in tables {
                steps.push(Step {
                    kind: StepKind::Archives {
                        source: *source,
                        dest: *dest,
                        table: table.clone(),
                    },
                    done: false,
                    archive: None,
                });
            }
        }
        // then every log move, per pair, on a cluster node
        if cluster {
            for (source, dest) in &pairs {
                steps.push(Step {
                    kind: StepKind::Log {
                        source: *source,
                        dest: *dest,
                    },
                    done: false,
                    archive: None,
                });
            }
        }
        // then reclaim every source, once everything it held is elsewhere
        for source in &sources {
            steps.push(Step {
                kind: StepKind::Reclaim { source: *source },
                done: false,
                archive: None,
            });
        }
        // and last the hosting itself
        steps.push(Step {
            kind: StepKind::Finalize,
            done: false,
            archive: None,
        });
        let report = RehomeReport {
            from: before.physical,
            to: after.physical,
            tablets_moved: if cluster { 0 } else { moves.len() },
            slots_moved: if cluster { moves.len() } else { 0 },
            ..RehomeReport::default()
        };
        Manifest {
            format: MANIFEST_FORMAT,
            from: before.physical,
            to: after.physical,
            cluster,
            before: before.clone(),
            after: after.clone(),
            steps,
            started_ms: now_ms(),
            report,
        }
    }

    /// Get the path to the manifest within a storage directory
    ///
    /// # Arguments
    ///
    /// * `root` - The root of the storage directory
    #[must_use]
    pub fn path(root: &Path) -> PathBuf {
        // beside the marker, where the claim looks for it
        root.join(MANIFEST_FILE)
    }

    /// Read the manifest a directory carries, if a rehome is in progress
    ///
    /// # Arguments
    ///
    /// * `root` - The root of the storage directory
    ///
    /// # Errors
    ///
    /// Fails if the file cannot be read or parsed, or names a format this build does not read.
    pub fn read(root: &Path) -> Result<Option<Self>, ServerError> {
        // read whatever manifest this directory carries
        let raw = match std::fs::read(Self::path(root)) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(ServerError::IO(error)),
        };
        let found: Manifest = serde_json::from_slice(&raw)?;
        // the format is checked before anything in the file is trusted
        if found.format != MANIFEST_FORMAT {
            return Err(ServerError::IO(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "the rehome manifest is format {} and this build reads format {MANIFEST_FORMAT}",
                    found.format
                ),
            )));
        }
        Ok(Some(found))
    }

    /// Write the manifest so that a crash leaves the old one or the new one
    ///
    /// # Arguments
    ///
    /// * `root` - The root of the storage directory
    ///
    /// # Errors
    ///
    /// Fails if the file cannot be written.
    pub fn write(&self, root: &Path) -> Result<(), ServerError> {
        // make sure the directory we are writing into exists
        std::fs::create_dir_all(root)?;
        // stage the new manifest beside the old one
        let staged = root.join(MANIFEST_TEMP_FILE);
        let mut file = File::create(&staged)?;
        file.write_all(&serde_json::to_vec_pretty(self)?)?;
        file.sync_all()?;
        drop(file);
        // and swap it in, which is the atomic step
        std::fs::rename(&staged, Self::path(root))?;
        // the rename is only durable once the directory entry is
        File::open(root)?.sync_all()?;
        Ok(())
    }

    /// Delete the manifest, durably: the rehome is finished
    ///
    /// # Arguments
    ///
    /// * `root` - The root of the storage directory
    ///
    /// # Errors
    ///
    /// Fails if the file cannot be removed.
    pub fn remove(root: &Path) -> Result<(), ServerError> {
        match std::fs::remove_file(Self::path(root)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(ServerError::IO(error)),
        }
        // the removal is only durable once the directory entry is
        File::open(root)?.sync_all()?;
        Ok(())
    }

    /// The index of the first step that is not done, if any
    #[must_use]
    pub fn next_step(&self) -> Option<usize> {
        // the steps run in order, so the first undone one is where a resume begins
        self.steps.iter().position(|step| !step.done)
    }

    /// Whether every step is done
    #[must_use]
    pub fn is_done(&self) -> bool {
        // nothing left to begin
        self.next_step().is_none()
    }

    /// The executors a source's items move to, lowest first
    ///
    /// # Arguments
    ///
    /// * `source` - The source
    #[must_use]
    pub fn dests_of(&self, source: u16) -> Vec<u16> {
        // every destination of an item leaving this source, once each
        let mut dests: Vec<u16> = self
            .before
            .moves_to(&self.after, self.cluster)
            .into_iter()
            .filter(|(_, from, _)| *from == source)
            .map(|(_, _, to)| to)
            .collect();
        dests.sort_unstable();
        dests.dedup();
        dests
    }

    /// Whether a source vanishes: it runs no executor under the hosting after
    ///
    /// # Arguments
    ///
    /// * `source` - The source
    #[must_use]
    pub fn vanishes(&self, source: u16) -> bool {
        // an executor numbered past the new count runs nothing afterwards
        usize::from(source) >= self.after.physical
    }
}

/// Now, in milliseconds since the epoch
#[must_use]
pub fn now_ms() -> u64 {
    // a clock before the epoch reads as zero rather than failing a plan
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The steps come in the order that is safe: folds, archives, logs, reclaims, finalize
    #[test]
    fn the_plan_orders_fold_archives_log_reclaim_finalize() {
        let tables = vec!["Note".to_string(), "Row".to_string()];
        // a standalone shrink from four to two: two sources, two tables
        let before = Hosting::identity(4);
        let after = before.plan(2, false).expect("a plan");
        let manifest = Manifest::plan(&before, &after, &tables, false);
        let kinds: Vec<&str> = manifest
            .steps
            .iter()
            .map(|step| match &step.kind {
                StepKind::Fold { .. } => "fold",
                StepKind::Archives { .. } => "archives",
                StepKind::Log { .. } => "log",
                StepKind::Reclaim { .. } => "reclaim",
                StepKind::Finalize => "finalize",
            })
            .collect();
        // every fold before any archives, every archives before any reclaim, finalize last
        let first = |kind: &str| kinds.iter().position(|k| *k == kind).expect(kind);
        let last = |kind: &str| kinds.iter().rposition(|k| *k == kind).expect(kind);
        assert!(last("fold") < first("archives"), "{kinds:?}");
        assert!(last("archives") < first("reclaim"), "{kinds:?}");
        assert_eq!(last("finalize"), kinds.len() - 1, "{kinds:?}");
        assert!(
            !kinds.contains(&"log"),
            "a standalone node has no logs to move: {kinds:?}"
        );
        assert_eq!(
            kinds.iter().filter(|k| **k == "fold").count(),
            4,
            "two sources, two tables"
        );
        assert_eq!(kinds.iter().filter(|k| **k == "reclaim").count(), 2);
        assert_eq!(manifest.report.tablets_moved, TABLET_COUNT / 2);
        assert_eq!(manifest.report.slots_moved, 0);
        assert!(manifest.vanishes(2) && manifest.vanishes(3) && !manifest.vanishes(1));
        assert_eq!(manifest.next_step(), Some(0));
        // a cluster shrink moves logs and folds nothing
        let before = Hosting::identity(4);
        let after = before.plan(2, true).expect("a plan");
        let manifest = Manifest::plan(&before, &after, &tables, true);
        let kinds: Vec<bool> = manifest
            .steps
            .iter()
            .map(|step| matches!(step.kind, StepKind::Fold { .. }))
            .collect();
        assert!(!kinds.contains(&true));
        let logs: Vec<(u16, u16)> = manifest
            .steps
            .iter()
            .filter_map(|step| match &step.kind {
                StepKind::Log { source, dest } => Some((*source, *dest)),
                _ => None,
            })
            .collect();
        assert_eq!(logs, vec![(2, 0), (3, 1)]);
        assert_eq!(manifest.report.slots_moved, 2);
        assert_eq!(manifest.dests_of(2), vec![0]);
        // the archives of every pair are between the logs and the folds
        let archives = manifest
            .steps
            .iter()
            .filter(|step| matches!(step.kind, StepKind::Archives { .. }))
            .count();
        assert_eq!(archives, 4, "two pairs, two tables");
        // a growth's donors are live and do not vanish
        let grown = Manifest::plan(&after, &before, &tables, true);
        assert!(!grown.vanishes(0) && !grown.vanishes(1));
        assert_eq!(grown.dests_of(0), vec![2]);
    }

    /// A manifest read back resumes at its first undone step, and a finished one is gone
    #[test]
    fn a_manifest_resumes_at_its_step() {
        let dir = tempfile::tempdir().expect("a temp dir");
        assert!(Manifest::read(dir.path()).expect("a read").is_none());
        let before = Hosting::identity(3);
        let after = before.plan(1, false).expect("a plan");
        let mut manifest = Manifest::plan(&before, &after, &["Note".to_string()], false);
        manifest.write(dir.path()).expect("a write");
        // the first two steps done, the third recorded with a partial archive
        manifest.steps[0].done = true;
        manifest.steps[1].done = true;
        let archive = Uuid::new_v4();
        manifest.steps[2].archive = Some(archive);
        manifest.report.records = 7;
        manifest.write(dir.path()).expect("a write");
        assert!(!dir.path().join(MANIFEST_TEMP_FILE).exists());
        // read back, it resumes at the third step with what it had recorded
        let found = Manifest::read(dir.path())
            .expect("a read")
            .expect("a manifest");
        assert_eq!(found, manifest);
        assert_eq!(found.next_step(), Some(2));
        assert_eq!(found.steps[2].archive, Some(archive));
        assert_eq!(found.report.records, 7);
        assert!(!found.is_done());
        // every step done is a finished rehome
        let mut done = found;
        for step in &mut done.steps {
            step.done = true;
        }
        assert!(done.is_done());
        Manifest::remove(dir.path()).expect("a removal");
        assert!(Manifest::read(dir.path()).expect("a read").is_none());
        // removing a manifest that is not there is nothing
        Manifest::remove(dir.path()).expect("a second removal");
        // a manifest from another format is refused
        let mut future = manifest;
        future.format = MANIFEST_FORMAT + 1;
        std::fs::write(
            Manifest::path(dir.path()),
            serde_json::to_vec(&future).expect("json"),
        )
        .expect("a write");
        assert!(Manifest::read(dir.path()).is_err());
    }

    use crate::server::ring::TABLET_COUNT;
}
