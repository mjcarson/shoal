//! `macro/cluster/background/backup` - what a backup costs the foreground while it runs
//!
//! # The question
//!
//! [M10](../../../docs/src/distributed/milestones.md#m10-operations-and-the-real-cluster)
//! delivers a backup as an operation every group's leader runs beside the foreground
//! ([F49](../../../docs/src/features/backup-and-recovery.md)), and
//! [Q12](../../../docs/src/distributed/protocol.md) wants its cost priced before a scheduled
//! backup gets a default, the way the repair arm priced a scrub. This arm is that capture: the
//! durable replication placement and the reference mixture, driven for the kill arm's time by
//! the kill arm's client, with the wire version the file header needs activated and a `Backup`
//! of the reference table asked for a third of the way through, its record polled each second
//! until every group is done. Every persistent group's leader nudges its checkpoint past what
//! it applied, cuts a snapshot at that boundary on its compactor and copies the file under the
//! workload's own storage root with a manifest beside it, while the mixture goes on; every
//! volatile group is skipped by name.
//!
//! # What is recorded
//!
//! `cluster.backup`: when the backup was asked for and when it was done, how many groups it
//! covered and how many wrote, were skipped or failed, the bytes and records the files hold,
//! the client's distribution before, during and after it, and a per second series. The
//! interference is `during` read against `before`; the resource is `bytes` over `seconds`.
//!
//! # What a smoke run shows
//!
//! The shape and little else: at smoke scale the table is a hundredth of its size, so the cut
//! is a few records per group and `bytes` is small. The full run is what prices a backup that
//! reads the archives whole.

use anyhow::Result;

use crate::model::macro_layer::Timing;
use crate::workloads::cluster_background::REPAIR_TABLE;
use crate::workloads::cluster_failover::Failover;
use crate::workloads::harness::seed::Scale;
use crate::workloads::workload::{BackgroundKind, BackgroundSpec, BoxFuture, Context, Measurement, Workload, WorkloadPlan};

/// The arm's identifier
pub const BACKUP_ID: &str = "macro/cluster/background/backup";

/// The fraction of the run the backup is asked for at: the repair arm's
pub const BACKUP_AT: (u32, u32) = (1, 3);

/// The table the backup cuts: the reference mixture's, which is the repair arm's
pub const BACKUP_TABLE: &str = REPAIR_TABLE;

/// The backup arm: the kill arm's placement and mixture with a backup instead of a kill
pub struct Backup {
    /// The kill arm this drives as, under this arm's own identity, without its fault
    twin: Failover,
}

impl Backup {
    /// The arm
    #[must_use]
    pub fn new() -> Self {
        Backup { twin: Failover::new() }
    }
}

impl Default for Backup {
    /// The arm
    fn default() -> Self {
        Backup::new()
    }
}

/// Every arm, in the order they were declared
pub fn all() -> Vec<Backup> {
    vec![Backup::new()]
}

impl Workload for Backup {
    /// What this workload is called
    fn id(&self) -> &'static str {
        BACKUP_ID
    }

    /// What this workload measures
    fn summary(&self) -> &'static str {
        "the reference mixture on a durable majority of three, with a backup of the table asked for a \
         third of the way through and cut in the background"
    }

    /// How this workload's samples are taken
    fn timing(&self) -> Timing {
        self.twin.timing()
    }

    /// Whether the hotpath layer may run this workload
    fn profiles(&self) -> bool {
        false
    }

    /// What this workload needs before it can run: the kill arm's placement and scale
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn plan(&self, scale: Scale) -> WorkloadPlan {
        self.twin.plan(scale)
    }

    /// Writes the rows the reads will find, the way the kill arm does, untimed
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn seed<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<()>> {
        self.twin.seed(ctx)
    }

    /// Runs the mixture for the scheduled time, as the kill arm does
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn run<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<Measurement>> {
        self.twin.run(ctx)
    }

    /// The backup: a cut of the reference table a third of the way in
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn background(&self, scale: Scale) -> Option<BackgroundSpec> {
        let run_for = Failover::run_for(scale);
        Some(BackgroundSpec {
            at: run_for * BACKUP_AT.0 / BACKUP_AT.1,
            run_for,
            table: BACKUP_TABLE,
            kind: BackgroundKind::Backup,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{all, BACKUP_ID, BACKUP_TABLE};
    use crate::workloads::cluster_background::REPAIR_ID;
    use crate::workloads::cluster_failover::{self, KILL_ID};
    use crate::workloads::cluster_rehome::ID as SHRINK_ID;
    use crate::workloads::harness::seed::Scale;
    use crate::workloads::workload::{BackgroundKind, Workload};

    /// The arm is the kill arm on the durable placement with no fault and a backup, its id
    /// appended after the rehome arm, under the repair arm's family
    #[test]
    fn the_backup_arm_shares_the_kill_arms_placement() {
        let arms = all();
        assert_eq!(arms.len(), 1);
        assert_eq!(arms[0].id(), BACKUP_ID);
        let ids = crate::workload_ids::IDS;
        let kill = ids.iter().position(|id| *id == KILL_ID).expect("the kill arm is registered");
        let repair = ids.iter().position(|id| *id == REPAIR_ID).expect("the repair arm is registered");
        let rehome = ids.iter().position(|id| *id == SHRINK_ID).expect("the rehome arm is registered");
        let backup = ids.iter().position(|id| *id == BACKUP_ID).expect("the backup arm is registered");
        assert!(kill < repair && repair < rehome && rehome < backup, "the backup arm is not appended after the rehome arm");
        assert_eq!(
            crate::render::family::family_for(BACKUP_ID).map(|family| family.name),
            crate::render::family::family_for(REPAIR_ID).map(|family| family.name),
            "the backup arm is not read beside the repair arm"
        );
        let kill = cluster_failover::all().into_iter().next().expect("the kill arm exists");
        for scale in [Scale::Smoke, Scale::Full] {
            let mine = arms[0].plan(scale);
            let theirs = kill.plan(scale);
            assert_eq!(mine.scale, theirs.scale);
            assert_eq!(mine.warmup, theirs.warmup);
            let mine_overrides = mine.server.overrides().expect("a server");
            let theirs_overrides = theirs.server.overrides().expect("a server");
            assert_eq!(mine_overrides.shards, theirs_overrides.shards);
            let mine_cluster = mine_overrides.cluster.as_ref().expect("a placement");
            let theirs_cluster = theirs_overrides.cluster.as_ref().expect("a placement");
            assert_eq!(mine_cluster.replication_factor, theirs_cluster.replication_factor);
            assert_eq!(mine_cluster.peers, theirs_cluster.peers);
            // no fault, no catch-up, and a backup inside the run of the reference table
            assert_eq!(arms[0].fault(scale), None);
            assert!(!arms[0].catchup());
            let spec = arms[0].background(scale).expect("the arm asks for a backup");
            assert_eq!(spec.table, BACKUP_TABLE);
            assert_eq!(spec.kind, BackgroundKind::Backup);
            assert!(!spec.kind.is_plan());
            assert!(spec.at < spec.run_for);
            assert_eq!(spec.run_for, kill.fault(scale).expect("the kill arm's schedule").run_for);
        }
        // the table the backup names is one the schema spells, and a persistent one
        let persistent = <crate::workloads::schema::Bench as shoal::server::database::ShoalDatabase>::persistent_tables();
        assert!(persistent.contains(&BACKUP_TABLE), "{persistent:?}");
    }
}
