# F49. Backup, restore, export and permanent quorum loss

## Context

[C9](../distributed/operations.md) set three runbooks this page delivers: backup and restore
(runbook 9), existing single-node data (runbook 10) and the disaster-recovery half of a lost
control quorum (runbook 8), with [Q12](../distributed/protocol.md#q12-at-m8)'s second half -
what operator recovery is possible after a majority is permanently lost - waiting on them.
[F44](repair.md) had built the pieces a restore needs and closed the corruption half of Q12: a
snapshot file cut by a group's compactor at a committed boundary, an install path that
restarts a group from its held checkpoint with a received file pending, a scrub that judges
every member's digest, and a quarantine a copy sits in until one agrees.
[F48](rolling-compatibility.md) had given the snapshot file a version 2 header that names the
cluster it was cut in, written only past the activation of wire version 5, so a backup file
could identify itself before its bytes were trusted. [F47](local-rehome.md) had made a fold of
a standalone node's intent logs into its archives a step that runs with no table built, so a
stopped directory's rows could be read whole. What was missing was the operations themselves:
a backup as a record every group's leader drives, a restore into a *new* cluster that refuses
the old identities by name, an export of single-node data in the shape a restore reads, and a
recovery that rewrites a stopped survivor's membership rather than any automatic choice.
[M10](../distributed/milestones.md#m10-operations-and-the-real-cluster) is that gate, and this
page is its middle third, M10b.

Decided on 2026-09-14, while building it: the single-node path is an **export restored into a
fresh cluster**, not an import into a node directory. The first cut was the latter - fold,
copy the archives and maps into an empty directory, mint a node and cluster into its marker,
start it as a cluster of one - and it served every row, but the joiners never got them:
[F39](membership.md) placed the tablets over the members with new group identities and rows
loaded before `Initialize` stay where the bootstrapper's rule put them, which the membership
page had said in its Limitations since M3. Seeding every new group from the archives its
tablets happened to be under would have been a second install path with the reversion
semantics of [#99](../appendix/resolved/durable-log-reversion.md) to get right, when the
restore driver already installs a file's records into every member of every group and
verifies them. So the export writes what a backup writes and the restore is the one path.

## What it does

### A backup is a record its groups' leaders drive

`Backup { table, path }` is an admin mutation ([F39](membership.md)'s family, `is_mutation`) a
principal the policy names may ask for, committed as `ControlCommand::Backup` and kept in
`ControlState.backups` (`shoal-core/src/server/control/backup.rs`, `KEPT_BACKUPS` 64). The
record names the table - or every table - the directory the files go under, who asked and
when, and every group the placement derives for the tables with a `GroupBackup` each:
`Pending`, `Queued { behind }` when a move of the set is under way, `Cutting`, `Writing`,
`Done`, with the driver, the boundary and an outcome. **It is not one cross-tablet snapshot**:
every group's file is cut at a committed boundary of its own, which the record names, and the
files together are a backup of the tables at those boundaries. A backup asked for before the
cluster has activated wire version 5 is refused by name: the version 2 file header is the part
of the artifact that identifies its cluster, and it is written only past the activation.

The record rides the pushed map (`TabletMap.backups`), and the leader of each group drives
that group (`shoal-core/src/server/shard/backup.rs`, `drive_backups` beside the repairs and
moves, `cluster.backup.concurrent` at a time): it commits `Cutting`, nudges the group's
checkpoint past what it has applied so there is a boundary to cut at, asks the shard loop for
the group's snapshot - the compactor's cut for a persistent group, exactly the file the group
would send a member behind the purge point - holds it so the sweep cannot delete it, commits
`Writing` with the boundary, copies the file to `<path>/<op>/<table>/<group>-<boundary>.snap`
with a `BackupManifest` beside it as `<file>.snap.json`, verifies the copy against that
manifest, and commits `Done { Written { file, bytes, checksum, records, retries } }`. An
ephemeral table's group is `Skipped` by name: its rows do not survive a restart to begin with,
so a file of them would be one nothing could restore into a group that had lost them. A driver
that loses the lead leaves the group `Pending` for the next leader; one that fails past
`cluster.backup.timeout` commits `Failed { reason }`. `BackupStatus { op }` reads the record and
`Backups` every one kept.

The manifest beside a file is JSON a person can read and a restore judges before a byte of the
file is trusted: the operation, the cluster and the node the cut was made on, the table by id
and by name, the schema id, the group, the boundary and its term, the tablets, the records,
the total bytes, the checksum, the retries in the trailer, the newest identity the group had
forgotten, and when it was cut. `BackupManifest::to_snapshot` rebuilds the manifest
`snapshot::verify` checks a file against, so a file that does not match the manifest beside it
is refused wherever it is offered. Nothing of the membership is in it: a backup file's records
are what matter, and the set they were cut from is another cluster's business.

### A restore is into a new cluster, and the old identities are refused

`Restore { path }` is asked of a **fresh** cluster - bootstrapped, joined and initialized at
whatever factor and size the operator wants, with nothing written. The control plane scans the
directory for every `.snap.json` (`scan_backup`), refuses a schema id that is not this
cluster's before proposing anything, and the state machine refuses the rest by name
(`apply_restore`): no placement to restore into, a cluster that already restored (a restore is
once), a backup cut in this very cluster (a backup is restored into a new one, never the one it
was cut in), and a set of files whose tablets leave a gap or overlap for any table the files
name (`judge_coverage`). The record (`RestoreRecord`, `KEPT_RESTORES` 16) names the source
cluster, the files, and every group of every table the files cover with a `GroupRestore` each;
`ControlState.restored_from` is set when it is applied, and every group is driven by its leader
in three phases, each committed before the next so a driver that dies leaves a phase the next
leader resumes from (`shoal-core/src/server/shard/restore.rs`):

- **Loading**: a scrub proves every member's copy holds nothing. A restore is into an empty
  table, and a populated one is refused by name rather than overwritten; there is no `force`.
- **Installing**: a nudge entry gives the group a committed boundary of its own; every backup
  file covering the group's tablets is verified against its manifest and read, the records of
  the group's tablets and every remembered request kept, and one snapshot file written at that
  boundary under this cluster's provenance. Every member's copy is quarantined under the
  operation and the file installed on each through [F44](repair.md)'s install path - a peer
  over the bulk lane, this shard through its own loop - which restarts each group from its
  checkpoint with the file pending, the same atomic install the crash matrix proves. Resumed,
  the phase is redone whole at a fresh boundary, which every install is judged against.
- **Verifying**: a scrub through the restarted group; every member reporting one verified
  digest lifts the quarantines and the group is `Done { Restored { boundary, records, bytes,
  retries, verified } }`; anything else fails it by name.

What travels is what a group's snapshot always carried: a deleted row is absent from the file,
so the delete travels; the retry table in the trailer means a request whose identity was
acknowledged before the backup's boundary is answered its original result through the new
cluster, and the token it answers with is the new cluster's, minted from new group identities;
a session token minted on the old cluster is `WrongCluster`. The old cluster's nodes are refused
at every door: their markers name another cluster, which was always `WrongCluster`, and since
this page a node whose cluster is the one this cluster was restored from is refused as
*removed* (`Verdict::Removed` at both admission judges, `PeerRefusal::Removed`,
`ShoalError::RestoredFrom`) so a returned old node stops rather than dialling forever. A control
loop whose links are refused as removed stops with `ShoalError::Removed`
(`PeerNetwork::refused_as_removed`) - which is also what a member lost to a recovery does when
it comes back, below.

### Permanent quorum loss is recovered by an operator, never by the cluster

A cluster whose control voters are permanently gone does not recover by itself, and the
cluster says so at every door: a write through a survivor is unknown or refused for want of a
leader, never acknowledged alone; a strong read is refused; an admin mutation is refused naming
the voters, which of them this node reaches, and `force_recover` (`quorum_hint` on
`ProposeError::NoLeader`). A survivor restarted with `bootstrap: true` keeps its cluster and
still has no leader: [F39](membership.md)'s rule that a second bootstrap never mints a second
cluster is what makes "no empty bootstrap" true. Its data groups keep serving wherever their own
quorums survive ([C7](../distributed/failover.md)); at a factor of three with two of three
members gone, none do.

What exists is `force_recover(conf, &[me])` (`shoal-core/src/server/recover.rs`): an operator
stops a survivor and runs it on the directory, under its lock. **One survivor, on purpose**: a
recovery keeps the node it runs on and nothing else, since two survivors each recovered to
themselves would be two clusters and one recovered to both would need the other's log to agree
at a term neither leads; the choice of which log is the history is the operator's, made by
naming one node. It refuses by name a directory that is not a cluster member's, a survivor list
that is not this node alone, a node that is not a committed member, and a cluster whose only
member is already this node. Then, on an executor of its own:

- the control store first (`store::force_recover`): every entry the log holds past what the
  machine applied is applied - the recovery commits the whole log, so a joiner replaying it
  applies those entries too, and a survivor that skipped them would carry a state no replay of
  its own log reproduces, which is exactly the divergence the fixture found; then a membership
  entry naming this node alone and a `ForceRecovered { survivors, lost, at, last_committed,
  recovered_ms }` after it, both at a term past every term the log and the vote have seen and
  led by this node, the vote granted at that term, both marked committed and applied;
- then every tablet group on every executor whose members include a lost node: a membership
  entry naming this shard alone at a term past the group's, the vote at that term, the commit,
  so the group's leader is this shard the moment it starts.

Applying `ForceRecovered` leaves every lost member `Removing`, down and tombstoned - its
identity never returns, and a clone of its directory is still it - with a `Remove` plan each
whose identity every replica derives the same way, and appends a `RecoveryRecord` to
`ControlState.recoveries` (`Recoveries` reads them). The survivor starts as a cluster of one
that leads, commits and serves: writes are admitted against `TabletMap::voting_rf`, the active
factor less the copies on members the cluster has given up on, so a set of three with two lost
needs one. Every step is idempotent by inspection, so a run interrupted anywhere is run again
whole: a control log whose last entry is already this recovery is left alone, and a group whose
membership is already the survivor alone is left alone.

Fresh identities then join, and the recovery's plans rebuild every set on them the way
[F46](capacity-rebalancing.md)'s removals do, with three things this page had to make true for
a set whose members are mostly gone: a fresh group built after a recovery - a volatile table's
after the survivor restarts - is initialized from the members the cluster has not tombstoned
(`GroupSpec.voters`), or it would wait on a vote that never comes; a move adds its destination
and nothing else, so a member a recovery rewrote out of a group's committed configuration is not
brought back by a record that still lists it (the driver narrows the target to what the
configuration names); and two plans draining two members of one set take turns, one planned
per pass and a pending step held while the set is already moving under the other, since two
planned against the same sets in one pass pick the same destination for the same tablet. A
tombstoned member is not pinged. A member lost to a recovery that starts again from its
directory is refused as removed at every door and stops.

### Single-node data is exported, and restored like a backup

`export_standalone::<S>(conf, target)` (`shoal-core/src/server/export.rs`) is run with the
standalone node's own configuration, on its stopped directory, under its lock: it folds every
intent log of every executor into the archives - what the node's own next start would do -
and writes every persistent table's archives as **one snapshot file** with a backup manifest
beside it, under `<target>/<table>/`, through a new step of the storage engine
(`export_archives`: every executor's map opened as it is on disk, every live entry read back
verified, the records written in key order under a version 2 header, exactly as a compactor
cuts a group's snapshot). The manifest names every tablet, the standalone node as the origin,
and a cluster identity minted for the export, since a backup names the cluster it was cut in
and a restore refuses one cut in the cluster restoring it. The report says what was folded and
written and names the source as the rollback. It refuses by name a configuration with a
`cluster:` block, a source that is not a standalone node's, a source another process holds
(`StorageDirectoryLocked`), a target that is not empty, and a persistent table under its own
storage root. Nothing ephemeral is exported: those rows were the source's memory.

A fresh cluster of any size restores the export with `Restore { path }` exactly as it restores a
backup - every group's leader building a file for its tablets from the export's one, installing
it on every member and verifying it - and the `DIGEST` of the table on every node equals the
source's, which is what the export is judged by. The source is untouched but for the fold and
starts standalone afterwards with every row. The digest itself changed for this: it is a **sum
of per-row hashes** now rather than a chain in key order per shard, so a table dealt over two
executors and the same rows on one, or on three nodes at a factor of three, digest the same.

### What the operator sees

Three admin verbs and their reads (`Backup`, `BackupStatus`, `Backups`, `Restore`,
`RestoreStatus`, `Recoveries`), a `cluster.backup` block (`concurrent` 1, `timeout` 10m, no
shorter than `replication.snapshot_timeout`), two offline functions (`force_recover`,
`export_standalone`) with reports, and in `Members` the tombstones, the recoveries and the
under-replicated sets. The fixture answers `BACKUP <dir>`, `BACKUP_STATUS`, `BACKUPS`,
`RESTORE <dir>`, `RESTORE_STATUS` and `RECOVERIES`, and runs the recovery and the export in
process.

## Design choices

- **A backup is per group at each group's boundary, said so.** C9 permitted the initial backup
  not to be one cross-tablet transactional snapshot and asked that it be said; the record says
  it per group. A cross-tablet cut would need a barrier across every group at once, which
  nothing else needs and which a restore does not want either: it installs per group.
- **The backup file is the snapshot file.** Nothing new is written to disk: a backup is the
  file the group would send a member behind the purge point, with its manifest beside it, and a
  restore installs it through the path the crash matrix proves. The one addition is the version
  2 header from F48, which is why a backup waits on the activation.
- **A restore is into an empty new cluster, once.** Restoring over data would need a merge
  rule; restoring into the cluster the backup was cut in would need a story for the identities
  that cut it. Both are refused by name, and `restored_from` is a committed fact rather than a
  file so every member judges the old identities the same way.
- **Recovery is offline, to one survivor, by hand.** The alternatives were all automatic in
  some measure; the page below says why each was rejected. An offline rewrite under the lock is
  the one shape where the operator's choice is the whole input and the cluster's state before
  the recovery is untouched until the entry lands.
- **The lost members are removed by plans, not by the recovery.** A recovery could have
  dropped them from every set's configuration; then every set would be under-replicated with no
  record of what to rebuild. Leaving them `Removing` with a `Remove` plan each is what
  [F46](capacity-rebalancing.md) already does for an expired member, and the plans are what
  rebuild the copies once there is somebody to rebuild on.
- **The export writes a backup.** One shape a restore reads, one verified path, any target
  cluster size. What it cost is that the table's every tablet is one file rather than one per
  group, which the restore already filters by tablet.
- **The digest is a sum.** A chain in key order was per shard and per order; the export needed
  a digest of the *set* of rows. A sum of per-row `gxhash64` values is that, and every equal
  replica still digests equal.

## Alternatives rejected

- **A cross-tablet transactional snapshot.** Needs a barrier over every group at once, a mark
  in every log and a cut at every boundary held until the last; the restore would install per
  group anyway. C9 permitted per-group boundaries; the record names them.
- **Restoring into the same cluster, or over data.** Both need a rule for what wins - the
  file, or what the group has applied since - and either rule loses something silently. A
  restore is into an empty table of a new cluster and refuses the rest by name.
- **Automatic recovery from a permanent majority loss** - an empty rebootstrap, a survivor
  electing itself past the quorum, or a clone of a lost node's directory counted as it. Each
  is a fork or a rollback the cluster chose for the operator; C9 said no automatic empty
  bootstrap or destructive choice, and the fixture proves the survivor stays unavailable until
  told.
- **Recovery to several survivors.** Two survivors recovered to themselves would be two
  clusters; one recovered to name both needs the other's log to agree at a term neither leads,
  which is the general Raft membership problem with no quorum to solve it. Naming one node is
  the operator's choice made explicit, and fresh identities rebuild the rest.
- **A recovery that applies nothing and lets the start replay.** The survivor's applied
  state is what its start reads; a log the recovery committed but the machine had not applied
  would be applied by every joiner and skipped by the survivor - the fixture caught exactly
  this, with two members' maps a version apart. The recovery applies the tail itself.
- **An import into a node directory** - the first cut, above. It served every row on the
  node of one and none on the joiners, because `Initialize` places tablets under new group
  identities and nothing carries rows into a fresh group but a snapshot install. Seeding those
  installs from the archives would have been a second install path; the restore is the first.
- **A backup at wire version 4.** The version 1 file header names no cluster; the manifest
  beside it would, but then a file separated from its manifest is anybody's. Refusing until
  the activation costs one admin operation on a cluster that has just upgraded.

## Limitations

- **A backup's files land on each group's leader's own disk**, under the path given; there
  is no shipping, no encryption and no retention. Storing them outside the failure domain is
  the operator's step, as C9 says, and a backup's age is not tracked.
- **A restore is once, into an empty new cluster.** No point-in-time restore, no merge, no
  restore of one table into a populated cluster. The refusals name each case.
- **A recovery keeps one survivor's data and nothing else.** A set whose every member was
  lost is gone; a set the survivor held is exactly as the survivor last applied it, which is
  the data-loss boundary the record names (`last_committed`). With more nodes than the factor,
  sets the survivor was not a member of are lost, and their `Remove` plans stay blocked until
  an operator restores them from a backup into a new cluster.
- **A recovery is of the control group and the durable tablet groups.** A volatile group is
  reinitialized from the surviving members when the node starts, which is what its rows'
  lifetime always was.
- **An export is of persistent tables under the default storage root.** A table under its own
  `storage.tables` root is refused by name; an ephemeral table's rows are not exported.
- **Neither backup nor restore is priced by a bench arm across a network.** The
  `macro/cluster/background/backup` arm prices a backup on one host; a restore is priced by
  nothing.

## Invariants to uphold

- **A backup file verifies against the manifest beside it, and a restore verifies before it
  reads.** `BackupManifest::to_snapshot` has to rebuild exactly what `snapshot::verify` judges;
  a field added to the file header goes into both.
- **A backup is refused below the activated wire version 5.** The version 2 header is what
  identifies the file's cluster; a backup written at version 1 would be a file that verifies
  under any cluster's manifest.
- **A restore is into an empty table, once, into a cluster that is not the source.** The
  `Loading` scrub is the emptiness proof; `restored_from` and the source check are the once.
  Any change that lets a group hold rows before a restore has to say what wins.
- **`restored_from` is refused at every door as removed.** Both admission judges
  (`control/listener.rs`, `map.rs`) return `Verdict::Removed` for it; a new door has to ask
  the same question.
- **`force_recover` applies the log's unapplied tail before it writes.** The survivor's
  applied state after a recovery has to equal a replay of its log; a joiner's does, and the map
  version is derived from that state on both.
- **A recovery is to this node alone.** The survivor list is checked against the marker, and
  the state machine refuses a survivor that is not a member or is also lost.
- **A fresh group after a recovery is initialized from the voters the cluster has not
  tombstoned**, and a move never adds a member the group's committed configuration does not
  name. Both are what let the recovery's `Remove` plans finish; a placement change that puts a
  tombstoned member into a fresh group's initial membership stalls every set it is in.
- **Two plans never place the same set in one pass.** `drive_plans` plans one plan's steps per
  pass and a pending step waits while its set moves under another plan; the planner's `busy`
  input carries every other open plan's live steps.
- **Every replica's digest of a table is a sum of per-row hashes.** The export is judged by it
  against a cluster of another shape; a digest that folds in the shard or the order breaks the
  migration test and says nothing a scrub does not already say.
- **The export's manifest names every tablet and a cluster no cluster has.** The restore's
  coverage judge reads the tablets, and its source check reads the cluster.

## Performance

`macro/cluster/background/backup` ([shoal-bench](bench-runner.md)): the repair arm's placement,
mixture and client with the wire version activated and a `Backup` of the reference table asked
for a third of the way through, its record polled each second. `cluster.backup` carries the
marks, how many groups wrote, were skipped or failed, the bytes and records the files hold, the
client's distribution before, during and after it, and a per second series; it is read beside
`background/repair` under the `cluster-background` family and `during` against `before` is the
number. Captured at smoke scale on the development host to prove the arm runs (nine groups
written, seven megabytes, the `during` median above `before`), and deleted: the number is the
benchmark host's, and no full capture has been taken. A backup's cost on the foreground is a
scrub's read of the archives plus a copy written, once per group, at the leader; a restore's
cost is not priced.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `backup_restore_verifies_history_in_new_cluster` | `shoal/tests/cluster_fixture.rs` | A backup is admitted below the activation, a group's file is missing or fails to verify, a volatile group is not skipped, a restore into a new cluster misses a key or resurrects a deleted one, a remembered identity is not answered its original result through the new cluster with the new cluster's token, an old cluster's token is not `WrongCluster`, a second restore is admitted, or an old node's directory started against the new cluster is not refused as removed |
| `permanent_quorum_loss_requires_explicit_recovery` | `shoal/tests/cluster_fixture.rs` | A survivor acknowledges a write alone, serves a strong read, or admits an admin mutation without naming the voters and `force_recover`; a restart with `bootstrap: true` mints a cluster or elects; a recovery with a survivor list that is not this node is admitted, or a second run writes again; the survivor does not lead alone, loses a key acknowledged before the loss, or fails to record the recovery and tombstone the lost members; a lost member's directory starts again; or two fresh joiners do not rebuild every set and serve every key with the lost members removed |
| `single_node_data_has_a_verified_cluster_migration_path` | `shoal/tests/cluster_fixture.rs` | An export of a running source is not refused as locked, one into a non-empty target is not refused or writes into it, an export folds nothing or writes anything of an ephemeral table, a fresh cluster of three does not restore and verify every group from the one file, a node's digest differs from the source's, a write through the restored cluster does not commit, the source does not start standalone with every row and its digest, or a cluster member's directory is not refused as a source |
| `a_recovery_rewrites_membership_and_a_sole_voter_leads` | `shoal-core/src/server/control/store.rs` | The recovery lands at the wrong index or term, an entry the log held unapplied is not applied, the lost members are not removing and tombstoned with a plan each, a second run writes again, or a group opened over the recovered store does not elect the sole voter and commit |
| `a_restore_refuses_gaps_overlaps_and_a_foreign_table` | `shoal-core/src/server/control/backup.rs` | A set of files with a tablet covered by no file or by two, or naming a table the cluster lacks, is accepted |
| `a_backup_file_identifies_its_cluster` | `shoal-core/src/server/replication/snapshot.rs` | A file cut past the activation does not name its cluster in its own header, the manifest is not stamped with the cluster and the origin, the backup manifest beside it does not rebuild one the file verifies against, or a manifest for another cluster verifies it |
| `the_backup_block_parses_with_its_defaults` | `shoal-core/src/server/conf/cluster.rs` | The block's defaults move or a timeout shorter than the snapshot timeout is accepted |
| `backup_capture_records_files_and_windows` | `shoal-bench/src/workloads/harness/background.rs` | A backup's record loses its marks, counts, bytes or records, its windows and series are not cut as a repair's are, or an older capture without the block fails to load |
| `the_backup_arm_shares_the_kill_arms_placement` | `shoal-bench/src/workloads/cluster_backup.rs` | The arm is not appended after the rehome arm, is not read beside the repair arm, differs from the kill arm's placement, or names a table that is not persistent |
| `node_identity_persists_and_wrong_cluster_is_refused` | `shoal/tests/cluster_fixture.rs` | The standalone-directory refusal stops naming `export_standalone` |
| `automatic_removal_and_rejoin_preserve_fencing` and the other M9b rows | `shoal/tests/cluster_fixture.rs` | The `Verdict::Removed` judge at the admission doors or the tombstone ping skip regresses a removal or a rejoin |

## Related

[F44](repair.md) the install path and the scrub a restore verifies by; [F48](rolling-compatibility.md)
the version 2 file header and the activation a backup waits on; [F47](local-rehome.md) the
fold an export runs first; [F46](capacity-rebalancing.md) the removal plans a recovery leaves;
[F39](membership.md) why loaded data does not follow `Initialize`; [C9](../distributed/operations.md)
the runbooks; [Q12 at M10b](../distributed/protocol.md#q12-at-m10b) the decision record;
[M10b](../distributed/milestones.md#m10b-backup-restore-export-and-permanent-quorum-loss) the
gate.
