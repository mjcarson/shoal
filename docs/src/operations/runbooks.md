# Runbooks

The procedures [C9](../distributed/operations.md#the-runbooks) named, as an operator follows
them: the exact operation, the keys in `shoal.yml` it turns on, what to wait for, and where the
rollback point is. Every operation below is an admin request - sent through a client with a
principal `cluster.admins` names, or typed into the `shoalctl` cluster tab
([`space c`](shoalctl.md#the-cluster-tab)), which previews it and follows its record - except
the three that run on a stopped directory (`force_recover`, `export_standalone`, a rehome),
which are said so. What each operation refuses is on the feature page it links; what it costs
the foreground is on [Performance](../distributed/performance.md).

Read first: **a directory never changes mode or identity**, an operation is **idempotent by
its operation id** (retry it with the same id and it is answered as the first time), and a
mutation is written against a **topology version** and refused `StaleVersion` if the cluster
moved under it - read `Members` again and send it again.

## 1. Bootstrap

**Keys.** On the first node: `cluster.bootstrap: true`, `cluster.replication_factor`,
`cluster.control_voters` (1, 3 or 5), `cluster.admins`, `cluster.port`, `cluster.control_port`,
`cluster.advertise` when the interface is not the address peers reach; `cluster.tls` for
encrypted lanes ([14](#14-rotate-certificates-and-authorities)). On every other node:
`cluster.seeds` naming the first node's control address, `bootstrap: false`.

**Do.** Start the first node; it mints a cluster and a node id into its marker and leads a
control group of one. Start the others; each joins through a seed as a learner and is promoted
to voter up to `control_voters`. Wait until `Members` shows every node `up` and the voters you
asked for. Then `Initialize { nodes }` in the order you want the tablets dealt, once - typed as
`initialize <node> [<node>...]` on the cluster tab, or sent from code: it places
every tablet over those nodes at the factor. Wait for `Readiness.data.default_writes` to say
`Ok` before opening the cluster to clients - `Members = up` is not data readiness.

**Rollback.** Before `Initialize`, stop everything and delete the directories: nothing has been
placed. After it, the cluster exists; a mistaken order is fixed by moves, not by a second
`Initialize`, which is refused.

**Never.** Start a second node with `bootstrap: true` against an established cluster: it keeps
its own cluster and never joins yours. Load data before `Initialize` on a cluster of more than
one node: it stays where the bootstrapper's rule put it ([F39](../features/membership.md)).

## 2. Add a node

**Keys.** The new node's `cluster.seeds`, its `cluster.weight` if the machines differ.

**Do.** Start it; wait for `Members` to show it `up` and, if you asked for more voters than you
have, `voter`. Read its `free_bytes` and `weight`. Nothing is placed on it yet: ask for
`Rebalance` and follow `PlanStatus { op }` to `completed`; a `blocked` reason names the set and
the reserve or cap it waits on. A read of the plan's steps is the preview there is.

**Rollback.** A node nothing was placed on is stopped and its directory deleted. One a plan has
moved sets onto is [decommissioned](#4-decommission).

## 3. Replace a dead node

**Keys.** `cluster.auto_remove_after` (thirty minutes by default; `null` never removes on its
own).

**Do.** Start the replacement as a **new identity** (a fresh directory joining through the
seeds). Then either wait for the dead member's grace to expire into a removal plan, or ask for
`Remove { node, replacement }` now, which takes the replacement first and moves every set the
dead member held onto it. Follow the plan. At a factor of three on three machines the plan is
blocked naming the missing member until the replacement has joined; supply the capacity first
and do not wait for the removal.

**Rollback.** A member `Removing` is never brought back; the grace can be
[held](#5-automatic-removal-and-maintenance) before it expires, not after.

**Never.** Copy the dead node's directory onto the replacement: it is the same identity at a
lower or equal incarnation and is fenced, or it is a removed identity and is refused at every
door ([6](#6-a-removed-node-returns)).

## 4. Decommission

**Do.** `Decommission { node }`. The member is `leaving` and still serving; the plan moves
every set it holds to the members the planner picks, one at a time under `moves_per_node`;
follow `PlanStatus`. When every set has moved the member is tombstoned, taken out of the
control group, and its process stops with `ShoalError::Removed`. An impossible target - nowhere
for a set to go - is recorded `blocked` naming why, not refused; add capacity and the plan
resumes on its own.

**Rollback.** While the plan runs, a `Decommission` that fails puts the member back to
`member`; there is no cancel. Once tombstoned, the identity never returns.

## 5. Automatic removal and maintenance

**Keys.** `cluster.auto_remove_after`, `cluster.failure_detector`.

**Do.** A member the detector calls `down` opens a grace `Members` shows as
`grace_remaining_ms`. To hold it - a host being repaired - `Maintenance { node, suspend: true }`;
the count stops and the member is not removed. `Maintenance { node, suspend: false }` resumes
it from the committed count. An expired grace records an `Expiry` plan; a `blocked` one is what
to page on.

**Rollback.** A held grace is resumed; an expired one is a removal ([3](#3-replace-a-dead-node)).

## 6. A removed node returns

**Do.** Nothing. A tombstoned identity is refused at every door - the hello, observe, admit, its
own report, the join - and the process stops with `ShoalError::Removed`. The directory is
untouched: keep it as evidence, never start it into the cluster. What it held comes back only
through a [backup](#10-backup-and-restore) of a live cluster restored into a new one, or a
`Remove { replacement }` that already moved its sets.

**Never.** Delete the tombstone or the directory to "let it back in". A new identity on the
same host is a fresh directory.

## 7. Rolling upgrade

**Keys.** `cluster.transport.wire_version` to pin a node at the version it spoke before the
upgrade through the window.

**Do.** Read `Members.wire`: `activated`, and the lowest and highest version members speak.
Upgrade one failure domain at a time: stop the node, install the build, start it, wait for
`Readiness` and for its groups to catch up (`Replication.lag_max` back to zero), then the next.
Inside the window the old and new builds negotiate the older version on every link. When every
member reports the new version, `Activate { wire }`; it is refused naming any member below it.
After the activation no member starts on a build below it, which is the rollback point.

**Rollback.** Before the activation, reinstall the previous build on any node; after it, none.
A schema change is not a rolling operation: a node with another `schema_id` is refused, and
the path is a new cluster and a restore ([F48](../features/rolling-compatibility.md)).

## 8. Control quorum lost

**Do.** Established data groups keep serving where their own quorums survive; topology and
admin mutations are refused for want of a leader, and every refusal names the voters, which of
them this node reaches, and the way out. Restore the missing voters from their own directories
- start them again - and the group elects. Nothing rebootstraps: a survivor restarted with
`bootstrap: true` keeps its cluster and mints nothing.

**Rollback.** None needed; nothing was changed. If the voters are gone for good,
[9](#9-permanent-quorum-loss).

## 9. Permanent quorum loss

**Do.** Choose the one survivor whose log is the history; stop it. On its directory:

```rust
shoal::server::recover::force_recover(&conf, &[survivor_node_id])?
```

It refuses a survivor list that is not that node alone. It applies what the control log held
unapplied, rewrites the control membership and every durable tablet group to that node at a
new term, tombstones every lost member with a `Remove` plan each, and records the boundary
(`Recoveries`: `last_committed`). Start the node; it leads alone and serves every key it had
acknowledged. Join fresh identities; the plans rebuild every set on them. A lost member's
directory that comes back is refused as removed.

**Rollback.** None: the recovery is the choice of which log is the history. What the survivor
never held is gone, and a set it was not a member of stays blocked until restored from a
[backup](#10-backup-and-restore). Run again after an interruption, the recovery does nothing
it already did ([F49](../features/backup-and-recovery.md)).

## 10. Backup and restore

**Keys.** `cluster.backup.concurrent`, `cluster.backup.timeout`; wire version 5 activated
([7](#7-rolling-upgrade)).

**Backup.** `Backup { table, path }` - a table, or none for every table. Every group's leader
cuts the group's own snapshot at a committed boundary of its own and copies it to
`<path>/<op>/<table>/<group>-<boundary>.snap` on **its own disk** with a JSON manifest beside
it; follow `BackupStatus { op }` until every group is `Done`, and read each group's outcome:
`Written`, `Skipped` (every ephemeral table), or `Failed` with a reason. Copy the `<path>/<op>`
directory out of the failure domain yourself; nothing ships it. It is not one cross-tablet
snapshot; the record says each group's boundary.

**Restore.** Bootstrap, join and initialize a **fresh** cluster at the factor and size you want,
with nothing written ([1](#1-bootstrap)). `Restore { path }` with the `<path>/<op>` directory
reachable from every node's leader; it is refused naming a schema that is not this cluster's,
a gap or overlap in the files' tablets, a populated table, a cluster that already restored, or
the cluster the backup was cut in. Follow `RestoreStatus { op }` until every group is `Done`
and `verified`. Verify by `DIGEST` against the source if you have it, then point clients at the
new cluster: its session tokens are its own, and the old cluster's are refused `WrongCluster`.
A node of the old cluster started against the new is refused as removed.

**Rollback.** The backup is files; a restore that failed is a cluster to delete and bootstrap
again. The cluster the backup was cut in is untouched throughout.

## 11. Existing single-node data

**Do.** Stop the standalone node. With its own configuration:

```rust
shoal::server::export::export_standalone::<Schema>(&conf, &export_dir)?
```

It folds the intent logs into the archives - the one thing it writes to the source - and
writes each persistent table's archives as one backup-shaped file with a manifest under
`<export_dir>/<table>/`; it is refused while the node runs (the directory is locked), into a
directory that is not empty, and for a directory that is not a standalone node's. Nothing
ephemeral is exported. Then [restore](#10-backup-and-restore) `<export_dir>` into a fresh
cluster of any shape and verify by `DIGEST` against the source's.

**Rollback.** Start the standalone node again: it has every row. A standalone directory started
with a `cluster:` block is refused naming this path; nothing converts in place.

## 12. Change a node's cores

**Keys.** `resources.cores`; `cluster.slots` is claimed once and never changes.

**Do.** Stop the node, change `resources.cores`, start it. The start is held while the files of
the executors that no longer run are dealt onto the ones that do (`rehoming the storage
directory` in the log, `Hosting` afterwards). More cores than slots is refused: grow past the
slots with a replacement ([3](#3-replace-a-dead-node)).

**Rollback.** Change the count back and start again; a rehome interrupted anywhere is resumed
at its step by the next start at the same count and refused by name at any other
([F47](../features/local-rehome.md)).

## 13. Change a node's address

**Keys.** `cluster.advertise`, `cluster.port`, `cluster.control_port`; on the other nodes,
`cluster.dial` if the member is reached through a different address from each side.

**Do.** Stop the node, change the keys, start it from the same directory. It joins as the same
identity one start later; every member's record of it names the new address at the new
incarnation, the control leader dials it there and writes the address into the membership, and
the data lanes re-dial from the pushed map. Wait for `Members` to show the new address and
`Readiness` on the node. Under `cluster.tls` the leaf has to carry the new address in its
names as well as the node ([14](#14-rotate-certificates-and-authorities)).

**Rollback.** Change the keys back and start again: another restart, another incarnation. A
copy of the directory left running at the old address is the same identity at the same
incarnation from another address, refused as a duplicate, and stops
([F50](../features/cluster-operations.md)).

## 14. Rotate certificates and authorities

**Keys.** `cluster.tls.cert`, `cluster.tls.key`, `cluster.tls.ca` (a bundle is allowed),
`cluster.tls.bind_identity` (on by default).

**Provisioning.** A leaf is issued for a node id, so the id comes first: start the node once
plaintext or read the id `StorageMeta::claim` minted into its marker, issue a leaf whose names
include the address peers dial it at and the URI `shoal-node://<id>`, and start it under
`cluster.tls`. Under the binding a leaf naming another node is refused `IdentityMismatch` and
one naming none `Unauthorized`, on both ends of every lane. A deployment that shares one leaf
across every node sets `bind_identity: false` and accepts that a member can then speak as
another with it.

**Rotate a leaf.** Write the new certificate and key over the files, then `ReloadTls` on that
node (or `reload-tls` in the cluster tab). The report names the node the new leaf carries and
how many certificates the chain holds; every handshake after uses it and nothing established
is dropped. Material that does not parse is refused and changes nothing.

**Rotate the authority.** Write a bundle of the old and new authorities as `ca` on every node
and `ReloadTls` each (`authorities: 2`); reissue every node's leaf under the new authority and
reload each; write the new authority alone and reload each (`authorities: 1`). A node restarted
at any point joins, since every node trusts what every leaf chains to.

**Rollback.** The files: write the previous material back and reload. A leaf reloaded on the
wrong node is refused by every peer naming the certificate until it is put right, and the node
keeps its established connections meanwhile.
