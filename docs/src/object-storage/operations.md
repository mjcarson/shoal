# S14. Operating buckets and pools

## Context

A storage pool is run by somebody. Disks are added, fail and are replaced; a pool fills; a scrub
finds something a rule will not mend; a cluster is upgraded, backed up and, one day,
restored. This page is the operator's surface: what can be asked, what is reported, what is
deliberately not offered, and the one place where object storage collides with a decision
the book has already recorded.

## What exists today

**Since [F85](../features/devices-and-pools.md#operations), on 2026-10-12, the first of this
page's surface is built** (M14): the `devices` and `pool_status` admin reads, advertised in
`ADMIN_READS`; each pool's readiness in `Readiness`; a node's devices in its inventory, deployed,
drawn by `status` and wiped only when they hold Shoal's marker; and a `pools` cluster action in
`shoaladm` and shoalctl. ~~No verb on a device exists yet~~, and no write is refused for a pool that is
not ready before M15. Where the build differs from the design below, the design says so where the
detail is.

**Since [F88](../features/recovery-and-moves.md#operations), on 2026-10-14** (M16), `pools` shows
each pool's target generation, the moves admitted, the placement groups still behind their target,
the stale and overflowed positions the records name, what was rebuilt, backfilled, switched and
aborted, and the devices retired; and one verb on a device exists, `fail_device`
(`fail-device <device>` in the cluster tab and `shoaladm admin`), admins only. Since
[F89](../features/scrub-and-repair.md#operations) `scrub <pool> [light|deep]` asks a pool for a scrub
now, admins only, and a device past ten checksum failures within ten minutes is failed by its node
as that order would.

**Since [F92](../features/reclamation-and-device-lifecycle.md#a-devices-life), on 2026-10-16** (M20),
a device is drained (`drain-device`), removed and tombstoned (`remove-device [--lost]`) and
reweighted (`reweight-device`), each an operation with its id and version, admins only; a device past
three checksum failures in a day is drained on its own; a retired device stays retired; `devices`
leads with each device's id, and `pools` shows what reclamation removed and what is draining.

- **Admin operations ride the client connection** as JSON under a query id. A mutation is
  authorized against `cluster.admins`, carries the topology version it was written against
  and is refused if that is stale, is idempotent by its operation id, and is audited
  (`AdminRequest`, `shoal-proto/src/shared/protocol/admin.rs:44-54`;
  [C9](../distributed/operations.md#the-admin-frame)).
- **A kind a node cannot decode closes the connection**, so a new read is advertised before
  it is sent: `ADMIN_READS` names the reads a build answers
  (`shoal-core/src/server/control/plane.rs:388`, `admin.rs:240`).
- **Readiness names what is short.** A cluster with too few members up reports
  `default_writes` short by name and refuses the write; the factor never moves on its own
  ([C13](../distributed/protocol.md#failure-model-and-availability)).
- **Stats are counted where they happen**: what each table holds, what each group applied,
  and what a node's clients were answered by kind, in bytes and in latency
  (`QUERY_OPS`, `shoal-proto/src/shared/protocol/stats.rs:30`;
  [F52](../features/cluster-stats.md), [F65](../features/query-figures-home-tab.md)).
- **`shoaladm` deploys from an inventory** and `shoalctl` draws the cluster
  ([F63](../features/shoaladm.md)). ~~An inventory names two directories a node.~~ An inventory
  names two directories a node and, since F85, its devices, at the deployment, a group or the
  node, the most specific list winning whole.
- **A backup is one verified file a group**, cut at that group's committed boundary and
  restored only into a fresh, empty cluster that then refuses the old identities
  ([F49](../features/backup-and-recovery.md)).
- ~~**A schema change is not a rolling operation.** A node with another schema id is refused
  at the hello. The supported path is a new cluster and a restore, decided as
  [Q10 at M10a](../distributed/protocol.md#q10-at-m10a) and listed on
  [C15](../distributed/open-issues.md#explicitly-unsupported). As built, that restore refuses a
  backup of another schema id, so the path is refused too
  ([item 255](../appendix/resolved/restore-judged-by-table.md)).~~
  **An append of tables or buckets is rolling** since [F93](../features/operations-and-the-real-cluster.md):
  a node is judged by its schema's lineage at the hello, and the cluster's schema moves by
  `ExtendSchema`. Any other change is still a new cluster and a restore, which is judged table by
  table ([Resolved #255](../appendix/resolved/restore-judged-by-table.md)).
- **A lost majority is recovered by `force_recover`** on one stopped survivor, which
  rewrites its groups' membership to itself
  ([C9](../distributed/operations.md#permanent-quorum-loss)).

## The design

### Admin operations

They are admin kinds like the others: versioned, idempotent by id, authorized, audited, and
advertised before a client sends one.

| Kind | Does |
| --- | --- |
| `Pools`, `Devices` (reads) | The committed pools and their bindings, for every kind of consumer; every device with its node, class, state, size and free bytes, and each of its slices with its state |
| `BucketStats` (read) | Objects, bytes, and what clients were answered, for a bucket |
| `PoolStatus` (read) | For a pool: the consumers bound to it, failure domains available against needed, stale chunks, rebuilds and moves in flight, bytes staged and undecided, when each kind of scrub last finished |
| `DrainDevice` | Moves the placement groups on every slice of a device elsewhere while it goes on serving, as `Decommission` drains a member |
| `RemoveDevice` | Takes a device that is gone, and every slice on it, out of the map. Their chunks are rebuilt, and the device and its slices are tombstoned so that none of their ids is accepted again |
| `ReweightDevice` | Changes the share of a pool a device takes, which its slices divide equally |
| `Scrub` | A light or deep scrub now, of a pool, a consumer, a device or a slice |
| `ResolveStripe` | An operator's word on a stripe that stopped with its evidence ([S11](scrub.md#quarantine-and-repair)) |

As built at M14 ([F85](../features/devices-and-pools.md#reports-commands-and-reads)), two of these
are answered, as the admin reads `devices` and `pool_status`, beside F80's `pools`:
- **`devices`** is the pool map, its generation, and every member's devices as last reported, each
  with its slices.
- **`pool_status`** is every pool's readiness ([below](#readiness)). ~~It has no stale chunks,
  rebuilds, moves, staged and undecided bytes or scrubs yet, since none of them exists before M16
  and M17.~~ Since [F88](../features/recovery-and-moves.md#operations) it carries each pool's
  target, its moves admitted and its recovery figures: groups behind their target, stale and
  overflowed positions, what was rebuilt, backfilled, switched and aborted, and the devices
  retired, counted on the shards that lead each group and summed by the leader. Staged and
  undecided bytes~~, and scrubs,~~ are not in it~~; scrubs are M17's~~. Since
  [F89](../features/scrub-and-repair.md#operations) it carries each pool's scrub figures too - when
  the oldest light and deep pass finished, the groups pending, what was verified, and what was found
  stale, corrupt, unexplained, disagreeing and lost - and the operator's counts of scrub requests.
- **`scrub <pool> [light|deep]`** is `Scrub` as built by F89, for a pool alone: committed as a count
  of requests on the pool map, which the leader of every tablet group of a bucket the pool serves
  begins a pass for at once. ~~A scrub of a consumer, a device or a slice is not built.~~ Since
  [F93](../features/operations-and-the-real-cluster.md#the-operators-surface) it is narrowed to a
  bucket, a device or a slice, `scrub <pool> [light|deep] [bucket <name> | device <id> | slice <id>
  <index>]`: the request still counts on its pool, and a pass scrubs only the placement groups the
  target names.

Since [F87](../features/tablet-walk.md) the admin read `walk` pages the rows of one tablet of any
table a node holds a copy of, a bucket's two generated tables included, keys and figures a page;
on a cluster only `cluster.admins` may ask, since no table grant reaches a generated table. A
walk of a bucket's `StripeMeta` table names each stripe's consumer, object, index and sequence.

~~`BucketStats` and the verbs are not built: the device verbs are
[M20](milestones.md#m20-reclamation-and-device-lifecycle)'s, and the whole surface
[M21](milestones.md#m21-operations-and-the-real-cluster)'s.~~ ✅ Built: the device verbs by
[F92](../features/reclamation-and-device-lifecycle.md#a-devices-life), and the rest of the surface
by [F93](../features/operations-and-the-real-cluster.md#the-operators-surface) - `bucket_stats`,
each bucket's objects and bytes and its operations by kind; `ScrubNow` and `FailDevice` versioned
and remembered by id as every other mutation is; every mutation advertised in `admin_mutations`
beside `admin_reads`; every refusal audited; `ExtendSchema`; and `ResolveStripe`. Four control commands carry what a node
reports into the pool map, `ObserveDevices`, `ReportDevices`, `SetPoolWeights` and
`SetPoolExceptions`, and none of them is an admin kind: a node or the control leader proposes each,
and each is refused until the cluster activates wire version 8.

Since [F88](../features/recovery-and-moves.md#a-failed-device-a-down-node) one device verb is
built ahead of M20 (✅ the rest by [F92](../features/reclamation-and-device-lifecycle.md#a-devices-life):
`drain_device`, `remove_device` with `lost`, and `reweight_device`, each an operation with its id and
version; and since F92 a retired device stays retired): **`fail_device`**, an admin kind that commits `FailDevice`, an order on the
device's record its node carries out on its next tick as an I/O error would; its report of the
failure clears the order, and the device then retires as any failed device does. F88's control
commands, `TargetPool`, `RetireDevice`, `AdmitMove` and `MoveDone`, are proposed by the control
leader or a placement group's driver, none of them is an admin kind, and each is refused until the
cluster activates wire version 10.

Since [F89](../features/scrub-and-repair.md#operations) a second verb is built: **`scrub`**, an admin
kind that commits `ScrubNow`, a count of requests on the pool map for the pool named, light or deep,
which the leader of every tablet group of a bucket bound to the pool begins a pass for at once,
whatever its schedule. It is refused until the cluster activates wire version 11. A device past its
checksum failures' limit is failed by its node as `fail_device`'s order would, with no command of
its own: its report of the failure is what every other path acts on.

A device is **added** by a node's configuration and a restart, with its number of slices,
since a device is a node's fact ([S4](pools-and-devices.md#a-device-has-slices)); nothing is
moved onto it ~~until a plan says so~~, as nothing moves onto a joined node today. As built by
[F88](../features/recovery-and-moves.md#moves), no plan is asked for: a device that joins moves
the map's generation, the control leader moves each pinned pool's target to it once the pool is
ready there, and the placement groups whose holders change move onto the device, admitted sixteen
arriving on a device at once.

A device is added, drained, removed and taken down for maintenance whole: every verb above
that names a device acts on all of its slices together, since they fail together. A device's
number of slices is fixed while it holds a chunk, and changing it is draining the device and
adding it again.

### Readiness

A pool is ready for writes when its devices span the failure domains its redundancy needs
and its acknowledgement rule can be met. When it is not, readiness names the pool and what
it is short of, and a write by any of its consumers is refused with a code that says the
same thing. Nothing degrades in silence, and no redundancy shrinks to fit what is up.

Readiness also names each device and each slice that is not placeable, with its state. A
device down is every slice on it down.

As built at M14 ([F85](../features/devices-and-pools.md#readiness)), `Readiness` carries `pools`,
one `PoolReadiness` each, which reports:
- its devices of the class, how many are serving, and by id the ones failed and the ones on a member
  that is down. A slice is not named apart: it is down or failed with its device;
- the failure domains its devices span against the width it needs, and the fallback by name when a
  member stands in the pool's level by a level below it, for example "rack falls back to host on 3
  members";
- its free bytes above each serving device's reserve, and by id the devices at their reserve;
- the capacity a pool exactly as wide as its domains can use;
- `short`, each thing it lacks by name, for example "it places a stripe on 6 distinct hosts and its
  devices span 3". A pool with nothing short is ready.

Refusing a consumer's write to a pool that is not ready is M15's.

### What is reported

| Of | Figures |
| --- | --- |
| A bucket | Objects and bytes; operations by kind with their bytes in and out and their latency, counted at the front door as queries are |
| A device | Size, free bytes, checksum failures, what its scrub has read, and the state of each of its slices |
| A slice | Its state, bytes staged, chunks held |
| A pool | Its consumers, stale chunks, the rate rebuilds are closing them at, moves in flight |
| A stream | Nothing on the cluster's own figures: a stream's throughput is the benchmark's to record |

The stats view and the cluster tab gain a pools page, on the model the tables page has.

As built at M14 ([F85](../features/devices-and-pools.md#operations)), a node reports each device's
size, free bytes and reserve, its `major:minor`, filesystem and rotational flag, its warnings and its
health, and each slice's id, index, chunks held and bytes staged. ~~A device's checksum failures and
what its scrub has read wait for M17~~ Since [F89](../features/scrub-and-repair.md) each slice reports
its checksum failures, the chunks it holds set aside and the bytes its deep scrubs read, and `status`
shows a device's failures and chunks set aside once either is more than none; a slice has no state
apart from its device's. `status`
draws a line a device under the roots, and the `pools` cluster action, read-only like `grants`,
draws each pool's readiness, in `shoaladm` and shoalctl through one model. The stats view's pools
page is M21's (✅ built by [F93](../features/operations-and-the-real-cluster.md#the-tools): the
stats view's `pools` tab). Since [F88](../features/recovery-and-moves.md#operations) a pool's line in `pools`
is followed by its recovery: target, moves, behind, stale, overflowed, rebuilt, backfilled,
switched, aborted, and the devices retired. The rate at which rebuilds close stale chunks is not a
figure; `rebuilt` is a count.

### Day two

Six procedures, ~~each to become a runbook when the thing it operates exists~~ each a runbook since
[F93](../features/operations-and-the-real-cluster.md), runbooks 16 to 21 of
[the runbooks](../operations/runbooks.md#16-replace-a-failed-disk):

| Procedure | In one line |
| --- | --- |
| Replace a failed disk | Fit the new disk at the old path. It comes up as a new device with new slices; `RemoveDevice` the old id; the plan fills the new one |
| Add a disk | Name it, with its number of slices, in the node's configuration and restart the node. Ask for a rebalance |
| Retire a disk | `DrainDevice`, wait for the plan, then remove it |
| A node is down past its grace | Its devices and their slices are removed with it, and their chunks rebuilt, by the plan a member's expiry already makes |
| A pool is nearly full | Readiness says so before writes are refused. Add devices, or delete |
| A scrub stopped with evidence | Read what it found. `ResolveStripe` names what to trust, or declares the stripe lost. As built by [F89](../features/scrub-and-repair.md), what it found is logged by name and counted in `pools`; ~~`ResolveStripe` is M21's~~ `ResolveStripe` is built by [F93](../features/operations-and-the-real-cluster.md#a-stripe-ahead-of-its-row) for a stripe ahead of its row, and declares any stripe lost |

As built by [F88](../features/recovery-and-moves.md#a-failed-device-a-down-node), two of these
need less than they say. A failed disk on a node that is up is retired at once and its placement
groups move off it before the new one is fitted, and a disk added moves its pool's target, so its
groups move onto it with no rebalance asked for. ~~`RemoveDevice` and `DrainDevice` are still M20's.~~
✅ Built by [F92](../features/reclamation-and-device-lifecycle.md#a-devices-life): a drain copies a
device's groups off it while it serves, and a removal is admitted once nothing it holds is read, or on
`--lost`.

### Activation

The object lane and the client's object messages are gated by capability bits, and the lane
by an activated wire version, the way every addition since
[F48](../features/rolling-compatibility.md) has been. A build that knows objects serving a
schema with no bucket behaves as it did. Since [F93](../features/operations-and-the-real-cluster.md)
objects are activated on a running cluster by an append: its nodes are rolled onto a build whose
schema appends a bucket, its files binding the bucket to a pool, and `ExtendSchema`, which
`shoaladm upgrade --activate` sends, commits the pool and the binding once every member runs it.

### A schema change, a backup and a restore

This is where the design meets a recorded decision, and it is stated here and not designed
around.

**The collision.** A schema change is a new cluster and a restore
([Q10](../distributed/protocol.md#q10-at-m10a)). Adding a bucket is a schema change
([S2](buckets.md#adding-a-bucket-is-a-schema-change)), and so is adding a table to a schema
that has a bucket. A restore carries rows. The stripe chunks are on devices whose markers name
the old cluster, and a restored cluster "refuses the old identities" by design. So as things
stand, **adding one table to a cluster that holds objects would strand every object in it.**

**Two ways out**, and ~~this part chooses neither~~ [Q31](contract.md#q31-a-schema-change-a-backup-a-restore-and-force_recover-2026-10-17)
chose the second with the user on 2026-10-17:

| | A restore that carries or adopts stripe chunks | ✅ An additive schema change, made rolling |
| --- | --- | --- |
| What it is | The new cluster takes over the old cluster's devices: their markers are rewritten under a recorded adoption, and a scrub verifies what they hold against the restored rows | A node whose schema only adds tables or buckets to its peer's is admitted, and the new groups are created live |
| What it touches | [F49](../features/backup-and-recovery.md)'s restore and the device marker | The hello's schema comparison, and Q10 itself |
| What it leaves | A schema change is still an outage and a restore | Q10's rule for every change that is not an addition |

**As decided** ([the record](contract.md#q31-a-schema-change-a-backup-a-restore-and-force_recover-2026-10-17)),
and ~~built by [M21](milestones.md#m21-operations-and-the-real-cluster)~~ ✅ built at M21 by
[F93](../features/operations-and-the-real-cluster.md):

- **A schema change that only appends tables or buckets is rolling.** The derive emits the schema
  id of every prefix of a schema's fields, its *lineage*, and a peer, a joiner, a snapshot install
  and a client are admitted where one side's lineage holds the other's id. The cluster's schema is
  committed and moves only by `ExtendSchema`, refused until every member's build holds it, which
  starts the new tables' groups live and binds a new bucket to its pool. No member rolls back past
  it. Objects never leave their devices. Every other change, a table inserted between others or a
  row changed, is a new cluster and a restore, Q10's rule.
- **A restore is judged table by table**, by each table's identity and row fingerprint and each
  bucket's id and row layout, so a backup cut before an addition restores after it. ~~Today a restore
  refuses any other schema id, so Q10's own path is refused
  ([item 255](../appendix/resolved/restore-judged-by-table.md)).~~
  ([Resolved #255](../appendix/resolved/restore-judged-by-table.md))
- **A backup of a bucket is a backup of its objects, never of its rows**: an archive for each
  `ObjectMeta` group, every live object's entry and bytes read at one state, and nothing a cluster
  minted (no placement state, pool map, device, label or consumer id). A restore puts the tables
  back, then puts every object again into the pool the destination binds the bucket to.
- **A bucket's rows never travel as table files**, and a restore refuses a backup that holds them
  by name, so no restore makes a row older than its chunks.
- **After `force_recover` the survivor's rows are the state.** A stripe whose holders applied a
  label its row does not name is refused by name, on a read and a write, and its chunks are kept
  for `ResolveStripe`'s word; an object whose entry the survivor never held is reclaimed.

**A backup has the same shape of problem.** A backup of the metadata tables is taken group
by group at each group's boundary, and the stripe chunks go on changing in place. Restoring those
rows later gives rows that are older than the bytes, which is exactly the state
[P10](contract.md#the-contract) refuses to serve. So a backup of a cluster's tables is not a
backup of its buckets, and [P19](contract.md#the-contract) says so. As built, a backup carries a
bucket's two tables as rows and no byte, and on the lab
a restore of one brought every inline object back and stranded every striped one
([the record](contract.md#q31-a-schema-change-a-backup-a-restore-and-force_recover-2026-10-17)).
Since Q31 a bucket is backed up as its objects instead, each at one state; P19 still promises no
instant across them.

**And `force_recover` has it too.** Recovering a metadata group to one survivor can leave
its rows behind writes that were acknowledged and applied to stripe chunks. The chunks then
carry labels no surviving row names.

For all three the minimum is the same and is not optional: **the bucket refuses by name**
rather than read a row against a chunk that does not match it. What more is offered (an
adoption, a rolling addition, a backup that includes bytes, a way to accept the chunks'
labels on an operator's word) ~~is [Q31](contract.md#questions-to-answer), and it blocks
[M21](milestones.md#m21-operations-and-the-real-cluster) because an object store that a
schema change destroys is not one to put data in~~ was [Q31](contract.md#questions-to-answer)'s,
since an object store that a schema change destroys is not one to put data in. ✅ Decided on
2026-10-17: a rolling addition, a backup of objects, and `ResolveStripe`'s word on a stripe whose
holders are past its row are offered at [M21](milestones.md#m21-operations-and-the-real-cluster),
✅ built by [F93](../features/operations-and-the-real-cluster.md); an adoption is not.

### What is not offered at first

Changing a pool's redundancy or geometry; moving a bucket to another pool; moving a device
to another node; ~~a backup of object bytes~~; listing; quotas; ~~authorization for a bucket~~.
Each has a supported answer of "not yet", and the first three have a reason on
[S4](pools-and-devices.md). Authorization for a bucket is offered from the first: it was built
once for tables and buckets by [F77](../features/authorization.md), and a bucket's grants ~~arrive~~
arrived with the bucket at [M12](milestones.md#m12-tables-what-the-metadata-needs)
([F79](../features/bucket-schema.md)), changed by the same `grant` and `revoke` admin kinds,
`grant <who> bucket <name|*> <privileges>`. A backup of object bytes is offered at M21 (✅ built by
[F93](../features/operations-and-the-real-cluster.md#a-bucket-backed-up-as-its-objects)), as a
backup of each object at one state, since [Q31](contract.md#q31-a-schema-change-a-backup-a-restore-and-force_recover-2026-10-17) decided it with the user so that a backup of
tables and objects needs no other format later.

## Alternatives rejected

**A tool that formats and mounts disks.** `shoaladm` checks what it is given: a path, its
filesystem, its free space. What is on a block device is the operator's.

**Adopting a marked device automatically.** A disk from another cluster holds another
cluster's data. Taking it without being asked is how one cluster's failure becomes two.

**Pools edited by rewriting a file and restarting.** Policy is committed; a file is where
it was first written ([S4](pools-and-devices.md#pools-and-bindings-are-policy)).

**Leaving the collision as a limitation.** A limitation is something a user can live with.
This one is found on the day a schema changes, by losing everything.

**A restore that adopts the old cluster's devices**, the other way out of the collision, rejected
by [Q31](contract.md#q31-a-schema-change-a-backup-a-restore-and-force_recover-2026-10-17). It leaves every schema change an outage and a restore; it is an exception to "a
restore refuses the old identities", a device being held to its node and its cluster
(`devices/claim.rs`); and the restored placement state would name the old pool map's generations
and slices, so the pool map would have to travel in the backup too. A backup of objects re-put by
the destination answers a lost cluster without any of that.

**A backup that copies stripe chunks.** A destination places every stripe by its own pool map, so
a chunk copied whole would be moved again on arrival; and a chunk keeps changing in place after
its rows' boundary, which is the collision itself.

**Stripes ahead of their rows after `force_recover`, lost on their own.** The rule would remove
acknowledged bytes without an operator's word, which nothing else here does but a rebuild of a
chunk that failed its own checksum. They are refused by name and kept for `ResolveStripe`.

**The chunks' labels winning after `force_recover`.** Holders agreeing on a newer label could move
a stripe's row forward, but the object's entry, its size after an append among it, can be behind
too, and nothing on a holder says what it was.

## What it costs

More admin kinds, more of a status report, a pools page in two tools, and six runbooks. And
the cost of saying no: ~~until Q31 is answered~~ until M21 builds what [Q31](contract.md#q31-a-schema-change-a-backup-a-restore-and-force_recover-2026-10-17) decided, a
cluster with buckets cannot change its schema, and this page is where an operator is told. A
change that is not an addition stays a new cluster, its objects backed up and put again, which
costs what a copy of them costs.

## What it breaks

- "A backup of a cluster is a backup of its data"
  ([C9](../distributed/operations.md#backup-restore-and-export)): of its tables~~.~~, and since
  [Q31](contract.md#q31-a-schema-change-a-backup-a-restore-and-force_recover-2026-10-17) of its buckets' objects too, each at one state, from M21.
- ~~"A restore refuses the old identities": an adoption, if that is the answer, is an
  exception to it that has to be recorded.~~ Kept: Q31 offers no adoption.
- "`force_recover` recovers a set to one survivor": the rows, and not what the rows describe.
- "A schema change is not a rolling operation"
  ([Q10](../distributed/protocol.md#q10-at-m10a)): an append of tables or buckets is, from M21.
- "An inventory is hosts and two directories each."

## Invariants to uphold

- Every mutation is versioned, idempotent by its id, authorized and audited.
- A new admin kind is advertised before any client sends it.
- A device and its slices leave the map only when their chunks are current elsewhere, or
  when an operator has said by name that they are lost.
- A verb on a device acts on all of its slices together, and a device's number of slices
  changes only by draining it and adding it again.
- Nothing destructive is automatic, except rebuilding a stripe chunk that failed its own checksum.
- Readiness names what is short, and no redundancy shrinks on its own.
- A bucket whose rows do not match its stripe chunks refuses by name.
- A bucket's rows never travel as table files: a backup of a bucket is its objects, and nothing
  a cluster minted travels with them.
- An addition to a schema is an append, and no member rolls back past a committed schema.

## Prerequisites

[S4](pools-and-devices.md), [S10](recovery.md) and [S11](scrub.md), which this page puts
verbs on. ~~[Q31](contract.md#questions-to-answer), answered, before the last gate.~~ ✅
[Q31](contract.md#q31-a-schema-change-a-backup-a-restore-and-force_recover-2026-10-17), decided by design on 2026-10-17.

## How it would be measured

By doing it. Each procedure in [Day two](#day-two) is run on the lab against a pool with
data in it and its acknowledged writes checked afterwards, as the
[cluster testing](../cluster-testing/overview.md) chapter ran the runbooks of the cluster.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `pool_admin_is_authorized_versioned_and_idempotent` | A non-admin, a stale version and a repeat are each answered as the cluster's other mutations answer them | M21 |
| `drained_device_leaves_only_when_its_chunks_are_elsewhere` | A drain under load ends with every chunk of each of its slices current on another device and no acknowledged write lost | M20 |
| `removed_device_id_is_never_accepted_again` | A tombstoned device that returns is refused by name | M20 |
| `pool_readiness_names_what_is_short` | A pool short of failure domains, or of space, is named in readiness with what it is short of | M14 |
| `rows_older_than_chunks_are_refused_by_name` | ~~After a restore of the metadata tables alone, or a `force_recover`, a read of an affected stripe is refused and never served mixed~~ After a `force_recover` that lost a write its holders applied, a read and a write of the stripe are refused by name, never served mixed and never answered `Shedding`, and its chunks are kept; a restore of a backup holding a bucket's rows as table files is refused by name ([Q31](contract.md#q31-a-schema-change-a-backup-a-restore-and-force_recover-2026-10-17)) | M21 |
| `an_appended_table_rolls_through_a_cluster_holding_objects` | A build that appends a table and a bucket is rolled onto a cluster whose pool holds objects under load; nothing new is served before `ExtendSchema` commits, the new groups start after it, no object or row is lost, and a node of the old build is refused past it | M21 |
| `a_restore_judges_each_table_by_its_own_identity` | A backup cut before an addition restores into the schema after it, the added tables empty; one whose table's row changed is refused by name | M21 |
| `objects_survive_a_backup_and_restore_by_archive` | A bucket backed up under writes and restored into a new cluster of another pool map reads back every object as one state it held, with nothing of the source's placement in the destination | M21 |

## Related

[S4](pools-and-devices.md) for what a pool, a device and a slice are; [S10](recovery.md) and
[S11](scrub.md) for what the verbs start; [C9](../distributed/operations.md) for the admin
frame and the cluster's own verbs; [F49](../features/backup-and-recovery.md) for backup and
restore; the [runbooks](../operations/runbooks.md) for the form day two will take.
