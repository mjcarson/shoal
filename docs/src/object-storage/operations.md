# S14. Operating buckets and pools

## Context

A storage pool is run by somebody. Disks are added, fail and are replaced; a pool fills; a scrub
finds something a rule will not mend; a cluster is upgraded, backed up and, one day,
restored. This page is the operator's surface: what can be asked, what is reported, what is
deliberately not offered, and the one place where object storage collides with a decision
the book has already recorded.

## What exists today

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
  ([F63](../features/shoaladm.md)). An inventory names two directories a node.
- **A backup is one verified file a group**, cut at that group's committed boundary and
  restored only into a fresh, empty cluster that then refuses the old identities
  ([F49](../features/backup-and-recovery.md)).
- **A schema change is not a rolling operation.** A node with another schema id is refused
  at the hello. The supported path is a new cluster and a restore, decided as
  [Q10 at M10a](../distributed/protocol.md#q10-at-m10a) and listed on
  [C15](../distributed/open-issues.md#explicitly-unsupported).
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

A device is **added** by a node's configuration and a restart, with its number of slices,
since a device is a node's fact ([S4](pools-and-devices.md#a-device-has-slices)); nothing is
moved onto it until a plan says so, as nothing moves onto a joined node today.

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

### What is reported

| Of | Figures |
| --- | --- |
| A bucket | Objects and bytes; operations by kind with their bytes in and out and their latency, counted at the front door as queries are |
| A device | Size, free bytes, checksum failures, what its scrub has read, and the state of each of its slices |
| A slice | Its state, bytes staged, chunks held |
| A pool | Its consumers, stale chunks, the rate rebuilds are closing them at, moves in flight |
| A stream | Nothing on the cluster's own figures: a stream's throughput is the benchmark's to record |

The stats view and the cluster tab gain a pools page, on the model the tables page has.

### Day two

Six procedures, each to become a runbook when the thing it operates exists:

| Procedure | In one line |
| --- | --- |
| Replace a failed disk | Fit the new disk at the old path. It comes up as a new device with new slices; `RemoveDevice` the old id; the plan fills the new one |
| Add a disk | Name it, with its number of slices, in the node's configuration and restart the node. Ask for a rebalance |
| Retire a disk | `DrainDevice`, wait for the plan, then remove it |
| A node is down past its grace | Its devices and their slices are removed with it, and their chunks rebuilt, by the plan a member's expiry already makes |
| A pool is nearly full | Readiness says so before writes are refused. Add devices, or delete |
| A scrub stopped with evidence | Read what it found. `ResolveStripe` names what to trust, or declares the stripe lost |

### Activation

The object lane and the client's object messages are gated by capability bits, and the lane
by an activated wire version, the way every addition since
[F48](../features/rolling-compatibility.md) has been. A build that knows objects serving a
schema with no bucket behaves as it did.

### A schema change, a backup and a restore

This is where the design meets a recorded decision, and it is stated here and not designed
around.

**The collision.** A schema change is a new cluster and a restore
([Q10](../distributed/protocol.md#q10-at-m10a)). Adding a bucket is a schema change
([S2](buckets.md#adding-a-bucket-is-a-schema-change)), and so is adding a table to a schema
that has a bucket. A restore carries rows. The stripe chunks are on devices whose markers name
the old cluster, and a restored cluster "refuses the old identities" by design. So as things
stand, **adding one table to a cluster that holds objects would strand every object in it.**

**Two ways out**, and this part chooses neither:

| | A restore that carries or adopts stripe chunks | An additive schema change, made rolling |
| --- | --- | --- |
| What it is | The new cluster takes over the old cluster's devices: their markers are rewritten under a recorded adoption, and a scrub verifies what they hold against the restored rows | A node whose schema only adds tables or buckets to its peer's is admitted, and the new groups are created live |
| What it touches | [F49](../features/backup-and-recovery.md)'s restore and the device marker | The hello's schema comparison, and Q10 itself |
| What it leaves | A schema change is still an outage and a restore | Q10's rule for every change that is not an addition |

**A backup has the same shape of problem.** A backup of the metadata tables is taken group
by group at each group's boundary, and the stripe chunks go on changing in place. Restoring those
rows later gives rows that are older than the bytes, which is exactly the state
[P10](contract.md#the-contract) refuses to serve. So a backup of a cluster's tables is not a
backup of its buckets, and [P19](contract.md#the-contract) says so.

**And `force_recover` has it too.** Recovering a metadata group to one survivor can leave
its rows behind writes that were acknowledged and applied to stripe chunks. The chunks then
carry labels no surviving row names.

For all three the minimum is the same and is not optional: **the bucket refuses by name**
rather than read a row against a chunk that does not match it. What more is offered (an
adoption, a rolling addition, a backup that includes bytes, a way to accept the chunks'
labels on an operator's word) is [Q31](contract.md#questions-to-answer), and it blocks
[M21](milestones.md#m21-operations-and-the-real-cluster) because an object store that a
schema change destroys is not one to put data in.

### What is not offered at first

Changing a pool's redundancy or geometry; moving a bucket to another pool; moving a device
to another node; a backup of object bytes; listing; quotas; authorization for a bucket.
Each has a supported answer of "not yet", and the first three have a reason on
[S4](pools-and-devices.md).

## Alternatives rejected

**A tool that formats and mounts disks.** `shoaladm` checks what it is given: a path, its
filesystem, its free space. What is on a block device is the operator's.

**Adopting a marked device automatically.** A disk from another cluster holds another
cluster's data. Taking it without being asked is how one cluster's failure becomes two.

**Pools edited by rewriting a file and restarting.** Policy is committed; a file is where
it was first written ([S4](pools-and-devices.md#pools-and-bindings-are-policy)).

**Leaving the collision as a limitation.** A limitation is something a user can live with.
This one is found on the day a schema changes, by losing everything.

## What it costs

More admin kinds, more of a status report, a pools page in two tools, and six runbooks. And
the cost of saying no: until Q31 is answered, a cluster with buckets cannot change its
schema, and this page is where an operator is told.

## What it breaks

- "A backup of a cluster is a backup of its data"
  ([C9](../distributed/operations.md#backup-restore-and-export)): of its tables.
- "A restore refuses the old identities": an adoption, if that is the answer, is an
  exception to it that has to be recorded.
- "`force_recover` recovers a set to one survivor": the rows, and not what the rows describe.
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

## Prerequisites

[S4](pools-and-devices.md), [S10](recovery.md) and [S11](scrub.md), which this page puts
verbs on. [Q31](contract.md#questions-to-answer), answered, before the last gate.

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
| `rows_older_than_chunks_are_refused_by_name` | After a restore of the metadata tables alone, or a `force_recover`, a read of an affected stripe is refused and never served mixed | M21 |

## Related

[S4](pools-and-devices.md) for what a pool, a device and a slice are; [S10](recovery.md) and
[S11](scrub.md) for what the verbs start; [C9](../distributed/operations.md) for the admin
frame and the cluster's own verbs; [F49](../features/backup-and-recovery.md) for backup and
restore; the [runbooks](../operations/runbooks.md) for the form day two will take.
