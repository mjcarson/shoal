# S4. Storage pools and devices

## Context

Object bytes go to named storage pools: an SSD pool beside an HDD pool, or two pools made of
chosen devices (R16, R17). By the decision of 2026-10-02 a pool and its redundancy are bound
in the deployment and not in the schema, which is Ceph's arrangement: a pool owns a
redundancy and a failure domain rule, and what stores data in it is pointed at it.

Shoal has nothing to build that on. Its configuration names two directories a node and
nothing that could be called a device. This page says what a storage pool and a device are,
where each is defined, and what a node has to report that it does not report today.

## What exists today

- **Storage is two directories.** `Storage` is a default pair of paths, latency sensitive and
  throughput sensitive, plus overrides keyed by a table's name
  (`shoal-core/src/server/conf.rs:675-681`). A table's override moves that table's files; it
  names no device and carries no redundancy.
- **Every root is claimed.** `Storage::roots` lists each distinct directory, and each is
  locked and carries the marker `shoal-meta.json` or a mirror of it (`conf.rs:743`,
  `shoal-core/src/server/meta.rs:119`). A directory with no marker is claimed, which is
  [item 46](../appendix/known-issues.md#46-an-unmarked-storage-directory-is-claimed-rather-than-refused).
- **A member has no devices and no domain.** `MemberRecord` holds a node's addresses, slots
  and weights (`shoal-core/src/server/control/types.rs:85`). Its placement weight is the
  node's `cluster.weight`, the executor count by default (`:122`).
- **Capacity is one number.** A node reports the free bytes of its default latency path
  (`shoal-core/src/server/control/capacity.rs:33-55`), and the leader keeps it in memory.
- **Policy is committed once.** The replication factor and the consistencies are copied from
  the bootstrapping node's configuration into `BootstrapPolicy`, and "a node that joins later
  adopts what is committed and its own copy of these fields is ignored"
  (`shoal-core/src/server/conf/cluster.rs:929-937`). There is one replication factor a
  cluster, and none for a table.
- **An inventory names directories.** A node's `storage` is `{latency, throughput}`, resolved
  from the node, then its group, then the deployment
  (`shoaladm/src/deploy/inventory.rs:119-126`), and rendered into the default pair alone
  ([F53](../features/inventory-wizard.md)).
- **Nothing knows what a disk is.** No configuration, report or planner input mentions a
  rotational device. The glommio fork asks whether a file's device is rotational when it opens
  one, to decide how to poll it (`glommio/src/io/dma_file.rs:231`), and that is all.

In Ceph, by contrast, a device carries a class, "By default, OSDs automatically set their
class at startup to `hdd`, `ssd`, or `nvme` in accordance with the type of device they are
backed by", and a rule can be held to one class (`doc/rados/operations/crush-map.rst` at
`v20.2.0`). An erasure code profile belongs to a pool and "cannot be modified after the pool
is created" (`doc/rados/operations/erasure-code.rst`).

## The design

### Pools and bindings are policy

A **storage pool** is a name and five facts:

| Fact | Meaning |
| --- | --- |
| Class | Which devices it is made of: every device carrying this class |
| Redundancy | `replicas: r`, or `erasure: {data: k, parity: m}` |
| Failure domain | `device` or `host`: no two pieces of a stripe share one ([S5](placement.md#failure-domains)) |
| `f` | How many more losses an acknowledged write survives. At least one ([P11](contract.md#the-contract)) |
| Geometry and inline threshold | Stripe size, stripe unit, and the size at or under which an object stays in its row ([S3](objects.md#small-objects-stay-inline)) |

A **binding** points a bucket at one pool. Pools and bindings are cluster policy: they are
read from the bootstrapping node's configuration, committed by the control group, and from
then on a joining node's copy is ignored, exactly as the replication factor is today. A
configuration file is where an operator writes them and not where a node reads them back.

```yaml
storage:
  pools:
    bulk:
      class: hdd
      redundancy: {erasure: {data: 4, parity: 2}}
      failure_domain: host
    fast:
      class: ssd
      redundancy: {replicas: 3}
      failure_domain: host
      inline: 16KiB
  buckets:
    Posters: {pool: bulk}
  devices:                      # this node's own, and the one part a node reads for itself
    - {path: /mnt/hdd0/shoal, class: hdd}
    - {path: /mnt/hdd1/shoal, class: hdd}
    - {path: /mnt/nvme0/shoal-objects, class: ssd}
```

**A class is a label, not a detection.** `hdd` and `ssd` are the conventional two, and an
operator may write any other. That is how R17's second example is met: devices one to four
are given the class `ssd-a` and five to eight `ssd-b`, and each pool selects its own. A
device whose kernel says it is rotational and whose class says `ssd` is started with a
warning that names it, and is not overruled.

**A pool's redundancy and geometry do not change.** Changing them is rewriting every stripe,
which is a migration between two pools. The page says so rather than leaving a setting that
looks editable.

**A bucket with no binding is refused by name** when the cluster is bootstrapped, and so is a
binding to a pool nobody defined. There is no default pool: a bucket silently placed on
whatever devices exist is the kind of default this book has regretted before
([Configuration](../getting-started/configuration.md#design-notes)).

### A device has an identity

A **device** is one directory on one filesystem on one node. It has:

- **an id**, minted the first time the directory is claimed and kept in a marker inside it,
  beside the node and cluster that claimed it, its class and its format;
- **a lock**, held while the node runs, as a storage root's is;
- **one owning executor**, which does all of its I/O ([S13](isolation.md#who-owns-a-device)).
  A peer names the device and never the executor.

The claim has three outcomes and no fourth. A marked directory is checked against this node
and cluster and refused by name if it belongs to another. An **empty** directory is a new
device with a new id. Anything else, a directory with files and no marker, is refused. The
second case is the one that matters: a failed disk replaced by a new one mounted at the same
path is an empty directory, and it must come up as a device that holds nothing, whose pieces
are then rebuilt, never as the old device with its pieces mysteriously gone
([S1](prerequisites.md#required), item 46).

Devices are node facts. A node reports its devices when it joins and in every status report,
the control group commits each device into the pool map, and a device's state there mirrors
a member's: placeable, leaving while it drains, down through a grace, removed and
tombstoned ([S5](placement.md#the-pool-map), [S10](recovery.md)).

A device does not move between nodes at first. Its marker names its node, and a disk carried
to another host is refused there by name.

### Capacity and weight

A node reports the size and the free bytes of **each device**, where it reports one figure
today. The leader keeps them in memory beside the bytes each group holds, uncommitted, since
they change every second ([C8](../distributed/rebalancing.md#plans)).

A device's weight is its size, unless an operator sets another. Placement spreads pieces by
weight ([S5](placement.md)), and a device that is nearly full is skipped by the same reserve
check a snapshot stream meets today, made for each device.

Several pools may select the same class, and then they share its devices and its space. A
pool's fullness is its devices' fullness.

### The standalone node

A node with no `cluster:` block serves buckets too. Its pools are made of its own devices,
and the only failure domain it can offer is `device`: a `replicas: 2` pool over two disks, or
4+2 over six. A pool with one device and `replicas: 1` is what an example and a test use.
Nothing about the write path changes; the tablet group that orders a stripe is absent, and
the shard's own intent log orders it, as it orders a standalone table's writes.

### What a node checks before it serves a pool

- **The filesystem.** A device on tmpfs is refused: glommio turns direct I/O off there in
  silence, which would hide exactly the behaviour the device store depends on
  ([todos](../appendix/todos.md#storage-engine-abstraction)). Which other filesystems are
  accepted, warned about or refused is [Q22](contract.md#questions-to-answer)'s.
- **The redundancy.** A pool that asks for more failure domains than its devices span cannot
  place a stripe. Readiness says so by name and writes to its buckets are refused, as
  `default_writes` is reported short today; the redundancy never shrinks on its own.

### Inventories

`shoaladm`'s inventory gains `devices` at the three levels `storage` already has (the
deployment, a group of nodes, a node), and `pools` and `buckets` at the deployment's. The
wizard, which already reports each root's free space, probes each host's block devices for
size, filesystem and whether the kernel calls them rotational, and offers a class.

## Alternatives rejected

**A raw block device with its own allocator**, as BlueStore is. It removes the filesystem
from the write path and adds an allocator, a metadata store and a recovery procedure for
both. Files on a filesystem come first, and [X6](spikes.md#x6-the-device-store-on-ssd) says
what that costs.

**A table's storage override as a pool.** `storage.tables` moves one table's files on every
node. It names no set of devices, spreads nothing and has no redundancy of its own.

**Pools read from every node's configuration.** Two nodes whose files disagree would place
the same stripe differently. Policy is committed once and read from the map.

**A class detected and trusted.** Ceph sets it automatically. Here the two examples R17
gives cannot both be met by detection, since four SSDs and four other SSDs detect alike.

**A default pool.** See above.

**The redundancy on the bucket**, with pools as devices only. Offered on 2026-10-02 and not
taken.

## What it costs

A marker and a lock in every device directory. A status report that grows by a line a
device. A pool map the control group commits and every shard holds ([S5](placement.md)).
More configuration, and more ways to write it wrongly, which is why the unknown keys of a
`storage.pools` block are refused and not ignored.

## What it breaks

- "Storage is a latency path and a throughput path": a node also has devices.
- "A node reports its free bytes": it reports several.
- "The inventory renders two directories a node" ([F53](../features/inventory-wizard.md)).
- "A cluster has one replication factor": it still has one, for tables and for the metadata
  rows. A pool's redundancy is its own.

## Invariants to uphold

- A device id is minted once, kept in the device's marker, and never reused.
- An empty directory is a new device. A non-empty directory with no marker is refused.
- Pool definitions and bindings are committed policy; a joining node's copy is ignored.
- A pool's redundancy, failure domain and geometry never change after it holds a stripe.
- A class is what an operator wrote. Detection warns and never overrides.
- A peer names a device, never an executor.
- No pool, device or binding reaches the schema fingerprint.

## Prerequisites

[S1](prerequisites.md#required): known issue 46, a failure domain on a member, and free
bytes reported for every root. The inventory's part waits on nothing but the shape above.

## How it would be measured

Nothing on this page has a speed. What it adds to a status report and to the pushed map is
sized by [X2](spikes.md#x2-placement-simulation), which extends the fanout tables
`shoal-spike fanout` already prints.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `empty_directory_is_a_new_device` | A device directory emptied and restarted comes up under a new id, holding nothing, and the old id is never served | M14 |
| `unmarked_directory_with_files_is_refused` | A non-empty directory with no marker stops the node by name | M14 |
| `pool_policy_is_committed_and_a_joiners_copy_is_ignored` | A joiner whose file defines another redundancy adopts the committed one | M13 |
| `unbound_bucket_is_refused_by_name` | A bootstrap with a bucket and no binding, or a binding to an undefined pool, is refused | M13 |
| `pool_short_of_failure_domains_refuses_writes_by_name` | A 4+2 pool over five hosts acknowledges nothing, and refuses each write by the name readiness gives it | M15 |
| `mislabelled_device_is_warned_about_and_not_overruled` | A device the kernel calls rotational under a class that says otherwise starts, keeps its class, and is named in a warning | M14 |
| `every_device_reports_its_free_bytes` | The leader's view holds a figure for each device of each member | M14 |

## Related

[S5](placement.md) for how a pool's devices are chosen for a stripe; [S6](device-store.md)
for what is in a device's directory; [S13](isolation.md) for which executor owns it;
[S14](operations.md) for adding and removing one;
[Configuration](../getting-started/configuration.md#storage) and
[F53](../features/inventory-wizard.md) for what storage is today; [S17](prior-art.md#ceph)
for the model.
