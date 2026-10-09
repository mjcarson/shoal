# S4. Storage pools, devices and slices

## Context

Object bytes go to named storage pools: an SSD pool beside an HDD pool, or two pools made of
chosen devices (R16, R17). By the decision of 2026-10-02 a pool and its redundancy are bound
in the deployment and not in the schema, which is Ceph's arrangement: a pool owns a
redundancy and a failure domain rule, and what stores data in it is pointed at it.

A storage pool is not the bucket's alone. A file system and block volumes are expected later,
and one pool is to serve them beside buckets at the same time, ~~as one RADOS pool serves RGW,
CephFS and RBD~~ which goes further than Ceph. RGW, CephFS and RBD share a Ceph cluster, but each
in pools of its own: a pool is tagged with the one application that uses it, and a second is
refused without an override (`src/mon/OSDMonitor.cc:9484-9487` at `v20.2.0`,
[X14](ceph-and-s3-sources.md#9-how-osds-are-deployed-whom-a-pool-serves-and-crimson)). So a
pool here serves **consumers**, of which a bucket is the first kind, and that is this design's
choice, not one copied.

Shoal has nothing to build that on. Its configuration names two directories a node and
nothing that could be called a device. This page says what a storage pool, a device and a
slice are, where each is defined, and what a node has to report that it does not report
today.

## What exists today

- **Storage is two directories.** `Storage` is a default pair of paths, latency sensitive and
  throughput sensitive, plus overrides keyed by a table's name
  (`shoal-core/src/server/conf.rs:675-681`). A table's override moves that table's files; it
  names no device and carries no redundancy.
- **Every root is claimed.** `Storage::roots` lists each distinct directory, and each is
  locked and carries the marker `shoal-meta.json` or a mirror of it (`conf.rs:743`,
  `shoal-core/src/server/meta.rs:119`). ~~A directory with no marker is claimed, which is
  item 46.~~ Since [Resolved #46](../appendix/resolved/unmarked-directory-refused.md) the claim
  has the three outcomes this page's device claim needs: an empty root is claimed, a marked
  one is held to its marker, and files with no marker are refused by name. An empty root the
  primary lists as written to is refused as wiped; a device differs there, below.
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
`v20.2.0`). The code is narrower than the document. An OSD on a kernel block device reports
`hdd` or `ssd` from the device's rotational flag, so an NVMe drive is `ssd`, and only SPDK reports
`nvme`. A class set when the OSD was made wins and is never reset
(`src/os/ObjectStore.h:349`, `src/osd/OSD.cc:4968-4999`;
[X14](ceph-and-s3-sources.md#the-comparison)). An erasure code profile belongs to a pool and "cannot be modified after the pool
is created" (`doc/rados/operations/erasure-code.rst`).

## The design

### Pools and bindings are policy

A **storage pool**, `ShoalStoragePool` in code, is a name and five facts:

| Fact | Meaning |
| --- | --- |
| Class | Which devices it is made of: every device carrying this class |
| Redundancy | `replicas: r`, or `erasure: {data: k, parity: m}` |
| Failure domain | `device` or `host`: no two chunks of a stripe share one ([S5](placement.md#failure-domains)). Never `slice`: two slices of one device fail together |
| `f` | How many more losses an acknowledged write survives. At least one ([P11](contract.md#the-contract)) |
| Geometry and inline threshold | Stripe size, chunk unit, and the size at or under which an object stays in its row ([S3](objects.md#small-objects-stay-inline)) |

A **binding** points a consumer at one pool. A consumer is anything that stores stripes in a
pool: today a bucket, later a file system or a block volume. **A pool serves any number of
consumers at once, of mixed kinds**, and each is given a consumer id when its binding is
committed, minted once and never reused. That id is in every stripe's key and every chunk's
identity ([S5](placement.md), [S6](device-store.md)), so two consumers' stripes never share a
key, a placement group or a file, and nothing below the binding (placement, the device store,
scrub, recovery, reclamation) asks what kind of consumer it serves. Only reclamation asks the
consumer anything, and what it asks, whether an owner id is still named, is the one question
every kind has to answer ([S10](recovery.md#reclamation)).

Pools and bindings are cluster policy: they are
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
  buckets:                      # one kind of consumer; a file system's binding would sit beside
    Posters: {pool: bulk}
    Thumbnails: {pool: bulk}
  devices:                      # this node's own, and the one part a node reads for itself
    - {path: /mnt/hdd0/shoal, class: hdd, journal: /mnt/nvme0/shoal-journal}
    - {path: /mnt/hdd1/shoal, class: hdd, journal: /mnt/nvme0/shoal-journal}
    - {path: /mnt/nvme0/shoal-objects, class: ssd, slices: 2}
```

**A rotational device names its journal**, a directory on an SSD of the same node, since
[X7](device-store-hdd.md#recommendation): a stage on the disk itself took 42 to 251 ms beside
applies, where on the SSD it took a millisecond or less. Devices that name one journal device
share its failure, and placement counts them as one under a `device` failure domain. A
rotational device is also started only with its disk's write cache off, or with `write_cache:
on` written beside it, which is then named in a warning, and only on XFS.

**A pool of rotational devices survives a second loss while it rebuilds**, since
[X12](recovery-scrub-rates.md#8-the-arithmetic): at the pace its foreground tolerated, a disk took
22 MiB/s of rebuilt chunks, so a 16 TiB disk is more than eight days onto one destination and a day
only when its rebuild is spread over nine. Its redundancy is therefore at least two parity chunks
or three copies, and the inventory wizard proposes 4+2. A rotational pool of 2+1 or two copies is
refused unless its configuration accepts the exposure by name, which is then named in a warning, as
a disk's write cache is. An SSD pool rebuilds a device of a few TiB in hours and keeps the choice.

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

### A device has slices

A **device** is the physical thing bytes are stored on: today a disk with a filesystem,
mounted at a path on one node. It has:

- **an id**, minted the first time its path is claimed and kept in a marker at the path,
  beside the node and cluster that claimed it, its class and its format;
- **a class, a size and a weight** ([below](#capacity-and-weight));
- **a seat**: the key placement draws it and its slices by, minted with it and committed in
  the pool map. A device the operator names as another's replacement takes over that device's
  seat while keeping an identity of its own, so it is drawn for exactly the groups its
  predecessor held and nothing else moves ([S5](placement.md#seats-and-placement-weights),
  [X2](placement-simulation.md#a-replaced-device));
- **one or more slices**, `slices: n`, one by default.

A **slice**, `DeviceSlice` in code, is the part of a device one executor owns. On a
filesystem it is a directory under the device's path, `slice-0`, `slice-1` and on, and it has:

- **an id** of its own, minted when the slice is made and kept in a marker inside it, beside
  its device's id;
- **a lock**, held while the node runs, as a storage root's is;
- **one owning executor**, which does all of its I/O ([S13](isolation.md#who-owns-a-slice)).
  A peer names the slice and never the executor.

The two are split because they answer different questions. **A slice is a unit of work**:
what placement chooses, what holds a chunk, what one core drives. **A device is a unit of
failure**: when a disk dies, every slice on it dies with it. A device one core cannot keep
busy, a fast SSD, is given more slices so that more cores drive it; it is still one device,
and the failure domain `device` keeps every chunk of a stripe off all of its slices but one
([S5](placement.md#failure-domains)). Calling each directory a device, as this page first did,
would have let two chunks of one stripe sit on one disk under `failure_domain: device`.

The split also outlives the filesystem. If Shoal later manages a raw block device itself, as
BlueStore does, a slice becomes a range of the device instead of a directory, and nothing
above the device store has to learn the difference
([Alternatives rejected](#alternatives-rejected)).

The claim has three outcomes and no fourth, at the device's path and again at each slice's
directory. A marked one is checked against this node, cluster and device, and refused by name
if it belongs to another. An **empty** one is new and is given a new id. Anything else, files
and no marker, is refused. The second case is the one that matters: a failed disk replaced by
a new one mounted at the same path is an empty directory, and it must come up as a new device
whose slices hold nothing, whose chunks are then rebuilt, never as the old device with its
chunks mysteriously gone ([S1](prerequisites.md#required),
[Resolved #46](../appendix/resolved/unmarked-directory-refused.md)). A table's root differs on
exactly this point: an empty root the node's marker lists is refused rather than taken as new,
because a table's root has no rebuild of its own and a device's chunks do.

The number of slices is fixed once any slice of the device holds a chunk. Changing it is draining the
device and adding it again, as changing a pool's geometry is a migration; a slice's id is in
every chunk's path, and a live re-slicing is not worth its protocol at first.

Devices and slices are node facts. A node reports them when it joins and in every status
report, the control group commits each into the pool map, and the state of each mirrors a
member's: placeable, leaving while it drains, down through a grace, removed and tombstoned
([S5](placement.md#the-pool-map), [S10](recovery.md)). A device's state bounds its slices':
a device down is every slice on it down.

A device does not move between nodes at first. Its marker names its node, and a disk carried
to another host is refused there by name.

### Capacity and weight

A node reports the size and the free bytes of **each device**, where it reports one figure
today. Its slices share its filesystem, so the figure is the device's and not a slice's. The leader keeps them in memory beside the bytes each group holds, uncommitted, since
they change every second ([C8](../distributed/rebalancing.md#plans)).

A device's weight is its size, unless an operator sets another, and each of its slices
carries an equal share of it. Placement spreads chunks over slices by weight
([S5](placement.md)), so a device given four slices draws no more chunks than one given one;
and a device that is nearly full has all of its slices skipped by the same reserve check a
snapshot stream meets today, made for each device.

Beside that weight, its capacity, a device has a **placement weight**, which is what placement
draws by. It is the capacity unless the planner fits another, which it does only for a pool
whose devices differ in weight: drawing several devices of unequal weight includes the heavy
ones less than in proportion, and fitted weights correct that
([S5](placement.md#seats-and-placement-weights), [X2](placement-simulation.md#exceptions-and-fitted-weights)).

Several pools may select the same class, and then they share its devices and its space, as
the consumers of one pool share its slices. A pool's fullness is its devices' fullness.

### The standalone node

A node with no `cluster:` block serves buckets too. Its pools are made of its own devices,
and the only failure domain it can offer is `device`: a `replicas: 2` pool over two disks, or
4+2 over six. Two slices of one disk are not two disks, here as anywhere. A pool with one
device and `replicas: 1` is what an example and a test use.
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
- **The width against the domains.** A pool exactly as wide as its failure domains puts a chunk
  of every placement group in every domain, so it fills at the pace of its smallest domain
  whatever placement does. The lab as fitted, a pool three wide over europa's 1,192 GiB and
  466 GiB on each Zen1 host, fills the Zen1 hosts 52% faster than the mean
  ([X2](placement-simulation.md#the-lab)). Readiness names the capacity such a pool can use.

### Inventories

`shoaladm`'s inventory gains `devices`, each with its `slices`, at the three levels `storage`
already has (the deployment, a group of nodes, a node), and `pools` and the consumers'
bindings at the deployment's. The
wizard, which already reports each root's free space, probes each host's block devices for
size, filesystem and whether the kernel calls them rotational, and offers a class, and a
number of slices ~~once [X6](spikes.md#x6-the-device-store-on-ssd) has said what one core
drives~~: **one for an SSD**, which [X6](device-store-ssd.md#8-one-device-several-slices) found
one core drives at its ceiling for 64 KiB units and whole chunks, on the Optane and on the 970
EVO. A pool with 4 KiB units on a fast device would want two, and the wizard says so when it is
offered one. It warns on ext4 for a pool of small objects and refuses btrfs
([Q22, in part](contract.md#q22-in-part-the-device-store-on-ssd-2026-10-04)). For a rotational
device it offers one slice, an SSD of the host to journal on, XFS alone, and turning the disk's
write cache off, kept off across a power cycle
([Q23](contract.md#q23-what-a-rotational-device-needs-2026-10-06)).

## Alternatives rejected

**A raw block device with its own allocator**, as BlueStore is. It removes the filesystem
from the write path and adds an allocator, a metadata store and a recovery procedure for
both. Files on a filesystem come first, and [X6](spikes.md#x6-the-device-store-on-ssd) says
what that costs: on the lab's SSDs, a whole chunk from a pool of files written ahead runs within
30% of a slot in one shared file above 1 MiB, and a small write in place costs the two flushes
any store would pay ([X6](device-store-ssd.md)). It is not precluded: a device is the disk
whatever stores on it, and only a slice's form changes, from a directory to a range of the
device.

**A directory a device**, each owned by one executor. It is what this page first said, and it
names the unit of work and the unit of failure with one word: a disk two cores drive becomes
two devices, and the failure domain `device` stops protecting anything on it. The slice is
the unit of work and the device the unit of failure.

**A slice a core across every device**, so that each executor owns a part of every disk. It
spreads a node's I/O evenly by construction and makes every disk's queue a shared one, which
is what one owner a file exists to avoid. An operator gives slices where a device needs them.

**A pool a consumer**, a pool per bucket. It is the arrangement buckets alone would get away
with, and it is the one a file system and a block volume beside buckets would break: every
consumer would need its own devices, or devices would be shared between pools with nothing to
say which pool's chunks are whose.

**A table's storage override as a pool.** `storage.tables` moves one table's files on every
node. It names no set of devices, spreads nothing and has no redundancy of its own.

**Pools read from every node's configuration.** Two nodes whose files disagree would place
the same stripe differently. Policy is committed once and read from the map.

**A class detected and trusted.** Ceph sets it automatically. Here the two examples R17
gives cannot both be met by detection, since four SSDs and four other SSDs detect alike.

**A default pool.** See above.

**The redundancy on the bucket**, with pools as devices only. Offered on 2026-10-02 and not
taken, and the consumers a pool will serve are a second reason: a file system bound to the
same pool as a bucket gets the same redundancy without restating it.

## What it costs

A marker at every device and a marker and a lock in every slice. A status report that grows
by a line a device and a line a slice. A consumer id in every stripe's key and every chunk's
header. A pool map the control group commits and every shard holds ([S5](placement.md)).
More configuration, and more ways to write it wrongly, which is why the unknown keys of a
`storage.pools` block are refused and not ignored.

## What it breaks

- "Storage is a latency path and a throughput path": a node also has devices.
- "A node reports its free bytes": it reports one a device.
- "The inventory renders two directories a node" ([F53](../features/inventory-wizard.md)).
- "A cluster has one replication factor": it still has one, for tables and for the metadata
  rows. A pool's redundancy is its own.

## Invariants to uphold

- A device id and a slice id are each minted once, kept in a marker, and never reused. A
  device's seat is not its id: it passes to a device named as its replacement.
- An empty directory is a new device, or a new slice. A non-empty directory with no marker is
  refused.
- No two chunks of a stripe are on one device, however many slices it has. The failure domain
  is never a slice.
- A slice has one owning executor; a device's number of slices does not change while it holds
  a chunk.
- A consumer id is minted once and never reused, and nothing below a binding depends on what
  kind of consumer it is.
- Pool definitions and bindings are committed policy; a joining node's copy is ignored.
- A pool's redundancy, failure domain and geometry never change after it holds a stripe.
- A class is what an operator wrote. Detection warns and never overrides.
- A peer names a slice, never an executor.
- No pool, device, slice or binding reaches the schema fingerprint.

## Prerequisites

[S1](prerequisites.md#required): known issue 46, a failure domain on a member, and free
bytes reported for every root, which here means every device. The inventory's part waits on nothing but the shape above.

## How it would be measured

Nothing on this page has a speed. ~~What it adds to a status report and to the pushed map is
sized by [X2](spikes.md#x2-placement-simulation), which extends the fanout tables
`shoal-spike fanout` already prints.~~ [X2](placement-simulation.md#what-the-map-holds-over-time)
sized what it adds to the pool map: a device of one slice is 301 bytes of the frame, a slice
beyond its first 64, and the lab's six devices 2,747 bytes in all. What it adds to a status
report, a size and free bytes a device, is M14's to measure.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `empty_directory_is_a_new_device` | A device directory emptied and restarted comes up under a new id with new slices, holding nothing, and the old ids are never served | M14 |
| `unmarked_directory_with_files_is_refused` | A non-empty directory with no marker stops the node by name | M14 |
| `pool_policy_is_committed_and_a_joiners_copy_is_ignored` | A joiner whose file defines another redundancy adopts the committed one | M13 |
| `unbound_bucket_is_refused_by_name` | A bootstrap with a bucket and no binding, or a binding to an undefined pool, is refused | M13 |
| `pool_short_of_failure_domains_refuses_writes_by_name` | A 4+2 pool over five hosts acknowledges nothing, and refuses each write by the name readiness gives it | M15 |
| `mislabelled_device_is_warned_about_and_not_overruled` | A device the kernel calls rotational under a class that says otherwise starts, keeps its class, and is named in a warning | M14 |
| `every_device_reports_its_free_bytes` | The leader's view holds a figure for each device of each member | M14 |
| `slices_of_one_device_share_its_failure` | A device of two slices under a `replicas: 2` pool with `failure_domain: device` and two devices never holds both copies of a stripe, and a device marked down takes both slices down | M14 |
| `rotational_pool_of_one_loss_is_refused` | A pool of `hdd` devices with a redundancy of 2+1 or two copies is refused at bootstrap unless its configuration accepts the exposure, and then starts named in a warning ([X12](recovery-scrub-rates.md#8-the-arithmetic)) | M19 |
| `consumers_share_a_pool_and_nothing_else` | Two buckets bound to one pool, with an object id forced equal in both, write the same stripe index; neither reads, rebuilds or reclaims a chunk of the other | M15 |

## Related

[S5](placement.md) for how a pool's slices are chosen for a stripe; [S6](device-store.md)
for what is in a slice's directory; [S13](isolation.md) for which executor owns it;
[S14](operations.md) for adding and removing one;
[Configuration](../getting-started/configuration.md#storage) and
[F53](../features/inventory-wizard.md) for what storage is today; [S17](prior-art.md#ceph)
for the model.
