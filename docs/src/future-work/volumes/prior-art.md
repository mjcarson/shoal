# W15. Lessons from RBD, DRBD, Mayastor and others

## Context

Every part of a replicated block device has been built before, by systems whose shape is close to
Shoal's or deliberately far from it. Ceph's RBD is the closest: an image cut into fixed-size objects
placed by a hash over placement groups, which is a volume cut into stripes placed the same way.
DRBD is the furthest and the fastest: one writer replicating a whole device to its peers in the
kernel. This page records what each does with the four things a volume has to get right, who
orders writes, how many hops a write takes, how a superseded writer is stopped, and what is
resynced after a crash, and says for each what Shoal takes and what it does not. It extends
[S17](../../object-storage/prior-art.md) and [X14](../../object-storage/ceph-and-s3-sources.md),
which read Ceph's object path at the source, and does not repeat them.

Every fact below was read on 2026-10-26 from the page or source file its link names; **(src)** is a
source file at the tag its link names. A claim that was not read is marked **recalled**, and one
seen only in a search engine's summary is marked **(snippet)**. None of this has been run on the
lab; reading the systems' write and fencing paths at pinned releases, and running RBD and DRBD
beside the lane, is [X30](spikes.md#x30-rbd-drbd-and-mayastor-read-at-the-source)'s, optional.

## The systems

### Ceph RBD

**An image is a header and fixed-size data objects.** `rbd_id.<name>` names the image's id, the
header is `rbd_header.<id>`, the object map `rbd_object_map.<id>`, and the data objects
`rbd_data.<id>.%016llx` (src: [rbd_types.h:28-31](https://github.com/ceph/ceph/blob/v20.2.0/src/include/rbd_types.h#L28-L31),
[ImageCtx.cc:239-246](https://github.com/ceph/ceph/blob/v20.2.0/src/librbd/ImageCtx.cc#L239-L246)).
The default object is order 22, 4 MiB, settable from 4 KiB to 32 MiB, and the default features are
layering, exclusive-lock, object-map, fast-diff and deep-flatten
([rbd.yaml.in:44-54](https://github.com/ceph/ceph/blob/v20.2.0/src/common/options/rbd.yaml.in#L44-L54)).
This is the shape [W16](volumes-and-rows.md) takes: one row and fixed-size stripes, the stripe
smaller by default for a reason RBD does not have ([Q47](contract.md#questions-to-answer)).

**One writer, by a lock on the header, fenced by a blocklist.** The exclusive lock is a `cls_lock`
on the header ([AcquireRequest.cc:113](https://github.com/ceph/ceph/blob/v20.2.0/src/librbd/managed_lock/AcquireRequest.cc#L113)),
its cookie `"auto <watch handle>"` ([Utils.cc:14-35](https://github.com/ceph/ceph/blob/v20.2.0/src/librbd/managed_lock/Utils.cc#L14-L35)).
Holders hand it over cooperatively by `RequestLock`, `AcquiredLock` and `ReleasedLock` notifications
over watch and notify ([ImageWatcher.cc:556-616](https://github.com/ceph/ceph/blob/v20.2.0/src/librbd/ImageWatcher.cc#L556-L616)),
and a lock is broken only if its holder's address and cookie are no longer among the header's
watchers ([BreakRequest.cc:65-97](https://github.com/ceph/ceph/blob/v20.2.0/src/librbd/managed_lock/BreakRequest.cc#L65-L97)),
after which the old holder is blocklisted (`rbd_blocklist_on_break_lock`, true). The documentation
calls blocklisting "storage-level resource fencing" and says that between cooperating clients
"writes just get linearized" ([exclusive locks](https://docs.ceph.com/en/latest/rbd/rbd-exclusive-locks/)).
A blocklist lives in the OSD map, which the monitors commit and every OSD applies; Shoal has no
such map to wait on, and [W18](attach-and-takeover.md) bars a writer where its writes land instead.

**What exists is tracked beside the data.** The object map tracks "which objects actually exist", and
fast-diff uses it to compare snapshots ([config reference](https://docs.ceph.com/en/latest/rbd/rbd-config-ref/)).
With journaling, for mirroring, "every write ... is first recorded to the associated journal" and the
documentation says to "expect write latencies to nearly double"
([mirroring](https://docs.ceph.com/en/latest/rbd/rbd-mirroring/)).

**Clones read through to their parent.** A parent snapshot is protected; a read of an object the
clone does not have falls through to the parent, and the first write copies it up with an atomic
create; `rbd_children` maps parents to clones ([layering](https://docs.ceph.com/en/latest/dev/rbd-layering/)).
On an erasure coded pool the data goes to a `--data-pool` with `allow_ec_overwrites` and the
metadata stays replicated ([erasure code](https://docs.ceph.com/en/latest/rados/operations/erasure-code/));
X14 recorded what a partial write costs there.

**Caches on the client.** librbd's own cache defaults to write-around at 32 MiB
([config reference](https://docs.ceph.com/en/latest/rbd/rbd-config-ref/)). The persistent write-back
cache keeps a log on a local SSD with "checkpoints internally" so what reaches the cluster is crash
consistent, and needs the exclusive lock ([pwl](https://docs.ceph.com/en/latest/rbd/rbd-persistent-write-log-cache/)).

**Three ways onto a host.** librbd in a process such as QEMU; the kernel's `krbd`, which gained
exclusive-lock in 4.9, object-map and fast-diff in 5.3 and data pools in 4.11 and has no journaling
([reef reference](https://docs.ceph.com/en/reef/_sources/rbd/rbd-config-ref.rst.txt)); and `rbd-nbd`,
librbd driving the kernel's NBD over netlink (src: [rbd-nbd.cc:42-72](https://github.com/ceph/ceph/blob/v20.2.0/src/tools/rbd_nbd/rbd-nbd.cc#L42-L72)).

**What a 4 KiB write costs, and why.** With three copies on PM983 NVMe over 100 GbE, one client at
queue depth one, a 4 KiB `O_DSYNC` write took 0.418 to 0.421 ms on average and 0.75 ms at p99.9
([Reef](https://ceph.io/en/news/blog/2023/reef-freeze-rbd-performance/)); about 0.7 ms with three
copies over 10GBase-T ([dev list, 2021](https://lists.ceph.io/hyperkitty/list/dev@ceph.io/message/E2TJCNZRPYBFCIWWL6GHMD733JCFGJTS/));
about 1 ms on SATA SSDs over 25 GbE with two copies and 1.37 ms at 2+1
([Vitastor's test](https://vitastor.io/en/docs/performance/comparison1.html)); and 0.48 ms on one OSD
with one copy and no network ([Crimson, 2025](https://ceph.io/en/news/blog/2025/crimson-seastore-vs-classic/)).
The client writes to the primary, the primary to the replicas, and it answers once they confirm
([architecture](https://docs.ceph.com/en/reef/architecture/)); each copy pays a BlueStore commit
([X14](../../object-storage/ceph-and-s3-sources.md)); and the classic OSD passes each request
through thread pools, which "incurs significant latency costs due to cross-core communications"
([Crimson, 2023](https://ceph.io/en/news/blog/2023/crimson-multi-core-scalability)).

**Crimson is thread-per-core and not faster at this yet.** It is shared-nothing and run to
completion, with placement groups sharded over cores and at most one cross-core hop in its
messenger ([2023](https://ceph.io/en/news/blog/2023/crimson-multi-core-scalability)); on 4K writes
at queue depth one its SeaStore did 31.4K IOPS against classic's 65.9K
([2025](https://ceph.io/en/news/blog/2025/crimson-seastore-vs-classic/)); and it "is in a tech preview
stage and is **not suitable for production use**" (X14, `doc/dev/crimson/crimson.rst:95-96`). Shoal is
thread-per-core already; Crimson is the evidence that a core of one's own does not by itself make a
small write fast, and the path does.

### DRBD and LINSTOR

**Protocol C waits for every copy.** A write completes "only after both the local and the remote disk
write(s) have been confirmed", to up to 32 peers, with a quorum of a majority and a diskless node as a
tiebreaker; the out-of-sync bitmap is a bit for each 4 KiB, about 32 MiB of memory for each TiB
([user's guide](https://linbit.com/drbd-user-guide/drbd-guide-9_0-en/)).

**The activity log bounds a crash's resync.** Hot extents of 4 MiB, 1,237 of them by default, are
kept in an activity log; activating a new extent costs a metadata write, and after a primary's crash
only the extents in the log are resynced; reads prefer the local copy by default
([drbd.conf](https://manpages.ubuntu.com/manpages/noble/man5/drbd.conf-9.0.5.html)). This is the
tract bitmap's ancestor ([W16](volumes-and-rows.md#tracts)): a commit when a writer first enters a
region, none after, and a resync of the regions marked.

**Control apart from data.** LINSTOR is a controller, its database an embedded H2 by default, and
stateless satellites; "the control and data planes are separate"
([LINSTOR guide](https://linbit.com/drbd-user-guide/linstor-guide-1_0-en/)).

**Why it is fast.** It is "network RAID 1" in the kernel
([kernel documentation](https://docs.kernel.org/admin-guide/blockdev/drbd/index.html)). At 4K and queue
depth one over 25 GbE a local disk did 32.2K IOPS, protocol A 30.2K and protocol C 11K
([drbd-user, 2020](https://lists.linbit.com/pipermail/drbd-user/2020-November/025747.html)), against a
budget of 13.3 + 31.1 + 13.3 = 57.7 µs that protocol C reached 63% of
([reply](https://lists.linbit.com/pipermail/drbd-user/2020-November/025751.html)); over 100 GbE a write
took about 52 µs against 11.6 µs on the raw drive
([E4's test](https://linbit.com/blog/independent-performance-testing-of-drbd-by-e4/)). Each node
replicates one whole backing device, so nothing is striped and a volume is no larger than one node's
device: an inference from "network RAID 1", not read.

### OpenEBS Mayastor

**A nexus fans every write out.** Mayastor is Rust on SPDK; its nexus is a proxy whose children
"receive the same IO", replicated over NVMe-oF ([README](https://github.com/openebs/mayastor)). It
needs at least 1,024 hugepages of 2 MB, Linux 5.1 and `nvme_tcp`
([run.md](https://github.com/openebs/mayastor/blob/develop/doc/run.md)), and runs one reactor a core
in its core mask, each spinning in `poll()` ([design](https://github.com/openebs/mayastor/blob/develop/doc/design/mayastor.md)).

**A superseded nexus is fenced at the target.** On failover the new nexus installs NVMe reservation
keys, which fences the old one, and republishes under the same NQN for ANA multipath; its
configuration is in etcd ([ha-failover](https://github.com/openebs/mayastor/blob/develop/doc/design/ha-failover.md)).
A replica that returns within ten minutes gets a partial rebuild from logged writes
([Puls8](https://docs.datacore.com/Puls8/Puls8-WebHelp/Rebuilds.htm), **(snippet)**). The bar a
holder keeps ([W18](attach-and-takeover.md#the-three-bars)) is that reservation, and a missed mark
([W17](writer-lane.md#a-position-that-does-not-answer)) that log.

### Longhorn

**An engine per volume on the attaching node.** Replicas are sparse files and the read index costs a
byte for each 4K block; V1 exports over iSCSI; V2 uses SPDK, a RAID bdev over lvol replicas,
exported by NVMe-TCP or ublk, with polling on dedicated cores
([concepts](https://longhorn.io/docs/latest/concepts/)). Its ublk front end "may offer better
performance" than NVMe-oF and is less mature
([ublk front end](https://longhorn.io/docs/1.11.1/v2-data-engine/features/ublk-frontend-support/)).
Longhorn's V2 against V1 claims two to four times the writes and a half to two thirds less latency
(**(snippet)**; the page returned 503).

### Vitastor

**One hop from the client is DRBD's speed with striping.** Against Ceph 15.2.4 at queue depth one,
Ceph took about 1 ms and Vitastor 0.14 ms natively and 0.166 ms through NBD
([comparison](https://vitastor.io/en/docs/performance/comparison1.html)), which prices NBD at about
26 µs. That its client replicates to the OSDs itself is **recalled**, not read; it is the closest
published shape to the writer lane, and X30 reads it if it is run.

### SeaweedFS

**No block store ships.** The only trace is draft "V3 Phase 14" design notes dated 2026-04-19, which
exclude "user data-path production readiness"
([sw-block/design](https://github.com/seaweedfs/seaweedfs/tree/master/sw-block/design)). Its volume
servers' replication waits for every replica and nothing repairs a missing one automatically
([replication](https://github.com/seaweedfs/seaweedfs/wiki/Replication)), which the
[store comparison](../../object-storage/store-comparison.md) met from the other side.

### Published comparisons

Flant's comparison, republished by LINBIT in 2022, ranked Mayastor first for random writes, Vitastor
second and Ceph behind, called Ceph "resource-heavy", and gave its numbers only in charts
([LINBIT](https://linbit.com/?p=25009)); a 2020 study put Mayastor first on mixed workloads and Ceph
third ([Blocks and Files](https://www.blocksandfiles.com/block/2020/11/30/portworx-and-mayastor-kubernetes-block-storage-flies-highest/1601090)).
None is a measurement this plan leans on. [W25](performance.md) measures 4 KiB at queue depth one
and thirty-two, with cpu an IOPS, on the lab itself.

## What Shoal takes, and what it does not

| From | Taken | Not taken, and why |
| --- | --- | --- |
| RBD | An image as one row and fixed-size stripes over placement groups; a writer's right committed beside the image and handed over cooperatively; clones that read through to a parent; metadata kept apart from the data's pool policy | A blocklist in a cluster map every node must apply before a new writer is safe: Shoal has no such map, and a control proposal under load took 0.76 to 3.5 s at the median ([F88](../../features/recovery-and-moves.md#on-the-lab)). A journal that doubles a write. A primary that forwards every write: a second hop |
| DRBD | Protocol C's floor, one round trip and a remote write, as the target; the activity log as tracts; local reads | A volume bound to one node's device; a kernel module |
| Mayastor | A superseded writer fenced where its writes land; a partial rebuild from what was missed | Polling cores, which would starve the database's executors; hugepages; SPDK |
| Longhorn | An engine on the attaching host; ublk as the device | An engine process for each volume; polling cores |
| Vitastor | One hop from the client as the shape to measure against | Nothing yet: its replication is recalled, not read |
| Crimson | The lesson that a core of one's own does not make a small write fast | — |

## Implementation reading list

| Source | What to read it for | Before |
| --- | --- | --- |
| `librbd/managed_lock/` and `librbd/ImageWatcher.cc` at v20.2.0 | How a cooperative handover and a forced break are told apart | [M31](milestones.md#m31-across-nodes) |
| `librbd/object_map/` at v20.2.0 | When the map is written before a write, and what fast-diff reads | [M30](milestones.md#m30-the-lane-on-one-node) |
| DRBD's activity log and resync (`drbd_actlog.c`, **recalled** as its name) | How extents enter and leave the log, and what a crash resyncs | M31 |
| [Mayastor's ha-failover](https://github.com/openebs/mayastor/blob/develop/doc/design/ha-failover.md) | Reservations as fencing; what a republish does to writes in flight | M31 |
| [ublk](https://docs.kernel.org/block/ublk.html) | Queues, copies, zero copy and a server's recovery | [M33](milestones.md#m33-the-host-block-device) |

## The comparison

| System | Who orders writes | Hops a write | A superseded writer is stopped by | After a writer's crash | 4 KiB at queue depth one |
| --- | --- | --- | --- | --- | --- |
| RBD | Each object's primary OSD | Client, primary, replicas: two | A blocklist in the OSD map | Peering of each placement group's log | 0.42 ms (NVMe, 100 GbE) to 1 ms |
| DRBD | The one primary | One, from the primary's host | Quorum and a diskless tiebreaker | The activity log's extents | 52 to 91 µs |
| Mayastor | The nexus | Host to nexus, nexus to replicas: two | NVMe reservations at the replicas | A partial rebuild from logged writes | Not read |
| Longhorn | The engine on the attaching node | One | Not read | Not read | Not read |
| Vitastor | Recalled: its client | One, recalled | Not read | Not read | 0.14 ms |
| **Shoal, proposed** | **The attached writer, by block label** | **One, from the host** | **Bars in the volume row, the stripe table's groups and every holder** | **First touch of the dirty tracts** | **To be measured: [X24](spikes.md#x24-one-4-kib-write-three-ways)** |

## Sources

All read 2026-10-26.

- Ceph: at v20.2.0, `src/include/rbd_types.h`, `src/librbd/ImageCtx.cc`, `src/common/options/rbd.yaml.in`,
  `src/librbd/managed_lock/AcquireRequest.cc`, `src/librbd/managed_lock/Utils.cc`,
  `src/librbd/managed_lock/BreakRequest.cc`, `src/librbd/ImageWatcher.cc`, `src/tools/rbd_nbd/rbd-nbd.cc`;
  [exclusive locks](https://docs.ceph.com/en/latest/rbd/rbd-exclusive-locks/),
  [config reference](https://docs.ceph.com/en/latest/rbd/rbd-config-ref/),
  [mirroring](https://docs.ceph.com/en/latest/rbd/rbd-mirroring/),
  [layering](https://docs.ceph.com/en/latest/dev/rbd-layering/),
  [erasure code](https://docs.ceph.com/en/latest/rados/operations/erasure-code/),
  [persistent write log](https://docs.ceph.com/en/latest/rbd/rbd-persistent-write-log-cache/),
  [architecture](https://docs.ceph.com/en/reef/architecture/),
  [Reef performance](https://ceph.io/en/news/blog/2023/reef-freeze-rbd-performance/),
  [Crimson 2023](https://ceph.io/en/news/blog/2023/crimson-multi-core-scalability),
  [Crimson 2025](https://ceph.io/en/news/blog/2025/crimson-seastore-vs-classic/).
- DRBD and LINSTOR: [user's guide 9.0](https://linbit.com/drbd-user-guide/drbd-guide-9_0-en/),
  [drbd.conf](https://manpages.ubuntu.com/manpages/noble/man5/drbd.conf-9.0.5.html),
  [LINSTOR guide](https://linbit.com/drbd-user-guide/linstor-guide-1_0-en/),
  [kernel documentation](https://docs.kernel.org/admin-guide/blockdev/drbd/index.html),
  [drbd-user, 2020](https://lists.linbit.com/pipermail/drbd-user/2020-November/025747.html) and its
  [reply](https://lists.linbit.com/pipermail/drbd-user/2020-November/025751.html),
  [E4's test](https://linbit.com/blog/independent-performance-testing-of-drbd-by-e4/).
- Mayastor: [README](https://github.com/openebs/mayastor), [run.md](https://github.com/openebs/mayastor/blob/develop/doc/run.md),
  [design](https://github.com/openebs/mayastor/blob/develop/doc/design/mayastor.md),
  [ha-failover](https://github.com/openebs/mayastor/blob/develop/doc/design/ha-failover.md).
- Longhorn: [concepts](https://longhorn.io/docs/latest/concepts/),
  [ublk front end](https://longhorn.io/docs/1.11.1/v2-data-engine/features/ublk-frontend-support/).
- [Vitastor's comparison](https://vitastor.io/en/docs/performance/comparison1.html);
  [SeaweedFS sw-block notes](https://github.com/seaweedfs/seaweedfs/tree/master/sw-block/design);
  [LINBIT's republication of Flant's comparison](https://linbit.com/?p=25009).

## Related

[S17](../../object-storage/prior-art.md) and [X14](../../object-storage/ceph-and-s3-sources.md), which
this extends; [W17](writer-lane.md) and [W18](attach-and-takeover.md) for the lane and the bars these
lessons shaped; [the filesystem's W29](../filesystem/prior-art.md) for CephFS, Lustre, JuiceFS and the
others.
