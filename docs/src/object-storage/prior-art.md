# S17. Lessons from Ceph and other object stores

## Context

The brief for this part was to look at Ceph for inspiration. This page says what was looked
at, what was taken, what was changed and what was left, and it separates what was **read**
from what is **recalled**.

That separation is not politeness. This book already holds one design page that argued from
a dependency's prose and was confidently wrong, and it drew the lesson: "A design page that
argues from a dependency's prose rather than its source is a page that can be confidently
wrong" ([Direction](../direction/overview.md#the-recommended-order)). While this part was
being planned, a summary of Ceph's configuration reference gave its deep scrub read size as
4 MiB. The option file gives 512 K. Everything marked *read* below was read from Ceph's
repository at the tag `v20.2.0` (commit `69f84cc`, the Tentacle release), from the file
named, and not from a rendering or a summary of it. Everything else says *recalled*, and
[X14](spikes.md#x14-ceph-and-s3-at-the-source) is the work of turning the recalled into the
read wherever a decision leans on it. ~~X14 is the work~~ [X14 did that work](ceph-and-s3-sources.md)
on 2026-10-05. It read Ceph's code at the same tag, and AWS's model of the S3 API. It ran a Ceph
`v20.2.0` on the lab, so a third label joins the two: *observed*, for what that cluster did. Of
the nine recalled items below, four held, two held in part, two were wrong, and the ninth was a
gap.

**Similar words do not make similar protocols.** A Ceph placement group and a placement
group here share a name and a purpose and almost no mechanism.

## The systems

### Ceph

What each part of this design corresponds to, and what happened to the idea on the way. The
left column and the quotations use Ceph's own words. Its *shard* is a chunk's holder here, a
slice; its erasure coding *chunk* is a stripe chunk; its `stripe_unit` is a chunk unit; its
erasure coding *stripe* is a unit row; a RADOS object is a stripe; and RGW's *head object* is
what the `ObjectMeta` row is. An OSD is closest to a slice, but there is one for each executor
rather than one for each disk, and the failure domain is the device beneath it.

| Ceph | Here | Taken, or changed |
| --- | --- | --- |
| A RADOS object: bounded (`osd_max_object_size`, 128 MiB), written in place, with one write bounded too (`osd_max_write_size`, 90 MB) | A stripe ([S3](objects.md)) | Taken. The bounded, mutable unit is the reason an object of any size is not a special case |
| A file or an object striped over RADOS objects: "File data is chunked into RADOS objects of this size"; RGW's `rgw_obj_stripe_size` is 4 MiB | An object as stripes | Taken |
| A pool with a redundancy, and an erasure code profile that "cannot be modified after the pool is created" | A storage pool ([S4](pools-and-devices.md)) | Taken, including that it cannot change |
| RADOS pools that RGW, CephFS and RBD store into ~~at once~~, beside one another in one cluster, each in pools of its own: a pool is tagged with one application and a second is refused without an override (`src/mon/OSDMonitor.cc:9484-9487`; X14) | Storage pools serving consumers of mixed kinds ([S4](pools-and-devices.md#pools-and-bindings-are-policy)) | ~~Taken~~ Changed: a bucket is the first consumer, and a file system or a block volume can be bound to the same pool later, which a RADOS pool is not meant for |
| An OSD, one for each disk by default (`--osds-per-device` 1, `src/ceph-volume/ceph_volume/devices/lvm/batch.py:230-235`), and several for an NVMe device ("To fully utilize nvme devices multiple osds are required", `src/python-common/ceph/deployment/drive_group.py:238-241`), holding shards of many placement groups | A slice ([S4](pools-and-devices.md#a-device-has-slices)) | Changed: one for each executor, so a disk one core cannot drive is given several. The failure domain is the device, never the slice |
| A device class, set automatically to `hdd`, `ssd` or `nvme`, with a shadow hierarchy for each | A class ([S4](pools-and-devices.md#pools-and-bindings-are-policy)) | Changed: a label an operator writes, so that two pools of like devices are possible |
| A placement group; CRUSH; failure domains; `straw2`, which changes "mappings only to or from the bucket item whose weight has changed" | A placement group and a placement function ([S5](placement.md)) | Changed: a placement group is a sub-range of a tablet, so that it has a log. The function takes the property and not the hierarchy. [X2](placement-simulation.md) measured both halves: the property holds for a set, the hierarchy costs 1.5 to 2.3 times the least on a device change, and positions are kept by the tablet group, since no function of the map keeps them. It also measured the bias of drawing several devices of unequal weight, which Ceph's balancer corrects, and took the balancer's two remedies: placement weights (`crush-compat`'s weight set, `src/pybind/mgr/balancer/module.py:1221-1410`), and exceptions (`upmap`'s items, `src/osd/OSDMap.cc:5675-5995`). [X14](ceph-and-s3-sources.md#5-placement-indep-upmap-and-crush-compat) ran Ceph's own CRUSH on X2's shapes: 2.0 to 3.5 times the least for a device's change |
| A primary for each placement group, a log on every shard (`src/osd/PeeringState.h:1484`; entries ride every sub-write, `src/osd/ECMsgTypes.h:35`), and peering to reconcile them. Since Tentacle an optimized pool's data shards log only the writes that touch them | The row's tablet group and a conditional commit ([S7](write-path.md)) | **Not taken.** This is the largest difference, and [S18](contract.md#alternatives-rejected) is why |
| A write as "a two-phase process: commit and rollforward", committed in place with what is needed to roll it back kept aside: the old range cloned into an object named for the write's version (`src/osd/ECTransaction.cc:829-869`), removed when every shard has committed (`src/osd/PGBackend.cc:339-391`) | Stage, commit, apply | Changed: redo and not undo. One more round, and no decision to make after a failure |
| The proposal that a prepare writes "into a temporary object" and an apply "moves the data from the temporary object into the correct position", which `v20.2.0` did not build (X14) | A holder's stage and apply ([S6](device-store.md)) | Taken, from a document Ceph marks as a proposal |
| Overwrites on an erasure coded pool need BlueStore, "since BlueStore's checksumming is used during deep scrubs to detect bitrot or other corruption" | A checksum for every chunk unit, in the stripe chunk | Taken as a requirement: a store that takes writes in place has to carry its own checksums |
| Since Tentacle, in a pool with `allow_ec_optimizations`: partial writes, parity delta, and a version for each shard so that an untouched shard is not written. Shipped: each log entry names the shards it wrote (`src/osd/osd_types.h:4510`), the object's info keeps each untouched shard's version (`:6263`), and a shard with nothing to write is sent nothing (`src/osd/ECCommon.cc:824-840`; observed, X14 E2 and E3) | A label for each stripe chunk; parity delta ([S8](erasure-coding.md#a-partial-overwrite)) | Taken. There the versions live with the shards; here they are one replicated row |
| `min_size` of "`K+1` or greater to prevent loss of writes and loss of data". The default is `k + min(1, m - 1)` (`src/mon/OSDMonitor.cc:7800-7805`), so a 2+1 pool takes writes with no redundancy left (observed, X14 E1) | `f` of at least one ([P11](contract.md#the-contract)) | Taken, as a rule and not a default |
| BlueStore: checksums on everything written (`crc32c` by default, `xxhash32` and `xxhash64` offered), a log that is worth a separate device "only if the WAL device is faster than the primary device", small writes deferred on rotational media by default, and an overwrite under one allocation unit deferred on any device. A deferred write is redo: its new bytes are logged and applied in place after the commit (`src/os/bluestore/BlueStore.cc:15628-15635`, `:16340-16394`) | The device store's checksums and journal ([S6](device-store.md)) | Taken in shape. BlueStore itself, a raw device with its own allocator and key-value store, is not |
| RGW: a head object whose metadata is in extended attributes and which "may also inline up to `rgw_max_chunk_size` of object data, for efficiency and atomicity"; an index in a pool that is "necessarily replicated (cannot be EC)" | The `ObjectMeta` row, with small objects inline; metadata in replicated tables ([S3](objects.md)) | Taken. RGW's index, which is what lets it list, is what this part does without at first |
| "Erasure-coded pools do not support omap", so metadata goes to a replicated pool and data to an erasure coded one | R10 and R11 as the user stated them | The same split |
| Light scrubs daily and deep scrubs weekly (`osd_scrub_min_interval` 1 day, `osd_deep_scrub_interval` 7 days), three at once for an OSD, reading 512 K at a time. Each PG's next scrub is drawn: a light one 1 to 1.5 days after the last, a deep one at 7 ± 1.4 days (`src/osd/scrubber/scrub_job.cc:115-118`, `:251-255`). 512 K is 524,288 bytes (`src/common/options.h:424-426`) | Light and deep scrub ([S11](scrub.md)) | Taken as two kinds. The cadence is a hypothesis here |
| For an erasure coded pool, a shard checks its own chunk against its own stored checksum; a design for comparing shards by an XOR summary. Only a pool that never overwrites keeps a checksum a shard; an overwritable or optimized pool reports a digest of zero and relies on BlueStore (`src/osd/ECBackendL.cc:1797-1835`, `src/osd/ECBackend.cc:1223-1224`). The summary was not built (X14 E4) | A stripe chunk verified where it lies; a parity check by summaries ([S11](scrub.md#what-a-deep-scrub-proves-and-what-it-does-not)) | Taken, and the parity check goes further than Ceph does |
| A scheduler with classes for client work, recovery, and "backfill, scrub, snap trim and PG deletion": four in the code, `client`, `immediate` (sub-ops and peering), `background_recovery` (recovery or backfill of a degraded or undersized PG) and `background_best_effort` (`src/osd/scheduler/OpSchedulerItem.h:33-38`, `:204-211`) | An order of work on a slice's executor, and byte budgets ([S13](isolation.md#io-on-a-slice)) | Taken as an order. A share-based scheduler is not built |

Sources read, all under `https://github.com/ceph/ceph/blob/v20.2.0/`:
`doc/rados/operations/erasure-code.rst`, `doc/rados/operations/crush-map.rst`,
`doc/rados/configuration/bluestore-config-ref.rst`,
`doc/rados/configuration/osd-config-ref.rst`,
`doc/rados/configuration/mclock-config-ref.rst`, `doc/radosgw/layout.rst`,
`doc/cephfs/file-layouts.rst`, `doc/dev/osd_internals/erasure_coding/ecbackend.rst`,
`doc/dev/osd_internals/erasure_coding/proposals.rst`,
`doc/dev/osd_internals/erasure_coding/enhancements.rst`, and the defaults in
`src/common/options/osd.yaml.in`, `global.yaml.in` and `rgw.yaml.in`. Since X14, also the code
behind them at the same tag: both erasure coded back ends, peering and the PG log, the scrubber,
CRUSH and the balancer, BlueStore's write path, RGW and its index, the monitor's pool commands,
ceph-volume and cephadm, and Crimson ([X14](ceph-and-s3-sources.md#what-was-read) lists them).

Two of those are design documents and are cited as such: `proposals.rst` is headed "Proposed
Next Steps for ECBackend", and `enhancements.rst` opens with "Our objective is to improve the
performance of erasure coding" and goes on to say what its authors would like to build. The
user documentation confirms that the Tentacle release shipped optimizations that improve
"performance for smaller I/Os" and eliminate padding. It does not say that every mechanism
in the design document shipped as written, and this page does not either.

~~**Recalled, not read:** that an acknowledgement waits for every shard of the acting set;
that monitors fence a primary by map epochs; that a placement group below `min_size` blocks
reads as well as writes; that CRUSH keeps positions stable for an erasure coded pool by
choosing independently for each (X2 simulated that idea, and it moved 1.1 to 2.6 times the
least, [positions](placement-simulation.md#positions)); that RADOS carries a truncate sequence on every operation;
that RGW defers the deletion of a replaced object's tail for two hours (the default of
`rgw_gc_obj_min_wait` was read; what it governs was not); that an OSD is deployed one for
each disk; that RGW, CephFS and RBD store into RADOS pools side by side; and everything about
Crimson and SeaStore beyond their being Ceph's thread-per-core work.~~ **Recalled until X14, and
now read** ([its record](ceph-and-s3-sources.md#the-comparison)):

| Was recalled | Now | Where |
| --- | --- | --- |
| An acknowledgement waits for every shard of the acting set | **In part.** Every shard sent the write, durably committed, which is the acting set and any shard being recovered or backfilled; but an optimized pool sends nothing to a data shard a partial write does not touch, and does not wait for it. *Observed* | `src/osd/ECCommon.cc:824-840`, `src/osd/ECCommonL.cc:881-888`, `src/osd/ReplicatedBackend.cc:598-600`; X14 E2 |
| Monitors fence a primary by map epochs | **Wrong.** The monitors commit the map and each primary's `up_thru`; the OSDs fence, dropping anything sent in an older interval, so a deposed primary cannot gather the commits a write needs. Peering waits to hear from every earlier interval that could have written, and reads are fenced by a lease | `src/osd/PG.cc:1934-1938`, `:1991-2028`; `src/osd/osd_types.cc:4417-4419`; `src/osd/PrimaryLogPG.cc:853-882` |
| A PG below `min_size` blocks reads as well as writes | **Held.** It is `peered`, not active, and every client op waits. *Observed* | `src/osd/PeeringState.cc:6825-6829`, `src/osd/PrimaryLogPG.cc:1920-1927`; X14 E1 |
| CRUSH keeps an erasure coded pool's positions by choosing independently for each | **Held**, and dearer than X2's model of it: a position that finds nothing is a hole and is never shifted, and a device's change moves 2.0 to 3.5 times the least. *Observed* through `crushtool` | `src/crush/mapper.c:633-805`; X14 E6 |
| RADOS carries a truncate sequence on every operation | **Wrong.** Only on the extent operations CephFS sends; one sequence and size an object; a stale write clipped to the current size, which does not keep cut bytes out once the object has grown again | `src/include/rados.h:415-433`, `:585-593`; `src/osd/PrimaryLogPG.cc:6765-6803` |
| RGW defers deleting a replaced object's tail for two hours | **Held.** The old tail is queued with an expiry two hours on and removed by the next hourly pass. The defer-on-read the option describes is disabled, so the wait is a timer and not a lease. *Observed* | `src/rgw/driver/rados/rgw_rados.cc:6010-6049`, `src/rgw/driver/rados/rgw_gc.cc:120-138`, `src/rgw/rgw_op.cc:2380-2381`; X14 E5 |
| An OSD is deployed one for each disk | **In part.** One a device by default; several for a fast NVMe device | `src/ceph-volume/ceph_volume/devices/lvm/batch.py:230-235` |
| RGW, CephFS and RBD store into RADOS pools side by side | **Held** for a cluster: each in pools of its own, one application a pool | `src/mon/OSDMonitor.cc:9484-9487` |
| Crimson and SeaStore, beyond being thread-per-core | **Read.** A tech preview, "not suitable for production use"; Seastar reactors a core; its erasure coded back end a stub, so replicated pools only | `doc/dev/crimson/crimson.rst:71-72`, `:95-96`; `src/crimson/osd/ec_backend.cc` |

### What Ceph depends on that Shoal lacks, and the reverse

| Ceph leans on | Shoal has instead |
| --- | --- |
| A map authority that ~~appoints a primary and fences the old one~~ commits the map every party computes the primary from, and each primary's `up_thru`; the OSDs fence the old primary themselves ([X14](ceph-and-s3-sources.md#2-peering-fencing-and-min_size)) | Nothing may: [P5](../distributed/protocol.md#the-contract) forbids the control plane to authorize a writer. The row's group elects its own |
| A local store with transactions and a cheap clone of a range | Files on a filesystem. Hence redo |
| Clients that compute placement and talk to the primary | ~~Clients that reach any node ([D7](../direction/shard-aware-routing.md) is unbuilt)~~ Clients that compute the tablet placement and send a write to its group's preferred leader ([F74](../features/client-routing.md)), and reach any node for an object's bytes, since they hold no pool map |

| Shoal has | Which makes |
| --- | --- |
| A replicated, ordered, fenced log for every tablet already | The decision one conditional commit, with no peering |
| A retry identity and a table that remembers answers | A retried write the same write |
| A driver pattern with committed progress | Rebuild and scrub resumable by the next leader |
| State derived at apply by every replica | The record of missed writes, without a log on every holder |

### Other stores

All of this is **recalled** and none of it was read for this page. It is here to say where
ideas came from, and ~~nothing on another page rests on it~~ two other pages lean on it: the
overview takes "stripe" and "chunk" from md RAID, and candidate D of S7 is ZFS's way. X14 read
Ceph and S3 and none of these, so they stay recalled.

- **MinIO** erasure codes every object as it is written, across a fixed set of drives, keeps
  small objects inline in its metadata file, and checksums each block of each part. Its
  objects are replaced whole. It is the nearest thing to
  [the whole-object path](write-path.md#a-whole-object-in-one-commit) and has no path at all
  for a write in place.
- **Haystack, f4 and SeaweedFS** pack many small objects into large files with an index, and
  the last two erasure code a file once it is sealed. That is the "many chunks in one file"
  candidate of [X6](spikes.md#x6-the-device-store-on-ssd), ~~and~~ which X6 measured as a floor
  and did not take: a file a chunk is within 30% of it above 1 MiB
  ([X6](device-store-ssd.md#1-a-whole-chunk)), and "replicate now, encode later",
  which [S8](erasure-coding.md#alternatives-rejected) leaves out.
- **HDFS** erasure coding stripes a file in cells and has the client encode. Both are
  options here: the round-robin layout of [S8](erasure-coding.md#geometry), and the client
  that encodes once it can route.
- **ZFS** avoids the write hole by never overwriting in place: every write is a new full
  stripe and a pointer changed. It is candidate D of [S7](write-path.md#the-candidates), and
  it is the proof that the candidate is sound as well as the source of its cost.
- **Azure's** storage and several others use codes that rebuild from fewer chunks. A pool's
  redundancy is a pool's setting, so nothing here precludes one.
- **Linux md RAID** (*recalled*) is where "stripe" and "chunk" are taken from. Its stripe is
  one chunk on each disk of the array, combined by parity, and its chunk size is how many
  bytes go to one disk before the next. A stripe here is the same arrangement, made large
  enough to be placed on its own, with a slice where md has a disk.
- **GlusterFS** (*recalled*) builds a volume from bricks, a brick being a directory on one
  server's filesystem, and replicates or erasure codes across them. That is the slice's
  shape: a storage directory on a server as the unit storage is made of. Unlike a brick, a
  slice may share its device with other slices, and no two chunks of a stripe go to one
  device.

## Implementation reading list

`latest` documentation and default branches are for finding things. A decision is recorded
against a release and a path, as [S18](contract.md#decision-record) records its own.

| Reference | Why to read it | Gate |
| --- | --- | --- |
| [Ceph: erasure code](https://github.com/ceph/ceph/blob/v20.2.0/doc/rados/operations/erasure-code.rst) | Overwrites, the Tentacle optimizations, `stripe_unit` guidance, `min_size` | Q16, Q20; M18 |
| [Ceph: erasure coding enhancements](https://github.com/ceph/ceph/blob/v20.2.0/doc/dev/osd_internals/erasure_coding/enhancements.rst) | Partial writes, parity delta, the version vector, backfill with it, deep scrub by summaries | Q17, Q20, Q28; M16 to M18 |
| [Ceph: ECBackend](https://github.com/ceph/ceph/blob/v20.2.0/doc/dev/osd_internals/erasure_coding/ecbackend.rst) and [its proposals](https://github.com/ceph/ceph/blob/v20.2.0/doc/dev/osd_internals/erasure_coding/proposals.rst) | Commit and rollforward; prepare and apply; what the primary waits for | Q14; before M11 |
| Ceph: `src/osd/ECBackend.cc` and the peering state machine, at the same tag | What an acknowledgement really waits for, and what peering really decides. ~~Not read~~ Read by [X14](ceph-and-s3-sources.md#1-what-an-acknowledgement-waits-for) | Q14, Q16; before M11 |
| [Ceph: CRUSH maps](https://github.com/ceph/ceph/blob/v20.2.0/doc/rados/operations/crush-map.rst), and the CRUSH paper | Classes, failure domains, `straw2`; how positions stay put | Q19; M14 |
| [Ceph: BlueStore configuration](https://github.com/ceph/ceph/blob/v20.2.0/doc/rados/configuration/bluestore-config-ref.rst) | Checksums, the log on a faster device, deferred writes | Q21, Q22, Q23; M14, M19 |
| [Ceph: RGW layout](https://github.com/ceph/ceph/blob/v20.2.0/doc/radosgw/layout.rst) | Head and tail, inline data, the index | Q25, Q32; M12 |
| The S3 API reference: listing, multipart, conditional requests, checksums | What a later gateway would need the metadata to offer. ~~Not read~~ Read by [X14](ceph-and-s3-sources.md#what-the-metadata-must-keep-possible) in AWS's Smithy model (`aws/api-models-aws` at `a0767ac42e27`) and the User Guide | Q32 |
| [fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html), [rename(2)](https://man7.org/linux/man-pages/man2/rename.2.html), [fallocate(2)](https://man7.org/linux/man-pages/man2/fallocate.2.html), [copy_file_range(2)](https://man7.org/linux/man-pages/man2/copy_file_range.2.html) | What a sync covers, what a rename promises, what written-ahead space is, when a copy is a clone | Q22; M14 |
| The sources of the candidate crates, at the versions on [S18](contract.md#decision-record) | What each offers, as opposed to what its README says | Q20, Q21; M14, M18 |
| [RFC 6330](https://www.rfc-editor.org/rfc/rfc6330), RaptorQ | What a fountain code promises about decoding, which is not "any k" | Q20 |

## The comparison

Ceph solved a harder problem than this part sets itself: its placement groups are their own
authority, with no replicated log beside them to lean on. Shoal has that log, for every
tablet, built and tested. The design that follows from that is not Ceph with the names
changed. It keeps Ceph's units (a bounded mutable object, a pool, a class, a placement
group, a stripe chunk with its own checksums, two scrubs) and replaces Ceph's hardest machinery
(primaries, a log on every holder, peering, rollback) with one conditional commit in a group that
already exists, at the price of a round.

Whether that price is right is not something this page can say. It is
[Q14](contract.md#questions-to-answer), and it is settled by a model and two measurements.

## Related

[S18](contract.md) for the decisions these sources feed; [S7](write-path.md) for the
protocol compared above; [Spikes](spikes.md#x14-ceph-and-s3-at-the-source) for the reading
still owed; [C12](../distributed/prior-art.md) and [D9](../direction/prior-art.md) for the
book's other reviews of other people's systems.
