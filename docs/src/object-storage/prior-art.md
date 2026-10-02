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
read wherever a decision leans on it.

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
| RADOS pools that RGW, CephFS and RBD store into at once, beside one another | Storage pools serving consumers of mixed kinds ([S4](pools-and-devices.md#pools-and-bindings-are-policy)) | Taken: a bucket is the first consumer, and a file system or a block volume can be bound to the same pool later |
| An OSD, one for each disk, holding shards of many placement groups | A slice ([S4](pools-and-devices.md#a-device-has-slices)) | Changed: one for each executor, so a disk one core cannot drive is given several. The failure domain is the device, never the slice |
| A device class, set automatically to `hdd`, `ssd` or `nvme`, with a shadow hierarchy for each | A class ([S4](pools-and-devices.md#pools-and-bindings-are-policy)) | Changed: a label an operator writes, so that two pools of like devices are possible |
| A placement group; CRUSH; failure domains; `straw2`, which changes "mappings only to or from the bucket item whose weight has changed" | A placement group and a placement function ([S5](placement.md)) | Changed: a placement group is a sub-range of a tablet, so that it has a log. The function takes the property and not the hierarchy |
| A primary for each placement group, a log on every shard, and peering to reconcile them | The row's tablet group and a conditional commit ([S7](write-path.md)) | **Not taken.** This is the largest difference, and [S18](contract.md#alternatives-rejected) is why |
| A write as "a two-phase process: commit and rollforward", committed in place with what is needed to roll it back kept aside | Stage, commit, apply | Changed: redo and not undo. One more round, and no decision to make after a failure |
| The proposal that a prepare writes "into a temporary object" and an apply "moves the data from the temporary object into the correct position" | A holder's stage and apply ([S6](device-store.md)) | Taken, from a document Ceph marks as a proposal |
| Overwrites on an erasure coded pool need BlueStore, "since BlueStore's checksumming is used during deep scrubs to detect bitrot or other corruption" | A checksum for every chunk unit, in the stripe chunk | Taken as a requirement: a store that takes writes in place has to carry its own checksums |
| Since Tentacle: partial writes, parity delta, and a version for each shard so that an untouched shard is not written | A label for each stripe chunk; parity delta ([S8](erasure-coding.md#a-partial-overwrite)) | Taken. There the versions live with the shards; here they are one replicated row |
| `min_size` of "`K+1` or greater to prevent loss of writes and loss of data" | `f` of at least one ([P11](contract.md#the-contract)) | Taken |
| BlueStore: checksums on everything written (`crc32c` by default, `xxhash32` and `xxhash64` offered), a log that is worth a separate device "only if the WAL device is faster than the primary device", small writes deferred on rotational media by default | The device store's checksums and journal ([S6](device-store.md)) | Taken in shape. BlueStore itself, a raw device with its own allocator and key-value store, is not |
| RGW: a head object whose metadata is in extended attributes and which "may also inline up to `rgw_max_chunk_size` of object data, for efficiency and atomicity"; an index in a pool that is "necessarily replicated (cannot be EC)" | The `ObjectMeta` row, with small objects inline; metadata in replicated tables ([S3](objects.md)) | Taken. RGW's index, which is what lets it list, is what this part does without at first |
| "Erasure-coded pools do not support omap", so metadata goes to a replicated pool and data to an erasure coded one | R10 and R11 as the user stated them | The same split |
| Light scrubs daily and deep scrubs weekly (`osd_scrub_min_interval` 1 day, `osd_deep_scrub_interval` 7 days), three at once for an OSD, reading 512 K at a time | Light and deep scrub ([S11](scrub.md)) | Taken as two kinds. The cadence is a hypothesis here |
| For an erasure coded pool, a shard checks its own chunk against its own stored checksum; a design for comparing shards by an XOR summary | A stripe chunk verified where it lies; a parity check by summaries ([S11](scrub.md#what-a-deep-scrub-proves-and-what-it-does-not)) | Taken |
| A scheduler with classes for client work, recovery, and "backfill, scrub, snap trim and PG deletion" | An order of work on a slice's executor, and byte budgets ([S13](isolation.md#io-on-a-slice)) | Taken as an order. A share-based scheduler is not built |

Sources read, all under `https://github.com/ceph/ceph/blob/v20.2.0/`:
`doc/rados/operations/erasure-code.rst`, `doc/rados/operations/crush-map.rst`,
`doc/rados/configuration/bluestore-config-ref.rst`,
`doc/rados/configuration/osd-config-ref.rst`,
`doc/rados/configuration/mclock-config-ref.rst`, `doc/radosgw/layout.rst`,
`doc/cephfs/file-layouts.rst`, `doc/dev/osd_internals/erasure_coding/ecbackend.rst`,
`doc/dev/osd_internals/erasure_coding/proposals.rst`,
`doc/dev/osd_internals/erasure_coding/enhancements.rst`, and the defaults in
`src/common/options/osd.yaml.in`, `global.yaml.in` and `rgw.yaml.in`.

Two of those are design documents and are cited as such: `proposals.rst` is headed "Proposed
Next Steps for ECBackend", and `enhancements.rst` opens with "Our objective is to improve the
performance of erasure coding" and goes on to say what its authors would like to build. The
user documentation confirms that the Tentacle release shipped optimizations that improve
"performance for smaller I/Os" and eliminate padding. It does not say that every mechanism
in the design document shipped as written, and this page does not either.

**Recalled, not read:** that an acknowledgement waits for every shard of the acting set;
that monitors fence a primary by map epochs; that a placement group below `min_size` blocks
reads as well as writes; that CRUSH keeps positions stable for an erasure coded pool by
choosing independently for each; that RADOS carries a truncate sequence on every operation;
that RGW defers the deletion of a replaced object's tail for two hours (the default of
`rgw_gc_obj_min_wait` was read; what it governs was not); that an OSD is deployed one for
each disk; that RGW, CephFS and RBD store into RADOS pools side by side; and everything about
Crimson and SeaStore beyond their being Ceph's thread-per-core work.

### What Ceph depends on that Shoal lacks, and the reverse

| Ceph leans on | Shoal has instead |
| --- | --- |
| A map authority that appoints a primary and fences the old one | Nothing may: [P5](../distributed/protocol.md#the-contract) forbids the control plane to authorize a writer. The row's group elects its own |
| A local store with transactions and a cheap clone of a range | Files on a filesystem. Hence redo |
| Clients that compute placement and talk to the primary | Clients that reach any node ([D7](../direction/shard-aware-routing.md) is unbuilt) |

| Shoal has | Which makes |
| --- | --- |
| A replicated, ordered, fenced log for every tablet already | The decision one conditional commit, with no peering |
| A retry identity and a table that remembers answers | A retried write the same write |
| A driver pattern with committed progress | Rebuild and scrub resumable by the next leader |
| State derived at apply by every replica | The record of missed writes, without a log on every holder |

### Other stores

All of this is **recalled** and none of it was read for this page. It is here to say where
ideas came from, and nothing on another page rests on it.

- **MinIO** erasure codes every object as it is written, across a fixed set of drives, keeps
  small objects inline in its metadata file, and checksums each block of each part. Its
  objects are replaced whole. It is the nearest thing to
  [the whole-object path](write-path.md#a-whole-object-in-one-commit) and has no path at all
  for a write in place.
- **Haystack, f4 and SeaweedFS** pack many small objects into large files with an index, and
  the last two erasure code a file once it is sealed. That is the "many chunks in one file"
  candidate of [X6](spikes.md#x6-the-device-store-on-ssd), and "replicate now, encode later",
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
| Ceph: `src/osd/ECBackend.cc` and the peering state machine, at the same tag | What an acknowledgement really waits for, and what peering really decides. Not read | Q14, Q16; before M11 |
| [Ceph: CRUSH maps](https://github.com/ceph/ceph/blob/v20.2.0/doc/rados/operations/crush-map.rst), and the CRUSH paper | Classes, failure domains, `straw2`; how positions stay put | Q19; M14 |
| [Ceph: BlueStore configuration](https://github.com/ceph/ceph/blob/v20.2.0/doc/rados/configuration/bluestore-config-ref.rst) | Checksums, the log on a faster device, deferred writes | Q21, Q22, Q23; M14, M19 |
| [Ceph: RGW layout](https://github.com/ceph/ceph/blob/v20.2.0/doc/radosgw/layout.rst) | Head and tail, inline data, the index | Q25, Q32; M12 |
| The S3 API reference: listing, multipart, conditional requests, checksums | What a later gateway would need the metadata to offer. Not read | Q32 |
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
