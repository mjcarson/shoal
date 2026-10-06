# S6. The device store

## Context

A slice holds stripe chunks, and under [S7](write-path.md)'s preferred direction it does
three things to them: it **stages** an update durably without disturbing the chunk, it
**applies** a staged update once the stripe's row says it committed, and it **reads** a range
of a chunk at a label a reader names. This page is how a slice, a directory on a device's
filesystem, does those three things so that a crash at any instant leaves either the old
bytes or the new ones, verifiably.

It is also where R16 lands. A rotational disk and an SSD run the same code; what differs is
which of its costs dominate, and this page says which choices are made with that in mind.

## What exists today

The only storage engine that persists anything maps a `u64` key to one whole rkyv record
(`StorageSupport`, `shoal-core/src/server/tables/storage.rs:726`).

- **Records are appended and never changed in place.** `write_record` appends
  `[size][gxhash64][payload]` to the active archive
  (`shoal-core/src/server/tables/storage/fs/map.rs:135`); the map is repointed; a compaction
  rewrites what is still live ([Compaction](../storage/compaction.md)).
- **A read is one whole record.** `read_record` reads a record's every byte and verifies its
  checksum (`map.rs:1434`). Nothing reads a range.
- **Table files use direct I/O; the cluster's WAL does not.** Archives and intent logs go
  through glommio's `DmaFile`. The shared WAL is a `BufferedFile`, through the page cache,
  with one `fdatasync` a batch (`shoal-core/src/server/wal/mod.rs:64`).

What the glommio fork offers under that (`glommio/src/io/dma_file.rs`, at `f4643f7`; ~~`873fa44`~~
before F70 moved them):

| Call | Line | Use here |
| --- | --- | --- |
| `write_at`, `read_at`, `read_at_aligned`, `read_many` | 389, 495, 471, 535 | Ranged direct I/O. Shoal calls `read_at` and nothing else |
| `pre_allocate`, `hint_extent_size`, `truncate` | 710, 728, 736 | A chunk written ahead to its full length |
| `deallocate` | 700 | Punching a hole: a truncate inside a stripe |
| `fdatasync`, `rename`, `remove` | 680, 744, 756 | Durability and replacement. A rename and a remove go to the executor's blocking thread |
| `copy_file_range_aligned` | 590 | A copy inside the kernel, "CoW linked" where the filesystem has reflinks. It is dispatched to the blocking pool. [X6](device-store-ssd.md) rejected the clone, so the store does not use it |

Three facts in the same file matter to a device. Alignment is the device's logical block
size, at least 512 bytes (`:224`). A file on tmpfs has direct I/O **disabled**, in
silence (`:226`). And a file on a rotational device, or one without I/O polling, is
served from the ordinary ring and not the polled one (`:229-235`), which is the only place
anything under Shoal asks whether a disk spins. No device in the lab has I/O polling.

What the lab has measured about a sync, on titan's ext4
([F60](../features/shared-wal-flush.md),
[cluster testing](../cluster-testing/performance.md)): six writers appending 16 KiB through
the page cache and syncing, as the WAL does, took 6.1 ms at the median; six overwriting a
file written ahead, with direct I/O, took 2.9 ms. "A sync of a file that has grown commits
ext4's journal for its size." The 970 EVOs there flush their cache on every sync, 3 ms for
one writer, where europa's Optane takes 0.2 ms. [X6](device-store-ssd.md#two-things-about-the-labs-970-evos)
found that the 970 EVO's flush costs 0.9 ms after a rest and 3 ms after a minute of synced
writes, and that the Optane, whose cache writes through, needs no flush at all: a 4 KiB
overwrite and its sync take 11 µs there.

## The design

### What is in a slice's directory

```
<device>/
├── shoal-device.json                # the marker: device id, node, cluster, class, format
└── <slice>/                         # slice-0, slice-1, ...: one executor's
    ├── shoal-slice.json             # the marker: slice id, device id
    ├── shoal.lock
    ├── journal/                     # staged updates to parts of chunks
    │   ├── 000017                   # written ahead to a fixed size, overwritten in place
    │   └── 000018
    ├── free/                        # files written ahead with zeros, a chunk's length each
    │   └── 004211
    └── chunks/
        └── <consumer>/<placement group>/<object id>/
            ├── 000003.2             # stripe 3, position 2: one stripe chunk
            └── 000003.2.<label>     # a whole chunk staged beside it, from a free file
```

A stripe chunk is one file. Its path names the slice, the consumer, the placement group and
the object, so a directory listing is an inventory of a placement group on a slice, and
removing an object's directory is removing its chunks there.

**A whole chunk is written into a file the slice wrote ahead**, taken from `free/`, never into a
new one. A new file's `fdatasync` commits its allocation as well as its data, which on the lab's
970 EVO cost 7.2 ms against 3.2 ms to overwrite the same bytes; from a file written ahead, a file
a chunk reached 0.71 of a shared file's rate at 1 MiB where a new file reached 0.51
([X6](device-store-ssd.md#1-a-whole-chunk)). X6 took its free files from the object's own
directory, so its rename stayed within one directory; a pool in a directory of its own makes the
rename a move between two, both changed in one journal commit on XFS and ext4. How large the
pool is, how it is refilled off the write's path, and how a reclaimed chunk's file returns to it,
are M14's to design.

### A stripe chunk

```
┌───────────────────────────────┬──────────┬──────────┬─────┬──────────┐
│ header                        │ unit 0   │ unit 1   │ ... │ unit n-1 │
│ identity, geometry, label,    │          │          │     │          │
│ length, a checksum for every  │          │          │     │          │
│ unit, its own checksum        │          │          │     │          │
└───────────────────────────────┴──────────┴──────────┴─────┴──────────┘
  whole blocks                    each unit at an aligned offset
```

- **Identity**: consumer, object id, stripe index, position. It is in the header and it is
  mixed into every unit's checksum, so bytes that verify at one place fail at another
  ([P15](contract.md#the-contract)).
- **Label**: the sequence and tag of the write this chunk holds
  ([S18](contract.md#identity-and-progress)).
- **A checksum for every chunk unit**, in the header. The unit is the granule a read
  verifies, so a read of one byte reads and checks one unit.
- **The file is written ahead to its full length** when it is created. An overwrite then
  never grows it, which is the cheap half of the lab's measurement above, and a unit never
  written reads as zeros.

~~Which checksum, and how large a unit, are [Q21](contract.md#questions-to-answer) and
[Q20](contract.md#questions-to-answer).~~ The checksum is **CRC-64/NVME**, chosen by
[X5](checksums.md). It combines, so the identity is mixed in without reading the unit again:
the checksum stored is the CRC of the unit's bytes followed by its identity, made from the CRC
of the bytes and the CRC of the identity. That lets a client's checksum be the one stored. A
chunk's own checksum in the header can likewise be made from its units' at about 78 ns a unit
on titan. How large a unit is stays [Q20](contract.md#questions-to-answer)'s, and the checksum
sets no floor under it: CRC-64/NVME is at its rate from 4 KiB. The identity's exact bytes are
this page's to fix at M14.

### Staging: two cases

| | A whole chunk | Part of a chunk |
| --- | --- | --- |
| When | A put, or a write that covers the chunk | Any smaller write |
| Stage | A file from the slice's pool, written ahead with zeros, overwritten with the chunk under the write's label and `fdatasync`ed | A record in the journal: identity, the label it makes, the label it expects, the units' new bytes, a checksum. `fdatasync`ed with whatever else was staged since the last sync |
| Apply | Rename over the old chunk, sync the directory | Verify the old unit, merge, write unit and header in place, `fdatasync`, then drop the record |
| Discard | Return the file to the pool | Drop the record |
| Bytes written | Once | Twice: the journal, then the chunk |

**The journal is written ahead and overwritten.** It is a few files of fixed size, written
with direct I/O, so that staging is the cheap sync and not the dear one. One `fdatasync`
covers every record staged since the last, as the WAL's batch does. X6 measured why: written
ahead, a journal committed 2.3 times an appending journal's records at one stager on the 970
EVO, and sixteen times at 64 stagers on the Optane; allocated ahead without its zeros it ran no
faster than appending, so the zeros are written when it is made
([X6](device-store-ssd.md#2-the-journal)). A committer never syncs with nothing new to make
durable: on ext4 and XFS that still flushes the device.

**A staged record holds new values and never a patch.** Parity is staged as the new parity
bytes, not as the delta that would turn old parity into new. A record that is applied, then
replayed after a crash that lost the fact of its application, writes the same bytes again
and changes nothing. A patch replayed is parity corrupted under a label that says it is
current, which is one of the schedules that shaped [S7](write-path.md#the-schedules-that-shaped-it).

**The staged copy outlives the apply.** It is dropped only after the in-place write and the
header carrying the new label are `fdatasync`ed. A torn apply leaves a unit that fails its
checksum and a record that can write it again. That holds even once a committed fact excludes
the record: X1's holder dropped one mid-apply on hearing a later write had committed, and a crash
then left a torn chunk nothing could write again ([X1](stripe-model.md#what-the-search-found-and-the-repairs), P9).

**A stage names the label it expects**, when it changes part of a chunk. A holder ~~whose
chunk carries another label~~ that cannot make that label refuses it: a parity update computed
against one state cannot be applied to a different one. It can make a label from its chunk, or
from records it has staged over the chunk, each over the label the one beneath it makes. A holder
that laid a record over whatever its chunk held, as X1's first did, took a stage over a label it
could not make, and the row then called a chunk current that nobody held
([X1](stripe-model.md#what-the-search-found-and-the-repairs), P17). For the same reason, staging the
same write twice is staging it once only while the holder can make its label: a new stage of a
label whose record has lost its base replaces the record. Answered as a repeat, a rebuild's whole
chunk left the dead record in place and was committed current.

**A record stays while a later one stands on it.** A committed change to part of a chunk that
is not applied yet is beneath every later change staged over it, so it is kept until the chunk
reaches it, whatever the row says of its own label. Dropping it because the row had moved past
its base and named another label, as [S10](recovery.md#reclamation) first had it, lost a write
that had been acknowledged ([X1](stripe-model.md#what-the-search-found-and-the-repairs), P17).

**Space is taken at the stage.** A stage that would breach the device's reserve is refused,
before any commit depends on it. Applying needs no new space: in place it needs none, and a
whole chunk took its space when it was staged.

### Reads

A read names a chunk, a range and the label it wants. The holder answers with the bytes only
if its chunk carries that label, counting a staged update the reader's label says has
committed, which it overlays on the chunk. Otherwise it answers with the label it has, and
the reader decides what that means ([S9](read-path.md)).

A cold open costs more than the read it precedes: 0.6 ms on the 970 EVO under XFS for the
directory and inode reads, against 0.3 ms to read a 64 KiB unit once open
([X6](device-store-ssd.md#6-one-unit-at-a-random-offset)). Whether a slice keeps its hot
chunks' files open is M14's to decide.

Every unit a read touches is read whole and verified before a byte of it is returned. A read
and an apply of the same chunk never overlap: overlapping direct reads and writes are not
atomic, so the owner of the chunk's slice, one executor, runs them in turn.

### What a rotational device changes

The code is the same. The costs are not. The right-hand column was a forecast until
[X7](device-store-hdd.md) measured it on the lab's disks on 2026-10-06; the forecast is struck and
kept beside each measurement:

| Operation | On an SSD | On a rotational disk |
| --- | --- | --- |
| Staging to the journal | A sync | ~~A sync at the end of a sequential write: the best case a disk has~~ One revolution, 8.4 ms, with nothing else on the arm; 42 to 251 ms beside applies, since the stage and the apply take turns on one arm. So it goes to an SSD of the node, where it is 0.9 ms (970 EVO) or 37 µs (Optane) |
| Applying in place | A random write | ~~A seek for each chunk. Deferred and batched in offset order, it is the cost that can wait~~ A seek and a revolution for each chunk, about 9 ms. A whole batch in flight is ordered by the block layer and the disk's queue; offset order one at a time bought nothing |
| A sync | 11 µs on the Optane, which needs no flush; 0.9 ms rested and 3 ms loaded on the 970 EVO ([X6](device-store-ssd.md#two-things-about-the-labs-970-evos)) | ~~A cache flush, not yet measured here~~ One revolution, 8.35 ms, with the write cache off. With it on, the WD140EDFZ stalled a read behind a flush for 100 ms and the WD6001FZWX answered in 0.4 ms, before its platter could have the block ([X7](device-store-hdd.md#three-things-about-the-labs-disks)). So a disk runs with its cache off |
| A read during applies | Unaffected | ~~Competes for the one arm~~ Competes for the one arm, and waits up to one batch: a read's p99 rose with the batch, 0.2 to 0.8 s at 32 applies |
| Many small chunks | Fine | ~~A seek to create each, and one to find it~~ A file a chunk from the pool keeps X6's floor of 1 MiB on XFS, and a cold find costs half a read again. But a random read reaches half of the platter's rate only at 4 MiB, so a disk reads whole chunks of 4 MiB or more |

~~Three choices follow, each a question and none decided:~~ Four choices follow, decided by
[X7](device-store-hdd.md#recommendation) and recorded on
[S18](contract.md#q23-what-a-rotational-device-needs-2026-10-06):

- **The disk's write cache is off.** Not one of the three questions this page posed, but the one
  the disks answered first. With the cache on, the lab's 14 TB disks stall a read behind a flush
  for a tenth of a second, and the WD Black acknowledges a sync before its platter could hold the
  block. A rotational device's slices start only when the kernel says its disk writes through,
  or when its configuration says the cache is on deliberately, which is named in a warning.
- **Where the journal lives: on an SSD of the same node**, named in the device's configuration.
  ~~Each slice journals its own stages, in its directory on the device, or on an SSD of the same
  node named in the device's configuration.~~ A rotational device without one is refused.
  - Ceph says of its own log that it "is advantageous only if the WAL device is faster than the
    primary device", and defers small writes on rotational media by default where on an SSD it
    does not (`bluestore_prefer_deferred_size_hdd` is 64 KiB and its SSD twin is zero, in
    `src/common/options/global.yaml.in` at `v20.2.0`). That threshold is for writes into new
    space. An overwrite of written space under one allocation unit is journalled on any device
    (`src/os/bluestore/BlueStore.cc:16340-16394`). A deferred write logs its new bytes and applies
    them in place after the commit, which is this page's journal
    ([X14](ceph-and-s3-sources.md#6-bluestores-deferred-writes-and-checksums)).
  - A journal on another disk is also a second thing that can fail. Losing it leaves stale every
    chunk a committed write had not yet reached. Several devices that share one journal disk lose
    their staged writes together, so under a `device` failure domain they count as one
    ([S5](placement.md#failure-domains)).
  - X7 measured the cost of not doing it: a small write's stage beside applies took 42 to 251 ms
    on the disk and under a millisecond on the SSD.
- **Who owns it: an executor of its own, never shared with an SSD's slice, and one slice a
  disk.**
  - On an executor shared with a disk's slice, an SSD slice's stage or read p99 rose about a
    hundredfold on three legs of four.
  - A disk's operation costs its executor under 1% of a core, so the cost is a core a node sets
    aside for its disks.
  - A second slice on one disk read 40% less ([S13](isolation.md#who-owns-a-slice)).
- **How it is read: whole chunks.** ~~Whole units, read ahead, through `read_many`, which Shoal
  has never called.~~ A random read across the platter reaches half the sequential rate at 4 MiB
  and not before, so a rotational pool's chunk is 4 MiB or more and is read whole. The geometry
  is Q20's.

~~[Q23](contract.md#questions-to-answer) holds all three, and
[X7](spikes.md#x7-the-device-store-on-hdd) cannot run until a disk is fitted.~~ What stays open
is the apply batch's bound (M19), a disk's scrub budget (X12), and whether a rotational pool's
light scrub keeps an index (M17).

### Filesystems

A device on tmpfs is refused, since direct I/O is silently off there. ~~btrfs is recorded by
this book as a poor host for a synced write path
([Storage Overview](../storage/overview.md#limitations)), and it is what europa's devices
are. XFS is the book's guidance and the lab has none. ext4 is what titan and hyperion run.
Which are accepted, warned about or refused is
[Q22](contract.md#questions-to-answer), and it is one reason
[X6](spikes.md#x6-the-device-store-on-ssd) needs an XFS filesystem fitted.~~
[X6](device-store-ssd.md#the-comparison) measured all three on one device and decided
([Q22, in part](contract.md#q22-in-part-the-device-store-on-ssd-2026-10-04)):

- **XFS is preferred.** It commits a rename in 2 KiB and lists a wide placement group fastest.
- **ext4 is accepted.** The store needs no clone, and the journal and the apply run as on XFS.
  It commits a rename at 17 KiB, and lists a placement group of a million one-chunk objects in
  154 s cold against XFS's 52, so a pool of small objects warns on it.
- **btrfs is refused.** Every overwrite there is a new allocation: a 4 KiB write in place cost
  17 ms and 81 bytes a byte, writing the journal ahead bought nothing, and the rule below that
  applying needs no new space cannot hold.
- **On a rotational device, XFS only: ext4 is refused there**
  ([X7](device-store-hdd.md#the-comparison)). On a disk, ext4 commits a rename in two revolutions
  to XFS's one, holds a file a chunk below half a shared file's rate up to 4 MiB on the lab's
  14 TB disks, and did not list a placement group of a million one-chunk objects in half an hour
  where XFS took 87 s.

## Alternatives rejected

**Stripe chunks as records of the table engine.** An archive is appended and compacted, so
every write in place would be rewritten whole later, and a record is read whole, so a seek
would read a chunk to return a byte. The engine is the right shape for rows and the wrong one for
this.

**Undo in place of redo.** Ceph applies at once and keeps what it overwrote: "the rollback
information has the form of a sparse object containing the old values of the overwritten
extents populated using clone_range", which its own page calls "a place-holder
implementation" pending a store that can do it cheaply
(`doc/dev/osd_internals/erasure_coding/ecbackend.rst` at `v20.2.0`). It saves a round and
needs a cheap clone of a range, which a file on ext4 does not have. Redo needs nothing the
filesystem may not offer.

**A clone to apply**, splicing staged blocks into the chunk with `FICLONERANGE`. It would
make a partial write cost one write and not two. ~~It works only where the filesystem shares
blocks, the fork runs it on the blocking pool, and whether a cloned range's `fdatasync` is as
cheap as an overwrite's is unmeasured. It is a candidate of
[X6](spikes.md#x6-the-device-store-on-ssd), not a design.~~ [X6](device-store-ssd.md#3-a-partial-write)
measured it and rejected it. It wrote fewer bytes only at 1 MiB. Its stage is an allocation, its
apply a transaction, and the `fdatasync` after it cost three to eight times an overwrite's; the
whole write took two to three times the journal's on the 970 EVO. After a thousand clones a
chunk had fifteen hundred extents, and a cold read of one of its units took 2.4 to 2.6 times a
fresh chunk's.

**Many chunks in one large file with an index.** Fewer inodes and sequential writes, which
suits a disk and small chunks. It needs an index that survives a crash and a compaction for
what is deleted, and a chunk that changes in place fits a log badly. ~~Also an X6 candidate.~~
[X6](device-store-ssd.md#1-a-whole-chunk) measured its floor, a slot in a shared file written
ahead with no index at all: above 1 MiB a file a chunk from the pool is within 30% of it, and
within 10% from 4 MiB. Below 256 KiB on a device that flushes, a file a chunk costs more than
twice the floor, and that is a bound on the chunk size, not a reason to share files.

**A raw block device.** See [S4](pools-and-devices.md#alternatives-rejected).

**The page cache.** The WAL uses it and the lab measured what its sync costs. Object bytes
through the cache are held twice, counted by nobody, and written when the kernel chooses.

## What it costs

- **A partial write is written twice**, to the journal and then to the chunk. A whole chunk
  is written once.
- **A file and a header a chunk.** At 4 MiB chunks a 16 TiB disk holds four million files.
  [X6](device-store-ssd.md) measured what that costs on the lab's 970 EVO under XFS:
  - a whole chunk from the pool is within 30% of a shared file's rate above 1 MiB;
  - removing one takes 0.2 ms;
  - a light scrub's cold walk, reading every header, takes 37.5 µs a chunk, which is two and a
    half minutes for four million;
  - a cold open, before the first read of a chunk, takes 0.6 ms.
- **A pool of files written ahead**, a chunk's length each, of space that holds no chunk.
- **A sync a batch of stages and a sync a batch of applies.**
- **A unit read whole for a byte.** A larger unit is a cheaper checksum table and a dearer
  small read.

## What it breaks

- "Everything a shard writes is a log or an archive": a slice holds a third kind of file,
  changed in place.
- "The only read is a whole record": a chunk is read by range.
- "`throughput_sensitive` governs the bulk writer"
  ([item 71](../appendix/known-issues.md)): it governs nothing here. A slice's writer is
  configured with its device.

## Invariants to uphold

- A stage is durable before it is reported, holds new values only, and is refused if it
  would breach the reserve.
- A chunk changes in place only by applying a staged write its row has committed, and the
  staged copy is dropped only after that apply is `fdatasync`ed, whatever excludes it meanwhile.
- A committed record is kept while a label the row names stands on it, and a holder counts a
  label as held only if it can make it.
- A header's label and a unit's checksum are written with the bytes they describe, inside
  the same apply.
- No unit that fails its checksum is returned, merged into, or used as a source.
- A read and an apply of one chunk never overlap.
- Applying needs no new space.
- A slice's files are touched by one executor.
- A rotational device's slices start only when its disk writes through, or when its
  configuration says the cache is on, by name ([X7](device-store-hdd.md#recommendation)); they
  stage on an SSD journal and run on XFS.

## Prerequisites

[S1](prerequisites.md#required): known issue 46, and the torn-write, full-disk and
device-loss faults in the fixture, without which most of the invariants above have no test.
[S4](pools-and-devices.md) for what a device and a slice are. ~~A clone call in the fork, only
if X6 picks it ([S1](prerequisites.md#optional)).~~ X6 did not pick it.

## How it would be measured

~~[X6](spikes.md#x6-the-device-store-on-ssd) on the lab's SSDs and an XFS filesystem: what it
costs to create, sync and rename a chunk from 64 KiB to 64 MiB; journal commits a second at
one writer and at six; an overwrite of 4 KiB to 1 MiB by journal and apply against a clone;
removing and listing chunks at a hundred thousand and a million.~~ Measured on SSDs by
[X6](device-store-ssd.md), on the Optane under XFS and on the 970 EVO under XFS, ext4 and
btrfs, with the decisions on [S18](contract.md#q22-in-part-the-device-store-on-ssd-2026-10-04).
M14 takes X6's figures again from the store as built.
~~[X7](spikes.md#x7-the-device-store-on-hdd) repeats what matters on a rotational disk and
adds the two things only a disk shows: what a read costs during applies, and what a scrub's
reads cost a foreground write.~~ Measured on rotational disks by [X7](device-store-hdd.md), on
the lab's three under XFS and ext4, with what a read costs during applies, what a scrub's reads
cost a foreground write, and what the disk's write cache does, and the decisions on
[S18](contract.md#q23-what-a-rotational-device-needs-2026-10-06). M19 takes them again from the
store as built.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `staged_write_survives_a_crash_and_applies_once` | A process killed after a stage finds it on restart; applied twice, the chunk is what applying once made it | M14 |
| `torn_apply_is_written_again_from_the_journal` | A unit torn by an injected fault fails its checksum, is rewritten from the staged record, and is never returned torn | M14 |
| `apply_needs_no_space` | A device filled after a stage still applies it; a stage past the reserve is refused before any commit | M14 |
| `unit_checksum_binds_bytes_to_their_place` | A unit copied to another chunk or another offset fails verification | M14 |
| `read_and_apply_never_overlap` | A read racing an apply returns the old unit or the new one, verified, never a mix | M14 |
| `rotational_device_with_its_cache_on_is_refused` | A device the kernel calls rotational whose disk writes back is not started, unless its configuration says the cache is on, and then it is named in a warning | M19 |
| `rotational_device_needs_a_journal_device` | A rotational device with no SSD journal named, or one on a filesystem other than XFS, is refused at start by name | M19 |
| `rotational_stage_is_acknowledged_from_its_journal_device` | A stage for a rotational device's chunk is synced on its journal device and acknowledged before the disk is touched; a crash before the apply finds it there | M19 |

## Related

[S7](write-path.md) for what staging and applying are for; [S8](erasure-coding.md) for what
a parity chunk holds; [S11](scrub.md) for reading every unit on purpose;
[S13](isolation.md) for the executor that owns a slice;
[Storage Overview](../storage/overview.md) for the engine this is not;
[F60](../features/shared-wal-flush.md) for what the lab learnt about a sync.
