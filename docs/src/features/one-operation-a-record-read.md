# F120. One operation a record read

An archived record's read is now **one operation, its read**. Every archive a shard's map holds
open is opened once and shared by the map's cache and every read of it in flight (`OpenArchive`, an
`Rc<DmaFile>` beside the archive's format), where every read before duplicated the cached file and
closed the duplicate through the ring after it: three operations a record, each awaited one after
the other. A handle carries its archive's format, so a read through one taken before an archive pass
removed the archive is verified as it would have been then
([Resolved #341](../appendix/resolved/held-handle-reads-verified.md)), and an archive a pass removed
is never kept open again by a read that opened it meanwhile
([Resolved #342](../appendix/resolved/removed-archive-cached-again.md)). This is the first of the two
rows [W42](../future-work/scans/scan-speed.md) added to [W1](../future-work/scans/prerequisites.md#one-operation-a-record-read)
for M23's rate, and the first half of [O15](../appendix/optimizations.md#o15-one-partition-load-costs-a-dup-and-a-close).

## Context

`ArchiveMap::read_record` is the one place a partition's bytes leave an archive: a get of an evicted
partition, a merge's loads, an archive pass's copies, a rehome's copy and a walk's pages all read
through it ([F44](repair.md)). It took a handle from `get_archive`, which answered a cached archive
with `dup()` (glommio's `try_clone`, an `fcntl(F_DUPFD_CLOEXEC)`), read the record, and closed the
duplicate, an operation through the ring that the read awaited. The cache existed so a read would not
open its archive again, and every read paid two calls for a descriptor anyway. O15 filed that in
2026-08 as argued and contained.

W42 measured where it lands (2026-10-29). A scan's page reads a tablet's partitions one after another
at a depth of one, and on X16's side cluster a page of `Big` was 732 of them, so each partition's
three operations were three parks and wakes of its shard in a row. W42 estimated the `dup` and the
close at 2 to 5 µs of the 73 µs a walked partition cost a Zen1 host
([O141](../appendix/optimizations.md#o141-a-walked-partition-costs-a-zen1-host-about-73-microseconds-of-cpu)),
and with W5's reads in flight two of the three would still be paid a record. So it is required for
M23's rate, though nothing built without it is incorrect.

The duplicate was not there by accident. The storage page said why: a caller could close its copy
without disturbing the cached one, and two callers read through handles of their own rather than
through `read_record`: F44's canonical cut, which collects a handle an archive on the loop and reads
through them off it, and the snapshot cut, which reads each run of records through one. The canonical
cut reads archives an archive pass may have removed since it collected them.

## What it does

- **A shared handle.** `OpenArchive` (`shoal-core/src/server/tables/storage/fs/map.rs`) is the
  archive's open file, `Rc<DmaFile>`, and its format, decided once from the file's first bytes.
  `get_archive` answers a cached archive with a clone of the cache's handle, a count and no call, and
  `OpenArchive::release` lets a holder's share go through glommio's `close_rc`: the last holder
  closes the file through the ring, and any other drops its count.
- **A read is its read.** `read_record` takes a handle, reads and verifies, and releases it, and a
  read's own failure is the one it reports: a corrupt record is never hidden behind a failed close,
  which the old order allowed. `read_record_from` and `read_run_from` take an `&OpenArchive` and
  verify by the format it carries. The map's table of formats by archive id is gone, and
  `format_of` with it.
- **A miss is kept open only while the map knows the archive.** An archive not in the cache is
  opened, its header read, and its handle kept for the reads after it only if the map is not closed
  and `all_archives` still names it, both read with nothing awaited before the insert. If another
  read kept one meanwhile, that one stays and this one serves its read alone, its release closing it.
- **A removal takes the archive out before it awaits.** `remove_archive` takes the archive out of
  `all_archives`, then out of the cache, and only then releases the cache's share: closed there if no
  read holds the file, by the last read's release if one does. A close that fails is logged and not
  returned, since the archive pass that removes an archive has repointed its records already and has
  its delete to settle and its file to unlink still.
- **A map closes past a failure.** `close_all` marks the map closed, so nothing is kept open after
  it, releases every cached handle past one that fails, and returns the first failure.
- **The cuts hold shares.** A canonical cut collects an `OpenArchive` an archive
  (`FileSystem::archived_cut_once`, `ArchivedCut`) and releases them at its end; the snapshot cut
  takes one a run and releases it after the run's read.
- **The writer keeps its duplicate.** A `DmaStreamWriter` owns and closes its file, so
  `get_active_writer` still hands a job's writer a duplicate of the active archive, once a job rather
  than once a record.

## Design choices

- **Share the handle rather than borrow it.** A read awaits its read, and a `RefCell` borrow of the
  cache cannot be held across an await
  ([Resolved #111](../appendix/resolved/archive-removal-borrow.md)). A cloned `Rc` outlives the
  cache's entry, so a holder's read never fails because a pass removed the archive under it, which a
  duplicated descriptor never did either.
- **`close_rc`, never a `DmaFile::clone`.** glommio's `DmaFile` clone shares one descriptor too, but
  its `close` is refused `CanNotBeClosed` while another clone is alive. Every read in flight of a
  popular archive would meet that, and so would a pass's removal, after its repoint. An `Rc` of one
  `DmaFile` and `close_rc` is the form glommio provides for one file with several owners.
- **The format travels with the file.** The format is a fact of the open file, decided from its
  header once, so it belongs to the handle and to nothing the map can drop while a handle is held.
- **The cache's rule is read with the insert.** Whichever order a read's open and a removal take, the
  archive ends out of the cache: a removal wholly before the check leaves it out of the set, and one
  after the insert finds it in the cache and takes it out.
- **A removal never fails on its close.** Today's pass returned the error after its repoint and
  before its unlink, which left a file on disk whose delete it may already have logged: [item
  340](../appendix/known-issues.md#340-an-archive-file-the-map-no-longer-knows-is-never-deleted)'s
  kind of file.

## Alternatives rejected

- **Borrow the cached `DmaFile` by reference.** The read's await would hold the cache's borrow, which
  is Resolved #111's panic, or the read would have to take the file out and put it back, and a second
  read of the archive meanwhile would miss and open it again.
- **`DmaFile::clone()` a read and `close()` it after.** One descriptor, but a refused close for every
  read that is not the last, as above.
- **Keep the table of formats and look a read's format up at the read.** A holder outlives the map's
  entry for its archive: item 341.
- **Evict from `loaded_archives` as well (O15's second half).** Not taken: a descriptor an archive for
  the life of the process is unchanged, and still bounded by nothing but the archives a table keeps.
  A read no longer adds one of its own, so a shortage of descriptors is now met only by an open of an
  archive not yet cached. It stays on O15.
- **Wait for a removed archive's reads before unlinking it.** A pass would wait on reads it cannot
  see, and a held share already keeps a removed archive readable until its holder lets go.

## Limitations

- **A read is one operation only for an archive the cache holds.** The first read of an archive since
  a start opens it and reads its header, as before, and so does a read of an archive a pass removed.
- **A descriptor an archive is held until a removal or the map's close**, O15's second half, as
  before.
- **A job's writer still costs a duplicate and its close**, once a job.
- **A read by a stale view of an archive a pass removed and unlinked still fails `ArchiveMissing`**,
  and the loader gives up on it, though the partition only moved. It is filed as item 343 and fixed in
  a change of its own.
- **Not measured yet.** The lab's figures are X17's arm of one operation against three, in the scans
  plan's first lab window.

## Invariants to uphold

- **Every holder of an archive releases its share** with `OpenArchive::release` on its way out. A
  share dropped without it is closed by glommio on its next I/O cycle, and never once its executor
  stops.
- **The cache keeps an archive only while `all_archives` names it and the map is not closed**, both
  read with nothing awaited before the insert, and **`remove_archive` takes the archive out of
  `all_archives` and the cache before its first await**.
- **A record is verified by the format its handle carries**, never by anything the map looks up at
  the read.
- **Never `DmaFile::clone()` an archive's file**: its close is refused while another clone lives.
- **No borrow of the cache is held across an await** (Resolved #111).

## Performance

Not measured yet. W42 estimated the `dup` and the close at 2 to 5 µs of the 73 µs a walked partition
cost a Zen1 host, and at a depth of one each was also a park and a wake of the shard
([O141](../appendix/optimizations.md#o141-a-walked-partition-costs-a-zen1-host-about-73-microseconds-of-cpu)).
X17's reads half measures one operation against three on X16's side cluster, `scan/Big/none` at one
read in flight and at its best depth, and its figures go here.

## Tests

| Test | Proves |
| --- | --- |
| `a_walked_record_borrows_its_archive` (`shoal-core/src/server/tables/storage/fs/tests.rs`) | W42's M23 row, built now: two reads of a cached archive share one descriptor, so a read takes no duplicate and its release no close |
| `a_held_handle_reads_a_removed_archive_verified` (same) | Item 341: a handle taken before a pass removed its archive still verifies its record, and a flipped byte in it is refused |
| `a_removed_archive_is_never_cached_again` (same) | Item 342: a read that opens an archive while its removal waits on its close serves itself and leaves nothing in the cache |
| `removing_an_archive_does_not_hold_the_handle_map_across_the_close` (`.../fs/map.rs`) | Resolved #111's row, kept: a read during a removal's close is served |
| `archive_records_are_checksummed_and_a_flipped_byte_is_refused`, `a_format_1_archive_reads_unverified_and_is_counted` (`.../fs/tests.rs`) | A handle's format: a new archive's is checksummed, an archive from before F44 reads unverified and is counted |

## Related

[O15](../appendix/optimizations.md#o15-one-partition-load-costs-a-dup-and-a-close) and
[O8](../appendix/optimizations.md#o8-partitions-are-read-one-at-a-time-each-with-its-own-dup-and-close),
which named the pair; [W1](../future-work/scans/prerequisites.md#one-operation-a-record-read) and
[W42](../future-work/scans/scan-speed.md), which made it required for M23's rate;
[F44](repair.md) for the canonical cut; [Archives and the Archive
Map](../storage/archives-and-map.md); [Resolved #111](../appendix/resolved/archive-removal-borrow.md),
[#339](../appendix/resolved/archives-known-after-restart.md), whose replay keeps `all_archives`
true after a restart, [#341](../appendix/resolved/held-handle-reads-verified.md) and
[#342](../appendix/resolved/removed-archive-cached-again.md).
