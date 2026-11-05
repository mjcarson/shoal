# 241. A sorted partition written before it was read was digested without its archived rows

## Symptom

A scrub of a persistent sorted table could judge sound copies divergent. Two copies holding the same
rows hashed differently whenever one held a sorted partition that a write had started before its
archive was read, and the other had merged that partition or evicted it. A `Repair` in verify mode,
or a scheduled scrub (`scrub_interval`, off by default), could then quarantine a sound copy. A
table's digest (`ReplicationVerb::Digest`), which the fixture compares copies by and an import is
judged by ([F49](../../features/backup-and-recovery.md)), left the same rows out. On the lab the
tmdb cluster's `MovieByKeyword` partitions are in that state after a load: F87's walk found 29,002
of them resident on every member, each merged over its archive.

## Cause

A write to a sorted partition that is not resident starts a partition in memory holding only its
new rows, with `check_disk` set, since its archived rows have not been read (`insert`'s vacant arm,
`SortedPartition::new`, `shoal-core/src/server/tables/persistent/sorted.rs`). A get reads the
archive and merges it in before it answers, and so does F87's walk (`walk_fold`). Two readers of a
whole partition did not:

- `PersistentSortedTable::digest` took a `Loaded` partition as the whole of it and hashed its rows
  alone.
- `PersistentSortedTable::canonical_cut`, the loop's half of every scrub
  ([F44](../../features/repair.md)), hashed a resident partition's rows and put its key in
  `resident_keys`, which `archived_cut` leaves out. So the cut never even collected the key's
  archived records.

A partition that is merged, evicted or archived whole hashes all its rows, and one in this state
hashed only its new rows. The rule that a resident sorted partition may be an overlay over its
archive was kept by the read and the walk, and not by the two hashes.

## Evidence

**Established by reproduction, both halves.** The digest's half was reproduced when F87 found the
item: `a_partly_read_sorted_partition_is_digested_whole` (`shoal/tests/tablet_walk.rs`) was ignored
until now. The cut's half had been established only by reading the source, since no fixture schema
has a persistent sorted table. It was reproduced before the fix through a seam this change adds,
`ReplicationVerb::Cut`, which takes a canonical cut of some tablets on a standalone node's shard and
reads it to its report as a scrub's apply does.

Both tests write sixty groups of four entries, restart the node so every group is archived and
evicted, and add one row to each of twenty groups. That leaves 260 rows, twenty partitions of which
are overlays over their archives. Against the engine before the fix, both counted the overlays' rows
alone:

```text
---- a_partly_read_sorted_partition_is_cut_whole stdout ----
assertion `left == right` failed: the cut left out the archived rows of partly read partitions
  left: 180
 right: 260

---- a_partly_read_sorted_partition_is_digested_whole stdout ----
assertion `left == right` failed: the digest left out the archived rows of partly read partitions
  left: 180
 right: 260
```

With the row count's assertion set aside, the same run shows the false difference a scrub would
have judged. A get of the twenty groups merges each over its archive, and each of the node's two
shards then cut the same rows to another digest:

```text
assertion `left == right` failed: the merged partitions cut apart from their overlays
  left: [(143, 3474838338593331353), (117, 2943832336261267997)]
 right: [(99, 15467895462795946447), (81, 12397723874067020648)]

assertion `left == right` failed: the merged partitions digest apart from their overlays
  left: (260, 4157018178486388184)
 right: (180, 4775701030934389441)
```

## The fix

Both hashes fold an overlay over its archived rows by the rule a read merges them by, so every
layout of the same rows hashes the same.

- **A probe from memory alone.** `StorageSupport::may_hold_archived` says whether an engine may
  hold an archived copy of a partition, reading nothing. `FileSystem` asks its archive map's
  `probe`, which a load asks before every read: only the delta's removal, or every run's filter,
  rules a key out. `NoStorage` says never, so an ephemeral sorted table's partitions, which every
  insert starts unread, are hashed as they were. The trait's default is `true`, so an engine that
  does not answer costs a cut a copy and never a wrong digest.
- **The digest.** An overlay the probe cannot rule out has its archived record read
  (`load_partition_direct`, as the digest already reads every partition that is not resident). Each
  archived row it holds no row or tombstone of is counted with it. The digest is a sum of its rows'
  hashes, so the rows need no merge and the resident partition no copy.
- **The canonical cut.** An overlay the probe cannot rule out is serialized on the loop into
  `PendingDigest::overlays`, as a walk's capture takes one, and its key is left to the archived
  half. That half collects its records with the rest.
  - Off the loop, `PendingDigest::finish` hands each record to the table's hasher with its overlay,
    if the cut took one. Each overlay still left is then hashed alone. That covers a key the probe
    could not rule out but the archive did not hold, and a key whose record failed its checksum,
    which is still counted and still makes the report `Invalid`.
  - The hasher is now `CutHasher`, `fn(key, archived, overlay)`, the shape of a walk's fold. The
    sorted table's `hash_cut` merges an overlay over its archived partition with `merge_from_disk`
    and ends in `hash_rows`, as every other path does. The unsorted table's takes the same
    signature; its cut takes no overlay, since an unsorted partition is its one row.
- **What it costs.** Nothing on a query's path changed, so no lab A/B was taken.
  - A scrub's pause on the loop now does one serialization per overlay, where it re-serialized and
    hashed each row before, plus one in-memory probe per partition in this state.
  - Its task deserializes and merges what it read.
  - The cut holds the overlays' copies until its task has read them, which bounds the extra memory
    by the overlays of the group's tablets.
  - The digest reads one more record per overlay, on the loop, as it reads every partition that is
    not resident. It is a fixture's and an import's verb.
- `applied a scrub` logs the cut's `overlays` beside its `resident` and `archived` counts.

`ReplicationVerb::Cut` answers each shard's report with its `resident`, `overlays` and `archived`
counts. It is reached only through `ShoalPool::replication_verb`, the in-process seam the fixture
drives; no client and no wire carries it.

## Alternatives rejected

- **Read an overlay's archive on the loop during the cut.** A cut reads nothing from disk on the
  loop. The lab's 29,002 overlays would each have been a record read while every write waited.
- **Make each overlay whole before the cut**, as a get would. A scrub would then change what is
  resident, charge the shard's memory for rows nobody asked for and evict others for them. Nothing a
  cut or a scan reads becomes resident ([F127](../../features/folds-in-steps.md)).
- **Hash the overlay and its archive apart and combine the hashes.** A partition is hashed as its
  rows in sort order, where an overlay's rows interleave with its archive's and its tombstones hide
  archived rows. A combination would also be a new digest format, which two builds of one table
  would disagree on.
- **Copy every `check_disk` partition, with no probe.** Every partition an insert starts is in that
  state until it is read. A table being loaded would have had its whole resident set copied for
  each scrub, and an ephemeral table every partition, with nothing to fold any of them over.
- **Fold through the walk's `walk_fold` and hash its result.** It serializes the merged partition
  to hand it out, so a hash of it would deserialize it again. `hash_cut` merges once and hashes
  the rows it holds.
- **Add a persistent sorted table to the fixture's `ClusterDb`** to hold the scrub across three
  copies. Every fixture row would gain its groups and its share of each start, and the schema's
  lineage builds ([F93](../../features/operations-and-the-real-cluster.md)) would move with it. The
  standalone cut through `ReplicationVerb::Cut` compares the same rows in each layout on one copy,
  which is what a scrub compares across copies.

## Invariants to uphold

- **`may_hold_archived` is never false for a partition an engine holds a record of.** It may be
  true for one it does not, which costs a copy. An engine that cannot tell says true.
- **A sorted partition with `check_disk` is an overlay over whatever its archive holds**, and every
  reader of a whole partition folds it over its archived rows: a read, a walk, the digest and the
  cut.
- **Every hash of a partition ends in `hash_rows`** (`hash_cut` included), so a partition laid out
  correctly hashes as it always did, and copies on builds before and after this change agree on
  every partition that is not an overlay.
- **An overlay is folded by `merge_from_disk`**, the rule a read merges by: its rows and its
  tombstones are the newer wherever both hold a key.
- **An overlay's key is never in the cut's `resident_keys`**, or the archived half collects no
  record for it.
- **Nothing a cut reads becomes resident.**

## Still open

- **The fixture still has no persistent sorted table**, so no fixture row scrubs one across copies;
  the standalone `Cut` rows hold the cut's digest to every layout instead.
- **A copy on a build before this change still hashes an overlay alone.** While a rolling upgrade
  has copies on both builds, a scrub can still find a false difference on a group whose old-build
  copy holds an overlay.

## Tests

| Test | Where | Breaks if |
| --- | --- | --- |
| `a_partly_read_sorted_partition_is_digested_whole` | `shoal/tests/tablet_walk.rs` | The digest takes an overlay as its whole partition, or hashes the same rows apart held as overlays, merged by a read, or archived after a restart |
| `a_partly_read_sorted_partition_is_cut_whole` | same | The canonical cut takes an overlay as its whole partition, or folds none, or cuts the same rows apart in any of the three layouts |
| `an_overlay_without_an_archived_record_is_hashed_alone` | `shoal-core/src/server/replication/digest.rs` | An overlay whose archive held no record, or whose record was skipped, is left out of the report |

## Related

[F87](../../features/tablet-walk.md), whose walk folds an overlay and found this;
[F44](../../features/repair.md) for the scrub, its cut and the judge;
[F49](../../features/backup-and-recovery.md) for the digest an import is judged by;
[F61](../../features/fragmented-partitions.md) for the chains a cut folds before an overlay goes over
them; [F120](../../features/one-operation-a-record-read.md) for the handles a cut reads through;
[Resolved #160](unreadable-partition-stalls-one-copy.md) for the judge's other false verdict.
