# F87. The walk of one tablet's rows

## Context

Nothing could list a tablet's rows for a caller. A query has to name its partition keys, since a
`WHERE` on the partition key is mandatory ([SHQL](../api/shql.md)), and the engine enumerated a
tablet's keys only inside work that hands nothing back: a scrub's canonical cut hashes them
([F44](repair.md)), a snapshot writes them to a file ([F43](node-recovery.md)), and an export
copies every shard's archives ([F49](backup-and-recovery.md)).

The object store needs to enumerate rows. [M16](../object-storage/milestones.md#m16-recovery)'s
backfill compares a placement group's stripe rows with the inventories of its holders, since the
record of what a slice missed is bounded and a slice away too long, or new, has to be brought
current from what should be there ([S10](../object-storage/recovery.md#backfill)). M17's light
scrub reads the same rows ([S11](../object-storage/scrub.md), ✅ [F89](scrub-and-repair.md)), and M20's reclamation looks among
them for a retired object's stripes (✅ [F92](reclamation-and-device-lifecycle.md) walks the object
entries instead, and names a retired object's stripes from its extent). [S1](../object-storage/prerequisites.md#required) listed it
as the last required prerequisite: *an engine walk of one tablet's rows that a driver can ask for,
by tablet and by key range*, landing no later than the start of M16.

✅ [F88](recovery-and-moves.md) (M16) is its first reader. As built, a backfill of an overflowed
position reads the walk of its placement group's range alone, filtered by consumer, since a put that
misses a holder writes a row; a move's copier reads the walk and the old holders' listings both
([F88](recovery-and-moves.md#backfill)).

It waited on part of [Q17](../object-storage/contract.md#questions-to-answer): what a driver asks
the walk for. The user decided three things on 2026-10-13, before anything was built:

- **The walk is monotone, from a floor.** Each page is read from its copy's state when the page
  begins. Every partition live when the first page begins, the walk's floor, and still live when
  the walk reaches its key is handed out exactly once, never older than the floor. Nothing is
  held between pages, and the cursor is the last key handed out.
- **A table's resident rows are held by tablet**, so a walk reads one tablet's resident
  partitions and not the whole table's, and every other question about one tablet does too.
- **An operator asks through an admin read**, answered by the node it reaches from the copy that
  node holds; on a cluster only `cluster.admins` may ask.

## What it does

### A table's resident partitions are held by tablet

`ResidentPartitions` (`shoal-core/src/server/tables/resident.rs`) replaced each persistent table's
`HashMap<u64, MaybeLoaded<P>>`. It is an index of the 4096 tablets, allocated at the first insert,
into a list of the present tablets' own maps, with the map's API: `get`, `get_mut`, `insert`,
`remove`, `entry`, `len`, `iter`, plus `tablet_map` and `take_tablet`. A tablet is the placement
rule's `tablet_of`, which is now `#[inline]`. A key costs one more load than it did, into an
8 KiB index the hot path keeps in cache.

What asked about one tablet now reads only its map: the scrub's `canonical_cut`, a volatile
group's `snapshot_partitions`, and `evict_tablets`, which takes each tablet's map whole. O82's
shrink runs over each tablet after every eviction: a tablet holding nothing gives its map back,
and one holding under a quarter of its room past 64 buckets is shrunk to twice what it holds.

This landed as a change of its own (`347d17b`) ahead of the walk, measured before and after on
hyperion ([Performance](#performance)).

### A page is captured on the loop and read off it

A page asks for a tablet, a range of its keys, a cursor and bounds: at most `limit` partitions,
1,000 unless set and 10,000 at most, and no more partitions once `max_bytes` is reached, 1 MiB
unless set, one partition always. Three steps make it:

1. **The capture**, on the shard's loop and synchronous. The table takes its tablet's resident
   keys in the range past the cursor, sorts them, and captures each in order until the bounds are
   met by partitions certain to be handed out (`PendingWalk::capture`). A whole partition is
   copied as its archived bytes. A tombstone, or a sorted partition with no live row, is a
   *shadow* that hides its archived copy. A sorted partition a write started before it was read,
   which holds only its new rows (`check_disk`), is an *overlay*. In the same step, with nothing
   awaited, the table's storage takes the archive map's view of the range up to the last captured
   key (`StorageSupport::archived_walk`, `ArchiveMap::scan_range`), so both halves describe one
   moment: the page's boundary.
2. **The read**, on a task in the shard's medium priority queue (`PendingWalk::finish`). It merges
   the captured partitions with the archive's view in key order. A whole or shadowed partition is
   not read from its archive. An overlay is merged over its archived rows, as a read merges it. A
   partition only the archive holds is read through the map. A record whose archive was removed
   under the page has moved: it is looked up again in the map as it is now, and left out if the
   map no longer holds it.
3. **The answer**, back on the loop (`Shard::handle_walked`). The copy is checked again: a page
   whose group began an install or was quarantined while it read is refused, since its reads may
   have met the install's files.

A walked partition is in the snapshot record format: its key and its archived bytes, holding its
live rows alone. `walked_unsorted` and `walked_sorted` read one back as rows, and a bucket's
stripe table as `StripeRow`s (`WalkPage::stripes`, through the generated
`ObjectQueries::stripe_row` that an answer's rows are read by too).

Since [F117](scan-class.md) a scan's page is captured from the loop's second inbox once the loop is
idle, its copy checked again there and its boundary read there, and its reads run in the shard's scan
queue in 64 KiB steps; a driver's page runs as this section says.

### The copy is checked before and after

`Shard::handle_walk` (`shoal-core/src/server/shard/walk.rs`) routes a page to the executor hosting
the tablet's copy on the node: on a cluster the slot the map names for this node, a learner's
included; on a standalone node the executor the hosting deals the tablet to. A page that is not
for any copy here is refused `StaleTopology` by name. The copy is then held to its group:

| Refused | When |
| --- | --- |
| `Unavailable` | No group serves the tablet on this shard yet, its copy is starting or down, it is installing a snapshot, or its applied index is behind the page's `at_least` |
| `Quarantined` | The copy is quarantined |
| `Shedding` | The shard reads two pages already (`PAGES_IN_FLIGHT`); since [F116](scan-admission.md) two of the page's class, a scan's counted apart by `scans.pages_in_flight` |
| `InvalidRequest` | Since F116, a scan's page on a node whose `scans.enabled` is false |
| `InvalidRequest` | The tablet is past twelve bits, the range is not the tablet's, or a bound is zero or past its most |
| `UnknownOperation` | The schema has no such table |

A page reports its boundary, the copy's applied index when it began, and whether the copy led
its group then (`running_state` and the current leader, as Resolved #197 has it). On a
standalone node both are none.

### A walk keeps a cursor and a floor

`TabletWalk` is what a driver walks with: each page asks `after` the last key handed out and
`at_least` the highest boundary any page was read at, and a copy behind that refuses by name with
where it is. A driver commits both as its progress, and a walk resumed from them on another copy,
after a restart or a change of leader, never reads a state older than one it already saw.
`WalkAsk::range` asks for a sub-range of a tablet: a placement group's is
`PlacementGroup::range(groups_per_tablet)`, the keys `pg_of` puts there. A placement group's
range holds every consumer's stripes that fall in it, so a driver for one consumer filters on the
row's consumer. A bucket names its stripe table through `ObjectOps::stripe_table`.

### An operator walks through an admin read

`AdminKind::Walk { table, tablet, low, high, after, limit, bytes, at_least }` is a read named
`walk`, listed in `admin_reads`. The shard that reads it off the client connection answers it, a
standalone node's too, and it never reaches the control thread, which refuses one that does. The
answer, `WalkAnswer`, carries the keys and each partition's live rows, not the rows themselves,
with the boundary, whether the copy led, the cursor, the group, how many partitions came from
memory and from archives and how many records had moved, how long the capture held the loop and
how long the reads took; a bucket's stripe table adds each stripe's consumer, object (as 32 hex
digits, since a JSON number does not hold a `u128`), index and sequence.

On a cluster only a principal `cluster.admins` names may ask, the rule a mutation is held to,
since a walk reads a table whole and a bucket's generated tables are walked like any other. A
standalone node names no admins, so it answers only a connection that is not authenticated,
which on a node requiring none is its operator. `ShoalPool::walk` is the trusted seam a driver in
the process and the fixture's `WALK` verb use.

`tmdb-dataset-loader walk -i <inventory>` walks every tablet of each table on every member,
checks every page, compares the members with each other and, given `--dataset`, each member with
the partition keys of what `load` wrote, a keyword's rows with the movies that carry it. With
`--tables PostersStripeMeta` it checks every stripe row's key against the stripe it names and the
placement group that holds it.

## Design choices

- **Monotone pages, not a snapshot held across them.** A page is one moment, and a walk is a
  sequence of them whose boundaries never go back. What a backfill or a scrub compares a row with,
  a holder's inventory, is a live listing too, and every action either takes on a row is a
  commit conditional on the row's sequence ([S7](../object-storage/write-path.md)), so a row
  newer than the walk's floor is refused or retried, never wrong. Holding nothing lets a driver
  pace itself by idle time, as [X12](../object-storage/recovery-scrub-rates.md) asks, for as long
  as it likes.
- **The capture and the archive's view in one synchronous step.** A partition that is not
  resident has its current state in its archive, since every partition written since its last
  compaction is resident and is evicted only once that generation is flushed. So the two halves
  taken with nothing between them are the copy's state at the boundary, and the loop never waits
  on a page's reads.
- **A moved record is read where it is now.** The compactor is the map's only writer and never
  rewrites a record in place, so `ArchiveMissing` under a page means the record moved, never that
  it was lost. Reading the map as it is now gives a state at or after the boundary, which the
  floor allows.
- **An overlay is merged, never taken whole.** A sorted partition a write started before its
  archive was read holds only its new rows. Taking it as the partition, as the scrub's cut and
  the digest do ([item 241](../appendix/known-issues.md#241-a-sorted-partition-written-before-it-was-read-is-digested-without-its-archived-rows)),
  would hand out a partition without its archived rows.
- **`at_least` on every page.** Without it a walk resumed on a copy that restarted from its
  checkpoint, or a leader that lags, could read a state older than its floor. It is a field of the
  ask, so a copy behind refuses by name rather than a driver needing to wait on its apply.
- **Resident partitions by tablet, with a dense slot index and a list of present maps.** One more
  load on every lookup, against walking every resident partition of a table for each page:
  milliseconds a page at millions of rows ([alternatives](#alternatives-rejected)).
- **Two pages read at once a shard.** A page holds up to its bytes in memory until it is answered;
  more at once is refused `Shedding` and asked again.

## Alternatives rejected

- **A snapshot held for the whole walk**, as the scrub's cut is held until its digest is done. It
  would hold copies of the resident rows and handles to old archives, blocking compaction's space
  reclaim, for as long as a driver paces itself, and a new leader would start over. The user chose
  the floor.
- **A walk as a log entry**, like `Command::scrub`, to fix every replica's boundary at one index.
  Only the driving copy reads the walk, and a commit a page would cost every replica an apply.
- **A client query that returns rows.** It would change the wire, routing and authorization to
  serve a listing nobody has asked for; the admin read serves the lab and operators, and a driver
  is in the process. Since asked for: scans, and listing a bucket's objects, are planned on
  [Scans](../future-work/scans/overview.md), built on this walk rebuilt for load
  ([W5](../future-work/scans/tablet-reader.md)).
- **Filtering the whole resident map each page**, as `canonical_cut` did. Simple, and a pause of
  every resident partition of the table on the loop for each page.
- **A dense array of 4096 maps a table.** 128 to 192 KiB of empty maps a table a shard as soon as
  anything is resident, multiplied by every table of a wide schema.
- **A map of tablet to map.** A second hash on every lookup of every query.
- **Ordered maps (`BTreeMap`) for resident partitions.** A walk would not sort, and every get
  would pay a tree's depth in cache misses.
- **Duplicating every archive's handle at the capture**, as `archived_cut` does. Which archives a
  page needs is known only once the index pages are read, after the capture, and a moved record
  is a state the floor allows anyway.
- **Routing a walk through the control thread.** It has no channel to a shard, and a page answered
  by the shard the connection reached reaches the hosting executor over the mesh.
- **An identity hasher for the per-tablet maps.** Every key of a tablet shares its top twelve
  bits, which is where hashbrown takes its control byte from; recorded under
  [O5](../appendix/optimizations.md#o5-the-hottest-maps-use-siphash).

## Limitations

- **A page's rows are each at or after its boundary, not all at one index.** A record that moved
  under a page is read as the map holds it now, and a sorted overlay merged with such a record can
  hold rows of states between the boundary and the read. A row created after the floor may or may
  not be handed out.
- **Pacing is the driver's.** A walk is as fast as its pages are asked for. The lab's loader asks
  four tablets of a member at once, and about one page in six was shed and asked again. Since
  [F119](scan-pacing.md) a page asked as a scan's has its archived reads paced by their device's idle
  time; a driver's page is still the driver's to pace.
  [F88](recovery-and-moves.md#budgets)'s drivers pace their rebuilds and copies by their devices'
  idle time and ceilings, not their pages, and ask a shed page again.
- **A page's capture serializes its resident partitions on the loop.** On the lab it held the loop
  for at most 4.7 ms, a keyword partition's rows serialized whole; one sorted partition larger
  than a page's bytes is still captured whole, since a page always holds one partition.
- **A walk crosses no tablet.** A driver walks a tablet, or a range inside one, and a walk of a
  table is every tablet in turn.
- **No `shoaladm walk`.** The loader carries it for the lab; a `shoaladm` command of the same name
  would clash with it, since the loader flattens every `shoaladm` command
  ([todos](../appendix/todos.md#what-f87-left-for-later)).
- **No stats counters.** A page's figures are its answer's; a node keeps no count of walks.
- **`insert_ephemeral` leaned slower** after the per-tablet maps, 0.90 and 0.91 times at the
  median over four and eight rounds with the ranges overlapping, which is not a result by the
  lab's rule; a tablet's map starts empty and grows by doubling
  ([O109](../appendix/optimizations.md#o109-a-tablets-resident-map-starts-empty-and-grows-by-doubling)).

## Invariants to uphold

- **The capture and the archive's view are taken with nothing awaited between them.** An await
  there lets an apply, an eviction or a flush describe another moment than the view.
- **A partition that is not resident has its current state in its archive.** The walk leans on
  eviction's rule that a partition goes only once its generation is flushed
  ([Compaction](../storage/compaction.md)); a change that evicted earlier would make a page older
  than its boundary.
- **An archived record is never rewritten in place.** `ArchiveMissing` under a page is a move, and
  the map as it is now is newer; if the compactor ever rewrote a record in place, a page could read
  bytes of no state.
- **A resident partition shadows its archive, and an overlay is merged with it.** A tombstone
  hides; `check_disk` means the archive holds more rows.
- **A page is refused, never answered, from a copy that is not whole**: starting, down,
  installing, quarantined, or one whose install count moved while the page read. A new path that
  replaces a copy's files has to bump `Group::installs`.
- **A key is in the map of its own tablet**, the placement rule's, and a slot names its tablet's
  map or nothing; a removal repoints the map moved into its place in the same step.
- **Nothing on `ResidentPartitions` is traced or profiled.** Every query reaches a partition
  through it.
- **A walk's keys ascend, each once.** The cursor is the last key handed out, and the next page
  starts past it.

## Performance

### Resident partitions by tablet, before and after

`shoal-workload` at `da2475b` and at `347d17b`, on hyperion (Ryzen Embedded V1756B, `performance`
governor, kernel 7.0.0-34), two shards on cpus 2 and 4, the client on cpus 6 and 7, `/opt/shoal`
wiped before every run, locked memory unlimited, four rounds with the first side alternating,
and the three arms that leaned repeated at eight rounds. Medians and ranges over rounds:

| Arm | Before, ops/s | After, ops/s | |
| --- | ---: | ---: | --- |
| `grid/unsorted/r50/1024` | 8,325 [8,105–8,573] | 8,386 [8,240–8,399] | within noise, 1.01× |
| `grid/sorted/r50/1024` | 7,478 [7,461–7,574] | 7,471 [7,406–7,627] | within noise, 1.00× |
| `get_resident` (8 rounds) | 51,619 [45,429–55,206] | 50,373 [46,006–55,116] | within noise, 0.98× |
| `get_archived` | 26,539 [26,123–26,953] | 26,460 [24,657–26,976] | within noise, 1.00× |
| `insert_unsorted` | 18,459 [17,592–19,957] | 19,135 [18,938–21,140] | within noise, 1.04× |
| `get_ephemeral` (8 rounds) | 54,508 [50,755–59,212] | 58,469 [54,523–61,586] | within noise, 1.07× |
| `insert_ephemeral` (8 rounds) | 124,665 [114,888–146,179] | 113,054 [97,436–134,509] | within noise, 0.91× |
| `conf/resources/memory/r50/16Mi` | 8,287 [8,088–8,407] | 8,293 [8,241–8,347] | within noise, 1.00× |

No difference was a result: every pair of ranges overlaps, every latency's too. `get_ephemeral`
leaned 0.95 times at four rounds and 1.07 at eight; `insert_ephemeral` leaned 0.90 and 0.91,
which is recorded rather than dropped (O109).

On the tmdb cluster, the same 100,000 movies loaded into a fresh cluster of each build, `stats`
read each node's tables' indexes at **36.1 MiB before and 28.7 MiB after**, the rows at 229.7 MiB
on both: the maps no longer start at a thousand entries each, and each tablet's grows on its own.

### Walks on the lab

The tmdb cluster at the F87 build, bootstrapped afresh, 100,000 csv rows loaded and
`tmdb-dataset-loader stripes` run, then `tmdb-dataset-loader walk` of every tablet of the three
tables on each member, four tablets at once. Every member held exactly the csv's partitions:
100,000 movies, 29,002 keyword partitions of 346,468 rows each counted against the movies that
carry it, and 100,000 release rows, and every tablet one leader. The whole walk, three tables on
three members, took 30 s. The figures are a page's, by host: europa (Ryzen 9 7945HX, the Optane) and
titan and hyperion (Ryzen Embedded V1756B, the 970 EVO), their governors `powersave` and
`schedutil` as they rest:

| Table, rows in memory | Host | Partitions/s | Loop held p50 / p99 / max | Reads p50 / p99 |
| --- | --- | ---: | --- | --- |
| Movie | europa | 41,902 | 34 / 55 / 150 µs | 3 / 86 µs |
| Movie | titan | 30,943 | 99 / 167 / 792 µs | 28 / 400 µs |
| MovieByKeyword | europa | 10,422 | 14 / 173 / 1,130 µs | 330 / 1,174 µs |
| MovieByKeyword | hyperion | 5,861 | 41 / 511 / 4,665 µs | 1,573 / 5,133 µs |
| MovieRelease | europa | 43,879 | 9 / 16 / 116 µs | 9 / 86 µs |
| MovieRelease | titan | 33,834 | 27 / 55 / 1,308 µs | 39 / 369 µs |

A keyword partition after a load is an overlay, its new rows over its archive, so its page reads
the archive and merges. Every node was then restarted, and the walk taken again from what the
restart left: 79% of each member's partitions came from archives, a page held the loop for 2 to
12 µs at the median, and its reads took 0.7 ms on europa and 3.3 ms on the Zen1 hosts, 32,000
and 13,800 movies a second. Every member matched again.

A walk of titan's copy alone, with titan restarted four seconds into it, met one dropped
connection and two refusals while titan started, carried on from its cursor and floor, and
matched the csv. The bucket's tables walked alike on every member, 12 stripe rows each the key of
the stripe it names and inside its placement group, and 9 object rows. The `reader` principal was
refused by name.

## Tests

| Test | What breaks if F87 is reverted |
| --- | --- |
| `every_key_lives_in_its_tablets_map` (`tables/resident.rs`) | A key is filed under another tablet, or a lookup, a mutation, an entry or a removal looks in the wrong map |
| `an_emptied_tablet_gives_its_map_back` (same) | Taking a tablet forgets to repoint the map moved into its place, and its keys are lost |
| `a_sparse_tablet_map_is_shrunk` (same) | O82's shrink stops applying to a tablet's map |
| `len_and_is_empty_count_partitions_not_maps` (same) | An entry left vacant makes a table look full |
| `index_bytes_counts_the_slots_and_every_map` (same) | `table_index_bytes` undercounts the index |
| `a_range_scan_returns_only_its_range` (`storage/fs/tests.rs`) | A range scan leaks keys outside its range, misses the delta or a run, or hands out a removal |
| `a_range_scan_is_the_map_as_it_stood` (same) | A change after the view shows through it |
| `a_moved_record_is_read_where_it_moved` (same) | A record whose archive was removed fails the page or is lost, or a deleted partition is handed out |
| `tablet_range_matches_tablet_of` (`shared/placement.rs`) | The tablet range and the tablet rule disagree |
| `a_placement_groups_range_holds_exactly_what_pg_of_puts_there` (`pool_placement/tests.rs`) | A placement group's range disagrees with `pg_of`, or a tablet's groups do not tile it |
| `walk_is_a_read_named_walk`, `a_walk_request_round_trips_json` (`protocol/admin.rs`) | A walk is taken for a mutation, a bound left out fails to decode, or a stripe's object id loses bits |
| `every_kind_of_table_walks_in_key_order` (`shoal/tests/tablet_walk.rs`) | A table kind's walk is out of order, misses or repeats a partition, reads back other rows, or a tablet on the other executor is not routed |
| `an_archived_tablet_walks_from_its_archives` (same) | The archived half is not read, before a restart or after |
| `resident_changes_shadow_their_archives` (same) | A tombstone does not hide its archive, an update is read older, or an overlay is taken without its archived rows |
| `pages_are_bounded_and_resume_from_their_cursor` (same) | A page passes its bounds, a partition past its bytes never progresses, or a resumed walk repeats or skips |
| `the_floor_holds_while_writes_run` (same) | A partition live at the floor is missed or repeated, or one is handed out after its delete or older than its acknowledged write |
| `a_standalone_node_answers_a_walk_and_refuses_what_is_not_one` (same) | The admin read is not answered on a standalone node, or a bad ask is not refused by name |
| `a_walk_needs_no_principal_on_a_standalone_node` (same) | An authenticated principal walks a node that names no admins |
| `a_walked_stripe_row_decodes_and_its_placement_group_holds_it` (`shoal/tests/objects.rs`) | A walked stripe row does not read back, or a placement group's range is not its rows |
| `a_tablet_walks_alike_on_its_leader_and_followers` (`cluster_fixture.rs`) | Copies of a tablet walk differently, a tablet has other than one leader, or a restarted copy walks otherwise |
| `a_walk_behind_its_floor_is_refused` (same) | `at_least` is ignored |
| `an_installing_copy_is_refused_a_walk` (same) | A copy installing a snapshot is walked |
| `a_walk_needs_an_admin` (same) | A principal that is not an admin walks a cluster's table |
| `a_page_check_names_every_broken_rule` (`examples/tmdb_dataset/src/walk.rs`) | The lab check passes a walk that breaks a rule |
| `a_partly_read_sorted_partition_is_digested_whole` (`tablet_walk.rs`, ignored) | Item 241's reproduction: fails today, and passes once it is fixed |

## Related

- [S1's row](../object-storage/prerequisites.md#required), which this delivers, and
  [S10's backfill](../object-storage/recovery.md#backfill), its first reader, built by
  [F88](recovery-and-moves.md#backfill) with a move's copier beside it.
- [Q17's record](../object-storage/contract.md#q17-in-part-what-a-driver-asks-the-tablet-walk-for-2026-10-13).
- [F44](repair.md) for the canonical cut this reads beside, [F76](paged-archive-map.md) for the
  paged index a page's view is taken of, [F85](devices-and-pools.md) for placement groups.
- [O82](../appendix/optimizations.md#o82-a-tables-partition-index-kept-the-capacity-of-its-peak)
  for the shrink, now per tablet, and [O5](../appendix/optimizations.md#o5-the-hottest-maps-use-siphash)
  for the hasher.
- [Item 241](../appendix/known-issues.md#241-a-sorted-partition-written-before-it-was-read-is-digested-without-its-archived-rows),
  found on the way.
