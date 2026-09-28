# F61. A large sorted partition written as fragments

A segment merge used to rewrite every partition it touched whole. A sorted partition that grows by a
few rows a segment was read, deserialized, and written back in full every few seconds. On the lab
that was about sixty archive bytes for every keyword byte inserted
([O79](../appendix/optimizations.md#o79-a-merge-rewrites-every-partition-it-touches-whole)). Now a
merge writes only the rows a large partition gained or lost, as a *fragment* chained after its base
record. Every reader of a partition sees the chain folded back into one partition.

## Context

Round 14 of the [cluster testing](../cluster-testing/performance.md#o64-in-round-14-the-journal-not-the-flush)
found what paces a write-only load on the Zen1 hosts. It was the compactor's archive writes, not
the WAL's syncs. Titan wrote 114 MB/s to its archives, 68 MB/s of it the `MovieByKeyword` table,
while it applied about 15 MiB/s of rows. A keyword partition is a sorted list of every title
carrying the keyword, so a popular one holds thousands of rows. A merge rewrote it whole each time
a segment held one more title for it.

Larger segments spread the rewrites out: 40 MiB halved the archive writes and made loads 24% faster.
They also made the update p99 about 20% worse, so `segment_bytes` stayed an inventory key and not a
default.
The [todo](../appendix/todos.md#a-large-sorted-partition-written-as-fragments) that filed this
design said every consumer of a map entry would have to learn the chain. That was right. It also
called this the storage format's largest change since checksummed records, which is why it waited
until a round that did not have to keep the lab's data.

## What it does

**A map entry can name a chain.** A partition's record in `to_archive` is its *base*, a whole
partition as before. The fragments merged over it since it was last written whole sit in a side
map, `ArchiveMap::fragments`, oldest first. Only partitions that have fragments appear there. One
intent carries a chain, `MapIntent::Chain(ChainEntry { base, fragments })`, and it always carries
the whole chain. `Entry` replaces the base and ends the chain. `Remove` drops both. The serialized
map keeps the side map beside `to_archive`.

**A fragment is a `SortedPartition` of the delta.** It holds the rows the batch wrote and a
tombstone for each row it deleted. `IntentReadSupport::fragment` builds it from the intents alone,
without reading the partition, and `IntentReadSupport::fold` lays it over a base. Rows replace, a
tombstone removes, and no tombstone survives a fold. So a folded partition is exactly what an
archive held before this change, and `merge_from_disk`'s rule that archives hold no tombstones
still holds for every whole partition. Only fragments carry tombstones on disk.

**A merge decides per partition** (`FileSystemCompactor::split_fragments`, at the top of
`load_partitions_for_intents`). A partition is written as a fragment when all of these hold:

- its base is at least `throughput_sensitive.fragment_min_bytes` (4 KiB);
- its chain is shorter than `fragment_max_chain` (16);
- its fragments so far are under half its base;
- its batch holds only inserts and deletes.

An update needs the row it changes, so a batch with one goes to the ordinary merge. So does every
other partition. The merge reads the base and the chain, folds them, applies the batch and writes
one whole record, which ends the chain. `fragment_max_chain: 0` writes every partition whole, as
before.

**Every reader gets one partition.** `ArchiveMap::read_partition` and `read_chain` read the base
and each fragment, verifying every record against its checksum. A chain is folded by the map's
`FoldFn`. The table's storage sets it when it opens the map (`compactor::fold_chain::<P, R>`),
since the map holds no partition type. The folded bytes come back as `PartitionBytes::Folded`
beside `PartitionBytes::Record`, and both dereference to one whole partition's archived bytes.
Each reader handles a chain as follows:

| Reader | What it does with a chain |
| --- | --- |
| A get's load (`fs/loader.rs`) | Folds it. The table holds the folded partition `Loaded` at generation zero, check_disk off, rather than reading it in place |
| A standalone start's replay (`load_scanned`) | The same |
| A merge | Deserializes the base and folds the fragments typed, then applies its batch |
| A snapshot cut | Reads unchained records a run at a time (O78) and sends each chained partition folded, so the snapshot file and its install know no chains |
| A scrub's canonical cut (`ArchivedCut`) | Folds through the cut's own handles. The digest is over live rows, so a copy that wrote a chain and one that wrote it whole digest the same |
| A backup export | Folds each chain |
| The archive pass | An archive holding any record of a chain folds that chain and writes it whole. `entries_of` leaves chained bases out, since copying a base alone would end its chain |
| A rehome | Copies every record of a chain in order and sets the chain on the destination |
| An import digest, the fault injection, the install's absent-key sweep, a tablet drop | See whole partitions, or keys, as before |

**The figures.** `TabletUsage` counts a fragment's bytes on its tablet and counts a chain once in
`chained`. The group report carries `chained`, `TableStats` sums it, and `shoalctl cluster stats`
shows a table's chained partitions over every copy. The compactor's long-job report counts
`fragments` beside `written`. The inventory's `replication:` block names
`fragment_min_bytes` and `fragment_max_chain`, rendered into the archives' storage section.

## Design choices

**The whole chain in every intent.** A fragment's intent names the base and every fragment, not
just the one appended. Map intent logs are replayed over a saved map, and a fold whose log deletion
a crash stopped replays the same log again. An intent that appended would put fragment one back
after fragment two, and an older row over a newer one. A chain is at most sixteen entries of forty
bytes, so the whole chain costs little.

**A side map, not a wider entry.** `ArchiveEntry` stays forty bytes and `Copy`, and `to_archive`
stays the index every reader already walks. A shard holds millions of entries and only its large
sorted partitions ever chain, so the side map is small. Every path that sets a whole record, through
`set_partition`, drops the chain, so no writer can leave a stale chain under a fresh base.

**Fold to bytes at the map.** The readers that hold no partition type (a get's load, a cut, a scrub,
an export) would each have needed the partition type threaded to them. One function pointer set
where the type is known keeps them unchanged. The cost is a deserialize and a serialize per chained
read, which only large partitions pay.

**Consolidate in the archive pass.** A pass that finds a chain's record in an archive it is emptying
folds the chain and writes it whole, instead of copying each record. That is one record for several,
and it keeps a chain from spreading over archives that are otherwise dead.

**Updates merge whole.** A fragment could carry an update's new row only after reading the base,
which is the cost this removes. It could carry the update itself only if a read replayed it, which
would put apply logic on the read path. The keyword table is written by inserts and deletes.

## Alternatives rejected

- **A linked chain inside the records**, each fragment naming the record before it. The map would
  be unchanged, but the archive pass moves records, and moving one would have to rewrite every
  record that names it.
- **Wider segments**, which round 14 measured: they halve the rewrites at a 20% worse update p99,
  and every partition still gets rewritten whole, just less often.
- **Fragments for unsorted tables.** An unsorted partition is one row, so its whole record already
  is its delta. The trait's default refuses, and an unsorted table fails a chained read loudly as a
  map that is not its own.
- **Sending chains in snapshots.** The snapshot format and its install would have had to learn
  chains, and a receiver's chain would be a sender's accident of timing. A cut folds.

## Limitations

- A chained partition is read by folding it. That costs a deserialize of the base and each
  fragment, and a serialize, and it holds the partition `Loaded` rather than read in place. A get
  of one row in a large chained partition pays for the whole partition, as it did before F44's
  read-in-place.
- A chained read makes one read per record: the base and up to `fragment_max_chain` fragments.
  The merge's and the pass's reads of a chain are the same.
- A batch with one update rewrites its partition whole, however large.
- The half-the-base rule consolidates a small base early. A partition just over
  `fragment_min_bytes` gets one or two fragments before it is written whole again.
- Map files and intent logs from before F61 do not load: the serialized map gained a field. No
  compatibility was kept (round 15 of the cluster testing destroyed its data first).

## Invariants to uphold

- **Every path that writes a whole record goes through `set_partition` or an `Entry` intent**, and
  both end the chain. A path that wrote a base and kept the old fragments would fold stale rows
  over new ones.
- **A `Chain` intent names the whole chain, oldest first**, and replaying one is idempotent. Never
  add an intent that appends.
- **A fold leaves no tombstones.** `merge_from_disk` takes a partition read from disk as the base
  and trusts it to hold none.
- **`entries_of` never returns a chained base.** The archive pass copies what it returns as single
  records.
- **Every reader that wants a partition goes through `read_partition` / `read_chain`, or folds the
  chain itself.** A new reader of `to_archive` that reads only the base loses the fragments'
  rows, silently.
- **The map's `FoldFn` is set wherever a typed table opens its map**: `FileSystem::new`,
  `fold_intents` and `export_archives`. The rehome opens maps untyped and never folds.
- **A fragment is only ever written for a partition whose base the map names.** `split_fragments`
  checks it, and the write refuses a partition that lost its entry since.

## Performance

Measured on the lab in round 15 ([cluster testing](../cluster-testing/performance.md#o79-in-round-15-fragments)),
fresh clusters of the TMDB dataset, titan traced by table during a whole load:

| Arm | Keyword archive writes | All archive writes | Loads, rows a second | Mixed bench update p99 |
| --- | --- | --- | --- | --- |
| Every partition whole (`fragment_max_chain: 0`) | 55–80 MB/s | 98–136 MB/s | 42,100–56,200 | 106–124 ms |
| 16 KiB, chains of 8 | 29–40 MB/s | 81–101 MB/s | 47,000–53,200 | 108–118 ms |
| 4 KiB, chains of 16, the defaults | 15–19 MB/s | 55–66 MB/s | 41,400–51,300 | 110–119 ms |

The keyword table's archive writes fall by four fifths at the defaults, and a node's by half. The
loads and the mixed bench do not move: a load's rate is set at its bootstrap by something the
archives' bytes are not
([O64](../appendix/optimizations.md#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab)).
Cold keyword reads after every node was restarted, 14,000 of the table's 58,000 partitions chained,
ran 245,700 and 245,500 a second at a p99 of 31 and 32 ms, against 224,800 and 248,200 at 35 and
27 ms with no chains. Only the worst read was slower, 347–369 ms against 255–269, which is the
first fold of the largest chains.

## Tests

| Test | What breaks if F61 is reverted or regresses |
| --- | --- |
| `partitions::tests::a_folded_chain_holds_what_a_whole_merge_does` | 199 seeds of eight batches of inserts and deletes: a chain folded after archiving each fragment must hold exactly what applying every batch whole does, with no tombstone and the live rows' size |
| `partitions::tests::a_batch_with_an_update_is_not_a_fragment` | A batch holding an update is handed back whole, in order |
| `partitions::tests::a_fragment_deletes_a_base_row_by_tombstone` | A delete survives archiving as a tombstone and removes the base's row in the fold |
| `map::tests::a_chain_replayed_twice_is_the_same_chain` | A log replayed twice lands on the same chain; `Entry` and `Remove` end a chain |
| `map::tests::a_chain_past_its_archive_keeps_the_chain_before` | A chain whose newest fragment never reached its archive is skipped, as a torn `Entry` is (#159) |
| `map::tests::a_chain_is_counted_gathered_and_saved` | Fragment bytes and chains on `TabletUsage` match a pass; `entries_of` leaves a chained base out; `chained_in` finds a chain by any record; a save and an open keep it; a whole record ends it |
| `persistent_sorted_table::a_partition_written_as_fragments_reads_back_whole` | Eight sessions of inserts, deletes and an update through a server, each compacted into fragments or a consolidation by the next start, read back off disk against a model |
| `deploy::inventory` and `deploy::render` tests | The inventory keys validate and render beside the archives' path |

## Related

- [O79](../appendix/optimizations.md#o79-a-merge-rewrites-every-partition-it-touches-whole), the
  amplification this removes, and [O64](../appendix/optimizations.md#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab),
  what it paced.
- [Compaction](../storage/compaction.md), the merge and the archive pass.
- [F44](repair.md), checksummed records and the canonical digest.
- [F60](shared-wal-flush.md), the WAL change round 14 withdrew when it found this.
