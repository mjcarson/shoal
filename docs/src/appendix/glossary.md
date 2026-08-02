# Glossary

Terms as Shoal uses them. Several differ from their usual meaning in other databases; those
are called out.

---

**Accessible** — A partition state: raw bytes read from an archive, not yet deserialized.
Reads and filters run directly against the archived form. See
[Partitions](../tables/partitions.md#maybeloaded).

**Archive** — A uuid-named file holding many serialized partitions packed back to back.
Immutable once written; updated partitions are rewritten into the active archive and the old
copy becomes garbage. `<throughput_sensitive.path>/<table>/archives/<uuid>`.

**Archive map** — The per-shard, per-table index from partition key to `ArchiveEntry`
(`{archive uuid, offset, size}`). Also the authority on whether a partition exists on disk at
all. Persisted as a checksummed snapshot plus its own intent log. See
[Archives and the Archive Map](../storage/archives-and-map.md).

**Active archive** — The one archive currently receiving newly compacted partitions. Rotated
rather than compacted in place, to avoid invalidating entries written earlier in the same
compaction pass.

**Blocked query** — A query parked in `blocked: HashMap<u64, Vec<...>>` waiting for a
partition to be read from disk. Re-injected as a fresh `ServerMsg::Query` when the read
completes, rather than being resumed as a suspended future. See
[Query Execution](../tables/query-execution.md).

**check_disk** — A flag on `SortedPartition` meaning "there may be more of this partition in
an archive". Set on creation, cleared once the full archive copy has been merged in.

**Compaction** — Two distinct operations sharing one background task. *Intent compaction*
folds a sealed intent log into archives. *Archive compaction* reclaims space from archives
whose live fraction has dropped below 50%. See [Compaction](../storage/compaction.md).

**Coordinator** — A *role*, not a component. The shard whose TCP listener accepted a client's
connection routes that client's queries to their owning shards. Any shard can be a
coordinator; there is no coordinator process, despite CPU 0 being reserved and called "the
coordinator cpu".

**Ephemeral table** — An in-memory-only table type. Present in the codebase but not usable in
a `#[db]` database.

**Evictable** — A partition eligible for eviction. `Accessible` partitions always are;
`Loaded` partitions only once their generation has been compacted. See
[Memory and Eviction](../tables/memory-and-eviction.md).

**Generation** — A per-table counter incremented on each intent log rotation. Names the epoch
whose writes are now sealed. A `Loaded` partition records the generation it was last modified
in; comparing against the flushed generation is what makes eviction safe.

**Intent** — One logged mutation: `Insert`, `Delete`, or `Update`. The unit of the write-ahead
log. `Insert` carries the whole row; `Update` carries only changed fields, which is why
replaying one requires the base partition.

**Intent log** — Shoal's write-ahead log, one per shard per table. Records are framed
`[size][gxhash checksum][rkyv payload]`. The *active* log receives writes; on rotation it
becomes `-inactive-<generation>` and awaits compaction. Confusingly, the archive map has its
own separate intent log. See [The Intent Log](../storage/intent-log.md).

**Loaded** — A partition state: fully deserialized in memory, tagged with the generation it
was last modified in.

**Loader** — `FsLoader`, the per-shard background task that reads partitions from archives and
posts them back as `ServerMsg::Partition`. Must only ever hold its own shard's channel — see
*unsafe Send invariant*.

**MaybeLoaded** — The enum wrapping every resident partition: either `Loaded` or `Accessible`.
The seam where lazy deserialization lives.

**MaybeRow** — Either a `Row` or a `Tombstone`. Sorted partitions store these rather than rows
directly.

**Partition** — The unit of storage, caching, eviction, and IO, addressed by a `u64` partition
key. A sorted partition holds a `BTreeMap` of rows; an unsorted partition holds exactly one
row.

**Partition key** — A `u64` produced by `gxhash`ing the `#[shoal(partition)]` fields. Because
it is a hash, **partition keys collide**, and nothing detects it.

**Pending response** — A response held in `PendingResponse` against the intent log offset one
past its record, released once the durability watermark passes it. See
[Durability model](../storage/overview.md#durability-model).

**Recovery stats** — `RecoveryStats`, the counts of everything replaying a table's intent logs
had to discard. Three of its four counters mean data was lost; `updates_after_delete` does not,
and exists so the other three can be trusted. Summed across a shard's tables and reported once
when the shard finishes starting. See
[Recovery](../storage/recovery.md#what-recovery-discards).

**Ring** — The tablet map, still named `Ring` in the source. Maps a partition key to the tablet
holding it, and that tablet to the shard that owns it. Built whole from the shard count before
any shard starts. See [Partitioning](../architecture/partitioning.md).

**Tablet** — A slice of the partition key space named by the top 12 bits of the key, and the unit
ownership is recorded for. 4096 of them, assigned to shards round robin. Ownership is *stored*
per tablet rather than derived from a hash, which is what would let a tablet be moved between
shards. See [Partitioning](../architecture/partitioning.md#the-tablet-map).

**Shard** — One glommio executor pinned to one core, owning a slice of every table, its own
intent logs, archives, and background tasks. Named `Shard-N`, where N comes from a startup
counter, not the core id. Shard names become filenames, which is why shard count is part of
the on-disk format.

**SHQL** — Shoal Query Language. A small `SELECT`-only parser: `SELECT * FROM t WHERE f = v
[AND ...] [LIMIT n]`. Equality and `IN` on any field, plus `<`, `<=`, `>`, `>=` on a sort key;
`WHERE` mandatory, no `ORDER BY`. Rows come back partition by partition in the order the query
named them, and in sort-key order within each, so a `LIMIT` takes the first of those. See
[SHQL](../api/shql.md).

**Sort key** — The `#[shoal(sort)]` fields, ordering rows within a sorted partition. It addresses
a row for a delete or an update, decides the order a read returns a partition's rows in, and
**selects** them: a get or an exists naming sort keys seeks those rows and answers about them
alone, and one bounding them by a range seeks the span between the bounds. A query narrowing
itself neither way asks for the whole partition. See **Sort select** below.

**Sort select** — `SortSelect`, the three ways a sorted get or exists can choose rows: `All`,
`Keys([..])`, or `Range(..)`. It is an enum rather than a set of fields so that "these keys *and*
this range" is not a state a query off the wire can arrive in. `All` is the only arm that means
every row; an empty `Keys` list means none. See [F1](../features/sort-key-ranges.md).

**Sort range** — `SortRange`, a pair of `Bound<Sort>`. An exclusive lower bound is a **cursor**:
handed the sort key of the last row of a page it names the next page, which is how a large
partition is paged through without reading all of it.

**Sorted table** — `PersistentSortedTable`. Many rows per partition, ordered by sort key.

**StreamWriter** — The DMA-aware append writer behind the intent log. Hands out buffer slices
via `prep`/`consume` and writes full buffers through detached background tasks.

**Tombstone** — A `MaybeRow::Tombstone` marking a deleted row. Necessary because a delete may
target a row still sitting in an unread archive, so the deletion must be recorded in a form
that survives a later merge. Both table types use it: a sorted partition tombstones one entry
in its `BTreeMap`, an unsorted partition — which has only one row — becomes a tombstone whole.
Removed for real at compaction, which also drops the partition's `ArchiveEntry` when nothing
is left of it.

**Unsorted table** — `PersistentUnsortedTable`. Exactly one row per partition, so a partition
is either fully resident or not resident at all.

**unsafe Send invariant** — `ServerMsg` asserts `Send` by hand even though its `Partition`
variant carries a non-`Send` glommio `ReadResult`. The rule that makes this sound — a
`Partition` message may only be sent on its own shard's channel — is enforced only by
convention. The most dangerous thing in the codebase to change unknowingly. See
[Known Issues](known-issues.md#unsafe-send-invariant).

**Watermark** — An intent log offset below which everything satisfies some property. The
writer tracks two in `FlushState`: `written_pos` (every byte below it has been `write_at`
completed) and `synced_pos` (every byte below it has been `fdatasync`ed). Both are *contiguous
low-water marks*, not maxima — io_uring completions arrive out of order, so a maximum would
cover data still in flight. Pending responses are released against `synced_pos` by default.
See [Intent Log](../storage/intent-log.md#completion-notification).

**Pad region** — Filler written after a partial flush to round it up to a block boundary so it
can be written with O_DIRECT. Starts with `PAD_SENTINEL` (`u64::MAX`) so replay skips it rather
than mistaking it for the end of the log. See
[Intent Log](../storage/intent-log.md#pad-regions).

---

## Terms that mean something unusual here

| Term | Elsewhere | In Shoal |
| --- | --- | --- |
| Coordinator | A distinct node or process | A role any shard plays per connection |
| Intent log | — | The write-ahead log |
| Flushed | Durable on stable storage | Handed to the kernel; no `fdatasync` on the normal path |
| Sorted | Supports ordered scans and range queries | Rows are stored ordered, but no read predicate uses the order |
| Distributed | Multiple nodes | Multiple shards in one process |
| `sync` | Force to stable storage | On `StreamWriter`, issues a background write and returns. `sync_blocking` is the real one — but on glommio's `DmaStreamWriter`, `sync` *does* fsync |
