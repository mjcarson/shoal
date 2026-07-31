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

**Ring** — The consistent hash ring mapping partition key to shard. Each shard lays down 1000
virtual nodes at a fixed stride from its name hash. See
[Partitioning](../architecture/partitioning.md).

**Shard** — One glommio executor pinned to one core, owning a slice of every table, its own
intent logs, archives, and background tasks. Named `Shard-N`, where N comes from a startup
counter, not the core id. Shard names become filenames, which is why shard count is part of
the on-disk format.

**SHQL** — Shoal Query Language. A small `SELECT`-only parser: `SELECT * FROM t WHERE f = v
[AND ...] [LIMIT n]`. Equality only, `AND` only, `WHERE` mandatory, `LIMIT` ignored by the
server. See [SHQL](../api/shql.md).

**Sort key** — The `#[shoal(sort)]` fields, ordering rows within a sorted partition. Used by
deletes and updates; **ignored by reads** — there is no point lookup or range scan by sort
key.

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
