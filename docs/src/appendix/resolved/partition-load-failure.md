# 16, 51. A partition read that failed panicked its shard and stranded its queries

*Partial fixes of [item 16](../known-issues.md#16-panics-on-the-hot-path) and
[item 51](../known-issues.md#51-a-partition-load-that-fails-inside-load_partition-still-never-releases-its-queries).
Item 16's other twelve panic sites and item 51's early exits inside `load_partition` are still
open; both entries say which half is which.*

## Symptom

A benchmark capture died in the middle of its macro layer:

```
=== [3/5] macro benchmarks, 23 workload(s) x 5 runs ===

thread 'unnamed-2' (488458) panicked at shoal-core/src/server/tables/storage/fs/loader.rs:130:25:
not yet implemented: Add back onto loader channel
```

`unnamed-2` is a shard's glommio thread. The panic took the whole shard with it, and the process
was left with a server that could no longer answer for any partition that shard owned.

## Cause

`FsLoader::start` handled a `LoaderMsg::Request` by calling `spawn_task`, and the error arm of
that call had never been written:

```rust
if let Err(_) = self.spawn_task(table_name, partition_id).await {
    // add this back onto our loader channel
    todo!("Add back onto loader channel");
}
```

Three things could put it there, and they wanted opposite handling — which is why the arm stayed
unwritten. `FilteredFullArchiveMap::get_archive` could report `PartitionNotFound`, which is not
an error at all but the time-of-check-to-time-of-use race `FileSystem::load_partition` allows on
purpose: the compactor pruned the entry between the caller's `find_partition` and the read.
It could report `TableMapMissing`, which is structural and permanent. Or `dma_open` could fail,
which might not hold next time. Requeueing, which is what the `todo!()` names, is right for the
third and a spin for the other two.

Underneath that sat the reason a `continue` would not have done either. **A load completing is
the only thing that drains a table's `blocked` map** (`.../persistent/sorted.rs:362-367`,
`.../persistent/unsorted.rs:310-314`). Every query parked on a partition is waiting for a
`ServerMsg::Partition` that only a successful read sends. A read that fails and says nothing
leaves those queries parked for the life of the process, and their clients waiting, because
there is no timeout anywhere ([item 15](../known-issues.md#15-no-backpressure-anywhere)).

So the loader had two ways to fail and neither was survivable: panic the shard, or strand the
queries silently. The `todo!()` was the first.

The two `panic!`s three lines below it (`loader.rs:140`, `:158`) were the same defect reached
through the read rather than the spawn, and could not have been fixed separately: `read_partition`
returned `Result<AsyncSender<_>, ServerError>`, so on the error path it had already lost the table
and partition id, and could not have named the queries it was stranding even if it wanted to.

## Evidence

**Established by reading the source, then reproduced deterministically.**

The wild failure is rare. A full macro layer — 23 workloads × 5 runs, on the unfixed tree with the
discarded error turned into a `panic!` that printed it — completed clean, as did twelve consecutive
runs of `macro/fanout/evicted/256` on its own. The only trace of the original was the storage
directory the capture left behind, `/opt/shoal/macro-fanout-evicted-256`: the runner wipes storage
before every run, so the one directory still there names the workload that died. **Which of the
three errors fires in the wild is therefore not established** — the fix handles all three, and the
one reproduced below is the `dma_open` failure.

Reproducing it on demand needs no fault injection hook, because archives live in their own
directory apart from the intent logs and the archive map. Taking the permissions off that
directory fails the open of an archive without touching the map that says which archive a
partition is in — the shape of a real IO failure. Against the unfixed tree,
`a_get_whose_partition_cannot_be_read_does_not_hang` produces the reported panic exactly:

```
thread 'unnamed-3' (518473) panicked at shoal-core/src/server/tables/storage/fs/loader.rs:130:25:
not yet implemented: Add back onto loader channel

thread 'a_get_whose_partition_cannot_be_read_does_not_hang' (518464) panicked at
shoal/tests/persistent_sorted_table.rs:2484:5:
a get whose partition could not be read never came back
```

Both halves of the defect in one run: the panic, and then the get that never came back.

## The fix

**The read moved into the spawned task.** `spawn_task` now clones the one `ArchiveMap` it needs
out of the filtered map and hands it to the task; finding the partition, opening the archive and
reading it all happen there. A slow or failing archive delays its own partition instead of every
request queued behind it, and — the point — the task is somewhere that knows which partition it
is reading, so it can report its own failure.

**Failures are classified by whether reading again could answer differently.** `classify` is a
free function over `&ServerError`, deliberately not generic over the database so it can be tested
without standing up a schema:

| Error | Class | What happens |
| --- | --- | --- |
| `PartitionNotFound` | `Absent` | Reported at `DEBUG`. The partition really is gone. |
| `IO` / `GlommioIO` | `Retryable` | Three attempts, 2 ms apart, then given up on at `ERROR`. |
| anything else | `Fatal` | Given up on at `ERROR` without retrying. |

**Every outcome reaches the shard.** A new `ServerMsg::PartitionLoadFailed { table, partition_id }`
is what a read sends when it gives up, and `read_partition` cannot return without sending one
message or the other. It carries no error — the loader logs that, where the context is richest;
what the shard needs is only which partition to release.

**The shard releases the parked queries.** `ShoalDatabase::fail_partition` mirrors
`load_partition`: the derive dispatches on the table name, `PersistentTable::fail_partition`
drains `blocked.remove(&partition_id)`, and each released query is replayed as a
`ServerMsg::Query`. No `MarkEvictable` follows it, unlike a successful load — nothing was read,
so nothing entered `partitions` and nothing came out of the LRU that has to be put back.

**A released query is marked to skip the read that just failed.** This is the part without which
the fix livelocks. Only `Absent` is self-healing on replay: `find_partition` returns `None` the
second time and the query answers correctly by finding nothing. For every other class the archive
entry is still in the map, so a replay that consulted disk again would park on the same failure and
be released again without end. `QueryMetadata` — a server-side struct that never reaches a client —
gained `skip_disk: Option<u64>`, set per released query, and checked in `block_on_load`.

**`sorted.rs` gained a `block_on_load`.** The unsorted table already had one; the sorted table had
the same six lines copied into six query paths. They are now one function, which is why the
`skip_disk` check is in one place rather than six.

## Alternatives rejected

**Requeue onto the loader channel, as the `todo!()` said.** The loader holds only the receiver;
the sender is discarded by the derive, so this means widening `StorageSupport::spawn_loader` and
four call sites to thread one through. Worse, the channel is unbounded, so a requeue completes
instantly and `recv()` hands it straight back — a spin at full CPU with no backoff, on the two
error classes where reading again can never succeed. It needs a retry counter on `LoaderMsg` and
a timer before it is even correct, at which point it is the inline retry plus a channel round trip.

**An internal `VecDeque` backlog drained at the top of the loop.** `start` blocks on `recv()`, so a
backlog with no further messages behind it never drains at all.

**Retry forever with backoff.** Nothing is ever answered wrongly, and a permanently unreadable
archive leaves the query and its client hanging forever — which is the defect, restated politely.

**A `failed_loads: HashSet<u64>` on the table instead of `skip_disk` on the query.** This was the
first design and it has a bug: `blocked.remove` returns *n* parked queries, and the first replay to
consult the set clears it for the other *n−1*, which then request the same failed read again. A
refcount instead leaks whenever a released query does not come back through `block_on_load` — which
a multi-partition get whose slot `pending_data` has already filled does not. Per-query state has
neither problem.

**`ResponseAction::Error`, so a failed read is reported rather than read as empty.** This is the
honest answer and it is a wire format change reaching the gather/merge path, the client and every
site that builds a response. It is filed as [item 56](../known-issues.md#56-a-response-cannot-say-that-a-read-failed),
which items 51 and 55 also want.

**Ending the shard on a `Fatal` failure.** It is what a corrupt archive already does, and it turns
one unreadable archive into an outage. Releasing the queries is the lesser harm, and the `ERROR`
log is what stops it being silent.

## Invariants to uphold

- **Every `LoaderMsg::Request` the loader accepts produces exactly one shard-local message** —
  `Partition` or `PartitionLoadFailed`. Nothing else drains `blocked`. A path added to
  `read_partition` that can return without sending one reintroduces the hang.
- **`check_disk` stays true after a failed read.** Setting it false would assert that memory holds
  everything when disk was never read — silent data loss, and permanent. It is deliberately left
  alone so a later query tries the archive again; that is what makes a transient failure heal.
- **`skip_disk` exempts exactly one partition for exactly one execution.** It is an `Option<u64>`
  because `to_blocked` narrows a replayed query to a single partition and `fail_partition` fires
  per partition. A change that parks one query on several partitions at once needs more than an
  `Option` here, and would fail silently rather than loudly.
- **No `MarkEvictable` on the failure path.** It also advances `flushed_generation` via
  `max(generation)`, and a read that never happened has no generation to offer.
- **`classify`'s default is `Fatal`, not `Retryable`.** A new error class defaulting to retryable
  turns a permanent failure into a loop that reports nothing.
- **The loader never returns `Err` for a read failure.** `FsLoader::start`'s `Result` is not
  observed until shutdown — `spawn_loader` pushes the task into `FileSystem::tasks`, which are only
  drained in `FileSystem::shutdown` — so a loader that returns early stops loading silently, which
  is worse than the panic it replaced.

## Still open

- Item 16's other twelve panic sites, none of them on the storage read path.
- Item 51's remainder: `load_partition`'s own early exits, which still return `Err` past the
  drain of `blocked`. `fail_partition` now exists and does the releasing, so this is a matter of
  routing those errors into it.
- [Item 56](../known-issues.md#56-a-response-cannot-say-that-a-read-failed): a read that failed is
  reported to the client exactly as an empty partition is. The server logs it; the client cannot
  tell.
- [Item 57](../known-issues.md#57-a-missing-archive-is-created-empty-rather-than-reported), found
  on the way: `get_archive` opens with `create(true)`, so a missing archive is created empty rather
  than reported, and surfaces later as a validation failure on bytes nobody wrote.
- Nothing bounds how many reads are in flight, and each holds a duplicated file handle. That is
  what makes `Retryable` worth having, and it is [item 15](../known-issues.md#15-no-backpressure-anywhere).

## Tests

| Test | What breaks without it |
| --- | --- |
| `a_get_whose_partition_cannot_be_read_does_not_hang` (`shoal/tests/persistent_sorted_table.rs`) | The whole path. Against the unfixed tree it produces the reported panic and then times out. Its timeout also catches a replay that asks for the same failed read again, since a livelock and a hang look the same from here. |
| `a_get_whose_partition_cannot_be_read_does_not_hang` (`shoal/tests/persistent_unsorted_table.rs`) | The unsorted table's share. The two tables park and release queries through different code, so a fix applied to one only would leave this half hanging. |
| The second half of both, after the archives are readable again | `check_disk` being left true. A failed read that convinced the table its memory copy was complete would pass the first half and fail here. |
| `a_pruned_partition_is_classified_absent` (`.../fs/tests.rs`) | The ToCToU stops being self-healing and a pruned partition is retried three times before it is given up on. |
| `a_missing_table_map_is_not_retried` | `TableMapMissing` becomes a retry of something that cannot come good. |
| `an_archive_open_failure_is_retryable` | A momentary shortage of file descriptors becomes a permanent give-up. |
| `an_unrecognised_error_is_fatal` | `classify`'s default flips, and a new error class becomes an infinite retry that reports nothing. |

## Related

- [Item 15](../known-issues.md#15-no-backpressure-anywhere) — no timeouts, which is why a
  stranded query is stranded permanently rather than briefly.
- [Item 33](../known-issues.md#33-collected-split-query-state-has-no-expiry) — this was the
  concrete route by which a `Gather` leaked. That route is closed; the general defect is not.
- [F4](../features/validated-archives.md) — added the first failure in `load_partition` that
  returns rather than panics, which is what made item 51 worth filing.
- [F9](../features/ephemeral-tables.md) — ephemeral tables are the same tables over `NoStorage`,
  whose `load_partition` always answers `false`, so nothing they hold can ever be parked on a read.
