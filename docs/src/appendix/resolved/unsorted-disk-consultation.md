# 4. Unsorted updates and deletes never consult disk

## Symptom

An update or delete against an unsorted partition that was on disk but not resident — after
eviction, or after a restart before it had been faulted in — reported that the row did not
exist, and did nothing. The caller was told `false` about a row that was sitting in an archive.

## Cause

`delete` and `update` decided everything from `self.partitions`:

```rust
match self.partitions.remove(&key) {
    Some(old) => { /* write intent, ack true */ }
    None => { /* respond Delete(false) */ }
}
```

Neither called `storage.load_partition`, unlike `get` and `exists` in the same file, and unlike
every sorted equivalent (`.../persistent/sorted.rs:749-770`, `:924-947`). Resident-or-nothing
is a reasonable model for a cache; it is the wrong model for a table whose partitions are
evicted out from under it.

## The fix

Both now park the query on `self.blocked` and replay it once the loader delivers the partition,
the way `get` and `exists` already did. Three things fell out of that:

**Unsorted partitions carry tombstones.** `UnsortedPartition::row` is a `MaybeRow<R>`, and a
delete replaces the partition with `UnsortedPartition::tombstone(key)` rather than dropping it
from the map. Dropping it was only ever safe while an unsorted delete could not reach a
partition that existed on disk; once it could, removing the key left nothing to shadow the
archive copy with and the next read faulted the deleted row straight back in
([Partitions](../../tables/partitions.md#tombstones)).

**[Item 5](resurrected-deletes.md) had to be fixed with it**, because a tombstone is only a
memory-lifetime shadow. The archive entry itself has to go, or the row returns as soon as the
tombstone is evicted.

**Duplicate load requests are suppressed.** Every blocked query used to send its own
`LoaderMsg::Request`, so a partition with N waiters was read N times and the late arrivals
reinstalled it through the `Vacant` arm of `load_partition` — which would have undone a
tombstone. A query now queues behind an existing `blocked` entry
(`.../persistent/unsorted.rs:300-337`).

## Invariants to uphold

- **A query that cannot answer from memory must consult the archive map before answering
  "no".** "Not resident" and "does not exist" are different answers, and the second one is a
  lie unless the map has been asked.
- **An unsorted delete must leave a tombstone, not remove the key.** The key's absence means
  "ask disk"; a tombstone means "it is gone". Those are not interchangeable.
- **Only one loader request may be in flight per partition.** Beyond the wasted read, a late
  arrival taking the `Vacant` arm reinstalls the archive copy over whatever memory holds,
  including a tombstone.

## Tests

`delete_when_not_resident`, `update_when_not_resident`, and `insert_after_delete_when_not_resident`
(`shoal/tests/persistent_unsorted_table.rs`). Each inserts a row, cycles the server twice so the
partition lives only in an archive, and then mutates it. All three fail on the old code with
`QueryDidNotSucceed`.

## Related

- [Deleted rows came back](resurrected-deletes.md) — the other half of this fix
- [Table Types](../../tables/table-types.md)
- [Partitions](../../tables/partitions.md#tombstones)
