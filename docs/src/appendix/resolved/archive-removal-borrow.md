# 111. Removing an archive held the handle cache across its close, and a read landing meanwhile panicked the executor

## Symptom

A shard's executor died with `RefCell already mutably borrowed` at
`shoal-core/src/server/tables/storage/fs/map.rs:630`, the first line of
`ArchiveMap::get_archive`, whenever a read of an archived partition landed on the executor
while its compactor was deleting an old archive. Under a standalone node's usual mixture the
window was a few microseconds per archive compaction; a node hosting several slots on one
executor ([F47](../../features/local-rehome.md)) puts several groups' reads beside one
compactor and hit it in the second round of the rehome crash matrix.

## Cause

`ArchiveMap::remove_archive` opened the handle cache mutably inside the scrutinee of an `if
let` and awaited the handle's close in its body:

```rust
if let Some(removed) = self.loaded_archives.borrow_mut().remove(id) {
    removed.close().await?;
}
```

A temporary in an `if let` scrutinee lives to the end of the whole statement, so the
`RefMut` was alive across the `.await`. The close is an io_uring operation that suspends the
task, the executor runs whatever is ready, and a partition read - the loader, a digest, a
snapshot cut - reaches `get_archive`, which borrows the same cache and panics. The pattern has
been there since the eviction commit that introduced `remove_archive`; nothing else in the
tree holds a `RefCell` borrow across an await in an `if let` this way.

## Evidence

**Reproduced.** The crash matrix `local_rehome_recovers_after_each_crash_point` died in its
third round with the panic above in node two's child, after that node's compactor compacted
its archives while the test read every key through it. The unit test
`removing_an_archive_does_not_hold_the_handle_map_across_the_close` in
`shoal-core/src/server/tables/storage/fs/map.rs` then reproduced it against the map alone: two
archives open in the cache, one task removing the first, a second task reading the other
through the cache while the first is suspended at its close. Against the unfixed tree:

```text
thread 'server::tables::storage::fs::map::tests::removing_an_archive_does_not_hold_the_handle_map_across_the_close'
panicked at shoal-core/src/server/tables/storage/fs/map.rs:630:53:
RefCell already mutably borrowed
```

With the fix both tasks complete, the removed archive is gone from the cache and the other is
still there.

## The fix

The handle is taken out of the cache in a statement of its own, so the borrow ends before the
close is awaited:

```rust
let removed = self.loaded_archives.borrow_mut().remove(id);
if let Some(removed) = removed {
    removed.close().await?;
}
```

Nothing else changes: the format entry and the `all_archives` entry are removed after the
close as before.

## Alternatives rejected

**A `Mutex` or an async-aware cell for the handle cache.** The executor is single threaded and
the cache is read on every partition load; a lock buys nothing a correctly scoped borrow does
not, and costs a hot path an acquisition.

**Closing the handle without awaiting.** A `DmaFile` dropped while open is logged by glommio
and leaks its descriptor until the reactor notices; the close is the right thing to await, it
only has to be awaited without the borrow.

## Invariants to uphold

- **No `RefCell` borrow on an `ArchiveMap` field lives across an `.await`.** Every field of the
  map is borrowed by the loader, the compactor, the digest and the snapshot paths on one
  executor, and any of them can run while another is suspended.
- **A temporary in an `if let` or `match` scrutinee lives to the end of the statement.** A
  borrow taken there is held through every `.await` in the body; bind it to a `let` first.

## Still open

Nothing. The scan for the same shape - an `if let` whose scrutinee borrows a `RefCell` and
whose body awaits - found this one site in `shoal-core`.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `removing_an_archive_does_not_hold_the_handle_map_across_the_close` | `shoal-core/src/server/tables/storage/fs/map.rs` | The read during the close panics the executor with "already mutably borrowed" |
| `local_rehome_recovers_after_each_crash_point` | `shoal/tests/cluster_fixture.rs` | Node two dies in a round with several slots on one executor once its compactor compacts archives under reads |

## Related

[F47](../../features/local-rehome.md), [item 13](eviction-log-underflow.md) for the eviction
path this cache serves, [Memory and eviction](../../tables/memory-and-eviction.md).
