# 16. The rest of the hot path's panics

*This page closes [item 16](../known-issues.md). Three earlier changes took parts of it: the
loader's three sites ([Resolved #16, 51](partition-load-failure.md)), the merge path's two corrupt
archive sites ([Resolved #56, 61](response-error-channel.md)) and the two relays' five
([Resolved #34](unvalidated-length-prefix.md)). This change took every site the item still listed,
plus four it had missed. What is left are panics that hold by construction. They are listed under
[Invariants to uphold](#invariants-to-uphold), because they are what the next person to touch
that code must keep true.*

## Symptom

Several things a running server can meet ended the shard that met them, and every client that
shard was serving along with it:

- **A client that reused a query id.** Two gets sharing an id and an index, one asking for whole
  rows and the other for a projection, reached `PendingGets::resume` while the first was parked on
  a partition read. The downcast failed and the shard panicked. The id and the index are the
  client's own choice, so this was reachable by anything that could open a connection.
- **A loader that had gone.** `block_on_load` unwrapped the request it sent the loader, so once
  the loader's channel closed, the next query needing a partition from disk panicked the shard.
  The whole process went with it, since a panic on a glommio thread does not unwind.
- **A write that reached a table on a cluster node.** Such a table has no intent log of its own,
  because its writes are commands its tablet group commits. A write that got past the group
  anyway failed its commit, and all eight `storage.commit(&intent).await.unwrap()` sites turned
  that into a panic.
- **An archive that does not deserialize after validating.** Six `SortedPartition::deserialize(..)
  .unwrap()`s and one in `MaybeLoaded::<UnsortedPartition>::update` were on the write and replay
  paths.
- **Plumbing that could only fail on a bug:** an unknown local shard in `Comms::send` and
  `get_shards_channels`, a unicast `ServerMsg` handed to a broadcast (`Clone` panicked on 38
  variants), a gathered share with no collector named, a colliding client id, `peers` looked up
  again and expected to be there, and a `DataFlushed` wakeup that could not be sent.
- **An intent that could not be archived.** `RkyvSupport::serialize` unwrapped
  `rkyv::to_bytes`, which fails when a value's archive would put a relative pointer further than
  its width reaches. A partition grows until that happens.

## Cause

There were two causes, and the item had filed them as one.

**Most sites had nowhere to send an error.** A table answered a query by returning
`Option<(Uuid, Uuid, StageStamps, Response<P>)>`, and before
[F11](../../features/error-channel.md) a response could not carry a failure. A failure that could
not be answered was unwrapped. F11 gave every one of these sites somewhere to go, and none of
them had been changed to use it.

**The write path's sites were unwrapped in the wrong order.** Every write mutated its partition
and then committed, apart from insert and the unsorted delete. For an update or a sorted delete,
a commit failure left the table ahead of its log, so handling the error would not have been
enough. The write also had to be reordered, or undone, first. That is why nobody had simply
swapped the `unwrap` for a response.

**The item's description of the write path was wrong.** It said a full disk, an `EIO` or a closed
intent log panicked the shard on an ordinary insert. None of them could. `StreamWriter::write`
never returned `Err`: the write runs on a detached task, and an error it hits is recorded in
`FlushState` for `check_error` to surface. The three `write(..).await.unwrap()`s in `prep`,
`consume` and `sync` unwrapped a value that was always `Ok`. `FileSystem::commit` returns `Err`
in only two cases: the cluster-node sink check, and an archive that cannot be written. Both
happen before a byte is staged. A device error surfaces later, through `compact_if_needed` into
`get_flushed`, and the shard loop's `?` ends the shard there. That is not a panic, but it is the
same outage, and it was filed as item 122, since [resolved](intent-log-failure.md).

## Evidence

**Reproduced against the unfixed tree for every site a test can reach.** The one-shard harness
`table_backlog.rs` built for [item 15](backlog-bounds.md) builds a shard's tables on a glommio
executor without a shard. `hot_path_failures.rs` uses it to drive each failure through `handle`,
the way the shard does:

```
a_sorted_write_a_table_cannot_commit_is_refused_and_changes_nothing:
thread 'hot_path_failures-0-1' panicked at shoal-core/src/server/tables/persistent/sorted.rs:1265:74:
called `Result::unwrap()` on an `Err` value: GlommioGeneric("a table on a cluster node committed to
an intent log it does not have; every write goes through its tablet group")

an_unsorted_write_a_table_cannot_commit_is_refused_and_changes_nothing:
thread 'hot_path_failures-0-1' panicked at shoal-core/src/server/tables/persistent/unsorted.rs:1089:66:
called `Result::unwrap()` on an `Err` value: GlommioGeneric("a table on a cluster node committed to
an intent log it does not have; every write goes through its tablet group")

a_sorted_read_the_loader_cannot_take_is_answered:
thread 'hot_path_failures-0-2' panicked at shoal-core/src/server/tables/persistent/sorted.rs:645:14:
called `Result::unwrap()` on an `Err` value: KanalSend(Closed)
thread 'hot_path_failures-0-2' panicked at .../library/core/src/panicking.rs:233:5:
thread caused non-unwinding panic. aborting.

an_unsorted_read_the_loader_cannot_take_is_answered:
thread 'hot_path_failures-0-2' panicked at shoal-core/src/server/tables/persistent/unsorted.rs:560:14:
called `Result::unwrap()` on an `Err` value: KanalSend(Closed)
... thread caused non-unwinding panic. aborting.
```

The failing write in both write tests is the delete: a sorted delete of a resident row and an
unsorted delete of one. Each test runs its delete first. The loader failures abort the whole test
binary rather than failing one test, which is exactly what they did to a server. The projection
collision was reproduced by a unit test over `PendingGets`, run on the unfixed tree with the old
signature:

```
thread 'server::tables::persistent::tests::evidence_resume_collision' panicked at
shoal-core/src/server/tables/persistent.rs:593:27:
a parked get was resumed with a different projection
```

**The rest were established by reading the source.** A colliding client id takes a v4 UUID minted
twice. An unknown local shard and a unicast broadcast are routing bugs, with no input that
produces them. An archive that validates and then fails to deserialize is a bug in the archive
format. The `StreamWriter::write` claim was checked by reading every return in the function: there
were two, and both were `Ok(())`.

## The fix

**A write that cannot be committed is refused, and the table is left as it found it.**
`ErrorCode::StorageWrite = 13` is new, and it is a definite refusal. `persistent::storage_write`
answers with it, logs the whole error at `ERROR`, and tells the client only the table and the
partition, as `corrupt_archive` does. The code is added at the end of the storage range, and
`from_u16` maps a number it does not know to `Unknown`, so an older client reads it as a failure
it cannot classify.

What "left as it found it" took, per write:

| Write | Before | Now |
| --- | --- | --- |
| Insert (both tables), unsorted delete | commit, then mutate | Unchanged order. The error arm just answers. |
| Sorted delete, loaded partition | `remove`, then commit | Unchanged order. The error arm takes the sort key back out of the intent and calls the new `SortedPartition::restore`, the exact inverse of `remove`: the row, its size, one tombstone fewer. |
| Sorted update, loaded partition | `partition.update`, then commit | `live_row_mut` finds the row, then commit, then `update_row` and `resize`. Still one lookup. `SortedPartition::update` is now written over the same two helpers, so replay and the request path cannot drift. |
| Unsorted update | `MaybeLoaded::update`, then commit | Read an accessible archive into rows, commit, then apply: in place through `update_loaded`, or to the rows just read, which then replace the archive. |
| Any write, accessible partition | deserialize, mutate the copy, commit, swap it in | Unchanged, but a failed commit just drops the copy, since nothing was swapped yet. |
| Sorted insert, accessible partition | commit, then deserialize | Deserialize first, then commit, through one `HashMap::entry` held across the commit. |

**An archive that does not deserialize is answered, not unwrapped.** On the request paths,
`persistent::unreadable` answers `CorruptArchive` before anything is committed. That is the order
the table above exists to guarantee. On the replay paths the `unwrap` became `?`, since the
function already returned `Result` and used `?` on the rkyv calls beside it.
`MaybeLoaded::<UnsortedPartition>::update` returns `Result`, and the replicated apply answers
`ApplyStep::Refused` on an error, as the sorted one already did.

**A read that cannot be asked for is answered.** `Parking` gained
`Failed(ResponseError)`, which `persistent::read_not_asked` builds with `StorageRead` after logging
the send error. A query that parks at most once, like a delete, an update or an exists, answers
with it on the spot, because nothing of it is parked. A get may already be parked on another of
its partitions, so answering on the spot could put a second response at its index. Instead,
`PendingGet::fail` fills the slot empty and keeps the first failure, and the get answers with it
once nothing of it is parked, by the same `is_pending` test that decides when it answers at all.

**A get that collides with a parked one is refused, and the parked one is kept.**
`PendingGets::resume` returns `Result`. A failed downcast hands back the boxed state, which goes
straight back into the map, and the colliding get is answered `InvalidRequest`. The get that
parked first finishes as though nothing happened.

**`ServerMsg` is not `Clone` any more.** `try_clone` returns
`Result<Self, &'static str>`. Every variant that used to panic returns the sentence its `panic!`
carried. `Comms::broadcast` copies before each send and refuses with `ShoalError::NotBroadcast`
on the first `Err`. Whether a variant can be copied depends on the variant alone, so a refusal
always comes before the first shard is sent anything.

**The rest are local:**

- `Comms::send` and `get_shards_channels` return `ShoalError::UnknownShard`.
- The gathered-share `expect`s in `execute_query` and `answer_read_failure` read the collector and
  the metadata out together. Metadata naming no collector is a whole query and is answered to its
  client, which is what that metadata means.
- `flush_forwards` uses the `peers` it bound with `let … else` at the top of the iteration
  instead of looking them up twice more.
- A colliding client id removes the entry, so neither connection is answered, and logs at `ERROR`.
  Every shard is told of both connections, so every shard decides the same way.
- `StreamWriter::write` returns `()`.
- The `DataFlushed` sends discard their result. A closed channel means the shard is exiting, and a
  detached task has nobody else to tell.
- `RkyvSupport::serialize` returns `Result`. `FileSystem::commit` uses `?` on it. `build_intent`
  and `ShoalDatabase::write_command` return `Result<Option<..>>`, and a cluster node that cannot
  archive a write answers `StorageWrite` without proposing it. The digest and canonical cut paths
  use `?`.

**What it cost the success path.** Nothing measurable, and no capture was taken for it. Nothing
here claims a performance effect. Every write makes the same lookups it made before. Insert still
does one `entry`. The sorted update's `live_row_mut` is the lookup `update` was already doing.
The sorted delete keeps its removed row until the commit returns instead of dropping it at once,
which is the same drop, later. The extra work on a successful path is a `match` on a `Result` that
was already there to be unwrapped, and a match on the partition's variant before an insert or
unsorted update commits.

## Alternatives rejected

**Check that the row exists, then commit, then change it.** This makes a failed commit trivially
safe and costs a second `BTreeMap` lookup on every update and every delete. `live_row_mut` does
the same with one lookup for updates. For deletes, `restore` puts the cost on the failure arm,
which is the only arm that needs it.

**Clone the row before an update, to put back on failure.** One allocation per update to protect
against a failure that, on a standalone table, cannot happen.

**Keep `impl Clone for ServerMsg`, returning something harmless for the unicast variants.** There
is no harmless value. A broadcast of a `Forward` that silently became a `Shutdown` is a worse
bug than the panic.

**Make `commit` infallible.** Its one live failure is a write that reached a cluster table without
its group. That is a routing bug, and an infallible commit would swallow the write. Saying so
to the client is the point.

**Keep the first connection when a client id collides.** The shards would then route the newer
connection's replies by an id that names the older one, and hand one principal's rows to another.
Answering neither is the only outcome that cannot leak.

**Mint client ids from a counter, so they cannot collide.** This removes the collision, but the
id leaves the shard in a share's metadata. Whether it can meet a counter-minted id from another
process was not worth establishing for a 2⁻¹²² event. The random id keeps that question closed.

**Answer a get whose read could not be asked for at once.** This is correct when nothing of the
get is parked, and a second response at its index when anything is. Keeping the failure on the
`PendingGet` is correct in both cases and needs no test to tell them apart.

**Retry the request to the loader.** The only thing that refuses it is a closed channel, and
that means the loader is gone. Retrying waits for something that will not come.

## Invariants to uphold

- **`FileSystem::commit` fails only before it stages a byte.** Every write in the table above
  answers `StorageWrite` as a definite refusal because of this. A failure added after `prep`
  would have written part of an intent that the client is told did not apply.
- **No write changes a partition before its commit returns `Ok`, except the sorted delete's
  `remove`.** That one relies on `restore` being `remove`'s exact inverse. A field added to
  `SortedPartition` that `remove` changes has to be put back by `restore`.
- **An accessible partition is deserialized before the commit that will replace it.** Moving the
  deserialize after the commit logs a write whose partition the table then cannot hold.
- **`Parking::Failed` means nothing was parked.** A caller that answers on it relies on no read
  landing later to replay the query again.
- **A get answers only when `is_pending` is false.** That is what keeps a failed partition to one
  response. `PendingGet::fail` fills the slot for this reason.
- **`ServerMsg::try_clone`'s answer depends on the variant alone.** `Comms::broadcast` refuses on
  its first iteration because of that. A variant that can be copied only sometimes would be sent
  to some shards before the refusal.
- **`StreamWriter::write` has no failure before its task is spawned.** One added there has to
  come back as a `Result` again, and be handled by `prep` and `consume` rather than unwrapped.

**What is still a panic, and why each one holds.** None of these reads input. Each follows a
line in the same function that already decided the question:

| Site | Why it holds |
| --- | --- |
| `RowSink::iter`, `RowSink::into_owned` (`persistent.rs`) | The sink's own types: only the identity projection can point at an archived row, and a built row's index is where it was pushed. |
| `SeekBytes::archive_key` (`partitions.rs`), two | The key arrived in a frame bounded far below what an archive's relative pointers reach, and the bytes were archived a line earlier. |
| `SortedPartition::remove`'s `unreachable!` | The `get` two lines up found a live row. |
| `PersistentSortedTable::insert`'s `unreachable!` | An accessible partition is always read before the commit, or the insert has already returned. |
| `PersistentSortedTable::apply`, two `entry @ … else { unreachable!() }` | The arm's own pattern matched `Accessible`. Rust cannot bind the entry and its contents at once. |
| `execute_query`'s `unreachable!("an answer is either open or sealed")` | `Answer` has two variants. |
| `groups.rs`, `snapshots.rs` `expect("still here")` | Re-borrows after a call that needed `&mut self`, of a value the same function held a line before. |
| `peers.rs` `expect("just listed")`, `loader.rs` `expect("a read is attempted at least once")` | The keys were collected from the map a line before, and the retry loop runs at least once. |
| The `unreachable_unchecked` intent extractions | The intent was built from that variant a few lines above, as `insert` always did. |

## Still open

- ~~**Item 122**: a background write or fdatasync error ends the shard, and
  the shard's other clients with it. This is what the item used to claim the commit sites did.
  It needs the pending writes failed with `StorageWrite` and a decision about what a shard does
  with a log it can no longer write. That decision is its own item.~~
  **[Resolved](intent-log-failure.md).** The decision was to refuse writes until restart. The
  writes pending past the watermark are answered `OutcomeUnknown`, not `StorageWrite`, because
  some of them may be on disk and `StorageWrite` has to stay a definite refusal.
- ~~**Item 123**: a parked get is keyed by the client's `(id, index)`, so a
  client that reuses a key while a get is parked, with the *same* projection, has the two gets'
  rows merged. Found while fixing the collision above, which only refuses a mismatched projection.~~
  **[Resolved](parked-get-key.md).** The usual outcome was worse than a merge: the second get was
  never answered. The key now carries the client and the bundle's attempt.
- ~~**Item 124**: the unsorted table's client update never re-stamps a loaded
  partition's `generation`, where the sorted table's and the replicated apply's both do. Found
  while reordering that update. The reorder kept the behaviour exactly, so the fix has its own
  evidence to find.~~ **[Resolved](unsorted-update-generation.md)**, reproduced as a stale read after an eviction.
- The client does not retry `StorageWrite`. That is deliberate. ~~the one way to meet it today is a
  routing bug, which the same node would reproduce.~~ Since [Resolved #122](intent-log-failure.md)
  there are two ways to meet it: a routing bug, and a table whose log failed. The same node
  reproduces both until it is restarted.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `a_sorted_write_a_table_cannot_commit_is_refused_and_changes_nothing` (`shoal/tests/hot_path_failures.rs`) | The sorted commit sites panic again. With the order reverted, a refused delete leaves its row removed and a refused update leaves its row changed, both caught by reading the row back. |
| `an_unsorted_write_a_table_cannot_commit_is_refused_and_changes_nothing` | The same for the unsorted table. The update check catches the update applied before its commit. |
| `a_sorted_read_the_loader_cannot_take_is_answered` | `block_on_load` unwraps again, and aborts the binary. |
| `an_unsorted_read_the_loader_cannot_take_is_answered` | The same for the unsorted table. |
| `a_message_for_one_shard_cannot_be_copied_for_a_broadcast` | `try_clone` stops refusing a unicast variant, or refuses a broadcast one. |
| `a_get_resumed_with_another_projection_is_refused_and_the_parked_one_kept` (`persistent.rs`) | The collision panics again. A fix that answered it without reinserting the parked state would lose the first get's rows. |
| `a_failed_partition_fills_its_slot_and_keeps_the_first_failure` (`persistent.rs`) | A get with a failed partition waits on it forever, or answers with a later failure than the first. |
| `restoring_a_removed_row_undoes_the_removal` (`partitions.rs`) | `restore` drifts from `remove`: the size or the tombstone count is left wrong after a refused delete. |
| `an_update_split_around_a_commit_is_the_same_update` (`partitions.rs`) | The split update, which the request path uses, drifts from the whole one, which replay uses. |

## Related

- [Resolved #16, 51](partition-load-failure.md), the loader's three sites, and the release path a
  failed read already had.
- [Resolved #56, 61](response-error-channel.md) and [F11](../../features/error-channel.md), the
  error channel every site here now answers through.
- [Resolved #34](unvalidated-length-prefix.md), the relays' five sites.
- [Resolved #15, the remainder](backlog-bounds.md), whose harness these tests are built on.
- Items 122, 123 and 124, filed from this change and since resolved:
  [Resolved #122](intent-log-failure.md), [Resolved #123](parked-get-key.md),
  [Resolved #124](unsorted-update-generation.md).
