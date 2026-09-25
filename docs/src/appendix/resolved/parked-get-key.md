# 123. A query reusing a parked query's id and index was never answered

*Filed by [Resolved #16](hot-path-panics.md). That change refused a colliding get that asked
for a different projection. This item is the same-projection case it left.*

## Symptom

A client that sent a get while an earlier get of its own with the same query id and index was
parked on a partition read had the second get **swallowed**. It was never answered, and its own
partitions were never read. The first get finished normally. A sorted `exists` behaved the same
way.

The item was filed as "the two gets' rows are merged". The reproduction showed that the usual
outcome is worse. Rows merge only where the second get names partitions the first one also
named, and even then the answer goes to the first get's slot alone.

## Cause

A get that needs a partition from disk parks. What it has found so far goes into `PendingGets`,
and the get is replayed once the read lands. `PendingGets` was keyed by `(meta.id, meta.index)`,
the query id and the index within its bundle, and the client chooses both. So when a second
get arrived under the same pair:

1. `resume` found the first get's progress and handed it to the second get
2. the second get's partitions were not in that progress (`rank` returned `None`), so each was skipped as "already read"
3. the progress was still waiting on the first get's partition, so the second get parked it again and returned nothing
4. the first get's replay took the progress, answered, and removed the key. The second get had nothing left to be answered from.

The sorted table's `pending_exists: HashMap<(Uuid, usize), Vec<u64>>` had the same key and the
same flaw. The second `exists` took over the first one's list of partitions still to read, and
parked on them.

A different client could only collide by guessing a v4 id. So this was a client confusing its
own queries, not reading anyone else's. A client that reuses an id is, among other things, a
client retrying a read it gave up on.

## Evidence

**Reproduced against the unfixed tree.** Each test in `resident_reads.rs` does the same thing:
1. parks a first query on a partition read, with the loader not yet running
2. sends a second query with the same client, id and index but a different attempt, naming a partition the first did not
3. expects that second query to be answered at once

```text
test unsorted_gets_reusing_an_id_are_answered_apart ... FAILED
test sorted_gets_reusing_an_id_are_answered_apart ... FAILED
test sorted_exists_reusing_an_id_are_answered_apart ... FAILED
a get reusing a parked get's id was never answered
a get reusing a parked get's id was never answered
an exists reusing a parked one's id was never answered
```

## The fix

**A parked query is keyed by `ParkKey { client, id, index, attempt }`**, built by
`ParkKey::of(&meta)`. It is used for `PendingGets`, for every `is_parked`, `resume` and `park`
in both tables, and for the sorted table's `pending_exists`.

The attempt is what makes this work. The coordinator mints a new one for every bundle it takes
off a socket (`send_to_shard`), and `read_plan` puts it on every query in the bundle, writes
included. A replay carries a clone of the metadata of the execution that parked. So every
replay of one query shares its key, and every bundle that reuses an id gets a key of its own.
The client id covers two clients choosing the same id.

Two bundles that reuse an id are now two independent queries. Each is answered from its own
partitions, and neither is refused. The projection check that
[Resolved #16](hot-path-panics.md) added is kept as a defensive refusal, although with the
attempt in the key it has no way to be reached.

## Alternatives rejected

**Refuse a fresh query whose key is already parked.** This needs a way to tell a fresh
execution from a replay, which would be a flag on the metadata that only the parking path sets.
It also turns a client's retry of a read into an `InvalidRequest` for as long as the first
attempt is parked. The attempt already tells the two apart, without a flag and without a refusal.

**Key by `(client, id, index)`, as the item suggested.** This closes the cross-client case and
leaves the one that happens: the same client reusing its own id.

**Mint a server-side id per query and key by that.** It is equivalent to the attempt, but it
has to be threaded through every place a query is parked or replayed. The attempt is already
there.

## Invariants to uphold

- **A replay carries the metadata of the execution that parked.** `ParkedQueries::park` and
  `join` store `meta.clone()`, and the replay is handed that clone. A replay built with fresh
  metadata would get a new key and start over, losing what the query had already found.
- **Every query in a bundle carries the bundle's attempt**, reads and writes alike, including a
  query a peer forwarded (`preamble.attempt`). A path that builds metadata with attempt 0 for
  distinct bundles makes them collide again.
- **The attempt is minted per bundle and never reused on one coordinator.** `next_attempt`
  only increments.

## Still open

Nothing of this item.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `unsorted_gets_reusing_an_id_are_answered_apart` (`shoal/tests/resident_reads.rs`) | The second get resumes the first one's progress and is never answered. |
| `sorted_gets_reusing_an_id_are_answered_apart` | The same, for the sorted table. |
| `sorted_exists_reusing_an_id_are_answered_apart` | The second exists takes over the first one's partitions still to read and is never answered. |
| `a_reused_id_in_another_bundle_is_another_key` (`shoal-core/src/server/tables/persistent.rs`) | A key stops including the attempt or the client, or a replay's key stops matching its own. |

## Related

- [Resolved #16](hot-path-panics.md), which refused the mismatched-projection collision and filed this.
- [F41](../../features/read-consistency.md), which introduced the attempt, for the gather's own
  version of the same question: telling a late share apart by identity rather than by arrival.
