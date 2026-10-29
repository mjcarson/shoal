# 314. A read counted its own node's disk in that disk's queue only once its executor took it

A read of a pool of copies on disks chooses its holder by queue
([F97](../../features/disk-reads-by-queue.md)): each current holder's reads queued as this node sees
them, a margin of four added to a remote holder's ([F101](../../features/disk-read-margin.md)), the
shortest first. A remote holder's queue was the reads this node had outstanding to it, counted by
`read_from` from the moment the read chose it. This node's own disk's queue was its slice's
foreground requests in flight, which a request joins only once the slice's executor takes it from
its mailbox.

Between the choice and the executor's take, a local read was counted nowhere. On an idle host that
gap is microseconds. On a loaded one the coordinating shard made every choice of a burst before the
executor took any read, so each choice saw an empty local queue and stayed local.

Found as item 314, a fixture row that failed in workspace runs, and confirmed by the diagnostics
item 314 gave the row before it was fixed.

## Symptom

`copies_on_disks_are_read_by_queue` puts an object on a pool of copies on disks, slows node zero's
arm by 40 ms a request (`SLOW_ARM 40`), and sends sixteen gets through node zero at once. Past the
margin a read leaves its coordinator's disk, so nodes one and two should each serve some. In window
three's, F113's and item 315's workspace runs node zero's disk served all sixteen:

```text
node 1's disk served none of the reads node zero's queued: [4194304, 0, 0]
```

Every get was answered right. Only where the reads went was wrong.

## Cause

`ObjectStore::queue_of` (`objects/executor.rs`) counted the two kinds of holder at different times:

```rust
if self.is_local(holder) {
    return slices.get(&holder.slice).map_or(0, |state| state.foreground.load(Ordering::Relaxed));
}
remote_reads … .map_or(0, |count| count.load(Ordering::Relaxed))
```

and `read_from` counted only the second kind, from the choice:

```rust
let _outstanding = (!self.is_local(&holder)).then(|| { … count.fetch_add(1, …); Outstanding(count) });
```

A local read reaches its slice's `foreground` count when the executor that owns the slice takes it
from its mailbox and starts serving it. Until then a burst of choices saw no local read at all.

## Evidence

**Established by reproducing it, twice.**

**In item 315's workspace run** (2026-10-26), the row failed again, now keeping node zero's queue
while its reads ran, sampled through a fixture verb of its own (`WORK`, which reads the atomics
without asking an executor):

```text
node 1's disk served none of the reads node zero's queued: [4194304, 0, 0]; node zero's queue peaked at 16, (ms, in flight) as it changed: [(0, 0), (64, 16), (107, 15), (147, 14), …, (709, 0)]; reads answered at ms [107, 150, 191, 229, 268, …, 710]
```

The queue went from nothing to sixteen, far past the margin, but only once every read had chosen.
Each was answered one arm's 40 ms after the one before, from node zero's disk. The same row unloaded,
before the fix, peaked at ten with six reads moved: the count lagged there too, by less.

**By a test written before the fix.** `a_local_read_counts_in_its_queue_from_its_choice` holds a
store's one executor busy with a job that blocks its thread, as a loaded host holds it, and sends
eight reads of its slice meanwhile. On the tree before the fix:

```text
thread 'server::objects::lane::tests::a_local_read_counts_in_its_queue_from_its_choice' panicked at shoal-core/src/server/objects/lane/tests.rs:1379:5:
assertion `left == right` failed: eight reads chosen while their slice's executor was busy were counted as 0 in its queue
```

## The fix

**A local read is counted against its slice from its choice until it is answered**, as a remote read
always was.
- **`SliceState::chosen`** counts the reads this node chose the slice for. `read_from` takes its guard
  (`Chosen`) for a local holder, as it takes `Outstanding` for a remote one, and the guard gives the
  count back however the read ends, a cancelled one included. It is the slice's own atomic, so a
  local read takes no lock.
- **A local slice's queue is its foreground in flight, and never less than the reads chosen for it**
  (`SliceState::queue`): every request on it from any node once its executor has taken them, and
  this node's own reads from the moment they chose it.

With the fix the row's sixteen reads split as the margin computes: eight on node zero's disk, four on
each other holder's. Five runs of it beside the object and device store suites split them the same.

## Alternatives rejected

- **Counting a local read into the slice's foreground at its choice.** The executor counts a request
  as it takes it, from any node; every path that serves a request would have to know whether its
  coordinator had counted it already.
- **Adding the two counts.** A local read that reached its executor is in both, so a sum counts it
  twice. The larger of the two is never above the true queue, and is exact in the burst that failed.
- **The store's map, as a remote read uses.** It takes a mutex on every read. Remote reads already
  pay it, and a local read is most of what an SSD pool reads.

## Invariants to uphold

- **A read is counted against the slice it chose from the choice until it is answered**, by a guard
  its end drops, whichever node the slice is on.
- **A local slice's queue is never less than the reads this node chose it for.**
- **Counting a read never takes a lock for this node's own slice.**

## Still open

- ~~**F101's figures were taken under the lagging count.** In a burst a margin of four now moves the
  reads it says it moves, where the lab's A/B saw it move 1 to 2 percent. Whether the disk leg's
  figures move with it is for a disk-leg A/B to say.~~ Measured on 2026-10-27 with the cache of read
  units off on both sides
  ([F101's third run](../../features/disk-read-margin.md#performance)). No rate moved. At 1 MiB and
  sixteen in flight the margin now takes europa's disk from 49% of the bytes read to 43%, the ranges
  disjoint, where the lagging count left it at 49% and 51%. The lower tail it gave there held.
- **Reads other nodes chose this node's disk for** are counted here only once its executor takes
  them, as before.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_local_read_counts_in_its_queue_from_its_choice` (`shoal-core`, `objects::lane::tests`) | Eight reads chosen while their slice's executor is busy count as none in its queue, or a read answered keeps its place |
| `copies_on_disks_are_read_by_queue` (`cluster_fixture`) | Under load, sixteen reads through node zero all stay on its disk; its message says node zero's queue as they ran |
| `copies_on_disks_are_read_locally_without_a_margin` (`cluster_fixture`) | Unchanged: with no margin set, every read stays local whatever its queue |
| `a_read_leaves_its_local_disk_only_past_the_margin` (`shoal-core`, `shard::objects::read`) | Unchanged: the order a queue and the margin make |

## Related

- [F97](../../features/disk-reads-by-queue.md): reads from copies on disks by their queues.
- [F101](../../features/disk-read-margin.md): the margin a remote holder's queue carries.
- [F108](../../features/object-work-in-flight.md): the in-flight figures the row now samples.
- [Tests That Fail Under Load](../flaky-tests.md), where the row's failures are recorded.
