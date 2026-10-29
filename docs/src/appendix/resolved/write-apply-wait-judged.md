# 291. The lagging-copy row timed a write's commit where it meant the wait for its copy

A coordinator answers a committed write once its own copy of the group has applied it, so a `One`
read through the same node sees the write. Since [Resolved #146](apply-wait-on-a-lagging-copy.md)
it waits at most two heartbeats for that. Before the bound, a copy catching up from the log held
every write through its node until the copy had applied its whole backlog.
`a_write_through_a_lagging_copy_is_answered_within_two_heartbeats` holds the bound, and under load
it failed on something the bound does not cover.

Filed from the record of [Tests That Fail Under Load](../flaky-tests.md) (F90's run on); fixed in the
flaky-test plan's second wave (2026-10-27).

## Symptom

In five loaded runs the slowest of the row's twenty writes took longer than its 800 ms: 917, 825,
875 and 813 ms in the runs whose message was kept. Each run passed at three threads or alone.

## Cause

The row timed each write at the client, from its send to its answer. That time holds the hop to the
group's leader, the leader's proposal, the replication to a quorum, each copy's WAL sync, and the
way back, beside the coordinator's wait for its copy. A loaded host stretches every one of those, its
syncs most of all, and none of them is the wait the row was written to bound. Its own comment said
so: two heartbeats "and the round trips, with room for a loaded host". The room ran out. The engine
had nothing that measured the wait apart from the rest.

## Evidence

**Reproduced.** The row was run with the commit slowed on both other copies, each releasing its WAL
completions 700 ms apart (`SLOW_WAL`), as a loaded host's syncs slow them. The coordinator's waits
were read beside the client's time through the figure this fix adds:

```text
the slowest of twenty writes through the catching-up node took 939.039972ms
REPRO291 node two's waits: 20 in all, 1 ran out, 0 installing, the slowest in Some((196608.0, 229376.0)) us
a write through a catching-up copy took 939.039972ms; its commit is not what it waited on
```

Node two's waits kept their bound. The slowest fell in the bucket from 197 to 229 ms, around the
200 ms bound that one of them reached. The other 700 ms were the commit.

Run against the tree with the wait unbounded, as before #146, the fixed row fails on the wait:

```text
the slowest of twenty writes through the catching-up node took 1.615246878s
a write through a catching-up copy waited 1572864 to 1835008 us for it, 1.615246878s in all; its commit is not what it waited on
```

## The fix

- **A shard counts each committed write's wait for its own copy** (`ApplyWaits`, on its replication
  report and folded per node):
  - the wait, from the commit as the coordinator learned it to the answer, in the buckets a client
    query's wait is counted in;
  - how each wait ended: applied, ended at once by an install (`installing`), or at its bound
    (`ran_out`).

  `wait_applied_here` now says how its wait ended, and `propose_through` counts it. The fixture
  reads the figure with `APPLY_WAITS`.
- **The row judges node two's waits for its twenty writes**: a wait each, and the slowest one's
  bucket beginning under 800 ms. That is the room the row had, now taken by the wait alone. The
  client's time is printed beside it.

## Alternatives rejected

- **Lengthen the client's bound.** The commit and the trips have no bound a loaded host respects,
  so any figure for them is a guess at the load.
- **Slow the commit in the row for good**, as the reproduction did, so that the row always shows
  the two apart. The row is about a copy catching up while writes come through it. With every write
  near a second, the backlog would be applied by the third write, and the row would say less about
  the case it was written for.
- **Time the wait in the test from the client's side** by subtracting a write's time through a
  copy that keeps up. The two writes would meet different commits, and the difference would carry
  their noise.

## Invariants to uphold

- **A row bounds the part of a time its claim names**, and reads that part where it is spent.
- **Every committed write a coordinator answers counts one wait**, however the wait ended, so the
  count matches the writes.
- **The wait is counted from the commit as the coordinator learned it**, never from the client's
  send.

## Still open

- **The lab's bench does not keep the figure.** A capture keeps what the `Replication` report's
  reads and rounds gained ([F99](../../features/strong-read-rounds.md)), not its apply waits, so a
  write's tail on the lab cannot be told apart the same way yet. Recorded in
  [TODOs](../todos.md#a-capture-keeps-the-replication-reports-newer-counts).

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_write_through_a_lagging_copy_is_answered_within_two_heartbeats` (`cluster_fixture`) | Under load, a write whose commit and trips take 600 ms beside its wait fails the row with every wait within its bound: 939 ms in all with the commit slowed |
| `a_writes_apply_wait_is_counted_by_bucket_and_how_it_ended` (`shoal-core`, `replication::report`) | A wait is not counted in its bucket or by how it ended, or a node does not fold its shards' waits |

## Related

- [Resolved #146](apply-wait-on-a-lagging-copy.md), the bound the row holds.
- [Resolved #145](apply-wait-on-a-stalled-copy.md), the same wait on a copy that is installing.
- [O76](../optimizations.md#o76-a-write-through-a-lagging-copy-waits-its-whole-apply-bound), the
  poll a copy whose last wait ran out waits instead.
- [Tests That Fail Under Load](../flaky-tests.md).
