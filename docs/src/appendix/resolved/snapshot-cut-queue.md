# 174. A move's snapshot cut waited behind every compaction queued before it

## Symptom

In every rebuild of hyperion under the bench, one to three sets took minutes to move, where the
rest took seconds. In section 8's first two runs one of them ran out its 600 s window and failed.
The slow sets were all `Movie` groups led by titan, the four-core Zen1 host. For each, the move
driver found the new copy making no progress at all, and then, minutes later, a snapshot was cut
and the copy caught up in seconds. The report the driver now gives every 30 s
([below](#the-fix)) said the same thing each time, in rebuild 6:

```text
a move's destination has made no progress  group=15584a2294dd0efb stalled_secs=270
  sent=14788 data=1 accepted=14787 conflicts=1 failed=0 last_prev=Some(1092395) last_acked=None
```

One append carrying entries was ever sent: openraft's first probe, at the leader's purge point. It
conflicted, as a probe to an empty copy does. Every append after it was an empty heartbeat. The next
step had to be a snapshot, and it did not come.

## Cause

Openraft asked for the snapshot at once. Shoal's `full_snapshot` asks the shard loop for a cut, and
the loop sends it to the table's compactor as a `CompactionJob::Snapshot`. The compactor takes its
jobs from one FIFO channel, one at a time: WAL segment merges, archive compactions, installs and
cuts. On titan under the bench the `Movie` compactor was far behind, so the cut waited behind every
merge queued before it. Rebuild 7 measured it:

```text
20:38:36 titan  taking a snapshot cut  table="Movie" group=84dfae50a3fe833b queued_ms=566758 backlog=180
```

That is 9.4 minutes behind 180 jobs, for a cut that then took about 10 s. The move's destination
waited for all of it, and so did the plan behind it.

## Evidence

**Found on the lab, established by measurement.** The stall was silent at `info`. Openraft's own
debug tracing was tried and swamped the nodes
([section 8](../../cluster-testing/correctness.md#8-an-unplaced-member-coordinates), runs 3 and 4).
So the network was taught to count the appends to each member of each group (`AppendStats`), and
the move driver to report them. That showed the replication was waiting for a snapshot, not failing.
Then the job was taught when it was asked for, and the compactor to log the wait and the backlog
when it takes a cut. Rebuild 7, on the unfixed order, gave the figure above.

`a_snapshot_cut_is_taken_ahead_of_queued_merges` (`fs/compactor.rs`) pins the new order. It cannot
fail on the old tree, which had no reordering to test, so the lab measurement is the evidence.

## The fix

The compactor drains its channel into a backlog before each job and takes the first snapshot cut in
the backlog ahead of everything else (`take_next`). Every other job keeps its order. That is safe
because the compactor is the only writer of archives and runs one job at a time: a cut taken
between any two jobs reads a consistent archive, and its boundary is simply whatever has been merged
so far. The destination catches up from the log above it, and the log keeps every entry above the
checkpoint.

Two diagnostics stay in the tree:

- The move driver logs `a move's destination has made no progress` every 30 s while a destination
  stands still, with the appends sent, accepted, conflicting and failed, the last failure, and the
  highest index acknowledged.
- The compactor logs `taking a snapshot cut` with `queued_ms` and `backlog`.

## Alternatives rejected

- **Reorder installs and drops too.** An install's place among the merges is what lets a merge skip
  the frames it replaced ([#166](segment-compaction-corrupt-loop.md)). Only cuts are read-only with
  respect to the archives.
- **A second channel for cuts.** It would take the same order with more plumbing: the compactor
  would still pick between two channels, and every sender would need both.
- **Cut from a copy of the archive map, off the compactor.** Everything a cut reads is the
  compactor's to change, so a cut elsewhere would need a snapshot of the map, which is its own
  project.

## Invariants to uphold

- **A cut reads the archives only between two jobs.** Reordering is safe only because the compactor
  is their sole writer and runs one job at a time.
- **Only cuts jump the queue.** Installs, drops and merges keep the order they were sent in.
- **The log holds every entry above a cut's boundary.** A cut taken early has an older boundary, and
  purging never passes the checkpoint.

## Still open

- **Titan's compactor was 180 jobs behind.** Under the bench a Zen1 node merges segments more slowly
  than it takes them in. That is what forces the retention budget's purges, and it would starve any
  job that is not a cut. Filed as
  [O74](../optimizations.md#o74-a-zen1-nodes-compactor-falls-hundreds-of-jobs-behind-under-the-bench).
- **A new copy is still probed from the purge point**, so a copy whose log is still retained is fed
  the whole log. See [#170](uncached-log-reads.md)'s Still open.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_snapshot_cut_is_taken_ahead_of_queued_merges` (`shoal-core/src/server/tables/storage/fs/compactor.rs`) | A cut queued behind merges waits for them, or cuts or merges lose their own order |

## Related

- [#170](uncached-log-reads.md), first blamed for the same stalls.
- [F43](../../features/node-recovery.md), where the cut was put on the compactor.
- [F45](../../features/replica-migration.md), the moves that wait on it.
