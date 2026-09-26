# 170. A member fed from the log read one entry per I/O, and a move's catch-up never finished

## Symptom

The rebuild of hyperion that proved [#169](unplaced-member-forwards.md)'s fix on the lab
([section 8](../../cluster-testing/correctness.md#8-an-unplaced-member-coordinates)) was taken on
a cluster loaded from the csv minutes before. Five sets moved in about 10 s each. The sixth, a
`Movie` group led by titan, sat in `CatchingUp` for the whole of the move's 600 s window and failed:

```text
16:14:33 titan  driving a group's move  group=c7ec528e6824849d phase="planned"
16:22:19 titan  cutting a snapshot      group=c7ec528e6824849d boundary=187643 records=96739
16:22:29 titan  a snapshot was installed on a member  target=0935612f…/1 bytes=50846043
16:24:33 titan  a group's move failed   phase="catching_up"
                error="0935612f…/1 did not catch up within 600s"
```

Titan's compactor was idle for the whole wait, so no snapshot cut was queued behind it. The group
still held almost a gigabyte of log under its retention budget
(`held_bytes=1069583421 budget=1073741824`), so openraft fed the new copy entries from index one
instead of a snapshot. In eight minutes it fed fewer than about 110,000 entries. The snapshot came
only when the retention budget forced a purge past the copy's position, and by then the window was
nearly spent.

## Cause

`GroupStore::try_get_log_entries` served an entry that had left the cache with one `read_at` on
its segment, and awaited each read before issuing the next. The shard's WAL interleaves every
group on the shard, and the cache is a small byte-bounded tail shared by all of them. So a member
catching up from the start of a large log is fed almost entirely from the files: openraft asks
for up to 300 entries a call, and that was 300 I/Os in a row.

On an idle executor, each such read is a few microseconds from the page cache. On a shard serving
a bench, each completion waits for the reactor's next turn behind every task that is runnable. The
cost of a call is then 300 turns, not 300 reads.

## Evidence

**Measured, then reproduced on the lab.** `experiment_uncached_log_read_rate`
(`shoal-core/src/server/wal/tests.rs`, ignored, run by hand) appends 6,000 entries of 2 KB to
each of twelve interleaved groups behind a cache too small to hold any of them. It then reads one
group back 300 entries a call while a second task keeps the executor busy in 200 µs slices:

| Tree | Entries read | Time | Rate |
| --- | --- | --- | --- |
| Before, one read per entry | 6,000 | 601.3 s | 10 entries/s |
| After, one read per run of a segment | 6,000 | 4.9 s | 1,221 entries/s |

The competing task is always runnable, which is harsher than a real shard. So the absolute figures
are this test's own, but the ratio is the mechanism. On the lab the same reader fed a copy fewer
than 110,000 entries in 480 s, while the keyword groups, whose logs were mostly cached, caught up
52,000 to 58,000 entries in 3 to 4 s.

## The fix

The reader collects the uncached entries of a call into runs: consecutive indexes in one segment,
with no cached entry between them. It reads each run with as few `read_at`s as the span allows
(`ShardWal::read_entries`). One read covers the first frame's start to the last frame's end,
other groups' frames between them included. It is capped at `MAX_READ_SPAN` (4 MiB), and each
frame is decoded at its offset in the span. A run's frames are in append order, and every uncached
frame is durable, so every byte of the span is too. Twelve groups interleaved and 300 entries of a
few kilobytes each fit in one read.

## Alternatives rejected

- **Issue the reads concurrently.** 300 reads in flight would complete in one turn, but still cost
  300 submissions and completions for bytes that lie next to each other.
- **A bigger cache.** It would help a member that is a little behind. A new copy is a whole log
  behind, and no cache holds a whole log for every group on a shard.
- **Always feed a new copy a snapshot.** That is still worth doing where the log is far larger than
  the set (below), but a copy a few thousand entries behind should be fed the log, and it should
  be fed quickly.

## Invariants to uphold

- **A span ends at a durable frame.** A run holds only uncached entries, and an entry leaves the
  cache only once it is durable. A span that ended at a frame still in the writer's buffer would
  read bytes that are not there yet.
- **Entries come back in index order.** A cached entry ends a run, and the run is flushed before
  the cached entry is pushed.
- **A span is bounded.** Other groups' frames between ours are read and thrown away. The cap keeps
  that to a few megabytes a read, never a segment.

## Still open

- **A set moved onto a new copy is still fed its whole retained log** when the log has not been
  purged. That is up to `retained_bytes` (1 GiB) a group, where a snapshot of the set is a tenth
  of that on the lab. Filed in [todos](../todos.md#feed-a-new-copy-a-snapshot-when-its-log-is-larger).

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_lagging_read_returns_every_entry_across_spans_segments_and_the_cache` (`shoal-core/src/server/wal/tests.rs`) | A coalesced read returns an entry out of order, from the wrong offset, or loses one across a span cap, a segment or a cached entry |
| `experiment_uncached_log_read_rate` (same file, ignored) | Run by hand: the rate falls back to one entry per reactor turn |

## Related

- [#171](failed-group-publishes-its-set.md), which the failed move exposed next.
- [F40](../../features/replication.md), the shared WAL.
- [F45](../../features/replica-migration.md), the move's catch-up window.
