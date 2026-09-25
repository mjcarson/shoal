# 36. A partial intent log buffer was written only when the shard's queue drained

## Symptom

A write to a persistent table on a standalone node was answered only once it was durable. On a
shard whose queue never emptied, however, it was never written out at all until something else
did so. That was either a log rotation, after up to `intent_log_size` (10 MiB by default) of
*other* writes, or a lull. With only reads beside it, the write waited until the reads stopped.

Nothing unsafe was ever acknowledged. The defect was an unbounded acknowledgement delay for
the last writes before a lull that never came.

## Cause

A table stages each write into an aligned DMA buffer. `StreamWriter::prep` and `consume` write
the buffer out only when the next record will not fit. There was one other path, in the shard
loop:

```rust
// if we have no more messages then flush our current queries to disk
if self.shard_local_rx.is_empty() {
    self.tables.flush().await?;
}
```

Batching depends on this check: writes that arrive together share one DMA write and one
fdatasync. But it was the *only* way a partly filled buffer reached disk. A load that never let
the queue drain therefore never reached it. The writer's own `DataFlushed` wakeups are messages
on the same queue, so write traffic helped to keep the condition false.
[F23](../../features/self-sizing-staging-buffer.md) made this worse without changing its bound,
because a buffer sized for eight records can hold eight waiting clients.

A cluster node is not affected. Its shared WAL has a writer task of its own and does not wait
for the shard to go idle.

## Evidence

**Reproduced against the unfixed tree.** A flood of reads was not enough to show it on its own,
because a queue fed by real clients drains now and then. In five runs, 256 connections of gets
against one shard held back a write for 32 to 77 ms, which is long but not unbounded. After the
fix the same flood gives 24 to 76 ms. That is the same range, so under a flood the wait is queue
depth and not the staged tail.

The case the bound exists for is a queue that *never* drains. The test hook
`ShoalPool::busy_shard` produces that case: the shard handles one message that re-queues itself
behind whatever else arrived, pausing 100 µs each time, for as long as the test asks.
`a_write_behind_a_queue_that_never_drains_is_answered` keeps a single-shard server busy for
ten seconds and sends a write:

```text
a write behind a queue that never drains was not answered within 5s
test a_write_behind_a_queue_that_never_drains_is_answered ... FAILED
```

After the fix, the same write is answered in about 6 ms, which includes its fdatasync.

## The fix

**A shard writes out every table's staged writes when its queue drains, as before, *or* once
`storage.flush_interval` has passed since it last did.** The default is 1 ms.

```rust
if self.shard_local_rx.is_empty() || self.last_flush.elapsed() >= self.flush_interval {
    self.tables.flush().await?;
    self.last_flush = std::time::Instant::now();
}
```

The interval is read from the configuration once, when the shard is built. `last_flush` is an
`Instant` on the shard.

## Performance

- **The clock is only read while the queue is busy.** The `||` short-circuits, so an idle
  queue does exactly what it did before. A busy one pays one vDSO clock read per message.
- **`tables.flush()` with nothing staged costs a `buff_pos > 0` check per table**, which is
  what a busy shard now pays once a millisecond.
- **A write-heavy load is unchanged in shape.** Its buffers fill well inside a millisecond and
  are written full, just as they were. The bound only fires for a partial buffer that other
  traffic has held back, and that was exactly the case whose wait used to be unbounded.

**No capture was taken.** The change was made on the development host, whose numbers are not
committed. The grid's mixed arms at depth (`macro/grid/*/r50/*`, `r90`) are where a difference
would show, if there is one, and the capture is owed on the benchmark host. Adding
`flush_interval` to the [F20](../../features/configuration-sweeps.md) sweep is filed in
[Todos](../todos.md), not done here, because a new sweep changes every workload's fingerprint.

## Alternatives rejected

- **A bound on messages rather than time.** Flushing every N messages bounds how much *traffic*
  a staged write waits behind, not how *long* it waits, and a message can be one get or a
  bundle of a hundred. The defect was a latency, so the bound is a latency.
- **A timer armed when a table stages its first byte.** This is precise and costs nothing
  while idle, but every table's write path would have to arm and disarm it. That adds work to
  every write, where the chosen bound adds a clock read to a busy queue's messages.
- **Flushing after every message.** This removes the delay and also the batching, which is the
  reason the drain check existed. `flush_interval: 0ms` does this for anyone who wants it.
- **Writing the buffer out from the writer itself on a deadline.** The writer has no task of its
  own. It runs when the table calls it, and the table runs when the shard hands it a message.
  The shard loop is the one place that sees time pass under load.

## Invariants to uphold

- **`last_flush` is reset by every flush of the tables, whichever condition caused it.** A flush
  that did not reset it would make the bound fire on every message after a drain.
- **The bound is checked after every message, including messages that are not writes.** The
  point is that a write can be held back by traffic that is not writes. A check inside the
  write path would miss exactly that case.
- **The drain check stays.** It is what batches writes that arrive together, and the bound
  exists only for when it cannot fire.

## Still open

Nothing of this item.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `a_write_behind_a_queue_that_never_drains_is_answered` (`shoal/tests/staged_flush.rs`) | The write is not answered while the queue is busy. It waits the whole ten seconds. |
| `intent_log_batching::*` | Still passes. It guards the batching that the drain check provides, which the bound must not break. |

## Related

- [F23](../../features/self-sizing-staging-buffer.md), which made more writes wait behind this.
- [Resolved #1–3](durability.md), the acknowledge-only-when-durable rule that made this a latency and not a durability bug.
