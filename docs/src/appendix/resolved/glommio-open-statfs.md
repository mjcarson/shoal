# 113. glommio's `DmaFile::open_at` unwrapped `statfs` after a successful open

## Symptom

A storage test that makes a table's archives unreadable (`UnreadableArchives`) panicked a loader
task inside glommio, once in twelve runs of `shoal/tests/persistent_unsorted_table.rs` at six
threads. The query parked on that load waited out its deadline instead of being answered.

## Cause

`DmaFile::open_at` in the glommio fork opened the file through io_uring and then called
`statfs(path).unwrap()` on the same path, a separate synchronous call. A directory whose
permissions change between the two makes the second fail with `EACCES` on a path the first just
opened, and the `unwrap` panics the task.

## Evidence

**Established by running it**, while [Resolved #91, 107](compaction-retry.md) was being traced:

```text
thread 'unnamed-17' panicked at .../glommio/glommio/src/io/dma_file.rs:214:32:
called `Result::unwrap()` on an `Err` value: EACCES
```

then `a_get_whose_partition_cannot_be_read_does_not_hang` failed on its first get's twenty
second timeout.

## The fix

`fstatfs` on the descriptor just opened, with its error returned the way the open's is. It cannot
disagree with the open. The change sat uncommitted in the fork's working tree from the day it was
found. It was committed during the [distributed cluster testing](../../cluster-testing/overview.md)
chapter as fork commit `4c004c2` on `mjcarson/glommio` `ZeroCopyDmaStreamWriter`, beside the fix
for [#148](stale-intent-log-tail.md).

## Alternatives rejected

- **Map the `statfs` error without changing the call.** It would still ask a second question of a
  path that can change between the two calls, just failing instead of panicking.

## Invariants to uphold

- **An open that succeeded never fails on a second look at its path.** Anything asked about an
  opened file is asked of its descriptor.

## Still open

- Nothing.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_get_whose_partition_cannot_be_read_does_not_hang` (`shoal/tests/persistent_unsorted_table.rs`) | Intermittently, a loader task panics in glommio and the get waits out its deadline |

## Related

- [Resolved #148](stale-intent-log-tail.md), the other fork fix committed with this one.
