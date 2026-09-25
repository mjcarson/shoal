# 140. A shard read its intent logs one direct read per field, and a long one failed the start

## Symptom

During a rolling upgrade on the lab, hyperion's node did not come back. `ShoalPool::ready` gave up
with `ReadyTimeout { ready: 5, of: 6, timeout: 120s }`, systemd restarted it, and the next start
failed the same way. The upgrade's revert to the previous program failed the same way too, so
the cause was in hyperion's data. Its shard 2 never logged a line past construction, and its
archive map intent log was 29.7 MB. The other five shards' logs were empty.

## Cause

`IntentLogReader::next_buff` read each record with three direct reads, `read_at(position, 8)` for
its size, `read_at(position, 8)` for its checksum and `read_at(position, size)` for its payload,
each an uncached device read of at least one block. A map intent log's records are tens of bytes,
so the 29.7 MB log was about half a million records and a million and a half device reads. On the
lab's Zen1 NVMe that is minutes, past the two minute readiness timeout. The same reader replays a
table's intent log in recovery (`fs.rs`) and feeds the compactor.

It was always slow. At the one mebibyte bound the map's intent log was folded at, a log was
about 50,000 device reads on every start. It became a failed start when
[O62](../optimizations.md#o62-every-compaction-rewrites-the-shards-whole-archive-map) let the log
grow to a quarter of the map before folding it.

## Evidence

**Reproduced against the unfixed reader** with `a_long_intent_log_is_read_in_blocks`: 100,000
framed map intents, with the read-ahead window set to zero, which is what the reader did before:

```text
test ...::a_long_intent_log_is_read_in_blocks ... FAILED
100000 records took 300000 device reads
test result: FAILED. ... finished in 5.58s
```

That is 5.6 seconds on the development host's NVMe for a 2.6 MB log, against 0.19 seconds with the
window.

## The fix

The reader keeps a window, the last block it read from the device, and slices every record out of
it (`IntentLogReader::read`, `shoal-core/src/server/tables/storage/fs/reader.rs`). A read outside
the window reads `READ_AHEAD` (4 MiB) from the aligned block holding the position onwards. A
short read at the end of the file is returned short, exactly as a short direct read was, so every
truncation and damage path behaves as it did. `device_reads` counts the device reads, which is
what the test asserts on.

On the lab, the rolled-out fix started hyperion 3.2 seconds after systemd did, with shard 2's log
unfolded.

## Alternatives rejected

- **Buffered (page cache) reads instead of direct ones.** The page cache would absorb the repeated
  reads of one block, but the reader would still make a syscall or an io_uring submission per
  field, and the log would pass through the page cache twice.
- **Reading the whole file at once.** It is the simplest, and it is what `SerializedMap::new` does
  with the map. But a table's intent log in recovery can be much larger than a map's, and the
  window bounds the memory at 4 MiB whatever the file is.
- **Keeping the map's intent log small instead.** That is what folding at a mebibyte did, at the
  cost O62 measures. The reader was the part that was wrong.

## Invariants to uphold

- **A slice of the window is valid only while the window is.** `ReadResult` is reference counted,
  so a record handed out keeps its block alive after the window moves on.
- **The reader reads the same bytes it did before, in the same order.** Only the number of device
  reads changed, so the truncation, padding and checksum handling depend on nothing new.

## Still open

- `ShoalPool::ready`'s 120 second timeout is a constant. A node with a genuinely long recovery
  (a large standalone intent log, or a rehome) could still exceed it, and it would then crash-loop
  the same way.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_long_intent_log_is_read_in_blocks` (`shoal-core/src/server/tables/storage/fs/map.rs`) | A long intent log takes three device reads a record, or is not read back whole and in order |

## Related

- [O62](../optimizations.md#o62-every-compaction-rewrites-the-shards-whole-archive-map), which made
  the logs long enough to show this.
- [Recovery](../../storage/recovery.md).
