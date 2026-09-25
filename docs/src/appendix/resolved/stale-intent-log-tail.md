# 148. A node could not start after a crash: its archive map's intent log held stale buffer memory past its end

## Symptom

Every node of the lab was killed with `SIGKILL` at the same moment under the mixed bench
([cluster testing](../../cluster-testing/correctness.md#kill-every-node-at-once)). europa and
titan came back. hyperion failed at every start, thirteen times, with

```text
ERROR Shard::new:PersistentTable::new:ArchiveMap::new{shard_name="Shard-1" table_name="Movie"}:
  SerializableMap::load_intent_log{intent_path="/optane/shoal/Movie/archives/intents/Shard-1"}:
  error=Rkyv(Error { inner: Failure })
Error: ShardFailed { shard: 1, error: "Rkyv(Error { inner: Failure })" }
```

Nothing acknowledged was lost: every acknowledged insert was read back through the two
survivors. A scan of the other nodes' logs, copied while they ran, found **titan's** Movie
`Shard-5` intent log damaged the same way. titan would have failed its next start too, and with
hyperion down that would have taken the cluster below quorum.

## Cause

A table's archive map keeps an intent log (`archives/intents/Shard-N`) of every change since the
map was last saved, written through glommio's `DmaStreamWriter` and synced at the end of every
compaction. A sync with a partly filled buffer flushes it with `flush_padded`, and a direct write
is a whole aligned buffer. So the bytes past the log's end, up to the buffer's end, go to disk
too. Until a clean close truncates the file to its logical end, they stay there, and a crash
leaves them.

glommio recycles DMA buffers without zeroing them. So those bytes were whatever the memory last
held. In a node that compacts, that is often another writer's archive records. An archive record
is framed exactly like a map intent, `[size][gxhash64][payload]`, so the intent reader
(`IntentLogReader::next_buff`) accepted the first leftover record past the log's end as a frame,
and its checksum passed, because the record was whole. `rkyv` then failed to read a Movie row as a
`MapIntent`, and `load_intent_log` returned the error, which failed the shard and the node.

Both damaged files show the same shape. The damage starts partway into the file's **last**
128 KiB block and runs to that block's end. The first part of the block is valid intents, and
the rest is archive records ("My Boss, My Hero … 2001-12-14", then the bench's synthetic rows).
The same records sit, correctly, in one of hyperion's archives at a different offset. So they
were copies of memory, not writes aimed at the wrong file.

## Evidence

**Found on the lab**, and taken apart from the saved file (`target/lab/t12-kill-all/evidence/`):

| File | Last block | Damage from | First foreign record |
| --- | --- | --- | --- |
| hyperion `Movie/…/intents/Shard-1` | 26, at 3,407,872 | 17,216 bytes into it | a 568 byte Movie row, checksum valid |
| titan `Movie/…/intents/Shard-5` (running) | 34, at 4,456,448 | 36,544 bytes into it | a 552 byte Movie row, checksum valid |

**Reproduced twice.** In the fork, `partial_flush_writes_zeros_past_the_position`
(`glommio/src/io/dma_file_stream.rs`) writes 100 bytes after recycling buffers filled with
`0xAB`, syncs, and reads the file before any close: *"3946 bytes past the position are not
zero"*. In Shoal, `an_intent_log_synced_but_not_closed_holds_zeros_past_its_end` does the same with
the map writer's 128 KiB buffers and buffers full of archive-framed records: *"122686 bytes past
the intent log's end are not zero"*. `a_foreign_frame_past_an_intent_logs_end_ends_it` writes
three intents and an archive record, the way the lab's file had one, and loading it failed the
way the node did:

```text
a log with a stale tail loads: Rkyv(Error { … InvalidSubtreePointer { … } })
```

## The fix

Two, one at the source and one for logs already on disk.

- **glommio zeroes a partial buffer's tail before flushing it** (`flush_padded`, fork commit
  `873fa44` on `mjcarson/glommio` `ZeroCopyDmaStreamWriter`). A file written by a
  `DmaStreamWriter` now holds zeros past its logical end whatever the buffer pool held, and a
  reader meets a zero size there, which is how a healthy log ends. The table intent log's own
  writer already zeroed its pad regions, with the comment "DMA buffers are pooled and may hold
  stale data". The archive map's writer went through glommio's, which did not.
- **A frame that passes its checksum but is no map intent ends the log** (`load_intent_log`), with
  a warning naming its position, the way a torn frame or a checksum mismatch already did. That
  lets a log written before the fix load: hyperion's and titan's stop at the first foreign
  record.

**What the zeroing costs.** It clears only the unwritten tail of a partly filled buffer, at a
partial flush: a `sync()` or a close, which for these writers is about once per compaction. A
full buffer is flushed without it. At most 128 KiB of memset precedes an `fdatasync`. On the lab,
the same benches on the same cluster, with the zeroing line removed from the fork and then
restored (`target/lab/zero-ab.sh`):

| Build | Inserts, run 1 | Inserts, run 2 | Mixed bench, all operations |
| --- | --- | --- | --- |
| Without zeroing | 22,982/s, p99 307 ms | 20,175/s, p99 295 ms | 46,818/s, write p99 417–420 ms |
| With zeroing | 22,928/s, p99 342 ms | 19,171/s, p99 356 ms | 45,159/s, write p99 412–415 ms |

The difference in throughput is within the run-to-run spread: the two runs of the same build
differ by more than the builds do, and each second run is slower because the table grew. The insert
p99 was 10–20% higher with the zeroing in both runs, which two runs cannot tell from noise. This
is recorded rather than dismissed.

## Alternatives rejected

- **Truncate the intent log on every sync.** An `ftruncate` per compaction, and it still leaves
  the window between the partial write and the truncate.
- **Zero every DMA buffer at allocation.** Correct, but it pays for every full buffer too, where
  only a partial flush writes bytes it does not own. Zeroing the tail of a partial buffer is the
  bytes at risk and no more.
- **Only the reader's defense.** It stops at a foreign frame, but not at a *stale intent of the
  same log*: a recycled buffer that last held this log's own earlier intents decodes and passes
  its checksum, and replaying it would point partitions back at old archive locations. Only the
  writer can rule that out.

## Invariants to uphold

- **Nothing a `DmaStreamWriter` flushes past its position is anything but zeros.** Any writer of a
  framed log that bypasses it has to pad with zeros itself, as the table intent log's does.
- **A map intent reader stops, never fails, at the first frame it cannot use**: a short read, a
  bad checksum, or a frame that is no intent.
- **The workspace builds against a fork commit that has the zeroing.** A glommio from upstream,
  or a fork branch without `873fa44`, brings the bug back.

## Still open

- **Logs written before the fix may hold a stale intent that decodes.** The reader cannot tell one
  from a real one: a frame carries no position or sequence of its own. hyperion's and titan's
  damage began with a foreign record, so they stopped at their logical end. Any other node that
  crashed before the fix could replay one. Frames keyed by their position would rule this out,
  filed in [todos](../todos.md).
- The archives are written through the same writer. Their tail past a crash was junk too, but
  nothing reads an archive except at an offset the map names, so it was never read.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `an_intent_log_synced_but_not_closed_holds_zeros_past_its_end` (`shoal-core`, `fs/map.rs`) | The map writer leaves recycled memory past the intent log's end (the glommio zeroing) |
| `a_foreign_frame_past_an_intent_logs_end_ends_it` (`shoal-core`, `fs/map.rs`) | A leftover archive record past the log's end fails the shard at start (the reader's defense) |
| `partial_flush_writes_zeros_past_the_position` (the glommio fork) | glommio's partial flush writes recycled buffer memory |
| Kill every node at once ([cluster testing](../../cluster-testing/correctness.md#kill-every-node-at-once)) | A node never starts again after a crash |

## Related

- [Resolved #140](intent-log-read-ahead.md), the same reader, made to read in blocks.
- [O62](../optimizations.md#o62-every-compaction-rewrites-the-shards-whole-archive-map), which
  made intent logs long: long enough that their last block was often partial.
- [F44](../../features/repair.md), the record framing both logs share.
