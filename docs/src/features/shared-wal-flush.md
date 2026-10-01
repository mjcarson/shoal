# F60. WAL segments written directly, and one flush a device (withdrawn)

**Built, measured on the lab, and removed in round 14.** It made a whole load no faster and the
mixed bench's write tail worse, because under load the lab's devices were not bound by the WAL's
syncs. The page is kept because the measurements that led to it, and away from it, correct what
round 13 believed about a sync. The code is in `f03f938` and was removed in the change after it.

## Context

[O64](../appendix/optimizations.md#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab)
left a write-only load on the lab bound, it seemed, by the Zen1 hosts' WAL syncs. Each of a
node's six shards has its own WAL and writer, and round 13 measured each sync at 7 to 11 ms on the
970 EVOs, with six writers saturating the device at 450 to 630 syncs a second
([O64 in round 13](../cluster-testing/performance.md#o64-in-round-13-the-batches-seen)). It read
that as the device's cache flush, costing the same whatever a sync carried, and filed
[fewer WAL syncs per device](../appendix/todos.md#fewer-wal-syncs-per-device).

Round 14 measured a sync on idle titan first (`target/lab/r14/o64/flushprobe.py`, six writers of
16 KiB, 10 s a mode):

| How each writer writes and syncs | Commits a second | p50 |
| --- | --- | --- |
| Appends through the page cache, its own `fdatasync` (the WAL) | 951, 956 | 6.1 ms |
| Overwrites a file written ahead, with `O_DIRECT`, its own `fdatasync` | 2,286, 1,875 | 2.9 ms |
| The same, and one flusher's `fdatasync` covers every writer | 2,407, 2,071, 2,145, 1,854 | 2.2–2.9 ms |

At 64 KiB a write, the shared flush committed 1,264 a second and the own syncs 902. So most of an
idle sync's cost was not the device flush but ext4's journal: a sync of a file that has grown
commits the journal too, since the size is metadata the data cannot be found without.

## What it did

`cluster.replication.wal_mode` chose one of three ways a shard's WAL wrote:

- `buffered`, as before;
- `direct`: each segment created and filled with zeros up to `segment_bytes` plus 1 MiB, synced,
  before its first batch, the next one prepared in the background; each batch written with one
  `O_DIRECT` write of the whole blocks it touched, starting with the block the last batch ended
  in, and synced on its own file; a sealed segment cut to its last frame;
- `shared`: `direct`, with one device flush shared by every shard whose WAL was on the device
  (`FlushGroup`). A writer waited for a flush that started after its write completed, and issued
  one on its own file when none was in flight.

openraft's storage suite passed over both direct modes, and so did the new tests.

## Why it was withdrawn

Nine fresh clusters on the lab, whole loads interleaved (`target/lab/r14/o64/modes.sh`):

| Mode | Rows a second | Titan's device flushes a second |
| --- | --- | --- |
| `buffered` | 50,600, 47,201, 48,440 | 345–347 |
| `direct` | 54,848, 55,939, 46,559 | 1,492–1,604 |
| `shared` | 46,822, 53,936, 46,014 | 756–810 |

Within the loads' own spread. And with 40 MiB segments against the same buffered arm
(`seg.sh`):

| Arm | Load | Mixed bench | Update p99 | Titan's WAL writes, load / bench |
| --- | --- | --- | --- | --- |
| `buffered`, 40 MiB | 52,328, 54,486 | 43,165, 43,407 | 138, 144 ms | 25, 14 MB/s |
| `direct`, 40 MiB | 43,386, 47,205 | 43,558, 47,565 | 150, 172 ms | 67–87, 50 MB/s |

Three things the probe did not see:

- **The device was busy with archives, not the WAL.** Under a load titan's node wrote 114 MB/s
  to archives and 24.5 MB/s to the WAL. A flush empties the device's whole cache, so a WAL sync
  paid for the compactor's writes whatever mode it was in. The bound is
  [O79](../appendix/optimizations.md#o79-a-merge-rewrites-every-partition-it-touches-whole).
- **ext4 was already batching the syncs.** In `buffered` mode 404 syncs a second became 170
  journal commits and about 350 device flushes. In `direct`, every sync was a flush of its own.
- **Direct writes multiplied the WAL's bytes.** A batch is a few kilobytes and a direct write is
  whole blocks, so every batch wrote its partial block again, and every segment was written twice,
  once as zeros. Titan's WAL went from 25 to 67–87 MB/s, onto the device that was already the
  bound.

## Alternatives rejected

- **Keeping `shared` for devices with a volatile cache.** It was no faster on the one such device
  the lab has, and it adds a cross-core lock to the write path.
- **Recycling segments instead of writing zeros.** It saves the zeros, but a recycled segment
  holds an older incarnation's frames, and the frame has no generation to tell them apart by.

## Limitations

None: the feature is not in the tree.

## Invariants to uphold

For anything like it again:

- **A write is durable only through a flush that started after it completed.**
- **A file whose size or allocation changed needs its own sync**; a flush of another file does not
  commit its metadata.
- **Measure the device under the load the feature is for.** An idle probe measured the sync, and
  under load the sync was not what the device was doing.

## Performance

Above. No capture: the lab's figures are the reason it was removed.

## Tests

None in the tree. `f03f938` has them: the flush group's three tests, openraft's storage suite
over both modes, and `direct_segments_share_flushes_and_recover_whole`.

## Related

- [O64 in round 14](../cluster-testing/performance.md#o64-in-round-14-the-journal-not-the-flush).
- [O79](../appendix/optimizations.md#o79-a-merge-rewrites-every-partition-it-touches-whole), the
  bound it found.
- [Fewer WAL syncs per device](../appendix/todos.md#fewer-wal-syncs-per-device).
