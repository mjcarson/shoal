# F60. WAL segments written directly, and one flush a device

## Context

[O64](../appendix/optimizations.md#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab)
left a write-only load on the lab bound by the Zen1 hosts' WAL syncs. Each of a node's six shards
has its own WAL and its own writer, and round 13 of the cluster testing measured each sync at 7 to
11 ms on the 970 EVOs, with six writers saturating the device at 450 to 630 syncs a second
([O64 in round 13](../cluster-testing/performance.md#o64-in-round-13-the-batches-seen)). It read
that as the device's cache flush, costing the same whatever a sync carried, and filed
[fewer WAL syncs per device](../appendix/todos.md#fewer-wal-syncs-per-device) as the design
change that would move it.

Round 14 measured what a sync costs on titan before building anything
(`target/lab/r14/o64/flushprobe.py`, six writers of 16 KiB each, 10 s a mode):

| How each writer writes and syncs | Commits a second | p50 | p99 |
| --- | --- | --- | --- |
| Appends through the page cache, its own `fdatasync` (the WAL as it was) | 951, 956 | 6.1 ms | 11.7 ms |
| Overwrites a file written ahead, with `O_DIRECT`, its own `fdatasync` | 2,286, 1,875 | 2.9 ms | 6.9–8.6 ms |
| The same, and one flusher's `fdatasync` covers every writer | 2,407, 2,071, 2,145, 1,854 | 2.2–2.9 ms | 7.7–10.8 ms |

At 64 KiB a write, the shared flush committed 1,264 a second and the own syncs 902.

So round 13's reading was half wrong. Most of a sync's cost was not the device flush but the
filesystem journal: every sync of a file that has grown commits ext4's journal as well, since the
file's size is metadata the data cannot be found without. A file whose blocks were written before
it is appended to needs no journal commit. That alone was worth 2 to 2.4 times on the lab's
device. Sharing one flush between writers was worth more only for larger writes.

## What it does

`cluster.replication.wal_mode` chooses one of three ways a shard's WAL writes its segments
(`WalMode`, `shoal-core/src/server/wal/mod.rs`):

- **`buffered`**, the default: as before. Batches are appended through the page cache and each is
  synced on its own file.
- **`direct`**: each segment is created and filled with zeros up to `segment_bytes` plus 1 MiB,
  and synced, before its first batch (`direct::prepare`). The writer prepares the next generation
  in the background while the current one fills. A batch is written with one `O_DIRECT` write of
  the whole blocks it touches, starting with the block the last batch ended in, and synced on its
  own file (`DirectSegment::append`). A sealed segment is cut to its last frame.
- **`shared`**: `direct`, and the sync is one flush of the device shared by every shard whose
  WAL is on it (`FlushGroup`, `shoal-core/src/server/wal/flush.rs`). A writer whose write completed
  waits for a flush that started after it; if none is in flight it issues one, `fdatasync` on its
  own file, and every writer waiting when it ends is covered.

A batch that would write past its segment's prepared blocks is synced on its own file in either
direct mode, since a file that grows commits the journal and a flush of another file does not.

Turning a direct mode on over a WAL whose last segment holds frames moves appends to a new
generation (`ShardWal::set_mode`). A recovered segment was written through the page cache and
may end in a torn tail, so it is never appended to directly. Recovery reads zeros past a
segment's last frame as the end of it, which is where it already stopped at a torn tail, and
cuts them away at `DEBUG` rather than warning about a tear.

## Design choices

- **Zeros, not `fallocate`.** A preallocated extent on ext4 or XFS is unwritten until it is
  written, and converting it is a metadata change the journal has to commit, the cost this
  exists to avoid. Writing zeros once, off the write path, costs a segment's bytes of write
  bandwidth and one sync, and leaves blocks a later write only overwrites.
- **Never reuse a segment.** Recycling sealed segments would avoid the zeros. But a recycled
  file holds an older incarnation's frames, and a crash after a batch that overwrote only part of
  one would leave frames recovery could not tell from the current ones. The frame format has no
  generation to tell them apart by.
- **The block a batch starts in is written again.** A direct write covers whole blocks, and a
  batch rarely ends on one. Padding every batch to a block would waste half a block a sync. Writing
  the partial block again carries the bytes it already held unchanged, so a torn write of it
  cannot damage a frame already acknowledged: every sector of it holds either the old bytes or
  the same bytes.
- **Flushes shared by device, not by filesystem or node.** A device flush empties the device's
  whole cache, whatever file it was issued through. The key is the device of the filesystem a
  segment is on (`dev_major`, `dev_minor`), so shards whose WALs are on two devices share with
  their own device's shards only.
- **A failed shared flush fails every writer from then on.** Linux reports a writeback error
  once, so a later flush that succeeds says nothing about the writes the failed one covered. A
  failed sync stops the WAL either way ([#156](../appendix/resolved/wal-failure-stops-the-node.md)).
- **A mode, not a bool.** `direct` is most of the gain with nothing shared between shards, and
  `shared` is the rest. They are separate so a deployment can have one without the other.

## Alternatives rejected

- **One WAL a node, or a writer a device across shards.** The todo's shape. It moves every batch
  across cores, and it changes what a shard's WAL is for a rehome ([F47](local-rehome.md)) and for
  the crash matrix. Sharing the flush and nothing else keeps each shard's segments its own.
- **`sync_file_range` and a shared flush over buffered writes.** It writes a file's dirty pages
  without the journal. But a buffered append still grows the file, and growth is the metadata a
  flush of another file does not commit.
- **A longer commit delay.** Round 13 measured 5 ms against 2 ms: fewer syncs, and fewer rows
  a second.

## Limitations

- **Default `buffered`** until the lab measures the modes on whole loads (below).
- **Disk space.** A segment is its full prepared size while it fills, plus the next one prepared
  ahead: about 22 MiB a shard at the default `segment_bytes`, against the 10 MiB a buffered
  segment grows to.
- **Write bandwidth.** Every segment is written twice, once as zeros. On the lab that is the WAL's
  bytes again, off the path a write waits on.
- **Readers go through the page cache.** A segment is read back for a lagging member through a
  buffered handle, after direct writes to it. Linux invalidates the cached pages a direct write
  covers. If that fails, which it can only for pages mapped into memory, and nothing maps a
  segment, a reader could see a stale block, fail the frame's checksum and report it.

## Invariants to uphold

- **A write is durable only through a flush that started after it completed.** The flush in
  flight when a writer asked covers nothing it wrote (`FlushGroup::sync_with`).
- **A segment written directly was filled with zeros and synced before its first batch**, and
  a batch that would write past those blocks is synced on its own file.
- **A prepared segment is created empty.** Whatever was at its path is discarded.
- **A recovered segment with frames is never appended to directly.**
- **No lock is held across an `.await`** in the flush group, whose state every executor on the
  node shares.

## Performance

Measured on the lab in round 14; see
[the cluster testing's O64 section](../cluster-testing/performance.md#o64-in-round-14-the-journal-not-the-flush).

## Tests

| Test | What breaks if the feature is reverted |
| --- | --- |
| `shoal-core` `server::wal::flush::tests::waiters_share_the_next_flush_and_never_the_one_in_flight` | A writer is counted durable by a flush that started before its write, or waiters do not share the next flush |
| `shoal-core` `server::wal::flush::tests::a_failed_flush_poisons_the_group` | A flush after a failed one reports a write durable |
| `shoal-core` `server::wal::flush::tests::executors_on_other_threads_share_flushes` | Executors on other threads are not woken, or each flushes alone |
| `shoal-core` `server::wal::tests::data_store_passes_the_openraft_storage_suite` | openraft's log storage suite fails over a WAL in the `direct` or `shared` mode |
| `shoal-core` `server::wal::tests::direct_segments_share_flushes_and_recover_whole` | Direct segments lose a completion across rotations, are not cut to their frames when sealed, do not replay whole after a reopen, or a recovered segment with frames is appended to directly |
| `shoalctl` `deploy::inventory` and `deploy::render` tests | An inventory's `wal_mode` is not validated or not rendered |

## Related

- [O64](../appendix/optimizations.md#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab),
  the load rate this was for.
- [O61](../appendix/optimizations.md#o61-a-fast-device-syncs-the-wal-in-batches-too-small-to-fill-a-page),
  the commit delay.
- [Fewer WAL syncs per device](../appendix/todos.md#fewer-wal-syncs-per-device).
