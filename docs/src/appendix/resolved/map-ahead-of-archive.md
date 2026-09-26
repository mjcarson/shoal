# 159. A crash mid compaction could leave the archive map naming records its archive never got

## Symptom

In the fault suite's kill-all step on the lab, hyperion failed every start afterwards, 31
restarts until it was stopped by hand:

```text
ERROR ShoalPool::ready: error=ShardFailed { shard: 1, error: "partition 10483307249282197527 of Movie could not be read for a replicated apply" }
```

Its journal showed the same archive failing well before the kill. From 22:39, 50 minutes after
that archive was last written, every compaction of shard 1 failed and was retried:

```text
ERROR FileSystemCompactor::compact_segment:FileSystemCompactor::load_partitions_for_intents:
  error=Shoal(CorruptArchive { archive: 14b89df5-…, partition_id: 18428027355126310284, expected: 0, found: 0 })
WARN  msg="A compaction failed before writing and will be tried again" table="Movie" attempts=7
```

`expected: 0, found: 0` is the short-read branch of `ArchiveMap::read_record_from`. The map
named a record past the end of its archive. The archive's length was a whole number of 4 KiB
blocks, what a DMA writer leaves when it is killed before it closes and trims its file. Earlier
the same evening hyperion had been SIGKILLed by systemd four times, when restarts outran
`TimeoutStopSec`, while loads were running.

## Cause

A compaction pass writes each partition as a record into the active archive through one
`DmaStreamWriter`, and its `MapIntent::Entry` into the map's intent log through another. It
syncs both only at the end: the archive, then the log (`write_partition`, and the same shape in
the snapshot install, archive compaction and the fault injector). A `DmaStreamWriter` writes
each buffer out as soon as it fills, without waiting for a sync. A map intent is 64 bytes, so the
log's 128 KiB buffer fills every 2,048 partitions, whatever the archive's writer has done with
the records those intents name. From the moment a map buffer lands until the archive buffer
holding its records lands, the disk holds entries naming bytes that exist only in memory. A kill
in that window leaves them.

A torn entry then does more damage than the one partition:

- **The redo cannot finish.** The crashed pass's WAL segment is still there, so the pass is run
  again after a restart. It reads each partition's archived copy to merge the log over it,
  meets the torn entry and fails, and it is retried forever. Nothing it compacts can be purged
  behind it.
- **Its fold makes it permanent.** Every compactor start folds the loaded map into the map's
  snapshot (`compact_map`), so after one restart the torn entry is in the snapshot as well as
  the log.
- **An apply that needs the partition stops the node.** A write replayed over it at the next
  start parks on the read, the read fails, and a replica that cannot read its archive fails its
  shard rather than diverge (`resume_parked`). Filed on its own as item 160, since
  [resolved](unreadable-partition-stalls-one-copy.md): the one copy now stalls and is repaired.

## Evidence

**Found on the lab, then reproduced.** `a_crash_mid_compaction_leaves_every_note_readable`
(`shoal/tests/cluster_fixture.rs`) runs one node at a factor of one and arms a new crash point,
`mid_compaction`. The point waits for the first whole buffer of a pass's map intents to land
and then exits with 137, the way a kill does. Eight thousand one-byte notes make a pass whose
intents outweigh its records, so that buffer names records still in the archive's first
buffer. Restarted, the node has to come up, read every note, and compact again with no read
meeting a torn record. Against the unfixed tree, three runs out of three:

```text
assertion `left == right` failed: a compaction read a record the map named past what was on disk:
{"checksum_failures":7,…,"unverified_reads":2049}
```

The 2,049 unverified reads are entries naming the pass's new archive, whose header had never
reached disk, so it opened as a format 1 archive with no checksums. The seven checksum failures
are short reads. Getting there took two tries. The first run of the test used 200-byte notes,
and with records larger than intents the archive's writer stayed ahead of the map's, so no
entry could be torn. The window needs rows smaller than a map intent, or a map buffer landing
before the archive's has caught up. On the lab, Movie rows are large, and what put the map
ahead there was timing, not row size.

## The fix

- **A job's map intents are staged in memory and written after the archive is synced.** The
  intent macro frames into the compactor's `staged` buffer, and `sync_job` syncs the archive,
  writes what is staged, and syncs the log. Every job ends that way: a compaction pass, an
  install, an archive compaction, a removal, the fault injector. A job that stages more than
  4 MiB (about 65,000 partitions) syncs its archive and writes them early (`bound_staged`), so
  its memory is bounded.
- **Loading the intent log skips an entry past its archive's end.** Only an older build could
  have written one, and the job that wrote it never finished, so its WAL segment is still there.
  The entry before stands and the redo compacts over it. An archive that is missing altogether
  is not judged here: that is a different failure, reported by name when the record is read.

**Found on the way, established by reading the source:** an install that failed part way
(`install_snapshot_records`) reported its error to the loop and left its `entries` and
`removals` behind, and the next job to sync would publish them. That repointed partitions to the
records of a snapshot the loop had just refused. Staging would have written its intents to disk
as well. A failed install or injected fault now resets the job (`reset_job`, which also empties
`staged`), as a compaction that fails before writing always did. No test drives a failing install
into a later compaction, so this half of the change has none of its own.

## Alternatives rejected

- **Sync the archive before each map buffer flush.** It keeps the order, but it needs a hook in
  glommio's writer at every buffer boundary, and it costs an fdatasync per 2,048 partitions for
  the whole pass rather than one per 4 MiB of intents.
- **Write the map intents after the loop but before the archive sync.** A full map buffer is
  written the moment it fills, so the order would still depend on which writer's buffer landed
  first. Staging is what makes the order unconditional.
- **Check the snapshot's entries at load too, and drop those past the end.** A snapshot keeps
  one entry per partition. Dropping it makes the partition look absent, and a replica with a
  row missing answers reads wrongly, which is worse than refusing to start. The intent log is
  checked because the entry before is there to fall back on.
- **Check every entry's checksum at load.** It catches a hole in the middle of an archive as
  well as one at the end. But it reads every archive whole at every start, and with the order
  fixed no new hole can be named.

## Invariants to uphold

- **Nothing reaches the map's intent log before the record it names is durable.** Every write to
  the log goes through `staged`, and `staged` is written only after `writer.sync()`. A new path
  that writes records and intents has to end with `sync_job`.
- **A torn entry is skipped only where an earlier entry exists to fall back on.** That is true of
  the intent log, because a job that did not finish leaves its WAL segment behind. It is not true
  of the snapshot.
- **A job that fails leaves nothing behind for the next one.** `reset_job` empties what a job
  accumulated, `staged` included, after a retried compaction, a failed install and a failed fault.
- **The crash point `mid_compaction` fires only in the fixture.** It is checked against
  `crash_point::armed()` before anything else, and waiting for a map flush is only done when it
  is armed.

## Still open

- **Maps already poisoned in their snapshot are not repaired.** hyperion's was: the torn entry
  had been folded into the snapshot by the crash-loop starts before the fix, and the node was
  rebuilt by re-bootstrapping the lab. Rebuilding one node's copy from its peers is filed in
  [todos](../todos.md#rebuild-a-node-from-its-peers).
- ~~**One unreadable partition still stops the node**, known issue 160.~~ Resolved: only that
  copy stops, and its group's leader repairs it ([Resolved #160](unreadable-partition-stalls-one-copy.md)).
- ~~**Every start creates an active archive.** `ArchiveMap::new` gives each open a new id, and a
  start that fails before it writes leaves an empty file. hyperion's crash loop left 30 of them.~~
  Resolved: the file is created by the first record written to it
  ([Resolved #161](failed-start-empty-archive.md)).

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_crash_mid_compaction_leaves_every_note_readable` (`shoal/tests/cluster_fixture.rs`) | A kill inside a compaction pass leaves map entries naming records that never reached the archive, and the redo meets them |
| `a_logged_entry_past_its_archive_keeps_the_one_before` (`shoal-core`, `fs/map.rs`) | A map written by an older build with a torn entry in its intent log points past its archive again after a load |

## Related

- [F44](../../features/repair.md), the checksummed records that turned the torn reads into
  errors rather than garbage.
- [Compaction](../../storage/compaction.md#4-write-out), the write path.
- [Resolved #148](stale-intent-log-tail.md), the intent log's own torn tail.
