# 151. A WAL segment was deleted behind a purge whose marker a crash then lost

## Symptom

On the lab titan was killed by the kernel's OOM killer (#149) under an insert load. It never
started again, sixteen times in a row:

```text
ERROR ShoalPool::ready: error=ShardFailed { shard: 4, error: "GlommioGeneric(\"tablet group
6ab1d2578ab7bbfc could not be built: building the group: when Read LogIndex(2916741): log entry
not found\")" }
```

The cluster served on the other two nodes. titan's copy of every group was unusable until the
node was rebuilt.

## Cause

A group purges its log through `GroupStore::purge`, which stages a `Purged` marker frame in the
shard's WAL batch and, in the same call, moves the group's purge point in memory (`purged` and the
index behind it). The shard's segment sweep (`sweep_segments` in `shard/groups.rs`) deletes a
sealed segment once every group with frames in it is checkpointed and purged past them, judged by
`purged_index()`: the in-memory point, **which a purge moves before its marker is on disk**.

So the sweep could delete a segment behind a purge whose marker was still in a batch the writer had
not synced. A crash in that window lost the marker but not the deletion. The restart replayed the
last marker that *was* durable, an older purge point, and the log after it began with the
entries the deleted segment had held, which were gone. openraft asked for the first entry past its
purge point, and the WAL had none.

A `SIGKILL` loses nothing a buffered write already handed to the kernel. What it loses is a batch
the writer had not written yet, which is exactly where a just-staged marker sits.

## Evidence

**Taken apart from titan's WAL** (`target/lab/t13-titan-wal/`, copied off before the node was
rebuilt), with a diagnostic that opened it as a shard does:

```text
purged    …index: 2916740
last      …index: 3025703
committed …index: 3025703
present runs past the purge point (1 runs): [(2921361, 3025703)]
generation of the first present: Some(Some(399))
```

The durable purge point was 2,916,740, the log's first surviving entry 2,921,361, and the 4,620
entries between them were in a segment the sweep had deleted. The group's checkpoint was 3,024,740,
past all of them, so nothing acknowledged was lost. The group simply could not rebuild its log.

**Tested** by `a_purge_is_durable_only_once_its_marker_is` (`shoal-core`, `wal/tests.rs`): a purge
staged and not yet synced is seen by `purged_index` and not by `durable_purged_index`, becomes
durable with its batch, and is durable again after a reopen. The crash window itself was not
reproduced in a test. The lab reproduced it, and the WAL it left behind shows it.

## The fix

Each group keeps a second purge point, **`purged_durable`**, which moves only when the batch holding
a purge marker completes. `purge` records where its marker ends (`purge_pending`), and
`complete_batch` promotes every pending purge the durable watermark has passed. A marker replayed
at open is durable by being on disk. The sweep deletes a segment only behind
`durable_purged_index()`. The staged point still drives what the store answers openraft and when
the retention budget asks for a purge. Only deletion waits.

## Alternatives rejected

- **Make `purge` wait for its marker's sync.** openraft calls `purge` on its own task, and a purge
  that waits for a sync would hold the next purge behind every batch. Deletion is the one step
  that has to wait, and the sweep runs later anyway.
- **Keep segments until the next rotation.** A delay is not a guarantee: the marker's batch can
  still be unsynced when the rotation comes.
- **Recover by skipping to the first surviving entry.** It would hide the defect and invent a purge
  point nothing recorded.

## Invariants to uphold

- **Nothing is deleted behind a purge that is not durable.** Any deletion of WAL state keyed on a
  group's purge point uses `durable_purged_index()`.
- **A durable purge point never moves backwards**, and never past the staged one.
- **The same rule applies to any marker that licenses a deletion.** A checkpoint licenses
  compaction, and it is already written and synced before it is used (`write_atomic`).

## Still open

- **titan had to be rebuilt.** A node whose log has a hole cannot recover it from its own disk. It
  is wiped and fed by its peers. There is no tool for that short of `cluster destroy` and a fresh
  node ([todos](../todos.md)).

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_purge_is_durable_only_once_its_marker_is` (`shoal-core`, `wal/tests.rs`) | A staged purge counts as durable, so a segment may be deleted behind a marker a crash loses |
| Kill every node at once, and the OOM kills of #149 ([cluster testing](../../cluster-testing/correctness.md#kill-every-node-at-once)) | A node killed at the wrong moment never starts again |

## Related

- [Resolved #104](segments-recompacted-after-restart.md) and the rotation that carries every
  group's markers into the new segment, which this fix leaves as it is.
- [F40](../../features/replication.md), the shared WAL.
- [Resolved #149](node-memory-budget.md), what killed the node.
