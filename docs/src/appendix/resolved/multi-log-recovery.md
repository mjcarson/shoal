# 31. Multi-log recovery discarded already-replayed intents

Found while planning [item 9](orphaned-update-intents.md), by asking whether orphaned update
intents could be *prevented* rather than merely counted. They cannot, but the question exposed
that the prescan meant to prevent them was itself losing data.

## Symptom

A row written before a crash, silently absent after the restart. No error, no warning, nothing
in the logs — the shard started cleanly and simply did not have the row. Reads returned the
partition's older archive contents as though the newer writes had never been acknowledged.

## Cause

`FileSystem::read_intents` replayed each intent log with its own scan pass, one log at a time:
every inactive generation in turn, then the active log. Each `replay_intent_log` ran its whole
**scan** pass and then its whole **replay** pass, so from the second log onward the scan pass
ran *after* an earlier log had already been replayed into the partition map.

That scan pass loaded unconditionally:

```rust
// wrap this partition as being accessible
let wrapped = MaybeLoaded::Accessible(partition_read);
// load this partition
partitions.insert(partition_key, wrapped);
```

A partition an earlier generation had replayed into as `MaybeLoaded::Loaded` was therefore
overwritten by the copy read back from the archive the moment a later log contained an `Update`
naming it. The archive predates those intents by definition — that is why they were still in a
log — so every intent the earlier log had replayed into that partition was lost.

For a sorted table the loss compounds. Replaying the archive copy sets `check_disk = false`, so
the rows the earlier log inserted are not merely displaced from memory: the partition is now
marked as having nothing left to load, and they will never be read back from disk either.

The memory accounting drifted with it. `to_load` was built per *entry* rather than per log, so
two updates naming the same partition inside one log loaded and inserted it twice, adding
`partition_read.len()` to `memory_usage` both times, and the overwrite of a `Loaded` partition
never subtracted what it displaced.

**The original filing was wrong about the trigger.** It said reaching this needs two or more
inactive logs. It does not: the clobber happens between any two consecutive `replay_intent_log`
calls, and the active log is always the last of those. One inactive log plus the active log is
enough — a single interrupted compaction, not two.

~~And a clean shutdown leaves exactly that behind.~~ It did when this was written, because a
clean shutdown left an empty inactive log on disk. That was
[item 14](empty-rotated-logs.md), and now that it is fixed the state needs a real interrupted
compaction to reach. The trigger is rarer than this page originally claimed; the defect it
describes was not.

## Evidence

Reproduced, before any fix, by `multi_log_recovery_keeps_earlier_intents`
(`shoal/tests/persistent_sorted_table.rs`). The only way to leave an inactive log behind is to
interrupt a compaction, so the test does not try to: it lets real single-shard servers write two
genuine intent logs, then arranges them into the state an interrupted compaction leaves. One
directory supplies an archive holding row `first` plus an active log holding the insert of row
`second`; a donor directory supplies an active log holding an update to `first`. The first log
is renamed to `Shard-0-inactive-1` and the donor's is copied in as the active log.

Against the unfixed tree:

```
assertion `left == right` failed: Recovery dropped a row an earlier intent log replayed:
[TestRecord { partition_key: "partition_key", sort_key: "first", data: "one-updated" }]
  left: 1
 right: 2
```

The active log's update survived. The row the inactive log had inserted was gone.

## The fix

`read_intents` became four ordered phases instead of a per-log loop, and `replay_intent_log`
was split into a `read_intent_log` that only reads:

```
phase 1  read every log once, in generation order, collecting each log's entries and
         unioning the partition keys their Update intents name
phase 2  load every collected key exactly once, skipping any key already held
phase 3  replay every log's entries, in the same generation order
phase 4  delete the inactive logs
```

Three things fall out of the ordering rather than from any new check:

- **No scan pass ever runs after a replay pass**, so nothing can overwrite a `Loaded` partition.
  That is the fix — structural, not a guard.
- **Each partition is loaded once**, because the keys are a `HashSet` unioned across all logs.
  That is the memory double-count gone.
- **Inactive logs are deleted only after every replay succeeds.** Previously each was removed
  the moment it was replayed, so a crash part way through recovery could lose an entire
  generation that had been deleted but whose successors had not yet been applied. Now a crash
  mid-recovery leaves every log in place and recovery starts over.

`IntentReadSupport::scan` was split accordingly. It used to both decide what to load and load
it; it is now a synchronous `scan_keys` that only names partition keys, and the loading lives in
`read_intents` where the ordering is visible. This also removed an `async` trait method and its
`S: StorageSupport` generic parameter.

## Alternatives rejected

**Guard the `insert` with `contains_key` and keep the per-log loop.** This is the small fix and
it does stop the clobber, but it leaves the other two problems standing: the per-log double load
still charges `memory_usage` twice, and an inactive log is still deleted before the logs after
it have been replayed. It also leaves the ordering hazard in place for the next person — the
guard is a rule you have to know about, where phase ordering is a rule you cannot break by
accident.

**Read each log twice — once for keys, once for replay — to keep peak memory down.** Halves the
resident bytes and doubles the startup I/O. Intent logs are capped by
`latency_sensitive.intent_log_size` and the number of inactive logs is the number of interrupted
compactions, so the memory this saves is small and the I/O it costs is not.

## Invariants to uphold

- **No scan pass may ever run after a replay pass.** This ordering *is* the fix. Loading a
  partition writes an archive copy into the partition map, and an archive copy is by definition
  older than any intent still sitting in a log. If a future change reintroduces per-log loading
  — for streaming, for memory, for anything — it reintroduces this defect.
- **Inactive logs are deleted only after every replay has succeeded.** Recovery must be
  restartable from scratch at any point until it has finished, because a crash during recovery
  is not a rare case; it is the case recovery exists for.
- **A partition key is loaded at most once per recovery.** The keys are unioned across all logs
  before anything is loaded, and `load_scanned` skips a key already present. Both halves matter:
  the union keeps `memory_usage` honest, the skip keeps a newer copy from being replaced by an
  older one.
- **Logs are replayed in generation order, inactive before active.** `find_inactive_intent_logs`
  returns generations sorted and the active log is appended last. Phase 1 and phase 3 iterate
  the same list, so the two cannot drift apart.

## Still open

**Peak memory during recovery is now proportional to the total size of all intent logs**, rather
than to the largest one. Each log is capped by `latency_sensitive.intent_log_size` and the count
of inactive logs is the count of interrupted compactions — normally zero or one — so this is
bounded and small, but it is a real change and it is recorded in
[Recovery](../../storage/recovery.md#limitations) rather than left to be discovered.

**The loads this made batchable are still serial.** Collecting the key set before any load
happens is precisely the precondition for grouping those loads by archive file and issuing them
concurrently, which was not available while `scan` discovered keys one record at a time. Nothing
here took that opening; it is filed as
[O22](../optimizations.md#o22-recovery-loads-the-partitions-it-scanned-one-await-at-a-time),
to be done alongside [O8](../optimizations.md#o8-partitions-are-read-one-at-a-time-each-with-its-own-dup-and-close)
since it is the same work on the compaction path. The
[priority queue](../optimizations.md#the-priority-queue) makes that conditional rather than a
preference: O22 rides along with O8 at rank **B1**, behind O9, and on its own it is declined —
startup path, and the key set is normally small.

**`load_scanned` is where a partition enters the memory counter in archive bytes**, and replay
converts it to a form the counter is read back in deep size from. That mismatch is a bullet on
[item 22](../known-issues.md#22-size-accounting-inconsistencies) — this change shrank it sharply
without setting out to, since the old `scan` re-added the archive length once per update intent
rather than once per partition.

~~**A clean shutdown still leaves an empty inactive log behind.** Noticed while building the
reproduction: after `pool.exit()` a zero byte `Shard-N-inactive-1` remains on disk. It is
harmless — a zero length log reads as an immediate end of log — but it means "an inactive log
exists" is not the signal for "a compaction was interrupted" that it looks like.~~ **Fixed.**
This was the same defect as item 14, whose reproduction note it supplied, rather than a separate
item, and it is now [resolved](empty-rotated-logs.md): the compactor deletes a rotated log on
every path out, not only the one that had partitions to write. The test below no longer has to
assert the leftover is empty before staging its own log over it — there is no leftover.

## Tests

| Test | Fails without |
| --- | --- |
| `multi_log_recovery_keeps_earlier_intents` (`shoal/tests/persistent_sorted_table.rs`) | The whole fix. An update in the active log discards the row an inactive log inserted, and the partition is left marked as having nothing more to load |
| `update_intent_replay` (`shoal/tests/persistent_sorted_table.rs`) | Nothing — single log recovery was never affected, which is why it never caught this. It pins that the restructure did not break the ordinary path |
| `delete_when_not_resident`, `delete_survives_eviction` (`shoal/tests/persistent_sorted_table.rs`, `persistent_unsorted_table.rs`) | Nothing directly — they cover the load-from-archive path that phase 2 now owns |

## Related

- [Orphaned update intents](orphaned-update-intents.md) — item 9, whose planning turned this up
- [Recovery](../../storage/recovery.md#the-three-phase-replay)
- [Test Coverage](../test-coverage.md)
