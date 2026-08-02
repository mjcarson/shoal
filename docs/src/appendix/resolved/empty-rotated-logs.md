# 14. Empty rotated intent logs were never deleted

Filed from reading the compactor, then confirmed by reproduction while building the test for
[item 31](multi-log-recovery.md) — that test had to work around the leftover this defect leaves
in order to stage a log of its own.

## Symptom

A zero byte `Shard-N-inactive-1` sitting in a table's intent directory after a clean shutdown.
Nothing broke: the file is opened on the next startup, read as an immediate end of log, replayed
as nothing, and deleted by recovery. The cost was a lost signal rather than lost data. An
inactive log on disk is supposed to mean *a compaction was interrupted* — the log was rotated out
and the process died before the compactor consumed it — and it did not mean that, because the
most ordinary sequence there is, starting a server and stopping it, produced one every time.

## Cause

`FileSystemCompactor::compact_intent` removed the log it had just consumed from inside the branch
that had something to consume:

```rust
let partitions = if self.changes.is_empty() {
    // this log had nothing to compact so there is nothing to write
    Vec::default()
} else {
    self.load_partitions_for_intents().await?;
    self.apply_intents().await?;
    let partitions = self.write_partition().await?;
    // delete our no longer needed inactive intent log
    glommio::io::remove(path).await?;
    partitions
};
```

The removal is the last statement of the `else` arm. A rotated log that yielded no changes took
the other arm and was left on disk.

Empty rotations are not an edge case — they are what a quiet table does. Startup forces one
unconditionally (`.../tables/persistent/sorted.rs:218`), so a table nobody wrote to still hands
the compactor a zero length log on every restart, and `compact_if_needed`
(`.../storage/fs.rs:397-440`) rotates on `force` without looking at whether the active log has
anything in it.

The accumulation was bounded by an unrelated piece of code rather than by design: recovery
deletes every inactive log it finds once it has replayed them (`.../storage/fs.rs:522-529`), so
each restart cleared the previous leftover and created its own. One file was on disk at a time,
not one per restart — but it was on disk from every shutdown to the next startup, and it was
always the wrong signal.

## Evidence

**Reproduced before the fix** by `empty_rotated_intent_logs_are_deleted`
(`shoal/tests/persistent_sorted_table.rs`). It writes one row in a single-shard session, then
runs two more sessions that write nothing, and asserts the intent directory holds no
`*-inactive-*` file afterwards. Against the unfixed tree:

```
thread 'empty_rotated_intent_logs_are_deleted' panicked at
shoal/tests/persistent_sorted_table.rs:753:5:
A compacted intent log outlived its compaction:
["/home/.../target/tmp/.tmpOisb2d/TestRecord/intents/Shard-0-inactive-1"]
```

One leftover rather than three, which is the recovery sweep above doing the clearing. The
generation is `1` because each session's recovery removed the previous session's file before that
session rotated its own. (The `:753` is where the assertion stood at that run; it moved up when
the workaround came out of `multi_log_recovery_keeps_earlier_intents`.)

The earlier note on this — a zero byte `Shard-0-inactive-1` seen after three start/stop cycles
while building [item 31](multi-log-recovery.md#still-open)'s test — was the same observation
made from the other side.

## The fix

The removal moved out of the `else` arm and now runs on both paths:

```rust
let partitions = if self.changes.is_empty() {
    if truncated { /* warn, see below */ }
    Vec::default()
} else {
    self.load_partitions_for_intents().await?;
    self.apply_intents().await?;
    self.write_partition().await?
};
// delete our no longer needed inactive intent log, which is safe for both arms
// above: write_partition syncs everything it wrote before returning, and a log
// we compacted nothing from has nothing left to make durable
glommio::io::remove(path).await?;
```

It sits before `send_mark_evictables`, where it already was in the non-empty arm, so the order
"make durable, delete the log, then announce the generation" is unchanged.

Removing unconditionally does not add a failure mode for a missing file. `sort_intent_log` opens
the log through `IntentLogReader::new`, which `dma_open`s it read-only
(`.../storage/fs/reader.rs:34-50`) and errors before the removal is reached if the path is gone.

There is a second, smaller change. A log can also produce no changes because its *first* record
was damaged — the reader treats a torn or corrupt entry as the end of the log and sets
`truncated` (`.../storage/fs/reader.rs:22-28`). That case now deletes the file too, so
`sort_intent_log` returns the reader's `truncated` flag and `compact_intent` warns when it is
discarding a log it could read nothing from. Without it, "empty because nothing was written" and
"empty because we could not read it" would be the same silent path.

## Alternatives rejected

**Delete only when the log is zero length.** The narrow reading of the symptom: remove the file
when `size == 0` and leave anything else alone. It makes the rule depend on where the damage
starts — a log whose second record is torn is already deleted by the non-empty arm, and has been
since before this fix, so keeping a log whose *first* record is torn would be an inconsistency
with no rationale behind it. Both are logs the reader has decided end where the damage begins,
and recovery makes the same decision about the same bytes.

**Leave the delete where it is and have recovery sweep empty logs at startup.** Recovery already
deletes what it replays, so this is nearly free to write. It also fixes the wrong half: the file
would still be created by every shutdown and still be on disk in between, so "an inactive log
exists" would still not mean "a compaction was interrupted" for anyone looking at a stopped
server — which is when someone looks.

**Skip the rotation entirely when the active log is empty.** The most direct answer to "why is
there a log at all" — have `compact_if_needed` return early on `force` if the active log has
nothing in it. Rejected as a much larger behavioural change than the defect warrants: the
generation counter, `FlushProgress.rotated`, and the `MarkEvictable` that tells a table how far
its data has been compacted are all keyed off the rotation happening, and a forced rotation that
sometimes does not happen would need every one of them re-examined. It stays a rotation; it just
cleans up after itself now.

## Invariants to uphold

- **A rotated log is deleted only after everything it held is durable.** In the non-empty arm
  that is `write_partition`, which syncs `self.writer` and `self.map_writer` before it returns —
  the removal must stay *after* that call, not merely after `apply_intents`. The empty arm is
  safe because there is nothing to sync, not because deletion is cheap.
- **The removal happens before `send_mark_evictables`.** That message is what tells the table its
  generation is compacted, and a generation must never be announced durable while the log holding
  it is still on disk waiting to be replayed by a recovery that would apply it twice.
- **An inactive intent log on disk means an interrupted compaction.** This is now true again, and
  [Recovery](../../storage/recovery.md) states it as a diagnostic. Anything that reintroduces a
  routine leftover — a new early return, an error path that skips the removal — breaks a claim
  that page makes, not just this one.
- **A log the reader could read nothing from is still deleted, and still warns.** The two are a
  pair: the delete keeps the invariant above, the warning keeps the deletion from being silent.

## Still open

**A forced rotation of an empty log still happens at all.** The file is now cleaned up, but the
rename, the new active log, the intent compaction job and the `CompactionJob::Archives` behind it
are all still paid on every restart of a table nobody wrote to — and that last one is
[O9](../optimizations.md#o9-every-intent-log-rotation-walks-the-entire-on-disk-partition-set)'s
walk of the whole on-disk partition set. Filed as
[O21](../optimizations.md#o21-a-forced-rotation-of-an-empty-intent-log-does-the-whole-rotation-anyway),
with why suppressing the rotation is a larger change than suppressing its cleanup.

**The compactor still cannot distinguish a log it read nothing from because it was empty from one
that was zero length on disk.** `truncated` covers damage, not the difference between "no records
written" and "file never grown". Nothing currently needs that distinction.

## Tests

| Test | Fails without |
| --- | --- |
| `empty_rotated_intent_logs_are_deleted` (`shoal/tests/persistent_sorted_table.rs`) | The whole fix. Three sessions leave a `Shard-0-inactive-1` behind that no compaction consumed |
| `multi_log_recovery_keeps_earlier_intents` (`shoal/tests/persistent_sorted_table.rs`) | Nothing directly, but it stops working around this: it used to assert the pre-existing leftover was empty before staging its own log over it, and now stages onto a clean directory |
| `update_intent_replay`, `delete_after_restart` (`shoal/tests/persistent_sorted_table.rs`) | The ordering half. They pin that a log holding real intents is still deleted only after its partitions are written and synced |

## Related

- [Multi-log recovery](multi-log-recovery.md) — item 31, whose reproduction turned this up
- [Compaction](../../storage/compaction.md#5-delete-the-log)
- [Recovery](../../storage/recovery.md)
- [Test Coverage](../test-coverage.md)
