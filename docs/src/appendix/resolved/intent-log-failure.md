# 122. A background write or fdatasync error ended the shard

*Filed by [Resolved #16](hot-path-panics.md). Item 16 had long said that a full disk panicked an
ordinary insert. That change found that the device error never reached the commit sites. It
ended the shard later, in the sweep, and this is that defect.*

## Symptom

A table's intent log that hit a device error ended its whole shard. The error might be a full
disk, an `EIO` on a write, or a failed `fdatasync`. Every client the shard was serving lost its
connection, and every query in flight on it went unanswered. This included reads of other
tables, and writes that were already durable but not yet released.

## Cause

A standalone table's intent log (`StreamWriter`, `storage/fs/stream.rs`) writes on detached
tasks: `write_helper` for a buffer, and `start_sync` for the group fdatasync. A task that fails
has nobody to return to, so it records the error in the shared `FlushState`, and the writer's
`check_error` hands it to the next caller. There were two callers, and both used `?`:

- `FileSystem::flush`, which the shard loop calls whenever its channel drains (`self.tables.flush().await?`)
- `FileSystem::compact_if_needed`, called from the table's `get_flushed` sweep (`self.handle_flushed().await?`), and also through `writer.refresh(..)?` when the sweep rotated the log

Either way, the error reached `Shard::start`'s loop as `Err` and ended the shard. Nothing
between the device and the loop knew what the failure meant for the writes waiting on the log,
so nothing could answer them. Returning the error was the only thing any of it could do.

There was a second problem underneath the first. Suppose the shard had carried on after a failed
fdatasync. The next write would start another sync, and a sync that succeeds after one that
failed does not prove the earlier bytes are on disk. On Linux, a failed writeback can clear its
dirty state, so a later fsync has nothing left to report. The watermark would then have advanced
over data nobody knew was durable.

## Evidence

**Reproduced against the unfixed sweep.** A device cannot be made to fail on demand, so the test
harness asks the writer to. `LogFault::Write` or `LogFault::Sync` makes the writer's next write,
or next fdatasync, report `EIO` from the same background task and into the same state a real
failure uses. `shoal/tests/intent_log_failure.rs` builds a shard's tables on a glommio executor
and sweeps them the way the shard does. It was run with the fix's storage changes and with the
two `check_error()?` lines and the rotation's `refresh(..)?` put back as they were:

```text
test a_failed_log_shuts_down_and_replays_its_durable_prefix ... FAILED
test a_sorted_log_that_fails_an_fdatasync_answers_what_it_cannot_know ... FAILED
test an_unsorted_log_that_fails_a_write_answers_what_it_cannot_know ... FAILED
the sweep failed, which ends the shard: IO(Os { code: 5, kind: InputOutputError, message: "Input/output error" })
the flush failed: IO(Os { code: 5, kind: InputOutputError, message: "Input/output error" })
the sweep failed, which ends the shard: IO(Os { code: 5, kind: InputOutputError, message: "Input/output error" })

a_rotation_whose_fdatasync_fails_fails_the_log_where_it_is (refresh(..)? restored):
the sweep failed, which ends the shard: IO(Os { code: 5, ... })
```

Each of those `Err`s is what the shard loop's `?` turned into the end of the shard. The
end-to-end path from there, through `ShardFailed`, to the clients' connections is the one
[Resolved #38, 58, 88](pool-readiness.md) already covers.

## The fix

**A log that fails is a failed log for good, and its table carries on without it.** The policy is
"refuse writes until restart". Reads are still served. An operator fixes the device and restarts
the server, and replay recovers what was durable.

In the writer (`stream.rs`):

- **`FlushState.failed` is sticky.** `record_error` sets it, and taking the error does not clear
  it. `StreamWriter::failed` reads it.
- **No fdatasync is issued on a failed log.** `start_sync` returns early, and `sync_blocking` refuses with `ServerError::LogFailed`.
- **A write that failed is never retired**, as before, so `written_pos` stops below it and the
  synced watermark cannot pass it.
- **A failed write wakes the shard itself** (`DataFlushed`), since the sync that would have woken
  it will never run.
- **`close` on a failed log** closes the file without writing its staged tail and without syncing.

In the engine (`storage/fs.rs`), `FileSystem` gains a `failed` flag, and `fail()` sets it and
logs the failure once at `ERROR`, naming the shard, the log path and the generation. Then:

- **`compact_if_needed`** no longer returns a writer error. It fails the log and returns
  `FlushProgress { failed: true, durable_pos, .. }`, where `durable_pos` is where the watermark
  stopped. A rotation whose `refresh` fails is handled the same way, before the compaction job
  is sent.
- **`commit`** refuses with `ServerError::LogFailed` **before staging a byte**, so the tables'
  existing `storage_write` arms answer `StorageWrite`, which stays a definite refusal. It checks
  the writer's own flag too, so a write that arrives between the device failing and the next sweep is also refused.
- **`flush`** returns `Ok` on a failed log and leaves the error for the sweep to report.
- **`compaction_due`** is false, so the shard does not sweep on every message for a log that will never rotate.
- **`shutdown`** does not sync a failed log, and returns `Ok`, because the failure was already reported.

In the tables, `get_flushed` (sorted and unsorted) handles a failed progress in two steps. It
first releases everything below `durable_pos` as usual. Then
`PendingResponse::fail_all` answers every remaining write with `ErrorCode::OutcomeUnknown`, each
in its own place in its bundle. Those writes were committed to a buffer, and some of them may
have reached the file, but no watermark will ever say which. `OutcomeUnknown` already meant "a
write may or may not have applied" for a lost peer, so no new wire code was needed.

**What it cost the success path:** one `bool` test in `commit`, one in `compaction_due`, one in
`start_sync`, and one `borrow_mut` of the flush state per write and per sync to check for an
injected fault. No capture was taken, and nothing here claims a performance effect.

## Alternatives rejected

**Rotate to a new log and keep writing.** On a full disk, the new file fails the same way. The
old log would have a hole that the compactor has to skip, and a rotation needs an fdatasync of the
old log, which is exactly what cannot be trusted any more. This option would have meant much more
code and more crash windows, in service of a device that is failing.

**Answer the writes past the watermark `StorageWrite`, as the item suggested.** `StorageWrite` is
documented, and relied on, as a definite refusal: nothing was staged, and nothing applied.
Answering it for a write that may be on disk would make a client's retry a possible second
effect. `OutcomeUnknown` already says the right thing.

**Keep syncing after a failed fdatasync, and trust the next one that succeeds.** This is the
fsync-after-error trap described under *Cause*. A clean sync after a failed one proves nothing
about what the failed one covered.

**End the shard, but answer everything first.** This is better than silence, but it takes down
every other table and every read on the shard for one table's log.

**Inject faults with `RLIMIT_FSIZE`, which gives a real `EFBIG` from the kernel.** The limit is
process-wide, so it would need a binary of its own. It also depends on the kernel honouring the
limit for io_uring writes and on the log not being preallocated. The injected fault goes through
the same background task and the same `record_error` a device error does.

## Invariants to uphold

- **`FileSystem::commit` still fails only before it stages a byte.** The failed-log check is
  above `prep`. `StorageWrite` stays a definite refusal because of this, as
  [Resolved #16](hot-path-panics.md) requires.
- **A failed log is never synced again.** A new path that calls `fdatasync` on the writer's file
  has to check `failed()` first, as `start_sync` and `sync_blocking` do.
- **A failed write never retires.** `on_complete` is called only on `Ok`. Retiring a failed
  write would move the watermark past bytes that are not on disk.
- **`fail_all` runs only after `get(durable_pos)`.** Reversing them would answer durable writes
  as unknown. That is not unsafe, but it is wrong.
- **Nothing between the writer and the shard loop returns a log's IO error.** A new caller of
  `check_error` has to route the error to `FileSystem::fail`, not `?` it.

## Still open

- **Every partition written in the failed generation stays in memory until restart.** Its stamp
  names a log that is never compacted, so `mark_evictable` never frees it. That is at most one
  log's worth of writes (`intent_log_size`). If the memory limit is already reached, it adds to
  [item 59](../known-issues.md#59-a-shard-that-cannot-free-anything-keeps-trying-on-every-message-in-silence).
- **A write answered `OutcomeUnknown` is served by reads until the restart.** It was applied to
  the resident partition before its log failed. After the restart it is there if its bytes landed
  before the hole, and gone if not. That is what "unknown" means, but a client reading its own
  write may see it disappear.
- **A replay that meets the hole stops there as if it were the end of the log.** A region that
  was never written reads as a size of 0, which `IntentLogReader` takes as a clean end of log,
  not as damage. Records that landed past the hole are dropped without being counted. Every one
  of them was answered `OutcomeUnknown`, so this is one of the outcomes the client was told it
  could not know. The recovery stats simply do not see it.
- **A compactor that is gone still ends the shard** when a rotation's job cannot be sent to it.
  That is [item 91](../known-issues.md#91-a-compaction-that-fails-after-writing-ends-the-compactor)'s remainder, and was left as `?`.
- **A cluster node's tables do not have this path.** Their writes go through the shard's shared
  WAL (`server/wal/`), which records its own failures and answers the waiting batches with them.
  What a group does after that is openraft's storage-error handling, and this change did not
  examine it.
- **A retrying client reports the retry's refusal, not the first try's unknown.** A write behind
  the failure is answered `OutcomeUnknown`, which the client retries. The retry is refused
  `StorageWrite`, and that is what the caller sees, which reads as "never applied". Filed as
  [item 125](../known-issues.md#125-a-retried-write-whose-first-try-was-outcomeunknown-reports-the-last-trys-refusal),
  since a cluster node's retries meet the same thing.
- **No operator command clears a failed log** short of a restart. It is filed in
  [To-dos](../todos.md#recovering-a-failed-intent-log-without-a-restart), with what it needs.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `an_unsorted_log_that_fails_a_write_answers_what_it_cannot_know` (`shoal/tests/intent_log_failure.rs`) | The sweep returns the write's `EIO`, which ends the shard. Without `fail_all`, the insert behind the failure is never answered. Without the commit check, a later write is staged into the dead log instead of being refused. |
| `a_sorted_log_that_fails_an_fdatasync_answers_what_it_cannot_know` | The flush returns the sync's `EIO`, and the sorted table's sweep is left unhandled. |
| `a_failed_log_shuts_down_and_replays_its_durable_prefix` | The shutdown syncs the failed log and fails. A restart that replayed past the hole would bring back the write that never landed. |
| `a_rotation_whose_fdatasync_fails_fails_the_log_where_it_is` | The rotation's `refresh` error ends the shard, or a failed rotation leaves a renamed log behind. |
| `a_failure_is_sticky_and_freezes_the_watermark` (`stream_tests.rs`) | Taking the error forgets the failure, so the next write syncs again, or a completion after the failed write moves the watermark past it. |
| `a_failed_log_answers_what_is_past_its_watermark` (`storage.rs`) | `fail_all` answers a durable write, drops one, or answers at the wrong index. |

## Related

- [Resolved #16](hot-path-panics.md), which found that the item's full-disk claim was wrong and filed this.
- [Resolved #1–3](durability.md), the watermark and group fdatasync this relies on.
- [Resolved #38, 58, 88](pool-readiness.md), how a shard that does end is reported.
- [F11](../../features/error-channel.md), the error codes: `StorageWrite` and `OutcomeUnknown`.
