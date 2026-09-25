# 91, 107. A compaction that failed ended the compactor, and the tests that met it were read as a get answered by a stale load

Two numbers, one page: item 107 turned out to be item 91 seen from a test that had not been
told to expect it.

## Symptom

**91.** A compaction job that failed - an archive it could not open, a log it could not read -
returned the error out of the compactor's loop and the task ended. Nothing restarted it: the
shard kept rotating intent logs and queueing jobs onto a channel whose receiver was gone, the
next rotation's send failed, the shard's loop returned that error and the shard stopped
serving, and a client with a query in flight to it waited forever. `exit` reported whichever
error ended the shard.

**107.** `a_get_whose_partition_cannot_be_read_does_not_hang` and
`a_get_whose_archive_is_missing_does_not_end_its_shard` in
`shoal/tests/persistent_unsorted_table.rs` failed about one run in four at six threads with the
first get's error - `PermissionDenied` or `NotFound` on `Opening` an archive - after the archive
had been put back. The item read that as a second load of the same partition, requested before
the first one's failure was delivered, answering the get issued after the restore.

## Cause

Both tests make a table's whole archive directory unreadable or empty, get a row, put the
directory back, and get the row again. Under the pressured configuration they run with, the
intent log rotates every four kibibytes, and whether a rotation - and the compaction it queues -
lands inside the fault window depends on timing. When one does, the compactor's
`load_partitions_for_intents` opens the archive of a partition the log touched, meets the fault,
and the job fails; the loop ends; and the `pool.exit()?` at the end of the test returns the
compactor's error, which `TestError::Server` prints exactly like a failed get. The gets were
fine. The sorted twins of the same tests had been told to tolerate the error at their exit and
print it, naming item 91, since [Resolved #58](pool-readiness.md) made `exit` honest; the
unsorted twins were not, and item 107 was filed off their output.

## Evidence

**Reproduced, and the reading corrected by a trace.** With `eprintln!`s on every load request,
reply, park and release, the failing run of `a_get_whose_partition_cannot_be_read_does_not_hang`
shows one request and one reply per get, the second get's load answering `ok=true`, and the
test failing all the same - at `pool.exit()`, not at the get. The two tests together at six
threads failed ten of twenty runs; the whole binary, three of twelve. One run in twelve met a
third shape: a glommio task panicking at `dma_file.rs:214` on `statfs(path).unwrap()` after
an open that had succeeded, which left the load without a reply and the first get hanging to
its twenty-second timeout - the `.unwrap()` is glommio's and is filed below.

**The fix reproduced on purpose.** `a_compaction_that_meets_an_unreadable_archive_is_tried_again`,
in both table test files, archives a partition, makes the archives unreadable, writes to that
partition again and rotates the log behind it, waits a second, puts the archives back, and
waits for the rotated logs to be compacted. Against the tree at `939873f` the test never
finishes: the compactor dies on the rotation's job, the next rotation's send closes the shard,
and the writes after it never return - killed at a hundred and fifty seconds. With the fix
the logs are gone within a few hundred milliseconds of the restore, both rows of the sorted
partition are read, and the exit is clean. The unsorted binary then ran fifteen times at six
threads with three failures and the sorted one four times with one, every one of them the
glommio panic above and none the compactor's; with the fork's `statfs` replaced by `fstatfs`
on the descriptor (item 113), fifteen and four more with none.

## The fix

A job now says whether it can be tried again. `compact_intent` and `compact_segment` are
split at the point where they first write: sorting the log, reading the frames and loading the
partitions the intents name fail as `JobFailure::Retry`; applying, writing, removing the log
and marking evictables fail as `JobFailure::Fatal`. `compact_archives` copies live records
forward and re-points the map after each archive, so a failure part way leaves dead bytes in
the active archive and nothing lost, and is `Retry` whole. A cut and an install answer the
shard with their own outcome and only the channel to it can fail. The loop keeps a list of
retries with a backoff doubling from a tenth of a second to five, warns on each failure with
the attempt count, clears the accumulators a half-finished sort left behind, and takes new
jobs meanwhile - so one unreadable archive holds up its own log and nothing else. A `Fatal`
failure ends the compactor as before, and the shard's exit reports it. The startup fold, which
runs before any shard serves, still stops the start on a log it cannot read.

The two tests that filed 107 are unchanged; the sorted twin's tolerance at its exit is gone.

## Alternatives rejected

**Retry every failure.** A job that failed after writing has a partially written archive or a
map entry not yet re-pointed behind it, and running it again blind duplicates or loses what
it wrote. The "old complete or new complete" rule [C7](../../distributed/failover.md) sets for
snapshot installation is what a retry after the write would need, and it is not built; the
boundary is drawn where nothing has been written, which is where every failure seen so far was.

**Leave the log for the next rotation to pick up.** The next rotation queues its own log; the
failed one would sit until a restart's fold. A retry list is the same thing with a clock.

**Have the rotation notice a dead compactor and say so.** True but secondary: a compactor that
does not die on a transient fault has no death to notice, and a fatal one is reported through
the shard's exit.

**The generation-keyed loads item 107's fix direction named.** They fix a defect the trace shows
does not exist: every load is requested once and answered once, and a failure releases only
the queries parked on it.

## Invariants to uphold

- **A `Retry` failure has written nothing.** The boundary in `compact_intent` and
  `compact_segment` is the first write; a read added after it is `Fatal`, and a write moved
  before it makes the retry unsafe.
- **`reset_job` runs before every retry.** `changes` and `loaded` are filled as the sort and the
  load go; a retry on top of a half-filled `changes` counts every intent twice.
- **Retries are not persisted.** A shutdown drops them; the logs are on disk and the next
  start's fold compacts them, which is why the fold does not retry.
- **The tests that filed 107 assert a clean exit.** A compactor error at `pool.exit()` is a
  regression of this page, not a flake to tolerate.

## Still open

- A job that fails after writing still ends the compactor and, through the next rotation, the
  shard. That is the harder half item 91 named and is left on the known issues page as its
  remainder.
- glommio's `DmaFile::open_at` unwrapped `statfs(path)` after a successful open; a directory
  whose permissions change between the two panicked the task that opened the file, which is a
  load that never answers. Four of nineteen runs here once the compactor stopped dying. Fixed in
  the fork's working tree by `fstatfs` on the descriptor and filed as item 113 until the fork
  commits it. ~~Open~~ [Resolved #113](glommio-open-statfs.md): the fork committed it.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_compaction_that_meets_an_unreadable_archive_is_tried_again` | `shoal/tests/persistent_unsorted_table.rs` | The test never finishes: the rotation after the death closes the shard and the writes hang |
| `a_compaction_that_meets_an_unreadable_archive_is_tried_again` | `shoal/tests/persistent_sorted_table.rs` | The same, and the merge: two rows in one partition, one archived before the fault |
| `a_get_whose_partition_cannot_be_read_does_not_hang`, `a_get_whose_archive_is_missing_does_not_end_its_shard` | both table test files | Fail at `pool.exit()` about one run in four at six threads with the compactor's error |

## Related

[Resolved #38, #58, #88](pool-readiness.md), the honest `exit` that surfaced this;
[Resolved #16, #51](partition-load-failure.md), the load failure path the item misread;
[C7. Failover](../../distributed/failover.md), the atomic install rule a retry after the write
would need; [F36](../../features/cluster-harness.md), which filed 91.
