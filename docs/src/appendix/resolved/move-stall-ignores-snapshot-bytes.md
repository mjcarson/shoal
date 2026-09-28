# 189. A move whose snapshot was still streaming was reported stalled

Filed and fixed in one change, from round 14 of the lab testing
([the byte retention](../../cluster-testing/correctness.md#the-byte-retention)).

## Symptom

During a rebuild with every node's snapshot streams throttled to 2 MiB/s, the sources logged
`a move's destination has made no progress` 18 times in one arm and 6 in the other, each after
30 s, while the snapshot the warning named was still arriving at the destination:

```text
WARN msg="a move's destination has made no progress" group=7fb65afaf41d46d9 stalled_secs=30
     sent=1452 accepted=1451 conflicts=1 last_acked=None snapshot_bytes=62914560
```

Every one of those steps finished, and each group installed once. The warning is the one
[#174](snapshot-cut-queue.md) added to explain a real stall, so a false one sends the reader of a
journal after a stall that is not there.

## Cause

`catch_up` (`shoal-core/src/server/shard/migrate.rs`) judges progress by the destination's matched
log index. A destination being sent a snapshot matches nothing until the snapshot is installed,
since the snapshot is what gives it an index to match. So a transfer longer than `STALL_REPORT`
(30 s) was reported as a stall for as long as it streamed. The loop already read the bytes sent to
the destination every poll, to charge them to the move's record, and did not count them.

## Evidence

**Established by running it**, on the lab (`target/lab/r14/tb/slow-*`). A 45 to 60 MB cut at the
throttle's share of 1 to 2 MiB/s takes 30 to 60 s. Every warning named a group whose
`snapshot_bytes` was still growing and whose install followed.

## The fix

The bytes sent to the destination count as progress too. A stall is reported only when neither the
matched index nor the bytes sent have moved for `STALL_REPORT`.

## Alternatives rejected

- **A longer `STALL_REPORT`.** A snapshot at a terabyte a node takes minutes whatever the report
  interval, and a real stall would be reported later.
- **No report during a snapshot.** A snapshot stream can stall too (a receiver refusing its begin
  under `concurrent_streams`, or a link down), and bytes that stop moving are exactly that.

## Invariants to uphold

- **Progress is anything that reaches the destination**: an index matched, or snapshot bytes
  sent. A report means neither moved.
- **The report changes nothing it reports on.** The step's deadline is `migration.timeout`, and
  a stall report never fails a step.

## Still open

Nothing.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| The lab rebuild at a throttled stream (`target/lab/r14/tb/run.sh`), counting `made no progress` | Steps reported stalled while their snapshots stream |

No unit test: `catch_up` is driven by a group's live replication metrics, and the fixture's move
tests would need a throttle long enough to outlast a 30 s report interval, which is longer than any
of them runs.

## Related

- [#174](snapshot-cut-queue.md), which added the report.
- [#188](forced-purge-outruns-snapshot.md), found in the same runs.
