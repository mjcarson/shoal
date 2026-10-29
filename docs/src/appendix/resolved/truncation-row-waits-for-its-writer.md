# 332. `truncated_bytes_never_return` raced its writer with a 400 ms sleep

`truncated_bytes_never_return` (`shoal/tests/objects.rs`) holds S3's P13: a stripe committed past a
cut, by a writer that read the object's size before the truncate, is never shown after any later
extension. It holds a writer between its stages and its commit (`faults::hold_stager`), truncates
the object under it, grows the object back, and reads zeros where the cut was.

Found in the flaky-test plan by reading the row (2026-10-28), then reproduced.

## Symptom

The row failed once in F109's object suite run (`F109o`), and the run's output was filtered to its
summary, so its message was not kept ([Tests That Fail Under Load](../flaky-tests.md#left-the-page)).

## Cause

The row's truncate followed its writer by a 400 ms sleep and nothing else. A writer that had not
reached its stager's hold by then, on a host whose load scheduled it late, read the size after the
cut and wrote past it. Its write grew the object to where it ended, as a write past the end does
([S7](../../object-storage/write-path.md)), so its bytes were rightly in the object, and the row
called them returned.

## Evidence

**Established by reading the row, then reproduced**: the writer started 600 ms late, past the
row's sleep, as a loaded host would start it. On the row before:

```text
thread 'truncated_bytes_never_return' panicked at shoal/tests/objects.rs:1598:5:
the bytes a truncate cut never return
```

With the fix and the writer still 600 ms late, the row passes.

## The fix

- **The fault counts the stagers it holds**: every hold of a stager runs through
  `faults::hold_this_stager`, counted as held by a guard its end drops, and `faults::stagers_held`
  reads the count. The put, the write, the small write's and the erasure coded write's holds all
  go through it.
- **The row truncates once its writer is counted held**, its size read and its stages done, never
  after a time. It gives the writer 30 s to be held, and says so if it never is.

## Alternatives rejected

- **A longer sleep.** It moves the race and keeps it, and the row would take longer for nothing.
- **Hold the writer before it reads the size.** The row is about a writer that read the size before
  the cut. A writer held before its read would read the cut size, and prove nothing.

## Invariants to uphold

- **A row that races a held stager waits for the stager to be counted held**, never for a time.
- **A held stager is counted by a guard**, so one cancelled in its hold is let go too.

## Still open

Nothing.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `truncated_bytes_never_return` (`objects`) | A writer a loaded host starts late writes after the cut, and the row calls its bytes returned |

## Related

- [S3's P13](../../object-storage/contract.md), the property the row holds.
- [Tests That Fail Under Load](../flaky-tests.md), where the row's failure was recorded.
- [Resolved #250, the rest](empty-groups-move-without-a-slot.md), whose held puts are counted the
  same way (`faults::feeds_holding`).
