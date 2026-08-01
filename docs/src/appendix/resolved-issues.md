# Resolved Issues

Defects that have been fixed, kept because the reasoning behind a fix is worth more than
the fix itself. Each page states what went wrong, why, what shape the fix took, what was
rejected on the way, and — the part that matters when changing this code later — the
invariants the fix depends on.

Item numbers are shared with [Known Issues](known-issues.md) and never reused, so a number
appears on exactly one of the two pages. The exceptions are items 9, 20, and 24, which were
only partly fixed and appear on both: the fixed half here, the open remainder there.

| # | Issue | What fixed it |
| --- | --- | --- |
| 1–3 | [Acknowledged writes were not durable](resolved/durability.md) | A rewritten intent log: real commit positions, a contiguous flush watermark, `fdatasync` before acknowledgement, block aligned writes |
| 4 | [Unsorted updates and deletes never consult disk](resolved/unsorted-disk-consultation.md) | Both now park on `blocked` and replay once the partition is loaded, and an unsorted delete leaves a tombstone behind |
| 5 | [Deleted rows came back](resolved/resurrected-deletes.md) | `MapIntent::Remove` for pruned partitions, and a real flushed generation so a tombstone is never evicted before its delete has been compacted |
| 6 | [Memory accounting collapsed to zero on a partition load](resolved/memory-accounting.md) | A merge recomputes its size instead of inheriting the archive extent's, and the signed adjustment stopped going through a cast to `usize` |
| 7, 10 | [`limit` was ignored by persistent sorted tables](resolved/sorted-limit.md) | One limit-aware scan shared by both partition forms, counted against accumulated rows; queries narrowed per shard instead of broadcast whole; and the splitting shard merges the shares so a limit spans shards |
| 8 | [Sort keys were accepted and ignored](resolved/sort-keys.md) | Both scans seek their named keys instead of walking — in memory and in an archive — `exists` moved onto the same shared pair, and the keys are sorted and deduplicated once as a query enters the server |
| 9 | [Recovery and compaction panicked on orphaned update intents](resolved/orphaned-update-intents.md) | `apply_intents` seeds from the archive copy, and both sites warn and skip instead of panicking |
| 20 | [The storage tests were entirely commented out](resolved/storage-tests.md) | Rewritten against explicit on-disk fixtures and turned back on |
| 24 | [A bad query could leave the terminal in raw mode](resolved/shoalctl-panic.md) | The parse error is rendered instead of panicking past `ratatui::restore()` |
| 26, 39 | [A multi-partition get answered in an arbitrary order](resolved/partition-order.md) | `IN` and same-field `OR` replaced an `AND` that meant three different things; rows are slotted per partition on each shard and reordered by the coordinator before the limit is applied |

## How a fix gets written down

Not every bug earns a page. These do because each one had a wrong mental model behind it, and
a page that only said "fixed in commit X" would let the same mistake back in. The sections are
always the same:

**Symptom** what was observable. **Cause** the code as it was. **Evidence** how it was
established — read, or reproduced, and the difference is stated. **The fix** and **Alternatives
rejected**, together, because a fix is only understandable next to what it is not.
**Invariants to uphold**, which is the section to read before changing the code it describes.
**Tests**, naming what fails if the fix is reverted.
