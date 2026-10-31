# 346. A paced scan's reads reached their device one at a time

## Symptom

On M23's quick lab pass (2026-10-31) a scan of `Big` with nothing beside it read about 4 MiB a
second of each Zen1 host's 970 EVO, its share of the device's time binding. F128's share of 75 ms a
second was chosen to give about 32 MiB a second there ([F128](../../features/device-time-pace.md)).
Its scans had learned that a mebibyte costs the 970 EVO 18.4 ms of its time, eight times X18's 2.3 ms
([X18](../../future-work/scans/table-reads-paced.md#a-scan-alone-at-the-widest-pace-by-device)).
Beside the paced stream's synced inserts they learned 40 ms. Beside the reference cell they learned
133 to 167 ms, read 0.45 MiB a second, and took 229 s over a tenth of `Big`.

## Cause

F128 learns what a byte of a device costs from the scans' busy time on it, the time any scan read is
in flight there, over the bytes they read. So reads in flight together count once, as the device
counts its own time. `read_wave` (`shoal-core/src/server/tables/walk/reads.rs`) paced a wave's reads
one at a time: each read took its own device time from the share before it was issued.

Once the share bound, each read waited for its own credit, about two milliseconds on titan, and went
out alone. It was in flight about a seventh of a millisecond and done long before the next was due.
So no two scan reads were ever in flight together, and the busy time counted every read's whole
latency. The cost learned was one read's latency over its bytes, the most a byte of the device can
be shown to cost. That cost spaced the reads further, which kept them alone: the pace held itself at
the cost of reads one at a time.

## Evidence

**Reproduced**, in process and on the lab. `a_paced_pages_reads_go_to_their_device_together` (below)
holds a page back by its share from its first read and counts the most of its reads ever in flight on
the device (`TablePace::peak_reads`). Against the unfixed tree:

```text
assertion `left == right` failed: the page's reads reached their device 1 at a time
  left: 1
 right: 4
```

On the lab, the quick pass's churn round scanned `Big` alone, with no paced stream and no cell. On
titan its scans were busy 75.9 ms a second, the share, and read 4.12 MiB a second. They read 547 reads
a second of 8.5 KiB each, 139 µs of busy time a read, which is one read's latency on that device; the
learned cost was 18.4 ms a MiB on titan and on hyperion alike
(`target/lab/m23/quick/captures/m23-churn-10mib-scan`).

## The fix

A wave's reads are paced a group at a time (`group_of`). After its first read, a group takes the reads
behind it while it holds fewer than its device holds in flight (`scans.device_reads`, four at the fix and sixteen since [O146](../optimizations.md#o146-four-scan-reads-a-device-held-a-scan-alone-to-two-thirds-of-its-rate)) and they
fit what one take of its pace holds (`TablePace::chunk`). It takes their device time and bytes
together and then issues them together, so they overlap on the device and their busy time counts
once. A disk, one take of which holds about a read at its cost, still reads one at a time.

A read's place among its device's scan reads is now held for its read alone: its archive's open and
its release lie outside it, so neither counts as the device's time.

## Alternatives rejected

- **The cost a read paced alone shows, divided by the bound**: it assumes every device overlaps four
  reads, which a disk does not, and the reads would still go out alone.
- **A read charged its latency over the bound**: the same assumption, a disk charged a quarter of
  its time.
- **A larger share for solid state devices**: the share is the one setting every device is paced by,
  F128's choice; the defect was in what its reads showed, not in the share.
- **Groups larger than a take**: a group waits for its whole credit, so on a disk four runs of a
  mebibyte would be seconds of the disk at once, past the burst that keeps the share smooth.
- **More reads in flight a device**: four is F128's bound, X18's hint for the foreground's read tail.
  Grouping makes the overlap that bound allows real without moving it.

## Invariants to uphold

- **A paced read is never issued alone while the reads after it fit the same take and its device
  holds them in flight**: a group's reads go out together.
- **A group takes no more than one take holds**, so no read is issued past a bucket's burst (F128's).
- **A read's place among its device's scan reads spans its read alone**, so the scans' busy time is
  the device's.

## Still open

- **The cost learned beside a busy foreground is still higher than the device's own**: a group's
  reads wait behind the foreground's flushes, and the wait counts. That is F128's limitation, which
  M23's exit measures.
- A read waiting for a place still looks again every quarter of a millisecond.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `a_paced_pages_reads_go_to_their_device_together` (`shoal-core/src/server/tables/walk/reads.rs`) | A page its share holds back sends its eight reads as two groups of four in flight together; paced one at a time, the most ever in flight is one |
| `a_page_its_device_holds_back_ends_short_at_its_bound` (same file) | F128's row, kept: a page held back still ends short at its bound, its waves whole |
| `a_scan_page_keeps_its_reads_in_flight` (same file) | F125's row, kept: a page its pace does not hold keeps every read of its wave in flight before the first is answered |

## Related

- [F128, the pace in device time](../../features/device-time-pace.md), whose cost this is
- [F125, a page's reads in runs](../../features/page-reads-in-runs.md), whose waves are grouped
- [X18, table reads paced](../../future-work/scans/table-reads-paced.md), whose figures the share
  was chosen from
- [M23's exit](../../future-work/scans/milestones.md#m23-the-walk-rebuilt-for-load), which found it
- [Item 347](encode-steps-by-rows.md), found by the same pass
