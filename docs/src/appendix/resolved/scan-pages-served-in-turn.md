# 355. A scan's tablet beside the cell was given up: its pages starved at their device for eleven seconds at a time

## Symptom

In item 350's confirmation run (2026-11-01, four of M23's exit's rounds on `72c14316`), the scan of
`Keyword` beside the cell failed in round two's capture
(`target/lab/m23-i350-fixed/captures/m23-r2-keyword`):

```text
** failed at 202.9s: the scan of Keyword failed: tablet 313 of Keyword: a page went unanswered for 60s
```

The bench's scan driver asks a tablet's page again after every refusal for now, and gives the tablet
up once streams have asked it for 60 s without an answer (`GIVE_UP`, `shoal-loadgen/src/scans.rs`). It
was the first tablet given up in seventeen captures of the arm since M23's exit.

## Cause

**Not the shard's admission, as this item first said.** Its record blamed the two scan pages a shard
admits at once: a tablet shed again and again while other streams took the slots as they freed. But a
tablet's minute counts only time a stream spends asking it, never its time waiting in the scan's queue,
and a shed is answered in about one round trip, or after at most 250 ms when the node's scan budget is
spent (`DRAW_WAIT`, before admission). Sheds alone hardly fill a minute.

**Pages that went eleven seconds without progress filled it.** The failed arm's figures hold six
refusals of code `Timeout`, *"titan: the shard hosting the tablet did not answer within the
deadline"*. That is the admin walk's waiter: it waits for a scan's page as long as the page moves, and
gives it up once it has made no progress for `ADMIN_TIMEOUT`, 11 s ([F128](../../features/device-time-pace.md),
`answered_while_moving`). The driver counts the whole 11 s, and 200 ms more before it asks again,
against the tablet. They fell in the arm's seconds 149, 152, 153, 180, 196 and 200. One stream holds a
tablet at a time and a timeout takes eleven seconds, so three or four of them were tablet 313's, 33 to
45 s, and its shed asks filled the rest.

**They were routine.** Every full Keyword-beside-cell capture since M23's exit but four met one to
eight, and the arm's pages took 5 to 13 s at their 99th percentile and 20 to 132 s at their longest:

| Captures (`target/lab/m23*/captures/m23-r*-keyword`, full arms) | Timeouts a capture | Page p99 | Longest page | Tablets given up |
| --- | --- | --- | --- | --- |
| 42 | 0 to 8 | 4.6 to 13.2 s | 20.7 to 131.9 s | 1 |

**A device's pace served its waiting pages in no order.** A scan page's archived reads take their
bytes and device time from their device's pace before they are issued
([F119](../../features/scan-pacing.md), F128), every shard's pages of a device together:

- **A bucket grants a take only once it holds all of it** (`Ceiling::take`, `server/devices/mod.rs`),
  and a page refused one sleeps until its take would be due and asks again. Whichever page asks first
  after a refill wins. A page asking a whole burst, a tenth of a second of its device's share, loses
  to pages taking a credit at a time: each refill they take as it comes keeps the bucket from ever
  holding the burst. It waits until they stop.
- **A read waiting for a place among its device's sixteen reads in flight polls every quarter
  millisecond** (`TablePace::read`), and a page that ends a read and starts another without yielding
  takes the place back before the waiter looks.
- **A page starved before its first partition never ends short.** A page ends short once it has waited
  a second for its device, but only once it holds a partition (`paced_out`), so a page starved at its
  first take goes silent, and its waiter gives it up.

**A page given up kept going.** Nothing told the page: it read on with nobody to answer, holding its
admission slot, its scan budget's bytes and its device's share until it ended, so the pages behind it
were shed more and starved more.

## Evidence

**Established from the run's figures and the corpus of captures, then reproduced by three tests.** The
figures are the failed capture's `measured.scan` (`waits {"Timeout": 6}`, `shed 23018`), its
per-second series (the six timeouts' seconds), and every Keyword capture's arm under `target/lab/m23*`.

Each test was run against the tree before the fix, with nothing of the fix but the test:

```text
thread 'unnamed-1' panicked at shoal-core/src/server/scans.rs:1020:17:
a whole burst waited 1.086252736s behind pages taking a credit at a time
thread 'unnamed-2' panicked at shoal-core/src/server/scans.rs:1058:17:
a read waited 495.282823ms for a place a reader kept taking back

the page was given up after 11.000610959s
thread 'an_abandoned_page_reads_nothing_and_frees_its_slot' panicked at shoal/tests/scan_admission.rs:458:9:
a page given up by its waiter still held its slot 16.01033019s later
```

In `a_large_take_is_never_starved_by_smaller_ones` the burst waited until the two pages taking a credit
at a time stopped, a second later; in `a_read_waits_its_turn_for_a_slot` the read waited until the
greedy reader stopped, half a second later. With the fix:

```text
a whole burst among pages taking a credit at a time waited 111.232654ms
a read waited 281.336µs for a place another reader kept taking back
the page was given up after 11.000616279s
abandoned 1, pages 1, refused 0, reads 0
```

**On the lab**, the user chose to fix the starvation first and confirm after (2026-11-03): four rounds of
the cell beside a full scan of `Keyword` (`KEYWORD_ONLY=1 m23-lab.sh start` at `b3a4d7ef`, 02:55 to 03:48
UTC), each on the `m23` side cluster of europa, titan and hyperion bootstrapped fresh, preloaded,
restarted and destroyed after, the rounds' own spec and the cell warmed 100 s, read against the corpus
above by `m23-i355-report.py` (the record in `shoal-spike-scan/results/m23-i355/`). Every host was
identical to its snapshot after it:

| Figure | Before the fix, 42 captures | After, 4 captures |
| --- | --- | --- |
| Pages given up for want of progress (`Timeout`), a capture | 0 to 8 | **0** |
| Tablets given up, a capture | 0 to 1 | **0** |
| A page, p99 | 4.6 to 13.2 s | **1.5 to 2.1 s** |
| A page, longest | 20.7 to 131.9 s | **2.3 to 4.1 s** |
| The longest a tablet went unanswered | not kept | **2.3 to 4.1 s** |
| Pages shed, a capture | 23,018 to 60,475 | 30,654 to 33,452 |
| A whole scan of `Keyword` | 71.5 to 132.8 s | 99.4 to 114.9 s |
| The cell's insert p99 during the scan | 24.5 to 47.0 ms | 39.9 to 41.2 ms |
| The cell's read p99 during the scan | 6.9 to 9.0 ms | 6.1 to 6.2 ms |

No page went eleven seconds without progress and none was let go; a page's tail fell to a sixth at its
99th percentile and to a thirtieth at its longest. **A tablet's longest wait was its longest page in every
capture**: the sheds a tablet met beside it added nothing a capture can see, so sheds were never what
filled the minute, which ordering the admission would have been for. A whole scan took as long as
before, within the corpus's spread, since the device's share of time is unchanged and the order only
spreads it fairly; and the cell beside it is as it was, its read p99 a little lower.

## The fix

**A device serves its scans' waiting takes and reads in the order they came** (`server/scans.rs`):

- **Its takes wait in a queue** (`TablePace::takes`, a `Turns`): a page's take enters it, and only the
  head asks the buckets. The head keeps its turn through its own waits, for its share, its ceiling, its
  trickle while a foreground read is in flight, and a chunk sized again, until all of its bytes are
  taken; the rest look again every `PACE_POLL`. A wait is then bounded by the takes ahead of it, each
  at most a burst: a tenth of a second each at the share's rate.
- **Its reads wait for a place in a second queue** (`TablePace::places`): only the head looks for room
  among the reads in flight.
- **A waiter's place is a guard** (`Turn`) that leaves the queue on every way out, from the head or
  from wherever it gave up, so no ticket holds a queue.
- `Ceiling` is unchanged: recovery's reads share it, and the order is the scans' own to keep.

**A page its waiter gave up is let go.** A scan page's progress was an `AtomicU64` its waiter watched;
it is a `PageWatch` now, its progress and whether its waiter gave it up, which the waiter marks when it
answers `Timeout`:

- **Every wait of the page looks at it**: a queued capture of a page given up is refused uncaptured, its
  bytes given back with its permit; its pace's take and its reads' wait for a place end at once
  (`PagePace::take` returns whether it took everything); its reader issues nothing more; its merge ends
  at its next step, a read alone included, with `given_up()`.
- **Its shard counts it as given up** (`WalkClassFigures::abandoned`), whatever it came to, and answers
  nobody, its slot and draws going as any ended page's do. A record it found corrupt is still
  quarantined.
- A test's hold of a page's reads (`faults::wait_walk_reads`) lets a page given up go too, as any wait
  does, which is how the third test makes a page that stopped.

**The bench says how close a tablet came** (`ScanWindow::longest_unanswered_us`, a capture's
`longest_unanswered_ms`): the longest any tablet went unanswered while streams asked it, before a page
of it was answered or it was given up. `GIVE_UP` itself is unchanged.

## Alternatives rejected

- **The item's own direction: admit a page its shard shed ahead of fresh ones.** Sheds did not fill the
  minute, and an ordered admission does nothing for a page starved after it is admitted. It is in Still
  open, to be taken up if the lab's longest unanswered time turns out to be sheds.
- **A request the bucket takes in debt** (the bucket going below empty and every take waiting its
  debt out), which orders takes without a queue. It would change `Ceiling`, which recovery's reads
  share, and a take sized for an idle device that finds it busy is given back and sized again, which a
  debt cannot undo.
- **A longer `ADMIN_TIMEOUT` or `GIVE_UP`.** Either hides the starvation and keeps the slot a given-up
  page holds.
- **Telling a page it was given up by its reply channel closing** (`kanal::Sender::is_disconnected`).
  The waiter's task drops its receiver only after it has written its answer, and a page's reads and
  waits have no sender to look at; one shared watch reaches every wait.
- **A page given up ending where it stands, mid-read.** Its reads already issued were paid for by its
  device's share and are bounded; it issues nothing more and ends at its next step.

## Invariants to uphold

- **Only the head of a device's queue asks**, its buckets or its places, and **it keeps its turn through
  its own waits**: a head that gave its turn up between a refusal and its retry would let a smaller take
  pass it, which is the starvation.
- **A waiter's place leaves its queue on every way out** (`Turn`'s drop), or a page dropped while it
  waited holds the queue for ever.
- **A page its waiter gave up issues and reads nothing more**: every take and every place it asks
  for after looks at its watch first, and a read alone is never made without its take.
- **A page given up is counted as given up, never as answered or refused**, and answers nobody.
- **The waiter marks a page given up only once it has answered `Timeout`** for it, so a page that made
  progress is never let go.

## Still open

- **Admission is still in no order.** A shard's two scan slots go to whichever page asks as one frees.
  The lab run shows no need: every capture's longest unanswered tablet was its longest page, so sheds
  added nothing to it, and the item's first direction is not taken.
- **A device's queue is polled**, every millisecond for a take and every quarter millisecond for a
  place: a handful of loads a millisecond for each page that waits, which no measurement here has seen.
- **The driver still counts a page's eleven seconds toward its minute.** With no page starved, a
  `Timeout` means a page that stopped, and the lab run met none; a tablet came within 4.1 s of an
  answer at worst.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `a_large_take_is_never_starved_by_smaller_ones` (`shoal-core/src/server/scans.rs`) | A page asking a whole burst among two taking a credit at a time waits until they stop, 1.09 s, where in turn it waits 0.11 s |
| `a_read_waits_its_turn_for_a_slot` (the same file) | A read waiting for a device's one place waits until a reader taking it back without yielding stops, 495 ms |
| `an_abandoned_page_reads_nothing_and_frees_its_slot` (`shoal/tests/scan_admission.rs`) | A scan page given up by its waiter keeps its slot and reads on, so the shard sheds the next; or it is counted as answered or refused |
| `a_waiter_gives_up_only_on_a_page_that_stopped` (`shoal-core/src/server/shard/walk.rs`) | A page given up by its waiter is not told so, or one answered is |
| `scan_pages_add_and_summarize_by_member` (`shoal-loadgen/src/window.rs`) | A window does not keep the longest a tablet went unanswered across windows |

## Related

- [F128](../../features/device-time-pace.md), the share and the waiter whose deadline counts progress;
  [F119](../../features/scan-pacing.md), the pace; [F116](../../features/scan-admission.md), the admission.
- [F115](../../features/bench-scans.md), the scan driver and its minute; [O139](../optimizations.md#o139-a-scan-driver-asks-a-shed-tablet-again-where-another-tablet-would-be-answered)
  and [O148](../optimizations.md#o148-a-scan-driver-asked-a-shed-page-again-every-2-ms-beside-pages-paced-for-tens), its sheds.
- [Resolved #346](paced-reads-alone.md), a wave's reads paced a group at a time, which a take's turn
  keeps whole.
- [Resolved #350](encode-copy-deadline.md), whose confirmation run met this.
