# 350. A scan's steps of large resident partitions held their loop past 100 µs

## Symptom

On [M23's exit](../../future-work/scans/m23-exit.md)'s rounds (2026-10-31), steps of `Keyword`'s scans
held their shards' loops past W12's 100 µs a step, 90 to 208 times a capture on europa and up to 48 on
the Zen1 hosts, the longest 254 to 539 µs. `Keyword` is a sorted table of 171-byte rows, written into
after a restart by a paced stream of inserts. The steps that passed were where its resident partitions
were large, and [item 347](encode-steps-by-rows.md)'s bound, a step's rows learned from its table's
steps, was meant to keep every one of them near 50 µs.

## Cause

**An encode step's rows were bounded by a cost learned from steps that cost less a row.** Since item 347
each shard learns what a row of a table costs its encode steps (`EncodeCosts`) from every step of eight
rows or more, and a piece takes as many rows as 50 µs holds at that cost. Two kinds of step teach it, and
they cost different amounts a row:

- a first step that fits takes its partition whole and uncopied (`capture_loaded`): rkyv encodes the
  partition's rows where they lie;
- a piece of a larger partition clones each row into a fresh map, walks every row again to count its
  size (`recount`), encodes the clones and drops them, all inside the step.

The lab's figures fit it, though they do not say which steps were whole. A long step of `Keyword`'s took three to five times the rows of its
arm's mean step, and spent about twice the cpu a row: on europa 0.6 to 0.7 µs a row against 0.28, on
the Zen1 hosts 2.5 to 3 against 1.1 to 1.4. The count let them take that many because it was learned
from steps that cost under half as much a row, which is what a table whose small partitions are taken
whole teaches. A count learned that way bounds a step only while every step costs what the steps that
taught it cost.

No better count fixes it: a cost learned from other steps says nothing of the next piece's rows, which a
larger or colder partition makes costlier.

## Evidence

**Reproduced**, on the lab and in process.

The run that named it: the exit's own rounds, four of them, on F130's build (263a17fc), whose figures
count a page's captures apart from its encode steps, each with its own cpu and what a long step held
([F130](../../features/loop-steps-by-kind.md)). `shoal-spike-scan/results/m23-i350/`, read by
`m23-report.py`:

| Arm (four rounds) | Member | Encode steps past 100 µs, wall / own cpu | Every encode step: rows, own cpu | A step past 100 µs: rows, own cpu |
| --- | --- | --- | --- | --- |
| `Keyword` alone | europa | 118–211 / 111–204 | 3–73 rows, 2–20 µs | 148–232 rows, 138–161 µs |
| | hyperion | 22–40 / 19–32 | 15–21, 16–21 µs | 62–68, 153–201 µs |
| | titan | 20–27 / 17–22 | 10–15, 14–17 µs | 57–65, 141–189 µs |
| The cell beside a scan of `Keyword` | europa | 0–6 / 0–5 | 33–36, 10–11 µs | 208–231, 120–196 µs |
| | hyperion | 9–24 / 7–19 | 12–17, 16–19 µs | 55–63, 129–247 µs |
| | titan | 6–17 / 3–10 | 5–13, 12–17 µs | 26–60, 112–126 µs |

Captures of `Keyword` passed 100 µs at most once an arm. Europa's mean step in the first round's arm
alone was 3 rows, over 281,813 steps of mostly small partitions; in the others, 68 to 73.

Item 350's steps were encode steps, held on their own cpu, with three to five times the rows of their
arm's steps.

`an_encode_step_ends_once_its_rows_have_held_the_loop` (below) says a row costs a nanosecond, as a cost
learned from cheaper rows would, and makes every row a piece copies cost 5 µs of its loop's own cpu.
Against the unfixed tree:

```text
thread 'an_encode_step_ends_once_its_rows_have_held_the_loop' panicked at shoal/tests/scan_encodes.rs:342:5:
a step copied 63128 bytes
```

The same run printed the page's figures: 7 encode steps, all 7 past 100 µs on their own cpu, the longest
60 rows and 63,128 bytes at 492 µs of its own cpu, a piece bounded by its bytes alone. Fixed, three runs
of it took 100 steps of 4 rows each, none past 100 µs on its own cpu, the longest 60 to 65 µs in a
debug build.

**Confirmed on the lab** (2026-11-01): the same four rounds on the fix's build (`72c14316`), every
host identical to its snapshot after (`shoal-spike-scan/results/m23-i350-fixed/`):

| Arm (four rounds) | Member | Encode steps past 100 µs on own cpu, before | After | Rows of every step, after | Rows of one past 100 µs, after |
| --- | --- | --- | --- | --- | --- |
| `Keyword` alone | europa | 111–204 | 35–52 | 51–62 | 69–91 |
| | hyperion | 19–32 | 10–23 | 10–19 | 35–39 |
| | titan | 17–22 | 11–26 | 12–17 | 28–36 |
| The cell beside a scan of `Keyword` | europa | 0–5 | 1–3 | 19–44 | 91–146 |
| | hyperion | 7–19 | 1–7 | 13–19 | 28–37 |
| | titan | 3–10 | 0–3 | 7–14 | 2–18 |

A long step holds 1.3 to 1.5 times the rows of its arm's steps on europa and 2 to 3.5 times on the Zen1
hosts, where it held 3 to 6.5 times: a step that holds more rows is longer, and so likelier to meet
whatever else holds its cpu. What is left is read
against the light steps every arm holds
([item 353](../resolved/capture-allocations.md)),
as steps past 100 µs on their own cpu a second of the time spent in steps, over the four rounds:

| Member | `Keyword` alone's encode steps, before | After | `Big`'s light encode steps, after |
| --- | --- | --- | --- |
| europa | 364 | 158 | 89–100 |
| hyperion | 65 | 40 | 17–38 |
| titan | 68 | 42 | 20–27 |

On the Zen1 hosts `Keyword`'s steps now pass 100 µs as often as the light steps do. On europa about
1.6 times as often remain, steps of 69 to 91 rows that cost about ten times the cpu a row that every
step costs: not rows outrunning a count, and nothing a copy's deadline cuts. Closed on this, decided
with the user (2026-11-01), the rest read with item 353.

## The fix

**A piece's copy ends once it has held its loop its share of a step**, read as it is made rather than
from a count learned elsewhere:

- `Shard::encode_step` gives the piece a deadline, `ENCODE_COPY_TIME` (25 µs) from the step's start by
  the wall (`StepClock::began`, `PieceAsk::until`). Half of `ENCODE_TIME`, since the encode and the drop
  of the copied rows after it cost about as much again and cannot be cut.
- `SortedPartition::piece` reads the clock every fourth row it copies (`PIECE_CLOCK_ROWS`) and ends the
  piece once the deadline has passed: at least one row, and at most three past the deadline. A piece
  bounded to fewer rows never reads it.
- **The whole path, which cannot be cut, is capped at 64 rows** (`ENCODE_ROWS_WHOLE`) as well as the
  learned rows and 64 KiB. A larger partition is taken as pieces under the deadline, and a first piece
  that took every row is handed out as a whole capture is, so its page folds nothing more.
- **A piece is counted as it is copied.** Its size is the heap of its copies, summed as they are made,
  and its nodes, which is what `recounted` counts: the second walk of every row is gone. The copies are
  measured, never the rows they came from, since deepsize counts a string's capacity and a clone's
  capacity is its length.
- **A step teaches a row's cost by its own cpu**, never its wall: a preempted step did no more work a
  row, and the deadline guards the wall. `EncodeCosts` stays, a ceiling beside the deadline.

`faults::slow_piece_rows` makes every row a piece copies cost a time on its thread's cpu, for a test.

## Alternatives rejected

- **A better learned cost**, a cost for whole steps and another for pieces: each still says nothing of
  the next piece's rows, and a partition colder or wider than the ones that taught it outruns either.
- **The deadline on the thread's own cpu**: `CLOCK_THREAD_CPUTIME_ID` is a system call a read, 0.45 to
  0.62 µs, where the wall is 17 to 30 ns. A preempted copy ends early by the wall, the safe direction.
- **Reading the clock every row**: about 20 ns a read, an estimated 4 to 13% of a small row's copy by
  item 347's figures, for a bound three rows tighter.
- **The pieces encoded straight from the partition's rows, uncloned**: rkyv lays a map out from its
  length, which has to be known before the first row is written, so the encode still could not be cut,
  and the piece's format would change under every fold.
- **The whole path removed**: most partitions are small, and taking them whole costs about half what a
  piece costs a row.

## Invariants to uphold

- **Every piece takes at least one row, and ends at the first of 64 KiB by its own count, its learned
  rows, or `ENCODE_COPY_TIME` since its step began**, the clock read every `PIECE_CLOCK_ROWS` rows.
- **A step takes a partition whole and uncopied only within 64 KiB and the fewer of its learned rows and
  `ENCODE_ROWS_WHOLE`**: it is the one step no deadline cuts.
- **A piece's deadline is set from its own step's start**, on its loop's thread, with nothing awaited.
- **A piece's size is what `recounted` counts of its rows**, from its copies.
- **The cursor moves strictly forward**: a piece's `last` is none only when no row lies past it.
- **A row's cost is learned from a step's own cpu**, and only from a step of eight rows or more.

## Still open

- **Light steps past 100 µs on their own cpu**: steps of one row or a capture of a few keys passed 100 µs
  on their thread's cpu 0 to 14 times an arm, in every arm. These hosts' kernels charge the interrupts a
  cpu takes to the thread it interrupts; each of titan's shards' cpus takes about 1,300 NVMe interrupts a
  second, and one of them every NIC receive softirq
  ([item 353](../resolved/capture-allocations.md)).
  Attributed by item 353's six rounds (2026-11-02): they are the steps' own work, not those interrupts.
- **Europa's `Keyword` steps still pass 100 µs about 1.6 times as often as its light steps**, 35 to 52 a
  round alone, each of about 70 to 90 rows at ten times the cpu a row of every step. ~~Not attributed:
  item 353's attribution reads them beside the light steps, decided with the user (2026-11-01).~~
  Attributed by item 353's six rounds (2026-11-02): of 262 that passed on their own cpu, 72 took page
  faults, four each, and 190 none, so they are mostly the steps' own work, not interrupts.
- **A row whose own copy passes the budget is still a step of one row**, as item 347 left it.
- **A large partition takes more steps**, each a message through its loop
  ([O145](../optimizations.md#o145-a-planned-partitions-encode-step-is-a-message-through-its-loop-each)).

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `an_encode_step_ends_once_its_rows_have_held_the_loop` (`shoal/tests/scan_encodes.rs`) | With a learned cost of a nanosecond and rows that cost 5 µs each to copy, a 400-row partition takes pieces of at most eight rows, none past 100 µs on its own cpu; by its bytes and learned rows it took sixty a piece, 7 steps, every one past 100 µs on its own cpu, the longest 492 µs |
| `a_piece_ends_at_its_deadline_on_every_fourth_row` (`shoal-core/src/server/tables/partitions.rs`) | A piece past its deadline ends at its fourth row and the next goes on after it; one bounded to three rows never reads the clock; none or a far deadline takes every row |
| `a_pieces_size_is_its_rows_recount` (same) | A piece of rows whose strings hold spare room is counted as its copies' recount, smaller than the rows it copied |
| `an_encode_steps_rows_are_learned_from_its_tables_steps` (`shoal-core/src/server/shard/walk.rs`) | Item 347's row, kept: a table's steps take 32 rows until a step of eight or more says a row's cost |
| `an_encode_step_takes_the_rows_its_time_holds` (`shoal/tests/scan_encodes.rs`) | Item 347's row, kept: a learned cost of 5 µs bounds a piece to ten rows |

## Related

- [Item 347, encode steps bounded by rows](encode-steps-by-rows.md), whose bound this one sits beside
- [F130, a loop's steps counted by kind](../../features/loop-steps-by-kind.md), whose figures named it
- [F126, a planned partition encoded a step at a time](../../features/encodes-in-steps.md)
- [M23's exit](../../future-work/scans/m23-exit.md), which found it, and
  [what is left of M23](../../future-work/scans/whats-left-todo.md#finishing-m23)
