# F130. A loop's steps counted by kind, their own cpu beside

## Context

[M23's exit](../future-work/scans/m23-exit.md) found about one scan step in 460 holding its shard's
loop past W7's 100 µs. Nine in ten were steps of `Keyword`'s large resident partitions
([item 350](../appendix/known-issues.md#350-a-scans-steps-of-large-resident-partitions-hold-their-loop-past-100-µs)).
The exit could not say what held them. [F123](walk-and-scan-counters.md)'s counters counted a page's
capture and an encode step alike, by the wall alone, and kept no long step's bytes or rows. Item 350
has three candidates, and each needs a different fix: the rows bound an encode step learns from other
partitions' steps, a piece's own work, and a page's capture, which looks at every resident key of its
tablet. And a step can pass 100 µs without doing 100 µs of work. A shard's io_uring workers run on
its own cpu and preempt whatever it is doing
([O147](../appendix/optimizations.md#o147-each-shards-io_uring-workers-run-on-the-shards-own-cpu)).

So the user decided, on the exit's record, that item 350's steps are counted by kind before it is
fixed. The step clause is read on a step's own work, and a step past 100 µs by the wall alone is
named as O147's, which follows M23
([decision 2](../future-work/scans/m23-exit.md#decided-with-the-user-2026-10-31)). This is that
count, the first box of [what is left of M23](../future-work/scans/whats-left-todo.md#finishing-m23).

## What it does

**Every step a walk page holds its loop for is counted by its kind**, a class's figures each:

| Kind | The step | Its keys | Its rows | Its bytes |
| --- | --- | --- | --- | --- |
| `captures` | A page's capture: its tablet's resident keys looked at, its own selected, each planned or captured, its archive's view taken (`Shard::read_page`) | Every resident key of the tablet | What it copied: none for a planned page, a captured page's whole partitions | Copied off the loop |
| `encodes` | An encode step: a planned partition looked up again and a piece of it, the whole of it or its archive's bytes taken (`Shard::encode_step`) | One | A piece's rows, a whole partition's live rows, an overlay's rows; none for one shared or a shadow | Copied off the loop |

Each kind is a `StepFigures`:

| Figure | Counted |
| --- | --- |
| `all` | Every step: how many, their wall time and own cpu in nanoseconds, the keys they looked at, the rows and bytes they copied |
| `long` | The same for the steps past 100 µs by the wall |
| `past` | Steps past 100 µs, 1 ms and 10 ms by the wall |
| `past_cpu` | Steps past each on their thread's own cpu |
| `longest` | The longest step by the wall since the node started, whole: its wall, own cpu, keys, rows and bytes |

**A step's own cpu is its thread's cpu clock across the step.** A step's clock (`StepClock`,
`shoal-core/src/server/step_clock.rs`) reads the wall, then the thread's `CLOCK_THREAD_CPUTIME_ID`,
at the start, and the cpu, then the wall, at the stop. So the cpu's interval lies inside the wall's,
and it is clamped to it as well. A step past a bound on its own cpu is past it by the wall too, so
the steps past 100 µs by the wall alone, whose thread was off its cpu for part of them, are
`past[0] - past_cpu[0]` (`StepFigures::past_by_wall_alone`).

**The figures from before stay as they were.** `steps`, `steps_past` and `longest_step_us` count
every step by the wall in whole microseconds, as F123 does, from the same sample. So
`captures.all.steps + encodes.all.steps` is `steps`, and every capture of the first exit compares
with a run of this build. A bound is judged in whole microseconds by both.

**A long step says what held it at DEBUG**: its kind, class, table, tablet, wall and own cpu in µs,
keys, rows and bytes. It names a table and a tablet, never a key.

The figures reach the node, a member's report, the admin `Stats` read and a bench capture's server
samples as F123's do. `shoaladm stats` draws one more figure in its storage group, *scan steps past
100 µs on their cpu*, beside *scan steps past 100 µs*, which keeps reading the wall.
`shoal-spike-scan/results/m23-report.py` prints a table of steps by kind for each arm and member:
past 100 µs by the wall and on its own cpu, and what a long step held beside every step of its kind.
On captures that count kinds it reads the step clause on own cpu, with the steps past it by the wall
alone beside it as O147's. On the first exit's captures it reprints the record unchanged.

A standalone node's shard answers `ReadVerb::Walks` with its figures, for a test in process.

## Design choices

- **The two loop steps W5 and W7 name, and nothing else.** W7's clause is "no step of a scan on a
  shard's loop longer than 100 µs", and W5's loop steps are a page's selection and view and its
  encode steps ([W5](../future-work/scans/tablet-reader.md#what-it-costs)). The scan queue's work, a
  page's merge and F127's folds, shares the executor but is judged by the cell's p99 (below).
- **The thread's cpu clock, read twice a step.** It is the only source that tells a step's own work
  from its thread being off its cpu, to the nanosecond, as the step ends. Two reads cost a Zen1 host
  1.25 µs a step (Performance).
- **What a step held, beside its time.** Item 350's candidates differ in what a long step holds. A
  capture's keys grow with its tablet's resident partitions. An encode step's rows and bytes show a
  piece that outran its learned rows bound, or one whose rows cost more than its table's. So the long
  steps' mean keys, rows, bytes and own cpu are read against every step's of the same kind.
- **The longest kept whole.** Its time alone says nothing of its cause, so its own cpu, keys, rows and
  bytes are kept with it, and a node's figures take the whole sample from the shard whose step was
  the longer.
- **Nanoseconds for sums.** Steps are tens of microseconds, and a floor of whole microseconds a step
  would bias a mean by a tenth. The bounds are judged in whole microseconds, as F123's are, so the
  two sets of figures agree.
- **A failed read counts the step's cpu as its wall.** No step is taken for a preempted one without
  the evidence of it. `clock_gettime` on this clock does not fail on Linux.
- **The rows bound still learns from the wall.** Item 347's `EncodeCosts::learn` takes a step's wall
  time, as it was decided. Whether it should take a step's own cpu is item 350's to say once a run has
  named the step.
- **A failed capture is a step too.** It held the loop all the same. F123 counted none, but a
  capture fails only when a resident partition cannot be serialized.

## Alternatives rejected

- **`getrusage(RUSAGE_THREAD)`**, cheaper at 0.34 µs a read on europa and 0.40 on titan. It reports a
  thread's time as of its last scheduler event, up to a tick behind, and in whole microseconds: no
  use for a step of tens of microseconds.
- **The scheduler's statistics** (`/proc/thread-self/schedstat`): a file read a step, several
  microseconds, and what M23's exit sampled every 30 s for that reason.
- **A hardware counter read in user space** (`perf_event_open` and `rdpmc`): tens of nanoseconds, but
  it needs `perf_event_paranoid` low enough for it on every host, which Ubuntu's default is not.
- **Reading the stop's cpu only past the bound**, to halve the cost. It leaves every short step's
  own cpu unread, so a kind's mean cpu would mix two meanings. Filed as
  [O152](../appendix/optimizations.md#o152-a-steps-own-cpu-read-only-past-its-bound).
- **Timing the scan queue's steps.** A unit of the merge awaits archive reads between its yields, so
  its clock would have to restart at every await, or count an I/O wait as its wall. Filed in
  [todos.md](../appendix/todos.md#the-scan-queues-steps-timed).
- **A histogram of step times.** A count past three bounds subtracts across a run, which a capture
  needs. A histogram a shard would cost a bucket's lookup a step for a figure the step clause does not
  read.
- **Changing `steps`, `steps_past` or `longest_step_us` into kinds.** 317 tracked captures, the
  report and the fixture read them as integers, and a peer on the other build would fail to parse the
  whole member report.

## Limitations

- **The scan queue's steps are not timed**: a page's merge and its folds run between yields in the
  scan queue, on the same executor, and hold its cpu as long. They are judged by the cell's p99, not
  by a step's bound.
- **A capture's clock covers its `walk_tablet`**: the checks before it (`walk_host`, `walk_copy`) and
  what follows it, a budget draw, a map insert and the read task scheduled, are outside it. glommio's
  `spawn_local_into` schedules the task in its own queue and never polls it inside the call, so what
  follows is a few lookups. The second inbox's whole step is timed for its pacing (`IdleSteps::ran`)
  and reported nowhere.
- **A step's own cpu is the loop thread's alone**: work the step hands to another thread, the
  kernel's included, is not in it, and the cpu its io_uring workers take on the same cpu is what the
  wall alone shows.
- **Nothing names the step's partition**: the event names a table and a tablet, and the figures are
  sums. A test that needs the partition reads the page's own counts (`WalkPage::encode_steps`,
  `largest_step`).
- **The stats view draws five walk figures**; the kinds are in `NodeStats` and a capture.

## Invariants to uphold

- **A step's clock is started and stopped on its loop's thread with nothing awaited between**: a
  thread's cpu clock is its own, and a step that awaited would count its wait as its wall.
- **A step's own cpu is never more than its wall**, read inside it and clamped, so
  `past_by_wall_alone` never underflows and `past_cpu` never passes `past`.
- **Every step is counted once, among every step and among its kind's, from one sample**, so the
  figures before F130 keep their meaning and equal the kinds' sums.
- **No existing field changes its type**, and every new one is `#[serde(default)]`: a build on either
  side of F130 reads the other's figures, and a capture from before reads as no step of either kind.
- **The figures stay `Copy` and `Eq`**: integers and fixed arrays alone, since a shard's report is
  copied and skipped when equal to the last.

## Performance

`CLOCK_THREAD_CPUTIME_ID` has no vDSO path: each read is a system call, which brings the thread's
time up to date as it reads it. Measured 2026-11-01, two million reads pinned to one core, a release
build for znver1:

| Host | Thread cpu clock, a read | `Instant::now`, a read | `getrusage(RUSAGE_THREAD)`, a read |
| --- | --- | --- | --- |
| europa (Ryzen 9, kernel 7.0.0-38) | 0.45 µs | 17 ns | 0.34 µs |
| titan (Zen1, kernel 7.0.0-34) | 0.62 µs | 30 ns | 0.40 µs |
| hyperion (Zen1) | 0.62 µs | 30 ns | not run |

Two reads a step: 0.9 µs on europa and 1.25 µs on a Zen1 host. That is about 1% of W7's 100 µs a
step, and 2.5% of the 50 µs an encode step is sized to. At the exit's rates, 5,300 to 12,000 steps a
capture of minutes on a member, it is a fraction of a millisecond of a core a second. An encode step
is taken for a resident partition's piece and a capture for a page, so a scan of archived partitions,
which W12's cost is judged on, pays it once a page. Nothing is added to a query's path. The plan
moved the read behind a setting past 1 µs a read on a Zen1 host, which it does not reach.

A shard's report and a member's figures carry 92 more integers: 23 a kind, two kinds, two classes.
A shard sends its report only when it changed, and a member's figures cross to the leader every
`STATS_EVERY_REPORTS` reports.

## Tests

| Test | Proves |
| --- | --- |
| `a_step_is_counted_by_its_kind_wall_and_own_cpu` (`shoal-proto/src/shared/protocol/stats.rs`) | A step is counted among its kind's past each bound by the wall and by its own cpu, the long ones summed apart, the longest kept whole and taken whole from the shard whose step was longer, the figures before F130 unchanged, a bound judged in whole microseconds, and a class's figures from before read as no step of either kind |
| `a_threads_own_cpu_leaves_out_its_sleep` (`shoal-core/src/server/step_clock.rs`) | A step that spins is on its own cpu as long as its wall says; one whose thread sleeps holds its wall with almost none of it on its cpu |
| `a_step_is_counted_by_its_kind_with_its_own_cpu` (`shoal/tests/scan_encodes.rs`) | A scan's page and a driver's each count one capture and an encode step a piece, the capture's one key and no row, the steps' every row once and the bytes the class copied, each kind's cpu inside its wall; steps spun 300 µs on their cpu (`faults::stretch_steps`) are past 100 µs both ways, steps slept 1 ms past it by the wall alone, the longest kept whole |
| `a_nodes_walk_pages_are_counted_by_class` (`shoal/tests/cluster_fixture.rs`) | A cluster node's figures, through the admin `Stats` read, count a capture a page that looked at every resident key of its tablet, an encode step a note with its one row, and the steps of both kinds summing to every step |
| `a_nodes_walk_figures_are_its_shards` (`shoal-core/src/server/control/stats.rs`) | The control thread sums each kind over its shards, the longest kept whole |
| `a_samples_walk_figures_round_trip` (`shoal-loadgen/src/results.rs`) | A capture's sample keeps the kinds and reads them back as written |
| `every_metric_reaches_the_view` (`shoaladm/src/cluster/stats/view.rs`) | The stats view draws the steps past 100 µs on their own cpu |

## Related

- [F123, a node's walk and scan counters](walk-and-scan-counters.md), whose step figures these split
- [Item 350](../appendix/known-issues.md#350-a-scans-steps-of-large-resident-partitions-hold-their-loop-past-100-µs),
  which these count, and [O147](../appendix/optimizations.md#o147-each-shards-io_uring-workers-run-on-the-shards-own-cpu),
  whose preemptions a step past 100 µs by the wall alone is
- [F124](scan-page-planned.md), [F126](encodes-in-steps.md) and [item 347](../appendix/resolved/encode-steps-by-rows.md),
  the capture and the encode steps counted
- [M23's exit](../future-work/scans/m23-exit.md) and [what is left of M23](../future-work/scans/whats-left-todo.md#finishing-m23)
