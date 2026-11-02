# 353. Light scan steps passed 100 µs on their own cpu: the allocator, inside a capture's allocations

## Symptom

In [item 350](encode-copy-deadline.md)'s attribution run (2026-11-01, four of
[M23's exit](../../future-work/scans/m23-exit.md)'s rounds on [F130](../../features/loop-steps-by-kind.md)'s
build), every arm on every member held 0 to 14 light scan steps past W12's 100 µs that F130 counted as
their own cpu: encode steps of one 1 KiB row of `Big`, and planned captures that looked at about ten
keys, each 160 to 280 µs of cpu, the longest a capture of twelve keys at 614 µs. Their work is a few
lookups and a row. On the Zen1 hosts `Big`'s captures passed 100 µs on their own cpu 32 to 34 times in
10,000 at a mean of 14 µs, while `Keyword`'s captures on the same cpus passed 0.6 to 1 time at 11 µs.

## Cause

**No part of a capture's work: the allocator, inside the allocations a capture's step made.**

A planned capture on its shard's loop does three things: it looks at its tablet's resident keys,
keeps those of its range and selects and sorts its own (its *keys*), looks each up again and plans it
(its *plan*), and takes its archive map's view of the range (its *view*: the delta's entries copied
and a cursor made for each run). Each long capture held exactly one of these 100 to 300 µs while the
others ran at their usual one to six microseconds, the plan about as often as the view, though the
plan runs less than half as long. An event of its own size landed in whichever part was running.

Two kinds of allocator event were in it:

- **mimalloc's purge.** Every node runs mimalloc v2 (2.2.4), which hands a page of freed memory back to
  the kernel 10 ms after it empties: an `madvise(MADV_DONTNEED)` that waits for every other cpu of the
  process to flush its TLB, run inside whichever allocation or free comes next on the thread. With it
  off, the long captures the plan held all but vanished. That fits item 353's own run: windows that
  held a long step took more of the thread's page faults afterwards, and the Zen1 shards' cpus took
  12 to 26 TLB shootdowns a second against europa's 3.
- **The allocator's other slow paths**, whatever was not the purge, inside each allocation a capture
  made. A capture made a dozen or more: a vector of keys and a deque of captures grown as they filled,
  the delta's copy grown as it was collected, a vector of sources grown by its runs, a deque of ranges
  for each run's cursor. Each was a draw on the allocator's slow paths, and the view's copy of the
  delta, which allocated most, held most of the long captures left once the purge was rarer.

The kernels' lack of irq time accounting, the first suspicion, was ruled out by item 353's attribution:
the light steps passed as often on the shard whose cpu took every NIC interrupt as on the others,
moving the NIC changed nothing, and almost none of them faulted.

## Evidence

**Established by reproducing it** on the lab, M23's exit's rounds on the side cluster `m23`, each run's
hosts verified identical to their snapshot after it, and in process by the counting allocator's two
tests.

**Attributed (2026-11-02, before this fix):** six rounds on F130's build with each long step's faults
and switches and each shard's steps beside its cpu's interrupts, two of them with titan's and
hyperion's NIC interrupt moved off the shards' cpus (`shoal-spike-scan/results/m23-i353/`). The light
steps were the steps' own work; `Big`'s long captures looked at 10 to 14 keys against every capture's 9,
at 160 to 200 µs of their own cpu, none faulted. The extra was in the part of a capture past its keys.

**Its captures timed by their parts** (`47848b23`, six rounds, `results/m23-i353f/`, predictions written
first): by the part that took the most of each long capture, each part timed by the wall
(`CaptureClock`), each long capture judged on its own cpu:

| `Big`'s captures, Zen1 hosts | Captures | Past 100 µs | Keys | Plan | View | Rest |
| --- | --- | --- | --- | --- | --- | --- |
| mimalloc's 10 ms, six rounds | 55,048 | 120, 21.8 a 10,000 | 3 | 50 | 60 | 7 |

and in each one part was long: a plan of 230 µs beside a view of 6, a view of 118 beside a plan of 3.
The view copied as many delta entries (about ten) and took as many runs (about three) as every
capture's. The prediction that the view would hold most was wrong; that neither its delta nor its runs
would set a long view apart was right.

**The purge, tested** (two rounds, every node at `MIMALLOC_PURGE_DELAY=-1`, each node's environment read
from `/proc`, `results/m23-i353f-purge/`), then **the decided delay of 100 ms** with the view taken
apart (`0de7f3b5`, two rounds, `results/m23-i353f-view/` and `-view2/`), then **a capture allocating
once** (`345e17dd`, two rounds, `results/m23-i353f-alloc/`), and **its confirmation** (`345e17dd`, six
rounds, `results/m23-i353f-fixed/`), a 10,000 of the Zen1 hosts' `Big` captures:

| Build | Rounds | Captures | Past 100 µs | Held by the plan | Held by the view | `Keyword`'s, Zen1 | europa's `Big` |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 10 ms, before | 6 | 55,048 | 21.8 | 9.1 | 10.9 | 1.3 | 4.4 |
| purge off | 2 | 17,764 | 11.8 | 2.3 | 8.4 | 0 | 0 |
| 100 ms | 2 | 18,096 | 19.9 | 6.6 | 11.6, 16 of 21 its delta's copy | 0.3 | 0 |
| 100 ms, a capture allocating once | 2 | 18,881 | 12.7 | 3.2 | 6.9 | 1.0 | 0 |
| **the same, confirmation** | **6** | **57,039** | **11.4** | **2.1** | **6.8** | **0.6** | **1.2** |

The confirmation's 65 long captures against the first run's 120 are disjoint at 95%, and its plan's
12 against 50 too. The confirmation ran with the lab's 970 EVOs in their slower flush state (the cell's
inserts alone at a p99 of 40 ms, [Resolved #351](the-cells-fall-is-the-drives.md)), where item 353's own
run on the older build counted 32 to 34; the first run here ran in their faster state (25 ms). The
scan's rate beside the cell follows that state on every build, 13,200 to 14,700 partitions a second in
the slower and 20,800 to 23,400 in the faster, and `Big` alone 27,000 to 33,000 in both, so neither
the scan nor the cell paid for the fix: the cell's p99 during a scan over before it held at 1.02 to 1.10,
no member evicted, and the members' peak resident memory held within a percent and a half (0.661
to 0.653 of a node's 6 GiB beside `Big`).

**The counting allocator's tests, against the tree before the fix** (`shoal-core`'s unit binary,
`server/alloc_count.rs`):

```
test server::tables::walk::capture_tests::a_capture_allocates_alike_whatever_its_keys ... FAILED
test server::tables::storage::fs::index::tests::a_scans_start_allocates_alike_whatever_its_runs_and_entries ... FAILED
assertion `left == right` failed: a capture of 3 keys made 1 allocations, of 40 5, of 40 selected to 10 3
assertion `left == right` failed: a scan of 1 run and 2 entries made 6 allocations, of 4 runs and 200 15
```

and after it at most 2 and 4, whatever the keys, runs and entries. `every_unit_runs_its_allocator_at_a_longer_purge_delay`
failed against the renderer before it (`left: 0, right: 1`).

## The fix

1. **A capture is timed by its parts** ([F130](../../features/loop-steps-by-kind.md), extended): its
   keys, its plan, its view and its rest by the wall (`CaptureClock`, seven reads through the vDSO, about
   0.2 µs a capture on a Zen1 host), the view's delta and runs within it (`Scan::took_ns`), and each class
   counts its long captures by the part, and the view's part, that took the most (`capture_parts`). This
   is what named the allocator, and it stays.
2. **Every node's allocator purges at 100 ms**, ten times rarer than mimalloc's 10: every unit `shoaladm`
   renders runs with `Environment=MIMALLOC_PURGE_DELAY=100` (`ALLOCATOR_PURGE_DELAY_MS`,
   `shoaladm/src/deploy/unit.rs`). 100 ms is inside one of a node's 250 ms memory checks
   ([Resolved #149](node-memory-budget.md)), decided with the user.
3. **A capture's step allocates each collection once, at its size** (`PendingWalk::capture`,
   `PagedIndex::scan`): its kept keys for every key its tablet holds, its captures for as many as a page
   holds, the delta's copy counted before it is made, the scan's sources for its runs, one copy of its
   ranges shared by every run's cursor (`RunCursor`'s `Rc<[(u64, u64)]>`), and a single range kept where
   it is (`normalize`). A capture's step makes about eight allocations whatever its keys, runs and
   entries, where it made twelve to twenty and more as they grew.

The lab's script gained `NODE_ENV`, a drop-in of variables on every node's unit removed on every way
out, and every round records each node's environment (`env.txt`).

## Alternatives rejected

- **Never purging** (`MIMALLOC_PURGE_DELAY=-1`), which the lab ran to find the purge and which took
  the plan's half nearly all away. A node's budget is judged on its resident memory every 250 ms
  ([Resolved #149](node-memory-budget.md)); with no purge, memory an eviction frees never leaves the
  resident set, and a node past its budget could never evict its way back under it.
- **A purge delay of 1,000 ms**, mimalloc v3's own: past the memory check, a node could evict on about
  four checks before the memory it freed came back, evicting far more than it needed. **250 ms** sat on
  the check. Decided with the user.
- **Setting the delay in each program's `main`** through `mi_option_set`: it needs mimalloc's `extended`
  feature in every node program and the generated wrapper, and the engine cannot name the allocator. The
  unit is the one place every deployed node shares.
- **The listed bounds of the view**, a delta's copy cut at a page's worth and cursors made off the loop:
  a long view copied as many entries and took as many runs as any, so cutting the count would cut
  nothing; the allocations, not the work, held it.
- **Reading a part's own cpu** rather than its wall: three more system calls a capture, about 13% of a
  mean `Big` capture on a Zen1 host, for what the wall already says of a capture whose thread kept its cpu.
- **mimalloc v3**, whose design and purge differ: an allocator change across every node, measured by
  nothing here.

## Invariants to uphold

- **A capture's step allocates a fixed number of times whatever its tablet's keys, its map's runs and
  its delta's entries.** Every collection it makes is sized once, the delta's copy counted first, one
  copy of the ranges shared by every run's cursor. A collection grown as it fills on the loop is a draw
  on the allocator's slow paths a fill; the two `alloc_count` tests hold it.
- **Every node's unit runs its allocator at a purge delay inside one memory check, never `-1`**: a
  node's budget is judged on its resident memory, so memory it frees has to leave the resident set by
  the next check.
- **A capture's parts lie inside its step's clock**, its view's delta and runs inside its view, so a
  capture's parts and rest are its step's wall and no rest is below zero.
- **`alloc_count` is `shoal-core`'s unit tests' allocator alone**, counted on the calling thread; no
  program links it.

## Still open

- **`Big`'s captures still pass 100 µs on their own cpu about once in 900 on the Zen1 hosts**, 11.4 a
  10,000 against `Keyword`'s 0.6, 39 of 65 held by their view, its delta's one copy and its cursors,
  inside the allocations a capture still makes. The step clause reads them as the steps' own, decided
  with the user; a view that allocates nothing on the loop is
  [O154](../optimizations.md#o154-a-captures-view-allocates-nothing-on-the-loop), optional, after M23.
- **A chained partition's fragments are copied with its entry** in the delta's copy, an allocation each;
  `Big`'s have none.
- **The rare long encode steps**, about once in 10,000 on every host, are read by the step clause as the
  steps' own, decided with the user before this fix. Europa's `Keyword` residual, 69 to 91 rows at ten
  times the cpu a row, is [Resolved #350](encode-copy-deadline.md#still-open)'s.
- **A node run by hand keeps mimalloc's 10 ms** unless its environment names another; only a deployed
  node's unit sets 100.
- ~~**One preload of the view run failed** with inserts shed past every retry
  (item 357), not understood, the only one of 20 preloads across this fix's runs.~~ Fixed by
  [Resolved #357](preload-sheds-waited-out.md): a preload waits out a shed for a minute, and every
  node counts what shed its writes.

## Tests

| Test | Breaks if reverted |
| --- | --- |
| `a_capture_allocates_alike_whatever_its_keys` (`shoal-core/src/server/tables/walk.rs`) | A capture's kept keys or its captures grown as they fill: 1, 5 and 3 allocations for 3, 40 and 40 selected keys |
| `a_scans_start_allocates_alike_whatever_its_runs_and_entries` (`shoal-core/src/server/tables/storage/fs/index/mod.rs`) | A scan's delta copy grown as it fills, its sources grown by its runs, or a range deque a cursor: 6 and 15 allocations for 1 run and 2 entries and 4 runs and 200 |
| `every_unit_runs_its_allocator_at_a_longer_purge_delay` (`shoaladm/src/deploy/unit.rs`) | A unit without `MIMALLOC_PURGE_DELAY=100` in its service, before it starts, alone, plaintext or colocated |
| `a_capture_is_timed_by_its_parts` (`shoal/tests/scan_encodes.rs`) | A capture's parts not counted, or not summing to its wall, or a stretched part not the one that held it |
| `a_captures_parts_are_counted_by_the_part_that_held_it` (`shoal-proto/src/shared/protocol/stats.rs`) | The parts' figures: the rest, the part and the view's part that held a long capture, a shard's sums |
| `a_captures_parts_are_timed_where_they_end` (`shoal-core/src/server/step_clock.rs`) | A part timed from anywhere but where the last ended |
| `a_scan_counts_and_times_what_its_view_took` (`shoal-core/src/server/tables/storage/fs/index/mod.rs`) | A scan's count of delta entries and runs, or its times |

## Related

- [F130, a loop's steps counted by kind](../../features/loop-steps-by-kind.md), whose clock this takes apart
- [Resolved #350](encode-copy-deadline.md), whose runs found these steps, and
  [O147](../optimizations.md#o147-each-shards-io_uring-workers-run-on-the-shards-own-cpu), whose
  preemptions a step past 100 µs by the wall alone is
- [Resolved #149](node-memory-budget.md), the memory check a purge delay has to stay inside
- [Resolved #351](the-cells-fall-is-the-drives.md), the drives' two states every run's cell and scan follow
- [O154](../optimizations.md#o154-a-captures-view-allocates-nothing-on-the-loop), what is left
- [M23's exit](../../future-work/scans/m23-exit.md) and [Finishing M23](../../future-work/scans/whats-left-todo.md#finishing-m23)
