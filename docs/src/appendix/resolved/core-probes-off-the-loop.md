# 352. A shard asked every group's core whether it runs after every batch it applied

## Symptom

Under writes a shard's loop stopped taking messages after nearly every message that applied
anything, while it asked each of its groups in turn whether its openraft core still ran. On M23's side
cluster each table has 90 groups, so a Zen1 node's shard hosts about 54: 54 round trips to other tasks
of its executor, one after another, after each applied batch, and up to 200 ms each for a core busy
past the probe's bound. Nothing counted it.

## Cause

`probe_cores` (`shoal-core/src/server/shard/groups.rs`) finds a group whose core has stopped - a
panicked task leaves its handle held and its metrics frozen - by asking its handle a question only the
core answers. Its comment promised "a message per group every second or so". The loop ran it whenever
`sweep_due` was set, after `sweep_segments`, and the tick set that every `REPORT_EVERY_TICKS`, about
every 5 s; but `run_apply` set it too, at the end of every applied batch, so that a sealed segment
reaches its compactor promptly. Under steady writes the probe ran after nearly every message. It asked
each group with `raft.is_initialized()` under a 200 ms bound, one after another, and the loop awaited
it.

## Evidence

**Established by reproducing it.** `a_shard_probes_its_cores_at_most_once_a_second_under_writes`
(`shoal/tests/cluster_fixture.rs`) writes batches of twenty notes through node zero of three for six
seconds and reads every node's probe count and the loop time the probes held (`CoreProbes`, new in the
replication report). On the tree with the count and without the fix:

```text
node 0's shard probed its cores 660 times in 6.0s of writes (43541 keys written), holding its loop 883949 us
```

A probe every 9 ms, and 0.88 s of six held by it: a seventh of the loop, on a debug build of one shard a
node. On the fix each shard probes about once a second, and its loop's hold is the time it takes to
start a task.

**On the lab** (2026-11-05), the cell alone on M23's side cluster (`CELL_ONLY`,
`shoal-spike-scan/results/found-i352/`): eight side-rounds in `A B B A A B B A`, each a cluster
bootstrapped fresh at 6 GiB a node, the cell's first 100,000 rows preloaded, then `rw50` and `read100` at
a bundle of one, each warmed 100 s past item 351's change of flush and measured 120 s. Side A is the
build before (`4933602a`), side B the fix (`c0b0aad6`); every figure is the median of four side-rounds,
its range beside it.

| | Before | After | After / before | Disjoint |
| --- | --- | --- | --- | --- |
| `rw50` read p99 | 2.42 [2.35, 2.46] ms | 0.754 [0.736, 0.772] ms | 0.31 | yes |
| `rw50` read p999 | 5.43 [5.07, 6.95] ms | 3.27 [3.00, 4.17] ms | 0.60 | yes |
| `rw50` inserts a second | 1,523 [1,511, 1,528] | 1,687 [1,675, 1,712] | 1.11 | yes |
| `rw50` insert p50 | 20.1 [20.1, 20.3] ms | 17.9 [17.8, 18.1] ms | 0.89 | yes |
| `rw50` insert p99 | 37.1 [36.5, 37.2] ms | 34.2 [33.4, 34.7] ms | 0.92 | yes |
| `read100` read p99 | 0.436 [0.370, 0.462] ms | 0.375 [0.346, 0.417] ms | 0.86 | no |
| `read100` reads a second | 213,439 [205,455, 216,567] | 215,307 [192,942, 221,012] | 1.01 | no |
| Resident set once loaded, europa | 2,192 MiB | 2,261 MiB | 1.03 | yes |

The probes cost the foreground beside writes alone, as their cause says: a read with no write beside it
did not move, and a read beside the cell's inserts lost two thirds of its p99. This was most of
[O140](../optimizations.md#o140-a-resident-reads-p99-beside-the-cells-inserts-is-six-times-scylladbs-on-the-same-hosts)'s
gap: the cell's read p99 beside its inserts is 0.75 ms on the fix, where ScyllaDB's on the same hosts
was 1.15 ms. A loaded node holds 3% more because it inserted 11% more rows in the same two minutes.

## The fix

- **Probed on the tick alone, at most once a second** (`CORE_PROBE_EVERY`): a probe is due once a second
  has passed since the last began and none is still asking (`probe_due`, `probe_in_flight`,
  `last_probe`). An applied batch still asks for a segment sweep, which hands sealed segments to the
  compactors promptly, and no longer for a probe.
- **Never awaited on the loop.** The loop removes the floors fed past (Resolved #176's, the loop's
  state), takes each group's handle with the start of the core it is a handle to, and spawns a task that
  asks every core at once under the same bound and posts what it found back
  (`ServerMsg::CoresProbed`). A core is marked dead only if its slot still holds the core that was asked
  - up since the same start, not stopping, not marked already - so a late verdict never marks a group
  rebuilt or restarted since (`cores_probed`).
- **Counted**: `CoreProbes { probes, loop_held_us, longest_us }` in every shard's replication report and
  folded over a node's.

## Alternatives rejected

- **Probe on the tick alone, still on the loop.** It would cut the probes to one a second but keep a
  probe of a busy core holding the loop up to 200 ms a group, one after another.
- **Ask the groups together, still on the loop.** One round of the executor rather than one a group,
  but the loop still waits up to the bound for a busy core.
- **Drop the probe.** A dead core is otherwise silent until something it should answer times out, and
  the fixture reads `core_dead` to tell a stalled copy from a dead one.

## Invariants to uphold

- **An applied batch never makes a probe due**; a probe is the tick's, at most once a second, one at a
  time.
- **The loop never awaits a group's core**: as with an apply, a question to a core is a task's, and what
  it learns comes back as a message.
- **A probe's verdict is applied only to the core it asked**, judged by the slot's `up_since`.

## Still open

- **No test kills a core** to see it marked; the fixture holds that live cores are never marked
  (`two_volatile_voters_lost_at_once_do_not_kill_the_survivor` among them), and a probe's verdict is
  the same question it always asked.
- ~~**What the probe cost the lab's cell** is the A/B below once run on the lab.~~ Run: its read p99 beside
  inserts fell to a third, under **Evidence**.

## Tests

| Test | Where | Breaks if |
| --- | --- | --- |
| `a_shard_probes_its_cores_at_most_once_a_second_under_writes` | `shoal/tests/cluster_fixture.rs` | An applied batch makes a probe due again, or the probe stops running at all |
| `two_volatile_voters_lost_at_once_do_not_kill_the_survivor` | `shoal/tests/cluster_fixture.rs` | A late or stale probe marks a live core dead, or the floor fed past is no longer removed |
| `a_restarted_volatile_leader_elects_nobody_missing_its_commits` | `shoal/tests/cluster_fixture.rs` | The same, across a restarted leader |

## Related

[Resolved #176](unreadable-voter-log.md), the floors the probe removes;
[Resolved #351](the-cells-fall-is-the-drives.md), whose run counted the groups;
[F40](../../features/replication.md), the groups and their sweep.
