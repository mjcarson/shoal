# F52. Cluster stats: what every node holds and does, and how fast a plan moves

## Context

After [F51](cluster-deployment.md), an operator could deploy a cluster, add a node and rebalance
onto it. They could not see the numbers that tell them whether things were going well:

- whether a rebalance was making progress, how fast, and when it would finish;
- how many partitions and how many bytes each node held;
- whether a node was up, down, or down in maintenance, alongside the figures above;
- how many rows, and how many bytes, each node was inserting, updating and deleting.

The cluster tab ([F50](cluster-operations.md)) showed standing and open plans. It counted a
plan's steps, but nothing about how fast they moved. It showed held and free bytes only when it
happened to be pointed at the control leader: `MemberView` reads them from the leader's
in-memory capacity table, and a follower holds none. Nothing in the engine counted a write.
[Observability](../operations/observability.md#what-is-missing) listed "no metrics" and "no
introspection". The `Observability` section of [TODOs](../appendix/todos.md#observability) asked
for a metrics surface "that should expect to carry more than recovery".

The request was for trailing estimates, cheap enough to run all the time: counted per shard,
combined per node, over windows of ten seconds, a minute and five minutes. They should be shown
per node, with a per-table drilldown, both by a command and in the cluster tab.

## What it does

### One read: `Stats`

`AdminKind::Stats { table }` is an admin read (`shoal-proto/src/shared/protocol/admin.rs`).
It needs no principal and no version. Its answer is a `ClusterStatsView`
(`shoal-proto/src/shared/protocol/stats.rs`) with two parts:

- **Every member not removed**, with:
  - its role, health, phase and one-word state;
  - its **maintenance** flag, meaning a down member whose grace is suspended;
  - its remaining grace and its failed shards;
  - its figures, if the answering node holds them, and how old they are.
- **Every plan the control state holds**, open ones first, each as a `PlanProgress`.

A member's figures are a `NodeStats`. It has one `TableStats` per table the node holds anything
of, their total, and the node's snapshot stream rates, free bytes and volatile log bytes. Every
figure in a `TableStats` is counted twice: over every copy the node hosts, and over the copies
whose group it leads.

| Figure | Over every copy | Over the copies led |
| --- | --- | --- |
| Tablet groups | `groups` | `groups_led` |
| Tablets | `tablets` | `tablets_led` |
| Archived partitions | `partitions` | `partitions_led` |
| Archived bytes | `bytes` | `bytes_led` |
| Write rates (rows and bytes of insert, update and delete, plus misses), 10s/1m/5m | `applied` | `led` |
| Write counters since the shard started | `applied_total` | `led_total` |

The led column is what to sum across the members. Each row has exactly one leader, so the sum
counts it once. Summing the hosted column counts every row once per replica.
`ClusterStatsView::cluster_total` does the led sum.

`table` narrows every member's figures to that table (`NodeStats::narrowed`). A table the schema
does not serve is refused by name, the same way `SetTableReadPolicy` refuses it.

### Where the numbers come from

```text
 run_apply ── count_write ──▶ Replication.writes[group]     (a u64 add per applied row)
                                     │
            replication_report ──────┤ GroupReport.writes, .partitions, .bytes
                                     ▼       (the archive map walk already paid for bytes)
                  control thread: self.replication[shard]
                                     │  every ReportTick (failure_detector.interval_ms)
                  NodeStatsTracker::tick ── Δcounters / Δt ──▶ EWMA 10s / 60s / 300s
                                     │
         leader: note_stats  ◀── StatusReport.stats, every 4th report (STATS_EVERY_REPORTS)
                                     │
                          AdminKind::Stats ──▶ ClusterStatsView
```

1. **Counting writes.** Every replica counts a write when its group applies the committed
   command (`count_write`, `shard/groups.rs`):
   - A command is exactly one row.
   - The bytes counted are the size of the replicated intent. That is the row for an insert or
     update, and the key for a delete.
   - An update or delete that found no row counts as a `miss`.
   - A scrub or a remembered duplicate counts nothing.

   The counters are kept per group, outside the group's slot, so they survive a slot being
   rebuilt for a new map. They are dropped when the map stops naming the group.
2. **Counting partitions.** `ArchiveMap::tablet_usage` counts partitions in the same pass over
   the archive map that `tablet_bytes` already made every report tick ([O57](../appendix/optimizations.md#o57-tablet-bytes-are-rescanned-from-the-whole-archive-map-on-every-report)).
   Each group sums them over its tablets.
3. **Rates.** On every report tick, `NodeStatsTracker::tick` (`control/stats.rs`) takes each
   group's counters from its shard's newest report and subtracts the reading from the tick
   before. It divides by the time between the two ticks and feeds the result to three EWMAs,
   per table and in total. A group's change always counts towards `applied`. It also counts
   towards `led` if the group's report said this copy leads.
4. **The leader.** A member's `StatusReport` carries its `NodeStats` on every fourth report,
   about every two seconds at the default half-second interval. The leader keeps the newest one
   per member in memory with the time it arrived, and notes its own on every tick. A leadership
   change clears the table, the same way it clears capacity.

### How a plan's progress is computed

`plan_progress` (`control/stats.rs`) folds three sources together:

- **The plan record.** It gives each step's state (pending, moving, moved, failed) and
  its planned bytes.
- **The committed move records the steps issued.** Every group in a move records
  `stats.since`, when it entered its current phase, and `phase_ms`, what each earlier phase
  took. So a step started at `since - Σ phase_ms`, and a finished one ended at its last group's
  `since`. `stats.bytes`, the snapshot bytes sent, is summed as `bytes_streamed`.
- **The stream rates of the members the running steps move from.** Their one-minute
  `stream_sent` is summed as `throughput_now_bps`.

From these it derives:
- `elapsed_ms`: from the first step's start to now, or to the last step's end once the plan is
  done;
- `mean_step_ms`: the mean time of the finished steps;
- `throughput_avg_bps`: the planned bytes moved divided by the elapsed time;
- `eta_ms`: the **larger** of two estimates:
  - the bytes left divided by the current throughput;
  - the steps left, run as many at once as are running now, at the mean step time.

The second estimate is there because every step waits out its source's `retire_after`, however
few bytes it holds. A plan of small sets is paced by that wait, not by its bytes.

### In shoalctl

- **`shoalctl cluster stats -i <inventory> [--table <t>] [--watch [secs]] [--json]`**
  connects to any member and reads `Stats`. If the answer is a follower's local view, it dials
  the leader's advertised client address and reads again. It prints:
  - the cluster's led write and byte rates, and its partitions counted once per row;
  - a member table: state (with `(m)` for maintenance), report age, groups, tablets and
    partitions as hosted/led, archived bytes and free bytes;
  - an applied-rate table: rows per second of each kind and bytes in, all over 10s/1m/5m, plus
    stream out;
  - a table summary, when there is more than one table or one was asked for;
  - the open plans, then the last three finished ones.

  `--watch` redraws every two seconds or every N. `--json` prints the leader's answer as it
  came.
- **`cluster rebalance`** and **`cluster add --rebalance`** print the plan's progress line
  under its step lines each time a step moves.
- **The cluster tab** reads `Stats` after its six reads. It reads from the leader the same way,
  dialing it unauthenticated since a read needs no principal. It draws the compact lines under
  the model: a member table with 10s rates, and the open plans.

```text
stats from daa5325b at version 31
member       state       age  partitions/led   archived    ins/s    upd/s    del/s     in B/s   stream/s
daa5325b     up         0.4s        90/30        8.8KiB      360        0        0   36.0KiB/s   2.0KiB/s
8e26a1f0     down (m)      -              -           -        -        -        -          -          -
open plans
  875c6a8b rebalance running 2/4 moved (1 moving, 1 pending, 0 failed)  2.0GiB of 4.0GiB  elapsed 2m05s  now 64.0MiB/s  eta 1m01s
```

## Design choices

- **Rates are derived on the node, from cumulative counters.** The shard's apply path does one
  `u64` add. Everything to do with time happens on the control thread, twice a second.
  An EWMA of a sum is the sum of the EWMAs, so taking the rate of the node's summed counters is
  the same as keeping one average per shard and adding them up.
- **Counters are kept per group, and leadership is decided at the tick.** Asking raft whether
  this copy leads on every apply would cost a metrics borrow per row. Instead the tick
  attributes a group's whole change for the interval to whoever led at the tick. A leadership
  change misattributes at most one interval.
- **A counter that goes backwards is a reset.** A shard that started again reports its counters
  from zero, and `WriteCounters::delta` returns `None`. The group's reading is then taken as
  its whole change, which is also what a group seen for the first time contributes. The very
  first tick is only a baseline.
- **The EWMAs are debiased.** Each window divides its value by the weight its samples have
  accumulated. So the first sample reads as itself, and the five-minute window is usable a
  second after a node starts rather than climbing up from zero over five minutes. A long
  interval weighs as many short ones would (`1 - exp(-dt/τ)`), so a delayed tick does not
  distort the rates.
- **Nothing is committed, and a follower does not forward.** Figures ride the status report
  the leader already receives, the same path capacity took in F46. A follower answers with the
  committed half, meaning standing and plans, plus its own figures. It names the leader and the
  leader's client address, and the client redials.
- **The client checks before asking.** `TopologyView.admin_reads` lists `stats`. A node from
  before F52 fails to decode an unknown `AdminKind` and closes the connection. That would drop
  the tab's client every second during a rolling upgrade, so shoalctl only sends `Stats` to a
  node whose `Members` frame lists it.
- **Plan timings come from the move records, not from leader memory.** The group leaders that
  drive a move already commit when each group entered each phase. The committed record
  survives a control-leader change, which an in-memory timestamp would not.

## Alternatives rejected

- **Forwarding the read to the leader over the control lane.** This needs a new `ControlKind`.
  `ControlKind::from_byte` refuses an unknown byte, and `serve_control` drops the lane that also
  carries raft's `AppendEntries` and `Vote`. So during a rolling upgrade it would break the
  control group, not just the read. It would need a wire version bump and an activation, which
  is too much for a read the client can simply redial for.
- **Computing the rates on the leader from the members' counters.** A new leader would restart
  every member's five-minute window at zero. It would also turn the leader's report handler
  into a per-member, per-table rate computation.
- **Keeping step start and end times in the leader's memory.** These are lost on failover, and
  the committed `MoveStats` already hold them.
- **Counting per table on the apply path.** This needs a leadership check per row to split
  applied from led, and a table can have several groups on one shard with different leaders.
- **Sending the figures on every report.** Four busy tables add about 7.4 KB of JSON to a
  report (see below). On every report that would double the leader's intake at 64 members.
- **A separate, slower walk of the archive map for partitions.** The walk for bytes already
  runs on every report tick. Counting in the same pass costs nothing, and it means the two
  figures can never disagree.

## Limitations

- **Partitions and bytes are archived figures.** They lag an insert until the compactor
  archives it, which is the lag `held_bytes` already has in F46. A freshly written or freshly
  fed node reads low until it compacts.
- **Ephemeral tables report no partitions or bytes.** They have no archive map. Their write
  rates are counted.
- **Applied rates spike while a node catches up.** A learner being fed, or a node back from an
  outage, applies a backlog of old entries in a burst. The led rates are the cluster's real
  write rate, and that is what the cluster line shows.
- **Stream throughput counts snapshot bytes only.** A learner fed from the log shows little
  stream traffic. The step counts and times are then the better measure of progress, which is
  one reason the ETA takes the larger of its two estimates.
- **Move timings use the group leader's wall clock.** `elapsed_ms` and `mean_step_ms` can be
  off by the clock difference between nodes.
- **Figures from a member that has stopped reporting stay held.** They are marked `stale` after
  three of their intervals (`STALE_AFTER_INTERVALS`), and shoalctl blanks their rates and marks
  the age with `!`.
- **Cluster nodes only.** A standalone node refuses admin requests, so it has no `Stats`.
- **No metrics endpoint.** This is a read an operator's tool polls. Nothing is pushed to a
  monitoring system, and the rest of [Observability](../operations/observability.md#what-is-missing)'s
  list is still open.

## Invariants to uphold

- **`GroupReport.writes` is cumulative and only ever grows within one start of the shard.** The
  tracker reads a decrease as a restart. A counter that is reset for any other reason reads as
  a burst of writes.
- **A write is counted once per replica, at apply, and never on a duplicate.** The led figures
  are only correct across the cluster because every replica applies each committed command
  exactly once and exactly one of them leads.
- **`tablet_usage` and `tablet_bytes` come from the same pass.** If [O57](../appendix/optimizations.md#o57-tablet-bytes-are-rescanned-from-the-whole-archive-map-on-every-report)
  replaces the pass with per-tablet counters, the partition count has to be kept the same way,
  or bytes and partitions will drift apart.
- **Every field of every stats type decodes from a frame that leaves it out.** A build before a
  field reads a newer frame, and the other way round. Zero rates are omitted when serializing,
  and this depends on it.
- **`Stats` is never sent to a node that does not list it in `admin_reads`.** Removing that
  list, or sending the read without checking it, brings back the dropped connections during an
  upgrade.
- **The leader's figures table is memory only.** It is cleared when leadership changes, the same
  way `capacity` is, and never goes in the log.

## Performance

None claimed for the data path. The apply path gains one `u64` add per applied row. The report
path counts partitions in a walk it already made. The control thread does one pass over its
shards' reports every report tick.

What this does change is the size of the status report. `cargo run -p shoal-spike --release --
fanout` prices a report carrying a node's figures for four busy tables, with every field
non-zero and at full width, on one report in four:

| members | report bytes | bytes/s in at the leader | with stats | with stats bytes/s |
| --- | --- | --- | --- | --- |
| 3 | 323 | 1,292 | 7,766 | 8,735 |
| 8 | 548 | 7,672 | 7,991 | 33,722 |
| 16 | 908 | 27,240 | 8,351 | 83,062 |
| 32 | 1,628 | 100,936 | 9,071 | 216,302 |
| 64 | 3,068 | 386,568 | 10,511 | 621,022 |

That is about 1.6× the leader's intake at 64 members and 7× at three, where the absolute figure
is 9 KB/s. Idle tables are left out of the report and zero rates are not serialized, so a real
report is smaller. A compact encoding is filed as
[O60](../appendix/optimizations.md#o60-a-nodes-figures-ride-its-status-report-as-verbose-json).

The change is under the macro, hotpath and stages layers' source paths, so every capture taken
before it reads as not describing the current code. No capture was taken for it.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `stats_count_writes_partitions_and_status` | `shoal/tests/cluster_fixture.rs` | Rows are not counted once per copy and once per leader, archived partitions do not add up to the rows, rates or bytes do not move, a follower claims every member's figures or does not name the leader, a table is not narrowed or an unknown one is not refused, or a down member in maintenance does not read so |
| `stats_follow_a_rebalance` | `shoal/tests/cluster_fixture.rs` | A running plan is not seen moving, a finished one lacks its start, elapsed time or mean step, or the spare's figures do not count what it was fed |
| `ewma_is_debiased_and_converges` | `shoal-core/src/server/control/stats.rs` | The first sample does not read as itself, a step change does not converge, or a long interval weighs differently from the short ones it replaces |
| `tracker_derives_rates_from_counters` | `shoal-core/src/server/control/stats.rs` | The first tick is not a baseline, led and applied are not split by leadership, a restarted shard reads as a negative or zero rate, or a vanished group's table jumps rather than decays |
| `plan_progress_from_steps_and_moves` | `shoal-core/src/server/control/stats.rs` | Step counts, bytes, start, elapsed time, mean step, throughputs or the ETA's larger-of-two rule go wrong, or a done plan keeps an estimate |
| `tablet_bytes_follow_the_map` (extended) | `shoal-core/src/server/tables/storage/fs/tests.rs` | Partitions per tablet stop following an insert, replace, removal or reopen |
| `write_counters_delta_and_absorb` | `shoal-proto/src/shared/protocol/stats.rs` | A counter going backwards is not a reset, or a sum drops a field |
| `stats_frames_decode_from_older_shapes` | `shoal-proto/src/shared/protocol/stats.rs` | A frame missing a field no longer decodes, or a full one does not round trip |
| `narrowing_and_cluster_totals` | `shoal-proto/src/shared/protocol/stats.rs` | `--table` keeps other tables, or the cluster total is not the members' led sum |
| `admin_bodies_round_trip` (extended) | `shoal-proto/src/shared/protocol/admin.rs` | `Stats` becomes a mutation or stops round tripping |
| `the_stats_model_reads_a_server_frame` | `shoalctl/src/cluster/stats.rs` | The printed view loses the led cluster total, a member's pairs, maintenance, a table, or the open-then-finished order of plans |
| `a_local_view_says_so_and_an_empty_one_draws` | `shoalctl/src/cluster/stats.rs` | A follower's view reads as the whole cluster, or an empty frame fails |
| `stats_are_asked_only_of_a_node_that_answers_them` | `shoalctl/src/cluster/stats.rs` | `Stats` is sent to a node that does not list it, or the tab stops drawing the figures |
| `figures_are_short` | `shoalctl/src/cluster/stats.rs` | Rates, durations or byte rates print in forms the tables do not fit |

## Related

- [F46](capacity-rebalancing.md): its capacity reports are the path the figures ride, and its
  plans are what `PlanProgress` follows.
- [F45](replica-migration.md): its `MoveStats` hold the timings.
- [F50](cluster-operations.md): the cluster tab this draws under.
- [F51](cluster-deployment.md): the `cluster` commands this adds to.
- [O57](../appendix/optimizations.md#o57-tablet-bytes-are-rescanned-from-the-whole-archive-map-on-every-report):
  the walk the partition count shares.
- [O60](../appendix/optimizations.md#o60-a-nodes-figures-ride-its-status-report-as-verbose-json):
  filed here.
- [Observability](../operations/observability.md) and [shoalctl](../operations/shoalctl.md).
