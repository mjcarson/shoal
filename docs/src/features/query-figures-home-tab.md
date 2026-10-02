# F65. Every node counts what its clients were answered, and `shoaladm stats` opens on a home tab

## Context

[F64](stats-tui.md) made `shoaladm stats` a full screen view, with a tab per metric group.
It opened on the cluster tab's four charts, and nothing on any tab answered the first thing an
operator asks: how busy is the cluster, how fast is it answering, and is it healthy?

The user asked for a default home tab showing:

- read and write speed;
- operations per second, per node and per kind (insert, get, update, delete and the rest);
- memory usage;
- latency.

Half of that did not exist on the server. [F52](cluster-stats.md)'s figures count what a node's
copies **apply**: inserts, updates and deletes, on every replica. They also carry memory and
the WAL's sync time. They did not count **gets or exists**, **read bytes**, or **how long a
query took**. The "Latency beside the rates" item in [TODOs](../appendix/todos.md) said so. The only
per-query timing the server had was the stage profiler's `StageStamps`, which compiles to
nothing outside a `stage-profile` build. No histogram crate could be reached from `shoal-core`.

Decided with the user before any code was written:

- **Add the figures to the nodes**, as an engine and wire change, rather than build the tab from
  what existed.
- **Latency is p50 and p99 per kind**, from a fixed log-bucket histogram with no new dependency.
- **A node counts the queries its clients sent it.** Each client query counts once across the
  cluster, and its latency is what the client waited, less the network.
- **The layout** is a line of totals, a grid of charts, and a table of members.
- **Performance is checked before and after on the lab nodes**, which are the benchmark hosts
  from now on. A figure that costs too much is estimated more cheaply (sampled), or restricted
  to profiling builds.

## What it does

### Every node counts what its clients were answered

Every shard keeps a `QueryMeter` (`shoal-core/src/server/shard/meter.rs`). It is shared with
the two relays of every client connection that shard accepted, and with nothing else.

- **The read relay counts each bundle's bytes** as they come off the socket.
- **The coordinator notes each bundle frame's arrival.** It records the stamp the relay took
  when the frame's last byte arrived, keyed by client, bundle id and the frame's range of
  indexes. It does this before routing any of the frame's queries. A streamed query sends
  several frames under one id, and the index range keeps them apart.
- **The write relay counts each answer** once its last byte has gone to the socket:
  - It reads the answer's kind off the answer: `get`, `exists`, `insert`, `update` or `delete`,
    or `error` for a failure, whatever the query asked. Since
    [F68](conditional-writes.md) it can also be `refused`, for a conditional write whose condition
    did not hold, read off the answer the same way.
  - It adds the answer's bytes.
  - If the answer's frame is being timed, it records the time from the frame's arrival to the
    write. The histogram has four buckets to every power of two of microseconds, and the last
    frame's answer releases the clock.

A forwarded query's whole answer is a peer's bytes, which the origin passes to the client
without validating. The write relay reads the kind of every other answer without validating it
either, which is only sound on bytes this node sealed. So a forwarded answer carries the kind
of the query instead: `archived_action`, generated beside `archived_is_write`, reads it from
the bundle the origin did validate.

Each shard's cumulative counters ride its report to the control thread, as
`ShardReplication::queries`. The node's tracker turns them into its figures, `NodeStats::queries`:

- **Answers per second by kind**, and their bytes per second, over the same 10s/1m/5m windows as
  every other rate.
- **p50 and p99 by kind**, read from a histogram in which each report's timed answers are added
  and every earlier one's weight decays over ten seconds.
- **The node's own p50 and p99** over every kind but `error`. A failure answers early and would
  pull the percentiles down.
- **Request bytes in per second**, and totals of everything since the shards started.
- **`sampled_every`**: one in how many bundle frames is timed.

The block's shape, with example values:

```json
"queries": {
  "ops": [
    { "op": "get", "rate": { "r10s": 300.0, "r1m": 280.4, "r5m": 150.2 },
      "bytes_out": { "r10s": 30720.0, "r1m": 28712.9, "r5m": 15380.5 },
      "p50_ms": 0.31, "p99_ms": 2.04, "answers_total": 91022, "bytes_out_total": 9320653 },
    { "op": "insert", "rate": { "r10s": 100.0, "r1m": 98.1, "r5m": 60.0 },
      "bytes_out": { "r10s": 1600.0, "r1m": 1569.6, "r5m": 960.0 },
      "p50_ms": 1.02, "p99_ms": 3.51, "answers_total": 30008, "bytes_out_total": 480128 }
  ],
  "bytes_in": { "r10s": 40960.0, "r1m": 39001.2, "r5m": 22031.9 },
  "bytes_in_total": 12294877,
  "p50_ms": 0.42, "p99_ms": 3.5, "sampled_every": 1
}
```

### A home tab

`shoaladm stats` now opens on **home**. The group tabs follow it, keyed `1` to `8`. This is the
lab's side cluster under the loader's `bench` (its `drive` since [F66](dataset-benchmarks.md)), with each chart's body cut:

```text
shoaladm stats · tmdb-f65 · from hyperion (leader) · version 11 · every 2s
europa up   hyperion up   titan up
 1 home   2 queries   3 cluster   4 writes   5 streams   6 placement   7 memory   8 storage                                                              last 5m
queries 95.5k/s   reads 81.1k/s   writes 14.4k/s   errors 0/s   read 67.4MiB/s   write 6.0MiB/s   p99 100.88ms   resident 2.5GiB   rows 627.3MiB of 6.0GiB
┌ ops/s by kind ────────────────────────────────────┐┌ queries/s ─────────────────────────────────────────┐┌ read bytes/s ─────────────────────────────────────┐
│ ...
│get 81.1k  exists 0  insert 4.8k  update 9.6k      ││europa 41.4k  hyperion 29.0k  titan 25.0k           ││europa 29.2MiB/s  hyperion 20.5MiB/s  titan        │
│delete 0  error 0                                  ││                                                    ││17.7MiB/s                                          │
└───────────────────────────────────────────────────┘└────────────────────────────────────────────────────┘└───────────────────────────────────────────────────┘
┌ write bytes/s ────────────────────────────────────┐┌ p99 ms ────────────────────────────────────────────┐┌ resident ─────────────────────────────────────────┐
│ ...
│europa 2.0MiB/s  hyperion 2.0MiB/s  titan 2.0MiB/s ││europa 81.73ms  hyperion 100.88ms  titan 84.68ms    ││europa 834.1MiB  hyperion 897.1MiB  titan 829.1MiB │
└───────────────────────────────────────────────────┘└────────────────────────────────────────────────────┘└───────────────────────────────────────────────────┘
member   get/s   ins/s   upd/s   del/s   ex/s    err/s   read/s      write/s     p50      p99      rows/budget       resident
europa   35.2k   2.1k    4.2k    0       0       0       29.2MiB/s   2.0MiB/s    1.27     81.73    209.5MiB/2.0GiB   834.1MiB
hyperion 24.6k   1.5k    2.9k    0       0       0       20.5MiB/s   2.0MiB/s    3.40     100.88   210.6MiB/2.0GiB   897.1MiB
titan    21.3k   1.3k    2.5k    0       0       0       17.7MiB/s   2.0MiB/s    1.63     84.68    207.1MiB/2.0GiB   829.1MiB
cluster  81.1k   4.8k    9.6k    0       0       0       67.4MiB/s   6.0MiB/s    3.40     100.88   627.3MiB/6.0GiB   2.5GiB
```

- **A line of totals**, summed over the members whose figures are current:
  - queries, reads, writes and errors answered per second, with errors in red when there are any;
  - read and write speed;
  - the slowest member's p99;
  - the members' resident memory, and their rows in memory against their budgets.

  A member whose figures carry no query block is named under the line as running a build from
  before F65.
- **Six charts**:
  - ops per second by kind, with a line per kind in a color the kind keeps on every chart;
  - queries per second per member;
  - read bytes and write bytes per second;
  - p99;
  - resident memory.

  Each has a line of legend giving every line's newest value, rather than the full summary the
  other tabs draw, since the table carries the numbers. `Space` then `f` shows one of them with
  its full summary, as on any tab.
- **A table of members** gives each member's answers per second by kind, read and write speed,
  p50 and p99 in milliseconds, rows against budget, and resident memory. A terminal too narrow
  for all of them loses the least needed columns first, never a name. Under them is a
  `cluster` row with the sums and the slowest member's waits. A stale member's figures, and an
  older build's missing ones, are dashes.

Read speed is the answer bytes of gets and exists written to clients. Write speed is the
intent bytes through the groups a member leads, the figure [F52](cluster-stats.md) already
kept. Every row has one leader, so the members' write speeds add up to the cluster's, and the
table's `cluster` row is the sum of the rows above it.

### A queries tab

The new second tab charts every query figure:

- ops by kind;
- per member: queries, reads, client writes, errors, read bytes, write bytes and requests in;
- per member: p50 and p99;
- p99 by kind, on the member where each kind's is highest.

The help page explains each one, the home tab's words, and the keys. A wait nobody timed is a
gap on a chart and a dash in a summary, never a zero.

`--basic`, `--json` and a pipe print what they printed before, and `--json` carries the new
block.

## Design choices

- **Counted where the client connected.** The user chose this over counting where each share of a
  query runs. Each query counts once, so the members' figures add up to the cluster's, and the
  latency is the one a client sees. Where load lands is still on the writes tab: applied over
  every copy, and led over the copies a member leads.
- **The relays hold the meter, and nothing between them does.** Every client answer leaves
  through the connection's write relay exactly once, whatever path it took in between: local,
  split and gathered, forwarded, parked on a disk read, or behind a read barrier. Counting there
  needs nothing carried on `QueryMetadata`, which is cloned per query and again per partition a
  query blocks on. It also needs nothing in the reply paths. The one addition is `Reply::op`,
  set only for a forwarded whole answer.
- **The kind is read off the answer.** `QuerySupport::kind` matches two discriminants of the
  archived answer, with no validation, on bytes this node sealed. A failure reads as `error`
  whatever it asked, which is the figure an operator wants: how many clients were refused.
- **The clock starts where the stage profiler's does**, when a bundle frame's last byte comes
  off the socket. It stops where the profiler's `socket_written` does, so the two are
  comparable in a `stage-profile` build.
- **Log-linear buckets and no dependency.** Four buckets per power of two keep every bucket
  within a quarter of its lower bound, and interpolating within a bucket halves that. A hundred
  and twenty buckets reach past a quarter of an hour. A kind's buckets past its slowest wait are
  not sent, so a shard's report grows by a few hundred bytes.
- **Percentiles over a decayed histogram.** A report's timed answers enter at full weight and
  lose weight with a ten second time constant, the same as the `r10s` rate beside them. One
  interval's answers alone would read nothing on a quiet node and jump on a busy one. A node
  that stops answering sees its percentiles fade out, once less than one answer's weight is
  left.
- **A node always sends the block.** `sampled_every` is one or more on every node from F65 on, so
  an idle node sends an empty block and only an older build sends none. The home tab names the
  latter. The alternative, telling them apart by any figure being nonzero, would have named
  every member no client connects to as an older build.
- **Write speed is what the leaders write.** Request bytes in cannot be split into reads and
  writes, since a bundle mixes them, so they are charted as "requests in" and never as write
  speed.

## Alternatives rejected

- **Counting where each share executes.** It shows where load lands. But a get split over three
  nodes would be counted three times, and the latency would leave out the fan-out and the
  gather. The user chose the front door.
- **The arrival stamp and the kind on `QueryMetadata` and `Reply`.** The stamp is sixteen bytes
  on a struct cloned per query and per parked partition, and it would have to be carried
  through every reply site and every pending forward. The stage profiler keeps its stamps out
  of production builds for exactly that cost.
- **Validating an answer to read its kind.** A checked `access` walks the whole answer, rows and
  all, which is the cost the sealed get path exists to avoid.
- **Reading a forwarded answer's kind unchecked.** It is a peer's bytes, passed on unvalidated.
  A malformed one would be undefined behaviour on the origin, where today it only fails the
  client's own decode.
- **`hdrhistogram`.** It cannot be reached from `shoal-core`, and adding a dependency for a
  hundred and twenty counters buys nothing.
- **Mean latency only.** It is a sum and a count, and the cheapest option, but it hides the tail
  an operator is looking for. The user chose p50 and p99.
- **Sampling or feature-gating from the start.** The measurement below found no cost, so every
  frame is timed. `LATENCY_SAMPLE_EVERY` is the knob if a later measurement disagrees.
- **The home tab as a metric group.** A metric belongs to exactly one group, which the help page
  and a test hold it to. The home tab borrows charts from two groups (`HOME`), so it is a tab of
  its own rather than a group.

## Limitations

- **A member is only as busy as its clients make it.** A cluster whose clients all connect to
  one member shows that member answering everything and the others idle. The writes tab shows
  where the work lands.
- **The latency leaves out the network and includes a slow reader.** The clock stops when the
  answer is handed to the socket. A client that stops reading fills its socket and stretches
  the time the next answers take to be handed over.
- **A bundle's queries share its arrival.** A large bundle's last answer includes the time spent
  serving the others, which is what its client waited, but it is not one query's service time.
- **A forwarded answer that failed counts under its query's kind,** not `error`, since the
  peer's bytes are not read.
- **A standalone node counts and reports nothing.** It has no `Stats` read; that is still the
  "A standalone node's figures" item in [TODOs](../appendix/todos.md).
- **Percentiles are bucketed.** They are good to about an eighth of the value. The cluster's
  p99 is the slowest member's, not a merge of every member's answers.
- **`--basic` prints none of the query figures.** Its lines and their order are an invariant.
  `--json` carries them.
- **The tabs and the table are wide.** The table needs about a hundred and thirty columns for
  every figure. Narrower, it leaves out the least needed first: exists, deletes, rows against
  budget, then the p50 (`home_columns`), and never the member's name. At a hundred columns the
  tab bar's last labels are cut.
- **Admin requests and topology frames are not counted.** They are not queries.

## Invariants to uphold

- **Only a client connection's relays hold the meter.** A peer lane never does, so a query served
  for another node is counted on the node its client reached and nowhere else.
- **A frame's clock is noted before any of its queries is routed.** An answer finds only a clock
  noted before it was written. One routed first would be counted and never timed.
- **An answer's kind is read unchecked only off bytes this node sealed.** A forwarded whole
  answer carries `Reply::op`, from the validated bundle. Any new path that hands a client relay
  bytes this node did not seal must set it too.
- **No borrow of the meter is held across an `.await`.** It is shared on one executor.
- **`NodeStats::queries` decodes from a frame that leaves it out, and a node from F65 on always
  sends it**, with `sampled_every` of one or more.
- **`QUERY_OPS` is the order of every per-kind array**: in `QueryCounters`, in the meter and in
  the tracker. Appending a kind is safe; reordering is not.
- **`--basic` keeps its lines and their order**, and every `HOME` key is a metric in exactly one
  group.

## Performance

The meter's own cost, from the ignored `meter_cost` test run on hyperion. The build was for
`znver1` and the governor `performance`, and the times are the same over three runs:

| What | Per |
| --- | ---: |
| Register a one-query bundle frame and record its answer, two clock reads included | 163–167 ns |
| Count an answer whose frame is not timed | 30 ns |

A get on the same host takes about 290 µs at the reference depth, so timing every answer is
about a thousandth of it.

**Before and after.** The committed engine change (`bebadcd`)
was compared against `8c80f18`, the commit before it, both built for `znver1` and run by `shoal-workload` on hyperion. The conditions:

- hyperion: Zen1 V1756B, 4 cores and 8 threads, `performance` governor;
- the lab's tmdb node on that host stopped for the runs;
- two shards and one physical core left to the client;
- tracing at `Warn`;
- four rounds, each running both sides back to back, with the side that went first alternating.

**This is an A/B, not a capture.** The committed `shoal.yml` is sized for a sixteen-core host.

| Workload | Figure | Before, median [range] | After, median [range] | |
| --- | --- | ---: | ---: | --- |
| `get_ephemeral` | ops/s | 52,631 [40,621–53,963] | 51,458 [50,646–53,973] | within noise |
| `get_ephemeral` | get p99 | 534 µs [510–617] | 551 µs [501–569] | within noise |
| `get_resident` | ops/s | 48,212 [43,811–50,116] | 48,485 [45,600–49,089] | within noise |
| `transport/send_one/small` | ops/s | 46,205 [42,002–49,867] | 48,822 [46,846–50,849] | within noise |
| `grid/unsorted/r50/1024` | ops/s | 7,556 [7,418–7,613] | 7,657 [7,613–7,680] | within noise |
| `grid/unsorted/r50/1024` | read p99 | 522 µs [516–541] | 554 µs [534–570] | within noise |
| `cluster/overhead/nodes/1` | ops/s | 5,623 [5,596–5,657] | 5,636 [5,627–5,668] | within noise |
| `insert_ephemeral` | ops/s | 123,354 [96,550–125,179] | 114,330 [85,325–121,355] | within noise |
| `insert_ephemeral`, 8 rounds | ops/s | 94,503 [79,026–120,014] | 107,152 [95,846–120,360] | within noise |

Every other figure, twenty-two in all, was within noise too: each side's p50 and p99, per
operation. Following the compare tool's rule for the macro layer, a difference counts only when
the two sides' run intervals are disjoint.

`insert_ephemeral` records the most answers per bundle, and its first median was the lowest.
Repeated over eight rounds, it moved the other way. It swings about a fifth from run to run on
either side. **No cost was measured, so every frame is timed and nothing is gated.**

On the wire, the block adds a few hundred bytes to one status report in four. A node answering
all six kinds sends under two kilobytes of JSON, which a test holds it to. The fanout spike's
`busy_node_stats` was left without it, as F64 left the hostname, so
[F52](cluster-stats.md#performance)'s table can be reproduced as it was taken.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `stats_frames_decode_from_older_shapes` (extended) | `shoal-proto/src/shared/protocol/stats.rs` | A frame without `queries` does not decode, a full one does not round trip, an empty one is sent, or an idle node's is left out |
| `query_figures_name_every_kind_and_stay_small` | `shoal-proto/src/shared/protocol/stats.rs` | An answer kind is counted in another's place, a node's full figures pass two kilobytes, or an empty sum reads as a negative zero |
| `buckets_tile_and_percentiles_interpolate` | `shoal-core/src/server/shard/meter.rs` | A wait lands outside its bucket, buckets overlap or leave gaps, one is wider than a quarter of its bound, or a percentile falls outside its bucket |
| `answers_are_counted_and_their_frames_timed` | `shoal-core/src/server/shard/meter.rs` | An answer is not counted by its kind, a frame's clock is not released by its last answer, a streamed query's frames are timed against each other, an untimed answer is timed, or a relay's end forgets another client's clocks |
| `meter_cost` (ignored) | `shoal-core/src/server/shard/meter.rs` | Nothing; it prints the meter's cost on the bench host |
| `tracker_derives_query_rates_and_percentiles` | `shoal-core/src/server/control/stats.rs` | Answers do not become rates by kind, waits do not become percentiles, a restarted shard reads as a loss, or a quiet node's percentiles do not fade |
| `stats_count_writes_partitions_and_status` (extended) | `shoal/tests/cluster_fixture.rs` | Inserts and gets are not counted on the node the client reached, are counted on another, carry no bytes or waits, or a forwarded get through the spare is counted as an error or not at all |
| `every_metric_and_column_has_help` (extended) | `shoaladm/src/cluster/stats/metrics.rs` | The queries group loses a metric, a home chart names no metric, or a kind is read out of order or as a number where nothing was timed |
| `history_dedupes_skips_stale_and_trims` (extended) | `shoaladm/src/cluster/stats/history.rs` | A kind's line is not kept, an idle kind's line breaks, or a wait nobody timed draws a line |
| `keys_move_the_screen` (renumbered) | `shoaladm/src/cluster/stats/screen.rs` | The view does not open on home, home does not chart the `HOME` metrics, or the tabs do not follow it in order |
| `the_view_draws_the_home_tab` | `shoaladm/src/cluster/stats/view.rs` | The totals sum a stale member or lose a figure, an older build goes unnamed, a home chart or its legend is missing, the table's rows or the cluster's row read wrong, space f loses a kind's summary row, or a small terminal does not draw |
| `the_view_draws_a_tab_of_charts` (renumbered) | `shoaladm/src/cluster/stats/view.rs` | The tab bar stops naming home and queries first |
| `stats::every_metric_that_should_move_does` ([F67](bench-run-wizard.md)) | `examples/bench_dataset/tests/stats.rs` | The per kind counts, bytes and waits stop reaching the view from a real node under a bench run: `ops_by_kind` must draw `get` and `insert` above zero, and every other query figure above zero |

`the_view_draws_the_home_tab` also covers the narrow table. At a hundred columns it must leave
out exists, deletes and rows against budget, keep the p99 and the resident memory, and write
`hyperion` whole. A table of one column holds only the name.

**Proved on the lab.** A side cluster, `tmdb-f65` on ports 13000-13002, was built from this tree
and deployed onto hyperion, titan and europa. Its programs and state were kept under
`target/lab/f65/`. It was loaded with 30,000 movies (219,343 rows), then driven for four minutes
by the loader's `bench` (now `drive`), spread round robin over the three members.

- **The counts matched the client's.** The home tab read 81.1k gets, 9.6k updates and 4.8k
  inserts a second. Over the same seconds the bench counted 70–85k gets and keyword reads,
  9.8k updates and 4.9k inserts.
- **The p99 matched too.** Each member's p99 was 81–101 ms. The bench's writes waited 80–150 ms
  at their p99, and a p99 over a mixture with one write in seven lands among the writes.
- **At 160×45** the six charts took three columns by two.
- **At 100×30** the charts scrolled. The first look there found the table squeezing every
  column and cutting `hyperion` to `hyperio`; since then it leaves columns out instead
  (`home_columns`).
- **`--json`** carried each member's block, with `sampled_every` 1. **`--basic`** printed what
  it did before.

The side cluster was then destroyed, and the lab's tmdb cluster stayed up throughout.

## Related

- [F64. `shoaladm stats` names members by hostname, and charts the figures full screen](stats-tui.md),
  the view this adds a tab to.
- [F52. Cluster stats](cluster-stats.md), the figures and the report path the query figures ride.
- [F41. Read consistency](read-consistency.md), whose barriers and gathers a read's latency
  includes.
- [F26. Archive-routed requests](archive-routed-requests.md), why a query is read out of its
  bundle rather than carried.
- [shoaladm](../operations/shoaladm.md), the operator's page for the command.
