# F64. `shoaladm stats` names members by hostname, and charts the figures full screen

## Context

[F52](cluster-stats.md) gave `shoaladm stats` every member's standing, placement, write rates,
memory and storage pipeline, as tables printed once or reprinted with `--watch`. Two things made
it hard to read at a glance.

- **A member was named by its id.** Every table's first column was the first eight characters
  of the member's node id, such as `daa5325b`. Knowing which row was which machine meant reading
  `shoaladm status`, which prints each inventory name beside its id, and matching them up.
- **Only the present was shown.** Every rate is a trailing average, and `--watch` redraws the
  tables over themselves. Whether a write rate was climbing, whether one member's sync time had
  just jumped, or whether a rebalance's stream had stalled could only be seen by watching the
  numbers change.

The user asked for three things. Name each member by the hostname of its machine whenever that
is possible, and write `<hostname>(<id>)` when two members share one. Add a full screen view
that graphs the figures, with a help page saying what every metric means. Keep today's printed
output behind `--basic`.

## What it does

### Members are named by hostname

A node reads its machine's name with `gethostname` on every figures tick and sends it in its
figures, as `NodeStats::hostname` (`shoal-proto/src/shared/protocol/stats.rs`, set in
`ControlPlane::tick_stats`). The field is skipped when empty and defaults when absent, so a
figures frame from a build before F64 decodes, and one from F64 is read by an older tool.

The admin tools name each member once per answer (`member_labels`,
`shoaladm/src/cluster/stats.rs`), from the first of these that is known:

1. **the hostname its figures carry**;
2. **the name the deployment gave its id**, from the cluster record under
   `~/.shoal/clusters/<name>/`, which is the inventory's node name;
3. **the first eight characters of its id**.

A name two or more members share names none of them, so each of those is written
`name(id)`, for example `europa(8e26a1f0)`. That happens when two nodes of one cluster run on
one machine, and every test cluster the fixture starts is such a cluster.

Every table uses the names, as do the header, the leader named in a local view and the busiest
groups' `led by` column. The member column is as wide as the longest name, and never narrower
than it was. **The lines and their order are unchanged**, so a script that reads a line by its
number reads the same line.

```text
stats from hyperion at version 31
member       state       age  partitions/led   archived    ins/s    upd/s    del/s     in B/s   stream/s
hyperion     up         0.4s        90/30        8.8KiB      360        0        0   36.0KiB/s   2.0KiB/s
titan        down (m)      -              -           -        -        -        -          -          -
```

A down member the leader has never heard from carries no hostname. `shoaladm stats` names it
by the record, as it does every member of a cluster whose nodes run a build from before F64.
The cluster tab in `shoalctl` shares the model, so it names members by hostname too. It is built
from a client alone and has no record, so a member without a hostname is named by its id there.

### A full screen view, and `--basic`

`shoaladm stats` now draws the figures full screen (`shoaladm/src/cluster/stats/`). It does so
only when stdout is a terminal and neither `--basic` nor `--json` was given. Otherwise it prints
exactly what it printed before, once, or again every `--watch` seconds.

```text
┌ metrics ───────────────┐┌ applied writes/s · last 5m ────────────────────────────────────┐
│cluster                 ││12.1k┤ ━ europa  ━ hyperion  ━ titan         ⢀⡠⠤⠒⠒⠤⣀            │
│ cluster writes/s       ││     │                                  ⢀⠔⠉      ⠈⠒⠤⣀        │
│writes                  ││ 6.0k┤                           ⢀⣀⡠⠤⠒⠉                        │
│ applied writes/s       ││     │               ⣀⣀⣀⡠⠤⠤⠔⠒⠒⠉⠉                               │
│ inserts/s              ││    0┼───────────────────────────────────────────────────────  │
│ ...                    ││     -5m                     -2m30s                       now  │
└────────────────────────┘└───────────────────────────────────────────────────────────────┘
```

- **The list on the left** holds every metric the view can chart, under six groups: the
  cluster, writes, streams, placement, memory and storage. `↑`/`↓` choose one.
- **The chart** draws the chosen metric over the window, one line per member in a color it
  keeps from metric to metric, or one line for a cluster metric. `←`/`→` choose a window of
  one, five, fifteen or thirty minutes. Rates are drawn from their ten second window.
- **The table under the chart** gives each line's newest value and its low, mean and high over
  the window, beside each member's state and the age of its figures.
- **The foot** lists the open plans, as the `--basic` output does, and the keys.
- **`p`** freezes the picture. The figures keep being read underneath, so the lines that
  arrived meanwhile are there when it is unfrozen.
- **`?`** opens the help page. It explains how to read the view, then every metric with its
  unit, every other word the figures and the `--basic` tables use, each field of a plan's line,
  and the keys. `q` leaves.

The figures are read every `--watch` seconds, two by default, which is about how often a member
sends them to the leader. The view keeps what it read for half an hour and forgets it on exit.

## Design choices

- **The node reports its own name.** This is the real machine name, so it is right whatever an
  inventory calls a node. It reaches every tool that reads `Stats`, including `shoalctl --addr`
  with no inventory. It also makes the collision rule mean something: two nodes on one machine
  share a hostname, where inventory names never collide.
- **It is read on every figures tick**, which is one system call every two seconds. A host
  renamed while its node runs is named by its new name from the next tick, with no restart.
- **The record is the fallback, not the source.** A member the leader has never heard from,
  or one running an older build, still has a name an operator recognizes. The record is only
  read where it exists, which is `shoaladm stats`.
- **One catalog feeds the list, the chart, the table and the help page**
  (`stats/metrics.rs`). Each metric names the `--basic` columns that print it, and each other
  word the tables use is a `Term` naming its own. A test fails if any column of any table is
  explained by neither, so a column added to `--basic` without help is caught.
- **The history is sampled per report, not per poll.** A member's figures are recognized by
  their `at_ms`, so a poll faster than the member reports adds no repeated points. A stale
  member adds none at all, so a node that stopped reporting is a line that stops rather than a
  flat line of current load. Points are placed by this process's clock, since the members'
  clocks disagree.
- **The read is one future the loop keeps across its turns**, polled beside the terminal's keys
  and a one second redraw tick (`stats/tui.rs`). A key never drops a read in flight, and the next
  one is armed an interval after the last answer, so a slow read never stacks up. Nothing is
  spawned. A spawned poller would have needed `Send` bounds on the schema's client types added
  to the public `cli::run`, which every schema's admin program calls. With no channel between
  the read and the screen there is no kanal receive to race either
  ([#152](../appendix/resolved/kanal-receive-races.md)).
- **A pipe always gets lines.** The lab's scripts redirect `stats` to files and read lines by
  number. A full screen view on a pipe would break them silently, so the view is drawn only on a
  terminal.

## Alternatives rejected

- **Names from the deployment record alone.** This needs no engine change and works on a
  running cluster at once. But `shoalctl --addr` and any cluster not deployed by `shoaladm`
  would get nothing, and inventory names never collide, so the `name(id)` rule the user asked
  for would never apply. The record is kept as the fallback.
- **The hostname in the committed membership.** Recording it when a node joins would name even
  a member that never reported. It would also be a control state change for a display
  convenience, and it would go stale when a host is renamed. A down member is already covered by
  the record.
- **A tabbed dashboard of small charts.** The user chose one chart with a metric list over tabs
  by category with four to six charts each. One large chart reads more precisely, and the list
  shows every metric at once.
- **History kept on the server.** The leader keeps only the newest figures of each member, in
  memory ([F52](cluster-stats.md)). Keeping a history there would make every leader change lose
  it, and would grow the leader's memory for a view nobody may be running. The view's own
  history costs nothing when it is closed.
- **Charting all three windows of a rate.** The one minute and five minute windows are
  smoothings of the same series, which the chart's own history already shows. The ten second
  window is the one that follows a change.

## Limitations

- **Hostnames show only once the nodes run F64.** Until then every member is named by the
  record, or by its id where there is no record.
- **The cluster tab has no record fallback.** A member without a hostname is named by its id in
  `shoalctl`.
- **The history is the view's own.** It starts empty and is lost when the view exits. Nothing
  here is a metrics store; the missing metrics endpoint in
  [Observability](../operations/observability.md#what-is-missing) is still open.
- **A frozen picture's chart ends where it was frozen**, but the samples taken meanwhile are
  kept, and a thirty minute freeze outlives the oldest of them.
- **A long hostname widens every table.** The `--basic` tables grow by as many columns as the
  longest name is over twelve characters.

## Invariants to uphold

- **`NodeStats::hostname` decodes from a frame that leaves it out**, and is left out when empty.
  A mixed-version cluster reports through it during every rolling upgrade.
- **`--basic` output keeps its lines and their order.** Only the names and the member column's
  width changed. A script reads a line by its number.
- **The full screen view is drawn only on a terminal.** A pipe, a redirect, `--basic` and
  `--json` always get what they got before.
- **Every column `--basic` prints is named by a metric or a term.** The header arrays
  (`MEMBER_COLUMNS` and the rest) are what the tables print and what the help test reads, so a
  column cannot be added to one and not the other.
- **A read in flight is never dropped by a key.** The loop keeps the read's future across its
  turns. If it is ever moved to a spawned task, the answer must reach the screen through a
  kept receiver, never a raced kanal `recv()`.

## Performance

None claimed for the data path. The node makes one `gethostname` call per figures tick. The
hostname adds about fourteen bytes of JSON plus the name to each status report carrying
figures, which is one report in four. For three members with six character names that is about
thirty bytes a second at the leader, beside the 8,735 F52 measured. The fanout spike's
`busy_node_stats` was left without a hostname, so the table on [F52](cluster-stats.md#performance)
can be reproduced as it was taken.

The full screen view reads `Stats` at the interval `--watch` always used, so it costs a cluster
nothing that `--basic --watch` did not.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `stats_frames_decode_from_older_shapes` (extended) | `shoal-proto/src/shared/protocol/stats.rs` | A frame without `hostname` no longer decodes, a full one does not round trip, or an empty name is sent |
| `hostname_matches_the_kernel` | `shoal-core/src/server/control/stats.rs` | The node reports a name other than the kernel's, or a truncated one |
| `stats_count_writes_partitions_and_status` (extended) | `shoal/tests/cluster_fixture.rs` | A member's figures do not carry the machine's hostname |
| `members_are_named_by_hostname_then_record_then_id` | `shoaladm/src/cluster/stats.rs` | The naming order changes, a shared name is not told apart by id, a long name misaligns a table, or the number of lines changes |
| `the_stats_model_reads_a_server_frame` | `shoaladm/src/cluster/stats.rs` | A member with no name known is no longer named by its id |
| `record_names_are_keyed_by_the_claimed_ids` | `shoaladm/src/deploy/ops.rs` | The record's names are not found by node id |
| `every_metric_and_column_has_help` | `shoaladm/src/cluster/stats/metrics.rs` | A metric has no help or group, two share a key, or a column of a `--basic` table is explained nowhere |
| `units_write_values_as_the_tables_do` | `shoaladm/src/cluster/stats/metrics.rs` | The chart's values are written differently from the tables' |
| `history_dedupes_skips_stale_and_trims` | `shoaladm/src/cluster/stats/history.rs` | A report read twice is charted twice, a stale member draws a line, a window does not slice, or old points are kept |
| `keys_move_the_screen` | `shoaladm/src/cluster/stats/screen.rs` | A key does something else, the help page does not take the arrows, or Esc leaves with the help page open |
| `a_frozen_picture_keeps_sampling` | `shoaladm/src/cluster/stats/screen.rs` | Freezing stops the sampling or lets the picture move, or a failed read clears the answer |
| `the_view_draws_hostnames_and_help` | `shoaladm/src/cluster/stats/view.rs` | The view does not name members by hostname, loses the chart, the table, the plan or the keys, or the help page omits a metric or term, scrolls past its end or overflows its width |
| `spans_and_wrapping` | `shoaladm/src/cluster/stats/view.rs` | The axis labels or the help page's wrapping change |
| `stats_takes_basic` | `shoaladm/src/cli.rs` | `--basic` is not taken, or a bare `--watch` stops meaning two seconds |

Proved on the lab as well. The user's tmdb cluster, whose nodes run a build from before F64,
read as titan, europa and hyperion through the record, both with `--basic` and piped with no
flag, and full screen under `tmux`. A side cluster, `tmdb-f64` on ports 13000-13002, was built
and deployed from this tree onto the same three hosts. Each node's figures carried its own
hostname, and 200,000 rows loaded through it drew one line per host on the applied writes chart.
The side cluster was then destroyed.

## Related

- [F52. Cluster stats](cluster-stats.md), the figures this view names and charts.
- [F63. `shoalctl` and `shoaladm`](shoaladm.md), which made `stats` a `shoaladm` command.
- [F53. The inventory wizard](inventory-wizard.md), whose split between a model and a view the
  stats view follows.
- [shoaladm](../operations/shoaladm.md), the operator's page for the command.
