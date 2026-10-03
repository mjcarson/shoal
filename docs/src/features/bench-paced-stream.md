# F72. A paced stream beside the bench's load

A `shoaladm bench` run can drive one table at an offered rate beside its main load, with
windows of its own: `--paced Review --paced-rate 50`. The main load is still a closed loop over
every other table, and leaves the paced table alone. The paced stream sends on a fixed schedule
whatever the answers do. Each operation's latency counts from when it was due, so a stall shows
in its latency rather than as fewer operations. Every run of the capture carries what the
stream did, windows and a series of its own, which `bench show` prints and `bench compare`
judges. It is what a small table, driven lightly beside a large one, saw of the run.

The spikes page calls it **a paced neighbour stream**. It is called *paced* in the code, the
flags and the capture because `--allow-neighbours` already means other shoal units on the
hosts.

## Context

This is the second of the two optional rows on
[What a spike needs first](../object-storage/spikes.md#what-a-spike-needs-first), and it feeds
[X3](../object-storage/spikes.md#x3-bytes-through-the-tablet-groups). One of X3's records is a
neighbour: the p99 of a second, small table driven lightly throughout, beside rows of 64 KiB to
4 MiB through the tablet groups. It answers whether a stripe stored as a row hurts every table
on the node, which is what [S13](../object-storage/isolation.md) is about.

Before this feature a bench run could not take that record:

- **One closed loop.** Every worker sends more as answers come back
  ([F66](dataset-benchmarks.md#limitations)), so a table's share of the load is a weight of
  the same loop. That table is driven as hard as the large one, never lightly.
- **Windows by kind.** A window keeps reads, inserts and each supplied kind
  ([F69](driver-operation-kinds.md)), and drops which table an operation went to. A read of the
  small table and a read of the large one land in one histogram.

The spikes page named the way out: a second driver against the same cluster. The open-loop
generator in the [todos](../appendix/todos.md#what-f66-left-undone) is the same mechanism, so
the pacing is built into the one driver, where the main load can use it later.

## What it does

### The spec and the flags

`BenchSpec::paced` is a `Paced`, left out of a spec without one, so that spec's digest is what it
was:

| Field | Flag | Default | What it is |
| --- | --- | --- | --- |
| `table` | `--paced <TABLE>` | (required) | The table it drives |
| `workload` | `--paced-workload` | `read100` | Reads, inserts or both; a supplied kind is refused, since a kind is the schema's and not a table's |
| `per_sec` | `--paced-rate` | 20 | The rate it offers, over all its streams |
| `workers` | `--paced-workers` | 1 | How many streams it sends on, spread over the members as the main load's are |

These are refused:

- a rate that is not above zero;
- a workload that sends nothing, or names a supplied kind;
- the paced table weighted in `--tables`, since the main load never drives it;
- a table not in the dataset, or the dataset's only table, which would leave the main load
  nothing;
- `--paced-rate`, `--paced-workload` or `--paced-workers` without `--paced`.

An attached run whose paced stream inserts needs `--yes-write`, as a workload that inserts does
(`BenchSpec::writes`). The wizard passes a paced stream from the flags or the spec file through
untouched.

### Two drivers, one table each side

`Driver::narrowed` makes a driver through the same members over some of the tables. Each arm
runs two:

- **the main load**, over every table but the paced one, at the arm's workload, bundle and
  depth;
- **the paced stream**, over the paced table alone. It sends one query a bundle at its rate and
  starts its insert pool over rather than ending the arm. It has a picker of its own, drawn under
  `<arm>/<run>/paced`.

A table's insert feed is opened by the driver that runs it, so no feed is opened by both. The
driver over every table still preloads and still reads a preloaded attached cluster. The two run
together on the arm's clock (`tokio::join!`), so whatever ends the arm ends both: its time, an
abort, its inserts running out, an event keeping it past its time. The paced stream's seconds go
to no screen. After the arm its acknowledged inserts are read back like the main load's. An arm
whose paced stream inserted leaves the cluster changed, so the bench's own cluster is reset after
it (`BenchSpec::disturbs`).

### Pacing in the driver

`ArmSettings::pace` is a rate, operations a second over every worker; a closed loop has none.
For a paced arm:

- **Every operation has a slot**, fixed from the arm's start. Worker `w`'s `k`th operation is
  due at `start + (k·workers + w) / per_sec`
  (`shoal_loadgen::driver::slot`), so the workers share the rate evenly and a late send does not
  move the next slot.
- **A worker stages an operation only once its slot is due.** A retry that is due still goes
  first.
- **Latency counts from the slot.** A bundle is dated from its earliest operation's slot rather
  than from its send. An operation that waited behind a stall carries the wait.
- **A worker keeps at most a second of its share outstanding** (`Paced::in_flight`). An operation
  due while its worker is at that cap waits for an answer and keeps its slot's date, so the cap
  never hides a stall.
- **A worker with nothing owed sleeps until its next slot**, and looks again at least every
  100 ms, so an arm that ends between two slow slots is noticed.

A stream is now judged hung from the time since it last heard an answer or last owed none,
rather than from a count of ten-second waits. A paced worker's waits end at each slot, so a
count of waits would never reach a minute. A closed loop's behaviour is unchanged.

### What a run records

`RunResult::paced` is a `PacedResult`, the windows by table:

| Field | What it holds |
| --- | --- |
| `table`, `workload`, `per_sec` | What was asked |
| `measured`, `warmup` | Its windows, cut as the main load's are, latency from each slot |
| `series` | Every second of it |
| `feed` | What its insert feed did, if it inserted |
| `verify` | The read back of its acknowledged inserts |

`PacedResult::worst_second_p99_ms` is the worst p99 of any measured second, which is X3's figure.
`bench show` prints a line under each run, and the run logs it when its arm ends:

```text
    paced Review read100 at 50/s: read 50/s p50 0.26ms p99 1.20ms | bundle p50 0.26ms p99 1.20ms | sent 0.01 MiB/s received 0.01 MiB/s | worst second p99 41.66ms
```

`bench compare` reads `paced read p99 ms` and `paced insert p99 ms`. A capture without a paced
stream reads them as absent.

## Design choices

**One driver, paced.** The open-loop generator the todos ask for is this mechanism, a schedule
and latency from it. Built into `drive_stream` rather than into a second loop, it keeps one set
of retries, hung detection, byte counting and answer judging. The main load can be paced later
by setting the same field.

**The paced table is taken from the main load.** If both streams drove the table, its feed
would be opened by both and the numbers would mix: the main load's reads of the small table would
be in the main histogram, the paced stream's in its own, and neither would be the small table's
view. Splitting by table also makes the paced windows the table's windows, which is what "by
table" asks for.

**Bundle one.** A light neighbour sends single queries. A bundle would date several operations
from one slot and wait for the last.

**The paced stream wraps its pool.** A paced stream that ended the arm when its inserts ran out
would end the main load's measurement for a reason the main load never had.

**A second of operations outstanding at most.** Unbounded, a cluster that stopped answering would
be sent a queue of operations as fast as the slots came. A cap with the slot's date kept loses
nothing from the latency and bounds what a stall builds up.

## Alternatives rejected

**Windows by table in the main driver.** Every window would gain a table dimension: every
summary, every capture, every compare. And it would still not drive the small table lightly,
since it is one loop at one depth.

**A second process, `shoaladm bench` run twice against one cluster.** Two clocks, two captures,
and nothing to say which second of one is which of the other. The paced stream shares the arm's
clock, so its second 7 is the main load's second 7.

**Latency from the send.** That is what a closed loop measures. For a paced stream it would hide
exactly what the stream is for: a stall that delays the sends behind it would look like a lower
rate rather than a longer wait.

**Calling it a neighbour**, as the spikes page does. `--allow-neighbours` already means other
units on the hosts, and `neighbours_allowed` is in every capture's provenance.

## Limitations

- **The main load is still a closed loop.** Coordinated omission still applies to it. Pacing it
  is one field away (`ArmSettings::pace`), with a flag and its compare semantics left to do in
  the [todos](../appendix/todos.md#what-f71-and-f72-left-undone).
- **One paced stream, on one table.** Two neighbours of different weights would be a list.
- **The paced stream is not cut into an event's windows.** An event arm's before, during and
  after are the main load's; the paced stream's series shows the event second by second, but
  has no ratio of its own.
- **It is not on the wizard.** It comes from the flags or a spec file, and the wizard passes it
  through.
- **Reads and inserts only.** A supplied kind is the schema's, with no table to be paced on.
- **The driver's own scheduling is the floor of its latency.** A slot is honoured to tokio's
  timer resolution, a millisecond, so a sub-millisecond p99 is not resolved.

## Invariants to uphold

- **No table's feed is opened by both drivers.** `narrowed` gives the paced table to the paced
  driver alone, and the spec refuses weighting it for the main load.
- **A paced operation's latency counts from its slot**, including when it waited at the cap.
- **A paced worker never waits past its next slot for an answer**, unless it is at its cap or
  the arm is over, when it waits for an answer as a closed loop does.
- **The paced stream never stops the arm.** It wraps its pool, and it has no clock of its own.
- **A spec without a paced stream serializes and digests as before.** `table_arm_ids_are_unchanged`
  freezes the digest.
- **A closed loop's choices, timing and hung detection are unchanged.** A spec without `paced`
  runs exactly as it did.

## Performance

A closed-loop worker gains, for each top-up, a check that the arm is not paced, and for each wait
a computation of its bound. Nothing on a node's path changed. No A/B was taken.

On the lab (2026-10-03), in the same captures as [F71's](bench-device-memory.md#performance),
the paced stream read `Review` at 50/s beside a closed loop over `Item`, one stream, through
europa. It answered 50 a second in every run of every arm. The main load's windows hold none of
its operations, since `Item` is the only table they name. Over the second capture's two runs:

| Main load | Paced read p50 | Paced read p99 | Worst second's p99 |
| --- | --- | --- | --- |
| `read100/b1`, 159k–167k reads/s | 0.67–1.09 ms | 0.82–1.26 ms | 0.89–1.27 ms |
| `read100/b16`, 295k–328k reads/s | 0.61–0.77 ms | 2.15–4.02 ms | 2.74–4.46 ms |
| `insert100/b1`, 3.3k inserts/s | 0.39–1.14 ms | 1.18–1.94 ms | 1.25–2.28 ms |
| `insert100/b16`, 32k–33k inserts/s | 0.26–0.72 ms | 1.20–1.37 ms | 11.25–41.66 ms |

The last row is why the worst second is kept. Beside inserts at bundle sixteen, the paced reads'
p99 over the measured window barely moved, at 1.2–1.4 ms, but one second of each run reached
11–42 ms. A whole-window percentile averages a burst away; X3's neighbour is the p99 of a second.
The first capture, an hour earlier, showed the same shape: 3.56 ms and 1.53 ms over the window,
29.4 ms and 10.8 ms in the worst second.

## Tests

| Test | Where | What breaks if reverted |
| --- | --- | --- |
| `paced_slots_interleave_the_workers_at_the_rate` | `shoal-loadgen/src/driver.rs` | A slot moves from its place in the arm's schedule, or the workers do not share the rate |
| `a_paced_stream_reads_and_is_judged` | `shoal-loadgen/src/spec.rs` | A paced stream does not read with its defaults, moves the digest of a spec without one, or a stream that cannot run is accepted |
| `a_paced_stream_is_named_by_flag_or_spec` | `shoaladm/src/bench/args.rs` | The flags do not name or lay over a paced stream, an inserting one is allowed into an attached cluster, or its settings are accepted without a table |
| `a_paced_stream_keeps_its_rate_and_its_own_windows` | `examples/bench_dataset/tests/paced.rs` | The paced stream is not sent at its rate, second by second, beside a closed loop, its windows hold the main load's operations, it fed a table not its own, or it lost an insert |
| `a_run_with_a_paced_stream_writes_it_into_the_capture` | `examples/bench_dataset/tests/paced.rs` | A run's capture does not carry the paced stream, the main load fed the paced table, or compare and show do not read it |
| `devices_memory_and_the_paced_stream_are_compared` | `shoal-loadgen/src/compare.rs` | The paced stream's tail is not compared, or an insert tail is invented for a stream that only read |
| `devices_memory_and_the_paced_stream_round_trip` | `shoal-loadgen/src/results.rs` | A run's paced stream does not survive a capture, or its worst second counts the warmup |

## Related

[F66](dataset-benchmarks.md), the bench this adds to; [F71](bench-device-memory.md), the other
half of the same change; [X3](../object-storage/spikes.md#x3-bytes-through-the-tablet-groups),
which asked for it; [S13](../object-storage/isolation.md), the question its numbers bear on;
the open-loop generator in the [todos](../appendix/todos.md#what-f66-left-undone), which this
built half of.
