# F69. Operation kinds a schema supplies, and bytes counted both ways, in the driver

The benchmark driver of [F66](dataset-benchmarks.md) can now be handed kinds of operation
beside its own two, read and insert. It weighs, picks, times and reports them as it does its
own, and knows nothing else about them. Every window it keeps also counts the bytes its streams
sent and received on the wire, for each second, for the whole arm, and for each kind on the
receiving side. A workload that names read and insert alone runs, names its arms and digests its
spec exactly as before, so a capture taken before this feature and one taken after compare as
one benchmark.

## Context

This is the last required row of [S1](../object-storage/prerequisites.md#required) that depends
on no open question. It lands before
[M11](../object-storage/milestones.md#m11-step-0-the-harness-and-the-facts), which asks for a
driver whose operation kinds come from the schema and whose windows count bytes both ways. Every
gate after M11 is judged by a measurement taken through it: put, get, a ranged read, a write in
place, append, stat and delete, at rates and in bytes ([S15](../object-storage/performance.md)).
Before this feature the driver could measure none of that:

- read and insert were written into the mix (`spec.rs`), the operation kind (`window.rs`), the
  picker (`pick.rs`) and the response judge (`driver.rs`);
- a window counted operations and no bytes;
- `ShoalQueryStream::send` returned no byte count, and nothing told a caller how large an answer
  had been on the wire.

[Q30](../object-storage/contract.md#questions-to-answer) asks how the driver gains object
operations, and [X13](../object-storage/spikes.md#x13-the-benchmarks-shape) was to answer it by
reading the five places the two kinds are written into, then building a stub. The reading is
what decided it: every one of the five places takes a third kind with an extra case and no
reshaping, so the one driver is generalized rather than a second driver written beside it. The
stub's question, how fast one core makes seeded bytes, is left to X13. Nothing here makes bytes:
a supplied kind builds whatever query it builds.

## What it does

### A kind the driver is handed

`OperationKind<S>` (`shoal-proto/src/shared/dataset.rs`) is what a kind is:

| Method | What it says |
| --- | --- |
| `name` | What a workload, an arm and a capture call it: lowercase letters and underscores, never `read` or `insert` |
| `writes` | Whether an operation of it writes, which is what an attached run asks before it may send it |
| `build(seed)` | One operation's query, from that operation's own seed |
| `expect` | What its answer has to show to count as done rather than as a miss, `QuerySuceededOpts` |

A schema hands its kinds over through `DatasetSupport::operation_kinds`, which every schema has
and which returns none until a schema's buckets add theirs. A test hands one to a driver
directly with `Driver::with_kinds`. An operation is one query.

### Weights, picks and the arm's name

A workload names a supplied kind beside read and insert, `read:50,lookup:50`, and is named after
its weights, read and insert first and then each kind by name: `read50-insert0-lookup50`. The
four named workloads and every `read:N,insert:M` keep exactly their names and their written form,
and so their arm ids and the digest of a spec that names them.

Whether a kind exists is not a parse's question, since a workload is parsed before anything
knows the schema. It is the plan's: `Picker::new_with_kinds` is handed the driver's kind names
and refuses a workload naming any other, saying which kinds the schema supplies.
`shoaladm bench` asks that of every workload before a cluster exists, and its wizard asks it of
every custom row.

`Picker::at` chooses a supplied kind by weight beside read and insert, and hands back
`Pick::Supplied { kind, seed }`: the kind by its place among the driver's, and a seed drawn from
the operation's address, so two runs of an arm send the same operations. A workload that names
no supplied kind takes no extra draw and makes exactly the choices it made before.

### Recording and judging

`OpKind` gains `Supplied(name)`, and a `Window` keeps supplied kinds in `kinds`, by name,
recorded and added and summarized as read and insert are. An answer to a supplied kind is judged
by its kind's `expect`. A read is still judged as a get that has to find its row, and an insert
as it always was.

### Bytes both ways

- **Sent.** `ShoalQueryStream::bytes_sent` counts what each bundle a stream wrote took on the
  wire, the header and any trace or read-options section included. `flush` adds each bundle's
  bytes to the window of the second it was sent in.
- **Received.** `ShoalResponse::wire_bytes` is what an answer took on the wire: the preamble,
  the session token when one came, and the payload. Every answer is counted in the window, and an
  answer the stream owed is also counted for its kind. That includes one it was not owed, and
  the answers drained after an arm, which were still sent.

`WindowSummary` gains `kinds`, `bytes_sent`, `bytes_received`, `sent_per_sec` and
`received_per_sec`, and `KindSummary` gains `bytes_received`. Each is defaulted on read, and
`kinds` is left out when empty, so the capture format stays at 1 and every capture from before
reads unchanged. `compare` reads the bytes and each supplied kind's rate and latencies as
metrics of their own. A capture without them shows them as absent, never as a regression. The
stats screen's strip and its rate chart show the supplied kinds and the bytes beside read and
insert.

## Design choices

**A supplied kind is a trait object the driver reduces to a closure.** The driver's workers are
generic over the query type and not the schema, so `with_kinds` turns each `OperationKind<S>` into
a name, a `Fn(u64) -> K` and a copy of its `QuerySuceededOpts` as the driver is built. The
workers never name the schema, which is what they already were.

**A kind's name, not its place, keys a window.** Windows outlive the driver that filled them:
they are summed into a capture, compared across builds, and drawn by a screen that never saw the
driver. A name means the same thing in all of them. A place in one driver's list does not.

**The plan judges a kind; the parse does not.** `Workload` is parsed with no schema context,
from flags and spec files, so a name it cannot check is kept and checked where the schema's kinds
are known. That moved one refusal: `update:1` used to fail to parse, and now parses and is
refused when the arm is planned, by name.

**Bytes are counted where they are written and read, not estimated.** The sent count is the two
slices a bundle's write hands the socket, and the received count is the sizes the frame's own
header implied. Both are exact, which is what lets the test hold them equal to a proxy that
counts the wire.

## Alternatives rejected

**A second driver for object operations**, beside the table driver. X13 named it as the
alternative if read and insert were woven in too deeply. They were not: five places, each taking
a case. Two drivers would also mean two notions of an arm, two capture shapes and two compares,
and S15 asks for object arms beside table arms in one capture.

**Kinds as an enum in the loadgen crate.** The driver would then name every kind a bucket can
have, and a schema could not add one. S15's rule is that nothing in the driver names a bucket.

**Counting bytes from the archives' lengths at the driver.** That misses the headers, the trace
context and the session tokens, which are what a small operation's bytes mostly are. The client
knows the frame it wrote and the frame it read, so it is the one asked.

**Recording a supplied kind by its index in a `Vec` on the window.** Cheaper to record, but a
window then means nothing without the driver that filled it, and a capture is read by tools that
never saw it.

## Limitations

- **One operation is one query.** A kind cannot send several queries and be timed as one
  operation. An object operation that is a sequence of frames (S12) will need an operation
  identity and a count, as a bundle has.
- **A supplied kind is taken to write, unless the schema says otherwise.** A spec's attach check
  (`--yes-write`) runs before the schema is known, so any supplied kind counts as writing there.
  `Workload::writes_with` asks the kinds themselves where they are known.
- **No seeded bytes.** Nothing here generates object contents, and how fast one core can is X13's
  stub question, ~~unanswered~~ since answered: faster than any one lab device takes them, so a
  stream makes its own bytes inline, from SplitMix64 in counter mode
  ([X13](../object-storage/benchmark-shape.md)). M13 builds the described dataset.
- **No schema supplies a kind yet.** `operation_kinds` returns none until buckets exist (M12).
  Until then a supplied kind reaches a run only through a test.
- **The per-kind bytes are received bytes only.** A bundle mixes kinds and is written as one
  archive, so its sent bytes cannot be split among them without serializing each query alone.

## Invariants to uphold

- **A workload of read and insert alone is named, written, picked and digested as it was before
  this feature.** `table_arm_ids_are_unchanged` and `picks_of_read_insert_workloads_are_unchanged`
  freeze it. A change that moves either orphans every capture taken before it.
- **A supplied kind's choice takes no draw from a workload that names none.** The kind draw is
  skipped entirely when `supplied` is empty, so the read and insert choices see the same random
  stream.
- **Every byte counted is one the socket was handed or the frame header implied**, sent bytes
  only once a bundle's write has succeeded. A count from anywhere else stops agreeing with the
  wire, which `driver_counts_bytes_both_ways` holds it to.
- **New capture fields are defaulted and skipped when empty**, so the capture format does not
  move and an old capture reads.

## Performance

The driver's added work per operation is an `Arc<str>` clone for a supplied kind and two
additions per bundle and per answer. Nothing was measured, since nothing on a node's path
changed and a capture's numbers come from the node; the driver's own cpu figure in every capture
is what would show it. The lab's check of this feature is that a capture of the read and insert
workloads taken before it and one taken after are accepted by `compare` as one benchmark.

On the lab (2026-10-03) `shoaladm bench run` took two captures of `read100,insert100`, at
bundles 1 and 16, two runs each, on a side cluster of the bench's own built from
`tmdb_cluster.yaml`. One was built at `1b16939`, before this feature, and one with it.
`shoaladm bench compare` joined every arm by id and refused no fact. The bytes both ways appear
on the candidate side only, as `not on both sides`, which is what a capture from before F69
reads as:

```text
read100/b1/none
  read/s           142882.50..145309.10 -> 160268.70..168883.30   better by at least 10.3%
  sent MiB/s                          - -> 20.79..21.91           not on both sides
  received MiB/s                      - -> 32.94..34.71           not on both sides
```

The read rates are not a result: europa was compiling the other side's programs while the first
capture ran. The insert arms ran out of rows inside their warmup on both sides, since the
committed dataset is two thousand rows.

## Tests

| Test | Where | What breaks if reverted |
| --- | --- | --- |
| `table_arm_ids_are_unchanged` | `shoal-loadgen/src/spec.rs` | A read and insert workload's arm ids, written form or spec digest move |
| `picks_of_read_insert_workloads_are_unchanged` | `shoal-loadgen/src/pick.rs` | A read and insert workload makes other choices than it did before F69 |
| `a_supplied_kind_is_weighed_and_picked` | `shoal-loadgen/src/pick.rs` | A supplied kind is not picked by weight or seeded by its address, or an unknown one is accepted |
| `workloads_parse_by_name_or_weight` | `shoal-loadgen/src/spec.rs` | A supplied kind does not parse, name itself or write back |
| `kinds_and_bytes_add_and_summarize` | `shoal-loadgen/src/window.rs` | A supplied kind or the bytes are not recorded, added or summarized |
| `a_summary_from_before_kinds_still_reads` | `shoal-loadgen/src/window.rs` | A capture from before F69 fails to read, or one with no kinds writes a `kinds` field |
| `supplied_kinds_and_bytes_are_compared` | `shoal-loadgen/src/compare.rs` | A supplied kind or the bytes are not compared, or are judged against a capture without them |
| `driver_runs_kinds_a_schema_supplies` | `examples/bench_dataset/tests/driver_kinds.rs` | A kind the driver is handed is not run, timed or reported, or an unknown one is accepted |
| `driver_counts_bytes_both_ways` | `examples/bench_dataset/tests/driver_kinds.rs` | The driver's sent or received bytes differ from what a proxy counts on the wire |
| `a_custom_workload_is_added_and_builds` | `shoaladm/src/bench/wizard/form.rs` | A custom workload naming a kind the schema does not supply is accepted by the wizard |

## Related

[F66](dataset-benchmarks.md) and [F67](bench-run-wizard.md), the driver and the run this extends.
[S15](../object-storage/performance.md), which asks for both halves.
[X13](../object-storage/spikes.md#x13-the-benchmarks-shape) and
[Q30](../object-storage/contract.md#questions-to-answer), which this answers by reading; X13's
[record](../object-storage/benchmark-shape.md) for the rest of Q30.
