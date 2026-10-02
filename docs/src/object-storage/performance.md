# S15. Performance, and the benchmarks that judge it

## Context

Performance is important (R14), and object storage has to be benchmarkable by
`shoaladm bench` (R15). The two are one requirement: every design choice in this part is a
claim about cost, and a claim with no arm that would show the difference is not one this
book accepts ([Optimizations](../appendix/optimizations.md#how-these-are-ranked)).

This page says what is compared with what, what the driver has to gain, what an arm records,
and which numbers are budgets. The budgets are hypotheses. So is everything the lab says
about a network, and the page is explicit about that too.

## What exists today

`shoaladm bench` runs a schema's own workload against a copy of its own cluster
([F66](../features/dataset-benchmarks.md)).

- **It reads and inserts rows, and nothing else.** A mix is two weights, read and insert
  (`shoal-loadgen/src/spec.rs:21-28`); the operation kind, the picker and the per-table
  source know those two (`window.rs:20-25`, `pick.rs:22-35`, `feed.rs:305`).
- **It counts operations and not bytes.** A second's window holds a latency histogram,
  misses, retries and errors (`window.rs:80-91`). The only bytes recorded are the server's
  own figure for what it answered.
- **It refuses a bundle it thinks will not fit a frame**, against a 64 MiB constant of its
  own (`shoaladm/src/bench/orchestrate.rs:65`).
- **It is closed loop**: a worker sends more as answers come back, so a stall delays the
  operations behind it and is not counted against them.
- **An arm is `{mix}/b{bundle}[/{override}]/{event}`**, never renamed, with events that
  kill, stop, remove, rebalance, decommission, repair and back up
  (`shoal-loadgen/src/spec.rs:135-153`), each cut into before, during and after, and a
  rebalance judged by `p99_ratio_permille` (`shoal-loadgen/src/events.rs:122`).
- **It builds a cluster of its own**, with every root, port and directory of the inventory
  moved and checked disjoint, and it reads back every acknowledged insert.
- **A capture is compared only with its like**: `compare` refuses two captures that differ
  in dataset, spec, inventory shape or any node's machine.

And the lab, where it runs, is three hosts on 1 GbE with one device each
([cluster testing](../cluster-testing/overview.md#the-lab)), with rules for an A/B there
that any change to a node's query path follows
([Benchmarking](../performance/benchmarking.md#before-and-after-on-the-lab)).

## The design

### What is compared with what

| Experiment | Held constant | What it tells |
| --- | --- | --- |
| Stripes as rows against staged stripe chunks, replicated | Hosts, object sizes, concurrency | What a data plane buys at each size, and where it stops paying ([Q14](contract.md#questions-to-answer), [Q27](contract.md#questions-to-answer)) |
| Replicated against k+m | The pool's devices | What encoding costs a write, and decoding a read |
| A whole stripe against part of one | The pool | What a write in place costs over a put |
| A healthy read against one with a holder down | The pool and the range | What a degraded read costs |
| An SSD pool against a rotational one | The nodes | R16 |
| Object work on the table shards against cores of its own | The table workload | [S13](isolation.md) |
| Tables alone against tables beside objects | The table workload | Whether the overview's constraint holds |
| Foreground alone against foreground beside a rebuild, a move, a scrub | The foreground | What background work costs a tail |

Every experiment is two arms that differ in one thing, in the manner of the workload grid's
isolating pairs. An arm that changed two would say something got slower and not what.

### What the driver gains

Three things, and the first two are prerequisites in their own right
([S1](prerequisites.md#required)).

- **Operation kinds a schema's buckets add**: put, get, a ranged read, a write in place,
  append, stat and delete. The driver stays generic over the schema, as it is for tables:
  the bucket's generated half says what its operations are, and nothing in the driver names
  a bucket.
- **Bytes counted at the driver**, both ways, for every second's window, beside the
  operations.
- **An object dataset.** A table's dataset is a file of rows. A bucket's is a description:

| A bucket's dataset is | Use |
| --- | --- |
| A folder of real files | A workload somebody has |
| A description: how many objects, a distribution of sizes, a seed | A workload nobody has yet, repeatable to the byte |

The folder is judged whole before a host is touched, as a table's is. The first part is
preloaded, and **reads and writes in place ask only for objects that were preloaded**, so a
miss is a failure of the cluster and not a property of the data: F66's rule, kept.

### An arm

An object arm is named by what it does, how large, how many at once, and its event:
`{mix}/{size}/c{concurrency}[/{override}]/{event}`, such as `put100/s64m/c8/none` or
`write100/w4k/c32/device-kill`. Table arms keep the names they have; an arm's id is what
two captures are joined on and is never renamed.

What an arm records, each second:

| Figure | Why |
| --- | --- |
| Bytes in and out, and operations | A stream is judged in bytes, a small write in operations |
| Time to the first byte and to the last, for a read | A seek is the first; a stream is the second |
| Latency for each operation, as a histogram | Tails, and never a mean |
| What was left staged and undecided, and what was stale | Acknowledged throughput with a growing backlog is not sustainable throughput |
| The driver's CPU and how fast it made bytes | A driver that cannot make bytes as fast as the cluster takes them is measuring itself |
| The read back of everything acknowledged | A byte acknowledged and not there, or there and wrong, is a failure of the run |

Overrides vary one thing about the pool: its redundancy, its stripe size, its chunk unit, its
`f`. Events add to the ones F66 has: a device killed, a device added, a deep scrub asked for.
Each is cut into before, during and after as a kill or a rebalance is today.

### The bench's own cluster

The copy F66 makes of an inventory moves every root. It moves every device too, to a
directory beside it, and refuses a copy whose devices overlap the inventory's in either
direction. The wipe guard reads a device's marker before it wipes, as it reads a root's. No
object arm is ever pointed at the `tmdb` cluster's data.

### What the lab can show, and what it cannot

| It can | It cannot |
| --- | --- |
| What a core costs: checksums, encoding, the device store, the protocol. On loopback on europa | Throughput across hosts above about 117 MiB/s. Every such number is the network's and is labelled so |
| Two durable rounds against one, on a consumer SSD that flushes on every sync and on an Optane that barely does | A table's files and a pool's device on separate disks, until disks are fitted: each host has one |
| Recovery and scrub on real devices | A k+m wider than 2+1 across hosts |
| A rotational disk, once one is fitted | — |

A number taken on the lab is an A/B and not a capture, as every lab number has been since
the benchmarks moved there. It is quoted on the page that needed it, labelled by host, CPU
and governor, and nothing goes into `docs/perf/runs/` unless a lab corpus is asked for.

### The acceptance numbers

Initial budgets are hypotheses and not measured properties, as the Distributed chapter's
were. Each is agreed before the gate it judges and revised only with a recorded cause.

| Gate | Initial objective |
| --- | --- |
| Tables beside objects | The reference cell's p99 within 1.25 times what it is alone, with pools on their own devices |
| A streaming put, replicated | Bounded by the slower of the devices and the network, with the bound named; on the lab that is 1 GbE |
| A small write in place | No more than twice a table write's median on the same hosts, since it pays two rounds for one |
| A range read inside one stripe chunk, healthy | One lookup and one slice read: no decode, no second slice |
| A degraded read | Reported as a curve against `k`; no promised multiple |
| A rebuild | A device's worth inside a stated time at the default budget, with the foreground's p99 under twice its own |
| A deep scrub | The foreground's p99 within 1.25 times, at the default budget |
| A move | Zero final errors, and p99 under twice, the objective a tablet rebalance already has |

No objective is met by weakening a clause of the contract. A put that is fast because it
acknowledged before its bytes were durable is a different setting and is plotted as one.

## Alternatives rejected

**Extending `shoal-bench`.** It measures one schema it owns, and it is being retired in
favour of the tool this page extends
([todos](../appendix/todos.md#retiring-shoal-bench)).

**An existing object benchmark.** They speak S3. There is no S3 here to speak to.

**Operations a second alone.** A put of a gibibyte and a stat are both one operation.

**The inventory's own cluster.** F66 already says why: an arm that writes changes what the
next one reads.

## What it costs

- **A driver that makes bytes.** Seeded bytes at a device's rate take a core, and the
  capture has to show the driver was not the limit.
- **Reading back what was written** doubles an arm's I/O.
- **A reset between arms that wrote** is a bootstrap, minutes on the lab, as it is for
  tables.
- **Closed loop.** A stream that stalls is not charged for the stall. An open-loop generator
  is filed against F66 already and matters more here.

## What it breaks

- "Reads and inserts only" ([F66](../features/dataset-benchmarks.md#limitations)).
- "A window counts operations."
- "A dataset is a file of rows for each table."

What it does not break: an existing arm's id, the rule that nothing in the driver names a
schema, and the rule that `dataset` never reaches the fingerprint.

## Invariants to uphold

- An arm's id is never renamed, and a table arm's id is what it was.
- Nothing in `shoal-loadgen` or `shoaladm` names a bucket.
- Reads and writes in place ask only for objects that were preloaded.
- Every acknowledged byte is read back.
- The bench's devices are disjoint from the inventory's, and a marker is read before a wipe.
- A capture records the pool's shape, every device's class and filesystem and whether the
  kernel calls it rotational, and the driver's machine; `compare` refuses two that differ.
- A number is labelled by host, CPU and governor, and a number bounded by the network says
  so.

## Prerequisites

[S1](prerequisites.md#required): operation kinds beyond read and insert, and byte counters.
[S12](wire-and-client.md), since the driver drives the client.

## How it would be measured

This page is how. Its own first measurement is
[X13](spikes.md#x13-the-benchmarks-shape): whether the driver's kinds generalize or a second
driver is needed, and how fast one core makes seeded bytes.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `driver_runs_kinds_a_schema_supplies` | A mix naming an operation kind the driver was handed, and knows nothing else about, is weighted, picked, timed and reported as read and insert are | M11 |
| `driver_counts_bytes_both_ways` | A window's bytes equal what was sent and received, for rows and for a kind a test supplies | M11 |
| `object_arm_round_trips_against_one_node` | The driver over a described dataset against a node in process: preload, three mixes, nothing lost, nothing misread | M13 |
| `every_acknowledged_byte_is_read_back` | A write acknowledged and then damaged on disk fails the run | M13 |
| `bench_copy_moves_every_device` | A copied inventory's devices are disjoint from the original's, and an overlapping copy is refused | M14 |
| `compare_refuses_another_pool_shape` | Two captures whose pools differ in redundancy, devices or filesystem are not compared | M14 |
| `table_arm_ids_are_unchanged` | Every arm id a capture taken before this part holds is still produced | M11 |

## Related

[F66](../features/dataset-benchmarks.md) for the tool; [S13](isolation.md) for the budget
that matters most; [Spikes](spikes.md) for the measurements that come before any arm
exists; [Benchmarking](../performance/benchmarking.md) for the lab's rules;
[C10](../distributed/performance.md) for the form this page copies.
