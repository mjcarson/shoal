# S13. Sharing a node with tables

## Context

A node that serves buckets still serves tables, and a table's read is answered in tens of
microseconds ([Row size and what it costs](../tables/row-size.md#the-shape)). Object work
is the opposite kind: megabytes checksummed, encoded, written and synced at a time, and
recovery and scrub that would use every idle cycle and every idle sector if nothing held
them back. "Performance is important" (R14) cuts both ways here. An object store that is
fast and doubles a table's tail is a regression.

This page is where object work runs, what it may use, and what keeps the constraint the
[overview](overview.md#the-constraint-every-page-inherits) states: tables keep their
latency.

## What exists today

- **One executor a core, each owning its files.** A shard is a glommio executor pinned to a
  core, placed by `PoolPlacement::MaxSpread`
  (`shoal-core/src/server/shard.rs:4855`), and nothing on disk is shared between two
  ([Thread per Core](../architecture/thread-per-core.md)).
- **Two task queues a shard**: a high priority queue at 1000 shares with a latency goal of
  500 µs, which runs the listeners and every connection, and a medium one at 500 shares and
  100 ms for the compactor, the loader and the sweeper (`shard.rs:1700-1710`).
- **Nothing yields inside a computation.** No call to glommio's yield appears in
  `shoal-core/src`; a task gives up its core where it awaits and nowhere else. Row work is
  short enough that it has not mattered.
- **Memory is rows.** `resources.memory` is what a shard's resident partitions may sum to,
  and `node_memory` is judged against the process's resident set, at most four times a
  second (`shard.rs:910`, `:4251`); a node past it has every shard evict rows
  (`shoal-core/src/server/conf.rs:88-94`). The book has already met memory that was not
  rows squeezing rows out: openraft's channels, allocated to their bound
  ([Resolved #191](../appendix/resolved/raft-channels-preallocated.md)).
- **Four lanes between nodes**, each a socket of its own with a bound in bytes, because
  "bytes already written to a socket cannot be preempted"
  ([C2](../distributed/transport.md#design-choices)). A frame naming a slot is handed to
  the executor that hosts it (`shoal-core/src/server/peer/listener.rs:181`), and a peer
  never learns an executor's number ([F47](../features/local-rehome.md)).
- **A storage failure stops a node.** A node whose WAL cannot be written stops, by design
  ([Resolved #156](../appendix/resolved/wal-failure-stops-the-node.md)).

## The design

### Who owns a slice

**One executor does all of a slice's I/O.** That is the existing rule, "per-shard files, no
coordination", applied to a new kind of file, and it is what lets
[S6](device-store.md#reads) run a read and an apply of one stripe chunk in turn without a
lock.

Which executor is the node's own business, recorded beside its hosting table and never
sent to a peer. A frame names a slice; the node turns that into an executor in the one
place it turns a slot into one.

A device one core cannot keep busy is one slice, and that slice may share its executor with
other slices. A device one core cannot drive is given several slices, each owned by an
executor of its own. That is why the failure domain is the device and not the slice: the
slices of one disk fail together, however many cores drive them
([S5](placement.md#failure-domains)). How many slices a device is given, and which share an
executor, are an operator's choices, informed by [X6](device-store-ssd.md#8-one-device-several-slices):
one slice drives the lab's SSDs for 64 KiB units and whole chunks. A slice's executor thread can
show a whole core busy while it waits on a fast device, which says little about what it needs
([O89](../appendix/optimizations.md#o89-a-reactor-waiting-on-a-fast-device-does-not-sleep)).
Its blocking thread, where every rename and unlink goes, belongs on its core's sibling: glommio's
`Placement::Fixed` puts it on the executor's own cpu.

### Shared executors or dedicated ones

| | Object work on the table shards | Executors of its own |
| --- | --- | --- |
| How | A third task queue on each shard, below the other two | Cores named for object work in `resources`, each an executor owning slices |
| Isolation | By shares and by yielding. A computation that does not yield is a stall for every table on that shard | By construction. A table's shard never runs object code |
| Cost | No cores. Every loop has to be cut into steps short enough for a 500 µs goal | Cores. On a four-core host one is a quarter of the node |
| Crossings | None | A hop between executors for a client's bytes and for a commit |

**Dedicated executors are preferred** where a node has the cores, and
[Q24](contract.md#questions-to-answer) is whether a small node can do without them.
[X9](spikes.md#x9-table-latency-beside-object-work) measures both before either is built
on: it is the experiment this whole page waits for.

Whichever it is, **no call runs long**. A megabyte encoded at a gibibyte a second is a
millisecond, twice the high queue's latency goal. Checksumming and encoding are done a
chunk unit at a time with a yield between units, which makes this the first code in the
engine that yields in the middle of a computation. The checksum is the cheaper half:
[X5](checksums.md) found CRC-64/NVME holds a Zen1 core 5.3 µs for a 64 KiB unit and 85 µs
for 1 MiB. Fed in pieces, it keeps eight bytes of state between them, so a yield can fall
inside a unit as well as between units.

### A lane for object bytes

Bytes between nodes travel on a **fifth lane**, for the reason there are four: a megabyte
of stripe chunk already written to a socket would sit in front of a vote, a forward or a snapshot
chunk behind it. The lane has its own socket for each executor and peer, its own bound in
bytes, its own capability bit in the peer hello, and is gated by an activated wire version
as every addition since [F48](../features/rolling-compatibility.md) has been. It is
encrypted as the others are.

Its frames name a slice, a chunk and a label, and carry bytes that are not an archive:
stage and its answer, apply, read and its bytes, and a slice's inventory of a placement
group. Their layouts are left until [Q14](contract.md#questions-to-answer) and
[Q15](contract.md#questions-to-answer) say who sends them.

An accepted connection lands on whichever shard the kernel chose. A frame for a slice
owned elsewhere is handed across as an owned buffer, as a snapshot chunk is handed to its
slot today. Handing the connection itself to the owner would save that hop for every
frame; it is not something the glommio fork is known to allow, and
[X11](spikes.md#x11-streamed-bodies) finds out ([S1](prerequisites.md#optional)).

### Memory

Object work holds memory that is not a row: a stream's window, a stage in flight, the units
of a partial write, the `k` units of a rebuild, what is read ahead.

**It is budgeted, and the budget is the node's to check.** Each executor that does object
work has a budget every such buffer is drawn from. An operation that cannot draw what it
needs waits, and past a bound is shed by name, as a query to a full shard is shed today. At
start, the rows' budgets and the objects' are added and refused if they exceed
`node_memory`.

Without that, object buffers would grow the resident set, the node would cross
`node_memory`, and every shard would evict rows to make room for bytes that are not
theirs: the failure of item 191 again, with a different guest.

### I/O on a slice

One executor ordering one slice's work makes a priority order possible, and it is:

1. reads and stages for foreground operations;
2. applies of committed writes, batched, and on a rotational disk in offset order;
3. rebuilds and moves, inside the device's byte budget ([S10](recovery.md#budgets));
4. scrubs, inside the same budget, last ([S11](scrub.md#schedule-and-budget)).

Applies are below stages on purpose. A staged write is already durable and already readable,
so applying it late costs journal space and nothing else.

**A table's files and a pool's device may be the same disk.** On the lab they are: each host
has one device, so a stage's sync and the WAL's sync queue for the same flush. The promise
about table latency is made for a node whose tables and whose pools are on different
devices, and a node where they are not is measured and labelled as that.

### A failing pool device does not stop its node

A device that returns errors is marked failed by its node and reported, and every slice on
it fails with it; their chunks are rebuilt elsewhere ([S10](recovery.md)). Their executors go on
with their other slices, and no tablet group notices. That is
[P17](contract.md#the-contract), and it is the opposite of what a failing WAL does,
deliberately: a WAL is the node's ability to promise anything, and a pool device's slices
are among several holders of data the pool was sized to lose.

## Alternatives rejected

**Object work on the queue that serves connections.** It is where a client's bytes arrive,
and it is the queue with a 500 µs goal.

**A process for each device**, as Ceph runs an OSD. Isolation by process is real, and it is
a second program to build, deploy, upgrade and authenticate, speaking to the first over a
socket. The executor is the unit this engine already has.

**Buffers nobody counts.** See [Memory](#memory).

**The bulk lane.** It exists and it carries large frames. It is also the path a group's
snapshot takes, with a protocol judged against a group, and object bytes on it would queue
with a recovering replica's.

**Trusting shares alone.** Shares divide a core between queues that yield. They do nothing
about a task that does not.

## What it costs

- **Cores**, where executors are dedicated.
- **A lane**: as many more links as there are object executors and peers, each with buffers.
- **Memory set aside** that rows could have used.
- **A hop between executors** for bytes that arrive on one and belong to another.
- **Yielding in the middle of work**, which makes a checksum loop a state machine and costs
  a little throughput for a bounded stall.

## What it breaks

- "Every executor is a shard and every shard is alike."
- "Four lanes on two ports."
- "`node_memory` is rows, and what else the process happens to hold."
- "A task yields where it awaits."
- "A storage failure stops the node": not a pool device's.

## Invariants to uphold

- A slice's files are touched by one executor.
- A peer names a slice and never an executor.
- Every buffer of object work is drawn from a budget, and the budgets fit inside
  `node_memory`.
- No object computation runs past the latency goal of the queue it is on without yielding.
- The object lane is bounded in bytes and sheds before it queues without limit.
- A pool device's failure stops no tablet group and no other device's work.
- Scrub yields to rebuild, rebuild to apply, apply to foreground.

## Prerequisites

[S4](pools-and-devices.md) for devices and slices. Handing a connection to another executor,
only if X11 says the hop costs too much ([S1](prerequisites.md#optional)).

## How it would be measured

[X9](spikes.md#x9-table-latency-beside-object-work) is this page's experiment. It runs the
workload grid's reference cell, `macro/grid/unsorted/r50/1024`, beside a task that does
what object work does (checksums, encodes and direct writes at a set rate), once on the
table shards' own executors and once on a core of its own, and reports the table's p99 in
each case against the same cell alone. It follows the lab's before-and-after procedure,
since anything that adds work to a node's query path is measured that way
([Benchmarking](../performance/benchmarking.md#before-and-after-on-the-lab)).

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `tables_survive_pool_device_loss` | A pool device failed by an injected fault stops no tablet group; tables on the node answer throughout | M14 |
| `object_buffers_never_evict_rows` | Under a load of stages and reads inside its budget, a shard's resident rows do not shrink | M14 |
| `object_work_past_its_budget_is_shed_by_name` | An operation that cannot draw its buffers is refused with a code its caller can retry on, and nothing grows | M14 |
| `a_slice_is_served_by_one_executor` | Every read, stage and apply of a slice runs on the executor recorded as its owner | M14 |
| `stalled_object_lane_leaves_the_others_answering` | A peer that stops reading object frames holds only that lane's queue; votes, forwards and snapshots proceed | M15 |
| `rotational_applies_are_batched_in_offset_order` | On a device marked rotational, a batch of committed applies is written in offset order, and a foreground read waits for no more than one batch | M19 |

## Related

[S6](device-store.md) for what an owning executor does; [S10](recovery.md) and
[S11](scrub.md) for the work that has to be held back; [S12](wire-and-client.md) for the
client's side of a stream; [S15](performance.md) for the budget;
[Thread per Core](../architecture/thread-per-core.md) and
[C2](../distributed/transport.md) for the rules this page extends.
