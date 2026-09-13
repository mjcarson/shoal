# C10. Performance, and the benchmarks that judge it

## Context

The performance question has two parts: what distribution costs with fixed resources, and what
additional hardware buys. A replicated three-node cluster with RF=3 holds all data on every node;
that is a redundancy experiment, not proof of write/storage scale-out. Benchmark sustainable
committed work, latency tails and recovery debt rather than a short-lived acknowledgement rate.

## What exists today

`shoal-bench` runs one workload arm at a time, records provenance, compares committed captures and
renders the performance chapter. The frozen B1 baseline stays unchanged. Workload ids are stable
and their positions influence port allocation — since [F36](../features/cluster-harness.md) the
historical assignments are frozen in `docs/perf/ports.json` and a test holds every id to them,
and cluster arms get a block of their own above that range. Core selection is configurable, but
logical CPU 0 is excluded and the driver is not universally pinned. F36 added the
`ClusterFacts` record this page asks for below, absent from every single-node capture and named by
`compare` rather than compared across, and the separate load driver: `shoal-workload serve` starts
a workload's server and `run --server` drives it from another process. ~~No workload runs against a
cluster yet.~~ Since [F37](../features/node-identity-control-plane.md) the overhead arm runs
against a cluster of one, and since [F38](../features/inter-node-transport.md) the three hop arms
of the table below run against a two-node ~~static placement~~ cluster: the measured process stages the
identities, markers, disjoint physical cores and a port block, starts the peer as a `serve
--staged` child that joins node zero through its seed ([F39](../features/membership.md)), hosts
node zero in process, initializes the placement once the peer is up, and records the
initialization order, the committed members, the map version, the voter and learner counts,
every node's cores, the
hop the arm was built for with the mix its construction implies, and the data lane's frame and
shed counters on `ClusterFacts`. The driver is still in process with node zero, recorded as such.
~~`NoStorage::commit` does no serialization; its replication benchmark needs C5's common command
path.~~ Since [F40](../features/replication.md) three more arms run against a three-node
cluster of three shards a node: `macro/cluster/overhead/nodes/3` at a factor of one, and
`macro/cluster/replication/{durable,volatile}` at three on the persistent and the ephemeral
table, which share the command encoding C5 asked for. Every cluster record now carries
`offered_load` - the closed-loop depth the arm scheduled - `replicas`, every node's groups,
groups led, lag, pending and volatile bytes and unknown and rejected writes at the end of the
run, read from each node's own report, and `outcomes` summed; all three are mirrored into the
explorer with defaults, so every committed artifact still loads. An arm asking for more copies
than it places nodes is refused before a server starts. Since [F41](../features/read-consistency.md)
seven read arms run: `macro/cluster/reads/{one,barrier,session}` on the replication arms'
placement at a factor of three, differing only in what the read asks for, and
`macro/cluster/fanout/{get,filter,limit,empty}` on the same nodes at a factor of one, a six key
get split three ways in four shapes. Every read arm's record carries `reads` - the level, the
session flag, the fanout, and every node's barriers, hops, barrier and application wait means
and maxima, session waits, timeouts and late and duplicate shares - mirrored into the explorer
the same way. The harness waits for every peer to hold the placement before it seeds, and the
cluster port blocks are numbered among the cluster arms so none sits in the ephemeral range
(`ClusterOverride::feasibility`), which is what "not a Cartesian product that silently lowers
RF" means in code. Since [F42](../features/primary-failover.md) one fault arm runs:
`macro/cluster/failover/kill`, the durable replication arm's placement and mixture driven for
a fixed time by a client that does not retry, with node one killed a third of the way through
and started again from the same identity two thirds through by a thread of the harness on the
driver's clock. Its record carries `cluster.fault` - the kind, the node, the kill and restart
marks, the client's first failure and its first sustained success, the outage between them,
three windows each with its own distribution, and a per second series of operations, errors
and percentiles - which is the time series this page asks for below, mirrored into the
explorer whole; the timed driver behind it counts a failed operation rather than ending the
run. Since [F43](../features/node-recovery.md) two catch-up arms run:
`macro/cluster/catchup/{log,snapshot}`, the kill arm's shape with the survivors' retention at
the defaults and shortened past what the returning node missed, the returning node sampled
each second once it is placed again with its lag judged against node zero's committed index
per group. Their record carries `cluster.catchup` beside `cluster.fault` - how the node caught
up, the restart and convergence marks and the seconds between them, the bytes and entries the
snapshots moved and the entries the log fed, and a per second series of its lag - and says
`none` with the series kept when a run ends before the lag is held at zero, which is what both
arms recorded at smoke scale on the development host, where the outage outlasts the absence.
Since [F44](../features/repair.md) the background arm runs: `macro/cluster/background/repair`,
the kill arm's placement and mixture with nothing killed and a `Repair` of the reference table
in verify mode asked for a third of the way through, its record polled until every group is
done. Its record carries `cluster.background` - the marks, the groups and how many were clean,
what the scrubs hashed and read across every node, and the client's distribution before,
during and after with a per second series - which is the interference this page asks for.
The open-loop schedule is not built; the arms are closed loops at one depth.

## The design

### What distribution costs

| Experiment | Hold constant | Learn |
| --- | --- | --- |
| Standalone versus cluster RF=1 | Same commit, data, drivers, cores, storage policy and load | Routing/metadata/replication-adapter overhead without network replication |
| Local shard, another local shard, remote node | Same read-only data and query | Ownership and network/validation/merge cost |
| RF=1 versus RF=3 on the same hardware | Placement-aware routing, data size, read/write policy and offered load | Replication cost including every follower's work |
| RF=3 committed versus optional accepted-only write | Same topology and command mix | Acknowledgement latency tradeoff, with distinct semantics visibly labeled |
| One versus barrier versus session reads | Same replica placement, query and write background | Freshness cost and application wait |
| Idle and active groups versus tablet/table count | Same cores and aggregate active load | Consensus state, timers, heartbeat batching and scheduling cost |
| Add/repair/recover while serving | Same foreground offered load and data | Tail latency, errors, resource competition and convergence |

The Raft integration spike measures candidate memory/group, idle CPU, message rate, storage batch
size and active throughput. Try realistic table counts as well as one table: 4096 groups per table
can dominate before data arrives. Compare coarser grouping only with its lost migration/leadership
independence recorded. No library wins solely by a microbenchmark without durability semantics.

### Emulating a cluster on one machine

#### How

Use N server processes and a separate load driver. Assign explicit disjoint physical-core slices
for data shards, control runtimes and driver, excluding SMT siblings where isolation is claimed.
C1's configurable control core prevents every emulated node sharing CPU 0. Respect actual allowed
cpusets/NUMA topology and record sharing if the machine cannot support full isolation. Keep the
total control-plus-data CPU budget fixed for overhead experiments, not just data shard count.

Give each node independent storage directories and client/data/control ports. Partitioning tests
need every actual endpoint routed through the harness, including reconnects and direct control
connections. Run one complete cluster arm at a time; simultaneous processes inside that arm are
intentional, independent benchmark arms remain serialized.

The driver owns workload generation and latency recording; do not colocate node 1 inside the
same pinned driver process unless all runtime/thread placement has been explicitly accounted for.
Record every node's resource allocation, not just the coordinator. Put test storage on a filesystem
that exercises intended direct-I/O behavior; tmpfs is a separately labeled experiment.

#### The caveats

Shared disks, caches, memory bandwidth, NUMA paths and the kernel remain shared failure/performance
domains. Persistent emulation measures replication plus device contention. Ephemeral twins remove
durable storage work but add their own common replication serialization; their difference is a
useful comparison, not an exact additive decomposition of nonlinear contention.

Loopback does not emulate network bandwidth, congestion or independent machine failures. Never
multiply loopback latency by an RTT ratio and present it as a cluster forecast. Add deterministic
proxy latency/bandwidth/fault controls for protocol experiments and optional network namespaces/
netem for packet-level studies. Real hardware measurements remain necessary for capacity claims.

### Scaling and heterogeneous nodes

Run both fixed-resource distribution and scale-out experiments. Fixed resource counts test overhead;
scale-out holds resources per node and RF fixed while adding nodes, then tests fixed-data and
proportionally growing-data cases. N>RF is necessary to test partitioned write/storage gains.
Four or more emulated nodes can establish the workload shape even if only three real machines are
available; label that limitation and do not claim physical scale-out from it.

At N=RF each node stores/applies every mutation. Quorum latency may omit a slow follower, but
sustainable throughput cannot ignore indefinitely growing lag. Record that follower's disk/apply
rate and test headroom for recovery. Weight placement by feasible capacity when N>RF, and vary
primary placement on the unequal three real nodes. Include skewed traffic, a hot tablet, a hot
partition and a slow replica. Splitting a range cannot divide a single partition automatically.

Measure per-node standalone capacity under the same software/policy as useful context, plus
within-cluster paired comparisons with stable placement and driver allocation. Do not compare a
new three-machine absolute number to frozen one-machine B1 as if the difference were software.
Record CPU model/count, affinity/NUMA/SMT, memory, kernel, device/path/fsync policy, network link,
RTT distribution, versions, placement/configuration and active feature set for every node.

### The workloads

Append stable ids; do not rename or interleave existing ones. These are workload families to be
expanded only into feasible combinations, not a Cartesian product that silently lowers RF:

| Family | Purpose |
| --- | --- |
| `macro/cluster/hop/{same_shard,local_shard,remote_node}` | Read-only transport and ownership control |
| `macro/cluster/groups/{idle,active}` | Table/tablet count and batching/library spike |
| `macro/cluster/overhead/nodes/{1,2,3}` | Fixed total resource budget; explicit feasible RF/policy. `1` since F37, `3` since [F40](../features/replication.md) at three shards a node - not the one-node arm's twelve, which is why the three-node arms are read against each other |
| `macro/cluster/replication/{durable,volatile}` | Same command encoding and workload with distinct durability contracts. Both since [F40](../features/replication.md), on the `nodes/3` placement at a factor of three |
| `macro/cluster/scaleout/nodes/{3,4,6}` | RF=3, resources per node fixed; emulated cases explicitly identified |
| `macro/cluster/reads/{one,barrier,session}` | Read-only ~~and write-background~~ consistency costs. All three since [F41](../features/read-consistency.md), on the `replication/` placement at a factor of three and read against each other; the write-background variant is filed with the open-loop schedule |
| `macro/cluster/fanout/{get,filter,limit,empty}` | Remote gathering, decoding, coverage and ordering. All four since [F41](../features/read-consistency.md), on the `nodes/3` placement at a factor of one |
| `macro/cluster/writes/{insert,update,delete,conditional,retry}` | Result derivation, no-ops, deduplication and hot-key behavior |
| `macro/cluster/failover` | Outage and recovery under a specified fault schedule. `kill` since [F42](../features/primary-failover.md), on the `replication/` placement at a factor of three; a pause and a partition are the fixture's |
| `macro/cluster/catchup/{log,snapshot}` | Time/bytes to catch up ~~at several foreground mutation rates~~ at the reference mixture. Both since [F43](../features/node-recovery.md), the `failover/kill` arm with the retention at the defaults and shortened past the absence; the mutation rate sweep is filed with the open-loop schedule |
| `macro/cluster/rebalance/{add,decommission,remove,capacity_blocked}` | Transition progress and supported load envelope |
| `macro/cluster/background/{repair,backup}` | Foreground interference, integrity work and restore preparation. `repair` since [F44](../features/repair.md), the `failover/kill` arm with a verify of the table in the background and nothing killed; `backup` is M10's |

Extend ScaleFacts with separate read/write/durability policies, node count, desired/active RF,
data/control/driver core allocation, table/tablet count, offered load and dataset size. Mirror
portable fields in shoal-top and keep old artifacts parsing via optional defaults. Add a separate
cluster environment record and comparability verdict so historical single-node records remain
meaningful. Full profiles and traces cover all nodes or clearly identify partial attribution.

Preserve historical single-node port allocations. Add an explicit cluster port-range allocator
(or bind-zero with actual endpoints recorded) with room for all three endpoints per node and
fault proxies. Validate disjoint ranges and the u16 boundary; multiplying every existing port by
a new block size both changes old assignments and does not itself solve collisions.

### Measurement protocol

Warm up, then drive multiple offered-load levels, including an open-loop schedule or equivalent
latency accounting that includes waiting before request dispatch. Record p50/p95/p99 and maximum
latency, completed committed ops/sec, rejected/unknown outcomes, retries, queue bytes and replica
lag throughout. Closed-loop arms remain useful but cannot alone expose overload pauses.

Run long enough to reveal replication/compaction debt and demonstrate stable lag, then measure
drain/recovery. A capacity result passes only within an explicit latency/error/lag envelope.
Record raw distributions/time series around faults; do not average the outage away. Driver CPU,
network and scheduling must have headroom, and scheduled versus actual issue times reveal a
saturated generator. Use repeated paired runs and measured spread, with confidence/variability
reported rather than treating an arbitrary noise band as an unlimited acceptance margin.

### The acceptance numbers

Initial numeric budgets are hypotheses, not measured properties. Agree concrete budgets before
capturing each milestone and revise them only with recorded causes and tradeoffs:

| Gate | Initial objective |
| --- | --- |
| Standalone/one-node overhead | No material regression outside paired-run variation; investigate routing, apply and adapter costs |
| Loopback hop | Initial p50 added-latency budget 100 µs, with tails also reported |
| Durable replication | Report complete latency/throughput curves and lag; no universal 1.5×/2× promise across devices |
| Fixed-resource distribution | Initial read-throughput objective at least 0.8× matched single node, with latency/error envelope |
| Failover | Initial timeout-base + 2s objective only for healthy survivors, bounded delay and specified backlog |
| Healthy rebalance | Zero final operation errors within supported load/deadlines; initial p99 inflation budget 2× |
| Recovery | Bounded bytes/memory/disk and convergence under documented mutation rate/headroom |

RF=3 on one initialized replica is an unavailable-policy test, not a throughput arm claiming a
quorum. An optional `One` accepted-only result is never plotted as equivalent to committed success.
*At M4:* the durable replication gate has its first numbers, at smoke scale on the development
host and recorded on the [F40 page](../features/replication.md#performance) as not a capture -
a durable quorum at about twice a single fsync's median on a shared device, a volatile one
under two milliseconds; the curve, and whether the leader's flush overlaps its followers', is
the benchmark host's to draw ([O47](../appendix/optimizations.md)). *At M5:* the read gate has
its first numbers the same way, on the [F41 page](../features/read-consistency.md#performance):
a barrier about a millisecond over a `One` read at the median, of which the barrier wait itself
is 590 µs on average with the hop on two reads in three, an application wait of nothing with no
writes running, and a session read within a tenth of a millisecond of `One`.

## Alternatives rejected

Assuming replica-byte weighting works at N=RF, deriving real-network capacity from loopback ratios,
measuring only p50, and hiding a slow follower behind the fastest quorum are superseded. The
initial guesses are replaced by comparable experiments with visible semantics and resource budgets.

## What it costs

More environment/progress facts and longer steady-state/fault captures. Keep small smoke arms for
correctness and select groups for meaningful captures. Follow repository rules: commit executable
changes first, preserve B1, capture clean, render generated pages and commit rendered results
separately. This documentation-only change captures nothing.

## What it breaks

Harness process/port/environment modeling expands, while historical ids, files and frozen results
remain readable. Existing generated performance pages are not edited by hand.

## Invariants to uphold

- Compare like durability, completed-operation semantics and accounted resource budgets.
- Sustainable results include all replicas' work and bounded lag/debt.
- Emulation and physical-node measurements are visibly distinct.
- Faults report time-series windows, not a single averaged throughput.
- Existing workload identities and frozen baseline provenance remain intact.

## Prerequisites

M0 harness and C13 Q1/Q8/Q13; C2 onward supplies each measured path. No benchmark of an unbuilt
protocol is claimed as evidence.

## How it would be measured

The experiment and measurement tables above define the captures; [milestones](milestones.md)
assign the gates. Generated cluster pages retain the book's scope/comparability explanations.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `cluster_fixture_accounts_for_all_cores_and_endpoints` | Driver/control/data allocations and port ranges are recorded and disjoint where claimed | M0 |
| `historical_artifacts_and_ports_remain_compatible` | Existing facts parse and historical single-node port assignments remain unchanged | M0 |
| `infeasible_rf_policy_is_not_a_throughput_arm` | Desired RF=3 on one node yields an availability test, not downgraded quorum throughput | M4 |
| `capacity_capture_records_lag_and_offered_load` | Capacity records include scheduled load, completion/error/tail and every replica's debt | M4 |
| `read_capture_records_barrier_and_application_wait` | A read arm's record carries the level, the session flag, the fanout and every node's barrier and application waits, summed and per node; an older record still loads | M5 |
| `fault_capture_preserves_outage_time_series` | Failure/recovery window remains visible with separate before/during/after distributions | M6 |
| `catchup_capture_records_convergence` | A returning node's record carries its restart and convergence marks, the split by log and by snapshot and the lag series; a run that ends unconverged says so and keeps the series | M7 |
| `background_capture_records_scrub_interference` | A background repair's record carries its marks, the windows before, during and after it with their own distributions, a bucket per second and what the scrubs read; a run with no repair is all `before`; an older record loads without it | M8 |
| `physical_cluster_records_each_node_environment` | Unequal real hardware and primary placement are retained in comparability metadata | M10 |

## Related

[C5](replication.md), [C7](failover.md), [C8](rebalancing.md), [C11](testing.md),
[Benchmarking](../performance/benchmarking.md), [Baseline](../performance/baseline.md),
[C13](protocol.md) protocol-cost gates.
