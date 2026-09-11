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
a workload's server and `run --server` drives it from another process. No workload runs against a
cluster yet. `NoStorage::commit` does no serialization; its replication benchmark needs C5's
common command path.

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
| `macro/cluster/overhead/nodes/{1,2,3}` | Fixed total resource budget; explicit feasible RF/policy |
| `macro/cluster/replication/{durable,volatile}` | Same command encoding and workload with distinct durability contracts |
| `macro/cluster/scaleout/nodes/{3,4,6}` | RF=3, resources per node fixed; emulated cases explicitly identified |
| `macro/cluster/reads/{one,barrier,session}` | Read-only and write-background consistency costs |
| `macro/cluster/fanout/{get,filter,limit,empty}` | Remote gathering, decoding, coverage and ordering |
| `macro/cluster/writes/{insert,update,delete,conditional,retry}` | Result derivation, no-ops, deduplication and hot-key behavior |
| `macro/cluster/failover` | Outage and recovery under a specified fault schedule |
| `macro/cluster/catchup/{log,snapshot}` | Time/bytes to catch up at several foreground mutation rates |
| `macro/cluster/rebalance/{add,decommission,remove,capacity_blocked}` | Transition progress and supported load envelope |
| `macro/cluster/background/{repair,backup}` | Foreground interference, integrity work and restore preparation |

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
| `fault_capture_preserves_outage_time_series` | Failure/recovery window remains visible with separate before/during/after distributions | M6 |
| `physical_cluster_records_each_node_environment` | Unequal real hardware and primary placement are retained in comparability metadata | M10 |

## Related

[C5](replication.md), [C7](failover.md), [C8](rebalancing.md), [C11](testing.md),
[Benchmarking](../performance/benchmarking.md), [Baseline](../performance/baseline.md),
[C13](protocol.md) protocol-cost gates.
