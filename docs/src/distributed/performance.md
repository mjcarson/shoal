# C10. Performance, and the benchmarks that judge it

## Context

The ask carried one constraint through every requirement: distribution must not drastically slow
down reads or writes. Every `C` page has said what its piece costs; this page says what would
*measure* each cost, how a cluster can be measured on the one machine the benchmarks run on, what
that machine cannot tell you, and what the first real three-node capture can and cannot say given
that its nodes will not be identical.

It follows the rule [Optimizations](../appendix/optimizations.md#how-these-are-ranked) set for the
whole book: a claim about performance is not acted on until a benchmark exists that would show the
difference, and the claim names it. The claims here are hypotheses, stated as numbers to be
replaced by measurements, and the [milestones](milestones.md) make the first measurement an exit
criterion.

## What exists today

**The harness runs one server process at a time, on purpose.** `shoal-bench/src/groups.rs:23`:
"Nothing here makes anything run concurrently. A capture runs one `shoal-workload` process at a
time" — because two servers share a page cache, a device queue and a set of cores.
`harness::run` starts exactly one `ShoalPool` in the workload process
(`shoal-bench/src/workloads/harness.rs`), and its module doc gives four reasons for one process
per workload, the second of which is load-bearing here: "glommio pins its shards to cores at start
and there is no supported way to tear that down" (`harness.rs:10`), so a second pool in one process
is not an option.

**Cores are already sliceable.** `Resources { cores, exclude_cores, memory }`
(`shoal-core/src/server/conf.rs:26`) and `Resources::cpus()` (`conf.rs:72`): cpu 0 always dropped,
every cpu on an excluded *physical core* dropped (both SMT threads), the rest sorted and filled one
per physical core first. The committed `shoal.yml` uses it to leave physical cores 12–15 to the
client ([Performance Baseline](../performance/baseline.md#hardware)). Two servers on disjoint cores
of one machine is expressible today with no engine change.

**Every port is a position.** `port_for` (`shoal-bench/src/run/plan.rs:317`) is
`BASE_PORT + position in workload_ids::IDS`, one port per arm, so an arm binds the same port in
every capture. A node needs two ports and a cluster needs `2N`.

**The facts a workload records are the axes the explorer can sweep.** `ScaleFacts`
(`shoal-bench/src/model/macro_layer.rs:90`) — `rows`, `row_bytes`, `keys`, `concurrency`, and the
optional `clients`, `read_pct`, `row_profile`, `distribution`, `table_kind`, each `None` meaning
"not that kind of workload" — mirrored into `ScaleFactsLite` and `SweepAxis`
(`shoal-top/src/index.rs:171`, `:577`), which is what lets the explorer draw a value against a
swept fact ([F29](../features/benchmark-explorer.md)).

**The `shards` sweep is the precedent.** `macro/conf/resources/shards/r{50,100}/{1,2,4,8,12}`
(`shoal-bench/src/workloads/conf_sweep.rs:286`) is the grid's reference cell with one field moved
— how throughput scales with cores on one node. A "how it scales with nodes" sweep is its
structural twin.

**The baseline is one machine and it is frozen.** `B1-performance` was captured on a 9950X with
an Optane SSD and never overwritten ([Performance Baseline](../performance/baseline.md)); every
number in the book is read against it, and it says nothing about a second machine.

## The design

Five things, each a subsection: what each path costs and what measures it; how a cluster is
emulated on one machine and what that cannot say; the workloads and the harness changes behind
them; how a capture on real, unequal hardware is recorded; and the numbers every milestone exits
on, stated as hypotheses.

### What distribution costs

By path, with the workload that would show it. Each row is a hypothesis.

| Path | What is added | Expected | Measured by |
| --- | --- | --- | --- |
| Read at `One`, replica on the coordinator's node | Nothing but a wider map lookup and a level check | Inside the noise band | `nodes/3/rf/3/cl/one` vs `nodes/1/rf/1/cl/one` |
| Read at `One`, replica elsewhere | One socket hop each way, one `bytecheck` on the owner, one mesh relay if the kernel picked the wrong shard | The hop: tens of µs on loopback, hundreds on a network | `cluster/hop/{local,remote}` |
| Write at `Quorum`, RF=3 | The primary's fdatasync **or** the RTT plus the slowest quorum follower's fdatasync, whichever is later; one `Replicate` per staged buffer per follower | On Optane over loopback: near the single-node number. On consumer NVMe over a network: the follower's fsync, ~2× | `nodes/3/rf/3/cl/quorum` vs `cl/one` |
| Write at `One`, RF=3 | The fan-out's socket writes, no wait | Noise | `nodes/3/rf/3/cl/one` (writes) vs `rf/1` |
| Read at `Quorum`, RF=3 | `RF/2 + 1` fetches per tablet, a `u64` compare per partition | 2–3× the read traffic; latency = slowest of two | `nodes/3/rf/3/cl/quorum`, read-only arm |
| Fan-out get across nodes | Remote shares unsealed on the coordinator | Proportional to remote share count | `fanout/*/nodes/3` |
| The intent header | Fourteen bytes per record, every replica | Under noise at 1 KiB rows; measurable at 64 B | `nodes/1/rf/1` at `row-size` widths |
| Failover | A window, not a rate | `primary_failover_after` + detection + one Raft RTT | `cluster/failover` — reported as a duration |
| A rebalance under load | Streams sharing the device with the workload | p99 up during the move, zero errors | `cluster/rebalance/*` — a delta and a duration |

**The one number that matters first** is the top row's twin for writes:
`macro/cluster/nodes/1/rf/1/cl/one` against today's `macro/grid/unsorted/r50/1024`. A single node
with a `cluster:` block, one copy, no peers: the header, the widened map, the quorum gate with
`needed = 1`, and the control-plane thread on a core no shard uses. **The hypothesis is that it is
inside the reference cell's run-to-run spread**, and if it is not, the `C` pages have a cost they
did not name, and [M4](milestones.md#m4-replication-and-quorum-writes) does not exit until they do.

### Emulating a cluster on one machine

The ask: can distribution across nodes be emulated by pinning each "node" to a slice of cores on
one machine? **Yes, with three caveats that decide which arms are honest.**

#### How

`N` **processes**, not `N` pools — glommio's core pinning has no teardown, per the harness doc, and
[C1](node-identity.md) gives every node its own control-plane thread on cpu 0, which two pools in
one process could not both have. Each process gets:

- a disjoint core slice: `resources.cores` and `resources.exclude_cores`, computed by the harness
  from the machine's physical core list — on the development machine's sixteen physical cores,
  with 12–15 kept for the client as today, three nodes get four physical cores each
  (`exclude_cores` listing the other eight plus the client's four); cpu 0 is dropped by every
  process regardless, so node 1 has three shards and nodes 2 and 3 have four, which the harness
  records in `ScaleFacts` rather than hides;
- its own storage subdirectory under the workload's, `<slug(id)>/node-<n>`, the way
  `harness/conf.rs::resolve` already gives every workload its own
  ([F8](../features/purpose-built-workloads.md));
- its own `networking.port` and `cluster.port`, from a port **block** per arm;
- `cluster.seeds` naming node 1, which bootstraps.

The client half of the workload — the tokio runtime that drives queries — is pinned to the client
cores with `taskset`, which the harness does not do today (its runtime is unpinned, `harness.rs`)
and must for this, or the driver competes with node 3.

`shoal-workload` gains a `serve --node <n> --of <N> --arm <id>` subcommand that starts one node
of an emulated cluster and blocks; `harness::run` for a cluster arm spawns `N − 1` of them, runs
node 1 in-process as today, waits for every node to answer `Admin::Members` with `N` members `Up`
(the probe [F8](../features/purpose-built-workloads.md) built, made cluster-aware), seeds, runs,
and stops them in reverse. The four reasons for one-process-per-workload still hold: hotpath's
profile is per process, and a cluster arm reports the *coordinator* node's profile and says so.

#### The three caveats

Written as the page's *what would make it wrong*, because they are exactly that:

1. **One device.** Every node's intent log and archives are on one NVMe with one queue and one
   page cache — and the intent log's whole write path waits on `fdatasync`
   ([Storage Overview](../storage/overview.md#limitations)), which three nodes now issue against one
   drive. A `Quorum` write on the emulated cluster measures replication *and* device contention,
   and the two cannot be separated in that number. **The honest comparison is on the ephemeral
   controls** ([F9](../features/ephemeral-tables.md)): `cluster/hop` and the `nodes/rf/cl` sweep
   over an ephemeral table measure the transport and the gate with no device in them, and the
   persistent arms are read beside them as "and then storage". The development machine has a
   second drive (`baseline.md` names a Samsung 990 PRO Shoal is not on); putting node 2's storage
   there is a partial answer and changes the number in its own way, so it is a separate arm if it
   is done at all.
2. **One L3, mostly.** The 9950X has two CCDs and two L3 instances (`baseline.md`); two nodes on
   opposite CCDs share nothing, two on the same CCD share L3. The harness assigns slices so that no
   node straddles a CCD and records which CCD each got.
3. **No network.** Loopback RTT is tens of microseconds; a datacenter RTT is a few hundred. Every
   hop cost on this page is a loopback cost, and the page that reports it multiplies by the ratio
   to say what a real link would add — as an estimate, labelled as one, never as a measurement.

What the emulation *does* answer, and answers well: does the transport cost what [C2](transport.md)
says, does the gate release when it should, does a `One` read stay local, does failover complete
in the window, does a rebalance produce zero errors. Those are properties of the protocol, and the
protocol does not know it is on one machine.

### The workloads

A `cluster` family in `shoal-bench/src/render/family.rs`, a `Surface::Cluster` and its page
`docs/src/performance/cluster.md` (generated; registered in `SUMMARY.md` under Performance), and
identifiers **appended** to `workload_ids::IDS` and `workloads::all()` in this order, never
interleaved, because position is port:

| Identifier | Isolates |
| --- | --- |
| `macro/cluster/hop/{local,remote}` | The forward: the same single-key get over an ephemeral table, answered by the accepting shard, another local shard, another node |
| `macro/cluster/nodes/{1,2,3}/rf/{1,3}/cl/{one,quorum}` | The reference cell with the cluster's three axes moved. `rf/3` at `nodes/1` is a one-node cluster claiming three copies and holding one ([C4](tablet-map.md)); it exists so the map's width is measured without replication |
| `macro/cluster/nodes/{1,2,3}/rf/{1,3}/cl/{one,quorum}/ephemeral` | The same over the ephemeral table: the honest transport number |
| `macro/cluster/scale/nodes/{1,2,3}` | The **same total core count** split across nodes — twelve shards on one node, six on two, four on three. Does throughput hold when the cores are spread? This is the "improved performance" question the ask raised, asked in the only form one machine can |
| `macro/cluster/reads/{one,primary,quorum}` | The `r100` cell at each level, RF=3 |
| `macro/fanout/*/nodes/3` | The existing fan-out arms with remote shares |
| `macro/cluster/failover` | The reference mixture at `Quorum`; kills the node with the most primaries at a known instant; **reports the refusal window in seconds and throughput before and after**, never one folded rate |
| `macro/cluster/catchup/{log,snapshot}` | Seconds to lag zero after a fixed backlog, by path |
| `macro/cluster/rebalance/{add,decommission,remove}` | Error count, p99 delta, seconds to even |

Three new `ScaleFacts` fields — `nodes: Option<u32>`, `replication_factor: Option<u32>`,
`consistency: Option<String>` — each `#[serde(default, skip_serializing_if = "Option::is_none")]`
so every committed artifact still parses, mirrored into `ScaleFactsLite`, and `SweepAxis::Nodes`
and `SweepAxis::ReplicationFactor` added to `ALL` so the explorer can draw a value against either.
A `cluster` group in `groups.rs` selects the family, and its module doc's "nothing here makes
anything run concurrently" gains a paragraph: a cluster arm runs `N` server processes *for that
arm*, and still one arm at a time — the rule was about arms sharing a machine, and it still holds.

**Ports.** `port_for` becomes a block: `BASE_PORT + position × PORTS_PER_ARM`, with
`PORTS_PER_ARM = 8` (four nodes × two ports, room for one more). Every existing arm keeps a port
inside its block's first slot, so nothing that reads a historical capture's port changes meaning.
`stage_join.rs`'s reservation above the range moves up with it.

### The real three-node capture

The hardware will differ per node, so the capture records `EnvFacts` **per node** — a
`nodes: Vec<EnvFacts>` beside the coordinator's, each with its cpu model, core count, governor,
kernel and the device under its storage path — and the page that renders it draws **ratios on one
cluster** and never absolutes against the frozen baseline: `rf/3` against `rf/1` on these three
machines, `Quorum` against `One`, `nodes/3` against `nodes/1` on the coordinator's machine alone.
`B1-performance` stays frozen and stays one machine; a cluster capture is not comparable to it and
the freshness table says so with a new verdict, `different machines`, beside `current` and `stale`.

The honest reading of a heterogeneous cluster is that its `Quorum` write latency is its slowest
quorum member's fsync, and the capture names which node that was in every run.

### The acceptance numbers

Stated as hypotheses to falsify, the way [Optimizations](../appendix/optimizations.md) treats every
entry, and replaced by measurements as each milestone captures them:

| Claim | Arm | Number |
| --- | --- | --- |
| Cluster mode on one node costs nothing measurable | `nodes/1/rf/1/cl/one` vs `grid/unsorted/r50/1024` | Inside the reference cell's spread. **M4's exit criterion** |
| The remote hop is a hop | `hop/remote` vs `hop/local` | ≤ 100 µs added at p50 on loopback |
| Replication at `Quorum` on one machine | `nodes/3/rf/3/cl/quorum` vs `nodes/3/rf/1/cl/one`, ephemeral | ≤ 1.5× p50 write latency |
| Replication at `Quorum` on one machine, persistent | same, persistent | ≤ 2× — and the page says how much of the excess is the shared device, by reading the ephemeral arm beside it |
| Spreading cores across nodes | `scale/nodes/3` vs `scale/nodes/1` | ≥ 0.8× the single node's throughput at equal cores, reads at `One` |
| Failover | `cluster/failover` | Window ≤ `primary_failover_after` + 2 s at the defaults |
| Rebalance | `cluster/rebalance/add` | Zero client errors; p99 ≤ 2× during the move |

Every number in the right-hand column is a guess written down so that the first capture has
something to disagree with. The book's rule is that the disagreement, not the guess, is what gets
recorded.

## Alternatives rejected

**`N` pools in one process.** No teardown for glommio's pinning, and one cpu 0 for `N` control
planes.

**Containers or network namespaces per node.** Real network latency would need `tc netem`, root,
and a machine configured for it; the harness would stop being `cargo run`. A namespace per node
is the right way to get a *network* into the emulation and is filed as the arm to add when the
loopback numbers stop being enough.

**Comparing the three-node capture to the baseline.** Different machines; the freshness table
would call every cluster capture stale forever or, worse, current.

**A single throughput number for failover and rebalance.** Averages the event away.

**Per-workload `taskset` for every arm.** Only cluster arms need the driver pinned; pinning the
driver for a single-node arm changes a number the baseline was captured without.

## What it costs

- **A cluster capture is `N` processes' worth of startup per run**, on top of the seed. At three
  nodes and five runs per arm, the cluster family is the slowest in the suite by a wide margin,
  and `list --groups` has to say so; `FULL_MACRO_CAPTURE_SECS` moves again.
- **Three facts on every `ScaleFacts`**, `None` on every existing workload.
- **A port block per arm**, eight times the port range.
- **A generated page**, with its four mandatory blocks.

## What it breaks

- **"One process per workload"** becomes "one *coordinator* process per workload, and its peers".
  The harness doc's four reasons are re-argued for the cluster case when M0 lands.
- **The port formula.** Any external reader of a historical capture's port — there is none in the
  tree — would see a different number for the same arm.
- **The freshness table** gains a verdict.

## Invariants to uphold

- **A cluster arm still runs one arm at a time.** `N` processes for one measurement, never two
  measurements at once.
- **Every emulated node's core slice is disjoint from every other's and from the driver's**, and
  the slices are recorded, not assumed.
- **Failover and rebalance report a duration and a delta, never a rate.**
- **A cluster capture on real hardware records every node's environment** and is compared only
  to itself.
- **The persistent cluster arms are read beside their ephemeral twins**, and a page that quotes
  one without the other is quoting device contention as replication.
- **Identifiers are appended.** Position is port; the block multiplier does not change that rule.

## Prerequisites

[C2](transport.md) for the hop; [C5](replication.md) and [C6](reads.md) for the levels the sweep
moves; [C7](failover.md) and [C8](rebalancing.md) for the event arms.
[F8](../features/purpose-built-workloads.md), [F17](../features/workload-grid.md),
[F20](../features/configuration-sweeps.md), [F21](../features/benchmark-groups.md),
[F29](../features/benchmark-explorer.md) for the harness this extends, and `M0` for the harness
changes that are not a `C` page.

## How it would be measured

This page *is* how. The first capture is `M2`'s `cluster/hop`, the second is `M4`'s
`nodes/rf/cl` sweep, and the acceptance numbers above are the table the milestones page points
at for exit criteria.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `a_cluster_arm_starts_n_processes_on_disjoint_cores` | Three node processes; their `cpus()` sets are pairwise disjoint and exclude the driver's cores; recorded in the run's facts | M0 |
| `a_one_node_cluster_arm_records_the_same_facts_as_today` | `nodes/1/rf/1/cl/one`'s `ScaleFacts` equals the reference cell's plus the three new fields | M0 |
| `the_port_block_keeps_every_existing_arm_in_its_first_slot` | For every id in `IDS`, `port_for(id) % PORTS_PER_ARM == 0` and the sequence is monotonic | M0 |
| `every_cluster_id_has_a_family` | The `family_for` test extended to the new prefix | M0 |
| `nodes_is_a_sweep_axis` | `SweepAxis::ALL` contains `Nodes` and `value()` reads the recorded fact | M0 |
| `a_historical_artifact_still_parses` | Every committed `.macro.json` loads with the three new fields `None` | M0 |
| `failover_reports_a_window_not_a_rate` | The arm's `Measurement` has a `window_secs` counter and no folded `ops_per_sec` | M6 |
| `a_real_cluster_capture_records_every_nodes_env` | `meta.json` has `nodes: [EnvFacts; 3]` and the freshness verdict is `different machines` | M10 |

## Related

- [Benchmarking](../performance/benchmarking.md), [Performance Baseline](../performance/baseline.md) — the harness and the machine
- [F8](../features/purpose-built-workloads.md), [F20](../features/configuration-sweeps.md), [F21](../features/benchmark-groups.md), [F29](../features/benchmark-explorer.md) — the pieces this page extends
- [F9. Ephemeral tables](../features/ephemeral-tables.md) — the controls that make the emulated numbers honest
- [Optimizations — How these are ranked](../appendix/optimizations.md#how-these-are-ranked) — the rule that a claim names its benchmark
- [C2](transport.md#how-it-would-be-measured), [C5](replication.md#how-it-would-be-measured), [C7](failover.md#how-it-would-be-measured), [C8](rebalancing.md#how-it-would-be-measured) — the per-page measurement sections this one collects
