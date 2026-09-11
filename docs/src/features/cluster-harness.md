# F36 — The harness and the facts

## Context

[Distributed Shoal](../distributed/overview.md) is planned in thirteen design pages and eleven
milestones, and until this feature none of it was built. The first milestone,
[M0](../distributed/milestones.md#m0-step-0-the-harness-and-the-facts), deliberately builds
nothing distributed. It builds the two things every later milestone will be judged by: a pure,
deterministic model of the protocol contract agreed at the Before-M0 gate
([P1–P6](../distributed/protocol.md#the-contract)), so that the properties are executable before
any consensus library is chosen; and a process fixture that starts real Shoal servers as children
with real bound endpoints, readiness and failure handles, cleanup, explicit core allocation and
directed fault controls. Alongside them it lays the benchmark groundwork
[C10](../distributed/performance.md) asks for: a cluster environment record, a frozen map of the
historical port assignments, a port allocator for cluster arms, and a load driver that can run
in a process of its own.

Three known issues turned out to share the cause the fixture needed fixed first.
`ShoalPool::start` spawned its shard threads and returned, with no way to learn whether they had
bound, on which port, or whether one had died ([item 58](../appendix/resolved/pool-readiness.md)).
So every test slept for two seconds and hoped ([item 38](../appendix/resolved/pool-readiness.md):
the ports were handed out from a counter each test binary started at 13000), and the benchmark's
readiness probe raced the bind and logged a dozen refused connections per workload at `ERROR`
([item 88](../appendix/resolved/pool-readiness.md)). One handle closed all three, and is the
first thing this page describes.

## What it does

**A readiness and failure handle on the pool.** `ShoalPool::start` still returns before the
shards have bound, which is glommio's shape; what changed is that every shard now reports to the
pool on a channel — `Ready` with the address it actually bound, once its listener, its mesh join
and its loaders are up, or `Failed` with what went wrong, before or after. `ShoalPool::ready`
waits for every shard or returns the first failure by name; `ShoalPool::failure` asks afterwards,
without blocking, whether a shard has died since; `ShoalPool::bound_addr` says where the shards
are; and `exit` returns the first shard error instead of logging it to a subscriber nobody
installed. A configuration that asks for port `0` gets a real port: the pool reserves one with
`SO_REUSEPORT` before the shards start, hands them the number, and holds the reservation — bound,
never listening, so the kernel never routes a connection to it — until every shard has bound the
same port. Every integration test helper now starts on port zero and waits on `ready` rather than
on a sleep, and the benchmark harness waits on `ready` before its query probe.

**`shoal-model`, a new pure crate.** One replicated tablet group, Raft-shaped, written against
the contract's five positions — appended, durable, committed, applied and checkpointed are five
different things, and an entry becomes durable only through an explicit storage-completion event.
Its `Policy` has a safe setting, which is the contract, and six unsafe settings, each one of the
violations the contract table says the model must reject: election by heartbeat-max report (P5),
a quorum counted over the observer's Up list (P3), a duplicated acknowledgement counted twice (P3),
an `Async` receipt counted as durable (P3), reads and checkpoints that see the appended suffix (P4),
and an acknowledgement sent on receipt rather than after fsync (P1). A checker computes what is
committed from durable facts alone — which entries are in which stable logs, and what term each
node held when each became durable, which is Raft's figure 8 made explicit — and judges every
transition against the `P` number it enforces. A ledger records every client attempt with its
identity and outcome, and an oracle checks each key's history against the sequential machine with
successful, rejected and unknown outcomes held to their three contracts. Schedules are explicit
event lists: generated from a seed by a weighted random walk, written by hand with a builder, saved
as JSON, replayed, and minimized by delta debugging. Seven are saved under
`shoal-model/schedules/`, one per unsafe knob and the B=100/C=101/A+B=102 schedule from
[C7](../distributed/failover.md#when-a-primary-is-down) at a short prefix; the literal-number one
is built by the test from the same builder, because at 100 writes it is half a megabyte of JSON.

**The cluster fixture, `shoal/tests/cluster/`.** `Cluster::builder()` takes servers and mock
peers with a core claim each, starts every one as a re-execution of the test binary — the shape
`ack_survives_sigkill` already used — on port zero, waits for each to print the endpoints it bound
or the failure it hit, and builds a directed TCP proxy per ordered pair of nodes when asked to. A
node can be killed (`SIGKILL`), paused and resumed (`SIGSTOP`/`SIGCONT`), and asked whether it has
reported a shard's death. A link can be cut, delayed and healed; a cut ends every live stream and
closes every new one, so a reconnect is subject to the fault it reconnects into. Cores are handed
out as whole physical cores, both SMT threads of each, never the one cpu 0 is on, recorded per node
and for the driver, with sharing recorded where the machine cannot isolate and an exact claim that
overlaps refused. Dropping the cluster kills and reaps every child, on a panic too.

**The benchmark groundwork.** A `ClusterFacts` record — nodes, desired and active replication
factor, the two consistency policies, durability, driver placement, per-node cores, table and
tablet counts, offered load, whether the nodes shared a machine — is carried on a workload capture
as an `Option` that is absent for every single-node capture, mirrored field for field in
`shoal-top`, and named by `compare` when two captures differ in it rather than compared across.
`docs/perf/ports.json` freezes the port every declared workload has always bound, and a test fails
if any of them moves. `cluster_ports` allocates a block per workload above the single-node range
for the arms later milestones add, refusing to wrap or to overlap. `shoal-workload run --server
<addr>` drives a server somebody else started, and `shoal-workload serve` is that somebody: it
starts a workload's configuration and prints the address its shards bound.

## Design choices

**The pool reserves the port rather than the fixture guessing one.** C11 names the race in the
obvious design — bind zero, read the port, close, pass it on — and asks for the children to bind
zero themselves and report. Every shard binds the same port with `SO_REUSEPORT`, so zero handed to
each of them would give each its own ephemeral port. The pool holds a bound, non-listening
reuse-port socket while the shards bind: the kernel only routes a connection to a *listening*
member of a reuse-port group, so the reservation keeps the port from everyone else and steals no
connection from the shards. It is dropped once `ready` has counted every shard.

**The checker trusts no node's commit index.** The unsafe policies corrupt exactly that, so a
checker that read it would be checking the bug against itself. It recomputes commitment from
stable logs and the term each node held at fsync time, requiring a majority of the configured
voters to hold an entry durably *at its own term*, and then extends the committed prefix through
everything before it. That is the durable-storage form of Raft's "only count replicas for
current-term entries", and the reason the safe policy never trips it is that a delivered durable
acknowledgement carries the follower's term and the leader discards acks from any other.

**Every subsequence of a valid schedule is a valid schedule.** `World::apply` skips an event that
does not fit — a delivery of a message never sent, a restart of a node that is up — rather than
refusing it. A removed delivery is a dropped message, a removed storage completion is a stalled
disk, a removed crash is a node that stayed up. That is what lets the minimizer try leaving any
event out, and what lets a saved file replay without the generator's seed.

**A separate record, not more fields on `ScaleFacts`.** C10 asks that historical single-node
records remain meaningful. A record full of defaults would be a claim about a cluster nobody ran;
an absent one is the truth, and `compare` treats none-against-some as the largest difference
there is. The same test that guards the scale facts' key set guards that no committed capture
grew a `cluster` key.

**Ports are appended, never re-based.** `port_for` is untouched and the cluster block starts at
20,000, eight thousand ports above the single-node base, because moving the single-node range to
make room would change every historical assignment. The frozen map makes the append rule a test
rather than a comment.

**The literal stale-report schedule is built, not saved.** The seven saved files are small and
reviewable; the 100/101/102 one is 1,341 events because every write is replicated and fsynced in
full. The builder is deterministic, the test asserts the detail names 101 and 102, and a unit test
runs the same builder at prefixes of one, two and four.

## Alternatives rejected

**A fixed sleep after `start`, kept.** It was two seconds in every helper and it was the whole of
item 38's safety net: the binaries were simply never alive on one port at the same moment. A
readiness signal is what the pool's owner needs, and the probe the benchmark keeps is for a
different question — whether an encrypted arm's handshake works — not for whether a socket exists.

**Bind zero, close, pass the port to the child.** The race C11 warns about, and on a machine
running twenty test binaries at once not a theoretical one.

**Modelling the selected consensus library.** None is selected; that is C13's Q1 and M1's spike.
The model is the contract the adapter around whichever library is chosen will be held to, and it
is smaller for not being a library.

**A linearizability checker over the whole tablet.** The oracle works one key at a time, because
every operation reads or writes one key and its result depends on that key alone; a tablet's
history is consistent exactly when each key's is, and the search is bounded per key rather than
exponential in the schedule. Cross-tablet checks are P6's explicit non-promise.

**`exit` still swallowing shard errors.** It was the smallest honest half of item 58's fix
direction. It surfaced one shard error a test had been swallowing for as long as the test existed,
which is [item 91](../appendix/known-issues.md#91-a-compaction-that-fails-ends-the-compactor).

**Frame-level faults in the fixture's proxy.** A byte proxy cannot see inside TLS, and the fault
table wants frame-class manipulation before encryption. That is a fake transport for a later
milestone; the proxy models what it can, which is a directed link.

## Limitations

- **There is no peer transport, membership or node identity.** The children are isolated servers
  and a mock peer that echoes; a directed link carries client traffic and echoes, nothing a node
  says to a node. `Endpoints::data` and `Endpoints::control` are `None`, and stay so until M2 and
  M3. A test of a distributed invariant cannot be written against this fixture yet, and C11 says
  an empty mock test must not count as one.
- **The model is Raft-shaped, not a library.** Q1 is open. It has no configuration changes,
  learner promotion, snapshot transfer or compaction, and one voter configuration per run.
- **The oracle's reads are `One` reads.** A read is satisfied by any committed prefix, which is
  the contract's default and is deliberately weak: a read of an uncommitted suffix is caught by
  the P4 invariant, not by the oracle.
- **No `macro/cluster/*` workload exists**, and no capture carries a cluster record. `--server`
  refuses a workload that restarts its server between phases, and records the configuration
  file's facts as the caller's claim rather than verifying the server was started from it.
- **A kill is `SIGKILL` and proves nothing about durability.** The page cache and the device are
  untouched; durability tests need injected storage completions, which are M4's.
- **Core allocation is by physical core from sysfs.** Nothing pins the driver or the children's
  client threads; the allocation is what the server is configured to, and is recorded, not
  enforced against a cpuset.
- **The stage-profile test `stage_join.rs` still uses a fixed port**, above the capture range, as
  its own comment explains.

## Invariants to uphold

- **`ShoalPool::ready` reports `Ready` only after `Shard::init` has returned.** Recovery replays
  in `Shard::new`, the listener binds in `init`, and the loaders start there too; a shard that has
  reported ready is a shard that answers. Move the report earlier and every helper's first query
  races the bind again.
- **The port reservation never listens.** A listening member of the reuse-port group is offered
  connections; a bound one is not. If it ever calls `listen`, connections vanish into it.
- **A shard reports its own death with the id the pool minted for it.** The id is minted in the
  spawn closure, before `Shard::new`, so a failure in construction can still be named.
- **The checker computes commitment from stable storage and fsync terms only.** Reading a node's
  `commit_index` anywhere in `invariants.rs` makes the P3 and P4 checks circular.
- **`World::apply` is total.** An event that does not fit is skipped and counted, never refused;
  the minimizer's correctness and every saved file's replayability rest on it.
- **`Violation::detail` never contains the step.** `same_failure` compares property and detail, so
  a minimized schedule that finds the same violation earlier is the same failure.
- **A saved schedule replays to exactly its `expected`**, and is byte-identical to its own
  canonical form. Regenerate with `cargo run -p shoal-model --example regenerate_schedules` after
  any change to the model; the test says which file drifted.
- **`Cluster`'s `Drop` kills and reaps.** A child that outlives its test holds a port and a core;
  the port-race test asserts every pid is gone after a panic.
- **A single-node capture carries no `cluster` key.** The record is `Option` and skipped; a
  default record would be a claim about a cluster nobody ran.
- **Workload ids are appended.** `docs/perf/ports.json` is the frozen map; a new id goes after
  every id in it, and `cluster_ports` starts above every single-node port.
- **`shoal-model` links no shoal crate, no runtime and no engine.** `cargo tree -p shoal-model`
  mentions neither glommio nor tokio; that is what lets a schedule replay anywhere and the crate
  test while the engine does not build.

## Performance

**Nothing here is a capture.** No workload and no `shoal.yml` changed, and nothing measured
changed. Two files the macro layer's source fingerprint covers did — `shoal-core/src/server.rs`,
for the readiness handle, and `shoal-bench/src/workloads/harness.rs`, for waiting on it — so
`shoal-bench status` reports every macro, hotpath and stages capture as stale against this commit.
That is the fingerprint doing its job, not a claim that a number moved: the next capture taken for
a change that does claim one will re-establish freshness, and taking one for this change alone
would be two hours spent measuring that a server which waited on `ready` before its probe answers
the same queries at the same speed.

**The matched single-node arm**, M0's exit criterion that the harness did not change what the old
comparison meant: `macro/get_ephemeral` at `--scale smoke` was run twice on 2026-09-11 against a
scratch copy of `shoal.yml` with local storage and no remote sink — once in process, as every
capture runs, and once through `shoal-workload serve` and `run --server`. The two captures'
`scale` and `conf` facts were byte-identical, their key sets were identical, and neither carried
a `cluster` key. The numbers are not a measurement — smoke scale proves a workload runs — but for
the record the in-process run's wall clock was 6.2 ms against the external run's 5.2 ms, with 500
rows retrieved by each.

**The benchmark's readiness probe no longer logs refusals.** The in-process run above wrote one
`ERROR` line to stderr where item 88 had reproduced twelve, and that one is a different event: a
connection the client still held when the pool exited, noted under the resolved page's *Still
open*.

**Readiness costs nothing measurable.** One `std::sync::mpsc` send per shard at startup and a
`try_recv` when asked; the reservation is one socket for the duration of startup.

## Tests

| Test | What breaks if the feature is reverted |
| --- | --- |
| `protocol_model::protocol_model_preserves_acknowledged_history` | The contract as an executable thing: thirty-two seeded schedules under the safe policy with elections, truncations, crashes, pauses, duplicates, retries and unknown outcomes all shown to have happened, and a saved schedule per unsafe knob replaying to the `P`-numbered violation the contract table names, the stale-report schedule among them |
| `protocol_model::saved_protocol_schedule_reproduces_failure` | Reproducibility: a fresh failure minimized to a one-minimal subsequence that fails identically, the JSON round trip, and every saved file replaying to what it records in canonical form |
| `protocol_model::history_oracle_distinguishes_unknown_and_rejected` | The three outcome contracts: seven hand-written histories, each exercising one rule of the ledger |
| `protocol_model::the_stale_report_schedule_loses_the_write_at_any_size` | The builder producing the C7 schedule at any prefix, so the literal one the test builds is trusted |
| `invariants::tests::an_old_term_entry_on_a_majority_is_not_committed_until_a_current_one_is` | The checker's ground truth: Raft's figure 8, index 2 not committed on a majority at term 1 until a term-3 entry is durable after it |
| `raft::tests::*` (five) | Election, replication and commit; a conflicting suffix truncated; a heartbeat re-ack being a watermark; a retry returning the stored result; a crash keeping only stable storage |
| `observer::tests::*`, `policy::tests::*`, `schedule::tests::*`, `minimize::tests::*`, `rng::tests::*`, `storage::tests::*`, `oracle::tests::*` | The safe observer never promoting; each knob deviating in its own name only; same seed, same events; a padded failure shrinking; the reference vectors; the sequential semantics; per-key locality and the bound |
| `cluster_fixture::fixture_reports_bound_endpoints_without_port_race` | Two clusters started at once on port zero owning four distinct answering endpoints, and every child gone after its cluster is dropped by a panic |
| `cluster_fixture::fixture_faults_cover_directed_links_and_reconnects` | A cut ending the live stream and the reconnect after it while the other direction passes; a pause stalling a query that completes on resume, which a cut would fail |
| `cluster_fixture::cluster_fixture_accounts_for_all_cores_and_endpoints` | The allocator on a synthetic machine: disjoint counted claims, a refused overlapping exact claim, sharing recorded on a small machine; and a real cluster's plan carrying the endpoints its children bound |
| `pool::a_shard_that_cannot_bind_is_reported` | Item 58: a held port reported by `ready` as `ShardFailed` naming the shard and the error, where `start` used to return `Ok` and a client reached the holder |
| `pool::a_port_of_zero_resolves_to_one_every_shard_binds` | The reservation: `bound_addr` known before any shard binds, `ready` reporting the same address, a client served on it |
| `committed_artifacts::historical_artifacts_and_ports_remain_compatible` | Every committed capture parsing with no cluster record, every frozen port unchanged, and every new id appended after the frozen range |
| `committed_artifacts::a_single_node_capture_serializes_no_cluster_record` | The `Option` staying skipped, which keeps the committed corpus byte-identical |
| `explore_index::the_facts_mirrors_are_total` | Every field of `ScaleFacts`, `ConfFacts` and `ClusterFacts` reaching the explorer's copy — including the two trace fields F34 added and never mirrored |
| `compare::macro_layer::tests::a_clustered_capture_does_not_compare_to_a_single_node_one` | `compare` naming a cluster difference rather than comparing across it |
| `plan::tests::cluster_ports_are_disjoint_from_the_single_node_range_and_bounded` | The allocator's block above the single-node range, distinct across arms and nodes, refused rather than wrapped |
| `acceptance_tables::acceptance_tables_have_unique_tests_and_valid_milestones` | The chapter's tables: unique names, valid milestones, each milestone naming the chapters it gates, and every test of a delivered milestone existing as a function |

## Related

- [M0](../distributed/milestones.md#m0-step-0-the-harness-and-the-facts), the gate this delivers
- [C11](../distributed/testing.md), [C10](../distributed/performance.md) and
  [C13](../distributed/protocol.md), whose acceptance rows the tests above are
- [Resolved #38, #58, #88](../appendix/resolved/pool-readiness.md), the pool handle
- [Item 91](../appendix/known-issues.md#91-a-compaction-that-fails-ends-the-compactor), what
  the honest `exit` surfaced
- [F8](purpose-built-workloads.md), whose readiness probe this keeps for the question it answers
- [F29](benchmark-explorer.md), whose index now mirrors the cluster record
- [Benchmarking](../performance/benchmarking.md), for `run --server` and `serve`
