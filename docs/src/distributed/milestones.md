# Milestones

This is the first page in the book to use the word. [Direction](../direction/overview.md) calls
itself "a design record, not a roadmap" and [Introduction](../introduction.md#what-shoal-is-not)
repeats it, and both are right about what they describe: nine independent designs ranked by
evidence. This part is different in one way — its twelve pages are one feature with a dependency
graph, and the graph has to be walked in some order. A milestone here is **an order with reasons,
not a date**. Where a step could have gone elsewhere, the reason it did not is written down, and
that reason is the part worth reading.

Eleven steps in four groups. Each says what it delivers, which `C` page it turns into an `F` page
— the page stays as the design record and the `F` page describes what shipped, the rule from
`direction/overview.md:52-59` — which acceptance tests it builds, which benchmark arm it captures
and what number exits it, and what it deliberately leaves out. The tests are the ones on each
`C` page, indexed on [C11](testing.md#the-acceptance-test-table); the arms and numbers are
[C10](performance.md#the-acceptance-numbers)'s.

## Group 0 — Foundations

Nothing in this group changes what a single node does. Everything in it is what the later groups
need to be judged.

### M0. Step 0: the harness and the facts

**Goal.** Be able to start three nodes, kill one, and measure a cluster arm, before there is a
cluster to do it to.

**Delivers.** The `Cluster` fixture in `shoal/tests/cluster/mod.rs` ([C11](testing.md#the-cluster-fixture)):
child processes, bind-zero ports handed down, readiness by line, `kill`/`pause`/`resume`/
`partition`, the ledger, digests. `get_unique_port` and its counter deleted from
`shoal/tests/utils.rs`, closing [item 38](../appendix/known-issues.md#38-integration-test-binaries-all-bind-the-same-ports).
`ShoalPool::start` returns a handle with `ready()` and `shard_failed()`
([C9](operations.md#readiness)), closing [item 58](../appendix/known-issues.md#58-a-shard-that-dies-is-not-reported-to-whoever-started-the-pool)
and the readiness entry in `todos.md`; every sleep in `utils.rs` and `harness.rs` deleted. In
`shoal-bench`: `ScaleFacts` gains `nodes`, `replication_factor`, `consistency`; `ScaleFactsLite`
and `SweepAxis` mirror them; `port_for` becomes a block; a `cluster` family, surface and group;
`shoal-workload serve`; `harness::run` starts `N` processes with disjoint core slices and pins the
driver ([C10](performance.md#emulating-a-cluster-on-one-machine)).

**Pages it makes true.** None — C11 and the harness half of C10 become an `F` page together,
because a harness is a feature.

**Acceptance tests.** C11's seven fixture tests; C9's two readiness tests; C10's six harness
tests.

**Benchmark.** A one-node "cluster" arm, `nodes/1/rf/1/cl/one`, with no `cluster:` block yet —
which is today's reference cell run through the new harness. **Exit: it captures inside the
reference cell's spread**, proving the harness itself moved nothing.

**Leaves out.** Deterministic simulation, with the reason on C11; network namespaces for
partitions, filed as the arm to add when the relay is not enough.

### M1. Node identity and the control-plane thread

**Goal.** A node has a name, a cluster has a name, and the reserved core does something.

**Delivers.** `NodeId`, `ClusterId`, `StorageMeta` format 2, the `cluster:` block with
`deny_unknown_fields`, the control-plane thread on cpu 0 running a **single-node** `openraft`
group with a state machine holding one member and today's derived map, and `ServerMsg::Topology`
broadcast on the mesh ([C1](node-identity.md)). The `Topology` frame pushed to clients and
`Shoal::topology()` recording it — [D7](../direction/shard-aware-routing.md)'s step 1, built
here. `shoalctl` showing the map in its new cluster tab, read-only.
[Item 65](../appendix/known-issues.md#65-two-gxhash-majors-and-partition-keys-hashed-by-the-one-without-deterministic)
fixed first, as a gate, because two nodes built from different lockfiles would otherwise disagree
about which tablet a key is in.

**Pages it makes true.** C1 → F page. C4's push half.

**Acceptance tests.** C1's seven M1 rows; C4's `a_client_receives_a_topology_frame_on_connect`.

**Benchmark.** `nodes/1/rf/1/cl/one` again, now with the block present and the thread running.
**Exit: inside the spread.** If the thread on cpu 0 or the wider map shows up here, C1 named a
cost it does not have.

**Leaves out.** Any peer. The `openraft` dependency lands here, on a group of one, so that M3's
work is the network and not the integration; and the two source questions on
[C3](membership.md#openraft-and-the-runtime) are answered in this milestone's first commit, before
the crate is added, because the answers might change the plan.

### M2. The inter-node transport

**Goal.** A query whose partitions are on another node is answered, and the trace says so.

**Delivers.** `ShardContact::Remote`, the second arm in `Comms::send`, per-shard outbound peer
connections, the peer listener on `cluster.port` with accept-anywhere-and-relay, the peer
handshake with mTLS, `Forward` and `Forwarded`, revalidation on arrival, bounded peer channels
with `Shedding`, and the trace context on every peer frame ([C2](transport.md)). Membership is
**static** — a `cluster.static_split` test-only setting assigns tablets to two nodes from config,
so the transport is exercised before Raft exists.

**Pages it makes true.** C2 → F page.

**Acceptance tests.** C2's ten rows; C9's `a_cross_node_trace_has_the_forward_span`.

**Benchmark.** `macro/cluster/hop/{local,remote}` — the first cluster capture, over the ephemeral
controls. **Exit: `remote` adds ≤ 100 µs at p50 over `local` on loopback**, and `local` (another
shard, same node) is the number D7 said nobody had measured, captured for the first time.

**Leaves out.** Per-shard peer ports (D7's option 1) — measured against by `hop`, not built; the
`Replicate` family of frames, whose payloads are C5's.

## Group A — Distribute a live system with no failures

Three nodes, nothing dies. The cluster has to work before it has to survive.

### M3. Membership

**Goal.** Three processes become a cluster from one seed, agree on a map, and notice when one of
them stops answering — and do nothing about it.

**Delivers.** The Raft group of nodes with join, learner promotion, the member states, the
phi-accrual detector on the control-plane connection, `Unreachable` local and `Down` by majority
([C3](membership.md)). The map as Raft state with bootstrap assignment and `ReplicaSet` as the
value, `Ring` deleted, `find_replicas` at every routing site ([C4](tablet-map.md)) — at RF=1, so a
replica set has one entry. `Admin::Members` and `Admin::Topology`. A joiner does not serve until
its first `Topology`.

**Pages it makes true.** C3 → F page; C4 → F page (the map half; the push half was M1).

**Acceptance tests.** C3's nine M3 rows; C1's `a_joining_node_does_not_serve_before_its_first_topology`;
C4's four M3 rows; C9's two M3 rows.

**Benchmark.** `nodes/3/rf/1/cl/one`: three nodes, one copy, reads at `One`. **Exit: a local
partition's read is inside the single-node spread, a remote one is `hop/remote` away**, and the
detector's pings do not appear — if they do, they are on the wrong connection.

**Leaves out.** Failover, rebalancing, RF > 1. `SetPrimary` and `MoveTablet` exist as log entries
and nothing proposes them. A per-table `replication_factor` is accepted and refused by name.

### M4. Replication and quorum writes

**Goal.** Every tablet has three copies, a write is acknowledged at a quorum, and every replica
converges. **R5**, and the default half of **R4**.

**Delivers.** Intent record format 2 with the `(tablet, epoch, seq)` header, the replication
sender beside `commit`, `Replicate`/`ReplicateAck`, the quorum gate in `PendingResponse`,
`Unavailable` before commit, followers applying in order with `CatchUp` for gaps, rotation that
counts an ack rather than draining ([C5](replication.md)). RF=3 bootstrap assignment with distinct
nodes and even primaries ([C4](tablet-map.md)). Reads at `One` from the nearest `Up` replica, and
`limit` across replicas ([C6](reads.md)). `Admin::Lag`, `shoal.replication.lag` and
`shoal.replication.quorum_wait` ([C9](operations.md)).

**Pages it makes true.** C5 → F page. C6's `One` half.

**Acceptance tests.** C5's thirteen rows; C4's four M4 rows; C6's three M4 rows; C9's two M4 rows.

**Benchmark.** The `nodes/{1,2,3}/rf/{1,3}/cl/{one,quorum}` sweep, persistent and ephemeral.
**Exit, in order of importance:** `nodes/1/rf/1/cl/one` inside the reference cell's spread — the
"no drastic slowdown" criterion, and the milestone does not close until it holds; `rf/3/cl/quorum`
≤ 1.5× `rf/1/cl/one` on the ephemeral arm; the persistent excess over that attributed to the shared
device by reading the two beside each other.

**Leaves out.** `Primary` and `Quorum` reads; the per-bundle override; anything that happens when
a node dies. A `Down` follower is not waited for, which is testable without failover by killing a
follower and asserting the ack still comes, and that test is here.

### M5. Read consistency levels

**Goal.** A caller can read their own write, and the wire can say how strongly to read.

**Delivers.** `Primary` reads under the lease, `Quorum` reads with per-partition stamp reconcile
and `CatchUp` read repair, gather deadlines with `Error(Timeout)`, and `Queries.consistency` with
`PROTOCOL_VERSION` 4 — the flag day, taken once ([C6](reads.md)). The builder's `.consistency(..)`.

**Pages it makes true.** C6 → F page.

**Acceptance tests.** C6's seven remaining rows.

**Benchmark.** `macro/cluster/reads/{one,primary,quorum}` and `fanout/*/nodes/3`. **Exit:
`primary` ≤ `hop/remote` over `one`; `quorum` ≤ 2× `one` at p50 on the read-only arm.**

**Leaves out.** A linearizable level beyond `Primary`'s lease; the idempotency key, which is a
write concern and is C7's problem to name.

## Group B — Survive failures

A node dies, comes back, and is found to have been wrong. The ledger test lives here.

### M6. Primary failover

**Goal.** A tablet whose primary died gets a new one within the window, and no acknowledged
`Quorum` write is lost. **The single most valuable test in the part.**

**Delivers.** Per-tablet stamps in heartbeat replies, the leader's `primary_failover_after` timer,
`SetPrimary` to the highest `Up` replica, follower truncation on a new epoch, fencing by epoch on
every `Replicate` and `Primary` read, `Unavailable` during the window ([C7](failover.md)).
`shoal.failover.count` and `shoal.failover.window`.

**Pages it makes true.** C7's first half → F page.

**Acceptance tests.** C7's seven M6 rows, the ledger test among them, twenty times under `soak`;
C3's `down_moves_no_tablet`; C6's `a_primary_read_is_refused_when_the_lease_has_lapsed`; C10's
`failover_reports_a_window_not_a_rate`.

**Benchmark.** `macro/cluster/failover`. **Exit: the refusal window ≤ `primary_failover_after` +
2 s at the defaults, reported as a duration**, and throughput after equals throughput before
within the spread.

**Leaves out.** A returning node — the killed node stays dead in this milestone's tests. The
idempotency key, filed on `todos.md` with C7's argument for why the window is visible until it
exists.

### M7. Recover a node brought back online

**Goal.** A node that was down comes back, catches up, serves, and has moved nothing. **R3**,
proved.

**Delivers.** `CatchUp` by log; the snapshot stream — `StreamBegin`, `StreamPartition`,
`StreamEnd` — from the primary's partitions and archive map at a `seq`, installed through the
receiver's archive writer; the primary choosing log or snapshot by what it has compacted; a
returning primary becoming a follower; per-tablet routing around a replica still catching up
([C7](failover.md#a-returning-node)). `Admin::Lag` reporting catch-up progress.

**Pages it makes true.** C7 → F page, complete.

**Acceptance tests.** C7's six M7 rows, `down_for_less_than_auto_remove_after_moves_nothing`
among them.

**Benchmark.** `macro/cluster/catchup/{log,snapshot}`. **Exit: seconds to lag zero for a fixed
backlog, recorded**; there is no prior number to beat, so the exit is that both paths complete
and the snapshot path is the one taken when the log cannot be.

**Leaves out.** Primaries moving back — the rebalancer's job, off by default, M9. Repair — a
returning node is caught up by stamp, and corruption at equal stamps is M8.

### M8. Repair

**Goal.** Two replicas at the same stamp that disagree are found, and the wrong one is fixed
from the right one.

**Delivers.** Per-tablet digests, `Admin::Repair` on one tablet or all, snapshot-from-the-primary
on a mismatch, `cluster.repair_interval` for a scheduled run ([C9](operations.md#repair)).
`Cluster::digest` in the fixture becomes the operator's digest rather than a test-only one.

**Pages it makes true.** C9's repair section; the digest half of C11.

**Acceptance tests.** `repair_restores_a_deleted_archive`; a digest test per table kind.

**Benchmark.** None captured. A repair reads every archive of the tablet, which is a known cost
with no number to beat; the arm is filed for when a scheduled repair is on by default, which it
is not.

**Leaves out.** Merkle trees. A digest per tablet is 4096 hashes per replica, and a mismatch
streams the tablet; Cassandra's per-range Merkle trees exist to make the *diff* cheap on ranges
far larger than a Shoal tablet. Filed with the reason.

## Group C — Elastic membership

Nodes are added, removed, and replaced. **R1** and **R2**.

### M9. Migration and the rebalancer

**Goal.** Adding a node evens the cluster with no client error; decommissioning one drains it;
removing a dead one re-replicates from survivors; and a node down past `auto_remove_after` is
removed.

**Delivers.** The rebalancer on the leader's control plane with its three targets and one move
per pair; `AddReplica`, `DropReplica` and `MoveTablet`; the per-tablet index in the archive map;
`drop_grace`; orphan reporting and reuse; `Decommission`, `Remove`, `Leaving`/`Removing`/`Removed`;
`auto_remove_after`; `rebalance_primaries`; `ShardCountMismatch` retired and `cores` changeable
([C8](rebalancing.md)). The `Topology` frame pushed to clients on change, and the server-side
stale-route forward ([C4](tablet-map.md#staleness-on-servers-too)).

**Pages it makes true.** C8 → F page. C4's staleness half. [TODOs — Rebalancing](../appendix/todos.md#rebalancing)
is closed, with the note that its second half was answered differently.

**Acceptance tests.** C8's thirteen rows — R1, R2 and R3 each named in one; C4's two M9 rows;
C9's `remove_is_refused_for_an_up_node`.

**Benchmark.** `macro/cluster/rebalance/{add,decommission,remove}`. **Exit: zero client errors on
`add` and `decommission`; p99 ≤ 2× during the move; seconds to even, recorded.**

**Leaves out.** Rack and zone awareness in the plan — the map has room for it (a `Member` gains a
`zone` and the plan's first target gains "distinct zones"), and it is filed rather than built
because the three test machines are in one room. Tablet splitting, which the high-bit derivation
was kept for and which needs a per-table map first. `max_concurrent_moves_per_pair`.

### M10. Operations and the real cluster

**Goal.** An operator can run it, and the numbers exist on hardware that is not one machine.

**Delivers.** `cluster.admins` gating state-changing admin requests on a `Principal`; `shoalctl`'s
cluster tab with actions and confirmations; the remaining metrics; the peer handshake accepting
`n − 1`; the six runbooks written in full ([C9](operations.md)). The three-node capture on real
hardware with per-node `EnvFacts` and the `different machines` verdict ([C10](performance.md#the-real-three-node-capture)).
The docs sweep: [Introduction](../introduction.md#what-shoal-is-not)'s five bullets struck
through with what replaced each; [Partitioning](../architecture/partitioning.md#limitations)'s
four; the glossary's `Distributed` row; `CLAUDE.md`'s "distributed database" finally true;
[Storage Overview](../storage/overview.md#durability-model)'s durability model gaining its quorum
qualifier; every `C` page annotated with its `F` page and a *Still open* list.

**Pages it makes true.** C9 → F page; C10 → F page; C12 gets a "what was copied" annotation per
system.

**Acceptance tests.** C9's three M10 rows; C10's `a_real_cluster_capture_records_every_nodes_env`.

**Benchmark.** The full `cluster` group on the three real nodes. **Exit: `render --check` clean
with the generated cluster page, and the ratios recorded** — `rf/3` vs `rf/1`, `quorum` vs `one`,
on that cluster, compared to nothing else.

**Leaves out.** A standalone `shoalctl` binary for the cluster tab, filed with the packaging
entry. Rolling *protocol* upgrades beyond `n − 1`.

## The order is a claim

Three orderings here could have gone the other way, and each is a claim rather than a
convenience.

**M0 is first because nothing below it can be judged.** Direction made the same argument for
instrumenting the client (`direction/overview.md:134-155`): every number in the book included the
client and none could attribute anything to it, so the first step was the one that made the
others measurable. A cluster that cannot be started three at a time and killed one at a time
cannot be shown to do anything on C3 through C9, and a harness that changed the reference cell's
number would poison every comparison after it. M0's exit criterion — the same cell, inside the
spread, through the new harness — is what makes M4's exit criterion mean something.

**M4 is before M5 because eventual consistency is the default and the strong reads are the
option.** A cluster with `Quorum` writes and `One` reads is a complete, useful, eventually
consistent store — R4 and R5 both met — and a cluster with `Quorum` reads and no replication is
nothing. The flag day M5 carries is also better paid once the write path has settled what a stamp
is.

**M6 is before M9 because a cluster that cannot survive a dead node must not be allowed to
grow.** Rebalancing adds nodes; every node added is another node that will die; and a rebalance
under a failover is the interaction most likely to lose data. Failover is built, tested with the
ledger twenty times, and captured, before the first tablet moves.

One ordering that is *not* a claim: **M8 could go anywhere after M4.** It sits between M7 and M9
because the digest it builds is what M9's tests assert convergence with, and because a repair tool
is the thing an operator wants immediately after the first failover teaches them to worry.

## Related

- [Overview](overview.md) — the requirements each milestone is checked against, and the
  dependency graph this order walks
- [C11. Acceptance tests](testing.md#the-acceptance-test-table) — every test, by milestone
- [C10. Performance](performance.md#the-acceptance-numbers) — every exit number, as a hypothesis
- [Direction — The recommended order](../direction/overview.md#the-recommended-order) — the
  step-0 argument this page reuses
- [Delivered Features](../features/delivered-features.md) — where each milestone's `F` page will
  be listed
