# F37 — Node identity and the control-plane thread

## Context

[M0](../distributed/milestones.md#m0-step-0-the-harness-and-the-facts) built the things every
later milestone is judged by and nothing distributed. [M1](../distributed/milestones.md#m1-node-identity-and-the-control-plane-thread)
is the first milestone that changes the server, and it changes it in the one way that has to
come before any node can talk to another: a node has to *be* somebody. Until this feature a
Shoal server was its address. Its storage directory recorded a marker format and a shard count
([item 45](../appendix/resolved/storage-marker-format.md)) and nothing else, so two servers on
two copies of one directory were indistinguishable, a directory moved to another machine was a
different node, and the question "is this the node that wrote these files" had no answer.

[C1](../distributed/node-identity.md) is the design: a random node identity minted once per
directory, a cluster identity minted once per explicit bootstrap, a marker that carries both, a
`cluster:` block that separates what is one node's from what is the cluster's, and a control
thread on a core of its own running an embedded consensus group. This feature builds that up to
the point where a second node would be needed - the group has one member, the peer endpoints are
advertised and bound by nothing, and the replication policy is recorded and reported rather than
enforced - and it owes three other things the milestone page names: the Q1 spike that chooses the
consensus library and runtime with evidence, the first Q13 scale numbers, and the fix for
[item 65](../appendix/resolved/gxhash-pin.md), because two nodes built from different lockfiles
hashing one key to two tablets is a defect that has to be closed before a tablet has two homes.

## What it does

**One gxhash, a golden key.** The workspace pins `gxhash = "2.3"` once, `shoal-proto` and
`shoal-core` take the workspace entry, and the dead `gxhash 3` pin with its `deterministic`
feature - which reached nothing - is gone. `shoal/tests/partition_keys.rs` freezes eight keys, a
`u64` shape and a `String` shape, to the hash and the tablet 2.3.1 gives them; the literals were
obtained by running the test on the unfixed tree and are on the
[resolved page](../appendix/resolved/gxhash-pin.md). A build that hashes any of them differently
cannot read an existing directory, and now says so.

**`NodeId` and `ClusterId`** in `shoal-proto/src/shared/identity.rs`: uuid newtypes, serde
transparent, `Display` as the whole uuid because a prefix is not a key. In the protocol crate
rather than the engine, since the topology view M3 exposes and the handshake M2 adds both put
them on the wire, and neither needs a runtime.

**The marker at format 2.** `StorageMeta` is `{format: 2, shards, node, cluster, layout,
topology}`. `claim(root, shards, ClusterIntent)` mints a node id for an empty directory, and a
cluster id too if the intent is `Bootstrap`; on an established directory it holds the shard count
and layout as before and adds the mode: a directory bootstrapped into a cluster is refused by a
standalone config (`ClusterDirectoryInStandalone`, naming the cluster) and a standalone
directory by a cluster config (`StandaloneDirectoryInCluster`, naming the node and M10's
migration). Bootstrap on an established cluster directory keeps everything and mints nothing.
**Format 1 is refused**, with an error naming the format found, the formats this build reads and
that no migration exists yet - C1 permits that for a development build, and it is the choice
taken. The format is read first and alone, from a struct with one field, so a marker from another
build fails on its format and never on a field that build spelled differently. Every write is
temp file, fsync, rename, directory fsync. `Identity::verify_cluster` is the seam the M2 handshake
calls: it refuses `WrongCluster` and never touches the file. `observe_topology` is the one
rewrite the marker ever sees, moves the one field, and refuses to go backwards.

**A directory lock.** `shoal.lock` beside the marker, an advisory `flock` the pool holds for its
lifetime, so two local processes on one path get `StorageDirectoryLocked` rather than two servers
claiming one identity. The kernel releases it when the process dies.

**The `cluster:` block** in `shoal-core/src/server/conf/cluster.rs`, `deny_unknown_fields`, with
C1's fields and defaults and one addition, `control_core_shared`. Absent means standalone and
standalone is byte-for-byte what the server was. `Cluster::validate` refuses at startup what this
build does not act on, naming the milestone: `seeds` without `bootstrap` is joining (M3), `tls` is
the peer handshake (M2), `control_voters` outside {1, 3, 5} is not a quorum anyone wants, and an
unspecified interface with nothing advertised is not an address. Durations are a `DurationSpec`
written as `500ms`, `5s`, `30m`, `2h`, with a bare number refused. `Cluster::policy()` is the
`BootstrapPolicy` the bootstrap writes into the control state.
[Configuration](../getting-started/configuration.md#cluster) is the reference.

**The control core.** `ControlPlacement::resolve` reads the process's real affinity with
`sched_getaffinity` - a cgroup cpuset, a `taskset` - and refuses `ControlCoreNotAllowed` when
the configured cpu is outside it, naming what is allowed. It reads the cpu's physical core and
every SMT sibling from glommio's topology, and `Resources::cpus_reserving` keeps the shards off
the whole core unless `control_core_shared` says the machine cannot afford it, in which case the
sharing is recorded in the topology view and in every benchmark artifact. Standalone mode keeps
`Resources::cpus` exactly: only cpu 0 is reserved, its sibling stays a shard candidate, because
the benchmark layout depends on it.

**A glommio `AsyncRuntime` for openraft**, `shoal-core/src/server/control/runtime/`: spawn and
join over `spawn_local`, sleep and timeout over `Timer`, a bounded mpsc with weak senders, a watch
with the seen/unseen semantics openraft's metrics and storage callbacks rely on, an async mutex,
and `futures_channel`'s oneshot. openraft's `single-threaded` feature empties every `Send` and
`Sync` bound in its runtime abstraction, so every one of these is `Rc` and `RefCell`, and the
library's own conformance suite - `openraft_rt::testing::Suite` - passes on it.

**The control store**, `control/store.rs`, under `<latency_sensitive.path>/control/`: a log of
`[u32 len][u32 gxhash32][json]` frames appended and `fdatasync`ed before the `IOFlushed` callback
completes, with a torn tail detected by length or checksum and truncated at open; `vote.json`,
`committed.json` and `purged.json` replaced whole; `state.json` holding the applied log id, the
membership and the `ControlState`; `snapshot.json` the last snapshot. The whole log is held in
memory as well, so reads answer without IO and `truncate_after` and `purge` rewrite the file from
the map. `openraft::testing::log::Suite` passes on it, and the crash test tears an append.

**The control plane**, `control/plane.rs`: one thread, `LocalExecutorBuilder` pinned to the
control cpu and named `shoal-control`, that opens the store, builds `Raft<ControlConfig,
ControlStateMachine>` with a network whose every peer is unreachable, initializes the group with
itself on a fresh directory, waits to be leader, writes `Bootstrap {cluster, policy, member}` if
the applied state has no cluster and `ObserveMember` always, records each applied topology
version in the marker through `spawn_blocking`, reports `ControlEvent::Ready`, and then answers
`Topology` and `Shutdown` requests over a `kanal` channel until the pool says stop. The `Raft`
handle never leaves the thread. `ControlState` is `{cluster, topology_version, members, policy}`;
`apply` is pure, refuses a second bootstrap and an observation before one, and moves the version
only when something changed.

**The pool.** In cluster mode `ShoalPool::start` validates the block, resolves the placement,
builds the shard cpuset off the reserved core and checks the isolation, takes the lock, claims the
marker with `Bootstrap` intent, starts the control plane *before* the shards, and then the shards.
`ready` waits for the control plane first; `failure` reports a dead one; `exit` stops it last.
`identity()`, `control_placement()`, `shard_cpus()` and `topology()` are new; `topology()` on a
standalone node is `NotClustered`. `TopologyView` reports the cluster, the node, the version,
every member, the desired replication factor beside the active one - which is 1 - and the control
core and whether it is shared.

**The fixture.** `Allocation::control` is set: every cluster server gets a whole physical core for
its control thread, disjoint from its data cores and every other node's, or none - recorded as a
shared control thread - when the machine runs out. `NodeKind::Standalone` starts a child with no
block. `Node::spawn_with` takes an affinity mask applied with `sched_setaffinity` in `pre_exec`
and a marker to stage. `Endpoints` carries the node and cluster ids, the control core, whether it
is shared, the topology version and the shard cpus. `Cluster::restart` kills a node and starts it
again on its directory.

**The spike and the benchmark arm.** `shoal-spike` is a workspace binary that runs N three-member
groups on one pinned executor with a counting loopback network and prints the idle and durable
tables the [decision record](../distributed/protocol.md#q1-and-q13-decided-at-m1) now carries.
`macro/cluster/overhead/nodes/1` is the grid's reference cell with a `cluster:` block, appended to
`workload_ids::IDS` at port 12374, in the `cluster-overhead` family and the `cluster` group, and
its capture carries a `ClusterFacts` record read from the pool's topology view.

## Design choices

**glommio, not a current-thread Tokio.** C1 and C3 both said Tokio, and both are struck through.
The `single-threaded` feature is what changed the answer: with it, openraft asks nothing of a
runtime that a `LocalExecutor` does not already have, and the alternative was a second reactor,
a second timer wheel and a second family of channel types in a process that has one of each -
plus a `Send` bound on every store that glommio's `!Send` file handles would have had to be hidden
from behind a channel. Writing the five runtime primitives was under a thousand lines, and the
library's conformance suite is what says they are right.

**The format is checked first, alone, and from a one-field struct.** A marker's other fields mean
what they mean only under a format the reader understands, so `FormatOnly` reads `format` and
nothing else, and only a supported value lets the rest be parsed. A format 1 marker therefore
fails on "format 1" and never on "missing field `node`", which would have been true and useless.

**Refuse format 1 rather than upgrade it.** An upgrade would mint a node id for a directory whose
data predates the concept, which is exactly the "quietly change what the data means" C1 forbids;
and it would be a migration written before M10 has decided what a migration verifies. The error
says there is none yet and who owns one.

**`topology` is the one field ever rewritten.** The resolved page for item 45 said the marker is
never rewritten in place; that invariant is now narrowed, on that page, to the identities, the
shard count and the layout. A topology observation goes through the same staged-rename path as
the claim, on a blocking thread so the executor's timers keep ticking, and refuses to go
backwards because the field is a high-water mark for recovery to resume from.

**The control plane starts before the shards and stops after them.** A group that cannot start
refuses the node before a shard has bound a port a client could reach; a shard is never left
asking a group that has already shut down.

**Bootstrap on an established directory is a no-op.** `bootstrap: true` in a file is a statement
about how the cluster was created, not an instruction to create another on every restart. The
marker keeps the cluster id, `ControlState::apply` refuses a second `Bootstrap`, and the plane
writes one only when the recovered state has no cluster.

**The policy is recorded, not enforced.** A `write_consistency: Quorum` on a one node cluster
is honoured trivially and a `replication_factor: 3` cannot be. Recording both, and reporting the
desired factor beside the active one, is what makes the gap visible rather than a claim; enforcing
either would be pretending to a property the node does not have.

**The fixture gives every server a control core last.** Data cores are allocated exactly as M0
allocated them and the control cores come from what remains, so an M0 plan's data allocation is
unchanged by M1 and a machine short of cores degrades one node's control thread to shared rather
than failing the plan.

**Why the spike reuses `ControlConfig` and the control store.** The durable variant is the real
store under a temp dir, so the number it gives is the number the control plane pays; the memory
variant is the same traits with the files taken out, so the difference between the two is the IO.
A spike-only type configuration would have measured a harness.

## Alternatives rejected

- **A node id derived from the address**, which C1 rejects and this page inherits: an address is
  where a node is today.
- **A `Join` intent at M1.** Joining is refused at `validate` naming M3, so the marker's intent
  has two values. A joiner's directory before admission - a node id and no cluster yet - looks
  exactly like a standalone one, and M3 has to decide how the two are told apart; deciding it now,
  with no joiner to test it against, would be guessing. Filed on [Todos](../appendix/todos.md).
- **A lock through the marker itself** (a pid in the file). A pid is stale the moment the process
  dies uncleanly; an `flock` is released by the kernel.
- **Tokio's `LocalSet` on the control core.** Above.
- **An rkyv control log.** The files under `control/` are read by people as often as by the
  server - `cat state.json` is the topology view a node has - and the log is a few entries a day.
  JSON costs nothing that matters and is legible. The frames' checksum is gxhash because gxhash is
  already what every intent log frame is checksummed with.
- **Counting `active_rf` from placed tablets.** Nothing places tablets. The count of members
  that could hold a replica, capped at the desired factor, is the honest number until M4.
- **Measuring raft-rs in the spike.** It has no runtime abstraction to adapt; a comparison on
  the runtime seam would measure the harness. Recorded on the decision record with the reason.
- **A `nodes/3` benchmark arm or a replication-factor axis.** Three processes need M2's
  transport, and a factor that is recorded and not enforced is an axis with no effect - an arm
  that measures nothing looks like a result.
- **Enforcing isolation by refusing when the machine is too small**, rather than recording the
  sharing. C1 asks for the explicit shared core, and a benchmark on a small machine that refused
  to run would leave no record at all.

## Limitations

- **No control listener is bound.** `control_port` and `port` are advertised in the member record
  and listened on by nothing; `Endpoints.data` and `.control` stay `None`. M2.
- **No joiner.** `seeds` is refused naming M3. A second node cannot be admitted, and the group's
  network returns `Unreachable` for every peer it would ever be told about.
- **The replication policy is recorded and reported, never enforced.** A one node cluster serves
  every read and write locally exactly as a standalone node does. `active_rf` is the members that
  could hold a replica, not the replicas any tablet has.
- **The topology version is not proof of freshness.** C1 says so and the marker's docs repeat it:
  tablet term and vote live in the tablet's own manifest, which does not exist yet.
- **Format 1 has no migration**, and neither does standalone-to-cluster. Both refusals name M10.
- **`verify_cluster` has no caller.** It is the seam the M2 handshake will call, tested directly.
- **The marker's `layout` is always 1.** The check exists so that a rehome (M9c) has a place to
  bump it; nothing bumps it.
- **The control thread's failure is reported as shard `usize::MAX`** through `ShoalPool::failure`,
  which keeps that handle's shape; a caller matching on the shard index sees a number no shard
  has.
- **A cluster node with default settings may run one shard fewer than a standalone one** on a
  machine where `resources.cores` is the whole box, because the control core's sibling is reserved
  too. The benchmark arm's `cores` record shows which happened; on the development host it showed
  eleven data cores under twelve shards.
- **The spike ran on the development host under `powersave`**, so its numbers bound the shape of
  the Q13 answer rather than its value on the benchmark host. It measured no library under a
  *shard's* ownership.
- **A composite partition key does not compile** ([item 92](../appendix/known-issues.md)), which
  the golden test found and did not fix; the frozen shapes are the two that build.
- **The benchmark arm has no page of its own.** It lives on *Every workload* with its family's
  four blocks; a cluster page is for when there is a second node to draw.

## Invariants to uphold

- **The identities, the shard count and the layout are written once.** `StorageMeta::new` is the
  only constructor, the claim is the only writer of those fields, and `observe_topology` rewrites
  `topology` and nothing else. `a_topology_observation_moves_one_field` holds it.
- **The format is checked before anything else in the marker is trusted**, from `FormatOnly`,
  against `SUPPORTED_FORMATS`. Adding a format means adding to that list *and* deciding what a
  reader of the old one does, on the marker page.
- **A directory never changes mode.** Standalone stays standalone and a cluster directory stays
  in its cluster, until M10 writes the migration. `a_mode_change_is_refused_both_ways`.
- **A second bootstrap never mints a second cluster**, at the marker (`claim` keeps the id) and at
  the state machine (`apply` refuses).
- **`IOFlushed` completes after `fdatasync`, `save_vote` returns after the rename and directory
  sync.** That is P3's durable quorum at the control store, and the storage suite plus the torn
  append test are what hold it.
- **The `Raft` handle never leaves the control thread.** The pool holds a `ControlHandle` and a
  channel. A future that needs the handle elsewhere is a design change, not a convenience.
- **Standalone touches none of this.** No thread named `shoal-control`, no `control/` directory,
  no `cluster` key on the artifact, and `Resources::cpus` unchanged.
  `standalone_needs_no_peer_or_control_listener` and `a_single_node_capture_serializes_no_cluster_record`.
- **The control cpu is checked against `sched_getaffinity`, not against what is online.**
- **Shards stay off both SMT threads of the control core unless `control_core_shared` says
  otherwise, and the sharing is recorded** in `ControlPlacement::shared`, the topology view and
  `ClusterFacts`.
- **The runtime primitives are single-threaded by construction.** `Rc`, `RefCell`, no atomics.
  The `single-threaded` feature is what makes a `Send` bound reappearing a compile error.
- **A workload id is appended, never inserted**; `macro/cluster/overhead/nodes/1` is last and
  `ports.json` says so.
- **`shoal-proto` and `shoal-model` name no runtime and no openraft**, and `cargo tree` on either
  says so.

## Performance

**Nothing here is a capture.** `shoal-bench status` reports every macro capture stale against
this commit, correctly: `shoal-core/src/server.rs`, `shoal-bench/src/workloads/harness.rs`, the
harness's `conf.rs`, `workload.rs` and `workload_ids.rs` all moved, and a workload was added. The
capture that re-establishes freshness is the benchmark host's to take
([Benchmarking](../performance/benchmarking.md)); the development host is neither the hardware nor
the governor the corpus was measured under.

**The two arms ran at smoke scale on 2026-09-11 on the development host** (`europa`, 32
threads, `powersave`) against a scratch copy of `shoal.yml` with local storage, two runs each,
`--allow-dirty`, and the run was deleted rather than left in the corpus. Both `scale` records
were byte-identical (`smoke`, 200 rows of 1024 bytes, 200 keys, concurrency 32, `read_pct` 50,
`persistent_unsorted`) and both `conf` records were, but for the digest. The standalone cell
carried no `cluster` key. The cluster arm carried:

```json
{"nodes": 1, "desired_rf": 1, "active_rf": 1, "write_policy": "quorum", "read_policy": "one",
 "durability": "fsync", "driver": "in-process",
 "cores": [{"data": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], "control": 0}],
 "driver_cores": [], "tables": 4, "tablets": 4096, "emulated": true}
```

Eleven data cores under twelve shards: the control core's sibling was reserved, so one core
carried two shards. That is the placement effect the arm's family page warns about, on record.
The wall clocks - 3.67 ms standalone against 3.87 ms clustered - are smoke-scale and prove the
arms run; they measure nothing, and the standalone arm's own spread across its two runs was 26%.

**What the control plane costs at idle**, from the spike rather than the arm: one group of one
member ticks at openraft's default 50 ms heartbeat and sends nothing, at under 3% of one core in
the spike's three-member measurement and less for a group of one. The
[decision record](../distributed/protocol.md#q1-and-q13-decided-at-m1) has the tables.

**What the runtime conformance suite costs to run:** the timer tests sleep, so
`glommio_runtime_passes_the_openraft_suite` takes about a second and the storage suite, which
fsyncs, about eight. Both are unit tests of `shoal-core`.

## Tests

| Test | What breaks if the feature is reverted |
| --- | --- |
| `partition_keys::partition_keys_hash_to_frozen_values` | Item 65: eight keys, two shapes, hash and tablet each pinned to a literal from the unfixed tree; a changed gxhash major or seed fails here rather than re-homing every row |
| `cluster_fixture::node_identity_persists_and_wrong_cluster_is_refused` | A node bootstrapped, killed with `SIGKILL` and restarted coming back as the same node in the same cluster with its topology version recovered; the cluster directory refused standalone naming the cluster; the standalone directory refused as a member naming M10; `verify_cluster` refusing another cluster with the marker's bytes unchanged |
| `cluster_fixture::unknown_configuration_and_storage_formats_are_refused` | A misspelled key under `cluster:` naming the key; `seeds` naming M3; a format 1 marker naming the format, `reads [2]` and `no migration`; a format 3 marker refused the same way |
| `cluster_fixture::control_core_respects_cpuset_and_smt_reservation` | An affinity excluding the control core refused naming the affinity before a shard starts; two servers on distinct control cores with no shard cpu on either thread of its own; the allocator keeping an exact claim and a control core apart |
| `cluster_fixture::standalone_needs_no_peer_or_control_listener` | A standalone child with the M0 endpoints shape, a node id, no cluster, no control core, no `shoal-control` thread and no `control/` directory, beside a cluster child with each of those the other way |
| `cluster_fixture::documented_cluster_defaults_match_policy_bootstrap` | The `cluster:` block on the configuration page loading to exactly `Cluster::default().bootstrap(true)` - Quorum, One, three voters, a finite grace - and a bootstrap seeding exactly that policy into the control state |
| `cluster_fixture::cluster_fixture_accounts_for_all_cores_and_endpoints` | Every server owning a control core disjoint from its data cores; the squeezed machine giving control cores in node order and recording the rest as shared |
| `control::runtime::tests::glommio_runtime_passes_the_openraft_suite` | The whole of `openraft_rt::testing::Suite` on the glommio runtime: spawn, sleep, timeout, mpsc backpressure and weak senders, watch seen/unseen, oneshot, mutex, task locals, deterministic rng |
| `control::store::tests::control_store_passes_the_openraft_storage_suite` | The whole of `openraft::testing::log::Suite` on the control store: every read after every write, membership from log and state machine, truncate, purge, snapshot build and install |
| `control::store::tests::control_store_recovers_from_a_torn_append` | A frame cut off mid-body and a frame with a wrong checksum both truncated at open, the entries before them whole, the vote intact, the next append landing after the last whole frame |
| `control::types::tests::a_bootstrap_is_applied_once` | `ControlState::apply`: a second bootstrap refused, an observation before one refused, an unchanged observation moving nothing, a changed one moving the version |
| `control::cores::tests::*` (four) | The affinity read and ascending; the default core resolving to cpu 0's whole core or refused by name when cpu 0 is not allowed; an impossible cpu refused naming the affinity; standalone having no placement |
| `meta::tests::*` (nine) | The format 2 claim, restart, refusal of format 1 and 3 by name, shard count, bootstrap idempotence, both mode changes, `verify_cluster` without a write, the one-field rewrite that never goes backwards, and the exclusive lock |
| `conf::cluster::tests::*` (four) | `DurationSpec` parsing and round trip; the defaults being C1's; `validate` refusing seeds (M3), tls (M2), an even voter count and an unadvertised `0.0.0.0` |
| `identity::tests::*` (three) | Distinct mints, transparent serde, integer ids ordered and never minted |
| `cluster_overhead::tests::the_arm_is_the_reference_cell_with_a_cluster_block` | The arm differing from `macro/grid/unsorted/r50/1024` in the block alone |
| `workload_ids::tests::the_declared_ids_are_the_registered_ones`, `committed_artifacts::historical_artifacts_and_ports_remain_compatible` | The new id last, at port 12374, with every frozen port unchanged |
| `acceptance_tables::acceptance_tables_have_unique_tests_and_valid_milestones` | M1 marked delivered forcing all five C1 rows to exist as functions |
| `ephemeral_*::nothing_is_written_to_the_storage_directory` | The lock file beside the marker being the only things a server leaves in an ephemeral directory |

## Related

- [M1](../distributed/milestones.md#m1-node-identity-and-the-control-plane-thread), the gate
  this delivers, and [C1](../distributed/node-identity.md), [C3](../distributed/membership.md)
  and [C13](../distributed/protocol.md#q1-and-q13-decided-at-m1), the pages it changed
- [Configuration](../getting-started/configuration.md#cluster), the `cluster:` reference
- [Resolved #65](../appendix/resolved/gxhash-pin.md), the hash pin, and
  [Resolved #45](../appendix/resolved/storage-marker-format.md), the marker whose invariant this
  narrowed
- [F36](cluster-harness.md), the fixture this extended
- [Items 92 and 93](../appendix/known-issues.md), what the golden test found on the way
