# Milestones

Every milestone is delivered: the Before-M0 gate was settled on 2026-09-11
([decision record](protocol.md#decision-record)) and M0 through M10c were delivered between
2026-09-11 and 2026-09-14 as [F36](../features/cluster-harness.md) through
[F50](../features/cluster-operations.md). This page is the record of each gate as it was set
and what met it: the tests it named, the evidence, what was decided under it, and what it left
undone on purpose. M0–M10 are stable identifiers; M9a/b/c refine M9 and M10a/b/c refine M10.
Acceptance tests live on their owning C pages, indexed by [C11](testing.md#the-acceptance-table),
and each names one gate below; `acceptance_tables_have_unique_tests_and_valid_milestones` holds
every section here to those tables. What is still open after all of them is [C15](open-issues.md).

**Every stage uses embedded Shoal coordination. No external membership or failover service was
a prerequisite, a fallback, or a deployment step.** [C13](protocol.md) is the decision record,
with every dependency pinned at an exact version and every measurement labelled by host.

## Group 0 — Protocol and foundations

### Before M0: the protocol contract

**Settled 2026-09-11.** The six clauses are [C13's P1–P6](protocol.md#the-contract), one
numbered property each, with the schedule that violates it and the test that owns it: the
failure model, table-qualified stream identity, a durable quorum, committed visibility, the
control/data authority split and no cross-tablet transaction promise. The data protocol was
fixed as Raft; the [decision record](protocol.md#decision-record) pinned the candidate libraries
at exact versions and selected none, which was M1's spike. No code, type or dependency was added
at this gate, and a custom protocol could not have passed it by calling primary appointment a
topology edit.

### M0. Step 0: the harness and the facts

**Delivered** on 2026-09-11 as [F36](../features/cluster-harness.md), which also closed
[item 58](../appendix/resolved/pool-readiness.md) and
[item 88](../appendix/resolved/pool-readiness.md). The eight tests below are runnable:
`cargo test -p shoal-model`, `cargo test -p shoal --test cluster_fixture`, and
`cargo test -p shoal-bench --test committed_artifacts --test acceptance_tables`. What was
delivered, what was not, and the evidence are on the F page; the rest of this section is the
gate as it was set.

**Delivers.** Pure deterministic protocol/adapter model with stable-storage events, reproducible
schedules and history oracle. Process fixture with readiness/failure handles, real bound endpoints,
cleanup, explicit core allocation and directed fault controls. Initial children are isolated
servers/mock peers, so this stage does not require NodeId, membership or replicated digests.
Extend those fixture operations as the relevant code lands. Preserve historical workload ids,
port mappings and artifact decoding; add cluster environments and separate load-driver support.

**Acceptance.** C11's fixture/oracle/table-structure rows, C10 resource/artifact rows and C13's
`protocol_model_preserves_acknowledged_history`. Register runnable Cargo test entry points.

**Evidence/exit.** Fixture self-tests and saved schedules reproduce their expected violations.
A matched single-node arm verifies that the harness did not change what the old comparison meant.
No storage durability claim is inferred from SIGKILL alone. Full-engine simulation is not required.
*Met:* seven saved schedules under `shoal-model/schedules/` each replay to the violation they
record; `macro/get_ephemeral` at smoke scale driven in-process and through `--server` produced
byte-identical `scale` and `conf` facts and no `cluster` key
([F36, Performance](../features/cluster-harness.md#performance)); the fixture's `kill` is
documented as proving nothing about durability, and no test infers one.

### M1. Node identity and the control-plane thread

**Delivered** on 2026-09-11 as [F37](../features/node-identity-control-plane.md), which also
closed [item 65](../appendix/resolved/gxhash-pin.md). The five tests below are runnable as
`cargo test -p shoal --test cluster_fixture`, the two conformance suites and the crash test as
`cargo test -p shoal-core control`, and the spike as `cargo run -p shoal-spike --release`. What
was delivered, what was not, and the evidence are on the F page; the rest of this section is
the gate as it was set. *Not done at M1, on purpose:* no control listener was bound (M2), no joiner existed (M3),
and the replication policy was recorded and reported rather than enforced (M3, M4) - a one
node cluster served every read and write exactly as a standalone node did.

**Delivers.** Stable node/cluster identity, explicit bootstrap, configurable control core and
SMT/cpuset validation, versioned marker, basic topology observation and single-node embedded
OpenRaft integration. Fix stable partition hashing (item 65) before cross-node routing.
Perform Q1's embedded data-plane library/runtime spike and Q13's initial group/table scale study.
Q2/Q3/Q4 must have concrete storage/application/checkpoint designs before M4.

**Acceptance.** C1's M1 identity/config/affinity/standalone/default rows. Run the selected
control library's storage conformance checks and crash tests for persisted metadata.

**Evidence/exit.** Pin dependency versions and source/API references in Q1. Demonstrate the
network/storage/runtime seams and record idle/active group memory/CPU and batching feasibility.
Compare standalone versus matched one-node cluster; investigate any material overhead. The
control-plane choice does not force the same library or runtime onto every data shard.
*Met:* `openraft` `0.10.0-alpha.34` pinned exactly with the seams named by path
([decision record](protocol.md#q1-and-q13-at-m1)); the runtime and storage suites
pass; idle memory, CPU and message rate recorded at 1, 64, 1024 and 4096 groups under two
timer settings, and the batching finding - heartbeats do not coalesce across groups - is what
M4 inherits; `macro/cluster/overhead/nodes/1` exists beside its standalone twin and both ran at
smoke scale on the development host ([F37, Performance](../features/node-identity-control-plane.md#performance)),
with the real comparison waiting on the benchmark host.

### M2. The inter-node transport

**Delivered** on 2026-09-12 as [F38](../features/inter-node-transport.md), which also closed
[item 94](../appendix/resolved/disconnected-client-cleanup.md) on the way. The four C2 rows below
are runnable as `cargo test -p shoal --test cluster_fixture` (three of them) and `cargo test -p
shoal-core peer` (the malformed-peer one), the hop arms as `shoal-bench run --group cluster`.
What was delivered, what was not, and the evidence are on the F page; the rest of this section
is the gate as it was set. *Not done at M2, on purpose:* no joiner - a static placement named the nodes and every node
was a group of one (M3); no retry - `attempt` was always zero (M6); snapshots were counted,
checksummed and discarded (M7); a peer's certificate was checked to the cluster's authority and
not bound to its node identity (M10c); and the hop capture is the benchmark host's - the arms
ran at smoke scale on the development host.

**Delivers.** Remote contacts, validated forwarding/gathering, separate control/data/bulk lanes,
bounded byte queues, identity/authentication handshake and trace propagation. Use static test
placement before distributed membership. Define compatibility and certificate bootstrap contracts
(Q10/Q11), common replication command framing, deadlines and operation identity fields early.
No opaque widened rkyv struct is called compatible merely because n−1 handshakes.

**Acceptance.** C2's M2 forwarding, malformed peer, bounded lanes and tracing rows. Add fault hooks
before encryption where tests need individual frame manipulation, retaining real TLS tests.

**Evidence/exit.** Local/local-shard/remote hop capture with actual affinity/queue facts. Initial
loopback p50 added-hop budget 100 µs; report tails too. A slow snapshot stream cannot exhaust all
memory or block progress traffic. Source links and chosen encodings are reviewable.
*Met, with one deferral:* the three hop arms exist with every node's cores, the placement, the
hop's expected mix and the data lane's frame and shed counters on the artifact, and ran at smoke
scale on the development host ([F38, Performance](../features/inter-node-transport.md#performance))
- the capture against the 100 µs budget waits on the benchmark host; a half-gigabyte snapshot
stream stalled for a minute sheds at its 64 MiB bound, grows the process by under three bounds
and leaves the control lane pinging and the data lane answering
(`slow_peer_has_bounded_bytes_and_independent_lanes`); the encodings are in
`shoal-proto/src/shared/protocol/peer/` with their layouts drawn in the module docs, and the F
page names them. Q10 and Q11 have their contracts recorded at
[C13](protocol.md#q10-and-q11-at-m2): schema identity, wire version and capabilities are three
separately compared things, exact at M2; a peer certificate chains to `ca`, and its binding to a
node came at M10c.

## Group A — Replicate a live system

### M3. Membership

**Delivered** on 2026-09-12 as [F39](../features/membership.md), which also closed
[item 96](../appendix/resolved/ping-interval-consumer.md) on the way. All thirteen rows below
are runnable as `cargo test -p shoal --test cluster_fixture`; the fanout measurement as `cargo
run -p shoal-spike --release -- fanout`. What was delivered, what was not, and the evidence are
on the F page; the rest of this section is the gate as it was set. *Not done at M3, on purpose:* the map was an ordered node list pushed whole, not per-tablet
records or deltas - a moved set's configuration rides the map since M9a; `Initialize` is
applied once and a second is refused naming the `Move` operation; `Down` moved nothing, and
grace expiry, `Leaving`, `Removing` and removal came at M9b; a replication factor above one was
desired and reported with one copy served (M4); a certificate was not bound to a node - Q11's
identity half was the incarnation, and the SAN was read at M10c; the detector's grace on a
leader change is a constant; and the capture of the arms over real membership is the benchmark
host's - they ran at smoke scale on the development host.

**Delivers.** Embedded control membership, explicit three/five-voter policy, learners, durable
placement intent, stable table identity and replica readiness distinctions. Direct control traffic,
freshness-aware status reports, shard health, duplicate-node fencing and authorized/versioned admin
operations. Node joins with no tablet authority. Initial data bootstrap is explicit, not RF
reduction inferred from available members. Control and data configuration ids are separate.

**Acceptance.** C3's M3 membership/report rows; C4's M3 map/table/client topology rows; C1 duplicate
identity; C2 independent-control-networking; C9 readiness/admin rows; C13 no-external-coordinator.

**Evidence/exit.** Healthy metadata agreement plus minority isolation and restart tests; topology
fanout/group-scale budgets measured. Joining a fourth node leaves a three-voter policy at three.
Membership evidence is not used as a substitute for safe data election.
*Met:* three nodes agree on members, voters and leader and recover them after every node
restarts (`three_nodes_bootstrap_without_external_membership`); a cut minority cannot commit a
policy change or admit a joiner (`minority_cannot_commit_membership_changes`); a member restarted
with dead seeds keeps its identity and log (`lost_seeds_do_not_rebootstrap_existing_directory`);
a fourth node stays a learner under a three-voter policy
(`fourth_data_node_does_not_change_control_voter_count`); the fanout budgets are the
[Q13 record at M3](protocol.md#q11-and-q13-at-m3) - a frame under 16 KiB at sixty-four members
and sixty-four tables, a thousand subscribers pushed in four milliseconds, 370 KiB a second of
reports into the leader at sixty-four members; and no membership entry is data election - a
`Down` verdict changes the up count and nothing about any tablet's quorum.

### M4. Replication and quorum writes

**Delivered** on 2026-09-12 as [F40](../features/replication.md). All fourteen rows below are
runnable: the nine fixture rows as `cargo test -p shoal --test cluster_fixture`, the two WAL
rows and the placement row as `cargo test -p shoal-core`, the two C10 rows as `cargo test -p
shoal-bench`; openraft's storage conformance suite runs over the shared WAL and the memory log
under `cargo test -p shoal-core wal`. What was delivered, what was not, and the evidence are on
the F page; the rest of this section is the gate as it was set. *Not done at M4, on purpose:* a member behind the purge point could not catch up, since
installing a snapshot came at M7; the retry table was a bounded in-memory LRU rebuilt from the
log, and its durable low-water mark came at M6; an isolated leader learned it was not one at its
lease and not before (M6); leadership after a failover stays where the election put it; a node
holding no replica of a tablet routed its writes to the placement primary's node until M6
routed by health; the arms are closed-loop and the open-loop capacity schedule is filed; and
the capture is the benchmark host's - the arms ran at smoke scale on the development host.

| Test | Where | What it asserts |
| --- | --- | --- |
| `quorum_success_requires_distinct_durable_voters` | `shoal/tests/cluster_fixture.rs` ([C5](replication.md)) | The lane into both followers cut: a write through the leader is `OutcomeUnknown` at the deadline and three rotations and flushes of the leader's WAL release nothing; one follower healed, the write commits and reads back on both; the cut follower's digest differs |
| `rotation_preserves_pending_replication_requirements` | `shoal-core/src/server/wal/tests.rs` ([C5](replication.md)) | Appends from three groups across four forced rotations: every `IOFlushed` completes exactly once, every location names its generation, entries read back from a sealed segment |
| `bootstrap_does_not_reduce_configured_quorum` | `shoal/tests/cluster_fixture.rs` ([C5](replication.md)) | Factor three on one node: readiness reports `active_rf` 1 and short default writes, an insert refused `QuorumUnavailable`; two joiners and `Initialize`: groups of three, the write admitted, committed and read on every node, `active_rf` 3 |
| `async_replica_cannot_weaken_durable_quorum` | `shoal/tests/cluster_fixture.rs` ([C5](replication.md)) | A cluster node whose persistent table is `Async` refuses to start naming C5; a standalone `Async` node starts; `write_consistency: One` is refused at validation |
| `table_streams_recover_independently_without_holes` | `shoal-core/src/server/wal/tests.rs` ([C5](replication.md)) | Two groups interleaved with one's completions held, the tail dropped unflushed, reopened: each log a contiguous prefix ending at its last durable index, no frame of one in the other's index |
| `duplicates_gaps_and_old_terms_do_not_reapply` | `shoal/tests/cluster_fixture.rs` ([C5](replication.md)) | One follower's lane delayed, cut and healed under writes led by node zero; a leader of other groups killed, writes through a survivor, the old one restarted at a stale term: every acknowledged key present exactly once on every node, digests equal |
| `uncommitted_suffix_never_enters_checkpoint` | `shoal/tests/cluster_fixture.rs` ([C5](replication.md)) | The leader isolated and written through, its WAL rotated and compacted with nothing handed; the majority elects and writes; healed and restarted, the old leader holds the majority's value everywhere, digests equal, and only then does its segment resolve |
| `conditional_results_follow_committed_order` | `shoal/tests/cluster_fixture.rs` ([C5](replication.md)) | Concurrent inserts, updates, deletes and no-ops through all three nodes, every answer in a ledger the `shoal-model` oracle accepts, the converged reads accepted too, three equal digests |
| `slow_tablet_does_not_block_other_tablets` | `shoal/tests/cluster_fixture.rs` ([C5](replication.md)) | Both followers hold one group's flush completions: writes to it pend to `OutcomeUnknown` or are shed `Shedding` at the bound, writes to another group land at once, the leader's memory stays bounded; released, the pended writes commit and apply |
| `volatile_replication_uses_common_encoding` | `shoal/tests/cluster_fixture.rs` ([C5](replication.md)) | Rows of the ephemeral table written through one node read from the other two, the groups reported `volatile`; every node restarted, the rows gone and the persistent rows not |
| `placement_respects_distinct_nodes_and_feasible_capacity` | `shoal-core/src/server/map.rs` ([C4](tablet-map.md)) | Three nodes at three: every tablet on three distinct nodes and every node holding every tablet; four at three: distinct nodes, three quarters each within one; unequal shard counts spread on the shard the primary rule picks; a factor past the placement served at the placement |
| `one_reads_converge_without_exposing_uncommitted_state` | `shoal/tests/cluster_fixture.rs` ([C6](reads.md)) | A cut follower serves the old committed value and converges when healed; the isolated leader takes a write it cannot commit and a read through it does not show it; healed, every node converges on the majority's value |
| `infeasible_rf_policy_is_not_a_throughput_arm` | `shoal-bench/src/workloads/cluster_replication.rs` ([C10](performance.md)) | Three copies on one or two nodes refused as an availability test rather than run at a settled factor; every declared arm feasible |
| `capacity_capture_records_lag_and_offered_load` | `shoal-bench/src/workloads/harness/cluster.rs` ([C10](performance.md)) | A cluster record round-trips the load it scheduled, every replica's lag, pending bytes and unknown and rejected writes, and the outcomes summed; a record from before them still loads |

**Delivers.** Selected embedded data protocol, table-qualified logical histories over a specified
WAL adapter, persisted term/vote and configuration, distinct durable/commit/apply/checkpoint
positions, common command serialization, committed-order mutation/results, bounded pending state,
duplicate-safe acknowledgements and rotation. Default fsynced quorum, optional All, explicit refusal
of unimplemented accepted-only/volatile policies. `One` reads see committed state.

Define and start replicated retry identity/result storage here; complete its leader-change behavior
in M6. Design checkpoint/retention boundaries now even though full transfer lands in M7. Admission,
unknown outcomes, write deadlines and resource bounds ship with the first network write path.

**Acceptance.** C5's M4 durability/rotation/bootstrap/stream/conditional/encoding rows, C4 feasible
placement, C6 committed One reads, C10 policy/lag capture rows. Run library adapter conformance and
injected storage-order tests. Force compaction before commitment as a named regression.

**Evidence/exit.** Healthy convergence across several tables and restarts, exact durable evidence
before success, and bounded lag/queues under slow followers. Compare standalone, RF=1 and feasible
RF=3 with matching semantics and resource budgets. No universal replication-latency multiplier;
record curves and explain overhead. Default durability never changes to meet a target.
*Met:* two tables converge across a delayed, cut and healed lane, a killed leader and a stale
restart (`duplicates_gaps_and_old_terms_do_not_reapply`) and across every node restarting
(`volatile_replication_uses_common_encoding`); a write succeeds only when a second distinct
durable voter has it, and nothing the leader does alone releases it
(`quorum_success_requires_distinct_durable_voters`); a group without a quorum sheds at its
bound while another led by the same node is written at once
(`slow_tablet_does_not_block_other_tablets`); the three arms - RF=1 on three nodes, RF=3
durable, RF=3 volatile - run on one placement with the same cores and record every replica's
debt, and their smoke numbers on the development host are on the
[F40 page](../features/replication.md#performance) as what they are, not a capture; the
default stayed a fsynced quorum, and `One` writes and `Async` replicas are refused by name.

### M5. Read consistency levels

**Delivered** on 2026-09-13 as [F41](../features/read-consistency.md). All five C6 rows below
are runnable as `cargo test -p shoal --test cluster_fixture`, with the limit row's proof and the
timeout row's identity rules as unit tests beside them, the mixed-policy row's control state as
a unit test in `shoal-core`, the C10 row as `cargo test -p shoal-bench`, and the protocol model's
strong read as `cargo test -p shoal-model`. What was delivered, what was not, and the evidence
are on the F page; the rest of this section is the gate as it was set. *Not done at M5, on purpose:* a read was not rerouted within its budget, though the attempt
identity a reroute needs was minted and echoed (M6); a token through a leader change and a
barrier through one were M6's gates and were run there; leadership is not moved toward a
reader; `Primary` is not a level (Q5); no coverage list rides the response frame; leases stay
unbuilt (Q6); the stage report does not draw the two wait stamps a read carries; and the
capture is the benchmark host's - the seven arms ran at smoke scale on the development host.

| Test | Where | What it asserts |
| --- | --- | --- |
| `barrier_read_observes_prior_quorum_write` | `shoal/tests/cluster_fixture.rs` ([C6](reads.md)) | Twenty keys written through node zero and read at `Quorum` through the other two at once, never stale; node one cut from the others: a `Quorum` read through it is `Timeout` and never the old value, a `One` read is the old value, a session read past the new write's token is `Timeout`; healed, both see the new value and node one's counters show the barrier hopped and the replica waited |
| `empty_and_deleted_partitions_have_explicit_coverage` | `shoal/tests/cluster_fixture.rs` ([C6](reads.md)) | Six keys over three nodes at a factor of one, three never written: exactly three rows and success; the node-two key deleted through node two: two rows; a get over unwritten keys alone is a successful empty answer; the lane to node two cut: one error, never the two rows that did arrive; healed: two rows, and no gather resident |
| `limits_apply_after_complete_ordered_gather` | `shoal-proto/src/shared/responses.rs`, `shoal/tests/cluster_fixture.rs` ([C6](reads.md)) | Unit: over four hundred random layouts of partitions over shards and arrival orders, a limit applied per share and then to the ordered union is the limit applied once to the whole; fixture: a limited six key get over three nodes is the first four in named order, in either order, and with one node's shares held it is `Timeout` and never four rows of six |
| `gather_timeout_completes_once_and_discards_late_replies` | `shoal-core/src/server/shard/gather.rs`, `shoal/tests/cluster_fixture.rs` ([C6](reads.md)) | Unit: an expired gather is taken once, a share after expiry or for an older attempt is late, a covered slot again is a duplicate, completion fires once; fixture: node one's shares held and sent twice, node two's held longer, a half-second bundle of a six key get and a single key get is one `Timeout` and one success and the stream ends once, a second get completes at the release, and afterwards nothing is resident and late and duplicate shares were both counted |
| `mixed_table_bundle_resolves_each_table_policy` | `shoal-core/src/server/control/types.rs`, `shoal/tests/cluster_fixture.rs` ([C6](reads.md)) | Unit: a table's read policy records, clears, refuses `All` and an unknown table, moves the version once per change and is idempotent; fixture: `Note` set to `quorum` through the control plane, a `Row` get beside a `Note` get through node one pays one barrier with no override, two under `Quorum`, none under `One`; node one cut, the `Note` half is `Timeout` and the `Row` half succeeds; an unknown table and an unknown level are refused by name |
| `session_token_lineage_is_checked_by_name` | `shoal/tests/cluster_fixture.rs` ([C6](reads.md)) | A minted token is honoured; forged to another cluster it is `WrongCluster`, to another group `UnknownLineage` and counted, sent to a standalone node `WrongCluster`; a raw connection that asked for no token section is sent none and one that asked is sent the token |
| `a_standalone_gather_expires_at_the_query_deadline` | `shoal/tests/gather_expiry.rs` ([Resolved #33](../appendix/resolved/gather-expiry.md)) | A two shard standalone get whose shares are all held is `Timeout` within its second, once; with the expiry absent it never returns |
| `strong_reads_are_linearizable_and_the_cached_leader_knob_is_not` | `shoal-model/tests/protocol_model.rs` ([C13](protocol.md)) | Thirty-two seeded schedules with strong reads complete them under the safe barrier rule without a stale observation; the cached-leader knob deviates in its own name and its saved schedule replays to `Linearizable` |
| `read_capture_records_barrier_and_application_wait` | `shoal-bench/src/workloads/harness/cluster.rs` ([C10](performance.md)) | A read arm's record carries the level, the session flag, the fanout, every node's barriers, hops, barrier and apply waits, session waits, timeouts and dropped shares, summed and per node; a record from before it loads |
| `the_read_arms_share_the_replication_placement` | `shoal-bench/src/workloads/cluster_reads.rs` ([C10](performance.md)) | Three read arms on the factor three placement at every read, four fan-out arms on the factor one placement, every arm feasible, the ids in registry order |

**Delivers.** Data-quorum read barriers, application waits, session-token design/path, complete
negative-result coverage, ordered gather/limit semantics, deadlines/late-reply handling and mixed
bundle policy resolution. Resolve whether Primary and Quorum need distinct API names (Q5).
Wire compatibility uses C2's selected-version contract rather than a new unexplained flag day.

**Acceptance.** C6's M5 barrier, coverage, limits, timeout and mixed-policy rows. Validate empty,
filtered and deleted partitions. Strong reads during leader changes remain an M6 release gate.

**Evidence/exit.** One/barrier/session and fanout read captures with barrier/application wait and
tails visible. No cross-tablet snapshot claim; session lower bounds are scoped and bounded.
Leases remain deferred until Q6 has both a timing proof and worthwhile measured benefit.
*Met:* a strong read through another node sees the write at once and a cut node's cannot be
served stale (`barrier_read_observes_prior_quorum_write`); an empty share, a deleted row and a
missing share are three different answers (`empty_and_deleted_partitions_have_explicit_coverage`);
a limit is proved to commute with the gather rather than assumed to
(`limits_apply_after_complete_ordered_gather`); a gather answers once at its deadline and drops
what comes after by identity (`gather_timeout_completes_once_and_discards_late_replies`); a
mixed bundle resolves each table on its own (`mixed_table_bundle_resolves_each_table_policy`);
Q5 is decided as one strong level and recorded in [C13](protocol.md#q5-at-m5); a session lower
bound is one index in one group of one table of one cluster, sixteen a bundle at most, and is
refused by name outside that scope; no cross-tablet snapshot is claimed, and the read arms'
barrier and apply waits are on their record separately from the round trip - the smoke numbers
on the [F41 page](../features/read-consistency.md#performance) are what they are, not a capture;
and `LeaseRead` is never used.

## Group B — Survive failures and return safely

### M6. Primary failover

**Delivered** on 2026-09-13 as [F42](../features/primary-failover.md). All thirteen fixture rows
below are runnable as `cargo test -p shoal --test cluster_fixture -- --test-threads 6`, the C10
row as `cargo test -p shoal-bench`, with the retry table's persistence, the routing rules, the
detector's fix and the client's retry loop as unit tests beside them. What was delivered, what
was not, and the evidence are on the F page; the rest of this section is the gate as it was
set. *Not done at M6, on purpose:* leadership is not moved toward a reader or back to a returning
node; identity expiry came at M9a, with only the floor recorded here; a returning leader waits
out its old lease before its groups are led again ([Resolved #103](../appendix/resolved/returning-leader.md) made that wait a quiet one, with hops onto it refused at once);
catch-up past the purge point came at M7; streams never retry; no coverage list rides the
response frame; a `Down` member was never removed until M9b; leases stay unbuilt (Q6); and the
capture is the benchmark host's - the arm ran at smoke scale on the development host.

| Test | Where | What it asserts |
| --- | --- | --- |
| `stale_heartbeat_reports_cannot_lose_acked_write` | `shoal/tests/cluster_fixture.rs` ([C7](failover.md)) | The B=100/C=101/A+B=102 schedule on real nodes: node one cut off while 101 commits, healed and caught up, node two cut off while 102 commits, node zero killed; the election lands on node one, the only member whose log holds 102, it is on both survivors, and node zero converges when it returns |
| `delayed_topology_cannot_authorize_old_primary` | `shoal/tests/cluster_fixture.rs` ([C7](failover.md)) | Node zero's data lanes cut both ways with its control lanes up and delayed, so it stays `Up` and the map does not move; the survivors elect and commit; past its lease a write through node zero is `NotLeader` before anything is appended and a strong read is refused, never the stale value; with the lanes swapped every node still commits; healed, the refused write is nowhere and digests agree |
| `shard_stall_with_live_control_plane_can_fail_over` | `shoal/tests/cluster_fixture.rs` ([C7](failover.md)) | The leader's one shard blocked for six seconds while its control thread keeps reporting and the member stays `Up`: the survivors elect within the stall, a write through the new leader commits, and the shard follows the higher term when it wakes |
| `quorum_history_survives_repeated_elections` | `shoal/tests/cluster_fixture.rs` ([C7](failover.md)) | The oracle's mix of updates, deletes and no-ops through all three nodes, every operation under an identity with a retry budget, eight rounds of dropped replies on the hot group's leader, a kill and a restart, then the next leader's; the sequential oracle accepts the history and a read of every key on every node joins it |
| `strong_read_refuses_isolated_old_primary` | `shoal/tests/cluster_fixture.rs` ([C7](failover.md)) | Node zero isolated on every lane while the survivors elect and commit: a strong read through it is `QuorumUnavailable` past its lease and `Timeout` before, never the old value; a `One` read is the old value; a write past the lease is `NotLeader`; healed, its barrier hops and sees the new value |
| `quorum_loss_is_unavailable_without_data_loss` | `shoal/tests/cluster_fixture.rs` ([C7](failover.md)) | Two of three nodes killed: a write through the survivor is `OutcomeUnknown` while its lease lasts and `NotLeader` after, never acknowledged, and a strong read is refused, with the control plane quorumless too so admission is not what refuses; both restarted, a new write commits, every acknowledged key is everywhere and the unknown write's key holds one value everywhere |
| `lost_response_retry_returns_original_result` | `shoal/tests/cluster_fixture.rs` ([C5](replication.md)) | A delete under an identity with its reply dropped on the leader is `Timeout` at the client's deadline; the leader killed, the same delete under the same identity through a survivor is the original result once and a fresh delete finds nothing; checkpointed, purged and restarted on every node, the same identity is answered the same way from `retries.bin` |
| `session_read_waits_for_committed_lower_bound` | `shoal/tests/cluster_fixture.rs` ([C6](reads.md)) | A session read past a token through a cut follower is `Timeout` and served once healed; the leader killed, the token is served past on either survivor, a write through the new leader mints one the other serves past, and a forged lineage is still `UnknownLineage` |
| `read_barrier_survives_leader_change_and_delayed_messages` | `shoal/tests/cluster_fixture.rs` ([C6](reads.md)) | The leader paused with SIGSTOP while the survivors elect and commit and the control lanes into it are slowed; resumed and still believing it leads, its own barrier is refused at its lapsed lease, then hops to the new leader once the higher term reaches it; five seconds of strong reads through it are each the new value or an error and never the old |
| `deadline_and_operation_id_survive_forwarding` | `shoal/tests/cluster_fixture.rs` ([C2](transport.md)) | Four nodes at a factor of three: a delete under one identity through the non-holder and then a holder is the original result once; a write through the non-holder while the group cannot commit is answered at the bundle's own deadline, not the server's; with the lane to the primary cut, a write is sent to another holder under the same attempt and lands once |
| `down_retains_placement_during_grace` | `shoal/tests/cluster_fixture.rs` ([C3](membership.md)) | Node two killed and called `Down` with an episode: the placement and every group's members are unchanged on every survivor and still name it; a key it led is written through node zero once its group elected; restarted it is `Up` in the same placement and converges |
| `metadata_quorum_cannot_replace_a_missing_data_quorum` | `shoal/tests/cluster_fixture.rs` ([C13](protocol.md)) | Node zero's data lanes cut with every control lane up: a policy change commits and moves the map on every node, node zero included, and authorizes nothing - past its lease a write through it is `NotLeader`, a strong read is refused, the survivors commit through the new leader, and healed its refused write is nowhere |
| `established_tablets_survive_control_quorum_loss` | `shoal/tests/cluster_fixture.rs` ([C13](protocol.md)) | Every control lane cut so the control group has no leader: an admin mutation is refused naming `NotLeader`, writes and strong reads through every node still commit; healed, a leader is elected and the same mutation commits and moves the version |
| `fault_capture_preserves_outage_time_series` | `shoal-bench/src/workloads/harness/fault.rs` ([C10](performance.md)) | A ten second timeline with a kill at three, failures to six with one lucky success among them and slow successes after: `before`, `during` and `after` are cut at the client's first failure and its first sustained success, each with its own distribution, the outage is their gap, the series has a bucket per second with the dip in it, the record round trips, and an F40 cluster record loads with no fault |

**Delivers.** Data-group elections, matching-history recovery, current-term activation/read barriers,
old-primary fencing and complete stable retry/result handling. Default operations distinguish
rejected from unknown outcomes. Established data groups survive control-quorum loss where their
own majority remains; metadata mutations stop. Down detection is not an election prerequisite.

**Acceptance.** C7's M6 stale-report/delayed-map/shard-stall/history/read/quorum tests; C5 lost-response
retry; C6 session/leader-change tests; C2 forwarding identity; C3 grace placement; C10 outage series;
C13 separation of control/data quorums. Include updates/deletes/no-ops and conditional results.

**Evidence/exit.** No acknowledged operation/result lost across deterministic and real fault
schedules. Initial client outage objective is election-base + 2s only under the named bounded-delay,
healthy-survivor/backlog conditions. Record actual resource-reduced throughput after failure;
do not require it to equal three healthy nodes. Without safe retries and reads this is not HA-ready.
*Met:* no acknowledged result is lost across the schedules above, the oracle-judged one
included; a rejected outcome and an unknown one are distinct at the lease, on the lane and in
the client's retry list; established groups commit without a control quorum and metadata stops;
`Down` is not an election prerequisite (`shard_stall_with_live_control_plane_can_fail_over`).
*Not met as written:* the outage objective. The follower lease is `election_timeout_max`, twice
the base, and a randomized election follows it, so a failover completes between two and three
times the base - two to three seconds at the fixture's second, ten to fifteen at the default
five - and the [F42 page](../features/primary-failover.md#performance) records the arm's shape
at the default rather than a number against the objective; the throughput after the kill is
on the same record, not required to equal three nodes.

### M7. Recover a node brought back online

**Delivered** on 2026-09-13 as [F43](../features/node-recovery.md). All eight fixture rows below
are runnable as `cargo test -p shoal --test cluster_fixture -- --test-threads 6`, the C10 row as
`cargo test -p shoal-bench`, with the snapshot file format, the chunk assembler, the WAL's
checkpoint filter and marker carry, and the four settings' bounds as unit tests beside them. What
was delivered, what was not, and the evidence are on the F page; the rest of this section is the
gate as it was set. Two defects found while mapping the code were fixed on the way, each
reproduced first: [item 104](../appendix/resolved/segments-recompacted-after-restart.md) and
[item 105](../appendix/resolved/volatile-groups-never-purged.md). *Not done at M7, on purpose:* a
snapshot is per group, so a returning node installs every tablet its replica set shares rather
than the ones it is behind on; a partial transfer survives a lane cut but not a receiver restart;
the bench arms ran at smoke scale on the development host, where the outage outlasts the
absence and neither arm shows a catch-up; item 99 (a durable follower's log reversion) was left
for M8, where it was [fixed first](../appendix/resolved/durable-log-reversion.md);
and a member isolated on every lane long enough to inflate its term tripped an openraft
debug assertion in the control plane when healed, until an isolated member stopped standing
([Resolved #106](../appendix/resolved/isolated-member-term-inflation.md)).

| Test | Where | What it asserts |
| --- | --- | --- |
| `returning_node_catches_up_by_log_or_snapshot` | `shoal/tests/cluster_fixture.rs` ([C7](failover.md)) | Node two killed, a few writes, and back: fed from the retained log, nothing installed. Killed again while the persistent and the ephemeral table checkpoint and purge past what it holds, and back: every group it is behind on installs a snapshot - the persistent ones through the compactor, the volatile ones into memory - every digest agrees, and a delete under an identity made while it was down is the original result through it afterwards |
| `snapshot_has_one_stable_boundary_under_writes` | `shoal/tests/cluster_fixture.rs` ([C7](failover.md)) | Fifty notes take inserts, deletes, reinserts and updates through the leader, each write's committed index kept from its session token; half the mix sealed and compacted, a cut asked for, the rest written and compacted after; the cut's boundary is read from its manifest and every key is exactly the last write at or below it, every deleted key is absent, and nothing after the boundary is in the file |
| `snapshot_install_is_atomic_at_every_crash_point` | `shoal/tests/cluster_fixture.rs` ([C7](failover.md)) | Node two left behind the purge point of every group and restarted armed to die at each of the seven points of an install, then restarted clean: before the marker it is sent a fresh snapshot, from the marker to the checkpoint the install is redone, past the checkpoint the marker is cleaned up and the log feeds it; one generation at every point, every digest agrees and every key read through it is the survivors' value |
| `snapshot_duplicates_and_resume_are_safe` | `shoal-core/src/server/replication/install.rs`, `shoal/tests/cluster_fixture.rs` ([C7](failover.md)) | Unit: repeated and reordered chunks are written once and the resume offset is the prefix held. Fixture: node two left behind with wide rows so every snapshot is many chunks, back with its lanes cut and healed mid-stream, whatever was lost is dropped past the prefix, the end answers where to resume and each group installs once; left behind again with the leader of one group killed mid-stream, the new leader's stream replaces the partial and the group installs once |
| `installing_tablet_never_serves_partial_state` | `shoal/tests/cluster_fixture.rs` ([C7](failover.md)) | Node two left behind and back with every install paused after its first record: a `One` read of an installing persistent group's key through it is `Unavailable`, `GROUPS` shows the group installing and readiness counts it, a read of the ephemeral table - installed in memory at once - is served, and once the pause lifts every read is the new value |
| `retention_and_recovery_memory_are_bounded` | `shoal/tests/cluster_fixture.rs` ([C7](failover.md)) | Node two's data lanes cut both ways and the leader taking wide writes well past `retained_bytes` at a segment size that makes the budget four segments: the leader's sealed WAL stays under twice the budget as each sweep forces the groups pinning the oldest segments to snapshot and purge, its memory grows by less than a bound, writes commit on the majority throughout, and healed node two is behind the forced purge point, installs and converges |
| `down_within_grace_moves_no_replicas` | `shoal/tests/cluster_fixture.rs` ([C3](membership.md)/[C7](failover.md)) | Node one, leading a third of the groups, killed and called `Down`: the groups elect elsewhere and the placement and every group's members are unchanged on both survivors; enough is written to purge past what it held; restarted within the grace it is `Up` in the same placement, catches up by snapshot, and leads nothing until an election it wins |
| `whole_cluster_restart_preserves_durable_history` | `shoal/tests/cluster_fixture.rs` ([C7](failover.md)) | Writes land with one node's WAL completions held back and a batch of replies dropped on the leader, so keys are acknowledged, unknown to their client, or in one node's log only; every node rotates and compacts, is killed at once and restarted; every acknowledged key is on every node once, every unknown key holds one value everywhere, the digests agree, and no segment below a checkpoint was compacted again |
| `catchup_capture_records_convergence` | `shoal-bench/src/workloads/harness/catchup.rs` ([C10](performance.md)) | Samples of a returning node with a lag that falls, a snapshot that lands and a log tail after it are cut into a record with the restart mark, the second it converged and held, the split by path - the snapshot's bytes and entries against the log's - and a series with a bucket per sample; a run that ends before convergence says `none` and keeps the series; an F42 record loads with no catch-up |

**Delivers.** Retained-log catch-up, stable checkpoints, chunked resumable snapshots and atomic durable
installation. Include configuration/history, retry state and absence coverage. Per-tablet eligibility,
retention/recovery space budgets and backpressure. Q3/Q9 completed, including behavior when a hot
stream cannot catch up before its pinned-history budget expires.

**Acceptance.** C7's M7 catch-up/snapshot/crash/retention/readiness/full-restart rows. Kill at every
installation boundary and source failover point while writes and compaction continue.

**Evidence/exit.** Exact state and history after every crash, old or new complete installed generation,
bounded resources and convergence within a stated foreground-load envelope. Capture log/snapshot
catch-up rates and foreground tails; preserving a Down node's placement during grace is proved.
*Met:* one complete generation at every crash point and after a source failover; the
retention budget bounds the sealed WAL and the leader's memory with a cut follower under
load, and a hot stream past the budget is purged behind and fed a snapshot rather than pinning
history; a `Down` member's placement is preserved through the grace. *Met in shape, not in
number:* the catch-up arms record the rates and the tails, and ran at smoke scale on the
development host only, where the outage outlasted the absence; the capture is the benchmark
host's. *Decided:* [Q3 and Q9 at M7](protocol.md#q3-and-q9-at-m7) - the cut is taken between
two compactor jobs at the archives' boundary, and the budget is bytes of sealed WAL with a
forced purge behind the groups pinning it.

### M8. Repair

**Delivered** on 2026-09-13 as [F44](../features/repair.md). All seven fixture rows below are
runnable as `cargo test -p shoal --test cluster_fixture -- --test-threads 6`, the C10 rows as
`cargo test -p shoal-bench`, with the archive record format, the checkpoint and sidecar
checksums, the canonical fold, the judge and the `repair:` block's bounds as unit tests beside
them. What was delivered, what was not, and the evidence are on the F page; the rest of this
section is the gate as it was set. [Item 99](../appendix/resolved/durable-log-reversion.md),
left for this milestone by M7, was reproduced and fixed first. *Not done at M8, on purpose:* routing
around a quarantined copy is per tablet rather than per table, and on the fixture's placement
a read through the holding node is routed to another replica rather than refused, so the
refusal by name is observed only in the window before the map carries the quarantine; a
volatile copy's repair - the group restarted empty for its leader to feed - runs in no fixture
test, since every fault verb needs an archive to fault; a sorted table's canonical cut is
covered by the fold's unit test and no fixture test, the fixture's schema having no persistent
sorted table; `Repaired` records the boundary and the verified index and no archive
generation; a scheduled scrub whose proposal is refused stale waits for the next interval; and
the background arm ran at smoke scale on the development host, where every partition is
resident and the scrubs read nothing off the disk.

| Test | Where | What it asserts |
| --- | --- | --- |
| `repair_detects_corrupt_primary_and_preserves_evidence` | `shoal/tests/cluster_fixture.rs` ([C9](operations.md)) | The leader's record of a group corrupted: a repair finds its copy invalid against two verified copies that agree, the leader hands the lead to one of them and that member repairs the old leader from a snapshot, `Repaired` naming a source that is not the corrupt primary. A different partition erased on each of two followers under valid checksums: a repair stops `Unresolved` with all three digests recorded and no invalid copy, nothing quarantined, nothing installed, and every copy readable as it was. An operator's `source` naming the leader resolves it: both are quarantined under the operator's word and repaired from it. Every key read through every node joins a ledger with the inserts that made it, and the oracle accepts the history |
| `canonical_digest_ignores_archive_layout_at_same_boundary` | `shoal-core/src/server/replication/digest.rs`, `shoal/tests/cluster_fixture.rs` ([C9](operations.md)) | Unit: the fold depends on the rows and nothing else - a map built in either order, a tombstone-only partition beside a missing one, two rows that would run together as bytes. Fixture: three replicas holding the same rows merged into their archives thrice with deletes and updates between, once, and never, scrubbed at one committed index, report verified and equal; a verify of the table is clean on every group. A partition forgotten on one node is a verified digest that differs and a verify quarantines the copy: readiness and the frame name it, a read through it is routed elsewhere, a release lifts it. An erased partition differs the same way; a corrupted record is an invalid copy, refused by name to the read that met it and quarantined on the spot, with the quarantine outliving a restart. The fixture's independent fold agrees with every verdict |
| `corrupt_follower_is_quarantined_and_repaired_from_a_verified_source` | `shoal/tests/cluster_fixture.rs` ([C7](failover.md)) | A follower's record flipped in place: the read that meets it is refused by name and quarantines the copy; a repair of that tablet scrubs, finds it invalid, cuts a snapshot on the leader past the follower's checkpoint - cutting again when the follower answers `Behind` - streams it, restarts the follower's group from its held checkpoint to install it, scrubs again and lifts the quarantine, `Repaired` from the leader to the follower at a verified index past the boundary. The follower alone installed a snapshot, the committed state clears, every key reads through the repaired copy, every digest agrees, and writes after the repair compact and move the held checkpoint on |
| `repair_install_is_atomic_at_every_crash_point` | `shoal/tests/cluster_fixture.rs` ([C7](failover.md)) | Node two armed to die at each of the seven points of an install, its record corrupted and a repair asked for: the install kills it there. Restarted clean it is still quarantined by its marker, and a second repair finds the copy either installed already and agreeing, which lifts it, or still corrupt, which installs it again: at every point no quarantine is left anywhere, every digest agrees, every key of the group reads the survivors' value through node two, and nothing is left in its install directory |
| `durable_log_reversion_is_fed_not_fatal` | `shoal/tests/cluster_fixture.rs` ([C7](failover.md), [item 99](../appendix/resolved/durable-log-reversion.md)) | Node two's WAL segments removed, then its whole WAL directory: both times the survivors are the processes they were, writes through the leader keep committing, node two is fed by log or by snapshot until every digest agrees, and it reports the log it lost |
| `repair_is_authorized_versioned_and_resumable_by_id` | `shoal/tests/cluster_fixture.rs` ([C9](operations.md)) | A user who is not an admin is `Unauthorized`, a stale version `StaleVersion`, the same operation again `Repeated`; the record is readable through every node and completes clean on every group with three reports each. A second operation asked for and the control leader killed while its groups scrub: the drivers wait out the election, commit to the new leader, and the record completes through a survivor with every group done and nothing quarantined |
| `scheduled_scrub_quarantines_without_an_operator` | `shoal/tests/cluster_fixture.rs` ([C9](operations.md)) | With `scrub_interval` at four seconds, two passes over clean copies quarantine nothing and every node was scrubbed; a partition forgotten on node one is found by the next pass, its copy quarantined `Divergent` and named in the committed state, a read of the group through it routed elsewhere, and no snapshot installed anywhere |
| `archive_records_are_checksummed_and_a_flipped_byte_is_refused` | `shoal-core/src/server/tables/storage/fs/tests.rs` ([C9](operations.md)) | A format 2 record round-trips past the header and its prefix; a flipped byte on disk is `CorruptArchive` naming the archive and the partition, counted, and classed fatal. Beside it: a format 1 archive reads unverified and counts, and a torn record is refused |
| `the_repair_block_parses_with_its_defaults`, `validation_refuses_what_is_not_built` | `shoal-core/src/server/conf/cluster.rs` | No scheduled scrub by default, a five minute timeout and one repair at a time; a timeout under the write timeout, an interval under the timeout and no repairs at a time refused by name |
| `background_capture_records_scrub_interference` | `shoal-bench/src/workloads/harness/background.rs` ([C10](performance.md)) | A timeline cut at the repair's marks into three windows with the slower middle one and its failure, a bucket per second, the counters carried; a repair never asked for is a run that is all `before`; an F43 record loads without the block |
| `the_background_arm_shares_the_replication_placement` | `shoal-bench/src/workloads/cluster_background.rs` ([C10](performance.md)) | The arm on the kill arm's durable placement and scale, no fault, a repair of a persistent table inside the run, its id appended after the catch-up arms |

**Delivers.** Persistent integrity metadata, canonical digests at a common committed boundary,
quarantine and verified source selection, atomic snapshot repair, authorization and progress metrics.
Unresolved divergence preserves evidence instead of overwriting every copy from the primary.
Decide scheduled repair policy/cost in Q12.

**Acceptance.** C9's M8 corruption and canonical digest rows, with independent oracle comparison.
Corrupt primary and followers separately; vary archive layout, deletes and checkpoint boundaries.

**Evidence/exit.** Corruption detected and repaired from justified evidence, or stopped with an
actionable unresolved state. Measure scrub/repair resource and foreground-latency interference.
Migration interaction
is `repair_serializes_with_migration_and_new_commits`, delivered with [F45](../features/replica-migration.md).
*Met:* a corrupt primary and corrupt followers are found by their checksums and repaired from
a verified majority, a split nobody can judge stops with every digest recorded and every copy
as it was, and an operator's word resolves it; the layout, the deletes and the checkpoint
boundaries vary across the three replicas of the digest test; every history the oracle judged
is sequential. *Met in shape, not in number:* the background arm records the interference and
the resource, and ran at smoke scale on the development host only - nine groups verified in a
second, seventeen thousand partitions hashed on the loop, nothing read off the disk, the
foreground's median twice what the run's ramp had it at; the capture is the benchmark host's.
*Decided:* [Q12 at M8](protocol.md#q12-at-m8) - the scheduled half is verification only, off by
default and priced by the arm; the destructive half is an operator's, under the majority rule
or a named source, never automatic on an unresolved split; a backup as a second provenance and
permanent quorum loss came at M10b.

## Group C — Elastic membership

### M9. Migration and the rebalancer

M9 was complete once M9a, M9b and M9c were, on 2026-09-14. Each substage was independently
reviewable, and inter-node migration and safe node replacement were delivered before the local
shard-count refusal was retired.

### M9a. Safe replica migration

**Delivered** on 2026-09-13 as [F45](../features/replica-migration.md). All nine fixture rows
below are runnable as `cargo test -p shoal --test cluster_fixture -- --test-threads 6`, the C10
rows as `cargo test -p shoal-bench`, with the map's overlay, the record's apply and queueing,
the identity window, the forgotten log and the `migration:` block's bounds as unit tests beside
them. What was delivered, what was not, and the evidence are on the F page; the rest of this
section is the gate as it was set. *Not done at M9a, on purpose:* a move is of a whole replica set -
every table's group over the tablets the rule placed together - since routing is per tablet,
and the set's groups move one at a time per shard; the transfer budget and the disk reserve
came at M9b, and a same-node move, a replication factor change and an operator-chosen
destination shard are not built; a learner is fed the whole group's snapshot where a log tail would do
([O55](../appendix/optimizations.md#o55-a-learner-inside-the-retained-log-is-fed-a-snapshot-when-the-leaders-cached-cut-is-newer-than-its-purge-point));
a stale route is answered by the origin's one further send rather than a relayed second hop;
identity expiry is judged on the coordinator's own replica against a wall clock and its own
eviction watermark, never in apply; a failed move leaves each group where its committed
membership says and is finished by asking again; and the migration arm ran at smoke scale on
the development host, where the destination was fed by log and no snapshot byte was priced.

| Test | Where | What it asserts |
| --- | --- | --- |
| `move_preserves_write_after_zero_lag_report` | `shoal/tests/cluster_fixture.rs` ([C8](rebalancing.md)) | Three placed nodes and a spare, the set node two leads moved to node three while writes land on it throughout. Once the destination has reported no lag, the source's shard holds its shares and a batch through the source is acknowledged well after that report. Every write acknowledged during the move reads back through the destination and both survivors once the source has retired, the source no longer hosts the group, every digest agrees, and the map carries the set's configuration |
| `migration_resumes_after_each_phase_failure` | `shoal/tests/cluster_fixture.rs` ([C8](rebalancing.md)) | One set moved back and forth between node two and node three eighteen times: at each of the six phases a driver commits, once with the driver of the persistent table's group armed to die right after the commit, once with the destination killed as the phase is reached, once with the control leader killed there. Every move completes `Moved` from its record. Writers through every node update and delete the set's keys throughout under identities with a retry budget; the ledger of their answers and a read of every key on every holder afterwards is accepted by the sequential oracle, and every holder's digest agrees |
| `learner_never_counts_before_configuration_commit` | `shoal/tests/cluster_fixture.rs` ([C8](rebalancing.md)) | Once the destination is being fed, the old quorum is made short by one - a follower paused and the data lanes between the leader and the other cut - so the leader has itself and a learner with every entry. A write through the leader is acknowledged unknown and never committed: it is not visible on the leader's own copy, and no group passes `Reconfiguring`. Healed, the write commits, the transition commits, and the move finishes with every key on the destination |
| `retired_copy_never_serves_from_grace_files` | `shoal/tests/cluster_fixture.rs` ([C8](rebalancing.md)) | At a factor of two, node one's control lanes are cut so its map stays at the placement while the pair node two holds with node zero is moved to node three, the rows archived on the source first. After publication node zero writes values only the new configuration holds. A read through node one is forwarded to node two by its stale map, refused `StaleTopology` there rather than answered from the retained rows, sent once to node zero, and is the new value; the source's marker and archived partition exist during the grace and are gone after it; healed, node one reads its own way |
| `shared_wal_cleanup_preserves_other_tablets` | `shoal/tests/cluster_fixture.rs` ([C8](rebalancing.md)) | One set moved from node two and retired there while writes land on node two's other sets, the WAL rotated and compacted, node two restarted and compacted again. Every key of the other sets reads through node two as through node zero, the retired tablets' partitions are gone from node two, the moved set is whole on its new holders, and the segments that held the retired group's frames beside the others' are reclaimed once the others purge past them |
| `stale_routes_terminate_without_duplicate_writes` | `shoal/tests/cluster_fixture.rs` ([C4](tablet-map.md)) | The same stale router, writing under identities during the move, after the retirement, and after the source is killed. Each write is the value or a named error inside the bundle deadline, never silence; the stale refusals are met and sent on; every acknowledged key reads back on both holders with exactly the value last acknowledged |
| `data_configuration_outlives_stale_placement_hint` | `shoal/tests/cluster_fixture.rs` ([C4](tablet-map.md)) | The driver of the persistent table's group armed to die right after committing `Configured`, before `Activated` and so before publication. The destination's committed voters name it and not the source while every map still places the set by the rule and carries no configuration. Restarted, the record is finished forward from the group's committed membership - never a transition back - the move completes, writes commit through the new configuration and read back on the destination, and the source is never a voter again |
| `retry_identity_survives_snapshot_and_migration` | `shoal/tests/cluster_fixture.rs` ([C5](replication.md)) | A note written under a time-ordered identity through its set's leader, the leader checkpointed past it, the set moved to node three. A retry of the identity through the destination and through the leader is the original result, once; the identity under a changed payload is refused by name; an identity minted before the window is `IdentityExpired` through every node; a fresh identity that is not time-ordered is applied |
| `repair_serializes_with_migration_and_new_commits` | `shoal/tests/cluster_fixture.rs` ([C9](operations.md)) | A verify of the set asked for as it moves is queued behind the move; writes keep committing; the move finishes; the repair then runs on the new configuration and reports clean with node three among its reports and the retired source absent. A move back asked for while a second verify runs is queued behind the repair and runs after it. A partition corrupted on the source before the first move never reaches a holder |
| `a_configuration_overrides_the_rule_and_keeps_the_id` | `shoal-core/src/server/map.rs` ([C4](tablet-map.md)) | A move's destination derives the set's groups as learner specs on the shard the record names, is placed, and holds nothing; the configuration overrides the rule for exactly the set's tablets, the identity is the one the rule minted on every node, the source no longer hosts the group, and the rings route the moved set to its new holders |
| `the_migration_block_parses_with_its_defaults`, `validation_refuses_what_is_not_built` | `shoal-core/src/server/conf/cluster.rs` | A lag of sixty-four entries, a ten minute phase timeout, a five minute grace and one move at a time; a timeout under the snapshot timeout, no moves at a time and a retry window under the write timeout refused by name |
| `an_identity_past_the_window_is_expired` | `shoal-core/src/server/replication/machine.rs` ([C5](replication.md)) | An identity inside the window is not expired and one outside it is; a random identity never is; the table filled past its bound moves the watermark to the oldest identity's time and an unknown identity older than that is expired inside the window; a forgotten random identity moves nothing; the checkpoint carries the watermark and a file without it seeds zero |
| `migration_capture_records_transfer_and_pauses` | `shoal-bench/src/workloads/harness/background.rs` ([C10](performance.md)) | A timeline cut at the move's marks into three windows with the slower middle one, a bucket per second, the phases and the transfer carried, `unfinished` for a run that ended first; an F44 record loads without the block |
| `the_migration_arm_places_a_fourth_node` | `shoal-bench/src/workloads/cluster_migration.rs` ([C10](performance.md)) | The arm on the kill arm's durable placement and scale with one spare beside it and a short grace, no fault, a move from a placed node to the spare inside the run, its id appended after the background arm |

**Delivers.** Durable transition records; nonvoting learner catch-up; library configuration transition
with required old/new quorums; activation barrier; leadership transfer; metadata reconciliation;
bounded stale routing and delayed safe cleanup. Per-tablet serialization with repair and RF changes.

**Acceptance.** All C8 M9a rows; C4 stale-routing/configuration rows; C5 migrated retry history;
C9 repair/migration interaction. Failure matrix includes control leader, source and destination
at every phase and an acknowledged write after a zero-lag report.

**Evidence/exit.** Every phase resumes safely; old-config in-flight operations survive publication
and retirement. Verify foreground correctness first; record transfer bytes/duration and pauses.
*Met:* every phase resumes from its record under a killed driver, destination and control
leader, eighteen times over one set, with the oracle accepting the writers' history; a write
acknowledged after the zero-lag report is on the destination once the source retired; a
caught-up learner never counts before the uniform commit; a stale router's read is refused by
the retired source and answered by a survivor, its writes terminate in two sends without a
duplicate, and the source's files go after the grace without touching the other tablets' history
in the shared WAL; a retry identity is the original result across a checkpoint and a move and
expires by its own time. *Met in shape, not in number:* the migration arm records the transfer
and the pauses, and ran at smoke scale on the development host only - the move done in seven
seconds, the destination fed by log; the capture is the benchmark host's. *Decided:*
[Q4 and Q5 at M9a](protocol.md#q4-and-q5-at-m9a) - a time-ordered identity with a window and a
watermark, and a token that survives a move under the pinned identity.

### M9b. Capacity-aware rebalancing and removal

**Delivered** on 2026-09-14 as [F46](../features/capacity-rebalancing.md). All seven fixture
rows below are runnable as `cargo test -p shoal --test cluster_fixture -- --test-threads 6`,
the C10 rows as `cargo test -p shoal-bench`, with the phase machine, the grace count, the
planner, the tablet bytes, the rate limiter, the capacity override and the `migration:` and
`rebalance:` blocks' bounds as unit tests beside them. What was delivered, what was not, and
the evidence are on the F page; the rest of this section is the gate as it was set. *Not done at M9b,
on purpose:* the leader drains on its own - a decommission, a removal, an elapsed grace - and
spreads onto a new member only under an explicit `Rebalance`, decided with the user; the
balance target is measured archived bytes against node weight, water-filled to what a member
can hold, so a set whose rows are resident weighs nothing until it compacts; the transfer
budget is one token bucket per node, not per device or per pair, and it does not adapt to the
foreground's tail; the replication factor does not change and a `Decommission` is not
cancelled, so three nodes at a factor of three decommissioning to two stay blocked by name until
a fourth joins; the grace is the policy's and not per member; a plan has no preview; and the
p99 budget is judged on the arms' capture, not in the fixture, whose drain test asserts zero
final errors and prints the tails.

| Test | Where | What it asserts |
| --- | --- | --- |
| `automatic_removal_and_rejoin_preserve_fencing` | `shoal/tests/cluster_fixture.rs` ([C8](rebalancing.md)) | Four nodes, three placed at a factor of three and a spare. Node one killed: called down with a grace, the grace counted in committed eighths and expiring at the grace, the member removing under an expiry plan recorded by the policy. Every set moves to the spare, the member is tombstoned with its grace gone, its voter seat - if it had one - refilled by the spare, and no group names it. Writes and reads go on throughout. Node one started again from its directory one incarnation later is refused as removed and its pool fails so; a clone of the directory it died with is refused the same way; the directory is still there |
| `remove_without_replacement_capacity_stays_blocked` | `shoal/tests/cluster_fixture.rs` ([C8](rebalancing.md)) | Three nodes at a factor of three, one lost past the grace: removing under a plan that is blocked naming the missing member with no steps, the desired and active factors still three, three sets reported under-replicated, no tombstone and no copy dropped, reads and quorum writes served by the two survivors, and the plan still blocked two seconds on. A fourth identity started and joined: the plan runs, every set is rebuilt on it, the member is removed, every key reads through the fourth node, the survivors' digests agree and the voter policy refills |
| `removal_grace_survives_control_leader_restart` | `shoal/tests/cluster_fixture.rs` ([C3](membership.md)) | A twelve second grace on a killed member counted past four seconds; the control leader killed and started again; the elapsed time read through the new leader is at least what was committed before and never lower after; the member is removing no sooner than the grace after it was called down and no later than the grace plus two increments and an election; the expiry commits the whole grace |
| `maintenance_suspends_automatic_removal` | `shoal/tests/cluster_fixture.rs` ([C3](membership.md)) | Maintenance on an up member is refused for want of a grace. A killed member suspended inside its grace is still down, a plain member and suspended two seconds past the grace, with a remaining deadline that does not move across two reads a second apart; resumed, it is removing no sooner than the remaining deadline; once removing, maintenance is refused by name |
| `heterogeneous_placement_obeys_feasible_weights` | `shoal/tests/cluster_fixture.rs` ([C8](rebalancing.md)) | Four nodes at three, weights 3:1:1:1, ninety rows archived and every member's bytes reported. A `Rebalance` moves exactly two sets, one off each light placed node onto the spare, and completes; the heavy node holds every set and the most bytes, the three light ones two sets each within one set's bytes of one another after compacting. A second `Rebalance` is `Nothing` with no steps, and no move follows two intervals. Three nodes at three: a `Rebalance` is `Nothing` at once naming the full-copy constraint, and a second the same |
| `node_transfer_budgets_bound_concurrent_sources` | `shoal/tests/cluster_fixture.rs` ([C8](rebalancing.md)) | Four placed at a factor of two and a spare of twice their weight, the retention short, every node sending under a 512 KiB/s bucket, the spare installing one stream at a time, a cap of four moves per node. A `Rebalance` issues three moves onto the spare from three sources at once: the spare's peak concurrent streams is one, at least one begin is refused for the budget, the senders wait on their bucket, what the spare receives never passes the bucket's bound on any sample, every step is `Moved` and the spare holds three sets. Every member's free bytes overridden below the reserve: a `Decommission` is blocked naming the reserve with no steps, feeds no byte in two seconds, and runs to the end once the override is lifted. Writers under identities with a retry budget through node zero throughout, and the history joined by a read of every key on every survivor is accepted by the sequential oracle |
| `decommission_drains_within_supported_load_envelope` | `shoal/tests/cluster_fixture.rs` ([C8](rebalancing.md)) | Three placed at a factor of three and a spare, writers through every placed node under identities with a twenty second retry budget. `Decommission 1`: the member is leaving and up while its sets move, never more than one set moving at a time under the cap of one, then removed with its voter seat refilled by the spare and its process stopped on its own once it learned so. Zero writes unanswered inside their budget, every key on the new holders, the digests agreeing, the history accepted by the sequential oracle; the p99 before and during printed - 148 ms against 166 ms on the development host - and never asserted |
| `a_member_is_decommissioned_removed_and_tombstoned`, `grace_elapsed_is_monotonic_and_expires_once` | `shoal-core/src/server/control/types.rs` ([C3](membership.md)) | The phase machine and every refusal by name, a late `Up` on `Removing`, a replacement inside the set refused, a tombstone refused while a set is held and applied once every set moved, a tombstoned identity refused at observe, admit and health at any incarnation and never re-added by a configuration, a plan's progress and a failed drain's member put back; a grace's count monotonic and of its own episode, suspended counting nothing, resumed continuing, expiring once under the plan named, and none under a `null` policy |
| `the_planner_drains_balances_and_blocks` | `shoal-core/src/server/control/planner.rs` ([C8](rebalancing.md)) | A drain to the least loaded feasible member with the replacement first and one inside the set passed over; feasibility by reserve, by cap and by an absent report; a blocked reason naming the set and the missing member at N = RF; a busy set skipped and one failed twice blocked by name; the feasible weighted target at 3:1:1:1 met in two moves and nothing after; N = RF nothing naming the constraint; unmeasured sets balanced by count; a permuted input planning the same steps |
| `tablet_bytes_follow_the_map`, `a_rate_limiter_paces_a_stream`, `free_bytes_reads_the_filesystem_and_the_override` | `shoal-core/src/server/tables/storage/fs/tests.rs`, `shoal-core/src/server/replication/network.rs`, `shoal-core/src/server/control/capacity.rs` | Bytes per tablet follow an insert, a replacement, a removal and a reopen; the bucket admits a second's worth then paces at the rate, holds no more than a second and treats zero as no limit; the override wins over the filesystem and lifting it restores |
| `the_migration_block_parses_with_its_defaults`, `the_rebalance_block_parses_with_its_defaults`, `validation_refuses_what_is_not_built` | `shoal-core/src/server/conf/cluster.rs` | A 64 MiB/s budget, two streams, a 1 GiB reserve, one move per node, a hysteresis of a tenth and a five second look; a budget under a chunk, no streams, no moves, a hysteresis past one and a look shorter than the reports refused by name; `weight` parsed beside them |
| `rebalance_capture_records_plan_and_windows` | `shoal-bench/src/workloads/harness/background.rs` ([C10](performance.md)) | A timeline cut at the plan's marks into three windows with the slower middle one, a bucket per second, the kind, the steps, the bytes and the p99 ratio in thousandths; `unfinished` with its blocked reason for a run that ended first; no ratio without a during window; an F45 record loads without the block |
| `the_rebalance_arms_share_the_kill_arms_placement` | `shoal-bench/src/workloads/cluster_rebalance.rs` ([C10](performance.md)) | The four arms on the kill arm's durable placement and scale, a spare on three of them and none on the blocked one, the grace on the remove arm alone and its kill with no restart at the plan's mark, each asking for its plan inside the run, their ids appended after the migration arm in order |

**Delivers.** Feasible weighted placement, disk reserves, per-node/device transfer budgets,
Decommission/Remove/Replace workflows, automatic grace expiry and maintenance suspension. Persist
progress across control leader restart. No silent RF reduction when only two RF=3 nodes remain.
Resolve Q7/Q8 policy defaults and supported recovery-load envelope with evidence.

**Acceptance.** C8 M9b budgets/weights/capacity/removal/drain rows; C3 persisted grace and maintenance.
Add a fourth node, replace a dead member and exercise impossible drain targets explicitly.

**Evidence/exit.** Healthy add/drain meets zero final errors and an initial p99 inflation budget of
2× within its documented load/deadline envelope. Targets are feasible by bytes/load/capacity, not
exact tablet count. Capacity-blocked cases stay observable and retain surviving evidence.
*Met:* a drained member's writers see zero final errors under the oracle; a fourth node is
added by a `Rebalance` and by the plan a blocked removal was waiting on; a dead member is
replaced by the spare under an expiry and cannot return; the impossible drain target at N = RF
is blocked by name with every copy kept and the factor untouched; the target is bytes against
weight, water-filled to what a member can hold, and settles in one plan. *Met in shape, not in
number:* the p99 inflation is `p99_ratio_permille` on the four arms, which ran at smoke scale
on the development host only; the envelope it holds inside is the benchmark host's to measure.
*Decided:* [Q7 and Q8 at M9b](protocol.md#q7-and-q8-at-m9b) - a thirty minute default counted
in committed increments with maintenance as suspension, and weights as a byte share capped at
what a member can hold with a hysteresis of a tenth. Per-device budgets are not built and the
page says so.

### M9c. Change local shard count

**Delivered** on 2026-09-14 as [F47](../features/local-rehome.md). Both fixture rows below are
runnable as `cargo test -p shoal --test cluster_fixture -- --test-threads 6`, the in-process
row as `cargo test -p shoal --test storage_meta`, the C10 rows as `cargo test -p shoal-bench`,
with the marker, the hosting, the manifest, each step's redo, the rings, the dispatch and the
archive removal as unit tests beside them. What was delivered, what was not, and the evidence
are on the F page; the rest of this section is the gate as it was set. *Not done at M9c, on purpose:*
a cluster node's slots are claimed once and bound the executors, so growth past them is
[M9b](#m9b-capacity-aware-rebalancing-and-removal)'s `Replace` and not a local operation; the
deal is per slot on a cluster node and per tablet on a standalone one, by count and not by
bytes; the rehome runs before any shard starts, on one core, and never while serving; a
growth's donor keeps its dead records until its own compaction; a partial snapshot install is
dropped for the leader to feed again rather than carried; and every crash test runs one storage
root, so a table under its own `storage.tables` root is moved on the same manifest untested
(the root is marked and locked since [Resolved #43](../appendix/resolved/marker-every-root.md);
the crash matrix over it is still to be run). The "data configuration/address updates" the gate names are what did not have to
happen: no address changes, because a peer names a slot and a slot never moves.

| Test | Where | What it asserts |
| --- | --- | --- |
| `local_rehome_recovers_after_each_crash_point` | `shoal/tests/cluster_fixture.rs` ([C8](rebalancing.md)) | Three nodes at a factor of three, node two claiming four slots on two cores so every peer records four shards and it hosts two per executor from its first start. Rows in every set, some archived on node two by a rotate and a compaction and some left in its WAL past the checkpoint, and a delete under an identity remembered by its retry table. For each of the six points a cluster node's rehome can die at - `planned`, `after_archives`, `after_log`, `after_reclaim`, `before_finalize`, `after_finalize` - node two is restarted with its core count changed and the point armed, dies there, and is started again clean at the new count, alternating one executor hosting all four slots and two hosting two, so the vanished executor's path and the live donor's are each crossed at every point. After every round: the rehome reports the counts it was between with at least one step redone, a slot and a group moved; the hosting holds four slots on the new count; the only executors with files are the ones that run; every peer's record still says four shards and the executors it runs; every key of the persistent table reads through node two and every row of the ephemeral one once its leaders fed it again; every group's row counts agree across the three; and the remembered identity through node two is the original result once and a fresh delete finds nothing. Writers through the other two nodes run throughout under identities with a retry budget, and their ledger joined by a read of every key on every node is accepted by the sequential oracle |
| `standalone_rehome_rebalances_tablets_across_restarts` | `shoal/tests/cluster_fixture.rs` ([C8](rebalancing.md)) | A standalone node at two executors seeded with four hundred rows, half archived by a rotate and half left in its active intent logs. Restarted at three: a rehome from two to three that folded intent logs and copied records, tablets dealt per tablet within one of even over three executors, every row read back. Restarted at one armed to die after the first fold, dead; started again armed to die after the first copy, dead; started clean: the rehome finishes from three to one with two steps redone, every tablet on the survivor, every row read back, and only the survivor's files left |
| `a_changed_core_count_rehomes_and_reads_back`, `the_same_shard_count_restarts` | `shoal/tests/storage_meta.rs` ([C1](node-identity.md)) | In process through a real server: four hundred rows written at two cores; reopened at three, a rehome from two to three moving tablets and copying records with no step redone and every row read; reopened at three again, no rehome; reopened at one, a rehome from three to one and every row read, with executor zero the only one with files. The same count reopens as it always did |
| `a_changed_core_count_is_a_pending_rehome`, `slots_are_claimed_once_and_bound_the_cores` | `shoal-core/src/server/meta.rs` ([C1](node-identity.md)) | A directory claimed at four cores reopened at five is a pending rehome from four to five, the marker's `physical` unmoved until the finalize and the same rehome pending on a third claim; finished, five is the ordinary restart and four a rehome back, and a return to the origin count clears the field; a manifest towards two refuses a claim at three by name and resumes one at two. Slots default to the cores; four on two cores is headroom laid out on two; the same slots or none restart; three is `SlotsFixed` naming `Replace`; four cores is a rehome and five `CoresExceedSlots`; two slots on four cores is `SlotsBelowCores` with no marker written; a standalone node's slots are its cores whatever it asks |
| `the_identity_hosting_is_the_ring`, `hosting_deals_vanished_shards_to_the_least_loaded`, `a_hosting_file_round_trips` | `shoal-core/src/server/hosting.rs` ([C4](tablet-map.md)) | The identity hosting for every count is slot `n` on executor `n` and tablet `t % n`; a standalone shrink deals only the vanished executor's tablets, to within one of even, twice the same; a shrink to one puts everything on zero; a growth moves only donors' tablets onto new executors within one of even with every executor owning something; a cluster shrink deals slots and derives the tablets, a growth back leaves one slot per executor, a count past the slots and a count of zero are refused; three slots on one executor grown to two moves the highest; the file round trips and a table naming an executor the node does not run is refused |
| `the_plan_orders_fold_archives_log_reclaim_finalize`, `a_manifest_resumes_at_its_step` | `shoal-core/src/server/rehome/manifest.rs` ([C8](rebalancing.md)) | Every fold before any copy, every copy before any reclaim, finalize last, no log on a standalone node and no fold on a cluster one, the moves counted as tablets or slots by kind, a source past the new count vanishing; a manifest with two steps done and a partial archive recorded reads back resuming at the third with its report, every step done is finished, a removed one is gone twice, and another format is refused |
| `a_redone_archives_step_removes_its_partial_archive`, `a_redone_log_step_skips_a_group_already_moved`, `a_manifest_for_another_target_is_refused` | `shoal-core/src/server/rehome/tests.rs` ([C8](rebalancing.md)) | A step whose manifest names an archive the destination does not removes the partial file, copies every moving record into a fresh archive the destination names and reads each back with the source's bytes, keeps the destination's own record, and begun a third time copies nothing while still counting; a log step moves five entries above a purge point of two, the vote, the commit and the checkpoint of one slot's group and nothing of another slot's, and begun again appends nothing and duplicates no index; a cluster directory at four slots with a manifest towards two refuses three and four by name and resumes two |
| `a_ring_from_hosting_routes_by_the_table`, `a_placement_hosts_slots_on_executors` | `shoal-core/src/server/ring.rs`, `shoal-core/src/server/map.rs` ([C4](tablet-map.md)) | The ring from the identity hosting is `Ring::new`'s tablet for tablet, and one from four dealt onto three routes every tablet to the executor the table names and none past it; a placement of four slots on two executors beside a peer of three has two local contacts and three remote, every local tablet on the executor hosting its slot and every remote on its slot, every contact owning something, and a placement naming the executors refused; on the map, a node of four slots on two executors serves every local copy from its slot's host, routes every remote primary as before, splits its groups across both executors with `mine` still the slot, and refuses a hosting for another slot count |
| `the_listener_dispatches_a_slot_to_its_host` | `shoal-core/src/server/peer/tests.rs` ([C2](transport.md)) | Four slots on two executors: slots zero and two reach executor zero, one and three executor one; the identity hosting reaches the executor of the slot's number; slot four is malformed under either |
| `removing_an_archive_does_not_hold_the_handle_map_across_the_close` | `shoal-core/src/server/tables/storage/fs/map.rs` | Item 111: two archives open, one removed while the other is read through the cache during the close; both complete, the removed one is gone from the cache and the other is still there |
| `rehome_capture_records_the_move`, `the_rehome_arm_restarts_at_fewer_shards` | `shoal-bench/src/workloads/harness.rs`, `shoal-bench/src/workloads/cluster_rehome.rs` ([C10](performance.md)) | The record carries every count the pool reported and round trips under its own key, and an older cluster record loads without it; the arm restarts its server, at eight shards from twelve, as a cluster of one, at the reference mixture, at smoke scale too |

**Delivers.** Startup executor for vanished-shard files, full log/checkpoint/consensus/dedup recovery,
atomic rehome manifest, resumable local transfer and correct data configuration/address updates.
Only then retire `ShardCountMismatch` and update storage/partitioning documentation.

**Acceptance.** C8 local-rehome crash matrix across several tables, changed core counts and restart.

**Evidence/exit.** No abandoned, duplicated or double-owned data/history; resource and startup costs
recorded. An archive index alone does not pass this gate. *Met:* every key, every group's rows
and the remembered identity survive six crash points in both directions with the writers'
history accepted by the oracle, a vanished executor's files are gone and a donor's moved
entries leave its map, and `ShardCountMismatch` is retired with the storage and partitioning
pages rewritten. *Met in shape, not in number:* the startup cost is `cluster.rehome.millis` on
`macro/rehome/shrink`, smoke-run on the development host only - 589 ms for four slots and
eight groups with nothing archived - and the fixture's report on a node of four slots; the
benchmark host's capture is what the number is. *Not an index:* the manifest is a plan over the
files, and every step reads and writes them.

### M10. Operations and the real cluster

Split into three substages on 2026-09-14, each with an F page, a table of its acceptance
rows and a commit series of its own: M10a is the rolling upgrade, M10b backup, restore, export
and permanent quorum loss, M10c rotation, the cluster tab, the runbooks and the physical
capture. M10 was delivered when all three were, on 2026-09-14 with
[F50](../features/cluster-operations.md). The gate as it was set:

**Delivers.** Full runbooks/TUI, rolling wire/schema/storage compatibility and activation rules,
certificate/address rotation, backup/restore and permanent-quorum-loss recovery, supported
single-node data import/cutover. Real heterogeneous three-node capture with per-node facts and
all earlier open production gates resolved or explicitly unsupported. Admin auth already exists.

**Acceptance.** C9 M10 upgrade/backup/disaster rows, C2 actual mixed-version operations, C1 existing-data
migration and C10 physical environments. Fail a node during a mixed-version run. Perform a real
restore to a new cluster identity, not just create backup files. Q10–Q12 receive decision records.

**Evidence/exit.** Operable three-node RF=3 redundancy with measured tails, lag and failover on unequal
hardware; no claim of physical N>RF scale-out without that experiment. Render generated results
from committed captures and run render --check. Update delivered F pages and current docs only
for behavior actually implemented; keep unsupported limits visible. *Where it stands:* every
row is met on an emulated three-node cluster and the record and launcher for an unequal one
exist; no capture on unequal hardware is committed, and no scale-out is claimed. The renders
and `render --check` are the benchmark host's.

### M10a. Rolling compatibility and activation

**Delivered** on 2026-09-14 as [F48](../features/rolling-compatibility.md). Both fixture rows
below are runnable as `cargo test -p shoal --test cluster_fixture -- --test-threads 6`; the
third runs when `SHOAL_PREVIOUS_TEST_BINARY` names a `cluster_fixture` binary from an earlier
commit and prints a skip otherwise, and was run once against the binary of the commit before
F48. The negotiation, the codec, the header range, the activation and the pin are unit tests
beside them. What was delivered, what was not, and the evidence are on the F page; the rest of
this section is the gate as it was set. *Not done at M10a, and said:* a schema change as a rolling
operation is explicitly unsupported, and so is a marker format migration in place - both are a
new cluster and a restore of a backup or an export; no capability is optional yet, so the gate that
intersects them has nothing to gate; and the suite's mixed cluster is one binary pinned two
ways, with the run from a real previous build opt-in.

| Test | Where | What it asserts |
| --- | --- | --- |
| `mixed_versions_exchange_real_cluster_operations` | `shoal/tests/cluster_fixture.rs` ([C2](transport.md)) | Three nodes at a factor of three, nodes one and two pinned at wire version 4 and node zero at the build's 5. `Members.wire` reports the floor activated, the lowest member at 4 and the highest at 5. Writes led on every node are forwarded from every other and committed at quorum; every link that carried one negotiated 4; barrier reads through every node see every write; the digests agree. Node two is left behind the purge point of every group of both tables and fed snapshots by whichever version leads each group over version 4 links, installs them and converges, still spoken to at 4. Node zero - the only member at 5 - is killed; the two at 4 elect a leader that commits writes and serves barrier reads; node zero comes back and converges. `ACTIVATE 5` is refused naming the two pinned members and not the one that speaks it, and `ACTIVATE 4` is applied without moving anything. Nobody died |
| `rolling_upgrade_survives_operations_and_failure` | `shoal/tests/cluster_fixture.rs` ([C9](operations.md)) | Three nodes at a factor of three, all pinned at 4, every link speaking 4 and the highest member 4, under writers through every node with identities and a twenty second retry budget. Each node is restarted at 5 in turn with readiness and a leader waited on; inside the mixed window, after the first, node one is killed and brought back still at 4, is spoken to at 4, the members report 4 to 5, and `ACTIVATE 5` is refused. Once every member reports 5 and every link speaks it, `ACTIVATE 5` commits and every node sees it activated. A restart of node two pinned at 4 is refused by name before it serves anything, and it comes back at 5 speaking 5. The writers' ledger, joined by a read of every key on every node, is accepted by the sequential oracle |
| `rolling_upgrade_from_previous_binary` | `shoal/tests/cluster_fixture.rs` ([C9](operations.md)) | With `SHOAL_PREVIOUS_TEST_BINARY` set: three nodes started from that build at a factor of three, rows written; each restarted on this build in turn with a leader waited on and rows written through the mixed cluster, the upgraded node speaking 4 to whoever is on the previous build and 5 to whoever is upgraded; every member reporting 5, `ACTIVATE 5` committed and seen by every node; every row read back on every node. Unset, it says so and passes |
| `a_version_range_negotiates_to_the_highest_shared` | `shoal-proto/src/shared/protocol/peer/tests.rs` ([C2](transport.md)) | Ten pairs of ranges negotiate to the highest both hold or to nothing, from either side; the advertised range is the floor to the newest and a pin lowers the top and never passes the floor or the newest; the capability words intersect; every capability defined is required |
| `a_header_below_the_floor_is_refused_and_one_in_range_is_kept` | `shoal-proto/src/shared/protocol/tests.rs` ([C2](transport.md)) | Every version from the floor to the newest decodes and comes back as written; one below the floor is refused naming the newest; a negotiated ceiling refuses what is above it and keeps what is at it; the client lane's version is inside the range |
| `a_v4_manifest_round_trips_with_defaults` | `shoal-core/src/server/replication/snapshot.rs` ([C7](replication.md)) | A stamped v5 manifest goes to its v4 shape and back with the three fields defaulted, is filled by a receiver with its own cluster and the sender, and left alone when it names them; a pending marker written before F48 loads; a version 2 file header carries the cluster, the schema and the time, round trips, and cut short is refused rather than read as version 1; a begin encoded for a version 4 link is the shorter shape and decodes back, one at 5 carries everything, and an end is the same bytes at either |
| `peer_rejects_wrong_cluster_identity_and_malformed_payload` | `shoal-core/src/server/peer/tests.rs` ([C2](transport.md)) | Beside the M2 refusals: disjoint ranges are `NoCommonVersion`, a hello without a required capability `CapabilityMissing`, a hello pinned at the floor accepted, the same hello `BelowActivatedWire` once the map says the cluster activated the newest, and one speaking the newest still accepted |
| `activation_needs_every_member_at_the_wire` | `shoal-core/src/server/control/types.rs` ([C3](membership.md)) | An activation naming the one member below the version is refused naming it, and so is one naming nobody; the floor is applied without moving the version; a third member admitted at the floor holds the activation back until it is removed; a record persisted without the field is healed by the apply; a repeat is answered as the first was; a lower version is refused as never lowering; a stale version is refused; a member below the activated version is refused at observe at a higher incarnation and at admit, and one at it is admitted; the map carries the activation |
| `the_transport_block_refuses_a_pin_outside_the_range` | `shoal-core/src/server/conf/cluster.rs` | No pin is the default; a pin anywhere in the range validates; one below the floor or above the newest is refused naming the range; the pin parses from yaml |

**Delivers.** Rolling wire compatibility and activation rules; the storage and schema halves as
explicitly unsupported limits. **Acceptance.** C2 actual mixed-version operations and the C9
upgrade row, with a node failed during the mixed-version run; Q10's decision record.
**Evidence/exit.** *Met:* a snapshot crosses a version 4 link in both directions, an election
among mixed members commits, a kill inside the window is survived, the activation is refused
until every member speaks the version and refuses a rollback after, and one real previous
build was upgraded in place. *Decided:* [Q10 at M10a](protocol.md#q10-at-m10a).

### M10b. Backup, restore, export and permanent quorum loss

**Delivered** on 2026-09-14 as [F49](../features/backup-and-recovery.md). The three fixture rows
below are runnable as `cargo test -p shoal --test cluster_fixture -- --test-threads 6`; the
recovery, the coverage rules, the file's identity and the block are unit tests beside them,
and the arm's record and placement are the bench's. What was delivered, what was not, and the
evidence are on the F page; the rest of this section is the gate as it was set. *Not done at M10b, on
purpose:* the single-node path is an export restored into a fresh cluster rather than an
import into a node directory, since rows loaded before `Initialize` do not follow it; a
recovery keeps one survivor and nothing else; a backup is not shipped, encrypted or aged; and
a restore is once, into an empty cluster, never over data or into the cluster that cut it.
The section was retitled from "import" to "export" with the decision.

| Test | Where | What it asserts |
| --- | --- | --- |
| `backup_restore_verifies_history_in_new_cluster` | `shoal/tests/cluster_fixture.rs` ([C9](operations.md)) | Three nodes at a factor of three: `BACKUP` is refused until `ACTIVATE 5` is applied on every node; rows on both tables through every node with one key rewritten, one deleted, a retry identity acknowledged and a session token minted; `BACKUP <dir>` comes to `Done` with a file, a manifest, bytes, a checksum and a boundary for every persistent group and `Skipped` for every ephemeral one. A fresh three-node cluster with new identities restores the directory: every persistent group `Restored` and verified, the digests of every node equal, every key read on every new node with its last value and the deleted one absent, the remembered identity answered its original result through the new cluster with a token of the new cluster's groups, a fresh identity applied, the old cluster's token `WrongCluster`, a second `RESTORE` refused as already restored, and an old node's directory started against the new cluster refused as removed, stopping. Nobody died |
| `permanent_quorum_loss_requires_explicit_recovery` | `shoal/tests/cluster_fixture.rs` ([C9](operations.md)) | Three nodes at a factor of three with rows on every node and two identities held back; nodes one and two killed for good. A write through node zero is unknown or refused for want of a leader and never acknowledged, a strong read is refused, and an admin mutation is refused naming the voters, what this node reaches and `force_recover`. Node zero restarted with `bootstrap: true` keeps its cluster and still has no leader. `force_recover` on its stopped directory refuses a survivor list that is not this node, runs, and a second run writes nothing; started, node zero leads alone, writes commit, every key acknowledged before the loss reads back, `Members` shows one and two removing and tombstoned with the recovery and its boundary recorded, and node one started again from its directory is refused as removed and stops. The two held-back identities join, the recovery's plans move every set onto them, one and two are removed, no set is under-replicated, the three digests agree, and every old and new key reads through both newcomers. Nobody died |
| `single_node_data_has_a_verified_cluster_migration_path` | `shoal/tests/cluster_fixture.rs` ([C1](node-identity.md)) | A standalone node at two executors with rows on the persistent table and the ephemeral one, half of the former archived by a rotate and half in the active intent logs. An export while it runs is refused as locked; stopped, one into a non-empty directory is refused by name and writes nothing; `export_standalone` folds the intent logs and writes the persistent table as one file with a manifest, nothing of the ephemeral one. A fresh cluster of three at a factor of three restores it: every group `Restored` and verified from the one file, the records summing to the source's rows, the `DIGEST` of the table on every node equal to the source's, the ephemeral table empty, every row read through every node, a write through a follower committed at quorum. The source started standalone again has its identity, no cluster, every row and the same digest, and a cluster member's directory offered as a source is refused by name |
| `a_recovery_rewrites_membership_and_a_sole_voter_leads` | `shoal-core/src/server/control/store.rs` ([C9](operations.md)) | A cluster of three voters as the log holds it, with one entry appended and never applied: the recovery applies that entry, lands a membership of one and the record at the next index and term with the vote and the commit there, leaves the lost members removing and tombstoned with a plan each, records the recovery with its boundary, writes nothing when run again, and a group opened over the store elects the sole voter without reaching anybody and commits a write |
| `a_restore_refuses_gaps_overlaps_and_a_foreign_table` | `shoal-core/src/server/control/backup.rs` ([C9](operations.md)) | Files covering every tablet of a table are accepted; a tablet covered by none or by two, or a table the cluster lacks, is refused naming it |
| `a_backup_file_identifies_its_cluster` | `shoal-core/src/server/replication/snapshot.rs` ([C9](operations.md)) | A file cut past the activation names its cluster, schema and time in its own header and reads them back with no manifest in hand; one cut below is version 1 and names nothing; both manifests are stamped with the cluster and the origin; the backup manifest beside a file rebuilds one the file verifies against, round trips as JSON, and a manifest naming another cluster does not verify the file |
| `the_backup_block_parses_with_its_defaults` | `shoal-core/src/server/conf/cluster.rs` | `concurrent` 1 and `timeout` ten minutes by default; a timeout under the snapshot timeout or a concurrency of zero is refused; the block parses from yaml |
| `backup_capture_records_files_and_windows` | `shoal-bench/src/workloads/harness/background.rs` ([C10](performance.md)) | A backup's marks, counts, bytes and records are recorded, its windows and series cut as a repair's, an unfinished run has no `seconds` and an empty `after`, the record round trips, and an F47 capture loads without the block |
| `the_backup_arm_shares_the_kill_arms_placement` | `shoal-bench/src/workloads/cluster_backup.rs` ([C10](performance.md)) | The arm is appended after the rehome arm, read beside the repair arm, shares the kill arm's placement, scale and schedule with no fault, asks for a backup of a persistent table a third of the way in |

**Delivers.** Backup and restore, permanent-quorum-loss recovery, and the supported single-node
data path, as C9's runbooks 8, 9 and 10 asked. **Acceptance.** The C9 backup and disaster rows
and the C1 existing-data row; a real restore to a new cluster identity, not just backup files;
Q12's decision record. **Evidence/exit.** *Met:* a backup verified per group and restored into
a cluster with new identities that refuses the old ones, a survivor that stays unavailable
until an operator recovers it and then leads, rebuilds and serves every acknowledged key, and
single-node data restored into a cluster of three and judged by digest against its source.
*Decided:* [Q12 at M10b](protocol.md#q12-at-m10b).

### M10c. Rotation, the cluster tab, runbooks and the physical cluster

**Delivered** on 2026-09-14 as [F50](../features/cluster-operations.md), which makes
[M10](#m10-operations-and-the-real-cluster) delivered whole. The fixture rows below are
runnable as `cargo test -p shoal --test cluster_fixture -- --test-threads 6`; the certificate
row needs the kernel's TLS module and skips by name without it; the remote smoke runs when `SHOAL_REMOTE_SMOKE` names a host and
says so otherwise. What was delivered, what was not, and the table of every M10-named debt are
on the F page; the rest of this section is the gate as it was set. *Not done at M10c, on purpose:*
~~nothing issues or distributes a certificate - a leaf is issued for a node id that exists, by the
operator~~ (since [F51](../features/cluster-deployment.md) `shoalctl cluster` does); no physical capture is committed - the record and the launcher are, and the capture
is the benchmark host's to take; and the backup arm of this gate was delivered by
[F49](../features/backup-and-recovery.md) under M10b.

| Test | Where | What it asserts |
| --- | --- | --- |
| `certificate_rotation_binds_identity` | `shoal/tests/cluster_fixture.rs` ([C1](node-identity.md)) | Three nodes at a factor of three on mutual TLS under a fixture authority, every leaf naming its node, rows on every node. Node one's leaf reissued and reloaded: the report names the node, node two restarted dials it under the new leaf and is dialled by it, writes through both commit. The authority rotated through a bundle: every node trusts both, every leaf reissued under the new one and reloaded, the old retired, a restarted node joins with every link up. Node two's leaf reissued naming node one: node zero restarted refuses its hello as an identity mismatch and its own dials to node two fail naming the certificate; reissued naming no node, they fail as unauthorized; a key that does not parse is refused with nothing changed; reissued as itself, every link comes up and the cluster serves every row. Skips by name without kTLS |
| `address_change_is_observed_and_a_stale_clone_is_fenced` | `shoal/tests/cluster_fixture.rs` ([C1](node-identity.md)) | Three nodes at a factor of three with rows on every node. Node two stopped, its directory copied, started again at fresh peer ports: the same node one start later, every member's record of it - its own included - at the new address and incarnation, its links up both ways at the new address, a write through it committed and every earlier row read through it. The copy started at the old address is the same identity at the same incarnation from another address, refused as a duplicate, stopping on its own, while the restarted node stays on record and serving |
| `a_peer_certificate_names_its_node_and_a_reload_swaps_whole` | `shoal-proto/src/shared/tls/tests.rs` ([C2](transport.md)) | The `shoal-node://<id>` name is read off a leaf's DER and a leaf with a host name only, another scheme or no uuid names none, bytes that are not a certificate are an error; both ends of a finished handshake report the peer's node; a holder swaps both configs on new material, reports the new leaf's node and the counts, keeps both on material that does not parse, and a plaintext holder has nothing to reload |
| `peer_rejects_wrong_cluster_identity_and_malformed_payload` | `shoal-core/src/server/peer/tests.rs` ([C2](transport.md)) | Beside the M2 and M10a refusals: a leaf naming the hello's node is accepted, one naming another node is `IdentityMismatch`, one naming none is `Unauthorized`, with the binding off the chain alone is trusted, and a joiner's certificate is bound before it has a cluster |
| `the_cluster_model_reads_the_admin_frames` | `shoalctl/src/cluster/model.rs` ([C9](operations.md)) | The admin frames as the server writes them build the M9b figure - two copies, desired three, awaiting a member - with the members' phases, grace and bytes, a blocked plan, a backup, a recovery and the wire; the lines say what the figure says; frames from before a field leave the default |
| `an_action_previews_its_boundary_and_follows_its_record` | `shoalctl/src/cluster/actions.rs` ([C9](operations.md)) | Every operation parses from its line and a malformed one is refused by name; a preview names the identity as the model knows it, what moves and the boundary; each sends the request it names and is followed by the record that says when it is done |
| `physical_cluster_records_each_node_environment` | `shoal-bench/src/model/macro_layer.rs` ([C10](performance.md)) | Three unequal environments round trip under `cluster.environments` in node order, `emulated` is false only when the hostnames differ, a difference names the node and the fields, a build difference is not a machine difference, and an F47 record loads without the block |
| `a_remote_spec_parses_and_builds_its_commands` | `shoal-bench/src/workloads/harness/cluster.rs` ([C10](performance.md)) | `<index>=<user@host>:<dir>` parses into its parts, node zero and a relative directory are refused, and the copy, serve and kill command lines are what the launcher runs; a ready line with an environment yields it and one without yields none |
| `a_remote_node_serves_a_smoke_capture` | `shoal-bench/tests/remote_smoke.rs` ([C10](performance.md)) | With `SHOAL_REMOTE_SMOKE` set: the three node arm captured at smoke scale with node one on the named host, and the record naming the machines. Unset, it says so and passes |
| `duplicate_node_identity_is_fenced` | `shoal/tests/cluster_fixture.rs` ([C1](node-identity.md)) | The M3 row, which now reaches the winning clone and feeds it past its shorter log rather than stopping the leader's control thread |

**Delivers.** Certificate binding and rotation, the runbooks and the cluster tab, and the
per-node environment record with a node on another host. **Acceptance.** The C1 rotation row,
the C10 physical environments row, and Q11's decision record. **Evidence/exit.** *Met:* a
certificate is bound on both ends and rotated live, an address change is followed by the
cluster, an operator has a page per procedure and a tab that previews an operation's boundary,
every node's machine is on a capture and a node runs on another host. *Not met, and said:* no
heterogeneous three-node capture is committed; the launcher and the record are what make one
possible, and `render --check` is left for the benchmark host with the rest of this
milestone's renders. *Decided:* [Q11 at M10c](protocol.md#q11-at-m10c).

## The order is a claim

Protocol decisions precede irreversible format/API choices. Compaction safety, unknown outcomes
and resource bounds ship with replication. Read barriers, retry identity and failover form one
application-correctness gate. Atomic recovery precedes migration; safe migration precedes automatic
placement policy; local shard rehome is separate, and was. Compatibility is designed with the first transport,
admin authorization with its first mutation, and real upgrade/restore exercises gate operational
readiness - which they did: the rolling upgrade ran against a real previous build, the restore
went into a new cluster identity, and each found a defect before the page that describes it
was written. Performance evidence can change an implementation choice, not weaken its safety contract.

## Related

[Overview](overview.md), [C13](protocol.md#the-questions-and-where-each-was-decided),
[C11](testing.md#the-acceptance-table), [C10](performance.md),
[C12](prior-art.md#implementation-reading-list), [C15](open-issues.md).
