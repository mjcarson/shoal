# Milestones

The Before-M0 gate is settled ([decision record](protocol.md#decision-record), 2026-09-11), and
~~M0, M1 and M2~~ ~~M0 through M7~~ M0 through M8 and M9a are delivered. Keep M0–M10 as stable identifiers; M9a/b/c refine M9
without renumbering later work. Acceptance tests live in their owning C pages and are indexed
by [C11](testing.md#the-acceptance-test-table). Each test names one gate below. This is an order
with dependencies and measurable exit criteria, not dates.

**All stages use embedded Shoal coordination. No external membership or failover service is a
prerequisite, a fallback, or an eventual deployment step.** [C13](protocol.md) is the decision
record. Before implementation, settle its blocking questions and record the evidence, including
exact dependency source versions and benchmark provenance.

## Group 0 — Protocol and foundations

### Before M0: the protocol contract

**Settled 2026-09-11.** The six clauses below are [C13's P1–P6](protocol.md#the-contract), one
numbered property each, with the schedule that violates it and the test that owns it. The data
protocol is Raft. The [decision record](protocol.md#decision-record) holds the evidence and pins
the candidate libraries at exact versions; it selects none of them, which stays M1's spike. No
code, type or dependency was added at this gate.

Agree C13's failure model, table-qualified stream identity, durable quorum, committed visibility,
control/data authority split and no cross-tablet transaction promise. Prefer embedded data Raft;
~~Q1's spike selects the library/runtime and tests whether group count/batching are practical~~
the protocol is fixed here and Q1's spike, in M1, selects the library/runtime and tests whether
group count/batching are practical.
The model can begin with that protocol while integration alternatives remain under evaluation.
A custom protocol cannot bypass this gate by calling primary appointment a topology edit.

### M0. Step 0: the harness and the facts

**Delivered** on 2026-09-11 as [F36](../features/cluster-harness.md), which also closed
[item 58](../appendix/resolved/unreported-shard-death.md) and
[item 88](../appendix/resolved/readiness-probe-refusals.md). The eight tests below are runnable:
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
the gate as it was set. *Not done, on purpose:* no control listener is bound, no joiner exists,
and the replication policy is recorded and reported rather than enforced - a one node cluster
serves every read and write exactly as a standalone node does.

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
([decision record](protocol.md#q1-and-q13-decided-at-m1)); the runtime and storage suites
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
is the gate as it was set. *Not done, on purpose:* no joiner - a static placement names the
nodes and every node is still a group of one; no retry - `attempt` is always zero; snapshots are
counted, checksummed and discarded; a peer's certificate is checked to the cluster's authority
and not yet bound to its node identity, which is Q11's with the joiner; and the hop capture
itself is the benchmark host's - the arms ran at smoke scale on the development host.

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
separately compared things, exact at M2; a peer certificate chains to `ca` and its binding to a
node is the joiner's.

## Group A — Replicate a live system

### M3. Membership

**Delivered** on 2026-09-12 as [F39](../features/membership.md), which also closed
[item 96](../appendix/resolved/ping-interval-consumer.md) on the way. All thirteen rows below
are runnable as `cargo test -p shoal --test cluster_fixture`; the fanout measurement as `cargo
run -p shoal-spike --release -- fanout`. What was delivered, what was not, and the evidence are
on the F page; the rest of this section is the gate as it was set. *Not done, on purpose:* the
map is an ordered node list pushed whole, not per-tablet records or deltas - those arrive when a
tablet can move (~~M9a~~ a set's configuration rides the map since [F45](../features/replica-migration.md)); `Initialize` is applied once and a second is refused ~~naming M9a~~ naming the `Move` operation; `Down`
moves nothing, and grace expiry, `Leaving`, `Removing` and removal are M9b's; a replication
factor above one is desired and reported, with one copy served (M4); a certificate is still not
bound to a node - Q11's identity half is the incarnation, and the SAN stays unread; the
detector's grace on a leader change is a constant; and the capture of the arms over real
membership is the benchmark host's - they ran at smoke scale on the development host.

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
the F page; the rest of this section is the gate as it was set. *Not done, on purpose:* a
member behind the purge point cannot catch up, since installing a snapshot is M7's; the retry
table is a bounded in-memory LRU rebuilt from the log, and its durable low-water mark is M6's;
an isolated leader learns it is not one at its lease and not before (M6); leadership after a
failover stays where the election put it (~~M5~~ M6); a node holding no replica of a tablet still
routes its writes to the placement primary's node, which nothing moves before M6; the arms are
closed-loop and the open-loop capacity schedule is filed; and the capture is the benchmark
host's - the arms ran at smoke scale on the development host.

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
are on the F page; the rest of this section is the gate as it was set. *Not done, on purpose:*
a read is not retried within its budget, though the attempt identity a retry needs is minted
and echoed (M6); a token through a leader change and a barrier through one are M6 gates and
were not run; leadership is not moved toward a reader; `Primary` is not a level (Q5); no
coverage list rides the response frame; leases stay deferred (Q6); the stage report does not
yet draw the two wait stamps a read carries; and the capture is the benchmark host's - the seven
arms ran at smoke scale on the development host.

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
set. *Not done, on purpose:* leadership is not moved toward a reader or back to a returning
node; ~~identity expiry is M9a's and only the floor is recorded~~ identity expiry arrived with [F45](../features/replica-migration.md); a returning leader waits out
its old lease before its groups are led again ([item 103](../appendix/known-issues.md#103-a-returning-leader-is-refused-its-own-re-election-until-its-old-lease-lapses-and-hops-to-it-wait));
catch-up past the purge point is M7's; streams never retry; no coverage list rides the
response frame; a `Down` member is never removed (M9b); leases stay deferred (Q6); and the
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
[item 105](../appendix/resolved/volatile-groups-never-purged.md). *Not done, on purpose:* a
snapshot is per group, so a returning node installs every tablet its replica set shares rather
than the ones it is behind on; a partial transfer survives a lane cut but not a receiver restart;
the bench arms ran at smoke scale on the development host, where the outage outlasts the
absence and neither arm shows a catch-up; item 99 (a durable follower's log reversion) stays
for M8, ~~where it is still open~~ where it was [fixed first](../appendix/resolved/durable-log-reversion.md);
and a member isolated on every lane long enough to inflate its term trips an openraft
debug assertion in the control plane when healed
([item 106](../appendix/known-issues.md#106-a-member-isolated-on-every-lane-long-enough-to-inflate-its-term-trips-an-openraft-debug-assertion-when-healed)).

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
left for this milestone by M7, was reproduced and fixed first. *Not done, on purpose:* routing
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
~~Migration interaction is tested when its implementation arrives in M9a.~~ Migration interaction
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
permanent quorum loss stay M10's.

## Group C — Elastic membership

### M9. Migration and the rebalancer

M9 is complete only after M9a/b/c. Each substage is independently reviewable; inter-node migration
and safe node replacement can be delivered before retiring the local shard-count refusal.

### M9a. Safe replica migration

**Delivered** on 2026-09-13 as [F45](../features/replica-migration.md). All nine fixture rows
below are runnable as `cargo test -p shoal --test cluster_fixture -- --test-threads 6`, the C10
rows as `cargo test -p shoal-bench`, with the map's overlay, the record's apply and queueing,
the identity window, the forgotten log and the `migration:` block's bounds as unit tests beside
them. What was delivered, what was not, and the evidence are on the F page; the rest of this
section is the gate as it was set. *Not done, on purpose:* a move is of a whole replica set -
every table's group over the tablets the rule placed together - since routing is per tablet,
and the set's groups move one at a time per shard; there is no transfer budget, no disk reserve,
no same-node move, no replication factor change and no operator-chosen destination shard, all
M9b's; a learner is fed the whole group's snapshot where a log tail would do
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

**Delivers.** Feasible weighted placement, disk reserves, per-node/device transfer budgets,
Decommission/Remove/Replace workflows, automatic grace expiry and maintenance suspension. Persist
progress across control leader restart. No silent RF reduction when only two RF=3 nodes remain.
Resolve Q7/Q8 policy defaults and supported recovery-load envelope with evidence.

**Acceptance.** C8 M9b budgets/weights/capacity/removal/drain rows; C3 persisted grace and maintenance.
Add a fourth node, replace a dead member and exercise impossible drain targets explicitly.

**Evidence/exit.** Healthy add/drain meets zero final errors and an initial p99 inflation budget of
2× within its documented load/deadline envelope. Targets are feasible by bytes/load/capacity, not
exact tablet count. Capacity-blocked cases stay observable and retain surviving evidence.

### M9c. Change local shard count

**Delivers.** Startup executor for vanished-shard files, full log/checkpoint/consensus/dedup recovery,
atomic rehome manifest, resumable local transfer and correct data configuration/address updates.
Only then retire `ShardCountMismatch` and update storage/partitioning documentation.

**Acceptance.** C8 local-rehome crash matrix across several tables, changed core counts and restart.

**Evidence/exit.** No abandoned, duplicated or double-owned data/history; resource and startup costs
recorded. An archive index alone does not pass this gate.

### M10. Operations and the real cluster

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
for behavior actually implemented; keep unsupported limits visible.

## The order is a claim

Protocol decisions precede irreversible format/API choices. Compaction safety, unknown outcomes
and resource bounds ship with replication. Read barriers, retry identity and failover form one
application-correctness gate. Atomic recovery precedes migration; safe migration precedes automatic
placement policy; local shard rehome is separate. Compatibility is designed with the first transport,
admin authorization with its first mutation, and real upgrade/restore exercises gate operational
readiness. Performance evidence can change an implementation choice, not weaken its safety contract.

## Related

[Overview](overview.md), [C13 decisions](protocol.md#questions-to-answer),
[C11 tests](testing.md#the-acceptance-test-table), [C10 performance](performance.md),
[C12 implementation references](prior-art.md#implementation-reading-list).
