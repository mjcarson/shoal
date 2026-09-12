# Milestones

The Before-M0 gate is settled ([decision record](protocol.md#decision-record), 2026-09-11), and
~~M0, M1 and M2~~ M0 through M4 are delivered. Keep M0–M10 as stable identifiers; M9a/b/c refine M9
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
tablet can move (M9a); `Initialize` is applied once and a second is refused naming M9a; `Down`
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
failover stays where the election put it (M5); a node holding no replica of a tablet still
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

**Delivers.** Data-quorum read barriers, application waits, session-token design/path, complete
negative-result coverage, ordered gather/limit semantics, deadlines/late-reply handling and mixed
bundle policy resolution. Resolve whether Primary and Quorum need distinct API names (Q5).
Wire compatibility uses C2's selected-version contract rather than a new unexplained flag day.

**Acceptance.** C6's M5 barrier, coverage, limits, timeout and mixed-policy rows. Validate empty,
filtered and deleted partitions. Strong reads during leader changes remain an M6 release gate.

**Evidence/exit.** One/barrier/session and fanout read captures with barrier/application wait and
tails visible. No cross-tablet snapshot claim; session lower bounds are scoped and bounded.
Leases remain deferred until Q6 has both a timing proof and worthwhile measured benefit.

## Group B — Survive failures and return safely

### M6. Primary failover

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

### M7. Recover a node brought back online

**Delivers.** Retained-log catch-up, stable checkpoints, chunked resumable snapshots and atomic durable
installation. Include configuration/history, retry state and absence coverage. Per-tablet eligibility,
retention/recovery space budgets and backpressure. Q3/Q9 completed, including behavior when a hot
stream cannot catch up before its pinned-history budget expires.

**Acceptance.** C7's M7 catch-up/snapshot/crash/retention/readiness/full-restart rows. Kill at every
installation boundary and source failover point while writes and compaction continue.

**Evidence/exit.** Exact state and history after every crash, old or new complete installed generation,
bounded resources and convergence within a stated foreground-load envelope. Capture log/snapshot
catch-up rates and foreground tails; preserving a Down node's placement during grace is proved.

### M8. Repair

**Delivers.** Persistent integrity metadata, canonical digests at a common committed boundary,
quarantine and verified source selection, atomic snapshot repair, authorization and progress metrics.
Unresolved divergence preserves evidence instead of overwriting every copy from the primary.
Decide scheduled repair policy/cost in Q12.

**Acceptance.** C9's M8 corruption and canonical digest rows, with independent oracle comparison.
Corrupt primary and followers separately; vary archive layout, deletes and checkpoint boundaries.

**Evidence/exit.** Corruption detected and repaired from justified evidence, or stopped with an
actionable unresolved state. Measure scrub/repair resource and foreground-latency interference.
Migration interaction is tested when its implementation arrives in M9a.

## Group C — Elastic membership

### M9. Migration and the rebalancer

M9 is complete only after M9a/b/c. Each substage is independently reviewable; inter-node migration
and safe node replacement can be delivered before retiring the local shard-count refusal.

### M9a. Safe replica migration

**Delivers.** Durable transition records; nonvoting learner catch-up; library configuration transition
with required old/new quorums; activation barrier; leadership transfer; metadata reconciliation;
bounded stale routing and delayed safe cleanup. Per-tablet serialization with repair and RF changes.

**Acceptance.** All C8 M9a rows; C4 stale-routing/configuration rows; C5 migrated retry history;
C9 repair/migration interaction. Failure matrix includes control leader, source and destination
at every phase and an acknowledged write after a zero-lag report.

**Evidence/exit.** Every phase resumes safely; old-config in-flight operations survive publication
and retirement. Verify foreground correctness first; record transfer bytes/duration and pauses.

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
