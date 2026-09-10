# Milestones

Nothing in this chapter is implemented. Keep M0–M10 as stable identifiers; M9a/b/c refine M9
without renumbering later work. Acceptance tests live in their owning C pages and are indexed
by [C11](testing.md#the-acceptance-test-table). Each test names one gate below. This is an order
with dependencies and measurable exit criteria, not dates.

**All stages use embedded Shoal coordination. No external membership or failover service is a
prerequisite, a fallback, or an eventual deployment step.** [C13](protocol.md) is the decision
record. Before implementation, settle its blocking questions and record the evidence, including
exact dependency source versions and benchmark provenance.

## Group 0 — Protocol and foundations

### Before M0: the protocol contract

Agree C13's failure model, table-qualified stream identity, durable quorum, committed visibility,
control/data authority split and no cross-tablet transaction promise. Prefer embedded data Raft;
Q1's spike selects the library/runtime and tests whether group count/batching are practical.
The model can begin with that protocol while integration alternatives remain under evaluation.
A custom protocol cannot bypass this gate by calling primary appointment a topology edit.

### M0. Step 0: the harness and the facts

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

### M1. Node identity and the control-plane thread

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

### M2. The inter-node transport

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

## Group A — Replicate a live system

### M3. Membership

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

### M4. Replication and quorum writes

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
