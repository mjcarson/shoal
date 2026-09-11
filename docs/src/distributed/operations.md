# C9. Operating a cluster

## Context

An operator needs to see data readiness, durability, replication debt and migration progress,
and to recover without silently discarding evidence. All administration talks to Shoal nodes;
there is no external membership or failover service. Basic observability and authorization land
with the operations they expose, not only at the end of the feature.

## What exists today

Shoal has authenticated client principals, TLS, a schema-specific `shoalctl` TUI and optional
OTLP tracing/metrics. The pool lacks a reliable readiness/failure handle. It has no distributed
repair, migration, backup/restore or cluster-admin API. Existing disk archives do not have the
end-to-end integrity metadata required by this design.

## The design

### The Admin frame

Use the authenticated client connection and relay admin requests to the embedded control runtime.
Read-only replies identify the node and control/data versions observed. Mutations carry an
operation id and expected policy/topology version, are forwarded to the control leader when
appropriate, and return an accepted operation/status handle rather than block indefinitely for
a long migration. Repeating the same operation id is idempotent.

| Request family | Information/action |
| --- | --- |
| Members / Topology | Identities, control voters, data placement, leader hints and committed configuration ids |
| Lag / Health | Durable, committed, applied and checkpointed positions; eligible/quarantined/installing copies |
| Detector | Local suspicion, report freshness, incarnation and committed Down episodes |
| Rebalance / OperationStatus | Transition phases, blockers, remaining bytes, disk reserve and resource budgets |
| Decommission / Remove / Replace | Capacity-checked state transitions; replacement joins as a learner first |
| Policy / Maintenance | Versioned RF/default/grace/weight policies; suspend/resume automatic removal |
| Repair / Backup / Restore | Scoped operations with provenance, checksums and explicit recovery boundary |

Authorize every state-changing request, including Repair, through `cluster.admins` from its first
implementation. Log principal, request id, expected version and outcome. Read-only visibility
follows the deployment's auth policy, with topology exposure documented. Admin authorization
belongs in M3/M8/M9 as those requests appear, not retrofitted in M10.

### shoalctl's cluster tab

Show desired versus active RF, learners versus voters, leader hints/terms, per-tablet readiness,
max/histogram replication lag, Down grace remaining, migration phases and blocked reasons.
Surface “two durable copies, desired three, awaiting replacement node” explicitly. A member count
of three is not sufficient evidence that every tablet has three ready copies.

Actions show a preview naming the affected identity, planned data movement and irreversible
boundary, then submit the versioned operation. Long operations survive a disconnected TUI and are
resumable by id. Record state changes so automated and manual removal are equally auditable.

### Metrics

| Family | Essential measurements |
| --- | --- |
| Membership | Control quorum availability, voter count, policy/topology versions, Down/Removing age |
| Replication | Durable/commit/apply lag in entries, bytes and age; missing quorum and under-replicated tablets |
| Writes | End-to-end and quorum/application wait histograms; success, rejection and unknown outcomes |
| Reads | Barrier/application wait, stale/session routing, retry/timeout and incomplete-share errors |
| Recovery | Retained history bytes/oldest position, snapshot generation/progress, blocked recovery and time to catch up |
| Resources | Pending bytes, lane queue bytes, memory caps, free disk reserve, transfer throughput and I/O failures |
| Integrity | Checksum failures, quarantined copies, repair source/provenance and unresolved divergence |
| Failover | Detection/election/recovery/reconnect intervals and client-visible outage |

Aggregate by node/table/role by default; per-tablet series are opt-in to avoid unbounded collector
cardinality. Top-k diagnostics and admin queries identify individual hot or lagging tablets.
A zero sequence gap alone is not an integrity/readiness check. Record replication traffic and
all participating nodes' work, not just the coordinator's profile.

### Traces

Forward and replication spans retain originating context, with links/per-record metadata for
batches. Include term/configuration and transition ids where useful, without making every tablet
an unbounded metric label. Traces distinguish append, durable, commit, apply and reply. Repair and
snapshot operations carry independent operation ids and resource-wait spans.

### Readiness

`start` returns a handle with process readiness and shard-failure notification. Separate process
live, control-plane joined, and data-ready-for-policy states. Expose per-tablet readiness and a
summary that says whether default reads/writes can be accepted. A joining node can answer admin
without claiming ready quorum data. An installing tablet stays ineligible even if `One` tolerates
arbitrary lag. Client load balancers need a documented readiness probe, not a fixed sleep.

### Runbooks

1. **Bootstrap.** Explicitly create the first embedded control group; join the intended nodes.
   Wait for data configuration/replica readiness, not just Members=Up, before default writes.
2. **Add.** Join identity, inspect resource/domain capacity, follow learner transfer and safe
   reconfiguration until the feasible target is reached. Report blocked capacity clearly.
3. **Replace a dead node.** Start an authenticated replacement as a new identity or use Replace
   to pair it with the old member. At RF=3 on three machines, restoring RF needs that replacement;
   do not wait for removal to complete before supplying the missing capacity.
4. **Decommission.** Preview feasibility, mark Leaving, follow transition ids, wait for safe data
   and control-voter retirement, then stop. Refuse an impossible RF/domain target without override
   through a separate explicit policy change.
5. **Automatic removal and maintenance.** Show the proposed 30m grace, permit null or explicit
   maintenance suspension, persist episode/progress across leader changes, and page on blockers.
6. **Removed node returns.** Never restore its old authority. Preserve the directory for audit or
   verified import; an explicit replacement/import path may reuse validated checkpoint data as
   learner input. Do not delete the only remaining useful evidence on a count-only health check.
7. **Rolling upgrade.** Validate n/n−1 structural schema and codecs; upgrade one failure domain at
   a time, wait for data readiness/catch-up, then activate new capabilities through control state.
   State the last safe binary/storage rollback point. Changed schema needs its own migration.
8. **Control quorum lost.** Established data groups continue where their own quorums survive.
   Restore original control voters from durable storage; no automatic rebootstrap. Topology/admin
   mutations remain blocked. Permanent majority loss requires the disaster-recovery procedure.
9. **Backup and restore.** Capture checksummed per-tablet committed checkpoints with configuration,
   schema/format, deduplication state and boundary manifest. Store outside the failure domain being
   protected. The initial backup need not be one cross-tablet transactional snapshot; say so.
   Restore to an isolated new cluster identity, verify histories/data, then explicitly cut over.
10. **Existing single-node data.** Test supported offline conversion or export/import into fresh
    cluster storage, verification, cutover and rollback. Never require destroying the source.

### Repair

Detect storage corruption with persistent archive/checkpoint checksums and validate manifests.
For logical comparison, pin replicas to the same committed applied checkpoint and hash canonical
logical content in deterministic table/partition/key order, including schema and coverage.
Different archive layout, padding or compaction timing must not create false corruption reports.
If replicas cannot reach a common retained boundary, establish a new checkpoint for comparison.

Do not assume the primary is correct. Quarantine checksum-invalid copies, compare independent
verified replicas/backup provenance, and select a source under an explicit accidental-corruption
policy. If a trustworthy source cannot be established, stop destructive repair and preserve
copies for operator recovery. A majority digest can support diagnosis under the stated fault
model but does not prove arbitrary software-corruption immunity.

Repair uses C7's atomic snapshot mechanism and C8's per-tablet transition lock and resource budgets.
It cannot overwrite a newer committed history with an older snapshot. Repairing a corrupted
primary includes removing its serving eligibility and reestablishing authority on a healthy
quorum. Metrics record the evidence, source, replaced generation and verified resulting boundary.

Scheduled scrub/repair intervals and their default are a Q12 decision with a cost measurement;
manual repair is available at M8. Replication is not a backup against deletion, operator mistakes
or corruption applied consistently everywhere.

## Alternatives rejected

Repair-from-primary on any mismatch, raw archived-byte digest as universal logical equality,
handshake-only rolling upgrades, and deleting orphaned data based on RF counts are superseded.
A forced new majority after permanent quorum loss is disaster recovery with an explicit data-loss
boundary, not normal automatic failover.

## What it costs

Integrity scans, checksums, snapshot/backup storage, administrative state and telemetry. Scope and
throttle background work. Strong operational claims require restore and mixed-version exercises,
not just working UI controls. No performance capture is required for this plan-only revision.

## What it breaks

Readiness APIs and startup callers, admin protocol and authorization, storage integrity metadata,
release compatibility and backup tooling. Client-visible error semantics must expose unknown write
outcomes and blocked recovery instead of flattening them into success/failure.

## Invariants to uphold

- Admin mutations are authorized, versioned, idempotent and auditable from first implementation.
- Readiness reflects data eligibility and the requested policy, not just open sockets.
- Repair never trusts a primary solely because it is primary or overwrites unresolved evidence.
- Upgrade compatibility includes payloads, schema and storage activation boundaries.
- Backup/restore preserves identity boundaries and makes its consistency scope explicit.

## Prerequisites

[C1](node-identity.md), [C3](membership.md), [C7](failover.md), [C8](rebalancing.md),
C13 Q10–Q12. Readiness M0 — the process half is delivered as `ShoalPool::ready` and `failure`
([F36](../features/cluster-harness.md)); the control-plane and per-tablet states arrive with the
milestones that add them; basic admin M3; lag M4; repair M8; operations expand through M10.

## How it would be measured

[C10](performance.md) includes scrub/repair interference and restore time in addition to cluster
query capacity. Track backup age and retained recovery points; define RPO/RTO objectives for the
deployment instead of conflating replica failover with disaster recovery.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `readiness_distinguishes_process_control_and_data` | Ready process/admin cannot falsely imply ready default quorum writes | M3 |
| `admin_mutations_require_principal_and_operation_identity` | Unauthorized, stale-version and duplicate requests cannot repeat a membership mutation | M3 |
| `repair_detects_corrupt_primary_and_preserves_evidence` | Corrupt the primary; trusted surviving state repairs it, unresolved divergence stops | M8 |
| `canonical_digest_ignores_archive_layout_at_same_boundary` | Equivalent data compacted differently compares equal; changed/missing data does not | M8 |
| `repair_serializes_with_migration_and_new_commits` | Concurrent repair/move cannot install stale state or destroy current evidence | M9a |
| `rolling_upgrade_survives_operations_and_failure` | Mixed binaries replicate, read, snapshot and elect correctly, with activation/rollback limits | M10 |
| `backup_restore_verifies_history_in_new_cluster` | Restore isolated backups including retry state, validate data, and prohibit old identities joining | M10 |
| `permanent_quorum_loss_requires_explicit_recovery` | No automatic empty bootstrap or destructive choice when durable majority evidence is unavailable | M10 |

## Related

[C2](transport.md) compatibility, [C7](failover.md) checkpoints, [C8](rebalancing.md) transitions,
[C13](protocol.md) failure assumptions and gates, [Observability](../operations/observability.md),
[Authentication](../features/authentication.md).
