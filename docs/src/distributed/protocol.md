# C13. Protocol decisions, failure model, and open questions

## Context

The first draft appointed primaries from heartbeat progress and treated publishing a new epoch
as fencing. That does not establish which writes a new primary must preserve. This revision
makes the safety protocol a prerequisite for implementing replication, recovery, or migration.
Nothing in this chapter is built.

**Membership, placement, and failover run inside Shoal processes. No external membership,
configuration, or failover service is required.** The control plane uses an embedded `openraft`
group on a reserved core. Data replication must also remain embedded. An external coordinator,
whether operated separately or hidden behind a dependency, is outside the design.

## What exists today

Shoal has per-shard, per-table intent logs and archives, local recovery, and a derived tablet
assignment. It has no replicated log or election implementation. See [C5](replication.md) for
the storage seams and [C3](membership.md) for the control-plane integration.

## The design

### Decisions retained and revised

| Decision | Status |
| --- | --- |
| One primary orders a tablet's mutations | Retained; a tablet is qualified by table identity |
| Embedded `openraft` for cluster membership and placement | Retained; reserved core defaults to CPU 0 but is configurable |
| `Quorum` writes and `One` reads by default | Retained; durable quorum and committed-prefix reads are defined below |
| A down node keeps placement during a grace period | Retained; primary elections do not copy tablets |
| Automatic removal after a configurable timeout | Enabled in the proposed cluster defaults at 30 minutes; `null` explicitly disables it |
| Data-plane protocol | Prefer embedded Raft per tablet; library/runtime integration is a gated implementation decision |
| Control-plane `SetPrimary` alone authorizes a writer | Rejected; a tablet election and recovery establish authority |
| Sequence numbers reset each epoch | Replaced by a logical log index across terms and term/index history |
| External membership or failover service | Excluded |

The proposed data-plane baseline is a proven Raft implementation, with a primary per logical
tablet and batched transport/storage across groups where the library permits it. It need not
be `openraft`: the user's library choice is for the control plane. Assess the data-plane library
against Glommio ownership, storage completion, timer, and message-driving requirements first.
A design spike may propose fewer groups containing several tablets, but must account for coupled
leadership, migration, recovery, and hotspot behavior. Do not silently replace tablet independence.

Raft's ordinary write path replicates to a majority in one communication round; it does not add
a separate election vote to every write. Its log agreement also does not provide transactions
across independent tablets. These are the properties motivating the baseline, not a claim of
measured Shoal performance. [Raft paper, sections 5–6](https://raft.github.io/raft.pdf)

A custom centrally appointed primary protocol is not the fallback for an inconvenient library
API. It would need its own complete election, recovery, reconfiguration, and read specification,
a safety argument, and executable model before replacing this baseline. No external service is
an acceptable workaround.

### Failure model and availability

Assume crash/restart failures, lost, delayed, duplicated and reordered messages, asymmetric
network partitions, process pauses, and disks that report I/O failures. Stable storage honors
successful fsync; hardware that lies about flushes is outside that durability assumption.
Checksums detect accidental corruption; replicas are not Byzantine-tolerant. Clocks may be
unreliable: correctness in the initial protocol must not depend on lease timing.

| Condition | Required behavior |
| --- | --- |
| One failed replica in an established RF=3 tablet | Remaining majority can elect and acknowledge durable writes |
| No tablet majority | No successful quorum writes or strong reads; eligible replicas may serve `One` |
| Control-plane quorum lost, tablet quorum intact | Established tablet groups continue ordinary operations/elections; joins, placement changes, removal and policy changes stop |
| Control plane available, tablet quorum lost | Control plane must not manufacture a replacement authority from stale reports |
| Fewer nodes than configured RF at initial bootstrap | Admin/readiness available, but default writes wait for the intended initial configuration; no implicit RF reduction |
| Capacity insufficient to restore RF | Mark blocked under-replication; retain surviving copies and configuration evidence |
| Entire cluster restarted from durable storage | Recover committed state without inventing empty membership or discarding acknowledged writes |

A failover duration is an objective under a stated healthy-survivor and bounded-delay test
scenario. There is no unconditional time bound during arbitrary partitions or storage stalls.
No cross-tablet transaction, atomic bundle, or common multi-tablet read snapshot is promised.

### Identity and progress

A logical tablet is `(TableId, range_id)`. Initially `range_id` uses the current top twelve hash
bits. `TableId` is stable schema metadata, never process-local enum layout inferred by a peer.
Placement templates may be shared across tables, but their log histories and applied positions
are independent. This avoids one stream spanning separately fsynced table logs without a durable
cross-table ordering mechanism.

| Position | Meaning |
| --- | --- |
| Appended | Accepted into the local replication log; may not survive restart |
| Durable | Contiguous prefix with completed required stable-storage writes |
| Committed | Prefix the consensus protocol guarantees future leaders retain |
| Applied | Committed prefix reflected in the query-visible state |
| Checkpointed | Applied prefix represented in a durably installed checkpoint |

Persist term/vote before replying as required by the selected protocol. Keep term/index history,
configuration identity and checkpoint metadata sufficient for log matching after compaction.
Logical indices do not restart with physical WAL rotation. A restored copy must not claim
progress beyond the state and log it actually recovered. Data receipts never count as durable
acknowledgements until their required storage completions occur.

### Visibility and durability

Default persistent writes require a majority of the committed voter configuration to have fsynced
the record, plus local application before returning the operation's result. A node's local
`Async` setting cannot silently weaken that promise. Configuration changes use the consensus
protocol's transition rules, not a fresh majority computed from the current `Up` list.

Reads at `One` observe an eligible replica's committed, applied prefix. It may lag, including
after the caller receives a successful write response, but never exposes a suffix known only
to be speculative. The initial implementation derives state-dependent mutations in committed
order; pipelining must preserve those semantics without exposing speculative state or
checkpointing it. See Q4 before optimizing this path.

`Write::One` is an optional weaker acknowledgement: local stable append, possibly rolled back
on failover. It cannot promise a committed mutation result or immediate read-your-writes before
commit. The first release may refuse it explicitly until a distinct accepted/pending result API
exists. Replicated ephemeral tables likewise need an explicitly volatile policy; they cannot
satisfy the default stable-storage contract. Neither option may be silently emulated by `Quorum`.

## Alternatives rejected

The earlier heartbeat-max promotion proof assumes current, durable, compatible histories and an
intersecting tablet quorum; heartbeat reports establish none of these. A metadata majority and
a data majority may contain entirely different nodes. Increasing the failure timeout does not
repair this safety gap. External group membership is excluded by the deployment requirement.

## What it costs

Consensus introduces per-group state, scheduling, log matching, and configuration transitions.
The initial spike must measure idle group overhead and batched active throughput. Shared physical
WALs remain possible; independently fsyncing thousands of files is not a prerequisite for logical
groups. Safety mechanisms remain required even if the baseline misses a latency target.

## What it breaks

This page supersedes the first draft's heartbeat-max election, lease-by-recent-contact, fourteen
byte fixed record budget, and map-only replica transition. C1–C12 describe the revised baseline.
No delivered feature or frozen benchmark is changed by this documentation revision.

## Invariants to uphold

- Every acknowledged durable quorum operation remains in every future authoritative history.
- A replica's term/vote, durable acknowledgements, and installed checkpoints survive restart.
- Only a consensus-authorized primary can commit new operations in the active configuration.
- Checkpoints contain committed applied state; deleting WAL segments cannot remove required history.
- Joining, snapshot installation, corruption quarantine and voting eligibility are distinct states.
- Removal cannot lower a write's previously promised durability or bypass a missing data quorum.
- All coordination and election components are embedded in Shoal nodes.

## Prerequisites

The existing storage model and [C11](testing.md)'s pure protocol model. Resolve the blocking
questions below before the named milestone; record the decision and evidence here when resolved.
A preferred answer is a design hypothesis, not evidence that a library already supports it.

## Questions to answer

| ID | Question and preferred direction | Gate and evidence |
| --- | --- | --- |
| Q1 | Which embedded data-plane Raft library can be driven under shard ownership? Evaluate callbacks, durable term/vote handling, batching and idle-group cost; compare grouped tablets only as an explicit alternative | M1 before distributed storage work; executable spike and chosen API/version recorded |
| Q2 | How are logical tablet logs multiplexed into shared physical WALs without lost completion ordering? What alignment, table identity, versioning and checksums does the envelope need? | M4; format specification, restart and rotation tests |
| Q3 | Which checkpoint mechanism provides a stable boundary without long write pauses? Prefer immutable generations or copy-on-write; a bounded pause is a documented initial fallback | M4 design, M7 implementation; crash matrix and pause/memory measurements |
| Q4 | How are conditional mutation results, no-ops and retries derived in committed order while batching? Can `One` use a distinct accepted/pending API? How are volatile-table consensus metadata and full-cluster restart handled? | M4; operation/API matrix and state-machine tests; unsupported policies explicitly refused |
| Q5 | What token format gives session reads a portable committed lower bound across leaders, moves and eventual splits? | M5; lineage validation and expired/unknown-token outcomes |
| Q6 | Can a correct lease optimization beat quorum read barriers enough to justify clock assumptions? | After M6; explicit expiry, revocation and pause model plus measurements; barriers remain default |
| Q7 | What finite auto-removal default and capacity guardrails fit deployments? Proposed 30m; persist grace across control-plane restart, allow null and maintenance suspension | M9b; timed removal, partition healing, insufficient-capacity and maintenance tests |
| Q8 | What initial capacity weights, disk reserve and hotspot thresholds avoid oscillation on unequal hardware? | M9b; heterogeneous placement and stalled-recovery workloads |
| Q9 | What replication retention budget supports catch-up without pinning unbounded shared WALs? | M7; time/byte budgets, slow follower and snapshot starvation tests |
| Q10 | How do clients negotiate schema identity separately from wire capabilities and on-disk format? | M2 contract, M10 release gate; mixed-version operation and rollback tests |
| Q11 | How are node certificates provisioned before first join, identities protected against cloned directories, and address changes authenticated? | M2; join, replacement, duplicate identity and certificate rotation tests |
| Q12 | What checksummed checkpoint or backup is authoritative when replicas disagree? What operator recovery is possible after a majority is permanently lost? | M8/M10; corruption and disaster-recovery exercises, no automatic destructive choice |
| Q13 | What scale targets bound table count, tablet count, connections, map dissemination and control-plane reports? | M1/M3; memory, idle CPU and update-fanout budgets measured at target scale |

## How it would be measured

[C10](performance.md) specifies the control/data-plane spike, fixed-resource overhead tests,
scale-out tests, and failure measurements. Results include completed durable work, latency tails,
and replica lag; acknowledged throughput with growing lag is not sustainable throughput.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `protocol_model_preserves_acknowledged_history` | Deterministic schedules including stale reports, duplicate messages, elections and restart preserve all committed results | M0 |
| `cluster_needs_no_external_coordinator` | Isolated Shoal processes bootstrap, elect and recover using only configured peer connections and their own storage | M3 |
| `metadata_quorum_cannot_replace_a_missing_data_quorum` | A majority of control voters cannot activate a stale tablet minority | M6 |
| `established_tablets_survive_control_quorum_loss` | Existing data groups progress where their own quorum is healthy; all membership mutations stop | M6 |

## Related

[C3](membership.md), [C5](replication.md), [C7](failover.md), [C11](testing.md), and
[Milestones](milestones.md) implement and test this contract. The
[implementation reading list](prior-art.md#implementation-reading-list) maps primary sources,
library APIs and Linux persistence contracts to the gates that require them.
