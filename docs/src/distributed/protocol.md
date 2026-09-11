# C13. Protocol decisions, failure model, and open questions

## Context

The first draft appointed primaries from heartbeat progress and treated publishing a new epoch
as fencing. That does not establish which writes a new primary must preserve. This revision
makes the safety protocol a prerequisite for implementing replication, recovery, or migration.
Nothing in this chapter is built. [The contract](#the-contract) below was agreed on 2026-09-11 as
the Before-M0 gate and binds every later milestone; the [decision record](#decision-record) holds
its evidence, and says what that gate did not decide.

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
| One primary orders a tablet's mutations | Agreed 2026-09-11; a tablet is qualified by table identity ([P2](#the-contract)) |
| Embedded `openraft` for cluster membership and placement | Agreed 2026-09-11; reserved core defaults to CPU 0 but is configurable. The version is pinned by M1, not here |
| `Quorum` writes and `One` reads by default | Agreed 2026-09-11; durable quorum ([P3](#the-contract)) and committed-prefix reads ([P4](#the-contract)) are defined below |
| A down node keeps placement during a grace period | Agreed 2026-09-11; primary elections do not copy tablets |
| Automatic removal after a configurable timeout | Enabled in the proposed cluster defaults at 30 minutes; `null` explicitly disables it. The value stays open under Q7 |
| Data-plane protocol | ~~Prefer embedded Raft per tablet; library/runtime integration is a gated implementation decision~~ Raft per logical tablet, agreed 2026-09-11 ([P5](#the-contract)). Which library and runtime is Q1, still open, and M1's spike decides it |
| Control-plane `SetPrimary` alone authorizes a writer | Rejected; a tablet election and recovery establish authority ([P5](#the-contract)) |
| Sequence numbers reset each epoch | Replaced by a logical log index across terms and term/index history ([P2](#the-contract)) |
| External membership or failover service | Excluded ([R7](overview.md#what-is-being-asked-for)) |

The data-plane baseline is a proven Raft implementation, with a primary per logical
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

### The contract

The [Before-M0 gate](milestones.md#before-m0-the-protocol-contract) names six clauses. They are
numbered here so that a test, a page or a review can name one without quoting it; `P` numbers
are never reused, the rule `C`, `M` and `Q` numbers already follow. Each clause is written as a
property that can be checked against a history, next to the schedule that would violate it,
because [C11](testing.md)'s model checks properties rather than prose. The M0 test
`protocol_model_preserves_acknowledged_history` names the `P` number each of its checks enforces.

| # | Property | Binds | The violation the M0 model must reject | Owning tests |
| --- | --- | --- | --- | --- |
| P1 | **Failure model.** Correctness holds under crash/restart, lost, delayed, duplicated and reordered messages, asymmetric partitions, process pauses and reported I/O failures, with stable storage meaning a successful fsync. It never depends on clocks, leases or a non-Byzantine replica behaving well. The availability table below is part of this clause | C2, C3, C7, C11; every milestone from M0 | A paused old primary, a duplicated acknowledgement or a reordered append that changes which operations are in the authoritative history; any check that needs synchronized clocks | `protocol_model_preserves_acknowledged_history` (M0) |
| P2 | **Table-qualified stream identity.** The unit of replication, election and progress is `(TableId, range_id)`. Its log index is logical and continuous across terms and physical WAL rotation, and `TableId` is stable schema metadata, never a peer's enum layout | C4, C5, C7; M3, M4 | Two tables sharing one index sequence; an index that restarts at rotation; a peer that infers a table from a position | `table_ids_and_streams_are_stable_across_restart` (M3), `table_streams_recover_independently_without_holes` (M4) |
| P3 | **Durable quorum.** A default write succeeds only after a majority of the *committed voter configuration* has fsynced the record and the primary has applied it. A replica counts once, only with matching durable history; `Up`/`Down`, a local `Async` setting and learners never change the threshold | C3, C5; M4, M6 | A quorum computed from the current `Up` list; a repeated cumulative ack counted as a second voter; an `Async` receipt counted as durable | `quorum_success_requires_distinct_durable_voters` (M4), `async_replica_cannot_weaken_durable_quorum` (M4) |
| P4 | **Committed visibility.** A `One` read returns an eligible replica's committed, applied prefix, possibly stale, and never an appended-but-uncommitted suffix. Checkpoints hold only committed applied state, and the result of a state-dependent mutation is derived in committed order | C5, C6, C7; M4, M5 | A read or checkpoint that observes an entry a later leader truncates; a conditional result computed before its command commits | `one_reads_converge_without_exposing_uncommitted_state` (M4), `uncommitted_suffix_never_enters_checkpoint` (M4) |
| P5 | **Control/data authority split.** Membership, placement intent and transition records are committed by the embedded control group; a tablet's writer is established only by that tablet's own consensus election and recovery. A control majority cannot activate a data minority, and losing control quorum stops metadata mutation, not established tablet groups | C3, C4, C7; M3, M6 | The B=100 / C=101 / A+B=102 schedule from [C7](failover.md#when-a-primary-is-down): choosing C from cached reports discards B's acknowledged 102. Any promotion whose only evidence is a topology edit | `metadata_quorum_cannot_replace_a_missing_data_quorum` (M6), `established_tablets_survive_control_quorum_loss` (M6), `stale_heartbeat_reports_cannot_lose_acked_write` (M6) |
| P6 | **No cross-tablet transaction promise.** Nothing promises atomicity across tablets, an atomic bundle, or a common multi-tablet read snapshot. A bundle's queries complete independently, each with one complete result or one error, and partial outcomes stay visible | C5, C6; M0 oracle scope, M5 | An oracle, API or test that treats a bundle as atomic or reads two tablets at one instant | `limits_apply_after_complete_ordered_gather` (M5), `mixed_table_bundle_resolves_each_table_policy` (M5); M0's oracle checks single-tablet histories only, which is P6 applied to the oracle |

What the gate settled about the protocol is P1–P6 and that the data protocol is Raft. What it
left open is which library and runtime drive it, which is Q1. A custom protocol cannot pass this
gate by calling primary appointment a topology edit: that is P5 restated, and it is the reason
the first draft's heartbeat-max election is in [Alternatives rejected](#alternatives-rejected).

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

### Decision record

Recorded 2026-09-11 at the Before-M0 gate, on the tree at `8354e4a`, the commit before this
record. Each row says how the claim was checked, in the manner of the
[August review](../appendix/review-2026-08.md), so the next reader can skip what is verified.
Crate facts were read from the sources cargo fetched into the local registry, at the path and
line given for that release; `docs.rs` and default branches were not the source.

| Decision | Evidence |
| --- | --- |
| P1–P6 are the contract | The six clauses of the Before-M0 gate, mapped [above](#the-contract) one to one onto the pages that inherit each and the test that owns it. No clause was dropped, merged or added |
| Raft is the data-plane protocol; the control plane stays `openraft` | The two properties cited above from the Raft paper, one round to a majority per write and no agreement across independent logs, are what the baseline needs and what a centrally appointed primary lacks. A custom protocol is not the fallback: it would need its own election, recovery, reconfiguration and read specification plus an executable model before it could replace this baseline |
| Q1 is **not** settled: no library or runtime is selected | Selecting one needs M1's spike numbers, idle memory and CPU per group at 4096 groups per table, message batching across groups, durable term/vote before a reply, a read barrier, and election timing under Glommio ownership. None exist. The pins below are candidates for that spike to read, not a choice |
| Candidate: `openraft` `0.10.0-alpha.34`, the latest release; `0.9.25`, the latest stable | `cargo info openraft` and `cargo info openraft@0.9`, 2026-09-11. The `single-threaded` feature makes `OptionalSend`/`OptionalSync` empty bounds (`Cargo.toml:72-77`, `src/base/mod.rs:8,43`), so a `!Send` adapter owned by a Glommio shard is admissible in principle; 0.9 spells the feature `singlethreaded`. Storage is an async seam: `save_vote` (`src/storage/v2/raft_log_storage.rs:63`), `append(entries, IOFlushed)` (`:128`), `truncate_after` (`:141`), `purge` (`:148`) and `RaftStateMachine::apply` (`raft_state_machine.rs:98`). `RaftTypeConfig` requires an `AsyncRuntime` (`src/type_config.rs:84-99`), and the only one shipped is Tokio behind the default `tokio-rt` feature (`Cargo.toml:78,144-147`); no Glommio runtime exists, so the spike either writes one or drives `openraft` on a per-shard current-thread Tokio runtime. Default features also pull `clap` |
| Candidate: `raft` (raft-rs) `0.7.0` | `cargo info raft`, 2026-09-11. `RawNode` "is a thread-unsafe Node" by design (`src/raw_node.rs:284-286`): no runtime, no `Send`, driven by `tick`, `step`, `propose`, `ready` and `advance`. Persistence completion is separated from stepping by `advance_append_async` and `on_persist_ready` (`:697`, `:617`), which is the shape a DMA-completion-driven shard needs, and `read_index` (`:764`) is the read-barrier entry point. `Storage` is a synchronous trait of six methods, `initial_state`, `entries`, `term`, `first_index`, `last_index` and `snapshot` (`src/storage.rs:106-166`). Dependencies are `protobuf 2`, `raft-proto`, `slog`, `rand 0.8`, `fxhash`, `getset` and `thiserror` (`Cargo.toml:45-87`); messages are protobuf, so a Shoal command would ride as an opaque `data` field |
| What the spike inherits | rustc 1.100.0-nightly (2026-09-04). Glommio is the `../glommio` path dependency at 0.10.0, not the crates.io release, so a runtime adapter targets that fork. No consensus crate is in `Cargo.lock` |

**What this gate did not do.** It selected no library or runtime, wrote no types, added no
dependency, and measured nothing. A version above is a pin for the spike to start from, not a
selection, and nothing on the [milestones page](milestones.md) moved except the gate itself.

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
questions below before the named milestone; record the decision and evidence in the
[decision record](#decision-record) when resolved, as the Before-M0 rows do.
A preferred answer is a design hypothesis, not evidence that a library already supports it.

## Questions to answer

| ID | Question and preferred direction | Gate and evidence |
| --- | --- | --- |
| Q1 | Which embedded data-plane Raft library can be driven under shard ownership? Evaluate callbacks, durable term/vote handling, batching and idle-group cost; compare grouped tablets only as an explicit alternative. **Decided 2026-09-11:** the protocol is Raft and the candidates are pinned in the [decision record](#decision-record). **Open:** which library and runtime | M1 before distributed storage work; executable spike and chosen API/version recorded |
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
| `protocol_model_preserves_acknowledged_history` | Deterministic schedules including stale reports, duplicate messages, elections and restart preserve all committed results; each check names the [`P` number](#the-contract) it enforces | M0 |
| `cluster_needs_no_external_coordinator` | Isolated Shoal processes bootstrap, elect and recover using only configured peer connections and their own storage | M3 |
| `metadata_quorum_cannot_replace_a_missing_data_quorum` | A majority of control voters cannot activate a stale tablet minority | M6 |
| `established_tablets_survive_control_quorum_loss` | Existing data groups progress where their own quorum is healthy; all membership mutations stop | M6 |

## Related

[C3](membership.md), [C5](replication.md), [C7](failover.md), [C11](testing.md), and
[Milestones](milestones.md) implement and test this contract. The
[implementation reading list](prior-art.md#implementation-reading-list) maps primary sources,
library APIs and Linux persistence contracts to the gates that require them.
