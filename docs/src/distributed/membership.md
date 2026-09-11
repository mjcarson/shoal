# C3. Membership and failure detection

## Context

Membership is a durable decision; reachability is an observation that can differ between peers.
Shoal runs both mechanisms itself. An embedded `openraft` control-plane group owns cluster
membership and desired placement. Each tablet's embedded data group owns its election and log
configuration. No external membership, configuration or failover service is required.

## What exists today

`Shard::join_cluster` broadcasts a local join and `Ring::add` ignores unknown shards. Reserved
ping/pong frames have no implementation. ~~The pool lacks a dependable readiness/failure handle.~~
`ShoalPool::ready` and `failure` are that handle since
[F36](../features/cluster-harness.md), for the process's own shards. No consensus library is
currently in the workspace. [C1](node-identity.md) introduces identity and the control-plane
thread; ~~M0 first adds readiness and failure propagation~~ M0 added them.

## The design

### One Raft group of nodes

The control-plane state machine holds ClusterId, member identities/endpoints, policy versions,
placement intent, transition records, and removal tombstones. It stores its own WAL and snapshots
under the node's latency-sensitive storage path. Persistence is independent of application-table
compaction and must recover after abrupt power loss under C13's storage assumptions.

Prefer three control voters initially; permit an explicit five-voter policy for larger clusters.
Other data nodes are learners of this metadata group. Adding a fourth data node does not silently
change the control quorum to four. Membership transitions use the library's supported API,
including catch-up before promotion. Replace a lost voter only while the old configuration has
quorum; never force promotion using a detector verdict. Spread voters over configured failure
domains where available. Define placement feasibility and alert if domains are insufficient.

Control voters and tablet voters are separate populations. An RF=3 tablet may live entirely on
metadata learners. Reports are telemetry, not election votes for that tablet.

### Joining

An explicitly bootstrapped node creates the cluster once and persists its identity. Other nodes
join through configured seed addresses, verify cluster/schema/protocol/authentication, receive
control state as learners, and become members without automatically becoming tablet voters.
Their data readiness advances only through snapshot/catch-up and data configuration transitions.
An interrupted join is resumable by identity, with a recorded transition id and timeout policy.

An existing cluster directory never auto-bootstraps a new group because seeds fail to answer.
Duplicate processes using the same NodeId are rejected/fenced using authenticated identity and
incarnation rules settled in Q11. Local directory locking alone does not protect cloned disks
on two machines. Address changes update advertised peer/client endpoints through membership.

### The state machine of a member

| State | Meaning |
| --- | --- |
| Joining | Accepted identity; receiving metadata and not eligible as a data placement target yet |
| Up | Node control plane and required shard health known; per-tablet readiness still checked |
| Down | Sustained unreachability recorded; existing replica assignments retained during grace |
| Leaving | Graceful drain requested; no new placement onto the node |
| Removing | Grace elapsed or operator requested replacement; recovery transitions in progress |
| Removed | Identity tombstoned; old data cannot rejoin as an authoritative member |

`Unreachable` is a local observation, not a durable membership change. Membership state never
changes a data quorum threshold. `Down` does not delete copies or automatically change voters.
A node can return from Down after identity and shard health checks, while individual tablets
remain in recovery. Removing cannot be reversed by a late heartbeat; cancellation, if supported,
is an explicit versioned operation that reconciles all already committed data transitions.

### Failure detection

Use bounded peer probes over a control traffic lane, independent of bulk snapshot streams.
Phi-accrual is a candidate policy with configurable probe interval, sample window, minimum
samples and threshold. Phi is a suspicion score derived from an estimated arrival distribution,
not a literal probability that a node is dead and not a deterministic `interval × phi` deadline.

Node reports include freshness/sequence and incarnation. Prefer a dedicated `StatusReport` frame
rather than assuming arbitrary application fields can be attached to OpenRaft heartbeat replies.
Bound/coalesce per-tablet progress reports; they do not prove current durable state for promotion.
A leader commits Down according to the configured fresh-evidence policy; M3 specifies the required
reporter set and behavior when reports are missing. A metadata majority is required to commit
that decision regardless of the detector policy. Test partitions among control voters and data
learners separately.

Data-shard heartbeats and task failures feed node health. Healthy control pings must not mask a
dead shard or stalled data socket. Tablet elections and read barriers use their own protocol
traffic, so they can progress without waiting for a global Down verdict.

### What follows from Down, and when

Tablet elections may change primaries without moving data. The `primary_failover_after` base
configures data-election timing (C7); it is not added to a second post-Down failover sleep.

`auto_remove_after` defaults to a proposed 30 minutes, matching the requested automatic-removal
policy; `null` explicitly disables it. A persisted Down episode records the grace state, policy
version and removal operation id. Control-leader changes must not erase or accidentally restart
an elapsed grace. Q7 specifies elapsed-time accounting across restart and clock discontinuities:
when elapsed time cannot be established, delay removal conservatively rather than guess early.
Allow explicit maintenance suspension/resumption with an observable deadline.

On expiry, propose Removing and let C8 rebuild copies from surviving authoritative groups.
Insufficient data quorum, disk space, distinct destination nodes or failure domains leaves a
visible blocked operation. Do not shrink RF, erase the only remaining copy, or mark the node fully
Removed merely to make the timer complete. A three-node RF=3 cluster needs a replacement node
to restore three distinct copies after one machine is permanently lost.

### openraft and the runtime

Use a current-thread Tokio runtime on the reserved control core as the initial integration.
Run blocking filesystem work through an appropriate asynchronous/blocking adapter; a synchronous
fsync must not freeze all election timers on that runtime. Returning from an append or vote
operation must follow the library's durability contract, not merely enqueue work.

Implement the library's network adapter over Shoal's control traffic framing. The adapter must
preserve request identity, deadlines and shutdown behavior. It owns no external service; local
Tokio sockets or shard relays are implementation choices. Prefer direct control-plane socket
ownership so a stalled Glommio data shard cannot stall control elections. Peer transport has
separate control/data endpoints or a validated dispatch design; Q1/M2 settles the wiring.

The original source note named 0.9.25 and an alpha 0.10 alternative; C13's
[decision record](protocol.md#decision-record) reads both (0.9.25 and 0.10.0-alpha.34 on
2026-09-11) as data-plane candidates and pins neither for the control plane. Before implementation, pin an
actual version and record source/API evidence for runtime behavior, storage completions, learner
membership changes and network driving. Do not assume an alpha API or heartbeat extension is
available. No handwritten-Raft fallback is planned.

## Alternatives rejected

Gossip alone does not establish authoritative membership. A failure detector is not a consensus
protocol. Conversely, consensus does not turn reachability into objective truth. The initial
all-members-majority detector and first-five-automatically-vote rules are superseded by an explicit
control voter policy and defined evidence freshness. Auto-removal is enabled, guarded and observable,
rather than disabled despite the selected policy.

## What it costs

One embedded metadata group, control probes/reports, a reserved execution core and durable metadata
storage. All-pairs probing grows quadratically; Q13 must establish the intended node-count budget
and change probe/report topology if necessary. Control work is isolated from ordinary queries,
but still shares hardware unless explicitly provisioned separately.

## What it breaks

The local join seam, startup readiness, marker metadata and endpoint configuration change. A
node's Up state no longer means every tablet is ready. The pool reports shard failure and the
control plane reflects it without fabricating global data eligibility.

## Invariants to uphold

- All membership decisions are committed by the embedded control-plane group.
- Failure suspicion cannot create a voting majority or lower a data quorum.
- Down retains assignments during grace; Removing starts durable, capacity-checked transitions.
- Control quorum loss freezes metadata mutations, not independent healthy tablet elections.
- Rejoin, voter promotion and data readiness have distinct checks.

## Prerequisites

[C1](node-identity.md), [C2](transport.md), [C13](protocol.md) Q1/Q7/Q11/Q13 and M0 readiness.

## How it would be measured

Idle/control CPU, report bytes, election latency under bulk transfer and node-count scaling in
[C10](performance.md). Count blocking-I/O delay and control responsiveness during data-shard stalls.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `three_nodes_bootstrap_without_external_membership` | Shoal-only processes converge on one cluster and recover metadata after restart | M3 |
| `fourth_data_node_does_not_change_control_voter_count` | Joins as metadata learner under the three-voter policy | M3 |
| `minority_cannot_commit_membership_changes` | Isolated control minority cannot remove majority peers or promote replacement voters | M3 |
| `fresh_failure_reports_do_not_mask_shard_failure` | Old incarnation/status reports are ignored and data-shard failure is observable | M3 |
| `lost_seeds_do_not_rebootstrap_existing_directory` | Restart with unreachable seeds preserves established cluster identity and log | M3 |
| `down_retains_placement_during_grace` | Elections may change primaries, but no replica moves before the configured removal action | M6 |
| `removal_grace_survives_control_leader_restart` | Restart/failover preserves the episode, without an early or forgotten removal | M9b |
| `maintenance_suspends_automatic_removal` | Explicit suspension prevents removal, and resumption has a documented remaining deadline | M9b |

## Related and implementation references

- [C4](tablet-map.md), [C7](failover.md), [C8](rebalancing.md), [C13](protocol.md).
- [OpenRaft integration guide](https://docs.rs/openraft/latest/openraft/docs/getting_started/index.html): application-owned network and storage adapters; run its storage conformance suite in addition to Shoal crash tests.
- [OpenRaft log storage contract](https://docs.rs/openraft/latest/openraft/storage/trait.RaftLogStorage.html): vote persistence, append completion, truncation and purging.
- [OpenRaft source](https://github.com/databendlabs/openraft): pin source corresponding to the selected release and link the exact runtime/membership implementation in Q1's decision record.
