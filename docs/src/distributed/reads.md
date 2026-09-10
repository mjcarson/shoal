# C6. Reads and consistency levels

## Context

**A default `One` read may not see a successful `Quorum` write yet.** It observes one eligible
replica's committed applied prefix. Stronger reads establish current authority through the data
protocol; an asynchronously pushed map and recent control-plane contact do not establish it.
This meets R4 and R6 without claiming cross-tablet transactions or a common query snapshot.

## What exists today

`route_archived` splits queries across owning shards. `Shard::handle_gathered` merges responses,
restores partition order and applies the final limit. Local shares can carry open response
objects; remote shares will need validated decoding. Bundles contain multiple independent
queries, and existing gather state lacks expiry ([item 33](../appendix/known-issues.md#33-collected-split-query-state-has-no-expiry)).

## The design

### Three read levels

| Level | Behavior | Promise |
| --- | --- | --- |
| `One`, default | Select an eligible local replica if possible, otherwise another reachable replica | Its installed committed applied state, possibly stale |
| `Primary` | Route to the current leader, obtain the protocol's read barrier and wait for application through that barrier | Single-tablet linearizable read |
| `Quorum` | Obtain a current data-quorum barrier, then read an eligible replica applied through it | Same single-tablet freshness; alternate execution/routing policy, not a merge of speculative rows |

Q1/Q5 must decide whether `Primary` and `Quorum` warrant two public names or should share one
strong-read implementation. Until then the distinction is routing only; neither can promise
freshness based on a cached leader address. Neither uses the control-plane Raft log per read.
A strong read proves authority after invocation, chooses a safe committed position, waits for
local application, and reads a consistent tablet view. A leader change during the operation
must obey the chosen protocol's read-barrier contract.

The original `Primary` lease-by-recent-contact and “maximum tuple wins” quorum merge are
superseded. A delayed contact can arrive after replacement; a higher tuple can describe an
incompatible uncommitted history. Initial strong reads pay for a barrier. Lease optimization is
C13 Q6, gated by explicit clock, expiry, revocation and process-pause assumptions.

### Session tokens

A successful committed write may return a token identifying the cluster, logical tablet/history
and committed lower bound. A session read supplies that token and may execute on any eligible
replica that proves it has applied that committed history. Otherwise it waits within the deadline
or forwards. It need not always contact the primary, but it must validate lineage after moves,
elections and future splits. A token is not a cross-tablet transaction timestamp. A bundle
needing several tablet lower bounds carries several tokens with a bounded size.

Define behavior for unknown, expired or wrong-cluster tokens; never silently ignore them. A
locally appended `Write::One` record does not produce a committed token before commitment.

### Choosing a replica

Separate local reachability from authority and storage readiness. Exclude installing, corrupt,
unreconciled and removed copies at every read level. A stale but complete committed replica may
answer `One` even if it cannot reach a majority; returning speculative/conflicting state is not
allowed. Prefer local, then measured low latency with bounded load awareness. Node-level `Up`
is not sufficient evidence of tablet readiness, and a cached `Up` cannot prevent in-flight errors.

A node at RF=3 on three nodes holds a replica of every tablet, but a request generally still
crosses to its owning local shard. “Local node” does not imply the accepting shard owns the row.
On more nodes than RF, local replicas are no longer guaranteed.

### Fan-out across nodes

Partition each query by table-qualified tablet. Each share names its coverage, original query
index, execution attempt and routing version. Replies include explicit covered partitions and
snapshot/applied-position metadata even when no rows match. A deletion or an empty filtered
result must override an older nonempty answer if an optional future reconciliation path is used.
Do not infer “no data” from a missing reply.

The initial strong path uses one authoritative answer per tablet after its barrier, avoiding
row reconciliation entirely. Across tablets there is no common snapshot: state can be observed
at different instants. Document this for limits, projections, filters and concurrent mutations.
Merge in requested partition order and apply the final limit afterward. Any per-share limit
pushdown must be proved equivalent for that query shape. Pagination tokens must preserve the
stated ordering/consistency contract or explicitly permit changes between pages.

A bundle is not atomic. Each query index receives one complete result or one structured error;
do not return a successful partial row set for a query missing one of its required shares.
Other query indices may succeed independently.

### Deadlines on a gather

Carry an end-to-end deadline budget through forwarding, retries, barrier waits and gathering.
Do not reset it at every hop or compare absolute clocks on different machines. On expiry release
gather state, cancel outstanding read work where possible, and emit one error for that query.
Late and duplicate replies are ignored by attempt identity after completion. A bounded read
retry can reroute within the original budget; an accepted write is never replayed this way
without C5's stable operation identity.

### The per-bundle override

A bundle may override read and write policy; absent values fall back to each query's table policy,
then the cluster default. A bundle can contain several tables, so resolution is per routed query
or homogeneous sub-bundle, not once for the entire mixed bundle. The coordinator forwards the
resolved policy and servers validate compatibility rather than reinterpreting local defaults.
Cluster/table defaults are versioned control-plane state, not divergent per-node YAML decisions.

Keep wire types in `shoal-proto`, independent of the engine. Cap token/metadata size and version
these additions via C2's compatibility scheme. The old protocol version is not assumed to decode
a widened rkyv struct simply because its handshake was accepted.

## Alternatives rejected

Control-plane-contact leases and choosing rows by maximum reported progress are superseded.
A session token is useful for read-your-writes without making every read linearizable. If a later
quorum fetch optimization is proposed, it must preserve negative-result metadata and the read
barrier, and benchmark its traffic against selecting one eligible replica.

## What it costs

`One` adds eligibility/routing checks and sometimes a hop. Strong reads add barrier traffic and
application wait; leader-directed does not mean one-hop-only latency. Session reads can avoid
a new barrier once the replica validates a committed lower bound. Remote gather decoding,
coverage metadata, deadlines and retries consume bounded CPU/memory and are measured separately.

## What it breaks

The wire gains policies, tokens, request-attempt identity and deadline budgets. Gathers terminate
instead of leaking. Strong-read guarantees replace the draft's informal lease, and `One` reads
cannot expose apply-before-commit state from the original local write path.

## Invariants to uphold

- `One` reads only complete, eligible committed state; staleness is not partial installation.
- Strong reads require current data-protocol authority and application through the barrier.
- Empty results carry coverage when needed; absent shares never masquerade as successful emptiness.
- Every query index completes once, with complete rows or an error, within its budget.
- Table policy is resolved correctly for mixed bundles; no cross-tablet atomicity is implied.

## Prerequisites

[C5](replication.md), [C7](failover.md), [C2](transport.md), C13 Q5/Q6.
M5 builds the read paths; M6 validates them through leadership changes before claiming HA.

## How it would be measured

[C10](performance.md) read-only arms compare local `One`, remote `One`, barrier reads and session
reads, including lagging replicas, filters/limits and cross-node fan-out. Measure tails and
barrier/application wait separately; do not assume strong reads cost only a routing hop.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `one_reads_converge_without_exposing_uncommitted_state` | Paused follower yields stale committed reads, then converges; speculative mutations never leak | M4 |
| `barrier_read_observes_prior_quorum_write` | A different coordinator reads committed state after the write response | M5 |
| `session_read_waits_for_committed_lower_bound` | Behind replica waits/forwards; token works after leader change and refuses wrong lineage | M6 |
| `empty_and_deleted_partitions_have_explicit_coverage` | Filtering/deletion cannot resurrect an older row through missing metadata | M5 |
| `limits_apply_after_complete_ordered_gather` | Multi-tablet results preserve partition order and limit without silently dropping required rows | M5 |
| `gather_timeout_completes_once_and_discards_late_replies` | Timeout, retry and duplicate shares release state and produce one result/error per query | M5 |
| `mixed_table_bundle_resolves_each_table_policy` | Different table defaults resolve independently unless explicitly overridden | M5 |
| `read_barrier_survives_leader_change_and_delayed_messages` | Pauses, old leader replies and late control-plane contact cannot authorize stale strong reads | M6 |

## Related

[C5](replication.md), [C7](failover.md), [C13](protocol.md),
[Request lifecycle](../architecture/request-lifecycle.md).
For the data-plane candidate's read-index entry point and application integration inspect
[raft-rs RawNode read_index](https://docs.rs/raft/latest/raft/raw_node/struct.RawNode.html#method.read_index).
Q1 must document how the selected library establishes and returns the barrier before this API
is implemented; a method name alone is not a read-safety proof.
