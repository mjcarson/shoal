# C5. Replication and the write path

## Context

Default writes wait for a durable quorum, while default reads may lag. A primary orders each
tablet's noncommutative mutations. The protocol must preserve successful operations through
leader changes, reconfiguration, restart and compaction; an acknowledgement counter alone does
not establish this. [C13](protocol.md) is the protocol contract and decision gate.

## What exists today

`FileSystem::commit` (`shoal-core/src/server/tables/storage/fs.rs`) serializes an intent,
checksums it and stages it into a per-shard, per-table WAL. The table applies a mutation in
memory and parks its result in `PendingResponse`; local durability later releases it.
`DataFlushed` wakes the sweep, and rotation currently drains the queue because the old WAL was
fsynced. The [compactor](../storage/compaction.md) merges intents into archives and deletes logs.

`NoStorage::commit` (`storage/none.rs`) only advances a watermark. It does not serialize an
intent. The persistent and ephemeral tables share table implementations, but their storage
hooks cannot already supply the same replication bytes. Updates carry changed fields and require
an ordered base state; insert/delete results can also depend on existing state.

## The design

### Why a primary

Keep one primary per `(TableId, range_id)` tablet. Replicas apply the same committed commands in
the same order. Leaderless last-writer-wins partial updates would require a different conflict
model and storage metadata; the user chose ordered primary replication.

The preferred implementation is an embedded Raft group per tablet, with library and runtime
selection gated by C13 Q1. Cluster membership remains embedded `openraft`; it does not elect
individual tablet leaders by comparing heartbeat reports. A consensus library defines the
append, election and configuration rules, and Shoal's storage adapter must honor them.

### The primary stamps

Each record has stable table/range identity, logical log index, leadership term and record kind.
Configuration entries carry configuration identity, and mutation entries carry request identity.
Indices continue across terms and physical log rotations. Persist term/vote before sending the
responses the chosen protocol requires. Compare log history using that protocol's matching
rules, not a highest tuple observed asynchronously by the control plane.

Track appended, durable, committed, applied and checkpointed prefixes separately. Per-replica
progress is monotonic within the corresponding history/configuration. A higher term is not
permission to claim that all earlier data has been installed.

### The intent record, format 2

The earlier fourteen-byte header is superseded by a versioned replication envelope. Its exact
encoding is Q2, to be settled before M4. Required information includes table/range identity,
term/index, entry kind, request identity where applicable, payload size and checksum. Configuration
and snapshot-boundary records have typed payloads. Define byte order, length bounds, rkyv alignment
and checked decoding; do not cast a payload at an arbitrary fourteen-byte offset into an archive.

Keep logical logs independent while multiplexing physical writes where appropriate. A durability
completion identifies the physical segment/generation and covered offsets; an index maps those
completions to each logical stream's contiguous durable prefix. Never reuse a physical offset
without its generation. A shared WAL is retained until every dependent stream has a safe
checkpoint or retained history elsewhere under the specified recovery policy.

The storage marker and WAL/archive formats are versioned separately. A supported offline
migration/export path and refusal behavior for unsupported formats are documented before release;
starting an existing database empty is not an upgrade plan.

### The bytes are forwarded, not re-serialized

Move replication encoding to a common mutation boundary above `FileSystem` and `NoStorage`.
Serialize a command once, feed the immutable payload to local WAL staging and peer transport,
and validate it on arrival. Applying a command can require decoding fields; reusing bytes does
not imply zero deserialization in the state machine.

Existing intents may need to become deterministic commands rather than the result of mutating
local state before commitment. In the initial design, apply commands in committed order and
compute the response there, including no-op/conditional results. A retry of a committed command
returns its original result. Any speculative execution or pipeline must specify dependencies,
rollback and separate committed visibility in Q4 before being used as an optimization.

Batch messages across active groups with byte and time bounds; light load must not wait forever
for a full buffer. Disk group commit and network batching are separate queues with separate
completion signals. Benchmark the common path on both storage backends.

### The quorum gate

The default successful mutation response requires consensus commitment backed by a majority's
fsync completions, plus local application to derive its result. `All` additionally waits for
all voters in the operation's defined configuration; it is not reinterpreted when a node is
marked down. During reconfiguration follow the selected protocol and capture the operation's
required configuration(s), rather than changing a parked request's threshold retroactively.

Track `durable_match[replica]` and derive quorum progress from distinct voter identities. A
repeated cumulative ack or `DataFlushed` event updates a watermark; it does not add a vote.
Local durability counts once. Learners, installing copies and replicas from unrelated terms or
configurations do not count. Do not implement a second, weaker commit rule beside the library.

A preflight liveness check may reject a request known to be unavailable. It cannot guarantee
that a request accepted for processing will reach a quorum: peers can fail immediately after
the check. Deadlines and response loss have explicit outcomes below. `Up`/`Down` never changes
the size of a quorum. Initial bootstrap does not serve default writes until the intended RF
configuration is ready. RF=3 subsequently tolerates one failed member without shrinking RF.

**Rotation no longer drains responses.** It advances only the covered local durable positions.
Keep pending requests keyed by logical tablet/index and their local WAL generation. Wake the
response machinery after every relevant durability, commitment and application advance. Isolate
pending queues by tablet so one stalled tablet does not block unrelated successful operations.

### Followers apply in order

Use the selected protocol's append matching, conflict rejection, duplicate handling and retry
rules. A duplicate matching entry is acknowledged without reapplying it; a gap requests retained
history or a snapshot. Buffer limits apply to gaps, future terms and reconnects. Persist a higher
term before responding as required. A stale topology may delay routing, but cannot override the
replication group's authority.

Apply only committed prefixes to query-visible state. Compaction checkpoints only applied,
committed state. Retain the checkpoint's last term/index and configuration plus enough log
history for matching and catch-up. Recovery may truncate an uncommitted WAL suffix; it never
tries to undo such a suffix after merging it into an authoritative archive.

### What the client is promised

| Policy/outcome | Contract |
| --- | --- |
| `Quorum` success, default | Durable committed mutation and its original result; survives loss of a minority in the established configuration |
| `All` success | Same, with all required voters durably acknowledging; no silent downgrade |
| `One` | Optional local durable append acknowledgement, potentially rolled back; needs a distinct accepted/pending API, otherwise refused in v1 |
| Volatile replicated table | Explicit memory-only policy; no full-cluster restart durability claim; never counted as stable-storage quorum |
| Definitely rejected | Admission/routing failed before accepting the command; safe to retry with its identity |
| Outcome unknown | Command may have committed; retry only with the same identity or query its result |

A disk configured `Async` cannot satisfy a required fsync acknowledgement. Validate cluster/table
policy compatibility on admission and refuse unsupported combinations. Document the physical
failure assumptions from C13 rather than calling replication alone durability.

### Retry identity

Before application-ready failover, support a stable client/session identity plus operation
sequence or an equivalent unique id, scoped to a tablet command. Carry it across redirects,
reconnects and coordinator changes. Replicate the payload digest and original result with the
state-machine effect. Reject reuse with a different payload. Replaying an already committed
identity must not reapply it, even if its first response never reached the client.

Checkpoint deduplication state with the data; migrate and restore it. Bound retention explicitly:
prefer a durable session low-water mark and reject expired requests instead of silently executing
them as new. Client cancellation does not roll back an accepted command. A bundle carries stable
per-operation/sub-operation identities; no atomicity across tablets is implied, and partial
outcomes remain visible to the caller.

## Alternatives rejected

The original ack counter can count the same replica repeatedly. The original `Unavailable`
precheck cannot distinguish a lost response from an uncommitted request. Both are replaced above.
Per-tablet Raft is no longer rejected on the assumption of a second vote round per write.
A custom protocol must pass C13's design gate, not be introduced to avoid an adapter.

Physical WAL sharing and logical tablet independence are compatible. A separate duplicate WAL
is not mandatory, but adapting today's WAL must supply all consensus persistence guarantees.
The earlier unconditional requirement to preserve today's apply-before-ack path is superseded
where it exposes or checkpoints uncommitted data.

## What it costs

Replication adds network traffic, per-group state, and durable follower work. For a design that
requires local fsync plus one of two follower fsyncs at RF=3, healthy write latency is approximately
`max(local_sync, min(follower_B_roundtrip_and_sync, follower_C_roundtrip_and_sync))`, plus routing,
queuing and apply time. Other quorum completion rules must be measured according to their actual
implementation. Every follower still needs sustainable apply/storage capacity even when omitted
from the fastest quorum. Record pending bytes, lag and catch-up debt alongside latency.

## What it breaks

WAL format, `StorageSupport::commit`, mutation execution, `PendingResponse`, rotation, recovery
and compaction contracts all change. Client errors gain explicit ambiguous outcomes and retry
identities. The wire format and table identity must remain usable by client-only builds.

## Invariants to uphold

- A replica counts once, only for durable matching data in the required configuration.
- An acknowledged committed operation and its deduplication result survive promotion and checkpoints.
- Logical history remains contiguous across separate WAL files and restarts.
- Query-visible and checkpointed state contain only committed applied commands.
- Liveness observations never lower the quorum or change a request's promised durability.
- Rotation and duplicate acknowledgements cannot release an insufficiently replicated response.

## Prerequisites

[C13](protocol.md) Q1–Q4, [C4](tablet-map.md), [C2](transport.md), and C7's checkpoint contract
before M4. Basic application lands in M4; the full retry/failover release gate is M6.

## How it would be measured

[C10](performance.md)'s paired durable/volatile replication arms, idle-group scale test, row-size
sweep and conditional-update workload. Compare completed committed operations and result latency;
an accepted-only `One` result is not interchangeable with a committed mutation result.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `quorum_success_requires_distinct_durable_voters` | Duplicate remote/local completions cannot release a response; durable hooks establish the required voters before success | M4 |
| `rotation_preserves_pending_replication_requirements` | Several WAL rotations never count local storage twice or compare offsets from different generations | M4 |
| `bootstrap_does_not_reduce_configured_quorum` | RF=3 on one node is observable but rejects default writes until initialization completes | M4 |
| `async_replica_cannot_weaken_durable_quorum` | Incompatible policies are refused or that receipt is excluded from stable acknowledgements | M4 |
| `table_streams_recover_independently_without_holes` | Interleaved tables, different fsync completion orders and restart preserve each stream | M4 |
| `duplicates_gaps_and_old_terms_do_not_reapply` | Duplicate append, lost frame, reordered response and stale term converge through protocol recovery | M4 |
| `uncommitted_suffix_never_enters_checkpoint` | Force rotation and compaction before commitment, elect another leader, restart: no speculative effect remains | M4 |
| `conditional_results_follow_committed_order` | Concurrent insert/update/delete/no-op commands give consistent results on all replicas | M4 |
| `slow_tablet_does_not_block_other_tablets` | A tablet without quorum consumes bounded resources while others continue | M4 |
| `lost_response_retry_returns_original_result` | Drop a committed reply and change primary; same identity returns the original result once | M6 |
| `retry_identity_survives_snapshot_and_migration` | Checkpoint, move and retry preserve effect and result; changed payload and expired identity are refused | M9a |
| `volatile_replication_uses_common_encoding` | Ephemeral replica digests converge through the same command encoding, with explicit weaker durability | M4 |

## Related

[C6](reads.md), [C7](failover.md), [C8](rebalancing.md), [C13](protocol.md).
The adapter must honor the chosen library's storage completion contract; for the control-plane
candidate see [OpenRaft log storage](https://docs.rs/openraft/latest/openraft/storage/trait.RaftLogStorage.html).
For the data-plane spike inspect [raft-rs RawNode](https://docs.rs/raft/latest/raft/raw_node/struct.RawNode.html),
including Ready processing and persistence/application advancement, rather than assuming an async
runtime or shared-WAL adapter is already provided.
