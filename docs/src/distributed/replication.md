# C5. Replication and the write path

## Context

A default write waits for a durable quorum; a default read may lag. A primary orders each
tablet's mutations, and every replica applies the same committed commands in the same order,
so an acknowledged write survives leader changes, reconfiguration, restart and compaction. The
protocol is Raft under the shard - `openraft`, one group per table and replica set - on one
shared WAL per shard ([C13](protocol.md#the-contract), P2–P4). Built by
[F40](../features/replication.md), with the persisted retry table and the lease from
[F42](../features/primary-failover.md), snapshots and the retention budget from
[F43](../features/node-recovery.md), and identity expiry from [F45](../features/replica-migration.md).

## How it works

### The groups and the shared WAL

A shard's `Replication` (`shoal-core/src/server/shard/groups.rs`) holds one `Group` per
`GroupSpec` the map gives it ([C4](tablet-map.md#the-placement-rule)) - its `openraft` handle,
its `GroupMachine`, its checkpoint - and a table from `(TableId, tablet)` to the group serving
it. Every persistent group's log is the shard's shared format 2 WAL under `wal/Shard-N/`
(`shoal-core/src/server/wal/`): a frame is `[len u32][gxhash32]` unhashed, then
`[kind][version=2][flag][reserved][group u64][index u64][term u64][leader ShardAddr]` hashed
with the body, so the index over a file is rebuilt from headers alone and a group's history is
the frames that name it. Kinds are `Blank`, `Normal`, `Membership`, `Vote`, `Committed`,
`Purged`, `Truncate` and `Forget`; a membership body is postcard; vote and committed records
carry no body. The store stages every group's appends into the open batch, and one writer task
does one `write_at` and one `fdatasync` per batch **across every group on the shard**, then
completes every `IOFlushed` in it - the group commit across groups the M1 spike asked for
([C13](protocol.md#q1-and-q13-at-m1)). A segment is sealed past `replication.segment_bytes`;
rotation advances the durable position and releases nothing. A volatile group - an ephemeral
table's - logs into a `MemoryWal` bounded by `volatile_log_bytes`, which a restart empties.

### A write's path

```mermaid
sequenceDiagram
    participant C as client
    participant K as coordinator shard
    participant E as executing shard (holds the replica)
    participant L as group leader
    participant F as followers
    C->>K: bundle (identity = bundle id, uuid v7)
    Note over K: Queries::access validates once<br/>route by key on the ring; write_admission
    K->>E: ServerMsg::Query (mesh) or Forward (data lane)
    Note over E: write_command -> (table, key, payload)<br/>serves_tablet? else StaleTopology<br/>propose_write: pending_bytes / volatile bound -> Shedding<br/>is_expired(identity) -> IdentityExpired
    E->>L: raft.client_write(Command) here if leading,<br/>else one hop: ReplicateKind::Propose
    Note over L: Lease::of == Lapsed -> NotLeader before anything is appended
    L->>L: append to wal/Shard-N (batch, one fdatasync)
    L->>F: AppendEntries (replication lane)
    F-->>L: ack after the follower's own fdatasync
    Note over L: majority durable: committed
    L->>E: apply in committed order on every replica
    Note over E: dedup LRU -> Duplicate / Refused<br/>apply_command derives the result, commits nothing to storage<br/>remember(identity, digest, result)
    E-->>K: ServerMsg::Proposed: Applied(result) + SessionToken
    K-->>C: response (Flags::SESSION_TOKEN)
```

The coordinator - the shard whose client sent the bundle - validates the bytes once, routes
each query's keys on the replica ring and admits a write against the map
(`QuorumUnavailable` naming the shortfall when fewer members are up than the factor's quorum).
The executing shard, the one hosting the tablet's slot on this node, turns the query into a
`Command { table, tablet, request: RequestId { bundle, index }, payload }` - the same intent
bytes a standalone node would log, built once by `build_intent` above both storage engines -
and `propose_write` admits it: `pending_bytes` (proposed and unanswered bytes per group) and
`volatile_log_bytes` shed before anything is recorded; an identity older than
`replication.retry_window` or than the group's `expired_before` watermark is `IdentityExpired`
before it is proposed, so the log never carries it. The command is proposed through this
replica's handle when it leads and hops once to the leader over the replication lane when it
does not (`ReplicateKind::Propose`); a hop whose frame the link never wrote is `NotLeader` at
once, one written and unanswered is `OutcomeUnknown` at the deadline. The leader whose lease
lapsed (`Lease::of`, [C7](failover.md#the-lease)) answers `NotLeader` before it appends.

The leader appends, the batch syncs once, followers append to their own shards' WALs and
acknowledge after their own fsync, and the entry commits at a majority of the committed voter
configuration. Every replica then applies it in committed order (`GroupMachine::apply` posts
`ServerMsg::Apply` to the shard loop, which runs `apply_command` on the table): a partition the
apply needs from disk parks the batch and blocks that group alone; the result - inserted or
not, deleted or not, updated or not - is derived from the state the replica finds, and no
storage commit of its own happens, because durability is the log's. The proposer waits for its
own replica's apply, then `answer_proposal` mints a `SessionToken { cluster, table, tablet,
group, index }` and the answer goes back the way the query came. `All` waits until every
voter's matched index has passed the entry.

### The quorum gate

The threshold is the committed voter configuration's majority, counted by distinct voter
identity from the library's own matching. A repeated acknowledgement updates a watermark and
adds no vote; local durability counts once; learners, installing copies and replicas from
another term or configuration count for nothing; `Up`/`Down` never changes the size of a
quorum (`quorum_success_requires_distinct_durable_voters`, `async_replica_cannot_weaken_durable_quorum`).
A persistent table configured `Async` on a cluster node is refused at start, since a receipt
that precedes an fsync cannot make a durable quorum. A cluster of one at a factor of three is
observable and refuses default writes until two more members are up
(`bootstrap_does_not_reduce_configured_quorum`). One stalled group holds only its own pending
bytes; the shard's other groups continue (`slow_tablet_does_not_block_other_tablets`).

### Followers apply in order

Append matching, conflict rejection and duplicate handling are the library's: a duplicate
entry is acknowledged without reapplying it, a gap is fed from the retained log or a snapshot,
a higher term is persisted before the reply (`duplicates_gaps_and_old_terms_do_not_reapply`).
Only committed prefixes reach query-visible state and checkpoints hold only applied, committed
state: a leader's uncommitted suffix that a later leader truncates is never in an archive,
because a sealed segment is handed to the compactor only once every group applied past its
frames (`uncommitted_suffix_never_enters_checkpoint`).

### Checkpoints and retention

A group's checkpoint is the last log id its table's archives are complete to, recorded per
group with its membership in `wal/Shard-N/checkpoint.json` and moved by the compactor: a sealed
segment is compacted once every group applied past it and deleted once every group purged past
it. openraft's snapshot is that checkpoint, and the purge follows it every
`replication.checkpoint_entries`, keeping `retained_entries` behind for a slow member. The
sealed WAL is bounded in bytes by `replication.retained_bytes`: past it the sweep forces the
groups pinning the oldest segments to snapshot and purge, and a member behind the purge point
is fed a snapshot ([C7](failover.md#a-returning-node)). A volatile group's checkpoint is its
applied position and its snapshot is cut from memory.

### Retry identity

A bundle's identity is its id, a version 7 uuid minted by the client (`SendOptions::identity`
pins one; the client's retry loop reuses it), and every command carries it as `RequestId`. On
apply, every replica remembers the identity, a digest of the payload and the result in a
per-group LRU (`MachineState`), so a retry after a lost reply is answered as the first attempt
was - `Duplicate` with the original result - and never applied again, and a reuse with another
payload is `Refused`. The table is persisted beside the checkpoint as `wal/Shard-N/retries.bin`,
staged as `retries.next.bin` before `checkpoint.json` names the index it is complete to and
renamed over it once that landed, so a crash anywhere leaves one describing the checkpoint on
disk ([Resolved #115](../appendix/resolved/retry-sidecar-crash-window.md)), and seeded at
open only from a sidecar written for exactly that checkpoint; a snapshot carries it in its
trailer, so a retry across an election, a compaction, a restart, a snapshot and a move is
answered the same (`lost_response_retry_returns_original_result`,
`retry_identity_survives_snapshot_and_migration`). An identity's timestamp bounds its life:
older than `retry_window` (five minutes) or than `expired_before` - the newest time-ordered
identity the table evicted, carried by the checkpoint and the snapshot manifest - is
`IdentityExpired` before proposal, never applied as new. An identity that is not time-ordered
never expires by time.

### What the client is promised

| Outcome | Code | What was recorded | Retry |
| --- | --- | --- | --- |
| `Applied` with a result and a token | — | Durable, committed, applied on a majority; survives loss of a minority | — |
| `Duplicate` | — | The first attempt's result, from the retry table | — |
| `Shedding` | 30 | Nothing: the byte bound refused it before proposal | Safe, same identity |
| `NotLeader` | 62 | Nothing: a lapsed lease, a hop the link never wrote, or no leader within the deadline | Safe, same identity |
| `QuorumUnavailable` | 51 | Nothing: too few members up for the factor's quorum | Safe, same identity |
| `IdentityExpired` | 22 | Nothing: the identity is older than the window or the group's watermark | Mint a new identity |
| `StaleTopology` | 55 | Nothing: the node serves no group for the tablet; the origin re-sent it once | Safe, same identity |
| `Unavailable` | 50 | Nothing written: the link went down before the frame | Safe, same identity |
| `OutcomeUnknown` | 32 | Proposed or written and never answered within the deadline; may have committed | Same identity only, answered from the retry table |
| `Timeout` | 31 | The client's own deadline passed | Same identity only |

`SendOptions::retry(within)` loops on `NotLeader`, `Unavailable`, `QuorumUnavailable`,
`ConnectionLost`, `OutcomeUnknown`, `Timeout` and connection errors with a backoff from 20 ms
to 500 ms under the same identity ([Client](../api/client.md)). `One` writes are refused at
validation: a local append that may be rolled back needs an accepted-or-pending result API
that nothing offers. A volatile table's write is committed by its group and survives no
full-cluster restart, and is never counted as a stable-storage quorum.

## Design choices

One group per ordered replica set rather than per tablet, because heartbeats do not coalesce
across groups and the spike found a thousand groups saturate a thread. One shared WAL per shard
with one fsync per batch, because sixty-four independent groups fsyncing on one executor cost
27× one group ([C13](protocol.md#q1-and-q13-at-m1)). Apply-and-derive on every replica rather
than execute-on-the-leader-and-ship-the-effect, because the result of an insert or a
conditional update depends on state only committed order settles. A time-ordered identity,
because an index is nothing a client can compare its retry to. The retry table beside the
checkpoint, because a retry past the purge point has no log to be answered from.

## Alternatives rejected

An acknowledgement counter, which counts one replica twice; an `Unavailable` precheck as a
proof that nothing was committed; a second, weaker commit rule beside the library's; a
separate duplicate WAL per group; speculative execution before commit; `One` writes emulated
by `Quorum`; raft-rs's `RawNode`, which has no runtime seam to adapt.

## What it costs

Network traffic to every follower, per-group state, and durable follower work. Healthy write
latency at a factor of three is about `max(local_sync, min(follower_B, follower_C))` plus
routing, queueing and apply, where each follower term is a round trip and an fsync; every
follower still needs sustainable apply capacity even when omitted from the fastest quorum. The
proposer waits on its own apply, so a parked partition load is on the write's path. Pending
bytes, lag and catch-up debt are recorded beside latency on every cluster arm.

## Limitations

The checkpoint file is rewritten whole. `All` is polled from metrics. An isolated leader
*inside* its lease still appends and answers `OutcomeUnknown` at the deadline. Leadership
stays where an election put it. The identity watermark is replica-local, so two coordinators
can answer one late retry differently, though neither applies it twice. A standalone node has
no retry table. ~~A volatile group's survivor trips a debug assertion when a majority loses its
memory log at once~~ - an empty copy that held the group before waits to be fed by the
survivor rather than electing with the other empty
([Resolved #109](../appendix/resolved/volatile-majority-loss.md)); a group where every member
lost its memory is new again after two election timeouts. See [C15](open-issues.md).

## Invariants to uphold

- A replica counts once, only for durable matching data in the required configuration.
- An acknowledged committed operation and its retry result survive promotion, checkpoints and moves.
- Logical history is contiguous across WAL files and restarts.
- Query-visible and checkpointed state contain only committed applied commands.
- Liveness observations never lower the quorum or change a request's promised durability.
- Rotation and duplicate acknowledgements cannot release an insufficiently replicated response.
- Nothing is proposed after a refusal that says nothing was.

## How it is measured

`macro/cluster/replication/{durable,volatile}` against `macro/cluster/overhead/nodes/3`: the
same placement at a factor of three and of one, on the persistent and the ephemeral table
([C10](performance.md#the-arms)). Completed committed operations and result latency are what
is compared; every cluster record carries the groups, lag, pending and volatile bytes and the
writes answered unknown or rejected.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `quorum_success_requires_distinct_durable_voters` | Duplicate remote and local completions cannot release a response; durable hooks establish the required voters before success | M4 |
| `rotation_preserves_pending_replication_requirements` | Several WAL rotations never count local storage twice or compare offsets from different generations | M4 |
| `bootstrap_does_not_reduce_configured_quorum` | A factor of three on one node is observable and rejects default writes until initialization completes | M4 |
| `async_replica_cannot_weaken_durable_quorum` | An incompatible policy is refused, or its receipt is excluded from stable acknowledgements | M4 |
| `table_streams_recover_independently_without_holes` | Interleaved tables, different fsync completion orders and a restart preserve each stream | M4 |
| `duplicates_gaps_and_old_terms_do_not_reapply` | A duplicate append, a lost frame, a reordered response and a stale term converge through protocol recovery | M4 |
| `uncommitted_suffix_never_enters_checkpoint` | Rotation and compaction before commitment, another leader elected, a restart: no speculative effect remains | M4 |
| `conditional_results_follow_committed_order` | Concurrent insert, update, delete and no-op commands give consistent results on every replica | M4 |
| `slow_tablet_does_not_block_other_tablets` | A tablet without quorum consumes bounded resources while others continue | M4 |
| `lost_response_retry_returns_original_result` | A committed reply dropped and the primary changed: the same identity returns the original result once | M6 |
| `retry_identity_survives_snapshot_and_migration` | A checkpoint, a move and a retry preserve effect and result; a changed payload and an expired identity are refused | M9a |
| `volatile_replication_uses_common_encoding` | Ephemeral replica digests converge through the same command encoding, with explicit weaker durability | M4 |

## Related

[C4](tablet-map.md), [C6](reads.md), [C7](failover.md), [C8](rebalancing.md),
[C13](protocol.md), [Storage](../storage/overview.md), the
[OpenRaft log storage contract](https://docs.rs/openraft/latest/openraft/storage/trait.RaftLogStorage.html)
the shared WAL implements.
