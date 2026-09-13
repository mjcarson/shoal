# C2. The inter-node transport

## Context

Add remote communication while preserving shard ownership, validated framing and bounded work.
Shoal's embedded control and data consensus protocols use this transport; no external service
owns discovery or elections. The transport must make its persistence, timeout and retry semantics
explicit rather than assume reliable sockets eliminate distributed failures.

## What exists today

**Delivered at M2 by [F38](../features/inter-node-transport.md)**, against a static placement
~~rather than the membership M3 brings~~ that [F39](../features/membership.md) then replaced
with the committed map: a node's peers are the members the control group holds, the handshake
judges by the committed incarnation and admits a joiner on the control lane alone, the control
lane carries `Join`, `StatusReport` and `Propose` beside the group's RPCs, and a stalled data
receiver leaves control elections working (`control_elections_do_not_depend_on_data_shard_relay`).
`ShardContact` has `Local` and `Remote { node, shard }`;
`mesh_id()` is gone and a remote contact cannot become a local index. Three lanes on three
sockets - data and bulk owned by shards, control by the control thread - each with a byte bound
that sheds before anything is recorded, and an in-flight bound per accepted connection. A 68
byte pre-schema hello is judged in one order on both ends; a bundle is forwarded as the client's
bytes, one frame per node, and re-validated on arrival; whole answers come back sealed and shares
are merged at the gather; `Shedding`, `Unavailable` and `OutcomeUnknown` keep a definite refusal
apart from an unknown outcome. The control group's RPCs go over the control lane as JSON. A trace
crosses the hop from each query's own span. Every lane is mutual kTLS when `cluster.tls` is set.
The four M2 rows of the table below exist as tests, and the hop arms of
[C10](performance.md#the-workloads) exist as workloads. **At M4** ([F40](../features/replication.md))
a fourth lane, `Lane::Replication`, on the data port and owned by every shard, carries the
data consensus family: `Replicate` (type 25) and `ReplicateResponse` (26), a 24 byte head
naming the correlation id, the group, the target shard, the kind - `AppendEntries`, `Vote`,
`Propose`, `Snapshot` - and the deadline, and a postcard body; one link per peer node per
shard with its own correlation table and its own bound, `transport.replication_queue_bytes`,
so a follower that stops reading holds nothing but its queue. `Propose` is the one hop a write
takes from a replica to its leader; ~~`Snapshot` is answered by name until M7~~ `Snapshot` is
the M7 control pair ([F43](../features/node-recovery.md)): `Begin` carries the sender's vote,
the stream id and the manifest and `End` the total and checksum, answered `Resume { from }`,
`Installed` or `Refused`, while the bytes ride the bulk lane as `SnapshotBegin`, chunks and
`SnapshotEnd` frames routed to the target shard, so a stalled transfer holds the bulk lane's
queue and never the replication lane's. **At M5**
([F41](../features/read-consistency.md)) the lane gains `ReadBarrier`, the one hop a strong read
takes from a replica to its leader for a read index; a forward entry carries a read plan - the
resolved level, the gather slot it fills and its tokens - under `FLAG_READ`; the `Forwarded`
head widens from thirty-two bytes to ninety-six for the attempt, the slot and a token; and all
three sit behind `CAP_READ_CONSISTENCY_V1`, which the M2 exact-match rule refuses an M4 peer
by. **At M8** ([F44](../features/repair.md)) the replication lane gains `Digest`, a member's
canonical report of a scrub asked by the operation and answered pending, the report or
unknown, and `Quarantine`, a driver's word to a member about its copy, answered once the
member's marker is durable; a repair snapshot rides the M7 stream with the operation on its
begin, judged against the receiver's checkpoint and answered `Behind` when the cut has to be
taken again. `PROTOCOL_VERSION` went to 4 for the scrub entry a peer built before could not
tell from a write. Between a client and a node the same milestone spends the hello's reserved byte fourteen on
a capability set, and only a granted bit puts a read options section behind a bundle's trace
context or a session token ahead of a response's payload - the selected-version contract, with
no version bump. **At M6** ([F42](../features/primary-failover.md)) the transport learns the
difference between a frame it wrote and one it did not, on both lanes: a forward the data link
never wrote is sent to another holder once under the same attempt and slot, a proposal or a
barrier the replication link never wrote is `NotSent` and answered `NotLeader` at once, and
either that was written and never answered stays unknown. A link a frame wants redials at
`reconnect_min` rather than waiting out its exponential backoff, which is what bounds the wait
for that answer at a dead peer to the floor and what keeps the control plane's leader from
losing its lead every time a member returns. The identity and the budget cross the hop
unchanged (`deadline_and_operation_id_survive_forwarding`); nothing on the wire moved.

Before that: ~~`ShardContact::Local`, `Comms::send` and the kanal mesh route queries within a
process.~~ `ServerMsg::Partition` still carries a Glommio read result with a restricted Send
safety argument, and has no wire form. `QueryMetadata` carries routing/gather and trace metadata;
local responses bypass the coordinator *within a node* because client relays are shared there,
and across nodes they return to the coordinator holding the client. F14 provides TLS/kTLS and
F35 trace context. ~~Existing local queues are mostly unbounded~~ - the peer queues are bounded
in bytes; the local mesh queues still are not ([item 15](../appendix/known-issues.md#15-no-backpressure-anywhere)).

## The design

### The second variant

Add `ShardContact::Remote { node, shard }`, and audit every `mesh_id()` caller so a remote contact
cannot be converted into a local array index. *Built at M2: `mesh_id()` was replaced by
`local_index() -> Option<usize>`, so the audit is the type's.* Peer encoders accept only explicitly serializable
message types; there is no wire form for `ServerMsg::Partition`, a kanal sender, a Span handle,
or any other process-local ownership object.

### Who owns a connection

Initially a sending data shard owns lazy outbound connections per remote node and traffic lane.
A data peer listener can use SO_REUSEPORT and relay bounded validated bytes to the named shard.
Measure the extra local hop before adopting per-shard endpoints. A lane may need a separate
socket to avoid TCP head-of-line blocking; priority queues on one stream cannot preempt bytes
already written to it.

The embedded control runtime owns independent control connections/listener (C1), including
metadata Raft and status reports. A stalled data shard must not block them. Data-consensus vote,
heartbeat and acknowledgement messages also need reserved scheduling/buffer capacity so bulk
transfer cannot cause elections or deadlock. ~~Q1/M2 documents which lanes use separate sockets.~~
*At M2 every lane is a separate socket: data, bulk and control, the first two dialled to the
peer's data address and the third to its control address.*

With S shards and N nodes, one lane starts with S × (N−1) outbound data connections per node;
additional lanes/inbound connections/control traffic increase that. Bound reconnect attempts,
file descriptors, queued bytes and total buffers. Stagger reconnects with backoff/jitter.
Record and test limits at Q13's target cluster size, not just N=3.

### The peer handshake

Use a fixed, bounded pre-schema handshake, carrying cluster/node identity, incarnation, supported
wire versions/capabilities and schema identity. Exchange authenticated control/data/client
endpoints during discovery. Reject a wrong cluster and mismatched identity before any data frame.
A seed address discovers the embedded cluster; it is not an external membership authority.

Use mTLS and a defined certificate-to-node binding when configured; validate chain, expected
identity and authorization to join. *At M2 the chain is validated to `ca` on every lane and the
`shoal-node://<id>` SAN is written and not yet read; ~~the binding lands with the joiner~~ the
joiner landed at M3 fencing by incarnation and the SAN is still unread.* Q11 resolves first-boot certificate provisioning before a
random NodeId exists, SAN encoding, CA/certificate rotation and cloned-node fencing. TLS cannot
be described as a complete identity design until that bootstrap path exists. When deployment
policy allows plaintext, document that peer identity is trusted inside that explicit boundary.
Client encryption requires equivalent protection on both control and data peer lanes.

### The message types

| Family | Required information |
| --- | --- |
| Peer hello/ack | Identity, incarnation, capabilities, schema identity, refusal reason |
| Forward / Forwarded | Original operation/query and attempt ids, destination, coverage, resolved policy, remaining deadline, bounded hop count and return address. *At M5:* the attempt minted per bundle and echoed, the resolved level and the slot per entry, the budget remaining rather than a fresh one, and a token on a write's answer |
| Data consensus | Tablet/group identity plus selected library's election, append, configuration and read-barrier payloads. *At M4:* `Replicate`/`ReplicateResponse` on the replication lane, openraft's `AppendEntries` and `Vote` as postcard. *At M5:* `ReadBarrier`, answered with the leader's `ReadLogId` or a leader hint. *At M8:* `Digest` and `Quarantine`, and the scrub entry as a command whose tablet no write can name |
| Replication receipts | Matching term/history, replica/configuration and durable completion evidence; duplicate-safe. *At M4:* openraft's append response, sent after the follower's `fdatasync` |
| Catch-up | Tablet/group, matching term/index boundary and snapshot fallback negotiation. *At M7:* openraft's, from the retained log while the follower is inside it and a snapshot once it is not |
| Snapshot begin/chunk/end | Snapshot/transition identity, manifest, boundary, offset, length, checksum and resume metadata. *At M7:* the `Begin`/`End` RPCs on the replication lane and the `SnapshotBegin`/chunk/`SnapshotEnd` frames on the bulk lane, each chunk `[offset u64][bytes]` under `replication.snapshot_chunk_bytes`, resumed from the prefix the receiver holds |
| Control Raft | Typed request/response identity and embedded OpenRaft payload |
| Ping / Pong / StatusReport | Probe sequence/incarnation and bounded/coalesced status; no assumption about extensible library heartbeat replies |
| Admin | Authenticated request id, expected version for mutations, operation id and status/result |

Append numeric message types rather than renumbering existing ones. Exact encodings follow the
chosen library/version and C13 Q2/Q10; the first draft's fixed Replicate/SetPrimary sketches are
not a substitute for election, commit, configuration and read-barrier messages.

### The bundle is forwarded as bytes

Forward immutable serialized requests and validate on every process boundary before unchecked
archive access. Validate frame lengths, offsets, query indices, keys/coverage and destination as
well as the rkyv payload. Preserve alignment of archived data. Network arrival cannot inherit
another process's unsafe-memory preconditions. Consensus and snapshot payloads get corresponding
bounds, checksums and validated decoding; trusted mTLS is not a replacement for memory safety.

Serialize replication commands at the common boundary above both storage backends (C5).
Network batches contain typed records routed to their actual replica destinations; a physical
WAL buffer can mix tablets and must not simply be broadcast to one tablet's followers.

### Responses stop bypassing the coordinator across nodes

Remote owners return results to the coordinator holding the client connection. Forward sealed
bytes where no merge is needed. Gathered remote shares require validated decoding and explicit
coverage, including empty results. Preserve one complete response/error per query index and
ignore duplicate/late attempts after completion. Propagate trace context across a request's hops;
a batch spanning several requests needs links or per-record context rather than one false parent.

### Backpressure, from the first line

Bound bytes as well as message counts on inbound/outbound channels, pending client operations,
consensus/gap buffers, gathers and snapshot chunks. Admission sheds before accepting a command
when capacity is unavailable. Once accepted, a timeout or lost peer gives an unknown outcome
unless definitely rejected; do not report ordinary shedding as proof it was never committed.

Separate bulk snapshots/repair from foreground queries and replication, with reserved capacity
for progress messages. A slow follower must not block replication to the other follower or all
of its shard's tablets. Never hold a consensus/storage resource while awaiting a queue whose
consumer needs that same resource. End-to-end budgets and cancellation bound orphaned work.

### Compatibility and rolling upgrades

Negotiate a supported protocol and capability set, then actually encode/decode that version.
~~The current schema fingerprint folds PROTOCOL_VERSION (`shoal-derive/src/traits/fingerprint.rs`);
separate structural schema identity from transport capabilities~~ *Done at M2: `SCHEMA_ID` is the
structural fingerprint without the version, and the hello carries it beside a version range and a
capability set as three things; at M2 all three must match exactly* ~~or provide explicit versioned
fingerprints/codecs~~. Merely accepting n−1 in the handshake leaves incompatible payload layouts,
which is why M2 does not.

Define a cluster minimum/active feature version. Enable new commands or formats only after all
required participants can process them; persist that activation decision. Record rollback limits
once a new storage feature is activated. Test old/new binaries exchanging queries, replication,
snapshots, elections and reconfiguration, not only a successful hello. Schema evolution beyond
exact structural compatibility needs a separately specified migration path (Q10).

## Alternatives rejected

Sending process-local objects, trusting remote validation, unbounded peer buffers and accepting
an old version without its codec are excluded. Using the existing framing is the baseline;
choice of RPC library is secondary to proving compatible payload and flow-control contracts.
Control traffic routed exclusively through a data shard is rejected because it masks partial failure.

## What it costs

Cross-node requests/responses add socket work and validation; remote merges add decoding.
Independent lanes consume sockets/buffers but prevent bulk work monopolizing progress traffic.
Topology, trace and policy metadata are measured at actual encoded size. Reconnect storms and
slow peers are part of the performance and availability tests.

## What it breaks

Comms/contact routing, return addresses, request metadata and framing compatibility. Existing
client-only schemas must remain independent of Glommio and the consensus implementations.

## Invariants to uphold

- A data connection remains owned by one shard; control sockets by their embedded runtime.
- A process boundary reestablishes checked decoding and alignment invariants.
- One slow peer cannot exhaust all memory or block independent quorum progress.
- Accepted write identity and deadline survive every hop and reconnect.
- Compatible handshake implies working selected-version payloads, not just accepted metadata.

## Prerequisites

[C1](node-identity.md), [C13](protocol.md) Q1/Q2/Q10/Q11/Q13, F10 framing, F14 TLS and F35 tracing.

## How it would be measured

[C10](performance.md) local/remote hop controls, row-size/batch sweeps, bulk-stream interference,
connection counts, queue byte limits and mixed-version operation. Record validation and merge cost.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `remote_query_returns_one_result_per_index` | Cross-node split/unsplit requests preserve coverage, ordering and response identity | M2 |
| `peer_rejects_wrong_cluster_identity_and_malformed_payload` | Invalid identity, lengths, offsets, archives and alignment cannot reach unchecked access | M2 |
| `slow_peer_has_bounded_bytes_and_independent_lanes` | Snapshot/peer stall cannot exhaust memory or stop unrelated progress messages | M2 |
| `control_elections_do_not_depend_on_data_shard_relay` | A stalled data receiver leaves direct control networking functional | M3 |
| `trace_context_crosses_nodes_without_false_batch_parent` | Remote work retains the originating context or correct batch links | M2 |
| `deadline_and_operation_id_survive_forwarding` | Redirect/reconnect cannot reset budgets or replay accepted writes under a new identity | M6 |
| `mixed_versions_exchange_real_cluster_operations` | n/n−1 codecs support queries, replication, snapshots and elections until explicit activation | M10 |

## Related and implementation references

[C3](membership.md), [C5](replication.md), [C7](failover.md), [C13](protocol.md).
[Wire protocol](../architecture/wire-protocol.md), [F14 TLS](../features/encryption-in-transit.md),
[F35 trace context](../features/wire-trace-context.md).
[OpenRaft integration guide](https://docs.rs/openraft/latest/openraft/docs/getting_started/index.html)
identifies the application network adapter seam; Shoal supplies that adapter over its own endpoints.
