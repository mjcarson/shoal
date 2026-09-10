# C2. The inter-node transport

## Context

Add remote communication while preserving shard ownership, validated framing and bounded work.
Shoal's embedded control and data consensus protocols use this transport; no external service
owns discovery or elections. The transport must make its persistence, timeout and retry semantics
explicit rather than assume reliable sockets eliminate distributed failures.

## What exists today

`ShardContact::Local`, `Comms::send` and the kanal mesh route queries within a process.
`ServerMsg::Partition` carries a Glommio read result with a restricted Send safety argument.
`QueryMetadata` carries routing/gather and trace metadata; local responses can bypass the
coordinator because client relays are shared within the node. The protocol module in
`shoal-proto` has runtime-free framing and handshake code. F14 provides TLS/kTLS and F35 trace
context. Existing local queues are mostly unbounded.

## The design

### The second variant

Add `ShardContact::Remote { node, shard }`, and audit every `mesh_id()` caller so a remote contact
cannot be converted into a local array index. Peer encoders accept only explicitly serializable
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
transfer cannot cause elections or deadlock. Q1/M2 documents which lanes use separate sockets.

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
identity and authorization to join. Q11 resolves first-boot certificate provisioning before a
random NodeId exists, SAN encoding, CA/certificate rotation and cloned-node fencing. TLS cannot
be described as a complete identity design until that bootstrap path exists. When deployment
policy allows plaintext, document that peer identity is trusted inside that explicit boundary.
Client encryption requires equivalent protection on both control and data peer lanes.

### The message types

| Family | Required information |
| --- | --- |
| Peer hello/ack | Identity, incarnation, capabilities, schema identity, refusal reason |
| Forward / Forwarded | Original operation/query and attempt ids, destination, coverage, resolved policy, remaining deadline, bounded hop count and return address |
| Data consensus | Tablet/group identity plus selected library's election, append, configuration and read-barrier payloads |
| Replication receipts | Matching term/history, replica/configuration and durable completion evidence; duplicate-safe |
| Catch-up | Tablet/group, matching term/index boundary and snapshot fallback negotiation |
| Snapshot begin/chunk/end | Snapshot/transition identity, manifest, boundary, offset, length, checksum and resume metadata |
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
The current schema fingerprint folds PROTOCOL_VERSION (`shoal-derive/src/traits/fingerprint.rs`);
separate structural schema identity from transport capabilities or provide explicit versioned
fingerprints/codecs. Merely accepting n−1 in the handshake leaves incompatible payload layouts.

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
