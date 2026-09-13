# F38 — The inter-node transport

## Context

[M1](../distributed/milestones.md#m1-node-identity-and-the-control-plane-thread) made a node
somebody: an identity in its marker, a cluster it was bootstrapped into, a control thread with a
consensus group of one. It left the group with a network that answered `Unreachable` to every
peer it would ever be told about, the peer endpoints advertised in the member record and bound by
nothing, and `ShardContact` with one variant. [M2](../distributed/milestones.md#m2-the-inter-node-transport)
is the milestone where a node speaks to a node, and [C2](../distributed/transport.md) is the
design it speaks under: remote contacts, a bounded pre-schema handshake, forwarding as validated
bytes, separate lanes for control, data and bulk traffic, bytes bounded on every queue, and a
trace that survives the hop. This feature builds that, with a **static placement** standing in
for the membership M3 delivers - a map that names every node's identity, endpoints and shard
count, which every node reads and none of them elects - and it takes the M2 evidence line's hop
capture as three benchmark arms with the process model they needed.

It was landed across six commits. The first five put the transport, the control lane, the
validation, the bounds and the trace into the engine, the fixture and the protocol crate, each
with its acceptance test; the sixth added the hop arms and the spawn path, found and fixed a hole
in the stage attribution of a forwarded query on the way, and wrote this page.

## What it does

**Three lanes, three sockets** (`shoal-core/src/server/peer/`). The *data* lane carries forwarded
bundles and their answers and is owned by the shard that sends on it; the *bulk* lane carries
snapshot streams and is owned by a shard too; the *control* lane carries the control group's RPCs
and pings and is owned by the control thread. A lane is a socket of its own because priority on
one stream cannot preempt bytes already written to it, which is C2's reason and the one a stalled
snapshot proves below. A link is owned by one task on one executor and is `Rc`, never `Send`; it
dials lazily on its first frame, backs off from `reconnect_min` to `reconnect_max` with a quarter
of jitter, and reports itself as `idle`, `connecting`, `up` or `backoff` with its counters -
frames and bytes sent, frames shed at the bound, frames dropped when it went down, dials, bytes
queued - through `ShoalPool::transport()`.

**`ShardContact::Remote { node, shard }`** in `shard.rs`, beside `Local`. `mesh_id()` is gone,
replaced by `local_index() -> Option<usize>`, so a remote contact cannot become an index into the
local mesh by anybody's oversight; `Comms::send` on a remote contact is `NotLocal`, a routing bug
named rather than a channel that does not exist. `Ring::with_placement` builds the map: tablet
`t` belongs to node `t % N` and, on that node, to shard `(t / N) % shards`, chosen independently
so shared factors starve nothing; a placement of one node is exactly the standalone ring, which
`a_one_node_placement_is_the_standalone_ring` holds. `Ring::owner_of` and `Ring::tablet_of` are
public so a benchmark choosing keys for a node cannot drift from the map the server routes with.

**A 68 byte hello before any schema** (`shoal-proto/src/shared/protocol/peer/hello.rs`): cluster
id, node id, incarnation, lane, the wire versions the sender reads, a refusal reason, a
capability set, the schema identity, the shard count and the largest frame it accepts. Both ends
run one judge, in one order: wire version, then whether this listener serves the lane, then the
cluster, then whether the node is in the placement, then whether it runs the shards the placement
says, then the schema. The refusal is written into the ack before the error returns, so the other
side learns why. `PeerRefusal` has ten codes and fails closed on an unknown one. The schema
identity is **`SCHEMA_ID`**, the structural fingerprint *without* `PROTOCOL_VERSION` folded in
(`shoal-derive/src/traits/fingerprint.rs`), which is C2's "separate structural schema identity
from transport capabilities": two nodes compare the schema, the wire version and the capabilities
as three things, and at M2 all three have to match exactly.

**A bundle is forwarded as the client's bytes** (`forward.rs`, `shard.rs`). The coordinator
routes every query in a bundle by the scalars in its archive as it always has
([F26](archive-routed-requests.md)); the shares whose partitions live on another node become one
`Forward` frame *per node* - a 48 byte preamble, one 22-byte-plus-keys entry per query the node
owns, and the whole bundle once, shared as `Bytes` and never copied. The receiving node validates
the bundle again with `Queries::access`, refuses an offset past the end, an entry naming a shard
it does not have, a hop count that is not zero, more than 4096 entries or 4 MiB of them, and hands
each entry to the shard it names exactly as its own coordinator would - the peer connection is
announced to every shard as a client, so there is one reply path, not two. A whole answer comes
back as sealed bytes the origin relays to its client without re-validating; a share of a gathered
query is validated and deserialized on the origin, where the merge is. `ErrorCode::Shedding` is
what a query gets when the queue to its node is full, `Unavailable` when the link went down
before the frame was written, and the new `OutcomeUnknown` when it went down after, or when
`forward_timeout` passed - a sweeper runs every tenth of that, never less than fifty
milliseconds. ~~Nothing retries at M2; the `attempt` field exists for M6.~~ Since
[F42](primary-failover.md) a forward the link never wrote is sent to another holder once,
under the same attempt; the client retries the rest under its identity.

**Bytes are bounded on every queue.** Each lane to each peer has a bound in bytes
(`data_queue_bytes` and `bulk_queue_bytes` 64 MiB, `control_queue_bytes` 8 MiB) and sheds
synchronously, before anything is recorded, when a frame would pass it. Each accepted connection
has `inflight_bytes` (64 MiB) of forwarded bundles unanswered, counted until every entry of a
bundle is answered, and a connection at its bound stops reading; a bundle larger than the whole
bound is admitted alone. The bulk lane at M2 has one producer, `probe_bulk`, and a receiver that
counts, checksums and discards; what it proves is that a snapshot stream stalled for a minute
sheds at its own bound, grows the process by less than three times that bound, and leaves the
control lane answering pings and the data lane answering reads - or failing them with a named
code within the timeout - which is `slow_peer_has_bounded_bytes_and_independent_lanes`.

**The control lane is real.** `PeerNetwork` is the group's `RaftNetworkFactory`; a `ControlLink`
wraps a peer link on the control lane with a table of pending correlation ids, and
`append_entries`, `vote` and `full_snapshot` go over it as JSON under a 16 byte head, a link
failure mapping to `Unreachable` so openraft retries. The control thread binds a listener of its
own after the leader wait, serves the control lane alone, and dispatches into its `Raft`; a
`Ping` answers with the node's incarnation and topology version. It dials the placement's control
address for a node, never openraft's own record of where the peer is. `ControlHandle::ping` and
`vote_probe` are what the fixture drives: a vote for term one from a peer that has elected itself
is not granted, both ways, which is `control_lane_answers_a_vote_from_a_placed_peer`.

**A trace crosses the hop without a false parent.** `trace::context_of` is the inverse of the
`adopt_remote_parent` [F35](wire-trace-context.md) built, and it is taken once per forwarded
*entry* from the query's own `Coordinator::route` span, so the `Shoal::forwarded` span the
serving node opens hangs off that query and never off the bundle's `Shoal::request` root. The
fixture's `trace()` writes every exported span on both nodes to a file, and
`trace_context_crosses_nodes_without_false_batch_parent` reads both back.

**Mutual TLS 1.3 on every lane, handed to the kernel.** `cluster.tls` - `cert`, `key`, `ca` -
makes every lane a kTLS session exactly as `networking.tls` does for clients
([F14](encryption-in-transit.md)): the listener requires a certificate chained to `ca`, the
dialler presents its own, tickets are off and secrets are extracted. Absent, the lanes are
plaintext and peer identity is trusted inside whatever boundary the deployment draws around them,
which the configuration page now says. A SAN of `shoal-node://<id>` is written; nothing checks it
yet (Q11, below).

**The fixture places a cluster** ([F36](cluster-harness.md)). `Cluster::builder().cluster(n,
cores)` mints one cluster id and a node id per child, reserves a data and a control port each,
writes each marker, and hands every child the same placement; a child builds
`Cluster::default().bootstrap(true).control_core(..).port(..).control_port(..).placement(..)`.
`lane_links(true)` puts a byte proxy in front of each node's data and control ports and rewrites
the placement to the proxies, so a test can `cut`, `delay` or `heal` one lane to one node.
`Endpoints` carry the bound data and control addresses, and a child answers `PING <idx>`,
`VOTE_PROBE <idx>`, `TRANSPORT`, `PROBE_BULK <idx> <bytes>` and `FLUSH` on stdin.

**The hop arms and the process they needed** (`shoal-bench/src/workloads/cluster_hop.rs`,
`harness/cluster.rs`). `macro/cluster/hop/{same_shard,local_shard,remote_node}` are one read - a
get of one 1024 byte row from the ephemeral unsorted table, one outstanding - against one two-node
static placement, differing in where the row lives relative to the shard that accepted the
connection: node zero's keys on a one-shard node zero, the same keys on a four-shard node zero so
three queries in four cross the mesh, and node one's keys so every query crosses the data lane.
The measured process stages the placement itself - mints the identities, writes each node's
marker, allots disjoint physical cores by walking the machine in the order
`Resources::cpus_reserving` walks it and refuses a machine too small to isolate, takes a port
block from `cluster_ports` - and starts the peer as a `shoal-workload serve --staged <json>` child
it kills on the way out. `ClusterFacts` gains the placement, every node's cores, the `hop` the arm
was built for with the mix its construction implies, and the data lane's counters. Under the
`stage-profile` build the stage report splits every op by the hop it took, keyed `get/same`,
`get/local`, `get/remote`.

## Design choices

**A peer connection is a client.** The peer listener announces an accepted data connection to
every shard with the same `NewClient` a client gets, so a shard answering a forwarded query
writes to the connection's channel exactly as it writes to a client's, and the relay task on the
other side of that channel frames the answer as `Forwarded`. One reply path rather than a second
one that would have to be kept equivalent to the first.

**Every shard binds the peer port with `SO_REUSEPORT`, and a relay hands the entry to the shard
it names.** C2 offered per-shard endpoints as the alternative and asked for the extra local hop
to be measured before adopting them; the relay costs a channel send per entry and needs one port
per node rather than one per shard, which is what keeps a placement a list of two addresses per
node. The hop arms are where that cost becomes a number.

**Static placement instead of a joiner.** A placement names identities in a file, which only
something that staged the markers can do - a fixture, or the harness. It is test-shaped on
purpose and is replaced, not extended, when M3's control plane commits membership; `seeds` is
still refused naming M3, and every node of a placed cluster still bootstraps a group of one.
Building the transport against a map that cannot change is what let its invariants be tested
before membership added the ways a map changes.

**Exact match on version, capabilities and schema.** The hello carries a version range and a
capability set so that M10 has somewhere to negotiate, and at M2 `speaks_our_version` requires
equality. C2's rule is that a compatible handshake has to imply working payloads at the selected
version, and the only version with a codec is this one; accepting n−1 now would be accepting a
layout nothing can decode.

**The control lane speaks JSON.** openraft's RPC types are serde types, the control lane moves
a few small messages a second per group, and a JSON body under a fixed head is the encoding
whose every field is readable in a capture of the wire. The data lane, which moves the client's
rkyv bytes, was never a candidate for the same treatment.

**Incarnation is process start.** Nanoseconds since the epoch at process start, sent in the
hello and answered in a pong, so a peer can tell a restart from a reconnect. Q11 records it as
provisional: a persisted, monotonic incarnation is part of the cloned-directory fencing that
milestone owes.

**The forwarded answer carries what the peer learned running the query.** The origin forwards
bytes it never decodes, so its own stage record of a forwarded query knew neither what kind of
query it was nor where it ran until the sixth commit put the serving node's op and durability
into one of the `Forwarded` preamble's reserved bytes and stamped `RemoteNode` on the pending
record. Found by the stage split on the smoke run, which reported `remote_node` as `other/same`.

**`local_shard` is a mixture, and says so.** The accepting shard is the kernel's choice and
nothing in a release build reports it to a client, so a pure local-shard control per query is
[D7](../direction/shard-aware-routing.md)'s to build. Four shards rather than two so the median
is unambiguously a mesh hop; the artifact carries `expected_mix: {same: 25, local: 75}`, and the
stage split reports what the run actually did - which varies more than 200 samples suggest,
because the mix is decided per connection, not per query.

**The spawn path lives in the workload binary, not the runner's plan.** The runner half of
`shoal-bench` builds with no engine and cannot mint a marker; the plan's process model is one
blocking command per step with no notion of a child that outlives it. Staging in the measured
process keeps both true: a multi-node arm is one `run` command, and the children die with it.

## Alternatives rejected

- **Sending `ServerMsg::Partition`, a kanal sender, a `Span` or any process-local object over
  the wire.** There is no encoder for them and cannot be; the peer frames are the client's bytes,
  the sealed answer, a JSON RPC or a checksummed chunk.
- **Trusting the origin's validation.** A forwarded bundle was validated once, by another
  process, whose memory-safety preconditions do not travel with it. The receiver runs
  `Queries::access` again and checks every length, offset, index and shard before an unchecked
  read; mTLS is not a substitute.
- **Priority queues on one socket.** Bytes already written to a stream cannot be preempted, so a
  bulk frame ahead of a heartbeat delays the heartbeat by its size. Three sockets.
- **Unbounded peer buffers, or bounds in message counts.** A count bounds nothing when a message
  is a 64 MiB snapshot chunk. Every bound here is in bytes, judged before the frame is queued.
- **Routing control traffic through a data shard.** C2 rejects it because it masks partial
  failure; the control thread owns its listener and its links, and the bounded-lanes test stalls
  the data lane and expects the control lane not to notice.
- **Per-shard peer ports.** Measured first, per C2; the relay hop is what the hop arms price.
- **A joiner, or seeds, at M2.** Membership is M3's whole subject; a static map is enough to
  prove the transport and small enough to hold in a test.
- **Frame-class fault hooks under TLS.** The fixture's link is a byte proxy: it can cut, delay
  or heal a lane but cannot see inside a kTLS stream. C11 asks for a fake transport for
  frame-level manipulation; that is a later milestone's, and the real-TLS test stays real.
- **A pure `local_shard` by decoding the accepting shard from somewhere.** Nothing in the
  handshake, the response or the trace carries it in a release build; inventing a channel for it
  is D7's design, not a benchmark's workaround.
- **Spawning the peer from `run/plan.rs` with `Spawn`/`Stop` steps and driving with `--server
  --cluster-facts`.** Closer to C10's separate-driver shape, but it needs a `stage` subcommand for
  the markers and a process lifetime the plan does not have; recorded here as the shape the
  separate driver takes when a node is no longer in the measured process.

## Limitations

- **A certificate is not yet bound to a node.** The listener checks the chain to `ca` and
  nothing else; the `shoal-node://<id>` SAN is written and unread, and `PeerRefusal::Unauthorized`
  and `IdentityMismatch` are defined and never produced. ~~That binding is Q11's and lands with
  the joiner at M3.~~ The joiner landed at M3 ([F39](membership.md)) and fences by incarnation;
  the certificate binding is still open.
- ~~**The placement is static and every node is a group of one.** `seeds` is refused naming M3;
  a placement is replaced, never extended; nothing observes a peer's topology version beyond the
  pong that carries it.~~ Since [F39](membership.md) the placement is the committed map, pushed
  whole to every shard, and the `placement` block is gone.
- ~~**Nothing retries.** `attempt` is always zero. A shed, a lost link and a deadline are answered
  with a code; M6 owns the identity that makes a retry safe.~~ Since [F42](primary-failover.md)
  a never-written forward is rerouted once by the server and the client retries under
  `SendOptions::identity`; a shed and a deadline are still answered with a code.
- ~~**Snapshots are counted, checksummed and discarded.** The bulk lane has a probe for a producer
  and a receiver that installs nothing; `full_snapshot` over the control lane is written and
  unexercised until ~~M4 and~~ M7 - [F40](replication.md)'s tablet groups refuse a snapshot by
  name over their own lane, the fourth one on the data port.~~ Since [F43](node-recovery.md)
  the bulk lane carries a tablet group's snapshot - `SnapshotBegin`, chunks and `SnapshotEnd`
  routed to the target shard - with its control on the replication lane; the probe stays for
  the lane's own test, and `full_snapshot` over the control lane is still unexercised, since
  the control group's members never fall behind its snapshot.
- **`ShoalPool::transport()` reaches shard zero.** The pool asks one shard for its links; the
  relay that would gather every shard's view is not built. A four-shard node's artifact shows
  shard zero's links, which for the hop arms is the whole story on one-shard node zero and an
  empty list on the four-shard one. Filed as an item.
- ~~**`transport.ping_interval` is parsed and consumed by nothing.** No periodic pinger exists;
  the pings the fixture sends are on demand. The failure detector is M3's. Filed as an item.~~
  Consumed since [F39](membership.md) ([Resolved #96](../appendix/resolved/ping-interval-consumer.md)).
- **`StatusReport` (message type 19) is reserved and unsent** - and stays so: the report is a
  `ControlKind` over the control lane's existing framing ([F39](membership.md)).
- **`local_shard` is a mixture until D7.** Its artifact says so, and its median is a mesh hop
  by construction, but a pure per-query local-shard control needs a shard-addressable
  connection. Filed in todos against D7.
- **The stage records a serving node makes for a peer are not drained by the harness.** They are
  made in the peer's process and flagged `served_for_peer`; the report is the origin's half, which
  since the sixth commit carries the op and the hop.
- **The hop capture on the benchmark host has not been taken.** What ran is a smoke run on the
  development host, below. The `cluster-hop` family lands on the all-workloads page; a page of
  its own comes with the first committed capture.
- ~~**Incarnation is provisional.** Process start time, not a persisted counter.~~ A persisted
  counter in the marker since [F39](membership.md), and the fencing rule is written against it.
- **Wire version and capabilities match exactly or refuse.** M10's codecs are what make a
  range mean something.

## Invariants to uphold

- **A link is owned by one task on one executor and is never `Send`.** `Peers` lives in the
  shard, the control link in the control thread, and neither crosses. A future that wants to
  share one is a design change.
- **A process boundary re-establishes every checked-decoding invariant.** The receiver validates
  the bundle, every length, every offset, every index and every shard before an unchecked read,
  and nothing ahead of an rkyv payload shares its buffer - `codec::read_body` reads it into a
  fresh aligned allocation at offset zero.
  `peer_rejects_wrong_cluster_identity_and_malformed_payload` and
  `a_malformed_forward_is_refused_before_allocation` hold it.
- **Admission is judged before anything is recorded.** `Link::enqueue` sheds synchronously; a
  pending entry exists only for a frame the queue took. A shed is therefore always a definite
  refusal, and the codes keep that distinction: `Shedding` and `Unavailable` mean nothing was
  accepted, `OutcomeUnknown` means something may have been.
- **One slow lane cannot stop another.** The bulk, data and control lanes to one peer are three
  sockets with three bounds; `slow_peer_has_bounded_bytes_and_independent_lanes` stalls one and
  expects the others to answer.
- **The judge's order does not change.** Wire version before lane before cluster before node
  before shards before schema; a refusal names the first thing wrong, and a test that reads a
  reason depends on which that is.
- **A key's node does not depend on any node's shard count.** `node = tablet % N` first, shard
  second. `hop_keys_are_owned_by_the_node_the_arm_names` holds the two node-zero arms to one
  list; a placement rule that mixed the two would make `same_shard` and `local_shard` read
  different data.
- **A staged cluster never shares a physical core between nodes.** `allocate` refuses rather
  than shares, and `placed_facts` fails the run if node zero's pool took cores other than the
  ones it was allotted. A number measured on shared cores is a measurement of the sharing.
- **Node zero's client port is the runner's.** A multi-node arm binds every other endpoint from
  its own block above 20000, so `a_workload_always_binds_the_same_port` and every frozen port in
  `ports.json` stay true.
- **The `Forwarded` preamble's `served` byte is opaque to the protocol.** Zero means
  unclassified; nothing about an answer's meaning may come to depend on it.

## Performance

**Nothing here is a capture.** `shoal-bench status` reports every macro capture stale against
this commit, correctly, and the hop capture the milestone's evidence line names is the benchmark
host's to take ([Benchmarking](../performance/benchmarking.md)); the development host is neither
the hardware nor the governor the corpus is measured under.

**The three arms ran at smoke scale on 2026-09-12 on the development host** (`europa`, 32
threads, `powersave`) against a scratch copy of `shoal.yml` with local storage, two runs each,
written to a scratch directory rather than the corpus. 200 rows, 190 timed gets each after
warmup, one outstanding:

| Arm | node 0 | p50 | p90 | p99 | max | data lane |
| --- | --- | --- | --- | --- | --- | --- |
| `same_shard` | 1 shard, core 1 | 35.8 µs | 36.7 µs | 45.6 µs | 51.5 µs | `up`, 1 frame sent (the readiness probe's key lives on node 1), 0 shed |
| `local_shard` | 4 shards, cores 1–4 | 72.1 µs | 74.6 µs | 78.2 µs | 79.1 µs | shard 0 held no link |
| `remote_node` | 1 shard, core 1 | 82.4 µs | 88.1 µs | 114.7 µs | 117.7 µs | `up`, 203 frames and 265,954 bytes sent (two seed bundles, the probe, two hundred reads), 0 shed, 1 dial |

Node one had one shard on core 3 (core 6 in `local_shard`) and its control thread on core 2
(core 5); node zero's control thread was cpu 0. The record each arm wrote is the `cluster` block
described above, and the wall clocks spread under one percent across the two runs. On this host
and governor the peer hop is 46 µs at the median over the same shard and the tails widen by
about 70 µs; the mesh hop is 36 µs. **These numbers prove the arms run and that the record is
complete; they measure nothing about the benchmark host**, where the loopback budget of 100 µs
added at the median is judged.

**The stage split on the same run** (`--layer stages`, one run): `same_shard` 200 `get/same`;
`remote_node` 200 `get/remote`; `local_shard` 138 `get/local` and 62 `get/same` on one run and
159/41 on another, against the 75/25 the construction implies - the pool's ten connections fall
across four shards unevenly and each carries a share of the reads, so the realised mix is a
per-connection draw. Both are on the artifact; neither is a pure control.

**What the bounds cost is nothing until they are reached.** A queue bound is a comparison per
enqueue; the in-flight bound is a counter per accepted bundle. The bounded-lanes test is the
only place a bound has been reached.

## Tests

| Test | What breaks if the feature is reverted |
| --- | --- |
| `cluster_fixture::remote_query_returns_one_result_per_index` | Two nodes of one shard; two hundred inserts through node zero, about half forwarded; each read back correct; one 24-key get split across the nodes returning each key exactly once |
| `peer::tests::peer_rejects_wrong_cluster_identity_and_malformed_payload` | The judge over a real loopback handshake: a foreign cluster, an unknown node, a wrong shard count, a wrong schema and the control lane on a data listener each refused by name, a matching hello accepted; then a hop count of one, zero entries, a frame too short for its bundle, a truncated entry list and an entry count that disagrees each refused before any allocation |
| `cluster_fixture::slow_peer_has_bounded_bytes_and_independent_lanes` | A data lane delayed sixty seconds and a half-gigabyte snapshot stream: the bulk link sheds at its bound with under 64 MiB queued, the process grows by less than three bounds, the control lane pings; then the lane cut, a read for node one's key answering within twelve seconds with `Unavailable`, `OutcomeUnknown` or `Shedding` rather than hanging, and the control lane still pinging |
| `cluster_fixture::trace_context_crosses_nodes_without_false_batch_parent` | Three queries in one bundle, all node one's: every `Shoal::forwarded` span on node one parented to a `Coordinator::route` span on node zero in the same trace, and none to the bundle's `Shoal::request` root |
| `cluster_fixture::control_lane_answers_a_vote_from_a_placed_peer` | A ping over the control lane answered in microseconds; a vote for term one refused by a peer that elected itself, in both directions |
| `client_disconnect::a_client_that_leaves_before_its_answers_does_not_end_the_shard` | Twenty rounds of a raw client writing two hundred inserts and closing at once; later handshakes still answered and no shard dead - the `reply_sealed` panic (items 32 and 94) a cut-and-reconnecting peer link made reachable |
| `protocol::peer::tests::*` (seven) | The hello and its ack round-tripping; every refusal code round-tripping and an unknown one failing closed; a forward, a forwarded answer with its `served` byte, the control heads and the three snapshot frames round-tripping; the snapshot checksum; a malformed forward refused before allocation |
| `tls::tests::a_peer_listener_requires_a_certificate_from_the_cluster_authority` | A peer server config refusing a client whose certificate is not chained to `ca`, and accepting one that is |
| `ring::tests::a_one_node_placement_is_the_standalone_ring`, `ring::tests::a_placement_interleaves_nodes_then_shards` | The map: one node being the standalone ring; nodes taken first and shards second so a shared factor starves nothing |
| `trace::tests::extracting_a_context_is_the_inverse_of_adopting_one` | `context_of` producing what `adopt_remote_parent` consumes, with the sampled flag intact |
| `fingerprint::the_schema_id_is_the_fingerprint_without_the_version` | `SCHEMA_ID` non-zero, not the seed, not the fingerprint, and distinct across a changed, reordered and projected schema |
| `conf::cluster::tests::validation_refuses_what_is_not_built` | `seeds` still refused naming M3; an unreadable peer certificate refused at validation; the backoff range checked |
| `cluster_hop::tests::the_hop_arms_share_one_placement_and_differ_in_keys_and_node_zero_shards` | Every arm placing one peer of one shard at factor one, all reads, one outstanding, the ephemeral unsorted table, the same row count, and node zero's shard count the one server-side difference |
| `cluster_hop::tests::hop_keys_are_owned_by_the_node_the_arm_names` | Every key an arm reads owned by its node on `Ring::owner_of`; the two node-zero arms reading one list; the lists ascending and distinct |
| `cluster_hop::tests::expected_hop_mixes_sum_to_one` | The mixes whole percentages summing to a hundred, the local arm's being the arithmetic of its shard count |
| `harness::cluster::tests::allocate_places_nodes_on_disjoint_physical_cores`, `allocate_refuses_a_machine_too_small_to_isolate` | Node zero's shards on the first distinct cores, the peer's control thread on the next and its shards after; nothing shared; a machine one core short refused naming both counts |
| `harness::cluster::tests::a_staged_node_round_trips_through_json` | The file a `serve --staged` child reads holding everything it was written with |
| `workloads::stages::tests::a_stage_report_splits_each_op_by_hop` | Six gets across three hops pooled under `get` and split under `get/same`, `get/local`, `get/remote`; a report without the split still parsing |
| `workload_ids::tests::the_declared_ids_are_the_registered_ones`, `committed_artifacts::historical_artifacts_and_ports_remain_compatible`, `family::tests::every_workload_has_a_family` | The three ids last, at ports 12375–12377, with every frozen port unchanged and the `cluster-hop` family taking `macro/cluster/hop/` before the wider prefix |
| `explore_index::the_index_mirrors_every_portable_fact` | The `hop` record reaching the explorer's `ClusterFactsLite` field for field |
| `acceptance_tables::acceptance_tables_have_unique_tests_and_valid_milestones` | M2 marked delivered forcing all four C2 rows to exist as functions |

## Related

- [M2](../distributed/milestones.md#m2-the-inter-node-transport), the gate this delivers, and
  [C2](../distributed/transport.md), [C10](../distributed/performance.md), [C11](../distributed/testing.md)
  and [C13](../distributed/protocol.md#q10-and-q11-at-m2), the pages it changed
- [Configuration](../getting-started/configuration.md#cluster), for `tls`, `placement` and
  `transport`
- [F37](node-identity-control-plane.md), whose identity the hello carries and whose cluster
  check the judge repeats, and [F36](cluster-harness.md), the fixture that now places a cluster
- [F35](wire-trace-context.md), the trace context this carries across the hop, and
  [F14](encryption-in-transit.md), the kTLS the lanes reuse
- [D7](../direction/shard-aware-routing.md), the pure `local_shard` control
- [Benchmarking](../performance/benchmarking.md), for `serve --staged` and the process model
- [Items 95, 96 and 97](../appendix/known-issues.md), what the arms and the count found on the
  way, and [Resolved #94](../appendix/resolved/disconnected-client-cleanup.md), what the transport
  made reachable and closed
