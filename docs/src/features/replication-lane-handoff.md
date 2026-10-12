# F84. A replication lane handed to the executor of its slot

## Context

[S1](../object-storage/prerequisites.md#optional) lists **handing an accepted connection to
another executor** as an optional prerequisite. Its "what it would add" is "bytes read by the
executor that owns the slice they are for".

- **What existed.** Every shard binds the node's peer port with `SO_REUSEPORT`, and the kernel
  picks which shard takes a connection. A frame naming a slot is relayed, as a buffer over the
  shard mesh, to the executor hosting it (`peer::listener::dispatch_target`). Nothing ever moved a
  connection, and the glommio fork's `TcpStream` had `FromRawFd` and no `IntoRawFd`.
- **What X11 measured** ([section 4](../object-storage/streamed-bodies.md#4-a-connection-handed-over-and-bytes-that-hop)).
  A connection under kTLS was handed between executors with a `dup` and `from_raw_fd` at 0.93 to
  1.05 times a direct stream's rate and cpu. The same bytes crossing executors as buffers cost 1.30
  to 1.72 times the cpu a gibibyte at 1 MiB in plaintext, and twice at 64 KiB.
- **What S13 decided** ([a lane for object bytes](../object-storage/isolation.md#a-lane-for-object-bytes)).
  The object lane between nodes hands each connection to the executor of the slice it names, since
  such a connection carries one slice's bytes. A client connection, which carries many slices',
  hops its buffers.

The object lane is M15's, so nothing carried one slot's bytes on one connection yet. Every existing
lane was keyed by its peer node: a shard's replication link to a node carried requests for every
slot of that node its groups had members on. **Decided with the user (2026-10-12): replication
lanes, and no other.**

- A replication link becomes one per member, a slot on a peer node, and its hello names the slot.
- The shard that accepts it hands the connection to the executor hosting that slot.
- Bulk lanes, data lanes and client connections stay as they were. A data lane's bundles name
  many slots. A client connection is S13's to keep hopping, and routing one to an executor was
  declined on 2026-10-09 ([todos](../appendix/todos.md#client-routing)). Bulk lanes are recorded as
  [a todo](../appendix/todos.md#handing-a-bulk-lane-to-its-slots-executor).

The replication lane is where the bytes are. At a factor of three every write's payload reaches
two followers, and each follower read it on whichever shard the kernel chose for the lane, one in
the number of executors by chance, and handed it to the slot's executor across cores. An object
held inline in its row, up to 16 MiB at M13, is replicated that way.

Building it found [item 236](../appendix/resolved/replaced-link-answers.md), a defect in the very
map this rekeys, and fixed it in the same change.

## What it does

### The glommio fork gives up a descriptor

`IntoRawFd` for a non-buffered `TcpStream` (the fork's `d781abd`, on `ZeroCopyDmaStreamWriter`)
takes the stream apart without closing anything:

- both read and write timers are taken off the reactor;
- both sources are dropped, which cancels a poll still in flight by its request and never by the
  descriptor;
- the descriptor is returned.

Reads and writes on a glommio socket are plain `recv` and `send` calls, and only polls are ever
outstanding on the ring, so nothing pending can consume a byte. A buffered stream has no such
conversion, since it may hold bytes it read ahead.

### A replication link per member

`ShardNetwork` keeps `HashMap<ShardAddr, Rc<ReplicationLink>>`, one link per member and not per
peer node.

- **Every request on a link names the link's slot.** The RPC methods lost their `target_shard`
  argument and write the link's own.
- **Its hello names the slot,** in the two bytes after `shards` that were reserved, as the slot
  plus one, zero for none (`PeerHello::slot`, `shoal-proto/src/shared/protocol/peer/hello.rs`).
  Every other lane, and every ack, names none.
- **The number of connections does not change on equal slot counts.** The placement rule
  (`placement::rule_replicas`) puts a tablet's copies on the slot `(t / N) % shards` of every
  node, the same index everywhere, so a shard still opens one link to each peer.

### The acceptor hands the lane over

`peer_acceptor` runs on every shard. After the TLS handshake and the hello, a replication lane is
judged by `peer::handoff::destination`:

- **It named a slot this node has, which another executor hosts.** The stream, still whole and
  with nothing read past the hello, becomes an `OwnedFd` (`HandedLane::release`). It is sent as
  `ServerMsg::AdoptLane` to the executor hosting the slot. That executor makes it a stream on its
  own reactor (`HandedLane::adopt`) and serves it with `serve_replication`, on the queue its own
  listener serves lanes on. No `dup`, so the descriptor has one owner at every instant.
- **It named a slot this executor hosts.** It is served in place, as every lane was.
- **It named no slot** (a build before F84) **or one past the count.** It is served in place,
  and every request is dispatched by the slot its own head names, as before.

kTLS lives on the socket, so a lane encrypted on the executor that accepted it is still encrypted
on the one that adopts it. The connection's first requests are still in the socket when it moves,
whether the dialler sent them before or after reading the ack.

`serve_replication` still dispatches each request by its head's slot, so a request for another
slot would still hop. None does on a handed lane, which carries its slot's requests alone; the
counters below say so.

### Where every lane was read

Each shard counts, in `ReplicationLanes` (`shoal::shared::protocol::stats`):

| Counter | What it counts |
| --- | --- |
| `handed_out` | lanes this shard accepted and handed to the executor hosting their slot |
| `taken_in` | lanes this shard took in from the executor that accepted them |
| `in_place` | lanes this shard accepted for a slot it hosts |
| `unnamed` | lanes whose hello named no slot, or one the node does not have |
| `requests_here` | requests read on the executor hosting their slot |
| `requests_crossed` | requests read elsewhere and handed across as a buffer |
| `bytes_crossed` | the payload bytes of those |

The figures appear in each shard's transport view (`ShoalPool::transport`, the fixture's
`TRANSPORT`), each shard's replication report, and the node's `Stats` as `replication_lanes`,
which `shoaladm stats --json` prints. On a node whose peers all name a slot, `requests_crossed`
stays at zero and `handed_out` equals `taken_in`.

### Item 236, fixed

One correlation-id counter is shared by every link of a shard's network, where each link had its
own starting at one. A late answer from a link replaced after an address change no longer
completes the new link's RPC of the same id ([Resolved #236](../appendix/resolved/replaced-link-answers.md)).

## Design choices

**The slot rides the hello, not a frame after it.** The peer hello had two reserved bytes, and
the slot is what the acceptor has to know before it reads anything else. A frame after the hello
would be read on the accepting executor, and every byte read there is a byte the adopting one
cannot see.

**No capability bit.** Nothing about a frame changes, and the handoff is the acceptor's choice. An
acceptor that ignores the slot serves the lane where it lands, with the hop. An old dialler writes
zero, which is no slot. A bit would only move `CAPABILITIES`, which mixed-version tests read.

**The acceptor finishes the handshake before it hands the lane over.** The hello is judged and
answered by the executor that read it, so a refused peer is refused exactly as before, and the
executor that adopts the lane receives one that is already let in.

**One owner of the descriptor at every instant.** `IntoRawFd` in the fork, rather than X11's `dup`
and close, means no window with two owners and no extra system call. An `OwnedFd` travels in the
message, so a message dropped on the way, at a shutdown, closes the connection and the peer dials
again. Nothing is leaked.

**A member's silence is judged on its own link.** Resolved #143 judges a peer silent by its link's
unanswered requests, by the node not being heard from, or by the kernel saying the path is cut off.
With several links to one node, `member_silent_for` reads the member's own link, because that is
the queue its RPC waits in.

- "Any of the node's links" would spread one busy link's silence ([item 215](../appendix/known-issues.md#215-a-replication-lane-busy-with-wide-rows-is-judged-silent-and-refuses-forwarded-writes))
  to every slot on its node.
- "All of them" would let a hop to a wedged slot wait out its whole deadline, #143's symptom.
- The kernel's verdict is about the path every link to the node shares, so it counts from any of
  them.
- `heard`, anything received from a node, stays per node. The lease's quorum check reads each
  voter the same way.

**Requests are still dispatched by their own head.** A lane served in place, from a peer that
names no slot, works as it always did. Correctness never depends on where a lane is read.

## Alternatives rejected

- **`dup` and close, as X11 did.** It leaves two owners for a moment and costs a system call. The
  fork's `IntoRawFd` is twenty lines.
- **A port or a listener per executor.** C2 rejected per-shard endpoints, and on 2026-10-09 the
  user declined ports per executor for clients. They reach the listener, the configuration, the
  inventory, the renderer and every firewall. A slot in the hello reaches nothing outside the peer
  protocol.
- **A reuseport BPF program choosing the shard.** The slot is inside the hello, which is under TLS
  on an encrypted cluster, and a dialler never learns which executor hosts a slot.
- **A cleartext prefix before TLS naming the slot.** A new frame on every lane, outside the
  encryption, for what two reserved bytes inside it already carry.
- **A capability bit, or a longer hello.** See *Design choices*. Neither buys anything over zero
  meaning none.
- **An acknowledgement from the adopting executor, with a fallback.** The accepting executor
  would keep the connection until the other confirmed, so two owners again. The mesh delivers a
  message to a running shard. A shard whose loop has ended is the limitation below, the same one
  its requests already meet.
- **Handing bulk lanes, data lanes and client connections now.** A data lane's bundles name many
  slots. A client connection is S13's to keep hopping. A bulk lane carries one slot's snapshot
  stream at a time and could be keyed the same way: it was left for later by the user's choice,
  and is [a todo](../appendix/todos.md#handing-a-bulk-lane-to-its-slots-executor).

## Limitations

- **Bulk lanes, data lanes and client connections still hop.** A snapshot chunk, a forwarded
  bundle and a client's object bytes are still read on whichever executor the kernel chose. Every
  client query is still coordinated by the shard that accepted its connection.
- **More links after a move, or with unequal slot counts.** A shard opens one link per member it
  has a group with. A move can give a group a member on another slot index, and a node with more
  slots than its peers has members on several. Each link has its own `replication_queue_bytes`.
  Links to members no group uses any more are kept until the address changes, as a link to a node
  was.
- **A lane is handed only at its start.** A lane already serving is never moved. Nothing needs
  that: a slot's host does not change while a process runs.
- **A lane handed to a shard whose loop has ended is not read.** The mesh's channels are unbounded
  and held at both ends, so the send succeeds. The peer's requests time out and it redials, and
  requests for that slot met the same fate before F84.
- **Item 215 is unchanged.** A busy replication lane is still judged silent the way it was. Per
  member, its judgement no longer spreads to the node's other slots once they have links of their
  own.

## Invariants to uphold

- **Nothing is read past the hello before a lane is given up.** The stream is non-buffered, the
  TLS handshake reads one record at a time, and the hello is read exactly. A buffered stream, or a
  handshake that reads ahead, would strand the peer's first requests on the wrong executor.
  `a_lane_given_up_after_its_hello_loses_no_frame` holds this.
- **One owner of the descriptor at every instant.** It leaves its stream through `IntoRawFd`, rides
  the message as an `OwnedFd`, and becomes a stream again only on the executor that will drive it.
- **A lane is adopted only on the executor that serves it.** A glommio stream registers with the
  reactor of the thread it is made on.
- **Every request on a replication link names the link's slot.** The RPC methods take no slot of
  their own, so `requests_crossed` is zero by construction and not by luck.
- **A slot's host never changes while a process runs.** A rehome runs before any shard starts.
  A lane handed to a slot's executor stays right for its life.
- **A correlation id names one RPC across every link of a shard's network.** This is item 236's
  fix. A second way of minting replication ids draws on the same counter.
- **The hello's bytes 62 and 63 are the slot plus one, zero for none.** An old build writes zero,
  so a new acceptor reads none.

## Performance

**What it removes, by construction.** For every replication request on a follower, the read and,
under kTLS, the decryption now run on the core that applies it; before, they ran on another core
in all but one case in the number of executors. The request still passes through its shard's own
queue, which is a channel send on the same thread. What goes away is the crossing between cores:
the payload read on one core and touched again on another. What it adds is one branch in the
acceptor, once per connection, and no work on any request's path.

### The tmdb cluster

`tmdb_cluster.yaml`: europa (Ryzen 9 7945HX) and titan and hyperion (Ryzen Embedded V1756B), six
cores a node, a factor of three, holding the whole dataset. The nodes were built for znver1 at
F84's tree and every host kept its own governor (`powersave` on europa, `schedutil` on the Zen1
hosts). This is a check of correctness, not a measurement.

- **`verify --read quorum`, before**, on F83's build: all 1,187,691 movies, 1,188,009 release rows
  and 58,418 keyword partitions found, none missing or different.
- **`upgrade`**, one node at a time with the leader last, in 35 s. Each node's journal then named
  its replication lanes handed over: of the twelve each node accepts from its two peers' six
  shards, 10 or 11 were handed to the executor hosting their slot and the rest had landed on it.
- **A minute of writes** (`drive`, a quarter each updates and inserts) acknowledged 754,587
  inserts, about 12,400 a second beside as many updates. 32 writes were refused `NotLeader` while
  the balancer handed leads back after the restarts, as a rolling restart always does. Every
  insert read back at quorum and through each member at `One`.
- **The rolling window, counted.** Hyperion was upgraded first, while titan and europa still ran
  F83's build, whose replication links name no slot. Its counters showed those twelve lanes
  `unnamed`, served where they landed with the hop: 2,034 requests and 6.4 MiB read for another
  executor's slot before the two were upgraded. Titan showed six, from europa, the last upgraded.
  Europa showed none.
- **Every member on F84.** Over 30 s more of writes, 592,347 inserts, the three nodes read 64,544,
  29,647 and 37,003 more replication requests on the executor hosting their slot and **none**
  for another's. Every insert read back at quorum, and `verify --read quorum` found the whole
  dataset again.
- **A restart.** Titan was restarted. Its new process handed the ten lanes that landed on the
  wrong executor and took the other two in place, and europa and hyperion each took in six more,
  titan's new shards'. Over 30 s more of writes, 535,665 inserts, no request crossed on any node,
  and every insert read back at quorum.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `tcp_stream_into_raw_fd_moves_to_another_executor` | glommio fork, `net/tcp_socket.rs` | A stream cannot give up its descriptor, or loses the bytes queued behind it, or is duplicated |
| `a_hello_names_the_slot_its_frames_are_for` | `shoal-proto` peer tests | The slot is not written as itself plus one in the reserved bytes, or does not read back |
| `a_hello_from_before_the_slot_names_none` | `shoal-proto` peer tests | A hello with zeroes there names a slot, or the slot moves another field |
| `the_listener_hands_a_replication_lane_to_its_slots_host` | `shoal-core` peer tests | A lane is handed when it should stay, or stays when its slot is hosted elsewhere |
| `a_hello_names_its_slot_to_the_acceptor` | `shoal-core` peer tests | The dialler's slot does not reach `Accepted`, or the ack names one |
| `a_lane_given_up_after_its_hello_loses_no_frame` | `shoal-core` peer tests | A request the dialler sent behind its hello is lost or read on the wrong executor, or the descriptor changes |
| `what_a_query_carries_between_shards_is_send` | `shoal-core` messages | A handed lane carries something that may not cross threads |
| `an_answer_completes_the_rpc_on_its_own_slots_link` | `shoal-core` replication network | Two members of one node share a link, or an answer completes another link's RPC |
| `a_late_answer_from_a_replaced_link_completes_nothing` | `shoal-core` replication network | Item 236: a replaced link's late answer completes the new link's RPC |
| `replication_lanes_are_read_on_the_executor_hosting_their_slot` | `shoal/tests/cluster_fixture.rs` | Any replication request is read on another executor than its slot's, a lane names no slot, or a lane handed out is never taken in |
| `replication_lanes_are_handed_over_under_ktls` | `shoal/tests/cluster_fixture.rs` | The same under mutual kTLS, with every note read back at quorum; skips by name without the kernel's TLS module |
| `a_restarted_nodes_lanes_are_redialled_and_handed_again` | `shoal/tests/cluster_fixture.rs` | A lane dialled again after a restart is not handed over, on the restarted node or a survivor |

With `destination` made to keep every lane where it landed, all three fixture tests fail. On the
first, node zero had read 512 replication requests for another executor's slot against 174 for
its own.

## Related

- [S1](../object-storage/prerequisites.md#optional), the row this delivers, and
  [S13](../object-storage/isolation.md#a-lane-for-object-bytes), the object lane that will hand
  its connections the same way.
- [X11](../object-storage/streamed-bodies.md#4-a-connection-handed-over-and-bytes-that-hop), which
  measured the handoff and the hop.
- [F40](replication.md), the replication lane; [F38](inter-node-transport.md), the peer listener
  and its lanes; [F47](local-rehome.md), slots and the executors hosting them.
- [Resolved #143](../appendix/resolved/silent-partition-hops.md), the silence a member is judged
  by, and [Resolved #236](../appendix/resolved/replaced-link-answers.md), fixed with this.
- [F14](encryption-in-transit.md), the kTLS a handed lane keeps.
