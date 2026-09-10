# C3. Membership and failure detection

## Context

A cluster has to agree on who is in it, and disagree safely about who is reachable. The two are
different questions with different answers: membership changes rarely, by decision, and has to be
the same everywhere; reachability changes constantly, by observation, and is legitimately
different from where each node stands. Putting both in one mechanism — gossip, in Cassandra's
lineage — is what makes a partition look like a membership change and start moving data. This
page keeps them apart: membership is Raft, reachability is a detector, and the only thing that
turns the second into the first is a timeout an operator chose.

This is also the page that meets **R3**. A node that is down loses nothing by being down.

## What exists today

**A join that is ignored.** Every shard broadcasts `ServerMsg::Join(self.info)` when it starts
(`Shard::join_cluster`, `shoal-core/src/server/shard.rs:1109`), and `Ring::add` recognises the
ones it already placed and warns about any other:

```rust
event!(Level::WARN, msg = "Ignoring a join from an unknown shard, as rebalancing is unimplemented", ..)
```

`shoal-core/src/server/ring.rs:121-133`. [Partitioning](../architecture/partitioning.md#why-every-shard-agrees)
calls it "a seam for the multi-node case, not a working membership protocol", which is exactly
right.

**No liveness at all.** `Ping` and `Pong` are message types 7 and 8 with nothing behind them
([Wire Protocol](../architecture/wire-protocol.md#the-header)); the client pool's health check
calls `peer_addr()` and detects nothing ([D6](../direction/connection-pool.md#health-checks-that-work));
a shard that dies is reported to nobody
([item 58](../appendix/known-issues.md#58-a-shard-that-dies-is-not-reported-to-whoever-started-the-pool));
and `ShoalPool::start` returns before any shard is ready, so there is no moment at which a node
could say it is up ([TODOs](../appendix/todos.md), "A readiness signal in `ShoalPool::start`").

**No consensus library in the tree.** `Cargo.lock` names no raft, paxos or gossip crate. This page
adds the first, and [the overview](overview.md#how-a-c-page-is-written) says how an added
dependency has to be argued for.

## The design

### One Raft group of nodes

Every node is a member of one `openraft` group whose state machine is small and changes rarely:

```rust
pub struct ClusterState {
    pub cluster: ClusterId,
    pub members: BTreeMap<NodeId, Member>,
    pub tablets: TabletMap,                 // C4
    pub version: u64,                       // the log index of the last change
}

pub struct Member {
    pub addr: SocketAddr,                   // cluster.advertise : cluster.port
    pub state: MemberState,
    pub shards: u16,                        // how many shards it runs, for the rebalancer's weights
    pub joined: u64,                        // the log index it joined at
}

pub enum MemberState { Joining, Up, Down, Leaving, Removing, Removed }
```

Log entries are the verbs of every later page: `Join`, `SetState`, `Leave`, `MoveTablet`
([C8](rebalancing.md)), `SetPrimary` ([C7](failover.md)). Nothing on a query path ever proposes
one; they come from the control plane on cpu 0 ([C1](node-identity.md#the-control-plane-thread)),
and the leader's control plane is the only one that proposes `SetState`, `MoveTablet` and
`SetPrimary`.

**Voters and learners.** The first five members are voters; every member after that is a learner
that receives the log and votes on nothing. Scylla's rule, and for Scylla's reason — a vote is a
round trip to a majority, and a majority of thirty is not a faster quorum than a majority of five,
only a larger one. When a voter is removed the leader promotes the oldest learner.

### Joining

```
 joiner                         seed                          leader
 ───────                        ─────                         ──────
 PeerHello {cluster: None, node} ─▶
                              ◀─ PeerHelloAck {cluster, node: seed}
 (adopts cluster id, writes it to the marker)
 Raft(Join {node, addr, shards}) ─▶  forwards to ─────────────▶ proposes Join as learner
                                                                 commits; member is Joining
 ◀────────────── log replication, snapshot if far behind ─────────────
 (holds the full ClusterState; has no tablets)
                                                                 promotes to voter if < 5 voters
                                                                 sets Up
 (binds the client listener; serves nothing yet — C8 assigns)
```

A joiner with a marker naming a cluster id, and seeds that answer for another, is refused by the
handshake ([C2](transport.md#the-peer-handshake)) and never reaches this diagram. A joiner whose
seeds do not answer waits, and does not bind its client port until it has a map
([C1](node-identity.md#the-control-plane-thread)). A bootstrap node — empty `seeds`, no cluster id
in its marker — mints the cluster, proposes itself, and is `Up` alone.

**A node is in the cluster with no tablets** until the rebalancer gives it some. That is
deliberate: joining and taking load are different steps, so a node can be added, inspected, and
only then loaded, and a join that fails half way has moved nothing.

### The state machine of a member

```
              Join committed             detector: phi crossed          leader records it
  ┌─────────┐ ───────────────▶ ┌────┐ ─ ─ ─ (local only) ─ ─ ─ ▶  ┌──────┐ ─────▶ ┌──────┐
  │ Joining │                  │ Up │       "Unreachable"           │ (Up) │        │ Down │
  └─────────┘                  └────┘ ◀──────────────────────────  └──────┘ ◀───── └──────┘
                                  │            pings answer again                      │
                                  │ operator: Decommission                             │ auto_remove_after, or operator: Remove
                                  ▼                                                    ▼
                              ┌─────────┐   tablets drained (C8)    ┌──────────┐  tablets re-homed (C8)   ┌─────────┐
                              │ Leaving │ ─────────────────────────▶│ Removing │ ────────────────────────▶│ Removed │
                              └─────────┘                           └──────────┘                          └─────────┘
```

Two of these are not states in the log. **`Unreachable` is a local verdict**, held by each node's
detector about each peer, never proposed, never agreed. **`Down` is the cluster's verdict**,
proposed by the leader when its own detector and a majority of the members' reports agree, and it
is the only reachability fact in the log. The distinction is what stops a node on the wrong side
of a partition from failing over the majority: it can call everyone unreachable, and it cannot
make that stick.

What each state permits:

| State | Holds tablets | Primary for tablets | Answers reads at `One` | Counted toward quorum |
| --- | --- | --- | --- | --- |
| `Joining` | no | no | no | no |
| `Up` | yes | yes | yes | yes |
| `Down` | **yes** | until `primary_failover_after` ([C7](failover.md)) | no — nobody routes to it | no — a write does not wait for it |
| `Leaving` | yes, draining | yes, until moved | yes | yes |
| `Removing` | being re-homed | no | no | no |
| `Removed` | no | no | no | no |

**`Down` holds tablets.** That row is R3. A down node's data is exactly where it was, its replica
sets still name it, and when it returns it catches up ([C7](failover.md#a-returning-node)). The
only thing that moves off it is the role of primary, after `primary_failover_after`, and that is a
map edit with no bytes behind it.

### Failure detection

A **phi-accrual detector** (Hayashibara et al., the one Cassandra and Akka use) over
`PeerPing`/`PeerPong` on the control-plane connection, every node to every other node, every
`failure_detector.interval_ms`. Each node keeps, per peer, a window of inter-arrival times, and
computes at any moment the probability that a pong this late means the peer is gone, expressed as
phi. `phi_threshold: 8.0` means "one in a hundred million that a live peer is this late", and it
adapts: a link whose RTT has always been noisy needs a longer silence to reach the same phi than a
link that has always been quiet.

**It is the control plane's connection that is pinged, not the data plane's.** The shards' peer
connections ([C2](transport.md#who-owns-a-connection)) carry queries and replication and are busy
exactly when liveness matters most; a ping queued behind a megabyte of `Replicate` would measure
the queue. The control plane's connection carries Raft traffic and pings and nothing that
competes with them.

A shard's evidence still counts. A replication ack that has not arrived, a forward that timed
out, a peer connection that reset — each is reported to the control plane
([C1](node-identity.md#the-control-plane-thread)) and folded into the detector as a late
arrival, never as a verdict. A shard cannot declare a node unreachable; it can only make the
detector more suspicious sooner.

**Every node reports its verdicts to the leader** in its Raft heartbeat responses (openraft lets a
follower attach application data to its replies, which is the thing to verify in its source — see
below). The leader proposes `SetState(Down)` when it and a majority agree a node is unreachable,
and `SetState(Up)` when a majority say it answers again.

### What follows from `Down`, and when

Nothing, immediately. Then, by two timers the operator set:

| After | The leader proposes | Page |
| --- | --- | --- |
| `primary_failover_after` | `SetPrimary` for every tablet the down node was primary for | [C7](failover.md) |
| `auto_remove_after`, if set | `Leave`, which starts the drain — except a node that is `Down` cannot drain, so this is `Removing`: its tablets are re-homed from their other replicas | [C8](rebalancing.md#auto_remove_after) |

Between those two, a down node costs the cluster one replica of every tablet it holds — writes
still reach quorum on the remaining replicas of an RF=3 set, reads at `One` route elsewhere, and
nothing is copied. **The cluster runs degraded and does not repair itself**, because repairing
itself is what moves terabytes over a blip. When `auto_remove_after` is unset, it runs degraded
until an operator acts, which is what Cassandra and Scylla do and is the default here.

### `openraft` and the runtime

Verified against `openraft`'s source and manifest on **2026-09-10**, not against its
documentation, following the rule [Direction](../direction/overview.md#the-recommended-order)
learned from D4:

- **Stable is 0.9.25** (2026-07-28). Its `AsyncRuntime` trait is runtime-agnostic: ten
  associated types (`JoinHandle`, `Sleep`, `Instant`, `Timeout`, `ThreadLocalRng`, the oneshot
  pair, and their error types) and eight methods (`spawn`, `sleep`, `sleep_until`, `timeout`,
  `timeout_at`, `is_panic`, `thread_rng`, `oneshot`). No tokio type appears in the trait's
  signatures — only in `TokioRuntime`, the one implementor it ships. A `singlethreaded` feature
  drops the `Send`/`Sync` bounds and expects a `spawn_local`. **But `openraft/Cargo.toml` at
  v0.9.25 lists `tokio = { workspace = true }` unconditionally**, so 0.9 always links tokio,
  whether or not it runs on it.
- **0.10.0-alpha.34** (2026-08-14) moves the runtime into `openraft-rt`, makes tokio optional
  behind a default-on `tokio-rt` feature, widens the trait with `Mpsc`, `Watch` and `Mutex`
  abstractions, and says in its own words that it "allows Openraft to work with different async
  runtimes (tokio, compio, monoio, etc.)" — monoio being an io_uring, thread-per-core runtime of
  the same shape as glommio. It is an alpha.
- **Linking tokio in `shoal-core` costs nothing new.** `cargo tree -p shoal-core -i tokio`
  already resolves it through `opentelemetry-otlp → reqwest` and `tonic`. The crate split
  ([F15](../features/client-server-split.md)) is about a *client* not linking glommio; it never
  claimed the server does not link tokio, and it does.

**The recommendation is 0.9.25 with `TokioRuntime`, on a current-thread tokio runtime owned by the
control-plane thread on cpu 0.** The control plane is not a hot path; the dependency is already in
the graph; and `TokioRuntime` is the only implementor that exists. A glommio implementation of
`AsyncRuntime` — `spawn_local`, `glommio::timer`, a `futures` oneshot — under `singlethreaded` is
the upgrade path, recorded rather than rejected, and becomes tokio-free once 0.10 stabilises.

**Two things the source has not settled and M3 must read before it starts:**

1. **How `RaftNetwork` is driven.** Raft messages have to travel on [C2](transport.md)'s
   control-plane peer connection as `Raft` frames, not on sockets openraft opens itself. The
   trait is a set of async methods the application implements, which is the right shape, but
   whether it allows a follower to attach the detector's verdicts to its heartbeat reply — this
   page's mechanism for reporting reachability — is a question about `AppendEntriesResponse`'s
   extensibility, and the answer decides whether verdicts ride on Raft or on a separate `Admin`
   frame.
2. **Where `RaftLogStorage` and `RaftStateMachine` (`storage-v2`) may block.** The control-plane
   thread is off every shard's core, so `std::fs` writes there block nothing that matters — but
   openraft calls these from its own tasks on the runtime it is given, and a current-thread
   runtime blocked on an `fdatasync` stalls its own election timer. The log is tiny and the
   snapshot is 112 KiB; if the source says the calls are on the runtime's thread, they go through
   `spawn_blocking`, which a current-thread runtime still provides.

If either answer is bad enough, the fallback is a hand-written Raft for this one state machine —
a few thousand lines, well understood, and a well-known place to be subtly wrong. It is recorded
so it does not have to be re-argued.

## Alternatives rejected

**Gossip for membership (SWIM, Cassandra's gossiper).** A second consensus-shaped mechanism
beside Raft, eventually consistent about the one thing that must not be — who owns which tablet.
Scylla moved membership *into* Raft in 5.x for this reason, and its topology-over-Raft work is
the model here ([C12](prior-art.md#scylladb)).

**Raft's own heartbeats as the failure detector.** They only detect the leader, in one direction.
A follower learns nothing about another follower from them, and the leader learns only that a
follower stopped acking, which it cannot distinguish from a slow disk.

**A fixed timeout as the failure detector.** Rejected in favour of phi-accrual, which is what
`failure_detector.phi_threshold` configures. A fixed timeout is one point on phi's curve chosen
before the link's variance was known; on a link whose RTT swings, it either fires on every swing
or fails to fire on a real death.

**Pinging on the data-plane connections.** They are busy exactly when liveness matters, so the
ping would measure the replication queue.

**Every node a voter.** A majority of thirty is not more available than a majority of five, and
every proposal waits for it.

**Marking `Down` from one node's detector.** The partition case: one node on the wrong side calls
everyone down and fails everything over to itself. `Down` needs a majority for the same reason a
Raft leader does.

**Auto-removal on by default.** The user's decision was to have the knob; the default is off,
because a partition longer than the timeout would remove a live node from the majority's cluster,
rebalance every byte it held, and then meet it again when the partition heals — with a cluster id
that matches and a topology version that does not. [C8](rebalancing.md#auto_remove_after) says
what happens then.

## What it costs

- **One ping per peer per `interval_ms`** on every node — `N − 1` small frames every half second,
  on the control-plane connection, off every shard's core.
- **A Raft round trip per membership change**, which is rare, and per `SetState`, which is rarer
  than it sounds because `Unreachable` is local and proposes nothing.
- **The `openraft` dependency**, its log under `<latency_sensitive.path>/cluster/`, and a tokio
  runtime on one thread.
- **A startup wait for a joiner**, until it holds the log.

## What it breaks

- **`Ring::add` and `ServerMsg::Join`** are deleted. The seam did its job by naming where this
  page goes.
- **A node with seeds does not serve immediately.** Every test that starts a server and connects
  after a fixed sleep has to wait for readiness instead — which is the probe
  [F8](../features/purpose-built-workloads.md) already built and `shoal/tests/utils.rs` has not
  adopted.
- **`ShoalPool::start` gains a return value it never had**: the control plane knows when every
  shard is up, which closes [item 58](../appendix/known-issues.md#58-a-shard-that-dies-is-not-reported-to-whoever-started-the-pool)
  and the readiness entry in `todos.md` as a side effect.

## Invariants to uphold

- **`Unreachable` is never in the log and `Down` is never local.** A node's detector may say
  anything; the cluster's state changes only by a committed entry, and only the leader proposes
  one, and only with a majority's evidence.
- **`Down` moves no tablet.** The map entry for a tablet names the same replica set before and
  after a node goes down. Only `SetPrimary` (after `primary_failover_after`) and `MoveTablet`
  (after removal) edit it, and neither is caused by `Down` alone.
- **A shard's evidence is evidence, not a verdict.** A timed-out ack makes the detector more
  suspicious; it never bypasses it.
- **The control plane proposes; shards never do.** No query path holds a Raft handle.
- **Pings travel on the control-plane connection.** Moving them onto a data-plane connection makes
  the detector measure the queue in front of them.
- **Voters are capped at five.** A sixth member joins as a learner.

## Prerequisites

[C1](node-identity.md) for the identities and the thread; [C2](transport.md) for the `Raft`,
`PeerPing` and `PeerPong` frames and the control-plane connection they ride on. The two
`openraft` questions above, read from source, before M3 starts.

## How it would be measured

Membership is not on a query path and no workload should see it. The claim is that
`macro/cluster/nodes/3/rf/1/cl/one` — three nodes, one copy, reads at `One` — is within the noise
band of `nodes/1/rf/1` for a query whose partition is local, and differs from it only by the hop
[C2](transport.md#how-it-would-be-measured) measured for one that is not. If the detector's pings
show up in that number, they are on the wrong connection.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `three_nodes_form_a_cluster_from_one_seed` | Every node's `Members` admin reply names the same three ids, all `Up`, at one version | M3 |
| `a_fourth_node_joins_as_a_learner_with_no_tablets` | Voter count stays at the bootstrap three; the joiner holds no tablet | M3 |
| `every_node_holds_the_same_map` | The `Topology` admin reply is byte-identical on every node after bootstrap | M3 |
| `a_killed_node_is_unreachable_then_down` | `SIGKILL` one; the others report `Unreachable` within `interval × phi` locally and `Down` at the next committed entry | M3 |
| `a_paused_node_is_a_partition_of_one` | `SIGSTOP` behaves as `SIGKILL` above; `SIGCONT` brings it back to `Up` | M3 |
| `a_restarted_node_returns_to_up` | Kill, restart from the same directory; same `NodeId`, state `Up`, tablets unchanged | M3 |
| `a_minority_cannot_mark_the_majority_down` | Partition one node from two; the one's detector says `Unreachable` for both; the log never records either as `Down` | M3 |
| `down_moves_no_tablet` | Kill a node, wait past `primary_failover_after`, diff the map: only `primary` fields changed, every replica set names the same nodes | M6 |
| `a_shard_timeout_makes_the_detector_suspicious_not_certain` | A paused *shard* (not node) raises phi for its node and does not by itself reach the threshold | M3 |
| `the_cluster_log_lives_under_the_latency_path` | The Raft log and snapshot are where the page says | M3 |

## Related

- [C1. Nodes, identity, and the cluster configuration](node-identity.md) — the thread and the block
- [C4. The replicated tablet map](tablet-map.md) — the largest thing in the state machine
- [C7. Primary failover](failover.md), [C8. Rebalancing](rebalancing.md) — what `Down` eventually causes, and what it does not
- [C12. Lessons from other clusters](prior-art.md#scylladb) — Scylla's move of membership into Raft; Cassandra's phi-accrual detector
- [Partitioning](../architecture/partitioning.md#why-every-shard-agrees) — the `Join` seam this replaces
- [item 58](../appendix/known-issues.md#58-a-shard-that-dies-is-not-reported-to-whoever-started-the-pool) — closed as a side effect
- [D6. A production connection pool](../direction/connection-pool.md#health-checks-that-work) — the client-side detector this is not
