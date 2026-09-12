# F39 — Membership

## Context

[M3](../distributed/milestones.md#m3-membership) is the milestone where the cluster agrees on
who is in it. After [F38](inter-node-transport.md) a node could speak to a node, but only to the
nodes a `placement:` block named: every node was a consensus group of one, `seeds` was refused
at startup naming this milestone, both handshake judges resolved a peer from the static file,
and the ring was built once in `Shard::new` and never replaced. Nothing could join, nothing could
be fenced, nothing could be called down, and no client could be told where the cluster was.

The gate asks for embedded control membership under an explicit three or five voter policy,
learners, durable placement intent, stable table identity, replica readiness distinctions,
direct control traffic, freshness-aware status reports, shard health, duplicate-node fencing and
authorized, versioned admin operations - thirteen acceptance rows across
[C1](../distributed/node-identity.md), [C2](../distributed/transport.md),
[C3](../distributed/membership.md), [C4](../distributed/tablet-map.md),
[C9](../distributed/operations.md) and [C13](../distributed/protocol.md), and an evidence line
that wants healthy metadata agreement, minority isolation and restart tests, the topology fanout
budgets measured, and a fourth node leaving a three-voter policy at three.

Four decisions were taken with the user before anything was built, and the page records them
as decisions rather than derivations: **a cluster at a replication factor above one serves one
copy and reports the gap** rather than refusing to serve until the copies exist; **a write under
a quorum consistency is refused when the cluster does not have enough nodes up to meet it**;
**the failure detector is phi-accrual on the control leader, which commits `Down` and `Up`
through the group**; and **the highest incarnation of a node identity wins**, with the lower one
fenced. The benchmarks got a measurement and no new arm.

## What it does

**A node joins.** A `cluster:` block with `seeds` and no `bootstrap` claims its directory with
`ClusterIntent::Join`: the marker is written at **format 3** with a `mode` of `joining`, a
minted node id and no cluster id. The control thread dials the seeds' control addresses in
order with a hello that names no cluster, which a listener admits on the control lane alone
and lets ask two things, `Join` and `Ping`. The leader admits it: `add_learner`, then a
committed `Admit`, one admission at a time since the group takes one membership change at a
time, and a joiner that arrives while another is being admitted is told to retry. The joiner
adopts the cluster id into its marker once - `StorageMeta::adopt_cluster` moves `joining` to
`cluster` and refuses to do it twice - and is `Joined` once its own record is applied. A
directory that never finished joining is refused by `bootstrap: true` and by a standalone
configuration, each naming the joiner. A member restarted with unreachable seeds does not join
again: its peers are the applied members, and it is `recovering` until a leader observes it.

**The voter policy is enforced.** A joiner is a learner. The leader promotes learners while the
committed voter count is below `control_voters` and only while the effective and committed
memberships agree, so a joint configuration is never stacked on another: `add_learner` with
`blocking` catches the learner up, then `change_membership` adds it as a voter with the others
retained. A fourth data node under a three-voter policy stays a learner; `SetControlVoters` is
an admin operation over {1, 3, 5} and the next tick promotes to the new count. Killing the
leader while a joint configuration is in flight was what made the fixture wait for `joint:
false` before it calls a cluster settled - a joint configuration needs a majority of the old
voters and of the new, which the survivors of such a kill may not have.

**A node identity is fenced by its incarnation.** Every claim of an established directory bumps
the marker's `incarnation`, and the number rides in the committed member record, the hello, the
pong, every status report and every proposal. The state machine's `observe` rule is the whole
of the policy: a lower incarnation than the committed one is `Fenced`; an equal one from a
different control address is `Fenced` too, since it is a second copy of one run; an equal one
from the same address is a re-observation; a higher one supersedes the record and remembers
the old. A running node that sees a higher incarnation of itself committed stops with
`Fenced`, a report or a proposal from a superseded run is answered fenced so that run stops,
and a hello below the committed incarnation is refused `Fenced` on every lane. A directory
copied while its node runs comes back one run later than the original and wins; the original
stops. The lock on the directory still refuses two processes on one path.

**Membership is the map.** A `TabletMap` (`shoal-core/src/server/map.rs`) is built by the
control thread from the applied state on every version: the cluster, the leader, the members
with their endpoints, shard counts, roles, health, incarnations and failed shards, the
placement as an ordered node list, the tables with their ids, the desired replication factor,
the two consistencies and the admins. It is pushed whole to every shard through a sink the
pool attaches once the shards are up; a shard installs a newer version between two messages,
rebuilds its ring from the order with the M2 rule - tablet `t` on `nodes[t % N]`, then shard
`(t / N) % shards` - and never routes against half a map. Before `Initialize`, the bootstrapper
places every tablet on itself, which is the standalone ring with a name on it, and a joiner
holds no tablets and answers every data query `NotInitialized`. `Initialize { nodes }` is the
one explicit placement, applied once in the order given; a second is refused naming
[M9a](../distributed/milestones.md#m9a-safe-replica-migration). `cluster.dial` says where a
member is dialled instead of where it advertises, for a split-horizon network and for the
fixture's directional faults.

**A table has an identity.** `TableId` is the gxhash of the table's name under a frozen seed,
emitted by the derive as `table_ids()` and `table_id()` and never the enum's discriminant, so
reordering a schema moves nothing. The bootstrapper's ids are committed by `Initialize` and
the fixture pins the two literals of its schema.

**Clients hear the topology.** A client connection subscribes as it opens, after the handshake
and any authentication, with a `Topology` request under a nil id; the accepting shard answers
with the current frame and pushes one for every newer map it installs, always under the nil id
since a push answers no query. A run of frames queued to one connection is folded to the newest
before any is written. The client library holds the newest frame across its connections:
`Shoal::topology()` returns it and `topology_changed(since)` waits for a version past one. A
`TopologyFrame` names every member's client, data and control endpoints, role, health,
incarnation and failed shards, the placement order, the factors, the consistencies and the
tables. A peer connection never subscribes. A client from before this feature never sends the
request and is never sent a frame; the wire version did not move.

**Admin rides the client connection.** An `Admin` frame carries an `AdminRequest { op,
expected_version, kind }` as JSON under a query id, and the answer comes back as an
`AdminResponse` under the same id. The accepting shard refuses a mutation whose principal the
map's admins do not name and hands the rest to the control thread, which answers `Members`,
`Readiness` and `Detector` from the applied state, refuses a stale `expected_version`, proposes
`Initialize` and `SetControlVoters` through the leader, and answers an operation id it has
already applied from the applied state before any of that - so an identical retry after the
version moved is `Repeated`, not stale, and costs no log entry. Every mutation is logged with
its principal, operation, expected version and kind. `Shoal::admin` is the client call;
`ShoalPool::admin` is the process's own trusted seam, which the fixture and the benchmark
harness initialize with.

**A write needs its quorum.** The origin shard judges every bundle against the map it holds:
under the cluster's write consistency a write needs `One` → 1, `Quorum` → `rf / 2 + 1`, `All`
→ `rf` members `Up`. The writes of a bundle that falls short are refused by name with
`QuorumUnavailable` - "writes need 2 up nodes for quorum at rf 3; have 1" - before anything is
routed, and the reads in the same bundle are served. A fresh one-node cluster at the default
factor of three refuses writes until two more nodes are up. A `Down` member does not count.

**Readiness has three parts.** `ShoalPool::readiness()` and the `Readiness` admin read report
`process`, `control` (`joining`, `recovering` or `joined`, with the leader and whether this
node is it, the voter and learner counts), and `data`: whether the placement is initialized,
whether this node is placed, how many members are up, the desired and active factors, whether
default writes are admitted - the same `write_admission` on the same map the shards judge by,
so the two cannot disagree - and this node's failed shards. `Ready` no longer implies a
leader: a control thread is ready once its store is open, its group built and its listener
bound, and a fresh bootstrap once its own `Bootstrap` has committed.

**The leader detects failure.** Every member sends the leader a `StatusReport` at
`failure_detector.interval_ms` over the control lane - its incarnation, a sequence, its
topology version and applied index, its failed shards and what its own pings learned. The
leader keeps the intervals between each member's fresh reports, fits a normal distribution to
the last `window` of them with the deviation floored at a quarter of the interval, and at
every tick computes phi, `-log10` of the probability that a report arrives later than now. Past
`phi_threshold` it proposes `SetHealth Down` with a fresh episode; the member's next fresh
report proposes `Up`. One verdict is in flight per member, the leader never judges itself, and
a minority can never call anybody down since it cannot commit. A report is fresh when its
sequence is above the last from the same run; a replayed or reordered one is counted as
`stale_ignored` and changes nothing, and one from an older run is answered fenced. A new leader
seeds every up member with the expected pace and a grace of five intervals, so the election
calls nobody down and a member that never reports to it is suspected once the grace has
passed. A dead shard is reported and committed separately as `ReportShards`, so a member with
a dead shard stays `Up` with that shard marked failed. The control thread pings every member at
`transport.ping_interval` into a local reachability view that is reported and never proposed,
which closes [item 96](../appendix/resolved/ping-interval-consumer.md).

**The control thread is one loop.** Every producer - the pool's requests, admin calls, inbound
membership RPCs, the report and ping timers, the metrics watch, the applied-index hook, the
outcomes of spawned work - posts an `Event` on one channel, and the loop owns the state. A
proposal goes through `propose`: this node's own group first, which waits out a lease that is
still being established, then the leader the write named, following a few hints. Leader restore
is off, so a node that led when it stopped comes back a follower. A failed observation waits
before the next metrics change may retry it.

**The fixture stages a membership cluster.** Node zero's marker names the cluster; every other
child's is a `joining` marker with a pre-minted id and node zero's control address as its seed;
the lane proxies are per direction and each child's `dial` map points at its own set, so a
test cuts, delays or heals one direction into one node. A cluster is initialized once every
child has joined unless the builder says `initialize(false)`. `Cluster` keeps every child's
staging and can restart, restart with other seeds, start a deferred node, kill, clone a
directory, spawn the clone, isolate and heal; a child answers `MEMBERS`, `READINESS`, `MAP`,
`INITIALIZE`, `SET_VOTERS`, `ADMIN`, `INCARNATION`, `LOG_LEN`, `FAIL_SHARD` and `STALE_REPORT`
beside M2's commands, and `SHOAL_CHILD_LOG` writes each child's log to a directory.

**The benchmark harness stages joiners.** `shoal-workload serve --staged` children join node
zero through their seeds, node zero initializes the placement through its admin seam once every
member is up, and the cluster record carries the committed members, the map version and the
voter and learner counts beside the initialization order. `shoal-spike fanout` prices the
topology push and the report traffic.

## Design choices

- **Seeds are control addresses.** C1 said the data peer seed endpoint would return the control
  endpoint during discovery; the joiner's only conversation before it is a member is on the
  control lane, so the seed is that lane's address and a seed that is a data port is refused
  `LaneRefused` naming the control port. One fewer hop and one fewer thing a listener has to
  answer for a stranger.
- **Marker format 3 adds `mode` and `incarnation` and nothing else.** A joiner's directory
  before admission has a node id and no cluster, which at format 2 was indistinguishable from
  a standalone directory; the mode tells them apart and decides every refusal. The incarnation
  is bumped by every claim on an established directory because the claim is the one moment a
  run provably begins. Format 2 is read, its mode inferred and its incarnation zero, and the
  first rewrite writes 3; the rewritten fields are now `topology`, `incarnation` and a joiner's
  one-time `cluster`, and identity, shard count and layout stay write-once.
- **`TableId` is the hash of the name.** [P2](../distributed/protocol.md#the-contract) forbids
  the enum discriminant, since reordering a schema would rename every stream on disk. The seed
  is frozen beside the function and the literals are pinned in a test; the model's
  `TableId(u32)` is its own index space and stays.
- **The map is an ordered node list, pushed whole.** At M3 a map is the members, the policy and
  the `Initialize` order; every shard rebuilds its ring from the same rule, so a push is a few
  kilobytes and not 4096 records, and a client is handed a frame it can hold as it is. Per-tablet
  records arrive when tablets move (M9a), and a delta is the day a whole map is too large.
- **Bodies are JSON** for `Topology`, `Admin`, `AdminResponse` and every control-lane RPC, as
  the control lane already was. Query bytes stay rkyv. A topology frame is read by an operator
  as often as by a program, and a control RPC is never on a query path.
- **Admin rides the client connection.** The principal the connection authenticated as is what
  authorizes a mutation, so there is one credential store and one listener; a separate admin
  port would be a second authentication design.
- **`Ready` does not imply a leader.** A member whose peers are all gone still comes up, reports
  its identity and log, and never re-initializes; readiness says `recovering` until a leader
  observes it. A `Ready` that waited for a leader would make a restart of a minority hang.
- **The bootstrapper is placed on itself before `Initialize`.** A one-node map is the
  standalone ring, which is F38's invariant, so a cluster of one serves from its first second
  and data written before the placement is initialized lives where the standalone rule put it.
- **`cluster.dial` is a real setting.** Q11's private-address behaviour and the fixture's
  per-direction faults are the same shape: this node dials that member somewhere other than
  where it advertises.
- **One fan-in loop, no `select!`.** Anything that awaits is a spawned task holding `Rc` clones
  for a short borrow, posting an `Event` back, so no `RefCell` is ever borrowed across an
  await on the control core.
- **Status reports are a `ControlKind`.** `MessageType::StatusReport` (19) stays reserved: a
  second framing for one JSON body over a lane that already frames control requests would be
  a second codec for nothing.
- **Leader restore is off.** openraft would restore a node that led when it stopped as the
  leader of its old term, with no lease until a quorum answered; a directory copied from a
  leader would come back as a second leader of that term; and a member whose peers are gone
  would append a blank entry. A follower that stands for election is the ordinary case and the
  one the tests can reason about.
- **A write through this node's own group waits out the lease.** openraft answers a write on a
  leader whose lease is not established with an empty forward hint while its metrics name it
  as the leader. Read as "no leader" and retried at once, that was a busy loop on the control
  core that starved the links the acknowledgements ride, timed out the leader's heartbeats and
  never let the lease start. `write_here` polls at 50 ms instead, and a failed observation
  waits 250 ms before the next metrics change may retry it.
- **Only the leader judges health, and only through the log.** Phi-accrual gives a suspicion,
  not a verdict; the verdict is a committed entry a minority cannot write. A new leader is
  seeded rather than blind, and given a grace, so an election is not evidence of anything.
- **Highest incarnation wins.** The alternative - first to claim keeps the identity - would let
  a stale copy that happened to start first fence the real node. The newest run of a directory
  is the one an operator just started, which is the one they mean.
- **A repeated admin operation is answered from the applied state.** The state machine already
  answers a repeat as it did the first time, but reaching it costs a log entry and, since the
  version moved when the operation applied, an identical retry would be refused as stale before
  it was ever proposed. The control thread checks its operations first.

## Alternatives rejected

**Refuse to serve until the replication factor is met.** C4 allows a one-node control plane to
expose admin and readiness without pretending to a three-copy quorum, and the user chose to
serve one copy and report the gap: `desired_rf` beside `active_rf`, `default_writes` refused
by name. A cluster that served nothing until its third node arrived would have no way to load
its first table.

**Every member detects, or an all-members-majority detector.** C3 rejected it before this
feature did: a detector is not a consensus protocol, and a verdict that is not a committed
entry is one a partition can produce on both sides. The leader alone feeds the detector, and
what it decides goes through the log.

**Incarnation as process start time.** F38's provisional number told a restart from a reconnect
and nothing more; two copies of a directory started a millisecond apart would have had two
different, equally valid incarnations and no rule between them. A persisted counter bumped by
the claim is what a fencing rule can be written against.

**Per-tablet map records now.** 4096 records per table pushed to every shard and every client
on every membership change is Q13's fanout question asked before a tablet can move. The
ordered list carries exactly what M3 decides and nothing it does not.

**Push a topology frame to every client, unsolicited.** A client from before this feature reads
a frame type it does not know and closes the connection. The subscription is the client's
statement that it can parse one, and the wire version does not have to move.

**Retry a failed proposal on every metrics change.** It is the busy loop above, and it was
found by a test rather than by reasoning: a restarted pair spun the control core at an
attempt every two milliseconds until the run timed out.

**Keep openraft's leader restore.** It is an availability optimisation for a leader that
restarts quickly; against a copied directory it is a second leader of the same term, and
against a lost majority it is a blank entry appended by a node that should write nothing.

**Propose a repeated operation and let the state machine answer `Repeated`.** Correct and one
log entry per retry; the test that measures the log caught it.

**A second framing for status reports.** `StatusReport = 19` was reserved with that in mind;
the control lane already carries JSON requests with an id and a deadline, and a report is one.

**Seeds on the data port.** The data lane serves forwarded bundles and shares its listener with
every shard; a stranger's first question belongs on the lane the control thread owns.

## Limitations

- ~~**A replication factor above one is a desired factor.** Placement is one owner per tablet;
  `active_rf` is 1 wherever the node is placed and readiness says so. Replication is M4's.~~
  Since [F40](replication.md) the factor is served: `active_rf` is the smaller of the desired
  factor and the placement's size, and every tablet has that many copies on distinct nodes.
- **Data loaded before `Initialize` on a cluster of more than one node stays where the
  bootstrapper's rule put it.** `Initialize` places the tablets over every node, and a tablet
  that moved to another node under the new rule is read there, where it has no data. Moving a
  tablet with data is M9a's; a cluster loads its tables after it initializes.
- **`Initialize` is applied once.** A second is refused naming M9a. Adding a node after
  initialization admits it, promotes it under the policy, and places nothing on it.
- **`Down` moves nothing.** Grace expiry, `Leaving`, `Removing` and removal are M9b's; the
  episode is minted and recorded so that they have something to name.
- **A certificate is still not bound to a node.** Q11's identity half is answered by the
  incarnation; the `shoal-node://<id>` SAN is written and unread, and `IdentityMismatch` is
  defined and never produced.
- **A partitioned leader keeps its map for one detection window** and admits writes it should
  not, since its `up()` count is stale until the detector moves. ~~M4's data quorum is what
  refuses those writes.~~ Since [F40](replication.md) the data quorum refuses them: a write
  admitted on a stale count is proposed to a group that cannot commit it, and is answered
  `OutcomeUnknown` at the write deadline.
- **Only the leader's detector view means anything.** The `Detector` admin read on a follower
  shows its local reachability and an empty table.
- **The `All` consistency after a `Down` is not exercised.** A cluster's policy is fixed at
  bootstrap and the detector test runs at `Quorum`.
- **A topology frame is per connection.** A client's pool of ten connections reads ten frames
  of one version; the client keeps the newest and there is no per-client count.
- **The detector's grace is a constant** of five intervals, not a setting.
- **The admin audit line is logged and not asserted** by any test.
- **A control refusal's code is derived from its reason** - a refusal whose text mentions a
  stale version is `StaleVersion`, every other is `Internal`. Filed as
  [item 98](../appendix/known-issues.md#98-an-admin-refusals-error-code-is-derived-from-its-reason-text).
- **`ShoalPool::transport()` still reaches shard zero** ([item 95](../appendix/known-issues.md#95-shoalpooltransport-reports-shard-zeros-links-and-calls-them-the-nodes)).
- **The fixture suite is what a loaded machine makes it.** Every test allocates whole cores and
  runs beside the others; under a full workspace run with child logging on, timeouts were
  observed that never reproduced alone. The waits were made per node where a follower could
  lag; the sixty second bounds stayed.
- **No capture.** The hop arms and the overhead arm run over real membership and were smoke-run
  on the development host; the benchmark host's capture waits. The fanout numbers below are the
  development host's under `powersave`.
- **`shoal-top`'s browser check was not run** for this feature: the `wasm32-unknown-unknown`
  target is not installed on the development host. Its native check passes and its index types
  changed by three scalars.

## Invariants to uphold

- **A fence is judged before anything else, by incarnation.** The state machine's `observe`,
  the report handler, the proposal handler and both handshake judges compare against the
  committed incarnation first. A run below it stops, and nothing it says is acted on.
- **Only a committed entry changes membership or health.** Admission, promotion, `Down`, `Up`,
  `Initialize` and the voter policy are log entries; a detector verdict, a ping and a
  reachability view are observations that never become state on their own.
- **Only the leader feeds the detector**, and a new leader seeds and resets. A report reaching
  a follower is answered `NotLeader` and forgotten.
- **A report's freshness is `(incarnation, seq)`**, judged at the leader. A report at or behind
  the last from the same run is counted and ignored; it never resets the clock.
- **A map is installed whole, and only a newer one.** A shard swaps its map and its ring between
  two messages; a client installs only a newer version. Every ring is derived from the map's
  order with one rule, so no two nodes disagree about an owner.
- **Write admission and readiness judge the same map with the same function.** A shard's
  `write_admission` and readiness's `default_writes` cannot disagree.
- **`TableId::of` is a persistence format.** The seed is frozen and the literals pinned; a change
  renames every persisted stream.
- **A joiner never initializes and never writes `Bootstrap`.** It has no cluster until a leader
  admits it; an unadmitted directory is refused by every other intent.
- **`Ready` is not a leader.** Readiness says `recovering`; a caller that needs a leader waits
  on `joined`.
- **A write through this node's own group waits out the lease, and a failed observation backs
  off.** The control core is one executor shared by RaftCore, the links and the loop; a
  proposal retried on every metrics change starves the links its acknowledgements ride.
- **Leader restore stays off.**
- **A promotion happens only while the effective and committed memberships agree**, and only
  while the committed voter count is below the policy. Never on detector evidence.
- **A repeated admin operation is answered before its version is judged**, from the applied
  state, and proposes nothing.
- **A peer relay never subscribes**, and a control reply queued to one is a bug, not a frame.
- **The client subscribes after authentication and before the split**, so the request is on the
  wire before anything else the connection carries and a server that never heard of a
  subscription can only refuse a client that has proven itself.

## Performance

**Nothing here is a capture.** The hop arms and the overhead arm now run over real membership -
node zero bootstraps, the peer joins through its seed and is promoted, node zero initializes
the placement, and a cluster of one places itself - and their records carry the members, the
map version and the voter and learner counts; the capture against the M2 budget is still the
benchmark host's.

**The arms ran at smoke scale on 2026-09-12 on the development host** (`europa`, 32 threads,
`powersave`) against a scratch copy of `shoal.yml` with local storage, two runs each, twice
over, written to a scratch directory and deleted. Every cluster record carried its members as
two `up` voters at map version 7 (one voter at version 3 on the one-node arm), `active_rf` 1,
and the hop and lane counters F38 recorded. The very first run recorded one voter and one
learner on every two-node arm: the harness read the record while the leader was still
promoting the joiner, so `initialize` now waits for the promotion the policy allows before it
places, and the record describes a settled group. 190 timed gets each after warmup, one
outstanding, medians of two runs, p50 / p90 / p99:

| Arm | F39, first run | F39, second run | F38's commit, same host, an hour later | F38's page |
| --- | --- | --- | --- | --- |
| `same_shard` | 65.6 / 72.1 / 91.5 µs | 68.1 / 75.1 / 87.1 µs | 67.2 / 72.0 / 83.9 µs | 35.8 / 36.7 / 45.6 µs |
| `local_shard` | 106.6 / 124.0 / 147.4 µs | 108.7 / 122.3 / 132.9 µs | 100.9 / 107.6 / 220.5 µs | 72.1 / 74.6 / 78.2 µs |
| `remote_node` | 102.0 / 107.6 / 115.1 µs | 105.9 / 114.3 / 124.2 µs | 101.9 / 108.3 / 147.5 µs | 82.4 / 88.1 / 114.7 µs |
| `overhead/nodes/1`, read p50 | 228.5 µs | 197.0 µs | 196.7 µs | - |
| `grid/unsorted/r50/1024`, read p50 | 221.9 µs | 203.6 µs | 227.1 µs | - |

The two-node arms came out thirty microseconds above the medians
[F38's page](inter-node-transport.md#performance) records at the same shard, and that was not
written down as membership's cost before it was checked: F38's commit (`2f6c437`) was built in
a worktree and run the same way on the same host an hour later, and its medians were F39's to
within a few microseconds (third column). The shift is the host's state under `powersave`
between two runs hours apart, not the code. What held is what a hop costs: the peer hop is
36-38 µs over the same shard and the mesh hop about 40 µs under both commits, and the
one-node arm stayed level with its standalone twin under both. **These numbers prove the arms
run over membership and that the record is complete; they measure nothing about the benchmark
host**, where the loopback budget is judged, and none of the four runs was kept.

**The fanout spike**, `cargo run -p shoal-spike --release -- fanout`, is the Q13-at-M3
measurement the milestone's evidence line asks for, taken on the same host and governor on
2026-09-12 and recorded at [C13](../distributed/protocol.md#q11-and-q13-at-m3). What it says:
a topology frame is 857 bytes for three members and one table and 15 KiB for sixty-four
members and sixty-four tables, encoded in under ten microseconds either way; a push of one
version to a thousand subscribers on a sixty-four member cluster is 13 MiB and four
milliseconds of copying; and the status reports of sixty-four members at the default half
second interval are 126 reports and 370 KiB a second into the leader. The frame grows with
members, not tables - a table is a name and an id - and a hundred subscribers per version is
cheaper than one Raft round trip. The budget the numbers set is the one C4 asks to be measured
rather than assumed: whole-map fanout is fine at M3's scale and per-tablet records are not,
which is why they wait for the day a tablet moves.

**What membership costs a query is nothing.** The map is read through an `Rc` a shard already
held for its placement; admission is one comparison per bundle on a cluster node and nothing
on a standalone one; a subscription is one entry in a set per connection.

## Tests

| Test | What breaks if the feature is reverted |
| --- | --- |
| `cluster_fixture::three_nodes_bootstrap_without_external_membership` | Node zero bootstraps, two join through it and are promoted to three voters; every node's members, voters and leader agree; all three killed and restarted come back as themselves in the same cluster with a leader elected from what they recovered; a write through one is read through another |
| `cluster_fixture::fourth_data_node_does_not_change_control_voter_count` | A fourth node joins as an up learner under a three-voter policy and stays one; `SET_VOTERS 5` promotes it |
| `cluster_fixture::minority_cannot_commit_membership_changes` | Node zero cut from the other two: the majority elects among themselves, zero's `SetControlVoters` is refused, a joiner through zero is not admitted and never appears at the majority; healed, zero rejoins with the majority's state |
| `cluster_fixture::lost_seeds_do_not_rebootstrap_existing_directory` | Two of three killed, the third restarted with a dead seed: ready, `recovering`, same identity, members and log length, no bootstrap; a peer back makes a majority and it is `joined`; a fresh joiner against dead seeds stays `joining` and `bootstrap: true` on its directory is refused |
| `cluster_fixture::cluster_needs_no_external_coordinator` | Only the three children exist and every address they know is a member's; the leader killed, another elected, a write through one read through another, the killed node back as a follower |
| `cluster_fixture::duplicate_node_identity_is_fenced` | A directory copied and started at the same incarnation from other ports is refused as a duplicate; started once more it is a run later and wins, and the original stops `Fenced`; the cluster holds the newest run at the clone's address |
| `cluster_fixture::control_elections_do_not_depend_on_data_shard_relay` | Every data lane delayed then cut and the leader killed: a new leader within a bound, pings answered, a read failing with a named code |
| `cluster_fixture::map_versions_install_atomically_and_resync` | Five rapid restarts behind slowed control lanes: every node on the newest version with three members, a client through the slowed node only moving forward and reaching it, a late client handed the newest map on subscribing |
| `cluster_fixture::table_ids_and_streams_are_stable_across_restart` | Two committed ids equal to the derived ones; rows in the persistent table read back after every node restarts, under the same ids; the ephemeral rows gone |
| `cluster_fixture::client_receives_topology_with_client_endpoints` | The frame names three members whose client endpoints are the fixture's and each answers a round trip; a fourth joining moves the frame; a burst of restarts leaves the client at the cluster's version |
| `cluster_fixture::readiness_distinguishes_process_control_and_data` | One node at factor three: joined, placed, `default_writes` short by one, an insert refused `QuorumUnavailable` naming have 1 need 2, a get served; two joiners lift it before initialization while each is unplaced and answers a get `NotInitialized`; initialization places them; a factor-one cluster admits writes at once |
| `cluster_fixture::admin_mutations_require_principal_and_operation_identity` | An anonymous client refused at connect; a non-admin reads and is refused a mutation `Unauthorized`; the admin refused `StaleVersion`, then applied, then answered `Repeated` for the same request with the log unchanged, then refused a fresh operation once initialized |
| `cluster_fixture::fresh_failure_reports_do_not_mask_shard_failure` | A dead shard committed as `shards_failed` with the member still up and pinging; three replayed reports counted and moving nothing; the member paused and called `Down` within a bound while a quorum write is admitted with two up; resumed and called `Up` |
| `control::types::tests::*` | Every apply rule: bootstrap, admit, observe and its four fencing outcomes, promotion mirrored from the membership, `Initialize` once with its refusals, `SetControlVoters` over {1, 3, 5}, health by incarnation, repeated operations answered as first applied and refusals not remembered |
| `control::detector::tests::*` (three) | Phi growing with silence and zero before the mean; a regular reporter calm, replays and older runs counted and ignored, silence suspected, a newer run starting over; a seeded member's grace, its first real report replacing the seed, a reset forgetting everything |
| `map::tests::*` (three) | Write admission following the policy and the up count; a joiner unplaced until initialized and the bootstrapper placed on itself; the map cell installing only newer maps and judging by them |
| `shard::tests::topology_frames_fold_to_the_newest_and_answers_keep_their_order` | A run of queued frames folded to the newest where the last one sat, every answer keeping its place |
| `peer::tests::peer_rejects_wrong_cluster_identity_and_malformed_payload` | A superseded incarnation refused `Fenced`; a joiner admitted on the control lane and refused on the data lane; an empty map trusting any member of its cluster |
| `meta::tests::*` | Format 2 read as 3; the join matrix; incarnation strictly increasing across claims; adoption once |
| `fingerprint::table_ids_are_stable_across_a_reorder_and_distinct_by_name` | The same ids from a reordered schema, distinct by name, and the two literals of the fixture schema |
| `protocol::admin::tests::admin_bodies_round_trip` | The request, the three outcomes, a refusal with its code and the topology frame round-tripping |
| `conf::cluster::tests::*` | `seeds` accepted alone and refused beside `bootstrap`; `dial` and the detector's window and samples parsed; the documented block matching the defaults |
| `harness::cluster::tests::*` | The staged node round-tripping with its seeds; the allocation on disjoint cores |
| `explore_index::the_index_mirrors_every_portable_fact` | The map version and the voter and learner counts reaching the explorer's `ClusterFactsLite` |
| `acceptance_tables::acceptance_tables_have_unique_tests_and_valid_milestones` | M3 marked delivered forcing all thirteen rows to exist as functions |

## Related

- [M3](../distributed/milestones.md#m3-membership), the gate this delivers, and
  [C1](../distributed/node-identity.md), [C2](../distributed/transport.md),
  [C3](../distributed/membership.md), [C4](../distributed/tablet-map.md),
  [C9](../distributed/operations.md), [C11](../distributed/testing.md) and
  [C13](../distributed/protocol.md#q11-and-q13-at-m3), the pages it changed
- [Configuration](../getting-started/configuration.md#cluster), for `seeds`, `dial`,
  `failure_detector` and the block as it stands
- [F37](node-identity-control-plane.md), whose group this is the membership of, and
  [F38](inter-node-transport.md), whose static placement this replaces
- [F36](cluster-harness.md), the fixture that now stages a membership cluster
- [Resolved #96](../appendix/resolved/ping-interval-consumer.md), the pinger, and
  [Resolved #45](../appendix/resolved/storage-marker-format.md), the marker at format 3
- [D7](../direction/shard-aware-routing.md), the client half of the topology
- [Items 95 and 98](../appendix/known-issues.md), what is left open
