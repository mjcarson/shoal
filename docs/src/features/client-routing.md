# F74. Client routing by topology

A client now sends each query to the node that serves it. Every connection has been pushed the
cluster's topology since [F39](membership.md), and until now nothing on the send path read it: a
bundle went to whichever node the pool handed a connection to, and that node forwarded it,
proposed it through a leader on another node, or asked another node for a read barrier. The
client builds a route table from every frame it is pushed, gives each query a target - its
group's leader for a write or a strong read, the preferred one until a hopped write's answer
names the actual one, and any holder for a read at `One` - and sends a bundle whose queries
belong on different nodes as a run to each. Routing is node level and on
by default; `Routing::Endpoints` is the client every caller had before.

## Context

This is the optional S1 prerequisite "D7, client routing by topology"
([S1](../object-storage/prerequisites.md#optional)). The direction page it is named after,
[D7](../direction/shard-aware-routing.md), was written for one node: it ranked routing last,
because the hop it removed was a `kanal` send between two cores of one machine, and asked for that
hop to be measured first. A cluster changed what the hop is. On the lab - three nodes at a factor
of three - every node holds every tablet, so a client that reaches any node is never forwarded;
what it does pay is a proposal hopped from the node it reached to its group's leader, for two
writes in three, and a read barrier asked of the leader for two strong reads in three. Both are a
network round trip between hosts, and [X3](../object-storage/bytes-through-groups.md) had already
seen the first on every write it drove.

What was there:

- **The topology, pushed and stored.** Every connection subscribes as it opens; a
  `TopologyFrame` carries the members' client addresses, the placement, the factors, the tables,
  each table's read policy, the moved replica sets and the quarantines
  ([C4](../distributed/tablet-map.md#how-a-map-reaches-a-shard-and-a-client)).
  `Shoal::topology()` read it; nothing else did.
- **The placement rule, on the server only.** `Ring::tablet_of`, `TabletMap::rule_replicas_of`
  and `TabletMap::preferred_leader` lived in `shoal-core`, which a client never links
  ([F15](client-server-split.md)).
- **A forward path that is always right.** A node that is sent what it does not serve forwards it
  to a holder, a follower proposes through the leader, and a stale route is answered and
  rerouted once. So a client's guess can cost a hop and never an answer.

Decided with the user: routing is by node, not by core; a bundle whose queries belong on
different nodes is cut into runs; and a client routes unless it asks not to.

## What it does

### One placement rule, in the crate both peers link

`shoal-proto/src/shared/placement.rs` holds the rule: `TABLET_BITS`, `TABLET_COUNT`,
`tablet_of`, `owner_of`, `active_rf`, `rule_replicas` and the weighted rendezvous
`preferred_leader` ([F58](weighted-leadership.md)), moved out of `ring.rs` and `map.rs`. The
server's `Ring` and `TabletMap` now call it, so a client and a server compute the same answer from
the same code, and `the_rendezvous_score_is_frozen` pins the score a client of another build
relies on.

The frame gains the two inputs it lacked: each member's `lead_weight`, and the cluster's
`tombstones`, which a group's voters are its members less. Both are `#[serde(default)]`, so a
frame from either side of F74 decodes on the other.

### The route table

`shoal-proto/src/shared/routes.rs` builds a `RouteTable` from a frame, once per version, inside
`TopologyState::install`: for every tablet its replica set - a move's configuration, else the
rule - and for every table the preferred leader of the group over each set; each member's client
address, whether it is up, and its quarantined tablets as a bitset. A frame that places nothing,
a standalone node's or a joiner's, builds none, and the client sends as it always did.

### Where a query goes

A query names its partitions through `ShoalQuerySupport::route_keys`, every key of every kind
(`partition_keys` names only a get's, the order its rows merge in), and `is_write`. The client
then (`shoal-client/src/client/routing.rs`, `plan_runs`):

| Query | Goes to |
| --- | --- |
| A write | its group's leader: the one its last hopped write was answered with, else its preferred leader, when that member is up and can be routed to |
| A read at `Quorum`, or naming no level on a table whose policy is stronger than `One` | the same leader, unless its copy is quarantined |
| A read at `One` | any member that holds the tablet, is up and has no quarantined copy of it |
| Anything none of those can take | the endpoints, as before F74 |

A query is placed by the first of these that accepts every one of its keys: the node of the run
it would join, so a bundle is cut as rarely as it can be; the bundle's *home* - the member the
client was given as its endpoint, when it is one, else a member chosen in turn bundle by bundle -
so reads at `One` go where the client was pointed, as they did before it routed; and otherwise
the member the most keys accept, ties to the first key's own choice. A member that advertises a
name rather than an address is matched to the endpoints once it is resolved, off the send path. **A
query is never split**: a get whose keys live on several nodes goes whole to one, and that node
gathers the rest, as every coordinator does.

### Runs

Adjacent queries with one target form a run. A run is a bundle of its own on the wire: the
bundle's id, its queries, and a `base_index` at its first query's offset, so every answer comes
back under the bundle's own index and every write keeps the identity, `(bundle, index)`, that a
retry repeats and a group remembers. The server needed no change for it: its query meter already
times several frames under one bundle id, its forwards, gathers and identity window are keyed by
`(bundle, index)`, and each frame is coordinated on its own.

`send` serializes each run by moving its queries out of the bundle, from the back, so no row is
copied; a bundle of one run is serialized whole, byte for byte what a client sent before. It
takes a connection for every target before anything is written, writes each connection's runs in
the bundle's order, and records each run as owed by its connection once it is written. A frame
flags its own last answer as the end, so a bundle sent as several runs is read as an unbounded
stream ended by a local `End(n)` - the mechanism query streams already used. `ShoalQueryStream`
plans each bundle it sends the same way, and registers every run's answers as owed before it
writes any, so the first run's answers cannot settle a slot a later run is still owed on.

### Leader hints

The frame names no group's actual leader, so a client starts from the preferred one, where the
balancer hands every lead once it has settled. Until it has - a fresh cluster, a restart, an
election, or a busy group whose followers are too far behind for the balancer to hand its lead
on - a write sent to the preferred leader is proposed through the actual one, a hop. **The answer
says who that was.** A committed write that hopped carries a leader hint after its session token:
the leading node's identity, under `Flags::LEADER_HINT` (bit 8), written only to a connection
whose hello asked for `CLIENT_CAP_LEADER_HINTS`. The token already names the group, so the client
keeps the hint by group (`LeaderHints`) and sends that group's next writes and strong reads to
the hinted node while it is up and can be routed to. ~~A hint that goes stale is corrected by the
next hop's answer~~ Only a write that hops is told, so a read never corrects a hint, and the
balancer hands every lead it can back to its preferred leader: a hint kept until the next hop
sent a client that had gone on to read its strong reads to a node that had stopped leading, for
good ([Resolved #223](../appendix/resolved/leader-hints-lapse.md)). **A hint lapses** after
`LEADER_HINT_FOR`, five seconds, the balancer's own pace, and its group is routed by its weights
again; a lead still away then costs the group's next write one hop, which teaches the client
again. A hinted node that cannot be reached is passed over for the preferred leader.

A throwaway run on the lab, taken on the uncommitted tree before committing, is why this exists,
and its figures are not quoted: the bench resets its cluster before every arm that follows one
that writes, so its write arms ran while the leads were still moving, and routed writes still
hopped. In the fixture, 300 routed writes at lead weights of 4:1:1 straight after start hop 195
times without hints and twice with them.

### Pools to nodes

A `Router` keeps a pool to each node a run has gone to, opened on first use from the member's
advertised client address (`client_advertise`, which `shoaladm` renders), with
`PoolConfig::per_node()`: two connections kept idle, at most 32, a one second checkout. A node's
connections share the client's ids, proxy, credentials and encryption, ask for the member's
host name under TLS when the caller named none, and do not subscribe to the topology, which the
endpoint pool's connections already hear. A member whose address is the client's one endpoint
shares the endpoint pools. A node the newest frame no longer names, or names at another address,
has its pools closed.

### When a guess is wrong

- **A node that cannot give a connection** - its address does not resolve or names nobody a client
  could dial, or its pool gives none within the checkout - has its runs sent through the
  endpoints, and the bundle is never failed for it.
- **A node a connection died on owing answers** is *suspect* for two seconds
  (`routing::SUSPECT_FOR`), and nothing is routed to it meanwhile; a connection reaped idle owed
  nothing and says nothing about its node. The control group calls a node that stays gone down,
  and the next frame takes it out of every route.
- **A stale target** - a leader that moved, a copy that retired - is the server's to put right:
  a follower proposes through the leader, a node with no copy forwards, and a retired copy
  answers `StaleTopology`, which `exec_with` now retries
  ([Resolved #220](../appendix/resolved/stale-topology-retried.md)). A retry re-aims every run:
  a bundle of one run is planned again, one of several keeps its runs and sends each through the
  endpoints if its node can no longer be routed to, and a try refused `StaleTopology` is sent
  through the endpoints until the client's map moves.
- **A run written before a later one failed** may have applied, so the send fails as
  `ConnectionLost`, an unknown outcome that `exec_with` retries under the same identity.

### Seeing it

Every node counts the hops it took for its clients: queries and shares it forwarded, writes it
proposed through a leader on another node, and strong reads whose barrier it asked of one
(`HopCounters`, in `ShardReplication.hops` and `NodeReplication.hops`). The stats view rates them
as three metrics in its queries tab - `forwarded/s`, `proposal hops/s`, `barrier hops/s` - and a
`shoaladm bench` capture records them a second as `ServerSample::hops_per_sec`, compared as
`member hops/s`. A client that routes well holds all three at zero.

`Shoal::routing()` says which kind of client this is, and `Shoal::node_connections()` lists the
pools it opened.

### Choosing it

| Caller | Routing |
| --- | --- |
| `Shoal::new`, `with_credentials`, `with_options`, `Shoal::builder()` | by topology |
| `ShoalBuilder::routing(Routing::Endpoints)` | through its endpoints, as before F74 |
| `shoaladm bench run --routing topology\|endpoints` | by topology, unless told; a spec from before F74 reads back as `endpoints` |
| `Deployment::connect` / `connect_with(.., routing)` | by topology / as told |
| the cluster fixture's `pinned()`, `shoal-bench`'s `Context::client()` | through the one node they name |
| `tmdb-dataset-loader contend`, and `load --addr` | through the one node they name |

## Design choices

**Node level only.** A query reaches the node that serves it and is then handed to the executor
hosting its slot through a `kanal` channel, the hop D7 called plausibly noise. Removing that one
too needs a port per executor or a way to steer a connection to a core, and a pool per node and
core; the user chose not to, and it is filed ([TODOs](../appendix/todos.md#client-routing)).

**The preferred leader, corrected by the answers.** The frame names no group's leader, and C4
rejected naming them, since every election would rewrite every client's map. The client computes
the leader the weights prefer instead, and the server hands every group's lead back to exactly
that member once it has settled ([O63](../appendix/optimizations.md#o63-leadership-never-returns-to-a-groups-placement-primary),
F58), so in a steady cluster the two are the same. While they differ, the first write to a group
hops and its answer names the actual leader, which the group's next writes go to: a hop a group
and a change of lead, not a hop a write. ~~While they differ the write takes the hop it always
took~~ was the first version, and it made routing worth little on any cluster whose leads had not
settled.

**Reads at `One` stick to their run, then to the client's own endpoint.** Sending every read to
its leader would have been one rule, and would have moved every read of a weighted cluster onto
the member weighted to lead - the lab's europa leads about half of every table. Reads at `One` are
served by any copy, so they go where their bundle already goes and, failing that, to the member
the client was pointed at. ~~A member chosen in turn~~ was the first version, and a throwaway run
on the lab, taken on the uncommitted tree to look for a regression before committing, showed what
it cost: a bench client made for europa, on europa, read over the network two times in three where
it had read over loopback, and `read100` at a bundle of one lost about a quarter of its reads a
second. A client pointed at a member reads there, as before; one given no member - a load
balancer's address - spreads its reads in turn.

**Runs under the bundle's own id and indexes.** A run could have had an id of its own, or the
wire could have carried an index per query. The first would have needed a map of ids back to
the caller's bundle and a retry identity that still names one request; the second, a change to
`Queries` and to every place the server computes an index from an offset. Contiguous runs under
the bundle's id needed neither, at the cost of more frames for a bundle whose neighbours belong on
different nodes ([O97](../appendix/optimizations.md#o97-a-write-bundle-routed-by-topology-is-sent-as-many-small-frames)).

**A query is never split.** A multi-key get keeps its server-side gather, which is where its merge,
order and limit already are. Splitting it would have moved D7's step 4 - merging, ordering and
truncating shares - into the client, for a query the lab's cluster serves whole from any node
([O98](../appendix/optimizations.md#o98-a-get-whose-keys-live-on-several-nodes-is-still-gathered-by-one)).

**On by default.** Routing changes no answer and no format; its failure mode is today's path.
What it changes is which node a query reaches, which is what every caller wants unless it means to
reach one node in particular - and those callers, the fixture, the shoal-bench harness and
`contend`, say so.

**The route table lives in `shoal-proto`.** `shoal-core` may never depend on the client, and the
test that holds a client's routes to the server's map runs in `shoal-core`.

## Alternatives rejected

| Alternative | Why not |
| --- | --- |
| Per-shard ports, so a query reaches its executor too | The hop removed is an in-process channel send; the ports reach the inventory, the listener, the renderer and every firewall, and multiply idle connections by the cores. Filed, not built |
| Pushing each group's actual leader in the frame | Every election would rewrite every client's map; C4 rejected it for that, and the preferred leader is where the lead settles anyway |
| Learning leaders from `AdminKind::Replication` | A poll of every node's groups, stale by its interval; a hint on the answer of the write that hopped costs nothing until a lead moves and is current as of that write |
| A hint on every answer, or on a refusal | A write that did not hop needs none, and a refused one carries no token to name its group by; a hint only beside a committed write's token keeps the section's meaning one thing |
| Routing a bundle whole to the node most of its queries want | A bundle of sixteen random writes would still hop two in three of them |
| An id per run, or an index per query on the wire | Either needs more machinery than contiguous runs under one id, which the server already serves (Design choices) |
| Client-side merge of a multi-node get | D7's step 4; moves the gather's merge, order and limit into every client for a query that a cluster at full replication serves whole anyway |
| A per-shard `MOVED` answer for a stale route | The server already forwards what it does not serve; a client that routes is an optimization over a path that is always correct, which is the shape D7 asked for |
| Off by default | Nothing would have benefited unless it asked; the callers that must reach one node are few and say so |

## Limitations

- **Routing stops at the node.** A query that reaches its node is still handed to the executor
  hosting its slot over a channel, and the cluster hop arms' `local_shard` control is still a
  mixture ([C2](../distributed/transport.md)).
- **A lead that moves costs one hop a group.** The first write to a group after its lead moved
  hops and brings back a hint; a strong read hints nothing, so a read at `Quorum` follows a lead
  only through the writes before it, and a forwarded write's answer carries no hint across the
  peer that forwarded it.
- **A hint lapses after five seconds**
  ([Resolved #223](../appendix/resolved/leader-hints-lapse.md)), so a lead the balancer cannot
  hand back - under writes, its followers too far behind - re-hops every five seconds each write
  to its group planned before the next hop's answer returns: about 150 proposals a second at a
  bundle of 16 and 440 at 64 on the lab, under one write in a hundred
  ([O99](../appendix/optimizations.md#o99-a-writes-leader-hint-lapses-with-a-reads-so-a-lead-held-away-re-hops-every-groups-writes-in-flight)).
  And a strong read of a group no write touches asks a barrier of the leader on every read once
  its hint has lapsed, as it did before F74. A hint on a read's answer would close both
  ([TODOs](../appendix/todos.md#client-routing)).
- **A bundle of writes is cut into many small frames.** At three nodes with keys spread evenly a
  bundle of sixteen writes is about eleven runs over three connections
  ([O97](../appendix/optimizations.md#o97-a-write-bundle-routed-by-topology-is-sent-as-many-small-frames)).
- **A multi-key get spanning nodes is still gathered by one of them** (O98).
- **Suspicion is a fixed two seconds** and is not shared between clients.
- **The topology comes only through the endpoints' connections.** If every endpoint the client
  was given is gone, its routes stop moving; its node pools keep working on the map it holds.
- **An advertised address is trusted.** A member that advertises a name no client can resolve is
  routed through the endpoints; one that advertises a reachable address of another server of the
  same schema would be sent queries it does not serve - which it forwards, since it is not a
  member - until the hello names the node it is
  ([TODOs](../appendix/todos.md#client-routing)).
- **The spikes' drivers route now.** X3's and X8's drivers build clients with the defaults, so a
  run of either today routes its writes to their leaders; their recorded results were taken
  before F74 and say so on their pages.

## Invariants to uphold

- **The placement rule exists once.** `placement.rs` is what the server routes by and what a
  client routes by; a copy of any of it on either side lets the two drift. The rendezvous score is
  frozen: change it and every client of another build hops its writes.
- **The frame carries everything the route table reads.** `routes_agree_with_the_map_for_every_table_and_tablet`
  holds the table built from `map.frame()` to `replicas_of`, `preferred_holder` and
  `preferred_leader` for every table and tablet; a new input to placement is a new frame field.
- **A run's indexes are absolute.** Each run's `base_index` is the bundle's base plus its first
  query's offset, so its answers land at the caller's indexes and its writes keep the identity a
  retry repeats. A run renumbered from zero would make two writes of one bundle one identity.
- **Runs to one connection are written in the bundle's order**, so two queries for one tablet in
  one bundle reach their node in the order they were added, as they did through one coordinator.
- **Once any run is on a socket, a failure of that send is an unknown outcome**
  (`ConnectionLost`), never a bare I/O error, so `exec_with` keeps the identity; and a stream
  whose bundle was partly written spends that bundle's indexes and is told it failed.
- **A stream's slot is owed every run's answers before any run is written.**
- **Routing is advice.** Nothing in the client may refuse a query because a route looks wrong:
  a target that cannot be reached becomes the endpoints, and the server's forward stays the path
  of last resort.
- **A leader hint is written only beside a committed write's token, and only to a connection
  that asked.** The token names the group the hint is for; a hint without one would name nobody's
  leader, and a client that did not ask would read it as its archive.
- **A hint is advice.** A hinted node that cannot be reached is passed over, and a hinted node that
  no longer leads hops the write and hints again; nothing may wait on a hint being right.
- **A hint is evidence about a moment, and the preferred leader is where leads go.** Only a write
  that hops is told, so nothing corrects a hint for a client that reads; a hint is followed for
  `LEADER_HINT_FOR` and no longer, a bound set by the balancer's pace
  ([Resolved #223](../appendix/resolved/leader-hints-lapse.md)).
- **A caller that means to reach one node pins `Routing::Endpoints`.** Every test of the forward
  path, a hop, a gather, a barrier or one node's own state, and every benchmark arm that prices a
  hop, depends on it.

## Performance

Measured on the lab with `shoaladm bench`, run from `examples/tmdb_dataset` against a cluster of
its own: `tmdb_cluster.yaml`'s three hosts copied under another name (`f74-bench`), at a factor of
three, deployed, preloaded with 200,000 TMDB rows and destroyed for every run. The conditions:

- the driver on europa (Ryzen 9 7945HX, 16 cores and 32 threads), the nodes on europa, titan and
  hyperion (titan and hyperion Zen1 V1756B, 4 cores and 8 threads), one gigabit between them;
- the `performance` governor on every host for each run, set and restored by the bench;
- eight workers, keys uniform, 5 seconds of warm-up and 20 measured, bundles of 1, 16 and 64 with
  4, 64 and 256 queries in flight, no retries;
- four rounds a side, each running both back to back, the side that went first alternating;
- **both sides one build**, `--routing endpoints` against `--routing topology`, so the difference is
  the client alone. Each of the bench's clients is given one member's address, so through the
  endpoints a client sends everything to that one member.

A difference counts only when the two sides' run intervals are disjoint. Hops a second are the
members' own counters (`forwarded`, `proposals_hopped`, `barriers_hopped`), summed over the
members, a ten second rate averaged from 15 seconds into the arm. **This is an A/B, not a capture.**

**Routing, at `1ae254b`** (F74 with [Resolved #223](../appendix/resolved/leader-hints-lapse.md)),
reads at the table's default level, `One`:

| Arm | Endpoints, ops/s median [range] | Topology | | p99, endpoints → topology | Hops/s, endpoints → topology |
| --- | ---: | ---: | --- | --- | --- |
| `insert100`, 1 | 3,275 [3,238–3,378] | 3,919 [3,869–3,989] | **1.20×** | 20.1 → 14.1 ms | 1,745 → 0 |
| `insert100`, 16 | 27,074 [25,784–27,789] | 37,923 [36,810–38,578] | **1.40×** | 82.7 → 66.8 ms | 17,044 → 152 |
| `insert100`, 64 | 37,642 [33,780–39,810] | 53,869 [53,241–55,677] | **1.43×** | 379 → 284 ms | 23,758 → 443 |
| `rw50`, 1 | 6,417 [6,375–6,709] | 7,527 [7,444–7,661] | **1.17×** | 18.3 → 13.2 ms | 3,439 → 60 |
| `rw50`, 16 | 47,656 [44,237–49,647] | 64,427 [62,441–66,486] | **1.35×** | 74.5 → 48.8 ms | 15,324 → 151 |
| `rw50`, 64 | 63,842 [57,523–64,536] | 83,689 [81,136–85,408] | **1.31×** | 440 → 415 ms, within noise | 20,440 → 248 |
| `read100`, 1 | 161,465 [151,660–167,268] | 161,628 [151,306–166,523] | within noise | 0.55 → 0.49 ms, within noise | |
| `read100`, 16 | 292,530 [262,556–314,736] | 303,649 [274,037–314,521] | within noise | 19.0 → 16.6 ms, within noise | |
| `read100`, 64 | 301,203 [271,935–340,618] | 317,045 [281,555–340,683] | within noise | 77.3 → 66.0 ms, within noise | |

**Every write arm gained, from a fifth at a bundle of one to two fifths at sixty-four**, and every
p99 but `rw50`'s at 64 fell. Through one member, two writes in three are proposed through a leader
on another node; routed, under one in a hundred is. A read at `One` takes the same path either way
- each client's home is its own endpoint, and every member holds every tablet - so reads are
within noise, as they should be: what routing costs a read is the planning, and no cost showed.
A read arm's hop figures are left out: they are the preload's proposal rate decaying through the
ten second window, not reads, and no read was forwarded or asked a barrier on either side.

The same pass at `77f37df`, F74 as first committed, agreed: writes 1.17× to 1.51×, every arm
disjoint, and reads within noise over eight rounds, the last four repeated because one routed
round of the first four read 12 to 20% below its others, and wrote below them at a bundle of 64;
across eight it sat inside both sides' spread.

**Strong reads**, `read100` at `--read-level quorum`, four rounds at each commit. At `77f37df` a
routed client still asked a barrier of another node for 38% of its reads at a bundle of one and 56
to 57% at sixteen and sixty-four, against 60% through the endpoints: the leader hints its preload
had learned were followed after the balancer had handed the leads back
([Resolved #223](../appendix/resolved/leader-hints-lapse.md)). With hints lapsing, at `1ae254b`:

| Bundle | Endpoints, reads/s median [range] | Topology | | Barrier hops a read at the arm's last sample, endpoints → topology |
| --- | ---: | ---: | --- | --- |
| 1 | 16,638 [16,352–17,124] | 17,554 [17,132–24,279] | **1.06×** | 0.60 → 0.17 |
| 16 | 24,293 [23,696–30,087] | 28,419 [24,914–46,905] | 1.17×, within noise | 0.59 → 0.01 |
| 64 | 27,369 [26,372–33,893] | 32,126 [27,449–50,256] | 1.17×, within noise | 0.59 → 0.00 |

The barrier hop is gone from a routed strong read once the leads have settled; the bundle-one arm
runs first after the preload, while they still are. A strong read is bound by its tail - p99 about
13 ms at a bundle of one on both sides, where a routed read's p50 was 0.6 to 0.9 ms - so removing
the hop moved the rate less than it moved the median; one routed round of four read 1.5 to 1.9
times its others at sixteen and sixty-four, and why is not established. Three runs of sixteen at a
bundle of 64, on both sides, met `QuorumUnavailable` with all three members up
([item 224](../appendix/known-issues.md#224-a-strong-read-on-a-healthy-cluster-under-load-is-refused-because-the-leaders-heartbeat-round-found-no-quorum)).

**What the counters cost a node**, the commit before F74 (`62ff509`) against `77f37df`, both
through the endpoints, so the difference is the server's hop counters and leader hint and the
client's planning of one run. Four rounds, `insert100` and `rw50` at bundles of 1 and 16, 10
seconds of warm-up:

| Arm | Before, ops/s median [range] | After | |
| --- | ---: | ---: | --- |
| `insert100`, 1 | 3,296 [3,224–3,348] | 3,339 [3,328–3,386] | within noise |
| `insert100`, 16 | 26,285 [25,168–27,900] | 27,155 [25,075–28,030] | within noise |
| `rw50`, 1 | 6,481 [6,340–6,565] | 6,413 [6,347–6,486] | within noise |
| `rw50`, 16 | 47,868 [45,347–50,868] | 46,859 [46,568–48,858] | within noise |

Every p99 was within noise too. **No cost was measured, so the counters are always on.** The first
two of six rounds lost their F74 side to a build broken by an edit made to the tree while it ran,
and its first `before` side ran beside rust-analyzer checking that edit; the four rounds above are
the four complete ones, two of them repeated after the rest.

Both sides of every pass, and the commit before F74, met `NotLeader` while leads moved, which a
bench with no retries counts as a failure: 809 of 6.7 million answers before F74 and 687 of 6.7
million after it in the cost pass, 1,114 of 75 million through the endpoints and 971 of 82 million
routed in the routing pass. No run lost a row.

## Tests

| Test | What breaks if F74 is reverted |
| --- | --- |
| `shoal-core` `map::tests::routes_agree_with_the_map_for_every_table_and_tablet` | 120 seeded maps - one to six nodes, one to four shards, factors one to three, equal and unequal weights, a move's configuration, a tombstone, a member down, a quarantine - each held to the server's replicas, forward holder and preferred leader for every table and tablet |
| `shoal-proto` `placement::tests::the_rendezvous_score_is_frozen` | the score a client of another build computes |
| `shoal-proto` `placement::tests::{a_tablet_is_the_top_twelve_bits_of_a_key, the_rule_places_copies_on_distinct_nodes, the_lead_follows_the_weights}` | the rule moved out of the server |
| `shoal-proto` `routes::tests::{a_frame_routes_by_the_placement_rule, holders_skip_what_cannot_serve}` | the route table's replicas, leaders, holders and read policy |
| `shoal-proto` `admin::tests::admin_bodies_round_trip` | the frame's two new fields, and a frame from before them decoding |
| `shoal-client` `routing::tests::a_bundle_is_cut_into_runs_that_keep_their_offsets` | runs that cover the bundle once, in order, at their own offsets |
| `shoal-client` `routing::tests::reads_at_one_stick_to_their_run` | a bundle of reads at `One` sent as one frame, its home turning |
| `shoal-client` `routing::tests::writes_go_to_the_leader_and_fall_back_to_a_holder` | the weighted leader, a holder when it cannot be routed to, the endpoints when nothing can |
| `shoal-client` `routing::tests::a_multi_key_get_goes_whole_to_the_member_holding_most` | a query never split |
| `shoal-client` `routing::tests::writes_follow_a_hinted_leader` | a write planned to the leader a hint names over its preferred one, and the newest hint kept by group |
| `shoal-client` `routing::tests::the_home_is_the_clients_own_endpoint` | reads at `One` kept where the client was pointed, and spread in turn once that member cannot be routed to |
| `shoal-proto` `protocol::tests::{a_leader_hint_is_sized_after_the_token, flag_bits_are_stable}` | the hint's flag, its length after the token, and a hint without a token read as none |
| `cluster_fixture` `a_routed_client_follows_the_leader_it_is_told_of` | at lead weights of 4:1:1 straight after start, 300 routed writes hop about once a group - twice in a run - where without hints they hopped 195 times |
| `shoal-client` `routing::tests::a_hint_lapses_back_to_the_preferred_leader` | a hint followed until `LEADER_HINT_FOR` and not from then on (#223) |
| `cluster_fixture` `a_routed_clients_strong_reads_follow_a_lead_moved_back` | at 4:1:1, routed writes taught while the leads sat at their primaries, then `Quorum` reads once every lead was at its preferred leader asking 106 barriers of other nodes in 300 (#223) |
| `shoal-client` `routing::tests::{a_suspect_is_routed_around_for_a_moment, only_dialable_addresses_are_routed_to}` | suspicion, and addresses no client could dial |
| `shoal-core` `control::stats::tests::tracker_derives_hop_rates` | the hop counters' rates and totals, and a node that never hopped writing none |
| `shoal-loadgen` `spec::tests::{table_arm_ids_are_unchanged, routing_reads_back_as_what_it_measured}` | a spec from before F74 digesting as before, and `routing` read back as what was measured |
| `cluster_fixture` `a_routed_client_writes_through_the_preferred_leaders` | three nodes at a factor of three: routed writes and strong reads hop no proposal and ask no barrier once leads are at their primaries, while a client pinned to node zero does both |
| `cluster_fixture` `a_split_bundle_answers_every_index_in_order` | three nodes at a factor of one: a bundle of 64 cut into runs, ordered and unordered streams of such bundles, each answering every index once and in order, nothing forwarded; pinned, the same writes forwarded |
| `cluster_fixture` `a_routed_client_survives_a_killed_member` | routed writes and reads retried through a killed member and after its return, and the client hearing the cluster call it down |

## Related

[D7](../direction/shard-aware-routing.md), the direction this delivers at node level;
[C4](../distributed/tablet-map.md), the map and the push it routes by; [F39](membership.md), which
pushed it; [F58](weighted-leadership.md), the leader it computes; [F42](primary-failover.md) and
[F45](replica-migration.md), the forward path it falls back on;
[Resolved #220](../appendix/resolved/stale-topology-retried.md), the retry it needed, and
[Resolved #223](../appendix/resolved/leader-hints-lapse.md), the lapse its hints needed;
[S1](../object-storage/prerequisites.md#optional), the prerequisite it closes; and
[O97](../appendix/optimizations.md#o97-a-write-bundle-routed-by-topology-is-sent-as-many-small-frames),
[O98](../appendix/optimizations.md#o98-a-get-whose-keys-live-on-several-nodes-is-still-gathered-by-one),
what it left.
