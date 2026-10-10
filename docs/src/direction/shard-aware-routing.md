# D7. Shard-aware routing

## Context

Shoal runs one shard per core and routes a partition key to its owner through a tablet map. The
client knows none of this. It holds one address, sends every bundle down whichever pooled
connection bb8 hands it, and whichever shard the kernel happened to give that connection to acts as
coordinator — splitting the bundle across owners, gathering the shares back, and replying.

A token-aware client would skip that hop. Every mature partitioned store has one, and building it
is the obvious next step. ~~**This page recommends building it last, and says why the recommendation
is weaker than it looks.**~~

**Built at node level by [F74](../features/client-routing.md) (2026-10-09).** This page was
written for one node, where the hop a routing client removes is a channel send between two cores.
A cluster changed that: the hop is a proposal or a read barrier sent to another node, and the
[object storage](../object-storage/prerequisites.md#optional) plan listed this page as an optional
prerequisite. F74 routes each query to the node that serves it - steps 1, 3 and 4 below, with a
pool per node rather than per shard - and leaves the shard, step 2, as it was. What follows is the
page as written, with what F74 changed struck through and kept.

## What exists today

**The client is topology-blind.** `Shoal::new` resolves an address and takes the first result
(`shoal-core/src/client.rs:130`). There is no shard count, no map, no per-shard connection.

**The kernel does the spreading.** Every shard binds the same port
(`shoal-core/src/server/shard.rs:422`, `Shard::spawn_client_listener`), glommio's listener sets
`SO_REUSEPORT`, and the kernel picks which shard's listener accepts a connection. So connections
are spread across shards; individual *queries* are not routed at all.

**The coordinator does the routing.** `Shard::send_to_shard` splits each query in the bundle by the
tablet map and dispatches the shares (`shard.rs:512-602`). A query naming partitions on more than
one shard registers a `Gather`, and `handle_gathered` merges the shares, orders by partition, and
truncates to the limit before replying once (`shard.rs:765-822`). The client is owed exactly one
response per query index, which is what the reorder buffer depends on
([The Client](../api/client.md#the-reorder-buffers)).

**Most of the machinery is already in the client's dependency tree.** The tablet map is
deliberately a table rather than a hash:

```rust
/// The number of bits of a partition key that name its tablet
///
/// Taken from the top of the key rather than the bottom so that a tablet can later be
/// split in two by consuming one more bit: its keys stay contiguous and no other tablet
/// is disturbed.
const TABLET_BITS: u32 = 12;
```

`shoal-core/src/server/ring.rs:20-25`

4096 tablets, ownership stored per tablet in a `Vec<u16>` — 8 KiB — rather than derived, precisely
so a tablet can be moved ([items 11, 12, 37](../appendix/resolved/tablet-ring.md)). And
~~`split_by_shard` is already a method on the *client-linked* `QueryKinds`~~:

```rust
fn split_by_shard<'a>(&self, ring: &'a Ring, found: &mut Vec<(&'a ShardInfo, Self)>);
```

~~`shoal-core/src/shared/traits.rs:108`, `ShoalQuerySupport::split_by_shard`~~

~~**The routing logic is compiled into every client already. It is simply never called there.**~~

**No longer true, and it was a defect rather than a head start.** A signature naming `Ring` and
`ShardInfo` on a trait the client implements is what made a client link the engine, which is what
[F15](../features/client-server-split.md) was about. The method now lives on `ShardRouting`, a
server-side extension trait in `shoal-core::server::routing`, implemented for `SortedQuery`,
`UnsortedQuery` and the generated `QueryKinds`; the bound sits on `ShoalDatabase::ClientType`, so
only something owning a ring asks for it, and a `#[shoal::db(client)]` schema implements none of it.

**This does not block anything on this page** — it changes what the first step is. Routing is no
longer *already* in the client, so giving a client a tablet map means moving a trait impl into a
crate both peers see, rather than leaving a method where it was and starting to call it. The
signature, the tablet map and the split logic are all unchanged and all still there; only which
crate they live in moved, and F15 chose the side that made a client buildable rather than the side
that anticipated this page.

## What is missing

Five things, and the first is the hard one.

### 1. A way to reach a chosen shard

`SO_REUSEPORT` actively prevents this: the kernel chooses which listener accepts, and a client
cannot ask for a particular one. Three known answers, all from Scylla:

| Approach | How |
| --- | --- |
| **A per-shard port range**, advertised in the handshake | Each shard binds its own port in addition to (or instead of) the shared one. The client connects to the port of the shard it wants. Scylla's `shard_aware_port` |
| **Connect and discover** | The server tells a new connection which shard it landed on; the client keeps opening connections and discarding duplicates until it holds one per shard. Scylla's original driver |
| **Source-port selection** | The client picks a *source* port whose hash selects the shard it wants, and arrives on the right one in a single connect. Scylla's modern trick |

Source-port selection is the elegant one — no extra ports, one connect per shard, no discovery
round. It is also the fragile one: it requires the client to know the shard count and reproduce the
server's steering hash exactly, and it fails behind anything that rewrites source ports, which
includes most container networking. **Recommend the per-shard port range** as the robust datacenter
answer, and record source-port selection as the zero-configuration alternative for a deployment
that controls its own network path.

*Not built. [F74](../features/client-routing.md) reaches the node, where every shard still shares
one port, and filed the shard as a todo ([TODOs](../appendix/todos.md#client-routing)).*

### 2. The topology, pushed

A [D2](framing.md) `Topology` frame carrying the shard count, each shard's endpoint, the
tablet→shard map, and a version. The map is 4096 `u16`s — **8 KiB, one frame** — which is small
enough that pushing the whole thing on every change is simpler and cheaper than any incremental
scheme, and small enough that this decision will still be right after tablet splitting exists.

Push, not poll. Cassandra has clients read `system.peers`; Scylla pushes; Aerospike refreshes on a
tend interval ([D9](prior-art.md)). Pushing on an existing connection is strictly less machinery
than a polling loop and it is what RESP3's push messages exist for.

*Built by [C4](../distributed/tablet-map.md#how-a-map-reaches-a-shard-and-a-client), as a frame of
the placement rule's inputs rather than of 4096 owners, and given the two inputs it lacked - each
member's lead weight and the cluster's tombstones - by F74.*

### 3. Client-side tablet computation

`tablet_of` is `partition >> (u64::BITS - TABLET_BITS)` (`ring.rs:102-107`), and the partition key
itself is already reachable from the client through `PartitionKeySupport::get_partition_key` and
`get_partition_key_from_values` (`shoal-core/src/shared/traits.rs:448-464`). Nothing new is needed
here beyond [D5](runtimes.md) moving `Ring` somewhere a client can name it.

*Built by F74: the rule moved to `shoal-proto/src/shared/placement.rs`, which the server's `Ring`
and `TabletMap` call, and a client builds a `RouteTable` from every frame with it. A query names
every partition it touches through `ShoalQuerySupport::route_keys`.*

### 4. Client-side merge

Routing a multi-partition query directly means one bundle becomes N bundles to N shards and **the
client** runs `merge`, `order_by_partitions`, and `truncate` — the `ShoalResponseSupport` methods
the coordinator runs today (`shoal-core/src/shared/traits.rs:126-168`).

That is where a token-aware driver's cost lives, and it has a real secondary benefit: the
server-side `Gather` disappears, and with it
[~~item 33~~ Resolved #33](../appendix/resolved/gather-expiry.md), which ~~is~~ was that
nothing bounds how long a `Gather` waits for a share that may never come - a gather expires at
its bundle's deadline since [F41](../features/read-consistency.md), so what the move would now
remove is the state, not a hang.

`limit` semantics survive the move. Each shard returning up to `limit` rows and the client
truncating the merged result is equivalent to the coordinator truncating a merge of the same
shares — it is the same operation on the same inputs, run one hop later.

*Not built. F74 never splits a query: a get whose keys live on several nodes goes whole to one,
which gathers the rest, and a bundle is cut between queries into runs instead
([O98](../appendix/optimizations.md#o98-a-get-whose-keys-live-on-several-nodes-is-still-gathered-by-one)).*

### 5. Staleness, which is what makes it safe

A tablet map goes stale the moment a tablet moves. **A stale map must degrade to today's behaviour,
never to an error**: the shard that receives a query it does not own forwards it, exactly as the
coordinator does now, and marks the response with the current topology version so the client
refreshes. This is Redis Cluster's `MOVED` and Cassandra's coordinator fallback, and it is the
entire reason token-aware drivers are safe to deploy. A client-side routing layer with no fallback
turns every rebalance into an outage.

Note this makes routing an *optimization* rather than a protocol requirement, which is the right
shape: the slow path stays correct and stays exercised.

*Built that way by F74, with no `MOVED` marking: a node forwards what it does not serve, the
client's map is pushed whatever the client sends, and a node a client cannot reach has its runs
sent through the endpoints.*

## Recommendation

**Build it, last, and measure the hop it removes first.**

| | |
| --- | --- |
| **Rank** | ~~**C** — the largest-looking item in the chapter and the one whose value is least established~~ Built at node level by [F74](../features/client-routing.md) |
| **Impact** | ~~Argued, and weakly. The hop this removes is a `kanal` send between two cores of the same machine~~ On one node, still that; in a cluster, a proposal hopped to a leader on another node for two writes in three and a barrier asked of one for two strong reads in three, which F74's lab A/B measured: removing them made writes 1.2 to 1.4 times as fast at bundles of 1 to 64, and reads at `One`, which took no hop, did not move ([F74](../features/client-routing.md#performance)) |
| **Difficulty** | XL — reaches the wire format, the pool, the server's listener, and the merge path |
| **Depends on** | ~~[D2](framing.md) for `Topology`~~ — **satisfied**, message type 9 exists and is unwired since [F10](../features/framing-and-protocol-evolution.md); [D6](connection-pool.md) for a pool that can be resharded; [D5](runtimes.md) for `Ring` on the client side |
| **Blocks** | nothing here. It is a prerequisite for multi-node routing, which is ~~[TODOs](../appendix/todos.md#distribution)'s problem~~ [Distributed Shoal](../distributed/overview.md)'s problem — [C2](../distributed/transport.md) and [C4](../distributed/tablet-map.md) — not this chapter's. Note that C4 builds step 1 below, the `Topology` frame, ahead of this page's schedule |
| **Tradeoff** | Major — the pool stops being uniform, and a whole class of staleness bugs becomes possible |
| **Benchmark** | ~~`routing`, unbuilt — and until it exists **this entry cannot be justified at all**~~ `routing` micro benches since [F24](../features/routing-benchmarks.md); the hop between nodes priced by the `macro/cluster/hop/*` arms, and routing's worth by `shoaladm bench run --routing topology` against `--routing endpoints` (F74) |

Staged, so that each step is separately useful and separately revertible:

1. ✅ **The `Topology` frame, with the client doing nothing but recording it.** Pure observability, no
   behaviour change, and it makes the map inspectable from `shoalctl` — which is worth having
   whether or not routing follows. *Built by [C4](../distributed/tablet-map.md) and
   [F39](../features/membership.md).*
2. **Per-shard ports**, advertised in the handshake and connected to but not yet routed on.
   *Not built: F74 routes to the node, and filed this as a todo
   ([TODOs](../appendix/todos.md#client-routing)).*
3. ~~**Per-shard sub-pools** in [D6](connection-pool.md)'s builder.~~ ✅ **Per-node pools**, opened
   on first use and sized by `PoolConfig::per_node` in D6's builder (F74).
4. ✅ **Client-side routing behind a configuration flag**, with the server-side forward path always
   live. *F74: `Routing::Topology`, on by default, `Routing::Endpoints` the old client.*

### Why this is ranked below everything else

*True of one node, and still true of the hop F74 leaves: a query that reaches its node is handed
to its slot's executor this way. In a cluster the hop a routing client removes is another node's,
which is why F74 was built.*

**The hop being eliminated is a `kanal` send between two cores on the same machine.** `Comms::send`
resolves a `ShardContact::Local(usize)` to an unbounded in-process channel
(`shoal-core/src/server/comms.rs:46-58`); there is no network in it, no serialization, and no
syscall. Against a query that touches storage, that is plausibly noise. Against a resident get on
an ephemeral table it may be a meaningful fraction. **Nobody knows**, because the `routing`
workload named in
[TODOs](../appendix/todos.md#benchmark-coverage-the-harness-does-not-have) does not exist, and
neither does any client-side instrumentation
([O28](../appendix/optimizations.md#o28-the-client-takes-two-guards-on-its-response-map-for-every-query-it-sends)).

[DragonflyDB](prior-art.md#dragonflydb) is the counterweight worth taking seriously: also
thread-per-core, also shared-nothing, and it deliberately does **not** expose shard-awareness to
clients, on the grounds that moving work between cores internally is cheap enough that the
complexity is not repaid. Scylla reaches the opposite conclusion, but Scylla's clients are usually
on other machines and its shard counts are large. Shoal's situation is closer to Dragonfly's than
the resemblance to Scylla suggests.

This is not an argument against building it. It is an argument that the measurement comes first,
and that a `routing` bench is cheap next to the feature it would justify.

## What it costs

- **Per-shard sub-pools.** `min_idle` and `max_size` stop being global numbers — they are
  `PoolConfig` fields since [F16](../features/client-builder.md), which is the seam this would grow
  a per-shard variant on. With 12 shards and `min_idle: 10`, a naive translation opens 120 idle
  connections — and each one is a TLS handshake
  and an authentication once [D3](authentication.md) and [D4](encryption.md) land.
- **Merge work moves to the client**, so a client machine now pays what a shard used to. For a
  fan-out query that is real CPU.
- **A staleness window** on every topology change, during which some queries take the slow path.
  Correct, but it makes latency depend on cluster state in a way it currently does not.

## What it breaks

- **The pool of interchangeable connections**, which is the design [The
  Client](../api/client.md#split-connections) is built around and the reason a slow query occupies
  nothing. Per-shard sub-pools reintroduce the question of what happens when the sub-pool for one
  shard is exhausted while others are idle — a problem the flat pool does not have.
- **Reproducible latency.** A query's cost begins to depend on whether the client's map is current.
  [O20](../appendix/optimizations.md#o20-a-sort-key-get-reads-a-partition-it-may-not-need) was
  declined on exactly this ground — "makes the cost of a query depend on what happens to be
  resident, which turns a reproducible latency into a flaky one" — and the same objection applies
  here with less force, because the fallback is a hop rather than a disk read. Worth naming, since
  it is the same principle.
- **The `Gather` path stops being exercised** for routed queries while remaining the fallback,
  which is the worst combination: code that must work and is rarely run.

## Prerequisites

[D2](framing.md), [D6](connection-pool.md), [D5](runtimes.md) — and the `routing` benchmark, which
this page treats as a hard dependency rather than a nice-to-have, following
[Optimizations](../appendix/optimizations.md)' rule that a missing benchmark is a dependency.

## How it would be measured

The `routing` workload: `Ring::find_shard` and `split_by_shard` in isolation for the map cost, and
a macro pair for the hop — the same query run against a connection that landed on the owning shard
and one that did not. The second is constructible today without building any of this, by connecting
enough times to cover the shards and comparing, which makes it the cheapest experiment in this
chapter and the one that decides whether the rest of the page is worth reading.

The ephemeral control workloads ([F9](../features/ephemeral-tables.md)) are the right place to run
it, because they remove storage from the measurement and leave the hop as a larger share of what is
left.

## Related

- [Partitioning and the Tablet Map](../architecture/partitioning.md) — how the map works today
- [items 11, 12, 37](../appendix/resolved/tablet-ring.md) — why it is a tablet map and not a hash
  ring, which is what makes this page possible at all
- [D2](framing.md), [D5](runtimes.md), [D6](connection-pool.md) — the three prerequisites
- [D9. Lessons from other databases](prior-art.md#scylladb) — Scylla's shard-aware driver, and
  Dragonfly's argument against one
- [TODOs](../appendix/todos.md#distribution) — multi-node routing, which this is a prerequisite for
  and not a substitute for
- [Distributed Shoal](../distributed/overview.md) — where multi-node routing is now designed;
  [C4](../distributed/tablet-map.md#how-a-map-reaches-a-shard-and-a-client) is step 1 of this page, built for servers
  and clients at once
- [Object Storage](../object-storage/overview.md) — a second thing a routing client would be for:
  one that read and wrote stripe chunks itself would save one of the two network crossings every object
  byte makes ([S9](../object-storage/read-path.md#where-the-bytes-travel)). That plan lists this
  page as optional, has a node coordinate every operation, and keeps this page's rule that the hop
  is measured before it is removed ([S1](../object-storage/prerequisites.md#optional))
