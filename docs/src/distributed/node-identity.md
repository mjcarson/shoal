# C1. Nodes, identity, and the cluster configuration

## Context

A cluster is made of things that can name each other, and a Shoal process cannot name itself. It
has a bind address, which is not an identity — addresses are reassigned, containers are
rescheduled, and a node that comes back on a different address is the same node with the same
data. It has a storage directory that records the shard count that wrote it and nothing else. And
it has one cpu it has reserved since the beginning for a coordinator that has never existed.

This page gives a node a name, a cluster a name, a configuration block to say who its peers are,
and a thread to run the parts of the cluster that are not on any query path. Everything else in
this part is addressed to the identities minted here.

## What exists today

**One process, no name.** `ShoalPool::start` (`shoal-core/src/server.rs:69`) resolves the cpu set,
refuses an empty one, claims the storage directory, and spawns shards. Nothing in it, or in
`Conf`, identifies the process beyond `networking.interface` and `networking.port`
(`shoal-core/src/server/conf.rs:150`). That struct is **the entire networking surface**: a bind
address, an optional TLS pair, and a frame bound. There is no peer list, no advertised address and
no second port.

**The storage marker knows one fact.** `StorageMeta { format, shards }`
(`shoal-core/src/server/meta.rs:31`) is written on first start and compared on every later one;
a directory written by a different shard count is refused with `ShardCountMismatch`
(`meta.rs:79-98`, [Configuration](../getting-started/configuration.md#on-disk-layout)). Its module
doc already reserves the slot this page fills:

> This is also where a node's identity belongs once Shoal is distributed — a cluster id, a node
> id, and the epoch of the topology its data was written under all answer the same kind of
> question this file already answers.

`shoal-core/src/server/meta.rs:12-14`

**One cpu is reserved and idle.** `Resources::cpus()` filters cpu 0 out of every cpu set with the
comment "never run on cpu 0 as that is the coordinator cpu" (`conf.rs:75`). The configuration page
notes that "in practice there is no separate coordinator process"
([Configuration](../getting-started/configuration.md#resources)) — the word names a role a shard
plays per query ([Architecture Overview](../architecture/overview.md)). The core has been held
back for something that was never built.

**A schema fingerprint already names what a peer was built from.** The handshake compares a 64-bit
fingerprint of the schema on both sides and refuses a mismatch by name
([Wire Protocol](../architecture/wire-protocol.md#the-schema-fingerprint)). A cluster is a set of
peers built from one schema, and the check for that exists.

## The design

### Two identities

```rust
/// The identity of one Shoal process, minted on its first start and kept for its life
pub struct NodeId(Uuid);

/// The identity of the cluster a node belongs to, minted by the node that bootstrapped it
pub struct ClusterId(Uuid);
```

Both are uuid v4, both are persisted in the storage marker, and neither is derived from anything
an operator might change. A node started twice from the same directory is the same node. A node
started from an empty directory is a new node, whatever its address, and joins as one.

The `ClusterId` is minted exactly once, by the first node to start with a `cluster:` block and an
empty `seeds` list — the **bootstrap** node. Every node that joins through a seed adopts the seed's
cluster id in the peer handshake ([C2](transport.md#the-peer-handshake)) and writes it into its own
marker. From then on, a node whose marker names one cluster and whose seeds answer for another is
refused, in both directions, by name.

### The storage marker, format 2

```rust
pub struct StorageMeta {
    pub format: u32,                 // 2
    pub shards: usize,               // unchanged; still a refusal until C8
    pub cluster: Option<ClusterId>,  // None for a directory written by a single node
    pub node: NodeId,
    pub topology_version: u64,       // the last map version this directory served under
}
```

`format` goes to 2 and, following [item 45](../appendix/resolved/storage-marker-format.md), a
format-1 marker is **refused rather than upgraded**. There is no migration, for the same reason the
shard-count check has none: a directory this build cannot fully interpret is a guess, and a guess
that happens to match starts the server. A single-node deployment that has never had a `cluster:`
block still gets a `NodeId` and a `cluster: None`, so that the first time it is given peers it has
a name to give them.

`topology_version` is what [C4](tablet-map.md#persistence-and-what-a-restart-does) reads on restart to know whether the
node has to catch up before it may serve.

### The `cluster:` block

```yaml
cluster:                          # optional; omitting it is a single node, exactly today's behaviour
  seeds: ["10.0.0.1:12001"]       # peers to join through; the bootstrap node lists nothing
  advertise: "10.0.0.2"           # the address peers reach this node at; default = networking.interface
  port: 12001                     # the peer listener; distinct from networking.port on purpose
  replication_factor: 3           # cluster default; a table may override it
  write_consistency: Quorum       # One | Quorum | All
  read_consistency: One           # One | Primary | Quorum
  failure_detector:
    interval_ms: 500              # how often every node pings every other
    phi_threshold: 8.0            # the suspicion level at which a peer is Unreachable
  primary_failover_after: "5s"    # how long a primary may be Down before its tablets get a new one
  auto_remove_after: null         # off; "30m" removes a node Down that long, which rebalances
  tls:                            # mTLS between peers; required when networking.tls is set
    cert: "/etc/shoal/node.pem"
    key:  "/etc/shoal/node.key"
    ca:   "/etc/shoal/cluster-ca.pem"
  tables:                         # per-table overrides of the three consistency settings
    movies:
      replication_factor: 5
      write_consistency: All
```

**Omitting the block is today's server.** No control-plane thread beyond a trivial one, no peer
listener, no replication, `ShardContact::Local` everywhere. That is the same shape `auth:` and
`networking.tls:` have, for the same reason ([Configuration](../getting-started/configuration.md#auth)):
the committed `shoal.yml` is the config every frozen capture was taken under, and a required
section would invalidate the baseline for a setting a single node has no use for.

**The block is `#[serde(deny_unknown_fields)]`**, like `resources`, `networking` and `auth`. The
argument is F14's (`configuration.md:134-137`): a misspelled `replication_factor` under a block that
ignored it would produce a cluster that starts, joins, and holds one copy of everything, with
nothing anywhere saying so.

**`seeds` is a list of addresses, not identities**, because it is the one thing an operator has to
type before any identity exists. It is consulted only to join; once a node is a member the Raft
group is its source of peers, and the seed list may be stale or empty on every later restart.

**`advertise` exists because `interface` may be `0.0.0.0`.** A node that binds every interface
still has to tell its peers one address to reach it at.

**`port` is separate from `networking.port`.** A peer connection and a client connection are
different things — they are authenticated differently, framed with different message types, and
one of them should be reachable from outside the cluster and the other should not. Putting them
on one port would mean a handshake that has to decide which kind of peer it is talking to *before*
either kind has been authenticated, and would make it impossible to firewall the two apart. Two
ports is one more line of config and removes both problems.

**The three consistency settings are the cluster's defaults**, overridable per table here and per
bundle on the wire ([C6](reads.md#the-per-bundle-override)). A table's `replication_factor` may
differ from the cluster's, which [C4](tablet-map.md) has to honour in one map — see the limitation
there.

**`auto_remove_after: null` is the default, and the page that uses it says why** — a network
partition that outlasts the timeout removes a live node from the cluster it cannot reach, and
the rebalance that follows moves every byte it held ([C8](rebalancing.md#auto_remove_after)). It is
the footgun the user asked for, documented as one.

**`tls` is required when the client listener is encrypted.** A deployment that encrypts what
clients send and not what peers replicate has encrypted nothing. When `networking.tls` is absent,
`cluster.tls` may be too, and peers speak plaintext inside whatever trust boundary the operator has
drawn — the same stance the single node takes ([D4](../direction/encryption.md)).

### The control-plane thread

Every node runs **one extra thread, pinned to cpu 0**, which today is reserved and idle. It owns:

- the `openraft` instance and its log and snapshot storage ([C3](membership.md));
- the failure detector — the pings, the phi calculation, and the local `Unreachable` verdicts;
- the rebalancer, when this node is the Raft leader ([C8](rebalancing.md));
- the admin surface ([C9](operations.md)).

It owns **none** of: the peer listener, the peer connections that carry queries and replication,
or anything a query path touches. Those belong to shards ([C2](transport.md)). The control plane
is not on any hot path and must never be, which is what lets it run a different async runtime from
the shards without that being a data-path decision ([C3](membership.md#openraft-and-the-runtime)).

It reaches the shards the way everything else does — over the `kanal` mesh. `Comms::broadcast`
(`comms.rs:77`) is used exactly twice today ([Thread per Core](../architecture/thread-per-core.md#the-channel-mesh));
this adds a third caller and one new variant:

```rust
ServerMsg::Topology(Arc<TabletMap>)   // the map at a new version; every shard swaps its Arc
```

The shards reach it through a channel of its own, for the things a shard learns first — a peer
connection that dropped, a replication ack that timed out — and the control plane treats every one
of those as evidence for the detector, never as a verdict.

**Startup order changes for a node with seeds.** Today a shard binds its listener during `init`
and serves immediately. A node joining a cluster has no tablets and no map until the control plane
has joined the Raft group and received one, so its shards start, recover their local storage, and
then wait for the first `Topology` before binding the client listener. A bootstrap node, and any
node without a `cluster:` block, does what it does today.

### The address of a shard

`ShardInfo::name` is `"Shard-{id}"` today and names files and identifies a `Join`
(`shoal-core/src/server/shard.rs:831`, [Thread per Core](../architecture/thread-per-core.md#starting-the-shards)).
It becomes `"{node-short}/Shard-{id}"` in logs and traces, where `node-short` is the first eight
hex digits of the `NodeId` — enough to tell nodes apart in a trace, not enough to type. Filenames
do **not** change: a shard's files are still `Shard-N-*` under the node's own directory, because
the directory is already the node's and prefixing every filename with a uuid buys nothing
([C8](rebalancing.md#storage-stays-keyed-by-shard) says what does change on disk).

## Alternatives rejected

**Node identity from the hostname or the bind address.** Cassandra did this for years and grew a
`host_id` precisely because addresses move under a node and the ring has to know it is the same
node. An identity that changes when a container is rescheduled turns every reschedule into a
removal and a join, which is the rebalance R3 says must not happen.

**A required `cluster:` block, or a `mode: single | cluster` switch.** Both make every existing
config and every test config say something new, and the first invalidates the frozen benchmark
baseline for a setting a single node cannot use. Presence of the block is the switch, the same
way presence of `auth:` and `networking.tls:` already is.

**One port for peers and clients, distinguished by the handshake.** Discussed above: it cannot be
firewalled, and it asks the handshake to classify a peer it has not authenticated.

**A shared cluster secret instead of mTLS between peers.** Simpler to deploy, and it puts a
bearer credential in every `shoal.yml` in the cluster, from which anything that reads one file can
replicate into every node. The existing TLS path already produces a peer identity from a
certificate, and [D3](../direction/authentication.md) already wanted certificate identity for
clients; peers get it first.

**Deriving the `ClusterId` from the schema fingerprint.** Tempting, since every node in a cluster
shares one, but two clusters of the same application would then be one cluster, and a schema
change would rename the cluster. The fingerprint stays a mistake detector in the peer handshake
([C2](transport.md#the-peer-handshake)) and the cluster id stays random.

**Upgrading a format-1 marker in place.** A format-1 directory has no `NodeId`, so upgrading it
means minting one, which is exactly what starting from it does anyway — except that "upgrade"
implies the old directory was understood. Refuse, and let the operator move the data
([item 46](../appendix/known-issues.md#46-an-unmarked-storage-directory-is-claimed-rather-than-refused)
is the same stance for an older case).

## What it costs

- **One thread per node**, on a core that was already excluded from every shard. It costs the
  single node nothing it was using.
- **Two more fields in every `Conf`**, `cluster` and its default, which every test config and the
  benchmark harness's in-memory `Conf` carry whether they use them or not.
- **A second listener per shard** once C2 lands, and a second TLS certificate to provision.
- **A startup wait** for a joining node — it cannot serve until it has a map. A single node has no
  such wait.

## What it breaks

- **Format-1 storage directories are refused.** Every directory written before this change has to
  be started from empty. The book already tells operators this for two older markers.
- **`Conf::default()` and every builder in `shoal/tests/utils.rs`** gain a section. Nothing
  observable changes for them, but the struct is wider.
- **The meaning of "coordinator cpu."** The comment at `conf.rs:75` finally becomes true, and
  [Configuration](../getting-started/configuration.md#resources)'s note that no such process exists
  becomes false and is struck through when M1 lands.
- **`ShardInfo::name` in logs and traces** gains a prefix. Anything grepping for `Shard-3` still
  matches; anything matching the whole string does not.

## Invariants to uphold

- **A `NodeId` is minted once and never changes for the life of a storage directory.** Every other
  page addresses this node by it. A node that changed its id would be a new node holding an old
  node's data, which the map could not describe.
- **A `ClusterId` is minted once per cluster, by the bootstrap node, and adopted, never minted, by
  every joiner.** Two nodes with different cluster ids never exchange a frame after the handshake
  that discovers it.
- **The control plane is never on a query path.** No shard awaits it to answer a client. The one
  thing a shard takes from it is a `Topology` it already holds a previous version of.
- **Omitting `cluster:` is exactly today's server.** Every capture and every test that does not
  name the block must behave, and measure, as it did before the block existed.
- **The marker is refused, never guessed at.** A format this build does not know, a cluster this
  node is not in, a shard count this directory was not written by — each is a refusal to start.

## Prerequisites

None among the `C` pages; this is the root of the graph. It rests on
[F10](../features/framing-and-protocol-evolution.md)'s handshake and fingerprint,
[F14](../features/encryption-in-transit.md)'s TLS path, and
[item 45](../appendix/resolved/storage-marker-format.md)'s rule that the marker's format is read
before anything else in it.

**[Item 65](../appendix/known-issues.md#65-two-gxhash-majors-and-partition-keys-hashed-by-the-one-without-deterministic)
must be fixed before any two nodes route the same key.** Two `gxhash` majors are in the tree and the
partition key is hashed by the one without `deterministic`. On one node that is a persistence
hazard across upgrades; across two nodes built from different lockfiles it is two nodes disagreeing
about which tablet a row is in, silently. The fix is the item's, not this page's, and M1 names it
as a gate.

## How it would be measured

Nothing here is on a query path, and the one thread it adds runs on a core no shard uses, so the
claim is that the reference cell (`macro/grid/unsorted/r50/1024`) does not move when a `cluster:`
block naming a single node is present. That is `macro/cluster/nodes/1/rf/1/cl/one` in
[C10](performance.md#the-workloads), and it is the first cluster arm to capture, because every
later arm is read against it.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `a_node_started_twice_keeps_its_id` | Two starts from one directory report one `NodeId` | M1 |
| `a_directory_from_another_cluster_is_refused_by_name` | The error names both cluster ids | M1 |
| `a_format_one_marker_is_refused` | A marker with `format: 1` refuses to start and says which format it wanted | M1 |
| `omitting_the_cluster_block_is_a_single_node` | No control-plane thread beyond the trivial one, no peer listener, `ShardContact::Local` everywhere | M1 |
| `a_misspelled_cluster_setting_is_rejected` | `deny_unknown_fields` names the key | M1 |
| `the_documented_defaults_are_the_defaults` | Every default in the block above is what `Conf::default()` resolves | M1 |
| `the_control_plane_runs_on_cpu_zero` | The thread's affinity is cpu 0 and no shard's is | M1 |
| `a_joining_node_does_not_serve_before_its_first_topology` | With seeds that never answer, the client port is not bound | M3 |

## Related

- [C2. The inter-node transport](transport.md) — what the identities are exchanged in
- [C3. Membership and failure detection](membership.md) — what the control-plane thread runs
- [C9. Operating a cluster](operations.md) — where the block's settings are observed at runtime
- [Configuration](../getting-started/configuration.md) — the schema this block joins
- [item 45](../appendix/resolved/storage-marker-format.md), [item 46](../appendix/known-issues.md#46-an-unmarked-storage-directory-is-claimed-rather-than-refused) — the marker's existing rules
- [item 65](../appendix/known-issues.md#65-two-gxhash-majors-and-partition-keys-hashed-by-the-one-without-deterministic) — the hazard that becomes a cluster bug
- [D3. Authentication](../direction/authentication.md), [D4. Encryption](../direction/encryption.md) — where peer mTLS comes from
