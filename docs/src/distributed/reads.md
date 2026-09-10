# C6. Reads and consistency levels

**A read at `One` may not see a write acknowledged at `Quorum`.** That is the promise this page
makes, in one sentence, before anything else: the default read is fast and may be stale by the
replication lag of whichever replica answered it, and the stronger reads are one field away.

## Context

Replication gives every tablet several copies, and a read has to choose one — or several, and
reconcile. This page is where **R4** (eventually consistent by default) and **R6** (reads may be
coordinated across nodes) are met, and where a caller who needs to read their own write is told
how.

## What exists today

**The coordinator routes each query to its owners without deserializing it**
(`route_archived`, [Partitioning](../architecture/partitioning.md#query-fan-out)), and a query
naming partitions on several shards is split. Each share comes back as `Gathered`; the splitter
merges them, restores the order the query named its partitions in, and truncates to the limit
(`Shard::handle_gathered`, `shoal-core/src/server/shard.rs:1648`,
[Request Lifecycle](../architecture/request-lifecycle.md#gathering-a-split-query)). The client is
owed exactly one response per query index, which the reorder buffer depends on.

**A gather waits forever.** "A shard that dies mid-query leaks it and the client waits forever,
since there are no timeouts anywhere"
([Request Lifecycle](../architecture/request-lifecycle.md#gathering-a-split-query),
[item 33](../appendix/known-issues.md#33-collected-split-query-state-has-no-expiry)). On one node
a shard dying is a bug; across nodes it is Tuesday.

**A bundle carries no consistency level**, because there has been nothing to choose:

```rust
pub struct Queries<S: QuerySupport> {
    pub id: Uuid,
    pub queries: Vec<S::QueryKinds>,
    pub base_index: usize,
}
```

`shoal-proto/src/shared/queries.rs`, [Wire Protocol](../architecture/wire-protocol.md#queriess).
`PROTOCOL_VERSION` is 3 (`shoal-proto/src/shared/protocol.rs:74`) and the schema fingerprint
folds the protocol version, so a change to `Queries` is a flag day for every peer — which
[F10](../features/framing-and-protocol-evolution.md) argues is the right kind of change to make
deliberately and once.

**`ErrorCode::Timeout` exists and nothing sends it** (`shoal-proto/src/shared/protocol/error.rs`,
`Timeout = 31`).

## The design

### Three read levels

| Level | Which replica | Sees a `Quorum` write? | Cost |
| --- | --- | --- | --- |
| `One` | Any `Up` replica. Preference: a local shard, then a shard on this node, then the replica with the lowest RTT in the failure detector's window | Eventually — after that replica has applied it | One hop at most |
| `Primary` | The tablet's primary, and only while it holds its lease | Always, for a writer who wrote at `Quorum` or `All`; and for one who wrote at `One`, since the primary has it | One hop at most, always to one specific shard |
| `Quorum` | `RF/2 + 1` replicas, the primary among them when it is `Up` | Always: any quorum intersects the write's quorum | `RF/2 + 1` hops, and a reconcile |

**`One` is the default**, and the preference order is what makes it cheap: on a three-node
cluster at RF=3 every tablet has a replica on every node, so a `One` read is answered on the node
that received it, and usually on the shard that received it, exactly as today. Distribution costs
a `One` read nothing until a node is down.

**`Primary` is the read-your-writes level**, and it needs a lease to be one. A primary that has
been replaced and does not know it would answer stale reads confidently. So a primary answers a
`Primary` read only if it has heard from the control plane within `primary_failover_after` — the
same interval after which [C7](failover.md) would replace it — and otherwise refuses with
`Unavailable` and lets the client retry, by which time the map has moved. This is the
CockroachDB and Kudu lease, at the coarsest granularity that works
([C12](prior-art.md#kudu-tikv-and-cockroachdb)).

**`Quorum` reconciles by `(epoch, seq)`, per partition.** Every replica answers with its rows and,
per partition, the highest `(epoch, seq)` it has applied to that tablet. The coordinator keeps the
answer from the replica with the highest stamp per partition, and **read-repairs** the others: for
each replica that answered with a lower stamp, it sends the primary a `CatchUp { tablet,
from_seq }` on that replica's behalf, so the lagging replica is brought forward by the mechanism
[C7](failover.md#a-returning-node) already builds, not by the coordinator writing rows. Read
repair is therefore a nudge, not a write, and a `Quorum` read never mutates anything on the path
that answers it.

Because a follower's partition state is a function of the intents it has applied, and it applies
them in order ([C5](replication.md#followers-apply-in-order)), "highest seq" is "most recent
state" without any per-row comparison. That is what the primary model buys reads: a reconcile is
a `u64` comparison per partition, not a merge per row.

### Choosing a replica

`find_replicas(key)` returns the `ReplicaSet` ([C4](tablet-map.md#the-value-widens)); the
coordinator picks by level:

```rust
fn choose(set: &ReplicaSet, level: Read, here: &ShardAddr, members: &Members) -> Targets {
    match level {
        Read::One     => Targets::One(set.nearest_up(here, members)),
        Read::Primary => Targets::One(set.primary),
        Read::Quorum  => Targets::Many(set.up_replicas(members).take(set.quorum())),
    }
}
```

`nearest_up` is the preference order above, computed from the map and the member states the
shard already holds — no lookup leaves the shard. A `Down` replica is never chosen at any level;
a `Leaving` one is.

### Fan-out across nodes

A multi-partition get is split by tablet, as today, and each share goes to a replica chosen by
the level — local or `Forward`. Shares come back as `Gathered` from local shards and as
`Forwarded` with `gather` set from remote ones ([C2](transport.md#responses-stop-bypassing-the-coordinator-across-nodes)),
and `handle_gathered` merges them exactly as it merges local shares, after unsealing the remote
ones. `order_by_partitions` and `truncate` run last, in that order, for the reason
[Resolved #26, 39](../appendix/resolved/partition-order.md) gives, and `limit` semantics survive
the extra hop for the reason [D7 §4](../direction/shard-aware-routing.md#4-client-side-merge)
gives: each replica returns up to `limit` and the coordinator truncates the union, which is the
same operation on the same inputs one hop later.

At `Quorum` a multi-partition get fans out to `RF/2 + 1` replicas *per tablet*, and the merge
reconciles per partition before it orders and truncates. That is the expensive read, and the
level exists for the caller who needs it, not as a default.

### Deadlines on a gather

Every `Gather` gets a deadline — the bundle's, if the client set one ([F16](../features/client-builder.md)
put `Deadlines` on the builder), else `cluster.read_timeout`, a new field defaulting to ten
seconds. A share that has not arrived by then is answered as `ResponseAction::Error(Timeout)`
for that query index, the gather is released, and the coordinator reports the missing replica to
the detector as evidence ([C3](membership.md#failure-detection)). The client still receives exactly
one response per index, so the reorder buffer's contract holds. This closes
[item 33](../appendix/known-issues.md#33-collected-split-query-state-has-no-expiry) for the case
that makes it reachable — a node dying — and leaves the single-node bug that could cause the same
leak as the item's remaining half.

### The per-bundle override

```rust
pub struct Queries<S: QuerySupport> {
    pub id: Uuid,
    pub queries: Vec<S::QueryKinds>,
    pub base_index: usize,
    /// How strongly to read and write, or `None` for the server's defaults
    pub consistency: Option<Consistency>,
}

pub struct Consistency { pub read: Read, pub write: Write }
pub enum Read  { One, Primary, Quorum }
pub enum Write { One, Quorum, All }
```

`PROTOCOL_VERSION` goes to 4, the fingerprint changes with it, and every peer is rebuilt
together — the F10 flag day, taken once more, for a field every future feature that touches
consistency will use. The client builder gains `.consistency(..)` for the pool's default and
`send`/`stream` gain a variant taking one per bundle. A bundle's `None` resolves on the server to
the table's override, else the cluster's default, and the resolution happens once per bundle on
the coordinator, not per query.

**Why per bundle and not per query.** A query id is a bundle id
([Wire Protocol](../architecture/wire-protocol.md#design-notes)), the reorder buffer is per
bundle, and a caller who wants two levels sends two bundles. Per-query levels would put a byte in
every `QueryKinds` variant and a branch in every route.

## Alternatives rejected

**A `Quorum` read that compares rows.** Cassandra's read repair merges per column by timestamp,
because its replicas can legitimately hold different rows. Shoal's cannot — they hold prefixes of
one log — so a stamp per partition is the whole comparison, and a row merge would be work that
finds nothing.

**Read repair by having the coordinator write the winning rows to the losers.** It would make a
read into a write from a node that is not the primary, which is the one thing the primary model
forbids. A `CatchUp` nudge to the primary keeps one writer per tablet.

**`Primary` without a lease.** A replaced primary would answer stale reads and call them fresh.
The lease is what makes the word mean something; Kafka's "read from leader" has the same problem
and solves it with the ISR high-watermark, which is a lease by another name.

**A linearizable read level (`Raft`, `Serial`).** Would need the read to go through the control
plane's log, or a per-tablet Raft. `Primary` under a lease is linearizable for a single tablet in
every case except a lease that expired during the read, and the page says so rather than
promising more.

**Consistency levels in `shoal.yml` only, no wire change.** Avoids the flag day and gives a
caller no way to say "this one read must be fresh" without restarting the server. The flag day is
cheap now — [F10](../features/framing-and-protocol-evolution.md#context)'s argument, still true —
and will not be later.

**Per-query levels.** Above.

## What it costs

- **`One`: nothing on a healthy cluster** where every node holds a replica of every tablet. One
  hop when the nearest `Up` replica is elsewhere.
- **`Primary`: one hop to a specific shard**, and a lease check on that shard — a comparison
  against a timestamp the control plane updates.
- **`Quorum`: `RF/2 + 1` fetches per tablet, one reconcile per partition, and a `CatchUp` per
  lagging replica.** Two to three times the read traffic of `One`, by design.
- **A deadline per gather** — one timer entry, on the coordinator, per split query.
- **The wire change**: one `Option<Consistency>` per bundle, two bytes archived, and a flag day.

## What it breaks

- **Every peer and every client, once**, at the version bump. Tests, `shoal-bench`, `shoalctl`
  and the examples are all built from the tree, which is the point of doing it now.
- **A gather that used to wait forever now fails after the deadline.** A client that treated a
  slow gather as a slow gather now sees an `Error(Timeout)` at that index. That is the point.
- **`send_one`'s "empty is failure" rule** ([item 55](../appendix/known-issues.md)) meets a new
  case: a `Quorum` read that timed out on one share and answered the rest. The item's fix is what
  resolves it, and it is not this page's.
- **The reorder buffer's "one response per index"** now has a case where that response is an
  error from the coordinator rather than an answer from a shard. The contract holds; the shape of
  what fills it widens.

## Invariants to uphold

- **A `One` read never waits for anything but the replica it chose.** It is the default because
  it is the cheap one, and a `One` read that consulted the primary "just to be sure" would be a
  `Primary` read with a misleading name.
- **A `Down` replica is never chosen at any level.** Routing reads member state, and a read that
  lands on a down node is a timeout that could have been avoided.
- **A `Primary` read is refused, not answered stale, when the lease has lapsed.** The refusal is
  what makes the level's promise true.
- **A `Quorum` read never mutates on the answering path.** Read repair is a `CatchUp` nudge to the
  primary, never a write from the coordinator.
- **A gather answers every index exactly once**, with an answer or with an error, and always
  before its deadline plus one sweep.
- **Consistency is resolved once per bundle on the coordinator.** No shard re-resolves it.

## Prerequisites

[C5](replication.md) for the stamps a `Quorum` read compares and the `Up`/`Down` states a choice
reads; [C4](tablet-map.md) for the replica set; [C2](transport.md) for the forward;
[C7](failover.md) for the `CatchUp` that read repair nudges and the lease
`primary_failover_after` defines; [F10](../features/framing-and-protocol-evolution.md) for the
flag-day mechanics; [F16](../features/client-builder.md) for the builder the level lands on.

## How it would be measured

`macro/cluster/nodes/3/rf/3/cl/{one,primary,quorum}` at the reference cell and, because the
reference cell is 50% reads, its `r100` twin — a read-only arm at each level, which is where the
difference between `One` and `Quorum` is not diluted by writes that cost the same at every read
level. `macro/fanout/*` ([F8](../features/purpose-built-workloads.md)) gains a `nodes/3` arm for
the multi-partition case, because a fan-out that crosses nodes is the read R6 is about, and
nothing else in the suite measures a merge of remote shares.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `a_one_read_prefers_the_local_replica` | On a three-node cluster at RF=3, a `One` read is answered by a shard on the coordinator's node (span attributes) | M4 |
| `a_one_read_never_targets_a_down_replica` | Kill the node holding the local replica; `One` reads route elsewhere and succeed | M4 |
| `a_quorum_read_after_a_quorum_write_sees_it_on_another_coordinator` | Write via node A, read via node B at `Quorum`, every time, 10k times | M5 |
| `a_one_read_sees_a_quorum_write_eventually` | Same, at `One`, with a bounded retry; and at least one first attempt is stale with a follower paused | M5 |
| `a_primary_read_sees_a_one_write` | Write at `One`, read at `Primary` via another node: seen | M5 |
| `a_primary_read_is_refused_when_the_lease_has_lapsed` | Partition the primary from the control plane past `primary_failover_after`; `Primary` reads are `Unavailable` | M6 |
| `a_quorum_read_repairs_a_lagging_replica` | Pause a follower, write, resume, `Quorum` read; the follower's digest matches within one sweep | M5 |
| `a_gather_times_out_and_answers_every_index` | Kill a node holding a share mid-query; the index gets `Error(Timeout)`, every other index its rows, the reorder buffer completes | M5 |
| `a_per_bundle_level_overrides_the_table_and_the_cluster` | Three bundles at three levels; span attributes name the level each resolved to | M5 |
| `a_client_from_protocol_three_is_refused` | A `Hello` at version 3 gets a `HelloAck` with `UnsupportedVersion` | M5 |
| `limit_spans_replicas` | A limited get across tablets on three nodes returns exactly `limit` rows in partition order | M4 |

## Related

- [C5. Replication](replication.md) — what the stamps are and what `Quorum` writes promise
- [C7. Failover](failover.md) — the lease interval and the `CatchUp` read repair uses
- [Request Lifecycle — Gathering a split query](../architecture/request-lifecycle.md#gathering-a-split-query) — the merge this page runs across nodes
- [Resolved #26, 39](../appendix/resolved/partition-order.md) — why order precedes truncate
- [D7 §4](../direction/shard-aware-routing.md#4-client-side-merge) — why `limit` survives a hop
- [F10](../features/framing-and-protocol-evolution.md), [F16](../features/client-builder.md) — the flag day and the builder
- [item 33](../appendix/known-issues.md#33-collected-split-query-state-has-no-expiry) — half closed here
- [C12 — Kudu, TiKV and CockroachDB](prior-art.md#kudu-tikv-and-cockroachdb) — leases; [Cassandra](prior-art.md#cassandra) — read repair, and why Shoal's is simpler
