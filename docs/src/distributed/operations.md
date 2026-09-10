# C9. Operating a cluster

## Context

Every page before this one describes something an operator would want to see and cannot: which
nodes are in the cluster, which tablets each holds, how far a follower is behind, how suspicious
the failure detector is, whether a rebalance is running. And two things an operator would want to
*do* — remove a node, decommission one — have so far been described as log entries the leader
proposes, with nothing said about who asks it to. This page is the surface: the frames, the
`shoalctl` tab, the metrics, and the runbooks for the six things an operator will actually type.

## What exists today

**No introspection of any kind.** [Observability](../operations/observability.md) says it in its
first paragraph: "no metrics endpoint, no health check, and no introspection API". What a server
knows about itself reaches an operator as log lines, and since
[F34](../features/benchmark-tracing.md) as spans and metrics on an OTLP sink if `shoal.yml` names
one ([Configuration](../getting-started/configuration.md#tracing)).

**`shoalctl` is a query tool** — a ratatui TUI compiled against a schema, with query tabs, a SHQL
bar, completion and a result table ([shoalctl](../operations/shoalctl.md)). It has no
administrative surface because there has been nothing to administer.

**`ShoalPool::start` returns before anything is ready and `exit` reports nothing**
([item 58](../appendix/known-issues.md#58-a-shard-that-dies-is-not-reported-to-whoever-started-the-pool)).
There is no moment at which a node can say it is up, and no way to ask.

**A client learns the topology** since [C4](tablet-map.md#pushed-to-clients) and does nothing
with it but hold it — `Shoal::topology()`.

## The design

### The `Admin` frame

One message type, appended at the end of [C2](transport.md#the-message-types)'s list, carrying a
request the control plane answers. A client sends it on an ordinary client connection; the
accepting shard hands it to the control plane over the shard-to-control-plane channel
([C1](node-identity.md#the-control-plane-thread)) and relays the answer back under the request's
query id, so the client's proxy demultiplexes it like any response.

```rust
pub enum AdminRequest {
    Members,                          // every member, its state, its address, its shard count
    Topology,                         // the map at its version — the same thing the Topology frame pushes
    Lag,                              // per tablet this node holds: primary's stamp, own stamp, the gap
    Detector,                         // per peer: phi, last pong, the window
    Rebalance,                        // the plan and the moves in flight
    Decommission(NodeId),             // Up → Leaving; refused for a node that is not Up
    Remove(NodeId),                   // Down → Removing; refused for a node that is Up (decommission it)
    RebalancePrimaries(bool),         // toggle the third plan target
    Repair(Option<u16>),              // run anti-entropy for one tablet or all — see below
}
```

A request that changes state — `Decommission`, `Remove`, `RebalancePrimaries` — is **refused unless
the connection authenticated as a principal in `cluster.admins`**, a new list in the block
([C1](node-identity.md#the-cluster-block)). The read-only requests are open to any authenticated
client, and to any client at all on a server with no `auth:` section, which is the same stance
every query takes today ([Wire Protocol](../architecture/wire-protocol.md#limitations)). This is
the first thing in Shoal that a `Principal` ([F12](../features/authentication.md)) gates, and the
per-table authorization `todos.md` filed as blocked on having a principal gets its precedent.

`Members`, `Topology`, `Lag`, `Detector` and `Rebalance` are answered by *this node's* control
plane from *its* view; a state-changing request is forwarded to the leader, which proposes it,
and the answer is the commit or the refusal. An operator asking three nodes for `Members` may get
three answers during a change, and the version in each says which is newest.

### `shoalctl`'s cluster tab

A fourth pane kind beside the query tabs, opened with a key, drawing the five read-only replies
on a refresh interval:

```
 ┌ cluster 8f3a… ───────────────────────── version 1042 ── leader node-2c91 ─┐
 │ node        state    shards  tablets  primaries  lag(max)  phi            │
 │ 2c91a0b3    Up       12      1024     342        0         0.3   *leader* │
 │ 7e12f6d9    Up       12      1024     341        0         0.4            │
 │ b04d1e77    Down     12      1024     0          —         9.8   3m12s    │
 │ e9a3c5f1    Joining  8       0        0          —         0.2            │
 ├ rebalance ─────────────────────────────────────────────────────────────────┤
 │ 1024 → 683 per node; 341 moves planned, 3 in flight                       │
 │ tablet 0x3f2  2c91→e9a3  streaming  2.1 GiB / 3.4 GiB   lag 0             │
 └────────────────────────────────────────────────────────────────────────────┘
```

State-changing actions are keys with a confirmation line that names what will happen — `remove
b04d1e77: re-replicate 1024 tablets from survivors, this node will not be able to rejoin` — because
the action is the one an operator cannot take back. `shoalctl` stays a library compiled against a
schema ([shoalctl](../operations/shoalctl.md)); the cluster tab needs nothing from the schema and
would work in a standalone binary, which is worth knowing when `todos.md`'s "build and packaging"
entry is picked up.

### Metrics

On the OTLP metrics sink the configuration already has (`tracing.metrics`,
[Configuration](../getting-started/configuration.md#tracing)), recorded by the control plane and
by shards:

| Metric | Kind | Labels | What it says |
| --- | --- | --- | --- |
| `shoal.cluster.members` | gauge | `state` | How many members in each state |
| `shoal.cluster.topology_version` | gauge | | Whether every node agrees |
| `shoal.replication.lag` | gauge | `tablet`, `replica` | Seqs behind the primary. **The** metric — a follower that is falling behind is the earliest sign of everything else |
| `shoal.replication.quorum_wait` | histogram | `level` | Time from commit to release, per write; what `Quorum` costs a caller |
| `shoal.detector.phi` | gauge | `peer` | Suspicion, per peer |
| `shoal.failover.count` | counter | | How many `SetPrimary`s the leader has proposed for a `Down` node |
| `shoal.failover.window` | histogram | | Seconds a tablet had no `Up` primary |
| `shoal.rebalance.moves` | counter | `kind` | `add`, `drop`, `primary` |
| `shoal.rebalance.streaming_bytes` | counter | | Bytes streamed as snapshots |
| `shoal.reads.stale` | counter | `level` | `Quorum` reads that found a lagging replica and nudged it |

`shoal.replication.lag` at 4096 tablets × RF is too many series for a collector by default, so it
is exported per node as a max and a histogram, and per tablet only when `tracing.metrics.per_tablet`
is set — a knob that exists so the answer to "which tablet" is available when it is needed and not
paid for when it is not.

### Traces

A query that crosses nodes is one trace, by [C2](transport.md#the-message-types)'s rule, and its
shape is [Observability](../operations/observability.md#how-one-query-stays-one-trace)'s with two
new spans: `Coordinator::forward` on the coordinator, parent of the remote node's
`Shoal::request`, and `Shard::replicate` on a primary, parent of each follower's
`Shard::apply_replicate`. A trace of a `Quorum` write therefore shows the fan-out and, on each
branch, which follower's fsync the ack waited for — which is the attribution
[C10](performance.md#what-distribution-costs) needs and no throughput number gives.

### Readiness

`ShoalPool::start` gains what it never had: the control plane knows when every shard has bound
its listener, and `start` returns a handle whose `ready()` resolves then, and whose
`shard_failed()` resolves if one exits. That closes
[item 58](../appendix/known-issues.md#58-a-shard-that-dies-is-not-reported-to-whoever-started-the-pool)
and the readiness entry in `todos.md` that [F8](../features/purpose-built-workloads.md) worked
around with a probe. An `Admin::Members` reply from a node is also a readiness probe — a node that
answers it has a control plane, and one whose own entry says `Up` has a map.

### Runbooks

Six, each a numbered list of what to type and what to expect, kept short here and written in full
when M10 lands:

1. **Bootstrap.** One node, `cluster:` with empty `seeds`. It mints the cluster and is `Up`
   alone at `replication_factor` copies of nothing. Start the second and third with the first as
   seed; the rebalancer widens every tablet as they join. Do not send writes at `Quorum` until
   `Members` shows `RF` nodes `Up`, or they will be refused `Unavailable`.
2. **Add a node.** Start it with any `Up` node as seed. Watch `Rebalance` until the plan is empty.
   Nothing else.
3. **Replace a dead node.** `Remove` the dead one (or let `auto_remove_after`); add the new one.
   Two runbooks, in that order, and the reason for the order is that a `Removing` node's tablets
   are widened from survivors and the new node is then a destination for balance — doing it the
   other way round widens onto the new node and then rebalances again.
4. **Decommission.** `Decommission` it; watch until `Removed`; stop the process. Its directory can
   be deleted.
5. **Rolling upgrade.** One node at a time: stop it, upgrade, start it, wait for `Lag` to reach
   zero and `Members` to show it `Up`, then the next. A node running a protocol version its peers
   do not speak is refused by the peer handshake ([C2](transport.md#the-peer-handshake)) — which
   is the *safety* of a rolling upgrade, since the version byte is what makes a mismatch a
   refusal and not a misread ([D9](../direction/prior-art.md#cassandra)) — so a release that bumps
   the peer protocol has to speak the old version too, for the duration of the roll. That is a
   constraint on how `PeerHello` is versioned, and it is why the peer handshake carries the
   version in its body as well as in the header: a node can accept `n − 1` for one release.
6. **A partition healed and a node came back `Removed`.** It is refused. Its data is orphaned and
   stale. Start it from an empty directory as a new node; the old directory is a backup of nothing
   the cluster does not already hold, and can be deleted once `Members` shows every tablet at RF.

### Repair

`Admin::Repair` runs anti-entropy: every replica of a tablet computes a digest of its partitions
— a hash per partition of its archived bytes, folded into one per tablet — and the primary
compares. A replica whose digest differs is caught up by snapshot ([C7](failover.md#a-returning-node)),
because a digest mismatch at equal stamps means corruption, not lag, and corruption is not in the
log. This is the check every "digest-identical" acceptance test on the other pages uses, exposed
as an operator's tool and, on a timer (`cluster.repair_interval`, default off), as a scheduled
one. [M8](milestones.md#m8-repair) builds it, and it closes the gap
[Storage Overview](../storage/overview.md#limitations) names — "no checksums on archive data" —
from the other end: an archive that does not match its peers is repaired from them.

## Alternatives rejected

**An HTTP admin endpoint.** A second listener, a second protocol, a second auth path, and a
dependency on an HTTP stack the server otherwise lacks. The `Admin` frame reuses the client
connection, the client's auth, and the client's proxy.

**Admin over the peer port.** It would have to be authenticated as a peer, which is a node
certificate, which an operator's laptop does not have.

**Metrics per tablet by default.** 12,288 series per node for lag alone. Exported as a max and
a histogram, per tablet on request.

**A standalone `shoalctl` binary for the cluster tab.** It would work, and it is filed with the
packaging entry rather than built here, because the tab is a pane in the tool that exists.

**Gating read-only admin behind `cluster.admins`.** `Members` reveals addresses and states, which
the `Topology` frame already pushes to every client. Gating what is already public buys nothing.

## What it costs

- **A control-plane round trip per admin request**, and a Raft round trip for a state-changing
  one.
- **Ten metric families**, most of them one series per node, exported on the interval the
  configuration already has.
- **Two spans per cross-node hop**, subject to the same cost
  [O44](../appendix/optimizations.md#o44-one-trace-per-request-costs-a-span-per-query-and-one-per-frame)
  and O45 put on the existing ones.
- **A digest walk per repair**, which reads every archive of every tablet repaired.

## What it breaks

- **`ShoalPool::start`'s signature**, gaining a handle. Every caller — tests, the bench harness,
  the examples — changes, and every one of them gets to delete a sleep.
- **`shoalctl`'s `PaneKind`** gains a variant, and its help overlay grows a section.
- **A `Principal` now gates something**, so the "no authorization" limitation on
  [Wire Protocol](../architecture/wire-protocol.md#limitations) is partly false and is annotated.

## Invariants to uphold

- **A state-changing admin request is proposed by the leader and nowhere else.** A follower's
  control plane forwards it; it does not act on it.
- **A read-only admin request is answered from the local view and says which version it is.**
  No admin read blocks on the leader.
- **`Remove` is refused for an `Up` node.** The graceful path exists and is the one to take; an
  operator who wants the ungraceful one on a live node has to stop it first, which is the
  confirmation.
- **A repair that finds a mismatch at equal stamps streams a snapshot; it never patches a
  partition in place.**
- **Peer protocol version `n` accepts `n − 1` for one release.** This is what makes a rolling
  upgrade possible and it is a promise the release process has to keep.

## Prerequisites

[C1](node-identity.md) for the control plane and `cluster.admins`; [C3](membership.md) for
`Members` and the states `Decommission`/`Remove` transition; [C8](rebalancing.md) for
`Rebalance`; [C7](failover.md) for the snapshot repair streams; [F12](../features/authentication.md)
for the `Principal`; [F34](../features/benchmark-tracing.md) for the metrics sink.

## How it would be measured

The admin path is not a query path and no workload should see it. `shoal.replication.quorum_wait`
is itself a measurement, and [C10](performance.md) reads it beside the `nodes/rf/cl` sweep to
attribute a `Quorum` write's latency to the wait rather than to the fan-out — the first time a
metric rather than a stage stamp does attribution in this book.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `members_names_every_node_and_its_state` | Three nodes; kill one; `Members` from a survivor shows two `Up`, one `Down`, one version | M3 |
| `topology_admin_matches_the_pushed_frame` | The `Admin::Topology` reply equals the client's `Shoal::topology()` | M3 |
| `lag_reports_a_paused_follower` | Pause a follower, write; `Lag` on the primary shows the gap; resume; zero | M4 |
| `a_state_change_needs_an_admin_principal` | `Remove` from an unlisted principal is refused; from a listed one, accepted | M10 |
| `remove_is_refused_for_an_up_node` | The refusal names `Decommission` | M9 |
| `decommission_from_shoalctl_drains_the_node` | The tab's key sends the frame; the node reaches `Removed` | M10 |
| `ready_resolves_when_every_shard_listens` | `start().ready().await` returns and a client connects with no sleep | M0 |
| `shard_failed_resolves_when_a_shard_panics` | Inject a panic in one shard (test hook); the handle reports it | M0 |
| `a_cross_node_trace_has_the_forward_span` | `Coordinator::forward` parents the remote `Shoal::request` under one trace id | M2 |
| `quorum_wait_is_recorded_per_write` | The histogram has one sample per `Quorum` write | M4 |
| `repair_restores_a_deleted_archive` | Delete one replica's archive for a tablet; `Repair`; digest matches; span attributes name the snapshot path | M8 |
| `a_peer_one_version_behind_is_accepted` | A `PeerHello` at `n − 1` shakes hands; at `n − 2` it is refused | M10 |

## Related

- [Observability](../operations/observability.md) — what exists, and the trace shape this extends
- [shoalctl](../operations/shoalctl.md) — the tool the tab joins
- [Configuration — tracing](../getting-started/configuration.md#tracing) — the sink the metrics use
- [F12. Authentication](../features/authentication.md) — the principal that gates admin writes
- [item 58](../appendix/known-issues.md#58-a-shard-that-dies-is-not-reported-to-whoever-started-the-pool) — closed by readiness
- [TODOs — Observability](../appendix/todos.md), [Build and packaging](../appendix/todos.md) — the entries a standalone `shoalctl` would touch
- [D9 — Cassandra](../direction/prior-art.md#cassandra) — why the version byte makes rolling upgrades safe
