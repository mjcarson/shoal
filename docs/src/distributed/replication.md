# C5. Replication and the write path

## Context

There is exactly one copy of every partition, and this page makes there be `RF`. It is the page
that meets **R5** — a write is not acknowledged until a quorum of its replicas hold it — and it
is the page where the decision to give every tablet a primary is argued rather than assumed.

The single-node write path is already shaped like a replication protocol with one participant.
A write appends an intent to a log, parks its response against the log position, and is released
when the position is durable ([Storage Overview](../storage/overview.md#durability-model)). The
change here is that "durable" comes to mean "durable on a quorum", and the thing that arrives to
release the response is an acknowledgement from another node as well as an `fdatasync` from this
one.

## What exists today

**An insert writes the log, then memory, then parks** (`PersistentSortedTable::insert`,
`shoal-core/src/server/tables/persistent/sorted.rs`, and
[Request Lifecycle](../architecture/request-lifecycle.md#write-path)):

```rust
let intent = SortedIntents::Insert(row);
let pos = self.storage.commit(&intent).await?;   // the only disk write on the hot path
...                                              // apply to the partition, stamp the generation
self.pending.add(meta, pos, action);             // parked until `pos` is durable
None
```

`commit` (`FileSystem::commit`, `shoal-core/src/server/tables/storage/fs.rs:366`) serializes the
intent with rkyv, checksums it with `gxhash`, and stages `[size][checksum][archive]` into the
`StreamWriter`'s DMA buffer ([The Intent Log](../storage/intent-log.md#record-format)). **The
serialized bytes exist in `commit`'s hand.** That is the fact this page is built on.

**The gate is a watermark.** `PendingResponse` (`shoal-core/src/server/tables/storage.rs:47`) is a
queue of `(pos, meta, response)`; `get` (`storage.rs:115`) releases every entry at or below the
durable position, and `drain_all` (`storage.rs:86`) releases every entry when the log rotates,
because rotation fsynced everything ([The Intent Log](../storage/intent-log.md#rotation)). The
position comes from `FlushState`, advanced contiguously by completions, and `Durability::Fsync`
gates on `synced_pos` while `Async` gates on `written_pos`
([Configuration](../getting-started/configuration.md#durability)).

**`DataFlushed` is a required wakeup.** Since [F5](../features/flushed-sweep-gate.md) the shard
sweeps its pending responses only on that message, so "every advance of the watermark must be
followed by a `DataFlushed`" is a contract ([The Intent Log](../storage/intent-log.md#completion-notification)).

**Recovery replays the log** ([Recovery](../storage/recovery.md)): sealed logs oldest first, then
the active one, loading every partition an update needs before replaying anything. The same
records serve the write path, recovery and compaction — "one representation, three uses"
(`recovery.md`, *Design notes*).

**Updates carry only the changed fields.** `SortedIntents::Update(SortedUpdate<T>)` holds
`T::UpdateData` ([The Intent Log](../storage/intent-log.md#intent-types)), which is why replaying
one needs the base partition. This is the fact that decides the replication model.

## The design

### Why a primary

Two replicas that apply the same intents in different orders diverge, and Shoal's intents are not
commutative — an insert then a delete is not a delete then an insert (`recovery.md`, *Replay
order*). Leaderless replication (Scylla, Cassandra) tolerates that by resolving on read with a
timestamp per value: the newest write wins, and two writes to different fields of one row both
survive because each field carries its own timestamp. **Shoal's update carries the changed fields
and no timestamp, and its rows store no timestamp per field.** Under last-writer-wins at row
granularity, two updates to different fields of one row, arriving in different orders on two
replicas, leave the replicas holding different rows forever — the later timestamp wins the row,
and whichever fields the loser changed are gone on one replica and present on the other. Fixing
that means a timestamp per field in every partition and every intent, which is a redesign of row
storage, not an addition to it.

So every tablet has a **primary**, one of its replicas, which serializes that tablet's writes. A
follower applies what the primary sends, in the order the primary sent it, and never accepts a
write from anyone else. Replicas cannot diverge because there is one order. What it costs is a
window during which a tablet whose primary is down accepts no writes, and [C7](failover.md) is
about making that window short.

### The primary stamps

The coordinator routes a write to the tablet's primary — `find_replicas(key).primary`, which is
local or a `Forward` ([C2](transport.md)). The primary keeps, per tablet it is primary for, the
current `epoch` from the map and a `seq` it increments per write:

```rust
/// What a primary knows about each tablet it leads
struct Lead {
    epoch: u32,        // from the map; changes only on SetPrimary
    next_seq: u64,     // the next write's sequence number, monotonic within the epoch
    acked: u64,        // the highest seq a quorum has acknowledged
}
```

### The intent record, format 2

```
┌────────────┬────────────────┬─────────────┬────────────┬───────────┬──────────────────────┐
│ size (8 B) │ checksum (8 B) │ tablet (2B) │ epoch (4B) │ seq (8 B) │ rkyv-archived intent │
└────────────┴────────────────┴─────────────┴────────────┴───────────┴──────────────────────┘
                              ◀──────── 14 bytes of replication header ────────▶
```

`size` counts the header and the archive; the checksum covers both. `IntentLogReader`
(`fs/reader.rs`) reads format 1 or format 2 by `StorageMeta.format`, and a format-2 directory is
never read by a format-1 build, which is the marker's job ([C1](node-identity.md#the-storage-marker-format-2)).
A single node with no `cluster:` block writes format 2 too — `epoch 0, seq n` — so there is one
write path, not two, and a single node that later joins a cluster has a log the cluster can read.

Fourteen bytes per record. On a 256-byte row that is five percent of the record and on a
kilobyte row it is one; [C10](performance.md) measures it, and the answer is expected to be under
the run-to-run noise of the reference cell.

### The bytes are forwarded, not re-serialized

`commit` has the archive in hand. It stages the record into its own log — exactly as today, plus
the header — **and hands the same bytes to the shard's replication sender**, which writes them
into a `Replicate` frame for each follower named in the replica set. The follower receives the
record, checks the epoch against its own view of the tablet, checks the seq against the last one
it applied, and if both are right stages it into its own log through the same `StreamWriter`
path and applies it to its own partition. It has not deserialized the intent to do any of that —
the header is fixed-offset, and the intent is applied by the same replay code recovery uses.

```
 coordinator            primary (A3)                       follower (B7)              follower (C1)
 ───────────            ────────────                       ─────────────              ─────────────
 Forward ──────────────▶ commit: stage [hdr][intent] ─┐
                          apply to partition           ├─ Replicate ──────────────────▶ check epoch, seq
                          pending.add(seq, meta)       │                                stage the same bytes
                                                       └─ Replicate ─────────────────────────────────────▶ same
                          (own fdatasync) ─▶ DataFlushed ─▶ ack #1
                                        ◀───────────────── ReplicateAck {tablet, epoch, seq, pos} ── ack #2
                          quorum (2 of 3) ─▶ release ─▶ reply
                                        ◀────────────────────────────────────────────── ReplicateAck ── ack #3, late, recorded
```

**A follower acks after its own durability.** `ReplicateAck` is sent when the follower's watermark
— `synced_pos` or `written_pos`, by *its* `durability` setting — has passed the record. A follower
configured `Async` acks sooner and promises less; the primary does not know or care which. The
cluster's durability at `Quorum` is therefore "on a quorum of replicas, to each replica's
configured standard", and the page for `durability` gets a sentence saying so.

**Batching is inherited.** The `StreamWriter` stages records into a buffer and writes whole
buffers ([The Intent Log](../storage/intent-log.md#streamwriter)); the replication sender does
the same, writing a `Replicate` frame per staged buffer rather than per record, so under load the
fan-out is one socket write per DMA buffer per follower. Under light load it is one frame per
write, exactly as it is one DMA write per write today.

### The quorum gate

`PendingResponse` keeps its shape and changes its condition:

```rust
struct Pending<T> {
    seq: u64,
    acks: u8,           // this node's durability counts as one, arriving as DataFlushed
    needed: u8,         // 1 for One, RF/2 + 1 for Quorum, RF for All
    meta: QueryMetadata,
    response: ResponseAction<T>,
}
```

`get` releases every entry whose `acks >= needed`. Two things feed it: the existing `DataFlushed`
path, which now increments `acks` for every entry at or below the local watermark, and a new
`ServerMsg::Replicated { tablet, epoch, seq }` posted by the shard's replication receiver when a
`ReplicateAck` arrives, which increments `acks` for every entry at or below `seq`. Both are
wakeups in F5's sense, and both must follow every advance they cause.

`needed` comes from the write's consistency level: the bundle's override, else the table's, else
`cluster.write_consistency` ([C6](reads.md#the-per-bundle-override)).

**A `Down` follower is not waited for.** The primary counts only replicas the map marks `Up` (or
`Leaving`) toward `needed`, so an RF=3 tablet with one node down still acks at `Quorum` on two.
If fewer than `needed` replicas are `Up`, the write is refused before it is committed with a new
`ErrorCode::Unavailable` — appended to the error code table after `GoingAway = 41`, retryable, on
the channel [F11](../features/error-channel.md) built. It is refused *before* commit so that the
primary's log never holds a write the client was told failed.

**Rotation no longer drains.** `drain_all` released every parked response on rotation because
rotation fsynced everything, and that is still true of this node — but a quorum is not this
node. Rotation now increments `acks` for every entry (this node is durable) and releases only
those that reach `needed`. This is the one invariant of the single-node write path that changes,
and it is named below.

### Followers apply in order

A follower holds, per tablet it follows, the `epoch` it believes current and the last `seq` it
applied. A `Replicate` whose epoch is **older** than the follower's is from a primary that has
been replaced and does not know it; it is refused with a `ReplicateAck` carrying the follower's
epoch, which is how the old primary learns ([C7](failover.md#fencing)). A `Replicate` whose epoch
is **newer** means the follower's map is stale; it is buffered until the `Topology` that explains
it arrives. A `Replicate` whose seq is **not the next one** is a gap — a lost frame, a reconnect —
and the follower buffers it and asks the primary for `CatchUp { tablet, from_seq }`, which
re-sends the missing range from the primary's log ([C7](failover.md#a-returning-node)). Nothing is
ever applied out of order.

### What the client is promised

At `Quorum`, the default: **the write is durable, to each replica's configured standard, on a
majority of the tablet's replicas under the current epoch.** It survives the loss of any minority
of them. It will be visible on every replica once they catch up — which is R4, and which
[C6](reads.md) is careful to say a `One` read may precede. At `One`: durable on the primary alone.
At `All`: on every replica, and a single `Down` follower makes every write wait until it is
marked so.

## Alternatives rejected

**Leaderless quorum writes with hybrid-logical-clock timestamps.** The Scylla and Cassandra
model, and the user's decision was against it, for the per-field-timestamp reason argued under
*Why a primary*. What it would have bought is writes that never pause — any quorum of replicas
alive is enough — and [C7](failover.md) is what this design pays for not having that.

**Multi-Raft, one group per tablet.** Linearizable writes, automatic leader election per tablet,
and a protocol with a proof. 4096 groups is 4096 heartbeats per interval per node pair; TiKV
makes that work by batching every group's messages between two nodes into one frame, and that
batching layer is most of the complexity ([C12](prior-art.md#kudu-tikv-and-cockroachdb)). The
control plane already has one Raft group; this design uses it to *choose* primaries and lets the
primaries replicate without a vote per write.

**Chain replication.** Latency is the chain length and the tail is the bottleneck. Kafka's ISR —
a leader that fans out and waits for a quorum — is the closer model and the one taken
([C12](prior-art.md#kafka)).

**Replicating rows rather than intents.** A follower would receive partitions rather than
mutations, and the "one representation, three uses" property (`recovery.md`) would become two
representations. Intents are already bytes in `commit`'s hand; rows would have to be built.

**A separate replication log beside the intent log.** Every write would be staged twice. The
intent log with a fourteen-byte header *is* the replication log, on every replica.

**Upgrading format-1 logs in place.** Refused instead, for [C1](node-identity.md#alternatives-rejected)'s
reason; an in-place upgrade is filed in `todos.md` when M4 lands.

## What it costs

- **Write latency at `Quorum` becomes** `max(own fdatasync, RTT + slowest fdatasync among the
  quorum's other members)`. On the development machine's Optane an fdatasync is under a
  millisecond and a loopback RTT is tens of microseconds, so the emulated number will be close to
  today's; on a real network with consumer NVMe it is the follower's fsync that dominates
  ([C10](performance.md#what-distribution-costs)).
- **One `Replicate` frame per staged buffer per follower**, and one socket write to carry it.
- **Fourteen bytes per intent record**, on every replica.
- **A `Pending` entry holds an ack count** and lives until a quorum, which under a slow follower
  is longer than until an fdatasync. [Item 15](../appendix/known-issues.md#15-no-backpressure-anywhere)'s
  "nothing bounds `PendingResponse`" gets worse before C2's shedding makes it better.

## What it breaks

- **`drain_all` on rotation.** Rotation used to mean "everything parked is done". It now means
  "this node's ack for everything parked". The rotation invariant on
  [The Intent Log](../storage/intent-log.md#rotation) is rewritten when M4 lands.
- **Format-1 intent logs are refused.** A format-2 build will not replay a format-1 directory.
- **`Durability::Async` on a follower** changes what a `Quorum` ack means without the primary
  knowing. The configuration page has to say that the cluster's durability is the weakest
  replica's in the quorum.
- **`RecoveryStats` gains nothing but recovery gains a step**: after replay, a replica compares its
  last applied `(epoch, seq)` per tablet with the primary's and catches up before serving
  ([C7](failover.md#a-returning-node)).

## Invariants to uphold

- **A follower never acks a record it has not made durable to its own configured standard.** The
  ack is sent from the flushed sweep, never from the receive path.
- **A primary never releases a response below `needed` acks**, and its own durability is one ack,
  never more.
- **`seq` is never reused within an epoch, and a follower never applies out of order.** A gap is a
  catch-up, not a skip.
- **A write is refused before commit when a quorum is not `Up`.** The primary's log never holds a
  write whose client was told `Unavailable`.
- **The bytes in a `Replicate` are the bytes in the primary's log.** Nothing re-serializes an
  intent between `commit` and the follower's `StreamWriter`. A follower's log replays to the
  primary's state because it *is* the primary's log.
- **`Replicate` carries bytes and never a `Partition`.** The `Send` rule, restated because this is
  the message most likely to be "optimized" into carrying one.
- **Every advance of `acks` is followed by a wakeup** — `DataFlushed` for the local one,
  `Replicated` for a remote one. F5's contract, with a second sender.

## Prerequisites

[C4](tablet-map.md) for `primary` and `epoch`; [C2](transport.md) for `Replicate` and
`ReplicateAck`; [C3](membership.md) for `Up` and `Down`, which decide `needed`;
[F5](../features/flushed-sweep-gate.md) for the wakeup contract;
[F11](../features/error-channel.md) for `Unavailable`; [C1](node-identity.md) for the format in the
marker.

## How it would be measured

`macro/cluster/nodes/{1,2,3}/rf/{1,3}/cl/{one,quorum}` at the grid's reference cell —
`macro/grid/unsorted/r50/1024`, 50% reads, kilobyte rows — which is the structural twin of the
`shards` configuration sweep (`shoal-bench/src/workloads/conf_sweep.rs:286`): one cell, one axis
moved at a time. Three of its arms are the questions this page has to answer:

| Arm | Question |
| --- | --- |
| `nodes/1/rf/1/cl/one` against today's reference cell | What the header and the gate cost with nothing to replicate. **Expected: inside the noise band.** This is the "no drastic slowdown" criterion made falsifiable |
| `nodes/3/rf/3/cl/quorum` against `nodes/3/rf/1/cl/one` | What replication costs on the same cluster |
| `nodes/3/rf/3/cl/one` against `cl/quorum` | What the wait costs, separately from the fan-out |

[C10](performance.md#emulating-a-cluster-on-one-machine) says why the persistent arms of that
sweep measure device contention as much as replication on one machine, and why the ephemeral
controls are read beside them.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `a_quorum_write_is_on_two_logs_before_the_ack` | Read both followers' active logs on ack; at least one holds the record | M4 |
| `an_all_write_is_on_every_log_before_the_ack` | All three | M4 |
| `a_one_write_waits_for_nobody` | Pause both followers; the write acks | M4 |
| `a_down_follower_does_not_delay_a_quorum_ack` | Kill a follower; writes ack at quorum without waiting for `primary_failover_after` | M4 |
| `too_few_replicas_up_is_unavailable_before_commit` | Kill two of three; the write is `Unavailable` and the primary's log does not hold it | M4 |
| `every_replica_converges_after_quiescence` | 100k mixed writes at quorum, wait for lag 0, digest every replica's partitions: identical | M4 |
| `a_followers_log_replays_to_the_primarys_state` | Restart a follower from its log alone; digest equals the primary's | M4 |
| `a_stale_primarys_replicate_is_refused` | Inject an old epoch; the follower refuses and answers with its epoch | M4 |
| `a_gap_is_caught_up_not_skipped` | Drop one `Replicate` frame (test proxy); the follower asks for it and applies in order | M4 |
| `rotation_does_not_release_below_quorum` | Rotate the primary's log with followers paused; nothing is released until they ack | M4 |
| `a_single_node_writes_format_two` | With no `cluster:` block, records carry `epoch 0` and increasing `seq` | M4 |
| `a_format_one_log_is_refused` | Startup names the format it found and the one it wanted | M4 |
| `an_async_follower_acks_before_its_fsync` | Configure one follower `Async`; its ack precedes its `synced_pos` | M4 |

## Related

- [Storage Overview — Durability model](../storage/overview.md#durability-model), [The Intent Log](../storage/intent-log.md) — the single-participant version of this protocol
- [Recovery](../storage/recovery.md) — the replay a follower's log goes through, unchanged
- [F5](../features/flushed-sweep-gate.md) — the wakeup contract this page adds a second sender to
- [C4. The tablet map](tablet-map.md) — where `primary` and `epoch` come from
- [C6. Reads](reads.md) — what a `One` read may not see, and the override that sets `needed`
- [C7. Failover](failover.md) — the window a primary costs, and the catch-up this page's gaps use
- [C12 — Kafka](prior-art.md#kafka), [Kudu, TiKV and CockroachDB](prior-art.md#kudu-tikv-and-cockroachdb), [ScyllaDB](prior-art.md#scylladb) — the three models this one sits between
- [item 15](../appendix/known-issues.md#15-no-backpressure-anywhere) — `PendingResponse` was unbounded before and is longer-lived now
