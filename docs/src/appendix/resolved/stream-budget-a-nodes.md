# 204. The stream budget was built for a shard and documented for a node

## Symptom

`cluster.migration.stream_bytes_per_sec` is described everywhere as one token bucket for a node:
the configuration's comment ("One bucket per node, not per device"),
[C8](../../distributed/rebalancing.md#transfer-budgets) ("one token bucket per sending node across
every stream and group"), the [configuration page](../../getting-started/configuration.md), the
[glossary](../glossary.md) and `CLAUDE.md`. A node of six executors could send six times it: 384 MiB a
second at the default of 64, where its operator set 64. The bound meant to keep a move's or a repair's
streams away from the foreground was as loose as the node had cores. On the lab it was hidden by the
network, since 1 GbE carries about 117 MiB a second whatever the budget says.

## Cause

Each shard built its own bucket from the whole setting. `ShardNetwork::new`
(`shoal-core/src/server/replication/network.rs`) made a `RateLimiter` from the `stream_bytes_per_sec`
each shard's start passed it, held in a `RefCell` on the shard's own executor, and `send_snapshot`
drew every chunk of every stream that shard sent from it. The limiter's own comment said "One per
shard network", one sentence of [F46](../../features/capacity-rebalancing.md) said the same, and every
other page said a node. The fixture's budget rows ran one shard a node, so none could see it.

The limiter's doc said the debt of a take larger than the bucket was carried; it never was, and a
chunk larger than a second's worth would have waited until its stream's deadline. The configuration
refuses a budget below a chunk, so nothing met that.

## Evidence

**Established by reproducing it.** `a_nodes_streams_share_one_budget_whichever_shard_sends`
(`shoal/tests/cluster_fixture.rs`) runs three nodes of four shards at a budget of 256 KiB a second,
leaves node two behind the purge point of every group, and starts it again, so each survivor feeds it
from several shards at once; it samples every shard's `bytes_sent` each 250 ms and holds each
survivor's sum to one bucket, a second's worth to begin with and then the rate. On the tree before the
fix:

```text
node 0 sent 690839 bytes from shards {0, 2, 3} in 0.3s, over one budget's bound of 402634
```

On the fix the same row holds the bound while more than one shard of a survivor sends, until node two
has caught up with both tables.

## The fix

Decided with the user (2026-11-05): one bucket a node, as every page says.

- **`StreamBudget`** (`replication/network.rs`) is the node's budget: a `Ceiling` of
  `stream_bytes_per_sec` bytes a second, a second's worth deep, or none for unlimited. `Ceiling` is
  the bucket a device's recovery and a table's scans already share across a node's executors, a mutex
  over its tokens; a take larger than it holds waits until it is full and takes all of it, so no chunk
  waits for ever.
- **Built once a node** where the shards' peer setup is (`ShoalPool::start`, `PeerSetup::stream_budget`),
  as the installing tablets are, and handed to every shard's `ShardNetwork` in place of its own limiter.
- **`send_snapshot` draws each chunk from it** and sleeps exactly the wait it is told, then asks again,
  since another executor may have taken the refill; the wait is charged to `snapshots.budget_wait_ns`
  and to the stream's deadline, as before.

`RateLimiter` is gone, its wrong comment with it.

## Alternatives rejected

- **Divide the setting by the node's executor count where each shard is built.** It keeps a bucket a
  shard and no lock across executors, but holds a shard sending alone - the common case, one move's
  learner - to a fraction of what its operator set, and every page would have had to say a shard's
  share instead.
- **A pacer task a node that hands credit out.** One more task to place, and every chunk a message to
  it and back; the lock is taken once a chunk, a 64 KiB to 1 MiB piece of a stream, which is not where a
  stream's time goes.
- **Per device.** Still not built: the device behind a table's path is a root's, which a budget could
  be keyed by since [F82](../../features/root-capacity.md), and is filed on
  [the todos](../todos.md) as before.

## Invariants to uphold

- **A node's snapshot streams together send at most its budget**, whatever executor sends them: a
  second's worth at once and then its rate.
- **Every shard's network shares the one `StreamBudget` its node built**; a network built with a budget
  of its own would bring the defect back unseen by a row of one shard a node.
- **A wait for the budget is slept exactly and asked again**, never assumed taken.
- **Waiting on the budget counts against the stream's deadline**
  (`replication.install_snapshot_timeout`, 300 s by default). A node feeding several large streams at
  once now feeds each more slowly than before, so a stream that met its deadline at several times its
  budget may now resume from its receiver's `Resume { from }` instead, keeping what it sent.

## Still open

- **Per-device and per-pair budgets**, as before ([todos](../todos.md)).
- **First come, first served across shards.** A shard whose chunk is refused sleeps its wait and may
  find another shard took the refill; with chunks of one size this evens out, but nothing orders the
  waiters as the scans' paces do ([Resolved #355](scan-pages-served-in-turn.md)). Over one second it
  need not: ws10, the workspace run of 2026-11-06, saw one of four threads in this page's unit test take
  nothing in a second while the others took the rest ([Tests That Fail Under
  Load](../flaky-tests.md#left-the-page)), which the test no longer claims. A stream runs for seconds
  to minutes inside its 300 s deadline, where losing every race is no chance at all.

## Tests

| Test | Where | Breaks if |
| --- | --- | --- |
| `a_nodes_streams_share_one_budget_whichever_shard_sends` | `shoal/tests/cluster_fixture.rs` | A shard draws on a budget of its own again, or a node's streams together pass one budget |
| `a_nodes_stream_budget_is_one_for_every_shard` | `shoal-core/src/server/replication/network.rs` | The budget stops pacing at its rate after a second's worth, refuses a chunk larger than it for ever, lets threads sharing it take more than one budget, starves one of them, or limits at zero |
| `node_transfer_budgets_bound_concurrent_sources` | `shoal/tests/cluster_fixture.rs` | A sender's streams stop waiting on the budget, at one shard a node |
| `a_restore_waits_out_a_members_stream_budget` | `shoal/tests/cluster_fixture.rs` | A restore stops riding out a full receiver |

## Related

[F46](../../features/capacity-rebalancing.md), which built the budget; [C8](../../distributed/rebalancing.md),
which promised one a node; [F119](../../features/scan-pacing.md), whose paces share the same bucket
across a node's executors; [S10](../../object-storage/recovery.md), which had to learn what the existing
budget is before designing a device's.
