# 143. A write to a leader cut off by dropped packets waited out its whole deadline

## Symptom

On the lab, hyperion was cut off from its peers' data and control ports by dropped packets for
20 seconds, with its client port still reachable
([cluster testing](../../cluster-testing/correctness.md#partition-one-node)). The whole cluster's
throughput went to zero from two seconds into the partition until five seconds after it healed,
reads included. Every five seconds exactly 1,024 writes failed `OutcomeUnknown`: eight bench
workers × 128 in flight. Nothing acknowledged was lost.

## Cause

A write's coordinator proposes through its local replica, which hops the write to the group's
leader when it is not the leader itself (`propose_through`, `ShardPeer::propose`). Until the group
elects around a cut-off leader, which takes the lease and an election timeout, every coordinator
still believes the cut-off node leads the groups it led. A peer link over which packets are
dropped stays up: nothing resets it, and every RPC over it waits for its deadline. So each hop to
hyperion waited the full `write_timeout` (5 s) and failed `OutcomeUnknown`. A client with a fixed
window of outstanding queries, which is every pipelined client, then filled its window with those
writes and sent nothing else, so the cluster served nothing while it could have served
everything but hyperion's twelve groups.

A kill does not do this: the process's sockets close, the hop fails at once with `NotLeader`, and
the client moves on. A reset partition (the fixture's `cut`) does not either. Only a silent one
does, and the fixture had no silent partition.

## Evidence

**Reproduced on the lab, then against the unfixed tree** with `a_write_to_a_silently_cut_leader_fails_fast`.
It uses three nodes at the default failover base, with node one leading the key's group,
blackholes node one's lanes (the fixture's new `blackhole`: connections stay up and carry
nothing), waits three seconds, and writes through node zero. With the silence check disabled:

```text
a write to the cut-off leader's group was answered in 5.001489893s: Err(Server { ..., code: OutcomeUnknown, msg: "the replication rpc timed out" })
a write to a silently cut leader failed OutcomeUnknown, not a retriable refusal
```

With it: answered in 1.6 ms, `NotLeader`, "e2af90f2-… has answered nothing on the replication lane
for 2.658s; the write was not sent".

## The fix

`ReplicationLink` keeps `waiting_since`: since when RPCs have been outstanding on the link with no
answer to any of them. It starts when the first request goes out on an idle link, restarts with
every answer, and clears when nothing is pending. `ShardPeer::propose` refuses a hop over a link
silent for `HOP_SILENCE` (two seconds) with `RpcFailure::NotSent`, which the proposal answers as
`NotLeader`: definite, retriable, immediate (`shoal-core/src/server/replication/network.rs`).

Every shard leads some groups that replicate to every other node, so a link carries requests,
heartbeats at least, every tenth of the failover base. Two seconds of silence while requests are
outstanding is a peer that is cut off or stopped.

On the lab, the rerun served 60,000–110,000 operations a second from two seconds into the partition
until the next problem appeared, with writes to hyperion's groups refused `NotLeader` at once:

| Seconds of partition | Before | After |
| --- | --- | --- |
| 0–2 | 18,000 then 0 | 11,000 then 0 (the silence has not yet reached two seconds) |
| 2–17 | 0, with 1,024 `OutcomeUnknown` every 5 s | 22,000–110,000, with writes to hyperion's groups refused |

What followed, elections repeating between europa and titan for hyperion's groups and journald
suppressing tens of thousands of lines, is the subject of
[O65](../optimizations.md#o65-heartbeats-to-followers-that-just-acknowledged-replication).

## Alternatives rejected

- **Application-level pings on the replication lane.** The control thread already pings every
  member on the control lane, and the replication lane already carries a heartbeat per group every
  tenth of the base. The answers to what is already sent say the same thing without a second
  timer.
- **Using the failure detector's verdict.** The detector commits `Down` after its own phi
  threshold, many seconds in, through the control group. A shard's own link knows first.
- **A short fixed deadline on hops.** A healthy leader under load commits in hundreds of
  milliseconds to seconds, and a deadline short enough to help here would fail writes to it.
  Silence while requests are outstanding is a property of the link, not of any one write.

## Invariants to uphold

- **A hop over a silent link is never sent.** The refusal is `NotSent`, and so `NotLeader`, only
  because nothing reached the peer. A write that was sent keeps its deadline and its unknown
  outcome.
- **`waiting_since` moves only with the pending set.** Starting it on the first outstanding
  request, never on an idle link, is what keeps a link that was merely quiet from reading as
  silent.

## Still open

- The first two seconds of a silent partition still hold hops to the cut-off node.
- A coordinator on the cut-off node itself still proposes locally to the groups it leads, which
  cannot commit, and waits `write_timeout` for each. A client connected only there sees what the
  whole cluster used to.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_write_to_a_silently_cut_leader_fails_fast` (`shoal/tests/cluster_fixture.rs`) | A write hopped to a leader cut off by dropped packets waits out its deadline and fails `OutcomeUnknown` |
| Partition one node ([cluster testing](../../cluster-testing/correctness.md#partition-one-node)) | One silently partitioned node takes the whole cluster's throughput to zero |

## Related

- [Resolved #106](isolated-member-term-inflation.md), where an isolated shard stops standing for
  election.
- [C2](../../distributed/transport.md), the transport and its lanes.
