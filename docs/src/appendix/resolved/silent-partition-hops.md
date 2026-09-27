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

`ShardPeer::propose` refuses a hop to a peer that has been silent for four of the groups'
heartbeats, and never less than two seconds, with `RpcFailure::NotSent`, which the proposal
answers as `NotLeader`: definite, retriable, immediate
(`shoal-core/src/server/replication/network.rs`). Silence is judged both ways:

- **outbound:** `ReplicationLink::waiting_since`, since when this shard's RPCs over the link have
  had no answer. It starts when the first request goes out on an idle link, restarts with every
  answer, and clears when nothing is pending;
- **inbound:** `ShardNetwork::heard`, when anything last arrived from the peer over the replication
  lane, a request it sent or an answer to one of ours.

The first cut judged only the outbound side, and the fixture test passed three runs in five: a
coordinating shard that led no group replicating to the cut-off node had nothing outstanding on its
link, so its hop was the first request and waited out its deadline. The inbound side covers it: a
hop goes to a node believed to lead a group this shard follows, and a leader heartbeats its
followers every tenth of the failover base, so it is heard from several times a second while it
is reachable. The threshold follows the base (`set_failover_base`, on every map install), so a long
base does not make a healthy leader look silent.

On the lab, the rerun served 60,000–110,000 operations a second from two seconds into the partition
until the next problem appeared, with writes to hyperion's groups refused `NotLeader` at once:

| Seconds of partition | Before | After |
| --- | --- | --- |
| 0–2 | 18,000 then 0 | 11,000 then 0 (the silence has not yet reached two seconds) |
| 2–17 | 0, with 1,024 `OutcomeUnknown` every 5 s | 22,000–110,000, with writes to hyperion's groups refused |

What followed, elections repeating between europa and titan for hyperion's groups and journald
suppressing tens of thousands of lines, is the subject of
[O65](../optimizations.md#o65-heartbeats-to-followers-that-just-acknowledged-replication).

### The first seconds, closed

The fix above left two waits, and the lab showed both on every partition and pause since
([cluster testing, section 11](../../cluster-testing/correctness.md#11-overload-silence-and-a-nearly-full-disk)):
the first two to three seconds of a silent partition ran at zero throughput, reads included. A
hop sent just before the leader went silent still waited its whole deadline, and so did a write
through the cut-off node itself. The cut-off node's leader kept taking writes for openraft's
lease, `election_timeout_max`, which is ten seconds at the default base, and each waited out
`write_timeout`. Three changes close them:

- **A hop in flight gives up once its leader is judged silent.** `ReplicationLink::rpc_watched`
  waits for the answer in steps of 100 ms and asks the same silence judgement between them. A
  write that was sent may still land, so it ends `Unreachable`, which the proposal answers
  `OutcomeUnknown`, at the threshold instead of the deadline. Read barrier hops, which skipped
  the silence check altogether, now take both the refusal before sending and the watch.
- **A leader with no quorum in reach takes no write.** `Lease::quorum_quiet` counts the group's
  voters whose nodes this shard's network has heard from within the hop silence, itself
  included. A leader with fewer than a quorum in reach refuses a write `NotLeader` before
  `client_write`, so nothing is appended, and refuses a read barrier `QuorumUnavailable`. A
  write it already took is watched the same way while it waits in openraft, and given up on as
  unknown.
- **The threshold is three heartbeats, with a one second floor.** That is a second and a half at
  the default base, where it was four heartbeats and two seconds.

The quiet check was first cut on openraft's `last_quorum_acked`. Under a saturated load a
follower's acknowledgements queue behind its appends for seconds while its node goes on talking.
On the lab that cut refused and abandoned writes a healthy group would have committed: one
overloaded load ran at 12,027 rows a second, with 221,343 writes answered unknown and about five
million refused, where the network judgement ran at 24,050 with none of either
([Resolved #129](overload-sheds.md#evidence)).

**Reproduced first** with `a_silent_partitions_first_seconds_hold_no_writes`: a write hopped to a
blackholed leader and a write through the blackholed node, both sent at the moment of the cut, at
the default base. On the tree with only the hop watch in place, the hopped write was answered in
1.50 s and the other waited out its deadline:

```text
hopped: Err(Server { …, code: OutcomeUnknown, msg: "the peer has answered nothing on the replication lane for 1.501169128s; the request was sent and may yet land" }) in 1.503081543s; through the cut-off node: Err(Server { …, code: OutcomeUnknown, msg: "group 14e32280e0a30949 did not commit the write within the deadline" }) in 5.00213591s
```

With the leader's watch as well, both were answered in 1.50 s, and a write through the cut-off
node half a second later was refused `NotLeader` at once.

**On the lab**, the same partition as before: `iptables` dropping hyperion's peer ports both
ways for 20 s under the mixed bench (`target/lab/fault.sh`, run `r11/143-partition`), on a
freshly loaded cluster, against t05f, the last run before this change. Operations a second from
the second before the cut:

| Second | t05f, before | After |
| --- | --- | --- |
| cut −1 | 44,300 | 72,067 |
| cut | 10,784 | 25,961 |
| +1 | 372 | 22,447 (write p99 1.0 s) |
| +2 | 0 | 121,792, hyperion's groups refused |
| +3 | 0 | 122,195 |
| +4 | 25,835, hyperion's groups refused | 121,276 |

The zero seconds are gone. The two seconds after the cut run at a third of the rate, while the
hops already sent to hyperion wait for the second and a half the silence takes to be judged. From
then on the cluster served 120,000 operations a second with writes to hyperion's groups refused
(about 22,000 a second). All 620,159 acknowledged inserts were read back through each member, and
no node restarted.

## Alternatives rejected

- **Application-level pings on the replication lane.** The control thread already pings every
  member on the control lane, and the replication lane already carries a heartbeat per group every
  tenth of the base. The answers to what is already sent say the same thing without a second
  timer.
- **Using the failure detector's verdict.** The detector commits `Down` after its own phi
  threshold, many seconds in, through the control group. A shard's own link knows first.
- **Judging a quiet leader by `last_quorum_acked`.** It is openraft's own record of a quorum's
  acknowledgement, and under a saturated load it lags for seconds on a healthy group, as the lab
  run above shows. The members' nodes on the network keep talking under any load and stop only
  when they are cut off.
- **`TCP_USER_TIMEOUT` on the peer links.** It would turn a silent link into a link that is down,
  which the rest of the node already handles. But it fires only while data is unacknowledged, it
  closes the connection, which every lane then redials, and the judgement above was already
  there to use.
- **A short fixed deadline on hops.** A healthy leader under load commits in hundreds of
  milliseconds to seconds, and a deadline short enough to help here would fail writes to it.
  Silence while requests are outstanding is a property of the link, not of any one write.

## Invariants to uphold

- **A hop over a silent link is never sent.** The refusal is `NotSent`, and so `NotLeader`, only
  because nothing reached the peer. ~~A write that was sent keeps its deadline and its unknown
  outcome.~~ A write that was sent is given up on once the link is judged silent, and its outcome
  is unknown, never a refusal.
- **A leader refuses before `client_write`, and only there.** `NotLeader` from a quiet leader is
  definite because nothing was appended. Once a write is in openraft, giving up on it is
  `OutcomeUnknown`.
- **Quiet is judged on the members' nodes, never on openraft's acknowledgements.** A node is out
  of reach only when this shard's requests to it have gone unanswered for the hop silence, or
  nothing at all has been heard from it for that long (`ShardNetwork::node_silent_for`). Asking
  must never dial a link.
- **`waiting_since` moves only with the pending set.** Starting it on the first outstanding
  request, never on an idle link, is what keeps a link that was merely quiet from reading as
  silent.
- **A peer is judged silent only after it was heard from once**, and only against ~~four~~ three of
  its own heartbeats, never under a second. A node never heard from, or one whose base is long,
  is hopped to as before.

## Still open

- ~~The first two seconds of a silent partition still hold hops to the cut-off node.~~ Closed by
  the watch on a hop in flight and the second and a half threshold. What is left is that
  threshold itself: hops sent in it wait until it passes.
- ~~A coordinator on the cut-off node itself still proposes locally to the groups it leads, which
  cannot commit, and waits `write_timeout` for each.~~ Closed: its leader refuses once no quorum
  of its members' nodes is in reach.
- A client connected only to the cut-off node still gets nothing done for groups led elsewhere:
  their hops are refused `NotLeader`, and it has to move to another member.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_write_to_a_silently_cut_leader_fails_fast` (`shoal/tests/cluster_fixture.rs`) | A write hopped to a leader cut off by dropped packets waits out its deadline and fails `OutcomeUnknown` |
| `a_silent_partitions_first_seconds_hold_no_writes` (the same file) | A write hopped just before the cut, or sent through the cut-off node, waits out its deadline; a write through the quiet leader is appended |
| Partition one node ([cluster testing](../../cluster-testing/correctness.md#partition-one-node)) | One silently partitioned node takes the whole cluster's throughput to zero |

## Related

- [Resolved #106](isolated-member-term-inflation.md), where an isolated shard stops standing for
  election.
- [C2](../../distributed/transport.md), the transport and its lanes.
