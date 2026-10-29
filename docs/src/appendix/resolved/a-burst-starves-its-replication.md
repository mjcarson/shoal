# 284. A burst of writes on a leader starved its replication, and a follower gave its hops up as unknown

A write sent to a group's follower is hopped to the leader over the replication lane, and a hop
whose leader falls silent is given up rather than waited on for its whole deadline
([Resolved #143](silent-partition-hops.md)). A write sent once and given up that way is answered
`OutcomeUnknown`. [Resolved #129](overload-sheds.md) made an overloaded group answer every write
by name, committed or `Shedding`, and `an_overloaded_group_sheds_rather_than_timing_out` holds
that for writes through the leader and through a follower alike.

Filed from the record of [Tests That Fail Under Load](../flaky-tests.md) (F90's run on); fixed in
the flaky-test plan's first wave (2026-10-27).

## Symptom

In seven loaded workspace runs the row's writes through the follower were answered
`OutcomeUnknown`: all 8,000 in F95's run, and 6,892, 4,209 and 6,735 in others, the rest `NotLeader`.
The writes through the leader were answered by name every time. Each run passed alone or at three
threads.

## Cause

The row's last loaded failure, caught with the new messages, said the leader had sent nothing at
all: *the peer has answered nothing on the replication lane for 1.048s; the request was sent and may
yet land (the member last sent 1.048s ago, its node 1.048s ago)*. Three things together:

- **A shard's loop took every message it had queued before any other task of its executor ran.**
  A receive from a queue that holds messages is ready at once, so the loop never yielded. A bundle
  of thousands of writes is thousands of messages. While the loop worked through them, none of the
  executor's other tasks ran: openraft's cores and their replication streams, the peer links, the
  timers. The leader sent nothing and answered nothing.
- **A replication link's answers reached the network only through that same queue**
  (`ServerMsg::Peer`). Even with the loop yielding, the answers to the leader's own appends waited
  behind the burst, its lease lapsed (*no quorum acknowledged it within 1s*), and the writes it had
  taken were given up as unknown.
- **A follower judged a member silent by its own requests' answers alone.** A leader whose answers
  wait behind a burst is silent by that rule, though its appends and heartbeats still reach the
  follower.

## Evidence

**Caught in a loop, then reproduced.** The row was looped beside the fixture's heaviest rows on the
tree before the fix, with the hop messages extended to say what the member last sent. Its third
round failed with the message above. Its earlier rounds, and the loop's first two runs, are on
[the flaky page](../flaky-tests.md#runs) with the two that were discarded: in one, every child
logged at DEBUG; in the other, a lane test beside it spent the user's locked memory.

`a_burst_on_a_leader_leaves_its_hops_answered` was written first. Node zero leads the keys' group
and takes a bundle of 4,000 writes. Its loop is held half a millisecond longer over each message, as
a loaded host holds it (`SLOW_MESSAGES 4000 500`): two seconds of work, past the 1 s a hop's silence
is judged at here. Ten writes through node one hop to it meanwhile, each sent once. Each piece of the
fix was taken out in turn:

| The loop yields | Answers reach the network directly | Silence counts what the member sends | The burst through the leader | The hops through a follower |
| --- | --- | --- | --- | --- |
| no | no | no | 4,000 `NotLeader` | given up unknown; the member last sent 1.004 s ago |
| yes | no | no | 2,975 `NotLeader`, 1,025 `OutcomeUnknown` | given up unknown; the member last sent 170 ms ago |
| yes | no | yes | 2,975 `NotLeader`, 1,025 `OutcomeUnknown` | the first refused `NotLeader`, its leader's lease lapsed; the rest done |
| yes | yes | no | 3,230 done, 770 `Shedding` | given up unknown; the member last sent 169 ms ago |
| no | yes | yes | 4,000 `NotLeader` | given up unknown; the member last sent 1.004 s ago |
| yes | yes | yes | 3,230 done, 770 `Shedding` | every one done, the first after 2.4 s |

## The fix

- **A shard's loop yields between messages once its slice is spent** (`glommio::yield_if_needed`).
  The executor's cores, links and timers run through a burst.
- **A replication link's answers reach the network from the link's own task**
  (`ShardNetwork::link`), never through the loop's queue. A link's drop still goes through the loop.
- **A member's requests unanswered past the hop silence count as its silence only while the member
  has sent nothing for as long either** (`member_quiet_for`). A member is heard from by every answer
  on this shard's link to it and by every request it sends for a group this shard hosts
  (`heard_from_member`, kept by member). The kernel's verdict and a node's silence count as before.
- **A hop given up or refused for silence says what the member and its node last sent**
  (`heard_of`).
- `SLOW_MESSAGES` (`faults::slow_messages`) holds a node's shard loops longer over each of their next
  messages. The overloaded row keeps each code's first message.

## Alternatives rejected

- **Lengthen the hop silence.** A hop to a stopped peer would wait longer, which is what #143's first
  seconds closed.
- **Judge silence by the kernel's acknowledgements alone.** A stopped process's kernel acknowledges
  as a busy one's does (#143).
- **A queue of its own for all replication traffic, taken ahead of client queries.** It would also
  serve inbound appends and hops ahead of a burst, but it changes the order the loop judges
  admission in, and the row did not need it. Recorded in
  [TODOs](../todos.md#inbound-replication-ahead-of-a-burst).
- **Yield inside each query's handling.** The loop's messages are its unit of work. A yield between
  them leaves every handler's ordering as it was.

## Invariants to uphold

- **The shard loop never takes a message past its slice without yielding.** Every task of the
  executor waits on it otherwise.
- **A replication answer never waits in the loop's queue.** A leader's lease and every RPC's future
  depend on it.
- **A member is silent by its own link only while it sends nothing.** One that sends is waited on to
  the RPC's deadline, and its answer comes.
- **Silence is judged by member**, so a wedged slot is silent while its node's others talk
  ([F84](../../features/replication-lane-handoff.md)).

## Still open

- **An inbound replication request still waits behind a burst in its shard's loop.** That is a
  hop, another leader's append, or a vote. A hop is answered after the burst (2.4 s in the row), and
  a follower's answers to other leaders wait too. Those leaders do not judge it silent while it
  sends, but their commits wait on the rest of the quorum. Recorded in
  [TODOs](../todos.md#inbound-replication-ahead-of-a-burst).
- **[Item 215](../known-issues.md#215-a-replication-lane-busy-with-wide-rows-is-judged-silent-and-refuses-forwarded-writes)'s
  lab case**, a lane busy with wide rows over a full 1 GbE link, is the third rule's other case. It
  has not been run on the lab since, and stays open until it is.
- **The yield and the bypass are on every query's path**, and the lab's before and after has not
  been taken. Recorded in [TODOs](../todos.md#the-shard-loops-yield-measured-on-the-lab).

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_burst_on_a_leader_leaves_its_hops_answered` (`cluster_fixture`) | Each piece taken out, as in the table above |
| `an_overloaded_group_sheds_rather_than_timing_out` (`cluster_fixture`) | Under load, a follower's 8,000 hops given up unknown, the leader silent for 1.05 s |
| `a_write_to_a_silently_cut_leader_fails_fast`, `a_silent_partitions_first_seconds_hold_no_writes`, `a_silently_cut_node_rejoins_without_elections` (`cluster_fixture`) | Unchanged: a member that sends nothing is still given up at the silence bound |

## Related

- [Resolved #143](silent-partition-hops.md), the silence rule this narrows.
- [Resolved #129](overload-sheds.md), the gate whose answers the row holds.
- [F84](../../features/replication-lane-handoff.md), a link per member.
- [Item 215](../known-issues.md#215-a-replication-lane-busy-with-wide-rows-is-judged-silent-and-refuses-forwarded-writes).
- [Tests That Fail Under Load](../flaky-tests.md).
