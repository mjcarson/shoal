# 358. A cluster's first initialization found no control leader for fifteen seconds under load, both its voters reachable

## Symptom

In item 355's workspace run (2026-11-03), `barrier_read_observes_prior_quorum_write` failed before its
test began:

```text
Error: ChildFailed("the initialization was not applied: {\"error\":\"NotLeader: no control leader could be reached; the cluster may lack a quorum. the voters are [NodeId(4362220d-…), NodeId(6619c245-…)] and this node reaches [NodeId(4362220d-…), NodeId(4e1c3c6a-…), NodeId(6619c245-…)]; …\"}")
```

It passed alone in 6.4 s. It was the second time since
[Resolved #337](initialization-rides-out-an-election.md), which has a child ask its initialization
again for 15 s through a control election; F129r's run had met it in
`node_down_inside_its_grace_rebuilds_nothing`. Both voters reached each other, so a group of two
voters that talk should have elected within a few election timeouts.

## Cause

**The control group threw away every append answer later than 50 ms.** openraft budgets an append
or a heartbeat at the group's heartbeat interval, and the control group runs openraft's defaults:
a heartbeat every 50 ms, elections at 150 to 300 ms. `ControlPeer::append_entries`
(`shoal-core/src/server/control/network.rs`) waited exactly `option.hard_ttl()` and dropped a later
answer as *the control rpc timed out*. A follower on a loaded host - one whose control log's
`fdatasync` or whose executor was slow - answered in 100 ms and had every answer thrown away. Its
leader never learned what it had matched and committed nothing, and the follower, which went on
hearing the leader, refused every pre-vote, so nobody stood. A leader existed, every voter reached
every other, and nothing committed: the proposal waited out its 10 s deadline, which `propose`
answers as *no control leader could be reached*. It was
[Resolved #190](append-answer-thrown-away.md)'s defect, fixed then for the data groups alone,
whose links gained a floor on an append's wait; the control group's did not.

**A control connection answered its requests strictly in turn.** `serve_control`
(`control/listener.rs`) awaited a membership request's answer - a join, a status report, a forwarded
proposal - before reading the next request, and a forwarded proposal waits at the leader until it
commits, up to 10 s. Every append, vote, pre-vote and ping its sender sent after it on the same
connection waited as long, past every budget. Not what item 358's run met, which proposed through
its own node, but the same kind of wait on the same lane, found reading it.

Two more things beside them: the control group sent a heartbeat to a follower that had just
acknowledged replication, which the data groups stopped doing at
[O65](../optimizations.md#o65-heartbeats-to-followers-that-just-acknowledged-replication), so a
group whose appends are answered late still had heartbeats that time out at openraft's own 50 ms;
and openraft's heartbeat worker drops its call at its budget, which left the RPC's waiter in the
link's map until a late answer or the link's end came for it.

## Evidence

**Established by reproducing it**, deterministically, and by counting it under load.

- `a_control_leader_hearing_its_appends_late_still_commits` (`shoal/tests/cluster_fixture.rs`) holds
  every answer each node's control links read 100 ms (`faults::delay_control_answers`,
  `SLOW_CONTROL_ANSWERS 100`): past an append's 50 ms budget, inside a vote's 150 ms. On the tree
  before the fix the cluster's initialization failed with item 358's own text, every voter reached:

  ```text
  Error: ChildFailed("the initialization was not applied: {\"error\":\"NotLeader: no control leader could be reached; the cluster may lack a quorum. the voters are [NodeId(78249a7c-…), NodeId(9ca875a6-…), NodeId(a4e75259-…)] and this node reaches [NodeId(78249a7c-…), NodeId(9ca875a6-…), NodeId(a4e75259-…)]; …\"}")
  ```

  On the fix it commits within a few seconds.
- `a_forwarded_proposal_holds_no_ping_behind_it` has the control leader hold its answers to
  forwarded proposals 4 s (`faults::hold_propose_answers`, `HOLD_PROPOSE_ANSWERS`), a follower forward
  one, and then ping the leader over the same connection. Before the fix:

  ```text
  a ping behind a forwarded proposal took 3803341 us: {"ok":{"micros":3803341}}
  ```

  On the fix the ping is answered at once while the proposal is still held.
- **Under the suite's load**: the two rows that met item 358, run beside the fixture's seven heaviest
  rows at six threads (`target/lab/found/i358/count-loop.sh`), their children logged at INFO, the
  control RPCs that timed out counted a round, six rounds a build:

  | Build | Control RPCs timed out, a round | Heartbeats failed, a round | Elections, a round | Rows failed |
  | --- | --- | --- | --- | --- |
  | Before (`89ff6f2c`) | 44, 36, 76, 58, 82, 63 | 522 to 591 | 44 to 48 | 0 of 12 |
  | The fix (`01f72ed4`) | 0, 0, 0, 0, 0, 0 | 517 to 569 | 44 to 52 | 0 of 12 |

  The rows passed in both: the full fifteen seconds without a commit is the rare end of what every
  round met. Every one of those timed-out control RPCs was an answer thrown away, and on the fix there
  are none. The heartbeats and elections are mostly the data groups', the same either side; a control
  heartbeat still fails at openraft's own budget (below).

## The fix

- **A control append or heartbeat waits at least a second for its answer**
  (`CONTROL_APPEND_FLOOR`): an answer is worth having for the commit it advances, and a link that drops
  fails what it carried at once, so only a hung but connected peer pays the second. The data groups'
  floor's default, as #190 chose it. Votes and pre-votes keep openraft's budget, the election timeout's
  low end, which #190 left alone too.
- **The control group suppresses a heartbeat to a follower that just acknowledged replication**
  (`heartbeat_min_interval: Some(50)`, O65's setting): 50 + 50 + a 10 ms tick is under the 150 ms
  election timeout openraft holds it to.
- **An RPC's waiter leaves the link's map on every way out** (`PendingGuard`), its caller's own timeout
  among them.
- **A control connection answers a membership request whenever the control loop does**: each waits on a
  task of its own and one writer frames every answer as it is ready, under its request's id, which the
  asking link already matches answers by, so no frame changed. The consensus requests and pings are
  answered in turn as before. A connection may have 64 membership requests waiting at once before the
  next is answered in turn, and a joiner's connection, which carries its join and its pings and ends on
  a refusal, is answered in turn as it was.

## Alternatives rejected

- **Lengthen the ride-out of a cluster's initialization**, Resolved #337's 15 s. The logs and the row
  show a leader that commits nothing, which no wait mends; the item's fix direction ruled it out unless
  the elections each took their time, and they did not.
- **Scale the control group's timeouts with the failover base**, as the data groups' are. It would
  lengthen every control failover to hide answers that a floor accepts; a vote answered past 150 ms would
  still be late, and the rows show the appends are what stalled.
- **A floor on votes too.** A vote answered after its candidate's timeout decides an election that has
  moved on, and #190's reasoning kept a vote's budget; nothing seen here put votes past it.
- **Spawn every control request, consensus ones included.** openraft's appends and votes from one
  peer are meant to be applied in the order they were sent; only the membership requests, which the
  control loop answers on its own time, needed to leave the line.

## Invariants to uphold

- **A control append's or heartbeat's answer is waited for at least `CONTROL_APPEND_FLOOR`**, whatever
  openraft's budget, and a dropped link fails it at once.
- **No membership request holds a consensus request read after it on the same connection**; the
  consensus requests on a connection are answered in the order they were read.
- **Every answer on a control connection carries its request's id**, and the asking link completes
  requests by id, never by order.
- **The control group's `heartbeat_interval + heartbeat_min_interval` plus a tick stays under its
  election timeout's low end.**

## Still open

- **openraft's heartbeat worker bounds its own call at 50 ms**, so a heartbeat answered late still
  fails and is logged; with suppression a follower acknowledging appends is not sent one, but an idle
  control group's heartbeats are, and nothing it does depends on them.
- **Resolved #337's other two failures** (F90's `an_installing_copy_is_refused_a_walk`, #322's
  `a_restore_refuses_a_bucket_missing_an_archive`) had the same shape, two voters and a third being
  promoted; the promotion's joint configuration commits through the same appends, so they were likely
  this too. Not shown.

## Tests

| Test | Where | Breaks if |
| --- | --- | --- |
| `a_control_leader_hearing_its_appends_late_still_commits` | `shoal/tests/cluster_fixture.rs` | A control append's answer past openraft's budget is thrown away again |
| `a_forwarded_proposal_holds_no_ping_behind_it` | `shoal/tests/cluster_fixture.rs` | A control connection answers a membership request in turn again, holding what follows it |
| `an_initialization_rides_out_a_moment_with_no_control_leader` | `shoal/tests/cluster_fixture.rs` | The ride-out Resolved #337 built stops working beside the floor |
| `control` (`shoal-core`, the control store's and runtime's conformance suites) | `shoal-core/src/server/control/` | The control group's configuration or its listener breaks openraft's contract |

## Related

[Resolved #190](append-answer-thrown-away.md), the data groups' floor;
[Resolved #337](initialization-rides-out-an-election.md), the ride-out of an election;
[O65](../optimizations.md#o65-heartbeats-to-followers-that-just-acknowledged-replication), heartbeat
suppression; [F39](../../features/membership.md), the control group and its lane.
