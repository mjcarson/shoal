# 96. `cluster.transport.ping_interval` is parsed, documented and consumed by nothing

## Symptom

The setting said how often the control thread pinged every member over its control lane. A
file that set it to a second or an hour got the same server: the only pings sent were the ones
the fixture asked for on demand through `ControlHandle::ping`, and nothing periodic existed.
The configuration page listed a knob that turned nothing, which is the shape
[item 71](../known-issues.md#71-throughput_sensitive-is-configured-documented-and-mostly-unused)
has.

## Cause

[F38](../../features/inter-node-transport.md) added the field so that the `cluster.transport`
block would not change shape when the failure detector landed, and said so when it filed this
item: the periodic pinger was the detector's, and the detector was
[M3](../../distributed/milestones.md#m3-membership)'s. `grep ping_interval` found the field,
its default and its serde attribute, and no reader.

## Evidence

**Established by reading the source**, as the item recorded when it was filed. No test asserted
a ping was ever sent on its own; the fixture's `PING` command sends one and waits, which proves
the lane and not the timer.

## The fix

[F39](../../features/membership.md) built the consumer, and it is deliberately not the
detector. The control thread runs a ping timer at `transport.ping_interval` (`ping_timer` in
`shoal-core/src/server/control/plane.rs`) that posts a `PingTick`; `ping_members` sends every
member this node knows one control-lane ping, and `handle_pinged` records the round trip in
microseconds and the run of consecutive misses per node in a local `reachability` table. That
table is what the `Detector` admin read reports as `local`, and what a member's status report
carries to the leader as the nodes it can reach with their round trips - telemetry beside the
leader's own evidence, never a vote.

The failure detector the item expected is the leader's phi-accrual over the members' status
reports at `failure_detector.interval_ms` (`shoal-core/src/server/control/detector.rs`), which
is a different timer with a different job: a report is the member saying "I am here", a ping is
this node asking "are you there", and only the first is evidence the leader commits on.

## Alternatives rejected

**Remove the field until M3.** The item's own fix direction allowed it. Rejected because the
block's shape is what a deployment's file is written against, and M3 was the next milestone.

**Make the pings the detector's evidence.** A ping is one node's observation of one lane at one
moment; C3 says reachability is an observation that can differ between peers and membership is
a durable decision. A detector fed by every node's pings would have every node's opinion, which
is a gossip protocol, not a verdict.

**Ping only from the leader.** The reachability view is most useful where the operator is
looking, which is any node; the cost is one small frame per member per second.

## Invariants to uphold

- **A ping never changes membership or health.** `reachability` is a local table, reported and
  read; nothing proposes on it.
- **The two intervals are two settings.** `transport.ping_interval` paces pings,
  `failure_detector.interval_ms` paces reports; the detector's fit is over report arrivals and
  its floor is a quarter of that interval.
- **A miss is counted, not judged.** `misses` grows until a pong; the report carries only the
  members with none.

## Still open

Nothing on this item. The detector's verdicts are
[F39](../../features/membership.md)'s and tested there.

## Tests

| Test | What it pins |
| --- | --- |
| `cluster_fixture::fresh_failure_reports_do_not_mask_shard_failure` | A `PING` still answered by a member with a dead shard while the leader holds it up, and the `Detector` read carrying the local table beside the leader's |
| `cluster_fixture::control_elections_do_not_depend_on_data_shard_relay` | Pings answered over the control lane while every data lane is cut |
| `conf::cluster::tests::documented_cluster_defaults_match_policy_bootstrap` (`cluster_fixture.rs`) | The documented `ping_interval` default matching the parsed one |

## Related

- [F39](../../features/membership.md), which built the consumer and the detector beside it
- [F38](../../features/inter-node-transport.md), which added the field and filed this item
- [C3](../../distributed/membership.md#failure-detection), the distinction between a report
  and a probe
