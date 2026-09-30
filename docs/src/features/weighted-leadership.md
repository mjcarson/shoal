# F58. Leads weighted by what each member can commit

## Context

A group's lead goes back to its placement primary
([O63](../appendix/optimizations.md#o63-leadership-never-returns-to-a-groups-placement-primary)), and
the placement spreads primaries evenly over the members. On the lab that means europa, a 16-core
Zen4 on an Optane, leads the same third of the groups as each four-core Zen1 on a SATA-class
NVMe. [#182](../appendix/resolved/slow-link-leadership.md) moved leads off a node whose *links*
are far slower than its peers', which is the extreme case. The todo
([leadership is spread evenly](../appendix/todos.md#leadership-is-spread-evenly-whatever-each-member-can-do))
asked for the general one: leads in proportion to what a member can commit. It gave two options,
choosing primaries by weight or weighing by measured commit latency, and it warned that the
first "changes the placement's primaries, which every group identity and every initialization
reads".

That warning is right, and this feature does not change the primaries. What it changes is where
the balancer sends a lead.

## What it does

Each member has a **lead weight**. It is set by `cluster.lead_weight` in its `shoal.yml`, or by
`lead_weight` on a node or a group in an inventory. It is recorded in the member's record when
the member observes itself, and carried to every shard in the map (`MapMember::lead_weight`).
Absent or zero counts as one.

`TabletMap::preferred_leader(spec)` names the voter a group's lead belongs with:

- **When every voter has the same weight**, which is the case for a cluster that sets none, it
  is the placement primary, `voters[0]`. That is O63 unchanged.
- **Otherwise** it is the up voter with the highest weighted rendezvous score for the group:
  `-weight / ln(u)`, with `u` in (0, 1) drawn from the group's and the node's identities by
  SplitMix64's finaliser. A voter wins a group with probability proportional to its weight
  among that group's voters.

`balance_leadership` hands a lead to that voter instead of to the primary. Everything else about
the handback is O63's: at most one group a shard every five seconds, only a lead held for ten,
only when every voter is within 16 entries of the log, never a group a repair, move, backup or
restore is working on, and only after the target says it `MayLead`. A node that says it may not
lead is refused under the append reserve
([#156](../appendix/resolved/wal-failure-stops-the-node.md)) or while its links are slow
([#182](../appendix/resolved/slow-link-leadership.md)), and its group stays where it is.

## Design choices

- **The weight is static and set by the operator.** It says what a machine can do, as `weight`
  does for bytes ([F46](capacity-rebalancing.md)). A lab host's disk and cores do not change
  between restarts.
- **Rendezvous, not a count to balance to.** Each shard decides for its own groups with only the
  map, as O63 does. There is no global tally of who leads what, nothing is committed, and every
  node computes the same answer. A voter that goes down loses only the groups it would have
  led, and they come back when it returns.
- **Equal weights short-circuit to the primary**, so a cluster with no weights behaves exactly
  as before, down to which member leads which group.
- **The initial election still prefers the primary.** `start_group` reads the placement, and so
  does every group identity. The balancer moves the lead once it has settled.

## Alternatives rejected

- **Choosing primaries by weight.** The primary is `voters[0]` in the placement rule. It decides
  which member initializes a fresh group and names the group's identity
  (`GroupId::of(table, rule_replicas_of)`). Changing it moves identities, which is a migration,
  not a setting.
- **Weighing by measured commit latency.** The todo's second option. A member's commit latency
  depends on where the leads already are: a node leading more commits slower. So a controller
  on it needs hysteresis to avoid moving leads back and forth, and the lab has not yet shown
  that a static split gains anything. It is filed as the follow-up, to be built only if the
  static weights earn it.
- **Counting leads per node and handing groups off until the counts match the weights.** That
  needs a view of every shard's leads, which no shard has, or a leader-side controller with a
  committed plan. Rendezvous reaches the same shares with neither.

## Limitations

- **A weight is a restart.** It is recorded when the member observes itself, like `weight`.
- **The shares are over each group's voters.** At a factor of three on three nodes, every node
  votes in every group, so 2:1:1 is half, a quarter and a quarter. On a larger cluster a node
  leads its weight's share of the groups it holds, not of all groups.
- **Nothing measures whether a weight is right.** `shoaladm stats` shows the groups each member
  leads and the busiest groups' leaders, but not the commit latency behind them.
- **The inventory wizard carries a `lead_weight` through an edit unchanged** but has no field to
  set one. It is written by hand, as `failover` once was.

## Invariants to uphold

- **`preferred_leader` is a pure function of the map and the group.** Every shard must reach the
  same answer, or two shards hand the same lead back and forth. Nothing from a node's own clock,
  metrics or process may enter it.
- **Equal weights must return `voters[0]`.** O63's behaviour, and the fixture tests built on it,
  depend on that.
- **The balancer's guards stay in front of the target.** A lead is only handed to a voter that
  is up, caught up, not busy with an operation and willing (`MayLead`). The weight chooses among
  those, and never overrides one.

## Performance

Measured on the lab in [round 12](../cluster-testing/performance.md#weighted-leadership), with
europa (16 cores, Optane) at `lead_weight: 2` against two four-core Zen1 hosts at one, interleaved
with even weights on one running cluster. Europa led 21 of 36 groups. The mixed bench ran about 9%
faster (118,600 against 108,400 operations a second), reads' p99 fell by about a third (20 ms
against 30) and writes' by 13% (164 ms against 190). A write-only load did not move, since every
member applies every write and the Zen1 hosts' disk flushes pace it whoever leads. These are lab
runs, not captures.

## Tests

| Test | What breaks if the feature is reverted |
| --- | --- |
| `leads_follow_the_voters_lead_weights` (`shoal-core/src/server/map.rs`) | Equal weights stop giving the primary, 2:1:1 over 6,000 groups leaves the shares outside 45–55% and 20–30%, an answer changes when asked again, or a down voter is chosen |
| `leads_follow_the_members_lead_weights` (`shoal/tests/cluster_fixture.rs`) | A three-node cluster at 4:1:1 settles with the heavy node leading no more than half the groups |
| `a_returning_node_is_handed_back_its_groups` (`shoal/tests/cluster_fixture.rs`) | With no weights, a returning node is no longer handed back its share (O63) |
| `a_group_split_renders_the_roots_the_engine_claims` (`shoal-bench/tests/deploy_render.rs`) | A group's `lead_weight` in an inventory does not reach the engine's `cluster.lead_weight` |

## Related

- [O63](../appendix/optimizations.md#o63-leadership-never-returns-to-a-groups-placement-primary),
  the handback this redirects.
- [#182](../appendix/resolved/slow-link-leadership.md), which takes leads off a node with slow
  links whatever its weight.
- [F46](capacity-rebalancing.md), whose `weight` does for bytes what this does for leads.
