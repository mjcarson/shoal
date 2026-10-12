# F83. Racks and zones above the host

## Context

[S1](../object-storage/prerequisites.md#optional) lists **a failure domain above the host** as an
optional prerequisite. Its reason is that no deployment has a rack to name, and the member's field
is a list from the start, so a level can be added without a format change.
[F81](failure-domains.md) built that field as `MemberRecord::failure_domains`, a map of levels
whose only entry was `host`, and gave its `cluster.failure_domains` block one level. It refused
`rack:` in a node's configuration and in an inventory, so a level no build placed by was never
accepted and then ignored. A pool's `failure_domain` could name `device` or `host`.

A host is not the widest thing that fails at once.
- A rack loses its power feed or its top-of-rack switch, and every host in it goes together.
- A zone - a room, a building, a cloud's availability zone - loses its racks together.

[P11](../object-storage/contract.md#the-contract) counts a stripe's chunks in distinct failure
domains. A pool that names only the host can still put every chunk of a stripe in one rack.

What to build was decided with the user on 2026-10-12, before anything was built:

- **two fixed levels above the host, nested: a zone holds racks and a rack holds hosts**, not a
  list of levels the operator defines;
- **spread by them softly, as F81 does by host**:
  - each level is committed on the record, shown in `status`, and accepted in a pool's
    `failure_domain`;
  - a drain and a voter seat prefer a new zone, then a new rack, then a new host;
  - a rebalance never lowers a set's spread at any level;
  - each falls back to the choice it made before when nothing qualifies.

## What it does

### A node names its rack and zone

```yaml
cluster:
  failure_domains:
    host: titan       # absent: the machine's own name
    rack: shelf-1     # absent: the host is a rack of its own
    zone: lab-east    # absent: the rack, or the host, is a zone of its own
```

`FailureDomains` (`shoal-proto/src/shared/domains.rs`) gains `rack` and `zone` beside `host`.
`LEVELS` lists the three, widest first: `zone`, `rack`, `host`.

- **A rack and a zone have no default.** Neither can be read off the machine, and a wrong guess
  would claim a spread that does not exist.
- **A name given has to be one.** It is held to the host's rule: not empty, at most 255 bytes, no
  whitespace or control character. It is refused at validation naming its own key, such as
  `cluster.failure_domains.rack`.
- **Any other level is still refused when the block is read.** That covers `row:`, `region:` and
  anything else no build places by.
- **They are resolved once a start, with the host.** `FailureDomains::resolve` returns the host
  and, when named, `rack` and `zone`. The record carries a key only for a level that is named.
- **A restart under another rack is committed** at the node's next incarnation, as a host is.

### A level left out is the one below it

`shoal::shared::domains::domain_at(domains, level)` is the one rule that says where a member stands
at a level:
- the name it gives at that level;
- otherwise the name at the nearest level below that it does name, returned with the level it came
  from;
- otherwise `None`, when it names nothing at or below the level. Such a member stands in a domain
  of its own, as F81's member with no host does.

| The record names | Its zone | Its rack | Its host |
| --- | --- | --- | --- |
| `host: titan, rack: r1, zone: east` | zone `east` | rack `r1` | host `titan` |
| `host: titan, rack: r1` | rack `r1` | rack `r1` | host `titan` |
| `host: titan, zone: east` | zone `east` | host `titan` | host `titan` |
| `host: titan` | host `titan` | host `titan` | host `titan` |
| nothing (before F81, or a test's record) | alone | alone | alone |

Two consequences follow:
- **A cluster that names no rack or zone plans and promotes exactly as F81's did.** Every level's
  key is then the host key.
- **A host standing in as its own rack never equals a rack of that name.** The level the name came
  from is part of the domain.

### A name is the cluster's at its level

A rack's name is not scoped to its zone. Two zones that each name a rack `r12` hold one rack as far
as the planner can tell, and two members naming one host are on one machine whatever racks they
name. A mistake in the hierarchy can therefore only make domains look more shared than they are.
It never claims a spread that does not exist.

### A drain, a rebalance and a voter seat spread widest first

The planner (`control/planner.rs`) is given each member's whole map on `NodeInput::domains`, where
F81 gave it the host. Each rule reads the domain at a level through `domain_at`:

- **A drain** takes the operator's replacement first when it is feasible, as before. Otherwise it
  ranks feasible members by:
  1. whether the member would stand in a zone, then a rack, then on a host that the set's
     remaining members already hold, judged widest first;
  2. its load per unit of weight;
  3. its node id.

  So a member in a new zone wins over a less loaded one in a new rack of a held zone, and that
  member wins over a less loaded one on a new host in a held rack. Nothing is left unplanned for
  want of any of them.
- **A rebalance** skips a move that would leave a set in fewer domains at any level than it
  stands in now. Every other move is judged by load as before.
- **Voter promotion** (`ControlState::promotion_candidate`) ranks the placeable learners by
  whether each would stand in a zone, then a rack, then on a host a voter already holds, widest
  first, and promotes the first of the best in node order. In a hierarchy that nests, that is the
  first learner in a zone no voter stands in, failing that the first in a new rack, then the first
  on a new host, then the first learner of all.

### A pool can name a rack or a zone

A pool's `failure_domain` (`shoal::shared::pools::FailureDomain`) gains `rack` and `zone` beside
`device` and `host`, spelled as the member's levels are. It is committed with the pool policy at
bootstrap. Nothing places a stripe's chunks before [M14](../object-storage/milestones.md#m14-devices-and-pools-on-one-node),
so nothing reads it yet. A device's rack and zone will be its member's.

### An inventory names them, and refuses a hierarchy that does not nest

`failure_domains` on a node or a group takes `rack` and `zone` as it takes `host`, field by field.
A level the node leaves out comes from its group. A rack or zone that neither names is left out,
where an absent host becomes the node's name. Every rendered `shoal.yml` writes a rack and a zone
only when one is named, so a file never names a level nobody gave.

`validate` checks that the levels nest, since the engine never compares them across members. It
refuses, naming both nodes:
- **a host whose nodes stand in two places**, such as two racks, two zones, or one naming a rack
  and one naming none;
- **a rack named in two zones.** Racks in different zones have to be named apart.

### `status` shows them

`shoaladm status` and the `shoalctl` cluster tab add a `rack` and a `zone` column after `host`, and
`racks N` and `zones N` after `hosts N` in the header, each only once a member names that level.
On a cluster that names neither, the tab is as F81 drew it. The counts read `domain_at`, so they
are the planner's count: a member that names no rack counts its host as its rack.

```text
leader 3a8c…  voters 3  learners 0  up 3  hosts 3  racks 2  zones 1  tombstones 0
member                               host         rack         zone         role    health …
```

### The fixture names them

`ClusterBuilder::rack(id, …)` and `ClusterBuilder::zone(id, …)` sit beside `host`. A child's staged
settings carry the whole `FailureDomains`, so a test can restart a node under another rack as it
restarts one under another host.

## Design choices

- **Two fixed levels, chosen with the user.** A level is a key that every build spells alike, a
  pool's `failure_domain` stays an enum checked when the policy is read, and the order a spread
  is judged in is fixed in `LEVELS` rather than being a policy every node has to agree on.
- **The level below stands in for an absent one.**
  - A member that names no rack has no rack anyone knows of, and its host is the narrowest truthful
    answer.
  - A cluster that names no rack or zone loses nothing it had, because every level's key collapses
    to F81's host key.
  - The level the name came from travels with it, so the fallback never makes a host and a rack
    of one name look like one domain.
- **Names are the cluster's at their level.** A mistake then errs toward "shared", which costs a
  preference and never a guarantee. F81 refused the opposite error for its default: "a default that
  made each its own domain would claim a spread that does not exist".
- **Widest level first, in lexicographic order.** In a hierarchy that nests, a new zone is a new
  rack and a new host. The order only matters between candidates of different spread, and the
  widest difference is the one worth paying load for. Load breaks ties within one spread.
- **A rebalance is judged at every level, not by a combined score.** In a hierarchy that nests, a
  move cannot raise one level while lowering another. "Never lower any level" is then a rule a
  reader can check, and a score would only hide the same rule behind weights.
- **Read by the leader's planning, never by apply.** Nothing committed depends on a rack or a
  zone, so two builds applying one log cannot diverge on them and no wire version gates them.
  This is F81's choice, kept.
- **The inventory checks the nesting, the engine does not.** The inventory sees every node at once,
  before anything starts. A node sees only its own file, and the control group would have to refuse
  a node's observation of itself to check it.

## Alternatives rejected

- **Levels the operator defines, as CRUSH's bucket types are.** A pool's `failure_domain` would
  become a free string that nothing can check before M14, and the order of the levels would be a
  policy every node has to hold alike. No deployment Shoal targets has more than a zone and a
  rack, and adding a third fixed level later is a key, not a format change.
- **Names scoped to the level above, a rack known by its zone and its name.** Racks named alike in
  two zones would be two racks, which is right. But a host named in two racks would be two hosts,
  and a mistake would then claim a spread over one machine. The inventory refuses racks named
  alike in two zones, and asks for them to be named apart, which keeps every mistake on the safe
  side.
- **Refusing a hierarchy that does not nest in the control group's apply.** `ObserveMember` would
  refuse a node's own record. The node would then start without being observed, for a mistake that
  another node's file may have made, and whichever node observed itself first would win. An apply
  rule that reads the domains also needs the wire version gate F81 kept clear of.
- **A member with no rack in one shared rack, "none".** On a cluster that names no rack it plans as
  the fallback does, since every candidate doubles "none" and the host decides. A pool under
  `failure_domain: rack` would then see one rack and could place nothing wider than one chunk.
  Under the fallback it places by host, which is what a cluster without racks can promise.
- **A rack read off the machine** (DMI chassis fields, LLDP from the switch). There is no source
  every machine has, and a wrong guess claims a spread. The host defaults to the machine's name
  because that name is the machine.

## Limitations

- **The engine does not check the nesting.** Only `validate` does. A file written by hand can put
  two nodes on one machine in two racks. The planner then reads two racks, though its host level
  still keeps the two nodes together.
- **A cluster that names a rack on some members and not others** reads each unnamed member's host
  as its rack. That is the fallback working as designed, but a drain may then prefer an unnamed
  member that in fact stands in a named rack.
- **Nothing reads a pool's `rack` or `zone` before M14.** Whether M14 refuses a pool under a level
  that none of its devices' members names, rather than placing by the fallback, is its decision to
  make ([todos](../appendix/todos.md#distribution)).
- **Nothing seeks the spread on its own.** This is F81's limitation, now at every level. A set
  already doubled in a rack stays doubled until a drain or a rebalance moves it, and no plan moves a
  set for its racks alone.
- **A rack or a zone is a restart**, as a host is.
- **A node on F81's build refuses a file that names a rack or a zone**, since its block refuses
  every level but `host`. An inventory that names one is reconfigured onto a cluster only after
  `upgrade` has put every node on F83's build. A leader on F81's build keeps a rack it reads, since
  the record is a map, but plans by host alone for the rolling window.
- **The inventory wizard has no field for either.** It carries them through an edit unchanged, as
  it does `host`.
- **An upgrade still restarts one node at a time.** Runbook 7 would allow a whole domain at once
  ([todos](../appendix/todos.md#distribution), under what F55 left).
- **The lab has no racks or zones.** Its run names them to show they are carried. The fixture and
  the planner's unit tests are the evidence of the preference.

## Invariants to uphold

- **One rule says where a member stands at a level: `domain_at`.** The planner, voter promotion and
  `status` all read it. A second rule would let the tab count domains the planner does not plan by.
- **A level left out is the nearest one below it named, tagged with that level.** A cluster that
  names no rack or zone plans and promotes exactly as F81's did. F81's tests hold this, their
  expectations unchanged.
- **A name is the cluster's at its level.** Never scope a rack to its zone or a host to its rack in
  the planner. A mistake must make domains look more shared, never less.
- **A domain is a preference, never a block, at every level.** A drain takes a held domain when
  nothing else is feasible, a rebalance only refuses a move that lowers a level, and promotion falls
  back to the first learner.
- **`LEVELS` is widest first and is the order a spread is judged in.** A new level is a constant
  placed in it, a field in `FailureDomains` and in `FailureDomains::named`, and a variant of the
  pool's `FailureDomain` spelled the same.
- **The record's map takes levels it does not know**, and no apply rule reads it without a wire
  version gate. Both are F81's invariants, unchanged.

## Performance

Nothing here is on a query's path. A start resolves two more optional names. The planner judges
three levels for each candidate where it judged one, on the control leader, every `plan_interval`.
Promotion runs on a membership change. No before-and-after was taken, by the rule that only work
added to a query's path earns one.

### The tmdb cluster

`tmdb_cluster.yaml` (europa, titan and hyperion, a factor of three), built for znver1 at F83's
tree, holding the whole dataset as [F82](root-capacity.md) left it: about 950 MiB a member.

- **`verify --read quorum`, before**, on F82's build: all 1,187,691 movies, 1,188,009 release rows
  by their composite key and 58,418 keyword partitions were found, none missing or different.
- **`upgrade`**, one node at a time with the leader last, in 28 s. No file named a rack, and
  `status` was F81's to the column, each member's incarnation one higher.
- **A rack and a zone on each group.** `failure_domains: {rack: desk, zone: lab}` went on
  europa's group and `{rack: zen1-shelf, zone: lab}` on the Zen1 hosts' group. `reconfigure`
  rendered every node's file again, titan's naming `host: titan`, `rack: zen1-shelf` and
  `zone: lab`, and restarted all three in 24 s. `status` drew the two columns from each member's
  committed record:

  ```text
  leader 40091edd-…  voters 3  learners 0  up 3  hosts 3  racks 2  zones 1  tombstones 0
  member        host         rack         zone         role    health   phase      inc …
  40091edd-…    europa       desk         lab          voter   up       member       4
  5dd2bea0-…    titan        zen1-shelf   lab          voter   up       member       4
  f207b8f7-…    hyperion     zen1-shelf   lab          voter   up       member       4
  ```
- **A rack moved in the inventory.** `failure_domains: {rack: zen1-bench}` on titan's node, over
  its group's, and `reconfigure titan` committed it at titan's next incarnation, 5. The header read
  `racks 3`, and hyperion and europa were not restarted.
- **One machine in two racks, refused.** A copy of that inventory naming `host: titan` on
  hyperion's node was refused by `status` and by `reconfigure` before either touched a host:
  `the nodes "titan" and "hyperion" stand on the host "titan" in two places: rack "zen1-bench" in
  zone "lab" and rack "zen1-shelf" in zone "lab"; one machine stands in one rack and one zone`.
- **Put back.** The inventory was restored and reconfigured. Every record lost its rack and zone at
  its next incarnation, since a re-observation replaces the whole record, and the tab drew F81's
  columns again.
- **`verify --read quorum`, after**: the same counts, none missing or different. Every unit was
  active and every host's governor was as it had been.

The movies read back in 24.7 s after the last restart, then 10.6 s and 9.9 s, against 4.1 s before.
To rule out the build, it was rolled back to F82's (`upgrade --rollback`), which restarts every
node the same way. F82's build then read them in 9.1 s and 8.4 s. Upgraded to F83's again, it read
them in 9.1 s. So the slower read follows a restart of every node, not this feature, which adds
nothing to a read. Where a restart's cost goes was not pursued here, and is filed as
[O101](../appendix/optimizations.md#o101-a-quorum-read-of-every-row-runs-at-less-than-half-its-rate-once-every-node-has-restarted).

At a factor of three on three hosts, every set is on every host and all three members vote, so
neither the planner's preference nor promotion's had a choice to make on the lab, as under F81.

## Tests

| Test | What breaks if F83 is reverted |
| --- | --- |
| `shoal-proto` `domains::the_documented_shape_is_read_and_unknown_levels_refused` | `{host, rack, zone}` stops parsing or writing back as read, the levels stop being widest first, or a `row:` or `region:` is accepted |
| `shoal-proto` `domains::a_host_that_cannot_be_one_is_refused` | A blank rack or a zone holding whitespace is accepted, or refused under another key |
| `shoal-proto` `domains::a_host_resolves_to_the_named_one_or_the_machines` | A named rack or zone stops reaching the record, or a bad rack passes beside a good host |
| `shoal-proto` `domains::a_level_left_out_is_the_nearest_named_below` | A level left out stops being the one below it, the fallback loses the level it came from, a rack's level reaches up to a zone, or a record naming nothing stands somewhere |
| `shoal-proto` `pools::the_documented_shape_is_read_and_unknown_keys_refused` | A pool stops reading `failure_domain: rack` or `zone`, its spelling parts from the member's levels, or a `row` is accepted |
| `shoal-core` `conf::cluster::tests::failure_domains_parse_and_a_bad_host_is_refused` | The `cluster:` block stops carrying a rack and a zone, a blank rack validates, or a `row:` is read |
| `shoal-core` `control::planner::tests::the_planner_spreads_sets_over_racks_and_zones` | A drain takes a less loaded member in a held zone over one in a new zone, or in a held rack over one in a new rack, a zone without a rack stops deciding, a cluster naming no rack or zone plans differently, or a rebalance lowers a set's racks or zones |
| `shoal-core` `control::planner::tests::the_planner_spreads_sets_over_hosts` | F81's, its hosts now given as the record's map and its expectations unchanged: a cluster naming hosts alone stops planning as it did |
| `shoal-core` `control::types::tests::promotion_spreads_the_voters_over_hosts` | F81's, unchanged: a cluster naming hosts alone stops promoting as it did |
| `shoal-core` `control::types::tests::promotion_spreads_the_voters_over_racks_and_zones` | A learner in a new rack or on a new host is promoted before one in a new zone, a new host before a new rack, or a cluster naming no rack or zone promotes differently from F81's |
| `shoal` `cluster_fixture::a_member_reports_its_failure_domain` | A member's record stops carrying the rack and zone it named, carries a level nobody named, or a restart into another rack is not committed |
| `shoal` `cluster_fixture::a_drain_and_a_voter_seat_spread_over_racks` | A drained voter's sets split between the spare in a voter's rack and the one in a rack of its own, or its seat goes to the spare in a voter's rack |
| `shoaladm` `deploy::inventory::tests::racks_and_zones_resolve_over_the_group_and_nest` | A rack or zone stops resolving node then group then nothing, a blank one passes, or a host in two places or a rack in two zones passes the inventory |
| `shoaladm` `deploy::render::tests::a_node_file_says_how_it_enters_and_holds_no_password` | A rendered file names a rack nobody gave, or drops one an inventory gave |
| `shoaladm` `wizard::form::tests::a_draft_round_trips_an_inventory` | An edit in the wizard drops a node's or a group's rack or zone |
| `shoaladm` `cluster::model::tests::the_cluster_model_reads_the_admin_frames` | `status` draws a rack or zone column nobody named, stops drawing one somebody did, or counts racks and zones by another rule than the planner's |
| `shoal-bench` `deploy_render::a_group_split_renders_the_roots_the_engine_claims` | A group's rack and zone in an inventory stop reaching the engine's `cluster.failure_domains` |

**Established by reproducing it.** `a_drain_and_a_voter_seat_spread_over_racks` was run with the
planner's and promotion's levels above the host switched off, which is F81's planner. It failed:

```text
assertion `left == right` failed: a set went beside a copy in its rack: {... "steps":[
  {"tablet":0,"to":"04b5cbcb-…"},{"tablet":1,"to":"9d5e819c-…"},{"tablet":2,"to":"04b5cbcb-…"}]}
```

Node one's three sets split two to one between the spare in a rack of its own and the spare in
node zero's rack. By host alone the two are alike, and that is what the planner does with two
idle destinations. A first try switched off the rack level alone and passed. The nodes named no
zone, so the zone level read each rack in its place. That is the fallback doing what it is for,
and it is why the run that counts as evidence switches off both levels above the host. The test
plans with `moves_per_node: 3`, so all three sets are planned in one pass and the split by load
does not depend on the order the spares' identities fall in.

The workspace run at six threads passed 2,330 of 2,331. The one failure,
`automatic_removal_and_rejoin_preserve_fencing`, drains to a single spare, so no domain chose
anything in it. A step's move had spent its stale-version retries and been planned again, and the
test asserts no step failed. It passed alone three times and is filed as
[item 234](../appendix/known-issues.md#234-a-plans-move-can-spend-its-stale-version-retries-under-churn-and-a-fencing-test-counts-the-failed-step).

## Related

- [S1](../object-storage/prerequisites.md#optional), the row this delivers;
  [S5](../object-storage/placement.md#failure-domains), the placement that will read a pool's
  level as a rule; [P11](../object-storage/contract.md#the-contract).
- [F81](failure-domains.md), the host it stands above, and the record and preference it extends.
- [F46](capacity-rebalancing.md), the planner, and [F39](membership.md), the promotion.
- [F51](cluster-deployment.md) and [F63](shoaladm.md), the inventory that names them.
