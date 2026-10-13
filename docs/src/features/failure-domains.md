# F81. A failure domain on a member

## Context

[P11](../object-storage/contract.md#the-contract) counts a stripe's chunks in distinct failure
domains, and [S5](../object-storage/placement.md#failure-domains) and
[X2](../object-storage/placement-simulation.md#recommendation) decided what placement reads: a
device's own id and **its member's host**. A pool under a host domain never puts two chunks of a
stripe on one host. Before this feature a member had no host to read.
`MemberRecord` carried addresses, slots, weights and wire facts and said nothing about where the
node stood. The F46 planner spread replica sets by node alone
([todos](../appendix/todos.md#distribution)), and the control group promoted its voters in node
order ([C3](../distributed/membership.md#limitations),
[C15](../distributed/open-issues.md#filed-as-unbuilt)). [S1](../object-storage/prerequisites.md#required)
lists it as a required prerequisite of [M14](../object-storage/milestones.md#m14-devices-and-pools-on-one-node),
since a stripe placed without it can put two chunks on one host, and one host lost is then two
chunks lost.

Where the domain comes from was decided with the user on 2026-10-03, before anything was built:

- `cluster.failure_domains: {host: <name>}` in a node's configuration, the host defaulting to the
  name the operating system gives the machine, so nodes on one machine truthfully share a host and
  a test that needs several hosts names them;
- an inventory sets it per node or per group, defaulting to the node's name;
- the member's field is a list from the start, so a level above the host is added without a
  format change.

What reads it was decided with the user on 2026-10-11. Stripe placement does not exist before M14,
so a domain that was only recorded would have no reader and no test of its behaviour until then.
C15 and F46's todo both said what it was for besides: "so voters and copies can be spread over
one". So the feature records the domain and also **spreads by it softly**. The planner and voter
promotion prefer a member on a host the set or the voters do not stand on yet, and fall back to
the choice they made before when none does.

## What it does

### A node names where it stands

```yaml
cluster:
  failure_domains:
    host: titan     # absent: the machine's own name
```

`FailureDomains` (`shoal-proto/src/shared/domains.rs`, `shoal::shared::domains`) is the one shape
a node's configuration, `shoaladm`'s inventory and the cluster fixture hold. ~~Its one level is
`host`.~~ Its one level was `host`; since [F83](rack-and-zone-domains.md) a `rack` and a `zone`
stand above it, each named or absent.

- **An absent host is the machine's own name.** It is read at start with `gethostname`, by the
  same `control::stats::hostname` that F64's figures name members with.
- **A host given has to be one.** It is refused at validation, naming
  `cluster.failure_domains.host`, if it is empty, longer than 255 bytes, or holds whitespace or a
  control character.
- **A machine that gives no usable name stops the start.** The node refuses to start, naming the
  key that fixes it.
- **A level no build places by is refused when the block is read.** ~~A `rack:` is refused, as
  every unknown key in the `cluster:` block is.~~ A `rack:` was refused until
  [F83](rack-and-zone-domains.md) read it; a `row:` or any other level is refused, as every
  unknown key in the `cluster:` block is.

The domains are this node's own, beside `weight` and `lead_weight`, not the cluster's policy. They
are resolved once, in `ControlPlane::start`, before anything binds.

### The control group commits it

`MemberRecord::failure_domains` is a map from a level to a name, `{"host": "titan"}`, and
`MemberRecord::host()` reads it. The node writes it into the record it observes itself with, so
it is committed by the same `ObserveMember` that records the node's addresses and incarnation.
A restart under another host is committed at the next incarnation with no command of its own,
since a re-observation replaces the whole record (`ControlState::observe`).

`Members` answers it under `members[i].record.failure_domains`. `shoaladm status` and the
`shoalctl` cluster tab, which draw one model (`shoaladm/src/cluster/model.rs`), show it as a
`host` column beside each member and count the hosts the members stand on in the header line:

```text
leader 3a8c…  voters 3  learners 0  up 3  hosts 3  tombstones 0
member                               host         role    health   phase     …
```

### A drain prefers a new host

The planner (`control/planner.rs`) is given each member's host on `NodeInput::host`. A member
whose record names none stands in a domain of its own.

- **A drain** (`Decommission`, `Remove`, an expiry) takes the operator's replacement first when it
  is feasible, as before. Otherwise it ranks the feasible members by three keys, in order:
  - whether the member would stand on a host the set's remaining members already stand on;
  - its load per unit of weight;
  - its node id.

  A member on a new host wins over a less loaded one on a host the set holds. A member on a held
  host is taken only when no feasible member stands anywhere else, and nothing is left unplanned
  for want of a host.
- **A rebalance** never takes a move that would leave a set on fewer hosts than it stands on now.
  Every move that keeps or raises the count is judged by load as before.

### A voter seat prefers a new host

`ControlState::promotion_candidate` names the learner the leader promotes next when the control
group has fewer voters than its policy asks for. It is the first placeable learner, in node order,
whose host no voter stands on, and otherwise the first placeable learner, as before.
`maybe_promote` calls it. So a seat a drained voter leaves goes to a learner on a host the other
voters do not hold, where there is one.

### An inventory names it

`failure_domains` is a key of a node and of a group in an inventory. A node's host is resolved in
this order:
1. its own;
2. its group's;
3. the node's name.

Every rendered `shoal.yml` names the host, so a node's file says where the node stands. `validate`
refuses a host the engine would refuse, naming the node it reaches. The inventory wizard carries
the key through an edit unchanged, as it does `lead_weight`. The two loopback inventories, which
put three nodes on europa (`shoal-spike-bytes` and `shoal-spike-small`), name `europa` for each
node, since the name default would have made them three hosts.

## Design choices

- **The machine's name by default, not the node's identity.** Two nodes on one machine fail
  together, and a default that made each its own domain would claim a spread that does not exist.
  The cost falls on tests. Every child of the cluster fixture stands on one host unless the test
  names hosts (`ClusterBuilder::host`), as the decision intended.
- **A map of levels on the record.** It is the "list" S1 and S5 asked for. A rack is another key,
  and an older build that decodes a record with one keeps it when it writes the record back, since
  serde reads a map's keys as data, not as fields. The configuration is the typed half and refuses
  a level it does not know, so nothing is accepted and ignored. [F83](rack-and-zone-domains.md)
  bore it out: a rack and a zone were two keys and two fields, and no format changed.
- **A preference, not a rule.** The one hard rule P11 names is a stripe's: no two chunks in one
  domain, which M14's placement enforces over devices. For a tablet's copies a hard rule would
  block every drain on a cluster with fewer hosts than its factor, and every one-host fixture.
  A replica set that doubles a host still has every copy, so the spread is worth having and not
  worth refusing a drain over.
- **Read by the leader's planning, never by apply.** The planner and promotion run on the control
  leader from the committed state. No command's apply reads the domain: not `Initialize`, not a
  move's destination, not a replacement. So two builds applying the same log cannot diverge on
  it, and no wire version gates it. A future rule inside apply would need that gate
  ([F48](rolling-compatibility.md)).
- **The inventory always renders the host.** It defaults to the node's name, as decided, which is
  the machine's own name on every lab host. A file with the host written out says where the node
  stands without a lookup on the host it runs on.

## Alternatives rejected

- **The host in the storage marker.** A marker is written once, at the first claim, and records
  what a directory *is*. The host is where the directory is mounted *now*: a disk moved to another
  machine moves host. Configuration read at every start, committed at every observation, follows
  the machine. C1's limitation "the marker records no failure domain" stays true on purpose.
- **A typed enum of levels in the record.** An older build reading a record with a level it does
  not know would fail to decode it. The record is openraft's node type, so the whole control state
  would be undecodable. Strings in a map cost a typo nothing, since the typed configuration is the
  only writer.
- **The host on the map and the client's topology frame.** No shard and no client reads it, and
  X2 measured every member field growing the frame pushed to every subscriber
  ([today's tablet frame](../object-storage/placement-simulation.md#todays-tablet-frame-again)).
  M14 derives its pool map on each node from the committed state, which holds the record.
- **The hostname read on every tick, as F64's figures are.** A domain that moved without a
  restart would move under the planner between two steps of one plan. The record is what the
  cluster agreed on, and a change goes through a start like a weight's.
- **Spreading by reordering the placement rule or `Initialize`.** Tablet `t`'s copies are a
  window over the initialized order, and every group's identity is minted from the window
  ([C4](../distributed/tablet-map.md#the-placement-rule)). Reordering it by host moves identities,
  which is a migration, not a setting. F58 turned down changing the primaries for the same reason.

## Limitations

- **Nothing seeks the spread on its own.** A set already doubled on a host stays doubled until a
  drain or a rebalance moves it, and no plan moves a set for its hosts alone. A voter on a doubled
  host keeps its seat.
- **The initialized order can put two nodes of one host side by side**, and then the rule's sets
  double that host. `shoaladm` initializes in inventory order. Interleaving by host is filed as a
  [todo](../appendix/todos.md#distribution), not built.
- **A voter seat goes to whoever is up when it frees.** Promotion is greedy. A learner on a held
  host that is up before one on a new host takes the seat, and is not swapped out when the other
  arrives.
- **A host is a restart**, as a weight is.
- ~~**There is no level above the host.** A `rack:` is refused until a build places by one
  ([S1's optional row](../object-storage/prerequisites.md#optional)).~~ Since
  [F83](rack-and-zone-domains.md) a zone and a rack stand above the host, and the drain, the
  rebalance and promotion judge them widest first.
- **The inventory wizard has no field for it.** It carries one through an edit, and an operator
  writes it by hand, as `lead_weight` is written.
- **A rolling upgrade from a build before F81 drops the field for its window.** A member on the
  older build decodes a newer record without the map and writes it back without it, and a leader
  on that build plans without hosts. It is carried, not gated, as every record field since F46 has
  been.
- **The membership's copy of a record keeps the domain it was admitted or promoted with.** openraft
  holds a `MemberRecord` per node in the membership, and `maybe_readdress` rewrites it only when an
  address moves. Nothing reads its domain: the planner and promotion read the committed state.
- **On the lab the spread has nothing to do.** Three hosts at a factor of three put every set on
  every host, and all three are voters. The fixture and the planner's unit tests are its evidence.

## Invariants to uphold

- **A host is a preference, never a block.** A drain takes a held host when nothing else is
  feasible, a rebalance only refuses a move that lowers a set's hosts, and promotion falls back to
  the first learner. A cluster on one machine, or one that names no hosts, plans and promotes
  exactly as it did before this feature. The fixture's whole drain and rebalance suite runs on one
  host and depends on that.
- **A member that names no host stands alone.** Records from before F81, and a test's literal
  records, must never collapse into one domain, or a cluster mid-upgrade would plan as if every old
  member shared a machine.
- **The record's map takes levels it does not know.** Never decode it into a typed struct that
  refuses unknown keys, and keep `#[serde(default)]` on it.
- **The domain is resolved once a start, before anything binds**, and only the start writes it
  into the record. Reading it later would let one run of a node report two hosts.
- **No apply rule reads it without a wire version gate.** Planning and promotion are the
  leader's; apply stays a function of the log alone across builds.

## Performance

Nothing here is on a query's path. A start reads the hostname once. The planner runs every
`plan_interval` on the control leader, and compares a set's hosts for each candidate it already
judged. Promotion runs on a membership change. No before-and-after was taken, by the rule that
only work added to a query's path earns one.

### The tmdb cluster

`tmdb_cluster.yaml` (europa, titan and hyperion, a factor of three) was upgraded to F81's build for
znver1. It held the whole dataset as [Resolved #229](../appendix/resolved/hotpath-build.md) left it:
about 940 MiB a member.

- **`verify --read quorum`, before.** All 1,187,691 movies, 1,188,009 release rows by their
  composite key and 58,418 keyword partitions were found, none missing or different.
- **`upgrade`**, one node at a time with the leader last, in 34 s, with every node's file as the
  build before rendered it. No file named a host, so each node took its machine's name. `status`
  then showed europa on `europa` and **hyperion and titan on no host at all**. They had restarted
  first, while europa still led on the build before F81. That leader decoded their observations
  without the field and committed them without it, which is the rolling upgrade's limitation
  above, met on the lab. Europa restarted last, under a leader on the new build, and its host was
  committed.
- **`reconfigure`** rendered every node's file again, now naming its host, and restarted all three
  in 27 s. Each observation went through a leader on the new build. `status` showed hyperion,
  titan and europa, `hosts 3`.
- **A host moved in the inventory.** `failure_domains: {host: zen1-shelf}` on titan's node and
  `reconfigure titan` committed it at titan's next incarnation, 7. The same key on the `v1000`
  group put hyperion beside titan. `reconfigure` restarted hyperion alone, since titan's file
  already named `zen1-shelf` and europa's had not changed, and the header read `hosts 2`. The
  inventory was then put back and reconfigured, and the members stood on their three machines again.
- **`verify --read quorum`, after.** The same counts, none missing or different. `status` showed
  3 of 3 copies with writes admitted.

At a factor of three on three hosts every set is on every host, and all three members vote, so
neither the planner's preference nor promotion's had a choice to make on the lab.

## Tests

| Test | What breaks if F81 is reverted |
| --- | --- |
| `shoal-proto` `domains::the_documented_shape_is_read_and_unknown_levels_refused` | `{host: titan}` stops parsing, an empty block stops being the default, or ~~a `rack:`~~ a level no build places by (a `row:` since [F83](rack-and-zone-domains.md)) is accepted |
| `shoal-proto` `domains::a_host_that_cannot_be_one_is_refused` | An empty, over-long, or whitespace or control-character host is accepted |
| `shoal-proto` `domains::a_host_resolves_to_the_named_one_or_the_machines` | A named host stops winning over the machine's, an absent one stops being the machine's, or a node with no name at all starts |
| `shoal-core` `conf::cluster::tests::failure_domains_parse_and_a_bad_host_is_refused` | The `cluster:` block stops carrying `failure_domains`, an empty host validates, or ~~a rack~~ a level no build places by is read |
| `shoal-core` `control::planner::tests::the_planner_spreads_sets_over_hosts` | A drain takes a less loaded member on a held host over one on a new host, a drain on one machine or with no hosts plans differently from before, the replacement stops coming first, or a rebalance leaves sets on fewer hosts |
| `shoal-core` `control::types::tests::promotion_spreads_the_voters_over_hosts` | A learner beside a voter is promoted before one on a new host, the fallback stops being the first learner, a node outside the membership is promoted, or a host changed at a restart is not committed |
| `shoal` `cluster_fixture::a_member_reports_its_failure_domain` | A member's record stops carrying the host it named, or the machine's for one that named none, or a restart under another host is not committed |
| `shoal` `cluster_fixture::a_drain_and_a_voter_seat_spread_over_hosts` | A drained voter's sets split between a learner beside a voter and one alone on its host, or its seat goes beside a voter |
| `shoaladm` `deploy::inventory::tests::a_host_resolves_over_the_group_and_defaults_to_the_name` | A node's host stops resolving node, then group, then name, or a bad host or ~~a rack~~ a level no build places by passes the inventory |
| `shoaladm` `deploy::render::tests::a_node_file_says_how_it_enters_and_holds_no_password` | A rendered file stops naming the node's host |
| `shoaladm` `wizard::form::tests::a_draft_round_trips_an_inventory` | An edit in the wizard drops a node's or a group's `failure_domains` |
| `shoaladm` `cluster::model::tests::the_cluster_model_reads_the_admin_frames` | `status` loses the host column or the count of hosts |
| `shoal-bench` `deploy_render::a_group_split_renders_the_roots_the_engine_claims` | A group's host in an inventory does not reach the engine's `cluster.failure_domains` |
| `shoal` `cluster_fixture::documented_cluster_defaults_match_policy_bootstrap` | The documented `cluster:` block, `failure_domains: {}` among it, stops being the default |

**Established by reproducing it.** `a_drain_and_a_voter_seat_spread_over_hosts` was run with the
drain's host key switched off (`false && doubles_a_domain(..)`) and failed:

```text
assertion `left == right` failed: a set went beside a copy on its host: {... "steps":[
  {"tablet":0,"to":"1113c522-…"},{"tablet":1,"to":"ba20d370-…"},{"tablet":2,"to":"1113c522-…"}]}
```

Node one's three sets split two to one between the learner beside node zero and the learner alone
on its host, which is what the planner by load alone does with two idle destinations.

## Related

- [S1](../object-storage/prerequisites.md#required), the row this delivers;
  [S5](../object-storage/placement.md#failure-domains) and
  [X2](../object-storage/placement-simulation.md#recommendation), the placement that will read it
  as a rule; [P11](../object-storage/contract.md#the-contract).
- [F46](capacity-rebalancing.md), the planner it spreads, and [F39](membership.md), the promotion.
- [F58](weighted-leadership.md), the other setting a member records when it observes itself.
- [F64](stats-tui.md), which named members by their machine's name first.
