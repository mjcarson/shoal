# F46. Capacity-aware rebalancing and removal

## Context

[M9a](../distributed/milestones.md#m9a-safe-replica-migration) moved one replica set at an
operator's word ([F45](replica-migration.md)): a durable record, a learner fed and caught up,
the joint transition, publication in apply, a retired copy under a grace. Nothing decided
*which* set went *where*, nothing moved more than one set, and a move fed its destination as
fast as the bulk lane ran. A member that fell silent was called `Down`
([F39](membership.md)) and kept its assignments for a grace nobody counted:
`auto_remove_after` was parsed into the policy and read by nobody, and `Leaving`, `Removing`
and `Removed` were three rows of [C3](../distributed/membership.md)'s table with nothing behind
them.

[M9b](../distributed/milestones.md#m9b-capacity-aware-rebalancing-and-removal) asks for the
policy half: feasible weighted placement, disk reserves and transfer budgets, the
`Decommission`, `Remove` and `Replace` workflows, automatic expiry of the grace with
maintenance suspension, progress that survives a control leader restart, and no silent
reduction of the factor when only two of three nodes remain. [C8](../distributed/rebalancing.md)
sets the priorities - restore the factor where a quorum and capacity permit, protect the
reserve, balance measured bytes by explicit weights with hysteresis over exact tablet counts -
and [C13](../distributed/protocol.md)'s Q7 and Q8 leave the removal default, the grace's
persistence and the weights' shape to this milestone.

Decided with the user on 2026-09-14: the leader plans and runs drains by itself - a leaving
member, a removing one, an elapsed grace - and spreads onto a new node only under an explicit
`Rebalance`; the balance target is measured bytes against node weight; all four C10 arms are
built. This page is the model. The mechanism that most needed one is the split between a
member's *phase* and its *health*: the operator's and the policy's word on one side, the
detector's on the other, read together as the one state C3 names.

## What it does

### A phase beside the health

`MemberState` carries `phase: MemberPhase` - `Member`, `Leaving`, `Removing`, `Removed` -
beside `health` (`shoal-core/src/server/control/types.rs`). A `Leaving` member can be up or
down; a `Removing` one usually is down; every `== Up` check in the tree keeps its meaning, and
the one name C3's six-state machine gives a member is `state_name`: the phase past plain
membership, the health otherwise. `is_placeable` is `Up` and `Member`, and it is what
`Initialize`, `apply_move`'s destination, `maybe_promote`'s candidate and the planner's
eligibility all read, so no new placement lands on a member on its way out. The map's
`MapMember` and the client's `TopologyMember` carry `phase` and `state`; a frame from before
this feature reads as a plain member.

### The grace, counted in committed increments

A `Down` verdict under a policy with `auto_remove_after` opens a `GraceState` on the member:
the episode, `elapsed_ms`, `suspended`, `expired`, and the plan the expiry records. The
leader accrues it: `GraceLocal` in `plane.rs` remembers the committed value it started from
and when, and every eighth of the grace - or sixty seconds, whichever is shorter - proposes
`ControlCommand::GraceElapsed` with the committed value plus what it has counted; when that
reaches the grace it proposes with `expire: Some(plan)`. Apply refuses a count of another
episode, applies a suspended or expired grace as nothing, and never lets `elapsed_ms` go
backwards. A new leader starts its local count at zero on top of the committed value, so a
leader change loses at most one increment and never restarts or skips a grace, and nothing is
guessed early - Q7's "delay conservatively" ([decision record](../distributed/protocol.md#q7-and-q8-at-m9b)).
`null` disables the count entirely: no grace opens. `Maintenance { node, suspend }` is a
versioned operation that sets `suspended`; the leader neither counts nor commits while it is,
and resumption continues from the committed value. `Members` reports `grace_remaining_ms` -
the grace less the committed elapsed - present while suspended too, which is the observable
deadline C3 asks for.

### The three operations, and the expiry

`AdminKind::Decommission { node }`, `Remove { node, replacement }`, `Maintenance { node,
suspend }` and `Rebalance` are mutations: authorized against `cluster.admins`, versioned,
idempotent by operation id and audited, exactly as `Move` is. `Replace` is `Remove` with a
replacement named. Their apply rules:

- `Decommission` needs a plain member - `Leaving` already is applied and changes nothing -
  and moves it to `Leaving` with a `Decommission` plan recorded under the operation's id. A
  leaving member still serves and still counts; it takes no new placement.
- `Remove` needs a member that is `Down` or `Leaving` - a live plain member is refused by
  name: "decommission a live one" - and moves it to `Removing` with its grace, if one runs,
  marked expired under the operation, and a `Remove` plan. A `replacement` has to be a
  placeable member outside every set the member holds.
- `Maintenance` needs a member under a grace that has not expired; a removal cannot be
  suspended.
- `Rebalance` needs a placement, and one at a time.
- An elapsed grace is `GraceElapsed { expire: Some(plan) }`: the member is `Removing` from
  wherever it stood and an `Expiry` plan is recorded, principal `policy`.

One plan per member at a time, and one `Rebalance` at a time; a second is refused naming the
first. `SetHealth Up` on a `Removing` member keeps the phase and keeps the grace: a late
heartbeat cannot reverse a removal, which C3 states as a rule and this enforces in apply. A
`Decommission` that fails puts the member back to `Member`; a `Removing` one stays so.

### The plan and the planner

A plan (`control/plan.rs`) is a control record: its kind, its steps, why it is blocked if it
is, its phase - `Planned`, `Running`, `Blocked`, `Finishing`, `Done` - and its outcome. A step
is one replica set from one member to another with the bytes the set held on its source when
planned, and once issued, the `Move` it became. The leader's `PlanProgress` command appends
steps, moves a step's state, records or clears a blocked reason, marks a plan finishing or
done; a done plan stays done. The record rides `TopologyView` - not the shard map, since no
shard reads it - and `PlanStatus` and `Plans` read it.

The planner (`control/planner.rs`) is pure, and a new leader given the same input derives the
same steps. Its input is the sets as the map serves them - the rule overlaid by the
configurations, through `TabletMap::rule_sets_served` - every member's eligibility, weight and
last reported free bytes, the bytes each holder reported for each set's groups, the disk
reserve, the per-node move cap, the hysteresis, and the moves in flight. Its rules, in C8's
priority order:

1. A **drain** - `Decommission`, `Remove`, `Expiry` - moves every set the member holds, each
   to the feasible eligible member outside the set with the lowest load per unit of weight,
   the replacement first when one is named and feasible. A set with no feasible destination
   is left unplanned under a reason naming it and what is missing; the other steps still go,
   and the leader plans again as the membership and the reported capacity change. At N = RF
   the reason is that every up member holds the set and a further member is needed.
2. **Feasible** means the destination is placeable, not already in the set, reported enough
   free bytes for the set above the reserve, and has room under the cap. A member that has
   reported no capacity yet is taken at its word: the receiver's own reserve check is what
   makes a stale or absent report safe.
3. A **rebalance** gives every eligible member a feasible target: its weight's share of the
   bytes held, capped at holding every set, the excess spread over the rest by weight - a
   water fill. It moves a set from the member most over its target to the member below its
   target that gains the most, while the move brings both closer and the source is over by
   more than the hysteresis. At N = RF every member holds every set, no move exists, and the
   answer is nothing - twice in a row, the same answer. A set nobody has measured counts as
   one byte, so a cluster with nothing archived still balances by count.

Never a same-node move, never a change of factor: a step replaces one member of a set with
one outside it.

### The leader drives it

`drive_plans` runs every `cluster.rebalance.plan_interval` and sooner when the state or the
capacity table moved, floored at a quarter second. For each open plan: a step whose move is
done becomes `Moved` or `Failed` as the move's record says; a pending step under the cap -
`moves_per_node` as source and as destination, counted over every move not done - is issued
as `ControlCommand::Move` under the principal `plan <op>` against the current version, retried
when the version moved under it, and committed `Moving` with the move's id; with nothing
moving, the planner is asked what is left, and its steps are appended, or its blocked reason
recorded, or - with nothing left - a drain is `Finishing` and a rebalance is `Done`. One
proposal per plan is in flight at a time, and a leader change resumes from the record: a
moving step's move is its own record, driven by the group's leader as [F45](replica-migration.md)
built it.

A `Finishing` drain is `finish_removal`: the tombstone first - `ControlCommand::Tombstone`,
which moves the member to `Removed`, clears its grace and records `tombstones[node]` - while
the member still receives the log, so a live member learns it is removed and stops; then out
of the control group's configuration, a voter through `change_membership(RemoveVoters)` -
which openraft refuses while the old configuration has no quorum, and the plan says so and
tries again - a learner through `RemoveNodes`; then `Done` with what moved. `maybe_promote`
then refills the voter policy from the placeable learners, which is the spare. A leader that
is the member being removed hands the lead to another voter and lets it finish. The tombstone
is refused while the member still holds a set: its plan is what takes it out.

### A removed identity never returns

`observe` and `Admit` answer a tombstoned node `ControlResponse::Removed` at any incarnation,
before the fencing rule; `SetHealth` on a removed member the same; `drain_joins` refuses a
joiner naming a tombstoned identity with `retry: false`; the leader answers a removed member's
status report `removed`, and the reporter stops on it; `observe_membership` never re-admits a
tombstoned node from a configuration that still names it, and drops the role of a member the
configuration no longer names. A node that finds its own phase `Removed` in the applied state,
or its own observation answered so, stops with `ShoalError::Removed`, which the pool reports
through `failure`; its directory is left where it is. A replacement joins as a new identity.
Since [F49](backup-and-recovery.md) the peer doors say so too: both admission judges answer a
tombstoned or removed identity `Verdict::Removed` at the hello, a node of the cluster this one
was restored from the same, a link refused that way stops the control loop rather than
redialling, and a tombstoned member is not pinged.

### What a node reports

`GroupReport.bytes` is the table's archived bytes over the group's tablets on the shard, from
one pass over the archive map per table per report (`ArchiveMap::tablet_bytes`). The control
thread folds every shard's into `StatusReport.group_bytes` and reads `free_bytes` from
`statvfs` on the latency sensitive storage path once per tick (`control/capacity.rs`); the
leader keeps both in memory as `NodeCapacity` per member, never committed, and only the plan it
derives from them enters the log. `Members` reports `free_bytes` and `held_bytes` per member
beside `weight`. `cluster.weight` is the node's own setting, recorded on `MemberRecord` when it
observes itself; zero means the shard count, so a cluster of like machines needs no weights.

### Budgets and the reserve

`cluster.migration` gains `stream_bytes_per_sec` (64 MiB, zero unlimited),
`concurrent_streams` (2) and `disk_reserve` (1 GiB). Every stream a shard sends draws chunks
from one token bucket (`RateLimiter` in `replication/network.rs`) refilled from the clock and
holding one second's worth - a move's learner, a returning member, a repair alike; the wait is
charged to `snapshots.budget_wait_ns`. A receiving shard refuses a begin with
`SnapshotAnswer::Refused("stream budget: ...")` when as many streams as it may are assembling,
and `Refused("disk reserve: ...")` when the storage is short of the stream plus the reserve,
counted as `refused_budget` and `refused_reserve`; openraft's backoff feeds the learner again,
the move's `timeout` bounds it, and `peak_streams` records the most at once. The planner's cap
and the reported free bytes keep the refusals rare; the refusals are what make a stale report
safe. `cluster.rebalance` is `moves_per_node` (1), `hysteresis` (0.10) and `plan_interval`
(5 s, no shorter than the report interval).

### The fixture

Verbs `DECOMMISSION <node>`, `REMOVE <node> [replacement]`, `MAINTENANCE <node> on|off`,
`REBALANCE`, `PLAN_STATUS <op>`, `PLANS` and `FREE_BYTES <bytes>|none`; builder
`auto_remove_after`, `weight(node, w)`, `stream_budget(node, bytes_per_sec, streams)`,
`disk_reserve`, `moves_per_node`, `plan_interval`. The free bytes override is process-wide,
read by the report and the receiver's reserve check both, so a capacity test fills no disk.

### Found on the way

- A plan's move proposed against the version the leader read is refused `stale version` when
  another commit lands between the read and the write - the other steps' moves, a report -
  and the first run of the budget test recorded a failed step for it. The leader now retries a
  plan's move against the current version, as an operator's tool does.
- A member taken out of the control group's configuration kept its `voter` role in the state,
  since `observe_membership` only ever set the role of nodes the configuration named; the
  blocked-removal test found four voters. It now drops the role of a member no longer named.
- A learner fed by log holds its rows resident until it compacts, and what a set is weighed by
  is what the archives hold, so a spare's `held_bytes` reads zero right after a move. The
  weights test compacts before it compares; the planner's ledger takes the source's bytes for
  a set whose destination reports none, so the balance is right even so.
- At a factor below the node count no two nodes hold the same groups, and the fixture's
  node-level digest cannot agree; `wait_group_rows_agree` compares per group.

## Design choices

**Phase beside health, not a six-variant enum.** Decided here, not asked. A `Leaving` node can
be up or down and a `Removing` one can come back up without coming back; folding the two into
one enum would have made every `== Up` in the tree a match with arms nobody had thought
about. The one name is derived where a reader wants it.

**A plan's steps are ordinary moves.** Every transition [F45](replica-migration.md) proved
carries over unchanged - the record, the driver, the phases, the crash matrix - and the plan is
judged by the move's outcome rather than by a lag report. The cost is a second record per
step, which is the record an operator would have written by hand.

**Elapsed time is committed in increments, never a deadline.** A wall-clock deadline in the
log would be wrong on a leader whose clock differs and would restart from zero on the leader
that never saw the episode begin. An increment the leader accrues from its own monotonic clock
and commits is what a new leader resumes from; it can lose one, never gain one.

**Capacity is reported, not committed.** Free bytes and per-group bytes change every tick on
every node; committing them would fill the log with what nobody replays. The plan derived from
them is deterministic given the input, and the input is the leader's, which is why a plan is
committed with its steps rather than recomputed on each node.

**Feasible target, by water fill.** A member's weighted share can exceed what any one member
can hold - the heavy node in a 3:1:1:1 cluster of four at a factor of three - and a planner
chasing it would never settle. Capping at "every set" and spreading the excess is the feasible
share, and the hysteresis is what keeps a second plan from moving the last few bytes back.

**The leader drains on its own; it spreads only when asked.** Decided with the user. A drain
is what a decommission or an expiry means; a spread onto a member that just joined is a
decision an operator makes about capacity they added, and an automatic one would move bytes
onto a node that came up for something else.

**Tombstone before the membership change.** A member taken out of the configuration first
stops receiving the log and never learns it was removed; a live decommissioned member would
have kept serving from a map that no longer names it. Committed first, the tombstone is the
last entry it applies.

**One bucket per node, not per device.** A node with two storage devices shares one budget;
per device would need the device behind each table's path, which the configuration does not
name. Stated in Limitations.

**The receiver checks what the planner checked.** A report is a tick old and the planner's
free bytes are a report old; the refusal at the begin is the check that cannot be stale, and
the planner's is what keeps it from being hit.

## Alternatives rejected

- **A six-state `MemberHealth`.** Above.
- **A deadline in the log.** Above; and a suspended grace under a deadline would need the
  deadline rewritten on resumption, which is the same accounting done worse.
- **Committing capacity.** Above.
- **Balancing tablet counts.** [C8](../distributed/rebalancing.md) rejects exact tablet-count
  equality by name; a set of a thousand rows and a set of a million would count the same. Bytes
  with a floor of one byte per unmeasured set is the count when nothing is measured and the
  bytes when something is.
- **Automatic rebalance on join.** Above.
- **Per-pair streams.** C8's "one stream per pair alone does not prevent N peers overloading
  one destination"; the cap is per destination shard, and the bucket per sender node.
- **Refusing a `Decommission` at N = RF.** The plan is recorded and blocked by name instead,
  which is what "capacity-blocked cases stay observable" asks for and what lets a fourth member
  joined later complete it without the operator asking again.
- **A planner that runs on every node.** Deterministic, but the capacity input is the
  leader's memory; every node would need it committed. The plan is the committed thing.

## Limitations

- **Bytes are archived bytes.** A set whose rows are resident - a fresh learner, a volatile
  table - weighs nothing until a compaction; the planner floors a set at one byte and takes
  the source's figure for a destination that reports none. A volatile group reports zero.
- **One bucket per node.** No per-device budget; no per-pair budget. A node feeding three
  learners shares 64 MiB/s among them.
- **The budget is not adaptive.** Nothing throttles a stream when the foreground's tail or a
  replica's lag passes a threshold; the budget is a constant the operator sets, and the
  supported envelope is what the arms measure under it.
- **The factor does not change and a `Decommission` cannot be cancelled.** A three-node
  cluster at a factor of three decommissioning to two stays blocked until a fourth member
  joins or the request is left blocked; there is no `SetReplicationFactor` and no operation
  that takes a `Leaving` member back to `Member` short of the plan failing.
- **The grace is the policy's, not per member.** `SetPolicy` for `auto_remove_after` is not an
  operation; changing it is a bootstrap.
- **A plan is never previewed.** The steps appear on the record as the leader issues them;
  there is no dry run.
- **A removed member's replacement is its identity's replacement, not its data's.** The sets
  are rebuilt from the surviving copies; a removed node's directory is evidence and never a
  source.
- **`under_replicated_sets` counts sets holding a removing or removed member's copy**, not
  sets short of the factor for any other reason; a `Down` member's copy still counts, as C3
  says it should.
- **The leader's own `Decommission` hands the lead over and waits for the next leader to
  finish**; the record stays `Finishing` with the reason across the handoff.
- **The p99 budget is judged on the arm, not in the fixture.** The drain test prints the p99
  before and during and asserts zero final errors; a two-times bound at smoke scale on a
  shared machine would be a flake, and the capture is where the number lives.
- **The detector under load.** A busy development host loses enough of a member's reports at
  a 200 ms interval to call it down during a drain; the drain test runs at the default
  interval. The control lane's RPC timeouts under load are older than this feature
  ([item 100](../appendix/resolved/clone-fencing-under-load.md), whose suite-load half turned
  out to be the host's io_uring limits).

## Invariants to uphold

- **`is_placeable` is the one placement check.** `Initialize`, `apply_move`'s destination,
  `maybe_promote` and the planner's eligibility read it; a new placement check written against
  `health == Up` alone will place onto a leaving member.
- **A `Removing` member's phase and grace survive `SetHealth Up` and `ObserveMember`.** Both
  branch on the phase; a late heartbeat that cleared either would reverse a removal.
- **`GraceElapsed` is monotonic and of one episode.** A count below the committed value or of
  another episode is applied as nothing; the leader's `GraceLocal` restarts from the committed
  value whenever it is higher than what it started from.
- **A grace is counted by the leader alone, from its last commit.** `grace_seen` is cleared
  on election and on any change to the grace's suspension or episode.
- **A plan's step is judged by its move's record and nothing else.** `next_plan_update` reads
  `state.moves[op]`; a move the state forgot is a failed step.
- **The planner is pure and deterministic.** No clock, no random source, no state; ties break
  by node id and tablet; the test permutes the input.
- **A tombstone needs every set gone, and comes before the membership change.**
  `apply_tombstone` refuses while `rule_sets_served` names the member; `finish_removal`
  proposes it before `change_membership`.
- **A tombstoned identity is refused at every door:** `observe`, `Admit`, `SetHealth`,
  `drain_joins`, `handle_report`, `observe_membership`, and the node's own `handle_applied`.
- **The role of a member the configuration no longer names is `Learner`.**
  `observe_membership` sets it; `voters()` is what `wait_voters` and the promotion read.
- **Capacity lives in the leader's memory and the plan in the log.** Nothing in `apply` reads
  `NodeCapacity`; nothing outside the leader reads it at all.
- **The receiver's reserve and stream checks read the same override the report does.**
  `capacity::free_bytes` is the one function; a test that overrides one overrides both.
- **The rate limiter is taken before the read, per chunk, on every stream.** A stream that
  bypassed it would be a stream the budget does not bound.
- **The harness never asks a node killed for good for a report.** `node_reports` takes the
  dead node; a fault with `restart: false` is the only source of one.

## Performance

The cost is measured by four arms under one family, `cluster-rebalance`
([C10](../distributed/performance.md)), each the kill arm's placement, mixture and client with
a plan the control leader drives from a third of the way through:
`macro/cluster/rebalance/add` stages a spare and asks for a `Rebalance`; `decommission`
stages a spare and drains node one onto it; `remove` stages a spare, kills node one for good
under a five second grace and follows the expiry plan; `capacity_blocked` stages no spare and
decommissions node one at N = RF. `cluster.rebalance` records the kind, the marks, the steps
and what they moved, the blocked reason, the windows and the series, and `p99_ratio_permille`
- `during` over `before`, in thousandths - which is the number M9b's two-times budget is judged
on. The remove arm carries `cluster.fault` beside it with no restart mark.

**Smoke-run on the development host only** (europa, `--scale smoke --runs 1 --allow-dirty`
into a scratch directory under `target/` that was deleted, every core available since four
nodes of three shards need fifteen, twenty-four second runs with the plan at eight seconds).
`add`: a `Rebalance` derived six steps at once - two off each placed node onto the spare,
which is the feasible target at equal weights over nine sets - and moved five of them before
the run ended, `unfinished`, the client's p99 at 0.82 of its `before`. `decommission`: six
steps derived in two rounds of three under the cap, four moved, `unfinished`, the p99 at
1.02. `remove`: the kill at eight seconds, the grace elapsed at sixteen, the expiry plan's
first three steps issued and two moved in the eight seconds left, `unfinished`, the p99 at
0.99, and `cluster.fault` beside it with no restart mark and the errors of
[item 110](../appendix/known-issues.md#110-the-kill-arms-client-fails-a-steady-share-of-its-operations-for-as-long-as-node-one-is-dead)
running to the end. `capacity_blocked`: no step, blocked naming every one of the nine sets
and the further member each needs, `unfinished` by construction, the p99 at 1.02. A move on
this host under the smoke mixture is five to six seconds - the learner, the catch-up, the
joint transition, the activation and a one second grace - so three rounds of three outlast
the sixteen seconds a smoke run leaves; a full run leaves forty. Every `bytes` is the count of
sets, since nothing was archived at smoke scale. The shape of the record and nothing about
magnitude; the capture is the benchmark host's, and `finished_ms` on the three arms with a
spare is what it has to show.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `automatic_removal_and_rejoin_preserve_fencing` | `shoal/tests/cluster_fixture.rs` | The grace never expires or expires early, the expiry plan does not move every set to the spare, the member is not tombstoned or keeps a voter seat, a node restarted from its directory or a clone of it is admitted, or the directory is gone |
| `remove_without_replacement_capacity_stays_blocked` | `shoal/tests/cluster_fixture.rs` | At N = RF the plan is not blocked naming the missing member, the factor shrinks, a copy is dropped, a tombstone lands, the survivors stop serving, or a fourth member joined does not complete it |
| `removal_grace_survives_control_leader_restart` | `shoal/tests/cluster_fixture.rs` | The committed elapsed time is lower after a leader restart, or the member is removed before the grace or more than two increments and an election after it |
| `maintenance_suspends_automatic_removal` | `shoal/tests/cluster_fixture.rs` | A suspended grace counts or expires, the remaining deadline moves while suspended, resumption does not remove at the remaining deadline, or a removal can be suspended |
| `heterogeneous_placement_obeys_feasible_weights` | `shoal/tests/cluster_fixture.rs` | Weights 3:1:1:1 over four do not settle at 3:2:2:2 sets within one set's bytes on the light nodes, a second rebalance moves something, or three at three do not answer nothing naming the constraint |
| `node_transfer_budgets_bound_concurrent_sources` | `shoal/tests/cluster_fixture.rs` | Two streams assemble at once on a shard capped at one, the spare receives more than the bucket's bound, no sender waits on its budget, a plan under the reserve is not blocked naming it or does not run once it is met, or the oracle rejects the writers' history |
| `decommission_drains_within_supported_load_envelope` | `shoal/tests/cluster_fixture.rs` | A write is unanswered inside its budget, more than one set moves at a time under a cap of one, the member is not leaving throughout or removed at the end, its seat is not refilled, or the history is not sequential |
| `a_member_is_decommissioned_removed_and_tombstoned` | `shoal-core/src/server/control/types.rs` | A phase transition, a refusal, a late `Up` on `Removing`, a tombstone with sets still held, a tombstoned identity at observe or admit, a plan's progress, or a dropped role is wrong |
| `grace_elapsed_is_monotonic_and_expires_once` | `shoal-core/src/server/control/types.rs` | A count goes backwards or crosses episodes, a suspended grace counts, a resumed one restarts, an expiry records no plan or records two, or a `null` policy opens a grace |
| `the_planner_drains_balances_and_blocks` | `shoal-core/src/server/control/planner.rs` | A drain picks the wrong destination or ignores the replacement, feasibility ignores the reserve or the cap, a blocked reason names the wrong thing, the weighted target is not the feasible one, N = RF moves something, or a permuted input plans differently |
| `a_plan_record_reads_its_steps` | `shoal-core/src/server/control/plan.rs` | Live steps, failures per set or the completed outcome are read wrong |
| `tablet_bytes_follow_the_map` | `shoal-core/src/server/tables/storage/fs/tests.rs` | Bytes per tablet do not follow an insert, a replacement, a removal or a reopen |
| `a_rate_limiter_paces_a_stream` | `shoal-core/src/server/replication/network.rs` | The bucket admits faster than the rate, holds more than a second, or zero is a limit |
| `free_bytes_reads_the_filesystem_and_the_override` | `shoal-core/src/server/control/capacity.rs` | The override does not win, or lifting it does not restore the filesystem's figure |
| `the_migration_block_parses_with_its_defaults`, `the_rebalance_block_parses_with_its_defaults`, `validation_refuses_what_is_not_built` | `shoal-core/src/server/conf/cluster.rs` | The budget, stream, reserve and rebalance defaults move, or a budget under a chunk, no streams, no moves per node, a hysteresis past one or a plan interval under the reports is accepted |
| `admin_bodies_round_trip` | `shoal-proto/src/shared/protocol/admin.rs` | A placement operation is not a mutation, a plan read is, one fails to round trip, or an older member frame does not read as a plain member |
| `rebalance_capture_records_plan_and_windows` | `shoal-bench/src/workloads/harness/background.rs` | The record's marks, steps, bytes, blocked reason, windows, series or ratio are wrong, or an F45 record fails to load |
| `the_rebalance_arms_share_the_kill_arms_placement` | `shoal-bench/src/workloads/cluster_rebalance.rs` | An arm's placement differs from the kill arm's, a spare is missing or present on the wrong arm, the grace is on the wrong arm, the kill restarts, or an id is out of registry order |

## Related

[C3](../distributed/membership.md), [C8](../distributed/rebalancing.md),
[C9](../distributed/operations.md), [C13](../distributed/protocol.md#q7-and-q8-at-m9b),
[F39](membership.md), [F42](primary-failover.md), [F43](node-recovery.md),
[F44](repair.md), [F45](replica-migration.md),
[O56](../appendix/optimizations.md#o56-the-planner-recomputes-every-rule-set-on-every-look),
[O57](../appendix/optimizations.md#o57-tablet-bytes-are-rescanned-from-the-whole-archive-map-on-every-report).
