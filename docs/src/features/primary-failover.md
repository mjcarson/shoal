# F42. Primary failover

## Context

[M4](../distributed/milestones.md#m4-replication-and-quorum-writes) put a Raft group under every
tablet and [M5](../distributed/milestones.md#m5-read-consistency-levels) gave a read a way to see
the write. What neither did was survive the loss of the node that led. A leader that died took
the writes to its groups with it until an election, and the election was openraft's and worked;
what did not was everything around it. A write whose reply was lost could never be asked about
again, since the client minted a new bundle id for every send and the group's retry table was
rebuilt from a log the compactor purges. An isolated leader kept believing it led and answered
`OutcomeUnknown` at its deadline for writes nothing had accepted. A barrier asked of a member
that had lost the lead waited out the deadline on its own handle. A node holding no copy of a
tablet routed every query for it to the placement primary, dead or not. A member that fell
silent before its fifth report was never called `Down`. And the failover base every node's
groups ran at was its own file's, not the cluster's.

[M6](../distributed/milestones.md#m6-primary-failover) is that list, closed, and the fourteen
acceptance rows the C pages had filed against it since the plan was written - across
[C2](../distributed/transport.md), [C3](../distributed/membership.md),
[C5](../distributed/replication.md), [C6](../distributed/reads.md), [C7](../distributed/failover.md),
[C10](../distributed/performance.md) and [C13](../distributed/protocol.md) - written as tests and
run. On the way it closes [item 101](../appendix/resolved/short-lived-member-detection.md), the
detector's silence on a member that died young, reproduced first.

The plan for this milestone was drawn up with the user on 2026-09-13 and three decisions were
taken there: **the retry is opt-in in the client**, an identity a caller pins and a budget it
names, rather than a retry the client does on its own; **one failover benchmark arm**, a kill and
a restart under the reference mixture, smoke-run on the development host only; and **the server
reroutes definite non-answers only** - a forward the link never wrote goes to another holder
once, and nothing that might have been accepted is ever sent twice by the server.

## What it does

### A retry table that survives the purge point

Every persistent group's remembered requests - identity, payload digest, result and the index
each was applied at - are written to `wal/Shard-N/retries.bin` as postcard, atomically, on the
same trigger as `checkpoint.json` ~~and before it~~ - staged as `retries.next.bin` before it
and renamed over `retries.bin` once it landed, so a crash anywhere in the write leaves a
sidecar for the checkpoint on disk, which `Retries::recover` settles at open
([Resolved #115](../appendix/resolved/retry-sidecar-crash-window.md)). The checkpoint names the index the sidecar is
complete to (`retries_at`) and the table's low-water mark (`retry_floor`), both defaulting to
zero so an M4 file still loads. At open a group is seeded from a sidecar only when the sidecar
was written for exactly its checkpoint, and only with entries applied at or below it: a seeded
entry above the checkpoint would make the replay answer `Duplicate` and skip the apply the
table needs. The checkpoint counts as durable only once `checkpoint.json` itself landed, as
before. Volatile groups persist nothing. ~~Identity expiry is M9a's; the floor is what its check
will read~~ Identity expiry arrived with [F45](replica-migration.md), and it reads the
identity's own time and the table's eviction watermark rather than the floor
(`Retries::seed_for`, `MachineState::remembered_through`, `retry_floor`, `expired_before`).

### A lapsed lease is `NotLeader`, at once

openraft's leader never steps down on its own. Isolated, it keeps believing it leads, and what
its lease decides is only whether it takes a *new* write: `client_write` on a leader whose lease
lapsed is refused with an empty forward hint. Both the proposal loop and the barrier loop met
that hint by waiting for "a leader", which a handle that names itself satisfies at once, so they
spun until their deadline and answered `OutcomeUnknown` for a write nothing had accepted.
`Lease::of` (`shoal-core/src/server/replication/lease.rs`) classifies a handle once - `Leads`,
`NotStarted`, `Lapsed`, `Elsewhere(member)`, `Electing` - from the metrics' state and last
quorum acknowledgement against `election_timeout_max`, with a voter alone its own quorum as
openraft has it. A lapsed lease is `NotLeader` before anything is appended and `QuorumUnavailable`
for a barrier at once; a fresh one polls; an election waits within the deadline. The lease is
judged from the metrics' state rather than the vote the log persists, since a restarted node
that voted for itself before the crash carries that vote through the restart.

### A barrier follows the leader, and a forwarded write's budget counts down

A barrier asked of a member that names another leader asks that member next rather than
polling its own handle, bounded by the deadline with `LEASE_POLL` between hops; the `Fatal` arm
of the lane's barrier handler sends the error it built rather than dropping it. A write proposes
under the shorter of the proposal deadline and what is left of its bundle's budget, so a write
forwarded from another node counts down from the origin's budget and is answered at the
bundle's own deadline rather than the server's proposal deadline.

### Routing by health, and one reroute of a definite non-answer

A node holding no copy of a tablet routes it to the first replica the map calls `Up`, primary
first and the primary anyway when nobody is (`TabletMap::preferred_holder`, `read_ring_for`).
Health is routing advice and never authority: a holder the map has wrong refuses the query where
the link is. A forward the link never wrote - reported in `LinkEvent::Down`'s unsent list, a
definite non-answer with nothing accepted - is sent again, once, to another holder that is up and
neither the failed node nor this one (`TabletMap::alternate_holder`), under the same attempt and
slot, since the gather's slot still waits for exactly that share. A `Pending` keeps its entry,
its bundle's bytes and the partitions it covers for that; the reroute is counted on
`ReadStats::reroutes`; a share whose partitions have no holder in common is `Unavailable` as
before, and a rerouted share that fails again is too. The routing's archived partition keys
answer for writes as well as reads - insert, delete, update, exists - which the reroute needs to
find a share's other holder.

The same rule on the replication lane: a proposal or a barrier hopping to a leader whose link
is down is failed as `RpcFailure::NotSent` when its frame is in the unsent list, which the
proposal loop answers `NotLeader` at once, and stays `Unreachable` - unknown - only when the
frame was written. And a link a frame wants redials at `reconnect_min` rather than waiting out
its backoff: the backoff grows for an idle link and is cut short at the floor by the first frame
that wants to go, so a write hopping to a dead leader is refused in a hundred milliseconds
rather than after the five seconds a dial nobody asked for would have taken. Both came out of
the failover arm's first smoke run, below.

### The failover base is the cluster's

The map carries the policy's `primary_failover_after` (`TabletMap::primary_failover_ms`), and
`group_config` reads it when it is nonzero, so every node's groups miss a leader at the pace the
cluster agreed rather than the pace its own file says. The mapping is unchanged from M4:
`election_timeout_min` is the base, `election_timeout_max` twice it, the heartbeat a tenth. A
`SetHealth` commit moves the map's version, so shards rebuild their rings on it.

### A member that died young is called `Down`

The detector waited for five real intervals before it would judge a member, so one that fell
silent before its fifth report was never called `Down`: three samples, a phi of zero, forever.
The expected interval now stands in for the missing samples, as a new leader's seed already did
([Resolved #101](../appendix/resolved/short-lived-member-detection.md)).

### A client that can ask again

`SendOptions::identity(uuid)` pins the bundle id, which is what every `Command` already carries
as its `RequestId`, so a retry under the same identity reaches the group's retry table and is
answered as the first attempt was: the original result, applied once. A pinned id still in
flight is refused with `Errors::Config` rather than minted over. `SendOptions::retry(within)` is
the loop that uses it: `send_one_with` and the new `exec_with` repeat a send with a backoff from
twenty milliseconds to half a second while the answer says to try again - `NotLeader`,
`Unavailable`, `QuorumUnavailable`, `ConnectionLost`, `OutcomeUnknown`, `Timeout`, a lost
connection or a dead pool - and stop on anything definite; a failure delivered as a response
counts the same as one delivered as an error. Neither option rides the wire. A send with a
deadline now also has a timer: `DEADLINE_SLACK`, a second past the deadline the server was given,
after which the waiter is withdrawn and the outcome reported unknown as `Timeout`, so a server
that died with the bundle cannot hold the caller past the budget it named.
`ShoalResponse::bundle()` and `attempts()` say what was sent and how many times. Streams never
retry. The response is tracked before the bundle is serialized, so the archived bytes always
carry the id the waiter is registered under.

### What the fixture can do to a cluster now

`STALL_SHARD <shard> <ms>` blocks one shard's executor with a blocking sleep on a spawned task
while the control thread keeps reporting, which is a data shard stalled under a live control
plane. `DROP_REPLIES <n>` drops the next `n` committed write replies on a shard, so a client sees
its own deadline for a write that landed. Helpers ask a named node rather than node zero
(`wait_group_leader_via`, `wait_group_leader_change`), since the tests kill node zero.

### The failover arm

`macro/cluster/failover/kill` is the durable replication arm's placement and mixture driven for
a fixed time - a minute at full scale, twenty-four seconds at smoke - by a client that does not
retry, with node one, which leads a third of the groups, killed a third of the way through and
started again from the same staged identity two thirds through. The harness does the killing on
a thread of its own on the driver's clock (`workloads/harness/fault.rs`); the timed driver
(`drive_mixed_timed`) stamps every operation on a timeline and pauses ten milliseconds after a
failure. The `cluster.fault` record cuts the timeline at what the client saw: `before`, up to
its first failed operation after the kill; `during`, until the first operation after which two
seconds succeeded; `after`, the rest. Each window carries its own distribution, the outage is
their boundary in milliseconds, and a per second series of operations, errors and percentiles
keeps the dip visible when the windows are read as three numbers, which is what C10 means by
"do not average the outage away". `FaultFacts` is mirrored into the explorer whole.

## Design choices

**The retry is the caller's.** A client that retried on its own would have to decide, for every
send, whether the caller can tolerate a second attempt landing after it gave up; only the caller
knows. So the identity and the budget are options, the default sends once as it always has, and
a caller that wants the retry says so and gets `attempts()` back.

**Identity is the bundle id, not a new field.** Every `Command` already carried `RequestId {
bundle, index }`, and every group already answered a repeat from its table. Pinning the id the
client mints is the smallest change that makes a retry a retry, and it means an identity never
rides the wire twice - it *is* the id the frame is addressed under.

**The sidecar is a file beside the checkpoint, not part of it.** `checkpoint.json` is small and
read by hand; the retry table is thousands of entries. A sidecar keyed to the checkpoint's index
lets an M4 checkpoint load with no sidecar and a sidecar written for another checkpoint be
ignored, and it is written before the checkpoint so a crash between the two leaves a sidecar
nothing names rather than a checkpoint whose sidecar is missing.

**The lease is judged, never waited on.** The `None` hint has three meanings on openraft's side
and waiting on any of them on a handle that names itself returns at once. `Lease::of` reads the
metrics once and answers; only `Electing` waits, and only within the deadline.

**Definite non-answers are rerouted; nothing else is.** A frame the link never wrote was never
seen, so sending it again cannot apply it twice. A frame that was written might have been, and
the server cannot know; that one is the client's to retry under its identity. The line is the
link's unsent list, which was already what `resolve_lost_link` judged by, on both the data lane
and the replication lane.

**A wanted link redials at the floor.** The first smoke run of the failover arm showed writes to
the dead node's groups waiting up to five seconds and coming back `OutcomeUnknown`: the hop's
frame sat in a link queue waiting out an exponential backoff, and when the next dial failed the
link failed every pending RPC as unreachable. Two changes, both in the transport: a frame in the
unsent list is `NotSent`, and a frame queued during a backoff cuts the wait short at
`reconnect_min`. The second also stopped the control plane losing its leader every time a member
returned - the leader's link to the returning member waited out its backoff, the member heard no
heartbeat, elected itself at a higher term, and the leader stepped down for it; the smoke run
counted five such step-downs before and none after.

**Node one is the arm's victim.** Node zero is the driver's own process and cannot be killed
without ending the run; node one is the first peer and leads a third of the groups, since every
group starts where the placement put its primary, so a kill there is a third of the writes.

**A closed loop with a pause after failure.** A closed loop against a dead endpoint fails as fast
as the kernel refuses it, and a slot that sent again at once would count thousands of refusals a
second and call them operations. The pause keeps the error count a count of attempts a client
would plausibly make; the outage is measured in time, never in errors.

## Alternatives rejected

**A retry the client does by default.** Rejected with the user: a second attempt landing after
the caller gave up is a semantic the caller has to opt into.

**Stepping an isolated leader down at its lease.** openraft has no API for it short of a higher
vote, and a leader that stepped down on a timer would step down under a slow network as readily
as under a partition. Judging the lease and refusing the write gets the client the same answer -
`NotLeader`, try elsewhere - without inventing a step-down the library does not have.

**Rerouting an unknown outcome.** A forward that was written to a socket may have been applied.
Sending it again from the server would apply it twice under the same identity only if the retry
table had forgotten it, and the table is bounded; the client's identity is what makes a second
attempt safe, and the client is where the decision belongs.

**A read retry of its own.** The reroute covers a read whose share the link never wrote, which is
the case a retry within the budget was filed for; a share that was written and never answered
is a timeout, and re-sending it after the deadline would be a second read for a caller that
already got an error.

**A shorter lease.** `election_timeout_max` is the lease, and it is twice the base: a follower
refuses every vote until the leader it last heard from has been silent that long, so a failover
at a base of five seconds completes between ten and fifteen. A lease of the base alone would
elect faster and flap under a slow heartbeat. The base is the operator's knob and the fixture
runs at one second; the arm runs at the default and shows what the default costs.

**Refusing to queue on a link in backoff.** It would answer a hop at once, and it would also mean
a link nobody could queue on never redialled, since a dial is what a queued frame asks for. The
floor keeps the queue the trigger and bounds the wait instead.

**The kill on a node with a share of the driver.** C10 asks for a separate load driver; the arm's
driver is in process with node zero, as every cluster arm's is, and the record says so.

## Limitations

- **Failover takes two to three times the base.** The follower lease is `election_timeout_max`,
  twice the base, and a randomized election follows it; the fixture at one second fails over in
  two to three, the default of five in ten to fifteen. The arm's outage number is the policy's
  before it is the code's.
- **A returning leader waits out its own lease.** A killed leader restarted before its lease
  lapses ~~asks for its old term back and is refused by the followers' lease of it~~ does not
  stand for one lease length, since the followers' lease of it would refuse it; its groups are
  led again once the survivors elect, ~~and writes that hop to it meanwhile wait on that
  election within their deadline~~ and a write that hops to it meanwhile is refused
  `NotLeader` at once ([Resolved #103](../appendix/resolved/returning-leader.md)).
- **Leadership is not moved toward a reader or back to a returning node.** An election puts it
  where the election puts it; nothing transfers it.
- ~~**Identity expiry is M9a's.** The floor is recorded; nothing reads it yet. An identity below
  the floor is applied as new, as at M4.~~ Built by [F45](replica-migration.md): a time-ordered
  identity older than the retry window or the table's eviction watermark is `IdentityExpired`
  before it is proposed; one that is not time-ordered is applied as new once forgotten, still.
- **Streams never retry.** `stream_with` ignores `retry`; a bundle that is open-ended has no
  answer to compare a second attempt against.
- **A rerouted share is rerouted once.** A second failure is `Unavailable`.
- ~~**Catch-up past the purge point is M7's.** A returning node behind its leader's purge point is
  still refused by name.~~ Delivered by [F43](node-recovery.md).
- ~~**A `Down` member is never removed.** `auto_remove_after` is M9b's; the grace test asserts the
  placement holds and nothing more.~~ Since [F46](capacity-rebalancing.md) a `Down` member is
  removed once its grace elapses; the grace test still asserts the placement holds inside it.
- **The capture is the benchmark host's.** The arm ran at smoke scale on the development host,
  three times; the numbers below are the third run's shape, not a capture.
- **The wasm explorer was not checked.** The `wasm32-unknown-unknown` target is not installed on
  the development host's toolchain; the native feature check passed, and the change to
  `shoal-top` is serde types alone.
- ~~**The fixture's port reservations sit in the ephemeral range.** A deferred node can lose its
  port to an outbound connection under the full suite.~~ The fixture's ports come from a block
  below the floor since [Resolved #102](../appendix/resolved/fixture-port-block.md).

## Invariants to uphold

- **A seeded retry entry is never above the checkpoint.** `seed_for` takes entries with `applied
  <= retries_at` and only from a sidecar whose `retries_at` equals the checkpoint's; a seeded
  entry the replay will re-apply makes the replay answer `Duplicate` and skip the table.
- ~~**`retries.bin` is written before `checkpoint.json`, and the checkpoint is durable only once
  the latter landed.** The order is what makes a crash between the two harmless.~~ It did not:
  the sidecar that described the checkpoint on disk was the one overwritten, and a crash
  between the two opened the group remembering nothing below its checkpoint
  ([Resolved #115](../appendix/resolved/retry-sidecar-crash-window.md)). **The new sidecar is
  staged as `retries.next.bin` before `checkpoint.json` and renamed over `retries.bin` only once
  it landed, and a staged file found at open is settled before any group starts**: at every
  point some file describes the checkpoint on disk.
- **The lease is read from the metrics' state, never from the persisted vote.** A restarted node
  carries its old vote for itself; the state says whether it leads.
- **A lapsed lease appends nothing.** `NotLeader` is returned before `client_write`; a write
  refused this way is a definite refusal a client may send elsewhere at once.
- **Only a frame in the unsent list is rerouted or failed `NotSent`.** The link's list is the one
  source; anything else is unknown.
- **A reroute keeps the attempt and the slot.** The gather's slot waits for exactly that share;
  a reroute under a new attempt would drop the answer as late.
- **A pinned identity is never re-minted.** A collision on a pinned id is `Errors::Config`; a
  retry that silently changed its id would be a second write.
- **The retry loop stops on anything definite.** `retriable` is a closed list; a code not on it
  ends the loop with that error.
- **The deadline timer withdraws the waiter.** A `Timeout` from the timer means the response, if
  it ever arrives, is dropped by the map; the outcome is unknown and the identity is how to ask.
- **A wanted link never dials faster than `reconnect_min`.** The floor is what bounds the dials
  at a dead peer; the backoff above it is for idle links.
- **Node one is the only node the arm kills, and node zero never.** `fault::inject` refuses
  node zero by construction.
- **The windows are cut by the client, not the schedule.** `first_failure_ms` is the first
  failure at or after the kill; `recovered_ms` the first success with `SUSTAINED` clean after
  it. A window cut at the schedule would put the election's refusals in `before`.
- **The fault thread is joined before the servers are read.** A peer half restarted when the
  reports are taken is a record of a cluster that does not exist.

## Performance

One arm, `macro/cluster/failover/kill`, smoke-run on the development host three times (europa:
`powersave` governor, 32 threads, three nodes of three shards on nine physical cores, two runs
each at a hundredth of the data, twenty-four second runs, the cluster's default failover base
of five seconds). **Not a capture**: the numbers say what shape an answer has and nothing about
magnitude on the benchmark host, and the capture is jove's to take. The `--allow-dirty` output
went to a scratch directory and was deleted. The three runs are the story of the two transport
changes above:

| Run | Outage | `during` errors | Outcomes on node zero | Shape |
| --- | --- | --- | --- | --- |
| Before either change | 15.9 s | 256 of 1,723 ops | 231 unknown, 39 rejected | Whole seconds with no completed operation: every slot waiting five seconds for a hop that was never written |
| Unsent frames `NotSent` | 11.7 s | 214 of 1,324 ops | 43 unknown, 211 rejected | Refusals instead of unknowns, and the same empty seconds: the refusal waited out the link's backoff |
| And the floor on a wanted link | not resolved in the run | 1,418 of 8,773 ops | 15 unknown, 1,415 rejected | Nine hundred operations a second through the outage, a third of them refused at once; the empty seconds are after the restart, while the returning node waits out its old lease |

What the shape says: with both changes a client without a retry sees the dead node's third of
the writes refused within a hundred milliseconds and everything else served at the rate it was
before - the `during` window's p50 of 174 µs is the reads and the surviving groups' writes -
until the survivors elect, which at a five second base is ten to fifteen seconds after the
kill. The third run's outage is unresolved because the restart at sixteen seconds put the old
leader back before its lease lapsed, and its re-election is what the rest of the run waited on
([Resolved #103](../appendix/resolved/returning-leader.md), since fixed); the second run, where the election beat the restart, recovered at 19.7 seconds. The
write p50 of about forty milliseconds in `before` is the durable quorum on this host's shared
disk under `powersave`, the same as the replication arm shows here, and the read p50 of a
hundred microseconds is the local replica. The control plane lost its leader once per restart
in the first two runs and not at all in the third.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `stale_heartbeat_reports_cannot_lose_acked_write` | `shoal/tests/cluster_fixture.rs` | An election chooses a member whose log lacks an acknowledged write, or the survivors do not converge on it |
| `delayed_topology_cannot_authorize_old_primary` | `shoal/tests/cluster_fixture.rs` | A write through an old primary past its lease is `OutcomeUnknown` at a deadline or succeeds; a strong read through it is the stale value; the refused write appears somewhere after healing |
| `shard_stall_with_live_control_plane_can_fail_over` | `shoal/tests/cluster_fixture.rs` | A stalled shard's groups are not failed over while the control plane calls the node `Up`, or the shard does not follow the higher term when it wakes |
| `strong_read_refuses_isolated_old_primary` | `shoal/tests/cluster_fixture.rs` | An isolated old primary passes a barrier, or a `One` read through it stops being served, or a write through it past the lease is not `NotLeader` |
| `quorum_loss_is_unavailable_without_data_loss` | `shoal/tests/cluster_fixture.rs` | A minority commits a write, or a key's value differs between nodes after the survivors return |
| `read_barrier_survives_leader_change_and_delayed_messages` | `shoal/tests/cluster_fixture.rs` | A paused-and-resumed leader serves a strong read of the old value, or its barrier never hops to the new leader |
| `down_retains_placement_during_grace` | `shoal/tests/cluster_fixture.rs` | A `Down` verdict moves a replica or a placement, a member that died young is never called `Down` (item 101), or a key it led cannot be written once its group elected |
| `metadata_quorum_cannot_replace_a_missing_data_quorum` | `shoal/tests/cluster_fixture.rs` | A control commit lets a tablet minority take a write, or a write refused by an old primary appears after healing |
| `established_tablets_survive_control_quorum_loss` | `shoal/tests/cluster_fixture.rs` | Writes or strong reads stop when the control plane has no quorum, or an admin mutation succeeds without one |
| `lost_response_retry_returns_original_result` | `shoal/tests/cluster_fixture.rs` | A retry under the same identity is applied twice or answered as a delete of nothing, through a leader change or through a restart past the purge point |
| `retry_table_survives_a_crash_between_sidecar_and_checkpoint` | `shoal/tests/cluster_fixture.rs` | A crash between the staged sidecar and the checkpoint file opens the group remembering nothing below its checkpoint, and a retry is applied as new ([Resolved #115](../appendix/resolved/retry-sidecar-crash-window.md)) |
| `session_read_waits_for_committed_lower_bound` | `shoal/tests/cluster_fixture.rs` | A session read on a behind replica is served early, a token stops working through a leader change, or a forged lineage is served |
| `deadline_and_operation_id_survive_forwarding` | `shoal/tests/cluster_fixture.rs` | A forwarded write's identity or budget is reset across the hop, or a write whose link is down is not sent to another holder |
| `quorum_history_survives_repeated_elections` | `shoal/tests/cluster_fixture.rs` | The oracle rejects the history after repeated elections with lost replies: a result changed, an operation applied twice, or a read that disagrees |
| `retry_table_survives_the_purge_point` | `shoal-core/src/server/wal/tests.rs` | A retry after a checkpoint and a purge is applied as new, an M4 checkpoint stops loading, or a sidecar for another checkpoint is seeded |
| `a_stopped_checkpoint_write_leaves_a_sidecar_for_the_checkpoint_on_disk` | `shoal-core/src/server/wal/tests.rs` | A write stopped before its checkpoint file seeds nothing, or one stopped before its rename ignores the staged sidecar ([Resolved #115](../appendix/resolved/retry-sidecar-crash-window.md)) |
| `routing_prefers_holders_that_are_up_and_reroutes_a_never_sent_share` | `shoal-core/src/server/map.rs` | A non-holder routes to a `Down` primary, or the alternate holder is the failed node or this one |
| `a_member_silent_before_its_fifth_report_is_suspected` | `shoal-core/src/server/control/detector.rs` | Item 101 returns: three samples and silence is a phi of zero |
| `a_retry_repeats_only_what_says_to_try_again` | `shoal-client/src/client.rs` | The loop retries a definite failure, stops on a retriable one, re-mints a pinned id, or miscounts attempts |
| `fault_capture_preserves_outage_time_series` | `shoal-bench/src/workloads/harness/fault.rs` | The windows are cut at the schedule, the outage is averaged into the run, the series loses a second, or a record from before the arm stops loading |
| `a_fault_nobody_noticed_has_no_outage`, `an_unresolved_outage_has_no_recovery` | `shoal-bench/src/workloads/harness/fault.rs` | A run with no failures reports an outage, or one that ended inside the outage reports a recovery |
| `the_failover_arm_shares_the_replication_placement` | `shoal-bench/src/workloads/cluster_failover.rs` | The arm drifts off the durable arm's placement or mixture, kills node zero, or schedules the fault outside the run |
| `the_facts_mirrors_are_total` | `shoal-bench/tests/explore_index.rs` | The explorer's mirror loses a fault fact |
| `acceptance_tables_have_unique_tests_and_valid_milestones` | `shoal-bench/tests/acceptance_tables.rs` | An M6 row's test stops existing |

## Related

[C7. Primary failover and recovering a node](../distributed/failover.md),
[C5. Replication](../distributed/replication.md), [C6. Reads](../distributed/reads.md),
[C2. The transport](../distributed/transport.md), [C3. Membership](../distributed/membership.md),
[C10. Performance](../distributed/performance.md), [C13. The protocol](../distributed/protocol.md#q4-at-m6),
[F40. Replication and quorum writes](replication.md), [F41. Read consistency levels](read-consistency.md),
[F39. Membership](membership.md), [F38. The inter-node transport](inter-node-transport.md),
[Resolved #101](../appendix/resolved/short-lived-member-detection.md),
[the client](../api/client.md).
