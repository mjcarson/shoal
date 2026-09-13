# F41. Read consistency levels

## Context

[M4](../distributed/milestones.md#m4-replication-and-quorum-writes) made a default write a
durable quorum and a `One` read the local replica's applied state. That left the promise
[C6](../distributed/reads.md) opens with: a `One` read may not see a `Quorum` write yet, and
nothing a client could ask for changed that. A read through another node could be stale by
however far that replica's apply lagged, a read through the same node was current only because
[F40](replication.md) waits for the proposer's own apply before it answers, and no read anywhere
could be made to prove that the value it returned was the newest committed one.

[M5](../distributed/milestones.md#m5-read-consistency-levels) is that promise, built. It asks
for data-quorum read barriers, application waits, a session token that a write hands back and
a later read is served past, coverage that tells an empty share from a missing one, ordered
gather and limit semantics that are proved rather than assumed, deadlines and late-reply
handling on every gather, per-table policy resolved once for a mixed bundle, the Q5 decision on
whether `Primary` and `Quorum` are two names, and all of it on the wire through
[C2](../distributed/transport.md)'s selected-version contract rather than a flag day. On the
way it closes [item 33](../appendix/resolved/gather-expiry.md), which said a split query whose
share never arrived held its client forever - and it did, reproduced before the fix.

The plan for this milestone was drawn up with the user on 2026-09-12 and three decisions were
taken there: **one strong level, named `Quorum`**, with `Primary` not a level; **seven benchmark
arms**, three read levels on the replicated placement and four fan-out shapes on the factor one
placement, smoke-run on the development host only; and **the protocol model gets a strong read**
with one unsafe knob and one saved schedule.

## What it does

### Two levels, one strong

A read is served at `One` or `Quorum`. `One` is what every read was: the local replica's applied
state, or the placement primary's when this node holds no replica, possibly stale. `Quorum` is
the read barrier: before the shard reads anything, it asks the leader of the tablet's group for a
**read index** - openraft's `get_read_linearizer(ReadPolicy::ReadIndex)`, which records the
leader's commit index and confirms the leader's term with a heartbeat round a majority of
voters answer - and then waits until its own replica has applied through that index. The shard
that executes the read is the shard the ring routes it to, exactly as for `One`; what changes is
what it does first. When that shard leads the group the linearizer is its own handle's; when it
does not, it asks the leader over the replication lane with a new `ReplicateKind::ReadBarrier`,
rebuilds the linearizer over the leader's `ReadLogId` as openraft documents for follower reads,
and waits the same way (`shoal-core/src/server/shard/reads.rs`, `read_barrier`, `wait_on_group`).
A hint naming this shard as a leader whose lease has not started is polled at `LEASE_POLL`; a
group with no leader waits for an election; a link that cannot be sent on is retried until the
budget runs out. Every one of those ends at the bundle's deadline with `Timeout`, and a group
that cannot confirm a majority is `QuorumUnavailable`.

The wait runs on a task of its own. `execute_query` sees a read whose plan needs a wait and is
not yet `ready`, hands it to `await_read_barrier`, and the task posts `ServerMsg::ReadReady`
with what the waits cost or why the read cannot be served; the loop marks the plan ready and
runs the read, or answers the refusal where the rows would have gone. The loop never awaits a
`Raft` method, which is [F40](replication.md)'s rule, and a read parked on a disk load after its
barrier keeps `ready` and never waits twice.

`Quorum` is the one strong level. The draft's `Primary` - route to the leader and read there -
was two names for one freshness promise, and the decision under Q5 is recorded in
[C13](../distributed/protocol.md#q5-at-m5): a name is earned by an implementation, and the
barrier is the implementation. `Primary` as a routing choice would be additive - "execute the
share at the leader" is a third arm of the same match - and it is filed, not built.

### A token a write hands back

A committed write's answer carries a **session token**: the cluster, the table, the tablet the
key hashed to, the group serving it and the log index it committed at, forty-eight bytes minted
in `answer_proposal` and framed ahead of the response payload under `Flags::SESSION_TOKEN`
(`shoal-proto/src/shared/protocol/read.rs`). `ShoalResponse::session_token()` hands it to the
caller. A read carrying one - `SendOptions::token`, up to sixteen a bundle - is served only once
the replica has applied at least that index in that group's log: the shard checks the token's
cluster against the map, its group against the group serving that tablet here, and then
`raft.wait(remaining).applied_index_at_least(index)`. A token from another cluster, or any token
sent to a standalone node, is refused `WrongCluster`; one naming a group that does not serve its
tablet on the replica asked is refused `UnknownLineage`, counted as a lineage refusal. Under
`Quorum` the barrier subsumes the wait and the lineage check still runs. A token is an index and
never a term: a committed index is never lost, so a lower bound on one is enough, and the
barrier's own read log id is what carries a term. A duplicate write's token names the index its
repeat committed at rather than the original's, which is later than it need be and never wrong.

### Every gather has a slot per share and a deadline

The shares of a split query used to be counted down; now each is a **slot**
(`shoal-core/src/server/shard/gather.rs`). The coordinator records one slot per shard it sends a
piece to, puts the slot's index on the piece's plan - in `QueryMetadata.read.slot` over the mesh,
in `EntryRead.slot` over a forward - and every share arrives naming its slot and the **attempt**
at the bundle it was sent under. A share for a key nobody holds, or for an attempt the gather has
moved past, is *late*; one for a slot already filled is a *duplicate*; both are counted, logged
at `DEBUG` and dropped, and neither touches the answer. A share with no rows covers its slot
exactly as one with a thousand does, which is what makes an empty partition and a missing share
two different answers. The gather replies once every slot is covered, or at once when one has
failed, and the key leaves the map either way.

Every bundle has a **deadline**: `networking.query_deadline`, ten seconds, standalone and
cluster alike, or the shorter budget a bundle names in its options, measured from when its last
byte came off the socket. A gather still owed shares at its deadline is answered once with
`Timeout` - "did not complete within its deadline; N of M shares arrived" - in the query's own
table variant, and every pending forward for it is forgotten so the forward sweep cannot answer
it a second time. The tick that sweeps them runs on every node now, not only a cluster one, at a
tenth of the shortest deadline in play and never under fifty milliseconds. A forwarded query's
pending deadline is the sooner of the forward timeout and the bundle's, and the forward carries
the budget *remaining* rather than a fresh one, so a whole forwarded query never outlives the
client's budget. A client that goes away takes its gathers with it.

### Policy, resolved once

A bundle may name a level for every read in it. Absent that, a read is served at its table's
policy, and absent that at the cluster's `read_consistency`. The table's policy is **versioned
control state**: `ControlCommand::SetTableReadPolicy`, proposed by the new
`AdminKind::SetTableReadPolicy { table, level }` over the client connection, refuses `All`, a
table the placement was not initialized with and a stale version, moves the topology version
once per change, is idempotent when unchanged, and clears with `level: None`. The map carries
it (`TabletMap::read_level_of`), the topology frame names it, and the coordinator resolves
every query's level once in `read_plan` and forwards it resolved: the serving node validates the
byte and never re-resolves against a default of its own. A mixed bundle - a `Row` get beside a
`Note` get - resolves each on its own, and an override covers both.
`cluster.read_consistency: All` is refused at validation naming C6.

### On the wire, under a capability

Nothing about the rkyv payload moved, so `PROTOCOL_VERSION` and the schema fingerprint did not.
A client spends the hello's reserved byte fourteen on a capability set, `CLIENT_CAP_READ_OPTIONS`;
the server's ack grants the subset it reads. Only then does a bundle carry a **read options
section** - `Flags::READ_OPTIONS`, sixteen bytes of head (version, level, token count, deadline in
milliseconds) and the tokens - behind its trace context, and only then does a response carry a
token between its id and its payload. Both sections are read into buffers of their own, so the
archive still lands at the start of its allocation. A section behind a bit is not something a
peer can skip by round-tripping the bit, because its length is not in the header; that is why
the bit is negotiated first, which is [C2](../distributed/transport.md)'s selected-version
contract and the same precedent [F35](wire-trace-context.md) set. An M4 server grants nothing and
is sent nothing.

Between nodes, `CAP_READ_CONSISTENCY_V1` names three things: a forward entry may carry an
`EntryRead` (level, slot, tokens) under `FLAG_READ`; the `Forwarded` head widened from thirty-two
bytes to ninety-six for the attempt, the slot and a token; and the replication lane serves
`ReadBarrier`. Six reserved bytes could not hold an attempt, a slot and a token, and widening under
the capability bit is the same exact-match break the bit already causes at the hello.

### What a client sees

`Shoal::send_with`, `send_one_with`, `send_one_stamped_with` and `stream_with` take a
`SendOptions` - a level, a deadline shorter than the server's, tokens - and
`ShoalBuilder::read_options` sets one for every send. Four error codes are new or newly
produced: `Timeout` (31), `WrongCluster` (52), `UnknownLineage` (53), `UnsupportedReadLevel` (54).
Every counter the shard keeps - barriers, hops, barrier and application wait totals and maxima,
session waits, lineage refusals, timeouts, late and duplicate shares - is a `ReadStats` on the
shard's replication report, folded per node, read by `AdminKind::Replication` and the fixture's
`GATHERS`, and written into a capture's `cluster.reads`.

## Design choices

**Coverage is the slot, not the rows.** [C6](../distributed/reads.md) asks that replies carry
explicit coverage even when no rows match. A row list cannot say what it does not contain; the
slot can. The table layer did not change - an empty share is still `Get(None)`, kept `None` when
every share is - and the invariant lives on the coordinator: no reply is built from fewer covered
slots than the query has. The response frame carries no coverage list, because nothing at M5
reconciles across shares and metadata with no consumer is a wire commitment for nothing; it is
filed with the capability bit it would sit behind.

**The deadline is a budget carried, not a timer reset.** A forward writes the milliseconds
*remaining* at send and the serving node counts down from its own arrival; a pending forward
expires at the sooner of its own timeout and the bundle's. C6's "do not reset it at every hop or
compare absolute clocks on different machines" is why: two machines' clocks are never compared
and a budget only ever shrinks.

**One barrier per group per read, joined sequentially.** A read over several tablets on one
shard waits on each group's barrier in turn. Nothing is gained by joining them, since C6 promises
no cross-tablet snapshot, and one task per read keeps the executor's queues short. The obvious
sharing - one barrier per group per *bundle* - is [O49](../appendix/optimizations.md#o49-one-barrier-per-group-per-bundle-rather-than-per-read).

**A refusal is answered where the rows would have gone.** The first draft routed a failed barrier
through the metadata's carried failure, the way a failed partition load is. That path swaps the
failure into an *open* answer, and a get whose partitions are resident is sealed in place and
never open - `apply_failure` asserts as much - so the read would have hung in a debug build and
returned rows in a release one. The refusal now builds the failed response itself and sends it as
a share or a whole answer.

**The plan travels with the share, never by rewriting the bundle.** Forward-as-bytes is
[F38](inter-node-transport.md)'s rule and it held: the level, the slot and the tokens are on the
entry, the token section is a separate read on the client, and no byte is ever prepended to a
payload buffer.

**A cluster arm's ports are numbered among the cluster arms.** The smoke run found the port
blocks - `20000 + 64 × position in IDS` - crossing 32768, the floor of Linux's ephemeral range,
around the two hundredth workload; a control port there is taken by the `TIME_WAIT` of a link
some earlier arm dialled out through it, and no socket option on the listener gets past that (it
was tried, with `SO_REUSEADDR` and `SO_REUSEPORT` together). The blocks are numbered by position
among the `macro/cluster/` arms now and the allocator refuses the range. Every node listener binds
with `SO_REUSEADDR` as well, which is what lets a node restarted on its own ports bind at once
after its own accepted connections closed.

## Alternatives rejected

**Lease reads.** openraft's `ReadPolicy::LeaseRead` would answer a strong read from the leader's
lease with no heartbeat round. It rests on clock, expiry, revocation and process-pause
assumptions the barrier does not, and Q6 says it is taken only after a timing proof and a measured
benefit; the barrier stays the default and `LeaseRead` is never used.

**A maximum-tuple merge across replicas.** C6's draft `Quorum` read fetched from several
replicas and kept the newest. A higher tuple can describe an uncommitted history, so the merge can
return a value that is later un-written; the barrier reads one authoritative replica after
proving authority and reconciles nothing.

**`Primary` as a second public level.** Decided under Q5 and recorded in C13: the same freshness
under two names would have been two code paths to keep equivalent. Routing the share to the
leader is filed as an additive third arm.

**A coverage list on the response frame.** No consumer at M5; filed with the capability bit.

**A sorted table in the fixture.** It would add a third table - nine more groups a node at a
factor of three - to every fixture test's start, restart and digest, for a limit semantics the
`responses.rs` proof and `sorted.rs`'s own limit tests already pin. ~~Filed for M9a.~~ M9a did not
need it either: its tests move the fixture's two tables and read them by key.

**A version bump for the section.** A new `PROTOCOL_VERSION` folds into the fingerprint and
refuses every older client, for a section most bundles never send. The capability byte lets an
old client and a new server, or the reverse, keep working with the section simply absent.

**Retrying a read within its budget.** The attempt is minted and echoed so a bounded reroute can
be added; ~~the reroute has to invalidate the old attempt's slots and re-forward the bundle's
bytes, which is M6's retry work, and it is not done here.~~ [F42](primary-failover.md) reroutes
a share the link never wrote under the *same* attempt and slot, since nothing under them was
accepted; a written share is never re-sent by the server.

## Limitations

- ~~**No read retry.** A share that fails or times out fails the query; nothing reroutes within the
  budget. The attempt identity is in place for it.~~ Since [F42](primary-failover.md) a share
  the link never wrote is sent to another holder once; a share that was written and timed out
  still fails the query.
- **No cross-tablet snapshot.** A read over several tablets waits on each group's barrier in turn
  and may observe the tablets at different instants, which is what C6 says.
- ~~**Tokens across a leader change are untested until M6.** The lineage check is by group identity
  and the wait by index; `session_read_waits_for_committed_lower_bound` and
  `read_barrier_survives_leader_change_and_delayed_messages` are M6 gates and were not run.~~
  Both run since [F42](primary-failover.md).
- **Leadership is not moved.** A strong read through a follower hops to the leader on every read;
  nothing transfers leadership toward the reader. F42 left it where it was too.
- **`Primary` is absent.** Filed.
- **Coverage is not on the wire.** The slot is server-side state; a client reads a complete
  answer or one error and nothing in between, and cannot see which partitions were covered.
- **A token does not expire.** An index lower bound has no reason to; a token from a lineage
  that no longer exists is refused by name rather than aged out.
- **The stage report does not draw the two wait stamps.** `StageStamps` carries `barrier_wait`
  and `apply_wait`; the report renders neither yet, so the read arms stay out of the stage layer
  and the waits are on the `cluster.reads` record instead. Filed.
- **The capture is the benchmark host's.** The seven arms ran at smoke scale on the development
  host and the numbers below are what that means.
- **`SO_REUSEADDR` on a listener does not get past a client-side `TIME_WAIT`.** Established by
  test on the development host's kernel; the port allocator is what avoids it, and a deployment
  that puts a Shoal port inside the ephemeral range is on its own.

## Invariants to uphold

- **A `Quorum` read applies through a read index a confirmed leader gave it, and only then
  reads.** The index comes from `get_read_linearizer(ReadIndex)` on the leader - never from a
  cached leader address, a lease, or a follower's own commit index - and the replica waits for
  its *own* apply to reach it. The protocol model's `strong_read_from_cached_leader` schedule is
  what a shortcut here loses.
- **A share is judged by attempt and slot, never by arrival.** Late and duplicate shares are
  dropped by identity; a gather's key is removed on completion or on expiry and never both.
- **No reply from fewer covered slots than slots.** An empty share covers; a missing one never
  does. The table layer's `Get(None)` is not coverage and must not be read as it.
- **A gather expires before its pendings do.** The tick sweeps gathers first, and an expired
  gather forgets its pending forwards; otherwise the forward sweep answers a query twice.
- **A deadline only shrinks.** A forward carries the budget remaining; a serving node counts down
  from arrival; a bundle's own deadline is never longer than the server's.
- **A section is sent only to a peer that granted its bit.** A client writes `READ_OPTIONS` only
  after the ack granted `CLIENT_CAP_READ_OPTIONS`; a server writes `SESSION_TOKEN` only down a
  connection whose hello asked. The header bit round-trips; the bytes behind it do not.
- **The plan is resolved once, on the coordinator.** A forwarded entry's level is a level, never
  "inherit"; `decode_entries` refuses a plan that names none.
- **The loop never awaits a `Raft` method, and the wait task never touches a group's `RefCell`.**
  `await_read_barrier` clones the handles and the network and nothing else.
- **A read refusal is answered as a response, not a carried failure.** The carried-failure path
  cannot reach a sealed answer.
- **A token names a lineage by group identity and is refused by name otherwise.** Never ignored,
  never served as `One`.
- **`read_consistency: All` and a `SetTableReadPolicy` of `All` are refused.** The strong level
  is `Quorum`; there is no read that waits on every replica.
- **Cluster port blocks stay under `EPHEMERAL_PORT_FLOOR`.** The allocator's test holds every
  cluster arm to it.

## Performance

Seven arms, smoke-run on the development host (europa: `powersave` governor, 32 threads, three
nodes of three shards on nine physical cores, two runs each at a hundredth of the data). **Not a
capture**: the numbers say what shape an answer has and nothing about magnitude on the benchmark
host, and the capture is jove's to take. The `--allow-dirty` output went to a scratch directory
and was deleted.

| Arm | p50 | p99 | `cluster.reads` |
| --- | --- | --- | --- |
| `macro/cluster/reads/one` | 714 µs | 17.3 ms | no barriers |
| `macro/cluster/reads/barrier` | 1.63 ms | 24.0 ms | 200 barriers, 147 hopped, mean barrier wait 590 µs, max 1.65 ms; apply wait max 2 µs |
| `macro/cluster/reads/session` | 782 µs | 1.43 ms | 200 session waits, apply wait max 3 µs, no barriers |
| `macro/cluster/fanout/get` | 239 µs | 1.10 ms | six rows |
| `macro/cluster/fanout/filter` | 232 µs | 290 µs | three rows |
| `macro/cluster/fanout/limit` | 241 µs | 335 µs | three rows |
| `macro/cluster/fanout/empty` | 201 µs | 974 µs | no rows, every slot covered |

What the shape says: `barrier` less `one` at the median is about a millisecond, of which the
barrier wait itself is 590 µs on average - the leader's heartbeat round, plus the hop on 147 of
200 reads, which is the two-in-three a three-node placement predicts - and the apply wait is
nothing, because nothing was being written. `session` costs a token check and a wait that was
already satisfied, and its tail is the tightest of the three because every read is served past a
known index on a replica that already holds it. The fan-out shapes cost the same three-way split
whether they carry six rows, three or none, which is the point of `empty`: a missing share and an
empty one cost the same to prove absent. The `one` and `barrier` p99 tails at 17 and 24 ms are
cold partitions under a `powersave` governor at depth thirty-two on a hundredth of the data, not
a property of either level.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `barrier_read_observes_prior_quorum_write` | `shoal/tests/cluster_fixture.rs` | A `Quorum` read through another node is served stale, or a cut node's strong read answers the old value instead of `Timeout`, or a session read is served before its apply |
| `session_token_lineage_is_checked_by_name` | `shoal/tests/cluster_fixture.rs` | A token from another cluster, another lineage or sent to a standalone node is served instead of refused `WrongCluster`/`UnknownLineage`; a client that asked for no token section is sent one |
| `empty_and_deleted_partitions_have_explicit_coverage` | `shoal/tests/cluster_fixture.rs` | An empty share fails a get, a deleted row is resurrected, or a get missing a share answers with the rows that did arrive |
| `limits_apply_after_complete_ordered_gather` | `shoal-proto/src/shared/responses.rs`, `shoal/tests/cluster_fixture.rs` | The per share limit pushdown drops a required row for some layout or arrival order, or a held share lets a limited get answer with fewer shares than it has |
| `gather_timeout_completes_once_and_discards_late_replies` | `shoal-core/src/server/shard/gather.rs`, `shoal/tests/cluster_fixture.rs` | A gather answers twice, a late share is merged after the answer, a duplicate is counted as coverage, or the stream ends more than once |
| `a_failed_share_completes_at_once_and_an_empty_share_still_covers` | `shoal-core/src/server/shard/gather.rs` | A failed share waits for the rest, or an empty share stops covering its slot |
| `a_gone_client_drops_only_its_own_gathers` | `shoal-core/src/server/shard/gather.rs` | A client's gathers outlive it, or another client's go with it |
| `mixed_table_bundle_resolves_each_table_policy` | `shoal-core/src/server/control/types.rs`, `shoal/tests/cluster_fixture.rs` | A table's policy applies to another table, an override does not cover both, `All` or an unknown table is accepted, or the version moves on an unchanged policy |
| `a_standalone_gather_expires_at_the_query_deadline` | `shoal/tests/gather_expiry.rs` | [Item 33](../appendix/resolved/gather-expiry.md) returns: a split get whose share never arrives never returns |
| `read_options_and_tokens_round_trip_on_the_wire` | `shoal-proto/src/shared/protocol/tests.rs` | The section or the token drifts, a refused version or count is accepted, the un-optioned preamble stops being byte-identical, or the capability byte moves |
| `forward_entries_carry_read_plans_and_answers_carry_attempts` | `shoal-proto/src/shared/protocol/peer/tests.rs` | A plan or the widened head drifts, an inherited level or too many tokens is accepted, or an unknown answer flag is |
| `strong_reads_are_linearizable_and_the_cached_leader_knob_is_not` (`protocol_model_preserves_acknowledged_history`, `saved_protocol_schedule_reproduces_failure`) | `shoal-model/tests/protocol_model.rs` | The model's strong read stops observing an acknowledged write under the safe rule, or the cached-leader knob stops being caught by `schedules/strong_read_from_cached_leader.json` |
| `read_capture_records_barrier_and_application_wait` | `shoal-bench/src/workloads/harness/cluster.rs` | A capture's `cluster.reads` loses a field, or a record from before it stops loading |
| `the_read_arms_share_the_replication_placement` | `shoal-bench/src/workloads/cluster_reads.rs` | An arm moves off the placement, an infeasible factor is declared, or the ids leave `IDS` order |
| `fanout_keys_are_two_per_node_in_node_order` | `shoal-bench/src/workloads/cluster_reads.rs` | A fan-out read stops touching every node |
| `validation_refuses_what_is_not_built` | `shoal-core/src/server/conf/cluster.rs` | `read_consistency: All` starts a node |
| `a_config_without_a_query_deadline_gets_the_default` | `shoal-core/src/server/conf.rs` | The committed `shoal.yml` stops loading, or the deadline stops being read |
| `cluster_ports_are_disjoint_from_the_single_node_range_and_bounded` | `shoal-bench/src/run/plan.rs` | A cluster arm's ports enter the ephemeral range |
| `the_facts_mirrors_are_total` | `shoal-bench/tests/explore_index.rs` | The explorer's mirror loses a read fact |
| `acceptance_tables_have_unique_tests_and_valid_milestones` | `shoal-bench/tests/acceptance_tables.rs` | An M5 row's test stops existing |

## Related

[C6. Reads and consistency levels](../distributed/reads.md), [C2. The transport](../distributed/transport.md),
[C4. The tablet map](../distributed/tablet-map.md), [C10. Performance](../distributed/performance.md),
[C13. The protocol](../distributed/protocol.md#q5-at-m5), [F40. Replication and quorum writes](replication.md),
[F38. The inter-node transport](inter-node-transport.md), [F35. The wire trace context](wire-trace-context.md),
[Resolved #33](../appendix/resolved/gather-expiry.md), [The error channel](error-channel.md).
