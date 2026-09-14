# C11. Acceptance tests and the cluster harness

## Context

Use both deterministic protocol schedules and real Shoal processes. A pure protocol model can
explore elections, persistence completions and configuration changes without emulating Glommio.
Real process/storage tests verify that the adapter obeys that model. Neither replaces the other.
The first draft's twenty repetitions of an insert ledger could miss systematic rollback,
conditional-result, snapshot and reconfiguration defects.

## What exists today

~~Integration helpers use fixed sleeps and process-local port counters.~~ Since
[F36](../features/cluster-harness.md), every helper starts on port zero and waits on
`ShoalPool::ready`, which reports the address the shards bound or the first shard that failed
([Resolved #38, 58, 88](../appendix/resolved/pool-readiness.md)). Existing crash tests re-exec
a test binary and synchronize on a readiness line, and the cluster fixture below is that shape
generalized. Since [F37](../features/node-identity-control-plane.md) its servers are cluster
nodes of one, each with a control core the allocator owns; it can start a standalone child, stage
a marker, narrow a child's affinity, and restart a node on its directory, and its `Endpoints`
carry the identities and the control core a child reported. Since
[F38](../features/inter-node-transport.md) it places a cluster: one cluster id and a node id per
child, a marker each, a data and a control port each, and one placement every child reads; a
byte proxy can stand in front of each node's lanes so a test can cut, delay or heal one lane to
one node, and a child answers `PING`, `VOTE_PROBE`, `TRANSPORT`, `PROBE_BULK` and `FLUSH` on
stdin. Since [F39](../features/membership.md) it stages a membership cluster - node zero's
marker names the cluster, every other child's says `joining` with a pre-minted id and node
zero's control address as its seed, the proxies are per direction and each child dials the
others through its own set - initializes it once every child has joined, and can restart a
node on its directory or with other seeds, start a deferred node, kill, clone a directory and
spawn the clone, isolate and heal; a child also answers `MEMBERS`, `READINESS`, `MAP`,
`INITIALIZE`, `SET_VOTERS`, `ADMIN`, `INCARNATION`, `LOG_LEN`, `FAIL_SHARD` and `STALE_REPORT`,
and `SHOAL_CHILD_LOG` keeps every child's log. Since [F40](../features/replication.md) the
fixture drives replication: a child answers `GROUPS` - every group a node hosts, with its
members, leader, applied, committed and checkpoint indexes - `DIGEST <table>`, the rows and a
hash over every shard's applied state that every convergence test compares across nodes,
`ROTATE` and `COMPACT`, and `STALL_WAL <group>` / `RELEASE_WAL <group>`, which hold a group's
flush completions on that node so a test can build a quorum short by exactly one durable
voter; the builder sets `primary_failover_after` (a second by default in tests), the write
deadline, the pending bound and one node's durability; and the write ledger below is what
`conditional_results_follow_committed_order` feeds the `shoal-model` oracle. Since
[F41](../features/read-consistency.md) a child answers `HOLD_SHARES <shard> <ms> [dup]`, which
keeps every share that shard would send for that long and sends each twice on release if asked,
`GATHERS`, the resident gathers and every read counter folded over the node, and
`SET_TABLE_READ_POLICY <table> <one|quorum|clear>`; the builder sets `read_consistency` and
`query_deadline`; and the helpers read and write with `SendOptions` and keep a write's session
token. Since [F42](../features/primary-failover.md) a child answers `STALL_SHARD <shard> <ms>`,
which blocks that shard's executor for that long while the control thread keeps reporting -
a data shard stalled under a live control plane - and `DROP_REPLIES <n>`, which drops the next
`n` committed write replies on a shard so a client sees only its own deadline for a write that
landed; the helpers ask a named node for a key's leader rather than node zero, since the M6
tests kill node zero, and write and delete under a `SendOptions` identity with a retry budget.
Since [F43](../features/node-recovery.md) a child answers `SNAPSHOT <group>`, the manifest of
a cut taken now, and `CRASH_AT <point>`, which arms it to exit at one of the seven points of
an install; the builder sets `checkpoint_entries`, `retained_entries`, `segment_bytes`,
`retained_bytes`, `snapshot_chunk_bytes` and `bulk_queue_bytes`, and a staged node `crash_at`
and `install_hold_ms`; a proxy link can be throttled to a byte rate so a stream that a loopback
would carry in milliseconds takes seconds; `DIGEST` reads archived partitions beside resident
ones, since after a restart or an install nothing is resident; and the helpers leave a node
behind the purge point on purpose and wait for it to install and converge.
Since [F46](../features/capacity-rebalancing.md) a child answers `DECOMMISSION <node>`,
`REMOVE <node> [<replacement>]`, `MAINTENANCE <node> on|off` and `REBALANCE`, each as the
process and answering the operation the plan was recorded under, `PLAN_STATUS <op>` and
`PLANS` for the records, and `FREE_BYTES <bytes>|none`, which overrides what the node reports
as free and what its receiver checks against the reserve, so a capacity test fills no disk;
the builder sets `auto_remove_after`, `weight(node, w)`, `stream_budget(node, bytes_per_sec,
streams)`, `disk_reserve`, `moves_per_node` and `plan_interval`.
Since [F45](../features/replica-migration.md) a child answers `MOVE <key-hex> <from> <to>`,
which asks as the process for the set holding the key's tablet to move from one node to
another and answers the operation, `MOVE_STATUS <op>`, the record, and `MOVE_CRASH_AT <phase>
[<group>]`, which arms the child to exit right after its driver's commit of that phase is
acknowledged - for one group when named, else for whichever commits it first, which on a set
of two tables is two nodes at once; the builder sets `retire_after`, `catchup_lag`,
`migration_timeout` and `retry_window`; and the helpers start three placed nodes with a spare
beside them, find a pair one node leads and another completes, wait for a move's phase or its
end, read a group's committed voters from `GROUPS`, and say whether a retired copy's marker or
an archived partition is still on a node - the last by corrupting it, which on a copy the
cluster no longer counts is harmless.
A standalone node in the same test binary is started in process, since the fixture's
directories all belong to the cluster. Benchmark
readiness probes and tracing-based path assertions provide reusable patterns. There is no whole-engine deterministic simulator; this proposal does not require
building one before testing the new protocol state machine, and `shoal-model` is the pure model
it asks for instead.

## The design

### The Cluster fixture

Run nodes as child processes with independent directories, explicit data/control/driver affinity,
and client/data/control endpoints. Startup reports actual bound addresses and process/control/data
readiness separately. Cleanup stops/reaps all children and relays even when a test fails. Use
storage paths exercising direct I/O, with tmpfs controls explicitly separated.

Avoid the bind-zero/drop/pass race: preferably let children bind port zero and report the actual
endpoints, or inherit already-bound listeners where runtime APIs permit it. A fallback reserves
then releases with bounded collision retries, not a claim that bind-zero followed by close is
race-free. Coordinate SO_REUSEPORT setup so all shard listeners use the one resolved node port.

M0 exercises process startup and generic fault controls against isolated servers or mock peers.
It cannot require replicated digests, NodeId or membership before those features land. Add their
fixture operations in the milestone that implements them. Do not make an empty mock test count
as proof of a distributed invariant.

### Deterministic protocol model

Factor the new protocol/adapter decisions into explicit events: client invocation, message
receipt, timer tick, storage completion, crash/restart, snapshot completion and configuration
commit. Model stable storage separately from volatile buffers and track what survives each crash.
Use seeded schedules, small bounded exhaustive cases and saved failing traces; minimize a failure
into a reproducible regression. Check invariants on every transition.

Drive the selected library through fake transport/storage where possible; also model Shoal's
wrapper decisions such as admission, durability completion, checkpoint installation and
control/data transition reconciliation. Run its storage conformance tests in addition to Shoal's
crash suite. Formal modeling is an optional additional tool; pure Rust event simulation is enough
to avoid making the whole Glommio runtime a prerequisite.

### Faults

| Fault | Injection and coverage |
| --- | --- |
| Process crash/pause | SIGKILL and SIGSTOP/SIGCONT, distinguished from disk failure and pure link partition |
| Directed partition | Independently block A→B and B→A, plus full cuts and nontransitive connectivity |
| Traffic-class fault | Delay/drop data consensus, status, control, snapshot or client responses independently |
| Duplicate/reorder | Frame-level fake transport/proxy and protocol-model schedules, not an assumption about TCP packet order |
| Partial node failure | Stall one shard, control thread, data connection, disk completion or application task |
| Storage faults | Delayed/failed fsync, short/torn writes, disk full, bit corruption, checkpoint/manifest interruption |
| Recovery faults | Kill at each transition or snapshot phase; repeat while old/new versions and stale maps coexist |

All discovered endpoints and reconnects must traverse the configured directed fault mechanism;
rewriting only seeds leaves post-join connections unpartitioned. With TLS, a byte proxy can pause
or cut a stream but cannot safely parse/drop encrypted application frames. Use test-only hooks
before encryption or a fake transport for frame-class manipulation, plus TLS process tests for
the real channel. The model must not depend on silently disabling validation in production paths.
*At M2 the fixture's link is that byte proxy - cut, delay, heal - and the frame-class fake
transport is still to come; the real-TLS test is `a_peer_listener_requires_a_certificate_from_the_cluster_authority`.
At M7 the proxy can also throttle a lane to a byte rate, and the recovery row's "kill at each
snapshot phase" is `CRASH_AT`, a child armed to exit at one of the seven points of an install
(`snapshot_install_is_atomic_at_every_crash_point`); the storage row's checkpoint/manifest
interruption is the same matrix, since the marker and the checkpoint are what the points
straddle. At M9a the migration row's "kill at every phase" is `MOVE_CRASH_AT`, a child armed to
exit right after committing a move phase for one named group, beside the destination and the
control leader killed by the fixture as the phase is reached
(`migration_resumes_after_each_phase_failure`). Delayed and failed fsyncs are still `STALL_WAL`, ~~and torn writes, disk full and bit
corruption are M8's~~ and at M8 bit corruption is `CORRUPT <table> <key>`, a byte of a
partition's archived record flipped in place by the compactor that owns the archives, beside
`FORGET` - the partition's map entry dropped - and `ERASE` - the partition rewritten with no live
row under a valid checksum; a corrupt checkpoint file or retry sidecar is refused at open by
its own checksum (`checkpoint_and_retries_are_checksummed`). Disk full and a torn write to an
archive are still filed.*

SIGKILL does not model loss of OS/device caches. Durability tests need injected persistence
completions and failure semantics, with controlled machine/power-loss experiments optional later.
Seeing bytes in a live file before acknowledgement does not prove they were fsynced.

### The write ledger

Record invocation/completion times, identity, payload, key/tablet, policy and outcome. Include
inserts, updates, deletes, delete/reinsert, conditional no-ops and original returned results.
Keep successful, definitely rejected and outcome-unknown operations distinct. Unknown commands
may appear zero or once; a successful retry with the same identity must resolve consistently.
Do not assume an error means an effect is absent.

Check histories against an independent sequential state-machine oracle at the scope promised by
C13: single-tablet strong operations, session lower bounds, default committed-prefix reads and
eventual convergence after quiescence. Multi-tablet queries/bundles have explicitly weaker scope.
A quorum read alone is not sufficient verification because a shared merge bug can hide divergent
replicas; inspect authoritative history and individual replica state at a common boundary too.

### Digests

Canonical logical digests include table/partition coverage and compare the same committed boundary.
Keep an independent expected-state oracle; tests and Admin::Repair sharing one digest function
must not be the only source of truth. Add a separate digest self-test for equal logical content
with different archive layouts and for corrupted/missing data. Quarantine/repair source selection
is tested by corrupting the primary as well as followers.
*At M4 the digest is `DIGEST <table>`: every shard folds its partitions' rows into a row count
and a hash over their serialized bytes, the child sums the counts and folds the hashes, and a
test compares nodes at a boundary it establishes itself - waiting on `wait_digests_equal`
until every node agrees, which is what the applied indexes the same verb reports per group
make legible when one does not. It is a digest of applied state, not of archives, so a layout
self-test is still to come.*
*At M8 ([F44](../features/repair.md)) the canonical digest is the scrub's: a log entry every
replica applies in committed order and takes a cut at, folding every partition's rows
re-serialized in key order under the schema fingerprint and the tablet list, so coverage is in
the digest and the boundary is the entry's index. `DIGEST` stays as it was, on purpose - the
independent fold this section requires - and `canonical_digest_ignores_archive_layout_at_same_boundary`
is the layout self-test: three replicas merged thrice, once and never agree at one boundary, a
forgotten or an erased partition does not, a corrupted one is invalid, and the fixture's fold
agrees with every verdict. Quarantine and source selection are tested by corrupting the primary
(`repair_detects_corrupt_primary_and_preserves_evidence`) and a follower
(`corrupt_follower_is_quarantined_and_repaired_from_a_verified_source`).*

### Assertions on the path

Use span attributes, events and explicit test hooks for admission, persistence, election, snapshot
and migration boundaries. Child subscribers report bounded structured traces to the parent. A
schedule names the point at which it blocks, instead of hoping repeated wall-clock races hit it.
Bound all waits and print node/term/configuration/progress evidence when a deadline fails.

### Not ignored

Small deterministic regressions and focused process faults run in the ordinary suite. Longer
random/soak matrices run in CI/nightly with preserved seeds/artifacts and explicit runtime budgets.
Repetition adds coverage but is not a substitute for the named failure schedules. CI must execute
the soak configuration rather than merely provide an unused feature flag.

### Where it lives

Keep shared fixture helpers under `shoal/tests/cluster/`, with actual Cargo integration-test entry
points in top-level `tests/*.rs` or explicit `[[test]]` targets; nested files are not automatically
separate Cargo test binaries. Keep the pure protocol/adapter model near the implementation or in
a test-support crate that does not require a running storage engine. *As built by
[F36](../features/cluster-harness.md):* the fixture is `shoal/tests/cluster/` with its entry
point `shoal/tests/cluster_fixture.rs`; the model is the workspace crate `shoal-model`, which
links no shoal crate, no runtime and no engine, with its tests in
`shoal-model/tests/protocol_model.rs` and its saved schedules under `shoal-model/schedules/`; the
docs check below is `shoal-bench/tests/acceptance_tables.rs`.

### The acceptance test table

C1–C10 and C13 are the source of truth for named tests. Do not duplicate every row and hand-count
it here. A docs check parses those tables and verifies unique names, a valid milestone and a
matching gate on the milestone page. This is the index of the owning tables:

| Page | Test scope |
| --- | --- |
| [C1](node-identity.md#acceptance-tests) | Identity, configuration, affinity and existing-data migration |
| [C2](transport.md#acceptance-tests) | Framing, flow control, compatibility and response identity |
| [C3](membership.md#acceptance-tests) | Control membership, reporting, grace and maintenance |
| [C4](tablet-map.md#acceptance-tests) | Placement/configuration distinction and bounded stale routing |
| [C5](replication.md#acceptance-tests) | Durability, progress, conditional results and retries |
| [C6](reads.md#acceptance-tests) | Barriers, sessions, empty results, limits and deadlines |
| [C7](failover.md#acceptance-tests) | Elections, checkpoint boundaries, recovery and corruption eligibility |
| [C8](rebalancing.md#acceptance-tests) | Transition crash matrix, capacity and local rehome |
| [C9](operations.md#acceptance-tests) | Authorization, repair, upgrades and disaster recovery |
| [C10](performance.md#acceptance-tests) | Comparable resource/semantic facts and failure time series |
| [C13](protocol.md#acceptance-tests) | Protocol invariants and separation of data/control quorums |

## Alternatives rejected

Rejecting all deterministic testing because Glommio itself is not simulated, an insert-only
success set as the complete oracle, fixed sleeps as readiness, and bind-zero/close as a guaranteed
reservation are superseded. A process pause and an asymmetric link partition exercise different
states and are labeled separately.

## What it costs

A protocol model, storage fault hooks, process fixtures and retained traces. Introduce those
hooks with the adapters they validate. CI tiers bound ordinary feedback time while regularly
running deeper schedules. No claim of under-two-second cluster startup is made before measurement.

## What it breaks

Readiness helpers, port allocation, crash synchronization and result-history recording. Existing
single-node tests adopt the generic fixes without depending on unbuilt cluster APIs.

## Invariants to uphold

- Faults cover actual post-discovery connections and distinguish disk persistence from process death.
- Tests preserve reproducible schedules and independent expected-state evidence.
- Successful, rejected and unknown outcomes are checked against their different contracts.
- Initial fixture tests do not depend on features scheduled for later milestones.
- Every named acceptance test has one milestone and an executable entry point when implemented.

## Prerequisites

M0 starts with today's server and a pure model. Add C1 identity, C3 membership, C5 log hooks and
C7/C8 snapshot/transition hooks incrementally. [C13](protocol.md) defines the properties tested.

## How it would be measured

Record test coverage by failure schedule and invariant, not only count or repetition. Track runtime
and retained failing seeds. [C10](performance.md) handles performance separately.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `fixture_reports_bound_endpoints_without_port_race` | Concurrent fixtures own distinct actual listeners and clean up after failure | M0 |
| `fixture_faults_cover_directed_links_and_reconnects` | All modeled/proxied paths remain subject to the requested fault after reconnect | M0 |
| `history_oracle_distinguishes_unknown_and_rejected` | Ambiguous effects are permitted only under the specified operation history rules | M0 |
| `saved_protocol_schedule_reproduces_failure` | A seeded/minimized synthetic violation is detected identically on replay | M0 |
| `acceptance_tables_have_unique_tests_and_valid_milestones` | Owning page tables and milestone gate references stay structurally consistent | M0 |

## Related and implementation references

[C13](protocol.md), [Test coverage](../appendix/test-coverage.md), [C10](performance.md).
[FoundationDB paper, testing section](https://www.foundationdb.org/files/fdb-paper.pdf) motivates
controlled scheduling and fault coverage; Shoal starts with its new protocol/adapter boundaries,
not a claim to have reproduced FoundationDB's entire simulator.
[OpenRaft integration guide](https://docs.rs/openraft/latest/openraft/docs/getting_started/index.html)
links its storage test suite; pin and run the suite matching the library release.
