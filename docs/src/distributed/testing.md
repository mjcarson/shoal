# C11. Acceptance tests and the cluster harness

## Context

Use both deterministic protocol schedules and real Shoal processes. A pure protocol model can
explore elections, persistence completions and configuration changes without emulating Glommio.
Real process/storage tests verify that the adapter obeys that model. Neither replaces the other.
The first draft's twenty repetitions of an insert ledger could miss systematic rollback,
conditional-result, snapshot and reconfiguration defects.

## What exists today

Integration helpers use fixed sleeps and process-local port counters. Existing crash tests re-exec
a test binary and synchronize on a readiness line. Benchmark readiness probes and tracing-based
path assertions provide reusable patterns. There is no whole-engine deterministic simulator;
this proposal does not require building one before testing the new protocol state machine.

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
a test-support crate that does not require a running storage engine.

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
