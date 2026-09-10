# C11. Acceptance tests and the cluster harness

## Context

Every `C` page ends with a table of tests, and most of them start with "kill a node". None of
that is possible today: the integration suite starts one server in-process, connects after a
fixed sleep, and cannot run two of its own binaries at once without them colliding on a port.
This page is the harness that makes the other pages' tables runnable, and the index that rolls
every one of those tables into one place with the milestone that builds each row. It is step 0
of the [milestones](milestones.md) for the same reason "instrument the client" was step 0 of
[Direction](../direction/overview.md#the-recommended-order): nothing below it can be judged
without it.

## What exists today

**One server, one sleep, one counter.** `shoal/tests/utils.rs`: `start_with_conf` starts a
`ShoalPool`, sleeps two seconds (`utils.rs:333`), and connects. Ports come from a
process-local `AtomicU16` starting at 13000 (`utils.rs:63-66`), which is
[item 38](../appendix/known-issues.md#38-integration-test-binaries-all-bind-the-same-ports):
every integration binary hands out the same numbers at the same time, and
[Test Coverage](../appendix/test-coverage.md#the-suite-cannot-safely-run-its-binaries-in-parallel)
calls it "the one place where [the suite] is unsound".

**The better primitives exist and are not shared.** `shoal/tests/pool.rs:58` has `dead_port`,
which binds `127.0.0.1:0`, reads the port back and drops it — used only to obtain a *refused*
port. `shoal-bench/src/workloads/harness/ready.rs:45` has `wait_until_answering`, a real probe
that replaced [F8](../features/purpose-built-workloads.md)'s five-second sleep, and
`shoal/tests` has not adopted it.

**A test can already run a second process.** The crash tests re-exec the test binary as a child
server, passing `SHOAL_CRASH_TEST_DIR` and `SHOAL_CRASH_TEST_PORT` and synchronising on the child
printing `SHOAL_CRASH_TEST_READY` (`utils.rs:340-346`). That is "start `N` server processes and
coordinate them" with `N = 1`, and the readiness-line convention is reusable as is.

**A test can already assert on which path ran, not just on the answer.** `tracing_topology.rs`
and `disk_lookups.rs` read `tracing`'s own span attributes rather than an exporter's output
([Test Coverage](../appendix/test-coverage.md#integration--shoaltests)), because "both paths
return the same rows, so nothing about a response says which one ran". A cluster test asking
"was this answered locally or forwarded" is the same question.

**Deterministic simulation is not available.** [D9](../direction/prior-art.md#foundationdb) named
FoundationDB's simulation — the whole system against a simulated network, disk and clock — as the
most valuable thing to copy. glommio and io_uring are not simulable without a runtime abstraction
Shoal does not have, and this page does not pretend otherwise: the harness is process-level, with
real sockets, real signals, and real time bounded by short timeouts.

## The design

### The `Cluster` fixture

```rust
pub struct Cluster {
    nodes: Vec<Node>,          // one child process each, node 0 is the bootstrap
    proxies: Vec<Proxy>,       // one per node pair, when a test asked for partitions
    dir: TempDir,              // one subdirectory per node
}

pub struct Node { id: NodeId, child: Child, client_port: u16, peer_port: u16, dir: PathBuf }

impl Cluster {
    pub async fn start(n: usize, conf: ClusterConf) -> Result<Self, TestError>;
    pub async fn client(&self, node: usize) -> Result<Shoal<TestDbClient>, TestError>;
    pub fn kill(&mut self, node: usize);                      // SIGKILL
    pub fn pause(&mut self, node: usize);                     // SIGSTOP: a partition of one
    pub fn resume(&mut self, node: usize);                    // SIGCONT
    pub async fn restart(&mut self, node: usize) -> Result<(), TestError>;   // same directory, same NodeId
    pub fn partition(&mut self, a: usize, b: usize);          // the proxy between a and b drops frames
    pub fn heal(&mut self, a: usize, b: usize);
    pub async fn admin(&self, node: usize, req: AdminRequest) -> Result<AdminReply, TestError>;
    pub async fn wait_lag_zero(&self, within: Duration) -> Result<(), TestError>;
    pub async fn digest(&self, tablet: u16) -> Result<Vec<(NodeId, Digest)>, TestError>;
}
```

**Nodes are child processes**, each a re-exec of the test binary with `SHOAL_CLUSTER_NODE=<n>`
and the node's ports, directory and seed in the environment — the crash tests' pattern with the
variable names generalised. The child runs `ShoalPool::start` on `TestDb` with a `cluster:` block
and prints the readiness line when `ready()` resolves ([C9](operations.md#readiness)), which the
fixture waits for instead of sleeping. One process per node is not a convenience: it is what
[C10](performance.md#emulating-a-cluster-on-one-machine) says a node has to be.

**Ports come from bind-zero, handed down.** The parent binds `127.0.0.1:0` twice per node, reads
the ports, drops the listeners, and passes the numbers to the child — `pool.rs::dead_port`
promoted to `utils.rs` and used for its other purpose. That closes item 38 for every binary that
uses the fixture, and is the pattern by which the item is closed for the rest: `get_unique_port`
becomes `dead_port` and the counter is deleted.

**Directories are under `CARGO_TARGET_TMPDIR`**, one per node, for the reason `test_dir` already
gives: `/tmp` is usually tmpfs, where glommio silently disables `O_DIRECT` and an alignment bug
cannot surface.

**The test config is small and fast.** `build_cluster_config` sets `failure_detector.interval_ms:
100`, `primary_failover_after: "1s"`, `auto_remove_after` per test, `intent_log_size: 64KiB` so a
snapshot path is reachable in seconds, and `replication_factor: 3`. Every timing assertion is
written against these values plus a margin, and the margin is a named constant.

### Faults

Three kinds, and they are different:

| Fault | Mechanism | Models |
| --- | --- | --- |
| `kill` | `SIGKILL` | A crash. The process is gone; its directory is intact; `restart` brings it back with the same `NodeId` |
| `pause` / `resume` | `SIGSTOP` / `SIGCONT` | A partition of one node from everyone, or a very long GC pause. The process is alive and will answer everything it was sent when it resumes — which is the case that finds fencing bugs |
| `partition(a, b)` / `heal` | A relay process between `a` and `b` that forwards frames until told to drop them | An asymmetric partition — `a` and `b` cannot reach each other and both can reach `c`. The case that finds "a minority marked the majority down" |

The relay is a small tokio TCP proxy started by the fixture when a test asks for partitions: node
`a`'s `cluster.seeds` and peer address for `b` point at the proxy, which forwards to `b`'s real
peer port. It is the only way to partition on loopback without root, and it is a test binary's
dependency, not the server's. It proxies the *peer* port only — a partition is between nodes, and
a test that wants a client cut off from a node kills the client.

### The write ledger

```rust
pub struct Ledger { acked: BTreeSet<(u64 /* partition key */, u64 /* sort key */)> }
```

A client-side record of every write the client was acknowledged for, at the level it asked. The
[C7](failover.md#acceptance-tests) ledger test drives writes at `Quorum` through a fault, then
reads every ledgered key at `Quorum` and asserts each is present. It is the test the whole
replication design is for, and it is run twenty times per suite because a failover bug is a
race, and a race that fails one time in twenty has to be given twenty chances.

### Digests

`Cluster::digest(tablet)` asks every replica, through `Admin::Repair`'s digest half
([C9](operations.md#repair)), for a hash of the tablet's partitions, and returns them per node. A
test asserting "every replica converged" compares them. The digest is the same code the operator's
repair uses, so the test and the tool cannot disagree about what "identical" means.

### Assertions on the path

Every test that asks *which* replica answered, *which* node forwarded, *which* catch-up path was
taken, asserts on span attributes the way `tracing_topology.rs` does — a subscriber in the test
process that records `Attributes` — rather than on the answer, which is the same either way. For
a child process the subscriber is in the child, and its recorded attributes are written to a file
the parent reads after the fact. One such test per binary where a subscriber is process-wide, the
rule [F35](../features/wire-trace-context.md) established.

### Not `#[ignore]`d

The fault tests run in the ordinary suite. They are bounded by the test config's short timers, a
three-node cluster starts in under two seconds when readiness is a line rather than a sleep, and
a test that is `#[ignore]`d is a test nobody runs. What *is* gated is the twenty-repetition
ledger test, behind `--features soak`, with a single-repetition version in the default run.

### Where it lives

`shoal/tests/cluster/` — `mod.rs` for the fixture, one binary per page: `identity.rs`,
`transport.rs`, `membership.rs`, `map.rs`, `replication.rs`, `reads.rs`, `failover.rs`,
`rebalancing.rs`, `operations.rs`. Each is a row group in the table below and in
[Test Coverage](../appendix/test-coverage.md)'s per-binary count when it lands.

### The acceptance test table

Every test named on C1–C10, one row per milestone, generated from the pages' own tables so the
two cannot drift. The per-page tables are the source; this one is the index.

| Milestone | Binary | Tests | Named |
| --- | --- | --- | --- |
| **M0** | `cluster/mod.rs`, plus `shoal-bench` | 15 | **C11**: `the_two_test_tables_agree`, `digests_agree_on_a_healthy_cluster`, `the_ledger_records_exactly_the_acked_writes`, `a_restarted_node_keeps_its_directory_and_id`, `kill_pause_resume_and_partition_do_what_they_say`, `readiness_is_a_line_not_a_sleep`, `the_fixture_starts_n_nodes_on_disjoint_ports`; **C9**: `ready_resolves_when_every_shard_listens`, `shard_failed_resolves_when_a_shard_panics`; **C10**: `a_cluster_arm_starts_n_processes_on_disjoint_cores`, `a_one_node_cluster_arm_records_the_same_facts_as_today`, `the_port_block_keeps_every_existing_arm_in_its_first_slot`, `every_cluster_id_has_a_family`, `nodes_is_a_sweep_axis`, `a_historical_artifact_still_parses` |
| **M1** | `identity.rs` | 8 | **C1**: `a_node_started_twice_keeps_its_id`, `a_directory_from_another_cluster_is_refused_by_name`, `a_format_one_marker_is_refused`, `omitting_the_cluster_block_is_a_single_node`, `a_misspelled_cluster_setting_is_rejected`, `the_documented_defaults_are_the_defaults`, `the_control_plane_runs_on_cpu_zero`; **C4**: `a_client_receives_a_topology_frame_on_connect` |
| **M2** | `transport.rs` | 11 | **C2**: `a_get_whose_partitions_live_on_the_other_node_is_answered`, `a_bundle_spanning_both_nodes_is_answered_once_per_query`, `one_trace_id_spans_client_and_both_nodes`, `a_peer_from_another_cluster_is_refused_with_an_ack`, `a_peer_built_from_a_different_schema_is_refused_with_an_ack`, `a_peer_whose_certificate_names_another_node_is_refused`, `a_frame_the_kernel_gave_the_wrong_shard_reaches_the_right_one`, `a_forwarded_bundle_is_validated_on_arrival`, `a_slow_peer_sheds_rather_than_growing_a_queue`, `a_partition_message_has_no_frame`; **C9**: `a_cross_node_trace_has_the_forward_span` |
| **M3** | `membership.rs`, `map.rs` | 16 | **C1**: `a_joining_node_does_not_serve_before_its_first_topology`; **C3**: `three_nodes_form_a_cluster_from_one_seed`, `a_fourth_node_joins_as_a_learner_with_no_tablets`, `every_node_holds_the_same_map`, `a_killed_node_is_unreachable_then_down`, `a_paused_node_is_a_partition_of_one`, `a_restarted_node_returns_to_up`, `a_minority_cannot_mark_the_majority_down`, `a_shard_timeout_makes_the_detector_suspicious_not_certain`, `the_cluster_log_lives_under_the_latency_path`; **C4**: `every_node_holds_the_same_map_at_the_same_version`, `bootstrap_spreads_tablets_by_shard_count`, `the_map_survives_a_restart`, `routing_benchmarks_still_run`; **C9**: `members_names_every_node_and_its_state`, `topology_admin_matches_the_pushed_frame` |
| **M4** | `replication.rs` | 22 | **C4**: `a_replica_set_names_distinct_nodes`, `primaries_are_dealt_evenly`, `a_one_node_cluster_at_rf_three_serves_one_copy`, `a_per_table_replication_factor_is_refused_by_name`; **C5**: `a_quorum_write_is_on_two_logs_before_the_ack`, `an_all_write_is_on_every_log_before_the_ack`, `a_one_write_waits_for_nobody`, `a_down_follower_does_not_delay_a_quorum_ack`, `too_few_replicas_up_is_unavailable_before_commit`, `every_replica_converges_after_quiescence`, `a_followers_log_replays_to_the_primarys_state`, `a_stale_primarys_replicate_is_refused`, `a_gap_is_caught_up_not_skipped`, `rotation_does_not_release_below_quorum`, `a_single_node_writes_format_two`, `a_format_one_log_is_refused`, `an_async_follower_acks_before_its_fsync`; **C6**: `a_one_read_prefers_the_local_replica`, `a_one_read_never_targets_a_down_replica`, `limit_spans_replicas`; **C9**: `lag_reports_a_paused_follower`, `quorum_wait_is_recorded_per_write` |
| **M5** | `reads.rs` | 7 | **C6**: `a_quorum_read_after_a_quorum_write_sees_it_on_another_coordinator`, `a_one_read_sees_a_quorum_write_eventually`, `a_primary_read_sees_a_one_write`, `a_quorum_read_repairs_a_lagging_replica`, `a_gather_times_out_and_answers_every_index`, `a_per_bundle_level_overrides_the_table_and_the_cluster`, `a_client_from_protocol_three_is_refused` |
| **M6** | `failover.rs` | 10 | **C3**: `down_moves_no_tablet`; **C6**: `a_primary_read_is_refused_when_the_lease_has_lapsed`; **C7**: `no_acknowledged_quorum_write_is_lost_across_a_primary_kill`, `writes_resume_within_the_failover_window`, `the_new_primary_has_the_highest_stamp`, `a_follower_ahead_of_the_quorum_truncates`, `a_stale_primary_cannot_acknowledge`, `a_one_write_may_be_lost_and_the_page_says_so`, `reads_at_one_continue_through_a_failover`; **C10**: `failover_reports_a_window_not_a_rate` |
| **M7** | `failover.rs` | 6 | **C7**: `a_killed_node_catches_up_by_log`, `a_killed_node_catches_up_by_snapshot`, `a_returning_primary_becomes_a_follower`, `a_returning_node_serves_current_tablets_while_others_catch_up`, `down_for_less_than_auto_remove_after_moves_no_tablet`, `a_torn_tail_on_a_follower_is_filled_not_lost` |
| **M8** | `operations.rs` | 1 | **C9**: `repair_restores_a_deleted_archive` |
| **M9** | `rebalancing.rs` | 16 | **C4**: `a_client_receives_a_topology_frame_on_change`, `a_stale_route_is_forwarded_not_refused`; **C8**: `adding_a_node_under_load_causes_no_client_error`, `a_move_adds_before_it_drops`, `one_move_per_pair_at_a_time`, `decommission_drains_and_serves`, `remove_of_a_down_node_re_replicates_from_survivors`, `a_removed_node_cannot_rejoin`, `a_stale_forward_during_a_move_is_answered`, `a_dropped_tablet_is_kept_for_the_grace_then_tombstoned`, `auto_remove_after_removes_and_rebalances`, `down_shorter_than_auto_remove_after_moves_nothing`, `orphans_are_reused_as_a_source`, `changing_cores_re_homes_tablets_on_the_same_node`, `shard_count_mismatch_is_gone`; **C9**: `remove_is_refused_for_an_up_node` |
| **M10** | `operations.rs` | 4 | **C9**: `a_state_change_needs_an_admin_principal`, `decommission_from_shoalctl_drains_the_node`, `a_peer_one_version_behind_is_accepted`; **C10**: `a_real_cluster_capture_records_every_nodes_env` |

A test that appears on a `C` page and not here, or here and not on a page, is a defect in this
part, and when M0 lands a test in `shoal-bench` asserts the two agree — the same shape as
`the_runners_copy_of_the_profiled_workloads_is_current`.

## Alternatives rejected

**Deterministic simulation.** Above: not available without a runtime abstraction, and building
one is larger than the feature. Filed, with the reason, so it is not re-argued.

**In-process nodes.** glommio's pinning has no teardown, one cpu 0, and a `SIGKILL` of a thread
is not a thing. Processes are what nodes are.

**`iptables` or network namespaces for partitions.** Root, and a machine configured for it. The
relay is a test dependency and runs anywhere `cargo test` does; namespaces are the arm to add
when a test needs a partition the relay cannot express — a lossy link, say.

**Sleeping for readiness.** Three nodes × two seconds × every test. The readiness line and the
probe exist; the sleep is deleted.

**`#[ignore]` on the fault tests.** A test nobody runs.

**A single ledger run.** A race needs repetitions; the soak feature is where they live.

## What it costs

- **A child process per node per test**, and a re-exec of the test binary for each. Under two
  seconds per three-node start with readiness lines; the cluster binaries will be the slowest in
  the suite and `test-coverage.md` says so.
- **A relay process per partitioned pair.**
- **The `soak` feature** and a CI configuration that runs it — which is a CI configuration, which
  the repository does not have ([TODOs — Build and packaging](../appendix/todos.md)).

## What it breaks

- **`get_unique_port` and the counter.** Deleted, and item 38 closed, for every binary at once.
- **`start_with_conf`'s sleep.** Replaced by readiness, which is a change to every integration
  test's timing and the only reason any of them gets faster.
- **`ShoalPool::start`'s callers**, once it returns a handle ([C9](operations.md#readiness)).

## Invariants to uphold

- **A node in a test is a process.** Never a thread, never a second pool.
- **Ports come from bind-zero, never from a counter.**
- **Readiness is observed, never assumed.** No `sleep` stands in for a node being up.
- **A `pause` is a partition and a `kill` is a crash, and a test names which it means.** They find
  different bugs.
- **Every "which path" assertion is on span attributes, never on the answer.**
- **The per-page tables and this table agree**, and a test enforces it.

## Prerequisites

[C9](operations.md)'s readiness handle and digest; [C1](node-identity.md)'s `cluster:` block for
the child's config. `M0` builds the fixture before any `C` page has code to test, against a
one-node "cluster" that is today's server.

## How it would be measured

Not a benchmark. The measure of a harness is what it finds, and
[Test Coverage](../appendix/test-coverage.md) records, per binary, what each reaches; the cluster
binaries' rows are added as each milestone lands, with the count re-measured rather than
incremented, which is that page's own rule.

## Acceptance tests

The fixture's own, all M0:

| Test | Asserts |
| --- | --- |
| `the_fixture_starts_n_nodes_on_disjoint_ports` | Three children, six distinct ports, none colliding with a concurrently running fixture in another binary |
| `readiness_is_a_line_not_a_sleep` | `start` returns within a bound after the last child prints the line, and no `sleep` is on the path |
| `kill_pause_resume_and_partition_do_what_they_say` | After `kill`, the child is reaped; after `pause`, it answers nothing and after `resume` it answers everything queued; after `partition(a, b)`, `a`'s pings to `b` are dropped and `a`'s to `c` are not |
| `a_restarted_node_keeps_its_directory_and_id` | `restart` reuses the directory and the child reports the same `NodeId` |
| `the_ledger_records_exactly_the_acked_writes` | A write that returned an error is not in the ledger; one that returned `Insert(true)` is |
| `digests_agree_on_a_healthy_cluster` | Three replicas of one tablet, no faults: identical |
| `the_two_test_tables_agree` | In `shoal-bench`: every test named on a `C` page is in this page's table under exactly one milestone, and vice versa |

## Related

- [Test Coverage](../appendix/test-coverage.md) — what the suite reaches, and the unsoundness this closes
- [item 38](../appendix/known-issues.md#38-integration-test-binaries-all-bind-the-same-ports) — closed by bind-zero
- [C9. Operating a cluster](operations.md) — readiness and digests, which the fixture calls
- [C10. Performance](performance.md) — the same "a node is a process" rule, for the harness
- [D9 — FoundationDB](../direction/prior-art.md#foundationdb) — the simulation this is not, and why
- [F35](../features/wire-trace-context.md) — one subscriber per binary
- [TODOs — Tests](../appendix/todos.md), [Build and packaging](../appendix/todos.md) — the CI this part will need
