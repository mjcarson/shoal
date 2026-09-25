# Test Coverage

What the test suite reaches, what it does not, and the one place where it ~~is~~ was unsound.

**Established by running it.** `cargo check --workspace --all-targets` passes with warnings and
`cargo test --workspace` passes: ~~**1,187 tests**~~ ~~**1,198 tests**~~ ~~**1,238 tests**~~
~~**1,265 tests**~~ ~~**1,289 tests**~~ ~~**1,320 tests**~~ ~~**1,342 tests**~~ ~~**1,361 tests**~~ ~~**1,382 tests**~~ ~~**1,398 tests**~~ ~~**1,414 tests**~~ ~~**1,432 tests**~~ ~~**1,449 tests**~~ ~~**1,467 tests**~~ ~~**1,475 tests**~~ ~~**1,484 tests**~~ ~~**1,492 tests**~~ ~~**1,493 tests**~~ ~~**1,494 tests**~~ ~~**1,495 tests**~~ ~~**1,496 tests**~~ ~~**1,497 tests**~~ ~~**1,499 tests**~~ ~~**1,502 tests**~~ ~~**1,503 tests**~~ ~~**1,504 tests**~~ ~~**1,506 tests**~~ ~~**1,507 tests**~~ ~~**1,509 tests**~~ ~~**1,529 tests**~~ ~~**1,541 tests**~~ ~~**1,543 tests**~~ ~~**1,549 tests**~~ ~~**1,555 tests**~~ ~~**1,564 tests**~~ ~~**1,576 tests**~~ ~~**1,587 tests**~~ ~~**1,589 tests**~~ ~~**1,605 tests**~~ ~~**1,608 tests**~~ ~~**1,611 tests**~~ ~~**1,614 tests**~~ ~~**1,619 tests**~~ ~~**1,629 tests**~~ ~~**1,633 tests**~~ ~~**1,634 tests**~~ ~~**1,635 tests**~~ ~~**1,636 tests**~~ ~~**1,637 tests**~~ ~~**1,639 tests**~~ ~~**1,640 tests**~~ ~~**1,641 tests**~~ **1,642 tests**, seven ignored, plus ~~**13**~~ **14** behind
`--features stage-profile` that a default run does not reach - ~~and one of those fourteen,
`stage_join.rs`, had not *compiled* since [F36](../features/cluster-harness.md) added two fields
to `RunRequest`~~ and one of those fourteen, `stage_join.rs`, ran only on a host with
`/opt/shoal` until [Resolved #97](resolved/stage-join-storage.md) gave it a scratch copy of the
committed configuration. The feature-gated binaries are built by the runbook line below, since
nothing else builds them:

```bash
# both feature-gated binaries compile, and the one server-starting test behind a feature runs
cargo check -p shoal-bench --features stage-profile,hotpath --all-targets
cargo test -p shoal-bench --features stage-profile --test stage_join
```

**Resolved #147 added 1**, 1,641 → 1,642: `a_paused_control_leader_calls_nobody_down` in
`cluster_fixture.rs`.

**Resolved #146 added 1**, 1,640 → 1,641: `a_write_through_a_lagging_copy_is_answered_within_two_heartbeats`
in `cluster_fixture.rs`. The workspace run at six threads after it: **1,633 passed, 1 failed, seven ignored**; the failure was a race in
`returning_node_catches_up_by_log_or_snapshot`'s own counting, fixed in the same change ([item 142](known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host)).

**Resolved #145 added 1**, 1,639 → 1,640: `a_write_through_an_installing_copy_is_answered_at_commit` in
`cluster_fixture.rs`. The workspace run at six threads after it: **1,633 passed, 0 failed, seven ignored**.

**Resolved #144 added 2**, 1,637 → 1,639: `a_silently_cut_node_rejoins_without_elections` in
`cluster_fixture.rs`, and `pre_vote_is_an_optional_capability` in the `shoal-proto` unit tests.

**O66 added 1**, 1,636 → 1,637: `repeating_openraft_targets_are_quiet_by_default` in the
`shoal-core` unit tests.

**O65 added 1**, 1,635 → 1,636: `a_group_config_keeps_the_timers_its_base_derives` in the
`shoal-core` unit tests.

**Resolved #143 added 1**, 1,634 → 1,635: `a_write_to_a_silently_cut_leader_fails_fast` in
`cluster_fixture.rs`, with the fixture proxy's new `blackhole` state and `Cluster::blackhole`, a
partition by dropped packets where `cut` is one by resets.

**O63 added 1**, 1,633 → 1,634: `a_returning_node_is_handed_back_its_groups` in
`cluster_fixture.rs`. The inventory's new `failover` key is covered inside the existing
`an_inventory_that_cannot_be_a_cluster_is_refused`, the render test in `render.rs` and
`a_rendered_node_claims_starts_and_initializes`, none of them new tests. The fixture suite at six
threads then passed 109 of 110, the one failure `local_rehome_recovers_after_each_crash_point`,
which passed four of four alone (item 100's shape under load).

**Its fixes for items 139 to 141 and O62 added 4 more**, 1,629 → 1,633:
`a_stopped_leader_hands_its_groups_off` in `cluster_fixture.rs` (item 139, with the fixture's new
`EXIT` verb and `Cluster::stop`), `a_long_intent_log_is_read_in_blocks` (item 140) and
`the_intent_log_is_folded_against_the_map_it_rewrites` (O62) in the `shoal-core` unit tests, and
`a_late_answer_for_a_closed_stream_is_dropped` (item 141) in the `shoal-client` unit tests.

**The [distributed cluster testing](../cluster-testing/overview.md) chapter added 10**, and the
total went 1,619 → 1,629:

| Binary | Added | Tests |
| --- | --- | --- |
| `shoal-client` unit tests | 2 | `a_frame_for_a_dropped_stream_does_not_end_the_read_loop`, `a_stream_is_failed_by_every_connection_that_owes_it` (items 130, 131) |
| `shoal/tests/ephemeral_unsorted_table.rs` | 1 | `a_stream_dropped_early_is_not_tracked` (item 60) |
| `shoal-core` unit tests | 2 | `what_a_query_carries_between_shards_is_send` (item 133), `a_leftover_temp_map_does_not_stop_a_save` (item 135) |
| `shoalctl` unit tests | 2 | `a_down_node_named_for_upgrade_is_a_repair` (item 136), `a_node_still_starting_its_shards_has_not_caught_up` (item 137) |
| `tmdb-dataset` unit tests | 2 | `a_mix_names_kinds_and_weights`, `synthetic_movies_are_apart_from_the_dataset` (the driver) |
| `shoal/tests/cluster_fixture.rs` | 1 | `a_stream_older_than_the_retry_window_still_writes` (item 138) |

The whole workspace run at six threads with `--no-fail-fast`, before the last of these was added,
gave **1,620 passed, 1 failed, seven ignored**. The failure was
`lost_response_retry_returns_original_result`, a checkpoint that did not land in time while the
lab was loading the same host, and it passed alone. Two earlier runs that shared europa with lab
load tests failed 15 and 9 fixture tests on handshake and election timeouts, all of which passed
on an idle host: this suite is what a loaded machine makes it.

**[Resolved #128](resolved/hop-deadline-margin.md) added 3**, and the total went 1,611 → 1,614: a
fixture test and a `shoal-core` unit test for the hop's budget, and a `tmdb-dataset` unit test for
the loader's retry. The whole workspace run at six threads with `--no-fail-fast` gave **1,599
passed, 8 failed, seven ignored**, all eight in `cluster_fixture.rs`. Two,
`backup_restore_verifies_history_in_new_cluster` and `local_rehome_recovers_after_each_crash_point`,
passed alone at once (item 100's shape). The other six fail alone too, with `AddrInUse` on
port 12000, because the development host was running a deployed lab node
(`shoal-tmdb.service`) on 12000–12002. The fixture's first fixed ports collide with a real
deployment on the same host, whatever the change.

**[F55](../features/cluster-upgrade.md) added 5**, and the total went 1,614 → 1,619: four
`shoalctl` unit tests over the upgrade's health gate, its order with the leader last, its caught-up
check and its activation target, and one over the `cluster upgrade` command line. The whole
workspace run at six threads with `--no-fail-fast` gave **1,590 passed, 7 failed, seven ignored**,
and one binary aborted:
- the seven failures were all `cluster_fixture`, `AddrInUse` on its first fixed ports, which a
  deployed `shoal-tmdb.service` held on the development host (the collision described above);
- `ephemeral_sorted_table` aborted in glibc's thread-cache teardown after one test and passed all
  fifteen alone. That is filed as [item 132](known-issues.md#132-ephemeral_sorted_table-aborted-once-in-glibcs-thread-cache-teardown).

`deploy_smoke.rs` gained an `upgrade --force` and a plain `upgrade` step but no new test.

**[F54](../features/tmdb-dataset-deployment.md) added 3**, and the total went 1,608 → 1,611: a new binary, the
`tmdb-dataset` crate's unit tests (2), over the loader's arguments and a csv row fanned out into
its keyword rows, and the seventh ignored test, the `ignore` doctest in `shoalctl::cli` showing
how a program flattens `Command`. `shoal/examples/tmdb_dataset.rs`, which it replaced, had no
tests. The whole workspace run at six threads with `--no-fail-fast` gave **1,603 passed, 1
failed, seven ignored**: the failure was `scheduled_scrub_quarantines_without_an_operator`,
item 100's shape, which passed alone at once.

**[Resolved #127](resolved/wizard-loopback-address.md) added 3**, and the total went 1,605 → 1,608: three `shoalctl` unit
tests, over a loopback-only name refused before saving, a source file warned about as the server
program, and the probe's resolution telling loopback from no answer.

**[F53](../features/inventory-wizard.md) added 16**, and the total went 1,589 → 1,605:
- thirteen `shoalctl` unit tests: three over node groups and the refused storage directories,
  one over the rendered split, seven over the wizard's form, one over the probe's script and one
  over the rebased server path
- a new binary, `shoalctl/tests/wizard.rs` (2), drawing the review and loading a saved file
- one test in `shoal-bench`'s `deploy_render.rs`, comparing a group's rendered roots with the
  engine's

The whole workspace run at six threads with `--no-fail-fast` gave **1,599 passed, 0 failed, six
ignored**, the fixture binary included.

**[Resolved #27](resolved/shql-quote-escape.md), [#32](resolved/client-gone-broadcast.md),
[#36](resolved/staged-tail-deadline.md) and [#125](resolved/retry-unknown-outcome.md) added 11**,
and the total went 1,576 → 1,587:
- two new binaries, `retry_outcome.rs` (3, against a scripted server) and `staged_flush.rs` (1)
- one test in `shql.rs` and one in `client_disconnect.rs`
- four `shoal-proto` unit tests over the doubled quote, and one `shoal-client` unit test over
  `outcome_unknown` and `settle`

Every test a fix changes failed against the unfixed tree, and each page's **Evidence** has the
output. `refusals_alone_stay_refusals` passed before the fix, as it was written to.
The whole workspace run at six threads with `--no-fail-fast` gave **1,579 passed, 2 failed, six
ignored**. The two failures were fixture tests, and each passed when run alone:
`a_dead_primary_fails_writes_only_until_its_election`, which is item 100's shape, and
`local_rehome_recovers_after_each_crash_point`, which no earlier run had recorded failing under
load.

**[Resolved #122, #123 and #124](resolved/intent-log-failure.md) added 12**, and the total went
1,564 → 1,576:
- a new binary, `intent_log_failure.rs` (4)
- five tests in `resident_reads.rs`: two for the stale read after an eviction
  ([#124](resolved/unsorted-update-generation.md)), and three for a query reusing a parked one's
  id ([#123](resolved/parked-get-key.md))
- three `shoal-core` unit tests: the parked key in `persistent.rs`, `fail_all` in `storage.rs`,
  and the sticky failure in `stream_tests.rs`

Each integration test that a fix changes failed against the unfixed tree. The #122 tests did so
with the old `check_error()?` and `refresh(..)?` put back. The sorted twin of the #124 test
passed before the fix, as it was written to.
Confirmed by the whole workspace run at six threads with `--no-fail-fast`: **1,570 passed,
0 failed, six ignored**. An earlier fail-fast run of the same tree failed
`a_dead_primary_fails_writes_only_until_its_election`, which is item 100's shape.

**[Resolved #16](resolved/hot-path-panics.md) added 9**, and the total went 1,555 → 1,564: the
new `hot_path_failures.rs` (5), two unit tests in `persistent.rs` and two in `partitions.rs`.
Every one a test could drive failed against the unfixed tree - the two write tests and the two
loader tests panicking the shard, the loader pair aborting the binary, and the `resume`
collision panicking in its unit test. `an_unknown_error_code_reads_as_unknown` changed rather
than grew: its probe of an unclaimed code moved from 13 to 14 when `StorageWrite` took 13.
Confirmed by the whole workspace run at six threads with `--no-fail-fast`: **1,554 passed,
4 failed, six ignored** - the probe above, fixed, and three fixture tests
(`a_dead_primary_fails_writes_only_until_its_election`,
`lost_response_retry_returns_original_result`, `scheduled_scrub_quarantines_without_an_operator`)
that each passed alone at once.

**[The remainder of item 15](resolved/backlog-bounds.md) added 6**, and the total went
1,549 → 1,555: the new `table_backlog.rs`, five tests on the `resident_reads.rs` harness, and one
in `backpressure.rs`. All six failed with their bounds switched off. Confirmed by the whole
workspace run at six threads with `--no-fail-fast`: **1,548 passed, 1 failed, six ignored**. The
failure was `migration_resumes_after_each_phase_failure`, which passed alone; an earlier run had
failed `backup_restore_verifies_history_in_new_cluster` and `down_retains_placement_during_grace`
(items 117 and 119), which also passed alone.

**[Resolved #30, 120, 121](resolved/resident-copy-collision.md) added 6**, and the total went
1,543 → 1,549. They are one new binary, `resident_reads.rs`, the first test that builds a
shard's tables without a shard: a glommio executor, the tables built through
`ShoalDatabase::new` over the storage a real server left behind, every partition archived and
evicted, and `request_load`, `handle` and `load_partition` driven directly. Five of the six
failed against the unfixed tree. Confirmed by the whole workspace run at six threads with
`--no-fail-fast`: **1,541 passed, 2 failed, six ignored**. The two were fixture tests,
`mixed_table_bundle_resolves_each_table_policy` and `lost_response_retry_returns_original_result`,
both of which have failed under load before (below) and each passed alone at once; a fixture
suite run on its own failed `local_rehome_recovers_after_each_crash_point` on a crash it waited
for too long, and it passed alone too - item 100's shape. **The `shoal` integration row below
was re-derived from this run rather than incremented**: it read 315 while its own binaries
summed to 334, and is now 340.

**[Resolved #115](resolved/retry-sidecar-crash-window.md) and [Resolved #116](resolved/map-version-test-snapshot.md)
added 2**, and the total went 1,541 → 1,543. They are the two tests F52's run below failed.
`retry_table_survives_a_crash_between_sidecar_and_checkpoint` is new in `cluster_fixture.rs`:
it crashes a node between a staged retry sidecar and its checkpoint file, and it failed three
runs out of three against the unfixed engine. `a_stopped_checkpoint_write_leaves_a_sidecar_for_the_checkpoint_on_disk`
is a new `shoal-core` unit test over `Retries::recover` at each point a checkpoint write can
stop. `lost_response_retry_returns_original_result` now waits for the checkpoint file itself,
and `map_versions_install_atomically_and_resync` for the version the cluster settles on; both
were changed rather than added. Both passed in two fixture suite runs at six threads and in the
whole-workspace run at six threads with `--no-fail-fast`: **1,536 passed, 1 failed, six
ignored**. The fixture suite runs each failed one other test, and the workspace run a third,
none of them these. Each passed alone and is filed:
`single_node_data_has_a_verified_cluster_migration_path` as [item 117](known-issues.md),
`mixed_versions_exchange_real_cluster_operations` as [item 118](known-issues.md) and
`down_retains_placement_during_grace` as [item 119](known-issues.md).

**[F52](../features/cluster-stats.md) added 12**, and the listed total went 1,529 → 1,541:
three `shoal-proto` unit tests over the stats frames, three `shoal-core` unit tests over the
EWMA, the node tracker and a plan's progress, the two fixture tests that count a cluster's
writes and partitions and follow a rebalance, and four `shoalctl` unit tests over the stats
model, the local view, the `admin_reads` gate and the short figures;
`tablet_bytes_follow_the_map` and `admin_bodies_round_trip` were extended rather than added.
Confirmed by the whole workspace run at six threads with `--no-fail-fast`: **1,533 passed, 2
failed, six ignored**. The two were fixture tests - `map_versions_install_atomically_and_resync`,
whose extra topology version was a member the detector called down under the load, and
`lost_response_retry_returns_original_result`, which the F51 run below failed the same way -
and each passed alone at once.

**[F51](../features/cluster-deployment.md) added 16, [Resolved #114](resolved/cluster-tab-voter-count.md)
among them**, and the listed total went to 1,529: twelve `shoalctl` unit tests over the
deployment library and the voter count, `deploy_render.rs` and the opt-in `deploy_smoke.rs` in
`shoal-bench`, and two `ignore`d doctests, the three-line programs on `server::node` and
`shoalctl::cli`. The 1,509 above was counted before the lists were re-taken, and `--list` now
reads 1,529 against 1,525 from those additions; the four between are not ones this change
added, and are left to the next count to place. Confirmed by the whole workspace run at six
threads with `--no-fail-fast`: **1,520 passed, 3 failed, six ignored**. The three were fixture
tests - `canonical_digest_ignores_archive_layout_at_same_boundary`,
`mixed_table_bundle_resolves_each_table_policy`, `node_transfer_budgets_bound_concurrent_sources`
- and each passed alone at once; a run before it failed two others
(`lost_response_retry_returns_original_result`, `repair_is_authorized_versioned_and_resumable_by_id`)
that passed alone too, which is item 100's shape. The live smoke test passed against
hyperion, titan and europa in 144 seconds.

**[Resolved #112](resolved/certificate-test-leader.md) added none**: the certificate test's
identity-mismatch step restarts the misnamed node rather than node zero, so its rejoin no
longer depends on which member leads; eight runs of eight where it had been about half.

**[Resolved #15](resolved/shard-mesh-admission.md) added 2**, and the total went 1,507 → 1,509:
a `shoal` integration test in the new `backpressure.rs`,
`a_query_for_a_shard_that_fell_behind_is_shed` - two shards, a bound of eight, one shard held
for two seconds, two hundred gets over twenty connections, fifty to eighty of them shed
`Shedding` inside two milliseconds, the rest answered, the held shard answering once released,
a shed query retried to success, the transport view counting them - and a `shoal-core` unit
test in `conf.rs` that the bound defaults to sixty-four thousand and reads when named; the
client's retry test now expects `Shedding` to be tried again. Counted from `--list`, and
confirmed by the whole workspace run at six threads that closes the C15 defects' work:
**1,507 passed, 2 failed, four ignored** of the 1,509 listed. The two: the certificate test
of [item 112](resolved/certificate-test-leader.md), since resolved,
and `single_node_data_has_a_verified_cluster_migration_path`, which failed under the suite's
load on a restore finding a table not yet empty and passed alone at once.

**[Resolved #106](resolved/isolated-member-term-inflation.md) added 1**, and the total went
1,506 → 1,507: a `shoal` integration test in `cluster_fixture.rs`,
`a_member_isolated_on_every_lane_heals_without_dying` - node two isolated on every lane for
twelve seconds under writes, healed, alive, serving, its control term unmoved where it had
climbed by fifty-one. Counted from `--list`; the fixture suite ran whole at six threads twice,
the first run finding that [Resolved #109](resolved/volatile-majority-loss.md)'s markers were
scanned on every rebuild and stalled a fresh bootstrap, and that two wire-version tests waited
on a restarted node's own links, both corrected in the same change.

**[Resolved #109](resolved/volatile-majority-loss.md) added 2**, and the total went
1,504 → 1,506: a `shoal` integration test in `cluster_fixture.rs`,
`two_volatile_voters_lost_at_once_do_not_kill_the_survivor` - twenty ephemeral rows, nodes one
and two killed and restarted at once, every volatile group elected again with the survivor's
copies alive, the rows read back and ten more agreed everywhere - and a `shoal-core` unit
test in `shard/groups.rs` over the grant rule case by case. Counted from `--list`; ten of the
fixture's volatile, repair and restart tests were run beside it, one of them
(`returning_node_catches_up_by_log_or_snapshot`) failing once under three threads on timing
and passing five times after.

**[Resolved #103](resolved/returning-leader.md) added 1**, and the total went 1,503 → 1,504: a
`shoal` integration test in `cluster_fixture.rs`,
`a_leader_restarted_inside_its_lease_stalls_no_hop` - node one killed and restarted inside
its lease at a base of one second, a key it led written through node zero every tenth of a
second, every write answered inside a second and a half, the failures `NotLeader`, and the
group's term one election higher afterwards. Counted from `--list`; ten of the fixture's
election and restart tests were run beside it.

**[Resolved #110](resolved/dead-primary-write-failures.md) added 1**, and the total went
1,502 → 1,503: a `shoal` integration test in `cluster_fixture.rs`,
`a_dead_primary_fails_writes_only_until_its_election` - node one killed at a failover base of
one second, a key it led written through node zero without a retry every quarter second,
thirteen refusals over four seconds every one `NotLeader` and immediate, then served, then a
hundred writes with no failure. The fault window test in `harness/fault.rs` now reads the
errors by code too. Counted from `--list`; the bench tests and the new fixture test were run,
and the kill arm was smoke-run once for the codes. Before this item the whole workspace was
run at six threads after [Resolved #43](resolved/marker-every-root.md): **1,502 passed, four
ignored, none failed**, which is the mid-way count of the C15 defects' work and agrees with the
listing.

**[Resolved #43](resolved/marker-every-root.md) added 3**, and the total went 1,499 → 1,502:
two `shoal-core` unit tests in `meta.rs` - a second root taking the primary's marker, keeping
it across a reclaim and a joiner's adoption, and another node's root or the same node at
another slot count refused without a write - and a `shoal` integration test in
`storage_meta.rs`, a table under its own root marked, restarted and read, with a root another
server claimed refused at start. Counted from `--list`; `shoal-core meta`, `storage_meta.rs`
and the four fixture tests that restart a directory were run.

**[Resolved #91, 107](resolved/compaction-retry.md) added 2**, and the total went
1,497 → 1,499: `a_compaction_that_meets_an_unreadable_archive_is_tried_again` in both
`persistent_unsorted_table.rs` and `persistent_sorted_table.rs` - a partition archived, the
archives made unreadable, the partition written to again and the log rotated behind it, the
archives put back, the rotated logs compacted by the retried job and the exit clean; the sorted
twin reads both rows of the partition, which is the merge. Against the unfixed tree the test
never finishes: the compactor dies, the next rotation ends the shard, and the writes hang.
Counted from `--list`; both table binaries were looped at six threads - fifteen and four runs
with no failure once the fork's `statfs` was on the descriptor (item 113), and the two tests
that filed 107 no longer fail at their exit - and `shoal-core storage` was run.

**[Resolved #100](resolved/clone-fencing-under-load.md) added 1**, and the total went
1,496 → 1,497: a `shoal` integration test in `cluster_fixture.rs`,
`a_clone_that_stands_before_it_observes_is_fenced`, a clone whose first observation is held
back past the control group's election timeout so it stands first, fenced all the same.
Counted from `--list`; the fixture suite ran whole at six threads, ninety-seven of ninety-nine
passing, with `repair_is_authorized_versioned_and_resumable_by_id` passing again at two
threads and `certificate_rotation_binds_identity` failing about half its runs alone on this
host on both trees, filed as [item 112](resolved/certificate-test-leader.md) and since resolved;
`node_transfer_budgets_bound_concurrent_sources` allowed one full bucket for three sources
and passed on the margin until this change moved the timing around it, and its bound now
counts a bucket per source that has streamed. The suite was also run
twice at the default thirty-two threads on this host to reproduce the item as filed, and
61 and 82 of 98 failed on glommio's io_uring probe returning null at executor start, which
is the host's limit on a few hundred rings at once and not a defect; that is the reason the
suite stays at six threads now that the item is resolved.

**[Resolved #102](resolved/fixture-port-block.md) added 1**, and the total went 1,495 → 1,496:
a unit test in the fixture's new `cluster/ports.rs`, run as part of the `cluster_fixture.rs`
binary, that every port the fixture hands out sits below the host's ephemeral floor as
`/proc` reports it and above the benchmark harness's ranges. Counted from `--list`; the fixture
suite ran whole at six threads in five minutes, ninety-five of ninety-eight passing and the
three that failed - a scrub, a quarantine and a restore not done in time under the load, none
of them a bind - passing together at two threads afterwards.

**[Resolved #95](resolved/transport-view-every-shard.md) added 1**, and the total went
1,494 → 1,495: a `shoal` integration test in `pool.rs`, `the_transport_view_names_every_shard`,
a two-shard standalone pool whose transport view names shards zero and one in order with no
link on either. Counted from `--list`; `pool.rs`, `shoal-bench` and the two fixture tests that
read the view were run.

**[Resolved #98](resolved/admin-refusal-kinds.md) added 1**, and the total went 1,493 → 1,494:
a `shoal-core` unit test in `control/types.rs`, `a_refusal_names_its_kind`, driving ten
refusals of nine kinds through the state machine and reading each by kind alone, with a
refusal decoded without a kind reading as `Other`. The count is from
`cargo test --workspace -- --list` less the four ignored, which is how the counts on this page
are kept between the whole-workspace runs the C15 defects are being worked under; the
binaries the change touched were run - `shoal-proto`, `shoal-core control`, `shoalctl`, and
the F39 admin test in the fixture, which now asserts `AlreadyInitialized` by code.

**The distributed chapter's rewrite added 1**, and the total went 1,492 → 1,493: a `shoalctl`
unit test over the cluster tab's new `initialize` verb - its members in the order typed, one
line each in the preview, the factor, the once, and the request carrying them in that order
([C14](../distributed/deploying.md)).

**[F50](../features/cluster-operations.md) added 8**, and the total went 1,484 → 1,492 -
across four commits, the last of which carries the docs, so this is the count of all four
together, and the count of a `--workspace --no-fail-fast` run at `--test-threads 6`. Two are
`shoal` integration tests in `cluster_fixture.rs`, the M10c acceptance tests that
[C11](../distributed/testing.md) names - a certificate bound to its node and rotated across a
live cluster with an authority rotated through a bundle, which needs kTLS and skips by name
without it, and a member restarted at another address observed, reached and served with a
clone at the old address refused. One is a `shoal-proto` unit test: the SAN read off the DER,
both ends of a handshake reporting it, and the holder's reload in `shared/tls/tests.rs`. Two
are `shoalctl` unit tests, the crate's first: the cluster model built from the admin frames
and the actions' parses, previews and follow-ups. Two are `shoal-bench` unit tests: the node
environment record and the remote spec with its command lines. One is a `shoal-bench`
integration test, the remote smoke, which runs when `SHOAL_REMOTE_SMOKE` names a host and
says so otherwise. The whole run was taken once at six threads with three fixture tests
failed and retried at two threads - two of them item 100's load, the third a defect this
page found and fixed, a winning clone's shorter log stopping the leader - and the fixture
suite once more on its own with two failed under the load and passed again at two threads.

**[F49](../features/backup-and-recovery.md) added 9**, and the total went 1,475 → 1,484 -
across four commits, the last of which carries the docs, so this is the count of all four
together, and the count of a `--workspace --no-fail-fast` run at `--test-threads 6`. Three are
`shoal` integration tests in `cluster_fixture.rs`, the M10b acceptance tests that
[C11](../distributed/testing.md) names - a backup of three nodes refused until the activation
and then written and verified per group, restored into a fresh cluster with new identities that
answers a remembered identity its original result, refuses the old cluster's token, a second
restore and an old node; a permanent majority loss the survivor refuses to repair by itself
until `force_recover` is run on its stopped directory, after which it leads alone, serves every
acknowledged key and rebuilds every set on two fresh identities; and a standalone node's data
exported and restored into a cluster of three, judged by digest, with the source as the
rollback. Four are `shoal-core` unit tests: the recovery's rewrite over a control store with an
unapplied tail in `control/store.rs`, the restore's coverage rules in `control/backup.rs`, the
backup file's identity in `replication/snapshot.rs`, and the block in `conf/cluster.rs`. Two
are `shoal-bench` unit tests: the backup arm's placement and the record's cut. The whole run
was taken once at six threads with one of F46's fixture tests failed and retried alone, and
the fixture suite once more on its own with four failed under the load and every one passed
again at two threads (item 100).

**[F48](../features/rolling-compatibility.md) added 8**, and the total went 1,467 → 1,475 -
across four commits, the last of which carries the docs, so this is the count of all four
together, and the count of a `--workspace --no-fail-fast` run at `--test-threads 6`. Three are
`shoal` integration tests in `cluster_fixture.rs`, the M10a acceptance tests that
[C11](../distributed/testing.md) names - a mixed cluster of one binary pinned two ways under
forwards, quorum writes, barrier reads, a snapshot over the older link and an election, with
the activation refused naming the pinned members; a rolling upgrade under writers with a kill
inside the mixed window, the activation committed once every member reports the newest and a
pinned restart refused after it, judged by the oracle; and the opt-in run against a real
previous build of the binary, which says so and passes when none is named. Two are
`shoal-proto` unit tests: the negotiation table with the pin and the capability intersection
in `protocol/peer/tests.rs`, and the header's version range in `protocol/tests.rs`. Three are
`shoal-core` unit tests: the manifest's two codecs and the version 2 file header in
`replication/snapshot.rs`, the activation's every refusal and the healed record in
`control/types.rs`, and the pin's bounds in `conf/cluster.rs`; the peer handshake test and
the marker format test grew cases and count the same. The whole run was taken once at six
threads with one of F46's fixture tests retried alone, and the fixture suite once more on its
own with one of F47's retried alone (item 100).

**[F47](../features/local-rehome.md) added 18**, and the total went 1,449 → 1,467 - across
five commits, the last of which carries the docs, so this is the count of all five together,
and the count of a `--workspace --no-fail-fast` run at `--test-threads 6`. Two are `shoal`
integration tests in `cluster_fixture.rs`, the M9c acceptance tests that
[C11](../distributed/testing.md) names - a node of four slots on two cores killed at each of
the six points a cluster node's rehome can die at, alternating one executor and two, with every
key, the remembered identity, the groups' row counts, the reclaim, the hosting and the member
record held after each and the writers' history accepted by the oracle; and a standalone node
grown from two executors to three per tablet and shrunk to one through a crash after the fold
and another after the copy. Fourteen are `shoal-core` unit tests: the pending rehome and the
slots in `server/meta.rs`, the identity hosting, the deal and the file in `server/hosting.rs`,
the plan's order and the resume in `server/rehome/manifest.rs`, the archives step's partial,
the log step's skip and the refusal of another target in `server/rehome/tests.rs`, the ring
from a hosting and the placement over slots in `server/ring.rs` and `server/map.rs`, the
listener's dispatch in `server/peer/tests.rs`, and item 111's reproduction in
`tables/storage/fs/map.rs`; `storage_meta.rs`'s refusal test became the in-process rehome and
counts the same. Two are `shoal-bench` unit tests: the rehome arm's shape and the record's
round trip. The whole run was taken once at six threads, and the fixture suite once more on its
own, 89 passed. The one finding on the way that is a defect was item 111, reproduced first and
fixed in the same change ([Resolved #111](resolved/archive-removal-borrow.md)).

**[F46](../features/capacity-rebalancing.md) added 17**, and the total went 1,432 → 1,449 - across
seven commits, the last of which carries the docs, so this is the count of all seven together,
and the count of a `--workspace --no-fail-fast` run at `--test-threads 6`. Seven are `shoal`
integration tests in `cluster_fixture.rs`, the M9b acceptance tests that
[C11](../distributed/testing.md) names - a dead member's grace expiring into a removal that
moves its sets, tombstones it, refills its seat and refuses its return from its directory and
from a clone; a removal at three of three blocked naming the missing member and completed by
the fourth identity that joins; the grace's count never lower after a control leader restart;
maintenance holding a deadline; weights 3:1:1:1 settling at 3:2:2:2 in one plan and nothing
after; three streams onto one spare taken one at a time under a byte budget, a drain blocked by
the reserve and released; and a live member drained under writers with zero final errors under
the oracle. Eight are `shoal-core` unit tests: the phase machine and the grace's count in
`control/types.rs`; the planner in `control/planner.rs`; the plan record in `control/plan.rs`;
the free bytes override in `control/capacity.rs`; the rate limiter in `replication/network.rs`;
the tablet bytes in `tables/storage/fs/tests.rs`; and the `rebalance:` block in
`conf/cluster.rs`, with the `migration:` block's new fields and the admin round trip extending
tests that existed. Two are `shoal-bench` unit tests: the four arms on the kill arm's placement
and `rebalance_capture_records_plan_and_windows`, the C10 row. The whole run was taken once at
six threads. The one finding on the way that was filed as a defect
([item 110](resolved/dead-primary-write-failures.md), which turned out to be the lease)
was met by the remove arm's smoke run and confirmed against the tree at F45, not by this run.

**[F45](../features/replica-migration.md) added 18**, and the total went 1,414 → 1,432 - across
nine commits, the last of which carries the docs, so this is the count of all nine together,
and the count of a `--workspace --no-fail-fast` run at `--test-threads 6`. Nine are `shoal`
integration tests in `cluster_fixture.rs`, the M9a acceptance tests that
[C11](../distributed/testing.md) names - a write acknowledged after the zero-lag report on the
destination once the source retired, the eighteen-move crash matrix over the driver, the
destination and the control leader judged by the oracle, a caught-up learner never counting
before the uniform commit, a retired copy refusing by name and reclaimed after its grace, the
other tablets' history kept through a retirement, a restart and a compaction, a stale router's
writes terminating without a duplicate, a configuration outliving the stale placement a driver
died under, a retry identity across a checkpoint and a move, and a repair and a move queued
behind each other. Seven are `shoal-core` unit tests: the map's overlay in `map.rs`; the
record's apply and queueing and the two-way serialization in `control/types.rs`; the phases'
order in `control/migrate.rs`; the `migration:` block and the retry window in
`conf/cluster.rs`; the identity window in `replication/machine.rs`; and the forgotten log in
`wal/tests.rs`. Two are `shoal-bench` unit tests: the migration arm on its placement with a
spare and `migration_capture_records_transfer_and_pauses`, the C10 row. The whole run was
taken once at six threads and passed every fixture test, all eighty; one `shoal-bench` test
failed on a milestone cell this page's sibling had annotated on the C pages' acceptance
tables, which the tables' own test refuses, and the cells were restored. The item the run
found on the way ([item 109](resolved/volatile-majority-loss.md), since resolved)
was met by the crash matrix's first shape, not by this run.

**[F44](../features/repair.md) added 16**, and the total went 1,398 → 1,414 - across seven
commits, the last of which carries the docs, so this is the count of all seven together, and
the count of a `--workspace --no-fail-fast` run at `--test-threads 6`. Seven are `shoal`
integration tests in `cluster_fixture.rs`: the six M8 acceptance tests that
[C11](../distributed/testing.md) names - a corrupt primary repaired from a healthy quorum and a
split preserved as evidence, the canonical digest across three archive layouts with the
quarantine, the frame, the release and a restart, a corrupt follower repaired from a verified
source, the seven-point crash matrix of a repair install, the authorized, versioned and
resumable operation, and the scheduled scrub - and the reproduction of
[item 99](resolved/durable-log-reversion.md), written against the unfixed tree first. Seven are
`shoal-core` unit tests: the format 2 record, the format 1 archive and the torn record in
`fs/tests.rs`; the checkpoint and sidecar checksums in `wal/tests.rs`; the canonical fold in
`replication/digest.rs`; the judge in `shard/repair.rs`; and the `repair:` block in
`conf/cluster.rs`. Two are `shoal-bench` unit tests: the background arm on its placement and
`background_capture_records_scrub_interference`, the C10 row. The whole run was taken once at
six threads and failed one fixture test, the authorization one, on a status read through a
follower that had not applied the record yet - a fixture wait made to tolerate that - and the
fixture binary was then run again alone at six threads and passed all seventy-one.

**[F43](../features/node-recovery.md) added 16**, and the total went 1,382 → 1,398 - across eight
commits, the last of which carries the docs, so this is the count of all eight together, and
the count of a `--workspace` run with the fixture binary run at `--test-threads 6`. Ten are
`shoal` integration tests in `cluster_fixture.rs`: the eight M7 acceptance tests that
[C11](../distributed/testing.md) names - a node left behind the purge point and fed a snapshot
per group, the cut's boundary against an oracle of session tokens, the seven-point crash
matrix, a stream cut and a sender killed mid-way, an installing tablet refusing reads, the
retention budget under a cut follower, the grace and the whole-cluster restart - and the
reproductions of [items 104](resolved/segments-recompacted-after-restart.md) and
[105](resolved/volatile-groups-never-purged.md), each written against the unfixed tree first.
Four are `shoal-core` unit tests: the snapshot file round-tripping and refusing a torn or
foreign one in `replication/snapshot.rs`; the assembler's duplicates and resume in
`replication/install.rs`; and, in `wal/tests.rs`, a frame at or below the checkpoint never
handed again and a group's markers surviving the deletion of their segment. Two are
`shoal-bench` unit tests: the catch-up arms on their placement and
`catchup_capture_records_convergence`, the C10 row. The whole run was taken twice: the first
had `SHOAL_CHILD_LOG` set to an empty string, which logs every child at DEBUG into the test's
working directory, and seven fixture tests failed on timing under that load and passed alone;
the second passed every fixture test and failed two of `persistent_unsorted_table.rs`, which
[item 107](resolved/compaction-retry.md) recorded as flaky on the F42 tree too - and which
turned out to be the compactor dying on the tests' own fault, item 91.

**[F42](../features/primary-failover.md) added 21**, and the total went 1,361 → 1,382 - across six
commits, the last of which carries the docs, so this is the count of all six together, and
the count of a `--workspace` run with the fixture binary run at `--test-threads 6` - at the
default thirty-two nineteen of its fifty-four fail under the load
([item 100](resolved/clone-fencing-under-load.md), since resolved),
every one of which passes at six. Thirteen are the M6 acceptance tests in
`cluster_fixture.rs` that [C11](../distributed/testing.md) names, killing, stalling, isolating
and restarting the nodes the earlier milestones only cut. Three are `shoal-core` unit tests:
`retry_table_survives_the_purge_point` in `wal/tests.rs`, which checkpoints, purges, reopens
and retries; the routing rules in `map.rs`; and the detector's
`a_member_silent_before_its_fifth_report_is_suspected`, the reproduction of
[item 101](resolved/short-lived-member-detection.md). One is a `shoal-client` unit test over
the retry loop against the fake server. Four are `shoal-bench` unit tests: the three in the
new `harness/fault.rs` - the C10 row `fault_capture_preserves_outage_time_series` and its two
edge cases - and the failover arm on its placement. The first smoke run of the arm found two
things in the transport rather than the harness - a hop to a dead leader waiting out a dial's
backoff and answered unknown though never written, and the control plane losing its leader
on every member's return - and the three runs are the evidence on the F page.

**[F41](../features/read-consistency.md) added 19**, and the total went 1,342 → 1,361 - across six
commits, the last of which carries the docs, so this is the count of all six together. Three
are `shoal-proto` unit tests: the read options head, sixteen tokens, the refused shapes, the
response token section and the capability bytes in `protocol/tests.rs`; the read plan on a
forward entry and the widened answer head in `protocol/peer/tests.rs`; and the limit pushdown
proved over four hundred random layouts and arrival orders in `responses.rs`. Five are
`shoal-core` unit tests: 3 in `shard/gather.rs` over expiry, late and duplicate shares, a failed
share completing at once, an empty share covering, and a departed client's gathers; 1 in
`control/types.rs` over the table read policy; 1 in `conf.rs` over `query_deadline`'s default
and parsing - and `validation_refuses_what_is_not_built` grew a refusal of `read_consistency:
All`. Three are `shoal-bench` unit tests: the read arms on their placements, the fan-out keys,
and `read_capture_records_barrier_and_application_wait`, the C10 row. One is a `shoal-model`
integration test, `strong_reads_are_linearizable_and_the_cached_leader_knob_is_not`, over the
model's new strong read and its saved schedule. Seven are `shoal` integration tests: the six
M5 tests in `cluster_fixture.rs` that [C11](../distributed/testing.md) names, and
`gather_expiry.rs`, a **new** binary holding the reproduction of
[item 33](resolved/gather-expiry.md), which was run against the tree with the expiry absent
before it was run against the fix. The first smoke run of the arms found two defects in the
harness rather than the server - a seed sent before every peer held the placement, and cluster
port blocks inside the ephemeral range - and `cluster_ports_are_disjoint_from_the_single_node_range_and_bounded`
now holds every block under 32768.

**[F40](../features/replication.md) added 22**, and the total went 1,320 → 1,342 - across five
commits, the last of which carries the docs, so this is the count of all five together. Seven
are `shoal-core` unit tests: 3 in `wal/tests.rs` - the shared WAL and the memory log under
openraft's storage conformance suite, four forced rotations completing every `IOFlushed` once,
and two groups' streams recovering independently with one's completions held and the tail
dropped - 1 in `wal/tests.rs` over the checkpoint file round-tripping a membership keyed by
shard addresses, 1 in `wal/frame.rs` over every frame kind and a torn tail, 1 in `map.rs` over
the placement rule at three and four nodes and unequal shard counts, and 1 in
`conf/cluster.rs` over the `replication:` block. Three are `shoal-proto` unit tests: the
command envelope and the replicate heads in `protocol/peer/replicate.rs`, and shard addresses
and group ids in `identity.rs`. Three are `shoal-bench` unit tests: the three replication arms
on one placement, `infeasible_rf_policy_is_not_a_throughput_arm`, and
`capacity_capture_records_lag_and_offered_load`, the two C10 rows. Nine are `shoal`
integration tests, the M4 acceptance tests in `cluster_fixture.rs`, which
[C11](../distributed/testing.md) names; three of them drove the design rather than confirmed
it - the isolation test that moved writes to the local replica, the first restart test that
found openraft stopping a leader on a volatile follower's empty log, and the segment test that
found a blank entry handed to a compactor - and the first smoke run of the arms found a write
proposed to a group whose handle was still being built, which became a wait rather than a
test. The fixture suite is run at `--test-threads 6` since
[item 100](resolved/clone-fencing-under-load.md), and still is now that the item is resolved:
at thirty-two threads on this host the children die at glommio's io_uring probe.

**[F39](../features/membership.md) added 31**, and the total went 1,289 → 1,320 - across five
commits, the last of which carries the docs, so this is the count of all five together. Fifteen
are `shoal-core` unit tests: 3 in `control/detector.rs` - phi growing with silence, regular
reports fresh and stale ones ignored, and a seeded member's grace period; 6 net in
`control/types.rs` over `ControlState::apply` - a joiner admitted and observing itself up,
incarnations fenced with the highest winning, health and shard reports moving the version once
per change, an initialization versioned once and remembered, the voter policy as a versioned
operation, and a membership entry reflected in the roles, with the F37 bootstrap test rewritten
for the new state; 3 in `map.rs` - write admission following the policy and the up count, a
joiner unplaced until initialized, and the shard's map cell installing only newer maps; 2 in
`meta.rs` - a format 2 marker upgraded on its first rewrite and a joiner adopting its cluster
once; and 1 in `shard.rs`, that queued topology frames fold to the newest while answers keep
their order. Two are `shoal-proto` unit tests: `table_ids_follow_their_names` in `identity.rs`
and the admin bodies' round trip in `protocol/admin.rs`. Fourteen are `shoal` integration
tests: the thirteen M3 acceptance tests in `cluster_fixture.rs`, which
[C11](../distributed/testing.md) names, and
`table_ids_are_stable_across_a_reorder_and_distinct_by_name` in `fingerprint.rs`, which pins
the fixture schema's two ids as literals. The first smoke run of the cluster arms found the
harness reading its record while the leader was still promoting the joiner; that became a
wait in `initialize`, not a test.

**[F38](../features/inter-node-transport.md) added 24**, and the total went 1,265 → 1,289 - across
six commits, the first five of which added tests and no docs, so this is the first count since.
Eight are `shoal-proto` unit tests: the seven in `protocol/peer/tests.rs` over the hello, the
refusal codes, the forward, the forwarded answer, the control heads and the snapshot frames, and
`a_peer_listener_requires_a_certificate_from_the_cluster_authority` in `tls/tests.rs`. Four are
`shoal-core` unit tests: `peer_rejects_wrong_cluster_identity_and_malformed_payload` - a C2 M2
acceptance row that runs a real loopback handshake - two in `ring.rs` over the placement map, and
`extracting_a_context_is_the_inverse_of_adopting_one` in `trace.rs`. Six are `shoal` integration
tests: the other three C2 rows and `control_lane_answers_a_vote_from_a_placed_peer` in
`cluster_fixture.rs`, `client_disconnect.rs` - a **new** binary holding the one test for
[Resolved #94](resolved/disconnected-client-cleanup.md), which was run against the unfixed line
and failed on its eighteenth round - and `the_schema_id_is_the_fingerprint_without_the_version`
in `fingerprint.rs`. Six are `shoal-bench` unit tests: three over the hop arms in
`cluster_hop.rs` and three over the staging in `harness/cluster.rs`. The fourteenth feature-gated
test is `a_stage_report_splits_each_op_by_hop`, which is what found the origin's record of a
forwarded query saying `other/same`.

**[F37](../features/node-identity-control-plane.md) added 27**, and the total went 1,238 →
1,265. Seventeen are `shoal-core` unit tests: 5 more in `meta.rs` over the format 2 marker —
bootstrap idempotence, both mode changes, `verify_cluster` refusing without a write, the one
field that is rewritten and never backwards, and the exclusive directory lock — with the existing
four rewritten for the new signature and the format 1 refusal; 4 in `conf/cluster.rs` over
`DurationSpec`, the C1 defaults and what `validate` refuses by milestone; 4 in `control/cores.rs`
over the affinity; 1 in `control/types.rs` over `ControlState::apply`; and the two conformance
suites plus the crash test in `control/runtime` and `control/store` — three functions that run
ninety of openraft's own tests between them. Three are `shoal-proto` unit tests over `NodeId` and
`ClusterId`. Six are `shoal` integration tests: the five M1 acceptance tests in
`cluster_fixture.rs`, and `partition_keys.rs`, a **new** binary holding the one golden-key test
for [Resolved #65](resolved/gxhash-pin.md) — which found [items 92 and 93](known-issues.md) on
its first expansion. One is a `shoal-bench` unit test holding the cluster overhead arm to its
standalone twin. `shoal-spike` has no tests: it is a measurement, not a claim.

**[F36](../features/cluster-harness.md) added 40**, and two new crates' worth of them: 24
`shoal-model` unit tests and the 4 in its `protocol_model.rs` — the three M0 acceptance tests
over the protocol model and the builder check behind the stale-report schedule — 4 in a new
`shoal` integration binary, `cluster_fixture.rs`, whose two `#[ignore]`d functions are the
**children** the fixture re-executes the binary as and are what moved the ignored count from two
to four; 2 in `pool.rs` over `ShoalPool::ready` and port zero
([Resolved #38, 58, 88](resolved/pool-readiness.md)); 2 `shoal-bench` unit tests, over the
cluster port allocator and `compare` naming a cluster difference; 2 in `committed_artifacts.rs`
holding every workload to its frozen port and every capture to having no cluster record; 1 in
`explore_index.rs` making the three facts mirrors total; and the 1 in a new
`acceptance_tables.rs`, which parses the distributed chapter's tables and requires every test of a
delivered milestone to exist. The sixth of those groups is the one that found something on its
first run: two F34 fields the explorer had never mirrored.

**[F35](../features/wire-trace-context.md) added 11**: 7 `shoal-proto` unit tests over the trace
context codec and the widened preamble, 2 `shoal-core` unit tests over adopting a peer's parent, 1
doctest on `TraceContext::new` — a doctest worth having rather than an example, because what it
demonstrates is the **refusal**: an id of all zeroes builds no context — and one new integration
binary, `trace_propagation.rs`.

**That binary is the one place in this suite where a feature it does not ask for decides whether it
runs at all**, and it is worth understanding before somebody "fixes" it. It carries
`#![cfg(feature = "otel")]`, because `otel` is what puts a trace context on the wire and a client
built without it is joined to nothing on purpose. So `cargo test -p shoal` compiles it away and
reports 214. `cargo test --workspace` runs it and reports 215 — because `shoal-bench`'s `workloads`
feature enables `shoal/otel`, and cargo unifies features across a workspace build. Neither number
is wrong and the difference is not a flake; it is a **feature reaching this binary from another
crate entirely**. A change to `shoal-bench`'s feature list could silently stop this test running,
and nothing would fail.

**[Resolved #89](resolved/fragmented-query-traces.md) and
[Resolved #90](resolved/divergent-layer-filters.md) added 3**, and the total went 1,184 → 1,187.
One is a new integration binary, `tracing_topology.rs`, and it is the second binary here to assert
on the **shape** of what the engine emitted rather than on an answer it gave — `disk_lookups.rs`
was the first, and for the same reason: both paths return the same rows, so nothing about a
response says which one ran. It was written before the fix and reports what the tree did then, root
by root:

```
one query's spans resolved to 3 separate traces:
  root … (Coordinator::handle_client): Coordinator::handle_client, Shard::handle_query, PersistentTable::block_on_load, Shard::handle_released, Shard::reply
  root … (loader::read_partition): loader::read_partition
  root … (Fsloader::spawn_task): Fsloader::spawn_task
```

It reads `tracing`'s own `Attributes` rather than an exporter's output, so it asserts about the
span tree the registry builds and not about one collector's rendering of it — which also means it
needs no OpenTelemetry dependency in `shoal`'s dev-dependencies.

It carries a **second** assertion for a defect that looks identical and is not: that both spans the
fix opens by hand are **entered** somewhere. `tracing-opentelemetry` timestamps a span when it is
exited, so a span that is only ever held as a parent exports with zero duration — the trace joins
correctly and the root draws as a tick. That was reached while building the fix, and the assertion
was checked the same way the first one was: replacing the relay's `span.enter()` with an empty span
fails it with `Coordinator::route was opened and never entered`.

The other two are `shoal-core` unit tests, and neither could be an integration test.
`one_source_decides_what_every_layer_filters_on` covers a private decision function that was split
out of `filter_directives` **so that it reads no environment** — the alternative is a test that
sets `RUST_LOG`, which every other test in that binary shares.
`a_read_links_every_query_but_the_one_that_asked` needs `on_follows_from`, which is the only place
a span link is visible at all; it scopes its own subscriber with `tracing::subscriber::with_default`
rather than installing a global one, which is the pattern
[item 69](known-issues.md#69-shoalctl-and-the-tests-still-install-no-tracing-subscriber) asks
`trace.rs` for and the first test here to use it.

**[F34](../features/benchmark-tracing.md) added 20** before that, and the total went 1,164 → 1,184. Three of
them are the **first tests this repository has over a binary target** — `shoal-workload`'s `main`
had none, because the only thing in it worth asserting used to be argument parsing. What made it
worth testing is `trace_options`, and specifically that it sets `stderr`: the runner harvests the
hotpath profile from the last line the workload writes to **stdout**, so a log line on that stream
lands inside `<label>.hotpath.json` and is found a capture later, reading as code that was never
called. `a_workload_logs_to_stderr` is that assertion and is the reason the function was split out
of `run` at all. `an_unlabelled_run_still_reports_itself` covers the hotpath phase, which is the one
phase that passes no `--label`.

Two are in `compare::macro_layer` and they come in a pair, which is the point.
`a_traced_capture_does_not_compare_to_an_untraced_one` asserts the warning fires;
`matching_trace_facts_are_not_reported` asserts it does not fire otherwise, because a warning that
appears on every ordinary comparison is one nobody reads by the third time. The third leg is
`two_lifted_v1_captures_still_share_their_workload`, which already existed and gained an assertion:
the whole committed corpus records no tracing facts at all, and reading that silence as *untraced*
would have declared it uncomparable with itself.

Three are over `Meters::install`, and all three are about what is **not** installed: that a configuration naming no sink gets no pipeline — the property that keeps a capture whose numbers will be committed from spending its run failing to reach a default endpoint — and that either sink alone is enough, since metrics without spans is a real configuration and requiring a trace backend would make the cheap half depend on the expensive one.

Two more are in `harness::conf` — that the harness no longer forces `tracing.level` to `Warn`, and
that the level and the sink both reach the artifact — and seven are in `shoal-core`, five over the
new configuration (`sample_ratio`, the metrics sink, its derivation from the trace endpoint, and
both `deny_unknown_fields` refusals) and two over `trace.rs` itself. Of those two,
`a_sample_ratio_selects_a_sampler` is the one worth naming: half of it asserts a configured ratio
is used, and the other half asserts that **no** ratio still means the SDK's default — a default that
quietly started sampling would throw spans away for every deployment that has never heard of the
setting.

**The tracing configuration rework added 9**, and the total went 1,155 → 1,164 — eight unit tests
and one doctest, which is the first doctest this repository has added since `Networking::tls`. Five
are over `RemoteTracing` in `server::conf`, and the one that matters is
`the_deprecated_grpc_spelling_still_loads`: the variant was renamed from `Grpc` to `Otlp` because it
never spoke gRPC, and every config written before the rename — including the one checked into this
repository — uses the old word. The test pins that the old spelling still parses **and** that it
widens into the same settings the new one produces, so the compatibility is a property with a test
rather than a promise in a changelog. `a_misspelled_otlp_key_is_rejected` carries the
`deny_unknown_fields` rule down to the new struct, for the same reason item 18 exists: a typo in a
tenant header would otherwise export to the wrong place in silence.

Three are over `TraceGuard` in `server::trace`, and they are the first tests in this tree over a
`Drop` impl's observable effect. `dropping_the_guard_flushes_queued_spans` asserts the premise
before the conclusion — the exporter is empty while the span sits in a batch queue whose timer is
an hour out, and non-empty after the drop — so it fails if the batching it relies on stops holding
the span, rather than passing vacuously against an exporter that was never going to be empty. They
run against a `RecordingExporter` written in the test module rather than the SDK's own
`InMemorySpanExporter`, because enabling that crate's `testing` feature forces a full lockfile
re-resolution.

**[F33](../features/chart-readout.md) added 12**, and the total went 1,143 → 1,155. One of them is
the evidence on [Resolved 86](resolved/hover-label-reads-in-decades.md#evidence) and it is a
reproduction rather than a reading, which is unusual for a defect in a canvas nobody here can draw:
`egui_plot::default_label_formatter` and `HoverPosition` are both public, so the test calls the exact
function the unfixed tree used, with the position the unfixed tree would have handed it, and keeps
the `\nx = 3.000\ny = 5.615` it returned — an empty series name and a logarithm, where the
measurement is 412,500 queries a second. Six more are over `readout`, which is written as pure
functions with the drawing at the end for exactly this reason. Three are over
`Index::captures_answering` on the fixture and two over the committed corpus, and both corpus tests
carry a **non-vacuity assertion**: one fails if every capture turns out to carry a macro layer, the
other if no selection narrows what any capture answers. Without those two the greying could pass
while saying nothing about anything, which is the failure mode of every test written over a property
of a corpus rather than of a function.

**[F32](../features/chart-line-identity.md) added 14**, and the total went 1,129 → 1,143. Two of them
are the evidence on [Resolved 84](resolved/identical-facts-one-line.md#evidence), written against the
unfixed tree, and they answer a question no test over `Source::series` had asked in three features:
what happens when two selected workloads agree in every fact the curve key holds. The answer was
`left: ["one"]` — the same line [Resolved 82](resolved/one-line-per-capture.md#evidence) opens with,
on a smaller set, which is what makes this item that one's remainder rather than a new kind of
defect. `explore_index::two_workloads_the_facts_cannot_tell_apart_are_two_lines` is the corpus half,
and it asserts the *premise* before it asserts the chart: the two keyed gets record identical
`ScaleFactsLite`, and the day somebody gives one of them a distinguishing fact the test stops being
about anything and says so rather than passing vacuously.

Three are the first tests this repository has over a drawing module's **decisions** rather than its
palette. `plot::stroke_of` is the one place the rule *dash means the capture on a sweep, and the mark
carries both channels on a timeline* is written, so it is tested directly — including
`a_timeline_ignores_the_capture_position`, which pins the half of the rule that is a deliberate
absence. That is as close to the pixels as this machine gets: the interface has never been rendered
here, and F29's note about that is still the honest description.

Two more are in `app.rs`, over `Framing`, and they are the closest thing to a test of
[Resolved 85](resolved/chart-framed-for-the-last-selection.md) that exists. The defect is a stale
zoom on a canvas, which cannot be reproduced without a display, so what is tested is the mechanism
that replaces it: `each_input_that_changes_the_chart_is_a_different_framing` walks every field and
asserts each one moved is a different framing **and** that an unchanged one is not — the second half
matters as much, because a framing that always differs is a chart that discards the reader's zoom
every frame. The resolved page says which half is evidence and which is a substitute for it.

**[F31](../features/metric-availability.md) added 9**, and the total went 1,120 → 1,129. One of
them is the evidence on
[Resolved 83](resolved/default-metric-half-the-corpus-cannot-answer.md#evidence), written against
the unfixed tree, and it is the shape of test this module keeps needing: it asks a question of the
**real corpus** that no fixture could have raised, and the answer was `88 workloads are offered on
a metric they carry no value for`. Twenty-two tests already covered `shoal-top` and none of them
had ever asked whether a measurement carried the metric being read off it — the six F29 tests over
series construction assert that an absent measurement stays absent once it is *drawn*, which is the
same property one step too late. Three more run against the corpus:
`every_offered_metric_has_a_measurement_behind_it` walks every measured workload and every metric
the control would offer for it, `the_presets_pick_arms_that_answer_their_own_metric` covers the
opening chart, which is the one nobody chose and so the one failure a reader cannot attribute to
something they did, and `a_metric_list_narrows_to_what_the_whole_selection_answers` checks the
intersection holds on the corpus and not only on the fixture that models it. The five in
`shoal-top` needed the fixture extended for the first time since F30: it gave every point an
`ops_per_sec` and a single operation called `read`, so the two shapes half the real corpus has —
an arm that counted no queries, and an arm that recorded a different operation — could not be
written down at all.

**[F29](../features/benchmark-explorer.md)'s dark theme added 5**, and the total went 1,115 →
1,120. They are the first tests in this repository that touch a drawing module at all, and they
exist because the amendment they cover is otherwise held by nothing: the explorer's UI has never
been rendered on this machine, so a theme that stopped installing would look exactly like a theme
that installed and was ignored. `the_explorer_opens_dark` is the requirement stated as an
assertion — the stock preference is `System`, and the test checks that first, so it is a change
being observed rather than a default being restated. `both_themes_are_registered` catches one
theme installed over the other's values, which is a switch that appears to do nothing.
`the_two_palettes_have_the_same_arity` guards the property that made two palettes safe rather than
confusing: `theme::series` wraps on the palette's own length, so two lengths would mean a curve
resolving to a *different hue* in each theme — pressing the switch would change identity rather
than shade. Since [F32](../features/chart-line-identity.md) the index into them is the **table**
rather than the curve's position, which makes that test stronger: the first four slots are
`index::TABLE_ORDER`'s and have to mean the same table in either theme. They need only an
`egui::Context::default()`, so they run headless.

**[F30](../features/plot-axis-units.md) added 12**, and the total went 1,103 → 1,115. Ten are unit
tests in `shoal-top`, and three of those ten were written **against the unfixed tree and kept the
output** — they are the evidence on
[Resolved 82](resolved/one-line-per-capture.md#evidence), which is a defect the six tests that
already covered that module could not have caught, because none of them had ever asked how many
series a multi-workload selection produces. Two more are the ones worth having for a different
reason: `row_count_does_not_split_a_width_sweep` guards a trap the fix walked into once — a curve
key holding a fact that *co-varies* with the key axis turns a sweep into a scatter of single points
— and `timing_does_not_split_a_throughput_chart` guards the opposite failure, a refusal that is
correct for a percentile being applied to a rate. The last two are in `explore_index.rs`, against
the **real corpus**, and they are what turns F29's claim that the explorer reproduces the book's
charts *arm for arm* into something checked: each applies a preset, joins its arms against
`arms::grid` filtered exactly as the generated page filters it, and asserts the curves come out one
per table.

**[F29](../features/benchmark-explorer.md) added 19**, and the total went 1,084 → 1,103. Nine are in
a new binary, `explore_index.rs`, and they run against the **real corpus** rather than a fixture,
the way `committed_artifacts.rs` does and for the same reason. Three of those nine are the ones
worth having. `the_four_blocks_are_carried_whole` compares each projected block with the
`&'static str` on `FAMILIES`, because the four mandatory blocks are the one thing on a results page
a chart cannot carry and a truncation on the way across would be a silent downgrade.
`formatters_agree` walks every branch of five functions that exist **twice** — `shoal_top::fmt` is a
copy, because the explorer cannot link a crate that pulls `walkdir` — and is the only thing standing
between the two copies and a fork. `the_explorer_crate_is_wired_the_way_the_lockfile_needs` reads
`Cargo.toml` as text, because `default-features = false` on the `shoal-top` dependency is a manifest
property that nothing else in the suite could notice breaking. Six more are unit tests in
`shoal-top` over series construction, where the whole question is whether an absent measurement
stays absent; three are in `shoal-bench`'s server, over the route table and the content types; one
is a doctest.

**[F28](../features/rearchived-rows.md) added 10**, and the total went 1,074 → 1,084. Four are in
`shoal-proto`: two over the generated mirror, which serialize a row out of its own archive and
compare the bytes with what rkyv writes for the owned row, and two over `RowRef`'s archived variant
— including a reply mixing resident and archived rows, which is what an ordinary get across several
partitions is. Two are counts in `shoal-core` asserting that an archived scan **points at** every
row it returns and that a projected one still builds every row; one more is the unsorted twin. The
last four are integration tests over a partition that is genuinely on disk, sorted and unsorted, in
both cases reading the same rows back resident and archived and requiring the two answers to be
identical. The count tests are the ones that matter: both paths answer the same rows, so nothing
else in the suite would notice a get that quietly went back to materializing them.

**[Resolved #80](resolved/never-flushed-partitions.md) added 1**, and the total went 1,073 → 1,074.
It is one test in a new binary, `disk_lookups.rs`, and it is the first test in the suite that
counts what the engine **asked storage** rather than what it answered a client. It has to: the
defect it reproduces changed nothing about any answer — a partition that had only ever been
written to asked its storage engine about an archive that did not exist, on every single get, and
both the wasteful path and the fixed one return the same rows in the same order. The count comes
from the `PersistentTable::block_on_load` tracing span, which is in the shipping code rather than
added for the test, so what it measures is the lookups the engine actually makes. Six gets over
two never-flushed partitions: six lookups before the fix, two after. **That binary holds one test
and must** — the counter and the subscriber that feeds it are process wide.

**[F27](../features/grouped-responses.md) added 13**, and the total went 1,060 → 1,073. Three sit
in `shoal-proto` over `RowRef` — and the third of those is the reason the other two mean anything:
rkyv `memcpy`s a type it can prove has no padding, and a `Vec<T>` reaches that branch while a
`Vec<RowRef<'_, T>>` cannot, so the byte-identity test compares two different writers rather than
one against itself. That is asserted rather than assumed, because if rkyv ever stopped enabling the
optimization the comparison would pass while proving nothing.

Four more are in `shoal-proto` over the group index — including one that merges four shares in six
different arrival orders and checks the result against the hashing implementation it replaced,
which is kept `#[cfg(test)]` for exactly that. Three are a new `grouped_responses.rs`, archiving
every variant of both generated response enums and comparing the bytes. Two are in `shoal-core`
and state [O2](optimizations.md)'s claim as a **count**: a resident unprojected scan of three rows
copies zero of them, and an archived scan of the same three builds all three. One is an
integration test in `persistent_sorted_table.rs`.

**That last one was a test whose own doc comment recorded that it did not do what it was written to
do**, which is unusual enough to say here. It was meant to compare the two reply paths against each
other, and did not, because the sorted table never reached the borrowing one — see
[Resolved #80](resolved/never-flushed-partitions.md), which has since fixed that, so the test now
does what it was written for. It was kept, with the claim corrected, because what it does check was
still worth checking. It was only known to be vacuous because the path was probed rather than
reasoned about — and the probe is what the lookup count in `disk_lookups.rs` replaced.

**[F26](../features/archive-routed-requests.md) added 5**, and the total went 1,055 → 1,060. It
added a sixth that a default run does not reach, taking the `stage-profile` extras from 12 to 13:
`a_parked_get_does_not_count_its_disk_wait_as_execution`, over the stamp a replayed query used to
overwrite. That one was found by reading the change rather than by a failure, which makes it the
only test here written for a defect that had never been captured — the layer it is about only runs
under a feature flag, so nothing would have reported it. They
are all in one new binary, `archive_routing.rs`, and they are all the same kind of test: the
coordinator now routes a bundle without deserializing it, which splits one function into three,
so each of them checks the three against the one they replaced rather than against a hardcoded
answer. `split_by_shard` is kept in the tree for exactly that reason — it is no longer on the live
path and it is the only definition of correct the new path has. Four of the five were confirmed by
breaking the code under them: dropping the `normalized()` call fails two, and routing a write with
keys to narrow to fails a third.

**[F25](../features/read-buffers-are-filled-not-zeroed.md) and
[Resolved #79](resolved/micro-only-capture-current.md) added 8**, and the total went 1,047 → 1,055.
Five are over reads that no longer zero the buffer they are about to fill, and all five are about
the same risk rather than about the removal: a buffer nothing wrote is uninitialized memory, so
every one of them delivers its bytes *in pieces* or stops halfway. `a_body_read_in_chunks_holds_every_byte_it_was_sent`
and `a_payload_that_arrives_in_pieces_is_read_whole` build their payloads out of bytes that are
never zero, so a tail the read missed is visible rather than plausible.

The other three are over `Page::current_for`, and two of them fail against the tree before the fix.
They are the cheapest kind of test for the most expensive kind of defect: `render` chose one
capture for all eleven pages, so a capture of one layer emptied the pages of the others, and
nothing failed — the pages rendered, said nothing had been measured, and were committed in that
state. Reproduced end to end first with `render --current inline-probe`, which is a real committed
micro-only capture and takes `grid.md` from 59,287 bytes to 3,459.

**[Resolved #78](resolved/sources-manifest-drift.md) and [F24](../features/routing-benchmarks.md) added 2**, and the total went 1,045 → 1,047. The first is
is the cheapest test on this page and it is here as an argument about what tests are for:
`every_source_the_manifest_names_exists` walks the paths in `docs/perf/sources.json` and asserts each
one resolves. Six of the seventeen did not — the client and the wire protocol had moved crates in
[F15](../features/client-server-split.md) and the list had not followed — so the micro layer was not
hashing the protocol module `wire.rs` half exists to measure, and eight captures had been reported as
*unaffected* by changes that affected them. Nothing failed, nothing warned, and the table kept
printing plausible verdicts for four months. **A hand-maintained list with no test is a list that is
wrong and cannot say so**, and the check that catches it is nine lines.

The second is the same lesson arriving twice in one change. `BENCH_TARGETS` is a second
hand-maintained list — the one that makes `shoal-bench` discover a criterion bench at all — and
[F24](../features/routing-benchmarks.md)'s new bench reached `shoal/Cargo.toml` and
`docs/perf/sources.json` and not that. `cargo bench` ran it, `list --refresh` reported the same 527
ids as before, and nothing said a bench target was missing, because a shorter list is a valid list.
`every_bench_target_is_declared_and_exists` asserts every `[[bench]]` in `shoal/Cargo.toml` is named
there and that every source named there is a file, and it fails against the tree before the fix.

**[Resolved #76](resolved/stage-join.md) added 6**, and it is the only change here to move the two
counts in opposite proportions: 2 in a default run and 4 behind the feature. The default two are
over `collect::stages::check`, which judged the stage layer on the artifact's summed join and now
judges each report on its own — one of them rebuilds `f22-row-size`'s shape, where 200,000 joins
from one workload carried three zeros past the threshold, and it passes against the tree before the
fix. Three of the feature-gated four are unit tests over the `StageLog` every driver now gathers
through. The fourth is `stage_join.rs`, a new integration binary and **the first test anywhere that
starts a server under `stage-profile` and reads what it wrote**: it runs one grid arm at smoke scale
and asserts the report has a join in it. The eight tests that existed before it are pure functions
over `build_report` fed fabricated halves, which is why all eight passed while three of the layer's
four reports were empty. The total went 1,043 → 1,045, and the feature-gated count 8 → 12.

**[F23](../features/self-sizing-staging-buffer.md) added 7**, and the total went 1,036 → 1,043. Five
are pure functions over `staging_target`, the rule that decides how wide a staging buffer is: that
it batches eight records, that it never drops below the configured floor, that it stops at the
ceiling, that a record wider than the ceiling still gets a buffer of its own — records are never
split, so that is a correctness property no configuration may override — and that a ceiling pinned
to the floor reproduces the old `max(default_buffer_size, size)` at nine widths either side of it,
which is simultaneously the escape hatch and the definition of what changed. The other two are a new
integration binary, `intent_log_batching.rs`, and they are the reproduction: one bundle of 128 rows
of 8 KiB against a 4096 byte buffer, and the shard's intent log read back off disk. It failed against
the tree before the fix with `128 rows were written in 128 flushes, which is one record per write`.
Its control pins the other direction — a ceiling at the floor gives back exactly one write per
record — so the first test measures the sizing and not something else that happens to batch.

**[F22](../features/row-size-benchmarks.md) added 21**, every one of them a `shoal-bench` unit test,
and the total went 1,015 → 1,036. Four over the grid — that the two width arrays do not overlap,
that the two depth ladders cross at exactly one arm rather than minting it twice, that the width
axis is swept at all three mixtures on all four tables, and that the three arms the stage layer
profiles are three arms that exist. Two over the configuration sweep's width repeats: that a repeat
names a declared knob at a width it does not already run, and that it runs every rung its sweep
does. One that reproduces [item 73](known-issues.md) — two stage runs getting two artifacts. And
one that should have existed already: `the_runners_copy_of_the_profiled_workloads_is_current`
asserts the runner's two lists of profiled workloads match what the workloads say, which two doc
comments claimed a test did before one did. Five more over the stage artifact: that a version 1
bare report still reads and is filed under the only workload that could have written one, that a
report naming its own workload keeps that name, that an artifact of several reads as several and
sums their joins, that the report a page draws when it wants one is chosen deterministically, and
that an artifact from a future version is refused rather than rendered. And three over the
configuration page's reading order, which reproduce and pin
[item 74](resolved/conf-knob-dropped.md): that a knob the order does not name reaches the
recommendation table anyway, that a *section* still takes only its own half, and that a sweep
repeated at another row width sorts beside the sweep it repeats. And four over the stage
collector: that several reports fold into one artifact keyed by the workload each names, that an
unnamed report is refused rather than filed under a guess, that two reports claiming one workload
are refused, and that a file the plan named and no run wrote is an error rather than a shorter
artifact — which is what stops the never-cleared scratch directory leaking a previous capture's
reports into this one's. And one that guards something the identifiers had only been getting
right by luck: `no_two_workloads_share_a_slug` asserts no two of the 374 flatten to the same file
name, which is what names both a workload's scratch results and its storage directory —
`macro/grid/depth/1/512`, new with this feature, is one character from colliding with
`macro/grid/depth/128`.

**The one that carries the feature is `grid::the_width_axis_is_swept_at_every_declared_mixture`.**
The whole point of the `r0` and `r100` sweeps is that a width effect can be attributed to the write
path by subtracting one from the other; a sweep short of a width or short of a table is a pair that
cannot be subtracted, and the arms would still be minted, still run, and still render. Counting per
mixture per table is what catches it.

**[F20](../features/configuration-sweeps.md) and [F21](../features/benchmark-groups.md) added 29**,
every one of them in `shoal-bench`, and the split says what each feature rests on. Twenty-six unit
tests: nine over the configuration sweep, eleven over the group table, three in the registry over
`--group` intersecting rather than replacing, one over seed bundles fitting inside a *narrowed*
frame, and two over parsing a swept value back into the number behind it. Plus three doctests. The
total went 986 → 1,015.

**The one that carries the feature is `conf_sweep::an_arm_moves_one_field`.** Everything on
[Configuration and what each setting is worth](../performance/configuration.md) rests on an arm
differing from the base in exactly one field, because that is what makes the gap between two arms
attributable to a setting; the test counts the moved fields with one term per field, so a field
added to `ConfOverrides` without a term is a visible omission in the diff rather than a check that
silently stops covering it. `every_sweep_covers_the_shipped_default` is the second: it resolves the
committed `shoal.yml` and asserts each sweep contains the value in use, so retuning that file fails
a test instead of leaving the page recommending against a reference that is not on the chart.

**`groups::the_conf_halves_partition_the_sweep` is a counting test that earns its place.** It
asserts every configuration arm is in exactly one of `conf/storage` and `conf/resources` — not
neither, not both — which is what makes two half-captures add up to the whole one, and it is the
kind of thing that goes wrong silently when a prefix is added.

**[F19](../features/chart-legends.md) added 19.** Sixteen unit tests — seven over the shared
legend's layout, four over the sweep's canvas and its data-derived ticks, three over the encryption
charts now being drawn in nanoseconds and covering every depth, and two over the axis byte
formatter — plus two integration tests in `chart_geometry` and one doctest. The total went
967 → 986. **The two integration tests are the ones that matter**, because the fourteen unit tests
draw charts and assert about strings, while `every_series_is_named_once` and
`legend_names_stay_with_their_swatches` assert about *geometry* over the widest legend the renderer
can be asked for — which is the only place the text-width estimate is checked at all. Nothing
measures text here, so nothing else can.

**[F17](../features/workload-grid.md) and [F18](../features/results-pages.md) added 71**, every one
of them in `shoal-bench`. Sixty unit tests net: sixty-three added — the grid's fourteen, the
row-width and key distribution generators' eleven, the two new chart kinds' fourteen, the family and
page registries' nineteen, and the arm selector's five — less the three that replaced six in
`page.rs`, whose section builders moved out to the ten page modules. Two integration tests over the committed corpus, both of which
exist to protect it rather than to test new code — one asserts the four new `ScaleFacts` fields stay
skipped when absent, because if one stops being skipped every existing workload re-serializes with
new nulls and the whole corpus churns on a change that measured nothing. And nine doctests. The
total went 896 → 967.

**The two that matter most are registry tests rather than behaviour tests.**
`family::every_workload_has_a_family` and `pages::every_page_renders_with_nothing_captured` are what
stop a new sweep landing on the site as an unexplained chart, and what stops a clean checkout
failing to build the book. Neither tests what any code computes; both test that a thing was not
forgotten, which is the failure mode the page they replace actually had.

**[F16](../features/client-builder.md) added 20.** Twelve `shoal-client` unit tests over
`PoolConfig`'s defaults, the endpoint resolver and `endpoint_order`; a new `pool.rs` integration
binary carrying six; and two doctests. The total went 876 → 896. **Six of the twelve unit tests
exist because the integration test was not enough** — `an_endpoint_that_is_down_is_tried_past`
passes against a build with the failover loop stubbed out, since `bb8`'s retries and the round
robin counter reach a live endpoint on their own, so what the loop actually buys is pinned over
`endpoint_order` directly. That is the general lesson: an end-to-end test that passes either way is
not a test of the mechanism.

**[F15](../features/client-server-split.md) moved where they live without changing what they
cover.** Splitting `shoal-core` into three crates re-attributed 177 unit tests and added 8. The
total went 868 → 876; nothing was lost, and a reader seeing `shoal-core` drop by more than half
should read this table rather than assume it was.

**Every number below was re-measured**, package by package, rather than incremented. Four of them
were stale — `shoal-core`, both `shoal-bench` rows and the doctests — and one whole row was
missing, which is why the table summed to 1,084 while the header said 1,155. ~~It sums to 1,187
now.~~ ~~It sums to 1,198 now~~ ~~It sums to 1,238 now~~ ~~It sums to 1,289 now~~ ~~It sums to 1,320 now~~ It sums to **1,342** now, against a `--workspace` run — which is
the run the `shoal` integration row is counted from, and the only one that builds
`trace_propagation.rs`.

| Where | Tests | |
| --- | --- | --- |
| `shoal-proto` unit | ~~214~~ 218 | up 4 with [Resolved #27](resolved/shql-quote-escape.md): a doubled quote in a literal, the raw span across one, a literal ending on one, and the completion tokenizer reading one as part of its literal; up 3 with [F52](../features/cluster-stats.md): the stats frames' deltas, older shapes and totals; up 1 with [F50](../features/cluster-operations.md): the certificate's node and the holder's reload; up 2 with [F48](../features/rolling-compatibility.md): the negotiation and the header range; the protocol, the SHQL parser, SCRAM, the TLS config — moved out of `shoal-core`. Up 3 with [F41](../features/read-consistency.md) over the read options and token sections, the read plan on a forward entry with the widened answer head, and the limit pushdown proof; 3 with [F40](../features/replication.md) over the command envelope, the replicate heads, shard addresses and group ids; 2 with [F39](../features/membership.md) over `TableId` and the admin bodies; 8 with [F38](../features/inter-node-transport.md) over the peer frames and the peer TLS config; 3 with [F37](../features/node-identity-control-plane.md) over `NodeId` and `ClusterId`; 4 with [F28](../features/rearchived-rows.md), over the mirror that writes an archived value back into its own layout and over `RowRef`'s archived variant; and 7 with [F35](../features/wire-trace-context.md), over the trace context codec and the request preamble that carries it — a round trip, the two refusals (an id that names no parent, a version this build does not write), the traced preamble's flag and length accounting, the untraced one being **byte identical** to what it always was, a frame too short for the context it claims, and the flag staying on bit 4 |
| `shoal-core` unit | ~~284~~ ~~288~~ ~~291~~ 294 | 294 measured by the workspace run for [Resolved #128](resolved/hop-deadline-margin.md), which added 1, `a_hop_leaves_the_leader_less_than_it_waits`; the other two since 291 are [Resolved #126](resolved/storage-directory-unusable.md)'s two `meta` tests, which this row had not counted; up 3 with [Resolved #122, #123](resolved/intent-log-failure.md): the parked key, `fail_all` and the sticky writer failure; up 4 with [Resolved #16](resolved/hot-path-panics.md): a colliding get refused with the parked one kept, a failed partition filling its slot, `restore` undoing `remove`, and the split update matching the whole one; up 1 with [Resolved #115](resolved/retry-sidecar-crash-window.md), a checkpoint write stopped at each of its points; up 3 with [F52](../features/cluster-stats.md): the debiased EWMA, the node tracker's rates and a plan's progress; up 1 with [Resolved #15](resolved/shard-mesh-admission.md), the admission bound's default; up 1 with [Resolved #109](resolved/volatile-majority-loss.md), the empty copy's grant rule; up 2 with [Resolved #43](resolved/marker-every-root.md): a second root mirrored and one refused; up 1 with [Resolved #98](resolved/admin-refusal-kinds.md): every refusal kind read by kind; up 4 with [F49](../features/backup-and-recovery.md): the recovery, the coverage rules, the file's identity and the backup block; up 3 with [F48](../features/rolling-compatibility.md): the manifest codecs, the activation and the pin; up 14 with [F47](../features/local-rehome.md): the pending rehome, the slots, the hosting, the manifest, the three step redos, the two rings over a hosting, the listener's dispatch and item 111's reproduction; up 8 with [F46](../features/capacity-rebalancing.md): the phase machine, the grace's count, the planner, the plan record, the free bytes override, the rate limiter, the tablet bytes and the `rebalance:` block; up 7 with [F45](../features/replica-migration.md): the map's configuration overlay and learner spec, the move record's apply and queueing, the two-way serialization with a repair, the phases' order, the `migration:` block with the retry window, the identity window and eviction watermark, and the forgotten log; up 7 with [F44](../features/repair.md): the archive record, the format 1 archive, the torn record, the checkpoint and sidecar checksums, the canonical fold, the judge and the `repair:` block; up 4 with [F43](../features/node-recovery.md): the snapshot file, the assembler, the checkpoint filter and the marker carry; up 3 with [F42](../features/primary-failover.md): the retry sidecar, the health routing and the detector's fix; the engine: partitions, storage, the shard. Was 323 before the split. Up 5 with [F41](../features/read-consistency.md) — the gather map's expiry, identity and completion rules, the table read policy, and the query deadline; 7 with [F40](../features/replication.md) — the shared WAL under openraft's storage suite, its rotation and its independent streams, the checkpoint file, the frame codec, the placement rule and the `replication:` block; 15 with [F39](../features/membership.md) — the phi-accrual detector, every rule of `ControlState::apply`, the tablet map and the shard's map cell, the format 3 marker, and the topology fold; 4 with [F38](../features/inter-node-transport.md) — the malformed-peer acceptance test, the placement map, the trace context's inverse; 17 with [F37](../features/node-identity-control-plane.md) — the format 2 marker, the `cluster:` block, the control core, the control state, and the three functions that run openraft's runtime suite, its storage suite and a torn append; 5 with [F23](../features/self-sizing-staging-buffer.md), all of them over the staging buffer's sizing rule, and 3 with [F25](../features/read-buffers-are-filled-not-zeroed.md) over `RequestBody` — a body delivered in pieces, a stream that ends early, and an empty one; and 2 with [F28](../features/rearchived-rows.md), which are copy counts over an archived scan and its projected control — one of them replacing the F27 test that asserted the opposite; and 8 with the tracing configuration rework — 5 over `RemoteTracing`, including the one that pins the deprecated `Grpc:` spelling still parsing, and 3 over `TraceGuard`'s flush-on-drop; and 7 with [F34](../features/benchmark-tracing.md) — 5 more over the configuration (`sample_ratio`, the metrics sink and its derivation from the trace endpoint, and both `deny_unknown_fields` refusals) and 2 over `trace.rs`, one asserting that resource attributes reach the exporter and one that an **absent** `sample_ratio` still means every trace; and 2 with [Resolved #89](resolved/fragmented-query-traces.md) and [Resolved #90](resolved/divergent-layer-filters.md) — that one decision names what every layer of the subscriber filters on, and that a partition read links every query it released except the one that asked for it; and 2 with [F35](../features/wire-trace-context.md) over `adopt_remote_parent` — that a span handed a peer's context resolves into the peer's trace, and that an unsampled peer does not have its trace sampled for it on this side |
| `shoal-top` unit | 49 | the explorer's portable index and its drawing decisions ([F29](../features/benchmark-explorer.md)) — **missing from this table until now**, which is most of why it did not sum to the figure above it |
| `shoal-workload` bin | 3 | **new** with [F34](../features/benchmark-tracing.md), and the first tests here over a **binary target**: that the workload's console layer writes to stderr rather than into the hotpath artifact, that every span it emits names the workload, the capture and the scale, and that a run given no `--label` — which is every run of the hotpath phase — still reports itself |
| `shoal-client` unit | ~~21~~ 22 | up 1 with [Resolved #125](resolved/retry-unknown-outcome.md), which failures are remembered as unknown and how a refusal after one is reported; up 1 with [F42](../features/primary-failover.md), the retry loop; the client read loop and its error routing, and — new with [F16](../features/client-builder.md) — the builder, the pool defaults and the endpoint order; and — new with [F25](../features/read-buffers-are-filled-not-zeroed.md) — a response payload arriving in pieces and a connection that closes halfway through one |
| `shoal-bench` unit | 447 | up 2 with [F50](../features/cluster-operations.md): the node environment record and the remote spec; up 2 with [F49](../features/backup-and-recovery.md): the backup arm on the kill arm's placement and its record cut at its marks; up 2 with [F47](../features/local-rehome.md): the rehome arm restarting at fewer shards and the record's round trip; up 2 with [F46](../features/capacity-rebalancing.md): the four rebalance arms on the kill arm's placement and the plan's record cut at its marks; up 2 with [F45](../features/replica-migration.md): the migration arm on its placement with a spare, and the move's record cut at its marks; up 2 with [F44](../features/repair.md), the background arm and the background record's cut; up 2 with [F43](../features/node-recovery.md), the catch-up arms and the catch-up record's cut; up 4 with [F42](../features/primary-failover.md), the fault record's cut and the failover arm; the harness, the workloads, the charts — **442 with `--features stage-profile`** — up 3 with [F41](../features/read-consistency.md): the read arms on their placements, the fan-out keys, and the read record; 3 with [F40](../features/replication.md): the replication arms on one placement, the infeasible factor refused, and the capacity record; 6 with [F38](../features/inter-node-transport.md) over the hop arms and the cluster staging, 1 with [F37](../features/node-identity-control-plane.md) over the cluster overhead arm, and — new with [F17](../features/workload-grid.md) and [F18](../features/results-pages.md) — the grid, the row-width and key generators, the family and page registries, and the two new chart kinds; and — new with [F19](../features/chart-legends.md) — the shared legend, the data-derived axis ticks, and the encryption charts in nanoseconds; and — new with [F20](../features/configuration-sweeps.md) and [F21](../features/benchmark-groups.md) — the configuration sweep, the group table, and `--group` in the registry; and — new with [F22](../features/row-size-benchmarks.md) — the three width passes, the configuration sweep's width repeats, both runner-side lists of profiled workloads, and the per-workload stage artifact; and — new with [Resolved #76](resolved/stage-join.md) — that the stage layer's collector judges each report rather than their sum; and — new with [Resolved #79](resolved/micro-only-capture-current.md) — that each page resolves the current capture of the layer it draws. and — new with [F34](../features/benchmark-tracing.md) — that the harness no longer forces `tracing.level`, that the level and the sink both reach the artifact, the pair in `compare` that a capture traced differently is named while one traced the same is not, and three over `Meters::install` asserting what it declines to install; and — new with [F36](../features/cluster-harness.md) — that a cluster arm's ports never meet the single-node range or wrap, and that `compare` names a capture with a cluster record against one without rather than comparing them. ~~**428 with `--features stage-profile`**~~ (436 now, see the start of the row), which adds the 9 over the report builder — one of them new with [F26](../features/archive-routed-requests.md), over a parked get's stages — and 3 over the `StageLog` |
| `shoal` integration | ~~315~~ ~~340~~ ~~346~~ ~~351~~ ~~360~~ 366 | ~~24~~ ~~25~~ ~~26~~ ~~27~~ ~~28~~ 30 binaries, three ignored — **365 under `cargo test -p shoal`** — up 6 with [Resolved #27, #32, #36 and #125](resolved/shql-quote-escape.md): the new `retry_outcome.rs` (3) and `staged_flush.rs` (1), one in `shql.rs` and one in `client_disconnect.rs`; up 9 with [Resolved #122, #123, #124](resolved/intent-log-failure.md): the new `intent_log_failure.rs` (4) and five in `resident_reads.rs`; up 5 with [Resolved #16](resolved/hot-path-panics.md): the new `hot_path_failures.rs`; up 6 with [the remainder of item 15](resolved/backlog-bounds.md): the new `table_backlog.rs` and one in `backpressure.rs`; up 6 with [Resolved #30, 120, 121](resolved/resident-copy-collision.md): the new `resident_reads.rs`, and re-derived from the run rather than incremented, having read 315 against the 334 its binaries summed to; up 1 with [Resolved #115](resolved/retry-sidecar-crash-window.md): the sidecar crash test in `cluster_fixture.rs`; up 2 with [F52](../features/cluster-stats.md): the stats tests in `cluster_fixture.rs`; up 2 with [F50](../features/cluster-operations.md): the M10c acceptance tests in `cluster_fixture.rs`; up 3 with [F49](../features/backup-and-recovery.md): the M10b acceptance tests in `cluster_fixture.rs`; up 3 with [F48](../features/rolling-compatibility.md): the M10a acceptance tests in `cluster_fixture.rs`; up 2 with [F47](../features/local-rehome.md): the M9c acceptance tests in `cluster_fixture.rs`; up 9 with [F45](../features/replica-migration.md): the M9a acceptance tests in `cluster_fixture.rs`; up 7 with [F44](../features/repair.md): the six M8 acceptance tests and item 99's reproduction in `cluster_fixture.rs`; up 10 with [F43](../features/node-recovery.md): the M7 acceptance tests and the two reproductions in `cluster_fixture.rs`; up 13 with [F42](../features/primary-failover.md): the M6 acceptance tests in `cluster_fixture.rs`; 7 with [F41](../features/read-consistency.md): the six M5 acceptance tests in `cluster_fixture.rs` and `gather_expiry.rs`, a **new** binary holding item 33's reproduction; 9 with [F40](../features/replication.md): the M4 acceptance tests in `cluster_fixture.rs`; 14 with [F39](../features/membership.md): the thirteen M3 acceptance tests in `cluster_fixture.rs` and the table-id literals in `fingerprint.rs`; 6 with [F38](../features/inter-node-transport.md): four in `cluster_fixture.rs`, one in `fingerprint.rs` and the new `client_disconnect.rs`; the one short of the `--workspace` count is because one of them is `trace_propagation.rs` and it is gated on `otel`, which that invocation does not enable and a `--workspace` run does, through `shoal-bench`. It is **new** with [F35](../features/wire-trace-context.md) and holds exactly one test for the same reason the two below it do. `tracing_topology.rs` is **new** with [Resolved #89](resolved/fragmented-query-traces.md) and holds exactly one test, for the same reason `disk_lookups.rs` does: the subscriber that feeds it is process wide. `disk_lookups.rs` is **new** with [Resolved #80](resolved/never-flushed-partitions.md) and holds exactly one test, because what it asserts is a process-wide count. Up 4 with [F28](../features/rearchived-rows.md), which reads a partition that is genuinely on disk back both ways in both table suites. `grouped_responses.rs` is **new** with [F27](../features/grouped-responses.md) and starts no server, for the same reason `archive_routing.rs` does not: it asserts what the derive generates and what rkyv does with it. `cluster_fixture.rs` is **new** with [F36](../features/cluster-harness.md) and holds the three M0 fixture tests plus two ignored functions that are not tests at all but the children it re-executes the binary as, and up 5 with [F37](../features/node-identity-control-plane.md), the M1 acceptance tests; `partition_keys.rs` is **new** with F37 and holds the one golden-key test of [Resolved #65](resolved/gxhash-pin.md); `pool.rs` is **new** with [F16](../features/client-builder.md) and up 2 with F36, `intent_log_batching.rs` with [F23](../features/self-sizing-staging-buffer.md), and `archive_routing.rs` with [F26](../features/archive-routed-requests.md). All but the last run against a live server; `archive_routing.rs` starts nothing, because `Ring`, the routing traits and rkyv are pure CPU over plain data — the same property `shoal/benches/routing.rs` relies on |
| `shoalctl` unit | ~~19~~ ~~32~~ ~~35~~ 40 | up 5 with [F55](../features/cluster-upgrade.md): the upgrade's health gate, its order, its caught-up check, its activation target and its command line; up 3 with [item 127](resolved/wizard-loopback-address.md): a loopback-only name refused before saving, a source file as the server program, and the probe's resolution; up 13 with [F53](../features/inventory-wizard.md): groups and per-field storage, whole resources from a group, the refused storage directories, the split rendered file, the wizard's round trip, typing, issue placement, group deletion, confirmations and placeholders, the source names, the probe's script and the rebased server path; up 4 with [F52](../features/cluster-stats.md): the stats model, the local view, the `admin_reads` gate and the short figures; up 12 with [F51](../features/cluster-deployment.md): the inventory's defaults, refusals and resolution, the state's privacy, the leaf under a rebuilt authority, the rendered file, the unit, the ssh command lines and output, the readiness wait and the seeds, and [item 114](resolved/cluster-tab-voter-count.md)'s voter count; up 1 with the distributed chapter's rewrite: the `initialize` verb's order, preview and request; new with [F50](../features/cluster-operations.md): the cluster tab's model and its actions |
| `tmdb-dataset` unit | ~~2~~ 3 | up 1 with [Resolved #128](resolved/hop-deadline-margin.md) and F54's retry: a transient failure retried after a capped backoff, and the new defaults; new with [F54](../features/tmdb-dataset-deployment.md): a load names one target and a pipeline that flows, and a csv row becomes a movie and its keyword rows in title-then-id order |
| `shoalctl` integration | ~~34~~ 36 | the completion menu, driven the way the key handler does; and since [F53](../features/inventory-wizard.md) `wizard.rs` (2): the review drawn on a `TestBackend` and a saved inventory loaded back |
| `shoal-client-check` integration | 7 | **new.** A schema compiling and running against the client alone |
| `shoal-model` unit | 24 | **new** with [F36](../features/cluster-harness.md): the reference vectors of the seeded generator; the sequential semantics of the four mutations and an identity applied once; every unsafe knob deviating in exactly its own name; a three-voter election, replication and commit, a conflicting suffix truncated, a heartbeat re-ack that is a watermark, a retry returning the stored result, a crash keeping only stable storage; the safe observer never promoting and the unsafe one promoting the highest report; Raft's figure 8 against the checker's ground truth; the same seed generating the same schedule and a schedule surviving JSON; a padded failure shrinking to its core; and the oracle judging keys apart and refusing too many |
| `shoal-model` integration | 5 | up 1 with [F41](../features/read-consistency.md), the strong read under the safe rule and the cached-leader knob on its saved schedule; **new** with [F36](../features/cluster-harness.md): the three M0 acceptance tests over the protocol model — the contract holding across thirty-two seeded schedules with everything shown to have happened and each unsafe knob caught by its saved schedule, a fresh failure minimizing to a reproducible core with every saved file replaying to what it records, and the oracle's seven histories — plus the builder producing the C7 stale-report schedule at any prefix |
| `shoal-bench` integration | ~~46~~ 47 | up 1 with [F53](../features/inventory-wizard.md): `deploy_render.rs` renders a group's split roots and compares them with the engine's; up 2 with [F51](../features/cluster-deployment.md): `deploy_render.rs`, which parses a rendered `shoal.yml` as a `Conf`, claims it, starts it and initializes it through its admin - the second test here that starts a server, and the first in a default run - and `deploy_smoke.rs`, opt-in by `SHOAL_DEPLOY_INVENTORY`; up 1 with [F50](../features/cluster-operations.md): the remote smoke, opt-in by `SHOAL_REMOTE_SMOKE`; committed artifacts, chart geometry, CSS sync, the explorer's index, and — new with [F36](../features/cluster-harness.md) — the acceptance tables of the distributed chapter. Up 2 with [F17](../features/workload-grid.md), both guarding the committed corpus against the four fields it added, and 2 more with [F19](../features/chart-legends.md) over the legend's layout. **26 with `--features stage-profile`**, which adds `stage_join.rs` — the only test here that starts a server ([Resolved #76](resolved/stage-join.md)) |
| doctests | 51 | up 9 with [F17](../features/workload-grid.md): the row profile's five, `Seeded::at`, `queries_for`, and the two byte formatters; one with [F19](../features/chart-legends.md) over the third; 3 with [F21](../features/benchmark-groups.md) and [F20](../features/configuration-sweeps.md) over `human_duration`, `numeric` and the page's list formatter; and one with the tracing configuration rework over `OtlpTracing::new`; and 3 with [F34](../features/benchmark-tracing.md) over `OtlpMetrics::new`, `Tracing::metrics_sink` and `TraceOptions::new` |

The 8 added are the 7 in `shoal-client-check` and one in `hotpath_scopes`
(`a_scope_from_any_crate_loses_its_prefix`). The `chart_geometry` count did not move, but
`stacked_labels_have_room` began failing on real data and now passes for a reason rather than by
luck ([items 67, 68](resolved/chart-labels.md)).

Before the split it was **498 integration tests** (one ignored), **323 `shoal-core` unit
tests**, **29 doctests**.

**The encryption sweeps added 15 more** `shoal-bench` unit tests after that — 9 over the forty-eight
sweep arms (that every point is minted once, that a TLS arm differs from its twin in the wire alone,
that each sweep varies one thing, and that the byte budget keeps any one width from dominating a
capture) and 6 over the chart that pairs them, including that a pair whose runs overlapped is not
called a result and that the two sweeps cannot pair with each other. The integration and
`shoal-core` counts did not move: this is measurement, and no engine code changed.

That is up from 490, 301, and 25 with [F14](../features/encryption-in-transit.md) — one new
integration binary over encryption carrying 8 tests, 17 unit tests over what a TLS session is
loaded from and allowed to negotiate and over the key material handed to the kernel, 5 over the
config section, and 4 new doctests (`TlsClientOptions::new`, `Networking::tls`,
`ClientOptions::tls`, and `Shoal::with_options`, the last `no_run` because it needs a server).
5 more workload unit tests landed in `shoal-bench` over the encrypted transport arms.

**Nine of these need the `tls` kernel module and skip loudly without it**, the same way the thirteen
`stage-profile` tests sit outside a default run: the 8 in `tls.rs` and
`a_socket_reports_the_tls_ulp_once_it_is_attached`. That last one is the only assertion anywhere
that can tell a kTLS socket from a plaintext one, which makes it the one that would catch the
feature being quietly replaced by a userspace implementation — every other test in the file passes
either way, and the file says so.

Before that it was 482, 301, and 24 with [F13](../features/transport-workloads.md) — 8 unit tests in
`shoal-bench` over the eight transport workloads, and one doctest over `seed_batch`. The unit tests
are all about the axes rather than the driving: that every mode is minted at both row sizes, that
the two sizes of one mode differ in the row width and in nothing else, that a seed bundle fits in a
frame and is never empty, that the large arm's outstanding responses stay bounded in bytes, and
that only the single-send mode reports a service time. **The `shoal-core` unit count did not move,
because no engine code changed** — this is a measurement feature. The four driving paths are
covered by running them, not by a unit test: there is no way to assert a transport mode works
without a server, and a smoke capture exercises all eight.

Before that it was 473, 272, and 21 with [F12](../features/authentication.md) — one new integration
binary over authentication carrying 9 tests, 17 unit tests over the mechanism and its credential
store, 8 over the auth frame codec and the two handshake fields it added, and 4 over the config
section. The three new doctests are the first movement in that number since
[F10](../features/framing-and-protocol-evolution.md): `Credentials::scram`,
`StoredCredential::from_password` and `Shoal::with_credentials` each carry an example, and the
third is `no_run` because it needs a server. **No existing test changed what it asserts** — four
had to name a new struct field, which is a different thing.

Before that it was 470, 258, and 21 with [F11](../features/error-channel.md) — one new integration
binary over the error channel, 7 unit tests over the error frame codec and its pinned codes, 3 over
the response payload's precedence rules, and 4 over the client's frame dispatch and its dead
connection sweep. Four existing integration tests changed what they assert rather than being added
to: the two per table that used to pin "a read that failed is reported the same way an empty
partition is" now pin the two failure classes arriving as distinct codes. The doctest count did not
move.

Before that it was 456, 231, and 21 with [F10](../features/framing-and-protocol-evolution.md) — three
new integration binaries over the framing, the handshake and the fingerprint, 23 unit tests over
the frame codec and the fingerprint's mixing function, 2 over the client's read path including the
alignment guard, and 2 over the frame bound's config default. The doctest count did not move.

**Re-run and re-counted in August 2026** ([Review](review-2026-08.md)), binary by binary, and every
number on this page was already right: 455 integration tests passing plus the one ignored, 231 unit
tests, and 21 doctests split 10 in `shoal-bench`, 9 in `shoal-core` and 2 in `shoalctl`. That is
worth recording rather than assuming, because it is the one part of this documentation that is
cheap to verify and expensive to trust wrongly — the counts are what every other page's "up from"
chain hangs off. What the same run *did* change is the port table at the bottom of this page, which
described two colliding test binaries and now describes five.

Before that it was up from 454, 229, and 21 with
[Resolved #57](resolved/missing-archive.md) — one integration test per persistent table over a read
whose archive is not on disk, and two unit tests over `get_archive` itself and over how the failure
it now reports is classified. Before that it was up from 452, 225, and 21 with
[Resolved #16, 51](resolved/partition-load-failure.md) — one integration test per persistent table
over a partition read that cannot be done, and four unit tests over how a read failure is
classified. Before that it was up from 410, 219, and 21 with
[F9](../features/ephemeral-tables.md) — two new integration binaries over the ephemeral tables, six
unit tests over the storage engine that makes them ephemeral, and fifteen in `shoal-bench` over the
eight workloads they made possible. Before that it was up from 359, 219, and 16 with
[F8](../features/purpose-built-workloads.md).

**F8's count moved in both directions, which is the only time that has happened.** It added 61
tests to `shoal-bench` and removed 19 from `shoal`, and the removal is not lost coverage:

- `shoal/src/bencher.rs` had **11**. Five of them — the percentile and summary statistics — moved
  to `shoal-bench`'s workload harness with the code they test. The other six covered loading a
  baseline file, refusing one from another schema version, and a throughput figure; all three
  belonged to a comparison engine that F8 **deleted**, because `shoal-bench` has owned comparison
  since [F7](../features/bench-runner.md) and a second one that nobody reads can only disagree with the one
  that counts. Tests for deleted code are not coverage.
- `shoal/src/stages.rs` had **8**, and they were the eight a default run could not reach. They
  moved with the module into `shoal-bench` and are still feature-gated — but the command that runs
  them is now `cargo test -p shoal-bench --features stage-profile` rather than
  `cargo test -p shoal --features stage-profile`, which is the crate a person working on the
  harness already runs. **That is a change of address, not a fix**: a default
  `cargo test --workspace` still does not run them, and still would not notice if they broke. Nor
  were they, on their own, coverage of the layer — all eight passed against a tree where three of
  the four stage reports a capture produced had nothing in them, because all eight fabricate the
  halves they join ([Resolved #76](resolved/stage-join.md), which added the four that do not).

Before F8 it was up from 172, 219, and 11 with the 187 tests and 5 doctests
[F7](../features/bench-runner.md) added — the whole of `shoal-bench`, which is testable in a way
the three bash scripts it replaced were not. Before those it was up from 172, 215, and 11 with the
four stamp and offset tests
[F6](../features/stage-breakdown.md) added — and F6 also added **eight tests that a default run
does not reach**, because the stage report is behind the `stage-profile` feature. They run under
`cargo test -p shoal-bench --features stage-profile` (`-p shoal` until
[F8](../features/purpose-built-workloads.md) moved the module), and nothing in the default
workspace run would notice if they broke. Before those it was up from 172, 213, and 11 with the two tests
[F5](../features/flushed-sweep-gate.md) added to pin the premises its gate rests on — that staging a
response cannot release one, and that submitting a write moves neither watermark. It is up from
172, 199, and 11 with the fourteen archived-partition
tests added by [F4](../features/validated-archives.md) — the first coverage the `Accessible` arm has
ever had, because until F4 gave `MaybeLoaded` a buffer type parameter that variant could not be
constructed outside a running server. It is up from 168, 194, and 11 with the five config and cpu
selection tests added by [items 18 and 50](resolved/excluded-cores-typo.md) and the four
baseline versioning and throughput tests added by
[F3](../features/performance-harness.md). It is up from 157, 194, and 11 with the query error display coverage
added by [item 48](resolved/query-error-display.md) — eleven rendering tests and no unit tests,
because everything the fix does it does on screen. It is up from 136, 183, and 11 with the
projection coverage added by
[F2](../features/projections.md), and from 136, 178, and 11 with the compaction tail loss tests
added by [item 44](resolved/compaction-tail-loss.md) and the marker format test added by
[item 45](resolved/storage-marker-format.md) — both fixes are unit-testable end to end and
neither added an integration test, which is itself the
[observability gap](todos.md#observability) talking: no test can observe an event the server
emits. It is up from 135, 178, and 11 with the empty rotated log test added
by [item 14](resolved/empty-rotated-logs.md), from 135, 177, and 11 with the eviction accounting
test added by [item 13](resolved/eviction-log-underflow.md), from 133, 168, and 10 with the tablet map and
storage marker tests added by [items 11, 12 and 37](resolved/tablet-ring.md), from 132, 159, and 10 with the
multi-log recovery test added by [item 31](resolved/multi-log-recovery.md) and the recovery
counting tests added by [item 9](resolved/orphaned-update-intents.md), from 115, 129, and 8 with
the range coverage added by [F1](../features/sort-key-ranges.md), from 105 and 116 with the
sort-key selection coverage added with [item 8](resolved/sort-keys.md), and from 87, 95, and 6
before the row-order and `IN`/`OR` coverage added with [items 26 and 39](resolved/partition-order.md). The two persistent-table
binaries take about 24 seconds each; everything else finishes in well under a second. That is with
the default parallelism — the sorted binary takes nearly four minutes under `--test-threads=1`,
because its restart and eviction tests each wait out a real server shutdown.

Defects found while writing this page are in [Known Issues](known-issues.md); performance findings
are in [Optimizations](optimizations.md).

---

## What is covered

### Integration — `shoal/tests/`

| Binary | Count | What it reaches |
| --- | --- | --- |
| `persistent_sorted_table.rs` | 62, one ignored | up 1 with [Resolved #91, 107](resolved/compaction-retry.md), a compaction retried through an unreadable archive and merging what it loads; insert; `exists` true and false; delete; delete after restart; delete surviving restart; delete and update when the partition is not resident; delete and writes surviving eviction; update; update intent replay; multi-log recovery; empty rotated log cleanup; acknowledgement surviving `SIGKILL`; five limit tests; two cross-shard tests; five row-order tests; six sort-key selection tests; two sort-key `exists` tests; six range tests including the archived seek and the memory/disk span; the paging walk; two range `exists` tests; three end-to-end SHQL tests; nine projection tests including the archived scan, the blocked disk read, the cross-partition order, and a projected and an unprojected get in one batch; and the two tests that reach the loader's failure path — a get whose archive cannot be opened ([Resolved #16, 51](resolved/partition-load-failure.md)), and a get whose archive is not on disk at all, which also asserts that the read did not create the archive it could not find ([Resolved #57](resolved/missing-archive.md)) |
| `trace_propagation.rs` | 1 (needs `otel`, which a `--workspace` run supplies) | **new** with [F35](../features/wire-trace-context.md): that one query's spans are **one OpenTelemetry trace** across both processes, and that the server's `Shoal::request` names the client's send as its parent rather than merely sharing a trace id with it — which is what a propagator that carried the trace id and dropped the span id would produce, and reads as one trace with two roots. Unlike `tracing_topology.rs` it asserts on **exported spans** rather than on `tracing`'s `Attributes`, and it has to: `Shoal::request` is still an explicit root as far as the registry is concerned, so the join is only visible in the `SpanContext` the OTLP layer resolves. It was written before the feature and reported `one query's spans were exported as 2 separate traces`, naming what was in each. It drops the client before reading what was exported, because `channel_map` is a `papaya` map and removing an entry defers reclamation — so the span a query was sent in stays open for a while after the query ends |
| `tracing_topology.rs` | 1 | **new** with [Resolved #89](resolved/fragmented-query-traces.md): that every span one query opens on the server resolves to **one** root, and that the root is the socket read rather than the first handler to run. The query is a get after a restart, so the partition really is only on disk and the loader task is really on the query's latency — which is the hop the tree before the fix split into two further traces of its own. It also asserts that both spans the fix opens by hand are **entered**, which is a different defect with the same appearance: `tracing-opentelemetry` timestamps a span on exit, so one that is only ever held as a parent exports with zero duration. It asserts on `tracing`'s `Attributes` rather than on an exporter, so it is about the span tree the registry builds; it holds one test, because the subscriber feeding it is process wide |
| `resident_reads.rs` | 6 | **new** with [Resolved #30, 120, 121](resolved/resident-copy-collision.md): what a partition read does when it lands on a copy already resident, and that a get waits on a replicated apply's read in flight rather than asking for a second. The first binary that builds a shard's tables without a shard: the tables over a seeded directory on a glommio executor, every partition archived and evicted, and a duplicate or divergent read delivered on demand rather than raced for. A body returns its failure instead of panicking, since a panic on an executor thread aborts the binary |
| `disk_lookups.rs` | 1 | **new** with [Resolved #80](resolved/never-flushed-partitions.md): that a sorted partition which was never on disk is asked about **once**, not once per get. Six gets over two never-flushed partitions — one persistent, one ephemeral — must open exactly two `PersistentTable::block_on_load` spans; the tree before the fix opens six. The only test here that asserts on what the engine asked its storage engine rather than on an answer, which it has to be: both paths return the same rows, which is why nothing caught this for as long as it did. It holds one test on purpose, since the counter and the subscriber feeding it are process wide |
| `persistent_unsorted_table.rs` | 20 | up 1 with [Resolved #91, 107](resolved/compaction-retry.md), a compaction retried through an unreadable archive; insert; delete; update; delete and update when not resident; delete surviving eviction; insert after delete when not resident; zero limit; three multi-partition tests; three projection tests; and the unsorted twins of the unreadable-archive and missing-archive tests, because the two tables park and release blocked queries through different code |
| `ephemeral_sorted_table.rs` | 15 | the sorted read and write paths with no storage engine beneath them ([F9](../features/ephemeral-tables.md)): insert; `exists` true and false; delete; update; a limit; cross-shard row order; named sort-key selection; a range and its bounds; a range `exists`; an end-to-end SHQL range; a projection. Plus the three that are about the table rather than about sorted tables — that nothing is written to the storage directory, that nothing survives a restart, and that memory pressure evicts none of it |
| `ephemeral_unsorted_table.rs` | 12 | the same for the unsorted table, over a schema that also holds a persistent one and declares the ephemeral table **first** — which is what pins that a persistent table declared after an ephemeral one still gets its loader spawned, and therefore can still read a partition off disk |
| `tls.rs` | 8 | encryption against a running server ([F14](../features/encryption-in-transit.md)), every one of which skips without `modprobe tls`: that a query round trips over TLS at all; that a MiB response — about sixty four TLS records — comes back byte for byte and lands at the start of a buffer the client aligned; that SCRAM runs over TLS in that order and that TLS does not authenticate on its own; that a plaintext client is refused by an encrypted server and an encrypted client by a plaintext one; and that a client trusting an unrelated authority is refused, so the certificate is checked rather than merely presented |
| `auth.rs` | 9 | authentication against a running server ([F12](../features/authentication.md)): that the right credentials connect **and can then query**, which is what catches an exchange that left a byte unread on the stream the relays are handed afterwards; that a wrong password and a user that does not exist are refused in the same variant carrying the same sentence; that a client with no credentials is turned away in the `HelloAck` rather than after an exchange; that credentials offered to a server which requires none are ignored rather than used, so adding them to a client cannot break it against every server that has not opted in; and four raw-socket tests — that the ack names the mechanism the server selected and names none when it requires none, that a bundle of queries sent instead of a proof is refused **and the shard keeps serving**, that an auth frame past the 4 KiB auth bound is refused inside the 64 MiB frame bound, and that a refusal is flagged in its header |
| `errors.rs` | 3 | the error channel against a running server ([F11](../features/error-channel.md)): that a response too large for the frame bound a raw socket advertised comes back as an `Error` frame naming the query and both sizes rather than as a closed connection, that the *same* connection answers the next query normally afterwards — which is the whole of [Resolved #61](resolved/response-error-channel.md) — and that a get of a partition that was never written is still not a failure, which is the half of the distinction that did not change |
| `backpressure.rs` | ~~1~~ 2 | **new** with [Resolved #15](resolved/shard-mesh-admission.md): a query for a shard that fell behind is shed at once and by name, the rest answered, the held shard back afterwards, a shed query retried, the transport view counting what was turned away; up 1 with [the remainder](resolved/backlog-bounds.md): a raw connection that reads none of its answers stops being read, well short of the 64 MiB it writes, and every query it sent is answered once it reads |
| `table_backlog.rs` | 5 | **new** with [the remainder of item 15](resolved/backlog-bounds.md), on the `resident_reads.rs` harness - a shard's tables held so nothing sweeps them and no loader runs: a write past `max_pending_writes` is shed and never written in both tables, a read past `max_parked_queries` is shed before it parks or asks for a read in both, and a get that has parked on one partition is never shed at the next. All five failed with the bounds switched off |
| `hot_path_failures.rs` | 5 | **new** with [Resolved #16](resolved/hot-path-panics.md), on the `table_backlog.rs` harness: a write whose commit fails - a table built as a cluster node's, rows put in through `apply` - answered `StorageWrite` with the row still there and unchanged, for a delete, an update and an insert on each table; a read the loader cannot take answered `StorageRead` on each table; and a unicast `ServerMsg` refused a copy for a broadcast |
| `pool.rs` | 9 | up 1 with [Resolved #95](resolved/transport-view-every-shard.md): the transport view naming every shard in order, with no link on any of them; the pool and the builder against a running server ([F16](../features/client-builder.md)), and — new with [F36](../features/cluster-harness.md) — the pool's readiness handle: that a shard which cannot bind a held port is reported by `ready` as `ShardFailed`, naming the shard and the error, where `start` used to return `Ok` and a client reached the holder ([Resolved #58](resolved/pool-readiness.md)); and that a port of zero is resolved to one real port before any shard binds, reported by `ready` as the same address, and served on by a client: that a client the builder built answers a query at all, so the route every constructor now takes through it loses nothing; that a client given a dead endpoint ahead of a live one reaches the live one; that a client whose every endpoint is dead fails rather than reporting one that worked; that a pool held to two connections still answers eight concurrent sends, so the numbers reach `bb8` rather than being taken and dropped; and two that open no socket at all — an unsatisfiable pool and a builder with no endpoint are both refused by looking at the configuration rather than by failing to connect with it |
| `framing.rs` | 6 | the framing against a running server ([F10](../features/framing-and-protocol-evolution.md)): four raw sockets sending a hostile length prefix, an unknown message type, a frame that only travels the other way, and a frame from a version that does not exist — each asserting both that its own connection closed **and that a healthy client beside it still answers**, which is the assertion the shard-killing panics used to fail. Plus the two handshake refusals a raw socket can provoke, checking that the reply is a `HelloAck` written in a header the client can read, with the refused flag set and the server's own fingerprint in the body |
| `handshake.rs` | 2 | two schemas in one binary, differing by one field: that a client built from one cannot open a connection to a server built from the other and gets both fingerprints back, and that a client built from the server's own schema connects to the very same server and can query it — the second being what stops the first passing against a check that refuses everybody |
| `fingerprint.rs` | 8 | that the compile-time schema fingerprint actually moves when a schema moves: a field added, a row's fields reordered, a projection declared on an otherwise identical table, and that a whole row and its own identity projection agree; up 1 with [F38](../features/inter-node-transport.md), the schema id being the fingerprint without the version, and 1 with [F39](../features/membership.md), the fixture schema's two table ids pinned as literals and unmoved by a reorder. No server, so all eight run instantly |
| `shql.rs` | ~~53~~ 54 | up 1 with [Resolved #27](resolved/shql-quote-escape.md), a partition key holding an apostrophe bound through a doubled quote; SHQL parsing and binding against a real schema, including range binding and the role refusals, projection binding and its two refusals, plus completion suggestions |
| `storage_meta.rs` | 3 | up 1 with [Resolved #43](resolved/marker-every-root.md), a table under its own root marked and guarded; that a storage directory restarts under the shard count that wrote it, and ~~refuses a changed one~~ since [F47](../features/local-rehome.md) is rehomed under a changed one - two to three to one, every row read back each time and only the survivor's files left - end to end through a real server |
| `intent_log_batching.rs` | 2 | **new** with [F23](../features/self-sizing-staging-buffer.md): that a bundle of 128 rows wider than the staging buffer lands in at most a quarter as many writes — the reproduction for [O34](optimizations.md), which fails on the tree before the fix — and its control, that a `max_buffer_size` pinned to `buffer_size` gives back exactly one write per record. Both read the property off the intent log on disk by counting pad regions, rather than out of the writer, because what O34 is about is how many writes reached the device |
| `staged_flush.rs` | 1 | **new** with [Resolved #36](resolved/staged-tail-deadline.md): a write sent to a single shard whose queue is kept from draining (`ShoalPool::busy_shard`) is answered while the queue is still busy, where it used to wait for the queue to drain |
| `client_disconnect.rs` | ~~1~~ 2 | **new** with [F38](../features/inter-node-transport.md), holding [Resolved #94](resolved/disconnected-client-cleanup.md)'s test that a client leaving with answers owed does not end the shard; up 1 with [Resolved #32](resolved/client-gone-broadcast.md): eight closed connections let go by every shard, read through `ShardTransportView::clients` |
| `retry_outcome.rs` | 3 | **new** with [Resolved #125](resolved/retry-unknown-outcome.md), and starts no server: a scripted one answers each try of `exec_with` with the next code in a list, so an `OutcomeUnknown` followed by a refusal or a budget of sheds comes back `OutcomeUnknown`, and refusals alone stay refusals |
| `completion.rs` (`shoalctl`) | 34 | the completion menu, key handling, query wrapping, and rendering, including the projection slot; and the error box, the underline under the part of a query that failed to parse, the cases where that underline is refused as misleading, and that an error never becomes part of the query it describes |
| `wizard.rs` (`shoalctl`) | 2 | the inventory wizard ([F53](../features/inventory-wizard.md)): the review page drawn on a `TestBackend` showing every node's resolved resources and directories with their source, every page drawn at 200×50 and 80×24, and a saved file that `Inventory::load` accepts and that equals what was built |
| `lib.rs` (`shoal`) | 0 | ~~the bencher's percentile and summary statistics; baseline file handling…~~ `shoal` is a facade with no code of its own since [F8](../features/purpose-built-workloads.md), so it has nothing to unit test. See the note above for where the eleven went |
| `lib.rs` (`shoal-bench`) | 416 | the benchmark runner ([F7](../features/bench-runner.md)): artifact parsing; the registry and its `cargo test` style filtering, including that a partial selection becomes an anchored alternation criterion cannot mis-match; the noise band, including that the tier comes from the baseline so a change cannot pick the band that judges it; the macro layer's interval comparison; the staleness verdict matrix, including that a source digest can only narrow a verdict and never promote one to fresh; the capture plan, including that the uninstrumented rebuild is not inside a phase that a failure could truncate; the wipe guard; the six charts, including that no colour escapes the themed palette and no coordinate comes out `NaN`; and the hand-rolled date conversion over a whole 400-year Gregorian cycle. Since [F8](../features/purpose-built-workloads.md) also the workloads: that the declared id list cannot drift from the registered workloads, that two workloads cannot share a storage directory, that a seed is reproducible and its named streams independent, that each control-and-null pair differs in residency and nothing else, that the fanout curve names distinct partitions and pins its shard count, that each workload's median is picked from its own runs, and that a workload present on only one side of a comparison is named rather than dropped. Since [F9](../features/ephemeral-tables.md) also that each ephemeral workload and the persistent one it is a control for have plans that agree field for field — the gap between the pair is reported as the storage layer, so any other difference between them would be read as storage — and that no ephemeral workload asks to be restarted after seeding, which would measure an empty table. Since [F13](../features/transport-workloads.md) also the transport pair: that every mode is minted at both row widths, that the two sizes of one mode differ in the row and in nothing else, that a seed bundle fits inside the frame bound and never comes out empty, that the large arm's outstanding responses stay bounded in bytes rather than in queries, and that only the single-send mode reports a service time. Since [F14](../features/encryption-in-transit.md) also the encryption sweeps: that every point of both is minted once, that a TLS arm differs from its plaintext twin in the wire and in nothing else, that each sweep varies one thing rather than two at once, that the byte budget keeps the widest arm within an order of the narrowest, that no arm seeds past the memory limit the benchmark config sets, and — on the chart that pairs them — that a pair is joined on its recorded facts rather than its name, that an arm without a twin is dropped, that overlapping runs are never called a result however far apart the medians sit, and that the depth and client sweeps cannot pair with each other despite having identical facts at their first point |
| `explore_index.rs` (`shoal-bench`) | 19 | **new** with [F29](../features/benchmark-explorer.md): that the real corpus projects into the explorer's index and projects the same bytes twice; that every workload in it is explained by a family; that the four mandatory blocks cross unchanged; that a measurement nobody took is absent rather than zero; that the measurement vector is sorted for the binary search that reads it, which would otherwise fail to find rows that are there rather than fail loudly; that the explorer's mirror of `Layer` still agrees with the real one; that the five duplicated formatters agree over every branch; and that `shoal-bench` still enters `shoal-top` with `default-features = false`, which is what keeps an egui tree out of the runner. [F30](../features/plot-axis-units.md) added the last two: that each of the explorer's two presets selects exactly the arms the generated page it reproduces selects, and draws them as one curve per table rather than one line through all four. [F31](../features/metric-availability.md) added four more, all against the corpus: that no workload is offered a metric it carries no value for — the reproduction for [Resolved 83](resolved/default-metric-half-the-corpus-cannot-answer.md), which reported 88 of them against the unfixed tree; that every metric the control would offer for a workload has a measurement behind it; that an intersection over two arms recording different things narrows to what both carry and stays non-empty; and that each preset's own arms can answer the preset's own metric, which is the chart the explorer opens on. [F32](../features/chart-line-identity.md) added one, and strengthened the two preset tests: the added one is the corpus half of [Resolved 84](resolved/identical-facts-one-line.md) — that the two keyed gets still record identical scale facts, and that the chart draws them as two lines named by their identifiers rather than as one line through both — and the preset tests now also assert that each table is drawn in the colour its own slot in `TABLE_ORDER` reserves, and that a chart of one curve per table carries no markers. [F33](../features/chart-readout.md) added two, and both carry a non-vacuity assertion because both are properties of the corpus rather than of a function: that a capture carrying no macro layer answers no macro metric — and that no capture is flagged as answering one without a measurement behind it, which is what stops the greying passing on a corpus that projected nothing — and that a selection only ever *narrows* what a capture answers, with a count that fails if no selection narrows anything. [F36](../features/cluster-harness.md) added one: that every field of `ScaleFacts`, `ConfFacts` and `ClusterFacts` reaches the explorer's copy, by serializing each with every optional field set and comparing key sets — which is how `trace_level` and `trace_remote` were found to have been dropped since [F34](../features/benchmark-tracing.md) |
| `lib.rs` (`shoal-top`) | 49 | the explorer's series construction: that a capture which never measured a workload contributes a hole rather than a point; that a sweep is ordered by its key rather than by the order the boxes were ticked; that a newer index is refused and an older one read; that two captures taken in different places are reported as such, naming the field; and that the newest capture *with a macro layer* is what a macro view opens on — the four most recent captures in the corpus have no macro layer, so this is the live case rather than a hypothetical. [F30](../features/plot-axis-units.md) added ten over what may share a chart and how a legitimate selection is split into curves: that a smoke arm, a per-batch percentile and a mean row width each stay off an axis they cannot be read on, and that a rate is *not* split by the stamping mode, which is the over-refusal on the other side of the same rule; that the row count does not split a width sweep, which is what stops a fact the workload derives from the axis re-entering the curve key; that the first ticked workload decides the units; that a curve is named by what separates it and nothing else; and that two captures of one curve share a hue and differ in the dash. The dark theme added five in `theme.rs`, the first here to touch a drawing module: that `install` leaves the preference on `Dark` rather than on egui's `System` default; that both themes are registered and hold different grounds, so the switch has somewhere to go in either direction; that the two series arrays are the same length, which is what stops a curve changing hue rather than shade when the reader flips it; that neither array hands out one colour for two series; and that the dash cycle keeps its period of three and its solid first arm, which is the other half of hue-and-dash. [F31](../features/metric-availability.md) added five over which metrics may be chosen at all: that a measurement carrying no value for a metric has no units on that axis, and the same for an operation a workload never recorded — the two halves of [Resolved 83](resolved/default-metric-half-the-corpus-cannot-answer.md); that the offered list is the **intersection** of what the ticked workloads answer rather than the union, which is what stops half a selection being drawn as curves that are not there; that an empty selection offers the whole corpus rather than the empty set, without which there would be no metric to choose and therefore nothing to tick; and that the list stays a subsequence of the corpus order, so entries do not move under the cursor as boxes are ticked. They needed the fixture extended for the first time since F30, with an arm that counted no queries and an arm that recorded a `write` where every other one records a `read`. [F32](../features/chart-line-identity.md) added thirteen and moved the fixture's table kinds to the corpus's own spellings, because `index::TABLE_ORDER` reserves a colour for each of those four **by name** and a fixture using short ones would have been testing the unrecorded-kind path throughout. Six are in `index.rs`: two are the reproduction for [Resolved 84](resolved/identical-facts-one-line.md) — that two workloads the recorded facts cannot tell apart are two lines, and that each is named by the identifier that separates them — and four are over the colour rule, including that a named table holds its slot whether or not the chart draws it, which is what stops the colours shuffling as unrelated arms are ticked. Two are in `theme.rs`, over the marker cycle and over the rule that no marker in it may be the circle a lone measurement is already drawn with. Three are the first in `plot.rs`, over `stroke_of`, which is the one place the split of dash and marker is written for both axes. The last two are the first in `app.rs`, over `Framing` — the mechanism that stands in for a reproduction of [Resolved 85](resolved/chart-framed-for-the-last-selection.md), which is a stale zoom on a canvas this machine cannot draw. [F33](../features/chart-readout.md) added ten. Six are the first in `readout.rs`, which is written as pure functions with the drawing at the end so that they can exist: that a bar group and a line sit at different positions for one key, that a logarithmic axis places a column at its logarithm and refuses a key that has none, that the pointer reads the column it is nearest, that every drawn line is a row — largest first, absent last, and absent as `None` rather than `0.0` — that a column is headed the way its tick is, and, holding [Resolved 86](resolved/hover-label-reads-in-decades.md), the label the unfixed tree drew beside the readout that replaces it. One more is in `plot.rs`, asserting that a caption sits where the readout reads, which is the only thing keeping `key_ticks` and `columns` from becoming two definitions of one position. The last three are in `index.rs`, over `captures_answering`: that a capture answers a metric something in it carries, that the question narrows to what is ticked, and that holding the measurement is not the same as holding the number |
| `committed_artifacts.rs` (`shoal-bench`) | 13 | that every artifact committed under `docs/perf/` still parses, that the frozen and trailing baselines differ by exactly the 24 `maybe_loaded` ids, that every pre-[F8](../features/purpose-built-workloads.md) macro capture still lifts to the single `macro/tmdb` workload with what it recorded intact, and that a field added to the version 1 shape fails a test naming it rather than being silently dropped. The drift alarm now applies to version 1 only: version 2 is written by the workloads in this crate out of these very structs, so there is no mirror left to drift. [F36](../features/cluster-harness.md) added two: that every committed capture still parses with no cluster record and every workload in `docs/perf/ports.json` still gets its frozen port from `port_for`, with any id absent from the map sitting after every id in it; and that a single-node capture spells no `cluster` key at all |
| `acceptance_tables.rs` (`shoal-bench`) | 1 | **new** with [F36](../features/cluster-harness.md): the acceptance tables of every page under `docs/src/distributed/` parsed line by line — names unique across the chapter, every milestone one that has a section on the milestones page, that section naming the chapter each test comes from, and every test of a milestone marked delivered existing as a `fn` somewhere in the workspace. It is the check [C11](../distributed/testing.md#the-acceptance-table) promised instead of a hand-copied list |
| `protocol_model.rs` (`shoal-model`) | 4 | **new** with [F36](../features/cluster-harness.md), described in the per-binary table above |
| `partition_keys.rs` | 1 | **new** with [F37](../features/node-identity-control-plane.md), for [Resolved #65](resolved/gxhash-pin.md): eight keys of two shapes, through the real derive, each pinned to the `u64` and the tablet gxhash 2.3.1 gave it on the unfixed tree. It starts no server; a hash is pure CPU over plain data. A composite key was meant to be the third shape and does not compile ([item 92](known-issues.md)) |
| `cluster_fixture.rs` | ~~106~~ ~~109~~ ~~114~~ ~~115~~ ~~116~~ 117, two ignored | 117 counted by the binary's own listing after [Resolved #147](resolved/paused-detector-verdicts.md): up 8 with the [distributed cluster testing](../cluster-testing/overview.md) chapter - a stream older than the retry window still writing (item 138), a stopped leader handing its groups off (item 139), a returning node handed back its groups (O63), a write to a silently cut leader failing fast (item 143) a silently cut node rejoining without elections (item 144) a write through an installing copy answered at its commit (item 145) one through a lagging copy answered within two heartbeats (item 146) and a paused control leader calling nobody down (item 147). 109 measured by the workspace run for [Resolved #128](resolved/hop-deadline-margin.md), which added 1, `a_hopped_write_reports_the_leaders_outcome`, a hop that carries the leader's "did not commit" back instead of timing out first; the row had fallen two behind before that; up 1 with [Resolved #115](resolved/retry-sidecar-crash-window.md), a crash between the staged retry sidecar and the checkpoint file forgetting no identity; up 2 with [F52](../features/cluster-stats.md), a cluster's writes, partitions and standing counted through the leader's `Stats` and a rebalance followed through it; up 1 with [Resolved #106](resolved/isolated-member-term-inflation.md), a member isolated on every lane healing without dying; up 1 with [Resolved #109](resolved/volatile-majority-loss.md), two volatile voters lost at once; up 1 with [Resolved #103](resolved/returning-leader.md), a leader restarted inside its lease stalling no hop; up 1 with [Resolved #110](resolved/dead-primary-write-failures.md), a dead leader's writes refused until its election; up 1 with [Resolved #100](resolved/clone-fencing-under-load.md), the clone that stands before it observes; up 1 with [Resolved #102](resolved/fixture-port-block.md), the port block's bounds against the live ephemeral floor; up 2 with [F50](../features/cluster-operations.md), the M10c acceptance tests - a certificate bound and rotated across a live cluster, skipping by name without kTLS, and a member restarted at another address reached and served with a stale clone refused; up 3 with [F49](../features/backup-and-recovery.md), the M10b acceptance tests - a backup written and verified per group and restored into a fresh cluster that refuses the old identities, a permanent majority loss recovered only by `force_recover` on a stopped survivor which then rebuilds every set on fresh identities, and a standalone node's data exported and restored into a cluster of three judged by digest; up 3 with [F48](../features/rolling-compatibility.md), the M10a acceptance tests - a mixed cluster pinned two ways exchanging forwards, quorum writes, barrier reads, a snapshot over the version 4 link and an election with the activation refused by name, a rolling upgrade under writers with a kill inside the window ending in an activation a pinned restart is refused past, and the opt-in upgrade from a real previous build; up 2 with [F47](../features/local-rehome.md), the M9c acceptance tests - the six-point rehome crash matrix on a node of four slots on two cores, alternating a vanished executor with a live donor under writers and the oracle, and a standalone node dealt per tablet up to three and down to one through two crashes; up 7 with [F46](../features/capacity-rebalancing.md), the M9b acceptance tests - the expiry, the blocked removal, the grace across a leader restart, maintenance, the weighted target, the budgets and the drain; up 9 with [F45](../features/replica-migration.md), the M9a acceptance tests - a write acknowledged after the zero-lag report read on the destination, the eighteen-move crash matrix over the driver, the destination and the control leader judged by the oracle, a caught-up learner never counting before the uniform commit, a retired copy refused by name and reclaimed after its grace, the other tablets' history kept through a retirement, a restart and a compaction, a stale router's writes terminating without a duplicate, a configuration outliving the stale placement a driver died under, a retry identity across a checkpoint and a move, and a repair and a move queued behind each other; up 7 with [F44](../features/repair.md), the M8 acceptance tests - a corrupt primary handing the lead to a verified member and repaired from it, a three way split stopped with its digests and resolved by a named source, the canonical digest over three replicas merged thrice, once and never with a forgotten, an erased and a corrupted partition told apart, a corrupt follower repaired from the leader's cut past its held checkpoint, the seven-point crash matrix of a repair install, the operation refused to a non-admin and a stale version and repeated by id with the control leader killed mid-scrub, and the scheduled scrub quarantining without an operator - and the reproduction of item 99; up 10 with [F43](../features/node-recovery.md), the M7 acceptance tests - a node behind the purge point fed a snapshot per group, the cut's boundary, the crash matrix, duplicates and resume, the installing tablet, the retention budget, the grace and the whole-cluster restart - and the reproductions of items 104 and 105; up 13 with [F42](../features/primary-failover.md), the M6 acceptance tests - a cached report unable to choose a history, a delayed map and term unable to authorize an old primary, a stalled shard under a live control plane failing over, a paused leader unable to serve a stale strong read, an isolated old primary refused, a minority never committing, a `Down` member keeping its placement, a control majority unable to activate a data minority, established groups outliving a control quorum, a lost reply recovered by a retry across an election and past the purge point, a session token through a leader change, identity and budget across a forward with a reroute, and the oracle over eight rounds of dropped replies, kills and restarts; 6 with [F41](../features/read-consistency.md), the M5 acceptance tests, which this row did not count until now; 9 with [F40](../features/replication.md), the M4 acceptance tests over replication - a quorum needing a second distinct durable voter, a factor of three on one node refusing writes until initialized, an `Async` replica refused, a delayed, cut and healed lane with a stale restart converging, an isolated leader's suffix never reaching a checkpoint, concurrent conditional writes judged by the oracle, a stalled group shedding at its bound while another is written, the ephemeral table replicating through the same encoding, and `One` reads converging without exposing an uncommitted entry; 13 with [F39](../features/membership.md), the M3 acceptance tests over a membership cluster the fixture stages - three nodes bootstrapping through node zero, a fourth staying a learner, a minority unable to commit, lost seeds not re-bootstrapping, no external coordinator, a duplicate identity fenced, elections without the data lanes, atomic map versions, stable table ids, the client's topology frame, the three readinesses, admin by principal and operation id, and fresh reports not masking a dead shard; 4 with [F38](../features/inter-node-transport.md), the M2 ones, which this row did not count until now; 5 with [F37](../features/node-identity-control-plane.md), the M1 acceptance tests: a node killed and restarted coming back as itself with its topology version recovered and both mode changes refused; unknown configuration keys, unimplemented settings and format 1 and 3 markers refused by name; the control core refused outside the affinity and kept with both SMT threads away from every shard; a standalone child with none of it beside a cluster child with all of it; and the documented `cluster:` defaults being the policy a bootstrap seeds. The three M0 tests are now run against cluster children, each owning a control core. **New** with [F36](../features/cluster-harness.md): that two clusters started at once on port zero own four distinct answering endpoints and that every child is gone after its cluster is dropped by a panic; that a cut ends a live stream and the reconnect after it while the other direction passes, and that a paused server stalls a query that completes on resume where a cut would fail it; and that the allocator on a synthetic machine hands out disjoint counted claims, refuses an overlapping exact one and records sharing on a machine too small, with a real cluster's plan carrying the endpoints its children bound. The two ignored functions are the children — a server on port zero that reports what it bound and watches its pool for a death, and a listener that echoes — which the fixture runs by name with `--exact --ignored` |
| `css_sync.rs` (`shoal-bench`) | 4 | that every chart colour sentinel has a `fill` and a `stroke` rule in `docs/theme/charts.css` and every rule there matches a sentinel — a sentinel with no rule is drawn literally, bright red on a navy page — and that the stylesheet is still registered in `book.toml` |
| `chart_geometry.rs` (`shoal-bench`) | 6 | that no chart drawn from the real artifacts puts two labels on top of each other or draws outside its canvas. plotters is built without a font backend and estimates text extents, so this is the failure that no other test can see. Up 2 with [F19](../features/chart-legends.md): that a series is named exactly once, which is what an end label surviving would break, and that a legend entry's name stays with its own swatch rather than running under the next column — the only check anywhere on the width estimate the columns are laid out from |
| `stages.rs` (`shoal-bench`) | 8, **feature gated** | the stage report: that a bucket's stage means reconcile with its total, that a bucket is a window rather than one record, that an unreached stage is not reported as an instant one, that a write reports its four durability stages, that every record is accounted for as joined, one-sided or duplicate, that a stage the size of a clock read is marked rather than reported, that a batch level cost is labelled, and that a report from another schema version is refused. **Only built with `--features stage-profile`** — a default `cargo test --workspace` does not run any of them. Run them with `cargo test -p shoal-bench --features stage-profile`. Every one of these passed while three of the layer's four reports were empty, because every one of them fabricates the halves it joins |
| `stage_log.rs` (`shoal-bench`) | 3, **feature gated** | **new** with [Resolved #76](resolved/stage-join.md): that a query sent and never answered is counted rather than dropped when the driver returns, that two slots' logs pool into one — which every per query driver depends on — and that the streaming path keeps one query in every `--stage-sample`, the same rule the server applies |
| `stage_join.rs` (`shoal-bench`) | 1, **feature gated** | **new** with [Resolved #76](resolved/stage-join.md), and the one test here that starts a server: one grid arm at smoke scale under `stage-profile`, asserting the report has a join in it, that neither half is one-sided, that both halves of the mixture produced a breakdown, and that a bucket has stages in it. It fails against the tree before the fix with `joined: 0`, which is the whole defect. This is the check the layer never had — that a workload on `STAGED_WORKLOADS` can actually produce a joined record, as opposed to being correctly listed. Since [Resolved #97](resolved/stage-join-storage.md) it runs against a scratch copy of the committed `shoal.yml` with its storage under `CARGO_TARGET_TMPDIR`, and asserts the server wrote there, so it runs on any host rather than only one with `/opt/shoal` |

The restart, eviction, and `SIGKILL` tests are the valuable ones: they are the only tests that
exercise durability end to end, and they exist because
[items 1-3](resolved/durability.md), [4](resolved/unsorted-disk-consultation.md), and
[5](resolved/resurrected-deletes.md) needed them.

### Unit — `shoal-core`

| Module | Count | What it reaches |
| --- | --- | --- |
| `shared/queries/parser/tests.rs` | 59 | the SHQL grammar, including `IN` lists, `OR` folding, each range operator, the folding and refusals around a range, and the projection slot with its offsets |
| `shared/queries/parser/complete/tests.rs` | 27 | completion suggestion generation, including the range operator tokens and a projection standing where the star does |
| `.../storage/fs/tests.rs` | 27 | the intent log reader against real files, including which tail shapes are damage and which are how a healthy log ends, and what a compaction is about to throw away with the log it deletes; how a failed partition read is classified — which of the three classes is retried, and that an unrecognised error is given up on rather than retried forever ([Resolved #16, 51](resolved/partition-load-failure.md)); and `ArchiveMap::get_archive` over an archive that is not on disk, that it names the archive rather than creating one and that the failure is never retried ([Resolved #57](resolved/missing-archive.md)) |
| `.../storage/fs/stream_tests.rs` | 19 | `StreamWriter` alignment, padding, and watermarks, including that submitting a write advances neither watermark in either durability mode — the premise [F5](../features/flushed-sweep-gate.md)'s sweep gate rests on. Up 5 with [F23](../features/self-sizing-staging-buffer.md): the five pure tests over `staging_target`, which need no executor, no `DmaFile` and no schema, since the sizing rule was deliberately factored out as a free function so it could be tested without one |
| `tables/partitions.rs` | 59 | tombstone bookkeeping, limits, sort-key selection and range selection on `get` and `exists`, the empty-range guard, `merge_from_disk` sizing, the recovery counting that separates a correctly dropped update from a lost one, and the projected scan across all three selections; plus the archived arm of all of those — that a truncated or root-corrupted archive is refused, that the unchecked read lands on the same reference the checked one does, and that an archived partition answers every selection identically to a resident one holding the same rows ([F4](../features/validated-archives.md)) |
| `shared/protocol/tests.rs` | 38 | the frame format itself ([F10](../features/framing-and-protocol-evolution.md)): that every message type is still written as the byte it has always been written as and that a zeroed buffer is not a valid one, that every flag bit is where it was and an unknown one is round-tripped rather than masked off, that both preambles are the size they were before the header existed, that a version this build does not speak is refused but still readable, that a length past the bound and a payload past a `u32` are both refused before anything allocates or truncates, and that the fingerprint's separator stops two adjacent fields concatenating. Since [F11](../features/error-channel.md) also the error frame: that every error code is still written as the number it has always been written as and that one this build does not know reads as `Unknown` rather than failing to decode, that an error frame round trips with its flag and its type byte, that its query id sits at exactly the offset a response frame's does — which is what lets the client read one preamble for both — that a frame shorter than its own fixed fields is refused, that a message past the four kibibyte bound is refused in both directions even though the frame bound would allow it, and that a message which is not valid UTF-8 still delivers the code it came with. Since [F12](../features/authentication.md) also the auth frames: that every mechanism and every status is still written as the byte it has always been written as and that a zeroed buffer decodes as neither — in particular that it is never a success — that a mechanism bit this build cannot name is carried through rather than masked off, that selection walks the *server's* preference order rather than the client's bits, that both frames round trip and a refusal is flagged in its header as well as its status byte, that a payload past the four kibibyte auth bound is refused in both directions inside a frame bound that would allow it, that a body too short to hold its own fixed part is refused rather than indexed into, and that a `HelloAck` naming a mechanism from a build that does not exist yet reads as none |
| `shared/responses.rs` | 3 | the precedence rules a failure has to obey ([F11](../features/error-channel.md)): that a failed share wins a merge in either direction — the one place three shards' rows can hide a fourth shard's failure — that an error response is never a success however permissive the `QuerySuceededOpts` are, since the options say which outcomes count and a failure is not an outcome, and that a limit trims rows and leaves a failure alone |
| `client.rs` | 6 | that a response payload lands at the start of a sixteen byte aligned allocation across seven awkward payload lengths — the guard on the two-read structure the zero-copy response path rests on — and that a frame larger than the client's own bound is refused before it is allocated for. Since [F11](../features/error-channel.md) also the frame dispatch: that an error frame ahead of a response leaves that response's payload aligned, which is the guard on the type branch that now sits between the two reads; that an error frame reaches the query it names; that one for a query nobody is waiting on is dropped rather than ending the read loop and taking every other query on that connection with it; and that a connection which dies fails the queries written to it **and no others**, which is what stops a dead socket failing the other forty nine connections in the pool |
| `shared/queries.rs` | 10 | sort-key normalization, and `SortRange` emptiness and containment |
| `tables/storage.rs` | 7 | `PendingResponse` release against a durable watermark — including that staging a response never releases one, which is why [F5](../features/flushed-sweep-gate.md) can skip its sweep on a write — and `RecoveryStats` merging and cleanliness |
| `server/ring.rs` | 10 | the tablet map: that an empty one cannot be built, that tablets are split evenly and no shard is starved, that ids come from the high bits so a split stays incremental, and that two independently built maps agree; the placement of one node being the standalone ring and one of several interleaving nodes then shards; and since [F47](../features/local-rehome.md) that a ring from the identity hosting is the ring of old and one from a dealt hosting routes by the table, and that a placement judges the slots and owns a local tablet through the executor hosting its slot while every remote slot stays its own contact |
| `server/hosting.rs` | 3 | **new** with [F47](../features/local-rehome.md): the identity hosting being the ring's, the deal - per tablet standalone and per slot on a cluster node, both directions, deterministic, within one of even, refusing a growth past the slots and leaving no executor empty - and the file's round trip and refusal of a torn table |
| `server/rehome/manifest.rs`, `server/rehome/tests.rs` | 2, 3 | **new** with [F47](../features/local-rehome.md): the plan's order - folds, archives, logs, reclaims, finalize - and a manifest resuming at its first undone step; a redone archives step removing its partial archive and counting a finished copy, a redone log step appending nothing to a group already moved and touching no other slot's group, and a manifest towards one count refusing a claim under another |
| `server/map.rs` | 7 | the map's state, the placement of tablets over distinct nodes, the routing that prefers holders that are up, the configuration overlay a move leaves, and since [F47](../features/local-rehome.md) a node of four slots on two executors serving every local copy from the executor hosting its slot and splitting its groups by the same table |
| `server/peer/tests.rs` | 2 | the judge over a real loopback handshake, and since [F47](../features/local-rehome.md) the listener's dispatch of a slot to its host and its refusal of one past the count |
| `server/conf.rs` | 11 | that a misspelled resource key fails the load instead of being dropped, that `exclude_cores` is parsed and removes both threads of a core, that cpu selection is deterministic and fills distinct physical cores before pairing onto an SMT sibling, and that a config which never mentions `max_frame_bytes` still gets one — which is what let the frame bound be added without touching the committed `shoal.yml` every frozen benchmark was captured against. Since [F12](../features/authentication.md) also the `auth` section: that a config with no such section requires nothing, which is the property that keeps every existing deployment and every benchmark connecting; that a named password is derived at load and nothing downstream holds one; that a derived credential can be written in the file instead; and that a mechanism name nothing knows fails the load rather than yielding a server which requires proof it can never grant |
| `server/meta.rs` | ~~12~~ 16 | claiming a storage directory, reopening it under the same shard count, ~~refusing a changed one~~ since [F47](../features/local-rehome.md) reporting a changed core count as a pending rehome and a manifest towards another count as a refusal, the slots claimed once and bounding the cores, refusing a marker whose format this build does not know, the format 2 upgrade, the joiner's adoption, the bootstrap's idempotence, both mode changes, the wrong cluster without a write, the topology observation and the directory lock, and since [Resolved #126](resolved/storage-directory-unusable.md) a primary root and a mirrored root that cannot be created, each refused naming its path |
| `tables/persistent.rs` | 2 | the two pieces of arithmetic on the shard memory counter: that a shrink subtracts instead of wrapping, and that an eviction summarizes itself without underflowing on a drifted counter |
| `.../storage/none.rs` | 6 | the watermark that stands in for an intent log's positions ([F9](../features/ephemeral-tables.md)): that commits hand out distinct rising positions, that a release covers every one of them, and that a sweep is asked for exactly when a response is parked and not otherwise — the last being the only thing that ever answers an ephemeral insert |
| `shared/auth/tests.rs` | 17 | the mechanism itself ([F12](../features/authentication.md)), all of it without a socket: that RFC 7677's test vector produces RFC 7677's proof and its server signature byte for byte — the one test standing between "this implements SCRAM" and "this implements what this repository thought SCRAM was" — that the right password authenticates and the wrong one does not, that a user which does not exist fails in the same variant as a wrong password **and gets a challenge with the same stable, plausible salt**, that a tampered proof, a replaced nonce, an echoed nonce nobody sent and a tampered server signature are each refused by the half that should refuse them, that a client asking for channel binding is turned away rather than quietly answered without it, that messages out of order are refused on both sides, that a username containing a comma or an equals survives the exchange rather than injecting a field into the signed message, and that a stored credential carries no password, salts freshly per derivation, round trips through the YAML a config spells it in, and prints neither key in a log line |
| `.../storage/fs/map.rs` | 2 | map intent replay, and since [Resolved #111](resolved/archive-removal-borrow.md) that removing an archive does not hold the handle cache across its close, so a read landing meanwhile is served rather than panicking the executor |

The storage tests run against a real filesystem on purpose — `TempDir::new_in(CARGO_TARGET_TMPDIR)`
rather than `/tmp` — because glommio silently disables `O_DIRECT` on tmpfs, which would make
alignment unenforced and `fdatasync` meaningless (`shoal/tests/utils.rs`, and the note in
[TODOs](todos.md#storage-engine-abstraction)).

---

## What is not covered

Ordered by what would find the most, soonest.

### Compaction and archive rotation

Nothing. `MIN_ARCHIVE_COMPACTABLE` is 10 MiB (`.../fs/compactor.rs:33`) and no test writes near
that, so `compact_archives` never does real work in the suite. That leaves the archive read path,
entry rewriting into a new active archive, the 50% utilization decision, archive deletion, and
`sort_by_load` all unexercised.

`build_pressured_config` (`shoal/tests/utils.rs`) shrinks the *intent log* to 4 KiB so generations
advance quickly, which is what the eviction tests need — but archives are a separate threshold and
nothing shrinks it. Making `MIN_ARCHIVE_COMPACTABLE` configurable is already an open TODO
([TODOs](todos.md#storage)) and is the cheapest way in.

This is the largest gap on the page: compaction is the only component that rewrites committed data.

*Intent* log compaction has one test now — `empty_rotated_intent_logs_are_deleted`, added with
[item 14](resolved/empty-rotated-logs.md) — but it asserts on what the compactor removed, not on
what it wrote. The archive side above is untouched by it.

### Multi-log recovery

**Now covered**, by `multi_log_recovery_keeps_earlier_intents`
(`persistent_sorted_table.rs`), written to reproduce
[item 31](resolved/multi-log-recovery.md). It is worth reading before writing another recovery
test, because it works around the thing that made this gap persist: the only way to leave an
inactive log behind is to interrupt a compaction, and a test cannot interrupt one reliably. So it
does not try. Two real single-shard servers write two genuine intent logs, and the test then
arranges them on disk — one renamed to `Shard-0-inactive-1`, the other copied in as the active
log — into the state an interrupted compaction leaves behind. No `SIGKILL`, no timing.

Writing it also corrected the gap's premise. Two inactive logs are not needed: the active log is
always replayed last, so one inactive log plus the active log is enough — a single interrupted
compaction, rather than two. ~~And that is a state a clean shutdown produces.~~ It was, while
every clean shutdown left an empty inactive log behind; since
[item 14](resolved/empty-rotated-logs.md) a clean shutdown leaves none, which is why the test
stages the log by hand rather than arranging for one.

What is still not covered is a recovery spanning *three or more* logs, and one where the same
partition is touched in three different generations.

### Anything that is only reported through `tracing`

`ShoalPool::start` does not initialize a subscriber — `trace::setup` is called by the binaries, not
by the server — and ~~since [F8](../features/purpose-built-workloads.md) neither the example nor the
benchmark workloads call it either~~ ~~the example calls it again, while the benchmark workloads
still do not~~ **the example and `shoal-workload` both call it**, since
[F34](../features/benchmark-tracing.md). No integration test can therefore observe any event the
server emits, and none tries — and F34 did not change that, for the reason below.

The obstacle is not that nobody calls `setup`; it is that `setup` installs a **global** subscriber.
A test that called it would decide what every other test in that binary sees, and the first one to
run would win. Covering these events needs a non-global path — a `TraceGuard` built over a local
subscriber, or the events asserted through something other than `tracing`.

That is what the per-shard recovery summary added by
[item 9](resolved/orphaned-update-intents.md) runs into: the counting that feeds it is unit
tested from four directions, but the event itself — its level, its fields, and that it fires once
per shard — was verified by hand against the `tmdb` example, which no longer exists in that
form, and has no automated coverage. The
same is true of the compaction summary and every eviction event.

Closing this needs a subscriber a test can install and read back. The obstacle is that a
subscriber is process-global while these binaries run their tests in parallel threads
([below](#the-suite-cannot-safely-run-its-binaries-in-parallel)), so captured events would have
to be attributed to the test that caused them.

**A shard that dies is only reported this way, which is why no test can assert on one.**
`ShoalPool::exit` logs each shard's join result at `ERROR` and returns `Ok(())` regardless
([item 58](known-issues.md#58-a-shard-that-dies-is-not-reported-to-whoever-started-the-pool)). So a
test whose server lost a shard sees it as a query that never came back, and cannot say why — which
is exactly how [Resolved #57](resolved/missing-archive.md) presented, and why establishing what
killed the shard needed a temporary `eprintln!` rather than an assertion.

### The streaming client APIs

`stream()`, `stream_unordered()`, `ShoalResultStream::skip`, and the out-of-order reassembly
through `pending: BTreeMap` / `BTreeSet` (`shoal-core/src/client.rs`) have **no test at all**.
Every integration test goes through `send`, `exec`, `send_one`, or `exists`.

That is where `skip(0)` panicking has been able to sit unnoticed
([item 23](known-issues.md#23-client-stream-and-pool-rough-edges)), and the reassembly logic is
the part of the client most likely to be wrong, since it is the only part that has to hold state
across responses.

**A second defect surfaced here during the [August 2026 review](review-2026-08.md)**, and it is
worth reading as evidence about the gap rather than only about the bug: a stream that is not
drained to its last response never releases its entry in the client's response map, because the
release lives inside the `if end` arm of `next`
([item 60](known-issues.md#60-a-result-stream-that-is-not-drained-to-the-end-leaks-its-slot-in-the-client)).
Every supported way of ending a stream early leaks. It has sat there since the streams were
written, and no test could have caught it, because no test constructs one.

What the contract of these buffers actually is — and why keying them by response index is sound —
is now written down in [The Client](../api/client.md#the-reorder-buffers), which is the thing to
read before writing the tests this section is asking for.

### Concurrency and the connection pool

~~Nothing issues concurrent queries, exhausts the pool, or forces a reconnect.~~ **Partly closed by
[F16](../features/client-builder.md)**, which added `pool.rs`: `a_pool_sized_by_the_caller_still_answers`
holds a client to two connections and sends eight queries at once, so the pool is exhausted and
shared for the first time, and `an_endpoint_that_is_down_is_tried_past` forces a connect to fail and
be retried elsewhere.

**What is still not reached is the health checks.** `is_valid` and `has_broken` are exercised
incidentally by any test that checks a connection out, but nothing kills a peer underneath a live
client, so the case they are known not to catch — a server gone without its socket being reset
([item 23](known-issues.md#23-client-stream-and-pool-rough-edges)) — is still not something a test
would notice either way. That needs a harness that can take a server away from a client mid-query,
which is what [D6](../direction/connection-pool.md#how-it-would-be-measured) calls the single most
valuable test infrastructure this client could grow, and it is what the `Ping` work will need.

### Filters, end to end

The SHQL tests confirm a filter is *parsed and bound* into the query
(`shoal/tests/shql.rs`, `binds_unsorted_filters` and friends), and the partition tests confirm
limits are applied. Nothing confirms the server actually excludes a row: no test asserts that a
get with a filter returns fewer rows than the same get without one.

### Unsorted tables lag sorted ones

Sorted tables now have `exists`, multi-partition gets, limits, and cross-shard coverage. Unsorted
tables have none of those — no `exists` test, no multi-partition get, no cross-shard test. The two
implementations have diverged before ([item 4](resolved/unsorted-disk-consultation.md) was
unsorted-only), so the asymmetry is worth closing.

### Lifecycle and hostile input

- ~~**Client disconnect** — item 32. No test opens a connection, closes it, and asserts anything
  was released.~~ Covered since [Resolved #32](resolved/client-gone-broadcast.md):
  `a_client_that_leaves_is_forgotten_by_every_shard` closes eight connections and sees every
  shard's `clients` count return to zero.
- ~~**A shard that never answers its share** — item 33.~~ Covered since [F41](../features/read-consistency.md): `a_standalone_gather_expires_at_the_query_deadline` holds every share and sees the gather answered `Timeout` ([Resolved #33](resolved/gather-expiry.md)).
  The cross-shard tests only cover the happy path.
- ~~**Malformed wire input**~~ — covered since [F10](../features/framing-and-protocol-evolution.md).
  `shoal/tests/framing.rs` sends an oversized length prefix, an unknown message type, a frame that
  only travels the other way, and a frame from a version that does not exist, each from a raw
  socket beside a healthy client ([Resolved #34](resolved/unvalidated-length-prefix.md)). What is
  still uncovered is a **truncated body** — a header followed by fewer bytes than it claimed, which
  parks the relay rather than failing it, because nothing anywhere has a deadline
  ([TODOs](todos.md#timeouts)).
- **Composite sort keys** — [item 42](known-issues.md#42-shql-cannot-express-a-composite-sort-key).
  Sort-key selection is covered on both scans and both table forms
  ([item 8](resolved/sort-keys.md)), but every table in the suite has a single-field sort key, so
  nothing exercises a tuple `Sort` through SHQL or through a seek.

---

## ~~The suite cannot safely run its binaries in parallel~~ — it can, since F36

This section recorded [item 38](resolved/pool-readiness.md) while it was open: every test binary
handed out ports from its own counter starting at 13000, cargo ran the binaries in parallel, and
glommio's `SO_REUSEPORT` let the second bind of a number succeed silently, so a client in one test
could be handed a server from another. Eleven binaries contended, the sorted one bound 13000
through 13102, and the suite passed only because no two servers were alive on one port at the
same instant. It is kept as a record of what bounded the confidence of everything above.

[F36](../features/cluster-harness.md) took the fix the section proposed: bind port zero and read
the assignment back. `ShoalPool::start` resolves a zero to a real port through a reuse-port
reservation before its shards spawn, `ShoalPool::ready` reports it, and no helper under
`shoal/tests/` chooses a port any more — the counter, the hardcoded 13900 in the crash test and the
`listening on` line the table was measured with are all gone. Two binaries still bind numbers of
their own: `stage_join.rs` in `shoal-bench`, feature gated and above the capture range, and the
capture itself, whose ports are its workloads' positions and are frozen in `docs/perf/ports.json`.
There is nothing left here to re-measure.
