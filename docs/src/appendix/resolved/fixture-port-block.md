# 102. A deferred fixture node could lose its reserved port to an outbound connection

## Symptom

`minority_cannot_commit_membership_changes` failed once under the full fixture suite at
`--test-threads 6` while [F42](../../features/primary-failover.md) ran it: its deferred node
failed to bind with `AddrInUse`. It passed alone and in the next two suite runs. Any test that
defers a node - eight of them - could meet the same failure at the same odds.

## Cause

The fixture reserved every node's data and control port from the **ephemeral range**, by
binding a never-listening `SO_REUSEPORT` socket at port zero and reading the number back, and
dropped every reservation once the nodes it started had bound. A node the builder deferred had
not bound anything at that point, so its two ports were free until `start_deferred` ran - free
for any other test in the suite to be handed as the *local* end of an outbound connection,
which the kernel takes from the same range. When that happened the deferred node's bind failed.

The reservation could not be extended to cover the gap: a listener cannot bind over a
client-side `TIME_WAIT` whatever it sets (the note under
[F41](../../features/read-consistency.md#limitations), verified on this host with a socket test
when the benchmark harness met the same thing), so holding the socket longer would only have
moved the failure to the moment it was dropped.

## Evidence

**Established by reading the reservation code**, after the failure the item records: one
`AddrInUse` on a deferred node under the suite, not reproduced on demand, since it needs another
test's outbound connection to land on the freed port in the window. The mechanism is the one the
benchmark harness had already met from the other side - an arm's control port lost to the
`TIME_WAIT` of a link an earlier arm had dialled out through it - and fixed there by moving its
blocks under the floor ([F41](../../features/read-consistency.md)). With the fix the eight
deferred-node tests, the fencing test and the address-change test pass at six threads, and the
whole suite ran once at six threads in five minutes with ninety-five of ninety-eight passing -
the three that failed (`repair_is_authorized_versioned_and_resumable_by_id`,
`scheduled_scrub_quarantines_without_an_operator`,
`single_node_data_has_a_verified_cluster_migration_path`) on a scrub, a quarantine and a
restore not done in time under the load, none on a bind, and all three passing together at two
threads afterwards.

## The fix

A port below the ephemeral floor is never the local end of an outbound connection, which is the
whole defect. `shoal/tests/cluster/ports.rs` hands out sequential ports from
`FIXTURE_BASE_PORT = 28_000`, reads the floor from `/proc/sys/net/ipv4/ip_local_port_range`
(32 768 when the file cannot be read), refuses to reach it with a message naming this item, and
reserves nothing: the number is the reservation. Each port is probed once with a plain bind so a
port some other process on the host holds is skipped with a note rather than handed to a child.
`build_membership_cluster`, `spawn_clone` and `restart_at_new_address` take their ports from it;
`StagedPlan` no longer carries sockets, and `Cluster::start` no longer has a drop to time.

The block sits above everything the benchmark harness binds: `12000` plus a workload's position
for the single node arms, `13871` for `stage_join`, `20000` plus sixty-four per cluster arm for
the cluster arms. Only `cluster_fixture.rs` includes the fixture module, so one process-wide
counter is the whole space, and the suite's worst case - ninety-nine tests at eight nodes with
two ports each, plus every clone and restart - fits under the floor with room.

## Alternatives rejected

**Hand the deferred nodes' reservations to `start_deferred` instead of dropping them.** It
closes the window this item names and leaves the one the F41 note describes: a reservation
dropped at any moment can be taken by a `TIME_WAIT` that a listener cannot bind over. Below the
floor there is no such moment.

**Keep the `SO_REUSEPORT` reservation and take the number from the block.** A bound socket
below the floor keeps nothing away that the number does not already; it costs a socket per
port and the drop it was timed by.

**A block per test binary computed from its name.** There is one binary; the counter is
simpler and a second binary would take the module by copy in any case.

## Invariants to uphold

- **Every port the fixture binds is below the host's ephemeral floor and above the benchmark
  harness's ranges.** `fixture_ports_sit_below_the_ephemeral_floor_and_above_the_bench_ranges`
  checks both against the live floor; a new listener in a test takes its port from
  `ports::next_port` and nowhere else.
- **The block is process-wide.** A second test binary that included the fixture module would
  start its own counter at the same base; if one is ever written, it takes a base of its own.
- **A restart at a node's own ports still relies on `SO_REUSEADDR`** in `peer::bind_reusable`,
  for the server-side `TIME_WAIT`s its old connections left. That is a different state from
  the client-side one and the one the flag does cover.

## Still open

- The client port every child binds is still zero, reserved by the pool the way
  [F36](../../features/cluster-harness.md) describes and reported on the ready line. It is
  bound before the reservation is dropped, so it has no window; it is in the ephemeral range,
  so a restart at the same client port is not something the fixture does.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `fixture_ports_sit_below_the_ephemeral_floor_and_above_the_bench_ranges` | `shoal/tests/cluster/ports.rs` | A port handed out in the ephemeral range, or under the bench's blocks |
| `minority_cannot_commit_membership_changes` and the seven other `deferred_from` tests | `shoal/tests/cluster_fixture.rs` | The window comes back, at the odds the item records |
| `duplicate_node_identity_is_fenced`, `address_change_is_observed_and_a_stale_clone_is_fenced` | `shoal/tests/cluster_fixture.rs` | A clone's and a moved node's fresh ports come from the block |

## Related

[F36. Cluster harness](../../features/cluster-harness.md), the fixture; [C11. Testing](../../distributed/testing.md);
[F41. Read consistency](../../features/read-consistency.md), where the bench harness met the
same range; [Resolved #100](clone-fencing-under-load.md), the other suite-load failure.
