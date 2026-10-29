# 328. The fixture's clusters started beside each other were all given the same cores

The cluster fixture ([F36](../../features/cluster-harness.md)) gives every node whole physical
cores: data cores for its shards, a control core for its control thread, and its object executor
on a sibling thread. [C10](../../distributed/performance.md) asks that sharing be recorded rather
than hidden when the machine cannot support a claim. The fixture binary runs its rows at six threads
in one process, so up to six clusters run at once.

Found and fixed in the flaky-test plan's confirmation (2026-10-27).

## Symptom

In the confirmation loop's rounds, rows that had passed in every earlier loop failed together:

- `an_overloaded_group_sheds_rather_than_timing_out` gave up thousands of hops through a follower
  as unknown, its leader silent for longer than a second: *the member last sent 1.156s ago, its
  node 1.156s ago*.
- `drained_device_leaves_only_when_its_chunks_are_elsewhere` and
  `move_serves_reads_and_writes_throughout` had writes not acknowledged through a drain and a move.

A node silent for a second, its whole process and not one task, is a thread that did not run.

## Cause

`Cluster::start` allocated each cluster from the whole machine as if it ran alone. It read the
machine (`Topology::detect`) and handed out cores (`cores::allocate`), and nothing passed between
two clusters. A counted claim takes the lowest free cores, so every cluster took cores one, two,
three and on: up to six clusters' shards, control threads and object executors were pinned to the
same few cpus, while the rest of the machine ran none of them. Neither the plans nor anything else
recorded the sharing.

## Evidence

**Established by counting the running children's pinned threads**, during the first round of the
glommio fix's A/B as first started (2026-10-27, `w5ab-nofile1024` on
[the flaky page](../flaky-tests.md#runs), restarted for its open-file limit, which the allocation does
not depend on): every thread of every child process whose affinity was one cpu, grouped by that cpu.

| cpu | pinned threads | node processes |
| --- | --- | --- |
| 1 to 6 | 24 to 27 each | 6 each |
| 7, 8 | 4 each | 1 each |
| 20 to 22 | 8 each | 4 each |
| the other twenty | none | none |

Twenty node processes ran, six tests' clusters, and every one had pinned its shards to cpus one
to six.

**Reproduced** by `two_live_clusters_share_no_core_unrecorded`, written first and run alone, where
the machine has room for both clusters:

```text
two live clusters took the same cores [1, 2, 3, 4]: {1, 2, 3, 4} and {1, 2, 3, 4}
```

With the fix, the same count during the first round of the loop after it (`w6`, nineteen node
processes) found every core in use and none carrying more than three nodes:

| cpu | pinned threads | node processes |
| --- | --- | --- |
| 1 to 6 | 9 to 13 each | 3 each |
| 7 to 15 | 6 to 9 each | 2 each |
| 18 to 31, the siblings | 2 to 10 each | 1 to 3 each |

The machine still runs more nodes than it has cores, and they share evenly now.

## The fix

- **Every started cluster holds its cores in one count for the process** (`cores::HELD`), from its
  start until it drops (`CoreLease`, kept on the `Cluster`).
- **A counted claim and a control core take the cores held least, then the lowest**
  (`allocate_among`). A cluster beside others gets cores none of them holds while the machine has
  them, and an even share once it has not. A cluster alone is allocated exactly as before.
- **A plan records every core it shares with a live cluster** (`ClusterPlan::overlap`), as C10 asks.
- The count is locked from its read to the hold, so two clusters starting at once never both take
  a core as free.

## Alternatives rejected

- **Make a cluster wait for free cores.** It would serialize the heavy rows, and a row that runs two
  clusters at once, as a restore into a new cluster does, would need both together. The suite
  would take far longer than six threads' worth.
- **Run every node unpinned.** A deployed node's shards and object executors are pinned, and rows
  that place an object executor or a control thread test what pinning gives them.
- **Fewer test threads.** The suite takes longer, and the clusters that do run at once still stack
  on the same cores.
- **A cpuset for each test.** It needs privileges a test run does not have, and the count does the
  same within one process.

## Invariants to uphold

- **A cluster's lease lives as long as its nodes.** A node's thread pinned to a core no lease holds
  is one the next cluster takes too.
- **Allocation and holding happen under one lock**, or two clusters starting together take the same
  cores as free.
- **An exact claim is taken as named**, whoever holds the core; only counts and control cores move.
- **A plan's overlap names exactly the cores another live cluster held** when it was allocated.

## Still open

- **A row that plans a cluster alone and starts it beside others is given other cores.**
  `control_core_respects_cpuset_and_smt_reservation` excluded the control core such a plan named,
  and under load started on another: [Resolved #334](control-core-row-takes-its-own-allocation.md).
  A row reads its cores from its own start since.
- **The machine is still smaller than six heavy clusters.** Europa has fifteen physical cores to
  hand out, and a three-node cluster takes about six, so the clusters of a full round still share.
  They now share evenly and say so.
- **Which loaded failures were this.** The rows that failed under the stacking were looped again
  after the fix ([the flaky page](../flaky-tests.md#runs)); each row that still fails is followed
  there.
- **The test's own threads and a child's client threads are not pinned**, as before.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `two_live_clusters_share_no_core_unrecorded` (`cluster_fixture`) | Two clusters started at once take the same cores one to four, with room for both and nothing recorded |
| `clusters_beside_each_other_take_the_cores_held_least` (`cluster_fixture`) | Beside a cluster holding cores one to four, another takes the same four and records nothing |
| `a_clusters_lease_holds_its_cores_until_it_drops` (`cluster_fixture`) | Two clusters starting from one count take the same cores, or a dropped cluster's cores stay held |
| `cluster_fixture_accounts_for_all_cores_and_endpoints` (`cluster_fixture`) | Unchanged: one cluster alone is allocated as it always was |

## Related

- [F36](../../features/cluster-harness.md), the fixture and its allocator.
- [Resolved #249](an-executor-parked-with-work.md), whose trigger, a thread held past its preempt
  timer, the stacking made common.
- [Tests That Fail Under Load](../flaky-tests.md).
