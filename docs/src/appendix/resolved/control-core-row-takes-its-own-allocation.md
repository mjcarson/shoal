# 334. The control core row excluded the control core a plan made alone named, not the one its start gave

`control_core_respects_cpuset_and_smt_reservation` (`shoal/tests/cluster_fixture.rs`) holds that a
child whose affinity excludes its control core is refused by name before a shard starts
([F37](../../features/node-identity-control-plane.md)). It narrowed the child's affinity to every cpu
but its control core's and expected the start to be refused.

Found in the flaky-test plan's first workspace run after the confirmation loops (2026-10-28), and
reproduced.

## Symptom

```text
thread 'control_core_respects_cpuset_and_smt_reservation' panicked at shoal/tests/cluster_fixture.rs:891:18:
a control core outside the affinity started
```

## Cause

The row learned the control core to exclude from a plan made alone, `Cluster::builder().plan(&Topology::detect())`,
and then started the cluster. Since [Resolved #328](clusters-beside-each-other.md) a start takes the
cores the process's other clusters hold least (`cores::lease`), and a plan made alone holds nothing
and knows nothing of them. Beside a running cluster the start gave the node another control core,
inside the narrowed affinity, and the node started. Item 328's fix made the row depend on running
alone, which a workspace run at six threads never is.

## Evidence

**Established by reading the row, then reproduced** by starting one cluster before the row's last
part, which holds the cores a plan made alone names. On the row before:

```text
thread 'control_core_respects_cpuset_and_smt_reservation' panicked at shoal/tests/cluster_fixture.rs:893:18:
a control core outside the affinity started
```

With the fix and that cluster still beside it, the row passes.

## The fix

**The fixture narrows a node's affinity from its own allocation** (`ClusterBuilder::affinity_without_control`,
`NodeSpec::affinity_without_control`): once the start has leased the node's cores, the child is
given the process's cpus but both threads of the control core it was given. The row starts with
it, and a node given no control core, on a machine too small to isolate one, says nothing more, as
before.

## Alternatives rejected

- **Plan beside the held cores** (`plan_beside`). Another test's cluster can start or stop between
  the plan and the start, so the race narrows and stays.
- **Narrow the affinity to the data core alone.** The start would be refused for its data core
  rather than its control core, and the refusal the row is about would go untested.

## Invariants to uphold

- **A row that needs a node's cores reads them from the start that leased them**, never from a plan
  made apart from it.

## Still open

Nothing.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `control_core_respects_cpuset_and_smt_reservation` (`cluster_fixture`) | Beside another cluster, the node's control core is not the one excluded, and it starts |

## Related

- [Resolved #328](clusters-beside-each-other.md), the allocator among live clusters.
- [Tests That Fail Under Load](../flaky-tests.md#left-the-page).
