# 200. A bench run by `--addr` never read the node's own figures

Filed and fixed in one change, beside [item 199](bench-unfigured-members.md), while tracing why the
stats view's ops/s by kind chart was empty during a bench
([F66](../../features/dataset-benchmarks.md)).

## Symptom

A `shoaladm bench run --addr <node>` run never read the node's own figures:
- On a terminal, the stats view drawn beside the run said "waiting for the first answer" on every
  chart for the whole run.
- In every mode, every run in the capture had an empty `server_series`.
- Nothing said why.

## Cause

Both places that read the nodes' figures needed a deployment:
- `hand_poller`, which hands the screen its reader, returned early with the comment "one node by
  hand, or no screen, has nothing to read".
- The per-arm sampler in `run_arm` was built from `admin`, a client the run makes only when there
  is a deployment.

A node named by `--addr` has no deployment, so neither ran. The comment was true of a standalone
node, which keeps no figures, because only the control plane answers `AdminKind::Stats`. It was not
true of a cluster member started by hand, which answers the read like any other member.

## Evidence

**Reproduced.** `bench-dataset`'s `stats::a_run_by_addr_records_the_nodes_answers` starts a
cluster of one node in process and initializes it the way a deployment does. It then runs
`bench run --addr` at it with rw50 and asserts that the capture recorded the node's gets and
inserts. Against the unfixed tree:

```text
test a_run_by_addr_records_the_nodes_answers ... FAILED
thread 'a_run_by_addr_records_the_nodes_answers' panicked at examples/bench_dataset/tests/stats.rs:162:5:
the node's answers were never recorded: []
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 9.73s
```

## The fix

- **`Cluster::poller` builds a reader of the figures for every kind of cluster.**
  - For a deployment: through any member, as the admin, with the members named as deployed. This is
    what both places did before.
  - For `--addr`: through the address, with no credentials, the way the driver connects.
  - `hand_poller` and the sampler both use it.
- **A read that never answers is said once and recorded.** A standalone node refuses the read, as
  does a node that requires credentials the bench does not have. The run logs why once, and the
  capture records it as `RunResult.figures_unread`, which `bench show` and `bench compare` print.
- **The sampler's wait ends when its arm does.** A sampler that slept out its two seconds after the
  arm ended would have added up to two seconds to every run of an `--addr` capture. It now waits on
  a `Notify` beside the sleep. `bench_run`'s whole run takes the same 59 s it took before the fix.

## Alternatives rejected

- **Sampling only when the node is known to be a cluster member.** Asking costs one refused read,
  and it is the only way to know. A pre-check would be the same read under another name.
- **Credentials for `--addr`.** A node started by hand with authentication required still cannot
  be read. That is recorded rather than worked around: a flag for the admin's password is a feature
  of its own, and nobody has asked for it.
- **Retrying a refused read less often.** A refused read is cheap, and a cluster that comes back
  mid-run is read again at once.

## Invariants to uphold

- **Every cluster a run can drive gets a reader of its figures**, or a recorded reason why none
  answered. An empty `server_series` with no `figures_unread` and no `unfigured` means only that no
  sample was taken.
- **No arm waits for the sampler.** Stopping it wakes it.

## Still open

- `--addr` has no way to name credentials, so a node that requires authentication is never read.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `bench-dataset` `stats::a_run_by_addr_records_the_nodes_answers` | The capture's server series is empty for a cluster node reached by `--addr` |
| `bench-dataset` `stats::every_metric_that_should_move_does` | The figures are read off the same node beside a real run; it fails if the node's figures stop reaching the view |
| `bench-dataset` `bench_run::a_run_against_one_node_writes_a_capture_that_compares` | A standalone node's runs no longer say why they read no figures |

## Related

- [Item 199](bench-unfigured-members.md).
- [F64](../../features/stats-tui.md), the stats view the bench draws.
- [F67](../../features/bench-run-wizard.md), whose test reads the same node's figures beside a run.
