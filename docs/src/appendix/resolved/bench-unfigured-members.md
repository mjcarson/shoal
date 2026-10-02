# 199. A bench against a cluster on a build from before F65 recorded an empty server series and said nothing

Filed and fixed in one change, from a user's `shoaladm bench` run against the tmdb cluster on the
lab ([F66](../../features/dataset-benchmarks.md)). [Item 200](bench-addr-reads-no-figures.md)
was found and fixed beside it.

## Symptom

The user ran `shoaladm bench run --attach` against the deployed tmdb cluster and watched the stats
view while it ran. The **ops/s by kind** chart on the home tab never drew a line above zero, even
though the bench tab showed the driver inserting about 13,000 rows a second. The capture written
by that run (`target/shoaladm-bench/20261002T032718Z-ba7edf12/bench.json`) has the same gap: in
all nine runs, every sample in `server_series` reads `{"answers_per_sec": {}, "p99_ms": null}`.

## Cause

**The nodes were running a build from before F65**, so they had no per-kind figures to report.
Attach mode drives a cluster as it is deployed and builds nothing:
- the node on hyperion was `/opt/shoal-deploy/tmdb/bin/tmdb-dataset-node`, installed on
  2026-09-29 at 18:52;
- F65's per-kind counters (`QueryMeter`) landed on 2026-09-30 at 04:54.

A node from before F65 leaves `queries` out of its `NodeStats` frame. Every reader of the frame
then falls back to an empty `QueryStats`, so every line on the chart reads zero.

**The defect is that the bench never noticed.**
- `server_sample` (`shoaladm/src/bench/orchestrate.rs`) folded the members' `queries.ops` into the
  sample. With no ops to fold, it recorded an empty map and moved on.
- Nothing in the run, the headless lines or the capture said that a member had no figures.
- The only signal anywhere was the home tab's yellow line, `no query figures from …: a build from
  before F65`. That line is easy to miss, and a headless run never draws it.

So the capture claimed to hold a server series, and nothing in it showed that the series was
missing rather than zero.

## Evidence

**Established by reading the captures and the deployed binaries; nothing was reproduced.**
- The capture's `provenance.nodes.*.program_sha256` is `null`, because attach mode records no
  build.
- The `sha256sum` of hyperion's node binary (`80e2f32b…`) is not the build installed on europa
  (`958bac28…`). The binary's date is ten hours before F65's commit.
- `QueryStats::is_empty` is the one test that tells a build from before F65 apart from an idle node
  (`shoal-proto/src/shared/protocol/stats.rs`). It is true for every member of that cluster's
  answers.

The new unit test then feeds `server_sample` an answer with one member on each kind of build.
Against the unfixed function it does not compile, because the function returned no members to
name. The fix had to change the signature for the test to exist at all.

## The fix

- **`server_sample` returns the members whose figures are empty**, judged by `home_totals`'s
  `older`, the same test the home tab uses.
- **The sampler unions them over the arm.** The first time a set of names is seen, the run logs it
  once to the TUI's log and to the headless lines (`Told`). The line says that the server series
  leaves those members out and that a redeploy fixes it.
- **The capture records them**: `RunResult.unfigured`, `#[serde(default)]`, so older captures still
  load.
- **`bench show` prints them** under the run. `bench compare` notes either side whose series is
  partial.

## Alternatives rejected

- **Refusing the run.** A cluster on an older build is still a cluster to measure: the driver's
  figures are whole, and only the nodes' own series is missing. The user chose a warning and a
  record over a refusal.
- **Comparing the nodes' build with the tool's.** `NodeStats` carries no build identifier; the
  header's "version" is the topology version. That would be the general check, catching the next
  missing figure as well as this one, but it needs a build id in the frame first. Filed in
  [Todos](../todos.md).
- **Warning on every sample.** At one poll every two seconds, that would bury the log. The warning
  is given once per distinct set of members.

## Invariants to uphold

- **An empty figure and an absent one are never written the same way.** `ServerSample`'s empty map
  means "nothing answered" only when `unfigured` is empty.
- **`QueryStats::is_empty` is the only test of a build from before F65.** A new figure that an
  older build leaves out needs its own test, or a build id to compare.
- **A warning that the capture's data is partial reaches a headless run too.** It goes through
  `Progress::log`, never through the screen alone.

## Still open

- Attach mode still records no build for the nodes it drives (`program_sha256: null`). See the
  build id in [Todos](../todos.md).

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `bench::orchestrate::tests::a_sample_names_the_members_without_figures` | A member on a build from before F65 is folded in silently, and nothing names it |
| `bench::orchestrate::tests::a_warning_is_given_once` | The warning repeats every two seconds, or is never given |
| `bench-dataset` `stats::a_run_by_addr_records_the_nodes_answers` | A node on the current build is reported as unfigured |

## Related

- [F65](../../features/query-figures-home-tab.md), the figures the nodes were missing.
- [F66](../../features/dataset-benchmarks.md), the bench.
- [F67](../../features/bench-run-wizard.md), which holds every metric of the stats view to a real
  node.
- [Item 200](bench-addr-reads-no-figures.md), the other half of the empty-chart report.
