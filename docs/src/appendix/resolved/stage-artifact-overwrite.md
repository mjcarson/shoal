# 73. Two instrumented runs wrote to one artifact, and the last one won

Filed and half fixed while building [F22](../../features/row-size-benchmarks.md), which is the
change that first needed the stage layer to profile more than one workload. The stage half is
fixed. The hotpath half has the same shape and is still open — see **Still open**.

## Symptom

Nothing, for as long as one workload opted into attribution. A capture that profiled a second one
would have written both reports to the same path, kept whichever ran last, and produced an artifact
indistinguishable from a correct one: right schema, right label, plausible join counts, and silently
describing one workload where it claimed to describe the run.

That is the worst shape a defect of this kind can have. There is no error, no short file, and no
count anywhere that would come out wrong — the missing report simply never existed.

## Cause

`shoal-bench/src/run/plan.rs`, in `build_plan`. Both instrumented phases loop over the workloads
that opted in and hand each of them the **layer's** artifact path, which does not vary with the
workload:

```rust
if wants(Layer::Stages) {
    let mut steps = vec![Step::Command(build(inputs, Some("stage-profile")))];
    for id in &inputs.instrumented {
        steps.extend(wipe_steps(inputs));
        steps.push(Step::Command(workload(
            inputs,
            id,
            vec![
                "--label".to_string(),
                inputs.label.clone(),
                "--json".to_string(),
                scratch_result(inputs, id, 0).display().to_string(),
                "--stage-json".to_string(),
                artifact(inputs, Layer::Stages).display().to_string(),
            ],
```

The line above it does the right thing and shows what the right thing looks like: `scratch_result`
takes the workload id and puts it in the file name, with a comment saying exactly why — *"the
workload is in the name, so a leftover file from another workload cannot be folded into this one's
numbers"*. The stage artifact did not get the same treatment, because at the time there was only
ever one of them.

Underneath it, the artifact **model** could not have held two reports anyway. `StageReport` is a
single report with no field naming the workload it came from, so even a correct writer would have
had nowhere to put the second one.

`PROFILED_WORKLOADS` compounded it. One list drove both instrumented layers, so a workload added
for the stage layer was silently added to the hotpath layer as well — which is a second copy of the
same collision, in a phase nobody asked to grow.

## Evidence

**Reproduced**, not read. A test over `build_plan` with two workloads in the stage layer's list,
run against the unfixed tree:

```
thread 'run::plan::tests::each_staged_workload_writes_to_its_own_artifact' panicked at
shoal-bench/src/run/plan.rs:872:9:
assertion `left == right` failed: two stage runs write to one artifact:
["/repo/docs/perf/runs/L.stages.json", "/repo/docs/perf/runs/L.stages.json"]
  left: 2
 right: 1
```

Note what it took to reproduce: the list had to be forced to two entries. With the shipped list the
same test passes, which is the sense in which this was latent — the defect was in the plan the
whole time and no configuration of the tool would produce it.

## The fix

Three pieces, and the first is the smallest.

**A path per workload.** `scratch_stages` names a scratch file after the workload, exactly as
`scratch_result` does, and each stage run writes there.

The collector is handed **the paths the plan named**, not a glob over scratch. That directory is
created and never cleared, so it holds every run of every capture ever taken in this tree — a glob
would fold a previous capture's stage reports into this one's artifact, keyed under the same
workload names, and the result would look exactly like a correct capture. That is this same defect
one directory up, and it is what the first draft of the fix did.

**An artifact that can hold several.** `StageReports { version, reports: BTreeMap<String,
StageReport> }` is what the layer now writes, keyed by workload. `StageReport` gained a
`workload: Option<String>` so a report says which workload produced it, and
`collect::stages::collect` keys the map on **what the report says** rather than on the file name it
was found under — a file name is something the runner chose and the workload id is something the run
knows.

**A list per layer.** `STAGED_WORKLOADS` sits beside `PROFILED_WORKLOADS`, `Workload` gained
`stage_profiles()` defaulting to `profiles()`, and `PlanInputs::instrumented` became a map keyed by
layer instead of the union of the two.

Version 1 artifacts still read. `StageReports::read` tries the current shape and falls back to a
bare `StageReport`, filed under `macro/insert_unsorted` — the only workload that could have written
one. That is what keeps the nine committed stage artifacts rendering rather than trading them for a
schema change.

## Alternatives rejected

**One file per workload, addressed by globbing.** The obvious fix, and it makes the stage layer the
only layer whose artifact is not a file. A snapshot addresses a layer by name; a reader that had to
enumerate a directory to assemble one layer would be the only such reader in the tool, and
`--check`, the freshness table and the provenance all assume one artifact per layer per capture.

**Merging in the writer.** Each run reads the artifact, inserts its own report, writes it back. It
needs no collector change, and it makes three separate processes take turns read-modify-writing one
file — with no ordering guarantee, no way to tell a stale report from a previous capture apart from
this one's, and a partial write on a crash. The `scratch` directory plus a `Collect` step is the
shape every other layer already uses, and it was right there.

**Keeping one list and letting the hotpath layer grow.** Free to implement, and it would have
tripled the hotpath phase to produce three profiles that mostly repeat each other — the exact cost
`Workload::profiles` exists to avoid. The two layers ask different questions; they get different
lists.

**Bumping `STAGE_REPORT_VERSION` and dropping the old artifacts.** The report's shape did not
change; the artifact's did. Bumping the report version would have refused nine captures over a
change that does not affect a single field any of them holds. Two version constants say two
different things, which is why there are now two.

## Invariants to uphold

- **A per-workload artifact goes in `scratch`, never in the run directory.** Everything in
  `docs/perf/runs/` appears on the freshness table as a capture. Intermediate files do not belong
  there.
- **`collect::stages::collect` is given its inputs and never discovers them.** Scratch is never
  cleared. A directory walk there cannot tell this capture's reports from the last one's.
- **`collect::stages::collect` keys on `report.workload`, not on the file name.** A report that does
  not name its workload is an error rather than a guess: filing one workload's breakdown under
  another's name is the same defect again, one layer up.
- **`PROFILED_WORKLOADS` and `STAGED_WORKLOADS` are the runner's copies of
  `Workload::profiles` and `Workload::stage_profiles`.** The runner links no engine and cannot ask a
  workload anything, so the copies are load-bearing and a test asserts they agree. Both doc comments
  claimed such a test existed before one did.
- **Version 1 stage artifacts must keep reading.** The fallback in `StageReports::read` is not
  legacy cruft to tidy away; it is what nine committed captures depend on.
- **`STAGE_ARTIFACT_VERSION` and `STAGE_REPORT_VERSION` move independently.** Bump the first when
  the file's shape changes and the second when a *stage* changes meaning. Conflating them refuses
  captures for no reason, or accepts ones whose stages mean something else.

## Still open

**The hotpath half.** `Stdout::LastLine(artifact(inputs, Layer::Hotpath))` has exactly the same
collision, and it is not fixed here. Three reasons, in order: `PROFILED_WORKLOADS` still holds one
workload so nothing reaches it; the hotpath artifact is a profile blob written by redirecting the
last line of stdout, so making it hold several means a model change with no caller asking for one;
and a fix to a layer this change does not otherwise touch is speculative work in a change that is
already large. It stays on [Known Issues](../known-issues.md) under this number for that reason, and
`each_staged_workload_writes_to_its_own_artifact` says so where somebody reading the test will find
it.

Whoever adds a second hotpath workload will need to do the same three pieces there. The shape of the
fix is on this page.

## Tests

| Test | What it pins |
| --- | --- |
| `run::plan::tests::each_staged_workload_writes_to_its_own_artifact` | Two stage runs get two paths, and neither is the layer's own artifact. This is the reproduction; it fails against the unfixed plan |
| `collect::stages::tests::the_reports_fold_into_one_artifact` | Several reports become one artifact keyed by workload, and the joins sum |
| `collect::stages::tests::an_unnamed_report_is_refused` | A report that does not say which workload it describes is an error, not a guess |
| `collect::stages::tests::two_reports_for_one_workload_are_refused` | The collision itself, one layer up from the plan |
| `collect::stages::tests::a_report_the_run_never_wrote_is_an_error` | A named-but-missing report fails rather than yielding a shorter artifact — which is what a glob over the never-cleared scratch directory could not distinguish |
| `workloads::tests::the_runners_copy_of_the_profiled_workloads_is_current` | Both runner-side lists are what the workloads actually say, for both layers. Did not exist before this fix, though two doc comments said it did |
| `registry::tests::the_registry_holds_every_layer` | The registry mints one entry per workload per instrumented layer, counting the two lists separately |
| `committed_artifacts::every_committed_stage_report_parses` | Every committed stage artifact still parses — which is the version 1 fallback, since all nine of them predate the new shape |

## Related

- [F22](../../features/row-size-benchmarks.md) — the change that needed this, and why the stage
  layer profiles three row widths
- [F6](../../features/stage-breakdown.md) — what the nineteen stages are
- [F7](../../features/bench-runner.md) — the layer/artifact/provenance model this fits into
- [Known Issues](../known-issues.md) — item 73, the hotpath half, still open
