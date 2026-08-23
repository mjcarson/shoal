# F21. Benchmark groups

## Context

A full capture is four to five hours, and [F20](configuration-sweeps.md) adds another hour and a
half to it. Long before that, the practical question stopped being "run the benchmarks" and became
"run the part of the benchmarks that could possibly answer this".

*Both figures above were guesses, and this page was written before anybody checked them.* `F20-conf`
timed a full capture at **seventy-five minutes** on 2026-08-22 — the first one anybody measured —
and [F22](row-size-benchmarks.md) then added 165 arms, putting it at roughly two hours. The argument
this page makes does not change; the numbers in it were never measurements, which is the same thing
`FULL_MACRO_CAPTURE_SECS` is still true of.

[F7](bench-runner.md) gave the runner `cargo test`'s filtering: a positional argument is a substring
of an identifier, `--exact` switches to equality, `--layer` intersects. That is the right primitive
and it is a poor way to express an *intention*. Three things go wrong once the registry is 307
entries:

**A prefix is not discoverable.** Nothing enumerates the useful ones. That `macro/conf/storage/` is a
coherent set — and that `macro/grid/` plus `macro/skew/` is one, and `macro/grid/depth/` separately
is another — is knowledge held outside the tool, in this book if anywhere.

**A typo selects the wrong set silently.** `macro/conf/storag` matches nothing and errors, which is
fine. `macro/conf` matches all forty-eight configuration arms when twenty-three were wanted, and a
capture that measured twice what was asked for is indistinguishable from one that measured the right
thing until the bill arrives three hours later.

**A useful set is often not one prefix.** "Everything that isolates one path" is every macro workload
*except* the grid, the skew sweep and the configuration sweeps — a set that cannot be written as a
substring at all, and the set somebody wants precisely when they are attributing a regression.

CLAUDE.md had accumulated the recipes as prose: `--layer macro macro/grid macro/skew` runs the grid
alone, and "a filter excluding it" runs everything else, which is not a thing the tool can do.

## What it does

**Twelve named groups**, declared in `shoal-bench/src/groups.rs`, each answering one question:

```
$ shoal-bench list --groups
GROUP             BENCHES   SHARE  ~CAPTURE  WHAT IT ANSWERS
quick                  12     11%    30m45s  does everything still run - one arm from each family, not a measurement
micro                  98       -         -  did the partition internals or the wire format move
attribution             2       -         -  where the time inside one query goes - never a latency or a throughput
controls               23      9%    23m49s  the storage-free pairs - what persistence costs, with everything else held
fanout                 18      2%     4m56s  what reading many partitions at once costs, as a curve in the key count
transport              64     36%     1h37m  what the client's streaming mode and the wire's encryption cost
isolating              87     45%      2h1m  which path moved - every workload that drives one path and only one
conf/storage           23       -         -  what each filesystem writer setting is worth
conf/resources         25       -         -  how the server scales with cores and memory, and what the frame bound costs
conf                   48       -         -  what every configuration setting is worth - both halves of the sweep
grid                   74     55%     2h28m  what a caller's mixture costs, across row width, read share, skew and depth
macro                 209    100%     4h30m  every workload that runs against a live server
```

**`--group <NAME>`, repeatable, on `list` and `run`.** Several groups combine with **or**; the result
intersects with `--layer` and with the positional filters, the same way those two intersect with each
other. So `--group conf durability` is the two durability arms, and `--group conf/storage --group
fanout` is both sets.

**An unknown group is an error that names the real ones**, in the shape the unmatched-filter message
already had:

```
$ shoal-bench run --label x --group conf/storag
error: unknown group 'conf/storag'
  groups are: quick, micro, attribution, controls, fanout, transport, isolating, conf/storage, conf/resources, conf, grid, macro
```

**A group selects. It never schedules.** Nothing here makes anything run concurrently, and that is a
deliberate non-feature — see below.

## Design choices

**A group is expressed over identifier shapes, not over workloads.** `Members` carries layers,
prefixes, exclusions and an explicit list, and nothing else. The reason is the same one
[`workload_ids::IDS`](purpose-built-workloads.md) exists for: the runner half of `shoal-bench` builds
with `--no-default-features` and no engine at all, and `list` has to work there. It is also why
[F20](configuration-sweeps.md) puts its section into its identifier — `conf/storage` is a prefix
check here because the identifier was built to make it one.

**The vocabulary stops at four fields.** Every set anybody has wanted turns out to be "these layers",
"these prefixes", or "these prefixes but not those". `excluding` is what makes `isolating`
expressible, and it is the only reason that field exists. Growing this into a query language would be
building a worse `--filter`.

**`quick` names its members explicitly.** One arm per family, hand-picked, and
`the_quick_group_names_only_real_benchmarks` checks every one against the registry. A hand-picked set
is a hand-picked set; expressing it as a pattern would make it look like it generalises, and a
workload renamed out from under it would leave the group quietly smaller rather than failing.

**The listing prints a share, not a duration.** The measured phases of every macro workload add up to
about two and a half minutes; the capture they come from takes four and a half hours, because
seeding, building and starting a server per arm dominate and **no artifact records any of it**.
Printing the sum as a duration would be wrong by two orders of magnitude. So the listing computes
each group's share of a whole capture's measured time and projects it onto
`FULL_MACRO_CAPTURE_SECS`, a named constant with the observed figure in it.

**A group with nothing captured shows `-`.** The three `conf` groups do, until the first capture that
contains them. Estimating an uncaptured workload from the mean of the captured ones would be a guess
wearing an artifact's clothes, and the groups people most want to estimate are exactly the new ones.

**Groups are declaration-ordered, cheapest first.** Which is also roughly the order to reach for
them: prove everything runs, narrow to the question, then take the wide ones.

## Alternatives rejected

**No new flag — write the prefixes down.** What CLAUDE.md was already doing. It cannot express
`isolating`, nothing enumerates what exists, and a prefix that selects a superset fails silently
three hours later. The documentation-only option is the status quo that prompted this.

**A group that also pins `--runs` and `--scale`.** `--group quick` implying `--scale smoke --runs 2`
is convenient and makes a group name mean two things: a set of benchmarks and a way of running them.
A capture's provenance would then have to record which preset produced it, or two captures under one
label would not be comparable. The scale and the run count stay where they are, on the command line
and in the artifact.

**Deriving groups from the render families.** `render/family.rs` already partitions every workload
into eleven families, and reusing them would guarantee the two never drift. They answer a different
question: a family is "which page explains this arm", so `grid` and `row-size` and `tables` are three
families drawing one set of workloads from three angles, and `depth` is its own family of four arms
that nobody would capture alone. `isolating` and `quick` correspond to no family at all.

**Running a group's members concurrently.** Rejected outright, and it is the reason to read the next
section rather than a footnote here.

## Limitations

**A group is a set, not a schedule.** There is no way to say "these, in this order" or "these, but
stop after an hour".

**The estimate assumes `--runs 5`** and says so. It also cannot cost the micro layer at all, because
criterion never wrote a duration into the artifact.

**`FULL_MACRO_CAPTURE_SECS` is a constant somebody has to update.** If a capture gets slower, every
projection is proportionally wrong until the number is changed, and nothing detects it.

**Nothing checks that the groups cover the registry.** A workload in no group is not an error — the
declared sets are the useful ones, not a partition — so a family added without a group is reachable
only by prefix.

**A group's membership can drift from its summary.** `every_group_selects_something` checks a group
is non-empty; nothing checks that `isolating` still describes what it holds.

## Invariants to uphold

**A group selects and never schedules.** `run/plan.rs` stays workload-outer and run-inner, with one
`shoal-workload` process at a time. Two servers running at once share a page cache, a device queue
and a set of cores, so every number either produced would be a number about the other one as well —
and the whole corpus would silently stop being comparable to everything captured before. The
port-per-workload scheme exists so an arm binds the same port in *every* capture, which makes a
capture reproducible; it is not headroom for parallelism.

**Group names are stable.** They appear in this book, in CLAUDE.md and in whatever anybody has in
shell history. Renaming one is a documentation change as well as a code change.

**`groups.rs` links no engine.** It is compiled by `--no-default-features`, like `workload_ids.rs`.
An import from `workloads::` here breaks `list` for the half of the crate that judges a capture.

**`quick`'s members are real identifiers**, checked by a test, because they are named rather than
matched.

## Performance

None. The group filter is a prefix check per registry entry, run once per invocation on 307 entries.

The point is the capture cost it avoids: `--group conf/storage` is 23 arms instead of 253.

## Tests

| Test | What breaks if the feature is reverted |
| --- | --- |
| `groups::every_group_selects_something` | A group matches nothing and `--group` blames the caller for a mistake in the table |
| `groups::every_group_name_is_unique` | `--group` resolves to whichever duplicate is declared first |
| `groups::the_quick_group_names_only_real_benchmarks` | A renamed workload leaves `quick` quietly smaller |
| `groups::the_conf_halves_partition_the_sweep` | An arm is in both halves or neither, so two captures do not add up to one |
| `groups::the_isolating_group_excludes_every_mixture` | A regression gets attributed to a grid arm, which is what [F8](purpose-built-workloads.md) forbids |
| `groups::groups_are_combined_with_or` | Two groups intersect to nothing instead of combining |
| `groups::an_unknown_group_is_reported` | A mistyped group falls through to selecting everything |
| `groups::a_duration_reads_out_loud` | The listing prints raw nanoseconds |
| `registry::a_group_restriction_intersects` | `--group` replaces the filters instead of narrowing them |
| `registry::an_unknown_group_is_an_error` | The same, at the layer that actually runs a capture |

## Related

- [F20. What each setting is worth](configuration-sweeps.md) — the sweep that made this necessary
- [F7. The benchmark runner](bench-runner.md) — the filtering this extends
- [F8. Purpose-built workloads](purpose-built-workloads.md) — why `groups.rs` cannot see the workloads
- [Benchmarking](../performance/benchmarking.md) — the runbook, including which group to take when
