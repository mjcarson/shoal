# F7. A benchmark runner that renders its own results

## Context

[F3](performance-harness.md) built three measurement layers and [F6](stage-breakdown.md) added a
fourth. All four were driven by three bash scripts — `scripts/bench.sh`, `scripts/collect-micro.sh`
and `scripts/compare.sh` — which captured JSON into `docs/perf/` and stopped there. Five things
followed from that, and none of them was a scripting problem that a better script would have
fixed:

- **Nothing read the artifacts back.** `../performance/baseline.md` carried around twelve
  tables of numbers transcribed by hand out of files sitting in the repository.
- **Only the micro layer could be compared.** `compare.sh` diffed two maps of criterion estimates.
  The layer that measures what a client actually experiences was captured five times per run,
  committed, and never compared to anything.
- **A committed number could not say whether it still described the current code.** There was no
  record of the commit, the tree state, the machine or the toolchain a capture came from. A number
  in `docs/perf/runs/` was either believed or not, and nothing said which it should be.
- **It was all or nothing.** `bench.sh <label>` ran four layers over 59 criterion ids and took about
  thirteen minutes. There was no way to run the eight benchmarks an optimization could actually
  move.
- **`.stages.json` had never been committed**, despite `bench.sh` producing one, because nothing
  read it and nobody noticed.

`todos.md` had already filed the comparison gap, and was blunt about why it mattered: the
confirming repeat the protocol requires "is a manual step today, which means it is a step that will
be skipped."

## What it does

`shoal-bench` is a workspace crate with six commands.

| Command | What it does |
| --- | --- |
| `list [FILTER…]` | prints the benchmarks a filter selects, running nothing |
| `run --label <name> [FILTER…]` | captures the selected layers and stores them with their provenance |
| `compare <label>` | judges a capture against the frozen baseline and the trailing one |
| `status` | says what has been captured and whether each layer still describes the current code |
| `render` | regenerates every page under [Performance](../performance/overview.md); `--check` verifies them |
| `promote <label>` | advances a baseline to a capture |

**Filtering works the way `cargo test` does.** A positional argument is a substring of a
benchmark's identifier, any of them matching selects it, `--exact` switches to equality, and
`--layer` intersects. The micro list is *discovered* by asking criterion — `cargo bench -- --list`
measures nothing — rather than written down, so a benchmark added to `shoal/benches/partitions.rs`
is selectable immediately. A filter matching nothing is an error that names the three closest ids.

Since [F8](purpose-built-workloads.md) the workload list is *not* discovered, for the opposite
reason: the workloads are compiled into this crate, so `workload_ids::IDS` is the list and there is
nothing to go and ask. A test under the `workloads` feature asserts that list equals what
`workloads::all()` registers, so the two cannot drift.

**Every capture records what it was taken on**, in `docs/perf/runs/<label>.meta.json`: the commit
and whether the tree was dirty, a content hash of the sources each layer measures, and the host,
CPU, governor, compiler and rustflags. `status` and the generated page turn that into a per-layer
verdict of `fresh`, `unaffected`, `stale`, `uncommitted`, `diverged` or `no provenance`.

**The macro layer is compared for the first time**, on interval disjointness rather than a
percentage, and each capture now keeps every run's own distribution rather than only its wall
clock.

**The results page is generated**, charts and all, from the committed artifacts.

## Design choices

**Criterion is driven, not replaced.** `shoal/benches/partitions.rs` and criterion's sampling are
untouched; `shoal-bench` spawns `cargo bench`, parses `target/criterion/**/new/estimates.json`, and
keys the result on criterion's `full_id` verbatim. That is what keeps
`docs/perf/baselines/B1-performance.json` — captured before this crate existed — directly
comparable. A tool that changed how the numbers were sampled would have invalidated every number in
the tree on its first run.

~~**The crate depends on nothing else in the workspace.** Not `shoal`, not `shoal-core`. It is a
tool over committed artifacts and subprocess output.~~ **The half of it that judges a capture
still does not**, which is what that invariant was actually protecting.
[F8](purpose-built-workloads.md) moved the workloads into this crate behind a default-on
`workloads` feature, because a benchmark that links `shoal` is a benchmark an API change breaks
loudly. `cargo build -p shoal-bench --no-default-features` still builds `compare`, `status`,
`render` and `promote` with no glommio anywhere, which is what lets a capture be judged while the
engine is mid-refactor and will not compile.

The original alternative — reusing `shoal::bencher::BenchResult` — was rejected then and is moot
now: F8 deleted that struct. The artifact's types are defined once, in this crate, and built by
the workloads themselves, so the mirror this crate used to keep against it is gone too.

**Provenance lives in a sibling file, not inside the artifacts.** It describes the *capture*, not
the micro numbers. Keeping it out means `<label>.micro.json` stays byte-shape compatible at
`"version": 1`, so the frozen baseline needed no version bump and no migration.

**The macro band is interval disjointness.** Each side has an observed interval across its runs; a
difference is a result only when the two do not overlap, and the effect reported is the gap between
the nearest endpoints, which is a conservative lower bound. A percentage would be wrong: the frozen
baseline spread 10.5% across five identical runs, wider than most changes worth making. This is not
a new standard — it is exactly the test [F5's](flushed-sweep-gate.md#performance) interleaved A/B
was judged by when it was run by hand. The tool now applies the standard the documentation already
described.

**Charts are drawn in sentinel colours and themed by CSS.** plotters has no notion of a theme and
emits fixed `#RRGGBB` presentation attributes, and the book has five themes and defaults to a dark
one. So the charts are drawn in recognisable sentinel colours and `docs/theme/charts.css` maps each
onto one of mdbook's own variables with an attribute selector. A presentation attribute loses to any
author stylesheet rule *by specification*, so the override is guaranteed without `!important`, and
`light-dark()` resolves against the `color-scheme` mdbook sets per theme, so the charts follow the
reader's theme picker at runtime.

**The page is committed and verified rather than built on demand.** `create-missing = false` means a
`SUMMARY.md` entry for a missing file fails the whole book build, and regenerating the page needs a
twelve-core machine, a populated `/opt/shoal` and a 65 MB dataset that is not in the repository. A
clean checkout has to be able to build the book. `render --check` is what says the committed page is
current.

**The wipe guard requires a marker.** The storage directory is emptied before every macro run, so
the tool issues a recursive delete against a path read out of a config file. `bench.sh` guarded that
by checking the path was neither empty nor `/`, which would have cheerfully emptied a home
directory. `shoal-bench` requires the directory to be empty or to carry the `shoal-meta.json` the
server writes, and refuses a relative path, a symlink, or anything within two components of the
root.

**`run` refuses a dirty tree** unless given `--allow-dirty`, and records that it was used. A
measurement of bytes that exist in no commit cannot be located in history afterwards.

## Alternatives rejected

**Replacing criterion with a native sampler.** Considered and rejected first, because it invalidates
the frozen baseline. Every existing number would have had to be recaptured before any new one could
be believed, which is a large cost paid up front for a benefit — a registry declared in Rust — that
the `--list` discovery gets anyway.

**A directory per capture, `runs/<label>/micro.json`.** Reads better in `ls`. Rejected because
freezing a baseline means freezing its path too: any grouping scheme either moves
`baselines/B1-performance.json` or leaves it as the single exception, and thirty-two committed
artifacts are referenced by exact path in the commit messages that justified them.

**Depending on `shoal` for `BenchResult` and `StageReport`.** See above. The accepted cost is that
`shoal-bench` carries mirror structs that can drift from what they mirror; the mitigation is a
catch-all field on `MacroCapture` and a test asserting it comes back empty over every committed
artifact, so a field added to `BenchResult` fails a test that names it.

**Emitting `fill="var(--fg)"` from the generator.** The obvious way to theme an SVG, and it does not
work: CSS variables in SVG presentation attributes are not reliably supported. It would also have
put the theme mapping in Rust string constants rather than in a reviewable stylesheet.

**`{{#include}}` fragments in `../performance/baseline.md`.** Converting that page's hand-transcribed
tables to generated fragments was the obvious way to remove the duplication, and was rejected for
three reasons. The page is a narrative about a frozen moment, pinned to `B1` on purpose. It is full
of strikethrough annotations that a generator would either destroy or need to model. And decisively:
`{{#include}}` of a missing file **does not fail the book build**, unlike a missing `SUMMARY.md`
entry — it inlines an error string. That trades a page that is merely hand-maintained for one that
can silently degrade to error text, on the page that is the authority on every number in the
project. The page got a pointer to the generated one instead.

**A date dependency.** `chrono` is not in the workspace lockfile, and `time` — which is — pulls
`time-macros` in behind the `formatting` feature it would have needed. The whole requirement was one
civil-date conversion, so `shoal-bench/src/clock.rs` does it in forty tested lines and the crate adds
**zero packages** to `Cargo.lock`.

**`git2`.** Three `git` subprocesses against a 400-kLOC dependency. Likewise `regex`: the criterion
filter is *built* and never parsed, so the only need was `regex::escape`, which is eighteen
characters and a unit test.

**Hashing the built binary instead of the sources.** It would remove the hand-maintained source list
entirely. Rejected because a release build is not reproducible enough for the hash to mean anything —
it would report every capture as stale, which is the same as reporting none of them.

**Using criterion's confidence interval as the screen.** Inherited from `compare.sh` along with its
reasoning, which is ported verbatim into the module docs on `compare/micro.rs`. That interval
describes stability inside one process; everything that differs between two processes is invisible
to it. Across four identical repeats `get_key/4096` moved 22% while reporting its outlying value
with a ±0.2% interval.

## Limitations

- **`docs/perf/sources.json` is a hand-maintained approximation and will drift.** The design makes
  drifting safe in one direction only, and the whole staleness feature rests on that: the commit
  hash is checked first and catches every change coarsely, and the per-layer digests can only ever
  *narrow* a `stale` verdict to `unaffected`, never turn `stale` into `fresh`. A path missing from
  the manifest therefore produces a capture wrongly called unaffected — which still shows the commit
  distance — and never one wrongly called fresh. A `--strict-stale` mode is filed in
  [TODOs](../appendix/todos.md#benchmark-coverage-the-harness-does-not-have).
- **Macro percentiles have no error bar on any capture taken before this feature.** `bench.sh` kept
  only each run's wall clock, so the percentiles of the run it chose cannot acquire an interval
  after the fact. Those comparisons report `no error bar` and are never called a result. The gap
  closes only as new captures accumulate.
- **The seven pre-existing captures have no provenance at all**, and are reported as such rather
  than being guessed at.
- **The micro layer still takes one capture per side.** The confirming repeat the protocol requires
  is still a manual step.
- **plotters is built without a font backend**, so it estimates text extents rather than measuring
  them. Labels can collide when a chart's data changes shape; `tests/chart_geometry.rs` catches a
  collision in a column and gross overflow, and cannot catch everything a pair of eyes would. Since
  [F19](chart-legends.md) the estimate decides a legend column's width rather than where a label
  lands, so being wrong is a gap rather than an overlap — but it is still an estimate.
- **The generated diffs are unreadable.** ~~It is 110 KB, most of it SVG path data.~~ It reached
  **1.1 MB** on one page before [F18](results-pages.md) split it into ten and dropped the
  per-workload chart wall; it is 529 KB across those ten now, and still mostly SVG path data. Reviewers are expected
  to read `render --check` and the tables, not the hunks. It is deliberately *not* marked `-diff` in
  `.gitattributes`, because that would hide real changes too.
- **`render --check` fails after any commit.** Every page states which commit it was rendered
  against and every staleness verdict on it is relative to that commit, so this is a page reporting
  that it no longer describes the tree — but it does mean the check cannot be a blanket CI gate.
- **`render` writes every page or none.** A tree holding four current pages and six stale ones is
  worse than one holding ten stale ones, because nothing on a page says which kind it is. There is
  deliberately no `--page` flag.
- **A partial capture cannot be promoted** without `--force-partial`.
- **Three of the eight series colours fall below 3:1 contrast on the book's light themes**, and four
  on `rust`. That is permitted only under the relief rule, which holds because every chart on the
  page ships beside a table carrying the same numbers. Adding a chart without a table breaks it.

## Invariants to uphold

- **The uninstrumented rebuild happens on every path out of a capture, including a failed one.**
  This is why it is a field on the plan rather than the last item in the phase list: a list can be
  truncated by an error and a field cannot. Without it the tree is left holding whichever profiling
  build ran last, and the next manual run measures an instrumented binary while looking exactly like
  a normal run.
- **Micro benchmark ids stay byte-identical to criterion's `full_id`.** They are the keys the
  artifacts are written under and the keys a comparison joins on. Change how they are derived and
  every baseline captured before the change stops matching — silently, as a set difference rather
  than as an error.
- **`layer_digest` may only narrow a verdict, never widen one, and is never consulted before the
  commit hash.** See the first limitation; reversing that order makes a hand-maintained list load
  bearing.
- **The frozen baseline is never written.** `promote` refuses it with no override.
- **Hotpath and stage numbers are never quoted as a latency or a throughput.** Both come from builds
  that add work to the measured path. They attribute time; they do not measure it.
- **`hotpath`'s `percent_total` is never plotted or tabulated.** It is not normalised across
  concurrent scopes — the committed `B1` profile reports one at 12,530%. Rank by `total`. Filed as
  [known issue 53](../appendix/known-issues.md).
- **The four determinism rules**, without which `render --check` fails on a tree nobody touched: no
  wall clock reaches the page; every map walked while rendering is a `BTreeMap` and every list is
  explicitly sorted; every float goes through `fmt` at a fixed precision; chart geometry is a pure
  function of the data and the fixed canvas size. Two consequences of that rule are easy to undo by
  accident: the page reports the tree as dirty or clean rather than counting the paths, because a
  count churns with every unrelated file and would fail `--check` for reasons that have nothing to
  do with the benchmarks; and the page is excluded from its own dirty check, because otherwise
  writing it changes what it says about itself.
- **Every palette sentinel has a `fill` and a `stroke` rule in `docs/theme/charts.css`, and every
  rule there matches a sentinel.** A sentinel with no rule is drawn literally — bright red on a navy
  page. `tests/css_sync.rs` checks both directions.

## Performance

Capturing takes as long as it did before, because the work is identical: about thirteen minutes for
all four layers at the default settings, of which the criterion phase is the long pole.
`--limit 20000 --runs 2 --layer micro` is about one minute.

Everything that does not run a benchmark is sub-second on the committed corpus: `list` (from cache),
`compare`, `status` and `render` all complete in well under a second over 32 artifacts, and `render`
draws five charts while doing it. `list` on a cold cache costs one `cargo bench --list`, which has to
build the bench target.

The crate adds **zero packages** to `Cargo.lock`. `cargo build -p shoal-bench` compiles in a couple
of seconds and does not rebuild when the database changes.

## Tests

| Test | Breaks if |
| --- | --- |
| `committed_artifacts::every_committed_micro_capture_parses` | a micro artifact stops being readable, or its schema version moves |
| `committed_artifacts::no_macro_capture_has_an_unmirrored_field` | `shoal::bencher::BenchResult` grows a field the mirror does not have — fails naming it |
| `committed_artifacts::the_two_baselines_disagree_by_the_maybe_loaded_group` | the 24-benchmark difference between the frozen and trailing baselines changes silently |
| `committed_artifacts::a_name_resolves_to_a_capture` | a baseline stops winning over a run of the same name |
| `collect::micro::tests::a_stale_result_is_not_collected` | criterion output from an earlier capture can leak into a new one |
| `collect::micro::tests::a_saved_baseline_is_not_collected` | `--save-baseline`'s sibling directory is mistaken for this run's results |
| `compare::micro::tests::the_tier_is_chosen_from_the_baseline_not_the_run` | a change can select the band that judges it |
| `compare::micro::tests::the_tier_boundary_is_where_it_says_it_is` | the 1 µs tier boundary moves |
| `compare::micro::tests::set_differences_are_reported_both_ways` | a benchmark can vanish from a comparison unreported |
| `compare::macro_layer::tests::two_real_overlapping_captures_are_not_a_result` | the macro band stops being interval disjointness — B1 vs o17 differ by 1.7% and are not a result |
| `compare::macro_layer::tests::a_single_run_has_no_error_bar` | a one-run capture is screened as though it had an interval |
| `stale::tests::a_digest_can_only_narrow_a_verdict` | the source digest can promote a capture to `fresh` |
| `stale::tests::uncommitted_outranks_every_other_verdict` | a capture taken on a dirty tree is reported as fresh |
| `stale::tests::dirt_in_another_layer_does_not_spread` | one layer's uncommitted sources condemn another layer |
| `run::plan::tests::the_restore_is_last_and_is_not_a_phase` | the uninstrumented rebuild can be skipped by a failure |
| `run::plan::tests::a_full_capture_matches_the_five_phases` | the phase order drifts from what the runbook documents |
| `run::plan::tests::every_run_passes_no_wait` | the example is run without `--no-wait` and hangs on stdin |
| `run::storage::tests::a_populated_directory_without_a_marker_is_refused` | the wipe guard stops requiring a Shoal store |
| `run::storage::tests::a_symlink_is_refused` | the wipe follows a symlink into its target |
| `registry::tests::a_partial_selection_becomes_an_anchored_alternation` | a filter stops being anchored and a short id matches inside a long one |
| `registry::tests::the_real_ids_need_no_escaping` | a benchmark is named with a regex metacharacter |
| `render::chart::tests::the_plotters_root_is_what_we_strip` | a plotters upgrade changes its root element and every chart goes out unthemed |
| `render::chart::tests::no_unthemed_colour_escapes` | a chart is drawn in a colour the stylesheet cannot theme |
| `render::chart::tests::no_coordinate_is_nan` | an empty series or a log axis touching zero produces an invisible chart |
| `render::page::tests::the_page_carries_no_wall_clock` | a timestamp reaches the page and `--check` starts failing at random |
| `css_sync::every_sentinel_is_themed_for_fill_and_stroke` | a sentinel is added in Rust without a stylesheet rule |
| `css_sync::the_stylesheet_is_registered_with_the_book` | `additional-css` is dropped from `book.toml` and every chart loses its theme |
| `chart_geometry::no_two_labels_share_an_anchor` | two chart labels are drawn on top of each other |
| `chart_geometry::stacked_labels_have_room` | a row height or tick count leaves labels touching |
| `clock::tests::a_whole_gregorian_cycle_round_trips` | the hand-rolled date conversion is wrong on any day of a 400-year cycle |

**What is not tested: actually running a benchmark.** It needs twelve cores, a writable
`/opt/shoal` and a 65 MB dataset that is not in the repository, so no test can do it. `run` is
covered through `--dry-run`, which prints the exact argv of every phase and writes nothing — the
same plan a real capture executes, with the execution left out. Execution was verified by hand.

## Related

- [Benchmarking](../performance/benchmarking.md) — the runbook, rewritten around these commands
- [Benchmark Results](../performance/overview.md) — what this feature generates
- [Performance Baseline](../performance/baseline.md) — the frozen `B1` capture
- [F3, the performance harness](performance-harness.md) — the layers this drives
- [F6, the stage breakdown](stage-breakdown.md) — the fourth layer, whose artifact this finally commits
- [Known issue 53](../appendix/known-issues.md) — `hotpath`'s unnormalised `percent_total`
- [TODOs](../appendix/todos.md#benchmark-coverage-the-harness-does-not-have) — the micro half of
  multi-capture comparison, and the source-manifest drift
