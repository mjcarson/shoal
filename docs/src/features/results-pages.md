# F18. Results pages that explain themselves

## Context

`docs/src/operations/benchmark-results.md` reached **13,617 lines and 1.1 MB**. Ninety percent of
that was eighty-eight near-identical inline SVG charts, drawn one per workload identifier in
alphabetical order, each under a heading that was the raw identifier and each with the same sentence
of caption. Alphabetical order put the forty-eight arms of the newest, narrowest experiment first —
`macro/encryption/clients/plain/1048576/1` and its neighbours — and buried `macro/insert_unsorted`
at line 8,559.

Two of its nine sections consisted of one sentence saying the current capture had no data, sitting
directly below the charts that did have it. Nothing anywhere on the page said how to read any of it:
a reader who did not already know what a fan-out curve was, or that a `per_batch` percentile is not
a service time, could not find out from the page that drew one.

[F7](bench-runner.md) had already recorded the diff half of this as a limitation — *"the generated
page's diffs are unreadable. It is 110 KB, most of it SVG path data"* — at a tenth of the size it
eventually reached. And [F17](workload-grid.md) was about to add seventy-four more workloads, which
under the old renderer meant seventy-four more alphabetically-sorted charts.

## What it does

`shoal-bench render` writes ~~**ten pages**~~ **eleven pages** under `docs/src/performance/` instead
of one under `docs/src/operations/` — ten as delivered, and one more since
[F20](configuration-sweeps.md). The split is by question, not by workload:

| Page | The question it answers |
|---|---|
| `overview.md` | what is true of all of it, and what has been captured |
| `grid.md` | what a workload costs at a given ratio of reads to writes |
| `row-size.md` | how the cost moves as rows get wider |
| `table-types.md` | what durability costs, and what ordering costs |
| `access-patterns.md` | what skew buys, and where the server stops keeping up |
| `transport.md` | what the sending mode costs, and what encryption costs |
| `fanout.md` | what reading many partitions in one query costs |
| `configuration.md` | what each setting in `shoal.yml` is worth ([F20](configuration-sweeps.md), added after) |
| `micro.md` | whether a change made one function faster |
| `attribution.md` | which code the time is spent inside |
| `all-workloads.md` | every raw number, with no selection |

Every page that draws workloads opens with the same four blocks, before any chart:

- **What this measures** — what the numbers are of
- **How to read it** — how to turn one into a conclusion
- **What would make it wrong** — saturation, sample count at the wide end, warming, whatever it is
- **What it cannot tell you** — the question a reader will want to ask that this cannot answer

Those come from a **family**: a group of workloads answering one question, declared in
`shoal-bench/src/render/family.rs`. `family_for` must answer for every identifier in
`workload_ids::IDS`, and every family must fill in all four blocks. Both are tests.

The per-workload chart wall is gone. `all-workloads.md` carries the same numbers as a table, with a
second table saying which page explains each family — and a row for anything no family claims, which
is a workload nothing on the site explains.

## Design choices

**The page set is a constant, not derived from the captures.** Every page needs an entry in
`SUMMARY.md`, and `create-missing = false` means an entry with no file behind it fails the whole
book build. A page that appeared because somebody added a workload would break the book on the
commit that added it. For the same reason **every page renders even when it has nothing to draw** —
a capture that measured no encryption still produces `transport.md`, saying so.

**`render` writes all ~~ten~~ eleven or none.** [F20](configuration-sweeps.md) added
`configuration.md`, so the count moved; the rule did not. Every page is built into memory before
any is written. A tree
holding four current pages and six stale ones is worse than one holding ten stale ones, because
nothing on a page says which kind it is.

**Selection reads the recorded facts, not the identifier.** A page picks its arms by `read_pct`,
`row_bytes`, `table_kind` and `distribution` out of `ScaleFacts`, not by parsing
`macro/grid/unsorted/r50/1024` back into three values. Parsing is fewer lines and it is a naming
convention pretending to be a schema: the first workload named differently is either mis-read or
dropped silently, and a chart with an arm quietly missing is worse than one that failed to draw.

**The four blocks are mandatory.** This is the entire mechanism. A family with an empty block fails
a test; a workload with no family fails a test. Before this, an unexplained chart was the *default*
outcome of adding a sweep, and that is how the page got into the state this replaces.

**`what_it_cannot_say` earns its keep.** Every family has a real limit — the fan-out curve warms up
as it runs, the grid's throughput is throughput at one depth, the instrumented builds cannot produce
a latency — and every one of those limits has at some point been read past in this repository.

**Two new chart kinds rather than four more copies of one.** `chart::sweep` is `micro_scaling`
generalised: numeric x, one line per named series, ~~labels at the right-hand end~~ (a shared
legend since [F19](chart-legends.md)), and the axis kind a *parameter* rather than a type — which
needed one `Ranged` wrapper, because plotters picks linear or logarithmic in the type and the
read-share axis starts at zero where the width axis spans four orders of magnitude. `chart::bars`
exists because four table kinds and three key distributions are not numbers, and drawing them as a
line implies an ordering they do not have.

~~**Labels at the end of the line, never a legend.** Three of the eight series colours fall below a
3:1 contrast ratio on the light themes, permitted only under the relief rule that every chart ships
beside a table. A legend would make reading the chart require the colour match that the relief rule
exists to avoid.~~

**Superseded by [F19](chart-legends.md).** The colour-contrast half of that argument still holds
and is why a legend entry is an eleven by eleven filled block rather than a sample of the line. The
conclusion drawn from it did not: a label at the end of a line is only attached to that line when
the lines end far apart, and on a chart of a mixture they do not. `chart-grid-latency` ended eight
curves inside a six pixel band, and the pass that pushed the names apart to fit put two of them
three pixels apart. A name pushed a third of the way down the chart to find room costs the colour
match anyway, without the legend that would have made it possible.

## Alternatives rejected

**Keeping one page and adding a table of contents.** The problem is not navigation, it is that
eighty-eight charts of equal weight assert that eighty-eight things matter equally. A contents list
at the top of 13,000 lines is a better index into the same claim.

**Deriving the page set from the families.** Would let a page appear on the commit that adds a
workload, and break the book on that commit because `SUMMARY.md` had not caught up.

**A template engine.** The old renderer's prose is Rust string literals and this one's still is. A
template file is a second place for the four blocks to live and a second thing to keep in sync with
the type that requires them; the compiler checks one of those and not the other.

**Making the four blocks optional, with a default.** A default would render, look finished, and say
nothing — which is precisely the failure state being fixed. Empty is a test failure.

**Keeping the per-workload history charts, on `all-workloads.md`.** A chart of one workload's wall
clock across four captures says something only to a reader who already knows what that workload is,
which is what the family pages are for. On a page whose job is finding a number, a table is better
at it, and it is 90% of the byte weight.

**Rendering a subset with a `--page` flag.** Invites exactly the mixed-freshness tree the
all-or-none rule avoids, to save seconds on a command that already takes seconds.

## Limitations

- **`render --check` still fails after any commit.** Every page states the commit it was rendered
  against, because the staleness verdicts on it are relative to that commit. It cannot be a blanket
  CI gate. Unchanged from [F7](bench-runner.md).
- **~~Ten~~ Eleven pages is ~~ten~~ eleven `SUMMARY.md` entries to maintain by hand.** A page added
  in `PAGES` without a `SUMMARY` entry is simply unreachable; nothing catches that.
  [F20](configuration-sweeps.md) added the eleventh and had to remember, which is the limitation
  being demonstrated rather than fixed.
- **A family is a partition by identifier prefix.** A workload that belongs to two questions is
  placed in one family and selected onto the other page by predicate, which works and is two
  mechanisms where one would be nicer.
- **The diffs are smaller, not small.** ~~169 KB across ten files~~ — 529 KB since
  [F19](chart-legends.md), which put a legend under every chart and widened the encryption sweep —
  against 1.1 MB in one, and still mostly SVG path data. Reviewers should still read
  `render --check` and the tables.
- **`all-workloads.md` is still a long table** — one row per workload per capture, and the grid
  multiplied the workload count by nearly three.
- **Nothing checks that a page's prose still describes what it draws.** The four blocks are
  hand-written; the charts are generated. A chart that changed what it plots would leave its own
  description behind, and only a reader would notice. [F19](chart-legends.md) is the first change
  to hit this: three captions said a curve was "labelled at its right hand end" and one described a
  percentage axis, and all four had to be found by grep rather than by a test.

## Invariants to uphold

**Every workload identifier maps to exactly one family.** This is what stops a new sweep landing as
an unexplained chart, and it is a test rather than a convention because the convention already
failed once.

**Every family fills in all four blocks.** An empty one renders a heading with nothing under it,
which is worse than not having the heading.

**The four determinism rules from [F7](bench-runner.md) apply to every page, not to one.** No
wall clock reaches a page; every map walked is a `BTreeMap` and every list explicitly sorted; every
float goes through `crate::fmt`; chart geometry is a pure function of the data and the canvas size.
`render --check` compares byte for byte, so any of these breaks the check on a tree nobody touched.

**Every page must render from an empty tree.** A clean checkout with no captures has to build the
book, and `create-missing = false` means a page that declined to render breaks it.

**The dirty check ignores the whole `docs/src/performance/` directory.** The pages are outputs of
`render`, not inputs to it. Counting them would make every page report itself as an uncommitted
change the moment it was written, and `--check` could never pass.

**An absent measurement is drawn as a gap, never as a zero.** A workload that was never run is not a
workload that took no time. `ops_per_sec` returns `None` rather than `0.0` for the same reason, and
every pre-F17 capture depends on it.

## Performance

Rendering is unchanged in cost — the same artifacts read once, the same charts drawn. The output is
**169 KB across ten files** against 1.1 MB in one, because the eighty-eight per-workload charts are
gone and the ninety-two SVGs are now about twenty.

## Tests

| Test | What breaks if this is reverted |
|---|---|
| `family::tests::every_workload_has_a_family` | a workload lands on no page with no prose, which was the old default |
| `family::tests::every_family_says_all_four_things` | a page renders a heading with nothing under it |
| `family::tests::every_page_that_draws_workloads_explains_them` | a page draws charts nothing explains |
| `family::tests::every_page_has_its_own_path` | two pages overwrite each other |
| `pages::tests::every_page_is_registered_exactly_once` | a declared page is never written, and `SUMMARY.md` points at nothing |
| `pages::tests::every_page_renders_with_nothing_captured` | the book stops building in a clean checkout |
| `pages::tests::every_page_renders_deterministically` | `render --check` fails on a tree nobody touched |
| `pages::tests::no_page_carries_a_wall_clock` | the same, on every run |
| `pages::tests::every_page_names_its_commit` | the staleness verdicts become claims with no referent |
| `pages::tests::a_footer_links_the_siblings_and_not_the_page_itself` | pages become unreachable from each other |
| `arms::tests::the_ladder_is_not_a_grid_cell` | the depth ladder is drawn as four more grid cells at one width |
| `arms::tests::table_kinds_are_in_reading_order` | the control is charted before the thing it controls for |
| `arms::tests::a_mixture_labels_itself_by_name` | a mixture's mean is read as a fixed width |
| `chart::bars::tests::an_absent_value_is_not_drawn_as_zero` | a workload that was never run is drawn as one that took no time |
| `chart::bars::tests::a_ragged_series_is_refused` | a bar is drawn under the wrong category |
| `chart::sweep::tests::a_linear_axis_can_start_at_zero` | the read-share axis cannot be drawn at all |
| `chart::sweep::tests::a_non_positive_value_does_not_break_a_log_axis` | plotters emits `NaN` into the path data and the chart silently draws nothing |
| `chart::sweep::tests::drawing_is_deterministic` | `render --check` fails on a tree nobody touched |
| `css_sync` | a chart ships in a colour the stylesheet has no rule for |

## Related

- [F7, the benchmark runner](bench-runner.md) — the renderer this replaces the output half of
- [F17, the workload grid](workload-grid.md) — the seventy-four workloads that made one page
  untenable
- [Performance](../performance/overview.md) — what this generates
- [Benchmarking](../performance/benchmarking.md) — how to take the captures it draws
