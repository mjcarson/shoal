# F29. An explorer that draws more than one capture

## Context

Every chart under `docs/src/performance/` is a static inline SVG, drawn by `plotters` when
`shoal-bench render` runs, and **each one draws exactly one capture** — whichever
`Page::current_for` resolves for that layer. Twenty-seven captures are committed under
`docs/perf/runs/`, twenty-two of them with a macro layer, and nothing in the tool plots a metric
across them.

So the question the corpus exists to answer — is this number getting better or worse — could not be
read off the pages that exist to answer it. `compare` answers it, in text, two labels at a time, and
[the todos](../appendix/todos.md) have twice asked for the neighbouring thing:

> **Multi-capture comparison, on the micro layer.** `shoal-bench compare` still takes one capture per
> side […] It needs to take several, because one cannot be trusted.

A results page is the wrong shape for this and cannot be made the right one. A chart drawn at render
time has to choose its axes, its metric and its arms before anybody has seen it, and the useful
choice depends on the question being asked. So the axes become a control rather than a decision.

## What it does

`shoal-bench explore` projects the whole corpus into a compact index and draws it with `egui` and
`egui_plot`, with the metric, the axes, the workloads and **an arbitrary set of captures** all
chosen at runtime.

It opens on throughput against the read share for the most recent capture that measured anything —
the interactive counterpart of `chart-grid-throughput` on [the grid page](../performance/grid.md),
~~reproducing `render/pages/grid.rs::throughput` arm for arm~~ **selecting that chart's arms and,
since [F30](plot-axis-units.md), drawing them as that chart draws them: four curves, one per table.
It picked the right twenty-four arms from the day it shipped and folded all four tables into one
zigzag — [item 82](../appendix/resolved/one-line-per-capture.md).** It is a real chart on sight
rather than an empty one waiting to be configured, and no second capture is selected, so nothing is
being compared until somebody asks for a comparison.

Three flags, and the middle one is the one that gets used:

| Invocation | What it does |
|---|---|
| `shoal-bench explore` | Opens a window, or explains why this machine has no display and names the flag that works |
| `shoal-bench explore --serve` | Builds the explorer to WebAssembly and serves it on `127.0.0.1:8321`, which a port forward reaches |
| `shoal-bench explore --index-only` | Writes `target/explore/index.json` and stops |

### The two axis modes

This is the part that makes it more than a chart viewer, and it falls out of the opening view being
a *sweep* rather than a timeline.

| Mode | Key axis | One series per |
|---|---|---|
| **Sweep** | a recorded fact — read share, row width, load depth, rows, keys | ~~capture~~ **(capture, curve)**, since [F30](plot-axis-units.md) |
| **Timeline** | the capture, oldest first | workload |

A *curve* is the facts a caller set minus whichever one the axis reads, so four tables at one width
are four curves rather than four points at every mixture on one line. Since
[F32](chart-line-identity.md) a curve that would draw two of its arms at one position on the axis is
split into one line per workload — two workloads that set none of the recorded facts differently are
one key and still two workloads. Which workloads may share the
chart at all is decided by their **axis units** — see [F30](plot-axis-units.md).

Neither subsumes the other. A timeline of one capture is a single point; a sweep says nothing about
when. Sweep mode is what makes selecting a second capture worth doing: the **whole curve** is
redrawn beside the first, which is a much stronger signal than one point moving. Timeline mode asks
the regression question directly.

Both draw as lines or as grouped bars. The metrics are the three macro rates, the median wall clock,
the run spread, and every percentile of every operation the corpus recorded — the operation names
are read out of the artifacts rather than hardcoded, because a capture lifted from the first artifact
version calls them `insert` and `get` where a current one calls them `write` and `read`.

~~All of them are offered, in one list, whatever is selected.~~ Since
[F31](metric-availability.md) the control is **three** lists — a kind, and an operation and a rank
underneath `latency` alone — each holding only what every ticked workload can actually answer. The
flat list was thirty-three entries against a corpus where a workload answers at most fourteen, and
`Index::units` never asked whether a measurement carried the metric at all, so the rest were offered
and drew nothing: [item 83](../appendix/resolved/default-metric-half-the-corpus-cannot-answer.md).

## Design choices

**The explorer is a crate, `shoal-top`, not a module.** Partly because it is meant to grow into a
live view of a running server, which is what the name is for and why every chart is written against
a `Source` trait rather than against the index directly. But mostly because `shoal-bench` **cannot
be compiled for `wasm32-unknown-unknown` at all**: it links `walkdir`, whose `same-file` selects its
implementation on `cfg(unix)` and `cfg(windows)`, and a browser is neither. Any design where the
drawing code reaches back into `shoal-bench` for `family_for` or the model types is dead on arrival.

**`shoal-bench` enters it with `default-features = false`.** That takes the index types — `serde`
structs and nothing else — and leaves egui behind. `cargo tree -p shoal-bench --no-default-features`
gains one line, `shoal-top v0.1.0 (path)`, with `serde` beneath it and no graphics package, so the
half of that crate which judges a change is still the cheapest build in the workspace. Because
`shoal-top` is a **workspace member** rather than an excluded directory, `cargo check --workspace
--all-targets` still type-checks the whole UI, so it cannot rot silently.

**The corpus is projected, not re-read.** `explore::project` calls the same `render::gather` the
committed pages are built from, so the explorer and the pages can never disagree about what the
corpus holds — and if `render` is ever reimplemented on top of the explorer, that function is already
the shared half. Thirteen megabytes of captures become a 1.3 MB index, 215 KB gzipped.

**A gap is never a zero.** Fifteen hundred of the ten thousand possible (capture, workload) pairs
exist. A capture that did not measure a workload did not measure it slowly, so a line is split into
runs of consecutive measurements and drawn as one `Line` per run; a run of a single measurement also
gets a `Points`, or a workload only one capture ever measured would be invisible; and a bar with no
measurement is **not pushed at all**, its slot left empty, because the x position is computed from
the group index rather than from how many bars came before. `f64::NAN` is deliberately not the gap
marker: `egui_plot` folds every point into the plot's bounds, and one NaN takes them to `[NaN, NaN]`,
which renders as an empty chart.

~~**Hue carries the series, dash pattern carries the capture.** With several captures and several
tables selected, their product passes the number of hues anybody can tell apart. So two captures of
one workload are the same colour in two styles, which is the comparison the crate exists to make.~~
(True of `theme.rs` from the start and **not** of the plot until [F30](plot-axis-units.md), which
passed the series' position to `theme::capture_style` — so with two captures ticked the styles walked
the series list rather than standing for a capture.) The contract is unchanged by the theme below;
what moved is the shades, and that there are now two sets of them.

**Superseded by [F32](chart-line-identity.md): identity is three channels, and hue carries the
table.** The half of the sentence above about the dash is unchanged and still says why. The half
about hue was never true of the plot at all — `theme::series` was indexed by the curve's *position*
among the ones drawn, so the sorted table was blue on its own and orange with the unsorted one ticked
ahead of it. Hue is now the table's fixed slot in `index::TABLE_ORDER`, all four reserved whether or
not a chart draws them; the dash is still the capture on a sweep; and a **marker** carries which of
that table's curves a line is. `theme::capture_style` is `theme::line_style` for the same reason —
on a timeline there is no single capture for it to stand for, and the mark drives it there.

**The explorer has a theme, and it opens dark.** It had none at all until this was written: no
`Visuals`, no `Style`, no theme preference, so it ran on egui's default of
`ThemePreference::System` and the served page's appearance was decided by whatever
`prefers-color-scheme` the reader's browser happened to report — while `index.html` had all along
hardcoded a dark body behind the canvas, so a reader in light mode got a light chart letterboxed in
a dark page. `theme::install` now registers both themes with `Context::set_visuals_of` and calls
`set_theme(ThemePreference::Dark)` once, from the `eframe` creation closure on each path.

The palette is [One Dark] — the theme Helix, JetBrains and VS Code all ship a version of — because
this is read in a browser over a port forward beside a terminal that is already dark. Light stays
reachable through `egui::global_theme_preference_switch` in the toolbar, and is One Light rather
than egui's stock theme, so the switch lands somewhere deliberate in both directions.

That is also why there are **two** series arrays rather than one. `theme.rs` had always claimed its
eight hues were picked to stay legible against a light *and* a dark ground, which is a set tuned for
neither: `#61AFEF` is washed out on `#FAFAFA` and `#4078F2` is heavy on `#282C34`. The two arrays
hold the same eight hue roles in the same order, so a curve changes shade when the reader flips the
switch and never changes identity. The red/green exclusion — the one categorical pair that fails for
the commonest colour vision deficiencies — holds in both, as it did before.

[One Dark]: https://github.com/atom/atom/tree/master/packages/one-dark-ui

**The four mandatory blocks are projected, never retyped.** `Family`'s *what it measures / how to
read it / what would make it wrong / what it cannot tell you* ([F18](results-pages.md)) are copied
out of `render/family.rs` into the index and ~~shown in a panel under the chart~~ **shown in a panel
along the bottom of the window, shut on first sight.** The explorer needs
them **more** than the pages do: a page's arms were chosen by somebody who knew what the page was
for, and the explorer's are whatever the reader ticked. A selection spanning several families says so
rather than stacking four panels and hoping.

They were open by default until the chart was given the window. Four blocks for the default family
wrap to more than the chart above them was tall, so on every frame after the first time anybody read
them they were most of the page — which is a strange thing to do to the four paragraphs whose whole
claim is that they are mandatory.

**Folding is not dropping**, and the always visible row is what carries the difference. The header
names the family the chart is being read against, so a reader knows there is something to open and
what it is about; and the caveat about a selection spanning several families sits **above** the
fold, in the panel proper, rather than inside the collapsed body. That is the one line somebody can
be harmed by not seeing — every other block explains a chart they are looking at, while that one
says the explanation below does not fit the chart at all — so it is the one line that is never
behind a click. egui remembers the fold against the header's own id, so a reader who opens it keeps
it open across a change of metric.

**The bundle is served by about a hundred lines of `std::net`.** Four static files over `GET`, on
loopback, for one reader — against a manifest that is explicit about not growing its dependency tree.
`src/clock.rs` exists for the same reason, doing one civil-date conversion rather than taking a date
library. Nothing is ever joined onto a directory: the four paths resolve through a fixed table, which
makes traversal impossible rather than merely checked for.

## Alternatives rejected

**Putting the whole thing in `shoal-bench` behind a feature.** This was the first plan and it does
not compile. The wasm build needs the crate's own lib as a `cdylib`, which means the entire
`shoal-bench` lib — `walkdir` included — has to build for `wasm32-unknown-unknown`, and it does not.
Working around it would have meant target-gating a dozen module declarations and moving four
dependencies into a `cfg(not(target_arch = "wasm32"))` table, to arrive at a worse version of the
crate split.

**Putting `shoal-top` outside the workspace, in `tools/`, with its own lockfile.** This keeps the
root `Cargo.lock` at exactly 517 packages, which is what the manifest's dependency rule literally
asks for. It was rejected because an excluded crate is invisible to `cargo check --workspace`, so it
rots the first time an index field is renamed — and this is meant to become the primary way the
numbers are read, which is a bad thing to build on a foundation nothing checks.

**`egui-charts`.** The crate of that name on crates.io is a third-party *financial* charting engine
— candlesticks, 130+ trading indicators, drawing tools. `egui_plot` is the egui organisation's own
plotting crate and is what a benchmark trend view needs.

**Committing the index.** It would become a fourth thing that can be stale, and — because
`gather` computes `Page::dirty` from every uncommitted path outside `docs/src/performance/` — a
regenerated index would flip every rendered page's dirty flag and break `render --check`
permanently. It is written to `target/explore/` only.

## Limitations

**It draws the macro layer and nothing else.** No micro, no hotpath, no stages. Those are the two
`#[serde(default)]` sections the index is shaped to grow — see *Invariants* — but today
[`micro.md`](../performance/micro.md) and [`attribution.md`](../performance/attribution.md) have no
counterpart here.

**Only twenty-two of the twenty-seven captures have a macro layer.** The four most recent —
`f25-read-buffers`, both `f27-*` and `f28-rearchive` — are micro and meta only, so the explorer opens
on `f24-routing` and the newest captures are absent from every chart it draws. That is the corpus's
state, not the explorer's, but it is the first thing that will look like a bug.

~~**A value has to be read off the axis.**~~ Closed by [F33](chart-readout.md): the pointer selects a
column and every drawn line's value in it is listed, in the metric's own unit. That page also carries
what the label this replaces was doing —
[item 86](../appendix/resolved/hover-label-reads-in-decades.md), where `egui_plot`'s own formatter
reported a logarithmic chart in decades, for a series it named with the empty string, because this
feature's gap rule names only a line's first run.

~~**Nothing checks that the workloads a reader ticks belong on one chart.**~~ Closed by
[F30](plot-axis-units.md): a workload whose axis units differ from the chart's is ~~disabled in the
picker with the reason on it~~ **hidden from the picker and counted on its family's header, since
[F31](metric-availability.md)**, and `Source::series` enforces the same rule for any other
implementor of that trait. F31 also closed the half F30 did not cover — whether the workload carries
the metric at all, which nothing had ever asked.

**The four blocks are one click away rather than on screen.** This is the cost of the paragraph
above, stated as a cost: a reader who never opens the panel reads a chart without the four blocks,
which on a results page is impossible. What is preserved is that they are always *there* and always
*named*, and that the multi-family caveat is not foldable. Whether that trade is right is a judgement
about readers, not a fact about the code, and it is the first thing to revisit if the explorer is
ever handed to somebody who did not build the corpus.

**The user interface has not been seen.** This machine is a headless SSH session with no `DISPLAY`,
no Wayland socket, no `libGL` and no Vulkan, so the window has never opened. Everything below the
drawing code is tested — the projection against the real corpus, the series construction including
every gap case, the server's routes and content types — and the drawing code itself compiles for
both targets and has been checked against `egui_plot` 0.37's actual signatures. It has not been
looked at.

~~`wasm-bindgen` is not installed, so the served page has never been rendered either.~~ **It is
installed now, and the served page builds and serves end to end**: `explore --serve` compiles the
crate to `wasm32-unknown-unknown`, runs `wasm-bindgen` over it, and answers all four routes — the
10.2 MB bundle as `application/wasm`, the index as `application/json`, the shell page with the One
Dark ground in it. That is a build and a transfer, not a render. **Nothing in this repository has
ever put a pixel of this crate on a screen**, so every claim on this page about how it *looks* —
the palette, the panel that folds, the chart filling the window — is a claim about the code that
produces those things and not about the result. The theme's five tests exist because of exactly
that gap: a theme that stopped installing and a theme that installed and was ignored are
indistinguishable from here.

**The bundle is 11.7 MB.** No `wasm-opt` pass runs. Over loopback this is a non-issue and over a
port forward it is a one-time second; it would matter if the explorer were ever served anywhere else.

**`cargo check --workspace --all-targets` does not cover the native window.** `native` is not a
default feature, so `native.rs` and the `shoal-top` binary need
`cargo check -p shoal-top --features native`, which is in CLAUDE.md beside the `shoal-client-check`
line that exists for the same reason.

## Invariants to uphold

**`shoal-bench` must enter `shoal-top` with `default-features = false`.** Without it the runner
grows an egui tree and the manifest's dependency rule stops holding. This is a manifest property that
nothing else in the test suite could notice, so `the_explorer_crate_is_wired_the_way_the_lockfile_needs`
reads the manifest as text.

**`shoal-top::index` must never read a clock, a file or a process.** `SystemTime::now` panics on
`wasm32-unknown-unknown` and `std::process` is a stub there. Every timestamp in the index is the RFC
3339 string the capture recorded, carried verbatim. The crate has one non-optional dependency and it
is `serde`; keep it that way.

**An absent measurement stays absent.** Every metric accessor returns `Option` and every `None`
survives to the plot as a gap. The moment one becomes a `0.0` or a `NAN` for convenience, the chart
starts inventing measurements at exactly the place a reader is most likely to misread it.

**No explorer colour may be added to `render::chart::palette`.** That module holds *sentinel* values
which only become colours through `docs/theme/charts.css`, and `tests/css_sync.rs` asserts a
bijection between the two — a constant added there without a stylesheet rule fails, and a rule for a
colour that never reaches an SVG fails the other half. The explorer's palette lives in
`shoal-top/src/theme.rs` and is the first set of real colours in this repository. This now covers
**both** palettes there, not one — the light array is as ineligible as the dark one.

**The two series arrays stay the same length and the same hue order.** `theme::series` wraps on the
palette's own length, so two lengths would mean a curve's position resolving to a different hue in
each theme — which is to say, a curve changing identity when somebody presses the switch rather
than changing shade. `the_two_palettes_have_the_same_arity` holds the length; the order is held by
the comment naming the eight roles beside both arrays. Since [F32](chart-line-identity.md) the index
into them is the *table*, not the curve's position, which makes this invariant stronger rather than
weaker: the first four slots are `TABLE_ORDER`'s and mean the same table in either theme.

**The four mandatory blocks may be folded, never removed, and the multi-family caveat may not be
folded.** The whole justification for shutting the panel by default is that nothing was hidden: the
header stays visible and names the family, and the caveat about a chart mixing families stays above
the fold. Moving that caveat back inside the collapsing body, or dropping the header row for a bare
icon, turns a fold into a removal and takes [F18](results-pages.md)'s guarantee with it.

**The wasm build's `RUSTFLAGS` is set on the child process and never exported.** The workspace sets
`-Ctarget-cpu=native`, which reaches wasm32, where rustc substitutes the host CPU name and LLVM
rejects it — about two hundred warning lines per crate, fifty thousand across an eframe build.
`shoal-top/.cargo/config.toml` overrides it for that triple, and **that is not enough**: a config
file loses to a `RUSTFLAGS` environment variable, and this repository's shell exports one. So
`explore/build.rs` sets it on the spawned cargo. It must stay on the child: `fingerprint.rs` hashes
`RUSTFLAGS` into every capture's provenance, and a capture must record the flags it was really built
with. Note also that the override cannot be an **empty** list — cargo treats an empty join as no
match and falls straight through to `build.rustflags`.

**Only measurements whose axis units agree may share a chart.** The rule and the reasoning are
[F30](plot-axis-units.md)'s; what matters here is that `Source` is the contract. `Source::series`
performs the refusal itself rather than trusting the picker, because a second implementor — the live
server view this crate is named for — would otherwise be free to build points out of mismatched
units at the one place a reader has no way to check them.

**`INDEX_VERSION` is a floor, not an equality check.** Deliberately unlike
`MicroCapture::check_version`, which guards a committed historical artifact. The index is a derived
cache rebuilt on every invocation, so the only compatibility that matters is that a stale browser
bundle refuses a newer index rather than silently dropping a section it has never been told about.
Sections added later are `#[serde(default)]`, and an empty one means *this index does not carry that
layer*, never *that layer measured nothing*.

**The `wasm-bindgen` CLI version is read from the lockfile, never hardcoded.** The CLI and the crate
must be the same version, not merely compatible ones, and a mismatch is not a build error — it is
`import object field '__wbindgen_placeholder__' is not a Function` in the browser, hours later. This
is not hypothetical: the lock resolved **0.2.127** once eframe was added, where it had held 0.2.104
for `plotters`.

## Performance

Nothing measured changed, so **no capture was taken** — no workload, no `shoal.yml`, no seed and
nothing under `shoal-bench/src/workloads/` was touched, and CLAUDE.md excludes renderer and test-only
changes from needing one.

Projecting the whole corpus takes about a second, most of it reading the thirteen megabytes that
`gather` reads anyway; two of the macro artifacts are 3 MB each. The index is built **once**, before
the server binds, never per request. The workspace lockfile went from **517 packages to 766**,
almost all of it eframe's native backend — winit, glutin, glow, wgpu and their trees. `egui` and
`egui_plot` alone are about twenty, and the browser half cost nothing, because `plotters` had already
put `wasm-bindgen`, `web-sys`, `js-sys` and `wasm-bindgen-futures` in the lock.

## Tests

| Test | What breaks without it |
|---|---|
| `explore_index::the_corpus_projects_and_is_not_empty` | A projection that silently produces nothing, which would pass every other test here |
| `explore_index::every_workload_is_explained_by_a_family` | A workload the explorer can draw but nothing explains |
| `explore_index::the_four_blocks_are_carried_whole` | The four mandatory blocks truncated or reworded on the way across, which is a silent downgrade of the one thing a chart cannot carry |
| `explore_index::an_absent_measurement_is_absent_rather_than_zero` | A missing measurement becoming a zero, and a gap becoming a drop to the origin |
| `explore_index::projecting_twice_produces_the_same_bytes` | A `HashMap` or an unsorted vector reaching the index, which is the determinism rule the pages keep |
| `explore_index::measurements_are_sorted_for_the_binary_search` | `Index::macro_point` silently failing to find measurements that are there |
| `explore_index::the_layer_mirror_is_total` | The explorer's copy of `Layer` drifting from the real one |
| `explore_index::formatters_agree` | The five copied formatters forking from `shoal_bench::fmt` — the only thing keeping them honest |
| `explore_index::the_explorer_crate_is_wired_the_way_the_lockfile_needs` | `default-features = false` dropped, and an egui tree appearing in the runner |
| `index::tests::a_timeline_carries_the_gap_through_to_the_points` | A capture that never measured a workload contributing a point rather than a hole |
| `index::tests::a_sweep_is_ordered_by_its_key_not_by_selection_order` | A line drawn in the order the boxes were ticked rather than along the axis |
| `index::tests::a_newer_index_is_refused_and_an_older_one_is_read` | A stale bundle silently mis-reading a newer index |
| `index::tests::captures_taken_in_one_place_are_comparable` | Two incomparable captures sharing an axis with nothing said about it |
| `index::tests::the_newest_capture_with_a_layer_is_the_last_one_that_has_it` | Opening on `f28-rearchive`, which has no macro layer, and drawing nothing |
| `explore_index::the_grid_preset_reproduces_chart_grid_throughput` | The opening chart drifting from the page it claims to reproduce — added by [F30](plot-axis-units.md), which is where the rest of the series-construction tests now live |
| `serve::tests::traversal_does_not_resolve` | A future edit introducing a path join, and with it a traversal |
| `serve::tests::the_bundle_is_served_as_wasm` | `instantiateStreaming` refusing the bundle, for a blank page and one unhelpful console line |
| `theme::tests::the_explorer_opens_dark` | `install` no longer pinning the preference, and the theme going back to whatever the reader's browser reports |
| `theme::tests::both_themes_are_registered` | One theme installed over the other's values, for a switch that appears to do nothing |
| `theme::tests::the_two_palettes_have_the_same_arity` | A curve resolving to a different hue in each theme, so pressing the switch changes identity rather than shade |
| `theme::tests::no_theme_repeats_a_series_colour` | Two series drawn in one colour, which on a line chart is one series |
| `theme::tests::a_line_style_stands_for_a_capture` (was `a_capture_style_stands_for_a_capture` before [F32](chart-line-identity.md)) | The dash cycle losing its period or its solid first arm, which is the channel that still stands for the capture |
| `theme::tests::a_marker_stands_for_a_curve_within_its_hue` | The third channel [F32](chart-line-identity.md) added losing its cycle, or a chart of one line per table growing markers |

## Related

- [F18. Results pages that explain themselves](results-pages.md) — where the four mandatory blocks
  come from, and the pages this is eventually meant to replace
- [F7. The benchmark runner](bench-runner.md) — the tool this is a subcommand of
- [F15. The client/server split](client-server-split.md) — the same rule, that a crate boundary is
  enforced by what will not compile across it
- [Benchmarking](../performance/benchmarking.md) — the runbook
- [F30. Only comparable axes share a chart](plot-axis-units.md) — what may be drawn together, and
  how a legitimate selection is split into curves
- [F31. A metric a workload can actually answer](metric-availability.md) — which metrics may be
  chosen at all, and the three lists that replaced the one
- [F33. The chart answers where you point, and the capture list says whether it can](chart-readout.md)
  — reading a value off the chart rather than off the axis, and greying a capture that has none
- [Todos](../appendix/todos.md) — what `render` would still need before it could be retired, and the
  live-trace source this crate is named for
