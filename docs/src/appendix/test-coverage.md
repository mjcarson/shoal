# Test Coverage

What the test suite reaches, what it does not, and the one place where it is unsound.

**Established by running it.** `cargo check --workspace --all-targets` passes with warnings and
`cargo test --workspace` passes: **1,074 tests**, two ignored, plus **13** behind
`--features stage-profile` that a default run does not reach.

**[Resolved #80](resolved/never-flushed-partitions.md) added 1**, and the total went 1,073 → 1,074.
It is one test in a new binary, `disk_lookups.rs`, and it is the first test in the suite that
counts what the engine **asked storage** rather than what it answered a client. It has to: the
defect it reproduces changed nothing about any answer — a partition that had only ever been
written to asked its storage engine about an archive that did not exist, on every single get, and
both the wasteful path and the fixed one return the same rows in the same order. The count comes
from the `PersistentTable::block_on_load` tracing span, which is in the shipping code rather than
added for the test, so what it measures is the lookups the engine actually makes. Six gets over
two never-flushed partitions: six lookups before the fix, two after. **That binary holds one test
and must** — the counter and the subscriber that feeds it are process wide.

**[F27](../features/grouped-responses.md) added 13**, and the total went 1,060 → 1,073. Three sit
in `shoal-proto` over `RowRef` — and the third of those is the reason the other two mean anything:
rkyv `memcpy`s a type it can prove has no padding, and a `Vec<T>` reaches that branch while a
`Vec<RowRef<'_, T>>` cannot, so the byte-identity test compares two different writers rather than
one against itself. That is asserted rather than assumed, because if rkyv ever stopped enabling the
optimization the comparison would pass while proving nothing.

Four more are in `shoal-proto` over the group index — including one that merges four shares in six
different arrival orders and checks the result against the hashing implementation it replaced,
which is kept `#[cfg(test)]` for exactly that. Three are a new `grouped_responses.rs`, archiving
every variant of both generated response enums and comparing the bytes. Two are in `shoal-core`
and state [O2](optimizations.md)'s claim as a **count**: a resident unprojected scan of three rows
copies zero of them, and an archived scan of the same three builds all three. One is an
integration test in `persistent_sorted_table.rs`.

**That last one was a test whose own doc comment recorded that it did not do what it was written to
do**, which is unusual enough to say here. It was meant to compare the two reply paths against each
other, and did not, because the sorted table never reached the borrowing one — see
[Resolved #80](resolved/never-flushed-partitions.md), which has since fixed that, so the test now
does what it was written for. It was kept, with the claim corrected, because what it does check was
still worth checking. It was only known to be vacuous because the path was probed rather than
reasoned about — and the probe is what the lookup count in `disk_lookups.rs` replaced.

**[F26](../features/archive-routed-requests.md) added 5**, and the total went 1,055 → 1,060. It
added a sixth that a default run does not reach, taking the `stage-profile` extras from 12 to 13:
`a_parked_get_does_not_count_its_disk_wait_as_execution`, over the stamp a replayed query used to
overwrite. That one was found by reading the change rather than by a failure, which makes it the
only test here written for a defect that had never been captured — the layer it is about only runs
under a feature flag, so nothing would have reported it. They
are all in one new binary, `archive_routing.rs`, and they are all the same kind of test: the
coordinator now routes a bundle without deserializing it, which splits one function into three,
so each of them checks the three against the one they replaced rather than against a hardcoded
answer. `split_by_shard` is kept in the tree for exactly that reason — it is no longer on the live
path and it is the only definition of correct the new path has. Four of the five were confirmed by
breaking the code under them: dropping the `normalized()` call fails two, and routing a write with
keys to narrow to fails a third.

**[F25](../features/read-buffers-are-filled-not-zeroed.md) and
[Resolved #79](resolved/micro-only-capture-current.md) added 8**, and the total went 1,047 → 1,055.
Five are over reads that no longer zero the buffer they are about to fill, and all five are about
the same risk rather than about the removal: a buffer nothing wrote is uninitialized memory, so
every one of them delivers its bytes *in pieces* or stops halfway. `a_body_read_in_chunks_holds_every_byte_it_was_sent`
and `a_payload_that_arrives_in_pieces_is_read_whole` build their payloads out of bytes that are
never zero, so a tail the read missed is visible rather than plausible.

The other three are over `Page::current_for`, and two of them fail against the tree before the fix.
They are the cheapest kind of test for the most expensive kind of defect: `render` chose one
capture for all eleven pages, so a capture of one layer emptied the pages of the others, and
nothing failed — the pages rendered, said nothing had been measured, and were committed in that
state. Reproduced end to end first with `render --current inline-probe`, which is a real committed
micro-only capture and takes `grid.md` from 59,287 bytes to 3,459.

**[Resolved #78](resolved/sources-manifest-drift.md) and [F24](../features/routing-benchmarks.md) added 2**, and the total went 1,045 → 1,047. The first is
is the cheapest test on this page and it is here as an argument about what tests are for:
`every_source_the_manifest_names_exists` walks the paths in `docs/perf/sources.json` and asserts each
one resolves. Six of the seventeen did not — the client and the wire protocol had moved crates in
[F15](../features/client-server-split.md) and the list had not followed — so the micro layer was not
hashing the protocol module `wire.rs` half exists to measure, and eight captures had been reported as
*unaffected* by changes that affected them. Nothing failed, nothing warned, and the table kept
printing plausible verdicts for four months. **A hand-maintained list with no test is a list that is
wrong and cannot say so**, and the check that catches it is nine lines.

The second is the same lesson arriving twice in one change. `BENCH_TARGETS` is a second
hand-maintained list — the one that makes `shoal-bench` discover a criterion bench at all — and
[F24](../features/routing-benchmarks.md)'s new bench reached `shoal/Cargo.toml` and
`docs/perf/sources.json` and not that. `cargo bench` ran it, `list --refresh` reported the same 527
ids as before, and nothing said a bench target was missing, because a shorter list is a valid list.
`every_bench_target_is_declared_and_exists` asserts every `[[bench]]` in `shoal/Cargo.toml` is named
there and that every source named there is a file, and it fails against the tree before the fix.

**[Resolved #76](resolved/stage-join.md) added 6**, and it is the only change here to move the two
counts in opposite proportions: 2 in a default run and 4 behind the feature. The default two are
over `collect::stages::check`, which judged the stage layer on the artifact's summed join and now
judges each report on its own — one of them rebuilds `f22-row-size`'s shape, where 200,000 joins
from one workload carried three zeros past the threshold, and it passes against the tree before the
fix. Three of the feature-gated four are unit tests over the `StageLog` every driver now gathers
through. The fourth is `stage_join.rs`, a new integration binary and **the first test anywhere that
starts a server under `stage-profile` and reads what it wrote**: it runs one grid arm at smoke scale
and asserts the report has a join in it. The eight tests that existed before it are pure functions
over `build_report` fed fabricated halves, which is why all eight passed while three of the layer's
four reports were empty. The total went 1,043 → 1,045, and the feature-gated count 8 → 12.

**[F23](../features/self-sizing-staging-buffer.md) added 7**, and the total went 1,036 → 1,043. Five
are pure functions over `staging_target`, the rule that decides how wide a staging buffer is: that
it batches eight records, that it never drops below the configured floor, that it stops at the
ceiling, that a record wider than the ceiling still gets a buffer of its own — records are never
split, so that is a correctness property no configuration may override — and that a ceiling pinned
to the floor reproduces the old `max(default_buffer_size, size)` at nine widths either side of it,
which is simultaneously the escape hatch and the definition of what changed. The other two are a new
integration binary, `intent_log_batching.rs`, and they are the reproduction: one bundle of 128 rows
of 8 KiB against a 4096 byte buffer, and the shard's intent log read back off disk. It failed against
the tree before the fix with `128 rows were written in 128 flushes, which is one record per write`.
Its control pins the other direction — a ceiling at the floor gives back exactly one write per
record — so the first test measures the sizing and not something else that happens to batch.

**[F22](../features/row-size-benchmarks.md) added 21**, every one of them a `shoal-bench` unit test,
and the total went 1,015 → 1,036. Four over the grid — that the two width arrays do not overlap,
that the two depth ladders cross at exactly one arm rather than minting it twice, that the width
axis is swept at all three mixtures on all four tables, and that the three arms the stage layer
profiles are three arms that exist. Two over the configuration sweep's width repeats: that a repeat
names a declared knob at a width it does not already run, and that it runs every rung its sweep
does. One that reproduces [item 73](known-issues.md) — two stage runs getting two artifacts. And
one that should have existed already: `the_runners_copy_of_the_profiled_workloads_is_current`
asserts the runner's two lists of profiled workloads match what the workloads say, which two doc
comments claimed a test did before one did. Five more over the stage artifact: that a version 1
bare report still reads and is filed under the only workload that could have written one, that a
report naming its own workload keeps that name, that an artifact of several reads as several and
sums their joins, that the report a page draws when it wants one is chosen deterministically, and
that an artifact from a future version is refused rather than rendered. And three over the
configuration page's reading order, which reproduce and pin
[item 74](resolved/conf-knob-dropped.md): that a knob the order does not name reaches the
recommendation table anyway, that a *section* still takes only its own half, and that a sweep
repeated at another row width sorts beside the sweep it repeats. And four over the stage
collector: that several reports fold into one artifact keyed by the workload each names, that an
unnamed report is refused rather than filed under a guess, that two reports claiming one workload
are refused, and that a file the plan named and no run wrote is an error rather than a shorter
artifact — which is what stops the never-cleared scratch directory leaking a previous capture's
reports into this one's. And one that guards something the identifiers had only been getting
right by luck: `no_two_workloads_share_a_slug` asserts no two of the 374 flatten to the same file
name, which is what names both a workload's scratch results and its storage directory —
`macro/grid/depth/1/512`, new with this feature, is one character from colliding with
`macro/grid/depth/128`.

**The one that carries the feature is `grid::the_width_axis_is_swept_at_every_declared_mixture`.**
The whole point of the `r0` and `r100` sweeps is that a width effect can be attributed to the write
path by subtracting one from the other; a sweep short of a width or short of a table is a pair that
cannot be subtracted, and the arms would still be minted, still run, and still render. Counting per
mixture per table is what catches it.

**[F20](../features/configuration-sweeps.md) and [F21](../features/benchmark-groups.md) added 29**,
every one of them in `shoal-bench`, and the split says what each feature rests on. Twenty-six unit
tests: nine over the configuration sweep, eleven over the group table, three in the registry over
`--group` intersecting rather than replacing, one over seed bundles fitting inside a *narrowed*
frame, and two over parsing a swept value back into the number behind it. Plus three doctests. The
total went 986 → 1,015.

**The one that carries the feature is `conf_sweep::an_arm_moves_one_field`.** Everything on
[Configuration and what each setting is worth](../performance/configuration.md) rests on an arm
differing from the base in exactly one field, because that is what makes the gap between two arms
attributable to a setting; the test counts the moved fields with one term per field, so a field
added to `ConfOverrides` without a term is a visible omission in the diff rather than a check that
silently stops covering it. `every_sweep_covers_the_shipped_default` is the second: it resolves the
committed `shoal.yml` and asserts each sweep contains the value in use, so retuning that file fails
a test instead of leaving the page recommending against a reference that is not on the chart.

**`groups::the_conf_halves_partition_the_sweep` is a counting test that earns its place.** It
asserts every configuration arm is in exactly one of `conf/storage` and `conf/resources` — not
neither, not both — which is what makes two half-captures add up to the whole one, and it is the
kind of thing that goes wrong silently when a prefix is added.

**[F19](../features/chart-legends.md) added 19.** Sixteen unit tests — seven over the shared
legend's layout, four over the sweep's canvas and its data-derived ticks, three over the encryption
charts now being drawn in nanoseconds and covering every depth, and two over the axis byte
formatter — plus two integration tests in `chart_geometry` and one doctest. The total went
967 → 986. **The two integration tests are the ones that matter**, because the fourteen unit tests
draw charts and assert about strings, while `every_series_is_named_once` and
`legend_names_stay_with_their_swatches` assert about *geometry* over the widest legend the renderer
can be asked for — which is the only place the text-width estimate is checked at all. Nothing
measures text here, so nothing else can.

**[F17](../features/workload-grid.md) and [F18](../features/results-pages.md) added 71**, every one
of them in `shoal-bench`. Sixty unit tests net: sixty-three added — the grid's fourteen, the
row-width and key distribution generators' eleven, the two new chart kinds' fourteen, the family and
page registries' nineteen, and the arm selector's five — less the three that replaced six in
`page.rs`, whose section builders moved out to the ten page modules. Two integration tests over the committed corpus, both of which
exist to protect it rather than to test new code — one asserts the four new `ScaleFacts` fields stay
skipped when absent, because if one stops being skipped every existing workload re-serializes with
new nulls and the whole corpus churns on a change that measured nothing. And nine doctests. The
total went 896 → 967.

**The two that matter most are registry tests rather than behaviour tests.**
`family::every_workload_has_a_family` and `pages::every_page_renders_with_nothing_captured` are what
stop a new sweep landing on the site as an unexplained chart, and what stops a clean checkout
failing to build the book. Neither tests what any code computes; both test that a thing was not
forgotten, which is the failure mode the page they replace actually had.

**[F16](../features/client-builder.md) added 20.** Twelve `shoal-client` unit tests over
`PoolConfig`'s defaults, the endpoint resolver and `endpoint_order`; a new `pool.rs` integration
binary carrying six; and two doctests. The total went 876 → 896. **Six of the twelve unit tests
exist because the integration test was not enough** — `an_endpoint_that_is_down_is_tried_past`
passes against a build with the failover loop stubbed out, since `bb8`'s retries and the round
robin counter reach a live endpoint on their own, so what the loop actually buys is pinned over
`endpoint_order` directly. That is the general lesson: an end-to-end test that passes either way is
not a test of the mechanism.

**[F15](../features/client-server-split.md) moved where they live without changing what they
cover.** Splitting `shoal-core` into three crates re-attributed 177 unit tests and added 8. The
total went 868 → 876; nothing was lost, and a reader seeing `shoal-core` drop by more than half
should read this table rather than assume it was.

| Where | Tests | |
| --- | --- | --- |
| `shoal-proto` unit | 178 | the protocol, the SHQL parser, SCRAM, the TLS config — moved out of `shoal-core` |
| `shoal-core` unit | 156 | the engine: partitions, storage, the shard. Was 323 before the split. Up 5 with [F23](../features/self-sizing-staging-buffer.md), all of them over the staging buffer's sizing rule, and 3 with [F25](../features/read-buffers-are-filled-not-zeroed.md) over `RequestBody` — a body delivered in pieces, a stream that ends early, and an empty one |
| `shoal-client` unit | 20 | the client read loop and its error routing, and — new with [F16](../features/client-builder.md) — the builder, the pool defaults and the endpoint order; and — new with [F25](../features/read-buffers-are-filled-not-zeroed.md) — a response payload arriving in pieces and a connection that closes halfway through one |
| `shoal-bench` unit | 404 | the harness, the workloads, the charts, and — new with [F17](../features/workload-grid.md) and [F18](../features/results-pages.md) — the grid, the row-width and key generators, the family and page registries, and the two new chart kinds; and — new with [F19](../features/chart-legends.md) — the shared legend, the data-derived axis ticks, and the encryption charts in nanoseconds; and — new with [F20](../features/configuration-sweeps.md) and [F21](../features/benchmark-groups.md) — the configuration sweep, the group table, and `--group` in the registry; and — new with [F22](../features/row-size-benchmarks.md) — the three width passes, the configuration sweep's width repeats, both runner-side lists of profiled workloads, and the per-workload stage artifact; and — new with [Resolved #76](resolved/stage-join.md) — that the stage layer's collector judges each report rather than their sum; and — new with [Resolved #79](resolved/micro-only-capture-current.md) — that each page resolves the current capture of the layer it draws. **416 with `--features stage-profile`**, which adds the 9 over the report builder — one of them new with [F26](../features/archive-routed-requests.md), over a parked get's stages — and 3 over the `StageLog` |
| `shoal` integration | 209 | 18 binaries, one ignored. `disk_lookups.rs` is **new** with [Resolved #80](resolved/never-flushed-partitions.md) and holds exactly one test, because what it asserts is a process-wide count. `grouped_responses.rs` is **new** with [F27](../features/grouped-responses.md) and starts no server, for the same reason `archive_routing.rs` does not: it asserts what the derive generates and what rkyv does with it. `pool.rs` is **new** with [F16](../features/client-builder.md), `intent_log_batching.rs` with [F23](../features/self-sizing-staging-buffer.md), and `archive_routing.rs` with [F26](../features/archive-routed-requests.md). All but the last run against a live server; `archive_routing.rs` starts nothing, because `Ring`, the routing traits and rkyv are pure CPU over plain data — the same property `shoal/benches/routing.rs` relies on |
| `shoalctl` integration | 34 | the completion menu, driven the way the key handler does |
| `shoal-client-check` integration | 7 | **new.** A schema compiling and running against the client alone |
| `shoal-bench` integration | 21 | committed artifacts, chart geometry, CSS sync. Up 2 with [F17](../features/workload-grid.md), both guarding the committed corpus against the four fields it added, and 2 more with [F19](../features/chart-legends.md) over the legend's layout. **22 with `--features stage-profile`**, which adds `stage_join.rs` — the only test here that starts a server ([Resolved #76](resolved/stage-join.md)) |
| doctests | 45 | up 9 with [F17](../features/workload-grid.md): the row profile's five, `Seeded::at`, `queries_for`, and the two byte formatters; one with [F19](../features/chart-legends.md) over the third; and 3 with [F21](../features/benchmark-groups.md) and [F20](../features/configuration-sweeps.md) over `human_duration`, `numeric` and the page's list formatter |

The 8 added are the 7 in `shoal-client-check` and one in `hotpath_scopes`
(`a_scope_from_any_crate_loses_its_prefix`). The `chart_geometry` count did not move, but
`stacked_labels_have_room` began failing on real data and now passes for a reason rather than by
luck ([items 67, 68](resolved/chart-labels.md)).

Before the split it was **498 integration tests** (one ignored), **323 `shoal-core` unit
tests**, **29 doctests**.

**The encryption sweeps added 15 more** `shoal-bench` unit tests after that — 9 over the forty-eight
sweep arms (that every point is minted once, that a TLS arm differs from its twin in the wire alone,
that each sweep varies one thing, and that the byte budget keeps any one width from dominating a
capture) and 6 over the chart that pairs them, including that a pair whose runs overlapped is not
called a result and that the two sweeps cannot pair with each other. The integration and
`shoal-core` counts did not move: this is measurement, and no engine code changed.

That is up from 490, 301, and 25 with [F14](../features/encryption-in-transit.md) — one new
integration binary over encryption carrying 8 tests, 17 unit tests over what a TLS session is
loaded from and allowed to negotiate and over the key material handed to the kernel, 5 over the
config section, and 4 new doctests (`TlsClientOptions::new`, `Networking::tls`,
`ClientOptions::tls`, and `Shoal::with_options`, the last `no_run` because it needs a server).
5 more workload unit tests landed in `shoal-bench` over the encrypted transport arms.

**Nine of these need the `tls` kernel module and skip loudly without it**, the same way the thirteen
`stage-profile` tests sit outside a default run: the 8 in `tls.rs` and
`a_socket_reports_the_tls_ulp_once_it_is_attached`. That last one is the only assertion anywhere
that can tell a kTLS socket from a plaintext one, which makes it the one that would catch the
feature being quietly replaced by a userspace implementation — every other test in the file passes
either way, and the file says so.

Before that it was 482, 301, and 24 with [F13](../features/transport-workloads.md) — 8 unit tests in
`shoal-bench` over the eight transport workloads, and one doctest over `seed_batch`. The unit tests
are all about the axes rather than the driving: that every mode is minted at both row sizes, that
the two sizes of one mode differ in the row width and in nothing else, that a seed bundle fits in a
frame and is never empty, that the large arm's outstanding responses stay bounded in bytes, and
that only the single-send mode reports a service time. **The `shoal-core` unit count did not move,
because no engine code changed** — this is a measurement feature. The four driving paths are
covered by running them, not by a unit test: there is no way to assert a transport mode works
without a server, and a smoke capture exercises all eight.

Before that it was 473, 272, and 21 with [F12](../features/authentication.md) — one new integration
binary over authentication carrying 9 tests, 17 unit tests over the mechanism and its credential
store, 8 over the auth frame codec and the two handshake fields it added, and 4 over the config
section. The three new doctests are the first movement in that number since
[F10](../features/framing-and-protocol-evolution.md): `Credentials::scram`,
`StoredCredential::from_password` and `Shoal::with_credentials` each carry an example, and the
third is `no_run` because it needs a server. **No existing test changed what it asserts** — four
had to name a new struct field, which is a different thing.

Before that it was 470, 258, and 21 with [F11](../features/error-channel.md) — one new integration
binary over the error channel, 7 unit tests over the error frame codec and its pinned codes, 3 over
the response payload's precedence rules, and 4 over the client's frame dispatch and its dead
connection sweep. Four existing integration tests changed what they assert rather than being added
to: the two per table that used to pin "a read that failed is reported the same way an empty
partition is" now pin the two failure classes arriving as distinct codes. The doctest count did not
move.

Before that it was 456, 231, and 21 with [F10](../features/framing-and-protocol-evolution.md) — three
new integration binaries over the framing, the handshake and the fingerprint, 23 unit tests over
the frame codec and the fingerprint's mixing function, 2 over the client's read path including the
alignment guard, and 2 over the frame bound's config default. The doctest count did not move.

**Re-run and re-counted in August 2026** ([Review](review-2026-08.md)), binary by binary, and every
number on this page was already right: 455 integration tests passing plus the one ignored, 231 unit
tests, and 21 doctests split 10 in `shoal-bench`, 9 in `shoal-core` and 2 in `shoalctl`. That is
worth recording rather than assuming, because it is the one part of this documentation that is
cheap to verify and expensive to trust wrongly — the counts are what every other page's "up from"
chain hangs off. What the same run *did* change is the port table at the bottom of this page, which
described two colliding test binaries and now describes five.

Before that it was up from 454, 229, and 21 with
[Resolved #57](resolved/missing-archive.md) — one integration test per persistent table over a read
whose archive is not on disk, and two unit tests over `get_archive` itself and over how the failure
it now reports is classified. Before that it was up from 452, 225, and 21 with
[Resolved #16, 51](resolved/partition-load-failure.md) — one integration test per persistent table
over a partition read that cannot be done, and four unit tests over how a read failure is
classified. Before that it was up from 410, 219, and 21 with
[F9](../features/ephemeral-tables.md) — two new integration binaries over the ephemeral tables, six
unit tests over the storage engine that makes them ephemeral, and fifteen in `shoal-bench` over the
eight workloads they made possible. Before that it was up from 359, 219, and 16 with
[F8](../features/purpose-built-workloads.md).

**F8's count moved in both directions, which is the only time that has happened.** It added 61
tests to `shoal-bench` and removed 19 from `shoal`, and the removal is not lost coverage:

- `shoal/src/bencher.rs` had **11**. Five of them — the percentile and summary statistics — moved
  to `shoal-bench`'s workload harness with the code they test. The other six covered loading a
  baseline file, refusing one from another schema version, and a throughput figure; all three
  belonged to a comparison engine that F8 **deleted**, because `shoal-bench` has owned comparison
  since [F7](../features/bench-runner.md) and a second one that nobody reads can only disagree with the one
  that counts. Tests for deleted code are not coverage.
- `shoal/src/stages.rs` had **8**, and they were the eight a default run could not reach. They
  moved with the module into `shoal-bench` and are still feature-gated — but the command that runs
  them is now `cargo test -p shoal-bench --features stage-profile` rather than
  `cargo test -p shoal --features stage-profile`, which is the crate a person working on the
  harness already runs. **That is a change of address, not a fix**: a default
  `cargo test --workspace` still does not run them, and still would not notice if they broke. Nor
  were they, on their own, coverage of the layer — all eight passed against a tree where three of
  the four stage reports a capture produced had nothing in them, because all eight fabricate the
  halves they join ([Resolved #76](resolved/stage-join.md), which added the four that do not).

Before F8 it was up from 172, 219, and 11 with the 187 tests and 5 doctests
[F7](../features/bench-runner.md) added — the whole of `shoal-bench`, which is testable in a way
the three bash scripts it replaced were not. Before those it was up from 172, 215, and 11 with the
four stamp and offset tests
[F6](../features/stage-breakdown.md) added — and F6 also added **eight tests that a default run
does not reach**, because the stage report is behind the `stage-profile` feature. They run under
`cargo test -p shoal-bench --features stage-profile` (`-p shoal` until
[F8](../features/purpose-built-workloads.md) moved the module), and nothing in the default
workspace run would notice if they broke. Before those it was up from 172, 213, and 11 with the two tests
[F5](../features/flushed-sweep-gate.md) added to pin the premises its gate rests on — that staging a
response cannot release one, and that submitting a write moves neither watermark. It is up from
172, 199, and 11 with the fourteen archived-partition
tests added by [F4](../features/validated-archives.md) — the first coverage the `Accessible` arm has
ever had, because until F4 gave `MaybeLoaded` a buffer type parameter that variant could not be
constructed outside a running server. It is up from 168, 194, and 11 with the five config and cpu
selection tests added by [items 18 and 50](resolved/excluded-cores-typo.md) and the four
baseline versioning and throughput tests added by
[F3](../features/performance-harness.md). It is up from 157, 194, and 11 with the query error display coverage
added by [item 48](resolved/query-error-display.md) — eleven rendering tests and no unit tests,
because everything the fix does it does on screen. It is up from 136, 183, and 11 with the
projection coverage added by
[F2](../features/projections.md), and from 136, 178, and 11 with the compaction tail loss tests
added by [item 44](resolved/compaction-tail-loss.md) and the marker format test added by
[item 45](resolved/storage-marker-format.md) — both fixes are unit-testable end to end and
neither added an integration test, which is itself the
[observability gap](todos.md#observability) talking: no test can observe an event the server
emits. It is up from 135, 178, and 11 with the empty rotated log test added
by [item 14](resolved/empty-rotated-logs.md), from 135, 177, and 11 with the eviction accounting
test added by [item 13](resolved/eviction-log-underflow.md), from 133, 168, and 10 with the tablet map and
storage marker tests added by [items 11, 12 and 37](resolved/tablet-ring.md), from 132, 159, and 10 with the
multi-log recovery test added by [item 31](resolved/multi-log-recovery.md) and the recovery
counting tests added by [item 9](resolved/orphaned-update-intents.md), from 115, 129, and 8 with
the range coverage added by [F1](../features/sort-key-ranges.md), from 105 and 116 with the
sort-key selection coverage added with [item 8](resolved/sort-keys.md), and from 87, 95, and 6
before the row-order and `IN`/`OR` coverage added with [items 26 and 39](resolved/partition-order.md). The two persistent-table
binaries take about 24 seconds each; everything else finishes in well under a second. That is with
the default parallelism — the sorted binary takes nearly four minutes under `--test-threads=1`,
because its restart and eviction tests each wait out a real server shutdown.

Defects found while writing this page are in [Known Issues](known-issues.md); performance findings
are in [Optimizations](optimizations.md).

---

## What is covered

### Integration — `shoal/tests/`

| Binary | Count | What it reaches |
| --- | --- | --- |
| `persistent_sorted_table.rs` | 59, one ignored | insert; `exists` true and false; delete; delete after restart; delete surviving restart; delete and update when the partition is not resident; delete and writes surviving eviction; update; update intent replay; multi-log recovery; empty rotated log cleanup; acknowledgement surviving `SIGKILL`; five limit tests; two cross-shard tests; five row-order tests; six sort-key selection tests; two sort-key `exists` tests; six range tests including the archived seek and the memory/disk span; the paging walk; two range `exists` tests; three end-to-end SHQL tests; nine projection tests including the archived scan, the blocked disk read, the cross-partition order, and a projected and an unprojected get in one batch; and the two tests that reach the loader's failure path — a get whose archive cannot be opened ([Resolved #16, 51](resolved/partition-load-failure.md)), and a get whose archive is not on disk at all, which also asserts that the read did not create the archive it could not find ([Resolved #57](resolved/missing-archive.md)) |
| `disk_lookups.rs` | 1 | **new** with [Resolved #80](resolved/never-flushed-partitions.md): that a sorted partition which was never on disk is asked about **once**, not once per get. Six gets over two never-flushed partitions — one persistent, one ephemeral — must open exactly two `PersistentTable::block_on_load` spans; the tree before the fix opens six. The only test here that asserts on what the engine asked its storage engine rather than on an answer, which it has to be: both paths return the same rows, which is why nothing caught this for as long as it did. It holds one test on purpose, since the counter and the subscriber feeding it are process wide |
| `persistent_unsorted_table.rs` | 17 | insert; delete; update; delete and update when not resident; delete surviving eviction; insert after delete when not resident; zero limit; three multi-partition tests; three projection tests; and the unsorted twins of the unreadable-archive and missing-archive tests, because the two tables park and release blocked queries through different code |
| `ephemeral_sorted_table.rs` | 15 | the sorted read and write paths with no storage engine beneath them ([F9](../features/ephemeral-tables.md)): insert; `exists` true and false; delete; update; a limit; cross-shard row order; named sort-key selection; a range and its bounds; a range `exists`; an end-to-end SHQL range; a projection. Plus the three that are about the table rather than about sorted tables — that nothing is written to the storage directory, that nothing survives a restart, and that memory pressure evicts none of it |
| `ephemeral_unsorted_table.rs` | 12 | the same for the unsorted table, over a schema that also holds a persistent one and declares the ephemeral table **first** — which is what pins that a persistent table declared after an ephemeral one still gets its loader spawned, and therefore can still read a partition off disk |
| `tls.rs` | 8 | encryption against a running server ([F14](../features/encryption-in-transit.md)), every one of which skips without `modprobe tls`: that a query round trips over TLS at all; that a MiB response — about sixty four TLS records — comes back byte for byte and lands at the start of a buffer the client aligned; that SCRAM runs over TLS in that order and that TLS does not authenticate on its own; that a plaintext client is refused by an encrypted server and an encrypted client by a plaintext one; and that a client trusting an unrelated authority is refused, so the certificate is checked rather than merely presented |
| `auth.rs` | 9 | authentication against a running server ([F12](../features/authentication.md)): that the right credentials connect **and can then query**, which is what catches an exchange that left a byte unread on the stream the relays are handed afterwards; that a wrong password and a user that does not exist are refused in the same variant carrying the same sentence; that a client with no credentials is turned away in the `HelloAck` rather than after an exchange; that credentials offered to a server which requires none are ignored rather than used, so adding them to a client cannot break it against every server that has not opted in; and four raw-socket tests — that the ack names the mechanism the server selected and names none when it requires none, that a bundle of queries sent instead of a proof is refused **and the shard keeps serving**, that an auth frame past the 4 KiB auth bound is refused inside the 64 MiB frame bound, and that a refusal is flagged in its header |
| `errors.rs` | 3 | the error channel against a running server ([F11](../features/error-channel.md)): that a response too large for the frame bound a raw socket advertised comes back as an `Error` frame naming the query and both sizes rather than as a closed connection, that the *same* connection answers the next query normally afterwards — which is the whole of [Resolved #61](resolved/response-error-channel.md) — and that a get of a partition that was never written is still not a failure, which is the half of the distinction that did not change |
| `pool.rs` | 6 | the pool and the builder against a running server ([F16](../features/client-builder.md)): that a client the builder built answers a query at all, so the route every constructor now takes through it loses nothing; that a client given a dead endpoint ahead of a live one reaches the live one; that a client whose every endpoint is dead fails rather than reporting one that worked; that a pool held to two connections still answers eight concurrent sends, so the numbers reach `bb8` rather than being taken and dropped; and two that open no socket at all — an unsatisfiable pool and a builder with no endpoint are both refused by looking at the configuration rather than by failing to connect with it |
| `framing.rs` | 6 | the framing against a running server ([F10](../features/framing-and-protocol-evolution.md)): four raw sockets sending a hostile length prefix, an unknown message type, a frame that only travels the other way, and a frame from a version that does not exist — each asserting both that its own connection closed **and that a healthy client beside it still answers**, which is the assertion the shard-killing panics used to fail. Plus the two handshake refusals a raw socket can provoke, checking that the reply is a `HelloAck` written in a header the client can read, with the refused flag set and the server's own fingerprint in the body |
| `handshake.rs` | 2 | two schemas in one binary, differing by one field: that a client built from one cannot open a connection to a server built from the other and gets both fingerprints back, and that a client built from the server's own schema connects to the very same server and can query it — the second being what stops the first passing against a check that refuses everybody |
| `fingerprint.rs` | 6 | that the compile-time schema fingerprint actually moves when a schema moves: a field added, a row's fields reordered, a projection declared on an otherwise identical table, and that a whole row and its own identity projection agree. No server, so all six run instantly |
| `shql.rs` | 53 | SHQL parsing and binding against a real schema, including range binding and the role refusals, projection binding and its two refusals, plus completion suggestions |
| `storage_meta.rs` | 2 | that a storage directory restarts under the shard count that wrote it and refuses a changed one, end to end through a real server |
| `intent_log_batching.rs` | 2 | **new** with [F23](../features/self-sizing-staging-buffer.md): that a bundle of 128 rows wider than the staging buffer lands in at most a quarter as many writes — the reproduction for [O34](optimizations.md), which fails on the tree before the fix — and its control, that a `max_buffer_size` pinned to `buffer_size` gives back exactly one write per record. Both read the property off the intent log on disk by counting pad regions, rather than out of the writer, because what O34 is about is how many writes reached the device |
| `completion.rs` (`shoalctl`) | 34 | the completion menu, key handling, query wrapping, and rendering, including the projection slot; and the error box, the underline under the part of a query that failed to parse, the cases where that underline is refused as misleading, and that an error never becomes part of the query it describes |
| `lib.rs` (`shoal`) | 0 | ~~the bencher's percentile and summary statistics; baseline file handling…~~ `shoal` is a facade with no code of its own since [F8](../features/purpose-built-workloads.md), so it has nothing to unit test. See the note above for where the eleven went |
| `lib.rs` (`shoal-bench`) | 289 | the benchmark runner ([F7](../features/bench-runner.md)): artifact parsing; the registry and its `cargo test` style filtering, including that a partial selection becomes an anchored alternation criterion cannot mis-match; the noise band, including that the tier comes from the baseline so a change cannot pick the band that judges it; the macro layer's interval comparison; the staleness verdict matrix, including that a source digest can only narrow a verdict and never promote one to fresh; the capture plan, including that the uninstrumented rebuild is not inside a phase that a failure could truncate; the wipe guard; the six charts, including that no colour escapes the themed palette and no coordinate comes out `NaN`; and the hand-rolled date conversion over a whole 400-year Gregorian cycle. Since [F8](../features/purpose-built-workloads.md) also the workloads: that the declared id list cannot drift from the registered workloads, that two workloads cannot share a storage directory, that a seed is reproducible and its named streams independent, that each control-and-null pair differs in residency and nothing else, that the fanout curve names distinct partitions and pins its shard count, that each workload's median is picked from its own runs, and that a workload present on only one side of a comparison is named rather than dropped. Since [F9](../features/ephemeral-tables.md) also that each ephemeral workload and the persistent one it is a control for have plans that agree field for field — the gap between the pair is reported as the storage layer, so any other difference between them would be read as storage — and that no ephemeral workload asks to be restarted after seeding, which would measure an empty table. Since [F13](../features/transport-workloads.md) also the transport pair: that every mode is minted at both row widths, that the two sizes of one mode differ in the row and in nothing else, that a seed bundle fits inside the frame bound and never comes out empty, that the large arm's outstanding responses stay bounded in bytes rather than in queries, and that only the single-send mode reports a service time. Since [F14](../features/encryption-in-transit.md) also the encryption sweeps: that every point of both is minted once, that a TLS arm differs from its plaintext twin in the wire and in nothing else, that each sweep varies one thing rather than two at once, that the byte budget keeps the widest arm within an order of the narrowest, that no arm seeds past the memory limit the benchmark config sets, and — on the chart that pairs them — that a pair is joined on its recorded facts rather than its name, that an arm without a twin is dropped, that overlapping runs are never called a result however far apart the medians sit, and that the depth and client sweeps cannot pair with each other despite having identical facts at their first point |
| `committed_artifacts.rs` (`shoal-bench`) | 9 | that every artifact committed under `docs/perf/` still parses, that the frozen and trailing baselines differ by exactly the 24 `maybe_loaded` ids, that every pre-[F8](../features/purpose-built-workloads.md) macro capture still lifts to the single `macro/tmdb` workload with what it recorded intact, and that a field added to the version 1 shape fails a test naming it rather than being silently dropped. The drift alarm now applies to version 1 only: version 2 is written by the workloads in this crate out of these very structs, so there is no mirror left to drift |
| `css_sync.rs` (`shoal-bench`) | 4 | that every chart colour sentinel has a `fill` and a `stroke` rule in `docs/theme/charts.css` and every rule there matches a sentinel — a sentinel with no rule is drawn literally, bright red on a navy page — and that the stylesheet is still registered in `book.toml` |
| `chart_geometry.rs` (`shoal-bench`) | 6 | that no chart drawn from the real artifacts puts two labels on top of each other or draws outside its canvas. plotters is built without a font backend and estimates text extents, so this is the failure that no other test can see. Up 2 with [F19](../features/chart-legends.md): that a series is named exactly once, which is what an end label surviving would break, and that a legend entry's name stays with its own swatch rather than running under the next column — the only check anywhere on the width estimate the columns are laid out from |
| `stages.rs` (`shoal-bench`) | 8, **feature gated** | the stage report: that a bucket's stage means reconcile with its total, that a bucket is a window rather than one record, that an unreached stage is not reported as an instant one, that a write reports its four durability stages, that every record is accounted for as joined, one-sided or duplicate, that a stage the size of a clock read is marked rather than reported, that a batch level cost is labelled, and that a report from another schema version is refused. **Only built with `--features stage-profile`** — a default `cargo test --workspace` does not run any of them. Run them with `cargo test -p shoal-bench --features stage-profile`. Every one of these passed while three of the layer's four reports were empty, because every one of them fabricates the halves it joins |
| `stage_log.rs` (`shoal-bench`) | 3, **feature gated** | **new** with [Resolved #76](resolved/stage-join.md): that a query sent and never answered is counted rather than dropped when the driver returns, that two slots' logs pool into one — which every per query driver depends on — and that the streaming path keeps one query in every `--stage-sample`, the same rule the server applies |
| `stage_join.rs` (`shoal-bench`) | 1, **feature gated** | **new** with [Resolved #76](resolved/stage-join.md), and the one test here that starts a server: one grid arm at smoke scale under `stage-profile`, asserting the report has a join in it, that neither half is one-sided, that both halves of the mixture produced a breakdown, and that a bucket has stages in it. It fails against the tree before the fix with `joined: 0`, which is the whole defect. This is the check the layer never had — that a workload on `STAGED_WORKLOADS` can actually produce a joined record, as opposed to being correctly listed |

The restart, eviction, and `SIGKILL` tests are the valuable ones: they are the only tests that
exercise durability end to end, and they exist because
[items 1-3](resolved/durability.md), [4](resolved/unsorted-disk-consultation.md), and
[5](resolved/resurrected-deletes.md) needed them.

### Unit — `shoal-core`

| Module | Count | What it reaches |
| --- | --- | --- |
| `shared/queries/parser/tests.rs` | 59 | the SHQL grammar, including `IN` lists, `OR` folding, each range operator, the folding and refusals around a range, and the projection slot with its offsets |
| `shared/queries/parser/complete/tests.rs` | 27 | completion suggestion generation, including the range operator tokens and a projection standing where the star does |
| `.../storage/fs/tests.rs` | 27 | the intent log reader against real files, including which tail shapes are damage and which are how a healthy log ends, and what a compaction is about to throw away with the log it deletes; how a failed partition read is classified — which of the three classes is retried, and that an unrecognised error is given up on rather than retried forever ([Resolved #16, 51](resolved/partition-load-failure.md)); and `ArchiveMap::get_archive` over an archive that is not on disk, that it names the archive rather than creating one and that the failure is never retried ([Resolved #57](resolved/missing-archive.md)) |
| `.../storage/fs/stream_tests.rs` | 19 | `StreamWriter` alignment, padding, and watermarks, including that submitting a write advances neither watermark in either durability mode — the premise [F5](../features/flushed-sweep-gate.md)'s sweep gate rests on. Up 5 with [F23](../features/self-sizing-staging-buffer.md): the five pure tests over `staging_target`, which need no executor, no `DmaFile` and no schema, since the sizing rule was deliberately factored out as a free function so it could be tested without one |
| `tables/partitions.rs` | 59 | tombstone bookkeeping, limits, sort-key selection and range selection on `get` and `exists`, the empty-range guard, `merge_from_disk` sizing, the recovery counting that separates a correctly dropped update from a lost one, and the projected scan across all three selections; plus the archived arm of all of those — that a truncated or root-corrupted archive is refused, that the unchecked read lands on the same reference the checked one does, and that an archived partition answers every selection identically to a resident one holding the same rows ([F4](../features/validated-archives.md)) |
| `shared/protocol/tests.rs` | 38 | the frame format itself ([F10](../features/framing-and-protocol-evolution.md)): that every message type is still written as the byte it has always been written as and that a zeroed buffer is not a valid one, that every flag bit is where it was and an unknown one is round-tripped rather than masked off, that both preambles are the size they were before the header existed, that a version this build does not speak is refused but still readable, that a length past the bound and a payload past a `u32` are both refused before anything allocates or truncates, and that the fingerprint's separator stops two adjacent fields concatenating. Since [F11](../features/error-channel.md) also the error frame: that every error code is still written as the number it has always been written as and that one this build does not know reads as `Unknown` rather than failing to decode, that an error frame round trips with its flag and its type byte, that its query id sits at exactly the offset a response frame's does — which is what lets the client read one preamble for both — that a frame shorter than its own fixed fields is refused, that a message past the four kibibyte bound is refused in both directions even though the frame bound would allow it, and that a message which is not valid UTF-8 still delivers the code it came with. Since [F12](../features/authentication.md) also the auth frames: that every mechanism and every status is still written as the byte it has always been written as and that a zeroed buffer decodes as neither — in particular that it is never a success — that a mechanism bit this build cannot name is carried through rather than masked off, that selection walks the *server's* preference order rather than the client's bits, that both frames round trip and a refusal is flagged in its header as well as its status byte, that a payload past the four kibibyte auth bound is refused in both directions inside a frame bound that would allow it, that a body too short to hold its own fixed part is refused rather than indexed into, and that a `HelloAck` naming a mechanism from a build that does not exist yet reads as none |
| `shared/responses.rs` | 3 | the precedence rules a failure has to obey ([F11](../features/error-channel.md)): that a failed share wins a merge in either direction — the one place three shards' rows can hide a fourth shard's failure — that an error response is never a success however permissive the `QuerySuceededOpts` are, since the options say which outcomes count and a failure is not an outcome, and that a limit trims rows and leaves a failure alone |
| `client.rs` | 6 | that a response payload lands at the start of a sixteen byte aligned allocation across seven awkward payload lengths — the guard on the two-read structure the zero-copy response path rests on — and that a frame larger than the client's own bound is refused before it is allocated for. Since [F11](../features/error-channel.md) also the frame dispatch: that an error frame ahead of a response leaves that response's payload aligned, which is the guard on the type branch that now sits between the two reads; that an error frame reaches the query it names; that one for a query nobody is waiting on is dropped rather than ending the read loop and taking every other query on that connection with it; and that a connection which dies fails the queries written to it **and no others**, which is what stops a dead socket failing the other forty nine connections in the pool |
| `shared/queries.rs` | 10 | sort-key normalization, and `SortRange` emptiness and containment |
| `tables/storage.rs` | 7 | `PendingResponse` release against a durable watermark — including that staging a response never releases one, which is why [F5](../features/flushed-sweep-gate.md) can skip its sweep on a write — and `RecoveryStats` merging and cleanliness |
| `server/ring.rs` | 6 | the tablet map: that an empty one cannot be built, that tablets are split evenly and no shard is starved, that ids come from the high bits so a split stays incremental, and that two independently built maps agree |
| `server/conf.rs` | 11 | that a misspelled resource key fails the load instead of being dropped, that `exclude_cores` is parsed and removes both threads of a core, that cpu selection is deterministic and fills distinct physical cores before pairing onto an SMT sibling, and that a config which never mentions `max_frame_bytes` still gets one — which is what let the frame bound be added without touching the committed `shoal.yml` every frozen benchmark was captured against. Since [F12](../features/authentication.md) also the `auth` section: that a config with no such section requires nothing, which is the property that keeps every existing deployment and every benchmark connecting; that a named password is derived at load and nothing downstream holds one; that a derived credential can be written in the file instead; and that a mechanism name nothing knows fails the load rather than yielding a server which requires proof it can never grant |
| `server/meta.rs` | 4 | claiming a storage directory, reopening it under the same shard count, refusing a changed one, and refusing a marker whose format this build does not know |
| `tables/persistent.rs` | 2 | the two pieces of arithmetic on the shard memory counter: that a shrink subtracts instead of wrapping, and that an eviction summarizes itself without underflowing on a drifted counter |
| `.../storage/none.rs` | 6 | the watermark that stands in for an intent log's positions ([F9](../features/ephemeral-tables.md)): that commits hand out distinct rising positions, that a release covers every one of them, and that a sweep is asked for exactly when a response is parked and not otherwise — the last being the only thing that ever answers an ephemeral insert |
| `shared/auth/tests.rs` | 17 | the mechanism itself ([F12](../features/authentication.md)), all of it without a socket: that RFC 7677's test vector produces RFC 7677's proof and its server signature byte for byte — the one test standing between "this implements SCRAM" and "this implements what this repository thought SCRAM was" — that the right password authenticates and the wrong one does not, that a user which does not exist fails in the same variant as a wrong password **and gets a challenge with the same stable, plausible salt**, that a tampered proof, a replaced nonce, an echoed nonce nobody sent and a tampered server signature are each refused by the half that should refuse them, that a client asking for channel binding is turned away rather than quietly answered without it, that messages out of order are refused on both sides, that a username containing a comma or an equals survives the exchange rather than injecting a field into the signed message, and that a stored credential carries no password, salts freshly per derivation, round trips through the YAML a config spells it in, and prints neither key in a log line |
| `.../storage/fs/map.rs` | 1 | map intent replay |

The storage tests run against a real filesystem on purpose — `TempDir::new_in(CARGO_TARGET_TMPDIR)`
rather than `/tmp` — because glommio silently disables `O_DIRECT` on tmpfs, which would make
alignment unenforced and `fdatasync` meaningless (`shoal/tests/utils.rs`, and the note in
[TODOs](todos.md#storage-engine-abstraction)).

---

## What is not covered

Ordered by what would find the most, soonest.

### Compaction and archive rotation

Nothing. `MIN_ARCHIVE_COMPACTABLE` is 10 MiB (`.../fs/compactor.rs:33`) and no test writes near
that, so `compact_archives` never does real work in the suite. That leaves the archive read path,
entry rewriting into a new active archive, the 50% utilization decision, archive deletion, and
`sort_by_load` all unexercised.

`build_pressured_config` (`shoal/tests/utils.rs`) shrinks the *intent log* to 4 KiB so generations
advance quickly, which is what the eviction tests need — but archives are a separate threshold and
nothing shrinks it. Making `MIN_ARCHIVE_COMPACTABLE` configurable is already an open TODO
([TODOs](todos.md#storage)) and is the cheapest way in.

This is the largest gap on the page: compaction is the only component that rewrites committed data.

*Intent* log compaction has one test now — `empty_rotated_intent_logs_are_deleted`, added with
[item 14](resolved/empty-rotated-logs.md) — but it asserts on what the compactor removed, not on
what it wrote. The archive side above is untouched by it.

### Multi-log recovery

**Now covered**, by `multi_log_recovery_keeps_earlier_intents`
(`persistent_sorted_table.rs`), written to reproduce
[item 31](resolved/multi-log-recovery.md). It is worth reading before writing another recovery
test, because it works around the thing that made this gap persist: the only way to leave an
inactive log behind is to interrupt a compaction, and a test cannot interrupt one reliably. So it
does not try. Two real single-shard servers write two genuine intent logs, and the test then
arranges them on disk — one renamed to `Shard-0-inactive-1`, the other copied in as the active
log — into the state an interrupted compaction leaves behind. No `SIGKILL`, no timing.

Writing it also corrected the gap's premise. Two inactive logs are not needed: the active log is
always replayed last, so one inactive log plus the active log is enough — a single interrupted
compaction, rather than two. ~~And that is a state a clean shutdown produces.~~ It was, while
every clean shutdown left an empty inactive log behind; since
[item 14](resolved/empty-rotated-logs.md) a clean shutdown leaves none, which is why the test
stages the log by hand rather than arranging for one.

What is still not covered is a recovery spanning *three or more* logs, and one where the same
partition is touched in three different generations.

### Anything that is only reported through `tracing`

`ShoalPool::start` does not initialize a subscriber — `trace::setup` is called by the example
binary, not by the server — and since [F8](../features/purpose-built-workloads.md) neither the
example nor the benchmark workloads call it either. No integration test can therefore observe
any event the server emits, and none tries.

That is what the per-shard recovery summary added by
[item 9](resolved/orphaned-update-intents.md) runs into: the counting that feeds it is unit
tested from four directions, but the event itself — its level, its fields, and that it fires once
per shard — was verified by hand against the `tmdb` example, which no longer exists in that
form, and has no automated coverage. The
same is true of the compaction summary and every eviction event.

Closing this needs a subscriber a test can install and read back. The obstacle is that a
subscriber is process-global while these binaries run their tests in parallel threads
([below](#the-suite-cannot-safely-run-its-binaries-in-parallel)), so captured events would have
to be attributed to the test that caused them.

**A shard that dies is only reported this way, which is why no test can assert on one.**
`ShoalPool::exit` logs each shard's join result at `ERROR` and returns `Ok(())` regardless
([item 58](known-issues.md#58-a-shard-that-dies-is-not-reported-to-whoever-started-the-pool)). So a
test whose server lost a shard sees it as a query that never came back, and cannot say why — which
is exactly how [Resolved #57](resolved/missing-archive.md) presented, and why establishing what
killed the shard needed a temporary `eprintln!` rather than an assertion.

### The streaming client APIs

`stream()`, `stream_unordered()`, `ShoalResultStream::skip`, and the out-of-order reassembly
through `pending: BTreeMap` / `BTreeSet` (`shoal-core/src/client.rs`) have **no test at all**.
Every integration test goes through `send`, `exec`, `send_one`, or `exists`.

That is where `skip(0)` panicking has been able to sit unnoticed
([item 23](known-issues.md#23-client-stream-and-pool-rough-edges)), and the reassembly logic is
the part of the client most likely to be wrong, since it is the only part that has to hold state
across responses.

**A second defect surfaced here during the [August 2026 review](review-2026-08.md)**, and it is
worth reading as evidence about the gap rather than only about the bug: a stream that is not
drained to its last response never releases its entry in the client's response map, because the
release lives inside the `if end` arm of `next`
([item 60](known-issues.md#60-a-result-stream-that-is-not-drained-to-the-end-leaks-its-slot-in-the-client)).
Every supported way of ending a stream early leaks. It has sat there since the streams were
written, and no test could have caught it, because no test constructs one.

What the contract of these buffers actually is — and why keying them by response index is sound —
is now written down in [The Client](../api/client.md#the-reorder-buffers), which is the thing to
read before writing the tests this section is asking for.

### Concurrency and the connection pool

~~Nothing issues concurrent queries, exhausts the pool, or forces a reconnect.~~ **Partly closed by
[F16](../features/client-builder.md)**, which added `pool.rs`: `a_pool_sized_by_the_caller_still_answers`
holds a client to two connections and sends eight queries at once, so the pool is exhausted and
shared for the first time, and `an_endpoint_that_is_down_is_tried_past` forces a connect to fail and
be retried elsewhere.

**What is still not reached is the health checks.** `is_valid` and `has_broken` are exercised
incidentally by any test that checks a connection out, but nothing kills a peer underneath a live
client, so the case they are known not to catch — a server gone without its socket being reset
([item 23](known-issues.md#23-client-stream-and-pool-rough-edges)) — is still not something a test
would notice either way. That needs a harness that can take a server away from a client mid-query,
which is what [D6](../direction/connection-pool.md#how-it-would-be-measured) calls the single most
valuable test infrastructure this client could grow, and it is what the `Ping` work will need.

### Filters, end to end

The SHQL tests confirm a filter is *parsed and bound* into the query
(`shoal/tests/shql.rs`, `binds_unsorted_filters` and friends), and the partition tests confirm
limits are applied. Nothing confirms the server actually excludes a row: no test asserts that a
get with a filter returns fewer rows than the same get without one.

### Unsorted tables lag sorted ones

Sorted tables now have `exists`, multi-partition gets, limits, and cross-shard coverage. Unsorted
tables have none of those — no `exists` test, no multi-partition get, no cross-shard test. The two
implementations have diverged before ([item 4](resolved/unsorted-disk-consultation.md) was
unsorted-only), so the asymmetry is worth closing.

### Lifecycle and hostile input

- **Client disconnect** — [item 32](known-issues.md#32-a-disconnected-client-is-never-cleaned-up-anywhere).
  No test opens a connection, closes it, and asserts anything was released.
- **A shard that never answers its share** — [item 33](known-issues.md#33-collected-split-query-state-has-no-expiry).
  The cross-shard tests only cover the happy path.
- ~~**Malformed wire input**~~ — covered since [F10](../features/framing-and-protocol-evolution.md).
  `shoal/tests/framing.rs` sends an oversized length prefix, an unknown message type, a frame that
  only travels the other way, and a frame from a version that does not exist, each from a raw
  socket beside a healthy client ([Resolved #34](resolved/unvalidated-length-prefix.md)). What is
  still uncovered is a **truncated body** — a header followed by fewer bytes than it claimed, which
  parks the relay rather than failing it, because nothing anywhere has a deadline
  ([TODOs](todos.md#timeouts)).
- **Composite sort keys** — [item 42](known-issues.md#42-shql-cannot-express-a-composite-sort-key).
  Sort-key selection is covered on both scans and both table forms
  ([item 8](resolved/sort-keys.md)), but every table in the suite has a single-field sort key, so
  nothing exercises a tuple `Sort` through SHQL or through a seek.

---

## The suite cannot safely run its binaries in parallel

Filed as [item 38](known-issues.md#38-integration-test-binaries-all-bind-the-same-ports), and
repeated here because it bounds the confidence of everything above.

Ports come from a counter that is **per test binary** (`shoal/tests/utils.rs:48-53`):

```rust
static PORT_COUNTER: AtomicU16 = AtomicU16::new(13000);
fn get_unique_port() -> u16 { PORT_COUNTER.fetch_add(1, Ordering::SeqCst) }
```

Cargo runs binaries in parallel, so they all start at 13000 together. Capturing the `listening on`
line (`conf.rs:166`) from each binary in turn:

| Binary | Ports bound |
| --- | --- |
| `persistent_sorted_table` | 13000-13102, plus 13900 and 13901 |
| `persistent_unsorted_table` | 13000-13033 |
| `ephemeral_sorted_table` | 13000-13015 |
| `ephemeral_unsorted_table` | 13000-13014 |
| `storage_meta` | 13000-13002 |
| `framing` | 13000-13005 |
| `handshake` | 13000-13001 |
| `pool` | 13000-13002 |
| `disk_lookups` | 13000 |

**It is nine binaries now, not two.** This table listed the two persistent ones; the two ephemeral
binaries arrived with [F9](../features/ephemeral-tables.md), `storage_meta` was never counted, and
`framing` and `handshake` arrived with
[F10](../features/framing-and-protocol-evolution.md) — which made this worse in a way worth
naming, because both of them connect raw sockets to a port by number and would be handed a server
belonging to another binary just as readily as a client would. `pool` arrived with
[F16](../features/client-builder.md) and makes it worse again in a **new** way: it is the first
binary whose tests assert on *failing* to connect, and a `SO_REUSEPORT` server from another binary
answering on what it believes to be a dead port would turn `a_client_with_no_live_endpoint_fails`
into a flake. It avoids that by taking its dead ports from the kernel — bind port 0, read the
assignment back, release it — rather than from the shared counter, which is also the fix
[item 38](known-issues.md#38-integration-test-binaries-all-bind-the-same-ports) proposes for every
binary here. `disk_lookups` arrived with
[Resolved #80](resolved/never-flushed-partitions.md) and binds one port, 13000 — the *most*
contended number in the table, and the one where a server from another binary answering its client
would take its partitions somewhere it is not counting and turn the count it asserts on into a
zero.
Every range grows with every test that restarts a server — the sorted binary was 13034 when this
was first measured and 13100 at the last one. Re-measure them with `-- --nocapture` rather than
trusting the numbers above; the overlap is the point, not the endpoints.

**Every port any of the other six binds is also bound by the sorted one.** The second bind does
not fail: glommio sets `SO_REUSEPORT` on listening sockets, so it succeeds silently and the kernel
load balances connections between the servers. A client can be handed a server belonging to a
different test, with a different schema and a different temp dir, and nothing reports it.

`persistent_sorted_table.rs:829` also hardcodes `let port = 13900`, which collides with itself
across concurrent runs of that one binary.

**This has not been observed to fail.** The full suite was run four times while establishing the
baseline above, and twice more during the [August 2026 review](review-2026-08.md) that re-measured
the table; it passed every time — the servers simply are not alive on the same port at the same
instant. Nothing arranges that.

Binding port 0 and reading back the assigned port would remove the shared namespace entirely,
which is the only fix that does not just relocate the problem to the next binary someone adds.
