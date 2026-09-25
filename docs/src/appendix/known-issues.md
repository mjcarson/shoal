# Known Issues

A severity-ranked index of open defects on the `ZeroCopyResponses` branch. Each entry names
the symptom, the cause, and a `file:line`.

**How these were established.** Almost everything here comes from reading the source. Entries
that were later confirmed by reproduction say so on their resolved page, and item 14 was the last
one on this page to carry that note before it moved
([Resolved #14](resolved/empty-rotated-logs.md#evidence)). Item 75 is the exception and says so in
its own **Evidence** note: it was found by reading a committed capture artifact against the page
generated from it, which is a third way of finding a defect that this list had no instance of until
`f22-row-size` was taken. Item 76 was found the same way and is now
[resolved](resolved/stage-join.md), where its note records the run that reproduced it.

**Line numbers drift, and they had.** Every citation on this page was re-resolved against the tree
in August 2026 ([Review](review-2026-08.md)) and most of them had moved — item 16's whole table by
a hundred lines or more. They are correct as of that sweep and will rot again; the symbol name
beside each one is what a reader should grep for, and the line is a shortcut. That review also
found that two entries had been quietly overtaken by the code — item 17 is now mostly fixed, and
item 25 is fully [resolved](resolved/claude-md-drift.md).

Reading is enough to find a defect and not always enough to characterise it. Item 13 was filed
from a reading that called its panic reachable; trying to reproduce it showed the panic was
latent and that the line's live cost was a different one
([Resolved #13](resolved/eviction-log-underflow.md#evidence)). An entry here is a claim about the
source, not yet a claim about a running server.

Performance findings are catalogued separately in [Optimizations](optimizations.md), and what the
test suite does and does not reach is in [Test Coverage](test-coverage.md).

Defects that have been fixed move to [Resolved Issues](resolved-issues.md), one page each,
carrying the reasoning and the invariants the fix depends on. Item numbers are shared between
the two pages and never reused, so a number appears on exactly one of them — which is why this
list starts at ~~15~~ ~~16~~ 19 and skips 25, 26, 27, 30, 31, 32, 33, 34, 36, 38, 39, 43, 44, 45, 48, 51, 56, 57, 58, 61, 67, 68, 74,
76, 78, 79, 80, 82, 83, 84, 85, 86, 88, 89, 90, 94, 95, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 114, 115, 116, 120, 121, 122, 123 and 124, and
why ~~item 91~~ ~~item 97~~ ~~item 100~~ ~~item 103~~ ~~item 107~~ ~~item 109~~ ~~item 110~~ ~~item 112~~ ~~item 113~~ ~~item 119~~ ~~item 124~~ ~~item 125~~ ~~item 119 is the newest entry here again~~ ~~and the newest number~~ ~~with 114 the newest number, on the resolved page~~ ~~and the newest number~~ ~~with 121 the newest number, on the resolved page~~ ~~with 124 the newest number, on the resolved page~~ ~~and the newest number~~ ~~with 125 the newest number, on the resolved page~~ ~~with 126 the newest number, on the resolved page~~ ~~with 127 the newest number, on the resolved page~~ ~~item 129 is the newest entry and the newest number~~ ~~item 131 is the newest entry and the newest number~~ item 132 is the newest entry and the newest number, and why 16 and 112 are on the resolved page beside them, and why 17, 30, 33, 43, 78, 79, 80, 82,
83, 84, 85, 86, 88, 89, 90, 94, 95, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 115, 116, 120, 121, 122, 123, 124, 125, 126, 127 and 128 are on the resolved page, with 27, 32 and 36 beside them. **126 never appeared here**:
it was filed and fixed in one change, from a user's run of `tmdb_dataset` on a host without
`/opt/shoal` ([Resolved #126](resolved/storage-directory-unusable.md)). **127 never appeared here**
either: it was filed and fixed in one change, from a user's bootstrap of a wizard-built inventory
([Resolved #127](resolved/wizard-loopback-address.md)). **128 never appeared here** either:
it was found by the same user's TMDB load against the lab, reproduced by a fixture test, and
fixed in the change that filed [item 129](#129-an-overloaded-group-answers-outcomeunknown-rather-than-shedding)
beside it ([Resolved #128](resolved/hop-deadline-margin.md)). **120 and 121 never appeared here**:
they were found by reading the code around item 30 - the unsorted table's version of its arm
charging the shard twice, and the duplicate read that let either arm be reached - reproduced by
a harness that builds a shard's tables without a shard, and fixed with it
([Resolved #30, 120, 121](resolved/resident-copy-collision.md)). **115 and 116 never appeared here**:
they were the two fixture tests F52's whole-workspace run failed and each passed alone -
a crash between the retry sidecar and the checkpoint file forgetting every identity below
the checkpoint, reproduced by a crash point between the two
([Resolved #115](resolved/retry-sidecar-crash-window.md)), and a test reading the newest map
version once while the detector committed another
([Resolved #116](resolved/map-version-test-snapshot.md)) - and were fixed in the change that
filed them. **114 never appeared here**: it was found by
[F51](../features/cluster-deployment.md)'s deployment test - the cluster tab's model counted
no voters in any cluster - reproduced against a real node and by a unit test, and fixed in the
same change ([Resolved #114](resolved/cluster-tab-voter-count.md)). **111 never
appeared here**: it was found by [F47](../features/local-rehome.md)'s crash matrix - a read
landing while an archive closed panicked the executor - reproduced against the map alone and
fixed in the same change ([Resolved #111](resolved/archive-removal-borrow.md)). **108 never
appeared here**: it was found by [F45](../features/replica-migration.md)'s first smoke run and
fixed in the same change ([Resolved #108](resolved/cluster-arm-overrides-dropped.md)). **99 moved at M8**
([Resolved #99](resolved/durable-log-reversion.md)): it was filed at M6, left for M8 by M7 on
purpose, and reproduced before it was fixed. **101 never appeared here
either**: it was found by an M6 test and fixed in the same change
([Resolved #101](resolved/short-lived-member-detection.md)), reproduced first. **104 and 105
never appeared here either**: both were found by reading the code for M7, reproduced by a
fixture test each, and fixed before the milestone's own work began
([Resolved #104](resolved/segments-recompacted-after-restart.md),
[Resolved #105](resolved/volatile-groups-never-purged.md)). **33 moved at M5**
([Resolved #33](resolved/gather-expiry.md)): it was the oldest open hang, and it was reproduced
before it was fixed. **94 never appeared here either**: it
was found and fixed while [F38](../features/inter-node-transport.md) made a peer link a client,
and was the first half of item 32 ([Resolved #94](resolved/disconnected-client-cleanup.md)), whose
second half is now [resolved](resolved/client-gone-broadcast.md) too. **38, 58 and 88 moved together**
([Resolved #38, 58, 88](resolved/pool-readiness.md)): three symptoms of one cause, the pool
returning before its shards had bound. **91 was found by fixing them** — the honest `exit` that
closed 58 surfaced a compactor that had been dying silently for as long as one test had existed. **79, 82, 83, 84, 85 and 86 never appeared here at all**: each was
found and fixed in the same change ([Resolved #79](resolved/micro-only-capture-current.md),
[Resolved #82](resolved/one-line-per-capture.md),
[Resolved #83](resolved/default-metric-half-the-corpus-cannot-answer.md),
[Resolved #84](resolved/identical-facts-one-line.md),
[Resolved #85](resolved/chart-framed-for-the-last-selection.md),
[Resolved #86](resolved/hover-label-reads-in-decades.md)), which is allowed and is
worth noting because it makes the numbering look like six entries went missing. Item 80 is the other way round —
it was filed here rather than fixed, because the fix turned on a question about the storage layer
that reading `block_on_load` alone could not answer, and stayed here until somebody answered it
([Resolved #80](resolved/never-flushed-partitions.md)). The exceptions are items 20, 24, ~~32,~~ 54 and 73, which were only
partly fixed: the open remainder is here and the rest is there. Items 9, 15, 16, 32 and 51 were each one such
exception until their last part was fixed, and are now on the resolved page alone - 16 in four
parts, the last of which ([Resolved #16](resolved/hot-path-panics.md)) also found that the item's
write path claim was wrong and filed what it should have said as item 122, since
[resolved](resolved/intent-log-failure.md) with 123 and 124 beside it; item 25 was one
in the other direction — it had one row left open, that row was fixed, and the whole item
[moved](resolved/claude-md-drift.md).

**Baseline as of writing:** `cargo check --workspace --all-targets` passes with warnings;
`cargo test --workspace` passes — ~~**1,238 tests**~~ ~~**1,289 tests**~~ ~~**1,320 tests**~~ ~~**1,342 tests**~~ ~~**1,361 tests**~~ ~~**1,382 tests**~~ ~~**1,398 tests**~~ ~~**1,414 tests**~~ ~~**1,432 tests**~~ ~~**1,449 tests**~~ ~~**1,467 tests**~~ ~~**1,475 tests**~~ ~~**1,484 tests**~~ ~~**1,492 tests**~~ ~~**1,529 tests**~~ ~~**1,541 tests**~~ ~~**1,543 tests**~~ ~~**1,549 tests**~~ ~~**1,555 tests**~~ ~~**1,564 tests**~~ ~~**1,576 tests**~~ ~~**1,587 tests**~~ ~~**1,589 tests**~~ ~~**1,605 tests**~~ ~~**1,608 tests**~~ ~~**1,611 tests**~~ ~~**1,614 tests**~~ ~~**1,619 tests**~~ ~~**1,629 tests**~~ ~~**1,633 tests**~~ ~~**1,634 tests**~~ ~~**1,635 tests**~~ ~~**1,636 tests**~~ ~~**1,637 tests**~~ ~~**1,639 tests**~~ ~~**1,640 tests**~~ ~~**1,641 tests**~~ ~~**1,642 tests**~~ ~~**1,644 tests**~~ ~~**1,647 tests**~~ ~~**1,651 tests**~~ ~~**1,653 tests**~~ ~~**1,654 tests**~~ ~~**1,655 tests**~~ **1,656 tests**, seven ignored, plus ~~13~~ 14
more behind `--features stage-profile` that a default run does not reach ([Test Coverage](test-coverage.md)) -
with the fixture binary run at `--test-threads 6`, since at the default thirty-two nineteen of
its ~~fifty-four~~ ~~sixty-four~~ ~~seventy-one~~ ~~eighty~~ ~~eighty-nine~~ ~~ninety-two~~ ~~ninety-five~~ ninety-seven fail under the load (item 100) and every one of them passes at six;
two of `persistent_unsorted_table.rs` fail about one run in five of that binary (item 107).
[F53](../features/inventory-wizard.md) added 16 and took it to 1,605: thirteen `shoalctl` unit
tests, a new binary `shoalctl/tests/wizard.rs` (2), and one in `deploy_render.rs`. The workspace
run at six threads with `--no-fail-fast` passed every test.
[Resolved #127](resolved/wizard-loopback-address.md) added 3 `shoalctl` unit tests and took it to
1,608, and [F54](../features/tmdb-dataset-deployment.md) 2 in the new `tmdb-dataset` crate and an ignored doctest,
to 1,611. [Resolved #128](resolved/hop-deadline-margin.md) added 3 (a fixture test, a `shoal-core`
unit test, a `tmdb-dataset` unit test) and took it to 1,614. Its workspace run failed eight
fixture tests: two passed alone, and six could not bind ~~port 12000~~ ports 12001 and 12002
because a deployed lab node held them on the same host ([Test Coverage](test-coverage.md)), since
[resolved](resolved/fixture-default-peer-ports.md) as item 134. The
[distributed cluster testing](../cluster-testing/overview.md) chapter's fixes (items 60, 130, 131,
133 to 157, O61, O62, O63, O65 to O68) and its driver added 37 and took it to 1,656 ([Test Coverage](test-coverage.md)).
[Resolved #27](resolved/shql-quote-escape.md), [#32](resolved/client-gone-broadcast.md),
[#36](resolved/staged-tail-deadline.md) and [#125](resolved/retry-unknown-outcome.md) added 11
and took it to 1,587. There are two new binaries, `retry_outcome.rs` (3) and `staged_flush.rs`
(1), plus one test in each of `shql.rs` and `client_disconnect.rs`, four in `shoal-proto` and
one in `shoal-client`. The workspace run at six threads with `--no-fail-fast` failed two fixture
tests, and each passed when run alone: `a_dead_primary_fails_writes_only_until_its_election`
(item 100's shape) and `local_rehome_recovers_after_each_crash_point`, which no earlier run had
recorded failing under load.
[Resolved #122](resolved/intent-log-failure.md), [#123](resolved/parked-get-key.md) and
[#124](resolved/unsorted-update-generation.md) added 12 and took it to 1,576: one new binary,
`intent_log_failure.rs` (4), five in `resident_reads.rs`, and three `shoal-core` unit tests. They
filed item 125. The workspace run at six threads with
`--no-fail-fast` passed every test. An earlier fail-fast run failed
`a_dead_primary_fails_writes_only_until_its_election` (item 100's shape).
[Resolved #16](resolved/hot-path-panics.md) added 9 and took it to 1,564: one new binary,
`hot_path_failures.rs` (5, on the `table_backlog.rs` harness), and four unit tests in
`shoal-core`; one `shoal-proto` test moved its probe of an unclaimed code from 13, which
`StorageWrite` now claims, to 14. It filed items 122, 123 and 124. Under the workspace run at six
threads three fixture tests failed and each passed alone:
`a_dead_primary_fails_writes_only_until_its_election`,
`lost_response_retry_returns_original_result` and
`scheduled_scrub_quarantines_without_an_operator` (item 100's shape).
[The remainder of item 15](resolved/backlog-bounds.md) added 6 and took it to 1,555, one new
binary, `table_backlog.rs`, and one test in `backpressure.rs`, and filed nothing. Under the
workspace run at six threads three fixture tests failed across two runs and each passed alone:
`backup_restore_verifies_history_in_new_cluster` (item 117), `down_retains_placement_during_grace`
(item 119) and `migration_resumes_after_each_phase_failure` (item 100).
[Resolved #30, 120, 121](resolved/resident-copy-collision.md) added 6 and took it to 1,549,
one new binary, `resident_reads.rs`, and filed nothing: the two defects it found beside item 30
were fixed with it. Under the workspace run at six threads two fixture tests failed and passed
alone (item 100).
[Resolved #115](resolved/retry-sidecar-crash-window.md) and [Resolved #116](resolved/map-version-test-snapshot.md)
added 2 and took it to 1,543, filing items 117, 118 and 119: one fixture test each that
failed under load in their runs and passed alone.
[F50](../features/cluster-operations.md) added 8 and took it to 1,492, filing nothing and
resolving nothing here: the two defects it met - the control leader dialling a moved member at
the address it was admitted with rather than the one it committed, and a winning clone's
shorter log stopping the leader's control thread on a debug assertion once the leader did
reach it - were found by the M3 fencing row and its own address row and designed out before
the page was written, and are recorded on the F page. Its gate table names items 100, 102,
106, 107, 109 and 110 as open and not gates. Under the workspace run at six threads three
fixture tests failed, two of them item 100's, and under the fixture suite on its own two did
(item 100).
[F49](../features/backup-and-recovery.md) added 9 and took it to 1,484, filing nothing and
resolving nothing: the three defects it met - a recovered survivor's state diverging from a
joiner's replay of the same log, two plans planned in one pass picking the same destination
for one set, and a move re-adding a member a recovery had rewritten out of a group - were
found by its own fixture test and designed out before the page was written, and are recorded
on the F page. Under the workspace run at six threads one of F46's fixture tests failed and
passed alone, and under the fixture suite on its own four did (item 100).
[F48](../features/rolling-compatibility.md) added 8 and took it to 1,475, filing nothing and
resolving nothing: the one defect it met - a control state persisted by an older build
dropping a field the newer one judged by - was found by the run against a real previous
binary and designed out before the page was written, and is recorded on the F page rather
than here. Under the workspace run at six threads one of F46's fixture tests failed and
passed alone, and under the fixture suite on its own one of F47's did the same (item 100).
[F47](../features/local-rehome.md) added 18 and took it to 1,467, resolving item 111 on the
way - found by its crash matrix, reproduced first - and filing nothing new: what it left undone
is on the todos page, and item 43 now says what the rehome does not cover.
[F46](../features/capacity-rebalancing.md) added 17 and took it to 1,449, filing item 110 on the
way; this line did not say so until F47, which is the staleness the paragraph below warns of.
[F45](../features/replica-migration.md) added 18 and took it to 1,432, resolving item 108 on
the way and filing 109.
[F44](../features/repair.md) added 16 and took it to 1,414, resolving item 99 before its own
work began and filing nothing new: what it left undone is on the todos page.
[F43](../features/node-recovery.md) added 16 and took it to 1,398, resolving items 104 and 105
before its own work began and filing 106 and 107 on the way.
[F42](../features/primary-failover.md) added 21 and took it to 1,382, resolving item 101 and
filing 102 and 103 on the way.
[F37](../features/node-identity-control-plane.md) took the total to 1,265 and did not update this
line; [F38](../features/inter-node-transport.md) added 24 more and did;
[F39](../features/membership.md) added 31 and took it to 1,320;
[F40](../features/replication.md) added 22 and took it to 1,342, filing items 99 and 100 on
the way - the second of which is why the fixture suite is run at `--test-threads 6`.
[F36](../features/cluster-harness.md) added 40 — 28 of them in the new `shoal-model` crate, 4 in
the new `cluster_fixture.rs` binary whose two ignored functions are the children it re-executes
the binary as, and the rest over the pool's readiness handle, the frozen ports, the cluster record
and the distributed chapter's acceptance tables — and closed items 17, 38, 58 and 88 while
filing 91.
~~1,045~~ ~~1,084~~ ~~1,164~~ ~~1,187~~ ~~1,198~~ — this figure had gone stale by two features while the sentences below
it kept naming what each added, which is what a running total is supposed to prevent, and it then
went stale by four more in exactly the same way. It is re-derived from a run
rather than incremented, and [Test Coverage](test-coverage.md) is the page that carries the
per-binary breakdown. It had gone stale by a **whole feature** again this time:
[F34](../features/benchmark-tracing.md) added 20 and moved the figure on the coverage page and not
on this one, so 1,164 was two changes behind rather than one.
[Resolved #89](resolved/fragmented-query-traces.md) and
[Resolved #90](resolved/divergent-layer-filters.md) added the other 3 — a new
`tracing_topology.rs` integration binary carrying 1, which fails against the tree before the fix
and reports the three separate traces one query produced, and 2 `shoal-core` unit tests.
[F35](../features/wire-trace-context.md) added 11, and one of them is the first test here whose
existence depends on a feature **another crate** turns on: `trace_propagation.rs` is gated on
`otel`, which `cargo test -p shoal` does not enable and a `--workspace` run does, through
`shoal-bench`. So this figure counts it and a per-package run would not
([Test Coverage](test-coverage.md) says which is which).
[Resolved #76](resolved/stage-join.md) added 6, and moved the count in both directions at once: 2
in a default run and 4 behind the feature, including the first test anywhere that starts a server
under `stage-profile` and reads what it wrote. That is the test the stage layer never had, and its
absence is why three of four reports in `f22-row-size` were empty.
[F22](../features/row-size-benchmarks.md) added 21, two of which reproduce items 73 and 74 and fails against
the tree before its fix. [F23](../features/self-sizing-staging-buffer.md) added 7, one of which
reproduces [O34](optimizations.md) and fails against the tree before its fix — the first entry from
the optimizations page ever reproduced by a test rather than argued from source and sized by a
capture. [F25](../features/read-buffers-are-filled-not-zeroed.md) and
[Resolved #80](resolved/never-flushed-partitions.md) added 1, which fails against the tree before
the fix and is the first test anywhere that counts what the engine asked storage rather than what
it answered a client.
[Resolved #79](resolved/micro-only-capture-current.md) added 8, two of which fail against the tree
before the fix. [F26](../features/archive-routed-requests.md) added 5, four of which were confirmed
by breaking the code under them rather than by being written after it.
**Unchanged by the `f22-row-size` capture**, which added no tests and moved no count, and which is
where item [75](#75-a-control-that-was-measured-at-every-width-is-not-drawn-and-the-caption-says-it-is)
and [item 76](resolved/stage-join.md) came from — both read out of the committed artifact rather
than out of the source, which is the reverse of how everything above them was found and the reason
neither was caught earlier.
**Unchanged by `f23-staging-buffer` too**, which is where
[item 77](#77-a-macro-only-capture-can-never-reach-the-pages-it-was-taken-for) came from — found by
rendering the pages with that capture committed and noticing that none of them drew it. A third way
of finding a defect, after reading the source and reading an artifact: running the tool that
consumes one.
**Unchanged by the `F20-conf` capture** before it, which added no tests and moved no count: a capture is
evidence rather than a test, and what it produced was a reproduction for
[item 71](#71-throughput_sensitive-is-configured-documented-and-mostly-unused), a second and worse
reproduction for [item 58](resolved/pool-readiness.md), since resolved, and
[item 72](#72-an-arm-with-no-writes-is-reported-as-an-arm-that-ran-once) filed from reading the
renderer against the page it had just produced.
That is up from 986 with [F20](../features/configuration-sweeps.md) and
[F21](../features/benchmark-groups.md), which added 29 between them, all in `shoal-bench`. Before
that it was up from 967 with [F19](../features/chart-legends.md), which added 16 `shoal-bench` unit
tests over the shared chart legend, the data-derived axis ticks and the encryption charts in
nanoseconds, 2 integration tests over the legend's geometry, and 1 doctest. That is up from 896
with [F17](../features/workload-grid.md) and [F18](../features/results-pages.md), which added 71.
That is up from 876 with [F16](../features/client-builder.md), which added 12 `shoal-client` unit
tests over the builder and the endpoint order, a new `pool.rs` integration binary carrying 6, and 2
doctests. That is up from 868 with [F15](../features/client-server-split.md), which also
re-attributed 177
of them: `shoal-core` went from 323 unit tests to 146 as the protocol and the client became crates
of their own, and none were lost. That is up from 490, 301 and 25 with
[F14](../features/encryption-in-transit.md) — one new integration binary (`tls.rs`) carrying 8
tests, 17 unit tests over the TLS configuration and the kernel key material, 5 over the config
section, and 4 new doctests. The 8 in `tls.rs` and one of the 17 need the `tls` kernel module and
skip loudly without it, the same way the `stage-profile` tests sit outside a default run. No new
defect was filed while building it; item 64 was filed just before it, while correcting
[D4](../direction/encryption.md). Before that it was 482, 301 and 24 with
[F13](../features/transport-workloads.md) — 8 unit tests in `shoal-bench` over the eight transport
workloads and one doctest over `seed_batch`, with the `shoal-core` count unmoved because no engine
code changed. No new defect was filed while writing it. Item 64 was filed later, while correcting
[D4](../direction/encryption.md), and changes no count — it is a documentation defect and no test
was added or could be. Before that it was 473, 272 and 21 with
[F12](../features/authentication.md) — one new integration binary (`auth.rs`) carrying 9 tests, 17
unit tests over the mechanism and its credential store, 8 over the auth frame codec and the
handshake's new fields, 4 over the config section, and 3 new doctests. Item 63 was filed while
writing it, from reading the source. Before that it was 470, 258 and 21 with
[F11](../features/error-channel.md) — one new integration binary (`errors.rs`) carrying 3 tests,
7 unit tests over the error codec, 3 over the response payload, and 4 over the client's read path
and its dead connection sweep. Four existing integration tests changed what they assert rather than
being added to, which is the signal that the behaviour they pinned is the behaviour that moved.
Item 62 was filed while writing it, from reproducing it, and its reproduction is not committed —
the test that found it asserted behaviour F11 does not provide.
Before that it was 456, 231 and 21 with
[F10](../features/framing-and-protocol-evolution.md) — three new integration binaries
(`framing.rs`, `handshake.rs`, `fingerprint.rs`) carrying 14 tests between them, 23 unit tests over
the frame codec and the fingerprint, 2 over the client's read path, and 2 over the frame bound's
config default.
Before that it was re-run and re-counted binary by binary in August 2026, unchanged — items 59 and
60 are filed from reading and neither added a test.
That is up from 454, 229 and 21 with
[Resolved #57](resolved/missing-archive.md) — one integration test per persistent table over a
read whose archive is not on disk, and two unit tests over `get_archive` and how the failure it
now reports is classified. Before that it was up from 452, 225 and 21 with
[Resolved #16, 51](resolved/partition-load-failure.md) — two integration tests over a partition
read that fails and four unit tests over how a read failure is classified. Before that it was up
from 410, 219 and 21 with
[F9](../features/ephemeral-tables.md), and before that from 359, 219 and 16 with
[F8](../features/purpose-built-workloads.md), whose count moved in **both** directions — it
deleted a comparison engine along with its tests and moved others between crates, which that page
accounts for line by line. Before that it was up from 172, 215 and 11 with the stamp and
offset tests added by [F6](../features/stage-breakdown.md). Before that it was up from 168, 194 and 11 with the config and cpu selection tests
added by [items 18 and 50](resolved/excluded-cores-typo.md) and the baseline versioning and
throughput tests added by [F3](../features/performance-harness.md). Before those it was up
from 14 and 32 with the addition of SHQL coverage
([SHQL](../api/shql.md#testing)), the restart and eviction tests added with items 4 and 5, the
limit and cross-shard coverage added with item 7, the row-order and `IN`/`OR` coverage added
with items 26 and 39, the sort-key selection coverage added with item 8, the range coverage
added with [F1](../features/sort-key-ranges.md), the multi-log recovery and recovery
counting coverage added with items 31 and 9, the tablet map and storage marker coverage
added with items 11 and 12, the eviction accounting coverage added with item 13, the empty
rotated log coverage added with item 14, the compaction tail loss and marker format coverage
added with items 44 and 45, and the query error display coverage added with
[item 48](resolved/query-error-display.md). The counts before item 48 were 157, 194 and 11;
before F2 were 136, 183 and 11; before items 44 and 45
were 136, 178 and 11; before
item 14 were 135, 178 and 11;
before item 13 were 135, 177 and 11; before items 11 and 12 were 133, 168 and 10; before
items 9 and 31 were 132, 159 and 10; before F1 they were 115, 129, and 8, and before item 8
were 105 and 116. [Resolved #126](resolved/storage-directory-unusable.md) added 2, both
`shoal-core` unit tests in `meta.rs`.

---

## High — data loss and silent failure

~~*Nothing is currently filed at this severity.*~~ ~~Item 124 is, filed at the end of this page
with the entries that came after the triage order.~~ *Nothing is currently filed at this
severity* again: item 124, the last one, is [resolved](resolved/unsorted-update-generation.md).
Items 9 and 31 were the two before it and are both [resolved](resolved-issues.md).

~~One thread they shared is still worth pulling.~~ Both were about what happens when a copy read
from disk meets a copy already in memory: item 31's `scan` overwrote the in-memory copy, and
item 30 was the `Occupied` arm of the sorted table's `load_partition` throwing the disk copy
away without a word. Item 31 answered the question for recovery by removing the collision
entirely — nothing loads after a replay. **Item 30 closed the thread**, together with the two
defects found by reading its code: the unsorted table's arm replaced the resident copy and
charged the shard for both (item 120), and the collision was only reachable because a get did
not wait on a replicated apply's read already in flight (item 121). All three were reproduced
and fixed in one change ([Resolved #30, 120, 121](resolved/resident-copy-collision.md)). Both
tables now keep the resident copy, charge nothing, and log at `ERROR` if the two copies
disagree, and a partition has at most one read outstanding.

---

## Medium — robustness

Items 129, 130 and 131 came out of one TMDB load against the lab; ~~130 and 131~~ are
[resolved](resolved/stream-connection-accounting.md), with the client half of item 60, from the
[cluster testing](../cluster-testing/findings.md) chapter's first runs. The four items that were
here before them are resolved, all in one change:
[27](resolved/shql-quote-escape.md), SHQL strings that could not hold a quote;
[32](resolved/client-gone-broadcast.md), an ordinary client's departure retiring nothing;
[36](resolved/staged-tail-deadline.md), a staged write that waited for a queue to drain; and
[125](resolved/retry-unknown-outcome.md), a retried write whose unknown first outcome was
reported as the last try's refusal. Item 125 was filed at the end of this page, with the
entries that came after the triage order, and carried its severity in its first line.

### 129. An overloaded group answers `OutcomeUnknown` rather than `Shedding`

A client that keeps more writes outstanding than a cluster commits within
`replication.write_timeout` gets `OutcomeUnknown` for the writes at the back of the queue. It
does not get `Shedding`, the refusal that says nothing was applied. The only admission bound
on a group's proposals is `replication.pending_bytes` (`GroupSlot::pending_bytes`, checked in
`ShardGroups::propose` in `server/shard/groups.rs`). At its default of 64 MiB per group, with
rows of a few kilobytes, that is tens of thousands of writes, far more than a group commits in
5s. The bound is also counted on the shard that proposes. A write that hops to its leader
(the `ReplicateKind::Propose` arm) goes straight into `propose_through` there, and is counted
against no budget on the leader.

So overload shows up as timeouts. A caller has to treat `OutcomeUnknown` as retriable to get
through it, and that is safe only for a write that replaces its row. A cheap refusal at
admission would be definite.

**Established by running it**: the [TMDB loader](../features/tmdb-dataset-deployment.md) at its
old defaults (8 workers × 4096 in flight, plus each movie's keyword rows) against the three-node
lab cluster. `OutcomeUnknown` came six seconds in, one `write_timeout` after the load started,
with every member up and no election. The loader now retries and ships with a smaller gate,
which works around this item without fixing it. Where the reason was lost on the way back is
[Resolved #128](resolved/hop-deadline-margin.md).

A fix would shed at admission once a group's queue would take longer than `write_timeout` to
commit. That means an estimate of commit rate per group, or a bound on count or bytes derived
from `write_timeout`, and a check on the leader for writes that arrive by hop. That is a
behaviour change and needs a benchmark (the grid's high-depth rungs are where it would show).

### 132. `ephemeral_sorted_table` aborted once in glibc's thread-cache teardown

One workspace run aborted this test binary with SIGABRT after its first test passed. glibc
printed `tcache_thread_shutdown(): unaligned tcache chunk detected`. That message means a heap
chunk on a thread's cache list was corrupted before the thread exited: something wrote past an
allocation or freed one twice. The other fourteen tests in the binary never reported.

**Established by running it, once.** The run was `cargo test --workspace --no-fail-fast --
--test-threads 6` for [F55](../features/cluster-upgrade.md), a change that touches no engine
code. Run alone straight after, the binary passed all fifteen tests. The cause is not known.
The suspects are the code that is `unsafe` about a heap it does not own: the zero-copy archived
responses, and the `Send` wrappers this page lists under **Unsafe `Send` invariant**. The next
step is running the binary in a loop under ASan (`RUSTFLAGS=-Zsanitizer=address` on nightly)
until it aborts, then reading the report's first frame.

**A candidate cause was found and fixed, and is not shown to be this.**
[Resolved #133](resolved/read-plan-rc-across-shards.md) was a shared `Rc` in every split query's
metadata, raced on by two shard threads: heap corruption from exactly the `unsafe` `Send` this
entry suspected. A standalone server splits gets across shards too. But this binary, run under
ASan (`-Zsanitizer=address -Zsanitizer-recover=address`) on the unfixed tree, passed all fifteen
tests and reported no use-after-free, only gxhash reading past the end of a 13-byte key, which it
does on purpose within a page. So this stays open until it recurs on a tree with #133 fixed, or
never does.

### 142. Two fixture tests fail intermittently on an idle host

`lost_response_retry_returns_original_result` stops at `group …'s checkpoint never reached 5:
[(0, Some(0))]`: node zero, killed and restarted, catches up (its digest matches) but reports
checkpoint 0 for the group for the whole 60 second wait, through repeated `ROTATE` and `COMPACT`
verbs. `migration_resumes_after_each_phase_failure` fails less often.

**Established by running it**, on the development host with nothing else running, at the commit
that fixed items 139 to 141 and applied O62:

| Test | Current tree | `b681dcc` (before 139–141, O62) |
| --- | --- | --- |
| `lost_response_retry_returns_original_result` | 4 of 17 runs failed | 0 of 6 |
| `migration_resumes_after_each_phase_failure` | 2 of 8 runs failed | 0 of 3 |

Restoring the old fixed map fold on the current tree gave 8 of 8 passes, but the next 8 runs with
O62 on passed too, and the lost-response test had also failed once in an earlier loaded run from
before these changes. With logs on (`SHOAL_CHILD_LOG`), 6 of 6 runs passed. The samples are too
small to say whether the changes made the failure more likely, and nothing found links them. What
decides a restarted member's checkpoint is that a sealed WAL segment is handed to the compactor
only once every group on the shard has applied past it. A group on node zero that has not applied
past a segment would hold every other group's checkpoint on that shard. Filed with its rates so
that the next change to that path, or the next run, can say more.

The suite run for [Resolved #144](resolved/post-heal-elections.md) at six threads failed
`lost_response_retry_returns_original_result` again, and also `scheduled_scrub_quarantines_without_an_operator`,
which had not failed before. Both passed alone afterwards: two and six runs out of two and six.
The run for [O67](optimizations.md#o67-ten-thousand-retained-entries-is-seconds-of-a-busy-group)
failed the lost-response test again, with `local_rehome_recovers_after_each_crash_point` and
`returning_node_catches_up_by_log_or_snapshot` (which sets its own retention, so the new default
does not reach it). All three passed alone straight after. What the loaded failures share is a
deadline the suite's load outruns, which is what this item is now tracking.
The run for [#146](resolved/apply-wait-on-a-lagging-copy.md) failed
`returning_node_catches_up_by_log_or_snapshot` a second time, and this one was the test's own
race, now fixed: *"the senders counted 6 transfers and node two installed 7"*. A receiver counts
an install when it lands, and its sender counts the transfer only when the end's answer reaches
it, so a read of both in between finds one more install than transfers. The test now waits up
to ten seconds for the senders' count.
The first suite run after [#155](resolved/restore-retries-unreachable.md) failed its new test,
`a_restore_rides_out_an_unreachable_member`, which passes alone (four of four) and passed in the
next full suite run. That run's log kept only the summary line, so the failure's message was not
captured. The test waits out a 25 s isolation with a 20 s phase deadline and a 5 s retry pause, and
the margins are what a loaded host erodes.
The `shoal-core` change for [#148](resolved/stale-intent-log-tail.md) was followed by one failure
in the first four runs of `shoal/tests/persistent_unsorted_table.rs` at six threads, whose name the
run did not keep, and none in the next nine.

### 152. `a_compaction_that_meets_an_unreadable_archive_is_tried_again` fails intermittently

`shoal/tests/persistent_unsorted_table.rs`: the rotated intent logs are not all compacted within
the test's 30 s once the archives are readable again (*"the rotated logs were never compacted once
the archives were readable again: N left"*). Found during the
[distributed cluster testing](../cluster-testing/overview.md) chapter: one failure in the first
four runs of the suite at six threads before [#148](resolved/stale-intent-log-tail.md)'s change,
one in the first of six after [#150](resolved/inline-partition-buckets.md)'s, and none in the
ten other runs or in three runs of the test alone. The test takes the permissions off a table's
archives, so it is the family item 113's `statfs` panic came from. That panic is fixed, so this
is another timing. Filed with its rate so the next change to the retry path can say more.

### 155. A restore whose group failed cannot be finished

*Partly resolved:* a group whose driver could not reach a member is now driven again
([Resolved #155](resolved/restore-retries-unreachable.md)). What remains is every other failure.
`Restore` is refused on *"a cluster that already restored"*, and on a populated table. So a restore
in which one group failed for another reason leaves a cluster with that group's tablets empty, and
the only way to finish is to delete the cluster, bootstrap another, and restore everything again. On the lab that was 3 minutes of restore, and
it would be hours for a large backup. A retry of the failed groups alone, under the same operation
and against the same files, would finish it: every other group is verified and is not touched
again. Found by the [distributed cluster testing](../cluster-testing/correctness.md#back-up-destroy-and-restore)
chapter.

### 156. A full disk stops every group on a node until it is restarted

*Partly resolved:* a node whose WAL cannot be written now stops, and a restart recovers it
([Resolved #156](resolved/wal-failure-stops-the-node.md)). What remains is refusing appends before
the disk is full.

When a node's WAL write fails for want of space, openraft stops the core of every group the write
was for with a fatal storage error, and the shard's probe marks each copy dead "until the process
restarts" (`probe_cores`). Nothing refuses writes while the disk nears full: `disk_reserve` guards
snapshot installs, not appends. And a write coordinated through the node goes to its own dead core,
failing `Unavailable` for as long as the node runs, rather than hopping to the group's new leader.
Found on the lab ([cluster testing](../cluster-testing/correctness.md#fill-a-nodes-disk)), where
a loader connected to every member stopped at those writes. Nothing was corrupted: freed and
restarted, the node caught up exactly. The fixes it needs are three and separate: shed appends
below a reserve with a retriable refusal, hop a write whose local core is dead to the group's
leader, and restart a dead core once space returns.

---

## Low — hygiene and documentation drift

### 19. `memory` has no serde default

`shoal-core/src/server/conf.rs:30-32` — the only field in `Resources` without a default. A
`resources:` block omitting `memory` fails to deserialize. Omitting the whole block yields
`memory: 0`, so eviction runs continuously.

### 21. Constant and comment mismatches

| Constant | Comment says | Value is |
| --- | --- | --- |
| `MIN_ARCHIVE_COMPACTABLE` (`.../fs/compactor.rs:31-33`) | 100 MiB | `10 << 20` = 10 MiB |
| Its two use sites (`.../fs/compactor.rs:472`, `:474`) | "under 100MiB" | the same 10 MiB |

(`default_intent_log_size` had the same mismatch and has been corrected to say 10 MiB.)

Also `RemoteTracing::Grpc` exports over HTTP (`trace.rs:33-37`, `.with_http()`), and
`FileSystemThroughputWriterConf::write_behind` — a count of buffers, defaulting to 4 — is
deserialized with `deserialize_byte_size` (`.../fs/conf.rs:154-156`). Its latency-sensitive twin
one struct up is *not* (`:68-69`), so the two writers disagree about what the same field name
means.

### 22. Size accounting inconsistencies

- `UnsortedPartition::new` sets `size = row.deep_size_of() + 17`
  (`.../tables/partitions.rs:228`); `UnsortedPartition::update` sets
  `size = self.deep_size_of()` (`:313`). Different bases for the same field.
- `SortedPartition` tombstones subtract the row's bytes but the tombstone still occupies a
  `BTreeMap` slot, so delete-heavy partitions under-report.
- Sorted partition sizes are maintained by delta and never recomputed, so they drift.
- Recovery adds a partition to the counter in *archive bytes* and eviction takes it off in
  *deep size*. `FileSystem::load_scanned` (`.../storage/fs.rs:242`) does
  `*memory_usage.borrow_mut() += archive.len()`, which matches `MaybeLoaded::size()` while the
  entry is `Accessible`. Replay then converts it to `MaybeLoaded::Loaded`
  (`.../persistent/sorted.rs:336`, `.../persistent/unsorted.rs`) adding only the update's `diff`,
  so `size()` starts answering
  `partition.size()` — a `deep_size_of` — against an amount that was the archive extent's length.
  `evict` subtracts the new base. The residual per partition is `read.len() - partition.size()`,
  either sign. This is the most concrete candidate for the `drift` that
  [item 13](resolved/eviction-log-underflow.md) now reports, because it is the one place two
  different size *bases* meet on the same counter rather than two different arithmetic paths.
  Note [item 31](resolved/multi-log-recovery.md) shrank this considerably without meaning to —
  the old `scan` re-added `read.len()` once per update intent, so a partition with *N* updates
  was counted *N* times.

The fix for the first bullet is the one filed as
[O4](optimizations.md#o4-deep_size_of-is-a-recursive-walk-called-on-every-mutation), ranked **B2**
there. The mismatched bases exist *because* the size is re-derived at each call site instead of
being owned by one, so carrying a row's measured size alongside it settles this item and removes a
recursive walk from every mutation with the same edit. That is why O4 outranks the other write-path
entries despite the profile saying the write path is waiting on the device — it is bought for a
correctness fix, and the cost removal is change left over.

### 23. Client stream and pool rough edges

- `ShoalResultStream::skip(0)` panics: `skip -= 1` precedes the zero check
  (`shoal-core/src/client.rs:1001-1008`).
- `is_valid` / `has_broken` use `peer_addr()`, which does not probe the peer (still marked TODO).
  [F11](../features/error-channel.md) narrowed this rather than closing it: both now also refuse a
  connection whose *read half* has stopped, which is the case they were silently passing. A peer
  that is gone but whose socket has not been reset still looks healthy, and that needs a `Ping`.
  **[F16](../features/client-builder.md) narrowed it again from the other side** — a client now
  has other endpoints to fall back to when one stops answering — without closing it either, since
  detecting that it stopped is still what is missing.
- `Shoal::send` archives the bundle before `track_response` may regenerate its id
  (`shoal-core/src/client.rs:209-215`).
- Two large commented-out blocks remain (`shoal-core/src/client.rs:549-603`, `:1048-1114`).
- `suceeded` / `QuerySuceededOpts` are misspelled in the public API.

**Fix direction:** the health-check half is [D6](../direction/connection-pool.md#health-checks-that-work),
which needs [D2](../direction/framing.md#message-types)'s `Ping` — a discriminant the wire format
has and nothing is behind. The rest are local edits and need nothing.

### 24. shoalctl warnings

`TabState::next` / `prev` are dead code (`shoalctl/src/components/tab.rs:548`, `:558` — the
compiler says so on every build), and `submit_query` `tokio::task::spawn`s the query only to
`.await` the join handle immediately (`:275-279`), so the UI blocks for the round trip anyway.

That `.await` ends `.unwrap()`, which is worth naming separately: a panic inside `query_bar::run`
comes back as a `JoinError` and is unwrapped on the UI task, which is the exact shape
[item 24's fixed half](resolved/shoalctl-panic.md) was about — a panic that unwinds past
`ratatui::restore()` and leaves the terminal in raw mode. The fixed half moved the *parse* error
off that path; a panic from the query round trip still takes it.

### 28. SHQL "Unknown field" names the field it is listing as valid

A field declared on a table but marked neither `partition`, `sort`, nor `filter` — an
`#[shoal(update)]`-only field, for instance — has no `FieldRole`, so using it in a `WHERE`
clause fails. The message it fails with is confusing:

```
Unknown field 'data'. Valid fields are: ["id", "title", "data"]
```

`data` appears on both sides. The cause is that `field_names()` is generated from *all* fields
while the `get_field_role` match arms are generated only for fields that have a role
(`shoal-derive/src/traits/table_schema.rs`), so the two lists disagree by construction.

The error is now raised in **one** place rather than two: the sorted and unsorted parse arms share
a `check_conditions` quote (`shoal-derive/src/structs/client.rs:238-247`). This entry used to name
`:127-137` and `:198-208` as the two sites, and that is worth recording because it makes the fix
smaller than the entry implies — a single generated block to change instead of a matched pair to
keep in step.

**Fix direction:** two different errors are being conflated. Distinguish them — if
`field_names()` contains the name, say `Field 'data' cannot be used in a WHERE clause because it
is not a partition key, sort key, or filter`; only say `Unknown field` when it is genuinely
absent. Better still, generate a `queryable_field_names()` alongside `field_names()` so the
"valid fields" list is the set that can actually appear in a `WHERE` clause.

### 29. SHQL rejects exponent notation with a misleading error

`float_number` is `(opt(sign), digit1, ".", digit1)`
(`shoal-core/src/shared/queries/parser.rs:524`), so digits are required on both sides of the
point and there is no exponent form. `.5` and `5.` fail with a reasonable
`Expected a value for field 'x'`, but `1e9` does something worse:

```
SELECT * FROM Movie WHERE id = 1e9
  =>  Unexpected trailing input: 'e9'
```

The integer parser matches the leading `1`, the condition completes, and the leftover `e9` is
reported by the end-of-input check as though the problem were somewhere else entirely. Confirmed
by running it.

**Fix direction:** low priority, since spelling the number out always works. If it is fixed, add
an optional exponent to `float_number` rather than special-casing the error — the error is only
misleading because the grammar accepts a prefix of what the user meant. Note that
`serde_json::Number::from_f64` already rejects the infinities a large exponent can produce, so
the overflow path is covered.

### 49. The coarsest parse errors report a span covering the whole query

Three of the errors a user hits most often blame the entire query rather than the part of it that
is wrong:

| Query | Reported span |
| --- | --- |
| `SELECT * FROM Nope WHERE id = 1` | `0..31` — all of it, for a table name in bytes 14 to 18 |
| `SELECT * FROM Movie WHERE title = 'a'` | `0..37` — all of it, for a missing partition key |
| `PICK * FROM Movie WHERE id = 1` | `0..30` — all of it, for a keyword in bytes 0 to 4 |

The first comes from the unknown-table arm the derive generates
(`shoal-derive/src/structs/client.rs:281`), which passes `0, query.len()`; the second from the
missing-partition-key arm beside it (`:507`), which does the same; the third from
`ParsedSelect::new` (`shoal-core/src/shared/queries/parser.rs:1312-1318`), which reports
`at_position(..., 0, query)` because the winnow parser it wraps returns a `ContextError`, and a
`ContextError` carries no input slice to recover an offset from.

Everywhere else the parser tracks real offsets — `field_start`/`field_end` on a `WhereClause`,
`start`/`end` on a `WhereValue` and a `ParsedProjection` — and computes them from
`original.len() - input.len()` on the surrounding `&str`. These three sites can do the same; the
`ContextError` is not the obstacle it looks like, because the offset comes from the input the
parser was handed rather than from the error it returned.

The cost is paid in shoalctl, which underlines the span an error names
([item 48](resolved/query-error-display.md)). A span covering the whole query is refused as
misleading, so exactly the errors a beginner hits first are the ones drawn without a mark. The
message names the position instead, which is correct and less useful.

**Fix direction:** for the unknown table, `ParsedSelect` already has the table name and could
carry its offsets alongside it the way `ParsedProjection` does. For the missing partition key,
the honest span is the whole `WHERE` clause rather than the whole query, since that is the clause
that has to change. For the failed `SELECT`, `query.len() - parsable.len()` at the point winnow
gave up is the offset, and it is already in scope — literally on the line above, as
`select_start` (`parser.rs:1310`), computed for the projection's sake and then not used for this.

### 40. `UnsortedExists` still names a single partition

`UnsortedGet` now carries `partition_keys: Vec<u64>` (`shared/queries/unsorted.rs:131`) and splits
across shards like its sorted counterpart ([26, 39](resolved/partition-order.md)), but
`UnsortedExists` (`:194-196`) was left with a scalar `partition_key`, and `split_by_shard` routes
an exists with a single `find_shard` (`:56`) where a get gets `group_by_shard` (`:50`).
`SortedExists` has taken a `Vec` all along, so the two table kinds now disagree about what an
exists can ask.

Nothing is wrong today: SHQL emits no exists queries, so the only way to reach one is the typed
API, where the single-partition shape is what the generated `*Exists` offers anyway. It is a
consistency gap that will bite whoever adds `EXISTS` to the query language, since the sorted
spelling will accept an `IN` list and the unsorted one will not.

**Fix direction:** the same change `UnsortedGet` took — a `Vec<u64>`, `for_partitions`, and
`group_by_shard` in `split_by_shard` — plus a `PendingGet`-shaped wait in the table so an exists
spanning partitions can park on more than one read.

### 41. SHQL cannot express a composite partition key

A table with several `#[shoal(partition)]` fields gets a tuple `PartitionKey`
(`shoal-derive/src/traits/partition_key.rs`), and the generated parse arm deserializes one
literal straight into that type. No SHQL literal is a tuple, so every query against such a table
fails with `Failed to deserialize partition key` however it is written.

The parser now guarantees one clause per field, which is what makes the fix tractable: the arm
could collect the conditions naming each partition field and build the tuple from them in
declaration order, so `WHERE a = 1 AND b = 2` would name one partition. Note that this is the one
place where `AND` across two fields is a conjunction *within* a key rather than a filter on top
of one, and that `IN` over a composite key would need each field's values crossed with the
others.

### 42. SHQL cannot express a composite sort key

The same defect as [41](#41-shql-cannot-express-a-composite-partition-key), one key over. Several
`#[shoal(sort)]` fields make a tuple `Sort` (`shoal-derive/src/structs/get.rs`), and the sorted
parse arm deserializes one literal straight into it, so `WHERE partition = 'x' AND a = 1 AND b = 2`
fails with `Failed to deserialize sort key` rather than naming a row.

Unlike the partition-key case the query still runs when the sort condition is simply left out — it
just returns the whole partition — so this is a missing capability rather than a table that cannot
be reached at all. It only started mattering with [item 8](resolved/sort-keys.md): while sort keys
were ignored there was nothing to express.

**Fix direction:** the same shape as 41, and worth doing in the same change. Collect the conditions
naming each sort field, build the tuple in declaration order, and reject a partial one. A prefix of
a composite sort key is a *range*, not a point, and although ranges exist now
([F1](../features/sort-key-ranges.md)) a range over a *prefix* still does not — lowering one needs
synthesized minimum and maximum values for the fields the prefix leaves out, which `Sort` does not
name ([TODOs](todos.md#sort-key-range-predicates--built)).

### 35. A `RefCell` borrow is held across three awaits in the compactor

```rust
if let Some(entry) = self.map.to_archive.borrow().get(partition) {
    let handle = self.map.get_archive(&entry.archive).await?;
    let read = handle.read_at(entry.offset, entry.size).await?;
    let archived = <T as RkyvSupport>::access(&read)?;
    handle.close().await?;
```

`.../fs/compactor.rs:232-241`

`shoal-core` is edition 2021, where the temporary `Ref` produced by `.borrow()` lives to the end
of the `if let` statement. So the shared borrow on `to_archive` is held across all three awaits,
during which other tasks on the same executor run.

It does not panic today, because the only `borrow_mut` callers — `set_partition` and
`remove_partition` (`.../fs/map.rs:442-459`) — are the compactor itself, which is not running
while it is parked here. It is listed because that is a property of who happens to call what, not
of anything enforced, and because the fix is already sitting next to it:
`ArchiveMap::find_partition` (`.../fs/map.rs:514-520`) exists to copy the entry out, `ArchiveEntry`
is `Copy`, and the loader path already uses it.

Worth reading against the edition too: this becomes correct for free under edition 2024's
temporary scoping, which means an edition bump would silently change the failure mode rather than
the code. **The workspace is already mixed** — `shoal-bench` and `shoalctl` are edition 2024 while
`shoal`, `shoal-core` and `shoal-derive` are 2021 — so the bump is a per-crate decision that has
already been made three times without this being considered.

### 46. An unmarked storage directory is claimed rather than refused

`StorageMeta::claim` (`server/meta.rs:79`) treats a missing `shoal-meta.json` as a directory
nothing has written to, creates one, and starts (`:105-111`):

```rust
// this directory has never been written to, so claim it for this shard count
Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
    std::fs::create_dir_all(root)?;
    std::fs::write(&path, serde_json::to_vec_pretty(&StorageMeta::new(shards))?)?;
```

"Has no marker" and "has never been written to" were the same statement for exactly as long as
the marker has existed, which is one commit. Every directory written before
[items 11 and 12](resolved/tablet-ring.md) has no marker and plenty of data, and that change also
replaced the vnode ring with a tablet map — so ownership moved from a hash of the shard's *name*
to `top-12-bits-of-key % shard_count`, and effectively every partition now belongs to a different
shard than the one whose archive map holds it. A shard's data is stored under its own name, so
each shard reads its own archives, finds none of the partitions it is now asked for, and the
server comes up empty.

This is the exact failure `StorageMeta` was built to prevent. It is missed because the marker is
newer than the data it guards, and a guard that only fires when it recognises the directory
cannot fire on the one case where it does not.

The severity is bounded by who has such a directory: this is a pre-1.0 branch and the only known
instances are disposable dev data, which is why this is filed rather than fixed. It is recorded
because the reasoning generalises — the next on-disk marker will have the same blind spot on the
day it ships.

**Fix direction:** claiming is only safe for a directory that is genuinely empty. Before writing a
marker, check the root for archives and `*-active` intent logs; if any exist, refuse with a
distinct error saying the directory predates the marker and no migration exists. An empty
directory is still claimed, which keeps first start working. Note this cannot be a `format`
check — [item 45](resolved/storage-marker-format.md) covers a marker that is *wrong*, and this is
one that is *absent*.

### 47. A torn tail on the active log is counted as data loss

`FileSystem::read_intent_log` (`.../storage/fs.rs:202-206`) counts a `truncated` reader
against `RecoveryStats::truncated_logs`, and it does so for the active log on the same terms as
for an inactive one:

```rust
// a reader that stopped on a bad tail dropped everything after it
if reader.truncated {
    stats.truncated_logs += 1;
}
```

`RecoveryStats::is_clean` treats any non-zero `truncated_logs` as loss
([Recovery](../storage/recovery.md#what-recovery-discards)), so `Shard::report_recovery` emits
`WARN Recovery discarded data` — after an ordinary crash, where nothing was lost.

A torn tail on the active log is what a crash *looks like*. Writes are acknowledged only after
they are durable — that is [items 1-3](resolved/durability.md), and `ack_survives_sigkill`
(`shoal/tests/persistent_sorted_table.rs`) is the end-to-end proof — so the half-written entry at
the end of the log belongs to a write no client was ever told about. Dropping it is the design
working.

This matters by the recovery page's own argument. `updates_after_delete` is kept out of
`is_clean` because "an update that lands on a row a delete already tombstoned is correctly
dropped, and counting it as loss would make the numbers that do mean loss useless"
([storage.rs](../storage/recovery.md#what-recovery-discards)). Counting benign torn tails is the
same mistake in the opposite direction: every unclean shutdown produces a `WARN` that claims
data was discarded, so the warning that means real corruption is buried in warnings that mean a
process was killed. [Item 44](resolved/compaction-tail-loss.md) draws this same distinction
correctly on the compaction side.

**Fix direction:** the reader already knows where it stopped. A tail whose remainder is padding
or zeros is the benign shape; damage with non-zero bytes after it is a corrupt record with data
behind it. `IntentLogReader` can scan the remainder once on the way out and set two different
flags. Failing that, the cheaper split is positional — count damage in the *active* log
separately from damage in an inactive one, since an inactive log has been fully written and
rotated, so damage in it is never benign. Either way `is_clean` should ignore the benign counter,
the way it already ignores `updates_after_delete`.

---

## Unsafe `Send` invariant

Not a bug, but the most dangerous thing in the codebase to change without knowing. **It was a
bug once:** [Resolved #133](resolved/read-plan-rc-across-shards.md) was an `Rc` inside
`QueryMetadata`, which crosses shards on every split get and which this section did not name.
Anything a query carries between shards must be `Send` on its own, and
`what_a_query_carries_between_shards_is_send` checks `ReadPlan` and `QueryMetadata`.

```rust
/// # Safety
///
/// The Partition variant should not be sent across threads ever.
unsafe impl<D: ShoalDatabase> Send for ServerMsg<D> where ... {}
```

`shoal-core/src/server/messages.rs:248-257`, and similarly for `Comms`
(`shoal-core/src/server/comms.rs:16-23`).

`ServerMsg::Partition` carries a glommio `ReadResult`, which is not `Send`. The whole enum
asserts `Send` anyway so it can travel kanal channels. The invariant — a `Partition` message
may only ever be sent on its own shard's channel — is upheld solely by `FsLoader` being
constructed with a clone of its own shard's sender and no other
(`.../fs/loader.rs:256-271`).

Give a loader, compactor, or any task a sender belonging to a different shard, and `Partition`
messages become cross-thread: undefined behaviour, no compiler error, and probably no test
failure. Any change touching loader construction should be read against this.

**There is now a second place that has to hold, and it is not the constructor.** `FsLoader` keeps
a `senders: Vec<AsyncSender<ServerMsg<D>>>` reuse pool: `spawn_task` pops a sender for the read
task to report on and falls back to cloning `self.shard_local_tx` (`.../fs/loader.rs:292-295`),
and a finished task hands its sender back into the pool (`:370-372`, `:386`). So the invariant is
no longer "the loader holds one sender it cloned at construction" but "**every** sender that ever
enters that pool is a clone of this shard's". It holds today because the only two things that put
one there are the fallback clone and a task that was handed one from the same pool. A future
change that seeds the pool from anywhere else — a shared pool across loaders, most obviously —
breaks the invariant without touching the constructor this section used to point at.

The related `messages.rs:223` panic is the same invariant seen from the other side: cloning a
`Gathered` message is refused because a broadcast would be the way one reached a shard it was not
addressed to.

---

## Suggested triage order

1. **Item 38** — not a production defect, but the test suite is what every other fix on this page
   is judged by, and right now **five** of its binaries can silently serve each other's traffic.
   Worth doing before the fixes below rather than after them. It said "two" until the
   [August 2026 review](review-2026-08.md) re-measured, which is the argument for doing it rather
   than tracking it: every new test binary that starts a server joins the collision by default.
2. ~~**Items 124 and 122**~~ — ~~item 16, the hot-path panics~~ [resolved](resolved/hot-path-panics.md),
   and these are what fixing it found. Both are resolved too:
   [124](resolved/unsorted-update-generation.md) and [122](resolved/intent-log-failure.md). 124 was a stale read after an eviction nobody would think
   to look for; 122 was the storage failure item 16 was always said to be about, which ended the
   shard rather than panicking it. This entry used to read "items 11 and 16"; item 11, the
   empty-ring window a client could hit during startup, is [resolved](resolved/tablet-ring.md) and
   was made unbuildable rather than checked.
3. **Item 22** — the size accounting the whole memory limit rests on, now with *four* different
   bases for the same field. It moved up this list because the two things that used to sit in
   front of it are done: [item 6](resolved/memory-accounting.md) fixed the path that destroyed the
   counter outright, and [item 13](resolved/eviction-log-underflow.md) turned the eviction log into
   something that reports drift instead of breaking on it. That `drift` field is the instrument for
   this one, and it now has a first hypothesis to test rather than a whole page to reason about:
   recovery adds a partition in archive bytes and eviction takes it off in deep size, which
   predicts drift proportional to how many partitions a shard's recovery loaded and none on a
   shard that started clean.
4. **Item 47** — cheap, and it is making the recovery counters harder to trust the longer it sits.
   Every unclean shutdown currently reports discarded data, so the warning that means real
   corruption is buried under warnings that mean a process was killed.
5. ~~**Items 27 and 42**~~ **Item 42** — data that SHQL cannot reach at all: ~~a partition key
   containing a quote, and~~ a composite sort key. Item 27, the quote, is
   [resolved](resolved/shql-quote-escape.md): a literal doubles it, as SQL does. Item 42 was
   already the sharper of the two now that [item 8](resolved/sort-keys.md) is fixed, since a
   sort key is a thing you can query with.
6. ~~**Items 32 and 33**~~ — two leaks with one shape: state keyed by something that goes away and is
   never told. Both are resolved: [33](resolved/gather-expiry.md) at M5, and
   [32](resolved/client-gone-broadcast.md) by sending the `ClientGone` broadcast the peer lanes
   already sent ([Resolved #94](resolved/disconnected-client-cleanup.md)) for ordinary clients
   too. The cost it was waiting on is one message per shard per disconnect, the same as
   `NewClient` per connect; the capture of a connection-churn workload is still owed.
7. **Items 43 and 46** — the two remaining holes in the storage marker. Worth doing together,
   since both are changes to what `StorageMeta::claim` looks at before it writes.

### 52. A resident hit in `exists` answers a query a blocked clone will answer again

`PersistentSortedTable::exists` (`.../persistent/sorted.rs:684-760`) walks the partition keys an
exists named. A key whose partition has to be read from disk pushes a clone of the whole query
into `self.blocked` — through `block_on_load`, which is where the clone is parked — and moves on
(`:711-725`):

```rust
if check_disk {
    // build a query for just this blocked partition
    let blocked_exists = SortedQuery::Exists(exists_query.to_blocked(*partition_key));
    // park this exists if this partition has to be read from disk first
    if self.block_on_load(*partition_key, &meta, blocked_exists).await {
        // remember that we are still waiting on this partition
        blocked.push(*partition_key);
        continue;
    }
}
```

A *later* key in the same loop that is resident and does hold a matching row returns straight
away (`:733-741`):

```rust
if partition.exists(exists_query, &mut seek) {
    return Some((meta.client, meta.id, meta.stamps, response));
}
```

That early return never reaches the `self.pending_exists.insert` at the bottom of the loop
(`:749`), and it does not remove the clone from `self.blocked`. When the partition finishes
loading the clone is replayed and answers the same `(id, index)` a second time. The client is owed
exactly one response per query, and an unordered stream will surface both.

The refactor that introduced `block_on_load` moved the parking out of this function and did not
change the shape of the defect: the clone is parked by the callee and the early return is still
the caller's.

Only reachable when one exists names partitions on the same shard where at least one is
evicted and a *later* one is resident and matching — the order matters, since a resident hit
before the blocked key would have returned before the clone was ever parked.
`PersistentUnsortedTable::exists` has no such path: it names one partition and has one exit.

**Established by reading the source**, while building [F6](../features/stage-breakdown.md).
The stage report counts these as `join.duplicates` rather than folding them into a bucket, so
a run that hits this says so — but nothing yet reproduces it.


### 53. `hotpath`'s `percent_total` is meaningless for a concurrent scope

`docs/perf/runs/*.hotpath.json`, every capture

`hotpath` reports a `percent_total` per scope, and it is not a percentage of anything a reader
would take it for. It is not normalised across scopes that ran at the same time on different
shards, so twelve shards each spending most of a run inside a scope sum to far more than the run
did. The committed `B1-performance.hotpath.json` reports:

```
shoal_core::server::tables::storage::fs::stream::write_helper   percent_total: 1253041
shoal_core::server::shard::handle_query                         percent_total: 6245
```

Those are 12,530% and 62%, of a run that was 100% of itself.

The field is parsed and kept (`shoal-bench/src/model/hotpath.rs:35`) and never plotted or
tabulated, which is the right treatment. The trap is that it looks exactly like the
number anybody would reach for first. `total` — nanoseconds summed across every shard that entered
the scope — is the field to rank by, and it needs saying that it is a sum across shards rather
than a share of the wall clock, which is why the chart's axis on
[Benchmark Results](../performance/overview.md) says so.

**Established by reading the committed artifacts**, while building
[F7](../features/bench-runner.md). `shoal-bench` never plots or tabulates the field, and
`render::chart::hotpath_scopes::tests::the_unnormalised_percentage_is_never_drawn`
(`shoal-bench/src/render/chart/hotpath_scopes.rs:149-186`) pins that by feeding a scope a
`percent_total` of 12,530 and asserting the number never reaches the chart.
Fixing it properly is upstream in `hotpath`, or means dividing by the shard count that actually
touched each scope — which the profile does not record.

### 54. A schema still declares `rkyv` and `deepsize2` itself

`shoal-proto/src/shared/traits/sorted.rs:10`, `unsorted.rs:10` and `traits.rs`, the `DeepSizeOf`
supertrait bound; and every schema's own `#[derive(Archive, Serialize, Deserialize)]`

**Mostly fixed — see [Resolved #54](resolved/macro-emits-three-crates.md) for the part that is
done and why.** The macros no longer name any crate by path: they emit `::shoal::` and the facade
re-exports what they reach for, and [F15](../features/client-server-split.md) took glommio out of
a client's dependency graph rather than re-exporting it. `glommio`, `uuid` and `kanal` are gone
from the manifests that carried them under a comment saying they should not have been there.

What remains is not a macro path and could not be fixed by re-pointing one. A schema writes

```rust
use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, DeepSizeOf)]
```

by hand, and both derives expand to absolute paths into their own crates, so both have to be
declared. `deepsize2` is there because `ShoalTableSupport` is bound on `DeepSizeOf` — the server
sizes a row to charge it against the memory budget — and that bound is visible to a client that
will never evict anything.

The original filing said the generated code "names `glommio`, `uuid` and `deepsize2` by path".
That was wrong about `deepsize2`, which no macro has ever emitted; the third name should have been
`kanal`. Corrected on the resolved page rather than here.

**Established by reproducing it**, while building [F8](../features/purpose-built-workloads.md).

**Fix direction:** two options, both filed in [TODOs](todos.md). Emit the `DeepSizeOf` impl from
`#[derive(ShoalSortedTable)]` by summing the row's fields, which removes the derive from the
schema author's hands but hand-rolls an accounting impl the eviction budget depends on being
right; or take the bound off the client-visible traits and put it where the server needs it.
`rkyv` is harder and may not be worth closing: a schema's types are rkyv types, and saying so is
arguably honest rather than leaky. `shoal::rkyv` and `shoal::deepsize2` exist as escape hatches
meanwhile.

### 55. A get that found nothing is reported as a query that failed

`shoal-core/src/client.rs:313`, `Shoal::send_one`

`send_one` calls `suceeded` (`:816`) on the response and turns a get that matched no rows into
`Err(QueryDidNotSucceed)`. "The row is not there" and "the query did not work" are different
answers, and a caller that wants the first has no way to ask for it through `send_one` — it has to
drop to `send` and drain the stream itself, or use `exists`, which only answers a yes-or-no.

This is a usability defect rather than a correctness one, and it bites in a specific way: any code
that probes with a get treats an empty table as a broken server. The F8 readiness probe did exactly
that and timed out for thirty seconds against a server that was answering every query correctly,
until it was changed to use `exists`.

**Established by reproducing it**, while building F8. `QuerySuceededOpts` already exists as the
knob that decides what counts as success, so the fix is plausibly to let `send_one` take one rather
than always using the default.

**Unblocked, not closed, by [F11](../features/error-channel.md).** "Found nothing" and "failed" are
now different answers on the wire, and `suceeded` reports the second as `Errors::Server` whatever
the options say. What is left is the first half: letting `send_one` say that an empty get is
acceptable. It no longer has to, to tell the two apart — a caller can ask `response.error()`
directly — but it is still the ergonomic gap this item was filed for.

### 59. A shard that cannot free anything keeps trying, on every message, in silence

```rust
// check if we need to evict any data
if *self.memory_usage.borrow() > self.conf.resources.memory {
    // try to evict our least recently used data
    self.evict_data().await?;
}
```

`shard.rs:984-988`, and `Shard::evict_data` at `:847-882`

The trigger is a level, not an edge: it is re-tested at the bottom of **every** iteration of the
shard loop. `evict_data` answers it by draining the LRU until it has found 40% of current usage
worth of victims, or until the LRU runs out:

```rust
// we have no more rows we could evict even if we wanted too
None => break,
```

`shard.rs:872-873`

When the LRU is empty that arm is taken immediately, the function allocates a
`HashMap::with_capacity(10)` (`:852`), evicts nothing, and returns `Ok(())`. The condition above
it is still true, so it runs again on the next message, and on every message after that, for as
long as the process is over its limit. Nothing is logged: the `None` arm is the only exit and it
says nothing, and the eviction event ([Resolved #13](resolved/eviction-log-underflow.md)) is
emitted per *pass that dropped something*.

So a shard that is over its memory limit and has nothing it is allowed to drop looks, from
outside, exactly like a shard that is under it.

**This is reachable rather than theoretical, and [F9](../features/ephemeral-tables.md) is what
makes it so.** An ephemeral partition never enters the LRU, because `NoStorage` never sends a
`MarkEvictable` — which is the invariant ephemeral data being safe rests on, and is correct. But
ephemeral rows *are* counted in `memory_usage`. A database whose ephemeral tables alone exceed
`resources.memory` therefore sits permanently in this state. The ephemeral page says memory is not
bounded and that bounding it is the caller's problem
([Limitations](../features/ephemeral-tables.md#limitations)); what it does not say is that
exceeding it is silent and costs a pass per message.

The cost of the pass itself is small — a `RefCell` borrow, a comparison, one allocation and an
empty pop. It is filed here rather than in [Optimizations](optimizations.md) because the defect is
the silence, not the work.

**Established by reading the source**, during the [August 2026 review](review-2026-08.md).
Nothing reproduces it, and the cheapest reproduction is an ephemeral table and a small
`resources.memory` — which `ephemeral_sorted_table.rs`'s "memory pressure evicts none of it" test
already sets up, and asserts the data survives rather than what the shard did to keep it.

**Fix direction:** distinguish "nothing needed freeing" from "nothing could be freed". The `None`
arm knows which it is — `need` is still non-zero — so it can emit one `WARN` naming the shortfall.
Rate-limiting it is the whole difficulty: at one per message it is worse than silence. The honest
shape is probably a latch, warning on entry to the state and again on leaving it. Note this should
*not* be fixed by making the trigger an edge — a level trigger is what lets a shard recover when a
partition becomes evictable later.

### 60. A result stream that is not drained to the end leaks its slot in the client

**The client half is [resolved](resolved/stream-connection-accounting.md)**: both stream types
remove their slot on `Drop`, and a frame for a slot whose reader is gone is dropped and the slot
removed rather than ending the connection's read loop (item 130). What is left open is the server
half below - nothing tells the server to stop producing answers for a stream the client dropped -
and the channel pair a dropped stream does not return to the reuse queue. The rest of this entry
is the state before that fix.

`ShoalResultStream::next` releases the client-side state for a query only when the response it
just returned was the last one:

```rust
if end {
    // remove this stream id from our channel map
    self.channel_map.pin().remove(&self.id);
    // take the ends of our channel
    if let (Some(tx), Some(rx)) = (self.response_tx.take(), self.response_rx.take()) {
        self.channel_queue_tx.send((tx, rx)).await?;
    }
}
```

`shoal-core/src/client.rs:1032-1039`, and the same block in `ShoalUnorderedResultStream::next`
at `:1231-1238`

Neither stream type implements `Drop`. `Shoal` does (`:467`) and it does not walk `channel_map`.
So there are three ways to end a stream without reaching that block:

| How | What is left behind |
| --- | --- |
| Drop the stream before its last response | One `channel_map` entry, and a channel pair that never returns to `channel_queue` |
| ~~`next()` returns `Err` — a failed `access`, a closed proxy~~ | ~~The same~~ — fixed by [F11](../features/error-channel.md): both `next()`s now run the release block on the `Err` path as well as the `end` path |
| `skip()` past the end, or any early `return` in the caller | The same |

The `channel_map` entry is what the proxy looks a response up in (`:536`), so the leak is not only
memory: every response the server later sends for that query id is delivered into a channel with
no reader, which is unbounded. And `channel_queue` is a reuse pool, so a leaked pair is a channel
the next query has to allocate instead of reusing (`:188-191`).

**This is the client-side twin of [item 32](resolved/client-gone-broadcast.md)**, since resolved,
— state keyed by something that goes away, with nothing told about it — and it has the same
consequence, which is that an ordinary well-behaved caller leaks. It is bounded differently,
though: the server's leak is per connection and this one is per query.

`send_one` and `exists` are safe today, and it is worth saying why, because it is not obvious. Each
sends a single-query bundle, so the one response it reads back *is* the end of the stream and the
block above runs. Change either of them to read one response out of a multi-query bundle and they
stop being safe.

**Established by reading the source**, during the [August 2026 review](review-2026-08.md). It has
never been observed, and it could not have been: the streaming APIs have
[no test at all](test-coverage.md#the-streaming-client-apis), and every integration test goes
through `send_one`, `exists`, or a fully drained `send`.

**Fix direction:** a `Drop` impl on both stream types doing what the `end` arm does. The obstacle
is that returning the channel pair to `channel_queue` is an `async` send, which `Drop` cannot
await — so either the queue gains a synchronous `try_send` for this path, or `Drop` removes the
map entry only and lets the pair be dropped. Removing the map entry is the half that matters;
losing a pooled channel is a missed reuse, not a leak.

**`Drop` alone fixes the client and leaves the server wrong**, which is worth knowing before taking
the easy half ([D6](../direction/connection-pool.md#drop-on-both-stream-types)). Once the entry is
gone, the responses the server is still producing arrive at a proxy that cannot find a channel for
them, ~~which today returns `Errors::ProtocolError` and kills the read task for that connection~~ —
[F11](../features/error-channel.md) made that a `WARN` and a `continue`, precisely because killing a
connection over one caller's leak takes every other query on it down too. So the easy half is no
longer actively harmful, and it is still half. The pair that is actually correct is `Drop` plus a `Cancel` message telling
the server to stop, and `Cancel` is a message type the wire format **has a discriminant for and no
wiring behind** since [F10](../features/framing-and-protocol-evolution.md) — so this is no longer
blocked on a flag day, only on the send and the handler
([D2](../direction/framing.md#message-types)).

### 62. A server that has exited leaves its client connections open

`ShoalPool::exit` (`shoal-core/src/server.rs:104-117`) sets `should_shutdown` and joins the shard
threads. The listener goes with them — a connection attempt to the port is refused afterwards — but
the sockets of connections that were already **established** are not closed. A client holding one
sees no `FIN`, so its read loop stays parked in `read_exact` forever and any query written to that
connection waits for a response no thread is left to produce.

The per-connection tasks `client_acceptor` spawns hold the `TcpStream`s, and nothing drains or
cancels them before the executor goes away. When the server runs in the same process as the client —
which every integration test does — there is no process exit to have the kernel clean up after it
either.

**Established by reproducing it**, while building [F11](../features/error-channel.md). A test that
started a server, called `pool.exit()`, and then sent one query never got an answer; instrumenting
`TcpProxy::start` showed the read loop had not returned for any of the pool's ten connections, so
the client-side sweep F11 added had nothing to fire on. Connecting to the port after `exit()`
returns `ECONNREFUSED`, which is what separates "the listener closed" from "the connections closed".

This is the server-side twin of the gap F11 closed on the client. F11 made a connection that
*ends* tell the queries it owed; this is a connection that never ends.

**Fix direction:** two halves, and the first is small. `client_acceptor`'s per-connection tasks
should be held rather than detached, and cancelled on shutdown, so the sockets are dropped before
the executor is — which turns this into an ordinary EOF that the client already handles.
The second half is that a client cannot rely on the peer being well behaved about it, so it wants a
deadline: a `Ping`/`Pong` health check ([item 23](#23-client-stream-and-pool-rough-edges),
`MessageType::Ping` is reserved and unwired) or a per-query timeout
([TODOs](todos.md)). Neither is the error channel — F11 gave a failure somewhere to go and cannot
invent a failure nobody detected.

### 63. Nothing limits how often a peer may guess a password

`server_auth` (`shoal-core/src/server/shard.rs`) runs an exchange for anything that reaches the
port and completes a handshake, and there is no counter anywhere — not per source address, not per
username, not per shard. A peer can open a connection, guess, be refused, and open another one, as
fast as it can complete three round trips.

The asymmetry is what makes it worth filing rather than shrugging at. Each guess costs the
**server** one PBKDF2-HMAC-SHA-256 at the credential's iteration count — on the order of a
millisecond of CPU on a shard that is single-threaded and is also serving queries — while costing
the client a `format!`, because a client that intends to fail does not have to derive anything. A
peer that sends a syntactically valid proof of the wrong 32 bytes gets the server to do all of the
work and does none of it.

That makes this two defects wearing one number. It is a **credential** exposure, and it is more
immediately a **denial of service** one: a few hundred connections a second is a shard spending
its time on PBKDF2 rather than on the table it owns, without ever authenticating.

**Established by reading the source**, while building
[F12](../features/authentication.md). Not reproduced, and the reproduction is the interesting part:
what a working `connect` workload ([O30](optimizations.md)) would have to show is how many refused
exchanges per second it takes for a resident get's p99 to move.

**Fix direction:** the cheap half is a per-connection cap of one exchange — a peer that fails is
already closed, so what this really bounds is reconnection, which belongs with a per-source-address
rate limit and does not exist. The half that removes the asymmetry is to do the derivation
**after** cheap validation and to bound concurrent in-flight exchanges per shard, so that a flood
queues rather than compounds. Note the decoy path ([F12](../features/authentication.md)) constrains
the shape of any fix: whatever is added must cost the same for a user that exists and one that does
not, or it becomes the enumeration oracle the decoy exists to prevent.

### 64. Fifteen pages cite the client at a path it left when the crates were split

**Filed as four `direction/` pages and it is fifteen pages across four chapters** — see the widened
scope at the end of this item.

Every page in the direction chapter that argues from the zero-copy response path quotes the same
three lines and cites them as `shoal-core/src/client.rs:524-527`, `TcpProxy::start`. The read is now
at `client.rs:1063-1066` and lives in `TcpProxy::read_frame`, because
[F10](../features/framing-and-protocol-evolution.md) split frame decoding out of the relay loop. The
quoted code is also one refactor stale — it reads `len` where the tree reads `frame.rest_len`.

| Page | What it cites |
| --- | --- |
| `direction/transport.md` | `client.rs:524-527` — inside D1's argument that QUIC's crypto ends the zero-copy read |
| `direction/overview.md` | `client.rs:524-527` — inside "the constraint every page inherits", which every other page refers back to |
| `direction/authentication.md` | `conf.rs:132-138` for `Networking`, which is now `conf.rs:138-157` and has a third field |
| `api/client.md` | the same read, in the `ShoalResponse` section |

`direction/encryption.md` carried the same citation and is corrected, which is how this was found.
The other four are left as they are rather than fixed in passing, because a sweep that fixes one
stale citation and not the fifteen others on the same pages is the drift this item is about.

**Established by reading the source**, while correcting [D4](../direction/encryption.md).

**Fix direction:** the same treatment this page already gives itself — a
re-resolution pass over every `file:line` in `docs/src/direction/` and `docs/src/api/`, with the
symbol name kept beside each one so the next drift is greppable. ~~The August 2026 review did this
for the appendix and did not cover the direction chapter, which had just been written and was
correct at the time.~~

**Both halves of that sentence are now wrong, and the item is larger than it was filed as.**

*The appendix is no longer clean.* [F15](../features/client-server-split.md) moved the client into
its own crate after the review, so the citations did not drift by lines — the file they name stopped
existing. `shoal-core/src/client.rs` is cited **26 times across 15 pages**, and two of them are on
[Optimizations](optimizations.md), the page whose whole function is to say where a cost lives.
Those two, and six more that drifted the ordinary way, are corrected; the remaining pages are not.

*And the line this item quotes is itself stale twice over.* It says the read "is now at
`client.rs:1063-1066`". That was true of `shoal-core/src/client.rs` when this was written and there
is no such file; the read is at `shoal-client/src/client.rs:1489-1492`, and it has since acquired a
third defect worth naming —
~~[O37](optimizations.md#o37-the-client-zeroes-a-response-buffer-and-immediately-overwrites-it), the
`resize(len, 0)` that memsets a buffer the next line overwrites~~ — since
[done](../features/read-buffers-are-filled-not-zeroed.md), so that quoted read is now stale a third
time and in a third way: the lines it names hold a different call. **An item about stale citations
went stale**, twice, which is the strongest possible argument for the fix direction it proposes.

The scope is therefore `docs/src/direction/`, `docs/src/api/`, `docs/src/architecture/` and the
remainder of the appendix — and the pass is worth doing as one sweep with the symbol names added,
rather than as fifteen incidental corrections, for the reason this item already gives.

### 66. The release profile nothing has been reading

`shoalctl/Cargo.toml:28-30`, and the absence of a `[profile.release]` in `Cargo.toml`

`shoalctl` sets `codegen-units = 1` and `lto = true`. Cargo reads `[profile.*]` only from the
workspace root, so both lines have been ignored for as long as they have existed — cargo says so
on every build:

```
warning: profiles for the non root package will be ignored, specify profiles at the workspace root:
package:   /home/mcarson/projects/shoal/shoalctl/Cargo.toml
workspace: /home/mcarson/projects/shoal/Cargo.toml
```

There is no `[profile.release]` at the root either, so **every release build in this workspace —
the server, `shoal-workload`, and every benchmark capture ever taken — uses the default profile:
`lto = false`, `codegen-units = 16`.** This is the same class of trap the root manifest already
records about a `[build]` table being silently ignored there (`Cargo.toml:23-25`).

It is not only a missed optimization. Without LTO, rustc will not inline across a crate boundary
unless a function is generic, `const`, or carries `#[inline]` — which is why splitting the crate
in [F15](../features/client-server-split.md) had to add thirty `#[inline]` attributes, and why
doing so turned out to *improve* the header codec by up to 80%: those calls had always been
cross-crate from the benches and the client, and had never been inlinable.

**Established by reproducing it** — cargo emits the warning above on every command run in this
workspace.

**Fix direction:** decide what the release profile should be and put it at the root. Note that
adding one changes the codegen of every binary and therefore invalidates every stored capture, so
it needs its own before-and-after and a note on
[Performance Baseline](../performance/baseline.md). Whether `lto = "thin"` would let
the thirty `#[inline]` attributes be dropped again is the interesting half of the question.

### 69. `shoalctl` and the tests still install no tracing subscriber

**Mostly resolved.** [F34](../features/benchmark-tracing.md) put a subscriber on `shoal-workload`,
so a benchmark capture honors the `tracing:` section of its config and the engine's spans can be
seen — [the resolved page](resolved/benchmark-tracing.md) has the whole of it. What is written
there and not here is closed. Three things are not.

`shoalctl/src/main.rs`, and every test in the workspace

**`shoalctl` installs nothing**, so an operator using it to talk to a server gets no structured
output from the client half — including the `event!(Level::ERROR, ...)` calls that report a dead
connection or a refused frame. The fix is the one `shoal-workload` took: call `trace::setup_with`
in `main` and hold the guard. It has not been done because nothing has needed it yet, which is a
reason to file it rather than a reason it is fine.

**No test can observe any event the server emits**, and this one cannot be fixed the same way.
`setup` installs a **global** subscriber, so the first test to call it decides what every other
test in that binary sees — which is why `shoal/tests/disk_lookups.rs` installs its own local one
rather than reaching for `setup`. The recovery summary has no automated coverage for exactly this
reason ([Test Coverage](test-coverage.md)). Closing it needs a **non-global** path out of
`trace.rs` — something returning a `Subscriber` for a caller to scope with
`tracing::subscriber::with_default` — and F34 did not build one.

**Whether a library should install a global subscriber at all is still unmade.** F34 sidestepped it
by putting the install on the binary, which is right for the benchmark and says nothing about a
program embedding `ShoalPool::start` and expecting the config it handed over to be honored. That
program today gets silence unless it calls `setup` itself, and nothing tells it so.

**Fix direction:** the `shoalctl` half is three lines and is unblocked. The test half wants
`trace::subscriber(conf, &TraceOptions) -> impl Subscriber` alongside `setup_with`, with `setup_with`
built on it — then a test scopes one for its own duration and the global install stays the binary's
decision.

### 70. An axis writes more digits than its ticks have room for, and twice labels two at the same number

`shoal-bench/src/render/chart/sweep.rs`, the `y_label_formatter` passed to `themed_mesh!`

`chart-grid-latency` draws `900.00 µs`, `1.00 ms` and `1.00 ms` as three consecutive y ticks, and
`chart-row-size-latency` does the same at several places on its axis. Two gridlines carrying the
same label is a reader counting decades wrong: the chart says the two lines are at the same value
and they are not.

The cause is rounding, not placement. plotters chooses key points on a logarithmic axis at ratios
that are not round numbers, and `fmt::duration_ns` writes two decimal places in each band, so
1,000,000 ns and 1,004,000 ns both come out `1.00 ms`. Nothing detects the collision because the
formatter sees one value at a time and has no memory of the previous tick.

[F19](../features/chart-legends.md) fixed this for the **x** axis of every sweep, by ticking at the
values the workloads were measured at instead of at a spacing plotters chose — there are never many
of those and they are exact. The y axis has no such list: it is a continuum of measured costs, and
picking ticks for it means choosing them rather than reusing them.

**The same formatter crowds an axis it does not duplicate.** `chart-hotpath-scopes` ticks at
`0.00 ns`, `50.000 s`, `100.000 s` … `450.000 s`, spaced 38 user units apart. `150.000 s` is nine
characters at a 9.68 unit font, so two adjacent labels want about 44 units between their centres
and have 38. Nothing here is wrong, only unreadable, and the cause is the same one: a formatter
that writes a fixed number of digits per band, chosen without reference to how much room the axis
gave it. Three decimals in the seconds band exist so that a 1.802 s measurement in a *table* keeps
its precision; on an axis whose ticks are round multiples of fifty they are three characters of
nothing. This half is older than item 70's other half and predates
[F19](../features/chart-legends.md) — `hotpath_scopes.rs` is untouched by it.

**Established by reading the generated pages.** `sed -n '/chart-grid-latency/,/<\/svg>/p'
docs/src/performance/grid.md | grep -E '^[0-9.]+ (ns|µs|ms|s)$' | uniq -d` prints `1.00 ms`. The
crowding is read off the tick anchors in `docs/src/performance/attribution.md`, which are `x="..."`
attributes 38 apart on the `y="348"` row.

**Fix direction:** give the value axis a key-point list of its own — round numbers of the unit the
range lands in, one per half decade — rather than deduplicating labels after the fact. Dropping the
duplicate leaves an unlabelled bold gridline, which is a smaller lie than the current one but is
still a chart that has a gridline it will not name. `sweep::Scale` already takes a tick list, so
the mechanism exists; what it needs is a generator for it.

Everything that has been fixed, and why it was fixed the way it was, is in
[Resolved Issues](resolved-issues.md). The SHQL parser has gained test coverage at both stages
([SHQL](../api/shql.md#testing)) — items 26–29 were found while writing it.

### 71. `throughput_sensitive` is configured, documented and mostly unused

`shoal-core/src/server/tables/storage/fs/map.rs:415` and `:433`, `ArchiveMap::get_active_writer`

`FileSystemThroughputWriterConf` carries a `buffer_size` and a `write_behind`, both defaulted, both
documented in [Configuration](../getting-started/configuration.md) as the settings for "the lower
latency but high throughput sensitive io". Exactly one writer reads them:

```rust
// map.rs:403 - ArchiveMap::new_writer, the map's own intent log
let writer = DmaStreamWriterBuilder::new(file)
    .with_buffer_size(self.conf.throughput_sensitive.buffer_size)
    .with_write_behind(self.conf.throughput_sensitive.write_behind)
    .build();
```

The writers for the archives themselves do not:

```rust
// map.rs:415 - the cached-handle branch
return Ok(DmaStreamWriterBuilder::new(file.dup()?).build());
// map.rs:433 - the freshly-opened branch
Ok(DmaStreamWriterBuilder::new(file).build())
```

Both build with glommio's defaults. `map.rs:224`, the temporary file a map snapshot is staged
through, does the same. So the section named for throughput governs the intent log of the archive
*map* — a small, latency-shaped write — and not the bulk data path it appears to name. A deployment
that raises `throughput_sensitive.buffer_size` to tune bulk ingest changes nothing about how the
archives are written.

There is a second, smaller wart in the same struct. `FileSystemThroughputWriterConf::write_behind`
is a **count** and carries `deserialize_with = "utils::deserialize_byte_size"`
(`fs/conf.rs:154-156`), so `write_behind: "4Ki"` parses to 4096 in-flight writes. The matching field
on the latency writer has no such annotation. This is already noted in
[Configuration](../getting-started/configuration.md) and is filed here so it is fixed alongside the
line above rather than separately.

**Established by reading the source, and since reproduced by measurement.**
[F20](../features/configuration-sweeps.md) swept both settings for exactly this reason —
`macro/conf/storage/throughput_buffer/*` and `macro/conf/storage/throughput_write_behind/*` — and
the `F20-conf` capture of 2026-08-22 came back flat: **1.01× between the fastest and slowest arm of
each**, across a four-fold sweep of the buffer size (32Ki through 1Mi) and a sixteen-fold sweep of
the queue depth (1 through 16). Both fail the *Real?* gate — their arms' observed run intervals
overlap — so there is no evidence either setting changes anything at all on this path. The argument
from reading `map.rs` predicted a flat sweep and the sweep is flat, which is as close as a sweep
gets to reproducing a wiring defect.
[Configuration and what each setting is worth](../performance/configuration.md) says so on the page
rather than leaving a reader to conclude the device does not care.

**Fix direction:** thread `&self.conf` into both branches of `get_active_writer` the way
`new_writer` already does. The cached-handle branch is the awkward one — it builds a writer from a
`dup()`ed handle it did not open, so the configuration has to reach it from `self` rather than from
the call site, which it can. Decide separately whether the staging writer at `:224` should be
configured or should stay on glommio's defaults deliberately; it writes a whole map in one pass and
is not obviously the same kind of write. Then re-run `--group conf/storage` against `F20-conf` as
the before, and the two sweeps should stop being flat — which is also the test that the fix did
anything. That is nineteen minutes of machine time, not an afternoon.

### 72. An arm with no writes is reported as an arm that ran once

`shoal-bench/src/render/tables.rs:483-493`, `interval`

The *write p50 across runs* column of
[Configuration](../performance/configuration.md#every-arm) reads `one run` for every `r100` arm of
the shard and memory sweeps. All eleven of them ran five times — `F20-conf` records
`runs: 5` and five wall clocks for each. They have no *writes*, because `r100` is a pure read share.

```rust
// tables.rs:486 - `interval`
match arm.capture.stat_interval_ns(op, metric) {
    Some((low, high)) => format!(...),
    None => "one run".to_string(),
}
```

`stat_interval_ns` (`model/macro_layer.rs:351`) returns `None` for two unrelated reasons: fewer than
two runs, and `run.ops.get(op)?` failing because the arm never performed that operation. The caller
collapses both into the sentence that describes only the first. The neighbouring `write p50` and
`write p99` columns get this right and print `–`, so one row says the arm has no writes twice and
then says it ran once.

It is a wrong sentence rather than a wrong number, which is why it is filed here rather than
mattering to a verdict — but it is a statement about provenance on a page whose whole argument is
that a difference counts only when the intervals are disjoint, and a reader checking whether an arm
was measured enough times is told it was not.

**Fix direction:** distinguish the two `None`s. The narrow version is for `interval` to ask whether
the op exists before asking for its spread, and return `–` when it does not. The version that stops
this recurring is for `stat_interval_ns` to return something with three cases rather than an
`Option`, since every caller that formats it has the same choice to make and two of the three
call sites (`tables.rs:59`, `:492`) already make it independently.

### 73. The hotpath layer's artifact is shared between the workloads that write it

`shoal-bench/src/run/plan.rs:461-482`, `build_plan`

**Half of this is fixed.** The stage layer had the same defect and is
[resolved](resolved/stage-artifact-overwrite.md) under this number; what stays here is the hotpath
half, which the fix deliberately did not take.

The hotpath phase loops over the workloads that opted into attribution and directs every one of
their profiles to the same file:

```rust
// plan.rs:466 - the hotpath phase
for id in instrumented_for(inputs, Layer::Hotpath) {
    steps.extend(wipe_steps(inputs));
    steps.push(Step::Command(workload(
        inputs,
        id,
        vec![/* ... */],
        Stdout::LastLine(artifact(inputs, Layer::Hotpath)),
    )));
}
```

A second profiled workload would overwrite the first, and the artifact that resulted would be
indistinguishable from a correct one — right schema, plausible scopes, and silently describing one
workload where it claims to describe the capture.

Nothing reaches it today: `PROFILED_WORKLOADS` holds one workload, and it stopped being the list the
stage layer reads, so growing the stage layer no longer grows this one. That is why this is filed
rather than fixed — it is latent in exactly the way the stage half was, and the moment somebody adds
a second hotpath workload it is live.

**Fix direction:** the same three pieces the stage half took, and
[Resolved #73](resolved/stage-artifact-overwrite.md#the-fix) has them written down. A scratch path
per workload; a `HotpathProfiles` artifact keyed by workload, with the current single-profile shape
accepted as version 1 so the committed captures keep rendering; and `collect::hotpath` folding the
scratch files rather than only checking one. The awkward part is that a hotpath profile arrives as
the last line of stdout rather than as a file the run writes, so `Stdout::LastLine` has to take a
per-workload path and the collector has to parse each one.

### 75. A control that was measured at every width is not drawn, and the caption says it is

`shoal-bench/src/render/chart/micro_scaling.rs`, the width series selection

[Micro benchmarks](../performance/micro.md#how-the-cost-grows-with-the-width-of-one-row) opens its
width chart by naming its own control:

> A **flat** line is a fixed per-call cost - the header decode is here as exactly that control,
> since eight bytes is eight bytes at every width.

It is not here. `chart-micro-scaling-width` draws eight series and its `aria-label` says so
(`Cost of 8 operations against row width`), while [F22](../features/row-size-benchmarks.md) swept
**ten** `wire_codec/width/*` groups. The two it drops are `request/decode/header` — the control the
caption points at — and `request/decode/access_unchecked`. The growth table underneath the chart has
the same eight rows and the same two omissions.

The measurement itself is fine, which is what makes this a documentation defect rather than a
benchmark one. From `f22-row-size.micro.json`, `request/decode/header` runs 0.7071 ns at 64 B and
0.7088 ns at 64 KiB — a quarter of a percent across a thousand-fold change in row width, which is
exactly the flat control the axis needs to be read against. A reader who trusts the caption
concludes the axis has been validated against a control they can see; a reader who counts the series
finds it has not.

**Evidence: established from the committed artifact**, not from reading the source — the ten swept
identifiers are in `f22-row-size.micro.json` and eight of them are on the page. That is the reverse
of how most entries here were found, and it is why it survived review: nothing compares the number
of series a chart draws against the number its data contains.

**Fix direction:** two choices, and they are not equivalent. Drawing all ten makes the caption true
and costs a reader two more curves on a chart that already has eight — which is the palette's cap
([F19](../features/chart-legends.md)), so ten would need a decision about colour rather than a
selection change. Naming the control in the table but not on the chart is cheaper and keeps the
chart at eight, but then the caption has to say where the control is. The third option is the one to
avoid: quietly deleting the sentence, which loses the fact that the axis *has* a control and that it
holds. This is also the second instance of what [F18](../features/results-pages.md) filed as a
limitation — nothing checks that a page's prose still describes what it draws — after the four
caption drift found by grep in `0851a22`.

### 77. A macro-only capture can never reach the pages it was taken for

`shoal-bench/src/render/page.rs`, `Page::current_for`

**The selection this item was filed against is gone.**
[Resolved #79](resolved/micro-only-capture-current.md) found the same conflation from the other
side — a *micro*-only capture becoming current and emptying the seven macro pages — and resolved the
current capture **per layer**, so a macro-only capture can now become the macro pages' source.
**That is half of what this item asks for and not the half it is about.** What is left is the part
that matters: a page has no way to say *these rows in particular have been re-measured since*, so a
correction that is committed, comparable and newer is still invisible to the reader looking at the
figure it corrects.

The selection it was filed against was this, one label shared by every page:

```rust
// which capture the current numbers come from: the caller's choice, or the most recent one
// that produced a micro layer
let current = match &args.current {
    Some(label) => label.clone(),
    None => timeline
        .iter()
        .rev()
        .find(|snapshot| snapshot.micro.is_some())
        .map(|snapshot| snapshot.label.clone())
        .unwrap_or_default(),
};
```

**A capture taken with `--group` or `--layer macro` has no micro layer**, so it can never be
`current`, however new it is and however exactly it covers the arms a page draws. That is most of
what [F21](../features/benchmark-groups.md) exists to make cheap: `list --groups` prints twelve
sets and prices them precisely so a narrow question does not cost two hours, and
[CLAUDE.md](../../../CLAUDE.md) tells a reader to use a group rather than a prefix. A capture taken
that way lands in `docs/perf/runs/`, appears on the freshness table, appears on
[Every workload](../performance/all-workloads.md) — and is not what any other page reports.

~~**The live instance**, which is how this was found.~~ **Overtaken, and not by a fix.**
`f24-routing` is a full capture taken after F23, so the configuration page now reports these arms
at 22,486/s from a capture that describes the current writer, and the figures below no longer
appear anywhere. The instance is kept because the *mechanism* it describes is untouched: it took a
two hour capture of every layer to dislodge a stale number that a ninety second one had already
corrected, which is exactly the cost this item is about.
[Configuration and what each setting is worth](../performance/configuration.md) reports the
`latency_buffer` sweep at `4Ki @ 64 KiB` as **18,537/s** and calls `256Ki` the best rung at 1.22×.
Those come from `f22-row-size`, whose macro layer the same site marks **stale, four commits**,
because [F23](../features/self-sizing-staging-buffer.md) changed the writer underneath it.
`f23-staging-buffer` measured those exact fifteen identifiers at the current commit on a clean
tree and says **22,619/s** and 1.010× of spread. Both numbers are committed, one is drawn, and it
is the one that describes code that no longer exists.

**Not a wrong number, and that is what makes it bad.** The page is internally consistent, its
provenance line is accurate, and the badge on the overview page does say the capture is stale. The
defect is that the page has no way to say *these rows in particular have been re-measured since*,
so the correction is available, committed, and invisible to the reader who needs it.

**Evidence: established by rendering.** `render` was run on a clean tree at `c46310b` with
`f23-staging-buffer` committed; the fifteen new rows appear on `all-workloads.md` and every other
page is unchanged apart from its provenance line and the staleness badges. Then traced to the
`find(|snapshot| snapshot.micro.is_some())` above.

**Fix direction:** the choice conflates two questions — which capture is the *reference* for the
micro layer's comparisons, and which capture most recently measured *this arm*. The first genuinely
wants a full capture. ~~The second is per-arm and has an answer here.~~ The second turned out to be
**two** questions rather than one: which capture measured this *layer*, which
[Resolved #79](resolved/micro-only-capture-current.md) answered, and which capture measured this
*arm*, which is what is still open. Drawing each arm from the newest
capture that measured it would mix captures within a page, which is exactly what
[Baseline](../performance/baseline.md) forbids and should stay forbidden — so the cheaper fix is to
keep one `current` and have a page *say* when a newer capture covers arms it is drawing, the way the
freshness table says a layer is stale. A row that could be labelled "re-measured in
`f23-staging-buffer`" would have made this visible without joining two machines' numbers into one
table. Filed as work rather than fixed here, in [TODOs](todos.md).


### 81. A stage flag saying a query waited on disk is defined, never set and never read

`StageFlags::loaded_from_disk` (`shoal-core/src/server/stage_profile.rs:110`) is documented as
"whether this query had to wait on a partition being read from disk", and the setter for it is
generated beside every other flag's:

```rust
set_loaded_from_disk(bool) => loaded_from_disk
```

`stage_profile.rs:423`

**Nothing calls it.** A repository-wide search for the name finds the field, its `Default`, the
generated setter, and one integration test whose name happens to contain the words. No query
anywhere sets it, so every stage record ever written says `false` — including the records of
queries that did park on a disk read, which is exactly the population the flag exists to separate.
Nothing reads it either: no report in `shoal-bench` joins on it, so the wrong answer has never
been printed.

That is what makes this worth filing rather than deleting. The stage layer's job is to say where a
query spent its time, and *whether it waited on IO* is the single largest fork in that answer — a
report that pooled parked and unparked gets would put the boundary between two distributions
wherever the mixture happened to fall, which is the same mistake `StageOp` exists to avoid
([Resolved #76](resolved/stage-join.md) is what happens when a stage layer is trusted without
being read back).

**Evidence: established by reading the source**, and by the search above rather than by running
anything — a flag that is never set produces no failure to reproduce.

**Fix direction:** `block_on_load` returning `true` is the moment a query becomes one that waited,
and `PersistentTable::load_partition` releasing it is the moment it stops. The flag belongs on the
`QueryMetadata` that is parked in `blocked` and carried into the replay, which already survives
that round trip — `skip_disk` and `failed` are set on exactly that path
([Resolved #16, 51](resolved/partition-load-failure.md)). Setting it is small; the part worth
doing carefully is the report, which has to treat the two populations as two rather than
subtracting one from the other. Found while fixing
[Resolved #80](resolved/never-flushed-partitions.md), which is about the other side of the same
call.

### 87. A collector that rejects every span is indistinguishable from one that accepts them

`opentelemetry-otlp` 0.28, `OtlpHttpClient::export`
(`~/.cargo/registry/.../opentelemetry-otlp-0.28.0/src/exporter/http/trace.rs:53-71`)

The OTLP/HTTP response body is an `ExportTraceServiceResponse`, whose `partial_success` field
carries `rejected_spans` and an `error_message`. A collector uses it to say *I took the request and
threw the contents away* — the wrong tenant, a resource limit, a malformed attribute. The exporter
never reads it:

```rust
let response = client.send_bytes(request).await.map_err(...)?;

if !response.status().is_success() {
    return Err(OTelSdkError::InternalFailure(error));
}

Ok(())
```

The status is checked and the body is dropped on the floor. So a collector answering `200` with
`partial_success { rejected_spans: 412 }` produces exactly the same result inside Shoal as one that
stored all 412 — `Ok(())`, no `BatchSpanProcessor.ExportError`, nothing at any log level.

**Established by reproduction**, while diagnosing spans that were not appearing in Grafana. A proxy
placed between the example and the collector captured the real answer:

```
sent=178434B tenant='Shoal' -> 200 body=b'\n\x00'
```

`\n\x00` is field 1, length zero: an empty `partial_success`, meaning nothing was rejected. That
run was healthy, and the point is that **the unhealthy case would have looked identical from
inside Shoal** — which is why the failure was chased through Shoal for as long as it was when it
was never there. The 412 is the exact span count of one `cargo run --example tmdb`, counted by
decoding the protobuf the exporter sent.

**Fix direction:** wrapping `SpanExporter` to decode the response is not possible from outside the
crate — `OtlpHttpClient::export` discards the body before returning, so there is nothing left for a
wrapper to inspect. The options are upstream, or a hand-rolled exporter over `opentelemetry-proto`
0.28 (already in the graph, as a dependency of `opentelemetry-otlp`), which would be the third
implementation of an OTLP client in this tree's dependency closure and is hard to justify for one
field. The cheap and honest thing, done in
[Observability](../operations/observability.md#tracing), is to write down that confirming delivery
means asking the collector rather than reading Shoal's logs.

### 91. A compaction that fails after writing ends the compactor

`shoal-core/src/server/tables/storage/fs/compactor.rs`, `FileSystemCompactor::start`,
`JobFailure::Fatal`

~~Every job is `?`. A compaction that fails - an archive it cannot open, a read that comes back
short - returns the error out of the loop, and the compactor task ends.~~ A job that fails
**before it writes** is tried again with a backoff since
[Resolved #91, 107](resolved/compaction-retry.md), which is where every failure seen so far
was. The remainder is the harder half that page names: a job that fails **after** its first
write - a partially written archive, a map entry not yet re-pointed - still ends the compactor
with the error, the next rotation's send into the closed channel ends the shard, and a client
with a query in flight to that shard waits forever ([item 62](#62-a-server-that-has-exited-leaves-its-client-connections-open)).

**Established by reading the source.** No failure after the write has been reproduced; the
fault the tests inject is met at the load, before it.

**Fix direction:** the "old complete or new complete" rule
[C7](../distributed/failover.md#snapshots-and-atomic-installation) sets for snapshot
installation, applied to the archive write and the map's re-pointing, after which a failed
write can be retried by discarding the incomplete new; and a rotation that finds its compactor
gone should say so rather than end the shard.

### 92. A table with two `#[shoal(partition)]` fields does not compile

`shoal-derive/src/traits/partition_key.rs`, `add`: the `partition_key_args` branch for several
fields

A composite partition key is a tuple: `type PartitionKey = (A, B)`. The generated
`get_partition_key` calls `get_partition_key_from_values(&(&self.a, &self.b))`, which is a
`&(&A, &B)` where the signature wants `&(A, B)`, so every table with more than one partition field
fails to expand:

```
error[E0308]: mismatched types
   |     Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, ...
   |                                                    ^^^^^^^^^^^^^^^^^^ expected `u64`, found `&u64`
```

**Established by reproduction**, by the golden key test [F37](../features/node-identity-control-plane.md)
wrote for [item 65](resolved/gxhash-pin.md): its third table, keyed by a `u64` and a `String`,
was the first composite partition key in the workspace and did not build. The output above is
what `cargo test -p shoal --test partition_keys` printed before the table was removed. Item 41
already records that SHQL cannot express such a key; this says the derive cannot either.

**Fix direction:** hash the fields directly in `get_partition_key` - one `hash_field` per field
in declaration order, which is what `get_partition_key_from_values` does with the tuple - rather
than building a tuple of references to pass through it. Then add the composite shape to the
frozen key set, obtaining its literals the way the other eight were.

### 93. The archived partition hash disagrees with the live one for every string key

`shoal-derive/src/traits/partition_key.rs`, `add`: `hash_archived_stmts`

`get_partition_key_from_values` hashes a `String` through `Hash for str`, which writes the bytes
and then a `0xff` terminator. `get_partition_key_from_archived_insert` hashes the same field as
`hasher.write(intent.name.as_bytes())`, with no terminator. The two produce different `u64`s for
every string key, and would place an archived row in a different partition and tablet from the
row that was inserted.

**Latent.** `get_partition_key_from_archived_insert` has no caller in the workspace, so nothing
routes by the wrong number today. It is filed because the function is on the trait every schema
implements, a caller is one line away, and the first one would be a silent data loss rather than
an error. **Established by reading the source** while writing the golden key test for
[item 65](resolved/gxhash-pin.md); the test deliberately freezes the live path alone.

**Fix direction:** either delete the archived variant until something needs it, or make it write
what `Hash for str` writes - `write_str`, which the std hasher contract spells as the bytes then
`0xff` - and freeze the archived hash beside the live one in `partition_keys.rs` so the two cannot
drift again.

### 117. A restore under the fixture suite's load finds its target table not yet empty

`single_node_data_has_a_verified_cluster_migration_path` fails under the fixture suite at six
threads, about one run in two here, and passes alone. A group's restore comes to:

```text
{"Failed":{"reason":"2a02ce9f-…/0 holds 117 rows of the group's tablets; a restore is into
an empty table, and this one is not"}}
```

The emptiness check is the `Loading` phase's scrub in `server/shard/restore.rs`. The table was
new, and the only rows that could be in it are the restore's own. **Suspected, from reading the
source and not reproduced:** the step is driven again after a leader change. The group commits
`Loading`, loads rows, and loses its leader before committing past `Loading`. The next leader
resumes at `Loading` and scrubs rows the first attempt put there. The scrub needs to tell those
rows apart from rows that were there before the restore, for example by the restore's own
boundary. It must not skip the check on a resume, because the check is what refuses a
non-empty target.

**Established by running it**: the run for [Resolved #15](resolved/shard-mesh-admission.md)
failed it first, and the loaded fixture runs for
[Resolved #115](resolved/retry-sidecar-crash-window.md) failed it once in two.

### 118. The fixture's admin retry gives up while node zero's topology lags the leader

`mixed_versions_exchange_real_cluster_operations` fails under the fixture suite at six threads
and passes alone. The `ACTIVATE` it expects to be refused, by naming the pinned members, is
refused as stale instead:

```text
the refusal did not name the pinned members: {"error":"StaleVersion: stale version: the
request was written against topology version 15 and the cluster is at 19"}
```

The fixture's `admin` helper (`shoal/tests/cluster_fixture.rs`) reads node zero's own topology
version and retries on `StaleVersion`, eight times at 100 ms. Under load, node zero stayed four
versions behind the leader for longer than that. The server is right to refuse. The helper's
budget is a count of attempts where it should be a deadline, and a lagging view outlasts it.
This is the same shape as [Resolved #116](resolved/map-version-test-snapshot.md): a test
assuming the version it read is the current one. **Established by running it**, once in two
loaded fixture runs for [Resolved #115](resolved/retry-sidecar-crash-window.md).

### 119. `down_retains_placement_during_grace` kills a member before the third voter is promoted

The test starts three nodes and kills one without waiting for three voters
(`cluster.wait_voters(0, 3)`, which the M3 tests call). Under the workspace run at six threads
the third node was still a **learner** when the kill landed. The control group then had two
voters, one of them dead, and could not commit the verdict. The detector's own read had the
dead member at `phi` 300 after thirty misses, while `MEMBERS` kept it `up`:

```text
the dead member was never called down: {… "learners":["c21edec1-…"], …}
detector: {… "3c3efa4a-…":{"misses":30, …}, … "phi":300.0, "since_last_ms":30175.8, …}
```

The detector and the commit are right. The test's schedule is wrong: it asserts a verdict that
needs a control quorum, and its kill can land before that quorum exists. The fix is to wait for
three voters before killing. **Established by running it**: it failed once in the
whole-workspace run for [Resolved #115](resolved/retry-sidecar-crash-window.md) and passed
alone at once.
