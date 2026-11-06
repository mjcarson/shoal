# Optimizations

Work that would make Shoal faster, indexed the same way as [Known Issues](known-issues.md): each
entry names what the code does, where, and why it costs. Item numbers are prefixed `O` and are
never reused.

~~**None of these are measured.** They come from reading the source, and they are ordered by the
size of the argument for them, not by observed benefit. Anything here should be confirmed against
a profile before it is acted on — [Benchmarking](../performance/benchmarking.md) and the `hotpath`
feature are what that is for.~~

That was true of every entry below when it was filed, and it is worth keeping because it says
what the entries *are*: arguments from the source, ordered by the strength of the argument
rather than by observed benefit. What has changed is that there is now somewhere to settle
them. [F3](../features/performance-harness.md) built a harness that can resolve a few percent,
[Performance Baseline](../performance/baseline.md) records what the system currently
does, and the `hotpath` feature — which until then was wired up in a way that produced an empty
profile — reported 57 scopes on a `tmdb` run, which was the workload the figure was taken
from before [F8](../features/purpose-built-workloads.md) retired it.

So the rule is now stronger rather than weaker. **An entry is not acted on until a benchmark
exists that would show the difference**, and the entry says which one. An entry with no such
benchmark is asking for the benchmark first. A change that removes work from a path nothing is
waiting on is a change that only adds risk, and a change that cannot be measured is
indistinguishable from one.

**The first entry settled this way was [O3 + O23](#o3-every-archived-read-is-fully-validated-inside-a-tracing-span)**,
and [F4](../features/validated-archives.md) is worth reading for what the rule cost and bought. It
cost an extra capture: the benchmark that would show the difference did not exist and could not be
written without part of the change, so the enabling half landed and was measured on its own first.
It bought a 99.5% result with a control, a null, and a confirming repeat behind it — and it caught a
[measurement defect](#o24-two-benchmarks-move-with-the-shape-of-the-binary-around-them) that would
otherwise have read as a regression the change caused.

Defects are in [Known Issues](known-issues.md); several entries below share a root cause with one
and say so. An entry that has been done is struck through and kept, with what replaced it, for the
same reason a resolved issue keeps its page.

**Every citation on this page was re-resolved against the tree in August 2026**
([Review](review-2026-08.md)) and most had drifted. Two things that sweep is worth knowing about:
the quoted *original* text of a struck-through entry keeps its **original** line numbers, which now
point at unrelated live code — O6's and O17's are called out where they appear — and O2's code
snippets were still showing the pre-[F2](../features/projections.md) lines that the paragraph below
them said had changed.

## How these are ranked

Every open entry carries a scorecard under its heading, and [the priority queue](#the-priority-queue)
orders all of them. Four axes, graded the same way everywhere.

**Impact** is graded by *what backs the claim*, not by how large the claim is. That distinction is
the whole point of the rule above, so it is a column rather than a caveat:

| Grade | Means |
| --- | --- |
| **Measured** | A micro-benchmark or a baseline figure, cited on the entry |
| **Profiled** | A `hotpath` scope. Attribution only — [never a result](../performance/baseline.md#profile--where-the-time-goes) — so a call count is worth more here than a duration |
| **Asymptotic** | The cost grows in something a caller controls, so it is established without a number |
| **Argued** | Source reading alone. The default, and the weakest |

**Difficulty**:

| Grade | Means |
| --- | --- |
| **S** | A local edit or a type annotation |
| **M** | Contained to one module |
| **L** | Crosses module boundaries, or changes an internal type that travels between them |
| **XL** | Reaches the wire format, the on-disk format, or the client |

**Tradeoff** is `None`, `Contained` (a local behaviour change, revertible), or `Major` (safety,
determinism, or a compatibility break). A `Major` entry is not a worse entry — O3 is the best one
here — but it is one that needs a decision rather than a patch.

**Depends on** and **Blocks** carry hard edges only. **A missing benchmark is a dependency**, since
this page forbids acting without one, and it is the most common one. ~~Five~~ ~~Four entries are
blocked on a benchmark rather than on any code, after `f22-row-size` discharged O34's.~~ **The count
was never enumerated, and it was low.** Enumerated, the entries whose only blocker is an instrument
are: **O5**, **O12** and **O13** (a table-layer bench, unbuilt); **O11** and **O21** (a storage
write-path bench, unbuilt); **O25** (a with/without capture, which is a build rather than a bench);
**O27** (a workload over a schema that mixes table kinds, which none does); and **O30** (a `connect`
workload, where the missing benchmark *is* the entry). That is **eight**, not four.

~~**O11** and **O29** are blocked differently and should not be counted with them. Their instrument
exists and is [repaired](resolved/stage-join.md); what they wait on is a *capture* taken with it.~~
**That capture was taken** — `f24-routing` — so what those two waited on is discharged. O11 stays on
the list above, because the stage layer says *which* stage its copies live in and only a write-path
bench says what removing one is worth; ~~O29 comes off it entirely~~ **O29 came off it and has since
been done**, by [F25](../features/read-buffers-are-filled-not-zeroed.md) — which also found that the
stage reading of O29 was wrong, since the body read happens before the bundle's clock starts and no
stage contains it at all.

**A benchmark that runs is not the same as a benchmark that answers**, and this page has had one
instance of each failure. O35's ran and came back *negative* — it reattributed the entry's evidence
to load depth and cost it its rank, which is the dependency working. O11 and O29's ran and returned
*nothing*, because the layer it lives in joined no records for the workloads it was pointed at
([Resolved #76](resolved/stage-join.md)) — and when it was re-run and did join, the reading taken
off it for O29 named a stage that cannot contain what it was said to
([F25](../features/read-buffers-are-filled-not-zeroed.md)). **A benchmark that answers is not the
same as an answer to the question asked**, which is the third failure and the one this page had not
had an instance of. The second is worse than having no benchmark, because the
artifact it produced has the right shape and an empty middle. **When a Benchmark row here says a
capture exists, check that it joined** — the collector checks that per report now rather than across
the artifact, so a future capture cannot repeat it.

**That second failure is now closed.** `f24-routing` is the first capture taken with the repaired
instrument, and all four of its stage reports join completely — 20,000, 20,000, 512 and 200,000
records, with **zero** server-only and **zero** client-only on every one. So the question the stage
layer was pointed at three widths to answer, and could not, has an answer; it is quoted under
[O2](#o2-every-returned-row-is-copied-at-least-twice--the-resident-half-is-done-done) and
[O11](#o11-a-fresh-alignedvec-per-write-and-per-response) rather than here.

## The priority queue

Tiered rather than a single 1-to-N ordering, because an entry backed by a measurement and an entry
backed by an argument are not comparable on one scale, and pretending otherwise would undo the rule
this page opens with. Ordered inside each tier.

**Tier A — the cost is established and the change is contained.** These are actionable now.

| # | Entry | Impact | Diff | Depends on | Tradeoff | Adjudicable today |
| --- | --- | --- | --- | --- | --- | --- |
| ~~**A1**~~ | ~~[**O3** + **O23**](#o3-every-archived-read-is-fully-validated-inside-a-tracing-span)~~ — **done**, by [F4](../features/validated-archives.md) | Measured — 29.89 µs of a 30.43 µs cold single-row get | M | — | Contained | it was, and it was |
| ~~**A2**~~ | ~~[**O17**](#o17-handle_flushed-runs-on-every-message)~~ — **done**, by [F5](../features/flushed-sweep-gate.md) | Profiled — 711,638 calls became 21,279 | S | — | Contained | it was, on the profile alone |
| ~~**A3**~~ | ~~[**O34**](#o34-a-record-wider-than-the-staging-buffer-defeats-intent-log-batching)~~ — **done**, by [F23](../features/self-sizing-staging-buffer.md) | Measured, before and after — 1.22× at 64 KiB rows became **+22.0%** at the shipped setting, on disjoint intervals, and the sweep's spread collapsed 1.225× → 1.010× | S–M | ~~a `latency_buffer` sweep above the buffer~~ — discharged | Contained | it was, and it was, twice |
| **A4** | [**O13**](#o13-a-multi-partition-get-is-quadratic-in-the-partitions-it-names) (+ [**O12**](#o12-to_blocked-clones-the-whole-filter-set-per-blocked-partition) and [**O39**](#o39-routing-a-multi-partition-get-is-quadratic-before-the-query-reaches-a-table) beside it) — the quadratic multi-partition get, **twice**: once in the table and once in the router | Asymptotic — O(n²) in a caller-set n. **O39 measured** — 4.13 ns·n + 0.0109 ns·n² | S | — | None | O39 **yes**, by `routing/split_by_shard/get` ([F24](../features/routing-benchmarks.md)); O13 and O12 still need a bench over `PersistentSortedTable::get` |
| **A5** | [**O5**](#o5-the-hottest-maps-use-siphash), [**O14**](#o14-fixed-thousand-element-preallocations-on-per-call-paths), [**O28**](#o28-the-client-takes-two-guards-on-its-response-map-for-every-query-it-sends), [**O36**](#o36-every-get-re-collects-its-rows-into-a-fresh-vec-even-when-it-read-one-partition), [**O38**](#o38-a-response-that-arrives-out-of-order-is-validated-twice), [**O42**](#o42-a-get-replayed-after-a-disk-read-copies-rows-its-partition-is-now-holding), [**O43**](#o43-a-borrowed-row-costs-a-discriminant-it-usually-does-not-need) — hasher, allocation sizes, a doubled map guard, a re-collect per get, a second validation per reordered response, the one get per partition read that still copies, and the discriminant every borrowed row now carries | Argued, except O38 which is **asymptotic** in the row width | S | — | None | no — needs a table-layer bench, and O28 needs the client measured at all. **O38 is the exception**: `macro/transport/stream` against `stream_unordered` is a ready-made control |
| **A6** | [**O25**](#o25-two-instrument-spans-remain-on-per-query-paths) — two `#[instrument]` spans on per-query paths | Argued — but the cost is in the *uninstrumented* binary | S | — | Contained | no — needs a with/without capture |
| **A7** | [**O44**](#o44-one-trace-per-request-costs-a-span-per-query-and-one-per-frame) — one trace per request costs a span per query and one per frame | Argued — one more registry slab insert per query at `level: Info` | S | — | **Not contained** — every span on the query path re-parents off it | no, and the run that would settle it is the same one A6 wants: one capture at `level: Off` against one at `level: Info` |
| **A8** | [**O45**](#o45-the-clients-return-half-costs-two-spans-per-response) — the client's return half costs two spans per response | Argued — two more registry slab inserts per **response** at `level: Info`, and in the client rather than the server | S | — | **Not contained** — `Shoal::response` is what carries an answer back into the trace its send opened | no, and it is the *third* entry the one capture at `level: Off` against `level: Info` would settle |

~~**Do O34 first.**~~ **Done**, by [F23](../features/self-sizing-staging-buffer.md). It moved from
the bottom of this tier to the top on the strength of one capture, was for one release **the only
entry on this page whose cost was both measured and contained**, and was then acted on. Both
cautions that came with it were carried into the fix rather than around it. The *shape* correction
— the gain is in how many records share an aligned write, not in whether the record fits — is why
F23 is a sizing rule and not a larger default: it became `TARGET_RECORDS_PER_BUFFER = 8`, read off
the rung where the capture says the gain arrives. `f23-staging-buffer` then re-took the same fifteen
arms and confirmed it: **+22.0%** at 64 KiB rows for the shipped setting, and a sweep whose spread
collapsed from 1.225× to 1.010×. The boundary at 4096 is **still unbracketed**,
because the width axis still jumps 1024 → 8192 around it, and that is the one thing this entry still
cannot state.

**A4 is the head of the queue now**, and it is the first one here that a profile cannot settle: it
needs a benchmark over `PersistentSortedTable::get` that does not exist yet. So the tier has gone
back to being blocked on measurement rather than on code — which is the state this page prefers to
be honest about, since A3 is the only entry that has ever left it by being built.

**A4 grew a third entry, and it changes how its own evidence reads.**
[O39](#o39-routing-a-multi-partition-get-is-quadratic-before-the-query-reaches-a-table) is a second
O(n²) in the same caller-set *n*, in `group_by_shard` on the routing path rather than in the table —
found and measured by [F24](../features/routing-benchmarks.md), which built the `routing` bench this
page had wanted since [F3](../features/performance-harness.md). The consequence for O13 is the part
worth reading: **`macro/fanout/n` now has two known quadratics under it.** That curve is the only
evidence O13 has, the note under [the F8 block](#which-entries-a-benchmark-can-currently-adjudicate)
already says it answers O13's question rather than its cost, and this is a second and sharper reason
— a bend in it is not attributable to either entry until the isolated table-layer bench exists.
O39 itself is small in absolute terms and the entry says so.

A1, A2 and A3 are struck rather than deleted because what each got wrong is the useful part — A1
claimed the narrow form needed no `unsafe`, and there is no such form; A2 accepted a rotation delay
that turned out not to be necessary, and would have quietly changed what four tests exercise if it
had been; A3 described a step and was a slope. See
[O3](#o3-every-archived-read-is-fully-validated-inside-a-tracing-span),
[O17](#o17-handle_flushed-runs-on-every-message) and
[O34](#o34-a-record-wider-than-the-staging-buffer-defeats-intent-log-batching).

**A6 is ranked last despite being the smallest diff**, because unlike everything else here its
impact is argued rather than profiled — a span's cost is invisible to the profile that would
normally rank it, which is precisely what makes it worth filing. **A7 and A8 are behind it for the
same reason**, and the three of them now share one experiment: a capture at `level: Off` against
one at `level: Info` on the same commit settles all three at once, which is an argument for taking
it rather than for taking it three times.

**Tier B — argued, contained, waiting on its benchmark.** The profile is what orders this tier:
`write_helper` is 30.5 ms per call against roughly 350 ns for the insert it persists, so a write-path
entry is removing work from a path that is [already waiting on the
device](../performance/baseline.md#profile--where-the-time-goes). O9 and O8 lead it
because they are the exception — their cost scales with data on disk rather than with request rate,
so they get worse by existing longer rather than under load.

| # | Entry | Impact | Diff | Depends on | Tradeoff | Adjudicable today |
| --- | --- | --- | --- | --- | --- | --- |
| **B1** | [**O9**](#o9-every-intent-log-rotation-walks-the-entire-on-disk-partition-set) → [**O8**](#o8-partitions-are-read-one-at-a-time-each-with-its-own-dup-and-close) (with [**O22**](#o22-recovery-loads-the-partitions-it-scanned-one-await-at-a-time) in the same change) | Argued — but O(data on disk) | M, then L | O9 before O8 | Contained | no |
| **B2** | [**O4**](#o4-deep_size_of-is-a-recursive-walk-called-on-every-mutation) — carry a row's measured size | Argued | M | — | Contained | no — and it settles [item 22](known-issues.md#22-size-accounting-inconsistencies) either way |
| **B3** | [**O10**](#o10-serializedmapsave-snapshots-by-cloning), [**O15**](#o15-one-partition-load-costs-a-dup-and-a-close) (its first half built, by [F120](../features/one-operation-a-record-read.md)), [**O21**](#o21-a-forced-rotation-of-an-empty-intent-log-does-the-whole-rotation-anyway) — contained cleanups | Argued | S–M | — | Contained | no |
| **B4** | [**O11**](#o11-a-fresh-alignedvec-per-write-and-per-response) — reuse the serialization buffer. ~~[**O29**](#o29-a-request-body-is-zeroed-and-then-immediately-overwritten) and [**O37**](#o37-the-client-zeroes-a-response-buffer-and-immediately-overwrites-it) beside it — a buffer zeroed and overwritten, on each end of the same round trip~~ — **both done**, by [F25](../features/read-buffers-are-filled-not-zeroed.md), which built the benchmark this row said neither had | Argued; ~~O37 **asymptotic** in the row width~~ measured, both ends | M | a storage write-path bench (O11); ~~nothing (O29, O37)~~ `wire_codec/width/{request,response}/body` | None | no |
| **B5** | [**O46**](#o46-the-shared-wal-is-a-buffered-file-where-the-intent-log-was-direct-io) — the shared WAL is a buffered file where the intent log was direct I/O | Argued — a kernel copy per batch, on a path waiting on the sync | M | a capture of the replication arms on the benchmark host | Contained | no |
| **B6** | [**O47**](#o47-a-followers-fsync-may-be-waiting-for-the-leaders-rather-than-running-beside-it) — a follower's fsync may wait for the leader's rather than run beside it | Indicated — 2.1× the single-copy median at smoke scale on a shared device | S to establish | the same capture, on separate devices | Not yet known | no — one host, one device |
| **B7** | [**O48**](#o48-resolving-a-segment-scans-every-groups-whole-index) — resolving a segment scans every group's whole index | Argued — `groups × retained_entries` comparisons per handoff, off every query path | S | — | Contained | no |
| **B8** | [**O49**](#o49-one-barrier-per-group-per-bundle-rather-than-per-read) — one barrier per group per bundle rather than per read | Indicated — 590 µs a barrier at smoke scale, paid once per `Quorum` read whatever the bundle | M | `macro/cluster/reads/barrier` at a depth above one query a bundle, which no arm sends yet | Contained | no |
| **B9** | [**O50**](#o50-a-read-plan-is-built-and-cloned-per-share), [**O51**](#o51-every-committed-write-answers-with-a-forty-eight-byte-token) — a plan per share, a token per write | Argued — a clone of an `Rc` and a `Copy` per share; forty-eight bytes and one more `IoSlice` per committed write down a capable connection | S | `macro/cluster/replication/durable` for O51 | Contained | no |
| **B10** | [**O52**](#o52-a-snapshot-copies-every-record-of-the-archives-into-one-file) — a snapshot copies the archives rather than pinning them | Argued — every byte of a group's tablets read, written, synced and read again per cut, on the table's one compactor, before a byte reaches the lane | L | `macro/cluster/catchup/snapshot` on the benchmark host | Contained on the wire, not on the compactor | no |
| **B11** | [**O53**](#o53-the-assembler-keeps-a-map-of-received-chunks-and-forgets-them-on-a-restart) — the assembler keeps a map of received chunks and forgets them on a restart | Argued — a `BTreeMap` entry per chunk out of order, and a stream started over after the receiver restarts | S | `macro/cluster/catchup/snapshot` with a receiver restart, which no arm does | Contained | no |
| **B12** | [**O54**](#o54-a-scrub-reads-every-archived-partition-of-a-group-once-per-pass) — a scrub reads every archived partition of a group once per pass | Measured in shape — the background arm's `bytes` is the group's archives whole, per pass | M | `macro/cluster/background/repair` at full scale, where the archives are wider than memory | Contained | no |
| **B13** | [**O55**](#o55-a-learner-inside-the-retained-log-is-fed-a-snapshot-when-the-leaders-cached-cut-is-newer-than-its-purge-point) — a learner inside the retained log is fed a snapshot when the leader's cached cut is newer than its purge point | Argued — a whole group's archives on the bulk lane where a log tail would do | M | `macro/cluster/migration/move` at full scale, whose `bytes` is zero when the log fed the destination | Contained | no |
| **B14** | [**O56**](#o56-the-planner-recomputes-every-rule-set-on-every-look) — the planner recomputes every rule set on every look | Argued — four thousand rule derivations per open plan per look, on the control core | S | none; the rebalance arms' windows would carry a control stall as a tail | Contained | no |
| ~~**B15**~~ | ~~[**O57**](#o57-tablet-bytes-are-rescanned-from-the-whole-archive-map-on-every-report) — tablet bytes are rescanned from the whole archive map on every report~~ **done**, a counter kept by the map, measured on the lab | Argued — a pass over every archived partition of a table per report tick, on the shard | S | none; a grid cell on a persistent table under writes is where the pass would show | Contained | no |
| **B16** | [**O58**](#o58-a-rehomes-moved-records-are-copied-and-a-donors-archives-keep-the-dead-ones) — a rehome's moved records are copied, and a donor's archives keep the dead ones | Argued — a read and a write per moved record at start, and a growth's donor holding dead records until its own compaction | M | `macro/rehome/shrink`, whose `bytes` over `millis` is the copy's pace | Contained | no |
| **B17** | [**O59**](#o59-the-rehome-runs-on-one-core-and-blocks-the-start) — the rehome runs on one core and blocks the start | Argued — the start held for the whole move while every other core idles | M | `macro/rehome/shrink`, whose `millis` is the hold | Contained | no |
| **B18** | [**O60**](#o60-a-nodes-figures-ride-its-status-report-as-verbose-json) — a node's figures ride its status report as verbose JSON | Measured in shape — about 7.4 KB a report for four busy tables, one report in four, 1.6× the leader's intake at 64 members | S | none; the spike's `fanout` table prices it, and no arm drives a cluster of that size | Contained | no |
| ~~**B19**~~ | ~~[**O61**](#o61-a-fast-device-syncs-the-wal-in-batches-too-small-to-fill-a-page) — a fast device syncs the WAL in batches too small to fill a page~~ **done**, a per-node setting off by default | Measured on the lab — 6.8× the device writes of the slower hosts for the same replicated rows, 5.6k `fdatasync`s a second against 680 | S–M | the lab's insert `bench` with disk counters | Latency for wear | yes, on the lab |
| ~~**B20**~~ | ~~[**O62**](#o62-every-compaction-rewrites-the-shards-whole-archive-map) — every compaction rewrites the shard's whole archive map~~ **done**, measured and kept | Measured — 70% of a node's writes under load, in bursts that stalled its fsyncs | S | the lab's mixed `bench` with bytes per file | A longer replay at start | it was |
| ~~**B21**~~ | ~~[**O63**](#o63-leadership-never-returns-to-a-groups-placement-primary) — leadership never returns to a group's placement primary~~ **done**, measured and kept | Measured — one node leading every group cost about a sixth of the throughput and a quarter of the write p99 | S | the lab's mixed `bench`, skewed against spread | One transfer per group handed back | it was |
| **B22** | [**O64**](#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab) — a shorter failover base halves write throughput on the lab | Measured — 1 s: 20–24k rows/s and 4 s failover; 5 s: 40–46k rows/s and 16 s failover | ? | the lab's load at each base | Crash failover against write throughput | yes, on the lab |
| ~~**B23**~~ | ~~[**O65**](#o65-heartbeats-to-followers-that-just-acknowledged-replication) — heartbeats to followers that just acknowledged replication~~ **done**, no measurable effect, kept | Indicated — a heartbeat per follower per group every tenth of the base, under load and in a partition | S | the lab's load at 1 s and 5 s | none expected | yes, on the lab |
| ~~**B24**~~ | ~~[**O66**](#o66-a-partitioned-peer-floods-the-log) — a partitioned peer floods the log~~ **done**, measured and kept | Measured — 50–60k lines suppressed by journald per node in a 20 s partition | S | the lab's partition test | Per-attempt warnings need `RUST_LOG` | it was |
| ~~**B25**~~ | ~~[**O67**](#o67-ten-thousand-retained-entries-is-seconds-of-a-busy-group) — ten thousand retained entries is seconds of a busy group~~ **done**, measured and kept | Measured — a 20 s partition cost 13 snapshot installs and 70 s of refused reads on the returning node | S | the lab's partition test | About 0.5 GB more WAL a node under load | it was |
| ~~**B26**~~ | ~~[**O68**](#o68-every-archive-compaction-copies-the-shards-whole-partition-index) — every archive compaction copies the shard's whole partition index~~ **done**, contained | Measured in shape — the one Shoal frame in a page fault profile, 9.5% | S | a page fault profile under the insert bench | CPU for memory | no |

**Tier C — blocked on a design pass, not on effort.**

**One entry has now left this tier by being built**, which is the first time that has happened
here — Tier A's A3 was the only other entry on this page ever to leave a tier that way.
[F26](../features/archive-routed-requests.md) did O1, and what it cost is worth recording next to
what the tier means. "Blocked on a design pass" turned out to be accurate: the change is four
files of plumbing and one new trait, and none of it was hard, but deciding *where* the deserialize
should happen — on the coordinator, on the executing shard, or nowhere — was a decision no amount
of effort substitutes for. It also found a second copy nobody had filed, which is an argument for
scheduling the design pass rather than waiting for the entry to look actionable: the entry never
would have, because what made it worth doing was not in the entry.

| # | Entry | Impact | Diff | Depends on | Tradeoff | Adjudicable today |
| --- | --- | --- | --- | --- | --- | --- |
| ~~**C1**~~ | ~~[**O1**](#o1-queries-are-fully-deserialized-on-arrival) — zero-copy the request half~~ — **done**, by [F26](../features/archive-routed-requests.md) | Argued — and **understated**: there were two copies per write, not one | L | ~~a `wire_codec` bench~~ (built, [F10](../features/framing-and-protocol-evolution.md)); ~~the `BytesMut` reaching the shard~~ — it reaches it as a `Bytes` | Contained | it was not, and it was taken anyway — the design pass is what found the second copy |
| ~~**C1a**~~ | ~~[**O2**](#o2-every-returned-row-is-copied-at-least-twice--the-resident-half-is-done-done) + [**O18**](#o18-the-gathered-reorder-rehashes-every-rows-partition-key), together~~ — **done**, by [F27](../features/grouped-responses.md) and [F28](../features/rearchived-rows.md) | **Measured per byte** — the response codec grows ×432.8 on decode, and `r100` says the read path owns the wide end | ~~**XL**~~ **M**, twice | each other; ~~a `wire_codec` bench~~ — discharged | ~~**Major** — wire format and the client~~ **none** — the bytes are identical either way | it was, on the per-byte half — and the change turned out not to need the format break it was ranked on |
| **C3** | [**O30**](#o30-nothing-can-see-what-a-connection-costs-to-open) — the connect path is unmeasured | **Unknown, and that is the entry** | S for the workload, unknown for whatever it finds | a `connect` workload | — | **no, and that is the point** |
| **C4** | [**O31**](#o31-the-disjointness-rule-cannot-tell-a-result-from-a-saturated-workload) — a saturated workload passes the rule that decides what is real | **Measured** — four points report encryption making queries faster | S to detect, M to decide | nothing | Contained | **yes, it already has been** |
| ~~**C5**~~ | ~~[**O35**](#o35-the-per-connection-response-relay-writes-one-response-at-a-time) — one response written at a time, per connection~~ — **moved to Tier D**, its evidence reattributed to load depth | ~~Argued, indicated — a 45× p50-to-p99 spread at 512 KiB~~ | M to reorder, **XL** to interleave | [D2](../direction/framing.md), for the interleaving form only | Contained, or **Major** | it was, and it came back negative |

~~**C1a is where the largest win now sits, and it is still not actionable.**~~ **Done**, by
[F27](../features/grouped-responses.md) and [F28](../features/rearchived-rows.md), and the way it
came out is worth more than the entry was. `f22-row-size` gave O2 a measurement on exactly the half
that grows in what a caller controls, and the mixture sweep put the wide end of the axis on the read
path — so for anyone storing rows of hundreds of kilobytes, the response copies are the thing. It
sat in Tier C on a difficulty estimate that named the wire format and the client, and **the wire
format never moved**: `RowRef` and `ArchivedRef` archive as the row's own archived type, so a reply
written out of rows the server is holding is byte for byte the reply the copying path wrote. The
**Major** tradeoff this row carried for four features was an artifact of assuming the response type
had to change to stop copying, and it did not. What was actually hard was the thing the entry did
not mention: rkyv has no serializer for an archived value, and one had to be generated.

**The lesson is about the estimate, not the win.** An XL that reaches the wire format is scheduled
differently from an M that does not, and this one was mis-tiered for as long as it was filed. The
design pass is what corrected it — the same thing that happened to [O1](#o1-queries-are-fully-deserialized-on-arrival),
where the pass found a second copy nobody had filed.

**Tier D — declined, kept with the reason.** A rejected optimization is recorded, not dropped.

| Entry | Why it is not in the queue |
| --- | --- |
| [**O35**](#o35-the-per-connection-response-relay-writes-one-response-at-a-time) | Was **C5** until `f22-row-size`. The 45× p50-to-p99 spread it was ranked on is queueing, not the relay: at one outstanding query the spread collapses to 1.3–2.5× at *every* width, including the 512 KiB arm the entry quoted. The mechanism is real and the entry keeps its reasoning, but it has no evidence of its own and nothing separates reordering the relay from simply not queueing thirty-two wide queries — which [Row size](../tables/row-size.md#what-to-do-today) now recommends with a factor of eighteen behind it. Declined on evidence, not on difficulty |
| [**O20**](#o20-a-sort-key-get-reads-a-partition-it-may-not-need) | Makes the cost of a query depend on what happens to be resident, which turns a reproducible latency into a flaky one. Declined on determinism, not on difficulty |
| [**O22**](#o22-recovery-loads-the-partitions-it-scanned-one-await-at-a-time) standalone | Startup path, and the set is normally small. It rides along with O8 in **B1** or it does not happen |
| [**O16**](#o16-compaction-shares-the-shards-executor) | Not an actionable entry — it is a consequence of thread-per-core. Its effect on this page is that it **raises O8 and O9**, since it means their cost lands on query serving rather than in the background |

### Dependency edges

Rendered as a table rather than a graph. The book renders mermaid since the
[distributed chapter](../distributed/overview.md)'s diagrams; this table predates that and reads
well enough as it is.

| Edge | Why |
| --- | --- |
| ~~**O23 ⊂ O3**~~ | Not two entries. O23 was the measurement of O3, and they were taken as one — [F4](../features/validated-archives.md) |
| **O9 → O8** | O9 replaces the full scan with an incremental total. Doing O8 first means grouping reads inside a walk that O9 would have deleted |
| **O8 ↔ O22** | The same edit — group by archive file, issue concurrently — in the compactor and in recovery |
| **O2 ↔ O18** | Both change `ResponseAction::Get`. Landing either alone means paying the wire-format break twice |
| **O18 → F2** | A projection is [required to carry its table's partition key](../features/projections.md#limitations) only so that O18's rehash can work. Taking O18 lifts that |
| **O4 → item 22** | The mismatched size bases exist *because* the size is re-derived at each site. One edit closes both |
| **O16 raises O8, O9** | Compaction shares the query executor, so their cost is not background cost |
| **O3 → archive checksums** | Only for the *drop validation outright* form, which [F4](../features/validated-archives.md) did **not** take. ~~Still unpaid: validation, now once per read rather than once per query, is still the only thing between a corrupt archive and a bad pointer~~ **Paid** by [F44](../features/repair.md): a format 2 record is verified against its checksum before rkyv sees it. The unchecked form is now *possible*, and still not taken - a checksum catches a flipped byte, not a compactor that wrote a well formed archive of the wrong type, and a format 1 archive is unverified until it is rewritten |
| ~~**`wire_codec` bench → O1, O2, O18**~~ | ~~Unbuilt~~ **Discharged for O1 and O2** — built by [F10](../features/framing-and-protocol-evolution.md) and captured in `f22-row-size`. O1 has since been [done](#o1-queries-are-fully-deserialized-on-arrival). **O18 was never really on this edge**: it is *uncovered* rather than unbuilt, because no arm of `wire_codec` varies the thing it is about |
| **table-layer bench → O5, O12, O13** | Unbuilt, and until recently believed to exist — see below. [F4](../features/validated-archives.md) closed half the gap by making `MaybeLoaded` constructible, but all three of these live a layer above it in `PersistentSortedTable` |
| **write-path bench → O11, O21** | Unbuilt, and it is the layer that dominates the profile |
| ~~**a width-aware `latency_buffer` sweep → O34**~~ | **Discharged.** Built by [F22](../features/row-size-benchmarks.md) and captured in `f22-row-size`; O34 is measured and O34's *shape* was corrected by it |
| **O35 ↔ D2** | Only the interleaving form. Reordering the relay's queue needs no format change; splitting a response across frames is [D2](../direction/framing.md) |
| **row width raises ~~O1,~~ O2, O11, ~~O29~~** | ~~All four~~ ~~**Three**~~ **Two** are per-byte costs filed as constants. They do not get worse under load — they get worse per query as the caller's rows widen ([Row size](../tables/row-size.md)). **Measured for O1 and O2** by the codec width axis; still argued for O11. O1 is [done](#o1-queries-are-fully-deserialized-on-arrival), and this row is why it was worth doing: it was the entry on it whose cost landed on a single core. ~~and O29, whose instrument is broken rather than absent~~ **O29 does not belong on this row at all**: `BytesMut::zeroed` is `alloc_zeroed`, so it is a cost the allocator may decline to pay, and it is [done](#o29-a-request-body-is-zeroed-and-then-immediately-overwritten) either way |
| ~~**a stage capture → O11, O29**~~ | **Discharged for O11, and never possible for O29.** `f24-routing` is the first capture taken with the repaired instrument and all four reports join completely. The breakdown at 1 KiB, 8 KiB and 512 KiB says `reply_serialize` grows **238×** on the read path and `client_serialize` **503×** on the write path, which are O11's response buffer and its client-side bundle buffer respectively. ~~and `decode` ×199 O29's `memset`~~ — O29's buffer is read *before* the bundle's clock starts, so it falls in `net_in` beside wire time and no stage isolates it ([F25](../features/read-buffers-are-filled-not-zeroed.md)) |
| **load depth → O35** | Not a dependency so much as the reason O35 left the queue: the depth-1 ladder explains its whole observation, so nothing can rank it until something measures the relay under a bounded queue |

### Which entries a benchmark can currently adjudicate

| Entry | Benchmark that would show it |
| --- | --- |
| O1, O18, O19 | ~~none yet~~ `wire_codec/request/decode/{access,deserialize}/*` and the **width** axis beside it, `wire_codec/width/request/decode/*` ([F22](../features/row-size-benchmarks.md)). **Captured.** The per-byte half is ×61.5 for `deserialize` and ×171.5 for `access` over 64 B → 64 KiB, against a control flat to 0.25%. O18 and O19 are still uncovered: neither is about a payload width |
| O2 | `partition_sorted/maybe_loaded/get_all` and `archived/walk_all`, with `get_all` for the resident twin; and `wire_codec/width/response/*` for the per-byte half ([F22](../features/row-size-benchmarks.md)). **Captured, and it is the steepest curve in the micro layer** — decode ×432.8, encode ×72.2. The `r100` width sweep adds which half of the mixture pays it: the read path owns everything past ~64 KiB |
| ~~O3, O23~~ | `partition_sorted/maybe_loaded/get_key` and `exists_key`, against `codec/access` — **settled**, see [F4](../features/validated-archives.md#performance) |
| O5, O12 | **none yet** — an isolated bench over `PersistentSortedTable::get` is still unbuilt ([TODOs](todos.md#benchmark-coverage-the-harness-does-not-have)). `maybe_loaded/*` reaches `MaybeLoaded`, one layer below where both live |
| O13 | ~~none yet~~ `macro/fanout/{resident,evicted}/n` since [F8](../features/purpose-built-workloads.md) — **the question, not the isolated cost**. See the note below |
| O20 | ~~none yet — a `routing` bench over `Ring::find_shard` and `split_by_shard` is unbuilt~~ — **built** ([F24](../features/routing-benchmarks.md)), and it does **not** adjudicate this entry. `routing/*` prices the placement decision; O20 is about *residency* — whether a get should read a partition it may not need — and nothing in the micro layer varies what happens to be resident. The bench this row asked for exists and the entry it was asked for is still uncovered, which is worth recording as a case of a benchmark being specified by the code it touches rather than by the question it answers |
| O11, ~~O29~~, ~~O37~~ | the `r0` width sweep against the `r100` one — **captured**, and it says the write path owns the axis below ~64 KiB, which is where O11's three passes live. ~~The per-stage breakdown that would say *which* stage they are in ran at three widths and **joined zero queries at all three**~~ — **it has now run and answered**, in `f24-routing`, the first capture taken after [Resolved #76](resolved/stage-join.md). `client_serialize` ×503 and `reply_serialize` ×238 are O11's two buffers. ~~`decode` ×199 contains O29's `memset`~~ — **it does not**, and could not: that buffer is filled before the bundle's clock starts. **O29 and O37 have their own instrument now** — `wire_codec/width/{request,response}/body/{zeroed,uninit}`, which runs the old shape and the new one in one build ([F25](../features/read-buffers-are-filled-not-zeroed.md)) — and both entries are done. This was for one release the only row in this table where a capture made things worse than an absent benchmark, because an absent one is honest; the correction above is the second half of that lesson, since a capture that joins can still be read to say something it does not |
| O21 | `hotpath` `fs::commit` and `stream::prep` only; no micro-benchmark of the write path exists, though [F8](../features/purpose-built-workloads.md) built the standalone binary that would host one |
| ~~O34~~ | ~~**none yet**~~ ~~built and not yet captured~~ `macro/conf/storage/latency_buffer/r50/w8192/*` and `.../w65536/*` ([F22](../features/row-size-benchmarks.md)). **Captured, and it settled the entry**: 1.22× at 64 KiB rows on disjoint intervals against 1.06× at the reference cell — and it corrected the shape, because crossing the buffer threshold at 8 KiB bought nothing while 8–32 records per buffer bought 6%. The sweep that could not see this now can, and the entry it settled has been **acted on** ([F23](../features/self-sizing-staging-buffer.md)). The same arms re-judged the fix, in `f23-staging-buffer`, and the `w65536` rungs **converged**: 1.225× of spread became 1.010×, which is a sweep whose knob is now a floor under a ceiling having less to say the wider the rows get — the shape of a knob that stopped mattering |
| O35 | ~~none~~ ~~built, not captured~~ `macro/grid/depth/1/<width>` against the `r50` width sweep ([F22](../features/row-size-benchmarks.md)). **Captured, and it came back negative.** The test was the entry's own: a p99 that collapses at one outstanding query is a queue rather than a cost inside the relay. It collapses — 1.3–2.5× at every width against 37–52× at depth 32 — so the entry lost its evidence and left the queue |
| O36 | none — `macro/fanout/*` drives the many-partition path this entry is *not* about, and no isolated bench reaches a single-partition get. The table-layer bench O5, O12 and O13 want would cover it |
| ~~O37~~ | ~~none — the client's socket read is measured by nothing. `wire_codec/width/response/decode` starts *after* the read this entry is about.~~ — **built**, as `wire_codec/width/response/body/{zeroed,uninit}` ([F25](../features/read-buffers-are-filled-not-zeroed.md)), and the entry is **done**. A stage capture would place it in `net_in`, which is still true and is why one was never going to settle it: so is the whole of the server's socket read |
| O38 | `macro/transport/stream/*` against `macro/transport/stream_unordered/*` — the unordered mode never reorders, so the pair is a control with the entry's cost in one half and not the other. **Built and captured**, but at one row width, so the per-byte half of it is invisible |
| O28, O30, O31 | ~~none — **the client is not instrumented at all**. No `tracing` span, no `hotpath` scope, and no workload that isolates it.~~ **False against a committed artifact.** `shoal-client/src/client.rs` carries eleven instrumentation sites — `#[instrument]` spans on `connect_to`, `send`, `send_stamped` and `ShoalQueryStream::send`, and `hotpath::measure` on those plus `track_response`, `read_frame` and both `next` implementations — and `f22-row-size.hotpath.json` reports five of them: `read_frame` at 200,022 calls, `next` at 200,001, `send` at 2,001, `connect_to` at 35, `track_response` at **2**. What is still missing is the **subtraction**: a macro sample bounds client and server together and nothing separates them, and no workload isolates the connect path (O30). **And the scope O28 asked for does not measure O28**: `track_response` is entered twice in a run of 200,000 queries, so whatever it wraps, it is not the per-query double guard the entry is about |

> This table previously claimed that `partition_sorted/insert` and `get_key` adjudicated **O5**,
> and that `seek_bytes/*` adjudicated **O12** and **O13**. ~~It was wrong about all three.~~ Those
> benchmarks construct a `SortedPartition` directly (`shoal/benches/partitions.rs`) and never build
> a `PersistentSortedTable`, which is where all three entries live — the `partitions` and `blocked`
> maps, `to_blocked`, and the quadratic loop are in `persistent/sorted.rs` and
> `shared/queries/sorted.rs`, none of which those benches reach. `seek_bytes/new` measures
> `SeekBytes::new` and nothing around it. **The three cheapest entries on this page were the three
> whose evidence was furthest away**, and the table said the opposite.

> **What F8 changed, and what it did not.**
> [F8](../features/purpose-built-workloads.md) built `macro/fanout/{resident,evicted}/n` over
> *n* ∈ {1, 2, 4, 16, 64, 256} — a get over *n* partition keys against both a resident table and
> one that has to be read. That is the shape [TODOs](todos.md) asked for, and it needed no glommio
> executor inside criterion, because driving `PersistentSortedTable::get` through a live server
> does not need one.
>
> It adjudicates **O13's question** and not **O13's cost**. Every sample includes the wire, the
> routing, `split_by_shard`, the response merge and the client, so the absolute number is not a
> cost of the table method. But a quadratic per-partition term bends the curve against a flat
> control at *n* = 1 whatever constant overhead sits on top of it.
>
> **The first full capture does not settle it.** `f8-powersave` puts the median below the chord at
> both *n* = 16 and *n* = 64, which is the right sign, but the marginal cost between adjacent
> points — 0.91, 0.64, 1.11, 0.83, 1.12 µs — does not rise monotonically and is consistent with
> noise around a straight line. A smoke-scale run had suggested a clean rise off forty samples and
> did not survive contact with the full one, which is worth recording as a caution about reading
> smoke runs rather than as evidence about O13.
>
> **O5 and O12 are not touched**: neither is about how cost scales with the partition count, so
> neither shows up as a bend.
>
> The isolated, criterion-sampled bench remains unbuilt and remains the thing that would settle
> all three.

---

## Read path

### ~~O1. Queries are fully deserialized on arrival~~

**Done**, by [F26](../features/archive-routed-requests.md). The coordinator no longer deserializes
anything: it validates the bundle once, reads the partition keys and limit straight out of the
archive, and hands each destination shard the buffer itself as a shared `Bytes`. The shard that
answers a query is the shard that turns it back into one.

**The entry understated it, and the way it did is the useful part.** O1 counted one copy per
query — the deserialize. There were two on every write, because `split_by_shard`'s write arms end
in `self.clone()` and `SortedQuery::Insert { key, row }` carries the row. So an insert's row was
deserialized out of the archive and then deep-copied again, both times on core 0. An entry filed
by reading one function found the cost that function pays and missed the one the function it calls
pays, which is an argument for walking the callees when filing rather than for filing less.

**Two things it got right that were worth the wait.** The obstacle it named was the real one —
`ServerMsg::Query` had to give up its owned `QueryKinds`, and that is exactly what the fix does.
And the machinery it pointed at, `ShoalDatabase::unarchive_queries`, was indeed what this needed;
it had been sitting callerless since it was written. It became `unsafe fn` on the way, because as
a *safe* fn wrapping `rkyv::access_unchecked` it let any caller hand it any bytes — which is a
defect the entry did not notice while quoting it as ready to use.

**Not closed: the table layer still executes against owned queries.** The remaining copy is one per
query rather than two, and it is paid on the shard that reads the row rather than on the
coordinator, but it is still paid. Executing against `&ArchivedQueryKinds` reaches every table
method and the derive's filter and update codegen, and it is filed in
[TODOs](todos.md) rather than kept here, because it is a different entry: `XL` rather than `L`, and
with a lower ceiling than it looks, since an insert needs an owned row for its partition map
whatever the query layer does.

**Sequencing note, now moot.** The entry asked for this to be taken together with
[D2](../direction/framing.md) to avoid two visits to the same code. D2 landed as
[F10](../features/framing-and-protocol-evolution.md) before this was picked up, so the two visits
happened anyway — and the second one was cheaper for it, since F10 is what built the `wire_codec`
benchmark this was blocked on.

The original entry follows.

| | |
| --- | --- |
| **Rank** | ~~**C1** — blocked on a design pass~~ — **done**, [F26](../features/archive-routed-requests.md) |
| **Impact** | **Measured per byte**, argued per call — `request/decode/deserialize` grows ×61.5 and `request/decode/access` ×171.5 over 64 B → 64 KiB, against a header-decode control flat to a quarter of a percent. The request half is the smaller one: the response's decode grows ×432.8 |
| **Difficulty** | L — the `BytesMut` has to survive as far as the shard that executes the query |
| **Depends on** | ~~A `wire_codec` bench~~ (built, [F10](../features/framing-and-protocol-evolution.md)); `ServerMsg::Query` giving up its owned `QueryKinds` |
| **Blocks** | nothing |
| **Tradeoff** | Contained — a lifetime on the query type, not a format change |
| **Benchmark** | `wire_codec/request/decode`, which runs the validated `access`, the unchecked `access_unchecked` and the full `deserialize` as three separate functions at 1, 10 and 100 queries per bundle — so the gap between the second and the third is what this entry is worth. **Plus `wire_codec/width/request/decode/*`** ([F22](../features/row-size-benchmarks.md)), which sweeps the same three at five row widths and is the half that grows in what the caller controls. Both captured in `f22-row-size` |

```rust
// load our arhived query from buffer
let archived = Queries::access(&data)?;
// deserialize our queries
let queries = <Queries<D::ClientType> as RkyvSupport>::deserialize(archived)?;
```

`shard.rs:1182-1184`, `Shard::handle_client`

Every `String`, `Vec`, and filter in every query of the bundle is allocated and copied out of a
buffer that already holds them in a readable layout. This branch is named for making *responses*
zero-copy; the request half was not converted.

The machinery for it already exists and is unused: `ShoalDatabase::unarchive_queries`
(`shoal-core/src/server/database.rs:98`, `ShoalDatabase::unarchive_queries`) returns `&ArchivedQueries` via `access_unchecked` and has no callers.

The obstacle is real, though, and worth stating: `send_to_shard` consumes the queries by value
(`shard.rs:1061`, `Shard::send_to_shard`) and `ServerMsg::Query` carries an owned `QueryKinds` (`messages.rs:159-163`), so
this is not a call-site swap. It needs the archived form to survive as far as the shard that
executes the query, which means the `BytesMut` has to travel with it.

**Sequencing, filed while writing [Direction](../direction/overview.md).** This is not a format
change and does not need [D2](../direction/framing.md) — but D2 *is* a format change, it rewrites
both read loops, and ~~it is the point at which the server's `BytesMut::zeroed(len)` gets replaced
anyway~~ — the zeroing was replaced ahead of it, by
[F25](../features/read-buffers-are-filled-not-zeroed.md), so what D2 now finds there is a
`RequestBody` whose invariant it has to keep rather than a buffer to fix. Taking this in the same
pass still costs one visit to that code instead of two, and the `wire_codec` bench both are blocked
on is the same bench.

**This cost is proportional to the row, not constant.** It was filed against the reference cell's
1 KiB rows, where it is small. The [row-size sweep](../performance/row-size.md) is where it stops
being small: a 4 MiB bundle is 4 MiB of `String` and `Vec` allocated and copied out of
a buffer that already holds them. Its *Impact* grade should be read as **Asymptotic** in the row width —
a quantity the caller chooses — rather than as the *Argued* constant above. See
[Row size and what it costs](../tables/row-size.md#the-payload-is-walked-about-six-times-per-round-trip).

### O2. Every returned row is copied at least twice — ~~*the resident half is done*~~ **done**

**Done**, in two halves: the resident one by [F27](../features/grouped-responses.md), and the
archived one by [F28](../features/rearchived-rows.md), which was filed as
[O40](#o40-a-row-read-out-of-an-archive-is-materialized-before-it-is-re-serialized) in between so
that the open remainder could not be rediscovered inside this entry.

**The resident half.** A get that named no projection and read only resident partitions no longer
copies its rows at all on the way out. The reply is serialized from the rows the partition is
holding, through a `RowRef<'a, T>` whose archived type *is* the row's archived type, so the bytes
are identical to what the copying path wrote and no wire version had to move for it. The claim is
a count and is tested as one: a resident unprojected scan of three rows copies **zero** of them,
against a projected control that builds all three.

**The archived half.** A partition read from disk stays an archive, and what it holds is
`Archived<T>`. rkyv has no `Serialize` for an archived value back into its own layout, so
[F28](../features/rearchived-rows.md) generates one — a mirror per row type, emitted field by field
against rkyv's own archived struct, with a **per-field** fallback for a type the derive cannot see
inside so that no schema is refused and no row loses more than that one field costs. The same count
test says so: an archived unprojected scan of three rows now builds **zero** of them, against a
projected control that builds all three, on both tables.

**What is still copied, and it is no longer this entry.** A projection is a strict subset of its
row and has to be built whatever its partition is held as. A share of a split get is owed to
another shard, which has to merge it. And a get that *parked* on a disk read copies on its replay —
filed as [O42](#o42-a-get-replayed-after-a-disk-read-copies-rows-its-partition-is-now-holding),
because it is a property of parking rather than of archives.

~~**And one thing neither half reached, which is the finding that matters.**~~ **Reached now**, by
[Resolved #80](resolved/never-flushed-partitions.md). The sorted table did not take the new path
at all, because `check_disk` was set when a partition was created and nothing cleared it when
storage reported there was never anything on disk to read. So a sorted partition that had only
ever been written to was judged non-resident for ever, and refused the borrowing path correctly
but permanently. Found by probing the path rather than by reasoning about it — both paths answer
identically, so nothing failed. **The unsorted table always took it**, throughout its integration
suite.

**The archived half is now measured**, by `f28-rearchive`: `partition_sorted/maybe_loaded/get_all`
fell **95.5%** at 1024 rows and **95.8%** at 4096, against a `build_all` arm in the same build that
holds the old behaviour at 53.55 µs and 215.1 µs. Writing the reply out of an archive rather than
copying the rows first is **5.8×** (38.9 µs against 224.2 µs at 4096 rows). Together that is about
**9× on the whole answer path** of a wide archived get. It cost something, and that is filed as
[O43](#o43-a-borrowed-row-costs-a-discriminant-it-usually-does-not-need) rather than netted off
here.

**The resident half is still not measured end to end.** The count
tests are over `RowSink` rather than over a running server, and no *macro* capture has been taken
since the sorted table became eligible — which is precisely the capture
[F27](../features/grouped-responses.md) said would show the feature working, and predicted would
show nothing while item 80 stood.

The rest of this entry stands as written, and describes what the copies *were* — the difficulty and
tradeoff rows in particular, which are what the change proved wrong:


| | |
| --- | --- |
| **Rank** | **C1a**, with O18 — the largest read-path win, and the largest change |
| **Impact** | **Measured per byte**, argued per row — the response codec grows ×432.8 on decode and ×72.2 on encode over 64 B → 64 KiB. And the `r100` sweep says the read path owns the wide end of the axis: at 4 MiB a pure-read mixture runs at 0.3% of its own 64 B rate against a pure write's 0.9% |
| **Difficulty** | ~~**XL** — `ResponseAction::Get` reaches the wire format and the client~~ **M, twice.** `ResponseAction::Get` never had to change: a reply written out of borrowed or archived rows archives as the same bytes as one written out of owned rows |
| **Depends on** | O18, which changes the same shape; ~~a `wire_codec` bench~~ (built, [F10](../features/framing-and-protocol-evolution.md)) |
| **Blocks** | O18 |
| **Tradeoff** | ~~**Major** — a wire-format break, and `FromShoal::retrieve`'s signature with it~~ **None.** There was no format break: the bytes are identical either way, `PROTOCOL_VERSION` did not move, and `FromShoal::retrieve` kept its signature. F10's version byte and schema fingerprint went unused by this after all |
| **Benchmark** | `partition_sorted/archived/walk_all` and `get_all` bound the copies; `wire_codec/response/encode` and `/decode` at 16, 256, 1024 and 4096 rows are the wire half. **Plus `wire_codec/width/response/*`** at five row widths ([F22](../features/row-size-benchmarks.md)), which is the per-byte half and the steepest curve in the micro layer. Captured in `f22-row-size` |

`SortedPartition::get` copies out of the `BTreeMap`:

```rust
found.push(P::from_row(row));
```

`tables/partitions.rs:585`, and `:293` for unsorted

The archive path is worse — it materializes an owned value from bytes per row
(`tables/partitions.rs:1092`, and `:363` for unsorted):

```rust
found.push(P::from_archived(archived));
```

Then `Shard::reply` serializes the whole `Vec<T>` back into bytes (`shard.rs:1212`, `rkyv::to_bytes`). A read served
from an `Accessible` partition therefore goes **bytes → owned rows → bytes**, and a read served
from memory goes **rows → cloned rows → bytes**.

~~The response type is what forces it: `ResponseAction::Get(Option<Vec<T>>)`
(`shoal-proto/src/shared/responses.rs:89`) can only hold owned rows.~~ **This was the mistake in
the entry, and it is what mis-tiered it.** `Vec<T>` is generic, and `T` does not have to be an
owned row: [F27](../features/grouped-responses.md) instantiates it at `RowRef<'a, R>` and
[F28](../features/rearchived-rows.md) at the archived variant of the same enum, both of which
archive as `<R as Archive>::Archived`. Nothing about the response type had to change, and so
nothing about the wire format did.

**Narrowed, not closed, by [F2](../features/projections.md).** The two lines above used to read
`found.push(row.clone())` and `let loaded = R::deserialize(row).unwrap(); found.push(loaded)`;
they are now `P::from_row` and `P::from_archived`, where `P` is what the get asked to be answered
with. A get that named a projection copies only the fields that projection declared, so the archive
path materializes a smaller owned value and the wire carries less. A get that named none still
copies the whole row twice — the identity projection is exactly the two lines above — so the shape
of this entry is unchanged and only its magnitude moved.

~~**A third entry now wants the same flag day.**~~ **There was no flag day.** This and O18 were
believed to have to land together to avoid paying the wire break twice
([dependency edges](#dependency-edges)); they did land together, in
[F27](../features/grouped-responses.md), and broke nothing, so
[D2](../direction/framing.md) is now the *first* break rather than a third. The paragraph is kept
because the reasoning is right about breaks that are real — it was simply applied to one that was
not. As written: **The expensive part of a wire-format change is
the flag day, and it is paid per break rather than per field** — so if D2 is taken first and these
two later, the cost is two. Whether they can realistically be designed together is the open
question, since D2 is a header change and these are a payload change; but the sequencing decision
should be made deliberately rather than by whichever is picked up first.

**This cost is proportional to the row, not constant.** It was filed against the reference cell's
1 KiB rows, where it is small. The [row-size sweep](../performance/row-size.md) is where it stops
being small: two copies of a 4 MiB row is 8 MiB of memory traffic for one get, and the
archive path's `from_archived` materialization is a third.

**The stage layer now names both copies, and they are the two fastest-growing stages on the read
path.** `f24-routing` profiled `macro/grid/unsorted/r50/{1024,8192,524288}` with a join on every
record, which is what [Resolved #76](resolved/stage-join.md) had to be fixed for. Over 1 KiB →
512 KiB, at the p50 of a get:

| Stage | 1 KiB | 512 KiB | Growth | Share of a 512 KiB get |
| --- | ---: | ---: | ---: | ---: |
| `reply_serialize` — `rkyv::to_bytes(&response)` | 364 ns | 86.8 µs | **×238** | 21.6% |
| `execute` — `P::from_row` out of the partition | 826 ns | 54.1 µs | **×65** | 13.5% |
| `socket_write` | 12.3 µs | 127.0 µs | ×10.3 | 31.6% |
| `net_out` | 4.56 µs | 50.8 µs | ×11.1 | 12.6% |
| *whole get* | 36.9 µs | 401.8 µs | ×10.9 | — |

**This entry's two copies are 35% of a wide get**, and they are the only two stages growing faster
than the query around them — everything else on the path grows at or below the ×10.9 the whole query
does. That is a sharper statement than the `r100` sweep could make: that said the read path owns the
wide end of the axis, and this says which two stages inside it, by name, with the rest of the
pipeline as the control. Its *Impact* grade should be read as **Asymptotic** in the row width —
a quantity the caller chooses — rather than as the *Argued* constant above. See
[Row size and what it costs](../tables/row-size.md#the-payload-is-walked-about-six-times-per-round-trip).

### ~~O3. Every archived read is fully validated, inside a tracing span~~

**Done, in the narrow form**, by [F4](../features/validated-archives.md), together with
[O23](#o23-a-cold-get-of-one-row-validates-the-whole-partition-it-landed-in) as this page said it
had to be. A partition read off disk is validated once, when the read lands, and held as a
`ValidatedArchive` that carries that fact; the thirteen `access(read).unwrap()` sites on the
`MaybeLoaded::Accessible` arms became `read.archived()`. `SeekBytes` took the same treatment, so a
sort key is validated once per query rather than once per key per partition.

The original entry read:

> **Take the narrow form first.** Validating once when a partition is read off disk, and holding the
> validated form, removes the per-get walk without a single `unsafe` and without depending on
> checksums — and O23's measurement says that is where nearly all of the cost is. `access_unchecked`
> is a second, separable decision; the precedent for it already exists in the tree, at
> `shared/traits.rs` for queries and in `sorted.rs` for intent replay, both on data this process
> wrote moments earlier.
>
> ```rust
> #[instrument(name = "RkyvSupport::access", skip_all, err(Debug))]
> fn access(raw: &[u8]) -> Result<&<Self as Archive>::Archived, rkyv::rancor::Error> {
>     rkyv::access::<<Self as Archive>::Archived, rkyv::rancor::Error>(raw)
> }
> ```
>
> `shared/traits.rs:62-77`
>
> `rkyv::access` is the *checked* entry point: it runs `bytecheck` over the whole buffer, O(bytes),
> every call. The `#[instrument]` adds a span creation and enter on top of that.
>
> The bytes were written by this process and read back from a file it owns, so they are validated
> once per read from disk at best and once per *query* as it stands. Validating at load and using
> `access_unchecked` afterwards removes both the walk and the span from the read path — at the cost
> of making [archive checksums](todos.md#archive-checksums) matter more, since validation is
> currently the only thing standing between a corrupt archive and a bad pointer.
>
> This is the entry with the best ratio of cost removed to code changed.

It was right about the ranking, right that the narrow form was the one to take, and right that
`access_unchecked` everywhere is a separate decision — which is **still not taken**. It ~~still
depends on~~ no longer waits on [archive checksums](todos.md#archive-checksums), which
[F44](../features/repair.md) delivered: a format 2 record is verified against its checksum
before rkyv sees it, so the validation is now the *second* check on the bytes rather than the
only one. What still argues against dropping it is that a checksum proves the bytes are the
ones the compactor wrote, not that the compactor wrote a valid archive of this row type, and
that every format 1 archive is unverified until archive compaction rewrites it.

**It was wrong about `unsafe`, and that is the part worth keeping.** ~~"without a single
`unsafe`"~~ — there is no such form. `rkyv::access` returns a reference *into* the buffer, so
holding "the validated form" means holding a reference beside the thing it borrows from, which is
self-referential. What is holdable is the *fact* that validation happened, and reading through that
fact is one `unsafe` block behind a private constructor. Every byte is still validated, exactly
once, by the same validator. See
[F4's invariants](../features/validated-archives.md#invariants-to-uphold) before touching it.

The `#[instrument]` was kept rather than removed: it now fires once per partition load, which is
where a span belongs.

### O5. The hottest maps use SipHash

| | |
| --- | --- |
| **Rank** | **A5** — near-free, do it whenever the surrounding code is open |
| **Impact** | Argued — one SipHash per query at minimum, more on a parked one |
| **Difficulty** | S — a type annotation |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | None |
| **Benchmark** | none — the `partition_sorted/*` benches never build a `PersistentSortedTable` |

`partitions: HashMap<u64, MaybeLoaded<..>>` and `blocked: HashMap<u64, ..>`
(`.../persistent/sorted.rs:142`, `:167`, `:238`, `:257`; `.../persistent/unsorted.rs:95`, `:113`,
`:184`, `:201`) all use std's default hasher. `partitions` is looked up at least once per query.

Two more have joined them since this was filed, and both are keyed by ~~`(Uuid, usize)`~~ a
`ParkKey` - client, id, index and attempt since
[Resolved #123](resolved/parked-get-key.md) - rather than by a `u64`: ~~sixteen~~ forty-eight
bytes of SipHash instead of eight: `PendingGets::parked`
(`.../tables/persistent.rs`) and `pending_exists` (`.../persistent/sorted.rs`). They are
touched only by a query that parked on a disk read, which is the path that is already waiting, so
they are the less interesting half of the entry.

The LRU sitting beside them already uses `BuildHasherDefault<GxHasher>` (`shard.rs:320`, and `shared/traits.rs:307`),
and `gxhash` is already a dependency, so this is a type annotation rather than a change.

Since F87 `partitions` is a `ResidentPartitions`, a map a tablet (`tables/resident.rs`), and each
tablet's map still uses std's hasher. The keys are hashes already, but they are not a reason to
reach for an identity hasher: every key of one tablet shares its top twelve bits, which is where
hashbrown takes its control byte from, so an identity or a rotation would put a tablet's keys under
one tag. `GxHasher`, as the LRU has, is still the annotation.

### O12. `to_blocked` clones the whole filter set per blocked partition

| | |
| --- | --- |
| **Rank** | **A4**, beside O13 — same function, same loop |
| **Impact** | Argued — one filter-set clone per cold partition named |
| **Difficulty** | S — an `Rc`/`Arc` around the filters, or a borrowed narrowed query |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | None |
| **Benchmark** | none — needs the table-layer bench |

`SortedGet::to_blocked` (`shared/queries/sorted.rs:445`) calls `for_partitions` (`:422-443`), which
clones `sort_select` and `filters` into the narrowed query, and `get` calls it once for every
partition that has to be read from disk (`.../persistent/sorted.rs:617`). A get across 100 cold
partitions makes 100 copies of the same filters and the same selection, all of which are then held
in `blocked` until the loads land. A range clones two bounds rather than a key set, so it is the
cheaper of the two selections to copy — but the filters dominate either way.

`SortedExists::to_blocked` (`:516`, calling `:496-514`) and the unsorted twin
(`shared/queries/unsorted.rs:165`, `:207`) have the same shape, reached from
`.../persistent/sorted.rs:713` and `.../persistent/unsorted.rs:570`.

### O13. A multi-partition get is quadratic in the partitions it names

| | |
| --- | --- |
| **Rank** | **A4** — the best difficulty-to-argument ratio on the page |
| **Impact** | **Asymptotic** — O(n²) in *n*, the partition count a caller sets directly |
| **Difficulty** | S — a rank index on `PendingGet`, and a running count in `filled_before` |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | None |
| **Benchmark** | ~~none — needs the table-layer bench~~ `macro/fanout/{resident,evicted}/n` since [F8](../features/purpose-built-workloads.md), which shows the curve bend but not the isolated cost — see [the table above](#which-entries-a-benchmark-can-currently-adjudicate) |

**This entry was filed against code that has since moved, and it came out broader.** It used to
read — and, like every quoted original on this page, **its line numbers point at code that is no
longer there**:

> ### ~~O13. `blocked.retain(..)` runs inside the per-key loop~~
>
> `.../persistent/sorted.rs:424`, `:473`, `:630` — a linear scan of the blocked list per partition
> key, making a get over *n* keys O(n²). Small *n* today, but *n* is the number of partitions a
> single query names, which is the one thing a caller controls directly.

Two of those three sites are gone, and the surviving `blocked.retain` is not on the get path at
all. What is left, and what was found in its place:

| Where | Per key | Reached by |
| --- | --- | --- |
| `PendingGet::rank` — `keys.iter().position(..)` (`.../tables/persistent.rs:58`) | scan of every key the get named | **every** multi-partition get |
| `PendingGet::filled_before` — walks `slots[..rank]` (`:93`) | scan of every slot before this one | every multi-partition get **with a limit** |
| `blocked.retain(..)` (`.../persistent/sorted.rs:727`) | scan of the keys still outstanding | an `exists` replayed after a disk read |

So the quadratic term did not go away when [items 26 and 39](resolved/partition-order.md)
introduced slot-based gathering — it moved from the blocked list into `PendingGet`, and **widened
in the process**. The old shape cost O(n²) only on a get whose partitions were being read from
disk; `rank` is called once per key on the resident path too, so every multi-partition get pays it
now, and a limited get pays `filled_before` on top. That is worse than what was filed, on a path
that is not waiting for anything.

It is still small *n* today and still O(n²), and *n* is still the one quantity a caller controls
directly — which is the argument for fixing it while it is cheap. `PendingGet` already owns `keys`
and `slots` side by side, so a `HashMap<u64, usize>` built once in `new` answers `rank` in O(1),
and carrying a running row count answers `filled_before` the same way.

**Filed on the way:** the entry's original claim is now false as written, which is why the old text
is kept above rather than edited in place. Nothing here is a defect — a quadratic term in a small
*n* is a cost, not a bug — so it stays on this page rather than moving to
[Known Issues](known-issues.md).

### ~~O19. A wanted sort key is re-archived for every archived partition it is sought in~~

**Done**, by [F1](../features/sort-key-ranges.md). `MaybeLoaded::seek_archived` used to serialize
and validate the key it was looking for once per key per partition, so a get naming *k* sort keys
across *p* archived partitions did that *k × p* times for *k* distinct values.

`SeekBytes` (`.../tables/partitions.rs`) now owns the archived forms of a query's keys and bounds,
and `PersistentSortedTable::get`/`exists` build it **at most once per execution** — lazily, on the
first partition actually being read in place, so a get every one of whose partitions is resident
still builds none of it. Serialization is down from *k × p* to *k*; validation is still per
archived partition, because holding a `&Archived<Sort>` across the loop would need a
self-referential struct.

Kept here rather than deleted because the shape it settled on is the one an equivalent change
elsewhere should copy: bytes in the carrier, references built at the point of use.

### O20. A sort-key get reads a partition it may not need

| | |
| --- | --- |
| **Rank** | **Tier D — declined.** Not on difficulty; on determinism |
| **Impact** | Argued — saves a whole archive read, but only when the *entire* key set hits in memory |
| **Difficulty** | M |
| **Depends on** | a `routing` bench; the invariant in [item 8](resolved/sort-keys.md#invariants-to-uphold) |
| **Blocks** | nothing |
| **Tradeoff** | **Major** — makes query cost depend on residency, turning a reproducible latency into a flaky one |
| **Benchmark** | none — `routing` is unbuilt, and no bench varies residency |

An in-memory row or tombstone shadows whatever an archive holds for the same key
(`SortedPartition::merge_from_disk`), so a get whose named sort keys are *all* resolved in memory —
as live rows or as tombstones — could answer without reading the partition at all, even with
`check_disk` set. The read is currently unconditional, which is deliberate: see the invariant in
[item 8](resolved/sort-keys.md#invariants-to-uphold). Recorded here rather than lost, with two
warnings attached. It pays only when the *whole* key set hits, and it makes the cost of a query
depend on what happens to be resident, which is the kind of thing that turns a reproducible
latency into a flaky one.

**It does not extend to a range.** A key set can in principle be checked off; a range cannot,
because there is no way to know that the rows in memory are *all* of the rows in that span without
reading the archive that might hold more. Ranges made this optimization strictly narrower rather
than more attractive — see [F1](../features/sort-key-ranges.md#invariants-to-uphold).

---

### ~~O18. The gathered reorder rehashes every row's partition key~~

**Done**, by [F27](../features/grouped-responses.md), together with the resident half of
[O2](#o2-every-returned-row-is-copied-at-least-twice--the-resident-half-is-done-done) as this page said it had to be. A get's
answer carries an index of the partitions its rows came from, so the shard collecting the shares
of a split get ranks the **groups** — as many lookups as the query named partitions — instead of
hashing every row's partition key. Nothing is hashed there at all now.

The shape the entry proposed is not quite the shape that was built, and the difference is the
whole reason it was cheap. It suggested a share carry `Vec<(u64, Vec<T>)>`; what landed is a flat
`Vec<T>` beside a `Vec<RowGroup>` index. Both carry the same information. The nested one would have
changed the payload a client walks from an `ArchivedVec<Archived<T>>` into a vec of vecs, rewriting
all sixty-eight `access::<T>()` call sites in the tree; the flat one leaves every one of them
untouched, because `ShoalResponse::access` reaches into `.rows` and keeps its signature. The entry
graded itself **XL** on the strength of that rewrite. It was **M**.

**What it unblocked is worth more than what it saved.** `order_by_partitions` no longer needs
`T: PartitionKeySupport`, and that bound was the only reason a projection had to carry its table's
partition key — a constraint [F2](../features/projections.md) recorded as a limitation and could
not lift on its own. A projection of a title alone is now expressible.

The original entry read:


| | |
| --- | --- |
| **Rank** | **C1a**, with O2 — the same shape, so the same change |
| **Impact** | Argued — one gxhash per row of a split query |
| **Difficulty** | **XL** — a grouped share is a wire-format change |
| **Depends on** | O2; ~~a `wire_codec` bench~~ — built ([F10](../features/framing-and-protocol-evolution.md)), and it does not reach this entry. See the Benchmark row |
| **Blocks** | [F2](../features/projections.md#limitations) — a projection must carry its partition key only because of this |
| **Tradeoff** | **Major** — wire format, shared with O2 |
| **Benchmark** | ~~none — `wire_codec` is unbuilt~~ ~~**Uncovered, not unbuilt.**~~ **Built**, by [F27](../features/grouped-responses.md): `wire_codec/response/gather/{hash,groups}` runs both shapes in one build over 1024 rows swept across 1 → 256 partitions. It says the entry had the axis right and the direction wrong — the win is ×15.8 at four partitions and ×1.16 at 256, because ranking groups only beats ranking rows while there are many fewer groups than rows. Captured as `f27-grouped-responses` and again as `f27-row-sink`, which agree to within a percent on these arms |

`ResponseAction::order_by_partitions` (`shared/responses.rs:109`) sorts the merged rows of a split
query by where their partition was named. A `Response` carries rows and nothing else, so the only
way to ask a row which partition it came from is to hash its partition key again:

```rust
rows.sort_by_cached_key(|row| ranks.get(&row.get_partition_key()).copied().unwrap_or(usize::MAX));
```

`sort_by_cached_key` keeps that to one hash per row rather than one per comparison, and for a
string partition key a gxhash over the field is cheap next to the row clone that already happened
to get here. Still, the information was known and thrown away: every shard produced its rows
grouped by partition already, in the right relative order.

Two ways out, both bigger than they look. A k-way merge over the shares by partition rank would
be `O(n)` with no hashing, but `merge` is called pairwise as shares arrive rather than once at
the end, so it means buffering the shares and merging them together. Alternatively a share could
carry its rows grouped — `Vec<(u64, Vec<T>)>` rather than `Vec<T>` — which removes the question
entirely, at the cost of a wire format change that lands on the same `ResponseAction::Get` shape
**O2** wants to change for a different reason. Worth doing with O2 rather than before it.

[F2](../features/projections.md) added a second argument for the grouped share. A projection has to
carry its table's partition key for no reason other than this rehash, which is a real constraint on
what a projection is allowed to leave out — a projection of a title alone is not expressible. Taking
this entry would lift that requirement as well as removing the hash.

---

### ~~O23. A cold get of one row validates the whole partition it landed in~~

**Done**, by [F4](../features/validated-archives.md), as one unit with
[O3](#o3-every-archived-read-is-fully-validated-inside-a-tracing-span). This was never a separate
entry — it was O3's measurement, and it is the reason O3 was taken before anything else on this
page.

The first entry here that was found by measurement rather than by reading, and it is worth keeping
for what the measurement said:

| Partition rows | Access, then project one row |
| --- | --- |
| 16 | 158 ns |
| 256 | 1.93 µs |
| 1,024 | 7.89 µs |
| 4,096 | 31.0 µs |

Linear in the partition, for a query whose answer is one row — the same shape
[F1](../features/sort-key-ranges.md) removed from the *resident* path, still present on the archived
one. `partition_sorted/get_range_64` is flat at ~3.03 µs across those same sizes.

The entry closed with a warning that was worth having and turned out to matter:

> Worth confirming against a realistic row shape before acting: the benchmark rows are small, so
> this measures validator overhead per row at close to its worst ratio.

That is still true, and it bounds what F4 claims. The rows here are two short strings, so the
validator does the most work it can per byte of payload. On a wide row the same partition is more
bytes of payload and fewer relative pointers to check, and the ratio moves. What does not move is
the *shape*: the cost was per query and is now per read.

**A benchmark that reached the real code did not exist when this was filed.**
`archived/access_and_one_row` mimicked `MaybeLoaded::Accessible` by hand because that variant could
not be constructed outside a running server. `partition_sorted/maybe_loaded/*` is the group that
actually calls it, and `archived/*` and `codec/*` are kept beside it as controls — they exercise
`RkyvSupport::access` directly, which F4 did not change, so if they move the machine moved.

## Write path

### O4. `deep_size_of()` is a recursive walk called on every mutation

| | |
| --- | --- |
| **Rank** | **B2** — the entry whose *correctness* value exceeds its performance value |
| **Impact** | Argued, and discounted — the write path is [waiting on the device](../performance/baseline.md#profile--where-the-time-goes), not on this |
| **Difficulty** | M — 13 call sites, but they all want the same thing |
| **Depends on** | nothing |
| **Blocks** | [item 22](known-issues.md#22-size-accounting-inconsistencies) — the same edit settles it |
| **Tradeoff** | Contained — a row's size becomes state that can go stale, which is the thing to test |
| **Benchmark** | none; `partition_sorted/insert` covers the partition but not the accounting |

Ranked above the other write-path entries despite the profile, because it is the only one that
buys a correctness fix with the same edit. Take it for item 22 and treat the cost removal as
change left over.

It measures the whole object graph, so its cost is proportional to the row, not constant. Call
sites on the write path:

| Where | Calls per operation |
| --- | --- |
| `SortedPartition::insert` (`tables/partitions.rs:481`, `:487`) | Two — the new row and the one it replaced |
| `SortedPartition::update` (`:809`, `:813`) | Two — before and after |
| `SortedPartition::remove` (`:708`), `tombstone` (`:731`) | One |
| `UnsortedPartition::new` (`:228`), `update` (`:313`) | One — and `update`'s is a walk of the whole partition, not of the row |
| `merge_from_disk` (`:774`) | Every live row in the merged result |

Carrying a row's measured size alongside it would make all of these O(1). It would also settle
[item 22](known-issues.md#22-size-accounting-inconsistencies) — the mismatched bases between
`UnsortedPartition::new` and `update` exist precisely because the size is re-derived at each site
instead of being owned by one.

### O11. A fresh `AlignedVec` per write and per response

| | |
| --- | --- |
| **Rank** | **B4** — last in its tier, because the profile says its path is already waiting |
| **Impact** | Argued — one allocation and one extra copy per write and per response |
| **Difficulty** | M — rkyv can serialize into a caller-supplied buffer |
| **Depends on** | a storage write-path bench, which is [the biggest gap in the harness](todos.md#benchmark-coverage-the-harness-does-not-have) |
| **Blocks** | nothing |
| **Tradeoff** | None |
| **Benchmark** | ~~none usable yet~~ — **captured**, in `f24-routing`, the first capture taken with the repaired instrument ([Resolved #76](resolved/stage-join.md)). All four reports join completely. `client_serialize` grows **×503** over 1 KiB → 512 KiB on the write path and `reply_serialize` **×238** on the read path: this entry's two buffers are the two fastest-growing stages in the whole nineteen. What is still absent is a **write-path micro benchmark** that would say what reusing them is worth, as opposed to what they cost |

- `FileSystem::commit` (`.../fs.rs:368`) allocates via `RkyvSupport::serialize`, then copies the
  bytes a second time into the DMA buffer (`.../fs.rs:387`) — and hashes the whole record in
  between (`.../fs.rs:373`), so that one function walks the payload **three** times. The checksum
  is a separate pass only because the copy it could ride along with happens five lines later.
- `Shard::reply` (`shard.rs:1212`) allocates one per response.
- `Shoal::send` (`shoal-client/src/client.rs:1023`) allocates one per bundle on the **client** side,
  which this entry never mentioned and which is on the same round trip.
- `write_map_intent!` (`.../fs/compactor.rs:85`) allocates one per archive entry written, and
  `write_partition` (`:305`) allocates one per partition.

rkyv can serialize into a caller-supplied buffer, so all three could reuse one. `commit` is the
interesting one, because the destination buffer it copies into is already there — `prep` hands
back a `&mut [u8]` sized for the record (`.../fs/stream.rs:755-768`).

**This cost is proportional to the row, not constant.** It was filed against the reference cell's
1 KiB rows, where it is small. The [row-size sweep](../performance/row-size.md) is where it stops
being small: `commit` alone walks the record three times at 4 MiB — serialize,
checksum, copy into the DMA buffer — before the device sees a byte. Its *Impact* grade should be read as **Asymptotic** in the row width —
a quantity the caller chooses — rather than as the *Argued* constant above. See
[Row size and what it costs](../tables/row-size.md#the-payload-is-walked-about-six-times-per-round-trip).

### ~~O17. `handle_flushed` runs on every message~~

**Done**, by [F5](../features/flushed-sweep-gate.md). The shard sweeps its tables when a write has
landed or a log is due to rotate, and not otherwise: **711,638 calls became 21,279** over the same
workload, and the time in them fell from 1.344 s to 0.453 s summed across twelve shards.

The original entry read:

> | | |
> | --- | --- |
> | **Rank** | **A2** — the cheapest entry with evidence behind it |
> | **Impact** | **Profiled** — 705,886 calls, 1.8 µs each, 11% of wall clock |
> | **Difficulty** | S — a dirty flag, or gate it on `DataFlushed` having arrived |
> | **Depends on** | nothing |
> | **Blocks** | nothing |
> | **Tradeoff** | Contained — a compaction check is delayed by at most one message |
> | **Benchmark** | `hotpath` `shard::handle_flushed`; no micro-benchmark |
>
> **Why a `hotpath` number is enough here, when the page says it is attribution only.** The
> [caveat](../performance/baseline.md#profile--where-the-time-goes) is about *durations* —
> the instrumented binary perturbs them. The **call count** is not perturbed: 705,886 calls against
> 617,175 queries is a structural fact about the loop, and it is the part of this entry that matters.
> The 1.8 µs is the soft half of the claim.
>
> `shard.rs:844` calls it unconditionally each loop iteration — and `:852` again after the loop — and
> it reaches
> `tables.handle_flushed` → per-table `get_flushed` → `compact_if_needed`
> (`.../persistent/sorted.rs:1097-1116`).
>
> *(Those line numbers are the pre-F5 ones. The gate is now `shard.rs:980-983` and
> `get_flushed` is `.../persistent/sorted.rs:1172`.)* So every message pays a pass over every table, including
> every `DataFlushed` wakeup — of which there is one per completed write.

**It was right about the ranking and about the evidence, and wrong about the tradeoff.** The
"compaction check is delayed by at most one message" was accepted here and then not paid: rotation
is driven by bytes *accepted*, and the accepted byte count is two field reads away, so a synchronous
`compaction_due()` predicate keeps rotation firing on exactly the message it fired on before. That
mattered more than it looks — `build_pressured_config` forces rotation every few writes on purpose,
and a gate that let it drift would have changed what four eviction tests exercise while leaving them
green.

**It also missed half of its own cost.** The `#[instrument]` on `handle_flushed` created an INFO span
per call, and unlike everything else the profile attributes, *that* cost is in the uninstrumented
binary too. It is removed. The same reasoning applies to two more spans on hot paths and is filed as
[O25](#o25-two-instrument-spans-remain-on-per-query-paths).

### ~~O34. A record wider than the staging buffer defeats intent-log batching~~

**Done**, by [F23](../features/self-sizing-staging-buffer.md). `latency_sensitive.buffer_size` is now
a floor and a new `max_buffer_size` a ceiling, and between them `StreamWriter` sizes each staging
buffer to hold about eight of the widest record the last one held. The entry is kept because what it
got wrong is the useful part: it described a step and the behaviour is a slope, and that correction
is what decided the fix. A bundle of 128 rows of 8 KiB took 128 DMA writes and 128 DMA allocations
before the change and takes 16 after it. ~~**The capture has not been re-taken** — the prediction is
below.~~ **It has been**, as `f23-staging-buffer` at `57b44d7`: the same fifteen `latency_buffer` arms
re-run against the new writer, at **+22.0%** for the shipped floor on 64 KiB rows, with the sweep's
spread collapsing from 1.225× to 1.010×. The prediction below is kept beside what it predicted.

| | |
| --- | --- |
| **Rank** | ~~**A1** — the head of the queue. The only entry that moved from argued to measured with a contained fix~~ — done |
| **Impact** | **Measured** — **1.22×** across the sweep at 64 KiB rows (`256Ki` 2.46 ms against `4Ki` 2.82 ms, run intervals disjoint), against 1.06× at the reference cell. It grows with the row width, which is a quantity the caller chooses |
| **Difficulty** | S–M — `prep`, `write` and `alloc_buffer` are one file, but whether the buffer should size itself from observed records or stay a configured floor is a decision rather than a patch |
| **Depends on** | ~~a `latency_buffer` sweep taken at a width above the buffer~~ — **discharged** by `f22-row-size` |
| **Blocks** | any tuning advice about wide rows |
| **Tradeoff** | Contained if the fix is a larger default — padding on a partial flush, and a larger DMA allocation per shard. A self-sizing buffer is a behaviour change and needs the decision above. **It was the self-sizing buffer**, and the tradeoff it actually carries is the one the ceiling bounds: up to `write_behind + 1` buffers of `max_buffer_size` per table per shard |
| **Benchmark** | `macro/conf/storage/latency_buffer/r50/w8192/*` and `.../w65536/*`, the same five rungs above the staging buffer ([F22](../features/row-size-benchmarks.md)). **Captured**, and it corrected the shape of this entry as well as sizing it |

`StreamWriter::prep` flushes the staging buffer whenever the next record will not fit. **What it
allocated when this entry was filed and measured**, kept with its original line numbers the way this
page keeps every struck entry's original text (`.../fs/stream.rs:655-665`, now
`self.staging_target(size)`):

```rust
pub async fn prep(&mut self, size: usize) -> &mut [u8] {
    // if we don't have enough usable space then write our current buffer out
    if self.usable() < size + self.buff_pos {
        // we won't have enough space to write this new data to out buffer so get a new one
        // make this new buffer big enough for our next write or bigger
        let new_usable = std::cmp::max(self.default_buffer_size, size);
        // write but not sync our current buffer to disk
        self.write(new_usable).await.unwrap();
    }
```

and `write` then calls `alloc_buffer(new_usable)` (`:617`), a fresh `alloc_dma_buffer` sized to the
record. `default_buffer_size` is `align_up(max(conf.buffer_size, alignment), alignment)` (`:149`),
and the committed `shoal.yml` sets `latency_sensitive.buffer_size: 4096`.

So a table whose rows exceed about four kilobytes gets **one DMA write and one DMA buffer allocation
per insert**, and the group commit that amortizes the durability barrier across concurrent writers
has nothing left to group. ~~The transition is a step at the buffer size, not a slope.~~ **It is a
slope, in records per buffer** — see below.

~~**Why the sweep that names this setting says nothing about it.**~~ It was swept at
`macro/grid/unsorted/r50/1024` — 1 KiB rows against a 4096 byte buffer, which is the one width where
the setting cannot bite — and came back at 1.06× with a `yes` in the *Real?* column. **The same five
rungs now run at 8 KiB and 64 KiB** ([F22](../features/row-size-benchmarks.md)), and they settle it:

| Buffer | 1 KiB rows | 8 KiB rows | 64 KiB rows |
| ---: | ---: | ---: | ---: |
| `512` | 50,492 | 36,433 | 20,885 |
| `4Ki` | 53,285 | 36,465 | 18,537 |
| `16Ki` | 52,587 | 36,319 | 19,726 |
| `64Ki` | 53,222 | 38,419 | 20,535 |
| `256Ki` | 52,499 | 38,619 | **22,701** |

**The entry is confirmed and its shape is wrong.** Confirmed: the setting is worth 1.22× at 64 KiB
rows on disjoint run intervals, against 1.06× at the reference cell, so it does grow with the row
exactly as the entry claimed. Wrong: the gain is not at the threshold. At 8 KiB rows, going from a
buffer that cannot hold one record (`4Ki`) to one that holds two (`16Ki`) buys **nothing** — 36,465
against 36,319 — and the gain arrives only at `64Ki` and `256Ki`, where 8 and 32 records share a
write. What matters is how many records share an aligned write, not whether the record fits.

**This changes the fix, not just the description.** A larger default sized to "just above the widest
expected row" is the obvious patch and the measurement says it would buy nothing. The default has to
be a multiple of the row, or the buffer has to size itself from what it observes — which is the
decision in the *Difficulty* row, and it is now a decision with a number attached.

**It was decided as the self-sizing buffer** ([F23](../features/self-sizing-staging-buffer.md)). The
number this paragraph asked for became `TARGET_RECORDS_PER_BUFFER = 8`, taken from the rung where the
table above says the gain arrives, and the memory the *Tradeoff* row worried about became a
configured ceiling rather than an unbounded consequence. What the fix does **not** change is
anything above that ceiling: a 4 MiB row still gets one write and one allocation to itself, which is
this entry's mechanism still fully in force at widths the sweep never reached.

**Reproduced before it was fixed**, which this entry never was while it was open — it was filed from
source reading and sized by a capture, and neither of those is a reproduction.
`intent_log_batching::wide_records_share_an_aligned_write` sends one bundle of 128 rows of 8 KiB at a
4096 byte buffer and reads the shard's intent log back off disk:

```
128 rows were written in 128 flushes, which is one record per write
```

16 flushes afterwards, of eight records each, and 5.1% fewer bytes on disk because the per-record pad
regions are gone.

**Re-captured as `f23-staging-buffer`**, the same fifteen arms at `57b44d7` on a clean tree, and it
confirms the fix on the prediction written down before it ran — the `w65536` rungs converge at or
above the 22,701 that `256Ki` alone reached, the `w8192` rungs converge near 38,619, and the
reference cell does not move:

| Buffer | 1 KiB before | after | 8 KiB before | after | 64 KiB before | after |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `512` | 50,492 | 52,993 | 36,433 | 38,474 | 20,885 | 22,592 |
| `4Ki` | 53,285 | 53,156 | 36,465 | 38,497 | **18,537** | **22,619** |
| `16Ki` | 52,587 | 51,863 | 36,319 | 38,182 | 19,726 | 22,665 |
| `64Ki` | 53,222 | 52,551 | 38,419 | 37,768 | 20,535 | 22,813 |
| `256Ki` | 52,499 | 53,052 | 38,619 | 38,305 | 22,701 | 22,691 |

**The sweep's spread went 1.225× → 1.010× at 64 KiB rows**, 1.063× → 1.019× at 8 KiB, and
1.055× → 1.025× at 1 KiB. For the shipped `buffer_size: 4096` that is **+22.0%** at 64 KiB rows
(18,537 → 22,619, disjoint) and **+5.6%** at 8 KiB (36,465 → 38,497, disjoint), against −0.24% and
overlapping intervals at the reference cell.

**Read the shape rather than the headline.** The rungs that already held eight records did not move
— `256Ki` at 64 KiB rows went 22,701 → 22,691, and `64Ki`/`256Ki` at 8 KiB rows are flat or a shade
down — because at those widths the ceiling is what sizing resolves to anyway. The fix moved the bad
rungs up to the good ones and left the good ones alone, which is a floor replacing a fixed size and
not a general speedup. **The 1.22× this entry was ranked on was therefore real and is now
unavailable to tune for**: it was the distance between a badly set knob and a well set one, and
there is no longer a badly set setting of it at these widths.

The width axis corroborates the mechanism from the other side: over 1 KiB → 8 KiB the persistent
arms lose 31% of their throughput while the ephemeral arms — the same tables with `NoStorage` and
nothing else changed — lose 5%, and the `r0`/`r100` split says that octave is the **write** path
(58.8% of its own 64 B rate against the read path's 96.7%). See
[Row size and what it costs](../tables/row-size.md#the-intent-log-batches-fewer-records-as-rows-widen).

**What is still not measured** is the boundary itself. The fixed widths jump 1024 → 8192 with
nothing between them and the staging buffer sits at 4096 inside that gap, so a discontinuity there
would be invisible. Two arms at 2 KiB and 4 KiB would bracket it
([TODOs](todos.md#the-row-size-axis)).

---

## Recovery

### ~~O7. Startup reads the same archive once per update intent~~

**Done**, as a side effect of fixing [item 31](resolved/multi-log-recovery.md) rather than as an
optimization in its own right — the correctness fix and this wanted the same change.

`scan` used to be called once per intent record and, per call, allocate a set sized for a
thousand keys in order to hold at most one:

```rust
// build a set of partitions to load from disk
let mut to_load = HashSet::with_capacity(1000);
```

It then called `load_partition_direct` for that key, which opens, reads, and closes an archive.
Because the set was per record, nothing deduplicated across records: *N* update intents against
one partition cost *N* archive reads.

`scan` was split into a synchronous `scan_keys` that only names partition keys, and the loading
moved into `read_intents`, which unions the keys across **every** log and then loads each one
once. So the deduplication is now across all logs rather than merely across the records of one —
and doing it only per log was never an option, because loading after a replay is precisely what
item 31 was.

The set is allocated once per recovery instead of once per record.

Note in passing that recovery still holds every record's `ReadResult` in memory before replaying
any of them, and now does so for every log at once rather than one log at a time, so peak memory
went from the size of the largest log to the total size of all logs. That is a deliberate
trade — see [Recovery](../storage/recovery.md#limitations).

### O22. Recovery loads the partitions it scanned one await at a time

| | |
| --- | --- |
| **Rank** | **B1** as a passenger on O8; **Tier D** on its own |
| **Impact** | Argued — one serial await, one `dup` and one `close` per scanned partition |
| **Difficulty** | S once O8 exists — it is the same grouping applied to a second loop |
| **Depends on** | O8 |
| **Blocks** | nothing |
| **Tradeoff** | None |
| **Benchmark** | none; startup is not measured at all |

`FileSystem::load_scanned` (`.../storage/fs.rs`) walks the key set the prescan built and awaits
one load per key:

```rust
for partition_key in to_load {
    if partitions.contains_key(&partition_key) { continue; }
    if let Some(partition_read) = self.load_partition_direct(partition_key).await? {
```

This is [O8](#o8-partitions-are-read-one-at-a-time-each-with-its-own-dup-and-close)'s shape on
the recovery path rather than the compaction one, and each iteration also pays
[O15](#o15-one-partition-load-costs-a-dup-and-a-close)'s `dup`/`close`. Neither of those covers
this loop, so it is filed separately — but it should be fixed in the same change as O8, since
grouping by archive file is the same work in both places.

What makes it newly worth filing is that [item 31](resolved/multi-log-recovery.md) is what made
it possible. Under the old `scan` the loads were interleaved with reading, one key at a time,
discovered as each record went past — there was no set to batch. The whole key set is now known
before a single load happens, which is exactly the precondition for grouping them by archive and
issuing them concurrently. The correctness fix handed this optimization its opening.

Worth taking together with the [item 22](known-issues.md#22-size-accounting-inconsistencies)
bullet about this same loop: it is where a partition enters the memory counter in archive bytes,
so whatever touches it next is already reading that line.

**Not taken**, because it is on the startup path rather than a hot one, and the set is normally
small — an interrupted compaction is rare and the active log usually names few partitions.

---

## Compaction

### O8. Partitions are read one at a time, each with its own `dup` and `close`

| | |
| --- | --- |
| **Rank** | **B1**, after O9 and carrying O22 |
| **Impact** | Argued — but O(partitions changed), and [O16](#o16-compaction-shares-the-shards-executor) means it lands on query serving |
| **Difficulty** | L — grouping by archive plus concurrent reads, in two loops and in recovery |
| **Depends on** | **O9 first** — otherwise the grouping is built inside a walk O9 deletes |
| **Blocks** | O22 |
| **Tradeoff** | Contained — concurrency against a `RefCell` borrow that is [already held across awaits](known-issues.md#35-a-refcell-borrow-is-held-across-three-awaits-in-the-compactor) |
| **Benchmark** | none; compaction is not measured at all |

Worth taking with [item 35](known-issues.md#35-a-refcell-borrow-is-held-across-three-awaits-in-the-compactor)
rather than around it — this is the loop that holds the borrow, and issuing the reads concurrently
is exactly the change that would turn that latent defect into a live one.

```rust
for partition in self.changes.keys() {
    if let Some(entry) = self.map.to_archive.borrow().get(partition) {
        let handle = self.map.get_archive(&entry.archive).await?;
        let read = handle.read_at(entry.offset, entry.size).await?;
```

`.../fs/compactor.rs:230-241`

Serially awaited, one read per partition, with no grouping by archive file and no coalescing of
entries that happen to be adjacent in the same archive. ~~`get_archive` returns a `dup` of a cached
handle (`.../fs/map.rs:481-509`) and the caller closes it, so each read also costs a `dup`/`close`
pair.~~ Since [F120](../features/one-operation-a-record-read.md) (2026-10-30) `get_archive` hands out
a share of the cached file, so a read costs no `dup` and no close, and what this entry still names is
the reads one at a time. `compact_archives` (`:443-500`) has the same shape.

Grouping `changes` by `entry.archive` before reading would let one handle serve many reads, and
glommio's read APIs can issue them concurrently rather than one await at a time.

(This is also the loop with the borrow-across-await in
[item 35](known-issues.md#35-a-refcell-borrow-is-held-across-three-awaits-in-the-compactor).)

### O9. Every intent log rotation walks the entire on-disk partition set

| | |
| --- | --- |
| **Rank** | **B1** — the head of Tier B, and the entry that ages worst |
| **Impact** | Argued — but **O(total partitions on disk) per rotation**, regardless of how few changed |
| **Difficulty** | M — maintain a per-archive used-byte total in `set_partition` and `remove_partition` |
| **Depends on** | nothing |
| **Blocks** | O8, O21 |
| **Tradeoff** | Contained — an incremental total is state that can drift from the truth it summarises |
| **Benchmark** | none; compaction is not measured at all |

Ranked first in its tier because it is one of the two entries whose cost grows with how long the
database has existed rather than with how hard it is being used. Everything else on this page gets
worse under load; this gets worse while idle.

`compact_if_needed` queues a `CompactionJob::Archives` on every rotation
(`.../fs.rs:460-461`), and that job calls `sort_by_load` (`.../fs/map.rs:551-585`), which iterates
all of `to_archive` and **copies every `ArchiveEntry`** into a fresh
`HashMap<Uuid, Vec<ArchiveEntry>>`:

```rust
for (_, archive_entry) in self.to_archive.borrow().iter() {
    let entry: &mut usize = used_by.entry(archive_entry.archive).or_default();
    *entry += archive_entry.size;
    let entries_entry = sorted.entries.entry(archive_entry.archive).or_default();
    entries_entry.push(*archive_entry);
}
```

The cost is O(total partitions on disk) per rotation, regardless of how few of them changed.

`compact_archives` then opens **every** candidate archive with `DmaFile::open`
(`.../fs/compactor.rs:460`) — bypassing the handle cache in `loaded_archives` that
`get_archive` maintains — purely to call `file_size()`, and closes it again for the ones it skips
on the 50% utilization test (`:462-478`).

Maintaining a per-archive used-byte total incrementally in `set_partition` and `remove_partition`
would replace the whole scan, and archive sizes are already known to the writer.

### O10. `SerializedMap::save` snapshots by cloning

| | |
| --- | --- |
| **Rank** | **B3** — a contained cleanup |
| **Impact** | Argued — a full copy of the archive map per serialization |
| **Difficulty** | S — serialize from the borrow rather than from a copy of it |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Contained — the clone is what keeps the borrow short, so removing it holds a `RefCell` open across serialization. Compare [item 35](known-issues.md#35-a-refcell-borrow-is-held-across-three-awaits-in-the-compactor) before doing it |
| **Benchmark** | none |

```rust
all_archives: map.all_archives.borrow().clone(),
to_archive: map.to_archive.borrow().clone(),
```

`.../fs/map.rs:205-206`, inside `SerializedMap::save` (`:202`) — a full copy of the archive map
before every serialization. Both `HashSet` and `HashMap` are also rebuilt at
`with_capacity(1000)` (`:194-195`), so the copy allocates for a thousand entries whether or not
there are that many.

### O16. Compaction shares the shard's executor

| | |
| --- | --- |
| **Rank** | **Tier D — not an actionable entry.** It is a consequence of thread-per-core |
| **Impact** | Argued — and it is what makes O8 and O9 foreground costs rather than background ones |
| **Difficulty** | n/a — changing it means giving up the design |
| **Depends on** | nothing |
| **Blocks** | nothing. It **raises** O8 and O9 |
| **Tradeoff** | The design itself: no cross-core locking, in exchange for compaction competing with queries |
| **Benchmark** | none |

Kept in the queue as a rank of its own because it changes how two other entries should be read, and
a reader who skips it will under-rate them.

The compactor and the loader are both spawned onto `medium_priority` on the same glommio executor
as the query loop (`.../fs.rs:164-167`, `:594`). A long `compact_archives` competes directly
with query serving, and the task queue's share (`Shares::Static(500)` against the high priority
queue's 1000, `shard.rs:356-366`) is the only lever over it. That is a deliberate design — it is
what thread-per-core buys — but it means O8 and O9 are not merely background costs.

~~Note also that `write_partition` iterates `self.loaded`, a `HashMap`
(`.../fs/compactor.rs:303`), so partitions land in the archive in hash order and reads of
related partitions get no locality from it.~~ Since [F121](../features/merges-in-key-order.md)
(2026-10-30) `write_partition` writes a job's partitions in key order, so a tablet's records of a
merge lie together ([O142](#o142-a-merge-writes-its-partitions-in-a-hash-maps-order)).

### O21. A forced rotation of an empty intent log does the whole rotation anyway

| | |
| --- | --- |
| **Rank** | **B3** — but take only the cheap two thirds |
| **Impact** | Argued — startup cost only, per restart per shard, dominated by the O9 walk behind it |
| **Difficulty** | S for suppressing the `Archives` job; **L** for skipping the rotation itself |
| **Depends on** | O9 removes most of what makes this expensive |
| **Blocks** | nothing |
| **Tradeoff** | None for the cheap form. **Major** for the full form — the generation counter, `FlushProgress.rotated` and `MarkEvictable` are all keyed off the rotation happening |
| **Benchmark** | none; startup is not measured |

**The split is the whole entry.** Queue the `Archives` job only when a rotation had changes to
compact: S, no tradeoff, and it removes the expensive part. Suppressing the rotation is a separate,
much larger decision that touches generations, and it should not be bundled in.

`compact_if_needed` rotates on `force` without looking at whether the active log holds anything
(`.../fs.rs:435-475`), and startup always forces one
(`.../tables/persistent/sorted.rs:274`). A table nobody wrote to therefore pays, per restart per
shard, a rename, a fresh file for the new active log, a `CompactionJob::IntentLog` that reads a
zero length file, the `glommio::io::remove` that now deletes it
([item 14](resolved/empty-rotated-logs.md)), and a `CompactionJob::Archives` behind it — which is
[O9](#o9-every-intent-log-rotation-walks-the-entire-on-disk-partition-set)'s full walk of the
on-disk partition set, the expensive part by some margin.

Skipping the rotation when the log is empty is not a local change, which is why item 14 cleaned up
after the rotation instead of preventing it: the generation counter, `FlushProgress.rotated`, and
the `MarkEvictable` that advances a table's compacted generation are all keyed off the rotation
happening. Suppressing the `Archives` job alone — queue it only when a rotation had changes to
compact — is the cheap two thirds of this and does not touch generations at all.

Startup cost only, not per write, which is why it is here rather than in Known Issues.

---

## The harness itself

### O24. Two benchmarks move with the shape of the binary around them

| | |
| --- | --- |
| **Rank** | **Tier B**, unranked inside it — this is a measurement defect, not a cost in Shoal |
| **Impact** | **Measured** — `codec/access` and `archived/access_and_one_row` moved +9% to +13% across a change that does not touch the function they call |
| **Difficulty** | M — the fix is a benchmark harness question, not a Shoal one |
| **Depends on** | nothing |
| **Blocks** | nothing, but it **widens the noise band** for anything validated against those two ids |
| **Tradeoff** | None |
| **Benchmark** | itself |

Found while taking [F4](../features/validated-archives.md), which carried both of these ids forward
untouched precisely so that they would act as controls.

Both moved together, outside the ±5% band, and a repeat capture put them within 0.7% of the first —
so the shift is reproducible within a build and meaningless across builds. It is not the change that
was being measured: nothing in F4 touches `RkyvSupport::access`, and `archived/walk_all`, which calls
the same function, moved −4% in the opposite direction at the same time.

**Having a third baseline is what identified which run was wrong**, and it is the argument for
keeping `B1` frozen. The *post*-change build sits +3.5% to +4.1% from B1 — inside the band. It was
the *pre*-change capture that was the outlier, at 27,835 ns against B1's 29,891 ns for
`codec/access/4096`. With only a trailing baseline the drift would have looked like a regression the
change caused.

What the two that moved have in common is that they are dominated by a linear walk over one
`AlignedVec` built at group setup. `AlignedVec` guarantees 16-byte alignment and nothing about where
the buffer lands relative to a cache set or a page, so adding a benchmark group ahead of them in the
binary changes what they are walking over. This is the between-process variation
[Performance Baseline](../performance/baseline.md#what-the-micro-layer-can-actually-resolve)
says a confidence interval cannot see, caught in the act.

**Fix direction:** neither obvious nor free. Allocating the buffer at a known page offset would pin
it, at the cost of measuring an alignment production does not have — a `ReadResult` off a DMA read is
page aligned, an `AlignedVec` is not, so the honest fix may be to make the benchmark buffers match
the DMA case rather than to pin them arbitrarily. Until then, treat a movement in
`codec/access` or `archived/access_and_one_row` of under ~15% as saying nothing.

### O25. Two `#[instrument]` spans remain on per-query paths

| | |
| --- | --- |
| **Rank** | **A6** — last in Tier A, because it is the only entry there a profile cannot rank |
| **Impact** | Argued — 617,175 INFO spans each, per run, in the **uninstrumented** binary; see [O44](#o44-one-trace-per-request-costs-a-span-per-query-and-one-per-frame) for what tracing the whole request added |
| **Difficulty** | S — delete an attribute, or set `level = "trace"` |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Contained — it is observability, and `reply`'s span is a real parent |
| **Benchmark** | none — and the point of this entry is that the obvious one does not exist |

Filed while taking [F5](../features/flushed-sweep-gate.md), which removed a third one.

`Shard::handle_query` and `Shard::reply` both carry `#[instrument]`, both default to
`INFO`, and the subscriber is a `Registry` with a `fmt` layer filtered at `Info` (`server/trace.rs`)
against a `shoal.yml` that sets `level: Info`. So both callsites are enabled: each call allocates a
span in the registry's slab, enters, exits and closes it. At 617,175 calls apiece that is 1.2 million
span lifecycles per run.

**There is a third one now, and it is not a candidate for removal.**
[Resolved #89](resolved/fragmented-query-traces.md) moved `Coordinator::send_to_shard`'s
function-level span into its routing loop as `Coordinator::route`, one per query rather than one
per bundle, and that span is the parent every other span on the query path re-parents off. Deleting
it does not flatten one edge of the trace the way deleting `reply`'s would; it fragments the whole
of it, because an empty parent starts a new trace rather than an orphan. The same now applies to
`reply`'s more strongly than when this entry was written. **`handle_query`'s is still the one to go
if only one goes** — nothing takes it as a parent.

**This is the entry the profile is structurally unable to rank**, which is why it is filed rather
than taken. `hotpath` attributes time to *its own* scopes; a span inside a scope is counted as part
of that scope's duration and never appears as a row. Worse, the cost is present in the baseline
binary and absent from nothing — so unlike every other entry on this page, there is no capture in
`docs/perf/` that contains the number. Settling it needs a purpose-built pair of runs with the
attributes present and absent, which is a capture nobody has taken.

**The two are not equivalent, and should not be decided together.** `reply`'s span uses
`parent = &span` to attach a reply to the query that caused it, so it carries the one piece of trace
structure this path has; deleting it flattens the trace. `handle_query`'s is a plain wrapper around a
function that is already the top of its own `hotpath` scope. If only one goes, it is `handle_query`'s.

**Fix direction:** `level = "trace"` on both is the conservative form — the callsite survives for a
debugging session and costs a cached interest check rather than a slab insert. F5 removed its span
outright instead, but that one had no fields, no children, and wrapped a function that usually did
nothing. Neither of these is that.

## Routing and memory

### ~~O6. The ring is a 1000×N `BTreeMap` answering a question arithmetic would answer~~ — done

**Taken, with [items 11, 12 and 37](resolved/tablet-ring.md).** The ring was replaced by a tablet
map: `find_shard` is now a shift and two indexed loads into a 4096-entry `Vec<u16>` — 8 KiB,
against a 16,000-entry `BTreeMap` — and there is no search at all.

The original entry read — **its line numbers describe code that no longer exists**, and `ring.rs`
today is the tablet map (`Ring::new` at `:72`, `find_shard` at `:140`, `TABLET_BITS` at `:25`):

> `ring.rs:26-44` builds it, `ring.rs:51-68` searches it, and `find_shard` runs once per partition
> key per query. At 16 shards that is a 16,000-entry `BTreeMap` — pointer-chasing, one allocation
> per node — consulted on the hot path.
>
> As built it does not need to be a search structure at all. Every shard uses the same fixed stride
> `RING_JUMP`, so the ring is exactly periodic and the owning shard is computable directly; that is
> the same property that makes the vnodes useless in item 12. Once item 12 is fixed and positions
> become independent, a search is needed again — but a sorted `Vec<(u64, usize)>` with
> `partition_point` is still strictly better than a `BTreeMap` for a structure that is built once
> and then only read.
>
> The two items should be done together: fixing item 12 without touching this doubles down on the
> structure that costs the most.

It was right that the two had to move together, and right that arithmetic could answer the
question — but it framed the choice as *periodic ring, so compute* versus *independent positions,
so search*. Tablets are neither: an explicit assignment table is a third option that is both O(1)
*and* evenly balanced, which is the combination the entry assumed was unavailable. The reason to
prefer it is not speed, though — it is that a stored assignment can be **moved**, which a computed
one cannot, and that is what a distributed Shoal needs.

~~Still unmeasured, as everything on this page is. The array is small enough to stay cache resident
where the `BTreeMap` was not, but that is an argument, not a profile.~~ **Measured**, by
`routing/find_shard` ([F24](../features/routing-benchmarks.md)): **392 ps, flat to 0.4% across 1, 4,
12 and 64 shards.** A lookup that does not move across a 64× change in the ring is a lookup that is
not searching it, which is what this change was for and what nothing had checked. The cache-residency
argument above is still an argument — the benchmark says the cost is constant, not why.

### O14. Fixed thousand-element preallocations on per-call paths

| | |
| --- | --- |
| **Rank** | **A5** — near-free, do it whenever the surrounding code is open |
| **Impact** | Argued — a 1,000-element allocation to hold a handful of entries |
| **Difficulty** | S |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | None |
| **Benchmark** | none |

- `evict_data` allocates a `Vec::with_capacity(1000)` per table it touches, to hold however many
  victims that table has (`shard.rs:861`), plus a `HashMap::with_capacity(10)` per call (`:852`) —
  and it is called on **every message** while a shard is over its memory limit, including when it
  can free nothing at all ([item 59](known-issues.md#59-a-shard-that-cannot-free-anything-keeps-trying-on-every-message-in-silence)).
- `write_partition` allocates `to_mark` at 1000 per call (`.../fs/compactor.rs:299`).
- `SerializedMap::save` rebuilds both halves of the map at 1000 per serialization
  (`.../fs/map.rs:194-195`), which is [O10](#o10-serializedmapsave-snapshots-by-cloning)'s clone
  seen from the allocation side.
- ~~The per-record `HashSet` in [O7](#o7-startup-reads-the-same-archive-once-per-update-intent).~~
  Gone — it is allocated once per recovery now.

### O15. One partition load costs a `dup` and a `close`

| | |
| --- | --- |
| **Rank** | **B3** — a contained cleanup, with a second half that is not one; since [W42](../future-work/scans/scan-speed.md) (2026-10-29) the first half is required for M23's rate, a [W1 row](../future-work/scans/prerequisites.md#one-operation-a-record-read). **The first half is built** (2026-10-30), by [F120](../features/one-operation-a-record-read.md); the second, evicting from `loaded_archives`, stays B3 |
| **Impact** | ~~Argued — two syscalls per partition read~~ Measured in part by W42: a scan's page pays the `dup` and the close for each of a tablet's 732 partitions, one after another at a depth of one, on a Zen1 host spending up to 73 µs of cpu a partition; the pair's own share is estimated at 2 to 5 µs until X17's profile ([O141](#o141-a-walked-partition-costs-a-zen1-host-about-73-microseconds-of-cpu)); measured by X17 (2026-10-30) once [F120](../features/one-operation-a-record-read.md) removed them: 12.1 µs of a Zen1 host's node cpu a partition at a depth of one, 58.5 µs against 46.4, and 1.9 µs at sixteen in flight |
| **Difficulty** | S for the `dup`/`close`; M for evicting from `loaded_archives` |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Contained — borrowing the cached handle means the cache's lifetime now bounds the read's |
| **Benchmark** | ~~none~~ X17's arm of one operation a record against three, `scan/Big/none` on X16's side cluster |

Since W42 the pair is in `ArchiveMap::read_record` (`shoal-core/src/server/tables/storage/fs/map.rs:1298-1305`),
which every get, merge load, archive pass and walk takes; the lines below are where it stood when
this was filed.

**Built, the first half:** a read takes a share of the cached archive's file, `OpenArchive`, an
`Rc<DmaFile>` beside its format, and lets it go with `release`, glommio's `close_rc`, which closes
the file for the last holder alone ([F120](../features/one-operation-a-record-read.md)). A record's
read is one operation. It is judged by X17's arm of one operation against three, which its page's
Performance section will carry. Building it found two races in the handle's lifetime,
[Resolved #341](resolved/held-handle-reads-verified.md) and
[#342](resolved/removed-archive-cached-again.md). The second half below is untouched: a descriptor
an archive is still held for the life of the process, though a read no longer adds one of its own.

**The second paragraph of this entry is not an optimization.** A file descriptor held per archive
for the life of the process is a resource leak with a hard ceiling behind it, and it gets worse as
a table accumulates archives. It is filed here because it was found here, but it should be read as
a defect that has not been filed as one — the reason it has not is that no `EMFILE` has been
observed, so it is an argument rather than a symptom.

It is no longer only an argument about the future, though. `EMFILE` is the failure the loader's
retry classification exists for: an archive that cannot be opened is the one error class worth
attempting again, precisely because the descriptor another read is holding may come back
([Resolved #16, 51](resolved/partition-load-failure.md#the-fix)). Doing this optimization would
narrow what that retry is for.

`read_partition_helper` closes the handle the map just handed it (`.../fs/loader.rs:92-94`), even
though `ArchiveMap` caches open handles in `loaded_archives` specifically so it does not have to
reopen (`.../fs/map.rs:338`, `:481-509`). Borrowing the cached handle rather than duplicating it
would remove both syscalls from every partition read.

**The retry loop multiplies it.** A read is now attempted up to `MAX_LOAD_ATTEMPTS` times
(`.../fs/loader.rs:25`, three), so a `Retryable` failure pays the `dup`/`close` pair once per
attempt. That is the right behaviour and it is worth noticing here, because the descriptor
shortage the retry exists to ride out is the one this entry's second paragraph is about — the
retry is treating a symptom that borrowing the cached handle would reduce the incidence of.

The cache has the opposite problem at the other end: nothing evicts from `loaded_archives` except
`remove_archive` (`.../fs/map.rs:538-548`) and the shutdown drain (`:615`), so a table with many
archives holds a file descriptor per archive for the life of the process. It is preallocated for a
thousand of them (`:382`), which is the shape of the expectation.

### O27. An ephemeral write makes a mixed database sweep every table

| | |
| --- | --- |
| **Rank** | **Tier B**, last — the cost only exists in a database that mixes table kinds |
| **Impact** | Argued — one extra walk of every table per shard loop iteration that had an ephemeral write in it |
| **Difficulty** | S |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | None, but the fix has to preserve [F5](../features/flushed-sweep-gate.md)'s gate exactly |
| **Benchmark** | none — needs a workload over a schema holding both kinds, which no workload does |

`ShoalDatabase::compaction_due` is generated as an OR across every table
(`shoal-derive/src/traits/db.rs`), and the shard sweeps when
`data_flushed || tables.compaction_due()` ([F5](../features/flushed-sweep-gate.md)). An ephemeral
table answers `true` from the moment a row is inserted until the next sweep releases its response,
which is correct and necessary — it is the only thing that wakes the shard to answer that insert
([F9](../features/ephemeral-tables.md#design-choices)). But the sweep it asks for is a sweep of
*every* table, so a persistent table sharing the database has `get_flushed` called on it — a
`compact_if_needed`, a pending-response scan — for a wakeup that had nothing to do with it.

An all-ephemeral or all-persistent database pays nothing: in the first case every table genuinely
had something to release, and in the second nothing changed. The cost is exactly the mixed case,
and it scales with how many persistent tables share the database with a busy ephemeral one.

The shape of the fix is a sweep that asks each table rather than the database — `handle_flushed`
already visits every field, so the gate could move to the same place as the visit instead of
sitting above it. That is a change to F5's mechanism, which is why it is filed rather than taken:
F5 exists because that sweep used to run unconditionally, and the way to get this wrong is to
reintroduce that.

**No benchmark would show it today.** Every workload drives one table. A workload over a schema
holding both kinds is the thing to build first, and it is worth having for its own sake — a mixed
database is the shape a real use of ephemeral tables has.

---

## The client

### O28. The client takes two guards on its response map for every query it sends

| | |
| --- | --- |
| **Rank** | **A5**, beside O5 and O14 — near-free, and on a path whose cost nobody has measured |
| **Impact** | Argued — two `papaya` guard acquisitions per query where one would do, plus one owned guard per response |
| **Difficulty** | S — a single `pin()` held across the check and the insert |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | None |
| **Benchmark** | `macro/transport/send_one/small`, and it is **adjudicable now** — ~~`client.rs` has no `tracing` spans and no `hotpath` scopes at all~~, it has both since [F16](../features/client-builder.md) |

`Shoal::track_response` registers a query's response channel by asking whether an id is taken and
then inserting under it:

```rust
if self.channel_map.pin().get(&*query_id).is_none() {
    // insert this id
    self.channel_map.pin().insert(*query_id, tx.clone());
```

`shoal-core/src/client.rs:195-197`

`channel_map` is a `papaya::HashMap`, where `pin()` acquires a guard into the collector's epoch.
Two calls means two guards for one logical operation, and the pair is not atomic either — which
does not matter here, since a single-threaded caller cannot race itself for an id it just
generated, but does mean the two-call shape is buying nothing.

The other side pays a heavier one: `TcpProxy` uses `pin_owned()` once per response arriving
(`:536`), and an owned guard is the variant that allocates rather than borrowing the caller's.

**Why this is filed at all, given how small it is.** It is on the one layer of the system that has
no instrumentation whatsoever. `docs/src/appendix/todos.md` records that "`client.rs` has neither
`tracing` spans nor `hotpath` scopes, so the share of measured latency that is the harness's own is
unknown" — every macro number in
[Benchmark Results](../performance/overview.md) includes this code and none of them can
attribute anything to it. That makes a client-side entry worth *recording* even when it is too
small to act on, because the total it belongs to has never been bounded.

**Established by reading the source**, during the [August 2026 review](review-2026-08.md).

**Fix direction:** hold one guard — `let map = self.channel_map.pin();` — across the check and the
insert. `papaya` also has an `entry`-shaped API that expresses "insert if absent" in one operation,
which is what this loop actually wants. ~~Neither should be taken before
`transport/{send_one,send_batched,stream,stream_unordered}`
([TODOs](todos.md#what-f8-left-undone)) exists, which is the workload that would give the client
half a number at all.~~ **That workload exists** ([F13](../features/transport-workloads.md)), so
this entry is adjudicable for the first time — and the arm to adjudicate it on is
`macro/transport/send_one/small`, where a per-query cost is not buried under the bytes. It stays
open because nothing has been measured, not because nothing can be.

~~**That workload was blocking more than this entry, and half of that is now unblocked.**~~
**Both halves have landed.** The [Direction](../direction/overview.md) chapter is nine design pages
about the client, and its step 0 — before any of them — is exactly what this entry asks for: spans
and `hotpath` scopes in `client.rs`, plus the `transport/*` workloads
([D6](../direction/connection-pool.md#how-it-would-be-measured)). The workloads landed with
[F13](../features/transport-workloads.md) and the instrumentation with
[F16](../features/client-builder.md), which put a `hotpath` scope on `track_response` itself. **Step
0 is done and this entry is adjudicable for the first time since it was filed.**

**F16 deliberately did not take the fix**, and the reason is a rule worth reusing: it landed the
instrumentation and measured that, so that the capture carrying the instrumentation's cost is not
also the capture carrying this fix's benefit. Two changes in one capture is one number nobody can
attribute. The fix is still one guard instead of two, and it is still small; what it now has is a
before and an after that mean something.

**One thing to know before measuring it.** F16's own capture found no result in 144 metrics across
sixteen `transport` workloads, at spreads of a few percent on the `small` arms. A pair of `papaya`
guard acquisitions is nanoseconds against a wall clock of ~60 µs per query, so this is very likely
below what the macro layer can see at all, and the honest place to adjudicate it may be a
`hotpath` capture over the `client::track_response` scope rather than a `transport` wall clock.

## The wire

### ~~O29. A request body is zeroed and then immediately overwritten~~

**Done**, by [F25](../features/read-buffers-are-filled-not-zeroed.md), together with
[O37](#o37-the-client-zeroes-a-response-buffer-and-immediately-overwrites-it) — the same defect on
the other end of the same round trip. `ServerMsg::Client` carries a `RequestBody` whose field is
private and whose only constructor is the read that fills it, which is the shape change the
*Difficulty* grade below was about.

| | |
| --- | --- |
| **Rank** | ~~**B4** — free bytes on every request, behind a shape change that is not free~~ — **done** |
| **Impact** | ~~Argued — one `memset` of the whole bundle per request, discarded on the next line~~ **and the shape was wrong.** `BytesMut::zeroed(len)` is `BytesMut::from_vec(vec![0; len])`, and `vec![0u8; n]` is `alloc_zeroed` — **calloc, not an unconditional `memset`**. A size the allocator serves out of its heap really is memset; one it serves with a fresh `mmap` arrives zeroed from the kernel. So this entry's cost was never asymptotic in the row width the way the note at the foot of it claimed, and [O37](#o37-the-client-zeroes-a-response-buffer-and-immediately-overwrites-it) — an unconditional write at every size, over the larger payload — is the half that is |
| **Difficulty** | M — `ServerMsg::Client` has to stop carrying an owned `BytesMut` |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Contained — a shape change inside the server, no format change |
| **Benchmark** | ~~none usable yet~~ ~~— **captured**, in `f24-routing`. `wire_codec` measures the codec and not the relay's allocation, but the stage breakdown now locates it: `decode`, the stage that contains this `memset` and the deserialize beside it, grows **×199** over 1 KiB → 512 KiB on the write path~~ — **that reading was wrong, and no stage contains this at all.** `base`, the stamp every offset is measured from, is taken in `client_rx_relay` *after* `read_exact` returns, and `decode` is the span from `bundle_dequeued` to `decoded`. The allocation and the read both happen before the bundle's clock starts, so they fall in **`net_in`** — `base.since(client.written)` — mixed in with real wire time. The instrument that does adjudicate it is `wire_codec/width/request/body/{zeroed,uninit}`, built by [F25](../features/read-buffers-are-filled-not-zeroed.md), which runs both shapes in one build |

```rust
// allocate a buffer that is exactly the right size
let mut data = BytesMut::zeroed(header.body_len());
// wait for messages from our client
if let Err(error) = tcp_rx.read_exact(&mut data).await {
```

`shard.rs`, `client_rx_relay`

`zeroed` writes the whole buffer and `read_exact` overwrites every byte of it on the next line. At
a hundred queries a bundle that is tens of kibibytes of `memset` per request, for nothing.

[Item 34](resolved/unvalidated-length-prefix.md) named this alongside the unbounded allocation, and
[F10](../features/framing-and-protocol-evolution.md) fixed the allocation and left the zeroing. It
is now *bounded* waste, which is the part that item was actually about.

The reason it was left is worth stating, because the fix looks like a one-line swap for
`BytesMut::with_capacity` plus `unsafe { set_len }` and is not: `ServerMsg::Client` carries the
`BytesMut` by value across a channel into `handle_client`, so the buffer's initialization state
becomes a property of a message type that several call sites construct. Doing this with `MaybeUninit`
or an `unsafe` `set_len` needs the read that fills it to be the only way that message can be built,
which `server/messages.rs` does not currently guarantee.

~~**This cost is proportional to the row, not constant.** It was filed against the reference cell's
1 KiB rows, where it is small. The [row-size sweep](../performance/row-size.md) is where it stops
being small: a bundle of 4 MiB rows is a 4 MiB `memset` per request, for nothing. Its *Impact* grade should be read as **Asymptotic** in the row width —
a quantity the caller chooses — rather than as the *Argued* constant above.~~ **Not asymptotic, and
`alloc_zeroed` is why** — see the *Impact* row above. A bundle of 4 MiB rows is not a 4 MiB
`memset`; it is a 4 MiB allocation the allocator may or may not have to write, and a loaded server
recycling that size is the case where it does. See
[Row size and what it costs](../tables/row-size.md#the-payload-is-walked-about-six-times-per-round-trip)
and [F25](../features/read-buffers-are-filled-not-zeroed.md).

### O35. The per-connection response relay writes one response at a time

| | |
| --- | --- |
| **Rank** | **Tier D** — declined for now. ~~**C5** — blocked on the same design pass as the framing.~~ The capture took its evidence away |
| **Impact** | **Argued from the source only.** ~~Indicated by the capture — a 45× p50-to-p99 spread at 512 KiB on a fully resident table.~~ That spread is **queueing**: at one outstanding query it collapses to 1.7× at the same width, and to 1.3–2.5× at every width on the axis |
| **Difficulty** | M as a scheduling change; **XL** if it reaches the framing |
| **Depends on** | nothing to reorder; [D2](../direction/framing.md) to interleave |
| **Blocks** | nothing |
| **Tradeoff** | Contained while it stays a scheduling change. **Major** if the fix is chunking, because the protocol frames a response whole and splitting one is a format change |
| **Benchmark** | `macro/grid/depth/1/<width>` against the `r50` width sweep ([F22](../features/row-size-benchmarks.md)). **Captured, and it came back negative** — the alternative explanation turned out to be the whole explanation |

`client_tx_relay` (`shard.rs:200`) is one task per client connection, and it is a serial loop: take a
response off the channel, `write_vectored` it to completion, then look at the next one. Every shard's
replies for that client funnel through it.

```rust
let mut bufs = &mut [IoSlice::new(&preamble), IoSlice::new(&archived)][..];
while !bufs.is_empty() {
    match tcp_tx.write_vectored(bufs).await {
```

So a response's wall clock is its own write plus every write already queued in front of it, and
nothing orders that queue by size. ~~At 512 KiB the persistent unsorted arm reads at a 333.55 µs p50
and a 15.19 ms p99, against 2× at 8 KiB, on a table with nothing to load from disk.~~

**The depth-1 ladder refutes that reading.** The p50-to-p99 spread this entry was ranked on exists
only at a load depth of 32:

| Row | depth 1 | depth 32 |
| ---: | ---: | ---: |
| 8 KiB | 1.3× | 2.0× |
| 32 KiB | 1.5× | 6.7× |
| 64 KiB | 1.8× | **37.4×** |
| 128 KiB | 1.8× | **52.0×** |
| 512 KiB | 1.7× | 46.8× |
| 4 MiB | 2.5× | 7.4× |

A cost *inside* the relay would survive the collapse — one outstanding query still writes a 512 KiB
response through the same serial loop, and it comes back at 119.37 µs p50 and 201.54 µs p99. It does
not survive. The tail belongs to the queue thirty-two outstanding queries build in front of the
relay, and this entry has no evidence of its own left.

**Kept rather than deleted, and moved to Tier D.** The mechanism is still real — a wide response
does block narrow ones behind it whenever a queue exists — and the reasoning that produced the entry
was sound. What is gone is any reason to believe it is worth paying for: nothing separates the
benefit of reordering the relay's queue from simply not queueing thirty-two wide queries, and
[Row size](../tables/row-size.md#what-to-do-today) now recommends the latter with a factor of
eighteen behind it. Reopen this when something measures the relay under a bounded queue.

**Two fixes, and they are not the same size.** Reordering — serving the shortest queued response
first, or round-robining across shards — is contained to this function and changes no format, but it
only redistributes the wait and starves nothing only if it is bounded. Interleaving — chunking a
large response so a small one can pass it — actually removes the blocking and needs the wire to carry
a partial response, which is [D2](../direction/framing.md)'s territory. The cheap fix is worth
measuring first, and neither is worth doing before something can see the difference.

See [Row size and what it costs](../tables/row-size.md#a-wide-response-blocks-every-narrow-one-behind-it--the-tail-was-the-queue).

---

## ~~Suggested order~~ — superseded by [the priority queue](#the-priority-queue)

Kept because it was right about more than it was wrong about, and because what it left out is the
clearest argument for why the queue above exists. It read:

> 1. **O3**, then **O1** — both remove work from every read, neither changes an on-disk or wire
>    format. O3 is the smaller change and the larger win; do it first, and read
>    [archive checksums](todos.md#archive-checksums) before dropping validation.
> 2. **O4** — retires a real correctness wart
>    ([item 22](known-issues.md#22-size-accounting-inconsistencies)) with the same edit that removes
>    the cost, which makes it the easiest one to justify.
> 3. **O9**, then **O8** — the only entries whose cost scales with total data on disk rather than
>    with request rate. Everything else gets worse under load; these get worse just by existing
>    longer.
> 4. **O5** and **O14** — near-free, and worth doing whenever the surrounding code is open.
>
> **O6** has been taken, together with items 11, 12 and 37 as this list said it had to be.
>
> **O2** is deliberately not on this list. It is the largest single win available on the read path
> and also the largest change, because it needs `ResponseAction::Get` to hold something other than
> `Vec<T>`, which reaches the wire format and the client. It is worth its own design pass rather
> than a slot in an ordering.

**What it got right, and the queue keeps.** O3 first. O9 before O8. O4 justified by the correctness
fix rather than by the cost. O5 and O14 as near-free. O2 as a design pass rather than a queue slot —
the queue puts it in Tier C for exactly the stated reason, and only adds that O18 has to go with it.

**What it got wrong.** It paired **O1 with O3**, on the grounds that both remove work from every
read. That grouping does not survive the evidence: O3 is the one entry a benchmark already settles,
and O1 is one of five that no benchmark can currently see at all. They belong two tiers apart, and
the thing that separates them is not size but whether the claim can be checked.

**What it left out is the larger point.** It covered seven entries. It was silent on **O23**, which
turned out to be the only measured entry on the page; on **O17**, the cheapest change with evidence
behind it; and on **O13**, which was quietly getting worse the whole time — its quadratic term moved
onto the resident get path and the entry was never updated. Three of the top four ranks were not on
the list, which is what a flat catalogue with an ordering bolted to the end will do.

### O30. Nothing can see what a connection costs to open

| | |
| --- | --- |
| **Rank** | **C3** — not actionable, because there is nothing to act on yet |
| **Impact** | **Unknown.** Every other entry on this page is at least argued from the source; this one cannot be, because the quantity is a wall clock and no clock is started |
| **Difficulty** | S to build the workload. Unknown for whatever it then shows |
| **Depends on** | a `connect` workload in `shoal-bench`. The *instrumentation* half is done ([F16](../features/client-builder.md) put a `hotpath` scope and a span on `ShoalConnectionManager::connect_to`), so what is missing is now only the workload |
| **Blocks** | any judgement about [F12](../features/authentication.md)'s cost, and about [D4](../direction/encryption.md)'s and [D6](../direction/connection-pool.md)'s |
| **Tradeoff** | — |
| **Benchmark** | the missing one *is* the entry |

Every macro number in [Benchmark Results](../performance/overview.md) is measured against an
already-warm pool. `Shoal::new` runs before the timer starts, so the ten connections it opens, the
ten handshakes they exchange, and — since [F12](../features/authentication.md) — the ten SCRAM
exchanges and twenty PBKDF2 derivations that go with them, are all invisible to every capture this
repository has taken.

That was defensible while a connection was a `TcpStream::connect` and a `set_nodelay`. It is
becoming less so with each thing added in front of the first query:

| Change | What it added per connection |
| --- | --- |
| [F10](../features/framing-and-protocol-evolution.md) | one round trip, two 24 byte frames |
| [F12](../features/authentication.md) | two more round trips and a PBKDF2 derivation on each end, when a config asks for it |
| [D4](../direction/encryption.md) | a TLS handshake, when it exists |
| [D6](../direction/connection-pool.md) | whatever a real health check costs on a connection that is being created |
| [F16](../features/client-builder.md) | nothing per connection — but a client with several endpoints may now try, and be refused by, more than one before it opens one |

**What is needed is not a query workload.** The `transport/*` workloads
~~[TODOs](todos.md#benchmark-coverage-the-harness-does-not-have) plans~~ —
**built** ([F13](../features/transport-workloads.md)) — still measure a warm
pool, because that is what they are for, so this entry is no more adjudicable than it was. This
wants time to first successful query from a cold
client, with `min_idle` as a parameter, run against a server with and without an `auth` section —
the second being a control-and-null pair in the sense [F4](../features/validated-archives.md)
settled on, where the axis is whether authentication happened at all.

[D3](../direction/authentication.md#how-it-would-be-measured) predicted this and predicted it
correctly, which is why it is filed here rather than argued: the rule this page opens with does not
stop applying once something has shipped.

**[F14](../features/encryption-in-transit.md) tried to close this and did not.** Its client sweep
opens *n* independent `Shoal` instances, each with its own pool and therefore its own handshakes,
on the theory that the per-connection cost would appear as the count rose. It does not, and the
reason is worth recording so nobody builds the same thing twice: **`bb8` fills `min_idle` inside
`Pool::build()`**, so all ten connections of every client have connected, framed and authenticated
before the constructor returns and long before the first sample is taken. The sweep measures steady
state with *n* warm pools.

Opening more connections is not the same experiment as timing one. This entry still wants a clock
around `Shoal::new` itself.

### O31. The disjointness rule cannot tell a result from a saturated workload

| | |
| --- | --- |
| **Rank** | **B** — not a speed change at all, a correctness change to how speed is judged |
| **Impact** | **Measured.** Four points of the `f14-encryption` capture report encryption making queries 14% to 47% *faster*, and every one of them passes the rule that decides whether a difference is real |
| **Difficulty** | S to detect, M to decide what to do about it |
| **Depends on** | nothing |
| **Blocks** | trusting any macro comparison taken near saturation |
| **Tradeoff** | Contained — a workload that is refused or flagged is one that produced a number nobody should have read |
| **Benchmark** | `macro/encryption/depth/*/128`, which is the thing that exposed it |

The macro layer calls a difference a result when the two sides' **observed intervals are disjoint**
— when the slowest run of one is still faster than the fastest run of the other. That is a good rule
and it is doing its job. What it cannot do is notice that the workload was not measuring what its
name says.

At a load depth of 128 the encryption sweep leaves the regime where a service time means anything.
Throughput *falls* as depth rises — 351,150 queries a second at depth 32 against 277,402 at depth
128, for 256 byte rows on the plaintext arm — which is the signature of a queue past its knee, and
the p50 stops being a latency and becomes a measure of how long the queue is. In that regime the
encrypted arm measured **faster**:

| Row | depth 32 | depth 128 |
| ---: | ---: | ---: |
| 256 B | +5.7% | **−23.5%**, separated |
| 4 KiB | +11.3% | **−46.8%**, separated |

Both of the depth-128 rows are cleanly separated across five runs. They are reliably weird, and the
rule detects *reliably* different, not *meaningfully* different.

**This is not an argument for dropping the rule**, which is the only thing standing between the page
and a curve drawn through noise. It is an argument that disjointness is necessary and not
sufficient, and that a workload has no way today to say "the number I just produced is outside the
regime I am for".

**Fix direction**, cheapest first. A workload could record its own **throughput against the previous
point on its axis** and flag a capture where more load bought less work — the data is already in the
artifact, since `wall_clock_ns` and the query count are both recorded, so this is an analysis
change and not a measurement one. Beyond that, a saturating sweep wants a declared knee: an axis
that stops where throughput stops rising, which is a property of the machine rather than of the
workload and would have to be found once and recorded.

Found while reading the first capture that held the sweeps, which is the only way it could have
been found — every point of it is correct, the harness did nothing wrong, and the numbers are still
not readable.

**Partly addressed by [F17](../features/workload-grid.md), and still open.** The grid ships a
four-rung **load depth ladder** at its reference cell — depths 1, 8, 32 and 128, identical in every
other respect — and [Access patterns](../performance/access-patterns.md) draws throughput and
latency against depth together and states in prose whether throughput fell at any rung. That makes
the knee *visible* for the one cell every grid arm's depth was chosen from, which is the first time
anything here could see it at all. What it does not do is either half of the fix direction above:
nothing computes the flag automatically, no axis carries a declared knee, and the ladder covers one
cell rather than every sweep. **The entry stays open.**

### O32. `Queries::deserialize` costs about nine nanoseconds more than it did

`shoal-core/src/server/shard.rs:1184`, `<Queries<D::ClientType> as RkyvSupport>::deserialize`

The server deserializes the whole request bundle once per client request. Measured before and
after [F15](../features/client-server-split.md) moved `Queries` and `RkyvSupport` into
`shoal-proto`:

| `wire_codec/request/decode/deserialize` | before | after |
| --- | --- | --- |
| 1 query | 29.52 ns | 38.28 ns (+29.7%) |
| 10 queries | 400.12 ns | 423.43 ns (+5.8%) |
| 100 queries | 5.79 µs | 5.89 µs (+1.7%) |

**Measured, and reproduced.** A second capture on the same tree put the one-query case at 38.91 ns,
so it is not a noisy reading. The cost is roughly constant in absolute terms and dilutes as the
bundle grows, which is the signature of a fixed per-call overhead rather than a slower loop — the
shape a function that stopped being inlined leaves.

The obvious cause is not the cause. `RkyvSupport::serialize` and `deserialize` are default trait
bodies that moved crates, so both were given `#[inline]`; that recovered `encode/serialize/1`
(70.58 → 64.04 ns, outside the noise band) and moved `deserialize/1` not at all. Whatever this is,
it is not the trait method's own inlining.

**It does not show up end to end.** The macro layer over the transport and fanout workloads has no
reproducible movement — see F15's Performance section — and nine nanoseconds against a p50 get of
roughly 30 µs is about 0.03%. It is filed because it is real and unexplained, not because it is
urgent.

**Where to start:** compare the generated code for `<Queries<S> as RkyvSupport>::deserialize`
across the boundary; check whether rkyv's `Pool` allocation is being hoisted differently; and note
that [item 66](known-issues.md) means there is no LTO to hide any of this, so whatever it is would
likely vanish under `lto = "thin"` — which is itself worth measuring before chasing this further.

### O33. The archives are written with glommio's defaults, and nothing can tune them

| | |
| --- | --- |
| **Rank** | **C** — measured. The sweeps that would show the wiring are flat, so the defect is confirmed and the *value* of fixing it is still unknown |
| **Impact** | Unknown. The bulk write path uses whatever `DmaStreamWriterBuilder` defaults to, and the setting that appears to govern it does not |
| **Difficulty** | S — thread `&self.conf` into two branches of one function |
| **Depends on** | [item 71](known-issues.md), which is the same finding as a defect |
| **Blocks** | any tuning advice about bulk ingest |
| **Tradeoff** | None known. It is a setting that already exists reaching code it already names |
| **Benchmark** | `macro/conf/storage/throughput_buffer/*` and `macro/conf/storage/throughput_write_behind/*` ([F20](../features/configuration-sweeps.md)) |

`ArchiveMap::get_active_writer` builds a `DmaStreamWriter` with no `with_buffer_size` and no
`with_write_behind`, in both its branches (`map.rs:415`, `:433`), while `new_writer` twelve lines
above configures the map's own intent log from `throughput_sensitive`. So the archives — the actual
bulk data — are written at glommio's default buffer size and queue depth, and the only thing
`throughput_sensitive` reaches is a small latency-shaped write.

**Argued from reading the source, and the reading is now confirmed.** The `F20-conf` capture of
2026-08-22 swept both settings and both came back at **1.01×**, failing the *Real?* gate — which is
what a setting that never reaches the code it names looks like from the outside. What makes this
worth an `O` number rather than only a defect is
that the default may well be *wrong* for the workload: the archive writer is the one place in the
engine that streams whole compacted partitions, which is exactly the case a deep queue and a large
buffer exist for, and it is running at whatever a general-purpose default chose.

**How to adjudicate it.** The two sweeps named above *were* flat, and that flatness is
[item 71](known-issues.md)'s evidence. Half of this entry is therefore settled: the wiring is broken
as described. The other half is not, and cannot be until the wiring is fixed — flat sweeps say
nothing about what the setting would be worth if it reached anything. Fix it, re-run
`--group conf/storage` against `F20-conf` as the before, and the same two sweeps say whether the
setting is worth turning — if they are still flat afterwards, glommio's default was fine all along
and this entry closes as measured-and-declined rather than as taken. Nineteen minutes of machine
time settles it either way.

### ~~O26. `handle_query` cloned a `QueryMetadata` for a gather almost no query has~~

| | |
| --- | --- |
| **Rank** | **B** — measured as free, taken because it was free, not because it was ranked |
| **Impact** | One `QueryMetadata` clone per query removed — 617,175 per baseline run |
| **Difficulty** | S — one line, already taken |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | None — the clone was only ever read on a path that checked the same condition |
| **Benchmark** | none of its own; folded into no F6 number, see below |

**Done**, by [F6](../features/stage-breakdown.md) — filed and taken in the same change, which is why
it carries no before-and-after of its own. Struck through late: it was taken long before it was
marked, and the page's own convention says a done entry is struck and kept.

`Shard::handle_query` (`shard.rs:695`) cloned the whole `QueryMetadata` before handing it to the
tables:

```rust
// keep a copy of our metadata, since handling this query consumes it and a
// share of a split query has to travel back with the metadata it came from
let gathered_meta = meta.clone();
```

The copy is read in exactly one arm — the one taken when `meta.gather` is `Some`, meaning the
query was split across shards. Every other query paid for a clone of a `Uuid`, a `Uuid`, a
`usize`, a `bool`, an `Option<ShardContact>` and a `Span` and then dropped it. In the `tmdb`
workload **no query is ever split**: `MovieGet::new(vec![movie.id])` names one partition, so
`found.len() == 1` and `gather` is `None` for all 617,175 of them.

It is now cloned only when there is something to clone it for:

```rust
let gathered_meta = meta.gather.is_some().then(|| meta.clone());
```

F6 made this worth doing rather than merely tidy: [`StageStamps`](../features/stage-breakdown.md)
rides on `QueryMetadata`, so under a profiling build the clone got bigger, and a stage profile
that pays for its own instrumentation on a path it is measuring is the thing to avoid.

**Deliberately not measured as part of F6's capture.** An F6 run is a `stage-profile` build,
whose absolute latencies are not comparable to a shipping one, so folding a shipping-build
optimization into that capture would produce a number that means nothing. It needs its own
before-and-after macro capture against the frozen baseline, which has not been taken.

### ~~O36. Every get re-collects its rows into a fresh `Vec`, even when it read one partition~~

**Done**, by [F27](../features/grouped-responses.md), and it took the fix this entry proposed:
`GetRows::from_slots` hands over the first run rather than copying it, so a get that read one
partition answers with the `Vec` the scan already filled. It was `S`, as filed, and it was done
because [O18](#o18-the-gathered-reorder-rehashes-every-rows-partition-key) rewrote `finish` anyway
— which is the argument for taking near-free entries with whatever reaches their code rather than
on their own.

The original entry read:


| | |
| --- | --- |
| **Rank** | **A5**, beside O5, O14 and O28 — near-free, and on the path every get takes |
| **Impact** | Argued — one allocation and one full walk of the row set per get, including the single-partition case where there is nothing to merge |
| **Difficulty** | **S** — a length check and an `into_iter().next()` on the one-slot path |
| **Depends on** | a table-layer bench, the same one O5, O12 and O13 want |
| **Blocks** | nothing |
| **Tradeoff** | None |
| **Benchmark** | none yet. `macro/fanout/*` drives the many-partition path this is *not* about; what would show it is the single-partition get, which no isolated bench reaches |

`PendingGet::finish` flattens its slots into the rows a get answers with:

```rust
// collect every row we found, partition by partition, in the order they were named
let mut data: Vec<R> = self.slots.into_iter().flatten().flatten().collect();
```

`shoal-core/src/server/tables/persistent.rs:196-198`, `PendingGet::finish`

The slots are `Vec<Option<Vec<R>>>`, one per partition the query named, and the `collect` allocates a
new `Vec` and moves every row into it. That is the right shape when a get named several partitions
and their rows have to be concatenated in the order they were named. **It is the overwhelmingly
common case that it is wrong for**: a get naming one partition has one `Some(rows)`, and those rows
are already in the order and the container the caller wants. The walk is shallow — row structs are
memcpyd, their heap contents are not — but the allocation is per query and the walk is O(rows).

**Why it is not on the six-walk list.** [Row size and what it costs](../tables/row-size.md) counts
the copies that are O(*bytes*); this one is O(*rows*), so it does not grow with the row width and it
does not appear on that page's table. It grows with **cardinality** instead, which is the axis
`macro/fanout/*` and `wire_codec/response/*` sweep. That makes it a different entry from
[O2](#o2-every-returned-row-is-copied-at-least-twice--the-resident-half-is-done-done) rather than a part of it, and it is why it went
unfiled while four entries were written about the bytes.

**Found while tracing the response path end to end** for the row-size page's copy accounting, which
had never been walked in code against the source it was derived from.

### ~~O37. The client zeroes a response buffer and immediately overwrites it~~

**Done**, by [F25](../features/read-buffers-are-filled-not-zeroed.md), with
[O29](#o29-a-request-body-is-zeroed-and-then-immediately-overwritten). Taken as the second of the
two options under *Difficulty*: `ReadBuf::uninit` over the allocation, and `set_len` only once
`ReadBuf` reports the whole of it filled. **This was the larger half of the pair** — an
unconditional write at every size over the larger payload, against a server side that was calloc
and therefore only sometimes a write at all.

| | |
| --- | --- |
| **Rank** | ~~**B4**, beside O29 — the same defect on the other end of the same round trip~~ — **done** |
| **Impact** | **Asymptotic** in the row width — one `memset` of the whole response payload per response, discarded by the `read_exact` on the next line |
| **Difficulty** | S–M — `read_buf` over `MaybeUninit`, or `set_len` after a checked read |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Contained — but it reaches `unsafe` if taken as `set_len` |
| **Benchmark** | ~~none yet, and the client is the half nothing measures. `wire_codec/width/response/decode` measures the decode and not the read that precedes it; a stage capture would put it in `net_in`~~ — **built**, as `wire_codec/width/response/body/{zeroed,uninit}`, which runs both shapes in one build so a single capture adjudicates it. The stage note was right about `net_in` and is worth keeping for that reason: it is where O29 turned out to live too, and no stage separates either of them from wire time |

```rust
// Create an aligned vec to act as a pool of bytes
let mut aligned_buff = AlignedVec::<16>::with_capacity(frame.rest_len);
// resize our aligned vec
aligned_buff.resize(frame.rest_len, 0);
self.reader.read_exact(&mut aligned_buff).await?;
```

`shoal-client/src/client.rs:1489-1492`, `TcpProxy::read_frame`

This is [O29](#o29-a-request-body-is-zeroed-and-then-immediately-overwritten) exactly, with the
client reading a response where the server reads a request: a full write of zeroes over a buffer
whose every byte is overwritten before it is read. O29 was filed against `BytesMut::zeroed` on the
server and never mentioned that the client does the same thing to the larger of the two payloads.

**What must not be done to fix it.** The two `read_exact` calls are deliberate and are guarded by
two tests — `the_response_payload_lands_on_a_sixteen_byte_boundary` and
`an_error_frame_does_not_disturb_the_response_read`. Reading the 24-byte preamble and the payload
into one buffer would land the archive at offset 24 and silently destroy the alignment the whole
zero-copy read depends on. The fix is to stop zeroing, not to stop splitting the reads.

### O38. A response that arrives out of order is validated twice

| | |
| --- | --- |
| **Rank** | **A5** — small, contained, and on a path that already exists |
| **Impact** | **Asymptotic** in the row width — a second full `bytecheck` traversal of the whole payload, per out-of-order response |
| **Difficulty** | S — hold the wrapped response in the reorder map instead of the raw buffer |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | None |
| **Benchmark** | `macro/transport/stream/*` against `macro/transport/stream_unordered/*` — the unordered mode never reorders, so the pair is a control. Neither is swept at a width that would make the per-byte half visible |

`ShoalResultStream::next` builds a `ShoalResponse` to find out where a response belongs:

```rust
// wrap our response so we don't have to keep repaying access costs
let response = ShoalResponse::<S>::new(response, stamps)?;
// get the index for this message
let index = response.get_index();
```

`shoal-client/src/client.rs:2020-2023`

`ShoalResponse::new` runs the **validating** `rkyv::access` (`shoal-proto/src/shared/traits.rs:61`),
which walks every row, every string bound and every relative pointer. If the index is the one the
stream wants, that walk is paid once and the comment above it is correct. If it is not, the response
is torn back apart into raw bytes and re-wrapped:

```rust
let (buff, stamps) = response.inner();
let rewrapped = ClientMsg::Response(buff, stamps);
self.pending.insert(index, rewrapped);
```

`shoal-client/src/client.rs:2043-2046`

and `ShoalResponse::new` — and the whole traversal — runs **again** when it is popped
(`client.rs:1986`). The comment *"so we don't have to keep repaying access costs"* describes what
the code does on the fast path and the opposite of what it does on the slow one.

Two ways out, both small: keep the `ShoalResponse` in the `BTreeMap` rather than the `AlignedVec`,
or read the index out of the frame so the archive is never touched until the response is returned.
The first is the smaller change; the second is what would let a response be validated exactly once
whatever order it arrives in.

**Affects `send()` and `stream()` and not `stream_unordered()`**, which returns responses as they
land and never reorders — which is also what makes the pair a ready-made control.

**Found while tracing the response path end to end**, with O36 and O37.

### O39. Routing a multi-partition get is quadratic before the query reaches a table

| | |
| --- | --- |
| **Rank** | **A4**, beside O13 — the same asymptotic in the same caller-set *n*, one layer earlier |
| **Impact** | **Measured** — a quadratic term of **0.0109 ns·n²** beside a linear **4.13 ns·n**, fitted to the two widest points of `routing/split_by_shard/get`. That is 3% of the split at *n* = 16, **14.5%** at 64, **40%** at 256, and a projected 73% at 1024 |
| **Difficulty** | **S** — a `HashSet<u64>` of placed keys, or sort-and-dedup the key list once |
| **Depends on** | ~~a `routing` bench, which is unbuilt~~ — **discharged**, built as [F24](../features/routing-benchmarks.md) |
| **Blocks** | nothing |
| **Tradeoff** | None — the dedup and the ordering both survive |
| **Benchmark** | `routing/split_by_shard/get` at *n* ∈ {1, 2, 4, 16, 64, 256}, against `routing/split_by_shard/write` as the control ([F24](../features/routing-benchmarks.md)). **Built and run.** The control is flat to **0.3%** across all six points at 2.72 ns, so every nanosecond the get arm moves is the key count and not the call. `macro/fanout/*` sweeps the same *n* end to end and cannot attribute anything here |

`group_by_shard` places each partition key with the shard that owns it, and deduplicates by
scanning every key it has already placed:

```rust
// a key we have already placed names a partition we are already reading
if grouped.iter().any(|(_, keys)| keys.contains(key)) {
    continue;
}
```

`shoal-core/src/server/routing.rs:36-40`, `group_by_shard`

`grouped` holds one `Vec<u64>` per shard, and `keys.contains` is a linear scan. So placing the
*i*-th key costs a walk over the *i*−1 keys already placed, and routing a get naming *n* partitions
costs **O(n²)** comparisons — before the query is serialized to a shard, before a table is reached,
and on the coordinating shard that every share of that query passes through.

The `find` on the next lines is a second linear scan, over shards rather than keys. That one is
bounded by the core count and is not the problem.

**Why this is not [O13](#o13-a-multi-partition-get-is-quadratic-in-the-partitions-it-names).** O13
is `PendingGet::rank` and `filled_before`, inside a table, on the shard that answers a share. This
is `group_by_shard`, in `routing.rs`, on the shard that splits the query. They are the same
asymptotic in the same *n* — a number the caller chooses directly, by naming partition keys — at two
different layers, and a fix for either leaves the other. **A `macro/fanout/n` curve that bends
therefore has two candidate causes and cannot tell them apart**, which is worth knowing before that
curve is used as evidence for O13: the note under
[the adjudication table](#which-entries-a-benchmark-can-currently-adjudicate) says `fanout` answers
O13's *question* rather than its cost, and this entry is a second reason that is true.

**The fix is smaller than O13's.** The keys only need to be deduplicated, and the order within a
shard preserved — a `HashSet<u64>` of what has been placed does both and changes nothing else. The
ordering guarantee this function carries, which
[Resolved #26/#39](resolved/partition-order.md#invariants-to-uphold) depends on, is a property of the
per-shard `Vec` and not of the dedup scan.

**Established by reading the source**, while tracing the response path for the copy accounting on
[Row size and what it costs](../tables/row-size.md). It was found by looking for something else,
which is the usual way, and it had gone unfiled because the routing layer had no benchmark pointing
at it. **It has one now** ([F24](../features/routing-benchmarks.md)), written in the same change,
and the entry was measured before it was ranked rather than after.

**What the measurement says, including the part that argues against acting on it.** The curve is
superlinear and the control is flat, so the quadratic is real:

| *n* | `split_by_shard/get` | per key | quadratic share |
| ---: | ---: | ---: | ---: |
| 1 | 11.32 ns | 11.32 ns | 0.1% |
| 2 | 35.10 ns | 17.55 ns | 0.1% |
| 4 | 38.02 ns | 9.51 ns | 0.5% |
| 16 | 93.06 ns | 5.82 ns | 3.0% |
| 64 | 309.08 ns | 4.83 ns | **14.5%** |
| 256 | 1.774 µs | 6.93 ns | **40.4%** |

**But routing is nanoseconds against a query that costs tens of microseconds.** At *n* = 256 — the
widest arm `macro/fanout` runs — the whole split is 1.77 µs, of which 717 ns is the quadratic, against
a read service time of roughly 40 µs. Removing it entirely would buy under 2% of that query and
nothing at all of a query naming one partition, which is almost all of them. **So this is a correct
entry that does not deserve a high rank**, and the honest reading is that the axis has to reach
*n* = 1024 before the term dominates its own function, by which point the query is doing a thousand
partition reads that dwarf it anyway.

It is filed at **A4** beside O13 because it is S-sized, has no tradeoff, and is a genuine asymptotic
in a caller-set quantity — not because a capture is waiting on it. The more useful thing the
measurement bought is the warning above: **`macro/fanout`'s curve now has two known quadratics under
it**, and anybody about to read that curve as evidence for O13 has to subtract this one first.

**`find_shard` is not the problem, and that is now measured too.** `routing/find_shard` is **flat at
392 ps across 1, 4, 12 and 64 shards** — 0.4% of spread across a 64× change in the ring — which is
the tablet ring answering in constant time exactly as
[Resolved #11/#12/#37](resolved/tablet-ring.md) said it would. The lookup this entry calls once per
key is not what makes the function quadratic; the dedup scan around it is.

---

### ~~O40. A row read out of an archive is materialized before it is re-serialized~~

**Done**, by [F28](../features/rearchived-rows.md), and with it
[O2](#o2-every-returned-row-is-copied-at-least-twice--the-resident-half-is-done-done). An unprojected get whose partition is still
an archive now points at each archived row where it lies and serializes the reply straight from
those pointers. The missing direction — a serializer from an archived value back into its own
layout — is generated per row type by `shoal-derive`, field by field, against rkyv's own archived
struct.

**Measured at −95.5% and −95.8%** on `partition_sorted/maybe_loaded/get_all` at 1024 and 4096 rows
(`f28-rearchive`), against a `build_all` arm carrying the old behaviour in the same build.

**Two things this entry got wrong, both worth keeping.** The **XL** grade rested on the derive
having to recurse through field types it cannot see; the answer was to stop trying, and fall back
**per field** rather than per row — a type the derive cannot see inside materializes that one
field, and every field around it is still written out of the archive. Nothing is refused, no schema
stops compiling, and the change came out **M**. And the entry did not mention `Option`, which is
the one shape that could not be written by hand at all, because `ArchivedOption`'s tag type is
private to rkyv; it is served by substituting `ArchivedRef<'_, T>` into rkyv's *own* generic impl,
which is the trick that makes the whole thing compose.

**The last paragraph of the entry — that item 80 left the sorted table with no control — was
overtaken** by [Resolved #80](resolved/never-flushed-partitions.md), which landed first. Both
tables reach both paths now, and both are tested for it as counts.

The entry as filed:


| | |
| --- | --- |
| **Rank** | **C1a**, inheriting [O2](#o2-every-returned-row-is-copied-at-least-twice--the-resident-half-is-done-done)'s place — the open half of the largest established win |
| **Impact** | **Measured per byte** as part of O2: the response codec grows ×432.8 on decode and ×72.2 on encode over 64 B → 64 KiB, and the `f24-routing` stage breakdown puts `execute` at ×65 over 1 KiB → 512 KiB. What is *not* separated is how much of that belongs to the archived path rather than the resident one, because no capture distinguishes them |
| **Difficulty** | **XL** — a per-row-type re-serializer emitted by `shoal-derive`, recursing into field types it cannot see |
| **Depends on** | nothing that is missing; the design question is the whole of it |
| **Blocks** | nothing |
| **Tradeoff** | Contained — no wire format change. The bytes are the same either way, which is the property [F27](../features/grouped-responses.md) already relies on |
| **Benchmark** | `partition_sorted/maybe_loaded/get_all` against `partition_sorted/get_all` — the archived scan against the resident one — is the pair that would price it, and both already exist. What is missing is an arm that runs the same rows both ways in one build, the way [F25](../features/read-buffers-are-filled-not-zeroed.md) ran `body/{zeroed,uninit}` |

`MaybeLoaded::collect_archived` builds an owned row per row returned:

```rust
found.push_built(P::from_archived(row));
```

`partitions.rs`, and the unsorted twin beside it. [F27](../features/grouped-responses.md) removed
the same copy from the resident path by pointing at the row instead
(`found.push_resident(identity(row))`), and cannot do it here, for a reason worth stating precisely
because it is not a matter of effort.

**rkyv has no way to serialize an archived value back into its own layout.** There is no
`impl Archive for ArchivedString`, none for `ArchivedVec`, and none for any type the derive
generates. The only archived types that are re-serializable are the ones whose archived form is
themselves — rkyv's own `rend` scalars, `u8`, `i8`, `bool`, `()`. Verified against
`rkyv-0.8.12/src/impls/`.

The pieces to build one exist, one level deep. `ArchivedString::serialize_from_str`
(`string/mod.rs:81`) writes exactly what `impl Archive for String` writes;
`ArchivedVec::serialize_from_slice` (`vec.rs:90`) does the same for a slice whose element archives
to itself. So a mirror is writable field by field for a row of scalars, `String`s and
`Vec<u8>`-shaped collections.

**Where it stops is `Vec<Tag>` for any `Tag` the schema declares elsewhere.** `ArchivedVec<ArchivedTag>`
cannot be borrowed back into anything serializable, and `shoal-derive` sees only the *syntax* of a
field's type — it cannot look inside `Tag`, which may live in another crate. So the derive would
have to emit a re-serializer that recurses through types it cannot enumerate, or the feature would
have to be refused for any row with a nested user type, which is a rule a schema author would hit
without warning.

**Two things make this smaller than it looks.** The first is that only the *first* copy is at
stake: the archived path loses `PendingGet::finish`'s re-collect and the gather's rehash the same
way the resident path did, since those are [O36](#o36-every-get-re-collects-its-rows-into-a-fresh-vec-even-when-it-read-one-partition)
and [O18](#o18-the-gathered-reorder-rehashes-every-rows-partition-key) and both are done. The
second is that a projection is not covered by this or by O2 either way — a projection is a strict
subset of its row and has to be built whatever the row is held as.

~~**And it is not reachable on the sorted table today for an unrelated reason.**
Item 80
keeps `check_disk` set on any sorted partition that was never written to disk, which refuses that
table the resident path as well. Fixing that is `S` and would be worth doing before anyone prices
this one, because until it is fixed the sorted table's archived path is the *only* path it has and
the comparison has no control.~~ Item 80 is fixed
([Resolved #80](resolved/never-flushed-partitions.md)): a sorted partition that was never written to
disk has its `check_disk` cleared, which is what refused the sorted table the resident path.

---

### O41. Reordering a gathered get allocates a `Vec` per partition

| | |
| --- | --- |
| **Rank** | **A5**, beside the other near-free entries on paths every split get takes |
| **Impact** | **Measured** — it is what is left of `order_by` at high group counts, and it is why [O18](#o18-the-gathered-reorder-rehashes-every-rows-partition-key)'s win falls from ×15.8 at four partitions to ×1.16 at 256 |
| **Difficulty** | **S** — a different way of moving the runs, in one function |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | None |
| **Benchmark** | **Built and it is what found this**: `wire_codec/response/gather/{hash,groups}`, 1024 rows swept across 1, 4, 16, 64 and 256 partitions ([F27](../features/grouped-responses.md)) |

`GetRows::order_by` puts the runs of a merged get back into the order the query named their
partitions in. It ranks the groups — which is the part that replaced hashing every row — and then
moves the rows, like this:

```rust
// cut the rows into their runs, from the back so each cut is the tail of what is left
let mut rows = std::mem::take(&mut self.rows);
let mut runs: Vec<Vec<T>> = Vec::with_capacity(self.groups.len());
for group in self.groups.iter().rev() {
    let at = rows.len() - group.len as usize;
    runs.push(rows.split_off(at));
}
```

`shoal-proto/src/shared/responses.rs`, `GetRows::order_by`

**One allocation per group**, plus one per `append` back into the output. The moves themselves are
O(n) in the rows and unavoidable — the rows genuinely have to be permuted — but the allocations are
O(*groups*) and are not.

**This is the whole of what is left at the wide end.** The captured numbers on
[F27](../features/grouped-responses.md#performance) put the grouped reorder at 1.02 µs against the
hashing one's 16.08 µs over four partitions, and at 15.79 µs against 18.29 µs over 256 — same rows,
same total moves, 256 allocations instead of four.

**Two ways out, and the obvious one is worse than it looks.** Collecting into a `Vec<Option<T>>`
and taking each row out in ranked order is one allocation total, and costs `size_of::<Option<T>>()`
per row instead of `size_of::<T>()` — for a wide row with a niche that is free, and for one without
it is a whole extra tag per row on a path that exists to stop copying wide rows about. The better
shape is probably to compute the permutation and apply it in place with a cycle walk, which needs
no second buffer at all and is a well-known routine; the reason it is filed rather than done is
that it is fiddly to get right and the payoff is bounded by a case — many partitions holding few
rows each — that the fan-out workload drives and the grid does not.

**Worth taking with whatever next touches this function**, which is the same argument
[O36](#o36-every-get-re-collects-its-rows-into-a-fresh-vec-even-when-it-read-one-partition) was
eventually closed under: it was `S` for four features and was done in an afternoon by the change
that rewrote the code around it.

---

### O42. A get replayed after a disk read copies rows its partition is now holding

| | |
| --- | --- |
| **Rank** | **A5**, with the other near-free entries — one flag on one branch, on a path every cold read takes |
| **Impact** | **Argued, and bounded to one get per partition read.** It is the full cost [O2](#o2-every-returned-row-is-copied-at-least-twice--the-resident-half-is-done-done) used to be — a deserialize and a serialize per row — paid once, on the first get after a partition comes off disk. Every get after it is answered out of the archive |
| **Difficulty** | **S–M** — the flag is one line; what it needs is for a replayed get to be able to answer in place, which is a question about `PendingGets` rather than about rows |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Contained — no wire format change, the same as F27 and F28 |
| **Benchmark** | None, and the micro layer cannot reach this: it is about *which* path a running server takes, not what either path costs. `macro/get_sorted` against a cold table is the shape that would show it |

A get naming a partition that is on disk and not resident parks on the read and is replayed once
the load lands. `can_answer_in_place` refuses the sealed path to any get that has parked:

```rust
// a get that has already parked holds rows from an execution that has ended
if self.pending_data.is_parked(&(meta.id, meta.index)) {
    return false;
}
```

`persistent/sorted.rs`, and the unsorted twin beside it

That is correct as written — a parked get's earlier executions found rows that have to outlive the
execution that found them, so they are owned by definition. But by the time of the replay the
partition it was waiting for is `Accessible`, and its rows are exactly the rows
[F28](../features/rearchived-rows.md) can now write straight out of the archive. So the get that
paid for the disk read also pays the old copy, and the getters behind it do not.

**Found by probing rather than by reading.** With a probe on `RowSink::push_archived` and another
on `RowSink::into_owned`, the first get off disk reaches both and the second reaches only the first.
Nothing failed and no test could have caught it, because both paths answer identically — the same
trap that hid [item 80](resolved/never-flushed-partitions.md) inside F27, which is why the probe
was run at all.

**The shape of a fix.** A replay whose *only* outstanding partition is the one that just loaded
holds no rows from an earlier execution — `PendingGets` is empty for it — so it could take the
sealed path unchanged. That is a narrower condition than `is_parked` and is cheap to test for. A
get that parked on several partitions genuinely cannot, and should keep copying.

---

### O43. A borrowed row costs a discriminant it usually does not need

| | |
| --- | --- |
| **Rank** | **A5**, with the near-free entries — but it is a **regression this repository introduced**, not a cost that was always there |
| **Impact** | **Measured, in isolation, back to back on one machine**: `wire_codec/response/build/borrowed` is **+34.1%** at 16 rows, **+33.8%** at 256, **+38.5%** at 1024 and **+40.4%** at 4096, comparing `a1b0cff` against [F28](../features/rearchived-rows.md) |
| **Difficulty** | **M**, and possibly not worth taking — see below |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Any fix has to keep one `Vec` holding rows of both kinds, which is what the enum is for |
| **Benchmark** | **Built**: `wire_codec/response/build/{owned,borrowed,archived}` at 16, 256, 1024 and 4096 rows |

[F28](../features/rearchived-rows.md) made `RowRef<'a, T>` a two-variant enum so that one reply can
carry rows pointed at in a resident partition beside rows pointed at in an archive. A get across
several partitions is routinely a mixture of the two, so the reply cannot be homogeneous.

The cost is paid per row on **every** borrowed reply, including the ones with no archived row in
them at all:

- the `Vec` element is **16 bytes instead of 8** — a pointer and a discriminant, padded — so a
  4096-row reply writes 32 KiB more than it needs to;
- `resolve` and `serialize` each gained a match, with an arm that is `unreachable!()` because the
  value and its resolver must be the same variant. That arm is per row.

**It is not a reason to revert anything.** The resident path is still **6.3×** faster than copying
the rows first (35.5 µs against 224.2 µs at 4096 rows), which is the whole of what
[F27](../features/grouped-responses.md) bought; this gives back a third of the margin on top of that
win, in exchange for making the archived path 24× faster. But it is a real cost on the path most
gets take, and it went in without being noticed until the arm that prices it was read.

**Two things to try, in order.** First, check whether it is the discriminant or the panic: the
unreachable arm may be blocking inlining or dragging panic machinery into a per-row loop, and
restructuring so the match is on the resolver alone would settle that without changing the layout.
Second, if it is the layout, there is no obvious safe fix — `&T` and `&Archived<T>` are both
non-null so no niche is available for a two-pointer-kind enum, and pointer tagging would depend on
an alignment a `#[repr(C)]` row of bytes does not have to give. Measure the first before designing
for the second.

### O44. One trace per request costs a span per query and one per frame

| | |
| --- | --- |
| **Rank** | **A7** — beside [O25](#o25-two-instrument-spans-remain-on-per-query-paths), and unrankable for the same reason |
| **Impact** | Argued — one extra registry slab insert per query and one per frame, at `level: Info` |
| **Difficulty** | S — the cost is one `info_span!` call, and lowering it means losing what it bought |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | **Not contained** — every span on the query path re-parents off `Coordinator::route`, so removing it fragments the trace rather than shortening it |
| **Benchmark** | none, and the honest reason is [O25](#o25-two-instrument-spans-remain-on-per-query-paths)'s: the cost is in the baseline binary and absent from nothing in `docs/perf/` |

Filed by [Resolved #89](resolved/fragmented-query-traces.md) against itself, because a fix that
adds work to the per-query path should say so on this page rather than in its own *Performance*
section alone.

What that change did to the per-query cost, at `level: Info`, which is what the committed
`shoal.yml` names:

| Span | Per | Net |
| --- | --- | --- |
| `Coordinator::route` | query | **replaces** `Coordinator::send_to_shard`'s, which was per bundle |
| `Shoal::request` | frame | one more |
| the loader's two | partition read | none — they existed, and only gained parents |
| the response write | response | none — covered by entering the query's own span, which is what the relay already did |

So for a bundle of one, which is the common case and every arm of the isolating set, the query path
is **one span heavier** — the request root. For a bundle of *n* it is *n* heavier, because the span
that used to be shared is now per query. [F5](../features/flushed-sweep-gate.md) counted 711,638
slab inserts per run for a single per-message span, which is the order of magnitude to hold this
against.

**The cheap fix is the wrong one.** Dropping `Coordinator::route` to `DEBUG` would cost nothing at
`Info` and would silently re-root every span beneath it into its own trace, because `tracing` turns
an empty parent into a new root rather than an orphan — the exact defect item 89 fixed, reintroduced
by a level. Every span on this path has to sit at one level, because every one of them is a parent
to something. A *leaf* could safely sit lower, and item 89 built one — a span around the socket
write — then removed it, because the query's own span already covers those instants.

**What would actually settle it** is the pair of runs [O25](#o25-two-instrument-spans-remain-on-per-query-paths)
asks for and nobody has taken: one capture at `level: Off` and one at `level: Info`, on the same
commit. That measures both entries at once, which is a reason to take it once rather than twice.
Until then this is argued, and the number to argue against is F5's.

**The trace this prices now spans two processes.** [F35](../features/wire-trace-context.md) joined
the client's half to the server's, which changed nothing in the table above — every span here is on
the server and none of them moved — and added two more on the *client's* per-query path. Those are
[O45](#o45-the-clients-return-half-costs-two-spans-per-response) rather than more rows here,
because they are in a different process and a different crate, and because the run that would
settle them is the same one this entry has been waiting for.

### O45. The client's return half costs two spans per response

| | |
| --- | --- |
| **Rank** | **A7** — beside [O44](#o44-one-trace-per-request-costs-a-span-per-query-and-one-per-frame) and [O25](#o25-two-instrument-spans-remain-on-per-query-paths), and unrankable for the same reason |
| **Impact** | Argued — two extra registry slab inserts per **response** at `level: Info`, in the client |
| **Difficulty** | S — the cost is two callsites, and removing either loses what it bought |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | **Not contained** — `Shoal::response` is what carries a query's answers back into the trace its send opened, so removing it does not shorten a trace, it truncates one |
| **Benchmark** | none, and for the same reason as O44: the cost is in the baseline binary and absent from nothing in `docs/perf/` |

Filed by [F35](../features/wire-trace-context.md) against itself, the way O44 was filed by
[Resolved #89](resolved/fragmented-query-traces.md). A feature that adds work to a per-query path
says so here rather than in its own *Performance* section alone.

What it added, per **response** rather than per query — which for a bundle of *n* is *n* times, and
for a stream is once per row group:

| Span | Per | Net |
| --- | --- | --- |
| `Shoal::response` | frame read back | one more, in `TcpProxy::relay` |
| `ShoalResultStream::next` | response handed to the caller | one more |
| `Shoal::send_stamped` | bundle | none — it existed, and only started being parked in the `Waiter` |
| the trace context itself | traced frame | not a span: 26 bytes and one `read_exact`, and only when the client is tracing |

**This is on the client, which is the half that is meant to be cheap.** `shoal-client` links no
engine, and the 374 workloads drive it as hard as they drive the server — so two spans per response
is the same order of magnitude as O44's, arriving in the process that has the least other work to
hide it. F5's 711,638 slab inserts per run for one per-message span is the number to hold it
against, as it is for O44.

**The cheap fix is the wrong one, in the same way.** Dropping `Shoal::response` to `DEBUG` would
cost nothing at `Info` and would re-root every response into a trace of its own, since `tracing`
turns an empty parent into a new root — the defect item 89 fixed and
[item 90](resolved/divergent-layer-filters.md) fixed from the other side, reintroduced by a level.
`ShoalResultStream::next` is the one of the two that *is* a leaf and could safely sit lower; it is
at `INFO` because a query whose answer took a long time to be collected is exactly what somebody
reading a trace is looking for, and a leaf nobody can see is not worth a callsite either.

**What is genuinely contained here** is that `tracing.level` still decides all of it. The committed
`shoal.yml` names `Warn`, at which neither callsite is enabled and both cost a filter check. This
entry is about what a deployment at `Info` pays, which is the level
[F34](../features/benchmark-tracing.md) made a capture honor.

### O46. The shared WAL is a buffered file, where the intent log was direct I/O

| | |
| --- | --- |
| **Rank** | **B5** — argued and contained, waiting on the replication arms' capture |
| **Impact** | Argued — a page-cache copy per batch on the write path and a page-cache read per segment scan, on a node whose quorum cost is the fsync those pages precede |
| **Difficulty** | M — the frame format is fixed, but a batch of frames from many groups is not block aligned, and a DMA writer needs padding the reader has to skip |
| **Depends on** | a capture of `macro/cluster/replication/durable` on the benchmark host |
| **Blocks** | nothing |
| **Tradeoff** | Contained — the frame and the store's contract do not move; the alignment does |
| **Benchmark** | `macro/cluster/overhead/nodes/3` against `macro/grid/unsorted/r50/1024` at matched shards, which does not exist yet ([todos](todos.md#distribution)) |

Filed by [F40](../features/replication.md) against itself. The standalone intent log writes
O_DIRECT through a `DmaStreamWriter` that rounds every buffer up to the device's alignment
([F23](../features/self-sizing-staging-buffer.md)); the shared WAL writes a batch of frames from
every group a shard hosts with one `write_at` on a `BufferedFile` and one `fdatasync`, and reads a
sealed segment for compaction and for openraft's replication below the durable watermark with
`read_at` on the same file. The choice was made for the sync: what a quorum counts is the
`fdatasync`, a batch is whatever appended while the last one was in flight, and padding every
batch to an alignment so that DMA could take it would spend the bytes the batch was meant to save.
What it costs is a kernel copy per batch and a page-cache read the archive reader avoids -
neither is measured, and the write path this sits on is
[waiting on the device](../performance/baseline.md#profile--where-the-time-goes) by thirty
milliseconds a call on the benchmark host, which is why this is Tier B and not Tier A.

### O47. A follower's fsync may be waiting for the leader's rather than running beside it

| | |
| --- | --- |
| **Rank** | **B6** — argued from one smoke run, waiting on the capture that would show it |
| **Impact** | Indicated — the durable quorum's median was 2.1× the single-copy median on the development host, which is what two syncs in series cost and not what two in parallel do |
| **Difficulty** | S to establish, unknown to change — the order is openraft's, and the shared device is the host's |
| **Depends on** | a capture of the three replication arms on the benchmark host, and a per-shard stage record of the append path |
| **Blocks** | nothing |
| **Tradeoff** | Not yet known — the question is whether the leader replicates an entry before or after its own `IOFlushed`, and whether three processes fsyncing one device serialize in the device rather than in the code |
| **Benchmark** | `macro/cluster/replication/durable` against `macro/cluster/overhead/nodes/3`, the F40 smoke numbers on the [F page](../features/replication.md#performance) |

Filed by [F40](../features/replication.md). The [C5](../distributed/replication.md#what-it-costs)
model of a healthy write is `max(local_sync, min(follower_B, follower_C))` - a leader that sends
the entry to its followers and syncs its own copy at the same time pays one sync's latency plus a
round trip. The smoke run paid two: 52.3 ms at the median against 24.8 ms for the same write
replicated to nobody, on the same three nodes with the same cores. Two explanations fit, and the
entry exists to name both rather than pick one: openraft's leader may append locally and replicate
only once its own flush completes, in which case the fix is in how the store signals; or the three
processes' `fdatasync`s queue in the one device the host has, in which case there is nothing to fix
on one machine and the benchmark host's capture on separate devices is the measurement. The volatile
arm, which pays the round trip and no sync, came in at 1.7 ms - so whichever it is, the second sync
is what the durable quorum costs here, and the lane is not.

### O48. Resolving a segment scans every group's whole index

| | |
| --- | --- |
| **Rank** | **B7** — argued, contained |
| **Impact** | Argued — `frames_in` walks every entry of every group named in a segment to find the ones in it, on every sweep that hands a segment over; the index is bounded by `retained_entries` per group, so the walk is `groups × retained` per handoff |
| **Difficulty** | S — a per-segment list of the frames it holds, built as they are staged and pruned as they are truncated or purged |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Contained — the list is the index's mirror, and the invariant is that the two agree |
| **Benchmark** | none that exercises it: a sweep runs every ten deadline ticks and hands over at most a few segments, so the walk is off every query path |

Filed by [F40](../features/replication.md). The store keeps one `BTreeMap` of index to location per
group and a per-segment record of each group's last log id in it; resolving a segment for the
compactor asks for the frames of some groups that lie in one generation, which the store answers by
walking each group's map and keeping the entries whose location names it. At ten thousand retained
entries and thirty-six groups a node that is a third of a million comparisons per handoff, on the
shard loop, between two messages. It is off every query path and it is bounded, which is why it is
filed and not fixed; a per-segment frame list is the fix when a sweep shows up on a profile.

### O49. One barrier per group per bundle rather than per read

| | |
| --- | --- |
| **Rank** | **B8** — indicated, contained |
| **Impact** | Indicated — the smoke run of `macro/cluster/reads/barrier` paid a barrier of 590 µs on average per read, 147 of 200 of them a hop to the leader; a bundle of many `Quorum` reads over one group pays it once per read |
| **Difficulty** | M — the barrier's read index is a bound for every read of that group issued after it was obtained, so one per group per bundle serves them all; the plan travels per share and would have to say which barrier it may share |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Contained — the reads in a bundle already promise no common snapshot, so a shared barrier promises nothing less than a barrier each |
| **Benchmark** | `macro/cluster/reads/barrier`, once an arm sends more than one read a bundle; the current arm sends one |

Filed by [F41](../features/read-consistency.md). `await_read_barrier` obtains a read index per
group per read, on a task per read, because a read is the unit that reaches it. A ReadIndex
barrier is a bound on everything acknowledged before it was asked, so every strong read of the
same group in the same bundle - issued after the barrier - may apply through the same index; the
heartbeat round is the cost, and one round covers them. The saving is the round and the hop,
which is most of what a strong read costs over a `One` read at smoke scale. Not taken because no
arm and no fixture test sends a bundle of strong reads over one group, so nothing would show it,
and because the plan is per share: a shared barrier needs an identity on the plan and a table of
barriers in flight per group on the shard, which is a design pass.

### O50. A read plan is built and cloned per share

| | |
| --- | --- |
| **Rank** | **B9** — argued, contained |
| **Impact** | Argued — `read_plan` builds a `ReadPlan` per query on the coordinator and every local share clones it for its slot; the tokens are an ~~`Rc<[SessionToken]>`~~ `Arc<[SessionToken]>` so the clone is ~~a count~~ an atomic count and two words, and a remote share copies the tokens into its entry |
| **Difficulty** | S — a plan per query held once and a slot beside the metadata, or the tokens filtered once per bundle rather than once per query |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Contained |
| **Benchmark** | none that would show it: the clone is one ~~`Rc`~~ atomic increment on a path that then sends over a channel |

Filed by [F41](../features/read-consistency.md). Every query in a bundle filters the bundle's
tokens to its table and builds a plan, and every share of it takes a clone; a bundle of a hundred
gets over one table with sixteen tokens filters sixteen tokens a hundred times. It is a few
hundred nanoseconds on the coordinator, which the `grid` arms did not move at smoke scale, and it
is filed because the number of tokens is the one thing here a client controls.

**The `Rc` was a defect, not an optimization.** A share's plan is cloned on the coordinator and
dropped on the shard that runs it, so the count was raced on by two threads and a get naming two
partitions crashed the node ([Resolved #133](resolved/read-plan-rc-across-shards.md)). The `Arc`
that replaced it costs one atomic increment per share, which is still below anything a benchmark
here could show.

### O51. Every committed write answers with a forty-eight byte token

| | |
| --- | --- |
| **Rank** | **B9** — argued, contained |
| **Impact** | Argued — a committed write's answer down a connection that granted `CLIENT_CAP_READ_OPTIONS` carries a 48 byte token and a third `IoSlice`, whether or not the caller will read it; every client this repository builds asks for the capability |
| **Difficulty** | S — a per-connection or per-send opt-in for the token, or a client that asks only when a `SendOptions` will use one |
| **Blocks** | nothing |
| **Tradeoff** | Contained — a client that never reads tokens loses nothing but the bytes |
| **Benchmark** | `macro/cluster/replication/durable`, whose every write now carries the token, against the F40 capture of the same arm |

Filed by [F41](../features/read-consistency.md). The token is minted in `answer_proposal` for
every `Applied` and `Duplicate` outcome and framed by the relay whenever the connection's hello
asked for the section, which the client always does. Forty-eight bytes against a response of a
few hundred, and one more slice in a vectored write that was already two. Filed rather than
gated because the alternative - a client that decides per send whether it will want the token of
a write it has not yet seen the answer to - is a worse API than the bytes are a cost, until a
capture says otherwise.

### O52. A snapshot copies every record of the archives into one file

| | |
| --- | --- |
| **Rank** | **B10** — argued, contained on the wire and not on the compactor |
| **Impact** | Argued — `cut_snapshot_file` reads every partition of every tablet the group covers, writes each into the snapshot file, fdatasyncs it and hands the path to the lane, which reads it again; a group's snapshot costs the table's compactor twice the archives' bytes in I/O before a byte moves, and the sender holds a second copy of the rows on disk until the transfer's retirement |
| **Difficulty** | L — pinning means the compactor may not delete or rewrite a partition file under a snapshot, so the archive map needs a reference per generation and the lane a reader that walks the pinned set in a stable order; the manifest already carries the boundary and the checksum a pinned set would have to be computed over lazily |
| **Blocks** | nothing |
| **Tradeoff** | Contained on the wire — the receiver's format is the same either way; not contained on the sender, where a pinned generation is what O9's rotation walk and the compactor's merges would have to respect |
| **Benchmark** | `macro/cluster/catchup/snapshot` on the benchmark host, whose `snapshot_bytes` and `seconds_to_converge` are the cut's cost plus the transfer's; the fixture's `snapshot_has_one_stable_boundary_under_writes` is what keeps a pinned cut honest |

Filed by [F43](../features/node-recovery.md). The copy was chosen because it makes the cut's
boundary a property of one file rather than of a set of files and a map, which is what the
crash matrix and the checksum are computed over, and because the table's compactor is the one
writer of the archives, so cutting there needs no lock. The cost is paid once per snapshot per
group, off the foreground, and a snapshot is taken only when a member is behind the purge
point or the retention budget forces one. Filed rather than gated because a cut that is one
file was the smaller thing to get right first.

**Priced in round 14.** On titan under the mixed bench a Movie set's cut took 5.5 to 8.4 s and
its send and install 1.4 s, so streaming the cut could have overlapped at most the smaller part.
The cut's time was its reads, one per record, which
[O78](#o78-a-snapshot-cut-read-one-record-at-a-time-in-key-order) addressed instead. The disk a cut
holds on the sender is still this entry's, and matters at a terabyte a node, not at the lab's
scale.

### O53. The assembler keeps a map of received chunks and forgets them on a restart

| | |
| --- | --- |
| **Rank** | **B11** — argued, contained |
| **Impact** | Argued — `Assembler` keeps a `BTreeMap` of the chunks received past the prefix, one entry per out of order chunk, and its state lives in memory; a receiver that restarts mid-stream answers the next `End` with `Resume { from: 0 }` and the sender starts over, whatever was on disk |
| **Difficulty** | S — a bitmap over `total / chunk` bits replaces the map, and the prefix's extent written beside the partial after each fdatasync is what a restart would resume from; the marker and the redo at open already know the directory |
| **Blocks** | nothing |
| **Tradeoff** | Contained — the sender's side is unchanged, since `Resume { from }` is already the protocol |
| **Benchmark** | `macro/cluster/catchup/snapshot` with a receiver restarted mid-stream, which no arm does; the fixture's `snapshot_duplicates_and_resume_are_safe` covers the resume within a process |

Filed by [F43](../features/node-recovery.md). The map is the simpler structure and a lane
delivers in order, so it holds nothing in the common case; the restart is the real gap, and
it was left because a restart mid-install is the crash matrix's problem and a restart
mid-stream is only a slower catch-up.

### O54. A scrub reads every archived partition of a group once per pass

| | |
| --- | --- |
| **Rank** | **B12** — measured in shape, contained |
| **Impact** | Measured in shape — a canonical cut hashes every resident partition on the loop and reads every non-resident one off the archives on a task, so a pass over a group costs the group's disk once: `cluster.background.bytes` on `macro/cluster/background/repair` is exactly that, and a scheduled scrub spends it every `scrub_interval`. At smoke scale on the development host every partition was resident and the cost was the loop's hashing alone; at full scale the archived pass is the whole of it |
| **Difficulty** | M — an incremental per-partition digest kept in the archive map, written by the compactor beside each entry and folded by the cut without reading the record, would make a pass a walk of the map rather than of the disk; the digest has to be the canonical one over re-serialized rows, so the compactor pays the re-serialization it already does for the write, and a record read for any other reason still verifies its checksum |
| **Blocks** | nothing; a scheduled default for `scrub_interval` ([Q12](../distributed/protocol.md#q12-at-m8)) waits on the full-scale measurement first |
| **Tradeoff** | Contained — the map grows by eight bytes an entry and the `SerializedMap` format moves, and a digest in the map is a digest a corrupt map could misreport, which the map's own checksum covers |
| **Benchmark** | `macro/cluster/background/repair`, whose `bytes` over `seconds` is the rate a pass reads at and whose `during` against `before` is what it costs the foreground |

Filed by [F44](../features/repair.md). A cut that reads the disk was the smaller thing to get
right first, and it is what makes the digest independent of anything the compactor wrote
beside the record - which is worth keeping in mind before the map carries the digest, since a
digest the compactor computed is not evidence against the compactor.

### O55. A learner inside the retained log is fed a snapshot when the leader's cached cut is newer than its purge point

| | |
| --- | --- |
| **Rank** | **B13** — argued, contained |
| **Impact** | Argued — a move's destination starts with no log, and openraft feeds a member with no log from the leader's earliest retained entry unless the leader has a snapshot past that entry, in which case the snapshot goes first. A group that has cut a snapshot since its purge point - which under `LogsSinceLast(checkpoint_entries)` is most groups most of the time - therefore feeds every new learner the whole of its archives over the bulk lane and then the tail, where a group whose retained log reaches back far enough could have fed the log alone. The migration arm's `bytes` says which happened: zero at smoke scale, where the log fed it |
| **Difficulty** | M — the choice is openraft's, made from `last_purged_log_id` against the snapshot's `last_log_id`; shoal decides what it *offers* as its current snapshot, and could decline to offer one to a replication stream whose target is a learner the retained log covers. The cost of getting it wrong is a learner that waits for a log the leader purges under it, which the library recovers from by sending the snapshot after all |
| **Blocks** | nothing; a transfer budget ([C8](../distributed/rebalancing.md#transfer-budgets), M9b) is where the bytes would be paced either way |
| **Tradeoff** | Contained — a snapshot fed to a learner is the same file a returning member gets, and a log fed to one is the same entries a follower gets |
| **Benchmark** | `macro/cluster/migration/move`, whose `bytes` against `entries` says which path fed the destination and whose `catching_up` says what it took |

Filed by [F45](../features/replica-migration.md). The arm was built to price the transfer
before anything decided how to shape it.

### O56. The planner recomputes every rule set on every look

| | |
| --- | --- |
| **Rank** | **B14** — argued, contained |
| **Impact** | Argued — `drive_plans` builds a `TabletMap` from the state and asks `rule_sets_served` and `groups_of` for every table each time it looks at an open plan, which is `TABLET_COUNT` rule derivations and a configuration scan per tablet, per look: every `plan_interval` and every quarter second the state or the capacity moved while a plan is open, on the control core. At the fixture's scale it is a millisecond; at a thousand groups it is the same loop over the same four thousand tablets, since the sets are a function of the placement and the configurations and both change rarely |
| **Difficulty** | S — the sets could be cached on the map by its version, or the planner given the `GroupSpec`s the map already derives for `replica_groups`, which are the same sets keyed the same way |
| **Blocks** | nothing; a plan is looked at seconds apart |
| **Tradeoff** | Contained — a cache keyed by map version is invalidated by exactly what invalidates the sets |
| **Benchmark** | none names it; the rebalance arms' `cluster.rebalance` windows would carry a control-core stall as a foreground tail, and the Q13 spike is where a control-plane cost is priced |

Filed by [F46](../features/capacity-rebalancing.md). `TabletMap::from_state` is what
`under_replicated_sets` and `apply_tombstone` build too, so the same cache would serve three.

### O57. Tablet bytes are rescanned from the whole archive map on every report

| | |
| --- | --- |
| **Rank** | ~~**B15** — argued, contained~~ **done** — applied and measured on the lab |
| **Impact** | Argued — `replication_report` asks each table's archive map for its bytes per tablet once per report, which is one pass over `to_archive` - every archived partition of the table on the shard - summing sizes into a vector of four thousand; a report is built on every deadline tick and sent when it differs from the last, and with `bytes` on it a shard under writes differs on most ticks. A shard with a million archived partitions walks a million entries a few times a second on its own core |
| **Difficulty** | S — a counter per tablet maintained at `set_partition` and `remove_partition` and rebuilt at open, which is what the first plan for this feature sketched; the pass was chosen because it cannot drift from the map, and the drift a counter risks is exactly the compactor's replace-in-place paths |
| **Blocks** | nothing |
| **Tradeoff** | Contained — a counter is the same figure without the pass, and `tablet_bytes_follow_the_map` is the test that would catch it drifting |
| **Benchmark** | none names it; the grid's `r50` cells on the persistent tables would carry a per-tick pass as a shard-core cost, and the kill arm's placement is where a report is built under load |

**Since [F76](../features/paged-archive-map.md)** the counters are what the map has: its index is
paged and a pass over it reads it from disk, so they are kept by each change from the chain it
replaces, carried in the manifest so an open does not count them, and `tablet_usage_by_pass` is a
scan a test compares them to.

Filed by [F46](../features/capacity-rebalancing.md). Since [F52](../features/cluster-stats.md)
the same pass counts each tablet's partitions as well (`ArchiveMap::tablet_usage`), so a counter
that replaces it has to keep both figures, or they drift apart.

**Applied** in the [cluster testing's section 11](../cluster-testing/correctness.md#11-overload-silence-and-a-nearly-full-disk)
change. `ArchiveMap` keeps a `TabletUsage` of both figures. It is counted as the map is loaded at
open, and moved by `set_partition` (the old entry off, the new one on) and `remove_partition`,
which are the only two ways `to_archive` changes. `tablet_usage` copies it, and
`tablet_usage_by_pass` keeps the old pass for the drift test. **Measured:** on titan under the
lab's mixed bench, `ArchiveMap::tablet_usage` was 1.36% of the node's samples on `ebf237e` and
absent on the fixed build ([performance](../cluster-testing/performance.md#the-admission-gate-and-the-bench)).
`tablet_bytes_follow_the_map` now also churns 2,000 inserts, replacements and removals and
checks the counters against a pass.

### O58. A rehome's moved records are copied, and a donor's archives keep the dead ones

| | |
| --- | --- |
| **Rank** | **B16** — argued, contained |
| **Impact** | Argued — an `Archives` step reads every moved record verified through `read_record` and writes it as a fresh record through `write_record` into one new archive on the destination, so a shrink costs a read and a write per record of the vanished executors, linear in what they held; and a growth's donor only drops the moved entries from its map, so its archives hold the dead records - and the space - until its own compaction rewrites them, which `compact_archives` does at fifty percent utilization and not before |
| **Difficulty** | M — archive files are table-wide and a moved entry could be re-pointed rather than copied, but `all_archives` is per executor and a compactor deletes what it owns, so a shared file needs a reference count or an owner the reclaim respects; and a donor could rewrite its live records into a fresh archive at the reclaim, which is a compaction the rehome would then own |
| **Blocks** | nothing |
| **Tradeoff** | Contained — the copy is what makes the reclaim a plain delete and the redo a plain re-copy; a shared file would make both conditional on who else names it |
| **Benchmark** | `macro/rehome/shrink` ([F47](../features/local-rehome.md)), whose `cluster.rehome.bytes` over `millis` is the copy's pace, and whose growth twin - not built - would carry the dead records as a donor's archive size |

Filed by [F47](../features/local-rehome.md).

### O59. The rehome runs on one core and blocks the start

| | |
| --- | --- |
| **Rank** | **B17** — argued, contained |
| **Impact** | Argued — `Rehome::run` builds one executor on the first shard cpu and runs every step of the manifest on it in order, and `ShoalPool::start` waits for it before the shard pool is built, so a node changing its core count holds its start for the whole move while every other core idles; the steps of different tables and different source-destination pairs are independent and could run on every core the node has |
| **Difficulty** | M — an executor per source, each with its own slice of the manifest, and the manifest's marks written from one place; the crash points and the idempotence argument are per step and survive it, the ordering between a source's copies and its reclaim does not without a barrier |
| **Blocks** | nothing |
| **Tradeoff** | Contained — one core is what makes the manifest one writer and the crash matrix one sequence; the saving is start time on a node that is already down |
| **Benchmark** | `macro/rehome/shrink` ([F47](../features/local-rehome.md)), whose `cluster.rehome.millis` is the hold |

Filed by [F47](../features/local-rehome.md).

### O60. A node's figures ride its status report as verbose JSON

| | |
| --- | --- |
| **Rank** | **B18** — measured in shape, contained |
| **Impact** | Measured in shape — `cargo run -p shoal-spike --release -- fanout` prices a `StatusReport` carrying a `NodeStats` for four busy tables at 7,766 bytes against 323 without; on one report in four (`STATS_EVERY_REPORTS`) that takes the leader's intake from 387 KB/s to 621 KB/s at 64 members and from 1.3 KB/s to 8.7 KB/s at three. Every figure is a named JSON field, every rate three floats printed at full precision, and every table carries both its hosted and its led rates |
| **Difficulty** | S — round the rates to three significant figures before serializing, send only the led rates and the applied totals and let the leader derive the rest, or carry the figures as rkyv on the control lane; each is local to `NodeStats` and its tracker |
| **Blocks** | nothing |
| **Tradeoff** | Contained — the report is the control lane's and nothing on a query's path waits on it; zero rates and idle tables are already left out, so a real report is smaller than the priced one |
| **Benchmark** | none; the spike's `fanout` table is the price, and no arm drives a cluster of 64 |

Filed by [F52](../features/cluster-stats.md).

### O61. A fast device syncs the WAL in batches too small to fill a page

| | |
| --- | --- |
| **Rank** | ~~**B19**~~ **done** — applied as a per-node setting, measured on the lab; off by default. The lab's delays, 2 and 3 ms, measured beside none on the store comparison's legs (2026-10-20 and 21): on the SSDs no faster without them, on europa's loopback 2.2 to 3.4 times as fast for 17 to 44% more device bytes ([the figures](../object-storage/store-optimizations/group-commit-delay.md#on-europas-loopback)). Since [F98](../features/commit-delay-bounded-by-the-sync.md) the delay is the most the writer waits, never longer than its last batch took to write and sync, the user's choice. Measured with the lab's delays (2026-10-22): on europa's loopback 2.2 to 3.2 times as fast as before it and within 7% of none, keeping half the device bytes the delay saved at sixteen in flight; on the SSDs no rate moved ([F98's figures](../features/commit-delay-bounded-by-the-sync.md#performance)) |
| **Impact** | Measured — on the three-node lab, one 30 s insert run wrote 16.6 GB to europa's Optane and 2.45 GB to each Zen1 host's NVMe for the same replicated rows: 6.8×. The node process itself was charged 4,776 MB of writeback on europa and about 875 MB on each other host for 554,450 inserts, and it issued 5.6k `fdatasync`s a second against about 680 on titan. **Measured again in the comparison** (2026-10-25 and 26, [Shoal measured again](../object-storage/store-comparison.md#shoal-measured-again)): on europa's loopback 4 KiB puts, under F98 with the lab's delays as their ceiling, ran 3.4 times as fast at one in flight as the first run's and lead every store but SeaweedFS at sixteen in flight; on the SSDs and disks they moved by 6% or less |
| **Difficulty** | S to M — a group-commit delay in the WAL writer: once a batch completes, wait a bounded time for more frames before taking the next, but only while batches are arriving back to back |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Latency for wear and CPU: every write on a node that delays waits up to the delay longer for that node's sync. On a node that is not the slowest of a write's quorum it may cost nothing, and on the slowest it costs the delay |
| **Benchmark** | the lab's insert-only `bench` with `/proc/diskstats` and `/proc/<pid>/io` before and after ([cluster testing](../cluster-testing/performance.md#write-amplification-by-device-and-filesystem)); `macro/cluster/replication/durable` on the benchmark host for the latency side. Since [F71](../features/bench-device-memory.md) every `shoaladm bench` capture keeps the device counters of every run, so an insert arm of it shows this by itself |

Found by the [distributed cluster testing](../cluster-testing/performance.md#write-amplification-by-device-and-filesystem)
chapter. The WAL writer takes whatever frames arrived while the last batch was being written and
synced, writes them with one `write_at` and syncs them with one `fdatasync` (`server/wal/mod.rs`,
`writer`). The faster the sync, the smaller the next batch. Every batch re-dirties the page that
holds the file's tail, so the kernel writes that page back once per sync, and on btrfs each sync
also commits log-tree metadata. On europa a sync completes about every millisecond per shard, so
batches are small. The Zen1 hosts' NVMe completes one about every nine, so theirs are about eight
times larger. `nodatacow` on europa's storage directory (`chattr +C`) left the device bytes
unchanged at 16.6 GB, so this is not btrfs copying data. The remaining 1.8× between the process's
writeback and the device's writes on europa, against 1.26× on ext4, is the filesystem's
per-sync metadata. The group-commit delay is tried in [cluster testing](../cluster-testing/performance.md#o61-a-group-commit-delay),
which records the outcome.

**Applied:** `cluster.replication.wal_commit_delay`, at most 10 ms, zero by default. After each
sync the shard's WAL writer waits ~~that long~~ that long, or since [F98](../features/commit-delay-bounded-by-the-sync.md) no longer
than the batch took to write and sync, before it takes the next batch, and appends that
arrive in the wait join it (`writer` in `server/wal/mod.rs`, set by `ShardWal::set_commit_delay`
at the shard's open). Only a writer that has just synced waits, so the first append after an idle
spell is written at once. `a_commit_delay_groups_appends_into_fewer_syncs` checks the mechanism:
~~200 appends half a millisecond apart took 200 syncs with no delay and 22 with 5 ms~~ 200 appends
half a millisecond apart took 200 syncs with no delay and 22 with 5 ms before F98; since it, the test
holds each sync 4 ms, and a 5 ms delay takes at most two thirds the syncs none does.

**Outcome on the lab**, europa alone set in its `shoal.yml`, the insert bench at each setting,
europa's syncs counted by a probe on its io_uring submissions and its device writes read from
`/proc/diskstats`:

| europa's delay | europa's syncs in 10 s | europa's device writes in 30 s | Cluster inserts | p50 | p99 |
| --- | --- | --- | --- | --- | --- |
| 0 | 45,123 | 15.6 GB | 18,357/s | 25.3 ms | 506 ms |
| 3 ms | 14,215 | 7.7 GB | 18,649/s | 25.3 ms | 520 ms |
| 0 | 44,162 | 14.9 GB | 17,401/s | 25.9 ms | 601 ms |

Three milliseconds cut europa's syncs by two thirds and its device writes by half, with the
cluster's throughput and latency unchanged. A write's latency is set by the quorum, and a quorum
always includes a Zen1 host whose own sync takes longer than the delay. An earlier sequence, 0,
1 ms, 3 ms and 0 in that order, put 1 ms at 13.1 GB, −18%. Its throughput numbers could not be
used, because the table's growth moved them by more than the settings did. **Kept, off by
default.** On a device whose syncs are already slow, a batch fills while the last one syncs, and
the delay would only add latency. It is for the fast device in a mixed cluster, which the lab
has. ~~A deployment inventory cannot set it per group yet, filed in [todos](todos.md).~~ Since
[F57](../features/cluster-reconfigure.md) an inventory sets it per deployment, group or node.

**Beside the store comparison** (2026-10-20,
[the lab's group commit delay](../object-storage/store-optimizations/group-commit-delay.md#on-the-ssds)).
The comparison ran every Shoal capture at the lab's delays, 3 ms on europa and 2 ms on titan and
hyperion, and on one host found Shoal's 4 KiB puts at half Ceph's rate. On the SSD leg the same
build with no delay on any node, beside the lab's delays, in both groups:
- **No arm ran faster.** 4 KiB puts and the mixture, at one in flight and sixteen, were within in
  every run.
- **The delay saved what it was set to.** Without it titan's and hyperion's NVMe flushed 8 to 18%
  more an operation and were 10 to 16 points busier. The device bytes a byte put rose 4 to 13% in
  all but one arm.
- **At one in flight it moved a put's latency both ways.** It held the puts' p50 0.25 to 0.34 ms
  lower and their p99 1.4 to 1.5 ms higher, each apart in every run of the mixture.

The loop leg, where the comparison found the gap, is measured after the disk leg. A delay taken
only while appends are queued, the change the comparison's page plans, is designed on its
figures: on these it would behave as no delay at one in flight and win nothing.

### O62. Every compaction rewrites the shard's whole archive map

| | |
| --- | --- |
| **Rank** | ~~**B20**~~ **done** — measured on the lab, applied, kept |
| **Impact** | Measured — on the lab under a mixed load, map saves were 1,573 MB of the 2,235 MB one Zen1 node wrote in 28 seconds (70%), in bursts of up to 400 MB in one second that stalled every fsync behind them |
| **Difficulty** | S — a fold threshold proportional to the map instead of a fixed mebibyte |
| **Depends on** | [Resolved #140](resolved/intent-log-read-ahead.md), without which the longer intent logs it leaves failed a start |
| **Blocks** | nothing |
| **Tradeoff** | A restart replays up to a quarter of the map as intents instead of at most a mebibyte, and the intent log on disk is that much larger. With #140's read-ahead that replay costs seconds, not minutes |
| **Benchmark** | the lab's mixed `bench` with a bpftrace count of bytes written per file ([cluster testing](../cluster-testing/performance.md#o62-the-archive-map-rewrite)) |

**Superseded by [F76](../features/paged-archive-map.md).** ~~The fold at a quarter of the saved
map~~ There is no whole map to rewrite: a flush writes the delta as a new run and merges runs while
the newest one times `merge_ratio` is at least the next, so a partition's entry is written about
log4(N / delta) times and the whole map is rewritten only when a merge reaches the oldest run. The
map is flushed once its delta holds `delta_entries` partitions, and its intent log is begun again
once it passes 1 MiB or `delta_entries` times 128 bytes, whichever is more, so a restart replays at
most that.

Found by the [distributed cluster testing](../cluster-testing/performance.md#o62-the-archive-map-rewrite)
chapter. A table's archive map is saved whole, the map of every archived partition on the shard,
and changes between saves go to its intent log. The compactor folded the log into a new map
whenever the log passed one mebibyte (`current_flushed_pos() > Byte::MEBIBYTE`, at four places in
`compactor.rs`). With about a million partitions per shard a map is about 85–125 MB, so every
mebibyte of intents cost a hundred mebibytes of map rewrite.

**Applied:** `ArchiveMap::compaction_due` folds the log once it passes a quarter of the map as last
saved (`saved_bytes`, set at every save and read from the file at open), and never below the old
mebibyte (`MAP_FOLD_RATIO`, `MAP_FOLD_FLOOR`). A fold now writes at most four bytes of map per byte
of intent.

**Outcome:** over the same 30 second mixed bench, map saves on hyperion went from 1,573 MB to
26 MB. An A/B on the same cluster, rolling back to the old fold with `cluster upgrade --rollback`
and forward again, excluding the first run after each roll (caches cold, leadership moved):

| | Operations per second | Write p99 | Write max |
| --- | --- | --- | --- |
| Old fold, two runs | 91,000–106,000 | 174–198 ms | 461–596 ms |
| New fold, five runs | 99,500–142,800 | 128–201 ms | 290–389 ms |

Throughput rose and the worst write fell by about a third, because the bursts that stalled the
Zen1 hosts' fsyncs are gone. The median run moved less than the spread between runs, so the
change is not claimed for the median. The first rollout exposed [#140](resolved/intent-log-read-ahead.md):
hyperion's longer intent log, read one direct read per field, failed its start. **Kept**, with
#140.

### O63. Leadership never returns to a group's placement primary

| | |
| --- | --- |
| **Rank** | ~~**B21**~~ **done** — measured on the lab, applied, kept |
| **Impact** | Measured — after a few restarts and rolling upgrades, one Zen1 node led all 36 groups and proposed every write: 81,000 operations a second at a write p99 of 300 ms, against 90,000–97,000 at 217–234 ms with the leads spread 12, 12, 12 |
| **Difficulty** | S — a shard hands a group it has led for a while back to the group's placement primary |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | One leader change per group handed back, which costs that group's writes one transfer. An even spread is right for equal members, and not the best spread for unequal ones: on this lab, europa leading more than its share beat the even split |
| **Benchmark** | the lab's mixed `bench` with every group led by one node against the same with the leads spread ([cluster testing](../cluster-testing/performance.md#leadership-after-a-restart)) |

Found by the [distributed cluster testing](../cluster-testing/performance.md#leadership-after-a-restart)
chapter, and listed before that as unbuilt on [C15](../distributed/open-issues.md#filed-as-unbuilt)
("leadership moved … back to a returning node"). The placement puts each group's primary first in
its voters and spreads primaries evenly, and a group's first leader is its primary. An election
after a crash, or a handoff on a planned stop ([Resolved #139](resolved/leadership-handoff-on-stop.md)),
moves the lead to another voter, and nothing moved it back. The handoff made it worse: it picks the
most caught-up voter, so a rolling upgrade that ends with the leader tends to pile the leads onto
whichever node was restarted first.

**Applied:** `Shard::balance_leadership`, on the shard's tick. Every five seconds a shard hands at
most one group back to its placement primary, and only when all of these hold:

- it has led the group for ten seconds;
- the primary is up in the map;
- every voter is within 16 entries of its log, and a voter it has no progress for counts as behind;
- no repair, move, backup or restore is working on the group, and none of its copies here is
  quarantined;
- it has not tried to hand this group back in the last minute.

**The first cut restarted a snapshot install.** `snapshot_duplicates_and_resume_are_safe` failed
three times in three. A member being fed a snapshot had no progress in the leader's metrics, which
the check read as index zero, and in a group whose log was shorter than the allowed lag that
counted as caught up. The transfer to it failed, another voter was elected, and the member's
install started over, every fifteen seconds. Treating no progress as behind, and a one minute
retry per group, fixed it (three of three, then the whole fixture suite).

**Outcome:** after a rolling upgrade the leads went from 6/24/6 to 12/12/12 within 30 seconds.
Against every group led by titan, two runs each and the first after the upgrade excluded:

| Leads (europa/titan/hyperion) | Operations per second | Write p99 | Write max |
| --- | --- | --- | --- |
| 0/36/0 | 81,000, 81,000 | 300 ms, 299 ms | 614 ms, 610 ms |
| 12/12/12 | 96,700, 90,300 | 235 ms, 217 ms | 612 ms, 539 ms |

**Kept.** Earlier runs with europa leading 18 or 24 groups reached 99,000–143,000 operations a
second, because europa is the fastest host. Weighting the primaries by a member's capacity, which
the planner already reads for data placement, would do better than an even spread on unequal
hardware. It is filed in [todos](todos.md#leadership-is-spread-evenly-whatever-each-member-can-do).

**A test it made stale, found later.** `down_within_grace_moves_no_replicas` asserted that a
member back within its grace "leads nothing on its return". Once catching up took longer than
the settle time, which it did under the full suite's load, the member had been handed back a
group it is the primary of, and the test failed. It now asserts what still holds: a group the
returning member leads is one it is the placement primary of, never one an election gave it.

### O64. A shorter failover base halves write throughput on the lab

| | |
| --- | --- |
| **Rank** | **B22** — measured on the lab, cause not isolated, default kept |
| **Impact** | Measured — the full TMDB load at `primary_failover_after` 1 s ran at 19,800–24,200 rows a second in four runs, at 2 s at 30,300, and at the default 5 s at 40,200–46,500 in four runs. Crash failover went the other way: about 4 s of refused writes at 1 s, 8 s at 2 s, 16 s at 5 s |
| **Difficulty** | Unknown until the cause is found |
| **Depends on** | finding the cause |
| **Blocks** | a shorter default failover base |
| **Tradeoff** | Crash failover time against write throughput, on this hardware |
| **Benchmark** | `target/lab/failover-test.sh` and `profile-load.sh` in the [cluster testing](../cluster-testing/performance.md#failover-time-against-primary_failover_after) chapter |

Found by the [distributed cluster testing](../cluster-testing/performance.md#failover-time-against-primary_failover_after)
chapter while measuring failover against the base. A group's timers derive from the base alone
(`group_config`): a heartbeat every tenth of it, an election timeout of one to two bases, and a
leader lease of two. The mixed bench barely moved (108,000 operations a second at 1 s, 118,000 at
2 s and at 5 s), and no election happened under load at any base. The write-only load halved.

What was measured on titan during a load, over the same 10 seconds:

| Base | WAL `fdatasync`s | WAL bytes | user / system / idle / iowait |
| --- | --- | --- | --- |
| 5 s | 3,576 | 107 MB | 36 / 32 / 12 / 20 |
| 1 s | 1,836 | 51 MB | 22 / 41 / 9 / 28 |

At 1 s the node wrote and synced half as much. Its disk was busier and its cpu spent more of its
time in the kernel, with context switches up from 25,000 to 27,700 a second on half the work. A
`perf` profile of titan at 1 s is flat. The allocator leads (`_mi_page_malloc` 4.4%), and glommio's
`insert_timer` appears at 0.5%, which it does not in the 5 s profile. What is ruled out:
heartbeats do not fsync (an empty append completes without a batch), and the committed index is
staged, not waited on. The leading hypotheses are the heartbeat timers themselves, ten a second
per follower per group, and a follower applying commits in batches a fifth the size.

**Timers, measured after.** A count of titan's io_uring submissions by opcode over ten seconds of a
load:

| Base | Rows/s | `TIMEOUT` | `ASYNC_CANCEL` | Timer submissions per row |
| --- | --- | --- | --- | --- |
| 5 s | 43,900 | 84,700/s | 82,700/s | 3.8 |
| 1 s | 18,700 | 115,300/s | 113,700/s | 12.2 |

glommio arms an io_uring timeout for every timer and cancels it when the future completes first.
openraft ticks each group every 13/64 of a heartbeat interval, about 20 ms per group at a 1 s base,
and times out every RPC, heartbeat and replication wait through the runtime Shoal gives it. At 1 s
a node does three times the timer work per row, and even at 5 s a four core Zen1 host submits
170,000 timer operations a second. That is the leading explanation, not a proven one: heartbeat
suppression ([O65](#o65-heartbeats-to-followers-that-just-acknowledged-replication)) did not
change it, and nothing here moved the tick.

**Not applied.** The default stays at 5 s: a planned stop no longer waits for failover at all
([Resolved #139](resolved/leadership-handoff-on-stop.md)), and only a crash pays the window. An
operator who prefers the shorter window can set `failover` in the inventory, now knowing its cost
on hardware like this.

**Revisited on 2026-09-25, with every fix through [#157](resolved/destroy-mount-point.md)
deployed: the halving does not reproduce.** Each arm was a fresh bootstrap, one load to fill it
and then measured loads (`target/lab/sys-count.sh`), with the leads 12/12/12 before each measured
load (`wait-balanced.sh`):

| Arm | Load, rows a second |
| --- | --- |
| 1 s base, fresh bootstrap | 46,400, 48,300, 49,700 |
| 5 s base, fresh bootstrap | 34,300, 34,500, 33,500 |

The direction had reversed, so the 1 s base's timers were separated one at a time on the 5 s
cluster, through a temporary environment override that was never committed:

| Arm at the 5 s base, leads balanced first | Load, rows a second |
| --- | --- |
| defaults | 35,300, 34,300 |
| heartbeat 100 ms (a fiftieth of the base) | 48,100, 48,700 |
| heartbeat 100 ms, election timeout 1–2 s | 44,100, 46,700 |
| heartbeat 500 ms, apply wait bounded at 200 ms | 45,500, 46,200, then 35,200 on the next build |

The apply wait (`wait_applied_here`) was instrumented next. Over a whole load it never reached
its bound: europa's writes waited 5.6 ms on average when they waited at all, titan's 25 ms, and
none of 2.5 million waits ran out. With the bound at 200 ms the same instrumented build loaded at
35,200. So the high arms above are not explained by what each changed. **The load's throughput on
this cluster is bimodal, near 35,000 or near 46,000–50,000 rows a second, from one restart to the
next**, and none of the timer settings chose the mode reliably. The earlier figures for this entry
(19,800–24,200 at 1 s, 40,200–46,500 at 5 s) are what that variance looks like beside a real
difference. The candidate not yet tested is which node leads the groups holding the keyword
table's hottest partitions. Twelve leads each do not mean equal work, and europa is the fastest
host. Per-member stats cannot show it, since every member applies every row.

**Status:** the cause of the original halving is not established, and it is not present on the
current tree. The default stays at 5 s. ~~What would settle the mode question is per-group write
rates in `Stats`, filed in [todos](todos.md#per-group-write-rates-in-stats).~~

**The hypothesis tested, and not supported.** `Stats` now names each member's busiest led groups
(`NodeStats::hot_groups`, [cluster testing, round 11](../cluster-testing/performance.md#who-leads-the-busiest-groups)).
Five fresh bootstraps, each loaded whole, with the stats read 20 s into the load
(`target/lab/r11/o64.sh`):

| Run | Rows a second | The ten busiest groups, by the host leading them |
| --- | --- | --- |
| 1 | 40,023 | europa 5, titan 3, hyperion 2 |
| 2 | 39,389 | europa 5, titan 5 |
| 3 | 40,618 | titan 8, europa 1, hyperion 1 |
| 4 | 49,856 | europa 5, titan 4, hyperion 1 |
| 5 | 42,713 | hyperion 7, titan 2, europa 1 |

The fastest run and the two slowest had the same spread, europa leading five of the ten. And the
busiest groups each wrote about a thousand rows a second, within a few percent of each other: no
single hot group decides anything. The mode is still unexplained.

The investigation also found [#158](resolved/runtime-waker-lists.md), and tried
[O69](#o69-every-idle-moment-parks-an-executor).

**Round 12: five more candidates ruled out, and the resource named.** About forty fresh loads with
a rate every five seconds and each member's storage pipeline in `Stats`
([cluster testing, round 12](../cluster-testing/performance.md#o64-in-round-12-what-the-mode-is-not)):
the page cache, TRIM after `destroy`, the disks' flush latency before a load, one shard carrying
more than its share, who leads, and how a node's leads fall on its shards made no difference. The spread, 36,000 to 48,000 rows a second,
is set in a load's first ten seconds. A write-only load is paced by the Zen1 hosts' 970 EVOs, which
flush their cache on every `fdatasync` (3 ms for one writer, about 900 synced writes a second for
six). A 2 ms `wal_commit_delay` on those hosts made the slow mode rarer (1 of 8 loads under 42,000
against 9 of 15), which is what a group commit with two equilibria would show. The mixed bench under it showed no
latency cost and about 8% more throughput, so it is **applied to the lab's inventory** (a
deployment setting, not a default). ~~**Status:** the cause of the mode is still not established;
the next step is a batch-size distribution per sync on the Zen1 WALs, to see the two equilibria
directly.~~

**Round 13: there are no two equilibria.** `Stats` now carries each member's WAL sync time,
appends per sync and a distribution of sync sizes (`NodeStats::wal_sync_ms`,
`wal_appends_per_sync`, `wal_sync_sizes`). Twelve fresh loads at the 2 ms delay ran at 44,100 to
58,600 rows a second
([cluster testing, round 13](../cluster-testing/performance.md#o64-in-round-13-the-batches-seen)).
In every one of them each Zen1 node's six WAL writers synced 450 to 630 times a second at 7 to
11 ms a sync, which is the device's limit with six writers and the delay, and the batches were
mostly 16 to 64 KiB. A slow load and a fast one had the same distribution. The group commit does
not settle on small batches. It is saturated in every load, and a load's pace is how many appends
each sync happens to carry. A longer delay does not make that more: 5 ms gave batches of 9 to 13
appends and 350 to 450 syncs a second, 45,600 rows a second on average against 52,500 for the
interleaved 2 ms loads. So 2 ms stays. **Status:** the spread between bootstraps is not explained,
but it is not the WAL's batching. A Zen1 sync costs the same from 16 KiB to 256 KiB, so what would
move a write-only load on these devices is fewer syncs per device: fewer WAL writers than shards on
one disk, or one WAL a node. That is a design change, filed in
[todos](todos.md#fewer-wal-syncs-per-device).

**Round 14: the device is busy with archives, not the WAL.** Round 13's *a sync costs the same
from 16 KiB to 256 KiB* was half wrong. On idle titan a buffered append's sync was mostly ext4's
journal commit for the file's new size: six writers overwriting files written ahead with
`O_DIRECT` committed twice as often as six appending ([F60](../features/shared-wal-flush.md)).
That was built, as a direct WAL and as one flush shared by a device's shards, and under a load it
made no difference (46,600 to 55,900 rows a second in every mode) and the write p99 worse. A
trace of titan's writes by file during a load found why: the node wrote 114 MB/s to archives,
68 of them MovieByKeyword's, against 24.5 MB/s to the WAL, while it applied 15 MiB/s of rows.
Every WAL sync flushes the device's cache with the compactor's writes in it. The merges rewrite
each keyword partition whole every segment, which is
[O79](#o79-a-merge-rewrites-every-partition-it-touches-whole), and 40 MiB segments halved the
archive writes and made loads about 24% faster. **Status:** ~~what paces a write-only load on these
hosts is the merges' write amplification.~~ Round 15 removed most of the amplification
([F61](../features/fragmented-partitions.md)): the keyword table's archive writes fell by four
fifths and a node's by half, and the loads did not move (41,400 to 56,200 rows a second with and
without fragments). So the archives' write volume is not what paces a load either, and round
14's 40 MiB arms were faster by the spread between bootstraps
([cluster testing](../cluster-testing/performance.md#o79-in-round-15-fragments)). What differs
between one bootstrap and the next is still not named; the spread persists at every segment size,
WAL mode and archive volume tried, and is set within a load's first five seconds. And the halving this entry was filed for is gone: six loads at
a 1 s base ran at 42,600 to 55,000 rows a second, the same as at 5 s, with and without
[#190](resolved/append-answer-thrown-away.md)'s floor on an append's wait.

**Round 16: not the hosts' frequency governor.** The one candidate outside Shoal nobody had
varied: the Zen1 hosts run `schedutil` from 1.6 GHz. Ten fresh loads, six under `schedutil` and
four with both hosts on `performance`, with every Zen1 cpu's frequency sampled through each
([cluster testing](../cluster-testing/performance.md#o64-in-round-16-not-the-governor-either)):
under a load the cores run at 3.1 to 3.3 GHz on average whichever governor is set, the slow
load's cores 80 MHz below the fast ones', and the spread was the same under both (43,100–55,500
against 48,400–55,900). Nine candidates are ruled out, and the lab stays on `schedutil`.

### O65. Heartbeats to followers that just acknowledged replication

| | |
| --- | --- |
| **Rank** | ~~**B23**~~ **done** — applied, no measurable effect, kept |
| **Impact** | Indicated — every group's leader heartbeats every follower every tenth of the failover base, whether or not replication just proved the follower alive: at the default base, 36 groups × 2 followers × 2 a second on a three node cluster, and five times that at a 1 s base, where the load ran at half the throughput ([O64](#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab)). In a partition every missed one is an openraft warning, and journald on the lab suppressed 43,954 of a node's lines |
| **Difficulty** | S — openraft 0.10's `heartbeat_min_interval`, off by default |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | none expected: a replication response proves what a heartbeat does and moves the lease the same way |
| **Benchmark** | the lab's full load at 1 s and at 5 s ([O64](#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab)) |

Found by the [distributed cluster testing](../cluster-testing/correctness.md#partition-one-node)
chapter.

**Applied:** `group_config` sets `heartbeat_min_interval` to the heartbeat interval, so a follower
that acknowledged replication within the last interval is not also sent a heartbeat. openraft
requires the interval, this and one tick to fit under the minimum election timeout, which a tenth,
a tenth and a fiftieth of the base do. `group_config` falls back to openraft's defaults when a
config does not validate, so `a_group_config_keeps_the_timers_its_base_derives` checks the derived
timers survive at bases from 100 ms to 30 s. Since [Resolved #358](resolved/control-answers-waited-for.md) the control group does too, at its 50 ms interval:
there it is a correctness setting as much as a saving, since a control follower that acknowledges appends late would
otherwise be sent heartbeats that fail at openraft's own 50 ms budget.

**Outcome: no measurable effect on throughput.** Two loads each, fresh bootstraps:

| Base | Before | With suppression |
| --- | --- | --- |
| 5 s | 40,200–46,500 rows/s | 35,800 (the first after a bootstrap) and 43,000 rows/s |
| 1 s | 19,800–24,200 rows/s | 18,600 and 18,800 rows/s |

So heartbeats are not what makes a short base slow
([O64](#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab) has what was found instead).
It does nothing in a partition either, where the heartbeats that fail are to a follower that
acknowledges nothing. **Kept**, as the configuration openraft documents for sustained writes, at no
measured cost. The log flood in a partition is not addressed by it.

### O66. A partitioned peer floods the log

| | |
| --- | --- |
| **Rank** | ~~**B24**~~ **done** — applied, measured and kept |
| **Impact** | Measured on the lab — with one node cut off, every group leading a copy on it logged openraft warnings for each failed heartbeat and replication attempt, twice a heartbeat. journald on titan suppressed 43,954 of the node's lines in one partition test and 50,000–60,000 in later ones, and both journald and the node spent a Zen1 host's cpu formatting and writing them |
| **Difficulty** | S — a default filter directive |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | openraft's per-attempt warnings are gone at the default level; the peer being unreachable is still said once by the link and by the failure detector, and `RUST_LOG` brings them back |
| **Benchmark** | the lab's [partition test](../cluster-testing/correctness.md#partition-one-node), counting journald's suppressions |

Found by the [distributed cluster testing](../cluster-testing/correctness.md#partition-one-node)
chapter, and not addressed by [O65](#o65-heartbeats-to-followers-that-just-acknowledged-replication),
which only drops heartbeats to followers that are answering.

**Applied:** the default filter (`directives_from` in `shoal-core/src/server/trace.rs`) holds
`openraft::core::heartbeat`, `openraft::engine::handler::replication_handler` and
`openraft::replication` to errors when the configured level would show warnings. At `Error` or `Off`
the directives are left out, since there they could only turn those targets on. `RUST_LOG` still
replaces the whole default. The table stores' `mark_evictable` event, logged at INFO once per
partition made evictable, moved to DEBUG for the same reason: under a load it was most of what a
node logged at INFO.

**Outcome:** the same 20 second partition under the mixed bench made journald suppress **none** of
any node's lines, against 50,000–60,000 before, and each node logged about 7,000–8,000 lines over
the whole run. Throughput during the partition did not measurably change: the lines were a cost to
the host, not the cause of the stalls that followed the heal, which the
[partition test](../cluster-testing/correctness.md#partition-one-node) follows up. **Kept.**

### O67. Ten thousand retained entries is seconds of a busy group

| | |
| --- | --- |
| **Rank** | ~~**B25**~~ **done** — applied, measured and kept |
| **Impact** | Measured on the lab — a node partitioned for 20 s under the mixed bench came back behind the purge point of its groups. Several were fed a snapshot two or three times over, because the leader purged past each install's boundary while it ran. Its reads of those groups were refused `Unavailable` for about 70 s after the heal, and a read-back through it 35 s after the heal timed out |
| **Difficulty** | S — a default |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | more WAL on disk under a heavy load (about 0.5 GB a node on the lab), still bounded per shard by `retained_bytes` |
| **Benchmark** | the lab's [partition test](../cluster-testing/correctness.md#partition-one-node), counting installs; a restart timed after a minute of load; each node's resident memory after 90 s of inserts |

Found by the [distributed cluster testing](../cluster-testing/correctness.md#partition-one-node)
chapter. `replication.retained_entries` is how far behind its snapshot a group keeps its log for
a member to catch up from. openraft purges to it after every snapshot, and a snapshot is taken
every `checkpoint_entries` (1,024). Under the lab's mixed bench a group purged about 7,000 entries
every few seconds, so ten thousand held roughly ten seconds of log. None of the purges in the
test were the bytes bound's (`enforce_retention` forced nothing). The entries preference alone
decided it. [F43](../features/node-recovery.md#retention-in-bytes) describes the repeated
snapshot as what a stream that cannot keep up pays. Here it was paid by a node that was only
twenty seconds away.

**Applied:** the default is 100,000. It changes nothing about the bound: `retained_bytes` still
forces a purge past a member that would hold the WAL past a gibibyte a shard.

**Outcome**, one partition test each on the running cluster (the setting edited into each node's
`shoal.yml` and the nodes restarted one at a time):

| | 10,000 (t05e) | 100,000 (t05f) |
| --- | --- | --- |
| Snapshot installs on hyperion after the heal | 13 over 72 s, up to three per group | 0 |
| Reads refused `Unavailable` after the heal | 90–1,300 a second until the run ended | none |
| Read-back of every acknowledged insert through hyperion 10 s after the run | timed out | passed first time |
| Throughput from the heal to the end | 29,000–74,000 ops/s | 39,000–81,000 ops/s |

The costs, measured in matched pairs (set everywhere, nodes restarted, settled, then loaded):

| | 10,000 | 100,000 |
| --- | --- | --- |
| Titan's WAL after a minute of the mixed bench | 1.6, 1.7 GB | 2.1, 2.3 GB |
| Titan's restart to every member up | 24.3, 27.5 s | 22.9, 27.5 s |
| Resident memory after 90 s of inserts (europa, titan, hyperion) | 5.5, 4.8, 5.8 GB | 4.7, 4.6, 6.0 GB |
| Mixed bench, gets a second | 36,900, 33,900 | 32,900, 37,400 |

Restart time, memory and throughput did not move beyond run-to-run noise. The WAL index holds a
slot per retained entry, but a bundle's rows share one entry per tablet, so the entries are far
fewer than the rows. **Kept.**

A cluster benchmark arm that runs at the default retention (`macro/cluster/catchup/log`) now runs
a different server. Its captures from before this change do not describe it, and a new capture
belongs to the benchmark host, not the lab.

### O68. Every archive compaction copies the shard's whole partition index

| | |
| --- | --- |
| **Rank** | ~~**B26**~~ **done** — applied, contained |
| **Impact** | Measured in shape — `ArchiveMap::sort_by_load` was the one Shoal function in a page fault profile of a lab node under load, 9.5% of its faults. It copies every entry of the table's index, five million a shard on the lab, into vectors per archive, on every archive compaction, which runs after every sealed 10 MiB WAL segment |
| **Difficulty** | S |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | an archive's entries are gathered with a pass over the index when it is compacted, so a compaction of several archives passes over it several times: CPU for memory |
| **Benchmark** | the lab's insert bench with a page fault profile; `macro/grid/unsorted/*` for the compaction path's CPU |

**Since [F76](../features/paged-archive-map.md)** the index is on disk, so a pass over it an archive
would read it once for each: `sort_by_load` reads live bytes per archive that every change keeps,
and a pass chooses its archives first and gathers all of their records in one scan
(`ArchiveMap::gather`). ~~CPU for memory~~ One read of the index a pass, for no memory but the
records the pass moves.

Found by the [distributed cluster testing](../cluster-testing/performance.md) chapter while chasing
[#149](resolved/node-memory-budget.md). `sort_by_load` ranks a table's archives by the bytes they
still hold, so the compactor can rewrite the least used. It also built, for every archive, a vector
of every entry in it: a transient copy of the whole index, 40 bytes an entry, about 200 MB a shard
at the lab's size, on every compaction. The compactor uses the entries of the archives it
compacts, which is those under half used, not all of them.

**Applied:** `sort_by_load` counts bytes per archive and copies nothing, and the compactor asks
`entries_of(archive)` for each archive as it compacts it. The index is not changed until the pass
ends, so an archive's entries gathered then are the ones the old copy held.
`archives_are_ordered_by_load_and_gathered_one_at_a_time` pins both halves (since
[F76](../features/paged-archive-map.md) `archives_are_ordered_by_load_and_gathered_in_one_pass`). The memory is no
longer allocated. The node-level effect is folded into [#149](resolved/node-memory-budget.md)'s
runs, which changed several things at once, so it has no figure of its own. **Kept.**

### O69. Every idle moment parks an executor

| | |
| --- | --- |
| **Rank** | **B27** — tried on the lab, not kept |
| **Impact** | Measured — a lab node under the TMDB load issued 7,700 `membarrier` calls a second, and 16,800 context switches, from executors going to sleep. A 200 µs spin before parking cut the barriers tenfold, and the load's throughput did not move |
| **Difficulty** | S — glommio's `spin_before_park` on the shards' pool |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | cpu time on an idle core, and power, for fewer sleeps |
| **Benchmark** | `target/lab/sys-count.sh`: a TMDB load with `perf stat` counting titan's syscalls for 10 s |

Found by the [distributed cluster testing](../cluster-testing/performance.md#spinning-before-parking)
chapter while chasing [O64](#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab). O64
counted 170,000 io_uring timer operations a second on titan. glommio's reactor re-arms its two
preemption timers on every turn, so those count reactor turns, not openraft's timers. And every
time an executor sleeps it issues `membarrier(PRIVATE_EXPEDITED)`, which interrupts every core
the process runs on. A thread-per-core server usually polls briefly before sleeping, and Shoal
never set glommio's `spin_before_park`.

**Tried:** a `resources.spin_before_park` setting, applied to the shards' pool. Titan, over 10 s
of the load:

| Spin | Rows a second | `membarrier` | `io_uring_enter` | Context switches |
| --- | --- | --- | --- | --- |
| none | 33,900 | 102,000 | 4.4 M | 208,000 |
| 50 µs | 34,200 | 54,000 | 5.6 M | 150,000 |
| 200 µs | 33,700 | 11,400 | 8.2 M | 133,000 |

**Not kept.** The barriers fell tenfold and the throughput stayed where it was, while spinning
executors entered the ring twice as often. A setting with no measured benefit is surface
area and a way to burn a core, so the change was reverted. Worth trying again only on a
workload that is latency-bound at low load, where a sleep is on the critical path, and against
the microbenchmarks, not the lab's load.


### O70. A snapshot cut reads its records one at a time

| | |
| --- | --- |
| **Rank** | ~~**B28**~~ **done** — applied, contained |
| **Impact** | Measured on the lab — a repair's cut of a Movie group finished two to three minutes after it was asked for under the update bench, once it held 66,000 to 146,000 partitions, and each time the repair, and the stalled copy with it, waited on it. The cut reads every archived record of the group with a direct read at its offset, one awaited after another, in key order, which is random order on disk |
| **Difficulty** | S |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | 32 records' buffers held at once, about 13 MB for the lab's rows; the file is still written in key order |
| **Benchmark** | the lab: `repair` of a group under `bench --mix get:40,update:45,insert:15`, timed from the compactor's `cutting a snapshot` to its `cut a snapshot` |

Found by the [distributed cluster testing](../cluster-testing/correctness.md#7-a-copy-that-cannot-read-a-partition)
chapter while proving [#160](resolved/unreadable-partition-stalls-one-copy.md) on the lab.
`cut_snapshot_file` read each record with `read_record(entry).await` inside its loop, so the
device saw one request at a time, and under the bench's load each one waited its turn behind the
node's own reads and writes.

**Applied:** the records are read through a stream `buffered(32)`, which keeps 32 reads in flight
and yields them in the order asked, so the file's bytes and its checksum are what they were. On
the lab, the cut of a group of 268,753 partitions took **18 s** under the same bench, and one of
320,561 took **4 s** idle. The old cut's own duration was never logged (only its end), so the
before figure is the time from the request to the end, which includes the wait for the compactor's
job in progress. The new `cutting a snapshot` line at the start is what makes the two parts
separable now. **Kept.**

**Tried with it and not kept:** taking a cut out of turn, ahead of the merges queued before it in
the compactor. A cut is exact wherever it runs among merges, so it was safe. But with it in place,
a repair's cut on the third run still started 78 s after the request. The wait was the job in
progress, not the queue, and nothing measured a difference from the reordering, so it was
reverted.

### O71. A held snapshot is cut again whenever the checkpoint moves

| | |
| --- | --- |
| **Rank** | ~~**B29**~~ **done** — applied, contained |
| **Impact** | Measured on the lab — europa cut group `2c0307…`'s 125 MB snapshot 17 times in 97 s, once for every try its openraft made to send a snapshot to titan's stalled copy, because the held file was below a checkpoint that moved every few seconds under load |
| **Difficulty** | S |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | a receiver may install an older snapshot and take more of the log after it, which the log still holds by construction |
| **Benchmark** | the lab's stall runs: `cutting a snapshot` lines per group while a member needs one |

`handle_build_snapshot` answered with the held file only if its boundary was at or past the
group's checkpoint. A receiver needs no more than a file whose boundary the leader's log still
follows: everything above the boundary is replicated to it after the install.

**Applied:** a held file is the answer while the log's purge point is at or below its boundary
(or it is at the checkpoint, as before). Because a caller can need a newer one, a build now names
`at_least`, the lowest boundary its caller can use, and `build` asks again if a cut already in
flight lands below it:

- openraft's transmitter asks for any (0);
- a backup asks for its applied index, since everything applied before it was asked for has to be
  in the file;
- a repair asks for any, and then for past the target's checkpoint once the target has answered
  `Behind`;
- the fixture's `SNAPSHOT` asks for the checkpoint.

On the lab's next stall runs, each group was cut once per repair. **Kept.**

### O72. A refused snapshot build logs four lines per apply

| | |
| --- | --- |
| **Rank** | ~~**B30**~~ **done** — applied, contained |
| **Impact** | Measured on the lab — 3,604 lines in five minutes on europa, `push snapshot building command`, `build snapshot`, `snapshot building is refused by state machine` and `snapshot building deferred`, while compaction was behind the bench |
| **Difficulty** | S |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | openraft's snapshot handler and state machine worker say nothing below `WARN` by default, including an install's `install complete snapshot`, which Shoal logs itself |
| **Benchmark** | `journalctl -u shoal-tmdb \| grep -c "snapshot building is refused"` over a stall run |

openraft asks the state machine for a snapshot once `LogsSinceLast(checkpoint_entries)` entries
have been applied since the last. The machine refuses until its checkpoint has moved and is durable
(`try_create_snapshot_builder`), which, while compaction lags, is every apply. Each refusal is four
`INFO` lines from two openraft targets.

**Applied:** `openraft::core::sm::worker` and `openraft::engine::handler::snapshot_handler` join
O66's quiet targets, at `warn`. The fifth line is `openraft::engine::engine_impl`'s, which also
logs elections, so it is left. The refusal itself costs a message to the worker and back and is
left as it is. **Kept.**

### O73. A snapshot is cut for a member that cannot be reached

| | |
| --- | --- |
| **Rank** | ~~**B31**~~ **done** — applied, contained |
| **Impact** | Measured on the lab — during a rebuild, the leaders cut two or three snapshots of 270 MB a group for the rebuilt node's old identity, down until its removal reached each group, and failed each send as unreachable (15 failures over 22 minutes). The moves refilling the replacement cut from the same compactors |
| **Difficulty** | S |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | a member heard from again gets its snapshot on openraft's next retry, not at once |
| **Benchmark** | the lab's rebuild under the bench: `cut a snapshot` lines for groups no move names |

openraft sends a snapshot to a follower behind the leader's purge point, and our transmitter
(`GroupPeer::full_snapshot`) cut the file before its first RPC, so a member that could not be
reached cost a whole cut per try. It now refuses at once, before cutting, when the link has heard
nothing from the member for the hop-silence bound ([#143](resolved/silent-partition-hops.md)'s
two seconds). A member that returns is heard from again and is cut its snapshot on the next try.
**Kept**; the rebuild's time was not measured again with it, so its effect on that number is not
claimed.

### O74. A Zen1 node's compactor falls hundreds of jobs behind under the bench

| | |
| --- | --- |
| **Rank** | ~~**B32**~~ **done** — applied; the cost of reclaiming space at the rate the bench makes garbage is measured and kept |
| **Impact** | Measured on the lab — titan's `Movie` compactor had 180 jobs queued when a snapshot cut reached the front after 9.4 minutes, under the mixed bench through all three members ([#174](resolved/snapshot-cut-queue.md)). Sealed segments wait for their merge, the groups' logs pass `retained_bytes`, and the retention budget forces purges ("forcing a group past a sealed segment") |
| **Difficulty** | M |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | about 6% of steady-state throughput on the Zen1 hosts under a rewrite-heavy bench, which is the archive passes reclaiming space the backlog used to leave on disk; the WAL no longer grows 1.7 GB every five minutes |
| **Benchmark** | the lab's rebuild under the bench (`rebuild-exp.sh`), read from the compactor's `a compaction job ran long` phases, the cuts' `queued_ms`, the forced purges and the rebuild's time; and a steady-state A/B of builds on one cluster (`target/lab/o74/ab.sh`), read with each arm's archive and WAL bytes |

Since #174 a cut no longer waits for the backlog, but every other job still does, and a backlog is
held WAL. A cut still waits for the job running when it arrives. Rebuild 8 saw that cost 147 s,
behind a backlog that was mostly archive passes, each redundant with the next.

**Partly applied first: coalesced archive passes.** An archive pass with another queued behind it
is skipped (`is_redundant`), since a pass compacts whatever qualifies when it runs. Rebuild 9, on
the cluster rebuild 8 left:

| | Rebuild 8, passes not coalesced | Rebuild 9, coalesced |
| --- | --- | --- |
| Longest wait for a snapshot cut | 147.5 s | 39.5 s |
| Cuts' median and p90 wait | 0 and – | 0 and 6.2 s |
| Whole rebuild | 979 s | 748 s |

**Measured: where a merge's time goes.** A job that holds the compactor over 5 s now logs its
phases: the frames it read, the partitions it read from the archives, the apply, the writes, the
syncs and a map fold (`JobPhases`). On a fresh cluster under the bench with hyperion rebuilt
(run *base*, build `3e1133b`), the average long segment merge:

| Phase | titan (762 long merges) | hyperion (424) |
| --- | --- | --- |
| Whole job | 6.8 s | 6.5 s |
| Reading ~19,000 frames from the segment | 0.88 s | 0.85 s |
| **Reading ~9,500 partitions from the archives** | **5.8 s** | **5.4 s** |
| Apply, write ~14,000 records, sync, map fold | 0.19 s | 0.18 s |

So nearly all of it was one direct read at a random offset after another, about 0.6 ms each on a
busy four-core node. That is O70's shape again: O70 fixed it for a snapshot cut, and a merge still
had it. Not the merge's own work, not the map's intents (O62), and not the scheduling. The archive
passes were the same shape, and hyperion's longest took 294 s.

**Applied:**

1. **A merge reads its partitions 32 at a time** (`MERGE_READS_IN_FLIGHT`, `buffer_unordered`).
   Run *c1*: the long merges fell to 4 on titan and 1 on hyperion. The peak backlog fell from 139 to
   18, forced purges from 3 to 0, and the rebuild from 430 s to 315 s.
2. **Archive passes are paced.** The fast merges exposed a cost the backlog had hidden. A pass is
   queued behind every segment, and with no backlog nothing made it redundant, so one ran after
   every merge. A steady-state A/B on one cluster, 300 s arms, measured the c1 build 10% below the
   timing-only one: update p99 111 → 155 ms, get p99 roughly doubled. On titan the passes copied
   2,139 MiB in five minutes, against 42 MiB. Running a pass as soon as an archive crosses half
   live is the worst time to run one. A later pass finds the archive deader and copies less for the
   same space. So a new pass now waits out `archive_pass_interval`, a minute, since the last one
   began.
3. **A pass stops at its budget, inside an archive if it must.** The index is repointed only when
   a pass ends, so a queued cut waits for a whole pass, and one archive can hold hundreds of
   megabytes. Run *c2* had passes of up to 228 s and a cut that waited 32.9 s. A pass now checks
   `archive_pass_bytes` (16 MiB) after every record, repoints what it copied, leaves the rest of the
   archive for its next turn, and is queued again behind the jobs waiting. It keeps 16 reads in
   flight. 8 made each pass hold the compactor longer, and 32 took the foreground's device.
4. **A merge reads its frames as one span** of the segment, not one read per frame. Merges that
   met a segment out of the page cache spent 9–23 s reading 18,000 frames.
5. **A job's end syncs the archive once**, not twice.

The rebuild on each build, each on a fresh cluster loaded from the csv (section 10 of
[Correctness](../cluster-testing/correctness.md#10-the-compactors-backlog-and-four-rebuilds)
checked the data):

| | base | c1 | c2 | **c3** |
| --- | --- | --- | --- | --- |
| Build | phases logged | + 32 reads in flight | + passes paced, 8 in flight, span reads | + budget inside an archive, 16 in flight |
| Long merges, titan / hyperion | 762 / 424 | 4 / 1 | 0 / 0 | **0 / 0** |
| Long archive passes on titan, and the longest | 2, 23 s | 24, 67 s | 35, 228 s | **15, 5.9 s** |
| Peak backlog on titan | 139 | 18 | 76 | **4** |
| Longest wait for a cut on titan | 6.1 s | 3.5 s | 32.9 s | **0.7 s** |
| Forced purges on titan | 3 | 0 | 0 | **0** |
| Whole rebuild | 430 s | 315 s | 534 s | **272 s** |

The steady state, on one cluster, arms alternated so each build follows the other:

| A/B, 300 s arms | Build | ops/s, each arm | Update p99 | Titan's archives, each arm | Titan's WAL, each arm |
| --- | --- | --- | --- | --- | --- |
| first | phases logged (`3e1133b`) | 34,368 / 35,045 | 111 / 112 ms | passes copied 42 MiB in an arm | – |
| | c1: 32 in flight, passes unpaced (`3e1133b`) | 31,518 / 30,878 | 149 / 160 ms | passes copied 2,139 MiB in an arm | – |
| last | phases logged (`3e1133b`) | 35,824 / 32,852 | 109 / 125 ms | **+2,523 / +2,576 MB** | **+805 / +514 MB** |
| | c3, kept (`3e1133b`) | 32,762 / 31,354 | 133 / 143 ms | −948 / −736 MB | +62 / +53 MB |

**What it costs.** The kept build serves about 6% fewer operations a second than the timing-only
one in a 300 s window, with update p99 about 20 ms higher. The timing-only build is faster because it
is not doing the work. Its merges are backlogged, so nearly every archive pass is skipped as
redundant, and the space the bench's rewrites leave dead stays on disk: titan's archives grew
2.5 GB in each of its arms, where the kept build's shrank. Its WAL grows too, by 0.5 to 1.7 GB an
arm across the A/B runs, against a 1 GiB retention budget. That is where the forced purges come
from, and the snapshot installs they cause. The kept build reclaims the space as it is made, and its
WAL stays flat. A 5-minute interval against 1 minute moved throughput and p99 within the runs'
noise (`target/lab/o74/ab3`) and held more dead bytes, so the minute is the default. Europa, on the
Optane, logged no long compaction job in any run. The cost is the Zen1 hosts' devices, and the
cluster's throughput follows its slowest members.

**Kept.** Filed on the way: [#179](resolved/archive-usage-prefix.md), an archive's live bytes counted
without its records' prefixes.

~~**Still open:** the 50% threshold is hardcoded, and neither a merge nor a pass sorts its reads by
offset.~~ Both done in the [cluster testing's round 11](../cluster-testing/performance.md#o74s-remainder):

- **The threshold is a setting**, `throughput_sensitive.archive_pass_live_percent`, 50 by default
  and held to 1–99. It is not swept yet: the rewrite-heavy bench is where a sweep would show it.
- **Sorting the reads by offset was tried and not kept.** A merge's reads sorted by archive and
  offset, a pass's by offset, against the same build without, A B B A with 180 s of the
  rewrite-heavy mix: 35,606 and 34,863 operations a second sorted, 36,136 and 36,450 not. On the
  lab's NVMe with 16 to 32 reads in flight the order does not matter, and a sort costs a pass
  over the entries.

### O75. Every query formatted its metadata into a tracing span

| | |
| --- | --- |
| **Rank** | ~~**B33**~~ **done** — applied and measured on the lab |
| **Impact** | Measured — on titan under the lab's mixed bench, formatting and span bookkeeping were 3.4–3.6% of the node's samples; with the arguments skipped, 2.3% |
| **Difficulty** | S |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | The console and the collector lose the query's metadata on `PersistentTable::handle`, and its id on the three shard spans below `Coordinator::route`, which still carries it |
| **Benchmark** | a `perf` profile of a Zen1 node under the lab's bench (`target/lab/r11/profab.sh`); the grid's `r50` cells on the benchmark host would carry it as a per-query cost |

Found by the [distributed cluster testing](../cluster-testing/performance.md#where-the-time-goes)
chapter, where about 2% of titan's samples went to formatting strings for tracing fields.

`#[instrument]` records every argument it is not told to skip, with `Debug`, and the console layer
(`tracing_subscriber::fmt`) formats a span's fields when the span opens, whether or not an event
is ever written under it. At the default `Info` level every per-query span is open, so:

- `PersistentTable::handle`, on both table kinds, skipped only `self` and the query, and formatted
  the whole `QueryMetadata` and the seal with `Debug` for every query;
- `Coordinator::handle_client` recorded the bundle's `Stamp`;
- `Shard::handle_query`, `handle_released` and `handle_gathered` each built a `String` of the
  query's id.

**Applied:** the two `handle`s and `handle_client` skip every argument, and the three shard spans
keep only `index`. The id is on their parent, `Coordinator::route`, which every trace hangs off.
In an A B B A run on one cluster (`target/lab/r11/prof-o75`), the tracing and formatting symbols in
titan's profile, the slab of span data included, fell from 3.39% and 3.60% on the build before
to 2.35% and 2.29%. `DebugStruct::field`, `str`'s `Debug`, and the console's ANSI styling were gone
from the profile. Throughput was 93,941 and 91,703 operations a second before, 93,591 and 108,420
after: within the lab's spread, so the saving is claimed only as the CPU it measures.

**What is left** is the spans themselves: `sharded_slab`'s pool, 0.8% of titan's samples, is a
slot per open span. Dropping the per-query spans below `Info` would remove it, and the traces a
collector gets at `Info` with it ([F35](../features/wire-trace-context.md)).

### O76. A write through a lagging copy waits its whole apply bound

| | |
| --- | --- |
| **Rank** | ~~**B34**~~ **done** — applied, measured in the fixture and on the lab |
| **Impact** | Measured — on the lab with hyperion's storage delayed 50 ms, the write p99 was 1,022 ms for the whole fault: every write coordinated through hyperion waited the bound |
| **Difficulty** | S |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | None a client can rely on: a write whose wait runs out is answered unapplied either way; a lagging copy now says so after a poll rather than a second |
| **Benchmark** | the lab's slow-disk scenario (`target/lab/r11/slowdisk.sh`, [cluster testing, section 12](../cluster-testing/correctness.md#12-scenarios-nobody-had-run)); `writes_through_a_lagging_copy_are_not_held_for_the_bound` |

Found by the [distributed cluster testing](../cluster-testing/correctness.md#12-scenarios-nobody-had-run)
chapter's slow disk. A write committed through a follower waits for the follower's own copy to
apply it, so a `One` read through the same node sees it, for at most two heartbeat intervals: a
second at the default base ([Resolved #146](resolved/apply-wait-on-a-lagging-copy.md)). A copy
behind a slow device applies every write late, so every write through its node waited the whole
second, and was then answered unapplied anyway.

**Applied:** `MachineState::apply_lagging` records that this copy's last wait ran out. While it is
set, the wait is one `APPLY_POLL` (50 ms). The first write whose apply lands inside the wait clears
it (`wait_applied_here` in `server/shard/groups.rs`). A copy that is keeping up never sets it, and
waits as before.

**Measured.** In the fixture, ten writes one after another through a follower whose appends
complete 1.5 s apart, at the default base: 8.0 s before, 2.1 s after (the first waits its second,
the rest a poll each). On the lab, hyperion's storage delayed 50 ms again
(`r11/sc/slowdisk-50ms-o76`): the write p99 reached a second for the fault's first two seconds,
while each group's first wait ran out, and held at 107–253 ms for the rest of it, where it had
been 1,022 ms throughout. Throughput was 50–79k operations a second, as before, and all 540,837
acknowledged inserts were read back through each member.

### O77. An abandoned proposal logs a warning when it applies

| | |
| --- | --- |
| **Rank** | **done** — applied |
| **Impact** | Observed — about 1,000 lines a second on titan during a load past what the cluster commits (192,332 in three minutes), each `ProgressResponder.complete_tx.send: is_ok: false` |
| **Difficulty** | S |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | openraft's line is gone at the default levels. The proposer already answered the write `OutcomeUnknown` and counted it in `unknown_outcomes`, so nothing is lost. `RUST_LOG` brings it back |
| **Benchmark** | none: a log volume, read from the journal (`journalctl -u shoal-tmdb \| grep -c WARN`) |

Found by the [distributed cluster testing](../cluster-testing/correctness.md#13-round-12) chapter's
#180 sweep, with the admission gate switched off on a lab build. openraft warns once for every
entry whose proposer dropped the channel it would have been answered on. Shoal drops it for
every write it stops waiting for: at `write_timeout`, when a silent partition's hop watch gives
up ([#143](resolved/silent-partition-hops.md)), or when a leader falls quiet with the write
already appended. A write piled up in openraft past its deadline is one line when it finally
applies. Titan's storage shares its root device with the journal and `/var/log/syslog`, which
is how openraft's debug tracing once filled it
([round 8](../cluster-testing/correctness.md#8-an-unplaced-member-coordinates)).

**Applied:** `openraft::raft::responder=error` joins the targets `server/trace.rs` holds down at
any level that would show warnings, beside [O66](#o66-a-partitioned-peer-floods-the-log)'s and
[O72](#o72-a-refused-snapshot-build-logs-four-lines-per-apply)'s.

### O78. A snapshot cut read one record at a time in key order

| | |
| --- | --- |
| **Rank** | **done** — applied |
| **Impact** | Measured on the lab — under the mixed bench titan cut a Movie set of 52 to 55 MB in 5.5 to 8.4 s, and 0.55 s after, where europa cut the same shape in 0.5 s; the cut was 73 to 85% of each step titan sent, the send and install the rest (`target/lab/r14/tb/cuts.py` over round 13's and round 14's journals). A set is about 80,000 records of about 700 bytes, each a direct read at its own offset, in key order, which is random order on disk, on a device whose queue is full of the WAL's flushes |
| **Difficulty** | S |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | A cut's file is in disk order, not key order, so two cuts of one state are one file only while the archives do not move between them; a stream resumed across a compaction starts over. The installer never read the order. Up to eight runs of a mebibyte held at once, where 32 records were |
| **Benchmark** | the lab: a rebuild under `bench --mix get:40,update:45,insert:15`, each cut timed from the compactor's `cutting a snapshot` to its `cut a snapshot` (`cuts.py`) |

Found by the [distributed cluster testing](../cluster-testing/correctness.md#15-round-14) chapter
while pricing [O52](#o52-a-snapshot-copies-every-record-of-the-archives-into-one-file), which
would stream a cut instead of writing it first. On titan that could overlap at most the 1.4 s a
set's send and install took, against a cut of 5 to 8 s, so the cut's reads were the larger cost.
[O70](#o70-a-snapshot-cut-reads-its-records-one-at-a-time) had put 32 reads in flight and kept the
file in key order.

**Applied:** the cut sorts the group's records by archive and offset and splits them into runs
(`cut_runs`, `shoal-core/src/server/tables/storage/fs/compactor.rs`): records of one archive
whose span is at most 1 MiB and whose gaps of other groups' records are at most 128 KiB. Each run
is one read (`ArchiveMap::read_run_from`), every record in it verified against its checksum as a
single read verifies it, and eight runs are in flight. The file is written in the order read.

Measured on the lab with the same rebuild under the mixed bench as before
(`target/lab/r14/tb/o78`), the cuts timed by `cuts.py`:

| Sender | A Movie set's cut, before | After |
| --- | --- | --- |
| titan | 5.5–8.4 s (52–55 MB) | 0.55 s (50 MB) |
| europa | 0.5 s | 0.16 s |

The rebuild took 175 s, the fastest on the lab so far (187 s in round 13 on the same inventory),
and streamed 877.5 MiB for 878.1 MiB moved. Every acknowledged insert and the whole csv read back
through each member alone. **Kept.**

### O79. A merge rewrites every partition it touches whole

| | |
| --- | --- |
| **Rank** | ~~**B35**~~ **done** — the design built as [F61](../features/fragmented-partitions.md) in round 15 and measured on the lab |
| **Impact** | Measured on the lab — under a whole load titan wrote 114 MB/s to archives (MovieByKeyword 68, Movie 33, the maps' temp files 10) and 24.5 MB/s to its WAL while it applied about 15 MiB/s of rows, so about seven archive bytes for every byte inserted, and about sixty for the keyword table, whose rows are small and whose partitions hold thousands of them. Every WAL sync flushes the device's cache with those writes in it, which is what paces a write-only load on the 970 EVOs ([O64](#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab)) |
| **Difficulty** | S for the setting, L for the design |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | A larger `segment_bytes` rewrites a busy partition less often and makes each merge job larger: at 40 MiB loads ran about 24% faster and the mixed bench about 8%, while its update p99 rose from about 116 ms to about 141 ms |
| **Benchmark** | the lab: `target/lab/r14/o64/seg.sh`, a fresh cluster per arm, a whole load and a 120 s mixed bench, each traced by table on titan (`tables15.bt`) |

Found by the [distributed cluster testing](../cluster-testing/performance.md#o64-in-round-14-the-journal-not-the-flush)
chapter while measuring F60, whose shared flush made no difference under load. A segment merge
(`FileSystemCompactor`, `load_partitions`, `apply_intents`, `write_partition`) reads every
partition the segment's frames touch, applies them, and writes each partition as one new record.
A keyword partition is a sorted list of every title carrying the keyword, so a popular one is
rewritten whole each time a segment holds one more title for it. A segment is sealed every
`segment_bytes` of a shard's WAL, about every 2.5 s per shard under a load at the default 10 MiB.

The inventory can now set `segment_bytes` (`replication.segment_bytes`), and round 14 measured it
on buffered WALs, each arm a fresh cluster:

| `segment_bytes` | Loads, rows a second | Archive writes during a load | Mixed bench, ops a second | Update p99 |
| --- | --- | --- | --- | --- |
| 10 MiB (the default) | 43,975, 42,411 | 108, 93 MB/s | 39,433, 40,079 | 118, 115 ms |
| 20 MiB | 51,796, 44,513 | 99, 94 MB/s | 42,679, 42,193 | 123, 131 ms |
| 40 MiB | 52,328, 54,486 | 50, 60 MB/s | 43,165, 43,407 | 138, 144 ms |

**Not applied as a default.** It trades the write tail for throughput, and a deployment that loads
in bulk can set it. The fix that removes the amplification instead of spreading it out is a
partition written as fragments, filed in [todos](todos.md#a-large-sorted-partition-written-as-fragments).

**Acted on in round 15** as [F61](../features/fragmented-partitions.md): a merge writes a large
sorted partition's inserts and deletes as a fragment chained after its base, and readers fold the
chain. On the lab, at 10 MiB segments, the keyword table's archive writes during a load fell from
55–80 MB/s to 15–19 MB/s and the node's from 98–136 MB/s to 55–66, with the mixed bench, its update
p99 and cold keyword reads unchanged. Loads did not get faster, which is what moved
[O64](#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab)'s status. The benchmark is
`target/lab/r15/seg.sh` with an inventory that sets `fragment_max_chain: 0` against one that does
not.

### O80. Every query opens spans a collector may never read

| | |
| --- | --- |
| **Rank** | **not taken** — measured, and smaller than what removing it would cost |
| **Impact** | Measured on the lab — `sharded_slab`'s pool, a slot for every open span, was 0.8% of titan's samples under the mixed bench after [O75](#o75-every-query-formatted-its-metadata-into-a-tracing-span) stopped the spans formatting their fields |
| **Difficulty** | M — a sampling layer that decides per trace whether its spans are opened, below the per-query spans and above the console and the collector |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Dropping the per-query spans below `Info` would remove the cost and every trace a collector gets at `Info` ([F35](../features/wire-trace-context.md)), which is the one way to see where a slow query spent its time across nodes |
| **Benchmark** | a `perf` profile of a Zen1 node under the lab's bench (`target/lab/r11/profab.sh`), `sharded_slab` symbols |

Filed from what O75 left, which [what is left](../cluster-testing/todo.md) carried as not filed.
A span is a slot in the subscriber's slab from the moment it opens, whether or not anything records
under it, and every query opens several (`Coordinator::route`, `Shard::handle_query`,
`PersistentTable::handle`). Not taken: 0.8% is inside the lab's run-to-run spread, and the only
way to remove it without losing the traces is a sampled layer, which is a design of its own. It
is worth revisiting if a profile of a faster node shows the slab as a larger share.

### O81. A map save kept a copy of the map

| | |
| --- | --- |
| **Rank** | ~~**B36**~~ **done** — found by a heap profile on the lab in round 15 and applied |
| **Impact** | Measured on the lab — at ten copies of the dataset, rkyv's thread-local arenas held 768 MiB of a node at the end of a bench, each at the size of the largest archive map its shard had serialized, and every save cloned the map first |
| **Difficulty** | S |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Every save allocates its scratch anew and frees it, where the thread's arena was reused. A save is at most one per quarter of the map written as intents ([O62](#o62-every-compaction-rewrites-the-shards-whole-archive-map)) |
| **Benchmark** | the lab's memory arm, `target/lab/r15/mem.sh`, and a heap profile of the node (`jemalloc-prof`, `target/lab/r15/prof/heap.py`) |

Found by the [distributed cluster testing](../cluster-testing/performance.md#memory-at-ten-times-the-dataset)
beside [#191](resolved/raft-channels-preallocated.md). `SerializedMap::save` cloned `to_archive`,
the fragments and the archive set into a `SerializedMap` and serialized that with
`rkyv::to_bytes`, which borrows a thread-local `Arena` that keeps the capacity of the largest thing
it has ever built. A map is by far the largest thing a shard serializes, so each shard kept a
scratch arena the size of its map's hash table for good, and briefly held the map twice more
during a save. **Applied:** the save serializes a `SerializedMapRef` that borrows the live index,
archived as a `SerializedMap` is (same fields, same order), with an `Arena` of its own that is
dropped when the save ends. The map tests read back what it writes.

### O82. A table's partition index kept the capacity of its peak

| | |
| --- | --- |
| **Rank** | ~~**B37**~~ **done** — found on the lab in round 15 and applied |
| **Impact** | Measured on the lab — a table's `partitions` map held 480 to 576 MiB of a node's heap at ten copies of the dataset, sized for the most partitions it had held at once, which no budget counts ([#150](resolved/inline-partition-buckets.md) boxed the rows; the buckets stayed) |
| **Difficulty** | S |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | A rehash of the entries left when an eviction leaves the map under a quarter full, and a grow again if it fills |
| **Benchmark** | `table_index_bytes` on `Stats`, the lab's memory arm (`target/lab/r15/mem.sh`) |

A `HashMap` never gives back capacity on its own, so a table that once held every partition of a
load kept the buckets for all of them after eviction. **Applied:** ~~`shrink_if_sparse`
(`tables/persistent.rs`) shrinks a table's index to twice what it holds once it holds under a
quarter of its capacity and more than 4,096 buckets, after every eviction.~~ Since F87 a table's
index is a map a tablet (`ResidentPartitions`, `tables/resident.rs`), and
`ResidentPartitions::shrink_if_sparse` runs after every eviction over each tablet's map: a tablet
holding nothing gives its map back, and one holding under a quarter of its capacity and more than
64 buckets is shrunk to twice what it holds. `Stats` reports each node's `table_index_bytes` and
`archive_map_bytes` beside its rows, and `cluster stats` shows them.

### O83. The partition index held forty-eight bytes a partition

| | |
| --- | --- |
| **Rank** | ~~**B38**~~ **done** — found on the lab in round 15 and applied |
| **Impact** | Measured on the lab — the archive map was the largest structure a node held at scale: 1.2 GiB for 11.8 million partitions, 4.6 GiB for about 48 million, of an 8 GiB budget it is not counted against |
| **Difficulty** | M |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | An entry is built from its slot on every lookup, one index into a small table; archive ids are kept in a table that grows by one per archive a map has ever written to, 16 bytes each |
| **Benchmark** | `archive_map_bytes` on `Stats` at ten copies of the dataset, `target/lab/r15/scale.sh` |

**Since [F76](../features/paged-archive-map.md)** there is no `PartitionIndex` in memory: the slot
lives on as a page's 28 byte entry on disk, its archive a number into the run's own table, and
what a partition costs in memory is its share of a run's filter, about a byte and a quarter at ten
bits a key, where O83 had brought it to about twenty five.

Found by the [distributed cluster testing](../cluster-testing/performance.md#memory-at-ten-times-the-dataset).
`to_archive` was a `HashMap<u64, ArchiveEntry>`: the key, then an entry that repeated the key,
named its archive by a 16 byte `Uuid`, and its size as a `usize`, 48 bytes a bucket and a control
byte, at hashbrown's load factor and its doubling. **Applied:** the map holds a `PartitionIndex`,
a `HashMap<u64, Slot>` of 16 byte slots (the archive as a `u32` number into the index's own table
of ids, a `u32` size, a `u64` offset) with the table beside it, in memory and in the saved map, so
a start loads the compact form and a fold writes it. rkyv's relative pointers bound a record
below 4 GiB, so the size fits. Every reader still sees an `ArchiveEntry`, built from the slot. A
loaded map is also taken as rkyv deserialized it, where it used to be copied entry by entry into a
second map that grew by doubling from a thousand.

**Measured on the lab** (`target/lab/r15/scale2/`), a fresh cluster grown to ten copies of the
dataset, 11.8 million Movie partitions a node: the archive maps held 615 MiB a node, where the same
data held 1.2 GiB before, and a node started again on it was 1.4 GiB resident, where it was 2.7 to
2.9 GiB.

### O84. An erasure code is run on bytes that have left the cache

| | |
| --- | --- |
| **Rank** | ~~**design input** for [M18](../object-storage/milestones.md#m18-erasure-coding): nothing is built that runs a code yet~~ **not taken** at M18: under the contiguous layout the user chose, a unit row is a unit of every data chunk, and they arrive a chunk apart, so a put encodes its stripe once it is whole ([F90](../features/erasure-coded-pools.md#a-put)); only round-robin units, which were rejected, would let a row be encoded as it arrives |
| **Impact** | Measured by [X4](../object-storage/erasure-coding-crates.md): the chosen crate encodes 4+2 at 64 KiB units at 20 GiB/s on europa when the stripe has left the cache and 97 when it has not, and at 7.6 and 9.9 on titan |
| **Difficulty** | M — the write path encodes a unit row as soon as its bytes are in, rather than once a chunk or a stripe has been gathered |
| **Depends on** | M18's write path; [Q26](../object-storage/contract.md#questions-to-answer)'s frame size, which sets how much of a stripe arrives at once |
| **Blocks** | nothing |
| **Tradeoff** | Contained — encoding a row at a time holds the parity of a partial stripe in memory until the stripe is whole, and a short write still encodes once |
| **Benchmark** | `shoal-spike-erasure` cold against hot; at M18, a Zen1 node's encode rate inside the write path against X4's two figures for the same crate |

Filed from X4. X4 measured every code twice: once over rows taken in turn from an arena larger
than any cache, which is what encoding a stripe gathered from a socket a while ago would see, and
once over one row in cache, which is what encoding bytes that just arrived would see. On europa
the fastest three Reed-Solomon crates and plain XOR all stopped at the same rate cold, about
20 GiB/s, which is what memory feeds one Zen4 core; in cache the chosen crate's GFNI kernels ran
almost five times as fast. On titan, with no GFNI, the gap is a third. A node that buffers a
stripe and then encodes it pays the cold figure. One that encodes each unit row while its bytes
are still in L2, which a frame of a few unit rows allows, pays the hot one. The same holds for a
degraded read's decode and for a parity delta. The decision belongs to M18, and the number to
hold it to is X4's. ✅ Decided by M18: the contiguous layout puts every row's last unit in the stripe's
last chunk, so the encode pays the figure for bytes gathered a stripe ago; F90's lab figures are a
Zen1 node's encode rate inside the put against X4's two.

### O85. A CRC is combined by the general method

| | |
| --- | --- |
| **Rank** | **design input** for [M13](../object-storage/milestones.md#m13-the-wire-and-the-baseline): nothing combines a checksum yet |
| **Impact** | Measured by [X5](../object-storage/checksums.md#a-combine): `crc-fast`'s `checksum_combine` takes 124 to 236 µs for CRC-64/NVME on titan, and 23 to 40 µs on europa, by the length of the second part. Zlib's method with the multiplier for a unit's length made once takes 78 ns on titan and 50 ns on europa |
| **Difficulty** | S. About sixty lines, the harness's `CrcMath` (`shoal-spike-checksum/src/sums.rs`), held to one call over the whole by the check X5 ran |
| **Depends on** | M13 taking the checksum X5 chose |
| **Blocks** | a chunk's digest made from its units' checksums, and a client's checksum bound to its unit's place, both of which are only worth doing at the cheap figure |
| **Tradeoff** | None. It is the definition's arithmetic, so it cannot disagree with the crate except by a defect the check finds |
| **Benchmark** | `shoal-spike-checksum`'s combine pass; at M13, a combine inside the write path against X5's 78 ns |

Filed from X5. A CRC whose initial value equals its final xor combines as `crc(a ‖ b) = crc(a)
· x^(8·|b|) mod P ⊕ crc(b)`. Every crate measured computes the multiplier `x^(8·|b|) mod P` on
every call. `crc32c` and `crc-fast` do it by zlib's older method, squaring a GF(2) matrix as
wide as the CRC (`crc32c` `src/combine.rs:39-85`, `crc-fast` `src/combine.rs:60`), which is why the
call costs more than checksumming the bytes would. A chunk unit has one
length, fixed by its pool, so the multiplier is made once a pool and a combine is one carry-less
multiplication modulo P. The harness does that multiplication bit by bit; a PCLMULQDQ form would
be faster still, and is not needed at 78 ns.

### O86. A unit is checksummed after its bytes have left the cache

| | |
| --- | --- |
| **Rank** | **design input** for [M13](../object-storage/milestones.md#m13-the-wire-and-the-baseline) and [M15](../object-storage/milestones.md#m15-replicated-pools-across-nodes): nothing checksums a unit yet |
| **Impact** | Measured by [X5](../object-storage/checksums.md#against-the-code): CRC-64/NVME at 64 KiB runs at 40.5 GiB/s on europa with the unit out of cache and 75.0 with it in, and at 11.4 and 13.1 on titan. A 4+2 stripe checksums half as many bytes again as it encodes, which on titan costs as much CPU as the encode |
| **Difficulty** | M. The unit is checksummed as its frames arrive, by the incremental interface, which X5 measured at 40.1 GiB/s cold and 62.1 hot on europa fed 4 KiB at a time, rather than in a pass over a buffered chunk |
| **Depends on** | [Q26](../object-storage/contract.md#questions-to-answer)'s frame size; [O84](#o84-an-erasure-code-is-run-on-bytes-that-have-left-the-cache), which asks the same of the encode |
| **Blocks** | nothing |
| **Tradeoff** | Contained. A unit's checksum is held as a running state until its last frame arrives; a CRC's state is eight bytes |
| **Benchmark** | `shoal-spike-checksum` cold against hot; at M13, a node's checksum rate inside the write path against X5's two figures |

Filed from X5, beside O84. On Zen1 the cache hardly matters: the CRC is bound by its own
arithmetic, 11.4 against 13.1. On Zen4 it is bound by memory out of cache and runs nearly twice
as fast in it. So the gain is a Zen4 node's, as O84's is. The parity units a code writes are in
cache when the code finishes them, so checksumming each as it is produced is the same idea on
the other side of the encode.

### O87. A placement answer is computed again on every lookup

| | |
| --- | --- |
| **Rank** | ~~**design input** for [M14](../object-storage/milestones.md#m14-devices-and-pools-on-one-node): nothing places a chunk yet~~ **built** by [F85](../features/devices-and-pools.md) as `PlacementCache`, and not yet acted on: ~~nothing places a chunk an operation until M15, whose stage is the first thing the benchmark below can time~~ [F86](../features/replicated-pools.md)'s coordinator builds its pool's view an operation and places every stripe it touches without the cache, which on the lab's three devices is a stripe's 132 ns; its stage is now what the benchmark below can time |
| **Impact** | Measured by [X2](../object-storage/placement-simulation.md#a-lookup): weighted rendezvous over a pool's slices costs about ten nanoseconds a slice on a Zen1 core. That is 132 ns on the lab, 12 µs at fifty hosts of twenty-four devices, and 40 µs when each of those devices has four slices; europa takes about half as long |
| **Difficulty** | S. A placement group's answer at a generation never changes, so a node keeps the answers it has computed, keyed by placement group and generation, and drops a generation's when no group it holds is at it |
| **Depends on** | M14's pool map and its generations |
| **Blocks** | nothing |
| **Tradeoff** | Contained. A cached answer is up to sixteen slice ids, 64 bytes, so the cache is bounded by the groups a node stages or reads for; a miss costs one lookup |
| **Benchmark** | `shoal-spike placement lookups`; at M14, a stage's and a read's time spent placing, against X2's figures for the pool's shape |

Filed from X2. The cheap lookup was drawing down the hierarchy, a host and then a device in
it, which costs 3.9 µs on titan where flat costs 40. X2 rejected it because a device's change
moves its host's weight and then moves chunks off devices that did not change, at 1.5 to 2.3
times the least. A cache makes flat's cost a cost per map change and per group, where the
hierarchy's cost is in bytes moved on every change. On the lab, with six slices, nothing needs
caching: the lookup is 132 ns.

### O88. A configured set lists its tablets one by one

| | |
| --- | --- |
| **Rank** | **low**: the frame is pushed per topology version, not per query |
| **Impact** | Measured by [X2](../object-storage/placement-simulation.md#todays-tablet-frame-again): at sixty-four members and sixteen tables the topology frame is 16,555 bytes, and 50,004 once sixty-four replica sets are configured. That is three times what it encodes and copies to every subscriber on every version: 88 µs to encode on titan where an unconfigured frame takes 30 |
| **Difficulty** | S. `ConfiguredSet::tablets` (`shoal-proto/src/shared/protocol/admin.rs:424`) is a list of every tablet the set serves, which for a set the rule made is the tablets `t ≡ k (mod N)`. Written as the rule's residue and modulus, or as runs, a set is a few bytes. That is a wire change to the topology frame, so it rides a frame version |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Contained. A client that reads the frame expands the set, which is what `TabletMap` does with the rule already |
| **Benchmark** | `shoal-spike fanout`'s table of configured sets |

Filed from X2, which measured the tablet frame again before holding the pool map's against it.
F45 publishes a configured set for every replica set a move changes, and a rebalance that moves
every set leaves every set configured. A cluster that has been rebalanced pushes three times the
frame of one that has not, for the same placement.

### O89. A reactor waiting on a fast device does not sleep

| | |
| --- | --- |
| **Rank** | **low** until a node shares cores: it costs power and a sibling's time, not throughput |
| **Impact** | Measured by [X6](../object-storage/device-store-ssd.md#8-one-device-several-slices) on europa's Optane, one executor reading at depth one. With 4 KiB reads completing in 17 µs, the executor thread slept 0.03 times a read and was on its cpu 99% of the window, at 55,000 reads a second. With 64 KiB reads completing in 43 µs it slept once a read and was 46% busy, and on the 970 EVO, at 100 µs, it slept every read. glommio's own runtime, without its waiting, was a quarter to a half of the thread's time |
| **Difficulty** | Unknown. The executor parks when no task is runnable (`glommio/src/executor/mod.rs:1520-1535`), and the reactor sleeps only when no ring has work and nothing woke (`glommio/src/sys/uring.rs:1795-1860`). Which condition keeps it awake with one read in flight was not traced |
| **Depends on** | nothing |
| **Blocks** | nothing; [S13](../object-storage/isolation.md)'s choice of executors for slices reads executor cpu, and should read it knowing this |
| **Tradeoff** | Contained: a reactor that sleeps on a fast device pays a wake-up a completion, which may cost latency. The trade is measured, not argued |
| **Benchmark** | `shoal-spike device slices`, its `depth-r4` rows: thread cpu and sleeps a read at depth one |

Filed from X6. The thread's cpu time is therefore not what a slice needs: for 4 KiB reads at depth
32 it was 100% of a core while glommio's own runtime was 32% on Zen1 and 51% on Zen4. The rate a
slice reaches is the measure X6 judged by; a node that put a slice beside a table shard on one core
would find this thread competing for it.

### O90. The WAL keeps a hundred bytes of memory for every retained entry

| | |
| --- | --- |
| **Rank** | **low** until a node holds many busy groups: it is bounded by retention, not by rows |
| **Impact** | Measured by [X10](../object-storage/stripe-row-costs.md#2-bytes-a-row) on the lab. After a load of four million rows and a restart, every node's WAL index held 414 MB, about 100 bytes an entry, beside 159 MB of archive map for the same rows: the process held 204 bytes a cold row where the rows' own index was 39. The WAL on disk was 1.65 to 1.69 GB a node. Every entry of the load was still retained: a group keeps 100,000 entries behind its checkpoint ([O67](#o67-ten-thousand-retained-entries-is-seconds-of-a-busy-group)), up to 1 GiB of sealed WAL a shard |
| **Difficulty** | M — the index of a sealed segment is read only to serve an append to a slow member or a snapshot, so it could be kept compact (an offset a segment and a dense array of lengths), or on disk beside the segment and read when a member falls behind |
| **Depends on** | nothing |
| **Blocks** | nothing; [S2](../object-storage/buckets.md#what-it-costs) counts a bucket's groups, and each busy one can hold up to 10 MB of this index |
| **Tradeoff** | Contained: a lookup into a sealed segment for a member far behind would cost a read, which a snapshot already costs more than |
| **Benchmark** | `x10 spike rows` (`shoal-spike-rows`), its `cold` side: `wal_index_bytes` against `archive_map_bytes` on every member |

Filed from X10. The index is `(u64, wal::Slot)` an entry with a B-tree's fill
(`shoal-core/src/server/wal/mod.rs`, `index_bytes`), and `Stats` reports it as `wal_index_bytes`.
A cluster with few busy groups never notices: it is a few hundred megabytes at most on the lab's
shape. A node hosting many groups, as buckets add two tables each, holds it for every busy group
whose entries are inside the retention window.

### O91. A merge reads the archived row an insert replaced whole

| | |
| --- | --- |
| **Rank** | **low**: a background read, off the commit path |
| **Impact** | Measured by [X10](../object-storage/stripe-row-costs.md#1-rows-a-second-a-group) on the lab, at depth 32 in one group: an overwrite of a resident row, a whole insert over it, read 139 to 254 device bytes a row while it ran, and an insert of a new key 23 to 49, on disjoint intervals. Within 15 s windows only part of a cell's segments were merged, so the whole cost is larger than the in-window figure |
| **Difficulty** | S — the compactor gathers the keys a sealed segment changed and reads the base of every one the archive map names before folding (`compactor.rs`), whatever the first intent for it is. A key whose first intent in the segment is an insert needs no base |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Contained: the fold already throws such a base away (`unsorted.rs`, the merge's insert arm) |
| **Benchmark** | `x10 spike rate`, the `overwrite` cells against the `insert` cells: device bytes read a row |

Filed from X10, where the code reading had found it first. Object metadata is written by
conditional updates, which do need their base, so the object store gains little from this; a table
of whole-row overwrites gains a read a key a merge.

### O92. A group reads the rows its parked batch needs one at a time

| | |
| --- | --- |
| **Rank** | **medium** for the object store: a stripe row is cold whenever its stripe was last written long ago |
| **Impact** | Measured by [X10](../object-storage/stripe-row-costs.md#thirty-two-at-a-time) on the lab: thirty-two writers committing cold stripe rows in one group committed 0.62× (0.47 to 0.63) the rows a second of thirty-two committing resident ones when a Zen1 host led it, and 0.91× when europa did, its archives on the Optane. At depth one a cold commit cost what a warm one did. A writer of resident rows beside eight writers of cold ones in its group had a p99 1.24× its p99 beside eight writers of resident ones |
| **Difficulty** | M — `run_apply` parks the batch on the first command that needs a load and returns; `resume_parked` re-runs it when that one load lands, and the next cold command parks it again (`shoal-core/src/server/shard/groups.rs`). Requesting a load for every cold command of the batch at once, and resuming when all have landed, turns N reads in series into N in parallel. The loader already reads on a task each |
| **Depends on** | nothing |
| **Blocks** | nothing; [S7](../object-storage/write-path.md)'s read before a commit hides the leader's read from the commit either way |
| **Tradeoff** | Contained: the batch still applies in committed order once every row is in |
| **Benchmark** | `x10 spike rows`, its depth 32 `cold` and `warm` sides on the group a Zen1 host leads |

Filed from X10. On the leader the read is on the client's path, since the proposer waits on its
own apply; on a follower it delays only that follower's apply.

### O93. kTLS receivers are not told records carry no padding

| | |
| --- | --- |
| **Rank** | **medium**: every byte a node or a client receives under TLS pays it, and object streams make that most bytes |
| **Impact** | Measured by [X11](../object-storage/streamed-bodies.md#1-one-connection-rate-and-cpu-by-frame) over loopback: a 1 MiB read stream's receiving cpu a gibibyte fell from 943 to 796 ms on titan and from 347 to 288 on europa when the receiving socket was told TLS 1.3 records carry no padding, and the host's from 1,890 to 1,754 on titan; at 4 MiB frames 869 to 734 and 310 to 266. A read stream to europa's file rose from 1,876 to 2,559 MiB/s at 4 MiB frames |
| **Difficulty** | S — one `setsockopt(SOL_TLS, TLS_RX_EXPECT_NO_PAD, 1)` after `ktls::enable` sets `TLS_RX` (`shoal-proto/src/shared/tls/ktls.rs`), on Linux 6.0 and later, where an older kernel's refusal is ignored. rustls never pads a TLS 1.3 record it sends. A peer that does pad is still read correctly: the kernel falls back for that record and counts it |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | None found; a padded record costs a retry of its decryption, and no Shoal peer sends one |
| **Benchmark** | `shoal-spike stream` section 1, its `ktls` and `ktls-nopad` sides; for the product, the `f14-encryption` transport arms before and after |

Filed from X11, which set it on both ends of its own connections and found the receiving side's
cpu lower on reads, where the client receives, and within noise on writes, where the server's file
was the bound.

### O94. A node's sockets set no `TCP_NOTSENT_LOWAT`

| | |
| --- | --- |
| **Rank** | **low**: long streams travel on connections of their own since [F73](../features/bodies-across-frames.md), so only a long answer on a pooled connection has small answers behind it |
| **Impact** | Measured by item 213's repeat of [X11](../object-storage/streamed-bodies.md#3-a-small-request-beside-a-stream)'s section 3, with the server writing small frames first: across 1 GbE from titan to hyperion, a small request's p99 beside a 1 MiB kTLS read stream on the same connection fell from 31.2 ms to 10.4 ms with the sending socket's low water mark at 16 KiB, and from 8.6 to 3.8 ms beside 256 KiB frames. A connection of its own was 1.6 ms, so the low water mark narrows the gap and does not close it |
| **Difficulty** | S — one `setsockopt(IPPROTO_TCP, TCP_NOTSENT_LOWAT, …)` on an accepted client connection, where the outbox (`shoal-core/src/server/shard/outbox.rs`) already writes every whole answer before the next data frame. Without the low water mark the kernel takes megabytes of a long answer at once, and the outbox's order decides nothing |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | More wakeups for the writer of a long answer: the repeat measured a 16 KiB mark costing a read stream's server 1.2 to 1.8 times its cpu a GiB across the network, more the larger the frame (1.70 to 1.77 at 1 MiB under kTLS). Under kTLS on the *sending* side of a write stream X11 also measured it making the tail far worse, so it is set on a node's answers only, never on a client's requests |
| **Benchmark** | `shoal-spike stream` section 3, its `shared` and `shared-lowat-16K` read sides; for the product, a small query's tail beside a long answer on one pooled connection, which no arm drives yet |

Filed from item 213's repeat of X11's section 3 ([Resolved #213](resolved/x11-setup-fifo.md)), which
found that X11's "the low water mark cannot help a read" had measured a server writing first in
first out: with small frames first the mark helps a read several times.

### O95. A wide row's bytes are copied and faulted in on every write

| | |
| --- | --- |
| **Rank** | **medium** while stripes could be rows ([S7](../object-storage/write-path.md)'s candidate A), **low** once they are not: tables of small rows barely pay it |
| **Impact** | Measured by [X3](../object-storage/bytes-through-groups.md#5-what-the-cpu-went-to) on europa, in a put arm of rows of 1 MiB and 4 MiB: 36 to 48% of a node's cpu was one loop in libc, its AVX-512 `memmove` copying backwards, and 11 to 14% the kernel zeroing pages a node had just allocated (`kernel_init_pages`, 19% of the node inclusive of the fault path at 4 MiB). Shoal's own code was 9 to 10%. Rows of 4 MiB stored about 30% less a core than rows of 1 MiB, on one node and on three, and the copy's share grew with the row, from 41% to 48% on the one node |
| **Difficulty** | M to find, unknown to fix. perf's DWARF unwinding stopped at the `memmove`, so its caller is not known: a WAL frame built by copying a row in, a row's `Vec<u8>` moved into a batch, rkyv's serializer growing its buffer, or a buffer grown in place by a reallocation that copies. A build of `x3-node` against a libc with frame pointers, or `perf record --call-graph lbr` on a host that has it, would name it. The faults say each copy lands in fresh memory, which a reused buffer would not |
| **Depends on** | nothing |
| **Blocks** | nothing; X3's comparison is two writes a byte against one, which no copy changes |
| **Tradeoff** | Unknown until the caller is: a pool of reused buffers trades the faults for memory held between writes |
| **Benchmark** | `x3 spike europa --size 4194304` (`shoal-spike-bytes`), its `put` side, with `results/x3-supplement.sh`'s pidstat and perf around it; for the product, `macro/grid/unsorted/r50/*` at its widest rows |

Filed from X3. A node storing rows of 1 MiB at a factor of three received each one, wrote it to its
WAL, sent it twice over kTLS and merged it, and 58 to 67% of its cpu went to moving the bytes:
the copy and the zeroing above, kTLS, and the kernel's own copies, sockets' and files', 5 to 7%. How much of the user space copy a row
needs is the question the caller answers.

### O96. A deep scrub reads every unit twice: once for its checksum and once for its summary

| | |
| --- | --- |
| **Rank** | **low**: the parity check runs on a sample of deep scrubs since [X12](../object-storage/recovery-scrub-rates.md), so its cost is the sample's, and at a device's scrub rate the fold is 0.2% of a Zen1 core on a disk and 2% on the 970 EVO |
| **Impact** | Measured by [X12](../object-storage/recovery-scrub-rates.md#1-a-cores-cpu)'s `codec`, one pinned core, 4 MiB chunks out of cache: CRC-64/NVME through `crc-fast` alone at 11.56 GiB/s on Zen1 and 50.5 on Zen4; with each unit then folded into its summary while it is in cache, 7.76 and 28.9. The fold adds 0.49 of the checksum's cost on Zen1 and 0.71 on Zen4, and a summary one 4 KiB block long instead of a unit long saves little, 0.43 and 0.62: the cost is a second pass over every byte, which a Zen1 core checksums at the rate it reads memory |
| **Difficulty** | M: a kernel of Shoal's own that XORs each 64-byte block into the summary inside the checksum's loop, so every byte is loaded once. `crc-fast` exposes no such hook, so it is either a fork of its x86 kernels or a CRC of Shoal's own held to CRC-64/NVME's published check value, which [X5](../object-storage/checksums.md) already holds every candidate to |
| **Depends on** | M17's deep scrub existing (✅ [F89](../features/scrub-and-repair.md)), and M18's parity check, which is what folds (✅ [F90](../features/erasure-coded-pools.md): its summary is folded in a second pass over each verified unit, as measured) |
| **Blocks** | nothing; with it the parity check could run on every deep scrub, which [P1](../object-storage/recovery-scrub-rates.md#how-it-was-judged) decided against at today's cost |
| **Tradeoff** | A checksum kernel of Shoal's own is a second implementation of a frozen definition, which is the risk X5 chose `crc-fast` to avoid |
| **Benchmark** | `shoal-spike device codec`, its `crc` and `crc+fold` ops; for the product, M17's background arm with the parity check on every scrub |

Filed from X12, whose P1 line fired on every host as written and in its supplement: the parity
check's fold is not expensive in absolute terms, but it is half again the checksum's pass.

### O97. A write bundle routed by topology is sent as many small frames

| | |
| --- | --- |
| **Rank** | **low** until the lab says otherwise: a bundle of one query, the common case, is one frame as before |
| **Impact** | ~~Predicted, not yet measured~~ The cut itself is not isolated, but cut this way routed bundles of 16 and 64 writes were 1.40 and 1.43 times as fast as the same bundles sent whole through one member on the lab ([F74](../features/client-routing.md#performance)), so the runs cost less than the hops they remove. Predicted: a bundle's runs are contiguous by construction, so at N nodes with keys spread evenly a bundle of n writes, each bound for its group's leader, is about 1 + (n − 1)(N − 1)/N runs - about eleven frames for sixteen writes on the lab's three nodes, and about 43 for 64 - over at most N connections. Each run is a frame of its own, coordinated by its node on its own: a header, an id, a base index and the archive of its queries, and a pass of `Queries::access` and routing. Before [F74](../features/client-routing.md) the bundle was one frame and two thirds of its writes took a proposal hop instead |
| **Difficulty** | S to M. Either a floor on a run's length, below which a query joins its neighbour's run and takes the hop; or a frame that names its queries' indexes, so one frame a node carries every query bound there, which changes `Queries` and every place the server derives an index from an offset |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | A floor gives back some of the hops routing removed; indexes on the wire cost bytes on every bundle and a change to the server's hottest loop |
| **Benchmark** | `shoaladm bench run --routing topology` against `--routing endpoints`, `insert100` and `rw50` at bundles 16 and 64, which F74's page reports bundle size by bundle size |

Filed from F74, which cut a bundle where its neighbours belong on different nodes because its runs
then need nothing new of the server.

### O98. A get whose keys live on several nodes is still gathered by one

| | |
| --- | --- |
| **Rank** | **low**: on a cluster whose factor equals its size every node holds every key, and a read at `One` is served whole where it lands |
| **Impact** | Unmeasured. A get of k keys over N nodes at a factor below N goes whole to the node holding the most of them, which forwards the rest and gathers the shares: a hop for each share on another node, and the gather's state, where a client sending each share to its holder and merging the answers would take none |
| **Difficulty** | M: the split, and the merge, order and limit `ShoalResponseSupport` already defines, run in the client, with an answer under the query's one index made from several frames' |
| **Depends on** | nothing; D7's step 4 ([D7](../direction/shard-aware-routing.md#4-client-side-merge)) |
| **Blocks** | nothing |
| **Tradeoff** | Merge work moves to the client, and a query's one answer is assembled from several connections, which the reorder buffer does not do today |
| **Benchmark** | the cluster fanout arms (`macro/cluster/fanout/*`) with a routing client beside the pinned one |

Filed from F74, which deliberately never splits a query.

### O99. A write's leader hint lapses with a read's, so a lead held away re-hops every group's writes in flight

| | |
| --- | --- |
| **Rank** | **low**: under one write in a hundred hopped on the lab, against three in five unrouted |
| **Impact** | Measured by F74's lab A/B at `1ae254b`: routed writes hopped about 150 proposals a second at bundles of 16 and 440 at 64, where at `77f37df`, before hints lapsed, they hopped about 50. A lead the balancer cannot hand back under writes stays away; its hint lapses every five seconds ([Resolved #223](resolved/leader-hints-lapse.md)), and every bundle planned before the next hop's answer returns goes to the preferred leader and hops. No throughput difference between the two commits was established |
| **Difficulty** | S: follow a write's hint without the lapse, and keep the lapse for strong reads, which nothing else corrects; and follow a hint only to a member that holds the tablet, so a hint naming a former holder cannot send writes to be forwarded for good |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | A strong read of a group whose lead is held away then hops once its hint lapses, unless writes to the group keep teaching it; with the lapse on both, the writes' re-hops are what keep the reads' hints fresh |
| **Benchmark** | `shoaladm bench` `insert100` and `rw50` at bundles of 16 and 64 with `--routing topology`, the members' `proposals_hopped` beside the rate |

Filed from F74's lab A/B, measured after the fix it follows from.

### O100. A table's archives on a rotational disk slow a load to a tenth and shed its writes

| | |
| --- | --- |
| **Rank** | **low**: no deployment puts a table's archives on a rotational disk, and rotational devices are M19's to build for objects |
| **Impact** | Measured on the lab by [F82](../features/root-capacity.md#performance)'s check on 2026-10-12, at F82's tree. The tmdb cluster had each node's archives under `/hdd`, the hosts' rotational disks (XFS), with its WAL and maps on its SSD as before. The full TMDB load ran three passes, each stopping in 86 to 124 s on `Shedding` (a group's log queue at 1,208 writes, a write waiting 1.7 s for a place) or a lapsed lease. Each started near 42,662 rows/s and fell to 2,000 to 9,600, and every `/hdd` was 90 to 98% busy with no client running while the merges drained. The same build with the archives beside the WAL wrote the whole dataset in 38.0 s at 89,114 rows/s |
| **Difficulty** | M to L, and not established where the time goes. A shard's compactor writes the archives on the shard's executor, beside the WAL and the queries. [X7](../object-storage/device-store-hdd.md) found a rotational device wants an executor of its own, never shared with an SSD's work ([Q23](../object-storage/contract.md#q23-what-a-rotational-device-needs-2026-10-06)). An archive root on such a disk would want the same, or writes in larger sequential runs |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | An executor apart for archives is a core apart from the shards, as X9 found for object work |
| **Benchmark** | `tmdb-dataset-loader load` against an inventory whose groups name `throughput:` on a rotational root, its rows a second and whether it finishes; or `shoaladm bench insert100` on the same inventory |

Filed from F82's lab check, which put the archives on a disk of their own to show every root reported.

### O101. A quorum read of every row runs at less than half its rate once every node has restarted

| | |
| --- | --- |
| **Rank** | **medium**: every rolling upgrade and every reconfigure restarts every node, and three whole reads after it the rate had not come back |
| **Impact** | Measured on the lab by [F83](../features/rack-and-zone-domains.md#the-tmdb-cluster)'s check on 2026-10-12. The tmdb cluster, up for hours since its load, read its 1,187,691 movies back with `verify --read quorum` in 4.1 s and its 1,188,009 release rows in 2.2 s. After a `reconfigure` restarted all three nodes, the same reads took 24.7 s and 10.1 s, then 10.6 s and 7.9 s, then 9.9 s and 8.4 s, with europa leading its 49 groups again by the third. F82's build, rolled back to and so restarted the same way, read them in 9.1 s and 9.0 s, then 8.4 s and 7.9 s, so it is not F83's. The keyword partitions moved less, 1.7 s against 1.8 to 3.0 s |
| **Difficulty** | Not established where the time goes. A node up since the load still holds rows it was written in memory, and a restarted one reads them from its archives through the paged index ([F76](../features/paged-archive-map.md)) and its page cache, under direct I/O that the kernel's page cache does not help. Whether that is the whole difference, and whether the rate comes back after more reads or never does, was not measured |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Unknown until the cause is |
| **Benchmark** | `tmdb-dataset-loader verify --read quorum` on a cluster up since its load, then again after `reconfigure` or `upgrade` restarts every node, several times over; or `shoaladm bench read100` against a cluster freshly restarted and one that is not |

Filed from F83's lab check, which restarted every node to commit a rack and a zone and put them back.

### O102. A whole chunk costs its device twice its bytes

| | |
| --- | --- |
| **Rank** | ~~**design input** for [M15](../object-storage/milestones.md#m15-replicated-pools-across-nodes), whose writes are whole chunks first~~ ~~**wanted**: [F86](../features/replicated-pools.md) built M15 on the store as it is, every put's chunk written whole this way~~ **built** by [F96](../features/chunk-file-classes.md): nothing is written ahead, and [X15](../object-storage/short-chunk-files.md) measured a whole 4 MiB chunk at 1.00 device byte a byte against 2.0; ~~its before and after on the lab is the store comparison's 16 MiB puts~~ on the lab's SSDs a 16 MiB put with three copies wrote 3.0 device bytes a byte against 6.0, at a rate europa's link bounds there ([F96's figures](../features/chunk-file-classes.md#on-the-lab)); ~~the disk leg judges its rate~~ on the disks, which bound it, 3.1 against 6.1, and sixteen in flight ran 1.58 times as fast; at 2+1 1.5 against 3.0, and 1.14 times |
| **Impact** | Not measured as its own figure. A whole chunk is written into a file the slice's pool zero-wrote ahead, and renamed over the chunk; the pool's refill then zero-writes a file of the same length to replace it, and a chunk written again frees its old file. So a device takes a whole chunk's bytes twice, once as zeros. [F85's retake](../features/devices-and-pools.md#on-the-lab) wrote whole 1 MiB chunks, one at a time, at 0.36 to 0.39 of one file synced each on titan's 970 EVO and 0.41 to 0.42 on europa's Optane, where X6 measured a recycled file alone, six at once, at 0.71 and 0.93. The refill is one of three steps the product adds to X6's, with the record's sync and the directory's, and how much of the gap it is was not measured apart. **Measured again in the comparison** (2026-10-25 and 26, [Shoal measured again](../object-storage/store-comparison.md#shoal-measured-again)): 16 MiB puts wrote 3.0 device bytes a byte with three copies and 1.5 at 2+1, as Ceph's do, and on disks ran 1.21 to 1.54 times as fast as the first run's at sixteen in flight |
| **Difficulty** | M. A chunk's old file, once its rename is durable, goes back to the pool instead of being freed, holding old bytes that never verify under another chunk's identity (P15), as X6's `chunk-recycle` leg did. The refill then writes only what the device lacks. The free pool's invariant that a file enters it whole stays: an old chunk is whole |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | A recycled file holds a chunk's old bytes until it is overwritten, which matters only to an operator reading the device raw. A rename that keeps the old inode needs a link before the rename and a directory sync after |
| **Benchmark** | `shoal-spike store --only whole`, and the device counters of `shoal-bench`'s `object-load` beside the reference cell |

Filed from [F85](../features/devices-and-pools.md), whose store writes whole chunks this way as X6
chose it, and whose quick run found the rate below X6's. A stripe shorter than its chunk pays a
whole chunk's file and its refill, not twice its own bytes: that is
[O107](#o107-a-stripe-shorter-than-its-chunk-costs-its-device-the-whole-chunk).

[F96](../features/chunk-file-classes.md) took another way than the difficulty above: it retired the
refill, and makes a whole chunk's file for it, rather than giving a replaced chunk's file back to the
pool. With nothing written ahead, a recycled file would save only a fresh file's allocation, so a
replaced chunk's file is still freed (F96's limitations).

### O103. A small write costs three times X6's on a device whose cache writes through

| | |
| --- | --- |
| **Rank** | ~~**design input** for [M15](../object-storage/milestones.md#m15-replicated-pools-across-nodes), whose small writes are staged and applied this way~~ **wanted**: [F86](../features/replicated-pools.md) stages and applies every write in place this way, and folds a small write's bytes through the same apply |
| **Impact** | Measured by [F85](../features/devices-and-pools.md#on-the-lab)'s retake on europa's Optane, XFS, under `performance`: a 4 KiB unit staged and applied one at a time took 255 to 258 µs at the median (stage 92 to 93 µs, apply 161 to 163 µs), where X6's harness took 81 µs for the same two flushes. On titan's 970 EVO the flushes dominate, 2.5 to 2.65 ms against X6's 2.0 ms rested, so it shows only where a flush is cheap |
| **Difficulty** | Not established where the time goes. An apply opens its chunk's file and closes it again, verifies every unit of its record, writes the unit and then the header as two writes, and joins a flush; a stage encodes, checksums and appends its record and waits on the committer. Each is a candidate, and the open-file cache in [todos](todos.md#what-f85-left-for-later) is the first to try |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Unknown until the cause is |
| **Benchmark** | `shoal-spike store --only small` on the Optane, against X6's `device` harness's partial write on the same device |

Filed from F85's lab retake.

### O104. An object lane copies a read's bytes twice and a stage's once more

| | |
| --- | --- |
| **Rank** | ~~**wanted** once a striped read's rate across nodes is measured against its device's~~ ~~**wanted**, small: measured on 2026-10-22, its two copies are about 8% of an object executor's time in 2+1 gets of 1 MiB on one host, where the executor is a node's busiest thread ([where the executor's time goes](../object-storage/store-optimizations/object-lane.md#where-the-executors-time-goes))~~ **built** by [F106](../features/lane-copies.md) (2026-10-23): a holder writes a read's answer from the buffers its store read it into, and the asker keeps the bytes in the body its link read; a stage's bytes are one allocation every holder's request shares, sent as a part of its frame and taken as a slice of the body where it arrives. On europa's loopback F106's own step ran the 2+1 gets 1.05 to 1.24 times as fast and the puts 1.03 to 1.08, its shards spending 0.78 to 0.88 of the cpu a GiB; with F105, the 2+1 gets ran 1.05 to 1.26 times as fast as on the build before both, every range apart, and three copies' 1 MiB gets at sixteen in flight 1.07 ([F106's figures](../features/lane-copies.md#on-europas-loopback), [both together](../object-storage/store-optimizations/object-lane.md#before-and-after)). Across hosts on SSDs F106's step spent 0.86 to 0.96 of the nodes' cpu a GiB on every arm and ran the puts at one in flight 1.04 and 1.05 times as fast, every range apart; the gets at sixteen in flight were europa's link's ([its SSD leg](../features/lane-copies.md#on-the-ssd-leg)) |
| **Impact** | ~~Not measured.~~ Measured on the lab's loop leg (2026-10-22): at 2+1 and sixteen in flight the executor's copies are 20% of its samples, these two about two fifths of them, beside the lane's TCP at 18% and its kTLS at 15%. A holder answers a read by encoding a `ChunkResponse` body, which copies the bytes into it; the requester's link reads the payload into an allocation of its own and `ChunkReply::decode_parts` copies it into the answer's `Vec`. A request is read whole into a `Vec` and decoded into its units' own. At 4 MiB a read, two copies a mebibyte on each node beside the kTLS and the device. **Measured again in the comparison** (2026-10-25 and 26, [Shoal measured again](../object-storage/store-comparison.md#shoal-measured-again)): on europa's loopback the 1 MiB gets at 2+1 ran 1.76 to 1.90 times as fast as the first run's, F107's cache beside these copies' removal, and are within Ceph and, at sixteen in flight, MinIO |
| **Difficulty** | S to M. The holder writes the head and the bytes as two parts of one vectored frame, as a forward's preamble and bundle are; the requester decodes the answer's bytes in place in the payload it read |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | A reply that borrows its buffer outlives the frame's allocation only as the answer it is |
| **Benchmark** | `shoaladm bench` `get100` at 1 MiB and 4 MiB on the lab and over loopback, each member's cpu a gibibyte read |

Filed from [F86](../features/replicated-pools.md), whose lane chose the simplest encoding first.

The [store comparison](../object-storage/store-comparison.md#without-the-link-europas-optane-over-loopback)'s
loop leg (2026-10-19) measured a striped read beside its devices: 1 MiB gets at 2+1, sixteen in
flight, ran at 1,749 to 1,779 a second with each device half busy and europa's cpus four fifths
idle, behind Ceph and MinIO by at least 40%. ~~Whether this entry's copies on a node's one object
executor are the bound is not established ([A 2+1 get crosses the object lane](../object-storage/store-optimizations/object-lane.md)).~~
O104's diagnosis (2026-10-22) found that executor a node's busiest thread at 77 to 85% of a cpu, and
these copies about 8% of its time. The lane's TCP over loopback and its kTLS are a third of it, and
the holder's own two copies of every read another 12%, filed as
[O127](#o127-a-holder-copies-every-byte-it-reads-twice-before-it-leaves-the-store) ([A 2+1 get crosses the object lane](../object-storage/store-optimizations/object-lane.md#where-the-executors-time-goes)).

### O105. A write in place covering a whole chunk is cut and checksummed on a table shard

| | |
| --- | --- |
| **Rank** | **wanted** if the reference cell's p99 moves beside writes in place |
| **Impact** | Not measured. A put's bytes are cut, checksummed and sent from an object executor, as X9 placed them. A write in place is coordinated on the shard the operation reached, and one that covers a whole chunk is cut into pieces with every unit's CRC-64/NVME computed on that shard, 0.4 to 1 ms of a core a 4 MiB chunk; a stage of units computes theirs there too, a few microseconds |
| **Difficulty** | S. The cut and its checksums run as a job on an object executor, as the put pipeline does, the shard waiting on its answer |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | A hop to an object executor and back for every whole-chunk write in place |
| **Benchmark** | `shoaladm bench` `--paced` beside `patch100` at a whole chunk's size, the paced table's p99 |

Filed from [F86](../features/replicated-pools.md).

### O106. Every object executor dials its own lane to every remote slice

| | |
| --- | --- |
| **Rank** | low until a node runs several object executors |
| **Impact** | Not measured. A node holds one object lane a remote slice for each of its object executors that sends to it, so a node of `e` executors in a pool of `s` slices on other nodes can hold `e × s` connections, each with a kTLS handshake, its buffers and a queue bound of its own. The lab runs one executor a node |
| **Difficulty** | M. Senders dealt by holder, so one executor holds one slice's lane and every other hands its requests over, which is what a table shard's request does today, at the hop's cost X11 measured |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Fewer connections against a put's bytes hopping an executor, 1.3 to 1.7 times the cpu a gibibyte by X11 |
| **Benchmark** | `shoaladm bench` `put100` at 4 MiB on a node of two object executors, connections and cpu a gibibyte |

Filed from [F86](../features/replicated-pools.md), which chose a lane an executor so a put's bytes
never hop.

### O107. A stripe shorter than its chunk costs its device the whole chunk

| | |
| --- | --- |
| **Rank** | ~~**wanted**: every object past the inline threshold and under a few MiB pays it, and on the lab's 970 EVO it, not the network, bounds a put up to 1 MiB~~ **built** by [F96](../features/chunk-file-classes.md): a chunk's file is the class of units it holds, made for it and synced by itself, once a cluster activates wire 16. [X15](../object-storage/short-chunk-files.md) measured a 64 KiB chunk at 1.13 to 1.29 device bytes a byte against 65; ~~its before and after on the lab is the store comparison's put arms~~ on the lab's SSDs with three copies a 64 KiB put wrote 4.8 device bytes a byte against 197 and ran 3.9 times as fast at sixteen in flight, 1 MiB 1.32 times; on the lab's disks 4.6 against 203, 3.1 times as fast at sixteen in flight, 1 MiB 2.3 times, and at 2+1 3.4 against 131, 2.5 times and 1 MiB 2.2 times; on europa's loopback 4.6 against 195 and 8.0 times with three copies, 3.1 against 64 and 3.0 times at 2+1 ([F96's figures](../features/chunk-file-classes.md#on-the-lab)); what bounds a 64 KiB put now is [O124](#o124-a-64-kib-put-keeps-its-970-evo-busy-at-a-tenth-of-its-bytes), on the disks as on the SSDs |
| **Impact** | On titan's 970 EVO, one node and eight in flight, a put of 64 KiB to 1 MiB ran at 48 to 58 a second whatever its size, a p50 of 119 to 141 ms, where [M13's rows](../features/object-wire.md#the-baseline) ran 726 a second at 64 KiB and 110 at 1 MiB; at 4 MiB and up both are the network's. On the lab's three nodes it was 49 to 59 a second against the rows' 540 and 63. The pool's device wrote about 300 MiB/s throughout ([F86's figures](../features/replicated-pools.md#on-the-lab)). Every stripe takes a free file of the pool's whole chunk length, 4 MiB and its header block at the default geometry, whatever its length. It writes only the units holding bytes into it, and the pool's refill zero-writes another file of the same length to replace it. So a put whose stripe holds `n` bytes costs its device about `4 MiB + n`: 66 bytes written for each byte sent at 64 KiB, 17 at 256 KiB, 5 at 1 MiB, and the two of [O102](#o102-a-whole-chunk-costs-its-device-twice-its-bytes) at 4 MiB. **Measured again in the comparison** (2026-10-25 and 26, [Shoal measured again](../object-storage/store-comparison.md#shoal-measured-again)): 64 KiB puts wrote 3.3 to 5.6 device bytes a byte where they wrote 67 to 217, and ran 2.6 to 4.7 times as fast at sixteen in flight, still behind every store but RustFS on SSDs, by at least 22% to 3.0 times; 1 MiB puts are within a few per cent there |
| **Difficulty** | M. A stripe's chunk written into a file of its own length, the units it holds and its header, written whole so no extent is left unwritten, which keeps the shared flush's rule; and a write in place or an append past that length staging the chunk whole again rather than its units, since a chunk is never extended in place. O102's recycling spares the refill only where a chunk replaces one, so it does not reach a store that grows |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | A short chunk's file is made for it, with its sync, instead of taken ready from the pool; an append that grows a short chunk rewrites it whole, which a small write past an object's end pays once a stripe |
| **Benchmark** | `shoaladm bench` `put100` at 64 KiB, 256 KiB and 1 MiB on one node and three, ops/s and device bytes a byte |

Filed from [F86](../features/replicated-pools.md)'s lab run, whose bench read the pool devices
for the first time.

### O108. A small write in its commit carries whole units, and is slower than staging it

| | |
| --- | --- |
| **Rank** | **wanted** before the small-write threshold's default is relied on: on the lab the path it chooses is the slower one |
| **Impact** | On the lab's three nodes, a 4 KiB write in place into a 1 MiB object rode its commit at 73 a second, p50 12.8 ms and p99 28.7, at one in flight, and 201 a second, p50 24.9 and p99 195, at eight. With `small_writes: 0`, staged, the same writes ran at 93 a second, p50 10.3 and p99 18.9, and 263 a second, p50 24.5 and p99 102 ([F86's figures](../features/replicated-pools.md#on-the-lab)). A row's pending bytes are whole units, 64 KiB at the default geometry, so a 4 KiB write commits 64 KiB through the row's log on every member, and its fold and clear are a second commit. [X8](../object-storage/small-writes.md) measured the commit carrying the write's own bytes, and found it cost more than a stage from 128 KiB up |
| **Difficulty** | M. Pending bytes kept as byte ranges rather than units, so a write commits only what it wrote; a read laying them over and a fold applying them take ranges, and a unit's checksum is computed at the fold |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Ranges in a row merge less simply than units, and the 64 KiB bound counts bytes rather than units |
| **Benchmark** | `shoaladm bench` `patch100` and `patch100-32k` at 1 MiB, one and eight in flight, `small_writes` at 64 KiB and at 0 |

Filed from [F86](../features/replicated-pools.md)'s lab run, whose patch leg ran both paths on the
same hosts.

### O109. A tablet's resident map starts empty and grows by doubling

| | |
| --- | --- |
| **Rank** | **watch**: a lean the lab's rule does not call a result |
| **Impact** | Measured on hyperion, not a result: `macro/insert_ephemeral` ran at 0.90 and 0.91 times its median before F87's resident maps, over four rounds and then eight, the ranges overlapping both times (120,771 [90,129–141,941] against 108,446 [101,074–116,126] at four, 124,665 [114,888–146,179] against 113,054 [97,436–134,509] at eight); every other arm was within noise ([F87](../features/tablet-walk.md#performance)). A table held one map, made with room for a thousand partitions, that grew by doubling as one; it holds a map a tablet now, each made empty at its tablet's first partition and doubling on its own, so a load into a fresh table allocates and rehashes about once a tablet a doubling where it did once a doubling |
| **Difficulty** | S. Make a tablet's map with room for a few partitions, or for the table's partitions a tablet when it was last shrunk |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | A map a tablet sized ahead costs its buckets in every tablet a shard holds anything of, which is the memory F87 gave back: on the lab a node's tables' indexes held 28.7 MiB where they had held 36.1 MiB |
| **Benchmark** | `macro/insert_ephemeral` and `macro/insert_unsorted`, before and after, eight rounds or more on a Zen1 host |

Filed from [F87](../features/tablet-walk.md)'s before and after, which repeated the arm at eight
rounds when four leaned and found it leaning the same way.

### O110. A rebuild's bytes pass through the node that drives it

| | |
| --- | --- |
| **Rank** | **wanted** once a rebuild's rate is measured against its link's |
| **Impact** | Not measured apart. A rebuild and a move are driven on the leader of the stripe's tablet group, which reads the chunk from a current holder and writes it to the destination, each over the object lane: where the driver's node is neither, every byte crosses the network twice, and on the lab's 1 GbE a rebuild between two other nodes runs at half the link at best. X12 rebuilt at 0.95 of the link's rate with the destination fetching for itself ([X12](../object-storage/recovery-scrub-rates.md#7-a-rebuild-across-hosts)) |
| **Difficulty** | M. A request to the destination's object executor naming its source, the label and the units, which reads the source over its own lane and answers once the chunk is written; the driver commits as it does now |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | A destination holds a request open while it reads another node, and the background gate runs on both ends of a request it did not make |
| **Benchmark** | `shoaladm bench` `--events device_fail` at 1 MiB on the lab, the rebuild's rate and each member's network bytes |

Filed from [F88](../features/recovery-and-moves.md#design-choices), which chose the driver's read
and write so a destination serves no request it did not already serve.

### O111. A placement group's move costs two control commits beside its own three

| | |
| --- | --- |
| **Rank** | **watch** until a pool of more placement groups than the lab's moves |
| **Impact** | ~~Measured in the fixture only~~ On the lab, these commits are what a failed device's moves wait for: under `rw50` at two in flight, about 2,700 placement groups moved in 3.4 to 4.8 minutes, while each move's own work, its begin to its switch, took 30 to 120 ms at the median and under a second at the most ([F88](../features/recovery-and-moves.md#on-the-lab), `1220ff0`). The control leader's node, europa, finished its share in one to two and a half minutes; titan and hyperion moved 1.8 to 2.7 groups a second each to the end, and their admissions and frees that took over 250 ms, thousands a run, took 0.76 to 3.5 s at the median and up to 6.2 s, against 0.27 s for the leader's own. With nothing written beside the moves, 256 MiB objects read one at a time, the same members' took 0.36 to 0.47 s, they moved 9 to 11 groups a second, and the device's groups settled in 90 s. Why a member's proposal through the leader slows so under a write load is not established; the followers' control logs share their device with the tables' WAL. In the fixture: a failed device's two thousand placement groups, most of them empty, moved in about a minute alone, sixteen at a time onto the device that took them, and in more than three beside the suite's other clusters at six threads, which `move_serves_reads_and_writes_throughout` found at its first deadline ([F88](../features/recovery-and-moves.md#performance)). Each move is admitted and freed by a commit of the control group, `AdmitMove` and `MoveDone`, beside its tablet group's begin and switch; ~~every one moves the topology version, so each map is pushed again to every shard of the cluster~~ since [Resolved #243](resolved/move-slots-on-the-map.md) neither moves the topology version, and an admission is asked for only where the applied slots leave room, so a refusal is no longer a commit. A device of a pool with many groups a tablet moves tens of thousands |
| **Difficulty** | S to M. Admit a batch of a group leader's moves in one command, and free them in one as they finish |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | A batch admitted together holds its slots until its last move ends unless it is freed piece by piece |
| **Benchmark** | the fixture's `an_operator_fails_a_device_and_its_groups_move_off_it` timed, and `shoaladm bench` `--events device_fail` |
| **Since** | [Resolved #250, the rest](resolved/empty-groups-move-without-a-slot.md) (2026-10-28): a group that holds nothing moves with no slot, so its move costs neither commit, and most of a failed device's groups hold nothing on a cluster not full of data. A group that holds stripes still costs both. Not timed, so not taken |

Filed from [F88](../features/recovery-and-moves.md).

### O112. A put under load marks its slow holder missed, and hands recovery a rebuild

| | |
| --- | --- |
| **Rank** | **watch** until a pool's stale positions are read beside its foreground on a busier lab |
| **Impact** | Measured on the lab, not apart: under `rw50` at 1 MiB, two in flight, ~~a pool held 500 to 1,250 stale positions in the steady arm with no device failed and nothing moving~~ a pool held 915 stale positions when a failed device's moves settled and 1,254 two and a half minutes later, while 46 were rebuilt (`pools`, 2026-10-15, at `baf6335`; that sentence misread the run, whose device had failed). The same engine's other runs of the arm ended at 0, 1, 1 and 238 and falling, and its steady arms held none at any poll ([item 247](known-issues.md#247-a-failed-devices-pool-gathered-stale-positions-after-its-moves-settled)). A put that has `k + f` holders waits `straggler_wait`, 200 ms, for the others and marks a slower one missed ([F86](../features/replicated-pools.md#a-put-past-the-threshold)), so every stripe a slow holder was late for is a whole chunk a rebuild later copies through the same busy slices, in their idle time ([F88](../features/recovery-and-moves.md#budgets)). On 1 GbE a 1 MiB piece behind two others is about 20 ms, and a slice behind the moves of a failed device far more |
| **Difficulty** | S to M. A straggler wait scaled by the piece's bytes over the link's rate, or a holder that answered its stage given until its write is durable before it is marked |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | A put that waits longer for its last holder has a longer tail; one that marks it sooner leaves more for recovery |
| **Benchmark** | `shoaladm bench` `rw50` at 1 MiB, two and eight in flight, the pool's stale positions and rebuilds per second beside the put's p99 |

Filed from [F88](../features/recovery-and-moves.md)'s lab run, whose device arms waited for no stale
position once this was seen. It is one candidate for the part of item 247 a full device does not
explain.

### O113. Recovery rebuilds the stripes of objects already replaced

| | |
| --- | --- |
| **Rank** | ~~**watch** until M20, whose grace says when a replaced object's stripes need no copies~~ **acted on in part** by [F92](../features/reclamation-and-device-lifecycle.md#a-retired-object): a reclaimed object's rows are tombstoned and deleted past its grace, which takes its stripes out of the record; inside the grace they are still rebuilt, and dropping them sooner is left |
| **Impact** | Inferred from the lab, not counted: in each of `shoaladm bench`'s three `kill` runs, `rw50` at 1 MiB over 64 objects, the pool rebuilt 533 to 583 positions in the half minute after titan returned, the puts it missed while it was down ([F88](../features/recovery-and-moves.md#on-the-lab), `1220ff0`). A put replaces its object whole, and the replaced object's stripes and rows stay until M20 reclaims them, so their missed positions stay in the record; at most 64 of those rebuilds were of an object not replaced since. Under a load of overwrites, what a returning node copies grows with the writes it missed, not with the live data |
| **Difficulty** | S, with M20. A rebuild of a stripe whose owner is retired and past its grace is dropped from the record instead of copied |
| **Depends on** | M20's grace and retirement |
| **Blocks** | nothing |
| **Tradeoff** | A reader that began before the replace, inside the grace, reads a stripe one copy short until it finishes |
| **Benchmark** | `shoaladm bench` `rw50` `--events kill`, rebuilds counted in `pools` beside the objects live |

Filed from [F88](../features/recovery-and-moves.md)'s lab figures.

### O114. A scrub pass commits a step for every placement group, empty ones included

| | |
| --- | --- |
| **Rank** | **low**: a pass is daily or weekly, and its steps are small commits on a reserved tablet |
| **Impact** | Measured in the fixture: a deep pass over a pool's 4,096 placement groups on three nodes took about seventy seconds with two objects stored, most of it a `ScrubStep` committed and three holders listed for groups that hold nothing ([F89](../features/scrub-and-repair.md#in-the-fixture)). On the lab, under `rw50` at 1 MiB, a pass that read about a gigabyte a host committed its 4,096 steps in 40 to 43 seconds, about a hundred a second beside the arm's own writes, and the put's p50 rose from 68 ms to 110 through it ([F89](../features/scrub-and-repair.md#the-foreground-through-a-deep-scrub)). A pool of one placement group a tablet commits 4,096 steps a pass a kind |
| **Difficulty** | S. A step every several placement groups, or after one that found anything, with the cursor naming the last group committed; a holder's listing of a group could also say it holds none of the group's chunks without a page each |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | A new leader goes back to the last step committed, redoing the groups after it: resumption is coarser by as many groups as a step covers |
| **Benchmark** | `shoaladm bench` `--events deep_scrub` on an object arm, its done mark against its request; `flipped_bit_is_found_by_a_read_and_by_a_deep_scrub`'s pass time in the fixture |

Filed from F89's fixture runs, and measured by its lab figures.

### O115. An erasure coded position is rebuilt whole for one missed unit

| | |
| --- | --- |
| **Rank** | **medium**: a write a node down missed costs its rebuild `k` whole chunks read and one written, where a replicated position's costs the units it missed |
| **Impact** | By construction: at 4+2 with 1 MiB chunks, a 4 KiB write missed by one position is rebuilt by reading 4 MiB and writing 1 MiB; a replicated position reads and writes 4 KiB ([F88](../features/recovery-and-moves.md)'s ranged record) |
| **Difficulty** | L: X1's model gives units only to a replicated chunk, so an erasure coded position's ranged entry has to be modelled first - the label a position was last current at, now one a row, has to be one a position, and a range rebuilt by decode stands on the sources' labels as well as the target's |
| **Depends on** | X1's model extended to it |
| **Blocks** | nothing |
| **Tradeoff** | A stripe row a label longer by `k + m - 1` labels, and a missed record that knows each position's base |
| **Benchmark** | `shoaladm bench` `--events kill` on an erasure coded pool, the background bytes a rebuild moved against the bytes the writes it missed changed |

Filed from [F90](../features/erasure-coded-pools.md#limitations), which rebuilds an erasure coded
position whole, as the model has it.


### O116. An erasure coded stripe's first data chunk favours the heaviest device

| | |
| --- | --- |
| **Rank** | **medium**: every read of an object shorter than a chunk is served by one device, and the placement rule picks a heavy device for it more often than its weight's share |
| **Impact** | Measured on [F90](../features/erasure-coded-pools.md#on-the-lab)'s lab, 64 objects of 1 MiB: at 2+2 and 3+2 by device, every read of the first run without an event came off europa's two devices (12.2 and 11.2 GiB) and titan's and hyperion's four read nothing; at 2+1 by host europa served 94 to 97% of the bytes read, and at 4+2, which places on all six devices, 59 to 66% |
| **Difficulty** | S to M: a stripe's positions are the rule's answer in score order, lowest first; drawing the set by weight and then the order by an unweighted draw of the group keeps the set and evens the order. It changes where an existing erasure coded pool's chunks are, so it waits for a pool map generation that moves them, or a pool made after it |
| **Depends on** | Nothing; the move of an existing pool's groups is F88's |
| **Blocks** | nothing |
| **Tradeoff** | None by weight: the devices chosen are the same, only which position each holds changes. A replicated pool's copies are alike, and it is unaffected |
| **Benchmark** | `shoaladm bench` `get100` at 1 MiB on a 2+2 or 4+2 pool by device, each host's device reads from the capture, before and after |

Filed from [F90](../features/erasure-coded-pools.md#limitations). `View::rule_indices`
sorts the chosen domains by their best slice's score, and a weighted rendezvous score is lower for a
heavier device, so position zero goes to the heaviest domain in the set more often than its share.
For a replicated pool the order is nothing; for a code it is which device holds data chunk zero,
the only chunk an object shorter than a chunk has. On the lab europa's Optane, the largest device,
won it.


### O117. An archive restore reads each archive two or three times, and copies each object on every try

| | |
| --- | --- |
| **Rank** | **medium**: a restore is rare, but what it reads grows with how far a resume has come, and part of its work is on shard zero's loop |
| **Impact** | Asymptotic. `ArchiveReader::open` verifies every record whole and checksums each object twice (`objects/archive.rs`, 580 and 588), `skip` decodes and checksums every record already put, and `next` reads the rest, so a resume of a 50 GiB archive past 40 GiB of puts reads 100 GiB. `put_again` (`shard/objects/archive.rs`, 232) computes again the CRC its record carried, and each try of a striped object copies it whole into owned 1 MiB pieces (249), up to `PUT_TRIES`, 32, one after another. `next`'s checksum, `put_again`'s and the copy run on shard zero's loop, about a second a 10 GiB object, and `read_manifest` is a synchronous `std::fs::read` on it (`shard/restore.rs`, 1026) |
| **Difficulty** | M: `skip` reads an entry and a length and seeks past the bytes, which `verify` proved; the record's CRC rides `ArchivedObject`; an object is one `Bytes` its pieces are slices of, which needs the stream channel to carry `Bytes`; the checksum moves into the blocking closure |
| **Depends on** | nothing; [item 264](known-issues.md#264-a-corrupt-length-in-an-archive-record-kills-shard-zeros-blocking-thread-and-its-restore-hangs)'s bound on a record's length is in the same function, [resolved](resolved/archive-record-length-bound.md), and its open half, an object held whole outside the assembly budget, is this entry's pieces |
| **Blocks** | nothing |
| **Tradeoff** | None for a valid archive: `verify` still reads every byte once |
| **Benchmark** | None exists. A restore of a bucket's archive in the fixture (`objects_survive_a_backup`), timed whole and from a resume, with shard zero's loop's longest stall through it |

Filed from a code review of [F93](../features/operations-and-the-real-cluster.md), read again
against the source before it was filed. The work on shard zero's loop makes F93's page wrong where
it says nothing of an archive runs on a shard's loop; the page says so beside the claim.

### O118. A striped get pays two strong reads, its entry's and its stripes' rows'

| | |
| --- | --- |
| **Rank** | ~~**wanted**~~ **decided, no default change** (2026-10-21): every get and stat a client sends without a level is a strong read, and a get past the inline threshold is two, one after the other. [F100](../features/lease-reads.md#performance)'s lease reads take each one's round away, small reads 1.9 to 4.1 times as fast on the lab's SSDs, and the user kept the round as the default: lease reads are a deployment's choice, since they rest on its clocks |
| **Impact** | Measured as a gap, not attributed. On the lab's SSD leg at one in flight, three copies, Shoal got 547 to 597 objects of 64 KiB a second (p99 2.76 to 2.98 ms) where Ceph's RGW got 1,193 to 1,222 (1.21 to 1.32 ms), and 2,007 to 2,159 of 4 KiB (one strong read, the object inline) where Ceph got 2,325 to 2,412 ([Shoal beside the others](../object-storage/store-comparison.md#where-shoal-is-better-and-where-worse)). An object read takes its entry at the operation's level, `Quorum` unless it names one (`shard/objects/mod.rs`, 105; `read.rs`, 81), then every stripe's row at `Quorum` whatever the operation asked (`read.rs`, 191; `mod.rs`, 696), each through openraft's `ReadPolicy::ReadIndex` (`shard/reads.rs`, 959): a round to a quorum of the group before it answers, [O49](#o49-one-barrier-per-group-per-bundle-rather-than-per-read)'s 590 µs at smoke scale. The rest of the 64 KiB gap is the chunk read, direct I/O every time, against an OSD answering from BlueStore's cache. Measured by [F95](../features/bench-object-read-levels.md) on the SSD leg, three copies (2026-10-20): at `One` a 64 KiB get at one in flight ran 1,591 a second against 593 at `Quorum`, and a 4 KiB get 13,334 against 2,094, every difference disjoint. A get at `One` goes to its client's own node, so at one in flight every one was answered on the driver's host and never crossed the link. Across the link a 4 KiB get at `One` took about 0.27 ms, on the driver's host 0.075, and at `Quorum` 0.48: the round, and what waits on it, is about 0.3 ms of a small read. At 64 KiB `One` takes the entry's round alone, since a get reads its stripes' rows at `Quorum` whatever it asked ([One beside Quorum](../object-storage/store-optimizations/strong-reads.md#one-beside-quorum)). **Measured again in the comparison** (2026-10-25 and 26, [Shoal measured again](../object-storage/store-comparison.md#shoal-measured-again)): every read still strong, 4 KiB gets and stats across hosts lead Ceph at sixteen in flight by at least 10% to 60%, and 64 KiB gets at one in flight are behind Ceph by at least 13% to 35%: with three copies on SSDs 809 to 855 a second against Ceph's 1,193 to 1,222 |
| **Difficulty** | M for a stripe row read under its entry's barrier where both groups are led on one node; L for a lease (`ReadPolicy::LeaseRead`, which openraft 0.10 has), since every strong read of every table would then rest on it |
| **Depends on** | a benchmark: `shoaladm bench` `get100` at 64 KiB at one in flight on three nodes, with the barriers counted, before and after; ~~since [F95](../features/bench-object-read-levels.md) the same arm at `--read-level one` beside the default says how much of the gap the rounds are~~ the same arm at `--read-level one` beside the default was measured with F95 (2026-10-20); ~~a choice among one round, a lease and `One` by the caller, which those figures inform~~ the choice made with them (2026-10-20): the small reads' tail placed first, by [F99](../features/strong-read-rounds.md); [O125](#o125-every-strong-read-sends-a-round-of-its-own-however-many-of-its-groups-are-in-flight) built if the round's queue is its cause; a lease decided on O125's figures. F99 placed the tail on busy shards, not on a group's queue of rounds, so O125 was not built, and the user chose a lease measured first (2026-10-21): [F100](../features/lease-reads.md), lease reads as a setting, off by default, measured beside the round; then the default. Measured on the SSD leg (2026-10-21): with lease reads on, 4 KiB gets ran 1.9 to 3.1 times as fast and stats 2.3 to 4.1 times, their p99 at sixteen in flight 0.69 to 0.89 ms where it was 7.46 ([F100's figures](../features/lease-reads.md#performance)) |
| **Blocks** | nothing |
| **Tradeoff** | `Major` for a lease: it holds only while clocks drift less than its margin, which nothing on a node measures. A stripe row read at `One` after a strong entry is linearizable only if every commit that changes a stripe's row is ordered behind the entry the read saw, which nobody has established; a holder found ahead of its row already sends a read back to the rows (`read.rs`, 205-212), and whether that is enough is the argument this entry would need |
| **Benchmark** | None isolates it. The store comparison's `get100/s64k/c1` is the figure it would move |

Filed from the [store comparison](../object-storage/store-comparison.md)'s SSD leg, read against the
source at the commit the leg ran.

### O119. A get from a pool of copies reads its coordinator's own copy, so each disk serves the gets its node leads

| | |
| --- | --- |
| **Rank** | ~~**wanted**: on a pool of copies, the share of a pool's reads that each disk serves is its node's share of the bucket's leaders, whatever the disk and whatever its queue~~ **built** by [F97](../features/disk-reads-by-queue.md) on disks: a read goes to the current holder with the fewest reads queued, a local copy on a tie; ~~its before and after on the lab is the comparison's gets on disks~~ measured on the lab's disks with F96 in (2026-10-21): **slower** at sixteen in flight, by 28% at 64 KiB and 11% at 1 MiB, since the reads it moved filled europa's 1 GbE link, which a choice by queue does not see ([F97's figures](../features/disk-reads-by-queue.md#performance)); ~~whether it stays is the user's choice~~ the user chose a margin, measured (2026-10-21): [F101](../features/disk-read-margin.md) reads the local copy first unless a node sets `disk_read_margin`, and then leaves it only for a queue shorter by more than the margin. On the disk leg a margin of four moved one or two reads in a hundred and lost nothing, and two captures a side could not tell it from each deployment's draw of leaders ([F101's figures](../features/disk-read-margin.md#performance)). With eight captures a side it changed no rate and shortened the tail of 1 MiB reads at sixteen in flight, and the user made it the default (2026-10-21) |
| **Impact** | On the lab's HDD leg, three copies, at sixteen in flight: 64 KiB gets kept europa's disk 95% busy and titan's and hyperion's 53 to 54%, at 586 to 595 a second; the same arm at 2+1, where an object this short lies in a data chunk placement put on some host, kept all three 72 to 92% busy at 1,395 to 1,408 a second. At 1 MiB europa's disk read 65 to 74 MiB/s at 95 to 96% busy while the others read 38 to 49 MiB/s at 46 to 60% ([Shoal beside the others, on disks](../object-storage/store-comparison.md#on-disks)). A get is sent to the leader of its path row's group unless it reads at `One` (`shoal-client/src/client/objects.rs`, 666-671), and that coordinator tries the current positions with its own first (`read.rs`, 371-373), so with three copies over three nodes every get is read from the coordinator's own disk. A fresh deployment's node ids draw again which groups each node leads, which is the drift between two of its captures. **Measured again in the comparison** (2026-10-25 and 26, [Shoal measured again](../object-storage/store-comparison.md#shoal-measured-again)): with F107's cache answering the working set, Shoal's disks were all but idle in the 64 KiB and 1 MiB gets at sixteen in flight, which lead Ceph and SeaweedFS with three copies by at least 37% and 28% at 64 KiB. A working set larger than the cache, read off disks, was not run, and there this stands |
| **Difficulty** | S to choose among the current holders by turn rather than by locality; M to choose by load, since a coordinator knows the queues of its own slices and no report carries a remote one's |
| **Depends on** | a benchmark: `get100` at 64 KiB and 1 MiB, sixteen in flight, on a three-copy pool of disks across three nodes, each disk's busy share read beside it, before and after |
| **Blocks** | nothing |
| **Tradeoff** | A remote read crosses a link a local one does not. On the SSD leg the local read is what put Shoal ahead at large gets over 1 GbE, so a choice that ignores locality would lose there what it wins on disks; the choice wants the local copy while its slice keeps up and another holder once its queue costs more than the link |
| **Benchmark** | None isolates it. The store comparison's `get100/s64k/c16` on the HDD leg with three copies is the figure it would move |

Filed from the [store comparison](../object-storage/store-comparison.md)'s HDD leg, read against the
source at the commit the leg ran.

### O120. A pool's inline threshold was set by the row path alone, and a 64 KiB object pays a whole chunk

| | |
| --- | --- |
| **Rank** | ~~**wanted**~~ **decided, no default change** (2026-10-21): the cheapest change the store comparison points to, a default rather than code, for objects between 16 KiB and the size at which a row stops paying. Measured on the SSD leg since 2026-10-20, where it is a trade rather than a gain alone, and on the disk leg since 2026-10-21, where it moves small objects off the disks onto the nodes' SSDs; ~~the default is the user's choice with both legs' figures~~ the user kept the default at 16 KiB, and `inline: 64KiB` is a pool's own choice, documented with these figures ([configuration](../getting-started/configuration.md#pools-and-buckets)) |
| **Impact** | An object at or under its pool's inline threshold is a row committed through its tablet group; past it, a stripe on a whole chunk ([O107](#o107-a-stripe-shorter-than-its-chunk-costs-its-device-the-whole-chunk)). In the store comparison Shoal's 4 KiB puts, rows, were within Ceph on SSDs and ahead of every store on disks, while its 64 KiB puts, stripes, were behind every store on every leg, by at least 2.2 to 15.7 times beside Ceph and SeaweedFS on disks ([on disks](../object-storage/store-comparison.md#on-disks)). On titan's 970 EVO, one node and eight in flight, M13's rows put 726 objects of 64 KiB a second where F86's stripes put 48 to 58 (O107). The default of 16 KiB is where X10's row path bent ([S18](../object-storage/contract.md), T3: an even mixture of inline puts and gets kept 0.58 to 0.71 of its 1 KiB rate at 16 KiB and 0.32 to 0.55 at 32 KiB), measured before a stripe's cost on a device was ([F86](../features/replicated-pools.md#on-the-lab)). Measured on the lab's SSD leg with three copies (2026-10-20, [On the SSDs, three copies](../object-storage/store-optimizations/inline-threshold.md#on-the-ssds-three-copies)). At `inline: 64KiB` Shoal put 683 objects of 64 KiB a second at sixteen in flight, against 257 at the default, and 131 against 41.9 at one, every run apart. Its gets of 64 KiB ran 1.24 to 2.16 times as fast, and the 4 KiB arms were no worse. The costs: each node's peak memory rose about 1.5 GiB over a 35 second run of puts at sixteen in flight, every object put resident on every node; the device bytes a byte put rose from 4.8 to 5.1 to 7.0 to 7.3, while the flushes a put fell from 2.75 to 1.02; and in the mixture at sixteen in flight the gets' p99 doubled, from 10.4 to 22.9 ms, at 2.58 times the rate. On the disk leg (2026-10-21, [On the disks, three copies](../object-storage/store-optimizations/inline-threshold.md#on-the-disks-three-copies)) the 64 KiB puts ran 10.4 and 11.0 times as fast at one and sixteen in flight, 132 and 681 a second against 12.7 and 62.0. Each object became a row on its nodes' SSDs, where the lab keeps every node's logs, rather than a stripe on three disks, which wrote nothing in those arms. Memory and device bytes rose as on the SSDs. **Measured again in the comparison** (2026-10-25 and 26, [Shoal measured again](../object-storage/store-comparison.md#shoal-measured-again)): at the default 16 KiB, after F96, 64 KiB puts are behind every store but RustFS on SSDs, by at least 22% to 3.0 times, and behind Ceph and SeaweedFS on disks by at least 33% to 9.9 times |
| **Difficulty** | S for a pool's default (`inline` is a pool's setting already); M to choose it from the pool's media, since a rotational pool's stripe costs a seek and a 4 MiB chunk where its rows are on the SSD |
| **Depends on** | ~~a benchmark: the comparison's `put100`, `get100` and `rw50` at 64 KiB, Shoal at `inline: 16KiB` and at `inline: 64KiB`, on the SSD and HDD legs, with the 4 KiB arms and each node's peak memory beside them~~ that benchmark, run on the SSD leg (`o120-ssd-rep3`) and the disk leg (`o120-hdd-rep3`); ~~the disk leg's is the first run of the disk chain.~~ ~~Then the user's choice of a default~~ The user kept 16 KiB (2026-10-21) |
| **Blocks** | nothing |
| **Tradeoff** | Every inline byte replicates through its group's WAL and is held in its tablet's partitions, so larger rows slow the row path for every object and table on the shard (X10's knee). An inline object is kept at the group's replication factor whatever its pool's redundancy, so at 2+1 a 64 KiB inline object costs three copies, not one and a half |
| **Benchmark** | ~~None yet.~~ `stores-lab.sh ab`, one build with its pool at `inline: 16KiB` and at `inline: 64KiB`, the comparison's arms at 4 and 64 KiB (`o120-ssd-rep3`, `o120-hdd-rep3`). The store comparison's `put100/s64k` is the figure it would move ([Shoal beside the others: optimizations](../object-storage/store-optimizations/inline-threshold.md)) |

Filed from the [store comparison](../object-storage/store-comparison.md).

### O121. Every get reads its bytes off the device, however often the object is read

| | |
| --- | --- |
| **Rank** | ~~**wanted**~~ **built** as [F102](../features/read-unit-cache.md), a setting ~~off unless named~~ an eighth of node memory unless named since [F107](../features/cache-default-share.md), for objects read again soon: every other store in the comparison served most of its repeated gets from memory. Measured on 2026-10-22: with 2 GiB a node, gets at one in flight ran 1.25 to 3.1 times as fast on every leg, the devices reading 0.00 to 0.05 bytes a byte served where they read 1.00 to 1.07; at sixteen in flight across hosts the bench driver's 1 GbE link held both sides back ([F102's figures](../features/read-unit-cache.md#performance)). Whether a node keeps one by default is the user's choice, with these figures |
| **Impact** | Measured as a gap. On the SSD leg at 64 KiB the other stores read 2 to 16% of the bytes they served off their devices and Shoal read 107 to 184%, and at one in flight Shoal's gets of 64 KiB and up were behind every store by at least 0.9% to 49%. On disks its 64 KiB gets at one in flight ran at 130 a second with a p99 of 27 to 45 ms, where the others read at most 3 MiB/s off any disk and answered at 1.3 to 2.1 ms ([on disks](../object-storage/store-comparison.md#on-disks)). A slice reads a chunk's range with direct I/O for every request (`server/objects/store/`), by design: the page cache was rejected because object bytes held there are held twice and counted by nobody ([S6](../object-storage/device-store.md#alternatives-rejected)). **Measured again in the comparison** (2026-10-25 and 26, [Shoal measured again](../object-storage/store-comparison.md#shoal-measured-again)): with the default eighth, on SSDs Shoal's devices read 4 to 31% of what it served of 64 KiB, where they read 107 to 184%, and its 64 KiB gets at one in flight ran 1.33 to 1.47 times as fast there and 4.4 to 5.5 times on disks, still behind every store by at least 8.5% to 35% |
| **Difficulty** | M: a cache of chunk units by chunk and label, ~~drawn from the object budget and counted in it~~ a budget of its own, the user's choice (F102), on the executor that owns the slice; a read takes from it only bytes at the label its row names, as it takes from a holder. [A cache of chunks a disk read](todos.md#a-cache-of-chunks-a-disk-read) is the narrower form already filed |
| **Depends on** | a benchmark: the comparison's `get100` at 64 KiB and 1 MiB, one in flight and sixteen, on the SSD and HDD legs, device bytes read a byte served beside each |
| **Blocks** | nothing |
| **Tradeoff** | Memory ~~taken from the object budget~~ of its own, `resources.object_cache`, carved out of `node_memory` beside the object budget, and a cache that ~~only helps a working set that fits in it~~ helps the reads it holds, in proportion (F102 on the lab's disks: 256 MiB a node, about half of what europa read, ran 1 MiB gets at one in flight 1.79 times as fast where 2 GiB ran them 3.11; 4 MiB a node, holding one 1 MiB read in a hundred on europa's loopback, ran those gets within the range of none, 2% slower at the median): the comparison's 1,024 objects of each size fit every host's memory, and a working set larger than memory costs every store a device read. A write in place, a rebuild and a reclamation must each drop what they change |
| **Benchmark** | ~~None isolates it. The store comparison's `get100/s64k/c1` is the figure it would move~~ F102's before and after: the comparison's `get100` at 64 KiB and 1 MiB on each leg, with no cache beside one, `stores-lab.sh ab` with `AB_<side>_CACHE` (`f102-*` under `shoal-spike/results/stores-ab/`), and device bytes read a byte served beside each ([Shoal beside the others: optimizations](../object-storage/store-optimizations/read-cache.md)) |

Filed from the [store comparison](../object-storage/store-comparison.md).

### O122. A small read's p99 across hosts is six times its mean, and nobody knows why

| | |
| --- | --- |
| **Rank** | ~~**to diagnose**~~ **placed, the change to choose**: ~~the cause is not known, so neither is the change~~ the tail is the strong read's, since it goes at `One` ([F95](../features/bench-object-read-levels.md)); ~~which part of the round is not known, so neither is the change~~ since [F99](../features/strong-read-rounds.md) it is placed on the round's work on busy shards of the Zen1 hosts, and the change is the user's choice. The user chose a lease, measured first: with [F100](../features/lease-reads.md#performance)'s lease reads on (2026-10-21), the p99 of 4 KiB gets and stats at sixteen in flight was 0.89 and 0.69 ms, where it was 7.46 |
| **Impact** | On the SSD leg at sixteen in flight Shoal's 4 KiB gets and stats had a p99 of 5.8 to 7.5 ms against a mean of about 1.1 ms, where Ceph's p99 was 1.8 to 3.3 ms; on disks 6.3 to 7.9 ms. On europa's loopback leg, three copies, the same arms had a p99 of 0.79 to 0.86 ms against a mean of 0.41 to 0.43 ms, below Ceph's 0.88 to 1.04 ([the comparison](../object-storage/store-comparison.md#where-shoal-is-better-and-where-worse)). Every one of these reads is a strong read, a ReadIndex round from the leader to a quorum ([O118](#o118-a-striped-get-pays-two-strong-reads-its-entrys-and-its-stripes-rows)); the tail comes with the link between hosts. At `One` (F95, 2026-10-20) the tail goes: the p99 at sixteen in flight was 0.60 ms for gets and 0.43 for stats, against 6.62 and 6.63 at `Quorum`, though ten of the sixteen streams read across the link at `One`. At `Quorum` it begins between four and eight in flight, and at sixteen a tenth of the reads take more than 3 ms while the median is 0.54 ([At One and by depth](../object-storage/store-optimizations/small-read-tail.md#at-one-and-by-depth)). Placed with [F99](../features/strong-read-rounds.md)'s counts (2026-10-20, [A round by its parts](../object-storage/store-optimizations/small-read-tail.md#a-round-by-its-parts)). At sixteen in flight the barriers hyperion waited for had a p99 of 5.89 ms, titan's 2.13 and europa's 0.51, though europa waited for more than half. Europa answered appends within 0.04 ms at the p99, yet hyperion's barriers took longer than its appends, and a tenth of the appends hyperion answered as a follower waited more than 1.05 ms. Every strong read sent a round of its own, two appends. **Measured again in the comparison** (2026-10-25 and 26, [Shoal measured again](../object-storage/store-comparison.md#shoal-measured-again)): every read still strong and no lease, the p99 was 2.2 to 3.6 ms on the SSD and disk legs against a mean near 0.7 ms, and 0.52 to 0.65 ms on loopback; what halved it was not separated |
| **Difficulty** | ~~Unknown until the cause is found~~ That of the change chosen ([O118](#o118-a-striped-get-pays-two-strong-reads-its-entrys-and-its-stripes-rows)) |
| **Depends on** | ~~the driver naming a read level ([F94](../features/bench-object-endpoints.md#limitations))~~ the driver names one since [F95](../features/bench-object-read-levels.md), so the same arm ~~can be run~~ was run at `One`, which takes no round, and the tail went; then a trace of a slow read's barrier, or each peer lane's socket queues sampled while the arm runs. [O125](#o125-every-strong-read-sends-a-round-of-its-own-however-many-of-its-groups-are-in-flight), a round for every strong read, ~~is the candidate openraft's source suggests~~ was the candidate openraft's source suggests; F99's counts placed the tail instead (2026-10-20), and the change is the user's choice |
| **Blocks** | nothing |
| **Tradeoff** | None known |
| **Benchmark** | The store comparison's `get100/s4k/c16` and `stat100/s4k/c16` on the SSD leg, p99 at `Quorum` and at `One` ([Shoal beside the others: optimizations](../object-storage/store-optimizations/small-read-tail.md)) |

Filed from the [store comparison](../object-storage/store-comparison.md).

### O123. A whole chunk staged into a fresh file pays its own sync and its pool directory's beside its record's

| | |
| --- | --- |
| **Rank** | **wanted**: every whole stage on a device that flushes its cache pays it, a write's whole chunk and every erasure coded write's |
| **Impact** | Measured by [F96](../features/chunk-file-classes.md#the-stores-own-figures)'s retake of the store: 1 MiB whole chunks of a 1 MiB pool, staged and applied one at a time under the shared flush, ran at 67 to 68 MiB/s on titan's 970 EVO against 75 to 76 at the commit before F96, every round apart, and 4% faster on europa's Optane, where a sync costs no flush. Since F96 a staged whole chunk's fresh file is made durable by its own `fdatasync`, renamed into the pool and the pool's directory synced, then named by its record and that synced; before it a file of the pool was overwritten and made durable by the record's sync alone, since the journal is on the chunk's device. On the 970 EVO each sync is a cache flush. A put's chunk is placed without a record, and X15 measured that path faster with fresh files at every size |
| **Difficulty** | S to M. The adopt's rename and its directory sync could go: a record could name the file under its temporary name, which a start then keeps rather than removes, though the file's directory entry still has to be durable before the record is. Or the directory syncs of stages in flight together could be one, as the shared flush is one flush for every apply completed before it |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | A record that names a temporary file makes the start's rule about temporary files one with an exception, which the dm-log-writes test would have to hold too |
| **Benchmark** | `shoal-spike store --only whole,short` on the 970 EVO and the Optane; and writes in place on an erasure coded pool, every one of which is staged, which the store comparison's arms do not run: its puts, erasure coded ones too, are placed |

Filed from [F96](../features/chunk-file-classes.md), whose retake of the store measured it.

### O124. A 64 KiB put keeps its 970 EVO busy at a tenth of its bytes

| | |
| --- | --- |
| **Rank** | **wanted**: what is left of the largest gap the store comparison found, 64 KiB puts behind Ceph and SeaweedFS with three copies and behind Ceph and MinIO at 2+1, once F96 took away their bytes |
| **Impact** | Measured by [F96](../features/chunk-file-classes.md#on-the-lab)'s before and after on the lab's SSD leg, with three copies and sixteen in flight. Shoal put 263 objects of 64 KiB a second. Titan's and hyperion's 970 EVOs were 94% busy writing 24 MiB/s, and took 7.4 device write requests a put. Before F96 they were 95% busy writing 287 MiB/s. In the comparison's capture Ceph put 529 a second, its 970 EVOs 90% busy writing 39 MiB/s, at 4.2 writes a put ([the comparison](../object-storage/store-comparison.md)). Beside that capture, which ran on another day, Shoal is still behind Ceph by at least 31% and SeaweedFS by at least 43%. The writes were not counted by their source; the source says where they can come from (read 2026-10-20). **A holder places a put's chunk with four syncs of its own** (`Slice::apply_whole`, and the whole write's finish, `store/slice.rs`): the fresh file's `fdatasync`; the placement group's directory, after the object's directory is made in it; the object's directory, after the rename; and the pool's, which the file left. Each directory is opened afresh, and none of the four is shared with another chunk in flight. Before F96 the same three directory syncs followed the shared flush. **A put commits its object's row twice**, at registration and to make it current, and its stripe's row as well only when a holder was missed. Every member of the row's group syncs each entry in its shard's WAL batch, with a commit marker after it, and appends that arrive during a sync share the next. Each sync is a cache flush on the 970 EVO. Shoal's 4 KiB puts, rows alone, took 1.35 writes a put at 1,225 a second on the same devices. At 2+1 the same day Shoal put 301 a second, its 970 EVOs 93 to 94% busy writing 19 MiB/s at 6.0 to 6.1 writes a put, though each holds a chunk of only two puts in three. **The count of writes does not order the stores.** In the comparison's captures SeaweedFS's 970 EVOs took 1.1 to 1.2 writes a put at 510 puts a second, 94 to 95% busy, with three copies; at 2+1 RustFS's took 3.2 at 321 a second, 91% busy, and Ceph's 4.1 to 4.3 at 645. What each write costs the device, a cache flush or not, is what the LVs' counters cannot show. **On the disks a 64 KiB put is bound by its writes too** (2026-10-21, F96's before and after with three copies). Each disk made 143 to 148 writes a second before F96 and after it, their write caches off. A 64 KiB put took 2.6 to 2.7 of them at 55.5 puts a second, down from 8.1, and titan's and hyperion's disks were 87 to 91% busy writing 4.1 MiB/s. **Measured again in the comparison** (2026-10-25 and 26, [Shoal measured again](../object-storage/store-comparison.md#shoal-measured-again)): Shoal put 261 to 272 objects of 64 KiB a second, titan's and hyperion's 970 EVOs 93% busy writing 24 MiB/s, still behind Ceph by at least 31% and SeaweedFS by at least 43%; on disks the same arm kept the disks 67 to 93% busy writing 3.3 to 4.6 MiB/s |
| **Difficulty** | Not known until the writes are counted by source: the flushes a put on the NVMe device under the LV (the LV's own counters report none), and the WAL's syncs apart from the chunk's. If the chunk's syncs are most of them, S to M: XFS and ext4 commit metadata through one journal in order, which the shared flush already relies on, so one directory sync after the object's directory is made and the file renamed makes both durable; and the directory syncs of chunks placed together could be one, as [O123](#o123-a-whole-chunk-staged-into-a-fresh-file-pays-its-own-sync-and-its-pool-directorys-beside-its-records) proposes for stages. The file's own sync stays before its rename |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | A directory sync left to the journal's order holds on XFS and ext4, the filesystems a device may be, and not on a filesystem that orders nothing; the dm-log-writes test would have to replay it |
| **Benchmark** | The store comparison's `put100/s64k/c16` with three copies on the SSD leg: puts a second, and each 970 EVO's writes and flushes a put and its busy share, beside Ceph's |

Filed from [F96](../features/chunk-file-classes.md)'s before and after on the lab.

### O125. Every strong read sends a round of its own, however many of its group's are in flight

| | |
| --- | --- |
| **Rank** | ~~**a candidate** for [O122](#o122-a-small-reads-p99-across-hosts-is-six-times-its-mean-and-nobody-knows-why)'s tail: read in openraft's source, and not yet shown to be its cause~~ **not taken**: not O122's cause on the lab ([F99](../features/strong-read-rounds.md), 2026-10-20). Every strong read does send a round of its own, but the comparison's reads spread over a table's 90 groups, so a read seldom finds its group's round out. Kept for a workload whose reads crowd few groups |
| **Impact** | Not measured. A `Quorum` read's barrier on its group's leader is openraft's `ReadPolicy::ReadIndex` (`shard/reads.rs`, `read_barrier`), and openraft 0.10.0-alpha.34 answers each one with a round of its own (`handle_ensure_linearizable_read`, `core/raft_core.rs`): an empty append to every other voter, a task for each, and one more task to wait for a quorum of their answers. Each follower's core takes each append in turn. So sixteen strong reads of a group in flight are sixteen rounds, where one round sent after they had all arrived would confirm every one of them. [F95](../features/bench-object-read-levels.md)'s figures fit this without showing it. At `Quorum` on the SSD leg the rate rose 21% from eight reads in flight to sixteen, against 55% from four to eight, while a tenth of the reads took more than 3 ms. At `One`, which sends no round, there was no tail, and on loopback, where a round costs little, there is none at `Quorum` either ([At One and by depth](../object-storage/store-optimizations/small-read-tail.md#at-one-and-by-depth)) |
| **Difficulty** | M: one round in flight a group, and every read that arrives while it is out waits for the next, which answers them all. It could be built in Shoal's barrier or in openraft, whose source plans the same ("do not spawn, manage read requests with a queue by RaftCore") |
| **Depends on** | ~~O122's trace of a slow read~~ ~~[F99](../features/strong-read-rounds.md)'s counts of a round's parts, to show reads waiting on rounds~~ F99's counts, taken on 2026-10-20: they show reads waiting on busy shards, not on one group's rounds ([A round by its parts](../object-storage/store-optimizations/small-read-tail.md#a-round-by-its-parts)); then a benchmark, before and after |
| **Blocks** | nothing |
| **Tradeoff** | A read may share only a round sent after it arrived, or it is not linearizable. A read that arrives just after a round left waits out the rest of that round and the whole of the next, up to two rounds where it paid one. At one in flight nothing changes |
| **Benchmark** | The store comparison's `get100/s4k/c16` and `stat100/s4k/c16` on the SSD leg at `Quorum`: rate and p99 |

Filed from F95's figures for O122, against openraft 0.10.0-alpha.34's source as read on 2026-10-20.

### O126. A holder syncs two directories for every chunk it reclaims

| | |
| --- | --- |
| **Rank** | **wanted** where small striped objects are deleted or replaced often. Found by [O120](#o120-a-pools-inline-threshold-was-set-by-the-row-path-alone-and-a-64-kib-object-pays-a-whole-chunk)'s before and after, where it slowed the reads that ran beside it |
| **Impact** | Measured on the lab's SSD leg with three copies (2026-10-20, [Deleting them](../object-storage/store-optimizations/inline-threshold.md#deleting-them)). The bench deleted about 9,000 objects of 64 KiB, each a stripe. Over the next 37 to 47 seconds titan's and hyperion's NVMe each made 1.9 to 2.1 flushes and wrote 45 to 50 KB for each object; putting them had taken 2.75 flushes a put. A run of 4 KiB gets at one in flight beside the reclamation had a p99 of 2.83 ms, against 0.81 beside nothing, and the devices wrote 25 MB/s across the hosts during it. Objects held inline cost nothing like it: deleting 24,000 to 26,000 rows of 64 KiB made 0.6 flushes an object while the deletes ran, and almost none after. The flushes were not counted by their source; the source says where two can come from (read 2026-10-20). **A holder syncs a directory for each step of a chunk's removal** (`Slice::free_file` and `remove_empty`, `store/slice/reclaim.rs`). First the object's directory, once the chunk's file is unlinked from it, or that and its pool's directory when the file is renamed into its pool. Then the group's directory, once the object's emptied directory is removed from it. Each sync is a cache flush on the 970 EVO, and none is shared with another chunk's removal |
| **Difficulty** | S to M, as [O124](#o124-a-64-kib-put-keeps-its-970-evo-busy-at-a-tenth-of-its-bytes)'s directory syncs are: the syncs of chunks removed together could be one, as [O123](#o123-a-whole-chunk-staged-into-a-fresh-file-pays-its-own-sync-and-its-pool-directorys-beside-its-records) proposes for stages |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | A file renamed into its pool is given back only once its new name is durable (`free_file`), so a sync shared by several removals gives each file back later |
| **Benchmark** | None isolates it. In O120's arms it is the cost to a 4 KiB read arm at one in flight run after `put100/s64k/c16`. The same objects put, deleted and waited out with `tmdb-dataset-loader reclaim fill`, `delete` and `wait`, with each NVMe's flushes counted beside them, would isolate it |

Filed from O120's before and after on the lab.

### O127. A holder copies every byte it reads twice before it leaves the store

| | |
| --- | --- |
| **Rank** | ~~**wanted**: about 12% of an object executor's time in 2+1 gets of 1 MiB on one host, and every read pays it.~~ **built** by [F105](../features/store-reads-in-place.md) (2026-10-23): a range is read as one run on every device, verified where the device put it, and answered from those buffers; a local read copies it once to cross to its shard, and what is left is [O129](#o129-a-local-read-still-copies-its-range-once-to-cross-to-its-shard-and-scrubs-and-records-copy-theirs). On europa's loopback its 1 MiB gets ran 1.19 and 1.18 times as fast at 2+1 and 1.31 and 1.07 with three copies, every range apart, their object executors spending 0.78 to 0.85 and 0.40 to 0.47 of the cpu a GiB; its 64 KiB gets, one unit each, were within ([F105's figures](../features/store-reads-in-place.md#on-europas-loopback)). Across hosts on SSDs its 1 MiB gets at one in flight ran 1.15 times as fast, 1.08 between two captures that drew alike how many gets europa's own node answered, and at sixteen in flight they were europa's link's ([its SSD leg](../features/store-reads-in-place.md#on-the-ssd-leg)). Found by [O104](#o104-an-object-lane-copies-a-reads-bytes-twice-and-a-stages-once-more)'s diagnosis |
| **Impact** | Measured on the lab's loop leg (2026-10-22, [where the executor's time goes](../object-storage/store-optimizations/object-lane.md#where-the-executors-time-goes)). `read_exact` copies each piece of a unit out of glommio's DMA buffer into a `Vec`, and `read_chain` copies each unit's covered part into the range's own. At 2+1 and sixteen in flight an executor's copies are 20% of its samples, and these two are about three fifths of them |
| **Difficulty** | S to M. The range's units are read into one buffer the range owns, verified where they lie, and answered as it is |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | The buffer is held until the read is answered, and a cache of read units ([O121](#o121-every-get-reads-its-bytes-off-the-device-however-often-the-object-is-read)) still copies the units it keeps |
| **Benchmark** | `get100` at 1 MiB on the loop leg with three copies and at 2+1, each thread's cpu (`pidstat -t`) and the executor's profile |

A copy of glommio's buffer is how `read_exact` gives every caller an owned `Vec`, and the range's
copy is how `read_chain` lays units from records, memory and the file side by side. Both are the
simplest form, chosen before any read's rate was measured.

### O128. A coordinator copies a read's range twice before it answers, and checks it again

| | |
| --- | --- |
| **Rank** | ~~**wanted** once F105 and F106 are measured~~ ~~**wanted**: the copies a get's bytes still take on a table shard, the largest cost left in a 2+1 get since F105 and F106 were measured (2026-10-23).~~ **built** by [F109](../features/reads-from-stripe-buffers.md) (2026-10-24), ~~to be measured in lab 3b~~ measured in lab 3b (2026-10-25): on loopback the 1 MiB gets ran 1.06 to 1.37 times as fast, their table shards spending 0.67 to 0.94 of the cpu a GiB and `memmove` 2% of the shards' samples where it was 23%. A read's range is the parts its stripes were read in, its frames written from them and its checksum folded from them a step at a time, a hole's by its length. There were three copies in a 2+1 get, not two: `read_parts` laid each data chunk's part into a buffer of its own first. What is left is the checksum's one pass ([O132](#o132-a-coordinator-checksums-every-byte-of-a-reads-range-its-holders-checked-already)) and the client's copies ([O133](#o133-a-client-copies-every-object-answer-out-of-the-buffer-its-frames-filled)). Found by reading the read path while O104 was planned |
| **Impact** | Argued from the source (2026-10-23). A get's coordinating shard appends each stripe's bytes to the range it answers (`read_stripes`), copies the range again into the answer's buffer behind its head (`answer_reply`), and takes the range's CRC-64/NVME over it, where every unit was checked on its holder. That is two copies and a pass of every byte a get moves, on a table shard. Measured on lab 2's loop leg (2026-10-23): after F105 and F106, the shards' copies in user space were 24% of their samples in 2+1 gets of 1 MiB at sixteen in flight, 322 samples a GiB the gets moved, where they were 313 before both and the object executor's own copies fell from 295 to 45 ([where the executor's time goes since](../object-storage/store-optimizations/object-lane.md#where-the-executors-time-goes-since)) |
| **Difficulty** | M. The answer would be written as its head and the stripes' bytes where they arrived, as a vectored frame, and its checksum combined from the units' |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | An answer that holds its stripes' buffers until it is written holds them on the shard |
| **Benchmark** | `get100` at 1 MiB on the loop leg, each coordinating shard's cpu (`pidstat -t`) |

Filed with [F106](../features/lane-copies.md), which took the lane's copies off and left the
coordinator's.

### O129. A local read still copies its range once to cross to its shard, and scrubs and records copy theirs

| | |
| --- | --- |
| **Rank** | ~~**wanted** where local reads' rate matters: the copy every local get pays.~~ **built** by [F110](../features/store-checks-in-place.md) (2026-10-24), ~~to be measured in lab 3b~~ measured in lab 3b (2026-10-25), where it moved nothing: every rate within and the object executors' cpu a GiB 0.97 to 1.08 of F109's, a heap buffer's read costing about what the copy did ([O136](#o136-a-local-read-into-a-heap-buffer-forgoes-the-rings-registered-buffers)). A read whose answer crosses to a shard fills a buffer of the heap's with the fork's new `read_into_at` and hands it over whole, its draw with it; the scrub's, the verification's and the headers' reads check where the device put the bytes, and a record's units are applied from the journal's buffers. A cache's fill still copies the units it keeps ([O130](#o130-a-lanes-buffers-are-fresh-allocations-a-cache-hit-is-copied-to-be-sent-and-answers-go-a-frame-a-call)). Found by reading the store while [F105](../features/store-reads-in-place.md) took two copies off every read |
| **Impact** | Argued from the source (2026-10-23). Since F105 a read off a slice is answered from the buffers the device put it in, but a few paths still copy what they read. A local read's span crosses to its shard as a `Vec`, a step at a time (`Span::into_vec`): once a byte, on the executor. A deep scrub's and a verification's units (`verify`, `verifies`) are copied out of glommio's buffers by `read_exact`, as a chunk's header is. A staged record's units come from the journal as owned buffers (`Journal::read_units`), and a range they overlay is laid out once |
| **Difficulty** | M for a local read: the buffer it reads into would need to be one a shard can hold, which takes a read in the glommio fork that fills a caller's buffer and gives it back. S for the scrub's and the verification's units, which can be checked where they lie as a read's are |
| **Depends on** | a read into a caller's buffer in the glommio fork, for the local read |
| **Blocks** | nothing |
| **Tradeoff** | A buffer handed to a shard holds its draw until the shard drops it, on another thread |
| **Benchmark** | `get100` at 1 MiB on the loop leg with three copies, where every get is local, each executor's cpu (`pidstat -t`) |

Filed from F105, which built [O127](#o127-a-holder-copies-every-byte-it-reads-twice-before-it-leaves-the-store) and left these.

### O130. A lane's buffers are fresh allocations, a cache hit is copied to be sent, and answers go a frame a call

| | |
| --- | --- |
| **Rank** | ~~**wanted**, small, after F106 is measured.~~ **built** by [F111](../features/lane-buffers.md) (2026-10-25), ~~to be measured in lab 3b~~ measured in lab 3b (2026-10-25): at 2+1 the 1 MiB gets at sixteen in flight ran 1.11 times as fast, with three copies the 1 MiB puts 1.02 and 1.04, the object executors' cpu a GiB 0.83 to 0.94 where it moved and `memmove` 4.8% of their samples where it was 12.9%. A served lane writes every answer ready together in one call within 64 answers and 4 MiB; a cache hit is slices of the units it holds; a whole chunk's piece is read straight into the buffer its device writes from; and every other request body is read into the lane's arena. The client relay and the asking link still write a frame a call ([O134](#o134-a-client-relay-and-an-asking-link-write-a-frame-a-call)). Found by reading the lane while F106 was built |
| **Impact** | Argued from the source (2026-10-23). Every frame body a link reads is a fresh allocation, whose pages the kernel zeroes as it maps them, though `read_body` no longer does. A read the cache of read units answers is copied into a buffer of the span's own before it is written ([F102](../features/read-unit-cache.md) keeps copies). A whole chunk's piece arrives in a body and is copied again into the device's aligned buffer to be written. And each answer is a frame and a `sendmsg` of its own: answers ready together are not written in one call |
| **Difficulty** | M |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Buffers kept for reuse hold memory between frames, and answers held to be written together wait for each other |
| **Benchmark** | `get100` and `put100` at 1 MiB on the loop leg at 2+1, each executor's cpu, and its page zeroing in `perf` (`stores-ab-perf.py`) |

Filed with [F106](../features/lane-copies.md).

### O131. A put's tail at one in flight across hosts is twice as long under TLS

| | |
| --- | --- |
| **Rank** | ~~**to diagnose**: the cause is not known, so neither is the change. Mutual TLS is every deployment's default~~ ~~**to build** as F112, BBR on Shoal's object sockets, the user's choice with lab 3a's figures (2026-10-24)~~ ~~**built** by [F112](../features/object-congestion.md) (2026-10-25), to be measured in lab 3b: the object lanes, and a client's connection once it carries an object, run under BBR at both ends, which each node's unit loads and allows~~ **built** by [F113](../features/slow-start-after-idle.md) (2026-10-25), ~~to be measured in lab 3c~~ measured in lab 3c (2026-10-25): shoaladm keeps every connection's window through an idle on every node's host, and the 64 KiB and 1 MiB puts at one in flight ran 1.11 and 1.07 times as fast with p99s of 30.4 and 48.9 ms where they were 62.7 and 80.2, every other arm within 1% of F111's and the retransmissions F111's. Lab 3b measured F112's BBR gaining the puts and slowing the gets across hosts, resending 15 to 29 times the segments where they met at one link, and F113 made it opt-in |
| **Impact** | Measured on the lab's SSD leg at 2+1 (2026-10-23, [F103's figures](../features/plaintext-peer-lanes.md#on-the-ssd-leg)), one build beside itself with plaintext lanes. 64 KiB puts at one in flight had a p99 of 63.8 ms under TLS and 30.5 plaintext, 62.8 to 64.9 in every run against 29.5 to 35.4. 1 MiB puts had 106 and 80.0. Their p50 moved by 1 ms or less and their p90 by 5 to 8 ms, so most of the difference is in the slowest tenth. On europa's loopback the same arms' p99 were within each other's ranges, and at sixteen in flight on the SSD leg too. Lab 2 (2026-10-23, `w2-ssd-ec21`) ran the same build under TLS again, each settle waiting out its arm's reclamation ([Resolved #301](resolved/a-settle-waits-for-reclamation.md)). Its 64 KiB puts' p99 was 63.5 ms [62.5–65.7], and F105's and F106's builds had 63.8 and 62.1, so that half repeated. The 1 MiB half did not: TLS ran those puts at 25.0 to 25.4 a second with a p99 of 79.4 to 85.6 ms, where lab 1's TLS ran 17.6 to 17.9 with 101.6 to 108.8. Lab 1's 1 MiB arm ran beside the reclamation of the 64 KiB puts before it on both sides, writing 2,038 to 2,065 MB a run against lab 2's 1,905 to 1,956, so its 106 against 80.0 was not the lanes' alone. Plaintext has not run since the fix. [Resolved #304](resolved/lone-frame-loss-probe.md)'s first build set a minimum RTO of 5 ms on every connection, and with it this tail halved: a p99 of 32.2 ms at 64 KiB where it was 62.5, and 55.9 at 1 MiB where it was 80.9 (`w2-ssd-ec21-fix`). ~~So the tail is a lone segment's loss probe, on a connection its put's bytes cross.~~ The fix as kept holds the floor on a client's connection only until it carries an object, so a put's connection keeps the kernel's, and this tail stayed: a p99 of 62.6 ms beside 62.8 (2026-10-24, `w2-ssd-ec21-client`). Taking it off would need the window kept across an idle ([todos](todos.md#a-clients-connections-take-a-minimum-rto)). **Diagnosed in lab 3a (2026-10-24, below): it is that window.** With every host's `net.ipv4.tcp_slow_start_after_idle` at 0 and no floor, the same build's 64 KiB puts at one in flight ran 1.12 times as fast with a p99 of 30.3 ms where it was 62.9, and its 1 MiB puts 1.08 times with 50.7 where it was 81.5; no other arm moved |
| **Difficulty** | ~~Unknown until the cause is found~~ Small: a socket's congestion control set on two kinds of connection, and a module shoaladm loads as it loads kTLS's (F112); a host setting shoaladm writes in a file of each unit and takes back (F113) |
| **Depends on** | ~~a trace of a slow put's frames across its lanes, or each peer lane's socket queues sampled while the arm runs, TLS beside plaintext~~ nothing: every bench connection's kernel state was sampled beside lab 3a's captures |
| **Blocks** | nothing |
| **Tradeoff** | ~~None known~~ BBR paces its sends and probes the path's bandwidth on its own schedule, so an object socket beside a full link behaves otherwise than under cubic. Lab 3a ran BBR only with a 5 ms minimum RTO beside it, which alone halved this tail in item 304's first build, so BBR's own effect is first measured by F112's A/B. It was (lab 3b, 2026-10-25): BBR does not slow down for loss, and where many object flows met at one 1 GbE port the gets resent 15 to 29 times as many segments, the lanes of rounds beside them lost segments too, and the 1 MiB gets at sixteen in flight ran 0.94 times as fast with a p99 of 818 ms where it was 444. F113's host setting is host-wide: every connection on a node's host keeps its window through an idle, every program's |
| **Benchmark** | ~~`f103-ssd-ec21`'s~~ `put100/s64k/c1` and `put100/s1m/c1` on the SSD leg at 2+1, ~~TLS beside plaintext (`AB_<side>_PLAIN`),~~ each settle waiting out its arm's reclamation, as `f103-ssd-ec21`'s did not: lab 3a's arms (`w3-ssd-ec21-o131`), F112 beside the build before it (`w3-ssd-ec21-w3b`), and F113 beside F111 (`w3-ssd-ec21-f113`) |

Filed from [F103](../features/plaintext-peer-lanes.md)'s run on the lab. A put at one in flight
crosses several lanes in series, staging its chunks on their holders and replicating its commit,
and between puts each lane is idle. The tail appears only across hosts, and only at one in flight.

**Diagnosed in lab 3a (2026-10-24).** Item 304's build as kept (7154dbf2) ran on the SSD leg at 2+1,
its puts and gets at 64 KiB and 1 MiB, one and sixteen in flight.
- Each side set every host's TCP for its captures and put it back after them (`AB_<side>_SYSCTL`).
- Every bench connection's kernel state and every host's TCP counters were sampled each second beside
  the runs (`stores-sample.sh`).
- A follow-up took side e alone, beside two more captures of side a. The first follow-up was lost
  to [item 306](resolved/lab-run-restarted-over-its-cluster.md) and taken again.

Operations a second and p99, every run of two captures a side (side a, four):

| Side | Every host's TCP | 64 KiB puts, one in flight | 1 MiB puts, one in flight |
| --- | --- | --- | --- |
| a | as deployed | 40.0–41.2, 62.8–63.5 ms | 25.5–26.5, 80.8–85.3 ms |
| b | `tcp_rto_min_us=5000`, `tcp_slow_start_after_idle=0` | 44.8–45.5, 30.1–35.6 ms | 27.3–28.1, 48.7–49.6 ms |
| c | `tcp_rto_min_us=5000`, congestion control `bbr` | 44.7–45.1, 34.6–34.9 ms | 27.4–27.8, 49.7–50.2 ms |
| e | `tcp_slow_start_after_idle=0` alone | 45.4–45.6, 30.2–31.4 ms | 27.7–28.0, 49.4–51.3 ms |

The puts' p50 moved by 2 ms or less. Gets at both sizes and the arms at sixteen in flight were within
side a's runs on every side (`stores-ab-report.py` over `w3-ssd-ec21-o131e2`: every other arm 0.97
to 1.01).

The samples, read by `stores-ab-sockets.py` and `stores-ab-idle.py`
(`shoal-spike/results/stores-ab/w3-o131-sockets.txt`, `w3-o131-idle.txt`):
- **It is not loss.** No bench connection retransmitted a segment in side a's puts at one in flight.
  The loss probes its hosts counted sent new data.
- **Connections sit idle past their RTO between sends.**
  - The client's connections: about 28 sent in each arm, each idle for seconds between puts.
  - The object lanes: each host's eight lanes of bytes were idle past their RTO in a third to a half
    of their samples, a median of 110 to 190 ms against an RTO of about 200.
- **Each such send restarts its sender's window.** Linux does so while `tcp_slow_start_after_idle` is
  1, halving the window for each RTO idle, down to the initial ten segments.
  - On side a, titan's and hyperion's lanes of bytes had a median window of 10. On side e it was 68
    to 124.
  - The holders' hosts counted 75 to 161 loss probes in a run of 64 KiB puts on side a, and none on
    side e.

So the time is the restarted window's: a put's bytes go out in a window cut back to its start. A 5 ms
floor shortened the tail too (item 304's first build, side c), though it restarts windows more often.
That fits the wait being on delayed ACKs, which Linux caps at a socket's minimum RTO less one. Why
F103's plaintext lanes escaped the tail was not established: that run predates the sampler.

**The fix, the user's choice (2026-10-24): BBR on Shoal's object sockets** (F112). Linux skips a
window's restart after an idle for a congestion control with a control of its own (`cong_control`),
as BBR has.
- The object lanes, and a client's connection once it carries an object, set `TCP_CONGESTION` to
  `bbr` at both ends.
- shoaladm loads `tcp_bbr` on the node hosts, as it loads kTLS's module.
- A client on a host without the module keeps cubic.

[F112](../features/object-congestion.md) built it on 2026-10-25. A host that refuses it, by its
module or its allowance, leaves the connection under its own congestion control and says so once.

Two other fixes were not chosen:
- a host setting shoaladm writes, `tcp_slow_start_after_idle=0`, which is what side e measured but
  changes every connection on the host;
- documentation alone.

**Lab 3b (2026-10-25) measured F112 beside F111 on the same arms, and it failed its own check.** The
puts gained what side e's had: 64 KiB at one in flight 1.08 times as fast with a p99 of 34.8 ms where
it was 62.2, 1 MiB 1.04 times with 49.5 where it was 79.6. But three of the four get arms ran 0.89 to
0.94 times as fast, the 1 MiB gets at sixteen in flight had a p99 of 818 ms where it was 444, and in
that arm the nodes' ends of the bench's connections resent 272,069 to 411,649 segments a capture where
F111's resent 12,247 to 16,153. Every get's bytes meet at europa's port, where the bench reads them,
and BBR does not slow down for loss. The lanes of rounds beside them, still under cubic, resent 15 to
21 times as many. Side c had shown the same in `w3-o131-sockets.txt`, unread by its write-up
([F112's figures](../features/object-congestion.md#performance)).

**So the user chose the host setting after all (2026-10-25), and
[F113](../features/slow-start-after-idle.md) built it**: shoaladm sets
`net.ipv4.tcp_slow_start_after_idle = 0` on every node's host from an inventory's
`slow_start_after_idle`, false unless named, in a file of each unit under `/etc/sysctl.d` that
`destroy` takes back, and F112's BBR is null, the host's own, unless named.

**Lab 3c (2026-10-25) measured F113 beside F111 on the same arms, and it did what side e had.** The
64 KiB puts at one in flight ran 1.11 times as fast with a p99 of 30.4 ms where it was 62.7, and the
1 MiB puts 1.07 times with 48.9 where it was 80.2, every run apart. Every get and every arm at sixteen
in flight was within 1% of F111's, and every kind of connection resent what F111's did. Through the
idles the lanes of bytes on the holders' hosts kept a median window of 66 to 92 segments in the
64 KiB puts and 140 to 167 in the 1 MiB, where F111's had 10 to 14 in the 64 KiB
([F113's figures](../features/slow-start-after-idle.md#performance)). Europa ran kernel 7.0.0-38 for
it, the other hosts 7.0.0-34, both sides alike.

### O132. A coordinator checksums every byte of a read's range its holders checked already

| | |
| --- | --- |
| **Rank** | ~~**to weigh** in lab 3b's profile~~ **not now**: the last pass a get's bytes take on a table shard since F109, which lab 3b's profile put at 2.8% of the table shards' samples with F109 and 4.5% with F111, about 0.04 cpu-seconds a GiB of a 1 MiB get at sixteen in flight on loopback (2026-10-25): too little for a wire change. Found while [F109](../features/reads-from-stripe-buffers.md) was built |
| **Impact** | Argued from the source (2026-10-24). A read's answer carries the CRC-64/NVME of its range, which the coordinating shard takes over every part's bytes, 64 KiB at a time. Every unit of those bytes was checked against its own checksum on its holder a moment before, which computes each unit's CRC as it verifies it and throws it away (`store/chunk.rs`) |
| **Difficulty** | L to XL. A holder would answer its range's checksum beside its bytes, made from its units' and the pieces at the range's edges, and the coordinator would fold the parts' by the combine; a decoded part would still be checksummed where it was decoded. The lane's answer changes, so a wire version gates it, as F96's classes were |
| **Depends on** | ~~lab 3b's profile of F109, for the share of a shard's samples the pass is~~ nothing: measured in lab 3b |
| **Blocks** | nothing |
| **Tradeoff** | A holder's checksum would vouch for bytes after they crossed the lane, where the coordinator's covers the lane too: the lane's own checks (kTLS's tag, or a plaintext lane's nothing) would be what stands between |
| **Benchmark** | `get100` at 1 MiB on the loop leg at 2+1, each coordinating shard's cpu (`pidstat -t`) and its profile (`stores-ab-perf.py`) |

Filed from F109.

### O134. A client relay and an asking link write a frame a call

| | |
| --- | --- |
| **Rank** | **wanted** where many small frames go out together: the relays F111 left. Found while [F111](../features/lane-buffers.md) batched a served lane's answers |
| **Impact** | Argued from the source (2026-10-25). A shard's client relay writes each answer's frame in a call of its own (`write_whole`, `write_stream_frame`, `server/shard.rs`), and so does a link asking a peer, its requests one frame a call (`peer/link.rs`). A burst of small answers or requests is a call each, where F111's served lane writes every answer ready in one |
| **Difficulty** | M. The relay drains its outbox's ready frames into one vectored write, within a count and a byte bound, as F111's writer does; a link the same with its queue |
| **Depends on** | lab 3b's figures for F111, for what batching saved a served lane |
| **Blocks** | nothing |
| **Tradeoff** | A batch holds its frames' rooms until the call is written |
| **Benchmark** | the comparison's 4 KiB gets and stats at sixteen in flight, each relay's cpu (`pidstat -t`) |

Filed from F111.

### O133. A client copies every object answer out of the buffer its frames filled

| | |
| --- | --- |
| **Rank** | **wanted** where a client's cpu matters: two copies of every byte a get returns. Found while [F109](../features/reads-from-stripe-buffers.md) was built |
| **Impact** | Argued from the source (2026-10-24). A client reads an object answer's data frames into one aligned buffer, copies it into a `Vec` once the last frame is in (`done.buff.to_vec()`, `client.rs`), checks it whole, and a range read copies it again into the range it returns (`read_range`, `client/objects.rs`). The bench driver pays both on every get, beside the nodes it measures |
| **Difficulty** | M. The answer's buffer would be handed back as it is, and a range read of one server read would return it without appending |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | None: the buffer is the client's already |
| **Benchmark** | `get100` at 1 MiB, the bench driver's cpu a GiB beside the nodes' |

Filed from F109.

### O135. Pages zeroed as they are allocated are a tenth of a get path thread's samples

| | |
| --- | --- |
| **Rank** | **to diagnose**: where the pages come from is not known. Found in lab 3b's profile of F111 |
| **Impact** | Measured on europa's loopback (2026-10-25, lab 3b, `shoal-spike/results/stores-ab/w3-perf.txt`): through a 1 MiB get at sixteen in flight at 2+1, `kernel_init_pages` was 7.0% of the object executors' samples and 10.8% of the table shards' with F111, and the kernel's page zeroing and allocation 11.5% and 12.8% in all. It is the kernel zeroing every page it allocates, as `init_on_alloc`, on by default in Ubuntu's kernels, has it do: the sockets' buffers and the process's fresh anonymous memory alike. Without call stacks the profile cannot split them |
| **Difficulty** | Unknown. Where it is the process's fresh memory, an answer body that crossed to a shard handed back to the executor that read it (a `Send` way back) would reuse pages already zeroed, as F111's arena does for bodies that stay. Where it is the sockets' buffers, it is the host's: `init_on_alloc=0` on the kernel's command line, a hardening the kernel's default chose |
| **Depends on** | a profile with call stacks (`perf record -g`) through the same arm |
| **Blocks** | nothing |
| **Tradeoff** | A way back holds an executor's buffers for longer, and a pool of them is memory a node keeps; `init_on_alloc=0` gives up the zeroing that keeps one allocation's old bytes from another |
| **Benchmark** | `get100/s1m/c16` at 2+1 on the loop leg, each thread's cpu (`pidstat -t`) and a profile with call stacks |

Filed from lab 3b's profile of [F111](../features/lane-buffers.md), which had left an answer body that
crosses to a shard a fresh allocation until a profile said what zeroing costs.

### O136. A local read into a heap buffer forgoes the ring's registered buffers

| | |
| --- | --- |
| **Rank** | **to weigh**: the cost is measured and small, and so was the copy it replaced. Found in lab 3b's figures for [F110](../features/store-checks-in-place.md) |
| **Impact** | Measured on europa's loopback (2026-10-25, lab 3b): with three copies, where every get reads its local copy, a 64 KiB get at one in flight cost the object executors 0.44 cpu-seconds a GiB with F110 where it cost 0.41 with F109, every run apart; at sixteen in flight and at 1 MiB the ranges overlapped, at 0.99 to 1.08 of F109's. Every rate was within. Since F110 a read whose answer crosses to a shard reads into a heap buffer (`read_into_at`), whose pages the kernel pins for each read, where the ring's registered buffers are pinned once (`read_fixed`) |
| **Difficulty** | M. A pool of registered buffers that can cross to a shard and come back, as `OwnedDmaBuffer` crosses today, so the read keeps `read_fixed` and the copy stays gone |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | A registered buffer held by a shard until its client is written is one the ring cannot lend another read |
| **Benchmark** | `get100` at 64 KiB and 1 MiB with three copies on the loop leg, the object executors' cpu a GiB (`stores-ab-cpu.py`) |

Filed from lab 3b's figures for F110.

### O137. A 1 MiB put's p99 at sixteen in flight across hosts is 8% longer with F109 to F111

| | |
| --- | --- |
| **Rank** | **to diagnose**: which of F109 to F111 it belongs to is not known, nor why. Found in lab 3b's check of the SSD leg, and repeated in lab 3c |
| **Impact** | Measured on the lab's SSD leg at 2+1 (2026-10-25). In lab 3b (`w3-ssd-ec21-w3b`, two captures a side, the hosts on kernel 7.0.0-34) the 1 MiB puts at sixteen in flight had a p99 of 372 ms [365–382] with F111, where the build before F109 had 344 [332–359], every run apart. The rate was the same, 95.0 a second against 96.5, and no kind of connection resent more. The p90 rose with it, 258 ms to 274. Lab 3c's two captures of F111 (`w3-ssd-ec21-f113`, europa on 7.0.0-38) had 379 [370–385], and F113's 369 [355–380]. On europa's loopback the same puts' p99 fell a little at each of F109, F110 and F111, 35.4 ms to 34.7 at 2+1, so it shows only where a put's chunks cross 1 GbE |
| **Difficulty** | Unknown until it is placed |
| **Depends on** | the build before F109 beside F109, F110 and F111 each, on the SSD leg |
| **Blocks** | nothing |
| **Tradeoff** | None known |
| **Benchmark** | `put100/s1m/c16` on the SSD leg at 2+1, each step beside the one before it, with the sampler beside them (`stores-sample.sh`) |

Filed from lab 3b's check of [F109](../features/reads-from-stripe-buffers.md#performance) to
[F111](../features/lane-buffers.md#performance) on the SSD leg, where europa's link bounds the arms at
sixteen in flight, which lab 3c repeated.

### O138. A cancel cuts a stream only once its shard hands it on, which a client reading at memory speed outruns under load

| | |
| --- | --- |
| **Rank** | ~~**to measure**: what a cancelled stream costs a loaded node before its cut~~ **built** by [F114](../features/early-cancel-cut.md) (2026-10-27): the read relay leaves a cancel for the write relay as it reads it, which cuts the bundle's stream begun and holds its other streams until the shard's word is acknowledged, and the write relay yields between stream frames so the read relay runs under a long stream; ~~to be measured on the lab with `abandon`, and its yield's cost with object gets~~ measured on the lab (2026-10-27, [F114's figures](../features/early-cancel-cut.md#performance)): an abandoned bundle of 16 MiB wrote 0.83 times the bytes it did before and had 2.3 times the streams cut, one of 64 MiB 0.88 times and 1.4 times, every range disjoint, and object gets of 64 KiB and 1 MiB moved within noise |
| **Impact** | Found by [item 321](resolved/cancel-row-waits-for-its-cancel.md)'s loop (2026-10-26). A connection's read relay reads a cancel at once and hands it to its shard's queue behind the bundles it read; the shard records it and hands it to the connection's writer, which cuts a stream between two frames. A client that keeps reading at memory speed after its cancel, as a pooled connection's reader does for its other answers, was sent the whole of a 16 MiB stream in four of fifteen runs beside five other suites, and 254 of its 257 frames in a fifth. Every answer was right; the bytes the cancel could have spared were written |
| **Difficulty** | Moderate. A stream of the cancelled bundle already begun on the connection when the relay reads the cancel was an arrival before it, since a client writes its cancels ahead of any retry, so the relay could have the writer cut it then and leave the shard to bound what has not begun |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Two paths would cut one bundle. The shard's bound would still decide every answer not yet begun, which is what keeps a retry's answer from being taken for the cancelled one's |
| **Benchmark** | `tmdb-dataset-loader abandon -i <inventory> --cancel true`, which prints what the members wrote and were spared, run under load |

Filed from item 321, whose row read a cancelled stream at memory speed and so measured how soon the
shard handed on the cancel rather than whether the writer cut the stream.

### O139. A scan driver asks a shed tablet again where another tablet would be answered

| | |
| --- | --- |
| **Rank** | ~~**to measure**: what moving a shed stream to another tablet saves the driver and the members, beside what it costs a scan's order; since [W42](../future-work/scans/scan-speed.md) (2026-10-29) before X17 and X18, since a Zen1 node answered 1,500 to 1,700 sheds a second through X16's scans, a part of its cpu a partition those spikes measure~~ **applied** (2026-10-30), before X17 and X18 as W42 asked: a shed stream puts its tablet back on the scan's queue with its whole cursor and takes the next ([F115](../features/bench-scans.md)); a stream shed twice in a row with no page answered between still waits 2 ms, so a member whose shards are all full, or out of its scan budget, is asked no faster than before; since [O148](#o148-a-scan-driver-asked-a-shed-page-again-every-2-ms-beside-pages-paced-for-tens) each shed after that doubles its wait, up to 64 ms. ~~Its figures, sheds a page answered and partitions a second each way, are taken in the scans plan's next lab window~~ Measured by [X17](../future-work/scans/page-rebuilt.md#a-partitions-cost) (2026-10-30, four rounds, today's walk with the pace lifted): sheds a page answered 23.7 [20.4, 25.8] before and 19.2 [15.7, 21.1] after, partitions a second 153,000 and 150,000, the Zen1 hosts' cpu a partition 47.6 and 46.4 µs, each pair's ranges overlapping; europa's cpu a partition 20.9 [20.0, 21.7] µs before and 17.3 [16.5, 18.1] after, the one disjoint. Sheds fall with a page's time, not with this: 2.1 a page answered in runs, whose pages are fourteen times shorter |
| **Impact** | Found by [X16](../future-work/scans/walk-beside-the-foreground.md#what-was-found-on-the-way)'s quick passes (2026-10-29). The bench's scan driver ([F115](../features/bench-scans.md)) runs two streams a shard of every member, 32 on the lab's side cluster, and each walks the tablet the seeded queue gives it to its end. A client cannot know which shard of a member hosts a tablet, so streams meet on a shard past its two pages and are shed, and a shed stream asks for the same page again 2 ms later, `SHED_AFTER` and its jitter (`shoal-loadgen/src/scans.rs`). The longer a page, the worse: on X16's quick passes, a tenth of the rows, the driver answered about 1,100 to 3,400 pages a second and was shed about 4,400 times a second; on its full run a page of `Big` was a whole tablet's 732 partitions and took about 150 ms, and the driver answered about 125 pages a second and was shed about 3,800 times a second, some thirty sheds a page answered, every one a round trip and an admission check on a member, and a stream idle meanwhile |
| **Difficulty** | Small. A shed stream puts its tablet back on the queue, at its cursor, and takes the next one, so it is asked on another shard while the full one drains; a tablet taken back and shed again waits its turn |
| **Depends on** | nothing |
| **Blocks** | nothing, though M24's client driver ([W6](../future-work/scans/distribution.md)) meets the same choice |
| **Tradeoff** | A scan's tablets finish in an order further from the queue's, which nothing reads; a stream may hold several tablets' cursors |
| **Benchmark** | `scan/<Table>/none` beside its own shed count, which a capture keeps (`scan.shed`), on X16's side cluster |

Filed from X16, whose before and after kept the driver as it is on both sides, so the comparison
is fair.

### O140. A resident read's p99 beside the cell's inserts is six times ScyllaDB's on the same hosts

| | |
| --- | --- |
| **Rank** | **to measure**: where a resident read's tail beside synced inserts goes, before anything is changed for it |
| **Impact** | Found by [X20](../future-work/scans/scylladb-scan-sources.md#what-the-run-says) (2026-10-29). X16's cell on the lab's side cluster, four closed-loop workers of half reads at `One` of resident rows and half inserts, a bundle of one, has a read p50 of 0.43 ms but a p99 of 7.66 ms and a p999 of 31.8 ms, the medians of eight captures ([X16](../future-work/scans/walk-beside-the-foreground.md#the-cell-alone)). ScyllaDB 2026.3.4 on the same hosts and cpus, driven with the same mixture from a Python client, had a read p50 of 0.57 ms, a p99 of 1.15 ms and a p999 of about 3.4 ms, at a higher rate. Shoal's inserts sync their WAL before they answer (a p50 of 11.7 ms and a p99 of 24.5 ms) and ScyllaDB's do not (`commitlog_sync: periodic`), so what a synced write costs the reads beside it on a shard is the first suspect, not a finding |
| **Difficulty** | Unknown until measured. The candidates: a read queued on its shard behind an apply or a WAL batch's completion, a read at `One` answered by a Zen1 holder busy with every insert's apply, or the bench client's own pool |
| **Depends on** | nothing |
| **Blocks** | nothing; [W7's budget](../future-work/scans/isolation.md#the-budget) is a ratio, judged the same either way |
| **Tradeoff** | None known until the cause is |
| **Benchmark** | `rw50/b1/none` beside `read100/b1/none` on X16's side cluster, each also with `--routing endpoints` pinned to one member at a time, since a table arm's capture keeps no figures by member; and ScyllaDB's own figure with `commitlog_sync: batch`, which syncs its writes as Shoal's are |

Filed from X20, whose figures for ScyllaDB came from a slower client than Shoal's, driven faster, so
the gap is if anything wider than it reads.

**Measured** (2026-11-05 and 06), on the `m23` side cluster with the cell alone (`CELL_ONLY`: a cluster
bootstrapped fresh at 6 GiB a node, the cell's first 100,000 rows preloaded, `rw50` and `read100` at a
bundle of one, each warmed 100 s past item 351's change of flush and measured 120 s, every member's own
figures read each second).

- **Which reads.** X16's 7.66 ms was its build's, at 4 GiB a node beside its other tables; by `4933602a`,
  with items 338, 350, 353 and the rest in, the cell alone read at a p99 of 2.42 ms beside its inserts.
  Each member's own get p99 was 0.14 ms on europa and 0.45 to 0.49 ms on the Zen1 hosts, so the client's
  figure was not one member's.
- **Where on the shard.** A `loop-profile` build ([the observability page](../operations/observability.md#the-loop-profile),
  `shoal-spike-scan/results/found-o140/`) found a shard's loop idle 97 to 99% of `rw50`: a client's bundle
  waited 3 to 14 µs at the mean to be taken and a routed query 16 to 48 µs, 64 to 256 µs at the p99. What
  held it longest were two messages about every six seconds, a compacted segment's checkpoint write,
  16 ms at the mean on a Zen1 host, and its evictable marking, 4 ms.
- **The p99 was item 352's.** A shard asked each of its 54 groups' cores in turn whether it ran after
  every batch it applied. Its fix took the cell's read p99 beside its inserts from 2.42 to 0.75 ms and its
  inserts a second up 11%, every range disjoint over eight side-rounds
  ([Resolved #352](resolved/core-probes-off-the-loop.md#evidence)): under ScyllaDB's 1.15 ms with its writes
  unsynced.
- **The p999's holds were O159's.** Both became steps of a quarter of a millisecond at most
  ([O159](#o159-a-compacted-segments-checkpoint-and-its-evictable-marking-hold-a-shards-loop-for-milliseconds)),
  and the read p999 fell to 0.71 of what it was at the median, its ranges overlapping at four side-rounds
  a side. What holds a shard's loop longest now is its tick and a sealed segment, under a millisecond at
  the mean ([O160](#o160-a-shards-tick-and-a-sealed-segment-hold-a-zen1-shards-loop-for-up-to-two-milliseconds)).

SCYLLA_BATCH_PENDING

### O141. A walked partition costs a Zen1 host about 73 microseconds of cpu

| | |
| --- | --- |
| **Rank** | ~~**to measure first**: a profile of a walked partition, then the cuts it names; required for M23's cost budget ([W12](../future-work/scans/performance.md#the-budgets), [W42](../future-work/scans/scan-speed.md))~~ **measured** (2026-10-30) by [X17](../future-work/scans/page-rebuilt.md#a-partitions-cost): 46.4 µs of a Zen1 host's node cpu one record at a time, 22.3 µs with sixteen reads in flight and 5.6 µs in runs in disk order over merges in key order, which meets M23's cost budget; M23's walk is the cut it names |
| **Impact** | Measured by W42 from X16's captures (2026-10-29): `scan/Big/none` cost titan 75 µs and hyperion 73 µs of host cpu a partition (user, system and interrupts, not idle or I/O wait), every host's busy time over every partition the arm read, so an upper bound on the walk's own. The code path accounts for an estimated 20 to 40 µs: three operations a record at a depth of one, with an executor's park and wake around each; the `dup` and the close ([O15](#o15-one-partition-load-costs-a-dup-and-a-close)); the checksum, the bytecheck and a 1 KiB copy; and the shed streams' answers ([O139](#o139-a-scan-driver-asks-a-shed-tablet-again-where-another-tablet-would-be-answered)). At 73 µs five Zen1 shards stop near 68,500 partitions a second, under the default ceiling's 100,800 |
| **Difficulty** | Unknown until profiled |
| **Depends on** | nothing; O15 and O139 are parts of it |
| **Blocks** | M23's cost budget: at most a fifth of today's cpu a partition, about 15 µs |
| **Tradeoff** | Whatever the profile names; the bytecheck after a checksum is not traded without [F4](../features/validated-archives.md)'s reason weighed |
| **Benchmark** | X17's profile on titan and europa (`perf` of a node during `scan/Big/none`: user, kernel and interrupts, ring operations a partition), and an arm with only the background running, which no capture has |

Filed from W42, which split the gap to ScyllaDB's scan into factors and found this one estimated
at 6.8 times, the largest that is a mechanism rather than a choice.

**Measured:** X17's reads half (2026-10-30) took the node process's cpu a partition by the
partitions each host's member read, which leaves out the driver's and the kernel's work for other
processes that X16's host figure held. One record at a time it was 46.4 µs on the Zen1 hosts and
17.3 µs on europa; the three operations of before [F120](../features/one-operation-a-record-read.md)
were 12.1 µs of it on a Zen1 host. A profile of titan's node found the ring's submission and
completion and the kernel's syscall path a quarter of its samples one record at a time, and Shoal's
own code 3%. Sixteen reads in flight halved the cost; runs in disk order over merges in key order cut
it to 5.6 µs, the per-partition work then leading: the walk's fold and checksum, rkyv's validation,
the allocator, and in the spike a channel a record a product's runs would not need. Paced at the
default ceiling, runs cost 11.7 µs, so a page's waits for credit are worth a look before M23 counts
its cpu ([O143](#o143-a-paced-scan-pages-waits-for-credit-double-its-cost-a-partition)).

### O142. A merge writes its partitions in a hash map's order

| | |
| --- | --- |
| **Rank** | ~~**required at M23** for its rate, a [W1 row](../future-work/scans/prerequisites.md#merges-written-in-key-order), priced by [X37](../future-work/scans/spikes.md#x37-a-tablets-records-together-priced)~~ **built** (2026-10-30), by [F121](../features/merges-in-key-order.md), ahead of M23 as W1 asked; priced on the lab by X37 |
| **Impact** | Found by [W42](../future-work/scans/scan-speed.md#how-a-tablets-records-lie-on-disk) (2026-10-29). `write_partition` writes a merge's `loaded: HashMap<u64, T>` in its iteration order (`shoal-core/src/server/tables/storage/fs/compactor.rs:411`, `:752-773`), which std seeds at random, so every 10 MiB merge block holds every tablet's partitions of its segment in no order, and an archive pass rewrites in key order only archives under half live. A table loaded once keeps that order for good: on a Zen1 shard a tablet's records are about 12 a block, about 870 KB apart, and a scan's page reads them a read each. [O78](#o78-a-snapshot-cut-read-one-record-at-a-time-in-key-order)'s runs, which made a snapshot cut ten times faster, would read about six times a page's bytes to spare a seventh of its reads |
| **Difficulty** | S: the keys sorted before the loop |
| **Depends on** | nothing |
| **Blocks** | W5's runs over a tablet's records |
| **Tradeoff** | A sort of some 9,800 keys a segment on the compactor, and a vector of them; a snapshot cut's runs change shape, denser; archives written before it keep their order until a pass copies them |
| **Benchmark** | X37: a merge's time and memory each way; a tablet's stretches on a shard, from the map; the reads a page and device bytes a partition of a walk with runs; a get's p99 and a cut's time beside |

Filed from W42. Nothing reads a merge's order today, since every reader finds a record through the
map and the compactor's tests order jobs, not records, which is what makes it free to change.

**Applied:** `write_partition` sorts the job's whole records and fragments by key together and writes
them in that order ([F121](../features/merges-in-key-order.md)). In process, a merge of 2,000 items
into one block wrote 1,017 after a larger key before and none after. ~~Its lab figures, a merge's time
and memory each way and a tablet's stretches on a shard, are X37's.~~ On the lab
([X37](../future-work/scans/tablet-records-together.md), 2026-10-30) the sort took 302 µs of a merge of
9,150 entries at the median, 0.45% of its time, and the order 215 KiB at most; a tablet's records lay
in stretches of 11.3 partitions, and a walk in runs across 4 KiB read 11.1 partitions a device read
against 1.01, and 1,720,000 partitions a second for the cluster against 500,000.

### O143. A paced scan page's waits for credit double its cost a partition

| | |
| --- | --- |
| **Rank** | ~~**to measure**: where a paced page's cpu goes, before M23 counts a scan's cost at its ceiling ([W12](../future-work/scans/performance.md#the-budgets))~~ **built** (2026-10-31), by [F128](../features/device-time-pace.md), M23's sixth step; its lab figure is M23's exit's |
| **Impact** | Found by [X17](../future-work/scans/page-rebuilt.md#the-default-pace-binds) (2026-10-30). A walk in runs over merges in key order cost a Zen1 host 5.6 µs of its node's cpu a partition unpaced and 11.7 µs at the product's default ceiling, 100 MiB a second a device, where a page spent 38 ms of its 41 ms waiting for its device's credit. The node's background accounts for about 2 µs of it at the paced rate. The rest is likely the waits themselves: a page that finds no credit asks its pace again every millisecond (`PACE_POLL`, `shoal-core/src/server/scans.rs`), each ask a timer, a wake and a lock of the device's bucket, and at the ceiling every shard's pages are waiting. At about 96,000 partitions a second the difference is half a core of a Zen1 node's five shards |
| **Difficulty** | Unknown until measured. The candidates: a waiter told when credit is due rather than polling, one pacer a device that hands credit out rather than pages taking it, or credit taken for a whole run at once |
| **Depends on** | nothing |
| **Blocks** | nothing; M23's cost budget is judged alone at a scan's rate, which is the ceiling's |
| **Tradeoff** | A pacer that hands out credit is one more task a device; a page told when credit is due is woken later than one that asks, by up to its poll |
| **Benchmark** | X17's `g4k16` at the default pace and with the pace lifted, its Zen1 hosts' node cpu a partition, on X16's side cluster; or X18's arms, whose every pace binds |

Filed from X17's first full run, whose scans ran at the default pace, against its second, which
lifted it.

**Applied:** a read is taken in as few chunks as every bucket holds whole, most often one, and each
waits exactly until it is due; only a trickle a foreground read holds is asked again a millisecond
later ([F128](../features/device-time-pace.md)). The waiter told when its credit is due, the first
candidate, without a pacer's task. In process a mebibyte the ceiling holds is taken on one ask where
the credits asked sixteen, and the next waits once, for exactly a tenth of a second
(`a_paced_read_asks_its_device_once_a_wait`). What it saves a Zen1 node a partition at the pace is
judged by M23's exit, beside the device-time share that came with it.


### O144. A scan page holds its reads beside the partitions folded from them

| | |
| --- | --- |
| **Rank** | ~~**to weigh with M23's fifth step**, which settles where a scan page's folds run~~ **open**: the fifth step folds a page's partitions in steps in key order ([F127](../features/folds-in-steps.md)), so a run's buffer is still held until its last record is folded; ~~to weigh beside the scan budget's peak at M23's exit~~ **optional, after M23**, since the exit's rounds (2026-10-31) did not report the budget's peak; to weigh beside it ([W5](../future-work/scans/tablet-reader.md#5-fold-filter-project-and-encode-in-the-scan-queue)) |
| **Impact** | Found building [F125](../features/page-reads-in-runs.md) (2026-10-31). A scan page reads its archived partitions in runs ahead of its merge, and the merge folds each into a copy as it reaches it in key order, so a run's buffer is held until the merge passes its last record. In a tablet's fresh layout every run of a page spans its key range, so a page holds every buffer it read beside every partition it built, about twice its byte bound; F125 doubled the scan budget's default to eight mebibytes a shard to hold two pages so. Folding a run's records as it arrives would give its buffer back at once, a page holding its bytes once: four mebibytes a shard of `node_memory` given back to the rows, or room for twice the pages |
| **Difficulty** | Moderate: a fold out of key order draws its output before it allocates and keeps a step's bound, both of which the merge does now in key order; a fold's refusal, or a draw's shed, has to wait on its partition until the merge reaches it |
| **Depends on** | ~~M23's fifth step, which moves folds into the scan queue's steps~~ nothing since F127 |
| **Blocks** | nothing |
| **Tradeoff** | Folds done out of key order are done for partitions a page that ends sooner never hands out |
| **Benchmark** | The scan budget's peak (`NodeStats.scans.budget_peak`, [F123](../features/walk-and-scan-counters.md)) under X17's `g4k16`, before and after |

### O145. A planned partition's encode step is a message through its loop each

| | |
| --- | --- |
| **Rank** | ~~**to measure at M23's exit**, beside the drivers' own figures and a resident table's scan cost~~ **optional, after M23**: the exit's drivers' figures were no worse (2026-10-31), so it is not M23's; to price beside a resident table's scan cost |
| **Impact** | Found building [F129](../features/drivers-on-the-rebuilt-walk.md) (2026-10-31). Argued from the source. A planned page encodes each resident partition in a step of its own (`ServerMsg::EncodeStep`): a message on its loop's channel, a dispatch, a fresh lookup and a oneshot answer. A page of a thousand small resident partitions makes a thousand round trips through its loop, where a capture made one step. A scan's step also waits for the second inbox's idle time. A driver's is taken in order with the loop's messages, so under load each waits behind whatever the loop has queued, and a driver's page over a placement group of many resident stripe rows waits that many times. Batching consecutive small partitions into one step keeps every bound [F126](../features/encodes-in-steps.md) holds, a fresh lookup each and at most 64 KiB together, and divides the round trips by the partitions a step holds |
| **Difficulty** | Moderate: an encode ask names several keys, each looked up fresh in one step, its answers in order; a partition taken in pieces keeps its cursor of one key's rows |
| **Depends on** | nothing |
| **Blocks** | nothing; a regression in the drivers' figures at M23's exit would make it M23's |
| **Tradeoff** | A step of several partitions holds its loop for up to 64 KiB of them, as one large partition's step does now |
| **Benchmark** | M23's exit: the drivers' figures, a rebuild's and a move's time beside the reference cell, before and after; and X17's `Keyword` scan's cpu a partition |

### O146. Four scan reads a device held a scan alone to two thirds of its rate

| | |
| --- | --- |
| **Rank** | **applied** (2026-10-31), at M23's exit: `scans.device_reads` is sixteen unless set, a page's sixteen runs, where F128 set four |
| **Impact** | Found by M23's quick lab passes (2026-10-31), once [item 346](resolved/paced-reads-alone.md) sent a paced page's reads to their device a group at a time. A group overlaps on its device only as far as the device's bound on scan reads in flight lets it, and the share is spent by the time any of them is in flight. A scan of a tenth of `Big` with nothing beside it, at each bound, the Zen1 hosts' 970 EVO: at four, 8.5 MiB a second a device, 8.2 to 8.7 ms of busy time a MiB, 76 to 84 µs of node cpu a partition; at sixteen, 11.9 to 13.4 MiB, 5.2 to 5.8 ms and 52 to 56 µs; at sixty four, 12.9 to 14.6 MiB, 5.0 to 5.7 ms and 49 to 51 µs. The cluster read 20,700 partitions a second at four and 33,800 to 40,700 at sixteen and past it. Past sixteen a page's wave, at most sixteen runs, bounds a group before the device does |
| **Difficulty** | A default |
| **Depends on** | [Item 346](resolved/paced-reads-alone.md), without which no two paced reads were in flight together whatever the bound |
| **Blocks** | nothing; M23's exit runs at sixteen |
| **Tradeoff** | A foreground read can find sixteen of a scan's reads queued ahead of it on a device, where it found four. X18's depth supplement hinted that two runs in flight a page moved the cell's read tail less than sixteen at one byte rate (its ranges overlapped), and M23's exit judges the read clause at sixteen |
| **Benchmark** | `scan/Big/none` of M23's churn round, quick, `SCANS="device_reads: <n>"` in `m23-lab.sh` |

The share's 75 ms still buys a 970 EVO far less than the 32 MiB a second the plan expected. X18's
2.3 ms a MiB was the device's busy time over a scan reading 200 MiB a second, its reads overlapping
deeply. At sixteen a scan's busy time a MiB is more than twice that, so at 75 ms its share is about
13 MiB a second. M23's exit records it for the user, who chose the share.

### O147. Each shard's io_uring workers run on the shard's own cpu

| | |
| --- | --- |
| **Rank** | **to measure after M23**, decided with the user on 2026-10-31 ([the decisions](../future-work/scans/m23-exit.md#decided-with-the-user-2026-10-31)): what a shard's loop and a foreground read's tail lose to its ring's workers, then whether to move them. M23's step clause is read on a step's own work meanwhile, a step past 100 µs by the wall alone named as this one's. Since [F130](../features/loop-steps-by-kind.md) (2026-11-01) a node counts those steps itself, each kind's `past[0] - past_cpu[0]` (`StepFigures::past_by_wall_alone`), so a run names this one's share without sampling `schedstat` |
| **Impact** | Found by M23's quick lab passes (2026-10-31). Every io_uring worker a glommio executor's ring starts (`iou-wrk-<tid>`, io-wq) has its executor's one-cpu affinity, read on titan with `taskset -cp`. So the fdatasync of every WAL batch, and anything else a ring punts to a worker, runs in a kernel thread on its shard's own cpu, preempting the shard's loop for as long as it runs. A scan's loop steps that copied little held their loop 220 to 590 µs, past W12's 100. ~~There were 8 of them beside the paced stream's 200 inserts a second and 33 beside the cell's 2,400, so they grew with the syncs.~~ There were 8 of them beside the paced stream's 200 inserts a second and 33 beside the cell's 2,400, in a quick pass each. M23's rounds did not bear the rise out: 8 to 15 a capture of `Big` alone beside the paced stream, and 3 to 16 beside the cell. Most of the rounds' steps past 100 µs were large resident partitions' instead, at the shards preempted least ([item 350](resolved/encode-copy-deadline.md)). A foreground step is held the same way. Each shard's `/proc/<tid>/schedstat` and its involuntary switches over 30 s of M23's full rounds (2026-10-31) say how much: beside the cell and a scan of `Big`, every one of titan's shards was preempted about 430 times a second, 42 to 54 µs each on average, and spent 18 to 24 ms of every second runnable but off its cpu, about 4% of its time on it; beside a scan of `Keyword` and the paced stream alone, 142 to 257 times a second at 29 to 38 µs on titan, and 108 to 181 at 10 to 13 µs on europa. A step of 50 µs that meets a preemption from that distribution's tail passes 100 |
| **Difficulty** | Moderate: the glommio fork registers each ring's io-wq affinity (`IORING_REGISTER_IOWQ_AFF`, Linux 5.14) when its executor starts, to a set the caller names. Which set is the question: the control core and its sibling, or a shard's sibling thread |
| **Depends on** | nothing |
| **Blocks** | nothing; W12's step clause is judged at M23's exit with the workers where they are, their share of its long steps named. ~~At the exit that share was the light steps', 3 to 16 a capture beside the cell, and item 350's the rest.~~ At the exit the light steps, 3 to 16 a capture beside the cell, were put down to it, and item 350's were the rest. Item 350's run on F130's build (2026-11-01) found most light steps past 100 µs on their own cpu, not by the wall alone, so not this one's as it stands; the lab's kernels charge a cpu's interrupts to the thread they interrupt, which may be what they are ([item 353](resolved/capture-allocations.md)). Item 353's six rounds (2026-11-02) counted each long step's switches: as many long steps were switched off their cpu as passed 100 µs by the wall alone, titan's `Big` encode steps 38 and 38 and hyperion's 29 and 28, so a step past the bound by the wall alone is a preempted one, as this entry has it ([item 353](resolved/capture-allocations.md)) |
| **Tradeoff** | The workers' cpu moves to another core, which a node's other threads then share; a sync's completion still wakes its shard |
| **Benchmark** | M23's exit: the scan class's steps past 100 µs and the cell's p99, a node's rings' workers on their executors' cpus against another set |

### O148. A scan driver asked a shed page again every 2 ms beside pages paced for tens

| | |
| --- | --- |
| **Rank** | **applied** (2026-10-31), at M23's exit: a stream shed twice in a row waits 2 ms, and each shed after that doubles its wait up to 64 ms, until a page of it is answered (`shed_wait`, `shoal-loadgen/src/scans.rs`) |
| **Impact** | Found by M23's quick lab passes (2026-10-31). The bench's scan driver runs two streams a shard of every member, 32 on M23's side cluster, and asks its tablets' followers first, which on the lab are titan and hyperion, ten scan pages each. A stream shed twice in a row waited 2 ms and asked again, [O139](#o139-a-scan-driver-asks-a-shed-tablet-again-where-another-tablet-would-be-answered)'s wait, chosen when a page ended in about a millisecond. Under F128's pace a page takes tens to hundreds of milliseconds, so the surplus streams asked about 2,000 times a second a Zen1 member, 16 to 33 sheds a page answered. Each shed was a round trip through the member's front door and an admission check on its shard |
| **Difficulty** | Small |
| **Depends on** | nothing |
| **Blocks** | nothing; M23's exit judges a scan's cpu a partition with it |
| **Tradeoff** | A shard's freed place waits a little longer for a stream to ask it, which the other streams' waits, spread by their jitter, keep short |
| **Benchmark** | `scan/Big/none` of M23's churn round, its sheds a page and its Zen1 hosts' cpu a partition, before and after |

### O149. Larger WAL segments keep a tablet's records together four times longer

| | |
| --- | --- |
| **Rank** | **to decide after M23**, as the user decided on 2026-10-31 ([the decisions](../future-work/scans/m23-exit.md#decided-with-the-user-2026-10-31)): whether `replication.segment_bytes` defaults to 40 MiB, once what a larger segment costs a node's foreground, its memory and its start is measured; W12's rate is read again at the segment it sets |
| **Impact** | Found by [M23's exit](../future-work/scans/m23-exit.md) (2026-10-31). A merge writes the partitions of one WAL segment into one block of its archive, in key order since [F121](../features/merges-in-key-order.md), so a tablet's records lie in one stretch a block, as long as a segment holds of them. `Big` loaded once and scanned with nothing beside it: at today's 10 MiB segments a Zen1 host read 10.5 partitions a device read, its 970 EVO 19.8 MiB a second at 3.81 ms of busy time a MiB, and 22.6 µs of its node's cpu a partition, 10.1 above its idle; at 40 MiB, 40 partitions a device read, 35 MiB a second at 1.95 ms, and 12.2 µs a partition, 5.2 above idle, the cluster 99,300 partitions a second against 50,600. W12's cost budget is met gross at 40 MiB and missed at 10. After an hour of X17's churn the stretches shorten where they were long and stay where they were short: at 10 MiB 10.6 partitions a device read and 22.0 µs, at 40 MiB 23.6 and 16.8 µs, 60,600 partitions a second, every figure above idle under 6 µs at 40 MiB. |
| **Difficulty** | A default, once its costs are known |
| **Depends on** | nothing |
| **Blocks** | nothing; ~~W12's cost budget at the default segment is a decision M23's exit leaves to the user~~ W12's cost is read above the idle node since 2026-10-31, held at 10 MiB segments, and W12's rate is read again at the segment this sets |
| **Tradeoff** | A segment is the unit of a merge, of a replay and of the log's memory: a larger one holds four times the log in memory and in its index before it is merged, makes each merge four times larger, and takes longer to replay after a crash. None of it was measured at the exit |
| **Benchmark** | M23's churn round at each size, `scan/Big/none` alone; and, not run at the exit, the cell beside a scan, a node's memory and its start at 40 MiB |

### O150. A merge reads the archived record of every partition an insert replaces whole

| | |
| --- | --- |
| **Rank** | **after M23**, optional. Found by reading the merge path while [item 351](resolved/the-cells-fall-is-the-drives.md) was planned (2026-11-01) |
| **Impact** | Argued, with a figure beside it. Before it folds a segment's intents, a merge reads the archived record of every partition the map names among them, 32 at a time ([O74](#o74-a-zen1-nodes-compactor-falls-hundreds-of-jobs-behind-under-the-bench)) (`load_partitions_for_intents`, `tables/storage/fs/compactor.rs`). A persistent unsorted table's plain insert replaces its partition whole: `apply_intents` (`tables/persistent/unsorted.rs`) sets the partition anew on `UnsortedIntents::Insert`. So for a partition whose first intent in the job is a plain insert, the record read is thrown away. M23's controls overwrite rows archived before a restart, and on the Zen1 hosts they read 1.5 to 1.9 device reads an insert. The arms beside them, which insert new rows, read 0.22 to 0.29. How much of that difference is the merge's read is not measured. On a 970 EVO those reads share the device with the WAL's flushes |
| **Difficulty** | S |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | A partition is read only when an intent needs it: a conditional write, an update, or an update after a delete. Where a fold starts then depends on the kind of each partition's first intent, and every table kind's `apply_intents` would have to say which. A sorted table's insert adds a row to its partition and does not replace it |
| **Benchmark** | M23's control round (the cell overwriting archived rows): its device reads an insert and its insert p99, before and after |

### O151. A Zen1 node's WAL syncs are bound by its 970 EVO's flushes once the drive is loaded

| | |
| --- | --- |
| **Rank** | **after M23**, optional. Found by [item 351](resolved/the-cells-fall-is-the-drives.md)'s run (2026-11-01) |
| **Impact** | Measured on the lab. Under M23's cell alone (about 2,470 inserts a second through three nodes at factor 3), each Zen1 node's five shards synced their WALs about 592 times a second, one device flush each, at 0.82 ms a flush: 0.49 s of every second. About 75 s into the cell's synced writes the 970 EVO's flush went to 2.83 ms ([X6](../object-storage/device-store-ssd.md#two-things-about-the-labs-970-evos)'s loaded state), and the drive then gave 324 a second, 0.92 s of every second. A WAL batch's write and sync went from 6.1 to 13.2 ms, its appends from 3.96 to 4.35, and the cell's inserts from 2,471 to 1,454 a second. Each shard's writer flushes for itself, so the drive completes five flushes where one covers the same writes |
| **Difficulty** | M |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | [F60](../features/shared-wal-flush.md) built one flush a device for every shard whose WAL was on it, and withdrew it in round 14: under the mixed bench then, the lab's devices were not bound by the WAL's syncs, and a shared flush made the write tail worse. Under the cell, in the drive's loaded state, they are. A shared flush makes every shard's commit wait for the slowest batch beside it, and a segment it covers must hold no unwritten extent, as F60's `direct` mode made sure |
| **Benchmark** | M23's cell alone for 200 s with every host sampled (`FALL_ONLY=1 sh shoal-spike-scan/results/m23-lab.sh start`, `m23-fall-report.py`): inserts a second, flushes a second and ms a flush after the step, before and after |

### O152. A step's own cpu read only past its bound

| | |
| --- | --- |
| **Rank** | **after M23**, optional. Found while [F130](../features/loop-steps-by-kind.md) was built (2026-11-01) |
| **Impact** | Measured on the lab. F130 reads a thread's cpu clock (`CLOCK_THREAD_CPUTIME_ID`) at the start and the stop of every step a walk page holds its loop for, a system call each: 0.62 µs a read on titan and hyperion, 0.45 on europa, so 1.25 µs a step on a Zen1 host, 2.5% of the 50 µs an encode step is sized to. Only a step past 100 µs by the wall needs its own cpu to be judged: reading the stop's cpu only then would halve it, to 0.62 µs a step |
| **Difficulty** | S: `StepClock::stop` takes the bound, and reads the cpu only past it |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Every shorter step's own cpu goes unread, so `all.cpu_ns` would count a short step's wall as its cpu, or be dropped, and the mean step's own cpu that item 350's attribution reads against the long steps' would be gone. ~~Not taken while that attribution needs it.~~ Item 350's attribution is done ([Resolved #350](resolved/encode-copy-deadline.md), 2026-11-01), and it read exactly that: its long steps' rows and cpu a row against every step's. Not taken while [item 353](resolved/capture-allocations.md)'s attribution may need it. The start's read cannot be saved: a step's length is known only at its stop. Item 353's counters of a long step's page faults and switches (2026-11-01) are read this way already, at every start and at the stop only past the bound, since no figure reads a short step's |
| **Benchmark** | The scan class's steps past 100 µs and W12's cost a partition on M23's rounds again, a node's clock read both ways; and the read's own cost (`clock_gettime` against `Instant::now`, two million reads pinned to a core) |

### O153. Always-on measuring logic behind a feature flag, measured

| | |
| --- | --- |
| **Rank** | **after M23**, optional. Filed at the user's request while [item 353](resolved/capture-allocations.md)'s counts were planned (2026-11-01) |
| **Impact** | Not measured. Every figure a node keeps always on adds a small cost to a hot path, each judged alone and found small: [F130](../features/loop-steps-by-kind.md)'s thread cpu clock, two system calls a step, 1.25 µs a step on a Zen1 host; item 353's `getrusage(RUSAGE_THREAD)`, one more a step, 0.40 µs; and the counters a query, a write or an object operation bumps on its way, among them [F99](../features/strong-read-rounds.md)'s rounds, [F104](../features/memory-in-a-capture.md)'s evictions, [F108](../features/object-work-in-flight.md)'s object work in flight and item 291's apply waits. Many costs of about a microsecond could add up to something a node's foreground or a scan's cost shows. The question is whether their sum is worth compiling out of a product build behind one feature, as `hotpath` and `stage-profile` are, kept in the lab's and the fixture's builds |
| **Difficulty** | Moderate: first an inventory of every always-on measurement on a query's, a step's and an object operation's path, each with its cost an operation measured on europa and a Zen1 host; then, if the sum is worth it, one feature that compiles them to nothing; then an A/B |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | A product node's figures and captures lose what is compiled out, so a defect met in production carries no evidence and its first lab run has to be on another build; two builds to test, the fixture rows that read the figures run only under the feature; and the lab's evidence comes from a build that is not the one shipped. Cheaper first: read a figure only where it is judged ([O152](#o152-a-steps-own-cpu-read-only-past-its-bound)'s kind), or sample it |
| **Benchmark** | The cell's p99 and inserts a second, and W12's cost a partition beside a scan, on a node built with the figures in and out, ABBA on M23's side cluster; and the inventory's own costs an operation, each read pinned to a core |

### O154. A capture's view allocates nothing on the loop

| | |
| --- | --- |
| **Rank** | **after M23**, optional. Filed when [item 353](resolved/capture-allocations.md)'s fix closed (2026-11-02), decided with the user |
| **Impact** | Measured as what is left: after item 353's fix the Zen1 hosts' `Big` captures pass 100 µs on their own cpu 11.4 times a 10,000, against `Keyword`'s 0.6, and 39 of those 65 are held by their view, 26 by its copy of the delta's entries and 13 by its run cursors, with the same 9 or 10 entries and 2 or 3 runs as every capture (`shoal-spike-scan/results/m23-i353f-fixed/`). Each is an allocator event, not the view's work: a capture's view still allocates four times on its shard's loop, the delta's copy, its ranges shared by the cursors, its sources and the merge's heads, and the page around it twice more. A view that allocates nothing would leave the allocator's slow paths out of the step altogether |
| **Difficulty** | Moderate: the delta a view reads kept as a snapshot it can share rather than copy (a persistent map, or the delta's entries in an `Rc` slice made again only when it changes), and a scan's cursors and merge made at its first read off the loop; the index's invariant that a scan sees the map as it stood when it began kept by the snapshot |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | A shared snapshot makes every write to the delta pay for it, a copy on write or a persistent map's nodes, where today only a scan's start pays; and a cursor made off the loop must take the runs as they stood at the scan's start, never as they stand at its first read |
| **Benchmark** | M23's exit rounds on the Zen1 hosts, the long `Big` captures a 10,000 by the part that held them (`m23-report.py`'s captures by their parts), against item 353's confirmation, and the cell's inserts a second beside them |

### O155. Two scan reads a device collapse a scan's rate ninefold

| | |
| --- | --- |
| **Rank** | **after M23**, optional. Found by [item 359](resolved/the-cells-own-climb.md)'s run (2026-11-04) |
| **Impact** | Measured once: beside the cell, a whole scan of `Big` at `scans.device_reads: 2` read 1,476 partitions a second against 12,876 at the default sixteen, and did not finish inside its event's 1,200 s (`shoal-spike-scan/results/m23-i359/`). [O146](#o146-four-scan-reads-a-device-held-a-scan-alone-to-two-thirds-of-its-rate) measured four reads a device at two thirds of sixteen's rate before items 346 and 355 changed how a device's pages take their reads. So the rate falls far faster than the depth below four, and an operator lowering it to spare a device would stop a scan nearly dead. The shards' cores rose with the scan about as much at two reads as at sixteen, so at two a partition cost about ten times the cpu |
| **Difficulty** | Unknown until attributed: whether a wave's group of reads (item 346), the turn queues for a device's places (item 355), or the share learned from fewer reads in flight holds a page back |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | None known; sixteen stays the default, and two is a setting nobody runs |
| **Benchmark** | The Big capture alone at two, four, eight and sixteen scan reads a device (`BIG_ONLY=1` with `SCANS_ALT`), the scan's partitions a second and its shards' cpu a partition |

### O156. An integer sort key priced beside a padded string

| | |
| --- | --- |
| **Rank** | ~~**any time**, optional~~ **measured** (2026-11-06): an integer key saves a fifth of a resident get of named keys, a tenth of a range or a page and a ninth of a partition's load. Moving `Event`'s or TMDB's key is a baseline decision, not taken. Found by [Resolved #42, #207](resolved/sort-keys-of-any-type.md) (2026-11-05) |
| **Impact** | Unmeasured. Two sorted tables still order by numbers padded into strings, written when a sort key could only be a `String`: the bench's `Event.at` and `MemEvent.at` (`shoal-bench/src/workloads/schema.rs`, twenty digits a key) and TMDB's `MovieByKeyword.order`, a title joined to a padded id. An integer key is eight bytes inline where a padded one is an archived string's header and its twenty bytes beside it, a heap block a resident row, and a byte comparison at every step of a seek. What that saves a sorted grid arm, a range scan or a resident partition's memory is the question |
| **Difficulty** | Easy to measure, costly to adopt: a second sorted row in the workload schema keyed by `u64`, and a pair of workloads differing in the key alone, as F9's ephemeral pairs differ in the storage engine alone. Moving `Event` itself to `u64` moves the fingerprint of every workload and parts the corpus from the frozen baseline; moving TMDB's is a new lineage for the lab's cluster ([F93](../features/operations-and-the-real-cluster.md)) |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Two more workloads in every capture, and a schema change that orphans nothing only if it is added beside `Event` rather than made to it |
| **Benchmark** | The new pair, read against each other the way the isolating pairs are: a get of named keys, a range and a paged walk of one partition, each resident and archived, and the bytes a partition charges |

**Built** (2026-11-05), the user deciding the pair lives in shoal-bench: `Bench` gains `NumberedEvent`, `Event` field
for field with its sort key a `u64`, appended after every table before it so each keeps its fingerprint, and `Event`
keeps its padded key. Twelve workloads, `macro/sort_key/{padded,u64}/{get,range,walk}/{resident,evicted}`
(`shoal-bench/src/workloads/sort_key.rs`, family `sort-key` on the table types page): 64 partitions of 1,024 rows of
256 bytes on one shard, eight named keys, the 64 rows between two keys, or a page of 64 after a cursor, each pair held
to one difference by `the_pair_differs_only_in_its_key`. A resident row is 352 bytes with the padded key and 304 with
the integer (`a_rows_resident_bytes_by_its_key`: the `String` where a `u64` was and its twenty digits on the heap),
an eighth more a row. ~~What the reads cost is the lab's run, recorded below once run.~~

**Measured** (2026-11-06) on titan (a Zen1 Ryzen Embedded V1756B, the `performance` governor), as
CLAUDE.md's *Before and after on the lab* has it: the twelve arms in four rounds, the padded form first
in odd rounds and the integer first in even ones, each on a wiped `/opt/shoal`, two shards on cpus 2 and
4 and the client on 6 and 7, locked memory unlimited (`shoal-spike/results/o156-host.sh`,
`o156-conf.yml`; the runs and `o156-report.py`'s table in `shoal-spike/results/o156/`). Each resident arm
times 7,600 reads of 64 partitions at eight in flight; each evicted arm restarts its server after
seeding and times 973 reads, every one of a partition no read before it loaded, so each loads a whole
partition of 1,024 rows from its archive. The median of the four rounds, the range beside it, in
microseconds; every pair's ranges are disjoint.

| Read | Residency | Padded p50 | Integer p50 | Integer / padded, p50 | Mean | p99 |
| --- | --- | --- | --- | --- | --- | --- |
| eight named keys | resident | 270 [270, 274] | 209 [208, 210] | 0.77 | 0.78 | 0.82 |
| 64 rows between two keys | resident | 294 [292, 295] | 264 [264, 266] | 0.90 | 0.91 | 0.93 |
| a page of 64 after a cursor | resident | 285 [279, 286] | 262 [261, 264] | 0.92 | 0.92 | 0.92 |
| eight named keys | evicted | 3,017 [3,016, 3,053] | 2,680 [2,678, 2,722] | 0.89 | 0.88 | 0.83 |
| 64 rows between two keys | evicted | 3,034 [3,018, 3,056] | 2,701 [2,694, 2,721] | 0.89 | 0.88 | 0.83 |
| a page of 64 after a cursor | evicted | 3,025 [3,008, 3,071] | 2,695 [2,665, 2,707] | 0.89 | 0.89 | 0.84 |

An integer key is cheaper everywhere a key is touched. A resident get of named keys, which seeks each
key, saves a fifth of its time; a range or a page, which seeks once and copies 64 rows, saves a tenth;
and loading a partition from its archive, which validates and builds every row and its key, saves a
ninth, a sixth at the p99. The first run of the evicted arms (`runs-first/`) read 64 partitions 8,000
times, so its warmup loaded every one and it timed resident reads: its evicted figures were the
resident ones (fixed by `aac6ceb1`). Nothing here moves `Event`'s key or TMDB's: `Event` is the frozen
baseline's schema and TMDB's `MovieByKeyword.order` the lab cluster's lineage, so either move is a
baseline decision for its owner to take, and this is the figure it would be taken on.

### O157. A node's heap on transparent huge pages

| | |
| --- | --- |
| **Rank** | ~~**any time**, optional~~ **measured, not taken** (2026-11-06): without huge pages a node holds half as much at ready and a tenth less loaded, and its foreground's tails lose, at 6 GiB and at 1 GiB, so the default stays; at a memory its floor binds it keeps a node near its budget, which a deployment may choose ([todos](todos.md#a-nodes-huge-pages-a-deployments-choice)). Found by [Resolved #338](resolved/resident-floor-reserve.md) (2026-11-05) |
| **Impact** | About a quarter of a gibibyte of every node's floor. mimalloc 2 on Linux defaults `allow_large_os_pages` to 2, which madvises its arenas `MADV_HUGEPAGE`, and the lab's hosts run THP `madvise` with khugepaged's `max_ptes_none` at 511, so a 2 MiB page is resident once any byte of it is touched and a free inside it returns nothing. On europa's loopback cluster each tmdb node held 410 to 436 MiB at ready as deployed and 149 to 157 MiB with `MIMALLOC_ALLOW_LARGE_OS_PAGES=0`, and 1,145 to 1,181 MiB against 929 to 969 MiB at rest after 100,000 movies. Europa's tmdb node held 1.82 GiB of its 1.85 resident in huge pages. jemalloc, which madvises nothing, held what mimalloc held without them |
| **Difficulty** | Easy: `Environment=MIMALLOC_ALLOW_LARGE_OS_PAGES=0` in every unit shoaladm renders, beside item 353's `MIMALLOC_PURGE_DELAY` (`shoaladm/src/deploy/unit.rs`), and its test |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | A huge page covers 512 small ones in one TLB entry, so a node whose rows, maps and indexes are spread over gigabytes may take more TLB misses on its foreground. A node's memory budget is judged on its resident set ([Resolved #149](resolved/node-memory-budget.md)), so the quarter gibibyte goes to rows instead |
| **Benchmark** | A before and after on the lab, four rounds or more at the cell's mixture (`shoaladm bench run`, rw50 and read100), the units' environment the only difference, each member's resident set and rows kept in the capture beside the ops and the get p99, and the nodes at a `node_memory` their floor binds as well as at 4 GiB |

**Measured** (2026-11-06) on the `m23` side cluster, the cell alone (`CELL_ONLY`,
`shoal-spike-scan/results/found-o157/`): O158's build (`1b7e21cb`) both sides, side B's nodes started
with `MIMALLOC_ALLOW_LARGE_OS_PAGES=0` through a drop-in on their unit, read back from each node's
environment; eight side-rounds in `A B B A A B B A` at 6 GiB a node, each a cluster bootstrapped fresh,
the cell's first 100,000 rows preloaded, `rw50` and `read100` at a bundle of one, warmed 100 s and measured
120 s. The median of four side-rounds a side, the range beside it.

| At 6 GiB a node | Huge pages, as deployed | None | None / huge | Disjoint |
| --- | --- | --- | --- | --- |
| Resident set at ready, europa / hyperion / titan | 289 / 283 / 281 MiB | 153 / 143 / 144 MiB | 0.51 to 0.53 | yes |
| Of it in huge pages, once loaded | 683 / 658 / 648 MiB | 0 | 0 | yes |
| Resident set once loaded | 1,411 / 1,386 / 1,388 MiB | 1,287 / 1,264 / 1,265 MiB | 0.91 | yes |
| `rw50` read p99 | 0.744 [0.716, 0.778] ms | 0.774 [0.766, 0.784] ms | 1.04 | no |
| `rw50` read p999 | 2.97 [2.72, 3.07] ms | 4.29 [3.41, 6.38] ms | 1.44 | yes, worse |
| `rw50` insert p999 | 41.2 [40.3, 42.0] ms | 43.1 [42.0, 44.8] ms | 1.05 | yes, worse |
| `rw50` inserts a second | 1,703 [1,684, 1,717] | 1,697 [1,696, 1,701] | 1.00 | no |
| `read100` read p99 | 0.410 [0.338, 0.453] ms | 0.394 [0.351, 0.517] ms | 0.96 | no |

With O158 in, huge pages hold half a node's resident set at ready and about a tenth once it is loaded,
and taking them away costs the foreground's tail: a read beside the cell's inserts at the p999 is 1.44
times slower, an insert's 1.05, each disjoint. At a memory nothing binds, the default stays.

At a memory the floor binds, 1 GiB a node (`shoal-spike-scan/results/found-o157f/`, four side-rounds in
`A B B A`, so two a side and a range of two), huge pages are most of what a node holds past its budget:

| At 1 GiB a node | Huge pages, as deployed | None | Disjoint |
| --- | --- | --- | --- |
| Resident set once loaded, europa / hyperion / titan | 1,683 / 1,020 / 1,619 MiB | 1,598 / 1,016 / 1,005 MiB | yes |
| Of it in huge pages | 945 / 293 / 1,047 MiB | 0 | yes |
| `rw50` read p50 | 0.346 [0.332, 0.359] ms | 0.258 [0.255, 0.260] ms | yes |
| `rw50` read p99 | 5.67 [5.50, 5.85] ms | 5.50 [5.31, 5.68] ms | no |
| `rw50` read p999 | 11.9 [11.9, 12.0] ms | 12.4 [12.3, 12.5] ms | yes, worse |
| `rw50` inserts a second | 1,587 [1,575, 1,598] | 1,621 [1,615, 1,626] | yes |
| `read100` read p99 | 0.756 [0.728, 0.783] ms | 0.818 [0.810, 0.826] ms | yes, worse |
| `read100` read p999 | 1.17 [1.12, 1.22] ms | 1.33 [1.29, 1.36] ms | yes, worse |

Titan's node held 1.62 GiB against its gibibyte with huge pages and 1.0 without, and the cell's reads
beside its inserts were a quarter faster at the median, its inserts 2% faster. The tails still lost, a
read alone 8% at the p99 and 13% at the p999. So a node's default keeps its huge pages: what they cost a
node near its floor is offered as a choice its deployment may make
([todos](todos.md#a-nodes-huge-pages-a-deployments-choice)), not taken for every node.

### O158. What openraft holds of every node's floor

| | |
| --- | --- |
| **Rank** | ~~**any time**, optional~~ **applied** (2026-11-05), the forks and the table below, and measured on the lab: a node's resident set at ready a quarter of what it was and loaded 0.62 of it, the foreground unmoved. Found by [Resolved #338](resolved/resident-floor-reserve.md)'s heap profile (2026-11-05) |
| **Impact** | About 0.2 GiB a node of the lab's loopback shape, a quarter of what its heap held after 100,000 movies. Three parts: (1) `Engine::new` allocates `EngineOutput::new(4096)`, a `VecDeque` of 4,096 commands a group (`openraft-0.10.0-alpha.34/src/engine/engine_impl.rs`), 156 to 162 MiB a node. It is #191's shape again: a ring whose head moves on with every command, so its pages become resident as load passes through and are never given back. (2) Each group's runtime stats, and once a process the scale tables of `base2histogram` behind them (`LOG_SCALES`, a `LazyLock` built whole on first use), 39 to 40 MiB together. (3) Each group's remembered requests, `LruCache::new(REMEMBERED_REQUESTS = 4096)` (`shoal-core/src/server/replication/machine.rs`), 6 to 11 MiB |
| **Difficulty** | (1) and (2) are openraft's: a patch through `[patch.crates-io]` sizing the engine's queue as it fills (openraft's own `command_scheduler.rs` asks for 8) and building the histograms' scales on demand, then offered upstream. (3) is Shoal's own constant |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | A patched openraft is a fork to carry until upstream takes it. A queue that grows as it fills reallocates under a burst, which a pre-sized one never does |
| **Benchmark** | The profile `target/lab/i338` took, again on the loopback cluster after the same load (`jemalloc-prof`, `shoal-spike/results/heap.py`), the three stacks' bytes before and after; and every node's resident set at ready, loaded and at rest |

**Applied** (2026-11-05), the user deciding to carry forks of both crates as glommio's is: `../openraft`
(`29cc78e`) and `../base2histogram` (`319f4d3`), each the published crate in its first commit, patched through the
workspace's `[patch.crates-io]`, never pushed.

- **openraft**: the engine's command queue starts at 64 and its four respond queues at 16, each growing as it fills
  and giving back room a burst grew it past 1,024 or 256 once drained (`EngineOutput::release_idle`, after
  `RaftCore` runs its commands); openraft's 551 library tests pass, with one of its own for the release.
- **base2histogram**: a log scale's tables are built for the width asked for, a `OnceLock` a width, not all sixteen
  on first use.
- **Shoal**: a group's retry table starts at 64 identities and doubles as it fills, up to `REMEMBERED_REQUESTS`,
  forgetting only past it as before (`the_retry_table_grows_to_its_bound_and_forgets_only_past_it`); and
  `group_config` sizes openraft's never-written log of entry stages to one (`log_stage_capacity`).

The same loopback run as #338's (three tmdb nodes on europa, 100,000 movies loaded and walked,
`shoal-spike/results/o158/`), the build before against the build with it, jemalloc profiling every 128 MiB:

| | Before (`c0b0aad6`) | After |
| --- | --- | --- |
| One node's live heap, loaded (sampled) | 0.85 GiB | 0.63 GiB |
| `Engine::new`'s queues | 120.2 MiB | gone from the top 40 |
| `PendingResponds::new` | 38.2 MiB | gone |
| `LogScale::new` (`LOG_SCALES`) | 28.7 MiB | gone |
| `MachineState::at`'s pre-sized retry table | 10.5 MiB | gone; its entries 8.5 then 17.4 MiB as tables grew |
| Resident set at ready, each node | 155 to 163 MB | 127 to 134 MB |
| Resident set loaded | 836 to 843 MB | 765 to 772 MB |
| Resident set at rest | 849 to 856 MB | 765 to 772 MB |

~~What it costs a node's foreground on the lab is the cell's A/B, recorded below once run.~~ On the lab
(2026-11-05), the cell alone on the `m23` side cluster (`CELL_ONLY`, `shoal-spike-scan/results/found-o158/`):
eight side-rounds in `A B B A A B B A`, each a cluster bootstrapped fresh at 6 GiB a node, the cell's
first 100,000 rows preloaded, then `rw50` and `read100` at a bundle of one, each warmed 100 s past item
351's change of flush and measured 120 s. Side A is the build before (`c0b0aad6`), side B this one
(`1b7e21cb`). Every figure is the median of its four side-rounds, the range beside it; a difference is
a result only where the ranges are disjoint.

| | Before | After | After / before | Disjoint |
| --- | --- | --- | --- | --- |
| Resident set at ready, europa / hyperion / titan | 1,188 / 1,131 / 1,138 MiB | 289 / 283 / 283 MiB | 0.24 to 0.25 | yes |
| Of it in huge pages at ready | 1,142 / 1,083 / 1,094 MiB | 250 / 230 / 229 MiB | 0.21 to 0.22 | yes |
| Resident set once loaded | 2,264 / 2,233 / 2,212 MiB | 1,425 / 1,378 / 1,378 MiB | 0.62 to 0.63 | yes |
| Resident set 60 s after | 2,260 / 2,232 / 2,209 MiB | 1,421 / 1,377 / 1,372 MiB | 0.62 to 0.63 | yes |
| `rw50` inserts a second | 1,701 [1,678, 1,715] | 1,695 [1,673, 1,707] | 1.00 | no |
| `rw50` insert p99 | 33.7 [33.2, 34.3] ms | 33.7 [33.5, 34.4] ms | 1.00 | no |
| `rw50` read p99 | 0.776 [0.740, 0.792] ms | 0.774 [0.736, 0.790] ms | 1.00 | no |
| `rw50` read p999 | 3.15 [2.78, 3.61] ms | 2.92 [2.74, 3.19] ms | 0.92 | no |
| `read100` reads a second | 215,074 [204,321, 221,650] | 215,814 [205,580, 221,668] | 1.00 | no |
| `read100` read p99 | 0.386 [0.356, 0.467] ms | 0.400 [0.367, 0.452] ms | 1.04 | no |

A node of the side cluster's shape hosts many more groups than the loopback run's, so what openraft
allocated whole at every group's start was most of its resident set at ready: 0.85 to 0.9 GiB of each
node's, nearly all of it in huge pages. Nothing the foreground does moved; every range overlaps.

### O159. A compacted segment's checkpoint and its evictable marking hold a shard's loop for milliseconds

| | |
| --- | --- |
| **Rank** | ~~**to build** (2026-11-05), the user deciding on O140's attribution to fix both holds and A/B them on the lab~~ **applied** (2026-11-06, `bdd2958c`): both holds a step of a quarter of a millisecond at most on a Zen1 shard, where they were 16 and 4 ms, measured on the lab. Found by [O140](#o140-a-resident-reads-p99-beside-the-cells-inserts-is-six-times-scylladbs-on-the-same-hosts)'s attribution (2026-11-05) |
| **Impact** | Profiled, on the lab, by `shoal-core`'s `loop-profile` build: two side-rounds of the cell alone on the `m23` side cluster (`shoal-spike-scan/results/found-o140/`). A shard's loop is idle 97 to 99% of the cell's `rw50`, and the two longest things that hold it are rare: a compacted segment (`ServerMsg::SegmentCompacted`), 15.7 to 16.1 ms at the mean and 32 ms at the p99 on the Zen1 hosts and 6.7 to 7.3 ms on europa, about 21 to 24 times a node in 120 s; and the evictable marking of its partitions (`ServerMsg::MarkEvictable`), 3.9 to 4.3 ms at the mean and 16 ms at the p99 on the Zen1 hosts. A read on that shard waits behind them, and the cell's read p999 beside its inserts is 3.3 to 4.4 ms against 0.57 to 0.61 ms with no inserts, its max 29 to 71 ms |
| **Difficulty** | M. A segment's compaction moves the checkpoint of every group of its table, and `write_checkpoint` (`shoal-core/src/server/shard/groups.rs`) then builds the whole retry sidecar on the loop: every persistent group's remembered identities through its checkpoint, up to 4,096 a group (`remembered_through`), each walked through the retry table's `LruCache` (a heap node an identity), walked a second time for the group's floor (`retry_floor`), and the file and the sidecar cloned once more each to keep them; about 2.4 MB a shard on the lab's nodes. `mark_evictable` walks every partition the segment held in one step |
| **Depends on** | nothing |
| **Blocks** | nothing; [W7's budget](../future-work/scans/isolation.md#the-budget) is a ratio |
| **Tradeoff** | The sidecar is what a group's restart seeds its retry table from, and its crash safety is [Resolved #115](resolved/retry-sidecar-crash-window.md)'s: a group's entries must stay those through the checkpoint the file names beside them. A marking taken in steps leaves a partition unevictable for a few more messages |
| **Benchmark** | The cell alone side against side on the lab (`CELL_ONLY`), both sides `loop-profile` builds with sub-marks inside the two handlers: each message kind's hold by member, and the read p99 and p999 beside the inserts |

**Split first** (2026-11-06): side A of its A/B (`48173e91`) times the parts of both holds in a
`loop-profile` build, one side-round of the cell alone (`shoal-spike-scan/results/found-o159a/`). On
titan a compacted segment's 15.8 ms were 8.9 ms of capture (73,700 identities of 54 groups, every one in
one of the 18 groups the segment moved, so reusing the groups whose checkpoint had not moved would have
saved nothing), 0.8 ms of the two copies kept, under 0.1 ms finding the segment and moving its groups'
checkpoints, and about 6 ms the sidecar's encoding, which ran inside the message because glommio's
`spawn_local` polls the task it spawns before it returns. A marking walked 8,800 partitions at the
median, about 440 ns each.

**Built** (2026-11-06, `bdd2958c`), the user deciding on the attribution to fix both holds:

- **A checkpoint write is its task's, a group a step.** The task yields first, so nothing is taken inside
  the message; then it reads each persistent group's checkpoint and entries in one borrow and yields
  (`capture_checkpoints`), puts the capture whole where a group started from then reads it
  (`KeptCheckpoint`, shared by `Rc`, never copied), and encodes the sidecar a group a step
  (`RetriesEncoder`, byte for byte the whole encoding, its checksum filled into room left for it) before
  it stages, writes and promotes as before.
- **A group's entries and its floor come from one walk**, sized at once (`remembered_and_floor`).
- **A compacted segment's partitions are marked 256 a step** (`MARK_STEP`), the first chunk in the
  message and the rest from the loop's second inbox (`IdleStep::MarkEvictable`), each judged against the
  generation when its step runs.

**On the lab** (2026-11-06, `shoal-spike-scan/results/found-o159/`): the cell alone on the `m23` side
cluster, eight side-rounds in `A B B A A B B A`, both sides `loop-profile` builds, side A `48173e91`
(its holds timed by their parts) and side B `bdd2958c`. The median of four side-rounds a side, the
range beside it.

| | Side A | Side B | Disjoint |
| --- | --- | --- | --- |
| A compacted segment's hold, titan / hyperion, mean | 16.0 / 16.0 ms | 64 / 62 µs | yes |
| Its p99, the Zen1 hosts | 32,768 µs's bucket | 128 to 256 µs's | yes |
| A segment's marking's hold, titan / hyperion, mean | 4.2 / 4.3 ms | 133 / 125 µs | yes |
| Its p99, the Zen1 hosts | 16,384 µs's bucket | 256 µs's | yes |
| A checkpoint write's step, the Zen1 hosts, mean (p99) | none: the write was one hold | 113 to 114 µs (1,024 µs's bucket) | - |
| A routed query's wait to be taken, titan, mean (p99) | 48 µs (256's) | 47 µs (256's) | no |
| `rw50` read p999 | 3.63 [2.68, 4.34] ms | 2.58 [2.17, 2.81] ms | no |
| `rw50` read p99 | 0.784 [0.753, 0.796] ms | 0.772 [0.762, 0.812] ms | no |
| `rw50` inserts a second | 1,698 [1,690, 1,701] | 1,705 [1,697, 1,707] | no |
| `read100` read p99 | 0.409 [0.387, 0.529] ms | 0.413 [0.359, 0.466] ms | no |

The two holds are gone: the longest thing either does on a Zen1 shard's loop is a step of a quarter of
a millisecond at most, where it was 16 and 4 ms. The cell's read p999 beside its inserts fell to 0.71 of
side A's at the median, but the ranges overlap at four side-rounds a side, so it is not yet a result.
With the holds gone, a routed query waits at most 256 µs at the p99 on a Zen1 shard either side, so
what is left of the client's p999 is not the shard's loop: the longest holds left are its tick and a
sealed segment ([O160](#o160-a-shards-tick-and-a-sealed-segment-hold-a-zen1-shards-loop-for-up-to-two-milliseconds)).
Nothing else moved.

### O160. A shard's tick and a sealed segment hold a Zen1 shard's loop for up to two milliseconds

| | |
| --- | --- |
| **Rank** | **any time**, optional, **to measure**: where the tick's time goes, before anything is changed for it. Found by [O159](#o159-a-compacted-segments-checkpoint-and-its-evictable-marking-hold-a-shards-loop-for-milliseconds)'s A/B (2026-11-06) |
| **Impact** | Profiled, by `loop-profile` builds of the cell alone on the `m23` side cluster (`shoal-spike-scan/results/found-o159/`). With O159's two holds gone, the longest things holding a Zen1 shard's loop beside the cell's inserts are a peer message, 0.40 ms at the mean and in the 1,024 or 2,048 µs bucket at the p99, about ten a second, nearly all of them the loop's tick (`PeerEvent::Tick`); and a sealed segment (`ServerMsg::WalSealed`), 0.43 to 0.53 ms at the mean and 2,048 µs's bucket at the p99, about every six seconds. On europa the tick is 0.20 ms and a sealed segment 4 µs. Each is under a millisecond at the mean, so what either adds to a read's tail is small beside what O159 took away |
| **Difficulty** | Unknown until measured. The tick sweeps the gathers and the deadlines, reports, checks the disk, balances leadership, builds deferred snapshots, and drives repairs, moves, backups, restores, scrubs, stalled copies and retired ones, each over the shard's groups, ten times a second, much of it finding nothing to do |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Work the tick does less often is noticed later: a deadline, a move or a repair waits up to the longer period |
| **Benchmark** | The cell alone side against side (`CELL_ONLY`), `loop-profile` builds with the tick timed by its parts and the sealed segment by its own |
