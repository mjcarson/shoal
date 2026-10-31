# Milestones

**Nothing below is delivered, and no gate has been set**; the contract that comes before them was agreed on
2026-10-29 ([below](#before-m22-the-contract-agreed)). This page is an order of work, drawn before
any [spike](spikes.md) has reported, so that its dependencies can be argued with. It becomes a plan
gate by gate: each is set once the questions it waits on are decided on
[W13](contract.md#questions-to-answer), with its acceptance tests named on their owning pages and its
evidence stated before its work starts.

M22–M28 continue the object storage part's [M11–M21](../../object-storage/milestones.md) and are stable
identifiers. Acceptance tests live on their owning W pages, indexed by
[W13](contract.md#the-acceptance-index), and each names one gate below.

**Every gate keeps the cluster's rules.** No gate adds a consensus protocol, a coordination service
or a clock that decides anything ([R7](../../distributed/overview.md#what-is-asked-of-it)), and no gate
is passed by weakening a clause of [the contract](contract.md#the-contract). And no gate that a client
can reach is passed without [W7](isolation.md#the-budget)'s budget measured beside it.

## How a gate is written

| Line | Holds |
| --- | --- |
| **Closed before it** | The questions of [W13](contract.md#questions-to-answer) decided, with their evidence on the record, before the gate's work starts |
| **Lands first** | The required rows of [W1](prerequisites.md#required) placed at its start. Each is a change of its own, and nothing stops it landing sooner |
| **Delivers** | What exists afterwards |
| **Acceptance** | The owning pages' rows that name this gate |
| **Evidence/exit** | What is measured or demonstrated beyond tests passing. Every number labelled by host, cpu and governor |
| *Not at this gate* | What a reader might expect here and will not find |

## The gates at a glance

| Gate | In a line | Closed before it | Lands first |
| --- | --- | --- | --- |
| Before M22 | ✅ The contract agreed, 2026-10-29 | P20–P26 | — |
| M22 | ✅ The harness and the facts: the bench's scan, the fixture's oracle, the baseline, delivered 2026-10-30 (F122) | — | The bench's scan kind and `scan` event |
| M23 | The walk rebuilt for load, the drivers moved onto it, judged by the budget and, since W42, by a scan's own rate and cost | Q33, Q34, Q37's default, Q62 | The scan class, the budget, device pacing, an admission class; one operation a record read, merges written in key order |
| M24 | Scans on one node: the frames, the client, SHQL, the grant | Q35, Q36 | — |
| M25 | Scans across a cluster: copies, floors, refusals, activation | Q38, Q39 | — |
| M26 | Listing a bucket's objects, unordered | What a listing reads (X19) | — |
| M27 | Ordered scans by declared order | Q40 | — |
| M28 | The ordered listing | Q41 | — |

## The gates

### Before M22: the contract agreed

**Settled 2026-10-29**: P20–P26 agreed as written ([W13's record](contract.md#before-m22-the-contract-agreed-2026-10-29)).
~~**Not settled.**~~ It ~~is~~ was settled when P20–P26 on [W13](contract.md#the-contract) ~~are~~ were agreed, each
beside the violation that breaks it and the tests that own it, as the object store's P7–P19 were
before M11 ([S18's record](../../object-storage/contract.md#before-m11-the-contract-agreed-and-q27s-path-modelled-2026-10-09)).
No type, frame or file format of a scan is added at this gate.

### M22. The harness and the facts

**Delivered** (2026-10-30), as [F122](../../features/scan-oracle.md): the scan oracle, the fixture's `SCAN`
verb and a row that holds a scan beside writes to the oracle, and this part's acceptance tables joined
to the book's check, beside F115's scan kind and event and X16's baseline, which landed ahead of it.

**Closed before it.** The contract.

**Lands first.** The bench's scan kind and `scan` event ([W1](prerequisites.md#the-benchs-scan-kind-and-scan-event)):
✅ landed on 2026-10-29 as [F115](../../features/bench-scans.md).

**Delivers.**
- The bench's scan operation kind, ~~`list` kind~~ and `scan` event, the arm `{mix}/b{bundle}/scan` cut by
  `cut_background` ([W12](performance.md#what-the-driver-gains)) ✅ (F115). The `list` kind is M26's, since a
  listing needs M26's engine: W12's row for it, `a_list_kind_drives_a_buckets_listing`, gates M26.
- The fixture's scan oracle: a check of P20 to P22 for each group against the committed history, and
  nothing across groups, so no test can lean on what P23 withholds ✅ (F122, `shoal_model::scan`).
- A `SCAN` verb in the fixture that drives the walk as a scan would, ~~every shard's pages at once~~ its
  pages one at a time, each at its group's floor ✅ (F122): a node's pool is not `Sync`, so the child
  cannot share its walk among threads without an engine change this gate does not make, and pages in
  flight together are the bench's (F115) and M24's driver's.
- This part's acceptance tables joined to `shoal-bench/tests/acceptance_tables.rs`, which learns
  M22–M28, so a test name is unique across all three parts and every test of a delivered gate is a
  function in the workspace ✅ (F122), which found [W42](scan-speed.md)'s copy of W5's
  `a_page_reads_its_archives_in_runs` row.
- [X16](spikes.md#x16-the-walk-beside-the-foreground)'s baseline on the record: ✅ taken ahead of the gate on
  2026-10-29 ([its record](walk-beside-the-foreground.md)).

**Acceptance.** [W12](performance.md#acceptance-tests)'s M22 rows and W13's oracle test.

**Evidence/exit.** X16's tables: what the walk as it stands costs the reference cell on each host
(✅ [taken](walk-beside-the-foreground.md#the-tables), by member where the hosts differ).

*Not at this gate*: any change to the engine, the wire or the client.

### M23. The walk rebuilt for load

**Closed before it.** Q33, the engine, by X16 and [X17](spikes.md#x17-a-page-rebuilt); Q34, the budget
and the pace, by X16 and [X18](spikes.md#x18-table-reads-paced-by-idle-time); Q37's default, on the
shards under a ceiling; and since [W42](scan-speed.md) Q62, a tablet's records kept together, by
[X37](spikes.md#x37-a-tablets-records-together-priced).

**Lands first.** The scan class on every table shard, the budget for transient read buffers, table
reads paced by a device's idle time, and an admission class apart from the drivers'
([W1](prerequisites.md#required)): ✅ all four landed on 2026-10-29 as F116 to F119, for a scan's pages
alone, the drivers moved onto them here. Since [W42](scan-speed.md) (2026-10-29), for its rate, also one
operation a record read and merges written in key order, ~~neither started~~ ✅ both landed on 2026-10-30
as [F120](../../features/one-operation-a-record-read.md) and [F121](../../features/merges-in-key-order.md).

**Delivers.**
- [W5](tablet-reader.md)'s six steps (✅ the plan, [F124](../../features/scan-page-planned.md), the archive reads, [F125](../../features/page-reads-in-runs.md), the encodes in steps, [F126](../../features/encodes-in-steps.md), and the folds and the check, [F127](../../features/folds-in-steps.md), 2026-10-31): admission; a plan on the loop that serializes nothing; archive
  reads ~~in runs, in disk order,~~ several in flight, and in runs where a tablet's records lie together
  ([W42](scan-speed.md)), in idle time under a ceiling; resident partitions encoded in steps
  from a second inbox, folded over fresh lookups; folds, filters and encodes in the scan queue; the
  check at the page's end.
- The object store's drivers, backfill, copier, light scrub and reclaimer, on the rebuilt engine, with
  their admission kept: ✅ [F129](../../features/drivers-on-the-rebuilt-walk.md) (2026-10-31), M23's seventh step, the backup archive's walk
  left on the capture, and the backfill asking a shed page again
  ([Resolved #345](../../appendix/resolved/backfill-shed-page.md)).
- A node's counters of walks and scans: ✅ [F123](../../features/walk-and-scan-counters.md)
  (2026-10-31), M23's first step.
- The pace in device time, as M23's plan decided with the user on 2026-10-31: a share of each
  device's busy time a second, 75 ms, learned from the scans' own reads under the byte ceiling, ~~four~~
  sixteen scan reads in flight a device since [O146](../../appendix/optimizations.md#o146-four-scan-reads-a-device-held-a-scan-alone-to-two-thirds-of-its-rate),
  O143's waits cut, and a page its device holds back ending short inside its deadline:
  ✅ [F128](../../features/device-time-pace.md) (2026-10-31), M23's sixth step, its reads paced a group
  at a time since [item 346](../../appendix/resolved/paced-reads-alone.md).

**Acceptance.** [W5](tablet-reader.md#acceptance-tests)'s and [W7](isolation.md#acceptance-tests)'s M23
rows.

**Evidence/exit.** The budget held on the lab beside a full scan driven in process, of a table larger
than memory after a restart and of `MovieByKeyword`, eight rounds a host; every test on F87's page
unchanged; the drivers' own figures no worse than before. Since [W42](scan-speed.md): a scan of `Big`
alone at its ceiling on the lab's SSDs, within [W12](performance.md#the-budgets)'s cost budget, and
beside the cell within the foreground's.

**The exit, run on the lab 2026-10-31** ([its record](m23-exit.md)), at the exit's commit with every
scan setting its default. Held: the cell's read p99 beside a whole scan of `Big`, 1.07 times, and of
`Keyword`; no `Resident` eviction pass; ten partitions a device read and more for `Big` loaded once,
nine and more after an hour's churn at both segment sizes; the drivers' figures no worse. Open, for the
user to decide on the record's evidence: the insert clause, which W7's ratio misses because the
cell's feed wraps inside an arm this long, and which before the wrap holds beside `Big` and rises
beside `Keyword` no further than the cell alone does over the same seconds; loop steps
past 100 µs, about one in 460, nine in ten of them steps of large resident partitions, which item
347's bound does not hold
([item 350](../../appendix/known-issues.md#350-a-scans-steps-of-large-resident-partitions-hold-their-loop-past-100-µs)),
and the rest light steps on shards their own io_uring workers preempt
([O147](../../appendix/optimizations.md#o147-each-shards-io_uring-workers-run-on-the-shards-own-cpu));
a Zen1 host's cpu a partition, 22.6 µs gross and 10.1 above its idle node at today's segments, 12.2
gross at 40 MiB ([O149](../../appendix/optimizations.md#o149-larger-wal-segments-keep-a-tablets-records-together-four-times-longer));
and the share, which buys a 970 EVO about 20 MiB a second alone where the plan expected 32. The quick
passes before the rounds found [items 346](../../appendix/resolved/paced-reads-alone.md) and
[347](../../appendix/resolved/encode-steps-by-rows.md), O146 and O148, each fixed before the rounds,
and the rounds [items 348 to 350](../../appendix/known-issues.md#348-a-failed-devices-left-slice-waits-out-the-readers-grace-and-a-move-that-needs-it-waits-with-it),
filed.

*Not at this gate*: anything a client can call. Isolation is proven before any client can scan, for the
reason X9 ran before the object store's first device: what a design costs a table's tail decides the
design.

### M24. Scans on one node

**Closed before it.** Q35, what an answer carries; Q36, the cursor and the frames.

**Delivers.**
- `ScanPage` and `ScanAnswer`, `CLIENT_CAP_SCANS`, refusals by name, `Cancel` of a page
  ([W4](cursor-and-pages.md)).
- The client's driver against one node: streams by hosting shard, resume tokens, `split(n)`
  ([W6](distribution.md), [W10](api.md#the-client)).
- `MovieScan` generated beside each get, with its filter, limit and projection.
- SHQL's `SCAN`, and `shoalctl`'s paging.
- The `scan` privilege, read from a standalone node's file.
- Scan counters in a node's figures.

**Acceptance.** The M24 rows of [W2](ordering.md#acceptance-tests), [W4](cursor-and-pages.md#acceptance-tests),
[W7](isolation.md#acceptance-tests), [W10](api.md#acceptance-tests) and [W13](contract.md#acceptance-tests).

**Evidence/exit.** The budget beside a client's scan over the wire to a standalone node on the lab, the
foreground on the scan's own client and link too, as [X20](scylladb-scan-sources.md#what-the-run-says)'s
rounds carrying rows had it, with the hosts' TCP counters read beside it; and the scan's rate a node
reported.

*Not at this gate*: a cluster, a listing, an ordered scan.

### M25. Scans across a cluster

**Closed before it.** Q38, copies and levels; Q39, whether `all` includes `scan`, with the user.

**Delivers.**
- The driver over a cluster: voters only, followers first and spread, floors a group, one barrier a
  group at `Quorum`, every refusal routed around, a move, a failover and a restart carried through,
  a scan resumed by a fresh client from its token ([W6](distribution.md)).
- The scans' wire version, activated as every addition since F48 is, and `scan` grantable in the
  committed grants only behind it ([W10](api.md#grants)).

**Acceptance.** The M25 rows of [W4](cursor-and-pages.md#acceptance-tests),
[W6](distribution.md#acceptance-tests), [W10](api.md#acceptance-tests) and [W13](contract.md#acceptance-tests).

**Evidence/exit.** The budget on the lab's three nodes beside a scan at its defaults; a scan through a
node's kill and through a rebalance in the fixture with P20 checked by the oracle; a scan's figures on
followers beside the same scan forced onto leaders.

*Not at this gate*: a listing, an ordered scan, a coordinator for thin clients.

### M26. Listing a bucket's objects

**Closed before it.** What a listing reads against the inline threshold, by [X19](spikes.md#x19-the-edges).

**Delivers.** The `BucketListing` target: live entries, every entry of a row, the listing's projection,
never an object's bytes; `client.bucket::<M>().list()`; the bucket's `scan` grant; `shoalctl`'s bucket
listing; the bench's `list` kind ([W9](bucket-listing.md)).

**Acceptance.** [W9](bucket-listing.md#acceptance-tests)'s and [W12](performance.md#acceptance-tests)'s
M26 rows.

**Evidence/exit.** The budget beside a listing on the lab; entries a second a node, for a bucket of
inline objects and one of striped ones.

*Not at this gate*: path order, a prefix, a delimiter.

### M27. Ordered scans by declared order

**Closed before it.** Q40, how a declared order is kept, by [X21](spikes.md#x21-declared-order-priced).

**Delivers.** The `place`, `order` and `cover` attributes and a table's `spread`; `OrderedKey` and the
derive's encodings; the order index in each tablet, kept at apply and compaction and carried by an
install, a restore, a move, a rehome and a scrub; collisions refused by name; ordered scans and spread
merges in the driver; generated placement and order types ([W8](declared-order.md)).

**Acceptance.** [W8](declared-order.md#acceptance-tests)'s M27 rows.

**Evidence/exit.** X21's prices confirmed on what was built: gets and inserts of a declared-order table
beside the same table placed by its whole key, and ordered pages a second, covering and not.

*Not at this gate*: an ordered listing.

### M28. The ordered listing

**Designed, later.** **Closed before it.** Q41, by [X22](spikes.md#x22-the-ordered-listing-two-ways).
**Delivers.** A listing in the byte order of paths, with a prefix, a delimiter's common prefixes and a
continuation, on the layout X22 chose ([W9](bucket-listing.md#the-ordered-listing-a-later-gate)).
**Acceptance** and **evidence** are set at its gate.

## Beside the gates

~~None of these is needed by a gate, and each can be added later without changing anything built:~~
None of these is needed by a gate, and each could be added later without changing anything built.
**Since 2026-10-31 every one is planned**, decided with the user once the spikes before M23 had
reported, each where it costs least or can first be measured, which
[the work beside the gates](whats-left-todo.md#the-work-beside-the-gates) draws.

**After M23**, on the rebuilt walk:

- **Larger or consolidated merge blocks** ([W42](scan-speed.md#the-levers-in-the-order-they-land)). A
  larger block is a setting, not code: a merge's block is a WAL segment
  (`cluster.replication.segment_bytes`, which an inventory renders), so M23's lab exit churns at
  10 MiB segments and at 40 MiB, which sets the default and says whether an archive pass consolidates a
  scanned table's blocks as well ([X37](tablet-records-together.md#after-churn)).
- **Polled NVMe queues on a host, and the WAL on a device of its own** ([W42](scan-speed.md#the-levers-in-the-order-they-land)):
  a host's settings and a deployment's choice, the two roots already allowing the second, priced on
  the rebuilt walk, where runs cut the ring's share of a node's samples from about a quarter to under
  a tenth.

**With M24**, as it first defines what an answer carries, so no later wire version is needed:

- **Whole partitions in answers** for an unfiltered scan, the client decoding them
  ([Q35](contract.md#questions-to-answer)); [X17](page-rebuilt.md#rows-encoded-on-the-shard-against-walked-partitions)
  moved it forward.

**After M24**, once a client scans one node:

- **The next cursor sent ahead of a page's rows**, so a stream's next request leaves sooner
  ([W4](cursor-and-pages.md#alternatives-rejected)), which needs no new frame and is priced on M24's
  streams.
- **A scan connection handed to the executor of its slot**, saving the hop of a page's bytes
  ([W6](distribution.md#streams)).
- **Saved positions between pages** ([W5](tablet-reader.md#saved-positions-optional)), priced on a
  table whose tablets take many pages. X17 (2026-10-30) found nothing to save on the lab's tables,
  every tablet one page.
- **A client helper that steps through keys a caller names**, as pipelined gets ([W2](ordering.md#alternatives-rejected)).
- **A count of scans a principal**, so one tenant cannot take every shard's class
  ([W7](isolation.md#admission)).

**After M25**, once scans run across a cluster:

- **Aggregates pushed to the shards**, a count or a sum answered by each page as a partial, as
  ScyllaDB's parallelized aggregation does ([W3](prior-art.md#scylladb)).
- **A scan coordinated by a node**, for a client that is not Rust ([Q42](contract.md#questions-to-answer)).
- **Scan work on executors of their own**, Q37 asked again then ([W7](isolation.md#cpu)); X17's and
  X18's cpu a partition at a ceiling's rate gave no reason to move it sooner.

**Before M26**, decided by [X19](spikes.md#x19-the-edges), which prices a listing's bytes:

- **A listing's fields kept apart from an object's inline bytes**, so a listing reads less
  ([W9](bucket-listing.md#what-a-listing-reads)); it changes what a bucket stores, so it costs least
  before buckets hold much.

## The order is a claim

The order above argues for itself:

- **The engine before the wire.** A scan's cost to the queries beside it is proven on the rebuilt walk,
  driven in process, before any client can ask for one: the same reason X9 came before the object
  store built a device.
- **One node before a cluster.** The frames and the client against one node, then the driver's
  copies, floors and refusals across many, as M14 came before M15.
- **Scans before a listing.** A listing is a scan with a target and a projection.
- **The listing before ordered scans**, by the decision of 2026-10-25: an unordered listing first.
- **Ordered scans before the ordered listing.** Every way to list in path order stands on W8's index.

Two places in it are convenience, not dependency: M26 needs M25 and nothing after it, and M27 could
start beside M26 once X21 reports.

## Related

[W1](prerequisites.md) for the rows the gates begin with; [Spikes](spikes.md) for what has to report
first; [W13](contract.md) for the questions and the record; [What's left to do](whats-left-todo.md) for
all of it drawn.
