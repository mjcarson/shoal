# F115. The bench's scan kind and `scan` event

`shoaladm bench` can now scan a table whole. A **scan arm**, `scan/{Table}[/c{n}]/none`, runs whole
scans of a table back to back and measures a scan's own rate; a **`scan` event**, appended to the
events, scans a table once beside any table arm's load, so `rw50/b1/scan` is the reference cell's
mixture with a full scan run through it, cut at the scan's asking and its last page. Both drive the
same scan: every tablet walked once through the [admin walk](tablet-walk.md), each from a holder the
route table names, followers first and spread over the members, with as many pages in flight as it
has streams, two a shard of every member by default. A window that saw a scan's pages keeps what
they held: pages, partitions, rows and archived bytes, how long each page held its shard's loop and
read off it, by the member that answered, and every whole scan that ended in it. No arm id and no
spec's digest from before moved.

## Context

This is the last required row of [W1](../future-work/scans/prerequisites.md#the-benchs-scan-kind-and-scan-event)
that waits on nothing, and one of the three inputs [M22](../future-work/scans/milestones.md#m22-the-harness-and-the-facts)
is closed by, beside the contract (agreed on 2026-10-29) and [X16](../future-work/scans/spikes.md#x16-the-walk-beside-the-foreground)'s
baseline. Every gate of the scans plan is judged by the foreground's tail beside a scan
([W7](../future-work/scans/isolation.md#the-budget)), and before this feature no capture could
hold one: the bench knew no scan, and its events stopped at `device_drain`.

Client scans are M24's. Until then a tablet's rows reach a client one way, the admin walk F87 added
for the lab (`AdminKind::Walk`), which answers a page of one tablet from the copy the asked node
holds, as JSON: each partition's key and live rows and the page's archived bytes, never the rows.
So the scan here is the walk driven as [W6](../future-work/scans/distribution.md) drives a client's
scan, from the client, across every member, and what it measures is what the walk reads and holds
on a server. That is the half of a scan's cost [W7](../future-work/scans/isolation.md)'s budget is
about; the bytes of the rows on the wire are M24's.

F69 had made a kind a schema supplies one query and its answer
([F69](driver-operation-kinds.md#limitations)), so a scan could not be a supplied kind: one operation
here is a page, and one whole scan is thousands of them. The object arms' driver
([F80](object-wire.md)) is the model followed instead: arms of their own, after the others, each
driving a loop of its own on the arm's clock.

## What it does

### The scan

`ScanDriver` (`shoal-loadgen/src/scans.rs`) holds one admin client a member, each with its name and
node id. `scan_once` walks every one of the 4096 tablets once, in an order drawn from the run's seed
so every shard starts working, on `streams` tasks that each take the next tablet nobody took:

- **Where a tablet is read.** The route table built from a member's topology
  (`RouteTable::from_frame`) names the tablet's holders that are up and not quarantined; the driver
  asks its followers first, the one a tablet starts at rotated by the tablet so the tablets spread
  over them, then its leader. A node alone, whose topology places nothing, holds every tablet.
- **A tablet's pages.** Each asks `after` the key the last page ended at and `at_least` the highest
  boundary any earlier page of the tablet was read at, whichever copy answered it, the walk's own
  rule and P21's; the walk is done when a page names no next key.
- **Refusals.** ~~`Shedding` is asked again of the same copy after 2 ms and up to 2 ms more drawn a
  page, so streams do not ask in step;~~ Since [O139](../appendix/optimizations.md#o139-a-scan-driver-asks-a-shed-tablet-again-where-another-tablet-would-be-answered)
  (2026-10-30) a shed stream puts its tablet back on the scan's queue, its cursor, floor and holder
  with it, and takes the next, which another shard may answer; a stream shed twice in a row with no
  page answered between waits 2 ms and up to 2 ms more drawn a stream before it takes another, so a
  member whose shards are all full, or out of its scan budget, is asked no faster than it was. Since
  [O148](../appendix/optimizations.md#o148-a-scan-driver-asked-a-shed-page-again-every-2-ms-beside-pages-paced-for-tens) (2026-10-31) each shed after that doubles the
  wait, up to 64 ms and up to half as much again drawn, until a page of the stream is answered:
  under F128's pace a page takes tens of milliseconds, and a stream asking every 2 ms meanwhile spent
  its members' cpu on sheds. A scan
  ends once no tablet waits and no stream holds one. `Unavailable`, `Timeout`, `Quarantined` and a member not
  reached are asked of the next holder after 200 ms; `StaleTopology` reads the route table again and
  starts from the first holder. Any other refusal fails the scan by its code. A page unanswered for
  a minute of asking again gives the scan up.
- **What is recorded.** Each page in the second it was answered: its latency, from the ask to the
  answer, under the `scan` kind, and its figures in the window's `ScanWindow`. Each wait by what it
  was refused for, with the first message of each, so a capture says why pages waited.

### The scan arm

`--scan-tables Big,Keyword` makes one arm a table, after every object arm, each with every
`--scan-streams` value: `scan/Big/none`, or `scan/Big/c18/none` when the streams are named. An arm
runs whole scans back to back, each drawn its own order, until its time is up, and counts every one
that ended (`scans`, and how long each took, `whole`). A scan refused for good ends the arm with its
reason. Named alone, as object mixes are, scan tables run no default table workload. A scan arm
carries no event but `none` (`EventKind::fits_scans`).

### The `scan` event

`--events none,scan` runs every table arm a second time with a scan beside it: at `--event-at` a
`requested` mark, then one whole scan of `--event-table`, or of the dataset's first table, with the
first `--scan-streams` value or two a shard of every member. The arm is kept running while the scan
is, up to `--event-timeout`, as an admin operation's is; its last page leaves a `done` mark with its
pages, partitions and time, and the arm runs ten seconds past it so the after window has something in
it. The scan's pages are recorded in windows of their own on the arm's clock, which the arm's seconds
take in before they are cut by `cut_background`, from the asking to the last page. The foreground's
`p99_ratio_permille` is unchanged by them: the `scan` kind is present during the scan alone, and the
ratio is taken only over kinds present before and during.

A scan reads and writes nothing, so it takes no node down (`disrupts` is false: an attached cluster
runs it under `--yes-events`) and an arm with it disturbs the cluster only through its own workload's
writes (`read100/b1/scan` leaves the bench's own cluster unreset).

### What a capture keeps

A window summary that saw a scan's pages carries a `scan` section:

| Field | Holds |
| --- | --- |
| `pages`, `partitions`, `rows`, `bytes`, each with a rate a second | What the pages handed out, the bytes being their partitions' archived bytes |
| `resident`, `archived`, `reprobed` | Where the partitions came from, and how many archived records moved under a page |
| `shed`, `waited`, `waits`, `wait_samples` | The pages asked again: shed by a shard reading its fill, or refused for now, by code |
| `longest_unanswered_ms` | The longest any tablet went unanswered while streams asked it, before a page of it was answered or it was given up at a minute; zero from a capture before [Resolved #355](../appendix/resolved/scan-pages-served-in-turn.md) |
| `held`, `read` | How long each page held its shard's loop capturing it, and read off it, as the answers said, every member's |
| `queued`, `paced` | How long each page waited for its capture, from its admission ([F117](scan-class.md)), and how long its reads waited for their device, a part of `read` ([F119](scan-pacing.md)); zero from a node before each |
| `scans`, `whole`, `first_page` | Whole scans that ended, how long each took, and how long each took to its first page |
| `members` | The same pages, partitions, rows, bytes, holds and reads, by the member that answered |

The page latency is the `scan` kind's, in `kinds`, as any kind's is. A window that saw no scan
writes no `scan` section, so a capture of reads and inserts is byte for byte what it was.

## Design choices

**The admin walk, not an engine call.** shoal-loadgen and shoaladm link no engine (`cargo tree |
grep -c glommio` is 0 for both), so `ShoalPool::walk` is out of reach; the admin walk is the one
remote path, and an admin client of every member is what reaches every copy. The members' clients
are shoaladm's, connected as the inventory's admin whoever the bench's own clients connect as
(`Cluster::scan_members`), since a walk reads a table whole and a cluster answers it to an admin
alone.

**Followers first, spread by tablet.** It is how [W6](../future-work/scans/distribution.md#choosing-copies)
has a client's scan choose its copies, so a group's leader keeps its writes and its strong reads to
itself; and X16 is to measure the walk as a client's scan would drive it.

**Every tablet in a seeded order.** Tablets in key order land on a node's shards in whatever order
the placement rule gives them; drawn from the seed, every shard starts working at once, as
ScyllaDB's advice for a full scan says ([W3](../future-work/scans/prior-art.md#scylladb)), and a run
is repeatable.

**Two pages a shard by default.** A shard reads at most two walk pages at once and sheds the rest
(`PAGES_IN_FLIGHT`), so two a shard of every member, from the topology's shard counts, keeps every
shard busy without making shedding the rule. An arm's id names its streams only when the spec does,
so an id is the same on every cluster.

**One window a second, shared by the streams.** The object driver keeps a window a stream a second;
a scan's pages are milliseconds apart and its windows hold two histograms a member, so one window a
second, taken under a lock, holds what a stream's would at a fraction of the memory.

**A scan's histograms are two figures.** A page's hold and read are read to a percent; a member's
two histograms at two significant figures are a seventh of a latency's at three.

**A page is given up by time, not by count.** A shard sheds in microseconds, so a count a restart
outlasts would be spent by sheds in a second; a minute without an answer is a copy that is not
coming back. A page its member's waiter gives up for want of progress, `Timeout`, counts its whole
eleven seconds toward that minute: a scan of `Keyword` beside the cell met one to eight of them a
capture, and once a tablet's own pages met several and a minute ran out, until
[Resolved #355](../appendix/resolved/scan-pages-served-in-turn.md) served a device's pages in turn. Each window keeps how close a tablet came
(`longest_unanswered_ms`).

**The event's pages join the arm's seconds.** Kept apart, the windows before, during and after
could not show the scan's rate beside the foreground's tail, which is what X16 reads; joined, the
`scan` kind is excluded from the foreground's ratio by the rule that excludes every kind absent
before.

## Alternatives rejected

**A supplied operation kind.** One kind is one query ([F69](driver-operation-kinds.md#limitations));
a whole scan is thousands of pages, each a page of one tablet.

**The lab loader's walk** (`tmdb-dataset-loader walk`). It walks every tablet on every member,
four tablets at a time and each chunk awaited before the next, which is a check of copies, not a
scan: it reads every copy and under-drives each shard.

**A scan coordinated by the bench's admin client alone.** One member answers only for the copies it
holds, and a node holding no copy of a tablet refuses it `StaleTopology`; every member's client is
needed for a tablet to be read from any of its holders.

**The event's scan driven inside the arm's main load.** An event is the arm's main load beside
something done to the cluster; a scan is done to it like a repair, on the arm's clock, and cut the
same way.

## Limitations

- **No row reaches the wire.** An admin walk answers keys and counts, so a scan here costs a
  server what a page costs it and a client nothing like what a scan's rows will. M24's frames are
  that half.
- **A page is one tablet.** Today's walk crosses no tablet ([F87](tablet-walk.md#limitations)), so a
  scan of an empty table is 4096 pages, where W4's page carries many tablets of one shard.
- **Admin only.** A walk on a cluster is a `cluster.admins` principal's; the `scan` privilege is
  M24's.
- ~~**The pages are the drivers'**: a scan's pages share a shard's two pages with backfill, copies,
  scrubs and reclamation, and can shed them, until the admission class apart from the drivers'
  ([W1](../future-work/scans/prerequisites.md#an-admission-class-apart-from-the-internal-drivers))
  gives them a count of their own.~~ Since [F116](scan-admission.md) the bench asks its pages as a
  scan's, counted apart from the drivers', and refuses a member whose build does not advertise
  `walk_class`.
- **The event needs an inventory**, as every event does; a node started by hand (`--addr`) runs scan
  arms only.
- **A scan arm carries no other event.** A scan through a node's kill or a rebalance, checked against
  P20, is M25's.
- **The progress screen does not show an event's scan.** Its pages reach the arm's seconds after the
  arm, so the screen's per-second line shows the foreground alone; the capture holds both.
- **A scan names any table the schema serves**, a bucket's generated tables among them, since an
  admin walk does; a client's scan never will (P24).

## Invariants to uphold

- **A tablet's walk never asks below its floor**: every page asks `at_least` the highest boundary
  any earlier page of the tablet was read at, whichever copy answers it.
- **A shed tablet goes back with its whole cursor**, `after`, `floor` and its holder, and its time
  unanswered counts only while a stream asks it: losing `after` would walk it twice, losing `floor`
  would ask below it (O139).
- **A refusal for now is asked again and counted by what it was; any other fails the scan by its
  code**, never silently, and a failed scan ends its arm.
- **`EventKind::Scan` stays appended, after every other event**, and the scan arms' fields are left
  out of a spec that names none: `table_arm_ids_are_unchanged`, `object_arm_ids_are_unchanged` and
  `an_arm_beside_a_scan_keeps_its_id` hold every earlier id and digest to it.
- **A window that saw no scan writes no `scan` section**, so a capture of reads and inserts is what
  it was before this feature.
- **The foreground's ratio is taken over kinds present before and during an event**, so a scan's own
  pages never move it.

## Performance

Nothing on any query path changed: this is the bench's code alone. A scan arm's windows cost a
latency histogram a second, as any kind's does, and two histograms of a scan's figures a member a
second, about 20 KiB each. ~~No lab figure is claimed here: the first scan measured beside a
foreground is [X16](../future-work/scans/spikes.md#x16-the-walk-beside-the-foreground)'s, which
this feature exists to take.~~

**On the lab** ([X16](../future-work/scans/walk-beside-the-foreground.md), 2026-10-29): its scan read the side cluster's 3,000,000 kibibyte rows at
about 84,000 partitions a second with 32 streams, the TMDB keyword rows at 44,000 to 53,000, and was
shed about 35 times for every page of whole tablets it was answered, since a client cannot know a
tablet's shard ([O139](../appendix/optimizations.md#o139-a-scan-driver-asks-a-shed-tablet-again-where-another-tablet-would-be-answered)),
which a shed stream now meets by moving on, its figures taken in the scans plan's next lab window.
The driver, pinned to eight of europa's cpus, peaked at half a core in every measured arm.

## Tests

| Test | Proves |
| --- | --- |
| `an_arm_beside_a_scan_keeps_its_id` (`shoal-loadgen/src/spec.rs`) | `rw50/b1/scan` is a new arm and every other arm's id and the spec's digest are unchanged; scan arms name their streams only when the spec does; the event parses, takes no node down and disturbs only through its workload |
| `scan_pages_add_and_summarize_by_member` (`shoal-loadgen/src/window.rs`) | A scan's pages add and summarize by member, waits by code with their first message, whole scans and first pages; a window of reads writes no `scan` section |
| `a_scan_event_is_cut_at_its_marks` (`shoaladm/src/bench/orchestrate.rs`) | The arm is cut from the scan's asking to its last page, and the scan's own pages never enter the foreground's p99 ratio |
| `a_scan_kind_times_a_whole_scan_page_by_page` (`examples/bench_dataset/tests/scans.rs`) | Against a node in process, a whole scan walks every tablet and hands out every partition the preload wrote once; its windows count its pages, partitions, rows and holds; an arm of scans back to back counts each that ended; a sorted table's rows are each handed out |
| `everything_offered_is_explained` (`shoaladm/src/bench/wizard/form.rs`) | The wizard explains the new event |
| `a_shed_scan_moves_on_and_hands_out_every_partition_once` (`examples/bench_dataset/tests/scans.rs`) | With every scan read of a table held, the streams past each shard's two pages are shed again and again, put their tablets back and take others, and once the hold lifts every partition is handed out once, every shed counted as a page asked again (O139) |
| `a_tablet_put_back_resumes_at_its_cursor`, `the_queue_ends_once_no_walk_is_held`, `a_stream_waits_for_a_walk_another_puts_back` (`shoal-loadgen/src/scans.rs`) | A walk put back is taken again after those that waited, at its cursor, floor and holder; the queue ends only once nothing waits and nothing is held; a stream with nothing to take waits for a walk another puts back (O139) |

W12's three M22 rows are the first, the third and the fourth.

## Related

[W12](../future-work/scans/performance.md) for the plan this delivers the bench's part of;
[F66](dataset-benchmarks.md), [F69](driver-operation-kinds.md) and [F80](object-wire.md) for the
bench it extends; [F87](tablet-walk.md) for the walk it drives.
