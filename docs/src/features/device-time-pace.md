# F128. A scan's pace in its device's time

## Context

[F119](scan-pacing.md) paced a scan's archived reads in bytes: a ceiling of 100 MiB a second a
device, and a trickle of 2 MiB a second while any shard had a foreground read in flight on it. X18
measured that pace beside the reference cell and found the default too high: on the 970 EVO only 32
MiB a second held the cell's insert p99
([X18](../future-work/scans/table-reads-paced.md#what-it-says-for-the-design)). A byte is a different
share of each device's time: a mebibyte is 0.65 ms of the Optane, 2.3 ms of a 970 EVO and about
125 ms of a disk. The idle check saw too little to engage beside a warm foreground, and two runs in
flight a page moved the read tail less than sixteen. And a paced page's waits for credit doubled its
cost a partition ([O143](../appendix/optimizations.md#o143-a-paced-scan-pages-waits-for-credit-double-its-cost-a-partition)),
and a page its device held back could pass its answer's deadline
([the todo M23 owned](../appendix/todos.md#a-paced-scan-page-that-ends-short-at-a-time-bound)).
M23's plan decided, with the user on 2026-10-31, on four things: a ceiling in device time (a share
of each device's busy time a second, learned from the scan's own reads), the byte ceiling kept as its
bound, a device-wide bound on a scan's reads in flight, and a default share near 75 ms a second
([the plan](../future-work/scans/milestones.md#m23-the-walk-rebuilt-for-load)). This is M23's sixth
step.

## What it does

**A share of each device's busy time.** Every device's pace (`TablePace`) holds the busy time a
second that its node's scans may take of it together (`scans.device_time`, 75 ms unless set). It
holds a tenth of that at once. A scan's read takes its device time from the share before it is
issued, at what a byte of the device costs. On the lab's devices that is about 32 MiB a second of a
970 EVO, 115 MiB of the Optane (which the byte ceiling holds to 100) and under one of a disk.

**What a byte costs is learned from the scans' own reads.** A device is busy for its scans while any
scan read is in flight on it. Reads in flight together are counted once, as a device counts its own
busy time. Each window of a mebibyte or more the scans read sets what a byte cost (`TablePace::cost`):
the first window sets it outright, and every later window moves it a quarter of the way toward its
own figure. That figure is counted at most four times the cost it moves (`LEARN_RISE`), so one window
raises the cost at most 1.75 times and can lower it a quarter of the way. Until the first window is
read, the byte ceiling alone paces.

**A bound on a device's scan reads in flight.** Every scan read holds a place among its device's
(`scans.device_reads`, ~~four~~ sixteen unless set since [O146](../appendix/optimizations.md#o146-four-scan-reads-a-device-held-a-scan-alone-to-two-thirds-of-its-rate)) from before it is issued until its last byte is read
(`TablePace::read`, the `ScanRead` guard). That covers the reader's runs and chains, the merge's
reads alone, and a fold's archive read. A read with no room looks again every quarter of a
millisecond. ~~A run's place covers its archive's open and release too.~~ Since
[item 346](../appendix/resolved/paced-reads-alone.md) a run's place covers its read alone.

**A wave's reads paced a group at a time** ([item 346](../appendix/resolved/paced-reads-alone.md)).
A page's reader takes the device time and bytes of as many of its next reads as its device holds in
flight, within what one take holds (`group_of`), and then issues them together, so they overlap on
the device and their busy time counts once. ~~Each read took its own device time before it was
issued.~~ Paced alone, once the share bound each read waited for its own credit and went out alone,
so the busy time counted every read's whole latency, and a 970 EVO's mebibyte was learned at 18.4 ms
where X18 measured 2.3.

**A read taken in as few asks as its buckets allow.** A read's take (`PagePace::take`) is three
things, taken all or none: its device time from the share, its bytes from the ceiling, and, while a
foreground read is in flight, its bytes from the trickle too. It is taken in chunks every bucket
holds whole. The chunk and its time are sized from one reading of the cost (`TablePace::next_chunk`),
again at every ask, and most reads take one chunk. Each chunk waits exactly until it is due. Two
cases ask again sooner:

- A trickle that a foreground read holds is asked again a millisecond later, since the read may end
  sooner.
- A chunk sized while the device was idle, and taken once a foreground read began, is refused with
  nothing taken and sized again for the trickle.

F119's loop of 64 KiB credits, and its millisecond floor on every wait, are gone.

**A page its device holds back ends short.** The pace slows a page, and nothing used to bound how
long a page took. A shard waiting on a page's answer gives up on it at 11 s (`Timeout`), but the
page reads on, and its scan asks for the same page again. On a disk the default share gives a
node's scans about 0.6 MiB a second, so ten pages of a mebibyte in flight on one node take about
seventeen seconds each: every one would be given up on and read again for nothing. So since this
step:

- **A wave holds at most what its device gives its scans in a second** (`TablePace::second_bytes`):
  the ceiling, the trickle while a foreground read is in flight, and the share at what a byte has
  shown it costs. A wave holds at least one partition.
- **A page that has waited a second for its device** (`PACED_BOUND`, the merge's waits and its
  readers' together) **and holds a partition ends short** at the last partition it handed out. It
  checks before it plans another wave, reads a partition alone, or encodes a planned one, once every
  read its last wave paid for is handed out (`PageReads::holds_reads`), and its next page goes on
  from there.
- **The shard waiting on a page gives up only once the page has gone a whole deadline without
  progress** (`answered_while_moving`, `WalkCall::progress`). A chunk taken, a partition handed out
  and an encode step each count as progress.

The byte ceiling and the trickle are F119's, unchanged, and the share never lifts either. The
ceiling bounds the scans whatever a byte is learned to cost, and the trickle bounds them while a
foreground read is in flight. A node's scan figures carry its scans' busy time on their devices
(`ScanFigures::busy_us`), so a capture can show the share a scan took. An inventory's `scans:` passes
both new settings through to every node's file as written
([the shoaladm page](../operations/shoaladm.md#scan-settings)).

## Design choices

- **The share learned, never set a device at a time.** What a byte costs depends on the device, its
  filesystem, and the runs and depth a scan reads it at. Learned from the scan's own reads, the cost
  is what those reads cost on that device, and an operator names one time a second for every device.
- **Busy time as a device counts it.** A device is busy while any read is in flight on it, however
  many there are, so reads in flight together spend the share once. A page's runs in flight make the
  most of it.
- **A window of a mebibyte, its rise bounded.** A device's time a read is mostly its latency for a
  small read, and is stretched by whatever else the device or the scan's executor is doing, so one
  read says little of what a byte costs. A window of many reads says what they cost together. The
  bound on a window's rise keeps one window slowed by a stall elsewhere from throttling the scan for
  minutes, while the cost still climbs against a device that stays slower.
- **The byte ceiling stays the bound.** A cost learned on a cold device, or beside a foreground that
  slowed every read, can be wrong either way, and the ceiling bounds the scans whatever the cost
  says.
- **A group a take, never a read alone.** A read paced alone goes out alone once the share binds,
  so the share would be spent on one read's latency after another, the most a device can be shown
  to cost a byte. A group of the reads the device holds in flight spends it as the device spends its
  time. A group stays within one take, so on a disk, one take of which holds about a read, a group
  is a read ([item 346](../appendix/resolved/paced-reads-alone.md)).
- ~~**Four reads in flight a device.** X18's depth supplement found that fewer of a scan's reads in
  flight moved the read tail less. At 75 ms a second the scans keep a read in flight only part of the
  time, so the bound clips their bursts.~~ **Sixteen reads in flight a device**, a page's sixteen runs
  ([O146](../appendix/optimizations.md#o146-four-scan-reads-a-device-held-a-scan-alone-to-two-thirds-of-its-rate)). Once its reads went out a group at a time, a group
  could only overlap as far as the bound, and at four a 970 EVO's share bought a scan alone two
  thirds of what sixteen does. It is also the most scan reads a foreground read can find queued
  ahead of it.
- **Chunks within every burst, never owed.** The first form of this step took a read whole past a
  bucket's burst and owed the rest to the reads after it. Beside a foreground read it read past the
  trickle's bound, which F119's row caught. A chunk each bucket holds whole keeps every bucket's
  bound, and so does sizing it again at every ask.
- **Exact waits.** A wait is what the take is short of, divided by the bucket's rate, so a paced read
  wakes once it is due rather than polling.
- **A wave cut to a second of its device, never a read cut to the pace.** A run smaller than its
  gaps allow costs a disk more a byte, since its latency is spread over fewer bytes. So a pace that
  cut reads would learn a higher cost, cut them smaller again, and end with a scan of single
  records. A wave bounds how far ahead a page reads, never how a run is cut, and a second of the
  share is 75 ms of the device's time, many reads' latency, so the runs a wave holds stay whole.
- **A page ends short between waves.** A wave's reads are paced and issued ahead of the merge, so
  ending a page inside one would throw away reads already paid for. Every partition of a wave is
  handed out before the page ends for its device, a partition read alone or encoded between them
  included.
- **The bound counts the page's waits for its device, not its time.** A page held by a test, or by
  its loop's work under load, hands out what it always did. Only the pace ends a page short.
- **A deadline from the page's last progress.** A page its device holds back is slow by design. A
  deadline counted from the ask gives such a page up while it reads on, and its scan asks for it
  again. A page that stopped is still given up on at 11 s.

## Alternatives rejected

- **A byte ceiling of 32 MiB a second** (the plan's second choice). It is X18's figure for one kind
  of device: 75 ms a second of a 970 EVO, 21 ms of the Optane, and more than a disk's whole time.
- **A byte ceiling for each class of device, set by the operator**: an operator would have to know
  what each device costs a byte, at the runs a scan reads.
- **The device's own busy time from `/proc/diskstats`**: it counts every reader, the WAL's flushes
  and every get, so a scan would be charged for its foreground's time. It also needs a path to name
  a block device.
- **A cost from each read's latency alone**: reads in flight together overlap, and summing their
  latencies counts the device's time more than once.
- **A window closed by time, as this step's first form had it**: one small read slowed by a stall
  set every byte's cost. A read of 4 KiB in flight 60 ms makes a mebibyte cost 15 s, so the scan's
  next mebibyte would wait over three minutes for its share.
- **Whole reads owed past a burst**, described above.
- **Reads cut to what the pace gives a page**: the rising cost described above.
- **The share divided among a device's pages in flight**: at ten pages it is 7.5 ms a second each,
  less than one disk read's latency, which cuts reads to single records the same way.
- **A longer deadline for a scan's page**: a page that stopped would hold its scan as long, and the
  bench gives a tablet up once it has gone a minute without an answer.
- **The share never holding a scan below its trickle**: a disk's scans would read 2 MiB a second,
  250 ms of its time, past the share the plan chose.
- **One pacer a device handing out credit**, O143's other candidate: a task for each device, when
  exact waits already ask once a chunk.

## Limitations

- **A cost learned beside a busy foreground is higher**, so the scan slows there, which is what the
  share is for. A device kept busy can hold a scan below its trickle, as a disk's share always does
  (0.6 MiB a second at 75 ms). A scan still never stops while its device answers.
- **A scan alone is held to its share too.** The idle check cannot tell a node with no foreground
  from one whose foreground is warm, where X18 found the cell's tails came from. So on a 970 EVO a
  scan alone reads about 32 MiB a second, where F119's reached the 100 MiB ceiling, and W12's rate
  budget is judged against the share's rate
  ([W12](../future-work/scans/performance.md#the-budgets)).
- **A scan read is busy until its last byte is read and verified**, so the cost counts the read's
  checksums and the wait for its task to be run again too. It is a little high on a busy node.
- **The cost is of the scan's own reads**, at their run sizes: a scan of scattered records costs more
  a byte than one of runs, and its share buys it fewer bytes.
- **A read waiting for room looks again every quarter of a millisecond.**
- **A page answers only once its first wave is read.** A page its device holds back past its bound
  still waits for that wave. On a disk at the default share, with ten pages in flight on one node,
  that is about ten seconds, each wave 75 ms of the disk's time. The bench gives a tablet up once it
  has gone a minute without an answer, so a disk holds about sixty pages in flight before that.
- **The idle check is F119's**: it counts foreground reads in flight only, and sees neither flushes
  nor writes.
- ~~**Not yet judged on the lab.**~~ **Judged at M23's exit** (2026-10-31,
  [its record](../future-work/scans/m23-exit.md)): beside a whole scan of `Big` the cell's read p99
  rose 7%, inside W7's budget, so the plan's third choice is not needed for it. The insert clause by
  W7's ratio is open, the cell's own workload changing inside an arm this long. **The share buys a
  970 EVO about 20 MiB a second alone, not 32**: a MiB cost a scan alone 3.81 ms of the device's time
  at sixteen reads in flight, where X18's 2.3 ms was a scan at 200 MiB a second.
- **Two hooks are only for tests.** `ReadVerb::Evictions` reads a shard's eviction passes, and
  `faults::override_node_memory` moves a running node's memory, for the resident-eviction row below.

## Invariants to uphold

- **Before a scan read is issued, it takes its device time from its share and its bytes from the
  ceiling, and from the trickle as well while a foreground read is in flight.**
- **No take holds more than a bucket's burst**, so no single read passes a bucket's bound. A chunk
  and its time are sized from one reading of the cost, again at every ask.
- **Only a window of a mebibyte or more moves the cost, and one window raises it at most 1.75 times.**
- **At most `scans.device_reads` scan reads are in flight on a device**, and the device is busy for
  the scans while any one is.
- **A paced read is never issued alone while the reads after it fit the same take and its device
  holds them in flight** ([item 346](../appendix/resolved/paced-reads-alone.md)).
- **A wave holds at least one partition, and at most a second of its device once it holds one.**
- **A page ends short for its device only once every read a wave paid for is handed out, and only
  once it holds a partition**, so no paced read is thrown away and every page makes progress.
- **A page's waiter gives it up only after a whole deadline without progress**, and every chunk
  taken, partition handed out and encode step counts as progress.
- **A read holds its place from before it is issued until its last byte is read**, on every way out.
- **The byte ceiling bounds the share whatever a byte is learned to cost.**

## Performance

Measured in process. Two reads in flight together for 2 ms, a mebibyte between them, learn a cost of
2 ms a mebibyte, not 3. A share of 10 ms a second then holds the scans to five mebibytes a second. A
mebibyte that the ceiling holds is taken in one ask, where F119's credits asked sixteen times. A page
on a device that gives its scans 64 KiB a second ends short at thirty two partitions in about a
second, where it would have read all sixty four in three. ~~The lab figures, beside the reference cell,
come at M23's exit.~~ At M23's exit (2026-10-31): a scan of `Big` alone read 19.8 MiB a second of each Zen1 host's 970
EVO, its share binding at 3.81 ms a MiB, 3.47 after an hour's churn and 1.95 at 40 MiB segments;
europa's Optane read 19 MiB a second at 0.82 ms, its share not binding; beside the cell, 5.2 MiB a
second of a 970 EVO at 11.1 ms a MiB, the device's own writes stretching every read the share counts.

## Tests

| Test | Proves |
| --- | --- |
| `a_scans_device_time_is_learned_from_its_reads` (`shoal-core/src/server/scans.rs`) | A device's cost a byte is its scans' busy time over their bytes, reads in flight together counted once, and the share then bounds their bytes a second |
| `a_slow_window_or_a_lone_read_moves_a_devices_cost_little` (same) | A small read alone, however slow, sets no cost; a window three seconds a mebibyte raises it 1.75 times; a fast one lowers it a quarter of the way. Without either rule it fails |
| `a_chunk_sized_idle_is_sized_again_once_its_device_is_busy` (same) | A chunk sized for an idle device and taken once a foreground read began is refused with nothing taken, sized again for the trickle, and its bytes given back to the ceiling. Without the refusal it reads past the trickle |
| `a_page_its_device_holds_back_ends_short_at_its_bound` (`shoal-core/src/server/tables/walk/reads.rs`) | On a device that gives its scans 64 KiB a second, a page plans waves of sixteen records. It ends short at thirty two once paced past its bound, inside three seconds, where its bounds allow all sixty four. With the bound or the wave's limit taken out it reads all sixty four |
| `a_waiter_gives_up_only_on_a_page_that_stopped` (`shoal-core/src/server/shard/walk.rs`) | A page that moves every 20 ms is waited for past four deadlines of 50 ms and answered; one that never moves is given up on at its first. Waiting on the deadline alone, the first is given up on |
| `a_device_holds_at_most_its_scan_reads_in_flight` (`shoal-core/src/server/scans.rs`) | No more than the bound of scan reads is in flight on a device, and one ending makes room |
| `a_paced_pages_reads_go_to_their_device_together` (`shoal-core/src/server/tables/walk/reads.rs`) | [Item 346](../appendix/resolved/paced-reads-alone.md)'s row: a page its share holds back from its first read sends its eight reads as two groups of four in flight together. Paced one at a time, the most ever in flight is one |
| `a_paced_read_asks_its_device_once_a_wait` (same) | O143's waits cut: a read its buckets hold asks once, and a waiting one asks again only once it is due |
| `a_page_waits_for_its_trickle_while_a_read_is_in_flight`, `a_devices_pace_is_shared_by_its_shards` (same) | F119's rows on the chunked take: the trickle bounds a busy device's reads, and a read past its burst is taken a chunk at a time |
| `a_scans_share_of_a_device_is_read_as_a_time_and_refused_where_it_reads_nothing` (`shoal-core/src/server/conf.rs`) | The defaults, and a share read as a duration. A share of nothing, a share of more than a second a second, and no read in flight are each refused by name |
| `a_scan_reads_only_in_idle_time_or_its_trickle` (`shoal/tests/scan_admission.rs`) | F119's row: beside a foreground read held in flight, a whole scan reads on its trickle alone, no faster than it |
| `a_scans_buffers_are_drawn_from_its_budget_and_shed_past_it` (same) | F118's row, kept |
| `a_scan_never_causes_a_resident_eviction` (`shoal/tests/scan_encodes.rs`) | W7's row: with the node's memory set 24 MiB above the process, a full scan of a 48 MiB archived table begins no eviction pass by the process's memory. With the memory set below the process, it counts them |

## Related

- [X18](../future-work/scans/table-reads-paced.md), which measured what a byte of each device costs
  and found the default too high
- [F119, a scan's reads paced by idle time](scan-pacing.md), whose ceiling and trickle bound this
- [O143](../appendix/optimizations.md#o143-a-paced-scan-pages-waits-for-credit-double-its-cost-a-partition),
  whose waits this cuts
- [W7's device pace](../future-work/scans/isolation.md#device) and
  [Q34](../future-work/scans/contract.md#questions-to-answer)
