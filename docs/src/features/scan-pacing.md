# F119. Table reads paced by a device's idle time

A node now keeps a **pace for every device its tables' archives are on**, shared by every shard on
that device, and **every archived record a scan's page reads is paced by it**: taken from the
device's ceiling, `scans.ceiling`, while no shard has a foreground read in flight on the device, and
from its trickle, `scans.trickle`, as well while one does. Every partition read a get or a write
makes is counted in flight on its device from before it asks for a byte until its last is read. So a
scan reads a device as fast as its ceiling allows while the device has nothing else to read, slows
to its trickle the moment a get needs it, and never stops altogether. Unless named, the ceiling is
100 MiB a second and the trickle 2 MiB, both provisional until X18. A driver's page reads as fast as
it always did.

## Context

This is [W1](../future-work/scans/prerequisites.md#table-reads-paced-by-a-devices-idle-time)'s pace
row, the last of the four rows the scans plan's M23 begins with, built before M23 for scan pages
alone by the user's decision of 2026-10-29.

Table storage paced nothing. Compaction is unthrottled ([Compaction](../storage/compaction.md)), a
backup's copy runs in the default queue, and a walk's reads went as fast as their pages were asked
for ([F87](tablet-walk.md#limitations)). A table's archives share a device with its WAL and with every
get's partition read, so a scan reading at its device's speed would queue them all behind it.

The object store met this for its recovery: a background request waits until its slice has no
foreground request in flight, then draws its bytes from its device's ceiling
(`shoal-core/src/server/objects/executor.rs`, `background_gate`). X12 measured why that is the
shape: on the 970 EVO a scrub held to a fixed 50 MiB/s doubled the foreground's read p99, while one
issued only in the slice's idle time read at 385 MiB/s and left it alone
([S13](../object-storage/isolation.md#io-on-a-slice), [X12](../object-storage/recovery-scrub-rates.md)).
A scan is not a rebuild, so W1 adds one thing: a small trickle a scan always gets, so a device that
is never idle cannot starve it for ever ([W7](../future-work/scans/isolation.md#device)).

## What it does

### A pace a device

`TablePace` (`shoal-core/src/server/scans.rs`) is one device's pace:

- **a ceiling**, `scans.ceiling`, the bytes a second every shard's scans read of the device
  together;
- **a trickle**, `scans.trickle`, the bytes a second they read of it while any shard has a
  foreground read in flight on it;
- **a slot a shard** counting that shard's foreground reads in flight on the device, each slot on a
  cache line of its own.

The ceiling and the trickle are the object store's token bucket, `Ceiling`
(`shoal-core/src/server/devices/mod.rs`), which gains a burst: each holds at most a tenth of a
second's worth (`scans::burst_of`), never less than one credit. The object store's ceilings keep
their second's worth, unchanged.

A pace is made the first time a shard asks for the device a table's archives are on
(`Paces::of`, by the device number of the archive directory, or of the nearest directory above it
that exists), kept in the node's `ScanResources`, and set once on each table's `ArchiveMap` once the
shard's tables are made (`FullArchiveMap::pace_every`). Every shard asking for one device is handed
the one pace. A node that answers no scans makes none.

### Foreground reads, counted

Every partition read the loader makes for a get or a write (`loader::read_partition`,
`shoal-core/src/server/tables/storage/fs/loader.rs`) holds a guard on its shard's slot from before it
asks its device for a byte until its last is read, retries included, on every way out. The guard is
dropped before the read's outcome is sent to the shard, so an outcome waiting for a full inbox is no
read of the device.

### A scan page's reads, paced

Before each archived record a scan's page reads, after its budget's draw
([F118](scan-budget.md)), the page waits until it holds the record's bytes in credits of 64 KiB
(`PagePace::take`). A credit is taken:

| The device | The credit is taken from |
| --- | --- |
| No shard has a foreground read in flight on it | The ceiling alone; with none there, the page waits as long as the ceiling needs |
| A shard has one | The trickle and the ceiling both; the trickle's bytes given back if the ceiling has none. With no trickle to take, the page asks again in a millisecond, by when the read may be done |

A resident partition the page holds whole, and a deleted one's shadow, read nothing and wait for
nothing. The answer says how long the page's reads waited (`WalkAnswer::paced_us`, a part of
`read_us`, left out at zero, which a driver's page always is), and the bench keeps it by member
beside the wait for a capture ([F115](bench-scans.md)).

### Settings

| Setting | Means | Default |
| --- | --- | --- |
| `scans.ceiling` | Bytes a second every shard's scans read of one device together | 100 MiB, provisional until [X18](../future-work/scans/spikes.md#x18-table-reads-paced-by-idle-time) |
| `scans.trickle` | Bytes a second they read of it while a foreground read is in flight on it | 2 MiB, provisional until X18 |

Both are sizes (`50MiB`, `512KiB`). A node that answers scans refuses to start with either at zero,
since a scan reading at none would never end, or with a trickle past its ceiling, since every
trickled byte is drawn from the ceiling too.

## Design choices

**The device's idle time, not the shard's.** W1 and W7 wrote that a scan reads while *its shard* has
no foreground read in flight. Shards whose archives are on one device share its queue: a scan's read
on one shard queues a get's read on another behind it all the same. So the check is every shard's
on the device; on the lab, where a node's archives are on one root, it is the node's.

**Every scan byte draws the ceiling, trickled ones too.** The ceiling then bounds the scans' share
of the device whatever the mix of idle and busy time, which matters because the idle check sees
reads, never a commit's flush. A trickle credit the ceiling cannot match is given back, so neither
bucket loses bytes nothing read.

**A tenth of a second at most in either bucket.** The object store's ceiling holds a second's
worth. A scan that waited out a busy second would then read 100 MiB at the device's speed, and
every flush of the WAL beside it would wait behind that burst; a tenth of a second's worth bounds
it at 10 MiB, which an NVMe device reads in a few milliseconds.

**Credits of 64 KiB.** One lock of the device's bucket a credit, never one a partition: a scan of
kilobyte partitions takes a lock every sixty-four of them. And a page reads at most a credit past
the moment its device turned busy.

**A slot a shard, on a line of its own.** A get's read touches only its own shard's cache line, two
relaxed operations; a scan reads every slot, once a credit. One counter for the device would bounce
its line between cores on every disk read.

**Set on the table's map, after its tables are made.** The loader and a page's reads both hold the
table's `ArchiveMap` already; a pace set there once, as the map's fold is (`set_folder`), reaches both
and changes no table type, no derive and no `D::new`.

**A millisecond's poll, the object store's.** A wait asks the pace again every millisecond while its
device is busy, the object store's `IDLE_POLL`, so a scan resumes at its ceiling within a millisecond
of the device going idle.

## Alternatives rejected

**A fixed rate.** X12 found a fixed rate both too slow when the device is idle and too fast when it
is not ([W7](../future-work/scans/isolation.md#alternatives-rejected)).

**The object store's gate as it is.** A slice that is never idle runs no background there, which a
rebuild can afford, being bounded by the device's idle time elsewhere. A scan on a table that is
never idle would never end; the trickle is W1's answer.

**A wake when a foreground read ends.** Waking the waiting scans from the read's end would put a
cross-thread wake on every get's read, which costs more than the scans' poll and sits on the query's
path.

**Pacing every table read through one bucket, compaction and backups included, now.** By the user's
decision the rows act on a scan's pages alone until M23 moves the drivers; compaction and a backup's
copy may draw on the same pace then ([W1](../future-work/scans/prerequisites.md#table-reads-paced-by-a-devices-idle-time)
lists them as what else gains).

**Sharing a pool device's recovery ceiling** where a table's root is on the same disk. That ceiling
bounds a rebuild, which restores a pool's durability; a scan taking from it could hold a rebuild back.
The two are kept apart, and the limitation is recorded.

## Limitations

- **Only a scan's pages are paced.** A driver's page, an operator's walk, compaction and a backup's
  copy read as fast as they did.
- **The idle check sees reads, not writes.** A commit's flush and a compaction's writes are not
  counted; the ceiling is what bounds a scan's share beside them, and X18 measures the write p99
  beside it ([W7](../future-work/scans/isolation.md#device)).
- **A device is its device number.** A btrfs subvolume or a stack of device-mapper targets can give
  one disk two numbers, and so two paces; and a table root on a pool device's disk is paced apart
  from that device's recovery ceiling.
- **The index pages a page's view reads are not paced**; they are read before its steps, in its read
  task, as F117 left them. M23's plan step reads them.
- **A record looked up again after its archive moved** is read on its first credit.
- **A page's last credit's unused part is not given back**: at most 64 KiB a page.
- **The defaults are provisional**, 100 MiB and 2 MiB a second, until X18 measures them on the 970
  EVO, the Optane and a disk. ~~A deployed node runs them whatever its operator wants, since an
  inventory renders no `scans` section.~~ Since 2026-10-30 an inventory's `scans:` sets them on every
  node it deploys ([the shoaladm page](../operations/shoaladm.md#scan-settings)), which is how X18
  moves them.
- **No figure in a node's stats yet.** A pace's idle and trickled bytes and its foreground reads are
  read in process (`ShoalPool::scan_paces`); a node's counters of scans are M23's.
- **A page waiting on a busy device wakes a thousand times a second**, a few microseconds each.
- **Every page waiting on a device shares its trickle**, and a page's answer has the admin read's
  deadline, 11 s (`ADMIN_TIMEOUT`). With every shard of a six shard node holding its two scan pages
  on one busy device, a page of the default mebibyte gets a twelfth of 2 MiB a second and takes
  about six seconds; one of the 4 MiB a scan's page may ask for would pass the deadline, be answered
  `Timeout` and be asked again. The bench's scan asks the default; a page that ends short at a time
  bound is [M23's](../appendix/todos.md#a-paced-scan-page-that-ends-short-at-a-time-bound).

## Invariants to uphold

- **Every byte a scan's page reads of an archive is taken from its device's ceiling before it is
  read, and from its trickle too while any shard has a foreground read in flight on the device.**
- **One pace a device**, shared by every shard of the node whose tables' archives are on it.
- **A foreground read is counted from before it asks its device for a byte until its last is read**,
  on every way out, and never while its outcome waits to be sent.
- **A get's read touches only its own shard's slot.**
- **While scans are answered the trickle is never zero and never past the ceiling.**
- **A driver's page is never paced** until M23 decides how the drivers read.

## Performance

On a query's path: a get or a write that reads a partition from its archive makes two relaxed atomic
operations on its own shard's cache line, and one load of the test hold's flag. A read of a resident
partition, and every query on a node that answers no scans, does nothing new.

On a scan's: a lock of its device's bucket every 64 KiB, and a millisecond's poll while it waits.
`a_scan_reads_only_in_idle_time_or_its_trickle`, on europa: beside a get's read held in flight, a
scan of 832 KiB of archived partitions took 1.51 s, 1.48 s of it paced, which is its trickle of
512 KiB a second past the tenth of a second it holds; with the device idle, the same scan took
22 ms. What the pacing costs and buys a foreground on the lab is measured with the other rows in
X16's window ([the plan's lab window](../future-work/scans/spikes.md#x16-the-walk-beside-the-foreground)).
~~No figure is claimed here before it.~~

**On the lab** ([X16](../future-work/scans/walk-beside-the-foreground.md), 2026-10-29): at the provisional defaults the pace never bound beside the
reference cell. A scan of an archived table read about 28 MiB a second a node, under a third of the
100 MiB ceiling, and the resident cell kept its devices' reads idle, so a page waited for its device
5.9 ms at p99 and the cell's tails were the walk's own: read p99 1.12 times its before beside the
scan, insert p99 1.63 times, against the walk's 1.13 and 1.60. The insert tail is the scan's reads
beside the WAL's flushes, which the idle check does not see; which ceiling holds it, and whether a
flush should count, is [X18](../future-work/scans/spikes.md#x18-table-reads-paced-by-idle-time)'s.

## Tests

| Test | Proves |
| --- | --- |
| `a_scan_reads_only_in_idle_time_or_its_trickle` (`shoal/tests/scan_admission.rs`) | W7's M23 row, proved now: beside a get's read held in flight, a scan of a tablet's archived partitions reads on the trickle alone, no faster than the trickle and its burst and no sooner than they allow, and says it waited; once the read is answered nothing counts it, and the same scan reads in idle time with no wait; and a read of a lost device stays counted only until it fails. With the gate removed it fails, "the scan trickled only 0 bytes" |
| `a_devices_pace_is_shared_by_its_shards` (`shoal-core/src/server/scans.rs`) | A read on any shard's slot makes the device busy for every shard's scans; an idle credit is the ceiling's alone, a busy one the trickle's and the ceiling's, the trickle's given back when the ceiling has none |
| `every_shard_on_a_device_shares_its_pace` (same) | Every shard asking for one device is handed one pace, a path not made yet its parent's |
| `a_page_waits_for_its_trickle_while_a_read_is_in_flight` (same) | A page's credits come at the trickle while a read is in flight, at once while none is, and only a page that waited says so |
| `a_ceiling_holds_no_more_than_its_burst` (`shoal-core/src/server/devices/mod.rs`) | A bucket made with less than a second's worth holds no more, a request past it takes all of it, and a refund is held again up to it; `a_ceiling_lets_its_rate_through_and_no_more` holds the object store's ceiling unchanged |
| `a_scans_pace_is_read_as_sizes_and_refused_where_it_reads_nothing` (`shoal-core/src/server/conf.rs`) | The defaults, a pace read as sizes, and a pace of nothing or a trickle past its ceiling refused by name |
| `scan_pages_add_and_summarize_by_member` (`shoal-loadgen/src/window.rs`) | The bench keeps a page's paced time by member and summarizes it |

`faults::hold_partition_reads(table, held)` holds a table's partition reads in flight on their
device, counted as `faults::partition_reads_waiting`, so a test can keep a device busy as long as it
likes.

## Related

[W7](../future-work/scans/isolation.md#device) for the design;
[S13](../object-storage/isolation.md#io-on-a-slice) and [X12](../object-storage/recovery-scrub-rates.md)
for the evidence; [F88](recovery-and-moves.md) for the object store's ceiling it shares a bucket with;
[F117](scan-class.md) and [F118](scan-budget.md) for the queue and the budget a scan's page reads
under; [X18](../future-work/scans/spikes.md#x18-table-reads-paced-by-idle-time) for the measurement
that sets its defaults.
