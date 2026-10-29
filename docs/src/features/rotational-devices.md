# F91 — M19, rotational devices

## Context

[M19](../object-storage/milestones.md#m19-rotational-devices) is the object store's ninth gate. R16
asks that a rotational disk and an SSD run the same code, and [S6](../object-storage/device-store.md#what-a-rotational-device-changes)
says what differs is which of their costs dominate. [X7](../object-storage/device-store-hdd.md)
measured those costs on the lab's disks on 2026-10-06, and
[Q23](../object-storage/contract.md#q23-what-a-rotational-device-needs-2026-10-06) recorded what a
disk needs:
- its write cache off;
- its journal on an SSD of its node;
- an executor never shared with an SSD's slice;
- applies a batch at a time, with no offset order;
- whole chunks of 4 MiB or more;
- XFS alone.

[X12](../object-storage/recovery-scrub-rates.md) added a second loss survived while a disk rebuilds.
Until this gate a node only warned that "a rotational device's rules ... are not enforced before
M19" ([F85](devices-and-pools.md#limitations)). Both of the gate's prerequisites were done: Q23 was
recorded, and the lab's disks were fitted on 2026-10-06, one a host.

Eight decisions were taken with the user on 2026-10-16, before anything was built:

- **A whole chunk is staged on the disk**, in its pool's file, as on an SSD. The disk's own flush
  runs first, then its record is synced on the SSD journal. A part of a chunk and a fold are staged
  in the SSD journal alone. Whole chunks in the journal would write every byte twice, and fill a
  256 MiB journal with 64 chunks.
- **A batch of applies is bounded at 100 ms** unless the device says otherwise (`apply_batch`).
- **A disk reads the units a range covers in one I/O**, never the whole chunk for every read. A
  whole-object read still reads whole chunks, since the shard asks for 4 MiB ranges a chunk at a
  time. Whole chunks always would read each chunk four times under `ObjectFile`'s 1 MiB read-ahead.
- **The lab's cluster carries both pools**: `fast` on its SSDs and `bulk` on its disks, titan and
  hyperion giving up a shard for the disk's executor.
- **A lost journal: the disk is served, and scrubbed at once.** A journal directory found empty or
  someone else's at a start gets a fresh journal, and the disk serves with a warning that names the
  loss. The node then asks a light scrub of every pool its class serves. Only what the journal held
  was lost, and the scrub finds that stale; failing the disk would rebuild days of chunks that are
  intact.
- **A rotational device that breaks a rule refuses the node's start**, naming the device and the
  rule, as every refusal of the claim does.
- **`journal` names the SSD directory**, as S4 drew it, and F85's size became `journal_size`.
- **A pool is rotational by an explicit key**, `media: rotational`, beside its class, which stays a
  label.

The other decisions were mine, and are under **Design choices** with their reasons.

## What it does

### Configuration

A rotational device names its journal; a pool of disks says so. Wire version 13 is the rotational
wire: below it a node observes its devices with no journal, so every device is a `device` domain of
its own, as a build before it places them. `shoaladm` bootstraps at 13.

```yaml
storage:
  devices:
    - {path: /hdd/shoal-tmdb-bulk, class: hdd, journal: /xfs/shoal-tmdb-journal}
    - {path: /xfs/shoal-tmdb-pool, class: ssd}
  pools:
    bulk:
      class: hdd
      media: rotational            # the disks' rules and defaults
      redundancy: {replicas: 3}
      failure_domain: host
resources:
  object_cpus: [1, 7]              # a disk's slice never shares an SSD's executor
```

| Device key | Meaning | Default |
| --- | --- | --- |
| `journal` | A directory on an SSD of the node its slices journal in. A rotational device must name one; any other may | under each slice |
| `journal_size` | A slice's journal's bytes, four segments (F85's `journal`) | 256 MiB |
| `write_cache` | `on` starts a rotational device whose cache writes back, warned; `off` says what the kernel must | — |
| `apply_batch` | The longest a batch of a rotational device's applies takes, `100ms` or `1s` | 100 ms |

| Pool key | Meaning | Default |
| --- | --- | --- |
| `media` | `rotational` for disks, `solid_state` otherwise | `solid_state` |
| `accept_rebuild_exposure` | Whether a rotational pool that survives fewer than two losses starts, warned | `false` |

### The claim

A device is rotational when the kernel's `queue/rotational` for its disk reads `1`, through a
device mapper's slaves and a partition's parent, as F85's probe reads it. The class is a label and
decides nothing: a disk labelled `ssd` keeps a disk's rules and is warned that its class disagrees.
Each of these refuses the node's start, by the device's name (`DeviceRefused`):
- **a filesystem other than XFS** on a rotational device;
- **more than one slice** on a rotational device;
- **no journal named**;
- **a write cache that does not write through**, as the kernel's `queue/write_cache` says, unless
  the device says `write_cache: on`. A kernel that says nothing counts as writing back. With
  `write_cache: on` the device starts, and is warned that a sync may be acknowledged before its
  platter holds the block.

The warning that M19's rules were not enforced is gone.

**A journal is claimed before any device**, as a device is:
- its directory must exist, and is never created;
- it is marked with `shoal-journal.json`, holding its id, node and cluster, and locked;
- it is refused on tmpfs or btrfs, on a disk the kernel calls rotational, in or around a root or a
  device, or on a filesystem another journal of the node is on.

The last rule makes a journal's id name one SSD, which the failure domain below relies on. A journal
the claim cannot reach fails a known device and refuses a new one, as a device's own path does.

Under a journal, each device has a directory named by its id, and each slice one beneath it,
`<journal>/<device>/slice-N/`, marked `shoal-slice-journal.json` with the slice's id and holding its
four segments. A record's checksum is seeded with its slice's id, so a slice that read another's
directory would find no record and say nothing; the marker refuses it by name. The device's marker
records where its journal is (`journal: {id, path}`).

**A journal moved, or lost.** The claim reads the device's marker against the configuration:
- **the old journal is still where the marker says** and the configuration names another: that is a
  move, refused by name while any slice holds a chunk. With none held, the old directory goes and
  the new one is made;
- **the old journal is missing, empty or someone else's**, or a slice's directory under its own
  journal was emptied while the slice holds chunks: the journal was lost. The device is claimed with
  a fresh journal, carries a warning saying what was lost, and serves;
- **a device found empty and claimed new**, a replaced disk, has its predecessor's journal directory
  removed.

### A lost journal is scrubbed at once

A device that lost its journal lost every write it had staged and not applied, and nothing else: its
chunks are as they were, and the rows still name the lost writes' labels at its position. A read that
meets such a chunk is answered with the label it holds, and goes to another holder (S9). A light
scrub lists every holder's labels against the rows and marks a stale position, which F88 rebuilds.
So the node asks for one at once rather than waiting out the daily pass:
1. at its first report, once the cluster scrubs (wire 11);
2. for every committed pool of a class a device that lost its journal serves;
3. it proposes the operator's `ScrubNow` of a light scrub, asked again each second until it commits.

A standalone node, which scrubs nothing (F89), only warns.

### The failure domain of a shared journal

Devices that journal on one SSD lose their staged writes together, so placement counts them as one
`device` domain (S5):
- `DeviceFacts` and the pool map's `Era` carry the journal's id;
- under a `device` failure domain a device's domain is `journal:<id>` when it journals elsewhere, and
  `device:<id>` otherwise;
- a change of journal begins a new era and moves where the device's chunks belong;
- under `host`, `rack` or `zone` nothing changes, since a journal is on its device's host.

The control leader commits a journal only once wire 13 is active, and a node proposes its devices
again when a journal, the kernel's flag or the cache changes.

### Executors

A rotational device's slice never shares an executor with a solid state device's (S13, X7):
- every disk's slice is dealt to the node's last object executor, and every SSD's round the others;
- the split is logged at INFO by device, never by executor number;
- a put's pipeline, the code's arithmetic, and a request for another node's slice sent from a
  thread that is not an executor are dealt round the executors no disk is on;
- a node serving both kinds with fewer than two object cpus is refused, by name
  (`ObjectCpuRefused`), at its start and at `shoaladm`'s claim.

Disks share their one executor among themselves, which X7 could not judge with one disk a host
(**Limitations**).

### Applies, a batch at a time

**Every slice's applies run below its foreground**, as S13 orders them:
- an apply or a discard, sent by a writer, a decide or over a lane, runs on its executor's apply queue
  (500 shares, under the foreground's 1,000);
- it is never counted as foreground, which F85 to F90 did;
- a slice counts each apply while it runs (`applying`);
- a rebuild's or a move's request waits for no foreground request and no apply in flight, and a
  scrub's waits for those and for recovery too.

On an SSD an apply still runs as it arrives.

**A rotational slice's applies wait for their batch** (`store/applier.rs`). `Slice::apply` hands each
to the slice's applier, so a writer's, a decide's, a fold's and a rebuild's apply are all batched.
The applier's loop:
1. take every apply sent; one sent twice runs twice, and the second finds its chunk at the label;
2. wait until the slice has no foreground request in flight, for at most one bound;
3. take as many as the bound holds at what an apply was reckoned to take, a reckoning that starts at
   10 ms, X7's seek and revolution; at least one and at most 128, X7's largest batch, and capped
   at a quarter of the executor's budget;
4. issue every one of them at once on the apply queue, each its slice's own apply under its chunk's
   lock, and wait for all;
5. update the reckoning by what the batch took an apply, and go round again.

It never waits to fill a batch: an idle disk applies at once, and a busy one gathers its next batch
while the last is in flight. So a read waits for the batch in flight when it arrives and no other,
and an apply waits at most one bound a batch. A closed slice answers what it never ran with an
error, and its records stay for the next start's decide.

The block layer and the disk's queue order a batch in flight, which X7 found kept its offer where
one at a time in offset order bought nothing. The slice keeps figures a test reads:
- the batches it issued, and the most applies one batch took;
- the most applies in flight at once, and the longest batch;
- the most batches one read waited behind.

### Reads

A read of a range on a rotational slice reads every unit the range covers from its chunk in one
span, the span's 1 MiB pieces issued together (~~`read_span`~~ `read_pieces` since F105). ~~Units a
staged record or a kept previous state hold are laid over it in place, and every unit is verified. A
solid state slice reads a unit at a time, as before.~~ Since [F105](store-reads-in-place.md) a solid
state slice reads a range the same way, and every unit is verified where the device put it. A range
the file gives whole is answered from those buffers; one a staged record or a kept previous state
gives any of is laid out once, in a buffer of its own. The span is the range's units whole, inside
the draw a read already makes.

### The journal elsewhere and the shared flush

Under `apply_sync: shared` a whole chunk's stage was made durable by its record's sync, which flushes
the device only while the journal is on it. With the journal on an SSD that flush is the SSD's, so a
whole stage on a slice whose journal is elsewhere joins its own device's shared flush, the `flush`
file F85 kept on the chunk's device for this, before its record is synced. A part of a chunk is
journalled alone and touches the disk with no write and no sync. It still reads the chunk's 4 KiB
header from the disk, to know the label it stands on. Applies, renames and directory syncs stay on
the disk, and a start recovers a record from wherever its journal is: unit records read their bytes
from the SSD and write them to the disk, and whole records name files in the disk's pool.

**A journal is never made again over a record.** A journal's segments are opened at the length they
have, and a start whose recovery leaves the journal holding no record makes it again at the length
its device asks. Until then it keeps its length, and says so. Before this gate a change of the size
zeroed every segment, records and all ([Resolved #251](../appendix/resolved/journal-resized-over-records.md)).

### Pools

A pool that says `media: rotational`:
- **cuts its stripe into chunks of 4 MiB by default**, `k` chunks of them: 4 MiB for copies,
  16 MiB at 4+2;
- **refuses a stripe whose chunk is smaller**, naming the stripe of 4 MiB chunks;
- **refuses a redundancy that survives fewer than two losses** - two copies, 2+1, one copy - unless
  `accept_rebuild_exposure: true`, which starts the pool and names the exposure in a warning.

X12 found a disk rebuilds at 22 MiB/s at the pace its foreground tolerated, so a 16 TiB disk is more
than eight days onto one destination. The refusal is the bootstrapper's, at the check that commits
policy, and a standalone node's at its start. A pool's warnings are logged at every start and carried
in its readiness (`PoolReadiness::warnings`), which also names the devices whose kernel says other
than the pool's media.

A device's recovery ceiling is chosen by the kernel's word too: 256 MiB/s on a device it calls solid
state, 64 MiB/s on a disk. ~~By the class: `ssd` exactly got the first, and any other class, an
`nvme` among them, the second.~~

### Deploying a disk

`shoaladm` reads a device's journal from the inventory at the three levels a device is read:
- **validation** holds the journal clear of the node's roots, of every device and of the remote
  directory, which destroy deletes whole;
- **preflight** checks the journal directory is there, which deploy never creates;
- **the cache** under every rotational device whose configuration does not say it is on is turned
  off: `hdparm -W0`, then a rescan of the disk, since X7 found the kernel goes on flushing until it
  is, then a check that `queue/write_cache` reads `write through`. Writing that file alone changes
  only what the kernel believes, and is never done;
- **a udev rule** keyed on the disk's serial (`/etc/udev/rules.d/61-shoal-write-cache-<serial>.rules`)
  does the same at every boot;
- **a refusal** names a host with no `hdparm`, and a cache that does not turn off;
- **destroy and a wipe** empty a journal directory holding Shoal's marker, as they empty a device.
  A disk's cache is left off;
- `status` shows each device's journal;
- **the bench** moves a journal beside the inventory's as it moves a device, and makes it.

### The fixture

- **`.disks()`** makes every device of a cluster a disk: rotational, its cache writing through, its
  journal in a directory beside it.
- **`SLOW_ARM <ms>`** stands one arm in for a node's rotational slices, each apply and read holding
  it that long in turn.
- **`SHOAL_TEST_ROTATIONAL=1`** does for a whole run what `.disks()` does for one cluster, in process
  and in every child. Every M14 to M18 test that names a device runs again under it, as the gate
  asks. A test's multi-slice part is skipped there, and says so, since a disk has one slice.

## Design choices

**Rotational is the kernel's word, and a pool's media its configuration's.** A device's rules follow
what its disk is, which the kernel says and an operator's label may not. A pool's geometry is fixed
when it is committed, before a device has reported, so it cannot wait for the kernel, and the user
chose an explicit key over reading a class's name.

**The applier sits under `Slice::apply`**, not in the executor's routing, so every caller is batched,
the rebuild's and the fold's included, with no path that forgets.

**A batch waits for the foreground at most one bound.** Waiting for no foreground at all would starve
applies under a steady read load and fill the journal; not waiting would let a read wait behind two
batches.

**Applies stopped counting as foreground on every slice.** A batch that counted as foreground would
wait out its own bound for itself, and S13 always put applies below the foreground: the code had them
on the foreground queue though the executor's comment said otherwise. An SSD's behaviour changes only
in its share of the executor: a background still waits for every apply in flight.

**One executor for every disk.** X7 measured a disk's operation at under 1% of a core, and a second
executor a disk would take another core from the shards.

**A journal's directory per filesystem.** A device's domain is its journal's id, and a node with two
journal directories on one SSD would count one SSD as two domains; refusing that is cheaper than
probing a disk under every filesystem.

**A lost journal serves its disk.** Decided with the user; failing it would retire every chunk on it,
days of rebuild per X12, and on the lab's three hosts a retired disk had nowhere to go.

**A moved journal is refused while chunks are held**, not decided at open. A record dropped stays in
its segment, so the claim cannot tell a journal holding a staged write from one that held it once.

**The cache is turned off by `hdparm` and a rescan**, as X7's harness did, and kept off by udev, not
by the node: a node that could change its disk's cache would be a node that could turn it back on.

## Alternatives rejected

**Whole chunks staged in the SSD journal.** A put acknowledged at the SSD's latency, at the cost of
every byte written twice and a journal sized for bursts; rejected with the user.

**Every read of a chunk reads it whole**, as Q23's wording has it. Rejected with the user: with no
cache a small read pays 4 MiB, and `ObjectFile`'s 1 MiB read-ahead reads each chunk four times.

**Failing a device whose journal was lost**, the simplest rule. Rejected with the user for the days
of rebuild it costs a disk whose chunks are intact.

**Failing a device that breaks a rule** rather than refusing its node. Rejected with the user: F88
would retire it and rebuild every chunk for a setting that drifted.

**A pool rotational by its class's name**, `hdd*`, which the claim's warnings already read. Rejected
with the user for an explicit key.

**Applies in offset order**, S13's first answer. X7 found it bought nothing.

**A batch bounded by a count.** A count holds a different time on every disk and every chunk length;
a bound in time is what a read's wait is.

**An executor for each disk.** X7 could not say a disk needs one, and each costs a shard.

## Limitations

- **A part of a chunk's stage still reads the chunk's header from the disk**, to know the label it
  stands on: no write and no sync reach the disk, but a seek does. A header cache would take it off.
- **Disks share their executor among themselves**, unmeasured: X7 had one disk a host
  ([todo](../appendix/todos.md#whether-disks-may-share-an-executor)).
- **A journal that fails at run time fails its devices one at a time**, each at its own next journal
  operation, as any device error does; they are one domain already, so placement loses nothing.
- **A lost journal's scrub is light**: it finds a stale label, never a torn chunk, which the deep
  scrub's schedule finds.
- **A standalone node does not scrub a lost journal's pool**, since it scrubs nothing (F89).
- **The batch's bound is reckoned from the batches before it**, not measured ahead of each, so the
  first batch after a change of chunk size or load can run past it by the difference.
- **A disk with no serial in udev gets no rule**: its cache is off until its next power cycle.
- **The wizard neither probes a host's disks nor proposes 4+2 for one**
  ([todo](../appendix/todos.md#what-f85-left-for-later)).
- **A small read on a disk pays a seek for each range**; ~~there is no cache of chunks read
  ([todo](../appendix/todos.md#a-cache-of-chunks-a-disk-read)).~~ since
  [F102](read-unit-cache.md) a node may keep the units its reads verified and answer them again
  without the disk, ~~off unless named~~ an eighth of its memory unless it names otherwise since
  [F107](cache-default-share.md).
- **The lab has one disk a host**, so a rotational pool there is three copies by host; 4+2 over
  disks runs only in the fixture.

## Invariants to uphold

- **A rotational device's rules refuse its node's start**, by name: XFS, one slice, a journal, a
  cache that writes through or a configuration that says it is on.
- **A journal's directory and a device's path are never created.**
- **A slice reads only its own journal directory**, held to it by its marker.
- **A whole stage beside a journal elsewhere joins its own device's flush before its record is
  synced.** Under `apply_sync: shared` that record's sync flushes only the journal's device.
- **A journal is never made again over a record**: its segments are opened at their own length, and
  resized only once a start finds it holding none.
- **A journal's id names one filesystem**, so devices that share it are one `device` domain and no
  others are.
- **An apply is never counted as foreground**, and a background waits for every apply in flight.
- **A rotational slice's batch is issued only once its foreground is done or one bound has passed**,
  so a read waits for at most one batch.
- **A disk's slice is never on an executor with an SSD's.**
- **The sysfs write cache knob is never written alone**: it changes what the kernel believes, never
  the disk.

## Performance

### In the fixture

`rotational_applies_are_batched_and_bounded` drives sixty-four applies at once at a slow arm of 10 ms
and a bound of 50 ms. On europa: 19 batches of at most 4 applies, every apply of a batch in flight at
once, the longest batch 42 ms, and a read among them answered in 23 ms. A lost journal's light scrub
and rebuild run in under a minute.

### On the lab

*Recorded in a change of its own once the lab's runs are taken, as F86 to F90's were.*

## Tests

| Test | What breaks if it is reverted |
| --- | --- |
| `device_store` `rotational_applies_are_batched_and_bounded` | A disk's applies are issued one at a time, a batch is not in flight at once, runs past its bound, or a read waits behind more than one batch, or an SSD batches |
| `device_store` `rotational_slice_never_shares_an_ssd_executor` | A disk's slice shares an SSD's executor, or one executor is taken for both |
| `devices` `rotational_device_with_its_cache_on_is_refused` | A disk whose cache writes back starts, or one said to be on deliberately is refused or not warned of |
| `devices` `rotational_device_needs_a_journal_device` | A disk with no journal, of two slices, on ext4, with its journal missing, on a disk or inside it, starts; a journal is made where it is not named; two disks on one journal are two domains |
| `cluster_fixture` `rotational_stage_is_acknowledged_from_its_journal_device` | A stage of part of a disk's chunk writes or syncs the disk, is not in the SSD journal, or is lost to a crash before its apply |
| `objects` `rotational_pool_of_one_loss_is_refused` | A rotational pool of two copies, 2+1 or one copy starts unaccepted, an accepted one is not warned of, or its chunk is under 4 MiB |
| `cluster_fixture` `a_lost_journal_serves_its_device_and_asks_a_scrub` | A disk whose journal was lost is failed, not warned of, or its lost write is not found by a light scrub asked at once and rebuilt |
| `pool_placement` `devices_sharing_a_journal_are_one_failure_domain` | Devices on one journal are two `device` domains, or a moved journal does not move them |
| `pools` `a_rotational_pool_keeps_a_disks_rules` | A rotational pool's default stripe, its chunk floor or its exposure rule is lost |
| `slice` `a_whole_stage_beside_its_journal_flushes_its_device` | A whole stage beside a journal elsewhere is acknowledged before its own device is flushed |
| `slice` ~~`a_rotational_read_is_one_span`~~ `a_range_is_read_as_one_run` since F105 | A disk's read of several units is several reads, or a staged unit is not laid over its span; since [F105](store-reads-in-place.md), an SSD's too |
| `slice` `a_journal_is_resized_only_once_it_holds_nothing` | A journal of another length is made again over its records (Resolved #251) |
| `claim` `a_moved_journal_is_refused_while_chunks_are_held` | A journal moves from under a device holding chunks, or an empty device's old journal stays |
| `claim` `a_lost_journal_is_claimed_fresh_and_said` | A lost journal fails its disk, keeps its old id, or goes unsaid |
| `claim` `a_replaced_devices_journal_is_removed` | A replaced device's journal directory is left behind |
| `applier` `a_batch_is_what_its_bound_holds` | A batch's size does not follow its bound and reckoning |
| `devices` `a_device_reads_as_documented`, `a_device_that_cannot_be_is_refused` | A journal, a cache or a bound key is read wrong, or a journal inside its device, a relative one or a bound out of range is taken |
| `inventory` `devices_resolve_whole_and_are_held_clear_of_every_root` | An inventory's journal in a root, a device or the remote directory, or a relative one, is accepted |
| `ops` `a_disks_write_cache_is_turned_off_by_hdparm_and_kept_off` | Deploy turns a cache off by the sysfs knob alone, keeps it off by no rule, or writes a script that does not parse |
| `model` `devices_are_drawn_a_line_each` | `status` stops showing a device's journal |
| Every device test of M14 to M18 under `SHOAL_TEST_ROTATIONAL=1` | The same code fails on a disk: `devices`, `device_store`, `objects` and the fixture's device rows |

## Related

[M19](../object-storage/milestones.md#m19-rotational-devices) for the gate;
[S6](../object-storage/device-store.md#what-a-rotational-device-changes),
[S13](../object-storage/isolation.md), [S4](../object-storage/pools-and-devices.md) and
[S5](../object-storage/placement.md#failure-domains) for the design;
[X7](../object-storage/device-store-hdd.md) and [X12](../object-storage/recovery-scrub-rates.md) for
the figures it is built on; [S18's record](../object-storage/contract.md#m19-rotational-devices-2026-10-16)
for the decisions; [F85](devices-and-pools.md) for the store and the claim it extends;
[F88](recovery-and-moves.md) and [F89](scrub-and-repair.md) for the rebuild and the scrub a lost
journal relies on; [Resolved #251](../appendix/resolved/journal-resized-over-records.md) for the journal
found resized over its records on the way.
