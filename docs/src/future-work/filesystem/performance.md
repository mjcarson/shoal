# W39. Performance, and the benchmarks that judge it

## Context

Performance was the first thing asked of this plan, so every gate is judged by measurement before it
is passed, and every number is labelled by host, cpu, governor, device and filesystem
([R41](overview.md#what-is-being-asked-for)). A filesystem is judged two ways that pull apart: by its
metadata, operations a second and their latency, which is where JuiceFS's engines and CephFS's servers
differ most ([W29](prior-art.md)); and by its bytes, which here are the volumes' lane and judged as a
volume's are ([volumes' W25](../volumes/performance.md)). This page is what is compared with what, what
the bench gains, what a run records, the budgets, and what would make a number wrong.

## What exists today

- **`shoaladm bench`**, its object arms, its events and its captures
  ([F66](../../features/dataset-benchmarks.md), [F71](../../features/bench-device-memory.md),
  [F104](../../features/memory-in-a-capture.md)); the store comparison's method of alternating sides and
  disjoint intervals ([store comparison](../../object-storage/store-comparison.md)).
- **Row commits and strong reads on the lab**: a row commit 5.3 ms at the median on the SSD leg
  ([group commit delay](../../object-storage/store-optimizations/group-commit-delay.md)) and 0.64 ms on
  europa's Optane over loopback ([F98](../../features/commit-delay-bounded-by-the-sync.md#performance)); a
  strong 4 KiB read 0.48 ms on average ([strong reads](../../object-storage/store-optimizations/strong-reads.md)).
- **No metadata arm exists**, and no FUSE mount.

## The design

### What is compared with what

| Experiment | Held constant | What it tells | Where |
| --- | --- | --- | --- |
| A create, a `stat`, an unlink, a rename inside a directory and between directories | The lab | Each operation's latency and rate, and its rounds | [X32](spikes.md#x32-metadata-operations-priced), M36, M37 |
| Inodes co-located with their entries, and the owner in the sort key | The workload | What co-location buys | X32 |
| A namespace command, and the single writes it replaces | The rows | WAL bytes, latency, compaction | X32, M35 |
| A directory part of 256 to 16,384 entries | The operations | Where a part should split | [X33](spikes.md#x33-a-directory-parts-size) |
| FUSE classic, and over io_uring; the writeback cache on and off | The mount | What the kernel's trip costs, and what the cache saves | [X34](spikes.md#x34-fuse-classic-and-over-io_uring), M39 |
| A call-back at 0 to 256 MiB dirty; two mounts writing one file | The file | What a turn costs | [X35](spikes.md#x35-a-call-back-priced), M38 |
| A small file's create, write and close, and a bucket's put of the same bytes | The lab | What inline files and two commits buy | M38 |
| The reference cell alone, and beside a metadata storm | The cell | **The budget** | X32, every gate from M36 |
| A file tree, CephFS and JuiceFS on the same devices *(optional)* | The lab | Where Shoal stands | [X36](spikes.md#x36-cephfs-juicefs-and-infinifs-read-at-the-source) |

### What the driver gains

[W28](prerequisites.md#grants-for-file-trees-and-the-benchs-filesystem-kinds) places these before M36:

- **Namespace kinds**: create, `stat`, unlink, rename, `mkdir`, `rmdir` and listing, at a chosen number of
  files a directory and directories a tree, in mdtest's two shapes, **easy** (a directory a worker) and
  **hard** (one directory shared by every worker).
- **Small-file kinds**: a file of 1 to 64 KiB created, written and closed, and read back.
- **File I/O kinds**: a file's sequential and random reads and writes on the lane, as the volumes' block
  kinds are.
- **Workloads through a mount**: an `untar` of a source tree, a parallel build, a `find` and a `du`.
- **Events** appended to `EventKind` (`shoal-loadgen/src/spec.rs:196`): `mount_kill`, a call-back storm, a
  storm of renames between directories.

### What a run records

| Figure | Of | From |
| --- | --- | --- |
| Operations a second and latency at p50, p99 and p99.9, by kind | The tree | The driver's windows |
| Commits and groups an operation took | The tree | A node's figures: one, or two and their record |
| The reference cell's read and write p99, before, during and after | The foreground | Its windows |
| Evictions by cause, and the namespace's resident rows against its cap | Each member | F104's counters |
| FUSE requests and the daemon's round trips, by operation | The mount | The daemon's figures |
| A call-back's time, and its dirty bytes | The file | The daemon's figures |
| Device bytes a byte written, for file I/O | Each host | F71's counters |

### The budgets

Hypotheses, each set by a spike before the gate that is judged by it:

- **An operation in one tablet**, within 1.25 times one row commit's latency on each device.
- **A small file's create, write and close**, within 2.5 times one row commit.
- **An operation across groups**, within 2.5 times one row commit.
- **The foreground**: [W37](isolation.md#the-budget)'s, beside a metadata storm at a tree's default limits.
- **A call-back**, at the dirty cap, under a bound X35 states.
- **POSIX**: `pjdfstest` passing on a mount, every failure on [W41](contract.md#the-contract)'s list of what
  is not promised ([W36](fuse.md#acceptance-tests)).

### Where it runs

On the lab, by its procedure ([Spikes](../../object-storage/spikes.md#where-a-spike-runs)): a side cluster of
the bench's own, never the tmdb cluster; mounts on europa, where the driver runs, and on titan and
hyperion as a node's own host would mount; eight rounds or more for a budget, the sides alternating; every
number labelled by host, cpu, governor, device and filesystem.

### What would make a number wrong

- **The mount's caches.** A metadata arm through a mount is run with its caches at their defaults and again
  with them off, so a figure says which it measured.
- **europa's link**, for file arms, as for a volume's.
- **The 970 EVO's two flushes**, 0.9 ms rested and about 3 ms after synced writes, for any figure that waits
  on a commit.
- **The host's page cache**, for file arms: `O_DIRECT`, or its caches dropped between rounds.
- **One directory's group.** An mdtest-hard arm measures one group's rate on one node; it is reported as
  such, beside an easy arm that spreads.

### mdtest and fio

mdtest and fio are the industry's drivers and how a reader compares a figure with a published one; they run
beside a mount's arms as cross-checks, never in place of the bench, whose captures alone cut events, keep
device and memory counters and join the comparison's tables.

## Alternatives rejected

**mdtest as the driver**: no events, no counters, no capture, and its MPI harness on the lab for nothing the
bench cannot do (**recalled**: mdtest runs under MPI).

**Budgets against JuiceFS's or CephFS's published figures**: another host's devices. The lab's row commit is
what an operation is held to.

## What it costs

Namespace, small-file and file kinds and three events in `shoal-loadgen`; a mount on europa for the
through-a-mount arms; the lab's hours.

## What it breaks

Nothing.

## Invariants to uphold

- **Every number is labelled** and taken at a commit.
- **A difference is a result only when the intervals are disjoint.**
- **A mount's arm says whether its caches were on.**
- **No benchmark runs on the tmdb cluster.**

## Prerequisites

[W28](prerequisites.md)'s bench kinds; the volumes' block kinds for file I/O.

## How it would be measured

This page is how.

## Acceptance tests

| Test | Gate | Proves |
| --- | --- | --- |
| `a_namespace_kind_times_every_operation` | M36 | An arm's windows hold every operation's latency and its kind |
| `an_mdtest_hard_arm_reports_its_one_group` | M36 | A shared-directory arm's capture names the group it measured |
| `a_mount_arm_says_whether_its_caches_were_on` | M39 | A capture through a mount records its cache times |
| `a_call_back_storm_event_is_cut_at_its_marks` | M38 | A run with a storm of call-backs is cut into before, during and after |

## Related

[The volumes' W25](../volumes/performance.md), whose method this keeps; [W37](isolation.md) for the budget;
[W29](prior-art.md) for JuiceFS's figures and CephFS's; [F66](../../features/dataset-benchmarks.md) for the
bench.
