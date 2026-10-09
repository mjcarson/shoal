# F71. Device counters and node memory in a bench capture

Every run of a `shoaladm bench` capture now records what each host's block devices did while it
ran: the kernel's own counters for every device a node's storage root is on, read before the
run and after it. It also records each member's memory every two seconds: the resident set, the
rows in memory, and the bytes of every index no budget counts. `bench show` prints both under
each run. `bench compare` reads three figures from them:

- the bytes the devices wrote for each byte the driver sent;
- the largest member's resident peak;
- its index bytes.

Before this feature, every one of those numbers was taken beside a run by hand, with scripts
nobody committed.

## Context

This is one of the two optional rows on
[What a spike needs first](../object-storage/spikes.md#what-a-spike-needs-first). Both feed
[X3](../object-storage/spikes.md#x3-bytes-through-the-tablet-groups) and
[X10](../object-storage/spikes.md#x10-what-a-stripe-row-costs). X3 is `shoaladm bench` at rows of
64 KiB to 4 MiB on the lab, and it records, for each row size and mix:

- device bytes written for each byte stored, WAL and archives apart;
- the node's resident set at steady state.

X10 records the index's bytes for a row. (In the end X10 read them from every member's `Stats`
itself, since its cold commit is an update the bench cannot drive; the figures are the same ones,
[X10's record](../object-storage/stripe-row-costs.md#the-harness).) ~~X3 is `shoaladm bench`~~ X3
did not run through the bench either: it drove shoal-loadgen's own driver from a spike crate,
since the bench cannot place three nodes on one host and reads a run's counters before its merges
finish. It read the devices through this feature's script, `shoaladm::bench::devices`, around
steps it let settle, and every member's memory from `Stats` as this feature does
([X3's record](../object-storage/bytes-through-groups.md#the-harness)). Before this feature:

- **Nothing in the tree read the kernel's device counters.** The cluster testing took its write
  amplification by device from `/proc/diskstats` before and after each run, with
  `target/lab/diskstats.sh`, a scratch script that was never committed
  ([cluster testing](../cluster-testing/overview.md#the-lab),
  [write amplification by device](../cluster-testing/performance.md#write-amplification-by-device-and-filesystem)).
- **A capture kept none of a node's memory.** Every member has reported `resident_bytes`,
  `archive_map_bytes`, `table_index_bytes`, `wal_index_bytes`, `lru_bytes` and `memory_bytes`
  since the cluster testing's round 11 ([cluster stats](cluster-stats.md)). The bench read those
  figures every two seconds for `answers_per_sec` and `p99_ms` alone ([F65](query-figures-home-tab.md),
  [F67](bench-run-wizard.md)), and dropped the rest.

The row was optional because a script beside the run takes the same numbers. It was built
because a number taken beside the run is not in the capture: it cannot be compared, and it cannot
be found later beside the run it describes.

## What it does

### The devices of every host

Before each run's clock starts, the bench reads every host of the inventory over ssh, one
session a host, in parallel. It reads them again once the run's last answer is in and its
event is done, **before the read back** of its acknowledged inserts, which would otherwise count
as reads the arm never made. The two reads cover the warmup, the measured time and the drain,
which is the same time the driver's series covers. The script (`shoaladm/src/bench/devices.rs`,
`devices_script`) does three things:

1. For each storage root of each node on the host, labelled `<node> <role> <path>` with the role
   `latency` (the WAL) or `throughput` (the archives), it finds the device:
   - it walks up to the nearest path that exists, so a root the bench has not made yet is counted
     on the device it will be made on;
   - it asks `findmnt -n -o SOURCE -T` for the mount's source and cuts a btrfs subvolume's
     `[/path]` from it;
   - it follows the source with `readlink -f` and takes the basename, so a device mapper volume
     is named `dm-N`, as `/proc/diskstats` names it.
2. It prints the host's uptime, so the time between two reads is the host's own.
3. It prints every line of `/proc/diskstats`.

The difference between the two reads becomes, for each host, a `HostDevices`:

| Field | What it holds |
| --- | --- |
| `host` | The host, as the inventory reaches it |
| `nodes` | The nodes on it |
| `devices` | One `DeviceCounters` for each device a root is on, with the roots on it |
| `unresolved` | The roots on no device the kernel counts: a tmpfs, an overlay |

A `DeviceCounters` holds reads, bytes read, writes, bytes written, discards, bytes discarded,
flushes, the milliseconds the device had I/O in flight, and the seconds between the reads.
Sectors are 512 bytes in `/proc/diskstats` whatever the device's own size, and a counter that
went backwards counts as zero. A run keeps them as `RunResult::devices`. A run whose hosts could
not be read keeps the reason in `devices_unread` and says so once on the screen. One reason is a
node started by hand with `--addr`, which comes with no inventory and so no host.

**WAL and archives apart.** A node whose `throughput` root is on another device from its
`latency` root has each counted on its own device, so the WAL's bytes and the archives' are read
apart, as X3 asks. A node whose two roots share a device, as every node on the lab does, has one
device with both roots on it, and the two cannot be told apart from its counters.

### Each member's memory

Every two seconds the bench reads the leader's `Stats`. Each `ServerSample` in a run's
`server_series` now carries `memory`, each live member's `MemberMemory` by the name the stats
view gives it. A `MemberMemory` holds `resident_bytes`, `memory_bytes`, `archive_map_bytes`,
`table_index_bytes`, `wal_index_bytes` and `lru_bytes`. It costs nothing extra: it comes from
the read that was already made.

### Show, compare and the screen

`bench show` prints these lines under each run:

```text
insert100/b16/none
  run 0: insert 32991/s p50 12.97ms p99 49.22ms | bundle p50 12.97ms p99 49.22ms | sent 6.77 MiB/s received 3.90 MiB/s | acks 1000 lost 0
    devices: europa nvme0n1p1 wrote 3.31 GiB read 1.42 MiB | hyperion dm-0 wrote 298.02 MiB read 6.00 KiB | titan dm-0 wrote 288.82 MiB read 6.00 KiB | 48.85 bytes written a byte sent
    resident peak europa 495.29 MiB, hyperion 630.14 MiB, titan 605.94 MiB, indexes europa 41.43 MiB (archive map 600.00 KiB), hyperion 41.51 MiB (archive map 600.00 KiB), titan 36.38 MiB (archive map 600.00 KiB)
```

That is a run on the lab (see [Performance](#performance)).

When an arm ends, the run logs the same lines, so they appear on the screen's log and in
`log.txt`. `bench compare` reads three metrics of the whole run beside the driver's own. Each
one is lower-is-better:

| Metric | Read off |
| --- | --- |
| `device bytes / sent byte` | Every host's device bytes written, over the bytes every second of the run sent, warmup and drain included (`RunResult::device_bytes_per_sent_byte`) |
| `peak resident MiB` | The largest resident set any member reported (`RunResult::peak_resident`) |
| `index MiB` | The largest member's archive map, table and WAL indexes in the run's last sample (`MemberMemory::index_bytes`) |

A capture without them reads as absent rather than as a regression, as F69's bytes did.
`Metric` now reads a run rather than its measured window; every metric from before reads that
run's measured window, as it did.

## Design choices

**Two reads a run, outside its clock.** Each read is an ssh session per host, a round trip of
tens of milliseconds. Before the clock starts and after the last answer, it costs the arm
nothing. A series sampled during the run would show the device's rate second by second. It
would also put an ssh session on every host every second of every arm, and the four-core lab
hosts would feel it.

**The device is found by the script, every read.** The roots are resolved where they are, on
each read, so a root on a device that was swapped or remounted between two runs is still counted
where it is. A mapping kept from the start would count the old device.

**Kept by host, not by node.** A counter belongs to a device and a device to a host. Two nodes
on one machine share its counters, and keying by node would count the device twice.

**Counted over the whole run, and divided by the whole run's bytes.** The counters cannot be cut
at the warmup's end without a read there, and a read there would be inside the clock. So the
ratio divides by the bytes of every second of the series, warmup and drain included: both sides
of the division cover the same time.

**The script runs under `sh`, whatever the host logs in with.** ssh hands a command to the
login shell, and europa's is zsh, which ties a variable named `path` to `PATH`. The first
version assigned `path` and lost `head`, `sed` and `cut` on europa alone. The lab run found it,
and recorded `devices_unread` for every run, as it should have. The script is now wrapped in
`sh -c`, and its variables are named so that no shell gives them a meaning.

**Memory from the read already made.** The figures every member reports reach the leader with
its status reports and are read every two seconds for the stats charts. Keeping all of them is
a copy of six numbers.

## Alternatives rejected

**Scripts beside the run**, as the cluster testing did. They take the same numbers, as the
spikes page said when it marked this optional. But the numbers are not in the capture, so
`compare` cannot judge them and nothing ties them to the run they describe. A script also has to
be run by hand, every time.

**`/proc/<pid>/io` of each node's process.** It separates what the process asked to write from
what the device wrote: the filesystem's share, the column the cluster testing's second table
has. It needs the node's pid and root on the host, since the nodes run as the system user
`shoal`. That is a privilege the bench asks for nowhere else. It is filed in the
[todos](../appendix/todos.md#what-f71-and-f72-left-undone).

**A trace of writes by file name**, as O62 took with bpftrace. That is the only way to split the
WAL from the archives on one device, but it needs root and bpftrace on every host, and it costs
more than the counters it would replace.

**Reading memory from `/proc/<pid>/status` over ssh.** The node already reports its resident
set, and more than the process's view: the index bytes no budget counts.

## Limitations

- **A device counter is the device's, not the cluster's.** It counts the operating system, the
  logs and any other unit on the device. On titan and hyperion the bench's roots are directories
  on the root volume. `--stop-unit` stops another cluster's node for the run, and
  `--allow-neighbours` is recorded in the capture's provenance.
- **Writes deferred past the run's end are missed.** A compaction, or an archive written after
  the last answer, lands after the second read, or in the next run's counters.
- **The preload is not counted.** Only arms are read; the preload's bytes are in no capture.
- **WAL and archives apart only on separate devices.** On one device they are one figure. A
  trace of writes by file name would split them, and is not built.
- **A btrfs filesystem over several devices** is counted on the one device `findmnt` names.
- **Memory is as old as the member's last report**: a few seconds, since figures ride one status
  report in four.
- **No per-second device rate.** The counters are two reads a run. A rate over the run is
  `written_bytes / secs`.

**Since [F74](client-routing.md) a server sample carries the members' hops too**:
`ServerSample::hops_per_sec`, by kind, summed over the members, named even at zero so a routed
run reads as none rather than absent.

## Invariants to uphold

- **The second read comes before the read back.** A read back counted as device reads would
  charge the arm with reads it never made. `run_arm` reads the devices before `verify`.
- **Both reads are outside the arm's clock.** An ssh session inside it would be measured as the
  cluster's time.
- **The amplification divides by bytes over the same time as the counters**, the whole series,
  never the measured window alone.
- **New capture fields are defaulted and skipped when empty**, so the capture format stays at 1
  and every capture from before reads.
- **A root's role comes from the inventory's `latency` and `throughput`**, so a reader can tell
  the WAL's device from the archives' when they are apart.
- **Every script the bench sends a host is shell neutral.** A login shell can be anything, and
  only `sh -c` says which one runs it; `the_script_runs_under_every_login_shell` holds this one
  to sh, bash, dash and zsh.

## Performance

Nothing on a node's path changed. The bench adds two ssh sessions a host a run, outside the
arm's clock, and a copy of six numbers a member every two seconds. No A/B was taken, since the
procedure for one is for a change that adds work to a node's query path.

On the lab (2026-10-03) `shoaladm bench run` was run on a cluster of the bench's own built from
`tmdb_cluster.yaml` (europa, titan and hyperion), with `shoal-tmdb` stopped for the run by
`--stop-unit` and started again after:

- the committed catalog dataset, `read100,insert100` at bundles 1 and 16, two runs each;
- 2 s of warmup and 10 s measured, inserts wrapping their pool;
- a paced stream on `Review` at 50/s ([F72](bench-paced-stream.md));
- built at `4908df6` with this change uncommitted (`--allow-dirty`).

**The first run found a defect.** europa logs in with zsh, the script's `path` variable cleared
its `PATH`, and every run recorded `devices_unread` for all three hosts, as it should when a host
cannot be read (the design choice above). The second run, with the script under `sh`, read every
host in every run. Each figure below is the range over the two runs; resident and index are the
largest member's.

| Arm | europa `nvme0n1p1` (Optane, btrfs) | titan `dm-0` (970 EVO, ext4) | hyperion `dm-0` (970 EVO, ext4) | Device bytes a byte sent | Resident peak | Index |
| --- | --- | --- | --- | --- | --- | --- |
| `read100/b1` | 3.2–3.5 MiB | 33–47 MiB | 1.4–46 MiB | 0.23–0.35 | 414–428 MiB | 3.65 MiB |
| `read100/b16` | 4.0–5.7 MiB | 1.3–42 MiB | 45–58 MiB | 0.14–0.22 | 404–412 MiB | 3.65 MiB |
| `insert100/b1` | 3.08–3.10 GiB | 143–188 MiB | 150–193 MiB | 427–435 | 420–424 MiB | 7.5 MiB |
| `insert100/b16` | 3.31–3.36 GiB | 289–293 MiB | 290–298 MiB | 48.9–51.6 | 621–630 MiB | 40.0–41.5 MiB |

What the table shows:

- **europa's btrfs wrote sixteen to twenty-two times what either ext4 host wrote** for the same
  inserts. At bundle one, about 40,000 inserts an arm over the host's 13.4 s between reads, that
  is 81–83 KiB an insert against 3.7–5.1 KiB.
  It is the gap the cluster testing measured beside its runs, 16,577 MB against 2,454
  ([write amplification by device](../cluster-testing/performance.md#write-amplification-by-device-and-filesystem)),
  now in a capture. europa wrote 248–270 MB/s at bundle one and at bundle sixteen, whose
  insert rates are ten times apart: its writes follow its WAL syncs, not its rows, which is
  [O61](../appendix/optimizations.md#o61-a-fast-device-syncs-the-wal-in-batches-too-small-to-fill-a-page)'s
  finding. The bench's cluster ran europa's `wal_commit_delay` of 3 ms from the inventory, and O61
  recorded 7.7 GB in 30 s at that delay.
- **The read arms write.** On titan and hyperion it ranges from 1.3 MiB to 58 MiB between two
  runs of one arm, since their roots are directories on the root volume and the system's own
  writes land there. That is the first limitation below, visible. europa's Optane holds only the
  nodes' roots, and wrote 3–6 MiB in a read arm.
- **The indexes grew** from 3.65 MiB to 40–41.5 MiB on the arms that inserted at bundle sixteen.
  The archive map stayed at 600 KiB on every member in every run: the dataset is two thousand
  keys a table, and the inserts wrap over the same thousand, so the map, an entry a row, had
  nothing new to hold.

**Two captures of one build are not a result.** `bench compare` of the two runs joined every arm
and refused no fact. The device ratio read `not on both sides`, since the first had none.
`read100/b16`'s rate still read as better by at least 10.8%, and its paced p99 as better by at
least 26.8%, with nothing changed but the device script. Two runs make a narrow interval; a lab
difference takes the rounds the [lab procedure](../performance/benchmarking.md#before-and-after-on-the-lab)
asks for.

## Tests

| Test | Where | What breaks if reverted |
| --- | --- | --- |
| `the_script_names_each_root_by_its_place` | `shoaladm/src/bench/devices.rs` | The script does not quote a root, number it, or read the counters and the clock |
| `a_read_is_parsed` | `shoaladm/src/bench/devices.rs` | A root's device, the clock or a device's counters are misread, or a field is taken from the wrong column |
| `two_reads_become_what_each_device_did` | `shoaladm/src/bench/devices.rs` | A run's delta is wrong, a root on no counted device is not named, or a counter that went backwards wraps |
| `two_roots_on_one_device_are_counted_once` | `shoaladm/src/bench/devices.rs` | Two roots on one device are counted twice, or a kernel without discard fields is misread |
| `the_script_resolves_a_real_directory` | `shoaladm/src/bench/devices.rs` | The script, run on this machine, does not resolve a real directory, or one not made yet, to a device the kernel counts |
| `the_script_runs_under_every_login_shell` | `shoaladm/src/bench/devices.rs` | The script stops running under a login shell other than sh: under zsh, unwrapped, it failed with `command not found: head` |
| `devices_memory_and_the_paced_stream_round_trip` | `shoal-loadgen/src/results.rs` | A run's devices or memory do not survive a capture, or a run from before F71 fails to read |
| `devices_memory_and_the_paced_stream_are_compared` | `shoal-loadgen/src/compare.rs` | The amplification, the resident peak or the index bytes are not compared, are computed over the wrong bytes, or are judged against a capture without them |
| `a_run_by_addr_records_the_nodes_answers` | `examples/bench_dataset/tests/stats.rs` | A member's resident set or index bytes are not sampled into the server series |
| `a_run_against_one_node_writes_a_capture_that_compares` | `examples/bench_dataset/tests/bench_run.rs` | A run by `--addr` claims device counters, or does not say why it has none |

## Related

[F66](dataset-benchmarks.md), the capture this extends; [F72](bench-paced-stream.md), the other
half of the same change; [cluster stats](cluster-stats.md), where the memory figures come from;
[X3](../object-storage/spikes.md#x3-bytes-through-the-tablet-groups) and
[X10](../object-storage/spikes.md#x10-what-a-stripe-row-costs), which asked for them
([X10's record](../object-storage/stripe-row-costs.md) read them from `Stats` directly, and
[X3's](../object-storage/bytes-through-groups.md) through this feature's script around settled
steps);
[write amplification by device](../cluster-testing/performance.md#write-amplification-by-device-and-filesystem),
the numbers this takes in a capture.
