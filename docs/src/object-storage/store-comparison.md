# Shoal beside Ceph, MinIO, RustFS and SeaweedFS, measured

*The SSD leg was run on 2026-10-18, the disk leg on 2026-10-18 and 19, and the loop leg on
2026-10-19, every store's. Shoal was measured again alone on 2026-10-25 and 26, after the
optimizations its first run pointed to, and its verdicts below are that run's
([Shoal measured again](#shoal-measured-again)). Every chart and table below is drawn from the
committed captures by `shoal-bench render-stores`.*

Shoal's object store measured beside four others on the lab's three hosts, every store driven by
the one object driver Shoal is measured with
([F94](../features/bench-object-endpoints.md)): Ceph's RGW v20.2.0, MinIO's final community
release, RustFS 1.0.3 and SeaweedFS 4.48. Each store was deployed fresh on the same devices,
given the same objects, and run through the same arms in the same order:
- put, get, half of each, and stat;
- 4 KiB, 64 KiB, 1 MiB and 16 MiB objects;
- one operation in flight and sixteen.

**On SSDs over 1 GbE** ([where Shoal is better, and where worse](#where-shoal-is-better-and-where-worse)).
Each gap is a lower bound, the distance between the two stores' nearest runs. The first run's
summary is struck through, each statement above what Shoal's second run measured:
- ~~**Puts of 64 KiB to 1 MiB: Shoal is slower than every store.** At 64 KiB Ceph, MinIO and
  SeaweedFS are faster by at least 2.6 to 7.6 times, and RustFS by at least 2.9% to 49%. At 1 MiB
  every store is faster by at least 8.8% to 33%. Each stripe writes a whole 4 MiB chunk on each
  holder ([O107](../appendix/optimizations.md#o107-a-stripe-shorter-than-its-chunk-costs-its-device-the-whole-chunk)):
  196 device bytes for each byte put at 64 KiB with three copies.~~
- **Puts of 64 KiB: behind every store but RustFS**, which is within at both depths. Ceph, MinIO
  and SeaweedFS are faster by at least 22% to 3.0 times. Since
  [F96](../features/chunk-file-classes.md) a stripe's chunk takes a file sized to its units: 4.8
  device bytes for each byte put at 64 KiB with three copies, and Shoal's 64 KiB puts ran 2.7 to
  4.7 times as fast as its first run's.
- **Puts of 1 MiB: within a few per cent.** At one in flight every store but Ceph with three copies
  is faster, by at least 3.0% to 15%. At sixteen Shoal is ahead of Ceph in both groups, by at least
  6.0% and 2.7%, within RustFS, and behind MinIO and SeaweedFS by at least 3.3% and 6.5%.
- ~~**Puts of 4 KiB: within Ceph.** Ceph is ahead by at least 2.9% at sixteen in flight with three
  copies. SeaweedFS's synced put is ahead at one in flight, and it syncs one write fewer. Shoal is
  ahead of MinIO, whose puts wait out a lock retry, and of RustFS, by at least 80% to 3.2 times.~~
- **Puts of 4 KiB: within Ceph** at both depths in both groups. SeaweedFS's synced put is ahead by
  at least 24% at one in flight, and it syncs one write fewer. Shoal is ahead of MinIO, whose puts
  wait out a lock retry, and of RustFS, by at least 94% to 3.5 times.
- ~~**Small gets and stats: a little behind Ceph and SeaweedFS**, by at least 3.2% to 21%, and behind
  SeaweedFS's stats by at least 2.2 times. Every Shoal read here was a strong read. Shoal is within
  Ceph on 4 KiB gets at 2+1, and ahead of MinIO and RustFS.~~
- **Small gets and stats: ahead at sixteen in flight**, of Ceph by at least 13% to 60% and of
  SeaweedFS's gets by at least 15%. SeaweedFS's stats stay ahead, by at least 25%, and by 2.0 times
  at one in flight. At one in flight Shoal is within Ceph and SeaweedFS on gets with three copies,
  ahead of Ceph's at 2+1 by 15%, and behind Ceph's stats by at least 4.1% to 5.2%. It is ahead of
  MinIO and RustFS at both depths, by at least 32% to 3.8 times. Every Shoal read here was still a
  strong read; at sixteen in flight they ran 1.58 to 1.75 times as fast as the first run's, and
  their p99 fell by half.
- ~~**Gets of 64 KiB and up, one in flight: behind every store**, by at least 0.9% to 49%. Shoal read
  every byte off its device, where the others answered most gets from a cache.~~
- **Gets of 64 KiB, one in flight: behind every store**, by at least 8.5% to 28%. From 1 MiB Shoal
  is ahead of Ceph and SeaweedFS with three copies, by at least 3.9% to 35%. At 2+1 it is within
  Ceph at 1 MiB and behind it by 6.3% at 16 MiB, and behind MinIO and RustFS by at least 1.0% to
  42%. Since [F107](../features/cache-default-share.md) Shoal answers most of these gets from a
  cache of the units its reads verified, as the others answer from theirs.
- ~~**Gets of 64 KiB and up, sixteen in flight: mostly ahead.** With three copies Shoal is ahead of
  Ceph and SeaweedFS by at least 25% to 60%. A get is served from its group leader's own copy, and
  the driver shares a host with one leader. At 2+1 Shoal is ahead of Ceph by at least 12% to 23%,
  of RustFS by 15% and 16% at 64 KiB and 1 MiB, and of MinIO by 13% at 64 KiB. MinIO is ahead by
  1.1% and 1.5% at 1 MiB and 16 MiB.~~
- **Gets of 64 KiB and up, sixteen in flight: ahead of every store**, but MinIO from 1 MiB, which
  is within. With three copies Shoal is ahead of Ceph and SeaweedFS by at least 46% to 96%. A get is
  served from its group leader's own copy, and the driver shares a host with one leader. At 2+1
  Shoal is ahead of Ceph by at least 20% to 25%, of RustFS by 4.2% to 23%, and of MinIO by 21% at
  64 KiB.
- **Puts of 16 MiB are bounded by the network** for every store.

**On disks** ([on disks](#on-disks)). Shoal keeps its rows on each host's SSD, and Ceph its writes
of less than 64 KiB; MinIO, RustFS and SeaweedFS sync every put on the disk:
- **Puts of 4 KiB: ahead of every store at sixteen in flight**, by at least ~~2.9 times (Ceph) to
  17.6 times (RustFS)~~ 3.1 times (Ceph) to 19.0 times (RustFS), and within Ceph at one in flight. A
  4 KiB object is a row, which is on the SSD.
- ~~**Puts of 64 KiB and up: behind**, most at 64 KiB: Ceph and SeaweedFS are faster by at least 2.2
  to 15.7 times, MinIO and RustFS by at least 18% to 46%. At 1 MiB Ceph and SeaweedFS are faster by
  at least 43% to 3.3 times, while MinIO and RustFS are within or ahead by at least 16%. At 16 MiB
  every store is within or ahead, by at least 4.5% to 28%. A rotational pool's stripe writes a whole
  4 MiB chunk on each holder (O107), and a large object's chunks cost their disk twice their bytes
  ([O102](../appendix/optimizations.md#o102-a-whole-chunk-costs-its-device-twice-its-bytes)).~~
- **Puts of 64 KiB and 1 MiB: behind Ceph and SeaweedFS, and at sixteen in flight ahead of MinIO
  and RustFS.** Ceph and SeaweedFS are faster by at least 4.2% to 9.9 times, most at 64 KiB at 2+1,
  whose 32 KiB shards Ceph commits on its SSD. At sixteen in flight Shoal is ahead of MinIO by at
  least 44% to 2.3 times and of RustFS by 1.2% to 40%; at one in flight it is behind MinIO by 22% at
  64 KiB and ahead of both at 1 MiB. At 16 MiB Shoal is within every store or ahead, but Ceph with
  three copies at one in flight, by 6.2%. Since F96 a stripe's chunk takes a file sized to its units
  on a disk too, and nothing is written ahead: 4.5 device bytes a byte put at 64 KiB with three
  copies, where the first run wrote 204.
- **Small gets and stats: as on SSDs.**
- ~~**Gets of 64 KiB and up, one in flight: behind every store**, by at least 29% to 9.0 times,
  except at 1 MiB, where Shoal is within MinIO and ahead of RustFS by at least 33%. Every Shoal get
  is a seek; the others answer from memory.~~
- **Gets of 64 KiB and 16 MiB, one in flight: behind every store**, by at least 8.9% to 46%, and at
  1 MiB ahead of every store but Ceph at 2+1, by at least 1.2% to 2.1 times. Since F107 the 64 KiB
  and 1 MiB working sets are answered from Shoal's cache, and only at 16 MiB is every get a seek.
- ~~**Gets at sixteen in flight: behind at 64 KiB**, by at least 12% to 3.7 times. With three copies
  every get is read off the disk of the node that leads its group
  ([O119](../appendix/optimizations.md#o119-a-get-from-a-pool-of-copies-reads-its-coordinators-own-copy-so-each-disk-serves-the-gets-its-node-leads)),
  and europa's was busy while the others were half idle. From 1 MiB Shoal is ahead of Ceph in both
  groups and of SeaweedFS, and behind MinIO by at least 5.6% to 8.5%.~~
- **Gets at sixteen in flight: ahead of every store at 64 KiB**, by at least 10% to 37%, and from
  1 MiB of Ceph, SeaweedFS and RustFS, by at least 11% to 52%, but RustFS at 16 MiB, which is within.
  MinIO is ahead from 1 MiB, by at least 3.6% to 6.5%. The cache answers the 64 KiB and 1 MiB sets,
  where in the first run every get was read off the disk of the node that leads its group
  ([O119](../appendix/optimizations.md#o119-a-get-from-a-pool-of-copies-reads-its-coordinators-own-copy-so-each-disk-serves-the-gets-its-node-leads)).

**On one host, without the link** ([without the link](#without-the-link-europas-optane-over-loopback)).
Three instances of every store on europa's Optane:
- ~~**Small gets and stats: ahead of every store**, by at least 31% to 3.4 times, but SeaweedFS's
  stats at sixteen in flight, ahead by at least 12%. A strong read's round costs little on one host.~~
- **Small gets and stats: ahead of every store**, by at least 21% to 4.8 times. A strong read's
  round costs little on one host.
- ~~**Puts of 4 KiB: behind Ceph and SeaweedFS**, by at least 42% to 2.7 times, and ahead of MinIO and
  RustFS by at least 2.2 to 8.3 times. Shoal's put costs the least cpu of any store, and waits.~~
- **Puts of 4 KiB: ahead of every store**, by at least 20% to 26.5 times, but SeaweedFS at sixteen in
  flight, which is within. They ran 3.4 times as fast as the first run's at one in flight: since
  [F98](../features/commit-delay-bounded-by-the-sync.md) a WAL writer waits no longer than its last
  batch took to write and sync, which on the Optane is little.
- ~~**Puts of 1 MiB: behind every store.** At sixteen in flight Ceph, MinIO and SeaweedFS are faster
  by at least 2.6 to 4.1 times and RustFS by at least 20%: O107's largest gap on any leg.~~
- **Puts of 1 MiB: behind Ceph and MinIO**, by at least 6.1% to 40%, and SeaweedFS at sixteen in
  flight, by 28%. Shoal is ahead of RustFS, by at least 21% to 2.0 times, and of SeaweedFS at one in
  flight. Its 1 MiB puts ran 1.7 to 3.0 times as fast as the first run's.
- ~~**Gets of 1 MiB: behind every store**, by at least 9.1% to 2.1 times, but RustFS at sixteen in
  flight, which Shoal leads by at least 42%. Shoal reads every byte off the Optane, and the others
  answer from memory.~~
- **Gets of 1 MiB: ahead with three copies**, of Ceph and SeaweedFS by at least 10% to 2.1 times.
  At 2+1 Shoal is within Ceph, within MinIO at sixteen in flight and behind it by 11% at one, and
  ahead of RustFS by at least 40% to 2.4 times. Since F107 Shoal answers them from its cache, as the
  others answer from memory.
- ~~**Every other store spends more cpu**: 1.6 to 11 times Shoal's a 4 KiB get or stat, and 60 to 71%
  of europa's cpus busy for 1 MiB gets to Shoal's 18 to 19%.~~
- **Every other store spends more cpu**: 2.2 to 15 times Shoal's a 4 KiB get or stat at sixteen in
  flight, and 54 to 72% of europa's cpus busy for 1 MiB gets to Shoal's 21 to 34%, while Shoal
  served as many of them or more.

## Shoal measured again

Shoal was measured a second time on 2026-10-25 and 26, alone and at `257592b3`, after the
optimizations this comparison pointed to were built
([Optimizations the comparison points to](store-optimizations.md)). The other stores were not run
again, and every verdict on this page sets Shoal's new captures beside theirs of 2026-10-18 and 19:
- **Their programs did not change**, nor how each was deployed: `stores-lab.sh` deploys each from the
  same release or build. Checked against the snapshot taken before anything (`verify`), nothing
  changed on the hosts but Ubuntu's package updates (git, xz, zlib, tzdata and certificates among
  them) and the names europa's reboot gave its devices: its two NVMe drives swapped names and its
  loop devices were numbered again. F113 set each host's slow start after idle for Shoal's captures
  alone and put it back after each, as `stores-host-changes.txt` records.
- **But europa's kernel did.** Europa rebooted on 2026-10-25 into 7.0.0-38, its newest installed
  kernel, where every earlier capture ran 7.0.0-34. Titan and hyperion stayed on 7.0.0-34, and
  Shoal's second run was measured on these kernels rather than on a europa booted back into
  7.0.0-34. Europa holds the driver and a member of every store, so a kernel's difference reaches
  every arm. How much it is was not measured.
- **Shoal's first run is kept** as *Shoal, first run* on every chart and table, drawn beside today's
  Shoal and given no verdict. The distance between the two is what the optimizations moved on the
  comparison's own arms, with europa's kernel and a week's drift beside them.
- **Shoal's two captures of a group ran back to back**, with no store between them, so their union
  brackets less of the lab's drift than the first run's did, whose captures were six to eight hours
  apart ([Drift](#drift)).
- **Two runs are left out.** On the SSD and disk legs one run each of 2+1's 1 MiB gets at one in
  flight had a get that waited for its answer until the run ended
  ([item 316](../appendix/known-issues.md#316-a-gets-request-on-an-object-lane-went-unanswered-and-its-coordinator-waited-with-no-bound)).
  A run with a failed operation is left out of its arm's median and interval, so on each of those
  legs that arm rests on seven runs.

What Shoal ran differently, at the comparison's inventory as written, which names none of these:
- **A cache of the units its reads verified**, an eighth of node memory: 1 GiB a node across hosts,
  and 512 MiB on the loop leg ([F107](../features/cache-default-share.md)). The first run had none.
- **Every node's host keeps each connection's window through an idle**
  (`net.ipv4.tcp_slow_start_after_idle = 0`, [F113](../features/slow-start-after-idle.md)). The first
  run had the kernel's 1.
- **What its writes and reads cost**: a chunk's file sized to its units
  ([F96](../features/chunk-file-classes.md)), reads of copies on disks by their queues past a margin
  of four ([F97](../features/disk-reads-by-queue.md), [F101](../features/disk-read-margin.md)), a
  commit delay bounded by the sync with the inventory's 3 and 2 ms as its ceiling
  ([F98](../features/commit-delay-bounded-by-the-sync.md)), and a get's bytes read and sent without
  the copies it made ([F105](../features/store-reads-in-place.md),
  [F106](../features/lane-copies.md), [F109](../features/reads-from-stripe-buffers.md) to
  [F111](../features/lane-buffers.md)).
- **The lanes of rounds' minimum RTO** of 5 ms
  ([Resolved #304](../appendix/resolved/lone-frame-loss-probe.md)).
- **Its settle between arms** waits until every device's chunk bytes hold steady
  ([Resolved #301](../appendix/resolved/a-settle-waits-for-reclamation.md)), where the first run
  waited ten seconds.

The same as the first run: mutual TLS between nodes, every get and stat a strong read, a 16 KiB
inline threshold, and each host's own congestion control
([F112](../features/object-congestion.md)'s BBR is a deployment's choice).

## The question

Where is Shoal's object store faster or slower than the object stores an operator would otherwise
deploy, on the same machines, and why. [S15](performance.md) names the arms that judge Shoal
against itself; nothing on this part of the book had set it beside another store, and
[X14](ceph-and-s3-sources.md) ran a Ceph on the lab to see what it does, never how fast.

## How it was judged

**Two redundancy groups, compared only within each.** No store but Shoal and Ceph keeps a bucket
both ways, so each capture is of one store at one redundancy, and two stores are compared only
when they keep their objects alike:

| Group | What it survives | Stores |
| --- | --- | --- |
| **three copies by host** | two hosts lost | Shoal, Ceph, SeaweedFS |
| **erasure coded 2+1 by host** | one host lost, at 1.5 bytes a byte | Shoal, Ceph, MinIO, RustFS |

MinIO and RustFS keep no copies, only erasure coded sets. SeaweedFS codes only volumes it has
sealed, offline, and replicates what it writes.

**The lab's rule.** Every figure is the median of four runs of thirty seconds, after five of
warmup, with the lowest and highest run in brackets. A difference is a result only when the two
stores' run intervals do not overlap, and its size is the gap between their nearest ends, so it is
a lower bound ([before and after on the lab](../performance/benchmarking.md#before-and-after-on-the-lab)).
On every chart a whisker that crosses 1× is *within*.

**Shoal first and last in each group.** Shoal ran before the other stores of a group and again
after them, and its interval is the union of both captures' runs. A lab that moved over a group's
hours widens Shoal's interval rather than favouring either end ([Drift](#drift)). Measured again,
Shoal ran its two captures of a group back to back, with no store between them.

**Verdicts across a week.** Shoal's second run is judged against the other stores' captures of
2026-10-18 and 19, which were not taken again. Their programs and the hosts did not change, but
europa's kernel did ([Shoal measured again](#shoal-measured-again)).

**Statements made in advance.** What each leg was expected to show was written down before the
first leg ran (`shoal-spike/results/stores-predictions.md`), each with what would make it wrong.
They are reported beside the results and judged nowhere.

## What was run

### The harness

Every store is driven by `shoal-loadgen`'s object driver over an endpoint
([F94](../features/bench-object-endpoints.md)):
- Shoal through `shoaladm bench run --between-arms delete` on the tmdb schema's `Posters`
  bucket;
- every other store through `shoaladm bench s3`, path style, SigV4 with an unsigned payload, over
  one HTTP/1.1 client an endpoint.

Both are given the same spec, the same dataset (`shoal-spike/results/stores-objects/Posters.yaml`:
sixteen million objects described, each size's first 1024 preloaded), and the same arms in the
same order; `s3_and_run_make_one_spec_digest` holds the two commands to one spec.

Each operation is timed from before its bytes are made to its answer, and every get is compared
with its bytes made again. After every run, the driver:
1. reads back a sample of what the run put;
2. deletes everything it put;
3. runs the store's own settle command, after a run that put anything, which gives the space
   back: RGW's garbage collection with `rgw_gc_obj_min_wait` at zero, SeaweedFS's vacuum, and a
   ten second wait for Shoal's reclamation, whose grace for a reader of a deleted object is set
   to five seconds, from five minutes, with a pass every two. ~~Neither setting is on any arm's
   path; each only lets a deleted object's space come back before the next arm.~~ Neither setting
   is on an arm's path, but the wait was too short for an arm that put thousands of stripes: their
   reclamation ran through the arm after it, ~~in every capture here~~ on the loop leg
   ([Resolved #301](../appendix/resolved/a-settle-waits-for-reclamation.md)). Since that fix Shoal's
   settle reads every device's chunk bytes until they hold steady.
   - An arm that only reads writes nothing to a device. On the loop leg the 4 KiB gets and stats
     at one in flight, which followed the 1 MiB puts and the 1 MiB mixed arm at sixteen, wrote 256
     to 384 MB to the devices in each run at 2+1, and 16 to 82 MB with three copies. On the SSD and
     disk legs, whose arms put fewer stripes, they wrote 11 MB or less.
   - A put or mixed arm writes for itself, so how much of one after another was reclamation cannot
     be told from these captures, and they are not taken again.
   - What it cost one arm was measured later, on one build at 2+1 on the loop leg: 1 MiB puts at
     one in flight after 64 KiB puts at sixteen ran 128 a second with the old settle and 219 with
     the new (the resolved page's Tests).
   - Shoal's second run took the new settle, which waited 10 to 54 seconds an arm, and its read
     arms after the loop leg's puts wrote 0 to 18 MB a run.
   On the loop leg,
   whose devices are smaller than an arm's puts there, SeaweedFS vacuums each volume by its id until
   its devices empty, and MinIO and RustFS wait for theirs to
   ([without the link](#without-the-link-europas-optane-over-loopback)).

The whole preload is read back at the end of each capture. Before every run every host's page cache is
dropped, and an arm is refused on a device more than 80% full.
`shoal-spike/results/stores-lab.sh` deploys each store, runs it, and takes it down; its record of
every change to a host is `stores-host-changes.txt`, checked against a snapshot of each host taken
before anything.

### Where, and on what

| Host | CPU | Memory | Store device, SSD leg | HDD leg | Link |
| --- | --- | --- | --- | --- | --- |
| europa | Ryzen 9 7945HX, 16 cores, 32 threads | 43 GiB | an LV of a loop over a 100 GiB file on the Optane 900P's XFS | a directory on the WD6001FZWX's XFS; Ceph an LV of a loop over a 200 GiB file on it | 1 GbE |
| titan | Ryzen Embedded V1756B, 4 cores, 8 threads | 14 GiB | a 100 GiB LV of the 970 EVO | a directory on the WD140EDFZ's XFS; Ceph as europa | 1 GbE |
| hyperion | as titan | 14 GiB | as titan | as titan | 1 GbE |

- **One store at a time**, on devices made fresh for it: an LV discarded and given a new XFS, or
  left raw for BlueStore. A volume on a loop over a file is not discarded, which would punch the
  file's blocks out, and its file is allocated again instead.
- **The HDD leg's SSD.** The SSD leg's device holds Shoal's rows, WAL and journal and Ceph's DB,
  as each store would be deployed on disks. MinIO and RustFS have no such split. SeaweedFS's index
  files and filer could go there, but a put syncs neither, and both stayed on the disk
  ([on disks](#on-disks)). The disks' write caches are off, as F91's deploy keeps them, for every store.
  Shoal's 2+1 pool on disks writes `accept_rebuild_exposure: true`. Without it, F91 refuses a
  rotational pool that survives only one loss
  ([F91](../features/rotational-devices.md)), and the HDD leg's quick pass met that refusal. MinIO,
  RustFS and Ceph keep 2+1 on the same disks without being asked.
- **The loop leg** is three instances of every store on europa alone, each on a 50 GiB loop
  device of its own over the Optane, with no link between them. Ceph's three OSDs place by OSD
  there, not by host. SeaweedFS's loop filesystems are mounted with `allocsize=64k`, and SeaweedFS
  unsynced is not run there ([without the link](#without-the-link-europas-optane-over-loopback)).
- **The hosts.** Governor `performance` on every host, put back after each leg; `shoal-tmdb`,
  `e2scrub_all.timer` and `fstrim.timer` stopped, and started again after. Kernel 7.0.0-34 on every
  host, but europa's for Shoal's second run, 7.0.0-38; Ubuntu 26.04.
- **The driver** runs on europa, which is also a store host, pinned to cpus 12-15 and 28-31
  (13-15 and 29-31 on the loop leg). Its streams are dealt round an S3 store's endpoints with
  europa's first, so at one in flight every request reaches europa's own gateway over loopback,
  and at sixteen six streams of the sixteen do; what a gateway does behind it crosses the link
  wherever the store placed the object. Shoal has no gateway: its client sends each object to its
  group's leader, wherever that is, at every depth. No store was given a load balancer in front of
  it.

### Each store as deployed

| Store | Version | Built | Processes | Endpoints |
| --- | --- | --- | --- | --- |
| Shoal | `32bb0ad9` for its first run, `257592b3` for its second | the tmdb node for `znver1`, its sha256 in each capture | a node a host: five shards and two object executors on titan and hyperion, six and two on europa, as `tmdb_cluster.yaml` | none: a client routes to each group's leader |
| Ceph | v20.2.0 | the release's image, `quay.io/ceph/ceph@sha256:1228c3d0…`, by cephadm | a mon and a mgr on europa; an OSD and an RGW on every host; the bucket's index and data pools made with 32 placement groups each, the autoscaler off for them; every RGW's frontend `beast port=8080 tcp_nodelay=1` | the three RGWs, port 8080 |
| MinIO | RELEASE.2025-10-15T17-29-55Z | built from the archived repository's last tag (`9e49d5e`) with Go 1.24.8, since none of its binaries were published | a server a host, one drive each: one erasure set of three, its default parity EC:1 | the three servers |
| RustFS | 1.0.3 | the release's `rustfs-linux-x86_64-gnu` zip, sha256 `0cb0924d…` | as MinIO, its default parity EC:1 | the three servers |
| SeaweedFS | 4.48 | the release's `linux_amd64`, md5 `86707680…` | a master and a filer on europa; a volume server and an S3 gateway on every host, replication `002` | the three gateways |

Every store but Shoal runs unpinned, on every cpu its host has, at its own defaults except where
this table and the next say otherwise. Shoal runs on the cores its inventory gives it, which on
titan and hyperion is all eight threads but cpu 0's pair. It keeps `tmdb_cluster.yaml`'s group
commit delay too, `wal_commit_delay`: 3 ms on europa and 2 ms on titan and hyperion, and 3 ms on
every node of the loop leg. A shard's WAL writer that has just synced waits that long for more
appends before it takes its next batch
([O61](../appendix/optimizations.md#o61-a-fast-device-syncs-the-wal-in-batches-too-small-to-fill-a-page)).
~~How much of a small put's time that is was not measured.~~ Measured since, it cost small puts
nothing on the SSDs, and on europa's loopback they ran 3.4 times as fast at one in flight with no
delay ([the lab's group commit delay](store-optimizations/group-commit-delay.md)). Since
[F98](../features/commit-delay-bounded-by-the-sync.md) a writer waits no longer than its last batch
took to write and sync, the setting at most, and Shoal's second run kept these settings under it.

**Ceph's two settings, and what its defaults did.** The first SSD leg ran Ceph as bootstrapped,
and its capture was thrown away for two reasons:
- **One placement group.** RGW makes a bucket's pools when it first needs them, under the
  autoscaler, and the autoscaler starts a new pool with one placement group
  (`osd_pool_default_pg_autoscale_mode`, `src/common/options/global.yaml.in:2757`). That one
  group's primary was europa's OSD, so:
  - every read came off it over loopback, 715 MiB/s for 1 MiB gets at one in flight, behind a
    1 GbE link;
  - it sent both copies of every put over europa's one link.

  With 32 groups each, Ceph's own `osd_pool_default_pg_num`, every OSD leads 8 to 14 of each
  pool's. The deploy refuses a pool with an OSD that leads none.
- **Nagle's algorithm.** Beast keeps it on unless told otherwise (`doc/radosgw/frontends.rst`).
  A 4 KiB get at one in flight then waited out the driver's delayed acknowledgement, which gave
  24 gets a second at a p99 of 43 ms. With it off, as every other store and the driver have it, a
  quick try gave about 2,500 a second at 0.6 ms.

An operator would meet both. The comparison is of Ceph without them.

**What a put has done when it is answered**, each read in the version deployed:

| Store | Acknowledged once | Read at |
| --- | --- | --- |
| Shoal | its stripe is durable on `k + f` holders and its row committed, `f` being one unless a pool sets it: two of three copies, and all three chunks of 2+1; an object of 16 KiB or less is a row, committed through its tablet group | [F86](../features/replicated-pools.md#a-put-past-the-threshold) |
| Ceph | every shard of the acting set has committed it to disk | `src/osd/ReplicatedBackend.cc:698-700`, `src/osd/ECCommonL.cc:881-888` ([X14](ceph-and-s3-sources.md#1-what-an-acknowledgement-waits-for)) |
| MinIO | each shard and its `xl.meta` written with O_DIRECT and `fdatasync` on a write quorum of drives, two of three; the rename that publishes them is synced only under `MINIO_FS_OSYNC`, which is off | `cmd/xl-storage.go:2131-2195`, `:2557` |
| RustFS | `strict`, its default: shards, `xl.meta` and the commit's directory entries all synced | `docs/operations/durability-modes.md` |
| SeaweedFS | by default nothing is synced. With a path's `fsync` rule, which the compared captures set, each volume's `.dat` is synced before the answer, in batches; the filer's entry is written to leveldb unsynced either way | `weed/storage/volume_write.go:261-300`, `weed/filer/leveldb2/leveldb2_store.go:97` |

SeaweedFS's default is measured beside its synced setting as *SeaweedFS, unsynced*, and judged
against nothing: it is not durable on acknowledgement, and no other store was run that way.

### What differs by design

| | Shoal | Ceph RGW | MinIO | RustFS | SeaweedFS |
| --- | --- | --- | --- | --- | --- |
| Language, runtime | Rust, a glommio thread a core | C++ | Go | Rust, tokio | Go |
| A small object | 16 KiB or less in its path's row | a RADOS object and a bucket index entry | 128 KiB or less in its `xl.meta` | 128 KiB or less in its `xl.meta` | a needle in a volume, an entry in the filer |
| A larger object | 4 MiB stripes, ~~each a whole chunk on each holder ([O107](../appendix/optimizations.md#o107-a-stripe-shorter-than-its-chunk-costs-its-device-the-whole-chunk))~~ each chunk in a file sized to the units it holds since [F96](../features/chunk-file-classes.md) | RADOS objects of 4 MiB | shards in a part file | shards in a part file | needles of 4 MiB chunks |
| Copies kept | at write time | at write time | none | none | at write time |
| Codes | at write time | at write time | at write time | at write time | of sealed volumes, offline |
| Writes in place | yes | no | no | no | no |
| Listing | none | yes | yes | yes | yes |
| A client reaches | its group's leader, by topology | any RGW, which forwards | any server | any server | any gateway, and the one filer |
| Reads cached in | the store's own memory for rows; chunks read with direct I/O, and since [F107](../features/cache-default-share.md) the units its reads verified, in a cache of an eighth of node memory | BlueStore's own cache, in each OSD | the page cache | the page cache | the page cache |

## How to read the charts

- **Ratio charts** draw every arm a row and every other store a dot: its median rate over Shoal's,
  with a whisker from its slowest run over Shoal's fastest to its fastest over Shoal's slowest.
  Right of 1× is faster than Shoal, and a whisker that crosses 1× is within.
- **Size charts** draw a line a store across the four sizes, on log axes, at sixteen in flight
  unless they say one.
- **Verdict tables** give each store's median and interval, and beside each other store whether
  Shoal is ahead, within or behind, by at least how much: a share of the other store's nearer end,
  rounded down, or once one end is twice the other or more, the factor between them, either way
  round. *Shoal behind ≥6.2×* on a rate is a Shoal rate at most a sixth of the other's, and on a
  p99 a Shoal p99 at least 6.2 times the other's. A dash is an arm a store did not run. A run that
  failed an operation measured something else, and is left out of its arm's median and interval;
  its store's cell says how many were.
- A store is one colour on every chart: Shoal, Ceph RGW, MinIO, RustFS, SeaweedFS and SeaweedFS
  unsynced, in that order on every legend, with *Shoal, first run* after Shoal.
- **Shoal, first run** is Shoal as the comparison first measured it, kept as the record and judged
  against nothing. Every chart and table draws it beside today's Shoal, and each ratio chart sets it
  against today's Shoal as a dot with its whisker: left of 1× is an arm Shoal got faster at since.

## At a glance, SSDs over 1 GbE

### Three copies by host

{{#include store-comparison/ssd-rep3-glance.md}}

### Erasure coded 2+1 by host

{{#include store-comparison/ssd-ec21-glance.md}}

## Where Shoal is better, and where worse

From the SSD leg's verdict tables, one row a kind of arm. Each store's verdicts are at one in
flight, then at sixteen. *Within* means the two stores' run intervals overlap, and every gap is a
lower bound. Each cell gives the first run's verdict struck through and the second run's after it.
What Shoal could change, ordered and each with the benchmark that would judge it, is
[Optimizations the comparison points to](store-optimizations.md).

| Arms | Three copies by host | Erasure coded 2+1 by host | Why |
| --- | --- | --- | --- |
| **Puts, 4 KiB** | ~~Ceph within, then behind ≥2.9%. SeaweedFS behind ≥26%, then within~~ Ceph within, within. SeaweedFS behind ≥24%, then within | ~~Ceph within, within. MinIO ahead ≥3.2×, ≥80%. RustFS ahead ≥2.1×, ≥2.6×~~ Ceph within, within. MinIO ahead ≥3.5×, ≥94%. RustFS ahead ≥2.3×, ≥2.8× | In Shoal a 4 KiB object is a row, committed through its tablet group once two of its three copies have synced it. Ceph's put also waits for every shard to commit, and the two are within each other. SeaweedFS's synced put syncs its volume's `.dat`, in batches, and writes the filer's entry unsynced: one durable write fewer than Shoal's. MinIO's puts wait out a lock retry ([below](#minios-4-kib-puts-wait-out-a-lock-retry)). RustFS's strict mode syncs each put's shards, its `xl.meta` and its commit's directories. At one in flight its put took 14 ms at p50, with its devices a third busy and its hosts' cpus 8% busy. Measured again Shoal's 4 KiB puts ran 1.03 to 1.06 times as fast as its first run's, and with three copies came within Ceph at sixteen in flight |
| **Puts, 64 KiB** | ~~Ceph behind ≥2.9×, ≥6.3×. SeaweedFS behind ≥3.2×, ≥7.6×~~ Ceph behind ≥48%, ≥31%. SeaweedFS behind ≥2.1×, ≥43% | ~~Ceph behind ≥2.6×, ≥4.2×. MinIO behind ≥3.5×, ≥3.2×. RustFS behind ≥2.9%, ≥49%~~ Ceph behind ≥2.2×, ≥41%. MinIO behind ≥3.0×, ≥22%. RustFS within, within | ~~[O107](../appendix/optimizations.md#o107-a-stripe-shorter-than-its-chunk-costs-its-device-the-whole-chunk): every stripe takes a whole 4 MiB file on each holder, whatever it holds, and the pool's refill zero-writes another to replace it. At sixteen in flight Shoal wrote 196 device bytes for each byte put with three copies and 67 at 2+1, where Ceph wrote 3.5 and 2.1. Each host's device wrote 208 MiB/s and was 96% busy~~ Since [F96](../features/chunk-file-classes.md) a stripe's chunk takes a file sized to its units, made and synced by itself, and nothing is written ahead. At sixteen in flight Shoal wrote 4.8 device bytes for each byte put with three copies and 3.4 at 2+1, where Ceph wrote 3.5 and 2.1, and its puts ran 2.7 to 4.7 times as fast as its first run's. Titan's and hyperion's SSDs were still 93% busy, at 24 MiB/s, where Ceph's were 90% busy at 39 MiB/s: their writes are many and small, and which of a put's writes they are is not counted yet ([O124](../appendix/optimizations.md#o124-a-64-kib-put-keeps-its-970-evo-busy-at-a-tenth-of-its-bytes)) |
| **Puts, 1 MiB** | ~~Ceph behind ≥11%, ≥22%. SeaweedFS behind ≥25%, ≥33%~~ Ceph within, then ahead ≥6.0%. SeaweedFS behind ≥15%, ≥6.5% | ~~Ceph behind ≥28%, ≥8.8%. MinIO behind ≥20%, ≥17%. RustFS behind ≥27%, ≥13%~~ Ceph behind ≥12%, then ahead ≥2.7%. MinIO behind ≥3.0%, ≥3.3%. RustFS behind ≥12%, then within | ~~O107 again, at 15 device bytes a byte with three copies (Ceph 3.0) and 6.1 at 2+1 (Ceph 1.6). With three copies each host's device wrote 238 MiB/s at 96% busy, for 46 to 52 puts a second~~ F96 again: 3.1 device bytes a byte with three copies (Ceph 3.0) and 2.1 at 2+1 (Ceph 1.6), and puts 1.26 to 1.52 times as fast as the first run's. With three copies titan's and hyperion's SSDs were 83% busy at 72 MiB/s, for 71.5 to 73.6 puts a second |
| **Puts, 16 MiB** | ~~Ceph within, then ahead ≥2.2%. SeaweedFS within, then ahead ≥0.7%~~ Ceph ahead ≥3.4%, then within. SeaweedFS ahead ≥3.4%, then within | ~~Ceph ahead ≥8.7%, ≥15%. MinIO behind ≥15%, then within. RustFS within, within~~ Ceph ahead ≥20%, ≥15%. MinIO behind ≥7.6%, then within. RustFS within, within | Bounded by the network. Each store takes the object in over europa's one 1 GbE link and sends its copies or shards on over the hosts' links. ~~Shoal writes six device bytes a byte with three copies: each chunk's bytes once, and once more as the zeros the pool's refill writes into a file to replace the one the chunk took ([O102](../appendix/optimizations.md#o102-a-whole-chunk-costs-its-device-twice-its-bytes)). The 970 EVO keeps up.~~ Since F96 nothing is written ahead, and Shoal writes 3.0 device bytes a byte with three copies, as Ceph does. Why MinIO leads at one in flight was not looked into |
| **Gets and stats, 4 KiB** | ~~Gets: Ceph behind ≥7.1%, ≥3.2%; SeaweedFS behind ≥7.4%, ≥13%. Stats: Ceph behind ≥16%, ≥16%; SeaweedFS behind ≥2.2×, ≥2.3×~~ Gets: Ceph within, then ahead ≥24%; SeaweedFS within, then ahead ≥15%. Stats: Ceph behind ≥5.2%, then ahead ≥13%; SeaweedFS behind ≥2.0×, ≥25% | ~~Gets: Ceph within, within; MinIO ahead ≥38%, ≥10%; RustFS ahead ≥92%, ≥2.1×. Stats: Ceph behind ≥20%, ≥21%; MinIO ahead ≥14%, then behind ≥8.1%; RustFS ahead ≥2.1×, ≥2.2×~~ Gets: Ceph ahead ≥15%, ≥60%; MinIO ahead ≥61%, ≥83%; RustFS ahead ≥2.2×, ≥3.5×. Stats: Ceph behind ≥4.1%, then ahead ≥14%; MinIO ahead ≥32%, ≥39%; RustFS ahead ≥2.4×, ≥3.8× | Every Shoal get and stat here is a strong read, the server's default for an operation that names no level. The leader of the group holding the path's row confirms its lead with a quorum before it answers ([O118](../appendix/optimizations.md#o118-a-striped-get-pays-two-strong-reads-its-entrys-and-its-stripes-rows), [O49](../appendix/optimizations.md#o49-one-barrier-per-group-per-bundle-rather-than-per-read)). Ceph's primary OSD answers from its own state. SeaweedFS's stat is answered by its filer, on europa, and never reaches a volume server. RustFS is cpu bound: at 4 KiB gets at sixteen in flight, titan's and hyperion's cpus were 93% busy. ~~Shoal's get costs the least cpu of any store: 0.69 to 0.75 cpu-seconds a thousand, against Ceph's 0.97 to 1.15~~ Measured again, still strong reads, they ran 1.13 to 1.18 times as fast as the first run's at one in flight and 1.58 to 1.75 times at sixteen; what moved them was not separated. Shoal's get still costs the least cpu of any store: 0.38 to 0.41 cpu-seconds a thousand at sixteen in flight, against Ceph's 0.82 to 0.96, where the first run's was 0.57 to 0.63 by the same measure (below) |
| **Gets, 64 KiB, 1 MiB and 16 MiB, one in flight** | ~~Ceph behind ≥49%, ≥11%, ≥8.3%. SeaweedFS behind ≥44%, ≥0.9%, ≥2.4%~~ Ceph behind ≥28%, then ahead ≥22%, ≥3.9%. SeaweedFS behind ≥20%, then ahead ≥35%, ≥10% | ~~Ceph behind ≥35%, ≥20%, ≥17%. MinIO behind ≥47%, ≥46%, ≥49%. RustFS behind ≥32%, ≥23%, ≥25%~~ Ceph behind ≥13%, then within, then behind ≥6.3%. MinIO behind ≥28%, ≥30%, ≥42%. RustFS behind ≥8.5%, ≥1.0%, ≥15% | A striped get makes two strong reads, one for its entry and one for its stripes' rows ([O118](../appendix/optimizations.md#o118-a-striped-get-pays-two-strong-reads-its-entrys-and-its-stripes-rows)), then reads its range off the device with direct I/O, ~~every time~~ unless its cache holds it. With 1,024 objects of each size preloaded (64 MiB at 64 KiB, 1 GiB at 1 MiB), a run reads each object many times, and every other store serves most of those reads from memory. ~~At 64 KiB the others read 2 to 16% of what they served off their devices; Shoal read 107 to 184%.~~ Since [F107](../features/cache-default-share.md) Shoal keeps the units its reads verified in a cache of an eighth of node memory, 1 GiB here: at 64 KiB the others read 2 to 16% of what they served off their devices, and Shoal 4 to 31%, where its first run read 107 to 184%. Its gets at one in flight ran 1.18 to 1.59 times as fast as its first run's. At 2+1, Shoal's coordinator gathers the data chunks and sends them on, so when the coordinator is elsewhere europa's own chunk goes out and comes back |
| **Gets, 64 KiB, 1 MiB and 16 MiB, sixteen in flight** | ~~Ceph ahead ≥35%, ≥60%, ≥46%. SeaweedFS ahead ≥25%, ≥44%, ≥26%~~ Ceph ahead ≥64%, ≥96%, ≥70%. SeaweedFS ahead ≥52%, ≥77%, ≥46% | ~~Ceph ahead ≥12%, ≥23%, ≥20%. MinIO ahead ≥13%, then behind ≥1.1%, ≥1.5%. RustFS ahead ≥15%, ≥16%, then within~~ Ceph ahead ≥20%, ≥24%, ≥25%. MinIO ahead ≥21%, then within, within. RustFS ahead ≥23%, ≥16%, ≥4.2% | Every store is bounded by europa's link, at 113 MiB/s in. A Shoal get goes to the leader of its row's group, which reads its own copy first, so a get led from europa is served over loopback beside the link. At 1 MiB gets with three copies europa's loopback carried ~~111 MiB/s for Shoal~~ 124 to 151 MiB/s for Shoal measured again, its cache answering europa's own gets, and 44 for Ceph. A driver beside a member is the lab's shape: a client on a host of its own would see every store at its link. At 2+1 the gather takes most of the advantage away |
| **Half puts, half gets** | ~~Follows its put side. At 64 KiB Ceph behind ≥2.7×, ≥7.0× and SeaweedFS behind ≥2.9×, ≥8.1×. At 16 MiB within, then ahead of Ceph ≥11%~~ Follows its put side. At 64 KiB Ceph behind ≥47%, ≥32% and SeaweedFS behind ≥2.0×, ≥41%. At sixteen in flight ahead of Ceph from 1 MiB, by ≥38% and ≥11%, and of SeaweedFS at 1 MiB, by ≥8.4% | ~~Follows its put side. At 4 KiB ahead of MinIO and RustFS by ≥79% to ≥3.5×. At 64 KiB behind every store, by ≥4.6% to ≥4.0×. At 16 MiB at sixteen in flight ahead of Ceph ≥31%~~ Follows its put side. At 4 KiB ahead of MinIO and RustFS by ≥89% to ≥3.7×. At 64 KiB behind Ceph and MinIO, by ≥30% to ≥3.1×, and within RustFS. At sixteen in flight ahead of Ceph from 1 MiB, by ≥22% and ≥34% | As the puts. ~~A get also waits behind whole-chunk writes on its device: at 64 KiB, one in flight, 2+1, Shoal's get p99 was up to 44 ms, where the same get without puts beside it had a p99 of 3.6 to 3.8 ms~~ A get no longer waits behind whole-chunk writes: at 64 KiB, one in flight, 2+1, Shoal's get p99 was 4.3 to 4.7 ms, where the same get without puts beside it had 2.9 to 3.2 ms |
| **p99 of small reads, sixteen in flight** | ~~Gets of 4 KiB: Ceph behind ≥2.1×, SeaweedFS behind ≥2.9×. Stats: ≥2.5×, ≥6.9×~~ Gets of 4 KiB: Ceph within, SeaweedFS behind ≥13%. Stats: Ceph within, SeaweedFS behind ≥2.6× | ~~Gets of 4 KiB: Ceph behind ≥90%, MinIO ≥2.5×, RustFS ≥32%. Stats: ≥2.6×, ≥3.7×, ≥35%~~ Gets of 4 KiB: Ceph within, MinIO behind ≥9.1%, RustFS ahead ≥20%. Stats: Ceph behind ≥7.2%, MinIO ≥49%, RustFS ahead ≥20% | ~~Shoal's p99 was 5.8 to 7.5 ms against a mean of about 1.1 ms an operation, and Ceph's was 1.8 to 3.3 ms. This was not diagnosed~~ Shoal's p99 was 2.2 to 3.6 ms against a mean of about 0.7 ms an operation, where its first run's was 5.8 to 7.5 ms, and Ceph's was 1.8 to 3.3 ms. The first run's tail was placed since on a strong read's round, on the Zen1 hosts' busy shard loops ([small reads have a tail across hosts](store-optimizations/small-read-tail.md)). Every read here was still a strong read, and what halved the tail was not separated |
| **What each byte cost** | ~~Device bytes a byte put: the most from 64 KiB to 16 MiB (above); at 4 KiB 9.8 to 11.1, Ceph 10.3 to 11.5, SeaweedFS 5.5 to 5.9~~ Device bytes a byte put: at 64 KiB 4.7 to 4.8, Ceph 3.5, SeaweedFS 3.2, still the most; from 1 MiB 3.0 to 3.1, as theirs; at 4 KiB 10.3 to 12.1, Ceph 10.3 to 11.5, SeaweedFS 5.5 to 5.9 | ~~At 4 KiB the fewest: 9.8 to 10.7, Ceph 11.8 to 12.2, MinIO 17.9 to 28.1, RustFS 18.4 to 19.6~~ At 4 KiB the fewest, by a little: 10.3 to 11.8, Ceph 11.8 to 12.2, MinIO 17.9 to 28.1, RustFS 18.4 to 19.6. At 64 KiB 3.3 to 3.5, Ceph 2.1, MinIO 2.8 to 3.9, RustFS 2.8 to 3.4; from 1 MiB 1.5 to 2.1 | ~~Store cpu-seconds a thousand operations at sixteen in flight: 5.9 to 6.1 a 4 KiB put (Ceph 4.7 to 5.8, MinIO 4.6 to 4.9, SeaweedFS 3.2 to 3.5, RustFS 35 to 40), 0.67 to 0.72 a stat (SeaweedFS 0.36 to 0.38, every other store more). Peak memory: Shoal 2.4 to 2.5 GiB on every host.~~ Store cpu-seconds a thousand operations at sixteen in flight, as the charts take them: 4.1 to 4.3 a 4 KiB put (Ceph 3.9 to 4.7, MinIO 3.7 to 4.0, SeaweedFS 2.6 to 2.9, RustFS 29 to 33), 0.36 to 0.39 a stat (SeaweedFS 0.30 to 0.32, every other store more). The first run's figures on this page took a run's cpu over its thirty measured seconds' operations, about a fifth more than the charts', which take the operations over the whole time the counters cover. Peak memory: Shoal 3.5 to 3.6 GiB on every host, 1 GiB of it its cache, where its first run's was 2.4 to 2.5. Ceph's OSD and RGW 2.5 to 3.0 GiB on titan and hyperion, and 3.6 to 5.1 GiB on europa beside its mon and mgr. SeaweedFS 2.6 GiB on europa, with its master and filer, and 0.8 GiB elsewhere. RustFS 1.0 to 1.1 GiB. MinIO 0.6 to 0.8 GiB |

### MinIO's 4 KiB puts wait out a lock retry

Every MinIO put of 4 KiB had a p99 of 302 to 346 ms against a p50 near 5 ms, in all four runs of
every arm that put 4 KiB objects. Its 64 KiB puts at sixteen in flight had a p99 of 214 to 284 ms.
The cause was read in the deployed release (`RELEASE.2025-10-15T17-29-55Z`) and confirmed by two
runs of its own afterwards, kept apart from the compared captures under
`target/lab/stores/captures-try/ssd-diag/`:
- **A put reads the object's info before it writes**, even in a single pool
  (`cmd/erasure-server-pool.go:1092-1093`, `:516`). It does so under a read lock on the object,
  held at all three servers' lockers.
- **dsync releases a read lock asynchronously** (`internal/dsync/drwmutex.go:683-694`), and the
  put's write lock follows at once. When the write lock reaches the other two lockers before the
  release does, it has one grant of the two it needs. It gives that grant back and sleeps for a
  uniform draw of up to 250 ms plus 100 ms an attempt (`drwmutex.go:48-60`, `:270`;
  `utils.go:25-38`).

With dsync's trace on, a 20 second run of 4 KiB puts at one in flight logged 69 "Unable to acquire
lock in quorum" on europa. Each was a `putObject` on the object whose `GetObjectInfo` read lock had
been granted on the line before. With the retry's floor set to 10 ms
(`_MINIO_LOCK_RETRY_INTERVAL=10`) the p99 fell to 102 ms at one in flight and 116 ms at sixteen,
under the new ceiling of 110 ms. Puts ran at 116 and 782 a second, against the compared capture's
38 to 46 and 636 to 653. The compared captures keep MinIO's default, which is what an operator
deploys.

### SeaweedFS's fourth run of a 4 KiB put

SeaweedFS's synced put of 4 KiB at sixteen in flight ran at 1,389, 1,408 and 1,403 a second, then
622 in its fourth run, steadily for the whole run. That run widens its interval to within Shoal's.
The settle before it, `volume.vacuum`, made volumes 7, 8 and 9 unwritable for 12 seconds and
finished 7 seconds before the run began. Every other run had such a settle too, and none of them
slowed. It is reported as measured.

## What was said in advance

What `stores-predictions.md` expected before the first leg ran, beside what was measured: items
1 to 10 on the SSD leg, item 11 on disks. None of these is a verdict. What was measured is Shoal's
first run, which the statements were made of; its second run is in the tables above.

| | Expected | What was measured |
| --- | --- | --- |
| 1 | A put of 64 KiB to 1 MiB slower on Shoal than on every store, by at least half | Slower than every store at both sizes and both depths. By half or more only at 64 KiB, and not even there beside RustFS at one in flight (≥2.9%). At 1 MiB, by ≥8.8% to ≥33% |
| 2 | A 4 KiB put no slower on Shoal | Wrong. Ceph is ahead by ≥2.9% at sixteen in flight with three copies, and SeaweedFS's synced put by ≥26% at one in flight. Shoal is within Ceph at 2+1 and ahead of MinIO and RustFS |
| 3 | A 4 KiB stat fastest on Shoal | Wrong. Ceph is ahead in both groups (≥16% to ≥21%), SeaweedFS by ≥2.3× and MinIO by ≥8.1% at sixteen in flight. Shoal is ahead of RustFS |
| 4 | At 16 MiB, puts bounded by the network, no two stores of a group more than a quarter apart | Held. The medians are within 11% of each other with three copies and 23% at 2+1, where Ceph is the slowest |
| 5 | Gets of 16 MiB at sixteen in flight: no store ahead of Shoal by more than a quarter | Held. With three copies Shoal is ahead of both; at 2+1 MinIO is ahead by ≥1.5% and RustFS is within |
| 6 | On the loop leg the differences grow, and Shoal's put deficit at 64 KiB to 1 MiB is its largest there | Held, both ways. Its 1 MiB put deficit is its largest on any leg beside every store: with three copies at sixteen in flight, Ceph ahead by at least 4.0 times and SeaweedFS by 4.1, against 22% and 33% on SSDs. Its small reads went the other way, from behind Ceph by at least 3.2% to 21% across hosts to ahead of every store by at least 31%, but SeaweedFS's stats at sixteen in flight. The leg ran no 64 KiB arm |
| 7 | Shoal's p99 at one in flight within twice the best store's, but on item 1's puts | Wrong for gets past the inline threshold and for stats. At 64 KiB gets Shoal's was 2.76 to 2.98 ms against Ceph's 1.21 to 1.32, and 3.57 to 3.79 ms against MinIO's 1.36 to 1.37. At stats it was 0.67 to 0.71 ms against SeaweedFS's 0.30 to 0.32. Held at 4 KiB gets and puts |
| 8 | Shoal's device bytes a byte put the most from 64 KiB to 1 MiB, the fewest at 4 KiB | The first half held: 196 and 15 with three copies, 67 and 6.1 at 2+1, against every other store's 1.6 to 3.9. The second held at 2+1 and not with three copies, where SeaweedFS wrote 5.5 to 5.9 and Ceph's interval overlaps Shoal's |
| 9 | Ceph's OSDs hold the most memory, SeaweedFS's processes the least | Half right. Ceph's is the most, but MinIO's is the least, at 0.6 to 0.8 GiB a host |
| 10 | Shoal's cpu a thousand 4 KiB operations the lowest, Ceph's the highest | Half right. Shoal's get is the cheapest of any store. Its stat is second to SeaweedFS's, and its put costs more than every store's but RustFS's. RustFS's is the highest, not Ceph's |
| 11 | On disks, 4 KiB puts ten times slower but on Shoal and Ceph | Half right. At sixteen in flight Shoal's medians were 1,215 a second on SSDs and 1,222 on disks, and Ceph's fell 3.4 times, from 1,310 to 389, not ten. Of the three that sync on the disk, SeaweedFS fell 10.8 times (1,396 to 129), MinIO 9.7 times (639 to 65.7) and RustFS 7.1 times (448 to 63.1). MinIO's fall was the test set for the statement, and it fell short of ten by a hair, so the statement is wrong on its own terms |

## In detail, SSDs over 1 GbE

### Three copies by host

{{#include store-comparison/ssd-rep3-detail.md}}

### Erasure coded 2+1 by host

{{#include store-comparison/ssd-ec21-detail.md}}

## Without the link: europa's Optane over loopback

Run on 2026-10-19 between 13:10 and 21:09 UTC, and Shoal's second run on 2026-10-26 between 10:12
and 13:19: three instances of every store on europa alone, each on a 50 GiB loop device of its own
over the Optane 900P, and no link between them. The arms are the SSD leg's at 4 KiB and 1 MiB.
Shoal's verdict beside each store, at one in flight, then at sixteen, the first run's struck
through:

| Arms | Three copies | Erasure coded 2+1 | Why |
| --- | --- | --- | --- |
| **Puts, 4 KiB** | ~~Ceph behind ≥2.0×, ≥46%. SeaweedFS behind ≥2.2×, ≥2.7×~~ Ceph ahead ≥38%, ≥45%. SeaweedFS ahead ≥20%, then within | ~~Ceph behind ≥47%, ≥42%. MinIO ahead ≥8.3×, ≥3.7×. RustFS ahead ≥2.2×, ≥3.0×~~ Ceph ahead ≥54%, ≥56%. MinIO ahead ≥26.5×, ≥10.6×. RustFS ahead ≥7.1×, ≥8.7× | ~~Shoal's small put took 2.4 ms at the median at one in flight, Ceph's 1.0 ms, at the same rate in both of Shoal's groups, since a 4 KiB object is a row. Shoal's put cost the least cpu of any store, 0.95 to 1.00 cpu-seconds a thousand against Ceph's 3.1 to 3.6, so it waits rather than works. What it waits on was not measured ([the lab's group commit delay](store-optimizations/group-commit-delay.md))~~ Shoal's small put took 0.58 to 0.68 ms at the median at one in flight, where its first run's took 2.4, and Ceph's 1.0 ms, at the same rate in both of Shoal's groups, since a 4 KiB object is a row. What it waited on was the WAL writer's group commit delay, 3 ms on this leg ([the lab's group commit delay](store-optimizations/group-commit-delay.md)). Since [F98](../features/commit-delay-bounded-by-the-sync.md) a writer waits no longer than its last batch took to write and sync, which on the Optane is little. Its put still costs the least cpu of any store, 0.69 to 0.74 cpu-seconds a thousand at sixteen in flight against Ceph's 2.6 to 3.1, as the charts take them |
| **Puts, 1 MiB** | ~~Ceph behind ≥2.4×, ≥4.0×. SeaweedFS behind ≥27%, ≥4.1×~~ Ceph behind ≥8.4%, ≥27%. SeaweedFS ahead ≥56%, then behind ≥28% | ~~Ceph behind ≥2.2×, ≥2.6×. MinIO behind ≥2.6×, ≥2.8×. RustFS within, then behind ≥20%~~ Ceph behind ≥6.1%, ≥36%. MinIO behind ≥19%, ≥40%. RustFS ahead ≥2.0×, ≥21% | ~~[O107](../appendix/optimizations.md#o107-a-stripe-shorter-than-its-chunk-costs-its-device-the-whole-chunk), its largest gap on any leg: 15 device bytes a byte put with three copies and 6.0 at 2+1, against Ceph's 3.0 and 1.5. All three instances write to the one Optane, which with three copies wrote 724 MiB/s through each loop device, 90% busy, for 149 MiB/s of objects~~ Since [F96](../features/chunk-file-classes.md) Shoal writes 3.1 device bytes a byte put with three copies and 2.1 at 2+1, against Ceph's 3.0 and 1.5, and its puts ran 1.7 to 3.0 times as fast as its first run's. With three copies each loop device wrote 431 MiB/s, 56 to 59% busy, for 439 MiB/s of objects. What bounds them now was not looked into |
| **Gets and stats, 4 KiB** | ~~Gets: Ceph ahead ≥73%, ≥42%; SeaweedFS ahead ≥82%, ≥31%. Stats: Ceph ahead ≥61%, ≥34%; SeaweedFS ahead ≥16%, then behind ≥12%~~ Gets: Ceph ahead ≥87%, ≥2.0×; SeaweedFS ahead ≥97%, ≥85%. Stats: Ceph ahead ≥70%, ≥88%; SeaweedFS ahead ≥23%, ≥21% | ~~Gets: ahead of every store, by ≥62% to ≥3.4×. Stats: ahead of every store, by ≥34% to ≥3.4×~~ Gets: ahead of every store, by ≥2.1× to ≥4.7×. Stats: ahead of every store, by ≥69% to ≥4.8× | A strong read's round to a quorum costs little over loopback, and Shoal's p99 at sixteen in flight was ~~0.79 to 0.86 ms~~ 0.52 to 0.65 ms, below Ceph's 0.87 to 1.12 ([small reads have a tail across hosts](store-optimizations/small-read-tail.md)). The cheapest reads of any store: ~~0.24 to 0.25 cpu-seconds a thousand gets, Ceph's 0.74 to 0.88~~ 0.15 cpu-seconds a thousand gets at sixteen in flight, Ceph's 0.63 to 0.75, as the charts take them |
| **Gets, 1 MiB** | ~~Ceph behind ≥29%, ≥34%. SeaweedFS behind ≥46%, ≥9.1%~~ Ceph ahead ≥33%, ≥52%. SeaweedFS ahead ≥10%, ≥2.1× | ~~Ceph behind ≥47%, ≥40%. MinIO behind ≥2.1×, ≥41%. RustFS behind ≥22%, then ahead ≥42%~~ Ceph within, within. MinIO behind ≥11%, then within. RustFS ahead ≥40%, ≥2.4× | ~~With three copies Shoal read every byte it served off the Optane, 2.3 GB/s through the three devices at 83 to 85% busy, with europa's cpus 18% busy.~~ Since [F107](../features/cache-default-share.md) each node keeps 512 MiB of the units its reads verified, and Shoal's devices read 0.1 to 0.3% of what it served, where its first run read all of it. With three copies it served 5,941 gets a second, 2.5 times its first run's, with europa's cpus 34% busy. SeaweedFS, MinIO and RustFS read 31 to 87 MiB/s off theirs, answering from memory with europa's cpus 60 to 71% busy, and Ceph half of what it served ([a get reads the device every time](store-optimizations/read-cache.md)). ~~At 2+1 Shoal's devices were half busy, and what bounds its gets there was not found~~ What bounds Shoal's gets at 2+1, within Ceph and MinIO, was not looked into |
| **Half puts, half gets** | ~~Follows its put side: behind Ceph and SeaweedFS at every arm, by ≥24% to ≥3.7×~~ Follows its put side: ahead of Ceph and SeaweedFS at 4 KiB, by ≥6.6% to ≥77%. At 1 MiB within Ceph and ahead of SeaweedFS at one in flight, and behind both at sixteen, by ≥19% and ≥20% | ~~Follows its put side: ahead of MinIO and RustFS at 4 KiB by ≥2.4× to ≥8.2×, behind Ceph by ≥29%; at 1 MiB behind every store at sixteen in flight~~ Follows its put side: ahead of every store at 4 KiB, by ≥71% to ≥23.1×. At 1 MiB behind Ceph and MinIO, by ≥3.4% to ≥37%, and ahead of RustFS, by ≥32% and ≥87% | As the puts |
| **What each byte cost** | Peak memory: Shoal ~~4.2 GiB~~ 9.8 GiB for its three nodes, Ceph 14.6 GiB, SeaweedFS 3.1 GiB | Shoal ~~4.2 GiB~~ 9.7 GiB, Ceph 15.0 GiB, MinIO 2.3 GiB, RustFS 4.7 GiB | ~~Store cpu-seconds a thousand 4 KiB operations at sixteen in flight, Shoal the least at every kind: puts 0.95 to 1.00 (SeaweedFS 2.2 to 2.3, MinIO 2.4 to 2.7, RustFS 26 to 29), stats 0.23 (SeaweedFS 0.37 to 0.38)~~ Store cpu-seconds a thousand 4 KiB operations at sixteen in flight, as the charts take them, Shoal the least at every kind: puts 0.69 to 0.74 (SeaweedFS 1.9 to 2.0, MinIO 2.0 to 2.2, RustFS 21 to 24), stats 0.14 (SeaweedFS 0.31 to 0.32). Shoal's peak is each node's cache of 512 MiB and, at the end of the 4 KiB put arm at sixteen in flight, the rows of the objects it put: 9.8 GiB resident then, where its first run's held 4.2 |

Three stores needed help to finish on 50 GiB devices, and none of it touches an arm's clock:
- **SeaweedFS** appends to volumes of up to 4 GiB it keeps open, and XFS holds space past a
  growing file's end until the file is closed: every volume held 8 GiB, and an arm of 1 MiB puts
  filled the devices. A 3 GiB file appended and synced while open held 4,095 MiB on a default mount
  and 3,072 with `allocsize=64k`, which SeaweedFS's loop filesystems were mounted with. Its rates in
  the runs it finished without it were the same: 1 MiB puts at sixteen in flight at 621 to 651 a
  second, against 622 to 644. A vacuum of every volume skips a read-only one, which a full volume
  is, so its settle vacuums each volume by its id.
- **MinIO and RustFS** move what a delete removes into a trash their drives empty in the
  background (MinIO's `cmd/xl-storage.go:1232-1245`), so on this leg their settle waits for the
  space to come back. MinIO's trash was still emptying during the gets that followed its put arms.
- **SeaweedFS unsynced** is not on this leg. At about 760 MiB/s its 1 MiB puts filled the devices
  in one arm three times, the last from 9%.

### Three copies, by instance

{{#include store-comparison/loop-rep3-glance.md}}

{{#include store-comparison/loop-rep3-detail.md}}

### Erasure coded 2+1, by instance

{{#include store-comparison/loop-ec21-glance.md}}

{{#include store-comparison/loop-ec21-detail.md}}

## On disks

Run from 2026-10-18 19:29 to 2026-10-19 12:46 UTC, and Shoal's second run on 2026-10-26 from 03:24
to 10:11, the same arms in the same order as the SSD leg,
every store's data on each host's disk: a WD6001FZWX on europa and a WD140EDFZ on titan and
hyperion, their write caches off. Two stores keep part of themselves on the host's SSD, as each
would be deployed on disks:
- **Shoal** keeps its rows, its WAL and every disk's journal on the SSD ([F91](../features/rotational-devices.md)),
  so an object of 16 KiB or less, which is a row, never reaches a disk.
- **Ceph** keeps each OSD's RocksDB on the SSD, and BlueStore commits a write of less than 64 KiB
  there and applies it to the disk later (`src/os/bluestore/BlueStore.cc:17162`,
  `bluestore_prefer_deferred_size_hdd`).

MinIO, RustFS and SeaweedFS sync every put on the disk. SeaweedFS can keep its volumes' index files
and its filer's store elsewhere (`-dir.idx`, `-defaultStoreDir`), but a put syncs neither: with its
`fsync` rule the one write it waits for is its volume's `.dat` (`weed/storage/volume_write.go:261-300`).
Both were left on the disk.

Read as the [SSD leg's table](#where-shoal-is-better-and-where-worse) is: each store's verdict at
one in flight, then at sixteen, as Shoal's beside it, the first run's struck through.

| Arms | Three copies by host | Erasure coded 2+1 by host | Why |
| --- | --- | --- | --- |
| **Puts, 4 KiB** | ~~Ceph within, then ahead ≥3.0×. SeaweedFS ahead ≥6.1×, ≥8.2×~~ Ceph within, then ahead ≥3.2×. SeaweedFS ahead ≥6.2×, ≥8.8× | ~~Ceph within, then ahead ≥2.9×. MinIO ahead ≥7.0×, ≥17.4×. RustFS ahead ≥7.8×, ≥17.6×~~ Ceph within, then ahead ≥3.1×. MinIO ahead ≥7.6×, ≥18.7×. RustFS ahead ≥8.3×, ≥19.0× | Shoal's rows are on the SSD, and it put 4 KiB objects as fast as on the SSD leg: 1,222 a second at sixteen in flight against 1,215. Ceph commits them in its DB on the SSD and applies them to the disk after, and at sixteen in flight ran a third as fast as on its SSD leg. The others wait for a sync on the disk ([what was said in advance](#what-was-said-in-advance), item 11). Measured again Shoal put 1,283 to 1,288 a second at sixteen in flight, against 1,276 to 1,286 on the SSD leg |
| **Puts, 64 KiB** | ~~Ceph behind ≥2.2×, ≥12.6×. SeaweedFS behind ≥2.9×, ≥5.2×~~ Ceph behind ≥33%, ≥3.7×. SeaweedFS behind ≥48%, ≥34% | ~~Ceph behind ≥13.8×, ≥15.7×. MinIO behind ≥44%, ≥28%. RustFS behind ≥18%, ≥46%~~ Ceph behind ≥9.9×, ≥6.0×. MinIO behind ≥22%, then ahead ≥44%. RustFS within, then ahead ≥1.2% | ~~[O107](../appendix/optimizations.md#o107-a-stripe-shorter-than-its-chunk-costs-its-device-the-whole-chunk) on a disk: a rotational pool's chunks are 4 MiB, and each stripe writes a whole one on each holder. Shoal wrote 197 to 217 device bytes a byte put with three copies and 127 to 132 at 2+1, every disk 96 to 99% busy at 85 to 95 MiB/s.~~ Since [F96](../features/chunk-file-classes.md) a stripe's chunk takes a file sized to its units on a disk too: Shoal wrote 4.5 to 4.8 device bytes a byte put with three copies and 3.3 to 3.5 at 2+1, and its puts ran 2.6 to 3.3 times as fast as its first run's at sixteen in flight. Its disks were still 67 to 93% busy, writing 3.3 to 4.6 MiB/s, so seeks rather than bytes bound them; which of a put's writes seek is not counted yet, as on the SSDs ([O124](../appendix/optimizations.md#o124-a-64-kib-put-keeps-its-970-evo-busy-at-a-tenth-of-its-bytes)). A 2+1 shard of a 64 KiB object is 32 KiB, under Ceph's deferred size, so Ceph's 2+1 puts commit on the SSD. Its three copies are 64 KiB each, written to the disk at once, at 3.4 device bytes a byte |
| **Puts, 1 MiB** | ~~Ceph behind ≥44%, ≥2.7×. SeaweedFS behind ≥43%, ≥3.3×~~ Ceph behind ≥27%, ≥4.2%. SeaweedFS behind ≥25%, ≥21% | ~~Ceph behind ≥47%, ≥2.5×. MinIO within, within. RustFS within, then behind ≥16%~~ Ceph behind ≥29%, ≥10%. MinIO ahead ≥24%, ≥2.3×. RustFS ahead ≥1.3%, ≥40% | ~~O107 again: 15 to 16 device bytes a byte with three copies and 10 at 2+1, against Ceph's 3.0 and 1.5, MinIO's and RustFS's 1.6 to 1.7. MinIO and RustFS sync each shard and its `xl.meta` on the disk, and are within Shoal or a little ahead~~ F96 again: 3.1 device bytes a byte with three copies and 2.1 at 2+1, against Ceph's 3.0 and 1.5, MinIO's and RustFS's 1.6 to 1.7, and puts 1.33 to 2.43 times as fast as the first run's. MinIO and RustFS sync each shard and its `xl.meta` on the disk, and are now behind Shoal |
| **Puts, 16 MiB** | ~~Ceph behind ≥23%, ≥24%. SeaweedFS behind ≥10%, ≥28%~~ Ceph behind ≥6.2%, then ahead ≥0.7%. SeaweedFS within, within | ~~Ceph behind ≥9.9%, then within. MinIO behind ≥4.7%, then within. RustFS behind ≥7.9%, ≥4.5%~~ Ceph ahead ≥2.1%, ≥23%. MinIO within, then ahead ≥4.1%. RustFS within, within | ~~No longer the network's. With three copies Shoal writes each chunk's length twice on the same disk, its bytes and the zeros of the file the pool's refill makes to replace the one it took ([O102](../appendix/optimizations.md#o102-a-whole-chunk-costs-its-device-twice-its-bytes)): 5.8 to 6.1 device bytes a byte, its disks 96 to 100% busy at 74 to 81 MiB/s, where Ceph wrote 3.0 at 80 to 84% busy. At 2+1 Shoal writes 3.0 to Ceph's 1.5 and is within or just behind every store~~ No longer the network's. Since F96 nothing is written ahead: Shoal writes 3.0 device bytes a byte with three copies and 1.5 at 2+1, as Ceph does, its disks 92 to 97% busy at 64 to 71 MiB/s with three copies. It is within every store or ahead, but Ceph with three copies at one in flight |
| **Gets and stats, 4 KiB** | ~~Gets: Ceph behind ≥11%, ≥8.8%; SeaweedFS behind ≥12%, ≥20%. Stats: Ceph behind ≥20%, ≥20%; SeaweedFS behind ≥2.4×, ≥2.5×~~ Gets: Ceph within, then ahead ≥19%; SeaweedFS within, then ahead ≥9.3%. Stats: Ceph behind ≥6.6%, then ahead ≥10%; SeaweedFS behind ≥2.1×, ≥35% | ~~Gets: Ceph within, within; MinIO ahead ≥42%, ≥9.2%; RustFS ahead ≥86%, ≥2.0×. Stats: Ceph behind ≥17%, ≥15%; MinIO ahead ≥14%, then behind ≥6.8%; RustFS ahead ≥2.2×, ≥2.1×~~ Gets: Ceph ahead ≥19%, ≥53%; MinIO ahead ≥77%, ≥78%; RustFS ahead ≥2.3×, ≥3.4×. Stats: Ceph behind ≥2.8%, then ahead ≥12%; MinIO ahead ≥41%, ≥33%; RustFS ahead ≥2.7×, ≥3.6× | As on SSDs: a row read from memory, under a strong read ([O118](../appendix/optimizations.md#o118-a-striped-get-pays-two-strong-reads-its-entrys-and-its-stripes-rows)), at its SSD leg's rate |
| **Gets, 64 KiB, 1 MiB and 16 MiB, one in flight** | ~~Ceph behind ≥9.0×, ≥48%, ≥42%. SeaweedFS behind ≥8.2×, ≥29%, ≥35%~~ Ceph behind ≥35%, then ahead ≥1.2%, then behind ≥18%. SeaweedFS behind ≥29%, then ahead ≥45%, then behind ≥8.9% | ~~Ceph behind ≥4.9×, ≥45%, ≥48%. MinIO behind ≥5.9×, then within, then behind ≥2.6×. RustFS behind ≥4.7×, then ahead ≥33%, then behind ≥40%~~ Ceph behind ≥15%, ≥0.9%, ≥27%. MinIO behind ≥29%, then ahead ≥16%, then behind ≥46%. RustFS behind ≥12%, then ahead ≥2.1×, then behind ≥16% | ~~Shoal reads every get off a disk with direct I/O, a seek and more each: 130 gets of 64 KiB a second with three copies, a p99 of 27 to 45 ms.~~ Since [F107](../features/cache-default-share.md) Shoal answers the 64 KiB and 1 MiB working sets from its cache, its disks reading 5 to 34% of what it served at 64 KiB, and it ran 4.4 to 5.5 times as many 64 KiB gets a second as its first run, at a p99 of 2.8 to 3.9 ms. At 16 MiB the set does not fit, and every get is a seek. The others answered the 64 KiB working set from memory, as on SSDs, reading at most 3 MiB/s off any disk, at a p99 of 1.3 to 2.1 ms |
| **Gets, 64 KiB, 1 MiB and 16 MiB, sixteen in flight** | ~~Ceph behind ≥3.4×, then ahead ≥25%, ≥14%. SeaweedFS behind ≥3.7×, then ahead ≥14%, ≥2.2%~~ Ceph ahead ≥37%, ≥45%, ≥52%. SeaweedFS ahead ≥28%, ≥32%, ≥36% | ~~Ceph behind ≥14%, then ahead ≥12%, ≥8.9%. MinIO behind ≥13%, ≥8.5%, ≥5.6%. RustFS behind ≥12%, then ahead ≥7.1%, then within~~ Ceph ahead ≥10%, ≥16%, ≥13%. MinIO ahead ≥11%, then behind ≥6.5%, ≥3.6%. RustFS ahead ≥13%, ≥11%, then within | ~~With three copies every get is read off its coordinator's own disk ([O119](../appendix/optimizations.md#o119-a-get-from-a-pool-of-copies-reads-its-coordinators-own-copy-so-each-disk-serves-the-gets-its-node-leads)). At 64 KiB europa's disk was 95% busy and titan's and hyperion's 53 to 54%. At 2+1 an object that short lies in one data chunk, wherever placement put it, so all three disks were 72 to 92% busy and Shoal got more than twice as many. From 1 MiB Ceph and SeaweedFS are bounded by europa's link, 112 to 114 MiB/s in. Shoal with three copies served about 73 MiB/s more from europa's own disk over loopback, that disk 88 to 96% busy, and is ahead of both~~ The cache answers the 64 KiB and 1 MiB sets, and Shoal's disks were all but idle, where in its first run every get was read off its coordinator's own disk ([O119](../appendix/optimizations.md#o119-a-get-from-a-pool-of-copies-reads-its-coordinators-own-copy-so-each-disk-serves-the-gets-its-node-leads)), europa's 95% busy. Every store is bounded by europa's link, 106 to 114 MiB/s in, and Shoal served 61 to 79 MiB/s more over europa's loopback from europa's own cache. Why MinIO leads from 1 MiB at 2+1 was not looked into |
| **Half puts, half gets** | ~~Follows its put side. At 4 KiB ahead of Ceph at sixteen in flight by ≥2.7× and of SeaweedFS by ≥6.7×, ≥8.3×. Behind at every other size~~ Follows its put side. At 4 KiB ahead of Ceph by ≥0.9% and ≥2.9× and of SeaweedFS by ≥6.9×, ≥8.8×. Behind at every other size, but within both at 16 MiB at sixteen in flight | ~~Follows its put side. At 4 KiB ahead of every store at sixteen in flight, by ≥2.7× to ≥18.1×. At 64 KiB behind every store, by ≥28% to ≥16.3×~~ Follows its put side. At 4 KiB ahead of every store, by ≥5.8% to ≥19.6×. At 64 KiB behind Ceph by ≥5.8× and ≥8.1×, behind MinIO and RustFS at one in flight by ≥7.6% and ≥1.0% and ahead at sixteen by ≥76% and ≥27%. At 1 MiB ahead of both, by ≥44% to ≥2.5× | As the puts |
| **What each byte cost** | Device bytes a byte put at 4 KiB: Shoal ~~9.7 to 11.0~~ 10.3 to 11.6, Ceph 13.1 to 15.6, SeaweedFS 5.5 to 5.8 | At 4 KiB: Shoal ~~9.7 to 10.7~~ 10.3 to 11.7, Ceph 14.5 to 14.7, MinIO 14.6 to 21.1, RustFS 17.6 to 20.6 | Cpu and memory as on SSDs: Shoal's 4 KiB get the cheapest of any store, at ~~0.69 to 0.76 cpu-seconds a thousand~~ 0.38 to 0.42 cpu-seconds a thousand as the charts take them, and Shoal's peak ~~2.4 to 3.3 GiB~~ 3.4 to 3.5 GiB a host |

### Three copies by host

{{#include store-comparison/hdd-rep3-glance.md}}

{{#include store-comparison/hdd-rep3-detail.md}}

### Erasure coded 2+1 by host

{{#include store-comparison/hdd-ec21-glance.md}}

{{#include store-comparison/hdd-ec21-detail.md}}

## The driver beside warp

[warp](https://github.com/minio/warp), MinIO's own S3 benchmark, run after the SSD leg against MinIO
at 2+1 and Ceph with three copies, the bucket's default placement. Each store was deployed fresh on
the same devices, warp ran on the driver's cpus at sixteen in flight, and each arm was one run of
30 seconds (`shoal-spike/results/stores/warp/`). Warp puts objects of its own and gets the ones it
put, drops no cache, and reads nothing back. Its figures sit beside the driver's median and runs
for the same arm:

| Arm, c16 | MinIO: warp | MinIO: the driver | Ceph: warp | Ceph: the driver |
| --- | --- | --- | --- | --- |
| put 4 KiB, ops/s | 752, p99 287 ms | 639 [636–653], p99 302–312 ms | 1,272, p99 25.3 ms | 1,310 [1,281–1,324], p99 24.5–25.9 ms |
| put 1 MiB, ops/s | 111, p99 225 ms | 105 [99.4–106], p99 263–270 ms | 67.2, p99 388 ms | 67.1 [67.0–67.5], p99 369–377 ms |
| get 4 KiB, ops/s | 10,752, p99 3.0 ms | 11,432 [11,403–11,485], p99 2.48–2.54 ms | 14,763, p99 2.7 ms | 16,512 [16,278–17,064], p99 2.09–2.65 ms |
| get 1 MiB, ops/s | 132, p99 405 ms | 135 [135–136], p99 357–382 ms | 121, p99 440 ms | 124 [124–124], p99 519–602 ms |

Ceph's puts agree with the driver's: warp's 1 MiB put falls within its runs, and its 4 KiB put is
0.7% below the slowest of them. Warp's gets are slower than the driver's slowest run by 2% to 9%:
MinIO's by 2.2% at 1 MiB and 5.7% at 4 KiB, Ceph's by 2.6% at 1 MiB and 9.3% at 4 KiB. Its puts to
MinIO are faster, by 4.3% at 1 MiB and 15% at 4 KiB, with the same tail of about 300 ms at 4 KiB
([MinIO's lock retry](#minios-4-kib-puts-wait-out-a-lock-retry)), a race that turns on timing and
so moves with the client. Read against warp's figures instead, two verdicts would move to within:
Ceph's 4 KiB gets with three copies, and MinIO's 1 MiB gets at 2+1. ~~The driver puts Shoal behind
both, by at least 3.2% and 1.1%.~~ The driver put Shoal's first run behind both, by at least 3.2%
and 1.1%. Measured again Shoal is ahead of Ceph's 4 KiB gets by either figure, by at least 24%
against the driver's runs, and within MinIO's 1 MiB gets by either.

## Drift

On the SSD leg, Shoal ran first and last in each group, on a cluster deployed fresh each time: its
two three-copy captures began seven and a half hours apart, its two 2+1 captures six.
- **Three copies.** Every put arm's two intervals overlap. Gets ran 8 to 15% slower in the second
  capture: 12,514 to 14,071 gets of 4 KiB a second at sixteen in flight against 15,343 to 15,753,
  and 199 to 205 of 1 MiB against 224 to 231. Europa's link carried 113 MiB/s in at 1 MiB in both,
  but its loopback carried 111 in the first and 86 in the second. A get goes to the leader of its
  row's group, which reads its own copy. Which groups europa leads is drawn again from the node
  ids a fresh deployment mints, so the share of gets europa served itself, beside the link, changed
  between captures. The driver dealt its streams over the same members in the same order both
  times (a deployment's nodes by name, `shoaladm/src/bench/orchestrate.rs:222`).
- **2+1.** Every arm's intervals overlap or touch. The widest moves are gets of 4 KiB at sixteen in
  flight (13,063 to 13,343, then 12,741 to 14,704) and stats (13,706 to 14,080, then 13,748 to
  15,464). Both are rows, read by their group's leader as with three copies. A striped get at 2+1
  reads the data chunk that holds its bytes, on whichever host placement put it, whoever leads.

On disks the two three-copy captures began eight hours apart and the two 2+1 captures six and a
half, and the drift was larger:
- **Three copies.** Twelve arms' intervals are disjoint, and in eleven the second capture was the
  slower: gets by 5 to 23%, 155 to 158 gets of 1 MiB a second at sixteen in flight against 176 to
  178, and puts by 5 to 22%, most at one in flight. At 1 MiB gets europa's loopback carried
  74 MiB/s in the first and 64 in the second, and its disk was 95 to 96% busy in both: with three
  copies europa's disk serves every get europa leads
  ([O119](../appendix/optimizations.md#o119-a-get-from-a-pool-of-copies-reads-its-coordinators-own-copy-so-each-disk-serves-the-gets-its-node-leads)).
- **2+1.** Eighteen arms' intervals are disjoint, and they moved both ways. Puts of 64 KiB to
  16 MiB were 7 to 15% slower in the second capture. Gets of 4 KiB to 1 MiB and stats were 2 to 14%
  faster: 15,016 to 15,128 gets of 4 KiB a second at sixteen in flight against 12,623 to 13,956.
  Why the puts moved was not looked into.

On europa's loopback each pair of captures began about three and a quarter hours apart. Every arm
moved by 2% or less, as it should with all three nodes on one host, where which of them leads a
group decides nothing that crosses a link.

**Measured again**, Shoal's two captures of a group began an hour and forty minutes apart across
hosts and 47 minutes apart on loopback, back to back on a cluster deployed fresh each time:
- **On the SSD leg** six arms of 26 had disjoint intervals in each group. With three copies every
  one was a get or a stat, 2 to 8% slower in the second capture, as with the first run's
  leadership draw. At 2+1 they moved both ways, none by more than 5%.
- **On disks** eight arms with three copies and seven at 2+1. With three copies the gets were 6 to
  14% slower in the second capture, most at 1 MiB and sixteen in flight, 179.6 to 184.0 a second
  against 206.3 to 213.9. At 2+1, gets of 4 KiB and stats at sixteen in flight were 13 to 15%
  slower, and the rest moved by 6% or less.
- **On europa's loopback** no arm moved by more than 4%. With three copies no arm's intervals were
  apart; at 2+1 four were, gets of 4 KiB and stats at one in flight 2% slower in the second capture,
  and gets of 4 KiB and 1 MiB at sixteen in flight 4% faster.

Shoal's interval is the union of both captures, so every verdict above already carries this.

## Every capture

{{#include store-comparison/facts.md}}

## What the comparison does not settle

- **A faster network.** On 1 GbE every store's large objects are the link's. The loop leg takes
  the link away and puts every instance on one host's cpus and one device instead.
- **Larger clusters.** Three hosts are the fewest a group of three copies or 2+1 by host can be;
  how each store spreads over thirty is not measured here.
- **Failure and recovery.** No host, device or process is lost in any arm.
- **A working set larger than memory.** With 1,024 objects of each size preloaded, the 64 KiB and
  1 MiB working sets fit in every host's memory, and every store ~~but Shoal~~ served most of its
  gets from a cache: either its own, which the drop before each run leaves (Ceph's BlueStore, and
  Shoal's since [F107](../features/cache-default-share.md)), or the page cache, which is dropped
  before each run but not during one. Shoal's first run had none: across hosts its devices read
  1.1 to 3.4 bytes for each byte it served of 64 KiB, and measured again they read 0.35 at most.
  Reads that miss would cost every store a device read~~, as they cost Shoal~~. At 16 MiB the set
  does not fit, and every store read its devices.
- **Where the client is.** The driver shares europa with a member of every store. Shoal's lead at
  large gets at sixteen in flight with three copies is mostly the share it served from europa's own
  copy over loopback ([Drift](#drift)).
- **Disks with no SSD beside them.** On the disk leg Shoal's rows and Ceph's writes of less than
  64 KiB were on each host's SSD, as each would be deployed, and MinIO's, RustFS's and SeaweedFS's
  syncs were on the disk. A host with disks alone was not measured, and neither was SeaweedFS with
  its index and filer moved to the SSD, which its put does not wait on.
- **Unlike disks.** Europa's disk is a 6 TB WD6001FZWX and titan's and hyperion's are 14 TB
  WD140EDFZs, and the two are not compared here. A store whose reads favour one host is bounded by
  that host's disk alone, as Shoal's reads with three copies were in its first run ([O119](../appendix/optimizations.md#o119-a-get-from-a-pool-of-copies-reads-its-coordinators-own-copy-so-each-disk-serves-the-gets-its-node-leads)),
  before its cache answered the working set.

## What it did not measure

- Listing, multipart uploads, ranged reads, deletes as a workload, versioning, and every S3
  feature beyond a whole object's put, get, head and delete.
- ~~TLS, on any store.~~ TLS on any store's client connections. Shoal's lanes between nodes ran
  under mutual TLS, the object lane under kTLS, since shoaladm renders `cluster.tls` for every
  deployment, and no other store encrypted between its nodes. O104's diagnosis found this
  (2026-10-22): kTLS was 15% of an object executor's time in 2+1 gets on one host
  ([where the executor's time goes](store-optimizations/object-lane.md#where-the-executors-time-goes)).
  Since [F103](../features/plaintext-peer-lanes.md) an inventory may name its lanes plaintext, which
  would run Shoal's like for like. Beside TLS at 2+1 (2026-10-23), plaintext lanes ran europa's
  loopback 1.09 to 1.24 times as fast, every get and put arm but one with its ranges apart. On the
  SSD leg the gets moved only with the leadership draw, and puts at one in flight ran 1.08 and 1.14
  times as fast ([F103's figures](../features/plaintext-peer-lanes.md#performance)).
- A store tuned for this hardware: each ran at its defaults, but where this page says otherwise.
  MinIO's lock retry is one such default ([above](#minios-4-kib-puts-wait-out-a-lock-retry)).
- Shoal through an S3 gateway, which does not exist.
- Shoal with no group commit delay, which is the engine's default; the lab's inventory sets one
  ([each store as deployed](#each-store-as-deployed)).
- Shoal's reads at `One`. The driver named no level, so every Shoal get and stat was a strong read
  ([O118](../appendix/optimizations.md#o118-a-striped-get-pays-two-strong-reads-its-entrys-and-its-stripes-rows)).
  It can since [F95](../features/bench-object-read-levels.md) (`--read-level one`).

## What it found

- [Resolved #276, 277](../appendix/resolved/object-preload-and-wrap.md): a preload counted in
  objects put half of them, and a put past a description's last object put an earlier one again
  unmarked.
- [Item 278](../appendix/known-issues.md#278-an-inventory-whose-pool-the-engine-refuses-deploys-and-every-node-restarts-on-it-until-a-client-gives-up):
  an inventory whose pool a node refuses deploys all the same.
- [Item 279](../appendix/known-issues.md#279-shoal-benchs-runner-half-no-longer-builds-without-the-engine):
  shoal-bench's runner half does not build without the engine.
- [O118](../appendix/optimizations.md#o118-a-striped-get-pays-two-strong-reads-its-entrys-and-its-stripes-rows):
  a striped get makes two strong reads, one after the other.
- [O119](../appendix/optimizations.md#o119-a-get-from-a-pool-of-copies-reads-its-coordinators-own-copy-so-each-disk-serves-the-gets-its-node-leads):
  on a pool of copies, each disk serves the gets its node leads, found on the disk leg.
- [O120](../appendix/optimizations.md#o120-a-pools-inline-threshold-was-set-by-the-row-path-alone-and-a-64-kib-object-pays-a-whole-chunk):
  a 64 KiB object pays a whole chunk where a row would do.
- [O121](../appendix/optimizations.md#o121-every-get-reads-its-bytes-off-the-device-however-often-the-object-is-read):
  every get reads the device, however often its object is read. Since
  [F102](../features/read-unit-cache.md) a node may keep the units its reads verified, ~~off unless
  named~~ an eighth of its memory unless it names otherwise since
  [F107](../features/cache-default-share.md). With 2 GiB a node it ran gets at one in flight 1.25 to 3.1
  times as fast on every leg ([F102's figures](../features/read-unit-cache.md#performance)).
- [O122](../appendix/optimizations.md#o122-a-small-reads-p99-across-hosts-is-six-times-its-mean-and-nobody-knows-why):
  a small read's p99 across hosts, not yet explained.
- [O107](../appendix/optimizations.md#o107-a-stripe-shorter-than-its-chunk-costs-its-device-the-whole-chunk),
  filed before this page, measured beside other stores for the first time: the largest gap on it.

Measured again (2026-10-25 and 26), it found:
- [Item 316](../appendix/known-issues.md#316-a-gets-request-on-an-object-lane-went-unanswered-and-its-coordinator-waited-with-no-bound):
  a get's request on an object lane went unanswered until the next traffic on its lane, once on the
  SSD leg and once on the disk leg, both times in 2+1's 1 MiB gets at one in flight. Its read is
  now refused at a bound and goes to another holder
  ([Resolved #316, in part](../appendix/resolved/unanswered-read-bound.md)); which end held it is
  still open.
- [Resolved #317](../appendix/resolved/owed-connection-kept.md): a client's pooled connection
  past its pool's lifetime was closed with answers owed on it, found reading the client for how 316's
  first get failed. A connection keeps its write half while it owes answers since.
- [Resolved #318](../appendix/resolved/absent-partition-release.md): a query that waited on a read
  of its partition's index page, which then found no such partition, logged a WARN that the partition
  could not be read: 63,874 times in the loop leg's journals, most in 4 KiB puts at sixteen in flight,
  where nothing failed. It logs at DEBUG since.

Each optimization has a page of its own, in the order they are worth taking:
[Optimizations the comparison points to](store-optimizations.md).

## Related

[F94](../features/bench-object-endpoints.md), the driver every store was run through;
[S15](performance.md), the arms Shoal judges itself by; [X14](ceph-and-s3-sources.md), Ceph and
S3 read at the source; [S17](prior-art.md), what was taken from the others;
[F86](../features/replicated-pools.md), [F90](../features/erasure-coded-pools.md) and
[F91](../features/rotational-devices.md), the pools measured;
[O107](../appendix/optimizations.md#o107-a-stripe-shorter-than-its-chunk-costs-its-device-the-whole-chunk),
what a short stripe costs.
