# X14. Ceph and S3, read at the source

**Reported 2026-10-05.** This is the record of spike
[X14](spikes.md#x14-ceph-and-s3-at-the-source). It read Ceph's source at the release this part
pins, `v20.2.0` (commit `69f84cc`, Tentacle), where S17 had recalled it. It read the S3 API at
its source, AWS's Smithy model of it. Asked for with the user before the run, it also stood a
Ceph `v20.2.0` up on the lab and watched six of the things the reading claimed.
[S18](contract.md#q32-and-q14-q20-q28-in-part-ceph-and-s3-at-the-source-2026-10-05) records what
it decides: **Q32**, what the metadata leaves room for, and its part of Q14, Q20 and Q28.

Of S17's nine recalled items, four held, two held in part, two were wrong, and the ninth, Crimson,
was a gap and not a claim. Three more claims, written on other pages as if read, were wrong too
([the comparison](#the-comparison)). The three the spike page called likeliest to matter came out
like this:

- **What an acknowledgement of an erasure coded write waits for.** Every shard the primary
  sends a write, durably committed: the acting set, plus any shard being recovered or
  backfilled. Since Tentacle, a pool with `allow_ec_optimizations` sends nothing to the data
  shards a partial write does not touch, and so does not wait for them. The lab saw both
  halves ([1](#1-what-an-acknowledgement-waits-for)).
- **How positions are kept stable when a holder leaves.** CRUSH's `indep` gives each position its
  own draws and leaves a hole where one fails, as recalled. Ceph's own code on X2's shapes moves
  2.0 to 3.5 times the least for a device added, removed or reweighted, more than X2's model of
  it. A device marked out moves the least where the pool is narrower than its domains, and
  removing it afterwards moves again
  ([5](#5-placement-indep-upmap-and-crush-compat)).
- **How a truncate is fenced.** Not as recalled. The sequence rides only the extent operations
  CephFS sends, the OSD keeps one pair an object, and a stale write is clipped to the object's
  current size. The OSD alone does not promise what [P13](contract.md#the-contract) promises
  ([4](#4-a-truncate)).

Three findings outweigh the rest:

- **Ceph's deep scrub of an overwritable erasure coded pool checks no shard against another.**
  Only a pool that never overwrites keeps a checksum a shard. The lab corrupted a shard
  through the object store so its checksums stayed valid. A deep scrub found it on the plain
  pool only. On the others a client read failed or, for parity, nothing noticed
  ([7](#7-scrub-the-scheduler-and-what-a-deep-scrub-of-an-erasure-coded-pool-verifies)). Q28 is
  new ground, not Ceph's.
- **A Ceph pool serves one kind of client.** RGW, CephFS and RBD share a cluster, each in pools
  of its own, and a second application on a pool is refused without an override
  ([9](#9-how-osds-are-deployed-whom-a-pool-serves-and-crimson)).
- **S3 lists in byte order and keeps a multipart object's parts after completion.** Its
  default checksum is the full object's CRC-64/NVME, which S3 makes from the parts' CRCs. That
  is X5's checksum and X5's combine ([13](#13-s3-checksums)).

**What was found on the way.** cephadm 20.2.0 cannot bootstrap on a stock Ubuntu 26.04. Its
`install -o 167` meets uutils coreutils 0.8.0, which refuses a numeric owner with no passwd
entry. hyperion has no route to the internet. Neither changes a page;
[Defects found upstream](#defects-found-upstream) and [Where, and on what](#where-and-on-what)
record them. What X14 did not settle is under [What X14 does not settle](#what-x14-does-not-settle).

## The question

The spike page's question has two halves.

- **Do the mechanisms this design copies work the way these pages say?** S17 marked every
  claim it had not read as *recalled*. Eight pages lean on those claims, and five name X14 as the
  one to read them.
- **What would a later listing or an S3 gateway need the metadata to have left room for?** That
  is [Q32](contract.md#questions-to-answer). The decisions of 2026-10-02 put listing and S3
  compatibility out of scope ([overview](overview.md#decisions-taken-on-2026-10-02)). A
  [schema change is a new cluster](../distributed/protocol.md#q10-at-m10a), so a row that cannot
  hold what a gateway needs is a cluster rebuilt later.

## How it was judged

**What would have changed the design**, named on the spike page before the run: anything S17
lists as recalled that turns out otherwise where a page leans on it. The likeliest were what an
acknowledgement waits for, how positions are kept when a holder leaves, and how a truncate is
fenced. [The verdicts](#what-would-have-changed-the-design) say which came out.

**Three labels**, and a fourth for one source:

- **read**: from Ceph's repository at `v20.2.0`, commit `69f84cc`, at the path and line given;
  or from AWS's Smithy model at the commit below, at the shape, member and line.
- **observed**: seen on the lab's Ceph, from the image `v20.2.0` was released as. The
  prediction for each experiment was written from the source first, with its lines, in
  `shoal-spike/results/x14-predictions.md`, and was not edited after.
- **recalled**: neither. Kept as recalled, with why.
- **a rendering**: the S3 User Guide, which has no published source. Its pages are cited by name
  as fetched on 2026-10-05, and only where the model is silent.

Code is told from a design document wherever it matters. `doc/dev/` holds both proposals and
descriptions, and several of the claims S17 recalled came from design documents that the code
did not follow.

## What was read

| Source | Pinned at | What |
| --- | --- | --- |
| Ceph | `v20.2.0`, commit `69f84cc2651aa259a15bc192ddaabd3baba07489` | `src/osd` (both erasure coded back ends, peering, the PG log, scrub), `src/crush`, `src/os/bluestore`, `src/rgw` and `src/cls/rgw*`, `src/mon`, `src/osdc`, `src/tools`, `src/erasure-code`, `src/common/options`, the balancer and cephadm modules, `src/ceph-volume`, `src/crimson`, `doc/`, and `qa/standalone/scrub`. Six files outside those were fetched at the same tag: `src/client/Client.cc`, `src/mds/MDCache.cc`, `src/include/cephfs/types.h`, `src/mgr/DaemonServer.cc`, `src/librados/IoCtxImpl.cc`, `src/librbd/api/Pool.cc` |
| The S3 API | `github.com/aws/api-models-aws` at `a0767ac42e27` (2026-09-30), `models/s3/service/2006-03-01/s3-2006-03-01.json` | The Smithy model the API reference is generated from: every operation, shape and member, with its documentation |
| The S3 User Guide | Fetched 2026-10-05 | `checking-object-integrity`, `checking-object-integrity-upload`, `conditional-writes`, `conditional-deletes`, `conditional-reads`, `mpuoverview`, `qfacts`, `tutorial-s3-mpu-additional-checksums`, `ListingKeysUsingAPIs`, `object-keys`, `UsingMetadata`, `Versioning`, `DeleteMarker`, `list-obj-version-enabled-bucket`, `Welcome` (the consistency model) |

Ceph's repository is read sparsely; `CLAUDE.md` has the lines that fetch both pins again under
`target/lab/x14/`. Tentacle carries **two erasure coded back ends**, and most of what this page
says depends on which one a pool is given. A pool with `allow_ec_optimizations` gets the new one
(`ECBackend.cc`, `ECCommon.cc`, `ECTransaction.cc`). Every other erasure coded pool gets the old
one, whose files end in `L` (`ECBackendL.cc` and its kin), chosen once a PG
(`src/osd/ECSwitch.h:48`). The flag is off by default
(`osd_pool_default_flag_ec_optimizations`, `src/common/options/global.yaml.in:2706-2711`). It
needs the ISA-L or Jerasure plugin's `reed_sol_van` and every OSD at Tentacle, and it cannot be
turned off once set (`src/mon/OSDMonitor.cc:8377-8439`). This page calls the two **legacy** and
**optimized**.

## What was run

### Where, and on what

A Ceph `v20.2.0` cluster stood up with cephadm for the spike and taken down after it, with
`shoal-spike/results/x14-lab.sh` holding every step and `x14-host-changes.txt` every change it
made to a host. No shoal unit was running on any host. Each host was snapshotted before anything
was installed: its packages, enabled and running units, volumes, sysctls, keys and directories.
After the teardown each was diffed against its snapshot. They were identical, but for ssh session
scopes and titan's `kubelet`, which was restarting in a loop before the spike and was left alone.
`x14-cluster.txt` keeps the cluster as it ran: its daemons and their versions, its OSD tree and
pools.

| Host | Ran | Container engine |
| --- | --- | --- |
| europa | The one monitor, the one manager, one RGW, and every client | docker 29.2.1, already there |
| titan | Three OSDs, on 20 GiB LVs in its volume group: osd.1, osd.3, osd.4 | podman 5.7.0, installed for the spike and removed after |
| hyperion | Three OSDs, the same: osd.0, osd.2, osd.5 | podman 5.7.0, the same |

- **The image** is `quay.io/ceph/ceph@sha256:1228c3d0…`, which every daemon reported as `ceph
  version 20.2.0 (69f84cc2651aa259a15bc192ddaabd3baba07489) tentacle (stable - RelWithDebInfo)`.
  hyperion has no route to the internet, so it was given titan's copy, saved and loaded. A load
  keeps an image's layers but not its manifest's digest. So every host named the image by its
  tag, `quay.io/ceph/ceph:v20.2.0`, and the manager was told not to turn tags into digests
  (`mgr/cephadm/use_repo_digest`). The two layers were the same on all three hosts.
- **The configuration** was the defaults, except: `osd_crush_chooseleaf_type = 0`, so replicated
  pools spread by OSD over two OSD hosts; `osd_memory_target` 1.5 GiB with autotuning off, for
  14 GiB hosts; `mon_osd_adjust_heartbeat_grace = false`, so E2's grace did not grow between
  trials; and the flags `noscrub`, `nodeep-scrub` and `noautoscale` set, so nothing scrubbed or
  split a PG unless asked.
- **The pools** had one PG each, so a PG's acting set is its pool's, and every erasure code
  profile had `crush-failure-domain=osd` and `stripe_unit=4K`:

| Pool | Profile | Flags | Acting set, shard order | `min_size` |
| --- | --- | --- | --- | --- |
| `e42plain` | 4+2, ISA-L `reed_sol_van` | none | [5, 3, 1, 4, 2, 0] | 5 |
| `e42legacy` | 4+2 | `ec_overwrites` | [1, 0, 5, 2, 3, 4] | 5 |
| `e42opt` | 4+2 | `ec_overwrites`, `ec_optimizations` | [5, 4, 2, 0, 3, 1] | 5 |
| `e21plain`, `e21legacy`, `e21opt` | 2+1 | the same three | [5, 1, 0], [1, 5, 4], [2, 0, 4] | 2 |
| `rep3` | replicated, size 3 | none | [4, 3, 1] | 2 |

`ceph osd erasure-code-profile get` reported `plugin=isa technique=reed_sol_van` without being
asked for either. That is Tentacle's default
(`src/common/options/global.yaml.in:2617-2621`), which `PendingReleaseNotes:130-133` announces:
"The default plugin for erasure coded pools has been changed from Jerasure to ISA-L".

### What it took

- **A user at uid 167 on every host.** cephadm's bootstrap failed at once: `install -d -m0770
  -o 167 -g 167 /var/run/ceph/<fsid>: install: invalid user: '167'`. Ubuntu 26.04's
  `install(1)` is uutils coreutils 0.8.0, from the `rust-coreutils` package. It takes a
  numeric owner only if a passwd entry has that number; `install -o 0` and `-o 65534` worked
  and `-o 167` did not. GNU's `install`, which Ubuntu still ships as `gnuinstall`, took it. A
  system user and group `cephx14` at 167, the image's `ceph` user, made the bootstrap pass.
  They were removed with the cluster.
- **OSDs from LVs, one spec a host.** `ceph orch daemon add osd <host>:<lv>` answered "No
  devices found" while the manager's inventory of a new host was empty, yet still saved the
  spec. Each later add replaced it, so only some LVs became OSDs. A spec a host listing its
  three LVs made the rest, and ceph-volume skipped the LVs already used.
- **A start limit.** cephadm's OSD units allow five starts in thirty minutes. E2 restarts a pool's
  primary before every trial, to empty its extent cache, and the sixth restart of osd.5 was
  refused. The harness clears a unit's failed state before it starts one.
- **Three runs voided by the harness, and run again.** E1's first run waited forever on an OSD's
  `up` flag, which the JSON gives as a number. E2's first run read a pool's id under a key the JSON
  does not have. E4's first run looked up each shard's OSD after stopping it, when its place in the
  acting set had become a hole, so it corrupted nothing; its scrubs and reads were clean and are
  not reported. Each was fixed and the experiment run again from the start.

### The experiments

| | Claim it tests | What was done |
| --- | --- | --- |
| E1 | A PG below `min_size` blocks reads as well as writes | osd.1 and osd.3 stopped cleanly with `noout`. That left five PGs below `min_size`: a 4+2 PG of each kind with four shards, a 2+1 PG at `min_size 3` with two, and the replicated PG with one. Each was read and written under a 30 s timeout, then again with `min_size` lowered to k (and 1) |
| E2 | An acknowledgement waits for every shard of the acting set | The OSD of data shard 2 of a 4+2 PG stopped with SIGSTOP. One 4 KiB write into data shard 1's unit or shard 2's, timed to its acknowledgement, on the optimized and the legacy pool, at each `ec_pdw_write_mode` |
| E3 | Tentacle writes only the shards a write touches, and updates parity by delta | A thousand 4 KiB writes into data shard 1's unit of untouched stripes, with every OSD's counters read before and after and an idle stretch as long subtracted |
| E4 | What a deep scrub of an EC pool verifies beyond each shard's own checksum | One byte of a data shard, and one of a parity shard, changed through `ceph-objectstore-tool`, which writes fresh BlueStore checksums; and one object's every shard replaced by another object's. Then a deep scrub, and a read |
| E5 | RGW defers a replaced object's tail for `rgw_gc_obj_min_wait` | A 6 MiB object PUT through RGW, overwritten, and the garbage collector's queue read |
| E6 | How far Ceph's own CRUSH moves on X2's shapes, and whether `indep` keeps positions | `crushtool` from the same image, offline, on X2's shapes and changes, counted as X2 counted |

E1 to E5 ask what Ceph does, not how fast, so each is reported from one run to its end. Every
answer agreed across that run's own trials and cases. Their output is
`shoal-spike/results/x14-e*.txt`, E6's `x14-e6-crush.md`.

## How to read the tables

*Shard n* is position n of a PG's acting set: for a k+m code, shards 0 to k-1 hold data and k to
k+m-1 parity. 2147483647 is `CRUSH_ITEM_NONE`, a position no OSD holds. *The least* a placement
change could move is X2's: the chunks the departing devices held, the chunks a new device ends
up with, or the chunks a reweighted device gave up.

## 1. What an acknowledgement waits for

**Recalled** on S17: "an acknowledgement waits for every shard of the acting set". **In part**:
it waits for every shard it sends the write to, and since Tentacle an optimized pool does not send
it to every shard.

- **Replicated pools** wait for every shard of `acting_recovery_backfill`: the acting set, plus
  any shard being recovered asynchronously or backfilled (`src/osd/ReplicatedBackend.cc:598-600`;
  `src/osd/PeeringState.h:1551-1552`). A shard the object is not yet valid on gets an empty
  transaction carrying the log entry, and is waited for too (`src/osd/PrimaryLogPG.cc:568-595`).
  Every one must report its commit on disk (`ReplicatedBackend.cc:698-700`).
- **Legacy erasure coded pools** do the same: every shard is put in both `pending_apply` and
  `pending_commit` (`src/osd/ECCommonL.cc:881-888`), and the client is answered when both are
  empty (`src/osd/ECBackendL.cc:1194-1200`).
- **Optimized pools** skip a shard whose transaction is empty: "Skipping transaction for shard"
  (`src/osd/ECCommon.cc:824-836`). Only the rest are counted in `pending_commits` (`:840`), and the
  write completes when they have committed (`:936-955`). Which shards are written is the write's
  byte range, plus data shard 0 and every parity shard, which are always written because only they
  may become primary: "All primary shards must always be written, regardless of the write plan"
  (`src/osd/ECTransaction.cc:590-592`; the restriction, `src/mon/OSDMonitor.cc:8413-8428`). A
  change of size, a create, a delete or a truncate writes every shard (`ECTransaction.cc:364-638`).
- **Durable, in every case.** A shard's reply is registered on the commit of its transaction,
  which holds its bytes, its log entry and its undo record together
  (`src/osd/ECBackend.cc:434-453`).
- **A partial write reads first**, from the extent cache if it can and from the shards if not
  (`src/osd/ECExtentCache.cc:50-107`), and sends its sub-writes only when the reads are back
  (`ECCommon.cc:760-762`). The legacy back end reads the whole stripe (`src/osd/ECTransactionL.h:110-114`).
  The optimized one reads either the touched shard and the parity, for a **parity delta**, or the
  untouched data shards, to **reconstruct**. It picks the delta only when that reads fewer shards,
  or when a shard the reconstruction needs is unreadable (`ECTransaction.cc:208-231`;
  `ec_pdw_write_mode`: 0 for that choice, 1 for never a delta, 2 for a delta whenever one can be made,
  `src/common/options/global.yaml.in:6799-6803`).
- **No early acknowledgement.** The proposal to answer after `W` of `W+M` prepares
  (`proposals.rst:192-205`) was not built.

**E2 observed it.** The OSD of data shard 2 of the 4+2 PG was stopped with SIGSTOP, as a hung
host would stop it: alive to the monitors until its peers' reports reach the grace. Then one 4
KiB write landed in a stripe nothing had touched since the primary restarted, timed to its
acknowledgement. A write to the replicated pool, whose PG holds no stopped OSD, was the control:
5.3 to 18.3 ms.

| Pool | `ec_pdw_write_mode` | 4 KiB into | Predicted | Acknowledged after | The stopped OSD marked down after |
| --- | --- | --- | --- | --- | --- |
| optimized | 2, a delta whenever one can be made | data shard 1's unit | at once: reads shards 1, 4 and 5, writes 0, 1, 4 and 5, nothing to shard 2 | **18.6 ms** | not marked down; continued before its grace |
| optimized | 0, the cheaper | data shard 1's unit | blocks: a delta and a reconstruction both read three shards, so it reconstructs, reading shard 2 | **22,960 ms** | 23.7 s |
| optimized | 1, never a delta | data shard 1's unit | blocks, reading shard 2 | **23,524 ms** | 25.0 s |
| optimized | 2 | data shard 2's unit | blocks: its own shard | **21,486 ms** | 25.2 s |
| optimized | 0 | data shard 2's unit | blocks | **24,884 ms** | 25.8 s |
| legacy, overwrites | – | data shard 1's unit | blocks: the whole stripe is read and all six shards written | **23,963 ms** | 25.7 s |
| legacy, overwrites | – | data shard 2's unit | blocks | **24,613 ms** | 24.9 s |

Every blocked write was acknowledged within a second of the monitor's "osd.N failed ... after
24.9 >= grace 20" (`shoal-spike/results/x14-e2-ack.txt`), so its time is the failure detector's,
not the write's. That is two reporters from two hosts past `osd_heartbeat_grace`
(`src/common/options/global.yaml.in:1927-1946`, `:2910-2913`).

So the ack never waits for a shard the write does not touch, as Tentacle's design says, and it
waits for everything it does touch, reads included, until the monitors mark the shard down. Then,
the source says, the write is planned again without it (`src/osd/PrimaryLogPG.cc:13204-13206`): by parity delta if the reconstruction lost a shard, by
reconstruction if the delta did. **This is what [S8](erasure-coding.md#a-partial-overwrite)'s
label a chunk is for**, held in one replicated row instead of in every holder's log, and it is
why S17's row and S18's alternative are corrected rather than the design.

## 2. Peering, fencing and `min_size`

### Who fences a primary

**Recalled** on S17: "monitors fence a primary by map epochs". **Corrected**: the monitors decide
nothing about a write; they commit epochs, and the OSDs do the fencing.

- **The monitors commit the map, and everyone computes the primary from it.** Up, acting and
  primary come from one function over the map: `pg_temp` and `primary_temp` if set, else CRUSH,
  the upmap exceptions and primary affinity (`src/osd/OSDMap.cc:3063-3110`). The client runs the
  same function (`src/osdc/Objecter.cc:2996`). A primary that wants another acting set asks the
  monitors for a `pg_temp` (`src/osd/PeeringState.cc:2661`; committed at
  `src/mon/OSDMonitor.cc:4208`).
- **The OSDs drop what an older interval sent.** A client op from before the primary last changed
  is dropped: "if (m->get_map_epoch() < info.history.same_primary_since) ... dropping"
  (`src/osd/PG.cc:1934-1938`). A sub-write or sub-read from before the PG last re-peered is
  dropped too (`can_discard_replica_op`, `PG.cc:1991-2028`). A write completes only when every
  shard sent it commits ([1](#1-what-an-acknowledgement-waits-for)). So a deposed primary, whose
  sub-writes its shards discard, cannot complete one, even if it has not yet seen the new map.
- **`up_thru` is the monitors' record of which intervals could have written.** A primary may not
  go active until the map records its `up_thru` past the interval's start
  (`src/osd/PeeringState.cc:1480-1486`). Peering then counts an earlier interval as one that
  "maybe went rw" only if its primary had (`src/osd/osd_types.cc:4417-4419`), and will not
  activate a new interval while such an interval has too few survivors to speak for it
  (`src/osd/osd_types.h:3854-3859`).
- **Reads are fenced by a lease, not by epochs.** A primary serves a read only while its read
  lease, `osd_heartbeat_grace × 0.8`, has not lapsed. A new primary waits out its predecessor's
  (`src/osd/PrimaryLogPG.cc:853-882`, `src/osd/PeeringState.cc:6831-6836`).

That is closer to this book's own [lease](../distributed/failover.md#the-lease) than S17 said, and
it is not what [P5](../distributed/protocol.md#the-contract) forbids: Ceph's monitors appoint by
publishing a map and do not authorize a write. They do appoint, though, and nothing here may.

### Below `min_size`

**Recalled**: "a placement group below `min_size` blocks reads as well as writes". **Confirmed,
read and observed.**

- A PG whose acting set is smaller than the pool's `min_size` becomes `peered`, not `active`
  (`src/osd/PeeringState.h:2449-2451`, `src/osd/PeeringState.cc:6825-6829`). Every client op,
  read or write, waits for active before the two are told apart: "peered, not active, waiting for
  active" (`src/osd/PrimaryLogPG.cc:1920-1927`). No read escapes it. Balanced reads are for
  replicated pools only (`src/osdc/Objecter.cc:3106-3108`), and in `v20.2.0` an erasure coded pool
  has no read that bypasses its primary.
- **Recovery is allowed below `min_size`** by default (`osd_allow_recovery_below_min_size`,
  `src/common/options/osd.yaml.in:865-869`), so an erasure coded PG with k shards rebuilds while
  it serves nothing.
- **The default `min_size`** of an erasure coded pool is `k + min(1, m - 1)`
  (`src/mon/OSDMonitor.cc:7800-7805`). That is k+1 for 4+2, and **k for 2+1**. Ceph's own guide
  recommends "K+1 or greater to prevent loss of writes and loss of data"
  (`doc/rados/operations/erasure-code.rst:580-581`), and its own default does not follow it when m
  is one.

**E1 observed it.** osd.1 and osd.3 were stopped cleanly with `noout`, and every PG of the test
pools was read and written under a 30 s timeout. Then `min_size` was lowered to k, or to 1 for
the replicated pool, and both were tried again:

| PG | Pool | Shards left (holes are 2147483647) | `min_size` | State | `rados get` | `rados put` | With `min_size` lowered |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 6.0 | 4+2 plain | [5, –, –, 4, 2, 0] | 5 | `undersized+degraded+peered` | timed out at 30 s | timed out | 4: `active+undersized+degraded`, both in ~1 s |
| 7.0 | 4+2 legacy | [–, 0, 5, 2, –, 4] | 5 | the same, its primary moved from shard 0 to shard 1 | timed out | timed out | 4: active, both in ~1 s |
| 8.0 | 4+2 optimized | [5, 4, 2, 0, –, –] | 5 | the same | timed out | timed out | 4: active, both in ~1 s |
| 9.0 | 2+1 plain | [5, –, 0] | raised to 3 | the same | timed out | timed out | 2: active, both in ~1 s |
| 10.0 | 2+1 legacy | [–, 5, 4] | 2, the default | `active+undersized`: active, so **open to writes with no redundancy left** | not tried | not tried | – |
| 12.0 | replicated 3 | [4] | 2 | `undersized+degraded+peered` | timed out | timed out | 1: active, both in ~1 s |

Every object read back byte for byte once the two OSDs returned
(`shoal-spike/results/x14-e1-min-size.txt`). The positions held, too: each erasure coded acting
set kept its shards in their places and left a hole where a stopped OSD had been.

Here the rule is [P11](contract.md#the-contract)'s, `f` of at least one, and a pool cannot be set
below it. Ceph's default leaves a 2+1 pool writable at `f = 0`.

## 3. Partial writes and the shard versions

**Read on S17**, from the Tentacle design document: "partial writes, parity delta, and a version
for each shard so that an untouched shard is not written". **Shipped**, for pools with
`allow_ec_optimizations`, and in two records rather than one vector:

- **Each log entry names the shards it wrote**: `written_shards`, "EC partial writes do not update
  every shard" (`src/osd/osd_types.h:4510`). An empty set means all of them (`:4581-4584`).
- **The object's info keeps the version each untouched data shard was left at**: `shard_versions`
  (`osd_types.h:6263`), set to the write's prior version for every data shard it did not touch
  (`src/osd/ECTransaction.cc:877-921`). Only data shard 0 and the parity get the object's info on
  every write; the other data shards hold "only ... PG log entries for their own updates"
  (`doc/dev/osd_internals/erasure_coding/enhancements.rst:1008-1014`). That is why only those
  shards may become primary (`src/mon/OSDMonitor.cc:8413-8428`), which the pool enforces with a
  `pg_temp` when CRUSH would pick another (`src/osd/OSDMap.cc:2877-2918`).
- **Each PG keeps, for each untouched shard, the range of versions it skipped**:
  `partial_writes_last_complete` (`osd_types.h:3061-3062`), merged at peering by the newest epoch
  (`src/osd/PeeringState.cc:366-435`). It lets a shard that saw none of a run of partial writes
  count as current (`:324-363`), and it keeps peering from treating an entry as missing on a shard
  it was never meant for (`:3431-3480`).

**The geometry**, for [Q20](contract.md#questions-to-answer):

- **The stripe unit** is `osd_pool_erasure_code_stripe_unit`, 4 KiB by default for every pool,
  set by the profile and fixed when the pool is made (`src/common/options/mon.yaml.in:16-27`,
  `src/mon/OSDMonitor.cc:7836-7844`). The documentation advises 16 KiB or more with optimizations
  (`doc/rados/operations/erasure-code.rst:232-235`); nothing in the code changes the default.
- **Units are dealt round-robin**: unit `i` of a stripe is data shard `i`
  (`src/osd/ECUtil.h:632-643`), so a read longer than k − 1 units fans over every data shard, and
  a short one reads only the shards that hold it (`osd_ec_partial_reads`, on by default,
  `src/common/options/osd.yaml.in:1410-1413`).
- **No padding**: an optimized pool stores a data shard only as long as its bytes, rounded to
  4 KiB, and parity as long as shard 0 (`ECUtil.h:614-629`). A 1-byte object in 4+2 costs three
  4 KiB blocks where a legacy pool spends six.
- **The default code is ISA-L's**, `plugin=isa technique=reed_sol_van k=2 m=2`
  (`src/common/options/global.yaml.in:2617-2621`), for clusters made since Tentacle.

Nothing here moves [S8](erasure-coding.md)'s geometry. A 4 KiB unit is below where X4 found
encoding at full rate on Zen1, 16 to 64 KiB, and Ceph's own advice for its optimized pools is
16 KiB. Ceph keeps no file for a chunk: a shard is part of a RADOS object, extents in BlueStore.

**E3 observed which shards a small overwrite writes.** A thousand 4 KiB writes went one at a time
into data shard 1's unit of a thousand stripes nothing had touched since the primary restarted.
Every OSD's BlueStore counters were read before and after, less an idle stretch as long. What
each shard did, a thousand times over; *shard* is position in the acting set, data 0 to 3 and
parity 4 and 5:

| Pool, `ec_pdw_write_mode` | Shard 0 (data; may be primary) | Shard 1 (the one written) | Shards 2 and 3 (untouched data) | Shards 4 and 5 (parity) |
| --- | --- | --- | --- | --- |
| legacy, overwrites | read; data written | read; data written | read; data written | data written |
| optimized, 0 | read; object info and log only | data written | read; **no transaction at all** | data written |
| optimized, 1 | the same | data written | the same | data written |
| optimized, 2 | object info and log only, not read | read; data written | **neither read nor written** | read; data written |

Every count was a whole multiple of the thousand writes. A shard that was written took two
transactions a write: the write itself, and the roll-forward that follows it once every shard has
committed, which carries no data ([1](#1-what-an-acknowledgement-waits-for)). Each wrote 4 KiB as a
new allocation (BlueStore's `write_big`), never deferred, since a whole allocation unit is not
under one. The untouched shards of the optimized pool counted nothing in any column
(`shoal-spike/results/x14-e3-shards.txt`). Modes 0 and 1 reconstructed, reading shards 0, 2 and 3,
because on 4+2 a delta and a reconstruction both read three shards and the tie goes to the
reconstruction. Mode 2 updated the parity by delta, reading the shard and the parity. The writes
took 7.1 ms each on the legacy pool and 5.6 to 5.9 on the optimized one. That is one run at one
write outstanding, and not a measurement to compare.

**So Tentacle's optimized pool does what [S8](erasure-coding.md#a-partial-overwrite) does**: it
leaves the untouched chunks untouched and keeps what they were left at. Here that is one
replicated row's labels; there it is a per-object vector on the shards that can lead, a log entry
naming the shards it wrote, and peering to reconcile them.

## 4. A truncate

**Recalled** on the objects page, [S3](objects.md#size-holes-and-truncate): "RADOS carries a
truncate sequence on every operation for the same reason", the reason being that page's truncate
epoch. **Corrected**: it is
carried on a few operations, by one client, and enforced differently.

- **Where it rides.** `struct ceph_osd_op` holds `truncate_size` and `truncate_seq` in the
  `extent` arm of a union (`src/include/rados.h:585-593`). Only the extent operations use that
  arm: read, sparse read, write, write-full, append, zero, truncate, trim-truncate, and compare
  (`rados.h:415-433`). Nothing else carries a sequence.
- **Who sets it.** CephFS alone. The Objecter sends zero unless a caller uses its `_trunc` forms
  (`src/osdc/Objecter.h:578-580`, `:3326-3327`), and the CephFS client passes the inode's
  `truncate_seq` through them (`src/client/Client.cc:11441-11445`, `:11936-11939`). The MDS
  bumps the sequence when it truncates a file (`src/include/cephfs/types.h:729-739`) and sends a
  trim-truncate to each object asynchronously (`src/mds/MDCache.cc:6626-6627`). A librados
  user's sequence is zero.
- **What the OSD keeps.** One pair an object, `truncate_seq` and `truncate_size` in its
  `object_info_t` (`src/osd/osd_types.h:6184`, `:6249`).
- **What it enforces**, in `PrimaryLogPG::do_osd_ops`:
  - a write whose sequence is older than the object's, extending past the object's current size,
    is clipped to that size: "old write, arrived after trimtrunc"
    (`src/osd/PrimaryLogPG.cc:6765-6773`);
  - a write whose sequence is newer applies the truncate it carries first, then writes
    (`:6775-6803`), so a late trim-truncate becomes a no-op (`:6983-6988`);
  - a read whose sequence is newer is cut to the truncate size (`:5848-5852`).

**What it solves** is the problem the objects page's floors solve. A truncate reaches each object
asynchronously, and a write issued before it must not bring bytes past the cut back. **How**
differs, and the difference matters. The OSD clips a stale write against the object's *current*
size, `oi.size`, not against the truncate's size. If a newer write has already extended the
object past the stale write's range, the clip does not fire, and the stale bytes land. Nothing in
the write path compares a stale write with `oi.truncate_size`. So the OSD alone does not give
[P13](contract.md#the-contract): "bytes cut off by a truncate never return, whatever later extends
the object." Whether CephFS's capabilities stop that interleaving before it reaches an OSD was
not traced. **The objects page's floors stay**, and the sentence that leaned on Ceph is
corrected.

## 5. Placement: `indep`, `upmap` and `crush-compat`

### `indep`, read

**Recalled**: "CRUSH keeps positions stable for an erasure coded pool by choosing independently
for each". **Confirmed**, and X2's model of it was close but not the same.

- Position `p` draws at round `f` with `r = p + n·f`, `n` being the rule's width, the same `r` at
  every level of the descent: "we base the choice on the position even in the nested call"
  (`src/crush/mapper.c:693-709`). Rounds go across all positions breadth first (`:669`, `:684-686`).
- A draw is discarded if any position already settled holds that domain (`:755-763`), if the
  domain yields no usable device in its leaf tries (`:765-781`), or if the device is out
  (`:787-789`).
- A position still empty after the tries is left `CRUSH_ITEM_NONE` at its index and is never
  shifted (`:798-805`). That is what keeps the other positions where they were. The `firstn` mode
  a replicated pool uses shifts later positions down instead (`:603`, `:618-627`).
- The monitor's rule for an erasure code profile tries 100 rounds and 5 leaves
  (`set_choose_tries 100`, `set_chooseleaf_tries 5`; `src/crush/CrushWrapper.cc:2340-2366`).
- The draw is straw2's: a 16-bit hash, a fixed-point logarithm from tables (`crush_ln`), divided by
  the item's weight (`mapper.c:315-339`). Nothing in the source says why the logarithm is fixed
  point. The same `mapper.c` is built into the Linux kernel client (`:13-24`), which has no libm,
  but that is an inference, not a stated reason. So [S5](placement.md)'s "Ceph's `crush_ln` exists
  for the same reason" is corrected: the reason is ours.

X2's `rendezvous by position` differs from `indep` in four ways. It is flat where `indep` descends
the hierarchy. It tries 32 rounds where `indep` tries 100. It fills an exhausted position with the
best unused domain where `indep` leaves a hole. And it checks a collision against earlier
positions only where `indep` checks every settled one.

### `indep`, run

E6 ran Ceph's own CRUSH, `crushtool` from the `v20.2.0` image, on X2's shapes, with the rule the
monitor makes for an erasure code profile, over 16,384 placement groups (X2's four a tablet).
Every change is to host zero's first device, and every figure is moved over the least the change
could move, as X2 counts (`shoal-spike/results/x14-e6-crush.md`):

| Shape | Pool | Add | Remove | Reweight ½ | Out | Remove, after out | X2's `by position` (add, reweight) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| lab-1 | 2+1/host | 2.05× | 1.63× | 8,448 moved, none needed | 1.30× | 0.86× again | 1.78×, 8,583 moved |
| lab-2 | 2+1/host | 2.26× | 1.90× | 2.62× | 1.02× | 1.14× again | 1.86×, 2.25× |
| lab-2 | 4+2/device | 2.32× | 2.46× | 19,243 moved, 1 needed | 1.57× | 1.86× again | 1.78×, – |
| 6x12 | 4+2/host | 3.45× | 3.03× | 3.17× | 1.00× | 2.94× again | 2.64×, – |
| 6x12 | 8+3/device | 2.04× | 2.05× | 2.18× | 1.01× | 1.97× again | 1.16×, – |
| 50x24 | 10+4/host | 2.00× | 2.30× | 2.33× | 1.00× | 2.27× again | 1.36×, 1.42× |
| 6x12 | r3/host (a set) | 2.23× | 2.05× | 2.00× | 1.00× | 1.99× again | X2's `rendezvous`: 1.00× |
| 50x24 | r3/host (a set) | 1.97× | 1.55× | 1.56× | 1.00× | 1.55× again | 1.00× |

- **A device added, removed or reweighted moves 2.0 to 3.5 times the least**, more than X2's
  flat model of `indep` on every shape, and on replicated pools too, where X2's flat rendezvous
  moved exactly the least. The changed device's weight is its host's too, so its host's draw
  moves, and chunks leave the host from devices that did not change: the column "moved off
  untouched hosts" in the results is never zero for these changes. That is the hierarchy's cost
  X2 measured (1.5 to 2.3 times for `by domain`), paid by Ceph's own code.
- **A device marked out moves the least**, 1.00 to 1.02 times, wherever the pool is narrower than
  its failure domains. Out changes no weight in the map: the device's positions retry a leaf in
  the same host, and nothing else draws differently. Where the pool is as wide as its domains, the
  out device's positions find nowhere to go, become holes, and others shuffle: 1.30 and 1.57 times.
- **Removing a device already out moves data again**, 1.1 to 2.9 times the out step's least,
  because removal is the weight change that out avoided. Ceph's documentation describes the
  difference in bucket weight (`doc/rados/operations/add-or-rm-osds.rst:320-326`) without calling
  it a second movement, and cephadm drains a removal by reweighting to zero
  (`src/pybind/mgr/cephadm/services/osd.py:703-710`). X2's page said Ceph's operators know this as
  "the second movement"; the movement is real, and the phrase is X2's, now said so.
- **Fill**: straw2's fullest device is within a few points of X2's flat rendezvous at the same
  number of groups: +0.4% against X2's +0.9% on lab-2, +6.8% against +6.5% for 6x12 4+2/host,
  +25.6% against +28.2% for 50x24 10+4/host.

**[Q19](contract.md#q19-in-part-placement-2026-10-03)'s choice is firmer for it.** Positions held
as state moved exactly the least in X2. Ceph, which keeps no such state, pays two to three and a
half times the least on every change but an out.

### `upmap` and `crush-compat`, read

Neither was run: both need a cluster with data to balance.

- **`upmap`** adds one `pg_upmap_items` pair at a time (`src/osd/OSDMap.cc:5675-5995`). It works
  from the OSD furthest above its target, a share of PGs in proportion to its weight. It first
  tries to undo an exception that points at that OSD, then moves a PG to the most underfull OSD
  the rule's failure domain allows. A move is kept only if the variance of PG counts falls, and a
  pass stops within `upmap_max_deviation`, 5 PGs, or after 10 moves
  (`src/pybind/mgr/balancer/module.py:312-322`). Every map change drops the exceptions whose source
  left the set, whose target is out, or that break the rule (`OSDMap.cc:2105-2293`). X2's
  exceptions move chunks off the fullest device by bytes; Ceph's count PGs.
- **`crush-compat`** keeps a second set of weights in the CRUSH map's `choose_args`, used only for
  placement, and nudges them toward each OSD's target share, halving its step when a step moves
  too much (`module.py:1221-1410`; `src/crush/crush.h:243-295`). That is X2's fitted placement
  weights, kept beside the capacity weights as [S4](pools-and-devices.md) keeps them.
- **A replacement keeps its predecessor's place** by taking its id: `ceph osd destroy` keeps the id
  and the CRUSH entry, "to recreate the OSD, at the same crush location, with minimal data
  movement" (`src/mon/OSDMonitor.cc:13064-13067`), and `ceph-volume ... --osd-id` reuses it. A
  seat here does the same without tying the device's identity to it.

## 6. BlueStore's deferred writes and checksums

**Read**, and S17's row holds with two refinements:

- **A deferred write is a redo journal.** The write's new bytes go into RocksDB under the prefix
  `L`, in the same transaction as the metadata (`src/os/bluestore/BlueStore.cc:15628-15635`;
  the op carries `data`, `src/os/bluestore/bluestore_types.h:1300-1313`). The client's commit is answered then
  (`:14577-14582`). The bytes are written in place later in batches (`:15231-15233`, `:15377`)
  and dropped from the log only after a flush (`:15004-15006`, `:15073`). A restart replays the
  log (`:15488-15510`). That is [S6](device-store.md)'s journal and apply in place, which X6
  measured and kept.
- **When.** A write under `bluestore_prefer_deferred_size` is deferred: 64 KiB on a rotational
  device and 0 on an SSD (`global.yaml.in:4637-4651`; chosen by the device's rotational flag,
  `BlueStore.cc:6993-7011`). But an overwrite of already-written space smaller than one
  allocation unit is deferred whatever the device. That path does not consult the threshold
  (`BlueStore.cc:16340-16394`). So on an SSD, a small overwrite in place is journalled too, as
  S6's is.
- **Checksums.** `bluestore_csum_type` defaults to `crc32c`; `crc32c_16`, `crc32c_8`,
  `xxhash32` and `xxhash64` are offered (`global.yaml.in:4529-4543`). The block is the device's
  block, 4 KiB, unless hints raise it (`BlueStore.cc:17328-17363`). A mismatch is retried three
  times and then answered `-EIO` (`:12723-12725`, `:12871-12880`).

## 7. Scrub: the scheduler, and what a deep scrub of an erasure coded pool verifies

### The scheduler, read

S17's row holds, with the cadence drawn and not fixed:

- **A light scrub** is due 1 to 1.5 days after the last, drawn uniformly
  (`osd_scrub_min_interval` and `osd_scrub_interval_randomize_ratio` 0.5;
  `src/osd/scrubber/scrub_job.cc:115-118`). **A deep scrub** is due after a draw from a normal
  distribution around 7 days with a deviation of 1.4, clamped to 4.2 to 9.8
  (`osd_deep_scrub_interval_cv` 0.2; `scrub_job.cc:251-255`). `osd_scrub_max_interval` no longer
  schedules anything, and only the monitor's warning reads it (`src/mon/PGMap.cc:3339-3343`).
- **Three at once** an OSD as primary (`osd_max_scrubs` 3,
  `src/osd/scrubber/scrub_resources.cc:52`), each reserving its replicas one by one
  (`src/osd/scrubber/scrub_reservations.h:23-28`). A scrub waits for the load average, the hour
  window and recovery (`src/osd/scrubber/osd_scrub.cc:185-215`). A scrub an operator asks for
  skips all of those, and the `noscrub` flags too (`src/osd/scrubber/pg_scrubber.cc:137-157`).
- **A deep scrub reads 512 KiB at a time**, `512_K` being 524,288 bytes
  (`src/common/options.h:424-426`), rounded up to whole chunks on an erasure coded pool
  (`src/osd/ECBackend.cc:1194-1196`). So S17's "512 K" and S11's "512 KiB" agree.

### What a deep scrub verifies, read

S11 asks what a deep scrub of k+m verifies beyond each stripe chunk's own checksums
([Q28](contract.md#questions-to-answer)). For Ceph the answer depends on the pool's flags:

| Pool | What each shard checks | What the primary compares across shards |
| --- | --- | --- |
| Erasure coded, never overwritten | Reads its whole chunk and compares a running CRC-32C with the cumulative one in its `hinfo_key` attribute: a mismatch is `ec_hash_error` (`src/osd/ECBackendL.cc:1797-1818`). The attribute is kept current by appends only (`src/osd/ECUtilL.cc:196`) | Sizes, the object's info, the `hinfo` attribute byte for byte, and the hash each shard reports, which is its stored hash of shard 0 and not of its own bytes (`ECBackendL.cc:1822-1829`; `src/osd/scrubber/scrub_backend.cc:1179-1188`, `:1304-1322`) |
| Legacy, with `allow_ec_overwrites` | Reads its chunk, so BlueStore checks its checksums, then reports a digest of 0: "Hack! We must be using partial overwrites, and partial overwrites don't support deep-scrub yet" (`ECBackendL.cc:1831-1835`). An overwrite clears the cumulative hash (`src/osd/ECTransactionL.cc:625-635`) | Sizes, the object's info, attributes. The digests are all zero |
| Optimized | The same: the hash is made and thrown away, the digest is 0 (`src/osd/ECBackend.cc:1223-1224`) | The same, without even a size comparison between shards, which may differ (`scrub_backend.cc:1343`) |

- **The object's own data digest is compared for replicated pools only** (`if (m_is_replicated)`,
  `scrub_backend.cc:1201-1225`), though `write_full` keeps it for every pool
  (`src/osd/PrimaryLogPG.cc:6880-6884`). A client read of a whole object does check it, and fails
  with EIO on a mismatch (`PrimaryLogPG.cc:5880-5882`, `:5404-5413`).
- **Nothing encodes again and compares.** The XOR "longitudinal summary" S11 took its parity check
  from exists only in the design document: "Currently deep scrub of an EC with overwrite pool just
  checks that every shard can read the object, there is no checking to verify that the copies on
  the shards are consistent" (`enhancements.rst:713-728`).
- **A light scrub reads no data**: it stats each shard and reads its attributes
  (`src/osd/PGBackend.cc:810-870`).

### What a deep scrub verifies, observed

**E4.** On each 2+1 pool, three objects of 64 KiB were corrupted through `ceph-objectstore-tool`
on stopped OSDs. A1 had one byte of data shard 1 flipped, and A2 one byte of parity shard 2. A3
had every shard replaced by object B's, B being another object of the same size in the same PG.
The tool rewrites a shard through BlueStore, so its checksums are fresh and every shard reads
back clean. Then an operator's deep scrub, `rados list-inconsistent-obj`, and a whole read of each
object:

| Pool | A1: a data shard's byte | A2: a parity shard's byte | A3: every shard B's |
| --- | --- | --- | --- |
| Plain, never overwritten | `ec_hash_error` on shard 1; a read returned **the original bytes**, decoded around it | `ec_hash_error` on shard 2; a read returned the original bytes | `ec_hash_error` on all three shards; a read failed, **EIO** |
| Legacy, `allow_ec_overwrites` | **nothing reported**; a read failed, EIO | **nothing reported**; a read returned the original bytes. **Nothing noticed it** | **nothing reported**; a read failed, EIO |
| Optimized | **nothing reported**; a read failed, EIO | **nothing reported**; a read returned the original bytes. **Nothing noticed it** | **nothing reported**; a read failed, EIO |

As predicted in every cell (`shoal-spike/results/x14-e4-scrub.txt`). On the plain pool each shard's
own hash caught it, and the healthy shards reported the stored hash of shard 0 as their digest. On
the others, every shard read back clean from BlueStore, every digest was zero, and
`list-inconsistent-obj` was empty. A whole read failed only because the object's info keeps a
CRC-32C of the whole object (set by `write_full`) and a read of all of it is checked against it.
A ranged read would have returned the wrong bytes, and so would any read after an overwrite in
place, since a write that neither covers the whole object nor appends clears that digest
(`src/osd/PrimaryLogPG.cc:6833-6844`). The parity corruption stays until a rebuild decodes from it. The PG's
state read `active+clean` at the moment the scrub's stamp changed; the scrub's report is what
counts.

So **Ceph's deep scrub of an overwritable erasure coded pool finds a shard only when BlueStore
cannot read it**. A shard that is wrong but intact (a lost write, a misdirected one, a parity
computed from the wrong data) is caught on a pool that never overwrites, and nowhere else.
[S11](scrub.md#what-a-deep-scrub-proves-and-what-it-does-not)'s check of parity against data, by
summaries or by encoding again, is not a copy of anything Ceph does. Q28 now records it as this
part's own.

## 8. RGW: head, tail and index

**Read**, and the recalled claim holds: RGW defers deleting a replaced object's tail for
`rgw_gc_obj_min_wait`, two hours by default (`src/common/options/rgw.yaml.in:1792-1811`).

- **An overwrite** replaces the head object in one RADOS operation: the new head's data, its
  manifest and its attributes, guarded by the old object's tag
  (`src/rgw/driver/rados/rgw_rados.cc:3256-3409`, `:7082-7142`). The old manifest's tail
  objects, never the head, are then queued for the garbage collector
  (`rgw_rados.cc:6010-6049`). Each entry's time is the OSD's clock at enqueue plus the wait
  (`src/rgw/driver/rados/rgw_gc.cc:120-138`; `src/cls/rgw_gc/cls_rgw_gc.cc:71-72`). A collector
  pass every `rgw_gc_processor_period`, an hour, removes what is due (`rgw.yaml.in:1834-1846`;
  `rgw_gc.cc:797-816`), so a tail goes two to three hours after its overwrite.
- **Why the wait.** The option's own text: "RGW will not remove object immediately, as object
  could still have readers. A mechanism exists to increase the object's expiration time when it's
  being read." That mechanism is **disabled**: "defer_gc disabled for
  https://tracker.ceph.com/issues/47866" (`src/rgw/rgw_op.cc:2380-2381`). Octopus's notes record
  a read longer than half the wait losing data (`doc/releases/octopus.rst:1136-1142`). So the
  wait is a timer that decides, which [P16](contract.md#the-contract) forbids here: a slow reader
  of a replaced object in RGW can read a tail that is gone.
- **The head** inlines up to `rgw_max_chunk_size`, 4 MiB, of the object's first bytes. It does
  so only in the default storage class, with `inline_data` on and the head and tail in one pool
  (`rgw.yaml.in:84-101`; `src/rgw/driver/rados/rgw_putobj_processor.cc:328-347`). Tails are
  RADOS objects of up to `rgw_obj_stripe_size`, 4 MiB (`rgw.yaml.in:1942-1954`).
- **Multipart** writes each part as RADOS objects of its own, under the upload's prefix.
  Completing the upload writes a head whose manifest names the parts' objects, and no byte is
  copied (`src/rgw/driver/rados/rgw_sal_rados.cc:4102-4304`).
- **The bucket index** is omap on index shard objects, eleven shards by default
  (`src/rgw/rgw_zone_types.h:335-338`), keyed by the object's name. Each entry keeps the name and
  instance, `size`, `mtime`, `etag`, `owner`, `owner_display_name`, `content_type`,
  `accounted_size`, `user_data`, `storage_class` and `appendable`, with flags for a version, the
  current version and a delete marker (`src/cls/rgw/cls_rgw_types.h:202-213`, `:369-394`). A
  shard is chosen by a hash of the name (`src/rgw/services/svc_bi_rados.h:103-123`). So an
  ordered listing asks every shard and merges their answers
  (`RGWRados::cls_bucket_list_ordered`, `rgw_rados.cc:10298-10630`).
- **The index is not updated atomically with the object.** A write first adds a *pending* entry
  to the index, synchronously, before the head is written (`rgw_rados.cc:3400`;
  `src/cls/rgw/cls_rgw.cc:991-999`). It completes the entry asynchronously afterwards
  (`rgw_rados.cc:10209-10214`). A listing that meets a pending entry checks the head object and
  suggests a correction to the index (`rgw_rados.cc:10506-10518`, `:10581-10596`). Pending
  entries older than two minutes are dropped (`cls_rgw.cc:2453-2469`). That is how RGW lists after
  a write without a transaction across the head and the index, and it is the shape a later
  listing index here would take ([Q32](#what-the-metadata-must-keep-possible)).

**E5 observed the first bullet.** A 6 MiB object was PUT to the lab's RGW in one request, past
the 4 MiB head and under boto3's multipart threshold, and then PUT again over itself:

| Step | The data pool held | `gc list --include-all` |
| --- | --- | --- |
| The first PUT, acknowledged 15:43:21 | the head `…_six-mib`, holding 4 MiB inline, and one tail object `…__shadow_.TlBv…_1`, the other 2 MiB | empty: a first PUT has no old manifest |
| The overwrite, acknowledged 15:43:24 | the head, the old tail, and a new tail under a new random prefix, `…__shadow_.tOc9…_1` | one entry, for the old tail's one object, due at **17:43:24.39**: the overwrite plus 7,200 s to the second. `gc list` without `--include-all` showed nothing due |

As predicted (`shoal-spike/results/x14-e5-rgw-gc.txt`): one tail object, queued, not deleted, for
exactly `rgw_gc_obj_min_wait`. The collector runs every `rgw_gc_processor_period`, 3,600 s.

## 9. How OSDs are deployed, whom a pool serves, and Crimson

- **One OSD a device is the default, not a rule** (*recalled* on S17: "an OSD is deployed one
  for each disk"; **in part**). ceph-volume's `--osds-per-device` defaults to 1
  (`src/ceph-volume/ceph_volume/devices/lvm/batch.py:230-235`), and cephadm passes it only when a
  spec sets it (`src/python-common/ceph/deployment/translate.py:140-141`). The drive group's own
  comment: "To fully utilize nvme devices multiple osds are required"
  (`src/python-common/ceph/deployment/drive_group.py:238-241`). The hardware guide calls several
  OSDs on one HDD "NOT a good idea" (`doc/start/hardware-recommendations.rst:192-193`). That is
  [S4](pools-and-devices.md#a-device-has-slices)'s split, a device one core cannot drive given
  more than one slice, and X6's finding that an SSD wants one.
- **A pool serves one kind of client** (*recalled*: "RGW, CephFS and RBD store into RADOS pools
  side by side", **confirmed**; written on three pages as "one RADOS pool serves RGW, CephFS and
  RBD", **corrected**). "Each pool must be associated with an application before it can be used"
  (`doc/rados/operations/pools.rst:229`). Enabling a second application on a pool is refused:
  "Are you SURE? Pool ... already has an enabled application; pass --yes-i-really-mean-it to
  proceed anyway" (`src/mon/OSDMonitor.cc:9484-9487`). CephFS refuses a pool another application
  uses (`src/mon/FSCommands.cc:1947-1948`), RBD's `pool init` needs a force flag
  (`src/tools/rbd/action/Pool.cc:25-26`), and RGW makes and tags pools of its own
  (`src/rgw/driver/rados/rgw_tools.cc:31-50`). The three share a *cluster*, its OSDs and
  devices. [S4](pools-and-devices.md)'s pool, which consumers of several kinds are bound to at
  once, goes further than Ceph does. That is a choice here, not something copied, and the pages
  now say so.
- **Crimson** (*recalled*: everything but its being thread-per-core). "Crimson is in a tech
  preview stage and is **not suitable for production use**" (`doc/dev/crimson/crimson.rst:95-96`).
  It runs Seastar reactors, "each thread ... expected to run on a dedicated CPU core" (`:71-72`).
  It shards placement groups over the reactors, a new one to the core with the fewest
  (`src/crimson/osd/pg_map.cc:62-70`). Its erasure coded back end is a stub of `// todo`s
  (`src/crimson/osd/ec_backend.cc:13`, `:22`, `:35`), so it serves replicated pools only.
  SeaStore is its native store for NVMe; BlueStore runs beneath it on a pool of "alien" threads
  (`crimson.rst:130-147`). Thread-per-core erasure coding with in-place writes, which this part
  builds, is not something Ceph ships.

## 10. S3: listing

All **read** from the model unless marked a rendering.

- **Order.** "For general purpose buckets, ListObjectsV2 returns objects in lexicographical order
  based on their key names. ... For directory buckets, ListObjectsV2 does not return objects in
  lexicographical order" (`ListObjectsV2`, line 39568). The guide says what the order is: "Amazon
  S3 sorts object keys, including prefixes, lexicographically by their UTF-8 encoded byte values"
  (*a rendering*, `object-keys`). The order is of bytes, case-sensitive and unnormalised. A key
  is at most 1,024 bytes of UTF-8 (*a rendering*, `object-keys`).
- **A page** is at most 1,000 keys ("never contain more", `ListObjectsV2Request$MaxKeys`, line
  39735). A page continues from an opaque `ContinuationToken`, "not a real key" (line 39752), or
  from any key with `StartAfter` (line 39766).
- **Prefix and delimiter.** Keys under the prefix that share a string up to the next delimiter
  are "rolled up into a single result element in the CommonPrefixes collection", and each counts
  once against the page (`ListObjectsV2Output$Delimiter`, line 39646).
- **An entry** carries `Key`, `LastModified`, `ETag`, `ChecksumAlgorithm`, `ChecksumType`,
  `Size`, `StorageClass`, and on request `Owner` and `RestoreStatus` (`Object`, lines
  40763-40823). It carries no checksum value and no user metadata.
- **Versions** list with `KeyMarker` and `VersionIdMarker`. Delete markers are entries without
  size or ETag, and `IsLatest` marks the current version (`ListObjectVersions`, lines
  39112-39312; `DeleteMarkerEntry`, lines 31414-31438). Within a key the newest comes first
  (*a rendering*, `list-obj-version-enabled-bucket`; the model does not say).
- **Consistency.** "Any read (GET or LIST request) that is initiated following the receipt of a
  successful PUT response will return the data written by the PUT request ... The new object
  appears in the list", and after a delete, "The object does not appear in the listing" (*a
  rendering*, `Welcome`; the model does not say). A listing is strongly consistent with writes.

## 11. S3: multipart

- **Parts** are numbered 1 to 10,000, and a part uploaded again under its number replaces the old
  one (`UploadPart`, line 47831). Each part is at least 5 MiB except the last ("EntityTooSmall",
  `CompleteMultipartUpload`, line 28428) and at most 5 GiB (*a rendering*, `qfacts`). An object
  is at most 48.8 TiB (*a rendering*, `qfacts`), which the model's `CopyObject` text calls 50 TB.
- **Completion** "concatenates all the parts in ascending order by part number". Each part is
  named by its number and its ETag, which must match (`CompleteMultipartUpload`, line 28428).
  Part numbers may skip, unless the upload carries checksums, which need them consecutive from 1
  (`CompletedPart$PartNumber`, line 28871).
- **The parts outlive completion.** `GetObject` and `HeadObject` take a `PartNumber`, which
  "effectively performs a 'ranged' GET request for the part specified" (line 35818), and report
  the parts' count (line 35621). `GetObjectAttributes` lists the parts with each one's size and
  checksum (lines 35000-35111; `ObjectPart`, lines 41212-41242). So an object made by multipart
  keeps its part boundaries, up to 10,000 of them, for as long as it lives. A copy makes it one
  part again (`CopyObjectRequest$CopySource`, line 29153).
- **Metadata** is given when the upload starts and attached when it completes (*a rendering*,
  `mpuoverview`). The object's `Last-Modified` is the upload's *initiation* time (*a rendering*,
  `UsingMetadata`). In a versioned bucket, of two uploads to one key, the one *started* later is
  current, whichever finished last (*a rendering*, `mpuoverview`).

## 12. S3: conditional requests and ETags

- **Reads**: `If-Match`, `If-None-Match`, `If-Modified-Since` and `If-Unmodified-Since` on GET and
  HEAD, after RFC 7232. If-Match true and If-Unmodified-Since false is a 200, and If-None-Match
  false and If-Modified-Since true is a 304 (`GetObjectRequest$IfMatch`, line 35696, and the
  three after it).
- **Writes**: `PutObject` takes `If-None-Match: *`, write only if absent, and `If-Match: <etag>`,
  write only if the current object has that ETag. A failure is 412, and a conflicting operation
  during the upload is 409 `ConditionalRequestConflict` (lines 44743-44750). `DeleteObject` takes
  `If-Match` (line 31720), `CompleteMultipartUpload` both (lines 28746-28753), and `CopyObject`
  both on its destination (lines 29227-29234). "If multiple conditional writes or copies occur
  for the same object name, the first write operation to finish succeeds" (*a rendering*,
  `conditional-writes`). A delete marker as the current version counts as absent for
  `If-None-Match` and fails `If-Match` with 404 (*a rendering*, the same page).
- **The ETag** "reflects changes only to the contents of an object, not its metadata. The ETag
  may or may not be an MD5 digest of the object data". It is an MD5 for a plain PUT unencrypted
  or under SSE-S3, and "not an MD5 digest, regardless of the method of encryption" for a
  multipart object (`Object$ETag`, line 40781). For multipart, S3 takes the MD5 of each part as
  it is uploaded, then the MD5 of the concatenated digests, with a dash and the part count
  (*a rendering*, `checking-object-integrity-upload`). The model itself disagrees once:
  `ObjectVersion$ETag` says it "is an MD5 hash" (line 41402).

That is F68's conditional write with a tag for its field. `If-None-Match: *` is
`WriteCondition::Absent`, and `If-Match` is `Matches` on the object's ETag. The commit decides,
in committed order, the first to finish wins, and a loser gets a typed refusal
([F68](../features/conditional-writes.md)).

## 13. S3: checksums

- **Algorithms**: CRC-32, CRC-32C, CRC-64/NVME, SHA-1, SHA-256, and since this model also
  SHA-512, MD5, XXHASH64, XXHASH3 and XXHASH128 (`ChecksumAlgorithm`, lines 28261-28318). One an
  object (*a rendering*, `UsingMetadata`).
- **Two types**: `FULL_OBJECT` and `COMPOSITE` (`ChecksumType`, lines 28364-28373). A single PUT
  is always full object (line 44507). CRC-64/NVME "is always a full object checksum" (line 28663).
  CRC-32 and CRC-32C may be either, chosen when the upload starts. The SHA family, MD5 and XXHASH
  can only be composite (*a rendering*, `checking-object-integrity-upload`).
- **The default** is CRC-64/NVME: "if objects are uploaded without a checksum, S3 automatically
  attaches the recommended full object CRC-64/NVME (CRC64NVME) checksum" (*a rendering*, the same
  page); the model agrees (`Checksum$ChecksumCRC64NVME`, line 28205).
- **A full object's CRC is made from its parts'**: "Full object checksums in multipart uploads
  are only available for CRC-based checksums because they can linearize into a full object
  checksum ... S3 can compute the checksum of the whole object from the part-level checksums"
  (*a rendering*, the same page). A composite one is a checksum of the parts' checksums with "-N",
  which needs every part's checksum and its boundary.

S17 recalled that S3 offers full-object CRC-64/NVME and CRC-32C, "which a combine could answer
without reading the object" ([X5](checksums.md#what-x5-does-not-settle)). **Confirmed**: S3 says
it does exactly that, and offers CRC-32 the same way. **X5's checksum and its combine are S3's
default**: a whole object's CRC-64/NVME, combined from its units', is the value S3 returns for
`ChecksumCRC64NVME`, if the units' CRCs can be had without the bytes. Whether the row keeps a
digest a chunk is still [Q21](contract.md#q21-in-part-the-checksum-2026-10-03)'s open point, and
this is one more use for it.

## What the metadata must keep possible

This is [Q32](contract.md#questions-to-answer)'s answer: what a later listing index and a later
S3 gateway need, set against what the objects page's rows hold ([S3](objects.md)). A gateway is still out of scope.
The list is of what must not be made impossible, since a row that cannot hold it is a schema
change, and a [schema change is a new cluster](../distributed/protocol.md#q10-at-m10a).

| A gateway or an index needs | Because | What the objects page's rows have, or must keep possible |
| --- | --- | --- |
| **The whole key, compared as bytes** | S3 lists in UTF-8 byte order, case-sensitive and unnormalised, at most 1,024 bytes ([10](#10-s3-listing)) | `ObjectMeta` keeps the whole path for [path identity](objects.md#path-identity). Nothing may normalise it, and a listing index orders by its bytes |
| **A listing that sees a write as soon as it is acknowledged** | S3 promises list-after-write and list-after-delete ([10](#10-s3-listing)). A listing index is ordered by path, and `ObjectMeta` is partitioned by a hash of it, so the two are in different tablets, and [P6](../distributed/protocol.md#the-contract) has no commit across tablets | RGW's shape ([8](#8-rgw-head-tail-and-index)): an index entry marked pending before the object's commit and completed after, and a lister that meets a pending entry asks `ObjectMeta`. The row needs only the object id and its version, which it has |
| **An ETag that changes with the content and not with metadata** | S3 compares it in `If-Match` and reports it in every listing entry. S3 itself does not promise an MD5: multipart and encrypted objects get none ([12](#12-s3-conditional-requests-and-etags)) | Derived from the object id and the version the commit bumps for a change of content. **No MD5 on the write path**. A gateway that wants MD5 ETags for single PUTs computes them itself as the bytes arrive, since an MD5 cannot be combined or made later without a read |
| **A conditional write on the ETag, and on absence** | `If-Match`, `If-None-Match: *`, first to finish wins ([12](#12-s3-conditional-requests-and-etags)) | [F68](../features/conditional-writes.md)'s `Matches` and `Absent` on `ObjectMeta`, as the object's own writes already use them. A delete marker would count as absent |
| **The full object's CRC-64/NVME, without a read** | It is S3's default checksum, and S3 makes a multipart object's from its parts' CRCs ([13](#13-s3-checksums)) | X5's combine makes it from the units' checksums, if those can be read without the bytes. That is [Q21](contract.md#q21-in-part-the-checksum-2026-10-03)'s open digest a chunk, and one more reason for it |
| **A multipart object's parts after completion**: number, size and checksum, up to 10,000 | `GetObject` by `PartNumber`, `GetObjectAttributes`, and a composite checksum ([11](#11-s3-multipart)) | Not in `ObjectMeta`: 10,000 parts would break [P18](contract.md#the-contract)'s bounded row. A table of a gateway's own, keyed by the object id, beside the bucket's |
| **Content type, content encoding, cache control and the rest, and user metadata up to 2 KB; up to ten tags** | Returned on every HEAD; tags change without a new version or ETag ([10](#10-s3-listing), [12](#12-s3-conditional-requests-and-etags)) | A bounded attribute field in `ObjectMeta`, at most a few KiB, which a change of tags rewrites without touching the content's version |
| **A modification time** | `LastModified` in every listing entry and HEAD, and `If-Modified-Since`. A multipart object's is when its upload began | The commit's time, which the row can stamp, and for a gateway's multipart its own start time |
| **Versions and delete markers** | S3's versioning: a version id an object, newest first, a delete marker as a data-less version ([10](#10-s3-listing)) | Not designed. An object id is minted per object and never reused, which a version id could be, but nothing here keeps old versions. Left out, and said so |

**The four that shape M12's rows** are the ETag derived and not hashed, the bounded attribute
field, the unnormalised path, and the object id and version a pending index entry names. Each
costs a field or a rule now and saves a schema change later. The rest are a gateway's own tables
or are not designed, and need nothing of M12.

## The comparison

Every claim a page leaned on, what the source says, and what the lab saw:

| Claim | Leaned on by | Read at `v20.2.0` | Observed | Verdict |
| --- | --- | --- | --- | --- |
| An acknowledgement waits for every shard of the acting set | S17; S18's alternatives | `src/osd/ECCommon.cc:824-955`; `src/osd/ECCommonL.cc:881-888`; `src/osd/ReplicatedBackend.cc:598-600` | E2 | **In part**: every shard sent the write; an optimized pool sends nothing to an untouched data shard |
| Monitors fence a primary by map epochs | S17; S18's alternatives | `src/osd/PG.cc:1934-1938`, `:1991-2028`; `src/osd/osd_types.cc:4417-4419`; `src/osd/PrimaryLogPG.cc:853-882` | – | **Wrong**: the OSDs fence by interval; the monitors commit maps and `up_thru`; reads by a lease |
| A PG below `min_size` blocks reads as well as writes | S17 | `src/osd/PeeringState.cc:6825-6829`; `src/osd/PrimaryLogPG.cc:1920-1927` | E1 | **Held** |
| `min_size` of K+1 or more | S17; P11 | `doc/rados/operations/erasure-code.rst:580-581`; `src/mon/OSDMonitor.cc:7800-7805` | E1 | **Held as advice**; the default is k when m is 1 |
| A two-phase write, committed in place with undo aside | S7; S6; S17 | `src/osd/ECTransaction.cc:829-869`; `src/osd/PGBackend.cc:282-391` | – | **Held**, in both back ends |
| A prepare into a temporary object, an apply that moves it | S7; S17 | `doc/dev/osd_internals/erasure_coding/proposals.rst:60-88` | – | **A proposal, not built** |
| Partial writes, parity delta, a version a shard | S8; S17 | `src/osd/osd_types.h:4510`, `:6263`, `:3061-3062`; `src/osd/ECTransaction.cc:208-231` | E3 | **Held**, as two records and a per-PG summary, for optimized pools only |
| The stripe unit: 4 KiB, 16 KiB advised with optimizations | S8 | `src/common/options/mon.yaml.in:16-27` | – | **Held**; the advice changes no default |
| Units dealt round-robin over the data shards | S8 | `src/osd/ECUtil.h:632-643` | – | **Held** |
| Tentacle eliminates padding | S8 | `src/osd/ECUtil.h:614-629` | – | **Held**, for optimized pools |
| ISA-L is Ceph's default code | X4 | `src/common/options/global.yaml.in:2617-2621` | setup | **Held**, since Tentacle |
| An EC shard checks its chunk against its own checksum; shards compared by an XOR summary | S11; S17 | `src/osd/ECBackendL.cc:1797-1835`; `src/osd/ECBackend.cc:1223-1224`; `enhancements.rst:713-728` | E4 | **In part**: only a pool that never overwrites; the summary is not built |
| Overwrites need BlueStore, whose checksums deep scrub relies on | S17 | `src/mon/OSDMonitor.cc:8885-8895` | – | **Held** |
| Light daily, deep weekly, three at once, 512 K a read | S11; S17 | `src/osd/scrubber/scrub_job.cc:115-118`, `:251-255`; `src/common/options.h:424-426` | – | **Held**: drawn around those, and 512 K is 524,288 bytes |
| Scheduler classes for client, recovery, and background work | S13; S17 | `src/osd/scheduler/OpSchedulerItem.h:33-38`, `:204-211` | – | **Held**: four classes, recovery split by urgency |
| A log on every OSD makes recovery local and fast | S10 | `src/osd/PeeringState.h:1484`; `src/osd/PeeringState.cc:1929-1940`, `:3054-3106` | – | **In part**: a log on each OSD holding a shard, local only within 250 to 10,000 entries |
| Small writes deferred on HDD, not on SSD | S6 | `src/os/bluestore/BlueStore.cc:16340-16394`, `:17162` | – | **In part**: an overwrite under one allocation unit is deferred on SSD too |
| crc32c by default, xxhash offered | S17; X5 | `src/common/options/global.yaml.in:4529-4543` | – | **Held** |
| straw2 moves only to or from the item that changed | S5 | `doc/rados/operations/crush-map.rst`; `src/crush/mapper.c:315-353` | E6 | **Held within a bucket**, not down the hierarchy |
| `crush_ln` exists because libm's `ln` is not exact | S5; X2 | `src/crush/mapper.c:228-339`; `src/crush/crush_ln_table.h` | – | **No reason given** in the source |
| `indep` keeps positions by choosing independently | S17; S5; X2; S18 | `src/crush/mapper.c:633-805` | E6 | **Held**, and dearer than X2's model of it |
| The balancer corrects unequal-weight bias, with weights and exceptions | S17; X2 | `src/pybind/mgr/balancer/module.py:1188-1410`; `src/osd/OSDMap.cc:5675-5995` | – | **Held**; its exceptions count PGs, not bytes |
| A "second movement" when an OSD leaves the CRUSH map | X2 | `doc/rados/operations/add-or-rm-osds.rst:320-326` | E6 | **The movement is real**; the phrase is not Ceph's |
| A replacement reuses the OSD's id (`osd destroy`) | X2 | `src/mon/OSDMonitor.cc:13064-13067` | – | **Held** |
| Device class set at start to hdd, ssd or nvme | S4 | `src/osd/OSD.cc:4968-4999`; `src/os/ObjectStore.h:349` | – | **In part**: hdd or ssd from the rotational flag; nvme only through SPDK |
| One OSD a disk | S17; S13 | `src/ceph-volume/ceph_volume/devices/lvm/batch.py:230-235` | – | **In part**: the default; several for NVMe |
| One RADOS pool serves RGW, CephFS and RBD | S4; overview; S17 | `src/mon/OSDMonitor.cc:9484-9487`; `doc/rados/operations/pools.rst:229` | – | **Wrong**: side by side in a cluster, a pool each |
| RGW defers a replaced tail for two hours | S17 | `src/rgw/driver/rados/rgw_rados.cc:6010-6049`; `src/rgw/driver/rados/rgw_gc.cc:120-138`; `src/rgw/rgw_op.cc:2380-2381` | E5 | **Held**; a timer, the defer-on-read disabled |
| The head inlines up to `rgw_max_chunk_size` | S3; S17 | `src/common/options/rgw.yaml.in:84-101` | E5 | **Held**, in the default storage class |
| RADOS carries a truncate sequence on every operation | S3 | `src/include/rados.h:415-433`, `:585-593`; `src/osd/PrimaryLogPG.cc:6765-6803` | – | **Wrong**: extent operations from CephFS; a weaker guarantee |
| Crimson and SeaStore, thread-per-core | S17 | `doc/dev/crimson/crimson.rst:71-96`; `src/crimson/osd/ec_backend.cc` | – | **Read**: a tech preview, replicated pools only |
| S3 offers full-object CRC-64/NVME and CRC-32C, a combine | X5; S18 | model lines 28205, 28663; the guide's `checking-object-integrity-upload` | – | **Held**, and CRC-32 too; CRC-64/NVME is the default |

## What would have changed the design

The spike page named the result that would change the design: anything S17 recalled that turns
out otherwise where a page leans on it.

- **It came out, for three claims, and each page is corrected**:
  - The objects page, [S3](objects.md#size-holes-and-truncate), leaned on the truncate sequence for its floors. The floors
    were already stronger than Ceph's mechanism, so they stay, and the sentence is corrected
    ([4](#4-a-truncate)).
  - Three pages said one RADOS pool serves RGW, CephFS and RBD. A pool serving several kinds of
    consumer is now this part's own choice and said to be ([9](#9-how-osds-are-deployed-whom-a-pool-serves-and-crimson)).
  - S17 and the contract's alternatives said Ceph's acknowledgement waits for every holder.
    Tentacle's optimized pools skip the untouched ones, which is the same thing
    [S8](erasure-coding.md#a-partial-overwrite)'s label a chunk does here, so the comparison is
    corrected, not the design ([1](#1-what-an-acknowledgement-waits-for)).
- **Fencing came out otherwise and changes nothing here.** The monitors do not fence a primary:
  the OSDs drop what an older interval sent, and reads are fenced by a lease
  ([2](#2-peering-fencing-and-min_size)). [P5](../distributed/protocol.md#the-contract) already
  forbids this part's control plane either role.
- **Positions came out as recalled, and dearer.** Ceph's own `indep` moves 2.0 to 3.5 times the
  least for a device added, removed or reweighted on X2's shapes. That makes
  [Q19](contract.md#q19-in-part-placement-2026-10-03)'s choice, positions held as state at 1.00×,
  firmer, not different ([5](#5-placement-indep-upmap-and-crush-compat)).
- **Scrub came out as no comparison at all**, which moves no decision, since
  [S11](scrub.md#what-a-deep-scrub-proves-and-what-it-does-not) did not lean on Ceph having one.
  It does take away the precedent: Q28's parity check is new ground
  ([7](#7-scrub-the-scheduler-and-what-a-deep-scrub-of-an-erasure-coded-pool-verifies)).

## Recommendation

What [S18](contract.md#q32-and-q14-q20-q28-in-part-ceph-and-s3-at-the-source-2026-10-05)
records:

1. **Q32: the metadata leaves room for a listing index and a gateway in four places.** An ETag
   derived from the object id and its content version, never an MD5; a bounded attribute field
   for content type, user metadata and tags; the path compared as unnormalised bytes; and an
   index that is updated in two phases, pending then complete, as RGW's is, naming the object id
   and version. A multipart object's parts, if a gateway is built, are a table of its own. Versions
   are not designed.
2. **Q28, in part: a deep scrub of an erasure coded pool checks its chunks against each other**,
   by S11's summaries or by encoding again, since Ceph's checks nothing across shards of an
   overwritable pool and the lab showed a parity chunk corrupted that nothing noticed. How often,
   and at what budget, is X12's.
3. **Q20, in part: nothing of the geometry is taken from Ceph.** Ceph deals units round-robin
   over the data chunks, defaults to a 4 KiB unit and advises 16 KiB with optimizations, which is
   where X4 found encoding at full rate on Zen1. Those agree with X4 and X6 and decide nothing they
   did not; the geometry stays open.
4. **Q14, in part: the alternative B is measured against is Ceph as read.** One durable round of
   sub-writes to every shard written, undo on each of them, and a read round first for a partial
   write. Since Tentacle a partial write writes only the touched data chunk, chunk 0 and the
   parity. X1, X3 and X8 still decide Q14. ✅ X1 has since recorded its safety
   ([X1](stripe-model.md)): B holds under the model with redo alone, so undo is not what it
   needs. ✅ X3 and X8 have since priced it ([X3](bytes-through-groups.md), [X8](small-writes.md)):
   B's second round is 1.7 ms of a 7.0 ms small write on the lab's 970 EVO, which is the round
   Ceph does not pay.

## What X14 does not settle

- **Q32's rows themselves**: which fields M12's generated rows carry, at what bounds. M12.
- **A listing index**: its table, its two phases and its lister. After M21, if a listing is
  wanted.
- **Versioning**, which nothing here designs.
- **The geometry** (Q20): X4 and X6 bound it from below; nothing decides it yet.
- **Q28's cadence and budgets**: X12.
- **Whether CephFS's capabilities stop the stale-write interleaving** the OSD's truncate clip
  lets through. Not traced; it does not bear on the objects page's floors.

## What it did not read or measure

- **Ceph's performance.** The lab's Ceph was run to see what it does, never how fast. The times
  on this page are the failure detector's (E2) or one run's at one write outstanding (E3), and
  none is a rate to compare.
- **`crush-compat`** ran nowhere: the balancer needs a cluster with data. It was read
  ([5](#5-placement-indep-upmap-and-crush-compat)).
- **The upmap balancer** was read, not run. Its exceptions are counted in PGs against a
  weight-proportional target, which X2's byte-filled exceptions are not.
- **Peering under partitions.** E1 stopped OSDs cleanly and E2 paused one. Nothing was
  partitioned or killed outright, so the lease that fences reads
  ([2](#2-peering-fencing-and-min_size)) was read and not watched.
- **MinIO, HDFS, ZFS, Haystack, f4, SeaweedFS, GlusterFS, Azure and md RAID**, which S17 lists as
  recalled. They stay recalled. md RAID's words and ZFS's soundness are leaned on, and S17 says
  so now.
- **S3 itself.** The model and the guide were read; no request was made to AWS, and RGW is not
  S3.

## Defects found upstream

- **cephadm 20.2.0 cannot bootstrap on Ubuntu 26.04** as shipped. `install -o 167 -g 167` meets
  uutils coreutils 0.8.0, which refuses a numeric owner with no passwd entry; GNU coreutils takes
  any number. A user at 167 is the workaround. Not filed: it is Ubuntu's and Ceph's to settle.
- **`ceph orch daemon add osd <host>:<lv>`** answers "No devices found" before the manager's
  first inventory of a host, yet saves the spec, and a second add replaces the first's.
- **Stale documentation in Ceph's tree**:
  - `doc/dev/osd_internals/erasure_coding/ecbackend.rst:97` says "With overwrites, all scrubs are
    disabled". The code reads every shard and skips only the hash.
  - `enhancements.rst:702-704` says the CRC is still updated on overwrite; the code clears it
    (`src/osd/ECUtilL.h:258-259`).
  - `doc/dev/osd_internals/log_based_pg.rst:129-130` says a read needs `m` shards; the code needs
    k.
  - `doc/ceph-volume/lvm/batch.rst:32-33` says two OSDs are made for each SSD; the code makes one.
  - `rgw_gc_processor_max_time` is described as the time between cycles; the code uses it as a
    lease (`src/rgw/driver/rados/rgw_gc.cc:581-583`).
- **In AWS's model**: `ObjectVersion$ETag` says "an MD5 hash", which `Object$ETag` denies. A
  multipart part number out of order with checksums is a 400 in the model and a 500 in the guide.
  The largest object is "50 TB" in `CopyObject` and 48.8 TiB in the guide.

## Related

[S17](prior-art.md) for the claims and their labels; [S18](contract.md#q32-and-q14-q20-q28-in-part-ceph-and-s3-at-the-source-2026-10-05)
for what was decided; [Spikes](spikes.md#x14-ceph-and-s3-at-the-source) for the plan; [X2's
record](placement-simulation.md) for the movement figures E6 is read beside; [X5's
record](checksums.md) for the checksum S3 turns out to default to; [S11](scrub.md) for the scrub
Q28 is about; the objects page, [S3](objects.md), for the rows Q32 is about.
