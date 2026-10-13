# X14: what the reading predicts, written before the lab ran

Every prediction below was written from Ceph's source at `v20.2.0` (commit `69f84cc`) before the
experiment it predicts was run on the lab, and is not edited afterwards. Paths are relative to the
Ceph repository. The results are in `x14-e*.txt` beside this file, and the comparison is on
`docs/src/object-storage/ceph-and-s3-sources.md`.

## E1. Below `min_size`, reads block as well as writes

- An EC pool's `min_size` is `k + min(1, m - 1)` (`src/mon/OSDMonitor.cc:7800-7805`): 5 for 4+2, 2
  for 2+1. A replicated pool of size 3 has 2 (`src/common/config.h:355-358`).
- A PG whose acting set is smaller than `min_size` is `peered`, not `active`
  (`src/osd/PeeringState.h:2449-2451`, `src/osd/PeeringState.cc:6825-6829`), and every client op,
  read or write, waits for active before `do_op` tells them apart
  (`src/osd/PrimaryLogPG.cc:1870-1880`, `:1920-1927`). An EC pool has no read that bypasses the
  primary in v20.2.0, and balanced reads are for replicated pools only
  (`src/osdc/Objecter.cc:3106-3108`).
- Recovery below `min_size` is allowed by default (`osd_allow_recovery_below_min_size`,
  `src/common/options/osd.yaml.in:865-869`; `PeeringState.cc:2333-2337`), so the state is
  `undersized+degraded+peered`, not `incomplete`.

| Case | Predicted state | `rados get` | `rados put` | After `min_size` lowered |
| --- | --- | --- | --- | --- |
| 4+2, two shard OSDs stopped (4 = k left) | `undersized+degraded+peered` | blocks | blocks | `min_size 4`: active, both return |
| 2+1 at `min_size 3`, one stopped (2 = k left) | `undersized+degraded+peered` | blocks | blocks | `min_size 2`: active, both return |
| replicated 3/2, two stopped | `undersized+degraded+peered` | blocks | blocks | `min_size 1`: active, both return |

`ceph orch daemon stop` refuses a stop that would make a PG inactive unless forced
(`src/pybind/mgr/cephadm/module.py:2678-2682`; `src/mgr/DaemonServer.cc:1062-1064`).

## E2. What an acknowledgement waits for

- Replicated and legacy EC wait for a durable commit from every shard of
  `acting_recovery_backfill` (`src/osd/ReplicatedBackend.cc:598-600`;
  `src/osd/ECCommonL.cc:883-888`, `src/osd/ECBackendL.cc:1194-1200`).
- The optimized EC backend (`allow_ec_optimizations`) sends nothing to a shard whose transaction is
  empty and does not wait for it (`src/osd/ECCommon.cc:824-840`, `src/osd/ECBackend.cc:917-926`).
  A partial write always writes data shard 0 and the parity shards
  (`src/osd/ECTransaction.cc:590-592`), and the data shards it touches.
- A partial overwrite reads first unless the extent cache holds the bytes. On 4+2, a 4 KiB write into
  one data shard's unit reads three shards either way, so `ec_pdw_write_mode=0` reconstructs on the
  tie (`src/osd/ECTransaction.cc:208-231`); mode 2 forces parity delta, reading the shard and the
  parity.
- A stopped (SIGSTOP) OSD stays in the acting set until it is marked down: two reporters from two
  hosts and a grace of 20 s (`src/common/options/global.yaml.in:1927-1946`, `:2910-2913`).

| Pool, write | Predicted |
| --- | --- |
| optimized 4+2, mode 2, 4 KiB into data shard 1's unit, data shard 2's OSD stopped | acknowledged at normal latency: reads {1, 4, 5}, writes {0, 1, 4, 5}, nothing to shard 2 |
| optimized 4+2, mode 0 (or 1), the same write | blocks on the read of shard 2 (reconstruct reads {0, 2, 3}) until shard 2's OSD is marked down, then completes by parity delta |
| optimized 4+2, any mode, 4 KiB into shard 2's unit | blocks until shard 2's OSD is marked down (its commit, or in mode 2 its read) |
| legacy overwrite 4+2, either write | blocks until the OSD is marked down: the whole stripe is read and all six shards are written |
| a PG without the stopped OSD | normal latency |

## E3. Which shards a small overwrite writes

| Pool | Data written to | Metadata and log only | Nothing | Parity by |
| --- | --- | --- | --- | --- |
| legacy overwrite 4+2 | all six shards (touch, clone of the old range for rollback, the chunk, hinfo, object info) | none | none | full encode after reading the stripe |
| optimized 4+2, mode 0 or 1 | the touched data shard and both parity shards | data shard 0 (object info, snapset, log entry) | the two other data shards | full encode after reading the three untouched data shards |
| optimized 4+2, mode 2 | the same | the same | the same | delta: reads the touched shard and both parity shards |

Measured by each OSD's `bluestore` counters (`txc_count`, `write_small`, `write_small_bytes`,
`write_big_bytes`, `omap_setkeys_bytes`) around a run of writes, less an idle baseline
(`src/os/bluestore/BlueStore.cc:6208-6417`). `subop_w` counts the replicated backend only
(`src/osd/ReplicatedBackend.cc:107-115`). The mode is read when a PG is built
(`src/osd/ECCommon.h:664`), so an OSD restarts after it is changed.

## E4. What a deep scrub of an EC pool verifies

- `ceph-objectstore-tool ... set-bytes` truncates and rewrites the shard through BlueStore, so it
  carries fresh, valid checksums, and leaves its xattrs alone (`src/tools/ceph_objectstore_tool.cc:2531-2552`).
- Plain EC (no overwrites): each shard compares a running crc32c of its chunk with the cumulative
  hash in its `hinfo_key` xattr and reports `ec_hash_error` (`src/osd/ECBackendL.cc:1797-1818`).
- Legacy with overwrites: the digest is set to 0, "partial overwrites don't support deep-scrub yet"
  (`src/osd/ECBackendL.cc:1831-1835`).
- Optimized: the digest is 0 (`src/osd/ECBackend.cc:1222-1226`).
- The primary compares `oi.data_digest` only for replicated pools
  (`src/osd/scrubber/scrub_backend.cc:1201-1225`). Nothing re-encodes parity, and the "longitudinal
  summary" of `doc/dev/osd_internals/erasure_coding/enhancements.rst:713-728` is not in the code.
- A whole-object read checks `oi.data_digest`, which `write_full` sets on any pool
  (`src/osd/PrimaryLogPG.cc:6880-6884`, `:5880-5882`, `:5404-5413`).

| Corruption | Plain EC | Legacy overwrite | Optimized |
| --- | --- | --- | --- |
| (a) one data shard's bytes changed, same length | `ec_hash_error` on that shard; `rados get` returns the original bytes, decoded around it | no inconsistency; `rados get` fails with EIO (`full-object read crc ... != expected`) | no inconsistency; `rados get` EIO |
| (a) one parity shard changed | `ec_hash_error`; `rados get` returns the original bytes | no inconsistency; `rados get` returns the original bytes (parity is not read) | the same as legacy |
| (b) every shard of B copied over A | `ec_hash_error` on every shard, no authoritative copy; `rados get` EIO | no inconsistency; `rados get` EIO | no inconsistency; `rados get` EIO |

## E5. RGW's tail after an overwrite

- A 6 MiB PUT is a 4 MiB head (`rgw_max_chunk_size`, `src/common/options/rgw.yaml.in:84-101`) and
  one 2 MiB tail object (`rgw_obj_stripe_size` 4 MiB, `rgw.yaml.in:1942-1954`).
- An overwrite replaces the head in one op and sends the old tail to GC with an expiry of the
  enqueue time plus `rgw_gc_obj_min_wait` (2 h) (`src/rgw/driver/rados/rgw_rados.cc:6010-6035`,
  `src/rgw/driver/rados/rgw_gc.cc:120-138`, `src/cls/rgw_gc/cls_rgw_gc.cc:71-72`).
- `radosgw-admin gc list --include-all`: one entry, one object `<marker>__shadow_.<random>_1`, its
  time the overwrite plus 7,200 s. Without `--include-all` the list is empty until then.
- The defer-on-read the option's description mentions is disabled (`src/rgw/rgw_op.cc:2380-2381`).

## E6. Ceph's own CRUSH, offline

- `crush_choose_indep` draws position `p` at round `f` with `r = p + n·f` at every level, rejects a
  domain any settled position holds, and leaves a position empty after its tries as
  `CRUSH_ITEM_NONE` (2147483647) at its index (`src/crush/mapper.c:633-805`). The EC rule tries 100
  rounds and 5 leaves (`src/crush/CrushWrapper.cc:2340-2366`).
- A device marked out (test weight 0) changes no draw, so its positions move within its host and
  nothing else moves: about the least.
- A device reweighted, added or removed changes its host's weight, so positions on its siblings
  move as well: more than the least, about 1.7 times at 4 devices a host on 8 hosts.
- A host out moves only its positions, at later rounds: near the least. Removing the host from
  the map moves more. Out and then removed moves data twice.
