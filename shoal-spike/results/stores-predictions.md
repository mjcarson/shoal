# F94: what the store comparison is expected to show, written before its first leg ran

Written on 2026-10-18, after the quick tries of every store on the lab (one 64 KiB arm at four in
flight, two runs of five seconds, `--allow-dirty`) and before any leg of
`shoal-spike/results/stores-lab.sh`. Each statement names what would make it wrong. The page
reports them beside what was measured and judges none of them
([Shoal beside other object stores](../../docs/src/object-storage/store-comparison.md)).

What the quick tries already showed is not a prediction:
- **64 KiB puts.** Shoal's three-copy pool put 67/s, MinIO 384/s, SeaweedFS 388/s with its fsync,
  and Ceph 398/s at 2+1. Shoal wrote 197 device bytes for each byte sent
  ([O107](../../docs/src/appendix/optimizations.md#o107-a-stripe-shorter-than-its-chunk-costs-its-device-the-whole-chunk)).
- **64 KiB stats on the loop leg.** Shoal's ran at 19,626/s and MinIO's at 10,290/s.

## Rates

1. **A put between 64 KiB and 1 MiB is slower on Shoal than on every other store, in both groups
   and on every leg**, by at least half. A stripe takes a whole 4 MiB chunk on each holder whatever
   it holds (O107), where the others write about what they were sent. *Wrong if* any store's put
   interval overlaps Shoal's at 64 KiB or 1 MiB at sixteen in flight on the SSD leg.
2. **A 4 KiB put is no slower on Shoal than on the others.** Under the 16 KiB inline threshold an
   object is a row: one replicated commit through its tablet group, no chunk. MinIO and RustFS
   inline it too, into `xl.meta`; Ceph writes a RADOS object and a bucket index entry. *Wrong if*
   Shoal is behind any store at `put 4 KiB c16` on the SSD leg.
3. **A 4 KiB stat is fastest on Shoal**, as it was on the loop leg: one read of a row the shard
   holds in memory. *Wrong if* any store is ahead at `stat 4 KiB c16` on the SSD leg.
4. **At 16 MiB, puts are the network's on every store.** Each is bounded by what europa sends over
   its one 1 GbE link: with three copies, a coordinator or primary on europa sends two more; at 2+1
   the stripe's bytes once and a half. *Wrong if* two stores in a group differ by more than a
   quarter at `put 16 MiB c16`.
5. **A get of 1 MiB or more is within on every store at sixteen in flight**, the network's, except
   for what each reads from europa's own instance. *Wrong if* any store is ahead of Shoal by more
   than a quarter at `get 16 MiB c16` on the SSD leg.
6. **On the loop leg, with no link, the differences grow**, and Shoal's put deficit at 64 KiB to
   1 MiB is its largest there, since nothing hides the chunk cost.

## Latency

7. **Shoal's p99 at one in flight is within a factor of two of the best store's** on every arm
   but the puts of item 1.

## Costs

8. **Shoal's device bytes for each byte put are the highest of any store from 64 KiB to 1 MiB**,
   and the lowest at 4 KiB, where its put writes a WAL entry on each copy.
9. **Ceph's OSDs hold the most memory**, near `osd_memory_target`'s 4 GiB on each host, and
   SeaweedFS's processes the least.
10. **Shoal's cpu-seconds for each thousand 4 KiB operations are the lowest**, a thread a core
    serving rows, and Ceph's the highest.

## The HDD leg

11. **Every store but Shoal and Ceph is slower on disks than on SSDs at 4 KiB puts by a factor of
    ten or more.** Shoal keeps its rows, and so a 4 KiB object, on the SSD, and Ceph its RocksDB
    and its small writes in its DB on the SSD. MinIO, RustFS and SeaweedFS sync each put on the
    disk. *Wrong if* MinIO's `put 4 KiB c16` on the HDD leg is above a tenth of its SSD leg's.
