# 301. Shoal's settle between bench arms was a ten second sleep, and a put arm's reclamation ran into the next

After an object arm, the bench deletes what the arm put and runs each store's own settle command,
which [F94](../../features/bench-object-endpoints.md#between-arms-reset-or-delete) says gives the
store's space back before the next arm. Shoal's was `sleep 10`. Where an arm put thousands of
stripes, their reclamation took far longer than that, and ran on through the next arm on every
shard of every node. The comparison said neither of its reclamation settings was on an arm's path,
and for that arm it was.

Found in [F103](../../features/plaintext-peer-lanes.md)'s run on the lab (2026-10-23), filed as item
301, and fixed before the next run.

## Symptom

- **On the loop leg at 2+1**, 64 KiB puts at sixteen in flight put about 84,000 stripes, and in every
  round's order 1 MiB puts at one in flight came next.
  - In the last seven seconds of the settle's sleep, with no client operation in flight, each
    node's three shards used 1.9 to 2.1 cpus.
  - Through the next arm's measured window they used 1.6 to 1.75, where 64 KiB puts at one in
    flight, which follow no deletes, used 0.41 to 0.49.
  - That arm wrote 3.6 device bytes for every byte sent, against 2.1 for 1 MiB puts at sixteen in
    flight.
- **On the SSD leg**, O120's run had seen the same after 9,000 deleted stripes (2026-10-20): 37 to 47
  seconds of each NVMe's flushes, and a 4 KiB get arm's p99 at 2.83 ms against 0.81
  ([Deleting them](../../object-storage/store-optimizations/inline-threshold.md#deleting-them)).

## Cause

The settle's length was the reclamation's grace, not its work. The lab's inventories set
`reclaim_grace` to five seconds and `reclaim_poll` to two, so a retired object becomes reclaimable
within seven seconds of its delete, and ten seconds waited that out. Reclaiming is then work of its
own, a pass over the entries each shard leads and every holder's chunks removed under a seal
([F92](../../features/reclamation-and-device-lifecycle.md)), and its length grows with what was
deleted: about forty seconds for 84,000 stripes on the loopback. The other stores' settles wait for
their work: Ceph's runs RGW's garbage collection through, SeaweedFS's vacuums its volumes, and on
the loop leg MinIO's and RustFS's wait for their devices' space to come back. O120's page said so,
and nothing was filed.

## Evidence

**Established by the lab's samples**, not by a test: each node's threads sampled every second through
`f103-loop-ec21` ([F103's figures](../../features/plaintext-peer-lanes.md#on-europas-loopback),
`shoal-spike/results/stores-ab/f103-threads.txt`), the same in all sixteen runs of the arm. A
capture keeps each settle's length (`settle_secs`): ten seconds every time, however much the arm
before had put.

## The fix

- **`reclaim wait --steady <secs>`.** The tmdb dataset's loader, whose `reclaim wait` read every
  device's chunk bytes until they were within a margin, stops too once they have held the same for a
  window (`held_steady`).
- **Shoal's settle uses it** (`stores-lab.sh`, `shoal_settle`): the grace and a pass slept out, then
  `reclaim wait --steady 6` against the bench's own cluster, through the inventory the bench writes
  into the capture before its first arm. The repository's own loader runs it
  (`SETTLE_LOADER`), so a side built before the fix settles the same way.

## Alternatives rejected

- **A longer sleep.** What an arm leaves to reclaim varies a hundredfold between arms, and a sleep
  long enough for the most is minutes wasted after every other.
- **A margin of the preload's chunk bytes**, which `reclaim wait` already took. The settle would
  need to know what the preload left on every device, and a margin a byte off waits out the timeout.
- **Every host's filesystem read until it stops changing**, as MinIO's settle reads its devices. A
  Shoal node's roots and devices share filesystems on the lab, and its log and archives move while
  it reclaims.
- **The bench waiting on Shoal's reclamation itself.** The bench is generic over stores, and each
  store's settle is the operator's command.

## Invariants to uphold

- **A settle ends only once what a store frees has been freed**, so the next arm measures the store
  and not its housekeeping. For Shoal that is every device's chunk bytes holding the same.
- **The steady window starts after the reclamation's grace**: the bytes hold the same before
  reclaiming begins too, so the settle sleeps the grace and a pass out first.
- **The settle reads the bench's own cluster**, never the inventory the lab script started from.

## Still open

- ~~**The comparison's captures were taken with the old settle.** Its figures for an arm after a put
  arm measure Shoal beside its own reclamation, and are not taken again here.~~ **The comparison's
  first run was taken with the old settle**, so its figures for an arm after a put arm measure Shoal
  beside its own reclamation. Shoal's second run (2026-10-25 and 26) took this settle, which waited
  10 to 54 seconds an arm, and on the loop leg its read arms after the puts wrote 0 to 18 MB a run
  ([Shoal measured again](../../object-storage/store-comparison.md#shoal-measured-again)). An arm
  that only reads writes nothing to a device, so what one wrote shows where the reclamation reached.
  In the first run:
  - on the loop leg, the 4 KiB gets and stats at one in flight, which followed the 1 MiB puts and
    the 1 MiB mixed arm at sixteen, wrote 256 to 384 MB to the devices in each run at 2+1, and 16
    to 82 MB with three copies, where every read arm after a read arm wrote none;
  - on the SSD and disk legs, whose arms put fewer stripes, those arms wrote 11 MB or less.

  A put or mixed arm writes for itself, so how much of an arm after another was reclamation cannot
  be told from those captures.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `tmdb-dataset` `reclaim::tests::chunk_bytes_hold_steady_once_unchanged_for_the_window` | A total still falling, or the same for less than the window, is taken as steady, or one the same for the window is not |

The lab's run after the fix is its check: a settle as long as the arm's reclamation (`settle_secs`),
and an arm after a put arm with no reclaiming in it. Lab 2 of the store optimizations ran it on the
loop leg at 2+1 (2026-10-23, `w2-loop-ec21`, six captures):
- **Each settle was as long as what its arm left.** It took 52 to 54 seconds after the 64 KiB puts
  at sixteen in flight, which deleted 85,000 to 96,000 objects, and 20 to 24 after the 1 MiB puts
  at sixteen (19,000) and the 64 KiB puts at one (14,000). After the 1 MiB puts at one (7,600) it
  took 18, the settle's least: the grace and a pass slept out, then six seconds steady. Every
  settle before the fix was ten. With three copies (`w2-loop-rep3`) they took 18 to 52 seconds,
  after the same arms. On the SSD leg at 2+1 (`w2-ssd-ec21`) they took 44 to 52 seconds after the
  64 KiB puts at sixteen in flight and 16 to 28 after the others.
- **The arm after the 64 KiB puts ran without it.** On F104's build, the one F103's run measured,
  the 1 MiB puts at one in flight ran 219 a second, where they ran 128. The nodes spent 8.0
  cpu-seconds a GiB on them, where they spent 57.4, and each node's shards used about a third of a
  cpu, where they used 1.6 to 1.75. On the SSD leg the same arm ran 25.0 to 25.4 a second, where it
  ran 17.6 to 17.9. Its p99 was 79.4 to 85.6 ms, where it was 101.6 to 108.8: the 1 MiB half of
  [O131](../optimizations.md#o131-a-puts-tail-at-one-in-flight-across-hosts-is-twice-as-long-under-tls)'s
  tail was measured beside this reclamation.

## Related

- [F94](../../features/bench-object-endpoints.md#between-arms-reset-or-delete): the settle between
  arms; [F92](../../features/reclamation-and-device-lifecycle.md): reclamation.
- [O126](../optimizations.md#o126-a-holder-syncs-two-directories-for-every-chunk-it-reclaims), what
  reclaiming costs a device; [O120's page](../../object-storage/store-optimizations/inline-threshold.md#deleting-them),
  which saw it first.
- [The store comparison](../../object-storage/store-comparison.md).
