# Item 359's run: predictions, written before it (2026-11-04)

The run: six rounds of a round's preload and its `Big` capture alone, on the `m23` side cluster at
`b3a4d7ef`, the cell's arms warmed 100 s, the nodes at 6 GiB. Rounds 1, 4 and 5 run every scan setting
at its default (75 ms of a device's time a second, sixteen scan reads in flight a device). Rounds 2, 3
and 6 run `device_reads: 2` at the same share. Every host's disk and node threads are sampled each
second, pinned to cpu 7, and every member's own figures are read each second.

What the rounds again showed, which this run starts from: beside a whole scan of `Big` the cell's read
p99 rose 1.14 times (5.11 to 5.81 ms), and beside `Keyword` it did not rise. `Big`'s scan read 3.2 MiB
a second of each Zen1 host's 970 EVO in runs of up to a mebibyte, sixteen in flight, and 7 to 10% of
those bytes were read on the trickle, beside a foreground read of the member's own on the device.
Europa's scan read 2.2 MiB a second of its Optane, 0.3% on the trickle. `Keyword`'s scan took more of
a 970 EVO's time, 68.7 ms a second against 42.8, in small reads.

## If the slow reads wait in the device's queue behind the scan's reads

1. The Zen1 members' own get p99 rises during the scan, and europa's does not, or far less.
2. The Zen1 hosts' 970 EVO's time a read rises during the scan, from what a lone small read takes to
   several times that, and its queue holds the scan's reads.
3. At two scan reads in flight a device, the cell's read p99 rises less than half as far as at sixteen,
   while the scan's rate falls by less than half: its rise falls further than the rate does.
4. The shard threads' cores busy rise little during the scan, the same at both depths.

## If they wait for their shard's loop or its cpu

1. The Zen1 members' get p99 rises; europa's may too, on its own shards' steps.
2. The disks' time a read does not rise during the scan beyond what its MiB a second explains.
3. The shards' cores busy rise during the scan, and the busiest shard by more.
4. At two reads in flight the rise falls only as far as the scan's rate does.

## What I expect

The device's queue. Item 359's leads point there: the trickled bytes say the cell's reads were on the
970 EVOs while the scan's were, a scan of large reads hurt the cell where a scan of more device time in
small reads did not, and europa's lightly loaded Optane serves its share of the reads. I expect predictions
1 to 3 of the first list to hold, the Zen1 members' get p99 to rise about 20% during the scan and
europa's under 5%, and two reads in flight to take the client's ratio from about 1.14 to under 1.07 at a
scan rate of at least two thirds of sixteen's.

A miss of that sort would be a result too: if the shards' cores busy rise with the scan, or two reads in
flight change nothing, the loop is what to look at next.

# The replay: predictions, written before it (2026-11-04)

What the first two rounds left: two scan reads a device cut the scan's rate ninefold, its loop work with
its reads, so the sides could not tell the drive from the loop. The slow reads are all the Zen1 members'
(get p99 5 to 6 ms before a scan, 6 to 7 during, europa's 0.3 throughout, though europa's shards handed
out half of a Zen1 member's partitions). The scan's reads of a Zen1 host's 970 EVO average 10.5 KiB, not
a mebibyte, about 585 a second beside the cell's 70, and add about 0.7 reads in flight to a drive that
holds two to three of the WAL's requests and flushes in 2.8 ms.

The replay: four rounds, ABBA. In rounds 1 and 4, fio on titan and hyperion reads a file of its own on
the node's 970 EVO as the scan read it, 585 reads a second of 4 to 64 KiB (10.5 KiB on average),
Poisson spaced, up to sixteen in flight, for 223 s of the cell alone, from about where a scan is asked.
Rounds 2 and 3 keep the same times and run no fio. No Shoal scan runs in any of them.

## If the drive's queue is what the scan's reads cost the cell

1. With fio, the Zen1 members' own get p99 rises about as far as beside a scan, 1 ms or so, and europa's
   does not move.
2. The client's read p99 a second rises about as far as beside a scan, during over before about 1.15 to
   1.2, against about 1.0 in the control rounds, the ranges disjoint.
3. The Zen1 disks hold about as many reads in flight as beside a scan, 0.7 to 0.8, and the shards' cores
   busy do not rise.

## If they are not

The client's ratio with fio stays near the control's, and what a scan adds is in Shoal: its loop, its
cpu or something else its scan does beside the reads.

## What I expect

Most of it the drive's: fio's ratio at least two thirds of a scan's rise, about 1.12 or more, and the
control's about 1.0.
