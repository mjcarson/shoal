# Item 351's run: the predictions, written before it

Written 2026-11-01, before `FALL_ONLY=1 sh shoal-spike-scan/results/m23-lab.sh start` ran. The run
drives the cell alone, with no paced `Keyword` stream and no scan, for two arms of 200 s with 180 s of
rest between them, and samples every host each second. Then, with no Shoal unit running, `fio`
replays each host's flushes a second and bytes a flush in the same pattern.

What the captures said before the run
([item 351](../../../docs/src/appendix/known-issues.md#351-a-cells-steady-inserts-fall-to-three-fifths-of-their-rate-about-75-s-into-an-arm)):
- every long insert arm since X16 falls 75 to 80 s in, the two hour-long churns among them;
- the churns stay flat at the lower rate for the whole hour, at 10 MiB segments and at 40;
- the insert's whole distribution moves (p50 12 to 22 ms), and the reads' does not;
- titan's and hyperion's journals log no forced purge across the check round's fall.

## (A) The 970 EVO's flush regime

[X6](../../../docs/src/object-storage/device-store-ssd.md#two-things-about-the-labs-970-evos)
measured a 970 EVO's flush at about 0.9 ms after a rest and about 3 ms after a minute or so of
synced writes. If the drive is the cause:

1. **titan's and hyperion's NVMe, ms a flush**, steps from about 1 ms to about 3 ms at the cell's
   fall, or a few seconds before it. Their flushes a second do not rise at the step.
2. **europa's Optane** does not step.
3. **Shoal's own figures** stay level across the fall: each member's WAL appends a sync, its
   compacting segments, its apply lag, and its shards' cpu a second. No forced purge is logged.
   Each WAL's sync ms rises with the device's.
4. **The rest undoes it.** The second arm starts at the first arm's early rate, flushes back near
   1 ms, and falls again about as far in.
5. **fio alone** steps on titan and hyperion about as far into its synced writes, without Shoal,
   and resets after the same rest.

## (B) Something in Shoal

If the cause is Shoal's, something that builds over the cell's first ~75 s of writes and drains in a
pause:

1. one of Shoal's own figures steps at the fall: compacting segments, WAL appends a sync, apply lag,
   a shard's cpu, or a forced purge;
2. the NVMe's ms a flush does not step, or steps only after the fall, as a result of it;
3. fio alone holds its first rate through 200 s.

A result matching neither, for example a flush step that fio does not reproduce, is brought to the
user before anything is changed.
