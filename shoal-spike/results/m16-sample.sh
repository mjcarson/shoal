#!/bin/sh
# F88 on the lab: every second, a host's block devices' and NICs' counters, for reading what a
# failed device's moves copied and how fast (`docs/src/features/recovery-and-moves.md#on-the-lab`).
# It reads /proc and writes to stdout only, and runs until it is killed:
#
#   ssh titan sh -s < shoal-spike/results/m16-sample.sh > titan.log &
#   python3 shoal-spike/results/m16-rate.py titan.log dm-2 enp3s0 <from> <to>
#
# Each sample is a line `@ <unix seconds>` then the /proc/diskstats lines of every dm, nvme and loop
# device and the /proc/net/dev lines of every NIC whose name starts with e.
while :; do
    echo "@ $(date +%s.%N)"
    grep -E " (dm-[0-9]+|nvme[0-9]n1p?[0-9]*|loop[0-9]+) " /proc/diskstats
    grep -E "^ *e" /proc/net/dev
    sleep 1
done
