#!/bin/sh
# Item 351 on the lab: every second, a host's whole NVMe and device-mapper counters, its cpu, and
# every thread of its side node, so the cell's inserts can be read against what each device and
# each thread did that second (m23-fall-report.py, item 351). It reads /proc alone, writes to
# stdout only, and runs until its deadline, or until its output closes:
#
#   ssh titan sh -s -- <deadline, unix seconds> < shoal-spike-scan/results/m23-fall-sample.sh > titan.log &
#
# Each sample is a line `@ <unix seconds>`, then the /proc/diskstats lines of every whole NVMe
# (the only devices that count flushes: an LV and a partition report none) and every dm device,
# then /proc/stat's `cpu` line, then a line `# threads <pid>` and one line a thread of the node:
# `<tid> <utime> <stime> <comm>`, in clock ticks.
set -u
deadline=$1
# the node's process, found again whenever the one sampled is gone; the bracket keeps the pattern
# from matching this script's own command line
pid=""
while [ "$(date +%s)" -lt "$deadline" ]; do
    # the time first, as nanoseconds since the epoch, which GNU's and uutils' `date` both print
    echo "@ $(date +%s.%N)" || exit 0
    # every whole NVMe and every device-mapper device, never a partition
    grep -E " (nvme[0-9]+n[0-9]+|dm-[0-9]+) " /proc/diskstats
    # the host's cpu in ticks, every core together
    grep -E "^cpu " /proc/stat
    # the node, found once and again only when it has gone
    if [ -z "$pid" ] || [ ! -d "/proc/$pid" ]; then
        pid=$(pgrep -f "[x]16-node" | head -n 1)
    fi
    echo "# threads ${pid:-none}"
    if [ -n "$pid" ]; then
        for task in /proc/"$pid"/task/*; do
            # a thread that ended between the listing and the read is skipped
            read -r stat < "$task/stat" 2>/dev/null || continue
            # the name is between the first `(` and the last `)`, and may hold spaces
            comm=${stat#*(}
            comm=${comm%)*}
            # the fields after the name start at the third: utime is the fourteenth, stime the fifteenth
            rest=${stat##*) }
            # shellcheck disable=SC2086
            set -- $rest
            echo "${task##*/} ${12} ${13} $comm"
        done
    fi
    sleep 1
done
