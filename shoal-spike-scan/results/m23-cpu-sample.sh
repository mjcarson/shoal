#!/bin/sh
# Item 353 on the lab: every second, each of a host's cpus' time and the interrupts each took, and
# every thread of its side node with the cpu it ran on and the page faults it took, so a shard's
# long scan steps can be read beside what its cpu was doing (m23-i353-report.py, item 353). It reads
# /proc alone, writes to stdout only, and runs until its deadline, or until its output closes. Run
# it pinned clear of the node's shards, as m23-lab.sh does:
#
#   ssh titan taskset -c 7 sh -s -- <deadline, unix seconds> < shoal-spike-scan/results/m23-cpu-sample.sh > titan.log &
#
# Each sample is a line `@ <unix seconds>`, then sections each opened by a line `# <name>`:
#
# - `# stat`: /proc/stat's line of every cpu, `cpuN user nice system idle iowait irq softirq ...`, in
#   USER_HZ ticks (a tick-sampled split on a kernel without irq time accounting, as the lab's are)
# - `# softirqs`: /proc/softirqs whole, its header naming the cpus
# - `# interrupts`: /proc/interrupts' header naming the cpus, a row `NIC:` summing every row of a NIC
#   (`enp`, `eno`, `eth`) and a row `NVME:` every NVMe queue's, a count a cpu each, and the LOC, RES,
#   CAL and TLB rows (the local timer, rescheduling, function call and TLB shootdown interrupts)
# - `# vmstat`: the host's page faults, major faults, compaction stalls and huge page faults
# - `# threads <pid|none>`: one line a thread of the node, `<tid> <cpu> <minflt> <majflt> <utime>
#   <stime> <comm>`, the cpu it last ran on and its times in clock ticks
set -u
deadline=$1
# the node's process, found again whenever the one sampled is gone, as it is after every restart;
# the bracket keeps the pattern from matching this script's own command line
pid=""
while [ "$(date +%s)" -lt "$deadline" ]; do
    # the time first, as nanoseconds since the epoch, which GNU's and uutils' `date` both print
    echo "@ $(date +%s.%N)" || exit 0
    # every cpu's time, each apart
    echo "# stat"
    grep -E "^cpu[0-9]+ " /proc/stat
    # every softirq by cpu
    echo "# softirqs"
    cat /proc/softirqs
    # the NICs' and the NVMe queues' interrupts by cpu, each kind's rows summed into one, and the
    # IPIs' rows as they are
    echo "# interrupts"
    awk 'NR == 1 { cpus = NF; print; next }
        $1 ~ /^(LOC|RES|CAL|TLB):$/ { line = $1; for (i = 2; i <= cpus + 1; i++) line = line " " $i; print line; next }
        $NF ~ /^(enp|eno|eth)/ { for (i = 2; i <= cpus + 1; i++) nic[i] += $i; nics++ }
        $NF ~ /^nvme/ { for (i = 2; i <= cpus + 1; i++) nvme[i] += $i; nvmes++ }
        END {
            if (nics) { line = "NIC:"; for (i = 2; i <= cpus + 1; i++) line = line " " nic[i] + 0; print line }
            if (nvmes) { line = "NVME:"; for (i = 2; i <= cpus + 1; i++) line = line " " nvme[i] + 0; print line }
        }' /proc/interrupts
    # the host's faults and the kernel's work for huge pages
    echo "# vmstat"
    grep -E "^(pgfault|pgmajfault|compact_stall|thp_fault_alloc) " /proc/vmstat
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
            # the fields after the name start at the third: minflt is the tenth, majflt the twelfth,
            # utime the fourteenth, stime the fifteenth and the cpu it last ran on the thirty-ninth
            rest=${stat##*) }
            # shellcheck disable=SC2086
            set -- $rest
            echo "${task##*/} ${37} ${8} ${10} ${12} ${13} $comm"
        done
    fi
    sleep 1
done
