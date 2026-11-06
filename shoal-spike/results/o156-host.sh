#!/bin/sh
# O156 on a Zen1 lab host (2026-11-05): copied there as host.sh into DIR beside the znver1
# shoal-workload and o156-conf.yml as conf.yml, it runs the twelve sort key arms ROUNDS times, the
# padded form first in odd rounds and the integer first in even ones, each on a wiped /opt/shoal, the
# process's shards on cpus 2 and 4 (exclude_cores [0, 3]) and its client on 6 and 7, locked memory
# unlimited, the tmdb node stopped and the governor at performance, both put back on every way out.
# o156-report.py reads the runs it writes.
set -u
DIR=${DIR:-/var/tmp/o156}
ROUNDS=${ROUNDS:-4}
SCALE=${SCALE:-full}
mkdir -p "$DIR/runs"
was=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)
restore() {
    sudo cpupower frequency-set -g "$was" > /dev/null 2>&1
    sudo systemctl start shoal-tmdb
    echo "restored: governor $was, shoal-tmdb $(systemctl is-active shoal-tmdb)"
}
trap restore EXIT INT TERM
sudo systemctl stop shoal-tmdb
sudo cpupower frequency-set -g performance > /dev/null
sudo prlimit --pid $$ --memlock=unlimited:unlimited
round=1
while [ "$round" -le "$ROUNDS" ]; do
    if [ $((round % 2)) -eq 1 ]; then forms="padded u64"; else forms="u64 padded"; fi
    for res in resident evicted; do
        for acc in get range walk; do
            for form in $forms; do
                id=macro/sort_key/$form/$acc/$res
                sudo rm -rf /opt/shoal/*
                echo "== $(date -u +%FT%TZ) round $round $id"
                taskset -c 6,7 "$DIR/shoal-workload" run --id "$id" --conf "$DIR/conf.yml" --scale "$SCALE" \
                    --label "o156-r$round" --json "$DIR/runs/r$round-$form-$acc-$res.json" > "$DIR/runs/r$round-$form-$acc-$res.log" 2>&1 \
                    || echo "   failed: $(tail -2 "$DIR/runs/r$round-$form-$acc-$res.log")"
            done
        done
    done
    round=$((round + 1))
done
sudo rm -rf /opt/shoal/*
echo "== $(date -u +%FT%TZ) done"
