#!/bin/sh
# Spike X3's supplement, after the rounds: what a put arm spends its cpu on, at 1 MiB, on the
# loopback leg's three nodes and on europa's one node, and at 4 MiB on europa's one node, where
# the rounds put fewer bytes a second through than at 1 MiB. The rounds' figures could not say
# why either.
#
#   sh shoal-spike-bytes/results/x3-supplement.sh     # from the repository root on europa
#
# Each leg is brought up, run whole at 1 MiB as a round runs it (`x3 spike`), and taken down. When
# the put arm's measured window starts, every node's threads are sampled by pidstat for twenty
# seconds and one node is profiled by perf for ten, both on europa, where every node of the two
# legs runs. The governor is `performance` for the run and put back on exit.
set -u

X3=${X3:-target/lab/x3/znver1/release/x3}
NODE=${NODE:-target/lab/x3/znver1/release/x3-node}
DIR=shoal-spike-bytes
OUT=${OUT:-target/lab/x3/supplement}
mkdir -p "$OUT"

saved=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)
trap 'sudo -n cpupower frequency-set -g "$saved" >/dev/null; echo "governor restored: $saved"' EXIT
trap 'exit 143' INT TERM
sudo -n cpupower frequency-set -g performance >/dev/null

# the main process of every unit a leg's nodes run in
pids() {
    for unit in "$@"; do
        systemctl show -p MainPID --value "$unit"
    done
}

# sample a leg's put arm: wait for it to start, past its warm-up, then pidstat and perf
sample() {
    leg=$1
    size=$2
    log=$3
    shift 3
    until grep -q "$leg $size: put$" "$log"; do sleep 1; done
    sleep 12
    list=$(pids "$@" | tr '\n' ',' | sed 's/,$//')
    first=$(pids "$1")
    pidstat -t -u -p "$list" 20 1 > "$OUT/$leg-$size-pidstat.txt" 2>&1 &
    sudo -n perf record -F 499 -g -p "$first" -o "$OUT/$leg-$size-perf.data" -- sleep 10 > /dev/null 2>&1
    wait
    sudo -n perf report -i "$OUT/$leg-$size-perf.data" --no-children --sort symbol --stdio --percent-limit 0.7 \
        2>/dev/null | grep -v '^$' | grep -v '^#' | grep '%' | head -60 > "$OUT/$leg-$size-perf.txt"
}

# the loopback leg's three nodes
"$X3" local down -i "$DIR/inventory-loopback.yml" > /dev/null 2>&1 || true
"$X3" local up -i "$DIR/inventory-loopback.yml" --program "$NODE" > "$OUT/loopback-up.log" 2>&1
taskset -c 13-15,29-31 "$X3" spike loopback --size 1048576 -i "$DIR/inventory-loopback.yml" --round 5 \
    --out "$OUT/x3.json" > "$OUT/loopback-1048576.log" 2>&1 &
spike=$!
sample loopback 1048576 "$OUT/loopback-1048576.log" shoal-x3-local-loop-a shoal-x3-local-loop-b shoal-x3-local-loop-c
wait $spike
"$X3" local down -i "$DIR/inventory-loopback.yml" > /dev/null 2>&1

# europa's one node, at each size
for size in 1048576 4194304; do
    "$X3" destroy -i "$DIR/inventory-europa.yml" --yes > /dev/null 2>&1 || true
    "$X3" bootstrap -i "$DIR/inventory-europa.yml" > "$OUT/europa-up.log" 2>&1
    taskset -c 13-15,29-31 "$X3" spike europa --size $size -i "$DIR/inventory-europa.yml" --round 5 \
        --out "$OUT/x3.json" > "$OUT/europa-$size.log" 2>&1 &
    spike=$!
    sample europa $size "$OUT/europa-$size.log" shoal-x3-europa
    wait $spike
    "$X3" destroy -i "$DIR/inventory-europa.yml" --yes > /dev/null 2>&1
    sudo -n fstrim /optane || true
done
