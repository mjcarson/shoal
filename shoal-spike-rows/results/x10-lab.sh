#!/bin/sh
# Spike X10 on the lab: four rounds of the three legs, each leg on a cluster bootstrapped for it.
#
#   sh shoal-spike-rows/results/x10-lab.sh            # from the repository root, on europa
#   START=3 sh shoal-spike-rows/results/x10-lab.sh    # carry on from round three
#   QUICK=1 ROUNDS=1 OUT=target/lab/x10/quick sh shoal-spike-rows/results/x10-lab.sh   # prove it runs
#   LEGS=remedy sh shoal-spike-rows/results/x10-lab.sh  # the supplement, after the rounds
#
# The legs run rate, rows, size in odd rounds and size, rows, rate in even ones; each leg's cells
# reverse with the round too. Every leg is: destroy whatever x10 left, bootstrap the inventory,
# run the leg (which activates wire 7 and waits for its leaders to settle), destroy. The driver is
# pinned to europa's cores 8 to 15 and their siblings, clear of the node's shards on 0 to 6.
#
# For the run every host's governor is `performance` and titan's and hyperion's e2scrub timer is
# held; both are put back on exit, however it exits. No shoal unit but x10's may be running.
set -u

X10=${X10:-target/lab/x10/znver1/release/x10}
INV=${INV:-shoal-spike-rows/inventory.yml}
OUT=${OUT:-shoal-spike-rows/results}
LOGS=${LOGS:-target/lab/x10/logs}
ROUNDS=${ROUNDS:-4}
START=${START:-1}
PIN=${PIN:-8-15,24-31}
QUICK=${QUICK:-}
LEGS=${LEGS:-}
REMOTES="titan hyperion"

mkdir -p "$OUT" "$LOGS"

# run a script on a host under sh: europa locally, the others over ssh, the script on stdin so no
# login shell (europa's is zsh) gives its words a meaning and no quote has to survive ssh
on() {
    host=$1
    shift
    if [ "$host" = europa ]; then
        printf '%s\n' "$*" | sh -s
    else
        printf '%s\n' "$*" | ssh -o BatchMode=yes "$host" sh -s
    fi
}

# refuse to start beside another shoal unit
for host in europa $REMOTES; do
    active=$(on "$host" "systemctl list-units --state=active --no-legend 'shoal*' | grep -v shoal-x10 || true")
    if [ -n "$active" ]; then
        echo "$host runs another shoal unit: $active" >&2
        exit 1
    fi
done

# the facts every table is labelled with
FACTS="$OUT/x10-facts${LEGS:+-$(echo "$LEGS" | tr ' ' '-')}.txt"
{
    echo "# X10 facts, $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "tree: $(git rev-parse --short HEAD)$(git diff --quiet || echo ' (with the uncommitted spike)')"
    echo "rustc: $(rustc --version)"
    for host in europa $REMOTES; do
        echo "## $host"
        on "$host" "uname -r; lscpu | grep -E \"^Model name\"; cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"
        on "$host" "for d in /optane /xfs; do [ -d \$d ] && findmnt -no TARGET,SOURCE,FSTYPE,OPTIONS -T \$d; done; true"
        on "$host" "for i in /sys/class/net/*; do [ -e \$i/device ] && echo \${i##*/} \$(cat \$i/speed 2>/dev/null); done"
        on "$host" "lsblk -dno NAME,MODEL,SIZE | grep -i nvme"
    done
} > "$FACTS" 2>&1

# save every host's governor and the e2scrub timers, and put them back on any exit
SAVED=""
for host in europa $REMOTES; do
    SAVED="$SAVED $host=$(on "$host" "cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")"
done
SCRUB=""
for host in $REMOTES; do
    if [ "$(on "$host" "systemctl is-active e2scrub_all.timer || true")" = active ]; then
        SCRUB="$SCRUB $host"
    fi
done
restore() {
    for pair in $SAVED; do
        host=${pair%%=*}
        on "$host" "sudo -n cpupower frequency-set -g ${pair#*=} >/dev/null"
    done
    for host in $SCRUB; do
        on "$host" "sudo -n systemctl start e2scrub_all.timer"
    done
    echo "governors restored:$SAVED; e2scrub timers started again on:$SCRUB"
}
trap restore EXIT
trap 'exit 143' INT TERM
for host in europa $REMOTES; do
    on "$host" "sudo -n cpupower frequency-set -g performance >/dev/null"
done
for host in $SCRUB; do
    on "$host" "sudo -n systemctl stop e2scrub_all.timer"
done

round=$START
while [ "$round" -le "$ROUNDS" ]; do
    if [ -n "$LEGS" ]; then
        legs=$LEGS
    elif [ $((round % 2)) -eq 1 ]; then
        legs="rate rows size"
    else
        legs="size rows rate"
    fi
    for leg in $legs; do
        echo "round $round, $leg: $(date -u +%H:%M:%S)"
        "$X10" destroy -i "$INV" --yes > "$LOGS/r$round-$leg-deploy.log" 2>&1 || true
        if ! "$X10" bootstrap -i "$INV" >> "$LOGS/r$round-$leg-deploy.log" 2>&1; then
            echo "the bootstrap failed; see $LOGS/r$round-$leg-deploy.log" >&2
            exit 1
        fi
        if ! taskset -c "$PIN" "$X10" spike "$leg" -i "$INV" --round "$round" --out "$OUT/x10.json" \
            ${QUICK:+--quick} > "$OUT/x10-r$round-$leg.log" 2>&1; then
            echo "round $round's $leg failed; see $OUT/x10-r$round-$leg.log" >&2
            exit 1
        fi
        "$X10" destroy -i "$INV" --yes >> "$LOGS/r$round-$leg-deploy.log" 2>&1
        sleep 30
    done
    round=$((round + 1))
done

"$X10" report "$OUT/x10.json" --label "europa, titan, hyperion; tmdb_cluster.yaml's shape; performance governor" \
    > "$OUT/x10-report.md"
echo "done: $OUT/x10-report.md"
