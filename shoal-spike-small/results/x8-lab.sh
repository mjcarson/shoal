#!/bin/sh
# Spike X8 on the lab: two legs, every size, depth and path, each leg on a cluster brought up for
# it with a holder beside every node.
#
#   sh shoal-spike-small/results/x8-lab.sh              # the rounds, from the repository root on europa
#   START=3 sh shoal-spike-small/results/x8-lab.sh      # carry on from round three
#   QUICK=1 ROUNDS=1 OUT=target/lab/x8/quick sh shoal-spike-small/results/x8-lab.sh   # prove it runs
#   LEGS=loopback SIZES=4096 DEPTHS=32 sh shoal-spike-small/results/x8-lab.sh         # one cell's sizes
#   BOUND_SCALE=2 LEGS=loopback SIZES=4096 DEPTHS=32 OUT=target/lab/x8/bound sh ...   # the bound check
#   IN_PLACE_SYNC=batch LEGS=lab DEPTHS=32 OUT=shoal-spike-small/results/supplement sh ...  # the supplement
#   sh shoal-spike-small/results/x8-lab.sh teardown     # after: every cluster and holder down
#
# The legs are lab (europa, titan, hyperion at a factor of three over 1 GbE, keys in the groups
# europa leads) and loopback (three nodes on europa, `x8 local`, keys in every group). Every round
# runs the legs in order in odd rounds and the other way in even ones; `x8 spike` reverses its sizes,
# depths and paths the same way. Every leg is: take down whatever x8 left, bring the leg's cluster
# and its holders up, run every cell (`x8 spike`, which seals and settles every merge around each
# cell), take both down, trim the devices, rest.
#
# For the run every host's governor is `performance` and titan's and hyperion's e2scrub timer is
# held; both are put back on exit, however it exits. No shoal unit but x8's may be running.
set -u

X8=${X8:-target/lab/x8/znver1/release/x8}
NODE=${NODE:-target/lab/x8/znver1/release/x8-node}
HOLDER=${HOLDER:-target/lab/x8/znver1/release/x8-holder}
DIR=shoal-spike-small
OUT=${OUT:-$DIR/results}
LOGS=${LOGS:-target/lab/x8/logs}
ROUNDS=${ROUNDS:-4}
START=${START:-1}
QUICK=${QUICK:-}
LEGS=${LEGS:-lab loopback}
SIZES=${SIZES:-}
DEPTHS=${DEPTHS:-}
PATHS=${PATHS:-}
BOUND_SCALE=${BOUND_SCALE:-1}
HEAT=${HEAT:-60}
# how a holder makes an apply or a fold durable: each syncs its chunk, as S6 has it; batch covers
# every one that completed with one flush, which is the supplement
IN_PLACE_SYNC=${IN_PLACE_SYNC:-each}
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

# the inventory each leg's cluster is described by
inventory() {
    case $1 in
        lab) echo "$DIR/inventory.yml" ;;
        loopback) echo "$DIR/inventory-loopback.yml" ;;
    esac
}

# the cpus the driver is pinned to, clear of every node and holder on europa the leg runs
pin() {
    case $1 in
        lab) echo "8-15,24-31" ;;
        loopback) echo "13-15,29-31" ;;
    esac
}

# the member every key is aimed at: on the lab the one the driver shares a host with
lead() {
    case $1 in
        lab) echo "--lead europa" ;;
        loopback) echo "" ;;
    esac
}

# take a leg's holders and cluster down, whatever state they were left in
down() {
    inv=$(inventory "$1")
    "$X8" holders down -i "$inv" || true
    if [ "$1" = loopback ]; then
        "$X8" local down -i "$inv" || true
    else
        "$X8" destroy -i "$inv" --yes || true
    fi
}

# bring a leg's cluster and its holders up
up() {
    inv=$(inventory "$1")
    if [ "$1" = loopback ]; then
        "$X8" local up -i "$inv" --program "$NODE" || return 1
    else
        "$X8" bootstrap -i "$inv" || return 1
    fi
    "$X8" holders up -i "$inv" --program "$HOLDER" --in-place-sync "$IN_PLACE_SYNC" && "$X8" holders check -i "$inv"
}

# the words of a list in the other order
reverse() {
    out=""
    for word in $1; do
        out="$word $out"
    done
    echo "$out"
}

case ${1:-rounds} in
teardown)
    for leg in lab loopback; do
        down "$leg"
    done
    for host in europa $REMOTES; do
        on "$host" "systemctl list-units --all --no-legend 'shoal*' || true"
    done
    exit 0
    ;;
rounds) ;;
*)
    echo "usage: x8-lab.sh [rounds | teardown]" >&2
    exit 2
    ;;
esac

# refuse to start beside another shoal unit
for host in europa $REMOTES; do
    active=$(on "$host" "systemctl list-units --state=active --no-legend 'shoal*' | grep -v shoal-x8 || true")
    if [ -n "$active" ]; then
        echo "$host runs another shoal unit: $active" >&2
        exit 1
    fi
done

# the facts every table is labelled with
FACTS="$OUT/x8-facts.txt"
{
    echo "# X8 facts, $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "tree: $(git rev-parse --short HEAD)$(git diff --quiet || echo ' (with the uncommitted spike)')"
    echo "rustc: $(rustc --version)"
    echo "legs: $LEGS; sizes: ${SIZES:-all}; depths: ${DEPTHS:-all}; paths: ${PATHS:-all}; rounds: $START to $ROUNDS; bound scale: $BOUND_SCALE; in-place syncs: $IN_PLACE_SYNC${QUICK:+; quick}"
    for host in europa $REMOTES; do
        echo "## $host"
        on "$host" "uname -r; lscpu | grep -E \"^Model name\"; cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"
        on "$host" "for d in /optane /xfs; do [ -d \$d ] && findmnt -no TARGET,SOURCE,FSTYPE,OPTIONS -T \$d; done; true"
        on "$host" "for i in /sys/class/net/*; do [ -e \$i/device ] && echo \${i##*/} \$(cat \$i/speed 2>/dev/null); done"
        on "$host" "lsblk -dno NAME,MODEL,SIZE | grep -i nvme; grep MemTotal /proc/meminfo"
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

REST=120
[ -n "$QUICK" ] && REST=5
[ -n "$QUICK" ] && HEAT=5

round=$START
while [ "$round" -le "$ROUNDS" ]; do
    legs=$LEGS
    if [ $((round % 2)) -eq 0 ]; then
        legs=$(reverse "$LEGS")
    fi
    for leg in $legs; do
        inv=$(inventory "$leg")
        echo "round $round, $leg: $(date -u +%H:%M:%S)"
        log="$LOGS/r$round-$leg"
        down "$leg" > "$log-deploy.log" 2>&1
        if ! up "$leg" >> "$log-deploy.log" 2>&1; then
            echo "bringing $leg up failed; see $log-deploy.log" >&2
            exit 1
        fi
        # shellcheck disable=SC2046
        if ! taskset -c "$(pin "$leg")" "$X8" spike "$leg" -i "$inv" --round "$round" --out "$OUT/x8.json" \
            $(lead "$leg") --bound-scale "$BOUND_SCALE" --heat-secs "$HEAT" \
            ${SIZES:+--sizes "$SIZES"} ${DEPTHS:+--depths "$DEPTHS"} ${PATHS:+--paths "$PATHS"} ${QUICK:+--quick} \
            > "$OUT/x8-r$round-$leg.log" 2>&1; then
            echo "round $round's $leg failed; see $OUT/x8-r$round-$leg.log" >&2
            down "$leg" >> "$log-deploy.log" 2>&1
            exit 1
        fi
        down "$leg" >> "$log-deploy.log" 2>&1
        # the devices trimmed of what the cluster and the holders wrote, so the next starts alike
        on europa "sudo -n fstrim /optane || true"
        for host in $REMOTES; do
            on "$host" "sudo -n fstrim /xfs || true"
        done
        sleep "$REST"
    done
    round=$((round + 1))
done

"$X8" report "$OUT/x8.json" --label "europa, titan, hyperion; performance governor; see x8-facts.txt" \
    > "$OUT/x8-report.md"
