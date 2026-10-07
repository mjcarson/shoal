#!/bin/sh
# Spike X3 on the lab: four legs at four row sizes, each size on a cluster brought up for it.
#
#   sh shoal-spike-bytes/results/x3-lab.sh setup        # once: the archive volumes on titan and hyperion
#   sh shoal-spike-bytes/results/x3-lab.sh              # the rounds, from the repository root on europa
#   START=3 sh shoal-spike-bytes/results/x3-lab.sh      # carry on from round three
#   QUICK=1 ROUNDS=1 OUT=target/lab/x3/quick sh shoal-spike-bytes/results/x3-lab.sh   # prove it runs
#   LEGS=loopback SIZES=1048576 sh shoal-spike-bytes/results/x3-lab.sh                # one cell
#   DEPTH_SCALE=2 FIO=0 LEGS=loopback SIZES=1048576 OUT=target/lab/x3/depth sh ...      # the depth check
#   sh shoal-spike-bytes/results/x3-lab.sh teardown     # after: every cluster down, the volumes gone
#
# The legs are lab (europa, titan, hyperion at a factor of three), loopback (three nodes on europa,
# `x3 local`), titan and europa (one node each). Every round starts with fio on every device the
# lab's roots are on, then runs the legs in order in odd rounds and the other way in even ones, and
# each leg's sizes the same. Every size is: take down whatever x3 left, bring the leg's cluster up,
# run the leg at that size (`x3 spike`, which settles every merge before it reads the counters),
# take the cluster down, trim the devices, rest.
#
# For the run every host's governor is `performance` and titan's and hyperion's e2scrub timer is
# held; both are put back on exit, however it exits. No shoal unit but x3's may be running.
set -u

X3=${X3:-target/lab/x3/znver1/release/x3}
NODE=${NODE:-target/lab/x3/znver1/release/x3-node}
DIR=shoal-spike-bytes
OUT=${OUT:-$DIR/results}
LOGS=${LOGS:-target/lab/x3/logs}
ROUNDS=${ROUNDS:-4}
START=${START:-1}
QUICK=${QUICK:-}
LEGS=${LEGS:-lab loopback titan europa}
SIZES=${SIZES:-65536 262144 1048576 4194304}
DEPTH_SCALE=${DEPTH_SCALE:-1}
FIO=${FIO:-1}
REMOTES="titan hyperion"
# the archive volume titan and hyperion are given for the run, beside /xfs on the 970 EVO
LV=ubuntu-vg/x3-archives
MOUNT=/x3-archives
CHANGES=$DIR/results/x3-host-changes.txt

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

# say a change made to a host, in the file that lists every one
changed() {
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $1: $2" >> "$CHANGES"
}

# the inventory each leg's cluster is described by
inventory() {
    case $1 in
        lab) echo "$DIR/inventory.yml" ;;
        loopback) echo "$DIR/inventory-loopback.yml" ;;
        titan) echo "$DIR/inventory-titan.yml" ;;
        europa) echo "$DIR/inventory-europa.yml" ;;
    esac
}

# the cpus the driver is pinned to, clear of every node on europa the leg runs
pin() {
    case $1 in
        lab | titan) echo "8-15,24-31" ;;
        loopback | europa) echo "13-15,29-31" ;;
    esac
}

# take a leg's cluster down, whatever state it was left in
down() {
    inv=$(inventory "$1")
    if [ "$1" = loopback ]; then
        "$X3" local down -i "$inv" || true
    else
        "$X3" destroy -i "$inv" --yes || true
    fi
}

# bring a leg's cluster up
up() {
    inv=$(inventory "$1")
    if [ "$1" = loopback ]; then
        "$X3" local up -i "$inv" --program "$NODE"
    else
        "$X3" bootstrap -i "$inv"
    fi
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
setup)
    # an XFS volume on titan's and hyperion's 970 EVO for the archives, a device of its own to the
    # kernel; mounted for the run and never put in fstab
    for host in $REMOTES; do
        if on "$host" "sudo -n lvs $LV >/dev/null 2>&1"; then
            echo "$host already has $LV"
        else
            on "$host" "set -e; sudo -n lvcreate -q -y -L 80G -n x3-archives ubuntu-vg; sudo -n mkfs.xfs -q -L x3arch /dev/$LV"
            changed "$host" "lvcreate -L 80G -n x3-archives ubuntu-vg; mkfs.xfs -L x3arch /dev/$LV"
        fi
        if ! on "$host" "findmnt -n $MOUNT >/dev/null"; then
            on "$host" "set -e; sudo -n mkdir -p $MOUNT; sudo -n mount /dev/$LV $MOUNT"
            changed "$host" "mkdir $MOUNT; mount /dev/$LV $MOUNT (not in fstab)"
        fi
        on "$host" "findmnt -no TARGET,SOURCE,FSTYPE $MOUNT; df -h $MOUNT /xfs | tail -n +2"
    done
    exit 0
    ;;
teardown)
    # every x3 cluster down, then each volume unmounted and removed
    for leg in lab loopback titan europa; do
        down "$leg"
    done
    for host in $REMOTES; do
        if on "$host" "findmnt -n $MOUNT >/dev/null"; then
            on "$host" "set -e; sudo -n umount $MOUNT; sudo -n rmdir $MOUNT"
            changed "$host" "umount $MOUNT; rmdir $MOUNT"
        fi
        if on "$host" "sudo -n lvs $LV >/dev/null 2>&1"; then
            on "$host" "sudo -n lvremove -q -y $LV"
            changed "$host" "lvremove $LV"
        fi
        on "$host" "sudo -n vgs ubuntu-vg; systemctl list-units --all --no-legend 'shoal*' || true"
    done
    systemctl list-units --all --no-legend 'shoal*' || true
    exit 0
    ;;
rounds) ;;
*)
    echo "usage: x3-lab.sh [setup | rounds | teardown]" >&2
    exit 2
    ;;
esac

# refuse to start beside another shoal unit, or without the archive volumes
for host in europa $REMOTES; do
    active=$(on "$host" "systemctl list-units --state=active --no-legend 'shoal*' | grep -v shoal-x3 || true")
    if [ -n "$active" ]; then
        echo "$host runs another shoal unit: $active" >&2
        exit 1
    fi
done
for host in $REMOTES; do
    if ! on "$host" "findmnt -n $MOUNT >/dev/null"; then
        echo "$host has no $MOUNT; run \`x3-lab.sh setup\` first" >&2
        exit 1
    fi
done

# the facts every table is labelled with
FACTS="$OUT/x3-facts.txt"
{
    echo "# X3 facts, $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "tree: $(git rev-parse --short HEAD)$(git diff --quiet || echo ' (with the uncommitted spike)')"
    echo "rustc: $(rustc --version)"
    echo "legs: $LEGS; sizes: $SIZES; rounds: $START to $ROUNDS; depth scale: $DEPTH_SCALE${QUICK:+; quick}"
    for host in europa $REMOTES; do
        echo "## $host"
        on "$host" "uname -r; lscpu | grep -E \"^Model name\"; cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"
        on "$host" "for d in /optane /xfs $MOUNT; do [ -d \$d ] && findmnt -no TARGET,SOURCE,FSTYPE,OPTIONS -T \$d; done; true"
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

REST=30
[ -n "$QUICK" ] && REST=5

round=$START
while [ "$round" -le "$ROUNDS" ]; do
    # every device's own rate, before any cluster is up
    if [ "$FIO" = 1 ]; then
        echo "round $round, fio: $(date -u +%H:%M:%S)"
        if ! "$X3" fio -i "$DIR/inventory.yml" --round "$round" --out "$OUT/x3.json" ${QUICK:+--quick} \
            > "$OUT/x3-r$round-fio.log" 2>&1; then
            echo "round $round's fio failed; see $OUT/x3-r$round-fio.log" >&2
            exit 1
        fi
    fi
    legs=$LEGS
    sizes=$SIZES
    if [ $((round % 2)) -eq 0 ]; then
        legs=$(reverse "$LEGS")
        sizes=$(reverse "$SIZES")
    fi
    for leg in $legs; do
        inv=$(inventory "$leg")
        for size in $sizes; do
            echo "round $round, $leg at $size: $(date -u +%H:%M:%S)"
            log="$LOGS/r$round-$leg-$size"
            down "$leg" > "$log-deploy.log" 2>&1
            if ! up "$leg" >> "$log-deploy.log" 2>&1; then
                echo "bringing $leg up failed; see $log-deploy.log" >&2
                exit 1
            fi
            if ! taskset -c "$(pin "$leg")" "$X3" spike "$leg" --size "$size" -i "$inv" --round "$round" \
                --out "$OUT/x3.json" --depth-scale "$DEPTH_SCALE" ${QUICK:+--quick} \
                > "$OUT/x3-r$round-$leg-$size.log" 2>&1; then
                echo "round $round's $leg at $size failed; see $OUT/x3-r$round-$leg-$size.log" >&2
                down "$leg" >> "$log-deploy.log" 2>&1
                exit 1
            fi
            down "$leg" >> "$log-deploy.log" 2>&1
            # the devices trimmed of what the cluster wrote, so the next starts as this one did
            on europa "sudo -n fstrim /optane || true"
            for host in $REMOTES; do
                on "$host" "sudo -n fstrim /xfs || true; sudo -n fstrim $MOUNT || true"
            done
            sleep "$REST"
        done
    done
    round=$((round + 1))
done

"$X3" report "$OUT/x3.json" --label "europa, titan, hyperion; performance governor; see x3-facts.txt" \
    > "$OUT/x3-report.md"
