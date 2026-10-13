#!/bin/sh
# F85 on the lab: the workload grid's reference cell on titan, alone, beside an idle object
# executor, and beside the device store's own load on that executor, on two legs
# (`docs/src/features/devices-and-pools.md#performance`). X9's question asked again of the store
# as built: X9's spike code is gone, and `shoal-bench`'s `object-load` drives the node's real
# object executor. Run from the repository root on europa:
#
#   sh shoal-spike/results/object-load-lab.sh setup       # the null device, the dirs
#   QUICK=1 sh shoal-spike/results/object-load-lab.sh      # one round, every side, with where threads ran
#   sh shoal-spike/results/object-load-lab.sh              # the rounds, then the report
#   START=5 ROUNDS=8 sh shoal-spike/results/object-load-lab.sh   # carry on from round five
#   sh shoal-spike/results/object-load-lab.sh report      # the report again from what was fetched
#   sh shoal-spike/results/object-load-lab.sh teardown    # everything setup made, gone
#   sh shoal-spike/results/object-load-lab.sh verify      # the host as it was found
#
# The legs are `null` (the device XFS on a memory-backed null_blk device of its own, so the load
# shares no disk with the tables and every difference is the executor's and the memory's) and
# `evo` (the device a directory on the XFS volume of the 970 EVO the tables are on, the lab as it
# is fitted, labelled as sharing a disk). The evo leg runs at 100 MiB/s alone, as X9's did: a whole
# chunk costs the device twice its bytes (O102), and 500 MiB/s is past what the 970 EVO on one PCIe
# lane takes.
#
# For the rounds titan's governor is `performance` and its e2scrub timer is held; both are put
# back on exit, however it exits. Every change to the host is appended to
# object-load-host-changes.txt.
set -u

HOST=${HOST:-titan}
BIN=${BIN:-target/lab/f85/znver1/release/shoal-workload}
DIR=shoal-spike/results
OUT=${OUT:-$DIR/object-load}
LOGS=${LOGS:-target/lab/f85/logs}
ROUNDS=${ROUNDS:-8}
START=${START:-1}
QUICK=${QUICK:-}
LEGS=${LEGS:-null evo}
REMOTE=/var/tmp/f85
NULL_MOUNT=$REMOTE/null
CHANGES=$DIR/object-load-host-changes.txt
[ -n "$QUICK" ] && ROUNDS=1 && OUT=${QUICK_OUT:-target/lab/f85/quick}

mkdir -p "$OUT" "$LOGS"

# run a script on the host under sh, the script on stdin so no login shell gives its words a
# meaning and no quote has to survive ssh
on() {
    printf '%s\n' "$*" | ssh -o BatchMode=yes "$HOST" sh -s
}

# record a change to the host, with when it was made
changed() {
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $HOST: $*" >> "$CHANGES"
}

# the directory each leg's device is made under, and the rates it runs at
device() {
    case $1 in
        null) echo "$NULL_MOUNT" ;;
        evo) echo /xfs/f85 ;;
    esac
}
rates() {
    case $1 in
        null) echo "100 500" ;;
        evo) echo "100" ;;
    esac
}

# the binary, the configuration and the host script, onto the host
push() {
    on "mkdir -p $REMOTE/out $REMOTE/shoal"
    scp -q "$BIN" "$HOST:$REMOTE/shoal-workload"
    scp -q "$DIR/object-load-conf.yml" "$HOST:$REMOTE/conf.yml"
    scp -q "$DIR/object-load-host.sh" "$HOST:$REMOTE/object-load-host.sh"
}

# the facts every table is labelled with
facts() {
    {
        echo "# F85 object load facts, $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "tree: $(git rev-parse --short HEAD)$(git diff --quiet || echo ' (uncommitted)')"
        echo "rustc: $(rustc --version)"
        echo "binary: $(sha256sum "$BIN" | cut -c1-16) $BIN (znver1, --features object-load)"
        echo "legs: $LEGS; rounds: $START to $ROUNDS${QUICK:+; quick}"
        echo "## $HOST"
        on "uname -r; lscpu | grep -E '^Model name'; cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"
        on "for c in /sys/devices/system/cpu/cpu[0-9]*; do echo \"\${c##*/} core \$(cat \$c/topology/core_id) siblings \$(cat \$c/topology/thread_siblings_list)\"; done"
        on "grep MemTotal /proc/meminfo; echo memlock \$(ulimit -l) KiB"
        on "findmnt -no TARGET,SOURCE,FSTYPE,OPTIONS -T $REMOTE; findmnt -no TARGET,SOURCE,FSTYPE,OPTIONS -T /xfs; findmnt -no TARGET,SOURCE,FSTYPE,OPTIONS -T $NULL_MOUNT"
        on "lsblk -dno NAME,MODEL,SIZE,ROTA | grep -v '^loop'"
        on "for p in /sys/module/null_blk/parameters/*; do [ -e \$p ] && echo null_blk \${p##*/} \$(cat \$p); done; for q in logical_block_size rotational write_cache; do [ -e /sys/block/nullb0/queue/\$q ] && echo nullb0 \$q \$(cat /sys/block/nullb0/queue/\$q); done; true"
        on "systemctl is-active shoal-tmdb || true"
    } > "$OUT/object-load-facts.txt" 2>&1
}

case ${1:-rounds} in
setup)
    # the dirs, the binary, and a memory-backed null_blk device with XFS on it, mounted where
    # the null leg's device is made; refused if a null_blk is loaded already, whose shape is
    # somebody else's
    if [ -n "$(on "lsmod | grep '^null_blk' || true")" ]; then
        echo "$HOST has null_blk loaded already; unload it or teardown first" >&2
        exit 1
    fi
    push
    changed "made $REMOTE (the binary, conf.yml, object-load-host.sh) and /xfs/f85"
    on "mkdir -p /xfs/f85 $NULL_MOUNT"
    on "sudo -n modprobe null_blk nr_devices=1 gb=4 queue_mode=2 bs=4096 memory_backed=1"
    changed "loaded null_blk nr_devices=1 gb=4 queue_mode=2 bs=4096 memory_backed=1 (nullb0)"
    on "sudo -n mkfs.xfs -q -f /dev/nullb0 && sudo -n mount /dev/nullb0 $NULL_MOUNT && sudo -n chown \$(id -un) $NULL_MOUNT"
    changed "made XFS on /dev/nullb0 and mounted it at $NULL_MOUNT, owned by the run's user"
    on "df -hT $NULL_MOUNT /xfs/f85"
    exit 0
    ;;
teardown)
    on "sudo -n umount $NULL_MOUNT 2>/dev/null; true"
    changed "unmounted $NULL_MOUNT"
    on "sudo -n rmmod null_blk"
    changed "unloaded null_blk"
    on "rm -rf $REMOTE /xfs/f85"
    changed "removed $REMOTE and /xfs/f85"
    exit 0
    ;;
verify)
    # the host as it was found: nothing of F85's run left, the governor and the timer as they were
    on "echo governor \$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor); echo e2scrub_all.timer \$(systemctl is-active e2scrub_all.timer); echo shoal-tmdb \$(systemctl is-active shoal-tmdb); lsmod | grep -c '^null_blk' || true; ls -d $REMOTE /xfs/f85 2>&1"
    exit 0
    ;;
report)
    python3 "$DIR/object-load-report.py" "$OUT/raw" > "$DIR/object-load-report.md"
    exit 0
    ;;
rounds) ;;
*)
    echo "usage: object-load-lab.sh [setup | rounds | report | teardown | verify]" >&2
    exit 2
    ;;
esac

# refuse to run beside the tmdb node
if [ "$(on "systemctl is-active shoal-tmdb || true")" = active ]; then
    echo "$HOST runs shoal-tmdb; stop it first" >&2
    exit 1
fi
# the binary may have been rebuilt since setup
push
facts

# save the governor and the e2scrub timer, and put them back on any exit
SAVED=$(on "cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
SCRUB=$(on "systemctl is-active e2scrub_all.timer || true")
restore() {
    on "sudo -n cpupower frequency-set -g $SAVED >/dev/null"
    changed "governor back to $SAVED"
    if [ "$SCRUB" = active ]; then
        on "sudo -n systemctl start e2scrub_all.timer"
        changed "e2scrub_all.timer started again"
    fi
}
trap restore EXIT
trap 'exit 143' INT TERM
on "sudo -n cpupower frequency-set -g performance >/dev/null"
changed "governor $SAVED -> performance for the rounds"
if [ "$SCRUB" = active ]; then
    on "sudo -n systemctl stop e2scrub_all.timer"
    changed "e2scrub_all.timer stopped for the rounds"
fi

# a run of rounds from the first starts with nothing left on the host from an earlier one
if [ "$START" -eq 1 ]; then
    on "rm -rf $REMOTE/out/*"
fi

REST=${REST:-10}
round=$START
while [ "$round" -le "$ROUNDS" ]; do
    # the legs in order in odd rounds and the other way in even ones
    legs=$LEGS
    if [ $((round % 2)) -eq 0 ]; then
        legs=""
        for leg in $LEGS; do
            legs="$leg $legs"
        done
    fi
    for leg in $legs; do
        echo "round $round, $leg: $(date -u +%H:%M:%S)"
        # where every thread ran, read in the first round
        psr=""
        [ "$round" -eq 1 ] && psr=1
        if ! on "cd $REMOTE && RATES='$(rates "$leg")' PSR=$psr sh object-load-host.sh $leg $(device "$leg") $round" \
            > "$LOGS/r$round-$leg.log" 2>&1; then
            echo "round $round's $leg failed; see $LOGS/r$round-$leg.log" >&2
            exit 1
        fi
        cat "$LOGS/r$round-$leg.log"
        sleep "$REST"
    done
    round=$((round + 1))
done

# everything the rounds wrote, back on europa
mkdir -p "$OUT/raw"
scp -q "$HOST:$REMOTE/out/*" "$OUT/raw/"
if [ -z "$QUICK" ]; then
    python3 "$DIR/object-load-report.py" "$OUT/raw" > "$DIR/object-load-report.md"
fi
echo "done: $(date -u +%H:%M:%S)"
