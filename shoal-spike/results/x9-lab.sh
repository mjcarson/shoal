#!/bin/sh
# Spike X9 on the lab: the workload grid's reference cell on titan, alone and beside object work,
# on two legs (`docs/src/object-storage/table-latency.md`). Run from the repository root on europa:
#
#   sh shoal-spike/results/x9-lab.sh setup        # the pool device, the dirs, the rings written once
#   QUICK=1 sh shoal-spike/results/x9-lab.sh       # one round, every side, with where threads ran
#   sh shoal-spike/results/x9-lab.sh               # the rounds, then the report
#   START=5 ROUNDS=8 sh shoal-spike/results/x9-lab.sh   # carry on from round five
#   sh shoal-spike/results/x9-lab.sh report       # the report again from what was fetched
#   sh shoal-spike/results/x9-lab.sh teardown     # everything setup made, gone
#   sh shoal-spike/results/x9-lab.sh verify       # the host as it was found
#
# The legs are `null` (the pool a null_blk device of its own, which copies nothing and never
# queues, so every difference is the executor's) and `evo` (the pool an XFS volume on the 970 EVO
# the tables are on, the lab as it is fitted). The evo leg runs at 100 MiB/s alone: 500 MiB/s of
# 4+2 work is about 786 MB/s of writes, past what the 970 EVO on one PCIe lane takes (X6).
#
# For the rounds titan's governor is `performance` and its e2scrub timer is held; both are put
# back on exit, however it exits. Every change to the host is appended to x9-host-changes.txt.
set -u

HOST=${HOST:-titan}
BIN=${BIN:-target/lab/x9/znver1/release/shoal-workload}
DIR=shoal-spike/results
OUT=${OUT:-$DIR/x9}
LOGS=${LOGS:-target/lab/x9/logs}
ROUNDS=${ROUNDS:-8}
START=${START:-1}
QUICK=${QUICK:-}
LEGS=${LEGS:-null evo}
REMOTE=/var/tmp/x9
CHANGES=$DIR/x9-host-changes.txt
[ -n "$QUICK" ] && ROUNDS=1 && OUT=${QUICK_OUT:-target/lab/x9/quick}

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

# the pool each leg writes to, and the rates it runs at
pool() {
    case $1 in
        null) echo /xfs/x9/null/nullb0 ;;
        evo) echo /xfs/x9/pool ;;
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
    scp -q "$DIR/x9-conf.yml" "$HOST:$REMOTE/conf.yml"
    scp -q "$DIR/x9-host.sh" "$HOST:$REMOTE/x9-host.sh"
}

# the facts every table is labelled with
facts() {
    {
        echo "# X9 facts, $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "tree: $(git rev-parse --short HEAD)$(git diff --quiet || echo ' (with the uncommitted spike)')"
        echo "rustc: $(rustc --version)"
        echo "binary: $(sha256sum "$BIN" | cut -c1-16) $BIN (znver1, --features x9)"
        echo "legs: $LEGS; rounds: $START to $ROUNDS${QUICK:+; quick}"
        echo "## $HOST"
        on "uname -r; lscpu | grep -E '^Model name'; cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"
        on "for c in /sys/devices/system/cpu/cpu[0-9]*; do echo \"\${c##*/} core \$(cat \$c/topology/core_id) siblings \$(cat \$c/topology/thread_siblings_list)\"; done"
        on "grep MemTotal /proc/meminfo; echo memlock \$(ulimit -l) KiB"
        on "findmnt -no TARGET,SOURCE,FSTYPE,OPTIONS -T $REMOTE; findmnt -no TARGET,SOURCE,FSTYPE,OPTIONS -T /xfs"
        on "lsblk -dno NAME,MODEL,SIZE,ROTA | grep -v '^loop'"
        on "for p in /sys/module/null_blk/parameters/*; do [ -e \$p ] && echo null_blk \${p##*/} \$(cat \$p); done; for q in logical_block_size rotational nr_requests; do [ -e /sys/block/nullb0/queue/\$q ] && echo nullb0 \$q \$(cat /sys/block/nullb0/queue/\$q); done; true"
        on "systemctl is-active shoal-tmdb || true"
    } > "$OUT/x9-facts.txt" 2>&1
}

case ${1:-rounds} in
setup)
    # the dirs, the binary, and a null_blk device whose node sits on XFS: glommio drops O_DIRECT
    # for anything on devtmpfs, which is where /dev/nullb0 is
    push
    changed "made $REMOTE (the binary, conf.yml, x9-host.sh) and /xfs/x9/{pool,null}"
    on "mkdir -p /xfs/x9/pool /xfs/x9/null"
    on "sudo -n modprobe null_blk nr_devices=1 gb=4 queue_mode=2 bs=4096 memory_backed=0"
    changed "loaded null_blk nr_devices=1 gb=4 queue_mode=2 bs=4096 memory_backed=0 (nullb0)"
    on "dev=\$(cat /sys/block/nullb0/dev); sudo -n mknod /xfs/x9/null/nullb0 b \${dev%%:*} \${dev#*:} && sudo -n chown \$(id -un) /xfs/x9/null/nullb0"
    changed "made the device node /xfs/x9/null/nullb0 for nullb0, owned by the run's user"
    # the evo leg's rings, written through once by a run of each placement, kept from then on
    for place in shards core:1; do
        on "cd $REMOTE && env SHOAL_X9_PLACE=$place SHOAL_X9_RATE_MIB=100 SHOAL_X9_UNIT=65536 SHOAL_X9_DIR=/xfs/x9/pool taskset -c 6,7 ./shoal-workload run --id macro/grid/unsorted/r50/1024 --conf conf.yml --port 13900 --json warm.json > warm.log 2>&1; echo warmed $place: \$?; rm -rf shoal/* warm.json warm.log"
    done
    on "ls -la /xfs/x9/pool /xfs/x9/null"
    exit 0
    ;;
teardown)
    on "rm -rf $REMOTE /xfs/x9"
    changed "removed $REMOTE and /xfs/x9 (the rings and the device node)"
    on "sudo -n rmmod null_blk"
    changed "unloaded null_blk"
    exit 0
    ;;
verify)
    # the host as it was found: nothing of X9 left, the governor and the timer as they were
    on "echo governor \$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor); echo e2scrub_all.timer \$(systemctl is-active e2scrub_all.timer); echo shoal-tmdb \$(systemctl is-active shoal-tmdb); lsmod | grep -c '^null_blk' || true; ls -d $REMOTE /xfs/x9 2>&1"
    exit 0
    ;;
report)
    python3 "$DIR/x9-report.py" "$OUT/raw" > "$DIR/x9-report.md"
    exit 0
    ;;
rounds) ;;
*)
    echo "usage: x9-lab.sh [setup | rounds | report | teardown | verify]" >&2
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
        if ! on "cd $REMOTE && RATES='$(rates "$leg")' PSR=$psr sh x9-host.sh $leg $(pool "$leg") $round" \
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
    python3 "$DIR/x9-report.py" "$OUT/raw" > "$DIR/x9-report.md"
fi
echo "done: $(date -u +%H:%M:%S)"
