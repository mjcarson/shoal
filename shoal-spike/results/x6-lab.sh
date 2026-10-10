#!/bin/sh
# X6 on one lab host: four rounds of `shoal-spike device` over each filesystem it measures, the
# order of the filesystems alternating by round, as the lab's rule asks of any two sides.
#
#   sudo sh x6-lab.sh <only|all> <fs>:<scratch dir> [<fs>:<scratch dir> ...]
#   sudo sh x6-lab.sh all xfs:/x6/xfs/x6 ext4:/x6/ext4/x6 btrfs:/x6/btrfs/x6      # titan
#   sudo sh x6-lab.sh partial,journal,chunk,slices xfs:/x6/xfs/x6 ext4:/x6/ext4/x6  # hyperion
#   sudo sh x6-lab.sh all xfs:/optane/x6/run                                        # europa
#
# Run as root: the spike drops caches and locks memory. The governor is set to `performance`
# and put back on every way out, and on a host whose ext4 is on LVM the weekly e2scrub timer,
# which snapshots and reads every ext4 volume, is held for the run and started again after.
# Everything a run prints and records goes under /var/tmp/x6/out.
#
# START=<round> begins at a later round, QUICK=0 skips the quick pass and SLC=0 the write cache
# probe, which is how a run that was stopped is taken up again without repeating what it did.
# BIN=<path> runs another build, for a measurement added after the rounds began.
set -u
ONLY=$1
shift
BIN=${BIN:-/var/tmp/x6/shoal-spike}
OUT=/var/tmp/x6/out
HOST=$(hostname)
ROUNDS=${ROUNDS:-4}
START=${START:-1}
QUICK=${QUICK:-1}
SLC=${SLC:-1}
mkdir -p "$OUT"

# the governor as found, put back however the script ends
GOVERNOR=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)
SCRUB=$(systemctl is-active e2scrub_all.timer 2>/dev/null || true)
restore() {
    cpupower frequency-set -g "$GOVERNOR" >/dev/null 2>&1
    if [ "$SCRUB" = active ]; then systemctl start e2scrub_all.timer; fi
    echo "x6-lab: restored governor $GOVERNOR, e2scrub timer $SCRUB at $(date -u +%FT%TZ)"
}
# a stop is a stop: the measurement running is ended too, and the restore runs on the way out
trap restore EXIT
trap 'pkill -P $$; exit 143' INT TERM
if [ "$SCRUB" = active ]; then systemctl stop e2scrub_all.timer; fi
cpupower frequency-set -g performance >/dev/null
echo "x6-lab: $HOST governor $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor), shoal-tmdb $(systemctl is-active shoal-tmdb 2>/dev/null), start $(date -u +%FT%TZ)"

# the facts every table leans on, recorded once
{
    date -u +%FT%TZ
    uname -a
    lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINTS,MODEL,ROTA
    for pair in "$@"; do
        dir=${pair#*:}
        mkdir -p "$dir"
        findmnt -T "$dir"
        case ${pair%%:*} in
            xfs) xfs_info "$dir" ;;
            ext4) tune2fs -l "$(findmnt -n -o SOURCE -T "$dir")" ;;
            btrfs) btrfs filesystem df "$dir"; btrfs filesystem show "$dir" ;;
        esac
    done
} > "$OUT/$HOST-facts.txt" 2>&1

# every filesystem proved quickly before any round is measured
for pair in "$@"; do
    [ "$QUICK" = 1 ] || break
    fs=${pair%%:*}
    dir=${pair#*:}
    "$BIN" device quick --dir "$dir/quick" --expect-fs "$fs" > "$OUT/$HOST-$fs-quick.md" 2> "$OUT/$HOST-$fs-quick.err" \
        || { echo "x6-lab: quick failed on $fs"; exit 1; }
    echo "x6-lab: quick passed on $fs at $(date -u +%FT%TZ)"
done

# the write cache's size, once, on the first filesystem, then left to settle
first=${1#*:}
if [ "$SLC" = 1 ]; then
    "$BIN" device slc --dir "$first" > "$OUT/$HOST-slc.md" 2>&1
    fstrim "$(findmnt -n -o TARGET -T "$first")"
    sleep 300
fi

round=$START
while [ "$round" -le "$ROUNDS" ]; do
    # odd rounds in the order given, even rounds reversed
    order=$*
    if [ $((round % 2)) -eq 0 ]; then
        order=$(for pair in "$@"; do echo "$pair"; done | tac | tr '\n' ' ')
    fi
    for pair in $order; do
        fs=${pair%%:*}
        dir=${pair#*:}
        fstrim "$(findmnt -n -o TARGET -T "$dir")"
        echo "x6-lab: round $round $fs start $(date -u +%FT%TZ)"
        # a run of some measurements only is kept apart from the round's own output
        if [ "$ONLY" = all ]; then
            "$BIN" device all --dir "$dir" --expect-fs "$fs" --round "$round" --keep-populations \
                --leg "$HOST $fs" --out "$OUT/$HOST-$fs.json" > "$OUT/$HOST-$fs-r$round.md" 2> "$OUT/$HOST-$fs-r$round.err"
        else
            tag=$(echo "$ONLY" | tr ',' '-')
            "$BIN" device all --only "$ONLY" --dir "$dir" --expect-fs "$fs" --round "$round" --keep-populations \
                --leg "$HOST $fs" --out "$OUT/$HOST-$fs.json" > "$OUT/$HOST-$fs-r$round-$tag.md" 2> "$OUT/$HOST-$fs-r$round-$tag.err"
        fi
        echo "x6-lab: round $round $fs exit $? end $(date -u +%FT%TZ)"
        sleep 120
    done
    round=$((round + 1))
done

# the scratch directories are left for a run that follows, and emptied by `device clean`
echo "x6-lab: done $(date -u +%FT%TZ)"
