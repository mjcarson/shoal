#!/bin/sh
# Spike X15 on the lab: a short stripe chunk's file made fresh and synced by itself, or taken from
# a pool of its own length written ahead, beside the store as built, on each host's SSD and disk
# (docs/src/object-storage/short-chunk-files.md).
#
# Run from the repository root on europa. Each host's run is a transient systemd unit, `x15-lab`,
# so an ssh that drops cannot end it, and the hosts run at once:
#
#   sh shoal-spike/results/x15-lab.sh setup europa titan hyperion   # directories, checked (once)
#   sh shoal-spike/results/x15-lab.sh start titan                   # quick pass, then four rounds
#   sh shoal-spike/results/x15-lab.sh start hyperion
#   sh shoal-spike/results/x15-lab.sh start europa
#   sh shoal-spike/results/x15-lab.sh status titan                  # the unit and its last lines
#   sh shoal-spike/results/x15-lab.sh fetch titan                   # records into results/x15/
#   sh shoal-spike/results/x15-lab.sh report                        # results/x15-report.md
#   sh shoal-spike/results/x15-lab.sh finish europa titan hyperion  # directories gone
#   sh shoal-spike/results/x15-lab.sh verify europa titan hyperion  # every host as it was found
#
# The binary is one znver1 build, run on every host:
#
#   CARGO_TARGET_DIR=target/lab/x15/znver1 RUSTFLAGS="-C target-cpu=znver1" \
#       cargo build --release -p shoal-spike
#
# A round runs two legs on a host, the SSD (the Optane on europa, the 970 EVO's XFS volume
# elsewhere) and the disk at /hdd, the SSD first in odd rounds and the disk first in even ones; each
# leg runs `short` alone. The disk's write cache is off for the whole run, as a deployed node keeps
# it and as the harness requires. For the run the host's tmdb node is stopped, its governor is
# `performance`, its e2scrub and fstrim timers are held and its locked memory is unlimited; the node,
# the governor, the timers and the write cache are put back on any exit. Every change to a host is
# appended to shoal-spike/results/x15-host-changes.txt.
#
# START=<round> begins at a later round and QUICK=0 skips the quick pass, which is how a run that
# was stopped is taken up again. ROUNDS=<n> runs fewer. Never build on europa while its unit runs.
set -u

BIN=${BIN:-target/lab/x15/znver1/release/shoal-spike}
REMOTE=/var/tmp/x15
RESULTS=shoal-spike/results
CHANGES=$RESULTS/x15-host-changes.txt
DISK=/dev/sda
HDD=/hdd

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

# copy a file to a host's scratch directory
put() {
    host=$1
    from=$2
    on "$host" "mkdir -p $REMOTE/out"
    if [ "$host" = europa ]; then
        cp "$from" "$REMOTE/"
    else
        scp -q "$from" "$host:$REMOTE/"
    fi
}

# record one change to a host
changed() {
    echo "$(date -u +%FT%TZ) $1: $2" >> "$CHANGES"
}

# the SSD of a host: the Optane on europa, the 970 EVO's XFS volume elsewhere
ssd_of() {
    if [ "$1" = europa ]; then echo /optane; else echo /xfs; fi
}

# ---- the host's half, run as root on the host itself ----

# the disk's write cache off or on, through the drive and the kernel's view of it
write_cache() {
    want=$1
    if [ "$want" = off ]; then hdparm -W0 "$DISK" >/dev/null; else hdparm -W1 "$DISK" >/dev/null; fi
    echo 1 > /sys/block/sda/device/rescan
    sleep 3
    have=$(cat /sys/block/sda/queue/write_cache)
    case "$want:$have" in
        "off:write through" | "on:write back") return 0 ;;
    esac
    echo "x15: the write cache reads '$have' after turning it $want" >&2
    return 1
}

# the tmdb node, the governor, the timers and the write cache as found, put back on any exit
hold() {
    GOVERNOR=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)
    CACHE=$(cat /sys/block/sda/queue/write_cache)
    NODE=$(systemctl is-active shoal-tmdb 2>/dev/null)
    TIMERS=""
    for timer in e2scrub_all.timer fstrim.timer; do
        if [ "$(systemctl is-active "$timer" 2>/dev/null)" = active ]; then
            TIMERS="$TIMERS $timer"
            systemctl stop "$timer"
        fi
    done
    trap restore EXIT
    trap 'pkill -P $$; exit 143' INT TERM
    if [ "$NODE" = active ]; then systemctl stop shoal-tmdb; fi
    cpupower frequency-set -g performance >/dev/null
    write_cache off || exit 1
}

# put back what hold took
restore() {
    if [ "$CACHE" = "write back" ]; then write_cache on || true; fi
    cpupower frequency-set -g "$GOVERNOR" >/dev/null 2>&1
    for timer in $TIMERS; do systemctl start "$timer"; done
    if [ "$NODE" = active ]; then systemctl start shoal-tmdb; fi
    echo "x15: restored governor $GOVERNOR, timers$TIMERS, write cache $(cat /sys/block/sda/queue/write_cache), shoal-tmdb $(systemctl is-active shoal-tmdb 2>/dev/null) at $(date -u +%FT%TZ)"
}

# what a host is, for the record
facts() {
    ssd=$1
    date -u +%FT%TZ
    uname -a
    lscpu | grep -E 'Model name|^CPU\(s\)|Thread|MHz'
    cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor
    lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINTS,MODEL,ROTA
    hdparm -I "$DISK" | grep -E 'Model|Firmware|Rotation|Write cache'
    cat /sys/block/sda/queue/write_cache /sys/block/sda/queue/scheduler
    findmnt "$HDD"
    findmnt -T "$ssd"
    xfs_info "$HDD"
    xfs_info "$(findmnt -n -o TARGET -T "$ssd")"
    ulimit -l
}

# one leg: the SSD or the disk, a quick pass or a round
leg() {
    kind=$1
    round=$2
    ssd=$3
    quick=$4
    host=$(hostname)
    out=$REMOTE/out
    if [ "$kind" = hdd ]; then
        base=$HDD/x15
        name="$host hdd xfs"
    else
        base=$ssd/x15
        name="$host ssd xfs"
    fi
    if [ "$quick" = 1 ]; then
        "$REMOTE/shoal-spike" device quick --only short --dir "$base/quick" --expect-fs xfs --leg "$name" \
            > "$out/$host-$kind-quick.md" 2> "$out/$host-$kind-quick.err" \
            || { echo "x15: quick failed on $kind"; return 1; }
        rm -rf "$base/quick"
    else
        "$REMOTE/shoal-spike" device all --only short --dir "$base/run" --expect-fs xfs --round "$round" --leg "$name" \
            --out "$out/$host-$kind.json" > "$out/$host-$kind-r$round.md" 2> "$out/$host-$kind-r$round.err"
        echo "x15: round $round $kind exit $? at $(date -u +%FT%TZ)"
    fi
}

# a quick pass on each device, then the rounds
host_run() {
    ssd=$1
    hold
    rounds=${ROUNDS:-4}
    start=${START:-1}
    quick=${QUICK:-1}
    echo "x15: $(hostname) ssd $ssd governor $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor) cache $(cat /sys/block/sda/queue/write_cache) memlock $(ulimit -l) start $(date -u +%FT%TZ)"
    facts "$ssd" >> "$REMOTE/out/$(hostname)-facts.txt" 2>&1
    if [ "$quick" = 1 ]; then
        for kind in ssd hdd; do
            leg "$kind" 0 "$ssd" 1 || exit 1
            echo "x15: quick passed on $kind at $(date -u +%FT%TZ)"
        done
    fi
    round=$start
    while [ "$round" -le "$rounds" ]; do
        order="ssd hdd"
        if [ $((round % 2)) -eq 0 ]; then order="hdd ssd"; fi
        for kind in $order; do
            leg "$kind" "$round" "$ssd" 0 || exit 1
        done
        round=$((round + 1))
    done
    echo "x15: done $(date -u +%FT%TZ)"
}

# the directories the run writes in, each on XFS with room
host_setup() {
    ssd=$1
    for dir in "$HDD" "$ssd"; do
        if [ "$(findmnt -n -o FSTYPE -T "$dir")" != xfs ]; then
            echo "x15: $dir is not XFS" >&2
            exit 1
        fi
        free=$(df -B1G --output=avail "$dir" | tail -1 | tr -d ' ')
        if [ "$free" -lt 20 ]; then
            echo "x15: $dir has $free GiB free, under 20" >&2
            exit 1
        fi
    done
    mkdir -p "$HDD/x15" "$ssd/x15" "$REMOTE/out"
    echo "made $HDD/x15 and $ssd/x15; write cache $(cat /sys/block/sda/queue/write_cache); governor $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor); shoal-tmdb $(systemctl is-active shoal-tmdb 2>/dev/null)"
}

# what the run left, removed
host_finish() {
    ssd=$1
    rm -rf "$HDD/x15" "$ssd/x15"
    echo "removed $HDD/x15 and $ssd/x15; write cache $(cat /sys/block/sda/queue/write_cache); governor $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor); shoal-tmdb $(systemctl is-active shoal-tmdb 2>/dev/null)"
}

# ---- europa's half, which drives the hosts ----

command=${1:-help}
shift || true
case $command in
    host-run) host_run "$@" ;;
    host-setup) host_setup "$@" ;;
    host-finish) host_finish "$@" ;;
    setup)
        for host in "$@"; do
            put "$host" "$0"
            line=$(on "$host" "sudo -n sh $REMOTE/x15-lab.sh host-setup $(ssd_of "$host")") || { echo "x15: $host refused: $line"; exit 1; }
            changed "$host" "$line"
            echo "x15: $host set up"
        done
        ;;
    start)
        host=$1
        put "$host" "$0"
        put "$host" "$BIN"
        on "$host" "sudo -n systemctl reset-failed x15-lab 2>/dev/null; sudo -n systemd-run --unit=x15-lab --collect \
            -p LimitMEMLOCK=infinity -p LimitNOFILE=1048576 \
            --setenv=ROUNDS=${ROUNDS:-4} --setenv=START=${START:-1} --setenv=QUICK=${QUICK:-1} \
            sh -c 'sh $REMOTE/x15-lab.sh host-run $(ssd_of "$host") >> $REMOTE/out/x15-lab.log 2>&1'"
        changed "$host" "transient unit x15-lab started: shoal-tmdb stopped, governor performance, write cache off, timers held, all put back on exit"
        echo "x15: $host started"
        ;;
    status)
        host=$1
        on "$host" "systemctl is-active x15-lab shoal-tmdb; tail -5 $REMOTE/out/x15-lab.log 2>/dev/null; ls -t $REMOTE/out/*.err 2>/dev/null | head -1 | xargs -r tail -3"
        ;;
    fetch)
        host=$1
        mkdir -p "target/lab/x15/fetch/$host" "$RESULTS/x15"
        if [ "$host" = europa ]; then
            cp "$REMOTE"/out/* "target/lab/x15/fetch/$host/"
        else
            scp -q "$host:$REMOTE/out/*" "target/lab/x15/fetch/$host/"
        fi
        # every file is named by its host already but the log, which is named here
        for file in "target/lab/x15/fetch/$host"/*; do
            name=$(basename "$file")
            case $name in x15-*.log) name=$host-${name#x15-} ;; esac
            cp "$file" "$RESULTS/x15/$name"
        done
        echo "x15: $host fetched"
        ;;
    report)
        "$BIN" device report "$RESULTS"/x15/*.json > "$RESULTS/x15-report.md"
        echo "x15: $RESULTS/x15-report.md"
        ;;
    finish)
        for host in "$@"; do
            put "$host" "$0"
            line=$(on "$host" "sudo -n sh $REMOTE/x15-lab.sh host-finish $(ssd_of "$host")")
            on "$host" "rm -rf $REMOTE"
            changed "$host" "$line; $REMOTE removed"
            echo "x15: $host finished"
        done
        ;;
    verify)
        for host in "$@"; do
            echo "== $host"
            on "$host" "systemctl is-active x15-lab 2>/dev/null; cat /sys/block/sda/queue/write_cache; \
                cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor; systemctl is-active e2scrub_all.timer fstrim.timer; \
                systemctl is-active shoal-tmdb; ls -d $HDD/x15 $(ssd_of "$host")/x15 $REMOTE 2>&1"
        done
        ;;
    *)
        sed -n '2,33p' "$0"
        ;;
esac
