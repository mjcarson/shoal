#!/bin/sh
# Spike X12 on the lab: a rebuild's and a deep scrub's rates under a budget, beside a foreground,
# on each host's rotational disk and SSD, and a rebuild whose survivors come over 1 GbE.
#
# Run from the repository root on europa. Each host's run is a transient systemd unit, `x12-lab`,
# so an ssh that drops cannot end it, and the hosts run at once:
#
#   sh shoal-spike/results/x12-lab.sh setup europa titan hyperion   # directories, checked (once)
#   sh shoal-spike/results/x12-lab.sh start titan                   # quick pass, then four rounds
#   sh shoal-spike/results/x12-lab.sh start hyperion
#   sh shoal-spike/results/x12-lab.sh start europa
#   sh shoal-spike/results/x12-lab.sh status titan                  # the unit and its last lines
#   sh shoal-spike/results/x12-lab.sh net                           # after the hosts: across hosts
#   sh shoal-spike/results/x12-lab.sh supplement titan              # the two supplements, below
#   sh shoal-spike/results/x12-lab.sh fetch titan                   # records into results/x12/
#   sh shoal-spike/results/x12-lab.sh report                        # results/x12-report.md
#   sh shoal-spike/results/x12-lab.sh finish europa titan hyperion  # directories gone
#   sh shoal-spike/results/x12-lab.sh verify europa titan hyperion  # every host as it was found
#
# The binary is one znver1 build, run on every host:
#
#   CARGO_TARGET_DIR=target/lab/x12/znver1 RUSTFLAGS="-C target-cpu=znver1" \
#       cargo build --release -p shoal-spike
#
# A round runs two legs on a host, the disk at /hdd and the SSD (the Optane on europa, the 970
# EVO's XFS volume elsewhere), the disk first in odd rounds and the SSD first in even ones; each leg
# runs X12's four measurements, codec, granularity, deep and rebuild. On the disk's leg the
# foreground's journal is on the SSD, as Q23 has a rotational device's, and the disk's write cache
# is off for the whole run, as Q23 has it too: the harness refuses a disk whose cache is on. The
# populations are made once and kept across rounds. The host's governor is `performance`, its
# e2scrub and fstrim timers are held, its locked memory is unlimited as a deployed node's is, and
# the governor, the timers and the write cache are put back on any exit. No shoal unit may be
# running. Every change to a host is appended to shoal-spike/results/x12-host-changes.txt.
#
# `net` runs after the hosts' rounds: titan and hyperion serve their disk's populations from units
# of their own, `x12-serve`, and europa rebuilds from them over 1 GbE into its disk and into its
# Optane, four rounds, the destination's order alternating by round.
#
# `supplement` runs the two supplements added after the rounds, on each host under legs of their
# own. When P1 fired on a summary a unit long: four rounds of the cpu measurement alone, which now
# also folds each unit into a summary one block of 4 KiB long (`<host> codec`). When a fixed 5 MiB/s
# scrub, which should cost a disk's foreground nothing, straddled 1.25x on two disks of three: four
# rounds of the disk's scrub and rebuild at the paces the verdicts turn on, with windows of 90 s in
# place of 20, so a side's p99 is drawn from 900 writes and not 200 (`<host> hdd xfs long`).
#
# START=<round> begins at a later round and QUICK=0 skips the quick pass, which is how a run that
# was stopped is taken up again. ROUNDS=<n> runs fewer. Never build on europa while its unit runs.
set -u

BIN=${BIN:-target/lab/x12/znver1/release/shoal-spike}
REMOTE=/var/tmp/x12
RESULTS=shoal-spike/results
CHANGES=$RESULTS/x12-host-changes.txt
DISK=/dev/sda
HDD=/hdd
SET=codec,granularity,deep,rebuild
PORT=13400

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

# a host's address on the lab's network
addr_of() {
    case $1 in
        europa) echo 172.16.2.10 ;;
        titan) echo 172.16.2.4 ;;
        hyperion) echo 172.16.2.5 ;;
    esac
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
    echo "x12: the write cache reads '$have' after turning it $want" >&2
    return 1
}

# the governor, the timers and the write cache as found, put back on any exit; the cache off
hold() {
    GOVERNOR=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)
    CACHE=$(cat /sys/block/sda/queue/write_cache)
    TIMERS=""
    for timer in e2scrub_all.timer fstrim.timer; do
        if [ "$(systemctl is-active "$timer" 2>/dev/null)" = active ]; then
            TIMERS="$TIMERS $timer"
            systemctl stop "$timer"
        fi
    done
    trap restore EXIT
    trap 'pkill -P $$; exit 143' INT TERM
    cpupower frequency-set -g performance >/dev/null
    write_cache off || exit 1
}

# put back what hold took
restore() {
    if [ "$CACHE" = "write back" ]; then write_cache on || true; fi
    cpupower frequency-set -g "$GOVERNOR" >/dev/null 2>&1
    for timer in $TIMERS; do systemctl start "$timer"; done
    echo "x12: restored governor $GOVERNOR, timers$TIMERS, write cache $(cat /sys/block/sda/queue/write_cache) at $(date -u +%FT%TZ)"
}

# refuse to start beside a shoal unit
no_shoal() {
    active=$(systemctl list-units --state=active --no-legend 'shoal*' || true)
    if [ -n "$active" ]; then
        echo "x12: a shoal unit is running: $active" >&2
        exit 1
    fi
}

# the disk's temperature, from SMART or hdparm
temperature() {
    if command -v smartctl >/dev/null 2>&1; then
        smartctl -A "$DISK" | awk '/Temperature_Celsius/ { print $10 " C" }'
    else
        hdparm -H "$DISK" | awk -F: '/temperature \(celsius\)/ { gsub(/ /, "", $2); print $2 " C" }'
    fi
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
    ulimit -l
}

# one leg: the disk or the SSD, a quick pass or a round
leg() {
    kind=$1
    round=$2
    ssd=$3
    quick=$4
    host=$(hostname)
    out=$REMOTE/out
    if [ "$kind" = hdd ]; then
        base=$HDD/x12
        extra="--ssd-dir $ssd/x12/journal --expect-fs xfs"
        name="$host hdd xfs"
    else
        base=$ssd/x12
        extra="--expect-fs xfs"
        name="$host ssd xfs"
    fi
    dir=$base/run
    if [ "$quick" = 1 ]; then
        # shellcheck disable=SC2086
        "$REMOTE/shoal-spike" device quick --only "$SET" --dir "$base/quick" $extra --leg "$name" \
            > "$out/$host-$kind-quick.md" 2> "$out/$host-$kind-quick.err" \
            || { echo "x12: quick failed on $kind"; return 1; }
        rm -rf "$base/quick" "$ssd/x12/journal"
    else
        # shellcheck disable=SC2086
        "$REMOTE/shoal-spike" device all --only "$SET" --dir "$dir" $extra --round "$round" --leg "$name" \
            --out "$out/$host-$kind.json" > "$out/$host-$kind-r$round.md" 2> "$out/$host-$kind-r$round.err"
        echo "x12: round $round $kind exit $? at $(date -u +%FT%TZ), disk $(temperature)"
    fi
}

# a quick pass on each device, then the rounds
host_run() {
    ssd=$1
    no_shoal
    hold
    rounds=${ROUNDS:-4}
    start=${START:-1}
    quick=${QUICK:-1}
    echo "x12: $(hostname) ssd $ssd governor $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor) cache $(cat /sys/block/sda/queue/write_cache) memlock $(ulimit -l) start $(date -u +%FT%TZ)"
    facts "$ssd" >> "$REMOTE/out/$(hostname)-facts.txt" 2>&1
    if [ "$quick" = 1 ]; then
        for kind in hdd ssd; do
            leg "$kind" 0 "$ssd" 1 || exit 1
            echo "x12: quick passed on $kind at $(date -u +%FT%TZ)"
        done
    fi
    round=$start
    while [ "$round" -le "$rounds" ]; do
        order="hdd ssd"
        if [ $((round % 2)) -eq 0 ]; then order="ssd hdd"; fi
        for kind in $order; do
            leg "$kind" "$round" "$ssd" 0 || exit 1
        done
        round=$((round + 1))
    done
    echo "x12: done $(date -u +%FT%TZ)"
}

# serve this host's disk populations until stopped
host_serve() {
    no_shoal
    hold
    echo "x12: $(hostname) serving from $HDD/x12/run at $(date -u +%FT%TZ)"
    "$REMOTE/shoal-spike" device serve --dir "$HDD/x12/run" --port "$PORT" &
    wait $!
}

# the rebuild across hosts, on europa, from titan's and hyperion's servers
host_net() {
    ssd=$1
    peers=$2
    no_shoal
    hold
    rounds=${ROUNDS:-4}
    out=$REMOTE/out
    echo "x12: net from $peers start $(date -u +%FT%TZ)"
    round=1
    while [ "$round" -le "$rounds" ]; do
        order="hdd ssd"
        if [ $((round % 2)) -eq 0 ]; then order="ssd hdd"; fi
        for kind in $order; do
            if [ "$kind" = hdd ]; then
                dir=$HDD/x12/run
                extra="--ssd-dir $ssd/x12/journal"
            else
                dir=$ssd/x12/run
                extra=""
            fi
            # shellcheck disable=SC2086
            "$REMOTE/shoal-spike" device rebuild-net --dir "$dir" $extra --expect-fs xfs --peers "$peers" \
                --round "$round" --leg "$(hostname) $kind xfs net" --out "$out/$(hostname)-net.json" \
                > "$out/$(hostname)-net-$kind-r$round.md" 2> "$out/$(hostname)-net-$kind-r$round.err"
            echo "x12: net round $round $kind exit $? at $(date -u +%FT%TZ)"
        done
        round=$((round + 1))
    done
    echo "x12: net done $(date -u +%FT%TZ)"
}

# the first supplement: the cpu measurement alone, four rounds, its own leg
codec_rounds() {
    ssd=$1
    rounds=${ROUNDS:-4}
    round=1
    while [ "$round" -le "$rounds" ]; do
        "$REMOTE/shoal-spike" device all --only codec --dir "$ssd/x12/codec" --expect-fs xfs --round "$round" \
            --leg "$(hostname) codec" --out "$REMOTE/out/$(hostname)-codec.json" \
            > "$REMOTE/out/$(hostname)-codec-r$round.md" 2> "$REMOTE/out/$(hostname)-codec-r$round.err"
        echo "x12: codec round $round exit $? at $(date -u +%FT%TZ)"
        round=$((round + 1))
    done
    echo "x12: codec done $(date -u +%FT%TZ)"
}

# the second supplement: the disk's scrub and rebuild at the paces the verdicts turn on, with
# windows of 90 s, four rounds, its own leg
long_rounds() {
    ssd=$1
    rounds=${ROUNDS:-4}
    host=$(hostname)
    round=1
    while [ "$round" -le "$rounds" ]; do
        "$REMOTE/shoal-spike" device all --only deep,rebuild --dir "$HDD/x12/run" --ssd-dir "$ssd/x12/journal" \
            --expect-fs xfs --round "$round" --leg "$host hdd xfs long" --window-s 90 \
            --paces none,fixed-5,fixed-10,idle,idle-ceil-20 --roles dest --out "$REMOTE/out/$host-hdd-long.json" \
            > "$REMOTE/out/$host-hdd-long-r$round.md" 2> "$REMOTE/out/$host-hdd-long-r$round.err"
        echo "x12: long round $round exit $? at $(date -u +%FT%TZ), disk $(temperature)"
        round=$((round + 1))
    done
    echo "x12: long done $(date -u +%FT%TZ)"
}

# both supplements under one hold
host_supplement() {
    ssd=$1
    no_shoal
    hold
    codec_rounds "$ssd"
    long_rounds "$ssd"
}

# the directories setup made, and what is on each device
host_setup() {
    ssd=$1
    for dir in "$HDD" "$ssd"; do
        if [ "$(findmnt -n -o FSTYPE -T "$dir")" != xfs ]; then
            echo "x12: $dir is not XFS" >&2
            exit 1
        fi
        free=$(df -B1G --output=avail "$dir" | tail -1 | tr -d ' ')
        if [ "$free" -lt 20 ]; then
            echo "x12: $dir has $free GiB free, under 20" >&2
            exit 1
        fi
    done
    mkdir -p "$HDD/x12" "$ssd/x12" "$REMOTE/out"
    echo "made $HDD/x12 and $ssd/x12; write cache $(cat /sys/block/sda/queue/write_cache); governor $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor); shoal-tmdb $(systemctl is-active shoal-tmdb 2>/dev/null)"
}

# what the run left, removed
host_finish() {
    ssd=$1
    rm -rf "$HDD/x12" "$ssd/x12"
    echo "removed $HDD/x12 and $ssd/x12; write cache $(cat /sys/block/sda/queue/write_cache); governor $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)"
}

# ---- europa's half, which drives the hosts ----

command=${1:-help}
shift || true
case $command in
    host-run) host_run "$@" ;;
    host-serve) host_serve ;;
    host-net) host_net "$@" ;;
    host-setup) host_setup "$@" ;;
    host-supplement) host_supplement "$@" ;;
    host-finish) host_finish "$@" ;;
    setup)
        for host in "$@"; do
            put "$host" "$0"
            line=$(on "$host" "sudo -n sh $REMOTE/x12-lab.sh host-setup $(ssd_of "$host")") || { echo "x12: $host refused: $line"; exit 1; }
            changed "$host" "$line"
            echo "x12: $host set up"
        done
        ;;
    start)
        host=$1
        put "$host" "$0"
        put "$host" "$BIN"
        on "$host" "sudo -n systemctl reset-failed x12-lab 2>/dev/null; sudo -n systemd-run --unit=x12-lab --collect \
            -p LimitMEMLOCK=infinity -p LimitNOFILE=1048576 \
            --setenv=ROUNDS=${ROUNDS:-4} --setenv=START=${START:-1} --setenv=QUICK=${QUICK:-1} \
            sh -c 'sh $REMOTE/x12-lab.sh host-run $(ssd_of "$host") >> $REMOTE/out/x12-lab.log 2>&1'"
        changed "$host" "transient unit x12-lab started: governor performance, write cache off, timers held, all put back on exit"
        echo "x12: $host started"
        ;;
    net)
        peers=""
        for host in titan hyperion; do
            put "$host" "$0"
            put "$host" "$BIN"
            on "$host" "sudo -n systemctl reset-failed x12-serve 2>/dev/null; sudo -n systemd-run --unit=x12-serve --collect \
                -p LimitMEMLOCK=infinity -p LimitNOFILE=1048576 \
                sh -c 'sh $REMOTE/x12-lab.sh host-serve >> $REMOTE/out/x12-serve.log 2>&1'"
            changed "$host" "transient unit x12-serve started on port $PORT"
            peers="$peers${peers:+,}$(addr_of "$host"):$PORT"
        done
        # the servers open their populations before they listen
        sleep 30
        put europa "$0"
        put europa "$BIN"
        on europa "sudo -n systemctl reset-failed x12-net 2>/dev/null; sudo -n systemd-run --unit=x12-net --collect --wait \
            -p LimitMEMLOCK=infinity -p LimitNOFILE=1048576 --setenv=ROUNDS=${ROUNDS:-4} \
            sh -c 'sh $REMOTE/x12-lab.sh host-net $(ssd_of europa) $peers >> $REMOTE/out/x12-net.log 2>&1'"
        for host in titan hyperion; do
            on "$host" "sudo -n systemctl stop x12-serve"
            changed "$host" "transient unit x12-serve stopped"
        done
        echo "x12: net done"
        ;;
    supplement)
        host=$1
        put "$host" "$0"
        put "$host" "$BIN"
        on "$host" "sudo -n systemctl reset-failed x12-supplement 2>/dev/null; sudo -n systemd-run --unit=x12-supplement --collect \
            -p LimitMEMLOCK=infinity -p LimitNOFILE=1048576 --setenv=ROUNDS=${ROUNDS:-4} \
            sh -c 'sh $REMOTE/x12-lab.sh host-supplement $(ssd_of "$host") >> $REMOTE/out/x12-supplement.log 2>&1'"
        changed "$host" "transient unit x12-supplement started: governor performance, write cache off, timers held, all put back on exit"
        echo "x12: $host supplement started"
        ;;
    status)
        host=$1
        on "$host" "systemctl is-active x12-lab x12-serve x12-net x12-supplement; tail -5 $REMOTE/out/x12-*.log 2>/dev/null; ls -t $REMOTE/out/*.err 2>/dev/null | head -1 | xargs -r tail -3"
        ;;
    fetch)
        host=$1
        mkdir -p "target/lab/x12/fetch/$host" "$RESULTS/x12"
        if [ "$host" = europa ]; then
            cp "$REMOTE"/out/* "target/lab/x12/fetch/$host/"
        else
            scp -q "$host:$REMOTE/out/*" "target/lab/x12/fetch/$host/"
        fi
        # every file is named by its host already but the logs, which are named here
        for file in "target/lab/x12/fetch/$host"/*; do
            name=$(basename "$file")
            case $name in x12-*.log) name=$host-${name#x12-} ;; esac
            cp "$file" "$RESULTS/x12/$name"
        done
        echo "x12: $host fetched"
        ;;
    report)
        "$BIN" device report "$RESULTS"/x12/*.json > "$RESULTS/x12-report.md"
        echo "x12: $RESULTS/x12-report.md"
        ;;
    finish)
        for host in "$@"; do
            put "$host" "$0"
            line=$(on "$host" "sudo -n sh $REMOTE/x12-lab.sh host-finish $(ssd_of "$host")")
            on "$host" "rm -rf $REMOTE"
            changed "$host" "$line; $REMOTE removed"
            echo "x12: $host finished"
        done
        ;;
    verify)
        for host in "$@"; do
            echo "== $host"
            on "$host" "systemctl is-active x12-lab x12-serve x12-net x12-supplement 2>/dev/null; cat /sys/block/sda/queue/write_cache; \
                cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor; systemctl is-active e2scrub_all.timer fstrim.timer; \
                systemctl is-active shoal-tmdb; ls -d $HDD/x12 $(ssd_of "$host")/x12 $REMOTE 2>&1"
        done
        ;;
    *)
        sed -n '2,49p' "$0"
        ;;
esac
