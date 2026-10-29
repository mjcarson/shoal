#!/bin/sh
# Spike X7 on the lab: S6's device store measured on each host's rotational disk.
#
# Run from the repository root on europa. Each host's run is a transient systemd unit, `x7-lab`,
# so an ssh that drops cannot end it, and the hosts run at once:
#
#   sh shoal-spike/results/x7-lab.sh setup europa titan hyperion   # wipe, partition, /hdd (once)
#   sh shoal-spike/results/x7-lab.sh start titan full              # quick pass, then four rounds
#   sh shoal-spike/results/x7-lab.sh start hyperion core
#   sh shoal-spike/results/x7-lab.sh start europa full
#   sh shoal-spike/results/x7-lab.sh status titan                  # the unit and its last lines
#   sh shoal-spike/results/x7-lab.sh extra hyperion                # listing-1m, after the rounds
#   sh shoal-spike/results/x7-lab.sh cache titan                   # the write cache supplement
#   sh shoal-spike/results/x7-lab.sh fetch titan                   # records into results/
#   sh shoal-spike/results/x7-lab.sh finish titan                  # an empty XFS left at /hdd
#
# The binary is one znver1 build, run on every host:
#
#   CARGO_TARGET_DIR=target/lab/x7/znver1 RUSTFLAGS="-C target-cpu=znver1" \
#       cargo build --release -p shoal-spike
#
# A leg is one filesystem in one round, and every leg is made on a filesystem made for it: the
# disk's one partition spans it, so both filesystems see the same zones and the same seek spans,
# and are re-made, XFS then ext4 in odd rounds and the other way in even ones. After each XFS leg
# the disk's write cache is turned off for `wcoff` and on again. The host's governor is
# `performance` for the run, its e2scrub and fstrim timers are held, and all three and the write
# cache are put back on any exit. No shoal unit may be running. Every change setup and finish make
# to a host is appended to shoal-spike/results/x7-host-changes.txt.
#
# START=<round> begins at a later round and QUICK=0 skips the quick pass, which is how a run that
# was stopped is taken up again. ROUNDS=<n> runs fewer.
#
# `cache` is a supplement added after the first round, when titan's and hyperion's disks answered
# a flush among reads in about a tenth of a second where alone they took a rotation: fio, with no
# harness in the way, times reads and synced writes alone and together with the cache on and off,
# then four rounds of `contend` and `scrub` on one XFS leg, the cache on then off in odd rounds and
# the other way in even ones, every side named -wb or -wt.
set -u

BIN=${BIN:-target/lab/x7/znver1/release/shoal-spike}
REMOTE=/var/tmp/x7
RESULTS=shoal-spike/results
CHANGES=$RESULTS/x7-host-changes.txt
DISK=/dev/sda
PART=/dev/sda1
MOUNT=/hdd

# the measurements each set runs: everything on titan and europa, titan's core repeated on hyperion
FULL=seq,read,listing,partial,journal,chunk,chunk-recycle,remove,slices,sync,contend,scrub,shared
CORE=seq,sync,journal,partial,chunk,chunk-recycle,contend,scrub

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

# the SSD scratch directory of a host: the Optane on europa, the 970 EVO's XFS volume elsewhere
ssd_of() {
    if [ "$1" = europa ]; then echo /optane; else echo /xfs; fi
}

# ---- the host's half, run as root on the host itself ----

# what a disk looks like, for the record of host changes
disk_state() {
    echo "-- $(hostname) $(date -u +%FT%TZ)"
    lsblk -o NAME,SIZE,TYPE,FSTYPE,LABEL,MOUNTPOINTS,MODEL,ROTA "$DISK"
    blkid "$DISK"* || true
    sgdisk -p "$DISK" || true
}

# wipe the disk and give it one partition spanning it, named x7
host_setup() {
    echo "== setup on $(hostname): before"
    disk_state
    # nothing of the disk may be mounted
    if findmnt -rn -S "$PART" >/dev/null 2>&1 || grep -q "^$DISK" /proc/mounts; then
        echo "x7: $DISK is mounted; refusing" >&2
        exit 1
    fi
    # every old partition's signatures, then the first and last 16 MiB of each, where ZFS keeps
    # its four labels, then the disk's own
    for part in "$DISK"[0-9]*; do
        [ -b "$part" ] || continue
        wipefs -a "$part"
        sectors=$(blockdev --getsz "$part")
        # a partition of 32 MiB or less is zeroed whole (ZFS's 8 MiB reserve is one)
        if [ "$sectors" -le 65536 ]; then
            dd if=/dev/zero of="$part" bs=512 count="$sectors" conv=fsync status=none
            continue
        fi
        dd if=/dev/zero of="$part" bs=1M count=16 conv=fsync status=none
        dd if=/dev/zero of="$part" bs=512 seek=$((sectors - 32768)) count=32768 conv=fsync status=none
    done
    wipefs -a "$DISK"
    sgdisk --zap-all "$DISK"
    # one partition from 1 MiB to the end
    sgdisk -n 1:1MiB:0 -t 1:8300 -c 1:x7 "$DISK"
    partprobe "$DISK"
    udevadm settle
    mkdir -p "$MOUNT"
    echo "== setup on $(hostname): after"
    disk_state
}

# make a leg's filesystem on the partition and mount it
make_fs() {
    fs=$1
    umount "$MOUNT" 2>/dev/null || true
    case $fs in
        xfs) mkfs.xfs -f -q -L x7 "$PART" ;;
        ext4) mkfs.ext4 -F -q -L x7 -E lazy_itable_init=0,lazy_journal_init=0 "$PART" ;;
    esac
    mount -t "$fs" "$PART" "$MOUNT"
    mkdir -p "$MOUNT/x7"
}

# the disk's temperature, by whichever tool the host has
temperature() {
    if command -v smartctl >/dev/null 2>&1; then
        smartctl -A "$DISK" | awk '/Temperature_Celsius/ { print $10 " C" }'
    else
        hdparm -H "$DISK" | awk -F: '/temperature \(celsius\)/ { gsub(/ /, "", $2); print $2 " C" }'
    fi
}

# the facts of the filesystem a leg runs on
fs_facts() {
    fs=$1
    echo "-- $(hostname) $fs $(date -u +%FT%TZ) disk $(temperature)"
    findmnt "$MOUNT"
    case $fs in
        xfs) xfs_info "$MOUNT" ;;
        ext4) tune2fs -l "$PART" | head -45 ;;
    esac
    hdparm -W "$DISK" | tail -1
    cat /sys/block/sda/queue/write_cache
}

# the write cache off or on, through the drive and the kernel's view of it
write_cache() {
    want=$1
    if [ "$want" = off ]; then hdparm -W0 "$DISK" >/dev/null; else hdparm -W1 "$DISK" >/dev/null; fi
    echo 1 > /sys/block/sda/device/rescan
    sleep 3
    have=$(cat /sys/block/sda/queue/write_cache)
    case "$want:$have" in
        "off:write through" | "on:write back") return 0 ;;
    esac
    echo "x7: the write cache reads '$have' after turning it $want" >&2
    return 1
}

# one quick pass or one round's leg of the device measurements
leg() {
    set_name=$1
    fs=$2
    round=$3
    ssd=$4
    quick=$5
    host=$(hostname)
    out=$REMOTE/out
    rm -rf "$ssd/x7"
    mkdir -p "$ssd/x7"
    make_fs "$fs"
    fs_facts "$fs" >> "$out/$host-facts.txt" 2>&1
    sleep 60
    if [ "$quick" = 1 ]; then
        "$REMOTE/shoal-spike" device quick --only "$set_name" --dir "$MOUNT/x7/quick" --ssd-dir "$ssd/x7/quick" \
            --expect-fs "$fs" --leg "$host hdd $fs" > "$out/$host-hdd-$fs-quick.md" 2> "$out/$host-hdd-$fs-quick.err" \
            || { echo "x7: quick failed on $fs"; return 1; }
    else
        "$REMOTE/shoal-spike" device all --only "$set_name" --dir "$MOUNT/x7/run" --ssd-dir "$ssd/x7/run" \
            --expect-fs "$fs" --round "$round" --leg "$host hdd $fs" --out "$out/$host-hdd-$fs.json" \
            > "$out/$host-hdd-$fs-r$round.md" 2> "$out/$host-hdd-$fs-r$round.err"
        echo "x7: round $round $fs exit $? at $(date -u +%FT%TZ), disk $(temperature)"
    fi
    # the write cache off for the cells that ask what it costs, on XFS legs only
    if [ "$fs" = xfs ]; then
        write_cache off || return 1
        if [ "$quick" = 1 ]; then
            "$REMOTE/shoal-spike" device wcoff --quick --dir "$MOUNT/x7/quick" --ssd-dir "$ssd/x7/quick" \
                --expect-fs xfs --leg "$host hdd xfs" > "$out/$host-hdd-xfs-wcoff-quick.md" 2> "$out/$host-hdd-xfs-wcoff-quick.err"
        else
            "$REMOTE/shoal-spike" device wcoff --dir "$MOUNT/x7/run" --ssd-dir "$ssd/x7/run" --expect-fs xfs \
                --round "$round" --leg "$host hdd xfs" --out "$out/$host-hdd-xfs.json" \
                > "$out/$host-hdd-xfs-r$round-wcoff.md" 2> "$out/$host-hdd-xfs-r$round-wcoff.err"
        fi
        echo "x7: round $round wcoff exit $? at $(date -u +%FT%TZ)"
        write_cache on || return 1
    fi
}

# the governor, the timers and the write cache as found, put back on any exit
hold() {
    GOVERNOR=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)
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
}

# put back what hold took
restore() {
    write_cache on || true
    cpupower frequency-set -g "$GOVERNOR" >/dev/null 2>&1
    for timer in $TIMERS; do systemctl start "$timer"; done
    echo "x7: restored governor $GOVERNOR, timers$TIMERS, write cache $(cat /sys/block/sda/queue/write_cache) at $(date -u +%FT%TZ)"
}

# refuse to start beside a shoal unit
no_shoal() {
    active=$(systemctl list-units --state=active --no-legend 'shoal*' || true)
    if [ -n "$active" ]; then
        echo "x7: a shoal unit is running: $active" >&2
        exit 1
    fi
}

# a quick pass on each filesystem, then the rounds
host_run() {
    set_name=$1
    ssd=$2
    no_shoal
    hold
    rounds=${ROUNDS:-4}
    start=${START:-1}
    quick=${QUICK:-1}
    echo "x7: $(hostname) set $set_name ssd $ssd governor $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor) start $(date -u +%FT%TZ)"
    {
        date -u +%FT%TZ
        uname -a
        lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINTS,MODEL,ROTA
        hdparm -I "$DISK" | grep -E 'Model|Firmware|Rotation|Sector size|device size|Write cache|TRIM'
        cat /sys/block/sda/queue/scheduler /sys/block/sda/queue/nr_requests /sys/block/sda/device/queue_depth
        findmnt -T "$ssd"
    } >> "$REMOTE/out/$(hostname)-facts.txt" 2>&1
    if [ "$quick" = 1 ]; then
        for fs in xfs ext4; do
            leg "$set_name" "$fs" 0 "$ssd" 1 || exit 1
            echo "x7: quick passed on $fs at $(date -u +%FT%TZ)"
        done
    fi
    round=$start
    while [ "$round" -le "$rounds" ]; do
        order="xfs ext4"
        if [ $((round % 2)) -eq 0 ]; then order="ext4 xfs"; fi
        for fs in $order; do
            leg "$set_name" "$fs" "$round" "$ssd" 0 || exit 1
        done
        round=$((round + 1))
    done
    echo "x7: done $(date -u +%FT%TZ)"
}

# the million-chunk listing, once on each filesystem, after the rounds
host_extra() {
    ssd=$1
    no_shoal
    hold
    for fs in xfs ext4; do
        rm -rf "$ssd/x7"
        mkdir -p "$ssd/x7"
        make_fs "$fs"
        fs_facts "$fs" >> "$REMOTE/out/$(hostname)-facts.txt" 2>&1
        sleep 60
        "$REMOTE/shoal-spike" device listing-1m --dir "$MOUNT/x7/run" --ssd-dir "$ssd/x7/run" --expect-fs "$fs" \
            --round 1 --leg "$(hostname) hdd $fs" --out "$REMOTE/out/$(hostname)-hdd-$fs.json" \
            > "$REMOTE/out/$(hostname)-hdd-$fs-listing-1m.md" 2> "$REMOTE/out/$(hostname)-hdd-$fs-listing-1m.err"
        echo "x7: listing-1m $fs exit $? at $(date -u +%FT%TZ)"
    done
    echo "x7: extra done $(date -u +%FT%TZ)"
}

# fio alone, no harness: 64 KiB random reads and 8 KiB synced writes, each paced at 20 a second,
# alone and together, with the cache as it is
flush_check() {
    tag=$1
    dir=$MOUNT/x7/flush
    mkdir -p "$dir"
    for mix in reads stages both; do
        jobs=""
        case $mix in
            reads | both) jobs="$jobs --name=reads --filename=$dir/reads --size=8g --rw=randread --bs=64k --rate_iops=20 --rate_process=poisson" ;;
        esac
        case $mix in
            stages | both) jobs="$jobs --name=stages --filename=$dir/journal --size=256m --rw=write --bs=8k --overwrite=1 --fdatasync=1 --rate_iops=20 --rate_process=poisson" ;;
        esac
        # shellcheck disable=SC2086
        fio --output-format=json --direct=1 --ioengine=psync --time_based --runtime=30 --fallocate=none $jobs \
            > "$REMOTE/out/$(hostname)-flush-$tag-$mix.json" 2>&1
        echo "x7: flush check $tag $mix exit $? at $(date -u +%FT%TZ)"
    done
}

# the write cache supplement: fio's flush check with the cache on and off, then contend and scrub
# with the cache on and off, alternating by round, on one XFS leg
host_cache() {
    ssd=$1
    no_shoal
    hold
    rounds=${ROUNDS:-4}
    host=$(hostname)
    out=$REMOTE/out
    rm -rf "$ssd/x7"
    mkdir -p "$ssd/x7"
    make_fs xfs
    fs_facts xfs >> "$out/$host-facts.txt" 2>&1
    sleep 60
    flush_check wb
    write_cache off || exit 1
    flush_check wt
    write_cache on || exit 1
    round=1
    while [ "$round" -le "$rounds" ]; do
        order="on off"
        if [ $((round % 2)) -eq 0 ]; then order="off on"; fi
        for cache in $order; do
            write_cache "$cache" || exit 1
            if [ "$cache" = on ]; then suffix=-wb; else suffix=-wt; fi
            "$REMOTE/shoal-spike" device all --only contend,scrub --dir "$MOUNT/x7/run" --ssd-dir "$ssd/x7/run" \
                --expect-fs xfs --round "$round" --leg "$host hdd xfs cache" --side-suffix "$suffix" \
                --out "$out/$host-hdd-xfs-cache.json" > "$out/$host-hdd-xfs-cache-r$round$suffix.md" \
                2> "$out/$host-hdd-xfs-cache-r$round$suffix.err"
            echo "x7: cache round $round $cache exit $? at $(date -u +%FT%TZ), disk $(temperature)"
        done
        round=$((round + 1))
    done
    write_cache on || exit 1
    echo "x7: cache done $(date -u +%FT%TZ)"
}

# leave one empty XFS filesystem at /hdd, mounted at boot, for X12 and M19
host_finish() {
    umount "$MOUNT" 2>/dev/null || true
    mkfs.xfs -f -q -L hdd "$PART"
    uuid=$(blkid -s UUID -o value "$PART")
    sed -i '\| /hdd |d' /etc/fstab
    echo "UUID=$uuid /hdd xfs defaults,nofail 0 2" >> /etc/fstab
    systemctl daemon-reload
    mount "$MOUNT"
    echo "== finish on $(hostname) $(date -u +%FT%TZ)"
    findmnt "$MOUNT"
    grep ' /hdd ' /etc/fstab
    hdparm -W "$DISK" | tail -1
    cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor
    systemctl is-active shoal-tmdb || true
}

# ---- europa's half, which drives the hosts ----

command=${1:-help}
shift || true
case $command in
    host-setup) host_setup ;;
    host-run) host_run "$@" ;;
    host-extra) host_extra "$@" ;;
    host-cache) host_cache "$@" ;;
    host-finish) host_finish ;;
    setup)
        for host in "$@"; do
            put "$host" "$0"
            on "$host" "sudo -n sh $REMOTE/x7-lab.sh host-setup" >> "$CHANGES" 2>&1
            echo "x7: $host set up"
        done
        ;;
    start)
        host=$1
        case $2 in full) set_name=$FULL ;; core) set_name=$CORE ;; *) set_name=$2 ;; esac
        put "$host" "$0"
        put "$host" "$BIN"
        on "$host" "sudo -n systemctl reset-failed x7-lab 2>/dev/null; sudo -n systemd-run --unit=x7-lab --collect \
            --setenv=ROUNDS=${ROUNDS:-4} --setenv=START=${START:-1} --setenv=QUICK=${QUICK:-1} \
            sh -c 'sh $REMOTE/x7-lab.sh host-run $set_name $(ssd_of "$host") >> $REMOTE/out/x7-lab.log 2>&1'"
        echo "x7: $host started"
        ;;
    extra)
        host=$1
        put "$host" "$0"
        put "$host" "$BIN"
        on "$host" "sudo -n systemctl reset-failed x7-lab 2>/dev/null; sudo -n systemd-run --unit=x7-lab --collect \
            sh -c 'sh $REMOTE/x7-lab.sh host-extra $(ssd_of "$host") >> $REMOTE/out/x7-lab.log 2>&1'"
        echo "x7: $host extra started"
        ;;
    cache)
        host=$1
        put "$host" "$0"
        put "$host" "$BIN"
        on "$host" "sudo -n systemctl reset-failed x7-lab 2>/dev/null; sudo -n systemd-run --unit=x7-lab --collect \
            --setenv=ROUNDS=${ROUNDS:-4} sh -c 'sh $REMOTE/x7-lab.sh host-cache $(ssd_of "$host") >> $REMOTE/out/x7-lab.log 2>&1'"
        echo "x7: $host cache supplement started"
        ;;
    status)
        host=$1
        on "$host" "systemctl is-active x7-lab; tail -5 $REMOTE/out/x7-lab.log 2>/dev/null; ls -t $REMOTE/out/*.err 2>/dev/null | head -1 | xargs -r tail -3"
        ;;
    fetch)
        host=$1
        mkdir -p "target/lab/x7/fetch/$host"
        if [ "$host" = europa ]; then
            cp "$REMOTE"/out/* "target/lab/x7/fetch/$host/"
        else
            scp -q "$host:$REMOTE/out/*" "target/lab/x7/fetch/$host/"
        fi
        # every file is named by its host already but the run's log, which is named here
        for file in "target/lab/x7/fetch/$host"/*; do
            name=$(basename "$file")
            if [ "$name" = x7-lab.log ]; then name=$host-lab.log; fi
            cp "$file" "$RESULTS/x7-$name"
        done
        echo "x7: $host fetched"
        ;;
    finish)
        for host in "$@"; do
            put "$host" "$0"
            on "$host" "sudo -n sh $REMOTE/x7-lab.sh host-finish" >> "$CHANGES" 2>&1
            echo "x7: $host finished"
        done
        ;;
    *)
        sed -n '2,38p' "$0"
        ;;
esac
