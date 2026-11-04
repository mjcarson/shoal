#!/bin/sh
# M23's exit on the lab: the rebuilt walk beside the reference cell and alone, on the lab's SSDs
# (docs/src/future-work/scans/milestones.md, M23). Run from the repository root on europa:
#
#   sh shoal-spike-scan/results/m23-lab.sh snapshot     # every host's state, before anything
#   sh shoal-spike-scan/results/m23-lab.sh check        # the exit's worktree clean and built
#   sh shoal-spike-scan/results/m23-lab.sh data         # the rounds' dataset, once, after `build`
#   QUICK=1 sh shoal-spike-scan/results/m23-lab.sh start  # one round, short arms: every arm runs
#   sh shoal-spike-scan/results/m23-lab.sh start        # the rounds, in a unit
#   CONTROL_EVERY=2 sh shoal-spike-scan/results/m23-lab.sh start   # the rounds again, a control after every two
#   CHURN_ONLY=1 sh shoal-spike-scan/results/m23-lab.sh start   # the churn, at both segment sizes
#   FALL_ONLY=1 sh shoal-spike-scan/results/m23-lab.sh start    # item 351's run: the cell alone, sampled
#   CPU_SAMPLE=1 NIC_ROUNDS="3 6" ROUNDS=6 sh shoal-spike-scan/results/m23-lab.sh start   # item 353's run
#   NODE_ENV="MIMALLOC_PURGE_DELAY=-1" ROUNDS=2 sh shoal-spike-scan/results/m23-lab.sh start   # item 353's purge test
#   sh shoal-spike-scan/results/m23-lab.sh build-drivers         # the tmdb programs, before and after
#   DRIVERS_ONLY=1 sh shoal-spike-scan/results/m23-lab.sh start  # the drivers, before and after
#   LOAD_ONLY=1 sh shoal-spike-scan/results/m23-lab.sh start     # item 357's run: the rounds' preload alone
#   KEYWORD_ONLY=1 sh shoal-spike-scan/results/m23-lab.sh start  # item 355's run: a round's Keyword capture alone
#   BIG_ONLY=1 SCANS_ALT="device_reads: 2" ALT_ROUNDS="2 3 6" sh shoal-spike-scan/results/m23-lab.sh start   # item 359's
#   REPLAY_ONLY=1 sh shoal-spike-scan/results/m23-lab.sh start   # item 359's replay: the scan's reads by fio, the cell alone
#   BIG_ONLY=1 SCANS_ALT="device_time: 1ms" ALT_EVENT_TIMEOUT=225 ALT_ROUNDS="2 3 6" sh shoal-spike-scan/results/m23-lab.sh start   # its twin
#   sh shoal-spike-scan/results/m23-lab.sh status | stop | fetch | verify
#
# One build, the product at the exit's commit, in a clean worktree under $LAB with X16's schema's
# programs built in its target/deploy for znver1 (`build`). A round is one cluster, `m23`, on X16's
# hosts and cores at NODE_MEMORY a node and every scan setting the product's own default, bootstrapped
# fresh and destroyed after: a short run that preloads it, then two captures of it, attached, each
# after a restart so every row starts from its archives, as X16's were: the cell beside a full scan
# of Big, with Big and Keyword scanned alone, and the cell beside a full scan of Keyword. Keyword,
# the TMDB csv's keyword rows and the exit's stand-in for MovieByKeyword, is written into by a paced
# stream of inserts all the while, so its partitions are resident overlays when a scan reads them.
#
# The rounds run again (2026-10-31) on a cell whose feed never wraps inside an arm: the first exit's
# Cell of 300,000 rows left an arm 200,000 to insert, which ran out 88 to 103 s in, past which its
# inserts overwrote the rows the arm had written. Its inserts fell to three fifths of their rate 8 to
# 29 s before that, and a cell that never wraps falls at the same point, item 351's (2026-11-01).
# That fall is the lab's 970 EVOs, whose flush triples about 75 s into the cell's synced writes and
# holds until they rest (FALL_ONLY=1 found it), so every cell arm of the rounds again is warmed 100 s
# before it measures, past it. `data` writes Cell again
# at CELL_ROWS rows, the first 300,000 the same bytes as X16's, beside X16's own Big and Keyword
# linked in, and every capture is refused once its cell's feed wraps. A cell that never wraps keeps
# every row it inserts, about 4 KB of a Zen1 node's memory each, so the nodes run at 6 GiB, not the
# first exit's 4. DATASET=target/lab/x16/dataset NODE_MEMORY=4Gi runs the first exit's rounds, and
# CONTROL_SECS=120 its control.
#
# The control is a round whose Keyword capture is the cell alone, after the same preload, Big capture
# and restart, measured CONTROL_SECS past the same 100 s warmup, so the cell's own drift over a whole
# scan's seconds is told from what a scan beside it adds; m23-report.py cuts it at each table's scans
# as the rounds marked them. The first exit ran CONTROL_ONLY=1 after its rounds, two of 120 s, cut at
# their feed's wrap. The rounds again (2026-11-03) run CONTROL_EVERY=2: a control round after every
# two scan rounds, numbered in turn, each 270 s, past the longest whole scan of Big beside the cell
# (237 s, 18 s after the arm's measuring began, and 10 s after it), about 590,000 of the cell's rows.
#
# The churn (CHURN_ONLY=1) runs at each of SEGMENTS, a cluster of its own each with its WAL's
# segments that size: Big loaded and scanned alone once, then an hour of inserts and overwrites into
# it from X17's churn dataset, then a restart and Big scanned alone again, so the partitions a
# device read of a churned table are measured at today's segments and at larger merge blocks.
#
# The drivers (DRIVERS_ONLY=1) are measured before and after M23 by the bench's own copy of
# tmdb_cluster.yaml, two pool devices a host (F88's second devices, m16-lab.sh's), from a worktree at
# each side's commit with the tmdb programs built in it: rw50 at 1 MiB and two in flight, beside no
# event, a device failed, a deep scrub asked and a device drained, each run on a cluster of its own,
# the sides alternating by round, before first in odd ones. A device arm runs until its moves settle
# and a scrub's until its pass ends, so each arm's during window is the drivers' work as it ran.
#
# Item 353's run (CPU_SAMPLE=1) samples every host's cpus each second beside a scan round's
# captures (m23-cpu-sample.sh, pinned to SAMPLER_CPU, clear of every shard), so a shard's long steps
# can be read beside the interrupts its cpu took; NIC_ROUNDS names the rounds whose Zen1 hosts take
# their NIC's interrupt on NIC_CPU instead of a shard's cpu, moved after the round's bootstrap and
# put back exactly after its teardown and on every way out: the old effective cpu written first,
# then the old list, which x86 keeps a target already inside. Each move is recorded in
# m23-host-changes.txt and every NIC's affinity is in the snapshot `verify` checks.
#
# NODE_ENV names variables every host's node runs with, `KEY=value` separated by spaces: a drop-in
# on the side node's unit (shoal-m23.service.d/m23-env.conf) written before a round's bootstrap and
# removed at its teardown and on every way out, each node's own environment read back from /proc
# after every restart into the round's captures (env.txt). Item 353's purge test runs the rounds with
# mimalloc's purge of freed pages off (MIMALLOC_PURGE_DELAY=-1), so a capture's long step is told from
# the allocator's madvise inside it (2026-11-02).
#
# Item 359's run (BIG_ONLY=1) is a round's preload and its Big capture alone, BIG_ROUNDS times, the
# capture beside every host's devices and node threads sampled each second (m23-fall-sample.sh, pinned
# to SAMPLER_CPU) and every member's own figures read each second (`stats --json --watch 1`), so the
# cell's slow reads beside a scan of Big are read by the member that served them, its device and its
# shards (m23-i359-report.py). The rounds named in ALT_ROUNDS are bootstrapped with SCANS_ALT's scan
# settings in place of SCANS', each round's kept beside its samples (scans.txt) (2026-11-04).
#
# Its replay (REPLAY_ONLY=1) asks the device alone: fewer scan reads in flight cut the scan's rate
# ninefold, its loop work with its reads, so they could not tell the drive from the shard loop. A
# round is a preload, the Big capture's first three arms (the cell alone, Big and Keyword scanned
# alone), then the cell alone for REPLAY_ARM's arm. REPLAY_AT seconds after that arm's capture
# begins, about where a scan is asked, fio on REPLAY_HOSTS reads a file of its own on the node's own
# 970 EVO as the scan read the device: REPLAY_IOPS reads a second of REPLAY_BSSPLIT's sizes, Poisson
# spaced, up to sixteen in flight, for REPLAY_SECS, a whole scan of Big's length. The rounds in
# REPLAY_OFF_ROUNDS keep the same times and run no fio, the control. Each fio's own start and end are
# kept beside the samples (replay-<host>.txt, fio-<host>.json). Its file is written once, in
# REPLAY_DIR, before the first round, and removed with the directory on every way out.
#
# Its twin (BIG_ONLY=1 with SCANS_ALT="device_time: 1ms" and ALT_EVENT_TIMEOUT) asks whether a scan
# raised the cell's read p99 at all. Neither control so far was the scan arm's twin: one followed a
# restart, the replay's a capture of its own, and the replay's climbed further with nothing beside it.
# The twin is the same capture's fourth arm with its scan paced to a seventy-fifth of the default
# share, its event cut at a whole scan's length, so its windows are the default scan's.
#
# The tmdb node, the hosts' maintenance timers and their governors are held for the whole run and
# put back on every way out; every change is recorded in m23-host-changes.txt, which `verify` checks
# against the snapshot. Never edit this file while a run's unit runs: `start` runs a copy of it.
# Never touch /, /xfs and /optane beyond the side cluster's own `/xfs/shoal-m23` and
# `/optane/shoal-m23`, which its bootstrap makes and its destroy removes.
set -u

REPO=$(pwd)
LAB=${LAB:-target/lab/m23}
OUT=${OUT:-shoal-spike-scan/results}
case $LAB in /*) ;; *) LAB=$REPO/$LAB ;; esac
case $OUT in /*) ;; *) OUT=$REPO/$OUT ;; esac
BUILD=${BUILD:-$LAB/wt-m23}
SNAP=$LAB/snap
CHANGES=$OUT/m23-host-changes.txt
HOSTS="europa titan hyperion"
# X16's dataset folder, and X17's churn dataset, which wrote them once
X16_DATASET=${X16_DATASET:-$REPO/target/lab/x16/dataset}
# the rounds' own: a cell that never wraps inside an arm beside X16's Big and Keyword, linked, in
# one place whatever LAB a run keeps its captures under
DATASET=${DATASET:-$REPO/target/lab/m23/dataset}
# the rows of its cell: 100,000 preloaded and a million to insert, an arm's at 2,375 a second
# for 420 s, 1.6 times the first exit's longest
CELL_ROWS=${CELL_ROWS:-1100000}
# a node's memory, which the cell's rows a long arm inserts stay resident in
NODE_MEMORY=${NODE_MEMORY:-6Gi}
CHURN_DATASET=${CHURN_DATASET:-$REPO/target/lab/x17/churn}
ROUNDS=${ROUNDS:-8}
# the WAL's segment sizes the churn runs at: today's default and larger merge blocks
SEGMENTS=${SEGMENTS:-"10MiB 40MiB"}
# the drivers' two sides: the commit before M23's first step and the exit's own
DRIVERS_BEFORE=${DRIVERS_BEFORE:-$LAB/wt-before}
DRIVERS_AFTER=${DRIVERS_AFTER:-$BUILD}
DRIVER_ROUNDS=${DRIVER_ROUNDS:-2}
OBJECTS=${OBJECTS:-$REPO/shoal-spike/results/m16-objects}
DRIVERS_SPEC="--objects rw50 --object-sizes 1MiB --concurrency 2 --events none,device_fail,deep_scrub,device_drain \
--duration 90 --warmup 5 --runs 1 --event-timeout 600 --allow-neighbours"
# the driver's cpus on europa, clear of the side node's control core and shards (0-6, 16-22)
DRIVER_CPUS=12-15,28-31
# what every run of a round preloads, or takes as preloaded: the cell's first 100,000 rows, every
# row of Big and nine tenths of Keyword's, the rest inserted by the arms
PRELOAD="--attach --yes-write --preload 100% --preload-file Cell=100000 --preload-file Keyword=90%"
# the preload's own run: a few seconds of reads once the rows are in
LOAD="--workloads read100 --bundles 1 --tables Cell:1 --duration 5 --warmup 1 --runs 1"
# every capture's spec: rw50 on the cell alone at a bundle of one, Keyword written into beside it,
# the scan event a third of the way in, the clock waiting for it twenty minutes at most
COMMON="--preloaded --yes-events --workloads rw50 --bundles 1 --tables Cell:1 --on-exhaust wrap \
--paced Keyword --paced-workload insert100 --paced-rate 200 --event-at 30 --event-timeout 1200 --runs 1"
# every cell arm warmed 100 s before it measures: about 75 s into a cell's steady synced writes the
# lab's 970 EVOs change from a flush of 0.9 ms to one of 3 ms, and the cell's inserts fall to three
# fifths of their rate there and stay (item 351, the-cells-fall-is-the-drives.md), so an arm measured
# from 10 s compared windows taken in two states of the drive. Warmed past it, before, during and
# after are all in the state a busy node is in
TIMES="--duration 60 --warmup 100"
# the two captures of a round
BIG="--events none,scan --event-table Big --scan-tables Big,Keyword"
KEYWORD="--events scan --event-table Keyword"
# the control (CONTROL_ONLY=1, or among the rounds with CONTROL_EVERY): the cell alone where a round's
# Keyword capture scans, as long as the longest of its scan arms runs, so the cell's own climb over a
# long arm is told from what a scan beside it adds
CONTROL_SECS=${CONTROL_SECS:-270}
CONTROL_WARMUP=100
CONTROL_ROUNDS=${CONTROL_ROUNDS:-2}
# a control round after every this many scan rounds, none unless named
CONTROL_EVERY=${CONTROL_EVERY:-}
# item 357's run (LOAD_ONLY=1): the rounds' preload alone, LOAD_ROUNDS times, each on a cluster of its
# own, its capture keeping each member's write sheds by cause and its retries by code
LOAD_ROUNDS=${LOAD_ROUNDS:-4}
# item 355's run (KEYWORD_ONLY=1): a round's preload and its Keyword capture, the cell beside a full scan
# of Keyword, KEYWORD_ROUNDS times, each on a cluster of its own, its Big capture left out
KEYWORD_ROUNDS=${KEYWORD_ROUNDS:-4}
# item 359's run (BIG_ONLY=1): a round's preload and its Big capture, BIG_ROUNDS times, each on a
# cluster of its own, the rounds in ALT_ROUNDS bootstrapped with SCANS_ALT's scan settings
BIG_ROUNDS=${BIG_ROUNDS:-6}
SCANS_ALT=${SCANS_ALT:-}
ALT_ROUNDS=${ALT_ROUNDS:-}
# an alternate round's scan event cut at this many seconds where named, so a scan paced to almost
# nothing gives windows of a whole scan's length (item 359's twin)
ALT_EVENT_TIMEOUT=${ALT_EVENT_TIMEOUT:-}
# item 359's replay (REPLAY_ONLY=1): REPLAY_ROUNDS rounds, the ones in REPLAY_OFF_ROUNDS with no fio,
# ABBA; the scan's reads of a Zen1 host's 970 EVO over a whole scan of Big beside the cell (round 1 of
# item 359's run: 585 reads a second the disk took beyond the cell's own, 10.5 KiB each, by both the
# disk's counters and the scans' own; sixteen in flight a device at most)
REPLAY_ROUNDS=${REPLAY_ROUNDS:-4}
REPLAY_OFF_ROUNDS=${REPLAY_OFF_ROUNDS:-2 3}
REPLAY_HOSTS="titan hyperion"
REPLAY_IOPS=${REPLAY_IOPS:-585}
REPLAY_SECS=${REPLAY_SECS:-223}
REPLAY_AT=${REPLAY_AT:-135}
REPLAY_BSSPLIT=${REPLAY_BSSPLIT:-4k/50:8k/25:16k/15:32k/7:64k/3}
REPLAY_DIR=/xfs/shoal-m23-replay
REPLAY_FILE=4g
# the replay round's first capture, the Big capture's arms before its scan arm, and its arm of the
# cell alone, long enough that the replay ends inside it with seconds to spare
LEAD="--events none --scan-tables Big,Keyword"
REPLAY_ARM="--preloaded --yes-events --workloads rw50 --bundles 1 --tables Cell:1 --on-exhaust wrap \
--paced Keyword --paced-workload insert100 --paced-rate 200 --duration 280 --warmup 100 --runs 1"
# the churn's runs: Big scanned alone, and an hour of inserts and overwrites into it
SCAN_ALONE="--preloaded --scan-tables Big --duration 45 --warmup 5 --runs 1"
CHURN_PRELOAD="--attach --yes-write --preload 0% --preload-file Cell=100000 --preload-file Big=0%"
CHURN="--preloaded --workloads insert100 --bundles 1 --tables Big:1 --on-exhaust wrap --duration 3600 --warmup 5 --runs 1"
CHURN_LOAD_PRELOAD="--attach --yes-write --preload 0% --preload-file Cell=100000 --preload-file Big=100%"
# item 351's run (FALL_ONLY=1): the cell truly alone, no paced stream and no scan, for two arms a
# rest apart, every host sampled each second; then fio alone on every host, twice the same rest apart
FALL="--preloaded --yes-events --workloads rw50 --bundles 1 --tables Cell:1 --on-exhaust wrap \
--duration 200 --warmup 10 --runs 1"
FIO_SECS=200
REST_SECS=${REST_SECS:-180}
# the per-second sampler, beside this script: the unit runs copies of both from $LAB/run
SAMPLER=$(dirname "$0")/m23-fall-sample.sh
# item 353's: every cpu's time and interrupts and the node's threads a second, beside a scan round,
# on a cpu no host's node runs a shard on (the Zen1 hosts' shards are on 2-6, europa's 1-6)
CPU_SAMPLER=$(dirname "$0")/m23-cpu-sample.sh
SAMPLER_CPU=${SAMPLER_CPU:-7}
# the rounds whose Zen1 hosts take their NIC's interrupt on NIC_CPU, off every shard's cpu
NIC_ROUNDS=${NIC_ROUNDS:-}
NIC_CPU=${NIC_CPU:-7}
NIC_HOSTS="titan hyperion"
# what every host's side node runs with beside its unit's own environment, and where that is kept
NODE_ENV=${NODE_ENV:-}
NODE_DROPIN=/etc/systemd/system/shoal-m23.service.d
# how long a restarted cluster settles once every member is up, its leaders moved where they lead
SETTLE=30
# how long the idle cluster is sampled before each churn scan, its background cpu
IDLE_SECS=${IDLE_SECS:-30}
if [ -n "${QUICK:-}" ]; then
    PRELOAD="$PRELOAD --preload-file Big=10%"
    CHURN_LOAD_PRELOAD="--attach --yes-write --preload 0% --preload-file Cell=100000 --preload-file Big=10%"
    TIMES="--duration 10 --warmup 2"
    DRIVERS_SPEC="--objects rw50 --object-sizes 1MiB --concurrency 2 --events none,device_fail --duration 15 --warmup 2 \
--runs 1 --event-timeout 300 --allow-neighbours"
    SCAN_ALONE="--preloaded --scan-tables Big --duration 10 --warmup 2 --runs 1"
    CHURN="--preloaded --workloads insert100 --bundles 1 --tables Big:1 --on-exhaust wrap --duration 30 --warmup 2 --runs 1"
    FALL="--preloaded --yes-events --workloads rw50 --bundles 1 --tables Cell:1 --on-exhaust wrap --duration 20 --warmup 2 --runs 1"
    FIO_SECS=20
    REST_SECS=10
    CONTROL_SECS=10
    CONTROL_WARMUP=2
    # the replay's arm 40 s past a 2 s warmup, its fio 20 s from about 15 s in
    REPLAY_ARM="--preloaded --yes-events --workloads rw50 --bundles 1 --tables Cell:1 --on-exhaust wrap \
--paced Keyword --paced-workload insert100 --paced-rate 200 --duration 40 --warmup 2 --runs 1"
    REPLAY_AT=22
    REPLAY_SECS=20
fi
# the control's spec, once QUICK has had its say on its length
CONTROL="--preloaded --yes-events --workloads rw50 --bundles 1 --tables Cell:1 --on-exhaust wrap \
--paced Keyword --paced-workload insert100 --paced-rate 200 --duration $CONTROL_SECS --warmup $CONTROL_WARMUP --runs 1"

# run a script on a host, fed on stdin to `sh -s` so no login shell reads it; europa is here
on() {
    host=$1; shift
    if [ "$host" = europa ]; then printf '%s\n' "$*" | sh -s; else printf '%s\n' "$*" | ssh -o BatchMode=yes "$host" sh -s; fi
}

# append a line to the record of what was changed on the hosts
changed() {
    echo "$(date -u +%FT%TZ) $*" >> "$CHANGES"
}

die() {
    echo "m23-lab: $*" >&2
    exit 1
}

# ---------------------------------------------------------------------------------------------
# a host's state, before and after

snapshot_host() {
    sn_host=$1; sn_label=$2
    sn_dir="$SNAP/$sn_host-$sn_label"
    mkdir -p "$sn_dir"
    on "$sn_host" "systemctl list-unit-files --state=enabled --no-legend --plain | awk '{print \$1}' | sort" > "$sn_dir/units-enabled.txt"
    on "$sn_host" "systemctl list-units --all --plain --no-legend 'shoal-*' | awk '{print \$1, \$3}' | sort" > "$sn_dir/units-shoal.txt"
    on "$sn_host" "ls -d /xfs/shoal-m23* /optane/shoal-m23* /hdd/shoal-m23* /opt/shoal-deploy/m23* $NODE_DROPIN 2>/dev/null; ls /etc/sysctl.d | sort" > "$sn_dir/paths.txt"
    on "$sn_host" "cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor; systemctl is-active shoal-tmdb; \
        systemctl is-active e2scrub_all.timer fstrim.timer apt-daily.timer apt-daily-upgrade.timer; \
        cat /proc/sys/net/ipv4/tcp_slow_start_after_idle" > "$sn_dir/state.txt"
    # every NIC interrupt's cpus, which item 353's run moves and puts back
    on "$sn_host" 'for irq in $(grep -E " (enp|eno|eth)[^ ]*$" /proc/interrupts | cut -d: -f1 | tr -d " "); do
        echo "irq $irq $(cat /proc/irq/$irq/smp_affinity_list) $(cat /proc/irq/$irq/effective_affinity_list)"; done' > "$sn_dir/nic-irqs.txt"
}

cmd_snapshot() {
    for h in $HOSTS; do
        snapshot_host "$h" before
        echo "snapshot $h -> $SNAP/$h-before"
    done
}

cmd_verify() {
    vf_status=0
    for h in $HOSTS; do
        snapshot_host "$h" after
        if diff -r "$SNAP/$h-before" "$SNAP/$h-after" > "$SNAP/$h.diff"; then
            echo "$h: identical to its snapshot"
        else
            echo "$h: differs from its snapshot:"; cat "$SNAP/$h.diff"; vf_status=1
        fi
    done
    return $vf_status
}

# ---------------------------------------------------------------------------------------------
# the hosts held for a run, and put back

hold() {
    for h in $HOSTS; do
        [ -s "$LAB/governor-$h" ] || on "$h" "cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor" > "$LAB/governor-$h"
        on "$h" "sudo systemctl stop shoal-tmdb e2scrub_all.timer fstrim.timer apt-daily.timer apt-daily-upgrade.timer; \
            while systemctl show -p ActiveState --value apt-daily.service apt-daily-upgrade.service | grep -qx -e active -e activating -e deactivating; do sleep 10; done; \
            sudo cpupower frequency-set -g performance > /dev/null"
        changed "$h: shoal-tmdb, e2scrub_all.timer, fstrim.timer, apt-daily.timer and apt-daily-upgrade.timer stopped; governor performance (was $(cat "$LAB/governor-$h"))"
    done
}

restore() {
    for h in $HOSTS; do
        if [ -s "$LAB/governor-$h" ]; then
            rs_was=$(cat "$LAB/governor-$h")
            on "$h" "sudo cpupower frequency-set -g $rs_was > /dev/null"
        else
            rs_was="left as it is, since no hold recorded one"
        fi
        on "$h" "sudo systemctl start e2scrub_all.timer fstrim.timer apt-daily.timer apt-daily-upgrade.timer shoal-tmdb"
        changed "$h: governor $rs_was; e2scrub_all.timer, fstrim.timer, apt-daily.timer, apt-daily-upgrade.timer and shoal-tmdb started"
        rm -f "$LAB/governor-$h"
    done
}

# every root the cluster writes has room, and no host's own root, titan's syslog's, is near full
roots_ok() {
    for h in $HOSTS; do
        case $h in europa) ro_roots="/optane" ;; *) ro_roots="/xfs" ;; esac
        for ro_root in $ro_roots; do
            ro_used=$(on "$h" "df --output=pcent $ro_root | tail -n 1 | tr -dc '0-9'")
            [ "${ro_used:-100}" -lt 85 ] || die "$h's $ro_root is ${ro_used}% full"
        done
        ro_used=$(on "$h" "df --output=pcent / | tail -n 1 | tr -dc '0-9'")
        [ "${ro_used:-100}" -lt 96 ] || die "$h's / is ${ro_used}% full"
    done
}

# no side cluster, and no cluster the bench made, is left running on any host
no_bench_cluster() {
    for nb_host in $HOSTS; do
        nb_units=$(on "$nb_host" "systemctl list-units --all --plain --no-legend 'shoal-*-bench*' 'shoal-x16*' 'shoal-x17*' 'shoal-x18*' 'shoal-m23*' | awk '{print \$1}' | tr '\n' ' '")
        [ -z "$nb_units" ] || die "$nb_host runs ${nb_units}of a side cluster or the bench's: m23's \`destroy -i <its inventory> --yes\`, with SHOAL_DEPLOY_HOME=$LAB/state, takes it down"
    done
}

# ---------------------------------------------------------------------------------------------
# the exit's build, its cluster and its runs

build_ok() {
    [ -x "$BUILD/target/deploy/release/x16-node" ] && [ -x "$BUILD/target/deploy/release/x16" ] \
        || die "build the exit's programs in $BUILD first (\`build\`)"
    [ -z "$(git -C "$BUILD" status --porcelain --untracked-files=no)" ] || die "$BUILD has uncommitted changes"
}

# X16's schema's programs built for the Zen1 hosts in the exit's worktree, from its own target dir
cmd_build() {
    [ -d "$BUILD" ] || die "no worktree at $BUILD: \`git worktree add --detach $BUILD <commit>\` beside a glommio link at $LAB/glommio"
    [ -e "$LAB/glommio" ] || die "no glommio at $LAB/glommio: link it to the repository's sibling checkout"
    (cd "$BUILD" && CARGO_TARGET_DIR="$BUILD/target/deploy" RUSTFLAGS="-C target-cpu=znver1" \
        cargo build -q --release -p shoal-spike-scan) || die "the build failed"
    sha256sum "$BUILD/target/deploy/release/x16-node" "$BUILD/target/deploy/release/x16"
}

cmd_check() {
    build_ok
    echo "build: $(git -C "$BUILD" log --oneline -1); node $(sha256sum "$BUILD/target/deploy/release/x16-node" | cut -c1-16)"
    [ -s "$DATASET/Big.jsonl" ] && echo "dataset: $DATASET" || die "no dataset at $DATASET: \`data\` writes it"
    # the rounds' own folder holds a cell of CELL_ROWS beside X16's own Big and Keyword
    if [ "$DATASET" != "$X16_DATASET" ]; then
        ck_rows=$(wc -l < "$DATASET/Cell.jsonl")
        [ "$ck_rows" -ge "$CELL_ROWS" ] || die "$DATASET/Cell.jsonl holds $ck_rows rows, fewer than CELL_ROWS=$CELL_ROWS: \`data\` writes it again"
        for ck_table in Big Keyword; do
            [ "$DATASET/$ck_table.jsonl" -ef "$X16_DATASET/$ck_table.jsonl" ] \
                || die "$DATASET/$ck_table.jsonl is not X16's own file: \`data\` links it"
        done
        echo "cell: $ck_rows rows; Big and Keyword X16's own"
    fi
    [ -s "$CHURN_DATASET/Big.jsonl" ] && echo "churn: $CHURN_DATASET" || die "no churn dataset at $CHURN_DATASET: its README says how it was written"
}

# the rounds' dataset: a cell of CELL_ROWS written by the exit's build, beside X16's own Big and
# Keyword linked in, never copied, so the scanned tables are byte for byte the first exit's
cmd_data() {
    da_gen=$BUILD/target/deploy/release/x16-data
    [ -x "$da_gen" ] || die "build the exit's programs in $BUILD first (\`build\`)"
    [ "$DATASET" != "$X16_DATASET" ] || die "DATASET is X16's own folder, which \`data\` never writes into"
    for da_table in Big Keyword; do
        [ -s "$X16_DATASET/$da_table.jsonl" ] || die "no $da_table.jsonl in $X16_DATASET: x16-lab.sh's \`data\` writes it"
    done
    # every file renamed into place once whole, so a link to another folder's never loses its bytes
    "$da_gen" --out "$DATASET" --tables cell --cell "$CELL_ROWS" || die "the cell was not written"
    for da_table in Big Keyword; do
        ln -f "$X16_DATASET/$da_table.jsonl" "$DATASET/$da_table.jsonl" \
            || die "$da_table.jsonl could not be linked from $X16_DATASET: put DATASET on its filesystem"
    done
    printf '\n`Big.jsonl` and `Keyword.jsonl` are X16'"'"'s own files, linked from `%s` by m23-lab.sh.\n' "$X16_DATASET" >> "$DATASET/README.md"
    cat "$DATASET/README.md"
}

# the side cluster's inventory: X16's hosts and cores, every root on the SSDs, every scan setting
# the product's default, and the WAL's segments the size named if any is
inventory() {
    in_segment=${1:-}; in_path=$2
    mkdir -p "$(dirname "$in_path")"
    if [ -n "$in_segment" ]; then
        in_replication="replication:
  segment_bytes: $in_segment"
    else
        in_replication=""
    fi
    # a scan setting moved from the product's default for a trial run, as SCANS="device_reads: 16"
    if [ -n "${SCANS:-}" ]; then
        in_scans="scans:
  $SCANS"
    else
        in_scans=""
    fi
    cat > "$in_path" <<YAML
# M23's side cluster on the lab, written by m23-lab.sh: X16's schema on tmdb_cluster.yaml's hosts
# and cores, a node's memory $NODE_MEMORY, every scan setting the product's default
name: m23
server: $BUILD/target/deploy/release/x16-node
ports:
  client: 12700
  peer: 12701
  control: 12702
replication_factor: 3
control_voters: 3
$in_replication
$in_scans
groups:
  bd795i:
    storage:
      latency: /optane/shoal-m23
    resources:
      cores: 6
      memory: $NODE_MEMORY
      control_core_shared: false
    wal_commit_delay: 3ms
    lead_weight: 2
  v1000:
    storage:
      latency: /xfs/shoal-m23
    resources:
      cores: 5
      memory: $NODE_MEMORY
      control_core_shared: false
    wal_commit_delay: 2ms
tracing: Warn
user: shoal
admin: admin
nodes:
- name: europa
  address: 172.16.2.10
  group: bd795i
- name: titan
  group: v1000
- name: hyperion
  group: v1000
YAML
}

# one of the exit's admin commands, from its worktree's project, its deployments under $LAB alone
m23_cmd() {
    # shellcheck disable=SC2068
    (cd "$BUILD/shoal-spike-scan" && SHOAL_BIN_DIR="$LAB/state/bin" SHOAL_DEPLOY_HOME="$LAB/state" \
        taskset -c $DRIVER_CPUS "$BUILD/target/deploy/release/x16" "$@")
}

# one run of the cluster, driven by the exit's admin program from its worktree, which the bench
# refuses to measure if it is dirty
capture() {
    cp_label=$1; cp_flags=$2; cp_preload=$3; cp_dataset=${4:-$DATASET}
    cp_out=$LAB/captures/$cp_label
    roots_ok
    echo "== $(date -u +%FT%TZ) $cp_label ($(git -C "$BUILD" rev-parse --short HEAD))"
    # shellcheck disable=SC2086
    m23_cmd bench run -i "$LAB/inventories/m23.yml" --dataset "$cp_dataset" $cp_preload $cp_flags \
        --basic --label "$cp_label" --out "$cp_out" ${OVERWRITE:+--overwrite} || die "the capture $cp_label failed"
    # the cell's workload is its own only while every insert is a row its arm has not written; X16's
    # own cell, which the first exit's rounds ran, wraps by design
    if [ "$cp_dataset" = "$DATASET" ] && [ "$DATASET" != "$X16_DATASET" ]; then
        cell_unwrapped "$cp_out/bench.json"
    fi
    return 0
}

# refuse a capture whose cell's feed wrapped inside an arm: past a wrap the cell overwrites the rows
# its arm wrote, a workload of its own, and every later round would wrap as this one did
cell_unwrapped() {
    cu_wrapped=$(python3 -I -c 'import json, sys
capture = json.load(open(sys.argv[1]))
for arm in capture["arms"]:
    for run in arm["runs"]:
        at = ((run.get("feeds") or {}).get("Cell") or {}).get("wrapped_at")
        if at is not None:
            print(arm["id"], "run", run["run"], "at", at, "inserts")' "$1") || die "$1 could not be read"
    [ -z "$cu_wrapped" ] || die "the cell's feed wrapped in $1 ($cu_wrapped): a cell of more than CELL_ROWS=$CELL_ROWS rows is needed"
}

# every node restarted, so every row starts from its archives, and waited for: every member up with
# the control group's three voters, then a settle for its leaders to move
restart_settled() {
    echo "== $(date -u +%FT%TZ) restarting the cluster"
    m23_cmd restart -i "$LAB/inventories/m23.yml" || die "the cluster did not restart"
    rs_tries=0
    until m23_cmd status -i "$LAB/inventories/m23.yml" 2>/dev/null | grep -Eq 'voters 3 +learners [0-9]+ +up 3 '; do
        rs_tries=$((rs_tries + 1)); [ $rs_tries -lt 60 ] || die "the cluster was not up five minutes after its restart"
        sleep 5
    done
    sleep $SETTLE
}

# a node process's cpu so far on a host, in clock ticks, beside the ticks a second and the time
node_ticks() {
    on "$1" 'pid=$(pgrep -f "[x]16-node" | head -1); [ -n "$pid" ] || exit 1
        set -- $(cut -d" " -f14,15 "/proc/$pid/stat"); echo "$(( $1 + $2 )) $(getconf CLK_TCK) $(date +%s)"'
}

# every node's cpu with the cluster up and nothing driving it, over IDLE_SECS, one line a host of
# its cores busy, which a scan's cpu a partition is read above: the background no capture has
idle_sample() {
    is_out=$1
    mkdir -p "$(dirname "$is_out")"
    echo "== $(date -u +%FT%TZ) sampling the idle cluster ${IDLE_SECS}s"
    for is_host in $HOSTS; do node_ticks "$is_host" > "$LAB/run/idle-$is_host.before" || die "no node on $is_host"; done
    sleep "$IDLE_SECS"
    for is_host in $HOSTS; do node_ticks "$is_host" > "$LAB/run/idle-$is_host.after" || die "no node on $is_host"; done
    : > "$is_out"
    for is_host in $HOSTS; do
        read -r is_t0 is_hz is_s0 < "$LAB/run/idle-$is_host.before"
        read -r is_t1 _ is_s1 < "$LAB/run/idle-$is_host.after"
        echo "$is_host $is_t0 $is_t1 $is_hz $is_s0 $is_s1" | awk '{printf "%s %.4f %d\n", $1, ($3 - $2) / $4 / ($6 - $5), $6 - $5}' >> "$is_out"
    done
    cat "$is_out"
}

# the side cluster taken down, whichever way a round ends
teardown() {
    [ -n "${CURRENT:-}" ] || return 0
    echo "== $(date -u +%FT%TZ) destroying the cluster"
    m23_cmd destroy -i "$LAB/inventories/m23.yml" --yes
    CURRENT=
}

# the cluster bootstrapped fresh from an inventory
bootstrap() {
    bs_segment=${1:-}
    inventory "$bs_segment" "$LAB/inventories/m23.yml"
    roots_ok
    no_bench_cluster
    echo "== $(date -u +%FT%TZ) bootstrapping the cluster${bs_segment:+, its segments $bs_segment}"
    CURRENT=m23
    m23_cmd bootstrap -i "$LAB/inventories/m23.yml" || die "the cluster did not bootstrap"
}

# the side node's environment on every host, from NODE_ENV, in a drop-in its unit reads at its next
# start; nothing when NODE_ENV names nothing
node_env_set() {
    [ -n "$NODE_ENV" ] || return 0
    : > "$LAB/run/m23-env.conf"
    echo "[Service]" >> "$LAB/run/m23-env.conf"
    for ne_pair in $NODE_ENV; do
        case $ne_pair in *=*) ;; *) die "NODE_ENV holds $ne_pair, not KEY=value" ;; esac
        echo "Environment=$ne_pair" >> "$LAB/run/m23-env.conf"
    done
    for ne_host in $HOSTS; do
        if [ "$ne_host" = europa ]; then
            sudo mkdir -p "$NODE_DROPIN" && sudo cp "$LAB/run/m23-env.conf" "$NODE_DROPIN/m23-env.conf" && sudo systemctl daemon-reload
        else
            ssh -o BatchMode=yes "$ne_host" "sudo mkdir -p $NODE_DROPIN && sudo tee $NODE_DROPIN/m23-env.conf > /dev/null && sudo systemctl daemon-reload" < "$LAB/run/m23-env.conf"
        fi || die "$ne_host's drop-in could not be written"
    done
    NODE_ENV_SET=1
    changed "every host: $NODE_DROPIN/m23-env.conf with $NODE_ENV"
}

# the drop-in gone from every host, whichever way a round ends
node_env_gone() {
    [ -n "${NODE_ENV_SET:-}" ] || return 0
    for ne_host in $HOSTS; do
        on "$ne_host" "sudo rm -rf $NODE_DROPIN && sudo systemctl daemon-reload"
    done
    NODE_ENV_SET=
    changed "every host: $NODE_DROPIN removed"
}

# what each host's running node runs its allocator and Shoal with, its own environment from /proc,
# beside a capture: NODE_ENV's and its unit's own (item 353's purge delay)
node_env_read() {
    for ne_host in $HOSTS; do
        echo "$ne_host $(on "$ne_host" 'pid=$(pgrep -f "[x]16-node" | head -1); sudo cat /proc/$pid/environ | tr "\0" "\n" | grep -E "^MIMALLOC_|^SHOAL_" | tr "\n" " "')"
    done >> "$1"
}

# a round: its cluster bootstrapped fresh, the cell beside a scan of Big and each table scanned
# alone, then beside a scan of Keyword, and the cluster destroyed
scan_round() {
    sr_round=$1
    sr_prefix=m23-r$sr_round
    for sr_which in load big keyword ${CPU_SAMPLE:+cpu}; do
        [ -z "${OVERWRITE:-}" ] && [ -e "$LAB/captures/$sr_prefix-$sr_which" ] \
            && die "$LAB/captures/$sr_prefix-$sr_which is there already: another LAB, or OVERWRITE=1 to take it again"
    done
    # the side node's environment, from its first start (NODE_ENV)
    node_env_set
    bootstrap
    # item 353's run: the NIC's interrupt off the shards' cpus on this round's Zen1 hosts, and every
    # host's cpus sampled from here to the teardown, three hours at most
    if nic_round "$sr_round"; then nic_move; fi
    if [ -n "${CPU_SAMPLE:-}" ]; then
        mkdir -p "$LAB/captures/$sr_prefix-cpu"
        if nic_round "$sr_round"; then echo "moved to $NIC_CPU" > "$LAB/captures/$sr_prefix-cpu/nic.txt"; else echo "as it was" > "$LAB/captures/$sr_prefix-cpu/nic.txt"; fi
        cpu_samplers_start "$LAB/captures/$sr_prefix-cpu" $(($(date +%s) + 10800))
    fi
    capture "$sr_prefix-load" "$LOAD" "$PRELOAD"
    restart_settled
    node_env_read "$LAB/captures/$sr_prefix-env.txt"
    capture "$sr_prefix-big" "$COMMON $TIMES $BIG" "$PRELOAD"
    restart_settled
    node_env_read "$LAB/captures/$sr_prefix-env.txt"
    capture "$sr_prefix-keyword" "$COMMON $TIMES $KEYWORD" "$PRELOAD"
    if [ -n "${CPU_SAMPLE:-}" ]; then
        samplers_stop
        gzip -f "$LAB/captures/$sr_prefix-cpu"/samples-*.log
    fi
    teardown
    node_env_gone
    nic_restore
}

# a control round: its cluster left as a scan round leaves it by its Big capture, then the cell alone
# where the round's Keyword capture would scan, as long as one of its arms, and the cluster destroyed
control_round() {
    cl_round=$1
    cl_prefix=m23-control-r$cl_round
    for cl_which in load big cell; do
        [ -z "${OVERWRITE:-}" ] && [ -e "$LAB/captures/$cl_prefix-$cl_which" ] \
            && die "$LAB/captures/$cl_prefix-$cl_which is there already: another LAB, or OVERWRITE=1 to take it again"
    done
    # the side node's environment, as a scan round's (NODE_ENV)
    node_env_set
    bootstrap
    capture "$cl_prefix-load" "$LOAD" "$PRELOAD"
    restart_settled
    node_env_read "$LAB/captures/$cl_prefix-env.txt"
    capture "$cl_prefix-big" "$COMMON $TIMES $BIG" "$PRELOAD"
    restart_settled
    node_env_read "$LAB/captures/$cl_prefix-env.txt"
    capture "$cl_prefix-cell" "$CONTROL" "$PRELOAD"
    teardown
    node_env_gone
}

# the churn at one segment size: Big loaded and scanned alone, an hour of inserts and overwrites into
# it, a restart, and Big scanned alone again
churn_round() {
    cr_segment=$1
    cr_prefix=m23-churn-$(echo "$cr_segment" | tr 'A-Z' 'a-z')
    for cr_which in load scan churn afterchurn; do
        [ -z "${OVERWRITE:-}" ] && [ -e "$LAB/captures/$cr_prefix-$cr_which" ] \
            && die "$LAB/captures/$cr_prefix-$cr_which is there already: another LAB, or OVERWRITE=1 to take it again"
    done
    bootstrap "$cr_segment"
    capture "$cr_prefix-load" "$LOAD" "$CHURN_LOAD_PRELOAD"
    restart_settled
    # each scan alone beside its cluster's idle cpu, sampled after the same restart and settle
    idle_sample "$LAB/captures/$cr_prefix-idle/idle.txt"
    capture "$cr_prefix-scan" "$SCAN_ALONE" "$CHURN_LOAD_PRELOAD"
    capture "$cr_prefix-churn" "$CHURN" "$CHURN_PRELOAD" "$CHURN_DATASET"
    restart_settled
    idle_sample "$LAB/captures/$cr_prefix-afteridle/idle.txt"
    capture "$cr_prefix-afterchurn" "$SCAN_ALONE" "$CHURN_LOAD_PRELOAD"
    teardown
}

# a process and every process under it, deepest first, so a backgrounded function's program goes
# with the shell it ran in
kill_tree() {
    for kt_child in $(pgrep -P "$1"); do kill_tree "$kt_child"; done
    kill "$1" 2>/dev/null
}

# the per-second sampler started on every host into a directory, each ending at a deadline or
# once its output closes, pinned to a cpu where one is named (item 359's run pins it clear of every
# shard; item 351's ran it unpinned); SAMPLERS keeps the local processes that carry them
samplers_start() {
    sa_dir=$1; sa_deadline=$2; sa_cpu=${3:-}
    mkdir -p "$sa_dir"
    SAMPLERS=""
    # the command each host runs the sampler under: taskset where a cpu is named
    if [ -n "$sa_cpu" ]; then sa_pin="taskset -c $sa_cpu"; else sa_pin=""; fi
    for sa_host in $HOSTS; do
        if [ "$sa_host" = europa ]; then
            $sa_pin sh "$SAMPLER" "$sa_deadline" > "$sa_dir/samples-$sa_host.log" &
        else
            # shellcheck disable=SC2086
            ssh -o BatchMode=yes "$sa_host" $sa_pin sh -s -- "$sa_deadline" < "$SAMPLER" > "$sa_dir/samples-$sa_host.log" &
        fi
        SAMPLERS="$SAMPLERS $!"
    done
}

# every sampler stopped: closing a remote one's ssh closes its output, and its next line ends it
samplers_stop() {
    for sp_pid in $SAMPLERS; do kill_tree "$sp_pid"; done
    for sp_pid in $SAMPLERS; do wait "$sp_pid" 2>/dev/null; done
    SAMPLERS=""
}

# item 353's sampler started on every host, pinned to SAMPLER_CPU, into a directory, each ending at
# a deadline or once its output closes; it joins SAMPLERS, which samplers_stop ends
cpu_samplers_start() {
    cs_dir=$1; cs_deadline=$2
    mkdir -p "$cs_dir"
    for cs_host in $HOSTS; do
        if [ "$cs_host" = europa ]; then
            taskset -c "$SAMPLER_CPU" sh "$CPU_SAMPLER" "$cs_deadline" > "$cs_dir/samples-$cs_host.log" &
        else
            ssh -o BatchMode=yes "$cs_host" taskset -c "$SAMPLER_CPU" sh -s -- "$cs_deadline" < "$CPU_SAMPLER" \
                > "$cs_dir/samples-$cs_host.log" &
        fi
        SAMPLERS="$SAMPLERS $!"
    done
}

# whether a round is one whose NIC interrupts move off the shards' cpus
nic_round() {
    case " $NIC_ROUNDS " in *" $1 "*) return 0 ;; esac
    return 1
}

# the interrupt of the NIC a host reaches europa by: a single queue's on the Zen1 hosts
nic_irq() {
    on "$1" 'dev=$(ip -o route get 172.16.2.10 | sed -n "s/.* dev \([^ ]*\).*/\1/p")
        [ -n "$dev" ] || exit 1
        grep -E " $dev\$" /proc/interrupts | head -n 1 | cut -d: -f1 | tr -d " "'
}

# every Zen1 host's NIC interrupt moved to NIC_CPU, once no shard of its node runs there, each move
# recorded and kept under $LAB so nic_restore puts it back however the run ends
nic_move() {
    for nm_host in $NIC_HOSTS; do
        # a move an earlier run never put back would be recorded as the host's own setting
        [ ! -e "$LAB/nic-$nm_host" ] || die "$LAB/nic-$nm_host holds a move never put back: put it back by hand first"
        nm_irq=$(nic_irq "$nm_host")
        [ -n "$nm_irq" ] || die "no NIC interrupt found on $nm_host"
        # a shard's thread (glommio's unnamed-N) on the cpu would take every interrupt this moves
        nm_on=$(on "$nm_host" "pid=\$(pgrep -f '[x]16-node' | head -n 1); [ -n \"\$pid\" ] || exit 0
            ps -L -o psr=,comm= -p \$pid | awk '\$1 == $NIC_CPU && \$2 ~ /^unnamed-/' | head -n 3")
        [ -z "$nm_on" ] || die "$nm_host's node runs a shard on cpu $NIC_CPU: $nm_on"
        nm_was=$(on "$nm_host" "cat /proc/irq/$nm_irq/smp_affinity_list /proc/irq/$nm_irq/effective_affinity_list | tr '\n' ' '")
        # kept before the write, so a write that fails half way is still put back
        echo "$nm_irq $nm_was" > "$LAB/nic-$nm_host"
        on "$nm_host" "echo $NIC_CPU | sudo tee /proc/irq/$nm_irq/smp_affinity_list > /dev/null" \
            || die "$nm_host refused to move interrupt $nm_irq"
        nm_now=$(on "$nm_host" "cat /proc/irq/$nm_irq/effective_affinity_list")
        changed "$nm_host: interrupt $nm_irq, its NIC's, moved to cpu $NIC_CPU (list and effective were $nm_was; effective now $nm_now)"
        [ "$nm_now" = "$NIC_CPU" ] || die "$nm_host's interrupt $nm_irq went to cpu $nm_now, not $NIC_CPU"
    done
}

# every NIC interrupt nic_move moved put back as it was: its old effective cpu first, then its old
# list, which keeps that cpu since it lies inside the list; said loudly, never fatal, since a trap
# runs it
nic_restore() {
    for nr_host in $NIC_HOSTS; do
        [ -s "$LAB/nic-$nr_host" ] || continue
        read -r nr_irq nr_list nr_eff < "$LAB/nic-$nr_host"
        on "$nr_host" "echo $nr_eff | sudo tee /proc/irq/$nr_irq/smp_affinity_list > /dev/null; \
            echo $nr_list | sudo tee /proc/irq/$nr_irq/smp_affinity_list > /dev/null"
        nr_now=$(on "$nr_host" "cat /proc/irq/$nr_irq/smp_affinity_list /proc/irq/$nr_irq/effective_affinity_list | tr '\n' ' '")
        changed "$nr_host: interrupt $nr_irq put back, list and effective now $nr_now(were $nr_list $nr_eff)"
        if [ "$nr_now" = "$nr_list $nr_eff " ]; then
            rm -f "$LAB/nic-$nr_host"
        else
            echo "m23-lab: $nr_host's interrupt $nr_irq reads $nr_now, not $nr_list $nr_eff: put it back by hand" >&2
        fi
    done
}

# item 357's run: the rounds' own preload on a fresh cluster, which every round of the exit begins with,
# and the cluster destroyed; the capture keeps what every member shed by cause and what the preload
# sent again by code (preload-sheds-waited-out.md), which m23-preload-report.py reads
load_round() {
    lr_name=m23-load-r$1
    [ -z "${OVERWRITE:-}" ] && [ -e "$LAB/captures/$lr_name" ] \
        && die "$LAB/captures/$lr_name is there already: another LAB, or OVERWRITE=1 to take it again"
    node_env_set
    bootstrap
    capture "$lr_name" "$LOAD" "$PRELOAD"
    teardown
    node_env_gone
}

# item 355's run: a scan round without its Big capture, its captures named as a round's are, so `fetch`
# and every report read them as they read the rounds': the preload, a restart, and the cell beside a full
# scan of Keyword, where a scan's tablet was given up (scan-pages-served-in-turn.md)
keyword_round() {
    kr_prefix=m23-r$1
    for kr_which in load keyword; do
        [ -z "${OVERWRITE:-}" ] && [ -e "$LAB/captures/$kr_prefix-$kr_which" ] \
            && die "$LAB/captures/$kr_prefix-$kr_which is there already: another LAB, or OVERWRITE=1 to take it again"
    done
    node_env_set
    bootstrap
    capture "$kr_prefix-load" "$LOAD" "$PRELOAD"
    restart_settled
    node_env_read "$LAB/captures/$kr_prefix-env.txt"
    capture "$kr_prefix-keyword" "$COMMON $TIMES $KEYWORD" "$PRELOAD"
    teardown
    node_env_gone
}

# whether a round is one bootstrapped with SCANS_ALT's scan settings (item 359's run)
alt_round() {
    case " $ALT_ROUNDS " in *" $1 "*) return 0 ;; esac
    return 1
}

# every host's whole disk under the root its node's tables are on, a line a host, so a report reads
# the device the cell's reads and the scan's share
root_disks() {
    : > "$1"
    for rd_host in $HOSTS; do
        case $rd_host in europa) rd_root=/optane ;; *) rd_root=/xfs ;; esac
        rd_disk=$(on "$rd_host" "lsblk -s -n -o NAME,TYPE \"\$(findmnt -n -o SOURCE $rd_root)\" | awk '\$2 == \"disk\" { gsub(/[^a-z0-9]/, \"\", \$1); print \$1 }'")
        [ -n "$rd_disk" ] || die "no whole disk under $rd_host's $rd_root"
        echo "$rd_host $rd_root $rd_disk" >> "$1"
    done
}

# item 359's run: a scan round without its Keyword capture, named as a round's are, its Big capture
# beside every host's devices and node threads sampled each second, pinned clear of every shard, and
# every member's own figures read each second, so the cell's slow reads beside a scan of Big are read
# by the member that served them, its device and its shards; a round in ALT_ROUNDS is bootstrapped
# with SCANS_ALT's scan settings, and every round's are kept beside its samples
big_round() {
    br_prefix=m23-r$1
    for br_which in load big samples; do
        [ -z "${OVERWRITE:-}" ] && [ -e "$LAB/captures/$br_prefix-$br_which" ] \
            && die "$LAB/captures/$br_prefix-$br_which is there already: another LAB, or OVERWRITE=1 to take it again"
    done
    br_dir=$LAB/captures/$br_prefix-samples
    mkdir -p "$br_dir"
    # this round's scan settings, SCANS_ALT's in an alternate round, written into its inventory
    br_saved=${SCANS:-}
    br_common=$COMMON
    if alt_round "$1"; then
        SCANS=$SCANS_ALT
        # the scan's event cut at a whole scan's length, in place of the twenty minutes it may take
        if [ -n "$ALT_EVENT_TIMEOUT" ]; then
            br_common=$(printf '%s' "$COMMON" | sed "s/--event-timeout [0-9]*/--event-timeout $ALT_EVENT_TIMEOUT/")
            printf '%s\n' "$br_common" | grep -q -- "--event-timeout $ALT_EVENT_TIMEOUT" || die "COMMON carries no event timeout to cut"
        fi
    fi
    if [ "$br_common" = "$COMMON" ]; then
        printf '%s\n' "${SCANS:-the product's defaults}" > "$br_dir/scans.txt"
    else
        printf '%s, its event cut at %s s\n' "${SCANS:-the product's defaults}" "$ALT_EVENT_TIMEOUT" > "$br_dir/scans.txt"
    fi
    node_env_set
    bootstrap
    SCANS=$br_saved
    cp "$LAB/inventories/m23.yml" "$br_dir/inventory.yml"
    capture "$br_prefix-load" "$LOAD" "$PRELOAD"
    restart_settled
    node_env_read "$LAB/captures/$br_prefix-env.txt"
    root_disks "$br_dir/disks.txt"
    # every host each second and every member's figures, an hour at most, from here to the capture's end
    samplers_start "$br_dir" $(($(date +%s) + 3600)) "$SAMPLER_CPU"
    m23_cmd stats -i "$LAB/inventories/m23.yml" --json --watch 1 > "$br_dir/stats.json" 2> "$br_dir/stats.err" &
    # kept with the samplers, so a way out that stops them stops it too
    SAMPLERS="$SAMPLERS $!"
    capture "$br_prefix-big" "$br_common $TIMES $BIG" "$PRELOAD"
    samplers_stop
    gzip -f "$br_dir"/samples-*.log "$br_dir/stats.json"
    teardown
    node_env_gone
}

# whether a replay round is a control, its fio's times kept and no fio run (item 359's replay)
replay_off_round() {
    case " $REPLAY_OFF_ROUNDS " in *" $1 "*) return 0 ;; esac
    return 1
}

# the replay's file on every replay host, in a directory of its own made for it, written whole and
# synced once before the first round, the directory recorded so every way out removes it
replay_setup() {
    for rs_host in $REPLAY_HOSTS; do
        on "$rs_host" "mkdir $REPLAY_DIR" || die "$rs_host's $REPLAY_DIR could not be made: is a run's left there?"
        FALL_MADE="$FALL_MADE $rs_host:$REPLAY_DIR"
        changed "$rs_host: $REPLAY_DIR made for item 359's replay"
        on "$rs_host" "fio --name=replay-file --filename=$REPLAY_DIR/replay.dat --rw=write --bs=1M --size=$REPLAY_FILE \
            --direct=1 --ioengine=psync --end_fsync=1 --output-format=terse > /dev/null" || die "$rs_host's replay file was not written"
    done
}

# every replay host's fio started in the background REPLAY_AT seconds from now, reading its file as a
# whole scan of Big read the device, or, in a control round, the same seconds waited out with no fio;
# each host's start and end, by this host's clock, kept beside the samples; REPLAYS keeps the
# processes, which the caller waits for
replay_start() {
    rp_dir=$1; rp_on=$2
    REPLAYS=""
    for rp_host in $REPLAY_HOSTS; do
        (
            sleep "$REPLAY_AT"
            echo "start $(date +%s.%N)"
            if [ "$rp_on" = on ]; then
                # pinned with the sampler, clear of every shard; its output is fio's own figures
                ssh -o BatchMode=yes "$rp_host" "taskset -c $SAMPLER_CPU fio --name=replay --filename=$REPLAY_DIR/replay.dat \
                    --rw=randread --bssplit=$REPLAY_BSSPLIT --blockalign=4k --direct=1 --ioengine=io_uring --iodepth=16 \
                    --rate_iops=$REPLAY_IOPS --rate_process=poisson --time_based --runtime=$REPLAY_SECS \
                    --output-format=json" > "$rp_dir/fio-$rp_host.json"
            else
                sleep "$REPLAY_SECS"
            fi
            echo "end $(date +%s.%N)"
        ) > "$rp_dir/replay-$rp_host.txt" 2>&1 &
        REPLAYS="$REPLAYS $!"
    done
}

# item 359's replay: a round's preload, the Big capture's arms before its scan arm, then the cell alone
# with the scan's reads replayed on the Zen1 hosts' 970 EVOs by fio from about where a scan is asked,
# or with nothing beside it in a control round, every host's devices and node threads sampled each
# second and every member's own figures read each second over that arm
replay_round() {
    rr_prefix=m23-r$1
    for rr_which in load lead replay samples; do
        [ -z "${OVERWRITE:-}" ] && [ -e "$LAB/captures/$rr_prefix-$rr_which" ] \
            && die "$LAB/captures/$rr_prefix-$rr_which is there already: another LAB, or OVERWRITE=1 to take it again"
    done
    rr_dir=$LAB/captures/$rr_prefix-samples
    mkdir -p "$rr_dir"
    # the round's side, which the report groups it by
    if replay_off_round "$1"; then rr_on=off; else rr_on=on; fi
    if [ "$rr_on" = on ]; then echo "fio replay" > "$rr_dir/side.txt"; else echo "no replay" > "$rr_dir/side.txt"; fi
    printf '%s\n' "${SCANS:-the product's defaults}" > "$rr_dir/scans.txt"
    echo "$REPLAY_IOPS reads a second, $REPLAY_BSSPLIT, Poisson, 16 deep, ${REPLAY_SECS} s, ${REPLAY_AT} s after the arm's capture began" \
        > "$rr_dir/replay.txt"
    node_env_set
    bootstrap
    cp "$LAB/inventories/m23.yml" "$rr_dir/inventory.yml"
    capture "$rr_prefix-load" "$LOAD" "$PRELOAD"
    restart_settled
    node_env_read "$LAB/captures/$rr_prefix-env.txt"
    root_disks "$rr_dir/disks.txt"
    capture "$rr_prefix-lead" "$COMMON $TIMES $LEAD" "$PRELOAD"
    # every host each second and every member's figures over the replay's arm, half an hour at most
    samplers_start "$rr_dir" $(($(date +%s) + 1800)) "$SAMPLER_CPU"
    m23_cmd stats -i "$LAB/inventories/m23.yml" --json --watch 1 > "$rr_dir/stats.json" 2> "$rr_dir/stats.err" &
    SAMPLERS="$SAMPLERS $!"
    replay_start "$rr_dir" "$rr_on"
    # kept with the samplers too, so a way out that stops them stops these
    SAMPLERS="$SAMPLERS $REPLAYS"
    capture "$rr_prefix-replay" "$REPLAY_ARM" "$PRELOAD"
    for rr_pid in $REPLAYS; do wait "$rr_pid"; done
    samplers_stop
    gzip -f "$rr_dir"/samples-*.log "$rr_dir/stats.json"
    teardown
    node_env_gone
}

# item 351's run: the cell alone on a fresh cluster for two arms a rest apart, every host's devices,
# cpu and node threads sampled each second and every member's WAL and compactor figures read beside
# them, then the cluster destroyed; then the same flushes alone through fio (fall_fio)
fall_round() {
    for fr_which in load a b cluster fio; do
        [ -z "${OVERWRITE:-}" ] && [ -e "$LAB/captures/m23-fall-$fr_which" ] \
            && die "$LAB/captures/m23-fall-$fr_which is there already: another LAB, or OVERWRITE=1 to take it again"
    done
    [ -s "$SAMPLER" ] || die "no sampler at $SAMPLER"
    fr_dir=$LAB/captures/m23-fall-cluster
    bootstrap
    capture m23-fall-load "$LOAD" "$PRELOAD"
    restart_settled
    # the journals read from here, for any forced purge or long job the nodes logged
    fr_since=$(date -u '+%Y-%m-%d %H:%M:%S')
    # every sample until well past both arms and the rest between them
    samplers_start "$fr_dir" $(($(date +%s) + 2 * (FIO_SECS + 120) + REST_SECS + 600))
    m23_cmd stats -i "$LAB/inventories/m23.yml" --json --watch 1 > "$fr_dir/stats.json" 2> "$fr_dir/stats.err" &
    fr_stats=$!
    echo "== $(date -u +%FT%TZ) sampling every host each second into $fr_dir"
    capture m23-fall-a "$FALL" "$PRELOAD"
    echo "== $(date -u +%FT%TZ) resting ${REST_SECS}s"
    sleep "$REST_SECS"
    capture m23-fall-b "$FALL" "$PRELOAD"
    kill_tree "$fr_stats"
    samplers_stop
    for fr_host in $HOSTS; do
        on "$fr_host" "journalctl --utc --since '$fr_since' --no-pager | grep -F x16-node" > "$fr_dir/journal-$fr_host.txt"
    done
    teardown
    fall_fio
}

# each host's flushes a second and bytes a flush over the first arm's first fifty measured seconds,
# from its samples: `<syncs a second> <bytes a sync>`, the bytes rounded up to 4 KiB. A device whose
# cache writes through, europa's Optane, is sent no flush at all, so its writes a second stand in
fall_rate() {
    python3 -I -c 'import json, sys, datetime
samples, capture, host = sys.argv[1:4]
run = json.load(open(capture))["arms"][0]["runs"][0]
start = datetime.datetime.fromisoformat(run["started_at"].replace("Z", "+00:00")).timestamp() + 10
end = start + 50
# the whole NVMe under the latency root, named by the caller
device = sys.argv[4]
points = []
at = None
for line in open(samples):
    if line.startswith("@ "):
        at = float(line.split()[1])
        continue
    fields = line.split()
    if at is not None and len(fields) > 19 and fields[2] == device and start <= at <= end:
        points.append((at, int(fields[9]), int(fields[18]), int(fields[7])))
if len(points) < 2:
    sys.exit("too few samples of %s on %s" % (device, host))
(t0, s0, f0, w0), (t1, s1, f1, w1) = points[0], points[-1]
flushes = (f1 - f0) / (t1 - t0)
if flushes < 10:
    flushes = (w1 - w0) / (t1 - t0)
written = (s1 - s0) * 512 / (t1 - t0)
size = -(-int(written / max(flushes, 1)) // 4096) * 4096
print("%d %d" % (max(int(flushes), 1), max(size, 4096)))' "$@"
}

# the same flushes with no Shoal unit up: fio on every host at once, in a directory of its own on
# the node's filesystem, as many jobs as the node had shards, each writing and syncing at its share
# of the first arm's flushes a second, twice the rest apart, sampled as the cluster was
fall_fio() {
    ff_dir=$LAB/captures/m23-fall-fio
    mkdir -p "$ff_dir"
    no_bench_cluster
    for ff_host in $HOSTS; do
        case $ff_host in europa) ff_root=/optane; ff_jobs=6 ;; *) ff_root=/xfs; ff_jobs=5 ;; esac
        ff_disk=$(on "$ff_host" "lsblk -s -n -o NAME,TYPE \"\$(findmnt -n -o SOURCE $ff_root)\" | awk '\$2 == \"disk\" { gsub(/[^a-z0-9]/, \"\", \$1); print \$1 }'")
        [ -n "$ff_disk" ] || die "no whole disk under $ff_host's $ff_root"
        ff_rate=$(fall_rate "$LAB/captures/m23-fall-cluster/samples-$ff_host.log" "$LAB/captures/m23-fall-a/bench.json" "$ff_host" "$ff_disk") \
            || die "no rate for $ff_host"
        set -- $ff_rate
        echo "$ff_host $ff_root $ff_disk $ff_jobs $1 $2" >> "$ff_dir/rates.txt"
        on "$ff_host" "mkdir $ff_root/shoal-m23-fall" || die "$ff_host's $ff_root/shoal-m23-fall could not be made"
        FALL_MADE="$FALL_MADE $ff_host:$ff_root/shoal-m23-fall"
        changed "$ff_host: $ff_root/shoal-m23-fall made for item 351's fio run"
    done
    cat "$ff_dir/rates.txt"
    samplers_start "$ff_dir" $(($(date +%s) + 2 * (FIO_SECS + 60) + REST_SECS + 120))
    for ff_leg in a b; do
        echo "== $(date -u +%FT%TZ) fio $ff_leg, ${FIO_SECS}s"
        echo "$ff_leg start $(date +%s)" >> "$ff_dir/legs.txt"
        # every host's fio at once, each a background job of its own, waited for by its pid
        ff_pids=""
        while read -r ff_host ff_root ff_disk ff_jobs ff_flushes ff_size; do
            ff_each=$(( (ff_flushes + ff_jobs - 1) / ff_jobs ))
            on "$ff_host" "cd $ff_root/shoal-m23-fall && fio --name=fall --directory=$ff_root/shoal-m23-fall \
                --rw=write --bs=$ff_size --direct=1 --ioengine=psync --fdatasync=1 --rate_iops=$ff_each \
                --numjobs=$ff_jobs --size=512m --time_based --runtime=$FIO_SECS --group_reporting \
                --output-format=json" > "$ff_dir/fio-$ff_host-$ff_leg.json" &
            ff_pids="$ff_pids $!"
        done < "$ff_dir/rates.txt"
        for ff_pid in $ff_pids; do
            wait "$ff_pid" || die "fio $ff_leg failed on a host"
        done
        echo "$ff_leg end $(date +%s)" >> "$ff_dir/legs.txt"
        [ "$ff_leg" = b ] || { echo "== $(date -u +%FT%TZ) resting ${REST_SECS}s"; sleep "$REST_SECS"; }
    done
    samplers_stop
    fall_clean
}

# every directory fio's runs made removed, item 351's and item 359's replay's, on its way out or on any other
fall_clean() {
    for fc_made in ${FALL_MADE:-}; do
        fc_host=${fc_made%%:*}; fc_path=${fc_made#*:}
        # the only paths this removes are the runs' own, beside a root the cluster's directories are in
        case $fc_path in /xfs/shoal-m23-fall | /optane/shoal-m23-fall | "$REPLAY_DIR") ;; *) continue ;; esac
        on "$fc_host" "rm -rf $fc_path"
        changed "$fc_host: $fc_path removed"
    done
    FALL_MADE=""
}

# a side's worktree, by its name
drivers_tree() {
    case $1 in before) echo "$DRIVERS_BEFORE" ;; after) echo "$DRIVERS_AFTER" ;; esac
}

drivers_ok() {
    for do_side in before after; do
        do_tree=$(drivers_tree "$do_side")
        [ -x "$do_tree/target/deploy/release/tmdb-dataset-node" ] && [ -x "$do_tree/target/deploy/release/tmdb-dataset-loader" ] \
            || die "build the tmdb programs in $do_tree first (\`build-drivers\`)"
        [ -z "$(git -C "$do_tree" status --porcelain --untracked-files=no)" ] || die "$do_tree has uncommitted changes"
    done
}

# the tmdb programs built for the Zen1 hosts in each side's worktree, one after the other: glommio's
# build script configures liburing in the shared checkout, and two builds at once corrupt it
cmd_build_drivers() {
    for bd_side in before after; do
        bd_tree=$(drivers_tree "$bd_side")
        [ -d "$bd_tree" ] || die "no worktree at $bd_tree"
        (cd "$bd_tree" && CARGO_TARGET_DIR="$bd_tree/target/deploy" RUSTFLAGS="-C target-cpu=znver1" \
            cargo build -q --release -p tmdb-dataset) || die "the $bd_side side's build failed"
        echo "$bd_side: $(git -C "$bd_tree" log --oneline -1)"
        sha256sum "$bd_tree/target/deploy/release/tmdb-dataset-node" "$bd_tree/target/deploy/release/tmdb-dataset-loader"
    done
}

# one side's run of the drivers: the bench's own copy of tmdb_cluster.yaml, its node the side's
drivers_capture() {
    dc_side=$1; dc_round=$2
    dc_tree=$(drivers_tree "$dc_side")
    dc_label=m23-drivers-$dc_side-r$dc_round
    dc_out=$LAB/captures/$dc_label
    [ -z "${OVERWRITE:-}" ] && [ -e "$dc_out" ] && die "$dc_out is there already: another LAB, or OVERWRITE=1 to take it again"
    mkdir -p "$LAB/inventories"
    sed "s|^server: .*|server: $dc_tree/target/deploy/release/tmdb-dataset-node|" "$REPO/tmdb_cluster.yaml" > "$LAB/inventories/tmdb-$dc_side.yml"
    roots_ok
    no_bench_cluster
    echo "== $(date -u +%FT%TZ) $dc_label ($(git -C "$dc_tree" rev-parse --short HEAD))"
    # shellcheck disable=SC2086
    (cd "$dc_tree/examples/tmdb_dataset" && SHOAL_BIN_DIR="$LAB/state-drivers/bin" SHOAL_DEPLOY_HOME="$LAB/state-drivers" \
        taskset -c $DRIVER_CPUS "$dc_tree/target/deploy/release/tmdb-dataset-loader" bench run \
        -i "$LAB/inventories/tmdb-$dc_side.yml" --dataset "$OBJECTS" $DRIVERS_SPEC \
        --basic --label "$dc_label" --out "$dc_out" ${OVERWRITE:+--overwrite}) || die "the capture $dc_label failed"
}

cmd_run() {
    [ -s "$DATASET/Big.jsonl" ] || die "no dataset at $DATASET"
    build_ok
    [ -n "${QUICK:-}" ] && ROUNDS=1
    mkdir -p "$LAB/captures" "$LAB/inventories" "$(dirname "$CHANGES")"
    no_bench_cluster
    hold
    CURRENT=
    FALL_MADE=""
    SAMPLERS=""
    trap 'samplers_stop; fall_clean; teardown; node_env_gone; nic_restore; restore' EXIT
    trap 'exit 143' INT TERM
    if [ -n "${DRIVERS_ONLY:-}" ]; then
        drivers_ok
        [ -n "${QUICK:-}" ] && DRIVER_ROUNDS=1
        rn_round=1
        while [ "$rn_round" -le "$DRIVER_ROUNDS" ]; do
            # before first in odd rounds, after first in even ones
            if [ $((rn_round % 2)) -eq 1 ]; then rn_order="before after"; else rn_order="after before"; fi
            for rn_side in $rn_order; do drivers_capture "$rn_side" "$rn_round"; done
            rn_round=$((rn_round + 1))
        done
    elif [ -n "${CHURN_ONLY:-}" ]; then
        [ -s "$CHURN_DATASET/Big.jsonl" ] || die "no churn dataset at $CHURN_DATASET"
        if [ -n "${QUICK:-}" ]; then SEGMENTS=$(echo "$SEGMENTS" | awk '{print $1}'); fi
        for rn_segment in $SEGMENTS; do churn_round "$rn_segment"; done
    elif [ -n "${FALL_ONLY:-}" ]; then
        fall_round
    elif [ -n "${KEYWORD_ONLY:-}" ]; then
        [ -n "${QUICK:-}" ] && KEYWORD_ROUNDS=1
        rn_round=1
        while [ "$rn_round" -le "$KEYWORD_ROUNDS" ]; do
            keyword_round "$rn_round"
            rn_round=$((rn_round + 1))
        done
    elif [ -n "${BIG_ONLY:-}" ]; then
        # an alternate round with nothing to alternate to would be the default round by another name
        [ -z "$ALT_ROUNDS" ] || [ -n "$SCANS_ALT" ] || die "ALT_ROUNDS names rounds but SCANS_ALT names no scan setting"
        [ -s "$SAMPLER" ] || die "no sampler at $SAMPLER"
        rn_round=1
        while [ "$rn_round" -le "$BIG_ROUNDS" ]; do
            big_round "$rn_round"
            rn_round=$((rn_round + 1))
        done
    elif [ -n "${REPLAY_ONLY:-}" ]; then
        [ -s "$SAMPLER" ] || die "no sampler at $SAMPLER"
        # every replay host free of the run's own directory and with fio, before anything is made
        for rn_host in $REPLAY_HOSTS; do
            on "$rn_host" "command -v fio > /dev/null && [ ! -e $REPLAY_DIR ]" || die "$rn_host has no fio or has $REPLAY_DIR already"
        done
        [ -n "${QUICK:-}" ] && REPLAY_ROUNDS=2
        replay_setup
        rn_round=1
        while [ "$rn_round" -le "$REPLAY_ROUNDS" ]; do
            replay_round "$rn_round"
            rn_round=$((rn_round + 1))
        done
        fall_clean
    elif [ -n "${LOAD_ONLY:-}" ]; then
        [ -n "${QUICK:-}" ] && LOAD_ROUNDS=1
        rn_round=1
        while [ "$rn_round" -le "$LOAD_ROUNDS" ]; do
            load_round "$rn_round"
            rn_round=$((rn_round + 1))
        done
    elif [ -n "${CONTROL_ONLY:-}" ]; then
        [ -n "${QUICK:-}" ] && CONTROL_ROUNDS=1
        rn_round=1
        while [ "$rn_round" -le "$CONTROL_ROUNDS" ]; do
            control_round "$rn_round"
            rn_round=$((rn_round + 1))
        done
    else
        [ -z "$CONTROL_EVERY" ] || [ "$CONTROL_EVERY" -gt 0 ] 2>/dev/null || die "CONTROL_EVERY is $CONTROL_EVERY, not a count of rounds"
        rn_round=1
        rn_control=1
        while [ "$rn_round" -le "$ROUNDS" ]; do
            scan_round "$rn_round"
            # a control among the rounds, after every CONTROL_EVERY of them, numbered in turn
            if [ -n "$CONTROL_EVERY" ] && [ $((rn_round % CONTROL_EVERY)) -eq 0 ]; then
                control_round "$rn_control"
                rn_control=$((rn_control + 1))
            fi
            rn_round=$((rn_round + 1))
        done
    fi
    echo "== $(date -u +%FT%TZ) every round done"
}

cmd_start() {
    mkdir -p "$LAB/run"
    cp "$0" "$LAB/run/m23-lab.sh"
    cp "$SAMPLER" "$LAB/run/m23-fall-sample.sh"
    cp "$CPU_SAMPLER" "$LAB/run/m23-cpu-sample.sh"
    # refusing a manual stop or restart, as the stores legs' units do (Resolved #306), with a user
    # unit's open files raised past its soft 1024
    sudo systemd-run --unit=m23-lab --collect -p User="$(id -un)" -p Group="$(id -gn)" \
        -p KillMode=mixed -p TimeoutStopSec=600 -p RefuseManualStop=yes -p LimitNOFILE=524288 \
        -p WorkingDirectory="$REPO" --setenv=HOME="$HOME" --setenv=PATH="$PATH" \
        --setenv=LAB="$LAB" --setenv=OUT="$OUT" --setenv=BUILD="$BUILD" --setenv=ROUNDS="$ROUNDS" \
        --setenv=DATASET="$DATASET" --setenv=CHURN_DATASET="$CHURN_DATASET" --setenv=SEGMENTS="$SEGMENTS" \
        --setenv=X16_DATASET="$X16_DATASET" --setenv=CELL_ROWS="$CELL_ROWS" --setenv=NODE_MEMORY="$NODE_MEMORY" \
        --setenv=QUICK="${QUICK:-}" --setenv=OVERWRITE="${OVERWRITE:-}" --setenv=CHURN_ONLY="${CHURN_ONLY:-}" --setenv=SCANS="${SCANS:-}" \
        --setenv=CONTROL_ONLY="${CONTROL_ONLY:-}" --setenv=CONTROL_ROUNDS="$CONTROL_ROUNDS" \
        --setenv=CONTROL_EVERY="$CONTROL_EVERY" --setenv=CONTROL_SECS="$CONTROL_SECS" \
        --setenv=FALL_ONLY="${FALL_ONLY:-}" --setenv=REST_SECS="$REST_SECS" \
        --setenv=LOAD_ONLY="${LOAD_ONLY:-}" --setenv=LOAD_ROUNDS="$LOAD_ROUNDS" \
        --setenv=KEYWORD_ONLY="${KEYWORD_ONLY:-}" --setenv=KEYWORD_ROUNDS="$KEYWORD_ROUNDS" \
        --setenv=BIG_ONLY="${BIG_ONLY:-}" --setenv=BIG_ROUNDS="$BIG_ROUNDS" \
        --setenv=SCANS_ALT="$SCANS_ALT" --setenv=ALT_ROUNDS="$ALT_ROUNDS" --setenv=ALT_EVENT_TIMEOUT="$ALT_EVENT_TIMEOUT" \
        --setenv=REPLAY_ONLY="${REPLAY_ONLY:-}" --setenv=REPLAY_ROUNDS="$REPLAY_ROUNDS" \
        --setenv=REPLAY_OFF_ROUNDS="$REPLAY_OFF_ROUNDS" --setenv=REPLAY_IOPS="$REPLAY_IOPS" \
        --setenv=REPLAY_SECS="$REPLAY_SECS" --setenv=REPLAY_AT="$REPLAY_AT" --setenv=REPLAY_BSSPLIT="$REPLAY_BSSPLIT" \
        --setenv=CPU_SAMPLE="${CPU_SAMPLE:-}" --setenv=SAMPLER_CPU="$SAMPLER_CPU" \
        --setenv=NIC_ROUNDS="$NIC_ROUNDS" --setenv=NIC_CPU="$NIC_CPU" --setenv=NODE_ENV="$NODE_ENV" \
        --setenv=DRIVERS_ONLY="${DRIVERS_ONLY:-}" --setenv=DRIVERS_BEFORE="$DRIVERS_BEFORE" \
        --setenv=DRIVERS_AFTER="$DRIVERS_AFTER" --setenv=DRIVER_ROUNDS="$DRIVER_ROUNDS" --setenv=OBJECTS="$OBJECTS" \
        sh -c "exec sh $LAB/run/m23-lab.sh run >> $LAB/run/m23.log 2>&1"
    echo "started m23-lab; tail -f $LAB/run/m23.log"
}

cmd_stop() {
    st_main=$(systemctl show m23-lab -p MainPID --value)
    [ -n "$st_main" ] && [ "$st_main" != 0 ] || die "m23-lab is not running"
    # the script, then the bench under it, so its trap puts the hosts back once the bench exits
    kill -TERM "$st_main"
    for st_child in $(pgrep -P "$st_main"); do
        for st_bench in $st_child $(pgrep -P "$st_child"); do
            kill -INT "$st_bench" 2>/dev/null
        done
    done
    st_tries=0
    while [ "$(systemctl show m23-lab -p ActiveState --value)" != inactive ]; do
        st_tries=$((st_tries + 1)); [ $st_tries -lt 120 ] || die "m23-lab is still running"; sleep 5
    done
    tail -n 5 "$LAB/run/m23.log"
}

cmd_status() {
    systemctl show m23-lab -p ActiveState --value
    grep '^== ' "$LAB/run/m23.log" 2>/dev/null | tail -n 5
}

# every capture slimmed into the results by the exit's build, its per-second client series dropped
# but the cell's beside a scan and the control's, which the report cuts at the cell's feed's wrap
cmd_fetch() {
    mkdir -p "$OUT/m23"
    # what each round's nodes read from NODE_ENV, apart from the captures the report reads
    for env in "$LAB"/captures/m23-*-env.txt; do
        [ -f "$env" ] || continue
        mkdir -p "$OUT/env" && cp "$env" "$OUT/env/"
    done
    for dir in "$LAB"/captures/m23-*/; do
        name=$(basename "$dir")
        # item 359's samples stay under $LAB, the report reading them there; what each round ran
        # with, its scan settings and its hosts' disks, goes beside the captures
        if [ -f "$dir/scans.txt" ]; then
            mkdir -p "$OUT/samples/$name"
            cp "$dir/scans.txt" "$dir/disks.txt" "$OUT/samples/$name/" 2>/dev/null
            # and a replay round's side, its replay's settings and each fio's times and figures
            for fe_file in "$dir/side.txt" "$dir/replay.txt" "$dir"/replay-*.txt "$dir"/fio-*.json; do
                if [ -f "$fe_file" ]; then cp "$fe_file" "$OUT/samples/$name/"; fi
            done
            continue
        fi
        # an idle sample is its own small file, beside the captures it is read with
        if [ -f "$dir/idle.txt" ]; then
            cp "$dir/idle.txt" "$OUT/m23/$name.txt"
            continue
        fi
        [ -f "$dir/bench.json" ] || continue
        case $name in
            m23-drivers-*)
                fe_tree=$(drivers_tree "$(echo "$name" | cut -d- -f3)")
                "$fe_tree/target/deploy/release/tmdb-dataset-loader" bench slim "$dir" -o "$OUT/m23/$name.json" > /dev/null \
                    || die "$name did not slim" ;;
            m23-r*-big | m23-r*-keyword)
                m23_cmd bench slim "$dir" --keep-series rw50/b1/scan -o "$OUT/m23/$name.json" > /dev/null \
                    || die "$name did not slim" ;;
            m23-control-*-cell | m23-fall-a | m23-fall-b | m23-r*-replay)
                # item 351's arms and item 359's replay arms keep their seconds too, which their reports read against the samples
                m23_cmd bench slim "$dir" --keep-series rw50/b1/none -o "$OUT/m23/$name.json" > /dev/null \
                    || die "$name did not slim" ;;
            m23-churn-*-churn)
                # the hour's members' samples dropped too, five MiB a size: the report reads the scans either side
                m23_cmd bench slim "$dir" -o "$OUT/m23/$name.json" > /dev/null || die "$name did not slim"
                python3 -I -c 'import json, sys
capture = json.load(open(sys.argv[1]))
for arm in capture["arms"]:
    for run in arm["runs"]:
        run.pop("server_series", None)
json.dump(capture, open(sys.argv[1], "w"), separators=(",", ":"))' "$OUT/m23/$name.json" || die "$name did not slim" ;;
            *) m23_cmd bench slim "$dir" -o "$OUT/m23/$name.json" > /dev/null || die "$name did not slim" ;;
        esac
    done
    ls "$OUT/m23" | wc -l
}

case ${1:-} in
    snapshot) cmd_snapshot ;;
    verify) cmd_verify ;;
    build) cmd_build ;;
    build-drivers) cmd_build_drivers ;;
    check) cmd_check ;;
    data) cmd_data ;;
    run) cmd_run ;;
    start) cmd_start ;;
    stop) cmd_stop ;;
    status) cmd_status ;;
    fetch) cmd_fetch ;;
    *) die "snapshot | build | build-drivers | check | data | run | start | status | stop | fetch | verify" ;;
esac
