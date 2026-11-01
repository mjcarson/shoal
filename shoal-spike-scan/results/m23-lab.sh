#!/bin/sh
# M23's exit on the lab: the rebuilt walk beside the reference cell and alone, on the lab's SSDs
# (docs/src/future-work/scans/milestones.md, M23). Run from the repository root on europa:
#
#   sh shoal-spike-scan/results/m23-lab.sh snapshot     # every host's state, before anything
#   sh shoal-spike-scan/results/m23-lab.sh check        # the exit's worktree clean and built
#   sh shoal-spike-scan/results/m23-lab.sh data         # the rounds' dataset, once, after `build`
#   QUICK=1 sh shoal-spike-scan/results/m23-lab.sh start  # one round, short arms: every arm runs
#   sh shoal-spike-scan/results/m23-lab.sh start        # the rounds, in a unit
#   CHURN_ONLY=1 sh shoal-spike-scan/results/m23-lab.sh start   # the churn, at both segment sizes
#   FALL_ONLY=1 sh shoal-spike-scan/results/m23-lab.sh start    # item 351's run: the cell alone, sampled
#   sh shoal-spike-scan/results/m23-lab.sh build-drivers         # the tmdb programs, before and after
#   DRIVERS_ONLY=1 sh shoal-spike-scan/results/m23-lab.sh start  # the drivers, before and after
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
# first exit's 4. DATASET=target/lab/x16/dataset NODE_MEMORY=4Gi runs the first exit's rounds.
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
# the control (CONTROL_ONLY=1): the cell alone where a round's Keyword capture scans, as long as one
# of its arms runs, so the cell's own climb over a long arm is told from what a scan beside it adds
CONTROL="--preloaded --yes-events --workloads rw50 --bundles 1 --tables Cell:1 --on-exhaust wrap \
--paced Keyword --paced-workload insert100 --paced-rate 200 --duration 120 --warmup 100 --runs 1"
CONTROL_ROUNDS=${CONTROL_ROUNDS:-2}
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
fi

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
    on "$sn_host" "ls -d /xfs/shoal-m23* /optane/shoal-m23* /hdd/shoal-m23* /opt/shoal-deploy/m23* 2>/dev/null; ls /etc/sysctl.d | sort" > "$sn_dir/paths.txt"
    on "$sn_host" "cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor; systemctl is-active shoal-tmdb; \
        systemctl is-active e2scrub_all.timer fstrim.timer apt-daily.timer apt-daily-upgrade.timer; \
        cat /proc/sys/net/ipv4/tcp_slow_start_after_idle" > "$sn_dir/state.txt"
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

# a round: its cluster bootstrapped fresh, the cell beside a scan of Big and each table scanned
# alone, then beside a scan of Keyword, and the cluster destroyed
scan_round() {
    sr_round=$1
    sr_prefix=m23-r$sr_round
    for sr_which in load big keyword; do
        [ -z "${OVERWRITE:-}" ] && [ -e "$LAB/captures/$sr_prefix-$sr_which" ] \
            && die "$LAB/captures/$sr_prefix-$sr_which is there already: another LAB, or OVERWRITE=1 to take it again"
    done
    bootstrap
    capture "$sr_prefix-load" "$LOAD" "$PRELOAD"
    restart_settled
    capture "$sr_prefix-big" "$COMMON $TIMES $BIG" "$PRELOAD"
    restart_settled
    capture "$sr_prefix-keyword" "$COMMON $TIMES $KEYWORD" "$PRELOAD"
    teardown
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
    bootstrap
    capture "$cl_prefix-load" "$LOAD" "$PRELOAD"
    restart_settled
    capture "$cl_prefix-big" "$COMMON $TIMES $BIG" "$PRELOAD"
    restart_settled
    capture "$cl_prefix-cell" "$CONTROL" "$PRELOAD"
    teardown
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
# once its output closes; SAMPLERS keeps the local processes that carry them
samplers_start() {
    sa_dir=$1; sa_deadline=$2
    mkdir -p "$sa_dir"
    SAMPLERS=""
    for sa_host in $HOSTS; do
        if [ "$sa_host" = europa ]; then
            sh "$SAMPLER" "$sa_deadline" > "$sa_dir/samples-$sa_host.log" &
        else
            ssh -o BatchMode=yes "$sa_host" sh -s -- "$sa_deadline" < "$SAMPLER" > "$sa_dir/samples-$sa_host.log" &
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

# every directory fio's run made removed, on its way out or on any other
fall_clean() {
    for fc_made in ${FALL_MADE:-}; do
        fc_host=${fc_made%%:*}; fc_path=${fc_made#*:}
        # the only path this removes is the run's own, under a root the cluster never uses
        case $fc_path in /xfs/shoal-m23-fall | /optane/shoal-m23-fall) ;; *) continue ;; esac
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
    trap 'samplers_stop; fall_clean; teardown; restore' EXIT
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
    elif [ -n "${CONTROL_ONLY:-}" ]; then
        [ -n "${QUICK:-}" ] && CONTROL_ROUNDS=1
        rn_round=1
        while [ "$rn_round" -le "$CONTROL_ROUNDS" ]; do
            control_round "$rn_round"
            rn_round=$((rn_round + 1))
        done
    else
        rn_round=1
        while [ "$rn_round" -le "$ROUNDS" ]; do
            scan_round "$rn_round"
            rn_round=$((rn_round + 1))
        done
    fi
    echo "== $(date -u +%FT%TZ) every round done"
}

cmd_start() {
    mkdir -p "$LAB/run"
    cp "$0" "$LAB/run/m23-lab.sh"
    cp "$SAMPLER" "$LAB/run/m23-fall-sample.sh"
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
        --setenv=FALL_ONLY="${FALL_ONLY:-}" --setenv=REST_SECS="$REST_SECS" \
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
    for dir in "$LAB"/captures/m23-*/; do
        name=$(basename "$dir")
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
            m23-control-*-cell | m23-fall-a | m23-fall-b)
                # item 351's arms keep their seconds too, which its report reads against the samples
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
