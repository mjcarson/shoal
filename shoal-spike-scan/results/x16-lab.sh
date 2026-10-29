#!/bin/sh
# X16 on the lab: the walk beside the foreground, and W1's four engine rows before and after them
# (docs/src/future-work/scans/walk-beside-the-foreground.md). Run from the repository root on europa:
#
#   sh shoal-spike-scan/results/x16-lab.sh snapshot     # every host's state, before anything
#   sh shoal-spike-scan/results/x16-lab.sh check        # both sides' worktrees clean and built
#   sh shoal-spike-scan/results/x16-lab.sh data         # the dataset folder, once
#   QUICK=1 sh shoal-spike-scan/results/x16-lab.sh start  # one side, short arms: every arm runs
#   QUICK=1 QUICK_SIDE=before sh shoal-spike-scan/results/x16-lab.sh start   # the before side's
#   sh shoal-spike-scan/results/x16-lab.sh start        # the rounds, in a unit
#   sh shoal-spike-scan/results/x16-lab.sh status       # how far the unit is
#   sh shoal-spike-scan/results/x16-lab.sh stop         # the rounds ended early, the hosts put back
#   sh shoal-spike-scan/results/x16-lab.sh fetch        # every capture slimmed into results/x16
#   sh shoal-spike-scan/results/x16-lab.sh verify       # every host against its snapshot
#
# Two sides, each a worktree under $LAB with the spike's programs built in its target/deploy for
# znver1 (target/lab/x16/build.sh): `before`, F115 with the spike's crate cherry-picked (the walk as
# it stands, X16 itself), and `after`, F119 with it (W1's four rows). A side round is one cluster,
# `x16`, bootstrapped fresh from the side's node and destroyed after: a short run that preloads it,
# then two captures of it, attached, each after a restart so every row starts from its archives:
# the cell beside a scan of Big, with Big and Keyword scanned alone, and the cell beside a scan of
# Keyword. Attached, since a cluster of the bench's own is reset and preloaded again after every
# arm that writes, and every arm here writes; and an attached run never restarts its cluster, so
# the script does, waiting for every member and a settle after. Keyword is written into by a paced
# stream of inserts all the while, so its partitions are resident overlays when a scan reads them.
# The sides alternate by round, before first in odd rounds.
#
# The tmdb node, the hosts' maintenance timers and their governors are held for the whole run and
# put back on every way out; every change is recorded in x16-host-changes.txt, which `verify` checks
# against the snapshot. Never edit this file while a run's unit runs: `start` runs a copy of it.
# Never touch /, /xfs and /optane beyond the side cluster's own `/xfs/shoal-x16` and `/optane/shoal-x16`,
# which its bootstrap makes and its destroy removes.
set -u

REPO=$(pwd)
LAB=${LAB:-target/lab/x16}
OUT=${OUT:-shoal-spike-scan/results}
case $LAB in /*) ;; *) LAB=$REPO/$LAB ;; esac
case $OUT in /*) ;; *) OUT=$REPO/$OUT ;; esac
SNAP=$LAB/snap
CHANGES=$OUT/x16-host-changes.txt
HOSTS="europa titan hyperion"
DATASET=${DATASET:-$LAB/dataset}
TMDB=${TMDB:-$HOME/datasets/TMDB_movie_dataset_v11.csv}
ROUNDS=${ROUNDS:-4}
# the driver's cpus on europa, clear of the side node's control core and shards (0-6, 16-22)
DRIVER_CPUS=12-15,28-31
# what every run of a side round preloads, or takes as preloaded: the cell's first 100,000 rows,
# every row of Big and nine tenths of Keyword's, the rest inserted by the arms
PRELOAD="--attach --yes-write --preload 100% --preload-file Cell=100000 --preload-file Keyword=90%"
# the preload's own run: a few seconds of reads once the rows are in
LOAD="--workloads read100 --bundles 1 --tables Cell:1 --duration 5 --warmup 1 --runs 1"
# every capture's spec: rw50 on the cell alone at a bundle of one, Keyword written into beside it,
# the scan event a third of the way in, the clock waiting for it ten minutes at most
COMMON="--preloaded --yes-events --workloads rw50 --bundles 1 --tables Cell:1 --on-exhaust wrap \
--paced Keyword --paced-workload insert100 --paced-rate 200 --event-at 30 --event-timeout 1200 --runs 1"
TIMES="--duration 60 --warmup 10"
# the two captures of a side round
BIG="--events none,scan --event-table Big --scan-tables Big,Keyword"
KEYWORD="--events scan --event-table Keyword"
# how long a restarted cluster settles once every member is up, its leaders moved where they lead
SETTLE=30
if [ -n "${QUICK:-}" ]; then
    PRELOAD="$PRELOAD --preload-file Big=10%"
    TIMES="--duration 10 --warmup 2"
fi

ip_of() {
    case $1 in europa) echo 172.16.2.10 ;; titan) echo 172.16.2.4 ;; hyperion) echo 172.16.2.5 ;; esac
}

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
    echo "x16-lab: $*" >&2
    exit 1
}

# ---------------------------------------------------------------------------------------------
# a host's state, before and after

snapshot_host() {
    host=$1; label=$2
    dir="$SNAP/$host-$label"
    mkdir -p "$dir"
    on "$host" "systemctl list-unit-files --state=enabled --no-legend --plain | awk '{print \$1}' | sort" > "$dir/units-enabled.txt"
    on "$host" "systemctl list-units --all --plain --no-legend 'shoal-*' | awk '{print \$1, \$3}' | sort" > "$dir/units-shoal.txt"
    on "$host" "ls -d /xfs/shoal-x16* /optane/shoal-x16* /opt/shoal-deploy/x16* 2>/dev/null; ls /etc/sysctl.d | sort" > "$dir/paths.txt"
    on "$host" "cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor; systemctl is-active shoal-tmdb; \
        systemctl is-active e2scrub_all.timer fstrim.timer apt-daily.timer apt-daily-upgrade.timer; \
        cat /proc/sys/net/ipv4/tcp_slow_start_after_idle" > "$dir/state.txt"
}

cmd_snapshot() {
    for h in $HOSTS; do
        snapshot_host "$h" before
        echo "snapshot $h -> $SNAP/$h-before"
    done
}

cmd_verify() {
    status=0
    for h in $HOSTS; do
        snapshot_host "$h" after
        if diff -r "$SNAP/$h-before" "$SNAP/$h-after" > "$SNAP/$h.diff"; then
            echo "$h: identical to its snapshot"
        else
            echo "$h: differs from its snapshot:"; cat "$SNAP/$h.diff"; status=1
        fi
    done
    return $status
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
            was=$(cat "$LAB/governor-$h")
            on "$h" "sudo cpupower frequency-set -g $was > /dev/null"
        else
            was="left as it is, since no hold recorded one"
        fi
        on "$h" "sudo systemctl start e2scrub_all.timer fstrim.timer apt-daily.timer apt-daily-upgrade.timer shoal-tmdb"
        changed "$h: governor $was; e2scrub_all.timer, fstrim.timer, apt-daily.timer, apt-daily-upgrade.timer and shoal-tmdb started"
        rm -f "$LAB/governor-$h"
    done
}

# the cluster's root on every host has room, and no host's own root, titan's syslog's, is near full
roots_ok() {
    for h in $HOSTS; do
        case $h in europa) root=/optane ;; *) root=/xfs ;; esac
        used=$(on "$h" "df --output=pcent $root | tail -n 1 | tr -dc '0-9'")
        [ "${used:-100}" -lt 85 ] || die "$h's $root is ${used}% full"
        used=$(on "$h" "df --output=pcent / | tail -n 1 | tr -dc '0-9'")
        [ "${used:-100}" -lt 96 ] || die "$h's / is ${used}% full"
    done
}

# no side cluster, and no cluster the bench made, is left running on any host
no_bench_cluster() {
    for nb_host in $HOSTS; do
        nb_units=$(on "$nb_host" "systemctl list-units --all --plain --no-legend 'shoal-*-bench*' 'shoal-x16*' | awk '{print \$1}' | tr '\n' ' '")
        [ -z "$nb_units" ] || die "$nb_host runs ${nb_units}of a side cluster or the bench's: x16's \`destroy -i <its inventory> --yes\`, with SHOAL_DEPLOY_HOME=$LAB/state, takes it down"
    done
}

# ---------------------------------------------------------------------------------------------
# the sides

# a side's worktree is clean and holds its built programs
side_ok() {
    side=$1
    wt=$LAB/wt-$side
    [ -x "$wt/target/deploy/release/x16-node" ] && [ -x "$wt/target/deploy/release/x16" ] \
        || die "build the spike's programs in $wt first (target/lab/x16/build.sh)"
    [ -z "$(git -C "$wt" status --porcelain --untracked-files=no)" ] || die "$wt has uncommitted changes"
}

cmd_check() {
    for side in before after; do
        side_ok "$side"
        echo "$side: $(git -C "$LAB/wt-$side" log --oneline -1); node $(sha256sum "$LAB/wt-$side/target/deploy/release/x16-node" | cut -c1-16)"
    done
}

# the side cluster's inventory: tmdb_cluster.yaml's hosts and cores, a node's memory 4 GiB, every
# root on the host's SSD, no device, pool or bucket (the schema has none). Not 1 GiB, with Big three
# times it: the quick pass's nodes held 1.8 GB resident with 0.3 GB of it rows and indexes, so they
# were past their budget for good and evicted the cell every check. Big is read from its archives
# after every restart regardless, since a walk never makes a row resident
inventory() {
    side=$1; path=$2
    mkdir -p "$(dirname "$path")"
    cat > "$path" <<YAML
# X16's side cluster on the lab, written by x16-lab.sh for the $side side: the spike's schema on
# tmdb_cluster.yaml's hosts and cores, a node's memory 4 GiB, every root on the host's SSD
name: x16
server: $LAB/wt-$side/target/deploy/release/x16-node
ports:
  client: 12600
  peer: 12601
  control: 12602
replication_factor: 3
control_voters: 3
groups:
  bd795i:
    storage:
      latency: /optane/shoal-x16
    resources:
      cores: 6
      memory: 4Gi
      control_core_shared: false
    wal_commit_delay: 3ms
    lead_weight: 2
  v1000:
    storage:
      latency: /xfs/shoal-x16
    resources:
      cores: 5
      memory: 4Gi
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

# one of a side's admin commands, from its worktree's project, its deployments under $LAB alone
x16_cmd() {
    side=$1; shift
    wt=$LAB/wt-$side
    # shellcheck disable=SC2068
    (cd "$wt/shoal-spike-scan" && SHOAL_BIN_DIR="$LAB/state/bin" SHOAL_DEPLOY_HOME="$LAB/state" \
        taskset -c $DRIVER_CPUS "$wt/target/deploy/release/x16" "$@")
}

# one run of a side's cluster, driven by the side's admin program from the side's worktree, which
# the bench refuses to measure if it is dirty
capture() {
    side=$1; round=$2; which=$3; flags=$4
    label=x16-$side-r$round-$which
    out=$LAB/captures/$label
    roots_ok
    echo "== $(date -u +%FT%TZ) $label ($(git -C "$LAB/wt-$side" rev-parse --short HEAD))"
    # shellcheck disable=SC2086
    x16_cmd "$side" bench run -i "$LAB/inventories/x16-$side.yml" --dataset "$DATASET" $PRELOAD $flags \
        --basic --label "$label" --out "$out" ${OVERWRITE:+--overwrite} || die "the capture $label failed"
}

# every node of a side's cluster restarted, so every row starts from its archives, and waited for:
# every member up with the control group's three voters, then a settle for its leaders to move
restart_settled() {
    side=$1
    echo "== $(date -u +%FT%TZ) restarting the $side side's cluster"
    x16_cmd "$side" restart -i "$LAB/inventories/x16-$side.yml" || die "the $side side's cluster did not restart"
    rs_tries=0
    until x16_cmd "$side" status -i "$LAB/inventories/x16-$side.yml" 2>/dev/null | grep -Eq 'voters 3 +learners [0-9]+ +up 3 '; do
        rs_tries=$((rs_tries + 1)); [ $rs_tries -lt 60 ] || die "the $side side's cluster was not up five minutes after its restart"
        sleep 5
    done
    sleep $SETTLE
}

# the side cluster taken down, whichever way a round ends
teardown() {
    [ -n "${CURRENT:-}" ] || return 0
    echo "== $(date -u +%FT%TZ) destroying the $CURRENT side's cluster"
    x16_cmd "$CURRENT" destroy -i "$LAB/inventories/x16-$CURRENT.yml" --yes
    CURRENT=
}

# a side's round: its cluster bootstrapped fresh, the cell beside a scan of Big and each table
# scanned alone, then beside a scan of Keyword, and the cluster destroyed
side_round() {
    side=$1; round=$2
    for which in load big keyword; do
        [ -z "${OVERWRITE:-}" ] && [ -e "$LAB/captures/x16-$side-r$round-$which" ] \
            && die "$LAB/captures/x16-$side-r$round-$which is there already: another LAB, or OVERWRITE=1 to take it again"
    done
    inventory "$side" "$LAB/inventories/x16-$side.yml"
    roots_ok
    no_bench_cluster
    echo "== $(date -u +%FT%TZ) bootstrapping the $side side's cluster for round $round"
    CURRENT=$side
    x16_cmd "$side" bootstrap -i "$LAB/inventories/x16-$side.yml" || die "the $side side's cluster did not bootstrap"
    capture "$side" "$round" load "$LOAD"
    restart_settled "$side"
    capture "$side" "$round" big "$COMMON $TIMES $BIG"
    restart_settled "$side"
    capture "$side" "$round" keyword "$COMMON $TIMES $KEYWORD"
    teardown
}

cmd_run() {
    [ -s "$DATASET/Big.jsonl" ] || die "no dataset at $DATASET: \`data\` first"
    if [ -n "${QUICK:-}" ]; then sides="${QUICK_SIDE:-after}"; ROUNDS=1; else sides="before after"; fi
    for side in $sides; do side_ok "$side"; done
    mkdir -p "$LAB/captures" "$(dirname "$CHANGES")"
    no_bench_cluster
    hold
    CURRENT=
    trap 'teardown; restore' EXIT
    trap 'exit 143' INT TERM
    round=1
    while [ "$round" -le "$ROUNDS" ]; do
        # before first in odd rounds, after first in even ones
        if [ -n "${QUICK:-}" ]; then order="$sides"
        elif [ $((round % 2)) -eq 1 ]; then order="before after"
        else order="after before"; fi
        for side in $order; do side_round "$side" "$round"; done
        round=$((round + 1))
    done
    echo "== $(date -u +%FT%TZ) every round done"
}

cmd_data() {
    gen=$LAB/wt-after/target/deploy/release/x16-data
    [ -x "$gen" ] || die "build the spike's programs in $LAB/wt-after first"
    [ -r "$TMDB" ] || die "no TMDB csv at $TMDB"
    "$gen" --out "$DATASET" --tmdb "$TMDB"
}

cmd_start() {
    mkdir -p "$LAB/run"
    cp "$0" "$LAB/run/x16-lab.sh"
    # refusing a manual stop or restart, as the stores legs' units do (Resolved #306), with a user
    # unit's open files raised past its soft 1024
    sudo systemd-run --unit=x16-lab --collect -p User="$(id -un)" -p Group="$(id -gn)" \
        -p KillMode=mixed -p TimeoutStopSec=600 -p RefuseManualStop=yes -p LimitNOFILE=524288 \
        -p WorkingDirectory="$REPO" --setenv=HOME="$HOME" --setenv=PATH="$PATH" \
        --setenv=LAB="$LAB" --setenv=OUT="$OUT" --setenv=ROUNDS="$ROUNDS" --setenv=DATASET="$DATASET" \
        --setenv=QUICK="${QUICK:-}" --setenv=QUICK_SIDE="${QUICK_SIDE:-}" --setenv=OVERWRITE="${OVERWRITE:-}" \
        sh -c "exec sh $LAB/run/x16-lab.sh run >> $LAB/run/x16.log 2>&1"
    echo "started x16-lab; tail -f $LAB/run/x16.log"
}

cmd_stop() {
    st_main=$(systemctl show x16-lab -p MainPID --value)
    [ -n "$st_main" ] && [ "$st_main" != 0 ] || die "x16-lab is not running"
    # the script, then the bench under it, so its trap puts the hosts back once the bench exits
    kill -TERM "$st_main"
    for st_child in $(pgrep -P "$st_main"); do
        for st_bench in $st_child $(pgrep -P "$st_child"); do
            kill -INT "$st_bench" 2>/dev/null
        done
    done
    st_tries=0
    while [ "$(systemctl show x16-lab -p ActiveState --value)" != inactive ]; do
        st_tries=$((st_tries + 1)); [ $st_tries -lt 120 ] || die "x16-lab is still running"; sleep 5
    done
    tail -n 5 "$LAB/run/x16.log"
}

cmd_status() {
    systemctl show x16-lab -p ActiveState --value
    grep '^== ' "$LAB/run/x16.log" 2>/dev/null | tail -n 5
}

# every capture slimmed into the results by its own side's build, its per-second client series
# dropped and everything the report reads kept
cmd_fetch() {
    mkdir -p "$OUT/x16"
    for dir in "$LAB"/captures/x16-*/; do
        [ -f "$dir/bench.json" ] || continue
        name=$(basename "$dir")
        side=$(echo "$name" | cut -d- -f2)
        x16_cmd "$side" bench slim "$dir" -o "$OUT/x16/$name.json" > /dev/null || die "$name did not slim"
    done
    ls "$OUT/x16" | wc -l
}

case ${1:-} in
    snapshot) cmd_snapshot ;;
    verify) cmd_verify ;;
    check) cmd_check ;;
    data) cmd_data ;;
    run) cmd_run ;;
    start) cmd_start ;;
    stop) cmd_stop ;;
    status) cmd_status ;;
    fetch) cmd_fetch ;;
    *) die "snapshot | check | data | run | start | status | stop | fetch | verify" ;;
esac
