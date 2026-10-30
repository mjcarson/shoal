#!/bin/sh
# X18 on the lab: which ceiling and trickle let a scan read a table's archives beside the cell, on
# each kind of device the lab has (docs/src/future-work/scans/spikes.md, X18). Run from the
# repository root on europa:
#
#   sh shoal-spike-scan/results/x18-lab.sh snapshot     # every host's state, before anything
#   sh shoal-spike-scan/results/x18-lab.sh check        # the spike's worktree clean and built
#   QUICK=1 sh shoal-spike-scan/results/x18-lab.sh start  # one round, short arms: every arm runs
#   sh shoal-spike-scan/results/x18-lab.sh start        # the rounds, in a unit
#   sh shoal-spike-scan/results/x18-lab.sh status | stop | fetch | verify
#
# The build is X17's, branch x17-spike's in target/lab/x17/wt-spike (spike code, never merged), its
# walk at X17's best, WALK_READS reads in flight and runs across WALK_GAP bytes, over Big merged in
# key order, set at each node's start by a drop-in on its unit. The pace is the product's (F119),
# set through the inventory's `scans:` section (rendered into every node's file since the scans
# plan's fourth step) and applied with `reconfigure`, which restarts every node, so each arm's rows
# start from their archives. Two layouts, alternating which first by round: `ssd`, every root on the
# host's SSD, so on titan and hyperion a scan reads the 970 EVO the WAL flushes to, and europa's
# Optane; and `disk`, the archives' root on each host's disk at /hdd, the WAL on the SSD, Big at a
# twentieth so a scan takes minutes. Each pace runs the cell beside a scan of Big; each layout also
# scans Big alone once a round at the widest pace, which states a ceiling in device time.
#
# The tmdb node, the hosts' maintenance timers and their governors are held for the whole run and
# put back on every way out; every change is recorded in x18-host-changes.txt, which `verify` checks
# against the snapshot. Never edit this file while a run's unit runs: `start` runs a copy of it.
# Never touch /, /xfs, /optane and /hdd beyond the side cluster's own shoal-x18 directories, which
# its bootstrap makes and its destroy removes.
set -u

REPO=$(pwd)
LAB=${LAB:-target/lab/x18}
OUT=${OUT:-shoal-spike-scan/results}
case $LAB in /*) ;; *) LAB=$REPO/$LAB ;; esac
case $OUT in /*) ;; *) OUT=$REPO/$OUT ;; esac
SPIKE=${SPIKE:-$REPO/target/lab/x17/wt-spike}
SNAP=$LAB/snap
CHANGES=$OUT/x18-host-changes.txt
HOSTS="europa titan hyperion"
# X16's dataset folder, which x16-data wrote once
DATASET=${DATASET:-$REPO/target/lab/x16/dataset}
ROUNDS=${ROUNDS:-4}
UNIT=shoal-x18
DROPIN=/etc/systemd/system/$UNIT.service.d
# X17's best walk, which its record names
WALK_READS=${WALK_READS:-16}
WALK_GAP=${WALK_GAP:-4096}
# the driver's cpus on europa, clear of the side node's control core and shards (0-6, 16-22)
DRIVER_CPUS=12-15,28-31
# every pace: its name, its ceiling and its trickle, a second a device; the trickle at the ceiling
# turns idle gating off
PACES="c32t2:32MiB:2MiB c64t2:64MiB:2MiB c128t2:128MiB:2MiB c256t2:256MiB:2MiB c64t64:64MiB:64MiB c256t256:256MiB:256MiB c128t1:128MiB:1MiB c128t4:128MiB:4MiB"
# the widest pace, at which a scan alone states its device time
WIDEST=c256t256:256MiB:256MiB
# the preload's own run: a few seconds of reads once the rows are in
LOAD="--workloads read100 --bundles 1 --tables Cell:1 --duration 5 --warmup 1 --runs 1"
# the cell beside a scan of Big a third of the way in, the scan waited for five minutes at most
CELL_TIMES="--duration 60 --warmup 10"
SCAN_TIMES="--duration 45 --warmup 5"
EVENT_TIMEOUT=300
# how long a restarted cluster settles once every member is up, its leaders moved where they lead
SETTLE=30
if [ -n "${QUICK:-}" ]; then
    CELL_TIMES="--duration 15 --warmup 3"
    SCAN_TIMES="--duration 10 --warmup 2"
    EVENT_TIMEOUT=60
fi

# what a layout's cluster takes as preloaded: the cell's first 100,000 rows and all of Big on the
# SSDs, a twentieth of it on the disks, and none of Keyword's
preload_of() {
    case $1 in
        ssd) if [ -n "${QUICK:-}" ]; then big=10%; else big=100%; fi ;;
        disk) if [ -n "${QUICK:-}" ]; then big=1%; else big=5%; fi ;;
    esac
    echo "--attach --yes-write --preload 0% --preload-file Cell=100000 --preload-file Big=$big"
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
    echo "x18-lab: $*" >&2
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
    on "$sn_host" "ls -d /xfs/shoal-x18* /optane/shoal-x18* /hdd/shoal-x18* /opt/shoal-deploy/x18* $DROPIN 2>/dev/null; ls /etc/sysctl.d | sort" > "$sn_dir/paths.txt"
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
        case $h in europa) ro_roots="/optane /hdd" ;; *) ro_roots="/xfs /hdd" ;; esac
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
        nb_units=$(on "$nb_host" "systemctl list-units --all --plain --no-legend 'shoal-*-bench*' 'shoal-x16*' 'shoal-x17*' 'shoal-x18*' | awk '{print \$1}' | tr '\n' ' '")
        [ -z "$nb_units" ] || die "$nb_host runs ${nb_units}of a side cluster or the bench's: x18's \`destroy -i <its inventory> --yes\`, with SHOAL_DEPLOY_HOME=$LAB/state, takes it down"
    done
}

# ---------------------------------------------------------------------------------------------
# the spike's build, its cluster, its walk and its pace

spike_ok() {
    [ -x "$SPIKE/target/deploy/release/x16-node" ] && [ -x "$SPIKE/target/deploy/release/x16" ] \
        || die "build the spike's programs in $SPIKE first"
    [ -z "$(git -C "$SPIKE" status --porcelain --untracked-files=no)" ] || die "$SPIKE has uncommitted changes"
}

cmd_check() {
    spike_ok
    echo "spike: $(git -C "$SPIKE" log --oneline -1); node $(sha256sum "$SPIKE/target/deploy/release/x16-node" | cut -c1-16)"
    echo "walk: $WALK_READS reads in flight, runs across $WALK_GAP bytes"
    [ -s "$DATASET/Big.jsonl" ] && echo "dataset: $DATASET" || die "no dataset at $DATASET: x16-lab.sh's \`data\` writes it"
}

# the side cluster's inventory: X16's hosts and cores, a layout's roots, and a pace
inventory() {
    in_layout=$1; in_ceiling=$2; in_trickle=$3; in_path=$4
    mkdir -p "$(dirname "$in_path")"
    case $in_layout in
        ssd) in_fast="latency: /optane/shoal-x18"; in_slow="latency: /xfs/shoal-x18" ;;
        disk) in_fast="{latency: /optane/shoal-x18, throughput: /hdd/shoal-x18}"; in_slow="{latency: /xfs/shoal-x18, throughput: /hdd/shoal-x18}" ;;
    esac
    case $in_layout in
        ssd) in_fast_storage="storage:
      $in_fast"; in_slow_storage="storage:
      $in_slow" ;;
        disk) in_fast_storage="storage: $in_fast"; in_slow_storage="storage: $in_slow" ;;
    esac
    cat > "$in_path" <<YAML
# X18's side cluster on the lab, written by x18-lab.sh for the $in_layout layout: the spike's schema
# on tmdb_cluster.yaml's hosts and cores, a node's memory 4 GiB, a scan's pace $in_ceiling a second
# and its trickle $in_trickle
name: x18
server: $SPIKE/target/deploy/release/x16-node
ports:
  client: 12600
  peer: 12601
  control: 12602
replication_factor: 3
control_voters: 3
scans:
  ceiling: $in_ceiling
  trickle: $in_trickle
groups:
  bd795i:
    $in_fast_storage
    resources:
      cores: 6
      memory: 4Gi
      control_core_shared: false
    wal_commit_delay: 3ms
    lead_weight: 2
  v1000:
    $in_slow_storage
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

# one of the spike's admin commands, from its worktree's project, its deployments under $LAB alone
x18_cmd() {
    # shellcheck disable=SC2068
    (cd "$SPIKE/shoal-spike-scan" && SHOAL_BIN_DIR="$LAB/state/bin" SHOAL_DEPLOY_HOME="$LAB/state" \
        taskset -c $DRIVER_CPUS "$SPIKE/target/deploy/release/x16" "$@")
}

# X17's best walk over Big merged in key order, read by every node at its next start
walk_switches() {
    for h in $HOSTS; do
        printf '%s\n' "[Service]" "Environment=SHOAL_SPIKE_MERGE_ORDER=key" "Environment=SHOAL_SPIKE_READS=$WALK_READS" \
            "Environment=SHOAL_SPIKE_RUN_GAP=$WALK_GAP" "Environment=SHOAL_SPIKE_RECORD_OPS=one" > "$LAB/x18-spike.conf"
        if [ "$h" = europa ]; then
            sudo mkdir -p "$DROPIN" && sudo cp "$LAB/x18-spike.conf" "$DROPIN/x18-spike.conf" && sudo systemctl daemon-reload
        else
            ssh -o BatchMode=yes "$h" "sudo mkdir -p $DROPIN && sudo tee $DROPIN/x18-spike.conf > /dev/null && sudo systemctl daemon-reload" < "$LAB/x18-spike.conf"
        fi
    done
    changed "every host: $DROPIN/x18-spike.conf with merge order key, reads $WALK_READS, gap $WALK_GAP"
}

switches_gone() {
    for h in $HOSTS; do
        on "$h" "sudo rm -rf $DROPIN && sudo systemctl daemon-reload"
    done
    changed "every host: $DROPIN removed"
}

# one run of the cluster, driven attached by the spike's admin program
capture() {
    cp_label=$1; cp_flags=$2; cp_preload=$3
    cp_out=$LAB/captures/$cp_label
    roots_ok
    echo "== $(date -u +%FT%TZ) $cp_label ($(git -C "$SPIKE" rev-parse --short HEAD))"
    # shellcheck disable=SC2086
    x18_cmd bench run -i "$LAB/inventories/x18.yml" --dataset "$DATASET" $cp_preload $cp_flags \
        --basic --label "$cp_label" --out "$cp_out" ${OVERWRITE:+--overwrite} || die "the capture $cp_label failed"
}

# every member up with the control group's three voters, then a settle for its leaders to move
settled() {
    st_tries=0
    until x18_cmd status -i "$LAB/inventories/x18.yml" 2>/dev/null | grep -Eq 'voters 3 +learners [0-9]+ +up 3 '; do
        st_tries=$((st_tries + 1)); [ $st_tries -lt 60 ] || die "the cluster was not up five minutes after its restart"
        sleep 5
    done
    sleep $SETTLE
}

# a pace written into the inventory and every node's file, each node restarted on it
pace_set() {
    ps_layout=$1; ps_spec=$2
    ps_ceiling=$(echo "$ps_spec" | cut -d: -f2); ps_trickle=$(echo "$ps_spec" | cut -d: -f3)
    inventory "$ps_layout" "$ps_ceiling" "$ps_trickle" "$LAB/inventories/x18.yml"
    echo "== $(date -u +%FT%TZ) reconfiguring at $ps_ceiling, trickle $ps_trickle"
    x18_cmd reconfigure -i "$LAB/inventories/x18.yml" --force || die "the cluster did not reconfigure"
    settled
}

# the side cluster taken down, whichever way a round ends
teardown() {
    [ -n "${CURRENT:-}" ] || return 0
    echo "== $(date -u +%FT%TZ) destroying the cluster"
    x18_cmd destroy -i "$LAB/inventories/x18.yml" --yes
    switches_gone
    CURRENT=
}

# one layout's part of a round: its cluster bootstrapped fresh and loaded, then the cell beside a
# scan at every pace, and a scan alone at the widest
layout_round() {
    lr_layout=$1; lr_round=$2
    lr_prefix=x18-$lr_layout-r$lr_round
    lr_preload=$(preload_of "$lr_layout")
    [ -z "${OVERWRITE:-}" ] && [ -e "$LAB/captures/$lr_prefix-load" ] \
        && die "$LAB/captures/$lr_prefix-load is there already: another LAB, or OVERWRITE=1 to take it again"
    inventory "$lr_layout" 100MiB 2MiB "$LAB/inventories/x18.yml"
    roots_ok
    no_bench_cluster
    walk_switches
    echo "== $(date -u +%FT%TZ) bootstrapping the cluster for $lr_prefix"
    CURRENT=x18
    x18_cmd bootstrap -i "$LAB/inventories/x18.yml" || die "the cluster did not bootstrap"
    capture "$lr_prefix-load" "$LOAD" "$lr_preload"
    for lr_spec in $PACES; do
        lr_name=$(echo "$lr_spec" | cut -d: -f1)
        pace_set "$lr_layout" "$lr_spec"
        capture "$lr_prefix-cell-$lr_name" "--preloaded --yes-events --workloads rw50 --bundles 1 --tables Cell:1 --on-exhaust wrap \
--events none,scan --event-table Big --event-at 30 --event-timeout $EVENT_TIMEOUT $CELL_TIMES --runs 1" "$lr_preload"
    done
    # a scan alone at the widest pace, its device time a byte
    pace_set "$lr_layout" "$WIDEST"
    capture "$lr_prefix-scan-widest" "--preloaded --scan-tables Big $SCAN_TIMES --runs 1" "$lr_preload"
    teardown
}

cmd_run() {
    [ -s "$DATASET/Big.jsonl" ] || die "no dataset at $DATASET"
    [ -n "${QUICK:-}" ] && ROUNDS=1
    spike_ok
    mkdir -p "$LAB/captures" "$LAB/inventories" "$(dirname "$CHANGES")"
    no_bench_cluster
    hold
    CURRENT=
    trap 'teardown; restore' EXIT
    trap 'exit 143' INT TERM
    rn_round=1
    while [ "$rn_round" -le "$ROUNDS" ]; do
        # ssd first in odd rounds, disk first in even ones
        if [ $((rn_round % 2)) -eq 1 ]; then rn_layouts="ssd disk"; else rn_layouts="disk ssd"; fi
        for rn_layout in $rn_layouts; do layout_round "$rn_layout" "$rn_round"; done
        rn_round=$((rn_round + 1))
    done
    echo "== $(date -u +%FT%TZ) every round done"
}

cmd_start() {
    mkdir -p "$LAB/run"
    cp "$0" "$LAB/run/x18-lab.sh"
    # refusing a manual stop or restart, as the stores legs' units do (Resolved #306), with a user
    # unit's open files raised past its soft 1024
    sudo systemd-run --unit=x18-lab --collect -p User="$(id -un)" -p Group="$(id -gn)" \
        -p KillMode=mixed -p TimeoutStopSec=600 -p RefuseManualStop=yes -p LimitNOFILE=524288 \
        -p WorkingDirectory="$REPO" --setenv=HOME="$HOME" --setenv=PATH="$PATH" \
        --setenv=LAB="$LAB" --setenv=OUT="$OUT" --setenv=ROUNDS="$ROUNDS" --setenv=DATASET="$DATASET" \
        --setenv=SPIKE="$SPIKE" --setenv=WALK_READS="$WALK_READS" --setenv=WALK_GAP="$WALK_GAP" \
        --setenv=QUICK="${QUICK:-}" --setenv=OVERWRITE="${OVERWRITE:-}" \
        sh -c "exec sh $LAB/run/x18-lab.sh run >> $LAB/run/x18.log 2>&1"
    echo "started x18-lab; tail -f $LAB/run/x18.log"
}

cmd_stop() {
    sp_main=$(systemctl show x18-lab -p MainPID --value)
    [ -n "$sp_main" ] && [ "$sp_main" != 0 ] || die "x18-lab is not running"
    # the script, then the bench under it, so its trap puts the hosts back once the bench exits
    kill -TERM "$sp_main"
    for sp_child in $(pgrep -P "$sp_main"); do
        for sp_bench in $sp_child $(pgrep -P "$sp_child"); do
            kill -INT "$sp_bench" 2>/dev/null
        done
    done
    sp_tries=0
    while [ "$(systemctl show x18-lab -p ActiveState --value)" != inactive ]; do
        sp_tries=$((sp_tries + 1)); [ $sp_tries -lt 120 ] || die "x18-lab is still running"; sleep 5
    done
    tail -n 5 "$LAB/run/x18.log"
}

cmd_status() {
    systemctl show x18-lab -p ActiveState --value
    grep '^== ' "$LAB/run/x18.log" 2>/dev/null | tail -n 5
}

# every capture slimmed into the results
cmd_fetch() {
    mkdir -p "$OUT/x18"
    for fe_dir in "$LAB"/captures/x18-*/; do
        [ -f "$fe_dir/bench.json" ] || continue
        fe_name=$(basename "$fe_dir")
        x18_cmd bench slim "$fe_dir" -o "$OUT/x18/$fe_name.json" > /dev/null || die "$fe_name did not slim"
    done
    ls "$OUT/x18" | wc -l
}

case ${1:-} in
    snapshot) cmd_snapshot ;;
    verify) cmd_verify ;;
    check) cmd_check ;;
    run) cmd_run ;;
    start) cmd_start ;;
    stop) cmd_stop ;;
    status) cmd_status ;;
    fetch) cmd_fetch ;;
    *) die "snapshot | check | run | start | status | stop | fetch | verify" ;;
esac
