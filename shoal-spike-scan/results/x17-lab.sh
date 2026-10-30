#!/bin/sh
# X17's reads half and X37 on the lab: a walk page's archive reads in flight and in runs, one
# operation a record against three, and a merge's order priced, over archives merged in a hash
# map's order and in key order (docs/src/future-work/scans/spikes.md, X17 and X37). Run from the
# repository root on europa:
#
#   sh shoal-spike-scan/results/x17-lab.sh snapshot     # every host's state, before anything
#   sh shoal-spike-scan/results/x17-lab.sh check        # the spike's worktree clean and built
#   QUICK=1 sh shoal-spike-scan/results/x17-lab.sh start  # one round, short arms: every arm runs
#   sh shoal-spike-scan/results/x17-lab.sh start        # the rounds, in a unit
#   CHURN_ONLY=1 ROUND_FROM=5 ROUNDS=2 sh shoal-spike-scan/results/x17-lab.sh start
#                                                       # the churn's supplement, rounds 5 and 6
#   PLAN_ONLY=1 ROUND_FROM=9 sh shoal-spike-scan/results/x17-lab.sh start
#                                                       # X17's other half, rounds 9 to 12
#   sh shoal-spike-scan/results/x17-lab.sh encode       # Q35's benchmark on europa and titan
#   sh shoal-spike-scan/results/x17-lab.sh status       # how far the unit is
#   sh shoal-spike-scan/results/x17-lab.sh stop         # the rounds ended early, the hosts put back
#   sh shoal-spike-scan/results/x17-lab.sh fetch        # every capture slimmed into results/x17
#   sh shoal-spike-scan/results/x17-lab.sh verify       # every host against its snapshot
#
# One build, branch x17-spike's in $LAB/wt-spike (spike code, never merged), whose switches a node
# reads at its start from an Environment= drop-in on its unit, shoal-x17.service.d/x17-spike.conf:
# SHOAL_SPIKE_MERGE_ORDER (hash or key), SHOAL_SPIKE_RECORD_OPS (three or one), SHOAL_SPIKE_READS
# and SHOAL_SPIKE_RUN_GAP. A round loads Big fresh for each layout, the layouts' order alternating
# by round: the side cluster `x17` bootstrapped with the layout's merge order and the cell's rows
# and Big preloaded, then each arm after its switches are set and the cluster restarted and settled,
# so every row starts from its archives: scans of Big alone at each read setting, one with the
# driver's shed behaviour from before O139, one at four pages a shard, the cell beside a scan at two
# settings, and gets of Big. Every scan alone reads under a pace lifted past what any device gives,
# so it measures the walk, not F119's pace (X18's); the cell's arms run at the product's defaults.
# Each round first takes a minute of every host's cpu with nothing running. Round one also profiles
# titan's node under two scan arms, and runs twenty minutes of inserts and overwrites into Big under
# each layout, from a churn dataset of its own, before scanning it again, so a tablet's stretches
# are counted after churn.
#
# The tmdb node, the hosts' maintenance timers and their governors are held for the whole run and
# put back on every way out; every change is recorded in x17-host-changes.txt, which `verify` checks
# against the snapshot. Never edit this file while a run's unit runs: `start` runs a copy of it.
# Never touch /, /xfs and /optane beyond the side cluster's own `/xfs/shoal-x17` and `/optane/shoal-x17`,
# which its bootstrap makes and its destroy removes.
set -u

REPO=$(pwd)
LAB=${LAB:-target/lab/x17}
OUT=${OUT:-shoal-spike-scan/results}
case $LAB in /*) ;; *) LAB=$REPO/$LAB ;; esac
case $OUT in /*) ;; *) OUT=$REPO/$OUT ;; esac
SNAP=$LAB/snap
CHANGES=$OUT/x17-host-changes.txt
HOSTS="europa titan hyperion"
# X16's dataset folder, which x16-data wrote once
DATASET=${DATASET:-$REPO/target/lab/x16/dataset}
ROUNDS=${ROUNDS:-4}
# the first round's number, so a supplement's rounds follow the run's rather than take its names
ROUND_FROM=${ROUND_FROM:-1}
UNIT=shoal-x17
DROPIN=/etc/systemd/system/$UNIT.service.d
# the driver's cpus on europa, clear of the side node's control core and shards (0-6, 16-22)
DRIVER_CPUS=12-15,28-31
# what every run of a layout's cluster takes as preloaded: the cell's first 100,000 rows and every
# row of Big, and none of Keyword's, which no arm here reads
PRELOAD="--attach --yes-write --preload 0% --preload-file Cell=100000 --preload-file Big=100%"
# the preload's own run: a few seconds of reads once the rows are in
LOAD="--workloads read100 --bundles 1 --tables Cell:1 --duration 5 --warmup 1 --runs 1"
# a scan arm: whole scans of Big back to back, from every member's holders
SCAN_TIMES="--duration 45 --warmup 5"
# the cell beside a scan of Big a third of the way in, as X16 drove it
CELL_TIMES="--duration 60 --warmup 10"
# gets of Big at a bundle of one
GETS_TIMES="--duration 45 --warmup 5"
# twenty minutes of inserts and overwrites into Big, round one alone: its own dataset, 300,000 new
# rows past Big's last id and an overwrite of every tenth row in turn, nothing of it preloaded, so
# its insert pool is every row of it and, wrapped, overwrites after
CHURN_TIMES="--duration 1200 --warmup 5"
CHURN_DATASET=${CHURN_DATASET:-$LAB/churn}
CHURN_PRELOAD="--attach --yes-write --preload 0% --preload-file Cell=100000 --preload-file Big=0%"
# the pace a scan alone reads under, lifted past what any device gives, ceiling:trickle:pages a
# shard; the cell's arms take the product's defaults (100 MiB, 2 MiB and 2), written as none
OPEN_PACE="4GiB:4GiB:2"
# X17's other half: nine tenths of Keyword preloaded besides, the rest written by a paced stream of
# inserts through every arm as X16 wrote it, and every page read as g4k16 reads, captured or planned
PLAN_PRELOAD="--attach --yes-write --preload 0% --preload-file Cell=100000 --preload-file Big=100% --preload-file Keyword=90%"
PLAN_COMMON="--preloaded --yes-events --workloads rw50 --bundles 1 --tables Cell:1 --on-exhaust wrap \
--paced Keyword --paced-workload insert100 --paced-rate 200 --event-at 30 --event-timeout 1200 --runs 1"
# the arm at four pages a shard: g4k16's reads, its streams twice the default's
WIDE_ARM="g4k16p4:16:4096:one"
WIDE_PACE="4GiB:4GiB:4"
WIDE_STREAMS=64
# every read arm: its name, reads in flight, run gap and record operations
READ_ARMS="r1:1:none:one r4:4:none:one r16:16:none:one g4k2:2:4096:one g4k16:16:4096:one g128k2:2:131072:one t1:1:none:three t16:16:none:three"
# the two settings the cell runs beside a scan at
CELL_ARMS="r1:1:none:one g4k16:16:4096:one"
# how long a restarted cluster settles once every member is up, its leaders moved where they lead
SETTLE=30
if [ -n "${QUICK:-}" ]; then
    PRELOAD="--attach --yes-write --preload 0% --preload-file Cell=100000 --preload-file Big=10%"
    SCAN_TIMES="--duration 10 --warmup 2"
    CELL_TIMES="--duration 15 --warmup 3"
    GETS_TIMES="--duration 10 --warmup 2"
    CHURN_TIMES="--duration 30 --warmup 2"
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
    echo "x17-lab: $*" >&2
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
    on "$host" "ls -d /xfs/shoal-x17* /optane/shoal-x17* /opt/shoal-deploy/x17* $DROPIN /var/tmp/x17-perf* 2>/dev/null; ls /etc/sysctl.d | sort" > "$dir/paths.txt"
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
        nb_units=$(on "$nb_host" "systemctl list-units --all --plain --no-legend 'shoal-*-bench*' 'shoal-x16*' 'shoal-x17*' | awk '{print \$1}' | tr '\n' ' '")
        [ -z "$nb_units" ] || die "$nb_host runs ${nb_units}of a side cluster or the bench's: x17's \`destroy -i <its inventory> --yes\`, with SHOAL_DEPLOY_HOME=$LAB/state, takes it down"
    done
}

# ---------------------------------------------------------------------------------------------
# the spike's build, its cluster and its switches

# the spike's worktree is clean and holds its built programs
spike_ok() {
    wt=$LAB/wt-spike
    [ -x "$wt/target/deploy/release/x16-node" ] && [ -x "$wt/target/deploy/release/x16" ] \
        || die "build the spike's programs in $wt first"
    [ -z "$(git -C "$wt" status --porcelain --untracked-files=no)" ] || die "$wt has uncommitted changes"
}

cmd_check() {
    spike_ok
    echo "spike: $(git -C "$LAB/wt-spike" log --oneline -1); node $(sha256sum "$LAB/wt-spike/target/deploy/release/x16-node" | cut -c1-16)"
    [ -s "$DATASET/Big.jsonl" ] && echo "dataset: $DATASET" || die "no dataset at $DATASET: x16-lab.sh's \`data\` writes it"
}

# the side cluster's inventory: X16's, under the name x17 on roots of its own
inventory() {
    path=$1; in_pace=${2:-}
    mkdir -p "$(dirname "$path")"
    # a pace as the inventory's scans section, none for the product's defaults
    in_scans=""
    if [ -n "$in_pace" ]; then
        in_scans="scans:
  ceiling: $(echo "$in_pace" | cut -d: -f1)
  trickle: $(echo "$in_pace" | cut -d: -f2)
  pages_in_flight: $(echo "$in_pace" | cut -d: -f3)"
    fi
    cat > "$path" <<YAML
# X17's side cluster on the lab, written by x17-lab.sh: the spike's schema on tmdb_cluster.yaml's
# hosts and cores, a node's memory 4 GiB, every root on the host's SSD, as X16's
name: x17
server: $LAB/wt-spike/target/deploy/release/x16-node
ports:
  client: 12600
  peer: 12601
  control: 12602
replication_factor: 3
control_voters: 3
$in_scans
groups:
  bd795i:
    storage:
      latency: /optane/shoal-x17
    resources:
      cores: 6
      memory: 4Gi
      control_core_shared: false
    wal_commit_delay: 3ms
    lead_weight: 2
  v1000:
    storage:
      latency: /xfs/shoal-x17
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
x17_cmd() {
    wt=$LAB/wt-spike
    # shellcheck disable=SC2068
    (cd "$wt/shoal-spike-scan" && SHOAL_BIN_DIR="$LAB/state/bin" SHOAL_DEPLOY_HOME="$LAB/state" \
        taskset -c $DRIVER_CPUS "$wt/target/deploy/release/x16" "$@")
}

# every host's node reads these switches at its next start: the layout's merge order and an arm's
# reads, gap and record operations, in a drop-in its unit takes once systemd reloads
switches() {
    sw_order=$1; sw_reads=$2; sw_gap=$3; sw_ops=$4; sw_page=${5:-capture}
    for h in $HOSTS; do
        printf '%s\n' "[Service]" "Environment=SHOAL_SPIKE_MERGE_ORDER=$sw_order" "Environment=SHOAL_SPIKE_READS=$sw_reads" \
            "Environment=SHOAL_SPIKE_RUN_GAP=$sw_gap" "Environment=SHOAL_SPIKE_RECORD_OPS=$sw_ops" \
            "Environment=SHOAL_SPIKE_PAGE=$sw_page" > "$LAB/x17-spike.conf"
        if [ "$h" = europa ]; then
            sudo mkdir -p "$DROPIN" && sudo cp "$LAB/x17-spike.conf" "$DROPIN/x17-spike.conf" && sudo systemctl daemon-reload
        else
            ssh -o BatchMode=yes "$h" "sudo mkdir -p $DROPIN && sudo tee $DROPIN/x17-spike.conf > /dev/null && sudo systemctl daemon-reload" < "$LAB/x17-spike.conf"
        fi
    done
    changed "every host: $DROPIN/x17-spike.conf with merge order $sw_order, reads $sw_reads, gap $sw_gap, record operations $sw_ops, page $sw_page"
}

# the drop-ins gone, on every host
switches_gone() {
    for h in $HOSTS; do
        on "$h" "sudo rm -rf $DROPIN && sudo systemctl daemon-reload"
    done
    changed "every host: $DROPIN removed"
}

# one run of the spike's cluster, driven attached by its admin program, which the bench refuses to
# measure if its worktree is dirty
capture() {
    cp_label=$1; cp_flags=$2; cp_dataset=${3:-$DATASET}; cp_preload=${4:-$PRELOAD}
    cp_out=$LAB/captures/$cp_label
    roots_ok
    echo "== $(date -u +%FT%TZ) $cp_label ($(git -C "$LAB/wt-spike" rev-parse --short HEAD))"
    # shellcheck disable=SC2086
    x17_cmd bench run -i "$LAB/inventories/x17.yml" --dataset "$cp_dataset" $cp_preload $cp_flags \
        --basic --label "$cp_label" --out "$cp_out" ${OVERWRITE:+--overwrite} || die "the capture $cp_label failed"
}

# every node restarted with the switches written, waited for: every member up with the control
# group's three voters, then a settle for its leaders to move
restart_settled() {
    echo "== $(date -u +%FT%TZ) restarting the cluster"
    x17_cmd restart -i "$LAB/inventories/x17.yml" || die "the cluster did not restart"
    rs_tries=0
    until x17_cmd status -i "$LAB/inventories/x17.yml" 2>/dev/null | grep -Eq 'voters 3 +learners [0-9]+ +up 3 '; do
        rs_tries=$((rs_tries + 1)); [ $rs_tries -lt 60 ] || die "the cluster was not up five minutes after its restart"
        sleep 5
    done
    sleep $SETTLE
}

# every node's file rendered again from the inventory and every node restarted, the leader last,
# waited for as a restart is
reconfigure_settled() {
    echo "== $(date -u +%FT%TZ) reconfiguring the cluster"
    x17_cmd reconfigure -i "$LAB/inventories/x17.yml" --force || die "the cluster did not reconfigure"
    rc_tries=0
    until x17_cmd status -i "$LAB/inventories/x17.yml" 2>/dev/null | grep -Eq 'voters 3 +learners [0-9]+ +up 3 '; do
        rc_tries=$((rc_tries + 1)); [ $rc_tries -lt 60 ] || die "the cluster was not up five minutes after its reconfigure"
        sleep 5
    done
    sleep $SETTLE
}

# an arm's switches and its pace set and the cluster restarted under them
arm_set() {
    as_order=$1; as_spec=$2; as_pace=${3:-}
    as_reads=$(echo "$as_spec" | cut -d: -f2); as_gap=$(echo "$as_spec" | cut -d: -f3); as_ops=$(echo "$as_spec" | cut -d: -f4)
    switches "$as_order" "$as_reads" "$as_gap" "$as_ops"
    inventory "$LAB/inventories/x17.yml" "$as_pace"
    reconfigure_settled
}

# a profile of titan's node for twenty seconds, a quarter of the way into the arm under way
profile_titan() {
    pt_name=$1
    (
        sleep 15
        on titan "pid=\$(pgrep -f '[x]16-node' | head -n 1); [ -n \"\$pid\" ] && sudo perf record -q -F 999 -g -p \$pid -o /var/tmp/x17-perf-$pt_name.data -- sleep 20 > /dev/null 2>&1; \
            sudo perf report -q -i /var/tmp/x17-perf-$pt_name.data --stdio --no-children --sort dso,symbol 2>/dev/null | head -n 400; \
            sudo rm -f /var/tmp/x17-perf-$pt_name.data /var/tmp/x17-perf-$pt_name.data.old" > "$LAB/perf/$pt_name.txt" 2>&1
        changed "titan: /var/tmp/x17-perf-$pt_name.data recorded and removed"
    ) &
}

# every node's merges since a moment, as the spike logs them: each one's sort, its write and its
# order's size (X37)
merges_since() {
    ms_since=$1; ms_name=$2
    mkdir -p "$LAB/merges"
    for h in $HOSTS; do
        on "$h" "journalctl -u $UNIT --since '$ms_since' --no-pager -o cat 2>/dev/null | grep 'X37: a merge' || true" > "$LAB/merges/$ms_name-$h.log"
    done
}

# the switches every node said it read at its last start, beside the arm that ran on them
switches_said() {
    ss_name=$1
    mkdir -p "$LAB/switches"
    for h in $HOSTS; do
        on "$h" "journalctl -u $UNIT -n 2000 --no-pager -o cat 2>/dev/null | grep \"X17's spike switches\" | tail -n 1" > "$LAB/switches/$ss_name-$h.txt"
    done
}

# a minute of every host's cpu with the cluster up and nothing driving it
idle_minute() {
    im_round=$1
    mkdir -p "$LAB/idle"
    for h in $HOSTS; do on "$h" "head -n 1 /proc/stat" > "$LAB/idle/r$im_round-$h.before"; done
    sleep 60
    for h in $HOSTS; do on "$h" "head -n 1 /proc/stat" > "$LAB/idle/r$im_round-$h.after"; done
}

# the side cluster taken down, whichever way a round ends
teardown() {
    [ -n "${CURRENT:-}" ] || return 0
    echo "== $(date -u +%FT%TZ) destroying the cluster"
    x17_cmd destroy -i "$LAB/inventories/x17.yml" --yes
    switches_gone
    CURRENT=
}

# one layout's part of a round: its cluster bootstrapped fresh under its merge order and loaded,
# then every arm, each after its switches are set and the cluster restarted
layout_round() {
    order=$1; round=$2
    prefix=x17-$order-r$round
    [ -z "${OVERWRITE:-}" ] && [ -e "$LAB/captures/$prefix-load" ] \
        && die "$LAB/captures/$prefix-load is there already: another LAB, or OVERWRITE=1 to take it again"
    inventory "$LAB/inventories/x17.yml"
    roots_ok
    no_bench_cluster
    # the merge order in place before the first row is written, as every arm's reads after it
    switches "$order" 1 none one
    echo "== $(date -u +%FT%TZ) bootstrapping the cluster for $prefix"
    CURRENT=x17
    booted=$(date -u '+%Y-%m-%d %H:%M:%S')
    x17_cmd bootstrap -i "$LAB/inventories/x17.yml" || die "the cluster did not bootstrap"
    capture "$prefix-load" "$LOAD"
    merges_since "$booted UTC" "$prefix-load"
    # a minute of every host's cpu with the cluster up and idle, once a round
    if [ "$order" = "$FIRST" ]; then restart_settled; idle_minute "$round"; fi
    # scans of Big alone at every read setting
    for spec in $READ_ARMS; do
        name=$(echo "$spec" | cut -d: -f1)
        arm_set "$order" "$spec" "$OPEN_PACE"
        if [ "$round" = 1 ] && [ "$order" = key ] && { [ "$name" = r1 ] || [ "$name" = g4k16 ]; }; then
            mkdir -p "$LAB/perf"
            profile_titan "$order-$name"
        fi
        capture "$prefix-scan-$name" "--preloaded --scan-tables Big $SCAN_TIMES --runs 1"
        # the switches each node said it read, which it says at their first use, in the arm
        switches_said "$prefix-scan-$name"
        wait
    done
    # the driver's shed behaviour from before O139, on today's walk
    if [ "$order" = key ]; then
        arm_set "$order" r1:1:none:one "$OPEN_PACE"
        SHOAL_SPIKE_SHED_STAY=1 capture "$prefix-scan-shedstay" "--preloaded --scan-tables Big $SCAN_TIMES --runs 1"
    fi
    # g4k16's reads at four pages a shard and twice the streams, which says whether two pages a
    # shard is what the walk's rate stops at
    arm_set "$order" "$WIDE_ARM" "$WIDE_PACE"
    capture "$prefix-scan-$(echo "$WIDE_ARM" | cut -d: -f1)" "--preloaded --scan-tables Big --scan-streams $WIDE_STREAMS $SCAN_TIMES --runs 1"
    switches_said "$prefix-scan-$(echo "$WIDE_ARM" | cut -d: -f1)"
    # the cell beside a scan of Big at two settings, at the product's pace
    for spec in $CELL_ARMS; do
        name=$(echo "$spec" | cut -d: -f1)
        arm_set "$order" "$spec"
        capture "$prefix-cell-$name" "--preloaded --yes-events --workloads rw50 --bundles 1 --tables Cell:1 --on-exhaust wrap \
--events none,scan --event-table Big --event-at 30 --event-timeout 600 $CELL_TIMES --runs 1"
    done
    # gets of Big at a bundle of one
    arm_set "$order" r1:1:none:one
    capture "$prefix-gets" "--preloaded --workloads read100 --bundles 1 --tables Big:1 --on-exhaust wrap $GETS_TIMES --runs 1"
    # round one: inserts and overwrites into Big from the churn's own dataset, then a scan counting
    # its stretches again
    if [ "$round" = 1 ]; then
        churned=$(date -u '+%Y-%m-%d %H:%M:%S')
        capture "$prefix-churn" "--preloaded --workloads insert100 --bundles 1 --tables Big:1 --on-exhaust wrap $CHURN_TIMES --runs 1" \
            "$CHURN_DATASET" "$CHURN_PRELOAD"
        merges_since "$churned UTC" "$prefix-churn"
        arm_set "$order" r1:1:none:one "$OPEN_PACE"
        capture "$prefix-scan-afterchurn" "--preloaded --scan-tables Big $SCAN_TIMES --runs 1"
    fi
    teardown
}

# the churn's supplement, the key layout alone: its cluster bootstrapped and loaded, churned as
# round one churns it, then scanned at today's walk and in runs across 4 KiB and 128 KiB, so the
# partitions a device read of a walk in runs are measured on a churned table as well as stretches
churn_round() {
    order=key; round=$1
    prefix=x17-$order-r$round
    [ -z "${OVERWRITE:-}" ] && [ -e "$LAB/captures/$prefix-load" ] \
        && die "$LAB/captures/$prefix-load is there already: another LAB, or OVERWRITE=1 to take it again"
    inventory "$LAB/inventories/x17.yml"
    roots_ok
    no_bench_cluster
    switches "$order" 1 none one
    echo "== $(date -u +%FT%TZ) bootstrapping the cluster for $prefix"
    CURRENT=x17
    booted=$(date -u '+%Y-%m-%d %H:%M:%S')
    x17_cmd bootstrap -i "$LAB/inventories/x17.yml" || die "the cluster did not bootstrap"
    capture "$prefix-load" "$LOAD"
    merges_since "$booted UTC" "$prefix-load"
    churned=$(date -u '+%Y-%m-%d %H:%M:%S')
    capture "$prefix-churn" "--preloaded --workloads insert100 --bundles 1 --tables Big:1 --on-exhaust wrap $CHURN_TIMES --runs 1" \
        "$CHURN_DATASET" "$CHURN_PRELOAD"
    merges_since "$churned UTC" "$prefix-churn"
    for cr_spec in r1:1:none:one g4k16:16:4096:one g128k2:2:131072:one; do
        cr_name=$(echo "$cr_spec" | cut -d: -f1)
        arm_set "$order" "$cr_spec" "$OPEN_PACE"
        if [ "$cr_name" = r1 ]; then cr_label=$prefix-scan-afterchurn; else cr_label=$prefix-scan-afterchurn-$cr_name; fi
        capture "$cr_label" "--preloaded --scan-tables Big $SCAN_TIMES --runs 1"
        switches_said "$cr_label"
    done
    teardown
}

# X17's other half, the key layout alone: its cluster bootstrapped and loaded with nine tenths of
# Keyword besides, then each page mode's captures, the modes' order alternating by round: the cell
# beside a scan of Keyword with Keyword scanned alone, Keyword written into throughout, at the
# product's pace as X16 ran it; and Big scanned alone under the lifted pace, nothing of it resident
plan_round() {
    order=key; round=$1
    prefix=x17-key-r$round
    [ -z "${OVERWRITE:-}" ] && [ -e "$LAB/captures/$prefix-load" ] \
        && die "$LAB/captures/$prefix-load is there already: another LAB, or OVERWRITE=1 to take it again"
    inventory "$LAB/inventories/x17.yml"
    roots_ok
    no_bench_cluster
    switches key 16 4096 one capture
    echo "== $(date -u +%FT%TZ) bootstrapping the cluster for $prefix"
    CURRENT=x17
    booted=$(date -u '+%Y-%m-%d %H:%M:%S')
    x17_cmd bootstrap -i "$LAB/inventories/x17.yml" || die "the cluster did not bootstrap"
    capture "$prefix-load" "$LOAD" "$DATASET" "$PLAN_PRELOAD"
    merges_since "$booted UTC" "$prefix-load"
    if [ $((round % 2)) -eq 1 ]; then pr_modes="capture plan"; else pr_modes="plan capture"; fi
    for pr_mode in $pr_modes; do
        switches key 16 4096 one "$pr_mode"
        inventory "$LAB/inventories/x17.yml"
        reconfigure_settled
        capture "$prefix-page-$pr_mode-keyword" "$PLAN_COMMON --events scan --event-table Keyword --scan-tables Keyword $CELL_TIMES" \
            "$DATASET" "$PLAN_PRELOAD"
        switches_said "$prefix-page-$pr_mode-keyword"
        inventory "$LAB/inventories/x17.yml" "$OPEN_PACE"
        reconfigure_settled
        capture "$prefix-page-$pr_mode-big" "--preloaded --scan-tables Big $SCAN_TIMES --runs 1" "$DATASET" "$PLAN_PRELOAD"
        switches_said "$prefix-page-$pr_mode-big"
    done
    teardown
}

# Q35's benchmark on one core of europa and of titan, the hosts held as a run holds them: Big's
# first 200,000 rows and every Keyword row, rows encoded on the shard against walked partitions
cmd_encode() {
    spike_ok
    ce_bin=$LAB/wt-spike/target/deploy/release/x17-encode
    [ -x "$ce_bin" ] || die "build x17-encode in $LAB/wt-spike first (--features encode)"
    mkdir -p "$LAB/encode" "$LAB/encode-data"
    head -n 200000 "$DATASET/Big.jsonl" > "$LAB/encode-data/Big.jsonl"
    cp "$DATASET/Keyword.jsonl" "$LAB/encode-data/Keyword.jsonl"
    hold
    trap 'restore' EXIT
    # europa: a cpu clear of every node's, its own build
    taskset -c 12 "$ce_bin" --dataset "$LAB/encode-data" --label europa > "$LAB/encode/europa.tsv" || die "europa's encode failed"
    # titan: the binary and the rows copied to a directory of its own, removed after
    ssh -o BatchMode=yes titan "mkdir -p /var/tmp/x17-encode" && changed "titan: /var/tmp/x17-encode made"
    scp -q "$ce_bin" "$LAB/encode-data/Big.jsonl" "$LAB/encode-data/Keyword.jsonl" titan:/var/tmp/x17-encode/ || die "the copy to titan failed"
    ssh -o BatchMode=yes titan "taskset -c 7 /var/tmp/x17-encode/x17-encode --dataset /var/tmp/x17-encode --label titan" \
        > "$LAB/encode/titan.tsv" || die "titan's encode failed"
    ssh -o BatchMode=yes titan "rm -rf /var/tmp/x17-encode" && changed "titan: /var/tmp/x17-encode removed"
    cat "$LAB/encode/europa.tsv" "$LAB/encode/titan.tsv"
}

cmd_run() {
    [ -s "$DATASET/Big.jsonl" ] || die "no dataset at $DATASET"
    [ -s "$CHURN_DATASET/Big.jsonl" ] || die "no churn dataset at $CHURN_DATASET: its README says how it was written"
    [ -n "${QUICK:-}" ] && ROUNDS=1
    spike_ok
    mkdir -p "$LAB/captures" "$LAB/inventories" "$(dirname "$CHANGES")"
    no_bench_cluster
    hold
    CURRENT=
    trap 'wait; teardown; restore' EXIT
    trap 'exit 143' INT TERM
    round=$ROUND_FROM
    while [ "$round" -lt $((ROUND_FROM + ROUNDS)) ]; do
        # key first in odd rounds, hash first in even ones
        if [ $((round % 2)) -eq 1 ]; then layouts="key hash"; else layouts="hash key"; fi
        FIRST=${layouts%% *}
        if [ -n "${CHURN_ONLY:-}" ]; then
            churn_round "$round"
        elif [ -n "${PLAN_ONLY:-}" ]; then
            plan_round "$round"
        else
            for layout in $layouts; do layout_round "$layout" "$round"; done
        fi
        round=$((round + 1))
    done
    echo "== $(date -u +%FT%TZ) every round done"
}

cmd_start() {
    mkdir -p "$LAB/run"
    cp "$0" "$LAB/run/x17-lab.sh"
    # refusing a manual stop or restart, as the stores legs' units do (Resolved #306), with a user
    # unit's open files raised past its soft 1024
    sudo systemd-run --unit=x17-lab --collect -p User="$(id -un)" -p Group="$(id -gn)" \
        -p KillMode=mixed -p TimeoutStopSec=600 -p RefuseManualStop=yes -p LimitNOFILE=524288 \
        -p WorkingDirectory="$REPO" --setenv=HOME="$HOME" --setenv=PATH="$PATH" \
        --setenv=LAB="$LAB" --setenv=OUT="$OUT" --setenv=ROUNDS="$ROUNDS" --setenv=DATASET="$DATASET" \
        --setenv=QUICK="${QUICK:-}" --setenv=OVERWRITE="${OVERWRITE:-}" --setenv=CHURN_ONLY="${CHURN_ONLY:-}" --setenv=ROUND_FROM="$ROUND_FROM" --setenv=PLAN_ONLY="${PLAN_ONLY:-}" \
        sh -c "exec sh $LAB/run/x17-lab.sh run >> $LAB/run/x17.log 2>&1"
    echo "started x17-lab; tail -f $LAB/run/x17.log"
}

cmd_stop() {
    st_main=$(systemctl show x17-lab -p MainPID --value)
    [ -n "$st_main" ] && [ "$st_main" != 0 ] || die "x17-lab is not running"
    # the script, then the bench under it, so its trap puts the hosts back once the bench exits
    kill -TERM "$st_main"
    for st_child in $(pgrep -P "$st_main"); do
        for st_bench in $st_child $(pgrep -P "$st_child"); do
            kill -INT "$st_bench" 2>/dev/null
        done
    done
    st_tries=0
    while [ "$(systemctl show x17-lab -p ActiveState --value)" != inactive ]; do
        st_tries=$((st_tries + 1)); [ $st_tries -lt 120 ] || die "x17-lab is still running"; sleep 5
    done
    tail -n 5 "$LAB/run/x17.log"
}

cmd_status() {
    systemctl show x17-lab -p ActiveState --value
    grep '^== ' "$LAB/run/x17.log" 2>/dev/null | tail -n 5
}

# every capture slimmed into the results, its per-second client series dropped and everything the
# report reads kept, and the profiles and idle minutes beside them
cmd_fetch() {
    mkdir -p "$OUT/x17"
    for dir in "$LAB"/captures/x17-*/; do
        [ -f "$dir/bench.json" ] || continue
        name=$(basename "$dir")
        x17_cmd bench slim "$dir" -o "$OUT/x17/$name.json" > /dev/null || die "$name did not slim"
    done
    for extra in perf idle merges switches encode; do
        [ -d "$LAB/$extra" ] && cp -r "$LAB/$extra" "$OUT/x17/"
    done
    ls "$OUT/x17" | wc -l
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
    encode) cmd_encode ;;
    *) die "snapshot | check | run | start | status | stop | fetch | verify | encode" ;;
esac
