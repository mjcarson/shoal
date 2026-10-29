#!/bin/sh
# Spike X20's run on the lab: a three node ScyllaDB 2026.3.4, the release X20 read, in containers on
# europa (docker, which it has), titan and hyperion (podman, installed for this and purged after),
# and X16's foreground beside a tablet-era full scan, both from x20/x20_client.py in a container of
# their own on europa (docs/src/future-work/scans/scylladb-scan-sources.md). Run from the repository
# root on europa, one subcommand at a time:
#
#   sh shoal-spike/results/x20-lab.sh snapshot       # every host's state, before anything
#   sh shoal-spike/results/x20-lab.sh setup-tools    # podman on titan and hyperion
#   sh shoal-spike/results/x20-lab.sh setup-image    # the release's image on every host, the client's on europa
#   sh shoal-spike/results/x20-lab.sh up             # the hosts held, three nodes started one at a time
#   sh shoal-spike/results/x20-lab.sh load           # the keyspace, tablets on, and its two tables filled
#   sh shoal-spike/results/x20-lab.sh run            # ROUNDS rounds, each after a restart of every node
#   SELECT=rows ROUNDS=2 sh shoal-spike/results/x20-lab.sh run   # the scan carrying every row to the client
#   sh shoal-spike/results/x20-lab.sh down           # the nodes and their data gone, the hosts put back
#   sh shoal-spike/results/x20-lab.sh teardown       # podman purged, the images removed
#   sh shoal-spike/results/x20-lab.sh verify         # every host diffed against its snapshot
#
# Every node's data is a directory of its own on the host's SSD, /optane/scylla-x20 on europa and
# /xfs/scylla-x20 on titan and hyperion, on the cpus X16's nodes ran on. hyperion cannot reach the
# internet, so it is given the image titan pulled. The tmdb node, the hosts' maintenance timers and
# their governors are held from `up` to `down`; every change is recorded in x20-host-changes.txt.
# Nothing touches /, /xfs or /optane beyond the two directories, or titan's kubelet, etcd and
# containerd.
set -u

REPO=$(pwd)
LAB=${LAB:-target/lab/x20}
OUT=${OUT:-shoal-spike/results}
case $LAB in /*) ;; *) LAB=$REPO/$LAB ;; esac
case $OUT in /*) ;; *) OUT=$REPO/$OUT ;; esac
SNAP=$LAB/snap
CHANGES=$OUT/x20-host-changes.txt
REMOTE=/var/tmp/x20
IMAGE=docker.io/scylladb/scylla:2026.3.4
CLIENT=x20-client
HOSTS="europa titan hyperion"
ROUNDS=${ROUNDS:-4}
# what the scan carries to the client: the keys alone, as X16's carried keys and counts, or the rows
SELECT=${SELECT:-keys}
SEED=172.16.2.10
ADDRESSES=172.16.2.10,172.16.2.4,172.16.2.5
# the client's cpus on europa, clear of the node's (1-6), as X16's driver was
DRIVER_CPUS=12-15,28-31

ip_of() {
    case $1 in europa) echo 172.16.2.10 ;; titan) echo 172.16.2.4 ;; hyperion) echo 172.16.2.5 ;; esac
}

# a host's container runtime: europa's docker, titan's and hyperion's podman
runtime() {
    case $1 in europa) echo docker ;; *) echo podman ;; esac
}

# a host's node: its data directory, its cpus and its shard count, as X16's ran
data_of() { case $1 in europa) echo /optane/scylla-x20 ;; *) echo /xfs/scylla-x20 ;; esac; }
cpus_of() { case $1 in europa) echo 1-6 ;; *) echo 2-6 ;; esac; }
smp_of() { case $1 in europa) echo 6 ;; *) echo 5 ;; esac; }

# run a script on a host, fed on stdin to `sh -s` so no login shell reads it; europa is here
on() {
    host=$1; shift
    if [ "$host" = europa ]; then printf '%s\n' "$*" | sh -s; else printf '%s\n' "$*" | ssh -o BatchMode=yes "$host" sh -s; fi
}

changed() {
    echo "$(date -u +%FT%TZ) $*" >> "$CHANGES"
}

die() {
    echo "x20-lab: $*" >&2
    exit 1
}

# ---------------------------------------------------------------------------------------------
# a host's state, before and after

snapshot_host() {
    host=$1; label=$2
    dir="$SNAP/$host-$label"
    mkdir -p "$dir"
    on "$host" "dpkg-query -W -f='\${Package} \${Version}\n'" > "$dir/dpkg.txt"
    on "$host" "systemctl list-unit-files --state=enabled --no-legend --plain | awk '{print \$1}' | sort" > "$dir/units-enabled.txt"
    on "$host" "ss -ltnH | awk '{print \$4}' | sort -u" > "$dir/listening.txt"
    on "$host" "for d in $(data_of "$host") /etc/containers /var/lib/containers $REMOTE; do sudo test -e \$d && echo \"\$d present\" || echo \"\$d absent\"; done" > "$dir/dirs.txt"
    on "$host" "cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor; systemctl is-active shoal-tmdb; \
        systemctl is-active e2scrub_all.timer fstrim.timer apt-daily.timer apt-daily-upgrade.timer" > "$dir/state.txt"
    if [ "$host" = europa ]; then
        sudo docker image ls --format '{{.Repository}}:{{.Tag}} {{.ID}}' | sort > "$dir/docker-images.txt"
        sudo docker ps -a --format '{{.Names}} {{.Image}}' | sort > "$dir/docker-ps.txt"
    fi
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
        # titan's etcd times out at its start every 100 seconds, its peers long gone (20,175 restarts
        # in 23 days by 2026-10-29), and its ports close in each gap: a sample says nothing of X20
        if diff -r -I ':2379$' -I ':2380$' "$SNAP/$h-before" "$SNAP/$h-after" > "$SNAP/$h.diff"; then
            echo "$h: identical to its snapshot"
        else
            echo "$h: differs from its snapshot:"; cat "$SNAP/$h.diff"; status=1
        fi
    done
    return $status
}

# ---------------------------------------------------------------------------------------------
# the tools and the images

# podman on the two hosts without it, and exactly which packages that added, for teardown to purge
cmd_setup_tools() {
    mkdir -p "$LAB"
    for h in titan hyperion; do
        # sorted here, in one collation, which comm needs: a host's sort may collate otherwise
        on "$h" "dpkg-query -W -f='\${Package}\n'" | LC_ALL=C sort -u > "$LAB/packages-$h-before.txt"
        on "$h" "sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q --no-install-recommends podman catatonit" | tail -1
        on "$h" "dpkg-query -W -f='\${Package}\n'" | LC_ALL=C sort -u > "$LAB/packages-$h-after.txt"
        LC_ALL=C comm -13 "$LAB/packages-$h-before.txt" "$LAB/packages-$h-after.txt" > "$LAB/packages-$h-added.txt"
        changed "$h: apt-get install --no-install-recommends podman catatonit, adding $(tr '\n' ' ' < "$LAB/packages-$h-added.txt")"
    done
}

cmd_setup_image() {
    mkdir -p "$LAB/image"
    sudo docker pull -q "$IMAGE" || die "europa could not pull $IMAGE"
    changed "europa: docker pull $IMAGE"
    on titan "sudo podman pull -q $IMAGE && mkdir -p $REMOTE && sudo podman save -o $REMOTE/scylla.tar $IMAGE && sudo chown \$(id -un) $REMOTE/scylla.tar" \
        || die "titan could not pull $IMAGE"
    changed "titan: podman pull $IMAGE; saved to $REMOTE/scylla.tar"
    scp -q "titan:$REMOTE/scylla.tar" "$LAB/image/scylla.tar"
    on hyperion "mkdir -p $REMOTE"
    scp -q "$LAB/image/scylla.tar" "hyperion:$REMOTE/scylla.tar"
    on hyperion "sudo podman load -q -i $REMOTE/scylla.tar"
    changed "hyperion: podman load of titan's saved $IMAGE from $REMOTE/scylla.tar"
    # one image on every host: the same layers by its id
    for h in $HOSTS; do
        echo "$h $(on "$h" "sudo $(runtime "$h") image inspect --format '{{.Id}}' $IMAGE" | sed 's/^sha256://' | cut -c1-16)"
    done
    sudo docker build -q -t "$CLIENT" shoal-spike/results/x20 || die "the client image did not build"
    changed "europa: docker build $CLIENT from shoal-spike/results/x20 (python:3.12-slim, scylla-driver 3.29.12)"
}

# ---------------------------------------------------------------------------------------------
# the hosts held, and the nodes

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

# a node's container started on its host, its data in its directory, on its cpus
start_node() {
    h=$1
    ip=$(ip_of "$h")
    on "$h" "sudo $(runtime "$h") run -d --name x20-scylla --network host -v $(data_of "$h"):/var/lib/scylla $IMAGE \
        --smp $(smp_of "$h") --cpuset $(cpus_of "$h") --memory 6G --overprovisioned 0 --developer-mode 1 \
        --listen-address $ip --rpc-address $ip --broadcast-address $ip --broadcast-rpc-address $ip \
        --seeds $SEED --api-address 127.0.0.1 > /dev/null" || die "$h's node did not start"
}

# nodetool on a host's node
nodetool() {
    h=$1; shift
    on "$h" "sudo $(runtime "$h") exec x20-scylla nodetool $*"
}

# wait until this many nodes are up and normal, as europa's node sees them
wait_up() {
    want=$1
    tries=0
    until [ "$(nodetool europa status 2>/dev/null | grep -c '^UN ')" -ge "$want" ]; do
        tries=$((tries + 1)); [ $tries -lt 120 ] || die "fewer than $want nodes up after ten minutes"
        sleep 5
    done
}

cmd_up() {
    hold
    for h in $HOSTS; do
        on "$h" "sudo install -d -m 0777 $(data_of "$h")"
        changed "$h: $(data_of "$h") made for the node's data"
    done
    # one at a time, the seed first, as a ScyllaDB cluster joins
    n=0
    for h in $HOSTS; do
        start_node "$h"
        changed "$h: container x20-scylla started from $IMAGE"
        n=$((n + 1))
        wait_up $n
        echo "$h up: $n of 3"
    done
    nodetool europa status
}

cmd_load() {
    mkdir -p "$LAB/runs"
    sudo docker run --rm --network host --cpuset-cpus "$DRIVER_CPUS" "$CLIENT" load --hosts "$ADDRESSES" \
        || die "the load failed"
    for h in $HOSTS; do nodetool "$h" flush > /dev/null; done
    echo "every node flushed"
}

# every node restarted, so a round starts from its sstables with an empty row cache, and settled
restart_settled() {
    for h in $HOSTS; do
        on "$h" "sudo $(runtime "$h") restart -t 120 x20-scylla > /dev/null"
        sleep 20
    done
    wait_up 3
    sleep 30
}

cmd_run() {
    mkdir -p "$LAB/runs"
    round=1
    while [ "$round" -le "$ROUNDS" ]; do
        [ -e "$LAB/runs/r$round-$SELECT.json" ] && [ -z "${OVERWRITE:-}" ] && die "$LAB/runs/r$round-$SELECT.json is there already"
        echo "== $(date -u +%FT%TZ) round $round: restarting every node"
        restart_settled
        echo "== $(date -u +%FT%TZ) round $round"
        sudo docker run --rm --network host --cpuset-cpus "$DRIVER_CPUS" -v "$LAB/runs:/out" "$CLIENT" run \
            --hosts "$ADDRESSES" --select "$SELECT" --out "/out/r$round-$SELECT.json" || die "round $round failed"
        # a round whose scan read nothing, or failed a tablet, is no round
        python3 -I -c "import json, sys; scan = json.load(open(sys.argv[1]))['scan']; sys.exit(0 if scan['rows'] > 0 and scan['errors'] == 0 else 1)" \
            "$LAB/runs/r$round-$SELECT.json" || die "round $round's scan read nothing or failed a tablet"
        round=$((round + 1))
    done
}

cmd_down() {
    for h in $HOSTS; do
        on "$h" "sudo $(runtime "$h") rm -f x20-scylla > /dev/null 2>&1; sudo rm -rf $(data_of "$h")"
        changed "$h: container x20-scylla removed; $(data_of "$h") removed"
    done
    restore
}

cmd_teardown() {
    for h in titan hyperion; do
        added=$(tr '\n' ' ' < "$LAB/packages-$h-added.txt")
        [ -n "$added" ] || die "no record of what setup-tools added on $h"
        on "$h" "sudo podman system reset --force > /dev/null 2>&1; rm -rf $REMOTE"
        on "$h" "sudo DEBIAN_FRONTEND=noninteractive apt-get purge -y -q $added 2>&1 | tail -1"
        changed "$h: podman system reset; $added purged; $REMOTE removed"
        # the reset makes its empty storage again after it resets, which no package owns
        if grep -qx '/var/lib/containers absent' "$SNAP/$h-before/dirs.txt"; then
            on "$h" "sudo rm -rf /var/lib/containers"
            changed "$h: /var/lib/containers removed, absent before X20"
        fi
    done
    sudo docker rmi "$CLIENT" "$IMAGE" > /dev/null
    # the client's base image, where a build left it tagged
    sudo docker rmi python:3.12-slim > /dev/null 2>&1
    # and any client image a rebuild left untagged, and the build cache
    sudo docker image prune -f > /dev/null
    sudo docker builder prune -f > /dev/null
    rm -rf "$LAB/image"
    changed "europa: docker rmi $CLIENT $IMAGE, and python:3.12-slim where tagged; build cache pruned"
}

case ${1:-} in
    snapshot) cmd_snapshot ;;
    verify) cmd_verify ;;
    setup-tools) cmd_setup_tools ;;
    setup-image) cmd_setup_image ;;
    up) cmd_up ;;
    load) cmd_load ;;
    run) cmd_run ;;
    down) cmd_down ;;
    teardown) cmd_teardown ;;
    *) die "snapshot | setup-tools | setup-image | up | load | run | down | teardown | verify" ;;
esac
