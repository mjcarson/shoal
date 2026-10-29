#!/bin/sh
# Spike X14 on the lab: a small Ceph v20.2.0 (Tentacle) on europa, titan and hyperion, deployed
# by cephadm from the image the tag was released as, to watch what the reading of Ceph's source
# claims. Run from the repository root on europa, one subcommand at a time:
#
#   sh shoal-spike/results/x14-lab.sh snapshot          # every host's state before anything
#   sh shoal-spike/results/x14-lab.sh setup-tools       # cephadm, podman, the uid 167 user, checks
#   sh shoal-spike/results/x14-lab.sh setup-image       # the pinned image on every host
#   sh shoal-spike/results/x14-lab.sh setup-bootstrap   # the mon and mgr on europa
#   sh shoal-spike/results/x14-lab.sh setup-hosts       # titan and hyperion, their LVs and OSDs, RGW
#   sh shoal-spike/results/x14-lab.sh setup-pools       # one PG a pool, every kind the experiments use
#   sh shoal-spike/results/x14-lab.sh probe             # each pool's PG, primary and shard 2
#   sh shoal-spike/results/x14-lab.sh e1|e2|e3|e4|e5
#   sh shoal-spike/results/x14-lab.sh teardown    # everything setup did, undone
#   sh shoal-spike/results/x14-lab.sh verify      # every host diffed against its snapshot
#
# europa runs the one mon, the one mgr and the one RGW, in docker, which it already had. titan
# and hyperion each run three OSDs on 20 GiB LVs in ubuntu-vg, in podman, installed for this and
# purged after. Nothing touches `/`, `/xfs`, `/optane`, time sync, or titan's kubelet, etcd and
# containerd. Never `--all-available-devices` and never `--zap-osds`: the LVs share a PV with the
# root filesystem.
set -u

OUT=${OUT:-shoal-spike/results}
SNAP=${SNAP:-target/lab/x14/snap}
REMOTE=/var/tmp/x14
CEPHADM_URL=https://download.ceph.com/rpm-20.2.0/el9/noarch/cephadm
# the image v20.2.0 was released as, pulled by its digest where a host can reach quay.io. hyperion
# cannot reach the internet, so it is given the image saved on titan and loaded; a load keeps the
# image's layers but not its manifest's digest, so every host names it by the tag, the mgr is told
# not to turn tags into digests, and setup compares every host's layers
PINNED=quay.io/ceph/ceph@sha256:1228c3d05e45fbc068a8c33614e4409b6dac688bcc77369b06009b5830fa8d86
IMAGE=quay.io/ceph/ceph:v20.2.0
MON_IP=172.16.2.10
HOSTS="europa titan hyperion"
OSD_HOSTS="titan hyperion"
LVS="x14-osd0 x14-osd1 x14-osd2"

# run a command on a host: europa is this machine
on() {
    host=$1; shift
    if [ "$host" = europa ]; then sh -c "$*"; else ssh -o BatchMode=yes "$host" "$*"; fi
}

# the ceph cli, from the pinned image on europa; cephadm's own chatter goes to a log
ceph() {
    sudo "$REMOTE/cephadm" --image "$IMAGE" shell -- ceph "$@" 2>> target/lab/x14/ceph-stderr.log
}

# record one host's state to $SNAP/<host>-<label>/
snapshot_host() {
    host=$1; label=$2
    dir="$SNAP/$host-$label"
    mkdir -p "$dir"
    on "$host" "dpkg-query -W -f='\${Package} \${Version}\n'" > "$dir/dpkg.txt"
    on "$host" "systemctl list-unit-files --state=enabled --no-legend --plain | awk '{print \$1}' | sort" > "$dir/units-enabled.txt"
    on "$host" "systemctl list-units --state=running --no-legend --plain | awk '{print \$1}' | sort" > "$dir/units-running.txt"
    on "$host" "sudo lvs --noheadings -o vg_name,lv_name,lv_size; sudo vgs --noheadings -o vg_name,vg_size,vg_free" > "$dir/lvm.txt"
    on "$host" "lsblk -o NAME,SIZE,TYPE,MOUNTPOINT -n | grep -v '^loop'" > "$dir/lsblk.txt"
    on "$host" "sysctl fs.aio-max-nr kernel.pid_max" > "$dir/sysctl.txt"
    on "$host" "sha256sum ~/.ssh/authorized_keys 2>&1 | cut -c1-64; sudo sha256sum /root/.ssh/known_hosts 2>&1 | cut -c1-64" > "$dir/keys.txt"
    # /root is not readable as the user, so whether a path exists is asked through sudo
    on "$host" "for d in /root/.ssh /etc/ceph /var/lib/ceph /var/log/ceph /etc/containers /var/lib/containers /run/containers $REMOTE; do sudo test -e \$d && echo \"\$d present\" || echo \"\$d absent\"; done" > "$dir/dirs.txt"
    on "$host" "ls /etc/sysctl.d /etc/logrotate.d /etc/lvm/archive /etc/lvm/backup /etc/systemd/system 2>&1" > "$dir/etc.txt"
    on "$host" "getent passwd cephadm || echo 'no cephadm user'; ls /tmp/ssh_key_* 2>/dev/null || echo 'no /tmp/ssh_key_*'" > "$dir/misc.txt"
    on "$host" "ss -ltnH | awk '{print \$4}' | sort -u" > "$dir/listening.txt"
    if [ "$host" = europa ]; then
        sudo docker image ls --format '{{.Repository}}@{{.Digest}} {{.ID}}' | sort > "$dir/docker-images.txt"
        sudo docker ps -a --format '{{.Names}} {{.Image}}' | sort > "$dir/docker-ps.txt"
    fi
}

cmd_snapshot() {
    for h in $HOSTS; do
        snapshot_host "$h" before
        # the two files cephadm adds a line to are kept whole, to be put back exactly
        on "$h" "cp ~/.ssh/authorized_keys $REMOTE-authorized_keys.bak 2>/dev/null; sudo cp -p /root/.ssh/known_hosts $REMOTE-root-known_hosts.bak 2>/dev/null; true"
        echo "snapshot $h -> $SNAP/$h-before"
    done
}

# append a line to the record of what this run changed on the hosts
changed() {
    echo "$(date -u +%FT%TZ) $*" >> "$OUT/x14-host-changes.txt"
}

# the cephadm file on every host, podman on the OSD hosts, and each host checked
setup_tools() {
    mkdir -p target/lab/x14
    [ -f target/lab/x14/cephadm ] || curl -sfo target/lab/x14/cephadm "$CEPHADM_URL"
    sha=$(sha256sum target/lab/x14/cephadm | cut -c1-64)
    changed "cephadm $CEPHADM_URL sha256 $sha"
    for h in $HOSTS; do
        on "$h" "mkdir -p $REMOTE"
        if [ "$h" = europa ]; then cp target/lab/x14/cephadm "$REMOTE/cephadm"
        else scp -q target/lab/x14/cephadm "$h:$REMOTE/cephadm"; fi
        on "$h" "chmod 755 $REMOTE/cephadm"
        changed "$h: $REMOTE/cephadm"
    done
    for h in $OSD_HOSTS; do
        on "$h" "sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q --no-install-recommends podman catatonit" | tail -1
        changed "$h: apt-get install --no-install-recommends podman catatonit (see /var/log/apt/history.log)"
    done
    # Ubuntu 26.04's install(1) is uutils 0.8.0, which refuses a numeric owner that has no passwd
    # entry, and cephadm makes its run directory with `install -o 167 -g 167` (the image's ceph user)
    for h in $HOSTS; do
        on "$h" "getent passwd 167 || (sudo groupadd -r -g 167 cephx14 && sudo useradd -r -u 167 -g 167 -M -d /var/lib/ceph -s /usr/sbin/nologin cephx14)"
        changed "$h: groupadd -g 167 cephx14; useradd -u 167 cephx14"
    done
    for h in $HOSTS; do
        echo "== check-host $h"
        on "$h" "sudo $REMOTE/cephadm check-host --expect-hostname $h" 2>&1 | tail -4
    done
}

# the pinned image on every host, under the tag cephadm is given
setup_image() {
    sudo docker pull -q "$PINNED" && sudo docker tag "$PINNED" "$IMAGE"
    on titan "sudo podman pull -q $PINNED && sudo podman tag $PINNED $IMAGE"
    on titan "sudo podman save --format docker-dir -o $REMOTE/ceph-image-dir $PINNED && sudo chown -R \$(id -un) $REMOTE/ceph-image-dir"
    mkdir -p target/lab/x14/ceph-image-dir
    scp -q -r "titan:$REMOTE/ceph-image-dir/*" target/lab/x14/ceph-image-dir/
    on hyperion "mkdir -p $REMOTE/ceph-image-dir"
    scp -q -r target/lab/x14/ceph-image-dir/* "hyperion:$REMOTE/ceph-image-dir/"
    id=$(on titan "sudo podman image inspect --format '{{.Id}}' $PINNED")
    on hyperion "sudo podman load -q -i $REMOTE/ceph-image-dir && sudo podman tag $id $IMAGE"
    changed "europa: docker pull $PINNED, tagged $IMAGE"
    changed "titan: podman pull $PINNED, tagged $IMAGE; saved to $REMOTE/ceph-image-dir"
    changed "hyperion: podman load of titan's saved image $id, tagged $IMAGE"
    # the same two layers everywhere is the evidence the loaded image is the pinned one
    echo "europa $(sudo docker image inspect --format '{{json .RootFS.Layers}}' "$IMAGE" | sha256sum | cut -c1-16)"
    for h in $OSD_HOSTS; do
        echo "$h $(on "$h" "sudo podman image inspect --format '{{json .RootFS.Layers}}' $IMAGE" | sed 's/ //g' | sha256sum | cut -c1-16)"
    done
}

# the one mon and mgr on europa, from the pinned image, under a conf written for the lab's shape
setup_bootstrap() {
    cat > target/lab/x14/bootstrap.conf <<CONF
[global]
# two hosts carry the OSDs, so the default rule spreads replicas by OSD and not by host
osd_crush_chooseleaf_type = 0
# europa's root is 22% free, under the default warning of 30%
mon_data_avail_warn = 15
[mon]
mon_allow_pool_delete = true
# a grace that grows with every wrong mark-down would move E2's timings between trials
mon_osd_adjust_heartbeat_grace = false
[osd]
# titan and hyperion have 14 GiB for three OSDs; autotune would set a per-host target over this
osd_memory_target_autotune = false
osd_memory_target = 1610612736
CONF
    cp target/lab/x14/bootstrap.conf "$REMOTE/bootstrap.conf"
    sudo "$REMOTE/cephadm" --image "$IMAGE" bootstrap --mon-ip "$MON_IP" --ssh-user "$(id -un)" \
        --skip-pull --skip-dashboard --skip-monitoring-stack --config "$REMOTE/bootstrap.conf" \
        > target/lab/x14/bootstrap.log 2>&1
    rc=$?
    tail -5 target/lab/x14/bootstrap.log
    changed "europa: cephadm bootstrap (rc $rc), fsid $(sudo ls /var/lib/ceph 2>/dev/null | head -1)"
    # a tag turned into europa's repo digest would name an image hyperion's load does not carry
    ceph config set mgr mgr/cephadm/use_repo_digest false
    ceph config set global container_image "$IMAGE"
    ceph orch apply mon --placement=europa
    ceph orch apply mgr --placement=europa
    ceph versions
}

# titan and hyperion joined, and three OSDs on LVs of each
setup_hosts() {
    for h in $OSD_HOSTS; do
        ssh-copy-id -f -i /etc/ceph/ceph.pub "$h" > /dev/null 2>&1
        changed "$h: /etc/ceph/ceph.pub added to ~$(id -un)/.ssh/authorized_keys"
    done
    ceph orch host add titan 172.16.2.4
    ceph orch host add hyperion 172.16.2.5
    for h in $OSD_HOSTS; do
        for lv in $LVS; do
            on "$h" "sudo lvcreate -q -y -L 20G -n $lv ubuntu-vg"
            changed "$h: lvcreate ubuntu-vg/$lv 20G"
        done
    done
    # one spec a host naming its three LVs. `ceph orch daemon add osd <host>:<lv>` answers "No
    # devices found" until the manager has inventoried a new host, yet saves a spec that the next
    # add replaces, so one add an LV makes some of the OSDs and not others
    for h in $OSD_HOSTS; do
        printf 'service_type: osd\nservice_id: x14-%s\nplacement:\n  hosts: [%s]\nspec:\n  data_devices:\n    paths: [/dev/ubuntu-vg/x14-osd0, /dev/ubuntu-vg/x14-osd1, /dev/ubuntu-vg/x14-osd2]\n---\n' "$h" "$h"
    done > "$OUT/x14-osd-spec.yaml"
    tool ceph orch apply -i /x14/x14-osd-spec.yaml
    until [ "$(ceph osd stat -f json | python3 -c 'import json,sys; print(json.load(sys.stdin)["num_up_osds"])')" = 6 ]; do sleep 15; done
    ceph orch apply rgw x14 --placement=europa --port=8080
    ceph osd tree
}

# the experiments' pools, each of one PG so a PG's acting set is the pool's, every shard on an OSD
setup_pools() {
    ceph osd set noautoscale
    ceph osd set noscrub
    ceph osd set nodeep-scrub
    ceph osd erasure-code-profile set x14-42 k=4 m=2 crush-failure-domain=osd stripe_unit=4K
    ceph osd erasure-code-profile set x14-21 k=2 m=1 crush-failure-domain=osd stripe_unit=4K
    for km in 42 21; do
        for kind in plain legacy opt; do
            pool="e$km$kind"
            ceph osd pool create "$pool" 1 1 erasure "x14-$km" --autoscale-mode=off
            ceph osd pool application enable "$pool" rados
            # overwrites are a pool flag, and optimizations need them too to write in place
            [ "$kind" != plain ] && ceph osd pool set "$pool" allow_ec_overwrites true
            [ "$kind" = opt ] && ceph osd pool set "$pool" allow_ec_optimizations true --yes-i-really-mean-it
        done
    done
    ceph osd pool create rep3 1 1 replicated --autoscale-mode=off
    ceph osd pool set rep3 size 3
    ceph osd pool application enable rep3 rados
    ceph osd erasure-code-profile get x14-42
    ceph osd pool ls detail
}

# the host an OSD runs on, from the CRUSH map
osd_host() {
    [ -n "$1" ] || { echo "osd_host: no OSD named" >&2; exit 1; }
    ceph osd find "$1" -f json | python3 -c 'import json,sys; print(json.load(sys.stdin)["crush_location"]["host"])'
}

# the cluster's fsid, which names every unit cephadm makes
fsid() {
    sudo ls /var/lib/ceph | grep -E '^[0-9a-f-]{36}$' | head -1
}

# stop or start an OSD through its systemd unit, which the orchestrator's ok-to-stop does not see
osd_unit() {
    # cephadm's units allow five starts in thirty minutes, and E2 and E3 restart a primary for
    # every trial; a start the limit refused would look like an OSD that never came back
    on "$(osd_host "$2")" "sudo systemctl reset-failed ceph-$(fsid)@osd.$2.service; sudo systemctl $1 ceph-$(fsid)@osd.$2.service"
}

# wait until every OSD named is up (or down), as the monitors have it
wait_osds() {
    want=$1; shift
    for n in "$@"; do
        until ceph osd dump -f json | python3 -c "import json,sys; o=[x for x in json.load(sys.stdin)['osds'] if x['osd']==$n][0]; sys.exit(0 if o['up']==('$want'=='true') else 1)"; do sleep 2; done
    done
}

# wait until every PG is active+clean
wait_clean() {
    until ceph pg stat | grep -qE '^[0-9]+ pgs: [0-9]+ active\+clean;'; do sleep 3; done
}

# the state of the test pools' PGs, one line each
pg_states() {
    ceph pg ls -f json | python3 -c '
import json,sys
for p in json.load(sys.stdin)["pg_stats"]:
    if int(p["pgid"].split(".")[0]) >= 6:
        print(p["pgid"], p["state"], "acting", p["acting"], "primary", p["acting_primary"])'
}

# the experiments run the ceph tools from the pinned image with this directory mounted at /x14
tool() {
    sudo "$REMOTE/cephadm" --image "$IMAGE" shell --mount "$(pwd)/$OUT:/x14" -- "$@" 2>> target/lab/x14/ceph-stderr.log
}

# E1: a PG below min_size, and what it serves. osd.1 and osd.3 stopped put three pools' PGs below
# it at once: 6.0 (4+2, four of six left), 9.0 (2+1 raised to min_size 3, two of three left) and
# 12.0 (replicated, one of three left), and 7.0 and 8.0 (4+2 legacy and optimized) besides
e1() {
    log="$OUT/x14-e1-min-size.txt"
    : > "$log"
    for pool in e42plain e42legacy e42opt e21plain rep3; do
        tool python3 /x14/x14-rados.py fill "$pool" before 65536 1 >> "$log"
    done
    ceph osd set noout
    ceph osd pool set e21plain min_size 3
    echo "== $(date -u +%T) stopping osd.1 and osd.3 (titan) through systemd" >> "$log"
    osd_unit stop 1; osd_unit stop 3
    wait_osds false 1 3
    sleep 10
    echo "== $(date -u +%T) both down; min_size 6.0=5 7.0=5 8.0=5 9.0=3 12.0=2" >> "$log"
    pg_states >> "$log"
    for pool in e42plain e42legacy e42opt e21plain rep3; do
        start=$(date +%s)
        tool timeout 30 rados -p "$pool" get before /dev/null; rc=$?
        echo "get $pool/before rc $rc after $(( $(date +%s) - start )) s (124 is the 30 s timeout)" >> "$log"
        start=$(date +%s)
        tool timeout 30 rados -p "$pool" put during-peered /etc/hostname; rc=$?
        echo "put $pool/during-peered rc $rc after $(( $(date +%s) - start )) s" >> "$log"
    done
    echo "== $(date -u +%T) min_size lowered to k (EC) and 1 (replicated)" >> "$log"
    ceph osd pool set e42plain min_size 4; ceph osd pool set e42legacy min_size 4
    ceph osd pool set e42opt min_size 4; ceph osd pool set e21plain min_size 2
    ceph osd pool set rep3 min_size 1
    sleep 15
    pg_states >> "$log"
    for pool in e42plain e42legacy e42opt e21plain rep3; do
        start=$(date +%s)
        tool timeout 30 rados -p "$pool" get before /dev/null; rc=$?
        echo "get $pool/before rc $rc after $(( $(date +%s) - start )) s" >> "$log"
        start=$(date +%s)
        tool timeout 30 rados -p "$pool" put after-lowered /etc/hostname; rc=$?
        echo "put $pool/after-lowered rc $rc after $(( $(date +%s) - start )) s" >> "$log"
    done
    echo "== $(date -u +%T) restored: min_size back, osd.1 and osd.3 started" >> "$log"
    ceph osd pool set e42plain min_size 5; ceph osd pool set e42legacy min_size 5
    ceph osd pool set e42opt min_size 5; ceph osd pool set e21plain min_size 2
    ceph osd pool set rep3 min_size 2
    osd_unit start 1; osd_unit start 3
    wait_osds true 1 3
    wait_clean
    ceph osd unset noout
    pg_states >> "$log"
    for pool in e42plain e42legacy e42opt e21plain rep3; do
        tool python3 /x14/x14-rados.py get "$pool" before >> "$log"
    done
    cat "$log"
}

# the OSD at one shard of a pool's one PG, from the acting set, which is in shard order
shard_osd() {
    ceph pg map "$(pool_pg "$1")" -f json | python3 -c "import json,sys; print(json.load(sys.stdin)['acting'][$2])"
}

# a pool's one PG
pool_pg() {
    id=$(ceph osd pool ls detail -f json | python3 -c "import json,sys; print([p['pool_id'] for p in json.load(sys.stdin) if p['pool_name']=='$1'][0])")
    [ -n "$id" ] || { echo "no pool $1" >&2; exit 1; }
    echo "$id.0"
}

# stop or continue an OSD's process where it runs, as a hung disk or a hung host would stop it.
# A stopped OSD is continued after 60 s whatever happens, inside its op threads' 150 s timeout
osd_signal() {
    host=$(osd_host "$2")
    # the OSD itself, not the container's init that runs it
    pid=$(on "$host" "pgrep -f '^/usr/bin/ceph-osd -n osd.$2 '")
    [ -n "$pid" ] || { echo "osd_signal: no process for osd.$2" >&2; exit 1; }
    if [ "$1" = STOP ]; then
        on "$host" "sudo kill -STOP $pid; sudo setsid sh -c 'sleep 60; kill -CONT $pid' >/dev/null 2>&1 < /dev/null &"
    else
        on "$host" "sudo kill -CONT $pid"
    fi
}

# a pool's PG active+clean again after a trial
wait_pg_clean() {
    until ceph pg ls -f json | python3 -c "import json,sys; s=[p['state'] for p in json.load(sys.stdin)['pg_stats'] if p['pgid']=='$1'][0]; sys.exit(0 if s=='active+clean' else 1)"; do sleep 2; done
}

# E2: what a write waits for. The OSD holding data shard 2 of a 4+2 PG is stopped (SIGSTOP), and
# one 4 KiB write lands in data shard 1's unit or shard 2's, timed until it is acknowledged. The
# parity delta mode is set and the primary restarted before each trial, which also empties its
# extent cache, and every trial writes a stripe nothing has touched since
e2() {
    log="$OUT/x14-e2-ack.txt"
    : > "$log"
    trial=0
    for spec in "e42opt 2 1" "e42opt 0 1" "e42opt 1 1" "e42opt 2 2" "e42opt 0 2" "e42legacy 0 1" "e42legacy 0 2"; do
        set -- $spec; pool=$1; mode=$2; unit=$3
        trial=$((trial + 1))
        pg=$(pool_pg "$pool"); primary=$(shard_osd "$pool" 0); victim=$(shard_osd "$pool" 2)
        ceph config set osd ec_pdw_write_mode "$mode"
        tool python3 /x14/x14-rados.py fill "$pool" "e2-$trial" 1048576 "$trial" > /dev/null
        osd_unit restart "$primary"; wait_osds true "$primary"; wait_pg_clean "$pg"
        # a stripe of this object that the fill wrote and nothing has read since the restart
        off=$(( 7 * 16384 + unit * 4096 ))
        echo "== trial $trial: $pool (pg $pg, primary osd.$primary) ec_pdw_write_mode=$mode, 4 KiB into data shard $unit's unit; osd.$victim (shard 2) stopped at $(date -u +%T)" >> "$log"
        osd_signal STOP "$victim"
        tool python3 /x14/x14-rados.py write rep3 "e2-control-$trial" 0 4096 "$trial" | sed 's/^/control (rep3, no stopped OSD): /' >> "$log"
        tool python3 /x14/x14-rados.py write "$pool" "e2-$trial" "$off" 4096 "$trial" >> "$log"
        osd_signal CONT "$victim"
        ceph log last 30 cluster | grep -E "osd\.$victim (failed|marked|boot)|wrongly marked" | tail -3 >> "$log"
        wait_osds true "$victim"; wait_pg_clean "$pg"
        # a stopped OSD is marked down at most five times in ten minutes before it shuts itself down
        sleep 30
    done
    ceph config rm osd ec_pdw_write_mode
    cat "$log"
}

# every OSD's counters of one section, as one JSON object of OSD id to counters
perf_all() {
    for n in 0 1 2 3 4 5; do
        printf '%s\t' "$n"; ceph tell "osd.$n" perf dump -f json | tr -d '\n'; echo
    done
}

# E3: which shards a small overwrite writes. A thousand 4 KiB writes, one at a time, each into
# data shard 1's unit of a stripe nothing has touched since its primary restarted, with every
# OSD's counters read before and after and an idle stretch of the same length subtracted
e3() {
    log="$OUT/x14-e3-shards.txt"
    : > "$log"
    for spec in "e42legacy 0" "e42opt 0" "e42opt 1" "e42opt 2"; do
        set -- $spec; pool=$1; mode=$2
        pg=$(pool_pg "$pool"); primary=$(shard_osd "$pool" 0)
        acting=$(ceph pg map "$pg" -f json | python3 -c "import json,sys; print(','.join(map(str, json.load(sys.stdin)['acting'])))")
        ceph config set osd ec_pdw_write_mode "$mode"
        tool python3 /x14/x14-rados.py fill "$pool" "e3-$mode" 16777216 "$mode" > /dev/null
        osd_unit restart "$primary"; wait_osds true "$primary"; wait_pg_clean "$pg"
        perf_all > target/lab/x14/e3-idle0.tsv
        start=$(date +%s)
        result=$(tool python3 /x14/x14-rados.py writes "$pool" "e3-$mode" 1000 4096 16384 4096)
        took=$(( $(date +%s) - start ))
        perf_all > target/lab/x14/e3-run1.tsv
        sleep "$took"
        perf_all > target/lab/x14/e3-idle1.tsv
        echo "== $pool ec_pdw_write_mode=$mode, acting (shard order) [$acting], $result" >> "$log"
        python3 "$OUT/x14-perf-diff.py" "$acting" target/lab/x14/e3-idle0.tsv target/lab/x14/e3-run1.tsv target/lab/x14/e3-idle1.tsv >> "$log"
    done
    ceph config rm osd ec_pdw_write_mode
    cat "$log"
}

# change one byte of a shard in place through the object store tool, on a stopped OSD
# (the tool truncates and writes the shard again, so BlueStore keeps fresh checksums for it)
#   flip <osd> <pgid>s<shard> <object>        copy <osd> <pgid>s<shard> <from> <to>
e4_tool() {
    n=$1; host=$(osd_host "$n"); shift
    on "$host" "sudo mkdir -p $REMOTE/e4 && sudo $REMOTE/cephadm --image $IMAGE shell --name osd.$n --mount $REMOTE/e4:/mnt -- sh -c '$*'" 2>> target/lab/x14/ceph-stderr.log
}

# E4: what a deep scrub of an EC pool finds. On each 2+1 pool: A1 has one byte of data shard 1
# changed, A2 one byte of parity shard 2, and A3 every shard replaced by B's, all through the
# object store, so every shard reads back clean from BlueStore
e4() {
    log="$OUT/x14-e4-scrub.txt"
    : > "$log"
    for pool in e21plain e21legacy e21opt; do
        for o in A1:11 A2:12 A3:13 B:14; do
            tool python3 /x14/x14-rados.py fill "$pool" "${o%%:*}" 65536 "${o#*:}" >> "$log"
        done
    done
    run=$(date +%s)
    ceph osd set noout
    # every pool's PG and acting set, read while every OSD is up: once an OSD is stopped its
    # place in the acting set is a hole, and its shard can no longer be read off it
    plan=$(for pool in e21plain e21legacy e21opt; do
        pg=$(pool_pg "$pool")
        echo "$pool:$pg:$(ceph pg map "$pg" -f json | python3 -c "import json,sys; print(','.join(map(str, json.load(sys.stdin)['acting'])))")"
    done)
    echo "plan (pool:pg:acting) $plan" | tr '\n' ' ' >> "$log"; echo >> "$log"
    osds=$(echo "$plan" | cut -d: -f3 | tr ',' '\n' | sort -u)
    for n in $osds; do
        osd_unit stop "$n"; wait_osds false "$n"
        for entry in $plan; do
            pool=${entry%%:*}; rest=${entry#*:}; pg=${rest%%:*}; act=${rest#*:}
            shard=$(python3 -c "a=[$act]; print(a.index($n) if $n in a else -1)")
            [ "$shard" = -1 ] && continue
            t="ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-$n --pgid ${pg}s$shard"
            flip="python3 -c \"import sys; b=bytearray(open(sys.argv[1],\\\"rb\\\").read()); b[100]^=0xff; open(sys.argv[1],\\\"wb\\\").write(b)\""
            # get-bytes will not overwrite a file, so each read has a name of its own this run
            f="/mnt/$run-osd$n-$pg-s$shard"
            if [ "$shard" = 1 ]; then
                e4_tool "$n" "$t A1 get-bytes $f-A1 && $flip $f-A1 && $t A1 set-bytes $f-A1" && r=done || r=FAILED
                echo "osd.$n: $pool pg $pg shard 1: A1 one byte flipped: $r" >> "$log"
            fi
            if [ "$shard" = 2 ]; then
                e4_tool "$n" "$t A2 get-bytes $f-A2 && $flip $f-A2 && $t A2 set-bytes $f-A2" && r=done || r=FAILED
                echo "osd.$n: $pool pg $pg shard 2: A2 one byte flipped: $r" >> "$log"
            fi
            e4_tool "$n" "$t B get-bytes $f-B && $t A3 set-bytes $f-B" && r=done || r=FAILED
            echo "osd.$n: $pool pg $pg shard $shard: A3 given B's shard: $r" >> "$log"
        done
        osd_unit start "$n"; wait_osds true "$n"
    done
    wait_clean
    ceph osd unset noout
    for pool in e21plain e21legacy e21opt; do
        pg=$(pool_pg "$pool")
        before=$(ceph pg "$pg" query -f json | python3 -c "import json,sys; print(json.load(sys.stdin)['info']['stats']['last_deep_scrub_stamp'])")
        ceph pg deep-scrub "$pg"
        until [ "$(ceph pg "$pg" query -f json | python3 -c "import json,sys; print(json.load(sys.stdin)['info']['stats']['last_deep_scrub_stamp'])")" != "$before" ]; do sleep 3; done
        echo "== $pool pg $pg deep-scrubbed; state $(ceph pg ls -f json | python3 -c "import json,sys; print([p['state'] for p in json.load(sys.stdin)['pg_stats'] if p['pgid']=='$pg'][0])")" >> "$log"
        tool rados list-inconsistent-obj "$pg" --format=json-pretty >> "$log"
        for o in A1 A2 A3; do
            tool python3 /x14/x14-rados.py get "$pool" "$o" >> "$log"
        done
    done
    cat "$log"
}

# E5: RGW's tail after an overwrite. A 6 MiB object in one PUT, overwritten by another, and the
# garbage collector's queue read with and without entries not yet due
e5() {
    log="$OUT/x14-e5-rgw-gc.txt"
    : > "$log"
    keys=$(tool radosgw-admin user create --uid x14 --display-name x14 | python3 -c "import json,sys; k=json.load(sys.stdin)['keys'][0]; print(k['access_key'], k['secret_key'])")
    set -- $keys
    python3 "$OUT/x14-s3.py" "http://$MON_IP:8080" "$1" "$2" put >> "$log"
    echo "== data pool after the first PUT" >> "$log"
    tool rados -p default.rgw.buckets.data ls | sort >> "$log"
    echo "== gc list --include-all after the first PUT" >> "$log"
    tool radosgw-admin gc list --include-all >> "$log"
    python3 "$OUT/x14-s3.py" "http://$MON_IP:8080" "$1" "$2" overwrite >> "$log"
    echo "== data pool after the overwrite" >> "$log"
    tool rados -p default.rgw.buckets.data ls | sort >> "$log"
    echo "== gc list (due entries only) after the overwrite" >> "$log"
    tool radosgw-admin gc list >> "$log"
    echo "== gc list --include-all after the overwrite" >> "$log"
    tool radosgw-admin gc list --include-all >> "$log"
    echo "== rgw_gc_obj_min_wait $(ceph config get client.rgw rgw_gc_obj_min_wait) rgw_gc_processor_period $(ceph config get client.rgw rgw_gc_processor_period)" >> "$log"
    cat "$log"
}

# everything setup did, undone, host by host, back to the snapshot. Every path is spelled out
teardown() {
    f=$(fsid)
    [ -n "$f" ] || { echo "teardown: no cluster fsid under /var/lib/ceph" >&2; exit 1; }
    for h in $HOSTS; do
        # the cluster's units, data, logs, sysctl and logrotate files, and /etc/ceph's files.
        # Not --zap-osds: the LVs share a PV with the root filesystem and are removed by name
        on "$h" "sudo /var/tmp/x14/cephadm rm-cluster --fsid $f --force"
        changed "$h: cephadm rm-cluster --fsid $f --force"
    done
    for h in $OSD_HOSTS; do
        for lv in $LVS; do
            on "$h" "sudo lvremove -q -y ubuntu-vg/$lv"
        done
        on "$h" "sudo podman system reset --force"
        changed "$h: lvremove ubuntu-vg/x14-osd0..2; podman system reset"
    done
    on titan "sudo DEBIAN_FRONTEND=noninteractive apt-get purge -y -q podman catatonit conmon crun netavark golang-github-containers-common golang-github-containers-image libgpgme45 libsubid5 libyajl2"
    on hyperion "sudo dpkg --purge podman catatonit conmon crun netavark golang-github-containers-common golang-github-containers-image libgpgme45 libsubid5 libyajl2"
    changed "titan, hyperion: the ten packages purged"
    sudo docker image rm quay.io/ceph/ceph:v20.2.0 quay.io/ceph/ceph@sha256:1228c3d05e45fbc068a8c33614e4409b6dac688bcc77369b06009b5830fa8d86
    changed "europa: the ceph image removed from docker"
    for h in $HOSTS; do
        # the key cephadm added, the file restored from the copy the snapshot kept
        on "$h" "cp /var/tmp/x14-authorized_keys.bak ~/.ssh/authorized_keys && rm /var/tmp/x14-authorized_keys.bak"
        # root had no known_hosts before; cephadm's self-test made one on europa
        on "$h" "sudo rm -f /root/.ssh/known_hosts /var/tmp/x14-root-known_hosts.bak /tmp/ssh_key_*"
        on "$h" "sudo userdel cephx14; sudo groupdel cephx14 2>/dev/null; true"
        on "$h" "sudo sysctl -q -w fs.aio-max-nr=65536"
        on "$h" "sudo systemctl daemon-reload; sudo systemctl reset-failed"
        on "$h" "sudo rm -rf /var/tmp/x14 /etc/containers /var/lib/containers /run/containers"
        on "$h" "sudo rmdir /etc/ceph /var/lib/ceph /var/log/ceph 2>/dev/null; true"
        changed "$h: authorized_keys restored, root known_hosts removed, user cephx14 removed, fs.aio-max-nr 65536, /var/tmp/x14 and container and ceph directories removed"
    done
}

cmd_verify() {
    status=0
    for h in $HOSTS; do
        snapshot_host "$h" after
        if diff -r "$SNAP/$h-before" "$SNAP/$h-after" > "$SNAP/$h.diff"; then
            echo "$h: identical to its snapshot"
        else
            echo "$h: differs from its snapshot (listening ports and running units move on their own):"
            cat "$SNAP/$h.diff"
            status=1
        fi
    done
    return $status
}

case "${1:-}" in
    snapshot) cmd_snapshot ;;
    teardown) teardown ;;
    setup-tools) setup_tools ;;
    setup-image) setup_image ;;
    setup-bootstrap) setup_bootstrap ;;
    setup-hosts) setup_hosts ;;
    setup-pools) setup_pools ;;
    probe) for p in e42opt e42legacy e21plain rep3; do echo "$p $(pool_pg $p) primary osd.$(shard_osd $p 0) shard2 osd.$(shard_osd $p 2) on $(osd_host $(shard_osd $p 2))"; done ;;
    e1) e1 ;;
    e2) e2 ;;
    e3) e3 ;;
    e4) e4 ;;
    e5) e5 ;;
    verify) cmd_verify ;;
    *) echo "usage: $0 snapshot|setup-tools|setup-image|setup-bootstrap|setup-hosts|setup-pools|probe|e1|e2|e3|e4|e5|teardown|verify" >&2; exit 2 ;;
esac
