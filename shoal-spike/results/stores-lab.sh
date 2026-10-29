#!/bin/sh
# F94 on the lab: Shoal's object store beside Ceph's RGW, MinIO, RustFS and SeaweedFS, every store
# driven by the bench's one object driver (`shoaladm bench run` for Shoal, `shoaladm bench s3` for
# the others) over the same dataset, spec and arms, on the same devices made fresh for each store
# (docs/src/object-storage/store-comparison.md). Run from the repository root on europa:
#
#   sh shoal-spike/results/stores-lab.sh snapshot              # every host's state, before anything
#   sh shoal-spike/results/stores-lab.sh stage                 # every store's programs on every host
#   sh shoal-spike/results/stores-lab.sh devices ssd|hdd|loop  # a leg's devices made
#   sh shoal-spike/results/stores-lab.sh quick ssd             # one small arm against each store
#   sh shoal-spike/results/stores-lab.sh start ssd             # a leg's two groups, in a unit
#   sh shoal-spike/results/stores-lab.sh status                # how far the unit is
#   sh shoal-spike/results/stores-lab.sh stop ssd              # a leg ended early, its hosts put back
#   RESUME=1 sh shoal-spike/results/stores-lab.sh start ssd    # and started again past what finished
#   sh shoal-spike/results/stores-lab.sh fetch ssd             # its captures slimmed into results
#   sh shoal-spike/results/stores-lab.sh devices-down ssd      # a leg's devices removed
#   sh shoal-spike/results/stores-lab.sh warp                  # warp's figures beside the driver's
#   AB_before=<wt> AB_after=<wt> AB_ORDER="before after after before" FLAGS="<spec and arms>" \
#       sh shoal-spike/results/stores-lab.sh ab-start ssd rep3 <tag>   # Shoal before and after a change
#   sh shoal-spike/results/stores-lab.sh ab-fetch <tag>        # its captures slimmed into results
#   sh shoal-spike/results/stores-lab.sh unstage && sh shoal-spike/results/stores-lab.sh verify
#
# One store at a time runs on a leg's devices, each made fresh before it: a 100 GiB LV of the
# 970 EVO on titan and hyperion and an LV of a loop over a file on the Optane on europa (the ssd
# leg); a directory on each host's disk and, for Ceph, an LV of a loop over a file on it (the hdd
# leg, the ssd leg's device being Shoal's journal and Ceph's DB); three loops on europa's Optane
# (the loop leg, three instances of each store on 127.0.0.11-13). The tmdb node is stopped for a
# leg and started again after it, every governor set to performance and put back, the unattended
# upgrades held, and every change to a host recorded in stores-host-changes.txt, which `verify`
# checks against the snapshot.
#
# Never edit this file while a leg runs: `start` runs a copy of it, but `fetch` and the per-store
# commands read this one. Never touch /, /xfs, /xfs2, /optane/shoal-tmdb*, /optane2,
# /hdd/shoal-tmdb-bulk, or titan's kubelet, etcd and containerd.
#
# A leg's unit refuses a manual stop and a restart: `stop` ends a leg, and a restart once killed a
# run's bench with no trap and ran the run again over the cluster it left, whose devices it then
# discarded under its nodes (Resolved #306). Nothing is made fresh while a host still mounts it or
# runs a cluster the bench made, and no capture of a before and after is taken again over one that
# is there.
set -u

REPO=$(pwd)
OUT=${OUT:-shoal-spike/results}
LAB=${LAB:-target/lab/stores}
# both absolute, so a leg run from a worktree of the commit finds the programs and keeps its
# captures and its record of host changes in the checkout it was started from, never the worktree,
# which the bench refuses to measure once anything in it changed
case $LAB in /*) ;; *) LAB=$REPO/$LAB ;; esac
case $OUT in /*) ;; *) OUT=$REPO/$OUT ;; esac
SNAP=$LAB/snap
REMOTE=/var/tmp/stores
CHANGES=$OUT/stores-host-changes.txt
HOSTS="europa titan hyperion"
# the store's key, the same for every store; the lab is a private network and every store plaintext
export AWS_ACCESS_KEY_ID=stores-bench
export AWS_SECRET_ACCESS_KEY=stores-bench-secret-f94
# every arm the comparison runs, the same flags to `bench run` and `bench s3`
SPEC_FLAGS="--dataset shoal-spike/results/stores-objects --objects put100,get100,rw50,stat100 \
--object-sizes 4KiB,64KiB,1MiB,16MiB --concurrency 1,16 --only put100/,get100/,rw50/,stat100/s4k/ \
--preload 1024 --duration 30 --warmup 5 --runs 4 --drop-caches --max-used 80"
# the loop leg asks what the software does without the 1 GbE link, at two sizes
LOOP_SPEC_FLAGS="--dataset shoal-spike/results/stores-objects --objects put100,get100,rw50,stat100 \
--object-sizes 4KiB,1MiB --concurrency 1,16 --only put100/,get100/,rw50/,stat100/s4k/ \
--preload 1024 --duration 30 --warmup 5 --runs 4 --drop-caches --max-used 80"
QUICK_FLAGS="--dataset shoal-spike/results/stores-objects --objects put100,get100,stat100 \
--object-sizes 64KiB --concurrency 4 --preload 64 --duration 5 --warmup 1 --runs 2 --drop-caches --max-used 80"
# the driver's cpus on europa, clear of every store's and of Shoal's shards (1-7, 16-23); the loop
# leg's three Shoal nodes take cores 1-12, so its driver is on 13-15
DRIVER_CPUS=12-15,28-31
LOOP_DRIVER_CPUS=13-15,29-31
# the loop leg's devices, smaller for a quick try beside the ssd leg's on the Optane
LOOP_SIZE=${LOOP_SIZE:-50G}

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
    echo "stores-lab: $*" >&2
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
    on "$host" "sudo lvs --noheadings -o vg_name,lv_name,lv_size; sudo vgs --noheadings -o vg_name,vg_size" > "$dir/lvm.txt"
    on "$host" "lsblk -o NAME,SIZE,TYPE,MOUNTPOINT -n | grep -v '^loop'; losetup -a | sort" > "$dir/lsblk.txt"
    on "$host" "findmnt -rn -o TARGET | sort" > "$dir/mounts.txt"
    on "$host" "ls -la /srv /hdd /optane 2>&1 | grep -v ' \.\.\?$' | awk '{print \$1, \$3, \$NF}'" > "$dir/dirs.txt"
    on "$host" "for d in /etc/ceph /var/lib/ceph /var/log/ceph /etc/containers /var/lib/containers $REMOTE; do sudo test -e \$d && echo \"\$d present\" || echo \"\$d absent\"; done; getent passwd 167 || echo 'no uid 167'" > "$dir/paths.txt"
    on "$host" "sha256sum ~/.ssh/authorized_keys | cut -c1-64; cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor; systemctl is-active shoal-tmdb; systemctl is-active e2scrub_all.timer fstrim.timer" > "$dir/state.txt"
    if [ "$host" = europa ]; then
        sudo docker image ls --format '{{.Repository}}:{{.Tag}} {{.ID}}' | sort > "$dir/docker-images.txt"
    fi
}

cmd_snapshot() {
    for h in $HOSTS; do
        snapshot_host "$h" before
        on "$h" "cp ~/.ssh/authorized_keys /var/tmp/stores-authorized_keys.bak"
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
# every store's programs on every host, from what was downloaded and built under $LAB

cmd_stage() {
    [ -x "$LAB/bin/minio" ] && [ -x "$LAB/bin/rustfs" ] && [ -x "$LAB/bin/weed" ] || die "no programs under $LAB/bin"
    for h in $HOSTS; do
        on "$h" "mkdir -p $REMOTE/bin $REMOTE/etc && chmod 755 $REMOTE $REMOTE/bin $REMOTE/etc"
        for p in minio rustfs weed; do
            if [ "$h" = europa ]; then cp "$LAB/bin/$p" "$REMOTE/bin/$p"; else scp -q "$LAB/bin/$p" "$h:$REMOTE/bin/$p"; fi
        done
        on "$h" "chmod 755 $REMOTE/bin/*; sha256sum $REMOTE/bin/*"
        changed "$h: $REMOTE/bin/{minio,rustfs,weed}"
    done
}

cmd_unstage() {
    for h in $HOSTS; do
        on "$h" "rm -rf $REMOTE /var/tmp/stores-authorized_keys.bak"
        changed "$h: $REMOTE removed"
    done
}

# ---------------------------------------------------------------------------------------------
# a leg's devices. Every store device is an LV, so a host's is named alike on every leg: on titan
# and hyperion an LV of ubuntu-vg, on europa an LV of a VG of its own on a loop over a file on the
# Optane, since europa's VG has no room. The hdd leg's data is on each host's disk

# the block device a host's ssd store device is
ssd_lv() {
    case $1 in europa) echo /dev/stores-ssd/data ;; *) echo /dev/ubuntu-vg/stores ;; esac
}

# a loop over a file, as an LV of a VG of its own, made on a host; never a second loop over a file
# one is over already, whose PV LVM then sees twice and refuses to remove (Resolved #306)
make_loop_lv() {
    host=$1; file=$2; size=$3; vg=$4
    on "$host" "! sudo vgs $vg > /dev/null 2>&1 && [ -z \"\$(losetup -j $file 2>/dev/null)\" ]" \
        || die "$host: VG $vg, or a loop over $file, is there already; \`devices-down\` takes them down"
    on "$host" "set -e; sudo fallocate -l $size $file; dev=\$(sudo losetup --direct-io=on --find --show $file); \
        sudo pvcreate -q \$dev; sudo vgcreate -q $vg \$dev; sudo lvcreate -q -y -l 100%FREE -n data $vg; echo \$dev" \
        || die "$host: $file was not made VG $vg"
    changed "$host: $file ($size) behind a loop, PV, VG $vg, LV $vg/data"
}

# a loop's LV, VG and PV removed, then the loop and its file; the file is kept while its VG cannot
# be removed, so no loop is left over a file that is gone, and the record says which it was
drop_loop_lv() {
    host=$1; file=$2; vg=$3
    if ! on "$host" "sudo vgs $vg > /dev/null 2>&1"; then
        echo "$host: no VG $vg"
    elif on "$host" "dev=\$(sudo pvs --noheadings -o pv_name -S vg_name=$vg | tr -d ' ') && sudo vgremove -q -y $vg && \
        { [ -z \"\$dev\" ] || { sudo pvremove -q \$dev && sudo losetup -d \$dev; }; } && sudo rm -f $file"; then
        changed "$host: LV $vg/data, its VG, PV and loop removed, $file removed"
    else
        changed "$host: LV $vg/data, its VG, PV or loop not removed, and $file kept"
    fi
}

cmd_devices() {
    leg=$1
    case $leg in
        ssd)
            for h in titan hyperion; do
                on "$h" "sudo lvcreate -q -y -L 100G -n stores ubuntu-vg" \
                    || die "$h: ubuntu-vg/stores was not made; \`devices-down\` takes one that is there down"
                changed "$h: lvcreate ubuntu-vg/stores 100G"
            done
            make_loop_lv europa /optane/stores-ssd.img 100G stores-ssd ;;
        hdd)
            # the disk's data, beside the ssd leg's device that becomes the journal and the DB
            for h in $HOSTS; do
                make_loop_lv "$h" /hdd/stores-ceph.img 200G stores-hdd
                on "$h" "sudo mkdir -p /hdd/stores && sudo chown shoal:shoal /hdd/stores"
                changed "$h: /hdd/stores"
            done ;;
        loop)
            for i in 1 2 3; do make_loop_lv europa "/optane/stores-loop$i.img" "$LOOP_SIZE" "stores-loop$i"; done ;;
        *) die "devices: ssd, hdd or loop" ;;
    esac
}

cmd_devices_down() {
    leg=$1
    case $leg in
        ssd)
            for h in titan hyperion; do
                if on "$h" "sudo umount /srv/stores 2>/dev/null; sudo lvremove -q -y ubuntu-vg/stores"; then
                    changed "$h: lvremove ubuntu-vg/stores"
                else
                    changed "$h: lvremove ubuntu-vg/stores refused, the volume kept"
                fi
            done
            on europa "sudo umount /srv/stores 2>/dev/null; true"
            drop_loop_lv europa /optane/stores-ssd.img stores-ssd ;;
        hdd)
            for h in $HOSTS; do
                on "$h" "sudo rm -rf /hdd/stores"
                drop_loop_lv "$h" /hdd/stores-ceph.img stores-hdd
            done ;;
        loop)
            for i in 1 2 3; do on europa "sudo umount /srv/stores/$i 2>/dev/null; true"; drop_loop_lv europa "/optane/stores-loop$i.img" "stores-loop$i"; done ;;
    esac
    for h in $HOSTS; do on "$h" "sudo rmdir /srv/stores/1 /srv/stores/2 /srv/stores/3 /srv/stores 2>/dev/null; true"; done
}

# a device made fresh: every byte discarded, then an XFS of its own mounted at a directory, owned
# by the user the directory stores run as; or left raw, for BlueStore. sh has no local variables,
# so this and `prepare` name theirs apart from every caller's
fresh() {
    fr_host=$1; fr_dev=$2; fr_mnt=$3; fr_kind=$4; fr_opts=${5:-noatime}
    # a device still mounted once it was unmounted is in use, by a store's node a killed run left up,
    # and nothing of it is wiped under it: a forced discard emptied two hosts' devices from under
    # such a cluster's nodes, while the wipe and the filesystem it was made with refused them busy
    # (Resolved #306)
    on "$fr_host" "sudo umount $fr_mnt 2>/dev/null; ! findmnt -rn $fr_mnt > /dev/null && ! findmnt -rn -S $fr_dev > /dev/null" \
        || die "$fr_host: $fr_dev is still mounted, so something uses it; nothing of it was wiped"
    # the tags ceph-volume left on the volume go too, or the next cluster refuses it as an OSD's
    on "$fr_host" "for tag in \$(sudo lvs --noheadings -o lv_tags $fr_dev | tr -d ' ' | tr ',' ' '); do sudo lvchange -q --deltag \$tag $fr_dev > /dev/null; done"
    # a volume on a loop over a file is not discarded: a discard punches the file's blocks out, and
    # every write would then allocate them in the filesystem under it, as no LV of an SSD does. Its
    # file is allocated again instead, and its filesystem made without a discard
    fr_file=$(backing_file "$fr_dev")
    if [ -n "$fr_file" ]; then
        on "$fr_host" "sudo wipefs -q -a $fr_dev; sudo fallocate -l \$(stat -c %s $fr_file) $fr_file"
        fr_mkfs="mkfs.xfs -q -f -K"
    else
        # never forced: blkdiscard opens the device exclusively unless told not to, so a device
        # anything holds is refused here too, and said so
        on "$fr_host" "sudo wipefs -q -a $fr_dev; sudo blkdiscard $fr_dev || echo '$fr_host: $fr_dev was not discarded' >&2"
        fr_mkfs="mkfs.xfs -q -f"
    fi
    if [ "$fr_kind" = xfs ]; then
        on "$fr_host" "set -e; sudo $fr_mkfs $fr_dev; sudo mkdir -p $fr_mnt; sudo mount -o $fr_opts $fr_dev $fr_mnt; sudo chown shoal:shoal $fr_mnt" \
            || die "$fr_host: $fr_dev was not made an XFS at $fr_mnt"
        on "$fr_host" "findmnt -n $fr_mnt > /dev/null" || die "$fr_host: $fr_dev did not mount at $fr_mnt"
    fi
}

# the file a store volume's loop is over, or nothing for an LV of a disk
backing_file() {
    case $1 in
        /dev/stores-ssd/data) echo /optane/stores-ssd.img ;;
        /dev/stores-hdd/data) echo /hdd/stores-ceph.img ;;
        /dev/stores-loop[123]/data) echo "/optane/stores-loop$(echo "$1" | sed 's|/dev/stores-loop\([123]\)/data|\1|').img" ;;
    esac
}

# every device a store of a leg runs on, made fresh for it
prepare() {
    pr_leg=$1; pr_store=$2
    # never under a cluster the bench made, which a killed run leaves on the devices
    no_bench_cluster
    pr_kind=xfs
    [ "$pr_store" = ceph ] && pr_kind=raw
    case $pr_leg in
        ssd)
            for pr_host in $HOSTS; do fresh "$pr_host" "$(ssd_lv $pr_host)" /srv/stores $pr_kind; done ;;
        hdd)
            # the disk's directory emptied, its volume discarded, and the SSD beside it a journal's
            # filesystem or, for BlueStore, a DB
            for pr_host in $HOSTS; do
                on "$pr_host" "sudo find /hdd/stores -mindepth 1 -delete"
                fresh "$pr_host" /dev/stores-hdd/data /srv/stores-hdd raw
                fresh "$pr_host" "$(ssd_lv $pr_host)" /srv/stores $pr_kind
            done ;;
        loop)
            # SeaweedFS's volumes grow by appending to 4 GiB files, and XFS holds space past a growing
            # file's end: each held 8 GiB, and one arm of 1 MiB puts filled the 50 GiB devices twice
            # over. A fixed allocsize turns that off; no other store here appends to a growing file
            pr_opts=noatime
            [ "$pr_store" = seaweedfs ] && pr_opts=noatime,allocsize=64k
            for pr_i in 1 2 3; do fresh europa "/dev/stores-loop$pr_i/data" "/srv/stores/$pr_i" $pr_kind $pr_opts; done ;;
    esac
}

# ---------------------------------------------------------------------------------------------
# the hosts held for a leg: the tmdb node stopped, governors at performance, the scrub and trim
# timers held, and the unattended upgrade's timers held once an upgrade already running is done,
# since needrestart restarts every service an upgraded library is mapped in, a store's nodes and a
# leg's own unit among them (Resolved #306); all put back on the way out, however it ends. A host's
# governor is kept from its first hold until a restore puts it back, so a hold after a run killed
# before its restore keeps the governor that run found, never the performance it left

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

# every root has room, and none of titan's syslog has filled it
roots_ok() {
    for h in $HOSTS; do
        use=$(on "$h" "df --output=pcent / | tail -1 | tr -dc 0-9")
        [ "$use" -lt 95 ] || die "$h's root is $use% full"
    done
}

# ---------------------------------------------------------------------------------------------
# the directory stores, each in a transient unit a host as the shoal user

# where a store's data goes on a host, for a leg
data_dir() {
    leg=$1; store=$2; host=$3; instance=${4:-}
    case $leg in
        ssd) echo "/srv/stores/$store" ;;
        hdd) echo "/hdd/stores/$store" ;;
        loop) echo "/srv/stores/$instance/$store" ;;
    esac
}

unit() {
    host=$1; name=$2; shift 2
    on "$host" "sudo systemd-run --quiet --unit=$name --uid=shoal --gid=shoal -p LimitNOFILE=1048576 -p WorkingDirectory=/tmp $*"
    changed "$host: transient unit $name"
}

# wait until every endpoint answers HTTP, or say which did not and what its unit logged
wait_endpoints() {
    we_endpoints=$1; we_units=$2
    for we_endpoint in $(echo "$we_endpoints" | tr ',' ' '); do
        we_tries=0
        until curl -s -o /dev/null --max-time 2 "$we_endpoint/"; do
            we_tries=$((we_tries + 1))
            if [ $we_tries -ge 60 ]; then
                # each unit's last ten minutes, the loop leg's numbered ones (stores-weed-s3-2) too
                for we_host in $HOSTS; do on "$we_host" "for u in $we_units; do sudo journalctl -u \"\$u*\" --since -10min --no-pager -n 20 2>/dev/null; done" | sed "s/^/$we_host: /"; done
                die "$we_endpoint never answered"
            fi
            sleep 2
        done
    done
}

# no host runs a cluster the bench made (`shoal-<name>-bench`, or a node's of one on a shared
# machine): a run killed before its teardown leaves one up, on the leg's devices and the bench's
# ports, and the next capture would make its devices fresh under it (Resolved #306)
no_bench_cluster() {
    for nb_host in $HOSTS; do
        nb_units=$(on "$nb_host" "systemctl list-units --all --plain --no-legend 'shoal-*-bench*' | awk '{print \$1}' | tr '\n' ' '")
        [ -z "$nb_units" ] || die "$nb_host runs ${nb_units}of a cluster the bench made: its loader's \`destroy -i <the capture's inventory.bench.yml> --yes\`, with SHOAL_DEPLOY_HOME=$LAB/state, takes it down"
    done
}

stop_units() {
    # every store's unit, never a leg's own (stores-lab-*) or a before and after's (stores-ab-*),
    # which this runs inside of: a unit that stops itself waits on its own stop until systemd kills
    # it, and the hosts are never put back
    for h in $HOSTS; do
        on "$h" "for u in \$(systemctl list-units --all --plain --no-legend 'stores-*' | awk '{print \$1}' | grep -v -e '^stores-lab-' -e '^stores-ab-'); do sudo systemctl stop \$u; sudo systemctl reset-failed \$u 2>/dev/null; done; true"
    done
}

# MinIO: one erasure set of the three hosts' one drive each, its default parity for three, EC:1.
# MINIO_ENV, empty unless set, adds -E settings to every server's unit: how the diagnosis of its 4 KiB
# put tail ran it with _MINIO_DSYNC_TRACE=1 and with _MINIO_LOCK_RETRY_INTERVAL, never a compared capture
deploy_minio() {
    leg=$1
    if [ "$leg" = loop ]; then
        # one address and three ports: every 127.0.0.x is local, and MinIO refuses local endpoints of
        # different addresses
        set_list="http://127.0.0.1:9101/srv/stores/1/minio http://127.0.0.1:9102/srv/stores/2/minio http://127.0.0.1:9103/srv/stores/3/minio"
        for i in 1 2 3; do
            on europa "sudo -u shoal mkdir -p /srv/stores/$i/minio"
            unit europa "stores-minio-$i" -E MINIO_ROOT_USER=$AWS_ACCESS_KEY_ID -E MINIO_ROOT_PASSWORD=$AWS_SECRET_ACCESS_KEY \
                -E MINIO_BROWSER=off ${MINIO_ENV:-} $REMOTE/bin/minio server --address 127.0.0.1:910$i --console-address 127.0.0.1:920$i $set_list
        done
    else
        set_list=""
        for h in $HOSTS; do set_list="$set_list http://$(ip_of $h):9000$(data_dir $leg minio $h)"; done
        for h in $HOSTS; do
            on "$h" "sudo -u shoal mkdir -p $(data_dir $leg minio $h)"
            unit "$h" stores-minio -E MINIO_ROOT_USER=$AWS_ACCESS_KEY_ID -E MINIO_ROOT_PASSWORD=$AWS_SECRET_ACCESS_KEY \
                -E MINIO_BROWSER=off ${MINIO_ENV:-} $REMOTE/bin/minio server --address $(ip_of $h):9000 --console-address $(ip_of $h):9001 $set_list
        done
    fi
}

# RustFS: as MinIO is set up, its default durability strict and parity EC:1 for a set of three
deploy_rustfs() {
    leg=$1
    if [ "$leg" = loop ]; then
        volumes="http://127.0.0.1:9101/srv/stores/1/rustfs http://127.0.0.1:9102/srv/stores/2/rustfs http://127.0.0.1:9103/srv/stores/3/rustfs"
        for i in 1 2 3; do
            on europa "sudo -u shoal mkdir -p /srv/stores/$i/rustfs /srv/stores/$i/rustfs-logs"
            unit europa "stores-rustfs-$i" -E RUSTFS_ACCESS_KEY=$AWS_ACCESS_KEY_ID -E RUSTFS_SECRET_KEY=$AWS_SECRET_ACCESS_KEY \
                -E RUSTFS_CONSOLE_ENABLE=false -E RUSTFS_OBS_LOG_DIRECTORY=/srv/stores/$i/rustfs-logs \
                "-E 'RUSTFS_VOLUMES=$volumes'" -E RUSTFS_ADDRESS=127.0.0.1:910$i $REMOTE/bin/rustfs server
        done
    else
        volumes=""
        for h in $HOSTS; do volumes="$volumes http://$(ip_of $h):9000$(data_dir $leg rustfs $h)"; done
        volumes=${volumes# }
        for h in $HOSTS; do
            logs=$(dirname "$(data_dir $leg rustfs $h)")/rustfs-logs
            on "$h" "sudo -u shoal mkdir -p $(data_dir $leg rustfs $h) $logs"
            unit "$h" stores-rustfs -E RUSTFS_ACCESS_KEY=$AWS_ACCESS_KEY_ID -E RUSTFS_SECRET_KEY=$AWS_SECRET_ACCESS_KEY \
                -E RUSTFS_CONSOLE_ENABLE=false -E RUSTFS_OBS_LOG_DIRECTORY=$logs \
                "-E 'RUSTFS_VOLUMES=$volumes'" -E RUSTFS_ADDRESS=$(ip_of $h):9000 $REMOTE/bin/rustfs server
        done
    fi
}

# SeaweedFS: a master and one filer on europa, a volume server and an S3 gateway on every host,
# every volume three copies on three servers (002); the filer keeps the bucket's fsync rule
# write a file on a host from a value, inside the script the host is fed, since `on` takes stdin
put_file() {
    host=$1; path=$2; content=$3
    on "$host" "cat > $path <<'STORES_EOF'
$content
STORES_EOF
chmod 644 $path"
}

seaweed_s3_config() {
    cat <<JSON
{"identities": [{"name": "bench", "credentials": [{"accessKey": "$AWS_ACCESS_KEY_ID", "secretKey": "$AWS_SECRET_ACCESS_KEY"}], "actions": ["Admin", "Read", "Write", "List", "Tagging"]}]}
JSON
}

deploy_seaweedfs() {
    leg=$1; fsync=$2
    if [ "$leg" = loop ]; then
        master=127.0.0.11:9333; filer=127.0.0.11:8888
        put_file europa $REMOTE/etc/s3.json "$(seaweed_s3_config)"
        on europa "sudo -u shoal mkdir -p /srv/stores/1/seaweedfs/master /srv/stores/1/seaweedfs/filer"
        unit europa stores-weed-master $REMOTE/bin/weed master -ip=127.0.0.11 -port=9333 -mdir=/srv/stores/1/seaweedfs/master \
            -defaultReplication=002 -volumeSizeLimitMB=4096 -volumePreallocate=false
        sleep 3
        for i in 1 2 3; do
            on europa "sudo -u shoal mkdir -p /srv/stores/$i/seaweedfs/volume"
            unit europa "stores-weed-volume-$i" $REMOTE/bin/weed volume -ip=127.0.0.1$i -port=8080 -dir=/srv/stores/$i/seaweedfs/volume \
                -max=0 -mserver=$master -dataCenter=dc1 -rack=r1 -minFreeSpace=5
        done
        sleep 3
        unit europa stores-weed-filer $REMOTE/bin/weed filer -ip=127.0.0.11 -port=8888 -master=$master \
            -defaultReplicaPlacement=002 -defaultStoreDir=/srv/stores/1/seaweedfs/filer
        sleep 3
        # each gateway a unix socket of its own: by default it is named by the port alone
        # (/tmp/seaweedfs-s3-8333.sock, weed/command/s3.go), which the three share, and a gateway that
        # finds it taken exits
        for i in 1 2 3; do
            unit europa "stores-weed-s3-$i" $REMOTE/bin/weed s3 -ip.bind=127.0.0.1$i -port=8333 -port.iceberg=0 -port.lance=0 \
                -localSocket=/tmp/seaweedfs-s3-loop-$i.sock -filer=$filer -config=$REMOTE/etc/s3.json
        done
    else
        master=172.16.2.10:9333; filer=172.16.2.10:8888
        base=$(dirname "$(data_dir $leg seaweedfs europa)")/seaweedfs
        on europa "sudo -u shoal mkdir -p $base/master $base/filer"
        unit europa stores-weed-master $REMOTE/bin/weed master -ip=172.16.2.10 -port=9333 -mdir=$base/master \
            -defaultReplication=002 -volumeSizeLimitMB=4096 -volumePreallocate=false
        sleep 3
        for h in $HOSTS; do
            put_file "$h" $REMOTE/etc/s3.json "$(seaweed_s3_config)"
            dir=$(dirname "$(data_dir $leg seaweedfs $h)")/seaweedfs/volume
            on "$h" "sudo -u shoal mkdir -p $dir"
            unit "$h" stores-weed-volume $REMOTE/bin/weed volume -ip=$(ip_of $h) -port=8080 -dir=$dir \
                -max=0 -mserver=$master -dataCenter=dc1 -rack=r1 -minFreeSpace=5
        done
        sleep 3
        unit europa stores-weed-filer $REMOTE/bin/weed filer -ip=172.16.2.10 -port=8888 -master=$master \
            -defaultReplicaPlacement=002 -defaultStoreDir=$base/filer
        sleep 3
        for h in $HOSTS; do
            unit "$h" stores-weed-s3 $REMOTE/bin/weed s3 -ip.bind=$(ip_of $h) -port=8333 -port.iceberg=0 -port.lance=0 \
                -filer=$filer -config=$REMOTE/etc/s3.json
        done
    fi
    sleep 5
    # the bucket's writes synced, for the durable configuration
    if [ "$fsync" = fsync ]; then
        printf 'lock\nfs.configure -locationPrefix=/buckets/ -fsync -apply\nunlock\n' | $LAB/bin/weed shell -master=$master -filer=$filer
    fi
}

# what gives back the space a cleanup deleted
# MinIO's and RustFS's settle on the loop leg, nothing on the others: each moves what a delete
# removes into a trash its drive empties in the background (MinIO's cmd/xl-storage.go, 1232-1245),
# and one arm of 1 MiB puts there writes 19 GiB to each 50 GiB device, so the next arm waits until
# the fullest device is under 15% or stopped emptying for twenty tries
space_settle() {
    [ "$1" = loop ] || return 0
    mkdir -p "$LAB/run"
    cat > "$LAB/run/space-settle-loop.sh" <<'EOF2'
# a store's settle on the loop leg, written by stores-lab.sh: its devices given time to empty
prev=101; same=0
for n in $(seq 1 100); do
    used=$(df --output=pcent /srv/stores/1 /srv/stores/2 /srv/stores/3 | tail -n +2 | tr -d ' %' | sort -n | tail -1)
    [ "$used" -le 15 ] && break
    if [ "$used" -lt "$prev" ]; then same=0; else same=$((same + 1)); fi
    [ $same -ge 20 ] && break
    prev=$used
    sleep 3
done
exit 0
EOF2
    echo "sh $LAB/run/space-settle-loop.sh"
}

seaweed_settle() {
    leg=$1
    master=172.16.2.10:9333
    [ "$leg" = loop ] && master=127.0.0.11:9333
    vacuum="printf 'lock\\nvolume.vacuum -garbageThreshold=0.01\\nunlock\\n' | $LAB/bin/weed shell -master=$master > /dev/null"
    if [ "$leg" != loop ]; then
        echo "$vacuum"
        return
    fi
    # on the loop leg one arm's 1 MiB puts fill most of a 50 GiB device, and a sweep of every volume
    # skips a read-only one, which a volume at its size limit is (weed/topology/topology_vacuum.go,
    # 358-375), so the volumes those puts filled kept every byte the cleanup deleted. Each volume is
    # vacuumed by its id, which reaches a read-only one, and again until the fullest device is under
    # 15% or it stopped emptying for ten tries; the settle is a script of its own, run on europa
    mkdir -p "$LAB/run"
    cat > "$LAB/run/weed-settle-loop.sh" <<EOF
# SeaweedFS's settle on the loop leg, written by stores-lab.sh: every volume vacuumed by its id
dirs="/srv/stores/1/seaweedfs/volume /srv/stores/2/seaweedfs/volume /srv/stores/3/seaweedfs/volume"
prev=101; same=0
for n in \$(seq 1 60); do
    ids=\$(ls \$dirs | sed -n -e 's/^\([0-9][0-9]*\)\.dat\$/\1/p' -e 's/^.*_\([0-9][0-9]*\)\.dat\$/\1/p' | sort -un | paste -sd, -)
    [ -n "\$ids" ] && printf 'lock\nvolume.vacuum -garbageThreshold=0.01 -volumeId=%s\nunlock\n' "\$ids" | $LAB/bin/weed shell -master=$master > /dev/null
    used=\$(df --output=pcent /srv/stores/1 /srv/stores/2 /srv/stores/3 | tail -n +2 | tr -d ' %' | sort -n | tail -1)
    [ "\$used" -le 15 ] && break
    if [ "\$used" -lt "\$prev" ]; then same=0; else same=\$((same + 1)); fi
    [ \$same -ge 10 ] && break
    prev=\$used
    sleep 3
done
exit 0
EOF
    echo "sh $LAB/run/weed-settle-loop.sh"
}

# ---------------------------------------------------------------------------------------------
# Ceph: X14's cephadm path, with one OSD and one RGW on every host (three of each on europa for the
# loop leg), and two placements: three copies by host, and 2+1 by host

CEPHADM=$REMOTE/cephadm
CEPH_IMAGE=quay.io/ceph/ceph:v20.2.0
CEPH_PINNED=quay.io/ceph/ceph@sha256:1228c3d05e45fbc068a8c33614e4409b6dac688bcc77369b06009b5830fa8d86

ceph() {
    sudo "$CEPHADM" --image "$CEPH_IMAGE" shell -- ceph "$@" 2>> "$LAB/ceph-stderr.log"
}

ceph_tool() {
    sudo "$CEPHADM" --image "$CEPH_IMAGE" shell -- "$@" 2>> "$LAB/ceph-stderr.log"
}

ceph_fsid() {
    sudo ls /var/lib/ceph 2>/dev/null | grep -E '^[0-9a-f-]{36}$' | head -1
}

# cephadm, podman on titan and hyperion, the uid 167 user, and the pinned image, on every host
setup_ceph_tools() {
    [ -f target/lab/x14/cephadm ] || die "no target/lab/x14/cephadm"
    for h in $HOSTS; do
        if [ "$h" = europa ]; then cp target/lab/x14/cephadm "$CEPHADM"; else scp -q target/lab/x14/cephadm "$h:$CEPHADM"; fi
        on "$h" "chmod 755 $CEPHADM"
        on "$h" "getent passwd 167 || (sudo groupadd -r -g 167 cephstores && sudo useradd -r -u 167 -g 167 -M -d /var/lib/ceph -s /usr/sbin/nologin cephstores)"
        changed "$h: $CEPHADM; user cephstores (uid 167)"
    done
    on titan "sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q --no-install-recommends podman catatonit" | tail -1
    scp -q target/lab/x14/debs/*.deb hyperion:$REMOTE/
    on hyperion "sudo dpkg -i $REMOTE/*.deb > /dev/null && rm $REMOTE/*.deb"
    changed "titan: apt-get install podman catatonit; hyperion: dpkg -i X14's ten debs"
    sudo docker pull -q "$CEPH_PINNED" && sudo docker tag "$CEPH_PINNED" "$CEPH_IMAGE"
    changed "europa: docker pull $CEPH_PINNED, tagged $CEPH_IMAGE"
    for h in titan hyperion; do
        on "$h" "mkdir -p $REMOTE/ceph-image-dir"
        scp -q -r target/lab/x14/ceph-image-dir/* "$h:$REMOTE/ceph-image-dir/"
        on "$h" "id=\$(sudo podman load -q -i $REMOTE/ceph-image-dir | sed 's/.*sha256://'); sudo podman tag \$id $CEPH_IMAGE; rm -rf $REMOTE/ceph-image-dir"
        changed "$h: podman load of X14's saved image, tagged $CEPH_IMAGE"
    done
}

teardown_ceph_tools() {
    on titan "sudo podman system reset --force; sudo DEBIAN_FRONTEND=noninteractive apt-get purge -y -q podman catatonit conmon crun netavark golang-github-containers-common golang-github-containers-image libgpgme45 libsubid5 libyajl2 > /dev/null"
    on hyperion "sudo podman system reset --force; sudo dpkg --purge podman catatonit conmon crun netavark golang-github-containers-common golang-github-containers-image libgpgme45 libsubid5 libyajl2 > /dev/null"
    sudo docker image rm "$CEPH_IMAGE" "$CEPH_PINNED" > /dev/null
    for h in $HOSTS; do
        on "$h" "sudo userdel cephstores 2>/dev/null; sudo groupdel cephstores 2>/dev/null; sudo rm -rf /etc/containers /var/lib/containers /run/containers; sudo rmdir /etc/ceph /var/lib/ceph /var/log/ceph 2>/dev/null; sudo sysctl -q -w fs.aio-max-nr=65536; true"
        changed "$h: podman purged, the ceph image removed, user cephstores and container directories removed, fs.aio-max-nr 65536"
    done
}

deploy_ceph() {
    leg=$1
    mkdir -p "$LAB"
    chooseleaf=1
    [ "$leg" = loop ] && chooseleaf=0
    cat > "$LAB/ceph-bootstrap.conf" <<CONF
[global]
# a failure domain of a host, or of an OSD on the loop leg's one host
osd_crush_chooseleaf_type = $chooseleaf
mon_data_avail_warn = 5
osd_pool_default_pg_autoscale_mode = on
[mon]
mon_allow_pool_delete = true
[client.rgw]
# a deleted object's space given back as soon as a settle asks
rgw_gc_obj_min_wait = 0
CONF
    cp "$LAB/ceph-bootstrap.conf" "$REMOTE/ceph-bootstrap.conf"
    sudo "$CEPHADM" --image "$CEPH_IMAGE" bootstrap --mon-ip 172.16.2.10 --ssh-user "$(id -un)" \
        --skip-pull --skip-dashboard --skip-monitoring-stack --config "$REMOTE/ceph-bootstrap.conf" > "$LAB/ceph-bootstrap.log" 2>&1 \
        || { tail -5 "$LAB/ceph-bootstrap.log"; die "cephadm bootstrap failed"; }
    changed "europa: cephadm bootstrap, fsid $(ceph_fsid)"
    ceph config set mgr mgr/cephadm/use_repo_digest false
    ceph config set global container_image "$CEPH_IMAGE"
    ceph orch apply mon --placement=europa
    ceph orch apply mgr --placement=europa
    if [ "$leg" = loop ]; then
        osd_hosts=europa
        printf 'service_type: osd\nservice_id: stores-europa\nplacement:\n  hosts: [europa]\nspec:\n  data_devices:\n    paths: [/dev/stores-loop1/data, /dev/stores-loop2/data, /dev/stores-loop3/data]\n' > "$LAB/ceph-osd-spec.yaml"
        osds=3
    else
        for h in titan hyperion; do
            ssh-copy-id -f -i /etc/ceph/ceph.pub "$h" > /dev/null 2>&1
            changed "$h: /etc/ceph/ceph.pub added to authorized_keys"
            ceph orch host add "$h" "$(ip_of $h)"
        done
        : > "$LAB/ceph-osd-spec.yaml"
        for h in $HOSTS; do
            if [ "$leg" = hdd ]; then
                printf 'service_type: osd\nservice_id: stores-%s\nplacement:\n  hosts: [%s]\nspec:\n  data_devices:\n    paths: [/dev/stores-hdd/data]\n  db_devices:\n    paths: [%s]\n---\n' "$h" "$h" "$(ssd_lv $h)"
            else
                printf 'service_type: osd\nservice_id: stores-%s\nplacement:\n  hosts: [%s]\nspec:\n  data_devices:\n    paths: [%s]\n---\n' "$h" "$h" "$(ssd_lv $h)"
            fi
        done > "$LAB/ceph-osd-spec.yaml"
        osds=3
    fi
    # the device inventory read again, so a volume made fresh since the last cluster is seen as free
    ceph orch device ls --refresh > /dev/null
    sleep 20
    sudo "$CEPHADM" --image "$CEPH_IMAGE" shell --mount "$LAB/ceph-osd-spec.yaml:/spec.yaml" -- ceph orch apply -i /spec.yaml 2>> "$LAB/ceph-stderr.log"
    tries=0
    until [ "$(ceph osd stat -f json | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["num_up_osds"])')" = $osds ]; do
        tries=$((tries + 1)); [ $tries -lt 80 ] || die "ceph: $osds OSDs never came up"; sleep 15
    done
    # a bucket's index and object pools, made before any RGW starts, at Ceph's own default of 32
    # placement groups (`osd_pool_default_pg_num`) and with the autoscaler off for them. Left to RGW
    # to make, under the autoscaler, a new pool starts with one placement group
    # (`osd_pool_default_pg_autoscale_mode`, src/common/options/global.yaml.in:2757): on the first
    # ssd leg every primary was europa's OSD, which then served every read and sent every copy
    domain=host
    [ "$leg" = loop ] && domain=osd
    ceph osd erasure-code-profile set stores-21 k=2 m=1 crush-failure-domain=$domain
    ceph osd pool create default.rgw.buckets.index 32 32 replicated --autoscale-mode=off
    ceph osd pool create default.rgw.buckets.data 32 32 replicated --autoscale-mode=off
    ceph osd pool create default.rgw.buckets.data.ec21 32 32 erasure stores-21 --autoscale-mode=off
    for pool in default.rgw.buckets.index default.rgw.buckets.data default.rgw.buckets.data.ec21; do
        ceph osd pool application enable $pool rgw
    done
    ceph_spread default.rgw.buckets.index default.rgw.buckets.data default.rgw.buckets.data.ec21
    # the RGWs, each with Nagle's algorithm off as every other store and the driver have it: beast
    # keeps it on by default (doc/radosgw/frontends.rst), and a 4 KiB get then waits out the
    # driver's delayed ack, 40 ms, at one in flight
    if [ "$leg" = loop ]; then
        # three on the one host, on 8081 to 8083: a count a host is a spec's, not a placement string's
        printf 'service_type: rgw\nservice_id: stores\nplacement:\n  hosts: [europa]\n  count_per_host: 3\nspec:\n  rgw_frontend_port: 8081\n  rgw_frontend_extra_args: [tcp_nodelay=1]\n' > "$LAB/ceph-rgw-spec.yaml"
    else
        printf 'service_type: rgw\nservice_id: stores\nplacement:\n  hosts: [europa, titan, hyperion]\nspec:\n  rgw_frontend_port: 8080\n  rgw_frontend_extra_args: [tcp_nodelay=1]\n' > "$LAB/ceph-rgw-spec.yaml"
    fi
    sudo "$CEPHADM" --image "$CEPH_IMAGE" shell --mount "$LAB/ceph-rgw-spec.yaml:/rgw.yaml" -- ceph orch apply -i /rgw.yaml 2>> "$LAB/ceph-stderr.log"
    sleep 30
    ceph_tool radosgw-admin zonegroup placement add --rgw-zonegroup default --placement-id ec21
    ceph_tool radosgw-admin zone placement add --rgw-zone default --placement-id ec21 \
        --data-pool default.rgw.buckets.data.ec21 --index-pool default.rgw.buckets.index --data-extra-pool default.rgw.buckets.non-ec
    ceph_tool radosgw-admin period update --commit > /dev/null 2>&1 || true
    ceph_tool radosgw-admin user create --uid stores --display-name stores --access-key "$AWS_ACCESS_KEY_ID" --secret "$AWS_SECRET_ACCESS_KEY" > /dev/null
    restarted=$(date -u +%s)
    ceph orch restart rgw.stores
    # every RGW running again since the restart that gave it the new placement and user, which
    # cephadm rolls host by host after the command returns, and then answering
    tries=0
    until ceph orch ps --daemon_type rgw --refresh -f json | python3 -I -c "
import json, sys, datetime
since = $restarted
daemons = json.load(sys.stdin)
started = [datetime.datetime.fromisoformat(d.get('started', '1970-01-01T00:00:00Z').replace('Z', '+00:00')).timestamp() for d in daemons if d.get('status_desc') == 'running']
sys.exit(0 if daemons and len(started) == len(daemons) and min(started) >= since else 1)
"; do
        tries=$((tries + 1)); [ $tries -lt 60 ] || die "ceph: the RGWs never all restarted"; sleep 10
    done
    port=8080
    [ "$leg" = loop ] && port=8081
    wait_endpoints "$(endpoints_of $leg $port)" ""
    # every RGW's frontend as configured, which the capture's log keeps beside the pools
    ceph config dump | grep rgw_frontends
    ceph -s
}

# wait until each pool's placement groups are all active and clean, then print how many each OSD
# is the primary of, and refuse a pool with an OSD that is the primary of none
ceph_spread() {
    for cs_pool in "$@"; do
        cs_tries=0
        until ceph pg ls-by-pool "$cs_pool" -f json | python3 -I -c "
import json, sys
loaded = json.load(sys.stdin)
pgs = loaded['pg_stats'] if isinstance(loaded, dict) else loaded
sys.exit(0 if pgs and all(pg['state'] == 'active+clean' for pg in pgs) else 1)
"; do
            cs_tries=$((cs_tries + 1)); [ $cs_tries -lt 60 ] || die "ceph: $cs_pool never went active+clean"; sleep 5
        done
        ceph pg ls-by-pool "$cs_pool" -f json | python3 -I -c "
import json, sys
loaded = json.load(sys.stdin)
pgs = loaded['pg_stats'] if isinstance(loaded, dict) else loaded
# each OSD's count of the pool's primaries, every OSD of the pool's sets counted from zero
counts = {osd: 0 for pg in pgs for osd in pg['acting']}
for pg in pgs:
    counts[pg['acting_primary']] += 1
print('$cs_pool: %d placement groups, primaries by OSD %s' % (len(pgs), dict(sorted(counts.items()))))
sys.exit(0 if min(counts.values()) > 0 else 1)
" || die "ceph: an OSD is the primary of none of $cs_pool's placement groups"
    done
}

teardown_ceph() {
    f=$(ceph_fsid)
    [ -n "$f" ] || return 0
    for h in $HOSTS; do
        on "$h" "sudo $CEPHADM rm-cluster --fsid $f --force > /dev/null 2>&1; true"
        changed "$h: cephadm rm-cluster --fsid $f --force"
    done
    # every host's, europa's too: cephadm's bootstrap adds its cluster's key there, and twenty-three
    # of them were left on europa before this restored it
    for h in $HOSTS; do
        on "$h" "cp /var/tmp/stores-authorized_keys.bak ~/.ssh/authorized_keys"
    done
    sudo rm -f /root/.ssh/known_hosts /tmp/ssh_key_*
}

ceph_settle() {
    echo "sudo $CEPHADM --image $CEPH_IMAGE shell -- radosgw-admin gc process --include-all > /dev/null 2>&1"
}

# ---------------------------------------------------------------------------------------------
# Shoal: the tmdb schema on a side cluster of its own, every root and its pool device on the leg's
# devices, at a redundancy of the group's; the bench deploys and destroys it

shoal_inventory() {
    leg=$1; group=$2; path=$3
    # three copies take tmdb_cluster.yaml's small writes in their commits; an erasure coded pool
    # stages every write, and refuses the setting
    case $group in
        rep3) redundancy="{replicas: 3}"; small="small_writes: 64KiB" ;;
        ec21) redundancy="{erasure: {data: 2, parity: 1}}"; small="" ;;
    esac
    # a before and after may move the pool's inline threshold (O120); absent, the engine's default
    if [ -n "${SHOAL_INLINE:-}" ]; then
        small="$small
    inline: $SHOAL_INLINE"
    fi
    # and may serve strong reads inside their leaders' leases (F100); absent, a round each
    replication=""
    if [ -n "${SHOAL_LEASE_READS:-}" ]; then
        replication="replication:
  lease_reads: $SHOAL_LEASE_READS"
    fi
    # and may move the margin a read of a disk pool's copies leaves its local copy past (F101):
    # absent, the engine's four; null, the local copy first
    read_margin=""
    if [ -n "${SHOAL_DISK_READ_MARGIN:-}" ]; then
        read_margin="  disk_read_margin: $SHOAL_DISK_READ_MARGIN"
    fi
    # and may size every node's cache of the units its reads verify (F102); absent, the engine's
    # eighth of node memory since F107, and 0 for none
    cache=""
    if [ -n "${SHOAL_OBJECT_CACHE:-}" ]; then
        cache="      object_cache: $SHOAL_OBJECT_CACHE"
    fi
    # and may run every node's peer lanes plaintext (F103); absent, mutual TLS
    plaintext=""
    if [ -n "${SHOAL_PLAINTEXT_LANES:-}" ]; then
        plaintext="plaintext_lanes: $SHOAL_PLAINTEXT_LANES"
    fi
    # and may name the congestion control every node's object connections and the bench's run
    # under (F112); absent, each host's own since F113 (the engine's bbr before it), null too
    congestion=""
    if [ -n "${SHOAL_OBJECT_CONGESTION:-}" ]; then
        congestion="object_congestion: $SHOAL_OBJECT_CONGESTION"
    fi
    # and may let every host's idle connections start again from a small window (F113); absent, a
    # deploy keeps every window through an idle, and null leaves each host's own
    if [ -n "${SHOAL_SLOW_START_AFTER_IDLE:-}" ]; then
        congestion="$congestion
slow_start_after_idle: $SHOAL_SLOW_START_AFTER_IDLE"
    fi
    if [ "$leg" = loop ]; then
        shoal_loop_inventory "$group" "$redundancy" "$path"
        return
    fi
    case $leg in
        hdd) pool="class: hdd
    media: rotational"
             # a disk's 2+1 survives one loss, which F91 refuses unless the exposure is accepted:
             # MinIO, RustFS and Ceph keep it on the same disks, so the comparison accepts it too
             [ "$group" = ec21 ] && pool="$pool
    accept_rebuild_exposure: true"
             device="{path: /hdd/stores/shoal-pool, class: hdd, journal: /srv/stores/shoal-journal}"
             latency=/srv/stores/shoal ;;
        *)   pool="class: ssd"
             device="{path: /srv/stores/shoal-pool, class: ssd}"
             latency=/srv/stores/shoal ;;
    esac
    cat > "$path" <<YAML
# F94's side cluster on the lab, written by stores-lab.sh: the tmdb schema with tmdb_cluster.yaml's
# resources, every root and its one pool device on the leg's device, the pool $group
name: stores$leg$group
server: $REPO/target/deploy/release/tmdb-dataset-node
ports:
  client: 12400
  peer: 12401
  control: 12402
replication_factor: 3
control_voters: 3
retire_after: 15s
$replication
$plaintext
$congestion
groups:
  bd795i:
    storage:
      latency: $latency
    resources:
      cores: 6
      memory: ${SHOAL_MEMORY:-8Gi}
      control_core_shared: false
      object_cpus: [16, 7]
$cache
    devices:
      - $device
    wal_commit_delay: ${SHOAL_WAL_DELAY:-3ms}
    lead_weight: 2
  v1000:
    storage:
      latency: $latency
    resources:
      cores: 5
      memory: ${SHOAL_MEMORY:-8Gi}
      control_core_shared: false
      object_cpus: [1, 7]
$cache
    devices:
      - $device
    wal_commit_delay: ${SHOAL_WAL_DELAY:-2ms}
tracing: Warn
user: shoal
admin: admin
object_timings:
  reclaim_grace: 5s
  reclaim_poll: 2s
$read_margin
pools:
  fast:
    $pool
    redundancy: $redundancy
    failure_domain: host
    $small
buckets:
  Posters: {pool: fast}
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

# the loop leg's: three nodes on europa as loopback.yml places them, each a control core and three
# shard cores of 1-12 and its object executor on the control core's other thread, every root on
# its own loop device; cores 13-15 are the driver's
shoal_loop_inventory() {
    group=$1; redundancy=$2; path=$3
    {
        cat <<YAML
# F94's loop leg on europa, written by stores-lab.sh: three tmdb nodes over 127.0.0.11-13, every
# root and its pool device on its own loop device, the pool $group
name: storesloop$group
server: $REPO/target/deploy/release/tmdb-dataset-node
ports:
  client: 12400
  peer: 12401
  control: 12402
replication_factor: 3
control_voters: 3
retire_after: 15s
$replication
$plaintext
$congestion
tracing: Warn
user: shoal
admin: admin
object_timings:
  reclaim_grace: 5s
  reclaim_poll: 2s
$read_margin
pools:
  fast:
    class: ssd
    redundancy: $redundancy
    failure_domain: host
    $small
buckets:
  Posters: {pool: fast}
nodes:
YAML
        for i in 1 2 3; do
            control=$(( (i - 1) * 4 + 1 ))
            exclude=""
            for core in $(seq 0 15); do
                if [ "$core" -lt "$control" ] || [ "$core" -gt $((control + 3)) ]; then exclude="$exclude${exclude:+, }$core"; fi
            done
            cat <<YAML
  - name: loop-$i
    ssh: europa
    address: 127.0.0.1$i
    resources:
      cores: 3
      exclude_cores: [$exclude]
      control_core: $control
      memory: ${SHOAL_MEMORY:-4Gi}
      object_cpus: [$((control + 16))]
$cache
    storage:
      latency: /srv/stores/$i/shoal
    devices:
      - {path: /srv/stores/$i/shoal-pool, class: ssd}
    wal_commit_delay: ${SHOAL_WAL_DELAY:-3ms}
YAML
        done
    } > "$path"
}

# ---------------------------------------------------------------------------------------------
# one capture: a store deployed fresh on the leg's devices, every arm, the store torn down

# the programs the bench runs, from the worktree the leg was started at
WT=${WT:-$LAB/wt}
ADM=${ADM:-$WT/target/release/shoaladm}
LOADER=${LOADER:-$REPO/target/deploy/release/tmdb-dataset-loader}
# a quick pass measures a tree that may not be committed; a leg never does
DIRTY=${ALLOW_DIRTY:+--allow-dirty}
# the program Shoal's settle reads the devices with: the repository's own, which knows `reclaim
# wait --steady` whatever commit a side runs, and speaks to any of them (item 301)
SETTLE_LOADER=${SETTLE_LOADER:-$REPO/target/deploy/release/tmdb-dataset-loader}

# Shoal's settle between arms: its reclamation's grace and a pass slept out, then every device's
# chunk bytes read until they hold the same for six seconds, so a put arm's deleted stripes are
# reclaimed before the next arm and not during it, as Ceph's, MinIO's and SeaweedFS's settles give
# their space back (item 301). It reads the bench's own cluster, whose inventory the bench writes
# into the capture before its first arm
shoal_settle() {
    echo "sleep 10 && $SETTLE_LOADER reclaim wait -i $1/inventory.bench.yml --steady 6 --timeout 900"
}

s3_run() {
    leg=$1; store=$2; group=$3; label=$4; flags=$5; shift 5
    endpoints=$1; processes=$2; settle=${3:-}
    hosts_flags=""; devices_flags=""
    # a directory store's devices by the directory they are mounted at, so how full each is is
    # read too; BlueStore's by its volumes, which have no filesystem to be full
    if [ "$leg" = loop ]; then
        hosts_flags="--host europa"
        for i in 1 2 3; do
            if [ "$store" = ceph ]; then devices_flags="$devices_flags --device europa:/dev/stores-loop$i/data"
            else devices_flags="$devices_flags --device europa:/srv/stores/$i"; fi
        done
    else
        for h in $HOSTS; do
            hosts_flags="$hosts_flags --host $h"
            case $leg-$store in
                ssd-ceph) devices_flags="$devices_flags --device $h:$(ssd_lv $h)" ;;
                ssd-*) devices_flags="$devices_flags --device $h:/srv/stores" ;;
                hdd-ceph) devices_flags="$devices_flags --device $h:/dev/stores-hdd/data --device $h:$(ssd_lv $h)" ;;
                hdd-*) devices_flags="$devices_flags --device $h:/hdd/stores --device $h:/srv/stores" ;;
            esac
        done
    fi
    process_flags=""
    for p in $processes; do process_flags="$process_flags --process $p"; done
    location=""
    [ "$store" = ceph ] && [ "$group" = ec21 ] && location="--location-constraint default:ec21"
    # shellcheck disable=SC2086
    if [ -n "$settle" ]; then
        taskset -c $DRIVER_CPUS "$ADM" bench s3 $flags $DIRTY --endpoints "$endpoints" --bucket "Posters=posters-$group" $location \
            --system "$(echo $store | sed 's/-.*//')" --version "$(version_of $store)" --redundancy "$group" \
            --note "leg=$leg" --note "variant=$store" $hosts_flags $devices_flags $process_flags \
            --settle "$settle" --label "$label" --out "$LAB/captures/$leg/$label" --overwrite
    else
        taskset -c $DRIVER_CPUS "$ADM" bench s3 $flags $DIRTY --endpoints "$endpoints" --bucket "Posters=posters-$group" $location \
            --system "$(echo $store | sed 's/-.*//')" --version "$(version_of $store)" --redundancy "$group" \
            --note "leg=$leg" --note "variant=$store" $hosts_flags $devices_flags $process_flags \
            --label "$label" --out "$LAB/captures/$leg/$label" --overwrite
    fi
}

version_of() {
    case $1 in
        ceph) echo v20.2.0 ;;
        minio) echo RELEASE.2025-10-15T17-29-55Z ;;
        rustfs) echo 1.0.3 ;;
        seaweedfs*) echo 4.48 ;;
    esac
}

endpoints_of() {
    leg=$1; port=$2
    if [ "$leg" = loop ]; then
        case $port in
            8081) echo "http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083" ;;
            9000) echo "http://127.0.0.1:9101,http://127.0.0.1:9102,http://127.0.0.1:9103" ;;
            *) echo "http://127.0.0.11:$port,http://127.0.0.12:$port,http://127.0.0.13:$port" ;;
        esac
    else
        echo "http://172.16.2.10:$port,http://172.16.2.4:$port,http://172.16.2.5:$port"
    fi
}

capture() {
    leg=$1; store=$2; group=$3; label=$4; flags=$5
    [ "$leg" = loop ] && DRIVER_CPUS=$LOOP_DRIVER_CPUS
    roots_ok
    echo "== $(date -u +%FT%TZ) $leg $group $store -> $label"
    case $store in
        shoal)
            prepare "$leg" shoal
            inv="$LAB/inventories/$leg-$group.yml"
            mkdir -p "$LAB/inventories"
            shoal_inventory "$leg" "$group" "$inv"
            # shellcheck disable=SC2086
            (cd examples/tmdb_dataset && SHOAL_BIN_DIR="$LAB/state/bin" SHOAL_DEPLOY_HOME="$LAB/state" \
                taskset -c $DRIVER_CPUS "$LOADER" bench run -i "$inv" $(echo "$flags" | sed "s|shoal-spike/results/stores-objects|$REPO/shoal-spike/results/stores-objects|") \
                --between-arms delete --settle "$(shoal_settle "$LAB/captures/$leg/$label")" --basic $DIRTY --label "$label" \
                --out "$LAB/captures/$leg/$label" --overwrite) ;;
        minio)
            prepare "$leg" minio; deploy_minio "$leg"
            wait_endpoints "$(endpoints_of $leg 9000)" "stores-minio stores-minio-1"; sleep 10
            s3_run "$leg" minio "$group" "$label" "$flags" "$(endpoints_of $leg 9000)" minio "$(space_settle $leg)"
            stop_units ;;
        rustfs)
            prepare "$leg" rustfs; deploy_rustfs "$leg"
            wait_endpoints "$(endpoints_of $leg 9000)" "stores-rustfs stores-rustfs-1"; sleep 10
            s3_run "$leg" rustfs "$group" "$label" "$flags" "$(endpoints_of $leg 9000)" rustfs "$(space_settle $leg)"
            stop_units ;;
        seaweedfs|seaweedfs-default)
            fsync=fsync
            [ "$store" = seaweedfs-default ] && fsync=none
            prepare "$leg" seaweedfs; deploy_seaweedfs "$leg" $fsync
            wait_endpoints "$(endpoints_of $leg 8333)" "stores-weed-s3 stores-weed-filer stores-weed-volume stores-weed-master"
            s3_run "$leg" "$store" "$group" "$label" "$flags" "$(endpoints_of $leg 8333)" weed "$(seaweed_settle $leg)"
            stop_units ;;
        ceph)
            port=8080
            [ "$leg" = loop ] && port=8081
            s3_run "$leg" ceph "$group" "$label" "$flags" "$(endpoints_of $leg $port)" "ceph-osd radosgw ceph-mon ceph-mgr" "$(ceph_settle)" ;;
    esac
}

# a group's stores in order, Shoal first and again last as the drift control; every store is made
# fresh on its devices for its capture, Ceph's cluster bootstrapped and torn down around it
group() {
    leg=$1; group=$2; flags=$3
    case $group in
        # SeaweedFS unsynced is context for the hosts' legs: on the loop leg its 1 MiB puts, about
        # 760 MiB/s, fill a 50 GiB device in one arm
        rep3) stores="shoal ceph seaweedfs-default seaweedfs shoal"
              [ "$leg" = loop ] && stores="shoal ceph seaweedfs shoal" ;;
        ec21) stores="shoal ceph minio rustfs shoal" ;;
    esac
    # a leg may run some of a group's stores, in their order, comma separated: Shoal alone first and
    # last is `STORES_REP3=shoal,shoal`, as wave three measured it beside the others' captures
    case $group in
        rep3) [ -n "${STORES_REP3:-}" ] && stores=$(echo "$STORES_REP3" | tr ',' ' ') ;;
        ec21) [ -n "${STORES_EC21:-}" ] && stores=$(echo "$STORES_EC21" | tr ',' ' ') ;;
    esac
    first=1
    for store in $stores; do
        suffix=""
        if [ "$store" = shoal ] && [ $first = 0 ]; then suffix=-drift; fi
        [ "$store" = shoal ] && first=0
        label="$(echo $store | sed 's/seaweedfs-default/seaweedfs/')-$group$( [ "$store" = seaweedfs-default ] && echo -default)$suffix"
        # a leg started again with RESUME=1 keeps every capture that finished, in its place in the
        # order, and runs the rest; a capture that did not finish is run again from its start
        if [ -n "${RESUME:-}" ] && complete "$LAB/captures/$leg/$label/bench.json"; then
            echo "== $label finished before; kept"
            continue
        fi
        if [ "$store" = ceph ]; then
            prepare "$leg" ceph; deploy_ceph "$leg"
        fi
        capture "$leg" "$store" "$group" "$label" "$flags" || echo "== $store $group on $leg failed; going on"
        if [ "$store" = ceph ]; then teardown_ceph; fi
    done
}

# whether a capture finished: its file says so, and its read back of the whole preload lost nothing
complete() {
    [ -f "$1" ] && python3 -I -c "
import json, sys
capture = json.load(open(sys.argv[1]))
end = capture.get('verify_end') or {}
sys.exit(0 if capture.get('complete') and end.get('checked') and not end.get('lost') else 1)
" "$1"
}

cmd_leg() {
    leg=$1
    flags=$SPEC_FLAGS
    [ "$leg" = loop ] && flags=$LOOP_SPEC_FLAGS
    [ -n "${QUICK:-}" ] && flags=$QUICK_FLAGS
    no_bench_cluster
    mkdir -p "$LAB/captures/$leg"
    hold
    trap 'stop_units; teardown_ceph; restore' EXIT
    trap 'exit 143' INT TERM
    for g in rep3 ec21; do group "$leg" "$g" "$flags"; done
}

cmd_quick() {
    leg=$1
    QUICK=1 cmd_leg "$leg"
}

# a leg in a unit of its own on europa, from a copy of this script, so a dropped ssh stops nothing
cmd_start() {
    leg=$1
    [ -x "$WT/target/release/shoaladm" ] && [ -x "$WT/target/deploy/release/tmdb-dataset-loader" ] \
        && [ -x "$WT/target/deploy/release/tmdb-dataset-node" ] \
        || die "build shoaladm (--features s3) and tmdb-dataset (znver1, target/deploy) in $WT first"
    [ -z "$(git -C "$WT" status --porcelain --untracked-files=no)" ] || die "$WT has uncommitted changes"
    mkdir -p "$LAB/run"
    cp "$0" "$LAB/run/stores-lab.sh"
    # run in the worktree, whose tree is the commit every capture names, as a system unit run as
    # this user, since a user's own units end with its last session unless it lingers
    # the script is the unit's main process (the `exec`), so a stop's signal reaches it; `stop` is
    # what ends a leg, since the script's trap runs only once the bench it is waiting on exits, and
    # the unit refuses a manual stop or restart: needrestart restarted one after an unattended
    # upgrade, which killed its bench with no trap and ran it again over what it left (Resolved
    # #306). `systemctl kill` still reaches it. Its PATH is this shell's, since the loader finds its
    # project through cargo
    sudo systemd-run --unit="stores-lab-$leg" --collect -p User="$(id -un)" -p Group="$(id -gn)" \
        -p KillMode=mixed -p TimeoutStopSec=600 -p RefuseManualStop=yes \
        -p WorkingDirectory="$(cd "$WT" && pwd)" --setenv=HOME="$HOME" --setenv=PATH="$PATH" \
        --setenv=LAB="$LAB" --setenv=OUT="$OUT" --setenv=WT=. --setenv=ADM="$(cd "$WT" && pwd)/target/release/shoaladm" \
        --setenv=LOADER="$(cd "$WT" && pwd)/target/deploy/release/tmdb-dataset-loader" \
        ${QUICK:+--setenv=QUICK=1} ${RESUME:+--setenv=RESUME=1} \
        ${STORES_REP3:+--setenv=STORES_REP3=$STORES_REP3} ${STORES_EC21:+--setenv=STORES_EC21=$STORES_EC21} \
        sh -c "exec sh $LAB/run/stores-lab.sh leg $leg >> $LAB/run/$leg.log 2>&1"
    echo "started stores-lab-$leg; sh $0 status"
}

# a leg ended early with its hosts put back: the script is signalled first, and then the bench it
# is waiting on is interrupted, which takes down whatever the bench deployed of its own; the
# script's trap then stops the store and puts the hosts back. `systemctl stop` alone signals only
# the script, whose trap would wait for a bench that runs on until the unit is killed
cmd_stop() {
    leg=$1
    st_main=$(systemctl show "stores-lab-$leg" -p MainPID --value)
    [ -n "$st_main" ] && [ "$st_main" != 0 ] || die "stores-lab-$leg is not running"
    # the script itself, whether it is the main process or, started before the `exec`, its child
    st_script=$(pgrep -f -P "$st_main" "stores-lab.sh leg $leg" || echo "$st_main")
    kill -TERM "$st_script"
    for st_child in $(pgrep -P "$st_script"); do
        # the bench, or a subshell around it, and so the bench under that
        for st_bench in $st_child $(pgrep -P "$st_child"); do
            kill -INT "$st_bench" 2>/dev/null
        done
    done
    st_tries=0
    while systemctl is-active -q "stores-lab-$leg"; do
        st_tries=$((st_tries + 1)); [ $st_tries -lt 120 ] || die "stores-lab-$leg is still running"; sleep 5
    done
    tail -n 5 "$LAB/run/$leg.log"
}

# ---------------------------------------------------------------------------------------------
# Shoal before and after a change (store-optimizations.md): captures of Shoal alone, each on the
# leg's devices made fresh, in the order AB_ORDER names its sides, every one of FLAGS' arms. A side
# is a worktree at a commit with tmdb-dataset built in its target/deploy, and may add to it:
#   AB_<side>=<worktree>                 the side's worktree
#   AB_<side>_FLAGS="--read-level one"   bench flags its captures add
#   AB_<side>_INLINE=64KiB               its pool's inline threshold
#   AB_<side>_DELAY=0ms                  every node's wal_commit_delay
#   AB_<side>_LEASE=true                 every node's strong reads inside their leaders' leases (F100)
#   AB_<side>_MARGIN=null                every node's read margin on a pool of copies on disks, four unless set (F101)
#   AB_<side>_CACHE=2GiB                 every node's cache of the units its reads verify (F102): unset, none
#                                        before F107 and an eighth of node memory since; 0 for none
#   AB_<side>_PLAIN=true                 every node's peer lanes plaintext, mutual TLS unless set (F103)
#   AB_<side>_MEMORY=3Gi                 every node's memory, the leg's own unless set (F104)
#   AB_<side>_CONGESTION=null            the congestion control every node's object connections and the
#                                        bench's run under (F112): each host's own unless set since F113,
#                                        bbr before it
#   AB_<side>_SLOWSTART=null             whether every host's idle connections start again from a small
#                                        window (F113): a deploy keeps every window unless set, null for
#                                        each host's own; each capture logs every host's value before
#                                        and after it, which a destroy puts back
#   AB_<side>_SYSCTL="net.ipv4.tcp_rto_min_us=5000 net.ipv4.tcp_slow_start_after_idle=0"
#                                        every host's kernel settings for the side's captures, each
#                                        logged and put back after it, however the run ends (O131)
# A capture is labelled <tag>-<side>-<n>, n its place in the order, and one that is there already is
# refused before the hosts are held unless AB_OVERWRITE=1 (Resolved #306)

# a side's setting, by its name and the setting's suffix
side_var() {
    eval "printf '%s' \"\${AB_$1$2:-}\""
}

cmd_ab() {
    leg=$1; group=$2; tag=$3
    [ -n "${AB_ORDER:-}" ] || die "AB_ORDER names the sides in their order, e.g. 'before after after before'"
    [ -n "${FLAGS:-}" ] || die "FLAGS names the spec and the arms every capture runs"
    for side in $AB_ORDER; do
        ab_wt=$(side_var "$side" "")
        [ -n "$ab_wt" ] || die "AB_$side names no worktree"
        [ -x "$ab_wt/target/deploy/release/tmdb-dataset-node" ] && [ -x "$ab_wt/target/deploy/release/tmdb-dataset-loader" ] \
            || die "build tmdb-dataset (znver1, target/deploy) in $ab_wt first"
        [ -z "$(git -C "$ab_wt" status --porcelain --untracked-files=no)" ] || die "$ab_wt has uncommitted changes"
    done
    # every capture is a new one: a tag's run again, or restarted, would write over the captures it
    # finished before (Resolved #306). AB_OVERWRITE=1 takes them again
    ab_n=0
    for side in $AB_ORDER; do
        ab_n=$((ab_n + 1))
        [ -n "${AB_OVERWRITE:-}" ] || [ ! -e "$LAB/captures/$leg/$tag-$side-$ab_n" ] \
            || die "$LAB/captures/$leg/$tag-$side-$ab_n is there already: another tag, or AB_OVERWRITE=1 to take it again"
    done
    no_bench_cluster
    mkdir -p "$LAB/captures/$leg"
    hold
    trap 'put_sysctls_back; stop_units; teardown_ceph; restore' EXIT
    trap 'exit 143' INT TERM
    ab_n=0
    for side in $AB_ORDER; do
        ab_n=$((ab_n + 1))
        ab_wt=$(cd "$(side_var "$side" "")" && pwd)
        echo "== $(date -u +%FT%TZ) ab $tag: $side ($(git -C "$ab_wt" rev-parse --short HEAD)), $ab_n of $(echo $AB_ORDER | wc -w)"
        # the side's kernel settings on every host, for its capture alone
        ab_sysctl=$(side_var "$side" _SYSCTL)
        [ -z "$ab_sysctl" ] || set_sysctls "$ab_sysctl"
        # every path the capture names is the side's own: its inventory's node, its loader, its
        # dataset; and the inventory takes the side's overrides
        (
            cd "$ab_wt" || exit 1
            REPO=$ab_wt
            LOADER=$ab_wt/target/deploy/release/tmdb-dataset-loader
            SHOAL_INLINE=$(side_var "$side" _INLINE)
            SHOAL_WAL_DELAY=$(side_var "$side" _DELAY)
            SHOAL_LEASE_READS=$(side_var "$side" _LEASE)
            SHOAL_DISK_READ_MARGIN=$(side_var "$side" _MARGIN)
            SHOAL_OBJECT_CACHE=$(side_var "$side" _CACHE)
            SHOAL_PLAINTEXT_LANES=$(side_var "$side" _PLAIN)
            SHOAL_MEMORY=$(side_var "$side" _MEMORY)
            SHOAL_OBJECT_CONGESTION=$(side_var "$side" _CONGESTION)
            SHOAL_SLOW_START_AFTER_IDLE=$(side_var "$side" _SLOWSTART)
            slow_start_of_hosts before
            capture "$leg" shoal "$group" "$tag-$side-$ab_n" "$FLAGS $(side_var "$side" _FLAGS)"
            slow_start_of_hosts after
        ) || die "the $side capture of $tag failed"
        # and every host's settings as they were before it
        put_sysctls_back
    done
}

# every host's net.ipv4.tcp_slow_start_after_idle, before or after a capture: a deploy since F113 sets
# it on every node's host, and the bench's destroy puts each host's own back, which this shows
slow_start_of_hosts() {
    echo "== hosts' tcp_slow_start_after_idle $1 the capture: $(for h in $HOSTS; do printf '%s=%s ' "$h" "$(on "$h" "cat /proc/sys/net/ipv4/tcp_slow_start_after_idle" < /dev/null)"; done)"
}

# every host's kernel settings, given as name=value words, each old value kept under $LAB to be put
# back; a congestion control the setting loads is unloaded again when it is put back
set_sysctls() {
    for h in $HOSTS; do
        for kv in $1; do
            name=${kv%%=*}
            value=${kv#*=}
            old=$(on "$h" "sysctl -n $name")
            echo "$name=$old" >> "$LAB/sysctl-$h"
            # a congestion control is loaded for its setting, and unloaded again on the way back
            if [ "$name" = net.ipv4.tcp_congestion_control ] && [ "$(on "$h" "lsmod | grep -c '^tcp_$value '")" = 0 ]; then
                on "$h" "sudo modprobe tcp_$value"
                echo "tcp_$value" >> "$LAB/modules-$h"
                changed "$h: module tcp_$value loaded"
            fi
            on "$h" "sudo sysctl -q -w $kv"
            changed "$h: sysctl $name $value (was $old)"
        done
    done
}

# every host's kernel settings put back as set_sysctls found them, in reverse order so a setting
# changed twice ends at its first value, and any congestion control it loaded unloaded
put_sysctls_back() {
    for h in $HOSTS; do
        if [ -f "$LAB/sysctl-$h" ]; then
            tac "$LAB/sysctl-$h" | while IFS= read -r kv; do
                on "$h" "sudo sysctl -q -w $kv" < /dev/null
                changed "$h: sysctl ${kv%%=*} ${kv#*=} put back"
            done
            rm -f "$LAB/sysctl-$h"
        fi
        if [ -f "$LAB/modules-$h" ]; then
            while IFS= read -r module; do
                if on "$h" "sudo modprobe -r $module" < /dev/null; then
                    changed "$h: module $module unloaded"
                else
                    changed "$h: module $module left loaded, in use"
                fi
            done < "$LAB/modules-$h"
            rm -f "$LAB/modules-$h"
        fi
    done
}

# a before and after in a unit of its own on europa, from a copy of this script, every AB_ variable
# and FLAGS handed to it, so a dropped ssh stops nothing
cmd_ab_start() {
    leg=$1; group=$2; tag=$3
    mkdir -p "$LAB/run"
    cp "$0" "$LAB/run/stores-lab-ab.sh"
    # every AB_ variable as one argument of its own, since AB_ORDER's value has spaces: a string of
    # them split by the shell made systemd-run take its second side as the program to run
    set --
    for ab_var in $(env | sed -n 's/^\(AB_[A-Za-z0-9_]*\)=.*/\1/p'); do
        set -- "$@" "--setenv=$ab_var=$(side_raw "$ab_var")"
    done
    # refusing a manual stop or restart, as a leg's unit does
    sudo systemd-run --unit="stores-ab-$tag" --collect -p User="$(id -un)" -p Group="$(id -gn)" \
        -p KillMode=mixed -p TimeoutStopSec=600 -p RefuseManualStop=yes \
        -p WorkingDirectory="$REPO" --setenv=HOME="$HOME" --setenv=PATH="$PATH" \
        --setenv=LAB="$LAB" --setenv=OUT="$OUT" --setenv=FLAGS="$FLAGS" "$@" \
        sh -c "exec sh $LAB/run/stores-lab-ab.sh ab $leg $group $tag >> $LAB/run/ab-$tag.log 2>&1"
    echo "started stores-ab-$tag; tail -f $LAB/run/ab-$tag.log"
}

# a variable's value by its name
side_raw() {
    eval "printf '%s' \"\${$1:-}\""
}

# a before and after's captures slimmed into the results
cmd_ab_fetch() {
    tag=$1
    mkdir -p "$OUT/stores-ab/$tag"
    for dir in "$LAB"/captures/*/"$tag"-*/; do
        [ -d "$dir" ] || continue
        name=$(basename "$dir")
        # this tag's own captures, <tag>-<side>-<n>: another tag that begins with this one, as
        # f96-loop-rep3-patch begins with f96-loop-rep3, has a dash more and is its own
        case ${name#"$tag"-} in
            *-*-*) continue ;;
        esac
        "$ADM" bench slim "$dir" -o "$OUT/stores-ab/$tag/$name.json"
    done
    ls "$OUT/stores-ab/$tag"
}

cmd_status() {
    systemctl list-units --plain --no-legend 'stores-lab-*' 'stores-ab-*' | cat
    tail -n 5 "$LAB"/run/*.log 2>/dev/null
}

# every capture of a leg slimmed into the results, the series of the arms the charts draw kept; or
# only the captures FETCH_ONLY names, space separated, since a leg's directory holds every before
# and after taken on its devices too
cmd_fetch() {
    leg=$1
    mkdir -p "$OUT/stores/$leg"
    for dir in "$LAB/captures/$leg"/*/; do
        name=$(basename "$dir")
        if [ -n "${FETCH_ONLY:-}" ]; then
            case " $FETCH_ONLY " in *" $name "*) ;; *) continue ;; esac
        fi
        "$ADM" bench slim "$dir" --keep-series put100/s1m/c16/,get100/s1m/c16/,put100/s4k/c16/ -o "$OUT/stores/$leg/$name.json"
    done
}

# warp against MinIO and Ceph on the ssd leg, beside the driver's own figures for the same arms
cmd_warp() {
    [ -x "$LAB/bin/warp" ] || die "no $LAB/bin/warp"
    mkdir -p "$OUT/stores/warp"
    hold
    trap 'stop_units; teardown_ceph; restore' EXIT
    trap 'exit 143' INT TERM
    for store in minio ceph; do
        if [ "$store" = minio ]; then prepare ssd minio; deploy_minio ssd; sleep 15; port=9000
        else prepare ssd ceph; deploy_ceph ssd; port=8080; fi
        hosts="172.16.2.10:$port,172.16.2.4:$port,172.16.2.5:$port"
        for op in put get; do
            for size in 4KiB 1MiB; do
                taskset -c $DRIVER_CPUS "$LAB/bin/warp" $op --host "$hosts" --access-key "$AWS_ACCESS_KEY_ID" --secret-key "$AWS_SECRET_ACCESS_KEY" \
                    --bucket "warp-$store" --obj.size "$size" --concurrent 16 --duration 30s --noclear=false --no-color \
                    --benchdata "$OUT/stores/warp/$store-$op-$size" > "$OUT/stores/warp/$store-$op-$size.txt" 2>&1
            done
        done
        if [ "$store" = minio ]; then stop_units; else teardown_ceph; fi
    done
}

case "${1:-}" in
    snapshot) cmd_snapshot ;;
    verify) cmd_verify ;;
    stage) cmd_stage ;;
    unstage) cmd_unstage ;;
    ceph-tools) setup_ceph_tools ;;
    ceph-tools-down) teardown_ceph_tools ;;
    ceph-down) teardown_ceph ;;
    # the hosts put back by hand, after a leg that was killed before its trap could
    restore) stop_units; restore ;;
    devices) cmd_devices "$2" ;;
    devices-down) cmd_devices_down "$2" ;;
    quick) cmd_quick "$2" ;;
    leg) cmd_leg "$2" ;;
    start) cmd_start "$2" ;;
    stop) cmd_stop "$2" ;;
    status) cmd_status ;;
    fetch) cmd_fetch "$2" ;;
    ab) cmd_ab "$2" "$3" "$4" ;;
    ab-start) cmd_ab_start "$2" "$3" "$4" ;;
    ab-fetch) cmd_ab_fetch "$2" ;;
    warp) cmd_warp ;;
    # one store's capture by hand, for trying a deployment: try <leg> <store> <group>, which runs
    # `capture` from a copy of this file, since sh reads a script as it runs it
    try) mkdir -p "$LAB/run"; cp "$0" "$LAB/run/try.sh"; exec sh "$LAB/run/try.sh" capture "$2" "$3" "$4" ;;
    capture) hold; trap 'stop_units; teardown_ceph; restore' EXIT; trap 'exit 143' INT TERM
             [ "$3" = ceph ] && { prepare "$2" ceph; deploy_ceph "$2"; }
             capture "$2" "$3" "$4" "$3-$4-try" "${FLAGS:-$QUICK_FLAGS}" ;;
    *) sed -n '2,33p' "$0"; exit 2 ;;
esac
