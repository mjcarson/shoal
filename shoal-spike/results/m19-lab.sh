#!/bin/sh
# F91 on the lab: the tmdb cluster's disk pool, `bulk`, on each host's disk at /hdd, its journal on
# the host's SSD (`docs/src/features/rotational-devices.md#on-the-lab`). A device's path and a
# journal's directory are never created by a node or by deploy, so this makes them, owned by the
# node's user, for the cluster; a bench's side cluster makes its own beside them (`-bench`) and
# removes them once empty. The disks' write caches are turned off by `shoaladm deploy`, which also
# installs the udev rule that keeps them off; `status` reads both. Run from the repository root on
# europa:
#
#   sh shoal-spike/results/m19-lab.sh setup       # the directories, each host's disk and journal
#   sh shoal-spike/results/m19-lab.sh status      # the directories, each disk's cache and its rule
#   sh shoal-spike/results/m19-lab.sh teardown    # the directories setup made, emptied and gone
#   sh shoal-spike/results/m19-lab.sh verify      # each host as it was found, the cache aside
#
# Every change to a host is appended to m19-host-changes.txt. teardown leaves a disk's cache off and
# its udev rule in place, as `shoaladm destroy` does: nothing here turned it on, and X7 left the
# disks with it on only because no node ran on them. `restore-cache` turns it on again, and removes
# the rule, for a host that should go back to how X12 left it.
set -u

DIR=shoal-spike/results
CHANGES=$DIR/m19-host-changes.txt
HOSTS=${HOSTS:-europa titan hyperion}
USER_NAME=${USER_NAME:-shoal}

# run a script on a host under sh, the script on stdin; europa is this host
on() {
    host=$1
    shift
    if [ "$host" = "europa" ]; then
        printf '%s\n' "$*" | sh -s
    else
        printf '%s\n' "$*" | ssh -o BatchMode=yes "$host" sh -s
    fi
}

# record a change to a host, with when it was made
changed() {
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $1: $2" >> "$CHANGES"
}

# the directory each host's disks journal in, on its SSD: the Optane on europa, the 970 EVO's first
# XFS volume on titan and hyperion, as tmdb_cluster.yaml names them
journal_of() {
    case $1 in
        europa) echo /optane/shoal-tmdb-journal ;;
        *) echo /xfs/shoal-tmdb-journal ;;
    esac
}

# every directory a host's disk pool needs: the cluster's
dirs_of() {
    echo "/hdd/shoal-tmdb-bulk $(journal_of "$1")"
}

# every directory setup or a bench may have left: the cluster's and a bench's side cluster's
all_dirs_of() {
    j=$(journal_of "$1")
    echo "/hdd/shoal-tmdb-bulk /hdd/shoal-tmdb-bulk-bench $j $j-bench"
}

# make a host's directories, owned by the node's user; refuse a disk that is not the empty XFS at
# /hdd X7 and X12 left
setup_host() {
    host=$1
    dirs=$(dirs_of "$host")
    on "$host" "set -e
        test \"\$(findmnt -n -o FSTYPE /hdd)\" = xfs || { echo '$host: /hdd is not an XFS mount; refusing'; exit 1; }
        for d in $dirs; do
            if [ -e \$d ]; then echo \"$host: \$d is there already\"; continue; fi
            sudo -n mkdir \$d
            sudo -n chown $USER_NAME: \$d
            echo \"$host: made \$d\"
        done" || return 1
    changed "$host" "mkdir and chown $USER_NAME: $dirs (those not there)"
}

# empty and remove what setup made, whatever of it there is; the cache is left as it is
teardown_host() {
    host=$1
    dirs=$(all_dirs_of "$host")
    on "$host" "for d in $dirs; do
            if [ -d \$d ]; then sudo -n rm -rf \$d; echo \"$host: removed \$d\"; fi
        done"
    changed "$host" "rm -rf $dirs"
}

# what a host has: its directories, its disk's cache and the udev rule keeping it off
status_host() {
    host=$1
    dirs=$(all_dirs_of "$host")
    on "$host" "for d in $dirs; do
            if [ -d \$d ]; then echo \"$host: \$d (\$(stat -c %U \$d), \$(ls -A \$d | wc -l) entries)\"; else echo \"$host: no \$d\"; fi
        done
        disk=\$(lsblk -ndo PKNAME \$(findmnt -n -o SOURCE /hdd) | head -n1)
        echo \"$host: \$disk rotational \$(cat /sys/block/\$disk/queue/rotational), write cache \$(cat /sys/block/\$disk/queue/write_cache)\"
        ls /etc/udev/rules.d/61-shoal-write-cache-*.rules 2>/dev/null | sed \"s|^|$host: rule |\" || true"
}

# a host as it was found: none of the directories, whatever its cache now says
verify_host() {
    host=$1
    dirs=$(all_dirs_of "$host")
    on "$host" "bad=0
        for d in $dirs; do
            if [ -e \$d ]; then echo \"$host: \$d is still there\"; bad=1; fi
        done
        if [ \$bad = 0 ]; then echo '$host: as found'; fi"
}

# turn a host's disk's cache on again and remove the rule, as X12 left the disks
restore_cache_host() {
    host=$1
    on "$host" "disk=\$(lsblk -ndo PKNAME \$(findmnt -n -o SOURCE /hdd) | head -n1)
        sudo -n rm -f /etc/udev/rules.d/61-shoal-write-cache-*.rules
        sudo -n udevadm control --reload
        sudo -n hdparm -W1 /dev/\$disk >/dev/null
        echo 1 | sudo -n tee /sys/block/\$disk/device/rescan >/dev/null
        sleep 2
        echo \"$host: \$disk write cache \$(cat /sys/block/\$disk/queue/write_cache)\""
    changed "$host" "udev rule removed, hdparm -W1 and a rescan of the disk under /hdd"
}

case ${1:-status} in
    setup)
        for host in $HOSTS; do
            setup_host "$host" || { echo "setup failed on $host"; exit 1; }
        done
        for host in $HOSTS; do status_host "$host"; done
        ;;
    teardown)
        for host in $HOSTS; do teardown_host "$host"; done
        ;;
    verify)
        for host in $HOSTS; do verify_host "$host"; done
        ;;
    status)
        for host in $HOSTS; do status_host "$host"; done
        ;;
    restore-cache)
        for host in $HOSTS; do restore_cache_host "$host"; done
        ;;
    *)
        echo "usage: $0 setup|status|teardown|verify|restore-cache"
        exit 1
        ;;
esac
