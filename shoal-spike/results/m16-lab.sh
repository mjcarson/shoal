#!/bin/sh
# F88 on the lab: a second pool device on every host, so a device that fails or is added has
# somewhere for its placement groups to go (`docs/src/features/recovery-and-moves.md#on-the-lab`).
# Two devices on one filesystem are refused by the claim, so each host's second device is a
# filesystem of its own: an XFS logical volume beside /xfs on titan and hyperion, in ubuntu-vg's
# free space on the 970 EVO the first device is on, and on europa, whose volume group has no free
# space, an XFS image on the Optane mounted through a loop device. Run from the repository root on
# europa:
#
#   sh shoal-spike/results/m16-lab.sh setup       # the volumes, mounted, in fstab, and their dirs
#   sh shoal-spike/results/m16-lab.sh status      # what each host has of it
#   sh shoal-spike/results/m16-lab.sh teardown    # everything setup made, gone
#   sh shoal-spike/results/m16-lab.sh verify      # each host as it was found
#
# Each mount is in /etc/fstab with `nofail`, so a host that reboots comes up with its second
# device or without it, never stuck; a node whose device is not mounted finds its directory
# missing and reports the device failed. Every change to a host is appended to
# m16-host-changes.txt. Never point it at a disk with data: setup makes new filesystems only on
# the volume and the image it creates, and refuses either if it exists.
set -u

DIR=shoal-spike/results
CHANGES=$DIR/m16-host-changes.txt
HOSTS=${HOSTS:-europa titan hyperion}
SIZE=${SIZE:-40G}
# the devices' directories the tmdb cluster's inventory names
POOL_DIR=shoal-tmdb-pool2

# run a script on a host under sh, the script on stdin so no login shell gives its words a meaning;
# europa is this host
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

# where each host's second device is mounted
mount_of() {
    case $1 in
        europa) echo /optane2 ;;
        *) echo /xfs2 ;;
    esac
}

# make a host's second filesystem, mount it, put it in fstab and make the device's directory
setup_host() {
    host=$1
    mnt=$(mount_of "$host")
    case $host in
        europa)
            img=/optane/shoal-m16-dev2.img
            on "$host" "set -e
                test ! -e $img || { echo '$img exists; refusing'; exit 1; }
                sudo -n fallocate -l $SIZE $img
                sudo -n mkfs.xfs -q $img
                sudo -n mkdir -p $mnt
                echo '$img $mnt xfs loop,nofail 0 0' | sudo -n tee -a /etc/fstab >/dev/null
                sudo -n mount $mnt
                sudo -n mkdir -p $mnt/$POOL_DIR" || return 1
            changed "$host" "fallocate $SIZE $img, mkfs.xfs on it, mkdir $mnt, fstab '$img $mnt xfs loop,nofail', mount, mkdir $mnt/$POOL_DIR"
            ;;
        *)
            lv=/dev/ubuntu-vg/xfs2
            on "$host" "set -e
                test ! -e $lv || { echo '$lv exists; refusing'; exit 1; }
                sudo -n lvcreate -q -y -L $SIZE -n xfs2 ubuntu-vg
                sudo -n mkfs.xfs -q $lv
                sudo -n mkdir -p $mnt
                echo '$lv $mnt xfs defaults,nofail 0 0' | sudo -n tee -a /etc/fstab >/dev/null
                sudo -n mount $mnt
                sudo -n mkdir -p $mnt/$POOL_DIR" || return 1
            changed "$host" "lvcreate -L $SIZE -n xfs2 ubuntu-vg, mkfs.xfs on it, mkdir $mnt, fstab '$lv $mnt xfs defaults,nofail', mount, mkdir $mnt/$POOL_DIR"
            ;;
    esac
}

# undo everything setup_host made, whatever of it there is
teardown_host() {
    host=$1
    mnt=$(mount_of "$host")
    case $host in
        europa) what=/optane/shoal-m16-dev2.img ;;
        *) what=/dev/ubuntu-vg/xfs2 ;;
    esac
    on "$host" "if mountpoint -q $mnt; then sudo -n umount $mnt; fi
        sudo -n sed -i '\\#^$what $mnt xfs #d' /etc/fstab
        if [ -d $mnt ]; then sudo -n rmdir $mnt; fi
        case $what in
            /dev/*) if [ -e $what ]; then sudo -n lvremove -q -y $what; fi ;;
            *) if [ -e $what ]; then sudo -n rm -f $what; fi ;;
        esac"
    changed "$host" "umount $mnt, fstab line for $what removed, rmdir $mnt, $what removed"
}

# what a host has of it
status_host() {
    host=$1
    mnt=$(mount_of "$host")
    on "$host" "echo \"$host: \$(findmnt -n -o SOURCE,FSTYPE $mnt 2>/dev/null || echo 'not mounted') \$(grep -c ' $mnt ' /etc/fstab) fstab line(s) \$(test -d $mnt/$POOL_DIR && echo 'device dir' || echo 'no device dir')\""
}

# a host as it was found: no mount, no fstab line, no volume, no image
verify_host() {
    host=$1
    mnt=$(mount_of "$host")
    on "$host" "bad=0
        if mountpoint -q $mnt 2>/dev/null; then echo '$host: $mnt still mounted'; bad=1; fi
        if grep -q ' $mnt ' /etc/fstab; then echo '$host: $mnt still in fstab'; bad=1; fi
        if [ -e /dev/ubuntu-vg/xfs2 ] || [ -e /optane/shoal-m16-dev2.img ]; then echo '$host: the volume or the image is still there'; bad=1; fi
        if [ \$bad = 0 ]; then echo '$host: as found'; fi"
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
    *)
        echo "usage: $0 setup|status|teardown|verify"
        exit 1
        ;;
esac
