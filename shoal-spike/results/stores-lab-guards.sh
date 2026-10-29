#!/bin/sh
# stores-lab.sh's guards (Resolved #306), each run against scratch devices on europa alone: a loop
# over a file in a scratch directory, a VG of its own on another, and transient units standing in
# for a bench cluster's node and an upgrade, all removed again. It takes stores-lab.sh's functions
# from a copy of the script with its dispatcher cut off, so an older script can be checked too:
#
#   sh shoal-spike/results/stores-lab-guards.sh [stores-lab.sh]
#   GUARDS_WT=<a clean worktree with tmdb-dataset built in its target/deploy> sh ...
#
# GUARDS_WT is the side a before and after's check names, this checkout unless set; a checkout with
# changes is refused by `ab` before the guard it checks, and that check is skipped, saying so. Needs
# passwordless sudo, mkfs.xfs, xfs_repair, LVM and the user `shoal`, as a leg does. Never run it
# while a leg runs: it drops the page cache. It never touches the hosts' record of changes, since
# the script's LAB and OUT are the scratch directory. Prints a line a check, PASS, FAIL or SKIP,
# and exits non-zero if any failed.
set -u

SCRIPT=${1:-shoal-spike/results/stores-lab.sh}
GUARDS_WT=${GUARDS_WT:-$(pwd)}
[ -f "$SCRIPT" ] || { echo "no $SCRIPT"; exit 2; }
busy=$(systemctl list-units --plain --no-legend 'stores-lab-*' 'stores-ab-*' 'shoal-*-bench*' | awk '{print $1}')
[ -z "$busy" ] || { echo "a leg or a bench cluster runs ($busy): never beside one"; exit 2; }
SCRATCH=$(mktemp -d "${TMPDIR:-/tmp}/stores-lab-guards.XXXXXX")
mkdir -p "$SCRATCH/lab/captures/ssd" "$SCRATCH/out"
sed '/^case "\${1:-}" in$/,$d' "$SCRIPT" > "$SCRATCH/lib.sh"
failed=0

# a check's verdict
verdict() {
    if [ "$1" = 0 ]; then echo "PASS $2"; else echo "FAIL $2"; failed=1; fi
}

# a loop detached only while it is still over the given file, gone or not: a loop number freed is
# anybody's to take
detach_ours() {
    case "$(losetup -nO BACK-FILE "$1" 2>/dev/null)" in
        "$2"|"$2 (deleted)") sudo losetup -d "$1" ;;
    esac
}

# one of the script's functions in a shell of its own, against europa alone, every line it printed
# kept in the scratch directory; `hold` is stubbed when HOLD_STUB is set, so a guard that lets a run
# through stops at the hold and never touches the host
run() {
    (
        LAB=$SCRATCH/lab OUT=$SCRATCH/out
        . "$SCRATCH/lib.sh"
        HOSTS=europa
        if [ -n "${HOLD_STUB:-}" ]; then
            hold() { echo "the hold was reached"; exit 9; }
        fi
        "$@"
    ) >> "$SCRATCH/run.log" 2>&1
}

# a scratch XFS on a loop over a file, mounted at $SCRATCH/mnt with a file of 64 MiB written in it;
# prints the loop device
scratch_xfs() {
    truncate -s 512M "$SCRATCH/dev.img"
    sx_dev=$(sudo losetup --find --show "$SCRATCH/dev.img")
    [ "$(losetup -nO BACK-FILE "$sx_dev" | tr -d ' ')" = "$SCRATCH/dev.img" ] || { echo "$sx_dev is not the scratch loop" >&2; exit 2; }
    sudo mkfs.xfs -q -f "$sx_dev"
    mkdir -p "$SCRATCH/mnt"
    sudo mount "$sx_dev" "$SCRATCH/mnt"
    sudo dd if=/dev/urandom of="$SCRATCH/mnt/data" bs=1M count=64 status=none
    sync
    echo "$sx_dev"
}

# the scratch XFS's loop and file gone, whatever state they were left in
drop_scratch_xfs() {
    sudo umount "$SCRATCH/mnt" 2>/dev/null
    detach_ours "$1" "$SCRATCH/dev.img"
    rm -f "$SCRATCH/dev.img"
}

# the scratch VG and every loop over its file gone, whatever an older script left: a second loop
# first, so LVM sees its PV once
drop_scratch_vg() {
    sudo umount "$SCRATCH/lvmnt" 2>/dev/null
    dv_first=""
    for dv_loop in $loops; do
        if [ -z "$dv_first" ]; then dv_first=$dv_loop; else detach_ours "$dv_loop" "$SCRATCH/lv.img"; fi
    done
    sudo vgremove -q -y -f storesguardsvg > /dev/null 2>&1
    if [ -n "$dv_first" ] && [ -n "$(losetup -nO BACK-FILE "$dv_first" 2>/dev/null | grep -F "$SCRATCH/lv.img")" ]; then
        sudo pvremove -q "$dv_first" > /dev/null 2>&1
        detach_ours "$dv_first" "$SCRATCH/lv.img"
    fi
    rm -f "$SCRATCH/lv.img"
}

# 1. a device mounted and held, as a node holds its storage directory, is refused whole
dev=$(scratch_xfs)
sum=$(md5sum < "$SCRATCH/mnt/data")
(cd "$SCRATCH/mnt" && exec sleep 300) &
holder=$!
sleep 1
run fresh europa "$dev" "$SCRATCH/mnt" xfs
refused=$?
sync; echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
[ "$(md5sum < "$SCRATCH/mnt/data" 2>&1)" = "$sum" ]
kept=$?
kill "$holder"; wait "$holder" 2>/dev/null
sudo umount "$SCRATCH/mnt"
sudo xfs_repair -n "$dev" > /dev/null 2>&1
intact=$?
[ $refused != 0 ] && [ $kept = 0 ] && [ $intact = 0 ]
verdict $? "fresh refuses a device mounted and held, its data and filesystem untouched"
drop_scratch_xfs "$dev"

# 2. an idle device is made fresh as before: a new XFS, empty, mounted and the store user's
dev=$(scratch_xfs)
uuid=$(sudo blkid -s UUID -o value "$dev")
run fresh europa "$dev" "$SCRATCH/mnt" xfs
made=$?
[ $made = 0 ] && [ "$(sudo blkid -s UUID -o value "$dev")" != "$uuid" ] && [ -z "$(ls "$SCRATCH/mnt")" ] \
    && [ "$(stat -c %U "$SCRATCH/mnt")" = shoal ] && findmnt -rn "$SCRATCH/mnt" > /dev/null
verdict $? "fresh makes an idle device a new XFS, empty and the store user's"
drop_scratch_xfs "$dev"

# 3. a host running a unit of a cluster the bench made is refused, and one with none is not
run no_bench_cluster
clear=$?
sudo systemd-run --quiet --unit=shoal-guards-bench --collect sleep 120
run no_bench_cluster
blocked=$?
sudo systemctl kill shoal-guards-bench
[ $clear = 0 ] && [ $blocked != 0 ]
verdict $? "a cluster the bench made, left running, is refused before any device is touched"

# 4. no second loop over a file, and a removal refused is recorded as refused, its file kept
truncate -s 512M "$SCRATCH/lv.img"
run make_loop_lv europa "$SCRATCH/lv.img" 512M storesguardsvg
first=$?
run make_loop_lv europa "$SCRATCH/lv.img" 512M storesguardsvg
second=$?
loops=$(losetup -j "$SCRATCH/lv.img" | cut -d: -f1 | tr '\n' ' ')
[ $first = 0 ] && [ $second != 0 ] && [ "$(echo $loops | wc -w)" = 1 ]
verdict $? "a second loop over a file a loop is over is refused"
if [ "$(echo $loops | wc -w)" = 1 ] && sudo mkfs.xfs -q -f /dev/storesguardsvg/data 2>/dev/null; then
    mkdir -p "$SCRATCH/lvmnt" && sudo mount /dev/storesguardsvg/data "$SCRATCH/lvmnt"
    run drop_loop_lv europa "$SCRATCH/lv.img" storesguardsvg
    [ -f "$SCRATCH/lv.img" ] && grep -q "not removed, and $SCRATCH/lv.img kept" "$SCRATCH/out/stores-host-changes.txt"
    verdict $? "a loop's VG that cannot be removed keeps its file, and the record says so"
    sudo umount "$SCRATCH/lvmnt"
    run drop_loop_lv europa "$SCRATCH/lv.img" storesguardsvg
    [ ! -e "$SCRATCH/lv.img" ] && ! sudo vgs storesguardsvg > /dev/null 2>&1 \
        && [ -z "$(losetup -a | grep -F "$SCRATCH/lv.img")" ]
    verdict $? "and once unmounted, its LV, VG, PV, loop and file are removed"
else
    verdict 1 "a loop's VG that cannot be removed keeps its file (no single VG to try it on)"
fi
drop_scratch_vg

# 5. a before and after refuses a capture that is there, before its hold, and takes it again only
# when told to
mkdir -p "$SCRATCH/lab/captures/ssd/guards-a-1"
HOLD_STUB=1 AB_a=$GUARDS_WT AB_ORDER=a FLAGS=x run cmd_ab ssd ec21 guards
there=$?
HOLD_STUB=1 AB_OVERWRITE=1 AB_a=$GUARDS_WT AB_ORDER=a FLAGS=x run cmd_ab ssd ec21 guards
told=$?
if grep -q "uncommitted changes\|build tmdb-dataset" "$SCRATCH/run.log"; then
    echo "SKIP a before and after refuses a capture that is there ($GUARDS_WT has changes or no target/deploy build; GUARDS_WT names another)"
else
    [ $there = 1 ] && [ $told = 9 ]
    verdict $? "a before and after refuses a capture that is there, unless AB_OVERWRITE=1"
fi

# 6. a hold waits out an upgrade already running: its own wait, read from the script, on a stand-in
# oneshot unit that runs five seconds
wait_line=$(grep -o 'while systemctl show -p ActiveState --value apt-daily.service apt-daily-upgrade.service[^;]*; do sleep [0-9]*; done' "$SCRIPT")
if [ -z "$wait_line" ]; then
    verdict 1 "a hold waits out an unattended upgrade already running"
else
    sudo systemd-run --quiet --unit=stores-guards-upgrade -p Type=oneshot --no-block sleep 5
    sleep 1
    started=$(date +%s)
    sh -c "$(echo "$wait_line" | sed 's/apt-daily.service apt-daily-upgrade.service/stores-guards-upgrade.service/; s/sleep [0-9]*;/sleep 1;/')"
    waited=$(($(date +%s) - started))
    sudo systemctl reset-failed stores-guards-upgrade 2>/dev/null
    [ $waited -ge 3 ]
    verdict $? "a hold waits out an unattended upgrade already running ($waited s of 4)"
fi

# 7. a leg's and a before and after's units refuse a manual stop and a restart
[ "$(grep -A1 'systemd-run --unit="stores-\(lab\|ab\)-' "$SCRIPT" | grep -c 'RefuseManualStop=yes')" = 2 ]
named=$?
sudo systemd-run --quiet --unit=stores-guards-unit --collect -p RefuseManualStop=yes sleep 120
sudo systemctl restart stores-guards-unit > /dev/null 2>&1
restarted=$?
sudo systemctl kill stores-guards-unit
[ $named = 0 ] && [ $restarted != 0 ]
verdict $? "a leg's and a before and after's units refuse a manual stop and a restart"

rm -rf "$SCRATCH"
exit $failed
