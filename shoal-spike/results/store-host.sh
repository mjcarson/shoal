#!/bin/sh
# F85's X6 retake on one host: shoal-tmdb stopped and the governor at performance for the run, both
# put back on exit however it ends. Args: <dir> <core> <host label> [journal dir]
#
# Since F91 a fourth argument names a directory on the host's SSD for a disk's journal, as a node
# keeps one (`--journal-dir`), and SPIKE names the directory the binary and its records are in
# (/var/tmp/f85 unless set). LISTING=0 leaves the listing out. Since F96 ONLY names the sections the
# three rounds run (small,whole unless set), as `whole,short` retakes F96's figures.
set -u
DIR=$1
CORE=$2
LABEL=$3
JOURNAL=${4:-}
SPIKE=${SPIKE:-/var/tmp/f85}
LISTING=${LISTING:-1}
ONLY=${ONLY:-small,whole}
SAVED=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)
restore() {
    sudo -n cpupower frequency-set -g "$SAVED" >/dev/null
    sudo -n systemctl start shoal-tmdb
    sudo -n rm -rf "$DIR"
    if [ -n "$JOURNAL" ]; then sudo -n rm -rf "$JOURNAL"; fi
    echo "restored governor $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor), shoal-tmdb $(systemctl is-active shoal-tmdb)"
}
trap restore EXIT
trap 'exit 143' INT TERM
sudo -n systemctl stop shoal-tmdb
sudo -n cpupower frequency-set -g performance >/dev/null
echo "governor $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor), was $SAVED"
# the journal's directory on the SSD, when the scratch directory is a disk's
set --
if [ -n "$JOURNAL" ]; then set -- --journal-dir "$JOURNAL"; fi
for round in 1 2 3; do
    sudo -n "$SPIKE/shoal-spike" store all --only "$ONLY" --dir "$DIR" --core "$CORE" "$@" \
        --leg "$LABEL-r$round" --out "$SPIKE/store-$LABEL-r$round.json"
done
if [ "$LISTING" = 1 ]; then
    sudo -n "$SPIKE/shoal-spike" store all --only listing --dir "$DIR" --core "$CORE" "$@" \
        --leg "$LABEL-listing" --out "$SPIKE/store-$LABEL-listing.json"
fi
