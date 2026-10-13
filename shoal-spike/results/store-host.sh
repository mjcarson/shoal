#!/bin/sh
# F85's X6 retake on one host: shoal-tmdb stopped and the governor at performance for the run, both
# put back on exit however it ends. Args: <dir> <core> <host label>
set -u
DIR=$1
CORE=$2
LABEL=$3
SAVED=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)
restore() {
    sudo -n cpupower frequency-set -g "$SAVED" >/dev/null
    sudo -n systemctl start shoal-tmdb
    sudo -n rm -rf "$DIR"
    echo "restored governor $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor), shoal-tmdb $(systemctl is-active shoal-tmdb)"
}
trap restore EXIT
trap 'exit 143' INT TERM
sudo -n systemctl stop shoal-tmdb
sudo -n cpupower frequency-set -g performance >/dev/null
echo "governor $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor), was $SAVED"
for round in 1 2 3; do
    sudo -n /var/tmp/f85/shoal-spike store all --only small,whole --dir "$DIR" --core "$CORE" \
        --leg "$LABEL-r$round" --out "/var/tmp/f85/store-$LABEL-r$round.json"
done
sudo -n /var/tmp/f85/shoal-spike store all --only listing --dir "$DIR" --core "$CORE" \
    --leg "$LABEL-listing" --out "/var/tmp/f85/store-$LABEL-listing.json"
