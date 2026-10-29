#!/bin/sh
# F86's previous-state cost on one host: shoal-tmdb stopped and the governor at performance for the
# run, both put back on exit however it ends. Each round applies a unit and a whole chunk back to
# back with a chunk's previous state not kept, then kept. Args: <dir> <leg> <governor to restore>;
# ROUNDS="1 2 3 4" names the rounds. Records in results/store/previous-<leg>-r<round>.json
DIR=$1
LEG=$2
GOV=$3
restore() {
    sudo rm -rf "$DIR"
    sudo cpupower frequency-set -g "$GOV" >/dev/null
    sudo systemctl start shoal-tmdb
}
trap restore EXIT
sudo systemctl stop shoal-tmdb
sudo cpupower frequency-set -g performance >/dev/null
echo "$(hostname) $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor) $(lscpu | grep 'Model name' | sed 's/.*: *//')"
for round in ${ROUNDS:-1}; do
    echo "== round $round $(date -u +%T)"
    sudo prlimit --memlock=unlimited:unlimited /var/tmp/f86/shoal-spike store all --only previous \
        --dir "$DIR" --core 2 --leg "$LEG" --out /var/tmp/f86/previous-$LEG-$round.json | grep '^| [uw]'
done
