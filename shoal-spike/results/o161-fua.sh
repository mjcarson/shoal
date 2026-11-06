#!/bin/sh
# O161's first measurement (2026-11-06): on a lab host's drive, 4 KiB direct writes into a file whose
# extents are written, made durable two ways, each for SECS seconds at one in flight: opened O_DSYNC,
# which XFS completes as FUA writes on a drive with FUA, and written then `fdatasync`ed, a flush each, as
# Shoal's WAL does. The host's whole-NVMe flushes are counted around each, and fio's latencies kept.
# Run on the host, from its shell, with the tmdb node stopped for the run and started again on every
# way out; the file is removed. Usage: sh o161-fua.sh [dir] [secs]
set -u
DIR=${1:-/xfs}
SECS=${2:-20}
FILE=$DIR/o161-fua.bin
OUT=${OUT:-/var/tmp/o161}
DEV=nvme0n1
mkdir -p "$OUT"
restore() {
    rm -f "$FILE"
    sudo systemctl start shoal-tmdb
    echo "restored: $FILE removed, shoal-tmdb $(systemctl is-active shoal-tmdb)"
}
trap restore EXIT INT TERM
sudo systemctl stop shoal-tmdb
# the drive's own say on FUA and its cache
echo "fua $(cat /sys/block/$DEV/queue/fua) cache $(cat /sys/block/$DEV/queue/write_cache)" | tee "$OUT/drive.txt"
flushes() { awk -v d=$DEV '$3 == d {print $19}' /proc/diskstats; }
# the file written whole first, so every write after lands in a written extent and changes no metadata
fio --name=prep --filename="$FILE" --size=256M --rw=write --bs=1M --direct=1 --end_fsync=1 > /dev/null
for way in dsync fdatasync; do
    case $way in
        dsync) flags="--sync=dsync" ;;
        fdatasync) flags="--fdatasync=1" ;;
    esac
    before=$(flushes)
    fio --name=$way --filename="$FILE" --size=256M --rw=randwrite --bs=4k --direct=1 --ioengine=psync \
        --iodepth=1 --runtime="$SECS" --time_based $flags --output-format=json > "$OUT/$way.json"
    after=$(flushes)
    python3 -I -c "
import json, sys
job = json.load(open(sys.argv[1]))['jobs'][0]
write = job['write']
lat = write['clat_ns']['percentile']
line = '%-9s %6.0f durable writes a second  flushes %7d  write p50 %6.1f us p99 %6.1f us p99.9 %6.1f us' % (
    sys.argv[2], write['iops'], int(sys.argv[4]) - int(sys.argv[3]),
    lat['50.000000'] / 1e3, lat['99.000000'] / 1e3, lat['99.900000'] / 1e3)
# a write then fdatasync is durable only once its sync is, which fio times apart
sync = job.get('sync', {}).get('lat_ns', {}).get('percentile')
if sync:
    line += '  sync p50 %6.1f us p99 %6.1f us p99.9 %6.1f us' % (
        sync['50.000000'] / 1e3, sync['99.000000'] / 1e3, sync['99.900000'] / 1e3)
print(line)
" "$OUT/$way.json" "$way" "$before" "$after" | tee -a "$OUT/summary.txt"
done
