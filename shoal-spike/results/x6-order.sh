#!/bin/sh
# Does the partial write's cost depend on what ran before it? Alternate: idle first, frag first.
set -u
BIN=/var/tmp/x6/shoal-spike-r
OUT=/var/tmp/x6/out/order
mkdir -p $OUT
GOV=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)
trap 'cpupower frequency-set -g $GOV >/dev/null' EXIT
trap 'pkill -P $$; exit 143' INT TERM
cpupower frequency-set -g performance >/dev/null
for r in 1 2; do
  fstrim /x6/xfs
  sleep 120
  echo "idle-then-partial $r start $(date -u +%T) temp $(cat /sys/class/nvme/nvme0/hwmon*/temp1_input 2>/dev/null)"
  $BIN device partial --dir /x6/xfs/x6 --expect-fs xfs --round $r --leg "hyperion xfs idle-first" --out $OUT/idle-first.json > $OUT/idle-first-r$r.md 2>&1
  fstrim /x6/xfs
  sleep 120
  echo "frag-then-partial $r start $(date -u +%T) temp $(cat /sys/class/nvme/nvme0/hwmon*/temp1_input 2>/dev/null)"
  $BIN device all --only frag,partial --dir /x6/xfs/x6 --expect-fs xfs --round $r --leg "hyperion xfs frag-first" --out $OUT/frag-first.json > $OUT/frag-first-r$r.md 2>&1
done
echo "done $(date -u +%T)"
