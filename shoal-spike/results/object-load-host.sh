#!/bin/sh
# F85 on the lab node, one round of one leg: the reference cell alone, beside an idle object
# executor, and beside the device store's load at every rate, the order rotating by round and
# reversed in even ones. Run by object-load-lab.sh in /var/tmp/f85 on the host:
#
#   sh object-load-host.sh <leg> <device> <round>
#
# where <device> is the directory the load's device is made under, emptied by every run that has
# one. The tables' storage is wiped before every run. Every cell runs with locked memory unlimited,
# as a deployed node does.
set -u
cd /var/tmp/f85
LEG=$1
DEVICE=$2
ROUND=$3
RATES=${RATES:-100 500}
OBJECT_CPU=${OBJECT_CPU:-1}
DRIVER_CPU=${DRIVER_CPU:-0}
CLIENT_CPUS=${CLIENT_CPUS:-6,7}
RING_MIB=${RING_MIB:-256}
PSR=${PSR:-}
PORT=${PORT:-13900}
ID=macro/grid/unsorted/r50/1024
mkdir -p out shoal
# locked memory without limit, as a deployed node's unit has it (`LimitMEMLOCK=infinity`): under
# the login's 8 MiB glommio cannot register its buffers and every shard writes without them
sudo -n prlimit --pid $$ --memlock=unlimited:unlimited
echo "memlock $(ulimit -l)"

# every side of the leg: the cell alone, beside an executor with nothing to do, then each rate
sides="alone idle"
for rate in $RATES; do
    sides="$sides load:$rate"
done

# the list started at a word the round moves along, and reversed in even rounds
set -- $sides
count=$#
shift_by=$(( (ROUND - 1) % count ))
order=""
tail=""
index=0
for side in "$@"; do
    if [ "$index" -lt "$shift_by" ]; then
        tail="$tail $side"
    else
        order="$order $side"
    fi
    index=$((index + 1))
done
order="$order $tail"
if [ $((ROUND % 2)) -eq 0 ]; then
    reversed=""
    for side in $order; do
        reversed="$side $reversed"
    done
    order=$reversed
fi

for side in $order; do
    # the side's rate, and the environment that asks for it: none for the cell alone
    case $side in
        alone) name="$LEG-alone-r$ROUND"; rate="" ;;
        idle) name="$LEG-idle-r$ROUND"; rate=0 ;;
        load:*) rate=${side#load:}; name="$LEG-load-$rate-r$ROUND" ;;
    esac
    # the tables start empty every run
    rm -rf shoal/*
    start=$(date +%s.%N)
    if [ -z "$rate" ]; then
        taskset -c "$CLIENT_CPUS" ./shoal-workload run --id "$ID" --conf conf.yml \
            --port "$PORT" --json "out/$name.json" > "out/$name.log" 2>&1 &
    else
        env SHOAL_OBJECT_LOAD_DEVICE="$DEVICE" SHOAL_OBJECT_LOAD_CPU="$OBJECT_CPU" \
            SHOAL_OBJECT_LOAD_DRIVER_CPU="$DRIVER_CPU" SHOAL_OBJECT_LOAD_RATE_MIB="$rate" \
            SHOAL_OBJECT_LOAD_RING_MIB="$RING_MIB" SHOAL_OBJECT_LOAD_REPORT="out/$name.load.json" \
            taskset -c "$CLIENT_CPUS" ./shoal-workload run --id "$ID" --conf conf.yml \
            --port "$PORT" --json "out/$name.json" > "out/$name.log" 2>&1 &
    fi
    pid=$!
    # where every thread ran, read once halfway through, when asked
    if [ -n "$PSR" ]; then
        sleep 1.5
        ps -L -o tid=,psr=,pcpu=,comm= -p "$pid" > "out/$name.psr" 2>&1
    fi
    wait "$pid"
    code=$?
    echo "round $ROUND $LEG $side exit $code $(awk -v a="$start" -v b="$(date +%s.%N)" 'BEGIN { printf "%.1f", b - a }')s"
    # glommio warns when it cannot register its buffers, which changes what a shard writes with
    grep -h "registering\|register_buffers" "out/$name.log" | head -1
done
