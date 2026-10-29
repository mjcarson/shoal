#!/bin/sh
# X9 on the lab node, one round of one leg: the reference cell alone and beside object work on the
# table shards (`shards`, its queue NotImportant; `shards-lat`, its queue with a latency goal;
# `shards-fine`, the supplement, steps of 64 KiB inside a unit under a shorter goal) and on a core
# of its own (`core`), at every rate and unit, the order rotating by round and reversed in even
# ones. Run by x9-lab.sh in /var/tmp/x9 on the host:
#
#   sh x9-host.sh <leg> <pool> <round>
#
# where <pool> is the directory or device node the object work writes to. The tables' storage is
# wiped before every run; the pool's rings are kept, as a slice keeps its file written ahead.
# Every cell runs with locked memory unlimited, as a deployed node does.
set -u
cd /var/tmp/x9
LEG=$1
POOL=$2
ROUND=$3
RATES=${RATES:-100 500}
UNITS=${UNITS:-65536 1048576}
ARMS=${ARMS:-shards shards-lat shards-fine core}
LATENCY_US=${LATENCY_US:-250}
FINE_LATENCY_US=${FINE_LATENCY_US:-100}
FINE_STEP_KIB=${FINE_STEP_KIB:-64}
OWN_CPU=${OWN_CPU:-1}
CLIENT_CPUS=${CLIENT_CPUS:-6,7}
PSR=${PSR:-}
PORT=${PORT:-13900}
ID=macro/grid/unsorted/r50/1024
mkdir -p out shoal
# locked memory without limit, as a deployed node's unit has it (`LimitMEMLOCK=infinity`): under
# the login's 8 MiB glommio cannot register its buffers and every shard writes without them
sudo -n prlimit --pid $$ --memlock=unlimited:unlimited
echo "memlock $(ulimit -l)"

# every side of the leg: the cell alone, then each arm at each rate and unit
sides="alone"
for arm in $ARMS; do
    for rate in $RATES; do
        for unit in $UNITS; do
            sides="$sides $arm:$rate:$unit"
        done
    done
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
    # the side's arm, rate and unit, and the environment that asks for it
    arm=${side%%:*}
    if [ "$arm" = alone ]; then
        name="$LEG-alone-r$ROUND"
        place=off
        rate=0
        unit=0
    else
        rest=${side#*:}
        rate=${rest%%:*}
        unit=${rest#*:}
        name="$LEG-$arm-$rate-$unit-r$ROUND"
        case $arm in
            shards | shards-lat | shards-fine) place=shards ;;
            core) place="core:$OWN_CPU" ;;
        esac
    fi
    latency=""
    step=""
    [ "$arm" = shards-lat ] && latency=$LATENCY_US
    [ "$arm" = shards-fine ] && latency=$FINE_LATENCY_US && step=$FINE_STEP_KIB
    # the tables start empty every run
    rm -rf shoal/*
    start=$(date +%s.%N)
    env SHOAL_X9_PLACE="$place" SHOAL_X9_RATE_MIB="$rate" SHOAL_X9_UNIT="$unit" \
        SHOAL_X9_DIR="$POOL" SHOAL_X9_REPORT="out/$name.x9.json" \
        ${latency:+SHOAL_X9_LATENCY_US=$latency} ${step:+SHOAL_X9_STEP_KIB=$step} \
        taskset -c "$CLIENT_CPUS" ./shoal-workload run --id "$ID" --conf conf.yml \
        --port "$PORT" --json "out/$name.json" > "out/$name.log" 2>&1 &
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
