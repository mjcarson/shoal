#!/bin/sh
# O158: what openraft holds of every node's floor, before and after its forks, on three loopback
# nodes on europa, each arm a jemalloc build dumping a profile every 128 MiB allocated (adapted from
# target/lab/i338/run-c2.sh):
#   P  the build before O158 (c0b0aad6)
#   Q  the build with O158's forks and Shoal's own table
# item 338's header follows
#
# Each arm bootstraps shoaladm's loopback inventory (three tmdb nodes on 127.0.0.11-13, 4 GiB a
# node unless MEMORY says) with an arm's program and environment, reads every node at ready, after
# loading LIMIT rows and walking them, and after REST seconds at rest, then destroys it:
#   A  mimalloc as deployed (its arenas madvised for huge pages)
#   B  mimalloc with MIMALLOC_ALLOW_LARGE_OS_PAGES=0
#   C  jemalloc with sampled profiling, dumped at exit by a clean stop at rest
#   E  the build before the fix at ARM_MEMORY, 1 GiB unless set: D's before
#   D  the fixed build at ARM_MEMORY, 1 GiB unless set (X16's arm): FIXED and FIXED_LOADER name its programs
# The tmdb node shares these cpus and is stopped for the run and started again on every way out,
# as are the drop-ins, the device directories and the cluster.
set -eu

REPO=/home/mcarson/projects/claude/shoal-object-store
LAB=$REPO/target/lab/found/o158
DATASET=${DATASET:-$HOME/datasets/TMDB_movie_dataset_v11.csv}
ARMS=${ARMS:-"P Q"}
LIMIT=${LIMIT:-100000}
REST=${REST:-120}
MEMORY=${MEMORY:-4Gi}
FIXED=${FIXED:-$LAB/fixed/release/tmdb-dataset-node}
FIXED_LOADER=${FIXED_LOADER:-$LAB/fixed/release/tmdb-dataset-loader}
LOADER=${LOADER:-$LAB/mi/release/tmdb-dataset-loader}
NODES="loop-a loop-b loop-c"
OUT=${OUT:-$LAB/runs}
mkdir -p "$OUT"

# one of the arm's admin commands, with the arm's programs and deploy state
adm() {
    (cd "$REPO/examples/tmdb_dataset" && SHOAL_BIN_DIR="$LAB/bin-$ARM" SHOAL_DEPLOY_HOME="$LAB/state-$ARM" \
        "${loader:-$LOADER}" "$@")
}

# every node's journal since the arm began, the refusals and warnings it logged among it
journals() {
    for node in $NODES; do
        echo "== $node" >> "$OUT/$ARM-journal.txt"
        sudo journalctl -u "shoal-loop-$node" --since "$began" --no-pager -o short-iso \
            | grep -E "WARN|ERROR|refused|past its memory|memory at ready|invalid config" | tail -40 >> "$OUT/$ARM-journal.txt"
    done
}

# every way out puts the host back
cleanup() {
    set +e
    for node in $NODES; do
        sudo rm -rf "/etc/systemd/system/shoal-loop-$node.service.d/o158.conf"
    done
    sudo systemctl daemon-reload
    if [ -n "${INV:-}" ] && [ -f "$INV" ]; then
        adm destroy -i "$INV" --yes >> "$OUT/destroy.log" 2>&1
    fi
    for node in $NODES; do
        sudo rm -rf "/optane/shoal-$node-pool" "/optane/shoal-$node"
    done
    sudo systemctl start shoal-tmdb
    echo "cleanup done: shoal-tmdb $(systemctl is-active shoal-tmdb)"
}
trap cleanup EXIT INT TERM

# one reading of every node: its kernel figures, its huge pages, and the cluster's own figures
snap() {
    label=$1
    file="$OUT/$ARM-$label.txt"
    : > "$file"
    for node in $NODES; do
        pid=$(systemctl show -p MainPID --value "shoal-loop-$node")
        echo "== $node pid $pid" >> "$file"
        sudo grep -E '^(VmRSS|RssAnon|RssFile|VmPin|VmSwap|VmHWM)' "/proc/$pid/status" >> "$file" || true
        sudo grep -E '^AnonHugePages' "/proc/$pid/smaps_rollup" >> "$file" || true
    done
    adm stats -i "$INV" --json > "$OUT/$ARM-$label.json" 2>> "$OUT/$ARM.log" || true
    echo "$(date -u +%T) $ARM $label" >> "$OUT/$ARM.log"
}

sudo systemctl stop shoal-tmdb
for ARM in $ARMS; do
    began=$(date '+%Y-%m-%d %H:%M:%S')
    # the arm's program, admin program, memory and environment
    loader=$LOADER
    memory=$MEMORY
    case $ARM in
        P) server=$LAB/je-before/release/tmdb-dataset-node; loader=$LAB/je-before/release/tmdb-dataset-loader
           env="_RJEM_MALLOC_CONF=prof:true,prof_active:true,lg_prof_sample:19,lg_prof_interval:27,prof_final:false,prof_prefix:/var/tmp/shoal-heap-o158-P" ;;
        Q) server=$LAB/je-after/release/tmdb-dataset-node; loader=$LAB/je-after/release/tmdb-dataset-loader
           env="_RJEM_MALLOC_CONF=prof:true,prof_active:true,lg_prof_sample:19,lg_prof_interval:27,prof_final:false,prof_prefix:/var/tmp/shoal-heap-o158-Q" ;;
    esac
    INV=$LAB/inventory-$ARM.yml
    sed -e "s#^server: .*#server: $server#" -e "s#memory: 4Gi#memory: $memory#" \
        "$REPO/shoaladm/inventories/loopback.yml" > "$INV"
    # the drop-ins first, so a node's first start has its environment
    for node in $NODES; do
        if [ -n "$env" ]; then
            sudo mkdir -p "/etc/systemd/system/shoal-loop-$node.service.d"
            printf '[Service]\nEnvironment=%s\n' "$env" | sudo tee "/etc/systemd/system/shoal-loop-$node.service.d/o158.conf" > /dev/null
        fi
        # a device is a directory that exists, owned by the node's user, never created by it
        sudo mkdir -p "/optane/shoal-$node-pool"
        sudo chown shoal:shoal "/optane/shoal-$node-pool"
    done
    sudo systemctl daemon-reload
    echo "$(date -u +%T) $ARM bootstrap $server memory $memory ${env:-}" >> "$OUT/$ARM.log"
    if adm bootstrap -i "$INV" >> "$OUT/$ARM.log" 2>&1; then
        for node in $NODES; do
            systemctl show -p Environment --value "shoal-loop-$node" >> "$OUT/$ARM.log"
        done
        sleep 10
        snap ready
        adm load -i "$INV" --dataset "$DATASET" --limit "$LIMIT" >> "$OUT/$ARM.log" 2>&1 || echo "load failed" >> "$OUT/$ARM.log"
        adm walk -i "$INV" --dataset "$DATASET" --limit "$LIMIT" >> "$OUT/$ARM.log" 2>&1 || echo "walk failed" >> "$OUT/$ARM.log"
        snap loaded
        sleep "$REST"
        snap rest
    else
        echo "$(date -u +%T) $ARM bootstrap failed" >> "$OUT/$ARM.log"
    fi
    journals
    # every interval dump the arm's nodes wrote, kept, the nodes stopped first
    for node in $NODES; do sudo systemctl stop "shoal-loop-$node"; done
    sleep 5
    sudo sh -c "cp /var/tmp/shoal-heap-o158-$ARM.* $OUT/ && chown $(id -u):$(id -g) $OUT/shoal-heap-o158-$ARM.*" || true
    sudo sh -c "rm -f /var/tmp/shoal-heap-o158-$ARM.*"
    # the arm's cluster gone before the next
    adm destroy -i "$INV" --yes >> "$OUT/$ARM.log" 2>&1 || echo "destroy failed" >> "$OUT/$ARM.log"
    for node in $NODES; do
        sudo rm -f "/etc/systemd/system/shoal-loop-$node.service.d/o158.conf"
        sudo rm -rf "/optane/shoal-$node-pool"
    done
    INV=
done
