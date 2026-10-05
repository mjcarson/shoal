#!/bin/sh
# Spike X11 on the lab: four rounds on loopback on each host, then four across the network.
#
#   sh shoal-spike/results/x11-lab.sh                        # from the repository root, on europa
#   QUICK=1 ROUNDS=1 OUT=target/lab/x11/quick sh shoal-spike/results/x11-lab.sh   # prove it runs
#   PHASES="tloop net-th" sh shoal-spike/results/x11-lab.sh  # the legs that leave europa idle
#   PHASES="eloop net-et" START=3 sh shoal-spike/results/x11-lab.sh   # europa's, from round three
#
# The phases: `tloop` is titan's and hyperion's loopback, `eloop` europa's, `net-et` europa driving
# titan, `net-th` titan driving hyperion. Loopback phases named together run at once.
#
# The binary is one znver1 build, run on every host:
#
#   CARGO_TARGET_DIR=target/lab/x11/znver1 RUSTFLAGS="-C target-cpu=znver1" \
#       cargo build --release -p shoal-spike
#
# Loopback: europa, titan and hyperion each run `stream all` against their own server, the three
# hosts at once, a round at a time; hyperion repeats titan's rates and routes only. Network:
# europa drives a server on titan, then titan drives one on hyperion, every round, one at a time,
# with nothing else running. The sides of every cell alternate their order by round inside the
# binary. Every host's governor is `performance` for the run and its e2scrub timer is held, and
# both are put back on any exit. No shoal unit may be running.
set -u

BIN=${BIN:-target/lab/x11/znver1/release/shoal-spike}
REMOTE=${REMOTE:-/var/tmp/x11}
OUT=${OUT:-shoal-spike/results}
CERTS=${CERTS:-target/lab/x11/certs}
ROUNDS=${ROUNDS:-4}
START=${START:-1}
QUICK=${QUICK:-}
PHASES=${PHASES:-tloop eloop net-et net-th}
REMOTES="titan hyperion"
TITAN=172.16.2.4
HYPERION=172.16.2.5

mkdir -p "$OUT" "$CERTS"

# run a script on a host under sh: europa locally, the others over ssh, the script on stdin so no
# login shell (europa's is zsh) gives its words a meaning and no quote has to survive ssh
on() {
    host=$1
    shift
    if [ "$host" = europa ]; then
        printf '%s\n' "$*" | sh -s
    else
        printf '%s\n' "$*" | ssh -o BatchMode=yes "$host" sh -s
    fi
}

# refuse to start beside a shoal unit
for host in europa $REMOTES; do
    active=$(on "$host" "systemctl list-units --state=active --no-legend 'shoal*' || true")
    if [ -n "$active" ]; then
        echo "$host runs a shoal unit: $active" >&2
        exit 1
    fi
done

# the binary and the certificate on every host, the scratch directories made
[ -f "$CERTS/cert.pem" ] || "$BIN" stream certs --out "$CERTS"
for host in $REMOTES; do
    on "$host" "mkdir -p $REMOTE/out $REMOTE/certs && sudo -n mkdir -p /xfs/x11 && sudo -n chown \$(id -u) /xfs/x11"
    scp -q "$BIN" "$host:$REMOTE/shoal-spike"
    scp -q "$CERTS/cert.pem" "$CERTS/key.pem" "$host:$REMOTE/certs/"
done
mkdir -p /optane/x11

# the facts every table is labelled with
FACTS="$OUT/x11-facts.txt"
{
    echo "# X11 facts, $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "tree: $(git rev-parse --short HEAD)$(git diff --quiet || echo ' (with the uncommitted spike)')"
    echo "rustc: $(rustc --version)"
    echo "glommio: $(git -C ../glommio rev-parse --short HEAD)"
    for host in europa $REMOTES; do
        echo "## $host"
        on "$host" "uname -r; lscpu | grep -E \"^Model name\"; cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"
        on "$host" "for d in /optane /xfs; do [ -d \$d ] && findmnt -no TARGET,SOURCE,FSTYPE,OPTIONS -T \$d; done; true"
        on "$host" "for i in /sys/class/net/*; do [ -e \$i/device ] && echo \${i##*/} \$(cat \$i/speed 2>/dev/null) \$(tc qdisc show dev \${i##*/} | head -1); done"
        on "$host" "sysctl net.ipv4.tcp_wmem net.ipv4.tcp_rmem net.core.default_qdisc; lsmod | grep -w ^tls || echo 'no tls module'"
    done
} > "$FACTS" 2>&1

# save every host's governor and the e2scrub timers, and put them back on any exit; a server left
# on titan or hyperion is stopped too
SAVED=""
for host in europa $REMOTES; do
    SAVED="$SAVED $host=$(on "$host" "cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")"
done
SCRUB=""
for host in $REMOTES; do
    if [ "$(on "$host" "systemctl is-active e2scrub_all.timer || true")" = active ]; then
        SCRUB="$SCRUB $host"
    fi
done
restore() {
    for host in $REMOTES; do
        on "$host" "pkill -f 'shoal-spike stream' || true"
    done
    for pair in $SAVED; do
        host=${pair%%=*}
        on "$host" "sudo -n cpupower frequency-set -g ${pair#*=} >/dev/null"
    done
    for host in $SCRUB; do
        on "$host" "sudo -n systemctl start e2scrub_all.timer"
    done
    echo "governors restored:$SAVED; e2scrub timers started again on:$SCRUB"
}
trap restore EXIT
trap 'exit 143' INT TERM
for host in europa $REMOTES; do
    on "$host" "sudo -n cpupower frequency-set -g performance >/dev/null"
done
for host in $SCRUB; do
    on "$host" "sudo -n systemctl stop e2scrub_all.timer"
done

if [ -n "$QUICK" ]; then
    MODE=quick
else
    MODE=all
fi

# loopback: every host named at once, a round at a time
LOOP_HOSTS=""
case "$PHASES" in *eloop*) LOOP_HOSTS="europa" ;; esac
case "$PHASES" in *tloop*) LOOP_HOSTS="$LOOP_HOSTS titan hyperion" ;; esac
if [ -n "$LOOP_HOSTS" ]; then
    round=$START
    while [ "$round" -le "$ROUNDS" ]; do
        echo "x11-lab: loopback round $round on$LOOP_HOSTS"
        for host in $LOOP_HOSTS; do
            case $host in
                europa)
                    "$BIN" stream $MODE --dir /optane/x11 --round "$round" --leg "europa loopback" \
                        --out "$OUT/x11-europa-loop.json" > "$OUT/x11-europa-loop-r$round.md" 2> "$OUT/x11-europa-loop-r$round.err" &
                    ;;
                titan)
                    ssh -o BatchMode=yes titan "$REMOTE/shoal-spike stream $MODE --dir /xfs/x11 --round $round --leg 'titan loopback' --out $REMOTE/out/x11-titan-loop.json" \
                        > "$OUT/x11-titan-loop-r$round.md" 2> "$OUT/x11-titan-loop-r$round.err" &
                    ;;
                hyperion)
                    ssh -o BatchMode=yes hyperion "$REMOTE/shoal-spike stream $MODE --dir /xfs/x11 --round $round --sections rate,route --leg 'hyperion loopback' --out $REMOTE/out/x11-hyperion-loop.json" \
                        > "$OUT/x11-hyperion-loop-r$round.md" 2> "$OUT/x11-hyperion-loop-r$round.err" &
                    ;;
            esac
        done
        wait
        round=$((round + 1))
        sleep 30
    done
    case "$LOOP_HOSTS" in *titan*)
        scp -q titan:$REMOTE/out/x11-titan-loop.json "$OUT/"
        scp -q hyperion:$REMOTE/out/x11-hyperion-loop.json "$OUT/"
    ;; esac
fi

# across the network: one pair at a time, nothing else running
case "$PHASES" in *net-et*)
    round=$START
    while [ "$round" -le "$ROUNDS" ]; do
        echo "x11-lab: network round $round, europa to titan"
        ssh -o BatchMode=yes titan "$REMOTE/shoal-spike stream serve --dir /xfs/x11 --certs $REMOTE/certs" > /dev/null 2>&1 &
        "$BIN" stream drive --addr $TITAN --certs "$CERTS" --server-file --net --sections rate,tail ${QUICK:+--quick} \
            --round "$round" --leg "europa to titan" --out "$OUT/x11-europa-titan.json" \
            > "$OUT/x11-europa-titan-r$round.md" 2> "$OUT/x11-europa-titan-r$round.err"
        on titan "pkill -f 'shoal-spike stream serve' || true"
        wait
        round=$((round + 1))
        sleep 15
    done
;; esac
case "$PHASES" in *net-th*)
    round=$START
    while [ "$round" -le "$ROUNDS" ]; do
        echo "x11-lab: network round $round, titan to hyperion"
        ssh -o BatchMode=yes hyperion "$REMOTE/shoal-spike stream serve --dir /xfs/x11 --certs $REMOTE/certs" > /dev/null 2>&1 &
        ssh -o BatchMode=yes titan "$REMOTE/shoal-spike stream drive --addr $HYPERION --certs $REMOTE/certs --server-file --net --sections rate,tail ${QUICK:+--quick} --round $round --leg 'titan to hyperion' --out $REMOTE/out/x11-titan-hyperion.json" \
            > "$OUT/x11-titan-hyperion-r$round.md" 2> "$OUT/x11-titan-hyperion-r$round.err"
        on hyperion "pkill -f 'shoal-spike stream serve' || true"
        wait
        round=$((round + 1))
        sleep 15
    done
    scp -q titan:$REMOTE/out/x11-titan-hyperion.json "$OUT/"
;; esac

# every record merged, the triggers judged
"$BIN" stream report ${QUICK:+--quick} "$OUT"/x11-*.json > "$OUT/x11-report.md"
echo "x11-lab: done, $OUT/x11-report.md"
