#!/bin/sh
# Spike X13 on the lab: four rounds on titan and hyperion at once, then four on europa alone with
# both of its builds, then four with europa driving a server on titan across the network.
#
#   sh shoal-spike/results/x13-lab.sh                        # from the repository root, on europa
#   QUICK=1 ROUNDS=1 OUT=target/lab/x13/quick sh shoal-spike/results/x13-lab.sh   # prove it runs
#   PHASES="zen1" sh shoal-spike/results/x13-lab.sh          # the phase that leaves europa idle
#   PHASES="europa net" START=3 sh shoal-spike/results/x13-lab.sh   # europa's, from round three
#
# The phases: `zen1` is titan's and hyperion's every section, the two hosts at once; `europa` is
# europa's every section from its native build, which is what the bench's admin program is built
# as there, and its checks, making and streams from the znver1 build every node runs, the two
# builds' order alternating by round; `net` is europa's native client driving a server on titan.
#
# The binaries, one after the other (glommio's build script runs liburing's configure in the shared
# checkout, so two builds at once corrupt it):
#
#   CARGO_TARGET_DIR=target/lab/x13/znver1 RUSTFLAGS="-C target-cpu=znver1" \
#       cargo build --release -p shoal-spike
#   CARGO_TARGET_DIR=target/lab/x13/native RUSTFLAGS="-C target-cpu=native" \
#       cargo build --release -p shoal-spike
#
# The sides of every cell alternate their order by round inside the binary. Every host's governor
# is `performance` for the run and its e2scrub timer is held, and both are put back on any exit.
# No shoal unit may be running. Nothing may be built on europa while its rounds run.
set -u

ZEN1=${ZEN1:-target/lab/x13/znver1/release/shoal-spike}
NATIVE=${NATIVE:-target/lab/x13/native/release/shoal-spike}
REMOTE=${REMOTE:-/var/tmp/x13}
OUT=${OUT:-shoal-spike/results}
CERTS=${CERTS:-target/lab/x13/certs}
ROUNDS=${ROUNDS:-4}
START=${START:-1}
QUICK=${QUICK:-}
PHASES=${PHASES:-zen1 europa net}
REMOTES="titan hyperion"
TITAN=172.16.2.4

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
[ -f "$CERTS/cert.pem" ] || "$ZEN1" driver certs --out "$CERTS"
for host in $REMOTES; do
    on "$host" "mkdir -p $REMOTE/out $REMOTE/certs && sudo -n mkdir -p /xfs/x13 && sudo -n chown \$(id -u) /xfs/x13"
    scp -q "$ZEN1" "$host:$REMOTE/shoal-spike"
    scp -q "$CERTS/cert.pem" "$CERTS/key.pem" "$host:$REMOTE/certs/"
done
mkdir -p /optane/x13

# the facts every table is labelled with
FACTS="$OUT/x13-facts.txt"
{
    echo "# X13 facts, $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "tree: $(git rev-parse --short HEAD)$(git diff --quiet || echo ' (with the uncommitted spike)')"
    echo "rustc: $(rustc --version)"
    echo "glommio: $(git -C ../glommio rev-parse --short HEAD)"
    echo "znver1 binary: $(sha256sum "$ZEN1" | cut -c1-16)"
    echo "native binary: $(sha256sum "$NATIVE" | cut -c1-16)"
    for host in europa $REMOTES; do
        echo "## $host"
        on "$host" "uname -r; lscpu | grep -E \"^Model name\"; cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor; free -g | sed -n 2p"
        on "$host" "grep -E 'CONFIG_IRQ_TIME_ACCOUNTING|CONFIG_VIRT_CPU_ACCOUNTING_GEN|CONFIG_TLS=' /boot/config-\$(uname -r) || true"
        on "$host" "for d in /optane /xfs; do [ -d \$d ] && findmnt -no TARGET,SOURCE,FSTYPE,OPTIONS -T \$d; done; true"
        on "$host" "for i in /sys/class/net/*; do [ -e \$i/device ] && echo \${i##*/} \$(cat \$i/speed 2>/dev/null) \$(tc qdisc show dev \${i##*/} | head -1); done"
        on "$host" "sysctl net.ipv4.tcp_wmem net.ipv4.tcp_rmem; lsmod | grep -w ^tls || echo 'no tls module'"
    done
} > "$FACTS" 2>&1

# save every host's governor and the e2scrub timers, and put them back on any exit; a server left
# on titan is stopped too
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
        on "$host" "pkill -f 'shoal-spike driver' || true"
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

# titan and hyperion, every section, the two at once, a round at a time
case "$PHASES" in *zen1*)
    round=$START
    while [ "$round" -le "$ROUNDS" ]; do
        echo "x13-lab: round $round on titan and hyperion"
        for host in $REMOTES; do
            ssh -o BatchMode=yes "$host" "$REMOTE/shoal-spike driver $MODE --dir /xfs/x13 --certs $REMOTE/certs --round $round --out $REMOTE/out/x13-$host.json" \
                > "$OUT/x13-$host-r$round.md" 2> "$OUT/x13-$host-r$round.err" &
        done
        wait
        round=$((round + 1))
        sleep 30
    done
    for host in $REMOTES; do
        scp -q "$host:$REMOTE/out/x13-$host.json" "$OUT/"
    done
;; esac

# europa alone: the native build's every section and the znver1 build's checks, making and
# streams, the first of the two alternating by round
case "$PHASES" in *europa*)
    round=$START
    while [ "$round" -le "$ROUNDS" ]; do
        echo "x13-lab: round $round on europa"
        if [ $((round % 2)) -eq 1 ]; then
            ORDER="native znver1"
        else
            ORDER="znver1 native"
        fi
        for build in $ORDER; do
            if [ "$build" = native ]; then
                "$NATIVE" driver $MODE --dir /optane/x13 --certs "$CERTS" --round "$round" \
                    --out "$OUT/x13-europa-native.json" > "$OUT/x13-europa-native-r$round.md" 2> "$OUT/x13-europa-native-r$round.err"
            else
                "$ZEN1" driver $MODE --dir /optane/x13 --certs "$CERTS" --sections check,make,streams --round "$round" \
                    --out "$OUT/x13-europa-znver1.json" > "$OUT/x13-europa-znver1-r$round.md" 2> "$OUT/x13-europa-znver1-r$round.err"
            fi
            sleep 15
        done
        round=$((round + 1))
        sleep 15
    done
;; esac

# across the network: europa's native client, titan's server, nothing else running
case "$PHASES" in *net*)
    round=$START
    while [ "$round" -le "$ROUNDS" ]; do
        echo "x13-lab: network round $round, europa to titan"
        ssh -o BatchMode=yes titan "$REMOTE/shoal-spike driver serve --certs $REMOTE/certs" > /dev/null 2>&1 &
        "$NATIVE" driver drive --addr $TITAN --certs "$CERTS" --executors 2 ${QUICK:+--quick} \
            --round "$round" --leg "europa to titan" --out "$OUT/x13-europa-titan.json" \
            > "$OUT/x13-europa-titan-r$round.md" 2> "$OUT/x13-europa-titan-r$round.err"
        on titan "pkill -f 'shoal-spike driver serve' || true"
        wait
        round=$((round + 1))
        sleep 15
    done
;; esac

# every record merged, the triggers judged
"$ZEN1" driver report ${QUICK:+--quick} "$OUT"/x13-*.json > "$OUT/x13-report.md"
echo "x13-lab: done, $OUT/x13-report.md"
