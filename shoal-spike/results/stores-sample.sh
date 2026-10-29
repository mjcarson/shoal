#!/bin/sh
# Beside a capture on the lab: every few seconds, every TCP connection of a bench cluster's ports
# with its kernel state (`ss -tinm`: retransmissions, timeouts, windows and queues) and the host's
# TCP counters (`nstat`, loss probes among them), so what each connection did in a run can be read
# against the run's window afterwards (stores-ab-sockets.py), and a stall leaves the state of every
# connection behind it (item 305, O131). It writes to stdout only and runs until its deadline, or
# until it is killed or its output closes:
#
#   ssh titan sh -s -- <seconds> <deadline, unix seconds> <port>... < shoal-spike/results/stores-sample.sh > titan.log &
#   sh shoal-spike/results/stores-sample.sh 1 $(($(date +%s) + 7200)) 12500 12501 12502 > europa.log &
#
# Each sample is a line `@ <unix seconds>`, the connections' lines (each two: its addresses, then
# its state), a line `# nstat`, and the counters a line each.
set -u
every=$1
deadline=$2
shift 2
# the connections of any of the ports, either end
filter=""
for port in "$@"; do
    filter="$filter${filter:+ or }sport = :$port or dport = :$port"
done
while [ "$(date +%s)" -lt "$deadline" ]; do
    echo "@ $(date +%s.%N)" || exit 0
    ss -tinmH state established "( $filter )" || exit 0
    echo "# nstat"
    nstat -asz 2>/dev/null | grep -E '^(Tcp|TcpExt)' || true
    sleep "$every"
done
