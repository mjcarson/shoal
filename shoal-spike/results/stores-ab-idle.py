# O131's diagnosis: which connections sit idle past their RTO between sends, in each arm's measured
# windows, from the samples stores-sample.sh kept on every host beside the captures. For every
# connection that sent data in a window (its bytes_acked moved), each sample inside the window gives
# the time since it last sent (`lastsnd`) beside its current timeout (`rto`), and its congestion window
# (`cwnd`). With net.ipv4.tcp_slow_start_after_idle at 1, Linux restarts a sender's window at its next
# send once it was idle past its RTO, halving it for each RTO it was idle down to the initial ten
# segments. Prints, by capture, arm, host and kind, the connections that sent, the share of their
# samples idle past their RTO, and their median idle time and window. The kinds are
# stores-ab-sockets.py's: a peer lane under 200 ms of RTO is a lane of rounds, which item 304 floors,
# so on a host whose own minimum RTO is lower the lanes are not told apart.
# `python3 -I stores-ab-idle.py <samples dir> <arm filter> <capture dir>...`, the samples dir holding
# one <host>.log a host, and the filter a part of the arms' ids (put100/s64k/c1/)
import collections
import datetime
import json
import pathlib
import re
import statistics
import sys

samples_dir, arm_filter, captures = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3:]
CLIENT, PEER, CONTROL = 12500, 12501, 12502
NUM = re.compile(r"\b(rto|lastsnd|cwnd|bytes_acked|bytes_received):([0-9.]+)")


def secs(value):
    """A duration as a capture keeps it, in seconds"""
    if isinstance(value, dict):
        return value.get("secs", 0) + value.get("nanos", 0) / 1e9
    return float(value or 0)


def windows():
    """Every measured window of an arm the filter names: (label, arm, run, start, end)"""
    out = []
    for capture in captures:
        bench = json.loads((pathlib.Path(capture) / "bench.json").read_text())
        for arm in bench["arms"]:
            if arm_filter not in arm["id"]:
                continue
            for i, run in enumerate(arm.get("runs", [])):
                start = datetime.datetime.fromisoformat(run["started_at"].replace("Z", "+00:00")).timestamp()
                begin = start + secs(run.get("warmup"))
                out.append((pathlib.Path(capture).name, arm["id"], i, begin, begin + secs(run.get("measured"))))
    return out


def port(address):
    """An address's port"""
    return int(address.rsplit(":", 1)[1])


def kind(local, peer, rto):
    """What a connection is, and which end of it this host holds"""
    ports = (port(local), port(peer))
    if CLIENT in ports:
        return "client, node's end" if ports[0] == CLIENT else "client, driver's end"
    if CONTROL in ports:
        return "control lane"
    return "lane of rounds" if rto < 200 else "lane of bytes"


wins = windows()
lo, hi = min(w[3] for w in wins) - 2, max(w[4] for w in wins) + 2
# per window, host and connection: [(time, rto, lastsnd, cwnd, acked)]
seen = collections.defaultdict(list)
for log in sorted(samples_dir.glob("*.log")):
    host = log.stem
    when, inside, addrs = None, [], None
    with open(log, errors="replace") as f:
        for line in f:
            if line.startswith("@ "):
                when = float(line.split()[1])
                inside = [w for w in wins if w[3] - 1.5 <= when <= w[4]] if lo <= when <= hi else []
                addrs = None
            elif not inside:
                continue
            elif line.startswith("# nstat"):
                addrs = None
            elif line[:1] in "\t ":
                if addrs is not None:
                    vals = dict((k, float(v)) for k, v in NUM.findall(line))
                    for w in inside:
                        seen[(w, host, addrs)].append((when, vals.get("rto", 0), vals.get("lastsnd", 0),
                                                       vals.get("cwnd", 0), vals.get("bytes_acked", 0)))
                    addrs = None
            else:
                parts = line.split()
                addrs = (parts[2], parts[3]) if len(parts) >= 4 and ":" in parts[2] else None

# by capture, arm, host and kind: connections that sent, samples, samples idle past rto, idle and cwnd
agg = collections.defaultdict(lambda: [set(), 0, 0, [], []])
for (w, host, addrs), rows in seen.items():
    rows.sort()
    sent = rows[-1][4] - rows[0][4]
    if sent <= 0:
        continue
    measured = [r for r in rows if w[3] <= r[0] <= w[4]]
    if not measured:
        continue
    cell = agg[(w[0], w[1], host, kind(addrs[0], addrs[1], measured[-1][1]))]
    cell[0].add(addrs)
    for _, rto, lastsnd, cwnd, _ in measured:
        cell[1] += 1
        cell[2] += lastsnd > rto
        cell[3].append(lastsnd)
        cell[4].append(cwnd)
for key in sorted(agg):
    conns, n, idle, lastsnd, cwnd = agg[key]
    print(f"{key[0]:26} {key[1]:20} {key[2]:9} {key[3]:22} {len(conns):4} sent, {100 * idle / n:5.1f}% of {n:5} samples idle past rto,"
          f" idle median {statistics.median(lastsnd):7.0f} ms, cwnd median {statistics.median(cwnd):4.0f}")
