# O131's diagnosis: what each bench connection retransmitted in each run of each arm, from the
# samples stores-sample.sh kept on every host beside the captures, and each host's TCP counters over
# the same windows. A connection is a client's when either end is the bench cluster's client port, a
# control lane's at its control port, and a peer lane's at its peer port. A peer lane whose current
# timeout is under 200 ms takes a minimum RTO, which since item 304 is a lane of rounds; the others
# carry bytes (data, bulk and object lanes). Under a host's own minimum RTO every lane is under 200
# ms, so the lanes are told apart by what they carried instead: a lane that moved 8 MiB in a window
# carried bytes. A connection's retransmissions are counted on the host that sent them, so a node's
# end of a client's connection retransmits answers, and the driver's end requests.
# `python3 -I stores-ab-sockets.py <samples dir> <capture dir>...`, the samples dir holding one
# <host>.log a host
import collections
import datetime
import json
import pathlib
import re
import sys

samples_dir, captures = pathlib.Path(sys.argv[1]), sys.argv[2:]
CLIENT, PEER, CONTROL = 12500, 12501, 12502
BYTES_LANE = 8 << 20
COUNTERS = ["TcpRetransSegs", "TcpExtTCPLossProbes", "TcpExtTCPLossProbeRecovery", "TcpExtTCPTimeouts",
            "TcpExtTCPFastRetrans", "TcpExtTCPSlowStartRetrans", "TcpExtTCPSpuriousRTOs"]


def field(info, name):
    """A number a connection's state line names, or None"""
    match = re.search(rf"\b{name}:([0-9.]+)", info)
    return float(match.group(1)) if match else None


def parse(path):
    """Every sample of a host: (time, {connection: state}, {counter: value})"""
    out = []
    when, conns, counters, addrs, in_nstat = None, {}, {}, None, False
    for line in open(path, errors="replace"):
        if line.startswith("@ "):
            if when is not None:
                out.append((when, conns, counters))
            when, conns, counters, addrs, in_nstat = float(line.split()[1]), {}, {}, None, False
        elif line.startswith("# nstat"):
            in_nstat = True
        elif in_nstat:
            parts = line.split()
            if len(parts) >= 2 and parts[1].isdigit():
                counters[parts[0]] = int(parts[1])
        elif line.startswith("\t") or line.startswith(" "):
            if addrs is not None:
                info = line.strip()
                total = re.search(r"\bretrans:\d+/(\d+)", info)
                conns[addrs] = {
                    "retrans": int(total.group(1)) if total else 0,
                    "bytes_retrans": field(info, "bytes_retrans") or 0,
                    "moved": (field(info, "bytes_acked") or 0) + (field(info, "bytes_received") or 0),
                    "rto": field(info, "rto") or 0,
                }
                addrs = None
        else:
            parts = line.split()
            if len(parts) >= 4:
                addrs = (parts[2], parts[3])
    if when is not None:
        out.append((when, conns, counters))
    return out


def port(address):
    """An address's port"""
    return int(address.rsplit(":", 1)[1])


def kind(local, peer, state, moved):
    """What a connection is, and which end of it this host holds"""
    ports = (port(local), port(peer))
    if CLIENT in ports:
        return "client, node's end" if ports[0] == CLIENT else "client, driver's end"
    if CONTROL in ports:
        return "control lane"
    if state["rto"] and state["rto"] < 200 and moved < BYTES_LANE:
        return "lane of rounds"
    return "lane of bytes"


def windows(capture):
    """Each arm's measured windows: (arm id, start, end) in unix seconds"""
    bench = json.loads((pathlib.Path(capture) / "bench.json").read_text())
    out = []
    for arm in bench["arms"]:
        for run in arm.get("runs", []):
            start = datetime.datetime.fromisoformat(run["started_at"].replace("Z", "+00:00")).timestamp()
            warmup, measured = run.get("warmup") or 0, run.get("measured") or 0
            if isinstance(warmup, dict):
                warmup = warmup.get("secs", 0) + warmup.get("nanos", 0) / 1e9
            if isinstance(measured, dict):
                measured = measured.get("secs", 0) + measured.get("nanos", 0) / 1e9
            begin = start + float(warmup)
            out.append((arm["id"], begin, begin + float(measured)))
    return out


hosts = {path.stem: parse(path) for path in sorted(samples_dir.glob("*.log"))}
for capture in captures:
    print(f"== {pathlib.Path(capture).name}")
    by_arm = collections.defaultdict(lambda: collections.defaultdict(lambda: [0, 0.0, 0]))
    counted = collections.defaultdict(lambda: collections.defaultdict(int))
    for arm, begin, end in windows(capture):
        for host, samples in hosts.items():
            inside = [s for s in samples if begin <= s[0] <= end]
            before = [s for s in samples if s[0] < begin]
            if not inside:
                continue
            first = before[-1] if before else inside[0]
            last = inside[-1]
            for addrs, state in last[1].items():
                was = first[1].get(addrs, {"retrans": 0, "bytes_retrans": 0, "moved": 0})
                moved = state["moved"] - was["moved"]
                cell = by_arm[arm][(host, kind(addrs[0], addrs[1], state, moved))]
                cell[0] += state["retrans"] - was["retrans"]
                cell[1] += state["bytes_retrans"] - was["bytes_retrans"]
                cell[2] += 1
            for name in COUNTERS:
                if name in last[2] and name in first[2]:
                    counted[arm][(host, name)] += last[2][name] - first[2][name]
    for arm in sorted(by_arm):
        print(f"  {arm}")
        for (host, what), (retrans, retrans_bytes, n) in sorted(by_arm[arm].items()):
            if retrans:
                print(f"    {host:9} {what:22} {retrans:6} segments retransmitted, {retrans_bytes / 1024:9.1f} KiB, over {n} connection-windows")
        probes = {host: counted[arm][(host, "TcpExtTCPLossProbes")] for host in hosts}
        timeouts = {host: counted[arm][(host, "TcpExtTCPTimeouts")] for host in hosts}
        print(f"    loss probes by host {probes}, timeouts {timeouts}")
