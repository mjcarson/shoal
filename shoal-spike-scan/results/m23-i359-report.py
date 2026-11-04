"""Item 359 on the lab: what the cell's slow reads wait on beside a whole scan of Big

    python3 -I shoal-spike-scan/results/m23-i359-report.py target/lab/m23-i359/captures \\
        --csv shoal-spike-scan/results/m23-i359/seconds.csv > shoal-spike-scan/results/m23-i359/report.md

Reads `m23-lab.sh`'s BIG_ONLY=1 run: each round's whole Big capture (`m23-r<n>-big/bench.json`) and
its samples (`m23-r<n>-samples/`): every host's devices and node threads each second
(`samples-<host>.log.gz`, m23-fall-sample.sh's), every member's own figures each second
(`stats.json.gz`, `stats --json --watch 1`), the disk under each host's root (`disks.txt`) and the
round's scan settings (`scans.txt`), which name its side.

Every figure is read over the cell beside the scan arm (`rw50/b1/scan`), in two windows of it: its
*before*, the seconds the bench's own event window takes before the scan was asked, and its *during*,
from the ask to the scan's last page. For each round it gives the cell's read p99 as W7 judges it, from
the client; each member's own get p99, timed from a bundle's arrival to its answer on the member that
served it (a read at One is served by the member its bundle reached), over the ten seconds before each
of its reports, so only a report whose ten seconds lie inside a window is read for it; each host's disk
under its root, its reads a second, MiB read a second, time a read, reads in its queue and busy share;
each host's shard threads' cores busy, together and the busiest one's; and each member's share of its
scan bytes read on the trickle, beside one of its own foreground reads on the device. Then each side's
medians over its rounds, and whether the two sides' ranges are disjoint, the lab's rule.

How the run tells its causes apart. If the cell's slow reads wait in the device's queue behind the
scan's reads, the members whose devices the scan loads (the Zen1 hosts' 970 EVOs, where it reads most)
serve slower gets during the scan, their disks' time a read rises with it, and at fewer scan reads in
flight a device (SCANS_ALT) both fall by more than the scan's rate does. If they wait for their shard's
loop or cpu, the shard threads' cores busy rise with the scan, the disks' time a read does not, and
fewer reads in flight change the rise only as far as the scan's rate.
"""

import csv
import datetime
import gzip
import json
import pathlib
import statistics
import sys

# the clock ticks a second /proc's thread times count in
TICKS = 100
# the seconds a member's query figures look back over, its p99 and p50
LOOKBACK = 10
# a host's figures a second, its disk's and its shards'
DISK_AND_SHARDS = ("reads", "mib_read", "ms_read", "reads_in_flight", "queue", "busy", "flushes", "ms_flush",
                   "shards", "busiest_shard")


def epoch(stamp):
    """An ISO time in UTC as seconds since the epoch"""
    return datetime.datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp()


def opened(path):
    """A log or its gzipped copy, opened for text"""
    if path.exists():
        return open(path, encoding="utf-8", errors="replace")
    zipped = path.with_name(path.name + ".gz")
    if zipped.exists():
        return gzip.open(zipped, "rt", encoding="utf-8", errors="replace")
    return None


def scan_arm(root, round_):
    """The cell beside the scan arm of a round's Big capture, whole or slimmed, or none"""
    for path in (root / f"m23-r{round_}-big" / "bench.json", root / f"m23-r{round_}-big.json"):
        if path.exists():
            with open(path, encoding="utf-8") as handle:
                capture = json.load(handle)
            for entry in capture.get("arms", []):
                if entry["id"] == "rw50/b1/scan" and entry.get("runs"):
                    return entry["runs"][0]
    return None


def windows(run):
    """A scan arm's before and during windows as epoch seconds, `(low, high)` each, or none: before
    the bench's own before window, ending at the scan's ask; during from the ask to the last page"""
    event = run.get("event") or {}
    marks = event.get("marks") or []
    asked = [mark["at_ms"] / 1000 for mark in marks if mark.get("kind") == "requested"]
    done = [mark["at_ms"] / 1000 for mark in marks if mark.get("kind") == "done"]
    if not asked or not done:
        return None
    start = epoch(run["started_at"])
    before_secs = ((event.get("windows") or {}).get("before") or {}).get("secs") or 18
    ask, end = start + min(asked), start + max(done)
    return (ask - before_secs, ask), (ask, end)


def p99(summary, kind):
    """A kind's p99 in a window's summary, or none"""
    return ((((summary or {}).get(kind) or {}).get("latency")) or {}).get("p99_ms")


def samples(path):
    """Every sample of one host's log: its time, each device's counters and each node thread's
    ticks and name"""
    out = []
    current = None
    handle = opened(path)
    if handle is None:
        return out
    with handle:
        for line in handle:
            if line.startswith("@ "):
                current = {"at": float(line.split()[1]), "devices": {}, "threads": {}}
                out.append(current)
                continue
            if current is None or line.startswith("cpu ") or line.startswith("# threads"):
                continue
            fields = line.split()
            # a /proc/diskstats line: major, minor, name, then its counters
            if len(fields) >= 20 and fields[0].isdigit() and not fields[2].isdigit():
                current["devices"][fields[2]] = [int(field) for field in fields[3:]]
            # a thread: its id, its user and system ticks and its name
            elif len(fields) >= 4 and fields[0].isdigit():
                current["threads"][fields[0]] = (int(fields[1]) + int(fields[2]), " ".join(fields[3:]))
    return out


def per_second(host_samples, disk):
    """A host's figures for each second between two of its samples, keyed by the epoch second it
    began in: its disk's reads a second, MiB read a second, time a read, reads in flight on it, every
    request in flight on it, its busy share, its flushes a second and time a flush, and its shard
    threads' cores busy, together and the busiest one's"""
    out = {}
    for before, after in zip(host_samples, host_samples[1:]):
        elapsed = after["at"] - before["at"]
        if elapsed <= 0:
            continue
        row = {}
        old, new = before["devices"].get(disk), after["devices"].get(disk)
        if old and new:
            # /proc/diskstats after the name: reads 0, sectors read 2, ms reading 3, io ms 9,
            # weighted io ms 10, flushes 15 and their ms 16
            reads = new[0] - old[0]
            row["reads"] = reads / elapsed
            row["mib_read"] = (new[2] - old[2]) * 512 / elapsed / 2**20
            row["ms_read"] = (new[3] - old[3]) / reads if reads else None
            # the reads alone in flight, their time over the second's, and every request's, writes and
            # the WAL's syncs among them
            row["reads_in_flight"] = (new[3] - old[3]) / 1000 / elapsed
            row["queue"] = (new[10] - old[10]) / 1000 / elapsed
            row["busy"] = (new[9] - old[9]) / 1000 / elapsed
            if len(new) > 16:
                flushes = new[15] - old[15]
                row["flushes"] = flushes / elapsed
                row["ms_flush"] = (new[16] - old[16]) / flushes if flushes else None
        # every shard thread's cores busy this second, glommio's executors being `unnamed-N`
        shards = [(ticks - before["threads"][tid][0]) / TICKS / elapsed
                  for tid, (ticks, comm) in after["threads"].items()
                  if comm.startswith("unnamed-") and tid in before["threads"]]
        if shards:
            row["shards"] = sum(shards)
            row["busiest_shard"] = max(shards)
        out[int(before["at"])] = row
    return out


def stats_stream(path):
    """Every member's own figures `stats --json --watch 1` printed, by hostname and the epoch second
    the member derived them at"""
    out = {}
    handle = opened(path)
    if handle is None:
        return out
    with handle:
        text = handle.read()
    decoder = json.JSONDecoder()
    index = 0
    while True:
        # each view is one JSON object, printed one after another
        index = text.find("{", index)
        if index < 0:
            break
        try:
            view, index = decoder.raw_decode(text, index)
        except json.JSONDecodeError:
            index += 1
            continue
        for member in view.get("members") or []:
            stats = member.get("stats") or {}
            name = stats.get("hostname")
            if name and stats.get("at_ms"):
                out.setdefault(name, {})[int(stats["at_ms"] / 1000)] = stats
    return out


def get_figures(stats):
    """A member's own get figures in one report: its p99 and p50 in ms and its gets a second, each
    none where it has none"""
    for op in ((stats.get("queries") or {}).get("ops")) or []:
        if op.get("op") == "get":
            return op.get("p99_ms"), op.get("p50_ms"), (op.get("rate") or {}).get("r10s")
    return None, None, None


def median(values):
    """The median of the values that are there, or none"""
    values = [value for value in values if value is not None]
    return statistics.median(values) if values else None


def trickle_shares(run):
    """Each member's share of the scan bytes it read over the arm on the trickle, beside a
    foreground read of its own on the device, from the capture's members' samples"""
    series = run.get("server_series") or []
    if len(series) < 2:
        return {}
    first, last = series[0].get("scans") or {}, series[-1].get("scans") or {}
    out = {}
    for member, figures in last.items():
        before = first.get(member) or {}
        idle = figures.get("idle_bytes", 0) - before.get("idle_bytes", 0)
        trickled = figures.get("trickled_bytes", 0) - before.get("trickled_bytes", 0)
        if idle + trickled > 0:
            out[member] = trickled / (idle + trickled)
    return out


def round_figures(root, round_):
    """One round's figures, by window, or none when its capture or samples are missing"""
    run = scan_arm(root, round_)
    sample_dir = root / f"m23-r{round_}-samples"
    if run is None or not sample_dir.exists():
        return None
    cut = windows(run)
    if cut is None:
        return None
    side = (sample_dir / "scans.txt").read_text().strip() if (sample_dir / "scans.txt").exists() else "?"
    disks = {}
    if (sample_dir / "disks.txt").exists():
        for line in (sample_dir / "disks.txt").read_text().splitlines():
            fields = line.split()
            if len(fields) == 3:
                disks[fields[0]] = fields[2]
    event_windows = (run.get("event") or {}).get("windows") or {}
    out = {"round": round_, "side": side, "cut": cut, "hosts": {}, "seconds": []}
    # the client's view, as W7 judges it, and the scan's own rate
    before_p99, during_p99 = p99(event_windows.get("before"), "read"), p99(event_windows.get("during"), "read")
    out["read_before"], out["read_during"] = before_p99, during_p99
    out["read_ratio"] = during_p99 / before_p99 if before_p99 and during_p99 else None
    out["partitions"] = (((event_windows.get("during") or {}).get("scan")) or {}).get("partitions_per_sec")
    out["scan_secs"] = cut[1][1] - cut[1][0]
    trickles = trickle_shares(run)
    members = stats_stream(sample_dir / "stats.json")
    for host, disk in sorted(disks.items()):
        seconds = per_second(samples(sample_dir / f"samples-{host}.log"), disk)
        reports = members.get(host, {})
        figures = {"trickled": trickles.get(host)}
        for name, (low, high) in (("before", cut[0]), ("during", cut[1])):
            span = [second for second in seconds if low <= second < high]
            for figure in DISK_AND_SHARDS:
                figures[f"{figure}_{name}"] = median(seconds[second].get(figure) for second in span)
            # a member's report looks back LOOKBACK seconds: only one wholly inside the window is read
            inside = [stats for at, stats in reports.items() if low + LOOKBACK <= at <= high]
            gets = [get_figures(stats) for stats in inside]
            figures[f"get_p99_{name}"] = median(get[0] for get in gets)
            figures[f"get_p50_{name}"] = median(get[1] for get in gets)
            figures[f"gets_{name}"] = median(get[2] for get in gets)
        out["hosts"][host] = figures
        # every second from the before window's start to the scan's end, for the csv
        for second in range(int(cut[0][0]), int(cut[1][1])):
            row = seconds.get(second, {})
            get = get_figures(reports[second]) if second in reports else (None, None, None)
            out["seconds"].append({"round": round_, "side": side, "host": host, "disk": disk,
                                   "second": second - int(cut[1][0]),
                                   "window": "before" if second < cut[1][0] else "during",
                                   **{key: row.get(key) for key in DISK_AND_SHARDS},
                                   "get_p99": get[0], "get_p50": get[1]})
    return out


def fmt(value, form="%.3g"):
    """A figure for the tables, or a dash"""
    return "-" if value is None else form % value


def spread(values):
    """A figure's values over rounds as its median and range"""
    values = sorted(value for value in values if value is not None)
    if not values:
        return "-"
    return f"{statistics.median(values):.3g} [{values[0]:.3g}, {values[-1]:.3g}]"


def ranges_apart(one, other):
    """Whether two sides' values give ranges that do not overlap; none with fewer than two a side"""
    one, other = [v for v in one if v is not None], [v for v in other if v is not None]
    if len(one) < 2 or len(other) < 2:
        return None
    return max(one) < min(other) or max(other) < min(one)


def ratio(figures, name):
    """A host's figure during over before, or none"""
    before, during = figures.get(f"{name}_before"), figures.get(f"{name}_during")
    return during / before if before and during is not None else None


def main():
    """Every round's figures, then each side's, then the sides set against each other"""
    args = sys.argv[1:]
    csv_path = None
    if "--csv" in args:
        at = args.index("--csv")
        csv_path = pathlib.Path(args[at + 1])
        del args[at:at + 2]
    root = pathlib.Path(args[0])
    rounds = []
    for path in sorted(root.glob("m23-r*-samples")):
        figures = round_figures(root, int(path.name.split("-")[1][1:]))
        if figures:
            rounds.append(figures)
    rounds.sort(key=lambda figures: figures["round"])
    if not rounds:
        sys.exit(f"no round with a Big capture and samples under {root}")
    sides = sorted({figures["side"] for figures in rounds})
    hosts = sorted({host for figures in rounds for host in figures["hosts"]})
    print("# Item 359: what the cell's slow reads wait on beside a whole scan of Big\n")
    print(f"From `{root}`, {len(rounds)} rounds. Each side is a round's scan settings; a figure is the median of its "
          "seconds in a window, *before* the bench's before window and *during* the scan's ask to its last page.\n")
    print("## Each round\n")
    print("| Round | Side | Scan s | Partitions a second | Read p99 ms, before to during (client) | During over before |")
    print("| --- | --- | --- | --- | --- | --- |")
    for figures in rounds:
        print(f"| {figures['round']} | {figures['side']} | {fmt(figures['scan_secs'], '%.0f')} | {fmt(figures['partitions'], '%.0f')} | "
              f"{fmt(figures['read_before'], '%.2f')} to {fmt(figures['read_during'], '%.2f')} | {fmt(figures['read_ratio'], '%.3f')} |")
    print("\n| Round | Member | Get p99 ms, before to during | Get p50 ms | Gets a second | Disk ms a read | Disk reads a second | "
          "Disk MiB read a second | Reads in flight | Every request in flight | Disk ms a flush | Shards' cores busy | Busiest shard | "
          "Scan bytes on the trickle |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for figures in rounds:
        for host in hosts:
            f = figures["hosts"].get(host)
            if not f:
                continue
            pair = lambda name, form="%.3g": f"{fmt(f.get(name + '_before'), form)} to {fmt(f.get(name + '_during'), form)}"
            print(f"| {figures['round']} | {host} | {pair('get_p99', '%.2f')} | {pair('get_p50', '%.2f')} | {pair('gets', '%.0f')} | "
                  f"{pair('ms_read', '%.3f')} | {pair('reads', '%.0f')} | {pair('mib_read', '%.1f')} | {pair('reads_in_flight', '%.2f')} | "
                  f"{pair('queue', '%.2f')} | {pair('ms_flush', '%.2f')} | "
                  f"{pair('shards', '%.2f')} | {pair('busiest_shard', '%.2f')} | {fmt(f.get('trickled'), '%.3f')} |")
    print("\n## Each side, the median over its rounds and the range\n")
    rows = [("The cell's read p99, during over before (client)", lambda fig, host: fig["read_ratio"], False),
            ("The scan's partitions a second", lambda fig, host: fig["partitions"], False),
            ("The scan, s", lambda fig, host: fig["scan_secs"], False)]
    for name, label in (("get_p99", "get p99, during over before"), ("get_p99", "get p99 ms during"),
                        ("ms_read", "disk ms a read, during over before"), ("ms_read", "disk ms a read during"),
                        ("reads_in_flight", "disk reads in flight during"), ("queue", "disk requests in flight during"),
                        ("ms_flush", "disk ms a flush during"), ("mib_read", "disk MiB read a second during"),
                        ("shards", "shards' cores busy, during over before"), ("busiest_shard", "busiest shard during")):
        if label.endswith("during over before"):
            rows.append((label, lambda fig, host, name=name: ratio(fig["hosts"].get(host, {}), name), True))
        else:
            rows.append((label, lambda fig, host, name=name: fig["hosts"].get(host, {}).get(f"{name}_during"), True))
    rows.append(("scan bytes on the trickle", lambda fig, host: fig["hosts"].get(host, {}).get("trickled"), True))
    print("| Figure | " + " | ".join(sides) + " | Ranges |")
    print("| --- |" + " --- |" * len(sides) + " --- |")
    for label, value_of, by_host in rows:
        for host in (hosts if by_host else [None]):
            values = {side: [value_of(fig, host) for fig in rounds if fig["side"] == side] for side in sides}
            apart = ranges_apart(*values.values()) if len(sides) == 2 else None
            verdict = "-" if apart is None else ("disjoint" if apart else "overlap")
            name = f"{host}: {label}" if host else label
            print(f"| {name} | " + " | ".join(spread(values[side]) for side in sides) + f" | {verdict} |")
    every_second = [row for figures in rounds for row in figures["seconds"]]
    if csv_path and every_second:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with open(csv_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(every_second[0].keys()))
            writer.writeheader()
            writer.writerows(every_second)


if __name__ == "__main__":
    main()
