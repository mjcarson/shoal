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


def wrap_at(run, table="Cell"):
    """The second of a run's series by which its table's feed had taken the rows it wrapped at, or
    none if it never wrapped: past it the cell overwrites the rows its own arm wrote"""
    at = (((run.get("feeds") or {}).get(table)) or {}).get("wrapped_at")
    if not at:
        return None
    taken = 0
    for second in run.get("series") or []:
        taken += ((((second.get("summary") or {}).get("insert")) or {}).get("ok") or 0)
        if taken >= at:
            return second.get("at")
    return None


def windows(run):
    """A scan arm's before and during windows as epoch seconds, `(low, high)` each, and whether its
    scan ended inside the arm, or none: before the bench's own before window, ending at the scan's ask;
    during from the ask to the last page, or, for a scan its event's timeout cut, to that cut (the
    bench's own during window), else the arm's last measured second; either cut short at the cell's
    feed's wrap, past which its workload is another"""
    event = run.get("event") or {}
    marks = event.get("marks") or []
    asked = [mark["at_ms"] / 1000 for mark in marks if mark.get("kind") == "requested"]
    done = [mark["at_ms"] / 1000 for mark in marks if mark.get("kind") == "done"]
    if not asked:
        return None
    start = epoch(run["started_at"])
    before_secs = ((event.get("windows") or {}).get("before") or {}).get("secs") or 18
    ask = start + min(asked)
    measured = [second.get("at") for second in run.get("series") or [] if second.get("phase") == "measure"]
    # an unfinished scan's during window as the bench cut it, at its event's timeout, else the arm's end
    during_secs = ((event.get("windows") or {}).get("during") or {}).get("secs")
    if done:
        end = start + max(done)
    elif during_secs:
        end = ask + during_secs
    else:
        end = start + (max(measured) + 1 if measured else min(asked))
    wrap = wrap_at(run)
    if wrap is not None:
        end = min(end, start + wrap)
    return (ask - before_secs, ask), (ask, end), bool(done)


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


def replay_run(root, round_):
    """The cell alone arm of a replay round's last capture, whole or slimmed, or none"""
    for path in (root / f"m23-r{round_}-replay" / "bench.json", root / f"m23-r{round_}-replay.json"):
        if path.exists():
            with open(path, encoding="utf-8") as handle:
                capture = json.load(handle)
            for entry in capture.get("arms", []):
                if entry["id"] == "rw50/b1/none" and entry.get("runs"):
                    return entry["runs"][0]
    return None


def replay_cut(sample_dir, before_secs=18):
    """A replay round's before and during windows as epoch seconds: during from its hosts' earliest
    fio start to their latest end, by the driver's clock, a control round's the same seconds waited
    out; before the same length W7's before window is, ending at the start; or none"""
    starts, ends = [], []
    for path in sorted(sample_dir.glob("replay-*.txt")):
        for line in path.read_text().splitlines():
            fields = line.split()
            if len(fields) == 2 and fields[0] in ("start", "end"):
                (starts if fields[0] == "start" else ends).append(float(fields[1]))
    if not starts or not ends:
        return None
    start, end = min(starts), max(ends)
    return (start - before_secs, start), (start, end)


def fio_figures(sample_dir, host):
    """What a host's fio did over its replay: its reads a second, KiB a read and mean and p99 time a
    read in ms, or none in a control round"""
    path = sample_dir / f"fio-{host}.json"
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8", errors="replace")
    try:
        read = json.loads(text[text.index("{"):])["jobs"][0]["read"]
    except (ValueError, KeyError, IndexError):
        return None
    clat = read.get("clat_ns") or {}
    p99_ns = (clat.get("percentile") or {}).get("99.000000")
    return {"iops": read.get("iops"), "kib": read.get("bw_bytes", 0) / read["iops"] / 1024 if read.get("iops") else None,
            "ms_mean": clat.get("mean", 0) / 1e6 if clat.get("mean") else None,
            "ms_p99": p99_ns / 1e6 if p99_ns else None}


def client_per_second(run, cut):
    """The client's read p99 a second over a round's two windows, each the median of its measured
    seconds' p99s, and during over before: the same view of a scan's arm and of a replay's"""
    start = epoch(run["started_at"])
    out = []
    for low, high in cut:
        values = [((((second.get("summary") or {}).get("read") or {}).get("latency")) or {}).get("p99_ms")
                  for second in run.get("series") or []
                  if second.get("phase") == "measure" and second.get("at") is not None
                  and low <= start + second["at"] < high]
        out.append(median(values))
    before, during = out
    return before, during, (during / before if before and during is not None else None)


def round_figures(root, round_):
    """One round's figures, by window, or none when its capture or samples are missing: a scan round's
    windows are its scan's, a replay round's its fio's"""
    sample_dir = root / f"m23-r{round_}-samples"
    if not sample_dir.exists():
        return None
    run = scan_arm(root, round_)
    replay = run is None
    if replay:
        run = replay_run(root, round_)
        cut = replay_cut(sample_dir)
        if run is None or cut is None:
            return None
        finished = True
    else:
        cut = windows(run)
        if cut is None:
            return None
        cut, finished = cut[:2], cut[2]
    # a replay round is grouped by whether it replayed, a scan round by its scan settings
    named = sample_dir / ("side.txt" if (sample_dir / "side.txt").exists() else "scans.txt")
    side = named.read_text().strip() if named.exists() else "?"
    disks = {}
    if (sample_dir / "disks.txt").exists():
        for line in (sample_dir / "disks.txt").read_text().splitlines():
            fields = line.split()
            if len(fields) == 3:
                disks[fields[0]] = fields[2]
    event_windows = (run.get("event") or {}).get("windows") or {}
    out = {"round": f"{root.parent.name} r{round_}", "side": side, "cut": cut, "finished": finished, "hosts": {},
           "seconds": []}
    # the client's read p99 a second, the one view both kinds of round have
    out["read_before_s"], out["read_during_s"], out["read_ratio_s"] = client_per_second(run, cut)
    # the client's view, as W7 judges it, and the scan's own rate
    before_p99, during_p99 = p99(event_windows.get("before"), "read"), p99(event_windows.get("during"), "read")
    out["read_before"], out["read_during"] = before_p99, during_p99
    out["read_ratio"] = during_p99 / before_p99 if before_p99 and during_p99 else None
    out["partitions"] = (((event_windows.get("during") or {}).get("scan")) or {}).get("partitions_per_sec")
    out["scan_secs"] = cut[1][1] - cut[1][0]
    if replay:
        out["read_before"] = out["read_during"] = out["read_ratio"] = out["partitions"] = None
    trickles = {} if replay else trickle_shares(run)
    members = stats_stream(sample_dir / "stats.json")
    for host, disk in sorted(disks.items()):
        seconds = per_second(samples(sample_dir / f"samples-{host}.log"), disk)
        reports = members.get(host, {})
        figures = {"trickled": trickles.get(host), "fio": fio_figures(sample_dir, host)}
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
            out["seconds"].append({"round": out["round"], "side": side, "host": host, "disk": disk,
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
    roots = [pathlib.Path(arg) for arg in args]
    rounds = []
    for root in roots:
        for path in sorted(root.glob("m23-r*-samples"), key=lambda path: int(path.name.split("-")[1][1:])):
            figures = round_figures(root, int(path.name.split("-")[1][1:]))
            if figures:
                rounds.append(figures)
    if not rounds:
        sys.exit(f"no round with a capture and samples under {', '.join(map(str, roots))}")
    sides = sorted({figures["side"] for figures in rounds})
    hosts = sorted({host for figures in rounds for host in figures["hosts"]})
    print("# Item 359: what the cell's slow reads wait on beside a whole scan of Big\n")
    print(f"From {', '.join(f'`{root}`' for root in roots)}, {len(rounds)} rounds. A scan round's side is its scan settings "
          "and its windows its scan's: *before* the bench's before window, *during* the scan's ask to its last page. A replay "
          "round's side is whether fio replayed the scan's reads, and its windows its fio's: *during* from its start to its "
          "end, *before* the 18 s before it. A figure is the median of its seconds in a window.\n")
    print("## Each round\n")
    print("| Round | Side | During, s | Scan partitions a second | Read p99 ms, before to during (W7) | During over before (W7) "
          "| Read p99 a second, before to during | During over before, a second |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- |")
    for figures in rounds:
        secs = fmt(figures['scan_secs'], '%.0f') + ("" if figures["finished"] else ", unfinished")
        print(f"| {figures['round']} | {figures['side']} | {secs} | {fmt(figures['partitions'], '%.0f')} | "
              f"{fmt(figures['read_before'], '%.2f')} to {fmt(figures['read_during'], '%.2f')} | {fmt(figures['read_ratio'], '%.3f')} | "
              f"{fmt(figures['read_before_s'], '%.2f')} to {fmt(figures['read_during_s'], '%.2f')} | {fmt(figures['read_ratio_s'], '%.3f')} |")
    replays = [(figures["round"], host, figures["hosts"][host]["fio"]) for figures in rounds for host in sorted(figures["hosts"])
               if figures["hosts"][host].get("fio")]
    if replays:
        print("\n| Round | Host | fio reads a second | KiB a read | ms a read, mean | ms a read, p99 |")
        print("| --- | --- | --- | --- | --- | --- |")
        for round_, host, fio in replays:
            print(f"| {round_} | {host} | {fmt(fio['iops'], '%.0f')} | {fmt(fio['kib'], '%.1f')} | {fmt(fio['ms_mean'], '%.3f')} | "
                  f"{fmt(fio['ms_p99'], '%.3f')} |")
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
    rows = [("The cell's read p99, during over before (client, W7)", lambda fig, host: fig["read_ratio"], False),
            ("The cell's read p99 a second, during over before (client)", lambda fig, host: fig["read_ratio_s"], False),
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
    # with more than two sides, every pair of them on the figures the question turns on
    if len(sides) > 2:
        print("\n## Each pair of sides, whether their ranges are disjoint\n")
        print("| Figure | Pair | Verdict |")
        print("| --- | --- | --- |")
        for label, value_of, by_host in rows:
            if label not in ("The cell's read p99 a second, during over before (client)", "get p99, during over before",
                             "disk reads in flight during", "shards' cores busy, during over before"):
                continue
            for host in (hosts if by_host else [None]):
                for at, one in enumerate(sides):
                    for other in sides[at + 1:]:
                        apart = ranges_apart([value_of(fig, host) for fig in rounds if fig["side"] == one],
                                             [value_of(fig, host) for fig in rounds if fig["side"] == other])
                        verdict = "-" if apart is None else ("disjoint" if apart else "overlap")
                        name = f"{host}: {label}" if host else label
                        print(f"| {name} | {one} / {other} | {verdict} |")
    every_second = [row for figures in rounds for row in figures["seconds"]]
    if csv_path and every_second:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with open(csv_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(every_second[0].keys()))
            writer.writeheader()
            writer.writerows(every_second)


if __name__ == "__main__":
    main()
