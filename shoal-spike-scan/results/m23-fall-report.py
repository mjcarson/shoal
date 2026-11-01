"""Item 351's run from its captures and samples: what changed on the lab when the cell's inserts fell

    python3 -I shoal-spike-scan/results/m23-fall-report.py <captures> [--csv <file>] > report.md
    python3 -I shoal-spike-scan/results/m23-fall-report.py falls <captures>...

`<captures>` is the directory `m23-lab.sh`'s `FALL_ONLY=1` run writes, `$LAB/captures`, or the one its
`fetch` slims them into: `m23-fall-a` and `m23-fall-b`, the cell truly alone for two arms a rest
apart; `m23-fall-cluster`, every host's samples each second beside them (m23-fall-sample.sh), the
leader's view of every member's figures each second (`stats.json`) and each host's node journal; and
`m23-fall-fio`, the samples beside fio replaying each host's flushes with no Shoal unit up.

For each arm it finds where the cell's inserts fell (the rule `m23-report.py`'s `insert_fall`
applies), and for each host and each leg where its whole NVMe's time a flush stepped up and stayed
up. Then it prints every figure early in a leg beside the same figure late in it, so what moved at
the fall reads beside what did not (the predictions are in `results/m23-fall/predictions.md`).

`falls` reads any captures, whole or slimmed, and prints each arm's fall beside its event's marks:
how an older record is checked for a window that straddled the fall. Beside an event arm it splits
the event's window at the fall, by the median of the cell's insert p99 a second, and it says when a
fall came in an arm's warmup, as it does in every arm the rounds again warm past the drives' change.

The run found the lab's 970 EVOs: their flush goes from about 0.8 ms to 2.8 about 75 s into synced
writes and stays until they rest (Resolved #351, docs/src/appendix/resolved/the-cells-fall-is-the-drives.md).
`report.md` and `seconds.csv` in `results/m23-fall` are this script's output over the run's raw
samples, which stay in `target/lab/m23-fall`.
"""

import csv
import datetime
import json
import pathlib
import re
import statistics
import sys

# a clock tick, which /proc/<pid>/task/<tid>/stat counts a thread's cpu in, on every lab host
TICKS = 100


def started(run):
    """When a run began, its warmup's first second, in seconds since the epoch"""
    return datetime.datetime.fromisoformat(run["started_at"].replace("Z", "+00:00")).timestamp()


def capture(root, label):
    """A capture by its label, whole (`<label>/bench.json`) or slimmed (`<label>.json`)"""
    for path in (root / label / "bench.json", root / f"{label}.json"):
        if path.exists():
            with open(path, encoding="utf-8") as handle:
                return json.load(handle)
    return None


def insert_fall(series, share=0.75, hold=30, low=10, high=60, least=10):
    """Where an arm's inserts fell and stayed down: the first second at or past `high` from which
    every five seconds' mean is under a share of the median of its seconds `low` to `high`, for
    `hold` seconds or to the arm's end, and at least `least` of them, and the inserts taken by then;
    none if they held their rate. The rule `m23-report.py`'s `insert_fall` applies, the window its
    arguments: `least` keeps an arm's last few seconds, which every arm's wind-down dips in, from
    reading as a fall"""
    rates = [((second.get("summary") or {}).get("insert") or {}).get("ok") or 0 for second in series]
    ats = [second.get("at") or 0 for second in series]
    early = [rate for at, rate in zip(ats, rates) if low <= at < high]
    if len(early) < 30 or len(rates) < high + 10:
        return None
    floor = share * statistics.median(early)
    means = [sum(rates[i:i + 5]) / 5 for i in range(len(rates) - 4)]
    for start in range(len(means)):
        if ats[start] < high:
            continue
        held = means[start:start + hold]
        if len(held) >= least and all(mean < floor for mean in held):
            return ats[start], sum(rates[:start])
    return None


def step(points, low=10, high=40, after=20, factor=2.0, hold=30):
    """Where a figure stepped up and stayed up in a leg: the first second at or past `after` from
    which every five seconds' mean is over `factor` times the median of its seconds `low` to `high`,
    for `hold` seconds or to the leg's end; none if it held. `points` are (second into the leg, value)"""
    early = [value for at, value in points if low <= at < high and value is not None]
    if len(early) < 10:
        return None
    base = statistics.median(early)
    values = [value for _, value in points]
    means = []
    for i in range(len(values) - 4):
        window = [value for value in values[i:i + 5] if value is not None]
        means.append(sum(window) / len(window) if window else None)
    for start in range(len(means)):
        if points[start][0] < after:
            continue
        run = [mean for mean in means[start:start + hold] if mean is not None]
        if run and all(mean > factor * base for mean in run):
            return points[start][0]
    return None


def samples(path):
    """Every sample of one host's log: its time, each device's counters, the host's cpu ticks and
    each node thread's ticks and name"""
    out = []
    current = None
    with open(path, encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if line.startswith("@ "):
                current = {"at": float(line.split()[1]), "devices": {}, "cpu": None, "threads": {}}
                out.append(current)
                continue
            if current is None:
                continue
            fields = line.split()
            if line.startswith("cpu "):
                current["cpu"] = [int(field) for field in fields[1:]]
            elif line.startswith("# threads"):
                continue
            elif len(fields) >= 20 and fields[0].isdigit() and not fields[2].isdigit():
                current["devices"][fields[2]] = [int(field) for field in fields[3:]]
            elif len(fields) >= 4 and fields[0].isdigit():
                current["threads"][fields[0]] = (int(fields[1]) + int(fields[2]), " ".join(fields[3:]))
    return out


def kind(comm):
    """A node thread's kind by its name: a shard's executor, the control thread, an object executor"""
    if comm.startswith("unnamed-"):
        return "shards"
    if comm.startswith("shoal-control"):
        return "control"
    if comm.startswith("shoal-objects"):
        return "objects"
    return "other"


def per_second(host_samples, disk):
    """A host's figures for each second between two samples, keyed by the epoch second it began in:
    the disk's flushes a second and time a flush, writes a second and time a write, MB a second and
    busy share, and the node's cores busy by thread kind"""
    out = {}
    for before, after in zip(host_samples, host_samples[1:]):
        elapsed = after["at"] - before["at"]
        if elapsed <= 0:
            continue
        row = {}
        old, new = before["devices"].get(disk), after["devices"].get(disk)
        if old and new:
            # /proc/diskstats after the name: reads 0-3, writes 4-7, in flight 8, io ms 9,
            # weighted 10, discards 11-14, flushes 15 and their ms 16
            writes, write_ms = new[4] - old[4], new[7] - old[7]
            flushes, flush_ms = new[15] - old[15], new[16] - old[16]
            row["flushes"] = flushes / elapsed
            row["ms_flush"] = flush_ms / flushes if flushes else None
            row["writes"] = writes / elapsed
            row["ms_write"] = write_ms / writes if writes else None
            row["mb"] = (new[6] - old[6]) * 512 / elapsed / 1e6
            row["busy"] = (new[9] - old[9]) / 1000 / elapsed
        cores = {}
        for tid, (ticks, comm) in after["threads"].items():
            if tid in before["threads"]:
                kind_of = kind(comm)
                cores[kind_of] = cores.get(kind_of, 0) + (ticks - before["threads"][tid][0]) / TICKS / elapsed
        for kind_of, value in cores.items():
            row[f"cpu_{kind_of}"] = value
        out[int(before["at"])] = row
    return out


def stats_stream(path):
    """Every view `x16 stats --json --watch 1` printed: each member's figures by hostname, at the
    second the member derived them"""
    out = {}
    if not path.exists():
        return out
    text = path.read_text(encoding="utf-8", errors="replace")
    decoder = json.JSONDecoder()
    index = 0
    while True:
        while index < len(text) and text[index] != "{":
            index += 1
        if index >= len(text):
            break
        try:
            view, index = decoder.raw_decode(text, index)
        except json.JSONDecodeError:
            index += 1
            continue
        for member in view.get("members") or []:
            stats = member.get("stats") or {}
            name = stats.get("hostname") or member.get("name")
            if not name or not stats.get("at_ms"):
                continue
            out.setdefault(name, {})[int(stats["at_ms"] / 1000)] = stats
    return out


# the figures each member's own report carries, which the run reads beside its devices
NODE_FIGURES = ["wal_syncs_per_sec", "wal_sync_ms", "wal_appends_per_sync", "wal_segments",
                "compacting_segments", "apply_lag", "pending_bytes"]
# a host's figures, with the format each prints in
HOST_FIGURES = [("flushes", "%.0f"), ("ms_flush", "%.2f"), ("writes", "%.0f"), ("ms_write", "%.2f"),
                ("mb", "%.1f"), ("busy", "%.2f"), ("cpu_shards", "%.2f"), ("cpu_control", "%.2f")]


def window(start, low, high):
    """The epoch seconds of a leg's seconds `low` to `high`, the leg begun at `start`"""
    return range(int(start + low), int(start + high))


def median_of(by_second, seconds, figure):
    """The median of a figure over some epoch seconds, or None where none has it"""
    values = [by_second[second].get(figure) for second in seconds if second in by_second]
    values = [value for value in values if value is not None]
    return statistics.median(values) if values else None


def fmt(value, form="%.2f"):
    """A figure as text, or a dash"""
    return "-" if value is None else form % value


def disks(root):
    """Each host's whole NVMe under its node's root, from the fio leg's rates, or the one that
    flushed most where the leg did not run"""
    out = {}
    rates = root / "m23-fall-fio" / "rates.txt"
    if rates.exists():
        for line in rates.read_text().splitlines():
            fields = line.split()
            if len(fields) >= 3:
                out[fields[0]] = fields[2]
    return out


def busiest(host_samples):
    """The whole NVMe that wrote most over a host's samples, its node's: by writes, not flushes,
    since a device whose cache writes through, europa's Optane, is sent no flush"""
    if len(host_samples) < 2:
        return None
    first, last = host_samples[0]["devices"], host_samples[-1]["devices"]
    named = [name for name in last if name.startswith("nvme") and name in first]
    return max(named, key=lambda name: last[name][4] - first[name][4], default=None)


def report(root, csv_path=None):
    """The run's report, as markdown on stdout"""
    root = pathlib.Path(root)
    arms = {leg: capture(root, f"m23-fall-{leg}") for leg in ("a", "b")}
    cluster = root / "m23-fall-cluster"
    hosts = sorted(path.name[len("samples-"):-len(".log")] for path in cluster.glob("samples-*.log"))
    named = disks(root)
    seconds = {}
    for host in hosts:
        host_samples = samples(cluster / f"samples-{host}.log")
        disk = named.get(host) or busiest(host_samples)
        named[host] = disk
        seconds[host] = per_second(host_samples, disk)
    nodes = stats_stream(cluster / "stats.json")
    print("# Item 351's run\n")
    print("Every figure is a median over the seconds named: *early* is a leg's seconds 10 to 60 and *late*"
          " its seconds 120 to its end. A step is where a figure's five second mean went over twice its"
          " seconds 10 to 40 and stayed there 30 s.\n")
    # each arm: its client's rates and its fall
    print("## The cell's arms\n")
    print("| Arm | Seconds | Inserts a second, early | late | Insert p50 ms, early | late | The fall, s in | Inserts by then |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- |")
    legs = {}
    rows = []
    for leg, cap in arms.items():
        # a capture still being taken holds no run yet
        if cap is None or not (cap.get("arms") or [{}])[0].get("runs"):
            continue
        run = cap["arms"][0]["runs"][0]
        series = run.get("series") or []
        start = started(run)
        legs[leg] = (start, len(series))
        fall = insert_fall(series)
        by_at = {second["at"]: second["summary"] for second in series}
        def client(figure, low, high):
            values = []
            for at in range(low, high):
                summary = by_at.get(at)
                if summary is None:
                    continue
                insert = summary.get("insert") or {}
                values.append(insert.get("ok") if figure == "ok" else (insert.get("latency") or {}).get("p50_ms"))
            values = [value for value in values if value is not None]
            return statistics.median(values) if values else None
        print(f"| {leg} | {len(series)} | {fmt(client('ok', 10, 60), '%.0f')} | {fmt(client('ok', 120, len(series)), '%.0f')}"
              f" | {fmt(client('p50', 10, 60))} | {fmt(client('p50', 120, len(series)))}"
              f" | {fall[0] if fall else 'none'} | {fall[1] if fall else '-'} |")
        for second in series:
            at = second["at"]
            epoch = int(start + at)
            insert = second["summary"].get("insert") or {}
            row = {"leg": leg, "at": at, "epoch": epoch, "inserts": insert.get("ok"),
                   "insert_p50_ms": (insert.get("latency") or {}).get("p50_ms")}
            for host in hosts:
                for figure, _ in HOST_FIGURES:
                    row[f"{host}_{figure}"] = seconds[host].get(epoch, {}).get(figure)
            for host, by_second in nodes.items():
                for figure in ("wal_sync_ms", "wal_appends_per_sync", "compacting_segments"):
                    row[f"{host}_{figure}"] = (by_second.get(epoch) or {}).get(figure)
            rows.append(row)
    # each host: its figures early and late in each arm, and where its time a flush stepped
    print("\n## Each host's disk and node, beside the arms\n")
    print("| Host | Disk | Arm | Figure | Early | Late | Stepped, s in |")
    print("| --- | --- | --- | --- | --- | --- | --- |")
    for host in hosts:
        for leg, (start, length) in legs.items():
            for figure, form in HOST_FIGURES:
                early = median_of(seconds[host], window(start, 10, 60), figure)
                late = median_of(seconds[host], window(start, 120, length), figure)
                stepped = "-"
                if figure == "ms_flush":
                    points = [(at, seconds[host].get(int(start + at), {}).get(figure)) for at in range(length)]
                    stepped = step(points) or "none"
                print(f"| {host} | {named[host]} | {leg} | {figure} | {fmt(early, form)} | {fmt(late, form)} | {stepped} |")
    # each member's own figures
    print("\n## Each member's own figures, beside the arms\n")
    print("| Member | Arm | Figure | Early | Late |")
    print("| --- | --- | --- | --- | --- |")
    for host, by_second in sorted(nodes.items()):
        for leg, (start, length) in legs.items():
            for figure in NODE_FIGURES:
                early = median_of(by_second, window(start, 10, 60), figure)
                late = median_of(by_second, window(start, 120, length), figure)
                print(f"| {host} | {leg} | {figure} | {fmt(early)} | {fmt(late)} |")
    # what the nodes logged
    print("\n## What the nodes logged\n")
    for path in sorted(cluster.glob("journal-*.txt")):
        text = path.read_text(encoding="utf-8", errors="replace").splitlines()
        forced = sum("forcing a group past a sealed segment" in line for line in text)
        warns = sum(" WARN " in line or " ERROR " in line for line in text)
        print(f"- {path.stem[len('journal-'):]}: {len(text)} lines, {warns} at WARN or ERROR, {forced} forced purges")
        # what they said, by message: an event's `msg`, or the rest of a line from a crate that names none
        said = {}
        for line in text:
            match = re.search(r'msg="([^"]*)"', line)
            if match:
                key = match.group(1)
            else:
                match = re.search(r"Z\s+(\w+)\s+(\S+): (.{0,60})", line)
                key = f"{match.group(2)}: {match.group(3)}" if match else line[-60:]
            said[key] = said.get(key, 0) + 1
        for key, count in sorted(said.items(), key=lambda item: -item[1])[:6]:
            print(f"  - {count} x {key}")
    # fio alone
    fio = root / "m23-fall-fio"
    if fio.exists():
        print("\n## fio alone, no Shoal unit up\n")
        rates = (fio / "rates.txt").read_text().splitlines() if (fio / "rates.txt").exists() else []
        for line in rates:
            fields = line.split()
            print(f"- {fields[0]}: {fields[3]} jobs on {fields[2]} under {fields[1]}, {fields[4]} flushes a second"
                  f" of {int(fields[5]) // 1024} KiB, the first arm's")
        marks = {}
        for line in (fio / "legs.txt").read_text().splitlines() if (fio / "legs.txt").exists() else []:
            leg, what, at = line.split()
            marks.setdefault(leg, {})[what] = int(at)
        print("\n| Host | fio leg | Figure | Early | Late | Stepped, s in |")
        print("| --- | --- | --- | --- | --- | --- |")
        for path in sorted(fio.glob("samples-*.log")):
            host = path.name[len("samples-"):-len(".log")]
            by_second = per_second(samples(path), named.get(host))
            for leg, mark in sorted(marks.items()):
                start, end = mark.get("start"), mark.get("end")
                if start is None or end is None:
                    continue
                length = end - start
                for figure, form in HOST_FIGURES[:6]:
                    early = median_of(by_second, window(start, 10, 60), figure)
                    late = median_of(by_second, window(start, 120, length), figure)
                    stepped = "-"
                    if figure == "ms_flush":
                        points = [(at, by_second.get(int(start + at), {}).get(figure)) for at in range(length)]
                        stepped = step(points) or "none"
                    print(f"| {host} | {leg} | {figure} | {fmt(early, form)} | {fmt(late, form)} | {stepped} |")
                for at in range(length):
                    row = {"leg": f"fio-{leg}", "at": at, "epoch": start + at}
                    for figure, _ in HOST_FIGURES[:6]:
                        row[f"{host}_{figure}"] = by_second.get(start + at, {}).get(figure)
                    rows.append(row)
    if csv_path and rows:
        fields = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
        with open(csv_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            for row in rows:
                writer.writerow({key: ("" if value is None else (round(value, 4) if isinstance(value, float) else value))
                                 for key, value in row.items()})


def second_p99(series, kind, low, high):
    """The median of an arm's per-second p99 of a kind over its seconds `low` to `high`, or None"""
    values = [((second["summary"].get(kind) or {}).get("latency") or {}).get("p99_ms")
              for second in series if low <= second.get("at", -1) < high]
    values = [value for value in values if value]
    return statistics.median(values) if len(values) >= 3 else None


def falls(roots):
    """Every arm of every capture under some directories, its length, its event's marks and where its
    inserts fell, for an older record whose windows may straddle the fall. Beside an event arm's marks,
    the median of its per-second insert p99 before the event, and during it on each side of the fall,
    so what the event cost in one state of the drive reads apart from what the fall did"""
    print("| Capture | Arm | Seconds | Event marks, s in | The fall, s in | Insert p99 a second, before | during, before the fall | during, after it |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- |")
    for root in roots:
        for path in sorted(pathlib.Path(root).iterdir()):
            file = path / "bench.json" if path.is_dir() else path
            if not file.exists() or file.suffix != ".json":
                continue
            try:
                with open(file, encoding="utf-8") as handle:
                    cap = json.load(handle)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if not isinstance(cap, dict):
                continue
            for arm in cap.get("arms") or []:
                for run in arm.get("runs") or []:
                    series = run.get("series") or []
                    if not series:
                        continue
                    marks = ", ".join(f"{mark.get('kind')} {round((mark.get('at_ms') or 0) / 1000)}"
                                      for mark in ((run.get("event") or {}).get("marks") or []))
                    fall = insert_fall(series)
                    label = path.name[:-5] if path.name.endswith(".json") else path.name
                    # the event's window, from its request to its end, split at the fall
                    event = {mark.get("kind"): (mark.get("at_ms") or 0) / 1000
                             for mark in ((run.get("event") or {}).get("marks") or [])}
                    split = ["-", "-", "-"]
                    if "requested" in event:
                        asked, done = event["requested"], event.get("done", len(series))
                        warm = next((second.get("at") for second in series if second.get("phase") == "measure"), 10)
                        at = fall[0] if fall else done
                        split = [fmt(second_p99(series, "insert", warm, asked), "%.1f"),
                                 fmt(second_p99(series, "insert", asked, min(at, done)), "%.1f"),
                                 fmt(second_p99(series, "insert", at, done) if at < done else None, "%.1f")]
                    # a fall in an arm's warmup, which an arm warmed past the drives' change shows, is
                    # said to be one: its measured window is all after it
                    warmed = next((second.get("at") for second in series if second.get("phase") == "measure"), 0)
                    where = "none" if not fall else (f"{fall[0]}, in the warmup" if fall[0] < warmed else f"{fall[0]}")
                    print(f"| {label} | {arm.get('id')} | {len(series)} | {marks or '-'} | {where}"
                          f" | {' | '.join(split)} |")


def main():
    """The report, or the falls of older captures"""
    args = sys.argv[1:]
    if not args:
        sys.exit(__doc__)
    if args[0] == "falls":
        falls(args[1:])
        return
    csv_path = None
    if "--csv" in args:
        at = args.index("--csv")
        csv_path = args[at + 1]
        del args[at:at + 2]
    report(args[0], csv_path)


if __name__ == "__main__":
    main()
