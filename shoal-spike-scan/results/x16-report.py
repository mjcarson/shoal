"""X16's tables from its captures: the walk beside the foreground, and W1's rows before and after

    python3 -I shoal-spike-scan/results/x16-report.py shoal-spike-scan/results/x16 > x16-report.md

Every capture is `x16-<side>-r<round>-<which>/bench.json`, which `x16-lab.sh` names, or the same
slimmed into `x16-<side>-r<round>-<which>.json` by its `fetch`. For each side and
table this prints the scan's own rate and its pages' figures, alone and beside the cell; the cell's
read and insert p99 before, during and after a scan, and their ratio; the evictions memory began;
and the device bytes a scanned partition took. Every figure is each round's, then its median and
range over the rounds; a before and after is a difference only where the two sides' ranges are
disjoint, the lab's rule.
"""

import json
import pathlib
import re
import statistics
import sys

# a capture's name: its side, its round and which of a side round's runs it is
NAME = re.compile(r"^x16-(before|after)-r(\d+)-(load|big|keyword)(\.json)?$")


def load(root):
    """Every capture under a directory, whole or slimmed, by side, round and run"""
    captures = {}
    for path in sorted(pathlib.Path(root).iterdir()):
        match = NAME.match(path.name)
        if not match:
            continue
        file = path if path.is_file() else path / "bench.json"
        if not file.exists():
            continue
        side, round_, which = match.group(1), int(match.group(2)), match.group(3)
        with open(file, encoding="utf-8") as handle:
            captures[(side, round_, which)] = json.load(handle)
    return captures


def arm(capture, arm_id):
    """An arm's one run, or none"""
    for entry in capture.get("arms", []):
        if entry["id"] == arm_id and entry.get("runs"):
            return entry["runs"][0]
    return None


def p99(summary, kind):
    """A kind's p99 in a window's summary, or none"""
    latency = (summary or {}).get(kind, {}).get("latency") or {}
    return latency.get("p99_ms")


def evicted_by_resident(run):
    """The partitions memory pressure evicted over a run, every member's, from its server series"""
    series = run.get("server_series") or []
    if len(series) < 2:
        return None
    first, last = series[0].get("memory") or {}, series[-1].get("memory") or {}
    return sum(last[m].get("evicted_by_resident", 0) - first.get(m, {}).get("evicted_by_resident", 0) for m in last)


def read_bytes(run):
    """The bytes every host's devices read over a run"""
    return sum(device.get("read_bytes", 0) for host in run.get("devices") or [] for device in host.get("devices", []))


def figures(captures):
    """Every figure of every capture, by side, table and name, a value a round"""
    rows = {}

    def put(side, table, name, round_, value):
        if value is not None:
            rows.setdefault((side, table, name), {})[round_] = value

    for (side, round_, which), capture in captures.items():
        if which == "load":
            continue
        # the cell alone, the first arm of a side round
        alone = arm(capture, "rw50/b1/none")
        if alone:
            measured = alone["measured"]
            put(side, "cell", "read p99 ms, alone", round_, p99(measured, "read"))
            put(side, "cell", "insert p99 ms, alone", round_, p99(measured, "insert"))
            put(side, "cell", "reads a second, alone", round_, measured.get("read", {}).get("per_sec"))
            put(side, "cell", "Resident evictions, alone", round_, evicted_by_resident(alone))
        # each table scanned alone, whole scans back to back
        for table in ("Big", "Keyword"):
            run = arm(capture, f"scan/{table}/none")
            if not run:
                continue
            scan = run["measured"].get("scan") or {}
            put(side, table, "partitions a second, alone", round_, scan.get("partitions_per_sec"))
            put(side, table, "MiB a second, alone", round_, (scan.get("bytes_per_sec") or 0) / (1 << 20))
            for figure in ("held", "read"):
                for point in ("p50", "p99", "max"):
                    put(side, table, f"page {figure} {point} ms, alone", round_, (scan.get(figure) or {}).get(f"{point}_ms"))
            for member, mine in (scan.get("members") or {}).items():
                put(side, table, f"{member}: partitions a second, alone", round_, mine.get("partitions_per_sec"))
                put(side, table, f"{member}: page read p99 ms, alone", round_, (mine.get("read") or {}).get("p99_ms"))
            put(side, table, "pages shed a page, alone", round_, scan.get("shed", 0) / max(scan.get("pages", 0), 1))
            put(side, table, "Resident evictions, alone", round_, evicted_by_resident(run))
            # the device counters span the whole run, its warmup too, so its partitions are both windows'
            warmup = ((run.get("warmup") or {}).get("scan") or {}).get("partitions") or 0
            partitions = (scan.get("partitions") or 0) + warmup
            if partitions:
                put(side, table, "device bytes a partition, alone", round_, read_bytes(run) / partitions)
        # the cell beside a whole scan of a table: the event arm of either capture
        table = {"big": "Big", "keyword": "Keyword"}[which]
        run = arm(capture, "rw50/b1/scan")
        if not run or not run.get("event"):
            continue
        windows = run["event"].get("windows") or {}
        before, during, after = windows.get("before"), windows.get("during"), windows.get("after")
        for kind in ("read", "insert"):
            put(side, table, f"cell {kind} p99 ms before", round_, p99(before, kind))
            put(side, table, f"cell {kind} p99 ms during", round_, p99(during, kind))
            put(side, table, f"cell {kind} p99 ms after", round_, p99(after, kind))
            if p99(before, kind) and p99(during, kind):
                put(side, table, f"cell {kind} p99 during / before", round_, p99(during, kind) / p99(before, kind))
        scan = (during or {}).get("scan") or {}
        put(side, table, "partitions a second, beside the cell", round_, scan.get("partitions_per_sec"))
        put(side, table, "whole scan s, beside the cell", round_, ((scan.get("whole") or {}).get("max_ms") or 0) / 1000 or None)
        for figure in ("held", "read"):
            for point in ("p50", "p99", "max"):
                put(side, table, f"page {figure} {point} ms, beside the cell", round_, (scan.get(figure) or {}).get(f"{point}_ms"))
        put(side, table, "page paced p99 ms, beside the cell", round_, (scan.get("paced") or {}).get("p99_ms"))
        put(side, table, "page queued p99 ms, beside the cell", round_, (scan.get("queued") or {}).get("p99_ms"))
        put(side, table, "Resident evictions, beside the cell", round_, evicted_by_resident(run))
        # and by member, the page's hold and read
        for member, mine in (scan.get("members") or {}).items():
            put(side, table, f"{member}: partitions a second, beside the cell", round_, mine.get("partitions_per_sec"))
            put(side, table, f"{member}: page held p99 ms, beside the cell", round_, (mine.get("held") or {}).get("p99_ms"))
            put(side, table, f"{member}: page read p99 ms, beside the cell", round_, (mine.get("read") or {}).get("p99_ms"))
    return rows


def cell(values):
    """A figure's rounds as its median and range"""
    if not values:
        return "-"
    ordered = sorted(values.values())
    median = statistics.median(ordered)
    return f"{median:.3g} [{ordered[0]:.3g}, {ordered[-1]:.3g}] (n {len(ordered)})"


# the page's tables: each a title and its figures, in order
PAGE = [
    ("The cell alone", "cell", ["reads a second, alone", "read p99 ms, alone", "insert p99 ms, alone", "Resident evictions, alone"]),
    ("The cell beside a whole scan of Big", "Big", [
        "cell read p99 ms before", "cell read p99 ms during", "cell read p99 during / before",
        "cell insert p99 ms before", "cell insert p99 ms during", "cell insert p99 during / before",
        "whole scan s, beside the cell", "partitions a second, beside the cell", "Resident evictions, beside the cell",
    ]),
    ("The cell beside a whole scan of Keyword", "Keyword", [
        "cell read p99 ms before", "cell read p99 ms during", "cell read p99 during / before",
        "cell insert p99 ms before", "cell insert p99 ms during", "cell insert p99 during / before",
        "whole scan s, beside the cell", "partitions a second, beside the cell", "Resident evictions, beside the cell",
    ]),
    ("Big scanned alone", "Big", [
        "partitions a second, alone", "MiB a second, alone", "page held p50 ms, alone", "page held p99 ms, alone",
        "page held max ms, alone", "page read p50 ms, alone", "page read p99 ms, alone", "page read max ms, alone",
        "pages shed a page, alone", "device bytes a partition, alone",
    ]),
    ("Keyword scanned alone", "Keyword", [
        "partitions a second, alone", "MiB a second, alone", "page held p50 ms, alone", "page held p99 ms, alone",
        "page held max ms, alone", "page read p50 ms, alone", "page read p99 ms, alone", "page read max ms, alone",
        "pages shed a page, alone", "device bytes a partition, alone",
    ]),
    ("A page beside the cell, Big", "Big", [
        "page held p99 ms, beside the cell", "page held max ms, beside the cell", "page read p99 ms, beside the cell",
        "page queued p99 ms, beside the cell", "page paced p99 ms, beside the cell",
    ]),
    ("A page beside the cell, Keyword", "Keyword", [
        "page held p99 ms, beside the cell", "page held max ms, beside the cell", "page read p99 ms, beside the cell",
        "page queued p99 ms, beside the cell", "page paced p99 ms, beside the cell",
    ]),
]


def budget(before, during):
    """Whether a p99 during a scan is inside W7's budget of its before, max(1.10 x, + 0.1 ms)"""
    return during <= max(1.10 * before, before + 0.1)


def page(rows):
    """The page's tables, and W7's budget judged on the medians"""
    for title, table, names in PAGE:
        print(f"\n### {title}\n")
        print("| Figure | Before (F115) | After (F119) | Disjoint |")
        print("| --- | --- | --- | --- |")
        for name in names:
            before = rows.get(("before", table, name), {})
            after = rows.get(("after", table, name), {})
            if not before and not after:
                continue
            disjoint = ""
            if before and after:
                b, a = sorted(before.values()), sorted(after.values())
                disjoint = "yes" if b[-1] < a[0] or a[-1] < b[0] else "no"
            print(f"| {name} | {cell(before)} | {cell(after)} | {disjoint} |")
    print("\n### W7's budget, on the medians\n")
    print("| Scan | Kind | Before (F115) | After (F119) |")
    print("| --- | --- | --- | --- |")
    for table in ("Big", "Keyword"):
        for kind in ("read", "insert"):
            verdicts = []
            for side in ("before", "after"):
                b = rows.get((side, table, f"cell {kind} p99 ms before"), {})
                d = rows.get((side, table, f"cell {kind} p99 ms during"), {})
                if b and d:
                    mb, md = statistics.median(b.values()), statistics.median(d.values())
                    verdicts.append(f"{mb:.2f} -> {md:.2f} ms, {'inside' if budget(mb, md) else 'outside'}")
                else:
                    verdicts.append("-")
            print(f"| {table} | {kind} | {verdicts[0]} | {verdicts[1]} |")


def main():
    """Print every table, or with --page the page's"""
    captures = load(sys.argv[1])
    rows = figures(captures)
    if "--page" in sys.argv[2:]:
        page(rows)
        return
    tables = sorted({table for (_, table, _) in rows})
    for table in tables:
        names = sorted({name for (_, t, name) in rows if t == table})
        print(f"\n## {table}\n")
        print("| Figure | before (F115) | after (F119) | Disjoint |")
        print("| --- | --- | --- | --- |")
        for name in names:
            before = rows.get(("before", table, name), {})
            after = rows.get(("after", table, name), {})
            disjoint = ""
            if before and after:
                b, a = sorted(before.values()), sorted(after.values())
                disjoint = "yes" if b[-1] < a[0] or a[-1] < b[0] else "no"
            print(f"| {name} | {cell(before)} | {cell(after)} | {disjoint} |")


if __name__ == "__main__":
    main()
