"""X17's reads half and X37's tables from their captures: a page's reads in flight and in runs, one
operation a record against three, and a merge's order, beside each other

    python3 -I shoal-spike-scan/results/x17-report.py shoal-spike-scan/results/x17 > x17-report.md

Every capture is `x17-<order>-r<round>-<which>/bench.json`, which `x17-lab.sh` names, or the same
slimmed into `x17-<order>-r<round>-<which>.json` by its `fetch`; `<order>` is the merge order Big
was loaded under, `hash` or `key`. For each read setting and layout this prints a scan of Big's own
rate and what it cost: the walk's reads a page and the partitions a read and a stretch, the device
reads and bytes a partition, and the node's and the hosts' cpu a partition; then the cell beside a
scan at two settings, and gets of Big. Every figure is each round's, then its median and range over
the rounds; two figures differ only where their ranges are disjoint, the lab's rule.
"""

import json
import pathlib
import re
import statistics
import sys

# a capture's name: the merge order it was loaded under, its round and which run it is
NAME = re.compile(r"^x17-(hash|key)-r(\d+)-([a-z0-9-]+?)(\.json)?$")

# the read arms in the order the page prints them, and what each is
ARMS = [
    ("r1", "one read at a time, today's walk"),
    ("r4", "four reads in flight"),
    ("r16", "sixteen reads in flight"),
    ("g4k2", "runs across gaps of 4 KiB, two in flight"),
    ("g4k16", "runs across gaps of 4 KiB, sixteen in flight"),
    ("g128k2", "runs across gaps of 128 KiB, two in flight"),
    ("t1", "three operations a record, one read at a time"),
    ("t16", "three operations a record, sixteen in flight"),
    ("shedstay", "today's walk, the driver staying on a shed tablet as before O139"),
    ("afterchurn", "today's walk after twenty minutes of updates and inserts into Big"),
]


def load(root):
    """Every capture under a directory, whole or slimmed, by order, round and run"""
    captures = {}
    for path in sorted(pathlib.Path(root).iterdir()):
        match = NAME.match(path.name)
        if not match:
            continue
        file = path if path.is_file() else path / "bench.json"
        if not file.exists():
            continue
        order, round_, which = match.group(1), int(match.group(2)), match.group(3)
        with open(file, encoding="utf-8") as handle:
            captures[(order, round_, which)] = json.load(handle)
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


def p50(summary, kind):
    """A kind's p50 in a window's summary, or none"""
    latency = (summary or {}).get(kind, {}).get("latency") or {}
    return latency.get("p50_ms")


def hosts(run):
    """Every host's counters over a run"""
    return run.get("devices") or []


def device_sum(run, field):
    """A device counter summed over every host's devices"""
    return sum(device.get(field, 0) for host in hosts(run) for device in host.get("devices", []))


def node_cpu_secs(run):
    """The cpu every host's node process spent over a run"""
    return sum(process.get("cpu_secs", 0) for host in hosts(run) for process in host.get("processes", []))


def busy_secs(run):
    """Every host's busy cpu over a run, every process's"""
    return sum((host.get("cpu") or {}).get("busy_secs", 0) for host in hosts(run))


def scan_run_figures(run):
    """A scan arm's figures, the counters over its whole run divided by every partition it read"""
    scan = run["measured"].get("scan") or {}
    warm = (run.get("warmup") or {}).get("scan") or {}
    out = {}
    out["partitions a second"] = scan.get("partitions_per_sec")
    out["MiB a second"] = (scan.get("bytes_per_sec") or 0) / (1 << 20)
    pages = scan.get("pages") or 0
    if pages:
        out["walk reads a page"] = (scan.get("reads") or 0) / pages
        out["partitions a page"] = (scan.get("partitions") or 0) / pages
        out["pages shed a page"] = (scan.get("shed") or 0) / pages
    if scan.get("reads"):
        out["partitions a walk read"] = (scan.get("archived") or 0) / scan["reads"]
    if scan.get("stretches"):
        out["partitions a stretch"] = (scan.get("archived") or 0) / scan["stretches"]
    out["page read p50 ms"] = (scan.get("read") or {}).get("p50_ms")
    out["page read p99 ms"] = (scan.get("read") or {}).get("p99_ms")
    out["page held p99 ms"] = (scan.get("held") or {}).get("p99_ms")
    # the hosts' counters span warmup and measurement, so every partition of both counts
    partitions = (scan.get("partitions") or 0) + (warm.get("partitions") or 0)
    if partitions:
        reads = device_sum(run, "reads")
        out["device reads a partition"] = reads / partitions
        if reads:
            out["partitions a device read"] = partitions / reads
        out["device KiB a partition"] = device_sum(run, "read_bytes") / partitions / 1024
        out["node cpu us a partition"] = node_cpu_secs(run) / partitions * 1e6
        out["host cpu us a partition"] = busy_secs(run) / partitions * 1e6
    return out


def figures(captures):
    """Every figure of every capture, by order, which run and name, a value a round"""
    rows = {}

    def put(order, which, name, round_, value):
        if value is not None:
            rows.setdefault((order, which, name), {})[round_] = value

    for (order, round_, which), capture in captures.items():
        if which.startswith("scan-"):
            run = arm(capture, "scan/Big/none")
            if run:
                for name, value in scan_run_figures(run).items():
                    put(order, which[len("scan-"):], name, round_, value)
        elif which.startswith("cell-"):
            run = arm(capture, "rw50/b1/scan")
            if not run or not run.get("event"):
                continue
            windows = run["event"].get("windows") or {}
            before, during = windows.get("before"), windows.get("during")
            setting = which[len("cell-"):]
            for kind in ("read", "insert"):
                put(order, f"cell {setting}", f"{kind} p99 ms before", round_, p99(before, kind))
                put(order, f"cell {setting}", f"{kind} p99 ms during", round_, p99(during, kind))
                if p99(before, kind) and p99(during, kind):
                    put(order, f"cell {setting}", f"{kind} p99 during / before", round_, p99(during, kind) / p99(before, kind))
            scan = (during or {}).get("scan") or {}
            put(order, f"cell {setting}", "scan partitions a second during", round_, scan.get("partitions_per_sec"))
        elif which == "gets":
            run = arm(capture, "read100/b1/none")
            if run:
                measured = run["measured"]
                put(order, "gets", "gets a second", round_, (measured.get("read") or {}).get("per_sec"))
                put(order, "gets", "get p50 ms", round_, p50(measured, "read"))
                put(order, "gets", "get p99 ms", round_, p99(measured, "read"))
        elif which == "load":
            preload = capture.get("preload") or {}
            put(order, "load", "preload secs", round_, preload.get("secs") or preload.get("elapsed_secs"))
    return rows


def cell(values):
    """A figure's rounds as its median and range"""
    if not values:
        return "-"
    ordered = sorted(values.values())
    median = statistics.median(ordered)
    return f"{median:.3g} [{ordered[0]:.3g}, {ordered[-1]:.3g}] (n {len(ordered)})"


def disjoint(left, right):
    """Whether two figures' ranges are disjoint, the lab's rule for a difference"""
    if not left or not right:
        return ""
    a, b = sorted(left.values()), sorted(right.values())
    return "yes" if a[-1] < b[0] or b[-1] < a[0] else "no"


def ratio(left, right):
    """Two figures' medians as a ratio, right over left"""
    if not left or not right:
        return "-"
    lm, rm = statistics.median(left.values()), statistics.median(right.values())
    return f"{rm / lm:.2f}" if lm else "-"


SCAN_FIGURES = [
    "partitions a second", "MiB a second", "walk reads a page", "partitions a page", "partitions a walk read",
    "partitions a stretch", "device reads a partition", "partitions a device read", "device KiB a partition",
    "node cpu us a partition", "host cpu us a partition", "pages shed a page", "page read p50 ms",
    "page read p99 ms", "page held p99 ms",
]


def page(rows):
    """Every table: each scan figure by arm and layout, then the cell and the gets"""
    for figure in SCAN_FIGURES:
        print(f"\n### {figure}\n")
        print("| Arm | Hash order | Key order | Disjoint | Key / hash | Key's against r1 | Disjoint from r1 |")
        print("| --- | --- | --- | --- | --- | --- | --- |")
        base = rows.get(("key", "r1", figure), {})
        for name, what in ARMS:
            hashed = rows.get(("hash", name, figure), {})
            keyed = rows.get(("key", name, figure), {})
            if not hashed and not keyed:
                continue
            against = ratio(base, keyed) if name != "r1" else ""
            print(f"| {name}: {what} | {cell(hashed)} | {cell(keyed)} | {disjoint(hashed, keyed)} | {ratio(hashed, keyed)} | "
                  f"{against} | {disjoint(base, keyed) if name != 'r1' else ''} |")
    for which in ("cell r1", "cell g4k16", "gets", "load"):
        names = sorted({name for (order, w, name) in rows if w == which})
        if not names:
            continue
        print(f"\n### {which}\n")
        print("| Figure | Hash order | Key order | Disjoint |")
        print("| --- | --- | --- | --- |")
        for name in names:
            hashed = rows.get(("hash", which, name), {})
            keyed = rows.get(("key", which, name), {})
            print(f"| {name} | {cell(hashed)} | {cell(keyed)} | {disjoint(hashed, keyed)} |")


def main():
    """Print every table of the captures under a directory"""
    page(figures(load(sys.argv[1])))


if __name__ == "__main__":
    main()
