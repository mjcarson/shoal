"""X18's tables from its captures: the cell beside a scan at every pace, on the SSDs and the disks

    python3 -I shoal-spike-scan/results/x18-report.py shoal-spike-scan/results/x18 > x18-report.md

Every capture is `x18-<layout>-r<round>-<which>/bench.json`, which `x18-lab.sh` names, or the same
slimmed into `x18-<layout>-r<round>-<which>.json` by its `fetch`; `<layout>` is `ssd` or `disk`. For
each layout and pace this prints the cell's read and insert p99 before and during a scan of Big and
their ratio against W7's budget, and the scan's own rate and pacing; and from a scan alone at the
widest pace, the device time a MiB the walk costs, which states each ceiling in device time. Every
figure is each round's, then its median and range over the rounds.
"""

import json
import pathlib
import re
import statistics
import sys

# a capture's name: its layout, its round and which run it is
NAME = re.compile(r"^x18-(ssd|disk)-r(\d+)-([a-z0-9-]+?)(\.json)?$")

# every pace in the order the page prints it: its name, ceiling and trickle in MiB a second
PACES = [
    ("c32t2", 32, 2), ("c64t2", 64, 2), ("c128t2", 128, 2), ("c256t2", 256, 2),
    ("c64t64", 64, 64), ("c256t256", 256, 256), ("c128t1", 128, 1), ("c128t4", 128, 4),
]


def load(root):
    """Every capture under a directory, whole or slimmed, by layout, round and run"""
    captures = {}
    for path in sorted(pathlib.Path(root).iterdir()):
        match = NAME.match(path.name)
        if not match:
            continue
        file = path if path.is_file() else path / "bench.json"
        if not file.exists():
            continue
        with open(file, encoding="utf-8") as handle:
            captures[(match.group(1), int(match.group(2)), match.group(3))] = json.load(handle)
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


def budget(before, during):
    """Whether a p99 during a scan is inside W7's budget of its before, max(1.10 x, + 0.1 ms)"""
    return during <= max(1.10 * before, before + 0.1)


def figures(captures):
    """Every figure of every capture, by layout, pace and name, a value a round"""
    rows = {}

    def put(layout, pace, name, round_, value):
        if value is not None:
            rows.setdefault((layout, pace, name), {})[round_] = value

    for (layout, round_, which), capture in captures.items():
        if which.startswith("cell-"):
            pace = which[len("cell-"):]
            run = arm(capture, "rw50/b1/scan")
            if not run or not run.get("event"):
                continue
            windows = run["event"].get("windows") or {}
            before, during = windows.get("before"), windows.get("during")
            for kind in ("read", "insert"):
                put(layout, pace, f"cell {kind} p99 ms before", round_, p99(before, kind))
                put(layout, pace, f"cell {kind} p99 ms during", round_, p99(during, kind))
                if p99(before, kind) and p99(during, kind):
                    put(layout, pace, f"cell {kind} p99 during / before", round_, p99(during, kind) / p99(before, kind))
            scan = (during or {}).get("scan") or {}
            put(layout, pace, "scan MiB a second during", round_, (scan.get("bytes_per_sec") or 0) / (1 << 20))
            put(layout, pace, "scan partitions a second during", round_, scan.get("partitions_per_sec"))
            put(layout, pace, "page paced p99 ms during", round_, (scan.get("paced") or {}).get("p99_ms"))
            put(layout, pace, "page read p99 ms during", round_, (scan.get("read") or {}).get("p99_ms"))
        elif which == "scan-widest":
            run = arm(capture, "scan/Big/none")
            if not run:
                continue
            scan = run["measured"].get("scan") or {}
            warm = (run.get("warmup") or {}).get("scan") or {}
            mib = ((scan.get("bytes") or 0) + (warm.get("bytes") or 0)) / (1 << 20)
            busy = sum(device.get("busy_ms", 0) for host in run.get("devices") or [] for device in host.get("devices", []))
            reads = sum(device.get("reads", 0) for host in run.get("devices") or [] for device in host.get("devices", []))
            put(layout, "widest", "scan MiB a second alone", round_, (scan.get("bytes_per_sec") or 0) / (1 << 20))
            if mib:
                put(layout, "widest", "device ms a MiB scanned", round_, busy / mib)
                put(layout, "widest", "device reads a MiB scanned", round_, reads / mib)
    return rows


def cell(values):
    """A figure's rounds as its median and range"""
    if not values:
        return "-"
    ordered = sorted(values.values())
    return f"{statistics.median(ordered):.3g} [{ordered[0]:.3g}, {ordered[-1]:.3g}] (n {len(ordered)})"


def median(values):
    """A figure's median, or none"""
    return statistics.median(values.values()) if values else None


def page(rows):
    """Every table: the cell and the scan at every pace, a layout a column, and the budget judged"""
    names = [
        "cell read p99 ms before", "cell read p99 ms during", "cell read p99 during / before",
        "cell insert p99 ms before", "cell insert p99 ms during", "cell insert p99 during / before",
        "scan MiB a second during", "scan partitions a second during", "page paced p99 ms during",
        "page read p99 ms during",
    ]
    for pace, ceiling, trickle in PACES:
        print(f"\n### {pace}: a ceiling of {ceiling} MiB a second, a trickle of {trickle}\n")
        print("| Figure | SSD | Disk |")
        print("| --- | --- | --- |")
        for name in names:
            ssd, disk = rows.get(("ssd", pace, name), {}), rows.get(("disk", pace, name), {})
            if ssd or disk:
                print(f"| {name} | {cell(ssd)} | {cell(disk)} |")
    print("\n### A scan alone at the widest pace\n")
    print("| Figure | SSD | Disk |")
    print("| --- | --- | --- |")
    for name in ("scan MiB a second alone", "device ms a MiB scanned", "device reads a MiB scanned"):
        print(f"| {name} | {cell(rows.get(('ssd', 'widest', name), {}))} | {cell(rows.get(('disk', 'widest', name), {}))} |")
    print("\n### W7's budget on the medians, and each ceiling in device time\n")
    print("| Pace | Layout | Read | Insert | Ceiling in device ms a second |")
    print("| --- | --- | --- | --- | --- |")
    for pace, ceiling, _ in PACES:
        for layout in ("ssd", "disk"):
            verdicts = []
            for kind in ("read", "insert"):
                b = median(rows.get((layout, pace, f"cell {kind} p99 ms before"), {}))
                d = median(rows.get((layout, pace, f"cell {kind} p99 ms during"), {}))
                verdicts.append("-" if b is None or d is None else f"{b:.2f} -> {d:.2f} ms, {'inside' if budget(b, d) else 'outside'}")
            ms = median(rows.get((layout, "widest", "device ms a MiB scanned"), {}))
            device = "-" if ms is None else f"{ceiling * ms:.0f}"
            print(f"| {pace} | {layout} | {verdicts[0]} | {verdicts[1]} | {device} |")


def main():
    """Print every table of the captures under a directory"""
    page(figures(load(sys.argv[1])))


if __name__ == "__main__":
    main()
