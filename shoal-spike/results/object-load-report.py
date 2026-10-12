#!/usr/bin/env python3
"""F85's report: the reference cell's latency beside the device store's own load, merged and judged.

Reads a directory of what object-load-host.sh wrote - `<leg>-alone-r<n>.json`,
`<leg>-idle-r<n>.json` and `<leg>-load-<rate>-r<n>.json`, each a shoal-workload capture, and beside
every loaded one its `.load.json` report - and prints markdown: the cell's read and write latency by
side, each the median of the rounds with the lowest and highest in brackets, its ratio to the cell
alone, and whether the intervals are disjoint; the verdict on the null leg; and what the load did
in the measured window.

    python3 shoal-spike/results/object-load-report.py shoal-spike/results/object-load/raw > shoal-spike/results/object-load-report.md

The line is S15's, set before the rounds (`docs/src/object-storage/performance.md`): on the null
leg, a side fails when the cell's read or write p99 has a median above 1.25 times the cell alone's,
and its lowest round is above the cell alone's highest. The evo leg is reported beside it,
labelled as sharing the tables' disk, and not judged by the line.
"""
import glob
import json
import os
import re
import statistics
import sys

RAW = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "object-load", "raw")
NAME = re.compile(r"^(null|evo)-(alone|idle|load)(?:-(\d+))?-r(\d+)\.json$")
LINE = 1.25
LEGS = ["null", "evo"]
ARMS = ["alone", "idle", "load"]
ARM_NAMES = {
    "alone": "alone",
    "idle": "beside an idle object executor",
    "load": "beside the store's load",
}


def us(duration):
    """A serde Duration as microseconds"""
    return (duration["secs"] * 1_000_000_000 + duration["nanos"]) / 1000.0


def cell(path):
    """The figures of one capture: each operation's p50 and p99, and operations a second"""
    data = json.load(open(path))
    ((_, workload),) = data["workloads"].items()
    wall = workload["wall_clock_ns"][0] / 1e9
    counters = workload.get("counters", {})
    figures = {"ops/s": (counters.get("reads", 0) + counters.get("writes", 0)) / wall}
    for op in ("read", "write"):
        stats = workload["ops"][op]
        figures[f"{op} p50"] = us(stats["p50"])
        figures[f"{op} p99"] = us(stats["p99"])
    return figures


def load(path):
    """What the load did in the measured window"""
    if not os.path.exists(path):
        return None
    report = json.load(open(path))
    window = report["measured"]
    return {
        "asked": report["rate_mib"],
        "rate": window["mib_per_sec"],
        "chunks": window["chunks"],
        "stage p50": window["stage_us"]["p50"],
        "stage p99": window["stage_us"]["p99"],
        "apply p50": window["apply_us"]["p50"],
        "apply p99": window["apply_us"]["p99"],
        "behind max": window["behind_ms_max"],
        "refused": sum(window["refused"].values()),
        "served_on": report["served_on"],
        "driver_cpu": report.get("driver_cpu"),
    }


def median_range(values, digits=0):
    """The median with the lowest and highest in brackets"""
    if not values:
        return "-"
    fmt = f"{{:,.{digits}f}}"
    return f"{fmt.format(statistics.median(values))} [{fmt.format(min(values))}–{fmt.format(max(values))}]"


# every run, by leg, arm and rate
runs = {}
for path in sorted(glob.glob(os.path.join(RAW, "*.json"))):
    match = NAME.match(os.path.basename(path))
    if not match:
        continue
    leg, arm, rate, rnd = match.groups()
    side = (leg, arm, int(rate or 0))
    figures = cell(path)
    figures["load"] = load(path[: -len(".json")] + ".load.json") if arm == "load" else None
    runs.setdefault(side, {})[int(rnd)] = figures


def sides_of(leg):
    """A leg's sides, the cell alone first"""
    keys = [side for side in runs if side[0] == leg]
    return sorted(keys, key=lambda side: (ARMS.index(side[1]), side[2]))


def values(side, figure):
    """A figure over the rounds of one side"""
    return [figures[figure] for _, figures in sorted(runs[side].items())]


def load_values(side, figure):
    """A figure of the load's over the rounds of one side"""
    return [figures["load"][figure] for _, figures in sorted(runs[side].items()) if figures["load"]]


def judge(side, alone, figure):
    """The median ratio of a figure to the cell alone's, and whether the intervals are disjoint"""
    mine, base = values(side, figure), values(alone, figure)
    ratio = statistics.median(mine) / statistics.median(base)
    return ratio, min(mine) > max(base), max(mine) < min(base)


def verdict(ratio, above, below):
    """A ratio with what the intervals say of it"""
    if above:
        return f"**{ratio:.2f}×**, above" if ratio > LINE else f"{ratio:.2f}×, above"
    if below:
        return f"{ratio:.2f}×, below"
    return f"{ratio:.2f}×, within"


rounds = sorted({rnd for side in runs.values() for rnd in side})
if not rounds:
    sys.exit(f"no runs under {RAW}")
print("# F85 object load report")
print()
print(f"Rounds {rounds[0]} to {rounds[-1]}, {len(rounds)} in all, from `{RAW}`. Every figure is the median of")
print("the rounds with the lowest and highest in brackets, in microseconds unless it says otherwise. A")
print("ratio is the side's median over the cell alone's; *above* and *below* mean the two sides' run")
print(f"intervals are disjoint, *within* that they overlap. S15's line is {LINE}×, judged on the null leg.")
print()

for leg in LEGS:
    sides = sides_of(leg)
    alone = (leg, "alone", 0)
    if not sides or alone not in runs:
        continue
    shared = " (the device shares the tables' 970 EVO)" if leg == "evo" else ""
    print(f"## The {leg} leg{shared}: the cell")
    print()
    print("| Side | Rate MiB/s | Read p50 | Read p99 | Read p99 ÷ alone | Write p50 | Write p99 | Write p99 ÷ alone | Ops/s |")
    print("| --- | ---: | ---: | ---: | --- | ---: | ---: | --- | ---: |")
    for side in sides:
        _, arm, rate = side
        read = "-" if side == alone else verdict(*judge(side, alone, "read p99"))
        write = "-" if side == alone else verdict(*judge(side, alone, "write p99"))
        print(
            f"| {ARM_NAMES[arm]} | {rate if arm == 'load' else '-'} "
            f"| {median_range(values(side, 'read p50'))} | {median_range(values(side, 'read p99'))} | {read} "
            f"| {median_range(values(side, 'write p50'))} | {median_range(values(side, 'write p99'))} | {write} "
            f"| {median_range(values(side, 'ops/s'))} |"
        )
    print()
    loaded = [side for side in sides if side[1] == "load"]
    if loaded:
        print(f"## The {leg} leg: the load in the measured window")
        print()
        print("| Rate asked | MiB/s reached | Chunks | Stage p50 | Stage p99 | Apply p50 | Apply p99 | Behind max ms | Refused | Served on | Driver on |")
        print("| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |")
        for side in loaded:
            first = next(figures["load"] for _, figures in sorted(runs[side].items()) if figures["load"])
            print(
                f"| {side[2]} | {median_range(load_values(side, 'rate'), 1)} | {median_range(load_values(side, 'chunks'))} "
                f"| {median_range(load_values(side, 'stage p50'))} | {median_range(load_values(side, 'stage p99'))} "
                f"| {median_range(load_values(side, 'apply p50'))} | {median_range(load_values(side, 'apply p99'))} "
                f"| {median_range(load_values(side, 'behind max'), 1)} | {sum(load_values(side, 'refused'))} "
                f"| {first['served_on']} | {first['driver_cpu']} |"
            )
        print()

# the verdict, on the null leg
alone = ("null", "alone", 0)
if alone in runs:
    print("## The verdict on the null leg")
    print()
    print("| Side | Rate MiB/s | Fails on |")
    print("| --- | ---: | --- |")
    for side in sides_of("null"):
        if side == alone:
            continue
        failed = [
            op
            for op in ("read", "write")
            if (lambda r, a, _b: r > LINE and a)(*judge(side, alone, f"{op} p99"))
        ]
        print(f"| {ARM_NAMES[side[1]]} | {side[2] if side[1] == 'load' else '-'} | {', '.join(failed) if failed else 'nothing: held'} |")
    print()
