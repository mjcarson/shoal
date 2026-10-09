#!/usr/bin/env python3
"""X9's report: the reference cell's latency beside object work, merged over rounds and judged.

Reads a directory of what x9-host.sh wrote - `<leg>-alone-r<n>.json` and
`<leg>-<arm>-<rate>-<unit>-r<n>.json`, each a shoal-workload capture, and beside every arm's its
`.x9.json` task report - and prints markdown: the cell's read and write latency by side, each as
the median of the rounds with the lowest and highest in brackets, its ratio to the cell alone, and
whether the intervals are disjoint; the trigger on the null leg; and what the object work did in
the measured window.

    python3 shoal-spike/results/x9-report.py shoal-spike/results/x9/raw > shoal-spike/results/x9-report.md

T1, set before the rounds (`docs/src/object-storage/table-latency.md#how-it-was-judged`): on the
null leg, a shared arm fires at a rate and unit when the cell's read or write p99 has a median
above 1.25 times the cell alone's, and its lowest round is above the cell alone's highest. The
decision follows both planned shared arms: dedicated executors are required at a rate and unit
only if `shards` and `shards-lat` both fire there; if only `shards` fires, sharing is allowed with
a latency goal on the object queue. `shards-fine` is the supplement added after the quick run,
reported beside them and judged by the same line, and it does not move T1's verdict.
"""
import glob
import json
import os
import re
import statistics
import sys

RAW = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "x9", "raw")
NAME = re.compile(r"^(null|evo)-(alone|shards|shards-lat|shards-fine|core)(?:-(\d+)-(\d+))?-r(\d+)\.json$")
LINE = 1.25
LEGS = ["null", "evo"]
ARMS = ["alone", "shards", "shards-lat", "shards-fine", "core"]
ARM_NAMES = {
    "alone": "alone",
    "shards": "shards (NotImportant)",
    "shards-lat": "shards-lat (250 µs goal)",
    "shards-fine": "shards-fine (64 KiB steps, 100 µs goal; supplement)",
    "core": "core of its own",
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


def task(path):
    """What the object work did in the measured window, over every runner"""
    if not os.path.exists(path):
        return None
    report = json.load(open(path))
    runners = report["runners"]
    measured = [runner["measured"] for runner in runners if runner.get("measured")]
    if not measured:
        return None
    asked = sum(runner["rate_mib"] for runner in runners)
    return {
        "asked": asked,
        "rate": sum(window["data_mib_per_sec"] for window in measured),
        "hold p50": max(window["hold"]["p50_us"] for window in measured),
        "hold p99": max(window["hold"]["p99_us"] for window in measured),
        "hold max": max(window["hold"]["max_us"] for window in measured),
        "receive p99": max(window["receive"]["p99_us"] for window in measured),
        "checksum p99": max(window["checksum"]["p99_us"] for window in measured),
        "encode p99": max(window["encode"]["p99_us"] for window in measured),
        "sync p99": max(window["sync"]["p99_us"] for window in measured),
        "taken": sum(window["yields_taken"] for window in measured),
        "offered": sum(window["yields_offered"] for window in measured),
        "dropped ms": sum(window["dropped_ms"] for window in measured),
        "late": sum(window["late"] for window in measured),
        "stripes": sum(window["stripes"] for window in measured),
        "began before": all(runner.get("began_before_measured") for runner in runners),
        "cpus": sorted(runner["cpu"] for runner in runners if runner["cpu"] is not None),
        "shard_cpus": report["shard_cpus"],
        "kernels": report["kernels"],
        "crc": report["crc_target"],
    }


def median_range(values, digits=0):
    """The median with the lowest and highest in brackets"""
    if not values:
        return "-"
    fmt = f"{{:,.{digits}f}}"
    return f"{fmt.format(statistics.median(values))} [{fmt.format(min(values))}–{fmt.format(max(values))}]"


def unit_name(unit):
    """A unit in KiB or MiB"""
    unit = int(unit)
    return f"{unit >> 20} MiB" if unit >= 1 << 20 else f"{unit >> 10} KiB"


# every run, by leg, arm, rate and unit
runs = {}
for path in sorted(glob.glob(os.path.join(RAW, "*.json"))):
    match = NAME.match(os.path.basename(path))
    if not match:
        continue
    leg, arm, rate, unit, rnd = match.groups()
    side = (leg, arm, int(rate or 0), int(unit or 0))
    figures = cell(path)
    figures["task"] = task(path[: -len(".json")] + ".x9.json") if arm != "alone" else None
    runs.setdefault(side, {})[int(rnd)] = figures


def sides_of(leg):
    """A leg's sides, the cell alone first"""
    keys = [side for side in runs if side[0] == leg]
    return sorted(keys, key=lambda side: (ARMS.index(side[1]), side[2], side[3]))


def values(side, figure):
    """A figure over the rounds of one side"""
    return [figures[figure] for _, figures in sorted(runs[side].items())]


def judge(side, alone, figure):
    """The median ratio of a figure to the cell alone's, and whether it fires the line"""
    mine, base = values(side, figure), values(alone, figure)
    ratio = statistics.median(mine) / statistics.median(base)
    disjoint_above = min(mine) > max(base)
    disjoint_below = max(mine) < min(base)
    return ratio, disjoint_above, disjoint_below


def verdict(ratio, above, below):
    """A ratio with what the intervals say of it"""
    if above:
        return f"**{ratio:.2f}×**, above" if ratio > LINE else f"{ratio:.2f}×, above"
    if below:
        return f"{ratio:.2f}×, below"
    return f"{ratio:.2f}×, within"


rounds = sorted({rnd for side in runs.values() for rnd in side})
print("# X9 report")
print()
print(f"Rounds {rounds[0]} to {rounds[-1]}, {len(rounds)} in all, from `{RAW}`. Every figure is the median of")
print("the rounds with the lowest and highest in brackets, in microseconds unless it says otherwise. A")
print("ratio is the side's median over the cell alone's; *above* and *below* mean the two sides' run")
print("intervals are disjoint, *within* that they overlap. T1's line is 1.25×.")
print()

for leg in LEGS:
    sides = sides_of(leg)
    alone = (leg, "alone", 0, 0)
    if not sides or alone not in runs:
        continue
    print(f"## The {leg} leg: the cell")
    print()
    print("| Side | Rate MiB/s | Unit | Read p50 | Read p99 | Read p99 ÷ alone | Write p50 | Write p99 | Write p99 ÷ alone | Ops/s |")
    print("| --- | ---: | --- | ---: | ---: | --- | ---: | ---: | --- | ---: |")
    for side in sides:
        _, arm, rate, unit = side
        read = "-" if side == alone else verdict(*judge(side, alone, "read p99"))
        write = "-" if side == alone else verdict(*judge(side, alone, "write p99"))
        print(
            f"| {ARM_NAMES[arm]} | {rate or '-'} | {unit_name(unit) if unit else '-'} "
            f"| {median_range(values(side, 'read p50'))} | {median_range(values(side, 'read p99'))} | {read} "
            f"| {median_range(values(side, 'write p50'))} | {median_range(values(side, 'write p99'))} | {write} "
            f"| {median_range(values(side, 'ops/s'))} |"
        )
    print()

# the trigger, on the null leg
alone = ("null", "alone", 0, 0)
if alone in runs:
    print("## T1 on the null leg")
    print()
    print("| Rate MiB/s | Unit | `shards` fires on | `shards-lat` fires on | Decision | Supplement: `shards-fine` fires on |")
    print("| ---: | --- | --- | --- | --- | --- |")
    cells = sorted({(side[2], side[3]) for side in runs if side[0] == "null" and side[1] != "alone"})
    for rate, unit in cells:
        fired = {}
        for arm in ("shards", "shards-lat", "shards-fine"):
            side = ("null", arm, rate, unit)
            if side not in runs:
                fired[arm] = None
                continue
            fired[arm] = [
                op
                for op in ("read", "write")
                if (lambda r, a, _b: r > LINE and a)(*judge(side, alone, f"{op} p99"))
            ]
        plain, goal = fired["shards"], fired["shards-lat"]
        if plain is None or goal is None:
            decision = "incomplete"
        elif plain and goal:
            decision = "**dedicated executors required**"
        elif plain:
            decision = "sharing allowed with a latency goal on the object queue"
        elif goal:
            decision = "sharing allowed; the latency goal costs the cell here"
        else:
            decision = "sharing allowed"
        show = lambda ops: "-" if ops is None else (", ".join(ops) if ops else "neither")
        print(f"| {rate} | {unit_name(unit)} | {show(plain)} | {show(goal)} | {decision} | {show(fired['shards-fine'])} |")
    print()

# what the object work did
for leg in LEGS:
    sides = [side for side in sides_of(leg) if side[1] != "alone"]
    if not sides:
        continue
    print(f"## The {leg} leg: the object work in the measured window")
    print()
    print("Holds and steps are the largest of the runners' p99s; a hold is the time the work kept its")
    print("executor between two real suspensions. Yields are those taken of those offered, summed over")
    print("the rounds.")
    print()
    print("| Side | Rate | Unit | MiB/s done | Hold p50 | Hold p99 | Hold max | Receive p99 | Checksum p99 | Encode p99 | Sync p99 | Yields taken | Dropped ms |")
    print("| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for side in sides:
        _, arm, rate, unit = side
        tasks = [figures["task"] for _, figures in sorted(runs[side].items()) if figures["task"]]
        if not tasks:
            print(f"| {ARM_NAMES[arm]} | {rate} | {unit_name(unit)} | no report | | | | | | | | | |")
            continue
        pick = lambda key: [t[key] for t in tasks]
        taken, offered = sum(pick("taken")), sum(pick("offered"))
        print(
            f"| {ARM_NAMES[arm]} | {rate} | {unit_name(unit)} | {median_range(pick('rate'))} "
            f"| {median_range(pick('hold p50'))} | {median_range(pick('hold p99'))} | {median_range(pick('hold max'))} "
            f"| {median_range(pick('receive p99'))} | {median_range(pick('checksum p99'))} | {median_range(pick('encode p99'))} "
            f"| {median_range(pick('sync p99'))} | {taken:,} of {offered:,} | {median_range(pick('dropped ms'), 1)} |"
        )
    print()

# what makes a run count
print("## Validity")
print()
short, late_start, layouts, kernels = [], [], set(), set()
for side, by_round in runs.items():
    for rnd, figures in by_round.items():
        t = figures["task"]
        if not t:
            continue
        if t["rate"] < 0.98 * t["asked"]:
            short.append(f"{'-'.join(map(str, side))} r{rnd}: {t['rate']:.0f} of {t['asked']:.0f} MiB/s")
        if not t["began before"]:
            late_start.append(f"{'-'.join(map(str, side))} r{rnd}")
        layouts.add((tuple(t["shard_cpus"]), side[1], tuple(t["cpus"])))
        kernels.add((t["kernels"], t["crc"]))
print(f"- Runs under 98% of their rate in the measured window: {len(short)}" + (": " + "; ".join(short) if short else ""))
print(f"- Runners whose schedule began after the measured phase did: {len(late_start)}" + (": " + "; ".join(late_start) if late_start else ""))
print("- Shard cpus, and the cpus the runners ran on, by arm: " + "; ".join(
    f"{arm}: shards {list(shards)}, runners {list(cpus)}" for shards, arm, cpus in sorted(layouts)))
print("- Kernels and CRC target: " + "; ".join(f"{k}, {c}" for k, c in sorted(kernels)))
