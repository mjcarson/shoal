# The cpu-seconds each GiB an arm moved cost each node's threads, two sides of a loop leg's A/B
# beside each other (F103's plaintext lanes, docs/src/features/plaintext-peer-lanes.md): the nodes'
# every thread, their object executors (`shoal-objects-N`) and their shards (glommio's `unnamed-N`),
# from `pidstat -h -t -u 1` on europa over each run's measured window, divided by the GiB the run
# moved, its rate from the slim capture and its object size from the arm. Each figure is the median
# of a side's runs, the lowest and highest beside it, with the ratio of the medians.
# `python3 -I stores-ab-cpu.py <pidstat.txt> <date YYYY-MM-DD> <slim dir> <raw captures dir> <tag> <side> <side>`
import collections
import datetime
import json
import pathlib
import statistics
import sys

samples_path, day, slim_dir, raw_dir, tag, *sides = sys.argv[1:]

# every sample of a node or the driver: (time, pid, process, tid, thread, cpu)
samples = []
owner = None
for line in open(samples_path, errors="replace"):
    fields = line.split()
    if len(fields) < 11 or fields[0].startswith("#") or ":" not in fields[0]:
        continue
    try:
        when = datetime.datetime.fromisoformat(f"{day}T{fields[0]}+00:00")
        cpu = float(fields[8])
    except ValueError:
        continue
    tgid, tid, name = fields[2], fields[3], fields[10]
    if tid == "-":
        owner = (tgid, name)
        if name.startswith("tmdb-dataset"):
            samples.append((when, tgid, name, None, name, cpu))
    elif owner is not None and owner[1].startswith("tmdb-dataset"):
        samples.append((when, owner[0], owner[1], tid, name.removeprefix("|__"), cpu))
seconds = sorted({sample[0] for sample in samples})
by_second = collections.defaultdict(list)
for sample in samples:
    by_second[sample[0]].append(sample)


def kind_of(process, tid, thread):
    """What a sample counts as"""
    if not process.startswith("tmdb-dataset-no"):
        return "driver" if tid is None else None
    if tid is None:
        return "nodes"
    if thread.startswith("shoal-objects"):
        return "objects"
    if thread.startswith("unnamed"):
        return "shards"
    return None


def size_of(arm):
    """An arm's object size in GiB"""
    size = arm.split("/")[1][1:]
    unit = {"k": 1 << 10, "m": 1 << 20}[size[-1]]
    return int(size[:-1]) * unit / (1 << 30)


# per side, arm and kind: each run's cpu-seconds a GiB, and each run's rate
figures = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(list)))
for side in sides:
    for raw in sorted(pathlib.Path(raw_dir).glob(f"{tag}-{side}-*")):
        slim = json.loads((pathlib.Path(slim_dir) / f"{raw.name}.json").read_text())
        rates = {arm["id"]: [sum(k["per_sec"] for k in run["measured"]["kinds"].values()) for run in arm["runs"]] for arm in slim["arms"]}
        bench = json.loads((raw / "bench.json").read_text())
        for arm in bench["arms"]:
            for index, run in enumerate(arm.get("runs", [])):
                start = datetime.datetime.fromisoformat(run["started_at"].replace("Z", "+00:00"))
                warmup, measured = run.get("warmup") or 0, run.get("measured") or 0
                if isinstance(warmup, dict):
                    warmup = warmup.get("secs", 0) + warmup.get("nanos", 0) / 1e9
                if isinstance(measured, dict):
                    measured = measured.get("secs", 0) + measured.get("nanos", 0) / 1e9
                begin = start + datetime.timedelta(seconds=float(warmup))
                end = begin + datetime.timedelta(seconds=float(measured))
                window = [second for second in seconds if begin <= second < end]
                if not window:
                    continue
                sums = collections.defaultdict(float)
                for second in window:
                    for when, pid, process, tid, thread, cpu in by_second[second]:
                        kind = kind_of(process, tid, thread)
                        if kind:
                            sums[kind] += cpu
                rate = rates[arm["id"]][index]
                gib_a_second = rate * size_of(arm["id"])
                figures[side][arm["id"]]["rate"].append(rate)
                for kind in ("nodes", "objects", "shards", "driver"):
                    figures[side][arm["id"]][kind].append(sums[kind] / len(window) / 100 / gib_a_second)
                    figures[side][arm["id"]][kind + "%"].append(sums[kind] / len(window))

first, second = sides
print(f"## {tag}: cpu-seconds a GiB moved, {second} beside {first}, median [lowest-highest] of every run\n")
print(f"| arm | nodes {first} | nodes {second} | ÷ | objects {first} | objects {second} | ÷ | shards {first} | shards {second} | ÷ | nodes' cpu % {first} → {second} |")
print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")


def show(values):
    return f"{statistics.median(values):.2f} [{min(values):.2f}–{max(values):.2f}]"


for arm in sorted(figures[first]):
    a, b = figures[first][arm], figures[second][arm]
    cells = []
    for kind in ("nodes", "objects", "shards"):
        cells += [show(a[kind]), show(b[kind]), f"{statistics.median(b[kind]) / statistics.median(a[kind]):.2f}"]
    cpu = f"{statistics.median(a['nodes%']):.0f} → {statistics.median(b['nodes%']):.0f}"
    print(f"| `{arm}` | " + " | ".join(cells) + f" | {cpu} |")
