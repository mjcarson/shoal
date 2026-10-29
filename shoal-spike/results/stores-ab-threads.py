# O104's diagnosis: each node's cpu and each of its threads', from `pidstat -h -t -u 1` on europa
# (every active task each second, in UTC), over the measured windows of every run of each arm of the
# captures named. A node is a `tmdb-dataset-no` process, the driver `tmdb-dataset-lo`; a shard's
# thread is glommio's `unnamed-N`, an object executor's `shoal-objects-N`, the control thread's
# `shoal-control-N`. A task absent from a second's sample was idle in it, so every mean is over the
# window's seconds, not over the samples a task appears in.
# `python3 -I stores-ab-threads.py <pidstat.txt> <date YYYY-MM-DD> <capture dir>...`. Take the pidstat
# without -C: it filters threads by their own names too, and no thread of a node is named after it
import collections
import datetime
import json
import pathlib
import sys

samples_path, day, captures = sys.argv[1], sys.argv[2], sys.argv[3:]

# every sample: (time, process id, process name, thread id or None, thread name, %cpu, %usr, %system);
# a thread's line follows its process's
samples = []
owner = None
for line in open(samples_path, errors="replace"):
    fields = line.split()
    if len(fields) < 11 or fields[0].startswith("#") or ":" not in fields[0]:
        continue
    try:
        when = datetime.datetime.fromisoformat(f"{day}T{fields[0]}+00:00")
        cpu, usr, system = float(fields[8]), float(fields[4]), float(fields[5])
    except ValueError:
        continue
    tgid, tid, name = fields[2], fields[3], fields[10]
    if tid == "-":
        owner = (tgid, name)
        samples.append((when, tgid, name, None, name, cpu, usr, system))
    elif owner is not None:
        samples.append((when, owner[0], owner[1], tid, name.removeprefix("|__"), cpu, usr, system))
# the seconds sampled, so a window's mean counts the seconds a task was idle in
seconds = sorted({sample[0] for sample in samples})


def windows(capture):
    """Each arm's measured windows: (arm id, start, end), from its runs' starts and lengths"""
    bench = json.loads((pathlib.Path(capture) / "bench.json").read_text())
    out = []
    for arm in bench["arms"]:
        for run in arm.get("runs", []):
            start = datetime.datetime.fromisoformat(run["started_at"].replace("Z", "+00:00"))
            # a run's warm up first, then its measured window, each a length in seconds
            warmup = run.get("warmup") or 0
            measured = run.get("measured") or 0
            if isinstance(warmup, dict):
                warmup = warmup.get("secs", 0) + warmup.get("nanos", 0) / 1e9
            if isinstance(measured, dict):
                measured = measured.get("secs", 0) + measured.get("nanos", 0) / 1e9
            begin = start + datetime.timedelta(seconds=float(warmup))
            out.append((arm["id"], begin, begin + datetime.timedelta(seconds=float(measured))))
    return out


for capture in captures:
    print(f"== {pathlib.Path(capture).name}")
    # every arm's seconds and its tasks' cpu summed over them, by (process, thread)
    totals = collections.defaultdict(lambda: collections.defaultdict(float))
    users = collections.defaultdict(lambda: collections.defaultdict(float))
    systems = collections.defaultdict(lambda: collections.defaultdict(float))
    peaks = collections.defaultdict(lambda: collections.defaultdict(float))
    names = {}
    span = collections.defaultdict(int)
    for arm, begin, end in windows(capture):
        span[arm] += sum(1 for second in seconds if begin <= second < end)
        for when, pid, process, tid, thread, cpu, usr, system in samples:
            if not (begin <= when < end) or not process.startswith("tmdb-dataset"):
                continue
            key = (pid, tid)
            totals[arm][key] += cpu
            users[arm][key] += usr
            systems[arm][key] += system
            peaks[arm][key] = max(peaks[arm][key], cpu)
            names[key] = (process, thread)
    for arm in sorted(totals):
        print(f"  {arm}: {span[arm]} seconds measured")
        if not span[arm]:
            continue
        # the nodes in the order they started, the driver after them
        pids = sorted({pid for pid, _ in totals[arm]}, key=lambda pid: (names[(pid, None)][0] != "tmdb-dataset-no", int(pid)))
        for rank, pid in enumerate(pids):
            process = names[(pid, None)][0]
            who = f"node {rank + 1}" if process == "tmdb-dataset-no" else "driver"
            print(f"    {who}: every thread {totals[arm][(pid, None)] / span[arm]:6.1f}% mean, {peaks[arm][(pid, None)]:6.1f}% peak")
            threads = [key for key in totals[arm] if key[0] == pid and key[1] is not None]
            threads.sort(key=lambda key: -totals[arm][key])
            for key in threads:
                mean = totals[arm][key] / span[arm]
                if mean >= 2 or names[key][1].startswith(("shoal-objects", "unnamed")):
                    split = f"{users[arm][key] / span[arm]:5.1f}% user, {systems[arm][key] / span[arm]:5.1f}% system"
                    print(f"      {names[key][1]:<18} {mean:6.1f}% mean ({split}), {peaks[arm][key]:6.1f}% peak (tid {key[1]})")
