# A before and after of Shoal on the lab, read from its slimmed captures (stores-lab.sh `ab-fetch`,
# docs/src/object-storage/store-optimizations.md): each arm's operations a second on each side as the
# median of every run of every capture of that side, with the lowest and highest beside it, the
# ratio of the medians, and whether the two sides' intervals are disjoint; each side's p50 and p99;
# and the device bytes written a byte sent and read a byte received, every host's store devices
# summed over each run. Run with python3 -I <this> shoal-spike/results/stores-ab/<tag> [first second]
# [--beside shoal-spike/results/stores/<leg> <store>,...], which adds each side's verdict beside the
# comparison's captures of those stores by the comparison's own rule (store-comparison.md, how to read
# the charts): taken on another day than the sides, so a guide to where a gap went, never a verdict.
# --latency adds each arm's p50, p90, mean and p99 on each side, the p99 as the median of every run with
# the lowest and highest beside it, and whether the two sides' p99 runs are disjoint. --rounds adds what
# each side's strong reads waited for, read from the members' replication reports (F99): the barriers,
# their mean and quantiles, the empty appends sent a barrier, and the quantiles of the appends' answers
# as their leaders waited for them and as their followers gave them, every member and run summed.
# --memory adds each side's memory beside its load (F104): the highest resident memory any member
# reached in a run, the partitions its members' shards evicted during the run by what began each pass
# (their rows past their own budget, or the process past the node's memory), the most bytes their caches
# of read units held and the share of their reads the caches answered, and the paced stream's rate and
# latencies, each the median of a side's runs with the lowest and highest beside it. --objects adds
# each side's object work beside its load (F108): the most requests its members' lanes had waiting,
# answers their served lanes owed, object tasks their shards ran and bytes drawn from their budgets in
# any sample, and the requests their lanes refused, the draws their budgets shed, the tasks their shards
# refused and the puts they gave up during the run. It reads every run, those that measured nothing
# too, since a run that stalled is the one whose work in flight says where
import json
import math
import os
import statistics
import sys


def side_of(name, tag):
    """The side a capture is of, from its file's name: <tag>-<side>-<n>.json"""
    rest = name[len(tag) + 1:-len(".json")]
    side, _, number = rest.rpartition("-")
    return side, int(number)


def run_figures(run):
    """One run's operations a second, its latencies by kind (p50, p99, mean, p90), and its device and driver bytes"""
    kinds = run["measured"]["kinds"]
    ops = sum(kind["per_sec"] for kind in kinds.values())
    latency = {
        name: (
            kind["latency"]["p50_ms"],
            kind["latency"]["p99_ms"],
            kind["latency"]["mean_ms"],
            kind["latency"]["p90_ms"],
        )
        for name, kind in kinds.items()
        if kind["ok"]
    }
    written = sum(device["written_bytes"] for host in run.get("devices") or [] for device in host["devices"])
    read = sum(device["read_bytes"] for host in run.get("devices") or [] for device in host["devices"])
    totals = run.get("totals") or {}
    return ops, latency, written, read, totals.get("sent", 0), totals.get("received", 0)


def interval(values):
    """A list's median, lowest and highest"""
    return statistics.median(values), min(values), max(values)


def fmt(value):
    """A figure to three significant places"""
    if value >= 100:
        return f"{value:,.0f}"
    if value >= 10:
        return f"{value:.1f}"
    return f"{value:.2f}"


def rates_of(capture):
    """Each arm's operations a second, a figure a run"""
    return {arm["id"]: [run_figures(run)[0] for run in arm["runs"]] for arm in capture["arms"]}


def judged(shoal, other):
    """Shoal's runs beside another store's, as the comparison's verdict tables say it
    (shoal-bench/src/render/stores/verdict.rs): ahead, within or behind by at least the gap between
    the nearer ends as a share of the other store's end, rounded down, or once one end is twice the
    other or more, the factor between them"""
    # Shoal's fastest run slower than the other's slowest is behind, its slowest faster ahead
    if max(shoal) < min(other):
        word, gap, factor = "behind", (min(other) - max(shoal)) / min(other) * 100, min(other) / max(shoal)
    elif min(shoal) > max(other):
        word, gap, factor = "ahead", (min(shoal) - max(other)) / max(other) * 100, min(shoal) / max(other)
    else:
        return "within"
    # a factor of two or more says more than a share; every figure is a lower bound, so rounded down
    if factor >= 2:
        return f"{word} ≥{math.floor(factor * 10) / 10:.1f}×"
    if gap >= 10:
        return f"{word} ≥{math.floor(gap):.0f}%"
    return f"{word} ≥{math.floor(gap * 10) / 10:.1f}%"


def print_latencies(arms, first, second):
    """Each arm's p50, p90 and mean on each side as the median of its runs, and its p99 as the median
    of its runs with the lowest and highest beside it, judged disjoint or within as the rates are"""
    print(f"\n### Latencies, ms: p99 as median [lowest–highest] of every run\n")
    print(
        f"| arm | p50 {first} → {second} | p90 {first} → {second} | mean {first} → {second} | "
        f"p99 {first} | p99 {second} | p99 |"
    )
    print("| --- | --- | --- | --- | --- | --- | --- |")
    for arm_id in sorted(arms, key=lambda arm: (arm.split("/")[0], arm)):
        by_side = arms[arm_id]
        if first not in by_side or second not in by_side:
            continue
        # every run's figure of the arm's first kind with any operations, on one side
        def runs(side, at):
            return [figures[1][kind][at] for figures in by_side[side] for kind in sorted(figures[1])[:1]]
        # the two sides' p99 runs, judged as the rates are
        a, b = interval(runs(first, 1)), interval(runs(second, 1))
        if b[2] < a[1]:
            judged_p99 = f"{second} lower"
        elif b[1] > a[2]:
            judged_p99 = f"{second} higher"
        else:
            judged_p99 = "within"
        p50 = f"{fmt(statistics.median(runs(first, 0)))} → {fmt(statistics.median(runs(second, 0)))}"
        p90 = f"{fmt(statistics.median(runs(first, 3)))} → {fmt(statistics.median(runs(second, 3)))}"
        mean = f"{fmt(statistics.median(runs(first, 2)))} → {fmt(statistics.median(runs(second, 2)))}"
        print(
            f"| `{arm_id}` | {p50} | {p90} | {mean} | {fmt(a[0])} [{fmt(a[1])}–{fmt(a[2])}] | "
            f"{fmt(b[0])} [{fmt(b[1])}–{fmt(b[2])}] | {judged_p99} |"
        )


def bucket_bounds(bucket):
    """A bucket's lowest wait and the bound past its highest, in microseconds, as the node buckets a
    client query's wait (shoal-core's meter::bucket_bounds): one a microsecond under four, then four to
    every power of two"""
    if bucket < 4:
        return bucket, bucket + 1
    octave = bucket // 4 + 1
    width = 1 << (octave - 2)
    low = (4 + bucket % 4) << (octave - 2)
    return low, low + width


def percentile(buckets, quantile):
    """A quantile of the waits counted by bucket, in microseconds, placed into its bucket as far as it
    is into the bucket's count (shoal-core's meter::percentile), or None for no waits"""
    total = sum(buckets)
    if total < 1:
        return None
    target = quantile * total
    below = 0
    for bucket, count in enumerate(buckets):
        if count > 0 and below + count >= target:
            low, high = bucket_bounds(bucket)
            return low + min(max((target - below) / count, 0.0), 1.0) * (high - low)
        below += count
    return bucket_bounds(max(at for at, count in enumerate(buckets) if count > 0))[1]


def rounds_of(runs):
    """Every run's rounds summed over its members and over the runs: barriers, their nanoseconds, the
    apply waits' nanoseconds, each list of counts by bucket, and the seconds the reads cover"""
    total = {"barriers": 0, "barrier_wait_ns": 0, "apply_wait_ns": 0, "secs": 0.0, "runs": 0}
    lists = {"barrier_buckets": [], "sent_buckets": [], "answered_buckets": []}
    for run in runs:
        members = run.get("rounds") or {}
        if not members:
            continue
        total["runs"] += 1
        # the members' reads span the run as its device counters do
        total["secs"] += max((device["secs"] for host in run.get("devices") or [] for device in host["devices"]), default=0.0)
        for member in members.values():
            for key in ("barriers", "barrier_wait_ns", "apply_wait_ns"):
                total[key] += member.get(key, 0)
            for key, mine in lists.items():
                theirs = member.get(key, [])
                mine.extend([0] * (len(theirs) - len(mine)))
                for at, count in enumerate(theirs):
                    mine[at] += count
    total.update(lists)
    return total


def print_rounds(captures, sides):
    """What each side's strong reads waited for, arm by arm, every member and run summed (F99)"""
    print("\n### Strong reads' rounds, every member and run summed (F99)\n")
    print("| arm | side | barriers/s | barrier mean ms | barrier p50 / p90 / p99 ms | appends sent a barrier | sent p50 / p90 / p99 ms | answered p50 / p90 / p99 ms | apply wait mean µs |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    # each arm's runs by side, in the arms' order
    by_arm = {}
    for side in sides:
        for _, capture in captures[side]:
            for arm in capture["arms"]:
                by_arm.setdefault(arm["id"], {}).setdefault(side, []).extend(arm["runs"])
    # three quantiles of a list, in ms
    def three(buckets):
        found = [percentile(buckets, quantile) for quantile in (0.5, 0.9, 0.99)]
        return " / ".join("–" if value is None else f"{value / 1000:.2f}" for value in found)
    printed = 0
    for arm_id in sorted(by_arm, key=lambda arm: (arm.split("/")[0], arm)):
        for side in sides:
            total = rounds_of(by_arm[arm_id].get(side, []))
            if not total["runs"]:
                continue
            printed += 1
            barriers = total["barriers"]
            rate = barriers / total["secs"] if total["secs"] else float("nan")
            mean = total["barrier_wait_ns"] / barriers / 1e6 if barriers else float("nan")
            sent = sum(total["sent_buckets"])
            per_barrier = f"{sent / barriers:.2f}" if barriers else "–"
            apply_mean = total["apply_wait_ns"] / barriers / 1e3 if barriers else float("nan")
            print(
                f"| `{arm_id}` | {side} | {rate:,.0f} | {mean:.2f} | {three(total['barrier_buckets'])} | {per_barrier} | "
                f"{three(total['sent_buckets'])} | {three(total['answered_buckets'])} | {apply_mean:.1f} |"
            )
    if not printed:
        print("| no run of these captures kept its members' rounds: a build from before F99 | | | | | | | | |")


def memory_of(run):
    """One run's memory beside its load (F104): its members' highest resident bytes, the partitions
    they evicted during it by their rows' budget and by the node's memory, their caches' most held
    bytes and the share of reads the caches answered, its paced stream's rate, p50, p99 and p99.9,
    and the checks its members' caches gave units back on instead of rows being evicted (F107)"""
    samples = [sample.get("memory") or {} for sample in run.get("server_series") or []]
    members = sorted({member for sample in samples for member in sample})
    # the highest any member reached, and the most the caches held together
    peak = max((figures.get("resident_bytes", 0) for sample in samples for figures in sample.values()), default=0)
    held = max((sum(figures.get("cache_held_bytes", 0) for figures in sample.values()) for sample in samples), default=0)
    # each member's counters gained over the run, from its first sample to its last
    def gained(key):
        total = 0
        for member in members:
            seen = [sample[member].get(key, 0) for sample in samples if member in sample]
            if seen:
                total += seen[-1] - seen[0]
        return total
    hits, misses = gained("cache_hits"), gained("cache_misses")
    share = hits / (hits + misses) if hits + misses else None
    # the paced stream's reads, if it ran
    paced = (run.get("paced") or {}).get("measured", {}).get("read")
    paced = (
        (paced["per_sec"], paced["latency"]["p50_ms"], paced["latency"]["p99_ms"], paced["latency"].get("p999_ms"))
        if paced and paced.get("ok")
        else None
    )
    return peak, gained("evicted_by_budget"), gained("evicted_by_resident"), held, share, paced, gained("cache_yields")


def objects_of(run):
    """One run's object work (F108): the most requests waiting on its members' lanes, answers their
    served lanes owed, object tasks their shards ran and bytes drawn from their budgets, each summed
    over the members of one sample; and the requests their lanes refused, the draws their budgets
    shed, the tasks their shards refused and the puts they gave up during it, every member's gained
    from its first sample to its last"""
    samples = [sample.get("objects") or {} for sample in run.get("server_series") or []]
    members = sorted({member for sample in samples for member in sample})
    # a run of a build before F108 kept none, which is not the same as none in flight
    if not members:
        return None
    # the most the members held together in any one sample
    def most(key):
        return max((sum(figures.get(key, 0) for figures in sample.values()) for sample in samples), default=0)
    # each member's counter gained over the run
    def gained(key):
        total = 0
        for member in members:
            seen = [sample[member].get(key, 0) for sample in samples if member in sample]
            if seen:
                total += seen[-1] - seen[0]
        return total
    return (
        most("waiting"), most("owed"), most("tasks"), most("budget_used"),
        gained("refused"), gained("budget_shed"), gained("tasks_refused"), gained("puts_given_up"),
    )


def print_objects(everything, first, second):
    """Each arm's object work on each side, the median of its runs [lowest–highest], every run read,
    those that measured nothing too (F108)"""
    print("\n### Object work beside the load: the median of every run [lowest–highest], unmeasured runs included\n")
    print("| arm | side | runs | waiting, most | owed, most | tasks, most | budget MiB, most | lane refused | budget shed | tasks refused | puts given up |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    def spread(values, scale=1.0):
        if not values:
            return "–"
        low, high = min(values) / scale, max(values) / scale
        return f"{fmt(statistics.median(values) / scale)} [{fmt(low)}–{fmt(high)}]"
    for arm_id in sorted(everything, key=lambda arm: (arm.split("/")[0], arm)):
        for side in (first, second):
            runs = everything[arm_id].get(side, [])
            if not runs:
                continue
            figures = [entry for entry in (objects_of(run) for run, _ in runs) if entry is not None]
            unmeasured = sum(1 for _, measured in runs if not measured)
            count = f"{len(runs)}" + (f" ({unmeasured} measuring nothing)" if unmeasured else "")
            columns = [spread([entry[index] for entry in figures], 2**20 if index == 3 else 1.0) for index in range(8)]
            print(f"| `{arm_id}` | {side} | {count} | " + " | ".join(columns) + " |")


def print_memory(raw, first, second):
    """Each arm's memory beside its load on each side, the median of its runs [lowest–highest] (F104)"""
    print("\n### Memory beside the load: the median of every run [lowest–highest]\n")
    print("| arm | side | peak resident GiB | evicted, budget | evicted, resident | cache yields | cache held GiB | cache hit share | paced reads/s | paced p50 ms | paced p99 ms | paced p99.9 ms |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    def spread(values, scale=1.0, places=fmt):
        values = [value for value in values if value is not None]
        if not values:
            return "–"
        low, high = min(values) / scale, max(values) / scale
        return f"{places(statistics.median(values) / scale)} [{places(low)}–{places(high)}]"
    for arm_id in sorted(raw, key=lambda arm: (arm.split("/")[0], arm)):
        for side in (first, second):
            figures = [memory_of(run) for run in raw[arm_id].get(side, [])]
            if not figures:
                continue
            paced = [entry[5] for entry in figures if entry[5]]
            print(
                f"| `{arm_id}` | {side} | {spread([entry[0] for entry in figures], 2**30)} | "
                f"{spread([entry[1] for entry in figures])} | {spread([entry[2] for entry in figures])} | "
                f"{spread([entry[6] for entry in figures])} | {spread([entry[3] for entry in figures], 2**30)} | "
                f"{spread([entry[4] for entry in figures], places=lambda value: f'{value:.0%}')} | "
                f"{spread([entry[0] for entry in paced])} | {spread([entry[1] for entry in paced])} | "
                f"{spread([entry[2] for entry in paced])} | {spread([entry[3] for entry in paced])} |"
            )


def main():
    # the stores to judge each side beside, if any, are the last two arguments
    args = sys.argv[1:]
    # whether to print each side's latencies by run as well, its strong reads' rounds, and its memory
    latencies = "--latency" in args
    rounds = "--rounds" in args
    memory = "--memory" in args
    objects = "--objects" in args
    args = [arg for arg in args if arg not in ("--latency", "--rounds", "--memory", "--objects")]
    beside = None
    if "--beside" in args:
        at = args.index("--beside")
        beside = (args[at + 1], args[at + 2].split(","))
        args = args[:at] + args[at + 3:]
    folder = args[0].rstrip("/")
    tag = os.path.basename(folder)
    # every capture by its side
    captures = {}
    for name in sorted(os.listdir(folder)):
        if not name.endswith(".json"):
            continue
        side, number = side_of(name, tag)
        with open(os.path.join(folder, name)) as handle:
            captures.setdefault(side, []).append((number, json.load(handle)))
    sides = args[1:3] if len(args) >= 3 else sorted(captures, key=lambda side: min(n for n, _ in captures[side]))
    first, second = sides[0], sides[1]
    # each arm's runs on each side, as figures and as they are (F104's memory reads them whole)
    arms = {}
    raw = {}
    # a run that failed before it measured anything is left out, and said so; the object work reads
    # every run, whether it measured or not (F108)
    unmeasured = []
    everything = {}
    for side in (first, second):
        for number, capture in captures[side]:
            for arm in capture["arms"]:
                for run in arm["runs"]:
                    measured = "kinds" in run.get("measured", {})
                    everything.setdefault(arm["id"], {}).setdefault(side, []).append((run, measured))
                    if not measured:
                        unmeasured.append(f"{side} {number} {arm['id']} run {run.get('run')}")
                        continue
                    arms.setdefault(arm["id"], {}).setdefault(side, []).append(run_figures(run))
                    raw.setdefault(arm["id"], {}).setdefault(side, []).append(run)
    print(f"## {tag}: {second} beside {first}\n")
    if unmeasured:
        print(f"Left out, measuring nothing: {', '.join(unmeasured)}\n")
    print(f"{len(captures[first])} captures of {first}, {len(captures[second])} of {second}; operations a second as median [lowest–highest] of every run\n")
    print(f"| arm | {first} | {second} | {second} ÷ {first} | verdict | p50 ms | p99 ms | device B written a B sent | device B read a B received |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for arm_id in sorted(arms, key=lambda arm: (arm.split("/")[0], arm)):
        by_side = arms[arm_id]
        if first not in by_side or second not in by_side:
            continue
        a = interval([figures[0] for figures in by_side[first]])
        b = interval([figures[0] for figures in by_side[second]])
        ratio = b[0] / a[0] if a[0] else float("nan")
        if b[1] > a[2]:
            verdict = f"{second} ahead"
        elif b[2] < a[1]:
            verdict = f"{second} behind"
        else:
            verdict = "within"
        # latencies of the arm's first kind with any operations, each side's median of its runs
        def latency(side, at):
            values = [figures[1][kind][at] for figures in by_side[side] for kind in sorted(figures[1])[:1]]
            return statistics.median(values) if values else 0.0
        p50 = f"{fmt(latency(first, 0))} → {fmt(latency(second, 0))}"
        p99 = f"{fmt(latency(first, 1))} → {fmt(latency(second, 1))}"
        def per_byte(side, device, driver):
            device_bytes = sum(figures[device] for figures in by_side[side])
            driver_bytes = sum(figures[driver] for figures in by_side[side])
            return device_bytes / driver_bytes if driver_bytes else None
        def show(value):
            return "–" if value is None else fmt(value)
        written = f"{show(per_byte(first, 2, 4))} → {show(per_byte(second, 2, 4))}"
        read = f"{show(per_byte(first, 3, 5))} → {show(per_byte(second, 3, 5))}"
        print(
            f"| `{arm_id}` | {fmt(a[0])} [{fmt(a[1])}–{fmt(a[2])}] | {fmt(b[0])} [{fmt(b[1])}–{fmt(b[2])}] | "
            f"{ratio:.2f} | {verdict} | {p50} | {p99} | {written} | {read} |"
        )
    if latencies:
        print_latencies(arms, first, second)
    if rounds:
        print_rounds(captures, (first, second))
    if memory:
        print_memory(raw, first, second)
    if objects:
        print_objects(everything, first, second)
    if beside is None:
        return
    # each side beside the comparison's captures of the stores named
    stores_dir, stores = beside
    others = {}
    for store in stores:
        with open(os.path.join(stores_dir, f"{store}.json")) as handle:
            others[store] = rates_of(json.load(handle))
    print(f"\n### Beside the comparison's captures in {stores_dir} (another day: a guide, not a verdict)\n")
    print("| arm | " + " | ".join(f"{store}: {first}, {second}" for store in stores) + " |")
    print("| --- |" + " --- |" * len(stores))
    for arm_id in sorted(arms, key=lambda arm: (arm.split("/")[0], arm)):
        by_side = arms[arm_id]
        if first not in by_side or second not in by_side:
            continue
        cells = []
        for store in stores:
            if arm_id not in others[store]:
                cells.append("–")
                continue
            ours = [[figures[0] for figures in by_side[side]] for side in (first, second)]
            cells.append(f"{judged(ours[0], others[store][arm_id])}, {judged(ours[1], others[store][arm_id])}")
        print(f"| `{arm_id}` | " + " | ".join(cells) + " |")


main()
