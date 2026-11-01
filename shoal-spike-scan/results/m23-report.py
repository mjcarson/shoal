"""M23's exit from its captures: the rebuilt walk beside the cell and alone, after churn, and the
drivers before and after

    python3 -I shoal-spike-scan/results/m23-report.py shoal-spike-scan/results/m23 > m23-report.md

Every capture is one `m23-lab.sh` names, whole (`<label>/bench.json`) or slimmed by its `fetch`
(`<label>.json`): `m23-r<round>-<load|big|keyword>`, the rounds; `m23-churn-<segment>-<load|scan|
churn|afterchurn>`, the churn at a segment size; and `m23-drivers-<before|after>-r<round>`, the
drivers. Every figure is each round's, then its median and range over the rounds. The exit's
clauses are judged on the medians:

- the foreground (W7, W12): the cell's read and insert p99 during a whole scan within
  `max(1.10 x before, before + 0.1 ms)`, no eviction pass begun by the process's memory, no scan step
  over 100 us;
- a scan's cost (W12, decision 5): at least ten partitions a device read and at most 15 us of a Zen1
  host's cpu a partition, Big scanned alone at its ceiling once loaded; at least nine partitions a
  device read after an hour's churn;
- the drivers: each event's work and the foreground beside it no worse than before, a difference
  only where the two sides' ranges are disjoint, the lab's rule.

W7's insert clause is read as it is defined only on rounds whose cell's feed never wrapped inside an
arm: past a wrap the cell overwrites the rows its arm wrote, at about three fifths of the rate (item
351). Each round says how many of its cell's arms wrapped, the inserts of its longest, the pool they
were drawn from, and the members' peak resident memory over the node's, which a cell that never wraps
grows by every row it inserts. The first exit's rounds (`results/m23`) wrap in every round; the
rounds again run on a cell of 1,100,000 rows at 6 GiB a node.
"""

import json
import pathlib
import re
import statistics
import sys

ROUND = re.compile(r"^m23-r(\d+)-(load|big|keyword)(\.json)?$")
CHURN = re.compile(r"^m23-churn-(\w+)-(load|scan|churn|afterchurn)(\.json)?$")
DRIVERS = re.compile(r"^m23-drivers-(before|after)-r(\d+)(\.json)?$")
# the idle cluster's cpu before each churn scan alone: a host, its node's cores busy, the seconds
IDLE = re.compile(r"^m23-churn-(\w+)-(idle|afteridle)(\.txt)?$")
# the control: the cell alone where a round's Keyword capture scans, after the same Big capture
CONTROL = re.compile(r"^m23-control-r(\d+)-(load|big|cell)(\.json)?$")


def load(root):
    """Every capture under a directory, whole or slimmed, by its name's parts, and the idle
    samples beside them"""
    rounds, churn, drivers, idles, controls = {}, {}, {}, {}, {}
    for path in sorted(pathlib.Path(root).iterdir()):
        name = path.name
        match = IDLE.match(name)
        if match:
            file = path if path.is_file() else path / "idle.txt"
            if file.exists():
                with open(file, encoding="utf-8") as handle:
                    rows = [line.split() for line in handle if line.strip()]
                idles[(match.group(1), match.group(2))] = {row[0]: float(row[1]) for row in rows}
            continue
        file = path if path.is_file() else path / "bench.json"
        if not file.exists():
            continue
        for pattern, into, key in (
            (ROUND, rounds, lambda m: (int(m.group(1)), m.group(2))),
            (CHURN, churn, lambda m: (m.group(1), m.group(2))),
            (DRIVERS, drivers, lambda m: (m.group(1), int(m.group(2)))),
            (CONTROL, controls, lambda m: (int(m.group(1)), m.group(2))),
        ):
            match = pattern.match(name)
            if match:
                with open(file, encoding="utf-8") as handle:
                    into[key(match)] = json.load(handle)
                break
    return rounds, churn, drivers, idles, controls


def control_figures(controls, rounds):
    """The cell alone where a round's Keyword capture scans, cut at the second its scans begin and at
    its own feed's wrap: what the cell's p99 a second does over a long arm with no scan beside it"""
    rows = {}
    # where the Keyword captures' scans were asked, their median second
    starts = [min(mark["at_ms"] for mark in (run.get("event") or {}).get("marks", []) if mark.get("kind") == "requested") / 1000
              for (_, which), capture in rounds.items() if which == "keyword"
              for run in [arm(capture, "rw50/b1/scan")] if run and (run.get("event") or {}).get("marks")]
    start = statistics.median(starts) if starts else 28.0
    for (round_, which), capture in controls.items():
        if which != "cell":
            continue
        run = arm(capture, "rw50/b1/none")
        if not run:
            continue
        put_cell_feed(rows, "cell alone, the control", round_, capture)
        wrap = wrap_second(run)
        cut = wrap if wrap is not None else float("inf")
        put(rows, "cell alone, the control", "seconds before the wrap past the scans' start", round_, max(0.0, cut - start))
        for kind in ("read", "insert"):
            before, during = second_p99s(run, kind, 0, start), second_p99s(run, kind, start, cut)
            if len(before) >= 5 and len(during) >= 5:
                b, d = statistics.median(before), statistics.median(during)
                put(rows, "cell alone, the control", f"{kind} p99 a second, before the scans' start", round_, b)
                put(rows, "cell alone, the control", f"{kind} p99 a second, after it and before the wrap", round_, d)
                put(rows, "cell alone, the control", f"{kind} p99 a second, after it and before the wrap / before", round_, d / b)
    return rows


def arm(capture, arm_id):
    """An arm's one run, or none"""
    for entry in capture.get("arms", []):
        if entry["id"] == arm_id and entry.get("runs"):
            return entry["runs"][0]
    return None


def arms_like(capture, prefix):
    """Every arm whose id starts so, by id, its one run"""
    return {entry["id"]: entry["runs"][0] for entry in capture.get("arms", []) if entry["id"].startswith(prefix) and entry.get("runs")}


def p99(summary, kind):
    """A kind's p99 in a window's summary, or none"""
    latency = (summary or {}).get(kind, {}).get("latency") or {}
    return latency.get("p99_ms")


def kind_p99(summary, kind):
    """An object operation's p99 in a window's summary, kept by kind, or none"""
    latency = (((summary or {}).get("kinds") or {}).get(kind) or {}).get("latency") or {}
    return latency.get("p99_ms")


def evicted_by_resident(capture):
    """The partitions the process's memory evicted on every member since the restart before a
    capture, from its last arm's last sample: a memory sample counts since its node started, and
    leaves a count of nothing out (F104)"""
    last = None
    for entry in capture.get("arms", []):
        for run in entry.get("runs", []):
            for sample in run.get("server_series") or []:
                if sample.get("memory"):
                    last = sample["memory"]
    if last is None:
        return None
    return sum((figures or {}).get("evicted_by_resident", 0) for figures in last.values())


def scan_steps(capture):
    """The scan class's loop steps past 100 us, 1 ms and 10 ms on every member since the restart
    before a capture, and the longest, from the last sample that carries scan figures: a node's
    counts are since it started, and every capture follows a restart (F123)"""
    last = None
    for entry in capture.get("arms", []):
        for run in entry.get("runs", []):
            for sample in run.get("server_series") or []:
                if sample.get("scans"):
                    last = sample["scans"]
    if last is None:
        return None
    past, longest, steps = [0, 0, 0], 0, 0
    for figures in last.values():
        scan = (figures or {}).get("scan") or {}
        for i in range(3):
            past[i] += (scan.get("steps_past") or [0, 0, 0])[i]
        longest = max(longest, scan.get("longest_step_us") or 0)
        steps += scan.get("steps") or 0
    return past, longest, steps


def steps_by_arm(capture):
    """Each arm's scan steps a member, from the counts since the restart its last sample carries
    less the arm's before it: steps, those past 100 us, and the resident partitions and bytes they
    handed out, by arm and member (item 350)"""
    figures, before = {}, {}
    for entry in capture.get("arms", []):
        run = entry["runs"][0] if entry.get("runs") else {}
        samples = [sample for sample in run.get("server_series") or [] if sample.get("scans")]
        if not samples:
            continue
        for member, counts in samples[-1]["scans"].items():
            scan = (counts or {}).get("scan") or {}
            now = (scan.get("steps", 0), (scan.get("steps_past") or [0])[0], scan.get("resident", 0), scan.get("resident_bytes", 0))
            # an arm's own counts are what moved since the arm before it, or since the restart
            then = before.get(member, (0, 0, 0, 0))
            figures[(entry["id"], member)] = tuple(n - t for n, t in zip(now, then))
            before[member] = now
    return figures


def scan_series(run):
    """Each member's scan class and pace over a run, the last sample that carries them less the
    first, and the seconds between: reads, partitions, steps, busy time and bytes a member's"""
    samples = [sample for sample in run.get("server_series") or [] if sample.get("scans")]
    if len(samples) < 2:
        return None, None
    first, last = samples[0]["scans"], samples[-1]["scans"]
    secs = (samples[-1]["at_ms"] - samples[0]["at_ms"]) / 1000
    out = {}
    for member, now in last.items():
        then = first.get(member) or {}
        scan, was = now.get("scan") or {}, then.get("scan") or {}
        out[member] = {
            "reads": (scan.get("reads") or 0) - (was.get("reads") or 0),
            "partitions": (scan.get("partitions") or 0) - (was.get("partitions") or 0),
            "archived": (scan.get("archived") or 0) - (was.get("archived") or 0),
            "pages": (scan.get("pages") or 0) - (was.get("pages") or 0),
            "shed": (scan.get("shed_full") or 0) - (was.get("shed_full") or 0),
            "busy_s": ((now.get("busy_us") or 0) - (then.get("busy_us") or 0)) / 1e6,
            "bytes": ((now.get("idle_bytes") or 0) - (then.get("idle_bytes") or 0))
            + ((now.get("trickled_bytes") or 0) - (then.get("trickled_bytes") or 0)),
            "trickled": (now.get("trickled_bytes") or 0) - (then.get("trickled_bytes") or 0),
        }
    return out, secs


def pace_figures(run, members):
    """A run's pace on some members' devices: the scans' busy time a second, their MiB a second, and
    what a MiB cost them in busy time, the last the cost the pace learned, more or less"""
    series, secs = scan_series(run)
    if not series or not secs:
        return {}
    mine = [series[member] for member in members if member in series]
    if not mine:
        return {}
    busy = sum(figures["busy_s"] for figures in mine) / len(mine)
    mib = sum(figures["bytes"] for figures in mine) / len(mine) / (1 << 20)
    out = {"scans' busy ms a second": busy * 1000 / secs, "scan MiB a second": mib / secs}
    if mib:
        out["scans' busy ms a MiB"] = busy * 1000 / mib
    reads = sum(figures["reads"] for figures in mine)
    if reads:
        out["partitions a walk read"] = sum(figures["archived"] for figures in mine) / reads
    return out


def wrap_second(run, table="Cell"):
    """The second of a run's series by which its table's feed had taken the rows it wrapped at, or
    none if it never wrapped: past it the cell's inserts start again at the pool's first row and
    overwrite the rows the arm itself wrote, resident, at about three fifths of the rate, a workload
    of its own (item 351)"""
    feed = (run.get("feeds") or {}).get(table) or {}
    at = feed.get("wrapped_at")
    if not at:
        return None
    taken = 0
    for second in run.get("series") or []:
        taken += (((second.get("summary") or {}).get("insert") or {}).get("ok") or 0)
        if taken >= at:
            return second.get("at")
    return None


def cell_feed(capture, table="Cell"):
    """What a capture's arms took from the cell's feed: how many of the arms that inserted wrapped,
    how many arms inserted, the inserts of its longest arm, and the pool they were drawn from"""
    wrapped, inserting, longest = 0, 0, 0
    for entry in capture.get("arms", []):
        for run in entry.get("runs", []):
            feed = (run.get("feeds") or {}).get(table) or {}
            if not feed.get("taken"):
                continue
            inserting += 1
            wrapped += 1 if feed.get("wrapped_at") is not None else 0
            longest = max(longest, feed["taken"])
    pool = next((facts.get("insert_rows") for facts in ((capture.get("dataset") or {}).get("tables") or [])
                 if facts.get("table") == table), None)
    return wrapped, inserting, longest, pool


def peak_memory(capture):
    """The most any member's resident memory came to over its node's, at any sample of any arm: a cell
    that never wraps keeps every row it inserts, and an eviction pass past the node's memory evicts
    them"""
    peak = None
    for entry in capture.get("arms", []):
        for run in entry.get("runs", []):
            for sample in run.get("server_series") or []:
                for figures in (sample.get("memory") or {}).values():
                    resident, budget_bytes = (figures or {}).get("resident_bytes"), (figures or {}).get("memory_budget")
                    if resident and budget_bytes:
                        peak = max(peak or 0.0, resident / budget_bytes)
    return peak


def put_cell_feed(rows, key, round_, capture):
    """A capture's cell feed and memory, as rows of a table"""
    wrapped, inserting, longest, pool = cell_feed(capture)
    if inserting:
        put(rows, key, "cell arms whose feed wrapped", round_, wrapped)
        put(rows, key, "cell inserts, its longest arm", round_, longest)
    put(rows, key, "cell insert pool, rows", round_, pool)
    put(rows, key, "members' peak resident over the node's memory", round_, peak_memory(capture))


def second_p99s(run, kind, low, high):
    """The p99 of a kind in each second of a run's series in [low, high), its measured seconds alone"""
    out = []
    for second in run.get("series") or []:
        at = second.get("at")
        if second.get("phase") != "measure" or at is None or not low <= at < high:
            continue
        value = ((((second.get("summary") or {}).get(kind) or {}).get("latency")) or {}).get("p99_ms")
        if value:
            out.append(value)
    return out


def before_the_wrap(run):
    """The cell's read and insert p99 each second, before the scan and during it before the cell's feed
    wrapped, each the median of its seconds' p99s: what W7 asks with the cell's own change of workload
    taken out"""
    marks = (run.get("event") or {}).get("marks") or []
    asked = [mark["at_ms"] / 1000 for mark in marks if mark.get("kind") == "requested"]
    done = [mark["at_ms"] / 1000 for mark in marks if mark.get("kind") == "done"]
    if not asked:
        return {}
    start, end = min(asked), max(done) if done else float("inf")
    wrap = wrap_second(run)
    cut = min(end, wrap) if wrap is not None else end
    out = {"seconds of the scan before the wrap": max(0.0, cut - start)}
    for kind in ("read", "insert"):
        before, during = second_p99s(run, kind, 0, start), second_p99s(run, kind, start, cut)
        if len(before) >= 5 and len(during) >= 5:
            b, d = statistics.median(before), statistics.median(during)
            out[f"{kind} p99 a second, before"] = b
            out[f"{kind} p99 a second, during before the wrap"] = d
            out[f"{kind} p99 a second, during before the wrap / before"] = d / b
    return out


def event_secs(run):
    """How long a run's event took, from its marks, the first asked to the last done"""
    marks = (run.get("event") or {}).get("marks") or []
    asked = [mark["at_ms"] for mark in marks if mark.get("kind") == "requested"]
    done = [mark["at_ms"] for mark in marks if mark.get("kind") == "done"]
    if asked and done:
        return (max(done) - min(asked)) / 1000
    return None


def hosts(run):
    """Every host's counters over a run"""
    return run.get("devices") or []


def device_sum(run, field, members=None):
    """A device counter summed over every host's devices, or some hosts'"""
    return sum(device.get(field, 0) for host in hosts(run) if members is None or host.get("host") in members
               for device in host.get("devices", []))


def node_cpu_secs(run, members=None):
    """The cpu every host's node process spent over a run, or some hosts'"""
    return sum(process.get("cpu_secs", 0) for host in hosts(run) if members is None or host.get("host") in members
               for process in host.get("processes", []))


def device_secs(run, members):
    """The seconds some hosts' device counters span, their mean, which a run's warmup and its
    measurement and the counters' reads around them make"""
    spans = [device.get("secs") for host in hosts(run) if host.get("host") in members for device in host.get("devices", [])]
    spans = [span for span in spans if span]
    return sum(spans) / len(spans) if spans else None


# the hosts a figure is judged on: the Zen1 hosts together, as W12 states its budget, and europa apart
GROUPS = (("Zen1 hosts'", ("titan", "hyperion")), ("europa's", ("europa",)))


def scan_alone(run, idle=None):
    """A scan arm's figures, the hosts' counters over the whole run over every partition its
    members read in it, and with the idle cluster's cpu sampled before it, the cpu a partition
    above that background as well"""
    scan = run["measured"].get("scan") or {}
    warm = (run.get("warmup") or {}).get("scan") or {}
    out = {
        "partitions a second": scan.get("partitions_per_sec"),
        "MiB a second": (scan.get("bytes_per_sec") or 0) / (1 << 20),
        "page read p99 ms": (scan.get("read") or {}).get("p99_ms"),
        "page held p99 ms": (scan.get("held") or {}).get("p99_ms"),
        "page paced p50 ms": (scan.get("paced") or {}).get("p50_ms"),
    }
    pages = scan.get("pages") or 0
    if pages:
        out["pages shed a page"] = (scan.get("shed") or 0) / pages
    # each host's own, by the partitions its member read; the hosts' counters span warmup and
    # measurement, so every partition of both counts
    for name, members in GROUPS:
        read = sum(((scan.get("members") or {}).get(member) or {}).get("partitions", 0)
                   + ((warm.get("members") or {}).get(member) or {}).get("partitions", 0) for member in members)
        secs = device_secs(run, members)
        if read:
            out[f"{name} node cpu us a partition"] = node_cpu_secs(run, members) / read * 1e6
            # above the background: each node's idle cores over the counters' span taken out
            if idle and secs and all(member in idle for member in members):
                background = sum(idle[member] for member in members) * secs
                out[f"{name} node cpu us a partition above idle"] = (node_cpu_secs(run, members) - background) / read * 1e6
                out[f"{name} idle node cpu a second"] = sum(idle[member] for member in members) / len(members)
            reads = device_sum(run, "reads", members)
            if reads:
                out[f"{name} partitions a device read"] = read / reads
                out[f"{name} device KiB a read"] = device_sum(run, "read_bytes", members) / reads / 1024
        if secs:
            out[f"{name} node cpu a second"] = node_cpu_secs(run, members) / len(members) / secs
            out[f"{name} device busy ms a second"] = device_sum(run, "busy_ms", members) / len(members) / secs
        for figure, value in pace_figures(run, members).items():
            out[f"{name} {figure}"] = value
    return out


def put(rows, key, name, round_, value):
    """A figure of a round"""
    if value is not None:
        rows.setdefault((key, name), {})[round_] = value


def round_figures(rounds):
    """The rounds' figures, by table and name, a value a round"""
    rows = {}
    for (round_, which), capture in rounds.items():
        if which == "load":
            continue
        alone = arm(capture, "rw50/b1/none")
        if alone:
            measured = alone["measured"]
            put(rows, "cell", "read p99 ms, alone", round_, p99(measured, "read"))
            put(rows, "cell", "insert p99 ms, alone", round_, p99(measured, "insert"))
            put(rows, "cell", "reads a second, alone", round_, measured.get("read", {}).get("per_sec"))
        for table in ("Big", "Keyword"):
            run = arm(capture, f"scan/{table}/none")
            if run:
                for name, value in scan_alone(run).items():
                    put(rows, f"{table} alone", name, round_, value)
        table = {"big": "Big", "keyword": "Keyword"}[which]
        # the capture's loop steps and evictions, every arm of it since the restart before it
        key = f"cell beside {table}"
        put(rows, key, "Resident evictions in the capture", round_, evicted_by_resident(capture))
        put_cell_feed(rows, key, round_, capture)
        steps = scan_steps(capture)
        if steps:
            put(rows, key, "scan steps past 100 us in the capture", round_, steps[0][0])
            put(rows, key, "scan steps in the capture", round_, steps[2])
            put(rows, key, "scan longest step us in the capture", round_, steps[1])
        run = arm(capture, "rw50/b1/scan")
        if not run or not run.get("event"):
            continue
        windows = run["event"].get("windows") or {}
        before, during, after = windows.get("before"), windows.get("during"), windows.get("after")
        for kind in ("read", "insert"):
            put(rows, key, f"{kind} p99 ms before", round_, p99(before, kind))
            put(rows, key, f"{kind} p99 ms during", round_, p99(during, kind))
            put(rows, key, f"{kind} p99 ms after", round_, p99(after, kind))
            if p99(before, kind) and p99(during, kind):
                put(rows, key, f"{kind} p99 during / before", round_, p99(during, kind) / p99(before, kind))
            # beside the cell's own drift over a long arm: during over its before and after's mean,
            # which a scan that finished leaves the after window free of
            if p99(before, kind) and p99(during, kind) and p99(after, kind):
                beside = (p99(before, kind) + p99(after, kind)) / 2
                put(rows, key, f"{kind} p99 during / mean of before and after", round_, p99(during, kind) / beside)
            put(rows, key, f"{kind}s a second before", round_, ((before or {}).get(kind) or {}).get("per_sec"))
            put(rows, key, f"{kind}s a second during", round_, ((during or {}).get(kind) or {}).get("per_sec"))
        for name, value in before_the_wrap(run).items():
            put(rows, key, name, round_, value)
        scan = (during or {}).get("scan") or {}
        put(rows, key, "scan partitions a second", round_, scan.get("partitions_per_sec"))
        put(rows, key, "whole scan s", round_, event_secs(run))
        put(rows, key, "event p99 ratio permille", round_, run["event"].get("p99_ratio_permille"))
        put(rows, key, "page paced p99 ms", round_, (scan.get("paced") or {}).get("p99_ms"))
        for name, members in GROUPS:
            for figure, value in pace_figures(run, members).items():
                put(rows, key, f"{name} {figure}", round_, value)
    return rows


def churn_figures(churn, idles):
    """The churn's figures, by segment size: Big scanned alone before the churn and after it, each
    beside the idle cluster's cpu sampled before it"""
    rows = {}
    for (segment, which), capture in churn.items():
        if which not in ("scan", "afterchurn"):
            continue
        run = arm(capture, "scan/Big/none")
        if run:
            idle = idles.get((segment, "idle" if which == "scan" else "afteridle"))
            for name, value in scan_alone(run, idle).items():
                put(rows, f"{segment} {which}", name, 1, value)
    return rows


def window_seconds(window):
    """A window's span in seconds, if its summary says"""
    return (window or {}).get("secs")


def driver_figures(drivers):
    """The drivers' figures, by side and event: each event's work as its window spans it, and the
    object operations' p99 before it and during it"""
    rows = {}
    for (side, round_), capture in drivers.items():
        for arm_id, run in arms_like(capture, "").items():
            event = run.get("event") or {}
            name = arm_id.split("/")[-1]
            windows = event.get("windows") or {}
            before, during = windows.get("before"), windows.get("during")
            # an event's work from its asking to its last done, else as long as its window ran
            done = event_secs(run)
            if done is not None:
                put(rows, (side, name), "event done after s", round_, done)
            elif window_seconds(during):
                put(rows, (side, name), "during window s", round_, window_seconds(during))
            summary = during if during else run.get("measured")
            # an object arm's operations by kind, a get and a put, each a p99 where it ran
            for kind in ("put", "get"):
                value = kind_p99(summary, kind)
                if value:
                    put(rows, (side, name), f"{kind} p99 ms {'during' if during else 'measured'}", round_, value)
                if before and kind_p99(before, kind):
                    put(rows, (side, name), f"{kind} p99 ms before", round_, kind_p99(before, kind))
                rate = (((summary or {}).get("kinds") or {}).get(kind) or {}).get("per_sec")
                if rate:
                    put(rows, (side, name), f"{kind}s a second {'during' if during else 'measured'}", round_, rate)
    return rows


def cell(values):
    """A figure's rounds as its median and range"""
    if not values:
        return "-"
    ordered = sorted(values.values())
    median = statistics.median(ordered)
    return f"{median:.3g} [{ordered[0]:.3g}, {ordered[-1]:.3g}] (n {len(ordered)})"


def median(values):
    """A figure's median, or none"""
    return statistics.median(values.values()) if values else None


def budget(before, during):
    """Whether a p99 during a scan is inside W7's budget of its before, max(1.10 x, + 0.1 ms)"""
    return during <= max(1.10 * before, before + 0.1)


def main():
    """Every table, then the exit's clauses judged on the medians"""
    rounds, churn, drivers, idles, controls = load(sys.argv[1])
    rows = round_figures(rounds)
    # the control's rows beside the rounds', a table of their own
    rows.update(control_figures(controls, rounds))
    for key in sorted({key for (key, _) in rows}):
        print(f"\n### {key}\n")
        print("| Figure | Rounds |")
        print("| --- | --- |")
        for (k, name), values in sorted(rows.items()):
            if k == key:
                print(f"| {name} | {cell(values)} |")
    crows = churn_figures(churn, idles)
    if crows:
        print("\n### Big alone before and after an hour's churn\n")
        keys = sorted({key for (key, _) in crows})
        print("| Figure | " + " | ".join(keys) + " |")
        print("| --- |" + " --- |" * len(keys))
        for name in sorted({name for (_, name) in crows}):
            print(f"| {name} | " + " | ".join(cell(crows.get((key, name), {})) for key in keys) + " |")
    # every round's steps past 100 us by arm and member, beside the resident partitions' size, which
    # they follow (item 350): each round's figures, not a median, so a grouping of them can be checked
    steps = {(round_, which): steps_by_arm(capture) for (round_, which), capture in sorted(rounds.items()) if which in ("big", "keyword")}
    members = sorted({member for figures in steps.values() for (_, member) in figures})
    if members:
        print("\n### Scan steps past 100 us, by arm and member\n")
        print("Each cell is a member's KiB a resident partition handed out, then its steps past 100 us of its steps.\n")
        print("| Round | Arm | " + " | ".join(members) + " |")
        print("| --- | --- |" + " --- |" * len(members))
        for (round_, which), figures in sorted(steps.items()):
            for arm_id in sorted({arm_id for (arm_id, _) in figures}):
                cells = []
                for member in members:
                    taken, past, resident, resident_bytes = figures.get((arm_id, member), (0, 0, 0, 0))
                    size = resident_bytes / resident / 1024 if resident else 0.0
                    cells.append(f"{size:.1f} KiB, {past} of {taken}")
                print(f"| {round_} | {which} {arm_id} | " + " | ".join(cells) + " |")
    drows = driver_figures(drivers)
    if drows:
        print("\n### The drivers before and after\n")
        print("| Event | Figure | Before | After | Disjoint |")
        print("| --- | --- | --- | --- | --- |")
        events = sorted({event for ((_, event), _) in drows})
        for event in events:
            names = sorted({name for ((side, e), name) in drows if e == event})
            for name in names:
                before = drows.get((("before", event), name), {})
                after = drows.get((("after", event), name), {})
                # the lab's rule needs a range a side: one round each says nothing
                disjoint = ""
                if len(before) >= 2 and len(after) >= 2:
                    b, a = sorted(before.values()), sorted(after.values())
                    disjoint = "yes" if b[-1] < a[0] or a[-1] < b[0] else "no"
                print(f"| {event} | {name} | {cell(before)} | {cell(after)} | {disjoint} |")
    # the exit's clauses
    print("\n### The exit's clauses, on the medians\n")
    print("| Clause | Measured | Held |")
    print("| --- | --- | --- |")
    for table in ("Big", "Keyword"):
        for kind in ("read", "insert"):
            b = median(rows.get((f"cell beside {table}", f"{kind} p99 ms before"), {}))
            d = median(rows.get((f"cell beside {table}", f"{kind} p99 ms during"), {}))
            if b is not None and d is not None:
                print(f"| the cell's {kind} p99 beside a scan of {table} | {b:.2f} -> {d:.2f} ms | {'yes' if budget(b, d) else 'no'} |")
            # the same against the cell's drift, not a clause of W7's: during over before and after's mean
            a = median(rows.get((f"cell beside {table}", f"{kind} p99 ms after"), {}))
            if b is not None and d is not None and a is not None:
                beside = (b + a) / 2
                print(f"| (beside its drift) the cell's {kind} p99 beside a scan of {table}, against before and after | {beside:.2f} -> {d:.2f} ms | {'yes' if budget(beside, d) else 'no'} |")
            # and before the cell's feed wrapped into the rows its arm wrote, a second at a time
            bw = median(rows.get((f"cell beside {table}", f"{kind} p99 a second, before"), {}))
            dw = median(rows.get((f"cell beside {table}", f"{kind} p99 a second, during before the wrap"), {}))
            if bw is not None and dw is not None:
                print(f"| (before the cell's wrap) the cell's {kind} p99 a second beside a scan of {table} | {bw:.2f} -> {dw:.2f} ms | {'yes' if budget(bw, dw) else 'no'} |")
        # W7's insert lines above are read as defined only where no round's cell wrapped inside an arm
        wraps = rows.get((f"cell beside {table}", "cell arms whose feed wrapped"), {})
        if wraps:
            rounds_wrapped = sum(1 for value in wraps.values() if value)
            print(f"| the cell's feed never wrapped inside an arm, the {table} captures | {rounds_wrapped} of {len(wraps)} rounds wrapped | {'yes' if rounds_wrapped == 0 else 'no'} |")
        # every round's own, not a median: one pass or one long step in any round misses the clause
        evictions = rows.get((f"cell beside {table}", "Resident evictions in the capture"), {})
        if evictions:
            most = max(evictions.values())
            print(f"| no eviction pass by the process's memory, the {table} captures | {most:g} at most | {'yes' if most == 0 else 'no'} |")
        steps = rows.get((f"cell beside {table}", "scan steps past 100 us in the capture"), {})
        if steps:
            most = max(steps.values())
            print(f"| no scan step over 100 us, the {table} captures | {most:g} at most | {'yes' if most == 0 else 'no'} |")
    # a scan's cost is judged alone at its ceiling on Big loaded once: the churn round's first scan,
    # nothing beside it, at today's segments; the rounds' Big alone has the paced stream beside it
    for key, label in (("10mib scan", "Big loaded once and scanned alone, 10 MiB segments"),
                       ("40mib scan", "Big loaded once and scanned alone, 40 MiB segments"),
                       ("Big alone", "the rounds' Big alone, the paced stream beside it")):
        source = rows if key == "Big alone" else crows
        reads = median(source.get((key, "Zen1 hosts' partitions a device read"), {}))
        if reads is not None:
            print(f"| at least ten partitions a device read, Zen1 hosts, {label} | {reads:.3g} | {'yes' if reads >= 10 else 'no'} |")
        cpu = median(source.get((key, "Zen1 hosts' node cpu us a partition"), {}))
        if cpu is not None:
            print(f"| at most 15 us of a Zen1 host's cpu a partition, {label} | {cpu:.3g} | {'yes' if cpu <= 15 else 'no'} |")
        above = median(source.get((key, "Zen1 hosts' node cpu us a partition above idle"), {}))
        if above is not None:
            print(f"| the same above the idle node's cpu, {label} | {above:.3g} | {'yes' if above <= 15 else 'no'} |")
    for segment in sorted({key.split()[0] for (key, _) in crows}):
        after = median(crows.get((f"{segment} afterchurn", "Zen1 hosts' partitions a device read"), {}))
        if after is not None:
            print(f"| at least nine partitions a device read after an hour's churn, {segment} segments | {after:.3g} | {'yes' if after >= 9 else 'no'} |")


if __name__ == "__main__":
    main()
