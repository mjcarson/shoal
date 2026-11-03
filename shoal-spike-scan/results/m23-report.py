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
  over 100 us: on its own cpu where a capture counts steps by kind (F130), a step past it by the wall
  alone named as O147's, and by the wall on a capture from before F130;
- a scan's cost (W12, decision 5): at least ten partitions a device read and at most 15 us of a Zen1
  host's cpu a partition, Big scanned alone at its ceiling once loaded; at least nine partitions a
  device read after an hour's churn;
- the drivers: each event's work and the foreground beside it no worse than before, a difference
  only where the two sides' ranges are disjoint, the lab's rule.

W7's insert clause is read as it is defined only on rounds whose cell's feed never wrapped inside an
arm, since past a wrap the cell overwrites the rows its arm wrote, and whose cell's inserts held their
rate inside the measured window. About 75 s into every arm measured from 10 s they fell to three
fifths of it, wrap or not, because the lab's 970 EVOs flush three times slower after about 75 s of
synced writes (item 351, resolved). The rounds again warm every cell arm 100 s, past that change, so
a fall inside their measured window means its windows straddle a change of state. Each
round says when its cell's inserts fell, how many of its cell's arms wrapped, the inserts of its
longest, the pool they were drawn from, and the members' peak resident memory over the node's, which
a cell that never wraps grows by every row it inserts. The first exit's rounds (`results/m23`) wrap
in every round; the rounds again run on a cell of 1,100,000 rows at 6 GiB a node, its check round
in `results/m23-cell/m23`, and its check round warmed past the drives' change in
`results/m23-warm/m23`.

The control, `m23-control-r<round>-<load|big|cell>`, is the cell alone where a round's Keyword capture
scans, after the same Big capture. Its p99 a second is cut at each table's scans as the rounds marked
them, their median asking and done seconds, and read beside the scan arms' own p99 a second: what the
cell does over a scan's seconds with no scan beside it, never a clause. The first exit ran two after
its rounds, 120 s each and cut at their wrap; the rounds again (`results/m23-again/m23`) run four
among theirs, one after every two, 270 s each, past the longest whole scan of Big.
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


def scan_marks(rounds, table):
    """The median second a table's whole scans beside the cell were asked at and were done by, over
    the rounds' captures of it, each none if no capture marks it"""
    asked, done = [], []
    for (_, which), capture in rounds.items():
        # only the capture whose cell arm scans this table
        if which != table.lower():
            continue
        run = arm(capture, "rw50/b1/scan")
        marks = ((run or {}).get("event") or {}).get("marks") or []
        # the first ask and the last done of the run, as `before_the_wrap` cuts them
        at_asked = [mark["at_ms"] / 1000 for mark in marks if mark.get("kind") == "requested"]
        at_done = [mark["at_ms"] / 1000 for mark in marks if mark.get("kind") == "done"]
        if at_asked:
            asked.append(min(at_asked))
        if at_done:
            done.append(max(at_done))
    return (statistics.median(asked) if asked else None, statistics.median(done) if done else None)


def per_second_rate(run, kind, low, high):
    """How many of a kind succeeded a second over a run's measured seconds in [low, high), or none"""
    counts = [(((second.get("summary") or {}).get(kind) or {}).get("ok") or 0)
              for second in run.get("series") or []
              if second.get("phase") == "measure" and second.get("at") is not None and low <= second["at"] < high]
    return sum(counts) / len(counts) if counts else None


def control_figures(controls, rounds):
    """The cell alone where a round's Keyword capture scans, cut at each table's scans as the rounds
    marked them: its p99 a second before the median second the scans were asked at, and from there to
    the median second they were done by, its own feed's wrap or its arm's end, whichever came first.
    What the cell's p99 a second does over a scan's seconds with no scan beside it, read beside the
    scan arms' own p99 a second (`before_the_wrap`). The first exit's control ran 120 s and was cut at
    its wrap; the rounds again run it 270 s, past the longest whole scan of Big"""
    rows = {}
    key = "cell alone, the control"
    # where each table's scans were asked and done, their median seconds, read once
    marks = {table: scan_marks(rounds, table) for table in ("Big", "Keyword")}
    for (round_, which), capture in controls.items():
        if which != "cell":
            continue
        run = arm(capture, "rw50/b1/none")
        if not run:
            continue
        put_cell_feed(rows, key, round_, capture, "rw50/b1/none")
        # the control's own end: its last measured second, then any wrap of its feed
        measured = [second.get("at") for second in run.get("series") or [] if second.get("phase") == "measure"]
        end = max(measured) + 1 if measured else 0
        wrap = wrap_second(run)
        for table, (start, done) in marks.items():
            if start is None:
                continue
            # the scan's seconds, as far as the control's arm and its feed reach
            cut = min(done if done is not None else float("inf"), end, wrap if wrap is not None else float("inf"))
            name = f"over {table}'s scan:"
            put(rows, key, f"{name} seconds of the scan covered", round_, max(0.0, cut - start))
            for kind in ("read", "insert"):
                before, during = second_p99s(run, kind, 0, start), second_p99s(run, kind, start, cut)
                if len(before) >= 5 and len(during) >= 5:
                    b, d = statistics.median(before), statistics.median(during)
                    put(rows, key, f"{name} {kind} p99 a second, before", round_, b)
                    put(rows, key, f"{name} {kind} p99 a second, during", round_, d)
                    put(rows, key, f"{name} {kind} p99 a second, during / before", round_, d / b)
                put(rows, key, f"{name} {kind}s a second, before", round_, per_second_rate(run, kind, 0, start))
                put(rows, key, f"{name} {kind}s a second, during", round_, per_second_rate(run, kind, start, cut))
    return rows


def ranges_disjoint(one, other):
    """Whether two figures' values over their rounds give ranges that do not overlap, the lab's rule
    for a difference; none if either has fewer than two values"""
    if len(one) < 2 or len(other) < 2:
        return None
    a, b = sorted(one.values()), sorted(other.values())
    return a[-1] < b[0] or b[-1] < a[0]


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


# a kind's figures as one tuple a capture's samples subtract: every step's count, wall, own cpu, keys,
# rows and bytes, the long steps' the same, the steps past 100 us by the wall and by own cpu (F130), and
# of those the steps on their own cpu that faulted and the steps by the wall whose thread left its cpu
# (item 353), zero in a capture from before them
KIND_FIGURES = ("steps", "wall_ns", "cpu_ns", "keys", "rows", "bytes")


def kind_counts(scan, kind):
    """A class's figures of one kind of step as a tuple, or None if they were counted before F130"""
    figures = scan.get(kind)
    if not figures:
        return None
    every = figures.get("all") or {}
    long = figures.get("long") or {}
    return (tuple(every.get(name, 0) for name in KIND_FIGURES)
            + tuple(long.get(name, 0) for name in KIND_FIGURES)
            + ((figures.get("past") or [0])[0], (figures.get("past_cpu") or [0])[0])
            + (figures.get("past_cpu_faulted", 0), figures.get("past_switched", 0)))


def carries_kernel_events(capture):
    """Whether a capture's members counted what the kernel did inside a long step (item 353)"""
    for entry in capture.get("arms", []):
        for run in entry.get("runs", []):
            for sample in run.get("server_series") or []:
                for figures in (sample.get("scans") or {}).values():
                    scan = (figures or {}).get("scan") or {}
                    if any("past_kernel" in (scan.get(kind) or {}) for kind in ("captures", "encodes")):
                        return True
    return False


def scan_step_kinds(capture):
    """The scan class's steps by kind on every member since the restart before a capture, from the
    last sample that carries scan figures: each kind's counts summed, or None for a capture from
    before F130, whose figures count no kind"""
    last = None
    for entry in capture.get("arms", []):
        for run in entry.get("runs", []):
            for sample in run.get("server_series") or []:
                if sample.get("scans"):
                    last = sample["scans"]
    if last is None:
        return None
    kinds = {}
    for figures in last.values():
        scan = (figures or {}).get("scan") or {}
        for kind in ("captures", "encodes"):
            counts = kind_counts(scan, kind)
            if counts is None:
                return None
            kinds[kind] = tuple(a + b for a, b in zip(kinds.get(kind, (0,) * len(counts)), counts))
    return kinds or None


def kinds_by_arm(capture):
    """Each arm's scan steps by kind a member, from the counts since the restart its last sample
    carries less the arm's before it, or nothing for a capture from before F130 (item 350)"""
    figures, before = {}, {}
    for entry in capture.get("arms", []):
        run = entry["runs"][0] if entry.get("runs") else {}
        samples = [sample for sample in run.get("server_series") or [] if sample.get("scans")]
        if not samples:
            continue
        for member, counts in samples[-1]["scans"].items():
            scan = (counts or {}).get("scan") or {}
            for kind in ("captures", "encodes"):
                now = kind_counts(scan, kind)
                if now is None:
                    return {}
                # an arm's own counts are what moved since the arm before it, or since the restart
                then = before.get((member, kind), (0,) * len(now))
                figures[(entry["id"], member, kind)] = tuple(n - t for n, t in zip(now, then))
                before[(member, kind)] = now
    return figures


# a capture's parts as one tuple a capture's samples subtract: the captures summed, each part's wall,
# the keys planned, the delta entries and runs its view took and the view's delta and runs times (item
# 353's fix), for every capture and for those past 100 us on their own cpu, then the long ones by the
# part that held them the most, and those its view held by the part of the view that held them
PART_FIGURES = ("captures", "keys_ns", "plan_ns", "view_ns", "rest_ns", "planned", "delta_entries", "runs",
                "view_delta_ns", "view_runs_ns")
SET = len(PART_FIGURES)
PARTS = ("keys", "plan", "view", "rest")


def part_counts(scan):
    """A class's captures by their parts as a tuple, or None if they were counted before item 353's fix"""
    figures = scan.get("capture_parts")
    if not figures:
        return None
    every = figures.get("all") or {}
    long = figures.get("long") or {}
    return (tuple(every.get(name, 0) for name in PART_FIGURES)
            + tuple(long.get(name, 0) for name in PART_FIGURES)
            + tuple((figures.get("held_by") or [0, 0, 0, 0])[:4])
            + tuple((figures.get("view_held_by") or [0, 0, 0])[:3]))


def parts_by_arm(capture):
    """Each arm's scan captures by their parts a member, from the counts since the restart its last
    sample carries less the arm's before it, or nothing for a capture from before them"""
    figures, before = {}, {}
    for entry in capture.get("arms", []):
        run = entry["runs"][0] if entry.get("runs") else {}
        samples = [sample for sample in run.get("server_series") or [] if sample.get("scans")]
        if not samples:
            continue
        for member, counts in samples[-1]["scans"].items():
            scan = (counts or {}).get("scan") or {}
            now = part_counts(scan)
            if now is None:
                return {}
            # an arm's own counts are what moved since the arm before it, or since the restart
            then = before.get(member, (0,) * len(now))
            figures[(entry["id"], member)] = tuple(n - t for n, t in zip(now, then))
            before[member] = now
    return figures


def mean_parts(counts, offset):
    """A run of captures' mean parts as `keys/plan/view/rest us`, and the keys planned, delta entries
    and runs with the view's delta/runs/rest us, from a parts tuple at an offset: 0 for every capture,
    SET for the long ones"""
    captures, keys, plan, view, rest, planned, delta, runs, view_delta, view_runs = counts[offset:offset + SET]
    if not captures:
        return "-", "-"
    times = "/".join(f"{part / captures / 1000:.1f}" for part in (keys, plan, view, rest))
    took = f"{planned / captures:.1f} planned, {delta / captures:.0f} delta, {runs / captures:.1f} runs"
    # the view's own parts where the capture timed them
    if view_delta or view_runs:
        view_rest = max(view - view_delta - view_runs, 0)
        took += ", view " + "/".join(f"{part / captures / 1000:.1f}" for part in (view_delta, view_runs, view_rest)) + " us"
    return f"{times} us", took


def held_cells(counts):
    """The long captures by the part that held them, `keys/plan/view/rest`, with those the view held by
    its delta/runs/rest where any was counted"""
    held = "/".join(str(count) for count in counts[2 * SET:2 * SET + 4])
    view = counts[2 * SET + 4:2 * SET + 7]
    return f"{held} ({'/'.join(str(count) for count in view)})" if any(view) else held


def mean_step(counts, offset):
    """A run of steps' mean as `wall/cpu us, keys, rows, bytes`, from a kind's tuple at an offset:
    0 for every step, 6 for the long ones"""
    steps, wall, cpu, keys, rows, bytes_ = counts[offset:offset + 6]
    if not steps:
        return "-"
    return f"{wall / steps / 1000:.0f}/{cpu / steps / 1000:.0f} us, {keys / steps:.0f} keys, {rows / steps:.0f} rows, {bytes_ / steps / 1024:.1f} KiB"


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
    overwrite the rows the arm itself wrote, resident. The cell's inserts fell 8 to 29 s before each
    of the first exit's wraps, which is item 351's and not the wrap's (`insert_fall`)"""
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


def insert_fall(run, share=0.75, hold=30, least=10):
    """Where an arm's inserts fell and stayed down inside its measured window (item 351): the first
    second past its measured window's first fifty from which every five seconds' mean is under a share
    of their median, for `hold` seconds or to the arm's end and at least `least` of them, and the
    inserts taken by then; none if they held their rate. An arm's last few seconds, which every arm's
    wind-down dips in, never read as a fall. Since the rounds again warm each arm 100 s, past the lab's
    970 EVOs' change of flush, the window starts there; at a warmup of 10 s it is seconds 10 to 60, as
    it was"""
    series = run.get("series") or []
    rates = [(((second.get("summary") or {}).get("insert") or {}).get("ok") or 0) for second in series]
    ats = [second.get("at") or 0 for second in series]
    # the measured window's first second, the warmup's end
    begun = next((at for second, at in zip(series, ats) if second.get("phase") == "measure"), 10)
    early = [rate for at, rate in zip(ats, rates) if begun <= at < begun + 50]
    if len(early) < 30 or len(rates) < begun + 60:
        return None
    floor = share * statistics.median(early)
    means = [sum(rates[i:i + 5]) / 5 for i in range(len(rates) - 4)]
    for start in range(len(means)):
        if ats[start] < begun + 50:
            continue
        held = means[start:start + hold]
        if len(held) >= least and all(mean < floor for mean in held):
            return series[start].get("at"), sum(rates[:start])
    return None


def put_cell_feed(rows, key, round_, capture, arm_id):
    """A capture's cell feed and memory, and where its arm's inserts fell, as rows of a table"""
    wrapped, inserting, longest, pool = cell_feed(capture)
    if inserting:
        put(rows, key, "cell arms whose feed wrapped", round_, wrapped)
        put(rows, key, "cell inserts, its longest arm", round_, longest)
    put(rows, key, "cell insert pool, rows", round_, pool)
    put(rows, key, "members' peak resident over the node's memory", round_, peak_memory(capture))
    run = arm(capture, arm_id)
    if run and run.get("series"):
        fell = insert_fall(run)
        put(rows, key, "cell arms whose inserts fell", round_, 0 if fell is None else 1)
        if fell is not None:
            put(rows, key, "cell inserts fell, seconds into the arm", round_, fell[0])
            put(rows, key, "cell inserts fell, inserts by then", round_, fell[1])


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
        put_cell_feed(rows, key, round_, capture, "rw50/b1/scan")
        steps = scan_steps(capture)
        if steps:
            put(rows, key, "scan steps past 100 us in the capture", round_, steps[0][0])
            put(rows, key, "scan steps in the capture", round_, steps[2])
            put(rows, key, "scan longest step us in the capture", round_, steps[1])
        # and by kind, on their own cpu and by the wall alone, where the capture counts kinds (F130)
        kinds = scan_step_kinds(capture)
        if kinds:
            past = {kind: counts[12] for kind, counts in kinds.items()}
            own = {kind: counts[13] for kind, counts in kinds.items()}
            put(rows, key, "scan steps past 100 us on their own cpu in the capture", round_, sum(own.values()))
            put(rows, key, "scan steps past 100 us by the wall alone in the capture", round_,
                sum(past.values()) - sum(own.values()))
            for kind, name in (("captures", "captures"), ("encodes", "encode steps")):
                put(rows, key, f"scan {name} past 100 us in the capture, by the wall", round_, past[kind])
                put(rows, key, f"scan {name} past 100 us in the capture, on their own cpu", round_, own[kind])
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
    # and by kind where the captures count kinds (F130): what held a long step, a capture or an
    # encode step, on its own cpu or off it, and what it held beside every step of its kind
    kinds = {(round_, which): kinds_by_arm(capture) for (round_, which), capture in sorted(rounds.items()) if which in ("big", "keyword")}
    kernel = {key: carries_kernel_events(capture) for key, capture in rounds.items()}
    if any(kinds.values()):
        print("\n### Scan steps by kind, by arm and member\n")
        print("Past 100 us by the wall and on the step's own cpu; of those on their own cpu, the steps that took a page "
              "fault, and of those by the wall, the steps whose thread left its cpu inside them (item 353, `-` in a capture "
              "from before it); a mean step is its wall/own cpu, the keys it looked at, "
              "the rows and bytes it copied, for every step of the kind and for those past 100 us by the wall.\n")
        print("| Round | Arm | Member | Kind | Steps | Past 100 us, wall / own cpu | Faulted / switched | Mean step | Mean step past 100 us |")
        print("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
        for (round_, which), figures in sorted(kinds.items()):
            for (arm_id, member, kind), counts in sorted(figures.items()):
                if not counts[0]:
                    continue
                events = f"{counts[14]} / {counts[15]}" if kernel.get((round_, which)) else "-"
                print(f"| {round_} | {which} {arm_id} | {member} | {kind} | {counts[0]} | {counts[12]} / {counts[13]} | "
                      f"{events} | {mean_step(counts, 0)} | {mean_step(counts, 6)} |")
    # and captures by their parts where the captures count them (item 353's fix): the part that held
    # each capture past 100 us on its own cpu, and what every capture's and those captures' parts took
    parts = {(round_, which): parts_by_arm(capture) for (round_, which), capture in sorted(rounds.items()) if which in ("big", "keyword")}
    if any(parts.values()):
        print("\n### Scan captures by their parts, by arm and member\n")
        print("A capture's parts by the wall: its keys looked at, kept, selected and sorted; its plan of each key; its "
              "archive's view; and the rest of its step. *Long* is a capture past 100 us on its own cpu, and *held by* "
              "counts those by the part that took the most of each, those its view held by its view's delta, runs "
              "and rest; a mean is keys/plan/view/rest, then the keys it planned, the archive map's delta entries its "
              "view copied and the runs it took a cursor over, and where timed the view's delta/runs/rest.\n")
        print("| Round | Arm | Member | Captures | Long | Held by keys/plan/view/rest (view's delta/runs/rest) | Mean capture | Its plan and view | "
              "Mean long capture | Its plan and view |")
        print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
        for (round_, which), figures in sorted(parts.items()):
            for (arm_id, member), counts in sorted(figures.items()):
                if not counts[0]:
                    continue
                every, every_took = mean_parts(counts, 0)
                long, long_took = mean_parts(counts, SET)
                print(f"| {round_} | {which} {arm_id} | {member} | {counts[0]} | {counts[SET]} | {held_cells(counts)} | "
                      f"{every} | {every_took} | {long} | {long_took} |")
        # and every round's captures of one table scanned together, a member's: what held its long
        # captures; a scan alone names its table, and the cell's arm scans its capture's
        print("\n### Long scan captures by the part that held them, every round\n")
        print("| Table scanned | Member | Captures | Long | Held by keys/plan/view/rest (view's delta/runs/rest) | Mean long capture | Its plan and view |")
        print("| --- | --- | --- | --- | --- | --- | --- |")
        summed = {}
        for (round_, which), figures in parts.items():
            for (arm_id, member), counts in figures.items():
                pieces = arm_id.split("/")
                table = pieces[1] if pieces[0] == "scan" and len(pieces) > 1 else {"big": "Big", "keyword": "Keyword"}[which]
                key = (table, member)
                summed[key] = tuple(a + b for a, b in zip(summed.get(key, (0,) * len(counts)), counts))
        for (which, member), counts in sorted(summed.items()):
            if not counts[0]:
                continue
            long, long_took = mean_parts(counts, SET)
            print(f"| {which} | {member} | {counts[0]} | {counts[SET]} | {held_cells(counts)} | {long} | {long_took} |")
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
        # whether any round's cell wrapped beside this table's scan: a cell that never wraps has its
        # per-second view over the whole scan, not cut at a wrap
        wrapped_any = any(rows.get((f"cell beside {table}", "cell arms whose feed wrapped"), {}).values())
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
                view = "before the cell's wrap" if wrapped_any else "a second at a time"
                print(f"| ({view}) the cell's {kind} p99 a second beside a scan of {table} | {bw:.2f} -> {dw:.2f} ms | {'yes' if budget(bw, dw) else 'no'} |")
            # the control beside it, not a clause: the cell alone's p99 a second over the same seconds
            # against the scan arms' own, a difference only where their ranges are disjoint
            alone = rows.get(("cell alone, the control", f"over {table}'s scan: {kind} p99 a second, during / before"), {})
            beside = rows.get((f"cell beside {table}", f"{kind} p99 a second, during before the wrap / before"), {})
            if alone and beside:
                apart = ranges_disjoint(alone, beside)
                verdict = "-" if apart is None else ("disjoint" if apart else "overlap")
                print(f"| (the control, not a clause) the cell's {kind} p99 a second, during / before: alone over {table}'s scan seconds, "
                      f"then beside the scan | {cell(alone)}, {cell(beside)} | {verdict} |")
        # W7's insert lines above are read as defined only where no round's cell wrapped inside an arm
        wraps = rows.get((f"cell beside {table}", "cell arms whose feed wrapped"), {})
        if wraps:
            rounds_wrapped = sum(1 for value in wraps.values() if value)
            print(f"| the cell's feed never wrapped inside an arm, the {table} captures | {rounds_wrapped} of {len(wraps)} rounds wrapped | {'yes' if rounds_wrapped == 0 else 'no'} |")
        # and only where its inserts held their rate over the arm, scan or not (item 351)
        falls = rows.get((f"cell beside {table}", "cell arms whose inserts fell"), {})
        if falls:
            rounds_fell = sum(1 for value in falls.values() if value)
            print(f"| the cell's inserts held their rate inside an arm, the {table} captures | {rounds_fell} of {len(falls)} rounds fell | {'yes' if rounds_fell == 0 else 'no'} |")
        # every round's own, not a median: one pass or one long step in any round misses the clause
        evictions = rows.get((f"cell beside {table}", "Resident evictions in the capture"), {})
        if evictions:
            most = max(evictions.values())
            print(f"| no eviction pass by the process's memory, the {table} captures | {most:g} at most | {'yes' if most == 0 else 'no'} |")
        # the step clause on a step's own work where the captures count kinds, a step past it by the
        # wall alone named as O147's (decision 2, F130); by the wall on captures from before F130
        own = rows.get((f"cell beside {table}", "scan steps past 100 us on their own cpu in the capture"), {})
        steps = rows.get((f"cell beside {table}", "scan steps past 100 us in the capture"), {})
        if own:
            most = max(own.values())
            print(f"| no scan step over 100 us on its own cpu, the {table} captures | {most:g} at most | {'yes' if most == 0 else 'no'} |")
            alone = rows.get((f"cell beside {table}", "scan steps past 100 us by the wall alone in the capture"), {})
            print(f"| (O147's, not judged) scan steps over 100 us by the wall alone, the {table} captures | {max(alone.values()):g} at most | |")
        elif steps:
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
