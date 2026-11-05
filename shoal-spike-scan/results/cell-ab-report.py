"""The cell alone, side A against side B, from m23-lab.sh's CELL_ONLY captures

Reads every `cell-r<round>-<side>-cell.json` slim capture under a results directory and the
`cell-r<round>-<side>-mem/` beside it (its side, its nodes' environment and resident sets), and
prints each figure's median and range for each side, the ratio of the medians, B over A, and
whether the ranges are disjoint, which is all a difference is judged by.

    python3 -I shoal-spike-scan/results/cell-ab-report.py shoal-spike-scan/results/found-<item>/cell
"""

import json
import os
import re
import statistics
import sys


def median_range(values):
    """A list's median and range, or None for an empty list

    Arguments:
        values: the values
    """
    values = [value for value in values if value is not None]
    if not values:
        return None
    return statistics.median(values), min(values), max(values)


def disjoint(one, other):
    """Whether two ranges do not overlap

    Arguments:
        one: a (median, low, high)
        other: another
    """
    return one[2] < other[1] or other[2] < one[1]


def latency(measured, kind, field):
    """One latency figure of an op kind in an arm's measured window

    Arguments:
        measured: the window
        kind: read or insert
        field: p50_ms, p99_ms, p999_ms or per_sec
    """
    figures = (measured or {}).get(kind) or {}
    if field == "per_sec":
        return figures.get("per_sec")
    return (figures.get("latency") or {}).get(field)


def resident(mem_dir):
    """Each host's resident set and huge pages, in MiB, by when it was read

    Arguments:
        mem_dir: the side-round's memory directory
    """
    sets = {}
    path = os.path.join(mem_dir, "rss.txt")
    if not os.path.exists(path):
        return sets
    for line in open(path):
        fields = line.split()
        if len(fields) < 3:
            continue
        when, host = fields[0], fields[1]
        values = dict(re.findall(r"(\w+):(\d+)", " ".join(fields[2:])))
        sets[(when, host)] = {key: int(value) / 1024 for key, value in values.items()}
    return sets


def peak_resident(capture):
    """Each member's largest resident set over every arm's seconds, in MiB

    Arguments:
        capture: the slim capture
    """
    peaks = {}
    for arm in capture.get("arms", []):
        for run in arm.get("runs", []):
            for second in run.get("server_series") or []:
                for host, memory in (second.get("memory") or {}).items():
                    value = (memory or {}).get("resident_bytes")
                    if value:
                        peaks[host] = max(peaks.get(host, 0), value / (1 << 20))
    return peaks


def main(root):
    """Print the report for one results directory

    Arguments:
        root: where the slim captures and memory directories are
    """
    rows = {}
    sides = {}
    for name in sorted(os.listdir(root)):
        found = re.match(r"cell-r(\d+)-([AB])-cell\.json$", name)
        if not found:
            continue
        round_, side = int(found.group(1)), found.group(2)
        capture = json.load(open(os.path.join(root, name)))
        mem_dir = os.path.join(root, "cell-r%d-%s-mem" % (round_, side))
        side_file = os.path.join(mem_dir, "side.txt")
        if os.path.exists(side_file):
            sides.setdefault(side, open(side_file).read().strip())
        figures = rows.setdefault(side, {})
        for arm in capture.get("arms", []):
            workload = arm.get("workload") or arm["id"].split("/")[0]
            for run in arm.get("runs", []):
                measured = run.get("measured") or {}
                for kind in ("read", "insert"):
                    for field in ("per_sec", "p50_ms", "p99_ms", "p999_ms"):
                        value = latency(measured, kind, field)
                        if value is not None:
                            figures.setdefault("%s %s %s" % (workload, kind, field), []).append(value)
        for host, peak in peak_resident(capture).items():
            figures.setdefault("peak resident MiB %s" % host, []).append(peak)
        for (when, host), values in resident(mem_dir).items():
            for key in ("VmRSS", "RssAnon", "AnonHugePages"):
                if key in values:
                    figures.setdefault("%s %s MiB %s" % (key, when, host), []).append(values[key])
    if not rows:
        sys.exit("no cell captures under %s" % root)
    for side in sorted(sides):
        print("side %s" % sides[side])
    print()
    print("| figure | A median [range] | B median [range] | B/A | disjoint |")
    print("| --- | --- | --- | --- | --- |")
    names = sorted(set(rows.get("A", {})) | set(rows.get("B", {})))
    for name in names:
        a = median_range(rows.get("A", {}).get(name, []))
        b = median_range(rows.get("B", {}).get(name, []))
        cell = lambda figure: "-" if figure is None else "%.3f [%.3f, %.3f]" % figure
        ratio = "-" if not a or not b or not a[0] else "%.3f" % (b[0] / a[0])
        apart = "-" if not a or not b else ("yes" if disjoint(a, b) else "no")
        print("| %s | %s | %s | %s | %s |" % (name, cell(a), cell(b), ratio, apart))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "shoal-spike-scan/results/cell")
