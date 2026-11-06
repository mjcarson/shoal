"""O156 on a lab host: a sorted table's reads keyed by a padded string against an integer

Reads the runs `o156/host.sh` wrote, one `shoal-workload run` measurement a run named
`r<round>-<form>-<access>-<residency>.json`, and prints, for each access and residency, each form's
median and range over the rounds of its gets' p50, p99 and mean, the integer's median over the
padded string's, and whether the two ranges are disjoint, which is the only way a difference is a
result.

    python3 -I shoal-spike/results/o156-report.py shoal-spike/results/o156/runs
"""

import json
import os
import re
import sys

# the summary figures a run's gets carry that the report reads
FIGURES = ("p50", "p99", "avg")


def micros(duration):
    """A serialized duration in microseconds

    Arguments:
        duration: a `{"secs", "nanos"}` pair
    """
    return duration["secs"] * 1e6 + duration["nanos"] / 1e3


def runs(root):
    """Every run's gets by access, residency, form and figure, a value a round

    Arguments:
        root: the directory of runs
    """
    out = {}
    for name in sorted(os.listdir(root)):
        found = re.match(r"r(\d+)-(padded|u64)-(get|range|walk)-(resident|evicted)\.json$", name)
        if not found:
            continue
        _, form, access, residency = found.groups()
        measured = json.load(open(os.path.join(root, name)))
        for workload in measured["workloads"].values():
            gets = workload["ops"]["get"]
            for figure in FIGURES:
                out.setdefault((access, residency), {}).setdefault(form, {}).setdefault(figure, []).append(
                    micros(gets[figure])
                )
            out[(access, residency)][form].setdefault("count", []).append(gets["count"])
            out[(access, residency)][form].setdefault("rows", []).append(workload["counters"].get("retrieved", 0))
    return out


def median(values):
    """The median of a list, the lower of the middle two

    Arguments:
        values: the values
    """
    ordered = sorted(values)
    return ordered[(len(ordered) - 1) // 2]


def main(root):
    """Print the table

    Arguments:
        root: the directory of runs
    """
    table = runs(root)
    print("| access | residency | figure | padded median [range] us | u64 median [range] us | u64/padded | disjoint | rounds |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- |")
    for access in ("get", "range", "walk"):
        for residency in ("resident", "evicted"):
            forms = table.get((access, residency))
            if not forms or "padded" not in forms or "u64" not in forms:
                continue
            for figure in FIGURES:
                padded, integer = forms["padded"][figure], forms["u64"][figure]
                disjoint = max(padded) < min(integer) or max(integer) < min(padded)
                print(
                    "| %s | %s | %s | %.1f [%.1f, %.1f] | %.1f [%.1f, %.1f] | %.3f | %s | %d |"
                    % (
                        access,
                        residency,
                        figure,
                        median(padded),
                        min(padded),
                        max(padded),
                        median(integer),
                        min(integer),
                        max(integer),
                        median(integer) / median(padded),
                        "yes" if disjoint else "no",
                        min(len(padded), len(integer)),
                    )
                )
    print()
    print("rows a run retrieved, padded and u64, which must match arm by arm:")
    for (access, residency), forms in sorted(table.items()):
        print("  %s/%s: %s / %s" % (access, residency, sorted(set(forms["padded"]["rows"])), sorted(set(forms["u64"]["rows"]))))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "shoal-spike/results/o156/runs")
