"""X20's tables from its rounds: ScyllaDB's foreground beside a tablet-era full scan

    python3 -I shoal-spike/results/x20-report.py shoal-spike/results/x20-runs

Every round is `r<round>-<select>.json`, which `x20-lab.sh run` names: `select` is `keys`, the scan
carrying its keys alone as X16's carried keys and counts, or `rows`, every row's bytes to the client.
For each round this prints the scan's own figures, then the cell's read and insert p50 and p99
before and during the scan, its rate during over before, and the p99's ratio; then each selection's
median and range over its rounds.
"""

import json
import pathlib
import re
import statistics
import sys

# a round's name: its number and what its scan carried
NAME = re.compile(r"^r(\d+)-(keys|rows)\.json$")


def load(root):
    """Every round under a directory, by what its scan carried and its number"""
    rounds = {}
    for path in sorted(pathlib.Path(root).iterdir()):
        match = NAME.match(path.name)
        if not match:
            continue
        with open(path, encoding="utf-8") as handle:
            rounds[(match.group(2), int(match.group(1)))] = json.load(handle)
    return rounds


def spread(values, digits):
    """A median and its range, as the X pages print them"""
    return f"{statistics.median(values):.{digits}f} [{min(values):.{digits}f}, {max(values):.{digits}f}]"


def main():
    """Print the per round tables and each selection's summary"""
    rounds = load(sys.argv[1] if len(sys.argv) > 1 else "shoal-spike/results/x20-runs")
    if not rounds:
        sys.exit("no rounds found")
    # the scan, a round a row
    print("| Round | Carried | The scan | Rows a second | MiB a second carried | A tablet, p50 / max | Errors |")
    print("| --- | --- | --- | --- | --- | --- | --- |")
    for (select, number), report in sorted(rounds.items()):
        scan = report["scan"]
        tablet = scan["tablet"]
        print(
            f"| {number} | {select} | {scan['secs']:.2f} s | {scan['rows_per_sec']:,.0f} | {scan['mib_per_sec']:.1f} "
            f"| {tablet['p50_ms'] / 1000:.2f} / {tablet['max_ms'] / 1000:.2f} s | {scan['errors']} |"
        )
    print()
    # the cell, a round and a kind a row
    print("| Round | Carried | Kind | p50 before → during | p99 before → during | Rate during over before | p99 during over before |")
    print("| --- | --- | --- | --- | --- | --- | --- |")
    for (select, number), report in sorted(rounds.items()):
        for kind in ("read", "insert"):
            before, during = report["windows"]["before"][kind], report["windows"]["during"][kind]
            print(
                f"| {number} | {select} | {kind} | {before['p50_ms']:.2f} → {during['p50_ms']:.2f} ms "
                f"| {before['p99_ms']:.2f} → {during['p99_ms']:.2f} ms | {during['per_sec'] / before['per_sec']:.2f} "
                f"| {report['ratio'][kind]:.2f} |"
            )
    print()
    # each selection's medians and ranges over its rounds
    print("| Carried | Rounds | The scan | Read p99 during over before | Insert p99 during over before | The cell's rate during over before |")
    print("| --- | --- | --- | --- | --- | --- |")
    for select in ("keys", "rows"):
        chosen = [report for (kind, _), report in sorted(rounds.items()) if kind == select]
        if not chosen:
            continue
        secs = [report["scan"]["secs"] for report in chosen]
        ratios = {kind: [report["ratio"][kind] for report in chosen] for kind in ("read", "insert")}
        # the cell's operations a second during the scan over before it, both kinds together
        rates = [
            sum(report["windows"]["during"][kind]["per_sec"] for kind in ("read", "insert"))
            / sum(report["windows"]["before"][kind]["per_sec"] for kind in ("read", "insert"))
            for report in chosen
        ]
        print(
            f"| {select} | {len(chosen)} | {spread(secs, 2)} s | {spread(ratios['read'], 2)} "
            f"| {spread(ratios['insert'], 2)} | {spread(rates, 2)} |"
        )


if __name__ == "__main__":
    main()
