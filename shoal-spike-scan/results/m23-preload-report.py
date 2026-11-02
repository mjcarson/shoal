"""Item 357's run: what shed a preload on the lab, from the captures of `LOAD_ONLY=1 m23-lab.sh start`

Every capture named m23-load-r<n> (or any capture with a preload) in the directories given is read for
its preload: how long it took and how many rows it loaded, how many inserts it sent again and with
which codes, and every member's writes shed since it started, by what shed each, which a cluster
bootstrapped for the preload makes the preload's own
(docs/src/appendix/resolved/preload-sheds-waited-out.md). A capture whose preload failed is printed
with its error, which names the same figures.

    python3 -I shoal-spike-scan/results/m23-preload-report.py shoal-spike-scan/results/m23-i357/m23
"""

import json
import os
import sys


def captures(paths):
    """Every capture under the paths given, a directory's files and its captures' bench.json, by name"""
    found = []
    for path in paths:
        if os.path.isdir(path):
            for name in sorted(os.listdir(path)):
                full = os.path.join(path, name)
                # a slimmed capture is a file, a whole one a directory holding bench.json
                if name.endswith(".json"):
                    found.append(full)
                elif os.path.isfile(os.path.join(full, "bench.json")):
                    found.append(os.path.join(full, "bench.json"))
        else:
            found.append(path)
    return found


def named(counts):
    """A map of counts as `name count` pairs, the largest first, or a dash for none"""
    pairs = sorted(((count, name) for name, count in (counts or {}).items() if count), reverse=True)
    return ", ".join(f"{name} {count:,}" for count, name in pairs) or "-"


def main(paths):
    """Print a table of every preload found, and the sheds every member counted across them"""
    rows = []
    every = {}
    for path in captures(paths):
        with open(path) as file:
            capture = json.load(file)
        label = capture.get("label", os.path.basename(path))
        preload = capture.get("preload")
        sheds = capture.get("preload_sheds") or {}
        if preload is None and not capture.get("error"):
            continue
        insert = (preload or {}).get("insert", {})
        rows.append(
            (
                label,
                f"{preload['secs']:.0f}" if preload else "-",
                f"{insert.get('ok', 0):,}" if preload else "-",
                f"{insert.get('retried', 0):,}" if preload else "-",
                named(insert.get("retried_by")),
                "; ".join(f"{member}: {named(figures)}" for member, figures in sorted(sheds.items())) or "none counted",
                capture.get("error") or "",
            )
        )
        # every member's sheds summed over the preloads, by cause
        for member, figures in sheds.items():
            total = every.setdefault(member, {})
            for cause, count in figures.items():
                total[cause] = total.get(cause, 0) + count
    print("| Preload | Seconds | Rows | Sent again | Sent again by code | Each member's write sheds by cause | Error |")
    print("| --- | --- | --- | --- | --- | --- | --- |")
    for row in rows:
        print("| " + " | ".join(row) + " |")
    print()
    print("| Member | Write sheds over every preload, by cause |")
    print("| --- | --- |")
    for member in sorted(every):
        print(f"| {member} | {named(every[member])} |")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    main(sys.argv[1:])
