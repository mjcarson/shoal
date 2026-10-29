#!/usr/bin/env python3
"""F88 on the lab: each placement group's move, its begin to its switch, and its slow control proposals

Reads a node's journal on stdin: `journalctl -u shoal-<cluster> --since .. --until .. | python3
m16-moves.py [target]`, counting only the moves to that target generation when one is named.
"""
import re
import statistics
import sys
from datetime import datetime

STAMP = re.compile(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+)Z\s+(INFO|WARN)")
KEY = re.compile(r"key=\((\d+), PlacementGroup \{ tablet: (\d+), sub: (\d+) \}\)")
MS = re.compile(r"kind=(\w+).*? ms=(\d+)")


def when(line):
    """The node's own stamp of a line"""
    found = STAMP.search(line)
    return datetime.fromisoformat(found.group(1)[:26]) if found else None


def pct(values, p):
    """The p-th percentile of sorted values"""
    return values[min(len(values) - 1, int(len(values) * p / 100))]


WANT = sys.argv[1] if len(sys.argv) > 1 else None
TARGET = re.compile(r"target=(\d+)")


def main():
    """Read the journal and print the moves' timings and the slow proposals"""
    # every move begun, by its group, until its switch is read
    begun, took, slow, gave_up = {}, [], {}, 0
    first, last = None, None
    for line in sys.stdin:
        at = when(line)
        if at is None:
            continue
        key = KEY.search(line)
        if "began its move" in line and key:
            target = TARGET.search(line)
            if WANT and (not target or target.group(1) != WANT):
                continue
            begun[key.groups()] = at
            first = first or at
        elif "switched to its new generation" in line and key:
            start = begun.pop(key.groups(), None)
            last = at
            if start:
                took.append((at - start).total_seconds())
        elif "was given up" in line:
            gave_up += 1
        elif "was slow" in line:
            found = MS.search(line)
            if found:
                slow.setdefault(found.group(1), []).append(int(found.group(2)))
    took.sort()
    if took:
        span = (last - first).total_seconds()
        print(f"switched {len(took)} in {span:.0f}s ({len(took) / span:.1f}/s), begin to switch "
              f"p50 {pct(took, 50):.2f}s p90 {pct(took, 90):.2f}s p99 {pct(took, 99):.2f}s max {took[-1]:.2f}s; "
              f"given up {gave_up}, never switched {len(begun)}")
    for kind, values in sorted(slow.items()):
        values.sort()
        print(f"  slow {kind}: {len(values)}, p50 {pct(values, 50)} ms p99 {pct(values, 99)} ms max {values[-1]} ms")


if __name__ == "__main__":
    main()
