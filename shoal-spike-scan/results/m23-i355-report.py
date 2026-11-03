"""Item 355's run: a scan of Keyword beside the cell, its pages served in turn at their device

Every Keyword capture of M23's rounds (`m23-r<n>-keyword`, the cell's rw50 beside a full scan of Keyword)
under the directories given is read for what item 355 judged: the pages the admin walk's waiter gave up
for want of progress (`Timeout`), the tablets the scan gave up (`GaveUp`), the pages shed, how long a page
took at its 99th percentile and at its longest, the longest a tablet went unanswered while streams asked
it (zero in a capture from before the fix), the pages every member let go once given up (zero before),
how long a whole scan took, and the cell's insert and read p99 while the scan ran
(docs/src/appendix/resolved/scan-pages-served-in-turn.md).

    python3 -I shoal-spike-scan/results/m23-i355-report.py --run target/lab/m23-i355/captures \
        --corpus 'target/lab/m23*/captures'
"""

import glob
import json
import os
import sys


def keyword_captures(pattern):
    """Every Keyword capture under the directories a pattern names, a whole one's bench.json or a slim one"""
    found = []
    for root in sorted(glob.glob(pattern)):
        for name in sorted(os.listdir(root)):
            full = os.path.join(root, name)
            if name.startswith("m23-r") and name.endswith("-keyword") and os.path.isfile(os.path.join(full, "bench.json")):
                found.append(os.path.join(full, "bench.json"))
            elif name.startswith("m23-r") and name.endswith("-keyword.json"):
                found.append(full)
    return found


def given_up(run):
    """The pages every member let go once their waiter gave them up, over the capture's arm"""
    series = run.get("server_series") or []
    if not series:
        return 0
    first, last = series[0].get("scans") or {}, series[-1].get("scans") or {}
    return sum(
        ((last.get(member) or {}).get("scan") or {}).get("abandoned", 0)
        - ((first.get(member) or {}).get("scan") or {}).get("abandoned", 0)
        for member in last
    )


def row(path):
    """One capture's figures, or none if it has no scan arm"""
    capture = json.load(open(path))
    for arm in capture.get("arms", []):
        if "scan" not in arm["id"]:
            continue
        for run in arm["runs"]:
            measured = run.get("measured") or {}
            scan = measured.get("scan") or {}
            kind = (measured.get("kinds") or {}).get("scan") or {}
            latency = kind.get("latency") or {}
            during = ((run.get("event") or {}).get("windows") or {}).get("during") or {}
            whole = ((during.get("scan") or {}).get("whole") or {}).get("p50_ms", 0)
            parts = path.split(os.sep)
            label = f"{parts[-4] if parts[-1] == 'bench.json' else parts[-3]}/{os.path.basename(os.path.dirname(path)) if parts[-1] == 'bench.json' else os.path.basename(path)}"
            return {
                "capture": label,
                "timeouts": (scan.get("waits") or {}).get("Timeout", 0),
                "gave_up": (kind.get("errors") or {}).get("GaveUp", 0),
                "shed": scan.get("shed", 0),
                "p99_s": latency.get("p99_ms", 0) / 1000,
                "longest_s": latency.get("max_ms", 0) / 1000,
                "unanswered_s": (scan.get("longest_unanswered_us") or 0) / 1e6,
                "abandoned": given_up(run),
                "whole_s": whole / 1000,
                "insert_p99_ms": ((during.get("insert") or {}).get("latency") or {}).get("p99_ms", 0),
                "read_p99_ms": ((during.get("read") or {}).get("latency") or {}).get("p99_ms", 0),
            }
    return None


def table(rows):
    """The rows as a markdown table"""
    print("| Capture | Timeouts | Given up | Shed | Page p99 s | Page longest s | Longest unanswered s | Pages let go | Whole scan s | Cell insert p99 ms | Cell read p99 ms |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for r in rows:
        print(
            f"| {r['capture']} | {r['timeouts']} | {r['gave_up']} | {r['shed']:,} | {r['p99_s']:.1f} | {r['longest_s']:.1f} | "
            f"{r['unanswered_s']:.1f} | {r['abandoned']} | {r['whole_s']:.0f} | {r['insert_p99_ms']:.1f} | {r['read_p99_ms']:.1f} |"
        )


def spread(rows, key):
    """The least and most of one figure over some rows"""
    values = [r[key] for r in rows]
    return (min(values), max(values)) if values else (0, 0)


def main(argv):
    """The run's captures and the corpus's, each a table, then their spreads side by side"""
    groups = {"run": [], "corpus": []}
    current = None
    for arg in argv:
        if arg in ("--run", "--corpus"):
            current = arg[2:]
        elif current:
            groups[current].extend(keyword_captures(arg))
    if not groups["run"]:
        sys.exit(__doc__)
    run = [r for r in map(row, groups["run"]) if r]
    # a quick capture scans a tenth of the data, which no full capture is read against
    corpus = [
        r
        for r in map(row, (path for path in groups["corpus"] if "-quick" not in path))
        if r and r["capture"] not in {x["capture"] for x in run}
    ]
    print("## This run\n")
    table(run)
    if corpus:
        print("\n## The corpus, before the fix\n")
        table(corpus)
    print("\n## Spreads\n")
    print("| Figure | This run | The corpus |")
    print("| --- | --- | --- |")
    for key, name in [
        ("timeouts", "Timeouts a capture"),
        ("gave_up", "Tablets given up a capture"),
        ("shed", "Pages shed a capture"),
        ("p99_s", "Page p99, s"),
        ("longest_s", "Page longest, s"),
        ("unanswered_s", "Longest unanswered, s"),
        ("abandoned", "Pages let go a capture"),
        ("whole_s", "Whole scan, s"),
        ("insert_p99_ms", "Cell insert p99 during, ms"),
        ("read_p99_ms", "Cell read p99 during, ms"),
    ]:
        low, high = spread(run, key)
        clow, chigh = spread(corpus, key)
        print(f"| {name} | {low:,.1f} to {high:,.1f} | {clow:,.1f} to {chigh:,.1f} |")


if __name__ == "__main__":
    main(sys.argv[1:])
