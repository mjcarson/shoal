"""O140's attribution: what held each member's shard loops while the cell ran, from a `loop-profile` build

Reads m23-lab.sh's CELL_ONLY side-rounds: each `cell-r<round>-<side>-cell/bench.json` capture under
a captures directory and the `cell-r<round>-<side>-mem/stats.json` beside it, `stats --json --watch 1`
of a node built with `shoal-core`'s `loop-profile`. For each arm's measured window and each member it
prints the member's own get p99 and, from every shard's loop profile summed, how long a client's
bundle and a routed query waited to be taken and what held the loop, each kind's count, mean, p99
and share of the window, taken as the change between the window's first and last report.

    python3 -I shoal-spike-scan/results/loop-profile-report.py target/lab/found/o140/captures
"""

import datetime
import json
import os
import re
import sys

# the log2 buckets of microseconds a loop histogram keeps
BUCKETS = 24


def stats_stream(path):
    """Every member's own figures by hostname and epoch second, from a watch file

    Arguments:
        path: the `stats --json --watch 1` output, plain or gzipped
    """
    if path.endswith(".gz"):
        import gzip
        text = gzip.open(path, "rt").read()
    else:
        text = open(path).read()
    decoder = json.JSONDecoder()
    out = {}
    index = 0
    while True:
        index = text.find("{", index)
        if index < 0:
            break
        try:
            view, index = decoder.raw_decode(text, index)
        except json.JSONDecodeError:
            index += 1
            continue
        for member in view.get("members") or []:
            stats = member.get("stats") or {}
            name = stats.get("hostname")
            if name and stats.get("at_ms"):
                out.setdefault(name, {})[int(stats["at_ms"] / 1000)] = stats
    return out


def epoch(stamp):
    """An ISO timestamp as epoch seconds

    Arguments:
        stamp: the timestamp
    """
    return datetime.datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp()


def summed(stats):
    """Every shard's loop profile of a report summed into one histogram a kind

    Arguments:
        stats: a member's figures
    """
    kinds = {}
    for shard in stats.get("loop_profile") or []:
        for kind, histogram in (shard.get("kinds") or {}).items():
            total = kinds.setdefault(kind, {"count": 0, "total_us": 0, "buckets": [0] * BUCKETS})
            total["count"] += histogram.get("count", 0)
            total["total_us"] += histogram.get("total_us", 0)
            for bucket, count in enumerate(histogram.get("buckets") or []):
                total["buckets"][bucket] += count
    return kinds


def changed(first, last):
    """What each kind counted between two summed reports

    Arguments:
        first: the earlier
        last: the later
    """
    out = {}
    for kind, after in last.items():
        before = first.get(kind, {"count": 0, "total_us": 0, "buckets": [0] * BUCKETS})
        out[kind] = {
            "count": after["count"] - before["count"],
            "total_us": after["total_us"] - before["total_us"],
            "buckets": [a - b for a, b in zip(after["buckets"], before["buckets"])],
        }
    return out


def quantile_us(buckets, share):
    """The upper edge of the bucket a share of the counts falls in, in microseconds

    Arguments:
        buckets: the log2 buckets
        share: the share, 0.99 for the p99
    """
    total = sum(buckets)
    if not total:
        return None
    seen = 0
    for bucket, count in enumerate(buckets):
        seen += count
        if seen >= share * total:
            return 1 if bucket == 0 else 2 ** bucket
    return 2 ** (BUCKETS - 1)


def get_p99(stats):
    """A member's own get p99 in one report, or none

    Arguments:
        stats: the member's figures
    """
    for op in ((stats.get("queries") or {}).get("ops")) or []:
        if op.get("op") == "get":
            return op.get("p99_ms")
    return None


def main(root):
    """Print the attribution for every side-round under a captures directory

    Arguments:
        root: the captures directory
    """
    for name in sorted(os.listdir(root)):
        found = re.match(r"cell-r(\d+)-([AB])-cell$", name)
        if not found:
            continue
        mem = os.path.join(root, name[: -len("cell")] + "mem")
        stats_path = os.path.join(mem, "stats.json")
        if not os.path.exists(stats_path):
            stats_path += ".gz"
        if not os.path.exists(stats_path):
            continue
        streams = stats_stream(stats_path)
        capture = json.load(open(os.path.join(root, name, "bench.json")))
        for arm in capture.get("arms", []):
            for run in arm.get("runs", []):
                start = epoch(run["started_at"]) + (run.get("warmup") or 0)
                secs = (run.get("measured") or {}).get("secs") or 0
                print("## %s %s, %.0f s measured" % (name, arm["id"], secs))
                for host, seconds in sorted(streams.items()):
                    inside = sorted(at for at in seconds if start <= at <= start + secs)
                    if len(inside) < 2:
                        continue
                    first, last = seconds[inside[0]], seconds[inside[-1]]
                    window = inside[-1] - inside[0]
                    kinds = changed(summed(first), summed(last))
                    p99s = sorted(p for p in (get_p99(seconds[at]) for at in inside) if p is not None)
                    p99 = p99s[len(p99s) // 2] if p99s else None
                    print("### %s: its own get p99 %s ms (the median of its reports)" % (host, "%.2f" % p99 if p99 else "-"))
                    print("| kind | count | mean us | p99 us | share of the window |")
                    print("| --- | --- | --- | --- | --- |")
                    shards = len(first.get("loop_profile") or []) or 1
                    for kind, figures in sorted(kinds.items(), key=lambda item: -item[1]["total_us"]):
                        if not figures["count"]:
                            continue
                        mean = figures["total_us"] / figures["count"]
                        share = figures["total_us"] / (window * 1e6 * shards) if window else 0
                        q = quantile_us(figures["buckets"], 0.99)
                        print("| %s | %d | %.1f | %s | %.3f |" % (kind, figures["count"], mean, q, share))
                    print()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "target/lab/found/o140/captures")
