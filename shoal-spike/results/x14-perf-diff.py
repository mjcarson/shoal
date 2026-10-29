"""X14's E3: what each shard's OSD did during a run of writes, less an idle stretch as long.

    python3 x14-perf-diff.py <acting,in,shard,order> <idle0.tsv> <run1.tsv> <idle1.tsv>

Each file holds one line an OSD, `<id>\t<perf dump json>`, read before the run, after it, and after
an idle stretch as long as the run. A counter's figure is (run1 - idle0) - (idle1 - run1).
"""
import json
import sys

# (section, counter, what it says) for each column printed
COUNTERS = [
    ("bluestore", "txc_count", "transactions"),
    ("bluestore", "write_small", "small writes"),
    ("bluestore", "write_small_bytes", "small write bytes"),
    ("bluestore", "write_big", "big writes"),
    ("bluestore", "issued_deferred_writes", "deferred writes"),
    ("bluestore", "omap_setkeys_count", "omap sets"),
    ("bluestore", "read_lat", "reads"),
    ("osd", "subop", "sub-ops"),
    ("osd", "subop_in_bytes", "sub-op bytes in"),
    ("osd", "op_w", "client writes"),
]


def load(path):
    """Every OSD's counters, by OSD id."""
    out = {}
    for line in open(path):
        osd, blob = line.rstrip("\n").split("\t", 1)
        out[int(osd)] = json.loads(blob)
    return out


def value(dump, section, name):
    """A counter's value, or the count of a latency counter."""
    v = dump[section][name]
    return v["avgcount"] if isinstance(v, dict) else v


def main():
    """One row a shard, in shard order, with each counter net of the idle stretch."""
    acting = [int(x) for x in sys.argv[1].split(",")]
    idle0, run1, idle1 = (load(p) for p in sys.argv[2:5])
    print("| Shard | OSD | " + " | ".join(c[2] for c in COUNTERS) + " |")
    print("| --- | --- | " + " | ".join("---" for _ in COUNTERS) + " |")
    for shard, osd in enumerate(acting):
        cells = []
        for section, name, _ in COUNTERS:
            run = value(run1[osd], section, name) - value(idle0[osd], section, name)
            idle = value(idle1[osd], section, name) - value(run1[osd], section, name)
            cells.append(f"{run - idle:,}")
        print(f"| {shard} | osd.{osd} | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main()
