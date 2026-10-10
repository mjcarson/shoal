"""X14's E6: Ceph's own CRUSH, offline, on X2's shapes and changes, measured as X2 measured.

Run inside the pinned v20.2.0 image with this directory mounted at /x14 (x14-crush.sh does), so
the mappings are crushtool's own:

    python3 /x14/x14-crush.py > /x14/x14-e6-crush.md

For every shape and pool, a map is built with `crushtool --build`, given the rule the monitor makes
for an erasure code profile (`chooseleaf indep` with `set_chooseleaf_tries 5` and
`set_choose_tries 100`) or a replicated one, and mapped for 16,384 placement groups before and after
a change to host zero's first device. As in X2 (placement-simulation.md, "The rest of the setup"):
the least a change could move is the chunks the departing devices held, the chunks a new device ends
up with, or the chunks a reweighted device gave up; on an erasure coded pool a chunk that changed
position counts as moved, on a replicated pool only a changed set does.
"""
import collections
import os
import re
import subprocess
import sys
import tempfile

# X2's four groups a tablet over 4096 tablets
PGS = 16384
NONE = 2147483647

# shape name, hosts, devices a host; and the pools X2 ran on it: name, width, domain, kind
SHAPES = [
    ("lab-1", 3, 1, [("2+1/host", 3, "host", "ec")]),
    ("lab-2", 3, 2, [("2+1/host", 3, "host", "ec"), ("4+2/device", 6, "osd", "ec")]),
    ("6x12", 6, 12, [("r3/host", 3, "host", "rep"), ("4+2/host", 6, "host", "ec"),
                     ("8+3/device", 11, "osd", "ec")]),
    ("50x24", 50, 24, [("r3/host", 3, "host", "rep"), ("10+4/host", 14, "host", "ec")]),
]


def run(args, workdir):
    """Run crushtool in the work directory and return its output."""
    out = subprocess.run(["crushtool"] + args, cwd=workdir, capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"crushtool {' '.join(args)} failed: {out.stderr}")
    return out.stdout


def build(workdir, hosts, per_host):
    """A straw2 root over straw2 hosts of equal devices, with the three rules the pools use."""
    run(["-o", "base.map", "--build", "--num_osds", str(hosts * per_host),
         "host", "straw2", str(per_host), "root", "straw2", "0"], workdir)
    # the rules the monitor makes: an EC profile's is add_simple_rule with "indep"
    run(["-i", "base.map", "-o", "base.map", "--create-simple-rule", "ec-host", "root", "host", "indep"], workdir)
    run(["-i", "base.map", "-o", "base.map", "--create-simple-rule", "ec-osd", "root", "osd", "indep"], workdir)
    run(["-i", "base.map", "-o", "base.map", "--create-replicated-rule", "rep-host", "root", "host"], workdir)
    text = run(["-d", "base.map"], workdir)
    # every rule's name and id, from the decompiled map
    return {m.group(1): (int(m.group(2)), m.group(0))
            for m in re.finditer(r"^rule (\S+) \{\s*id (\d+)", text, re.M)}


def mappings(workdir, mapfile, rule, width, weights=()):
    """Every placement group's answer, as crushtool prints it, a list per group."""
    args = ["-i", mapfile, "--test", "--rule", str(rule), "--num-rep", str(width),
            "--min-x", "0", "--max-x", str(PGS - 1), "--show-mappings"]
    for dev, w in weights:
        args += ["--weight", str(dev), str(w)]
    out = run(args, workdir)
    result = {}
    for line in out.splitlines():
        if line.startswith("CRUSH rule"):
            parts = line.split()
            x = int(parts[4])
            vec = parts[5].strip("[]")
            result[x] = [int(v) for v in vec.split(",")] if vec else []
    return result


def counts(maps):
    """Chunks on each device."""
    c = collections.Counter()
    for vec in maps.values():
        for d in vec:
            if d != NONE:
                c[d] += 1
    return c


def moved(before, after, kind):
    """Chunks moved: a changed position on EC, a changed set member on a replicated pool."""
    total = 0
    for x in before:
        b, a = before[x], after[x]
        if kind == "ec":
            total += sum(1 for i in range(len(b)) if i >= len(a) or b[i] != a[i])
        else:
            total += len(set(b) - set(a))
    return total


def main():
    """Every shape, pool and change, as one markdown table, then fill and holes."""
    print("# X14 E6: Ceph's CRUSH (crushtool, v20.2.0) on X2's shapes\n")
    print(f"{PGS} placement groups a pool; every change is to host zero's first device (osd.0).\n")
    print("| Shape | Pool | Change | Least | Moved | Moved / least | Moved off untouched hosts |")
    print("| --- | --- | --- | --- | --- | --- | --- |")
    fill_rows = []
    for shape, hosts, per_host, pools in SHAPES:
        with tempfile.TemporaryDirectory() as workdir:
            rules = build(workdir, hosts, per_host)
            n = hosts * per_host
            host0 = list(range(per_host))
            for pool, width, domain, kind in pools:
                rule = rules["rep-host" if kind == "rep" else ("ec-host" if domain == "host" else "ec-osd")][0]
                base = mappings(workdir, "base.map", rule, width)
                c0 = counts(base)
                holes = sum(1 for v in base.values() for d in v if d == NONE)
                mean = sum(c0.values()) / n
                fill_rows.append((shape, pool, max(c0.values()) / mean - 1, min(c0.values()) / mean - 1, holes))
                changes = []
                # a device like the others added to host zero
                run(["-i", "base.map", "-o", "add.map", "--add-item", str(n), "1.0", f"osd.{n}",
                     "--loc", "host", "host0", "--loc", "root", "root"], workdir)
                changes.append(("add", mappings(workdir, "add.map", rule, width), "add"))
                # removed from the map, as `ceph osd purge` does
                run(["-i", "base.map", "-o", "remove.map", "--remove-item", "osd.0"], workdir)
                changes.append(("remove", mappings(workdir, "remove.map", rule, width), "depart"))
                # its weight halved
                run(["-i", "base.map", "-o", "half.map", "--reweight-item", "osd.0", "0.5"], workdir)
                changes.append(("reweight ½", mappings(workdir, "half.map", rule, width), "reweight"))
                # marked out: in the map, weight 0 to placement, which is how a failed OSD starts
                changes.append(("out", mappings(workdir, "base.map", rule, width, [(0, 0)]), "depart"))
                # its host lost: every device of host zero out
                changes.append(("host out", mappings(workdir, "base.map", rule, width,
                                                     [(d, 0) for d in host0]), "depart-host"))
                # the second step of a removal done Ceph's way: out first, then out of the map
                out_map = changes[3][1]
                second = moved(out_map, changes[1][1], kind)
                second_off = sum(1 for x in out_map for i, d in enumerate(out_map[x])
                                 if d != NONE and d >= per_host
                                 and (i >= len(changes[1][1][x]) or changes[1][1][x][i] != d)
                                 and (kind == "ec" or d not in changes[1][1][x]))
                print(f"| {shape} | {pool} | remove, after out | 0 | {second:,} | "
                      f"{second / c0[0]:.2f}× the out step's least | {second_off:,} |")
                for name, after, how in changes:
                    c1 = counts(after)
                    if how == "add":
                        least = c1[n]
                    elif how == "depart":
                        least = c0[0]
                    elif how == "depart-host":
                        least = sum(c0[d] for d in host0)
                    else:
                        least = c0[0] - c1[0]
                    m = moved(base, after, kind)
                    # chunks that left a device of another host, which no change to host zero asked for
                    off_others = 0
                    for x in base:
                        b, a = base[x], after[x]
                        for i, d in enumerate(b):
                            if d != NONE and d >= per_host and (i >= len(a) or a[i] != d) and (kind == "ec" or d not in a):
                                off_others += 1
                    ratio = f"{m / least:.2f}×" if least else ("none needed; none moved" if m == 0 else f"{m} moved, none needed")
                    print(f"| {shape} | {pool} | {name} | {least:,} | {m:,} | {ratio} | {off_others:,} |")
    print("\n## Fill and holes, before any change\n")
    print("| Shape | Pool | Fullest device over the mean | Emptiest under it | Holes (CRUSH_ITEM_NONE) |")
    print("| --- | --- | --- | --- | --- |")
    for shape, pool, hi, lo, holes in fill_rows:
        print(f"| {shape} | {pool} | {100 * hi:+.1f}% | {100 * lo:+.1f}% | {holes:,} |")


if __name__ == "__main__":
    main()
