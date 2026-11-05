#!/usr/bin/env python3
"""A jemalloc heap profile of a node, symbolized: what its live memory was allocated by

Reads one `heap_v2` dump a node built with `tmdb-dataset`'s `jemalloc-prof` feature wrote (sampled
every 2^lg_prof_sample bytes, `/var/tmp/shoal-heap.<pid>.<n>.<kind>.heap`), scales each sampled stack
to what it stands for as jeprof does, and resolves its frames against the node's binary with
`addr2line`, which binutils ships; jeprof itself is not on the lab's hosts. Replaces round 15's
`target/lab/r15/prof/heap.py`, which was scratch and is gone
([item 338](../../docs/src/appendix/resolved/resident-floor-reserve.md)).

    python3 -I heap.py <dump> <binary> [depth] [top]

`depth` is how many frames past the allocator name a stack (3 unless given) and `top` how many stacks
are printed (40 unless given). Build the binary with frame pointers, or the stacks are short:
RUSTFLAGS="-C force-frame-pointers=yes". Only frames in the binary are named; the rest print as
addresses.
"""
import math
import re
import subprocess
import sys
from collections import defaultdict

# frames that are the allocator's or the standard library's plumbing, never what allocated
PLUMBING = (
    "je_", "_rjem_", "prof_", "imalloc", "ialloc", "malloc", "calloc", "realloc", "posix_memalign",
    "__rust_alloc", "__rust_realloc", "__rdl_", "__rg_", "tikv_jemallocator", "alloc::alloc::",
    "<tikv_jemallocator", "alloc::raw_vec::", "<alloc::raw_vec::", "alloc::vec::Vec<T,A>::reserve",
    "<alloc::vec::Vec", "alloc::vec::", "<alloc::alloc::Global", "<alloc::boxed::Box", "alloc::boxed::",
    "hashbrown::raw::", "<hashbrown::raw::", "core::ptr::", "std::alloc::",
)


def parse(path):
    """The dump's sample period, its stacks as (addresses, objects, bytes), and its mappings"""
    period, stacks, maps = None, [], []
    stack, in_maps = None, False
    for line in open(path):
        line = line.rstrip("\n")
        if in_maps:
            # a /proc/<pid>/maps line, kept when it names a file
            parts = line.split(None, 5)
            if len(parts) == 6 and parts[5].startswith("/"):
                lo, hi = (int(x, 16) for x in parts[0].split("-"))
                maps.append((lo, hi, int(parts[2], 16), parts[5]))
            continue
        if line.startswith("heap_v2/"):
            period = int(line.split("/")[1])
        elif line.startswith("MAPPED_LIBRARIES"):
            in_maps = True
        elif line.startswith("@"):
            stack = [int(x, 16) for x in line[1:].split()]
        elif stack is not None and line.strip().startswith("t*:"):
            # the stack's live objects and bytes, every thread's together
            objects, size = (int(x) for x in re.match(r"\s*t\*:\s*(\d+):\s*(\d+)", line).groups())
            stacks.append((stack, objects, size))
            stack = None
    return period, stacks, maps


def scaled(objects, size, period):
    """What a sampled stack's bytes stand for, unsampled, by jeprof's own estimate"""
    if objects == 0 or size == 0 or not period:
        return size
    mean = size / objects
    return size / (1 - math.exp(-mean / period))


def segments(binary):
    """The binary's loaded segments as (file offset, virtual address, size), from its program headers"""
    out = []
    text = subprocess.run(["readelf", "-lW", binary], capture_output=True, text=True, check=True).stdout
    for line in text.splitlines():
        parts = line.split()
        if parts and parts[0] == "LOAD":
            out.append((int(parts[1], 16), int(parts[2], 16), int(parts[4], 16)))
    return out


def resolve(addresses, maps, binary):
    """Every address's frame name, through addr2line, for the addresses inside the binary"""
    loads = segments(binary)
    wanted = {}
    for address in addresses:
        for lo, hi, offset, path in maps:
            if lo <= address < hi and path == binary_path(binary, maps):
                # a return address is past its call, so the call is a byte before it
                file_offset = address - 1 - lo + offset
                for seg_offset, vaddr, size in loads:
                    if seg_offset <= file_offset < seg_offset + size:
                        wanted[address] = file_offset - seg_offset + vaddr
                break
    names = {}
    if wanted:
        ordered = list(wanted.items())
        out = subprocess.run(
            ["addr2line", "-f", "-C", "-e", binary] + [hex(v) for _, v in ordered],
            capture_output=True, text=True, check=True,
        ).stdout.splitlines()
        # two lines an address: the function, then its file and line
        for (address, _), function in zip(ordered, out[0::2]):
            names[address] = function
    return names


def binary_path(binary, maps, _cache={}):
    """The path the dump's mappings name the binary by, matched on its file name"""
    if binary not in _cache:
        name = binary.rsplit("/", 1)[-1]
        _cache[binary] = next((path for _, _, _, path in maps if path.rsplit("/", 1)[-1] == name), None)
    return _cache[binary]


def short(name):
    """A frame's name without its generic arguments' noise, kept readable at a line's width"""
    name = re.sub(r"::h[0-9a-f]{16}$", "", name)
    return name if len(name) < 140 else name[:137] + "..."


def main():
    """Print the dump's live total and its stacks, largest first, each named by its first frames"""
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    dump, binary = sys.argv[1], sys.argv[2]
    depth = int(sys.argv[3]) if len(sys.argv) > 3 else 3
    top = int(sys.argv[4]) if len(sys.argv) > 4 else 40
    period, stacks, maps = parse(dump)
    names = resolve({a for stack, _, _ in stacks for a in stack}, maps, binary)
    # each stack named by its first frames past the allocator, the same names summed together
    totals = defaultdict(float)
    for stack, objects, size in stacks:
        frames = [names.get(a, hex(a)) for a in stack]
        kept = [short(f) for f in frames if not f.startswith(PLUMBING) and f != "??"]
        totals[" <- ".join(kept[:depth]) or "(allocator only)"] += scaled(objects, size, period)
    whole = sum(totals.values())
    print(f"{whole / 2**30:.2f} GiB live, sampled every {period} bytes, {len(stacks)} stacks")
    for key, size in sorted(totals.items(), key=lambda kv: -kv[1])[:top]:
        print(f"{size / 2**20:10.1f} MiB  {key}")


if __name__ == "__main__":
    main()
