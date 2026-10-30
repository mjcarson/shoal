"""X17's reads half and X37's tables from their captures: a page's reads in flight and in runs, one
operation a record against three, and a merge's order, beside each other

    python3 -I shoal-spike-scan/results/x17-report.py shoal-spike-scan/results/x17 > x17-report.md

Every capture is `x17-<order>-r<round>-<which>/bench.json`, which `x17-lab.sh` names, or the same
slimmed into `x17-<order>-r<round>-<which>.json` by its `fetch`; `<order>` is the merge order Big
was loaded under, `hash` or `key`. For each read setting and layout this prints a scan of Big's own
rate and what it cost: the walk's reads a page and the partitions a read and a stretch, the device
reads and bytes a partition, and the node's and the hosts' cpu a partition; then the cell beside a
scan at two settings, and gets of Big. Every figure is each round's, then its median and range over
the rounds; two figures differ only where their ranges are disjoint, the lab's rule.
"""

import json
import pathlib
import re
import statistics
import sys

# a capture's name: the merge order it was loaded under, its round and which run it is
NAME = re.compile(r"^x17-(hash|key)-r(\d+)-([a-z0-9-]+?)(\.json)?$")

# the read arms in the order the page prints them, and what each is
ARMS = [
    ("r1", "one read at a time, today's walk"),
    ("r4", "four reads in flight"),
    ("r16", "sixteen reads in flight"),
    ("g4k2", "runs across gaps of 4 KiB, two in flight"),
    ("g4k16", "runs across gaps of 4 KiB, sixteen in flight"),
    ("g128k2", "runs across gaps of 128 KiB, two in flight"),
    ("t1", "three operations a record, one read at a time"),
    ("t16", "three operations a record, sixteen in flight"),
    ("g4k16p4", "g4k16's reads at four pages a shard and 64 streams"),
    ("shedstay", "today's walk, the driver staying on a shed tablet as before O139"),
    ("afterchurn", "today's walk after twenty minutes of inserts and overwrites into Big"),
    ("afterchurn-g4k16", "g4k16 after the same churn, the churn's supplement"),
    ("afterchurn-g128k2", "g128k2 after the same churn, the churn's supplement"),
]


def load(root):
    """Every capture under a directory, whole or slimmed, by order, round and run"""
    captures = {}
    for path in sorted(pathlib.Path(root).iterdir()):
        match = NAME.match(path.name)
        if not match:
            continue
        file = path if path.is_file() else path / "bench.json"
        if not file.exists():
            continue
        order, round_, which = match.group(1), int(match.group(2)), match.group(3)
        with open(file, encoding="utf-8") as handle:
            captures[(order, round_, which)] = json.load(handle)
    return captures


def arm(capture, arm_id):
    """An arm's one run, or none; a scan of a table names its streams when a run sets them
    (`scan/Big/c64/none`), so a scan's arm is found by its table and event alone"""
    for entry in capture.get("arms", []):
        if not entry.get("runs"):
            continue
        if entry["id"] == arm_id:
            return entry["runs"][0]
        parts, wanted = entry["id"].split("/"), arm_id.split("/")
        if wanted[0] == "scan" and len(parts) == 4 and parts[0] == "scan" and [parts[1], parts[3]] == wanted[1:]:
            return entry["runs"][0]
    return None


def p99(summary, kind):
    """A kind's p99 in a window's summary, or none"""
    latency = (summary or {}).get(kind, {}).get("latency") or {}
    return latency.get("p99_ms")


def p50(summary, kind):
    """A kind's p50 in a window's summary, or none"""
    latency = (summary or {}).get(kind, {}).get("latency") or {}
    return latency.get("p50_ms")


def hosts(run):
    """Every host's counters over a run"""
    return run.get("devices") or []


def device_sum(run, field):
    """A device counter summed over every host's devices"""
    return sum(device.get(field, 0) for host in hosts(run) for device in host.get("devices", []))


def node_cpu_secs(run):
    """The cpu every host's node process spent over a run"""
    return sum(process.get("cpu_secs", 0) for host in hosts(run) for process in host.get("processes", []))


def busy_secs(run):
    """Every host's busy cpu over a run, every process's"""
    return sum((host.get("cpu") or {}).get("busy_secs", 0) for host in hosts(run))


def scan_run_figures(run):
    """A scan arm's figures, the counters over its whole run divided by every partition it read"""
    scan = run["measured"].get("scan") or {}
    warm = (run.get("warmup") or {}).get("scan") or {}
    out = {}
    out["partitions a second"] = scan.get("partitions_per_sec")
    out["MiB a second"] = (scan.get("bytes_per_sec") or 0) / (1 << 20)
    pages = scan.get("pages") or 0
    if pages:
        out["walk reads a page"] = (scan.get("reads") or 0) / pages
        out["partitions a page"] = (scan.get("partitions") or 0) / pages
        out["pages shed a page"] = (scan.get("shed") or 0) / pages
    if scan.get("reads"):
        out["partitions a walk read"] = (scan.get("archived") or 0) / scan["reads"]
    if scan.get("stretches"):
        out["partitions a stretch"] = (scan.get("archived") or 0) / scan["stretches"]
    out["page read p50 ms"] = (scan.get("read") or {}).get("p50_ms")
    out["page read p99 ms"] = (scan.get("read") or {}).get("p99_ms")
    out["page held p99 ms"] = (scan.get("held") or {}).get("p99_ms")
    out["page paced p50 ms"] = (scan.get("paced") or {}).get("p50_ms")
    # the hosts' counters span warmup and measurement, so every partition of both counts
    partitions = (scan.get("partitions") or 0) + (warm.get("partitions") or 0)
    if partitions:
        reads = device_sum(run, "reads")
        out["device reads a partition"] = reads / partitions
        if reads:
            out["partitions a device read"] = partitions / reads
        out["device KiB a partition"] = device_sum(run, "read_bytes") / partitions / 1024
        out["node cpu us a partition"] = node_cpu_secs(run) / partitions * 1e6
        out["host cpu us a partition"] = busy_secs(run) / partitions * 1e6
    # each host's own, by the partitions its member read: the Zen1 hosts together, as W12's budget
    # is stated, and europa apart
    for name, members in (("Zen1 hosts'", ("titan", "hyperion")), ("europa's", ("europa",))):
        read = sum(((scan.get("members") or {}).get(member) or {}).get("partitions", 0)
                   + ((warm.get("members") or {}).get(member) or {}).get("partitions", 0) for member in members)
        own = [host for host in hosts(run) if host.get("host") in members]
        if read and own:
            cpu = sum(process.get("cpu_secs", 0) for host in own for process in host.get("processes", []))
            reads = sum(device.get("reads", 0) for host in own for device in host.get("devices", []))
            out[f"{name} node cpu us a partition"] = cpu / read * 1e6
            if reads:
                out[f"{name} partitions a device read"] = read / reads
    return out


def figures(captures):
    """Every figure of every capture, by order, which run and name, a value a round"""
    rows = {}

    def put(order, which, name, round_, value):
        if value is not None:
            rows.setdefault((order, which, name), {})[round_] = value

    for (order, round_, which), capture in captures.items():
        if which.startswith("scan-"):
            run = arm(capture, "scan/Big/none")
            if run:
                for name, value in scan_run_figures(run).items():
                    put(order, which[len("scan-"):], name, round_, value)
        elif which.startswith("cell-"):
            run = arm(capture, "rw50/b1/scan")
            if not run or not run.get("event"):
                continue
            windows = run["event"].get("windows") or {}
            before, during = windows.get("before"), windows.get("during")
            setting = which[len("cell-"):]
            for kind in ("read", "insert"):
                put(order, f"cell {setting}", f"{kind} p99 ms before", round_, p99(before, kind))
                put(order, f"cell {setting}", f"{kind} p99 ms during", round_, p99(during, kind))
                if p99(before, kind) and p99(during, kind):
                    put(order, f"cell {setting}", f"{kind} p99 during / before", round_, p99(during, kind) / p99(before, kind))
            scan = (during or {}).get("scan") or {}
            put(order, f"cell {setting}", "scan partitions a second during", round_, scan.get("partitions_per_sec"))
        elif which == "gets":
            run = arm(capture, "read100/b1/none")
            if run:
                measured = run["measured"]
                put(order, "gets", "gets a second", round_, (measured.get("read") or {}).get("per_sec"))
                put(order, "gets", "get p50 ms", round_, p50(measured, "read"))
                put(order, "gets", "get p99 ms", round_, p99(measured, "read"))
        elif which == "load":
            preload = capture.get("preload") or {}
            put(order, "load", "preload secs", round_, preload.get("secs") or preload.get("elapsed_secs"))
    return rows


def cell(values):
    """A figure's rounds as its median and range"""
    if not values:
        return "-"
    ordered = sorted(values.values())
    median = statistics.median(ordered)
    return f"{median:.3g} [{ordered[0]:.3g}, {ordered[-1]:.3g}] (n {len(ordered)})"


def disjoint(left, right):
    """Whether two figures' ranges are disjoint, the lab's rule for a difference"""
    if not left or not right:
        return ""
    a, b = sorted(left.values()), sorted(right.values())
    return "yes" if a[-1] < b[0] or b[-1] < a[0] else "no"


def ratio(left, right):
    """Two figures' medians as a ratio, right over left"""
    if not left or not right:
        return "-"
    lm, rm = statistics.median(left.values()), statistics.median(right.values())
    return f"{rm / lm:.2f}" if lm else "-"


SCAN_FIGURES = [
    "partitions a second", "MiB a second", "walk reads a page", "partitions a page", "partitions a walk read",
    "partitions a stretch", "device reads a partition", "partitions a device read", "device KiB a partition",
    "node cpu us a partition", "Zen1 hosts' node cpu us a partition", "europa's node cpu us a partition",
    "Zen1 hosts' partitions a device read", "europa's partitions a device read",
    "host cpu us a partition", "pages shed a page", "page read p50 ms",
    "page read p99 ms", "page held p99 ms", "page paced p50 ms",
]


def page(rows):
    """Every table: each scan figure by arm and layout, then the cell and the gets"""
    for figure in SCAN_FIGURES:
        print(f"\n### {figure}\n")
        print("| Arm | Hash order | Key order | Disjoint | Key / hash | Key's against r1 | Disjoint from r1 |")
        print("| --- | --- | --- | --- | --- | --- | --- |")
        base = rows.get(("key", "r1", figure), {})
        for name, what in ARMS:
            hashed = rows.get(("hash", name, figure), {})
            keyed = rows.get(("key", name, figure), {})
            if not hashed and not keyed:
                continue
            against = ratio(base, keyed) if name != "r1" else ""
            print(f"| {name}: {what} | {cell(hashed)} | {cell(keyed)} | {disjoint(hashed, keyed)} | {ratio(hashed, keyed)} | "
                  f"{against} | {disjoint(base, keyed) if name != 'r1' else ''} |")
    for which in ("cell r1", "cell g4k16", "gets", "load"):
        names = sorted({name for (order, w, name) in rows if w == which})
        if not names:
            continue
        print(f"\n### {which}\n")
        print("| Figure | Hash order | Key order | Disjoint |")
        print("| --- | --- | --- | --- |")
        for name in names:
            hashed = rows.get(("hash", which, name), {})
            keyed = rows.get(("key", which, name), {})
            print(f"| {name} | {cell(hashed)} | {cell(keyed)} | {disjoint(hashed, keyed)} |")


# a merge's line as the spike logs it, its colours taken out first
MERGE = re.compile(r'hash_order=(true|false) entries=(\d+) sort_us=(\d+) order_bytes=(\d+) write_us=(\d+)')
# the escapes a journal keeps of a log's colours
ESCAPES = re.compile(r"\x1b\[[0-9;]*m")
# a merge log's name: the layout, the round, what loaded it and the host
MERGE_LOG = re.compile(r"^x17-(hash|key)-r(\d+)-(load|churn)-([a-z]+)\.log$")


def merges(root):
    """Every merge each node logged, by layout, round and what wrote it: its entries, sort, order's
    bytes and write"""
    found = {}
    directory = pathlib.Path(root) / "merges"
    if not directory.is_dir():
        return found
    for path in sorted(directory.iterdir()):
        match = MERGE_LOG.match(path.name)
        if not match:
            continue
        key = (match.group(1), int(match.group(2)), match.group(3))
        with open(path, encoding="utf-8", errors="replace") as handle:
            for line in handle:
                merge = MERGE.search(ESCAPES.sub("", line))
                if merge:
                    found.setdefault(key, []).append(tuple(int(value) for value in merge.groups()[1:]))
    return found


def merge_figures(found):
    """Each layout's merges as figures a round: how many, their entries, the sort's time beside the
    write's, and the bytes the order held"""
    rows = {}
    for (order, round_, phase), entries in found.items():
        sorts = sorted(sort for _, sort, _, _ in entries)
        writes = sum(write for _, _, _, write in entries)
        figures_of_round = {
            "merges": len(entries),
            "entries a merge, median": statistics.median(count for count, _, _, _ in entries),
            "sort us a merge, median": statistics.median(sorts),
            "sort us a merge, p99": sorts[min(len(sorts) - 1, int(len(sorts) * 0.99))],
            "sort us a merge, most": sorts[-1],
            "sort ns an entry": sum(sorts) * 1000 / max(1, sum(count for count, _, _, _ in entries)),
            "write ms a merge, median": statistics.median(write for _, _, _, write in entries) / 1000,
            "sort's share of a merge's time, percent": 100 * sum(sorts) / max(1, sum(sorts) + writes),
            "order KiB a merge, most": max(size for _, _, size, _ in entries) / 1024,
        }
        for name, value in figures_of_round.items():
            rows.setdefault((order, phase, name), {})[round_] = value
    return rows


def idle(root):
    """Each host's busy cores over the idle minute of every round, from /proc/stat's first line"""
    rows = {}
    directory = pathlib.Path(root) / "idle"
    if not directory.is_dir():
        return rows
    for before in sorted(directory.glob("r*-*.before")):
        after = before.with_suffix(".after")
        if not after.exists():
            continue
        round_, host = before.stem.split("-", 1)
        first = [int(value) for value in before.read_text().split()[1:]]
        last = [int(value) for value in after.read_text().split()[1:]]
        spent = [b - a for a, b in zip(first, last)]
        total = sum(spent[:8])
        # idle and iowait are the 4th and 5th fields; the rest is busy
        busy = total - spent[3] - spent[4]
        if total:
            # a host's threads busy on average, from its share of every thread's time
            threads = {"europa": 32, "titan": 8, "hyperion": 8}.get(host, 1)
            rows.setdefault(host, {})[int(round_[1:])] = busy / total * threads
    return rows


# a perf report's line of self time: its share, its object, user or kernel, and its symbol
PERF_LINE = re.compile(r"^\s+([\d.]+)%\s+(\S+)\s+\[(.)\]\s+(.*?)\s*(-\s+-\s*)?$")
# every kind of work a symbol is sorted into, by its pattern, user and kernel apart
PERF_KINDS = [
    ("kernel: syscall entry, exit and SRSO", r"^(srso_|entry_SYSCALL|do_syscall_64|arch_exit_to_user|syscall_|__x64_sys|exit_to_user|__rseq|rseq_)"),
    ("kernel: io_uring", r"(io_uring|^io_|^__io_|tctx_task_work|^task_work|_io_uring)"),
    ("kernel: block, nvme, iomap, xfs", r"^(blk_|__blk|nvme|bio_|__bio|iomap|__iomap|xfs_|submit_bio|dio_|iov_iter|_copy_to_iter|bdev_)"),
    ("kernel: IPIs and membarrier", r"(smp_call_function|send_IPI|sysvec_call_function|membarrier|flush_smp_call|call_function_single|native_send_call|x2apic|ipi)"),
    ("kernel: memory", r"^(xas_|xa_|folio|__folio|kfree|kmem_cache|__kmem|__kmalloc|kmalloc|__alloc|alloc_|free_|__free|page_|flush_tlb|native_flush_tlb|_find_next|percpu|mod_objcg|obj_cgroup|memcg|__memcg|refill_obj|memset_orig)"),
    ("kernel: locks and rcu", r"^(mutex_|__mutex|__rcu|rcu_|__raw_spin_lock_irqsave|_raw_spin_unlock_irqrestore|up_read|down_read|up_write|down_write)"),
    ("kernel: copies", r"^(rep_movs|_copy_from_user|_copy_to_user|copy_user|__copy|memcpy_orig)"),
    ("kernel: timers and clocks", r"^(hrtimer|__hrtimer|read_tsc|ktime_|timerqueue|clockevents|lapic_next)"),
    ("kernel: scheduling and futex", r"^(schedule|__schedule|pick_next|finish_task_switch|update_|enqueue|dequeue|futex|__futex|try_to_wake|ttwu|psi_|select_task|__wake_up|wake_up|raw_spin|_raw_spin|native_queued|queued_spin|cpuacct)"),
    ("kernel: network", r"^(tcp_|__tcp|ip_|__ip|skb|__skb|kfree_skb|sock_|__sock|net_|__netif|napi|__dev_queue|dev_hard|igb|e1000|inet_|sk_|__sk|release_sock|lock_sock|tls_)"),
    ("kernel: files and descriptors", r"^(fget|__fget|fput|__fput|filp_|close_fd|__close|dup|__fdget)"),
    ("user: allocator", r"^(_?mi_|__libc_malloc|malloc|free|cfree|realloc)"),
    ("user: glommio and liburing", r"^(<?glommio|_?io_uring_|__io_uring|<glommio)"),
    ("user: rkyv validation", r"(rkyv|bytecheck|check_bytes|CheckBytes)"),
    ("user: checksum", r"(gxhash|crc)"),
    ("user: openraft", r"openraft"),
    ("user: memcpy and libc", r"^(__memmove|__memcpy|memcpy|memmove|__memset|memset|__memcmp|memcmp|__strlen)"),
    ("user: clocks", r"(clock_gettime|Timespec|Instant)|^0x"),
    ("user: shoal", r"shoal"),
    ("user: kanal and futures", r"(kanal|futures|async_task|core::future|Waker)"),
]


def perf_kinds(path):
    """A profile's self time by kind of work, the report's top symbols alone, and what was left
    unsorted"""
    out, total, left = {}, 0.0, {}
    with open(path, encoding="utf-8", errors="replace") as handle:
        for line in handle:
            match = PERF_LINE.match(line)
            if not match:
                continue
            pct, dso, kind, symbol = float(match.group(1)), match.group(2), match.group(3), match.group(4)
            total += pct
            for name, pattern in PERF_KINDS:
                if (name.startswith("kernel") == (kind == "k")) and re.search(pattern, symbol):
                    out[name] = out.get(name, 0) + pct
                    break
            else:
                bucket = "kernel: other" if kind == "k" else f"user: other ({dso})"
                out[bucket] = out.get(bucket, 0) + pct
                left[symbol[:80]] = left.get(symbol[:80], 0) + pct
    return out, total, left


def profiles(root):
    """Every profile of titan's node as a table, by kind of work, its arms side by side"""
    directory = pathlib.Path(root) / "perf"
    if not directory.is_dir():
        return
    found = {path.stem: perf_kinds(path) for path in sorted(directory.glob("*.txt"))}
    if not found:
        return
    names = sorted({name for out, _, _ in found.values() for name in out})
    print("\n### titan's node under a scan, its self time by kind of work\n")
    print("| Kind | " + " | ".join(found) + " |")
    print("| --- | " + " | ".join("---" for _ in found) + " |")
    for name in sorted(names, key=lambda name: -sum(out.get(name, 0) for out, _, _ in found.values())):
        print(f"| {name} | " + " | ".join(f"{out.get(name, 0):.1f}%" for out, _, _ in found.values()) + " |")
    print("| every symbol the report's top holds | " + " | ".join(f"{total:.1f}%" for _, total, _ in found.values()) + " |")


def page_figures(run, tablets=4096):
    """A scan arm's page figures for X17's other half: its loop holds, its encode steps and its
    rate"""
    scan = run["measured"].get("scan") or {}
    out = {}
    pages = scan.get("pages") or 0
    held = scan.get("held") or {}
    out["partitions a second"] = scan.get("partitions_per_sec")
    out["capture's hold p99 ms"] = held.get("p99_ms")
    out["capture's hold most ms"] = held.get("max_ms")
    out["page read p50 ms"] = (scan.get("read") or {}).get("p50_ms")
    out["page read p99 ms"] = (scan.get("read") or {}).get("p99_ms")
    if pages:
        out["partitions a page"] = (scan.get("partitions") or 0) / pages
        out["resident partitions a page"] = (scan.get("resident") or 0) / pages
        out["encode steps a page"] = (scan.get("steps") or 0) / pages
        out["step wait a page ms"] = (scan.get("step_wait_us") or 0) / pages / 1000
        whole = scan.get("scans") or 0
        if whole:
            out["pages a tablet"] = pages / (whole * tablets)
    if scan.get("steps"):
        out["a step's hold us"] = (scan.get("step_held_us") or 0) / scan["steps"]
        out["the longest step ms"] = (scan.get("step_max_us") or 0) / 1000
    return out


def plan_figures(captures):
    """X17's other half: each page mode's figures, by table, a value a round"""
    rows = {}

    def put(mode, which, name, round_, value):
        if value is not None:
            rows.setdefault((mode, which, name), {})[round_] = value

    for (order, round_, which), capture in captures.items():
        if not which.startswith("page-"):
            continue
        _, mode, table = which.split("-", 2)
        if table == "keyword":
            alone = arm(capture, "scan/Keyword/none")
            if alone:
                for name, value in page_figures(alone).items():
                    put(mode, "Keyword alone", name, round_, value)
            beside = arm(capture, "rw50/b1/scan")
            if beside and beside.get("event"):
                windows = beside["event"].get("windows") or {}
                before, during = windows.get("before"), windows.get("during")
                for kind in ("read", "insert"):
                    if p99(before, kind) and p99(during, kind):
                        put(mode, "the cell beside Keyword", f"{kind} p99 during / before", round_, p99(during, kind) / p99(before, kind))
                    put(mode, "the cell beside Keyword", f"{kind} p99 ms during", round_, p99(during, kind))
                scan = (during or {}).get("scan") or {}
                held = scan.get("held") or {}
                put(mode, "the cell beside Keyword", "capture's hold p99 ms", round_, held.get("p99_ms"))
                put(mode, "the cell beside Keyword", "capture's hold most ms", round_, held.get("max_ms"))
                if scan.get("steps"):
                    put(mode, "the cell beside Keyword", "the longest step ms", round_, (scan.get("step_max_us") or 0) / 1000)
        elif table == "big":
            alone = arm(capture, "scan/Big/none")
            if alone:
                for name, value in page_figures(alone).items():
                    put(mode, "Big alone", name, round_, value)
    return rows


def plan_page(captures):
    """X17's other half's tables: captured against planned, by table"""
    rows = plan_figures(captures)
    if not rows:
        return
    for which in ("Keyword alone", "the cell beside Keyword", "Big alone"):
        names = sorted({name for (mode, w, name) in rows if w == which})
        if not names:
            continue
        print(f"\n### {which}: captured against planned\n")
        print("| Figure | Captured | Planned | Disjoint |")
        print("| --- | --- | --- | --- |")
        for name in names:
            captured, planned = rows.get(("capture", which, name), {}), rows.get(("plan", which, name), {})
            print(f"| {name} | {cell(captured)} | {cell(planned)} | {disjoint(captured, planned)} |")


def encode_page(root):
    """Q35's benchmark: each host's sides, a line each"""
    directory = pathlib.Path(root) / "encode"
    if not directory.is_dir():
        return
    lines = []
    for path in sorted(directory.glob("*.tsv")):
        for line in path.read_text().splitlines():
            fields = line.split("\t")
            if len(fields) < 4 or fields[2] == "chunks":
                continue
            values = dict(zip(fields[3::2], fields[4::2])) if False else {}
            parts = fields[3:]
            for part in parts:
                key, _, value = part.partition(" ")
                values[key] = value
            lines.append((fields[0], fields[1], fields[2], values))
    if not lines:
        return
    print("\n### Q35: rows encoded on the shard against walked partitions, one core\n")
    print("| Host | Table | Side | Best ns a row | Median ns a row | Best cpu s a GiB | Median cpu s a GiB |")
    print("| --- | --- | --- | --- | --- | --- | --- |")
    for host, table, side, values in lines:
        print(f"| {host} | {table} | {side} | {values.get('best_ns_a_row')} | {values.get('median_ns_a_row')} | "
              f"{values.get('best_secs_a_gib')} | {values.get('median_secs_a_gib')} |")


def background(root):
    """The merges and the idle minute, each a table"""
    rows = merge_figures(merges(root))
    names = sorted({name for (_, _, name) in rows}, key=lambda name: (name.split(",")[0], name))
    for phase in ("load", "churn"):
        if not any(key[1] == phase for key in rows):
            continue
        print(f"\n### merges while {'loading' if phase == 'load' else 'churning'} Big\n")
        print("| Figure | Hash order | Key order | Disjoint |")
        print("| --- | --- | --- | --- |")
        for name in names:
            hashed, keyed = rows.get(("hash", phase, name), {}), rows.get(("key", phase, name), {})
            if hashed or keyed:
                print(f"| {name} | {cell(hashed)} | {cell(keyed)} | {disjoint(hashed, keyed)} |")
    quiet = idle(root)
    if quiet:
        print("\n### busy threads with the cluster idle, a minute a round\n")
        print("| Host | Busy threads |")
        print("| --- | --- |")
        for host in sorted(quiet):
            print(f"| {host} | {cell(quiet[host])} |")


def main():
    """Print every table of the captures under a directory"""
    captures = load(sys.argv[1])
    page(figures(captures))
    plan_page(captures)
    encode_page(sys.argv[1])
    background(sys.argv[1])
    profiles(sys.argv[1])


if __name__ == "__main__":
    main()
