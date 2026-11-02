#!/usr/bin/env python3
"""Item 353 on the lab: each shard's long scan steps beside what its cpu was doing.

Reads the captures and the cpu samples of `CPU_SAMPLE=1 m23-lab.sh start` and prints, as markdown,
where the scan steps that held their loop past 100 µs on their own cpu were, by shard and so by
cpu, beside the interrupts and softirqs that cpu took over the same arm, with the NIC on a shard's
cpu and moved off it (NIC_ROUNDS), and which of those steps took a page fault (item 353,
docs/src/appendix/known-issues.md):

    python3 -I shoal-spike-scan/results/m23-i353-report.py target/lab/m23-i353/captures \\
        --csv shoal-spike-scan/results/m23-i353/shards.csv > shoal-spike-scan/results/m23-i353/report.md

A round's captures are `m23-r<N>-big` and `m23-r<N>-keyword`, its samples `m23-r<N>-cpu/samples-<host>.log.gz`
and `nic.txt`. Two populations are read apart: `Big`'s scans (the big capture's `scan/Big/none` and
`rw50/b1/scan` arms), whose encode steps are one 1 KiB row each and whose captures look at a few
keys, and `Keyword`'s (the big capture's `scan/Keyword/none` and the keyword capture's
`rw50/b1/scan`), whose resident overlays take steps of tens of rows. A shard's counts in an arm are
its last sample's less its first's, within the arm's own samples; a cpu's figures are the samples
nearest inside the same span. Every count is a count of steps past 100 µs by the whole microsecond,
as the node judges them.
"""

import argparse
import csv
import datetime
import gzip
import json
import math
import os
import re
import sys

# the populations, each a set of (capture, arm) it is read over
POPULATIONS = {
    "Big": [("big", "scan/Big/none"), ("big", "rw50/b1/scan")],
    "Keyword": [("big", "scan/Keyword/none"), ("keyword", "rw50/b1/scan")],
}
# a kind's counts, as `StepCounts` names them
COUNTS = ("steps", "wall_ns", "cpu_ns", "past", "past_cpu", "past_cpu_faulted", "past_switched", "past_faults")
# the softirqs read by cpu, beside every one's total
SOFTIRQS = ("NET_RX", "NET_TX", "TIMER", "SCHED", "RCU", "BLOCK", "HRTIMER")
# the interrupts read by cpu: the NIC's and NVMe's summed rows and the IPIs
INTERRUPTS = ("NIC", "NVME", "LOC", "RES", "CAL", "TLB")


def started(text):
    """A run's start, `2026-11-01T18:28:19Z`, as unix seconds."""
    return datetime.datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc).timestamp()


def read_samples(path):
    """Every sample of one host's cpu sampler, oldest first."""
    opener = gzip.open if path.endswith(".gz") else open
    out, sample, section = [], None, None
    with opener(path, "rt") as lines:
        for line in lines:
            line = line.rstrip("\n")
            if line.startswith("@ "):
                sample = {"at": float(line[2:]), "stat": {}, "softirqs": {}, "interrupts": {}, "vmstat": {}, "threads": {}}
                out.append(sample)
                section = None
                continue
            if sample is None:
                continue
            if line.startswith("# "):
                section = line[2:].split()[0]
                continue
            fields = line.split()
            if not fields:
                continue
            if section == "stat" and fields[0].startswith("cpu"):
                # user nice system idle iowait irq softirq steal, guest time already inside user
                ticks = [int(value) for value in fields[1:9]]
                sample["stat"][int(fields[0][3:])] = (sum(ticks), ticks[5], ticks[6])
            elif section in ("softirqs", "interrupts") and fields[0].endswith(":"):
                name = fields[0][:-1]
                counts = [int(value) for value in fields[1:] if value.isdigit()]
                sample[section][name] = counts
            elif section == "vmstat" and len(fields) == 2:
                sample["vmstat"][fields[0]] = int(fields[1])
            elif section == "threads" and len(fields) >= 7:
                tid, cpu, minflt, majflt = (int(value) for value in fields[:4])
                sample["threads"][tid] = (cpu, minflt, majflt, " ".join(fields[6:]))
    # a sample cut short by the sampler's end is left out
    return [sample for sample in out if sample["stat"]]


def bracket(samples, begin, end):
    """The first sample at or after `begin` and the last at or before `end`, or none."""
    first = next((sample for sample in samples if sample["at"] >= begin), None)
    last = next((sample for sample in reversed(samples) if sample["at"] <= end), None)
    if first is None or last is None or last["at"] - first["at"] < 0.5:
        return None
    return first, last


def per_cpu(first, last):
    """Each cpu's figures between two samples: its softirq and irq share of its time, and each
    softirq's and interrupt's rate, and the page faults a second of the threads that ran on it."""
    secs = last["at"] - first["at"]
    out = {}
    for cpu, (total, irq, soft) in last["stat"].items():
        was = first["stat"].get(cpu)
        if was is None:
            continue
        ticks = max(total - was[0], 1)
        figures = {"softirq_share": (soft - was[2]) / ticks, "irq_share": (irq - was[1]) / ticks}
        for name in SOFTIRQS:
            now, then = last["softirqs"].get(name), first["softirqs"].get(name)
            figures[name] = (now[cpu] - then[cpu]) / secs if now and then and cpu < len(now) else 0.0
        for name in INTERRUPTS:
            now, then = last["interrupts"].get(name), first["interrupts"].get(name)
            figures[name] = (now[cpu] - then[cpu]) / secs if now and then and cpu < len(now) else 0.0
        figures["minflt"] = 0.0
        out[cpu] = figures
    # a thread's faults go to the cpu it ran on at the later sample: a shard's never moves
    for tid, (cpu, minflt, _, comm) in last["threads"].items():
        was = first["threads"].get(tid)
        if was is not None and cpu in out:
            out[cpu]["minflt"] += (minflt - was[1]) / secs
    return out


def nic_cpu(samples):
    """The cpu that took the most of a host's NIC interrupts over its samples, or none."""
    if len(samples) < 2:
        return None
    now, then = samples[-1]["interrupts"].get("NIC"), samples[0]["interrupts"].get("NIC")
    if not now or not then:
        return None
    deltas = [a - b for a, b in zip(now, then)]
    return max(range(len(deltas)), key=deltas.__getitem__) if max(deltas) > 0 else None


def shard_counts(entry):
    """One shard's two kinds, as a flat dict of `captures.<count>` and `encodes.<count>`."""
    out = {}
    for kind in ("captures", "encodes"):
        counts = entry.get(kind) or {}
        for name in COUNTS:
            out[f"{kind}.{name}"] = counts.get(name, 0)
    return out


def arm_shards(run):
    """Each member's shards' counts over an arm, its last sample's less its first's, with each
    shard's cpu, and the arm's span in unix seconds; and the arm's 2 s windows, each shard's counts
    and span apart."""
    series = run.get("server_series") or []
    if len(series) < 2:
        return {}, None, []
    begin = started(run["started_at"])
    span = (begin + series[0]["at_ms"] / 1000, begin + series[-1]["at_ms"] / 1000)

    def at(sample):
        # each member's shards by id: (cpu, counts)
        return {
            member: {entry["shard"]: (entry.get("cpu"), shard_counts(entry)) for entry in entries}
            for member, entries in (sample.get("scan_shards") or {}).items()
        }

    def less(later, earlier):
        # every shard of later, less its counts in earlier, which may not hold it yet
        out = {}
        for member, shards in later.items():
            for shard, (cpu, counts) in shards.items():
                was = earlier.get(member, {}).get(shard, (cpu, {}))[1]
                out.setdefault(member, {})[shard] = (cpu, {name: counts[name] - was.get(name, 0) for name in counts})
        return out

    first, last = at(series[0]), at(series[-1])
    windows = []
    for earlier, later in zip(series, series[1:]):
        windows.append(
            ((begin + earlier["at_ms"] / 1000, begin + later["at_ms"] / 1000), less(at(later), at(earlier)))
        )
    return less(last, first), span, windows


def interval(count, exposure):
    """A rate's count a unit of exposure, with an approximate 95% interval for a Poisson count."""
    if exposure <= 0:
        return "-"
    low = max(count - 1.96 * math.sqrt(count), 0) if count else 0.0
    high = count + 1.96 * math.sqrt(max(count, 1))
    return f"{count / exposure:.2f} [{low / exposure:.2f}–{high / exposure:.2f}]"


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("captures", help="the run's captures directory, $LAB/captures")
    parser.add_argument("--csv", help="write every shard's counts and its cpu's figures by round and arm here")
    args = parser.parse_args()

    rounds = sorted(
        int(match.group(1))
        for name in os.listdir(args.captures)
        if (match := re.fullmatch(r"m23-r(\d+)-cpu", name))
    )
    if not rounds:
        sys.exit(f"no m23-r<N>-cpu directory under {args.captures}")

    rows = []
    round_lines = []
    vm_lines = []
    # each round's hosts' NIC cpus, and each arm's shards beside their cpus
    for number in rounds:
        cpu_dir = os.path.join(args.captures, f"m23-r{number}-cpu")
        nic_state = open(os.path.join(cpu_dir, "nic.txt")).read().strip() if os.path.exists(os.path.join(cpu_dir, "nic.txt")) else "?"
        moved = nic_state.startswith("moved")
        samples = {}
        for name in os.listdir(cpu_dir):
            match = re.fullmatch(r"samples-(\w+)\.log(\.gz)?", name)
            if match:
                samples[match.group(1)] = read_samples(os.path.join(cpu_dir, name))
        nics = {host: nic_cpu(host_samples) for host, host_samples in samples.items()}
        # each host's own faults and its kernel's work for huge pages over the round
        for host in ("europa", "titan", "hyperion"):
            host_samples = samples.get(host, [])
            if len(host_samples) < 2:
                continue
            first, last = host_samples[0], host_samples[-1]
            secs = last["at"] - first["at"]
            delta = {name: last["vmstat"].get(name, 0) - first["vmstat"].get(name, 0) for name in last["vmstat"]}
            vm_lines.append(
                f"| {number} | {host} | {secs:,.0f} | {delta.get('pgfault', 0) / secs:,.0f} | {delta.get('pgmajfault', 0):,} | "
                f"{delta.get('compact_stall', 0):,} | {delta.get('thp_fault_alloc', 0):,} |"
            )
        shard_cpus = {}
        for population, arms in POPULATIONS.items():
            for capture_name, arm_id in arms:
                path = os.path.join(args.captures, f"m23-r{number}-{capture_name}", "bench.json")
                if not os.path.exists(path):
                    continue
                capture = json.load(open(path))
                for arm in capture["arms"]:
                    if arm["id"] != arm_id:
                        continue
                    for run in arm["runs"]:
                        shards, span, windows = arm_shards(run)
                        if span is None:
                            continue
                        for member, by_shard in shards.items():
                            host_samples = samples.get(member, [])
                            window = bracket(host_samples, *span)
                            cpus = per_cpu(*window) if window else {}
                            # each 2 s window's figures, for the reading by window below
                            windowed = []
                            for (w_begin, w_end), w_shards in windows:
                                w_bracket = bracket(host_samples, w_begin - 0.5, w_end + 0.5)
                                w_cpus = per_cpu(*w_bracket) if w_bracket else {}
                                windowed.append((w_shards.get(member, {}), w_cpus))
                            for shard, (cpu, counts) in sorted(by_shard.items()):
                                shard_cpus.setdefault(member, set()).add(cpu)
                                row = {
                                    "round": number,
                                    "nic": "moved" if moved else "as it was",
                                    "population": population,
                                    "capture": capture_name,
                                    "arm": arm_id,
                                    "host": member,
                                    "shard": shard,
                                    "cpu": cpu,
                                    "nic_cpu": nics.get(member),
                                    "secs": span[1] - span[0],
                                }
                                row.update(counts)
                                for name, value in (cpus.get(cpu) or {}).items():
                                    row[f"cpu.{name}"] = value
                                row["windows"] = [
                                    (w_shards.get(shard, (cpu, {}))[1], w_cpus.get(cpu) or {})
                                    for w_shards, w_cpus in windowed
                                ]
                                rows.append(row)
        round_lines.append(
            f"| {number} | {nic_state} | "
            + " | ".join(
                f"{nics.get(host)} ({', '.join(str(cpu) for cpu in sorted(c for c in shard_cpus.get(host, set()) if c is not None))})"
                for host in ("europa", "titan", "hyperion")
            )
            + " |"
        )

    out = sys.stdout
    out.write("# Item 353: long scan steps by shard, beside their cpus\n\n")
    out.write(
        "Written by `m23-i353-report.py` from the captures and cpu samples under "
        f"`{args.captures}`. A step is counted past 100 µs on its own cpu as its node judges it; "
        "*faulted* is such a step that took a page fault inside it, and every other was held by its "
        "work or by the interrupts its cpu took, which a kernel without irq time accounting charges to "
        "the thread it interrupts. A rate is steps past a second of the steps' own cpu.\n\n"
    )
    out.write("## Rounds\n\n| Round | NIC | europa's NIC cpu (shards' cpus) | titan's | hyperion's |\n| --- | --- | --- | --- | --- |\n")
    out.write("\n".join(round_lines) + "\n\n")

    def total(selected, name):
        return sum(row.get(name, 0) for row in selected)

    # by shard: each host's shards over the rounds of each NIC state, a population at a time
    out.write("## Steps past 100 µs on their own cpu, by shard\n\n")
    out.write(
        "Each shard's steps of every round of the NIC state, a population's arms together, beside its "
        "cpu's figures averaged over the same arms. *Unfaulted a second* is the steps past 100 µs on "
        "their own cpu that took no fault, a second of the shard's step cpu, with an approximate 95% "
        "interval.\n\n"
    )
    for population in POPULATIONS:
        out.write(f"### {population}\n\n")
        out.write(
            "| Host | NIC | Shard cpu | The NIC's cpu | Steps | Step cpu (s) | Captures past (faulted) | "
            "Encodes past (faulted) | Unfaulted a second | Softirq % | Irq % | NET_RX/s | NIC irq/s | "
            "NVMe irq/s | TLB/s | CAL/s | Faults/s |\n"
        )
        out.write("| --- " * 17 + "|\n")
        for host in ("europa", "titan", "hyperion"):
            for nic in ("as it was", "moved"):
                selected = [row for row in rows if row["population"] == population and row["host"] == host and row["nic"] == nic]
                for cpu in sorted({row["cpu"] for row in selected if row["cpu"] is not None}):
                    mine = [row for row in selected if row["cpu"] == cpu]
                    steps = total(mine, "captures.steps") + total(mine, "encodes.steps")
                    step_cpu = (total(mine, "captures.cpu_ns") + total(mine, "encodes.cpu_ns")) / 1e9
                    cap, cap_f = total(mine, "captures.past_cpu"), total(mine, "captures.past_cpu_faulted")
                    enc, enc_f = total(mine, "encodes.past_cpu"), total(mine, "encodes.past_cpu_faulted")
                    weight = total(mine, "secs") or 1
                    mean = lambda name: sum(row.get(f"cpu.{name}", 0) * row["secs"] for row in mine) / weight
                    is_nic = any(row["nic_cpu"] == cpu for row in mine)
                    out.write(
                        f"| {host} | {nic} | {cpu} | {'yes' if is_nic else ''} | {steps:,} | {step_cpu:.2f} | "
                        f"{cap} ({cap_f}) | {enc} ({enc_f}) | {interval(cap - cap_f + enc - enc_f, step_cpu)} | "
                        f"{100 * mean('softirq_share'):.2f} | {100 * mean('irq_share'):.2f} | {mean('NET_RX'):,.0f} | "
                        f"{mean('NIC'):,.0f} | {mean('NVME'):,.0f} | {mean('TLB'):,.0f} | {mean('CAL'):,.0f} | "
                        f"{mean('minflt'):,.0f} |\n"
                    )
        out.write("\n")

    # the NIC's shard against the rest of its host's, each NIC state
    out.write("## The NIC's shard against the rest\n\n")
    out.write(
        "Unfaulted steps past 100 µs on their own cpu a second of step cpu, on the shard whose cpu took "
        "the host's NIC interrupts against every other shard of the host, with their counts. With the "
        "NIC moved there is no NIC shard, and the shard that had it is read as the rest are.\n\n"
    )
    out.write("| Population | Host | NIC | The NIC's shard | Count | The rest | Count | Ratio |\n| --- | --- | --- | --- | --- | --- | --- | --- |\n")
    for population in POPULATIONS:
        for host in ("europa", "titan", "hyperion"):
            for nic in ("as it was", "moved"):
                selected = [row for row in rows if row["population"] == population and row["host"] == host and row["nic"] == nic]
                if not selected:
                    continue
                on_nic = [row for row in selected if row["cpu"] is not None and row["cpu"] == row["nic_cpu"]]
                rest = [row for row in selected if row not in on_nic]

                def unfaulted(group):
                    count = sum(row["captures.past_cpu"] - row["captures.past_cpu_faulted"] + row["encodes.past_cpu"] - row["encodes.past_cpu_faulted"] for row in group)
                    exposure = sum(row["captures.cpu_ns"] + row["encodes.cpu_ns"] for row in group) / 1e9
                    return count, exposure

                n_count, n_exp = unfaulted(on_nic)
                r_count, r_exp = unfaulted(rest)
                ratio = f"{(n_count / n_exp) / (r_count / r_exp):.2f}" if n_exp and r_exp and r_count else "-"
                out.write(
                    f"| {population} | {host} | {nic} | {interval(n_count, n_exp) if on_nic else '-'} | {n_count if on_nic else '-'} | "
                    f"{interval(r_count, r_exp)} | {r_count} | {ratio} |\n"
                )
    out.write("\n")

    # faulted against not, and the wall alone, by host and kind
    out.write("## What the kernel did to the long steps\n\n")
    out.write(
        "Every round together. *Past* is past 100 µs by the wall, *own cpu* on the step's own cpu; "
        "*switched* is a step past by the wall whose thread left its cpu inside it, and *faults a "
        "faulted step* the page faults the long steps took over those that faulted.\n\n"
    )
    out.write(
        "| Population | Host | Kind | Steps | Past | Own cpu | Faulted | Unfaulted | By the wall alone | Switched | Faults a faulted step |\n"
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n"
    )
    for population in POPULATIONS:
        for host in ("europa", "titan", "hyperion"):
            selected = [row for row in rows if row["population"] == population and row["host"] == host]
            if not selected:
                continue
            for kind in ("captures", "encodes"):
                steps, past, own = total(selected, f"{kind}.steps"), total(selected, f"{kind}.past"), total(selected, f"{kind}.past_cpu")
                faulted, switched, faults = total(selected, f"{kind}.past_cpu_faulted"), total(selected, f"{kind}.past_switched"), total(selected, f"{kind}.past_faults")
                per = f"{faults / faulted:,.0f}" if faulted else "-"
                out.write(
                    f"| {population} | {host} | {kind} | {steps:,} | {past} | {own} | {faulted} | {own - faulted} | {past - own} | {switched} | {per} |\n"
                )
    out.write("\n")

    # by window: does a shard's long step come with its cpu's softirq time
    out.write("## By window\n\n")
    out.write(
        "Every shard's 2 s windows of a host, as the bench sampled them, put in three equal groups by "
        "the softirqs a second its cpu ran over the window, every kind counted (a count, where the tick-"
        "sampled share is zero in most windows), sorted on that alone; and the unfaulted steps past "
        "100 µs on their own cpu in each group a second of step cpu. Steps that follow softirqs climb "
        "with them; steps of their own do not.\n\n"
    )
    out.write("| Population | Host | Softirqs/s (low) | Rate | Count | Softirqs/s (middle) | Rate | Count | Softirqs/s (high) | Rate | Count |\n")
    out.write("| --- " * 11 + "|\n")
    # each window as (softirqs a second, its unfaulted long steps, step cpu, every figure of its cpu)
    by_host = {}
    for population in POPULATIONS:
        for host in ("europa", "titan", "hyperion"):
            windows = []
            for row in rows:
                if row["population"] != population or row["host"] != host:
                    continue
                for counts, figures in row["windows"]:
                    if not counts or "softirq_share" not in figures:
                        continue
                    count = (counts.get("captures.past_cpu", 0) - counts.get("captures.past_cpu_faulted", 0)
                             + counts.get("encodes.past_cpu", 0) - counts.get("encodes.past_cpu_faulted", 0))
                    exposure = (counts.get("captures.cpu_ns", 0) + counts.get("encodes.cpu_ns", 0)) / 1e9
                    if exposure > 0:
                        softirqs = sum(figures.get(name, 0.0) for name in SOFTIRQS)
                        windows.append((softirqs, count, exposure, figures))
            by_host[(population, host)] = windows
            if len(windows) < 3:
                continue
            windows = sorted(windows, key=lambda window: window[0])
            cells = []
            third = len(windows) // 3
            for group in (windows[:third], windows[third:2 * third], windows[2 * third:]):
                rate = sum(window[0] for window in group) / len(group)
                count = sum(window[1] for window in group)
                exposure = sum(window[2] for window in group)
                cells.append(f"{rate:,.0f} | {interval(count, exposure)} | {count}")
            out.write(f"| {population} | {host} | " + " | ".join(cells) + " |\n")
    out.write("\n")

    # the windows that held an unfaulted long step against those that held none: what each shard's cpu
    # took a second in each
    out.write("## What a long step's window took\n\n")
    out.write(
        "The mean of each interrupt and softirq a second on the shard's own cpu, over the 2 s windows that "
        "held an unfaulted step past 100 µs on its own cpu against those that held none, each window "
        "weighted alike. Interrupts that hold a step would be more frequent in its windows.\n\n"
    )
    kinds = ("NET_RX", "TIMER", "SCHED", "RCU", "BLOCK", "NVME", "LOC", "RES", "CAL", "TLB", "minflt")
    out.write("| Population | Host | Windows | " + " | ".join(kinds) + " |\n")
    out.write("| --- " * (3 + len(kinds)) + "|\n")
    for (population, host), windows in by_host.items():
        held = [window[3] for window in windows if window[1] > 0]
        clear = [window[3] for window in windows if window[1] == 0]
        if not held or not clear:
            continue
        for label, group in (("with a long step", held), ("without", clear)):
            means = [sum(figures.get(kind, 0.0) for figures in group) / len(group) for kind in kinds]
            out.write(f"| {population} | {host} | {len(group):,} {label} | " + " | ".join(f"{mean:,.0f}" for mean in means) + " |\n")
    out.write("\n")

    # the host's own faults and its kernel's work for huge pages, a round each
    out.write("## Each host's faults\n\n")
    out.write(
        "Every process's on the host over the round's samples: page faults a second, and the major "
        "faults, compaction stalls and huge page faults in all.\n\n"
    )
    out.write("| Round | Host | Seconds | Faults/s | Major faults | Compaction stalls | Huge page faults |\n| --- | --- | --- | --- | --- | --- | --- |\n")
    out.write("\n".join(vm_lines) + "\n")

    if args.csv:
        fields = ["round", "nic", "population", "capture", "arm", "host", "shard", "cpu", "nic_cpu", "secs"]
        fields += [f"{kind}.{name}" for kind in ("captures", "encodes") for name in COUNTS]
        fields += [f"cpu.{name}" for name in ("softirq_share", "irq_share", *SOFTIRQS, *INTERRUPTS, "minflt")]
        with open(args.csv, "w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow({name: (f"{value:.6g}" if isinstance(value, float) else value) for name, value in row.items() if name in fields})


if __name__ == "__main__":
    main()
