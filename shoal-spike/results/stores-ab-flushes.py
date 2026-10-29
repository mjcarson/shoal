# O124 on the lab: what each run of a before and after cost a host's whole device, read from the
# counters m16-sample.sh took every second beside it (docs/src/appendix/optimizations.md, O124). The
# lab's stores sit on an LV, and an LV reports no flushes; the NVMe under it counts every flush its
# filesystems ask, so the device's flushes a second, divided by the run's operations a second, are a
# put's flushes. Run with python3 -I <this> <slim or full capture.json>... --samples <host log>... and
# it prints each arm's operations a second and, by host, the whole device's writes, flushes and
# busy share a second and a put, each the median of the arm's runs, over each run's measured window
# less a second at either end. A device's figures include whatever else its host wrote then, the
# root filesystem's journal and logs among them, so an idle second of the same log is the floor.
#
# Every whole NVMe in a host's samples is reported, each by its name. A host's names are its
# kernel's order of finding the drives at boot, so they swap across boots on a host with two:
# europa's Optane was nvme1n1 until its reboot of 2026-10-25 and is nvme0n1 since, where its
# system drive is the other (item 320). Which one holds a store is `lsblk -no PKNAME` of the
# store's device, read on the host at the time; never take it from the name alone
import json
import re
import statistics
import sys
from datetime import datetime

# a whole NVMe, never a partition of one and never a volume on it, which report no flushes
WHOLE_NVME = re.compile(r"nvme\d+n\d+")


def samples_of(path):
    """Each sample's unix time and each device's writes, flushes and busy milliseconds so far"""
    out, at, row = [], None, {}
    for line in open(path):
        parts = line.split()
        if not parts:
            continue
        if parts[0] == "@":
            # a sample is complete once the next one begins
            if at is not None and row:
                out.append((at, row))
            at, row = float(parts[1]), {}
        elif len(parts) >= 20 and WHOLE_NVME.fullmatch(parts[2]):
            # writes completed, ms doing I/O and flushes completed, after major, minor and name
            row[parts[2]] = (int(parts[7]), int(parts[12]), int(parts[19]))
    return out


def window(samples, device, start, end):
    """The device's writes, flushes and busy share a second between the first sample at or after
    start and the last at or before end, or None if the samples do not cover it"""
    inside = [(at, row[device]) for at, row in samples if start <= at <= end and device in row]
    if len(inside) < 2:
        return None
    (a, first), (b, last) = inside[0], inside[-1]
    span = b - a
    return (
        (last[0] - first[0]) / span,
        (last[2] - first[2]) / span,
        (last[1] - first[1]) / (span * 1000),
    )


def main():
    # the captures, then the sample logs, each named by the host it was taken on
    args = sys.argv[1:]
    at = args.index("--samples")
    captures, logs = args[:at], args[at + 1:]
    hosts = {log.rsplit("/", 1)[-1].split(".")[0]: samples_of(log) for log in logs}
    # every run's figures by arm and host
    arms = {}
    for path in captures:
        with open(path) as handle:
            capture = json.load(handle)
        for arm in capture["arms"]:
            for run in arm["runs"]:
                rate = sum(kind["per_sec"] for kind in run["measured"]["kinds"].values())
                begun = datetime.fromisoformat(run["started_at"].replace("Z", "+00:00")).timestamp()
                # the measured window after the warmup, less a second at either end
                start = begun + run["warmup"]["secs"] + 1
                end = begun + run["warmup"]["secs"] + run["measured"]["secs"] - 1
                for host, samples in hosts.items():
                    # every whole NVMe the host's samples name, each reported by its name
                    devices = sorted({device for _, row in samples for device in row})
                    for device in devices:
                        figures = window(samples, device, start, end)
                        if figures is None or not rate:
                            continue
                        arms.setdefault(arm["id"], {}).setdefault((host, device), []).append((rate,) + figures)
    print("| arm | host | ops/s | device writes/s | flushes/s | busy | writes an op | flushes an op |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- |")
    for arm_id in sorted(arms):
        for (host, device), runs in sorted(arms[arm_id].items()):
            # each figure the median of the arm's runs
            rate, writes, flushes, busy = (statistics.median(run[at] for run in runs) for at in range(4))
            per_op_writes = statistics.median(run[1] / run[0] for run in runs)
            per_op_flushes = statistics.median(run[2] / run[0] for run in runs)
            print(
                f"| `{arm_id}` | {host} {device} | {rate:,.0f} | {writes:,.0f} | {flushes:,.0f} | "
                f"{busy * 100:.0f}% | {per_op_writes:.2f} | {per_op_flushes:.2f} |"
            )


main()
