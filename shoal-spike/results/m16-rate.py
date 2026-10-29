#!/usr/bin/env python3
"""F88 on the lab: a device's bytes written and a NIC's bytes received and sent, a second at a time

Reads what shoal-spike/results/m16-sample.sh wrote: m16-rate.py <file> <device> <nic> [from] [to], the
times in unix seconds, and prints each second's MiB/s and the totals over the window.
"""
import sys

SECTOR = 512


def read(path, device, nic):
    """Each sample's time, the device's sectors written and the nic's bytes received and sent"""
    out, at, row = [], None, {}
    for line in open(path):
        parts = line.split()
        if not parts:
            continue
        if parts[0] == "@":
            if at is not None and len(row) == 3:
                out.append((at, row["w"], row["rx"], row["tx"]))
            at, row = float(parts[1]), {}
        elif len(parts) > 10 and parts[2] == device:
            row["w"] = int(parts[9]) * SECTOR
        elif parts[0].rstrip(":") == nic or parts[0].startswith(nic + ":"):
            fields = line.split(":", 1)[1].split()
            row["rx"], row["tx"] = int(fields[0]), int(fields[8])
    return out


def main():
    """Print a rate a second, and the totals over [from, to)"""
    path, device, nic = sys.argv[1:4]
    lo = float(sys.argv[4]) if len(sys.argv) > 4 else 0
    hi = float(sys.argv[5]) if len(sys.argv) > 5 else 1e12
    samples = [s for s in read(path, device, nic) if lo <= s[0] < hi]
    mib = 1 << 20
    for (a, w0, r0, t0), (b, w1, r1, t1) in zip(samples, samples[1:]):
        span = b - a
        print(f"{a:.0f} write {(w1 - w0) / span / mib:7.1f} MiB/s  rx {(r1 - r0) / span / mib:7.1f}  tx {(t1 - t0) / span / mib:7.1f}")
    if len(samples) > 1:
        (a, w0, r0, t0), (b, w1, r1, t1) = samples[0], samples[-1]
        print(f"total {b - a:.0f}s: written {(w1 - w0) / mib:.0f} MiB ({(w1 - w0) / (b - a) / mib:.1f} MiB/s), "
              f"received {(r1 - r0) / mib:.0f} MiB ({(r1 - r0) / (b - a) / mib:.1f} MiB/s)")


if __name__ == "__main__":
    main()
